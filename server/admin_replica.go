// Mochi server: /_/admin/replica/* handlers for mochictl
// Copyright Alistair Cunningham 2026
//
// Three endpoints that mochictl drives the whole-server pair join flow
// through, all UDS-only via the existing admin listener:
//
//   POST /_/admin/replica/join     — start a pair-join attempt
//   POST /_/admin/replica/leave    — leave the pair set
//   GET  /_/admin/replica/status   — poll current state
//
// The state of "a join attempt is in progress" lives in settings.db
// under the `replica.join.*` namespace; this avoids a new table for
// what's at most one row at a time. Join/Leave write the state;
// Status reads it and reconciles with the pair table.

//go:build linux

package main

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
)

// Package-level emit-function variables so tests can stub them out
// (the handlers fire emits via goroutines that outlive a test's
// cleanup, hitting a torn-down data_dir on the way out). Production
// uses the real emit functions.
var (
	admin_replica_emit_join            = replication_emit_join_request
	admin_replica_emit_pair_membership = replication_emit_pair_membership_change
)

// admin_replica_join is POST /_/admin/replica/join.
// Body: {"source": "<peer-id>"}
//
// Refuses if `users.db.users` is non-empty — the empty-replica rule
// from claude/plans/replication.md. Records the pending join in
// settings.db so the Status endpoint can report it, and emits the
// `replication/join-request` op to the source peer. Returns immediately;
// approval / denial arrives asynchronously and is detected by polling
// the Status endpoint.
//
// Idempotent on the same source — a repeat call with the same peer
// re-emits the join-request (in case the prior delivery was lost) and
// keeps the existing pending state. A call with a *different* source
// while another is in flight refuses with a 409.
func admin_replica_join(c *gin.Context) {
	var input struct {
		Source string `json:"source"`
	}
	if err := c.ShouldBindJSON(&input); err != nil || input.Source == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "source is required"})
		return
	}

	udb := db_open("db/users.db")
	if has, _ := udb.exists("select 1 from users limit 1"); has {
		c.JSON(http.StatusForbidden, gin.H{
			"error": "this server has users; replication requires a fresh install",
		})
		return
	}

	current := setting_get("replica.join.peer", "")
	if current != "" && current != input.Source {
		c.JSON(http.StatusConflict, gin.H{
			"error":          "another join attempt is already in progress",
			"current_source": current,
		})
		return
	}

	setting_set("replica.join.peer", input.Source)
	setting_set("replica.join.state", "waiting")
	setting_set("replica.join.reason", "")
	setting_set("replica.join.started", fmt.Sprintf("%d", now()))

	// Replica's own libp2p peer-id is the operator's correlation
	// handle — they'll see it on the source admin's Pair page.
	admin_replica_emit_join(input.Source, "")

	c.JSON(http.StatusOK, gin.H{
		"state":  "waiting",
		"peer":   p2p_id,
		"source": input.Source,
	})
}

// admin_replica_leave is POST /_/admin/replica/leave.
// Clears the local pair table and emits pair-membership-change to the
// peers we used to be paired with, telling them this server is leaving.
// Also clears any in-flight join state.
//
// Per the plan: leave stops sync but does not wipe local user data —
// admins delete users via the existing flow if they want that.
func admin_replica_leave(c *gin.Context) {
	rdb := db_open("db/replication.db")
	rows, _ := rdb.rows("select peer from pair")
	var members []string
	for _, r := range rows {
		if p, ok := r["peer"].(string); ok && p != "" {
			members = append(members, p)
		}
	}

	rdb.exec("delete from pair")
	setting_delete("replica.join.peer")
	setting_delete("replica.join.state")
	setting_delete("replica.join.reason")
	setting_delete("replica.join.started")

	// Tell the (now former) members that this server is gone from the
	// set. The announced set is {ourselves removed} — just leftover
	// members without us.
	if len(members) > 0 {
		admin_replica_emit_pair_membership(members, members)
	}

	c.JSON(http.StatusOK, gin.H{"state": "left", "former_members": members})
}

// admin_replica_status is GET /_/admin/replica/status.
// Polled by mochictl during `replica join`. Reconciles the pending
// state in settings.db with the current pair table:
//
//	pair has source ⇒ approved (and the pending state self-clears here)
//	state="denied"   ⇒ denied (with reason)
//	pending peer set ⇒ waiting
//	otherwise        ⇒ idle (this server isn't in any pair)
//
// Always returns 200 with a JSON state; mochictl never has to interpret
// HTTP codes for this endpoint.
func admin_replica_status(c *gin.Context) {
	rdb := db_open("db/replication.db")
	rows, _ := rdb.rows("select peer from pair")
	var members []string
	for _, r := range rows {
		if p, ok := r["peer"].(string); ok && p != "" {
			members = append(members, p)
		}
	}

	pendingPeer := setting_get("replica.join.peer", "")
	pendingState := setting_get("replica.join.state", "")
	reason := setting_get("replica.join.reason", "")

	state := "idle"
	if pendingPeer != "" {
		// Approved? Source landed in our pair table.
		inPair := false
		for _, m := range members {
			if m == pendingPeer {
				inPair = true
				break
			}
		}
		switch {
		case inPair:
			state = "approved"
			// Self-clearing: once the operator observes "approved" via
			// the status endpoint, the pending state can drop. The
			// per-scope bulk-bootstrap progress is reported via
			// `mochi.replication.status()` (aggregate `bootstrap_pending`)
			// and `mochi.replication.bootstrap_progress()` (drill-down).
			setting_delete("replica.join.peer")
			setting_delete("replica.join.state")
			setting_delete("replica.join.reason")
			setting_delete("replica.join.started")
		case pendingState == "denied":
			state = "denied"
		default:
			state = "waiting"
		}
	} else if len(members) > 0 {
		state = "approved"
	}

	c.JSON(http.StatusOK, gin.H{
		"state":   state,
		"peer":    p2p_id,
		"source":  pendingPeer,
		"members": members,
		"reason":  reason,
	})
}

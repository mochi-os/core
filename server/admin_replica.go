// Mochi server: /_/admin/replica/* handlers for mochictl
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.
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

//go:build linux || darwin || windows

package main

import (
	"fmt"
	"net/http"
	"path/filepath"

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
// Body: {"source": "<peer-id>", "addresses": ["<multiaddr>", ...]}
//
// Refuses if `users.db.users` is non-empty — the empty-replica rule
// from claude/plans/replication.md. Records the pending join in
// settings.db so the Status endpoint can report it, and emits the
// `replication/join-request` op to the source peer. Returns immediately;
// approval / denial arrives asynchronously and is detected by polling
// the Status endpoint.
//
// addresses is the operator escape hatch for a source that automatic
// discovery (mDNS, peers/request) cannot reach: each entry is seeded
// into the peer registry before the emit, so the queued join-request
// has somewhere to dial immediately. Entries may carry the /p2p/<id>
// suffix (which must then name the source) or omit it.
//
// Idempotent on the same source — a repeat call with the same peer
// re-emits the join-request (in case the prior delivery was lost) and
// keeps the existing pending state. A call with a *different* source
// while another is in flight refuses with a 409.
func admin_replica_join(c *gin.Context) {
	var input struct {
		Source    string   `json:"source"`
		Addresses []string `json:"addresses"`
	}
	if err := c.ShouldBindJSON(&input); err != nil || input.Source == "" {
		respond_error(c, http.StatusBadRequest, "source_required", "errors.source_required", nil)
		return
	}

	addresses, bad := peer_addresses_normalise(input.Source, input.Addresses)
	if bad != "" {
		respond_error(c, http.StatusBadRequest, "address_invalid", "errors.address_invalid", map[string]any{"address": bad})
		return
	}

	udb := db_open("db/users.db")
	if has, _ := udb.exists("select 1 from users limit 1"); has {
		respond_error(c, http.StatusForbidden, "users_db_not_empty", "errors.users_db_not_empty", nil)
		return
	}

	current := setting_get("replica.join.peer", "")
	if current != "" && current != input.Source {
		// respond_error returns the fixed {error, message} shape; this
		// callsite needs an extra `current_source` field so the operator
		// can see which source is currently in flight.
		lang := request_language(c, nil)
		c.JSON(http.StatusConflict, gin.H{
			"error":          "join_in_progress",
			"message":        resolve_core_label(lang, "errors.join_in_progress", nil),
			"current_source": current,
		})
		c.Abort()
		return
	}

	setting_set("replica.join.peer", input.Source)
	setting_set("replica.join.state", "waiting")
	setting_set("replica.join.reason", "")
	setting_set("replica.join.started", fmt.Sprintf("%d", now()))

	// Seed operator-supplied addresses so the join-request emit below
	// has somewhere to dial without waiting on discovery.
	for _, address := range addresses {
		peer_discovered_address(input.Source, address)
	}

	// Replica's own libp2p peer-id is the operator's correlation
	// handle — they'll see it on the source admin's Pair page.
	admin_replica_emit_join(input.Source, "")

	c.JSON(http.StatusOK, gin.H{
		"state":  "waiting",
		"peer":   net_id,
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
	pair_membership_refresh()
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

// admin_replica_approve is POST /_/admin/replica/approve. Approves a pending
// pair-join request from <peer>, adding it to the pair set and firing the
// join-approved op — the headless-ops equivalent of the settings Approve
// button.
func admin_replica_approve(c *gin.Context) {
	var input struct {
		Peer string `json:"peer"`
	}
	if err := c.ShouldBindJSON(&input); err != nil || input.Peer == "" {
		respond_error(c, http.StatusBadRequest, "source_required", "errors.source_required", nil)
		return
	}
	status, err := replication_join_approve(input.Peer)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"state": "error", "peer": input.Peer, "detail": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"state": status, "peer": input.Peer})
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

	pending_peer := setting_get("replica.join.peer", "")
	pending_state := setting_get("replica.join.state", "")
	reason := setting_get("replica.join.reason", "")

	state := "idle"
	if pending_peer != "" {
		// Approved? Source landed in our pair table.
		in_pair := false
		for _, m := range members {
			if m == pending_peer {
				in_pair = true
				break
			}
		}
		switch {
		case in_pair:
			state = "approved"
			// Self-clearing: once the operator observes "approved" via
			// the status endpoint, the pending state can drop. The
			// per-scope bulk-bootstrap progress is reported via
			// `mochi.replication.status()` (aggregate `bootstrap_pending`)
			// and `mochi.replication.bootstrap.progress()` (drill-down).
			setting_delete("replica.join.peer")
			setting_delete("replica.join.state")
			setting_delete("replica.join.reason")
			setting_delete("replica.join.started")
		case pending_state == "denied":
			state = "denied"
		default:
			state = "waiting"
		}
	} else if len(members) > 0 {
		state = "approved"
	}

	// Announced display names for the members, keyed by peer id —
	// annotation data for mochictl output. Self-asserted labels; the
	// fingerprint (also shown) is the authoritative identifier.
	names := gin.H{}
	for _, m := range members {
		if name := peer_name(m); name != "" {
			names[m] = name
		}
	}

	out := gin.H{
		"state":       state,
		"peer":        net_id,
		"fingerprint": fingerprint_hyphens(fingerprint(net_id)),
		"addresses":   net_addresses(),
		"source":      pending_peer,
		"members":     members,
		"names":       names,
		"reason":      reason,
	}
	if state == "waiting" {
		out["delivery"] = admin_replica_delivery(pending_peer)
	}
	c.JSON(http.StatusOK, out)
}

// admin_replica_delivery reports whether the pending join-request has
// actually left this server — the diagnostics mochictl renders while
// polling, so an undeliverable source (unknown address, unreachable)
// surfaces to the operator instead of an indefinite silent wait.
//
//	queued    — the join-request still sits in queue.db (false once
//	            the source acked it)
//	attempts  — delivery attempts so far
//	error     — last transport error, "" if none
//	addresses — how many addresses the registry holds for the source;
//	            0 means undeliverable until discovery or --address
//	silent    — the source is in the silent-failure cache (repeated
//	            connection failures)
func admin_replica_delivery(source string) gin.H {
	queued := false
	attempts := int64(0)
	last := ""
	if file_exists(filepath.Join(data_dir, "db", "queue.db")) {
		qdb := db_open("db/queue.db")
		row, _ := qdb.row(
			"select attempts, last_error from queue where target=? and service='replication' and event='join/request' order by created desc limit 1",
			source)
		if row != nil {
			queued = true
			attempts = row_int(row, "attempts")
			last = row_string(row, "last_error")
		}
	}
	return gin.H{
		"queued":    queued,
		"attempts":  attempts,
		"error":     last,
		"addresses": peer_addresses_count(source),
		"silent":    peer_is_silent(source),
	}
}

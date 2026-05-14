// Mochi server: /_/admin/replication/* handlers for mochictl
// Copyright Alistair Cunningham 2026
//
// Sibling to admin_replica.go. The /_/admin/replica/* endpoints are
// for the *joining replica's* perspective (join / leave / status of
// THIS server's pair attempt). The /_/admin/replication/* endpoints
// below are for operator inspection / management of the pair set as
// it exists right now — listing current members, removing a specific
// member, summarising replication health.
//
//   GET  /_/admin/replication/status         — pair + per-user-host summary
//   GET  /_/admin/replication/pair           — current pair members
//   POST /_/admin/replication/pair/remove    — kick a specific pair member
//
// Bootstrap progress / per-(user, scope) lag is part of mochi.replication.status()
// landed in #66; until that lands, `status` reports the bounded view it can
// compute from replication.db.pair + replication.db.hosts.

//go:build linux

package main

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// admin_replication_status is GET /_/admin/replication/status.
// Returns a minimal snapshot of the replication state visible from
// this server: own peer-id, current pair members, count of per-user
// opt-in hosts, count of pending link-requests, count of pending
// join-requests.
//
// The full health / lag / queue-depth view will land via the
// mochi.replication.status() Starlark API in #66; until then this
// is a bounded "what's the pair set, and is anything pending?" read.
func admin_replication_status(c *gin.Context) {
	rdb := db_open("db/replication.db")

	var pair []string
	if rows, err := rdb.rows("select peer from pair"); err == nil {
		for _, r := range rows {
			if p, ok := r["peer"].(string); ok && p != "" {
				pair = append(pair, p)
			}
		}
	}

	hosts_count := int64(0)
	if row, _ := rdb.row("select count(*) as c from hosts"); row != nil {
		if v, ok := row["c"].(int64); ok {
			hosts_count = v
		}
	}

	links_pending := int64(0)
	if row, _ := rdb.row("select count(*) as c from links"); row != nil {
		if v, ok := row["c"].(int64); ok {
			links_pending = v
		}
	}

	joins_pending := int64(0)
	if row, _ := rdb.row("select count(*) as c from joins"); row != nil {
		if v, ok := row["c"].(int64); ok {
			joins_pending = v
		}
	}

	bootstrap_pending := int64(0)
	if row, _ := rdb.row("select count(*) as c from bootstrap where state != 'done'"); row != nil {
		if v, ok := row["c"].(int64); ok {
			bootstrap_pending = v
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"peer":              p2p_id,
		"pair":              pair,
		"hosts_count":       hosts_count,
		"links_pending":     links_pending,
		"joins_pending":     joins_pending,
		"bootstrap_pending": bootstrap_pending,
	})
}

// admin_replication_pair is GET /_/admin/replication/pair.
// Returns the current pair member list.
func admin_replication_pair(c *gin.Context) {
	rdb := db_open("db/replication.db")
	var members []map[string]any
	if rows, err := rdb.rows("select peer, added, role from pair order by added"); err == nil {
		for _, r := range rows {
			peer, _ := r["peer"].(string)
			added, _ := r["added"].(int64)
			role, _ := r["role"].(string)
			members = append(members, map[string]any{
				"peer":  peer,
				"added": added,
				"role":  role,
			})
		}
	}
	c.JSON(http.StatusOK, gin.H{"members": members})
}

// admin_replication_resync is POST /_/admin/replication/resync.
// Body: {"peer": "<peer-id>"}
//
// Re-seeds the bootstrap state machine against `peer` by clearing
// any 'done' rows for that peer and re-running bootstrap_start.
// Idempotent on the wire: the receiver's manifest-diff skips files
// whose local copy already matches by size + sha256, so a resync of
// a fully-synced replica is cheap (just hash comparisons).
//
// Use cases: a transient transport failure dropped some chunks; the
// operator manually copied files between data dirs and wants to
// re-verify; alpha-rollout verification re-runs.
func admin_replication_resync(c *gin.Context) {
	var input struct {
		Peer string `json:"peer"`
	}
	if err := c.ShouldBindJSON(&input); err != nil || input.Peer == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "peer is required"})
		return
	}

	// Wipe any previous bootstrap rows for this peer so the state
	// machine starts fresh. bootstrap_start re-seeds the four scopes
	// at 'queued' and emits the manifest-requests.
	rdb := db_open("db/replication.db")
	rdb.exec("delete from bootstrap where peer=?", input.Peer)
	bootstrap_start(input.Peer)

	c.JSON(http.StatusOK, gin.H{
		"peer":  input.Peer,
		"state": "queued",
	})
}

// admin_replication_pair_remove is POST /_/admin/replication/pair/remove.
// Body: {"peer": "<peer-id>"}
//
// Removes the specified peer from this server's pair set and emits a
// pair-membership-change op to the remaining members so they update
// their views too. The removed peer's local copies are NOT wiped per
// the plan — leave-vs-admin-delete distinction is preserved. The
// removed peer learns it's been kicked the next time it processes a
// membership-change from one of the remaining members.
func admin_replication_pair_remove(c *gin.Context) {
	var input struct {
		Peer string `json:"peer"`
	}
	if err := c.ShouldBindJSON(&input); err != nil || input.Peer == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "peer is required"})
		return
	}

	removed, remaining, ok := replication_pair_remove(input.Peer)
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "peer is not a pair member"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"removed": removed,
		"members": remaining,
	})
}

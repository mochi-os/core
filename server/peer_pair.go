// Mochi server: Pair membership cache.
//
// peer_is_pair is consulted on every inbound libp2p stream (the
// rate-limit gate skips bootstrap + pair members as trusted
// infrastructure) and on every inbound pubsub message. At wasabi's
// traffic that's hundreds of calls per second for state that changes
// only when an operator runs `mochictl replica join/leave` — minutes
// or hours apart at most. Hitting replication.db for each one is
// wasted I/O.
//
// This file maintains an in-memory copy of replication.db's pair
// table. Reads (peer_is_pair) are O(1) under RLock. Writes refresh
// the cache via pair_membership_refresh — every site that mutates
// the pair table calls it. Truth still lives in the DB; the cache
// is a derived read path.
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import "sync"

var (
	pair_members      = map[string]bool{}
	pair_members_lock sync.RWMutex
)

// peer_is_pair returns true if the peer ID is in the local pair set.
// Pair members are our own infrastructure (whole-server replication
// partners we explicitly chose to pair with) — the inbound stream
// rate limit is anti-DoS for unknown peers and shouldn't throttle
// them. During bulk bootstrap the file-scope driver can legitimately
// fire >100 chunk-fetch streams per second on a fast local network,
// and rate-limiting them stalls the bootstrap with a flood of
// "Net rate limited peer" log lines.
func peer_is_pair(id string) bool {
	if id == "" {
		return false
	}
	pair_members_lock.RLock()
	defer pair_members_lock.RUnlock()
	return pair_members[id]
}

// pair_membership_refresh reloads pair_members from replication.db.
// Called at startup AND from every site that mutates the pair table
// (replication_link.go, admin_replica.go). Safe to call from any
// goroutine; the per-call SQL cost is small (pair set is tiny — N is
// typically 2-3, capped operationally at ~10).
func pair_membership_refresh() {
	rdb := db_open("db/replication.db")
	rows, err := rdb.rows("select peer from pair")
	if err != nil {
		warn("Pair-membership refresh: db error: %v", err)
		return
	}
	fresh := map[string]bool{}
	for _, row := range rows {
		if peer, _ := row["peer"].(string); peer != "" {
			fresh[peer] = true
		}
	}
	pair_members_lock.Lock()
	pair_members = fresh
	pair_members_lock.Unlock()
}

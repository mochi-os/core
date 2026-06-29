// Operator-free auto-reseed of an unfillable anchored replication gap (#101, Phase 2).
//
// When the receiver-initiated gap-fill exhausts its retries (the peer cannot supply
// the missing ops — pruned past retention, or a journal gap), the stream can't
// self-heal by re-ship. Today that escalates to an operator reseed. This dispatches
// the reseed automatically instead — made safe by two layers it does NOT bypass:
//   1. it skips a DB with un-shipped local writes (reseed_source_missing_ops) — those
//      would be lost; and
//   2. the swap itself is gated by the row-level subset guard
//      (bootstrap_swap_subset_ok), which refuses and leaves the operator escalation to
//      fire if the target uniquely holds any row.
//
// Default OFF: the guard must prove itself on operator reseeds before the automation
// is enabled (staged rollout, claude/plans/replication-auto-reseed.md).
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"strings"
	"sync"
)

// replication_auto_reseed_enabled gates the auto-reseed. OFF until the subset guard
// has soaked on operator reseeds; while off, an exhausted gap escalates as before.
var replication_auto_reseed_enabled = false

// auto_reseed_inflight prevents re-dispatching a reseed for a DB whose previous
// auto-reseed is still running (the stall sweep re-evaluates periodically).
var auto_reseed_inflight sync.Map // rel path -> struct{}

// auto_reseed_dispatch is the reseed call, a var so tests can capture it.
var auto_reseed_dispatch = bootstrap_db_reseed

// replication_auto_reseed_try attempts to self-heal an unfillable anchored gap by
// reseeding the stalled stream's DB(s) from the peer. Returns true if it dispatched a
// reseed (or one is already running), so the caller skips the operator-escalation
// warn. The async reseed either heals (info) or is refused by the subset guard (warn,
// re-escalating). No-op and returns false when disabled.
func replication_auto_reseed_try(s StalledStream) bool {
	if !replication_auto_reseed_enabled {
		return false
	}
	handled := false
	for _, rel := range replication_stream_db_paths(s.User, s.Database) {
		if _, busy := auto_reseed_inflight.Load(rel); busy {
			handled = true // a reseed is already running for this DB; don't escalate yet
			continue
		}
		// Un-shipped local writes would be lost by a reseed (the subset guard is by
		// primary key and won't catch a clobbered local edit) — leave it for an
		// operator, who can merge.
		if reseed_source_missing_ops(rel, s.Peer) {
			continue
		}
		scope := bootstrap_scope_userdbs
		if strings.HasPrefix(rel, "db/") {
			scope = bootstrap_scope_sysdbs
		}
		auto_reseed_inflight.Store(rel, struct{}{})
		rel, peer := rel, s.Peer
		go func() {
			defer auto_reseed_inflight.Delete(rel)
			if err := auto_reseed_dispatch(peer, scope, rel); err != nil {
				warn("Replication auto-reseed: %q from peer %q failed or was refused by the subset guard (#101): %v — operator reseed still required", rel, peer, err)
				return
			}
			info("Replication auto-reseed: %q re-seeded from peer %q — unfillable gap self-healed (#101)", rel, peer)
		}()
		handled = true
	}
	return handled
}

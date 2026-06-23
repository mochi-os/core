// Mochi server: bootstrap retry-loop convergence regression (#6).
//
// The 2026-06-23 prod feeds.db corruption was driven by a stuck bootstrap-retry
// loop: bootstrap_set_state(active) reset the retry-backoff counter (attempts)
// to 0 on every chunk, so a scope that kept making partial progress but never
// reached 'done' re-fired at the 30s floor forever — repeatedly Restoring the
// 1.4 GB feeds.db into the live handle and starving the WAL checkpoint until
// the DB corrupted. attempts must now stay monotonic across active (so the
// per-row backoff grows to its 30-minute cap and the loop settles into a slow
// probe); only 'done' (success) and an explicit operator resume/resync clear it.
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import "testing"

func TestBootstrapActiveKeepsRetryAttemptsMonotonic(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	rdb := db_open("db/replication.db")

	attemptsOf := func() int {
		return rdb.integer("select attempts from bootstrap where scope='userdbs' and peer='p'")
	}

	// A scope re-driven a few times already (attempts=5), still transferring.
	rdb.exec("insert into bootstrap (scope, peer, state, position, attempts) values ('userdbs', 'p', 'queued', '', 5)")

	// Progress (a chunk lands) must NOT reset the backoff counter — this is the
	// fix. It re-fired forever before because attempts went back to 0 here.
	bootstrap_set_state("userdbs", "p", bootstrap_state_active, "pos1")
	if got := attemptsOf(); got != 5 {
		t.Fatalf("attempts after active = %d, want 5 (must stay monotonic — the loop fix)", got)
	}
	bootstrap_set_state("userdbs", "p", bootstrap_state_active, "pos2")
	if got := attemptsOf(); got != 5 {
		t.Fatalf("attempts after 2nd active = %d, want 5", got)
	}

	// But active MUST still stamp progress, or the stall-check would re-drive a
	// live transfer (idle < bootstrap_stall_seconds is what leaves it alone).
	if p := rdb.integer64("select progress from bootstrap where scope='userdbs' and peer='p'"); p == 0 {
		t.Error("active must stamp progress so a live transfer is left alone by the stall check")
	}

	// Reaching 'done' clears attempts so a future re-bootstrap starts fresh.
	bootstrap_set_state("userdbs", "p", bootstrap_state_done, "")
	if got := attemptsOf(); got != 0 {
		t.Fatalf("attempts after done = %d, want 0 (reset on success)", got)
	}
}

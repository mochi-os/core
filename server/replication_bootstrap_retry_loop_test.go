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

// The no-progress abandon cap: a bootstrap that has retried past the cap with no
// real forward progress (a dead/nonexistent source) is abandoned, while a
// slow-but-progressing transfer is NEVER abandoned regardless of attempts.
func TestBootstrapShouldAbandon(t *testing.T) {
	origA, origS := bootstrap_retry_abandon_attempts, bootstrap_retry_abandon_seconds
	bootstrap_retry_abandon_attempts, bootstrap_retry_abandon_seconds = 48, 6*3600
	defer func() { bootstrap_retry_abandon_attempts, bootstrap_retry_abandon_seconds = origA, origS }()

	if !bootstrap_should_abandon(48, 7*3600) {
		t.Error("dead source (>= cap attempts, long no-progress) should be abandoned")
	}
	if bootstrap_should_abandon(200, 60) {
		t.Error("slow-but-progressing (recent progress) must NEVER be abandoned, whatever the attempts")
	}
	if bootstrap_should_abandon(47, 99*3600) {
		t.Error("below the attempts cap should not abandon yet (give it a fair chance)")
	}
}

// active stamps progressed (the real-progress timestamp the abandon cap reads),
// distinct from progress (which the retry re-fire also stamps for backoff).
func TestBootstrapActiveStampsProgressed(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	rdb := db_open("db/replication.db")
	rdb.exec("insert into bootstrap (scope, peer, state, attempts) values ('files', 'p', 'queued', 0)")
	bootstrap_set_state("files", "p", bootstrap_state_active, "pos1")
	if p := rdb.integer64("select progressed from bootstrap where scope='files' and peer='p'"); p == 0 {
		t.Error("active must stamp progressed")
	}
}

// The retry driver marks a never-progressing dead source irreparable and then
// drops it from the retry set (the query excludes irreparable), so it stops
// probing + re-alerting instead of churning until T_forget.
func TestBootstrapRetryAbandonsDeadSource(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	rdb := db_open("db/replication.db")

	origA, origS := bootstrap_retry_abandon_attempts, bootstrap_retry_abandon_seconds
	bootstrap_retry_abandon_attempts, bootstrap_retry_abandon_seconds = 3, 60
	defer func() { bootstrap_retry_abandon_attempts, bootstrap_retry_abandon_seconds = origA, origS }()

	// Never progressed (progressed=0 -> huge sinceProgress), past the attempts
	// cap, progress old enough to be eligible.
	rdb.exec("insert into bootstrap (scope, peer, state, position, progress, progressed, attempts) values ('files', 'deadpeer', 'queued', '', ?, 0, 5)", now()-100000)

	bootstrap_retry_incomplete_once()
	row, _ := rdb.row("select state from bootstrap where scope='files' and peer='deadpeer'")
	if row == nil || row["state"] != bootstrap_state_irreparable {
		t.Fatalf("dead-source bootstrap should be abandoned (irreparable), got %v", row)
	}
	// A second pass must not revive it (excluded from the retry set).
	bootstrap_retry_incomplete_once()
	row2, _ := rdb.row("select state from bootstrap where scope='files' and peer='deadpeer'")
	if row2 == nil || row2["state"] != bootstrap_state_irreparable {
		t.Fatalf("abandoned row must stay irreparable across passes, got %v", row2)
	}
}

// An abandoned (irreparable) userdbs scope must NOT count as in-progress, or the
// per-user bootstrap trigger stays wedged forever waiting on a dead pair pull.
func TestBootstrapUserdbsInProgressIrreparable(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	rdb := db_open("db/replication.db")
	rdb.exec("insert into bootstrap (scope, peer, state) values ('userdbs', 'p', ?)", bootstrap_state_irreparable)
	if bootstrap_userdbs_in_progress("p") {
		t.Error("abandoned userdbs must not count as in-progress")
	}
	rdb.exec("update bootstrap set state='active' where scope='userdbs' and peer='p'")
	if !bootstrap_userdbs_in_progress("p") {
		t.Error("active userdbs should count as in-progress")
	}
}

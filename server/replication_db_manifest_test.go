// Mochi server: #36 regression — an interrupted/failure-prone db-scope
// bootstrap must never settle 'done' while DBs are still missing.
//
// A transient empty db-manifest result (the source momentarily mid-walk or
// restarting) arriving while the bulk driver still had DBs in flight used to
// clobber the whole userdbs scope to 'done' — a silently incomplete replica
// that believes it is fully synced (rig T4, 2026-06-25: 'done' with failed>0
// and 184 DBs absent). The 0-entry path now mirrors the file scope: it settles
// only when the driver has drained (pending==0), and via bootstrap_settled_state
// so a prior failure yields 'incomplete'.
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import "testing"

func db_manifest_test_state(scope, peer string) string {
	rdb := db_open("db/replication.db")
	row, _ := rdb.row("select state from bootstrap where scope=? and peer=?", scope, peer)
	if row == nil {
		return ""
	}
	s, _ := row["state"].(string)
	return s
}

func TestDbManifestEmptyResultDoesNotClobberInflight(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	scope := bootstrap_scope_userdbs

	// (1) In-flight transfer (active, pending=50): a transient empty manifest
	// result must NOT clobber it to 'done'.
	peer := "peerInflight"
	bootstrap_set_pending(scope, peer, 50)
	replication_bootstrap_db_manifest_result_apply(peer, &BootstrapDBManifestResult{Scope: scope})
	if st := db_manifest_test_state(scope, peer); st == bootstrap_state_done {
		t.Errorf("SILENT-INCOMPLETE (#36): empty manifest clobbered an in-flight (pending=50) scope to %q", st)
	}

	// (2) Drained (pending=0), no failures: an empty result settles 'done'.
	peer2 := "peerClean"
	bootstrap_set_state(scope, peer2, bootstrap_state_active, "0")
	replication_bootstrap_db_manifest_result_apply(peer2, &BootstrapDBManifestResult{Scope: scope})
	if st := db_manifest_test_state(scope, peer2); st != bootstrap_state_done {
		t.Errorf("drained + no failures: empty result should settle 'done', got %q", st)
	}

	// (3) Drained (pending=0) but a transfer failed: settles 'incomplete', so the
	// retry manager drains it — never a false 'done'.
	peer3 := "peerFailed"
	bootstrap_set_state(scope, peer3, bootstrap_state_active, "0")
	bootstrap_failed_increment(scope, peer3)
	replication_bootstrap_db_manifest_result_apply(peer3, &BootstrapDBManifestResult{Scope: scope})
	if st := db_manifest_test_state(scope, peer3); st != bootstrap_state_incomplete {
		t.Errorf("drained + a failure: empty result should settle 'incomplete', got %q", st)
	}
}

// TestUserBootstrapSkipsDuringPairBootstrap is the #36 root-cause regression:
// while a whole-server (pair) userdbs bootstrap owns the shared (userdbs, peer)
// row, the per-user new-user-bootstrap trigger must be suppressed — else its
// bootstrap_start_user set_pending-clobbers the shared counter and settles the
// scope 'done' before the pair transfer finishes (rig-confirmed via instrument:
// 3 premature settles, per-user fetches at ndb=84, failed=0).
func TestUserBootstrapSkipsDuringPairBootstrap(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	var calls int
	orig := replication_user_bootstrap_hook
	replication_user_bootstrap_hook = func(peer, uid string) { calls++ }
	defer func() { replication_user_bootstrap_hook = orig }()

	peer := "peerPair"

	// Pair userdbs bootstrap in progress (active) → per-user trigger suppressed.
	bootstrap_set_state(bootstrap_scope_userdbs, peer, bootstrap_state_active, "50")
	replication_user_bootstrap(peer, "user1")
	if calls != 0 {
		t.Errorf("#36: per-user bootstrap fired while the pair userdbs bootstrap was in progress (clobbers the shared row); calls=%d", calls)
	}

	// Pair bootstrap done → a genuinely new user falls through and fetches.
	bootstrap_set_state(bootstrap_scope_userdbs, peer, bootstrap_state_done, "")
	replication_user_bootstrap(peer, "user2")
	if calls != 1 {
		t.Errorf("after the pair bootstrap is done, a new-user trigger should fetch; calls=%d", calls)
	}

	// No bootstrap row at all (pure per-user link case) → fetches.
	replication_user_bootstrap("peerOther", "user3")
	if calls != 2 {
		t.Errorf("with no in-progress bootstrap the trigger should fetch; calls=%d", calls)
	}
}

// Mochi server: timeout/retention model invariants + per-class retention.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import "testing"

// TestQueueRetentionIsOneFloor: queue_cleanup keeps every message class for
// queue_age_maximum (7d).
//
// It used to be two floors - replication ops kept 30 days (T_forget) so an
// offline replica could still replay and merge. Replication was removed in
// July 2026 and nothing sends that service, so the replication arm of the
// predicate could not match and the other arm (service != 'replication') was
// true of every row. The service names below are kept as ordinary strings: a
// message naming one gets the same 7 days as anything else.
func TestQueueRetentionIsOneFloor(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db := queue_test_table()

	insert := func(id, service string, age_days int64) {
		created := now() - age_days*86400
		db.exec(`insert into queue (id, type, target, from_entity, to_entity, service, event, next_retry, created, priority)
			values (?, 'direct', 'peer-x', '', '', ?, 'sql/op', ?, ?, ?)`,
			id, service, created, created, priority_interactive)
	}
	insert("repl-8d", "replication", 8) // no longer privileged -> drop (> 7d)
	insert("repl-3d", "replication", 3) // -> keep
	insert("app-8d", "feeds", 8)        // -> drop (> 7d)
	insert("app-3d", "feeds", 3)        // -> keep

	queue_cleanup()

	survives := func(id string) bool {
		ok, _ := db.exists("select 1 from queue where id = ?", id)
		return ok
	}
	for _, id := range []string{"repl-8d", "app-8d"} {
		if survives(id) {
			t.Errorf("%s is older than queue_age_maximum and should have been dropped", id)
		}
	}
	for _, id := range []string{"repl-3d", "app-3d"} {
		if !survives(id) {
			t.Errorf("%s is inside queue_age_maximum and should have survived", id)
		}
	}
}

// TestRetentionOrderingInvariant: the dedup window must outlive the longest
// retry gap, or a retry arrives after the receiver has forgotten the message
// and is delivered twice.
//
// This also asserted replication_op_retention >= queue_age_maximum, guarding
// the 7-vs-30 bug where replication ops were trimmed on the shorter schedule
// and the safe-merge window silently shrank. There is one floor now, so there
// is no ordering left to violate.
func TestRetentionOrderingInvariant(t *testing.T) {
	var maximum_retry int64
	for _, d := range retry_delays {
		if d > maximum_retry {
			maximum_retry = d
		}
	}
	if seen_messages_ttl < 2*maximum_retry {
		t.Fatalf("invariant violated: seen_messages_ttl (%d) must be >= 2x max retry gap (%d)",
			seen_messages_ttl, maximum_retry)
	}
}

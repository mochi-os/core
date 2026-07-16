// Mochi server: timeout/retention model invariants + per-class retention.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import "testing"

// TestQueueRetentionByClass: queue_cleanup keeps replication ops for
// replication_op_retention (30d) and every other message class for
// queue_max_age (7d), so an offline replica can still replay its ops while
// transient app traffic is still trimmed promptly.
func TestQueueRetentionByClass(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db := queue_test_table()

	insert := func(id, service string, ageDays int64) {
		created := now() - ageDays*86400
		db.exec(`insert into queue (id, type, target, from_entity, to_entity, service, event, next_retry, created, priority)
			values (?, 'direct', 'peer-x', '', '', ?, 'sql/op', ?, ?, ?)`,
			id, service, created, created, priority_bulk)
	}
	insert("repl-8d", "replication", 8)   // replication, 8d  -> keep (< 30d)
	insert("repl-31d", "replication", 31) // replication, 31d -> drop (> 30d)
	insert("app-8d", "feeds", 8)          // other, 8d        -> drop (> 7d)
	insert("app-3d", "feeds", 3)          // other, 3d        -> keep (< 7d)

	queue_cleanup()

	surv := func(id string) bool {
		ok, _ := db.exists("select 1 from queue where id = ?", id)
		return ok
	}
	if !surv("repl-8d") {
		t.Error("replication op at 8d should survive the 30d floor")
	}
	if surv("repl-31d") {
		t.Error("replication op at 31d should be dropped")
	}
	if surv("app-8d") {
		t.Error("non-replication message at 8d should be dropped (7d floor)")
	}
	if !surv("app-3d") {
		t.Error("non-replication message at 3d should survive")
	}
}

// TestRetentionOrderingInvariant: the replication op retention floor must
// outlive the generic queue floor — otherwise replication ops would be
// trimmed on the shorter schedule and the safe-merge window would silently
// shrink (the 7-vs-30 bug). Part of the unified timeout/retention model's
// CI gates.
func TestRetentionOrderingInvariant(t *testing.T) {
	if replication_op_retention < queue_max_age {
		t.Fatalf("invariant violated: replication_op_retention (%d) must be >= queue_max_age (%d)",
			replication_op_retention, queue_max_age)
	}
	// dedup window must outlive the longest retry gap (existing invariant,
	// re-asserted here so the model's gates live in one place).
	var maxRetry int64
	for _, d := range retry_delays {
		if d > maxRetry {
			maxRetry = d
		}
	}
	if seen_messages_ttl < 2*maxRetry {
		t.Fatalf("invariant violated: seen_messages_ttl (%d) must be >= 2x max retry gap (%d)",
			seen_messages_ttl, maxRetry)
	}
}

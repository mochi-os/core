// Mochi server: error-event wiring at the queue drop/expire sites
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import "testing"

// error_test_queue_insert adds a queue row with full control over the
// fields the error wiring reads (provenance + created age).
func error_test_queue_insert(db *DB, id, fromEntity, fromApp, toEntity, service, event string, created int64) {
	db.exec(`insert into queue (id, target, from_entity, to_entity, service, event, from_app, next_retry, created)
		values (?, '', ?, ?, ?, ?, ?, 0, ?)`,
		id, fromEntity, toEntity, service, event, fromApp, created)
}

// error_test_capture stubs queue_error_dispatch and returns the captured
// calls plus a restore func.
type error_call struct{ id, to, code, reason string }

func error_test_capture(calls *[]error_call) func() {
	orig := queue_error_dispatch
	queue_error_dispatch = func(q *QueueEntry, code, reason string) {
		*calls = append(*calls, error_call{q.ID, q.ToEntity, code, reason})
	}
	return func() { queue_error_dispatch = orig }
}

// TestQueueDropFiresErrorEvent: a NACK drop maps the reason to the right
// code (unknown/rejected), dedup/unmapped reasons fire nothing, and a
// missing row is a safe no-op.
func TestQueueDropFiresErrorEvent(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db := queue_test_table()

	var calls []error_call
	defer error_test_capture(&calls)()

	error_test_queue_insert(db, "q1", "owner", "feeds", "gone", "feeds", "post/create", now())
	queue_drop("q1", fail_unknown_user)
	if len(calls) != 1 || calls[0].code != error_code_message_unknown || calls[0].reason != "unknown" || calls[0].to != "gone" {
		t.Fatalf("unknown_user drop: calls = %+v", calls)
	}

	calls = nil
	error_test_queue_insert(db, "q2", "owner", "feeds", "peerX", "feeds", "post/react", now())
	queue_drop("q2", fail_unsupported)
	if len(calls) != 1 || calls[0].code != error_code_message_rejected || calls[0].reason != "unsupported" {
		t.Fatalf("unsupported drop: calls = %+v", calls)
	}

	calls = nil
	error_test_queue_insert(db, "q3", "owner", "feeds", "peerY", "feeds", "x", now())
	queue_drop("q3", fail_dedup)
	if len(calls) != 0 {
		t.Fatalf("dedup drop should dispatch nothing: calls = %+v", calls)
	}

	calls = nil
	queue_drop("does-not-exist", fail_unknown_user)
	if len(calls) != 0 {
		t.Fatalf("missing row should dispatch nothing: calls = %+v", calls)
	}
}

// TestQueueFailTimeout: queue_fail fires message/timeout only once the row
// has aged past queue_max_age; a young row reschedules with no dispatch.
func TestQueueFailTimeout(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db := queue_test_table()

	var calls []error_call
	defer error_test_capture(&calls)()

	error_test_queue_insert(db, "old", "owner", "feeds", "gone", "feeds", "post/create", now()-queue_max_age-100)
	queue_fail("old", "peer offline")
	if len(calls) != 1 || calls[0].code != error_code_message_timeout || calls[0].to != "gone" {
		t.Fatalf("aged queue_fail: calls = %+v", calls)
	}
	if db.integer("select count(*) from queue where id = 'old'") != 0 {
		t.Error("aged row not deleted")
	}

	calls = nil
	error_test_queue_insert(db, "young", "owner", "feeds", "peerZ", "feeds", "x", now())
	queue_fail("young", "peer offline")
	if len(calls) != 0 {
		t.Fatalf("young queue_fail should not dispatch: calls = %+v", calls)
	}
	if db.integer("select count(*) from queue where id = 'young'") != 1 {
		t.Error("young row should remain for retry")
	}
}

// TestQueueCleanupTimeoutDedup: the sweep fires message/timeout once per
// distinct (from_entity, from_app, to_entity) — fan-out makes many rows
// per recipient — and never touches a fresh row.
func TestQueueCleanupTimeoutDedup(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db := queue_test_table()

	seen := map[string]int{}
	orig := queue_error_dispatch
	defer func() { queue_error_dispatch = orig }()
	queue_error_dispatch = func(q *QueueEntry, code, reason string) {
		if code != error_code_message_timeout {
			t.Errorf("cleanup dispatched %q, want message/timeout", code)
		}
		seen[q.ToEntity]++
	}

	old := now() - queue_max_age - 100
	error_test_queue_insert(db, "a1", "owner", "feeds", "gone", "feeds", "post/create", old)
	error_test_queue_insert(db, "a2", "owner", "feeds", "gone", "feeds", "post/create", old)
	error_test_queue_insert(db, "a3", "owner", "feeds", "gone", "feeds", "post/create", old)
	error_test_queue_insert(db, "b1", "owner", "feeds", "other", "feeds", "post/create", old)
	error_test_queue_insert(db, "fresh", "owner", "feeds", "live", "feeds", "post/create", now())

	queue_cleanup()

	if seen["gone"] != 1 {
		t.Errorf("recipient 'gone' dispatched %d times, want 1 (deduped per sweep)", seen["gone"])
	}
	if seen["other"] != 1 {
		t.Errorf("recipient 'other' dispatched %d times, want 1", seen["other"])
	}
	if seen["live"] != 0 {
		t.Errorf("fresh-row recipient dispatched %d times, want 0", seen["live"])
	}
	if db.integer("select count(*) from queue where id = 'fresh'") != 1 {
		t.Error("fresh row should survive cleanup")
	}
}

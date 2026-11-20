// Mochi server: error-event wiring at the queue drop/expire sites
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"sync"
	"testing"
	"time"
)

// error_test_queue_insert adds a queue row with full control over the
// fields the error wiring reads (provenance + created age).
func error_test_queue_insert(db *DB, id, from_entity, from_app, to_entity, service, event string, created int64) {
	db.exec(`insert into queue (id, target, from_entity, to_entity, service, event, from_app, next_retry, created)
		values (?, '', ?, ?, ?, ?, ?, 0, ?)`,
		id, from_entity, to_entity, service, event, from_app, created)
}

// error_test_capture stubs queue_error_dispatch and returns the captured
// calls plus a restore func. The calls arrive on the queue_errors pool; read
// them after queue_errors_wait.
type error_call struct{ id, to, code, reason string }

func error_test_capture(calls *[]error_call) func() {
	orig := queue_error_dispatch
	var lock sync.Mutex
	queue_error_dispatch = func(q *QueueEntry, code, reason string) {
		lock.Lock()
		*calls = append(*calls, error_call{q.ID, q.ToEntity, code, reason})
		lock.Unlock()
	}
	return func() {
		queue_errors_wait()
		queue_error_dispatch = orig
	}
}

// queue_errors_wait returns once the queue_errors pool has delivered every
// report handed to it.
func queue_errors_wait() {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		queue_errors_lock.Lock()
		idle := len(queue_errors_waiting) == 0 && queue_errors_running == 0
		queue_errors_lock.Unlock()
		if idle {
			return
		}
		time.Sleep(time.Millisecond)
	}
}

// TestQueueDropDoesNotWaitOnTheHandler: the app's error handler may run for the
// Starlark timeout, and queue_drop is reached on a Sender's read loop, whose
// acks and pings wait behind it - its other messages then time out and a
// healthy stream is torn down. The report goes to the queue_errors pool.
func TestQueueDropDoesNotWaitOnTheHandler(t *testing.T) {
	setup_replication_test(t)
	db := queue_test_table()

	gate := make(chan struct{})
	started := make(chan struct{}, 8)
	orig := queue_error_dispatch
	queue_error_dispatch = func(q *QueueEntry, code, reason string) {
		started <- struct{}{}
		<-gate
	}
	t.Cleanup(func() {
		close(gate)
		queue_errors_wait()
		queue_error_dispatch = orig
	})

	for i := 0; i < 2*queue_errors_concurrency; i++ {
		id := "slow-" + i64toa(int64(i))
		error_test_queue_insert(db, id, "owner", "feeds", "gone", "feeds", "post/create", now())
		returned := make(chan struct{})
		go func() {
			queue_drop(id, fail_unsupported)
			close(returned)
		}()
		select {
		case <-returned:
		case <-time.After(2 * time.Second):
			t.Fatalf("queue_drop %d waited on the app's error handler", i)
		}
	}
	// A burst runs a few handlers at a time, not one goroutine each.
	time.Sleep(100 * time.Millisecond)
	if running := len(started); running != queue_errors_concurrency {
		t.Errorf("%d handlers running for %d drops, want the pool's %d", running, 2*queue_errors_concurrency, queue_errors_concurrency)
	}
}

// TestQueueDropFiresErrorEvent: a NACK drop maps the reason to the right
// code (unknown/rejected), dedup/unmapped reasons fire nothing, and a
// missing row is a safe no-op.
func TestQueueDropFiresErrorEvent(t *testing.T) {
	setup_replication_test(t)
	db := queue_test_table()

	var calls []error_call
	defer error_test_capture(&calls)()

	error_test_queue_insert(db, "q1", "owner", "feeds", "gone", "feeds", "post/create", now())
	queue_drop("q1", fail_unknown_user)
	queue_errors_wait()
	if len(calls) != 1 || calls[0].code != error_code_message_unknown || calls[0].reason != "unknown" || calls[0].to != "gone" {
		t.Fatalf("unknown_user drop: calls = %+v", calls)
	}

	calls = nil
	error_test_queue_insert(db, "q2", "owner", "feeds", "peerX", "feeds", "post/react", now())
	queue_drop("q2", fail_unsupported)
	queue_errors_wait()
	if len(calls) != 1 || calls[0].code != error_code_message_rejected || calls[0].reason != "unsupported" {
		t.Fatalf("unsupported drop: calls = %+v", calls)
	}

	calls = nil
	error_test_queue_insert(db, "q3", "owner", "feeds", "peerY", "feeds", "x", now())
	queue_drop("q3", "")
	queue_errors_wait()
	if len(calls) != 0 {
		t.Fatalf("a drop with no reason should dispatch nothing: calls = %+v", calls)
	}

	calls = nil
	queue_drop("does-not-exist", fail_unknown_user)
	queue_errors_wait()
	if len(calls) != 0 {
		t.Fatalf("missing row should dispatch nothing: calls = %+v", calls)
	}
}

// TestQueueFailTimeout: queue_fail fires message/timeout only once the row
// has aged past queue_age_maximum; a young row reschedules with no dispatch.
func TestQueueFailTimeout(t *testing.T) {
	setup_replication_test(t)
	db := queue_test_table()

	var calls []error_call
	defer error_test_capture(&calls)()

	error_test_queue_insert(db, "old", "owner", "feeds", "gone", "feeds", "post/create", now()-queue_age_maximum-100)
	queue_fail("old", "peer offline")
	queue_errors_wait()
	if len(calls) != 1 || calls[0].code != error_code_message_timeout || calls[0].to != "gone" {
		t.Fatalf("aged queue_fail: calls = %+v", calls)
	}
	if db.integer("select count(*) from queue where id = 'old'") != 0 {
		t.Error("aged row not deleted")
	}

	calls = nil
	error_test_queue_insert(db, "young", "owner", "feeds", "peerZ", "feeds", "x", now())
	queue_fail("young", "peer offline")
	queue_errors_wait()
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
	setup_replication_test(t)
	db := queue_test_table()

	seen := map[string]int{}
	var lock sync.Mutex
	orig := queue_error_dispatch
	defer func() { queue_error_dispatch = orig }()
	queue_error_dispatch = func(q *QueueEntry, code, reason string) {
		if code != error_code_message_timeout {
			t.Errorf("cleanup dispatched %q, want message/timeout", code)
		}
		lock.Lock()
		seen[q.ToEntity]++
		lock.Unlock()
	}

	old := now() - queue_age_maximum - 100
	error_test_queue_insert(db, "a1", "owner", "feeds", "gone", "feeds", "post/create", old)
	error_test_queue_insert(db, "a2", "owner", "feeds", "gone", "feeds", "post/create", old)
	error_test_queue_insert(db, "a3", "owner", "feeds", "gone", "feeds", "post/create", old)
	error_test_queue_insert(db, "b1", "owner", "feeds", "other", "feeds", "post/create", old)
	error_test_queue_insert(db, "fresh", "owner", "feeds", "live", "feeds", "post/create", now())

	queue_cleanup()
	queue_errors_wait()

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

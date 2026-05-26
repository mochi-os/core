// Mochi server: outbound queue unit tests
// Copyright Alistair Cunningham 2026

package main

import (
	"fmt"
	"testing"
)

// TestQueuePriority covers the classifier that assigns a message to a
// priority tier from its service and event.
func TestQueuePriority(t *testing.T) {
	cases := []struct {
		service, event string
		want           int
	}{
		{"feeds", "post/new", priority_interactive},
		{"chat", "message", priority_interactive},
		{"replication", "sql/op", priority_bulk},
		{"replication", "system/set", priority_bulk},
		{"replication", "system/row", priority_bulk},
		{"replication", "link/request", priority_control},
		{"replication", "link/approved", priority_control},
		{"replication", "link/denied", priority_control},
		{"replication", "host/membership/change", priority_control},
		{"replication", "keys/transfer", priority_control},
		{"replication", "join/approved", priority_control},
		{"replication", "bootstrap/scope/done", priority_control},
		// An unclassified replication event falls back to interactive —
		// delivered promptly, and never stuck behind bulk.
		{"replication", "future/unknown", priority_interactive},
		{"", "", priority_interactive},
	}
	for _, c := range cases {
		if got := queue_priority(c.service, c.event); got != c.want {
			t.Errorf("queue_priority(%q, %q) = %d, want %d", c.service, c.event, got, c.want)
		}
	}
}

// queue_test_table returns the queue.db handle, ensuring the schema
// exists (setup_replication_test already creates it; the `if not
// exists` keeps this safe to call regardless).
func queue_test_table() *DB {
	db := db_open("db/queue.db")
	db.exec("create table if not exists queue ( id text primary key, type text not null default 'direct', target text not null, from_entity text not null, to_entity text not null, service text not null, event text not null, from_app text not null default '', from_services text not null default '', content blob not null default '', data blob not null default '', file text not null default '', expires integer not null default 0, status text not null default 'pending', attempts integer not null default 0, next_retry integer not null, last_error text not null default '', created integer not null, priority integer not null default 20 )")
	return db
}

// queue_test_insert adds a minimal due (next_retry in the past) row.
func queue_test_insert(db *DB, id string, priority int) {
	db.exec(`insert into queue (id, type, target, from_entity, to_entity, service, event, next_retry, created, priority)
		values (?, 'direct', 'peer-X', '', '', 'test', 'msg', ?, ?, ?)`,
		id, now()-1, now()-1, priority)
}

// TestQueueSelectPriorityOrder: queue_select returns due messages most-
// urgent first, so a control message is never behind bulk data.
func TestQueueSelectPriorityOrder(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	db := queue_test_table()
	queue_test_insert(db, "bulk-1", priority_bulk)
	queue_test_insert(db, "interactive-1", priority_interactive)
	queue_test_insert(db, "control-1", priority_control)

	entries := queue_select(db)
	if len(entries) != 3 {
		t.Fatalf("queue_select returned %d entries, want 3", len(entries))
	}
	if entries[0].Priority != priority_control {
		t.Errorf("first entry priority = %d, want %d (control)", entries[0].Priority, priority_control)
	}
	if entries[len(entries)-1].Priority != priority_bulk {
		t.Errorf("last entry priority = %d, want %d (bulk)", entries[len(entries)-1].Priority, priority_bulk)
	}
}

// TestQueueSelectBulkFloor: a flood of higher-priority traffic cannot
// starve the bulk tier — queue_select's reserved lane guarantees bulk
// messages a share of every batch.
func TestQueueSelectBulkFloor(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	db := queue_test_table()
	// 55 interactive messages: more than the 50-slot urgent lane.
	for i := 0; i < 55; i++ {
		queue_test_insert(db, fmt.Sprintf("interactive-%d", i), priority_interactive)
	}
	// 12 bulk messages waiting behind them.
	for i := 0; i < 12; i++ {
		queue_test_insert(db, fmt.Sprintf("bulk-%d", i), priority_bulk)
	}

	entries := queue_select(db)

	bulk := 0
	for _, e := range entries {
		if e.Priority == priority_bulk {
			bulk++
		}
	}
	if bulk != queue_bulk_floor {
		t.Errorf("bulk messages selected = %d, want %d (the reserved floor) — bulk was starved by interactive traffic", bulk, queue_bulk_floor)
	}
}

// TestQueueDeferPushesRetryWithoutBumpingAttempts confirms queue_defer
// only moves next_retry forward - attempts stays put because a deferred
// row is NOT a failed attempt. Without this, the silent-peer pre-filter
// would escalate the backoff just by skipping the row.
func TestQueueDeferPushesRetryWithoutBumpingAttempts(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db := queue_test_table()
	queue_test_insert(db, "deferme", priority_interactive)
	// Force attempts to 3 so we can prove the defer didn't touch it.
	db.exec("update queue set attempts = 3 where id = ?", "deferme")
	queue_defer("deferme", 600)
	row, _ := db.row("select attempts, next_retry from queue where id = ?", "deferme")
	if row == nil {
		t.Fatal("row vanished")
	}
	if a, _ := row["attempts"].(int64); a != 3 {
		t.Errorf("attempts: got %d, want 3 (defer must not bump attempts)", a)
	}
	if nr, _ := row["next_retry"].(int64); nr < now()+599 {
		t.Errorf("next_retry: got %d, want >= now+599 (defer must move retry forward)", nr-now())
	}
}

// TestQueueResurrectPeerPullsDeferredRowsForward confirms the per-peer
// resurrection covers ALL pending rows for that target whose next_retry
// is in the future. Load-bearing for "silenced peer comes back" - the
// deferred rows need to drain immediately, not wait out the deferral.
func TestQueueResurrectPeerPullsDeferredRowsForward(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db := queue_test_table()

	// Two rows for the target peer (one deferred to the future, one
	// already due) plus one for a different peer.
	db.exec(`insert into queue (id, type, target, from_entity, to_entity, service, event, next_retry, created, priority)
		values (?, 'direct', 'peer-silent', '', '', 't', 'm', ?, ?, ?)`,
		"deferred-future", now()+3600, now()-100, priority_interactive)
	db.exec(`insert into queue (id, type, target, from_entity, to_entity, service, event, next_retry, created, priority)
		values (?, 'direct', 'peer-silent', '', '', 't', 'm', ?, ?, ?)`,
		"already-due", now()-1, now()-100, priority_interactive)
	db.exec(`insert into queue (id, type, target, from_entity, to_entity, service, event, next_retry, created, priority)
		values (?, 'direct', 'peer-other', '', '', 't', 'm', ?, ?, ?)`,
		"other-peer-future", now()+3600, now()-100, priority_interactive)

	queue_resurrect_peer("peer-silent")

	// Both peer-silent rows now have next_retry <= now.
	for _, id := range []string{"deferred-future", "already-due"} {
		row, _ := db.row("select next_retry from queue where id = ?", id)
		if row == nil {
			t.Fatalf("%s row vanished", id)
		}
		if nr, _ := row["next_retry"].(int64); nr > now() {
			t.Errorf("%s next_retry=%d (>%d=now); resurrect must pull it back", id, nr, now())
		}
	}
	// Other peer's row is untouched.
	row, _ := db.row("select next_retry from queue where id = ?", "other-peer-future")
	if nr, _ := row["next_retry"].(int64); nr <= now() {
		t.Errorf("other-peer-future next_retry=%d; resurrect must not touch other peers", nr)
	}
}

// TestQueueProcessReturnsCount confirms queue_process returns the
// number of rows acted on, so queue_manager's drain-loop can decide
// whether to re-enter immediately or sleep on the heartbeat. Without
// this signal the manager would have to time-poll or guess.
func TestQueueProcessReturnsCount(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	queue_test_table()

	// Empty queue: zero rows acted on.
	if n := queue_process(); n != 0 {
		t.Errorf("empty queue: got %d, want 0", n)
	}

	// Three expired rows: pre-filter drops them, count returns 3.
	db := db_open("db/queue.db")
	for i := 0; i < 3; i++ {
		db.exec(`insert into queue (id, type, target, from_entity, to_entity, service, event, expires, next_retry, created, priority)
			values (?, 'direct', 'p', '', '', 't', 'm', ?, ?, ?, ?)`,
			fmt.Sprintf("expired-%d", i), now()-10, now()-1, now()-100, priority_interactive)
	}
	if n := queue_process(); n != 3 {
		t.Errorf("3 expired rows: got %d, want 3", n)
	}
}

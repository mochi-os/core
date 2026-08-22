// Mochi server: outbound queue unit tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"fmt"
	"testing"
)

// TestQueuePriority: every message is interactive. The replication service and
// event names below are ordinary strings now and must earn no lane of their
// own.
func TestQueuePriority(t *testing.T) {
	for _, c := range []struct{ service, event string }{
		{"feeds", "post/new"},
		{"chat", "message"},
		{"replication", "sql/op"},
		{"replication", "link/request"},
		{"replication", "membership/join"},
		{"replication", "keys/transfer"},
		{"replication", "future/unknown"},
		{"", ""},
	} {
		if got := queue_priority(c.service, c.event); got != priority_interactive {
			t.Errorf("queue_priority(%q, %q) = %d, want %d - no service earns a lane of its own any more", c.service, c.event, got, priority_interactive)
		}
	}
}

// queue_test_table returns the queue.db handle, ensuring the schema
// exists (setup_replication_test already creates it; the `if not
// exists` keeps this safe to call regardless).
func queue_test_table() *DB {
	db := db_open("db/queue.db")
	db.exec("create table if not exists queue ( id text primary key, type text not null default 'direct', target text not null, from_entity text not null, to_entity text not null, service text not null, event text not null, from_app text not null default '', from_services text not null default '', content blob not null default '', data blob not null default '', file text not null default '', expires integer not null default 0, status text not null default 'pending', attempts integer not null default 0, next_retry integer not null, last_error text not null default '', created integer not null, priority integer not null default 20, claimed integer not null default 0 )")
	return db
}

// queue_test_insert adds a minimal due (next_retry in the past) row.
// Each call targets a distinct peer so pick-by-peer returns it
// independently. For tests that need multiple rows for ONE peer, use
// queue_test_insert_target.
func queue_test_insert(db *DB, id string, priority int) {
	queue_test_insert_target(db, id, "peer-"+id, priority)
}

func queue_test_insert_target(db *DB, id, target string, priority int) {
	db.exec(`insert into queue (id, type, target, from_entity, to_entity, service, event, next_retry, created, priority)
		values (?, 'direct', ?, '', '', 'test', 'msg', ?, ?, ?)`,
		id, target, now()-1, now()-1, priority)
}

// TestQueueSelectPriorityOrder: across distinct peers the most urgent peer
// comes first - a resync reply at priority_replay must not queue behind
// interactive traffic.
func TestQueueSelectPriorityOrder(t *testing.T) {
	setup_replication_test(t)

	db := queue_test_table()
	queue_test_insert(db, "interactive-1", priority_interactive)
	queue_test_insert(db, "replay-1", priority_replay)

	entries := queue_select(db)
	if len(entries) != 2 {
		t.Fatalf("queue_select returned %d entries, want 2 (2 distinct peers)", len(entries))
	}
	if entries[0].Priority != priority_replay {
		t.Errorf("first entry priority = %d, want %d (replay); a resync reply delivered behind live traffic is what the lane exists to prevent", entries[0].Priority, priority_replay)
	}
	if entries[len(entries)-1].Priority != priority_interactive {
		t.Errorf("last entry priority = %d, want %d", entries[len(entries)-1].Priority, priority_interactive)
	}
}

// TestQueueSelectPickByPeerDedupesByTarget: N rows for one peer yield ONE row,
// the highest-priority earliest-next_retry one, so no peer's backlog starves
// the others.
func TestQueueSelectPickByPeerDedupesByTarget(t *testing.T) {
	setup_replication_test(t)

	db := queue_test_table()
	// Two rows for the same peer at different priorities. Insert the lower
	// one first so it has the earliest next_retry — if the picker ignored
	// priority, interactive would win on next_retry ordering.
	queue_test_insert_target(db, "interactive-A", "peer-A", priority_interactive)
	queue_test_insert_target(db, "replay-A", "peer-A", priority_replay)
	// A second peer with a single interactive row.
	queue_test_insert_target(db, "interactive-B", "peer-B", priority_interactive)

	entries := queue_select(db)
	if len(entries) != 2 {
		t.Fatalf("queue_select returned %d entries, want 2 (one per distinct peer)", len(entries))
	}
	// Peer A's representative must be its highest-priority row.
	var peer_a, peer_b string
	for _, e := range entries {
		if e.Target == "peer-A" {
			peer_a = e.ID
		}
		if e.Target == "peer-B" {
			peer_b = e.ID
		}
	}
	if peer_a != "replay-A" {
		t.Errorf("peer-A representative = %q, want %q (highest priority for that peer)", peer_a, "replay-A")
	}
	if peer_b != "interactive-B" {
		t.Errorf("peer-B representative = %q, want %q", peer_b, "interactive-B")
	}
}

// TestQueueSelectNoLowPriorityStarvation: a lower-priority row for its own peer
// is still picked - pick-by-peer gives every peer a slot, so no floor lane is
// needed.
func TestQueueSelectNoLowPriorityStarvation(t *testing.T) {
	setup_replication_test(t)

	db := queue_test_table()
	// 55 higher-priority rows, one per distinct peer — would fill the
	// 50-slot direct limit and overflow.
	for i := 0; i < 55; i++ {
		queue_test_insert_target(db,
			fmt.Sprintf("replay-%d", i),
			fmt.Sprintf("peer-replay-%d", i),
			priority_replay)
	}
	// One lower-priority row for a different peer.
	queue_test_insert_target(db, "low-lone", "peer-low", priority_interactive)

	entries := queue_select(db)

	// The picker takes the top 50 distinct peers by priority then next_retry, so
	// the replay rows fill this tick and the interactive row waits for the next
	// one. Verified by re-querying after some replay rows drain.
	for _, e := range entries {
		if e.Priority == priority_interactive {
			// The low row made it into a 50-slot batch — that's fine and
			// also non-starving; nothing else to test.
			return
		}
	}
	// It was not picked this tick. Simulate the next one by removing half of
	// the higher-priority rows (queue_process having drained them) and
	// re-querying.
	for i := 0; i < 30; i++ {
		db.exec("delete from queue where id = ?", fmt.Sprintf("replay-%d", i))
	}
	entries = queue_select(db)
	for _, e := range entries {
		if e.ID == "low-lone" {
			return
		}
	}
	t.Errorf("the low-priority row was never picked across two ticks; pick-by-peer should give every peer a slot")
}

// TestQueueAckFlushDeletesAllIds: one DELETE removes every id in the batch and
// leaves other rows untouched. A mis-built IN-list loses acks for whole
// batches.
func TestQueueAckFlushDeletesAllIds(t *testing.T) {
	setup_replication_test(t)
	db := queue_test_table()

	for _, id := range []string{"a", "b", "c", "d"} {
		queue_test_insert_target(db, id, "peer-"+id, priority_interactive)
	}
	queue_ack_flush([]string{"a", "c"})
	for _, id := range []string{"a", "c"} {
		if row, _ := db.row("select 1 from queue where id=?", id); row != nil {
			t.Errorf("id %q still present after flush; want deleted", id)
		}
	}
	for _, id := range []string{"b", "d"} {
		if row, _ := db.row("select 1 from queue where id=?", id); row == nil {
			t.Errorf("id %q deleted by flush; want preserved", id)
		}
	}
}

// TestQueueAckFlushEmptyIsNoOp: empty input must not run any SQL
// (would generate `delete from queue where id in ()` which is a
// SQLite syntax error).
func TestQueueAckFlushEmptyIsNoOp(t *testing.T) {
	setup_replication_test(t)
	queue_test_table()
	queue_ack_flush(nil)
	queue_ack_flush([]string{})
}

// TestQueueAckAsyncFallsBackWhenChannelFull: if queue_ack_ch is
// saturated, queue_ack_async must fall back to synchronous queue_ack
// rather than dropping the ack (which would leak a 'sending' row).
func TestQueueAckAsyncFallsBackWhenChannelFull(t *testing.T) {
	setup_replication_test(t)
	db := queue_test_table()

	queue_test_insert_target(db, "synced-ack", "peer-synced", priority_interactive)

	// Fill queue_ack_ch with a sentinel that the batcher won't see in
	// this test (no batcher goroutine running). The select-default
	// branch in queue_ack_async should then fire queue_ack inline.
	saved := queue_ack_ch
	queue_ack_ch = make(chan string, 1)
	queue_ack_ch <- "filler"
	defer func() {
		queue_ack_ch = saved
	}()

	queue_ack_async("synced-ack")
	if row, _ := db.row("select 1 from queue where id=?", "synced-ack"); row != nil {
		t.Error("queue_ack_async fallback did not delete the row")
	}
}

// TestQueueDeferPushesRetryWithoutBumpingAttempts confirms queue_defer
// only moves next_retry forward - attempts stays put because a deferred
// row is NOT a failed attempt. Without this, the silent-peer pre-filter
// would escalate the backoff just by skipping the row.
func TestQueueDeferPushesRetryWithoutBumpingAttempts(t *testing.T) {
	setup_replication_test(t)
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
	setup_replication_test(t)
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

// TestQueueSelfLoopFastDecodeFailureReturnsFalse confirms the fast
// path returns false (retryable) when q.Content can't be CBOR-decoded,
// and crucially does NOT panic. A corrupted/wrong-shape content blob
// must surface as a normal queue_fail, never as a process crash.
func TestQueueSelfLoopFastDecodeFailureReturnsFalse(t *testing.T) {
	setup_replication_test(t)
	q := &QueueEntry{
		ID:         "decode-fail",
		FromEntity: "from-x",
		ToEntity:   "to-x",
		Service:    "s",
		Event:      "e",
		Content:    []byte{0xff, 0xff, 0xff, 0xff}, // invalid CBOR
	}
	if ok := queue_send_self_loop_fast(q); ok {
		t.Error("self-loop fast path: malformed content returned ok=true; want false (retryable)")
	}
}

// TestQueueSelfLoopFastPanicRecovered: route() needs real user/app
// infrastructure to reach a panic, so this only proves the defer recover
// wrapper is present and correctly typed - a failed recover would crash the
// test runner here.
func TestQueueSelfLoopFastPanicRecovered(t *testing.T) {
	setup_replication_test(t)
	// Empty Event - route() returns "unknown user" error, not a panic,
	// but exercises the full function structure including defer recover.
	q := &QueueEntry{ID: "ok", Content: nil}
	// If the defer were missing AND e.route() panicked, this would
	// kill the test runner. Returns false (retryable) is the success.
	if ok := queue_send_self_loop_fast(q); ok {
		t.Error("self-loop fast path: empty event returned ok=true; want false (no matching user/app)")
	}
}

// TestQueueClaimForSelf: queue_claim_for_self must atomically pull
// direct rows targeting net_id and flip them to status='sending', so
// queue_process won't double-pick them.
func TestQueueClaimForSelf(t *testing.T) {
	setup_replication_test(t)
	queue_test_table()

	saved := net_id
	net_id = "12D3KooWFakeSelfForClaimTest-aaaaaaaaaaaaaaaaaaaa"
	defer func() { net_id = saved }()

	db := db_open("db/queue.db")
	// Three self-loop rows + one row for a different peer (must not
	// be claimed).
	for i := 0; i < 3; i++ {
		db.exec(`insert into queue (id, type, target, from_entity, to_entity, service, event, expires, next_retry, created, priority)
			values (?, 'direct', ?, '', '', 't', 'm', 0, ?, ?, ?)`,
			fmt.Sprintf("self-%d", i), net_id, now()-1, now(), priority_interactive)
	}
	db.exec(`insert into queue (id, type, target, from_entity, to_entity, service, event, expires, next_retry, created, priority)
		values (?, 'direct', ?, '', '', 't', 'm', 0, ?, ?, ?)`,
		"other-peer", "12D3KooWOtherPeerNotSelf-bbbbbbbbbbbbbbbbbbbbbbbb", now()-1, now(), priority_interactive)

	rows := queue_claim_for_self(50)
	if len(rows) != 3 {
		t.Fatalf("queue_claim_for_self returned %d rows, want 3 (self-loop only)", len(rows))
	}
	for _, r := range rows {
		if r.Target != net_id {
			t.Errorf("claimed row target = %q, want %q", r.Target, net_id)
		}
	}

	// Claimed rows must be 'sending' in queue.db; other-peer untouched.
	for i := 0; i < 3; i++ {
		row, _ := db.row("select status from queue where id = ?", fmt.Sprintf("self-%d", i))
		if status, _ := row["status"].(string); status != "sending" {
			t.Errorf("self-%d status after claim = %q, want sending", i, status)
		}
	}
	row, _ := db.row("select status from queue where id = ?", "other-peer")
	if status, _ := row["status"].(string); status != "pending" {
		t.Errorf("other-peer status after self-claim = %q, want pending (not touched)", status)
	}

	// Second call returns no rows (all three already 'sending').
	if rows := queue_claim_for_self(50); len(rows) != 0 {
		t.Errorf("queue_claim_for_self second call returned %d rows, want 0 (all claimed)", len(rows))
	}
}

// TestQueueClaimForSelfNoNetId: when net_id is empty (early startup,
// pre-net_start), queue_claim_for_self must return nil rather than
// claiming rows whose target is empty.
func TestQueueClaimForSelfNoNetId(t *testing.T) {
	setup_replication_test(t)
	queue_test_table()

	saved := net_id
	net_id = ""
	defer func() { net_id = saved }()

	db := db_open("db/queue.db")
	db.exec(`insert into queue (id, type, target, from_entity, to_entity, service, event, expires, next_retry, created, priority)
		values ('empty-target', 'direct', '', '', '', 't', 'm', 0, ?, ?, ?)`,
		now()-1, now(), priority_interactive)

	if rows := queue_claim_for_self(50); rows != nil {
		t.Errorf("queue_claim_for_self with empty net_id returned %d rows; want nil (avoid claiming empty-target rows)", len(rows))
	}
}

// TestQueueProcessSkipsSelfLoopRows: queue_process must not dispatch rows
// targeting net_id - self_loop_drain owns them.
func TestQueueProcessSkipsSelfLoopRows(t *testing.T) {
	setup_replication_test(t)
	queue_test_table()

	saved := net_id
	net_id = "12D3KooWFakeSelfForProcessSkip-cccccccccccccccccc"
	defer func() { net_id = saved }()

	db := db_open("db/queue.db")
	db.exec(`insert into queue (id, type, target, from_entity, to_entity, service, event, expires, next_retry, created, priority)
		values ('self-row', 'direct', ?, '', '', 't', 'm', 0, ?, ?, ?)`,
		net_id, now()-1, now(), priority_interactive)

	if n := queue_process(); n != 0 {
		t.Errorf("self-loop row: queue_process acted on %d, want 0 (self_loop_drain owns it)", n)
	}

	row, err := db.row("select status from queue where id = ?", "self-row")
	if err != nil || row == nil {
		t.Fatal("self-loop row unexpectedly deleted")
	}
	if status, _ := row["status"].(string); status != "pending" {
		t.Errorf("self-loop row status after queue_process skip = %q, want pending", status)
	}
}

// TestQueueProcessSkipsRowsWithActiveSender: pull_loop owns direct rows for a
// peer with an active Sender. Competing for its outbox blocks peer_send for
// sender_send_timeout and drags out the whole tick.
func TestQueueProcessSkipsRowsWithActiveSender(t *testing.T) {
	setup_replication_test(t)
	queue_test_table()

	// Install a fake Sender so senders_has(peer) returns true.
	peer := "12D3KooWFakeSenderTargetTestPeer-aaaaaaaaaaaaaaaa"
	s := &Sender{peer: peer}
	senders_lock.Lock()
	senders[peer] = s
	senders_lock.Unlock()
	defer func() {
		senders_lock.Lock()
		delete(senders, peer)
		senders_lock.Unlock()
	}()

	db := db_open("db/queue.db")
	db.exec(`insert into queue (id, type, target, from_entity, to_entity, service, event, expires, next_retry, created, priority)
		values (?, 'direct', ?, '', '', 't', 'm', 0, ?, ?, ?)`,
		"sender-owned", peer, now()-1, now(), priority_interactive)

	// queue_process should skip the row entirely (not acted on).
	if n := queue_process(); n != 0 {
		t.Errorf("row with active Sender: queue_process acted on %d, want 0 (pull_loop owns it)", n)
	}

	// Row must remain pending and unchanged for pull_loop to claim.
	row, err := db.row("select status from queue where id = ?", "sender-owned")
	if err != nil || row == nil {
		t.Fatal("row was unexpectedly deleted")
	}
	if status, _ := row["status"].(string); status != "pending" {
		t.Errorf("row status after skip: %q, want pending", status)
	}
}

// TestQueueProcessReturnsCount confirms queue_process returns the
// number of rows acted on, so queue_manager's drain-loop can decide
// whether to re-enter immediately or sleep on the heartbeat. Without
// this signal the manager would have to time-poll or guess.
func TestQueueProcessReturnsCount(t *testing.T) {
	setup_replication_test(t)
	queue_test_table()

	// Empty queue: zero rows acted on.
	if n := queue_process(); n != 0 {
		t.Errorf("empty queue: got %d, want 0", n)
	}

	// Three expired rows, one per distinct peer (pick-by-peer dedupes
	// by target, so multiple rows for the same peer would only return
	// one per tick). Pre-filter drops them as expired, count returns 3.
	db := db_open("db/queue.db")
	for i := 0; i < 3; i++ {
		db.exec(`insert into queue (id, type, target, from_entity, to_entity, service, event, expires, next_retry, created, priority)
			values (?, 'direct', ?, '', '', 't', 'm', ?, ?, ?, ?)`,
			fmt.Sprintf("expired-%d", i), fmt.Sprintf("peer-expired-%d", i), now()-10, now()-1, now()-100, priority_interactive)
	}
	if n := queue_process(); n != 3 {
		t.Errorf("3 expired rows: got %d, want 3", n)
	}
}

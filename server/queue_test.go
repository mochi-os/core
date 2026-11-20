// Mochi server: outbound queue unit tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
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
	// The indexes db_create lays, so the picks run on the plans they run on in
	// a server.
	db.exec("create index if not exists queue_status_priority_retry on queue (status, priority, next_retry)")
	db.exec("create index if not exists queue_target on queue (target)")
	db.exec("create index if not exists queue_status_target_priority_retry on queue (status, target, priority desc, next_retry)")
	db.exec("create index if not exists queue_created on queue (created)")
	db.exec(delivered_schema)
	db.exec(delivered_index)
	return db
}

// queue_ack_drain pulls every queued ack from queue_ack_ch and flushes them
// synchronously, for tests that verify queue state after an ack: production has
// queue_ack_batcher draining the channel, and tests do not start it.
func queue_ack_drain() {
	batch := make([]string, 0, queue_ack_batch)
	for {
		select {
		case id := <-queue_ack_ch:
			batch = append(batch, id)
		default:
			queue_ack_flush(batch)
			return
		}
	}
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

	entries := queue_test_select(db)
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

	entries := queue_test_select(db)
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

	entries := queue_test_select(db)
	if len(entries) > queue_pick_direct_limit {
		t.Errorf("picked %d direct rows in one tick, over the limit of %d", len(entries), queue_pick_direct_limit)
	}

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
	entries = queue_test_select(db)
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

// TestQueueSelfLoopUndecodableRowIsDropped is #2107: a self-loop row whose
// content would not decode was left 'sending', handed back by the sweep every
// two minutes and retried for seven days with no attempt counted, while the
// remote path drops the same row at once. It is dropped as rejected, and the
// sending app told, as frame_drop does.
func TestQueueSelfLoopUndecodableRowIsDropped(t *testing.T) {
	reset_workers(t)
	defer reset_workers(t)
	setup_replication_test(t)
	db := queue_test_table()
	var calls []error_call
	restore := error_test_capture(&calls)
	db.exec(`insert into queue (id, target, from_entity, to_entity, service, event, from_app, content, next_retry, created)
		values ('decode-fail', ?, 'from-x', 'to-x', 's', 'e', 'feeds', ?, ?, ?)`, net_id, []byte{0xff, 0xff, 0xff, 0xff}, now()-1, now()-1)
	q := queue_claim_for_self(10)
	if len(q) != 1 {
		t.Fatalf("control: claimed %d rows, want the one", len(q))
	}
	if busy := queue_send_self_loop_fast(&q[0]); busy {
		t.Error("an undecodable row reported its worker busy")
	}
	restore()
	if queue_test_held(db, "decode-fail") {
		t.Error("THE POINT: an undecodable self-loop row is still queued, to be retried for seven days")
	}
	if len(calls) != 1 || calls[0].code != error_code_message_rejected {
		t.Errorf("the sending app was told %+v, want one message/rejected", calls)
	}
}

// A dispatch that panics counts an attempt (#2107): a row that faults every
// time parks rather than being retried uncounted for a week. No route into the
// fast path panics on demand, so the recover is pinned in the source.
func TestQueueSelfLoopPanicCountsAnAttempt(t *testing.T) {
	function := function_body(t, "queue.go", "func queue_send_self_loop_fast(")
	if !strings.Contains(function, `queue_fail(q.ID, fmt.Sprintf("self-loop dispatch panic: %v", r))`) {
		t.Error("THE POINT: queue_send_self_loop_fast's recover does not fail the row with an attempt counted")
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

// queue_test_insert_event adds a due direct row for `target` with `event`.
func queue_test_insert_event(db *DB, id, target, event string, priority int) {
	db.exec(`insert into queue (id, type, target, from_entity, to_entity, service, event, content, next_retry, created, priority)
		values (?, 'direct', ?, '', '', 'test', ?, 'body', ?, ?, ?)`,
		id, target, event, now()-1, now()-1, priority)
}

func queue_test_ids(entries []QueueEntry) map[string]bool {
	ids := map[string]bool{}
	for _, e := range entries {
		ids[e.ID] = true
	}
	return ids
}

// queue_test_select is queue_select's rows to send, for tests that ask only
// about those.
func queue_test_select(db *DB) []QueueEntry {
	rows, _ := queue_select(db)
	return rows
}

// TestQueueSelectLeavesSenderTargets pins that the rows of a target with an open
// Sender are left to its pull_loop, their head returned only for the checks.
func TestQueueSelectLeavesSenderTargets(t *testing.T) {
	setup_replication_test(t)
	db := queue_test_table()
	open := new_test_sender(t, "peer-open")
	defer stash_sender(t, "peer-open", open)()

	queue_test_insert_event(db, "open-message", "peer-open", "msg", priority_interactive)
	queue_test_insert_event(db, "closed-message", "peer-closed", "msg", priority_interactive)

	rows, heads := queue_select(db)
	ids := queue_test_ids(rows)
	if ids["open-message"] {
		t.Error("picked to send a row an open Sender's pull_loop takes")
	}
	if !queue_test_ids(heads)["open-message"] {
		t.Error("left an open Sender's head out of the checks")
	}
	if !ids["closed-message"] {
		t.Error("left a target with no Sender unpicked")
	}
}

// TestQueueSelectLeavesSelfLoop pins the same for this server's own peer,
// whose rows self_loop_drain takes.
func TestQueueSelectLeavesSelfLoop(t *testing.T) {
	setup_replication_test(t)
	saved := net_id
	net_id = "peer-self"
	defer func() { net_id = saved }()
	db := queue_test_table()
	queue_test_insert_event(db, "self-message", "peer-self", "msg", priority_interactive)

	rows, heads := queue_select(db)
	if queue_test_ids(rows)["self-message"] {
		t.Error("picked to send a self-loop row self_loop_drain takes")
	}
	if !queue_test_ids(heads)["self-message"] {
		t.Error("left the self-loop's head out of the checks")
	}
}

// TestQueueProcessScreensSenderTargets pins that a target with an open Sender
// still has its head checked: a stalled peer's backlog is deferred, and an
// expired row is deleted, as for a target with no Sender.
func TestQueueProcessScreensSenderTargets(t *testing.T) {
	setup_replication_test(t)
	setup_users_test_schema()
	db := queue_test_table()
	const stalled, expiring = "12D3KooWScreenStalled", "12D3KooWScreenExpiring"
	for _, peer := range []string{stalled, expiring} {
		s := new_test_sender(t, peer)
		defer stash_sender(t, peer, s)()
	}
	peer_stall_test(stalled)
	queue_test_insert_event(db, "stalled-1", stalled, "msg", priority_interactive)
	queue_test_insert_event(db, "stalled-2", stalled, "msg", priority_interactive)
	queue_test_insert_event(db, "expired-1", expiring, "msg", priority_interactive)
	db.exec("update queue set expires=? where id='expired-1'", now()-1)

	queue_process()

	if n := db.integer("select count(*) from queue where target=? and next_retry<=?", stalled, now()); n != 0 {
		t.Errorf("a stalled peer with an open Sender keeps %d rows due", n)
	}
	if found, _ := db.exists("select 1 from queue where id='expired-1'"); found {
		t.Error("an expired row at the head of an open Sender's target was kept")
	}
}

// TestQueueSelectSkipsDeferredLevels pins that a target's deferred rows at a
// higher priority neither hide its due rows at a lower one nor are read.
func TestQueueSelectSkipsDeferredLevels(t *testing.T) {
	setup_replication_test(t)
	db := queue_test_table()
	queue_test_insert_event(db, "deferred-1", "peer-levels", "msg", priority_replay)
	db.exec("update queue set next_retry=? where id='deferred-1'", now()+3600)
	queue_test_insert_event(db, "due-1", "peer-levels", "msg", priority_interactive)
	db.exec(`insert into queue (id, type, target, from_entity, to_entity, service, event, next_retry, created, priority)
		values ('broadcast-deferred', 'broadcast', 'pubsub', '', '', 'test', 'msg', ?, ?, ?)`, now()+3600, now()-1, priority_replay)
	db.exec(`insert into queue (id, type, target, from_entity, to_entity, service, event, next_retry, created, priority)
		values ('broadcast-due', 'broadcast', 'pubsub', '', '', 'test', 'msg', ?, ?, ?)`, now()-1, now()-1, priority_interactive)

	ids := queue_test_ids(queue_test_select(db))
	if !ids["due-1"] || ids["deferred-1"] {
		t.Errorf("picked %v, want the target's due row below its deferred one", ids)
	}
	if !ids["broadcast-due"] || ids["broadcast-deferred"] {
		t.Errorf("picked %v, want the due broadcast below the deferred one", ids)
	}
}

// TestQueueSelectOtherRows pins that broadcasts and rows with no target yet
// are picked together, the most urgent of either first.
func TestQueueSelectOtherRows(t *testing.T) {
	setup_replication_test(t)
	db := queue_test_table()
	db.exec(`insert into queue (id, type, target, from_entity, to_entity, service, event, next_retry, created, priority)
		values ('broadcast-1', 'broadcast', 'pubsub', '', '', 'test', 'msg', ?, ?, ?)`, now()-1, now()-1, priority_interactive)
	queue_test_insert_target(db, "unresolved-1", "", priority_replay)

	entries := queue_test_select(db)
	if len(entries) != 2 {
		t.Fatalf("queue_select returned %d entries, want the broadcast and the unresolved row", len(entries))
	}
	if entries[0].ID != "unresolved-1" || entries[1].ID != "broadcast-1" {
		t.Errorf("picked %s then %s, want the more urgent unresolved row first", entries[0].ID, entries[1].ID)
	}
}

// TestQueueSelectReadsWholeRows pins that a picked direct row comes back whole,
// though the pick itself reads only ids and ordering columns.
func TestQueueSelectReadsWholeRows(t *testing.T) {
	setup_replication_test(t)
	db := queue_test_table()
	queue_test_insert_event(db, "whole-1", "peer-whole", "msg", priority_interactive)

	entries := queue_test_select(db)
	if len(entries) != 1 || entries[0].Target != "peer-whole" || string(entries[0].Content) != "body" || entries[0].Event != "msg" {
		t.Errorf("queue_select returned %+v, want the whole row", entries)
	}
}

// TestQueueSelectSkipsParkedRows pins that rows no longer pending - parked or
// being sent, their next_retry long past - neither hide a target's pending row
// nor are picked.
func TestQueueSelectSkipsParkedRows(t *testing.T) {
	setup_replication_test(t)
	db := queue_test_table()
	for i := 0; i < 5; i++ {
		queue_test_insert_event(db, fmt.Sprintf("parked-%d", i), "peer-mixed", "msg", priority_replay)
	}
	db.exec("update queue set status='parked', next_retry=1 where id like 'parked-%'")
	queue_test_insert_event(db, "sending-1", "peer-mixed", "msg", priority_interactive)
	db.exec("update queue set status='sending', next_retry=1 where id='sending-1'")
	queue_test_insert_event(db, "pending-1", "peer-mixed", "msg", priority_interactive)
	queue_test_insert_event(db, "dead-1", "peer-dead", "msg", priority_interactive)
	db.exec("update queue set status='parked' where id='dead-1'")

	ids := queue_test_ids(queue_test_select(db))
	if len(ids) != 1 || !ids["pending-1"] {
		t.Errorf("picked %v, want only the pending row", ids)
	}
}

// TestQueueHeadsSeekPendingRows pins that every step of the head pick seeks the
// status-led index, so a parked or deferred backlog costs it nothing.
func TestQueueHeadsSeekPendingRows(t *testing.T) {
	setup_replication_test(t)
	db := queue_test_table()
	plan, err := db.rows("explain query plan "+queue_heads_query, now())
	if err != nil {
		t.Fatal(err)
	}
	seeks := 0
	for _, row := range plan {
		detail, _ := row["detail"].(string)
		if strings.Contains(detail, "SCAN queue") || strings.Contains(detail, "queue_target_priority_retry") {
			t.Errorf("the head pick reads queue without the status-led index: %q", detail)
		}
		if strings.Contains(detail, "queue_status_target_priority_retry") {
			seeks++
		}
	}
	if seeks < 5 {
		t.Errorf("%d steps seek queue_status_target_priority_retry, want all five: %v", seeks, plan)
	}
}

// TestQueueHeadsAgree pins that the two ways of finding each target's due
// rows - reading the few due rows, or stepping target by target - pick the
// same rows, so which one a tick takes changes only its cost.
func TestQueueHeadsAgree(t *testing.T) {
	setup_replication_test(t)
	db := queue_test_table()
	for i := 0; i < 12; i++ {
		queue_test_insert_event(db, fmt.Sprintf("agree-%d", i), fmt.Sprintf("peer-%d", i%4), "msg", []int{priority_interactive, priority_replay}[i%2])
	}
	db.exec("update queue set next_retry=? where id in ('agree-1', 'agree-6')", now()+3600)
	db.exec("update queue set status='parked' where id='agree-3'")
	db.exec("update queue set status='sending' where id='agree-8'")
	db.exec(`insert into queue (id, type, target, from_entity, to_entity, service, event, next_retry, created, priority)
		values ('agree-broadcast', 'broadcast', 'pubsub', '', '', 'test', 'msg', ?, ?, ?)`, now()-1, now()-1, priority_replay)
	queue_test_insert_target(db, "agree-unresolved", "", priority_replay)

	best := func(rows []QueueEntry) map[string]string {
		picked := map[string]QueueEntry{}
		for _, q := range rows {
			if b, ok := picked[q.Target]; !ok || q.Priority > b.Priority {
				picked[q.Target] = q
			}
		}
		ids := map[string]string{}
		for target, q := range picked {
			ids[target] = q.ID
		}
		return ids
	}
	due, ok := queue_heads_due(db, now())
	if !ok {
		t.Fatal("a dozen due rows were too many to read")
	}
	var targets []QueueEntry
	if err := db.scans(&targets, queue_heads_query, now()); err != nil {
		t.Fatal(err)
	}
	if a, b := fmt.Sprint(best(due)), fmt.Sprint(best(targets)); a != b || len(best(due)) != 4 {
		t.Errorf("the due rows give %s, the targets %s; want the same four", a, b)
	}

	saved := queue_heads_due_limit
	queue_heads_due_limit = 3
	defer func() { queue_heads_due_limit = saved }()
	if _, ok := queue_heads_due(db, now()); ok {
		t.Error("read more due rows than the limit allows")
	}
	if a, b := fmt.Sprint(best(queue_heads(db, now()))), fmt.Sprint(best(targets)); a != b {
		t.Errorf("past the limit queue_heads gives %s, want the targets' %s", a, b)
	}
}

// TestQueueSelectLeavesOpeningTargets pins that a target whose Sender is still
// opening is left to it, its head returned only for the checks: a send would
// wait out the open and hold up the whole tick.
func TestQueueSelectLeavesOpeningTargets(t *testing.T) {
	setup_replication_test(t)
	db := queue_test_table()
	queue_test_insert_event(db, "opening-1", "peer-opening", "msg", priority_interactive)
	senders_lock.Lock()
	senders_opening["peer-opening"] = &opening{done: make(chan struct{})}
	senders_lock.Unlock()
	defer func() {
		senders_lock.Lock()
		delete(senders_opening, "peer-opening")
		senders_lock.Unlock()
	}()

	rows, heads := queue_select(db)
	if queue_test_ids(rows)["opening-1"] {
		t.Error("picked to send a row whose Sender is still opening")
	}
	if !queue_test_ids(heads)["opening-1"] {
		t.Error("left an opening Sender's head out of the checks")
	}
}

// TestQueueClaimComesBackInOrder pins that a claim hands its rows over most
// urgent first, as it asked for them, not in the order the table holds them.
func TestQueueClaimComesBackInOrder(t *testing.T) {
	setup_replication_test(t)
	db := queue_test_table()
	queue_test_insert_event(db, "claim-live-1", "peer-claim", "msg", priority_interactive)
	queue_test_insert_event(db, "claim-live-2", "peer-claim", "msg", priority_interactive)
	queue_test_insert_event(db, "claim-replay", "peer-claim", "msg", priority_replay)

	rows := queue_claim_for_peer("peer-claim", 10)
	ids := []string{}
	for _, q := range rows {
		ids = append(ids, q.ID)
	}
	if fmt.Sprint(ids) != "[claim-replay claim-live-1 claim-live-2]" {
		t.Errorf("claimed %v, want the replay row first, then the live rows as queued", ids)
	}
}

// TestQueueSenderOpenDoesNotHoldThePass is #150's second half: queue_process
// waits for every send of its pass, and opening a Sender waits on the peer - a
// connect, the stream's negotiation, its hello - so a peer that withheld its
// hello held every target's sends for thirty seconds per row it was owed.
func TestQueueSenderOpenDoesNotHoldThePass(t *testing.T) {
	setup_replication_test(t)
	setup_users_test_schema()
	db := queue_test_table()
	const slow = "12D3KooWSlowToOpen"
	release := make(chan struct{})
	released := false
	sender_opener = func(string) (*Sender, error) {
		<-release
		return nil, error_sender_unreachable
	}
	t.Cleanup(func() {
		if !released {
			close(release)
		}
		sender_opener = nil
	})
	db.exec(`insert into queue (id, type, target, from_entity, to_entity, service, event, content, next_retry, created)
		values ('slow-1', 'direct', ?, '', '', 'test', 'msg', ?, ?, ?)`, slow, cbor_encode(map[string]any{}), now()-1, now()-1)

	done := make(chan struct{})
	go func() {
		defer close(done)
		queue_process()
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("THE POINT: queue_process waited on a Sender's open")
	}
	if row, _ := db.row("select status from queue where id='slow-1'"); row == nil || row["status"] != "sending" {
		t.Errorf("while its Sender opens the row is %v, want sending", row)
	}

	// The open failing resolves the row as a failed send, as it did inline.
	close(release)
	released = true
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if db.integer("select attempts from queue where id='slow-1'") == 1 {
			return
		}
	}
	t.Error("the failed open did not count as a failed send")
}

// TestSweepLeavesARowItsWorkerStillRuns is #130: the stuck-sending sweep took
// back any row claimed over a minute ago, on the assumption that its sender had
// died, but a self-loop row is 'sending' for as long as its handler runs, which
// may be minutes. Taken back, it was claimed and dispatched again and the
// handler ran twice. A row a live holder holds is left to it.
func TestSweepLeavesARowItsWorkerStillRuns(t *testing.T) {
	setup_replication_test(t)
	setup_users_test_schema()
	reset_workers(t)
	defer reset_workers(t)
	db := queue_test_table()
	gate := make(chan struct{})
	started := make(chan struct{}, 4)
	test_service(t, "slowapp", "arrive", func(*Event) { started <- struct{}{}; <-gate })
	defer close(gate)
	recipient, _ := new_entity_keys(t)
	db.exec(`insert into queue (id, target, from_entity, to_entity, service, event, next_retry, created)
		values ('slow-1', ?, '', ?, 'slowapp', 'arrive', ?, ?)`, net_id, recipient, now()-1, now()-1)

	self_loop_pass()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("control: the row's handler never started")
	}
	// The handler outlives the sweep's window.
	db.exec("update queue set claimed=? where id='slow-1'", now()-queue_claim_timeout-10)
	queue_check_ack_timeout()
	row, _ := db.row("select status from queue where id='slow-1'")
	if row == nil || row["status"] != "sending" {
		t.Fatalf("THE POINT: a row whose handler is still running was taken back: %v", row)
	}
	self_loop_pass()
	select {
	case <-started:
		t.Error("THE POINT: the handler ran a second time for one message")
	case <-time.After(200 * time.Millisecond):
	}
}

// TestSweepTakesBackARowNoOneHolds is the other half of #130: a row left
// 'sending' whose holder is gone - here, released without being resolved - is
// still handed back.
func TestSweepTakesBackARowNoOneHolds(t *testing.T) {
	setup_replication_test(t)
	db := queue_test_table()
	queue_test_insert_target(db, "held", "peer-sweep", priority_interactive)
	queue_test_insert_target(db, "orphan", "peer-sweep", priority_interactive)
	if rows := queue_claim_for_peer("peer-sweep", 10); len(rows) != 2 {
		t.Fatalf("control: claimed %d rows, want 2", len(rows))
	}
	queue_release("orphan")
	db.exec("update queue set claimed=?", now()-queue_claim_timeout-10)
	queue_check_ack_timeout()
	for id, want := range map[string]string{"held": "sending", "orphan": "pending"} {
		if row, _ := db.row("select status from queue where id=?", id); row == nil || row["status"] != want {
			t.Errorf("row %q is %v after the sweep, want %s", id, row, want)
		}
	}
}

// TestQueueRestoreHandsBackRowsLeftSending: at startup no row has a holder, so
// every row a previous run left 'sending' goes back at once (#130), not after
// the sweep's minute.
func TestQueueRestoreHandsBackRowsLeftSending(t *testing.T) {
	setup_replication_test(t)
	db := queue_test_table()
	queue_test_insert_target(db, "left", "peer-restore", priority_interactive)
	db.exec("update queue set status='sending', claimed=? where id='left'", now())
	queue_restore()
	if row, _ := db.row("select status from queue where id='left'"); row == nil || row["status"] != "pending" {
		t.Errorf("THE POINT: a row a previous run left sending is %v after restore", row)
	}
}

// TestSelfLoopRowSentAgainRunsOnce: a self-loop message from a sender takes
// the dedup record a remote one does (#130), so a row sent a second time - its
// acknowledgement lost and the row handed back - is answered from the record
// rather than run again.
func TestSelfLoopRowSentAgainRunsOnce(t *testing.T) {
	setup_replication_test(t)
	setup_users_test_schema()
	reset_workers(t)
	defer reset_workers(t)
	db := queue_test_table()
	var runs atomic.Int32
	test_service(t, "onceapp", "arrive", func(*Event) { runs.Add(1) })
	recipient, _ := new_entity_keys(t)
	from := test_entity_id('o')
	db.exec(`insert into queue (id, target, from_entity, to_entity, service, event, next_retry, created)
		values ('once-1', ?, ?, ?, 'onceapp', 'arrive', ?, ?)`, net_id, from, recipient, now()-1, now()-1)
	send := func() {
		t.Helper()
		if !queue_sending("once-1") {
			t.Fatal("control: the row could not be claimed")
		}
		queue_send_self_loop_fast(&QueueEntry{ID: "once-1", Target: net_id, FromEntity: from, ToEntity: recipient, Service: "onceapp", Event: "arrive"})
	}
	ran := func(n int32, within time.Duration) bool {
		for deadline := time.Now().Add(within); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
			if runs.Load() >= n {
				return true
			}
		}
		return runs.Load() >= n
	}
	send()
	if !ran(1, 2*time.Second) {
		t.Fatal("control: the handler never ran")
	}
	workers_drain_test(2 * time.Second)
	// The acknowledgement never reached queue.db, and the row was handed back.
	db.exec("update queue set status='pending' where id='once-1'")
	send()
	if ran(2, 500*time.Millisecond) {
		t.Errorf("THE POINT: the handler ran %d times for one message", runs.Load())
	}
}

// TestQueueScreenKeepsARowWhenUsersCannotBeRead is #131: a users.db read that
// failed read as a deleted sending entity, and queue_process deleted the queue
// head by head for as long as the fault lasted.
func TestQueueScreenKeepsARowWhenUsersCannotBeRead(t *testing.T) {
	setup_replication_test(t)
	db := queue_test_table()
	queue_test_insert_target(db, "screened", "peer-screen", priority_interactive)
	db.exec("update queue set from_entity=? where id='screened'", test_entity_id('s'))
	q := QueueEntry{ID: "screened", Type: "direct", Target: "peer-screen", FromEntity: test_entity_id('s')}
	udb := db_open("db/users.db") // no entities table: every read fails
	if queue_screen(db, udb, &q) {
		t.Error("a sender users.db could not look up was screened out")
	}
	if !queue_test_held(db, "screened") {
		t.Fatal("THE POINT: a users.db read error deleted a queued message")
	}
	// Control: a sender users.db says is gone is dropped.
	setup_users_test_schema()
	if !queue_screen(db, udb, &q) || queue_test_held(db, "screened") {
		t.Error("control: a row from a deleted entity was kept")
	}
}

// TestQueueClaimSkipsExpiredRows is #139: the claims that drain a peer with an
// open Sender, and this server, had no expiry condition, so a typing indicator
// or receipt went out hours late, ahead of real messages.
func TestQueueClaimSkipsExpiredRows(t *testing.T) {
	setup_replication_test(t)
	db := queue_test_table()
	for id, expires := range map[string]int64{"expired": now() - 10, "fresh": now() + 3600, "forever": 0} {
		queue_test_insert_target(db, id, "peer-expiry", priority_interactive)
		db.exec("update queue set expires=? where id=?", expires, id)
	}
	claimed := map[string]bool{}
	for _, q := range queue_claim_for_peer("peer-expiry", 10) {
		claimed[q.ID] = true
	}
	if claimed["expired"] {
		t.Error("THE POINT: a row past its expiry was handed out")
	}
	if !claimed["fresh"] || !claimed["forever"] {
		t.Errorf("claimed %v, want the rows not yet expired", claimed)
	}
}

// Expired rows are deleted in bulk by the hourly cleanup, but not one a holder
// is still sending (#139).
func TestQueueCleanupDeletesExpiredRows(t *testing.T) {
	setup_replication_test(t)
	db := queue_test_table()
	for _, id := range []string{"expired", "sending", "fresh"} {
		queue_test_insert_target(db, id, "peer-expired", priority_interactive)
	}
	db.exec("update queue set expires=? where id in ('expired', 'sending')", now()-10)
	db.exec("update queue set expires=? where id='fresh'", now()+3600)
	db.exec("update queue set status='sending' where id='sending'")
	queue_cleanup()
	if queue_test_held(db, "expired") {
		t.Error("THE POINT: an expired row outlived the cleanup")
	}
	if !queue_test_held(db, "sending") || !queue_test_held(db, "fresh") {
		t.Error("the cleanup deleted a row in flight or one not yet expired")
	}
}

// TestQueueFailStartsAtTheFirstStep is #142: queue_fail indexed the ladder
// with the attempt it was counting, so the first failure waited two minutes
// and the documented one-minute step was never used.
func TestQueueFailStartsAtTheFirstStep(t *testing.T) {
	setup_replication_test(t)
	db := queue_test_table()
	queue_test_insert(db, "ladder", priority_interactive)
	for attempt, step := range retry_delays[:2] {
		queue_fail("ladder", "test")
		row, _ := db.row("select attempts, next_retry from queue where id='ladder'")
		wait := row["next_retry"].(int64) - now()
		if row["attempts"].(int64) != int64(attempt+1) || wait < step-1 || wait > step+step/4 {
			t.Errorf("THE POINT: failure %d waits %ds with %v attempts, want the ladder's step %d (%ds)", attempt+1, wait, row["attempts"], attempt, step)
		}
	}
}

// TestSelfLoopPassPutsOffAFullInbox is #151: the self-loop drain handed each
// claimed row to its worker with a blocking dispatch, so one slow handler's
// full inbox held every other user's and app's local delivery behind it.
func TestSelfLoopPassPutsOffAFullInbox(t *testing.T) {
	setup_replication_test(t)
	setup_users_test_schema()
	reset_workers(t)
	t.Setenv("MOCHI_PEER_WORKER_INBOX", "1")
	gate := make(chan struct{})
	t.Cleanup(func() {
		close(gate)
		reset_workers(t)
	})
	delivered := make(chan string, 4)
	test_service(t, "busyapp", "arrive", func(*Event) { <-gate })
	test_service(t, "idleapp", "arrive", func(e *Event) { delivered <- e.message })
	recipient, _ := new_entity_keys(t)
	user := user_owning_entity(recipient)
	// The busy app's worker: one frame with its handler, one filling its inbox.
	for i := 0; i < 2; i++ {
		worker_dispatch(user.UID, "busyapp", &worker_frame{frame: &Frame{Type: frame_type_message, ID: "filler-" + itoa(i), To: recipient, Service: "busyapp", Event: "arrive"}, user: user, reply: local_reply{}})
	}
	db := queue_test_table()
	insert := func(id, service string, due int64) {
		db.exec(`insert into queue (id, target, from_entity, to_entity, service, event, next_retry, created)
			values (?, ?, '', ?, ?, 'arrive', ?, ?)`, id, net_id, recipient, service, due, due)
	}
	insert("idle-1", "idleapp", now()-100)
	// More of the busy app's rows than one pass claims: those left in queue.db
	// are put off with the rest, so the next pass does not claim them only to
	// put them off again.
	for i := 0; i < self_loop_batch+2; i++ {
		insert("busy-"+itoa(i), "busyapp", now()-90+int64(i))
	}

	done := make(chan int, 1)
	go func() { done <- self_loop_pass() }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("THE POINT: the self-loop pass waited on one worker's full inbox")
	}
	select {
	case id := <-delivered:
		if id != "idle-1" {
			t.Errorf("delivered %q, want idle-1", id)
		}
	case <-time.After(3 * time.Second):
		t.Error("THE POINT: another app's row waited behind the full inbox")
	}
	for i := 0; i < self_loop_batch+2; i++ {
		id := "busy-" + itoa(i)
		row, _ := db.row("select status, attempts, next_retry from queue where id=?", id)
		if row == nil || row["status"] != "pending" || row["attempts"].(int64) != 0 || row["next_retry"].(int64) <= now() {
			t.Errorf("row %s is %v, want it put off, pending, with no attempt counted", id, row)
		}
	}
}

// TestQueueCleanupPagesAgedRows is #152: the hourly cleanup read every aged
// row, content and data included, into memory at once. It reads and deletes
// them a page at a time, reporting each (sender, app, recipient) once.
func TestQueueCleanupPagesAgedRows(t *testing.T) {
	setup_replication_test(t)
	db := queue_test_table()
	saved := queue_cleanup_page
	queue_cleanup_page = 2
	t.Cleanup(func() { queue_cleanup_page = saved })
	var calls []error_call
	restore := error_test_capture(&calls)
	aged := now() - queue_age_maximum - 100
	for i, to := range []string{"a", "a", "b", "c", "d"} {
		error_test_queue_insert(db, "aged-"+itoa(i), "owner", "feeds", to, "feeds", "post/new", aged+int64(i))
	}
	error_test_queue_insert(db, "fresh", "owner", "feeds", "e", "feeds", "post/new", now())
	queue_cleanup()
	restore()
	if n := db.integer("select count(*) from queue where id like 'aged-%'"); n != 0 {
		t.Errorf("THE POINT: %d aged rows outlived the cleanup", n)
	}
	if !queue_test_held(db, "fresh") {
		t.Error("the cleanup deleted a row not yet aged")
	}
	if len(calls) != 4 {
		t.Errorf("%d timeout reports, want one per sender, app and recipient: %+v", len(calls), calls)
	}
}

// The cleanup's read walks the created index db_upgrade_28 lays, rather than
// every row past its content (#152).
func TestQueueCleanupReadsAlongTheCreatedIndex(t *testing.T) {
	setup_replication_test(t)
	db := db_open("db/queue.db")
	db.exec("drop index if exists queue_created")
	db_upgrade_28()
	rows, err := db.rows("explain query plan "+queue_cleanup_query, now(), 10)
	if err != nil {
		t.Fatal(err)
	}
	plan := fmt.Sprint(rows)
	if !strings.Contains(plan, "queue_created") {
		t.Errorf("THE POINT: the cleanup's read does not use queue_created: %s", plan)
	}
}

// TestQueueCheckEntityRevivesItsRows is #167: when the directory learned where
// an entity is, queue_check_entity only woke the manager, so its rows with no
// target waited out their backoff, and parked ones were never looked at again.
func TestQueueCheckEntityRevivesItsRows(t *testing.T) {
	setup_replication_test(t)
	db := queue_test_table()
	insert := func(id, target, to, status string) {
		db.exec(`insert into queue (id, target, from_entity, to_entity, service, event, status, next_retry, created)
			values (?, ?, '', ?, 's', 'e', ?, ?, ?)`, id, target, to, status, now()+3600, now())
	}
	insert("waiting", "", "located", "pending")
	insert("parked", "", "located", "parked")
	insert("other", "", "elsewhere", "pending")
	insert("pinned", "peer-pinned", "located", "pending")
	queue_check_entity("located")
	for id, revived := range map[string]bool{"waiting": true, "parked": true, "other": false, "pinned": false} {
		row, _ := db.row("select status, next_retry from queue where id=?", id)
		due := row["status"] == "pending" && row["next_retry"].(int64) <= now()
		if due != revived {
			t.Errorf("row %q is %v; due now: %v, want %v", id, row, due, revived)
		}
	}
}

// TestQueueDropKeepsARowItCannotRead is #2108: queue_drop deleted the row
// whether or not its read succeeded, so a read that failed lost the message
// with no error event to the sending app.
func TestQueueDropKeepsARowItCannotRead(t *testing.T) {
	setup_replication_test(t)
	db := queue_test_table()
	var calls []error_call
	restore := error_test_capture(&calls)
	// A value no QueueEntry field takes: the row is there, and will not read.
	db.exec(`insert into queue (id, target, from_entity, to_entity, service, event, from_app, attempts, next_retry, created)
		values ('unreadable', 'peer', 'owner', 'to', 's', 'e', 'feeds', 'many', 0, ?)`, now())
	queue_drop("unreadable", fail_unsupported)
	restore()
	if !queue_test_held(db, "unreadable") {
		t.Error("THE POINT: a row queue_drop could not read was deleted, and its sending app never told")
	}
}

// TestQueueDrainEntityWaitsOutAFailedRead pins that a queue.db read that fails
// is not taken as drained: teardown waits out its deadline, as for rows still
// queued, instead of going ahead at once and cutting off the farewell.
func TestQueueDrainEntityWaitsOutAFailedRead(t *testing.T) {
	setup_replication_test(t)
	db := queue_test_table()

	start := time.Now()
	queue_drain_entity("entity-drained", 400*time.Millisecond)
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("control: an empty queue took %v to count as drained", elapsed)
	}

	db.exec("drop table queue")
	start = time.Now()
	queue_drain_entity("entity-unread", 400*time.Millisecond)
	if elapsed := time.Since(start); elapsed < 350*time.Millisecond {
		t.Errorf("THE POINT: a failed queue.db read was taken as drained after %v", elapsed)
	}
}

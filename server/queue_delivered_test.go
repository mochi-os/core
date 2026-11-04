// Mochi server: the record of which peers a spread message was delivered to
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"strings"
	"testing"
	"time"
)

// delivered_test_setup gives a recipient live at the given peers, in order of
// freshness, and the queue.
func delivered_test_setup(t *testing.T, peers ...string) (*DB, string) {
	t.Helper()
	setup_replication_test(t)
	setup_users_test_schema()
	recipient := test_entity_id('d')
	for i, peer := range peers {
		add_directory_location(t, recipient, peer, now()-int64(100*i))
	}
	return db_open("db/queue.db"), recipient
}

func delivered_test_add(t *testing.T, id, target, to string) {
	t.Helper()
	if err := queue_add_direct(id, target, "from", to, "s", "e", "", nil, nil, nil, "", 0); err != nil {
		t.Fatalf("queue_add_direct %q: %v", id, err)
	}
}

func delivered_test_recorded(db *DB, stem, peer string) bool {
	found, _ := db.exists("select 1 from delivered where stem=? and peer=?", stem, peer)
	return found
}

// A copy acknowledged by peer-b is recorded, so when the message's original
// row is retargeted and peer-b is the only route left, the row is not moved
// onto peer-b beside the copy it already acknowledged: it goes.
func TestDeliveredCopyIsNotSentAgain(t *testing.T) {
	db, recipient := delivered_test_setup(t, "peer-b")
	root := uid()
	sibling := queue_sibling(root, "peer-b")
	delivered_test_add(t, root, "peer-gone", recipient)
	delivered_test_add(t, sibling, "peer-b", recipient)

	queue_ack_async(sibling)
	queue_ack_drain()
	if queue_test_held(db, sibling) {
		t.Fatal("control: the acknowledged copy is still queued")
	}
	if !delivered_test_recorded(db, root, "peer-b") {
		t.Fatal("the acknowledged copy left no record of its delivery")
	}

	q, held := queue_test_load(db, root)
	if !held {
		t.Fatal("control: the original row is gone before its retarget")
	}
	queue_retarget(db, &q)
	if n := db.integer("select count(*) from queue where target='peer-b'"); n != 0 {
		t.Errorf("%d rows of the message are queued for peer-b, which acknowledged it already", n)
	}
	if queue_test_held(db, root) {
		t.Error("the original row is still queued, for a message every route has had")
	}
}

// An original acknowledged while its message still has copies queued is
// recorded too: the copies may be retargeted onto its peer.
func TestDeliveredOriginalOfASpreadIsRecorded(t *testing.T) {
	db, recipient := delivered_test_setup(t, "peer-a", "peer-b")
	root := uid()
	delivered_test_add(t, root, "peer-a", recipient)
	delivered_test_add(t, queue_sibling(root, "peer-b"), "peer-b", recipient)

	queue_ack(root)
	if !delivered_test_recorded(db, root, "peer-a") {
		t.Error("an original acknowledged beside its queued copies left no record")
	}
}

// A message that was never spread leaves no record when it is acknowledged,
// on either acknowledgement path: the table holds spread messages only.
func TestDeliveredOrdinaryAckLeavesNoRecord(t *testing.T) {
	db, recipient := delivered_test_setup(t, "peer-a")
	batched := uid()
	single := uid()
	delivered_test_add(t, batched, "peer-a", recipient)
	delivered_test_add(t, single, "peer-a", recipient)

	queue_ack_async(batched)
	queue_ack_drain()
	queue_ack(single)

	if queue_test_held(db, batched) || queue_test_held(db, single) {
		t.Fatal("control: an acknowledged row is still queued")
	}
	if n := db.integer("select count(*) from delivered"); n != 0 {
		t.Errorf("%d delivery records for messages that were never spread", n)
	}
}

// A row that reaches a peer its message was already delivered to, under an id
// the peer has not seen, is dropped by the send rather than sent: by
// queue_process's send, and by an open Sender's pull.
func TestDeliveredRowIsDropped(t *testing.T) {
	db, recipient := delivered_test_setup(t, "peer-b")
	root := uid()
	db.exec("insert into delivered (stem, peer, created) values (?, ?, ?)", root, "peer-b", now())

	delivered_test_add(t, root, "peer-b", recipient)
	q, _ := queue_test_load(db, root)
	if !queue_send_direct(&q) {
		t.Error("queue_send_direct did not report the row settled")
	}
	if queue_test_held(db, root) {
		t.Error("the row was left queued for a peer that has its message")
	}

	// Control: a row of another message to the same peer is not dropped.
	other := uid()
	delivered_test_add(t, other, "peer-b", recipient)
	q, _ = queue_test_load(db, other)
	if delivered_drop(&q) {
		t.Error("a row of a message never delivered to the peer was dropped")
	}
}

func TestDeliveredRowIsDroppedByTheSender(t *testing.T) {
	var received <-chan string
	link := refuse_open(t, "delivered-sender-peer", func(link *refuse_link) {
		received = run_test_receiver(t, link.receive, link.challenge, "delivered-sender-peer")
	})
	db := db_open("db/queue.db")
	root := uid()
	db.exec("insert into delivered (stem, peer, created) values (?, ?, ?)", root, link.peer, now())
	good := uid()
	refuse_row(t, link, root, link.entity, "s", "e", map[string]any{}, nil)
	refuse_row(t, link, good, link.entity, "s", "e", map[string]any{}, nil)
	db.exec("update queue set next_retry = next_retry - 10 where id = ?", root)

	pulled := make(chan struct{})
	go func() {
		defer close(pulled)
		link.sender.pull_loop()
	}()
	t.Cleanup(func() {
		link.sender.shutdown()
		<-pulled
	})

	select {
	case id := <-received:
		if id != good {
			t.Errorf("the receiver got %q, a message it was delivered already", id)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("control: the other message was not delivered")
	}
	if queue_test_held(db, root) {
		t.Error("the row for a peer that has its message is still queued")
	}
}

// Records are reaped with the queue's retention, but not while a row of their
// message is still queued: a copy spread days after its message outlives the
// age counted from the record.
func TestDeliveredRecordsAreReaped(t *testing.T) {
	db, recipient := delivered_test_setup(t)
	old := now() - queue_age_maximum - 60
	guarded := uid()
	original := uid()
	db.exec("insert into delivered (stem, peer, created) values ('old', 'peer-a', ?)", old)
	db.exec("insert into delivered (stem, peer, created) values ('new', 'peer-a', ?)", now())
	db.exec("insert into delivered (stem, peer, created) values (?, 'peer-a', ?)", guarded, old)
	db.exec("insert into delivered (stem, peer, created) values (?, 'peer-a', ?)", original, old)
	delivered_test_add(t, queue_sibling(guarded, "peer-b"), "peer-b", recipient)
	delivered_test_add(t, original, "peer-c", recipient)

	queue_cleanup()

	if delivered_test_recorded(db, "old", "peer-a") {
		t.Error("a record older than the queue's retention was kept")
	}
	if !delivered_test_recorded(db, "new", "peer-a") {
		t.Error("control: a fresh record was reaped")
	}
	if !delivered_test_recorded(db, guarded, "peer-a") {
		t.Error("an old record was reaped while a copy of its message is still queued")
	}
	if !delivered_test_recorded(db, original, "peer-a") {
		t.Error("an old record was reaped while its message's own row is still queued")
	}
}

// A corrupt queue.db met while settling acknowledgements is quarantined, as
// exec_bg's writes quarantine it, rather than warned about on every flush.
func TestDeliveredSettleQuarantinesACorruptQueue(t *testing.T) {
	db, recipient := delivered_test_setup(t, "peer-b")
	root := uid()
	sibling := queue_sibling(root, "peer-b")
	delivered_test_add(t, sibling, "peer-b", recipient)
	db.exec("create trigger corrupt before insert on delivered begin select raise(abort, 'database disk image is malformed'); end")
	t.Cleanup(func() {
		db.exec("drop trigger if exists corrupt")
		db_quarantine_clear(db.path)
	})

	queue_ack_async(sibling)
	queue_ack_drain()
	if !db_quarantined(db.path) {
		t.Error("a corrupt queue.db met while settling was not quarantined")
	}
	if !queue_test_held(db, sibling) {
		t.Error("the acknowledged copy was deleted though its delivery could not be recorded")
	}
}

// A record that cannot be written keeps the acknowledged row queued, rather
// than forgetting the delivery with it.
func TestDeliveredRecordFailureKeepsTheRow(t *testing.T) {
	db, recipient := delivered_test_setup(t, "peer-b")
	root := uid()
	sibling := queue_sibling(root, "peer-b")
	delivered_test_add(t, sibling, "peer-b", recipient)
	allow := queue_test_refuse(t, db, "before insert on delivered")

	queue_ack_async(sibling)
	queue_ack_drain()
	if !queue_test_held(db, sibling) {
		t.Error("the acknowledged copy was deleted though its delivery could not be recorded")
	}

	// Control: with the record writable the same acknowledgement settles.
	allow()
	queue_ack_async(sibling)
	queue_ack_drain()
	if queue_test_held(db, sibling) || !delivered_test_recorded(db, root, "peer-b") {
		t.Error("control: the acknowledgement did not settle once the record could be written")
	}
}

// One flush of several acknowledged copies records every one of them.
func TestDeliveredFlushRecordsEveryCopy(t *testing.T) {
	db, recipient := delivered_test_setup(t, "peer-a", "peer-b", "peer-c")
	root := uid()
	for _, peer := range []string{"peer-a", "peer-b", "peer-c"} {
		delivered_test_add(t, queue_sibling(root, peer), peer, recipient)
		queue_ack_async(queue_sibling(root, peer))
	}
	queue_ack_drain()
	for _, peer := range []string{"peer-a", "peer-b", "peer-c"} {
		if !delivered_test_recorded(db, root, peer) {
			t.Errorf("no record for %q after the flush", peer)
		}
	}
}

// A row whose id is longer than a uid has a digested stem, which differs from
// its id; it is still no copy, and a message never spread leaves no record.
func TestDeliveredLongIdIsNoCopy(t *testing.T) {
	db, recipient := delivered_test_setup(t, "peer-a")
	long := uid() + "-and-some-more"
	if queue_stem(long) == long {
		t.Fatal("control: the long id's stem is the id itself, so this tests nothing")
	}
	delivered_test_add(t, long, "peer-a", recipient)
	queue_ack(long)
	if n := db.integer("select count(*) from delivered"); n != 0 {
		t.Errorf("%d records for a message that was never spread", n)
	}
}

// A record that cannot be read is logged, not passed over in silence.
func TestDeliveredReadFailuresAreLogged(t *testing.T) {
	db, recipient := delivered_test_setup(t, "peer-a")
	row := uid()
	delivered_test_add(t, row, "peer-a", recipient)
	db.exec("drop table delivered")
	captured := capture_log(t)

	q, _ := queue_test_load(db, row)
	if delivered_drop(&q) {
		t.Error("a row was dropped on a record that could not be read")
	}
	delivered_peers(db, queue_stem(row))

	out := captured.String()
	for _, want := range []string{"unable to read whether", "unable to read the deliveries"} {
		if !strings.Contains(out, want) {
			t.Errorf("no log line containing %q for an unreadable record; got:\n%s", want, out)
		}
	}
}

// An empty-target row's spread queues no copy for a peer its message was
// delivered to.
func TestDeliveredExpandSkipsDeliveredPeers(t *testing.T) {
	db, recipient := delivered_test_setup(t, "peer-a", "peer-b")
	root := uid()
	delivered_test_add(t, root, "", recipient)
	db.exec("insert into delivered (stem, peer, created) values (?, 'peer-b', ?)", root, now())

	q, _ := queue_test_load(db, root)
	queue_expand_empty_target(&q)

	if n := db.integer("select count(*) from queue where target='peer-a'"); n != 1 {
		t.Errorf("control: %d copies for the peer not yet delivered, want 1", n)
	}
	if n := db.integer("select count(*) from queue where target='peer-b'"); n != 0 {
		t.Errorf("%d copies queued for a peer its message was delivered to", n)
	}
}

// The self-loop pass drops a row for this server whose message was delivered
// here already, and hands any other to its worker.
func TestDeliveredSelfLoopPassDrops(t *testing.T) {
	db, recipient := delivered_test_setup(t)
	reset_workers(t)
	t.Cleanup(func() { reset_workers(t) })
	root := uid()
	delivered_test_add(t, root, net_id, recipient)
	db.exec("insert into delivered (stem, peer, created) values (?, ?, ?)", root, net_id, now())

	self_loop_pass()
	if queue_test_held(db, root) {
		t.Error("the row for this server is still queued though its message was delivered here")
	}
	if workers, _ := worker_count(); workers != 0 {
		t.Errorf("the row was handed to a worker: %d workers", workers)
	}

	// Control: a row of a message not delivered here goes to its worker.
	other := uid()
	delivered_test_add(t, other, net_id, recipient)
	self_loop_pass()
	if workers, _ := worker_count(); workers == 0 {
		t.Error("control: the undelivered row was not handed to a worker")
	}
}

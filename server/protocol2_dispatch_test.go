// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Tests for queue_send_direct's self-loop fast path and the
// queue_sending / queue_unsending / queue_untouched helpers that keep
// the async resolver from racing queue_process.

package main

import (
	"sync/atomic"
	"testing"
)

// install_queue_dispatch_row writes one row at the given target+file
// shape so we can probe which dispatch branch fires.
func install_queue_dispatch_row(t *testing.T, id, target, file string, content []byte) {
	t.Helper()
	db := db_open("db/queue.db")
	db.exec(`insert into queue
		(id, type, target, from_entity, to_entity, service, event,
		 from_app, from_services, content, data, file, expires, status,
		 attempts, next_retry, created, priority)
		values
		(?, 'direct', ?, '', '', 's', 'e', '', '', ?, '', ?, 0,
		 'pending', 0, ?, ?, ?)`,
		id, target, content, file, now(), now(), priority_interactive)
}

// --- queue_sending / queue_unsending / queue_untouched -----------------

// queue_dispatch_status reads a row's status, "" when it is gone.
func queue_dispatch_status(t *testing.T, id string) string {
	t.Helper()
	row, _ := db_open("db/queue.db").row("select status from queue where id=?", id)
	if row == nil {
		return ""
	}
	status, _ := row["status"].(string)
	return status
}

// TestQueueSendingClaimsOnlyAPendingRow is #133: queue_sending marked a row
// sending whatever it was, so the inline send went ahead on a new message's
// row a Sender's pull_loop had claimed a moment earlier, and the frame went
// out twice under one in-flight entry. It is a claim: it takes a pending row,
// and says so, or takes nothing.
func TestQueueSendingClaimsOnlyAPendingRow(t *testing.T) {
	setup_replication_test(t)

	install_queue_dispatch_row(t, "qs-1", "peer", "", nil)
	if !queue_sending("qs-1") {
		t.Fatal("control: queue_sending did not claim a pending row")
	}
	if got := queue_dispatch_status(t, "qs-1"); got != "sending" {
		t.Errorf("status after the claim = %q, want sending", got)
	}
	if !queue_held("qs-1") {
		t.Error("a claimed row is not held, so the stuck-sending sweep would take it from its sender")
	}
	if queue_sending("qs-1") {
		t.Error("THE POINT: a row already claimed was claimed again; its frame goes out twice")
	}
	if queue_sending("never-existed") {
		t.Error("a row that does not exist was claimed")
	}
}

func TestQueueUnsendingRollsBack(t *testing.T) {
	setup_replication_test(t)

	install_queue_dispatch_row(t, "qu-1", "peer", "", nil)
	queue_sending("qu-1")
	queue_unsending("qu-1")

	if got := queue_dispatch_status(t, "qu-1"); got != "pending" {
		t.Errorf("status after unsending: %q, want pending", got)
	}
	if queue_held("qu-1") {
		t.Error("a row handed back is still held")
	}
}

func TestQueueUnsendingNoopWhenNotSending(t *testing.T) {
	// queue_unsending should ONLY change the row when status='sending'.
	// A row already in another state (e.g., pending after legitimate
	// queue_fail) MUST NOT be touched.
	setup_replication_test(t)

	install_queue_dispatch_row(t, "nu-1", "peer", "", nil)
	// Force status to something other than 'sending'.
	db := db_open("db/queue.db")
	db.exec("update queue set status='custom' where id=?", "nu-1")

	queue_unsending("nu-1")
	row, _ := db.row("select status from queue where id=?", "nu-1")
	if st, _ := row["status"].(string); st != "custom" {
		t.Errorf("queue_unsending changed non-sending row: status=%q", st)
	}
}

// queue_process fails a row only when its send left it as it was: pending and
// due. One the send claimed, put off or removed is its own path's.
func TestQueueUntouchedOnlyForADuePendingRow(t *testing.T) {
	setup_replication_test(t)

	install_queue_dispatch_row(t, "due", "peer", "", nil)
	if !queue_untouched("due") {
		t.Error("a pending, due row reads as touched")
	}
	install_queue_dispatch_row(t, "claimed", "peer", "", nil)
	queue_sending("claimed")
	install_queue_dispatch_row(t, "deferred", "peer", "", nil)
	queue_defer("deferred", 60)
	for _, id := range []string{"claimed", "deferred", "never-existed"} {
		if queue_untouched(id) {
			t.Errorf("row %q reads as untouched; queue_process would fail it", id)
		}
	}
}

// --- queue_send_direct dispatch tree -----------------------------------

func TestQueueSendDirectSelfLoopFastPath(t *testing.T) {
	// When target == net_id, queue_send_direct claims the row and takes the
	// self-loop fast path, which resolves and admits it before any worker.
	setup_replication_test(t)
	reset_workers(t)
	defer reset_workers(t)

	id := "self-loop-dispatch"
	install_queue_dispatch_row(t, id, net_id, "", nil)
	q := &QueueEntry{ID: id, Target: net_id, Service: "any-svc", Event: "e"}

	ok := queue_send_direct(q)
	if ok {
		t.Error("queue_send_direct returned true for self-loop; expected false (async resolver owns the row)")
	}
	// No app takes the service, so the row is dropped as a receiver would
	// refuse it.
	if got := queue_dispatch_status(t, id); got != "" {
		t.Errorf("a row no app takes is still queued, status %q", got)
	}

	// Wait for the worker to finish so the test cleanup doesn't race.
	workers_drain_test(500 * 1e6) // 500ms (using ns; avoid import dance)
}

// TestInlineSendLeavesARowAlreadyClaimed is #133: the inline send marked a new
// message's row sending whatever it was, so a row an open Sender's pull_loop
// had claimed a moment earlier was sent by both, the frame written twice under
// one in-flight entry. The inline send claims the row or leaves it.
func TestInlineSendLeavesARowAlreadyClaimed(t *testing.T) {
	setup_replication_test(t)
	var opens atomic.Int32
	sender_opener = func(string) (*Sender, error) {
		opens.Add(1)
		return nil, error_sender_unreachable
	}
	t.Cleanup(func() { sender_opener = nil })
	const peer = "12D3KooWClaimedFirst"

	install_queue_dispatch_row(t, "taken-1", peer, "", nil)
	queue_sending("taken-1") // the peer's pull_loop got there first
	message_attempt_send_real(&Message{ID: "taken-1", Service: "s", Event: "e"}, peer, nil)
	if opens.Load() != 0 {
		t.Error("THE POINT: the inline send went ahead on a row a Sender had claimed")
	}
	if got := queue_dispatch_status(t, "taken-1"); got != "sending" {
		t.Errorf("the claimed row is %q, want it left sending to its Sender", got)
	}

	// Control: a row nothing has claimed is sent.
	install_queue_dispatch_row(t, "free-1", peer, "", nil)
	message_attempt_send_real(&Message{ID: "free-1", Service: "s", Event: "e"}, peer, nil)
	if opens.Load() != 1 {
		t.Errorf("control: the inline send of an unclaimed row opened %d Senders, want 1", opens.Load())
	}
}

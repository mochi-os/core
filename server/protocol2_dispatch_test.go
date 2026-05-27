// Tests for queue_send_direct's dispatch tree (self-loop vs file-push
// vs messages) and the queue_unsending / queue_is_inflight helpers
// that keep the async resolver from racing queue_process.
//
// Phase 3f per claude/plans/protocol2.md → Testing strategy.

package main

import (
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

// --- queue_sending / queue_unsending / queue_is_inflight ---------------

func TestQueueSendingMarksRow(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	install_queue_dispatch_row(t, "qs-1", "peer", "", nil)
	queue_sending("qs-1")

	if !queue_is_inflight("qs-1") {
		t.Errorf("queue_is_inflight false after queue_sending")
	}
}

func TestQueueUnsendingRollsBack(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	install_queue_dispatch_row(t, "qu-1", "peer", "", nil)
	queue_sending("qu-1")
	queue_unsending("qu-1")

	if queue_is_inflight("qu-1") {
		t.Errorf("queue_is_inflight true after queue_unsending")
	}
	// Row still present, status pending.
	db := db_open("db/queue.db")
	row, _ := db.row("select status from queue where id=?", "qu-1")
	if row == nil {
		t.Fatal("queue_unsending deleted the row")
	}
	if st, _ := row["status"].(string); st != "pending" {
		t.Errorf("status after unsending: %q, want pending", st)
	}
}

func TestQueueUnsendingNoopWhenNotSending(t *testing.T) {
	// queue_unsending should ONLY change the row when status='sending'.
	// A row already in another state (e.g., pending after legitimate
	// queue_fail) MUST NOT be touched.
	cleanup := setup_replication_test(t)
	defer cleanup()

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

func TestQueueIsInflightFalseForUnknownID(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	if queue_is_inflight("never-existed") {
		t.Errorf("queue_is_inflight true for non-existent row")
	}
}

// --- queue_send_direct dispatch tree -----------------------------------

func TestQueueSendDirectSelfLoopFastPath(t *testing.T) {
	// When target == net_id AND file == "", queue_send_direct takes
	// the self-loop fast path which dispatches to the worker pool.
	// Verify the row ends up in 'sending' state (the worker resolves
	// it asynchronously).
	cleanup := setup_replication_test(t)
	defer cleanup()
	reset_workers(t)
	defer reset_workers(t)

	id := "self-loop-dispatch"
	install_queue_dispatch_row(t, id, net_id, "", nil)
	q := &QueueEntry{ID: id, Target: net_id, Service: "any-svc", Event: "e"}

	ok := queue_send_direct(q)
	if ok {
		t.Error("queue_send_direct returned true for self-loop; expected false (async resolver owns the row)")
	}
	// queue_sending should have been called.
	if !queue_is_inflight(id) {
		// May have already been resolved by the worker — check whether
		// it's gone (queue_drop on unknown service).
		db := db_open("db/queue.db")
		row, _ := db.row("select status from queue where id=?", id)
		if row == nil {
			// Already resolved (fast worker). That's acceptable.
			t.Log("worker resolved row immediately (race-tolerable)")
		} else {
			st, _ := row["status"].(string)
			t.Errorf("row neither inflight nor deleted; status=%q", st)
		}
	}

	// Wait for the worker to finish so the test cleanup doesn't race.
	workers_drain_test(500 * 1e6) // 500ms (using ns; avoid import dance)
}

func TestQueueSendDirectFileBranchSkipsV2Messages(t *testing.T) {
	// When q.File != "", the /mochi/2/messages branch is skipped —
	// file pushes are structurally streams (use /mochi/2/stream
	// instead) and queue_process routes them to queue_send_file_push
	// directly. queue_send_direct's File-gate is `q.File == ""`; we
	// verify the v2 path is not entered (no 'sending' state set) by
	// using a peer that's already cached as v1-only AND with File
	// set — both conditions guarantee the v2 branch is skipped.
	cleanup := setup_replication_test(t)
	defer cleanup()
	reset_protocol_cache(t)
	saved := net_id
	net_id = "self-not-target"
	defer func() { net_id = saved }()

	const peer = "v1-only-peer-file"
	protocol_known_set(peer, protocol_messages, protocol_state_unsupported)

	id := "file-branch"
	install_queue_dispatch_row(t, id, peer, "/tmp/nonexistent", nil)
	q := &QueueEntry{ID: id, Target: peer, File: "/tmp/nonexistent",
		Service: "s", Event: "e"}

	// peer_stream's "unknown peer" branch calls publish() which needs
	// net_pubsub_1. Pre-create the peer entry in memory so peer_stream
	// goes straight to peer_connect (which fails cleanly with net_me=nil).
	peer_add_known(peer, []string{})

	ok := queue_send_direct(q)
	if ok {
		t.Error("queue_send_direct returned true with no real peer")
	}
	if queue_is_inflight(id) {
		t.Errorf("row marked sending despite File != \"\" (should skip v2 branch)")
	}
}

// --- is_v2_unsupported gating ------------------------------------------

func TestQueueSendDirectGatesOnCacheUnsupported(t *testing.T) {
	// If the protocol cache reports protocol_messages as unsupported,
	// queue_send_direct should NOT attempt peer_send and instead
	// fall through to legacy (which will fail with no real peer).
	cleanup := setup_replication_test(t)
	defer cleanup()
	reset_protocol_cache(t)
	saved := net_id
	net_id = "self-not-target"
	defer func() { net_id = saved }()

	const peer = "v1-only-peer"
	protocol_known_set(peer, protocol_messages, protocol_state_unsupported)
	peer_add_known(peer, []string{}) // avoid peer_stream's unknown-peer publish path

	id := "gated"
	install_queue_dispatch_row(t, id, peer, "", nil)
	q := &QueueEntry{ID: id, Target: peer, Service: "s", Event: "e"}
	ok := queue_send_direct(q)
	// Legacy peer_stream returns nil → false. queue_sending must NOT
	// have been called (we skip the v2 branch entirely).
	if ok {
		t.Error("unexpected true return")
	}
	if queue_is_inflight(id) {
		t.Errorf("row marked sending despite cache=unsupported (should bypass v2 branch)")
	}
}

// Mochi server: multi-host fan-out delivery resilience tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.
//
// A message to an entity on several hosts fans out to one queue row per host,
// and an unreachable host is deferred rather than stalling or dropping delivery
// to the others. Pick-by-peer fairness is
// TestQueueSelectPickByPeerDedupesByTarget.

package main

import (
	"testing"
)

// TestQueueExpandEmptyTargetFansOutToAllHosts: a row whose target could not be
// resolved at enqueue time re-resolves on retry into one row per live host -
// both the fan-out mechanism and the self-heal for a row enqueued before any
// host known. Each host's row is the message's copy for it, named by
// queue_sibling, and the empty-target row goes once they are queued.
func TestQueueExpandEmptyTargetFansOutToAllHosts(t *testing.T) {
	setup_replication_test(t) // sets net_id = "self", temp data dir

	// Entity ent-1 lives on three remote hosts (none is self).
	udb := db_open("db/users.db")
	udb.exec("create table if not exists entities ( id text primary key )") // empty: ent-1 is not local

	ddb := db_open("db/directory.db")
	ddb.exec("create table if not exists entries ( entity text not null, peer text not null, seen integer not null )")
	ddb.exec("insert into entries (entity, peer, seen) values (?, ?, ?)", "ent-1", "host-1", now()-300)
	ddb.exec("insert into entries (entity, peer, seen) values (?, ?, ?)", "ent-1", "host-2", now()-200)
	ddb.exec("insert into entries (entity, peer, seen) values (?, ?, ?)", "ent-1", "host-3", now()-100)

	qdb := queue_test_table()
	qdb.exec(`insert into queue (id, target, from_entity, to_entity, service, event, expires, next_retry, created)
		values ('primary', '', 'from-ent', 'ent-1', 'test', 'msg', ?, 0, ?)`, now()+3600, now())

	q := &QueueEntry{
		ID: "primary", Target: "", FromEntity: "from-ent", ToEntity: "ent-1",
		Service: "test", Event: "msg", Expires: now() + 3600,
	}
	queue_expand_empty_target(q)

	rows, _ := qdb.rows("select id, target, to_entity from queue order by target")
	if len(rows) != 3 {
		t.Fatalf("expand left %d rows, want one per host (host-1, host-2, host-3)", len(rows))
	}
	for i, r := range rows {
		target, _ := r["target"].(string)
		want := "host-" + string(rune('1'+i))
		if target != want {
			t.Errorf("row %d targets %q, want %q", i, target, want)
		}
		if id, _ := r["id"].(string); id != queue_sibling("primary", want) {
			t.Errorf("row for %q has id %q, want the message's copy id %q", want, id, queue_sibling("primary", want))
		}
		if to, _ := r["to_entity"].(string); to != "ent-1" {
			t.Errorf("row to_entity = %q, want ent-1", to)
		}
	}
}

// TestQueueProcessDefersSilentHost: a host in the silent-failure cache has its
// row parked for queue_silent_defer instead of attempted, so it neither stalls
// the tick nor consumes a send slot, and the row is deferred rather than
// dropped.
func TestQueueProcessDefersSilentHost(t *testing.T) {
	setup_replication_test(t)

	qdb := queue_test_table()
	queue_test_insert_target(qdb, "row-dead", "dead-host", priority_interactive) // due now, expires 0

	// Mark dead-host unreachable directly (no network, no side effects).
	peer_reachability_lock.Lock()
	peer_reachability["dead-host"] = PeerReachability{ConsecutiveFailures: peer_silent_failure_threshold}
	peer_reachability_lock.Unlock()
	defer func() {
		peer_reachability_lock.Lock()
		delete(peer_reachability, "dead-host")
		peer_reachability_lock.Unlock()
	}()

	if !peer_is_silent("dead-host") {
		t.Fatal("test precondition: dead-host should be silent")
	}

	processed := queue_process()
	if processed != 1 {
		t.Fatalf("queue_process acted on %d rows, want 1 (the deferred row)", processed)
	}

	// The row is deferred ~1h forward and still pending — parked, not
	// sent, not dropped.
	var next, expected int64 = 0, now() + queue_silent_defer
	var status string
	r, err := qdb.row("select next_retry, status from queue where id=?", "row-dead")
	if err != nil || r == nil {
		t.Fatal("row-dead was removed from the queue; want it deferred, not dropped")
	}
	next = row_int(r, "next_retry")
	status, _ = r["status"].(string)
	if next < expected-5 {
		t.Errorf("row-dead next_retry = %d, want >= %d (deferred by queue_silent_defer)", next, expected)
	}
	if status != "pending" {
		t.Errorf("row-dead status = %q, want pending", status)
	}
}

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
// host known.
func TestQueueExpandEmptyTargetFansOutToAllHosts(t *testing.T) {
	setup_replication_test(t) // sets net_id = "self", temp data dir

	// Entity ent-1 lives on three remote hosts (none is self). Distinct
	// seen times so the ordering is deterministic: host-3 most recent.
	udb := db_open("db/users.db")
	udb.exec("create table if not exists entities ( id text primary key )") // empty: ent-1 is not local

	ddb := db_open("db/directory.db")
	ddb.exec("create table if not exists entries ( entity text not null, peer text not null, seen integer not null )")
	ddb.exec("insert into entries (entity, peer, seen) values (?, ?, ?)", "ent-1", "host-1", now()-300)
	ddb.exec("insert into entries (entity, peer, seen) values (?, ?, ?)", "ent-1", "host-2", now()-200)
	ddb.exec("insert into entries (entity, peer, seen) values (?, ?, ?)", "ent-1", "host-3", now()-100)

	qdb := queue_test_table()

	q := &QueueEntry{
		ID: "primary", Target: "", FromEntity: "from-ent", ToEntity: "ent-1",
		Service: "test", Event: "msg", Expires: now() + 3600,
	}
	peer := queue_expand_empty_target(q)

	// The most-recently-seen host is returned for the caller's in-flight send.
	if peer != "host-3" {
		t.Fatalf("expand returned %q, want host-3 (most recently seen)", peer)
	}

	// Exactly the other two hosts get an independent sibling row — no row
	// for host-3 (the caller sends that one on q itself), none left empty.
	rows, _ := qdb.rows("select target, to_entity from queue order by target")
	if len(rows) != 2 {
		t.Fatalf("expand created %d sibling rows, want 2 (host-1, host-2)", len(rows))
	}
	got := map[string]bool{}
	for _, r := range rows {
		target, _ := r["target"].(string)
		got[target] = true
		if to, _ := r["to_entity"].(string); to != "ent-1" {
			t.Errorf("sibling row to_entity = %q, want ent-1", to)
		}
	}
	if !got["host-1"] || !got["host-2"] {
		t.Errorf("sibling targets = %v, want host-1 and host-2", got)
	}
	if got["host-3"] || got[""] {
		t.Errorf("unexpected sibling target (host-3 or empty) in %v", got)
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

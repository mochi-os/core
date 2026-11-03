// Mochi server: multi-host message-send fan-out
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"testing"
	"time"
)

// add_directory_location is a test helper that inserts a (entity, peer)
// row into the local directory so entity_peers() will find it.
func add_directory_location(t *testing.T, entity, peer string, seen int64) {
	t.Helper()
	db := db_open("db/directory.db")
	db.exec("create table if not exists entries ( entity text not null, peer text not null, name text not null, class text not null, data text not null default '', fingerprint text not null default '', version integer not null default 0, created integer not null, seen integer not null, signature text not null default '', attestation text not null default '', primary key ( entity, peer ) )")
	db.exec("insert or replace into entries (entity, peer, name, class, version, created, seen) values (?, ?, 'n', 'person', 1, 1, ?)",
		entity, peer, seen)
}

// stub_message_attempt_send replaces the real send path with a no-op so
// tests can exercise send_work's queue side-effects without reaching
// libp2p (which isn't set up under in-process tests). Returns a
// restore function.
func stub_message_attempt_send() func() {
	orig := message_attempt_send
	message_attempt_send = func(m *Message, peer string, content []byte) {}
	return func() { message_attempt_send = orig }
}

// TestSendWorkFansOutToAllPeers — when an entity has N live directory
// locations, send_work queues one row per location with each `target`
// pre-populated. Each replica gets its own direct delivery attempt.
func TestSendWorkFansOutToAllPeers(t *testing.T) {
	setup_replication_test(t)
	defer stub_message_attempt_send()()
	// entity_local errors (no entities table) resolve to nothing rather than
	// falling through to directory routes, so the schema is required here.
	setup_users_test_schema()

	// Directory: entity has 3 active locations.
	entity := "uid-target-entity"
	add_directory_location(t, entity, "peer-A", now())
	add_directory_location(t, entity, "peer-B", now()-3600)
	add_directory_location(t, entity, "peer-C", now()-7200)

	m := message("from-entity", entity, "service", "event")
	m.set("k", "v")
	// send_work is the synchronous core; m.send() would spawn a
	// goroutine that races with cleanup.
	m.send_work()

	db := db_open("db/queue.db")
	count := db.integer("select count(*) from queue where to_entity=? and target<>''", entity)
	if count != 3 {
		t.Errorf("fan-out: want 3 rows with target set, got %d", count)
	}

	// Each known peer should have exactly one row.
	for _, peer := range []string{"peer-A", "peer-B", "peer-C"} {
		n := db.integer("select count(*) from queue where to_entity=? and target=?", entity, peer)
		if n != 1 {
			t.Errorf("target=%s: want 1 row, got %d", peer, n)
		}
	}
}

// TestSendWorkUnknownEntityKeepsRetryRow — when entity_peers returns
// nothing (no fresh directory entry), send_work falls back to the
// pre-existing single empty-target row so the queue retry loop can
// re-resolve later via entity_peer.
func TestSendWorkUnknownEntityKeepsRetryRow(t *testing.T) {
	setup_replication_test(t)
	defer stub_message_attempt_send()()
	// Entity resolution is strict about the ownership check since the
	// 2026-07 fail-safe: entity_local errors (no entities table) resolve
	// to nothing rather than falling through to directory routes.
	setup_users_test_schema()

	m := message("from-entity", "uid-unknown-entity", "service", "event")
	m.send_work()

	db := db_open("db/queue.db")
	count := db.integer("select count(*) from queue where to_entity='uid-unknown-entity'")
	if count != 1 {
		t.Errorf("unknown entity fallback: want 1 row, got %d", count)
	}
	target, _ := db.row("select target from queue where to_entity='uid-unknown-entity'")
	if t2, _ := target["target"].(string); t2 != "" {
		t.Errorf("target should be empty (awaits resolution); got %q", t2)
	}
}

// TestSendPeerKeepsSingleRow — send_peer pins a specific target and
// should not fan out. Used by replication / system messages that
// already know which peer they're talking to.
func TestSendPeerKeepsSingleRow(t *testing.T) {
	setup_replication_test(t)
	defer stub_message_attempt_send()()
	// Entity resolution is strict about the ownership check since the
	// 2026-07 fail-safe: entity_local errors (no entities table) resolve
	// to nothing rather than falling through to directory routes.
	setup_users_test_schema()

	// Directory has 3 locations — but send_peer shouldn't care.
	add_directory_location(t, "uid-x", "peer-A", now())
	add_directory_location(t, "uid-x", "peer-B", now())

	m := message("from-entity", "uid-x", "service", "event")
	m.send_peer("peer-explicit")

	db := db_open("db/queue.db")
	count := db.integer("select count(*) from queue where to_entity='uid-x'")
	if count != 1 {
		t.Errorf("send_peer: want 1 row, got %d", count)
	}
	target, _ := db.row("select target from queue where to_entity='uid-x'")
	if t2, _ := target["target"].(string); t2 != "peer-explicit" {
		t.Errorf("target: want 'peer-explicit', got %q", t2)
	}
}

// TestQueueSendDirectExpandsEmptyTargetRow — when a queue row sits
// with an empty target (pending resolution) and entity_peers now
// returns N peers, queue_send_direct should expand the row by adding
// (N-1) sibling rows and use the first peer for this attempt.
func TestQueueSendDirectExpandsEmptyTargetRow(t *testing.T) {
	setup_replication_test(t)
	// Entity resolution is strict about the ownership check since the
	// 2026-07 fail-safe: entity_local errors (no entities table) resolve
	// to nothing rather than falling through to directory routes.
	setup_users_test_schema()

	entity := "uid-late-resolution"
	add_directory_location(t, entity, "peer-1", now())
	add_directory_location(t, entity, "peer-2", now()-3600)

	// Simulate a row enqueued when the entity had no directory entry.
	db := db_open("db/queue.db")
	db.exec(`insert into queue (id, type, target, from_entity, to_entity, service, event, from_app, from_services, content, data, file, expires, status, attempts, next_retry, created)
		values ('row-1', 'direct', '', 'from', ?, 's', 'e', '', '', x'', x'', '', 0, 'pending', 0, ?, ?)`,
		entity, now(), now())

	q := &QueueEntry{
		ID:         "row-1",
		Type:       "direct",
		Target:     "",
		FromEntity: "from",
		ToEntity:   entity,
		Service:    "s",
		Event:      "e",
	}
	// Drive the expansion directly; the rest of queue_send_direct
	// reaches into libp2p which isn't set up under tests.
	queue_expand_empty_target(q)

	count := db.integer("select count(*) from queue where to_entity=?", entity)
	if count != 2 {
		t.Errorf("after expansion: want 2 rows total, got %d", count)
	}
	for _, peer := range []string{"peer-1", "peer-2"} {
		n := db.integer("select count(*) from queue where to_entity=? and target=? and id=?", entity, peer, queue_sibling("row-1", peer))
		if n != 1 {
			t.Errorf("expansion created no copy for peer=%s (got count %d)", peer, n)
		}
	}
	if n := db.integer("select count(*) from queue where id='row-1'"); n != 0 {
		t.Error("the empty-target row is still queued beside the copies that carry its message")
	}
}

// inline_send_row waits for a queue row to settle at `status` with
// `attempts`, returning its next_retry.
func inline_send_row(t *testing.T, id, status string, attempts int64) int64 {
	t.Helper()
	db := db_open("db/queue.db")
	deadline := time.Now().Add(3 * time.Second)
	for {
		row, _ := db.row("select status, attempts, next_retry from queue where id=?", id)
		if row != nil && row["status"] == status && row["attempts"] == attempts {
			return row["next_retry"].(int64)
		}
		if time.Now().After(deadline) {
			t.Fatalf("row %s is %v, want %s with %d attempts", id, row, status, attempts)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestInlineSendBusySenderLeavesRowDue pins that an inline send meeting a
// Sender whose outbox stays full leaves the row due, for that Sender's
// pull_loop to take as the outbox drains, rather than on the retry ladder.
func TestInlineSendBusySenderLeavesRowDue(t *testing.T) {
	setup_replication_test(t)
	const peer = "busy-peer"
	s := new_test_sender(t, peer)
	s.outbox = make(chan *outbound) // never read: always full
	defer stash_sender(t, peer, s)()

	install_queue_row(t, "busy-1")
	m := message("from-entity", "to-entity", "service", "event")
	m.ID = "busy-1"
	message_attempt_send_real(m, peer, nil)

	if next := inline_send_row(t, "busy-1", "pending", 0); next > now() {
		t.Errorf("a busy Sender's row waits until %d (now %d): it went onto the retry ladder", next, now())
	}
}

// TestInlineSendUnreachablePeerBacksOff pins the other side: a peer that
// cannot be reached at all is still retried with backoff.
func TestInlineSendUnreachablePeerBacksOff(t *testing.T) {
	setup_replication_test(t)
	install_queue_row(t, "gone-1")
	m := message("from-entity", "to-entity", "service", "event")
	m.ID = "gone-1"
	message_attempt_send_real(m, "unreachable-peer", nil)

	if next := inline_send_row(t, "gone-1", "pending", 1); next <= now() {
		t.Errorf("an unreachable peer's row is due again at once (%d)", next)
	}
}

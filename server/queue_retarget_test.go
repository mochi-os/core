// Mochi server: a queued message follows the recipient when it moves hosts.
//
// A direct row's target is pinned at enqueue, so an entity that moves strands
// the row against the old peer - and parking it records the failure against the
// RECIPIENT, suspending a host that was reachable throughout.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// retarget_db builds queue.db and directory.db in a temporary data directory.
func retarget_db(t *testing.T) *DB {
	t.Helper()
	test_data_directory(t)
	if err := os.MkdirAll(filepath.Join(data_dir, "db"), 0o755); err != nil {
		t.Fatalf("creating the db directory: %v", err)
	}

	db := db_open("db/queue.db")
	db.exec(`create table if not exists queue (
		id text primary key, type text not null, target text not null default '',
		from_entity text not null default '', to_entity text not null default '',
		service text not null default '', event text not null default '',
		from_app text not null default '', from_services text not null default '',
		content blob, data blob, file text not null default '',
		expires integer not null default 0,
		status text not null default 'pending', attempts integer not null default 0,
		next_retry integer not null default 0, created integer not null default 0,
		claimed integer not null default 0, priority integer not null default 0)`)

	// entity_local refuses to resolve when its own query fails, so the table
	// has to exist for the resolver to reach the directory at all.
	users := db_open("db/users.db")
	users.exec(`create table if not exists entities (
		id text not null primary key, private text not null default '',
		fingerprint text not null default '', user text not null default '',
		parent text not null default '', class text not null default '',
		name text not null default '', privacy text not null default 'public',
		data text not null default '', published integer not null default 0)`)

	directory := db_open("db/directory.db")
	directory.exec(`create table if not exists entries (
		entity text not null, peer text not null, name text not null default '',
		class text not null default '', data text not null default '',
		fingerprint text not null default '', version integer not null default 0,
		created integer not null default 0, seen integer not null default 0,
		message text not null default '', expires text not null default '',
		signature text not null default '', primary key (entity, peer))`)
	return db
}

// retarget_route publishes a directory entry saying entity lives at peer.
func retarget_route(t *testing.T, entity, peer string) {
	t.Helper()
	db_open("db/directory.db").exec(
		"insert or replace into entries (entity, peer, created, seen) values (?, ?, ?, ?)",
		entity, peer, now(), now())
}

// retarget_row inserts one direct queue row pinned to peer.
func retarget_row(t *testing.T, db *DB, id, peer, to, status string, attempts int) {
	t.Helper()
	db.exec(`insert into queue (id, type, target, from_entity, to_entity, service, event,
		status, attempts, created) values (?, 'direct', ?, '', ?, 'feeds', 'subscribe', ?, ?, ?)`,
		id, peer, to, status, attempts, now())
}

// retarget_load reads one row back.
func retarget_load(t *testing.T, db *DB, id string) QueueEntry {
	t.Helper()
	var q QueueEntry
	if !db.scan(&q, "select * from queue where id = ?", id) {
		t.Fatalf("row %q is gone", id)
	}
	return q
}

// TestRetargetFollowsAnEntityThatMoved is the regression. The row is pinned to
// a peer with no directory row; the recipient is live somewhere else.
func TestRetargetFollowsAnEntityThatMoved(t *testing.T) {
	db := retarget_db(t)
	original := net_id
	net_id = "12D3KooWSELF"
	t.Cleanup(func() { net_id = original })

	retarget_route(t, "recipient-entity", "12D3KooWNEW")
	retarget_row(t, db, "row-one", "12D3KooWGONE", "recipient-entity", "pending", 7)

	q := retarget_load(t, db, "row-one")
	if !queue_retarget(db, &q) {
		t.Fatal("the row was not retargeted; it is pinned to a peer that is no longer a route, while the recipient is live at another")
	}

	moved := retarget_load(t, db, "row-one")
	if moved.Target != "12D3KooWNEW" {
		t.Errorf("target is %q, want the live route", moved.Target)
	}
	if moved.Attempts != 0 {
		t.Errorf("attempts is %d, want 0 - the budget was spent on a different destination", moved.Attempts)
	}
	if moved.Status != "pending" {
		t.Errorf("status is %q, want pending", moved.Status)
	}
}

// TestRetargetLeavesAnOfflinePeerAlone is what keeps the trigger narrow. The
// pinned peer is still a route; it is simply not answering. That is
// queue_resurrect_peer's case, and retargeting would trample it.
func TestRetargetLeavesAnOfflinePeerAlone(t *testing.T) {
	db := retarget_db(t)
	original := net_id
	net_id = "12D3KooWSELF"
	t.Cleanup(func() { net_id = original })

	retarget_route(t, "recipient-entity", "12D3KooWSTILLTHERE")
	retarget_row(t, db, "row-one", "12D3KooWSTILLTHERE", "recipient-entity", "pending", 30)

	q := retarget_load(t, db, "row-one")
	if queue_retarget(db, &q) {
		t.Error("a row was retargeted although its peer is still a route; \"not answering\" is not \"gone\", and moving the row would trample the reconnect path that exists for it")
	}
	if after := retarget_load(t, db, "row-one"); after.Attempts != 30 {
		t.Errorf("attempts was reset to %d for a peer that is merely offline", after.Attempts)
	}
}

// TestRetargetLeavesAnUnroutableEntityAlone: no route at all is not the same
// as a different route. Nowhere to move the row to, so it keeps its budget and
// its existing behaviour.
func TestRetargetLeavesAnUnroutableEntityAlone(t *testing.T) {
	db := retarget_db(t)
	original := net_id
	net_id = "12D3KooWSELF"
	t.Cleanup(func() { net_id = original })

	retarget_row(t, db, "row-one", "12D3KooWGONE", "unknown-entity", "pending", 7)

	q := retarget_load(t, db, "row-one")
	if queue_retarget(db, &q) {
		t.Error("a row was retargeted with no route to move it to")
	}
}

// TestRetargetFansOutLikeTheEmptyTargetPath. queue_expand_empty_target clones
// siblings for the remaining peers; the two re-resolution paths have to agree,
// or a multi-host recipient gets one copy from one path and N from the other.
func TestRetargetFansOutToEveryLiveRoute(t *testing.T) {
	db := retarget_db(t)
	original := net_id
	net_id = "12D3KooWSELF"
	t.Cleanup(func() { net_id = original })

	retarget_route(t, "recipient-entity", "12D3KooWONE")
	retarget_route(t, "recipient-entity", "12D3KooWTWO")
	retarget_row(t, db, "row-one", "12D3KooWGONE", "recipient-entity", "pending", 7)

	q := retarget_load(t, db, "row-one")
	if !queue_retarget(db, &q) {
		t.Fatal("the row was not retargeted")
	}

	targets := map[string]bool{}
	rows, err := db.rows("select target from queue where to_entity = ?", "recipient-entity")
	if err != nil {
		t.Fatalf("reading rows: %v", err)
	}
	for _, r := range rows {
		if target, ok := r["target"].(string); ok {
			targets[target] = true
		}
	}
	for _, want := range []string{"12D3KooWONE", "12D3KooWTWO"} {
		if !targets[want] {
			t.Errorf("no row targets %q; the empty-target path clones a sibling per peer and this one must match", want)
		}
	}
	if targets["12D3KooWGONE"] {
		t.Error("a row still targets the peer that is no longer a route")
	}
}

// TestRetargetRevivesAParkedRow is the second half of the fix. A parked row
// never reaches queue_fail again, so queue_fail's check cannot save it - and a
// row parked before this shipped is exactly the stranded case, with its
// recipient already suspended.
func TestRetargetRevivesAParkedRow(t *testing.T) {
	db := retarget_db(t)
	original := net_id
	net_id = "12D3KooWSELF"
	t.Cleanup(func() { net_id = original })

	retarget_route(t, "recipient-entity", "12D3KooWNEW")
	retarget_row(t, db, "row-parked", "12D3KooWGONE", "recipient-entity", "parked", 50)

	queue_retarget_parked()

	revived := retarget_load(t, db, "row-parked")
	if revived.Status != "pending" {
		t.Errorf("the parked row is still %q; nothing else revives it, because queue_resurrect_peer waits for the peer it was pinned to", revived.Status)
	}
	if revived.Target != "12D3KooWNEW" {
		t.Errorf("the parked row still targets %q", revived.Target)
	}
	if revived.Attempts != 0 {
		t.Errorf("the parked row kept %d attempts, so its first failure re-parks it immediately", revived.Attempts)
	}
}

// TestRetargetParkedMovesEveryRowForTheDestination. The sweep decides per
// destination and moves that destination's rows in one statement, so a backlog
// of parked rows to one moved entity must not leave all but one behind.
func TestRetargetParkedMovesEveryRowForTheDestination(t *testing.T) {
	db := retarget_db(t)
	original := net_id
	net_id = "12D3KooWSELF"
	t.Cleanup(func() { net_id = original })

	retarget_route(t, "recipient-entity", "12D3KooWNEW")
	for _, id := range []string{"row-a", "row-b", "row-c"} {
		retarget_row(t, db, id, "12D3KooWGONE", "recipient-entity", "parked", 50)
	}

	queue_retarget_parked()

	for _, id := range []string{"row-a", "row-b", "row-c"} {
		row := retarget_load(t, db, id)
		if row.Target != "12D3KooWNEW" || row.Status != "pending" {
			t.Errorf("%s is still %q at %q; the sweep moves a destination, not a row", id, row.Status, row.Target)
		}
	}
}

// TestRetargetParkedFansOutForAMultiPeerDestination covers the other branch.
// One route moves in a single statement with no row read; several routes need a
// sibling per peer, so those rows go through queue_retarget itself - the same
// path queue_fail uses, which is what stops the two fan-outs diverging.
func TestRetargetParkedFansOutForAMultiPeerDestination(t *testing.T) {
	db := retarget_db(t)
	original := net_id
	net_id = "12D3KooWSELF"
	t.Cleanup(func() { net_id = original })

	retarget_route(t, "recipient-entity", "12D3KooWONE")
	retarget_route(t, "recipient-entity", "12D3KooWTWO")
	retarget_row(t, db, "row-parked", "12D3KooWGONE", "recipient-entity", "parked", 50)

	queue_retarget_parked()

	targets := map[string]bool{}
	rows, err := db.rows("select target, status from queue where to_entity = ?", "recipient-entity")
	if err != nil {
		t.Fatalf("reading rows: %v", err)
	}
	for _, r := range rows {
		if target, ok := r["target"].(string); ok {
			targets[target] = true
		}
	}
	for _, want := range []string{"12D3KooWONE", "12D3KooWTWO"} {
		if !targets[want] {
			t.Errorf("no row targets %q; a parked row for a multi-peer recipient must fan out exactly as the queue_fail path does", want)
		}
	}
	if targets["12D3KooWGONE"] {
		t.Error("a row still targets the peer that is no longer a route")
	}
}

// TestRetargetParkedLeavesAValidRouteParked: the sweep must not un-park rows
// waiting on a peer that is simply away. Parking exists so those rows stop
// grinding, and queue_resurrect_peer revives them on reconnect.
func TestRetargetParkedLeavesAValidRouteParked(t *testing.T) {
	db := retarget_db(t)
	original := net_id
	net_id = "12D3KooWSELF"
	t.Cleanup(func() { net_id = original })

	retarget_route(t, "recipient-entity", "12D3KooWAWAY")
	retarget_row(t, db, "row-parked", "12D3KooWAWAY", "recipient-entity", "parked", 50)

	queue_retarget_parked()

	if still := retarget_load(t, db, "row-parked"); still.Status != "parked" {
		t.Errorf("a row parked against a peer that is still its route was revived (%q); that undoes the parking that exists to stop wedged rows grinding hourly", still.Status)
	}
}

// TestQueueFailRetargetsBeforeParking is the ordering. Retargeting after the
// park decision would leave a row parked for the rest of its age against a peer
// that has moved - which is the state the production evidence was in.
func TestQueueFailRetargetsBeforeParking(t *testing.T) {
	source, err := os.ReadFile("queue.go")
	if err != nil {
		t.Fatalf("reading queue.go: %v", err)
	}
	text := string(source)
	at := strings.Index(text, "func queue_fail(")
	if at < 0 {
		t.Fatal("queue.go no longer defines queue_fail")
	}
	body := text[at:]
	if end := strings.Index(body, "\n}\n"); end > 0 {
		body = body[:end]
	}

	retarget := strings.Index(body, "queue_retarget(db, &q)")
	park := strings.Index(body, "attempts >= queue_park_attempts")
	if retarget < 0 {
		t.Fatal("queue_fail does not re-resolve a failing row's target, so an entity that moves hosts strands every row already queued for it")
	}
	if park >= 0 && retarget > park {
		t.Error("queue_fail parks before it retargets; a row whose peer has moved then waits out the rest of its age against an address that is gone")
	}
}

// TestParkedSweepIsWiredIn: the helper existing is not the same as it running.
func TestParkedSweepIsWiredIn(t *testing.T) {
	source, err := os.ReadFile("queue.go")
	if err != nil {
		t.Fatalf("reading queue.go: %v", err)
	}
	text := string(source)
	at := strings.Index(text, "func queue_manager(")
	if at < 0 {
		t.Fatal("queue.go no longer defines queue_manager")
	}
	if !strings.Contains(text[at:], "queue_retarget_parked()") {
		t.Error("queue_manager never calls queue_retarget_parked, so an already-parked row is still stranded until the seven-day reap")
	}
}

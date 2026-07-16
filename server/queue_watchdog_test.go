// Mochi server: queue backlog watchdog regression.
//
// The News feed self-loop wedge (2026-07-06 to 2026-07-15) accumulated 1.4M
// undeliverable pending rows over a week with no direct alert — the WAL
// watchdog fired as an indirect side effect a week after onset. queue_watchdog
// warns when a (target, service) bucket's pending rows say deliveries are not
// draining, and re-warns at most once per queue_warn_repeat while the
// condition persists.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"fmt"
	"testing"
)

func TestQueueWatchdog(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	rows, age, attempts := queue_warn_rows, queue_warn_age, queue_warn_attempts
	queue_warn_rows = 5
	defer func() { queue_warn_rows, queue_warn_age, queue_warn_attempts = rows, age, attempts }()

	db := db_open("db/queue.db")
	db.exec("create table if not exists queue ( id text primary key, type text not null default 'direct', target text not null, from_entity text not null, to_entity text not null, service text not null, event text not null, from_app text not null default '', from_services text not null default '', content blob not null default '', data blob not null default '', file text not null default '', expires integer not null default 0, status text not null default 'pending', attempts integer not null default 0, next_retry integer not null, last_error text not null default '', created integer not null, priority integer not null default 20 )")

	add := func(id, target, service string, created, attempts int64) {
		db.exec("insert into queue (id, target, from_entity, to_entity, service, event, next_retry, created, attempts) values (?, ?, 'e-from', 'e-to', ?, 'event/test', 0, ?, ?)", id, target, service, created, attempts)
	}
	warned := func(target, service string) (int64, bool) {
		v, ok := queue_warned.Load(target + "|" + service)
		if !ok {
			return 0, false
		}
		return v.(int64), true
	}

	// Healthy bucket: a few young rows never warn.
	for i := 0; i < 3; i++ {
		add(fmt.Sprintf("healthy%d", i), "peer-a", "feeds", now(), 0)
	}
	queue_watchdog()
	if _, ok := warned("peer-a", "feeds"); ok {
		t.Fatal("healthy bucket must not warn")
	}

	// Row-count threshold: queue_warn_rows pending rows trip a warn.
	for i := 0; i < int(queue_warn_rows); i++ {
		add(fmt.Sprintf("bulk%d", i), "peer-b", "chat", now(), 0)
	}
	queue_watchdog()
	first, ok := warned("peer-b", "chat")
	if !ok {
		t.Fatal("bucket over queue_warn_rows must warn")
	}

	// Repeat window: an immediate second pass must not re-stamp the warn.
	queue_watchdog()
	if second, _ := warned("peer-b", "chat"); second != first {
		t.Error("bucket re-warned within queue_warn_repeat")
	}

	// Age threshold: a single old pending row trips even at low count.
	add("stale", "peer-c", "forums", now()-queue_warn_age-10, 0)
	queue_watchdog()
	if _, ok := warned("peer-c", "forums"); !ok {
		t.Fatal("bucket with a pending row older than queue_warn_age must warn")
	}

	// Attempts threshold: a wedged row retried past the cap trips.
	add("ground", "peer-d", "wikis", now(), queue_warn_attempts)
	queue_watchdog()
	if _, ok := warned("peer-d", "wikis"); !ok {
		t.Fatal("bucket with attempts at queue_warn_attempts must warn")
	}

	// Drained bucket: deleting its rows clears the re-warn tracking so a
	// future recurrence warns fresh instead of hitting the repeat window.
	db.exec("delete from queue where target='peer-b'")
	queue_watchdog()
	if _, ok := warned("peer-b", "chat"); ok {
		t.Error("drained bucket must clear its warn tracking")
	}
}

// TestQueueFailParksAtAttemptCap — a row that exhausts queue_park_attempts
// is parked (status='parked', outside every claim path) instead of being
// rescheduled forever, and queue_resurrect_peer revives it when its target
// peer reconnects. The 1.4M-row News wedge ground hourly retries for a week
// (attempts up to 157) and starved queue.db's WAL checkpoint.
func TestQueueFailParksAtAttemptCap(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	park := queue_park_attempts
	queue_park_attempts = 3
	defer func() { queue_park_attempts = park }()

	db := db_open("db/queue.db")
	db.exec("insert into queue (id, target, from_entity, to_entity, service, event, next_retry, created, attempts) values ('stuck', 'peer-p', 'e-from', 'e-to', 'feeds', 'event/test', 0, ?, 1)", now())

	status := func() string {
		row, _ := db.row("select status from queue where id='stuck'")
		s, _ := row["status"].(string)
		return s
	}

	// Below the cap: rescheduled as pending.
	queue_fail("stuck", "transient")
	if got := status(); got != "pending" {
		t.Fatalf("attempts below cap: status = %q, want pending", got)
	}

	// At the cap: parked, and the bucket's park warn is stamped.
	queue_fail("stuck", "transient") // attempts -> 3 == cap
	if got := status(); got != "parked" {
		t.Fatalf("attempts at cap: status = %q, want parked", got)
	}
	if _, ok := queue_park_warned.Load("peer-p|feeds"); !ok {
		t.Error("parking must stamp the bucket's park warn")
	}

	// Parked rows are invisible to the claim paths.
	if rows := queue_claim_for_peer("peer-p", 10); len(rows) != 0 {
		t.Errorf("claim must skip parked rows, got %d", len(rows))
	}

	// Peer reconnect revives the parked row for a fresh delivery attempt.
	queue_resurrect_peer("peer-p")
	if got := status(); got != "pending" {
		t.Errorf("after resurrect: status = %q, want pending", got)
	}
}

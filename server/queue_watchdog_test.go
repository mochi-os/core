// Mochi server: queue backlog watchdog regression.
//
// The News feed self-loop wedge (2026-07-06 to 2026-07-15) accumulated 1.4M
// undeliverable pending rows over a week with no direct alert — the WAL
// watchdog fired as an indirect side effect a week after onset. queue_watchdog
// warns per (target, service) bucket when rows or attempts say deliveries are
// not draining, warns across buckets when enough distinct destinations have
// gone stale or nothing is delivering anywhere, and re-warns at most once per
// queue_warn_repeat while a condition persists.
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
	db.exec("create table if not exists queue ( id text primary key, type text not null default 'direct', target text not null, from_entity text not null, to_entity text not null, service text not null, event text not null, from_app text not null default '', from_services text not null default '', content blob not null default '', data blob not null default '', file text not null default '', expires integer not null default 0, status text not null default 'pending', attempts integer not null default 0, next_retry integer not null, last_error text not null default '', created integer not null, priority integer not null default 20, claimed integer not null default 0 )")

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

	// Age is NOT a per-bucket criterion: one destination holding an old
	// row is a departed peer, not a fault here, and warned per bucket it
	// was a daily email for every dead subscriber for the ~6 weeks before
	// health parked it. Age warns across buckets - see
	// TestQueueWatchdogStaleBreadth.
	add("stale", "peer-c", "forums", now()-queue_warn_age-10, 0)
	queue_watchdog()
	if _, ok := warned("peer-c", "forums"); ok {
		t.Fatal("a single bucket must not warn on age alone")
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

// TestQueueWatchdogClassified — rows the health machinery has already
// classified are outside the watchdog's scope: parked rows and pending
// rows for suspended recipients must not count however old they are,
// while an unclassified recipient's old rows still do. Re-warning about
// classified rows until the reaper deleted them was ~5 admin emails per
// day per ghost subscriber (2026-07). Asserted through the stale-target
// breadth count, since age no longer warns per bucket.
func TestQueueWatchdogClassified(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	limit := queue_warn_stale_targets
	queue_warn_stale_targets = 1
	defer func() { queue_warn_stale_targets = limit; queue_stale_warned = 0 }()

	db := db_open("db/queue.db")
	add := func(id, target, recipient, service, status string) {
		db.exec("insert into queue (id, target, from_entity, to_entity, service, event, next_retry, created, status) values (?, ?, 'e-from', ?, ?, 'event/test', 0, ?, ?)", id, target, recipient, service, now()-queue_warn_age-10, status)
	}

	// Parked rows never count: parking is the classification.
	add("row-parked", "peer-parked", "r-parked", "feeds", "parked")
	// Pending rows for a suspended recipient never count: the health
	// machinery owns them (probe, evict, reap).
	db.exec("insert into health (recipient, suspended, since) values ('r-gated', ?, ?)", now(), now())
	add("row-gated", "peer-gated", "r-gated", "forums", "pending")

	queue_watchdog()
	if queue_stale_warned != 0 {
		t.Fatal("parked rows and suspended recipients must not count as stale destinations")
	}

	// An unclassified recipient's old row does count, and at threshold 1
	// is enough to warn.
	add("row-live", "peer-live", "r-live", "wikis", "pending")
	queue_watchdog()
	if queue_stale_warned == 0 {
		t.Error("an unclassified stale destination must count toward the breadth warn")
	}
}

// TestQueueWatchdogStaleBreadth — age is a breadth signal. One or two
// destinations holding an old undelivered row is a departed peer and
// must not email; queue_warn_stale_targets of them together is a
// resolution or connectivity fault on this side and must. The re-warn
// window and recovery clearing behave like the other watchdog signals.
func TestQueueWatchdogStaleBreadth(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	limit, silence := queue_warn_stale_targets, queue_warn_silence
	queue_warn_stale_targets = 3
	queue_warn_silence = 1 << 40 // silence never trips in this test
	defer func() {
		queue_warn_stale_targets, queue_warn_silence = limit, silence
		queue_stale_warned = 0
		queue_delivered.Store(0)
	}()
	queue_delivered.Store(now()) // deliveries are flowing elsewhere

	db := db_open("db/queue.db")
	stale := func(id, target string) {
		db.exec("insert into queue (id, target, from_entity, to_entity, service, event, next_retry, created) values (?, ?, 'e-from', 'e-to', 'feeds', 'event/test', 0, ?)", id, target, now()-queue_warn_age-10)
	}

	// Two departed peers, several rows each: below the breadth threshold,
	// no email however old the rows are or how many per peer.
	stale("a1", "peer-a")
	stale("a2", "peer-a")
	stale("b1", "peer-b")
	queue_watchdog()
	if queue_stale_warned != 0 {
		t.Fatal("stale destinations below queue_warn_stale_targets must not warn")
	}
	if _, ok := queue_warned.Load("peer-a|feeds"); ok {
		t.Fatal("age must never warn per bucket")
	}

	// A third distinct destination crosses the threshold. It is distinct
	// TARGETS that count, not rows: peer-a's two rows were one destination.
	stale("c1", "peer-c")
	queue_watchdog()
	first := queue_stale_warned
	if first == 0 {
		t.Fatal("at queue_warn_stale_targets distinct stale destinations the breadth warn must fire")
	}

	// Repeat window: an immediate second pass must not re-stamp.
	queue_watchdog()
	if queue_stale_warned != first {
		t.Error("stale breadth warn re-fired within queue_warn_repeat")
	}

	// Recovery: one destination draining drops the count below threshold
	// and clears the tracking so a future recurrence warns fresh.
	db.exec("delete from queue where target='peer-c'")
	queue_watchdog()
	if queue_stale_warned != 0 {
		t.Error("dropping below the threshold must clear the stale breadth tracking")
	}
}

// TestQueueWatchdogSilence — a single stale destination is not evidence
// of anything while deliveries flow elsewhere, but the same destination
// with nothing delivered to ANYONE for queue_warn_silence is
// indistinguishable from this server being off the network, and warns
// regardless of breadth. A zero delivery stamp (nothing since start) is
// no evidence either way and must not count as silence.
func TestQueueWatchdogSilence(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	limit, silence := queue_warn_stale_targets, queue_warn_silence
	queue_warn_stale_targets = 1 << 40 // breadth never trips in this test
	queue_warn_silence = 3600
	defer func() {
		queue_warn_stale_targets, queue_warn_silence = limit, silence
		queue_stale_warned = 0
		queue_delivered.Store(0)
	}()

	db := db_open("db/queue.db")
	db.exec("insert into queue (id, target, from_entity, to_entity, service, event, next_retry, created) values ('s1', 'peer-s', 'e-from', 'e-to', 'feeds', 'event/test', 0, ?)", now()-queue_warn_age-10)

	// Nothing delivered since start: unknown, not silent.
	queue_delivered.Store(0)
	queue_watchdog()
	if queue_stale_warned != 0 {
		t.Fatal("no delivery since start is not evidence of silence and must not warn")
	}

	// A recent delivery anywhere: that peer is simply gone. No email.
	queue_delivered.Store(now() - 60)
	queue_watchdog()
	if queue_stale_warned != 0 {
		t.Fatal("one stale destination with recent deliveries elsewhere must not warn")
	}

	// Silence past the window with a stale row pending: that is us.
	queue_delivered.Store(now() - queue_warn_silence - 10)
	queue_watchdog()
	if queue_stale_warned == 0 {
		t.Fatal("a stale destination with no delivery to anyone past queue_warn_silence must warn")
	}

	// A delivery landing clears it on the next tick.
	queue_delivered.Store(now())
	queue_watchdog()
	if queue_stale_warned != 0 {
		t.Error("a delivery must clear the silence warn tracking")
	}

	// Silence with NO stale rows is not this signal's business - a quiet
	// server with an empty queue has nothing undelivered to worry about.
	db.exec("delete from queue")
	queue_delivered.Store(now() - queue_warn_silence - 10)
	queue_watchdog()
	if queue_stale_warned != 0 {
		t.Error("silence with nothing stale pending must not warn")
	}
}

// TestQueueWatchdogSuspendedBreadth — individual suspensions are silent,
// so crossing queue_warn_suspended is the first email about them at all:
// that breadth is a systemic resolution failure, not ghost residue.
func TestQueueWatchdogSuspendedBreadth(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	limit := queue_warn_suspended
	queue_warn_suspended = 3
	defer func() { queue_warn_suspended = limit; queue_suspended_warned = 0 }()

	db := db_open("db/queue.db")
	suspend := func(recipient string) {
		db.exec("insert into health (recipient, suspended, since) values (?, ?, ?)", recipient, now(), now())
	}

	// Residue suspended before the day window never counts: without the
	// window this old row would make the r-1/r-2 pass reach the
	// threshold of 3 and fire.
	db.exec("insert into health (recipient, suspended, since) values ('r-old', ?, ?)", now()-2*86400, now()-2*86400)

	suspend("r-1")
	suspend("r-2")
	queue_watchdog()
	if queue_suspended_warned != 0 {
		t.Fatal("below the threshold the breadth warn must not fire (old residue must not count)")
	}

	suspend("r-3")
	queue_watchdog()
	first := queue_suspended_warned
	if first == 0 {
		t.Fatal("at the threshold the breadth warn must fire")
	}

	// Repeat window: an immediate second pass must not re-stamp.
	queue_watchdog()
	if queue_suspended_warned != first {
		t.Error("breadth warn re-fired within queue_warn_repeat")
	}

	// Recovery clears the tracking so a future recurrence warns fresh.
	db.exec("delete from health")
	queue_watchdog()
	if queue_suspended_warned != 0 {
		t.Error("recovery must clear the breadth warn tracking")
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

	// At the cap: parked.
	queue_fail("stuck", "transient") // attempts -> 3 == cap
	if got := status(); got != "parked" {
		t.Fatalf("attempts at cap: status = %q, want parked", got)
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

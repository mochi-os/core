// Mochi server: per-recipient delivery health regressions.
//
// A dead subscriber used to cost fifty dials per fan-out event forever,
// until the directory forgot their host (30 days, resettable by ghost
// re-announcements). The health record remembers failure per RECIPIENT:
// an exhausted retry budget with no contradicting success suspends them,
// suspension gates broadcast fan-out down to a periodic probe, and past
// the evict age the owning app is told to drop the subscriber.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"testing"
	"time"

	sl "go.starlark.net/starlark"
)

func health_row(t *testing.T, recipient string) (failures, denials, success, suspended, probed int64) {
	t.Helper()
	db := db_open("db/queue.db")
	row, _ := db.row("select failures, denials, success, suspended, probed from health where recipient=?", recipient)
	if row == nil {
		return 0, 0, 0, 0, 0
	}
	get := func(name string) int64 { v, _ := row[name].(int64); return v }
	return get("failures"), get("denials"), get("success"), get("suspended"), get("probed")
}

func TestHealthStateMachine(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	// An uncontradicted full-budget failure suspends.
	health_failure("r-dead", now()-3600)
	failures, _, _, suspended, _ := health_row(t, "r-dead")
	if failures != 1 || suspended == 0 {
		t.Fatalf("uncontradicted park must suspend: failures=%d suspended=%d", failures, suspended)
	}

	// A success DURING the failure window blocks suspension: mixed
	// outcomes are a per-message problem, not a dead recipient.
	health_success("r-mixed") // no row yet: bare update is a no-op
	health_failure("r-mixed", now()-3600)
	db := db_open("db/queue.db")
	db.exec("update health set suspended=0, success=? where recipient='r-mixed'", now())
	health_failure("r-mixed", now()-3600) // ladder started before the success
	if _, _, _, suspended, _ := health_row(t, "r-mixed"); suspended != 0 {
		t.Fatal("a success inside the failure window must block suspension")
	}

	// Success clears everything, including suspension.
	health_success("r-dead")
	failures, denials, success, suspended, _ := health_row(t, "r-dead")
	if failures != 0 || denials != 0 || success == 0 || suspended != 0 {
		t.Fatalf("success must clear the streak: failures=%d denials=%d success=%d suspended=%d", failures, denials, success, suspended)
	}

	// Authoritative denials suspend at the limit without a full ladder.
	for i := int64(0); i < queue_denial_limit; i++ {
		if _, _, _, suspended, _ := health_row(t, "r-gone"); suspended != 0 {
			t.Fatalf("suspended after only %d denials", i)
		}
		health_denial("r-gone")
	}
	if _, denials, _, suspended, _ := health_row(t, "r-gone"); denials != queue_denial_limit || suspended == 0 {
		t.Fatalf("denial limit must suspend: denials=%d suspended=%d", denials, suspended)
	}
}

func TestHealthGate(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db := db_open("db/queue.db")

	// No health row: healthy, passes.
	if skip, evict := health_gate("r-healthy"); skip || evict {
		t.Fatal("recipient with no history must pass the gate")
	}

	// Freshly suspended and freshly probed: skipped.
	db.exec("insert into health (recipient, suspended, probed, since) values ('r-down', ?, ?, ?)", now(), now(), now())
	if skip, evict := health_gate("r-down"); !skip || evict {
		t.Fatal("suspended recipient inside the probe interval must be skipped")
	}

	// Probe interval elapsed: one passthrough, and the probe stamp
	// updates so the next call skips again.
	db.exec("update health set probed=? where recipient='r-down'", now()-queue_probe_interval-10)
	if skip, _ := health_gate("r-down"); skip {
		t.Fatal("probe passthrough must not skip")
	}
	if skip, _ := health_gate("r-down"); !skip {
		t.Fatal("second call inside the refreshed probe interval must skip")
	}

	// Past the evict age: skip and signal eviction.
	db.exec("update health set suspended=? where recipient='r-down'", now()-queue_evict_age-10)
	if skip, evict := health_gate("r-down"); !skip || !evict {
		t.Fatal("suspension past the evict age must skip and evict")
	}
}

func TestHealthQueueIntegration(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	park := queue_park_attempts
	queue_park_attempts = 2
	defer func() { queue_park_attempts = park }()

	db := db_open("db/queue.db")
	add := func(id, recipient string) {
		db.exec("insert into queue (id, target, from_entity, to_entity, service, event, next_retry, created, attempts) values (?, 'peer-x', 'e-from', ?, 'feeds', 'event/test', 0, ?, 1)", id, recipient, now()-3600)
	}

	// A park feeds health_failure and suspends the recipient.
	add("row-1", "r-sub")
	queue_fail("row-1", "transient") // attempts -> 2 == cap: parks
	if failures, _, _, suspended, _ := health_row(t, "r-sub"); failures != 1 || suspended == 0 {
		t.Fatalf("park must record failure and suspend: failures=%d suspended=%d", failures, suspended)
	}

	// A single ack clears it.
	add("row-2", "r-sub")
	queue_ack("row-2")
	if failures, _, _, suspended, _ := health_row(t, "r-sub"); failures != 0 || suspended != 0 {
		t.Fatalf("ack must clear the streak: failures=%d suspended=%d", failures, suspended)
	}

	// The batched ack path clears it too. Backdate the ack's success stamp
	// first — the add() ladder starts an hour ago, and suspension requires
	// the last success to predate the ladder.
	db.exec("update health set success=? where recipient='r-sub'", now()-7200)
	add("row-3", "r-sub")
	queue_fail("row-3", "transient")
	if _, _, _, suspended, _ := health_row(t, "r-sub"); suspended == 0 {
		t.Fatal("setup: expected suspension before batch ack")
	}
	add("row-4", "r-sub")
	queue_ack_flush([]string{"row-4"})
	if _, _, _, suspended, _ := health_row(t, "r-sub"); suspended != 0 {
		t.Fatal("batched ack must clear suspension")
	}

	// An unknown_user NACK drop feeds health_denial.
	add("row-5", "r-vanished")
	queue_drop("row-5", fail_unknown_user)
	if _, denials, _, _, _ := health_row(t, "r-vanished"); denials != 1 {
		t.Fatalf("unknown_user drop must record a denial, got %d", denials)
	}
}

// TestHealthCleanupReap: the age sweep is the failure signal for rows
// whose attempts never climb (a gossip-announced ghost peer with no
// reachable addresses short-circuits before queue_fail) — reaping must
// suspend the recipient even though no row ever parked.
func TestHealthCleanupReap(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	db := db_open("db/queue.db")
	aged := now() - queue_max_age - 3600
	for _, id := range []string{"reap-1", "reap-2"} {
		db.exec("insert into queue (id, target, from_entity, to_entity, service, event, next_retry, created, attempts) values (?, 'peer-ghost', 'e-from', 'r-frozen', 'projects', 'event/test', 0, ?, 1)", id, aged)
	}
	queue_cleanup()

	if n := db.integer("select count(*) from queue where to_entity='r-frozen'"); n != 0 {
		t.Fatalf("aged rows must be reaped, %d remain", n)
	}
	failures, _, _, suspended, _ := health_row(t, "r-frozen")
	if failures != 1 || suspended == 0 {
		t.Fatalf("reap must record one failure per recipient and suspend: failures=%d suspended=%d", failures, suspended)
	}
}

// TestBroadcastSendHealthGate drives the real api_broadcast_send: a
// suspended subscriber gets no queue row, a due probe passes one through,
// and past the evict age the app is dispatched to instead.
func TestBroadcastSendHealthGate(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()

	feed, identity, healthy, down := exclude_test_entities(t)
	users := db_open("db/users.db")
	users.exec("insert into users (uid, username) values ('u-owner', 'owner@x')")
	users.exec("insert into entities (id, private, fingerprint, user, class, name) values (?, '', ?, 'u-owner', 'feed', 'Feed')", feed, fingerprint(feed))
	users.exec("insert into entities (id, private, fingerprint, user, class, name) values (?, '', ?, 'u-owner', 'person', 'Owner')", identity, fingerprint(identity))

	user := user_by_uid("u-owner")
	app := &App{id: "testapp"}
	thread := &sl.Thread{}
	thread.SetLocal("user", user)
	thread.SetLocal("app", app)
	builtin := sl.NewBuiltin("mochi.broadcast.send", api_broadcast_send)
	send := func() {
		subscribers := sl.NewList([]sl.Value{sl.String(healthy), sl.String(down)})
		data := sl.NewDict(1)
		_ = data.SetKey(sl.String("body"), sl.String("x"))
		if _, err := api_broadcast_send(thread, builtin, sl.Tuple{
			sl.String(feed), sl.String(feed), subscribers,
			sl.String("feeds"), sl.String("post/create"), data, sl.String(""),
		}, nil); err != nil {
			t.Fatalf("api_broadcast_send: %v", err)
		}
	}
	queue := db_open("db/queue.db")
	rows := func(recipient string) int {
		return queue.integer("select count(*) from queue where to_entity=?", recipient)
	}
	wait := func(recipient string, want int) {
		deadline := time.Now().Add(5 * time.Second)
		for rows(recipient) != want {
			if time.Now().After(deadline) {
				t.Fatalf("recipient %s: rows=%d, want %d", recipient[:8], rows(recipient), want)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	// Suspended subscriber: the healthy one gets a row, the suspended
	// one gets nothing.
	queue.exec("insert into health (recipient, suspended, probed, since) values (?, ?, ?, ?)", down, now(), now(), now())
	send()
	wait(healthy, 1)
	if n := rows(down); n != 0 {
		t.Fatalf("suspended subscriber must get no fan-out row, got %d", n)
	}

	// Probe due: exactly one row passes through.
	queue.exec("update health set probed=? where recipient=?", now()-queue_probe_interval-10, down)
	send()
	wait(healthy, 2)
	wait(down, 1)

	// Past the evict age: no row, one dispatch to the owning app, and
	// the per-day throttle holds on a second send.
	var dispatched []string
	original := subscriber_dispatch
	subscriber_dispatch = func(u *User, a *App, code, reason, service, entity string, orig map[string]any, detail func() map[string]any) {
		dispatched = append(dispatched, code+"|"+entity)
	}
	defer func() { subscriber_dispatch = original }()
	queue.exec("update health set suspended=? where recipient=?", now()-queue_evict_age-10, down)
	send()
	wait(healthy, 3)
	if n := rows(down); n != 1 {
		t.Fatalf("evict-age subscriber must get no new rows, got %d", n)
	}
	if len(dispatched) != 1 || dispatched[0] != error_code_subscriber_unreachable+"|"+down {
		t.Fatalf("evict dispatch: %v", dispatched)
	}
	send()
	wait(healthy, 4)
	if len(dispatched) != 1 {
		t.Fatalf("evict dispatch must throttle per day, got %d", len(dispatched))
	}
}

// TestHealthEvictOverdue — an (app, recipient) pair still receiving daily
// eviction dispatches health_evict_overdue after the first one means the
// app is ignoring them (missing subscriber/unreachable handler) and its
// ghost will recycle invisibly; the operator is warned once.
func TestHealthEvictOverdue(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	var dispatched int
	original := subscriber_dispatch
	subscriber_dispatch = func(u *User, a *App, code, reason, service, entity string, orig map[string]any, detail func() map[string]any) {
		dispatched++
	}
	defer func() { subscriber_dispatch = original }()

	app := &App{id: "testapp"}
	key := app.id + "|r-ignored"
	record := func() health_evict_record {
		t.Helper()
		v, ok := health_evict_state.Load(key)
		if !ok {
			t.Fatal("expected an eviction record")
		}
		return v.(health_evict_record)
	}

	// Fresh pair: dispatches, not yet overdue.
	health_evict_dispatch(nil, app, "feeds", "r-ignored")
	if dispatched != 1 {
		t.Fatalf("first dispatch must go through, got %d", dispatched)
	}
	if record().warned {
		t.Fatal("fresh pair must not be marked warned")
	}

	// Same day: throttled.
	health_evict_dispatch(nil, app, "feeds", "r-ignored")
	if dispatched != 1 {
		t.Fatalf("same-day dispatch must be throttled, got %d", dispatched)
	}

	// Backdate past the overdue window with the daily throttle open: the
	// dispatch goes through and the overdue warn is recorded.
	health_evict_state.Store(key, health_evict_record{first: now() - health_evict_overdue - 10, last: now() - 86400 - 10})
	health_evict_dispatch(nil, app, "feeds", "r-ignored")
	if dispatched != 2 {
		t.Fatalf("overdue dispatch must go through, got %d", dispatched)
	}
	if !record().warned {
		t.Fatal("overdue pair must be marked warned")
	}

	// The flag persists across later dispatches, so the warn cannot
	// re-fire for this pair.
	health_evict_state.Store(key, health_evict_record{first: now() - health_evict_overdue - 10, last: now() - 86400 - 10, warned: true})
	health_evict_dispatch(nil, app, "feeds", "r-ignored")
	if dispatched != 3 || !record().warned {
		t.Fatalf("later dispatches must keep the warned flag, dispatched=%d", dispatched)
	}
}

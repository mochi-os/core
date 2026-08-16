// Mochi server: broadcast fan-out is metered by recipient count.
//
// mochi.message.send and mochi.message.send_peer each charge one against
// rate_limit_net_send. Broadcast — the only send API that turns one call
// into N wire messages and N queue.db rows — charged nothing at all, so
// the one amplifying path was the one unmetered path. An uncapped
// recipient list is an uncapped write to a database whose 1GB ceiling has
// already been reached and panicked in production (see send_peer in
// messages.go).
//
// It is charged against rate_limit_broadcast, its own bucket, sized for
// the per-recipient unit; sharing rate_limit_net_send's 1000-per-second
// would refuse feeds' RSS ingest, which broadcasts once per imported item
// to every subscriber.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"strings"
	"testing"
	"time"

	sl "go.starlark.net/starlark"
)

// broadcast_limit_setup mints an owner with a feed entity and returns the
// thread api_broadcast_send runs on, the feed id, and the app whose budget
// is charged. The app's budget starts clean.
func broadcast_limit_setup(t *testing.T) (*sl.Thread, string, *App) {
	t.Helper()
	setup_users_test_schema()

	feed := withdraw_test_entity(t)
	identity := withdraw_test_entity(t)
	users := db_open("db/users.db")
	users.exec("insert into users (uid, username) values ('u-limit', 'limit@x')")
	users.exec("insert into entities (id, private, fingerprint, user, class, name) values (?, '', ?, 'u-limit', 'feed', 'Feed')", feed, fingerprint(feed))
	// user_owning_entity resolves only users with a person identity.
	users.exec("insert into entities (id, private, fingerprint, user, class, name) values (?, '', ?, 'u-limit', 'person', 'Owner')", identity, fingerprint(identity))

	user := user_by_uid("u-limit")
	if user == nil {
		t.Fatal("user_by_uid")
	}
	app := &App{id: "limitapp"}

	thread := &sl.Thread{}
	thread.SetLocal("user", user)
	thread.SetLocal("app", app)

	// Create the system tables up front so a REFUSED send can be
	// distinguished from one that never got a log table to write to.
	if db_app_system(user, app) == nil {
		t.Fatal("no system database")
	}
	rate_limit_broadcast.reset(app.id)

	return thread, feed, app
}

// broadcast_limit_send calls the real API with the given subscriber list.
func broadcast_limit_send(thread *sl.Thread, feed string, subscribers []sl.Value) error {
	data := sl.NewDict(1)
	_ = data.SetKey(sl.String("body"), sl.String("hello"))
	builtin := sl.NewBuiltin("mochi.broadcast.send", api_broadcast_send)
	_, err := api_broadcast_send(thread, builtin, sl.Tuple{
		sl.String(feed), sl.String(feed), sl.NewList(subscribers),
		sl.String("feeds"), sl.String("post/create"), data,
	}, nil)
	return err
}

// broadcast_limit_sequences counts log rows written for this stream. Zero
// means the call was refused before broadcast_log_append allocated a
// sequence — which is the property that matters, because resync replays
// from the log and would hand a half-delivered sequence to the recipients
// who never received it.
func broadcast_limit_sequences(t *testing.T, thread *sl.Thread, key string) int {
	t.Helper()
	user, _ := thread.Local("user").(*User)
	app, _ := thread.Local("app").(*App)
	db := db_app_system(user, app)
	if db == nil {
		t.Fatal("no system database")
	}
	return db.integer("select count(*) from log where key=?", key)
}

// TestBroadcastChargesOnePerRecipient. The cost of a call is its recipient
// count, not one: a budget of four is spent by a single four-subscriber
// broadcast. Charging one per call (the mochi.message.send shape) would
// leave three-quarters of the budget intact here.
func TestBroadcastChargesOnePerRecipient(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	thread, feed, app := broadcast_limit_setup(t)

	limit := rate_limit_broadcast.limit
	defer func() { rate_limit_broadcast.limit = limit }()
	rate_limit_broadcast.limit = 4

	subscribers := []sl.Value{}
	recipients := []string{}
	for range 4 {
		id := withdraw_test_entity(t)
		recipients = append(recipients, id)
		subscribers = append(subscribers, sl.String(id))
	}
	if err := broadcast_limit_send(thread, feed, subscribers); err != nil {
		t.Fatalf("a send within budget was refused: %v", err)
	}
	if !rate_limit_broadcast.exhausted(app.id) {
		t.Error("four recipients did not spend a budget of four - the call is being charged as one message, not as one per recipient")
	}

	// This is the only test here whose send reaches the fan-out, and m.send()
	// runs on a goroutine. Wait for the rows: a goroutine still in flight when
	// setup_replication_test tears the temp directory down writes to a queue.db
	// that no longer has its table, and panics the whole package's test binary.
	broadcast_limit_await(t, recipients)
}

// broadcast_limit_await blocks until each recipient has its queued row.
func broadcast_limit_await(t *testing.T, recipients []string) {
	t.Helper()
	queue := db_open("db/queue.db")
	deadline := time.Now().Add(5 * time.Second)
	for _, recipient := range recipients {
		for queue.integer("select count(*) from queue where to_entity=?", recipient) != 1 {
			if time.Now().After(deadline) {
				t.Fatalf("recipient %s: queue row never appeared", recipient)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// TestBroadcastRefusesASpentBudgetBeforeTheLogAppend. The refusal has to
// land before the sequence is allocated. Refusing partway through the
// fan-out would leave a log row that only some subscribers ever received,
// and resync — which replays from that log — would hand it to the rest
// later as though the original delivery had happened.
func TestBroadcastRefusesASpentBudgetBeforeTheLogAppend(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	thread, feed, app := broadcast_limit_setup(t)

	rate_limit_broadcast.spend(app.id, rate_limit_broadcast.limit)
	if !rate_limit_broadcast.exhausted(app.id) {
		t.Fatal("budget did not exhaust")
	}

	err := broadcast_limit_send(thread, feed, []sl.Value{
		sl.String(withdraw_test_entity(t)),
		sl.String(withdraw_test_entity(t)),
	})
	if err == nil {
		t.Fatal("a send on a spent budget was allowed")
	}
	if !strings.Contains(err.Error(), "rate limit") {
		t.Errorf("refused with %q, want the rate limit error", err)
	}
	if n := broadcast_limit_sequences(t, thread, feed); n != 0 {
		t.Errorf("%d log row(s) written for a refused send; the refusal must precede broadcast_log_append", n)
	}
}

// TestBroadcastCapsTheRecipientList. The rate limiter's window resets each
// minute, so it bounds sustained volume but not the size of one call — a
// single list is still a single burst of queue.db rows. The cap bounds that
// burst, and is applied before the log append for the same reason.
func TestBroadcastCapsTheRecipientList(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	thread, feed, _ := broadcast_limit_setup(t)

	// Synthetic ids: the cap is checked before the list contents are read,
	// so minting ten thousand real keys would only slow the test down.
	oversized := make([]sl.Value, broadcast_recipients_maximum+1)
	for i := range oversized {
		oversized[i] = sl.String("subscriber")
	}

	err := broadcast_limit_send(thread, feed, oversized)
	if err == nil {
		t.Fatal("a list past the cap was allowed")
	}
	if !strings.Contains(err.Error(), "too many subscribers") {
		t.Errorf("refused with %q, want the cap error", err)
	}
	if n := broadcast_limit_sequences(t, thread, feed); n != 0 {
		t.Errorf("%d log row(s) written for a capped send; the cap must precede broadcast_log_append", n)
	}
}

// TestBroadcastCapAdmitsExactlyTheMaximum. Pins the boundary as exclusive
// without fanning out ten thousand messages: with the budget already spent,
// a list AT the cap has to fall through to the rate limiter. If the cap were
// >= it would answer first, and the error would name the wrong bound.
func TestBroadcastCapAdmitsExactlyTheMaximum(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	thread, feed, app := broadcast_limit_setup(t)

	rate_limit_broadcast.spend(app.id, rate_limit_broadcast.limit)

	sized := make([]sl.Value, broadcast_recipients_maximum)
	for i := range sized {
		sized[i] = sl.String("subscriber")
	}

	err := broadcast_limit_send(thread, feed, sized)
	if err == nil {
		t.Fatal("a send on a spent budget was allowed")
	}
	if strings.Contains(err.Error(), "too many subscribers") {
		t.Error("a list of exactly broadcast_recipients_maximum was rejected by the cap; the bound must be exclusive")
	}
}

// TestBroadcastLimiterIsSeparateFromDirectSends. The two budgets must not be
// merged. Sharing would break both directions: a fan-out would exhaust the
// budget the app's direct mochi.message.send calls need, and broadcast would
// inherit a per-second budget sized for one-message-per-call — which feeds'
// RSS ingest, broadcasting once per imported item to every subscriber,
// exceeds in a burst while doing nothing wrong.
func TestBroadcastLimiterIsSeparateFromDirectSends(t *testing.T) {
	if rate_limit_broadcast == rate_limit_net_send {
		t.Fatal("broadcast shares the direct-send bucket; a fan-out can now starve mochi.message.send")
	}

	source, err := os.ReadFile("broadcast.go")
	if err != nil {
		t.Fatalf("read broadcast.go: %v", err)
	}
	if strings.Contains(string(source), "rate_limit_net_send") {
		t.Error("broadcast.go charges rate_limit_net_send; the fan-out belongs in its own bucket")
	}

	// A burst that a real RSS backfill can produce - 200 items to 40
	// subscribers - has to fit, or the limiter is refusing correct apps.
	if burst := 200 * 40; rate_limit_broadcast.limit <= burst {
		t.Errorf("budget %d cannot absorb a %d-message RSS backfill burst", rate_limit_broadcast.limit, burst)
	}
	if rate_limit_broadcast.window < 60 {
		t.Errorf("window is %ds; a backfill spread over several seconds needs the longer window to be absorbed", rate_limit_broadcast.window)
	}
}

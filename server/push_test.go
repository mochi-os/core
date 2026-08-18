// Mochi server: a push that fails once is retried, not lost.
//
// api_account_notify used to give a push exactly one attempt: it called the
// per-provider deliver function, counted a failure and moved on. A destination
// that was unreachable for a few seconds therefore lost the notification
// outright - no row, no record, nothing to retry it. On 2026-08-17 a
// seven-second FCM INTERNAL burst on yuzu did exactly that, outlasting the
// in-call retry fcm.go added for blips of a second or two.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sl "go.starlark.net/starlark"
)

// push_test_setup gives a test a data directory, the queue and user databases,
// and one verified url account pointed at the caller's server. The url provider
// is the only one whose destination a test can stand up locally and control the
// response of, so every queue test here drives that one; the queue itself is
// provider-agnostic.
func push_test_setup(t *testing.T, endpoint string) *User {
	t.Helper()
	private_endpoints_allowed(t)
	cleanup := setup_replication_test(t)
	t.Cleanup(cleanup)
	db_create()

	users := db_open("db/users.db")
	users.exec("insert into users (uid, username) values ('push-user', 'push@example.com')")
	// user_by_uid resolves the account's person entity and returns nil without
	// one, so the fixture needs it even though nothing here reads the identity.
	identity := uid()
	users.exec("insert into entities (id, private, fingerprint, user, class, name) values (?, '', ?, 'push-user', 'person', 'Push')",
		identity, fingerprint(identity))
	user := user_by_uid("push-user")
	if user == nil {
		t.Fatal("fixture user did not come back from user_by_uid")
	}
	db_user(user, "user").exec(
		"insert into accounts (id, type, identifier, created, verified) values ('acct', 'url', ?, ?, 1)",
		endpoint, now())
	return user
}

// push_test_queue inserts one queued push aimed at the fixture account, with
// the attempt count, due time and enqueue time the test wants to describe.
func push_test_queue(t *testing.T, id, endpoint string, attempts int, next_retry, created int64) {
	t.Helper()
	db_open("db/queue.db").exec(`insert into pushes
		(id, user, account, type, identifier, data, app, category, object,
			title, body, link, event, attempts, next_retry, created)
		values (?, 'push-user', 'acct', 'url', ?, '{}', 'test', 'message', 'o1',
			'Title', 'Body', '', 'e1', ?, ?, ?)`,
		id, endpoint, attempts, next_retry, created)
}

func push_test_row(t *testing.T, id string) map[string]any {
	t.Helper()
	row, _ := db_open("db/queue.db").row("select * from pushes where id=?", id)
	return row
}

// push_test_server answers every request with status, and counts the hits so a
// test can assert a delivery was attempted - or, just as importantly, was not.
func push_test_server(status int) (*httptest.Server, *int32) {
	var hits int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(status)
	}))
	return server, &hits
}

// TestPushFailureIsQueued is the finding. A transient failure has to leave
// something behind to retry; before this it left nothing at all.
func TestPushFailureIsQueued(t *testing.T) {
	server, _ := push_test_server(http.StatusInternalServerError)
	defer server.Close()
	user := push_test_setup(t, server.URL)

	push_queue_add(&Push{
		User: user, Account: "acct", Type: "url", Identifier: server.URL,
		App: "test", Category: "message", Object: "o1", Title: "Title", Body: "Body",
	})

	rows, _ := db_open("db/queue.db").rows("select * from pushes")
	if len(rows) != 1 {
		t.Fatalf("a failed push left %d queue rows, want 1: with none, the notification is gone and nothing will ever retry it", len(rows))
	}
	if got := row_int(rows[0], "next_retry"); got <= now() {
		t.Errorf("next_retry is %d, now is %d: a row due the instant it is written retries inside the outage it is waiting out", got, now())
	}
}

// TestNotifyQueuesAFailedSend is the wiring: the finding was in
// api_account_notify, so the assertion that matters is that a real notify call
// against a destination that is down leaves a row behind. Everything below
// tests the queue; this tests that the queue is reached.
func TestNotifyQueuesAFailedSend(t *testing.T) {
	server, hits := push_test_server(http.StatusInternalServerError)
	defer server.Close()
	user := push_test_setup(t, server.URL)
	permission_grant(user, "notifier", "accounts/notify")

	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("user", user)
	thread.SetLocal("app", &App{id: "notifier"})
	result, err := api_account_notify(thread,
		sl.NewBuiltin("mochi.account.notify", api_account_notify), nil, []sl.Tuple{
			{sl.String("app"), sl.String("test")},
			{sl.String("category"), sl.String("message")},
			{sl.String("object"), sl.String("o1")},
			{sl.String("title"), sl.String("Title")},
			{sl.String("body"), sl.String("Body")},
		})
	if err != nil {
		t.Fatalf("mochi.account.notify: %v", err)
	}
	if result == nil {
		t.Fatal("mochi.account.notify returned nothing")
	}
	if atomic.LoadInt32(hits) != 1 {
		t.Fatalf("the destination saw %d requests, want 1: the fixture account was not delivered to at all, so what follows would pass vacuously", *hits)
	}

	rows, _ := db_open("db/queue.db").rows("select * from pushes")
	if len(rows) != 1 {
		t.Fatalf("notify left %d queue rows after a failed send, want 1: the notification is lost with nothing to retry it", len(rows))
	}
	if got := row_string(rows[0], "account"); got != "acct" {
		t.Errorf("the queued row names account %q, want acct", got)
	}
	if got := row_string(rows[0], "title"); got != "Title" {
		t.Errorf("the queued row carries title %q, want Title: a row that loses the payload cannot be retried", got)
	}
}

// TestPushQueueRetriesUntilItLands: the row survives a failing destination,
// advances its ladder, and is delivered and cleared once the destination is
// back. This is the whole point of the queue.
func TestPushQueueRetriesUntilItLands(t *testing.T) {
	status := int32(http.StatusInternalServerError)
	var hits int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(int(atomic.LoadInt32(&status)))
	}))
	defer server.Close()
	push_test_setup(t, server.URL)

	push_test_queue(t, "p1", server.URL, 1, now()-1, now())

	if acted := push_queue_process(); acted != 1 {
		t.Fatalf("push_queue_process acted on %d rows, want 1", acted)
	}
	row := push_test_row(t, "p1")
	if row == nil {
		t.Fatal("the row was dropped on a 500: a destination that is briefly down loses the notification, which is the defect")
	}
	if got := row_int(row, "attempts"); got != 2 {
		t.Errorf("attempts = %d, want 2: without advancing, the cap never arrives and the row retries forever", got)
	}
	if got := row_int(row, "next_retry"); got <= now() {
		t.Errorf("next_retry = %d, now = %d: an unadvanced row is picked up again immediately, hammering a destination that is down", got, now())
	}

	// Destination recovers.
	atomic.StoreInt32(&status, http.StatusOK)
	db_open("db/queue.db").exec("update pushes set next_retry=? where id='p1'", now()-1)
	push_queue_process()

	if push_test_row(t, "p1") != nil {
		t.Error("the row survived a 200: a delivered push that stays queued is delivered again on every tick")
	}
	if atomic.LoadInt32(&hits) != 2 {
		t.Errorf("the destination saw %d requests, want 2 (one failing, one succeeding)", hits)
	}
}

// TestPushQueueGivesUpAtTheCap: a destination that never comes back must not
// be retried for ever. push_attempts_maximum total tries, then the row goes.
func TestPushQueueGivesUpAtTheCap(t *testing.T) {
	server, hits := push_test_server(http.StatusInternalServerError)
	defer server.Close()
	push_test_setup(t, server.URL)

	push_test_queue(t, "p1", server.URL, push_attempts_maximum-1, now()-1, now())
	push_queue_process()

	if push_test_row(t, "p1") != nil {
		t.Errorf("the row survived attempt %d of %d: a permanently dead destination accumulates rows that never clear", push_attempts_maximum, push_attempts_maximum)
	}
	if atomic.LoadInt32(hits) != 1 {
		t.Errorf("the destination saw %d requests, want 1: the last attempt is still made, only the row after it is dropped", *hits)
	}
}

// TestPushQueueDropsExpiredRows: a push is about something that just happened.
// Past push_expiry it is dropped without a further attempt, whatever attempts
// remain - buzzing a phone about a six-hour-old event is worse than silence.
func TestPushQueueDropsExpiredRows(t *testing.T) {
	server, hits := push_test_server(http.StatusOK)
	defer server.Close()
	push_test_setup(t, server.URL)

	push_test_queue(t, "old", server.URL, 1, now()-1, now()-push_expiry-1)
	push_test_queue(t, "new", server.URL, 1, now()-1, now())
	push_queue_process()

	if push_test_row(t, "old") != nil {
		t.Error("a row older than push_expiry survived: stale rows occupy the batch every tick and eventually deliver stale news")
	}
	if push_test_row(t, "new") != nil {
		t.Error("the fresh row was not delivered")
	}
	if atomic.LoadInt32(hits) != 1 {
		t.Errorf("the destination saw %d requests, want 1: the expired row must be dropped without being sent", *hits)
	}
}

// TestPushQueueSkipsRowsNotYetDue: the ladder is only worth anything if the
// due time is honoured.
func TestPushQueueSkipsRowsNotYetDue(t *testing.T) {
	server, hits := push_test_server(http.StatusOK)
	defer server.Close()
	push_test_setup(t, server.URL)

	push_test_queue(t, "later", server.URL, 1, now()+3600, now())

	if acted := push_queue_process(); acted != 0 {
		t.Errorf("push_queue_process acted on %d rows, want 0", acted)
	}
	if atomic.LoadInt32(hits) != 0 {
		t.Errorf("the destination saw %d requests, want 0: a row due in an hour was sent now", *hits)
	}
	if push_test_row(t, "later") == nil {
		t.Error("a row that is not yet due was deleted")
	}
}

// TestPushQueueDropsRowsForARemovedAccount: a user who deleted a destination
// must not have it retried at them, and the row must not outlive it.
func TestPushQueueDropsRowsForARemovedAccount(t *testing.T) {
	server, hits := push_test_server(http.StatusOK)
	defer server.Close()
	user := push_test_setup(t, server.URL)
	db_user(user, "user").exec("delete from accounts where id='acct'")

	push_test_queue(t, "orphan", server.URL, 1, now()-1, now())
	push_queue_process()

	if push_test_row(t, "orphan") != nil {
		t.Error("a queued push outlived the account it was aimed at")
	}
	if atomic.LoadInt32(hits) != 0 {
		t.Errorf("the destination saw %d requests, want 0: the account was removed, so nothing should be sent to it", *hits)
	}
}

// TestPushQueueDropsRowsForAnUnverifiedAccount is the same rule for an account
// that was un-verified rather than removed.
func TestPushQueueDropsRowsForAnUnverifiedAccount(t *testing.T) {
	server, hits := push_test_server(http.StatusOK)
	defer server.Close()
	user := push_test_setup(t, server.URL)
	db_user(user, "user").exec("update accounts set verified=0 where id='acct'")

	push_test_queue(t, "unverified", server.URL, 1, now()-1, now())
	push_queue_process()

	if push_test_row(t, "unverified") != nil {
		t.Error("a queued push survived its account losing verification")
	}
	if atomic.LoadInt32(hits) != 0 {
		t.Errorf("the destination saw %d requests, want 0", *hits)
	}
}

// TestPushQueueDropsRowsForADepartedUser: the user is read back per row, so a
// purged user must clear the queue rather than nil-deref or retry for ever.
func TestPushQueueDropsRowsForADepartedUser(t *testing.T) {
	server, hits := push_test_server(http.StatusOK)
	defer server.Close()
	push_test_setup(t, server.URL)
	db_open("db/users.db").exec("delete from users where uid='push-user'")

	push_test_queue(t, "ghost", server.URL, 1, now()-1, now())
	push_queue_process()

	if push_test_row(t, "ghost") != nil {
		t.Error("a queued push outlived the user it belonged to")
	}
	if atomic.LoadInt32(hits) != 0 {
		t.Errorf("the destination saw %d requests, want 0", *hits)
	}
}

// TestPushQueueDropsRowsForAnUnknownProvider: a row written by a build that
// knew a provider this one does not must not be retried until the cap - the
// result cannot change, and a queue that keeps such rows is a leak.
func TestPushQueueDropsRowsForAnUnknownProvider(t *testing.T) {
	server, _ := push_test_server(http.StatusOK)
	defer server.Close()
	push_test_setup(t, server.URL)

	db_open("db/queue.db").exec(`insert into pushes
		(id, user, account, type, identifier, next_retry, created)
		values ('alien', 'push-user', 'acct', 'carrier-pigeon', '', ?, ?)`,
		now()-1, now())
	push_queue_process()

	if push_test_row(t, "alien") != nil {
		t.Error("a row for a provider this build cannot deliver to was kept")
	}
}

// TestEveryNotifyProviderHasADeliverer is the drift guard the delivery table
// exists to make possible. A provider that declares the notify capability and
// has no entry is silently undeliverable: api_account_notify's !handled branch
// skips it without counting a failure, so it does not even appear in the sent
// or failed totals the caller sees.
func TestEveryNotifyProviderHasADeliverer(t *testing.T) {
	for _, p := range providers {
		if !provider_has_capability(p.Type, "notify") {
			continue
		}
		if push_deliverers[p.Type] == nil {
			t.Errorf("provider %q declares the notify capability but has no entry in push_deliverers, so notifications to it are silently discarded", p.Type)
		}
	}
}

// TestEveryDelivererHasANotifyProvider is the other direction: an entry with no
// provider behind it is dead code that reads as coverage.
func TestEveryDelivererHasANotifyProvider(t *testing.T) {
	for kind := range push_deliverers {
		if !provider_has_capability(kind, "notify") {
			t.Errorf("push_deliverers has an entry for %q, which no provider declares as notify-capable", kind)
		}
	}
}

// TestPushesTableOnAFreshInstall: db_create has to build the table, or the
// first failed push on a new server fails its insert instead of queueing.
func TestPushesTableOnAFreshInstall(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db_create()

	db := db_open("db/queue.db")
	if have, _ := db.exists("select 1 from sqlite_master where type='table' and name='pushes'"); !have {
		t.Error("db_create does not build the pushes table: every retry insert on a fresh install fails")
	}
	if have, _ := db.exists("select 1 from sqlite_master where type='index' and name='pushes_next_retry'"); !have {
		t.Error("db_create does not build pushes_next_retry: the due query scans the table on every tick")
	}
}

// TestPushesTableOnAnUpgrade is the other half, and the one that matters for
// every server already running: a schema-7 install has no pushes table, so the
// migration has to add it. Driven through db_upgrade rather than by calling
// db_upgrade_8 directly, so a missing `case 8:` fails here too.
func TestPushesTableOnAnUpgrade(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db_create()

	// Wind the install back to what a pre-migration server looks like.
	db := db_open("db/queue.db")
	db.exec("drop index if exists pushes_next_retry")
	db.exec("drop table if exists pushes")
	setting_set("schema", "7")

	db_upgrade()

	if have, _ := db.exists("select 1 from sqlite_master where type='table' and name='pushes'"); !have {
		t.Error("db_upgrade left a schema-7 install without the pushes table: every existing server would fail its retry inserts")
	}
	if got := setting_get("schema", ""); got != "8" {
		t.Errorf("schema is %q after the upgrade, want 8", got)
	}
}

// TestPushManagerIsStarted. The queue is inert without it: rows accumulate,
// nothing retries them, and the expiry sweep never runs either - strictly worse
// than the one-shot behaviour it replaces. Checked against main.go's source
// because starting the real manager in a test would leave a ticker goroutine
// running for the rest of the run.
func TestPushManagerIsStarted(t *testing.T) {
	source, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("read main.go: %v", err)
	}
	if !strings.Contains(string(source), "go push_manager()") {
		t.Error("main.go does not start push_manager: queued pushes are written and never retried")
	}
}

// TestPushQueueSendsConcurrently. Serially, one unreachable destination holds
// the whole batch for its timeout, and nobody else's notification is retried
// until it gives up. The sink here blocks until every request has arrived, so
// it can only answer at all if the sends overlap.
func TestPushQueueSendsConcurrently(t *testing.T) {
	const rows = push_queue_workers
	arrived := make(chan struct{}, rows)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		arrived <- struct{}{}
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	// Every blocked handler has to be let go before httptest.Server.Close,
	// which waits for outstanding requests - including on the failure paths
	// below, which return without having released anything.
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer server.Close()
	defer unblock()
	push_test_setup(t, server.URL)

	for i := 0; i < rows; i++ {
		push_test_queue(t, "p"+itoa(i), server.URL, 1, now()-1, now())
	}

	done := make(chan int, 1)
	go func() { done <- push_queue_process() }()

	// Every request has to be in flight at once. Serially the second would
	// never arrive, because the first is still blocked here.
	for i := 0; i < rows; i++ {
		select {
		case <-arrived:
		case <-time.After(10 * time.Second):
			t.Fatalf("only %d of %d sends were in flight: the batch is serialised, so one slow destination stalls every other user's retries", i, rows)
		}
	}
	unblock()

	select {
	case acted := <-done:
		if acted != rows {
			t.Errorf("push_queue_process acted on %d rows, want %d", acted, rows)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("push_queue_process did not return after every send completed")
	}
}

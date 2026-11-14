// Mochi server: a push that fails once is retried, not lost.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"fmt"
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
// and one verified url account pointed at the caller's server - the only
// provider whose destination a test can stand up locally. The queue is
// provider-agnostic.
func push_test_setup(t *testing.T, endpoint string) *User {
	t.Helper()
	private_endpoints_allowed(t)
	setup_replication_test(t)
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
	push_test_quiet(t)
	return user
}

// push_test_user adds a user, or another verified url account of one, aimed
// at endpoint, for a test that needs pushes of several users or accounts.
func push_test_user(t *testing.T, name, account, endpoint string) *User {
	t.Helper()
	users := db_open("db/users.db")
	if have, _ := users.exists("select 1 from users where uid=?", name); !have {
		users.exec("insert into users (uid, username) values (?, ?)", name, name+"@example.com")
		identity := uid()
		users.exec("insert into entities (id, private, fingerprint, user, class, name) values (?, '', ?, ?, 'person', ?)",
			identity, fingerprint(identity), name, name)
	}
	user := user_by_uid(name)
	if user == nil {
		t.Fatalf("user %s did not come back from user_by_uid", name)
	}
	db_user(user, "user").exec(
		"insert into accounts (id, type, identifier, created, verified) values (?, 'url', ?, ?, 1)",
		account, endpoint, now())
	return user
}

// push_test_queue_for inserts one queued push of user's to account.
func push_test_queue_for(t *testing.T, id, user, account, endpoint string, attempts int, next_retry, created int64) {
	t.Helper()
	db_open("db/queue.db").exec(`insert into pushes
		(id, user, account, type, identifier, data, app, category, object,
			title, body, link, event, attempts, next_retry, created)
		values (?, ?, ?, 'url', ?, '{}', 'test', 'message', 'o1',
			'Title', 'Body', '', 'e1', ?, ?, ?)`,
		id, user, account, endpoint, attempts, next_retry, created)
}

// push_test_fill takes every place of a kind, as though that many pushes of
// other users were in flight, and gives back at the test's end what it has not
// already. free gives one back, as a send ending does.
func push_test_fill(t *testing.T, retry bool) (free func()) {
	t.Helper()
	limit := push_senders
	if retry {
		limit = push_retry_senders
	}
	var held []string
	for i := 0; i < limit; i++ {
		name := "filler-" + itoa(i)
		if !push_flight.take(name, name, retry) {
			t.Fatalf("place %d of %d was not free", i, limit)
		}
		held = append(held, name)
	}
	t.Cleanup(func() {
		for _, name := range held {
			push_flight.release(name, name, retry, false)
		}
	})
	return func() {
		name := held[len(held)-1]
		held = held[:len(held)-1]
		push_flight.release(name, name, retry, true)
	}
}

// push_test_held reports the places held, of every kind.
func push_test_held() (fresh, retry, users, accounts int) {
	push_flight.lock.Lock()
	defer push_flight.lock.Unlock()
	return push_flight.fresh, push_flight.retry, len(push_flight.users), len(push_flight.accounts)
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

// push_test_pass makes one pass of the queue and waits for the sends it
// started to settle, as a pass no longer does itself.
func push_test_pass() int {
	acted := push_queue_process()
	push_sending.Wait()
	return acted
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

	push_settle("", 0, &Push{
		User: user, Account: "acct", Type: "url", Identifier: server.URL,
		App: "test", Category: "message", Object: "o1", Title: "Title", Body: "Body",
	}, false, false, true)

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

	if acted := push_test_pass(); acted != 1 {
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
	push_test_pass()

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
	push_test_pass()

	if push_test_row(t, "p1") != nil {
		t.Errorf("the row survived attempt %d of %d: a permanently dead destination accumulates rows that never clear", push_attempts_maximum, push_attempts_maximum)
	}
	if atomic.LoadInt32(hits) != 1 {
		t.Errorf("the destination saw %d requests, want 1: the last attempt is still made, only the row after it is dropped", *hits)
	}
}

// TestPushQueueDropsExpiredRows: a push is about something that just happened.
// Past push_expiry it is not sent, whatever attempts remain - buzzing a phone
// about a six-hour-old event is worse than silence - and the manager's tick
// drops it.
func TestPushQueueDropsExpiredRows(t *testing.T) {
	server, hits := push_test_server(http.StatusOK)
	defer server.Close()
	push_test_setup(t, server.URL)

	push_test_queue(t, "old", server.URL, 1, now()-1, now()-push_expiry-1)
	push_test_queue(t, "new", server.URL, 1, now()-1, now())
	push_test_pass()

	if push_test_row(t, "new") != nil {
		t.Error("the fresh row was not delivered")
	}
	if atomic.LoadInt32(hits) != 1 {
		t.Errorf("the destination saw %d requests, want 1: the expired row must not be sent", *hits)
	}

	push_queue_expire()
	if push_test_row(t, "old") != nil {
		t.Error("a row older than push_expiry survived the expiry: stale rows are read by every pass")
	}
}

// TestPushQueueSkipsRowsNotYetDue: the ladder is only worth anything if the
// due time is honoured.
func TestPushQueueSkipsRowsNotYetDue(t *testing.T) {
	server, hits := push_test_server(http.StatusOK)
	defer server.Close()
	push_test_setup(t, server.URL)

	push_test_queue(t, "later", server.URL, 1, now()+3600, now())

	if acted := push_test_pass(); acted != 0 {
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
	push_test_pass()

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
	push_test_pass()

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
	push_test_pass()

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
	push_test_pass()

	if push_test_row(t, "alien") != nil {
		t.Error("a row for a provider this build cannot deliver to was kept")
	}
}

// TestEveryNotifyProviderHasADeliverer: a notify provider with no entry in the
// table is silently undeliverable - api_account_notify's !handled branch skips
// it without counting a failure, so it appears in no total the caller sees.
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
	setup_replication_test(t)
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
	setup_replication_test(t)
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
	// Against schema_version rather than a literal: the assertion is that the
	// upgrade ran to completion, and pinning the number here means every later
	// migration fails this test for no reason.
	if got, want := setting_get("schema", ""), fmt.Sprintf("%d", schema_version); got != want {
		t.Errorf("schema is %q after the upgrade, want %s", got, want)
	}
}

// TestPushManagerIsStarted. Without it rows accumulate and nothing retries or
// expires them. Checked against main.go's source: starting the real manager
// here would leave a ticker goroutine running for the rest of the run.
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
// it can only answer at all if the sends overlap, and the pass returns without
// waiting for any of them.
func TestPushQueueSendsConcurrently(t *testing.T) {
	const rows = push_retry_senders
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
		name := "user" + itoa(i)
		push_test_user(t, name, "acct"+itoa(i), server.URL)
		push_test_queue_for(t, "p"+itoa(i), name, "acct"+itoa(i), server.URL, 1, now()-1, now())
	}

	done := make(chan int, 1)
	go func() { done <- push_queue_process() }()
	select {
	case acted := <-done:
		if acted != rows {
			t.Errorf("push_queue_process acted on %d rows, want %d", acted, rows)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("push_queue_process waited for its sends: the manager is held for as long as the slowest destination")
	}

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
	push_sending.Wait()
	if left, _ := db_open("db/queue.db").rows("select * from pushes"); len(left) != 0 {
		t.Errorf("%d rows left after every send landed, want 0", len(left))
	}
}

// push_test_hang is a destination that holds every request until the test
// ends, and reports each as it arrives.
func push_test_hang(t *testing.T, size int) (string, chan struct{}) {
	t.Helper()
	arrived := make(chan struct{}, size)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		arrived <- struct{}{}
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	// Registered after push_test_setup's check, so it runs first: the sends
	// end and give their places back before the check counts them.
	t.Cleanup(func() {
		close(release)
		server.Close()
		push_sending.Wait()
	})
	return server.URL, arrived
}

// push_test_arrive waits for count requests to reach a push_test_hang
// destination.
func push_test_arrive(t *testing.T, arrived chan struct{}, count int, why string) {
	t.Helper()
	for i := 0; i < count; i++ {
		select {
		case <-arrived:
		case <-time.After(10 * time.Second):
			t.Fatalf("%d of %d requests arrived: %s", i, count, why)
		}
	}
}

// TestPushRetriesLeaveNewPushesTheirPlaces: retries have places of their own. A
// queue full of destinations that hang holds none of the places a new
// notification is sent from, so it goes out at once rather than waiting for
// them.
func TestPushRetriesLeaveNewPushesTheirPlaces(t *testing.T) {
	fresh, hits := push_test_server(http.StatusOK)
	defer fresh.Close()
	user := push_test_setup(t, fresh.URL)
	hung, arrived := push_test_hang(t, push_senders)

	// Enough failing destinations to take every new push's place too, were
	// retries to draw on them: no user is held back by its own share.
	for i := 0; i < push_senders; i++ {
		name := "user" + itoa(i/push_user_senders)
		account := "acct" + itoa(i)
		push_test_user(t, name, account, hung)
		push_test_queue_for(t, "hung"+itoa(i), name, account, hung, 1, now()-1, now())
	}
	push_queue_process()
	push_test_arrive(t, arrived, push_retry_senders, "the retries to the hanging destinations did not start")

	push_test_notify(t, user, true)
	deadline := time.Now().Add(5 * time.Second)
	for atomic.LoadInt32(hits) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("a new notification waited behind retries to hanging destinations: they hold the places it is sent from")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestPushUserHasAtMostItsShare: one user's pushes hold at most
// push_user_senders places, so their slow destinations leave the rest of the
// server's places to everyone else; their next push waits as a row.
func TestPushUserHasAtMostItsShare(t *testing.T) {
	// Set up before the hanging destination, whose clean-up must run first.
	user := push_test_setup(t, "http://unused.invalid/")
	hung, arrived := push_test_hang(t, push_senders)
	db_user(user, "user").exec("update accounts set identifier=? where id='acct'", hung)
	for i := 1; i <= push_user_senders; i++ {
		push_test_user(t, "push-user", "acct"+itoa(i), hung)
	}
	other, hits := push_test_server(http.StatusOK)
	defer other.Close()
	neighbour := push_test_user(t, "neighbour", "neighbour", other.URL)

	result := push_test_notify(t, user, true)
	if got := fmt.Sprint(result["queued"]); got != itoa(push_user_senders+1) {
		t.Fatalf("notify answered %v, want queued %d", result, push_user_senders+1)
	}
	push_test_arrive(t, arrived, push_user_senders, "the user's own pushes did not start")
	if rows, _ := db_open("db/queue.db").rows("select * from pushes"); len(rows) != 1 {
		t.Errorf("%d rows waiting, want 1: a user's pushes beyond their share wait for one of their own to end", len(rows))
	}

	push_test_notify(t, neighbour, true)
	deadline := time.Now().Add(5 * time.Second)
	for atomic.LoadInt32(hits) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("another user's notification waited behind the first user's slow destinations")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestPushAccountSendsOneAtATime: an account has one push in flight, so its
// pushes arrive in the order they were raised, and a provider's first send,
// which makes a Matrix notices room, is never made twice at once. Each push's
// end wakes the manager for the next, the third included, which a pass found
// waiting.
func TestPushAccountSendsOneAtATime(t *testing.T) {
	arrived := make(chan struct{}, 4)
	release := make(chan struct{}, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		arrived <- struct{}{}
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	defer func() { release <- struct{}{}; release <- struct{}{}; release <- struct{}{} }()
	user := push_test_setup(t, server.URL)

	push_test_notify(t, user, true)
	push_test_arrive(t, arrived, 1, "the first push did not start")
	push_test_notify(t, user, true)
	push_test_notify(t, user, true)
	rows, _ := db_open("db/queue.db").rows("select * from pushes")
	if len(rows) != 2 {
		t.Fatalf("%d rows waiting, want the second and third pushes: sends to one account at once", len(rows))
	}

	for i := 2; i <= 3; i++ {
		release <- struct{}{}
		push_sending.Wait()
		select {
		case <-push_wake:
		default:
			t.Fatalf("push %d's account came free and the manager was not woken: it waits up to 30 s for the tick", i)
		}
		if acted := push_queue_process(); acted != 1 {
			t.Fatalf("the pass for push %d acted on %d rows, want 1: one at a time to an account", i, acted)
		}
		push_test_arrive(t, arrived, 1, "the waiting push was not sent once the account was free")
	}
}

// TestPushPlacesWaitWakesWhenAPlaceIsFree: a push left as a row while a place
// came free for it wakes the manager at once, rather than waiting for the
// tick; with none free, the next to come free wakes it.
func TestPushPlacesWaitWakesWhenAPlaceIsFree(t *testing.T) {
	push_test_setup(t, "http://unused.invalid/")
	push_flight.wait("push-user", "acct", false)
	select {
	case <-push_wake:
	default:
		t.Error("a push left waiting while a place was free did not wake the manager")
	}
}

// TestPushQueueSendsNewPushesBeforeRetries: a push left for want of a place is
// read apart from the retries, so a batch's worth of failing destinations
// never keeps it out of the pass.
func TestPushQueueSendsNewPushesBeforeRetries(t *testing.T) {
	failing, _ := push_test_server(http.StatusInternalServerError)
	defer failing.Close()
	fresh, hits := push_test_server(http.StatusOK)
	defer fresh.Close()
	push_test_setup(t, fresh.URL)
	push_test_user(t, "other", "other", failing.URL)

	for i := 0; i < push_queue_batch; i++ {
		push_test_queue_for(t, "retry"+itoa(i), "other", "other", failing.URL, 1, now()-100, now())
	}
	push_test_queue(t, "new", fresh.URL, 0, now()-1, now())
	push_test_pass()

	if got := atomic.LoadInt32(hits); got != 1 {
		t.Fatalf("the new push's destination saw %d requests, want 1: a full batch of older retries kept it out of the pass", got)
	}
	if push_test_row(t, "new") != nil {
		t.Error("the new push's row survived its delivery")
	}
}

// TestNotifySkipsADisabledAccount: enabled is the account's "Notify by default"
// switch. The settings app stored it and showed it, and nothing read it, so a
// switched-off account was delivered to exactly like a switched-on one.
// TestNotifyQueuesAFailedSend is the positive control: the same fixture with
// the default enabled=1 reaches the destination once.
func TestNotifySkipsADisabledAccount(t *testing.T) {
	server, hits := push_test_server(http.StatusOK)
	defer server.Close()
	user := push_test_setup(t, server.URL)
	db_user(user, "user").exec("update accounts set enabled=0 where id='acct'")
	permission_grant(user, "notifier", "accounts/notify")

	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("user", user)
	thread.SetLocal("app", &App{id: "notifier"})
	if _, err := api_account_notify(thread,
		sl.NewBuiltin("mochi.account.notify", api_account_notify), nil, []sl.Tuple{
			{sl.String("app"), sl.String("test")},
			{sl.String("category"), sl.String("message")},
			{sl.String("object"), sl.String("o1")},
			{sl.String("title"), sl.String("Title")},
			{sl.String("body"), sl.String("Body")},
		}); err != nil {
		t.Fatalf("mochi.account.notify: %v", err)
	}
	if got := atomic.LoadInt32(hits); got != 0 {
		t.Fatalf("the destination saw %d requests, want 0: a disabled account was delivered to", got)
	}
	if rows, _ := db_open("db/queue.db").rows("select * from pushes"); len(rows) != 0 {
		t.Fatalf("notify queued %d rows for a disabled account, want 0", len(rows))
	}
}

// TestPushQueueDropsRowsForAnUnconfiguredProvider: an FCM destination on a
// server with no service account cannot be delivered to until the operator
// supplies one, so a queued push to it is not walked through five attempts and
// a give-up mail; it is dropped on the first pass, and the account row stays
// for when the setting is filled.
func TestPushQueueDropsRowsForAnUnconfiguredProvider(t *testing.T) {
	server, _ := push_test_server(http.StatusOK)
	defer server.Close()
	user := push_test_setup(t, server.URL)
	db_user(user, "user").exec(
		"insert into accounts (id, type, identifier, data, created, verified) values ('fcm', 'fcm', 'probe', '{\"token\":\"t\"}', ?, 1)", now())
	db_open("db/queue.db").exec(`insert into pushes
		(id, user, account, type, identifier, data, next_retry, created)
		values ('unconfigured', 'push-user', 'fcm', 'fcm', 'probe', '{"token":"t"}', ?, ?)`,
		now()-1, now())
	push_test_pass()

	if push_test_row(t, "unconfigured") != nil {
		t.Error("a push to a provider this server has not configured was kept for retry: five doomed attempts and a give-up mail follow")
	}
	if have, _ := db_user(user, "user").exists("select 1 from accounts where id='fcm'"); !have {
		t.Error("the account row was removed: it must stay, and deliver once the operator configures FCM")
	}
}

// TestNotifySkipsAnUnconfiguredProvider: the first attempt, from
// mochi.account.notify, queued or not, sends and queues nothing for such a
// destination, and the other destinations of the same call are unaffected.
func TestNotifySkipsAnUnconfiguredProvider(t *testing.T) {
	for _, queue := range []bool{false, true} {
		t.Run(fmt.Sprint("queue=", queue), func(t *testing.T) {
			server, hits := push_test_server(http.StatusOK)
			defer server.Close()
			user := push_test_setup(t, server.URL)
			push_test_quiet(t)
			db_user(user, "user").exec(
				"insert into accounts (id, type, identifier, data, created, verified) values ('fcm', 'fcm', 'probe', '{\"token\":\"t\"}', ?, 1)", now())

			result := push_test_notify(t, user, queue)
			push_sending.Wait()
			if atomic.LoadInt32(hits) != 1 {
				t.Fatalf("the url destination saw %d requests, want 1: the configured destination must still be delivered to", *hits)
			}
			if queue && fmt.Sprint(result["queued"]) != "1" {
				t.Errorf("notify answered %v, want queued 1: the unconfigured destination was counted", result)
			}
			rows, _ := db_open("db/queue.db").rows("select * from pushes")
			if len(rows) != 0 {
				t.Errorf("notify queued %d row(s) for the unconfigured FCM destination, want 0", len(rows))
			}
		})
	}
}

// TestPushQueueHoldsBackABatchItCannotRecord. A pass that delivered its batch
// stops at the first refused write, and used to leave every later delivered
// push due, so the next tick, 30 s on, buzzed the same phones again. The batch
// is claimed before it is sent, so an unrecorded push waits out the claim
// instead; recorded, it is gone.
func TestPushQueueHoldsBackABatchItCannotRecord(t *testing.T) {
	server, hits := push_test_server(http.StatusOK)
	defer server.Close()
	push_test_setup(t, server.URL)
	for _, id := range []string{"p1", "p2", "p3"} {
		push_test_user(t, "user-"+id, "acct-"+id, server.URL)
		push_test_queue_for(t, id, "user-"+id, "acct-"+id, server.URL, 1, now()-1, now())
	}
	// The claim lands; the first outcome after the deliveries is refused.
	write := push_write
	push_write = func(db *DB, query string, values ...any) error {
		if strings.HasPrefix(query, "delete from pushes where id=") {
			return fmt.Errorf("database is locked")
		}
		return write(db, query, values...)
	}
	t.Cleanup(func() { push_write = write })

	push_test_pass()
	if got := atomic.LoadInt32(hits); got != 3 {
		t.Fatalf("the destination saw %d requests, want the batch's 3", got)
	}
	for _, id := range []string{"p1", "p2", "p3"} {
		row := push_test_row(t, id)
		if row == nil {
			t.Fatalf("%s is gone though its outcome was never recorded", id)
		}
		if due := row_int(row, "next_retry"); due < now()+push_claim-60 {
			t.Errorf("%s is due again at %d, %d s from now: its delivered push goes out again on the next pass", id, due, due-now())
		}
	}
	if acted := push_test_pass(); acted != 0 {
		t.Errorf("the next pass acted on %d rows, want 0", acted)
	}
	if got := atomic.LoadInt32(hits); got != 3 {
		t.Errorf("the destination saw %d requests after the next pass, want 3: the unrecorded pushes were sent again", got)
	}

	push_write = write
	db_open("db/queue.db").exec("update pushes set next_retry=?", now()-1)
	push_test_pass()
	for _, id := range []string{"p1", "p2", "p3"} {
		if push_test_row(t, id) != nil {
			t.Errorf("%s survived a recorded delivery", id)
		}
	}
}

// TestPushQueueGivesBackPlacesWhenTheClaimIsRefused: a pass that takes places
// for its rows and then cannot claim them sends nothing, and gives every place
// back without waking the manager, which would only start the same pass again.
func TestPushQueueGivesBackPlacesWhenTheClaimIsRefused(t *testing.T) {
	server, hits := push_test_server(http.StatusOK)
	defer server.Close()
	push_test_setup(t, server.URL)
	push_test_user(t, "other", "other", server.URL)
	push_test_queue(t, "new", server.URL, 0, now()-1, now())
	push_test_queue_for(t, "retry", "other", "other", server.URL, 1, now()-1, now())
	// A second push to the account in flight is left waiting, so the pass
	// records that a push waits for a place.
	push_test_queue(t, "waiting", server.URL, 1, now()-1, now())
	write := push_write
	push_write = func(db *DB, query string, values ...any) error {
		if strings.HasPrefix(query, "update pushes set next_retry=") {
			return fmt.Errorf("database is locked")
		}
		return write(db, query, values...)
	}
	t.Cleanup(func() { push_write = write })

	if acted := push_test_pass(); acted != 0 {
		t.Errorf("the pass acted on %d rows after its claim was refused, want 0", acted)
	}
	if got := atomic.LoadInt32(hits); got != 0 {
		t.Errorf("the destination saw %d requests, want 0: a push sent unclaimed is sent again by the next pass", got)
	}
	if fresh, retry, users, accounts := push_test_held(); fresh+retry+users+accounts != 0 {
		t.Errorf("%d new and %d retry places still held after the refused claim, want none", fresh, retry)
	}
	select {
	case <-push_wake:
		t.Error("giving back its own places woke the manager: a pass that cannot claim loops straight back into itself")
	default:
	}
}

// TestPushQueueGivesBackPlacesOnAPanic: a place taken for a row whose user
// cannot be read, here a lookup that panics, goes back with the pass. Kept, the
// row is due again on every pass, each one losing a place, until the queue
// sends nothing more.
func TestPushQueueGivesBackPlacesOnAPanic(t *testing.T) {
	push_test_setup(t, "http://unused.invalid/")
	push_test_queue(t, "fault", "http://unused.invalid/", 1, now()-1, now())
	lookup := push_lookup
	push_lookup = func(string) *User { panic("probe fault in a user lookup") }
	t.Cleanup(func() { push_lookup = lookup })

	guard("push queue", nil, func() { push_queue_process() })
	if fresh, retry, users, accounts := push_test_held(); fresh+retry+users+accounts != 0 {
		t.Errorf("a panic in a pass left %d new and %d retry places held, want none", fresh, retry)
	}
}

// push_test_notify calls mochi.account.notify for the fixture user as an app
// holding accounts/notify, with queue as given.
func push_test_notify(t *testing.T, user *User, queue bool) map[string]any {
	t.Helper()
	result, err := push_test_call(user, queue)
	if err != nil {
		t.Fatalf("mochi.account.notify: %v", err)
	}
	return result
}

// push_test_call is push_test_notify for a goroutine other than the test's,
// which must not call t.Fatalf.
func push_test_call(user *User, queue bool) (map[string]any, error) {
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
			{sl.String("queue"), sl.Bool(queue)},
		})
	if err != nil {
		return nil, err
	}
	decoded, _ := sl_decode(result).(map[string]any)
	return decoded, nil
}

// push_test_quiet empties push_wake, and once the test ends waits for its sends
// and checks that every place has come back, so neither a send nor a lost place
// leaks into the next test.
func push_test_quiet(t *testing.T) {
	t.Helper()
	select {
	case <-push_wake:
	default:
	}
	t.Cleanup(func() {
		push_sending.Wait()
		if fresh, retry, users, accounts := push_test_held(); fresh+retry+users+accounts != 0 {
			t.Errorf("places held at the end of the test: %d new, %d retries, %d users, %d accounts", fresh, retry, users, accounts)
			push_flight.lock.Lock()
			push_flight.fresh, push_flight.retry = 0, 0
			push_flight.users, push_flight.accounts = map[string]int{}, map[string]bool{}
			push_flight.lock.Unlock()
		}
		push_flight.lock.Lock()
		push_flight.backlog = false
		push_flight.lock.Unlock()
		select {
		case <-push_wake:
		default:
		}
	})
}

// TestNotifyQueuedDoesNotHoldTheCaller is the finding: a notify sent its push
// to each device in turn on the caller's thread, so a chat message or a
// comment waited a round trip per device, and 10 s or more for one that did not
// answer. Queued, the caller goes on while the destination still holds the
// request, and the push lands and clears its row.
func TestNotifyQueuedDoesNotHoldTheCaller(t *testing.T) {
	arrived := make(chan struct{}, 1)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		arrived <- struct{}{}
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer server.Close()
	defer unblock()
	user := push_test_setup(t, server.URL)
	push_test_quiet(t)

	type answer struct {
		result map[string]any
		err    error
	}
	returned := make(chan answer, 1)
	go func() {
		result, err := push_test_call(user, true)
		returned <- answer{result, err}
	}()
	select {
	case a := <-returned:
		if a.err != nil {
			t.Fatalf("mochi.account.notify: %v", a.err)
		}
		if got := a.result["queued"]; fmt.Sprint(got) != "1" {
			t.Errorf("notify answered %v, want queued 1", a.result)
		}
	case <-time.After(5 * time.Second):
		unblock()
		t.Fatal("a queued notify waited for the destination to answer: the caller is held as it was inline")
	}
	select {
	case <-arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("the queued push never reached the destination")
	}
	if rows, _ := db_open("db/queue.db").rows("select * from pushes"); len(rows) != 0 {
		t.Errorf("%d rows while the push is in flight, want 0: a push that finds a place writes to the queue only if it fails", len(rows))
	}

	unblock()
	push_sending.Wait()
	if rows, _ := db_open("db/queue.db").rows("select * from pushes"); len(rows) != 0 {
		t.Errorf("%d rows left after the push landed, want 0", len(rows))
	}
}

// TestNotifyQueuedRetriesAFailure: a queued push that fails is retried from its
// row like an inline one.
func TestNotifyQueuedRetriesAFailure(t *testing.T) {
	server, hits := push_test_server(http.StatusInternalServerError)
	defer server.Close()
	user := push_test_setup(t, server.URL)
	push_test_quiet(t)

	push_test_notify(t, user, true)
	push_sending.Wait()
	if got := atomic.LoadInt32(hits); got != 1 {
		t.Fatalf("the destination saw %d requests, want 1", got)
	}
	rows, _ := db_open("db/queue.db").rows("select * from pushes")
	if len(rows) != 1 {
		t.Fatalf("%d rows after a failed queued push, want 1 to retry it", len(rows))
	}
	if got := row_int(rows[0], "attempts"); got != 1 {
		t.Errorf("the row counts %d attempts, want 1", got)
	}
	if due := row_int(rows[0], "next_retry"); due <= now() || due > now()+push_claim-60 {
		t.Errorf("the row is due in %d s, want the ladder's first step", due-now())
	}
}

// TestNotifyQueuedWithEveryPlaceTaken: with every place in flight, a queued
// push is left due now, rather than holding the caller or opening a connection
// past the bound, and the manager is not woken for a pass that could send
// nothing; the next place to come free wakes it, whether the push or a pass
// was the one to find none free.
func TestNotifyQueuedWithEveryPlaceTaken(t *testing.T) {
	server, hits := push_test_server(http.StatusOK)
	defer server.Close()
	user := push_test_setup(t, server.URL)
	free := push_test_fill(t, false)

	push_test_notify(t, user, true)
	push_sending.Wait()
	if got := atomic.LoadInt32(hits); got != 0 {
		t.Fatalf("the destination saw %d requests with every place taken, want 0", got)
	}
	rows, _ := db_open("db/queue.db").rows("select * from pushes")
	if len(rows) != 1 || row_int(rows[0], "next_retry") > now() {
		t.Fatalf("rows %v, want one due now for the manager", rows)
	}
	select {
	case <-push_wake:
		t.Error("the manager was woken with no place free: its pass could send nothing")
	default:
	}

	// A pass that finds every place taken records the waiting push too.
	push_flight.lock.Lock()
	push_flight.backlog = false
	push_flight.lock.Unlock()
	if acted := push_test_pass(); acted != 0 {
		t.Fatalf("a pass with every place taken acted on %d rows, want 0", acted)
	}
	free()
	select {
	case <-push_wake:
	default:
		t.Error("a place came free and the manager was not woken: the waiting push waits up to 30 s for its tick")
	}

	push_test_pass()
	if got := atomic.LoadInt32(hits); got != 1 {
		t.Errorf("the manager's pass sent %d requests, want 1", got)
	}
}

// TestNotifyQueuedSendsOnTheCallerWhenNothingCanWait: with every place taken
// and queue.db refusing the row, the push is sent on the caller, as an
// unqueued one is, rather than lost.
func TestNotifyQueuedSendsOnTheCallerWhenNothingCanWait(t *testing.T) {
	server, hits := push_test_server(http.StatusOK)
	defer server.Close()
	user := push_test_setup(t, server.URL)
	push_test_fill(t, false)
	write := push_write
	push_write = func(db *DB, query string, values ...any) error {
		if strings.HasPrefix(query, "insert into pushes") {
			return fmt.Errorf("database is locked")
		}
		return write(db, query, values...)
	}
	t.Cleanup(func() { push_write = write })

	push_test_notify(t, user, true)
	if got := atomic.LoadInt32(hits); got != 1 {
		t.Errorf("the destination saw %d requests by the time notify returned, want 1: a push with neither a place nor a row is lost", got)
	}
}

// push_test_subscription makes the fixture account a Web Push subscription of
// kind (unifiedpush or browser) whose push service answers status: 410 for one
// its app or browser has dropped.
func push_test_subscription(t *testing.T, user *User, kind string, status int) *int32 {
	t.Helper()
	setup_vapid(t)
	auth, p256dh := generate_subscription_keys(t)
	server, hits := push_test_server(status)
	t.Cleanup(server.Close)
	data := fmt.Sprintf(`{"endpoint":%q,"auth":%q,"p256dh":%q}`, server.URL+"/push/abc", auth, p256dh)
	db_user(user, "user").exec("update accounts set type=?, identifier=?, data=? where id='acct'", kind, server.URL+"/push/abc", data)
	return hits
}

// TestNotifyRetiresAGoneSubscription: a distributor's 410 deletes the account,
// queued or inline, and leaves nothing to retry. The Android client leaves
// such accounts behind on uninstall, a data clear or a reinstall.
func TestNotifyRetiresAGoneSubscription(t *testing.T) {
	for _, c := range []struct {
		kind  string
		queue bool
	}{{"unifiedpush", false}, {"unifiedpush", true}, {"browser", false}, {"browser", true}} {
		kind, queue := c.kind, c.queue
		t.Run(fmt.Sprint(kind, " queue=", queue), func(t *testing.T) {
			user := push_test_setup(t, "http://unused.invalid/")
			push_test_quiet(t)
			hits := push_test_subscription(t, user, kind, http.StatusGone)

			push_test_notify(t, user, queue)
			push_sending.Wait()
			if got := atomic.LoadInt32(hits); got != 1 {
				t.Fatalf("the distributor saw %d requests, want 1", got)
			}
			if have, _ := db_user(user, "user").exists("select 1 from accounts where id='acct'"); have {
				t.Error("the account survived its distributor's 410: every later notification is sent to it again")
			}
			if rows, _ := db_open("db/queue.db").rows("select * from pushes"); len(rows) != 0 {
				t.Errorf("%d rows left to retry a subscription that is gone, want 0", len(rows))
			}
		})
	}
}

// TestNotifyKeepsABrowserSubscriptionThatFailsOnce: a browser push that fails
// for a reason that can pass, here a push service's 500, is retried like any
// other, queued or inline. Inline, every failure used to delete the browser's
// subscription, so a push service's bad minute turned push off for good.
func TestNotifyKeepsABrowserSubscriptionThatFailsOnce(t *testing.T) {
	for _, queue := range []bool{false, true} {
		t.Run(fmt.Sprint("queue=", queue), func(t *testing.T) {
			user := push_test_setup(t, "http://unused.invalid/")
			push_test_quiet(t)
			hits := push_test_subscription(t, user, "browser", http.StatusInternalServerError)

			push_test_notify(t, user, queue)
			push_sending.Wait()
			if got := atomic.LoadInt32(hits); got != 1 {
				t.Fatalf("the push service saw %d requests, want 1", got)
			}
			if have, _ := db_user(user, "user").exists("select 1 from accounts where id='acct'"); !have {
				t.Error("one failed push deleted the browser's subscription")
			}
			if rows, _ := db_open("db/queue.db").rows("select * from pushes where attempts=1"); len(rows) != 1 {
				t.Errorf("%d rows to retry the failed push, want 1", len(rows))
			}
		})
	}
}

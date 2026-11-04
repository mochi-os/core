// Mochi server: queue.db under concurrent and contended writes
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"errors"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sqlitedrv "github.com/ncruces/go-sqlite3/driver"
)

// queue_test_lock takes queue.db's write lock from a connection outside the
// server's pool, as another process or a long write would hold it, and
// returns the release.
func queue_test_lock(t *testing.T) func() {
	t.Helper()
	other, err := sqlitedrv.Open(filepath.Join(data_dir, "db/queue.db"), nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	conn, err := other.Conn(context.Background())
	if err != nil {
		t.Fatalf("connection: %v", err)
	}
	if _, err := conn.ExecContext(context.Background(), "begin immediate"); err != nil {
		t.Fatalf("begin immediate: %v", err)
	}
	var once sync.Once
	release := func() {
		once.Do(func() {
			conn.ExecContext(context.Background(), "commit")
			conn.Close()
			other.Close()
		})
	}
	t.Cleanup(release)
	return release
}

// queue_test_timeout shortens queue_add_timeout for the test. The driver's
// 60 s busy timeout is left as it ships: the bound has to cut that wait short.
func queue_test_timeout(t *testing.T, timeout time.Duration) {
	t.Helper()
	previous := queue_add_timeout
	queue_add_timeout = timeout
	t.Cleanup(func() { queue_add_timeout = previous })
}

// queue_test_refuse makes queue.db refuse the writes a trigger clause names, as
// a failing write would, and returns its removal.
func queue_test_refuse(t *testing.T, db *DB, clause string) func() {
	t.Helper()
	db.exec("create trigger refuse " + clause + " begin select raise(abort, 'refused by the test'); end")
	var once sync.Once
	remove := func() { once.Do(func() { db.exec("drop trigger if exists refuse") }) }
	t.Cleanup(remove)
	return remove
}

func queue_test_rows(db *DB) int {
	return db.integer("select count(*) from queue")
}

func queue_test_held(db *DB, id string) bool {
	found, _ := db.exists("select 1 from queue where id=?", id)
	return found
}

// A burst of concurrent queue.db writes, one goroutine each as concurrent
// handlers queue them, lands every row. Uncapped, each waiting writer opened a connection of
// its own and they starved one another past the busy timeout.
func TestQueueAddBurstLandsEveryRow(t *testing.T) {
	test_bare_directory(t)
	db := setup_queue_test_schema()

	const burst = 10500
	content := make([]byte, 1024)
	var failed atomic.Int64
	var first atomic.Value
	var group sync.WaitGroup
	for i := 0; i < burst; i++ {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			if err := queue_add_direct("burst-"+strconv.Itoa(i), "peer", "from", "to", "forums", "comment/create", "forums", nil, content, nil, "", 0); err != nil {
				failed.Add(1)
				first.CompareAndSwap(nil, err.Error())
			}
		}(i)
	}
	group.Wait()

	if n := failed.Load(); n > 0 {
		t.Errorf("%d of %d concurrent queue_add_direct calls failed, first: %v", n, burst, first.Load())
	}
	if rows := queue_test_rows(db); rows != burst {
		t.Errorf("queue holds %d rows after %d concurrent adds", rows, burst)
	}
}

// A write lock held past queue_add_timeout is an error the caller handles, not
// a panic, and it comes when the bound runs out rather than after the
// driver's busy timeout: a queue_add on a goroutine without a recover would
// end the process, and a write waiting a minute leaves no time to try again.
func TestQueueAddLockedReturnsError(t *testing.T) {
	test_bare_directory(t)
	db := setup_queue_test_schema()
	queue_test_timeout(t, 300*time.Millisecond)
	release := queue_test_lock(t)

	var err error
	start := time.Now()
	func() {
		defer func() {
			if fault := recover(); fault != nil {
				t.Fatalf("queue_add_direct panicked on a locked queue.db: %v", fault)
			}
		}()
		err = queue_add_direct("locked", "peer", "from", "to", "s", "e", "", nil, nil, nil, "", 0)
	}()
	took := time.Since(start)
	if err == nil {
		t.Fatal("queue_add_direct reported success while another connection held the write lock")
	}
	if !queue_add_transient(err) {
		t.Errorf("queue_add_direct error %q does not read as one worth trying again", err)
	}
	if took > 5*time.Second {
		t.Errorf("queue_add_direct took %v to give up; its bound is %v", took, queue_add_timeout)
	}
	if err := queue_add_broadcast("locked-broadcast", "from", "to", "s", "e", "", nil, nil, nil, 0); err == nil {
		t.Error("queue_add_broadcast reported success while another connection held the write lock")
	}
	if err := message("from", "to", "s", "e").send_peer("peer"); err == nil {
		t.Error("send_peer reported success while another connection held the write lock; its Starlark caller would never hear of the loss")
	}

	// Control: the same write succeeds once the lock is gone, so the error
	// above was the lock and nothing else.
	release()
	if err := queue_add_direct("locked", "peer", "from", "to", "s", "e", "", nil, nil, nil, "", 0); err != nil {
		t.Fatalf("queue_add_direct after the lock was released: %v", err)
	}
	if rows := queue_test_rows(db); rows != 1 {
		t.Errorf("queue holds %d rows, want the one added after the release", rows)
	}
}

// send waits out a lock held across a few attempts within its budget, rather
// than answering the app with an error at the first refusal; past the budget
// it answers with one, having queued nothing.
func TestMessageSendRetriesWithinItsBudget(t *testing.T) {
	// The shipped values leave room for a second attempt: each ends well inside
	// the driver's busy timeout, and the budget spans more than one.
	if queue_add_timeout >= time.Minute/2 || message_send_budget <= queue_add_timeout {
		t.Errorf("queue_add_timeout %v against a budget of %v leaves send no second attempt", queue_add_timeout, message_send_budget)
	}

	db, recipient := queue_test_spread_setup(t)
	defer stub_message_attempt_send()()
	queue_test_timeout(t, 300*time.Millisecond)
	previous_pause, previous_budget := message_send_pause, message_send_budget
	message_send_pause = 20 * time.Millisecond
	t.Cleanup(func() { message_send_pause, message_send_budget = previous_pause, previous_budget })

	// The lock outlasts several attempts but not the budget.
	message_send_budget = 10 * time.Second
	release := queue_test_lock(t)
	go func() {
		time.Sleep(1500 * time.Millisecond)
		release()
	}()
	m := message("from", recipient, "s", "e")
	if err := m.send(); err != nil {
		t.Errorf("send gave up while queue.db was only briefly locked: %v", err)
	}
	if !queue_test_held(db, m.ID) {
		t.Error("send answered without error but its row is not queued")
	}

	// Control: a budget longer than one attempt but shorter than the lock is an
	// error for the lock, so the row above landed because send tried again.
	message_send_budget = 400 * time.Millisecond
	release = queue_test_lock(t)
	go func() {
		time.Sleep(1500 * time.Millisecond)
		release()
	}()
	late := message("from", recipient, "s", "e")
	err := late.send()
	if err == nil {
		t.Error("send reported success through a held write lock; the case above proves nothing")
	} else if !queue_add_transient(errors.Unwrap(err)) {
		t.Errorf("send failed with %q, not for the lock; the control proves nothing", err)
	}
	if queue_test_held(db, late.ID) {
		t.Error("a row landed through a held write lock")
	}
}

// send's inline send runs on a goroutine of its own, outside any recover, so a
// fault in it would take the whole server down; the guard contains it to the
// one message.
func TestMessageSendPanicContained(t *testing.T) {
	test_bare_directory(t)
	setup_queue_test_schema()
	setup_users_test_schema()
	// The recipient is listed at a peer, so send reaches its inline send.
	recipient := test_entity_id('r')
	setup_directory_test_schema().exec("insert into entries (entity, peer, seen) values (?, ?, ?)", recipient, "peer-elsewhere", now())

	reached := make(chan struct{})
	previous := message_attempt_send
	message_attempt_send = func(*Message, string, []byte) {
		close(reached)
		panic("probe fault in the inline send")
	}
	t.Cleanup(func() { message_attempt_send = previous })

	if err := message("from", recipient, "s", "e").send(); err != nil {
		t.Fatalf("send: %v", err)
	}
	select {
	case <-reached:
	case <-time.After(5 * time.Second):
		t.Fatal("send never reached the inline send")
	}
	// An unguarded panic on that goroutine ends this test binary here.
	time.Sleep(200 * time.Millisecond)
}

// queue_test_copies records, per peer, every id the message's rows have held,
// as a receiver deduplicating by id would count deliveries.
type queue_test_copies map[string]map[string]bool

func (c queue_test_copies) record(t *testing.T, db *DB, to string) {
	t.Helper()
	rows, err := db.rows("select id, target from queue where to_entity=? and target != ''", to)
	if err != nil {
		t.Fatalf("reading rows: %v", err)
	}
	for _, r := range rows {
		target, _ := r["target"].(string)
		id, _ := r["id"].(string)
		if c[target] == nil {
			c[target] = map[string]bool{}
		}
		c[target][id] = true
	}
}

// once fails for every peer that has held the message under more than one id,
// or never held it.
func (c queue_test_copies) once(t *testing.T, peers ...string) {
	t.Helper()
	for _, peer := range peers {
		if n := len(c[peer]); n != 1 {
			t.Errorf("peer %q was queued the message under %d ids (%v); a receiver deduplicating by id delivers it %d times", peer, n, c[peer], n)
		}
	}
}

func queue_test_spread_setup(t *testing.T) (*DB, string) {
	t.Helper()
	setup_replication_test(t)
	setup_users_test_schema()
	recipient := test_entity_id('s')
	add_directory_location(t, recipient, "peer-a", now())
	add_directory_location(t, recipient, "peer-b", now()-100)
	add_directory_location(t, recipient, "peer-c", now()-200)
	return db_open("db/queue.db"), recipient
}

func queue_test_load(db *DB, id string) (QueueEntry, bool) {
	var q QueueEntry
	return q, db.scan(&q, "select * from queue where id = ?", id)
}

// A row spread over three peers whose spread is cut short is spread again on
// its next turns, and each peer is queued the message under one id: the copy
// queued before the failure is not queued again beside a second, a copy
// delivered in between comes back under the id its receiver has seen, and a
// change in the peers' order between turns changes nothing.
func TestQueueSpreadQueuesEachPeerOnce(t *testing.T) {
	db, recipient := queue_test_spread_setup(t)
	root := uid()
	if err := queue_add_direct(root, "", "from", recipient, "s", "e", "", nil, nil, nil, "", 0); err != nil {
		t.Fatalf("queue_add_direct: %v", err)
	}
	copies := queue_test_copies{}
	turn := func() {
		t.Helper()
		if q, held := queue_test_load(db, root); held && q.Target == "" {
			queue_expand_empty_target(&q)
		}
		copies.record(t, db, recipient)
	}

	allow := queue_test_refuse(t, db, "before insert on queue when new.target = 'peer-c'")
	turn()
	if !queue_test_held(db, root) {
		t.Fatal("the row went although a copy of it could not be queued")
	}
	if n := db.integer("select count(*) from queue where target = 'peer-b'"); n != 1 {
		t.Fatalf("control: the spread queued %d rows for peer-b before the refusal, want 1", n)
	}

	// peer-b's copy is delivered, and peer-b becomes the freshest route.
	db.exec("delete from queue where target = 'peer-b'")
	add_directory_location(t, recipient, "peer-b", now()+100)
	allow()

	turn()
	turn()

	copies.once(t, "peer-a", "peer-b", "peer-c")
	if queue_test_held(db, root) {
		t.Error("the row is still queued beside the copies that carry its message")
	}
	if n := db.integer("select count(*) from queue where to_entity=?", recipient); n != 3 {
		t.Errorf("%d rows queued for the message, want one per peer", n)
	}
}

// Retargeting a row whose message already has a copy at a live peer queues
// every live peer the message once, through a spread cut short and retried.
func TestQueueRetargetQueuesEachPeerOnce(t *testing.T) {
	db, recipient := queue_test_spread_setup(t)
	root := uid()
	// send queued the message to a peer that has since gone, and its
	// copy to peer-b.
	if err := queue_add_direct(root, "peer-gone", "from", recipient, "s", "e", "", nil, nil, nil, "", 0); err != nil {
		t.Fatalf("queue_add_direct: %v", err)
	}
	if err := queue_add_direct(queue_sibling(root, "peer-b"), "peer-b", "from", recipient, "s", "e", "", nil, nil, nil, "", 0); err != nil {
		t.Fatalf("queue_add_direct: %v", err)
	}
	copies := queue_test_copies{}
	copies.record(t, db, recipient)
	turn := func() {
		t.Helper()
		if q, held := queue_test_load(db, root); held {
			queue_retarget(db, &q)
		}
		copies.record(t, db, recipient)
	}

	allow := queue_test_refuse(t, db, "before insert on queue when new.target = 'peer-c'")
	turn()
	if !queue_test_held(db, root) {
		t.Fatal("the row went although a copy of it could not be queued")
	}
	allow()
	turn()
	turn()

	copies.once(t, "peer-a", "peer-b", "peer-c")
	if n := db.integer("select count(*) from queue where to_entity=? and target='peer-gone'", recipient); n != 0 {
		t.Error("a row still targets the peer that is no longer a route")
	}
}

// send queues a message's rows all or none: one queue.db refuses ends the
// send with an error and leaves none of the others, so the app can send again
// without a peer getting the message twice.
func TestMessageSendQueuesAllOrNone(t *testing.T) {
	db, recipient := queue_test_spread_setup(t)
	defer stub_message_attempt_send()()
	allow := queue_test_refuse(t, db, "before insert on queue when new.target = 'peer-b'")

	m := message("from", recipient, "s", "e")
	if err := m.send(); err == nil {
		t.Error("send reported success with one of its rows refused")
	}
	if n := db.integer("select count(*) from queue where to_entity=?", recipient); n != 0 {
		t.Errorf("%d rows of a send that failed are queued; sending it again would deliver them twice", n)
	}

	// Control: the same send lands every row once nothing refuses one.
	allow()
	if err := m.send(); err != nil {
		t.Fatalf("send: %v", err)
	}
	if n := db.integer("select count(*) from queue where to_entity=?", recipient); n != 3 {
		t.Errorf("%d rows queued, want one per peer", n)
	}
}

// A broadcast fan-out stops at the first row queue.db refuses, with the
// broadcast stream's lock held by its caller; resync covers the rest.
func TestMessagesFanoutStopsAtTheFirstRefusal(t *testing.T) {
	setup_replication_test(t)
	db := db_open("db/queue.db")
	previous := messages_fanout_open
	messages_fanout_open = func(string) (*Sender, error) { return nil, nil }
	t.Cleanup(func() { messages_fanout_open = previous })
	queue_test_refuse(t, db, "before insert on queue when new.target = 'peer-2'")

	var list []*Message
	for i := 1; i <= 3; i++ {
		m := message("from", test_entity_id(byte('a'+i)), "s", "e")
		m.target = "peer-" + strconv.Itoa(i)
		list = append(list, m)
	}
	messages_fanout(list, cbor_encode(map[string]any{}))

	if n := db.integer("select count(*) from queue where target='peer-1'"); n != 1 {
		t.Fatalf("control: %d rows for the recipient before the refusal, want 1", n)
	}
	if n := db.integer("select count(*) from queue where target='peer-3'"); n != 0 {
		t.Error("the fan-out went on queuing past a refused row")
	}
}

// The push pass stops writing at the first write queue.db refuses, and
// reports nothing done so its manager waits for the next tick.
func TestPushQueueStopsAtTheFirstRefusal(t *testing.T) {
	push_test_setup(t, "http://127.0.0.1:1")
	db := db_open("db/queue.db")
	// Rows of a user that has gone: the pass only deletes them.
	for i := 1; i <= 3; i++ {
		db.exec(`insert into pushes (id, user, account, type, identifier, data, app, category, object, title, body, link, event, attempts, next_retry, created)
			values (?, 'gone', 'acct', 'url', '', '{}', '', '', '', '', '', '', '', 1, ?, ?)`, "push-"+strconv.Itoa(i), i, now())
	}
	queue_test_refuse(t, db, "before delete on pushes when old.id = 'push-2'")

	if acted := push_queue_process(); acted != 0 {
		t.Errorf("the pass reported %d rows acted on after a refused write, so its manager loops straight back", acted)
	}
	held := func(id string) bool {
		found, _ := db.exists("select 1 from pushes where id=?", id)
		return found
	}
	if held("push-1") {
		t.Fatal("control: the row before the refusal was not deleted")
	}
	if !held("push-3") {
		t.Error("the pass went on writing past a refused write")
	}
}

// A provider that panics costs its push one attempt, retried as a transient
// failure; the pass and the process carry on.
func TestPushDeliveryPanicContained(t *testing.T) {
	push_test_setup(t, "http://127.0.0.1:1")
	push_deliverers["probe"] = func(*Push) (bool, bool) { panic("probe fault in a push provider") }
	t.Cleanup(func() { delete(push_deliverers, "probe") })
	db := db_open("db/queue.db")
	db.exec(`insert into pushes (id, user, account, type, identifier, data, app, category, object, title, body, link, event, attempts, next_retry, created)
		values ('probe-push', 'push-user', 'acct', 'probe', '', '{}', '', '', '', '', '', '', '', 1, 0, ?)`, now())

	push_queue_process()

	row := push_test_row(t, "probe-push")
	if row == nil {
		t.Fatal("the push was dropped as undeliverable; a provider fault is transient")
	}
	if attempts := row_int(row, "attempts"); attempts != 2 {
		t.Errorf("attempts is %d, want 2: the fault counts as one failed attempt", attempts)
	}
}

// A fault sending one queued row costs that row's attempt; the pass returns
// and the process carries on.
func TestQueueProcessSendPanicContained(t *testing.T) {
	setup_replication_test(t)
	setup_users_test_schema()
	from, _ := new_entity_keys(t)
	reached := make(chan struct{}, 1)
	previous := queue_direct
	queue_direct = func(*QueueEntry) bool {
		reached <- struct{}{}
		panic("probe fault sending a queued row")
	}
	t.Cleanup(func() { queue_direct = previous })
	if err := queue_add_direct(uid(), "peer-elsewhere", from, test_entity_id('p'), "s", "e", "", nil, nil, nil, "", 0); err != nil {
		t.Fatalf("queue_add_direct: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		queue_process()
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("queue_process did not return after a send panicked")
	}
	select {
	case <-reached:
	default:
		t.Fatal("control: queue_process never sent the row")
	}
}

// A refused eviction stamp does not stop the eviction, nor the ones after it.
func TestHealthEvictDispatchSurvivesARefusedStamp(t *testing.T) {
	setup_replication_test(t)
	db := db_open("db/queue.db")
	var dispatched []string
	previous := subscriber_dispatch
	subscriber_dispatch = func(_ *User, _ *App, _, _, _, target string, _ map[string]any, _ func() map[string]any) {
		dispatched = append(dispatched, target)
	}
	t.Cleanup(func() { subscriber_dispatch = previous })

	a := &App{id: "evict-probe-" + uid()}
	recipients := []string{test_entity_id('x'), test_entity_id('y')}
	for _, recipient := range recipients {
		db.exec("insert into health (recipient, suspended) values (?, ?)", recipient, now()-86400)
	}
	queue_test_refuse(t, db, "before update on health")

	for _, recipient := range recipients {
		func() {
			defer func() {
				if fault := recover(); fault != nil {
					t.Fatalf("health_evict_dispatch panicked on a refused stamp: %v", fault)
				}
			}()
			health_evict_dispatch(nil, a, "s", recipient)
		}()
	}
	if len(dispatched) != len(recipients) {
		t.Errorf("dispatched %v, want every eviction", dispatched)
	}
}

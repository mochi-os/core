// Mochi server: mochi.message.send queues before it returns, in an action
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

// message_send_test_thread is a Starlark thread whose user owns an entity, the
// sender every call below names. With action set it is an action's thread, as
// a web request runs one; without, an event handler's.
func message_send_test_thread(t *testing.T, action bool) (*sl.Thread, string) {
	t.Helper()
	setup_replication_test(t)
	setup_users_test_schema()
	from, _ := new_entity_keys(t)
	user := &User{UID: "uid-handshake-test", Username: "send@example.com", Role: "user"}
	thread := create_test_thread(user, create_external_app("message-send-test"))
	if action {
		thread.SetLocal("action", &Action{})
	}
	return thread, from
}

// message_send_test_call calls mochi.message.send from the thread as Starlark
// does, through the module's CallInternal, answering how long it took and its
// error.
func message_send_test_call(thread *sl.Thread, from, to string) (time.Duration, error) {
	headers := sl.NewDict(4)
	headers.SetKey(sl.String("from"), sl.String(from))
	headers.SetKey(sl.String("to"), sl.String(to))
	headers.SetKey(sl.String("service"), sl.String("probe"))
	headers.SetKey(sl.String("event"), sl.String("arrive"))
	content := sl.NewDict(1)
	content.SetKey(sl.String("body"), sl.String("hello"))
	start := time.Now()
	_, err := (&message_send_module{}).CallInternal(thread, sl.Tuple{headers, content}, nil)
	return time.Since(start), err
}

func message_send_test_rows(to string) int {
	return db_open("db/queue.db").integer("select count(*) from queue where to_entity=?", to)
}

// message_send_test_lock holds queue.db's write lock with the attempts and the
// action's budget short, so a test sees the failure in a second.
func message_send_test_lock(t *testing.T) func() {
	t.Helper()
	queue_test_timeout(t, 300*time.Millisecond)
	previous_budget, previous_pause := message_send_budget, message_send_pause
	message_send_budget = time.Second
	message_send_pause = 50 * time.Millisecond
	t.Cleanup(func() { message_send_budget, message_send_pause = previous_budget, previous_pause })
	return queue_test_lock(t)
}

// In an action the rows are queued before mochi.message.send returns, one per
// live peer.
func TestMessageSendQueuesBeforeReturning(t *testing.T) {
	thread, from := message_send_test_thread(t, true)
	defer stub_message_attempt_send()()
	recipient := test_entity_id('q')
	add_directory_location(t, recipient, "peer-a", now())
	add_directory_location(t, recipient, "peer-b", now()-100)

	if _, err := message_send_test_call(thread, from, recipient); err != nil {
		t.Fatalf("mochi.message.send: %v", err)
	}
	if n := message_send_test_rows(recipient); n != 2 {
		t.Errorf("%d rows queued when the call returned, want one per peer", n)
	}
}

// A recipient no peer is known for still gets its row, with no target, for
// the queue to resolve again later.
func TestMessageSendUnknownRecipientQueuesItsRow(t *testing.T) {
	thread, from := message_send_test_thread(t, true)
	defer stub_message_attempt_send()()
	recipient := test_entity_id('u')

	if _, err := message_send_test_call(thread, from, recipient); err != nil {
		t.Fatalf("mochi.message.send: %v", err)
	}
	row, _ := db_open("db/queue.db").row("select target from queue where to_entity=?", recipient)
	if row == nil {
		t.Fatal("no row queued for a recipient with no known peer")
	}
	if target := row_string(row, "target"); target != "" {
		t.Errorf("the row targets %q, want none until the recipient resolves", target)
	}
}

// In an action, a queue.db that will not take the message within the action's
// budget does not raise: the action would be left having told some recipients
// and not others, and a webhook's sender would redeliver it. The message is
// handed to the background and queued once the lock goes.
func TestMessageSendInAnActionQueuesLaterOnABusyQueue(t *testing.T) {
	thread, from := message_send_test_thread(t, true)
	defer stub_message_attempt_send()()
	recipient := test_entity_id('l')
	add_directory_location(t, recipient, "peer-a", now())
	release := message_send_test_lock(t)

	took, err := message_send_test_call(thread, from, recipient)
	if err != nil {
		t.Fatalf("mochi.message.send raised in an action while queue.db was locked: %v", err)
	}
	if took > 3*time.Second {
		t.Errorf("mochi.message.send took %v to give the message to the background; its budget is %v", took, message_send_budget)
	}
	release()
	if !refuse_wait(10*time.Second, func() bool { return message_send_test_rows(recipient) == 1 }) {
		t.Errorf("%d rows queued after the lock went, want the message's one", message_send_test_rows(recipient))
	}
}

// Outside an action - an event handler here - a locked queue.db does not
// raise: the handler would be run again by its event's sender, repeating its
// writes. The message is queued in the background once the lock goes, past the
// budget an action's own attempt would have had.
func TestMessageSendQueuesLaterOutsideAnAction(t *testing.T) {
	thread, from := message_send_test_thread(t, false)
	defer stub_message_attempt_send()()
	recipient := test_entity_id('e')
	add_directory_location(t, recipient, "peer-a", now())
	release := message_send_test_lock(t)

	if _, err := message_send_test_call(thread, from, recipient); err != nil {
		t.Fatalf("mochi.message.send raised in an event handler: %v", err)
	}
	time.Sleep(message_send_budget + 500*time.Millisecond)
	release()
	if !refuse_wait(10*time.Second, func() bool { return message_send_test_rows(recipient) == 1 }) {
		t.Errorf("%d rows queued after the lock went, want the message's one", message_send_test_rows(recipient))
	}
}

// A lone row to a peer is taken by the inline send and needs no wake-up of the
// queue; several rows do.
func TestMessageSendWakesTheQueueOnlyForMore(t *testing.T) {
	setup_replication_test(t)
	setup_users_test_schema()
	attempted := make(chan struct{}, 4)
	previous := message_attempt_send
	message_attempt_send = func(*Message, string, []byte) { attempted <- struct{}{} }
	t.Cleanup(func() { message_attempt_send = previous })
	drain := func() {
		for {
			select {
			case <-queue_wake_ch:
			default:
				return
			}
		}
	}
	woken := func() bool {
		select {
		case <-queue_wake_ch:
			return true
		case <-time.After(200 * time.Millisecond):
			return false
		}
	}

	lone := test_entity_id('o')
	add_directory_location(t, lone, "peer-a", now())
	drain()
	if err := message("from", lone, "s", "e").send(message_send_budget); err != nil {
		t.Fatalf("send: %v", err)
	}
	<-attempted
	if woken() {
		t.Error("the queue was woken for a lone row the inline send takes")
	}

	spread := test_entity_id('w')
	add_directory_location(t, spread, "peer-a", now())
	add_directory_location(t, spread, "peer-b", now()-100)
	drain()
	if err := message("from", spread, "s", "e").send(message_send_budget); err != nil {
		t.Fatalf("send: %v", err)
	}
	<-attempted
	if !woken() {
		t.Error("control: the queue was not woken for the row the inline send does not take")
	}
}

// Where this server is one of several routes, its copy is queued in the same
// transaction as the others, not handed to its worker first: a send whose
// other rows are refused leaves no copy delivered here.
func TestMessageSendQueuesTheLocalCopyWithTheRest(t *testing.T) {
	setup_replication_test(t)
	setup_users_test_schema()
	reset_workers(t)
	t.Cleanup(func() { reset_workers(t) })
	defer stub_message_attempt_send()()
	from, _ := new_entity_keys(t)
	recipient := test_entity_id('m')
	add_directory_location(t, recipient, "peer-a", now()-100)
	// The sending user has learned that the recipient is also reached here.
	user := user_owning_entity(from)
	if user == nil {
		t.Fatal("fixture: the sender has no user")
	}
	db_user(user, "user").exec("insert into directory (entity, peer, created, seen) values (?, ?, ?, ?)", recipient, net_id, now(), now())
	if peers := entity_peers_for(from, recipient); len(peers) != 2 {
		t.Fatalf("fixture: routes %v, want this server and peer-a", peers)
	}

	db := db_open("db/queue.db")
	allow := queue_test_refuse(t, db, "before insert on queue when new.target = 'peer-a'")
	if err := message(from, recipient, "s", "e").send(message_send_budget); err == nil {
		t.Fatal("send reported success with a row refused")
	}
	if workers, _ := worker_count(); workers != 0 {
		t.Errorf("the local copy went to its worker although the send failed: %d workers", workers)
	}
	if n := message_send_test_rows(recipient); n != 0 {
		t.Errorf("%d rows of a failed send are queued", n)
	}

	// Control: with nothing refused both rows are queued, this server's for
	// self_loop_drain.
	allow()
	if err := message(from, recipient, "s", "e").send(message_send_budget); err != nil {
		t.Fatalf("send: %v", err)
	}
	if n := db.integer("select count(*) from queue where to_entity=? and target=?", recipient, net_id); n != 1 {
		t.Errorf("%d rows queued for this server, want 1", n)
	}
}

// An action's sends share one budget: on a queue.db that will not take them,
// five sends cost the action the budget once, not five times, and each message
// is still queued in the background once the lock goes. A group's creation
// sends once per member, and the budget per send carried it past the request's
// own deadline with the later members never sent.
func TestMessageSendInAnActionSharesOneBudget(t *testing.T) {
	thread, from := message_send_test_thread(t, true)
	defer stub_message_attempt_send()()
	recipients := []string{test_entity_id('1'), test_entity_id('2'), test_entity_id('3'), test_entity_id('4'), test_entity_id('5')}
	for _, recipient := range recipients {
		add_directory_location(t, recipient, "peer-a", now())
	}
	release := message_send_test_lock(t)

	start := time.Now()
	for _, recipient := range recipients {
		if _, err := message_send_test_call(thread, from, recipient); err != nil {
			t.Fatalf("mochi.message.send raised in an action while queue.db was locked: %v", err)
		}
	}
	if took := time.Since(start); took > message_send_budget+time.Second {
		t.Errorf("five sends in one action took %v on a locked queue.db; together their budget is %v", took.Round(time.Millisecond), message_send_budget)
	}
	release()
	for _, recipient := range recipients {
		if !refuse_wait(10*time.Second, func() bool { return message_send_test_rows(recipient) == 1 }) {
			t.Errorf("%d rows queued for %s after the lock went, want the message's one", message_send_test_rows(recipient), recipient)
		}
	}
}

// The budget an action's sends share is time spent queueing, not time since
// the first send: an action that sends, works for longer than the budget, and
// sends again on a briefly busy queue.db still queues the second message
// before it returns, rather than handing it to the background unasked.
func TestMessageSendInAnActionKeepsItsBudgetForQueueing(t *testing.T) {
	thread, from := message_send_test_thread(t, true)
	defer stub_message_attempt_send()()
	first, second := test_entity_id('f'), test_entity_id('s')
	add_directory_location(t, first, "peer-a", now())
	add_directory_location(t, second, "peer-a", now())
	queue_test_timeout(t, 300*time.Millisecond)
	previous_budget, previous_pause := message_send_budget, message_send_pause
	message_send_budget = time.Second
	message_send_pause = 50 * time.Millisecond
	t.Cleanup(func() { message_send_budget, message_send_pause = previous_budget, previous_pause })

	if _, err := message_send_test_call(thread, from, first); err != nil {
		t.Fatalf("mochi.message.send: %v", err)
	}
	time.Sleep(message_send_budget + 200*time.Millisecond)
	release := queue_test_lock(t)
	time.AfterFunc(300*time.Millisecond, release)
	if _, err := message_send_test_call(thread, from, second); err != nil {
		t.Fatalf("mochi.message.send raised in an action on a briefly busy queue.db: %v", err)
	}
	if n := message_send_test_rows(second); n != 1 {
		t.Errorf("%d rows queued when the second send returned, want its one: time the action spent between its sends was charged to their budget", n)
	}
}

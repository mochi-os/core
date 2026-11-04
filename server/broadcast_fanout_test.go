// Mochi server: broadcast fan-out queueing.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/fxamacker/cbor/v2"
	sl "go.starlark.net/starlark"
)

const fanout_test_peer = "12D3KooWGPbbqBmyGpTLxVLZzhNYhbHZBMDdFbEcxHnLzVCJmqXt"

// fanout_test_send broadcasts one event from a feed owned by a test user to
// `subscribers`, as mochi.broadcast.send does.
func fanout_test_send(t *testing.T, subscribers []sl.Value) {
	t.Helper()
	thread, feed := fanout_test_feed(t)
	fanout_test_broadcast(t, thread, feed, subscribers)
}

// fanout_test_feed makes a test user owning a feed, and a thread running as
// that user in a test app.
func fanout_test_feed(t *testing.T) (*sl.Thread, string) {
	t.Helper()
	setup_users_test_schema()
	feed := withdraw_test_entity(t)
	users := db_open("db/users.db")
	users.exec("insert into users (uid, username) values ('u-fanout', 'fanout@x')")
	users.exec("insert into entities (id, private, fingerprint, user, class, name) values (?, '', ?, 'u-fanout', 'feed', 'Feed')", feed, fingerprint(feed))
	person := withdraw_test_entity(t)
	users.exec("insert into entities (id, private, fingerprint, user, class, name) values (?, '', ?, 'u-fanout', 'person', 'Owner')", person, fingerprint(person))
	user := user_by_uid("u-fanout")
	if user == nil {
		t.Fatal("user_by_uid")
	}
	thread := &sl.Thread{}
	thread.SetLocal("user", user)
	thread.SetLocal("app", &App{id: "testapp"})
	return thread, feed
}

// fanout_test_broadcast broadcasts one event from feed to `subscribers`.
func fanout_test_broadcast(t *testing.T, thread *sl.Thread, feed string, subscribers []sl.Value) {
	t.Helper()
	data := sl.NewDict(1)
	_ = data.SetKey(sl.String("body"), sl.String("hello"))
	builtin := sl.NewBuiltin("mochi.broadcast.send", api_broadcast_send)
	if _, err := api_broadcast_send(thread, builtin, sl.Tuple{
		sl.String(feed), sl.String(feed), sl.NewList(subscribers),
		sl.String("feeds"), sl.String("post/create"), data, sl.String(""),
	}, nil); err != nil {
		t.Fatalf("api_broadcast_send: %v", err)
	}
}

// TestBroadcastFanoutQueuesWithoutChurn pins that a broadcast's rows are queued
// recipient after recipient, not from a goroutine each: concurrent writers
// opened and closed a queue.db connection per recipient, and contended with
// each other for the write lock.
func TestBroadcastFanoutQueuesWithoutChurn(t *testing.T) {
	setup_replication_test(t)
	const count = 200
	subscribers := make([]sl.Value, count)
	for i := range subscribers {
		subscribers[i] = sl.String(withdraw_test_entity(t))
	}
	queue := db_open("db/queue.db")
	before := queue.internal.Stats().MaxIdleClosed

	fanout_test_send(t, subscribers)
	deadline := time.Now().Add(10 * time.Second)
	for queue.integer("select count(*) from queue where type='direct'") < count {
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d rows queued", queue.integer("select count(*) from queue where type='direct'"), count)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if closed := queue.internal.Stats().MaxIdleClosed - before; closed > 10 {
		t.Errorf("queueing %d recipients opened and closed %d queue.db connections", count, closed)
	}
}

// TestBroadcastFanoutPinnedQueuedBeforeReturn pins that a recipient pinned to
// a peer is queued before mochi.broadcast.send returns, as send_peer queued it.
func TestBroadcastFanoutPinnedQueuedBeforeReturn(t *testing.T) {
	setup_replication_test(t)
	recipient := withdraw_test_entity(t)
	pinned := sl.NewDict(2)
	_ = pinned.SetKey(sl.String("id"), sl.String(recipient))
	_ = pinned.SetKey(sl.String("peer"), sl.String(fanout_test_peer))

	fanout_test_send(t, []sl.Value{pinned})
	queue := db_open("db/queue.db")
	if n := queue.integer("select count(*) from queue where to_entity=? and target=?", recipient, fanout_test_peer); n != 1 {
		t.Errorf("pinned recipient has %d rows for its peer when the call returns, want 1", n)
	}
}

// TestMessagesFanoutRows pins the rows messages_fanout queues: a pinned
// recipient's to its peer under its own id; a resolved one's to every peer
// hosting it, the first under its own id; an unknown one's with no target;
// each carrying the content as encoded.
func TestMessagesFanoutRows(t *testing.T) {
	setup_replication_test(t)
	setup_users_test_schema()
	hosted, unknown, pinned := withdraw_test_entity(t), withdraw_test_entity(t), withdraw_test_entity(t)
	add_directory_location(t, hosted, "peer-A", now())
	add_directory_location(t, hosted, "peer-B", now())
	content := cbor_encode(map[string]any{"body": "hello"})

	first := message("from-entity", hosted, "feeds", "post/create")
	lost := message("from-entity", unknown, "feeds", "post/create")
	held := message("from-entity", pinned, "feeds", "post/create")
	held.target = "peer-C"
	messages_fanout([]*Message{first, lost, held}, content)

	queue := db_open("db/queue.db")
	if n := queue.integer("select count(*) from queue where to_entity=? and target in ('peer-A', 'peer-B')", hosted); n != 2 {
		t.Errorf("hosted recipient has %d rows, want one per peer", n)
	}
	if n := queue.integer("select count(*) from queue where id=? and to_entity=?", first.ID, hosted); n != 1 {
		t.Errorf("hosted recipient's first row is not under its own id")
	}
	if n := queue.integer("select count(*) from queue where id=? and target=''", lost.ID); n != 1 {
		t.Errorf("unknown recipient has no untargeted row")
	}
	if n := queue.integer("select count(*) from queue where id=? and target='peer-C'", held.ID); n != 1 {
		t.Errorf("pinned recipient has no row for its peer under its own id")
	}
	row, _ := queue.row("select content from queue where id=?", held.ID)
	if row == nil || fmt.Sprint(row["content"]) != string(content) {
		t.Errorf("queued content differs from the encoded content")
	}
}

// TestMessagesFanoutOpensColdPeers pins that a fan-out opens a Sender for each
// peer it queued rows for that has none, so the rows go at once rather than
// when the queue manager reaches the peer; peers with a Sender, and this
// server, are left alone.
func TestMessagesFanoutOpensColdPeers(t *testing.T) {
	setup_replication_test(t)
	setup_users_test_schema()
	saved := net_id
	net_id = "peer-self"
	defer func() { net_id = saved }()
	opened := make(chan string, 8)
	saved_open := messages_fanout_open
	messages_fanout_open = func(peer string) (*Sender, error) {
		opened <- peer
		return nil, error_sender_unreachable
	}
	defer func() { messages_fanout_open = saved_open }()
	warm := new_test_sender(t, "peer-warm")
	defer stash_sender(t, "peer-warm", warm)()
	senders_lock.Lock()
	senders_opening["peer-opening"] = &opening{done: make(chan struct{})}
	senders_lock.Unlock()
	defer func() {
		senders_lock.Lock()
		delete(senders_opening, "peer-opening")
		senders_lock.Unlock()
	}()

	hosted, pinned, local := withdraw_test_entity(t), withdraw_test_entity(t), withdraw_test_entity(t)
	add_directory_location(t, hosted, "peer-cold", now())
	add_directory_location(t, hosted, "peer-warm", now())
	add_directory_location(t, hosted, "peer-opening", now())
	first := message("from-entity", hosted, "feeds", "post/create")
	held := message("from-entity", pinned, "feeds", "post/create")
	held.target = "peer-pinned"
	self := message("from-entity", local, "feeds", "post/create")
	self.target = "peer-self"
	messages_fanout([]*Message{first, held, self}, cbor_encode(map[string]any{"body": "hello"}))

	got := map[string]bool{}
	timeout := time.After(2 * time.Second)
	for len(got) < 2 {
		select {
		case peer := <-opened:
			got[peer] = true
		case <-timeout:
			t.Fatalf("opened %v, want peer-cold and peer-pinned", got)
		}
	}
	select {
	case peer := <-opened:
		t.Errorf("also opened %q, which has a Sender, one opening, or is this server", peer)
	case <-time.After(200 * time.Millisecond):
	}
	if !got["peer-cold"] || !got["peer-pinned"] {
		t.Errorf("opened %v, want peer-cold and peer-pinned", got)
	}
}

// TestBroadcastFanoutKeepsStreamOrder pins that a stream's broadcasts are
// queued in the order they were sent: a later broadcast's fan-out waits for the
// earlier one's, so a recipient late in a long list still gets the first
// broadcast's row first.
func TestBroadcastFanoutKeepsStreamOrder(t *testing.T) {
	setup_replication_test(t)
	thread, feed := fanout_test_feed(t)
	shared := withdraw_test_entity(t)
	first := make([]sl.Value, 300)
	for i := range first {
		first[i] = sl.String(withdraw_test_entity(t))
	}
	first[len(first)-1] = sl.String(shared)

	fanout_test_broadcast(t, thread, feed, first)
	fanout_test_broadcast(t, thread, feed, []sl.Value{sl.String(shared)})

	queue := db_open("db/queue.db")
	deadline := time.Now().Add(10 * time.Second)
	for queue.integer("select count(*) from queue where to_entity=?", shared) < 2 {
		if time.Now().After(deadline) {
			t.Fatal("the shared recipient's rows were not both queued")
		}
		time.Sleep(20 * time.Millisecond)
	}
	rows, _ := queue.rows("select content from queue where to_entity=? order by rowid", shared)
	sequences := []any{}
	for _, row := range rows {
		content := map[string]any{}
		raw, ok := row["content"].([]byte)
		if !ok {
			text, _ := row["content"].(string)
			raw = []byte(text)
		}
		_ = cbor.Unmarshal(raw, &content)
		sequences = append(sequences, content[broadcast_content_sequence])
	}
	if len(sequences) != 2 || fmt.Sprint(sequences[0]) != "1" || fmt.Sprint(sequences[1]) != "2" {
		t.Errorf("the shared recipient's rows were queued with sequences %v, want 1 then 2", sequences)
	}
}

// TestBroadcastStreamKeepsSequenceOrder pins that broadcasts of one stream sent
// at once are queued in the order of their sequences: each takes its place in
// line in the same step as its sequence.
func TestBroadcastStreamKeepsSequenceOrder(t *testing.T) {
	setup_replication_test(t)
	thread, feed := fanout_test_feed(t)
	shared := withdraw_test_entity(t)
	const sends = 8
	lists := make([][]sl.Value, sends)
	for i := range lists {
		lists[i] = make([]sl.Value, 100)
		for j := range lists[i] {
			lists[i][j] = sl.String(withdraw_test_entity(t))
		}
		lists[i][len(lists[i])-1] = sl.String(shared)
	}
	var wait sync.WaitGroup
	for i := 0; i < sends; i++ {
		wait.Add(1)
		go func(list []sl.Value) {
			defer wait.Done()
			own := &sl.Thread{}
			own.SetLocal("user", thread.Local("user"))
			own.SetLocal("app", thread.Local("app"))
			fanout_test_broadcast(t, own, feed, list)
		}(lists[i])
	}
	wait.Wait()

	queue := db_open("db/queue.db")
	deadline := time.Now().Add(15 * time.Second)
	for queue.integer("select count(*) from queue where to_entity=?", shared) < sends {
		if time.Now().After(deadline) {
			t.Fatal("the shared recipient's rows were not all queued")
		}
		time.Sleep(20 * time.Millisecond)
	}
	rows, _ := queue.rows("select content from queue where to_entity=? order by rowid", shared)
	previous := int64(0)
	for _, row := range rows {
		content := map[string]any{}
		raw, ok := row["content"].([]byte)
		if !ok {
			text, _ := row["content"].(string)
			raw = []byte(text)
		}
		_ = cbor.Unmarshal(raw, &content)
		sequence := atoi(fmt.Sprint(content[broadcast_content_sequence]), 0)
		if sequence <= previous {
			t.Fatalf("the shared recipient's rows were queued out of sequence: %d after %d", sequence, previous)
		}
		previous = sequence
	}
}

// TestBroadcastEvictionRunsOutsideTheStreamLock pins that an eviction's app
// handler runs once the stream's lock is released: a handler that broadcasts in
// turn would otherwise wait on the lock its own broadcast holds.
func TestBroadcastEvictionRunsOutsideTheStreamLock(t *testing.T) {
	setup_replication_test(t)
	thread, feed := fanout_test_feed(t)
	down := withdraw_test_entity(t)
	db_open("db/queue.db").exec("insert into health (recipient, suspended, probed, since) values (?, ?, ?, ?)", down, now()-queue_evict_age-10, now(), now()-queue_evict_age-10)
	lock := broadcast_stream_lock(feed + "\x00" + feed)
	held := []bool{}
	original := subscriber_dispatch
	subscriber_dispatch = func(u *User, a *App, code, reason, service, entity string, orig map[string]any, detail func() map[string]any) {
		free := lock.TryLock()
		if free {
			lock.Unlock()
		}
		held = append(held, !free)
	}
	defer func() { subscriber_dispatch = original }()

	fanout_test_broadcast(t, thread, feed, []sl.Value{sl.String(down)})
	if len(held) != 1 {
		t.Fatalf("dispatched %d evictions, want one", len(held))
	}
	if held[0] {
		t.Error("the eviction's handler ran while its broadcast held the stream's lock")
	}
}

// TestBroadcastFanoutSurvivesAPanic pins that a panic in a broadcast's
// background fan-out is contained on its own goroutine, which nothing else
// recovers, and that the stream's next fan-out, waiting on it, still runs.
func TestBroadcastFanoutSurvivesAPanic(t *testing.T) {
	setup_replication_test(t)
	recipient := withdraw_test_entity(t)
	broadcast_fanout("fanout-panic", []*Message{nil}, []byte("x"))
	m := &Message{From: withdraw_test_entity(t), To: recipient, Service: "feeds", Event: "post/create", target: fanout_test_peer}
	broadcast_fanout("fanout-panic", []*Message{m}, []byte("x"))

	queue := db_open("db/queue.db")
	deadline := time.Now().Add(10 * time.Second)
	for queue.integer("select count(*) from queue where to_entity=?", recipient) < 1 {
		if time.Now().After(deadline) {
			t.Fatal("the broadcast after the one that panicked was never queued")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Mochi server: deleting an entity sends what it sent just before.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"slices"
	"sync"
	"testing"
	"time"

	sl "go.starlark.net/starlark"
)

// farewell_queue stands in for the queue: it sends a row, by removing it,
// only while the row's sender still holds the key that signs it, as a claim
// needs, and never a row for the peer it cannot reach.
func farewell_queue(t *testing.T, unreachable string) (func() []string, func()) {
	t.Helper()
	var lock sync.Mutex
	var sent []string
	stop := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		queue := db_open("db/queue.db")
		users := db_open("db/users.db")
		for {
			select {
			case <-stop:
				return
			case <-time.After(20 * time.Millisecond):
			}
			rows, _ := queue.rows("select id, target, from_entity from queue")
			for _, row := range rows {
				if row_string(row, "target") == unreachable {
					continue
				}
				if held, _ := users.exists("select 1 from entities where id=?", row_string(row, "from_entity")); !held {
					continue
				}
				queue.exec("delete from queue where id=?", row_string(row, "id"))
				lock.Lock()
				sent = append(sent, row_string(row, "id"))
				lock.Unlock()
			}
		}
	}()
	list := func() []string {
		lock.Lock()
		defer lock.Unlock()
		return slices.Clone(sent)
	}
	end := func() {
		close(stop)
		<-finished
	}
	return list, end
}

// TestEntityDeleteSendsItsFarewell: an entity deleted just after it sent a
// farewell - one queued, one whose fan-out is still queueing it, as an app's
// delete broadcasts to its subscribers and then deletes its entity - is
// deleted once the queue has sent both, not with them still queued: the
// delete takes the entity's queued messages and the key that signs them, and
// its subscribers on other servers would keep it live. A farewell that cannot
// be sent holds the delete no longer than entity_farewell_wait.
func TestEntityDeleteSendsItsFarewell(t *testing.T) {
	setup_replication_test(t)
	setup_users_test_schema()
	setup_directory_test_schema()
	saved := entity_farewell_wait
	entity_farewell_wait = 800 * time.Millisecond
	t.Cleanup(func() { entity_farewell_wait = saved })

	users := db_open("db/users.db")
	users.exec("insert into users (uid, username) values ('u-owner', 'owner@x')")
	forum, feed, kept := withdraw_test_entity(t), withdraw_test_entity(t), withdraw_test_entity(t)
	for _, id := range []string{forum, feed, kept} {
		users.exec("insert into entities (id, private, fingerprint, user, class, name) values (?, 'key', ?, 'u-owner', 'forum', 'Forum')", id, fingerprint(id))
	}
	queue := db_open("db/queue.db")
	farewell := func(id, from, target string) {
		queue.exec("insert into queue (id, target, from_entity, to_entity, service, event, next_retry, created) values (?, ?, ?, 'subscriber', 'forums', 'forum/delete', 0, ?)", id, target, from, now())
	}
	thread := create_test_thread(&User{UID: "u-owner"}, app_declaring("forums", "forum"))
	remove := sl.NewBuiltin("mochi.entity.delete", api_entity_delete)

	sent, end := farewell_queue(t, "peer-unreachable")
	defer end()

	// One farewell queued, and one a fan-out of the forum's is still to queue.
	farewell("farewell-queued", forum, "peer-far")
	stream := forum + "\x00forum"
	done := make(chan struct{})
	broadcast_fanouts_lock.Lock()
	broadcast_fanouts[stream] = done
	broadcast_fanouts_lock.Unlock()
	go func() {
		time.Sleep(150 * time.Millisecond)
		farewell("farewell-late", forum, "peer-far")
		broadcast_fanouts_lock.Lock()
		delete(broadcast_fanouts, stream)
		broadcast_fanouts_lock.Unlock()
		close(done)
	}()
	if _, err := sl.Call(thread, remove, sl.Tuple{sl.String(forum)}, nil); err != nil {
		t.Fatalf("delete: %v", err)
	}
	for _, id := range []string{"farewell-queued", "farewell-late"} {
		if !slices.Contains(sent(), id) {
			t.Errorf("THE POINT: %s was not sent before its forum was deleted; sent %v", id, sent())
		}
	}
	if held, _ := users.exists("select 1 from entities where id=?", forum); held {
		t.Error("the forum's entity was not deleted")
	}

	// Only queued, as a farewell to a subscriber pinned to its peer is before
	// the send returns: the delete waits for the queue to send it.
	farewell("farewell-pinned", feed, "peer-far")
	if _, err := sl.Call(thread, remove, sl.Tuple{sl.String(feed)}, nil); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !slices.Contains(sent(), "farewell-pinned") {
		t.Errorf("THE POINT: a queued farewell was not sent before its feed was deleted; sent %v", sent())
	}

	// One that can never be sent holds the delete only so long, then goes.
	farewell("farewell-stuck", kept, "peer-unreachable")
	start := time.Now()
	if _, err := sl.Call(thread, remove, sl.Tuple{sl.String(kept)}, nil); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if took := time.Since(start); took < entity_farewell_wait/2 || took > entity_farewell_wait+500*time.Millisecond {
		t.Errorf("THE POINT: a delete with an unsendable farewell took %v, want about %v", took, entity_farewell_wait)
	}
	if stuck, _ := queue.exists("select 1 from queue where id='farewell-stuck'"); stuck {
		t.Error("the unsendable farewell outlived its entity")
	}
}

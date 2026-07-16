// Mochi server: broadcast exclusion and owner-guard regressions.
//
// Send-time exclusion used to skip the excluded recipient's delivery while
// still allocating the sequence — a permanent hole in their stream that
// resync, blind to the exclusion, "healed" by redelivering the event. A
// redelivered post/edit ran feeds' subscriber-side handler against the feed
// OWNER's canonical DB and deleted its attachment files from disk
// (2026-07-15). Exclusion now rides in the payload (log + wire + replay
// identically) and the receive wrapper skips the handler for the excluded
// actor and for any user who owns the from entity; self-owned recipients
// are not sent to at all.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"strings"
	"testing"
	"time"

	sl "go.starlark.net/starlark"
)

// exclude_test_entities mints valid entity ids: feed, self, remote, actor.
func exclude_test_entities(t *testing.T) (string, string, string, string) {
	t.Helper()
	return withdraw_test_entity(t), withdraw_test_entity(t), withdraw_test_entity(t), withdraw_test_entity(t)
}

func TestBroadcastSkipFor(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()

	feed, identity, remote, actor := exclude_test_entities(t)
	users := db_open("db/users.db")
	users.exec("insert into users (uid, username) values ('u-owner', 'owner@x')")
	users.exec("insert into entities (id, private, fingerprint, user, class, name) values (?, '', ?, 'u-owner', 'feed', 'Feed')", feed, fingerprint(feed))
	// user_owning_entity resolves only users with a person identity.
	users.exec("insert into entities (id, private, fingerprint, user, class, name) values (?, '', ?, 'u-owner', 'person', 'Owner')", identity, fingerprint(identity))

	owner := &User{UID: "u-owner"}
	other := &User{UID: "u-other"}

	// The user who owns `from` must skip, whatever entity receives.
	if !broadcast_skip_for(owner, feed, remote, map[string]any{}) {
		t.Error("owner of the from entity must skip the handler")
	}
	// Any other user applies normally.
	if broadcast_skip_for(other, feed, remote, map[string]any{}) {
		t.Error("a non-owner must not skip")
	}
	// The excluded actor skips even when they don't own the from entity.
	content := map[string]any{broadcast_content_exclude: actor}
	if !broadcast_skip_for(other, feed, actor, content) {
		t.Error("the excluded actor must skip the handler")
	}
	// A different recipient of the same event applies normally.
	if broadcast_skip_for(other, feed, remote, content) {
		t.Error("a non-excluded recipient must not skip")
	}
}

// TestBroadcastSendExcludeRidesPayloadAndSkipsSelf drives the real
// api_broadcast_send: the exclusion must land in the log row's data (so
// resync replays carry it), the excluded REMOTE subscriber must still get
// a delivery (stream continuity — no hole), and a subscriber owned by the
// sending user must get nothing at all.
func TestBroadcastSendExcludeRidesPayloadAndSkipsSelf(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()

	feed, self, remote, actor := exclude_test_entities(t)
	users := db_open("db/users.db")
	users.exec("insert into users (uid, username) values ('u-owner', 'owner@x')")
	users.exec("insert into entities (id, private, fingerprint, user, class, name) values (?, '', ?, 'u-owner', 'feed', 'Feed')", feed, fingerprint(feed))
	users.exec("insert into entities (id, private, fingerprint, user, class, name) values (?, '', ?, 'u-owner', 'person', 'Self')", self, fingerprint(self))

	user := user_by_uid("u-owner")
	if user == nil {
		t.Fatal("user_by_uid")
	}
	app := &App{id: "testapp"}

	thread := &sl.Thread{}
	thread.SetLocal("user", user)
	thread.SetLocal("app", app)

	subscribers := sl.NewList([]sl.Value{
		sl.String(self),   // owned by the sender: must not be sent to
		sl.String(remote), // remote subscriber
		sl.String(actor),  // remote AND the excluded actor
	})
	data := sl.NewDict(1)
	_ = data.SetKey(sl.String("body"), sl.String("hello"))

	builtin := sl.NewBuiltin("mochi.broadcast.send", api_broadcast_send)
	result, err := api_broadcast_send(thread, builtin, sl.Tuple{
		sl.String(feed), sl.String(feed), subscribers,
		sl.String("feeds"), sl.String("post/create"), data, sl.String(actor),
	}, nil)
	if err != nil {
		t.Fatalf("api_broadcast_send: %v", err)
	}
	if sequence, err := sl.AsInt32(result); err != nil || sequence != 1 {
		t.Fatalf("allocated sequence = %v, want 1", result)
	}

	// The log row carries the exclusion for replays.
	sysdb := db_app_system(user, app)
	if sysdb == nil {
		t.Fatal("no system db")
	}
	defer sysdb.close()
	row, _ := sysdb.row("select data from log where key=? and sequence=1", feed)
	if row == nil {
		t.Fatal("no log row written")
	}
	logged, _ := row["data"].(string)
	if !strings.Contains(logged, `"_exclude":"`+actor+`"`) {
		t.Errorf("log data must carry the exclusion; got %s", logged)
	}

	// Fan-out: the self-owned subscriber got nothing; the remote ones —
	// including the excluded actor — each queued one row (unknown
	// entities queue with an empty target awaiting resolution).
	// m.send() runs on a goroutine, so poll briefly for the rows.
	queue := db_open("db/queue.db")
	deadline := time.Now().Add(5 * time.Second)
	for _, recipient := range []string{remote, actor} {
		for queue.integer("select count(*) from queue where to_entity=?", recipient) != 1 {
			if time.Now().After(deadline) {
				t.Fatalf("recipient %s: queue row never appeared", recipient)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	if n := queue.integer("select count(*) from queue where to_entity=?", self); n != 0 {
		t.Errorf("self-owned subscriber must not be sent to; got %d rows", n)
	}
}

// Mochi server: Matrix room snapshots as changes, the state event each replaced,
// and who an event wakes.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/spec"
	sl "go.starlark.net/starlark"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/id"
)

// snapshots_test_store lays the Matrix room store in a fresh directory.
func snapshots_test_store(t *testing.T) {
	t.Helper()
	test_bare_directory(t)
	federation_create(networks_database())
	rooms_kept_forget("")
	t.Cleanup(func() { rooms_kept_forget("") })
}

// snapshots_test_state is a state of n keys.
func snapshots_test_state(n int) rooms_state {
	state := rooms_state{}
	for i := 0; i < n; i++ {
		state[rooms_key(spec.MRoomMember, fmt.Sprintf("@u%d:example.org", i))] = fmt.Sprintf("$member%d", i)
	}
	return state
}

// snapshots_test_row is a stored snapshot's base, depth and stored length.
func snapshots_test_row(t *testing.T, id int64) (int64, int64, int) {
	t.Helper()
	row, _ := networks_database().row("select base, depth, length(state) as size from snapshots where id=?", id)
	if row == nil {
		t.Fatalf("snapshot %d not stored", id)
	}
	return row_int(row, "base"), row_int(row, "depth"), int(row_int(row, "size"))
}

// TestRoomsSnapshotsStoreChanges pins that a state close to an earlier one is
// stored as its changes, read back whole, with a full copy at least every
// rooms_snapshot_depth steps, and that a state far from every base is stored
// whole.
func TestRoomsSnapshotsStoreChanges(t *testing.T) {
	snapshots_test_store(t)
	const room = "!changes:example.org"
	first := snapshots_test_state(200)
	id := rooms_snapshot_save(room, first)
	if base, depth, _ := snapshots_test_row(t, id); base != 0 || depth != 0 {
		t.Fatalf("the first snapshot has base %d, depth %d, want a full copy", base, depth)
	}
	_, _, whole := snapshots_test_row(t, id)

	second := maps.Clone(first)
	second[rooms_key(spec.MRoomMember, "@u1:example.org")] = "$changed"
	delete(second, rooms_key(spec.MRoomMember, "@u2:example.org"))
	second[rooms_key("m.room.topic", "")] = "$topic"
	next := rooms_snapshot_save(room, second, id)
	base, depth, size := snapshots_test_row(t, next)
	if base != id || depth != 1 || size*10 > whole {
		t.Errorf("a state two keys from its base is stored with base %d, depth %d, %d bytes against %d whole", base, depth, size, whole)
	}

	rooms_kept_forget("")
	if got := rooms_snapshot(next); !maps.Equal(got, second) {
		t.Errorf("the changes read back as %d keys, want the %d stored", len(got), len(second))
	}
	if got := rooms_snapshot(id); !maps.Equal(got, first) {
		t.Errorf("the base reads back as %d keys, want %d", len(got), len(first))
	}

	// A chain of single changes: none more than rooms_snapshot_depth steps from
	// a full copy, every one read back as stored.
	states := map[int64]rooms_state{}
	previous, state := next, second
	for i := 0; i < rooms_snapshot_depth+5; i++ {
		state = maps.Clone(state)
		state[rooms_key("m.room.topic", "")] = "$topic" + strconv.Itoa(i)
		previous = rooms_snapshot_save(room, state, previous)
		states[previous] = state
	}
	rooms_kept_forget("")
	fulls := 0
	for id, want := range states {
		base, depth, _ := snapshots_test_row(t, id)
		if depth > rooms_snapshot_depth {
			t.Fatalf("snapshot %d is %d steps from a full copy, over %d", id, depth, rooms_snapshot_depth)
		}
		if base == 0 {
			fulls++
		}
		if got := rooms_snapshot(id); !maps.Equal(got, want) {
			t.Fatalf("snapshot %d at depth %d read back wrong", id, depth)
		}
	}
	if fulls == 0 {
		t.Errorf("a chain of %d changes never stored a full copy", len(states))
	}

	// A state sharing little with its base is stored whole.
	other := snapshots_test_state(10)
	far := rooms_snapshot_save(room, other, previous)
	if base, _, _ := snapshots_test_row(t, far); base != 0 {
		t.Errorf("a state far from its base was stored as changes from %d", base)
	}
}

// TestRoomsSnapshotsStayInTheirRoom pins that a snapshot is never stored as
// changes from another room's, and that a chain leaving its room is not read:
// deleting one room's snapshots must not break another's, and a chain this
// server did not write is not trusted.
func TestRoomsSnapshotsStayInTheirRoom(t *testing.T) {
	snapshots_test_store(t)
	state := snapshots_test_state(50)
	id := rooms_snapshot_save("!one:example.org", state)
	close := maps.Clone(state)
	close[rooms_key("m.room.topic", "")] = "$topic"
	other := rooms_snapshot_save("!two:example.org", close, id)
	if base, _, _ := snapshots_test_row(t, other); base != 0 {
		t.Errorf("a snapshot of one room was stored as changes from another's (%d)", base)
	}

	db := networks_database()
	result, err := db.internal.Exec("insert into snapshots (room, state, base, depth) values (?, ?, ?, 1)", "!two:example.org", `{"set":{"m.room.topic\u0000":"$x"}}`, id)
	if err != nil {
		t.Fatal(err)
	}
	crossing, _ := result.LastInsertId()
	rooms_kept_forget("")
	if got := rooms_snapshot(crossing); len(got) != 0 {
		t.Errorf("a chain crossing rooms read as %d keys, want none", len(got))
	}
}

// TestRoomsSnapshotsHandOutCopies pins that a state handed out is the
// caller's to change: the kept one is shared by every reader.
func TestRoomsSnapshotsHandOutCopies(t *testing.T) {
	snapshots_test_store(t)
	id := rooms_snapshot_save("!copies:example.org", snapshots_test_state(5))
	mine := rooms_snapshot(id)
	mine["changed"] = "$mine"
	if _, found := rooms_snapshot(id)["changed"]; found {
		t.Error("a change to one caller's state reached the next")
	}
	if _, found := rooms_snapshot_view(id)["changed"]; found {
		t.Error("a change to one caller's state reached the kept one")
	}
}

// TestRoomsSnapshotsLoadAKeptOne pins that a load finding the snapshot kept on
// its loop's first pass answers it. Two loads of one snapshot at once reached
// that pass with no chain to apply - the other had kept it between the load's
// two lookups - and indexed the empty chain: a panic, which on a presence
// goroutine ended the server. The loop's lookup is now the only one, so every
// kept snapshot goes that way.
func TestRoomsSnapshotsLoadAKeptOne(t *testing.T) {
	snapshots_test_store(t)
	id := rooms_snapshot_save("!kept:example.org", snapshots_test_state(4))
	if rooms_snapshot_load(id) == nil {
		t.Fatal("the snapshot did not load")
	}
	kept := rooms_snapshot_load(id)
	if kept == nil || len(kept.state) != 4 {
		t.Fatalf("the kept snapshot loaded as %v, want its 4 keys", kept)
	}
}

// TestRoomsSnapshotsKeptPerDatabase pins that a kept snapshot belongs to its
// database: ids start again in each.
func TestRoomsSnapshotsKeptPerDatabase(t *testing.T) {
	snapshots_test_store(t)
	first := rooms_snapshot_save("!a:example.org", snapshots_test_state(3))
	test_bare_directory(t)
	federation_create(networks_database())
	second := rooms_snapshot_save("!a:example.org", snapshots_test_state(7))
	if first != second {
		t.Skipf("ids %d and %d differ, so this run cannot tell", first, second)
	}
	if got := rooms_snapshot(second); len(got) != 7 {
		t.Errorf("the second database's snapshot %d reads as %d keys, the first's", second, len(got))
	}
}

// TestRoomsSnapshotsKeptBounded pins that the decoded snapshots kept stay
// within rooms_snapshot_memory, the least recently used going first and read
// again from the store.
func TestRoomsSnapshotsKeptBounded(t *testing.T) {
	snapshots_test_store(t)
	saved := rooms_snapshot_memory
	rooms_snapshot_memory = 20000
	defer func() { rooms_snapshot_memory = saved }()
	ids := []int64{}
	for i := 0; i < 10; i++ {
		ids = append(ids, rooms_snapshot_save(fmt.Sprintf("!bounded%d:example.org", i), snapshots_test_state(100)))
	}
	rooms_kept_lock.Lock()
	size := rooms_kept_size
	_, first := rooms_kept_entries[rooms_kept_key{networks_database().path, ids[0]}]
	rooms_kept_lock.Unlock()
	if size > rooms_snapshot_memory {
		t.Errorf("%d bytes kept, over the bound of %d", size, rooms_snapshot_memory)
	}
	if first {
		t.Error("the least recently used snapshot was kept past the bound")
	}
	if got := rooms_snapshot(ids[0]); len(got) != 100 {
		t.Errorf("a dropped snapshot reads back as %d keys", len(got))
	}
}

// TestRoomsKeptForgetsOneRoom pins that forgetting a room's kept snapshots
// leaves every other room's.
func TestRoomsKeptForgetsOneRoom(t *testing.T) {
	snapshots_test_store(t)
	gone := rooms_snapshot_save("!gone:example.org", snapshots_test_state(3))
	kept := rooms_snapshot_save("!kept:example.org", snapshots_test_state(3))
	rooms_kept_forget("!gone:example.org")
	path := networks_database().path
	if rooms_kept_get(rooms_kept_key{path, gone}) != nil {
		t.Error("the forgotten room's snapshot is still kept")
	}
	if rooms_kept_get(rooms_kept_key{path, kept}) == nil {
		t.Error("another room's snapshot was dropped with it")
	}
}

// replaced_test_room is a remote room alice is in, with members who join, one
// who changes name, a topic set twice and a message.
func replaced_test_room(t *testing.T) (string, *matrix_remote) {
	t.Helper()
	alice, _ := create_matrix_test_env(t)
	r := create_matrix_remote(t, gomatrixserverlib.RoomVersionV10)
	room := r.room("Replaced", "public")
	matrix_join(t, r, alice, room)
	var events []gomatrixserverlib.PDU
	for i := 0; i < 5; i++ {
		user := r.user("u" + strconv.Itoa(i))
		events = append(events, r.event(room, user, spec.MRoomMember, &user, map[string]any{"membership": "join", "displayname": "User " + strconv.Itoa(i)}))
	}
	r.push(t, "joins", events...)
	user := r.user("u1")
	empty := ""
	r.push(t, "rename", r.event(room, user, spec.MRoomMember, &user, map[string]any{"membership": "join", "displayname": "Renamed"}))
	r.push(t, "topic1", r.event(room, r.user("bob"), "m.room.topic", &empty, map[string]any{"topic": "first"}))
	r.push(t, "topic2", r.event(room, r.user("bob"), "m.room.topic", &empty, map[string]any{"topic": "second"}))
	r.push(t, "message", r.event(room, user, "m.room.message", nil, map[string]any{"msgtype": "m.text", "body": "hello"}))
	federation_workers_drain()
	return room, r
}

// TestRoomsReplacedStoredWithEvents pins that each state event is kept with
// the event it replaced, the one its key held in the state before it, as
// reading that snapshot answers, and that clients are shown it from there.
func TestRoomsReplacedStoredWithEvents(t *testing.T) {
	room, r := replaced_test_room(t)
	rows, _ := networks_database().rows("select id, type, key, before, replaces from events where room=? and key is not null and before>0", room)
	if len(rows) < 8 {
		t.Fatalf("only %d state events kept", len(rows))
	}
	replacing := 0
	for _, row := range rows {
		replaces, stored := row["replaces"].(string)
		if !stored {
			t.Errorf("state event %s was kept without what it replaced", row_string(row, "id"))
			continue
		}
		key, _ := row["key"].(string)
		if want := rooms_snapshot(row_int(row, "before"))[rooms_key(row_string(row, "type"), key)]; replaces != want {
			t.Errorf("state event %s replaces %q, the snapshot before it %q", row_string(row, "id"), replaces, want)
		}
		if replaces != "" {
			replacing++
		}
	}
	if replacing < 2 {
		t.Errorf("%d state events replaced another, want the rename and the second topic at least", replacing)
	}

	user := r.user("u1")
	rename := rooms_event(rooms_current_view(room)[rooms_key(spec.MRoomMember, user)])
	previous := clients_previous(rename)
	if previous == nil {
		t.Fatal("the rename shows no previous member event")
	}
	var content map[string]any
	json.Unmarshal(previous.Content(), &content)
	if content["displayname"] != "User 1" {
		t.Errorf("the rename replaced %v, want the join as User 1", content)
	}

	// A row kept before the column was filled reads from its snapshot.
	networks_database().exec("update events set replaces=null where id=?", rename.EventID())
	if got := rooms_replaced(rename); got != previous.EventID() {
		t.Errorf("a row with nothing stored reads as replacing %q, want %q", got, previous.EventID())
	}
}

// TestSchema22FillsReplaced pins that the upgrade fills in, for state events
// kept before it, the event each replaced, from the snapshot before it, and
// leaves events that are not state alone.
func TestSchema22FillsReplaced(t *testing.T) {
	test_bare_directory(t)
	rooms_kept_forget("")
	t.Cleanup(func() { rooms_kept_forget("") })
	matrix := db_open("db/networks.db")
	db_upgrade_18_federation(matrix)
	const room = "!old:example.org"
	key := rooms_key("m.room.topic", "")
	matrix.exec("insert into snapshots (room, state) values (?, ?)", room, json_encode(rooms_state{key: "$first"}))
	matrix.exec("insert into snapshots (room, state) values (?, ?)", room, json_encode(rooms_state{}))
	insert := func(id, kind string, key any, before int) {
		matrix.exec("insert into events (id, room, type, key, sender, depth, body, before, received) values (?, ?, ?, ?, '@a:example.org', 1, '{}', ?, 1)", id, room, kind, key, before)
	}
	insert("$second", "m.room.topic", "", 1)
	insert("$first", "m.room.topic", "", 2)
	insert("$message", "m.room.message", nil, 1)
	// A snapshot missing, unreadable or null reads as empty, as the live view
	// read it when the step was released.
	matrix.exec("insert into snapshots (room, state) values (?, 'not json'), (?, 'null')", room, room)
	insert("$missing", "m.room.topic", "", 99)
	insert("$unreadable", "m.room.topic", "", 3)
	insert("$null", "m.room.topic", "", 4)
	// More than one batch of them, so every batch is filled.
	tx, _ := matrix.internal.Begin()
	for i := 0; i < 2500; i++ {
		tx.Exec("insert into events (id, room, type, key, sender, depth, body, before, received) values (?, ?, 'm.room.topic', '', '@a:example.org', 1, '{}', 1, 1)", "$many"+strconv.Itoa(i), room)
	}
	tx.Commit()

	db_upgrade_22()
	rooms_kept_lock.Lock()
	kept := len(rooms_kept_entries)
	rooms_kept_lock.Unlock()
	if kept != 0 {
		t.Errorf("THE POINT: the step left %d snapshots in the server's cache: it read them through the live loader, so a later change to that would change the released step", kept)
	}
	for id, want := range map[string]any{"$second": "$first", "$first": "", "$message": nil, "$missing": "", "$unreadable": "", "$null": ""} {
		row, _ := matrix.row("select replaces from events where id=?", id)
		if got := row["replaces"]; fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("event %s replaces %v after the upgrade, want %v", id, got, want)
		}
	}
	if row, _ := matrix.row("select count(*) as n from events where id like '$many%' and replaces='$first'"); row_int(row, "n") != 2500 {
		t.Errorf("THE POINT: %d of 2500 events filled across batches", row_int(row, "n"))
	}
}

// TestKeyringLeaveAsksSharingOnce pins that leaving a room works out who the
// leaver shares a room with once, not once per member: per member, leaving a
// large room walked every room the leaver is in once per member.
func TestKeyringLeaveAsksSharingOnce(t *testing.T) {
	alice, _ := create_matrix_test_env(t)
	r := create_matrix_remote(t, gomatrixserverlib.RoomVersionV10)
	first := r.room("First", "public")
	second := r.room("Second", "public")
	matrix_join(t, r, alice, first)
	matrix_join(t, r, alice, second)
	for _, room := range []string{first, second} {
		var joins []gomatrixserverlib.PDU
		for i := 0; i < 30; i++ {
			user := r.user("m" + strconv.Itoa(i))
			joins = append(joins, r.event(room, user, spec.MRoomMember, &user, map[string]any{"membership": "join"}))
		}
		r.push(t, "joins"+room, joins...)
	}
	federation_workers_drain()

	asked := map[string]int{}
	keyring_sharing_counted = func(user string) { asked[user]++ }
	defer func() { keyring_sharing_counted = nil }()
	if err := rooms_leave(context.Background(), "@alice:example.com", first); err != nil {
		t.Fatal(err)
	}
	if n := asked["@alice:example.com"]; n > 2 {
		t.Errorf("leaving a room of 30 worked out alice's sharing %d times", n)
	}
	row, _ := networks_database().row("select count(*) as n from changes where gone=0")
	if row_int(row, "n") == 0 {
		t.Error("leaving noted no device-list change")
	}
}

// TestRulesWeighNamesAndCounts pins the two room facts a rule reads: the
// user's own name, which a message naming them highlights for, and how many
// have joined, which makes a room one to one.
func TestRulesWeighNamesAndCounts(t *testing.T) {
	room, r := replaced_test_room(t)
	state := rooms_current_view(room)
	// A member whose name does not hold their user name, which a rule of its
	// own matches.
	user := r.user("u3")
	name := rooms_name(state, user)
	if name != "User 3" {
		t.Fatalf("the member's name is %q", name)
	}
	named := r.event(room, r.user("u2"), "m.room.message", nil, map[string]any{"msgtype": "m.text", "body": "hello " + name})
	if _, highlight := rules_notifies(rules_weigh(user, named, state, 7)); !highlight {
		t.Errorf("a message naming %q does not highlight for them", name)
	}
	plain := r.event(room, r.user("u2"), "m.room.message", nil, map[string]any{"msgtype": "m.text", "body": "nothing"})
	sound := func(actions []any) bool {
		for _, action := range actions {
			if tweak, ok := action.(map[string]any); ok && tweak["set_tweak"] == "sound" {
				return true
			}
		}
		return false
	}
	if !sound(rules_weigh(user, plain, state, 2)) {
		t.Error("a message in a room of two does not sound as one to one")
	}
	if sound(rules_weigh(user, plain, state, 7)) {
		t.Error("a message in a room of seven sounds as one to one")
	}
}

// wake_test_room lays a room whose state names members, each by a member event
// of the membership given.
func wake_test_room(t *testing.T, room string, members map[string]string) {
	t.Helper()
	db := networks_database()
	state := rooms_state{}
	for member, membership := range members {
		id := "$" + room + member
		db.exec("insert into events (id, room, type, key, membership, sender, depth, body, received) values (?, ?, 'm.room.member', ?, ?, ?, 1, '{}', ?)", id, room, member, membership, member, now())
		state[rooms_key(spec.MRoomMember, member)] = id
	}
	db.exec("insert or replace into rooms (id, version, state, created) values (?, '10', ?, ?)", room, rooms_snapshot_save(room, state), now())
}

// wake_test_waiters sets each user waiting, answering which a call woke.
func wake_test_waiters(users ...string) func(func()) []string {
	return func(call func()) []string {
		waiting := map[string]chan struct{}{}
		var done []func()
		for _, user := range users {
			channel, release := clients_waiter(user)
			waiting[user] = channel
			done = append(done, release)
		}
		call()
		woken := []string{}
		for _, user := range users {
			select {
			case <-waiting[user]:
				woken = append(woken, user)
			default:
			}
		}
		for _, release := range done {
			release()
		}
		return woken
	}
}

// TestClientsWakeOnlyThoseConcerned pins who each change wakes: a room's
// typing and receipts its members, a user's settings, invitations,
// notifications read, rules and what is sent to their devices that user, a
// presence or a change to someone's devices everyone sharing a room with
// them, and a change of unworked-out reach everyone.
func TestClientsWakeOnlyThoseConcerned(t *testing.T) {
	snapshots_test_store(t)
	const amy, ben, cat, zed = "@amy:example.com", "@ben:example.com", "@cat:example.com", "@zed:example.org"
	wake_test_room(t, "!shared:example.com", map[string]string{amy: spec.Join, ben: spec.Join, zed: spec.Join})
	wake_test_room(t, "!apart:example.com", map[string]string{cat: spec.Join})
	networks_database().exec("insert into devices (user, device, created) values (?, 'AMY1', 1)", amy)
	wake := wake_test_waiters(amy, ben, cat)
	t.Cleanup(func() { typing_set("!shared:example.com", amy, false, 0) })

	for name, c := range map[string]struct {
		call func()
		want string
	}{
		"typing":            {func() { typing_set("!shared:example.com", amy, true, time.Minute) }, "[@amy:example.com @ben:example.com]"},
		"receipt":           {func() { receipts_set("!apart:example.com", cat, "m.read", "", "$e", 0) }, "[@cat:example.com]"},
		"setting":           {func() { clients_setting_set(ben, "", "m.test", json.RawMessage(`{}`)) }, "[@ben:example.com]"},
		"invitation":        {func() { clients_pending("!elsewhere:example.org", cat, "invite", "example.org", "[]") }, "[@cat:example.com]"},
		"notifications":     {func() { notifications_read(amy, "!shared:example.com", 1) }, "[@amy:example.com]"},
		"rule":              {func() { rules_keep(ben, "override", "test", 0, map[string]any{"actions": []any{}}, true) }, "[@ben:example.com]"},
		"presence":          {func() { clients_presence_record(amy, "online", "", true) }, "[@amy:example.com @ben:example.com]"},
		"room with someone": {func() { clients_wake_room("!apart:example.com", amy) }, "[@amy:example.com @cat:example.com]"},
		"to device":         {func() { keyring_deliver(zed, amy, "m.test", map[string]json.RawMessage{"*": json.RawMessage(`{}`)}) }, "[@amy:example.com]"},
		"devices":           {func() { keyring_changed(ben, "BEN1", nil, true) }, "[@amy:example.com @ben:example.com]"},
		"signing keys":      {func() { keyring_signing_changed(cat) }, "[@cat:example.com]"},
		"remote devices": {func() {
			keyring_receive("example.org", "m.device_list_update", json.RawMessage(`{"user_id":"`+zed+`"}`))
		}, "[@amy:example.com @ben:example.com]"},
		"everyone": {func() { clients_wake() }, "[@amy:example.com @ben:example.com @cat:example.com]"},
	} {
		if got := fmt.Sprint(wake(c.call)); got != c.want {
			t.Errorf("%s woke %s, want %s", name, got, c.want)
		}
	}
}

// TestClientsWakeSharingJoined pins whom a change to someone's presence or
// devices wakes: those joined to a room the person is joined or invited to,
// not those merely invited to it.
func TestClientsWakeSharingJoined(t *testing.T) {
	snapshots_test_store(t)
	const amy, dan, zed, yan = "@amy:example.com", "@dan:example.com", "@zed:example.org", "@yan:example.org"
	wake_test_room(t, "!joined:example.com", map[string]string{amy: spec.Join, dan: spec.Invite, zed: spec.Join})
	wake_test_room(t, "!invited:example.com", map[string]string{amy: spec.Join, yan: spec.Invite})
	wake := wake_test_waiters(amy, dan)
	if got := fmt.Sprint(wake(func() { clients_wake_sharing(zed) })); got != "[@amy:example.com]" {
		t.Errorf("THE POINT: a change of zed's woke %s, want amy alone (dan is only invited)", got)
	}
	if got := fmt.Sprint(wake(func() { clients_wake_sharing(yan) })); got != "[@amy:example.com]" {
		t.Errorf("THE POINT: a change of yan's, invited where amy is joined, woke %s, want amy", got)
	}
}

// TestClientsWakeSharingRemoteMembers pins that a change of a remote member's
// devices or presence wakes those sharing a room with them, also a member
// who was there before the room was joined, whose member event the join kept
// as an outlier.
func TestClientsWakeSharingRemoteMembers(t *testing.T) {
	alice, _ := create_matrix_test_env(t)
	r := create_matrix_remote(t, gomatrixserverlib.RoomVersionV10)
	room := r.room("Shared", "public")
	matrix_join(t, r, alice, room)
	bob := r.user("bob")
	wake := wake_test_waiters("@alice:example.com")
	if got := fmt.Sprint(wake(func() {
		keyring_receive(r.name, "m.device_list_update", json.RawMessage(`{"user_id":"`+bob+`"}`))
	})); got != "[@alice:example.com]" {
		t.Errorf("THE POINT: bob's new device woke %s, want alice", got)
	}
	if got := fmt.Sprint(wake(func() { clients_presence_record(bob, "online", "", true) })); got != "[@alice:example.com]" {
		t.Errorf("THE POINT: bob's presence woke %s, want alice", got)
	}
}

// TestClientsWaitersLeaveNothing pins that a sync which stops waiting
// unwoken takes its wait away, and that one of a user's two syncs stopping
// leaves the other woken as before.
func TestClientsWaitersLeaveNothing(t *testing.T) {
	const amy, ben = "@amy:example.com", "@ben:example.com"
	clients_waking_lock.Lock()
	clients_waking = map[string]*clients_wait{}
	clients_waking_lock.Unlock()
	count := func() int {
		clients_waking_lock.Lock()
		defer clients_waking_lock.Unlock()
		return len(clients_waking)
	}
	_, gone := clients_waiter(ben)
	gone()
	if count() != 0 {
		t.Error("THE POINT: a sync that stopped waiting unwoken left its wait behind")
	}
	_, first := clients_waiter(amy)
	second, last := clients_waiter(amy)
	first()
	clients_wake(amy)
	select {
	case <-second:
	default:
		t.Error("with one of amy's syncs gone, the other was not woken")
	}
	last()
	if count() != 0 {
		t.Errorf("%d waits left behind", count())
	}
}

// TestClientsTypingRunOutWakes pins that typing which runs out unrenewed
// wakes the room's syncs to show it stopped, which nothing else would.
func TestClientsTypingRunOutWakes(t *testing.T) {
	snapshots_test_store(t)
	const amy, ben, room = "@amy:example.com", "@ben:example.com", "!typing:example.com"
	wake_test_room(t, room, map[string]string{amy: spec.Join, ben: spec.Join})
	typing_set(room, amy, true, 100*time.Millisecond)
	waiting, done := clients_waiter(ben)
	defer done()
	select {
	case <-waiting:
	case <-time.After(3 * time.Second):
		t.Fatal("THE POINT: typing that ran out woke no one")
	}
	if users := typing_users(room); len(users) != 0 {
		t.Errorf("still typing after it ran out: %v", users)
	}

	// Ended first by a sync's own read of the room, which shows it to that
	// sync alone: the run out still wakes the others.
	typing_set(room, amy, true, time.Hour)
	typing_lock.Lock()
	typing_rooms[room][amy] = time.Now().Add(-time.Second)
	typing_lock.Unlock()
	typing_users(room)
	wake := wake_test_waiters(ben)
	if got := fmt.Sprint(wake(func() { typing_expired(room) })); got != "[@ben:example.com]" {
		t.Errorf("THE POINT: typing a sync's read ended first woke %s, want ben", got)
	}
	if got := fmt.Sprint(wake(func() { typing_expired(room) })); got != "[]" {
		t.Errorf("a run out already shown woke %s again", got)
	}

	// Renewing typing changes nothing a sync shows.
	typing_set(room, amy, true, time.Hour)
	if got := fmt.Sprint(wake(func() { typing_set(room, amy, true, time.Hour) })); got != "[]" {
		t.Errorf("THE POINT: renewed typing woke %s", got)
	}
	// A stop wakes the room itself, so its timer has nothing left to show.
	typing_set(room, amy, false, 0)
	if got := fmt.Sprint(wake(func() { typing_expired(room) })); got != "[]" {
		t.Errorf("THE POINT: the timer of typing already stopped woke %s", got)
	}
}

// TestRoomsWakeAfterSettle pins that a sync is woken for an event only once
// the room has taken it: alice's own join of a room elsewhere, someone
// joining from another server, and alice's own change to the room. A sync
// woken before builds its answer without the event, and is not woken for it
// again.
func TestRoomsWakeAfterSettle(t *testing.T) {
	alice, _ := create_matrix_test_env(t)
	r := create_matrix_remote(t, gomatrixserverlib.RoomVersionV10)
	room := r.room("Settled", "public")
	const me = "@alice:example.com"
	var lock sync.Mutex
	var seen []string
	clients_woken = func(user string) {
		if user == me {
			lock.Lock()
			view := rooms_current_view(room)
			seen = append(seen, fmt.Sprintf("%s %d %s", clients_memberships(me)[room], len(view), view[rooms_key(spec.MRoomMember, me)]))
			lock.Unlock()
		}
	}
	t.Cleanup(func() { clients_woken = nil })
	woken := func(step func()) []string {
		lock.Lock()
		seen = nil
		lock.Unlock()
		_, done := clients_waiter(me)
		defer done()
		step()
		federation_workers_drain()
		lock.Lock()
		defer lock.Unlock()
		return seen
	}

	matrix_invite(t, r, room)
	got := woken(func() {
		thread := linked_thread(alice)
		decoded(t)(api_account_room_join(thread, sl.NewBuiltin("test", nil), sl.Tuple{sl.String("m1"), sl.String(room)}, nil))
	})
	view := rooms_current_view(room)
	members, joined := len(view), view[rooms_key(spec.MRoomMember, me)]
	if want := fmt.Sprintf("%s %d %s", spec.Join, members, joined); len(got) == 0 || got[0] != want {
		t.Errorf("THE POINT: alice's join woke her sync seeing %v, want %q", got, want)
	}

	user := r.user("carol")
	got = woken(func() {
		r.push(t, "carol", r.event(room, user, spec.MRoomMember, &user, map[string]any{"membership": "join"}))
	})
	if want := fmt.Sprintf("%s %d %s", spec.Join, members+1, joined); len(got) == 0 || got[0] != want {
		t.Errorf("THE POINT: carol's join woke alice's sync seeing %v, want %q", got, want)
	}

	var renamed gomatrixserverlib.PDU
	got = woken(func() {
		key := me
		var err error
		if renamed, err = rooms_send(t.Context(), room, me, spec.MRoomMember, &key, map[string]any{"membership": "join", "displayname": "Alice Settled"}); err != nil {
			t.Fatal(err)
		}
	})
	if want := fmt.Sprintf("%s %d %s", spec.Join, members+1, renamed.EventID()); len(got) == 0 || got[0] != want {
		t.Errorf("THE POINT: alice's change of name woke her sync seeing %v, want %q", got, want)
	}
}

// TestRoomsSnapshotsBaseGone pins that a state whose base was deleted after
// it was read, as a room's forgetting can while a reader keeps it, is kept
// whole rather than as changes from a row no longer there.
func TestRoomsSnapshotsBaseGone(t *testing.T) {
	snapshots_test_store(t)
	const room = "!gone:example.com"
	first := snapshots_test_state(50)
	id := rooms_snapshot_save(room, first)
	networks_database().exec("delete from snapshots where id=?", id)
	second := maps.Clone(first)
	second[rooms_key("m.room.topic", "")] = "$topic"
	next := rooms_snapshot_save(room, second, id)
	if base, _, _ := snapshots_test_row(t, next); base != 0 {
		t.Errorf("THE POINT: the state was kept as changes from %d, which is gone", base)
	}
	rooms_kept_forget(room)
	if got := rooms_snapshot_view(next); !maps.Equal(got, second) {
		t.Errorf("THE POINT: the state read back has %d keys, want %d", len(got), len(second))
	}
}

// TestClientsEventWakesItsRoom pins that an event a room takes wakes that
// room's members, not every sync waiting on the server.
func TestClientsEventWakesItsRoom(t *testing.T) {
	room, r := replaced_test_room(t)
	const outside = "@zed:example.com"
	wake := wake_test_waiters("@alice:example.com", outside)
	got := wake(func() {
		r.push(t, "woken", r.event(room, r.user("u3"), "m.room.message", nil, map[string]any{"msgtype": "m.text", "body": "wake"}))
	})
	if fmt.Sprint(got) != "[@alice:example.com]" {
		t.Errorf("an event in alice's room woke %v, want alice alone", got)
	}
}

// TestRoomsReplacedSetOnPromotion pins that an event kept first as an outlier,
// with no state before it, gets what it replaced once it is taken into the
// timeline with one.
func TestRoomsReplacedSetOnPromotion(t *testing.T) {
	room, r := replaced_test_room(t)
	empty := ""
	topic := r.event(room, r.user("bob"), "m.room.topic", &empty, map[string]any{"topic": "third"})
	rooms_keep(topic, 0, false, true)
	if row, _ := networks_database().row("select replaces from events where id=?", topic.EventID()); row["replaces"] != nil {
		t.Fatalf("an outlier was kept as replacing %v", row["replaces"])
	}
	before := rooms_current_id(room)
	rooms_keep(topic, before, false, false)
	row, _ := networks_database().row("select replaces from events where id=?", topic.EventID())
	want := rooms_snapshot_view(before)[rooms_key("m.room.topic", "")]
	if got, _ := row["replaces"].(string); got == "" || got != want {
		t.Errorf("the promoted event replaces %v, want %q", row["replaces"], want)
	}
}

// TestClientsPushNamesSenderAndCounts pins two facts a push takes from the
// room: the sender's name, and how many have joined, which makes a room of two
// one to one, sounding.
func TestClientsPushNamesSenderAndCounts(t *testing.T) {
	alice, root := create_clients_test_env(t)
	ctx := t.Context()
	a := clients_client(t, alice, "m1", "@alice:example.com", "ALICE")
	r := clients_client(t, root, "m2", "@root:example.com", "ROOT")
	pushed := make(chan map[string]any, 10)
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		var body map[string]any
		json.NewDecoder(request.Body).Decode(&body)
		json.NewEncoder(w).Encode(map[string]any{"rejected": []string{}})
		pushed <- body
	}))
	t.Cleanup(gateway.Close)
	if status, body := clients_request(t, r, "POST", "/_matrix/client/v3/pushers/set", map[string]any{"kind": "http", "app_id": "org.example.app", "pushkey": "full-key", "app_display_name": "App", "device_display_name": "Phone", "lang": "en", "data": map[string]any{"url": gateway.URL + "/_matrix/push/v1/notify"}}); status != 200 {
		t.Fatalf("pusher = %d %v", status, body)
	}
	made, err := a.CreateRoom(ctx, &mautrix.ReqCreateRoom{Invite: []id.UserID{"@root:example.com"}, IsDirect: true})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-pushed:
	case <-time.After(5 * time.Second):
		t.Fatal("the invitation was not pushed")
	}
	if _, err := r.JoinRoomByID(ctx, made.RoomID); err != nil {
		t.Fatal(err)
	}
	if _, err := a.SendText(ctx, made.RoomID, "ping"); err != nil {
		t.Fatal(err)
	}
	want := rooms_name(rooms_current_view(string(made.RoomID)), "@alice:example.com")
	select {
	case body := <-pushed:
		notification, _ := body["notification"].(map[string]any)
		if want == "" || notification["sender_display_name"] != want {
			t.Errorf("the push names its sender %v, want %q", notification["sender_display_name"], want)
		}
		devices, _ := notification["devices"].([]any)
		device, _ := devices[0].(map[string]any)
		tweaks, _ := device["tweaks"].(map[string]any)
		if tweaks["sound"] == nil {
			t.Errorf("a message in a room of two pushed with tweaks %v, want the one to one sound", device["tweaks"])
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the message was not pushed")
	}
}

// TestLinkedPresenceContainsAPanic pins that what a presence change tells -
// the signals and chat updates, which read rooms' snapshots, as a snapshot
// load once panicked - runs guarded: each runs on a goroutine of its own, so a
// panic in either would otherwise end the server.
func TestLinkedPresenceContainsAPanic(t *testing.T) {
	t.Cleanup(func() { presence_signals, presence_chat = nil, nil })
	for _, name := range []string{"signals", "chat"} {
		presence_signals, presence_chat = func(string) {}, func(string) {}
		if name == "signals" {
			presence_signals = func(string) { panic("presence signals") }
		} else {
			presence_chat = func(string) { panic("presence chat") }
		}
		linked_presence_notify("u-panic")
		presence_pending.Wait()
	}
}

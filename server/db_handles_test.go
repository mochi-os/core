// Mochi server: per-user database handle tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"path/filepath"
	"testing"
	"time"
)

// handle_user is a user whose files live in the test's data directory.
func handle_user(t *testing.T, uid string) *User {
	t.Helper()
	return &User{UID: uid, Username: uid + "@example.com", Role: "user", Preferences: map[string]string{}}
}

// handle_key is the cache key of a user's file.
func handle_key(u *User, name string) string {
	return filepath.Join(data_dir, "users", u.UID, name+".db")
}

// handle_state is what the cache holds for a user's file: whether it is open,
// how many hold it, and whether db_manager would close it a minute from now.
func handle_state(u *User, name string) (bool, int64, bool) {
	databases_lock.Lock()
	defer databases_lock.Unlock()
	db, found := databases[handle_key(u, name)]
	if !found {
		return false, 0, false
	}
	return true, db.holders, db_idle(db, now()+61)
}

// TestAReleasedUserHandleIsClosedAMinuteLater. A user's file stayed open for
// the life of the process: nothing ever let go of it.
func TestAReleasedUserHandleIsClosedAMinuteLater(t *testing.T) {
	test_data_directory(t)
	u := handle_user(t, "released")
	db := db_user(u, "user")
	if open, holders, idle := handle_state(u, "user"); !open || holders != 1 || idle {
		t.Fatalf("held: open %v, holders %d, idle %v", open, holders, idle)
	}
	db.close()
	if _, holders, idle := handle_state(u, "user"); holders != 0 || !idle {
		t.Fatalf("THE POINT: a released handle is closed a minute later: holders %d, idle %v", holders, idle)
	}
	db_evict(now()+61, false)
	if open, _, _ := handle_state(u, "user"); open {
		t.Fatal("the released handle is still open after the manager's pass")
	}
	// The next use opens the file again, with what it held.
	again := db_user(u, "user")
	defer again.close()
	again.exec("replace into preferences (name, value) values ('language', 'fr')")
	if row, err := again.row("select value from preferences where name='language'"); err != nil || row_string(row, "value") != "fr" {
		t.Fatalf("the reopened handle reads %v, %v", row, err)
	}
}

// TestAHeldUserHandleOutlastsTheMinute. Another caller's release marks the
// handle idle; one still holding it must not have it closed underneath.
func TestAHeldUserHandleOutlastsTheMinute(t *testing.T) {
	test_data_directory(t)
	u := handle_user(t, "held")
	held := db_user(u, "user")
	db_user_brief(u, "user").exec("replace into preferences (name, value) values ('theme', 'dark')")
	if _, holders, idle := handle_state(u, "user"); holders != 1 || idle {
		t.Fatalf("THE POINT: a handle still held is not idle: holders %d, idle %v", holders, idle)
	}
	db_evict(now()+61, false)
	row, err := held.row("select value from preferences where name='theme'")
	if err != nil || row_string(row, "value") != "dark" {
		t.Fatalf("the holder's handle was closed under it: %v, %v", row, err)
	}
	held.close()
	if _, _, idle := handle_state(u, "user"); !idle {
		t.Fatal("the last holder's release leaves the handle in use")
	}
}

// TestABriefUserHandleIsReleasedAtOnce. One call made at once keeps nothing.
func TestABriefUserHandleIsReleasedAtOnce(t *testing.T) {
	test_data_directory(t)
	u := handle_user(t, "brief")
	db_user_brief(u, "user").exec("replace into preferences (name, value) values ('a', 'b')")
	if open, holders, idle := handle_state(u, "user"); !open || holders != 0 || !idle {
		t.Fatalf("THE POINT: a brief open leaves no hold: open %v, holders %d, idle %v", open, holders, idle)
	}
}

// TestDiscardLeavesAHeldUserHandleOpen. A walk that opened a user's file
// closed it outright when done, though a caller that found it meanwhile was
// still using it.
func TestDiscardLeavesAHeldUserHandleOpen(t *testing.T) {
	test_data_directory(t)
	u := handle_user(t, "discard")
	db_user(u, "user").close() // the file exists, with its tables
	db_evict(now()+61, false)

	walk, _, reused := db_open_work(filepath.Join("users", u.UID, "user.db"))
	if reused {
		t.Fatal("the walk found a handle it should have opened")
	}
	held := db_user(u, "user")
	db_discard(walk, reused)
	if _, err := held.row("select count(*) as n from preferences"); err != nil {
		t.Fatalf("THE POINT: the walk closed the file under its holder: %v", err)
	}
	if _, holders, _ := handle_state(u, "user"); holders != 1 {
		t.Fatalf("holders %d after the walk let go, want 1", holders)
	}

	// A walk that found the handle already open releases its own open.
	found, _, reused := db_open_work(filepath.Join("users", u.UID, "user.db"))
	if !reused {
		t.Fatal("the second walk opened a new handle")
	}
	db_discard(found, reused)
	held.close()
	if _, holders, idle := handle_state(u, "user"); holders != 0 || !idle {
		t.Fatalf("after the walk and the holder let go: holders %d, idle %v", holders, idle)
	}
}

// TestTheStartUpSeedLeavesNoUserHandleHeld. A second after start, and daily,
// the permission seed looked up every active user, so every one of their
// files stayed open from then on.
func TestTheStartUpSeedLeavesNoUserHandleHeld(t *testing.T) {
	create_test_users_db(t)
	users := db_open("db/users.db")
	users.exec("create table entities (id text primary key, private text, fingerprint text, user text not null default '', parent text default '', class text, name text, privacy text default 'public', data text default '', published integer default 0)")
	uids := []string{"seeda", "seedb", "seedc"}
	for _, uid := range uids {
		users.exec("insert into users (uid, username, role) values (?, ?, 'user')", uid, uid+"@example.com")
	}

	apps_seed_default_permissions()

	for _, uid := range uids {
		u := &User{UID: uid}
		open, holders, idle := handle_state(u, "user")
		if !open {
			t.Fatalf("the seed never opened %s's file", uid)
		}
		if holders != 0 || !idle {
			t.Errorf("THE POINT: the seed left %s's file held: holders %d, idle %v", uid, holders, idle)
		}
	}
}

// TestTheInterestsRebuildHoldsTheUsersFile. The rebuild runs on after the
// caller that started it has let go; the file must stay open until it is done.
func TestTheInterestsRebuildHoldsTheUsersFile(t *testing.T) {
	test_data_directory(t)
	u := handle_user(t, "rebuild")
	db := db_user(u, "user")
	// Hold the write lock, so the rebuild stops at its write.
	tx, err := db.internal.Beginx()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.Exec("replace into preferences (name, value) values ('blocker', 'x')"); err != nil {
		t.Fatalf("lock: %v", err)
	}
	if !interests_regenerate(u, db) {
		t.Fatal("no rebuild started")
	}
	db.close()
	_, holders, idle := handle_state(u, "user")
	tx.Rollback()
	if holders != 1 || idle {
		t.Errorf("THE POINT: the caller's release left the running rebuild's file unheld: holders %d, idle %v", holders, idle)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, running := interests_regenerating.Load(u.UID); !running {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the rebuild never finished")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, holders, idle := handle_state(u, "user"); holders != 0 || !idle {
		t.Errorf("the finished rebuild left the file held: holders %d, idle %v", holders, idle)
	}
}

// handle_watched says whether the vacuum and the WAL and integrity watchdogs
// see a user's file as in use.
func handle_watched(u *User, name string) bool {
	key := handle_key(u, name)
	for _, db := range db_in_use() {
		if db.key == key {
			return true
		}
	}
	return false
}

// TestAHeldUserHandleStaysWatched. Another caller's release marked a held
// user's file idle, so the WAL and integrity watchdogs passed over it for as
// long as it stayed held.
func TestAHeldUserHandleStaysWatched(t *testing.T) {
	test_data_directory(t)
	u := handle_user(t, "watched")
	held := db_user(u, "user")
	db_user_brief(u, "user").exec("replace into preferences (name, value) values ('a', 'b')")
	if !handle_watched(u, "user") {
		t.Fatal("THE POINT: another caller's release left the held file unwatched")
	}
	walk, _, reused := db_open_work(filepath.Join("users", u.UID, "user.db"))
	db_discard(walk, reused)
	if !handle_watched(u, "user") {
		t.Fatal("THE POINT: a walk's release left the held file unwatched")
	}
	held.close()
	if handle_watched(u, "user") {
		t.Fatal("the last holder's release leaves the file in use")
	}
}

// TestAUserHandleCountedAfterAnEarlierCloseIsInUse. An opener that closes
// before db_user counts the handle marks it idle under the old rule; the
// count then belongs to a caller holding it.
func TestAUserHandleCountedAfterAnEarlierCloseIsInUse(t *testing.T) {
	test_data_directory(t)
	u := handle_user(t, "counting")
	db_user(u, "user").close() // the file exists, with its tables
	db_evict(now()+61, false)
	path := filepath.Join("users", u.UID, "user.db")
	other := db_open(path)
	mine := db_open(path)
	other.close()
	db_count(mine)
	if _, holders, idle := handle_state(u, "user"); holders != 1 || idle || !handle_watched(u, "user") {
		t.Fatalf("THE POINT: the counting holder's file is idle: holders %d, idle %v, watched %v", holders, idle, handle_watched(u, "user"))
	}
	mine.close()
	if _, holders, idle := handle_state(u, "user"); holders != 0 || !idle {
		t.Fatalf("after the holder let go: holders %d, idle %v", holders, idle)
	}
}

// TestTheActivityPubLoopsReleaseEachUsersFile. Each activity at the shared
// inbox, and each Delete from an actor nobody here knew, opened the file of
// every user with an account on the fediverse, and none of those opens let go.
func TestTheActivityPubLoopsReleaseEachUsersFile(t *testing.T) {
	alice, bob := create_activitypub_test_env(t)
	db_user_brief(alice, "user").exec("insert into following (account, actor, inbox, activity, accepted, created) values ('f1', ?, ?, '', 1, 1)", bob.id, bob.inbox)
	_, before, _ := handle_state(alice, "user")
	for i := 0; i < 3; i++ {
		if len(activitypub_followers_of(bob.id)) != 1 {
			t.Fatal("alice's account is not found following bob")
		}
		activitypub_followed_by(bob.id)
		if !activitypub_known(nil, nil, bob.id) {
			t.Fatal("bob is not known here")
		}
		activitypub_known(nil, nil, "https://remote.example/u/nobody")
	}
	open, after, idle := handle_state(alice, "user")
	if !open || after != before {
		t.Fatalf("THE POINT: the loops left alice's file held: holders %d before, %d after", before, after)
	}
	if before == 0 && !idle {
		t.Error("alice's file, held by no one, is not idle after the loops")
	}
}

// TestTwoActivityPubLoopsShareAUsersFile. A file only those loops had opened
// kept the rule of the last close: one loop's release marked it idle while
// another loop still read it.
func TestTwoActivityPubLoopsShareAUsersFile(t *testing.T) {
	test_data_directory(t)
	u := handle_user(t, "loops")
	db_user(u, "user").close() // the file exists, with its tables
	db_evict(now()+61, false)
	first := activitypub_user_database(u.UID)
	second := activitypub_user_database(u.UID)
	first.close()
	if _, holders, idle := handle_state(u, "user"); holders != 1 || idle {
		t.Fatalf("THE POINT: one loop's release left the other's file idle: holders %d, idle %v", holders, idle)
	}
	second.close()
	if _, holders, idle := handle_state(u, "user"); holders != 0 || !idle {
		t.Fatalf("after both loops let go: holders %d, idle %v", holders, idle)
	}
}

// Mochi server: a.user.restore() tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"fmt"
	"strings"
	"testing"

	sl "go.starlark.net/starlark"
)

// The post-restore banner state used to ride in /_/shell and from there into
// every app frame's init. It now answers only to an app holding the sign-in
// settings' read grant, since the re-link list names e-mail addresses.
func TestUserRestore(t *testing.T) {
	create_test_users_db(t)
	db := db_open("db/users.db")
	db.exec("alter table users add column restore_source text not null default ''")
	db.exec("alter table users add column restore_passkeys integer not null default 0")
	db.exec("create table relinks (user text not null, service text not null, identifier text not null default '', linked integer not null default 0, primary key (user, service))")
	db.exec("insert into users (uid, username, restore_source, restore_passkeys) values ('u1', 'moved@example.com', 'https://old.example', 1)")
	db.exec("insert into relinks (user, service, identifier) values ('u1', 'github', 'moved@example.com')")

	user := create_permission_test_user(t, "u1")
	app := create_external_app("home-test")
	thread := create_test_thread(user, app)
	fn := sl.NewBuiltin("a.user.restore", user.restore)

	// Ungranted: refused, and the address never leaves.
	if _, err := user.restore(thread, fn, nil, nil); err == nil {
		t.Fatal("restore without user/authentication/read should be refused")
	}

	permission_grant(user, app.id, "user/authentication/read")
	value, err := user.restore(thread, fn, nil, nil)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	dict, ok := value.(*sl.Dict)
	if !ok {
		t.Fatalf("restore = %T, want dict", value)
	}
	if source, _, _ := dict.Get(sl.String("source")); source != sl.String("https://old.example") {
		t.Errorf("source = %v, want the source server", source)
	}
	if passkeys, _, _ := dict.Get(sl.String("passkeys")); passkeys != sl.True {
		t.Errorf("passkeys = %v, want True", passkeys)
	}
	relinks, _, _ := dict.Get(sl.String("relinks"))
	if text := fmt.Sprint(relinks); !strings.Contains(text, "github") || !strings.Contains(text, "moved@example.com") {
		t.Errorf("relinks = %s, want the github link with its identifier", text)
	}

	// Dismissed: nothing, whatever the row still says.
	user.Preferences = map[string]string{"restore.show": "false"}
	if value, err := user.restore(thread, fn, nil, nil); err != nil || value != sl.None {
		t.Errorf("dismissed restore = %v, %v; want None", value, err)
	}

	// An account that did not arrive by restore: nothing.
	user.Preferences = nil
	db.exec("update users set restore_source='' where uid='u1'")
	if value, err := user.restore(thread, fn, nil, nil); err != nil || value != sl.None {
		t.Errorf("unrestored account = %v, %v; want None", value, err)
	}
}

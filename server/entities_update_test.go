// Mochi server: mochi.entity.update writes all of its fields or none of them.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"strings"
	"testing"

	sl "go.starlark.net/starlark"
)

// entity_update_env creates a user with one private, classless entity and
// returns the thread and the entity id.
func entity_update_env(t *testing.T) (*sl.Thread, string) {
	t.Helper()
	setup_replication_test(t)
	setup_users_test_schema()
	user := &User{UID: "u-update", Username: "u-update@example.com"}
	db := db_open("db/users.db")
	db.exec("insert into users (uid, username) values (?, ?)", user.UID, user.Username)
	id := strings.Repeat("e", 50)
	db.exec("insert into entities (id, private, fingerprint, user, class, name, privacy) values (?, '', ?, ?, '', 'Before', 'private')", id, fingerprint(id), user.UID)
	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("user", user)
	thread.SetLocal("app", &App{id: "updater", internal: &AppVersion{Version: "1.0"}})
	return thread, id
}

func entity_update(thread *sl.Thread, id string, kwargs ...sl.Tuple) error {
	_, err := api_entity_update(thread, sl.NewBuiltin("mochi.entity.update", api_entity_update), sl.Tuple{sl.String(id)}, kwargs)
	return err
}

func entity_field(t *testing.T, id, field string) string {
	t.Helper()
	row, err := db_open("db/users.db").row("select "+field+" from entities where id=?", id)
	if err != nil || row == nil {
		t.Fatalf("entity row: %v", err)
	}
	return as_string(row[field])
}

// TestEntityUpdateIsAllOrNothing. A bad value after a good one used to leave
// the good one written and skip the republish: the caller was told nothing
// changed while the row had.
func TestEntityUpdateIsAllOrNothing(t *testing.T) {
	thread, id := entity_update_env(t)

	err := entity_update(thread, id, sl.Tuple{sl.String("name"), sl.String("After")}, sl.Tuple{sl.String("privacy"), sl.String("bogus")})
	if err == nil {
		t.Fatal("an invalid privacy was accepted")
	}
	if name := entity_field(t, id, "name"); name != "Before" {
		t.Errorf("the name was written to %q before the invalid privacy was refused", name)
	}

	err = entity_update(thread, id, sl.Tuple{sl.String("data"), sl.String("x")}, sl.Tuple{sl.String("bogus"), sl.String("y")})
	if err == nil {
		t.Fatal("an unknown parameter was accepted")
	}
	if data := entity_field(t, id, "data"); data != "" {
		t.Errorf("data was written to %q before the unknown parameter was refused", data)
	}

	if err := entity_update(thread, id, sl.Tuple{sl.String("name"), sl.String("After")}, sl.Tuple{sl.String("data"), sl.String("x")}); err != nil {
		t.Fatalf("a valid update failed: %v", err)
	}
	if name := entity_field(t, id, "name"); name != "After" {
		t.Errorf("name %q after a valid update", name)
	}
	if data := entity_field(t, id, "data"); data != "x" {
		t.Errorf("data %q after a valid update", data)
	}
}

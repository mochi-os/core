// Mochi server: a pooled handle carries the identity something reads.
//
// DB.kind tagged every handle with its per-host file role. Its own comment
// said what for: "so the matching emit/apply pair can route replicated writes
// back into the right file on the receiver". Multi-host replication was
// removed in July 2026 and took the apply half with it, leaving the tag
// computed and stored on every open with nothing left to consult it - the same
// residue #89 cleared out of the queue, one layer down.
//
// Nothing is lost by dropping it. The role it encoded is the shape of
// DB.path - users/<uid>/<app>/app.db against users/<uid>/user.db - so a future
// consumer can recover it from the handle it already has.
//
// DB.app was in this task too and is NOT dead: broadcast.go reads db.app.id to
// key broadcast_stall_clear, a reader that arrived with the #11 fix after the
// task was filed.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"strings"
	"testing"
)

// TestBindIsWriteOnce is the property db_bind exists for, and the one the
// parameter removal must not disturb. The pool key embeds user and app, so the
// values never change for a handle; binding under databases_lock replaced
// unconditional per-open writes that raced every other holder (#227 - two
// parallel Message.send goroutines both writing db.user).
func TestBindIsWriteOnce(t *testing.T) {
	first := &User{UID: "user-one"}
	second := &User{UID: "user-two"}
	app := &App{id: "app-one"}

	db := &DB{path: "users/user-one/user.db"}

	db_bind(db, first, app)
	if db.user != first || db.app != app {
		t.Fatal("db_bind did not bind an unbound handle")
	}

	// A later binder leaves it alone rather than racing the first.
	db_bind(db, second, &App{id: "app-two"})
	if db.user != first {
		t.Errorf("db_bind rebound the user to %v; the first binder must win", db.user.UID)
	}
	if db.app != app {
		t.Errorf("db_bind rebound the app to %v; the first binder must win", db.app.id)
	}
}

// TestBindHealsAnUnboundHandle: a handle first cached by a raw db_open or the
// app-system sweep carries no binding, and the next db_bind supplies it.
func TestBindHealsAnUnboundHandle(t *testing.T) {
	db := &DB{path: "users/user-one/apps/feeds/app.db"}
	if db.user != nil || db.app != nil {
		t.Fatal("a fresh handle is already bound")
	}

	user := &User{UID: "user-one"}
	app := &App{id: "feeds"}
	db_bind(db, user, app)

	if db.user != user || db.app != app {
		t.Error("db_bind did not heal a handle that carried no binding")
	}
}

// TestBindAcceptsANilApp: db_open binds a user-core handle with no app, and
// the nil must not stop the user binding from taking.
func TestBindAcceptsANilApp(t *testing.T) {
	db := &DB{path: "users/user-one/user.db"}
	user := &User{UID: "user-one"}

	db_bind(db, user, nil)

	if db.user != user {
		t.Error("db_bind did not bind the user when the app was nil")
	}
	if db.app != nil {
		t.Errorf("db_bind set app to %v from a nil argument", db.app)
	}
}

// TestNoHandleRoleTag is the gate. The tag is gone; if it returns it will
// return the way it left - written on every open, read by nothing.
func TestNoHandleRoleTag(t *testing.T) {
	source, err := os.ReadFile("db.go")
	if err != nil {
		t.Fatalf("reading db.go: %v", err)
	}
	text := string(source)

	for _, gone := range []string{"db_kind_app_system", "db_kind_user_core", "db.kind"} {
		if strings.Contains(text, gone) {
			t.Errorf("db.go references %s again; it tagged a handle for a replication apply path that no longer exists, and DB.path already carries the role", gone)
		}
	}

	// db_bind's signature is the other half: the tag was computed at one call
	// site purely to be passed here.
	if !strings.Contains(text, "func db_bind(db *DB, u *User, app *App) {") {
		t.Error("db_bind no longer takes exactly (db, user, app); if a fourth thing is being bound, it needs a reader")
	}
}

// TestDbAppHasAReader records why half of this task was already resolved, so a
// later reader does not delete DB.app as dead alongside DB.kind.
func TestDbAppHasAReader(t *testing.T) {
	source, err := os.ReadFile("broadcast.go")
	if err != nil {
		t.Fatalf("reading broadcast.go: %v", err)
	}
	if !strings.Contains(string(source), "db.app.id") {
		t.Error("broadcast.go no longer reads db.app.id. If that was the last reader, DB.app has become what DB.kind was, and the same question applies to it")
	}
}

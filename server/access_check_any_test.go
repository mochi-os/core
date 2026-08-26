// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"path/filepath"
	"strings"
	"testing"

	sl "go.starlark.net/starlark"
)

// access_check_any answers whether a user holds ANY of the operations. The
// contract that matters is that it is indistinguishable from calling
// access_check once per operation in the same order: apps replace a ladder of
// single calls with one call, and must not thereby change who can see what.
//
// The shared setup — the subject list, which costs a group_memberships walk —
// is what the batch form saves.

// access_test_database builds an app.db with the access table ready.
func access_test_database(t *testing.T) *DB {
	t.Helper()
	test_data_directory(t)
	db := db_open(filepath.Join("users", "u-access", "myapp", "db", "app.db"))
	if db == nil {
		t.Fatal("db_open returned nil")
	}
	t.Cleanup(func() { db.close() })
	db.access_setup()
	return db
}

func access_test_grant(t *testing.T, db *DB, subject string, resource string, operation string, grant int) {
	t.Helper()
	db.access_upsert(subject, resource, operation, grant, "test")
}

// ladder reproduces what an app used to do: one access_check per operation,
// stopping at the first true.
func ladder(db *DB, user string, resource string, operations []string) bool {
	for _, operation := range operations {
		if db.access_check(nil, user, "user", resource, operation) {
			return true
		}
	}
	return false
}

var access_levels = []string{"*", "view", "comment", "write", "design"}

func TestAccessCheckAnyMatchesTheLadder(t *testing.T) {
	// Each case is a set of rows; both forms must agree on every one. These
	// are the shapes the apps actually produce: a wildcard-operation grant, a
	// specific level, a deny, a public grant to subject "*", and a parent
	// resource rule competing with a child one.
	cases := []struct {
		name  string
		rows  [][4]any // subject, resource, operation, grant
		user  string
		allow bool
	}{
		{"no rows at all", nil, "u1", false},
		{"wildcard operation grant", [][4]any{{"u1", "project/p", "*", 1}}, "u1", true},
		{"explicit view grant", [][4]any{{"u1", "project/p", "view", 1}}, "u1", true},
		{"design grant satisfies a view question", [][4]any{{"u1", "project/p", "design", 1}}, "u1", true},
		{"explicit deny on every level", [][4]any{
			{"u1", "project/p", "view", 0}, {"u1", "project/p", "comment", 0},
			{"u1", "project/p", "write", 0}, {"u1", "project/p", "design", 0},
		}, "u1", false},
		{"public grant to subject star", [][4]any{{"*", "project/p", "*", 1}}, "u1", true},
		{"another user's grant does not carry", [][4]any{{"u2", "project/p", "view", 1}}, "u1", false},
		// The case that made dropping the leading "*" probe unsafe: a public
		// grant on the resource against a per-user deny on one level.
		{"public grant with a per-user view deny", [][4]any{
			{"*", "project/p", "*", 1}, {"u1", "project/p", "view", 0},
		}, "u1", true},
		// Parent resource competing with the child. The deny settles the
		// "view" question, but the caller asks about every level and a False
		// only moves on, so "design" is still reached and grants.
		{"parent deny does not stop a child design grant", [][4]any{
			{"u1", "project/p", "design", 1}, {"u1", "project", "view", 0},
		}, "u1", true},
		{"parent grant with no child rule", [][4]any{{"u1", "project", "view", 1}}, "u1", true},
		// The shape that pins the resource walk. Within ONE operation the
		// first row found decides, and resources run most specific first, so
		// the child's deny masks the parent's grant. Deciding each operation
		// against a single resource instead would let the parent grant
		// through - which is why access_decide takes the whole hierarchy.
		{"a child deny masks a parent grant on the same operation", [][4]any{
			{"u1", "project/p", "view", 0}, {"u1", "project", "view", 1},
		}, "u1", false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db := access_test_database(t)
			for _, r := range c.rows {
				access_test_grant(t, db, r[0].(string), r[1].(string), r[2].(string), r[3].(int))
			}

			batch := db.access_check_any(nil, c.user, "user", "project/p", access_levels)
			stepwise := ladder(db, c.user, "project/p", access_levels)

			if batch != stepwise {
				t.Errorf("batch=%v but the per-operation ladder=%v — the two forms must agree", batch, stepwise)
			}
			if batch != c.allow {
				t.Errorf("got %v, want %v", batch, c.allow)
			}
		})
	}
}

func TestAccessCheckAnyStopsAtTheFirstMatch(t *testing.T) {
	db := access_test_database(t)
	// Only "design" is granted, so asking for the levels in ladder order has
	// to reach it rather than giving up at "view".
	access_test_grant(t, db, "u1", "project/p", "design", 1)

	if !db.access_check_any(nil, "u1", "user", "project/p", access_levels) {
		t.Error("a design grant must satisfy a question that lists design")
	}
	// ...and a question that does not list it must not be satisfied.
	if db.access_check_any(nil, "u1", "user", "project/p", []string{"view", "comment"}) {
		t.Error("a design grant must not satisfy a question limited to view and comment")
	}
}

func TestAccessCheckDelegatesToAny(t *testing.T) {
	db := access_test_database(t)
	access_test_grant(t, db, "u1", "project/p", "view", 1)

	// access_check is now a one-element access_check_any; the single-operation
	// behaviour every existing caller relies on must be unchanged.
	if !db.access_check(nil, "u1", "user", "project/p", "view") {
		t.Error("a view grant must satisfy a view check")
	}
	if db.access_check(nil, "u1", "user", "project/p", "design") {
		t.Error("a view grant must not satisfy a design check")
	}
}

// mochi.access.check is no longer a plain builtin - it is a module so that
// .any can hang off it - and CallInternal has to hand the builtin on, or every
// error an app sees from it silently loses its "mochi.access.check()" prefix.
func TestAccessCheckModuleNamesItselfInErrors(t *testing.T) {
	module := &access_check_module{}

	// A bare thread carries no app, so this fails the permission check and
	// returns through sl_error - the same route every other refusal takes.
	_, err := module.CallInternal(&sl.Thread{}, sl.Tuple{sl.None, sl.String("project/p")}, nil)
	if err == nil {
		t.Fatal("a call with no app context must be an error")
	}
	if !strings.Contains(err.Error(), "mochi.access.check()") {
		t.Errorf("error %q does not name the function it came from", err)
	}
}

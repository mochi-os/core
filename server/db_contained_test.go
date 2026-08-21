// Mochi server: a database path cannot leave the data directory.
//
// Lexical containment only: a symlink inside the data directory pointing
// outside it still resolves, which TestContainmentDoesNotClaimSymlinkSafety
// pins.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// contained_root points data_dir at a temporary directory for one test.
func contained_root(t *testing.T) string {
	t.Helper()
	original := data_dir
	data_dir = t.TempDir()
	t.Cleanup(func() { data_dir = original })
	return data_dir
}

// TestContainmentAcceptsOrdinaryPaths. The templates every real call site
// builds must keep working; a guard that rejects them is worse than none.
func TestContainmentAcceptsOrdinaryPaths(t *testing.T) {
	root := contained_root(t)

	for _, file := range []string{
		"db/queue.db",
		"db/users.db",
		"users/0199a1b2c3d4/user.db",
		"users/0199a1b2c3d4/feeds/db/feeds.db",
		"users/0199a1b2c3d4/1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/app.db",
		"users/0199a1b2c3d4/feeds/db/name with spaces (1).db",
		"users/0199a1b2c3d4/feeds/db/a..b.db",
	} {
		if !db_path_contained(filepath.Join(root, file)) {
			t.Errorf("%q was refused; it is an ordinary database path and every app using it would stop opening its database", file)
		}
	}
}

// TestContainmentRefusesAnEscape is the regression, in each shape a traversal
// could take through the interpolated component of a real template.
func TestContainmentRefusesAnEscape(t *testing.T) {
	root := contained_root(t)

	for _, file := range []string{
		"../shadow.db",
		"../../../../etc/shadow",
		"users/../../../../etc/shadow",
		"users/0199a1b2c3d4/../../../outside.db",
		"users/0199a1b2c3d4/feeds/db/../../../../../outside.db",
	} {
		if db_path_contained(filepath.Join(root, file)) {
			t.Errorf("%q was accepted; joined it lands at %q, outside the data directory", file, filepath.Join(root, file))
		}
	}
}

// TestContainmentConfinesAnAbsoluteComponent: filepath.Join folds an
// absolute-looking component under the root rather than honouring it, so the
// result stays contained.
func TestContainmentConfinesAnAbsoluteComponent(t *testing.T) {
	root := contained_root(t)

	joined := filepath.Join(root, "/etc/shadow")
	if !strings.HasPrefix(joined, root+string(os.PathSeparator)) {
		t.Fatalf("filepath.Join honoured an absolute component: %q", joined)
	}
	if !db_path_contained(joined) {
		t.Errorf("%q was refused, but it is inside the data directory", joined)
	}
}

// TestContainmentRefusesTheRootItself. filepath.Join(root, "") is root, and
// "." and ".." both resolve to it or above. The data directory is not a
// database, and treating it as one would have os.Create truncate a directory
// entry's worth of nothing or fail confusingly.
func TestContainmentRefusesTheRootItself(t *testing.T) {
	root := contained_root(t)

	for _, file := range []string{"", ".", "./", "..", "../"} {
		if db_path_contained(filepath.Join(root, file)) {
			t.Errorf("%q resolved to the data directory itself (or above) and was accepted", file)
		}
	}
}

// TestContainmentToleratesAnUntidyDataDirectory. data_dir comes from the
// operator's config file, so it may carry a trailing slash or an interior
// ".."; the check cleans it rather than comparing raw strings. A false
// rejection here would take every database on the host down at once.
func TestContainmentToleratesAnUntidyDataDirectory(t *testing.T) {
	base := t.TempDir()
	original := data_dir
	t.Cleanup(func() { data_dir = original })

	for _, form := range []string{base + "/", base + "/./", base + "/sub/.."} {
		data_dir = form
		if !db_path_contained(filepath.Join(base, "db/queue.db")) {
			t.Errorf("with data_dir %q an ordinary path was refused", form)
		}
		if db_path_contained(filepath.Join(base, "../outside.db")) {
			t.Errorf("with data_dir %q an escape was accepted", form)
		}
	}
}

// TestContainmentRefusesASiblingWithTheSamePrefix. The obvious implementation
// is strings.HasPrefix(path, data_dir), which accepts "/var/lib/mochi-evil"
// for a root of "/var/lib/mochi" - a different directory that merely starts
// with the same characters.
func TestContainmentRefusesASiblingWithTheSamePrefix(t *testing.T) {
	base := t.TempDir()
	original := data_dir
	data_dir = filepath.Join(base, "mochi")
	t.Cleanup(func() { data_dir = original })

	if db_path_contained(filepath.Join(base, "mochi-evil", "queue.db")) {
		t.Error("a sibling directory sharing the root's name prefix was accepted; the check is comparing strings rather than path components")
	}
	if !db_path_contained(filepath.Join(base, "mochi", "queue.db")) {
		t.Error("the real root was refused")
	}
}

// TestOpenRefusesAnEscapingPath is the behavioural half: the guard is wired
// into db_open_work, before the cache lookup and before anything is created on
// disk, and the caller gets nil rather than a handle to a file outside the
// tree.
func TestOpenRefusesAnEscapingPath(t *testing.T) {
	root := contained_root(t)
	log_tables_reset(t)
	defer log_tables_reset(t)
	capture := log_captured(t)

	outside := filepath.Join(filepath.Dir(root), "escaped.db")
	if db := db_open("../escaped.db"); db != nil {
		t.Error("db_open returned a handle for a path outside the data directory")
	}
	if _, err := os.Stat(outside); err == nil {
		t.Errorf("%s was created on disk; the refusal must come before os.Create", outside)
	}

	if !strings.Contains(strings.Join(capture.lines, "\n"), "outside the data directory") {
		t.Errorf("the refusal was silent; an operator has nothing to see: %q", strings.Join(capture.lines, "\n"))
	}
}

// TestOpenRefusalDoesNotPoisonTheCache. db_open_work caches by path, so a
// rejected path must not leave an entry behind that a later legitimate open
// could find.
func TestOpenRefusalDoesNotPoisonTheCache(t *testing.T) {
	contained_root(t)
	log_tables_reset(t)
	defer log_tables_reset(t)
	log_captured(t)

	db_open("../escaped.db")

	databases_lock.Lock()
	defer databases_lock.Unlock()
	for key := range databases {
		if strings.Contains(key, "escaped.db") {
			t.Errorf("a refused path was cached under %q", key)
		}
	}
}

// TestOpenStillWorksForOrdinaryPaths guards the other direction: the check
// sits on the path every database open takes, so a mistake in it is total.
func TestOpenStillWorksForOrdinaryPaths(t *testing.T) {
	contained_root(t)

	db := db_open("db/queue.db")
	if db == nil {
		t.Fatal("an ordinary database path no longer opens")
	}
	db.exec("create table if not exists probe (id integer primary key)")
	if _, err := db.rows("select * from probe"); err != nil {
		t.Errorf("the opened database is not usable: %v", err)
	}
}

// TestContainmentDoesNotClaimSymlinkSafety records the deliberate limit: a
// symlink out of the data directory still resolves. A failure here means the
// scope changed.
func TestContainmentDoesNotClaimSymlinkSafety(t *testing.T) {
	root := contained_root(t)

	outside := t.TempDir()
	link := filepath.Join(root, "escape")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("symlinks unavailable here: %v", err)
	}

	if !db_path_contained(filepath.Join(root, "escape", "elsewhere.db")) {
		t.Error("the check now rejects a path through a symlink; that is stronger than it claims to be, and the scope comments should be updated")
	}
}

// TestOpenGuardIsWiredInBeforeTheCache pins the ordering. Checking after the
// cache lookup would let a poisoned key be returned before the guard ran, and
// checking after os.Create would leave the file behind.
func TestOpenGuardIsWiredInBeforeTheCache(t *testing.T) {
	source, err := os.ReadFile("db.go")
	if err != nil {
		t.Fatalf("reading db.go: %v", err)
	}
	body := string(source)
	at := strings.Index(body, "func db_open_work(")
	if at < 0 {
		t.Fatal("db.go no longer defines db_open_work")
	}
	body = body[at:]
	if end := strings.Index(body, "\n}\n"); end > 0 {
		body = body[:end]
	}

	guard := strings.Index(body, "db_path_contained(path)")
	if guard < 0 {
		t.Fatal("db_open_work does not check containment, so a component carrying \"..\" escapes the data directory")
	}
	for _, later := range []string{"databases_lock.Lock()", "os.MkdirAll(", "os.Create(", "sqlitedrv.Open("} {
		if at := strings.Index(body, later); at >= 0 && at < guard {
			t.Errorf("%s runs before the containment check", later)
		}
	}
}

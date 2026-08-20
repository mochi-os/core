// Mochi server: the comments describe the code that is here.
//
// A family of comments described core's user.db tables as versioned
// LWW-Registers - "each (app, permission, object) carries a per-key Lamport
// `version` and an originating-host `writer`... on a version tie a deny beats a
// grant (fail-closed)". None of it was true. No table in core has a writer
// column, none has a per-key version, and there is no tie-break code. The
// mechanism belonged to multi-host replication, removed July 2026, which also
// took the "converges across the account's hosts" premise underneath it.
//
// Two of these were traps rather than untidiness:
//
//   - permission_revoke's comment credited the revoke's survival to carrying a
//     higher version than the app's default. The revoke does survive, but
//     because permissions_default is insert-or-ignore and the granted=0 row is
//     already there. A reader who believed the version story could "simplify"
//     that insert-or-ignore into a plain upsert and silently resurrect every
//     permission every user ever revoked.
//   - access_list_resource/_subject both claimed "active rules only; tombstones
//     hidden". Their queries have no filter at all, because access_revoke hard
//     deletes. Anyone adding soft-delete later would have trusted a WHERE clause
//     that was never there.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// source_files returns every non-test .go file in the package.
func source_files(t *testing.T) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("reading the package directory: %v", err)
	}
	files := map[string]string{}
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		data, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("reading %s: %v", name, err)
		}
		files[name] = string(data)
	}
	if len(files) == 0 {
		t.Fatal("no source files found")
	}
	return files
}

// comment_lines yields every comment line in a file, with its 1-based number.
// Only comments - a string literal mentioning one of these words is data, not a
// claim about how the code works.
func comment_lines(source string) map[int]string {
	out := map[int]string{}
	for number, line := range strings.Split(source, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "//") {
			out[number+1] = trimmed
		}
	}
	return out
}

// removed_mechanism matches the vocabulary of the register/replication design.
// Anchored on the distinctive terms: "replicate" alone is too broad (app-level
// P2P between users is a live system and legitimately uses the word).
var removed_mechanism = regexp.MustCompile(`(?i)LWW|Lamport|versioned register|versioned-register|versioned LWW|versioned tombstone|convergent set|convergence audit|pair member|paired host|pair-replicat|passive replica|replica-pair|replicated apply|replication apply|exec_app_user`)

// TestNoCommentClaimsTheRemovedRegisterMechanism is the gate. It is written
// against the vocabulary rather than the exact sentences that were fixed,
// because the failure mode is a new comment reasoning from the old design - the
// same way the ones this replaced were written.
func TestNoCommentClaimsTheRemovedRegisterMechanism(t *testing.T) {
	for name, source := range source_files(t) {
		for number, line := range comment_lines(source) {
			if found := removed_mechanism.FindString(line); found != "" {
				t.Errorf("%s:%d describes the removed multi-host replication design (%q):\n  %s",
					name, number, found, line)
			}
		}
	}
}

// TestNoTableCarriesAVersionOrWriterColumn is the fact the gate rests on. If a
// per-key version or an originating-host writer is ever genuinely added, this
// fails and whoever adds it can decide what the comments should say - rather
// than the comments quietly becoming true again by accident.
func TestNoTableCarriesAVersionOrWriterColumn(t *testing.T) {
	// The tables the register comments named.
	for _, table := range []string{"permissions", "access", "groups", "group_members", "interests", "preferences", "classes", "services", "paths"} {
		pattern := regexp.MustCompile(`create table if not exists ` + table + ` ?\(([^)]*)\)`)
		for name, source := range source_files(t) {
			for _, match := range pattern.FindAllStringSubmatch(source, -1) {
				columns := match[1]
				for _, column := range []string{"writer", "version"} {
					if regexp.MustCompile(`\b` + column + `\b`).MatchString(columns) {
						t.Errorf("%s: table %s now has a %s column: %s", name, table, column, strings.TrimSpace(columns))
					}
				}
			}
		}
	}
}

// TestUpsertHelpersAreAPlainUpsertAndAHardDelete pins what the comments now
// describe. row_write is insert-or-update; row_remove is DELETE. Neither
// versions anything, and row_remove leaves nothing behind - which is why
// "tombstone" was the wrong word for every caller that used it.
func TestUpsertHelpersAreAPlainUpsertAndAHardDelete(t *testing.T) {
	source, err := os.ReadFile("upsert.go")
	if err != nil {
		t.Fatalf("reading upsert.go: %v", err)
	}
	text := string(source)

	if !strings.Contains(text, "on conflict") || !strings.Contains(text, "do update set") {
		t.Error("upsert_sql no longer builds an insert-or-update")
	}
	if !strings.Contains(text, `"delete from "+d.table+" where "`) {
		t.Error("row_remove no longer issues a plain DELETE; if it now soft-deletes, every caller's comment needs revisiting and access_list_* needs a filter it has never had")
	}
	for _, absent := range []string{"version", "writer", "tombstone"} {
		if strings.Contains(strings.ToLower(text), absent) {
			t.Errorf("upsert.go mentions %q; the helpers have no such concept", absent)
		}
	}
}

// TestRevokeSurvivesResetup is the behaviour the worst of the stale comments
// misdescribed. It is a real behavioural test, not a text check: the revoke
// must still beat a re-run of app_user_setup, whatever the reason.
func TestRevokeSurvivesResetup(t *testing.T) {
	original := data_dir
	data_dir = t.TempDir()
	defer func() { data_dir = original }()
	if err := os.MkdirAll(filepath.Join(data_dir, "db"), 0o755); err != nil {
		t.Fatalf("creating the db directory: %v", err)
	}
	db := db_open("db/user.db")
	db.permissions_setup()

	// The app's default grant, as app_user_setup seeds it.
	db.permissions_default("testapp", "interests", "read")
	if granted := db.integer("select granted from permissions where app='testapp' and permission='interests' and object='read'"); granted != 1 {
		t.Fatalf("the default seed did not grant: granted=%d", granted)
	}

	// The user revokes it.
	db.permissions_upsert("testapp", "interests", "read", 0)

	// Setup runs again - a server restart, or the app's default set changing.
	db.permissions_default("testapp", "interests", "read")

	if granted := db.integer("select granted from permissions where app='testapp' and permission='interests' and object='read'"); granted != 0 {
		t.Error("re-running the default seed resurrected a permission the user revoked")
	}
}

// TestRevokeLeavesTheRowBehind is the mechanism the corrected comment now
// names, stated separately so it cannot be lost while the test above still
// passes. The granted=0 row IS the protection: delete it instead and the next
// insert-or-ignore grants the permission straight back.
func TestRevokeLeavesTheRowBehind(t *testing.T) {
	original := data_dir
	data_dir = t.TempDir()
	defer func() { data_dir = original }()
	if err := os.MkdirAll(filepath.Join(data_dir, "db"), 0o755); err != nil {
		t.Fatalf("creating the db directory: %v", err)
	}
	db := db_open("db/user.db")
	db.permissions_setup()

	db.permissions_default("testapp", "interests", "read")
	db.permissions_upsert("testapp", "interests", "read", 0)

	rows := db.integer("select count(*) from permissions where app='testapp' and permission='interests' and object='read'")
	if rows != 1 {
		t.Errorf("a revoke left %d rows, want 1; if the revoke deletes the row, permissions_default's insert-or-ignore has nothing to collide with and re-grants on the next setup pass", rows)
	}
}

// TestAccessListReturnsEveryRow states what access_list_* actually do, since
// their comments claimed a filter for so long. If soft-delete is ever added,
// this is where the reader finds out that these two readers do no filtering.
func TestAccessListReturnsEveryRow(t *testing.T) {
	original := data_dir
	data_dir = t.TempDir()
	defer func() { data_dir = original }()
	if err := os.MkdirAll(filepath.Join(data_dir, "db"), 0o755); err != nil {
		t.Fatalf("creating the db directory: %v", err)
	}
	db := db_open("db/app.db")
	db.access_setup()

	db.exec("insert into access (subject, resource, operation, grant, granter, created) values ('alice','doc1','read',1,'owner',0)")
	db.exec("insert into access (subject, resource, operation, grant, granter, created) values ('bob','doc1','read',0,'owner',0)")

	rows, err := db.access_list_resource("doc1")
	if err != nil {
		t.Fatalf("access_list_resource: %v", err)
	}
	if len(rows) != 2 {
		t.Errorf("access_list_resource returned %d rows, want 2; it applies no filter, so a grant=0 row is returned like any other", len(rows))
	}

	// And a revoke removes the row rather than hiding it.
	db.access_revoke("alice", "doc1", "read")
	rows, _ = db.access_list_resource("doc1")
	if len(rows) != 1 {
		t.Errorf("after access_revoke the resource lists %d rows, want 1", len(rows))
	}
}

// TestGroupMembersRemoveIsNamedForWhatItDoes covers the rename. The old name
// said tombstone; the body loops calling row_remove, which is a DELETE.
func TestGroupMembersRemoveIsNamedForWhatItDoes(t *testing.T) {
	source, err := os.ReadFile("groups.go")
	if err != nil {
		t.Fatalf("reading groups.go: %v", err)
	}
	text := string(source)

	if strings.Contains(text, "group_members_tombstone") {
		t.Error("group_members_tombstone is back; it hard-deletes each matching row via row_remove, so the name promises a soft-delete the function has never performed")
	}
	if !strings.Contains(text, "func (db *DB) group_members_remove(") {
		t.Fatal("groups.go no longer defines group_members_remove")
	}
}

// TestGroupMembersRemoveDeletesTheRows is the behaviour behind the name.
func TestGroupMembersRemoveDeletesTheRows(t *testing.T) {
	original := data_dir
	data_dir = t.TempDir()
	defer func() { data_dir = original }()
	if err := os.MkdirAll(filepath.Join(data_dir, "db"), 0o755); err != nil {
		t.Fatalf("creating the db directory: %v", err)
	}
	db := db_open("db/user.db")
	db.groups_setup()

	for _, member := range []string{"alice", "bob", "carol"} {
		db.row_write(reg_members, map[string]any{"parent": "team", "member": member, "type": "user", "created": 0})
	}
	db.row_write(reg_members, map[string]any{"parent": "other", "member": "alice", "type": "user", "created": 0})

	db.group_members_remove("parent=?", "team")

	if left := db.integer("select count(*) from group_members where parent='team'"); left != 0 {
		t.Errorf("%d rows survive for the removed group; the rows are gone, not tombstoned", left)
	}
	// The predicate is a predicate: rows outside it are untouched.
	if other := db.integer("select count(*) from group_members where parent='other'"); other != 1 {
		t.Errorf("group_members_remove deleted %d unrelated rows", 1-other)
	}
}

// TestManifestHasNoReplicateField covers the dead manifest struct removed with
// these comments. Nothing read AppManifest.Replicate; it described which writes
// "fan out to the user's other hosts", and pointed at a plan file
// (claude/plans/replication.md) that no longer exists.
func TestManifestHasNoReplicateField(t *testing.T) {
	source, err := os.ReadFile("apps.go")
	if err != nil {
		t.Fatalf("reading apps.go: %v", err)
	}
	if strings.Contains(string(source), `json:"replicate"`) {
		t.Error("the app manifest declares a replicate field again; nothing reads it, and an app that sets it would be silently ignored")
	}
}

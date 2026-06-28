// Mochi server: mochi.db.merge / mochi.db.tombstone versioned-register semantics.
// A replicated app table written through merge converges under multi-master:
// concurrent writes from different hosts reach the same row on every host
// regardless of arrival order, and a tombstoned row can't be resurrected by a
// stale write.
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import "testing"

const members_schema = "create table members ( chat text not null, member text not null, name text, writer text not null default '', version integer not null, removed integer not null default 0, primary key ( chat, member ) )"

// apply_merge replays one already-originated merge (the journaled upsert) as a
// replica would — explicit version/writer, no local version computation.
func apply_merge(t *testing.T, db *DB, chat, member, name, writer string, version int64, removed int) {
	t.Helper()
	q := db_merge_statement("members", []string{"chat", "member"}, []string{"name"})
	if err := db.exec_e(q, chat, member, name, writer, version, removed); err != nil {
		t.Fatalf("merge (%s/%s name=%q w=%s v=%d removed=%d) failed: %v", chat, member, name, writer, version, removed, err)
	}
}

// member_name returns the active row's name (removed=0), and whether it's present.
func member_name(db *DB, chat, member string) (string, bool) {
	var r struct{ Name string }
	if db.scan(&r, "select name from members where chat=? and member=? and removed=0", chat, member) {
		return r.Name, true
	}
	return "", false
}

// Two hosts apply the SAME concurrent writes in OPPOSITE orders → identical
// final row, settled deterministically by writer on the version tie.
func TestMergeConverges(t *testing.T) {
	a, ca := create_test_db(t)
	defer ca()
	b, cb := create_test_db(t)
	defer cb()
	a.exec(members_schema)
	b.exec(members_schema)

	// host A writes name "Alice-A" (v1, writer A); host B writes "Alice-B" (v1, writer B).
	apply_merge(t, a, "c", "m", "Alice-A", "A", 1, 0)
	apply_merge(t, a, "c", "m", "Alice-B", "B", 1, 0)
	apply_merge(t, b, "c", "m", "Alice-B", "B", 1, 0)
	apply_merge(t, b, "c", "m", "Alice-A", "A", 1, 0)

	na, _ := member_name(a, "c", "m")
	nb, _ := member_name(b, "c", "m")
	if na != nb {
		t.Fatalf("diverged across arrival order: A=%q B=%q", na, nb)
	}
	if na != "Alice-B" {
		t.Errorf("writer tiebreak: got %q, want Alice-B (writer B > A on a v1 tie)", na)
	}
}

// A higher version wins outright, beating the writer tiebreak.
func TestMergeVersionBeatsWriter(t *testing.T) {
	db, cleanup := create_test_db(t)
	defer cleanup()
	db.exec(members_schema)

	apply_merge(t, db, "c", "m", "X", "Z", 1, 0) // v1, writer Z (high)
	apply_merge(t, db, "c", "m", "Y", "A", 2, 0) // v2, writer A (low) — newer
	if n, _ := member_name(db, "c", "m"); n != "Y" {
		t.Errorf("v2 should win over v1 regardless of writer: got %q, want Y", n)
	}
}

// A tombstone removes the row; a stale lower-version write can't resurrect it,
// but a higher-version merge re-adds it.
func TestMergeTombstone(t *testing.T) {
	db, cleanup := create_test_db(t)
	defer cleanup()
	db.exec(members_schema)

	apply_merge(t, db, "c", "m", "X", "A", 1, 0) // add v1
	apply_merge(t, db, "c", "m", "", "A", 2, 1)  // tombstone v2
	if _, ok := member_name(db, "c", "m"); ok {
		t.Fatal("after tombstone the row must be absent")
	}
	apply_merge(t, db, "c", "m", "X", "B", 1, 0) // stale add v1
	if _, ok := member_name(db, "c", "m"); ok {
		t.Error("stale v1 add resurrected a tombstoned row")
	}
	apply_merge(t, db, "c", "m", "Y", "A", 3, 0) // re-add v3
	if n, ok := member_name(db, "c", "m"); !ok || n != "Y" {
		t.Errorf("re-add v3 should win: got %q present=%v, want Y true", n, ok)
	}
}

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

import (
	"sync"
	"testing"
)

// TestMergeAllocateConcurrentSameKeyNoLoss drives the extracted merge allocation
// (db_merge_allocate) from many goroutines writing the SAME key at once. The
// per-key lock (#148) must make every version allocation strictly monotonic, so
// all N writes land (none silently dropped by an equal-version/equal-writer
// conflict) and the surviving row's version equals N. Before the fix, two
// goroutines would read the same max(version), both write version+1, and the
// loser's upsert returned affected=0 — a lost write.
func TestMergeAllocateConcurrentSameKeyNoLoss(t *testing.T) {
	db, cleanup := create_test_db(t)
	defer cleanup()
	db.exec(members_schema)

	const n = 50
	keyCols := []string{"chat", "member"}
	fieldCols := []string{"name"}
	keyVals := []any{"c", "m"}

	var wg sync.WaitGroup
	var landed int64
	var mu sync.Mutex
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			row := map[string]any{"chat": "c", "member": "m", "name": "v"}
			// av=nil, suppressed=true → the non-replicating local write path.
			affected, err := db_merge_allocate(db, "members", keyCols, fieldCols, row, keyVals, 0, nil, true)
			if err != nil {
				t.Errorf("merge %d: %v", i, err)
				return
			}
			if affected >= 1 {
				mu.Lock()
				landed++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	if landed != n {
		t.Errorf("%d of %d concurrent same-key merges landed; the rest were silently dropped (affected=0)", landed, n)
	}
	var got struct{ Version int64 }
	if !db.scan(&got, "select version from members where chat='c' and member='m'") {
		t.Fatal("row missing after concurrent merges")
	}
	if got.Version != n {
		t.Errorf("final version = %d, want %d (each merge must allocate a distinct monotonic version)", got.Version, n)
	}
}

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

// TestMergeReservedWordColumn: a register table whose column is named after a SQL
// reserved word (`order`, as in comptroller's threads/reviews) merges without a
// syntax error — db_merge_statement quotes identifiers.
func TestMergeReservedWordColumn(t *testing.T) {
	db, cleanup := create_test_db(t)
	defer cleanup()
	db.exec(`create table reviews ( id text not null, "order" text not null default '', writer text not null default '', version integer not null, removed integer not null default 0, primary key ( id ) )`)

	q := db_merge_statement("reviews", []string{"id"}, []string{"order"})
	if err := db.exec_e(q, "r1", "o1", "hostA", 1, 0); err != nil {
		t.Fatalf("merge with reserved-word column failed: %v", err)
	}
	// A newer merge updates the reserved-word column in place.
	if err := db.exec_e(q, "r1", "o2", "hostA", 2, 0); err != nil {
		t.Fatalf("second merge failed: %v", err)
	}
	var r struct{ Order string }
	if !db.scan(&r, `select "order" from reviews where id=? and removed=0`, "r1") {
		t.Fatal("row absent after merge")
	}
	if r.Order != "o2" {
		t.Errorf("reserved-word column merge: got %q, want o2", r.Order)
	}
}

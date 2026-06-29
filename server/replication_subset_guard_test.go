// The row-level subset swap-guard (#101): before a reseed swaps the source's scratch
// over the live target, every host-local-excluded row in the target must be present in
// the scratch, or the swap is refused — so a reseed can never wipe data the target
// uniquely holds. This is what makes an automatic reseed of an unfillable gap safe.
// The headline case is same-count-different-rows: the count-based no-shrink guard (#42)
// passes it, the subset guard must refuse it. claude/plans/replication-auto-reseed.md.
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func subsetguard_make_items(t *testing.T, path string, rows [][2]string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	d, err := sql.Open("sqlite3", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if _, err := d.Exec("create table items (id text primary key, body text)"); err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if _, err := d.Exec("insert into items (id, body) values (?, ?)", r[0], r[1]); err != nil {
			t.Fatal(err)
		}
	}
}

func TestBootstrapSwapSubsetGuard(t *testing.T) {
	orig := data_dir
	data_dir = t.TempDir()
	defer func() { data_dir = orig }()

	target := filepath.Join(data_dir, "users", "u1", "12app", "app.db")
	scratch := filepath.Join(data_dir, "scratch.db")
	reset := func() { os.Remove(target); os.Remove(scratch) }

	// 1. Subset: target ⊆ scratch (a normal catch-up reseed — receiver was behind).
	subsetguard_make_items(t, target, [][2]string{{"a", "1"}, {"b", "2"}})
	subsetguard_make_items(t, scratch, [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}})
	if ok, d := bootstrap_swap_subset_ok(target, scratch); !ok {
		t.Fatalf("subset (target ⊆ source) should pass: %s", d)
	}

	// 2. HEADLINE — same count, different rows. The count guard passes this; the
	//    subset guard MUST refuse it (the #42 wipe the count guard can't see).
	reset()
	subsetguard_make_items(t, target, [][2]string{{"a", "1"}, {"X", "9"}})  // target uniquely holds X
	subsetguard_make_items(t, scratch, [][2]string{{"a", "1"}, {"Y", "9"}}) // source has Y instead; same count
	if ok, _ := bootstrap_swap_subset_ok(target, scratch); ok {
		t.Fatal("same-count-different-rows MUST refuse — this is the #42 case the count guard misses")
	}

	// 3. Target superset (target has rows the source lacks) → refuse.
	reset()
	subsetguard_make_items(t, target, [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}})
	subsetguard_make_items(t, scratch, [][2]string{{"a", "1"}, {"b", "2"}})
	if ok, _ := bootstrap_swap_subset_ok(target, scratch); ok {
		t.Fatal("target-superset must refuse")
	}

	// 4. A shared key whose value changed is a catch-up UPDATE — the key still exists
	//    in the source, so the subset guard PASSES it. (Value freshness is the
	//    anti-revert gate's domain #62; a lost local edit is gated by
	//    local_writes_present at the reseed trigger. A content match here would refuse
	//    every legitimate catch-up reseed.)
	reset()
	subsetguard_make_items(t, target, [][2]string{{"a", "OLD"}})
	subsetguard_make_items(t, scratch, [][2]string{{"a", "NEW"}})
	if ok, d := bootstrap_swap_subset_ok(target, scratch); !ok {
		t.Fatalf("a catch-up update (same key, changed value) must pass: %s", d)
	}

	// 5. Fail-closed: a table the scratch lacks → refuse.
	reset()
	subsetguard_make_items(t, target, [][2]string{{"a", "1"}})
	os.MkdirAll(filepath.Dir(scratch), 0o755)
	d, _ := sql.Open("sqlite3", "file:"+scratch)
	d.Exec("create table other (x text)")
	d.Close()
	if ok, _ := bootstrap_swap_subset_ok(target, scratch); ok {
		t.Fatal("a table missing from the scratch must refuse (fail-closed)")
	}

	// 6. End-to-end through bootstrap_db_swap: a same-count-different-rows swap is
	//    refused with the #101 error, and the target is left untouched.
	reset()
	subsetguard_make_items(t, target, [][2]string{{"a", "1"}, {"X", "9"}})
	subsetguard_make_items(t, scratch, [][2]string{{"a", "1"}, {"Y", "9"}})
	err := bootstrap_db_swap(target, scratch)
	if err == nil || !strings.Contains(err.Error(), "subset guard #101") {
		t.Fatalf("bootstrap_db_swap must refuse same-count-different-rows via the subset guard, got: %v", err)
	}
	if rows, e := db_file_rows(target, "select id from items"); e != nil || len(rows) != 2 {
		t.Fatalf("target must be untouched after a refused swap (rows=%d err=%v)", len(rows), e)
	}
}

// A row differing ONLY in a host-local column is the SAME logical row, not a unique
// one — so the subset guard must NOT refuse over it (else legit reseeds break). Uses
// user.db's accounts.last_delivered, a host-local column the audit excludes.
func TestBootstrapSwapSubsetExcludesHostLocalColumns(t *testing.T) {
	orig := data_dir
	data_dir = t.TempDir()
	defer func() { data_dir = orig }()

	mkAccounts := func(path, lastDelivered string) {
		os.MkdirAll(filepath.Dir(path), 0o755)
		d, err := sql.Open("sqlite3", "file:"+path)
		if err != nil {
			t.Fatal(err)
		}
		defer d.Close()
		d.Exec("create table accounts (uid text primary key, name text, last_delivered text)")
		d.Exec("insert into accounts (uid, name, last_delivered) values ('u1','Alice', ?)", lastDelivered)
	}
	userdb := filepath.Join(data_dir, "users", "u1", "user.db")
	scratch := filepath.Join(data_dir, "scratch.db")
	mkAccounts(userdb, "111") // target: host-local column = 111
	mkAccounts(scratch, "999") // source: host-local column = 999 (legitimately differs per host)
	if ok, det := bootstrap_swap_subset_ok(userdb, scratch); !ok {
		t.Fatalf("a row differing only in the host-local accounts.last_delivered must be treated as a subset match: %s", det)
	}
}

// Multiset: the target holding more copies of an identical row than the source must
// refuse — a swap would lose a copy. (Replicated tables usually have a PK, but the
// guard is conservative.)
func TestBootstrapSwapSubsetMultiset(t *testing.T) {
	orig := data_dir
	data_dir = t.TempDir()
	defer func() { data_dir = orig }()

	mk := func(path string, n int) {
		os.MkdirAll(filepath.Dir(path), 0o755)
		d, err := sql.Open("sqlite3", "file:"+path)
		if err != nil {
			t.Fatal(err)
		}
		defer d.Close()
		d.Exec("create table notes (body text)")
		for i := 0; i < n; i++ {
			d.Exec("insert into notes (body) values ('dup')")
		}
	}
	target := filepath.Join(data_dir, "users", "u1", "12app", "app.db")
	scratch := filepath.Join(data_dir, "scratch.db")
	mk(target, 2)  // two identical copies
	mk(scratch, 1) // only one in the source
	if ok, _ := bootstrap_swap_subset_ok(target, scratch); ok {
		t.Fatal("multiset: two target copies vs one source copy must refuse (a copy would be lost)")
	}
}

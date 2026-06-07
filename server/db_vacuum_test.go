// Mochi server: database vacuum tests
// Copyright Alistair Cunningham 2026

package main

import "testing"

// TestDBAutoVacuumDefault verifies every new database is created with
// auto_vacuum=INCREMENTAL. The pragma must be set before journal_mode=WAL
// in db_setup_conn; if that ordering regresses, the mode silently falls
// back to NONE and incremental_vacuum becomes a no-op.
func TestDBAutoVacuumDefault(t *testing.T) {
	db, cleanup := create_test_db(t)
	defer cleanup()

	db.exec("create table t (x)")
	if av := db.integer("pragma auto_vacuum"); av != 2 {
		t.Fatalf("auto_vacuum = %d, want 2 (INCREMENTAL) - check pragma order in db_setup_conn", av)
	}
}

// TestDBVacuumReclaims verifies DB.vacuum returns freed pages to the OS
// once a database has churned past the gate, and reports the bytes freed.
func TestDBVacuumReclaims(t *testing.T) {
	db, cleanup := create_test_db(t)
	defer cleanup()

	db.exec("create table t (x)")
	db.exec("with recursive c(i) as (select 1 union all select i+1 from c where i<60000) insert into t select randomblob(200) from c")
	db.exec("delete from t")

	before := db.integer("pragma page_count")
	if free := db.integer("pragma freelist_count"); free == 0 {
		t.Fatalf("expected free pages after delete, got 0")
	}

	reclaimed := db.vacuum()
	if reclaimed <= 0 {
		t.Fatalf("vacuum reclaimed %d bytes, want > 0", reclaimed)
	}
	if after := db.integer("pragma page_count"); after >= before {
		t.Fatalf("page_count did not drop: before=%d after=%d", before, after)
	}
	if free := db.integer("pragma freelist_count"); free != 0 {
		t.Errorf("freelist_count = %d after vacuum, want 0", free)
	}
}

// TestDBVacuumConvertsLegacy verifies DB.vacuum converts a pre-change
// auto_vacuum=NONE database to INCREMENTAL while reclaiming, so existing
// production databases self-convert on their first bloated pass.
func TestDBVacuumConvertsLegacy(t *testing.T) {
	db, cleanup := create_test_db(t)
	defer cleanup()

	// Mimic a database created before this change: force it back to NONE.
	db.exec("pragma auto_vacuum=NONE")
	db.exec("vacuum")
	if av := db.integer("pragma auto_vacuum"); av != 0 {
		t.Fatalf("setup: auto_vacuum = %d, want 0 (NONE)", av)
	}

	db.exec("create table t (x)")
	db.exec("with recursive c(i) as (select 1 union all select i+1 from c where i<60000) insert into t select randomblob(200) from c")
	db.exec("delete from t")

	before := db.integer("pragma page_count")
	if r := db.vacuum(); r <= 0 {
		t.Fatalf("vacuum reclaimed %d bytes, want > 0", r)
	}
	if av := db.integer("pragma auto_vacuum"); av != 2 {
		t.Errorf("auto_vacuum = %d after convert, want 2 (INCREMENTAL)", av)
	}
	if after := db.integer("pragma page_count"); after >= before {
		t.Errorf("page_count did not drop: before=%d after=%d", before, after)
	}
}

// TestDBVacuumSkipsUnchurned verifies a database below the gate is left
// untouched (no needless full-file rewrite of stable databases).
func TestDBVacuumSkipsUnchurned(t *testing.T) {
	db, cleanup := create_test_db(t)
	defer cleanup()

	db.exec("create table t (x)")
	db.exec("insert into t values ('a'), ('b'), ('c')")
	if reclaimed := db.vacuum(); reclaimed != 0 {
		t.Fatalf("vacuum reclaimed %d bytes on an unchurned database, want 0", reclaimed)
	}
}

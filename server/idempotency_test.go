// Mochi server: idempotency cache migration regression test
// Copyright Alistair Cunningham 2026

package main

import (
	"os"
	"path/filepath"
	"testing"
)

// TestIdempotencySetup guards the per-app idempotency cache migration, which
// once regressed badly: the _idempotent_calls -> idempotency rename refactor
// left idempotency_setup calling itself (unbounded recursion -> stack overflow
// on the first idempotent mochi.url.post) and never creating the `idempotency`
// table the lookup/store queries depend on. This verifies the table IS created,
// the pre-rename orphan IS dropped, the call returns (no recursion), and a
// second call is a harmless no-op.
func TestIdempotencySetup(t *testing.T) {
	orig := data_dir
	data_dir = t.TempDir()
	defer func() { data_dir = orig }()
	if err := os.MkdirAll(filepath.Join(data_dir, "db"), 0o755); err != nil {
		t.Fatal(err)
	}
	db := db_open("db/test.db")

	// Seed the pre-rename orphan so we can prove the migration drops it.
	db.exec("create table if not exists _idempotent_calls (key text primary key, status integer not null, headers blob, body blob, ts integer not null)")

	idempotency_setup(db) // must create `idempotency`, drop the orphan, and RETURN

	if has, _ := db.exists("select name from sqlite_master where type='table' and name='idempotency'"); !has {
		t.Fatal("idempotency_setup did not create the idempotency table")
	}
	if has, _ := db.exists("select name from sqlite_master where type='table' and name='_idempotent_calls'"); has {
		t.Error("idempotency_setup left the pre-rename _idempotent_calls orphan in place")
	}

	// Idempotent second call + a usable round-trip through the cache columns.
	idempotency_setup(db)
	db.exec("insert or replace into idempotency (key, status, headers, body, ts) values ('k', 200, x'', x'', ?)", now())
	if n := db.integer("select count(*) from idempotency"); n != 1 {
		t.Fatalf("idempotency round-trip failed: rows=%d", n)
	}
}

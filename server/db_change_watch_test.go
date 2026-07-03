package main

import (
	"database/sql"
	"path/filepath"
	"testing"
)

// #172: db_change_watch is the subset-guard TOCTOU detector — a held read-only
// connection whose changed() reports whether another connection committed to the
// file since the watch opened, and fails CLOSED when it can't verify.
func TestDbChangeWatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "t.db")
	rw, err := sql.Open("sqlite3", "file:"+path+"?_pragma=journal_mode(WAL)")
	if err != nil {
		t.Fatal(err)
	}
	defer rw.Close()
	if _, err := rw.Exec("create table t (id integer primary key)"); err != nil {
		t.Fatal(err)
	}
	rw.Exec("insert into t values (1)")

	w := db_watch_target(path)
	defer w.close()
	if !w.ok {
		t.Fatal("watch should open on a healthy DB")
	}
	if w.changed() {
		t.Fatal("no commit since the watch opened — changed() must be false")
	}

	// A commit from ANOTHER connection is detected (the window write the swap must
	// not wipe).
	if _, err := rw.Exec("insert into t values (2)"); err != nil {
		t.Fatal(err)
	}
	if !w.changed() {
		t.Fatal("a concurrent commit must be detected as changed()")
	}

	// A watch that couldn't open (missing file) fails CLOSED so the swap aborts
	// rather than proceeding unverified.
	bad := db_watch_target(filepath.Join(dir, "nope.db"))
	defer bad.close()
	if bad.ok {
		t.Fatal("watch on a missing file must not be ok")
	}
	if !bad.changed() {
		t.Fatal("an unopened watch must fail closed (changed()=true)")
	}
}

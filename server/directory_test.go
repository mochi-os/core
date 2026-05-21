// Mochi server: directory routing-table unit tests
// Copyright Alistair Cunningham 2026

package main

import (
	"testing"
)

// directory_record_location is the shared helper that both the live
// directory-publish handler and the bulk directory-download loop use to
// populate the `locations` routing table. The bulk download previously
// skipped `locations` entirely, so a freshly-wiped server knew every
// entity (in `entities`) but could route to almost none — caught
// 2026-05-21 as a market app reporting "Comptroller is not available".

func setup_directory_test(t *testing.T) func() {
	cleanup := setup_replication_test(t) // sets data_dir + p2p_id="self"
	db := db_open("db/directory.db")
	db.exec("create table locations (entity text not null, peer text not null, seen integer not null, primary key (entity, peer))")
	return cleanup
}

// TestDirectoryRecordLocationStoresClaim: a well-formed "p2p/<peer>"
// location is recorded into `locations` with the supplied `seen`
// timestamp verbatim (not now()), so the failover tiering can judge
// freshness honestly.
func TestDirectoryRecordLocationStoresClaim(t *testing.T) {
	cleanup := setup_directory_test(t)
	defer cleanup()

	db := db_open("db/directory.db")
	directory_record_location(db, "ent-1", "p2p/peer-X", 1779000000)

	row, _ := db.row("select peer, seen from locations where entity='ent-1'")
	if row == nil {
		t.Fatal("expected a locations row for ent-1")
	}
	if p, _ := row["peer"].(string); p != "peer-X" {
		t.Errorf("peer = %q, want peer-X", p)
	}
	if s := row_int(row, "seen"); s != 1779000000 {
		t.Errorf("seen = %d, want 1779000000 (the passed value, not now())", s)
	}
}

// TestDirectoryRecordLocationSkipsSelf: a location claim pointing at
// this server is not recorded — a server isn't a routable remote peer
// for its own entities.
func TestDirectoryRecordLocationSkipsSelf(t *testing.T) {
	cleanup := setup_directory_test(t)
	defer cleanup()

	db := db_open("db/directory.db")
	directory_record_location(db, "ent-self", "p2p/self", 1779000000)

	if n := db.integer("select count(*) from locations where entity='ent-self'"); n != 0 {
		t.Errorf("self-claim recorded %d rows, want 0", n)
	}
}

// TestDirectoryRecordLocationSkipsEmpty: an empty / prefix-only
// location yields no peer and is skipped rather than inserting a
// blank-peer row.
func TestDirectoryRecordLocationSkipsEmpty(t *testing.T) {
	cleanup := setup_directory_test(t)
	defer cleanup()

	db := db_open("db/directory.db")
	directory_record_location(db, "ent-empty", "", 1779000000)
	directory_record_location(db, "ent-prefix-only", "p2p/", 1779000000)

	if n := db.integer("select count(*) from locations"); n != 0 {
		t.Errorf("empty/prefix-only locations recorded %d rows, want 0", n)
	}
}

// TestDirectoryRecordLocationReplaces: a second claim for the same
// (entity, peer) replaces the first — INSERT OR REPLACE on the
// (entity, peer) primary key, so a re-download refreshes `seen` in
// place rather than duplicating.
func TestDirectoryRecordLocationReplaces(t *testing.T) {
	cleanup := setup_directory_test(t)
	defer cleanup()

	db := db_open("db/directory.db")
	directory_record_location(db, "ent-1", "p2p/peer-X", 1000)
	directory_record_location(db, "ent-1", "p2p/peer-X", 2000)

	if n := db.integer("select count(*) from locations where entity='ent-1'"); n != 1 {
		t.Errorf("rows for (ent-1, peer-X) = %d, want 1", n)
	}
	row, _ := db.row("select seen from locations where entity='ent-1' and peer='peer-X'")
	if row == nil || row_int(row, "seen") != 2000 {
		t.Errorf("seen after re-record = %v, want 2000", row)
	}
}

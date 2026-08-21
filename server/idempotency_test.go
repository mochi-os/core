// Mochi server: the per-app idempotency cache table.
//
// No migration drops the pre-rename `_idempotent_calls` table; a restored old
// app.db keeps it as an inert orphan, deliberately.
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

	cbor "github.com/fxamacker/cbor/v2"
)

// idempotency_db builds an empty database in a temporary data directory.
func idempotency_db(t *testing.T) *DB {
	t.Helper()
	original := data_dir
	data_dir = t.TempDir()
	t.Cleanup(func() { data_dir = original })
	if err := os.MkdirAll(filepath.Join(data_dir, "db"), 0o755); err != nil {
		t.Fatalf("creating the db directory: %v", err)
	}
	return db_open("db/test.db")
}

// TestIdempotencySetupCreatesTheTable is the regression the recursion bug
// produced: no table, so every lookup and store failed.
func TestIdempotencySetupCreatesTheTable(t *testing.T) {
	db := idempotency_db(t)

	idempotency_setup(db) // must create the table and RETURN

	if has, _ := db.exists("select name from sqlite_master where type='table' and name='idempotency'"); !has {
		t.Fatal("idempotency_setup did not create the idempotency table")
	}
}

// TestIdempotencySetupIsRepeatable: it runs on every lookup and every store, so
// the second and hundredth call must be harmless.
func TestIdempotencySetupIsRepeatable(t *testing.T) {
	db := idempotency_db(t)

	idempotency_setup(db)
	db.exec("insert or replace into idempotency (key, status, headers, body, ts) values ('k', 200, x'', x'', ?)", now())

	for i := 0; i < 5; i++ {
		idempotency_setup(db)
	}

	if rows := db.integer("select count(*) from idempotency"); rows != 1 {
		t.Errorf("the cached row did not survive repeated setup calls: rows=%d", rows)
	}
	if status := db.integer("select status from idempotency where key='k'"); status != 200 {
		t.Errorf("the cached row reads status=%d after repeated setup calls, want 200", status)
	}
}

// TestIdempotencySetupDoesNotMigrate is the gate on this removal. The drop was
// taken out with no replacement, so re-adding one - or the sqlite_master lookup
// that gated it - is a decision to be made again, not a detail to slip back in
// while editing nearby.
func TestIdempotencySetupDoesNotMigrate(t *testing.T) {
	source, err := os.ReadFile("api.go")
	if err != nil {
		t.Fatalf("reading api.go: %v", err)
	}
	text := string(source)
	at := strings.Index(text, "func idempotency_setup(")
	if at < 0 {
		t.Fatal("api.go no longer defines idempotency_setup")
	}
	body := text[at:]
	if end := strings.Index(body, "\n}\n"); end > 0 {
		body = body[:end]
	}

	if strings.Contains(body, "_idempotent_calls") {
		t.Error("idempotency_setup drops _idempotent_calls again; the migration was removed on purpose, and an app.db old enough to carry that table has not been reachable since the rename")
	}
	if strings.Contains(body, "sqlite_master") {
		t.Error("idempotency_setup queries sqlite_master again; it runs twice per idempotent mochi.url.post, so a catalogue lookup here buys nothing now that there is no migration to gate")
	}
	if strings.Contains(body, "drop table") {
		t.Error("idempotency_setup issues a DROP; it is called on every lookup and store, which is not a safe place for one")
	}
}

func TestIdempotencySetupLeavesAnOrphanAlone(t *testing.T) {
	db := idempotency_db(t)

	// An app.db as it was before the rename.
	db.exec("create table if not exists _idempotent_calls (key text primary key, status integer not null, headers blob, body blob, ts integer not null)")

	idempotency_setup(db)

	if has, _ := db.exists("select name from sqlite_master where type='table' and name='idempotency'"); !has {
		t.Fatal("idempotency_setup did not create the current table alongside the orphan")
	}
	if has, _ := db.exists("select name from sqlite_master where type='table' and name='_idempotent_calls'"); !has {
		t.Error("the orphan was dropped; no migration is meant to run here any more")
	}

	// The point of the above: the orphan is inert, so the cache still works.
	db.exec("insert or replace into idempotency (key, status, headers, body, ts) values ('k', 204, x'', x'', ?)", now())
	if status := db.integer("select status from idempotency where key='k'"); status != 204 {
		t.Errorf("the cache misbehaves alongside the orphan table: status=%d", status)
	}
}

// TestIdempotencyRoundTrip covers the columns the lookup and store actually
// use, so a schema edit that breaks them fails here rather than at runtime.
func TestIdempotencyRoundTrip(t *testing.T) {
	db := idempotency_db(t)
	idempotency_setup(db)

	headers := map[string]string{"Content-Type": "application/json"}
	db.exec("insert or replace into idempotency (key, status, headers, body, ts) values (?, ?, ?, ?, ?)",
		"order-1", 201, cbor_encode(headers), []byte(`{"ok":true}`), now())

	row, err := db.row("select status, headers, body from idempotency where key=?", "order-1")
	if err != nil || row == nil {
		t.Fatalf("stored row not readable: %v", err)
	}
	if status, _ := row["status"].(int64); status != 201 {
		t.Errorf("status round-tripped as %d, want 201", status)
	}
	var back map[string]string
	switch v := row["headers"].(type) {
	case []byte:
		_ = cbor.Unmarshal(v, &back)
	case string:
		_ = cbor.Unmarshal([]byte(v), &back)
	}
	if back["Content-Type"] != "application/json" {
		t.Errorf("headers round-tripped as %v", back)
	}
}

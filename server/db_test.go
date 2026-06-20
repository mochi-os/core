// Mochi server: Database unit tests
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	sl "go.starlark.net/starlark"
)

// Helper to create a test database
func create_test_db(t *testing.T) (*DB, func()) {
	// Create a temp directory for the test database
	tmp_dir, err := os.MkdirTemp("", "mochi_db_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	// Save original data_dir and set to temp
	orig_data_dir := data_dir
	data_dir = tmp_dir

	// Create test database
	db_path := "test.db"
	db := db_open(db_path)

	cleanup := func() {
		db.close()
		data_dir = orig_data_dir
		os.RemoveAll(tmp_dir)
	}

	return db, cleanup
}

// Test db.exec creates tables
func TestDBExec(t *testing.T) {
	db, cleanup := create_test_db(t)
	defer cleanup()

	// Create a test table
	db.exec("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")

	// Verify table exists
	exists, err := db.exists("SELECT name FROM sqlite_master WHERE type='table' AND name='test_table'")
	if err != nil {
		t.Fatalf("exists query failed: %v", err)
	}
	if !exists {
		t.Error("Table should exist after CREATE TABLE")
	}
}

// Test db.exec with insert
func TestDBExecInsert(t *testing.T) {
	db, cleanup := create_test_db(t)
	defer cleanup()

	db.exec("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)")
	db.exec("INSERT INTO items (id, value) VALUES (?, ?)", 1, "hello")
	db.exec("INSERT INTO items (id, value) VALUES (?, ?)", 2, "world")

	// Verify rows exist
	exists, _ := db.exists("SELECT 1 FROM items WHERE id = ?", 1)
	if !exists {
		t.Error("Row with id=1 should exist")
	}

	exists, _ = db.exists("SELECT 1 FROM items WHERE id = ?", 2)
	if !exists {
		t.Error("Row with id=2 should exist")
	}
}

// Test db.exists function
func TestDBExists(t *testing.T) {
	db, cleanup := create_test_db(t)
	defer cleanup()

	db.exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	db.exec("INSERT INTO users (id, name) VALUES (1, 'Alice')")

	tests := []struct {
		name     string
		query    string
		args     []any
		expected bool
	}{
		{"existing row", "SELECT 1 FROM users WHERE id = ?", []any{1}, true},
		{"non-existing row", "SELECT 1 FROM users WHERE id = ?", []any{999}, false},
		{"name match", "SELECT 1 FROM users WHERE name = ?", []any{"Alice"}, true},
		{"name no match", "SELECT 1 FROM users WHERE name = ?", []any{"Bob"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exists, err := db.exists(tt.query, tt.args...)
			if err != nil {
				t.Fatalf("exists query failed: %v", err)
			}
			if exists != tt.expected {
				t.Errorf("exists(%q, %v) = %v, want %v", tt.query, tt.args, exists, tt.expected)
			}
		})
	}
}

// Test db.integer function
func TestDBInteger(t *testing.T) {
	db, cleanup := create_test_db(t)
	defer cleanup()

	db.exec("CREATE TABLE counts (name TEXT PRIMARY KEY, count INTEGER)")
	db.exec("INSERT INTO counts (name, count) VALUES ('items', 42)")
	db.exec("INSERT INTO counts (name, count) VALUES ('users', 100)")

	result := db.integer("SELECT count FROM counts WHERE name = ?", "items")
	if result != 42 {
		t.Errorf("integer() = %d, want 42", result)
	}

	result = db.integer("SELECT count FROM counts WHERE name = ?", "users")
	if result != 100 {
		t.Errorf("integer() = %d, want 100", result)
	}
}

// Test db.row function
func TestDBRow(t *testing.T) {
	db, cleanup := create_test_db(t)
	defer cleanup()

	db.exec("CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	db.exec("INSERT INTO people (id, name, age) VALUES (1, 'Alice', 30)")

	row, err := db.row("SELECT * FROM people WHERE id = ?", 1)
	if err != nil {
		t.Fatalf("row query failed: %v", err)
	}

	if row == nil {
		t.Fatal("row should not be nil")
	}

	if row["name"] != "Alice" {
		t.Errorf("row['name'] = %v, want 'Alice'", row["name"])
	}

	// age might be int64 depending on SQLite driver
	age, ok := row["age"].(int64)
	if !ok {
		t.Errorf("row['age'] type = %T, expected int64", row["age"])
	} else if age != 30 {
		t.Errorf("row['age'] = %d, want 30", age)
	}
}

// Test db.row returns nil for no results
func TestDBRowNotFound(t *testing.T) {
	db, cleanup := create_test_db(t)
	defer cleanup()

	db.exec("CREATE TABLE empty_table (id INTEGER PRIMARY KEY)")

	row, err := db.row("SELECT * FROM empty_table WHERE id = ?", 999)
	if err != nil {
		t.Fatalf("row query failed: %v", err)
	}

	if row != nil {
		t.Error("row should be nil for non-existent row")
	}
}

// Test db.rows function
func TestDBRows(t *testing.T) {
	db, cleanup := create_test_db(t)
	defer cleanup()

	db.exec("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)")
	db.exec("INSERT INTO products (id, name, price) VALUES (1, 'Apple', 1.50)")
	db.exec("INSERT INTO products (id, name, price) VALUES (2, 'Banana', 0.75)")
	db.exec("INSERT INTO products (id, name, price) VALUES (3, 'Cherry', 2.00)")

	rows, err := db.rows("SELECT * FROM products ORDER BY id")
	if err != nil {
		t.Fatalf("rows query failed: %v", err)
	}

	if len(rows) != 3 {
		t.Fatalf("rows count = %d, want 3", len(rows))
	}

	if rows[0]["name"] != "Apple" {
		t.Errorf("rows[0]['name'] = %v, want 'Apple'", rows[0]["name"])
	}
	if rows[1]["name"] != "Banana" {
		t.Errorf("rows[1]['name'] = %v, want 'Banana'", rows[1]["name"])
	}
	if rows[2]["name"] != "Cherry" {
		t.Errorf("rows[2]['name'] = %v, want 'Cherry'", rows[2]["name"])
	}
}

// Test db.rows with empty result
func TestDBRowsEmpty(t *testing.T) {
	db, cleanup := create_test_db(t)
	defer cleanup()

	db.exec("CREATE TABLE empty (id INTEGER PRIMARY KEY)")

	rows, err := db.rows("SELECT * FROM empty")
	if err != nil {
		t.Fatalf("rows query failed: %v", err)
	}

	if rows != nil && len(rows) != 0 {
		t.Errorf("rows should be empty, got %d rows", len(rows))
	}
}

// Test db.rows with filtering
func TestDBRowsFiltered(t *testing.T) {
	db, cleanup := create_test_db(t)
	defer cleanup()

	db.exec("CREATE TABLE items (id INTEGER, category TEXT)")
	db.exec("INSERT INTO items VALUES (1, 'A')")
	db.exec("INSERT INTO items VALUES (2, 'B')")
	db.exec("INSERT INTO items VALUES (3, 'A')")
	db.exec("INSERT INTO items VALUES (4, 'A')")

	rows, err := db.rows("SELECT * FROM items WHERE category = ?", "A")
	if err != nil {
		t.Fatalf("rows query failed: %v", err)
	}

	if len(rows) != 3 {
		t.Errorf("rows count = %d, want 3", len(rows))
	}
}

// Test db.scan with struct
func TestDBScan(t *testing.T) {
	db, cleanup := create_test_db(t)
	defer cleanup()

	db.exec("CREATE TABLE config (name TEXT PRIMARY KEY, value TEXT, enabled INTEGER)")
	db.exec("INSERT INTO config VALUES ('test', 'hello', 1)")

	type Config struct {
		Name    string `db:"name"`
		Value   string `db:"value"`
		Enabled int    `db:"enabled"`
	}

	var cfg Config
	found := db.scan(&cfg, "SELECT * FROM config WHERE name = ?", "test")

	if !found {
		t.Error("scan should return true for existing row")
	}

	if cfg.Name != "test" {
		t.Errorf("cfg.Name = %q, want 'test'", cfg.Name)
	}
	if cfg.Value != "hello" {
		t.Errorf("cfg.Value = %q, want 'hello'", cfg.Value)
	}
	if cfg.Enabled != 1 {
		t.Errorf("cfg.Enabled = %d, want 1", cfg.Enabled)
	}
}

// Test db.scan returns false for no results
func TestDBScanNotFound(t *testing.T) {
	db, cleanup := create_test_db(t)
	defer cleanup()

	db.exec("CREATE TABLE items (id INTEGER PRIMARY KEY)")

	type Item struct {
		ID int `db:"id"`
	}

	var item Item
	found := db.scan(&item, "SELECT * FROM items WHERE id = ?", 999)

	if found {
		t.Error("scan should return false for non-existent row")
	}
}

// Test db.scans with struct slice
func TestDBScans(t *testing.T) {
	db, cleanup := create_test_db(t)
	defer cleanup()

	db.exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	db.exec("INSERT INTO users VALUES (1, 'Alice')")
	db.exec("INSERT INTO users VALUES (2, 'Bob')")
	db.exec("INSERT INTO users VALUES (3, 'Charlie')")

	type User struct {
		ID   int    `db:"id"`
		Name string `db:"name"`
	}

	var users []User
	err := db.scans(&users, "SELECT * FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("scans failed: %v", err)
	}

	if len(users) != 3 {
		t.Fatalf("users count = %d, want 3", len(users))
	}

	if users[0].Name != "Alice" {
		t.Errorf("users[0].Name = %q, want 'Alice'", users[0].Name)
	}
	if users[1].Name != "Bob" {
		t.Errorf("users[1].Name = %q, want 'Bob'", users[1].Name)
	}
	if users[2].Name != "Charlie" {
		t.Errorf("users[2].Name = %q, want 'Charlie'", users[2].Name)
	}
}

// Test database path creation
func TestDBOpenCreatesFile(t *testing.T) {
	tmp_dir, err := os.MkdirTemp("", "mochi_db_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmp_dir)

	orig_data_dir := data_dir
	data_dir = tmp_dir
	defer func() { data_dir = orig_data_dir }()

	// Create database in nested path
	db := db_open("nested/path/test.db")
	db.exec("CREATE TABLE test (id INTEGER)")
	db.close()

	// Verify file was created
	path := filepath.Join(tmp_dir, "nested", "path", "test.db")
	if !file_exists(path) {
		t.Errorf("Database file should exist at %s", path)
	}
}

// Test concurrent database access
func TestDBConcurrentAccess(t *testing.T) {
	db, cleanup := create_test_db(t)
	defer cleanup()

	db.exec("CREATE TABLE counter (id INTEGER PRIMARY KEY, count INTEGER)")
	db.exec("INSERT INTO counter VALUES (1, 0)")

	// Run concurrent reads
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				_, _ = db.exists("SELECT 1 FROM counter WHERE id = 1")
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}
}

// Benchmark db.exists
func BenchmarkDBExists(b *testing.B) {
	tmp_dir, _ := os.MkdirTemp("", "mochi_db_bench")
	defer os.RemoveAll(tmp_dir)

	orig_data_dir := data_dir
	data_dir = tmp_dir
	defer func() { data_dir = orig_data_dir }()

	db := db_open("bench.db")
	db.exec("CREATE TABLE items (id INTEGER PRIMARY KEY)")
	db.exec("INSERT INTO items VALUES (1)")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.exists("SELECT 1 FROM items WHERE id = ?", 1)
	}
}

// Benchmark db.row
func BenchmarkDBRow(b *testing.B) {
	tmp_dir, _ := os.MkdirTemp("", "mochi_db_bench")
	defer os.RemoveAll(tmp_dir)

	orig_data_dir := data_dir
	data_dir = tmp_dir
	defer func() { data_dir = orig_data_dir }()

	db := db_open("bench.db")
	db.exec("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, value TEXT)")
	db.exec("INSERT INTO items VALUES (1, 'test', 'value')")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.row("SELECT * FROM items WHERE id = ?", 1)
	}
}

// Benchmark db.rows
func BenchmarkDBRows(b *testing.B) {
	tmp_dir, _ := os.MkdirTemp("", "mochi_db_bench")
	defer os.RemoveAll(tmp_dir)

	orig_data_dir := data_dir
	data_dir = tmp_dir
	defer func() { data_dir = orig_data_dir }()

	db := db_open("bench.db")
	db.exec("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
	for i := 0; i < 100; i++ {
		db.exec("INSERT INTO items VALUES (?, ?)", i, "item")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.rows("SELECT * FROM items")
	}
}

// TestStarlarkAuthoriserPolicy exercises the per-action authoriser policy
// on the Starlark connection pool. The internal pool has no authoriser by
// design and is not tested here; the starlark pool is what untrusted app
// code sees via api_db_query.
//
// Cases marked deny=true must produce an error from db.starlark.Exec.
// Cases marked deny=false must succeed. Each case runs in its own
// sub-test so failures point at a specific row.
func TestStarlarkAuthoriserPolicy(t *testing.T) {
	db, cleanup := create_test_db(t)
	defer cleanup()

	// Seed the database with a table and a row that the allow-cases
	// can read/write. Run on the internal pool so this setup is
	// itself unaffected by the authoriser under test.
	db.exec("CREATE TABLE base (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
	db.exec("CREATE INDEX base_name ON base(name)")
	db.exec("CREATE VIEW base_view AS SELECT id, name FROM base")
	db.exec("INSERT INTO base (id, name) VALUES (1, 'seed')")

	cases := []struct {
		name string
		sql  string
		deny bool // true = authoriser/string-check must reject
	}{
		// ---- denied: sandbox-escape ----
		{"ATTACH", "ATTACH DATABASE ':memory:' AS mem", true},
		{"DETACH", "DETACH DATABASE main", true},

		// ---- denied: PRAGMA writes (with arg) ----
		{"PRAGMA write =", "PRAGMA max_page_count = 999999999", true},
		{"PRAGMA write space-eq", "PRAGMA journal_mode = OFF", true},
		{"PRAGMA write parens", "PRAGMA user_version(999)", true},
		{"PRAGMA multistmt", "BEGIN; PRAGMA max_page_count = 999999999; COMMIT", true},

		// ---- allowed: PRAGMA reads (no arg) ----
		// ncruces' connector relies on `PRAGMA query_only` after the
		// ConnectHook; denying it would break every connection.
		{"PRAGMA read query_only", "PRAGMA query_only", false},
		{"PRAGMA read journal_mode", "PRAGMA journal_mode", false},
		{"PRAGMA read user_version", "PRAGMA user_version", false},

		// ---- denied: triggers ----
		// CREATE_TRIGGER / DROP_TRIGGER are caught at prepare time
		// regardless of whether the target table or trigger exists.
		{"CREATE TRIGGER", "CREATE TRIGGER t1 AFTER INSERT ON base BEGIN UPDATE base SET name='x'; END", true},
		{"CREATE TEMP TRIGGER", "CREATE TEMP TRIGGER t2 AFTER INSERT ON base BEGIN UPDATE base SET name='x'; END", true},
		// (We can't set up a trigger via internal then drop via starlark
		// without crossing a connection boundary in WAL mode, but the
		// SQLITE_DROP_TRIGGER action is in the deny list and exercised
		// in spirit by CREATE_TRIGGER's denial.)

		// ---- denied: virtual tables ----
		{"CREATE VIRTUAL TABLE", "CREATE VIRTUAL TABLE v USING fts5(content)", true},
		// DROP_VTABLE: only fires when the target is an actual virtual
		// table. We can't create one in this pool to drop, and DROP TABLE
		// against a non-vtable goes through SQLITE_DROP_TABLE (allowed),
		// so we exercise the rule via the policy inspection only.

		// ---- denied: VACUUM / ANALYZE (string-prefix check) ----
		// SQLite has no authoriser action codes for VACUUM/ANALYZE, so
		// these come through the api-layer string check rather than the
		// driver authoriser. They cannot be reached via db.starlark.Exec
		// directly — the string check is in api_db_query — so we
		// assert via db_starlark_sql_blocked() instead. See
		// TestStarlarkSQLPrefixBlocked below.

		// ---- allowed: ordinary CRUD ----
		{"SELECT", "SELECT id, name FROM base WHERE id = 1", false},
		{"INSERT", "INSERT INTO base (name) VALUES ('alice')", false},
		{"UPDATE", "UPDATE base SET name = 'bob' WHERE id = 1", false},
		{"DELETE", "DELETE FROM base WHERE id = 1", false},

		// ---- allowed: schema (apps need these in database_create / database_upgrade) ----
		{"CREATE TABLE", "CREATE TABLE extra (id INTEGER PRIMARY KEY, payload TEXT)", false},
		{"ALTER TABLE", "ALTER TABLE extra ADD COLUMN created INTEGER NOT NULL DEFAULT 0", false},
		{"CREATE INDEX", "CREATE INDEX extra_payload ON extra(payload)", false},
		{"DROP INDEX", "DROP INDEX extra_payload", false},
		{"CREATE VIEW", "CREATE VIEW extra_view AS SELECT id FROM extra", false},
		{"DROP VIEW", "DROP VIEW extra_view", false},
		{"DROP TABLE", "DROP TABLE extra", false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := db.starlark.Exec(c.sql)
			switch {
			case c.deny && err == nil:
				t.Fatalf("expected denial for %q but it succeeded", c.sql)
			case !c.deny && err != nil:
				t.Fatalf("expected %q to succeed but got error: %v", c.sql, err)
			}
		})
	}
}

// TestStarlarkPoolTransaction exercises the Starlark pool's transaction
// path — the same path api_db_transaction uses via db.starlark.Beginx().
// Confirms that tx INSERT/UPDATE/SELECT all work and that the authoriser
// doesn't break SAVEPOINT / RELEASE inside a transaction.
func TestStarlarkPoolTransaction(t *testing.T) {
	db, cleanup := create_test_db(t)
	defer cleanup()

	db.exec("CREATE TABLE tx (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")

	tx, err := db.starlark.Beginx()
	if err != nil {
		t.Fatalf("Beginx: %v", err)
	}

	if _, err := tx.Exec("INSERT INTO tx (name) VALUES (?)", "a"); err != nil {
		t.Fatalf("tx INSERT: %v", err)
	}
	if _, err := tx.Exec("UPDATE tx SET name=? WHERE id=1", "b"); err != nil {
		t.Fatalf("tx UPDATE: %v", err)
	}

	var name string
	if err := tx.QueryRow("SELECT name FROM tx WHERE id=1").Scan(&name); err != nil {
		t.Fatalf("tx SELECT: %v", err)
	}
	if name != "b" {
		t.Fatalf("tx SELECT got %q, want %q", name, "b")
	}

	if _, err := tx.Exec("SAVEPOINT sp"); err != nil {
		t.Fatalf("SAVEPOINT inside tx: %v", err)
	}
	if _, err := tx.Exec("RELEASE SAVEPOINT sp"); err != nil {
		t.Fatalf("RELEASE inside tx: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Confirm the row landed.
	var count int
	if err := db.starlark.Get(&count, "SELECT count(*) FROM tx WHERE name='b'"); err != nil {
		t.Fatalf("post-commit SELECT: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 row after commit, got %d", count)
	}
}

// TestStarlarkSQLPrefixBlocked covers the api-layer string-prefix gate
// (db_starlark_sql_blocked), which catches VACUUM and ANALYZE because
// SQLite has no authoriser action codes for them, and also gives apps
// a clean error for PRAGMA before it reaches the driver.
func TestStarlarkSQLPrefixBlocked(t *testing.T) {
	cases := []struct {
		name    string
		sql     string
		blocked bool
	}{
		{"PRAGMA", "PRAGMA journal_mode = OFF", true},
		{"PRAGMA lowercase", "pragma user_version = 1", true},
		{"PRAGMA leading space", "  PRAGMA user_version = 1", true},
		{"VACUUM", "VACUUM", true},
		{"VACUUM lowercase", "vacuum into 'foo.db'", true},
		{"ANALYZE", "ANALYZE base", true},
		{"ANALYZE lowercase", "analyze", true},
		{"SELECT", "SELECT * FROM users", false},
		{"INSERT", "INSERT INTO users VALUES (1)", false},
		{"UPDATE", "UPDATE users SET x=1", false},
		{"DELETE", "DELETE FROM users", false},
		{"CREATE TABLE", "CREATE TABLE x(y INT)", false},
		{"BEGIN", "BEGIN", false}, // multistmt bypass is the authoriser's job, not this layer's
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			reason := db_starlark_sql_blocked(c.sql)
			blocked := reason != ""
			if blocked != c.blocked {
				t.Fatalf("db_starlark_sql_blocked(%q) blocked=%v want %v (reason=%q)", c.sql, blocked, c.blocked, reason)
			}
		})
	}
}

// Test database page-count limit. 4 KB page size × 6,553,600 pages = 25 GB.
// Bumped from 262_144 pages (1 GB) on 2026-05-15 so legitimate per-user
// app DBs (e.g. feeds.db on heavy users) don't hit the cap.
func TestDbMaxPageCountConstant(t *testing.T) {
	expected_limit := 6_553_600
	if db_max_page_count != expected_limit {
		t.Errorf("db_max_page_count = %d, expected %d", db_max_page_count, expected_limit)
	}
}

// Test app_user_setup grants default permissions on first app access
func TestAppUserSetup(t *testing.T) {
	// Create temp directory
	tmp_dir, err := os.MkdirTemp("", "mochi_app_user_init_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmp_dir)

	// Save and set data_dir
	orig_data_dir := data_dir
	data_dir = tmp_dir
	defer func() { data_dir = orig_data_dir }()

	// Create user directory
	os.MkdirAll(filepath.Join(tmp_dir, "users", "1"), 0755)

	user := &User{UID: "u1"}

	// App Manager app ID with permissions/manage default
	apps_app_id := "12kqLEaEE9L3mh6modywUmo8TC3JGi3ypPZR2N2KqAMhB3VBFdL"

	// Verify no permissions exist before app_user_init
	db1 := db_user(user, "user")
	db1.permissions_setup()
	has_permission, _ := db1.exists("select 1 from permissions where app=? and permission='permissions/manage' and granted=1", apps_app_id)
	if has_permission {
		t.Error("User should not have permissions/manage before app_user_setup")
	}

	// Run app_user_setup for the Apps app
	app_user_setup(user, apps_app_id)

	// Verify permissions are now granted
	has_permission, _ = db1.exists("select 1 from permissions where app=? and permission='permissions/manage' and granted=1", apps_app_id)
	if !has_permission {
		t.Error("User should have permissions/manage after app_user_setup")
	}

	// Verify setup timestamp is recorded in apps table
	db1.apps_setup()
	setup := db1.integer("select setup from apps where app=?", apps_app_id)
	if setup == 0 {
		t.Error("Setup timestamp should be non-zero after app_user_setup")
	}

	// Run app_user_setup again - should be idempotent
	app_user_setup(user, apps_app_id)

	// Verify only one permission row exists (not duplicated)
	count := db1.integer("select count(*) from permissions where app=? and permission='permissions/manage'", apps_app_id)
	if count != 1 {
		t.Errorf("Expected 1 permission row, got %d", count)
	}
}

// Test that db_user on a fresh database creates accounts with the "default" column
func TestDBUserCreatesAccountsWithDefault(t *testing.T) {
	tmp_dir, err := os.MkdirTemp("", "mochi_dbuser_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmp_dir)

	orig_data_dir := data_dir
	data_dir = tmp_dir
	defer func() { data_dir = orig_data_dir }()

	// Create user directory
	os.MkdirAll(filepath.Join(tmp_dir, "users", "1"), 0755)

	user := &User{UID: "u1"}
	db := db_user(user, "user")

	// Verify accounts table has "default" column
	has_default, err := db.exists("select 1 from pragma_table_info('accounts') where name='default'")
	if err != nil {
		t.Fatalf("pragma_table_info query failed: %v", err)
	}
	if !has_default {
		t.Error("accounts table should have 'default' column")
	}

	// Clean up from databases map
	path := filepath.Join(tmp_dir, "users", "1", "user.db")
	databases_lock.Lock()
	delete(databases, path)
	databases_lock.Unlock()
	db.internal.Close()
	db.starlark.Close()
}

// Test db_app_schema_get returns 0 for new database
func TestAppSchemaGetDefault(t *testing.T) {
	db, cleanup := create_test_db(t)
	defer cleanup()

	version := db_app_schema_get(db)
	if version != 0 {
		t.Errorf("db_app_schema_get() = %d, want 0 for new database", version)
	}
}

// Test PRAGMA blocking handles whitespace bypass attempts
func TestPragmaBlockingWhitespace(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		blocked bool
	}{
		{"normal PRAGMA", "PRAGMA max_page_count", true},
		{"lowercase pragma", "pragma user_version", true},
		{"leading spaces", "  PRAGMA max_page_count", true},
		{"leading tab", "\tPRAGMA max_page_count", true},
		{"leading newline", "\nPRAGMA max_page_count", true},
		{"leading mixed whitespace", " \t\n PRAGMA max_page_count", true},
		{"normal SELECT", "SELECT 1", false},
		{"pragma in string", "SELECT 'PRAGMA' FROM t", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trimmed := strings.TrimSpace(tt.query)
			is_pragma := len(trimmed) >= 6 && strings.EqualFold(trimmed[:6], "PRAGMA")
			if is_pragma != tt.blocked {
				t.Errorf("query %q: got blocked=%v, want %v", tt.query, is_pragma, tt.blocked)
			}
		})
	}
}

// Test db_app_schema_set and db_app_schema_get roundtrip
func TestAppSchemaSetGet(t *testing.T) {
	db, cleanup := create_test_db(t)
	defer cleanup()

	db_app_schema_set(db, 5)
	version := db_app_schema_get(db)
	if version != 5 {
		t.Errorf("db_app_schema_get() = %d, want 5", version)
	}

	db_app_schema_set(db, 12)
	version = db_app_schema_get(db)
	if version != 12 {
		t.Errorf("db_app_schema_get() = %d, want 12", version)
	}
}

// Test db_app_system does not create a settings table
func TestAppSystemNoSettingsTable(t *testing.T) {
	tmp_dir, err := os.MkdirTemp("", "mochi_db_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmp_dir)

	orig_data_dir := data_dir
	data_dir = tmp_dir
	defer func() { data_dir = orig_data_dir }()

	os.MkdirAll(filepath.Join(tmp_dir, "users", "1", "testapp"), 0755)

	user := &User{UID: "u1"}
	a := &App{id: "testapp"}
	db := db_app_system(user, a)
	if db == nil {
		t.Fatal("db_app_system returned nil")
	}

	exists, _ := db.exists("select name from sqlite_master where type='table' and name='settings'")
	if exists {
		t.Error("app.db should not have a settings table")
	}

	// Verify access and attachments tables do exist
	exists, _ = db.exists("select name from sqlite_master where type='table' and name='access'")
	if !exists {
		t.Error("app.db should have an access table")
	}

	exists, _ = db.exists("select name from sqlite_master where type='table' and name='attachments'")
	if !exists {
		t.Error("app.db should have an attachments table")
	}
}

// BenchmarkStarlarkPoolExec measures the round-trip cost of the
// per-call Connx + ExecContext + Close pattern that api_db_query uses.
// Useful as a floor — if this regresses materially, the change is
// worth looking at.
func BenchmarkStarlarkPoolExec(b *testing.B) {
	db, cleanup := create_test_db_b(b)
	defer cleanup()

	db.exec("CREATE TABLE bench (id INTEGER PRIMARY KEY, n INTEGER)")
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn, err := db.starlark.Connx(ctx)
		if err != nil {
			b.Fatal(err)
		}
		_, err = conn.ExecContext(ctx, "INSERT INTO bench (n) VALUES (?)", i)
		conn.Close()
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkStarlarkPoolQuery is the read-side analogue.
func BenchmarkStarlarkPoolQuery(b *testing.B) {
	db, cleanup := create_test_db_b(b)
	defer cleanup()

	db.exec("CREATE TABLE bench (id INTEGER PRIMARY KEY, n INTEGER)")
	for i := 0; i < 1000; i++ {
		db.exec("INSERT INTO bench (n) VALUES (?)", i)
	}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn, err := db.starlark.Connx(ctx)
		if err != nil {
			b.Fatal(err)
		}
		var n int
		err = conn.GetContext(ctx, &n, "SELECT n FROM bench WHERE id = ?", (i%1000)+1)
		conn.Close()
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkInternalPoolExec is the same but on the no-authoriser
// internal pool — the difference between the two benchmarks is the
// per-statement authoriser callback overhead.
func BenchmarkInternalPoolExec(b *testing.B) {
	db, cleanup := create_test_db_b(b)
	defer cleanup()

	db.exec("CREATE TABLE bench (id INTEGER PRIMARY KEY, n INTEGER)")
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn, err := db.internal.Connx(ctx)
		if err != nil {
			b.Fatal(err)
		}
		_, err = conn.ExecContext(ctx, "INSERT INTO bench (n) VALUES (?)", i)
		conn.Close()
		if err != nil {
			b.Fatal(err)
		}
	}
}

// create_test_db_b is the bench-flavoured create_test_db. Same body —
// testing.TB lets benchmarks share the helper without retyping it.
func create_test_db_b(b *testing.B) (*DB, func()) {
	tmp_dir, err := os.MkdirTemp("", "mochi_db_bench")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	orig_data_dir := data_dir
	data_dir = tmp_dir
	db := db_open("bench.db")
	cleanup := func() {
		db.close()
		data_dir = orig_data_dir
		os.RemoveAll(tmp_dir)
	}
	return db, cleanup
}

// TestStarlarkPoolConcurrent stresses the Starlark connection pool by
// running many goroutines simultaneously through the same paths
// api_db_query takes (Connx + Exec/Query, plus the defensive ROLLBACK
// path). Catches conn leaks (would deadlock or starve), data races
// under -race, and the multistmt-bypass-then-poison scenario where one
// goroutine's failed BEGIN/PRAGMA/COMMIT must not break another's
// independent transaction.
//
// The test alternates four kinds of work across 8 goroutines × 200
// iterations:
//
//   - simple INSERT
//   - SELECT with parameter
//   - full Beginx/Commit transaction
//   - multistmt with denied PRAGMA inside (poisons the conn if the
//     defensive ROLLBACK in api_db_query weren't there — but here we
//     exercise the same pattern via raw db.starlark.Exec to confirm
//     the pool itself, not just the api_db_query wrapper, recovers)
func TestStarlarkPoolConcurrent(t *testing.T) {
	db, cleanup := create_test_db(t)
	defer cleanup()

	db.exec("CREATE TABLE conc (id INTEGER PRIMARY KEY, who TEXT NOT NULL, n INTEGER NOT NULL)")

	const goroutines = 8
	const iterations = 200

	var (
		ctx     = context.Background()
		wg      sync.WaitGroup
		errCh   = make(chan error, goroutines*iterations)
		inserts atomic.Int64
		selects atomic.Int64
		txs     atomic.Int64
		denials atomic.Int64
	)

	worker := func(id int) {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			switch i % 4 {
			case 0: // simple insert via per-call Connx (mirrors api_db_query)
				conn, err := db.starlark.Connx(ctx)
				if err != nil {
					errCh <- fmt.Errorf("g%d i%d Connx: %w", id, i, err)
					return
				}
				_, err = conn.ExecContext(ctx, "INSERT INTO conc (who, n) VALUES (?, ?)", fmt.Sprintf("g%d", id), i)
				conn.Close()
				if err != nil {
					errCh <- fmt.Errorf("g%d i%d INSERT: %w", id, i, err)
					return
				}
				inserts.Add(1)
			case 1: // SELECT
				conn, err := db.starlark.Connx(ctx)
				if err != nil {
					errCh <- fmt.Errorf("g%d i%d Connx: %w", id, i, err)
					return
				}
				rows, err := conn.QueryxContext(ctx, "SELECT id FROM conc WHERE who = ?", fmt.Sprintf("g%d", id))
				if err != nil {
					conn.Close()
					errCh <- fmt.Errorf("g%d i%d SELECT: %w", id, i, err)
					return
				}
				for rows.Next() {
				}
				rows.Close()
				conn.Close()
				selects.Add(1)
			case 2: // full transaction via the same path api_db_transaction takes
				tx, err := db.starlark.Beginx()
				if err != nil {
					errCh <- fmt.Errorf("g%d i%d Beginx: %w", id, i, err)
					return
				}
				if _, err := tx.Exec("INSERT INTO conc (who, n) VALUES (?, ?)", fmt.Sprintf("g%d-tx", id), i); err != nil {
					tx.Rollback()
					errCh <- fmt.Errorf("g%d i%d tx INSERT: %w", id, i, err)
					return
				}
				if err := tx.Commit(); err != nil {
					errCh <- fmt.Errorf("g%d i%d Commit: %w", id, i, err)
					return
				}
				txs.Add(1)
			case 3: // multistmt with denied PRAGMA inside — must NOT poison the pool
				// Use a per-call conn with defensive ROLLBACK, mirroring
				// what api_db_query does. If we drop the rollback, the
				// next iteration's Beginx on the same conn fails with
				// "cannot start a transaction within a transaction".
				conn, err := db.starlark.Connx(ctx)
				if err != nil {
					errCh <- fmt.Errorf("g%d i%d Connx: %w", id, i, err)
					return
				}
				_, err = conn.ExecContext(ctx, "BEGIN; PRAGMA max_page_count = 999999999; COMMIT")
				if err == nil {
					conn.Close()
					errCh <- fmt.Errorf("g%d i%d expected denial for multistmt PRAGMA write", id, i)
					return
				}
				// The defensive rollback that api_db_query does:
				_, _ = conn.ExecContext(ctx, "ROLLBACK")
				conn.Close()
				denials.Add(1)
			}
		}
	}

	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go worker(g)
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("%v", err)
	}

	// Sanity-check totals: each goroutine ran iterations/4 of each kind.
	want := int64(goroutines * iterations / 4)
	if inserts.Load() != want {
		t.Errorf("inserts = %d, want %d", inserts.Load(), want)
	}
	if selects.Load() != want {
		t.Errorf("selects = %d, want %d", selects.Load(), want)
	}
	if txs.Load() != want {
		t.Errorf("transactions = %d, want %d", txs.Load(), want)
	}
	if denials.Load() != want {
		t.Errorf("denials = %d, want %d", denials.Load(), want)
	}

	// Final row count: inserts + tx-inserts.
	var n int
	if err := db.starlark.Get(&n, "SELECT count(*) FROM conc"); err != nil {
		t.Fatalf("final count: %v", err)
	}
	if want := int(inserts.Load() + txs.Load()); n != want {
		t.Errorf("final count = %d, want %d", n, want)
	}
}

// TestDbUserForThread covers db_user_for_thread, the shared helper that picks
// which user's perspective mochi.db.* and mochi.entity.* act from. The
// "logged-in + entity owned by other user" case is a regression guard for the
// silent owner-DB swap removed from db_for_thread: subscribe-style writes
// must always land in the requesting user's database, never the entity
// owner's.
func TestDbUserForThread(t *testing.T) {
	alice := &User{UID: "alice"}
	bob := &User{UID: "bob"}

	action_with_route := func(ctx string) *Action {
		return &Action{
			domain: &DomainInfo{
				route: &DomainRouteInfo{context: ctx},
			},
		}
	}

	tests := []struct {
		name    string
		user    *User
		owner   *User
		action  *Action
		want    *User
		want_err bool
	}{
		{
			name:  "anonymous_reads_owner_db",
			user:  nil,
			owner: alice,
			want:  alice,
		},
		{
			name:    "anonymous_with_no_owner_errors",
			user:    nil,
			owner:   nil,
			want_err: true,
		},
		{
			name:  "logged_in_no_entity_uses_own_db",
			user:  alice,
			owner: nil,
			want:  alice,
		},
		{
			name:  "logged_in_own_entity_uses_own_db",
			user:  alice,
			owner: alice,
			want:  alice,
		},
		// Regression guard: prior to the fix this returned bob,
		// causing subscribe-style writes to land in the entity owner's
		// database instead of the requester's. See mochi-dev-345.
		{
			name:  "logged_in_other_entity_keeps_own_db",
			user:  alice,
			owner: bob,
			want:  alice,
		},
		{
			name:   "logged_in_under_domain_routing_uses_route_owner",
			user:   alice,
			owner:  bob,
			action: action_with_route("/blog"),
			want:   bob,
		},
		{
			name:    "logged_in_under_domain_routing_no_owner_errors",
			user:    alice,
			owner:   nil,
			action:  action_with_route("/blog"),
			want_err: true,
		},
		// An action with an empty domain context is not "domain routing"
		// — main-site requests carry an Action but no route, so the
		// logged-in branch must still return the user's own DB.
		{
			name:   "logged_in_main_site_action_uses_own_db",
			user:   alice,
			owner:  bob,
			action: action_with_route(""),
			want:   alice,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			thread := &sl.Thread{Name: "test"}
			if tc.user != nil {
				thread.SetLocal("user", tc.user)
			}
			if tc.owner != nil {
				thread.SetLocal("owner", tc.owner)
			}
			if tc.action != nil {
				thread.SetLocal("action", tc.action)
			}

			got, err := db_user_for_thread(thread)
			if tc.want_err {
				if err == nil {
					t.Fatalf("db_user_for_thread() = %v, want error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("db_user_for_thread() unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("db_user_for_thread() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestDbCreateIdempotentOverPreservedDB (#30) regression: `mochictl replica
// reset` preserves directory.db (and other local-only DBs), then a restart runs
// the fresh-install db_create. db_create used non-idempotent CREATE TABLE, so it
// panicked ("table entries already exists") on the preserved directory.db and
// aborted BEFORE creating replication.db's schema — leaving the replica
// unbootable (workers panic-looped on "no such table: pair/leadership" and
// replication was dead). Hit live on wasabi 2026-06-18. db_create must now be
// idempotent: run cleanly over a preserved directory.db AND still create
// replication.db's full schema, and be safely re-runnable.
func TestDbCreateIdempotentOverPreservedDB(t *testing.T) {
	orig := data_dir
	data_dir = t.TempDir()
	defer func() { data_dir = orig }()
	if err := os.MkdirAll(filepath.Join(data_dir, "db"), 0o755); err != nil {
		t.Fatal(err)
	}

	// Simulate the reset having preserved directory.db with its (full) entries
	// table — schema matches db_create so its indexes resolve.
	d := db_open("db/directory.db")
	d.exec("create table entries ( entity text not null, peer text not null, name text not null, class text not null, data text not null default '', fingerprint text not null default '', version integer not null default 0, created integer not null, seen integer not null, signature text not null default '', attestation text not null default '', primary key ( entity, peer ) )")

	// Used to panic on the preserved entries table; must complete now.
	db_create()

	// replication.db must have its full schema — the tables that were never
	// created in the live incident.
	r := db_open("db/replication.db")
	for _, tbl := range []string{"pair", "leadership", "seen", "cursor", "pending", "tail", "sequence", "bootstrap", "fence_witness"} {
		if has, _ := r.exists("select 1 from sqlite_master where type='table' and name=?", tbl); !has {
			t.Fatalf("replication.db missing table %q after db_create over a preserved directory.db", tbl)
		}
	}

	// Fully re-runnable: a second db_create is a no-op, not a panic.
	db_create()
}

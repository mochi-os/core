// Mochi server: Database unit tests
// Copyright Alistair Cunningham 2025

package main

import (
	"os"
	"path/filepath"
	"testing"
)

// Helper to create a test database
func createTestDB(t *testing.T) (*DB, func()) {
	// Create a temp directory for the test database
	tmpDir, err := os.MkdirTemp("", "mochi_db_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	// Save original data_dir and set to temp
	origDataDir := data_dir
	data_dir = tmpDir

	// Create test database
	dbPath := "test.db"
	db := db_open(dbPath)

	cleanup := func() {
		db.close()
		data_dir = origDataDir
		os.RemoveAll(tmpDir)
	}

	return db, cleanup
}

// Test db.exec creates tables
func TestDBExec(t *testing.T) {
	db, cleanup := createTestDB(t)
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
	db, cleanup := createTestDB(t)
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
	db, cleanup := createTestDB(t)
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
	db, cleanup := createTestDB(t)
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
	db, cleanup := createTestDB(t)
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
	db, cleanup := createTestDB(t)
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
	db, cleanup := createTestDB(t)
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
	db, cleanup := createTestDB(t)
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
	db, cleanup := createTestDB(t)
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
	db, cleanup := createTestDB(t)
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
	db, cleanup := createTestDB(t)
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
	db, cleanup := createTestDB(t)
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

// Test db.schema creates _settings table
func TestDBSchema(t *testing.T) {
	db, cleanup := createTestDB(t)
	defer cleanup()

	db.schema(5)

	// Check _settings table exists
	exists, _ := db.exists("SELECT name FROM sqlite_master WHERE type='table' AND name='_settings'")
	if !exists {
		t.Error("_settings table should exist after schema()")
	}

	// Check schema version is set
	version := db.integer("SELECT CAST(value AS INTEGER) FROM _settings WHERE name='schema'")
	if version != 5 {
		t.Errorf("schema version = %d, want 5", version)
	}
}

// Test db.schema updates existing version
func TestDBSchemaUpdate(t *testing.T) {
	db, cleanup := createTestDB(t)
	defer cleanup()

	db.schema(1)
	db.schema(2)
	db.schema(3)

	version := db.integer("SELECT CAST(value AS INTEGER) FROM _settings WHERE name='schema'")
	if version != 3 {
		t.Errorf("schema version = %d, want 3", version)
	}
}

// Test database path creation
func TestDBOpenCreatesFile(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mochi_db_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	origDataDir := data_dir
	data_dir = tmpDir
	defer func() { data_dir = origDataDir }()

	// Create database in nested path
	db := db_open("nested/path/test.db")
	db.exec("CREATE TABLE test (id INTEGER)")
	db.close()

	// Verify file was created
	path := filepath.Join(tmpDir, "nested", "path", "test.db")
	if !file_exists(path) {
		t.Errorf("Database file should exist at %s", path)
	}
}

// Test concurrent database access
func TestDBConcurrentAccess(t *testing.T) {
	db, cleanup := createTestDB(t)
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
	tmpDir, _ := os.MkdirTemp("", "mochi_db_bench")
	defer os.RemoveAll(tmpDir)

	origDataDir := data_dir
	data_dir = tmpDir
	defer func() { data_dir = origDataDir }()

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
	tmpDir, _ := os.MkdirTemp("", "mochi_db_bench")
	defer os.RemoveAll(tmpDir)

	origDataDir := data_dir
	data_dir = tmpDir
	defer func() { data_dir = origDataDir }()

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
	tmpDir, _ := os.MkdirTemp("", "mochi_db_bench")
	defer os.RemoveAll(tmpDir)

	origDataDir := data_dir
	data_dir = tmpDir
	defer func() { data_dir = origDataDir }()

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

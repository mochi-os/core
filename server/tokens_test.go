// Mochi server: Token unit tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"strings"
	"testing"
	"time"

	sl "go.starlark.net/starlark"
)

// Test token generation format
func TestTokenGenerate(t *testing.T) {
	token := token_generate()

	// Should start with mochi- prefix
	if !strings.HasPrefix(token, "mochi-") {
		t.Errorf("Token should start with 'mochi-', got: %s", token)
	}

	// Should be 46 characters total (6 prefix + 40 hex)
	if len(token) != 46 {
		t.Errorf("Token should be 46 characters, got: %d", len(token))
	}

	// Generate another token - should be different
	token2 := token_generate()
	if token == token2 {
		t.Error("Two generated tokens should not be identical")
	}
}

// Test token hashing is deterministic
func TestTokenHash(t *testing.T) {
	token := "mochi-0123456789abcdef0123456789abcdef01234567"

	hash1 := token_hash(token)
	hash2 := token_hash(token)

	if hash1 != hash2 {
		t.Error("Same token should produce same hash")
	}

	// Hash should be 64 characters (SHA256 hex)
	if len(hash1) != 64 {
		t.Errorf("Hash should be 64 characters, got: %d", len(hash1))
	}
}

// Test different tokens produce different hashes
func TestTokenHashUnique(t *testing.T) {
	token1 := token_generate()
	token2 := token_generate()

	hash1 := token_hash(token1)
	hash2 := token_hash(token2)

	if hash1 == hash2 {
		t.Error("Different tokens should produce different hashes")
	}
}

// Test token hash is hex encoded
func TestTokenHashFormat(t *testing.T) {
	token := token_generate()
	hash := token_hash(token)

	for _, c := range hash {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			t.Errorf("Hash should be lowercase hex, found character: %c", c)
		}
	}
}

// Test generating many tokens are all unique
func TestTokenGenerateUniqueness(t *testing.T) {
	tokens := make(map[string]bool)
	count := 1000

	for i := 0; i < count; i++ {
		token := token_generate()
		if tokens[token] {
			t.Errorf("Duplicate token generated: %s", token)
		}
		tokens[token] = true
	}
}

// Test token prefix is correct
func TestTokenPrefix(t *testing.T) {
	for i := 0; i < 100; i++ {
		token := token_generate()
		if !strings.HasPrefix(token, "mochi-") {
			t.Errorf("Token missing prefix: %s", token)
		}
	}
}

// Test empty token hash
func TestTokenHashEmpty(t *testing.T) {
	hash := token_hash("")
	if hash == "" {
		t.Error("Hash of empty string should not be empty")
	}
	if len(hash) != 64 {
		t.Errorf("Hash should be 64 characters, got: %d", len(hash))
	}
}

// Helper to create test database for token tests
func create_token_test_db(t *testing.T) (*DB, func()) {
	tmp_dir, err := os.MkdirTemp("", "mochi_token_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	orig_data_dir := data_dir
	data_dir = tmp_dir

	// Create users database with tokens table
	db := db_open("db/users.db")
	db.exec(`CREATE TABLE IF NOT EXISTS users (
		id INTEGER PRIMARY KEY,
		username TEXT NOT NULL DEFAULT ''
	)`)
	db.exec(`CREATE TABLE IF NOT EXISTS tokens (
		hash TEXT PRIMARY KEY NOT NULL,
		user INTEGER NOT NULL,
		name TEXT NOT NULL DEFAULT '',
		scopes TEXT NOT NULL DEFAULT '',
		expires TEXT NOT NULL DEFAULT '',
		created TEXT NOT NULL DEFAULT '',
		last_used TEXT NOT NULL DEFAULT ''
	)`)

	// Create a test user
	db.exec("INSERT INTO users (id, username) VALUES (1, 'testuser')")

	cleanup := func() {
		db.close()
		data_dir = orig_data_dir
		os.RemoveAll(tmp_dir)
	}

	return db, cleanup
}

// Test token creation in database
func TestTokenCreateInDB(t *testing.T) {
	db, cleanup := create_token_test_db(t)
	defer cleanup()

	token := token_generate()
	hash := token_hash(token)
	now := time.Now().Format("2006-01-02 15:04:05")

	db.exec(`INSERT INTO tokens (hash, user, name, scopes, created) VALUES (?, ?, ?, ?, ?)`,
		hash, 1, "Test Token", `["read"]`, now)

	// Verify token exists
	row, err := db.row("SELECT user, name, scopes FROM tokens WHERE hash = ?", hash)
	if err != nil {
		t.Fatalf("Failed to query token: %v", err)
	}
	if row == nil {
		t.Fatal("Token not found in database")
	}

	if row["name"] != "Test Token" {
		t.Errorf("Expected name 'Test Token', got: %v", row["name"])
	}
}

// Test token lookup by hash
func TestTokenLookupByHash(t *testing.T) {
	db, cleanup := create_token_test_db(t)
	defer cleanup()

	token := token_generate()
	hash := token_hash(token)

	db.exec(`INSERT INTO tokens (hash, user, name, created) VALUES (?, ?, ?, ?)`,
		hash, 1, "Lookup Test", time.Now().Format("2006-01-02 15:04:05"))

	// Lookup should work with hash
	exists, _ := db.exists("SELECT 1 FROM tokens WHERE hash = ?", hash)
	if !exists {
		t.Error("Token should exist when looking up by hash")
	}

	// Lookup with wrong hash should fail
	exists, _ = db.exists("SELECT 1 FROM tokens WHERE hash = ?", "wronghash")
	if exists {
		t.Error("Token should not exist with wrong hash")
	}
}

// Test token deletion
func TestTokenDeleteFromDB(t *testing.T) {
	db, cleanup := create_token_test_db(t)
	defer cleanup()

	token := token_generate()
	hash := token_hash(token)

	db.exec(`INSERT INTO tokens (hash, user, name, created) VALUES (?, ?, ?, ?)`,
		hash, 1, "Delete Test", time.Now().Format("2006-01-02 15:04:05"))

	// Verify exists
	exists, _ := db.exists("SELECT 1 FROM tokens WHERE hash = ?", hash)
	if !exists {
		t.Fatal("Token should exist before delete")
	}

	// Delete
	db.exec("DELETE FROM tokens WHERE hash = ?", hash)

	// Verify deleted
	exists, _ = db.exists("SELECT 1 FROM tokens WHERE hash = ?", hash)
	if exists {
		t.Error("Token should not exist after delete")
	}
}

// Test listing tokens for user
func TestTokenListForUser(t *testing.T) {
	db, cleanup := create_token_test_db(t)
	defer cleanup()

	// Create multiple tokens for user 1
	for i := 0; i < 5; i++ {
		token := token_generate()
		hash := token_hash(token)
		db.exec(`INSERT INTO tokens (hash, user, name, created) VALUES (?, ?, ?, ?)`,
			hash, 1, "Token", time.Now().Format("2006-01-02 15:04:05"))
	}

	// Create token for different user
	db.exec("INSERT INTO users (id, username) VALUES (2, 'otheruser')")
	other_token := token_generate()
	db.exec(`INSERT INTO tokens (hash, user, name, created) VALUES (?, ?, ?, ?)`,
		token_hash(other_token), 2, "Other", time.Now().Format("2006-01-02 15:04:05"))

	// Count tokens for user 1
	row, _ := db.row("SELECT COUNT(*) as count FROM tokens WHERE user = ?", 1)
	count := row["count"].(int64)
	if count != 5 {
		t.Errorf("Expected 5 tokens for user 1, got: %d", count)
	}

	// Count tokens for user 2
	row, _ = db.row("SELECT COUNT(*) as count FROM tokens WHERE user = ?", 2)
	count = row["count"].(int64)
	if count != 1 {
		t.Errorf("Expected 1 token for user 2, got: %d", count)
	}
}

// Test token scopes stored as JSON
func TestTokenScopesJSON(t *testing.T) {
	db, cleanup := create_token_test_db(t)
	defer cleanup()

	token := token_generate()
	hash := token_hash(token)
	scopes := `["repositories:read","repositories:write"]`

	db.exec(`INSERT INTO tokens (hash, user, name, scopes, created) VALUES (?, ?, ?, ?, ?)`,
		hash, 1, "Scoped Token", scopes, time.Now().Format("2006-01-02 15:04:05"))

	row, _ := db.row("SELECT scopes FROM tokens WHERE hash = ?", hash)
	if row["scopes"] != scopes {
		t.Errorf("Expected scopes %q, got: %v", scopes, row["scopes"])
	}
}

// Test token last_used update
func TestTokenLastUsedUpdate(t *testing.T) {
	db, cleanup := create_token_test_db(t)
	defer cleanup()

	token := token_generate()
	hash := token_hash(token)

	db.exec(`INSERT INTO tokens (hash, user, name, created, last_used) VALUES (?, ?, ?, ?, '')`,
		hash, 1, "Usage Test", time.Now().Format("2006-01-02 15:04:05"))

	// Check last_used is empty
	row, _ := db.row("SELECT last_used FROM tokens WHERE hash = ?", hash)
	if row["last_used"] != "" {
		t.Error("last_used should be empty initially")
	}

	// Update last_used
	now := time.Now().Format("2006-01-02 15:04:05")
	db.exec("UPDATE tokens SET last_used = ? WHERE hash = ?", now, hash)

	// Verify update
	row, _ = db.row("SELECT last_used FROM tokens WHERE hash = ?", hash)
	if row["last_used"] != now {
		t.Errorf("last_used should be %q, got: %v", now, row["last_used"])
	}
}

// Test cascade delete when user is deleted
func TestTokenCascadeDelete(t *testing.T) {
	db, cleanup := create_token_test_db(t)
	defer cleanup()

	// Need to recreate with foreign key support
	db.exec("DROP TABLE tokens")
	db.exec(`CREATE TABLE tokens (
		hash TEXT PRIMARY KEY NOT NULL,
		user INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		name TEXT NOT NULL DEFAULT '',
		scopes TEXT NOT NULL DEFAULT '',
		expires TEXT NOT NULL DEFAULT '',
		created TEXT NOT NULL DEFAULT '',
		last_used TEXT NOT NULL DEFAULT ''
	)`)
	db.exec("PRAGMA foreign_keys = ON")

	token := token_generate()
	hash := token_hash(token)
	db.exec(`INSERT INTO tokens (hash, user, name, created) VALUES (?, ?, ?, ?)`,
		hash, 1, "Cascade Test", time.Now().Format("2006-01-02 15:04:05"))

	// Verify token exists
	exists, _ := db.exists("SELECT 1 FROM tokens WHERE hash = ?", hash)
	if !exists {
		t.Fatal("Token should exist before user delete")
	}

	// Delete user
	db.exec("DELETE FROM users WHERE id = 1")

	// Token should be deleted via cascade
	exists, _ = db.exists("SELECT 1 FROM tokens WHERE hash = ?", hash)
	if exists {
		t.Error("Token should be deleted when user is deleted (cascade)")
	}
}

// TestTokenDeleteByTokenStringAndAppScope verifies mochi.token.delete accepts
// the token itself (not only its hash) — so an app that kept only the token
// string can revoke it — and that the delete stays scoped to the owning app.
func TestTokenDeleteByTokenStringAndAppScope(t *testing.T) {
	tmp, err := os.MkdirTemp("", "mochi_tokendel")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmp)
	orig := data_dir
	data_dir = tmp
	defer func() { data_dir = orig }()

	db := db_open("db/users.db")
	db.exec(`create table tokens (hash text primary key, user text not null, app text not null, name text not null default '', scopes text not null default '', created integer not null default 0, expires integer not null default 0)`)
	db_open("db/sessions.db").exec(`create table accesses (hash text primary key, user text not null, used integer not null default 0)`)

	// token_create returns the token STRING (not the hash), stamped app="feeds".
	token := token_create("u1", "feeds", "rss", []string{"rss"}, 0)
	if token == "" || token_lookup(token) == nil {
		t.Fatal("token_create/lookup failed")
	}

	user := &User{UID: "u1"}
	del := sl.NewBuiltin("mochi.token.delete", api_token_delete)

	// A different app must NOT be able to delete it, even holding the token.
	thForums := &sl.Thread{Name: "test"}
	thForums.SetLocal("user", user)
	thForums.SetLocal("app", &App{id: "forums"})
	_, _ = api_token_delete(thForums, del, sl.Tuple{sl.String(token)}, nil)
	if token_lookup(token) == nil {
		t.Fatal("forums must not be able to delete feeds' token")
	}

	// The owning app deletes it by the token STRING (the new behaviour).
	thFeeds := &sl.Thread{Name: "test"}
	thFeeds.SetLocal("user", user)
	thFeeds.SetLocal("app", &App{id: "feeds"})
	res, err := api_token_delete(thFeeds, del, sl.Tuple{sl.String(token)}, nil)
	if err != nil {
		t.Fatalf("delete by token string failed: %v", err)
	}
	if res != sl.True {
		t.Errorf("expected True, got %v", res)
	}
	if token_lookup(token) != nil {
		t.Error("token should be gone after delete-by-string")
	}
}

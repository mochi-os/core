// Mochi server: Users API unit tests
// Copyright Alistair Cunningham 2025

package main

import (
	"os"
	"strings"
	"testing"
)

// Helper to create test environment with users database
func create_test_users_db(t *testing.T) func() {
	tmp_dir, err := os.MkdirTemp("", "mochi_users_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	orig_data_dir := data_dir
	data_dir = tmp_dir

	// Create users table
	db := db_open("db/users.db")
	db.exec("create table users (id integer primary key, username text not null, role text not null default 'user')")
	db.exec("create unique index users_username on users (username)")

	// Create invites table
	db.exec("create table invites (code text not null primary key, uses integer not null default 1, expires integer not null)")
	db.exec("create index invites_expires on invites(expires)")

	cleanup := func() {
		data_dir = orig_data_dir
		os.RemoveAll(tmp_dir)
	}

	return cleanup
}

// Test user_by_id returns nil for non-existent user
func TestUserByIdNotFound(t *testing.T) {
	cleanup := create_test_users_db(t)
	defer cleanup()

	u := user_by_id(999)
	if u != nil {
		t.Error("user_by_id should return nil for non-existent user")
	}
}

// Test user_by_id returns user for existing user
func TestUserByIdFound(t *testing.T) {
	cleanup := create_test_users_db(t)
	defer cleanup()

	db := db_open("db/users.db")
	db.exec("insert into users (id, username, role) values (1, 'test@example.com', 'user')")

	// Need entities table for user_by_id to work fully
	db.exec("create table entities (id text primary key, private text, fingerprint text, user integer, parent text default '', class text, name text, privacy text default 'public', data text default '', published integer default 0)")
	db.exec("insert into entities (id, private, fingerprint, user, class, name) values ('e1', 'priv', 'fp', 1, 'person', 'Test User')")

	// Create preferences table
	db.exec("create table preferences (name text primary key, value text)")

	u := user_by_id(1)
	if u == nil {
		t.Fatal("user_by_id should return user for existing id")
	}
	if u.Username != "test@example.com" {
		t.Errorf("username = %q, want 'test@example.com'", u.Username)
	}
	if u.Role != "user" {
		t.Errorf("role = %q, want 'user'", u.Role)
	}
}

// Test User.administrator() method
func TestUserAdministrator(t *testing.T) {
	admin := &User{ID: 1, Username: "admin@example.com", Role: "administrator"}
	user := &User{ID: 2, Username: "user@example.com", Role: "user"}

	if !admin.administrator() {
		t.Error("administrator() should return true for admin role")
	}
	if user.administrator() {
		t.Error("administrator() should return false for user role")
	}
}

// Test invite creation in database
func TestInviteCreate(t *testing.T) {
	cleanup := create_test_users_db(t)
	defer cleanup()

	db := db_open("db/users.db")
	code := random_alphanumeric(16)
	expires := now() + 86400*7

	db.exec("insert into invites (code, uses, expires) values (?, ?, ?)", code, 5, expires)

	var inv Invite
	if !db.scan(&inv, "select * from invites where code=?", code) {
		t.Fatal("invite should exist after insert")
	}

	if inv.Code != code {
		t.Errorf("code = %q, want %q", inv.Code, code)
	}
	if inv.Uses != 5 {
		t.Errorf("uses = %d, want 5", inv.Uses)
	}
	if inv.Expires != int(expires) {
		t.Errorf("expires = %d, want %d", inv.Expires, expires)
	}
}

// Test invite validation query
func TestInviteValidation(t *testing.T) {
	cleanup := create_test_users_db(t)
	defer cleanup()

	db := db_open("db/users.db")

	// Valid invite
	valid_code := random_alphanumeric(16)
	db.exec("insert into invites (code, uses, expires) values (?, ?, ?)", valid_code, 1, now()+86400)

	// Expired invite
	expired_code := random_alphanumeric(16)
	db.exec("insert into invites (code, uses, expires) values (?, ?, ?)", expired_code, 1, now()-86400)

	// Zero uses invite
	zero_uses_code := random_alphanumeric(16)
	db.exec("insert into invites (code, uses, expires) values (?, ?, ?)", zero_uses_code, 0, now()+86400)

	// Check valid invite
	valid, _ := db.exists("select 1 from invites where code=? and uses > 0 and expires > ?", valid_code, now())
	if !valid {
		t.Error("valid invite should pass validation")
	}

	// Check expired invite
	valid, _ = db.exists("select 1 from invites where code=? and uses > 0 and expires > ?", expired_code, now())
	if valid {
		t.Error("expired invite should fail validation")
	}

	// Check zero uses invite
	valid, _ = db.exists("select 1 from invites where code=? and uses > 0 and expires > ?", zero_uses_code, now())
	if valid {
		t.Error("zero uses invite should fail validation")
	}

	// Check non-existent invite
	valid, _ = db.exists("select 1 from invites where code=? and uses > 0 and expires > ?", "nonexistent", now())
	if valid {
		t.Error("non-existent invite should fail validation")
	}
}

// Test user creation in database
func TestUserCreate(t *testing.T) {
	cleanup := create_test_users_db(t)
	defer cleanup()

	db := db_open("db/users.db")
	db.exec("insert into users (username, role) values (?, ?)", "new@example.com", "user")

	var u User
	if !db.scan(&u, "select id, username, role from users where username=?", "new@example.com") {
		t.Fatal("user should exist after insert")
	}

	if u.Username != "new@example.com" {
		t.Errorf("username = %q, want 'new@example.com'", u.Username)
	}
	if u.Role != "user" {
		t.Errorf("role = %q, want 'user'", u.Role)
	}
	if u.ID == 0 {
		t.Error("id should be non-zero after insert")
	}
}

// Test user update in database
func TestUserUpdate(t *testing.T) {
	cleanup := create_test_users_db(t)
	defer cleanup()

	db := db_open("db/users.db")
	db.exec("insert into users (username, role) values (?, ?)", "update@example.com", "user")

	var u User
	db.scan(&u, "select id, username, role from users where username=?", "update@example.com")

	// Update role
	db.exec("update users set role=? where id=?", "administrator", u.ID)

	var updated User
	db.scan(&updated, "select id, username, role from users where id=?", u.ID)

	if updated.Role != "administrator" {
		t.Errorf("role after update = %q, want 'administrator'", updated.Role)
	}
}

// Test user deletion
func TestUserDelete(t *testing.T) {
	cleanup := create_test_users_db(t)
	defer cleanup()

	db := db_open("db/users.db")
	db.exec("insert into users (username, role) values (?, ?)", "delete@example.com", "user")

	var u User
	db.scan(&u, "select id, username, role from users where username=?", "delete@example.com")

	// Delete user
	db.exec("delete from users where id=?", u.ID)

	exists, _ := db.exists("select 1 from users where id=?", u.ID)
	if exists {
		t.Error("user should not exist after delete")
	}
}

// Test invite deletion
func TestInviteDelete(t *testing.T) {
	cleanup := create_test_users_db(t)
	defer cleanup()

	db := db_open("db/users.db")
	code := random_alphanumeric(16)
	db.exec("insert into invites (code, uses, expires) values (?, ?, ?)", code, 1, now()+86400)

	// Verify it exists
	exists, _ := db.exists("select 1 from invites where code=?", code)
	if !exists {
		t.Fatal("invite should exist after insert")
	}

	// Delete invite
	db.exec("delete from invites where code=?", code)

	exists, _ = db.exists("select 1 from invites where code=?", code)
	if exists {
		t.Error("invite should not exist after delete")
	}
}

// Test user count query
func TestUserCount(t *testing.T) {
	cleanup := create_test_users_db(t)
	defer cleanup()

	db := db_open("db/users.db")

	// Initially empty
	row, err := db.row("select count(*) as count from users")
	if err != nil {
		t.Fatalf("count query failed: %v", err)
	}
	if row["count"].(int64) != 0 {
		t.Errorf("initial count = %d, want 0", row["count"])
	}

	// Add users
	db.exec("insert into users (username, role) values (?, ?)", "user1@example.com", "user")
	db.exec("insert into users (username, role) values (?, ?)", "user2@example.com", "user")
	db.exec("insert into users (username, role) values (?, ?)", "admin@example.com", "administrator")

	row, _ = db.row("select count(*) as count from users")
	if row["count"].(int64) != 3 {
		t.Errorf("count after inserts = %d, want 3", row["count"])
	}
}

// Test user list query with pagination
func TestUserList(t *testing.T) {
	cleanup := create_test_users_db(t)
	defer cleanup()

	db := db_open("db/users.db")

	// Add users
	for i := 1; i <= 10; i++ {
		db.exec("insert into users (username, role) values (?, ?)", "user"+itoa(i)+"@example.com", "user")
	}

	// Test limit
	rows, err := db.rows("select id, username, role from users order by id limit ? offset ?", 5, 0)
	if err != nil {
		t.Fatalf("list query failed: %v", err)
	}
	if len(rows) != 5 {
		t.Errorf("len(rows) with limit 5 = %d, want 5", len(rows))
	}

	// Test offset
	rows, _ = db.rows("select id, username, role from users order by id limit ? offset ?", 5, 5)
	if len(rows) != 5 {
		t.Errorf("len(rows) with offset 5 = %d, want 5", len(rows))
	}

	// Test offset beyond count
	rows, _ = db.rows("select id, username, role from users order by id limit ? offset ?", 5, 20)
	if len(rows) != 0 {
		t.Errorf("len(rows) with offset 20 = %d, want 0", len(rows))
	}
}

// Test invite list query filters expired
func TestInviteList(t *testing.T) {
	cleanup := create_test_users_db(t)
	defer cleanup()

	db := db_open("db/users.db")

	// Add valid invites
	db.exec("insert into invites (code, uses, expires) values (?, ?, ?)", "valid1", 1, now()+86400)
	db.exec("insert into invites (code, uses, expires) values (?, ?, ?)", "valid2", 5, now()+86400*7)

	// Add expired invite
	db.exec("insert into invites (code, uses, expires) values (?, ?, ?)", "expired", 1, now()-86400)

	// List should only show non-expired
	rows, err := db.rows("select code, uses, expires from invites where expires > ? order by expires", now())
	if err != nil {
		t.Fatalf("invite list query failed: %v", err)
	}
	if len(rows) != 2 {
		t.Errorf("len(rows) = %d, want 2 (non-expired only)", len(rows))
	}
}

// Test lookup by identity
func TestLookupByIdentity(t *testing.T) {
	cleanup := create_test_users_db(t)
	defer cleanup()

	db := db_open("db/users.db")
	db.exec("insert into users (id, username, role) values (1, 'test@example.com', 'user')")

	// Create entities table
	db.exec("create table entities (id text primary key, private text, fingerprint text, user integer, parent text default '', class text, name text, privacy text default 'public', data text default '', published integer default 0)")
	db.exec("insert into entities (id, private, fingerprint, user, class, name) values ('identity123', 'priv', 'abc123def456', 1, 'person', 'Test User')")

	// Lookup by identity
	row, err := db.row("select user from entities where id=? and class='person'", "identity123")
	if err != nil || row == nil {
		t.Fatal("should find entity by identity")
	}
	if row["user"].(int64) != 1 {
		t.Errorf("user = %d, want 1", row["user"])
	}

	// Lookup non-existent identity
	row, _ = db.row("select user from entities where id=? and class='person'", "nonexistent")
	if row != nil {
		t.Error("should not find non-existent identity")
	}
}

// Test lookup by fingerprint
func TestLookupByFingerprint(t *testing.T) {
	cleanup := create_test_users_db(t)
	defer cleanup()

	db := db_open("db/users.db")
	db.exec("insert into users (id, username, role) values (1, 'test@example.com', 'user')")

	// Create entities table
	db.exec("create table entities (id text primary key, private text, fingerprint text, user integer, parent text default '', class text, name text, privacy text default 'public', data text default '', published integer default 0)")
	db.exec("insert into entities (id, private, fingerprint, user, class, name) values ('identity123', 'priv', 'abc123def456', 1, 'person', 'Test User')")

	// Lookup by fingerprint (without hyphens)
	row, err := db.row("select user from entities where fingerprint=? and class='person'", "abc123def456")
	if err != nil || row == nil {
		t.Fatal("should find entity by fingerprint")
	}
	if row["user"].(int64) != 1 {
		t.Errorf("user = %d, want 1", row["user"])
	}

	// Lookup non-existent fingerprint
	row, _ = db.row("select user from entities where fingerprint=? and class='person'", "nonexistent")
	if row != nil {
		t.Error("should not find non-existent fingerprint")
	}
}

// Test fingerprint hyphen removal
func TestFingerprintHyphenRemoval(t *testing.T) {
	// Test that hyphens are properly removed
	fingerprint := "abc-123-def-456"
	cleaned := strings.ReplaceAll(fingerprint, "-", "")
	if cleaned != "abc123def456" {
		t.Errorf("cleaned = %q, want 'abc123def456'", cleaned)
	}
}

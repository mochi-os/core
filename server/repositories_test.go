// Mochi server: Repositories app tests
// Copyright Alistair Cunningham 2025

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ============ Repository Name Validation Tests ============

func TestRepositoryNameValidation(t *testing.T) {
	// Use "constant" validation which allows: alphanumeric, /, -, ., _
	tests := []struct {
		name    string
		input   string
		isValid bool
	}{
		// Valid names (alphanumeric, hyphen, underscore, dot)
		{"simple lowercase", "myrepo", true},
		{"with numbers", "repo123", true},
		{"with hyphens", "my-repo", true},
		{"with underscores", "my_repo", true},
		{"mixed case", "MyRepo", true},
		{"single char", "a", true},
		{"numbers only", "123", true},
		{"with dots", "my.repo", true}, // dots allowed in constant

		// Invalid names
		{"empty", "", false},
		{"with spaces", "my repo", false},
		{"special chars", "repo@home", false},
		{"unicode", "репо", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := valid(tc.input, "constant")
			if result != tc.isValid {
				t.Errorf("valid(%q, 'constant') = %v, want %v", tc.input, result, tc.isValid)
			}
		})
	}
}

// ============ Repository Database Tests ============

func create_repository_test_db(t *testing.T) (*DB, string, func()) {
	tmpDir, err := os.MkdirTemp("", "mochi_repo_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	origDataDir := data_dir
	data_dir = tmpDir

	// Create repositories database
	db := db_open("db/repositories.db")
	db.exec(`CREATE TABLE IF NOT EXISTS repositories (
		id TEXT PRIMARY KEY NOT NULL,
		name TEXT NOT NULL DEFAULT '',
		description TEXT NOT NULL DEFAULT '',
		default_branch TEXT NOT NULL DEFAULT 'main',
		size INTEGER NOT NULL DEFAULT 0,
		created TEXT NOT NULL DEFAULT '',
		updated TEXT NOT NULL DEFAULT ''
	)`)
	db.exec("CREATE INDEX IF NOT EXISTS repositories_name ON repositories(name)")

	cleanup := func() {
		db.close()
		data_dir = origDataDir
		os.RemoveAll(tmpDir)
	}

	return db, tmpDir, cleanup
}

func TestRepositoryCreate(t *testing.T) {
	db, _, cleanup := create_repository_test_db(t)
	defer cleanup()

	now := time.Now().Format("2006-01-02 15:04:05")
	repoID := "test-repo-id-12345"

	db.exec(`INSERT INTO repositories (id, name, description, default_branch, created, updated)
		VALUES (?, ?, ?, ?, ?, ?)`,
		repoID, "my-project", "A test repository", "main", now, now)

	// Verify creation
	row, err := db.row("SELECT * FROM repositories WHERE id = ?", repoID)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if row == nil {
		t.Fatal("Repository not found after creation")
	}

	if row["name"] != "my-project" {
		t.Errorf("name = %v, want 'my-project'", row["name"])
	}
	if row["description"] != "A test repository" {
		t.Errorf("description = %v, want 'A test repository'", row["description"])
	}
	if row["default_branch"] != "main" {
		t.Errorf("default_branch = %v, want 'main'", row["default_branch"])
	}
}

func TestRepositoryUpdate(t *testing.T) {
	db, _, cleanup := create_repository_test_db(t)
	defer cleanup()

	now := time.Now().Format("2006-01-02 15:04:05")
	repoID := "update-test-repo"

	db.exec(`INSERT INTO repositories (id, name, description, default_branch, created, updated)
		VALUES (?, ?, ?, ?, ?, ?)`,
		repoID, "original-name", "Original description", "main", now, now)

	// Update description
	db.exec("UPDATE repositories SET description = ?, updated = ? WHERE id = ?",
		"Updated description", now, repoID)

	row, _ := db.row("SELECT description FROM repositories WHERE id = ?", repoID)
	if row["description"] != "Updated description" {
		t.Errorf("description = %v, want 'Updated description'", row["description"])
	}
}

func TestRepositoryDelete(t *testing.T) {
	db, _, cleanup := create_repository_test_db(t)
	defer cleanup()

	now := time.Now().Format("2006-01-02 15:04:05")
	repoID := "delete-test-repo"

	db.exec(`INSERT INTO repositories (id, name, description, default_branch, created, updated)
		VALUES (?, ?, ?, ?, ?, ?)`,
		repoID, "to-delete", "", "main", now, now)

	// Verify exists
	exists, _ := db.exists("SELECT 1 FROM repositories WHERE id = ?", repoID)
	if !exists {
		t.Fatal("Repository should exist before delete")
	}

	// Delete
	db.exec("DELETE FROM repositories WHERE id = ?", repoID)

	// Verify deleted
	exists, _ = db.exists("SELECT 1 FROM repositories WHERE id = ?", repoID)
	if exists {
		t.Error("Repository should not exist after delete")
	}
}

func TestRepositoryListByName(t *testing.T) {
	db, _, cleanup := create_repository_test_db(t)
	defer cleanup()

	now := time.Now().Format("2006-01-02 15:04:05")

	// Create multiple repositories
	repos := []struct {
		id, name string
	}{
		{"id-1", "alpha-repo"},
		{"id-2", "beta-repo"},
		{"id-3", "gamma-repo"},
	}

	for _, r := range repos {
		db.exec(`INSERT INTO repositories (id, name, created, updated) VALUES (?, ?, ?, ?)`,
			r.id, r.name, now, now)
	}

	// Query by name
	row, _ := db.row("SELECT id FROM repositories WHERE name = ?", "beta-repo")
	if row == nil || row["id"] != "id-2" {
		t.Errorf("Failed to find repository by name")
	}

	// Count all
	countRow, _ := db.row("SELECT COUNT(*) as count FROM repositories")
	count := countRow["count"].(int64)
	if count != 3 {
		t.Errorf("count = %d, want 3", count)
	}
}

func TestRepositoryDefaultBranch(t *testing.T) {
	db, _, cleanup := create_repository_test_db(t)
	defer cleanup()

	now := time.Now().Format("2006-01-02 15:04:05")
	repoID := "branch-test-repo"

	// Create with default branch
	db.exec(`INSERT INTO repositories (id, name, default_branch, created, updated)
		VALUES (?, ?, ?, ?, ?)`,
		repoID, "test-repo", "main", now, now)

	row, _ := db.row("SELECT default_branch FROM repositories WHERE id = ?", repoID)
	if row["default_branch"] != "main" {
		t.Errorf("default_branch = %v, want 'main'", row["default_branch"])
	}

	// Change default branch
	db.exec("UPDATE repositories SET default_branch = ? WHERE id = ?", "develop", repoID)

	row, _ = db.row("SELECT default_branch FROM repositories WHERE id = ?", repoID)
	if row["default_branch"] != "develop" {
		t.Errorf("default_branch = %v, want 'develop'", row["default_branch"])
	}
}

// ============ Repository + Git Integration Tests ============

func create_repository_git_test_env(t *testing.T) (*DB, *User, string, func()) {
	tmpDir, err := os.MkdirTemp("", "mochi_repo_git_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	origDataDir := data_dir
	data_dir = tmpDir

	user := &User{ID: 1}

	// Create user directory
	userDir := filepath.Join(tmpDir, "users", "1", "repositories")
	if err := os.MkdirAll(userDir, 0755); err != nil {
		t.Fatalf("Failed to create user dir: %v", err)
	}

	// Create repositories database
	db := db_open("db/repositories.db")
	db.exec(`CREATE TABLE IF NOT EXISTS repositories (
		id TEXT PRIMARY KEY NOT NULL,
		name TEXT NOT NULL DEFAULT '',
		description TEXT NOT NULL DEFAULT '',
		default_branch TEXT NOT NULL DEFAULT 'main',
		size INTEGER NOT NULL DEFAULT 0,
		created TEXT NOT NULL DEFAULT '',
		updated TEXT NOT NULL DEFAULT ''
	)`)

	cleanup := func() {
		db.close()
		data_dir = origDataDir
		os.RemoveAll(tmpDir)
	}

	return db, user, tmpDir, cleanup
}

func TestRepositoryWithGitInit(t *testing.T) {
	db, user, tmpDir, cleanup := create_repository_git_test_env(t)
	defer cleanup()

	now := time.Now().Format("2006-01-02 15:04:05")
	repoID := "git-test-repo"

	// Create database record
	db.exec(`INSERT INTO repositories (id, name, description, created, updated)
		VALUES (?, ?, ?, ?, ?)`,
		repoID, "git-project", "A git repository", now, now)

	// Initialize git repository
	err := git_init(user, repoID)
	if err != nil {
		t.Fatalf("git_init failed: %v", err)
	}

	// Verify git directory exists
	gitPath := filepath.Join(tmpDir, "users", "1", "repositories", repoID)
	if _, err := os.Stat(gitPath); os.IsNotExist(err) {
		t.Errorf("Git repository directory not created at %s", gitPath)
	}

	// Verify it's a bare repository
	headPath := filepath.Join(gitPath, "HEAD")
	if _, err := os.Stat(headPath); os.IsNotExist(err) {
		t.Error("Not a valid git repository (no HEAD file)")
	}
}

func TestRepositoryGitDelete(t *testing.T) {
	db, user, tmpDir, cleanup := create_repository_git_test_env(t)
	defer cleanup()

	now := time.Now().Format("2006-01-02 15:04:05")
	repoID := "delete-git-repo"

	// Create database record and git repo
	db.exec(`INSERT INTO repositories (id, name, created, updated) VALUES (?, ?, ?, ?)`,
		repoID, "to-delete-git", now, now)
	git_init(user, repoID)

	gitPath := filepath.Join(tmpDir, "users", "1", "repositories", repoID)

	// Verify exists
	if _, err := os.Stat(gitPath); os.IsNotExist(err) {
		t.Fatal("Git repo should exist before delete")
	}

	// Delete git repo
	err := git_delete(user, repoID)
	if err != nil {
		t.Errorf("git_delete failed: %v", err)
	}

	// Verify deleted
	if _, err := os.Stat(gitPath); !os.IsNotExist(err) {
		t.Error("Git repo should not exist after delete")
	}

	// Delete database record
	db.exec("DELETE FROM repositories WHERE id = ?", repoID)
}

func TestRepositoryGitSize(t *testing.T) {
	_, user, _, cleanup := create_repository_git_test_env(t)
	defer cleanup()

	repoID := "size-test-repo"

	// Initialize
	err := git_init(user, repoID)
	if err != nil {
		t.Fatalf("git_init failed: %v", err)
	}

	// Get size (should be small for empty repo)
	size, err := git_size(user, repoID)
	if err != nil {
		t.Fatalf("git_size failed: %v", err)
	}
	if size < 0 {
		t.Errorf("git_size returned negative: %d", size)
	}

	// Size should be reasonable for a bare repo (typically 20-100KB)
	if size > 1024*1024 {
		t.Errorf("git_size unexpectedly large for empty repo: %d", size)
	}
}

// ============ Repository Path Tests ============

func TestRepositoryPathGeneration(t *testing.T) {
	origDataDir := data_dir
	data_dir = "/var/lib/mochi"
	defer func() { data_dir = origDataDir }()

	tests := []struct {
		userID   int
		repoID   string
		expected string
	}{
		{1, "repo-abc", "/var/lib/mochi/users/1/repositories/repo-abc"},
		{42, "my-project", "/var/lib/mochi/users/42/repositories/my-project"},
		{100, "test", "/var/lib/mochi/users/100/repositories/test"},
	}

	for _, tc := range tests {
		user := &User{ID: tc.userID}
		path := git_repo_path(user, tc.repoID)
		if path != tc.expected {
			t.Errorf("git_repo_path(user %d, %q) = %q, want %q",
				tc.userID, tc.repoID, path, tc.expected)
		}
	}
}

func TestRepositoryPathIsolation(t *testing.T) {
	origDataDir := data_dir
	data_dir = "/var/lib/mochi"
	defer func() { data_dir = origDataDir }()

	user1 := &User{ID: 1}
	user2 := &User{ID: 2}

	// Different users should have different paths
	path1 := git_repo_path(user1, "shared-name")
	path2 := git_repo_path(user2, "shared-name")

	if path1 == path2 {
		t.Error("Different users should have different repository paths")
	}

	if !strings.Contains(path1, "/users/1/") {
		t.Errorf("User 1 path should contain /users/1/, got: %s", path1)
	}
	if !strings.Contains(path2, "/users/2/") {
		t.Errorf("User 2 path should contain /users/2/, got: %s", path2)
	}
}

// ============ Repository Metadata Tests ============

func TestRepositoryTimestamps(t *testing.T) {
	db, _, cleanup := create_repository_test_db(t)
	defer cleanup()

	repoID := "timestamp-test"
	created := "2025-01-01 10:00:00"
	updated := "2025-01-01 10:00:00"

	db.exec(`INSERT INTO repositories (id, name, created, updated) VALUES (?, ?, ?, ?)`,
		repoID, "test", created, updated)

	// Update and check timestamp changes
	newUpdated := "2025-01-02 15:30:00"
	db.exec("UPDATE repositories SET description = 'updated', updated = ? WHERE id = ?",
		newUpdated, repoID)

	row, _ := db.row("SELECT created, updated FROM repositories WHERE id = ?", repoID)
	if row["created"] != created {
		t.Errorf("created changed unexpectedly: %v", row["created"])
	}
	if row["updated"] != newUpdated {
		t.Errorf("updated = %v, want %v", row["updated"], newUpdated)
	}
}

func TestRepositorySizeTracking(t *testing.T) {
	db, _, cleanup := create_repository_test_db(t)
	defer cleanup()

	now := time.Now().Format("2006-01-02 15:04:05")
	repoID := "size-tracking-test"

	db.exec(`INSERT INTO repositories (id, name, size, created, updated) VALUES (?, ?, ?, ?, ?)`,
		repoID, "test", 0, now, now)

	// Update size
	newSize := int64(1024 * 1024 * 10) // 10MB
	db.exec("UPDATE repositories SET size = ? WHERE id = ?", newSize, repoID)

	row, _ := db.row("SELECT size FROM repositories WHERE id = ?", repoID)
	if row["size"].(int64) != newSize {
		t.Errorf("size = %v, want %d", row["size"], newSize)
	}
}

// ============ Repository Query Tests ============

func TestRepositoryQueryByOwner(t *testing.T) {
	db, _, cleanup := create_repository_test_db(t)
	defer cleanup()

	// Note: In the actual schema, ownership is handled via entities table
	// This test demonstrates the query pattern that would be used
	now := time.Now().Format("2006-01-02 15:04:05")

	// Create repositories with different "owner" prefixes in name for simulation
	db.exec(`INSERT INTO repositories (id, name, created, updated) VALUES (?, ?, ?, ?)`,
		"user1-repo1", "user1-project-a", now, now)
	db.exec(`INSERT INTO repositories (id, name, created, updated) VALUES (?, ?, ?, ?)`,
		"user1-repo2", "user1-project-b", now, now)
	db.exec(`INSERT INTO repositories (id, name, created, updated) VALUES (?, ?, ?, ?)`,
		"user2-repo1", "user2-project-a", now, now)

	// Query repositories starting with user1
	rows, _ := db.rows("SELECT id FROM repositories WHERE id LIKE ?", "user1-%")
	if len(rows) != 2 {
		t.Errorf("Expected 2 repos for user1, got %d", len(rows))
	}
}

func TestRepositorySearchByName(t *testing.T) {
	db, _, cleanup := create_repository_test_db(t)
	defer cleanup()

	now := time.Now().Format("2006-01-02 15:04:05")

	// Create repositories with various names
	names := []string{"awesome-project", "another-awesome", "boring-repo", "cool-stuff"}
	for i, name := range names {
		db.exec(`INSERT INTO repositories (id, name, created, updated) VALUES (?, ?, ?, ?)`,
			"id-"+name, name, now, now)
		_ = i
	}

	// Search for "awesome"
	rows, _ := db.rows("SELECT name FROM repositories WHERE name LIKE ?", "%awesome%")
	if len(rows) != 2 {
		t.Errorf("Expected 2 repos matching 'awesome', got %d", len(rows))
	}
}

// ============ Repository Edge Cases ============

func TestRepositoryEmptyDescription(t *testing.T) {
	db, _, cleanup := create_repository_test_db(t)
	defer cleanup()

	now := time.Now().Format("2006-01-02 15:04:05")
	repoID := "no-desc-repo"

	// Create with empty description (default)
	db.exec(`INSERT INTO repositories (id, name, created, updated) VALUES (?, ?, ?, ?)`,
		repoID, "no-description", now, now)

	row, _ := db.row("SELECT description FROM repositories WHERE id = ?", repoID)
	if row["description"] != "" {
		t.Errorf("description should be empty, got %v", row["description"])
	}
}

func TestRepositoryLongDescription(t *testing.T) {
	db, _, cleanup := create_repository_test_db(t)
	defer cleanup()

	now := time.Now().Format("2006-01-02 15:04:05")
	repoID := "long-desc-repo"
	longDesc := strings.Repeat("This is a very long description. ", 100)

	db.exec(`INSERT INTO repositories (id, name, description, created, updated) VALUES (?, ?, ?, ?, ?)`,
		repoID, "long-description", longDesc, now, now)

	row, _ := db.row("SELECT description FROM repositories WHERE id = ?", repoID)
	if row["description"] != longDesc {
		t.Error("Long description was truncated or corrupted")
	}
}

func TestRepositorySpecialCharactersInDescription(t *testing.T) {
	db, _, cleanup := create_repository_test_db(t)
	defer cleanup()

	now := time.Now().Format("2006-01-02 15:04:05")
	repoID := "special-desc-repo"
	specialDesc := "Description with 'quotes', \"double quotes\", and special chars: <>&\n\ttabs and newlines"

	db.exec(`INSERT INTO repositories (id, name, description, created, updated) VALUES (?, ?, ?, ?, ?)`,
		repoID, "special-chars", specialDesc, now, now)

	row, _ := db.row("SELECT description FROM repositories WHERE id = ?", repoID)
	if row["description"] != specialDesc {
		t.Errorf("Special characters not preserved: got %v", row["description"])
	}
}

func TestRepositoryUniqueID(t *testing.T) {
	db, _, cleanup := create_repository_test_db(t)
	defer cleanup()

	now := time.Now().Format("2006-01-02 15:04:05")
	repoID := "unique-id-test"

	// First insert should succeed
	db.exec(`INSERT INTO repositories (id, name, created, updated) VALUES (?, ?, ?, ?)`,
		repoID, "first", now, now)

	// Second insert with same ID should fail (PRIMARY KEY constraint)
	// We test this by checking that only one row exists
	db.exec(`INSERT OR IGNORE INTO repositories (id, name, created, updated) VALUES (?, ?, ?, ?)`,
		repoID, "second", now, now)

	row, _ := db.row("SELECT name FROM repositories WHERE id = ?", repoID)
	if row["name"] != "first" {
		t.Error("Duplicate ID should not overwrite existing record")
	}
}

// ============ Repository Index Tests ============

func TestRepositoryNameIndex(t *testing.T) {
	db, _, cleanup := create_repository_test_db(t)
	defer cleanup()

	now := time.Now().Format("2006-01-02 15:04:05")

	// Create many repositories
	for i := 0; i < 100; i++ {
		name := "repo-" + string(rune('a'+i%26)) + string(rune('0'+i/26))
		db.exec(`INSERT INTO repositories (id, name, created, updated) VALUES (?, ?, ?, ?)`,
			"id-"+name, name, now, now)
	}

	// Query by name should be efficient (index exists)
	row, _ := db.row("SELECT id FROM repositories WHERE name = ?", "repo-a0")
	if row == nil {
		t.Error("Failed to find repository by indexed name")
	}
}

// ============ Repository + Database + Git Combined Tests ============

func TestRepositoryFullLifecycle(t *testing.T) {
	db, user, _, cleanup := create_repository_git_test_env(t)
	defer cleanup()

	now := time.Now().Format("2006-01-02 15:04:05")
	repoID := "lifecycle-test-repo"
	repoName := "my-lifecycle-project"

	// 1. Create database record
	db.exec(`INSERT INTO repositories (id, name, description, created, updated)
		VALUES (?, ?, ?, ?, ?)`,
		repoID, repoName, "Testing full lifecycle", now, now)

	// 2. Initialize git repository
	err := git_init(user, repoID)
	if err != nil {
		t.Fatalf("git_init failed: %v", err)
	}

	// 3. Verify both exist
	row, _ := db.row("SELECT * FROM repositories WHERE id = ?", repoID)
	if row == nil {
		t.Fatal("Database record not found")
	}

	repo, err := git_open(user, repoID)
	if err != nil {
		t.Fatalf("git_open failed: %v", err)
	}
	if repo == nil {
		t.Fatal("Git repository not found")
	}

	// 4. Update metadata
	db.exec("UPDATE repositories SET description = ? WHERE id = ?", "Updated description", repoID)

	// 5. Get size and update
	size, _ := git_size(user, repoID)
	db.exec("UPDATE repositories SET size = ? WHERE id = ?", size, repoID)

	// 6. Verify updates
	row, _ = db.row("SELECT description, size FROM repositories WHERE id = ?", repoID)
	if row["description"] != "Updated description" {
		t.Error("Description not updated")
	}

	// 7. Delete everything
	git_delete(user, repoID)
	db.exec("DELETE FROM repositories WHERE id = ?", repoID)

	// 8. Verify deletion
	exists, _ := db.exists("SELECT 1 FROM repositories WHERE id = ?", repoID)
	if exists {
		t.Error("Database record should be deleted")
	}
}

func TestRepositoryMultipleUsers(t *testing.T) {
	_, _, cleanup := create_repository_test_db(t)
	defer cleanup()

	user1 := &User{ID: 1}
	user2 := &User{ID: 2}

	// Create user directories
	os.MkdirAll(filepath.Join(data_dir, "users", "1", "repositories"), 0755)
	os.MkdirAll(filepath.Join(data_dir, "users", "2", "repositories"), 0755)

	// Each user creates a repo with the same name
	repoName := "same-name-repo"

	err := git_init(user1, repoName)
	if err != nil {
		t.Fatalf("git_init for user1 failed: %v", err)
	}

	err = git_init(user2, repoName)
	if err != nil {
		t.Fatalf("git_init for user2 failed: %v", err)
	}

	// Both should exist independently
	repo1, _ := git_open(user1, repoName)
	repo2, _ := git_open(user2, repoName)

	if repo1 == nil || repo2 == nil {
		t.Error("Both users should have their own repository")
	}

	// Paths should be different
	path1 := git_repo_path(user1, repoName)
	path2 := git_repo_path(user2, repoName)

	if path1 == path2 {
		t.Error("Repository paths should be different for different users")
	}
}

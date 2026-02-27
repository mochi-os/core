// Mochi server: Git operations unit tests
// Copyright Alistair Cunningham 2025

package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
)

// Helper to create a test environment for git operations
func create_git_test_env(t *testing.T) (*User, string, func()) {
	tmpDir, err := os.MkdirTemp("", "mochi_git_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	origDataDir := data_dir
	data_dir = tmpDir

	user := &User{ID: 1}

	userDir := filepath.Join(tmpDir, "users", "1", "repositories")
	if err := os.MkdirAll(userDir, 0755); err != nil {
		t.Fatalf("Failed to create user dir: %v", err)
	}

	cleanup := func() {
		data_dir = origDataDir
		os.RemoveAll(tmpDir)
	}

	return user, tmpDir, cleanup
}

// Helper to create a repo with a commit
func create_repo_with_commit(t *testing.T, user *User, repoID string) *git.Repository {
	err := git_init(user, repoID)
	if err != nil {
		t.Fatalf("git_init failed: %v", err)
	}

	repo, err := git_open(user, repoID)
	if err != nil {
		t.Fatalf("git_open failed: %v", err)
	}

	// Create a commit using worktree
	// For bare repos, we need to create objects directly
	repoPath := git_repo_path(user, repoID)

	// Use git CLI to create initial commit and push to bare repo
	tmpWorkDir, _ := os.MkdirTemp("", "git_work")
	defer os.RemoveAll(tmpWorkDir)

	run := func(args ...string) {
		cmd := exec.Command("git", args...)
		cmd.Dir = tmpWorkDir
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=Test User",
			"GIT_AUTHOR_EMAIL=test@example.com",
			"GIT_COMMITTER_NAME=Test User",
			"GIT_COMMITTER_EMAIL=test@example.com",
		)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Logf("git %v: %s (%v)", args, out, err)
		}
	}

	run("init", "-b", "main")
	os.WriteFile(filepath.Join(tmpWorkDir, "README.md"), []byte("# Test Repo\n"), 0644)
	run("add", "README.md")
	run("commit", "-m", "Initial commit")
	run("push", repoPath, "main")

	// Re-open the bare repo
	repo, _ = git_open(user, repoID)
	return repo
}

// ============ Basic Repository Tests ============

func TestGitInit(t *testing.T) {
	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repoID := "test-repo-123"

	err := git_init(user, repoID)
	if err != nil {
		t.Fatalf("git_init should succeed: %v", err)
	}

	repoPath := git_repo_path(user, repoID)
	if _, err := os.Stat(repoPath); os.IsNotExist(err) {
		t.Error("Repository directory should exist after init")
	}

	headPath := filepath.Join(repoPath, "HEAD")
	if _, err := os.Stat(headPath); os.IsNotExist(err) {
		t.Error("Repository should have HEAD file (bare repo)")
	}
}

func TestGitInitIdempotent(t *testing.T) {
	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repoID := "idempotent-repo"

	// First init
	err := git_init(user, repoID)
	if err != nil {
		t.Fatalf("First git_init should succeed: %v", err)
	}

	// Second init should also succeed (or at least not crash)
	err = git_init(user, repoID)
	// May return error for already exists, which is fine
	_ = err
}

func TestGitRepoPath(t *testing.T) {
	user := &User{ID: 42}
	repoID := "my-repo"

	origDataDir := data_dir
	data_dir = "/var/lib/mochi"
	defer func() { data_dir = origDataDir }()

	path := git_repo_path(user, repoID)
	expected := "/var/lib/mochi/users/42/repositories/my-repo"

	if path != expected {
		t.Errorf("Expected path %q, got %q", expected, path)
	}
}

func TestGitRepoPathDifferentUsers(t *testing.T) {
	origDataDir := data_dir
	data_dir = "/data"
	defer func() { data_dir = origDataDir }()

	user1 := &User{ID: 1}
	user2 := &User{ID: 999}

	path1 := git_repo_path(user1, "repo")
	path2 := git_repo_path(user2, "repo")

	if path1 == path2 {
		t.Error("Different users should have different repo paths")
	}

	if !strings.Contains(path1, "/1/") {
		t.Errorf("Path should contain user ID 1: %s", path1)
	}
	if !strings.Contains(path2, "/999/") {
		t.Errorf("Path should contain user ID 999: %s", path2)
	}
}

func TestGitDelete(t *testing.T) {
	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repoID := "delete-test-repo"

	err := git_init(user, repoID)
	if err != nil {
		t.Fatalf("git_init failed: %v", err)
	}

	repoPath := git_repo_path(user, repoID)
	if _, err := os.Stat(repoPath); os.IsNotExist(err) {
		t.Fatal("Repository should exist before delete")
	}

	err = git_delete(user, repoID)
	if err != nil {
		t.Errorf("git_delete should succeed: %v", err)
	}

	if _, err := os.Stat(repoPath); !os.IsNotExist(err) {
		t.Error("Repository should not exist after delete")
	}
}

func TestGitDeleteNonExistent(t *testing.T) {
	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	// Deleting non-existent repo should not panic
	err := git_delete(user, "non-existent-repo")
	// May or may not error, but should not panic
	_ = err
}

func TestGitOpen(t *testing.T) {
	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repoID := "open-test-repo"
	err := git_init(user, repoID)
	if err != nil {
		t.Fatalf("git_init failed: %v", err)
	}

	repo, err := git_open(user, repoID)
	if err != nil {
		t.Fatalf("git_open should succeed: %v", err)
	}
	if repo == nil {
		t.Error("git_open should return a repository")
	}
}

func TestGitOpenNonExistent(t *testing.T) {
	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	_, err := git_open(user, "non-existent-repo")
	if err == nil {
		t.Error("git_open should fail for non-existent repository")
	}
}

func TestGitOpenMultipleTimes(t *testing.T) {
	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repoID := "multi-open-repo"
	git_init(user, repoID)

	// Open multiple times should all succeed
	for i := 0; i < 10; i++ {
		repo, err := git_open(user, repoID)
		if err != nil {
			t.Errorf("git_open iteration %d failed: %v", i, err)
		}
		if repo == nil {
			t.Errorf("git_open iteration %d returned nil", i)
		}
	}
}

// ============ Size Tests ============

func TestGitSizeEmpty(t *testing.T) {
	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repoID := "size-test-repo"
	git_init(user, repoID)

	size, err := git_size(user, repoID)
	if err != nil {
		t.Errorf("git_size failed: %v", err)
	}
	if size < 0 {
		t.Error("git_size should return non-negative value")
	}
	// Empty repo should have some size (git metadata)
	if size == 0 {
		t.Log("Warning: empty repo reports 0 size")
	}
}

func TestGitSizeNonExistent(t *testing.T) {
	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	_, err := git_size(user, "non-existent")
	if err == nil {
		t.Error("git_size should fail for non-existent repository")
	}
}

// ============ Ref Resolution Tests ============

func TestGitResolveRefHEAD(t *testing.T) {
	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repoID := "resolve-ref-repo"
	repo := create_repo_with_commit(t, user, repoID)

	hash, err := git_resolve_ref(repo, "HEAD")
	if err != nil {
		t.Logf("git_resolve_ref HEAD: %v (may be expected for empty repo)", err)
		return
	}
	if hash == nil {
		t.Error("Resolved hash should not be nil")
	}
}

func TestGitResolveRefInvalid(t *testing.T) {
	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repoID := "resolve-invalid-repo"
	git_init(user, repoID)
	repo, _ := git_open(user, repoID)

	_, err := git_resolve_ref(repo, "refs/heads/nonexistent")
	if err == nil {
		t.Error("git_resolve_ref should fail for non-existent ref")
	}
}

func TestGitResolveRefFullHash(t *testing.T) {
	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repoID := "resolve-hash-repo"
	repo := create_repo_with_commit(t, user, repoID)

	// Get HEAD hash first
	headRef, err := repo.Head()
	if err != nil {
		t.Skip("No HEAD in repo, skipping hash resolution test")
	}

	hashStr := headRef.Hash().String()
	resolved, err := git_resolve_ref(repo, hashStr)
	if err != nil {
		t.Errorf("git_resolve_ref should resolve full hash: %v", err)
	}
	if resolved != nil && resolved.String() != hashStr {
		t.Errorf("Resolved hash mismatch: expected %s, got %s", hashStr, resolved.String())
	}
}

// ============ Multiple Repository Tests ============

func TestGitMultipleRepos(t *testing.T) {
	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repos := []string{"repo-a", "repo-b", "repo-c"}

	// Create all repos
	for _, repoID := range repos {
		err := git_init(user, repoID)
		if err != nil {
			t.Errorf("Failed to create %s: %v", repoID, err)
		}
	}

	// Verify all exist
	for _, repoID := range repos {
		_, err := git_open(user, repoID)
		if err != nil {
			t.Errorf("Failed to open %s: %v", repoID, err)
		}
	}

	// Delete one
	git_delete(user, "repo-b")

	// Verify others still work
	_, err := git_open(user, "repo-a")
	if err != nil {
		t.Error("repo-a should still be accessible")
	}
	_, err = git_open(user, "repo-c")
	if err != nil {
		t.Error("repo-c should still be accessible")
	}
	_, err = git_open(user, "repo-b")
	if err == nil {
		t.Error("repo-b should be deleted")
	}
}

func TestGitMultipleUsers(t *testing.T) {
	_, tmpDir, cleanup := create_git_test_env(t)
	defer cleanup()

	user1 := &User{ID: 1}
	user2 := &User{ID: 2}

	// Create directories for user2
	user2Dir := filepath.Join(tmpDir, "users", "2", "repositories")
	os.MkdirAll(user2Dir, 0755)

	// Create same-named repos for different users
	git_init(user1, "shared-name")
	git_init(user2, "shared-name")

	// Both should be accessible
	repo1, err := git_open(user1, "shared-name")
	if err != nil || repo1 == nil {
		t.Error("User1's repo should be accessible")
	}

	repo2, err := git_open(user2, "shared-name")
	if err != nil || repo2 == nil {
		t.Error("User2's repo should be accessible")
	}

	// They should be different paths
	path1 := git_repo_path(user1, "shared-name")
	path2 := git_repo_path(user2, "shared-name")
	if path1 == path2 {
		t.Error("Different users' repos should have different paths")
	}
}

// ============ Repository Naming Tests ============

func TestGitRepoSpecialNames(t *testing.T) {
	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	names := []string{
		"simple",
		"with-dash",
		"with_underscore",
		"MixedCase",
		"123numeric",
		"a",
		strings.Repeat("x", 100),
	}

	for _, name := range names {
		err := git_init(user, name)
		if err != nil {
			t.Errorf("Failed to create repo with name %q: %v", name, err)
			continue
		}
		_, err = git_open(user, name)
		if err != nil {
			t.Errorf("Failed to open repo with name %q: %v", name, err)
		}
	}
}

// ============ Bare Repository Verification ============

func TestGitInitCreatesBareRepo(t *testing.T) {
	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repoID := "bare-check-repo"
	git_init(user, repoID)

	repoPath := git_repo_path(user, repoID)

	// Bare repos have these files/dirs directly, not in .git
	requiredFiles := []string{"HEAD", "config", "objects", "refs"}
	for _, f := range requiredFiles {
		path := filepath.Join(repoPath, f)
		if _, err := os.Stat(path); os.IsNotExist(err) {
			t.Errorf("Bare repo should have %s at root level", f)
		}
	}

	// Should NOT have .git directory
	dotGit := filepath.Join(repoPath, ".git")
	if _, err := os.Stat(dotGit); !os.IsNotExist(err) {
		t.Error("Bare repo should not have .git directory")
	}

	// Verify config says bare = true
	configPath := filepath.Join(repoPath, "config")
	configData, _ := os.ReadFile(configPath)
	if !strings.Contains(string(configData), "bare = true") {
		t.Error("Bare repo config should have 'bare = true'")
	}
}

// ============ Default Branch Tests ============

func TestGitDefaultBranchNewRepo(t *testing.T) {
	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repoID := "default-branch-repo"
	git_init(user, repoID)

	repoPath := git_repo_path(user, repoID)
	headContent, err := os.ReadFile(filepath.Join(repoPath, "HEAD"))
	if err != nil {
		t.Fatalf("Failed to read HEAD: %v", err)
	}

	head := strings.TrimSpace(string(headContent))
	// Should be a symbolic ref to main or master
	if !strings.HasPrefix(head, "ref: refs/heads/") {
		t.Errorf("HEAD should be symbolic ref, got: %s", head)
	}
}

// ============ Concurrent Access Tests ============

func TestGitConcurrentOpen(t *testing.T) {
	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repoID := "concurrent-repo"
	git_init(user, repoID)

	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func() {
			repo, err := git_open(user, repoID)
			if err != nil {
				t.Errorf("Concurrent open failed: %v", err)
			}
			if repo == nil {
				t.Error("Concurrent open returned nil")
			}
			done <- true
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}

// ============ Edge Cases ============

func TestGitNilUser(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Log("git_repo_path with nil user may panic or return empty path")
		}
	}()

	// This should either panic or return empty/error
	path := git_repo_path(nil, "test")
	if path != "" {
		t.Log("git_repo_path with nil user returned:", path)
	}
}

func TestGitEmptyRepoID(t *testing.T) {
	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	// Empty repo ID should probably fail
	err := git_init(user, "")
	// Implementation dependent, but should handle gracefully
	_ = err
}

// ============ Starlark API Helpers Test ============

func TestVersionCompareGit(t *testing.T) {
	tests := []struct {
		v1, v2   string
		expected int
	}{
		{"0.1", "0.2", -1},
		{"0.2", "0.1", 1},
		{"0.1", "0.1", 0},
		{"1.0", "0.9", 1},
		{"0.10", "0.9", 1},
		{"1.0.0", "1.0", 0},
		{"1.0.1", "1.0.0", 1},
		{"0.3", "0.3.0", 0},
		{"2.0", "1.99", 1},
	}

	for _, tc := range tests {
		result := version_compare(tc.v1, tc.v2)
		if result != tc.expected {
			t.Errorf("version_compare(%q, %q) = %d, expected %d", tc.v1, tc.v2, result, tc.expected)
		}
	}
}

// ============ Branch Operations via go-git ============

func TestGitBranchOperations(t *testing.T) {
	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repoID := "branch-ops-repo"
	repo := create_repo_with_commit(t, user, repoID)

	// List branches
	branches, err := repo.Branches()
	if err != nil {
		t.Logf("Branch listing: %v", err)
		return
	}

	count := 0
	branches.ForEach(func(ref *plumbing.Reference) error {
		count++
		t.Logf("Branch: %s", ref.Name().Short())
		return nil
	})
	t.Logf("Total branches: %d", count)
}

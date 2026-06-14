// Mochi server: Git operations unit tests
// Copyright Alistair Cunningham 2025-2026

package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	sl "go.starlark.net/starlark"
)

// test_app is the per-app context the git_* helpers expect. Tests use
// the literal "repositories" id so the expected on-disk paths
// (users/<uid>/repositories/<repo>) match what the assertions check.
var test_app = &App{id: "repositories"}

// Helper to create a test environment for git operations
func create_git_test_env(t *testing.T) (*User, string, func()) {
	tmp_dir, err := os.MkdirTemp("", "mochi_git_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	orig_data_dir := data_dir
	data_dir = tmp_dir

	user := &User{UID: "u1"}

	user_dir := filepath.Join(tmp_dir, "users", "1", "repositories")
	if err := os.MkdirAll(user_dir, 0755); err != nil {
		t.Fatalf("Failed to create user dir: %v", err)
	}

	cleanup := func() {
		data_dir = orig_data_dir
		os.RemoveAll(tmp_dir)
	}

	return user, tmp_dir, cleanup
}

// Helper to create a repo with a commit
func create_repo_with_commit(t *testing.T, user *User, repo_id string) *git.Repository {
	err := git_init(user, test_app, repo_id)
	if err != nil {
		t.Fatalf("git_init failed: %v", err)
	}

	repo, err := git_open(user, test_app, repo_id)
	if err != nil {
		t.Fatalf("git_open failed: %v", err)
	}

	// Create a commit using worktree
	// For bare repos, we need to create objects directly
	repo_path := git_repo_path(user, test_app, repo_id)

	// Use git CLI to create initial commit and push to bare repo
	tmp_work_dir, _ := os.MkdirTemp("", "git_work")
	defer os.RemoveAll(tmp_work_dir)

	run := func(args ...string) {
		cmd := exec.Command("git", args...)
		cmd.Dir = tmp_work_dir
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
	os.WriteFile(filepath.Join(tmp_work_dir, "README.md"), []byte("# Test Repo\n"), 0644)
	run("add", "README.md")
	run("commit", "-m", "Initial commit")
	run("push", repo_path, "main")

	// Re-open the bare repo
	repo, _ = git_open(user, test_app, repo_id)
	return repo
}

// ============ Basic Repository Tests ============

func TestGitInit(t *testing.T) {
	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repo_id := "test-repo-123"

	err := git_init(user, test_app, repo_id)
	if err != nil {
		t.Fatalf("git_init should succeed: %v", err)
	}

	repo_path := git_repo_path(user, test_app, repo_id)
	if _, err := os.Stat(repo_path); os.IsNotExist(err) {
		t.Error("Repository directory should exist after init")
	}

	head_path := filepath.Join(repo_path, "HEAD")
	if _, err := os.Stat(head_path); os.IsNotExist(err) {
		t.Error("Repository should have HEAD file (bare repo)")
	}
}

func TestGitInitIdempotent(t *testing.T) {
	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repo_id := "idempotent-repo"

	// First init
	err := git_init(user, test_app, repo_id)
	if err != nil {
		t.Fatalf("First git_init should succeed: %v", err)
	}

	// Second init should also succeed (or at least not crash)
	err = git_init(user, test_app, repo_id)
	// May return error for already exists, which is fine
	_ = err
}

func TestGitRepoPath(t *testing.T) {
	user := &User{UID: "u42"}
	repo_id := "my-repo"

	orig_data_dir := data_dir
	data_dir = "/var/lib/mochi"
	defer func() { data_dir = orig_data_dir }()

	path := git_repo_path(user, test_app, repo_id)
	expected := "/var/lib/mochi/users/u42/repositories/my-repo"

	if path != expected {
		t.Errorf("Expected path %q, got %q", expected, path)
	}
}

func TestGitRepoPathDifferentUsers(t *testing.T) {
	orig_data_dir := data_dir
	data_dir = "/data"
	defer func() { data_dir = orig_data_dir }()

	user1 := &User{UID: "u1"}
	user2 := &User{UID: "u999"}

	path1 := git_repo_path(user1, test_app, "repo")
	path2 := git_repo_path(user2, test_app, "repo")

	if path1 == path2 {
		t.Error("Different users should have different repo paths")
	}

	if !strings.Contains(path1, "/u1/") {
		t.Errorf("Path should contain user UID u1: %s", path1)
	}
	if !strings.Contains(path2, "/u999/") {
		t.Errorf("Path should contain user UID u999: %s", path2)
	}
}

func TestGitDelete(t *testing.T) {
	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repo_id := "delete-test-repo"

	err := git_init(user, test_app, repo_id)
	if err != nil {
		t.Fatalf("git_init failed: %v", err)
	}

	repo_path := git_repo_path(user, test_app, repo_id)
	if _, err := os.Stat(repo_path); os.IsNotExist(err) {
		t.Fatal("Repository should exist before delete")
	}

	err = git_delete(user, test_app, repo_id)
	if err != nil {
		t.Errorf("git_delete should succeed: %v", err)
	}

	if _, err := os.Stat(repo_path); !os.IsNotExist(err) {
		t.Error("Repository should not exist after delete")
	}
}

func TestGitDeleteNonExistent(t *testing.T) {
	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	// Deleting non-existent repo should not panic
	err := git_delete(user, test_app, "non-existent-repo")
	// May or may not error, but should not panic
	_ = err
}

func TestGitOpen(t *testing.T) {
	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repo_id := "open-test-repo"
	err := git_init(user, test_app, repo_id)
	if err != nil {
		t.Fatalf("git_init failed: %v", err)
	}

	repo, err := git_open(user, test_app, repo_id)
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

	_, err := git_open(user, test_app, "non-existent-repo")
	if err == nil {
		t.Error("git_open should fail for non-existent repository")
	}
}

func TestGitOpenMultipleTimes(t *testing.T) {
	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repo_id := "multi-open-repo"
	git_init(user, test_app, repo_id)

	// Open multiple times should all succeed
	for i := 0; i < 10; i++ {
		repo, err := git_open(user, test_app, repo_id)
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

	repo_id := "size-test-repo"
	git_init(user, test_app, repo_id)

	size, err := git_size(user, test_app, repo_id)
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

	_, err := git_size(user, test_app, "non-existent")
	if err == nil {
		t.Error("git_size should fail for non-existent repository")
	}
}

// ============ Ref Resolution Tests ============

func TestGitResolveRefHEAD(t *testing.T) {
	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repo_id := "resolve-ref-repo"
	repo := create_repo_with_commit(t, user, repo_id)

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

	repo_id := "resolve-invalid-repo"
	git_init(user, test_app, repo_id)
	repo, _ := git_open(user, test_app, repo_id)

	_, err := git_resolve_ref(repo, "refs/heads/nonexistent")
	if err == nil {
		t.Error("git_resolve_ref should fail for non-existent ref")
	}
}

func TestGitResolveRefFullHash(t *testing.T) {
	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repo_id := "resolve-hash-repo"
	repo := create_repo_with_commit(t, user, repo_id)

	// Get HEAD hash first
	head_reference, err := repo.Head()
	if err != nil {
		t.Skip("No HEAD in repo, skipping hash resolution test")
	}

	hash_string := head_reference.Hash().String()
	resolved, err := git_resolve_ref(repo, hash_string)
	if err != nil {
		t.Errorf("git_resolve_ref should resolve full hash: %v", err)
	}
	if resolved != nil && resolved.String() != hash_string {
		t.Errorf("Resolved hash mismatch: expected %s, got %s", hash_string, resolved.String())
	}
}

// ============ Multiple Repository Tests ============

func TestGitMultipleRepos(t *testing.T) {
	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repos := []string{"repo-a", "repo-b", "repo-c"}

	// Create all repos
	for _, repo_id := range repos {
		err := git_init(user, test_app, repo_id)
		if err != nil {
			t.Errorf("Failed to create %s: %v", repo_id, err)
		}
	}

	// Verify all exist
	for _, repo_id := range repos {
		_, err := git_open(user, test_app, repo_id)
		if err != nil {
			t.Errorf("Failed to open %s: %v", repo_id, err)
		}
	}

	// Delete one
	git_delete(user, test_app, "repo-b")

	// Verify others still work
	_, err := git_open(user, test_app, "repo-a")
	if err != nil {
		t.Error("repo-a should still be accessible")
	}
	_, err = git_open(user, test_app, "repo-c")
	if err != nil {
		t.Error("repo-c should still be accessible")
	}
	_, err = git_open(user, test_app, "repo-b")
	if err == nil {
		t.Error("repo-b should be deleted")
	}
}

func TestGitMultipleUsers(t *testing.T) {
	_, tmp_dir, cleanup := create_git_test_env(t)
	defer cleanup()

	user1 := &User{UID: "u1"}
	user2 := &User{UID: "u2"}

	// Create directories for user2
	user2Dir := filepath.Join(tmp_dir, "users", "2", "repositories")
	os.MkdirAll(user2Dir, 0755)

	// Create same-named repos for different users
	git_init(user1, test_app, "shared-name")
	git_init(user2, test_app, "shared-name")

	// Both should be accessible
	repo1, err := git_open(user1, test_app, "shared-name")
	if err != nil || repo1 == nil {
		t.Error("User1's repo should be accessible")
	}

	repo2, err := git_open(user2, test_app, "shared-name")
	if err != nil || repo2 == nil {
		t.Error("User2's repo should be accessible")
	}

	// They should be different paths
	path1 := git_repo_path(user1, test_app, "shared-name")
	path2 := git_repo_path(user2, test_app, "shared-name")
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
		err := git_init(user, test_app, name)
		if err != nil {
			t.Errorf("Failed to create repo with name %q: %v", name, err)
			continue
		}
		_, err = git_open(user, test_app, name)
		if err != nil {
			t.Errorf("Failed to open repo with name %q: %v", name, err)
		}
	}
}

// ============ Bare Repository Verification ============

func TestGitInitCreatesBareRepo(t *testing.T) {
	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repo_id := "bare-check-repo"
	git_init(user, test_app, repo_id)

	repo_path := git_repo_path(user, test_app, repo_id)

	// Bare repos have these files/dirs directly, not in .git
	required_files := []string{"HEAD", "config", "objects", "refs"}
	for _, f := range required_files {
		path := filepath.Join(repo_path, f)
		if _, err := os.Stat(path); os.IsNotExist(err) {
			t.Errorf("Bare repo should have %s at root level", f)
		}
	}

	// Should NOT have .git directory
	dot_git := filepath.Join(repo_path, ".git")
	if _, err := os.Stat(dot_git); !os.IsNotExist(err) {
		t.Error("Bare repo should not have .git directory")
	}

	// Verify config says bare = true
	config_path := filepath.Join(repo_path, "config")
	config_data, _ := os.ReadFile(config_path)
	if !strings.Contains(string(config_data), "bare = true") {
		t.Error("Bare repo config should have 'bare = true'")
	}
}

// ============ Default Branch Tests ============

func TestGitDefaultBranchNewRepo(t *testing.T) {
	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repo_id := "default-branch-repo"
	git_init(user, test_app, repo_id)

	repo_path := git_repo_path(user, test_app, repo_id)
	head_content, err := os.ReadFile(filepath.Join(repo_path, "HEAD"))
	if err != nil {
		t.Fatalf("Failed to read HEAD: %v", err)
	}

	head := strings.TrimSpace(string(head_content))
	// Should be a symbolic ref to main or master
	if !strings.HasPrefix(head, "ref: refs/heads/") {
		t.Errorf("HEAD should be symbolic ref, got: %s", head)
	}
}

// ============ Concurrent Access Tests ============

func TestGitConcurrentOpen(t *testing.T) {
	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repo_id := "concurrent-repo"
	git_init(user, test_app, repo_id)

	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func() {
			repo, err := git_open(user, test_app, repo_id)
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
	path := git_repo_path(nil, test_app, "test")
	if path != "" {
		t.Log("git_repo_path with nil user returned:", path)
	}
}

func TestGitEmptyRepoID(t *testing.T) {
	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	// Empty repo ID should probably fail
	err := git_init(user, test_app, "")
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

	repo_id := "branch-ops-repo"
	repo := create_repo_with_commit(t, user, repo_id)

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

// ============ Merge / branch-mutation access control ============

// TestGitMergeAccessControl is the unit-level proof of the repository-merge
// ACL (task #3): git_can_write - the shared gate for api_git_merge_perform and
// the branch create/delete/default-set primitives - authorizes a mutation only
// when the acting identity holds repository/<id> write, the same grant a git
// push requires, and fails closed otherwise. This encodes the plan's acceptance
// criteria: merge without repo write is denied; with the grant it is allowed;
// the repository owner ('*') is allowed; an absent identity is denied.
func TestGitMergeAccessControl(t *testing.T) {
	owner, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repo_id := "merge-acl-repo"
	resource := "repository/" + repo_id

	owner_identity := "12OwnerAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	writer_identity := "12WriterBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
	nobody_identity := "12NobodyCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"
	owner.Identity = &Entity{ID: owner_identity}

	// db_app_system keys on owner.UID; ensure its directory and the users dir
	// (for the identity() fallback path) exist under the test data dir.
	os.MkdirAll(filepath.Join(data_dir, "users", owner.UID, test_app.id), 0755)
	os.MkdirAll(filepath.Join(data_dir, "db"), 0755)

	// Seed the repositories ACL exactly as action_create + action_access_set
	// would: the owner holds '*', a collaborator holds 'write'. Insert directly
	// to keep the test free of replication side-effects.
	db := db_app_system(owner, test_app)
	if db == nil {
		t.Fatal("db_app_system returned nil")
	}
	db.access_setup()
	db.exec("insert into access ( subject, resource, operation, grant, granter, created ) values ( ?, ?, ?, ?, ?, ? )",
		owner_identity, resource, "*", 1, owner_identity, now())
	db.exec("insert into access ( subject, resource, operation, grant, granter, created ) values ( ?, ?, ?, ?, ?, ? )",
		writer_identity, resource, "write", 1, owner_identity, now())

	thread := func(u *User) *sl.Thread {
		th := &sl.Thread{}
		if u != nil {
			th.SetLocal("user", u)
		}
		th.SetLocal("owner", owner)
		th.SetLocal("app", test_app)
		return th
	}

	cases := []struct {
		name string
		user *User
		want bool
	}{
		{"repository owner ('*' grant) may merge", owner, true},
		{"collaborator with write grant may merge", &User{UID: "u2", Identity: &Entity{ID: writer_identity}}, true},
		{"identity with no grant is denied", &User{UID: "u3", Identity: &Entity{ID: nobody_identity}}, false},
		{"user without an identity is denied (fail closed)", &User{UID: "u4"}, false},
		{"no authenticated user is denied (fail closed)", nil, false},
	}

	for _, c := range cases {
		if got := git_can_write(thread(c.user), owner, test_app, repo_id); got != c.want {
			t.Errorf("%s: git_can_write = %v, want %v", c.name, got, c.want)
		}
	}
}

// TestGitReadAccessControl proves git_can_read - the gate on the diff and
// merge-check primitives - permits public ("*") repositories including
// anonymous callers, permits identities with an explicit read grant, and denies
// identities (and anonymous callers) without one. This closes the read-preview
// gap where another app could diff/merge-check a private repository it lacks
// read access to.
func TestGitReadAccessControl(t *testing.T) {
	owner, _, cleanup := create_git_test_env(t)
	defer cleanup()

	owner_identity := "12OwnerAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	reader_identity := "12ReaderDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD"
	nobody_identity := "12NobodyCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"
	owner.Identity = &Entity{ID: owner_identity}

	pub := "read-acl-public"
	priv := "read-acl-private"

	os.MkdirAll(filepath.Join(data_dir, "users", owner.UID, test_app.id), 0755)
	os.MkdirAll(filepath.Join(data_dir, "db"), 0755)

	db := db_app_system(owner, test_app)
	if db == nil {
		t.Fatal("db_app_system returned nil")
	}
	db.access_setup()
	grant := func(subject, repo, op string) {
		db.exec("insert into access ( subject, resource, operation, grant, granter, created ) values ( ?, ?, ?, ?, ?, ? )",
			subject, "repository/"+repo, op, 1, owner_identity, now())
	}
	// Public repo: anyone may read; owner holds '*'.
	grant("*", pub, "read")
	grant(owner_identity, pub, "*")
	// Private repo: owner '*' plus one explicit reader; no public grant.
	grant(owner_identity, priv, "*")
	grant(reader_identity, priv, "read")

	thread := func(u *User) *sl.Thread {
		th := &sl.Thread{}
		if u != nil {
			th.SetLocal("user", u)
		}
		th.SetLocal("owner", owner)
		th.SetLocal("app", test_app)
		return th
	}
	reader := &User{UID: "u2", Identity: &Entity{ID: reader_identity}}
	nobody := &User{UID: "u3", Identity: &Entity{ID: nobody_identity}}

	cases := []struct {
		name string
		user *User
		repo string
		want bool
	}{
		{"public repo, anonymous caller", nil, pub, true},
		{"public repo, any identity", nobody, pub, true},
		{"private repo, owner '*'", owner, priv, true},
		{"private repo, identity with read grant", reader, priv, true},
		{"private repo, identity without grant denied", nobody, priv, false},
		{"private repo, anonymous denied", nil, priv, false},
	}
	for _, c := range cases {
		if got := git_can_read(thread(c.user), owner, test_app, c.repo); got != c.want {
			t.Errorf("%s: git_can_read = %v, want %v", c.name, got, c.want)
		}
	}
}

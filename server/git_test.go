// Mochi server: Git operations unit tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp/capability"
	sl "go.starlark.net/starlark"
)

// test_app is the per-app context the git_* helpers expect. Tests use
// the literal "repositories" id so the expected on-disk paths
// (users/<uid>/repositories/<repo>) match what the assertions check.
var test_app = &App{id: "repositories"}

// Helper to create a test environment for git operations
func create_git_test_env(t *testing.T) (*User, string) {
	t.Helper()
	tmp_dir := test_data_directory(t)

	user := &User{UID: "u1"}

	user_dir := filepath.Join(tmp_dir, "users", "1", "repositories")
	if err := os.MkdirAll(user_dir, 0755); err != nil {
		t.Fatalf("Failed to create user dir: %v", err)
	}

	return user, tmp_dir
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
	tmp_work_dir := t.TempDir()

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
	user, _ := create_git_test_env(t)

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
	user, _ := create_git_test_env(t)

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
	user, _ := create_git_test_env(t)

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
	user, _ := create_git_test_env(t)

	// Deleting non-existent repo should not panic
	err := git_delete(user, test_app, "non-existent-repo")
	// May or may not error, but should not panic
	_ = err
}

func TestGitOpen(t *testing.T) {
	user, _ := create_git_test_env(t)

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
	user, _ := create_git_test_env(t)

	_, err := git_open(user, test_app, "non-existent-repo")
	if err == nil {
		t.Error("git_open should fail for non-existent repository")
	}
}

func TestGitOpenMultipleTimes(t *testing.T) {
	user, _ := create_git_test_env(t)

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
	user, _ := create_git_test_env(t)

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
	user, _ := create_git_test_env(t)

	_, err := git_size(user, test_app, "non-existent")
	if err == nil {
		t.Error("git_size should fail for non-existent repository")
	}
}

// ============ Ref Resolution Tests ============

func TestGitResolveRefHEAD(t *testing.T) {
	user, _ := create_git_test_env(t)

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
	user, _ := create_git_test_env(t)

	repo_id := "resolve-invalid-repo"
	git_init(user, test_app, repo_id)
	repo, _ := git_open(user, test_app, repo_id)

	_, err := git_resolve_ref(repo, "refs/heads/nonexistent")
	if err == nil {
		t.Error("git_resolve_ref should fail for non-existent ref")
	}
}

func TestGitResolveRefFullHash(t *testing.T) {
	user, _ := create_git_test_env(t)

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
	user, _ := create_git_test_env(t)

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
	_, tmp_dir := create_git_test_env(t)

	user1 := &User{UID: "u1"}
	user2 := &User{UID: "u2"}

	// Create directories for user2
	user2_dir := filepath.Join(tmp_dir, "users", "2", "repositories")
	os.MkdirAll(user2_dir, 0755)

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
	user, _ := create_git_test_env(t)

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
	user, _ := create_git_test_env(t)

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
	user, _ := create_git_test_env(t)

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
	user, _ := create_git_test_env(t)

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
	user, _ := create_git_test_env(t)

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
	user, _ := create_git_test_env(t)

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

// TestGitMergeAccessControl pins git_can_write, the shared gate for
// api_git_merge_perform and the branch create/delete/default-set primitives: a
// mutation needs repository/<id> write, the same grant a git push requires.
func TestGitMergeAccessControl(t *testing.T) {
	owner, _ := create_git_test_env(t)

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

// TestGitReadAccessControl pins git_can_read, the gate on the diff and
// merge-check primitives: public ("*") repositories permit anonymous callers,
// and an identity without a read grant is denied.
func TestGitReadAccessControl(t *testing.T) {
	owner, _ := create_git_test_env(t)

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

// The upload-pack negotiation section follows the want section: "have" lines
// with interleaved flush packets, ended by "done" (final round) or end of body
// (exploratory round). A pkt-line is "%04x" length-prefixed.
func TestGitUploadPackNegotiation(t *testing.T) {
	pkt := func(s string) string { return fmt.Sprintf("%04x%s", len(s)+4, s) }
	a := "722cd9468569ef931a61b731279583fa268254b2"
	b := "562d8c4b0d0b5da858a17cee1887bd93686b874d"
	want := pkt("want " + a + "\n")

	cases := []struct {
		name  string
		body  string
		haves int
		done  bool
	}{
		{"wants only", want + "0000", 0, false},
		{"exploratory round", want + "0000" + pkt("have "+a+"\n") + pkt("have "+b+"\n") + "0000", 2, false},
		{"final round", want + "0000" + pkt("have "+a+"\n") + "0000" + pkt("done\n"), 1, true},
		{"done without haves", want + "0000" + pkt("done\n"), 0, true},
	}

	for _, c := range cases {
		request, err := git_request_decode(strings.NewReader(c.body))
		if err != nil {
			t.Errorf("%s: %v", c.name, err)
			continue
		}
		if len(request.haves) != c.haves || request.done != c.done {
			t.Errorf("%s: got %d haves done=%v, want %d done=%v", c.name, len(request.haves), request.done, c.haves, c.done)
		}
	}

	request, err := git_request_decode(strings.NewReader(want + "0000" + pkt("have "+a+"\n") + "0000"))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(request.haves) != 1 || request.haves[0] != plumbing.NewHash(a) {
		t.Errorf("hash not parsed: %v", request.haves)
	}
}

// TestGitRequestDecode — the whole v0 request grammar in one pass, including
// the lines packp.UploadRequest.Decode cannot represent: a filter, and more
// than one deepen-not.
func TestGitRequestDecode(t *testing.T) {
	pkt := func(s string) string { return fmt.Sprintf("%04x%s", len(s)+4, s) }
	a := "722cd9468569ef931a61b731279583fa268254b2"
	b := "562d8c4b0d0b5da858a17cee1887bd93686b874d"

	body := pkt("want "+a+" multi_ack_detailed side-band-64k shallow deepen-relative\n") +
		pkt("want "+b+"\n") +
		pkt("shallow "+a+"\n") +
		pkt("deepen 5\n") +
		pkt("deepen-since 1700000000\n") +
		pkt("deepen-not refs/tags/v1\n") +
		pkt("deepen-not refs/heads/old\n") +
		pkt("filter blob:none\n") +
		"0000" +
		pkt("have "+b+"\n") +
		pkt("done\n")

	request, err := git_request_decode(strings.NewReader(body))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(request.wants) != 2 || request.wants[0] != plumbing.NewHash(a) {
		t.Errorf("wants = %v", request.wants)
	}
	if len(request.shallows) != 1 || request.shallows[0] != plumbing.NewHash(a) {
		t.Errorf("shallows = %v", request.shallows)
	}
	if request.depth != 5 {
		t.Errorf("depth = %d, want 5", request.depth)
	}
	if request.since.Unix() != 1700000000 {
		t.Errorf("since = %v", request.since)
	}
	if len(request.exclude) != 2 {
		t.Errorf("exclude = %v, want both deepen-not lines: one is all packp can hold", request.exclude)
	}
	if request.filter != "blob:none" {
		t.Errorf("filter = %q; packp reports this line as a decode error", request.filter)
	}
	if !request.relative {
		t.Error("deepen-relative was advertised and requested but not recorded")
	}
	if !request.capabilities.Supports(capability.Sideband64k) {
		t.Error("capabilities from the first want line were not decoded")
	}
	if len(request.haves) != 1 || !request.done {
		t.Errorf("negotiation section: %d haves, done=%v", len(request.haves), request.done)
	}
	if !request.shallow() || !request.deepening() {
		t.Error("a request carrying shallow and deepen lines reports neither")
	}
}

// TestGitRequestDecodeRefusesMalformed — a request we cannot read must be
// refused, never half-understood: a dropped line is a pack built to the wrong
// specification, which the client cannot detect.
func TestGitRequestDecodeRefusesMalformed(t *testing.T) {
	pkt := func(s string) string { return fmt.Sprintf("%04x%s", len(s)+4, s) }
	a := "722cd9468569ef931a61b731279583fa268254b2"

	for _, c := range []struct{ name, body string }{
		{"short object name", pkt("want 722cd94\n")},
		{"object name that is not hex", pkt("want zzzzd9468569ef931a61b731279583fa268254b2\n")},
		{"all-zero object name", pkt("want 0000000000000000000000000000000000000000\n")},
		{"depth that is not a number", pkt("want "+a+"\n") + pkt("deepen soon\n")},
		{"negative depth", pkt("want "+a+"\n") + pkt("deepen -1\n")},
		{"deepen-since that is not a timestamp", pkt("want "+a+"\n") + pkt("deepen-since yesterday\n")},
		{"unknown keyword", pkt("want "+a+"\n") + pkt("enhance 4\n")},
	} {
		if _, err := git_request_decode(strings.NewReader(c.body)); err == nil {
			t.Errorf("%s: accepted, want a refusal", c.name)
		}
	}
}

// ============ Review Fix Tests ============

// A branch name is joined onto the repository directory by the filesystem
// storer, so one containing ".." is cleaned into a path outside the ref
// namespace. Creating such a branch overwrote the bare repository's own config
// file with a hash and deleting one removed it.
func TestGitBranchReferenceRejectsTraversal(t *testing.T) {
	for _, name := range []string{"../../config", "../HEAD", "../../packed-refs", "a/../../../x"} {
		if _, err := git_branch_reference(name); err == nil {
			t.Errorf("git_branch_reference(%q) was accepted, want rejection", name)
		}
	}
	for _, name := range []string{"main", "feature/login", "release-1.2", "a/b/c"} {
		reference, err := git_branch_reference(name)
		if err != nil {
			t.Errorf("git_branch_reference(%q) = %v, want acceptance", name, err)
			continue
		}
		if want := "refs/heads/" + name; reference.String() != want {
			t.Errorf("git_branch_reference(%q) = %q, want %q", name, reference, want)
		}
	}
}

// The whole point of validating: the repository's own files must survive a
// branch name that tries to address them.
func TestGitBranchTraversalLeavesRepositoryIntact(t *testing.T) {
	user, _ := create_git_test_env(t)

	repo := create_repo_with_commit(t, user, "repo1")
	config_path := filepath.Join(git_repo_path(user, test_app, "repo1"), "config")
	before, err := os.ReadFile(config_path)
	if err != nil {
		t.Fatalf("reading config: %v", err)
	}

	head, err := repo.Head()
	if err != nil {
		t.Fatalf("Head: %v", err)
	}

	name, err := git_branch_reference("../../config")
	if err == nil {
		// Validation is the fix; if it ever regresses, prove the consequence.
		repo.Storer.SetReference(plumbing.NewHashReference(name, head.Hash()))
	}

	after, err := os.ReadFile(config_path)
	if err != nil {
		t.Fatalf("config missing after traversing branch name: %v", err)
	}
	if string(before) != string(after) {
		t.Errorf("repository config was overwritten via a branch name: %q -> %q", before, after)
	}
}

// A merge resolves the target tip, then does tree work; an unconditional write
// at the end would discard any push that landed meanwhile.
func TestGitUpdateBranchRefusesChangedTarget(t *testing.T) {
	user, _ := create_git_test_env(t)

	repo := create_repo_with_commit(t, user, "repo1")
	head, err := repo.Head()
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	current := head.Hash()
	other := plumbing.NewHash("1111111111111111111111111111111111111111")
	next := plumbing.NewHash("2222222222222222222222222222222222222222")

	// Target no longer holds what the merge read: refuse.
	if err := git_update_branch(repo, "main", other, next); err == nil {
		t.Error("git_update_branch overwrote a branch that had moved, want refusal")
	}
	reference, err := repo.Reference(plumbing.NewBranchReferenceName("main"), false)
	if err != nil {
		t.Fatalf("Reference: %v", err)
	}
	if reference.Hash() != current {
		t.Errorf("branch was moved despite the refusal: %v", reference.Hash())
	}

	// Target still holds what the merge read: proceed.
	if err := git_update_branch(repo, "main", current, next); err != nil {
		t.Errorf("git_update_branch on an unchanged branch = %v, want success", err)
	}
	reference, err = repo.Reference(plumbing.NewBranchReferenceName("main"), false)
	if err != nil {
		t.Fatalf("Reference: %v", err)
	}
	if reference.Hash() != next {
		t.Errorf("branch = %v, want %v", reference.Hash(), next)
	}

	// An invalid target name is refused rather than written.
	if err := git_update_branch(repo, "../../config", next, next); err == nil {
		t.Error("git_update_branch accepted a traversing branch name")
	}
}

// Genuine concurrency rather than a simulated stale hash: every worker reads
// the same tip and then tries to move the branch. Compare-and-swap must let
// exactly one through; two winners would be a lost update.
func TestGitUpdateBranchConcurrentWritersLoseAtMostOne(t *testing.T) {
	user, _ := create_git_test_env(t)

	repo := create_repo_with_commit(t, user, "repo1")
	head, err := repo.Head()
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	start := head.Hash()

	const writers = 8
	var wg sync.WaitGroup
	results := make([]error, writers)
	candidates := make([]plumbing.Hash, writers)
	for i := 0; i < writers; i++ {
		candidates[i] = plumbing.NewHash(fmt.Sprintf("%040x", i+1))
	}
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = git_update_branch(repo, "main", start, candidates[i])
		}(i)
	}
	wg.Wait()

	won := []int{}
	for i, err := range results {
		if err == nil {
			won = append(won, i)
		}
	}
	if len(won) != 1 {
		t.Fatalf("%d of %d concurrent writers succeeded, want exactly 1 (more than one means a lost update)", len(won), writers)
	}

	reference, err := repo.Reference(plumbing.NewBranchReferenceName("main"), false)
	if err != nil {
		t.Fatalf("Reference: %v", err)
	}
	if reference.Hash() != candidates[won[0]] {
		t.Errorf("branch = %v, want the winner's hash %v", reference.Hash(), candidates[won[0]])
	}
	if reference.Hash() == start {
		t.Error("branch never moved, so the winner's write was lost")
	}
}

// The git handlers must refuse when the app-system database cannot be opened,
// not skip the access check. The fixture makes only the app directory
// unwritable: db_app_system returns nil only when app.db cannot be CREATED, and
// breaking data_dir wholesale makes the unfixed handler fail later anyway, so
// the test would pass either way.
func TestGitHandlerRefusesWithoutAppDatabase(t *testing.T) {
	user, _ := create_git_test_env(t)

	repo := "repo1"
	if err := git_init(user, test_app, repo); err != nil {
		t.Fatalf("git_init: %v", err)
	}

	// Make app.db uncreatable without touching the repository beside it.
	app_dir := filepath.Join(data_dir, "users", user.UID, test_app.id)
	if err := os.Chmod(app_dir, 0555); err != nil {
		t.Fatalf("making the app directory read-only: %v", err)
	}
	defer os.Chmod(app_dir, 0755)

	if db := db_app_system(user, test_app); db != nil {
		t.Skip("app.db was still creatable in a read-only directory (running as root?); premise does not hold")
	}

	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest("GET", "/x/git/info/refs?service=git-upload-pack", nil)

	entity := &Entity{ID: repo, Class: "repository"}
	if !git_http_handler_entity(c, test_app, user, nil, entity, "info/refs") {
		t.Fatal("handler did not handle the request")
	}
	if recorder.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want %d - an unopenable access database must refuse, not fall through to the git service",
			recorder.Code, http.StatusInternalServerError)
	}
	if strings.Contains(recorder.Body.String(), "service=git-upload-pack") {
		t.Errorf("refs were advertised to an anonymous caller despite the missing access database: %q",
			recorder.Body.String())
	}
}

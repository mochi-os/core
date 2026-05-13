// Mochi server: db_upgrade_54 migration test
// Copyright Alistair Cunningham 2026

package main

import (
	"os"
	"path/filepath"
	"testing"
)

// TestDbUpgrade54PublishedRenames covers the published-deployment branch:
// the migration detects that the published repositories app is installed
// under apps_root and moves every user's `users/<uid>/repositories/` to
// `users/<uid>/<published-entity-id>/`.
func TestDbUpgrade54PublishedRenames(t *testing.T) {
	const repositoriesEntity = "1SWnPXg9xpT2Cxemw2aw8CLZCP5yDatQ6ebF9dHoMTXQNFKLuw"
	tmp, _ := os.MkdirTemp("", "mochi_v54")
	defer os.RemoveAll(tmp)
	orig := data_dir
	data_dir = tmp
	defer func() { data_dir = orig }()

	// Published repositories app dir present — should trigger the rename.
	os.MkdirAll(filepath.Join(tmp, "apps", repositoriesEntity), 0755)

	// Two users with old-shape repositories dirs containing one repo each.
	os.MkdirAll(filepath.Join(tmp, "users", "uid-alice", "repositories", "repo-1", "objects"), 0755)
	os.WriteFile(filepath.Join(tmp, "users", "uid-alice", "repositories", "repo-1", "HEAD"), []byte("ref: refs/heads/main\n"), 0644)
	os.MkdirAll(filepath.Join(tmp, "users", "uid-bob", "repositories", "repo-2"), 0755)

	db_upgrade_54()

	// Old path is gone, new path has the data.
	if _, err := os.Stat(filepath.Join(tmp, "users", "uid-alice", "repositories")); !os.IsNotExist(err) {
		t.Error("users/uid-alice/repositories/ should no longer exist after rename")
	}
	if _, err := os.Stat(filepath.Join(tmp, "users", "uid-alice", repositoriesEntity, "repo-1", "HEAD")); os.IsNotExist(err) {
		t.Error("users/uid-alice/<repositories-entity>/repo-1/HEAD should exist after rename")
	}
	if _, err := os.Stat(filepath.Join(tmp, "users", "uid-bob", repositoriesEntity, "repo-2")); os.IsNotExist(err) {
		t.Error("users/uid-bob/<repositories-entity>/repo-2 should exist after rename")
	}
}

// TestDbUpgrade54DevSkips covers the dev-deployment branch: with no
// published-app directory present, the migration leaves source dirs alone
// because the source name already matches dev `app.id`.
func TestDbUpgrade54DevSkips(t *testing.T) {
	tmp, _ := os.MkdirTemp("", "mochi_v54_dev")
	defer os.RemoveAll(tmp)
	orig := data_dir
	data_dir = tmp
	defer func() { data_dir = orig }()

	os.MkdirAll(filepath.Join(tmp, "users", "uid-alice", "repositories", "repo-1"), 0755)

	db_upgrade_54()

	if _, err := os.Stat(filepath.Join(tmp, "users", "uid-alice", "repositories", "repo-1")); os.IsNotExist(err) {
		t.Error("dev source dir should be left in place")
	}
}

// TestDbUpgrade54TargetExistsMerges covers the rare collision case where a
// target dir already exists (e.g. a partial earlier migration). The
// migration must move each child individually rather than overwrite.
func TestDbUpgrade54TargetExistsMerges(t *testing.T) {
	const repositoriesEntity = "1SWnPXg9xpT2Cxemw2aw8CLZCP5yDatQ6ebF9dHoMTXQNFKLuw"
	tmp, _ := os.MkdirTemp("", "mochi_v54_merge")
	defer os.RemoveAll(tmp)
	orig := data_dir
	data_dir = tmp
	defer func() { data_dir = orig }()

	os.MkdirAll(filepath.Join(tmp, "apps", repositoriesEntity), 0755)
	os.MkdirAll(filepath.Join(tmp, "users", "uid-alice", "repositories", "repo-1"), 0755)
	os.MkdirAll(filepath.Join(tmp, "users", "uid-alice", repositoriesEntity, "repo-2"), 0755) // pre-existing target

	db_upgrade_54()

	for _, want := range []string{"repo-1", "repo-2"} {
		if _, err := os.Stat(filepath.Join(tmp, "users", "uid-alice", repositoriesEntity, want)); os.IsNotExist(err) {
			t.Errorf("target should contain %q after merge", want)
		}
	}
	if _, err := os.Stat(filepath.Join(tmp, "users", "uid-alice", "repositories")); !os.IsNotExist(err) {
		t.Error("source dir should be removed after merge")
	}
}

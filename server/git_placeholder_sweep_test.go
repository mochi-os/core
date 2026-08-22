// Mochi server: the placeholder sweep has to look where repositories are.
//
// git_repo_path composes users/<uid>/<app.id>/<entity>, and app.id is the app's
// name only on a dev install - on a published one it is the app's entity id.
// The sweep's glob therefore spans every app directory, which is safe because
// it is signature-gated rather than path-gated.
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
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
)

// sweep_placeholder_repo creates a bare repository at path carrying exactly the
// signature the sweep looks for: one ref, refs/heads/main, on a parentless
// empty-tree "Initial commit" by Mochi <mochi@localhost>.
func sweep_placeholder_repo(t *testing.T, path string) {
	t.Helper()
	sweep_repo(t, path, "Initial commit", "Mochi", "mochi@localhost", true)
}

// sweep_repo creates a bare repository with a single root commit. empty selects
// the empty tree; otherwise the commit gets a tree holding one file, which is
// what a repository with real work in it looks like.
func sweep_repo(t *testing.T, path, message, name, email string, empty bool) {
	t.Helper()
	if err := os.MkdirAll(path, 0o700); err != nil {
		t.Fatalf("creating %s: %v", path, err)
	}
	repo, err := git.PlainInit(path, true)
	if err != nil {
		t.Fatalf("init %s: %v", path, err)
	}

	tree := plumbing.NewHash("4b825dc642cb6eb9a060e54bf8d69288fbee4904")
	if !empty {
		// A blob, and a tree naming it: enough to be a different tree hash.
		blob := &plumbing.MemoryObject{}
		blob.SetType(plumbing.BlobObject)
		if _, err := blob.Write([]byte("real work\n")); err != nil {
			t.Fatalf("writing blob: %v", err)
		}
		blob_hash, err := repo.Storer.SetEncodedObject(blob)
		if err != nil {
			t.Fatalf("storing blob: %v", err)
		}
		real_tree := &object.Tree{Entries: []object.TreeEntry{
			{Name: "file.txt", Mode: 0o100644, Hash: blob_hash},
		}}
		encoded := &plumbing.MemoryObject{}
		if err := real_tree.Encode(encoded); err != nil {
			t.Fatalf("encoding tree: %v", err)
		}
		tree, err = repo.Storer.SetEncodedObject(encoded)
		if err != nil {
			t.Fatalf("storing tree: %v", err)
		}
	}

	when := time.Unix(1700000000, 0)
	commit := &object.Commit{
		Author:    signature(name, email, when),
		Committer: signature(name, email, when),
		Message:   message,
		TreeHash:  tree,
	}
	encoded := &plumbing.MemoryObject{}
	if err := commit.Encode(encoded); err != nil {
		t.Fatalf("encoding commit: %v", err)
	}
	hash, err := repo.Storer.SetEncodedObject(encoded)
	if err != nil {
		t.Fatalf("storing commit: %v", err)
	}
	ref := plumbing.NewHashReference(plumbing.NewBranchReferenceName("main"), hash)
	if err := repo.Storer.SetReference(ref); err != nil {
		t.Fatalf("setting ref: %v", err)
	}
}

// signature builds a commit signature.
func signature(name, email string, when time.Time) object.Signature {
	return object.Signature{Name: name, Email: email, When: when}
}

// sweep_has_main reports whether refs/heads/main still resolves.
func sweep_has_main(t *testing.T, path string) bool {
	t.Helper()
	repo, err := git.PlainOpen(path)
	if err != nil {
		t.Fatalf("opening %s: %v", path, err)
	}
	_, err = repo.Reference(plumbing.NewBranchReferenceName("main"), false)
	return err == nil
}

// sweep_data_dir points data_dir at a temporary tree for one test.
func sweep_data_dir(t *testing.T) string {
	t.Helper()
	test_data_directory(t)
	return data_dir
}

// TestSweepReachesAPublishedInstall is the regression. A published app's
// directory is its entity id, so the old glob's literal "repositories" never
// matched and these repositories were never repaired.
func TestSweepReachesAPublishedInstall(t *testing.T) {
	root := sweep_data_dir(t)
	// The repositories app as published: the directory is an entity id. The id
	// is invented - core is the public mirror, so a real one out of the
	// registry would publish which apps exist, and a PRIVATE app's id would
	// publish that it exists at all.
	published := filepath.Join(root, "users", "user-one",
		"1AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "repo-entity")
	sweep_placeholder_repo(t, published)

	git_placeholder_sweep()

	if sweep_has_main(t, published) {
		t.Error("the placeholder ref survives in a published install; a first push to this repository is still refused as unrelated history")
	}
}

// TestSweepStillReachesADevInstall: widening the glob must not lose the case
// that already worked.
func TestSweepStillReachesADevInstall(t *testing.T) {
	root := sweep_data_dir(t)
	dev := filepath.Join(root, "users", "user-one", "repositories", "repo-entity")
	sweep_placeholder_repo(t, dev)

	git_placeholder_sweep()

	if sweep_has_main(t, dev) {
		t.Error("the placeholder ref survives in a dev install, which the sweep already handled before")
	}
}

// TestSweepLeavesRealWorkAlone is what makes the wider glob safe. The sweep now
// visits every app directory, so the signature check is the only thing standing
// between it and somebody's repository.
func TestSweepLeavesRealWorkAlone(t *testing.T) {
	root := sweep_data_dir(t)

	cases := []struct {
		name    string
		message string
		author  string
		email   string
		empty   bool
	}{
		{"real-tree", "Initial commit", "Mochi", "mochi@localhost", false},
		{"real-author", "Initial commit", "Alice", "alice@example.com", true},
		{"real-message", "Add the parser", "Mochi", "mochi@localhost", true},
	}
	for _, c := range cases {
		path := filepath.Join(root, "users", "user-one", "app-entity-id", c.name)
		sweep_repo(t, path, c.message, c.author, c.email, c.empty)
	}

	git_placeholder_sweep()

	for _, c := range cases {
		path := filepath.Join(root, "users", "user-one", "app-entity-id", c.name)
		if !sweep_has_main(t, path) {
			t.Errorf("%s: the sweep deleted the branch of a repository that does not carry the placeholder signature", c.name)
		}
	}
}

// TestSweepIgnoresNonRepositories: the glob now matches every per-app child,
// which includes db directories and file stores. They have no HEAD and must be
// passed over without error.
func TestSweepIgnoresNonRepositories(t *testing.T) {
	root := sweep_data_dir(t)
	for _, dir := range []string{"db", "files", "files/thumbnails"} {
		if err := os.MkdirAll(filepath.Join(root, "users", "user-one", "app-entity-id", dir), 0o700); err != nil {
			t.Fatalf("creating %s: %v", dir, err)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "users", "user-one", "app-entity-id", "db", "app.db"), []byte("not a repo"), 0o600); err != nil {
		t.Fatalf("writing app.db: %v", err)
	}

	git_placeholder_sweep() // must simply return

	// And a real placeholder alongside them is still found.
	path := filepath.Join(root, "users", "user-one", "app-entity-id", "repo-entity")
	sweep_placeholder_repo(t, path)
	git_placeholder_sweep()
	if sweep_has_main(t, path) {
		t.Error("a placeholder sharing an app directory with db/ and files/ was not swept")
	}
}

// TestSweepDoesNotNameAnAppDirectory is the gate. The literal is the defect:
// any path segment naming one app resurrects the dev-only assumption.
func TestSweepDoesNotNameAnAppDirectory(t *testing.T) {
	source, err := os.ReadFile("git.go")
	if err != nil {
		t.Fatalf("reading git.go: %v", err)
	}
	text := string(source)
	at := strings.Index(text, "func git_placeholder_sweep(")
	if at < 0 {
		t.Fatal("git.go no longer defines git_placeholder_sweep")
	}
	body := text[at:]
	if end := strings.Index(body, "\n}\n"); end > 0 {
		body = body[:end]
	}

	if strings.Contains(body, `"repositories"`) {
		t.Error(`git_placeholder_sweep names the "repositories" directory again; app.id is the app NAME only on a dev install, so this sweeps the dev rig and silently finds nothing on a published deployment`)
	}
}

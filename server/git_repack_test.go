// Mochi server: packing a repository's loose objects.
//
// go-git writes every object loose and never packs, so a repository grows one
// small file per object for ever. A push is what creates them, so a push is
// what considers packing them.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage"
	"github.com/go-git/go-git/v5/storage/filesystem"
)

// repack_test_repository builds a bare repository holding `commits` commits,
// each adding one file, and answers its path and its head.
func repack_test_repository(t *testing.T, commits int) (string, plumbing.Hash) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "repository")
	repo, err := git.PlainInit(path, true)
	if err != nil {
		t.Fatalf("PlainInit: %v", err)
	}

	var parents []plumbing.Hash
	var head plumbing.Hash
	for i := 0; i < commits; i++ {
		blob_hash := repack_test_blob(t, repo, fmt.Sprintf("contents of file %d\n", i))

		tree := &object.Tree{Entries: []object.TreeEntry{
			{Name: fmt.Sprintf("file-%d", i), Mode: 0100644, Hash: blob_hash},
		}}
		encoded := repo.Storer.NewEncodedObject()
		if err := tree.Encode(encoded); err != nil {
			t.Fatalf("encoding a tree: %v", err)
		}
		tree_hash, err := repo.Storer.SetEncodedObject(encoded)
		if err != nil {
			t.Fatalf("storing a tree: %v", err)
		}

		when := time.Now().Add(-time.Duration(commits-i) * time.Minute)
		commit := &object.Commit{
			Author:       object.Signature{Name: "Test", Email: "test@example.com", When: when},
			Committer:    object.Signature{Name: "Test", Email: "test@example.com", When: when},
			Message:      fmt.Sprintf("commit %d\n", i),
			TreeHash:     tree_hash,
			ParentHashes: parents,
		}
		encoded = repo.Storer.NewEncodedObject()
		if err := commit.Encode(encoded); err != nil {
			t.Fatalf("encoding a commit: %v", err)
		}
		head, err = repo.Storer.SetEncodedObject(encoded)
		if err != nil {
			t.Fatalf("storing a commit: %v", err)
		}
		parents = []plumbing.Hash{head}
	}

	if err := repo.Storer.SetReference(plumbing.NewHashReference("refs/heads/main", head)); err != nil {
		t.Fatalf("SetReference: %v", err)
	}
	return path, head
}

// repack_test_blob stores one blob and answers its hash.
func repack_test_blob(t *testing.T, repo *git.Repository, contents string) plumbing.Hash {
	t.Helper()
	object := repo.Storer.NewEncodedObject()
	object.SetType(plumbing.BlobObject)
	writer, err := object.Writer()
	if err != nil {
		t.Fatalf("blob writer: %v", err)
	}
	if _, err := writer.Write([]byte(contents)); err != nil {
		t.Fatalf("writing a blob: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("closing a blob: %v", err)
	}
	hash, err := repo.Storer.SetEncodedObject(object)
	if err != nil {
		t.Fatalf("storing a blob: %v", err)
	}
	return hash
}

// packs reports how many packfiles a repository holds.
func packs(t *testing.T, path string) int {
	t.Helper()
	entries, err := filepath.Glob(filepath.Join(path, "objects", "pack", "*.pack"))
	if err != nil {
		t.Fatalf("globbing packs: %v", err)
	}
	return len(entries)
}

// TestRepackPacksAndStillReads - the point of the whole thing: the loose
// objects become one pack, and everything that reads a repository still does.
func TestRepackPacksAndStillReads(t *testing.T) {
	path, head := repack_test_repository(t, 20)

	loose := git_loose_count(path)
	if loose < 60 {
		t.Fatalf("setup: %d loose objects, expected three per commit", loose)
	}
	if packs(t, path) != 0 {
		t.Fatal("setup: go-git wrote a pack, which it is not supposed to do")
	}

	if err := git_repack(path); err != nil {
		t.Fatalf("git_repack: %v", err)
	}

	if after := git_loose_count(path); after != 0 {
		t.Errorf("loose objects after repack: %d, want 0 (was %d)", after, loose)
	}
	if count := packs(t, path); count != 1 {
		t.Errorf("packfiles after repack: %d, want 1", count)
	}

	// Reading: the history, a tree, and a blob's bytes - what serving a
	// repository actually does.
	repo, err := git.PlainOpen(path)
	if err != nil {
		t.Fatalf("PlainOpen after repack: %v", err)
	}
	commit, err := repo.CommitObject(head)
	if err != nil {
		t.Fatalf("reading the head commit from the pack: %v", err)
	}
	walked := 0
	iterator, err := repo.Log(&git.LogOptions{From: head})
	if err != nil {
		t.Fatalf("Log: %v", err)
	}
	_ = iterator.ForEach(func(*object.Commit) error { walked++; return nil })
	if walked != 20 {
		t.Errorf("commits reachable after repack: %d, want 20", walked)
	}
	file, err := commit.File("file-19")
	if err != nil {
		t.Fatalf("reading a file from the packed tree: %v", err)
	}
	contents, err := file.Contents()
	if err != nil || contents != "contents of file 19\n" {
		t.Errorf("blob contents from the pack: %q, %v", contents, err)
	}

	// Writing: a reference update is how a push ends.
	if err := repo.Storer.SetReference(plumbing.NewHashReference("refs/heads/second", head)); err != nil {
		t.Errorf("reference update against a packed repository: %v", err)
	}
}

// TestRepackKeepsUnreachableUntilItAges - the push window. Between promotion
// and the reference update a pushed object is unreachable, and pruning it then
// destroys the push. git_prune_age is what stops that.
func TestRepackKeepsUnreachableUntilItAges(t *testing.T) {
	path, _ := repack_test_repository(t, 3)

	repo, err := git.PlainOpen(path)
	if err != nil {
		t.Fatalf("PlainOpen: %v", err)
	}
	hash := repack_test_blob(t, repo, "promoted but not yet referenced\n")

	if err := git_repack(path); err != nil {
		t.Fatalf("git_repack: %v", err)
	}
	if _, err := repo.BlobObject(hash); err != nil {
		t.Fatalf("a fresh unreferenced object was pruned - this is a push being destroyed: %v", err)
	}

	// The same object, older than the expiry, is what prune is for.
	loose := filepath.Join(path, "objects", hash.String()[:2], hash.String()[2:])
	old := time.Now().Add(-git_prune_age - time.Hour)
	if err := os.Chtimes(loose, old, old); err != nil {
		t.Fatalf("ageing the object: %v", err)
	}
	if err := git_repack(path); err != nil {
		t.Fatalf("second git_repack: %v", err)
	}
	if _, err := os.Stat(loose); !os.IsNotExist(err) {
		t.Error("an unreachable object past the expiry survived the prune")
	}
}

// TestRepackKeepsAnOrphanedPackedCommit - a commit packed by one repack and
// then force-pushed away lives only in that pack, and the next repack deletes
// the pack. It must come out loose and keep git_prune_age like any other
// unreachable object, rather than go with the pack however recently it was
// orphaned; once it has aged, it goes.
func TestRepackKeepsAnOrphanedPackedCommit(t *testing.T) {
	path, head := repack_test_repository(t, 3)
	if err := git_repack(path); err != nil {
		t.Fatalf("first git_repack: %v", err)
	}

	repo, err := git.PlainOpen(path)
	if err != nil {
		t.Fatalf("PlainOpen: %v", err)
	}
	commit, err := repo.CommitObject(head)
	if err != nil {
		t.Fatalf("reading the head: %v", err)
	}
	tree, err := commit.Tree()
	if err != nil {
		t.Fatalf("reading the head's tree: %v", err)
	}
	orphans := []plumbing.Hash{head, commit.TreeHash, tree.Entries[0].Hash}
	if err := repo.Storer.SetReference(plumbing.NewHashReference("refs/heads/main", commit.ParentHashes[0])); err != nil {
		t.Fatalf("force-pushing main back a commit: %v", err)
	}

	if err := git_repack(path); err != nil {
		t.Fatalf("second git_repack: %v", err)
	}
	repack_test_readable(t, path, orphans...)
	if loose := git_loose_count(path); loose != len(orphans) {
		t.Errorf("loose objects after repacking around the orphan: %d, want %d", loose, len(orphans))
	}

	repack_test_age(t, path)
	if err := git_repack(path); err != nil {
		t.Fatalf("third git_repack: %v", err)
	}
	after, err := git.PlainOpen(path)
	if err != nil {
		t.Fatalf("PlainOpen: %v", err)
	}
	for _, hash := range orphans {
		if _, err := after.Storer.EncodedObject(plumbing.AnyObject, hash); err == nil {
			t.Errorf("orphaned object %s outlived git_prune_age", hash)
		}
	}
}

// repack_test_packs answers the base names - pack-<hash> - of a repository's packs.
func repack_test_packs(t *testing.T, path string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(path, "objects", "pack", "pack-*.pack"))
	if err != nil {
		t.Fatalf("globbing packs: %v", err)
	}
	var names []string
	for _, match := range matches {
		names = append(names, strings.TrimSuffix(filepath.Base(match), ".pack"))
	}
	return names
}

// repack_test_siblings writes the files git keeps beside a pack.
func repack_test_siblings(t *testing.T, path, base string) []string {
	t.Helper()
	var files []string
	for _, extension := range []string{".bitmap", ".rev"} {
		file := filepath.Join(path, "objects", "pack", base+extension)
		if err := os.WriteFile(file, []byte("written by git"), 0644); err != nil {
			t.Fatalf("writing %s: %v", file, err)
		}
		files = append(files, file)
	}
	return files
}

// TestRepackSweepsOrphanedPackFiles - the packs on the production server were
// written by git, with a bitmap and a reverse index beside each. go-git deletes
// only a replaced pack and its index, so both were left behind for ever - and
// one repository there already carries a stranded pair.
func TestRepackSweepsOrphanedPackFiles(t *testing.T) {
	path, _ := repack_test_repository(t, 3)
	if err := git_repack(path); err != nil {
		t.Fatalf("first git_repack: %v", err)
	}
	before := repack_test_packs(t, path)
	if len(before) != 1 {
		t.Fatalf("setup: %d packs, want 1", len(before))
	}
	orphans := repack_test_siblings(t, path, before[0])
	orphans = append(orphans, repack_test_siblings(t, path, "pack-8585dad0e179053f14ffd5aca38eb6280d15d61b")...)

	// Something new to pack, so the repack replaces the pack rather than
	// writing the same one again.
	repo, err := git.PlainOpen(path)
	if err != nil {
		t.Fatalf("PlainOpen: %v", err)
	}
	blob := repack_test_blob(t, repo, "tagged after the first repack\n")
	if err := repo.Storer.SetReference(plumbing.NewHashReference("refs/tags/new", blob)); err != nil {
		t.Fatalf("SetReference: %v", err)
	}
	if err := git_repack(path); err != nil {
		t.Fatalf("second git_repack: %v", err)
	}
	if after := repack_test_packs(t, path); len(after) != 1 || after[0] == before[0] {
		t.Fatalf("setup: packs %v after the second repack, want one replacing %s", after, before[0])
	}
	for _, orphan := range orphans {
		if _, err := os.Stat(orphan); !os.IsNotExist(err) {
			t.Errorf("%s outlived its pack", filepath.Base(orphan))
		}
	}
}

// TestPackSweepKeepsTheLivePack - a pack still in use keeps whatever git wrote
// beside it, and nothing in the directory that is not a pack's is touched.
func TestPackSweepKeepsTheLivePack(t *testing.T) {
	path, _ := repack_test_repository(t, 3)
	if err := git_repack(path); err != nil {
		t.Fatalf("git_repack: %v", err)
	}
	live := repack_test_packs(t, path)
	if len(live) != 1 {
		t.Fatalf("setup: %d packs, want 1", len(live))
	}
	kept := repack_test_siblings(t, path, live[0])
	stray := filepath.Join(path, "objects", "pack", "tmp_pack_123.rev")
	if err := os.WriteFile(stray, nil, 0644); err != nil {
		t.Fatalf("writing %s: %v", stray, err)
	}
	kept = append(kept, stray)

	repo, err := git.PlainOpen(path)
	if err != nil {
		t.Fatalf("PlainOpen: %v", err)
	}
	if err := git_pack_sweep(repo.Storer); err != nil {
		t.Fatalf("git_pack_sweep: %v", err)
	}
	for _, file := range kept {
		if _, err := os.Stat(file); err != nil {
			t.Errorf("the sweep removed %s: %v", filepath.Base(file), err)
		}
	}
}

// TestRepackRepeats - a long-lived server repacks the same repository again and
// again. The second time is the one that broke: RepackObjects replaces the
// packfile, and a Prune afterwards on the same handle reads a pack that is no
// longer there.
func TestRepackRepeats(t *testing.T) {
	path, head := repack_test_repository(t, 5)

	for cycle := 1; cycle <= 3; cycle++ {
		repo, err := git.PlainOpen(path)
		if err != nil {
			t.Fatalf("cycle %d: PlainOpen: %v", cycle, err)
		}
		repack_test_blob(t, repo, fmt.Sprintf("loose object for cycle %d\n", cycle))
		if err := git_repack(path); err != nil {
			t.Fatalf("cycle %d: %v", cycle, err)
		}
		if count := packs(t, path); count != 1 {
			t.Errorf("cycle %d: packfiles %d, want 1", cycle, count)
		}
	}

	repo, err := git.PlainOpen(path)
	if err != nil {
		t.Fatalf("PlainOpen: %v", err)
	}
	if _, err := repo.CommitObject(head); err != nil {
		t.Errorf("the history did not survive three repacks: %v", err)
	}
}

// TestRepackLeavesTheQuarantineAlone - a push in flight stages its objects in
// incoming-<id>/ inside the repository. A repack must not touch it.
func TestRepackLeavesTheQuarantineAlone(t *testing.T) {
	path, _ := repack_test_repository(t, 3)
	staging, err := git_quarantine(path)
	if err != nil {
		t.Fatalf("git_quarantine: %v", err)
	}
	staged := filepath.Join(staging, "objects", "ab", "cdef")
	if err := os.MkdirAll(filepath.Dir(staged), 0755); err != nil {
		t.Fatalf("staging directory: %v", err)
	}
	if err := os.WriteFile(staged, []byte("in flight"), 0644); err != nil {
		t.Fatalf("staged object: %v", err)
	}

	if err := git_repack(path); err != nil {
		t.Fatalf("git_repack: %v", err)
	}
	if _, err := os.Stat(staged); err != nil {
		t.Errorf("the repack disturbed a push's staged objects: %v", err)
	}
}

// TestRepackSurvivesACorruptObject - production carries 11 zero-headed objects
// from an interrupted write. They are unreachable, so a repack must pack what
// it can rather than refuse the repository for ever.
func TestRepackSurvivesACorruptObject(t *testing.T) {
	path, head := repack_test_repository(t, 5)
	corrupt := filepath.Join(path, "objects", "b0", "c409815d9a28e432d9d4591a927176d9dd207a")
	if err := os.MkdirAll(filepath.Dir(corrupt), 0755); err != nil {
		t.Fatalf("corrupt object directory: %v", err)
	}
	if err := os.WriteFile(corrupt, make([]byte, 4096), 0644); err != nil {
		t.Fatalf("corrupt object: %v", err)
	}

	if err := git_repack(path); err != nil {
		t.Fatalf("a corrupt unreachable object stopped the repack: %v", err)
	}
	repo, err := git.PlainOpen(path)
	if err != nil {
		t.Fatalf("PlainOpen: %v", err)
	}
	if _, err := repo.CommitObject(head); err != nil {
		t.Errorf("the repository is unreadable after repacking around a corrupt object: %v", err)
	}
}

// repack_test_empty makes an empty bare repository.
func repack_test_empty(t *testing.T) (string, *git.Repository) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "repository")
	repo, err := git.PlainInit(path, true)
	if err != nil {
		t.Fatalf("PlainInit: %v", err)
	}
	return path, repo
}

// repack_test_store encodes one object into the repository and answers its hash.
func repack_test_store(t *testing.T, repo *git.Repository, value interface {
	Encode(plumbing.EncodedObject) error
}) plumbing.Hash {
	t.Helper()
	encoded := repo.Storer.NewEncodedObject()
	if err := value.Encode(encoded); err != nil {
		t.Fatalf("encoding an object: %v", err)
	}
	hash, err := repo.Storer.SetEncodedObject(encoded)
	if err != nil {
		t.Fatalf("storing an object: %v", err)
	}
	return hash
}

// repack_test_commit stores a tree of these entries, sorted by name as git
// requires, under one commit on refs/heads/main.
func repack_test_commit(t *testing.T, repo *git.Repository, entries []object.TreeEntry) plumbing.Hash {
	t.Helper()
	tree := repack_test_store(t, repo, &object.Tree{Entries: entries})
	signature := object.Signature{Name: "Test", Email: "test@example.com", When: time.Now()}
	commit := repack_test_store(t, repo, &object.Commit{Author: signature, Committer: signature, Message: "commit\n", TreeHash: tree})
	if err := repo.Storer.SetReference(plumbing.NewHashReference("refs/heads/main", commit)); err != nil {
		t.Fatalf("SetReference: %v", err)
	}
	return commit
}

// repack_test_age backdates every loose object past git_prune_age, so an
// object the walk failed to count would be pruned rather than quietly kept.
func repack_test_age(t *testing.T, path string) {
	t.Helper()
	objects, err := filepath.Glob(filepath.Join(path, "objects", "??", "*"))
	if err != nil {
		t.Fatalf("globbing loose objects: %v", err)
	}
	old := time.Now().Add(-git_prune_age - time.Hour)
	for _, object := range objects {
		if err := os.Chtimes(object, old, old); err != nil {
			t.Fatalf("ageing %s: %v", object, err)
		}
	}
}

// repack_test_readable fails the test for any of these objects a fresh handle
// on the repository cannot read.
func repack_test_readable(t *testing.T, path string, hashes ...plumbing.Hash) {
	t.Helper()
	repo, err := git.PlainOpen(path)
	if err != nil {
		t.Fatalf("PlainOpen: %v", err)
	}
	for _, hash := range hashes {
		if _, err := repo.Storer.EncodedObject(plumbing.AnyObject, hash); err != nil {
			t.Errorf("object %s is gone: %v", hash, err)
		}
	}
}

// TestRepackEveryFileMode - a tree names a file as regular, executable or a
// symlink, and go-git's own walk handled only the first two: one symlink
// failed the repack of the whole repository with "unknown object". Every
// Mochi app repository holds a symlink.
func TestRepackEveryFileMode(t *testing.T) {
	path, repo := repack_test_empty(t)
	var entries []object.TreeEntry
	var blobs []plumbing.Hash
	for i, mode := range []filemode.FileMode{filemode.Regular, filemode.Executable, filemode.Symlink} {
		blob := repack_test_blob(t, repo, fmt.Sprintf("contents of file %d\n", i))
		entries = append(entries, object.TreeEntry{Name: fmt.Sprintf("file-%d", i), Mode: mode, Hash: blob})
		blobs = append(blobs, blob)
	}
	repack_test_commit(t, repo, entries)
	repack_test_age(t, path)

	if err := git_repack(path); err != nil {
		t.Fatalf("git_repack: %v", err)
	}
	if loose := git_loose_count(path); loose != 0 {
		t.Errorf("loose objects after repack: %d, want 0", loose)
	}
	repack_test_readable(t, path, blobs...)
}

// TestRepackSubmodule - a submodule entry names a commit in another repository,
// which this one does not hold. Following it fails the walk.
func TestRepackSubmodule(t *testing.T) {
	path, repo := repack_test_empty(t)
	blob := repack_test_blob(t, repo, "[submodule \"library\"]\n")
	absent := plumbing.NewHash("0123456789abcdef0123456789abcdef01234567")
	repack_test_commit(t, repo, []object.TreeEntry{
		{Name: ".gitmodules", Mode: filemode.Regular, Hash: blob},
		{Name: "library", Mode: filemode.Submodule, Hash: absent},
	})

	if err := git_repack(path); err != nil {
		t.Fatalf("git_repack: %v", err)
	}
	if loose := git_loose_count(path); loose != 0 {
		t.Errorf("loose objects after repack: %d, want 0", loose)
	}
}

// TestRepackTaggedBlob - a tag may name a blob directly, as git's own
// repository tags its maintainer's public key. Walking to a blob by any route
// other than a file entry failed go-git's walk.
func TestRepackTaggedBlob(t *testing.T) {
	path, repo := repack_test_empty(t)
	file := repack_test_blob(t, repo, "contents\n")
	repack_test_commit(t, repo, []object.TreeEntry{{Name: "file", Mode: filemode.Regular, Hash: file}})

	key := repack_test_blob(t, repo, "a public key\n")
	tagger := object.Signature{Name: "Test", Email: "test@example.com", When: time.Now()}
	tag := repack_test_store(t, repo, &object.Tag{Name: "key", Tagger: tagger, Message: "key\n", TargetType: plumbing.BlobObject, Target: key})
	if err := repo.Storer.SetReference(plumbing.NewHashReference("refs/tags/key", tag)); err != nil {
		t.Fatalf("SetReference: %v", err)
	}
	light := repack_test_blob(t, repo, "named by a reference alone\n")
	if err := repo.Storer.SetReference(plumbing.NewHashReference("refs/tags/light", light)); err != nil {
		t.Fatalf("SetReference: %v", err)
	}
	repack_test_age(t, path)

	if err := git_repack(path); err != nil {
		t.Fatalf("git_repack: %v", err)
	}
	if loose := git_loose_count(path); loose != 0 {
		t.Errorf("loose objects after repack: %d, want 0", loose)
	}
	repack_test_readable(t, path, key, tag, light)
}

// repack_test_counter counts the blobs read through it.
type repack_test_counter struct {
	storage.Storer
	blobs int
}

func (c *repack_test_counter) EncodedObject(kind plumbing.ObjectType, hash plumbing.Hash) (plumbing.EncodedObject, error) {
	found, err := c.Storer.EncodedObject(kind, hash)
	if err == nil && found.Type() == plumbing.BlobObject {
		c.blobs++
	}
	return found, err
}

// TestReachableReadsNoFiles - the walk counts a file's blob from its tree
// entry. Reading it instead would decompress every file in the history of
// every repository a push repacks.
func TestReachableReadsNoFiles(t *testing.T) {
	_, repo := repack_test_empty(t)
	var entries []object.TreeEntry
	for i, mode := range []filemode.FileMode{filemode.Regular, filemode.Executable, filemode.Symlink} {
		blob := repack_test_blob(t, repo, fmt.Sprintf("contents of file %d\n", i))
		entries = append(entries, object.TreeEntry{Name: fmt.Sprintf("file-%d", i), Mode: mode, Hash: blob})
	}
	repack_test_commit(t, repo, entries)

	counter := &repack_test_counter{Storer: repo.Storer}
	reachable, err := git_reachable(counter)
	if err != nil {
		t.Fatalf("git_reachable: %v", err)
	}
	if len(reachable) != 5 {
		t.Errorf("reachable objects: %d, want 5 - three blobs, a tree and a commit", len(reachable))
	}
	if counter.blobs != 0 {
		t.Errorf("the walk read %d file contents, want none", counter.blobs)
	}
}

// repack_test_spoiled is a repository whose packs fail to write: the first
// byte of each is corrupted, so closing one finds no valid pack to index.
type repack_test_spoiled struct {
	*filesystem.Storage
}

func (s repack_test_spoiled) PackfileWriter() (io.WriteCloser, error) {
	writer, err := s.Storage.PackfileWriter()
	if err != nil {
		return nil, err
	}
	return &repack_test_spoiler{WriteCloser: writer}, nil
}

type repack_test_spoiler struct {
	io.WriteCloser
	started bool
}

func (s *repack_test_spoiler) Write(data []byte) (int, error) {
	if !s.started && len(data) > 0 {
		s.started = true
		spoiled := append([]byte{}, data...)
		spoiled[0] ^= 0xff
		return s.WriteCloser.Write(spoiled)
	}
	return s.WriteCloser.Write(data)
}

// TestPackFailureLosesNothing - a pack is checked and indexed when it closes,
// and one that fails there never reaches objects/pack. Deleting the loose
// copies before that point, as go-git's own repack does, loses every object.
func TestPackFailureLosesNothing(t *testing.T) {
	path, _ := repack_test_repository(t, 5)
	repo, err := git.PlainOpen(path)
	if err != nil {
		t.Fatalf("PlainOpen: %v", err)
	}
	plain, ok := repo.Storer.(*filesystem.Storage)
	if !ok {
		t.Fatalf("setup: storage is %T, not the filesystem storer", repo.Storer)
	}
	reachable, err := git_reachable(plain)
	if err != nil {
		t.Fatalf("git_reachable: %v", err)
	}
	loose := git_loose_count(path)

	err = git_pack(repack_test_spoiled{Storage: plain}, reachable)
	if err == nil {
		t.Fatal("git_pack reported success for a pack that could not be written")
	}
	if after := git_loose_count(path); after != loose {
		t.Errorf("loose objects after a failed pack: %d, want %d", after, loose)
	}
	var hashes []plumbing.Hash
	for hash := range reachable {
		hashes = append(hashes, hash)
	}
	repack_test_readable(t, path, hashes...)
}

// TestRepackConsiderGates - when a push repacks and when it leaves well alone.
func TestRepackConsiderGates(t *testing.T) {
	minimum := git_repack_minimum
	dispatch := git_repack_dispatch
	defer func() {
		git_repack_minimum = minimum
		git_repack_dispatch = dispatch
	}()

	// The dispatch runs in a goroutine, so "nothing happened" has to be waited
	// for rather than read straight after the call - which passes by luck.
	done := make(chan struct{}, 8)
	var failure error
	git_repack_dispatch = func(string) error {
		done <- struct{}{}
		return failure
	}
	repacked := func(reason string) {
		t.Helper()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("no repack ran: %s", reason)
		}
	}
	quiet := func(reason string) {
		t.Helper()
		select {
		case <-done:
			t.Errorf("a repack ran when it should not have: %s", reason)
		case <-time.After(500 * time.Millisecond):
		}
	}

	path, _ := repack_test_repository(t, 5)
	git_repack_attempted.Delete(path)
	defer git_repack_remaining.Delete(path)

	// A repack records what it left once the dispatch returns, in the same
	// goroutine, so the next push has to wait for it to finish - or "quiet"
	// would only mean the repack was still running.
	settled := func() {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for {
			if _, running := git_repack_running.Load(path); !running {
				return
			}
			if time.Now().After(deadline) {
				t.Fatal("the repack never finished")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	push := func(label string) {
		t.Helper()
		repo, err := git.PlainOpen(path)
		if err != nil {
			t.Fatalf("PlainOpen: %v", err)
		}
		for i := 0; i < 3; i++ {
			repack_test_blob(t, repo, fmt.Sprintf("%s %d\n", label, i))
		}
	}

	// Under the threshold: nothing happens, however many pushes arrive.
	git_repack_minimum = 1000
	git_repack_consider(path)
	git_repack_consider(path)
	quiet("the repository holds far fewer loose objects than the threshold")

	// Over it: one repack.
	git_repack_minimum = 3
	git_repack_consider(path)
	repacked("the repository is over the threshold")

	// The next push does not repack again: the interval throttles it, so a
	// busy repository over the threshold does not repack per push.
	git_repack_consider(path)
	git_repack_consider(path)
	quiet("the interval since the last repack has not passed")

	// Once the interval has passed, a push that brought nothing new still
	// leaves it alone: what the last repack left loose - unreachable objects
	// too young to prune - another repack cannot reduce.
	settled()
	git_repack_attempted.Store(path, now()-git_repack_interval-1)
	git_repack_consider(path)
	quiet("nothing has arrived since the last repack")

	// Enough new objects on top of what it left bring it back.
	push("pushed after the repack")
	git_repack_consider(path)
	repacked("new objects arrived on top of what the last repack left")

	// A repack that fails says nothing about what the next one can do, so it
	// retries once the interval has passed, with nothing new arrived.
	settled()
	failure = errors.New("a corrupt object")
	push("pushed before a failing repack")
	git_repack_attempted.Store(path, now()-git_repack_interval-1)
	git_repack_consider(path)
	repacked("new objects arrived again")
	settled()
	failure = nil
	git_repack_attempted.Store(path, now()-git_repack_interval-1)
	git_repack_consider(path)
	repacked("the last repack failed, so only the interval holds it back")
	git_repack_attempted.Delete(path)
}

// TestRepackConsiderRunsOneAtATime - concurrent pushes to one repository must
// not start concurrent repacks of it.
func TestRepackConsiderRunsOneAtATime(t *testing.T) {
	minimum := git_repack_minimum
	dispatch := git_repack_dispatch
	defer func() {
		git_repack_minimum = minimum
		git_repack_dispatch = dispatch
	}()

	path, _ := repack_test_repository(t, 5)
	git_repack_attempted.Delete(path)
	defer git_repack_remaining.Delete(path)
	git_repack_minimum = 3

	release := make(chan struct{})
	var running, peak int
	var lock sync.Mutex
	git_repack_dispatch = func(string) error {
		lock.Lock()
		running++
		if running > peak {
			peak = running
		}
		lock.Unlock()
		<-release
		lock.Lock()
		running--
		lock.Unlock()
		return nil
	}

	for i := 0; i < 8; i++ {
		git_repack_attempted.Delete(path) // every caller passes the interval
		git_repack_consider(path)
	}
	time.Sleep(200 * time.Millisecond)
	close(release)

	lock.Lock()
	defer lock.Unlock()
	if peak > 1 {
		t.Errorf("%d repacks ran at once on one repository, want 1", peak)
	}
	if peak == 0 {
		t.Error("no repack ran at all, so the guard was not measured")
	}
	git_repack_attempted.Delete(path)
}

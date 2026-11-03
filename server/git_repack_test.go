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
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	stack "runtime/debug"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-git/go-billy/v5/osfs"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/cache"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/format/idxfile"
	"github.com/go-git/go-git/v5/plumbing/format/packfile"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/storage"
	"github.com/go-git/go-git/v5/storage/filesystem"
	"github.com/go-git/go-git/v5/storage/memory"
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

// packs reports how many live packfiles a repository holds: those a repack
// retired wait out their grace on disk, read by nothing new (git_packs).
func packs(t *testing.T, path string) int {
	t.Helper()
	return len(git_packs(path))
}

// repack_test_grace sets how long a retired pack waits before a sweep deletes
// it, for the length of a test.
func repack_test_grace(t *testing.T, grace time.Duration) {
	t.Helper()
	previous := git_pack_grace
	git_pack_grace = grace
	t.Cleanup(func() { git_pack_grace = previous })
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
	// Read as the server reads: the pack the second repack retired still
	// holds the orphans until its grace is over, and serves nothing new.
	after := git_kept(path)
	defer git_close(after)
	for _, hash := range orphans {
		if _, err := after.EncodedObject(plumbing.AnyObject, hash); err == nil {
			t.Errorf("orphaned object %s outlived git_prune_age", hash)
		}
	}
}

// repack_test_packs answers the base names - pack-<hash> - of a repository's
// live packs.
func repack_test_packs(t *testing.T, path string) []string {
	t.Helper()
	var names []string
	for _, match := range git_packs(path) {
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
	// The replaced pack goes at once, so what it leaves is swept with it.
	repack_test_grace(t, 0)
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
	// git wrote them beside its packs long before any repack here.
	old := time.Now().Add(-git_sweep_age - time.Minute)
	for _, orphan := range orphans {
		if err := os.Chtimes(orphan, old, old); err != nil {
			t.Fatalf("ageing %s: %v", orphan, err)
		}
	}

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
	reachable, _, err := git_reachable(counter)
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
	reachable, names, err := git_reachable(plain)
	if err != nil {
		t.Fatalf("git_reachable: %v", err)
	}
	loose := git_loose_count(path)

	err = git_pack(repack_test_spoiled{Storage: plain}, reachable, names, nil)
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

// repack_test_install puts a pack and its index into the repository, as
// git_keep does with a push.
func repack_test_install(t *testing.T, path string, data []byte) {
	t.Helper()
	name := repack_test_index(t, path, data)
	if err := os.WriteFile(name+".pack", data, 0444); err != nil {
		t.Fatal(err)
	}
}

// repack_test_index writes a pack's index into the repository, as git_keep
// does first, and answers the path both are named after.
func repack_test_index(t *testing.T, path string, data []byte) string {
	t.Helper()
	writer := new(idxfile.Writer)
	parser, err := packfile.NewParser(packfile.NewScanner(bytes.NewReader(data)), writer)
	if err != nil {
		t.Fatal(err)
	}
	checksum, err := parser.Parse()
	if err != nil {
		t.Fatalf("parsing the pack: %v", err)
	}
	index, err := writer.Index()
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Join(path, "objects", "pack", "pack-"+checksum.String())
	if err := os.MkdirAll(filepath.Dir(name), 0755); err != nil {
		t.Fatal(err)
	}
	encoded, err := os.Create(name + ".idx")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := idxfile.NewEncoder(encoded).Encode(index); err != nil {
		t.Fatal(err)
	}
	encoded.Close()
	return name
}

// repack_test_unpack parses a pack and answers the objects it holds.
func repack_test_unpack(t *testing.T, data []byte) *memory.Storage {
	t.Helper()
	read := memory.NewStorage()
	parser, err := packfile.NewParserWithStorage(packfile.NewScanner(bytes.NewReader(data)), read)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parser.Parse(); err != nil {
		t.Fatalf("the pack does not parse: %v", err)
	}
	return read
}

// An object held in two packs is read from whichever go-git finds first, and
// that choice varies from one read to the next. With A stored as a delta of B
// in one pack and B as a delta of A in the other, a clone or a repack could
// follow the two for ever: the stack overflowed, which no recover catches,
// and the server died. A branch deleted and pushed again stores them so.
func TestPackBreaksADeltaCycleAcrossPacks(t *testing.T) {
	// A cycle followed for ever dies at this limit rather than after a
	// gigabyte of stack.
	defer stack.SetMaxStack(stack.SetMaxStack(64 << 20))

	path, _ := repack_test_empty(t)
	build := memory.NewStorage()
	text := strings.Repeat("a line of a file that changes a little between versions\n", 300)
	blob := func(contents string) plumbing.Hash {
		encoded := build.NewEncodedObject()
		encoded.SetType(plumbing.BlobObject)
		writer, _ := encoded.Writer()
		io.WriteString(writer, contents)
		writer.Close()
		hash, err := build.SetEncodedObject(encoded)
		if err != nil {
			t.Fatal(err)
		}
		return hash
	}
	store := func(value interface {
		Encode(plumbing.EncodedObject) error
	}) plumbing.Hash {
		encoded := build.NewEncodedObject()
		if err := value.Encode(encoded); err != nil {
			t.Fatal(err)
		}
		hash, err := build.SetEncodedObject(encoded)
		if err != nil {
			t.Fatal(err)
		}
		return hash
	}
	a := blob(text + "the first ending\n")
	b := blob(text + "the second ending\n")
	tree := store(&object.Tree{Entries: []object.TreeEntry{
		{Name: "a.txt", Mode: filemode.Regular, Hash: a},
		{Name: "b.txt", Mode: filemode.Regular, Hash: b},
	}})
	signature := object.Signature{Name: "Test", Email: "test@example.com", When: time.Now()}
	commit := store(&object.Commit{Author: signature, Committer: signature, Message: "commit\n", TreeHash: tree})

	first, deltas, _, err := git_pack_thin(build, []plumbing.Hash{commit, tree, b, a}, map[plumbing.Hash]plumbing.Hash{a: b})
	if err != nil || deltas != 1 {
		t.Fatalf("the first pack: %d deltas, %v", deltas, err)
	}
	second, deltas, _, err := git_pack_thin(build, []plumbing.Hash{a, b}, map[plumbing.Hash]plumbing.Hash{b: a})
	if err != nil || deltas != 1 {
		t.Fatalf("the second pack: %d deltas, %v", deltas, err)
	}
	repack_test_install(t, path, first)
	repack_test_install(t, path, second)
	repo, err := git.PlainOpen(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Storer.SetReference(plumbing.NewHashReference("refs/heads/main", commit)); err != nil {
		t.Fatal(err)
	}

	want := map[plumbing.Hash]int64{}
	for _, hash := range []plumbing.Hash{commit, tree, a, b} {
		object, _ := build.EncodedObject(plumbing.AnyObject, hash)
		want[hash] = object.Size()
	}
	check := func(label string, read storer_reader) {
		for hash, size := range want {
			object, err := read.EncodedObject(plumbing.AnyObject, hash)
			if err != nil || object.Size() != size {
				t.Errorf("%s: object %s is missing or wrong (%v)", label, hash, err)
			}
		}
	}

	// Each read picks a pack afresh, so a clone meets the cycle one time in
	// four; forty make missing it unlikely past one in ten thousand.
	for i := 0; i < 40; i++ {
		storage := filesystem.NewStorage(osfs.New(path), cache.NewObjectLRUDefault())
		data, err := git_pack_plain(storage, []plumbing.Hash{commit, tree, a, b})
		if err != nil {
			t.Fatalf("clone %d: %v", i, err)
		}
		check(fmt.Sprintf("clone %d", i), repack_test_unpack(t, data))
	}

	if err := git_repack(path); err != nil {
		t.Fatalf("repack: %v", err)
	}
	repack_test_readable(t, path, commit, tree, a, b)
}

// storer_reader is what the checks above read objects through.
type storer_reader interface {
	EncodedObject(plumbing.ObjectType, plumbing.Hash) (plumbing.EncodedObject, error)
}

// A push writes its pack's index before the pack itself, so a pack is never
// seen without one; in between, the index is named after a pack that is not
// there. A repack ending at that moment swept it as an orphan, the pack then
// landed without it, and every read of the repository failed until the index
// was rebuilt by hand.
func TestPackSweepLeavesAPushArriving(t *testing.T) {
	path, _ := repack_test_repository(t, 3)
	if err := git_repack(path); err != nil {
		t.Fatalf("git_repack: %v", err)
	}

	build := memory.NewStorage()
	encoded := build.NewEncodedObject()
	encoded.SetType(plumbing.BlobObject)
	writer, _ := encoded.Writer()
	io.WriteString(writer, "pushed while a repack was ending\n")
	writer.Close()
	blob, err := build.SetEncodedObject(encoded)
	if err != nil {
		t.Fatal(err)
	}
	data, _, _, err := git_pack_thin(build, []plumbing.Hash{blob}, nil)
	if err != nil {
		t.Fatal(err)
	}

	name := repack_test_index(t, path, data)
	repo, err := git.PlainOpen(path)
	if err != nil {
		t.Fatalf("PlainOpen: %v", err)
	}
	if err := git_pack_sweep(repo.Storer); err != nil {
		t.Fatalf("git_pack_sweep: %v", err)
	}
	if err := os.WriteFile(name+".pack", data, 0444); err != nil {
		t.Fatal(err)
	}
	repack_test_readable(t, path, blob)
}

// A repository's first repack searches every object for deltas, and one of
// core's history took 1.4 GB inside the server process. Pushes to many
// repositories in a row, as pushing every repository does, started every
// repack at once; they now take turns, and every one still runs.
func TestRepackConsiderRunsOneAcrossRepositories(t *testing.T) {
	minimum := git_repack_minimum
	dispatch := git_repack_dispatch
	defer func() {
		git_repack_minimum = minimum
		git_repack_dispatch = dispatch
	}()
	git_repack_minimum = 3

	var paths []string
	for i := 0; i < 3; i++ {
		path, _ := repack_test_repository(t, 5)
		git_repack_attempted.Delete(path)
		paths = append(paths, path)
		defer git_repack_attempted.Delete(path)
		defer git_repack_remaining.Delete(path)
	}

	release := make(chan struct{})
	var running, peak, done int
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
		done++
		lock.Unlock()
		return nil
	}

	for _, path := range paths {
		git_repack_consider(path)
	}
	time.Sleep(200 * time.Millisecond)
	close(release)

	deadline := time.Now().Add(5 * time.Second)
	for {
		lock.Lock()
		finished := done
		lock.Unlock()
		if finished == len(paths) || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	lock.Lock()
	defer lock.Unlock()
	if peak != 1 {
		t.Errorf("%d repacks of different repositories ran at once, want 1", peak)
	}
	if done != len(paths) {
		t.Errorf("%d of %d repositories were repacked; a repack waiting its turn must still run", done, len(paths))
	}
}

// TestRepackAfterReadingAnUnsearchedRepository. A repository packed before
// the path search kept its larger pack, and its slow clones, until someone
// pushed to it again, since only a push considered a repack. A clone or fetch
// of one now starts it, through either protocol, under the same interval as a
// push; a clone of a repository already searched starts nothing.
func TestRepackAfterReadingAnUnsearchedRepository(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}
	dispatch := git_repack_dispatch
	defer func() { git_repack_dispatch = dispatch }()
	done := make(chan string, 8)
	git_repack_dispatch = func(path string) error {
		done <- path
		return nil
	}
	repacked := func(path, reason string) {
		t.Helper()
		select {
		case got := <-done:
			if got != path {
				t.Errorf("%s: %s was repacked, want %s", reason, got, path)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("THE POINT: no repack ran: %s", reason)
		}
	}
	quiet := func(reason string) {
		t.Helper()
		select {
		case got := <-done:
			t.Errorf("%s was repacked when it should not have been: %s", got, reason)
		case <-time.After(500 * time.Millisecond):
		}
	}
	settled := func(path string) {
		t.Helper()
		for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
			if _, running := git_repack_running.Load(path); !running {
				return
			}
			if time.Now().After(deadline) {
				t.Fatal("the repack never finished")
			}
		}
	}

	user, _ := create_git_test_env(t)
	searched, _ := git_negotiation_repo(t, user, "readsearched", 3)
	legacy, _ := git_negotiation_repo(t, user, "readlegacy", 3)
	// Pushes this small are stored loose; both are packed, the legacy one as
	// the repack packed before the path search, with no mark.
	for _, path := range []string{searched, legacy} {
		git_run(t, "", "-C", path, "repack", "-a", "-d", "-q")
	}
	git_run(t, "", "-C", legacy, "config", "--unset", "mochi.deltas")
	for _, path := range []string{searched, legacy} {
		git_repack_attempted.Delete(path)
		defer git_repack_attempted.Delete(path)
		defer git_repack_remaining.Delete(path)
	}

	clone := func(path, version string) {
		t.Helper()
		server := git_negotiation_server(t, path)
		git_run(t, "", git_protocol(version, "clone", "--quiet", "--bare", server.URL, t.TempDir())...)
	}

	clone(searched, "2")
	quiet("a clone of a repository already searched")

	for _, version := range git_versions {
		git_repack_attempted.Delete(legacy)
		clone(legacy, version)
		repacked(legacy, "a protocol v"+version+" clone of a repository packed before the path search")
		settled(legacy)
	}

	// The interval holds a read back as it holds a push.
	clone(legacy, "2")
	quiet("the interval since the last repack has not passed")
}

// repack_test_retiring packs a repository holding a blob larger than the 16 KB
// go-git keeps in memory, then tags a loose blob so the next repack replaces
// that pack. It answers the path, the large blob, its contents and the first
// pack.
func repack_test_retiring(t *testing.T) (string, plumbing.Hash, string, plumbing.Hash) {
	t.Helper()
	path, repo := repack_test_empty(t)
	var large strings.Builder
	for i := 0; large.Len() <= 64*1024; i++ {
		fmt.Fprintf(&large, "line %d of a file larger than go-git keeps in memory\n", i)
	}
	blob := repack_test_blob(t, repo, large.String())
	repack_test_commit(t, repo, []object.TreeEntry{{Name: "large", Mode: filemode.Regular, Hash: blob}})
	if err := git_repack(path); err != nil {
		t.Fatalf("first git_repack: %v", err)
	}
	first := repack_test_packs(t, path)
	if len(first) != 1 {
		t.Fatalf("setup: %d packs after the first repack, want 1", len(first))
	}
	tagged := repack_test_blob(t, repo, "tagged after the first repack\n")
	if err := repo.Storer.SetReference(plumbing.NewHashReference("refs/tags/new", tagged)); err != nil {
		t.Fatalf("SetReference: %v", err)
	}
	return path, blob, large.String(), plumbing.NewHash(strings.TrimPrefix(first[0], "pack-"))
}

// repack_test_retired answers the files of a pack, by its hash, with its mark.
func repack_test_retired(path string, pack plumbing.Hash) []string {
	base := filepath.Join(path, "objects", "pack", "pack-"+pack.String())
	return []string{base + ".pack", base + ".idx", base + ".retired"}
}

// TestRepackLeavesAFetchItsPack. go-git opens a pack lazily, and reads an
// object over 16 KB by opening the pack afresh by its path, even in a store
// holding its packs open as a fetch's does. A repack that deleted the packs it
// replaced at once failed a fetch that spanned it, part way through.
func TestRepackLeavesAFetchItsPack(t *testing.T) {
	path, blob, contents, _ := repack_test_retiring(t)

	store, err := (&git_loader{keep: true}).Load(&transport.Endpoint{Path: path})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	defer git_close(store)
	found, err := store.EncodedObject(plumbing.BlobObject, blob)
	if err != nil {
		t.Fatalf("setup: reading the large blob: %v", err)
	}

	if err := git_repack(path); err != nil {
		t.Fatalf("second git_repack: %v", err)
	}
	reader, err := found.Reader()
	if err != nil {
		t.Fatalf("THE POINT: a fetch begun before a repack could not read its object after it: %v", err)
	}
	defer reader.Close()
	if read, err := io.ReadAll(reader); err != nil || string(read) != contents {
		t.Errorf("THE POINT: a fetch begun before a repack read %d bytes of %d after it (%v)", len(read), len(contents), err)
	}
}

// TestRepackRetiredPackServesNothingNew. The pack a repack replaced stays on
// disk for the reads begun before it, and nothing begun after reads it: not a
// fetch's store, not a clone sent whole, not a repository opened to browse, not
// the next repack. Its files are spoiled here, so whatever reads it fails.
func TestRepackRetiredPackServesNothingNew(t *testing.T) {
	path, blob, contents, first := repack_test_retiring(t)
	if err := git_repack(path); err != nil {
		t.Fatalf("second git_repack: %v", err)
	}
	if count := git_pack_count(path); count != 1 {
		t.Errorf("THE POINT: the repository counts %d packs, want the live one alone", count)
	}

	// A fetch or a clone.
	store, err := (&git_loader{keep: true}).Load(&transport.Endpoint{Path: path})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	defer git_close(store)
	if stored := git_stored_open(store); stored == nil || len(stored.packs) != 1 {
		t.Error("THE POINT: a fetch copies stored entries from other than the one live pack")
	} else {
		stored.close()
	}

	files := repack_test_retired(path, first)
	for _, file := range files[:2] {
		if err := os.Chmod(file, 0644); err != nil {
			t.Fatalf("setup: the retired pack is not on disk for its grace: %v", err)
		}
		if err := os.WriteFile(file, []byte("spoiled"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	reachable, _, err := git_reachable(store.(*git_storage).Storer.(*filesystem.Storage))
	if err != nil {
		t.Fatalf("THE POINT: a fetch's store read the retired pack: %v", err)
	}
	var objects []plumbing.Hash
	for hash := range reachable {
		objects = append(objects, hash)
	}
	if whole := git_pack_whole(store, objects); whole == nil {
		t.Error("THE POINT: a clone was not sent the one live pack whole")
	} else {
		whole.Close()
	}

	// A repository opened to browse.
	kept := git_kept(path)
	defer git_close(kept)
	found, err := kept.EncodedObject(plumbing.BlobObject, blob)
	if err != nil {
		t.Fatalf("THE POINT: a browsing store read the retired pack: %v", err)
	}
	reader, err := found.Reader()
	if err != nil {
		t.Fatalf("reading the blob: %v", err)
	}
	read, _ := io.ReadAll(reader)
	reader.Close()
	if string(read) != contents {
		t.Errorf("THE POINT: a browsing store read %d bytes of the blob, want %d", len(read), len(contents))
	}

	// The next repack.
	if err := git_repack(path); err != nil {
		t.Errorf("THE POINT: the next repack read the retired pack: %v", err)
	}
}

// TestRetiredPackSweptAfterItsGrace. A retired pack is deleted once its grace
// is over and not before, by whichever comes first of a fetch, a push and a
// repack of its repository: the next repack may be hours away. The record is
// the mark on disk, so a sweep a crash cut short is finished by the next.
func TestRetiredPackSweptAfterItsGrace(t *testing.T) {
	repack_test_grace(t, time.Hour)
	for _, trigger := range []struct {
		name  string
		sweep func(string)
	}{
		{"fetch", git_repack_read},
		{"push", git_repack_consider},
		{"repack", func(path string) {
			if err := git_repack(path); err != nil {
				t.Errorf("git_repack: %v", err)
			}
		}},
	} {
		t.Run(trigger.name, func(t *testing.T) {
			path, _, _, first := repack_test_retiring(t)
			if err := git_repack(path); err != nil {
				t.Fatalf("second git_repack: %v", err)
			}
			git_repack_attempted.Store(path, now())
			defer git_repack_attempted.Delete(path)
			files := repack_test_retired(path, first)
			if _, err := os.Stat(files[0]); err != nil {
				t.Fatalf("THE POINT: the repack deleted the pack it retired inside its grace: %v", err)
			}
			// The pack was written long before its repack retired it, as git's
			// packs on the production server were: its files are not orphans
			// for the sweep of what a missing pack leaves.
			written := time.Now().Add(-git_sweep_age - time.Hour)
			for _, file := range files[:2] {
				if err := os.Chtimes(file, written, written); err != nil {
					t.Fatal(err)
				}
			}

			trigger.sweep(path)
			for _, file := range files {
				if _, err := os.Stat(file); err != nil {
					t.Errorf("THE POINT: %s went inside its grace: %v", filepath.Base(file), err)
				}
			}

			old := time.Now().Add(-time.Hour - time.Minute)
			if err := os.Chtimes(files[2], old, old); err != nil {
				t.Fatal(err)
			}
			trigger.sweep(path)
			for _, file := range files {
				if _, err := os.Stat(file); !os.IsNotExist(err) {
					t.Errorf("THE POINT: %s outlived its grace past a %s", filepath.Base(file), trigger.name)
				}
			}
			if count := len(git_packs(path)); count != 1 {
				t.Errorf("the sweep left %d live packs, want 1", count)
			}
		})
	}

	// A sweep that stopped after deleting the pack leaves its index and mark,
	// which the next deletes.
	path, _, _, first := repack_test_retiring(t)
	if err := git_repack(path); err != nil {
		t.Fatalf("second git_repack: %v", err)
	}
	files := repack_test_retired(path, first)
	old := time.Now().Add(-time.Hour - time.Minute)
	if err := os.Chtimes(files[2], old, old); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(files[0]); err != nil {
		t.Fatal(err)
	}
	if err := git_retired_sweep(path); err != nil {
		t.Fatalf("git_retired_sweep: %v", err)
	}
	for _, file := range files[1:] {
		if _, err := os.Stat(file); !os.IsNotExist(err) {
			t.Errorf("THE POINT: %s outlived a sweep cut short", filepath.Base(file))
		}
	}
}

// TestRetiredPackNameClaimedBack. A pack's name is its bytes' checksum, so a
// push or a repack can store a pack of the name of one retired and waiting out
// its grace. That pack is live again: reads see it, and no sweep may delete it
// from under the references that rely on it.
func TestRetiredPackNameClaimedBack(t *testing.T) {
	repack_test_grace(t, 0)
	live := func(t *testing.T, path string, pack plumbing.Hash) {
		t.Helper()
		if names := repack_test_packs(t, path); !slices.Contains(names, "pack-"+pack.String()) {
			t.Errorf("THE POINT: live packs %v, without the one stored again, pack-%s", names, pack)
		}
		git_pack_grace = 0
		if err := git_retired_sweep(path); err != nil {
			t.Fatalf("git_retired_sweep: %v", err)
		}
		files := repack_test_retired(path, pack)
		for _, file := range files[:2] {
			if _, err := os.Stat(file); err != nil {
				t.Errorf("THE POINT: the sweep deleted %s, stored again: %v", filepath.Base(file), err)
			}
		}
	}
	// A retired pack the sweep has not reached: the grace is long until the
	// pack is stored again, and over when live sweeps.
	retire := func(t *testing.T) (string, plumbing.Hash, []byte, *idxfile.MemoryIndex) {
		t.Helper()
		git_pack_grace = time.Hour
		path, _, _, first := repack_test_retiring(t)
		files := repack_test_retired(path, first)
		data, err := os.ReadFile(files[0])
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := os.ReadFile(files[1])
		if err != nil {
			t.Fatal(err)
		}
		index := idxfile.NewMemoryIndex()
		if err := idxfile.NewDecoder(bytes.NewReader(encoded)).Decode(index); err != nil {
			t.Fatal(err)
		}
		if err := git_repack(path); err != nil {
			t.Fatalf("second git_repack: %v", err)
		}
		if _, err := os.Stat(files[2]); err != nil {
			t.Fatalf("setup: the first pack was not retired: %v", err)
		}
		return path, first, data, index
	}
	push := func(t *testing.T, path string, data []byte, index *idxfile.MemoryIndex) {
		t.Helper()
		staging := t.TempDir()
		if err := os.WriteFile(filepath.Join(staging, git_push_pack), data, 0644); err != nil {
			t.Fatal(err)
		}
		if err := git_keep(staging, path, index); err != nil {
			t.Fatalf("git_keep: %v", err)
		}
	}

	t.Run("push", func(t *testing.T) {
		path, first, data, index := retire(t)
		push(t, path, data, index)
		live(t, path, first)
	})

	// A sweep cut short after the pack and its index, its mark left: the
	// push lands the pack anew, and the mark must not take it.
	t.Run("push after a sweep cut short", func(t *testing.T) {
		path, first, data, index := retire(t)
		files := repack_test_retired(path, first)
		for _, file := range files[:2] {
			if err := os.Remove(file); err != nil {
				t.Fatal(err)
			}
		}
		push(t, path, data, index)
		live(t, path, first)
	})

	// The tag gone, the references reach what the first repack packed, and
	// the third repack writes those bytes again.
	t.Run("repack", func(t *testing.T) {
		path, first, _, _ := retire(t)
		repo, err := git.PlainOpen(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := repo.Storer.RemoveReference("refs/tags/new"); err != nil {
			t.Fatal(err)
		}
		if err := git_repack(path); err != nil {
			t.Fatalf("third git_repack: %v", err)
		}
		if _, err := os.Stat(repack_test_retired(path, first)[0]); err != nil {
			t.Fatalf("setup: the third repack did not write the first pack's bytes again: %v", err)
		}
		live(t, path, first)
	})
}

// TestRepackKeepsTheStorageFigure. The owner's storage is walked at most once
// a minute, and a repack in that minute writes a pack the size of the
// repository while the packs it replaced wait out their grace: the figure
// must count what it writes, or the next push is handed room the repack
// filled. What it deletes comes off only when the walk counted it, so the
// figure never falls below what is stored.
func TestRepackKeepsTheStorageFigure(t *testing.T) {
	test_data_directory(t)
	repack_test_grace(t, time.Hour)
	user := &User{UID: "u-figure"}
	path := filepath.Join(user_storage_dir(user), "repositories", "repository")
	repo, err := git.PlainInit(path, true)
	if err != nil {
		t.Fatalf("PlainInit: %v", err)
	}
	compare := func(what string) {
		t.Helper()
		walked, err := dir_size(user_storage_dir(user))
		if err != nil {
			t.Fatal(err)
		}
		if figure := storage_test_figure(t, user.UID); figure != walked {
			t.Errorf("THE POINT: after %s the figure is %d bytes and a walk finds %d", what, figure, walked)
		}
	}

	// Before the walk: a pack holding an object a tag reached and no longer
	// does, a reachable object still loose, an unreachable one past the prune
	// age, and a file a pack that is gone left behind.
	var entries []object.TreeEntry
	for i := range 3 {
		entries = append(entries, object.TreeEntry{Name: fmt.Sprintf("file-%d", i), Mode: filemode.Regular, Hash: repack_test_blob(t, repo, fmt.Sprintf("contents %d\n", i))})
	}
	repack_test_commit(t, repo, entries)
	tagged := repack_test_blob(t, repo, "tagged, then untagged\n")
	if err := repo.Storer.SetReference(plumbing.NewHashReference("refs/tags/gone", tagged)); err != nil {
		t.Fatal(err)
	}
	if err := git_repack(path); err != nil {
		t.Fatalf("first git_repack: %v", err)
	}
	first := repack_test_packs(t, path)[0]
	if err := repo.Storer.RemoveReference("refs/tags/gone"); err != nil {
		t.Fatal(err)
	}
	entries = append(entries, object.TreeEntry{Name: "file-3", Mode: filemode.Regular, Hash: repack_test_blob(t, repo, "loose and reachable\n")})
	repack_test_commit(t, repo, entries)
	pruned := repack_test_blob(t, repo, "unreachable and old\n")
	orphan := filepath.Join(path, "objects", "pack", "pack-8585dad0e179053f14ffd5aca38eb6280d15d61b.rev")
	if err := os.WriteFile(orphan, []byte("left by git"), 0644); err != nil {
		t.Fatal(err)
	}
	walked := time.Now().Add(-time.Second)
	filepath.Walk(path, func(file string, information os.FileInfo, err error) error {
		if err == nil && !information.IsDir() {
			os.Chtimes(file, walked, walked)
		}
		return nil
	})
	old := time.Now().Add(-git_prune_age - time.Hour)
	if err := os.Chtimes(git_loose_path(path, pruned), old, old); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(orphan, old, old); err != nil {
		t.Fatal(err)
	}
	if _, err := user_storage_remaining(user); err != nil {
		t.Fatal(err)
	}

	// The repack writes a pack and an unreachable object loose, deletes the
	// loose copies it packed, the old unreachable object and the orphan, and
	// retires the first pack.
	if err := git_repack(path); err != nil {
		t.Fatalf("second git_repack: %v", err)
	}
	if _, err := os.Stat(git_loose_path(path, tagged)); err != nil {
		t.Fatalf("setup: the untagged object was not written out loose: %v", err)
	}
	if _, err := os.Stat(git_loose_path(path, pruned)); !os.IsNotExist(err) {
		t.Fatalf("setup: the old unreachable object was not pruned: %v", err)
	}
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatalf("setup: the orphan was not swept: %v", err)
	}
	compare("a repack")

	// An object written since the walk, where nothing charged it, then
	// packed: deleting its loose copy must not come off the figure.
	entries = append(entries, object.TreeEntry{Name: "file-4", Mode: filemode.Regular, Hash: repack_test_blob(t, repo, "written since the walk\n")})
	repack_test_commit(t, repo, entries)
	if err := git_repack(path); err != nil {
		t.Fatalf("third git_repack: %v", err)
	}
	compare("a repack of what nothing charged")

	// The first pack's grace is over; the second's is not.
	files := repack_test_retired(path, plumbing.NewHash(strings.TrimPrefix(first, "pack-")))
	if err := os.Chtimes(files[2], old, old); err != nil {
		t.Fatal(err)
	}
	if err := git_retired_sweep(path); err != nil {
		t.Fatalf("git_retired_sweep: %v", err)
	}
	if _, err := os.Stat(files[0]); !os.IsNotExist(err) {
		t.Fatalf("setup: the first pack was not swept: %v", err)
	}
	compare("a sweep")
}

// TestRetiredPackGraceOutlastsALongClone. A pack a repack replaced is read
// by a fetch begun before it for as long as the fetch runs, and nothing
// tracks the fetch: the grace is its bound, and must outlast a clone of a
// large repository over a slow link.
func TestRetiredPackGraceOutlastsALongClone(t *testing.T) {
	if git_pack_grace < time.Hour {
		t.Errorf("THE POINT: a retired pack is swept after %v, inside a long clone", git_pack_grace)
	}
}

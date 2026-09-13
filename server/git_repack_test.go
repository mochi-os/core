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
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
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
	git_repack_dispatch = func(string) error {
		done <- struct{}{}
		return nil
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

	// Once the interval has passed it may run again.
	git_repack_attempted.Store(path, now()-git_repack_interval-1)
	git_repack_consider(path)
	repacked("the interval has passed")
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

// Mochi server: packing a repository's loose tags
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
)

// tags_pack_env makes a repository with a branch, three lightweight tags and an
// annotated one, all loose, and returns its path, owner and tag targets.
func tags_pack_env(t *testing.T) (string, *User, map[plumbing.ReferenceName]plumbing.Hash) {
	t.Helper()
	id, owner, repo, _ := read_env(t, "tagspack")
	tree := read_snapshot(t, repo, map[string]read_file{"a.txt": read_text("a")})
	tip := read_chain(t, repo, tree, "main", 3)
	read_branch(t, repo, "main", tip)
	tags := map[plumbing.ReferenceName]plumbing.Hash{}
	for _, name := range []string{"v1", "v2", "release/v3"} {
		reference := plumbing.NewTagReferenceName(name)
		if err := repo.Storer.SetReference(plumbing.NewHashReference(reference, tip)); err != nil {
			t.Fatal(err)
		}
		tags[reference] = tip
	}
	annotated, err := repo.CreateTag("v4", tip, &git.CreateTagOptions{Message: "four", Tagger: &object.Signature{Name: "t", Email: "t@example.com", When: time.Unix(1700000000, 0)}})
	if err != nil {
		t.Fatal(err)
	}
	tags[annotated.Name()] = annotated.Hash()
	return git_repo_path(owner, test_app, id), owner, tags
}

// tags_loose_files lists the loose tag files under a repository.
func tags_loose_files(path string) []string {
	var files []string
	filepath.Walk(filepath.Join(path, "refs", "tags"), func(name string, information os.FileInfo, err error) error {
		if err == nil && !information.IsDir() {
			files = append(files, name)
		}
		return nil
	})
	return files
}

// Loose tags move into packed-refs and still resolve to what they named; the
// branch stays loose; git itself reads the result.
func TestTagsPackMovesLooseTags(t *testing.T) {
	path, _, tags := tags_pack_env(t)
	if git_tags_loose(path) != len(tags) {
		t.Fatalf("CONTROL: %d loose tags before packing, want %d", git_tags_loose(path), len(tags))
	}
	if err := git_tags_pack(path, git_repository_account(path)); err != nil {
		t.Fatalf("git_tags_pack: %v", err)
	}
	if loose := tags_loose_files(path); len(loose) != 0 {
		t.Errorf("THE POINT: loose tags left after packing: %v", loose)
	}
	if _, err := os.Stat(filepath.Join(path, "refs", "heads", "main")); err != nil {
		t.Errorf("the branch was packed too: only tags are packed, since branches have writers that hold no push lock")
	}
	repo, err := git.PlainOpen(path)
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range tags {
		reference, err := repo.Reference(name, false)
		if err != nil || reference.Hash() != want {
			t.Errorf("%s after packing = %v, %v; want %s", name, reference, err, want)
		}
	}
	if _, err := exec.LookPath("git"); err == nil {
		out, err := exec.Command("git", "-C", path, "for-each-ref", "--format=%(refname) %(objectname)", "refs/tags").CombinedOutput()
		if err != nil {
			t.Fatalf("git for-each-ref on the packed repository: %v: %s", err, out)
		}
		for name, want := range tags {
			if !strings.Contains(string(out), string(name)+" "+want.String()) {
				t.Errorf("git does not read %s as %s from the packed repository:\n%s", name, want, out)
			}
		}
	}
}

// A pack tries the owner's push lock rather than waiting for it: a push holds
// it for its whole receive, and a pack waiting there held the server's one
// repack slot. The tags stay loose and are packed next time.
func TestTagsPackSkipsWhileAPushRuns(t *testing.T) {
	path, owner, _ := tags_pack_env(t)
	lock := git_push_lock(owner.UID)
	lock.Lock()
	done := make(chan error, 1)
	go func() { done <- git_tags_pack(path, git_repository_account(path)) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("git_tags_pack: %v", err)
		}
	case <-time.After(2 * time.Second):
		lock.Unlock()
		t.Fatal("THE POINT: the pack waited for a push's lock, holding the server's repack slot")
	}
	if len(tags_loose_files(path)) == 0 {
		t.Error("the tags were packed while a push held the owner's lock: a tag the push was writing could be lost")
	}
	lock.Unlock()
	if err := git_tags_pack(path, git_repository_account(path)); err != nil {
		t.Fatalf("git_tags_pack after the push: %v", err)
	}
	if loose := tags_loose_files(path); len(loose) != 0 {
		t.Errorf("loose tags left once the lock was free: %v", loose)
	}
}

// Deleting a repository waits for the owner's push lock, which a pack holds,
// so a pack never recreates a repository deleted under it.
func TestRepositoryDeleteWaitsForThePushLock(t *testing.T) {
	id, owner, _, _ := read_env(t, "tagsdelete")
	lock := git_push_lock(owner.UID)
	lock.Lock()
	done := make(chan error, 1)
	go func() { done <- git_delete(owner, test_app, id) }()
	select {
	case <-done:
		lock.Unlock()
		t.Fatal("THE POINT: the repository was deleted while the owner's push lock was held")
	case <-time.After(300 * time.Millisecond):
	}
	lock.Unlock()
	if err := <-done; err != nil {
		t.Fatalf("git_delete: %v", err)
	}
}

// A dead rewrite's temporary is swept when old, and a fresh one is left.
func TestTagsPackSweepsOldTemporaries(t *testing.T) {
	path, _, _ := tags_pack_env(t)
	old, fresh := filepath.Join(path, "._packed-refs-old"), filepath.Join(path, "._packed-refs-new")
	for _, name := range []string{old, fresh} {
		if err := os.WriteFile(name, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	past := time.Now().Add(-time.Hour)
	os.Chtimes(old, past, past)
	if err := git_tags_pack(path, git_repository_account(path)); err != nil {
		t.Fatalf("git_tags_pack: %v", err)
	}
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Error("an hour-old packed-refs temporary survived the pack")
	}
	if _, err := os.Stat(fresh); err != nil {
		t.Error("a fresh packed-refs temporary, perhaps a rewrite under way, was removed")
	}
	if information, err := os.Stat(filepath.Join(path, "packed-refs")); err != nil || information.Mode().Perm() != 0o644 {
		t.Errorf("packed-refs after the pack: %v, %v; want 0644, as the loose tags were", information.Mode().Perm(), err)
	}
}

// A packed tag a push deletes stays deleted, and one pushed again after the
// pack names its new target: the loose file wins over the packed line.
func TestTagsPackedThenMovedOrDeleted(t *testing.T) {
	path, _, tags := tags_pack_env(t)
	if err := git_tags_pack(path, git_repository_account(path)); err != nil {
		t.Fatalf("git_tags_pack: %v", err)
	}
	repo, err := git.PlainOpen(path)
	if err != nil {
		t.Fatal(err)
	}
	head, err := repo.Reference(plumbing.NewBranchReferenceName("main"), true)
	if err != nil {
		t.Fatal(err)
	}
	parent, err := repo.CommitObject(head.Hash())
	if err != nil || parent.NumParents() == 0 {
		t.Fatalf("CONTROL: the branch tip has no parent: %v", err)
	}
	moved := parent.ParentHashes[0]
	if err := repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName("v1"), moved)); err != nil {
		t.Fatal(err)
	}
	if err := (&git_storage{Storer: repo.Storer, path: path}).RemoveReference(plumbing.NewTagReferenceName("v4")); err != nil {
		t.Fatal(err)
	}

	again, err := git.PlainOpen(path)
	if err != nil {
		t.Fatal(err)
	}
	if reference, err := again.Reference(plumbing.NewTagReferenceName("v1"), false); err != nil || reference.Hash() != moved {
		t.Errorf("a tag pushed again after packing = %v, %v; want its new target %s", reference, err, moved)
	}
	if _, err := again.Reference(plumbing.NewTagReferenceName("v4"), false); err == nil {
		t.Error("a packed tag that was deleted came back")
	}
	if reference, err := again.Reference(plumbing.NewTagReferenceName("v2"), false); err != nil || reference.Hash() != tags[plumbing.NewTagReferenceName("v2")] {
		t.Errorf("an untouched packed tag = %v, %v", reference, err)
	}
	if _, err := exec.LookPath("git"); err == nil {
		if out, err := exec.Command("git", "-C", path, "for-each-ref", "refs/tags").CombinedOutput(); err != nil {
			t.Errorf("git refuses the repository after a packed tag's removal: %v: %s", err, out)
		}
	}
}

// Loose tags past git_tags_minimum get packed after a push that brings too
// few objects for a repack, and the objects are left alone.
func TestRepackConsiderPacksTagsAlone(t *testing.T) {
	path, _, _ := tags_pack_env(t)
	repo, err := git.PlainOpen(path)
	if err != nil {
		t.Fatal(err)
	}
	head, err := repo.Reference(plumbing.NewBranchReferenceName("main"), true)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < git_tags_minimum; i++ {
		name := plumbing.NewTagReferenceName("many/" + strings.Repeat("x", 1) + string(rune('a'+i%26)) + string(rune('a'+i/26)))
		if err := repo.Storer.SetReference(plumbing.NewHashReference(name, head.Hash())); err != nil {
			t.Fatal(err)
		}
	}
	minimum, objects, tags := git_repack_minimum, git_repack_dispatch, git_tags_dispatch
	t.Cleanup(func() { git_repack_minimum, git_repack_dispatch, git_tags_dispatch = minimum, objects, tags })
	git_repack_minimum = 1 << 30
	repacked := make(chan struct{}, 1)
	packed := make(chan struct{}, 1)
	git_repack_dispatch = func(string) error { repacked <- struct{}{}; return nil }
	git_tags_dispatch = func(p string, account string) error {
		err := git_tags_pack(p, account)
		packed <- struct{}{}
		return err
	}
	git_repack_attempted.Delete(path)
	git_searched_mark(repo.Storer)
	t.Cleanup(func() { git_repack_attempted.Delete(path) })

	git_repack_consider(path)
	select {
	case <-packed:
	case <-time.After(5 * time.Second):
		t.Fatal("THE POINT: loose tags past the minimum were not packed after a push with too few objects for a repack")
	}
	select {
	case <-repacked:
		t.Error("the objects were repacked though there were too few of them")
	default:
	}
	if loose := tags_loose_files(path); len(loose) != 0 {
		t.Errorf("%d loose tags left after the pack", len(loose))
	}
}

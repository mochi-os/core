// Mochi server: branch listing tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/osfs"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	sl "go.starlark.net/starlark"
)

// heads_seen is a branch as both listings give it.
func heads_seen(refs []*plumbing.Reference) []string {
	var out []string
	for _, ref := range refs {
		out = append(out, fmt.Sprintf("%s %v %s %s", ref.Name(), ref.Type(), ref.Hash(), ref.Target()))
	}
	return out
}

// heads_reference is what go-git's own listing gives for the same store.
func heads_reference(t *testing.T, path string) []string {
	t.Helper()
	repo, err := git.PlainOpen(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	iter, err := repo.Branches()
	if err != nil {
		t.Fatalf("branches: %v", err)
	}
	var refs []*plumbing.Reference
	iter.ForEach(func(ref *plumbing.Reference) error {
		refs = append(refs, ref)
		return nil
	})
	return heads_seen(refs)
}

func heads_listed(t *testing.T, path string) []string {
	t.Helper()
	heads, err := git_heads(osfs.New(path))
	if err != nil {
		t.Fatalf("git_heads: %v", err)
	}
	return heads_seen(heads)
}

// heads_store makes a repository holding main, a nested branch, a symbolic
// branch and tags of both kinds, all loose.
func heads_store(t *testing.T, label string, tags int) (string, *git.Repository, *sl.Thread, string, plumbing.Hash, plumbing.Hash) {
	t.Helper()
	id, owner, repo, thread := read_env(t, label)
	base := read_commit(t, repo, nil, read_snapshot(t, repo, map[string]read_file{"a.txt": read_text("a\n")}), "base")
	head := read_commit(t, repo, []plumbing.Hash{base}, read_snapshot(t, repo, map[string]read_file{"a.txt": read_text("b\n")}), "head")
	read_branch(t, repo, "main", base)
	read_branch(t, repo, "feature/nested", head)
	read_branch(t, repo, "release", base)
	if err := repo.Storer.SetReference(plumbing.NewSymbolicReference("refs/heads/alias", "refs/heads/main")); err != nil {
		t.Fatalf("alias: %v", err)
	}
	for i := 0; i < tags; i++ {
		name := fmt.Sprintf("v%d", i)
		if i%2 == 0 {
			if err := repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName(name), base)); err != nil {
				t.Fatalf("tag: %v", err)
			}
			continue
		}
		tagger := &object.Signature{Name: "Test", Email: "test@example.com", When: time.Unix(1790000000, 0)}
		if _, err := repo.CreateTag(name, head, &git.CreateTagOptions{Tagger: tagger, Message: name}); err != nil {
			t.Fatalf("annotated tag: %v", err)
		}
	}
	return id, repo, thread, git_repo_path(owner, test_app, id), base, head
}

// TestGitHeadsListWhatBranchesLists. The heads-only listing gives go-git's
// answer, in its order, on a store of loose files, after git packs every ref
// (annotated tags' peeled lines included), and with files written since the
// pack: one overriding its packed line, one new.
func TestGitHeadsListWhatBranchesLists(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}
	_, repo, _, path, _, head := heads_store(t, "headssame", 6)

	loose := heads_listed(t, path)
	if want := heads_reference(t, path); !reflect.DeepEqual(loose, want) {
		t.Fatalf("loose store:\n got %v\nwant %v", loose, want)
	}
	if len(loose) != 4 {
		t.Fatalf("loose store lists %d branches, want 4: %v", len(loose), loose)
	}

	git_run(t, "", "-C", path, "pack-refs", "--all")
	if _, err := os.Stat(filepath.Join(path, "refs", "heads", "main")); !os.IsNotExist(err) {
		t.Fatalf("git left refs/heads/main loose: %v", err)
	}
	if packed, _ := os.ReadFile(filepath.Join(path, "packed-refs")); !strings.Contains(string(packed), "\n^") {
		t.Fatalf("packed-refs holds no peeled line to skip:\n%s", packed)
	}
	if got, want := heads_listed(t, path), heads_reference(t, path); !reflect.DeepEqual(got, want) {
		t.Fatalf("packed store:\n got %v\nwant %v", got, want)
	}

	// A push since the pack writes loose files again.
	read_branch(t, repo, "release", head)
	read_branch(t, repo, "topic", head)
	got, want := heads_listed(t, path), heads_reference(t, path)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("mixed store:\n got %v\nwant %v", got, want)
	}
	release := fmt.Sprintf("refs/heads/release hash-reference %s ", head)
	found := 0
	for _, line := range got {
		if strings.HasPrefix(line, "refs/heads/release ") {
			found++
			if line != release {
				t.Errorf("THE POINT: the file written since the pack wins over its packed line: %q", line)
			}
		}
	}
	if found != 1 {
		t.Errorf("release listed %d times: %v", found, got)
	}
}

// heads_counting records every path a listing reads.
type heads_counting struct {
	billy.Filesystem
	lock  sync.Mutex
	paths []string
}

func (c *heads_counting) note(path string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.paths = append(c.paths, filepath.ToSlash(path))
}

func (c *heads_counting) Open(path string) (billy.File, error) {
	c.note(path)
	return c.Filesystem.Open(path)
}

func (c *heads_counting) OpenFile(path string, flag int, mode os.FileMode) (billy.File, error) {
	c.note(path)
	return c.Filesystem.OpenFile(path, flag, mode)
}

func (c *heads_counting) ReadDir(path string) ([]os.FileInfo, error) {
	c.note(path)
	return c.Filesystem.ReadDir(path)
}

func (c *heads_counting) Stat(path string) (os.FileInfo, error) {
	c.note(path)
	return c.Filesystem.Stat(path)
}

// TestGitHeadsReadNoTag. A repository that tags every build holds thousands
// of loose tag files; listing its branches read every one of them.
func TestGitHeadsReadNoTag(t *testing.T) {
	_, _, _, path, _, _ := heads_store(t, "headscount", 200)
	if tags, _ := os.ReadDir(filepath.Join(path, "refs", "tags")); len(tags) != 200 {
		t.Fatalf("store holds %d loose tags, want 200", len(tags))
	}
	counting := &heads_counting{Filesystem: osfs.New(path)}
	heads, err := git_heads(counting)
	if err != nil {
		t.Fatalf("git_heads: %v", err)
	}
	if len(heads) != 4 {
		t.Fatalf("listed %d branches, want 4", len(heads))
	}
	for _, read := range counting.paths {
		if strings.HasPrefix(read, "refs/tags") || read == "refs" || read == "." || read == "" {
			t.Fatalf("THE POINT: the listing read %q beyond the branches", read)
		}
	}
	if len(counting.paths) > 8 {
		t.Errorf("the listing read %d paths for 4 branches: %v", len(counting.paths), counting.paths)
	}
}

// TestGitBranchesListABranchCaughtMidWrite. go-git rewrites a ref file in
// place, so a listing can find it empty: the branch then shows its packed
// value, where go-git's own walk failed the whole listing.
func TestGitBranchesListABranchCaughtMidWrite(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}
	id, _, thread, path, base, _ := heads_store(t, "headsempty", 2)
	git_run(t, "", "-C", path, "pack-refs", "--all")
	if err := os.WriteFile(filepath.Join(path, "refs", "heads", "main"), nil, 0644); err != nil {
		t.Fatalf("empty main: %v", err)
	}
	value, err := read_call(t, thread, api_git_branches, sl.String(id))
	if err != nil {
		t.Fatalf("THE POINT: a branch mid-write fails the whole listing: %v", err)
	}
	listed := map[string]string{}
	for _, branch := range sl_decode(value).([]any) {
		entry := branch.(map[string]any)
		listed[entry["name"].(string)] = entry["sha"].(string)
	}
	if listed["main"] != base.String() {
		t.Errorf("main lists %q, want its packed value %s: %v", listed["main"], base, listed)
	}
	if len(listed) != 4 {
		t.Errorf("listed %d branches, want 4: %v", len(listed), listed)
	}
}

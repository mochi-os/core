// Mochi server: git smart-HTTP shallow history.
//
// go-git's session refuses a shallow request and ignores the depth, so the walk
// that answers one is ours; without it `git clone --depth 1` fails outright
// rather than degrading to a full clone.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/transport"
)

// git_work drives a working copy for the fixtures below. Dates are fixed so
// --shallow-since has something deterministic to cut on.
type git_work struct {
	t    *testing.T
	dir  string
	when time.Time
}

func git_work_open(t *testing.T, name string) *git_work {
	t.Helper()
	dir := t.TempDir()
	t.Cleanup(func() { os.RemoveAll(dir) })
	work := &git_work{t: t, dir: dir, when: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
	work.run("init", "-b", "main")
	return work
}

func (w *git_work) run(args ...string) string {
	w.t.Helper()
	stamp := w.when.Format(time.RFC3339)
	cmd := exec.Command("git", args...)
	cmd.Dir = w.dir
	cmd.Env = append(os.Environ(),
		"GIT_AUTHOR_NAME=Test User", "GIT_AUTHOR_EMAIL=test@example.com",
		"GIT_COMMITTER_NAME=Test User", "GIT_COMMITTER_EMAIL=test@example.com",
		"GIT_AUTHOR_DATE="+stamp, "GIT_COMMITTER_DATE="+stamp,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		w.t.Fatalf("git %v: %s (%v)", args, out, err)
	}
	return strings.TrimSpace(string(out))
}

// commit adds one commit a day after the last, and returns its hash.
func (w *git_work) commit(name string) plumbing.Hash {
	w.t.Helper()
	w.when = w.when.AddDate(0, 0, 1)
	if err := os.WriteFile(filepath.Join(w.dir, name+".txt"), []byte(name), 0644); err != nil {
		w.t.Fatalf("write: %v", err)
	}
	w.run("add", ".")
	w.run("commit", "-m", name)
	return plumbing.NewHash(w.run("rev-parse", "HEAD"))
}

// git_shallow_repo builds a bare repository holding `commits` commits, an
// annotated tag on the second, and returns its path plus every commit hash
// oldest-first. The tag matters: a want can name a tag object rather than a
// commit, and the shallow walk has to peel it.
func git_shallow_repo(t *testing.T, user *User, repo_id string, commits int) (string, []plumbing.Hash) {
	t.Helper()
	if err := git_init(user, test_app, repo_id); err != nil {
		t.Fatalf("git_init: %v", err)
	}
	repo_path := git_repo_path(user, test_app, repo_id)

	work := git_work_open(t, "git_shallow_work")
	var hashes []plumbing.Hash
	for i := 0; i < commits; i++ {
		hashes = append(hashes, work.commit("commit"+strconv.Itoa(i)))
		if i == 1 {
			work.run("tag", "-a", "v1", "-m", "release one")
		}
	}
	work.run("push", repo_path, "main")
	work.run("push", repo_path, "v1")
	return repo_path, hashes
}

// git_clone_depth returns the number of commits reachable in a clone, and
// whether git considers it shallow.
func git_clone_state(t *testing.T, dir string) (int, bool) {
	t.Helper()
	count, err := strconv.Atoi(strings.TrimSpace(git_run(t, dir, "-C", dir, "rev-list", "--count", "HEAD")))
	if err != nil {
		t.Fatalf("rev-list --count: %v", err)
	}
	shallow := strings.TrimSpace(git_run(t, dir, "-C", dir, "rev-parse", "--is-shallow-repository"))
	return count, shallow == "true"
}

func git_temporary(t *testing.T, name string) string {
	t.Helper()
	dir := t.TempDir()
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// TestGitCloneDepth — the regression. --depth used to fail outright; now each
// depth has to produce exactly that much history, and a valid repository.
func TestGitCloneDepth(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}

	user, _ := create_git_test_env(t)

	repo_path, _ := git_shallow_repo(t, user, "depth", 8)
	server := git_negotiation_server(t, repo_path)

	full := git_temporary(t, "git_depth_full")
	git_run(t, "", "clone", "--quiet", server.URL, full)
	full_objects := git_client_objects(t, full)
	if count, shallow := git_clone_state(t, full); count != 8 || shallow {
		t.Fatalf("baseline clone has %d commits shallow=%v, want 8 and not shallow", count, shallow)
	}

	for _, version := range git_versions {
		for _, depth := range []int{1, 2, 5} {
			t.Run(fmt.Sprintf("protocol v%s depth %d", version, depth), func(t *testing.T) {
				dir := git_temporary(t, "git_depth")
				git_run(t, "", git_protocol(version, "clone", "--quiet", "--depth", strconv.Itoa(depth), server.URL, dir)...)

				count, shallow := git_clone_state(t, dir)
				if count != depth {
					t.Errorf("--depth %d produced %d commits, want %d", depth, count, depth)
				}
				if !shallow {
					t.Errorf("--depth %d produced a repository git does not consider shallow", depth)
				}
				if objects := git_client_objects(t, dir); objects >= full_objects {
					t.Errorf("--depth %d transferred %d objects against a full clone of %d: the depth bought nothing",
						depth, objects, full_objects)
				}
				git_run(t, dir, "-C", dir, "fsck", "--strict")
			})
		}
	}
}

// TestGitFetchInShallowClone — the follow-on case. A shallow clone that fetches
// later must be told only what it lacks, and the server must not assume it
// holds anything behind its own boundary.
func TestGitFetchInShallowClone(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}

	user, _ := create_git_test_env(t)

	for _, version := range git_versions {
		t.Run("protocol v"+version, func(t *testing.T) {
			repo_path, _ := git_shallow_repo(t, user, "shallowfetch"+version, 6)
			server := git_negotiation_server(t, repo_path)

			dir := git_temporary(t, "git_shallow_fetch")
			git_run(t, "", git_protocol(version, "clone", "--quiet", "--depth", "1", server.URL, dir)...)
			if count, shallow := git_clone_state(t, dir); count != 1 || !shallow {
				t.Fatalf("clone --depth 1 gave %d commits shallow=%v", count, shallow)
			}

			// Two more commits land on the server.
			work := git_work_open(t, "git_shallow_more")
			work.run("remote", "add", "origin", repo_path)
			work.run("fetch", "--quiet", "origin", "main")
			work.run("checkout", "-q", "-b", "main", "origin/main")
			work.commit("seven")
			work.commit("eight")
			work.run("push", repo_path, "main")

			before := git_client_objects(t, dir)
			git_run(t, dir, git_protocol(version, "-C", dir, "fetch", "--quiet", "--depth", "1", "origin", "main")...)
			after := git_client_objects(t, dir)

			git_run(t, dir, "-C", dir, "fsck", "--strict")
			if _, shallow := git_clone_state(t, dir); !shallow {
				t.Error("a --depth 1 fetch un-shallowed the repository")
			}
			t.Logf("shallow fetch of two new commits transferred %d objects", after-before)
			if after-before > 12 {
				t.Errorf("a shallow fetch of two commits transferred %d objects: it is not honouring the boundary", after-before)
			}
		})
	}
}

// TestGitFetchUnshallow — `git fetch --unshallow` arrives as a depth of
// 2147483647 and has to produce the complete history plus an unshallow line for
// every commit that was previously a boundary.
func TestGitFetchUnshallow(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}

	user, _ := create_git_test_env(t)

	repo_path, _ := git_shallow_repo(t, user, "unshallow", 6)
	server := git_negotiation_server(t, repo_path)

	for _, version := range git_versions {
		t.Run("protocol v"+version, func(t *testing.T) {
			dir := git_temporary(t, "git_unshallow")
			git_run(t, "", git_protocol(version, "clone", "--quiet", "--depth", "2", server.URL, dir)...)
			if count, shallow := git_clone_state(t, dir); count != 2 || !shallow {
				t.Fatalf("clone --depth 2 gave %d commits shallow=%v", count, shallow)
			}

			git_run(t, dir, git_protocol(version, "-C", dir, "fetch", "--quiet", "--unshallow")...)

			count, shallow := git_clone_state(t, dir)
			if count != 6 {
				t.Errorf("--unshallow left %d commits, want the whole history of 6", count)
			}
			if shallow {
				t.Error("--unshallow left the repository still marked shallow")
			}
			git_run(t, dir, "-C", dir, "fsck", "--strict")
		})
	}
}

// TestGitFetchDeepen — `git fetch --deepen=N` sends deepen-relative, so the
// depth counts from where the client's history stops rather than from the tip.
func TestGitFetchDeepen(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}

	user, _ := create_git_test_env(t)

	repo_path, _ := git_shallow_repo(t, user, "deepen", 8)
	server := git_negotiation_server(t, repo_path)

	for _, version := range git_versions {
		t.Run("protocol v"+version, func(t *testing.T) {
			dir := git_temporary(t, "git_deepen")
			git_run(t, "", git_protocol(version, "clone", "--quiet", "--depth", "1", server.URL, dir)...)

			git_run(t, dir, git_protocol(version, "-C", dir, "fetch", "--quiet", "--deepen", "2")...)
			count, shallow := git_clone_state(t, dir)
			if count != 3 {
				t.Errorf("--deepen 2 from a depth of 1 left %d commits, want 3", count)
			}
			if !shallow {
				t.Error("--deepen 2 un-shallowed a repository with history still to go")
			}

			git_run(t, dir, git_protocol(version, "-C", dir, "fetch", "--quiet", "--deepen", "2")...)
			if count, _ := git_clone_state(t, dir); count != 5 {
				t.Errorf("a second --deepen 2 left %d commits, want 5", count)
			}
			git_run(t, dir, "-C", dir, "fsck", "--strict")
		})
	}
}

// TestGitCloneShallowSince — --shallow-since arrives as deepen-since, cutting on
// commit date rather than on a count.
func TestGitCloneShallowSince(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}

	user, _ := create_git_test_env(t)

	// The fixture commits one a day from 2026-01-02.
	repo_path, _ := git_shallow_repo(t, user, "since", 8)
	server := git_negotiation_server(t, repo_path)

	for _, version := range git_versions {
		t.Run("protocol v"+version, func(t *testing.T) {
			dir := git_temporary(t, "git_since")
			// The cutoff carries an explicit time and zone: git's approxidate fills
			// anything unspecified from the current clock, so a bare date cuts at
			// today's time of day and the answer moves through the day.
			git_run(t, "", git_protocol(version, "clone", "--quiet", "--shallow-since", "2026-01-07T00:00:00+0000", server.URL, dir)...)

			count, shallow := git_clone_state(t, dir)
			if !shallow {
				t.Error("--shallow-since produced a repository git does not consider shallow")
			}
			// Commits are dated 12:00Z on 2026-01-02 through 2026-01-09, so
			// the three from the 7th onwards survive the cut.
			if count != 3 {
				t.Errorf("--shallow-since 2026-01-07T00:00:00Z kept %d commits, want the 3 dated on or after it", count)
			}
			git_run(t, dir, "-C", dir, "fsck", "--strict")
		})
	}
}

// TestGitCloneShallowExclude — --shallow-exclude arrives as deepen-not naming a
// ref, and cuts where that ref's history begins.
func TestGitCloneShallowExclude(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}

	user, _ := create_git_test_env(t)

	// The annotated tag v1 sits on the second of eight commits, so excluding
	// it leaves the six after it.
	repo_path, _ := git_shallow_repo(t, user, "exclude", 8)
	server := git_negotiation_server(t, repo_path)

	for _, version := range git_versions {
		t.Run("protocol v"+version, func(t *testing.T) {
			dir := git_temporary(t, "git_exclude")
			git_run(t, "", git_protocol(version, "clone", "--quiet", "--shallow-exclude", "v1", server.URL, dir)...)

			count, shallow := git_clone_state(t, dir)
			if !shallow {
				t.Error("--shallow-exclude produced a repository git does not consider shallow")
			}
			if count != 6 {
				t.Errorf("--shallow-exclude v1 kept %d commits, want the 6 after the tag", count)
			}
			git_run(t, dir, "-C", dir, "fsck", "--strict")
		})
	}
}

// TestGitHistoryBoundary — the walk itself, on a diamond, where the boundary is
// not simply "the oldest commit reached".
//
//	  D        depth 1: {D},       boundary {D}
//	 / \       depth 2: {D,B,C},   boundary {B,C}
//	B   C      depth 3: {D,B,C,A}, boundary {} - A is a root, and a root is
//	 \ /                            not shallow, it is complete.
//	  A
func TestGitHistoryBoundary(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}

	user, _ := create_git_test_env(t)

	if err := git_init(user, test_app, "diamond"); err != nil {
		t.Fatalf("git_init: %v", err)
	}
	repo_path := git_repo_path(user, test_app, "diamond")

	work := git_work_open(t, "git_diamond")
	a := work.commit("a")
	b := work.commit("b")
	work.run("checkout", "-q", "-b", "side", a.String())
	c := work.commit("c")
	work.run("checkout", "-q", "main")
	work.run("merge", "--no-ff", "-m", "d", "side")
	d := plumbing.NewHash(work.run("rev-parse", "HEAD"))
	work.run("push", repo_path, "main")

	storage, err := (&git_loader{}).Load(&transport.Endpoint{Path: repo_path})
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	for _, expected := range []struct {
		depth    int
		reached  []plumbing.Hash
		boundary []plumbing.Hash
	}{
		{1, []plumbing.Hash{d}, []plumbing.Hash{d}},
		{2, []plumbing.Hash{d, b, c}, []plumbing.Hash{b, c}},
		{3, []plumbing.Hash{d, b, c, a}, nil},
	} {
		reached, boundary, err := git_history(storage, []plumbing.Hash{d}, nil, git_depth_limit(expected.depth))
		if err != nil {
			t.Fatalf("depth %d: %v", expected.depth, err)
		}
		if !git_same_hashes(reached, expected.reached) {
			t.Errorf("depth %d reached %v, want %v", expected.depth, reached, expected.reached)
		}
		if !git_same_hashes(boundary, expected.boundary) {
			t.Errorf("depth %d boundary %v, want %v", expected.depth, boundary, expected.boundary)
		}
	}

	// A commit named as cut is a graft root: reached, but nothing behind it,
	// and shallow because its parents were withheld.
	reached, boundary, err := git_history(storage, []plumbing.Hash{d}, git_hash_set([]plumbing.Hash{b}), nil)
	if err != nil {
		t.Fatalf("cut walk: %v", err)
	}
	if !git_same_hashes(reached, []plumbing.Hash{d, b, c, a}) {
		t.Errorf("cutting at B reached %v; A is still reachable through C", reached)
	}
	if !git_same_hashes(boundary, nil) {
		t.Errorf("cutting at B gave boundary %v; A arrives through C, so nothing is missing", boundary)
	}
}

func git_same_hashes(got, want []plumbing.Hash) bool {
	if len(got) != len(want) {
		return false
	}
	seen := git_hash_set(got)
	for _, hash := range want {
		if !seen[hash] {
			return false
		}
	}
	return true
}

// Mochi server: git smart-HTTP fetch negotiation.
//
// A client offers what it holds as "have" lines and the pack must exclude them.
// These count objects rather than inspecting protocol lines, because the
// protocol looked correct throughout while the pack did not.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/format/packfile"
	"github.com/go-git/go-git/v5/plumbing/format/pktline"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp/capability"
	"github.com/go-git/go-git/v5/plumbing/revlist"
	"github.com/go-git/go-git/v5/storage/memory"
)

// git_negotiation_repo builds a bare repository holding `commits` commits and
// returns its path plus every commit hash oldest-first.
func git_negotiation_repo(t *testing.T, user *User, repo_id string, commits int) (string, []plumbing.Hash) {
	t.Helper()
	if err := git_init(user, test_app, repo_id); err != nil {
		t.Fatalf("git_init: %v", err)
	}
	repo_path := git_repo_path(user, test_app, repo_id)

	work := t.TempDir()

	run := func(args ...string) {
		cmd := exec.Command("git", args...)
		cmd.Dir = work
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=Test User", "GIT_AUTHOR_EMAIL=test@example.com",
			"GIT_COMMITTER_NAME=Test User", "GIT_COMMITTER_EMAIL=test@example.com",
		)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %s (%v)", args, out, err)
		}
	}

	run("init", "-b", "main")
	var hashes []plumbing.Hash
	for i := 0; i < commits; i++ {
		// A distinct file per commit, so each adds its own blob and tree and
		// the object counts below are meaningfully different per commit.
		name := fmt.Sprintf("file%d.txt", i)
		if err := os.WriteFile(filepath.Join(work, name), []byte(strings.Repeat("x", 64+i)), 0644); err != nil {
			t.Fatalf("write: %v", err)
		}
		run("add", name)
		run("commit", "-m", fmt.Sprintf("commit %d", i))
		out, err := exec.Command("git", "-C", work, "rev-parse", "HEAD").Output()
		if err != nil {
			t.Fatalf("rev-parse: %v", err)
		}
		hashes = append(hashes, plumbing.NewHash(strings.TrimSpace(string(out))))
	}
	run("push", repo_path, "main")
	return repo_path, hashes
}

// git_test_mode puts gin in test mode exactly once. SetMode writes package
// level state in gin, so calling it per request is a data race the moment two
// fetches run at the same time - which the concurrency tests do deliberately.
var git_test_mode sync.Once

func git_gin_mode() {
	git_test_mode.Do(func() { gin.SetMode(gin.TestMode) })
}

// git_negotiation_fetch drives git_upload_pack the way a stateless client
// does: one want, the given haves, then done. Returns the number of objects in
// the packfile the server produced.
func git_negotiation_fetch(t *testing.T, repo_path string, want plumbing.Hash, haves []plumbing.Hash) int {
	t.Helper()
	return git_negotiation_pack_objects(t, git_negotiation_response(t, repo_path, want, haves))
}

// git_negotiation_response is the same request, returning the whole response
// body rather than a count. Capabilities ride on the first want line, which is
// where a real client puts them, so a test can ask for a side band or a thin
// pack and inspect what comes back.
func git_negotiation_response(t *testing.T, repo_path string, want plumbing.Hash, haves []plumbing.Hash, capabilities ...string) []byte {
	t.Helper()
	return git_negotiation_round(t, repo_path, want, haves, true, capabilities...)
}

// git_negotiation_round is the same, with control over whether the request ends
// in "done". Without it the request is an exploratory round: the client is
// still discovering common history and expects acknowledgements and no
// packfile, which is a different response shape entirely.
func git_negotiation_round(t *testing.T, repo_path string, want plumbing.Hash, haves []plumbing.Hash, done bool, capabilities ...string) []byte {
	t.Helper()

	first := "want " + want.String()
	if len(capabilities) > 0 {
		first += " " + strings.Join(capabilities, " ")
	}

	var body bytes.Buffer
	encoder := pktline.NewEncoder(&body)
	if err := encoder.Encodef("%s\n", first); err != nil {
		t.Fatalf("encode want: %v", err)
	}
	if err := encoder.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	for _, have := range haves {
		if err := encoder.Encodef("have %s\n", have.String()); err != nil {
			t.Fatalf("encode have: %v", err)
		}
	}
	if done {
		if err := encoder.Encodef("done\n"); err != nil {
			t.Fatalf("encode done: %v", err)
		}
	} else if err := encoder.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	git_gin_mode()
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest("POST", "/x/git/git-upload-pack", bytes.NewReader(body.Bytes()))

	if !git_upload_pack(c, repo_path, io.NopCloser(bytes.NewReader(body.Bytes()))) {
		t.Fatal("git_upload_pack did not handle the request")
	}
	if recorder.Code != 200 {
		t.Fatalf("status = %d, body = %q", recorder.Code, recorder.Body.String())
	}

	return recorder.Body.Bytes()
}

// git_negotiation_pack_objects counts the objects in an upload-pack response,
// skipping the leading NAK packet line that precedes the pack.
func git_negotiation_pack_objects(t *testing.T, response []byte) int {
	t.Helper()
	index := bytes.Index(response, []byte("PACK"))
	if index < 0 {
		t.Fatalf("no packfile in the response: %q", truncate_for_test(string(response)))
	}
	scanner := packfile.NewScanner(bytes.NewReader(response[index:]))
	_, objects, err := scanner.Header()
	if err != nil {
		t.Fatalf("reading pack header: %v", err)
	}
	return int(objects)
}

func truncate_for_test(s string) string {
	if len(s) > 200 {
		return s[:200] + "..."
	}
	return s
}

// TestGitUploadPackExcludesHaves - a client one commit behind must be sent that
// commit's objects, not the repository. The whole-repository figure is measured
// in the same test rather than guessed.
func TestGitUploadPackExcludesHaves(t *testing.T) {
	user, _ := create_git_test_env(t)

	repo_path, commits := git_negotiation_repo(t, user, "negotiation", 8)
	head := commits[len(commits)-1]

	// Baseline: a clone with nothing gets everything.
	full := git_negotiation_fetch(t, repo_path, head, nil)
	if full < 8 {
		t.Fatalf("baseline pack has %d objects, expected at least one per commit — the fixture is wrong", full)
	}

	// One commit behind: offer every commit but the last.
	behind := git_negotiation_fetch(t, repo_path, head, commits[:len(commits)-1])

	t.Logf("whole repository: %d objects; one commit behind: %d objects", full, behind)

	if behind >= full {
		t.Errorf("a client one commit behind was sent %d objects and a client with nothing was sent %d: "+
			"the haves excluded nothing, so every fetch ships the whole repository", behind, full)
	}
	// One commit adds a commit, a tree and a blob. Allow headroom for the
	// root tree, but this must be nowhere near the full figure.
	if behind > 6 {
		t.Errorf("one commit behind produced %d objects, want a handful (commit + tree + blob); full clone is %d",
			behind, full)
	}
}

// TestGitUploadPackExcludesMostHistory — the shape seen in production: a client
// a few commits behind a longer history. Guards against a fix that only works
// for the single-commit case.
func TestGitUploadPackExcludesMostHistory(t *testing.T) {
	user, _ := create_git_test_env(t)

	repo_path, commits := git_negotiation_repo(t, user, "negotiation2", 20)
	head := commits[len(commits)-1]

	full := git_negotiation_fetch(t, repo_path, head, nil)
	behind := git_negotiation_fetch(t, repo_path, head, commits[:len(commits)-3])

	t.Logf("whole repository: %d objects; three commits behind: %d objects", full, behind)

	if behind >= full/2 {
		t.Errorf("three commits behind a 20-commit history produced %d objects against a full clone of %d: "+
			"the pack is not excluding the common history", behind, full)
	}
}

// TestGitFetchSelectsWhatTheClientLacks. A fetch walks the new history and
// where it meets the client's, as git does, rather than everything the client
// holds, which cost a fetch of one commit the whole history read twice. What
// it sends must still hold every object the client lacks and nothing the
// wants do not reach, and where the new history meets the client's at its
// parents it is exactly what the client lacks: across a merge, a side branch,
// a commit dated before its parent, an annotated tag, and a tag of a tree. A
// revert to a version only older history holds is sent again, as git sends
// it.
func TestGitFetchSelectsWhatTheClientLacks(t *testing.T) {
	_, repo, _ := read_env_repo(t, "lacks")
	snapshot := func(files map[string]string) plumbing.Hash {
		entries := map[string]read_file{}
		for path, text := range files {
			entries[path] = read_text(text)
		}
		return read_snapshot(t, repo, entries)
	}
	commit := func(message string, files map[string]string, parents ...plumbing.Hash) plumbing.Hash {
		return read_commit(t, repo, parents, snapshot(files), message)
	}
	tag := func(name string, target plumbing.Hash, kind plumbing.ObjectType) plumbing.Hash {
		encoded := repo.Storer.NewEncodedObject()
		value := &object.Tag{Name: name, Tagger: object.Signature{Name: "Reader", Email: "reader@example.com", When: time.Unix(1700000000, 0)}, Message: name, TargetType: kind, Target: target}
		if err := value.Encode(encoded); err != nil {
			t.Fatalf("encode tag: %v", err)
		}
		hash, err := repo.Storer.SetEncodedObject(encoded)
		if err != nil {
			t.Fatalf("store tag: %v", err)
		}
		return hash
	}

	root := commit("root", map[string]string{"a": "a1", "b": "b1", "lib/c": "c1"})
	main1 := commit("main1", map[string]string{"a": "a2", "b": "b1", "lib/c": "c1"}, root)
	side1 := commit("side1", map[string]string{"a": "a1", "b": "b2", "lib/c": "c1"}, root)
	side2 := commit("side2", map[string]string{"a": "a1", "b": "b2", "lib/c": "c1", "lib/d": "d1"}, side1)
	merge := commit("merge", map[string]string{"a": "a2", "b": "b2", "lib/c": "c1", "lib/d": "d1"}, main1, side2)
	revert := commit("revert", map[string]string{"a": "a1", "b": "b2", "lib/c": "c1", "lib/d": "d1", "r": "r1"}, merge)
	// Dated before its own parent, as a wrong clock writes it.
	read_clock -= 5
	skewed := commit("skewed", map[string]string{"a": "a1", "b": "b2", "lib/c": "c1", "lib/d": "d1", "r": "r1", "e": "e1"}, revert)
	read_clock += 10
	tip := commit("tip", map[string]string{"a": "a1", "b": "b2", "lib/c": "c1", "lib/d": "d1", "r": "r1", "e": "e1", "f": "f1"}, skewed)
	annotated := tag("release", tip, plumbing.CommitObject)
	chain := annotated
	for i := 0; i < 20; i++ {
		chain = tag(fmt.Sprintf("chain%d", i), chain, plumbing.TagObject)
	}
	loose := tag("loose", snapshot(map[string]string{"g": "g1"}), plumbing.TreeObject)

	// The blob a revert brings back: held only in the history behind the
	// commit the client holds.
	old, err := object.GetBlob(repo.Storer, plumbing.ComputeHash(plumbing.BlobObject, []byte("a1")))
	if err != nil {
		t.Fatalf("blob a1: %v", err)
	}

	cases := []struct {
		name  string
		wants []plumbing.Hash
		haves []plumbing.Hash
		extra []plumbing.Hash // sent although the client holds them
	}{
		{"one commit", []plumbing.Hash{tip}, []plumbing.Hash{skewed}, nil},
		{"across a merge", []plumbing.Hash{tip}, []plumbing.Hash{main1}, nil},
		{"from the side branch", []plumbing.Hash{tip}, []plumbing.Hash{side1}, nil},
		{"two haves", []plumbing.Hash{tip}, []plumbing.Hash{main1, side1}, nil},
		{"past a commit dated before its parent", []plumbing.Hash{tip}, []plumbing.Hash{revert}, nil},
		{"an annotated tag", []plumbing.Hash{annotated}, []plumbing.Hash{skewed}, nil},
		{"a tag of a tag, twenty deep", []plumbing.Hash{chain}, []plumbing.Hash{skewed}, nil},
		{"a tag of a tree", []plumbing.Hash{loose}, []plumbing.Hash{tip}, nil},
		{"a revert to a blob only older history holds", []plumbing.Hash{revert}, []plumbing.Hash{merge}, []plumbing.Hash{old.Hash}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			selection, err := git_upload_pack_select(context.Background(), repo.Storer, &git_request{
				capabilities: capability.NewList(),
				wants:        tt.wants,
				done:         true,
			}, tt.haves, true)
			if err != nil {
				t.Fatalf("select: %v", err)
			}
			got := git_hash_set(selection.objects)
			if len(got) != len(selection.objects) {
				t.Errorf("the selection names %d objects, %d of them distinct", len(selection.objects), len(got))
			}

			lacking, err := revlist.Objects(repo.Storer, tt.wants, tt.haves)
			if err != nil {
				t.Fatalf("revlist: %v", err)
			}
			reached, err := revlist.Objects(repo.Storer, tt.wants, nil)
			if err != nil {
				t.Fatalf("revlist: %v", err)
			}
			reachable := git_hash_set(reached)
			want := git_hash_set(append(append([]plumbing.Hash{}, lacking...), tt.extra...))

			for hash := range want {
				if !got[hash] {
					t.Errorf("THE POINT: %s is left out of the pack", hash)
				}
			}
			for hash := range got {
				if !reachable[hash] {
					t.Errorf("%s is sent, and the wants do not reach it", hash)
				}
				if !want[hash] {
					kind := plumbing.InvalidObject
					if encoded, err := repo.Storer.EncodedObject(plumbing.AnyObject, hash); err == nil {
						kind = encoded.Type()
					}
					t.Errorf("%s %s is sent, and the client holds it", kind, hash)
				}
			}
		})
	}
	// A clone holding only the tip, deepened by two: the two commits behind it
	// and their trees, and nothing of the tip's own tree.
	t.Run("deepening a clone that holds only the tip", func(t *testing.T) {
		selection, err := git_upload_pack_select(context.Background(), repo.Storer, &git_request{
			capabilities: capability.NewList(),
			wants:        []plumbing.Hash{tip},
			shallows:     []plumbing.Hash{tip},
			depth:        3,
			done:         true,
		}, []plumbing.Hash{tip}, true)
		if err != nil {
			t.Fatalf("select: %v", err)
		}
		want := map[plumbing.Hash]bool{skewed: true, revert: true}
		for _, hash := range []plumbing.Hash{skewed, revert} {
			c, err := object.GetCommit(repo.Storer, hash)
			if err != nil {
				t.Fatalf("commit: %v", err)
			}
			want[c.TreeHash] = true
		}
		got := git_hash_set(selection.objects)
		for hash := range want {
			if !got[hash] {
				t.Errorf("THE POINT: %s is left out of the pack", hash)
			}
		}
		for hash := range got {
			if !want[hash] {
				t.Errorf("%s is sent, and the client holds it", hash)
			}
		}
	})
	// A clone holding only the tip, fetching the side branch at depth one: its
	// history forked below where the clone's stops, so nothing the pack carries
	// meets the tip, and the tip's tree is still what the client holds. The
	// side branch's newest commit and its root tree, and not lib or a file.
	t.Run("a shallow clone fetching a branch that forked below it", func(t *testing.T) {
		selection, err := git_upload_pack_select(context.Background(), repo.Storer, &git_request{
			capabilities: capability.NewList(),
			wants:        []plumbing.Hash{side2},
			shallows:     []plumbing.Hash{tip},
			depth:        1,
			done:         true,
		}, []plumbing.Hash{tip}, true)
		if err != nil {
			t.Fatalf("select: %v", err)
		}
		c, err := object.GetCommit(repo.Storer, side2)
		if err != nil {
			t.Fatalf("commit: %v", err)
		}
		want := map[plumbing.Hash]bool{side2: true, c.TreeHash: true}
		got := git_hash_set(selection.objects)
		for hash := range want {
			if !got[hash] {
				t.Errorf("%s is left out of the pack", hash)
			}
		}
		for hash := range got {
			if !want[hash] {
				t.Errorf("THE POINT: %s is sent, and the client holds it", hash)
			}
		}
	})
}

// git_looped_storage answers one name with a tag naming itself, which only a
// damaged store can hold.
type git_looped_storage struct {
	*memory.Storage
	name plumbing.Hash
	tag  plumbing.EncodedObject
}

func (s *git_looped_storage) EncodedObject(kind plumbing.ObjectType, hash plumbing.Hash) (plumbing.EncodedObject, error) {
	if hash == s.name {
		return s.tag, nil
	}
	return s.Storage.EncodedObject(kind, hash)
}

// TestGitWantStopsAtALoopOfTags. A want follows a chain of tags however long
// it is, and a chain that comes back to a tag it passed is refused rather
// than followed for ever.
func TestGitWantStopsAtALoopOfTags(t *testing.T) {
	name := plumbing.ComputeHash(plumbing.TagObject, []byte("loop"))
	encoded := &plumbing.MemoryObject{}
	value := &object.Tag{Name: "loop", Tagger: object.Signature{Name: "Reader", Email: "reader@example.com", When: time.Unix(1700000000, 0)}, Message: "loop", TargetType: plumbing.TagObject, Target: name}
	if err := value.Encode(encoded); err != nil {
		t.Fatalf("encode tag: %v", err)
	}
	storage := &git_looped_storage{Storage: memory.NewStorage(), name: name, tag: encoded}

	done := make(chan error, 1)
	go func() {
		_, _, _, err := git_wants(storage, []plumbing.Hash{name})
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Errorf("a tag naming itself was peeled")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("THE POINT: a tag naming itself is still being followed after 5 s")
	}
}

// TestGitFetchOfferingManyHaves. A client offering every commit of a long
// history but the one it wants costs one walk of that history, not a scan of
// every have per commit walked, which a quadratic test for the walk's end
// made it. The walk is timed against one over the same history offering
// nothing, which loads the same commits; the scan took twenty times as long
// at this length. A walk whose client has gone stops.
func TestGitFetchOfferingManyHaves(t *testing.T) {
	repo, err := git.Init(memory.NewStorage(), nil)
	if err != nil {
		t.Fatalf("init: %v", err)
	}
	tree := read_snapshot(t, repo, map[string]read_file{"a": read_text("a")})
	const length = 10000
	var chain []plumbing.Hash
	var parents []plumbing.Hash
	for i := 0; i < length; i++ {
		hash := read_commit(t, repo, parents, tree, fmt.Sprint(i))
		chain = append(chain, hash)
		parents = []plumbing.Hash{hash}
	}

	fastest := func(tips, haves []plumbing.Hash, wanted int) time.Duration {
		best := time.Duration(math.MaxInt64)
		for run := 0; run < 3; run++ {
			start := time.Now()
			commits, _, err := git_fetch_commits(context.Background(), repo.Storer, tips, haves)
			took := time.Since(start)
			if err != nil {
				t.Fatalf("walk: %v", err)
			}
			if len(commits) != wanted {
				t.Fatalf("the walk wants %d commits, want %d", len(commits), wanted)
			}
			best = min(best, took)
		}
		return best
	}
	plain := fastest([]plumbing.Hash{chain[length-1]}, nil, length)
	offered := fastest([]plumbing.Hash{chain[0]}, chain[1:], 0)
	t.Logf("%d commits: %v offering nothing, %v offering all but the one wanted", length, plain, offered)
	if offered > 4*plain {
		t.Errorf("THE POINT: offering %d haves took %v, against %v for the same walk offering none", length-1, offered, plain)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = git_upload_pack_select(ctx, repo.Storer, &git_request{
		capabilities: capability.NewList(),
		wants:        []plumbing.Hash{chain[length-1]},
		done:         true,
	}, []plumbing.Hash{chain[0]}, true)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("THE POINT: a fetch whose client has gone answered %v, want its walk stopped", err)
	}
}

// TestGitFetchReadsWhatIsNew. A client one commit behind a long history is
// sent that commit, and the walk reads that commit and the one where it meets
// the client's history, not the 300 behind it. The trees of the commit it
// meets are walked once, and no blob is read at all, as the objects a tree
// names are known from the tree.
func TestGitFetchReadsWhatIsNew(t *testing.T) {
	_, repo, _ := read_env_repo(t, "fetchreads")
	files := map[string]read_file{}
	for i := 0; i < 20; i++ {
		files[fmt.Sprintf("lib/d%d/f", i)] = read_text(fmt.Sprintf("library %d", i))
	}
	var have, tip plumbing.Hash
	var parents []plumbing.Hash
	for i := 0; i < 300; i++ {
		files["counter"] = read_text(fmt.Sprint(i))
		have = tip
		tip = read_commit(t, repo, parents, read_snapshot(t, repo, files), fmt.Sprintf("c%d", i))
		parents = []plumbing.Hash{tip}
	}

	counted, counter := read_counted(t, repo)
	counter.reads = map[plumbing.ObjectType]int{}
	selection, err := git_upload_pack_select(context.Background(), counted.Storer, &git_request{
		capabilities: capability.NewList(),
		wants:        []plumbing.Hash{tip},
		done:         true,
	}, []plumbing.Hash{have}, true)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	// The commit, its root tree and the counter's new blob.
	if len(selection.objects) != 3 {
		t.Errorf("one commit behind selected %d objects, want 3", len(selection.objects))
	}
	t.Logf("reads: %d commits, %d trees, %d blobs", counter.reads[plumbing.CommitObject], counter.reads[plumbing.TreeObject], counter.reads[plumbing.BlobObject])
	if reads := counter.reads[plumbing.CommitObject]; reads > 2*(git_between_slop+3) {
		t.Errorf("THE POINT: a fetch one commit behind read %d commits of a history of 300", reads)
	}
	// The commit it meets has 22 trees - its root, lib and lib's 20 folders -
	// and the new commit adds a root; lib is the same in both.
	if reads := counter.reads[plumbing.TreeObject]; reads > 23 {
		t.Errorf("THE POINT: a fetch one commit behind read %d trees, where the commits involved hold 23", reads)
	}
	if reads := counter.reads[plumbing.BlobObject]; reads != 0 {
		t.Errorf("THE POINT: choosing what to send read %d blobs, want none", reads)
	}
}

// TestGitFetchOverARevertAndAMerge - the pack carries more than the client
// lacks when a commit brings back a version only older history holds, and a
// merge brings a branch that forked long ago. Real git must take it, through
// a thin pack, and end with the server's tree.
func TestGitFetchOverARevertAndAMerge(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}
	user, _ := create_git_test_env(t)

	for _, version := range git_versions {
		t.Run("protocol v"+version, func(t *testing.T) {
			id := "revertmerge" + version
			if err := git_init(user, test_app, id); err != nil {
				t.Fatalf("git_init: %v", err)
			}
			repo_path := git_repo_path(user, test_app, id)
			work := git_work_open(t, "git_revert_merge")
			write := func(name, text string) {
				if err := os.WriteFile(filepath.Join(work.dir, name), []byte(text), 0644); err != nil {
					t.Fatalf("write: %v", err)
				}
			}
			save := func(message string) {
				work.when = work.when.AddDate(0, 0, 1)
				work.run("add", ".")
				work.run("commit", "-q", "-m", message)
			}
			write("a.txt", strings.Repeat("first version\n", 200))
			save("first")
			work.run("branch", "old")
			write("a.txt", strings.Repeat("second version\n", 200))
			save("second")
			work.run("push", "-q", repo_path, "main")

			server := git_negotiation_server(t, repo_path)
			dir := git_temporary(t, "git_revert_merge_clone")
			git_run(t, "", git_protocol(version, "clone", "--quiet", server.URL, dir)...)

			// On the server: the old branch grows, main goes back to the first
			// version and merges the old branch.
			work.run("checkout", "-q", "old")
			write("old.txt", "an old branch\n")
			save("old branch")
			work.run("checkout", "-q", "main")
			write("a.txt", strings.Repeat("first version\n", 200))
			save("revert")
			work.when = work.when.AddDate(0, 0, 1)
			work.run("merge", "-q", "--no-edit", "old")
			work.run("push", "-q", repo_path, "main")

			git_run(t, dir, git_protocol(version, "-C", dir, "fetch", "--quiet", "origin", "main")...)
			git_run(t, dir, "-C", dir, "merge", "--quiet", "--ff-only", "FETCH_HEAD")
			git_run(t, dir, "-C", dir, "fsck", "--strict")
			expected := git_run(t, "", "-C", repo_path, "rev-parse", "main^{tree}")
			actual := git_run(t, dir, "-C", dir, "rev-parse", "HEAD^{tree}")
			if strings.TrimSpace(expected) != strings.TrimSpace(actual) {
				t.Errorf("the client's tree is %s after the fetch, want %s", strings.TrimSpace(actual), strings.TrimSpace(expected))
			}
		})
	}
}

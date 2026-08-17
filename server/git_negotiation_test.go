// Mochi server: git smart-HTTP fetch negotiation.
//
// A client that already holds most of a repository offers what it has as
// "have" lines, and the server must answer with a pack containing only what
// is missing. Measured against git.mochi-os.org 2026-08-16, it did not: a
// clone one commit behind was sent the entire repository (566 of 586
// objects), and core itself sent 16,144 objects to deliver a 506-object
// delta. github, given the identical repository and rollback, sent exactly
// the delta.
//
// The negotiation section is parsed and passed to UploadPack as req.Haves, so
// these assert the part that actually matters: that the resulting pack
// EXCLUDES what the client said it had. Counting objects rather than
// inspecting protocol lines, because the protocol looked correct throughout
// while the pack did not.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/format/packfile"
	"github.com/go-git/go-git/v5/plumbing/format/pktline"
)

// git_negotiation_repo builds a bare repository holding `commits` commits and
// returns its path plus every commit hash oldest-first.
func git_negotiation_repo(t *testing.T, user *User, repo_id string, commits int) (string, []plumbing.Hash) {
	t.Helper()
	if err := git_init(user, test_app, repo_id); err != nil {
		t.Fatalf("git_init: %v", err)
	}
	repo_path := git_repo_path(user, test_app, repo_id)

	work, err := os.MkdirTemp("", "git_negotiation_work")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(work)

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
	if err := encoder.Encodef("done\n"); err != nil {
		t.Fatalf("encode done: %v", err)
	}

	gin.SetMode(gin.TestMode)
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

// TestGitUploadPackExcludesHaves — the regression. A client one commit behind
// must be sent that commit's objects, not the repository. The whole-repository
// figure is measured in the same test so the assertion is a comparison rather
// than a guessed constant.
func TestGitUploadPackExcludesHaves(t *testing.T) {
	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

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
	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

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

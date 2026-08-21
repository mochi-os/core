// Mochi server: git smart-HTTP fetch negotiation, end to end.
//
// Driving git_upload_pack directly passes where production does not, because a
// real client negotiates over several stateless POSTs. These serve the handlers
// over HTTP and point the real git binary at them.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// git_negotiation_server serves one repository over the same handlers the
// production routes use, with no authentication in front of them.
func git_negotiation_server(t *testing.T, repo_path string) *httptest.Server {
	t.Helper()
	git_gin_mode()
	engine := gin.New()
	engine.GET("/info/refs", func(c *gin.Context) {
		git_info_refs(c, repo_path, git_service_name(c.Query("service")))
	})
	engine.POST("/git-upload-pack", func(c *gin.Context) {
		git_service_rpc(c, repo_path, "git-upload-pack", &User{UID: "u1"})
	})
	engine.POST("/git-receive-pack", func(c *gin.Context) {
		git_service_rpc(c, repo_path, "git-receive-pack", &User{UID: "u1"})
	})
	server := httptest.NewServer(engine)
	t.Cleanup(server.Close)
	return server
}

// git_client_objects counts the objects in a repository's object store.
func git_client_objects(t *testing.T, dir string) int {
	t.Helper()
	out, err := exec.Command("git", "-C", dir, "count-objects", "-v").Output()
	if err != nil {
		t.Fatalf("count-objects: %v", err)
	}
	total := 0
	for _, line := range strings.Split(string(out), "\n") {
		field, value, found := strings.Cut(line, ": ")
		if !found {
			continue
		}
		if field == "count" || field == "in-pack" {
			n, _ := strconv.Atoi(strings.TrimSpace(value))
			total += n
		}
	}
	return total
}

// git_versions is the two wire protocols this server speaks. A client defaults
// to v2 since git 2.26, so a test that does not name a version silently
// exercises only that one; every end-to-end test runs against both.
var git_versions = []string{"0", "2"}

// git_protocol prepends the wire-protocol selection to a git invocation.
func git_protocol(version string, args ...string) []string {
	return append([]string{"-c", "protocol.version=" + version}, args...)
}

// git_run runs a git command with a deadline. The deadline is not incidental:
// a negotiation round the server ends badly leaves fetch-pack blocked on a
// read, and without it the test hangs until the whole package times out with
// no output about which command stalled.
func git_run(t *testing.T, dir string, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(),
		"GIT_AUTHOR_NAME=Test User", "GIT_AUTHOR_EMAIL=test@example.com",
		"GIT_COMMITTER_NAME=Test User", "GIT_COMMITTER_EMAIL=test@example.com",
		"GIT_TERMINAL_PROMPT=0",
	)
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("git %v timed out after 60s: %s", args, out)
	}
	if err != nil {
		t.Fatalf("git %v: %s (%v)", args, out, err)
	}
	return string(out)
}

// TestGitFetchOverHttpExcludesCommonHistory - a clone a few commits behind must
// be sent those commits, not the repository. The history is long enough to need
// more than one negotiation round; git offers haves in batches of 16.
func TestGitFetchOverHttpExcludesCommonHistory(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}

	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repo_path, _ := git_negotiation_repo(t, user, "http", 40)
	if out, err := exec.Command("git", "-C", repo_path, "repack", "-ad").CombinedOutput(); err != nil {
		t.Fatalf("repack: %s (%v)", out, err)
	}

	server := git_negotiation_server(t, repo_path)

	for _, version := range git_versions {
		t.Run("protocol v"+version, func(t *testing.T) {
			// A full clone, for the baseline figure.
			full_dir, err := os.MkdirTemp("", "git_http_full")
			if err != nil {
				t.Fatalf("temp dir: %v", err)
			}
			defer os.RemoveAll(full_dir)
			git_run(t, "", git_protocol(version, "clone", "--quiet", server.URL, full_dir)...)
			full := git_client_objects(t, full_dir)

			// A second clone rolled back three commits, with the newer objects
			// dropped so the fetch has to transfer them again.
			behind_dir, err := os.MkdirTemp("", "git_http_behind")
			if err != nil {
				t.Fatalf("temp dir: %v", err)
			}
			defer os.RemoveAll(behind_dir)
			git_run(t, "", git_protocol(version, "clone", "--quiet", server.URL, behind_dir)...)
			old := strings.TrimSpace(git_run(t, behind_dir, "-C", behind_dir, "rev-parse", "HEAD~3"))
			git_run(t, behind_dir, "-C", behind_dir, "update-ref", "refs/heads/main", old)
			git_run(t, behind_dir, "-C", behind_dir, "checkout", "-f", "-q", "main")
			for _, ref := range strings.Fields(git_run(t, behind_dir, "-C", behind_dir, "for-each-ref", "--format=%(refname)", "refs/remotes", "refs/tags")) {
				git_run(t, behind_dir, "-C", behind_dir, "update-ref", "-d", ref)
			}
			git_run(t, behind_dir, "-C", behind_dir, "reflog", "expire", "--expire=now", "--all")
			git_run(t, behind_dir, "-C", behind_dir, "gc", "--prune=now", "--quiet")

			before := git_client_objects(t, behind_dir)
			git_run(t, behind_dir, git_protocol(version, "-C", behind_dir, "fetch", "--quiet", "origin", "main")...)
			after := git_client_objects(t, behind_dir)
			transferred := after - before

			t.Logf("full clone: %d objects; three commits behind transferred: %d", full, transferred)

			// Three commits add three commits, three trees and three blobs.
			// Allow generous headroom; the point is that it is nowhere near a
			// full clone.
			if transferred > full/2 {
				t.Errorf("a client three commits behind was sent %d objects against a full clone of %d: "+
					"the fetch is shipping the whole repository instead of the delta", transferred, full)
			}
		})
	}
}

// TestGitFetchOverHttpUpToDateTransfersNothing — the cheap case must stay
// cheap: a fetch with nothing to bring must not pull a pack.
func TestGitFetchOverHttpUpToDateTransfersNothing(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}

	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repo_path, _ := git_negotiation_repo(t, user, "uptodate", 10)
	server := git_negotiation_server(t, repo_path)

	for _, version := range git_versions {
		t.Run("protocol v"+version, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "git_http_uptodate")
			if err != nil {
				t.Fatalf("temp dir: %v", err)
			}
			defer os.RemoveAll(dir)
			git_run(t, "", git_protocol(version, "clone", "--quiet", server.URL, dir)...)

			before := git_client_objects(t, dir)
			git_run(t, dir, git_protocol(version, "-C", dir, "fetch", "--quiet", "origin", "main")...)
			after := git_client_objects(t, dir)

			if after != before {
				t.Errorf("an up-to-date fetch transferred %d objects, want 0", after-before)
			}
		})
	}
}

var _ = http.StatusOK

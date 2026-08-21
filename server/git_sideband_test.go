// Mochi server: git smart-HTTP side band.
//
// Without a side band the packfile is the whole response body: a clone prints
// no "remote:" lines, and a failure after the pack starts can only be an abrupt
// disconnect.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp/sideband"
)

// git_pktline_first splits one pkt-line off the front of a response and returns
// its payload plus the untouched remainder. The acknowledgement section is
// plain pkt-line and the side band starts after it, so a test that wants to
// demultiplex has to step over exactly this much.
func git_pktline_first(t *testing.T, body []byte) (string, []byte) {
	t.Helper()
	if len(body) < 4 {
		t.Fatalf("response is too short to hold a pkt-line: %q", body)
	}
	length, err := strconv.ParseInt(string(body[:4]), 16, 32)
	if err != nil {
		t.Fatalf("malformed pkt-line length %q: %v", body[:4], err)
	}
	if int(length) > len(body) {
		t.Fatalf("pkt-line claims %d bytes, response holds %d", length, len(body))
	}
	return string(body[4:length]), body[length:]
}

// git_sideband_repo builds a repository whose packfile comfortably exceeds the
// progress interval, so the byte-counting messages inside the copy loop fire
// rather than only the ones either side of it. The content is random so it does
// not compress away to nothing.
func git_sideband_repo(t *testing.T, user *User, repo_id string) (string, plumbing.Hash) {
	t.Helper()
	if err := git_init(user, test_app, repo_id); err != nil {
		t.Fatalf("git_init: %v", err)
	}
	repo_path := git_repo_path(user, test_app, repo_id)

	work, err := os.MkdirTemp("", "git_sideband_work")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(work)

	run := func(args ...string) string {
		cmd := exec.Command("git", args...)
		cmd.Dir = work
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=Test User", "GIT_AUTHOR_EMAIL=test@example.com",
			"GIT_COMMITTER_NAME=Test User", "GIT_COMMITTER_EMAIL=test@example.com",
		)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %s (%v)", args, out, err)
		}
		return string(out)
	}

	run("init", "-b", "main")
	// A fixed seed keeps the fixture identical from run to run: the assertions
	// below depend on the pack crossing git_progress_interval.
	source := rand.New(rand.NewSource(1))
	for i := 0; i < 3; i++ {
		payload := make([]byte, 3<<20)
		source.Read(payload)
		if err := os.WriteFile(filepath.Join(work, "blob"+strconv.Itoa(i)+".bin"), payload, 0644); err != nil {
			t.Fatalf("write: %v", err)
		}
		run("add", ".")
		run("commit", "-m", "commit "+strconv.Itoa(i))
	}
	run("push", repo_path, "main")

	head, err := exec.Command("git", "-C", work, "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}
	return repo_path, plumbing.NewHash(strings.TrimSpace(string(head)))
}

// TestGitCloneReportsProgress — the regression. A clone asking for progress has
// to be told what the server is doing.
func TestGitCloneReportsProgress(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}

	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repo_path, _ := git_sideband_repo(t, user, "sideband")
	server := git_negotiation_server(t, repo_path)

	for _, version := range git_versions {
		t.Run("protocol v"+version, func(t *testing.T) {
			git_clone_progress(t, version, server.URL)
		})
	}
}

// git_clone_progress clones with progress asked for and checks what the server
// said while it worked.
func git_clone_progress(t *testing.T, version, url string) {
	t.Helper()
	dir, err := os.MkdirTemp("", "git_sideband_clone")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	out := git_run(t, "", git_protocol(version, "clone", "--progress", url, dir)...)

	var remote []string
	for _, line := range strings.Split(strings.ReplaceAll(out, "\r", "\n"), "\n") {
		if strings.HasPrefix(line, "remote:") {
			remote = append(remote, line)
		}
	}
	if len(remote) == 0 {
		t.Fatalf("a clone with --progress produced no server-side progress at all:\n%s", out)
	}
	t.Logf("server progress lines: %d, first %q last %q", len(remote), remote[0], remote[len(remote)-1])

	joined := strings.Join(remote, "\n")
	for _, want := range []string{"Enumerating objects:", "Sending objects:"} {
		if !strings.Contains(joined, want) {
			t.Errorf("progress never mentioned %q:\n%s", want, joined)
		}
	}
	// The byte counter lives inside the copy loop; with a pack this size it
	// must have fired more than the two messages that bracket the transfer.
	if len(remote) < 3 {
		t.Errorf("only %d progress lines for a multi-megabyte pack: the counter inside the copy loop never fired", len(remote))
	}

	// Progress is worthless if the clone it describes is broken.
	git_run(t, dir, "-C", dir, "fsck", "--strict")
}

// TestGitCloneQuietSuppressesProgress — a client that sends no-progress must get
// the side band for the pack and nothing on the commentary channel.
func TestGitCloneQuietSuppressesProgress(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}

	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repo_path, _ := git_sideband_repo(t, user, "quiet")
	server := git_negotiation_server(t, repo_path)

	for _, version := range git_versions {
		t.Run("protocol v"+version, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "git_sideband_quiet")
			if err != nil {
				t.Fatalf("temp dir: %v", err)
			}
			defer os.RemoveAll(dir)

			out := git_run(t, "", git_protocol(version, "clone", "--quiet", server.URL, dir)...)
			if strings.Contains(out, "remote:") {
				t.Errorf("a quiet clone still received server progress:\n%s", out)
			}
			git_run(t, dir, "-C", dir, "fsck", "--strict")
		})
	}
}

// TestGitUploadPackSidebandMultiplexesPack — the wire format, asserted directly:
// with side-band-64k the packfile arrives inside channel 1 and the progress
// arrives on channel 2, and both survive demultiplexing.
func TestGitUploadPackSidebandMultiplexesPack(t *testing.T) {
	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repo_path, commits := git_negotiation_repo(t, user, "sidebandwire", 6)
	head := commits[len(commits)-1]

	body := git_negotiation_response(t, repo_path, head, nil, "side-band-64k")

	acknowledgement, rest := git_pktline_first(t, body)
	if strings.TrimSpace(acknowledgement) != "NAK" {
		t.Fatalf("acknowledgement section = %q, want NAK for a clone with no haves", acknowledgement)
	}
	if bytes.HasPrefix(rest, []byte("PACK")) {
		t.Fatal("the packfile was written raw: side-band-64k was requested but the response is not multiplexed")
	}

	var progress bytes.Buffer
	demuxer := sideband.NewDemuxer(sideband.Sideband64k, bytes.NewReader(rest))
	demuxer.Progress = &progress
	pack, err := io.ReadAll(demuxer)
	if err != nil {
		t.Fatalf("demultiplexing the response failed: %v", err)
	}

	if !bytes.HasPrefix(pack, []byte("PACK")) {
		t.Fatalf("channel 1 does not hold a packfile: %q", truncate_for_test(string(pack)))
	}
	if objects := git_negotiation_pack_objects(t, pack); objects < 6 {
		t.Errorf("the multiplexed pack holds %d objects, want at least one per commit", objects)
	}
	if !strings.Contains(progress.String(), "Enumerating objects:") {
		t.Errorf("channel 2 carried no progress: %q", progress.String())
	}
}

// TestGitUploadPackWithoutSidebandSendsRawPack — a client that does not ask for
// a side band must still get the bare packfile it expects. Advertising a
// capability must not change what happens to clients that decline it.
func TestGitUploadPackWithoutSidebandSendsRawPack(t *testing.T) {
	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repo_path, commits := git_negotiation_repo(t, user, "nosideband", 6)
	head := commits[len(commits)-1]

	body := git_negotiation_response(t, repo_path, head, nil)

	acknowledgement, rest := git_pktline_first(t, body)
	if strings.TrimSpace(acknowledgement) != "NAK" {
		t.Fatalf("acknowledgement section = %q, want NAK", acknowledgement)
	}
	if !bytes.HasPrefix(rest, []byte("PACK")) {
		t.Fatalf("expected a raw packfile straight after the acknowledgement, got %q", truncate_for_test(string(rest)))
	}
}

// TestGitBytes — the progress messages quote sizes, so the renderer is asserted
// rather than eyeballed in a log line.
func TestGitBytes(t *testing.T) {
	for _, c := range []struct {
		bytes int64
		want  string
	}{
		{0, "0 bytes"},
		{999, "999 bytes"},
		{1 << 10, "1.00 KiB"},
		{1536, "1.50 KiB"},
		{1 << 20, "1.00 MiB"},
		{(1 << 20) * 3 / 2, "1.50 MiB"},
		{1 << 30, "1.00 GiB"},
	} {
		if got := git_bytes(c.bytes); got != c.want {
			t.Errorf("git_bytes(%d) = %q, want %q", c.bytes, got, c.want)
		}
	}
}

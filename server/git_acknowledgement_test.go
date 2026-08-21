// Mochi server: git fetch negotiation, asserted as a conversation.
//
// The negotiation tests elsewhere assert object counts, which say nothing about
// the shape of the exchange - and every protocol defect here was invisible to a
// count. These assert the packet lines themselves.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/go-git/go-git/v5/plumbing"
)

// git_packets splits a response into its pkt-lines, rendering the three special
// packets and the start of the packfile as markers, so a test can state the
// exact sequence it expects rather than grepping for a substring.
func git_packets(t *testing.T, body []byte) []string {
	t.Helper()
	var lines []string
	for len(body) > 0 {
		// The packfile is raw bytes after the pkt-line section, not a packet.
		if bytes.HasPrefix(body, []byte("PACK")) {
			return append(lines, "<pack>")
		}
		if len(body) < 4 {
			t.Fatalf("trailing bytes that are not a pkt-line: %q", body)
		}
		length, err := strconv.ParseInt(string(body[:4]), 16, 32)
		if err != nil {
			t.Fatalf("malformed pkt-line length %q", body[:4])
		}
		switch length {
		case 0:
			lines, body = append(lines, "<flush>"), body[4:]
			continue
		case 1:
			lines, body = append(lines, "<delim>"), body[4:]
			continue
		case 2:
			lines, body = append(lines, "<end>"), body[4:]
			continue
		}
		if length < 4 || int(length) > len(body) {
			t.Fatalf("pkt-line claims %d bytes, %d remain", length, len(body))
		}
		lines = append(lines, strings.TrimSuffix(string(body[4:length]), "\n"))
		body = body[length:]
	}
	return lines
}

func git_same_lines(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// TestGitAcknowledgementLinesV0 — the shape of a v0 negotiation, round by round.
func TestGitAcknowledgementLinesV0(t *testing.T) {
	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repo_path, commits := git_negotiation_repo(t, user, "acklines", 8)
	head := commits[len(commits)-1]
	held := commits[:3]

	t.Run("exploratory round acknowledges each have and ends with NAK", func(t *testing.T) {
		// NAK here means "every have has been answered", not "nothing in common". A
		// flush packet in its place is refused, and omitting it blocks fetch-pack on
		// a socket read.
		want := []string{}
		for _, hash := range held {
			want = append(want, "ACK "+hash.String()+" common")
		}
		want = append(want, "NAK")

		got := git_packets(t, git_negotiation_round(t, repo_path, head, held, false))
		if !git_same_lines(got, want) {
			t.Errorf("exploratory round sent\n  %v\nwant\n  %v", got, want)
		}
	})

	t.Run("exploratory round with nothing in common sends only NAK", func(t *testing.T) {
		got := git_packets(t, git_negotiation_round(t, repo_path, head, nil, false))
		if !git_same_lines(got, []string{"NAK"}) {
			t.Errorf("exploratory round with no haves sent %v, want [NAK]", got)
		}
	})

	t.Run("exploratory round never sends a packfile", func(t *testing.T) {
		// The client is not expecting one and fatals on it with "bad line
		// length character: PACK".
		for _, haves := range [][]plumbing.Hash{nil, held} {
			for _, line := range git_packets(t, git_negotiation_round(t, repo_path, head, haves, false)) {
				if line == "<pack>" {
					t.Error("an exploratory round answered with a packfile")
				}
			}
		}
	})

	t.Run("done round sends exactly one bare ACK then the pack", func(t *testing.T) {
		// A bare ACK is the multi_ack_detailed close, naming the commit the pack was
		// built against: it must be the last common have, and there must be exactly
		// one.
		want := []string{"ACK " + held[len(held)-1].String(), "<pack>"}
		got := git_packets(t, git_negotiation_response(t, repo_path, head, held))
		if !git_same_lines(got, want) {
			t.Errorf("done round sent\n  %v\nwant\n  %v", got, want)
		}
	})

	t.Run("full clone answers NAK then the pack", func(t *testing.T) {
		got := git_packets(t, git_negotiation_response(t, repo_path, head, nil))
		if !git_same_lines(got, []string{"NAK", "<pack>"}) {
			t.Errorf("full clone sent %v, want [NAK <pack>]", got)
		}
	})
}

// git_unrelated_repo builds a repository sharing no history with the fixtures
// above. Its content and its dates are both different, so its commit hashes
// cannot collide with theirs - two repositories built from the same content in
// the same second would otherwise produce the same hashes and prove nothing.
func git_unrelated_repo(t *testing.T, user *User, repo_id string, commits int) (string, []plumbing.Hash) {
	t.Helper()
	if err := git_init(user, test_app, repo_id); err != nil {
		t.Fatalf("git_init: %v", err)
	}
	repo_path := git_repo_path(user, test_app, repo_id)

	work := git_work_open(t, "git_unrelated_work")
	var hashes []plumbing.Hash
	for i := 0; i < commits; i++ {
		hashes = append(hashes, work.commit("elsewhere"+strconv.Itoa(i)))
	}
	work.run("push", repo_path, "main")
	return repo_path, hashes
}

// TestGitUnknownHaves - a client offering commits this repository has never
// seen, which is what a second remote produces. Unknown haves must be filtered
// before the history walk: revlist fails outright on a hash it cannot resolve,
// so the fetch would 500 rather than degrade.
func TestGitUnknownHaves(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}

	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repo_path, commits := git_negotiation_repo(t, user, "unknownhaves", 8)
	head := commits[len(commits)-1]
	_, strangers := git_unrelated_repo(t, user, "unrelated", 5)

	t.Run("wholly unrelated haves are not acknowledged and do not break the round", func(t *testing.T) {
		got := git_packets(t, git_negotiation_round(t, repo_path, head, strangers, false))
		if !git_same_lines(got, []string{"NAK"}) {
			t.Errorf("exploratory round with only unknown haves sent %v, want [NAK]", got)
		}
	})

	t.Run("a fetch offering only unknown haves still sends everything", func(t *testing.T) {
		full := git_negotiation_fetch(t, repo_path, head, nil)
		stranger := git_negotiation_fetch(t, repo_path, head, strangers)
		if stranger != full {
			t.Errorf("offering unknown haves changed the pack from %d objects to %d; they exclude nothing, "+
				"so they must change nothing", full, stranger)
		}
	})

	t.Run("known haves are still honoured alongside unknown ones", func(t *testing.T) {
		// The mixture is the realistic case: a client with two remotes offers
		// its newest commits first, and those may come from either.
		mixed := append(append([]plumbing.Hash{}, strangers...), commits[:3]...)

		want := []string{}
		for _, hash := range commits[:3] {
			want = append(want, "ACK "+hash.String()+" common")
		}
		want = append(want, "NAK")

		got := git_packets(t, git_negotiation_round(t, repo_path, head, mixed, false))
		if !git_same_lines(got, want) {
			t.Errorf("mixed haves sent\n  %v\nwant only the known ones acknowledged\n  %v", got, want)
		}

		full := git_negotiation_fetch(t, repo_path, head, nil)
		if narrowed := git_negotiation_fetch(t, repo_path, head, mixed); narrowed >= full {
			t.Errorf("mixed haves produced %d objects against a full clone of %d: the known ones were ignored",
				narrowed, full)
		}
	})

	t.Run("fetching a second, unrelated remote works end to end", func(t *testing.T) {
		// The real shape of it: clone one repository, add another as a remote,
		// and fetch. The client offers the first repository's commits to a
		// server that has never seen them.
		other_path, _ := git_unrelated_repo(t, user, "second", 6)
		first := git_negotiation_server(t, repo_path)
		second := git_negotiation_server(t, other_path)

		for _, version := range git_versions {
			dir := git_temporary(t, "git_two_remotes")
			git_run(t, "", git_protocol(version, "clone", "--quiet", first.URL, dir)...)
			git_run(t, dir, "-C", dir, "remote", "add", "other", second.URL)
			git_run(t, dir, git_protocol(version, "-C", dir, "fetch", "--quiet", "other")...)
			git_run(t, dir, "-C", dir, "fsck", "--strict")

			// Both histories have to be present and separate.
			for _, ref := range []string{"refs/remotes/origin/main", "refs/remotes/other/main"} {
				if out := git_run(t, dir, "-C", dir, "rev-parse", "--verify", ref); strings.TrimSpace(out) == "" {
					t.Errorf("protocol v%s: %s is missing after fetching a second remote", version, ref)
				}
			}
		}
	})
}

// TestGitConcurrentFetches — several fetches of one repository at once. Each
// POST loads its own storage handle and builds its own pack, and the thin-pack
// path holds candidates in memory per request, so concurrency is where a shared
// handle or a reused buffer would show up.
func TestGitConcurrentFetches(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}

	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repo_path, commits := git_negotiation_repo(t, user, "concurrent", 20)
	head := commits[len(commits)-1]

	t.Run("handler level, mixed request shapes", func(t *testing.T) {
		// Deliberately not all the same request: a full clone, a fetch with
		// haves, a thin fetch and an exploratory round exercise different
		// paths through the same storage at the same moment.
		expected_full := git_negotiation_fetch(t, repo_path, head, nil)
		expected_behind := git_negotiation_fetch(t, repo_path, head, commits[:15])

		var group sync.WaitGroup
		failures := make(chan string, 64)
		for i := 0; i < 16; i++ {
			group.Add(1)
			go func(i int) {
				defer group.Done()
				switch i % 4 {
				case 0:
					if got := git_negotiation_fetch(t, repo_path, head, nil); got != expected_full {
						failures <- fmt.Sprintf("full clone gave %d objects, want %d", got, expected_full)
					}
				case 1:
					if got := git_negotiation_fetch(t, repo_path, head, commits[:15]); got != expected_behind {
						failures <- fmt.Sprintf("fetch gave %d objects, want %d", got, expected_behind)
					}
				case 2:
					body := git_negotiation_response(t, repo_path, head, commits[:15], "thin-pack")
					if !bytes.Contains(body, []byte("PACK")) {
						failures <- "thin fetch produced no packfile"
					}
				case 3:
					if lines := git_packets(t, git_negotiation_round(t, repo_path, head, commits[:3], false)); lines[len(lines)-1] != "NAK" {
						failures <- fmt.Sprintf("exploratory round ended with %q", lines[len(lines)-1])
					}
				}
			}(i)
		}
		group.Wait()
		close(failures)
		for failure := range failures {
			t.Error(failure)
		}
	})

	t.Run("real clients over HTTP", func(t *testing.T) {
		server := git_negotiation_server(t, repo_path)
		var group sync.WaitGroup
		results := make(chan string, 8)
		for i := 0; i < 6; i++ {
			group.Add(1)
			go func(i int) {
				defer group.Done()
				dir, err := os.MkdirTemp("", "git_concurrent")
				if err != nil {
					results <- fmt.Sprintf("temp dir: %v", err)
					return
				}
				defer os.RemoveAll(dir)
				version := git_versions[i%len(git_versions)]
				arguments := append(git_protocol(version, "clone", "--quiet"), server.URL, dir)
				command := exec.Command("git", arguments...)
				command.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
				if out, err := command.CombinedOutput(); err != nil {
					results <- fmt.Sprintf("protocol v%s clone: %s (%v)", version, out, err)
					return
				}
				if out, err := exec.Command("git", "-C", dir, "fsck", "--strict").CombinedOutput(); err != nil {
					results <- fmt.Sprintf("protocol v%s fsck: %s (%v)", version, out, err)
				}
			}(i)
		}
		group.Wait()
		close(results)
		for failure := range results {
			t.Error(failure)
		}
	})
}

// TestGitConcurrentFetchAndPush — a fetch while a push is in flight. The two
// share git_service_rpc and the same repository on disk, and the push is the
// only path that writes, so this is where a reader would see a half-written
// state if one were possible.
func TestGitConcurrentFetchAndPush(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}

	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	// The push budget is measured from the owner's storage directory, and a
	// push with no measurable budget is metered at one byte and refused.
	if err := os.MkdirAll(user_storage_dir(&User{UID: "u1"}), 0755); err != nil {
		t.Fatalf("storage dir: %v", err)
	}

	repo_path, _ := git_negotiation_repo(t, user, "fetchpush", 12)
	server := git_negotiation_server(t, repo_path)

	// A working copy with something to push, prepared before the race so the
	// timing is between the push and the fetches and not the setup.
	pusher := git_temporary(t, "git_pusher")
	git_run(t, "", "clone", "--quiet", server.URL, pusher)
	if err := os.WriteFile(pusher+"/pushed.txt", []byte("written during a fetch"), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	git_run(t, pusher, "-C", pusher, "add", ".")
	git_run(t, pusher, "-c", "user.email=t@example.com", "-c", "user.name=Test", "-C", pusher, "commit", "-qm", "concurrent")
	pushed := strings.TrimSpace(git_run(t, pusher, "-C", pusher, "rev-parse", "HEAD"))

	var group sync.WaitGroup
	failures := make(chan string, 16)

	group.Add(1)
	go func() {
		defer group.Done()
		command := exec.Command("git", "-C", pusher, "push", "--quiet", "origin", "HEAD:refs/heads/concurrent")
		command.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
		if out, err := command.CombinedOutput(); err != nil {
			failures <- fmt.Sprintf("push: %s (%v)", out, err)
		}
	}()

	for i := 0; i < 4; i++ {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			dir, err := os.MkdirTemp("", "git_racing_fetch")
			if err != nil {
				failures <- fmt.Sprintf("temp dir: %v", err)
				return
			}
			defer os.RemoveAll(dir)
			command := exec.Command("git", append(git_protocol(git_versions[i%len(git_versions)], "clone", "--quiet"), server.URL, dir)...)
			command.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
			if out, err := command.CombinedOutput(); err != nil {
				failures <- fmt.Sprintf("clone during push: %s (%v)", out, err)
				return
			}
			// Whichever side of the push it landed on, what arrived must be a
			// complete and consistent repository.
			if out, err := exec.Command("git", "-C", dir, "fsck", "--strict").CombinedOutput(); err != nil {
				failures <- fmt.Sprintf("fsck of a clone taken during a push: %s (%v)", out, err)
			}
		}(i)
	}

	group.Wait()
	close(failures)
	for failure := range failures {
		t.Error(failure)
	}

	if landed := strings.TrimSpace(git_run(t, "", "-C", repo_path, "rev-parse", "refs/heads/concurrent")); landed != pushed {
		t.Errorf("the pushed branch is at %s, want %s", landed, pushed)
	}
	git_run(t, "", "-C", repo_path, "fsck", "--strict")
}

// TestGitAcknowledgementLinesV2 — the same for protocol v2, whose rules are
// different in a way that is easy to get wrong: the acknowledgments section is
// omitted ENTIRELY once the client has said done, and "ready" commits the
// response to carrying a packfile in the same round.
func TestGitAcknowledgementLinesV2(t *testing.T) {
	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repo_path, commits := git_negotiation_repo(t, user, "acklinesv2", 8)
	head := commits[len(commits)-1]
	held := commits[:3]

	arguments := func(done bool, haves []plumbing.Hash) []string {
		args := []string{"want " + head.String(), "ofs-delta"}
		for _, hash := range haves {
			args = append(args, "have "+hash.String())
		}
		if done {
			args = append(args, "done")
		}
		return args
	}

	t.Run("nothing in common: acknowledgments, NAK, flush, and no packfile", func(t *testing.T) {
		got := git_packets(t, []byte(git_v2_request(t, repo_path, "fetch", arguments(false, nil)...)))
		if !git_same_lines(got, []string{"acknowledgments", "NAK", "<flush>"}) {
			t.Errorf("sent %v, want [acknowledgments NAK <flush>]", got)
		}
	})

	t.Run("common found: ACKs then ready, then the packfile section", func(t *testing.T) {
		got := git_packets(t, []byte(git_v2_request(t, repo_path, "fetch", arguments(false, held)...)))

		want := []string{"acknowledgments"}
		for _, hash := range held {
			want = append(want, "ACK "+hash.String())
		}
		want = append(want, "ready", "<delim>", "packfile")

		if len(got) < len(want) || !git_same_lines(got[:len(want)], want) {
			t.Fatalf("sent\n  %v\nwant it to begin\n  %v", got, want)
		}
		// "ready" is a promise: the pack must be in this same response, or the
		// client waits for one that never comes.
		if len(got) <= len(want) {
			t.Error("the response said ready and then carried no packfile")
		}
	})

	t.Run("done: the acknowledgments section is omitted entirely", func(t *testing.T) {
		got := git_packets(t, []byte(git_v2_request(t, repo_path, "fetch", arguments(true, held)...)))
		if len(got) == 0 || got[0] != "packfile" {
			t.Errorf("a done request began with %v; the specification requires the acknowledgments section be omitted", got)
		}
		for _, line := range got {
			if line == "acknowledgments" || line == "ready" || strings.HasPrefix(line, "ACK ") || line == "NAK" {
				t.Errorf("a done request still carried %q", line)
			}
		}
	})
}

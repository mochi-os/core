// Mochi server: git wire protocol version 2, and partial clone.
//
// Two measured gaps, both against git.mochi-os.org on 2026-08-16. The server
// was v0 only, so every connection - clone and fetch alike - carried the whole
// reference advertisement: 1,395 bytes for a 22-ref repository and 22,262 for
// core's 356 refs, before a single object moved. And `git clone
// --filter=blob:none` answered "filtering not recognized by server, ignoring"
// and quietly cloned everything, so a client asking for less got the lot.
//
// go-git has no protocol v2 at all - no ls-refs, no fetch command, and its
// pkt-line scanner rejects the delimiter packet v2 introduced - so the whole of
// it is implemented here.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"fmt"
	"net/http/httptest"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/transport"
)

// git_v2_request drives one v2 command against the real handler and returns the
// response body.
func git_v2_request(t *testing.T, repo_path, command string, arguments ...string) string {
	t.Helper()

	var body strings.Builder
	packet := func(text string) {
		body.WriteString(fmt.Sprintf("%04x%s", len(text)+4, text))
	}
	packet("command=" + command + "\n")
	packet("agent=test\n")
	packet("object-format=sha1\n")
	body.WriteString("0001")
	for _, argument := range arguments {
		packet(argument + "\n")
	}
	body.WriteString("0000")

	git_gin_mode()
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest("POST", "/x/git/git-upload-pack", strings.NewReader(body.String()))
	c.Request.Header.Set("Git-Protocol", "version=2")

	if !git_v2_serve(c, repo_path, nop_closer{strings.NewReader(body.String())}) {
		t.Fatal("git_v2_serve did not handle the request")
	}
	if recorder.Code != 200 {
		t.Fatalf("status = %d, body = %q", recorder.Code, recorder.Body.String())
	}
	return recorder.Body.String()
}

type nop_closer struct{ *strings.Reader }

func (nop_closer) Close() error { return nil }

// git_v2_advertisement fetches the opening advertisement for a protocol version
// and returns its body.
func git_v2_advertisement(t *testing.T, repo_path, version string) string {
	t.Helper()
	git_gin_mode()
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest("GET", "/x/git/info/refs?service=git-upload-pack", nil)
	if version != "" {
		c.Request.Header.Set("Git-Protocol", "version="+version)
	}
	git_info_refs(c, repo_path, "git-upload-pack")
	if recorder.Code != 200 {
		t.Fatalf("status = %d, body = %q", recorder.Code, recorder.Body.String())
	}
	return recorder.Body.String()
}

// git_branches adds branches to a bare repository, all pointing at the same
// commit, so the advertisement has something to be large about.
func git_branches(t *testing.T, repo_path string, count int) {
	t.Helper()
	head := strings.TrimSpace(git_run(t, "", "-C", repo_path, "rev-parse", "HEAD"))
	for i := 0; i < count; i++ {
		git_run(t, "", "-C", repo_path, "update-ref", "refs/heads/branch"+strconv.Itoa(i), head)
	}
}

// TestGitAdvertisementOmitsReferencesInV2 — the measurement this was done for.
// v0 sends every ref on every connection; v2 sends none until asked.
func TestGitAdvertisementOmitsReferencesInV2(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}

	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repo_path, _ := git_shallow_repo(t, user, "advert", 3)
	git_branches(t, repo_path, 200)

	whole := git_v2_advertisement(t, repo_path, "")
	narrow := git_v2_advertisement(t, repo_path, "2")

	t.Logf("advertisement for a 200-branch repository: %d bytes in v0, %d in v2", len(whole), len(narrow))

	if !strings.Contains(narrow, "version 2") {
		t.Fatalf("a client asking for v2 was not answered in v2: %q", truncate_for_test(narrow))
	}
	if strings.Contains(narrow, "refs/heads/branch100") {
		t.Error("the v2 advertisement carries references; the whole point is that it does not")
	}
	if len(narrow) >= len(whole)/4 {
		t.Errorf("the v2 advertisement is %d bytes against v0's %d: it is not saving the reference list",
			len(narrow), len(whole))
	}

	// A client that does not ask for v2 must still get exactly what it always
	// got, references and all.
	if !strings.Contains(whole, "refs/heads/branch100") {
		t.Error("the v0 advertisement lost its references")
	}
}

// TestGitPushAdvertisementStaysV0 — push is still served by go-git's session,
// which has no v2 at all. A client asks for v2 on every connection including a
// push, so the one that matters is that it is answered in v0 and falls back
// rather than being handed a version nothing behind it implements.
func TestGitPushAdvertisementStaysV0(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}

	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repo_path, _ := git_shallow_repo(t, user, "pushadvert", 2)

	git_gin_mode()
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest("GET", "/x/git/info/refs?service=git-receive-pack", nil)
	c.Request.Header.Set("Git-Protocol", "version=2")
	git_info_refs(c, repo_path, "git-receive-pack")

	body := recorder.Body.String()
	if strings.Contains(body, "version 2") {
		t.Error("a receive-pack advertisement claimed v2, which nothing behind it implements")
	}
	if !strings.Contains(body, "refs/heads/main") {
		t.Errorf("the receive-pack advertisement lost its references: %q", truncate_for_test(body))
	}
	if !strings.Contains(body, "report-status") {
		t.Errorf("the receive-pack advertisement lost its capabilities: %q", truncate_for_test(body))
	}
}

// TestGitCloneEmptyRepository — every Mochi repository starts empty, and a
// clone of one has to succeed rather than fail on the ref that is not there
// yet. HEAD is a symbolic ref to a branch with no commits, which is the one
// case ls-refs has nothing at all to say about.
func TestGitCloneEmptyRepository(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}

	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	if err := git_init(user, test_app, "empty"); err != nil {
		t.Fatalf("git_init: %v", err)
	}
	repo_path := git_repo_path(user, test_app, "empty")
	server := git_negotiation_server(t, repo_path)

	for _, version := range git_versions {
		t.Run("protocol v"+version, func(t *testing.T) {
			dir := git_temporary(t, "git_empty_clone")
			out := git_run(t, "", git_protocol(version, "clone", server.URL, dir)...)
			t.Logf("%s", strings.TrimSpace(out))
			git_run(t, dir, "-C", dir, "fsck", "--strict")
			if count := len(strings.Fields(git_run(t, dir, "-C", dir, "for-each-ref"))); count != 0 {
				t.Errorf("a clone of an empty repository has %d ref fields, want none", count)
			}
		})
	}
}

// TestGitPushOverHttp — a push round trip through the same dispatcher a fetch
// goes through. Nothing covered push over HTTP before, and the protocol-version
// branch now sits directly in front of it.
func TestGitPushOverHttp(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}

	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	// The push budget is measured from the owner's storage directory, and a
	// push with no measurable budget is metered at a single byte and refused.
	if err := os.MkdirAll(user_storage_dir(&User{UID: "u1"}), 0755); err != nil {
		t.Fatalf("storage dir: %v", err)
	}

	repo_path, _ := git_shallow_repo(t, user, "pushhttp", 3)
	server := git_negotiation_server(t, repo_path)

	for _, version := range git_versions {
		t.Run("protocol v"+version, func(t *testing.T) {
			dir := git_temporary(t, "git_push_http")
			git_run(t, "", git_protocol(version, "clone", "--quiet", server.URL, dir)...)

			if err := os.WriteFile(dir+"/pushed.txt", []byte("pushed over http"), 0644); err != nil {
				t.Fatalf("write: %v", err)
			}
			git_run(t, dir, "-C", dir, "add", ".")
			git_run(t, dir, "-c", "user.email=t@example.com", "-c", "user.name=Test", "-C", dir, "commit", "-qm", "pushed")
			expected := strings.TrimSpace(git_run(t, dir, "-C", dir, "rev-parse", "HEAD"))

			git_run(t, dir, git_protocol(version, "-C", dir, "push", "--quiet", "origin", "HEAD:refs/heads/pushed"+version)...)

			landed := strings.TrimSpace(git_run(t, "", "-C", repo_path, "rev-parse", "refs/heads/pushed"+version))
			if landed != expected {
				t.Errorf("the pushed branch is at %s on the server, want %s", landed, expected)
			}
			git_run(t, "", "-C", repo_path, "fsck", "--strict")
		})
	}
}

// TestGitLsRefsNarrowsByPrefix — the other half of the saving: having stopped
// sending every ref unasked, the client can ask for only the ones it wants.
func TestGitLsRefsNarrowsByPrefix(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}

	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repo_path, _ := git_shallow_repo(t, user, "lsrefs", 3)
	git_branches(t, repo_path, 20)

	everything := git_v2_request(t, repo_path, "ls-refs", "peel", "symrefs")
	if !strings.Contains(everything, "refs/heads/branch5") || !strings.Contains(everything, "refs/tags/v1") {
		t.Fatalf("ls-refs with no prefix omitted refs: %q", truncate_for_test(everything))
	}
	if !strings.Contains(everything, "HEAD symref-target:refs/heads/main") {
		t.Error("ls-refs did not report which branch HEAD points at, so a fresh clone cannot tell what to check out")
	}
	// The annotated tag has to name the commit behind it, or the client cannot
	// tell a tag object from the commit it tags without fetching first.
	if !strings.Contains(everything, "refs/tags/v1 peeled:") {
		t.Errorf("ls-refs did not peel the annotated tag: %q", truncate_for_test(everything))
	}

	tags := git_v2_request(t, repo_path, "ls-refs", "ref-prefix refs/tags/")
	if strings.Contains(tags, "refs/heads/") {
		t.Error("ref-prefix refs/tags/ returned branches too")
	}
	if !strings.Contains(tags, "refs/tags/v1") {
		t.Error("ref-prefix refs/tags/ returned no tags")
	}
	if len(tags) >= len(everything)/2 {
		t.Errorf("narrowing to refs/tags/ returned %d bytes against %d for everything", len(tags), len(everything))
	}
}

// TestGitAdvertisementPeelsTags — go-git's advertisement left a TODO where
// peeled refs belong, so `git ls-remote` against this server showed no "^{}"
// lines and a caller comparing a tag against a commit saw a difference that was
// not there.
func TestGitAdvertisementPeelsTags(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}

	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repo_path, commits := git_shallow_repo(t, user, "peeled", 4)
	server := git_negotiation_server(t, repo_path)

	for _, version := range git_versions {
		t.Run("protocol v"+version, func(t *testing.T) {
			out := git_run(t, "", git_protocol(version, "ls-remote", server.URL)...)
			if !strings.Contains(out, "refs/tags/v1^{}") {
				t.Fatalf("ls-remote shows no peeled tag:\n%s", out)
			}
			// The tag sits on the second commit, so that is what it peels to.
			if !strings.Contains(out, commits[1].String()+"\trefs/tags/v1^{}") {
				t.Errorf("refs/tags/v1^{} does not name the commit the tag points at (%s):\n%s", commits[1], out)
			}
		})
	}
}

// git_object_kinds counts the objects present in a repository by type. Only the
// four real type names are counted: a partial clone makes git print a warning
// about promisor remotes, and CombinedOutput puts its words in the same stream.
func git_object_kinds(t *testing.T, dir string) map[string]int {
	t.Helper()
	out := git_run(t, dir, "-C", dir, "cat-file", "--batch-all-objects", "--batch-check=%(objecttype)")
	counts := map[string]int{}
	for _, word := range strings.Fields(out) {
		switch word {
		case "commit", "tree", "blob", "tag":
			counts[word]++
		}
	}
	return counts
}

// git_filter_repo builds a repository with several files, so a blob filter has
// something to leave behind.
func git_filter_repo(t *testing.T, user *User, repo_id string) string {
	t.Helper()
	if err := git_init(user, test_app, repo_id); err != nil {
		t.Fatalf("git_init: %v", err)
	}
	repo_path := git_repo_path(user, test_app, repo_id)

	work := git_work_open(t, "git_filter_work")
	if err := os.MkdirAll(work.dir+"/deep/deeper", 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	for i := 0; i < 4; i++ {
		// Four blobs comfortably over a kibibyte and four comfortably under,
		// each distinct so the counts below mean what they say. The names
		// avoid the one work.commit writes itself.
		suffix := strconv.Itoa(i)
		if err := os.WriteFile(work.dir+"/large"+suffix+".bin", []byte(strings.Repeat("x", 2000+i)), 0644); err != nil {
			t.Fatalf("write: %v", err)
		}
		if err := os.WriteFile(work.dir+"/deep/deeper/small"+suffix+".txt", []byte("small "+suffix), 0644); err != nil {
			t.Fatalf("write: %v", err)
		}
		work.commit("commit" + suffix)
	}
	work.run("push", repo_path, "main")
	return repo_path
}

// TestGitCloneFilterBlobNone — a partial clone must actually arrive partial.
func TestGitCloneFilterBlobNone(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}

	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repo_path := git_filter_repo(t, user, "filterblob")
	server := git_negotiation_server(t, repo_path)

	for _, version := range git_versions {
		t.Run("protocol v"+version, func(t *testing.T) {
			dir := git_temporary(t, "git_filter_blob")
			out := git_run(t, "", git_protocol(version, "clone", "--quiet", "--filter=blob:none", "--no-checkout", server.URL, dir)...)
			if strings.Contains(out, "filtering not recognized") {
				t.Fatalf("the server did not honour the filter:\n%s", out)
			}

			counts := git_object_kinds(t, dir)
			t.Logf("objects after --filter=blob:none: %v", counts)
			if counts["blob"] != 0 {
				t.Errorf("a blob:none clone received %d blobs", counts["blob"])
			}
			if counts["commit"] == 0 || counts["tree"] == 0 {
				t.Errorf("a blob:none clone dropped more than blobs: %v", counts)
			}

			// The promisor half: a blob left out has to be fetchable by name
			// afterwards, or the clone is not usable.
			content := git_run(t, dir, git_protocol(version, "-C", dir, "cat-file", "blob", "HEAD:large0.bin")...)
			if len(strings.TrimSpace(content)) != 2000 {
				t.Errorf("lazily fetching an omitted blob gave %d bytes, want 2000", len(strings.TrimSpace(content)))
			}
		})
	}
}

// TestGitCloneShallowFilterLazyFetch — a shallow partial clone, which is what
// CI actually runs, and then the lazy fetch for a blob it was not sent.
//
// Measured against git.mochi-os.org on 2026-08-17: HTTP 500. The lazy fetch
// asks for the blob by name and carries the client's shallow lines with it, so
// the request takes the shallow path - where every want is peeled to the commit
// it names, and a blob peels to nothing. Neither half alone reaches it: the
// unshallow filter test passes, and the shallow tests never ask for a blob.
func TestGitCloneShallowFilterLazyFetch(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}

	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repo_path := git_filter_repo(t, user, "shallowfilter")
	server := git_negotiation_server(t, repo_path)

	for _, version := range git_versions {
		t.Run("protocol v"+version, func(t *testing.T) {
			dir := git_temporary(t, "git_shallow_filter")
			git_run(t, "", git_protocol(version, "clone", "--quiet",
				"--filter=blob:none", "--no-checkout", "--depth", "2", server.URL, dir)...)

			if counts := git_object_kinds(t, dir); counts["blob"] != 0 {
				t.Errorf("a shallow blob:none clone received %d blobs", counts["blob"])
			}

			content := git_run(t, dir, git_protocol(version, "-C", dir, "cat-file", "blob", "HEAD:large0.bin")...)
			if len(strings.TrimSpace(content)) != 2000 {
				t.Errorf("lazily fetching an omitted blob gave %d bytes, want 2000", len(strings.TrimSpace(content)))
			}
		})
	}
}

// TestGitCloneFilterBlobLimit — the same, cutting on size rather than on all
// blobs, so the small ones still travel.
func TestGitCloneFilterBlobLimit(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}

	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repo_path := git_filter_repo(t, user, "filterlimit")
	server := git_negotiation_server(t, repo_path)

	// Measured against an unfiltered clone of the same repository rather than
	// against a hard-coded count, so the assertion says what it means - four
	// blobs are over the limit - and does not quietly become wrong when the
	// fixture gains a file.
	plain := git_temporary(t, "git_filter_plain")
	git_run(t, "", "clone", "--quiet", "--no-checkout", server.URL, plain)
	whole := git_object_kinds(t, plain)

	dir := git_temporary(t, "git_filter_limit")
	git_run(t, "", "clone", "--quiet", "--filter=blob:limit=1k", "--no-checkout", server.URL, dir)
	limited := git_object_kinds(t, dir)

	t.Logf("blobs: %d unfiltered, %d with --filter=blob:limit=1k", whole["blob"], limited["blob"])
	if whole["blob"]-limited["blob"] != 4 {
		t.Errorf("blob:limit=1k dropped %d blobs (%d of %d kept); exactly the four over 1024 bytes should have gone",
			whole["blob"]-limited["blob"], limited["blob"], whole["blob"])
	}
	if limited["blob"] == 0 {
		t.Error("blob:limit=1k dropped the blobs under the limit as well")
	}
}

// TestGitCloneFilterTreeZero — tree:0 is what CI uses to take history with no
// content at all, and needs each object's depth rather than just its type.
func TestGitCloneFilterTreeZero(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}

	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repo_path := git_filter_repo(t, user, "filtertree")
	server := git_negotiation_server(t, repo_path)

	dir := git_temporary(t, "git_filter_tree")
	git_run(t, "", "clone", "--quiet", "--filter=tree:0", "--no-checkout", server.URL, dir)

	counts := git_object_kinds(t, dir)
	t.Logf("objects after --filter=tree:0: %v", counts)
	if counts["commit"] == 0 {
		t.Error("tree:0 dropped the commits, which it must not")
	}
	if counts["tree"] != 0 || counts["blob"] != 0 {
		t.Errorf("tree:0 still sent %d trees and %d blobs", counts["tree"], counts["blob"])
	}
}

// TestGitFilterParse — the specification grammar, including the forms real
// clients send and the ones this server refuses.
func TestGitFilterParse(t *testing.T) {
	for _, c := range []struct {
		specification string
		measured      bool
	}{
		{"blob:none", false},
		{"blob:limit=1024", false},
		{"blob:limit=1k", false},
		{"blob:limit=10m", false},
		{"tree:0", true},
		{"tree:2", true},
		{"object:type=commit", false},
		{"combine:blob%3Anone+tree%3A2", true},
	} {
		rules, measured, err := git_filter_parse(c.specification)
		if err != nil {
			t.Errorf("%s: %v", c.specification, err)
			continue
		}
		if len(rules) == 0 {
			t.Errorf("%s: parsed to no rules", c.specification)
		}
		if measured != c.measured {
			t.Errorf("%s: needs depth = %v, want %v", c.specification, measured, c.measured)
		}
	}

	// A filter this server cannot honour is refused rather than ignored:
	// quietly sending the whole repository is what the client was already
	// getting, and it is the thing being fixed.
	for _, specification := range []string{"sparse:oid=deadbeef", "blob:limit=", "blob:limit=zz", "tree:-1", "object:type=nonsense", "blob:some"} {
		if _, _, err := git_filter_parse(specification); err == nil {
			t.Errorf("%q was accepted, want a refusal", specification)
		}
	}
}

// TestGitFilterSizes — the scaling suffixes a client may send.
func TestGitFilterSizes(t *testing.T) {
	for _, c := range []struct {
		text  string
		bytes int64
	}{
		{"0", 0},
		{"1024", 1024},
		{"1k", 1 << 10},
		{"1K", 1 << 10},
		{"2m", 2 << 20},
		{"3g", 3 << 30},
	} {
		got, err := git_filter_size(c.text)
		if err != nil {
			t.Errorf("%s: %v", c.text, err)
			continue
		}
		if got != c.bytes {
			t.Errorf("git_filter_size(%q) = %d, want %d", c.text, got, c.bytes)
		}
	}
}

// TestGitIncludeTag — include-tag asks that annotated tags travel with the
// objects they point at, saving the client a second round trip.
func TestGitIncludeTag(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}

	user, _, cleanup := create_git_test_env(t)
	defer cleanup()

	repo_path, commits := git_shallow_repo(t, user, "includetag", 4)
	storage, err := (&git_loader{}).Load(&transport.Endpoint{Path: repo_path})
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	// The whole history, with no tag object in it yet.
	objects, _, err := git_history(storage, []plumbing.Hash{commits[len(commits)-1]}, nil, nil)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	before := len(objects)

	with_tags, err := git_include_tags(storage, objects)
	if err != nil {
		t.Fatalf("include tags: %v", err)
	}
	if len(with_tags) != before+1 {
		t.Errorf("include-tag added %d objects, want the one annotated tag", len(with_tags)-before)
	}

	// A tag whose target is not being sent must not be dragged in.
	partial, err := git_include_tags(storage, []plumbing.Hash{commits[len(commits)-1]})
	if err != nil {
		t.Fatalf("include tags: %v", err)
	}
	if len(partial) != 1 {
		t.Errorf("include-tag added a tag whose target is not in the pack: %v", partial)
	}
}

// TestGitV2DecodeCommand — the v2 request framing. go-git's pkt-line scanner
// cannot read it: a delimiter packet is a length of 1, which it rejects as
// invalid, so the reader is ours and the framing is worth asserting directly.
func TestGitV2DecodeCommand(t *testing.T) {
	packet := func(text string) string { return fmt.Sprintf("%04x%s", len(text)+4, text) }

	body := packet("command=fetch\n") +
		packet("agent=git/2.53.0\n") +
		packet("object-format=sha1\n") +
		"0001" +
		packet("thin-pack\n") +
		packet("want 722cd9468569ef931a61b731279583fa268254b2\n") +
		packet("done\n") +
		"0000"

	command, err := git_v2_decode(strings.NewReader(body))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if command.name != "fetch" {
		t.Errorf("command = %q, want fetch", command.name)
	}
	// The capabilities before the delimiter are not arguments.
	if len(command.arguments) != 3 {
		t.Errorf("arguments = %v, want the three after the delimiter", command.arguments)
	}

	request, err := git_v2_fetch_request(command.arguments)
	if err != nil {
		t.Fatalf("fetch request: %v", err)
	}
	if len(request.wants) != 1 || !request.done {
		t.Errorf("wants %v done=%v", request.wants, request.done)
	}

	if _, err := git_v2_decode(strings.NewReader(packet("agent=x\n") + "0000")); err == nil {
		t.Error("a request naming no command was accepted")
	}
}

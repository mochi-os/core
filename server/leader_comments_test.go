// Mochi server: no comment claims a leader or a replica that isn't there.
//
// closure.go argued with itself. Its header said closure_manager was
// "(leader-gated)"; closure_run_due said "Deliberately NOT leader-gated" and
// spent nine lines explaining why. Neither half could be checked, because
// there is no leader gate anywhere in core - every `leader` in the package was
// a comment, and there is no election, lock scope or API behind any of them.
// mochi.user.uid()'s own documentation told app authors the call was "typically
// to build a per-user leader scope", pointing at a pattern with no
// implementation.
//
// The rest was multi-host replication, removed July 2026. Three of these
// described code that is not there at all, which is the shape that costs a
// reader real time:
//
//   - directory.go listed "Not a pair-set member" as one of the conditions
//     directory_cleanup_dead_peers checks. It checks four things and that is
//     not one of them.
//   - passkeys.go said passkey_credential_finalize records three things,
//     including "the per-credential leadership claim". The body records two.
//   - schedule.go said 'System events (user == "") stay local'. schedule_create
//     has no such branch; it inserts whatever user it is given.
//
// This vocabulary needs exemptions the #91 gate does not, so it is checked
// separately rather than folded in:
//
//   - "replica" also means an app-level thing that is alive and well - a wikis
//     replica is another user's copy of a wiki, delivered over app-level P2P,
//     which the July removal did not touch.
//   - A comment may name replication in order to say it is GONE. queue.go does
//     exactly that in two places, deliberately, and those sentences are the
//     reason nobody re-adds the second retention floor.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// removed_coordination matches the vocabulary of the leader/replica design.
var removed_coordination = regexp.MustCompile(`(?i)leader-gate|leader-gated|leader scope|leadership|\breplicas?\b`)

// coordination_is_history matches a comment block that names the design in
// order to record that it is gone. Those sentences are load-bearing: they are
// what stops the removed thing being re-added by someone who finds its shape
// still fitting.
var coordination_is_history = regexp.MustCompile(`(?i)replication (went|was removed)|since replication was removed|no longer exists|used to be|there used to`)

// coordination_app_level matches the surviving app-level sense of "replica" -
// another user's copy of an object, delivered over the P2P that was never part
// of the removed design.
var coordination_app_level = regexp.MustCompile(`(?i)wiki replica`)

// comment_blocks returns each run of contiguous comment lines in a file, keyed
// by the line the run starts on. Block granularity is what lets a sentence
// exempt the paragraph it belongs to.
func comment_blocks(source string) map[int]string {
	blocks := map[int]string{}
	lines := strings.Split(source, "\n")
	for i := 0; i < len(lines); {
		if !strings.HasPrefix(strings.TrimSpace(lines[i]), "//") {
			i++
			continue
		}
		start := i
		var block []string
		for i < len(lines) && strings.HasPrefix(strings.TrimSpace(lines[i]), "//") {
			block = append(block, lines[i])
			i++
		}
		blocks[start+1] = strings.Join(block, "\n")
	}
	return blocks
}

// TestNoCommentClaimsALeaderOrAReplica is the gate.
func TestNoCommentClaimsALeaderOrAReplica(t *testing.T) {
	for _, file := range package_source_files(t) {
		source, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("reading %s: %v", file, err)
		}
		for line, block := range comment_blocks(string(source)) {
			found := removed_coordination.FindString(block)
			if found == "" {
				continue
			}
			if coordination_is_history.MatchString(block) || coordination_app_level.MatchString(block) {
				continue
			}
			t.Errorf("%s:%d describes a leader or a replica (%q). There is no leader gate in core, and multi-host replication was removed in July 2026:\n%s",
				file, line, found, block)
		}
	}
}

// TestNoLeaderGateExists is the fact the gate rests on. If one is ever built,
// this fails and the comments above become answerable again.
func TestNoLeaderGateExists(t *testing.T) {
	for _, file := range package_source_files(t) {
		source, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("reading %s: %v", file, err)
		}
		for number, line := range strings.Split(string(source), "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "//") || !strings.Contains(strings.ToLower(line), "leader") {
				continue
			}
			t.Errorf("%s:%d is executable code mentioning a leader: %s", file, number+1, trimmed)
		}
	}
}

// TestDetectorExemptsTheTwoLegitimateCases proves the exemptions work, so the
// gate is not passing by matching nothing.
func TestDetectorExemptsTheTwoLegitimateCases(t *testing.T) {
	cases := []struct {
		name   string
		block  string
		flag   bool
		reason string
	}{
		{
			name:   "bare claim",
			block:  "// closure_manager (leader-gated) hard-deletes the account.",
			flag:   true,
			reason: "a leader gate that does not exist",
		},
		{
			name:   "replica claim",
			block:  "// runs independently on every replica and must NOT be leader-gated",
			flag:   true,
			reason: "replicas that do not exist",
		},
		{
			name:   "recorded as removed",
			block:  "// There used to be a second, longer one for replication ops (30 days,\n// the budget within which an offline replica could replay); replication\n// went in July 2026 and took the only class it applied to.",
			flag:   false,
			reason: "names replication to record that it is gone",
		},
		{
			name:   "app-level sense",
			block:  "// required when the recipient is a private entity (not directory-listed),\n// such as a wiki replica; the app stores the peer at subscribe time",
			flag:   false,
			reason: "app-level P2P, which the removal did not touch",
		},
	}

	for _, c := range cases {
		matched := removed_coordination.MatchString(c.block)
		exempt := coordination_is_history.MatchString(c.block) || coordination_app_level.MatchString(c.block)
		flagged := matched && !exempt
		if flagged != c.flag {
			t.Errorf("%s: flagged=%v, want %v (%s)", c.name, flagged, c.flag, c.reason)
		}
	}
}

// TestCommentedCodeMatchesTheCode covers two that described operations the code
// does not perform. A comment listing a recorded thing or a branch is a claim
// about the body, and these are the ones a reader cannot disprove without
// reading the whole function.
func TestCommentedCodeMatchesTheCode(t *testing.T) {
	// passkey_credential_finalize: the comment used to list three recorded
	// things; the body records two.
	source, err := os.ReadFile("passkeys.go")
	if err != nil {
		t.Fatalf("reading passkeys.go: %v", err)
	}
	text := string(source)
	at := strings.Index(text, "func passkey_credential_finalize(")
	if at < 0 {
		t.Fatal("passkeys.go no longer defines passkey_credential_finalize")
	}
	if strings.Contains(strings.ToLower(text[:at]), "leadership claim") {
		t.Error("passkey_credential_finalize's doc claims a leadership claim again; the body records the sign count and the last-used row, and nothing else")
	}

	// schedule_create: the comment used to describe a user == "" branch.
	source, err = os.ReadFile("schedule.go")
	if err != nil {
		t.Fatalf("reading schedule.go: %v", err)
	}
	text = string(source)
	at = strings.Index(text, "func schedule_create(")
	if at < 0 {
		t.Fatal("schedule.go no longer defines schedule_create")
	}
	body := text[at:]
	if end := strings.Index(body, "\n}\n"); end > 0 {
		body = body[:end]
	}
	if strings.Contains(body, `user == ""`) {
		t.Error("schedule_create branches on an empty user; if that is now real the doc should describe it, and this test should assert what it does")
	}
}

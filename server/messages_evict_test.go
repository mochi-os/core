// Mochi server: dedup-map ceiling and verification-code generation tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"strings"
	"testing"
)

// dedup_isolate gives the test empty dedup records and restores the process's
// own, and its ceilings, afterwards.
func dedup_isolate(t *testing.T) {
	t.Helper()
	seen_messages_lock.Lock()
	saved_records, saved_sources := seen_messages, seen_sources
	saved_maximum, saved_source := seen_messages_maximum, seen_source_maximum
	seen_messages = map[string]*message_record{}
	seen_sources = map[string]*message_source{}
	seen_messages_lock.Unlock()
	t.Cleanup(func() {
		seen_messages_lock.Lock()
		seen_messages, seen_sources = saved_records, saved_sources
		seen_messages_maximum, seen_source_maximum = saved_maximum, saved_source
		seen_messages_lock.Unlock()
	})
}

// dedup_held reports whether id has a record.
func dedup_held(id string) bool {
	seen_messages_lock.Lock()
	defer seen_messages_lock.Unlock()
	return seen_messages[id] != nil
}

// The dedup map has a ceiling, and hitting it sheds the OLDEST records of the
// peers holding the most. The TTL alone does not bound it: the stream rate
// limit is charged per stream OPEN.
func TestMessageSeenEvictHonoursTheCeiling(t *testing.T) {
	dedup_isolate(t)
	seen_messages_maximum = 100

	for i := 0; i < 200; i++ {
		message_seen_mark(i64toa(int64(i)), "flooder")
	}
	seen_messages_lock.Lock()
	size := len(seen_messages)
	seen_messages_lock.Unlock()

	if size > seen_messages_maximum {
		t.Errorf("map still holds %d entries, above the %d ceiling", size, seen_messages_maximum)
	}
	if dedup_held("0") {
		t.Error("the oldest entry survived eviction; the ceiling must shed the oldest")
	}
	if !dedup_held("199") {
		t.Error("the newest entry was evicted; a retry inside the window would be applied twice")
	}
}

// Ids marked within the same second shed as readily as any. The cut by age
// that eviction used found nothing strictly older than the oldest timestamp,
// shed nothing, and then re-sorted a million ids on every later message.
func TestMessageSeenEvictShedsEqualTimestamps(t *testing.T) {
	dedup_isolate(t)
	seen_messages_maximum = 100

	// Every record in one second: nothing is older than anything else.
	for i := 0; i < seen_messages_maximum+1; i++ {
		message_seen_mark(i64toa(int64(i)), "flooder")
	}
	seen_messages_lock.Lock()
	size := len(seen_messages)
	seen_messages_lock.Unlock()
	if size > seen_messages_maximum-seen_messages_maximum/10+1 {
		t.Errorf("map holds %d records after passing its %d ceiling in one second; nothing was shed", size, seen_messages_maximum)
	}
	if dedup_held("0") {
		t.Error("the first record survived; equal timestamps shed nothing")
	}
}

// Under the ceiling, nothing is shed - eviction must not cost dedup coverage
// in normal operation.
func TestMessageSeenEvictLeavesAnUnderfullMapAlone(t *testing.T) {
	dedup_isolate(t)
	seen_messages_maximum = 100

	for i := 0; i < 50; i++ {
		message_seen_mark(i64toa(int64(i)), "peer")
	}
	seen_messages_lock.Lock()
	size := len(seen_messages)
	seen_messages_lock.Unlock()
	if size != 50 {
		t.Errorf("map holds %d entries after marking an underfull map, want 50", size)
	}
}

// One peer filling the map sheds its own records, not another's: a flooder past
// its share evicts its own oldest, and at the ceiling the peers holding the most
// shed first.
func TestMessageSeenAccountsPerPeer(t *testing.T) {
	dedup_isolate(t)
	seen_messages_maximum = 1000
	seen_source_maximum = 30

	for i := 0; i < 10; i++ {
		message_seen_mark("honest-"+i64toa(int64(i)), "honest")
	}
	for i := 0; i < 1000; i++ {
		message_seen_mark("flood-"+i64toa(int64(i)), "flooder")
	}
	for i := 0; i < 10; i++ {
		if !dedup_held("honest-" + i64toa(int64(i))) {
			t.Fatalf("an honest peer's record %d was shed by another peer's flood", i)
		}
	}
	seen_messages_lock.Lock()
	flooded := seen_sources["flooder"].size()
	seen_messages_lock.Unlock()
	if flooded > seen_source_maximum {
		t.Errorf("the flooder holds %d records, past its share of %d", flooded, seen_source_maximum)
	}

	// Many flooders, each within its share, fill the map: the ceiling sheds
	// the largest holders, so the honest peer's few still survive.
	for peer := 0; peer < 50; peer++ {
		for i := 0; i < seen_source_maximum; i++ {
			message_seen_mark("mint-"+i64toa(int64(peer))+"-"+i64toa(int64(i)), "mint-"+i64toa(int64(peer)))
		}
	}
	for i := 0; i < 10; i++ {
		if !dedup_held("honest-" + i64toa(int64(i))) {
			t.Errorf("an honest peer's record %d was shed at the ceiling while larger holders kept theirs", i)
		}
	}
}

// A copy of a message that arrives while the first is still running waits for
// its outcome: acknowledged at once, it would be lost when the first then fails
// for a reason the sender retries, since the sender has already deleted its row.
func TestMessageCopyWaitsForTheFirstOutcome(t *testing.T) {
	dedup_isolate(t)
	const id = "copy-waits"

	if state, _ := message_begin(id, "peer", nil); state != message_new {
		t.Fatalf("a first sighting was answered %d, want to run it", state)
	}
	copy := new_fake_reply()
	if state, reason := message_begin(id, "peer", copy); state != message_running || reason != "" {
		t.Fatalf("a copy of a running message was answered %d %q, want to wait", state, reason)
	}
	if copy.ack_count() != 0 || len(copy.fail_reasons()) != 0 {
		t.Fatal("the copy was answered before the first had an outcome")
	}

	// The first fails for a reason the sender retries: the copy is told so, and
	// the next copy runs.
	for _, waiting := range message_end(id, fail_transient) {
		waiting.fail(fail_transient)
	}
	if reasons := copy.fail_reasons(); len(reasons) != 1 || reasons[0] != fail_transient || copy.ack_count() != 0 {
		t.Errorf("the waiting copy got acks=%d fails=%v, want the first's transient failure", copy.ack_count(), reasons)
	}
	if state, _ := message_begin(id, "peer", nil); state != message_new {
		t.Errorf("after a retryable failure a copy was answered %d, want it run", state)
	}

	// This time it succeeds: a later copy is acknowledged from the record.
	message_end(id, "")
	if state, _ := message_begin(id, "peer", nil); state != message_applied {
		t.Errorf("a copy of an applied message was answered %d, want applied", state)
	}

	// A refusal no retry changes is answered with that refusal.
	const refused = "copy-refused"
	message_begin(refused, "peer", nil)
	message_end(refused, fail_unsupported)
	if state, reason := message_begin(refused, "peer", nil); state != message_refused || reason != fail_unsupported {
		t.Errorf("a copy of a refused message was answered %d %q, want refused %q", state, reason, fail_unsupported)
	}
}

// The copies waiting on one running message are bounded: past the bound a copy
// is answered transient rather than kept.
func TestMessageCopiesWaitingAreBounded(t *testing.T) {
	dedup_isolate(t)
	const id = "copies-bounded"
	message_begin(id, "peer", nil)
	for i := 0; i < message_waiting_maximum; i++ {
		if _, reason := message_begin(id, "peer", new_fake_reply()); reason != "" {
			t.Fatalf("copy %d of %d was turned away", i+1, message_waiting_maximum)
		}
	}
	if state, reason := message_begin(id, "peer", new_fake_reply()); state != message_running || reason != fail_transient {
		t.Errorf("a copy past the bound was answered %d %q, want running with %q", state, reason, fail_transient)
	}
	if waiting := message_end(id, ""); len(waiting) != message_waiting_maximum {
		t.Errorf("%d copies waited, want the bound of %d", len(waiting), message_waiting_maximum)
	}
}

// The TTL sweep forgets expired records and the places they held in their
// peer's order, so the peer's share is counted on what it still holds.
func TestMessageSeenCleanupForgetsExpired(t *testing.T) {
	dedup_isolate(t)
	message_seen_mark("old", "peer")
	message_seen_mark("new", "peer")
	seen_messages_lock.Lock()
	seen_messages["old"].when = now() - seen_messages_ttl - 1
	seen_messages_lock.Unlock()

	message_seen_cleanup()

	if dedup_held("old") || !dedup_held("new") {
		t.Errorf("after the sweep old held=%v new held=%v, want only the new", dedup_held("old"), dedup_held("new"))
	}
	seen_messages_lock.Lock()
	size := seen_sources["peer"].size()
	seen_messages_lock.Unlock()
	if size != 1 {
		t.Errorf("the peer is counted as holding %d records, want 1", size)
	}
}

// Verification codes are drawn uniformly from the unambiguous alphabet: a byte
// taken modulo the alphabet length favours the first (256 mod n) characters.
func TestAccountGenerateCodeIsUniform(t *testing.T) {
	const samples = 100000
	counts := make(map[rune]int, len(unambiguous))
	drawn := 0
	for drawn < samples {
		code := account_generate_code(50)
		for _, r := range code {
			if !strings.ContainsRune(unambiguous, r) {
				t.Fatalf("code contains %q, which is outside the unambiguous alphabet", r)
			}
			counts[r]++
			drawn++
		}
	}

	// Derive the (256 mod n) split from the alphabet rather than hardcoding it;
	// the set has already changed size once.
	runes := []rune(unambiguous)
	excess := 256 % len(runes)
	if excess == 0 {
		t.Skipf("alphabet of %d divides 256 exactly; modulo introduces no bias to detect", len(runes))
	}

	head, tail := 0, 0
	for i, r := range runes {
		if i < excess {
			head += counts[r]
		} else {
			tail += counts[r]
		}
	}
	head_mean := float64(head) / float64(excess)
	tail_mean := float64(tail) / float64(len(runes)-excess)
	ratio := head_mean / tail_mean

	// Unbiased this is 1.0. Biased it is (k+1)/k where k = 256/len(runes),
	// which for a 55-character alphabet is 5/4 = 1.25. At this sample size
	// the ratio's standard error is well under 1%, so 1.10 separates the two
	// by a wide margin without being flaky.
	k := 256 / len(runes)
	biased := float64(k+1) / float64(k)
	if ratio > 1.10 || ratio < 0.90 {
		t.Errorf("first-%d vs remaining-%d frequency ratio is %.3f, want ~1.0; "+
			"modulo bias over a %d-character alphabet produces ~%.2f",
			excess, len(runes)-excess, ratio, len(runes), biased)
	}
}

// Codes vary. A broken entropy source that returned a constant buffer would
// have produced one code forever under the old implementation.
func TestAccountGenerateCodeVaries(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 100; i++ {
		seen[account_generate_code(8)] = true
	}
	if len(seen) < 90 {
		t.Errorf("100 draws produced only %d distinct codes", len(seen))
	}
}

// Length is honoured.
func TestAccountGenerateCodeLength(t *testing.T) {
	for _, n := range []int{1, 6, 8, 32} {
		if got := len([]rune(account_generate_code(n))); got != n {
			t.Errorf("account_generate_code(%d) returned %d characters", n, got)
		}
	}
}

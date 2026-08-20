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

// The dedup map has a ceiling, and hitting it sheds the OLDEST entries.
//
// The TTL alone does not bound the map: the stream rate limit is charged per
// stream OPEN, not per frame, so one authenticated peer can hold a stream
// open and add an entry per message for the whole 8h window.
func TestMessageSeenEvictHonoursTheCeiling(t *testing.T) {
	saved_maximum := seen_messages_maximum
	seen_messages_lock.Lock()
	saved_map := seen_messages
	seen_messages = make(map[string]int64)
	seen_messages_lock.Unlock()
	t.Cleanup(func() {
		seen_messages_lock.Lock()
		seen_messages = saved_map
		seen_messages_maximum = saved_maximum
		seen_messages_lock.Unlock()
	})

	seen_messages_maximum = 100

	// Fill past the ceiling with a spread of ages: id N carries timestamp N,
	// so the lowest-numbered ids are the oldest.
	seen_messages_lock.Lock()
	for i := 0; i < 200; i++ {
		seen_messages[i64toa(int64(i))] = int64(i)
	}
	message_seen_evict()
	size := len(seen_messages)
	_, oldest_survived := seen_messages["0"]
	_, newest_survived := seen_messages["199"]
	seen_messages_lock.Unlock()

	if size > seen_messages_maximum {
		t.Errorf("map still holds %d entries, above the %d ceiling", size, seen_messages_maximum)
	}
	if oldest_survived {
		t.Error("the oldest entry survived eviction; the ceiling must shed by age")
	}
	if !newest_survived {
		t.Error("the newest entry was evicted; a retry inside the window would be applied twice")
	}
}

// Under the ceiling, nothing is shed - eviction must not cost dedup coverage
// in normal operation.
func TestMessageSeenEvictLeavesAnUnderfullMapAlone(t *testing.T) {
	saved_maximum := seen_messages_maximum
	seen_messages_lock.Lock()
	saved_map := seen_messages
	seen_messages = make(map[string]int64)
	seen_messages_lock.Unlock()
	t.Cleanup(func() {
		seen_messages_lock.Lock()
		seen_messages = saved_map
		seen_messages_maximum = saved_maximum
		seen_messages_lock.Unlock()
	})

	seen_messages_maximum = 100
	seen_messages_lock.Lock()
	for i := 0; i < 50; i++ {
		seen_messages[i64toa(int64(i))] = int64(i)
	}
	message_seen_evict()
	size := len(seen_messages)
	seen_messages_lock.Unlock()

	if size != 50 {
		t.Errorf("map holds %d entries after evicting an underfull map, want 50", size)
	}
}

// Verification codes are drawn uniformly from the unambiguous alphabet.
//
// The previous implementation read crypto/rand into bytes and took them
// modulo the alphabet length. 256 is not a multiple of 54, so indices 0..39
// came up 5/256 of the time and 40..53 only 4/256 - a 25% excess across the
// first 40 characters. It also ignored rand.Read's error, so a failing
// entropy source yielded an all-zeroes buffer and therefore the SAME code
// every call, silently.
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

	// Taking a uniform byte modulo n favours the first (256 mod n) indices,
	// which get one extra representative among the 256 byte values. Derive
	// that split from the alphabet rather than hardcoding it, so the test
	// stays honest if the alphabet changes - as it already caught once, the
	// set being 55 characters and not the 54 first assumed here.
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

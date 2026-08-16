// Mochi server: mochi.text.valid's pattern is the app's, not core's.
//
// valid() falls through to the raw pattern when the name is not a known
// validator type, and that landed in regex_cached: MustCompile, so "(" panicked
// out of the builtin, into a process-global map with no ceiling, keyed on
// whatever the app cared to invent. Measured at ~2KB retained per entry, held
// for the life of the process - a loop reaches gigabytes and never gives them
// back, across every user on the server.
//
// The raw-regex fallthrough itself stays: system_settings validates most of its
// values with one, and four app call sites use one too. What changes is where
// an app's pattern is compiled and how long the result lives.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	sl "go.starlark.net/starlark"
)

func text_valid_call(t *testing.T, thread *sl.Thread, s, match string) (sl.Value, error) {
	t.Helper()
	fn := sl.NewBuiltin("mochi.text.valid", api_text_valid)
	return api_text_valid(thread, fn, sl.Tuple{sl.String(s), sl.String(match)}, nil)
}

// TestTextValidBadPatternIsAnError. "(" used to reach MustCompile and panic out
// of the builtin; the operator got a stack trace for what is purely an app bug.
// It is now reported to the author, naming the pattern.
func TestTextValidBadPatternIsAnError(t *testing.T) {
	thread := &sl.Thread{}
	_, err := text_valid_call(t, thread, "x", "(")
	if err == nil {
		t.Fatal("an uncompilable pattern was accepted")
	}
	if !strings.Contains(err.Error(), "invalid match pattern") {
		t.Errorf("error was %q, want one naming the bad pattern", err)
	}
}

// TestTextValidAppPatternsStayOffTheGlobalCache is the finding. An app's
// patterns must not accumulate in the process-global map, which nothing bounds
// and nothing frees.
func TestTextValidAppPatternsStayOffTheGlobalCache(t *testing.T) {
	regex_cache_mu.Lock()
	before := len(regex_cache)
	regex_cache_mu.Unlock()

	thread := &sl.Thread{}
	for i := 0; i < 500; i++ {
		if _, err := text_valid_call(t, thread, "x", fmt.Sprintf("^invented%d$", i)); err != nil {
			t.Fatalf("pattern %d rejected: %v", i, err)
		}
	}

	regex_cache_mu.Lock()
	after := len(regex_cache)
	regex_cache_mu.Unlock()
	if after != before {
		t.Errorf("the global cache grew by %d entries; app patterns must not be retained there", after-before)
	}
}

// TestTextValidSessionCacheIsBounded. Within a session the cache is what makes
// a loop over one pattern cheap, but its key space is still the app's, so it
// needs a ceiling of its own.
func TestTextValidSessionCacheIsBounded(t *testing.T) {
	thread := &sl.Thread{}
	for i := 0; i < regex_session_maximum+200; i++ {
		if _, err := text_valid_call(t, thread, "x", fmt.Sprintf("^invented%d$", i)); err != nil {
			t.Fatalf("pattern %d rejected: %v", i, err)
		}
	}
	cache, _ := thread.Local("regexes").(map[string]*regexp.Regexp)
	if cache == nil {
		t.Fatal("no session cache was created")
	}
	if len(cache) > regex_session_maximum {
		t.Errorf("session cache holds %d entries, above the ceiling of %d", len(cache), regex_session_maximum)
	}
}

// TestTextValidPastTheCeilingStillWorks. Refusing to CACHE must not become
// refusing to validate - the ceiling trades heap for CPU, it does not change
// the answer.
func TestTextValidPastTheCeilingStillWorks(t *testing.T) {
	thread := &sl.Thread{}
	for i := 0; i < regex_session_maximum+10; i++ {
		if _, err := text_valid_call(t, thread, "x", fmt.Sprintf("^filler%d$", i)); err != nil {
			t.Fatalf("filler %d rejected: %v", i, err)
		}
	}
	// A fresh pattern with the cache full still has to give the right answer.
	yes, err := text_valid_call(t, thread, "hello", "^hello$")
	if err != nil {
		t.Fatalf("pattern past the ceiling errored: %v", err)
	}
	if yes != sl.True {
		t.Error("a matching pattern past the ceiling returned false")
	}
	no, err := text_valid_call(t, thread, "hello", "^goodbye$")
	if err != nil {
		t.Fatalf("pattern past the ceiling errored: %v", err)
	}
	if no != sl.False {
		t.Error("a non-matching pattern past the ceiling returned true")
	}
}

// TestTextValidSessionCacheIsPerSession. The whole point of hanging it on the
// thread: AppVersion.starlark() builds a fresh one per invocation, so the cache
// dies with the handler rather than outliving it.
func TestTextValidSessionCacheIsPerSession(t *testing.T) {
	first := &sl.Thread{}
	if _, err := text_valid_call(t, first, "x", "^only-in-first$"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	second := &sl.Thread{}
	if _, err := text_valid_call(t, second, "x", "^only-in-second$"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	one, _ := first.Local("regexes").(map[string]*regexp.Regexp)
	two, _ := second.Local("regexes").(map[string]*regexp.Regexp)
	if len(one) != 1 || len(two) != 1 {
		t.Fatalf("caches hold %d and %d entries, want one each", len(one), len(two))
	}
	if _, leaked := two["^only-in-first$"]; leaked {
		t.Error("one session's pattern reached another's cache")
	}
}

// TestTextValidNamedTypesUnchanged. The named vocabulary is what 461 of the
// app tree's calls use; routing app patterns elsewhere must not disturb it.
func TestTextValidNamedTypesUnchanged(t *testing.T) {
	thread := &sl.Thread{}
	for _, c := range []struct {
		value, match string
		want         sl.Value
	}{
		{"abc123", "id", sl.False}, // id is 32 chars
		{strings.Repeat("a", 32), "id", sl.True},
		{"9charsxyz", "fingerprint", sl.True},
		{"toolongforafingerprint", "fingerprint", sl.False},
		{"42", "natural", sl.True},
		{"-1", "natural", sl.False},
		{"public", "privacy", sl.True},
		{"secret", "privacy", sl.False},
	} {
		got, err := text_valid_call(t, thread, c.value, c.match)
		if err != nil {
			t.Errorf("valid(%q, %q) errored: %v", c.value, c.match, err)
			continue
		}
		if got != c.want {
			t.Errorf("valid(%q, %q) = %v, want %v", c.value, c.match, got, c.want)
		}
	}
}

// TestValidStillUsesTheGlobalCache. Core's own callers pass compile-time
// constants and are called on every request, so they must keep the permanent
// cache - the reason the two paths are separate rather than both bounded.
func TestValidStillUsesTheGlobalCache(t *testing.T) {
	pattern := "^core-constant-probe$"
	regex_cache_mu.Lock()
	delete(regex_cache, pattern)
	regex_cache_mu.Unlock()

	if !valid("core-constant-probe", pattern) {
		t.Fatal("valid() rejected a matching constant pattern")
	}

	regex_cache_mu.Lock()
	_, cached := regex_cache[pattern]
	regex_cache_mu.Unlock()
	if !cached {
		t.Error("valid() no longer populates the global cache; core's validators would recompile per call")
	}
}

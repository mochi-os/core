// Mochi server: Rate limiter sweep coverage
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// TestCleanupRemovesExpiredEntries — an unswept limiter keyed on a caller's
// address or peer id grows for as long as the process runs. cleanup drops
// entries past their window; these are the two shapes that create them.
func TestCleanupRemovesExpiredEntries(t *testing.T) {
	for _, test := range []struct {
		name string
		fill func(r *rate_limiter, key string)
	}{
		{"allow", func(r *rate_limiter, key string) { r.allow(key) }},
		{"spend", func(r *rate_limiter, key string) { r.spend(key, 1) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := &rate_limiter{entries: map[string]*rate_limit_entry{}, limit: 10, window: 60}
			test.fill(r, "198.51.100.7")
			test.fill(r, "198.51.100.8")
			if len(r.entries) != 2 {
				t.Fatalf("setup: %d entries, want 2", len(r.entries))
			}

			// Nothing has expired yet, so a sweep must not discard live state.
			r.cleanup()
			if len(r.entries) != 2 {
				t.Errorf("cleanup dropped %d live entr(ies)", 2-len(r.entries))
			}

			// Age one past its window; the other stays current.
			r.entries["198.51.100.7"].reset = now() - 1
			r.cleanup()
			if _, still := r.entries["198.51.100.7"]; still {
				t.Error("an expired entry survived the sweep")
			}
			if _, gone := r.entries["198.51.100.8"]; !gone {
				t.Error("a live entry was swept")
			}
		})
	}
}

// TestEveryRateLimiterIsSwept — the regression test for the actual defect.
//
// Six of the seventeen limiters were missing from ratelimit_manager, three of
// them keyed on values a caller chooses, so their maps grew from remote input
// with no ceiling. Nothing detected that: an unswept limiter makes exactly the
// same decisions as a swept one, because allow and spend both treat an expired
// entry as absent. Only memory suffers, and silently.
//
// So this reads the source rather than the runtime - a package variable cannot
// be enumerated otherwise - and fails when a limiter is declared without being
// added to the sweep. Adding a limiter and forgetting the manager is the
// mistake it exists to catch.
func TestEveryRateLimiterIsSwept(t *testing.T) {
	source, err := os.ReadFile("ratelimit.go")
	if err != nil {
		t.Fatalf("read ratelimit.go: %v", err)
	}
	text := string(source)

	declared := regexp.MustCompile(`(?m)^\s*([a-z_0-9]+)\s*=\s*&rate_limiter\{`)
	names := []string{}
	for _, m := range declared.FindAllStringSubmatch(text, -1) {
		names = append(names, m[1])
	}
	if len(names) < 10 {
		t.Fatalf("found only %d limiter declarations; the pattern has stopped matching", len(names))
	}

	start := strings.Index(text, "func ratelimit_manager()")
	if start < 0 {
		t.Fatal("ratelimit_manager not found")
	}
	end := strings.Index(text[start:], "\n}")
	if end < 0 {
		t.Fatal("could not find the end of ratelimit_manager")
	}
	manager := text[start : start+end]

	missing := []string{}
	for _, name := range names {
		if !strings.Contains(manager, name+".cleanup()") {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("declared but never swept, so these maps grow until the process restarts: %s",
			strings.Join(missing, ", "))
	}
}

// Mochi server: user-facing strings are not sorted in SQL
// Copyright © 2026 Mochisoft OÜ
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

// mochi.group.list used to end "order by name". SQLite's default collation is
// byte order, so "Ávila" sorts after "Zurich" and "abc" after "ZZZ", and the
// ordering would change again under Postgres. The consumer sorts - web with
// naturalCompare, Starlark with mochi.text.compare - so the query orders by an
// intrinsic column and only has to be stable.
//
// Source-level because the defect is the query text: a behavioural test would
// pass on any locale whose accented names happen not to collide.
func TestGroupListDoesNotSortByName(t *testing.T) {
	body, err := os.ReadFile("groups.go")
	if err != nil {
		t.Fatalf("reading groups.go: %v", err)
	}
	source := string(body)

	start := strings.Index(source, "func api_group_list")
	if start < 0 {
		t.Fatal("api_group_list not found; this test no longer checks what it claims")
	}
	end := strings.Index(source[start+1:], "\nfunc ")
	if end < 0 {
		t.Fatal("could not find the end of api_group_list")
	}
	list := source[start : start+1+end]

	// Comments in this function say "ORDER BY name" to explain why it is gone,
	// so scan the SQL string literals rather than the whole body.
	queries := strings.Join(regexp.MustCompile(`"[^"]*(?i:select)[^"]*"`).FindAllString(list, -1), "\n")
	if queries == "" {
		t.Fatal("no SELECT literal found in api_group_list; this test no longer checks what it claims")
	}

	sorts := regexp.MustCompile(`(?i)order by\s+(name|title|label|display)\b`)
	if m := sorts.FindString(queries); m != "" {
		t.Errorf("api_group_list sorts on a user-facing string in SQL (%q): accents and locale come out wrong, and the collation does not survive a move off SQLite", m)
	}
	if !strings.Contains(strings.ToLower(queries), "order by") {
		t.Error("api_group_list has no ORDER BY at all; the row order is then whatever the query planner gives, which is not stable across inserts")
	}
}

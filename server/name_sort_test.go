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

// Three APIs used to end their query "order by name". SQLite's default
// collation is byte order, so "Ávila" sorts after "Zurich" and "abc" after
// "ZZZ", and the ordering would change again under Postgres. Each now orders
// on an intrinsic column; whoever displays the names sorts them - the web with
// naturalCompare, Starlark with mochi.text.compare, and directory search in Go
// (see TestDirectorySortFoldsCaseAndAccents below, where the caller cannot).
//
// Source-level because the defect is the query text: a behavioural test would
// pass on any locale whose accented names happen not to collide.
var name_sort_sites = []struct{ file, function string }{
	{"groups.go", "api_group_list"},
	{"entities.go", "api_entity_owned"},
	{"directory.go", "api_directory_search"},
}

func TestQueriesDoNotSortByName(t *testing.T) {
	sorts := regexp.MustCompile(`(?i)order by\s+(name|title|label|display)\b`)
	// Comments in these functions name "ORDER BY name" to explain why it is
	// gone, so scan the SQL string literals rather than the whole body.
	literals := regexp.MustCompile(`"[^"]*(?i:select)[^"]*"`)

	for _, site := range name_sort_sites {
		body, err := os.ReadFile(site.file)
		if err != nil {
			t.Errorf("reading %s: %v", site.file, err)
			continue
		}
		source := string(body)

		start := strings.Index(source, "func "+site.function)
		if start < 0 {
			t.Errorf("%s not found in %s; this test no longer checks what it claims", site.function, site.file)
			continue
		}
		// The last function in a file has no following "\nfunc "; its body runs
		// to the end.
		body_end := len(source)
		if end := strings.Index(source[start+1:], "\nfunc "); end >= 0 {
			body_end = start + 1 + end
		}
		queries := strings.Join(literals.FindAllString(source[start:body_end], -1), "\n")
		if queries == "" {
			t.Errorf("no SELECT literal found in %s; this test no longer checks what it claims", site.function)
			continue
		}

		if m := sorts.FindString(queries); m != "" {
			t.Errorf("%s sorts on a user-facing string in SQL (%q): accents and locale come out wrong, and the collation does not survive a move off SQLite", site.function, m)
		}
		if !strings.Contains(strings.ToLower(queries), "order by") {
			t.Errorf("%s has no ORDER BY at all; the row order is then whatever the query planner gives, which is not stable across inserts", site.function)
		}
	}
}

// mochi.directory.search is the one that cannot hand the ordering to its
// caller: the API takes no limit, and its callers keep only the first 20
// (chat person search) or 50 (apps search) of what comes back. So the order
// decides which entries a user sees at all, and it has to be the accent- and
// case-folded one rather than SQLite's byte order.
func TestDirectorySortFoldsCaseAndAccents(t *testing.T) {
	ds := []map[string]any{
		{"id": "e1", "name": "Zurich", "created": int64(10)},
		{"id": "e2", "name": "ana", "created": int64(20)},
		{"id": "e3", "name": "Ávila", "created": int64(30)},
		{"id": "e4", "name": "Bogotá", "created": int64(40)},
	}
	directory_sort(ds)

	var got []string
	for _, d := range ds {
		got = append(got, d["name"].(string))
	}
	want := []string{"ana", "Ávila", "Bogotá", "Zurich"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("directory_sort = %v, want %v", got, want)
		}
	}
}

// Equal names are ordered oldest first, so an impersonator registering the
// same name later cannot sort above the original. This is the tie-break
// apps/people already applied for itself; doing it here gives it to every
// caller, including the ones that truncate.
func TestDirectorySortPutsTheOlderDuplicateFirst(t *testing.T) {
	ds := []map[string]any{
		{"id": "impostor", "name": "Alistair", "created": int64(900)},
		{"id": "original", "name": "alistair", "created": int64(100)},
	}
	directory_sort(ds)

	if ds[0]["id"] != "original" {
		t.Errorf("first entry is %q, want the older \"original\": a later registration of the same name must not sort above it", ds[0]["id"])
	}
}

// sl_encode hands numbers back in whatever shape the driver produced, so the
// tie-break must not silently read every created stamp as zero.
func TestDirectorySortReadsEveryNumericShape(t *testing.T) {
	for _, c := range []struct {
		name  string
		value any
	}{
		{"int64", int64(7)},
		{"int", 7},
		{"float64", float64(7)},
	} {
		if got := created_of(map[string]any{"created": c.value}); got != 7 {
			t.Errorf("created_of(%s) = %d, want 7", c.name, got)
		}
	}
	if got := created_of(map[string]any{}); got != 0 {
		t.Errorf("created_of(missing) = %d, want 0", got)
	}
}

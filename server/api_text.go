// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"regexp"
	"strings"
	"unicode"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
	"golang.org/x/text/collate"
	"golang.org/x/text/language"
	"golang.org/x/text/runes"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

// mochi.text.compare(a, b) -> int: Locale-friendly string comparison, case-
// and accent-insensitive. Returns -1 if a < b, 0 if equal, 1 if a > b. Use
// when sorting must happen in Starlark — for SQL-driven lists, push the sort
// to the consumer (web's naturalCompare) and don't sort by name in SQL.
func api_text_compare(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return sl_error(fn, "syntax: <a: string>, <b: string>")
	}
	a, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "a must be a string")
	}
	b, ok := sl.AsString(args[1])
	if !ok {
		return sl_error(fn, "b must be a string")
	}
	c := collate.New(language.Und, collate.IgnoreCase, collate.IgnoreDiacritics, collate.Numeric)
	return sl.MakeInt(c.CompareString(a, b)), nil
}

// mochi.text.markdown(markdown) -> string: Render markdown to HTML
func api_text_markdown(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <markdown: string>")
	}

	in, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid markdown")
	}

	return sl_encode(string(markdown([]byte(in)))), nil
}

// mochi.text.sortkey(s) -> string: Returns a normalised form of `s` suitable
// as a `key=` argument to `sorted()` for case- and accent-insensitive sort.
// Lowercases, NFD-decomposes, and strips combining marks (accents). The
// returned string is opaque — don't use it for display, only for comparison.
//
//	sorted(items, key=lambda x: mochi.text.sortkey(x["name"]))
func api_text_sortkey(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <s: string>")
	}
	s, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "s must be a string")
	}
	return sl.String(text_sortkey(s)), nil
}

// regex_session_maximum bounds one Starlark session's regex cache. A session is
// a single action or event invocation - AppVersion.starlark() builds a fresh
// thread per call, sharing only the compiled globals - and starlark_sem caps
// concurrent sessions at 32, so the worst case is that many caches of this size,
// all released when their handlers return.
const regex_session_maximum = 1000

// regex_session compiles pattern into a cache on the calling Starlark thread,
// for patterns an app supplies rather than the compile-time constants
// regex_cached holds.
//
// The process-global cache cannot hold these. Its key space would be whatever
// an app cares to invent, each entry retaining a compiled program for the life
// of the process - measured at ~2KB, so a loop reaches gigabytes and never
// gives them back. Nor is a ceiling on the global cache enough: core's own
// validators compile lazily on first use, so a flood that filled it would leave
// some of them recompiling on every request forever.
//
// Past the ceiling patterns still compile and still work; they are simply not
// retained, which turns the cost into CPU inside the caller's own timeout
// rather than heap nobody can reclaim.
func regex_session(t *sl.Thread, pattern string) (*regexp.Regexp, error) {
	if t == nil {
		return regexp.Compile(pattern)
	}
	cache, _ := t.Local("regexes").(map[string]*regexp.Regexp)
	if cache == nil {
		cache = map[string]*regexp.Regexp{}
		t.SetLocal("regexes", cache)
	}
	if compiled, ok := cache[pattern]; ok {
		return compiled, nil
	}
	compiled, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	if len(cache) < regex_session_maximum {
		cache[pattern] = compiled
	}
	return compiled, nil
}

// mochi.text.valid(string, pattern) -> bool: Check if a string matches a validation pattern
func api_text_valid(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return sl_error(fn, "syntax: <string to check: string>, <pattern to match: string>")
	}

	if args[0] == sl.None {
		return sl.False, nil
	}
	s, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid string to check %q", s)
	}

	match, ok := sl.AsString(args[1])
	if !ok {
		return sl_error(fn, "invalid match pattern %q", match)
	}

	// The pattern is the app's, so it is compiled into this session's cache
	// rather than the process-global one, and a bad one is reported to the
	// app author instead of panicking out of MustCompile.
	var failure error
	result := valid_with(s, match, func(pattern string) *regexp.Regexp {
		compiled, err := regex_session(t, pattern)
		if err != nil {
			failure = err
			return nil
		}
		return compiled
	})
	if failure != nil {
		return sl_error(fn, "invalid match pattern %q: %v", match, failure)
	}
	return sl_encode(result), nil
}

// mochi.text.slug(s) -> string: Convert s to a URL-friendly slug. Strips
// accents, lower-cases, replaces runs of non-letter/digit characters with a
// single dash, and trims leading/trailing dashes. Letters from any script
// are preserved ("Café Olé" → "cafe-ole", "你好 世界" → "你好-世界"); only
// punctuation/whitespace is collapsed. Returns "" if nothing slug-worthy
// remains.
func api_text_slug(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <s: string>")
	}
	s, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "s must be a string")
	}
	return sl.String(text_slug(s)), nil
}

// text_slug is the Go-callable version.
func text_slug(s string) string {
	cleaned := text_sortkey(s)
	var b strings.Builder
	prev_dash := true // suppress leading dashes
	for _, r := range cleaned {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
			prev_dash = false
		} else if !prev_dash {
			b.WriteRune('-')
			prev_dash = true
		}
	}
	return strings.TrimRight(b.String(), "-")
}

// text_sortkey is the Go-callable version. NFD-normalise → strip combining
// marks → recompose → lower-case. "Café" → "cafe", "Über" → "uber".
func text_sortkey(s string) string {
	t := transform.Chain(
		norm.NFD,
		runes.Remove(runes.In(unicode.Mn)),
		norm.NFC,
	)
	out, _, err := transform.String(t, s)
	if err != nil {
		return strings.ToLower(s)
	}
	return strings.ToLower(out)
}

var api_text = sls.FromStringDict(sl.String("mochi.text"), sl.StringDict{
	"compare":  sl.NewBuiltin("mochi.text.compare", api_text_compare),
	"markdown": sl.NewBuiltin("mochi.text.markdown", api_text_markdown),
	"slug":     sl.NewBuiltin("mochi.text.slug", api_text_slug),
	"sortkey":  sl.NewBuiltin("mochi.text.sortkey", api_text_sortkey),
	"valid":    sl.NewBuiltin("mochi.text.valid", api_text_valid),
})

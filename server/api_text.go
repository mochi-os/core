package main

import (
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

	return sl_encode(valid(s, match)), nil
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
	"sortkey":  sl.NewBuiltin("mochi.text.sortkey", api_text_sortkey),
	"valid":    sl.NewBuiltin("mochi.text.valid", api_text_valid),
})

// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"html"
	"regexp"
	"sort"
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

// text_sort orders items by the named string field the way mochi.text.compare
// orders strings: case- and accent-insensitive, numeric-aware. The server's
// own name-bearing lists use it, so Étoile sorts among the Es and Sprint 2
// precedes Sprint 10, the order the web layer's naturalCompare produces.
func text_sort[T ~map[string]any](items []T, key string) {
	c := collate.New(language.Und, collate.IgnoreCase, collate.IgnoreDiacritics, collate.Numeric)
	sort.SliceStable(items, func(i, j int) bool {
		a, _ := items[i][key].(string)
		b, _ := items[j][key].(string)
		return c.CompareString(a, b) < 0
	})
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

// mochi.text.plain(markdown) -> string: Render markdown to plain text: the
// text of its HTML, with block ends and line breaks as newlines, for a network
// that takes text and not markup
func api_text_plain(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <markdown: string>")
	}
	in, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid markdown")
	}
	return sl_encode(html_text(string(markdown([]byte(in))))), nil
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
// a single action or event invocation, and starlark_semaphore caps concurrency
// at 32, so the worst case is 32 caches of this size, all released on return.
const regex_session_maximum = 1000

// regex_session compiles an app-supplied pattern into a per-thread cache. The
// process-global cache cannot hold these: its key space would be the app's,
// each entry retained for the life of the process. Past the ceiling patterns
// still compile, they are simply not retained.
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
	previous_dash := true // suppress leading dashes
	for _, r := range cleaned {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
			previous_dash = false
		} else if !previous_dash {
			b.WriteRune('-')
			previous_dash = true
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
	"compare":   sl.NewBuiltin("mochi.text.compare", api_text_compare),
	"fediverse": sl.NewBuiltin("mochi.text.fediverse", api_text_fediverse),
	"markdown":  sl.NewBuiltin("mochi.text.markdown", api_text_markdown),
	"plain":     sl.NewBuiltin("mochi.text.plain", api_text_plain),
	"slug":      sl.NewBuiltin("mochi.text.slug", api_text_slug),
	"sortkey":   sl.NewBuiltin("mochi.text.sortkey", api_text_sortkey),
	"valid":     sl.NewBuiltin("mochi.text.valid", api_text_valid),
})

// === The fediverse's form of a post ===
//
// A post goes to the fediverse as HTML in which a mention is a link to the
// person's page on their server and a hashtag a link to the tag, each marked
// as Mastodon marks them so a receiving server shows them as its own. The
// mentions and hashtags are reported beside the HTML, for the Mention and
// Hashtag tags the object carries and the people it is addressed to.

// fediverse_mention is @user@host in text: the name as servers allow it, the
// host with a dot in it, not run on from a word, a path or an address.
var fediverse_mention = regexp.MustCompile(`(^|[^\w/@.])@([a-zA-Z0-9_.-]+)@([a-zA-Z0-9][a-zA-Z0-9.-]*\.[a-zA-Z]{2,})`)

// fediverse_hashtag is #tag in text: letters, digits and underscores with at
// least one letter, not run on from a word, an entity or an address.
var fediverse_hashtag = regexp.MustCompile(`(^|[^\w&/#])#([\p{L}\p{N}_]*\p{L}[\p{L}\p{N}_]*)`)

// fediverse_markup is one HTML tag.
var fediverse_markup = regexp.MustCompile(`<[^>]*>`)

// fediverse_render renders markdown as the fediverse shows it: the sanitised
// HTML, with mentions and hashtags linked in its text outside links, code and
// preformatted text, and the mentions and hashtags found, each once.
func fediverse_render(markdown_text string, tags string) (string, []map[string]any, []string) {
	source := string(markdown([]byte(markdown_text)))
	var out strings.Builder
	var mentions []map[string]any
	var hashtags []string
	seen := map[string]bool{}
	depth := 0 // inside a, code or pre
	position := 0
	for _, span := range fediverse_markup.FindAllStringIndex(source, -1) {
		if depth == 0 {
			out.WriteString(fediverse_text(source[position:span[0]], tags, &mentions, &hashtags, seen))
		} else {
			out.WriteString(source[position:span[0]])
		}
		tag := source[span[0]:span[1]]
		name := strings.ToLower(strings.TrimLeft(strings.Fields(strings.Trim(tag, "<>/"))[0], "/"))
		if name == "a" || name == "code" || name == "pre" {
			if strings.HasPrefix(tag, "</") {
				if depth > 0 {
					depth--
				}
			} else if !strings.HasSuffix(tag, "/>") {
				depth++
			}
		}
		out.WriteString(tag)
		position = span[1]
	}
	if depth == 0 {
		out.WriteString(fediverse_text(source[position:], tags, &mentions, &hashtags, seen))
	} else {
		out.WriteString(source[position:])
	}
	if mentions == nil {
		mentions = []map[string]any{}
	}
	if hashtags == nil {
		hashtags = []string{}
	}
	return out.String(), mentions, hashtags
}

// fediverse_text links the mentions and hashtags in one run of text.
func fediverse_text(text string, tags string, mentions *[]map[string]any, hashtags *[]string, seen map[string]bool) string {
	text = fediverse_mention.ReplaceAllStringFunc(text, func(found string) string {
		m := fediverse_mention.FindStringSubmatch(found)
		user, host := m[2], strings.ToLower(m[3])
		handle := "@" + user + "@" + host
		page := "https://" + host + "/@" + user
		if !seen[strings.ToLower(handle)] {
			seen[strings.ToLower(handle)] = true
			*mentions = append(*mentions, map[string]any{"handle": handle, "url": page})
		}
		return m[1] + `<span class="h-card" translate="no"><a href="` + html.EscapeString(page) + `" class="u-url mention">@<span>` + html.EscapeString(user) + `</span></a></span>`
	})
	text = fediverse_hashtag.ReplaceAllStringFunc(text, func(found string) string {
		m := fediverse_hashtag.FindStringSubmatch(found)
		tag := m[2]
		if !seen["#"+strings.ToLower(tag)] {
			seen["#"+strings.ToLower(tag)] = true
			*hashtags = append(*hashtags, tag)
		}
		if tags == "" {
			return found
		}
		return m[1] + `<a href="` + html.EscapeString(tags+tag) + `" class="mention hashtag" rel="tag">#<span>` + html.EscapeString(tag) + `</span></a>`
	})
	return text
}

// mochi.text.fediverse(markdown, tags="") -> dict: Render markdown as the fediverse shows a post: {html, mentions, hashtags}. In the HTML a mention @user@host is a link to the person's page on their server and a hashtag #tag a link under the tags address given, each marked as Mastodon marks them; with no tags address a hashtag stays as text. Mentions are [{handle, url}] and hashtags the tags as written, each once; mentions and hashtags inside links, code and preformatted text are left as they are
func api_text_fediverse(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var in, tags string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "markdown", &in, "tags?", &tags); err != nil {
		return nil, err
	}
	rendered, mentions, hashtags := fediverse_render(in, tags)
	return sl_encode(map[string]any{"html": rendered, "mentions": mentions, "hashtags": hashtags}), nil
}

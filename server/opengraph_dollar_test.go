// Mochi server: a dollar sign in an OpenGraph value is text, not a reference.
//
// The three replacers build a replacement string with the app-supplied value
// inside it and handed it to ReplaceAllString, which reads $ as a capture-group
// reference. escape_attr covers the HTML-significant characters - &, ", <, > -
// and not this one, because $ is regexp-replacement syntax rather than HTML.
// The patterns have no capture groups, so every $N and $name resolved to empty
// and took the digits or word after it with them: "Cost: $100" rendered as
// "Cost: ".
//
// It only ever deletes, never introduces markup, so this is silent corruption
// rather than injection - and it lands in the preview Slack, Discord and
// crawlers fetch, never in the page the author is looking at.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"strings"
	"testing"
)

// TestOpenGraphKeepsDollarSigns is the finding, across all three replacers,
// because they share one store of the same mistake: a cap on some of them is no
// cap at all. Prices are the obvious case - a marketplace listing title is
// exactly the text most likely to carry a dollar sign.
func TestOpenGraphKeepsDollarSigns(t *testing.T) {
	for _, value := range []string{
		"Cost: $100",    // $1 consumed the digits after it
		"Save $5 today", // $5 is a group reference
		"$name here",    // a named reference
		"Price $$",      // $$ collapsed to one $
		"A $ sign",      // a bare $ survived, so the corruption was inconsistent
		"$1 $2 $3",      // several at once
	} {
		t.Run(value, func(t *testing.T) {
			got := regexp_replace_meta(`<meta property="og:title" content="placeholder" />`, "og:title", value)
			if !strings.Contains(got, `content="`+value+`"`) {
				t.Errorf("og:title rendered as %s, want the value verbatim", got)
			}

			got = regexp_replace_meta_name(`<meta name="description" content="placeholder" />`, "description", value)
			if !strings.Contains(got, `content="`+value+`"`) {
				t.Errorf("meta name rendered as %s, want the value verbatim", got)
			}

			got = regexp_replace_tag(`<title>placeholder</title>`, "title", value)
			if !strings.Contains(got, `<title>`+value+`</title>`) {
				t.Errorf("title tag rendered as %s, want the value verbatim", got)
			}
		})
	}
}

// TestEscapeAttrLeavesDollarAlone states why the fix belongs in the replacer
// rather than in escape_attr. Escaping $ to $$ there would also make the
// dollar-sign test above pass - but escape_attr has twelve other callers in
// web.go that build HTML by concatenation with no regexp anywhere near them
// (<base href>, mochi:app, mochi:class, mochi:entity, mochi:fingerprint,
// mochi:domain), and every one of those would then render a doubled dollar. $$
// is regexp-replacement syntax; escape_attr produces HTML attribute escaping.
func TestEscapeAttrLeavesDollarAlone(t *testing.T) {
	for _, value := range []string{"$", "$$", "Cost: $100", "a $name b"} {
		if got := escape_attr(value); got != value {
			t.Errorf("escape_attr(%q) = %q; $ is not HTML-significant, and doubling it here would corrupt the twelve callers that concatenate the result straight into markup", value, got)
		}
	}
}

// TestOpenGraphStillEscapesMarkup. Taking the replacement literally must not
// weaken escape_attr: the characters that could close the attribute or open a
// tag still have to be entities, or a literal replacement would trade silent
// corruption for injection.
func TestOpenGraphStillEscapesMarkup(t *testing.T) {
	got := regexp_replace_meta(`<meta property="og:title" content="placeholder" />`, "og:title",
		`" onload="alert(1)`+`<script>`+` & `)

	for _, raw := range []string{`" onload=`, `<script>`} {
		if strings.Contains(got, raw) {
			t.Errorf("rendered %s, which still carries %q unescaped", got, raw)
		}
	}
	for _, entity := range []string{"&quot;", "&lt;script&gt;", "&amp;"} {
		if !strings.Contains(got, entity) {
			t.Errorf("rendered %s, which is missing the escape %q", got, entity)
		}
	}
}

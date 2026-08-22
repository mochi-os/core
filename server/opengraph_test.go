// Mochi server: a dollar sign in an OpenGraph value is text, not a regexp
// replacement reference. escape_attribute does not cover $ - it is not
// HTML-significant - so the replacers must take the value literally.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

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

// TestEscapeAttrLeavesDollarAlone: the fix belongs in the replacer, not in
// escape_attribute - $ is not HTML-significant, and doubling it there would
// corrupt the callers that concatenate the result straight into markup.
func TestEscapeAttrLeavesDollarAlone(t *testing.T) {
	for _, value := range []string{"$", "$$", "Cost: $100", "a $name b"} {
		if got := escape_attribute(value); got != value {
			t.Errorf("escape_attribute(%q) = %q; $ is not HTML-significant, and doubling it here would corrupt the twelve callers that concatenate the result straight into markup", value, got)
		}
	}
}

// TestOpenGraphStillEscapesMarkup. Taking the replacement literally must not
// weaken escape_attribute: the characters that could close the attribute or open a
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

// Mochi server: making an app's og:image absolute.
// TestOpenGraphAbsolute covers the shapes apps actually emit. The entity-route
// case is the defect: people and feeds both emit "-/avatar" from a page served at
// /<app>/<fingerprint>, and ordinary relative resolution would drop the
// fingerprint and point at an asset route that does not exist.
func TestOpenGraphAbsolute(t *testing.T) {
	cases := []struct {
		name   string
		image  string
		scheme string
		host   string
		path   string
		want   string
	}{
		{
			"entity route without a trailing slash",
			"-/avatar", "https", "mochi-os.org", "/people/1abcdef",
			"https://mochi-os.org/people/1abcdef/-/avatar",
		},
		{
			"entity route with a trailing slash",
			"-/avatar", "https", "mochi-os.org", "/people/1abcdef/",
			"https://mochi-os.org/people/1abcdef/-/avatar",
		},
		{
			"direct entity route",
			"-/avatar", "https", "mochi-os.org", "/1abcdef",
			"https://mochi-os.org/1abcdef/-/avatar",
		},
		{
			"root-relative is host-relative, not path-relative",
			"/static/logo.png", "https", "mochi-os.org", "/people/1abcdef",
			"https://mochi-os.org/static/logo.png",
		},
		{
			"an absolute URL is left alone",
			"https://cdn.example.com/a.png", "https", "mochi-os.org", "/people/1abcdef",
			"https://cdn.example.com/a.png",
		},
		{
			"a protocol-relative URL is left alone",
			"//cdn.example.com/a.png", "https", "mochi-os.org", "/people/1abcdef",
			"//cdn.example.com/a.png",
		},
		{
			"http when the server is not serving https",
			"-/avatar", "http", "localhost:8081", "/people/1abcdef",
			"http://localhost:8081/people/1abcdef/-/avatar",
		},
		{
			"a domain route keeps the domain it was reached on",
			"-/avatar", "https", "alice.example.com", "/",
			"https://alice.example.com/-/avatar",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := opengraph_absolute(c.image, c.scheme, c.host, c.path)
			if got != c.want {
				t.Errorf("opengraph_absolute(%q, %q, %q, %q) = %q, want %q",
					c.image, c.scheme, c.host, c.path, got, c.want)
			}
		})
	}
}

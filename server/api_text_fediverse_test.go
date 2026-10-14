// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"strings"
	"testing"
)

// TestFediverseRender: mentions and hashtags in a post's text become the links
// Mastodon marks, each reported once; those inside a link, code or a
// preformatted block, an HTML entity's hash and a bare number are left alone;
// a hashtag at the start of a line is a hashtag and not a heading.
func TestFediverseRender(t *testing.T) {
	text := "Hello @Bob@Remote.example and #Mochi, it's #mochi again #2 [a](https://x.example/#frag) `#code @x@y.example` me@mail.example\n\n#Tag2 at the start"
	rendered, mentions, hashtags := fediverse_render(text, "https://example.com/feeds/search?q=%23")
	for _, want := range []string{
		`<span class="h-card" translate="no"><a href="https://remote.example/@Bob" class="u-url mention">@<span>Bob</span></a></span>`,
		`<a href="https://example.com/feeds/search?q=%23Mochi" class="mention hashtag" rel="tag">#<span>Mochi</span></a>`,
		`<a href="https://example.com/feeds/search?q=%23Tag2" class="mention hashtag" rel="tag">#<span>Tag2</span></a>`,
		`href="https://x.example/#frag"`,
		`<code>#code @x@y.example</code>`,
		`it’s`,
		` #2 `,
		`me@mail.example`,
	} {
		if !strings.Contains(rendered, want) {
			t.Errorf("rendered lacks %q:\n%s", want, rendered)
		}
	}
	if strings.Contains(rendered, "<h1") {
		t.Errorf("a hashtag at the start of a line became a heading:\n%s", rendered)
	}
	if len(mentions) != 1 || mentions[0]["handle"] != "@Bob@remote.example" || mentions[0]["url"] != "https://remote.example/@Bob" {
		t.Errorf("mentions = %v", mentions)
	}
	if strings.Join(hashtags, ",") != "Mochi,Tag2" {
		t.Errorf("hashtags = %v, want Mochi and Tag2 once each", hashtags)
	}

	// Without a tags address a hashtag stays text and is still reported.
	rendered, _, hashtags = fediverse_render("just #one", "")
	if !strings.Contains(rendered, "just #one") || strings.Contains(rendered, "hashtag") || len(hashtags) != 1 {
		t.Errorf("without a tags address: %s %v", rendered, hashtags)
	}
}

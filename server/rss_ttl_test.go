// Mochi server: reading one integer should not cost a second parse or a third copy.
//
// api_rss_fetch read the feed into a []byte, then converted it to a string
// twice - once for gofeed and once for the TTL - and a Go string is immutable,
// so each conversion copies the whole document. Three live representations at a
// 100 MB cap. The TTL copy also drove a SECOND full parse through rss.Parser,
// building a complete extra document tree for one number, on every Atom and
// JSON feed too, where <ttl> does not exist at all.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	sl "go.starlark.net/starlark"
)

// TestRssTtlReadsTheChannel covers what the removed second parse used to
// answer, so the cheaper reader has to agree with it on real feed shapes.
func TestRssTtlReadsTheChannel(t *testing.T) {
	cases := []struct {
		name string
		body string
		want int
	}{
		{"plain", `<rss><channel><title>x</title><ttl>60</ttl><item><title>a</title></item></channel></rss>`, 60},
		{"before any item", `<rss><channel><ttl>15</ttl></channel></rss>`, 15},
		{"whitespace inside", `<rss><channel><ttl> 42 </ttl></channel></rss>`, 42},
		{"newlines around", "<rss><channel>\n  <ttl>\n7\n</ttl>\n</channel></rss>", 7},
		{"uppercase element", `<RSS><CHANNEL><TTL>30</TTL></CHANNEL></RSS>`, 30},
		{"absent", `<rss><channel><title>x</title></channel></rss>`, 0},
		{"atom has none", `<feed xmlns="http://www.w3.org/2005/Atom"><title>x</title><entry><title>a</title></entry></feed>`, 0},
		{"empty document", ``, 0},
		{"not a number", `<rss><channel><ttl>soon</ttl></channel></rss>`, 0},
		{"negative", `<rss><channel><ttl>-5</ttl></channel></rss>`, 0},
		{"larger than an int", `<rss><channel><ttl>99999999999999999999999</ttl></channel></rss>`, 0},
		{"leading zeroes", `<rss><channel><ttl>0060</ttl></channel></rss>`, 60},
	}
	for _, c := range cases {
		if got := rss_extract_ttl([]byte(c.body)); got != c.want {
			t.Errorf("%s: ttl = %d, want %d", c.name, got, c.want)
		}
	}
}

// TestRssTtlIgnoresItemContent. A feed's entries carry arbitrary publisher
// HTML, so a <ttl> quoted inside one must not be read as the channel's own -
// which is why the scan is bounded to the header rather than run over the
// whole document.
func TestRssTtlIgnoresItemContent(t *testing.T) {
	body := `<rss><channel><title>x</title>
	  <item><title>a</title><description>see &lt;ttl&gt;999&lt;/ttl&gt;</description></item>
	  <item><title>b</title><content:encoded><![CDATA[<ttl>888</ttl>]]></content:encoded></item>
	</channel></rss>`
	if got := rss_extract_ttl([]byte(body)); got != 0 {
		t.Errorf("ttl = %d, want 0: a value inside an entry was read as the feed's own", got)
	}

	// And the channel's own value still wins when both are present.
	body = `<rss><channel><ttl>20</ttl><item><description><![CDATA[<ttl>999</ttl>]]></description></item></channel></rss>`
	if got := rss_extract_ttl([]byte(body)); got != 20 {
		t.Errorf("ttl = %d, want 20", got)
	}
}

// TestRssTtlDoesNotParseTheDocument is the point of the change. A second full
// parse is invisible in the result and only shows as time and memory, so it is
// checked at the source: the extractor must not construct a parser.
func TestRssTtlDoesNotParseTheDocument(t *testing.T) {
	body, err := os.ReadFile("rss.go")
	if err != nil {
		t.Fatalf("reading rss.go: %v", err)
	}
	source := string(body)

	at := strings.Index(source, "func rss_extract_ttl")
	if at < 0 {
		t.Fatal("rss_extract_ttl not found")
	}
	region := source[at:]
	region = region[:strings.Index(region, "\n}")]
	for _, forbidden := range []string{"rss.Parser", "gofeed.", "NewParser"} {
		if strings.Contains(region, forbidden) {
			t.Errorf("rss_extract_ttl builds a %s: a whole document tree for one integer, on every feed including the Atom and JSON ones that have no ttl at all", forbidden)
		}
	}
}

// rss_source returns rss.go with its line comments stripped, so a scan for a
// construct cannot match the comment that explains why the construct is gone.
// Both assertions below first failed against correct code for exactly that
// reason: the comment saying "not string(body)" contains string(body).
func rss_source(t *testing.T) string {
	t.Helper()
	body, err := os.ReadFile("rss.go")
	if err != nil {
		t.Fatalf("reading rss.go: %v", err)
	}
	var code []string
	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "//") {
			continue
		}
		code = append(code, line)
	}
	return strings.Join(code, "\n")
}

// TestRssFetchDoesNotCopyTheBody. string(body) copies, and the fetch did it
// twice. Source-level because the cost is allocation, which a result cannot
// show, and because the fetch needs a live HTTP server to drive.
func TestRssFetchDoesNotCopyTheBody(t *testing.T) {
	source := rss_source(t)

	if n := strings.Count(source, "string(body)"); n != 0 {
		t.Errorf("rss.go converts the body to a string %d time(s); each one copies the whole feed, and three live copies at the cap is what this fixes", n)
	}
	if !strings.Contains(source, "parser.Parse(bytes.NewReader(body))") {
		t.Error("gofeed is not fed from a reader over the bytes already held")
	}
	if !strings.Contains(source, "io.LimitReader(r.Body, rss_maximum)") {
		t.Error("the feed read is not bounded by rss_maximum")
	}
}

// TestRssMaximumIsItsOwnBound. Sharing url_max_response_size meant a change to
// what any outbound fetch may return silently changed what a feed may be, and
// the reverse.
func TestRssMaximumIsItsOwnBound(t *testing.T) {
	if rss_maximum <= 0 {
		t.Fatalf("rss_maximum is %d", rss_maximum)
	}
	if strings.Contains(rss_source(t), "url_max_response_size") {
		t.Error("rss.go still reads url_max_response_size; the feed bound must be its own constant so neither moves the other")
	}
	// Documented so the next reader knows what it is measured against. Read
	// from the whole file, comments included - this one IS a comment.
	whole, err := os.ReadFile("rss.go")
	if err != nil {
		t.Fatalf("reading rss.go: %v", err)
	}
	if !strings.Contains(string(whole), "Real feeds are small") {
		t.Error("rss_maximum carries no note of what real feeds weigh, so a future change to it has nothing to judge against")
	}
	fmt.Sprintf("%d", rss_maximum) // keep the constant referenced if assertions are edited out
}

// TestRssFetchParsesARealFeed drives the whole path against a served document:
// the read, the bound, gofeed's parse from a reader, and the TTL scan. The unit
// tests above cover the extractor in isolation, but only this shows that
// feeding gofeed a reader instead of a string still produces the same feed.
func TestRssFetchParsesARealFeed(t *testing.T) {
	document := `<?xml version="1.0"?>
<rss version="2.0"><channel>
  <title>Test feed</title>
  <ttl>45</ttl>
  <item><title>First</title><link>https://example.com/1</link><guid>one</guid>
    <description>Body of the first item</description></item>
  <item><title>Second</title><link>https://example.com/2</link><guid>two</guid></item>
</channel></rss>`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/rss+xml")
		w.Write([]byte(document))
	}))
	defer server.Close()

	// The served address is loopback, which the SSRF guard refuses by design.
	previous := url_allow_private
	url_allow_private = true
	defer func() { url_allow_private = previous }()

	// An internal app, so require_permission_url's url:<domain> grant - which
	// no fixture here can hold for a random test port - is not what the test
	// ends up measuring.
	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("user", &User{UID: "u1", Username: "user@example.com"})
	thread.SetLocal("app", &App{id: "feeds", internal: &AppVersion{}})

	value, err := api_rss_fetch(thread, sl.NewBuiltin("mochi.rss.fetch", nil),
		sl.Tuple{sl.String(server.URL)}, nil)
	if err != nil {
		t.Fatalf("api_rss_fetch: %v", err)
	}
	result, ok := sl_decode(value).(map[string]any)
	if !ok {
		t.Fatalf("result is %T, want a dict", sl_decode(value))
	}

	if title, _ := result["title"].(string); title != "Test feed" {
		t.Errorf("title = %q, want \"Test feed\"", title)
	}
	if ttl, _ := result["ttl"].(int64); ttl != 45 {
		t.Errorf("ttl = %v, want 45: the channel's ttl did not survive losing the second parse", result["ttl"])
	}
	items, _ := result["items"].([]any)
	if len(items) != 2 {
		t.Fatalf("%d items, want 2: parsing from a reader did not produce the same feed as parsing from a string", len(items))
	}
	first, _ := items[0].(map[string]any)
	if title, _ := first["title"].(string); title != "First" {
		t.Errorf("first item title = %q, want \"First\"", title)
	}
}

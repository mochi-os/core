// Mochi server: RSS/Atom feed fetching API
// Copyright Alistair Cunningham 2025-2026

package main

import (
	"io"
	"net/url"
	"strconv"
	"strings"

	"github.com/mmcdole/gofeed"
	ext "github.com/mmcdole/gofeed/extensions"
	"github.com/mmcdole/gofeed/rss"
	sl "go.starlark.net/starlark"
	"golang.org/x/net/html"
)

// mochi.rss.fetch(url, headers?) -> dict: Fetch and parse an RSS or Atom feed
func api_rss_fetch(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return sl_error(fn, "syntax: <url: string>, [headers: dictionary]")
	}

	// Rate limit by app ID
	app, _ := t.Local("app").(*App)
	if app != nil && !rate_limit_url.allow(app.id) {
		return sl_error(fn, "rate limit exceeded (100 requests per minute)")
	}

	url, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid URL")
	}

	// Check url permission for external URLs
	if err := require_permission_url(t, fn, url); err != nil {
		return sl_error(fn, "%v", err)
	}

	var headers map[string]string
	if len(args) > 1 {
		headers = sl_decode_strings(args[1])
	}
	if headers == nil {
		headers = map[string]string{}
	}

	// Set default User-Agent unless overridden
	if _, found := headers["User-Agent"]; !found {
		headers["User-Agent"] = "Mochi/1.0"
	}

	empty := map[string]any{
		"status":  0,
		"headers": map[string]string{},
		"title":   "",
		"ttl":     0,
		"items":   []any{},
	}

	// Fetch the feed
	r, err := url_request("GET", url, nil, headers, nil)
	if err != nil {
		return sl_encode(empty), nil
	}
	defer r.Body.Close()

	resp_headers := header_to_map(r.Header)
	empty["status"] = r.StatusCode
	empty["headers"] = resp_headers

	// Non-2xx status: return status with empty items
	if r.StatusCode < 200 || r.StatusCode >= 300 {
		return sl_encode(empty), nil
	}

	// Read body
	body, err := io.ReadAll(io.LimitReader(r.Body, url_max_response_size))
	if err != nil {
		return sl_encode(empty), nil
	}

	// Parse with gofeed
	parser := gofeed.NewParser()
	feed, err := parser.ParseString(string(body))
	if err != nil {
		return sl_encode(empty), nil
	}

	// Extract TTL from RSS feeds (gofeed's unified Feed struct doesn't expose it)
	ttl := rss_extract_ttl(string(body))

	// Build items list
	items := make([]any, 0, len(feed.Items))
	for _, item := range feed.Items {
		// Prefer content over description
		description := item.Content
		if description == "" {
			description = item.Description
		}

		// GUID falls back to link
		guid := item.GUID
		if guid == "" {
			guid = item.Link
		}

		// Published timestamp: prefer Published, fall back to Updated
		var published int64
		if item.PublishedParsed != nil {
			published = item.PublishedParsed.Unix()
		} else if item.UpdatedParsed != nil {
			published = item.UpdatedParsed.Unix()
		}

		cats := make([]any, 0, len(item.Categories))
		for _, c := range item.Categories {
			cats = append(cats, c)
		}

		// Extract image URL, preferring explicit publisher choices and falling
		// back to the first usable image embedded in the item body. The feed
		// XML is already downloaded, so none of this costs an extra request.
		image := rss_image_from_media(item.Extensions)
		if image == "" {
			for _, enc := range item.Enclosures {
				if strings.HasPrefix(enc.Type, "image/") {
					image = enc.URL
					break
				}
			}
		}
		if image == "" && item.Image != nil {
			image = item.Image.URL
		}
		if image == "" {
			image = rss_image_from_html(description)
		}
		if image != "" {
			image = rss_resolve_url(item.Link, image)
		}

		items = append(items, map[string]any{
			"title":       item.Title,
			"description": description,
			"link":        item.Link,
			"guid":        guid,
			"published":   published,
			"categories":  cats,
			"image":       image,
		})
	}

	result := map[string]any{
		"status":  r.StatusCode,
		"headers": resp_headers,
		"title":   feed.Title,
		"link":    feed.Link,
		"ttl":     ttl,
		"items":   items,
	}

	return sl_encode(result), nil
}

// rss_extract_ttl parses TTL from RSS XML using the RSS-specific parser
func rss_extract_ttl(body string) int {
	rp := &rss.Parser{}
	feed, err := rp.Parse(strings.NewReader(body))
	if err != nil || feed == nil || feed.TTL == "" {
		return 0
	}
	ttl, err := strconv.Atoi(feed.TTL)
	if err != nil || ttl < 0 {
		return 0
	}
	return ttl
}

// rss_image_from_media extracts an image URL from Media RSS extension elements
// (media:thumbnail, media:content, media:group). gofeed parses these into the
// generic extension map rather than item.Image, so they are otherwise lost.
func rss_image_from_media(extensions ext.Extensions) string {
	media, found := extensions["media"]
	if !found {
		return ""
	}

	// media:thumbnail is the publisher's explicit preview choice.
	if url := rss_extension_url(media["thumbnail"]); url != "" {
		return url
	}

	// media:content carrying an image.
	if url := rss_content_image(media["content"]); url != "" {
		return url
	}

	// media:group wraps thumbnail/content elements as children.
	for _, group := range media["group"] {
		if url := rss_extension_url(group.Children["thumbnail"]); url != "" {
			return url
		}
		if url := rss_content_image(group.Children["content"]); url != "" {
			return url
		}
	}

	return ""
}

// rss_extension_url returns the "url" attribute of the first extension element.
func rss_extension_url(elements []ext.Extension) string {
	for _, e := range elements {
		if url := e.Attrs["url"]; url != "" {
			return url
		}
	}
	return ""
}

// rss_content_image returns the URL of the first media:content element that is
// an image. media:content can also describe video or audio, so only elements
// declaring an image medium or type are accepted.
func rss_content_image(elements []ext.Extension) string {
	for _, e := range elements {
		url := e.Attrs["url"]
		if url == "" {
			continue
		}
		if e.Attrs["medium"] == "image" || strings.HasPrefix(e.Attrs["type"], "image/") {
			return url
		}
	}
	return ""
}

// rss_image_from_html returns the source of the first usable <img> in a feed
// item's HTML body. data: URIs and images declared 1-2px (tracking pixels and
// spacers) are skipped. The returned URL may be relative; callers resolve it.
func rss_image_from_html(body string) string {
	if body == "" {
		return ""
	}

	tokenizer := html.NewTokenizer(strings.NewReader(body))
	for {
		token := tokenizer.Next()
		if token == html.ErrorToken {
			return ""
		}
		if token != html.StartTagToken && token != html.SelfClosingTagToken {
			continue
		}

		name, hasAttr := tokenizer.TagName()
		if string(name) != "img" || !hasAttr {
			continue
		}

		var source, width, height string
		for {
			key, value, more := tokenizer.TagAttr()
			switch string(key) {
			case "src":
				source = string(value)
			case "width":
				width = string(value)
			case "height":
				height = string(value)
			}
			if !more {
				break
			}
		}

		if source == "" || strings.HasPrefix(source, "data:") {
			continue
		}
		if rss_tiny(width) || rss_tiny(height) {
			continue
		}

		return source
	}
}

// rss_tiny reports whether a declared image dimension marks a tracking pixel or
// spacer (1-2px). An absent or non-numeric dimension is not considered tiny.
func rss_tiny(dimension string) bool {
	pixels, err := strconv.Atoi(strings.TrimSpace(dimension))
	if err != nil {
		return false
	}
	return pixels > 0 && pixels <= 2
}

// rss_resolve_url resolves a possibly-relative image URL against the item link.
// An unparseable base or reference falls back to the reference unchanged.
func rss_resolve_url(base, reference string) string {
	b, err := url.Parse(base)
	if err != nil {
		return reference
	}
	r, err := url.Parse(reference)
	if err != nil {
		return reference
	}
	return b.ResolveReference(r).String()
}

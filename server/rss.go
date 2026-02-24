// Mochi server: RSS/Atom feed fetching API
// Copyright Alistair Cunningham 2025

package main

import (
	"io"
	"strconv"
	"strings"

	"github.com/mmcdole/gofeed"
	"github.com/mmcdole/gofeed/rss"
	sl "go.starlark.net/starlark"
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

		// Extract image URL: prefer item.Image, fall back to image enclosure
		image := ""
		if item.Image != nil && item.Image.URL != "" {
			image = item.Image.URL
		} else {
			for _, enc := range item.Enclosures {
				if strings.HasPrefix(enc.Type, "image/") {
					image = enc.URL
					break
				}
			}
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

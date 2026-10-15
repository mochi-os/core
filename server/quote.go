// Mochi server: Posts on other networks, for quoting
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// A link to a post on another network is read from the network itself, not
// from the page's OpenGraph tags: a fediverse object as ActivityPub JSON from
// the server that holds it, a Bluesky post as its record from the account's
// data server. Either carries the author, the text and the media, which is
// what a quote shows; a page's tags carry a title and a picture. The fetch
// runs under the host policy, as a lookup of the person does, and needs no
// url grant: the app holds a link, not a host of its own choosing.

package main

import (
	"context"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"

	sl "go.starlark.net/starlark"
)

// quote_bluesky_re is a post's address on the Bluesky app: the account by
// handle or DID, and the record key.
var quote_bluesky_re = regexp.MustCompile(`^https://bsky\.app/profile/([^/?#]+)/post/([a-z2-7]+)/?$`)

// The object types a quote shows. An actor or a collection is not a post.
var quote_types = map[string]bool{"note": true, "article": true, "page": true, "question": true, "video": true, "audio": true, "image": true, "event": true}

// quote_fetch reads the post at a link, for the user whose blocks apply.
func quote_fetch(ctx context.Context, user *User, link string) (map[string]any, error) {
	if m := quote_bluesky_re.FindStringSubmatch(link); m != nil {
		return quote_bluesky(ctx, user, m[1], m[2])
	}
	u, err := url.Parse(link)
	if err != nil || u.Host == "" || u.Scheme != "https" && !(u.Scheme == "http" && url_allow_private) {
		return nil, fmt.Errorf("not a link to a post")
	}
	if err := host_allowed(user, "fediverse", u.Hostname()); err != nil {
		return nil, err
	}
	return quote_activitypub(ctx, user, link, true)
}

// quote_object is what a post's document carries that a quote shows.
type quote_object struct {
	ID           string `json:"id"`
	Type         string `json:"type"`
	Name         string `json:"name"`
	Summary      string `json:"summary"`
	Content      string `json:"content"`
	AttributedTo any    `json:"attributedTo"`
	URL          any    `json:"url"`
	Image        any    `json:"image"`
	Attachment   any    `json:"attachment"`
	Published    string `json:"published"`
}

// quote_activitypub reads an object from the server that holds it: its text,
// its first image, when it was published and who wrote it. A server that
// shows an object of another's - a Lemmy instance showing a post from another
// instance's community - names the original's id, and the quote is read from
// there, once: the object must be served by the host its id names, as an
// actor must.
func quote_activitypub(ctx context.Context, user *User, link string, follow bool) (map[string]any, error) {
	var object quote_object
	if err := resolve_json(ctx, link, "application/activity+json, application/ld+json", &object); err != nil {
		return nil, err
	}
	if !quote_types[strings.ToLower(object.Type)] {
		return nil, fmt.Errorf("%s is not a post", link)
	}
	if object.ID != "" && identifier_host(object.ID) != identifier_host(link) {
		if !follow || author_network(object.ID) != "fediverse" {
			return nil, fmt.Errorf("object %s served by %s", object.ID, identifier_host(link))
		}
		if err := host_allowed(user, "fediverse", identifier_host(object.ID)); err != nil {
			return nil, err
		}
		return quote_activitypub(ctx, user, object.ID, false)
	}
	// A content warning comes first, then a title where the object has one,
	// then the text.
	parts := []string{}
	for _, part := range []string{html_text(object.Summary), object.Name, html_text(object.Content)} {
		if part = strings.TrimSpace(part); part != "" {
			parts = append(parts, part)
		}
	}
	quote := map[string]any{"network": "fediverse", "url": link, "id": link, "text": strings.Join(parts, "\n\n"), "html": object.Content, "image": quote_image(object.Image, object.Attachment), "created": int64(0)}
	if object.ID != "" {
		quote["id"] = object.ID
	}
	if page := resolve_first_url(object.URL); page != "" {
		quote["url"] = page
	}
	if when, err := time.Parse(time.RFC3339, object.Published); err == nil {
		quote["created"] = when.Unix()
	}
	if actor := resolve_first_url(object.AttributedTo); actor != "" {
		quote["author"] = quote_author(ctx, actor)
	}
	return quote, nil
}

// quote_image is an object's picture: its image property, else its first
// attachment that is one.
func quote_image(image any, attachments any) string {
	if found := resolve_icon(image); found != "" {
		return found
	}
	list, ok := attachments.([]any)
	if !ok {
		if one, ok := attachments.(map[string]any); ok {
			list = []any{one}
		}
	}
	for _, item := range list {
		attachment, ok := item.(map[string]any)
		if !ok {
			continue
		}
		kind, _ := attachment["type"].(string)
		media, _ := attachment["mediaType"].(string)
		if strings.EqualFold(kind, "Image") || strings.HasPrefix(media, "image/") {
			if found := resolve_first_url(attachment["url"]); found != "" {
				return found
			}
			if found := resolve_first_url(attachment["href"]); found != "" {
				return found
			}
		}
	}
	return ""
}

// quote_author is what a quote shows of its writer, resolved where the network
// answers and the bare identifier where it does not.
func quote_author(ctx context.Context, identifier string) map[string]any {
	r, err := author_resolve(ctx, identifier)
	if err != nil {
		return map[string]any{"id": identifier, "name": "", "address": identifier, "avatar": "", "url": identifier}
	}
	return map[string]any{"id": r.ID, "name": r.Name, "address": r.Address, "avatar": r.Avatar, "url": r.URL}
}

// quote_bluesky reads a post record from the account's data server.
func quote_bluesky(ctx context.Context, user *User, identifier string, rkey string) (map[string]any, error) {
	identifier = strings.ToLower(identifier)
	if err := resolve_allowed(user, identifier); err != nil {
		return nil, err
	}
	did := identifier
	if !strings.HasPrefix(did, "did:") {
		var err error
		if did, err = resolve_handle(ctx, did); err != nil {
			return nil, err
		}
	}
	r, err := author_resolve(ctx, did)
	if err != nil {
		return nil, err
	}
	if r.Server == "" {
		return nil, fmt.Errorf("%s names no server", did)
	}
	var record struct {
		URI   string         `json:"uri"`
		CID   string         `json:"cid"`
		Value map[string]any `json:"value"`
	}
	address := r.Server + "/xrpc/com.atproto.repo.getRecord?repo=" + url.QueryEscape(did) + "&collection=app.bsky.feed.post&rkey=" + url.QueryEscape(rkey)
	if err := resolve_json(ctx, address, "application/json", &record); err != nil {
		return nil, err
	}
	text, _ := record.Value["text"].(string)
	quote := map[string]any{
		"network": "bluesky",
		"url":     "https://bsky.app/profile/" + strings.TrimPrefix(r.Address, "@") + "/post/" + rkey,
		"id":      record.URI,
		"cid":     record.CID,
		"text":    text,
		"html":    "",
		"image":   "",
		"created": int64(0),
		"author":  map[string]any{"id": did, "name": r.Name, "address": r.Address, "avatar": r.Avatar, "url": r.URL},
	}
	if created, _ := record.Value["createdAt"].(string); created != "" {
		if when, err := time.Parse(time.RFC3339, created); err == nil {
			quote["created"] = when.Unix()
		}
	}
	if embed := bluesky_embed_view(r.Server, did, record.Value["embed"]); embed != nil {
		if images, ok := embed["images"].([]any); ok && len(images) > 0 {
			if first, ok := images[0].(map[string]any); ok {
				quote["image"] = first["fullsize"]
			}
		} else if external, ok := embed["external"].(map[string]any); ok {
			if thumb, ok := external["thumb"].(string); ok {
				quote["image"] = thumb
			}
		}
	}
	return quote, nil
}

// mochi.url.quote(url) -> dict | None: The post at a link on another network,
// for quoting: network, url, id, text, html, image, created and the author
// {id, name, address, avatar, url}, read from the network itself - a fediverse
// object from the server that holds it, a Bluesky post from the account's data
// server - so the author comes with the text. None where the link is not a
// post Mochi can read, the network is off or the host is blocked
func api_url_quote(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var link string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "url", &link); err != nil {
		return nil, err
	}
	user := principal_caller(t)
	if user == nil {
		return sl_error(fn, "no user")
	}
	quote, err := quote_fetch(thread_context(t), user, strings.TrimSpace(link))
	if err != nil {
		return sl.None, nil
	}
	return sl_encode(quote), nil
}

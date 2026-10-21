// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: Mastodon clients' statuses
//
// What the feeds app holds - a post, a comment on one - and what the account
// reads from other servers - a Note - as Mastodon's statuses, the timelines
// made of them, posting, editing and deleting, favourites, boosts and
// bookmarks, media, polls and search. A status's number names its item for
// the user: a post or a comment of the feeds app's by its id, a Note by its
// id on its server.

package main

import (
	"encoding/json"
	"fmt"
	"html"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// === Statuses ===

// mastodon_visibility is a feeds visibility as Mastodon's.
func mastodon_visibility(v string) string {
	switch v {
	case "unlisted":
		return "unlisted"
	case "followers":
		return "private"
	case "direct":
		return "direct"
	}
	return "public"
}

// mastodon_visibility_feeds is Mastodon's visibility as the feeds app's. A
// direct status is never a post (mastodon_direct_create).
func mastodon_visibility_feeds(v string) string {
	switch v {
	case "unlisted":
		return "unlisted"
	case "private":
		return "followers"
	}
	return "public"
}

var mastodon_url_re = regexp.MustCompile(`https?://[^\s<>"]+`)

// mastodon_text renders plain text as Mastodon's HTML: paragraphs, line
// breaks and links.
func mastodon_text(text string) string {
	var paragraphs []string
	for _, paragraph := range strings.Split(strings.ReplaceAll(text, "\r", ""), "\n\n") {
		if strings.TrimSpace(paragraph) == "" {
			continue
		}
		escaped := html.EscapeString(paragraph)
		escaped = mastodon_url_re.ReplaceAllStringFunc(escaped, func(link string) string {
			return `<a href="` + link + `" rel="nofollow noopener noreferrer" target="_blank">` + link + `</a>`
		})
		paragraphs = append(paragraphs, "<p>"+strings.ReplaceAll(escaped, "\n", "<br>")+"</p>")
	}
	return strings.Join(paragraphs, "")
}

// mastodon_content is an item's text as HTML, with the mentions and
// hashtags it names: what another server sent as it was sent, made safe;
// markdown as the fediverse shows it; plain text as paragraphs.
func mastodon_content(s *mastodon_session, body string, format string, sent string) (string, []any, []any) {
	mentions, tags := []any{}, []any{}
	if sent != "" {
		return markdown_policy.Sanitize(sent), mentions, tags
	}
	if format != "markdown" {
		return mastodon_text(body), mentions, tags
	}
	rendered, found, hashtags := fediverse_render(body, "https://"+s.domain+"/feeds/search?q=%23")
	for _, m := range found {
		handle, _ := m["handle"].(string)
		address, _ := m["url"].(string)
		username, host, _ := strings.Cut(strings.TrimPrefix(handle, "@"), "@")
		mentions = append(mentions, map[string]any{"id": mastodon_number(s.user, "account", address, 0), "username": username, "acct": username + "@" + host, "url": address})
	}
	for _, tag := range hashtags {
		tags = append(tags, map[string]any{"name": strings.ToLower(tag), "url": "https://" + s.domain + "/feeds/search?q=%23" + tag})
	}
	return rendered, mentions, tags
}

// mastodon_media_type is a file's kind as Mastodon names it.
func mastodon_media_type(kind string) string {
	switch {
	case strings.HasPrefix(kind, "image/gif"):
		return "gifv"
	case strings.HasPrefix(kind, "image/"), kind == "image":
		return "image"
	case strings.HasPrefix(kind, "video/"), kind == "video":
		return "video"
	case strings.HasPrefix(kind, "audio/"), kind == "audio":
		return "audio"
	}
	return "unknown"
}

// mastodon_attachments are an item's files as Mastodon's media: its own,
// served here, and those another server holds.
func mastodon_attachments(s *mastodon_session, item map[string]any) []any {
	out := []any{}
	for _, att := range mastodon_list(item["attachments"]) {
		address := mastodon_string(att, "url")
		if address == "" {
			continue
		}
		preview := mastodon_string(att, "preview")
		if preview == "" {
			preview = address
		}
		description := mastodon_string(att, "description")
		if description == "" {
			description = mastodon_string(att, "caption")
		}
		entry := map[string]any{
			"id":                 mastodon_number(s.user, "media", mastodon_string(att, "id"), mastodon_integer(att, "created")),
			"type":               mastodon_media_type(mastodon_string(att, "type")),
			"url":                address,
			"preview_url":        preview,
			"remote_url":         nil,
			"preview_remote_url": nil,
			"text_url":           nil,
			"meta":               map[string]any{},
			"description":        nil,
			"blurhash":           nil,
		}
		if description != "" {
			entry["description"] = description
		}
		out = append(out, entry)
	}
	data, _ := item["data"].(map[string]any)
	for i, m := range mastodon_list(data["media"]) {
		address := mastodon_string(m, "url")
		if address == "" {
			continue
		}
		entry := map[string]any{
			"id":                 mastodon_number(s.user, "media", mastodon_string(item, "id")+"#"+strconv.Itoa(i), mastodon_integer(item, "created")),
			"type":               mastodon_media_type(mastodon_string(m, "type")),
			"url":                address,
			"preview_url":        address,
			"remote_url":         address,
			"preview_remote_url": nil,
			"text_url":           nil,
			"meta":               map[string]any{},
			"description":        nil,
			"blurhash":           nil,
		}
		if name := mastodon_string(m, "name"); name != "" {
			entry["description"] = name
		}
		if hash := mastodon_string(m, "blurhash"); hash != "" {
			entry["blurhash"] = hash
		}
		if width, height := mastodon_integer(m, "width"), mastodon_integer(m, "height"); width > 0 && height > 0 {
			entry["meta"] = map[string]any{"original": map[string]any{"width": width, "height": height, "size": fmt.Sprintf("%dx%d", width, height), "aspect": float64(width) / float64(height)}}
		}
		out = append(out, entry)
	}
	return out
}

// mastodon_poll is a post's poll as Mastodon's.
func mastodon_poll(s *mastodon_session, key string, poll map[string]any, created int64) any {
	if poll == nil {
		return nil
	}
	options := []any{}
	votes := int64(0)
	for _, option := range mastodon_list(poll["options"]) {
		count := mastodon_integer(option, "votes")
		votes += count
		options = append(options, map[string]any{"title": mastodon_string(option, "name"), "votes_count": count})
	}
	expires := mastodon_integer(poll, "expires")
	out := map[string]any{
		"id":           mastodon_number(s.user, "poll", key, created),
		"expires_at":   nil,
		"expired":      expires > 0 && expires < now(),
		"multiple":     poll["multiple"] == true,
		"votes_count":  votes,
		"voters_count": mastodon_integer(poll, "voters"),
		"options":      options,
		"emojis":       []any{},
		"voted":        poll["voted"] == true,
		"own_votes":    []any{},
	}
	if expires > 0 {
		out["expires_at"] = mastodon_time(expires)
	}
	if own, ok := poll["own"].([]any); ok {
		out["own_votes"] = own
	}
	return out
}

// mastodon_emojis are an item's custom emoji as Mastodon's.
func mastodon_emojis(data map[string]any) []any {
	out := []any{}
	for _, e := range mastodon_list(data["emoji"]) {
		code := strings.Trim(mastodon_string(e, "name"), ":")
		if code == "" {
			code = strings.Trim(mastodon_string(e, "shortcode"), ":")
		}
		address := mastodon_string(e, "url")
		if code != "" && address != "" {
			out = append(out, map[string]any{"shortcode": code, "url": address, "static_url": address, "visible_in_picker": false})
		}
	}
	return out
}

// mastodon_person_of is who wrote an item: someone elsewhere, as the item
// carries them; the account itself for the user's own; else the feed or the
// source the item came from.
func mastodon_person_of(s *mastodon_session, item map[string]any) mastodon_figure {
	data, _ := item["data"].(map[string]any)
	if person, ok := data["person"].(map[string]any); ok {
		key := mastodon_string(item, "author")
		if key == "" {
			key = mastodon_string(person, "id")
		}
		username, host, _ := strings.Cut(strings.TrimPrefix(mastodon_string(person, "address"), "@"), "@")
		return mastodon_figure{key: key, username: username, host: host, name: mastodon_string(person, "name"), avatar: mastodon_string(person, "avatar"), url: mastodon_string(person, "url")}
	}
	if item["owned"] == true || item["mine"] == true {
		return mastodon_figure{key: s.actor}
	}
	if subscriber := mastodon_string(item, "subscriber"); subscriber != "" {
		if strings.HasPrefix(subscriber, "https://") {
			username := mastodon_string(item, "name")
			host := identifier_host(subscriber)
			if address := mastodon_string(item, "address"); strings.Contains(address, "@") {
				username, host, _ = strings.Cut(strings.TrimPrefix(address, "@"), "@")
			}
			return mastodon_figure{key: subscriber, username: username, host: host, name: mastodon_string(item, "name"), url: subscriber}
		}
		return mastodon_figure{key: "identity:" + subscriber, username: strings.ReplaceAll(strings.ToLower(mastodon_string(item, "name")), " ", "_"), host: s.domain, name: mastodon_string(item, "name")}
	}
	if source, ok := item["source"].(map[string]any); ok && mastodon_string(source, "url") != "" {
		name := mastodon_string(source, "name")
		return mastodon_figure{key: "source:" + mastodon_string(source, "url"), username: mastodon_slug(name), host: identifier_host(mastodon_string(source, "url")), name: name, url: mastodon_string(source, "url"), bot: true}
	}
	fingerprint := mastodon_string(item, "fingerprint")
	return mastodon_figure{key: "feed:" + mastodon_string(item, "feed"), username: fingerprint, host: s.domain, name: mastodon_string(item, "feed_name"), url: "https://" + s.domain + "/feeds/" + fingerprint, group: true}
}

var mastodon_slug_re = regexp.MustCompile(`[^a-z0-9_]+`)

// mastodon_slug is a name as a username.
func mastodon_slug(name string) string {
	slug := strings.Trim(mastodon_slug_re.ReplaceAllString(strings.ToLower(name), "_"), "_")
	if slug == "" {
		return "source"
	}
	return slug
}

// mastodon_uri is an item's permanent id: its id elsewhere where it came
// from there, else its copy on the account's network, else its page.
func mastodon_uri(s *mastodon_session, item map[string]any) string {
	if remote := mastodon_string(item, "remote"); strings.HasPrefix(remote, "https://") {
		return remote
	}
	for _, copy := range mastodon_list(item["copies"]) {
		if mastodon_string(copy, "network") == "fediverse" && strings.HasPrefix(mastodon_string(copy, "remote"), "https://") {
			return mastodon_string(copy, "remote")
		}
	}
	return mastodon_string(item, "link")
}

// mastodon_post is a post of the feeds app's as a status.
func mastodon_post(s *mastodon_session, post map[string]any) map[string]any {
	key := "post:" + mastodon_string(post, "id")
	created := mastodon_integer(post, "created")
	data, _ := post["data"].(map[string]any)
	sent := ""
	if rss, ok := data["rss"].(map[string]any); ok {
		sent = mastodon_string(rss, "html")
	}
	content, mentions, tags := mastodon_content(s, mastodon_string(post, "body"), mastodon_string(post, "format"), sent)
	warning := mastodon_string(post, "warning")
	status := map[string]any{
		"id":                     mastodon_number(s.user, "status", key, created),
		"created_at":             mastodon_time(created),
		"in_reply_to_id":         nil,
		"in_reply_to_account_id": nil,
		"sensitive":              post["sensitive"] == true || warning != "",
		"spoiler_text":           warning,
		"visibility":             mastodon_visibility(mastodon_string(post, "visibility")),
		"language":               nil,
		"uri":                    mastodon_uri(s, post),
		"url":                    mastodon_string(post, "link"),
		"replies_count":          mastodon_integer(post, "comments"),
		"reblogs_count":          mastodon_integer(post, "boosts"),
		"favourites_count":       mastodon_integer(post, "likes"),
		"quotes_count":           0,
		"edited_at":              nil,
		"favourited":             mastodon_string(post, "reaction") != "",
		"reblogged":              false,
		"muted":                  false,
		"bookmarked":             post["saved"] == true,
		"pinned":                 mastodon_integer(post, "pinned") > 0,
		"content":                content,
		"filtered":               []any{},
		"reblog":                 nil,
		"application":            nil,
		"account":                mastodon_account(s, mastodon_person_of(s, post)),
		"media_attachments":      mastodon_attachments(s, post),
		"mentions":               mentions,
		"tags":                   tags,
		"emojis":                 mastodon_emojis(data),
		"card":                   nil,
		"poll":                   nil,
	}
	if language := mastodon_string(post, "language"); language != "" {
		status["language"] = language
	}
	if edited := mastodon_integer(post, "edited"); edited > 0 {
		status["edited_at"] = mastodon_time(edited)
	}
	if poll, ok := data["poll"].(map[string]any); ok {
		status["poll"] = mastodon_poll(s, key, poll, created)
	}
	for _, label := range mastodon_list(post["tags"]) {
		name := strings.ToLower(strings.TrimPrefix(mastodon_string(label, "label"), "#"))
		if name != "" {
			tags = append(tags, map[string]any{"name": name, "url": "https://" + s.domain + "/feeds/search?q=%23" + name})
		}
	}
	status["tags"] = tags
	status["filtered"] = mastodon_filtered(s, strip_tags(content)+" "+warning)
	// A boost by someone a feed follows is their status, holding the one they
	// boosted.
	if boost, ok := data["boost"].(map[string]any); ok && mastodon_string(boost, "name") != "" {
		key := mastodon_string(boost, "id")
		if key == "" {
			key = "boost:" + mastodon_string(post, "id")
		}
		username, host, _ := strings.Cut(strings.TrimPrefix(mastodon_string(boost, "address"), "@"), "@")
		booster := mastodon_figure{key: key, username: username, host: host, name: mastodon_string(boost, "name"), avatar: mastodon_string(boost, "avatar"), url: mastodon_string(boost, "url")}
		outer := map[string]any{}
		for k, v := range status {
			outer[k] = v
		}
		outer["id"] = mastodon_number(s.user, "status", "boost:"+mastodon_string(post, "id"), created)
		outer["account"] = mastodon_account(s, booster)
		outer["reblog"] = status
		outer["content"] = ""
		outer["media_attachments"] = []any{}
		outer["mentions"], outer["tags"], outer["emojis"] = []any{}, []any{}, []any{}
		outer["poll"] = nil
		mastodon_snapshot(s.user, "status", "boost:"+mastodon_string(post, "id"), outer)
		mastodon_snapshot(s.user, "status", key, status)
		return outer
	}
	mastodon_snapshot(s.user, "status", key, status)
	return status
}

// mastodon_comment is a comment on a post as a status: a reply to the post,
// or to the comment it answers.
func mastodon_comment(s *mastodon_session, comment map[string]any, post map[string]any) map[string]any {
	key := "comment:" + mastodon_string(comment, "id")
	created := mastodon_integer(comment, "created")
	content, mentions, tags := mastodon_content(s, mastodon_string(comment, "body"), mastodon_string(comment, "format"), "")
	person := mastodon_person_of(s, comment)
	account := mastodon_account(s, person)
	status := map[string]any{
		"id":                     mastodon_number(s.user, "status", key, created),
		"created_at":             mastodon_time(created),
		"in_reply_to_id":         nil,
		"in_reply_to_account_id": nil,
		"sensitive":              false,
		"spoiler_text":           "",
		"visibility":             mastodon_visibility(mastodon_string(post, "visibility")),
		"language":               nil,
		"uri":                    mastodon_string(comment, "remote"),
		"url":                    mastodon_string(post, "link"),
		"replies_count":          mastodon_integer(comment, "replies"),
		"reblogs_count":          0,
		"favourites_count":       mastodon_integer(comment, "likes"),
		"quotes_count":           0,
		"edited_at":              nil,
		"favourited":             mastodon_string(comment, "reaction") != "",
		"reblogged":              false,
		"muted":                  false,
		"bookmarked":             false,
		"pinned":                 false,
		"content":                content,
		"filtered":               mastodon_filtered(s, strip_tags(content)),
		"reblog":                 nil,
		"application":            nil,
		"account":                account,
		"media_attachments":      mastodon_attachments(s, comment),
		"mentions":               mentions,
		"tags":                   tags,
		"emojis":                 []any{},
		"card":                   nil,
		"poll":                   nil,
	}
	if status["uri"] == "" {
		status["uri"] = mastodon_string(post, "link") + "#" + mastodon_string(comment, "id")
	}
	if edited := mastodon_integer(comment, "edited"); edited > 0 {
		status["edited_at"] = mastodon_time(edited)
	}
	if parent := mastodon_string(comment, "parent"); parent != "" {
		status["in_reply_to_id"] = mastodon_number(s.user, "status", "comment:"+parent, 0)
		if held := mastodon_snapshot_get(s.user, "status", "comment:"+parent); held != nil {
			if by, ok := held["account"].(map[string]any); ok {
				status["in_reply_to_account_id"] = by["id"]
			}
		}
	} else if post != nil {
		status["in_reply_to_id"] = mastodon_number(s.user, "status", "post:"+mastodon_string(post, "id"), mastodon_integer(post, "created"))
		status["in_reply_to_account_id"] = mastodon_account(s, mastodon_person_of(s, post))["id"]
	}
	mastodon_snapshot(s.user, "status", key, status)
	return status
}

// mastodon_note is a Note another server holds as a status.
func mastodon_note(c *gin.Context, s *mastodon_session, note map[string]any) map[string]any {
	id := mastodon_string(note, "id")
	if id == "" {
		return nil
	}
	key := "note:" + id
	created := mastodon_parse(mastodon_string(note, "published"))
	by := resolve_first_url(note["attributedTo"])
	person := mastodon_figure{key: by, url: by, host: identifier_host(by)}
	if held := mastodon_snapshot_get(s.user, "account", by); held != nil {
		person.username, _ = held["username"].(string)
		person.name, _ = held["display_name"].(string)
		person.avatar, _ = held["avatar"].(string)
		person.url, _ = held["url"].(string)
	} else if r := mastodon_resolve(c, s, by); r != nil {
		person = mastodon_resolved(r)
	}
	content := markdown_policy.Sanitize(mastodon_string(note, "content"))
	warning := strip_tags(mastodon_string(note, "summary"))
	media := []any{}
	for _, att := range object_list_any(note["attachment"]) {
		m, ok := att.(map[string]any)
		if !ok {
			continue
		}
		address := resolve_first_url(m["url"])
		if address == "" {
			continue
		}
		kind := mastodon_string(m, "mediaType")
		if kind == "" {
			kind = strings.ToLower(mastodon_string(m, "type"))
		}
		entry := map[string]any{"type": kind, "url": address, "name": mastodon_string(m, "name"), "blurhash": mastodon_string(m, "blurhash"), "width": m["width"], "height": m["height"]}
		media = append(media, entry)
	}
	item := map[string]any{"id": id, "created": created, "data": map[string]any{"media": media}}
	visibility := "unlisted"
	for _, to := range object_strings(note["to"]) {
		if to == "https://www.w3.org/ns/activitystreams#Public" {
			visibility = "public"
		}
	}
	mentions, tags := []any{}, []any{}
	for _, raw := range object_list_any(note["tag"]) {
		tag, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		switch mastodon_string(tag, "type") {
		case "Mention":
			address := mastodon_string(tag, "href")
			username, host, _ := strings.Cut(strings.TrimPrefix(mastodon_string(tag, "name"), "@"), "@")
			mentions = append(mentions, map[string]any{"id": mastodon_number(s.user, "account", address, 0), "username": username, "acct": username + "@" + host, "url": address})
		case "Hashtag":
			name := strings.ToLower(strings.TrimPrefix(mastodon_string(tag, "name"), "#"))
			tags = append(tags, map[string]any{"name": name, "url": mastodon_string(tag, "href")})
		}
	}
	status := map[string]any{
		"id":                     mastodon_number(s.user, "status", key, created),
		"created_at":             mastodon_time(created),
		"in_reply_to_id":         nil,
		"in_reply_to_account_id": nil,
		"sensitive":              note["sensitive"] == true || warning != "",
		"spoiler_text":           warning,
		"visibility":             visibility,
		"language":               nil,
		"uri":                    id,
		"url":                    resolve_first_url(note["url"]),
		"replies_count":          0,
		"reblogs_count":          0,
		"favourites_count":       0,
		"quotes_count":           0,
		"edited_at":              nil,
		"favourited":             false,
		"reblogged":              false,
		"muted":                  false,
		"bookmarked":             false,
		"pinned":                 false,
		"content":                content,
		"filtered":               mastodon_filtered(s, strip_tags(content)+" "+warning),
		"reblog":                 nil,
		"application":            nil,
		"account":                mastodon_account(s, person),
		"media_attachments":      mastodon_attachments(s, item),
		"mentions":               mentions,
		"tags":                   tags,
		"emojis":                 []any{},
		"card":                   nil,
		"poll":                   nil,
	}
	if status["url"] == "" {
		status["url"] = id
	}
	if reply := resolve_first_url(note["inReplyTo"]); reply != "" {
		status["in_reply_to_id"] = mastodon_number(s.user, "status", "note:"+reply, 0)
	}
	if updated := mastodon_parse(mastodon_string(note, "updated")); updated > created {
		status["edited_at"] = mastodon_time(updated)
	}
	if mastodon_string(note, "type") == "Question" {
		multiple := note["anyOf"] != nil
		var options []any
		voters := mastodon_integer(note, "votersCount")
		choices := note["oneOf"]
		if multiple {
			choices = note["anyOf"]
		}
		for _, raw := range object_list_any(choices) {
			option, _ := raw.(map[string]any)
			replies, _ := option["replies"].(map[string]any)
			options = append(options, map[string]any{"name": mastodon_string(option, "name"), "votes": mastodon_integer(replies, "totalItems")})
		}
		status["poll"] = mastodon_poll(s, key, map[string]any{"options": options, "multiple": multiple, "voters": voters, "expires": mastodon_parse(mastodon_string(note, "endTime"))}, created)
	}
	mastodon_snapshot(s.user, "status", key, status)
	return status
}

// object_list_any is a value that may be one item or a list of them, as a
// list.
func object_list_any(v any) []any {
	switch items := v.(type) {
	case []any:
		return items
	case nil:
		return nil
	default:
		return []any{items}
	}
}

// === Finding statuses ===

// mastodon_item is the item a status number names: its key, and the feeds
// app's post or comment with its post, where it holds one.
type mastodon_item struct {
	key     string
	post    map[string]any
	comment map[string]any
	message map[string]any
}

// mastodon_find finds what a status number names, as it stands now.
func mastodon_find(c *gin.Context, s *mastodon_session, number string) *mastodon_item {
	key := mastodon_key(s.user, "status", number)
	if key == "" {
		return nil
	}
	item := &mastodon_item{key: key}
	switch {
	case strings.HasPrefix(key, "post:") || strings.HasPrefix(key, "boost:"):
		id := strings.TrimPrefix(strings.TrimPrefix(key, "post:"), "boost:")
		result, err := mastodon_call(s, "mastodon/posts", Map{"kind": "ids", "ids": []any{id}})
		if posts := mastodon_list(result); err == nil && len(posts) == 1 {
			item.post = posts[0]
			item.key = "post:" + id
			return item
		}
		return nil
	case strings.HasPrefix(key, "comment:"):
		result, err := mastodon_call(s, "mastodon/comment", Map{"id": strings.TrimPrefix(key, "comment:")})
		comment, _ := result.(map[string]any)
		if err != nil || comment == nil {
			return nil
		}
		item.comment = comment
		item.post, _ = comment["post"].(map[string]any)
		return item
	case strings.HasPrefix(key, "message:"):
		result, err := mastodon_direct(s, "message", Map{"message": strings.TrimPrefix(key, "message:")})
		message, _ := result.(map[string]any)
		if err != nil || message == nil {
			return nil
		}
		item.message = message
		return item
	case strings.HasPrefix(key, "note:"):
		// A Note the feeds app has taken in is its post.
		result, err := mastodon_call(s, "mastodon/posts", Map{"kind": "remote", "ids": []any{strings.TrimPrefix(key, "note:")}})
		if posts := mastodon_list(result); err == nil && len(posts) == 1 {
			item.post = posts[0]
		}
		return item
	}
	return nil
}

// mastodon_render is an item as the status it is now.
func mastodon_render(c *gin.Context, s *mastodon_session, item *mastodon_item) map[string]any {
	switch {
	case item.message != nil:
		return mastodon_direct_status(c, s, item.message)
	case item.comment != nil:
		return mastodon_comment(s, item.comment, item.post)
	case item.post != nil:
		return mastodon_post(s, item.post)
	case strings.HasPrefix(item.key, "note:"):
		if note := mastodon_fetch(c, s, strings.TrimPrefix(item.key, "note:")); note != nil {
			return mastodon_note(c, s, note)
		}
		return mastodon_snapshot_get(s.user, "status", item.key)
	}
	return nil
}

// mastodon_fetch reads a Note from its server, as the account, or nil.
func mastodon_fetch(c *gin.Context, s *mastodon_session, id string) map[string]any {
	if !activitypub_remote_ok(id) || host_allowed(s.user, "fediverse", identifier_host(id)) != nil {
		return nil
	}
	ctx := activitypub_signing(c.Request.Context(), s.user, s.account)
	var note map[string]any
	if err := resolve_json(ctx, id, "application/activity+json, application/ld+json", &note); err != nil {
		return nil
	}
	if mastodon_string(note, "id") != id && identifier_host(mastodon_string(note, "id")) != identifier_host(id) {
		return nil
	}
	if kind := mastodon_string(note, "type"); kind != "Note" && kind != "Article" && kind != "Question" && kind != "Page" {
		return nil
	}
	return note
}

// mastodon_statuses are posts as statuses, those the user muted or blocked
// the author of, or that a filter hides, left out.
func mastodon_statuses(s *mastodon_session, posts []map[string]any) []any {
	out := []any{}
	for _, post := range posts {
		status := mastodon_post(s, post)
		author := status["account"].(map[string]any)
		if key, _ := author["uri"].(string); key != s.actor && (actor_blocked(s.user, key) || actor_muted(s.user, key)) {
			continue
		}
		if mastodon_hidden(status) {
			continue
		}
		out = append(out, status)
	}
	return out
}

// === Routing ===

// mastodon_statuses_dispatch answers statuses, timelines, media, polls and
// search, reporting whether the request was one.
func mastodon_statuses_dispatch(c *gin.Context, s *mastodon_session, version string, parts []string) bool {
	method := c.Request.Method
	route := method + " " + version + "/" + strings.Join(parts, "/")
	switch route {
	case "POST v1/statuses":
		mastodon_status_create(c, s)
		return true
	case "GET v1/statuses":
		in := mastodon_read(c)
		out := []any{}
		for _, number := range in.list("id") {
			if item := mastodon_find(c, s, number); item != nil {
				if status := mastodon_render(c, s, item); status != nil {
					out = append(out, status)
				}
			}
		}
		c.JSON(http.StatusOK, out)
		return true
	case "GET v1/timelines/home", "GET v1/timelines/public":
		mastodon_timeline_page(c, s, "home", Map{})
		return true
	case "GET v1/favourites":
		mastodon_timeline_page(c, s, "favourites", Map{})
		return true
	case "GET v1/bookmarks":
		mastodon_timeline_page(c, s, "bookmarks", Map{})
		return true
	case "POST v1/media", "POST v2/media":
		mastodon_media_upload(c, s)
		return true
	case "GET v2/search", "GET v1/search":
		mastodon_search(c, s)
		return true
	case "GET v1/scheduled_statuses":
		c.JSON(http.StatusOK, []any{})
		return true
	}
	if taken, ok := mastodon_match(parts, "timelines", "tag", "*"); ok && method == "GET" {
		mastodon_timeline_page(c, s, "tag", Map{"tag": strings.ToLower(taken[0])})
		return true
	}
	if taken, ok := mastodon_match(parts, "timelines", "list", "*"); ok && method == "GET" {
		mastodon_timeline_page(c, s, "list", Map{"feed": mastodon_key(s.user, "list", taken[0])})
		return true
	}
	if taken, ok := mastodon_match(parts, "media", "*"); ok && (method == "GET" || method == "PUT") {
		mastodon_media(c, s, taken[0])
		return true
	}
	if taken, ok := mastodon_match(parts, "polls", "*"); ok && method == "GET" {
		item := mastodon_find(c, s, mastodon_poll_status(s, taken[0]))
		status := map[string]any(nil)
		if item != nil {
			status = mastodon_render(c, s, item)
		}
		if status == nil || status["poll"] == nil {
			mastodon_error(c, http.StatusNotFound, "Record not found")
			return true
		}
		c.JSON(http.StatusOK, status["poll"])
		return true
	}
	if taken, ok := mastodon_match(parts, "polls", "*", "votes"); ok && method == "POST" {
		mastodon_vote(c, s, taken[0])
		return true
	}
	if len(parts) >= 2 && parts[0] == "statuses" && version == "v1" {
		item := mastodon_find(c, s, parts[1])
		if item == nil {
			mastodon_error(c, http.StatusNotFound, "Record not found")
			return true
		}
		action := ""
		if len(parts) == 3 {
			action = parts[2]
		}
		mastodon_status_action(c, s, parts[1], item, method, action)
		return true
	}
	return false
}

// mastodon_poll_status is the status a poll number belongs to.
func mastodon_poll_status(s *mastodon_session, number string) string {
	key := mastodon_key(s.user, "poll", number)
	if key == "" {
		return ""
	}
	return mastodon_number(s.user, "status", key, 0)
}

// mastodon_status_action answers a status and what is done to it.
func mastodon_status_action(c *gin.Context, s *mastodon_session, number string, item *mastodon_item, method string, action string) {
	if item.message != nil {
		mastodon_direct_action(c, s, number, item, method, action)
		return
	}
	id := ""
	switch {
	case item.comment != nil:
		id = "c:" + mastodon_string(item.comment, "id")
	case item.post != nil:
		id = mastodon_string(item.post, "id")
	}
	remote := strings.TrimPrefix(item.key, "note:")
	if !strings.HasPrefix(item.key, "note:") {
		remote = ""
	}
	answer := func(result any, err error) {
		if err != nil {
			mastodon_error(c, http.StatusUnprocessableEntity, err.Error())
			return
		}
		if m, ok := result.(map[string]any); ok {
			if failure := mastodon_string(m, "error"); failure != "" {
				mastodon_error(c, http.StatusUnprocessableEntity, failure)
				return
			}
			if post, ok := m["post"].(map[string]any); ok {
				item.post = post
			}
			if comment, ok := m["comment"].(map[string]any); ok {
				item.comment = comment
			}
		}
		status := mastodon_render(c, s, item)
		if status == nil {
			mastodon_error(c, http.StatusNotFound, "Record not found")
			return
		}
		c.JSON(http.StatusOK, status)
	}
	switch method + " " + action {
	case "GET ":
		answer(nil, nil)
	case "DELETE ":
		if id == "" {
			mastodon_error(c, http.StatusNotFound, "Record not found")
			return
		}
		status := mastodon_render(c, s, item)
		result, err := mastodon_call(s, "mastodon/delete", Map{"id": id})
		if m, ok := result.(map[string]any); err != nil || (ok && mastodon_string(m, "error") != "") {
			answer(result, err)
			return
		}
		status["text"] = mastodon_string(item.post, "body")
		if item.comment != nil {
			status["text"] = mastodon_string(item.comment, "body")
		}
		c.JSON(http.StatusOK, status)
	case "PUT ":
		if id == "" {
			mastodon_error(c, http.StatusNotFound, "Record not found")
			return
		}
		in := mastodon_read(c)
		media, ok := mastodon_media_inputs(c, s, in)
		if !ok {
			return
		}
		args := Map{"id": id, "text": in.text("status"), "warning": in.text("spoiler_text"), "sensitive": in.flag("sensitive"), "language": in.text("language")}
		if in.has("media_ids") {
			args["media"] = media
		}
		answer(mastodon_call(s, "mastodon/edit", args))
	case "GET context":
		mastodon_thread(c, s, item)
	case "GET source":
		text := mastodon_string(item.post, "body")
		warning := mastodon_string(item.post, "warning")
		if item.comment != nil {
			text, warning = mastodon_string(item.comment, "body"), ""
		}
		c.JSON(http.StatusOK, map[string]any{"id": number, "text": text, "spoiler_text": warning})
	case "GET history":
		status := mastodon_render(c, s, item)
		if status == nil {
			mastodon_error(c, http.StatusNotFound, "Record not found")
			return
		}
		c.JSON(http.StatusOK, []any{map[string]any{"content": status["content"], "spoiler_text": status["spoiler_text"], "sensitive": status["sensitive"], "created_at": status["created_at"], "account": status["account"], "media_attachments": status["media_attachments"], "emojis": status["emojis"]}})
	case "GET favourited_by", "GET reblogged_by", "GET quotes":
		c.JSON(http.StatusOK, []any{})
	case "POST favourite", "POST unfavourite":
		on := action == "favourite"
		if id != "" {
			answer(mastodon_call(s, "mastodon/react", Map{"id": id, "on": on}))
		} else {
			status := mastodon_render(c, s, item)
			if status == nil {
				mastodon_error(c, http.StatusNotFound, "Record not found")
				return
			}
			author, _ := status["account"].(map[string]any)
			if err := mastodon_like(c, s, remote, mastodon_string(author, "uri"), on); err != nil {
				mastodon_error(c, http.StatusUnprocessableEntity, err.Error())
				return
			}
			status["favourited"] = on
			mastodon_snapshot(s.user, "status", item.key, status)
			c.JSON(http.StatusOK, status)
		}
	case "POST reblog", "POST unreblog":
		on := action == "reblog"
		uri := remote
		if uri == "" {
			if status := mastodon_render(c, s, item); status != nil {
				uri, _ = status["uri"].(string)
			}
		}
		result, err := mastodon_call(s, "mastodon/boost", Map{"id": id, "remote": uri, "on": on})
		if m, ok := result.(map[string]any); err != nil || (ok && mastodon_string(m, "error") != "") {
			answer(result, err)
			return
		}
		status := mastodon_render(c, s, item)
		if status == nil {
			mastodon_error(c, http.StatusNotFound, "Record not found")
			return
		}
		status["reblogged"] = on
		if on {
			outer := map[string]any{}
			for k, v := range status {
				outer[k] = v
			}
			outer["id"] = mastodon_number(s.user, "status", "reblog:"+item.key, now())
			outer["account"] = mastodon_own(s, false)
			outer["reblog"] = status
			outer["content"] = ""
			c.JSON(http.StatusOK, outer)
			return
		}
		c.JSON(http.StatusOK, status)
	case "POST bookmark", "POST unbookmark":
		if id == "" || item.comment != nil {
			mastodon_error(c, http.StatusUnprocessableEntity, "Only posts a feed holds can be bookmarked")
			return
		}
		answer(mastodon_call(s, "mastodon/save", Map{"id": id, "on": action == "bookmark"}))
	case "POST pin", "POST unpin", "POST mute", "POST unmute", "POST translate":
		answer(nil, nil)
	default:
		mastodon_error(c, http.StatusNotFound, "Record not found")
	}
}

// mastodon_like likes a Note no feed holds, as the account, or takes the
// like back: a Like sent to its author and listed as the account's, or the
// Undo of the one sent before (decision 42).
func mastodon_like(c *gin.Context, s *mastodon_session, remote string, author string, on bool) error {
	db := db_user(s.user, "user")
	var held map[string]any
	previous := ""
	rows, _ := db.rows("select id, addressing from objects where account=? and type='Like' and deleted=0", s.account)
	for _, row := range rows {
		if activity := object_activity(row); activity != nil && activity["object"] == remote {
			held, previous = activity, row_string(row, "id")
		}
	}
	if (on && held != nil) || (!on && held == nil) {
		return nil
	}
	activity := map[string]any{"type": "Like", "object": remote}
	if !on {
		activity = map[string]any{"type": "Undo", "object": held}
	}
	if author != "" && activitypub_remote_ok(author) {
		activity["to"] = []any{author}
		activity["cc"] = []any{}
	}
	id := uid()
	activity["id"] = s.actor + "/" + id
	activity["actor"] = s.actor
	activity["published"] = time.Now().UTC().Format(time.RFC3339)
	to, cc := object_addressing(activity, s.actor)
	db.exec("insert into objects (id, account, app, type, addressing, created) values (?, ?, '', ?, ?, ?)", id, s.account, activity["type"], json_encode(map[string]any{"to": to, "cc": cc, "activity": activity}), now())
	if !on {
		db.exec("update objects set deleted=? where id=?", now(), previous)
	}
	body := map[string]any{"@context": activitystreams}
	for key, value := range activity {
		body[key] = value
	}
	object_deliver(activitypub_signing(c.Request.Context(), s.user, s.account), s.user, s.account, s.actor, body, to, cc)
	return nil
}

// mastodon_thread answers what a status replies to and what replies to it:
// a post's comments, as the feeds app holds them; a Note's, as its server
// says.
func mastodon_thread(c *gin.Context, s *mastodon_session, item *mastodon_item) {
	ancestors, descendants := []any{}, []any{}
	if item.post == nil {
		if strings.HasPrefix(item.key, "note:") {
			if note := mastodon_fetch(c, s, strings.TrimPrefix(item.key, "note:")); note != nil {
				reply := resolve_first_url(note["inReplyTo"])
				for i := 0; reply != "" && i < 10; i++ {
					parent := mastodon_fetch(c, s, reply)
					if parent == nil {
						break
					}
					ancestors = append([]any{mastodon_note(c, s, parent)}, ancestors...)
					reply = resolve_first_url(parent["inReplyTo"])
				}
			}
		}
		c.JSON(http.StatusOK, map[string]any{"ancestors": ancestors, "descendants": descendants})
		return
	}
	result, _ := mastodon_call(s, "mastodon/comments", Map{"post": mastodon_string(item.post, "id")})
	comments := mastodon_list(result)
	byid := map[string]map[string]any{}
	for _, comment := range comments {
		byid[mastodon_string(comment, "id")] = comment
	}
	if item.comment == nil {
		for _, comment := range comments {
			descendants = append(descendants, mastodon_comment(s, comment, item.post))
		}
		c.JSON(http.StatusOK, map[string]any{"ancestors": ancestors, "descendants": descendants})
		return
	}
	// A comment's ancestors are the post and the comments it answers; its
	// descendants those answering it, however deep.
	ancestors = append(ancestors, mastodon_post(s, item.post))
	var chain []any
	for parent := mastodon_string(item.comment, "parent"); parent != ""; {
		comment := byid[parent]
		if comment == nil {
			break
		}
		chain = append([]any{mastodon_comment(s, comment, item.post)}, chain...)
		parent = mastodon_string(comment, "parent")
	}
	ancestors = append(ancestors, chain...)
	below := map[string]bool{mastodon_string(item.comment, "id"): true}
	for _, comment := range comments {
		if below[mastodon_string(comment, "parent")] {
			below[mastodon_string(comment, "id")] = true
			descendants = append(descendants, mastodon_comment(s, comment, item.post))
		}
	}
	c.JSON(http.StatusOK, map[string]any{"ancestors": ancestors, "descendants": descendants})
}

// === Timelines ===

// mastodon_timeline_page answers a page of a timeline: the posts of the feeds the
// user follows for the home timeline, a list's feed, a tag's posts, the
// favourites or the bookmarks.
func mastodon_timeline_page(c *gin.Context, s *mastodon_session, kind string, args Map) {
	in := mastodon_read(c)
	limit := mastodon_limit(in, 20)
	args["kind"] = kind
	args["limit"] = limit
	if kind == "list" && args["feed"] == "" {
		mastodon_error(c, http.StatusNotFound, "Record not found")
		return
	}
	cursor := func(number string) string {
		key := mastodon_key(s.user, "status", number)
		if id, ok := strings.CutPrefix(key, "post:"); ok {
			return id
		}
		if id, ok := strings.CutPrefix(key, "boost:"); ok {
			return id
		}
		return ""
	}
	if max := in.text("max_id"); max != "" {
		args["before"] = cursor(max)
	}
	if since := in.text("since_id"); since != "" {
		args["after"] = cursor(since)
	}
	if least := in.text("min_id"); least != "" {
		args["after"] = cursor(least)
		args["oldest"] = true
	}
	result, err := mastodon_call(s, "mastodon/posts", args)
	if err != nil {
		c.JSON(http.StatusOK, []any{})
		return
	}
	statuses := mastodon_statuses(s, mastodon_list(result))
	if len(statuses) > 0 {
		first := statuses[0].(map[string]any)["id"].(string)
		last := statuses[len(statuses)-1].(map[string]any)["id"].(string)
		mastodon_link(c, first, last)
	}
	c.JSON(http.StatusOK, statuses)
}

// mastodon_account_statuses answers an account's statuses: the signed-in
// account's own posts, as the feeds app holds them; someone else's, as their
// server lists them.
func mastodon_account_statuses(c *gin.Context, s *mastodon_session, number string, key string) {
	in := mastodon_read(c)
	if key == s.actor {
		if in.flag("pinned") {
			mastodon_timeline_page(c, s, "pinned", Map{})
			return
		}
		mastodon_timeline_page(c, s, "own", Map{})
		return
	}
	out := []any{}
	if !strings.HasPrefix(key, "https://") || in.flag("pinned") || in.text("max_id") != "" || in.text("min_id") != "" || in.text("since_id") != "" {
		c.JSON(http.StatusOK, out)
		return
	}
	ctx := activitypub_signing(c.Request.Context(), s.user, s.account)
	page, err := fediverse_public_posts(ctx, key, "")
	if err != nil {
		c.JSON(http.StatusOK, out)
		return
	}
	limit := mastodon_limit(in, 20)
	for _, item := range page.Items {
		note, ok := item.(map[string]any)
		if !ok || mastodon_string(note, "type") == "Announce" {
			continue
		}
		if status := mastodon_note(c, s, note); status != nil {
			out = append(out, status)
		}
		if len(out) == limit {
			break
		}
	}
	c.JSON(http.StatusOK, out)
}

// === Posting ===

// mastodon_idempotent is a request seen before under the same key, and what
// it was answered, so a client retrying a post posts once.
var mastodon_idempotent = map[string]mastodon_answered{}
var mastodon_idempotent_lock sync.Mutex

type mastodon_answered struct {
	body []byte
	when time.Time
}

// mastodon_status_create posts a status: a post in the home feed, or a comment on
// what it replies to where the feed holds it, or on a Note the feed takes in
// to answer.
func mastodon_status_create(c *gin.Context, s *mastodon_session) {
	key := ""
	if idempotency := c.GetHeader("Idempotency-Key"); idempotency != "" {
		key = s.token.Hash + "|" + idempotency
		mastodon_idempotent_lock.Lock()
		held, found := mastodon_idempotent[key]
		for k, v := range mastodon_idempotent {
			if time.Since(v.when) > time.Hour {
				delete(mastodon_idempotent, k)
			}
		}
		mastodon_idempotent_lock.Unlock()
		if found {
			c.Data(http.StatusOK, "application/json", held.body)
			return
		}
	}
	in := mastodon_read(c)
	text := in.text("status")
	if len([]rune(text)) > mastodon_characters {
		mastodon_error(c, http.StatusUnprocessableEntity, "Validation failed: Text character limit of 5000 exceeded")
		return
	}
	media, ok := mastodon_media_inputs(c, s, in)
	if !ok {
		return
	}
	if strings.TrimSpace(text) == "" && len(media) == 0 && !in.has("poll") {
		mastodon_error(c, http.StatusUnprocessableEntity, "Validation failed: Text can't be blank")
		return
	}
	if in.text("scheduled_at") != "" {
		mastodon_error(c, http.StatusUnprocessableEntity, "Scheduled statuses are not supported")
		return
	}
	args := Map{"text": text, "media": media, "visibility": mastodon_visibility_feeds(in.text("visibility")), "warning": in.text("spoiler_text"), "sensitive": in.flag("sensitive"), "language": in.text("language")}
	if poll := in.object("poll"); len(poll) > 0 {
		var options []any
		switch o := poll["options"].(type) {
		case []any:
			options = o
		case string:
			options = []any{o}
		}
		expires, _ := strconv.ParseInt(fmt.Sprint(poll["expires_in"]), 10, 64)
		multiple := fmt.Sprint(poll["multiple"]) == "true"
		args["poll"] = map[string]any{"options": options, "multiple": multiple, "duration": expires}
	}
	// A direct status is a message of a conversation, never a post.
	if in.text("visibility") == "direct" {
		mastodon_direct_create(c, s, text, in.text("in_reply_to_id"), key)
		return
	}
	if reply := in.text("in_reply_to_id"); reply != "" {
		item := mastodon_find(c, s, reply)
		if item == nil {
			mastodon_error(c, http.StatusNotFound, "Record not found")
			return
		}
		switch {
		case item.comment != nil:
			args["reply"] = "c:" + mastodon_string(item.comment, "id")
		case item.post != nil:
			args["reply"] = mastodon_string(item.post, "id")
		default:
			// A Note the feeds app does not hold is taken into the home feed,
			// so the reply is a comment on it, sent to its server.
			note := mastodon_fetch(c, s, strings.TrimPrefix(item.key, "note:"))
			if note == nil {
				mastodon_error(c, http.StatusNotFound, "Record not found")
				return
			}
			held, err := mastodon_call(s, "mastodon/hold", Map{"note": note})
			post, _ := held.(map[string]any)
			if err != nil || post == nil || mastodon_string(post, "error") != "" {
				mastodon_error(c, http.StatusUnprocessableEntity, mastodon_string(post, "error"))
				return
			}
			inner, _ := post["post"].(map[string]any)
			args["reply"] = mastodon_string(inner, "id")
		}
	}
	result, err := mastodon_call(s, "mastodon/create", args)
	if err != nil {
		mastodon_error(c, http.StatusUnprocessableEntity, err.Error())
		return
	}
	answer, _ := result.(map[string]any)
	if failure := mastodon_string(answer, "error"); failure != "" || answer == nil {
		mastodon_error(c, http.StatusUnprocessableEntity, failure)
		return
	}
	var status map[string]any
	if comment, ok := answer["comment"].(map[string]any); ok {
		post, _ := comment["post"].(map[string]any)
		status = mastodon_comment(s, comment, post)
	} else if post, ok := answer["post"].(map[string]any); ok {
		status = mastodon_post(s, post)
	}
	if status == nil {
		mastodon_error(c, http.StatusUnprocessableEntity, "The status could not be posted")
		return
	}
	for _, number := range in.list("media_ids") {
		mastodon_upload_remove(s.user, mastodon_key(s.user, "media", number))
	}
	body, _ := json.Marshal(status)
	if key != "" {
		mastodon_idempotent_lock.Lock()
		mastodon_idempotent[key] = mastodon_answered{body, time.Now()}
		mastodon_idempotent_lock.Unlock()
	}
	c.Data(http.StatusOK, "application/json", body)
}

// === Direct messages ===
//
// A direct status is a message of one of the account's conversations, which
// the chat app keeps (its direct/* functions): sent to the people it
// mentions, or into the conversation of the status it answers.

// mastodon_direct calls a direct message function of the app that keeps the
// conversations, as the account.
func mastodon_direct(s *mastodon_session, function string, args Map) (any, error) {
	a := mastodon_app(s.user, "direct/"+function)
	if a == nil {
		return nil, fmt.Errorf("No app here keeps conversations")
	}
	args["account"] = s.account
	return mastodon_caller(s, a, "direct/"+function, args)
}

// mastodon_member is someone in a conversation as an account: as last seen,
// else as their server describes them, else by the name chat keeps.
func mastodon_member(c *gin.Context, s *mastodon_session, id string, name string) map[string]any {
	if held := mastodon_snapshot_get(s.user, "account", id); held != nil {
		return held
	}
	if c != nil {
		if r := mastodon_resolve(c, s, id); r != nil && r.ID == id {
			return mastodon_account(s, mastodon_resolved(r))
		}
	}
	return mastodon_account(s, mastodon_figure{key: id, username: mastodon_slug(name), host: identifier_host(id), name: name, url: id})
}

// mastodon_direct_status is a direct message as a status: by the account where it
// is the user's, else by its sender, mentioning the conversation's people.
func mastodon_direct_status(c *gin.Context, s *mastodon_session, message map[string]any) map[string]any {
	key := "message:" + mastodon_string(message, "id")
	created := mastodon_integer(message, "created")
	account := mastodon_own(s, false)
	if message["mine"] != true {
		account = mastodon_member(c, s, mastodon_string(message, "member"), mastodon_string(message, "name"))
	}
	mentions := []any{}
	for _, person := range mastodon_list(message["people"]) {
		member := mastodon_member(c, s, mastodon_string(person, "id"), mastodon_string(person, "name"))
		mentions = append(mentions, map[string]any{"id": member["id"], "username": member["username"], "acct": member["acct"], "url": member["url"]})
	}
	if message["mine"] != true {
		mentions = append(mentions, map[string]any{"id": mastodon_own(s, false)["id"], "username": mastodon_own(s, false)["username"], "acct": mastodon_own(s, false)["acct"], "url": mastodon_own(s, false)["url"]})
	}
	content := mastodon_text(mastodon_string(message, "body"))
	status := map[string]any{
		"id":                     mastodon_number(s.user, "status", key, created),
		"created_at":             mastodon_time(created),
		"in_reply_to_id":         nil,
		"in_reply_to_account_id": nil,
		"sensitive":              false,
		"spoiler_text":           "",
		"visibility":             "direct",
		"language":               nil,
		"uri":                    mastodon_string(message, "remote"),
		"url":                    nil,
		"replies_count":          0,
		"reblogs_count":          0,
		"favourites_count":       0,
		"quotes_count":           0,
		"edited_at":              nil,
		"favourited":             false,
		"reblogged":              false,
		"muted":                  false,
		"bookmarked":             false,
		"pinned":                 false,
		"content":                content,
		"filtered":               mastodon_filtered(s, strip_tags(content)),
		"reblog":                 nil,
		"application":            nil,
		"account":                account,
		"media_attachments":      []any{},
		"mentions":               mentions,
		"tags":                   []any{},
		"emojis":                 []any{},
		"card":                   nil,
		"poll":                   nil,
	}
	if status["uri"] == "" {
		status["uri"] = "https://" + s.domain + "/chat/" + mastodon_string(message, "chat")
	}
	if edited := mastodon_integer(message, "edited"); edited > 0 {
		status["edited_at"] = mastodon_time(edited)
	}
	mastodon_snapshot(s.user, "status", key, status)
	return status
}

// mastodon_direct_action is what a client does to a direct status: read,
// edit and delete it, the user's own; it stands alone in its thread. Chat
// keeps no favourites, boosts or bookmarks of a message.
func mastodon_direct_action(c *gin.Context, s *mastodon_session, number string, item *mastodon_item, method string, action string) {
	id := mastodon_string(item.message, "id")
	answer := func(result any, err error) {
		m, _ := result.(map[string]any)
		if err != nil || m == nil || mastodon_string(m, "error") != "" {
			failure := mastodon_string(m, "error")
			if failure == "" {
				failure = "Record not found"
			}
			mastodon_error(c, http.StatusUnprocessableEntity, failure)
			return
		}
		full, _ := mastodon_direct(s, "message", Map{"message": id})
		if held, ok := full.(map[string]any); ok && held != nil {
			item.message = held
		}
		c.JSON(http.StatusOK, mastodon_direct_status(c, s, item.message))
	}
	switch method + " " + action {
	case "GET ":
		c.JSON(http.StatusOK, mastodon_direct_status(c, s, item.message))
	case "GET source":
		c.JSON(http.StatusOK, map[string]any{"id": number, "text": mastodon_string(item.message, "body"), "spoiler_text": ""})
	case "GET context":
		c.JSON(http.StatusOK, map[string]any{"ancestors": []any{}, "descendants": []any{}})
	case "GET history":
		c.JSON(http.StatusOK, []any{mastodon_direct_status(c, s, item.message)})
	case "PUT ":
		if item.message["mine"] != true {
			mastodon_error(c, http.StatusForbidden, "This action is not allowed")
			return
		}
		answer(mastodon_direct(s, "edit", Map{"message": id, "body": mastodon_read(c).text("status")}))
	case "DELETE ":
		if item.message["mine"] != true {
			mastodon_error(c, http.StatusForbidden, "This action is not allowed")
			return
		}
		status := mastodon_direct_status(c, s, item.message)
		result, err := mastodon_direct(s, "delete", Map{"message": id})
		if m, ok := result.(map[string]any); err != nil || !ok || mastodon_string(m, "error") != "" {
			mastodon_error(c, http.StatusUnprocessableEntity, mastodon_string(m, "error"))
			return
		}
		status["text"] = mastodon_string(item.message, "body")
		c.JSON(http.StatusOK, status)
	default:
		mastodon_error(c, http.StatusUnprocessableEntity, "Not available for direct messages")
	}
}

// mastodon_direct_create sends a direct status: into the conversation of the
// direct status it answers, else to the people it mentions.
func mastodon_direct_create(c *gin.Context, s *mastodon_session, text string, reply string, idempotency string) {
	args := Map{"body": text}
	if reply != "" {
		item := mastodon_find(c, s, reply)
		if item == nil || item.message == nil {
			mastodon_error(c, http.StatusUnprocessableEntity, "A direct status answers only a direct status")
			return
		}
		args["chat"] = mastodon_string(item.message, "chat")
	} else {
		var people []any
		for _, m := range fediverse_mention.FindAllStringSubmatch(text, -1) {
			if r := mastodon_resolve(c, s, "@"+m[2]+"@"+m[3]); r != nil && r.ID != s.actor {
				people = append(people, r.ID)
			}
		}
		if len(people) == 0 {
			mastodon_error(c, http.StatusUnprocessableEntity, "Validation failed: A direct status mentions who it is for")
			return
		}
		args["people"] = people
	}
	result, err := mastodon_direct(s, "send", args)
	message, _ := result.(map[string]any)
	if err != nil || message == nil || mastodon_string(message, "error") != "" {
		failure := mastodon_string(message, "error")
		if failure == "" && err != nil {
			failure = err.Error()
		}
		mastodon_error(c, http.StatusUnprocessableEntity, failure)
		return
	}
	full, _ := mastodon_direct(s, "message", Map{"message": mastodon_string(message, "id")})
	if held, ok := full.(map[string]any); ok && held != nil {
		message = held
	}
	body, _ := json.Marshal(mastodon_direct_status(c, s, message))
	if idempotency != "" {
		mastodon_idempotent_lock.Lock()
		mastodon_idempotent[idempotency] = mastodon_answered{body, time.Now()}
		mastodon_idempotent_lock.Unlock()
	}
	c.Data(http.StatusOK, "application/json", body)
}

// === Media ===

// mastodon_media_upload takes a file a client will attach: kept with the account
// until a status takes it, and answered at once, as Mastodon answers an
// image it has processed.
func mastodon_media_upload(c *gin.Context, s *mastodon_session) {
	file, header, err := c.Request.FormFile("file")
	if err != nil {
		mastodon_error(c, http.StatusUnprocessableEntity, "Validation failed: File can't be blank")
		return
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, clients_upload_maximum()+1))
	if err != nil || int64(len(data)) > clients_upload_maximum() {
		mastodon_error(c, http.StatusRequestEntityTooLarge, "File is too large")
		return
	}
	kind := header.Header.Get("Content-Type")
	if kind == "" || kind == "application/octet-stream" {
		kind = http.DetectContentType(data)
	}
	if mastodon_media_type(kind) == "unknown" {
		mastodon_error(c, http.StatusUnprocessableEntity, "Validation failed: File content type is invalid")
		return
	}
	// Uploads no status took within a day are let go.
	rows, _ := db_user(s.user, "user").rows("select key from snapshots where kind='upload' and updated<?", now()-86400)
	for _, row := range rows {
		mastodon_upload_remove(s.user, row_string(row, "key"))
	}
	id := uid()
	path := mastodon_upload_path(s.user, id)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		mastodon_error(c, http.StatusInternalServerError, "The file could not be kept")
		return
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		mastodon_error(c, http.StatusInternalServerError, "The file could not be kept")
		return
	}
	description := c.Request.FormValue("description")
	upload := map[string]any{"id": id, "name": header.Filename, "type": kind, "size": len(data), "description": description}
	mastodon_snapshot(s.user, "upload", id, upload)
	c.JSON(http.StatusOK, mastodon_upload_json(s, upload))
}

// mastodon_upload_path is where an upload waits for a status.
func mastodon_upload_path(user *User, id string) string {
	return filepath.Join(data_dir, "users", user.UID, "uploads", filepath.Base(id))
}

// mastodon_upload_remove lets an upload go.
func mastodon_upload_remove(user *User, id string) {
	if id == "" {
		return
	}
	os.Remove(mastodon_upload_path(user, id))
	db_user(user, "user").exec("delete from snapshots where kind='upload' and key=?", id)
}

// mastodon_upload_json is an upload as clients see media.
func mastodon_upload_json(s *mastodon_session, upload map[string]any) map[string]any {
	id := mastodon_string(upload, "id")
	out := map[string]any{
		"id":                 mastodon_number(s.user, "media", id, now()),
		"type":               mastodon_media_type(mastodon_string(upload, "type")),
		"url":                nil,
		"preview_url":        nil,
		"remote_url":         nil,
		"preview_remote_url": nil,
		"text_url":           nil,
		"meta":               map[string]any{},
		"description":        nil,
		"blurhash":           nil,
	}
	if description := mastodon_string(upload, "description"); description != "" {
		out["description"] = description
	}
	return out
}

// mastodon_media answers an upload not yet attached, or changes its
// description.
func mastodon_media(c *gin.Context, s *mastodon_session, number string) {
	id := mastodon_key(s.user, "media", number)
	upload := mastodon_snapshot_get(s.user, "upload", id)
	if upload == nil {
		mastodon_error(c, http.StatusNotFound, "Record not found")
		return
	}
	if c.Request.Method == "PUT" {
		in := mastodon_read(c)
		if in.has("description") {
			description := in.text("description")
			if len(description) > 1500 {
				mastodon_error(c, http.StatusUnprocessableEntity, "Validation failed: Description is too long")
				return
			}
			upload["description"] = description
			mastodon_snapshot(s.user, "upload", id, upload)
		}
	}
	c.JSON(http.StatusOK, mastodon_upload_json(s, upload))
}

// mastodon_media_inputs reads the uploads a status attaches, each with its
// bytes, and writes the refusal.
func mastodon_media_inputs(c *gin.Context, s *mastodon_session, in *mastodon_input) ([]any, bool) {
	numbers := in.list("media_ids")
	if len(numbers) > 4 {
		mastodon_error(c, http.StatusUnprocessableEntity, "Validation failed: You can attach at most 4 files")
		return nil, false
	}
	media := []any{}
	for _, number := range numbers {
		id := mastodon_key(s.user, "media", number)
		upload := mastodon_snapshot_get(s.user, "upload", id)
		if upload == nil {
			mastodon_error(c, http.StatusUnprocessableEntity, "Validation failed: The file is not one this account uploaded")
			return nil, false
		}
		data, err := os.ReadFile(mastodon_upload_path(s.user, id))
		if err != nil {
			mastodon_error(c, http.StatusUnprocessableEntity, "Validation failed: The file is gone")
			return nil, false
		}
		media = append(media, map[string]any{"name": mastodon_string(upload, "name"), "type": mastodon_string(upload, "type"), "description": mastodon_string(upload, "description"), "data": data})
	}
	return media, true
}

// === Polls ===

// mastodon_vote votes in a poll.
func mastodon_vote(c *gin.Context, s *mastodon_session, number string) {
	item := mastodon_find(c, s, mastodon_poll_status(s, number))
	if item == nil || item.post == nil {
		mastodon_error(c, http.StatusNotFound, "Record not found")
		return
	}
	in := mastodon_read(c)
	var choices []any
	for _, choice := range in.list("choices") {
		n, err := strconv.Atoi(choice)
		if err != nil {
			mastodon_error(c, http.StatusUnprocessableEntity, "Validation failed: The choice is invalid")
			return
		}
		choices = append(choices, n)
	}
	result, err := mastodon_call(s, "mastodon/vote", Map{"id": mastodon_string(item.post, "id"), "choices": choices})
	answer, _ := result.(map[string]any)
	if err != nil || mastodon_string(answer, "error") != "" {
		mastodon_error(c, http.StatusUnprocessableEntity, mastodon_string(answer, "error"))
		return
	}
	if post, ok := answer["post"].(map[string]any); ok {
		item.post = post
	}
	status := mastodon_render(c, s, item)
	if status == nil || status["poll"] == nil {
		mastodon_error(c, http.StatusNotFound, "Record not found")
		return
	}
	c.JSON(http.StatusOK, status["poll"])
}

// === Search ===

// mastodon_search finds accounts, statuses and hashtags: people by address,
// a status by its address elsewhere, those known whose names hold the
// words, and the hashtag asked.
func mastodon_search(c *gin.Context, s *mastodon_session) {
	in := mastodon_read(c)
	query := strings.TrimSpace(in.text("q"))
	kind := in.text("type")
	limit := mastodon_limit(in, 20)
	accounts, statuses, hashtags := []any{}, []any{}, []any{}
	if query == "" {
		c.JSON(http.StatusOK, map[string]any{"accounts": accounts, "statuses": statuses, "hashtags": hashtags})
		return
	}
	if kind == "" || kind == "accounts" {
		accounts = mastodon_people_search(c, s, query, in.flag("resolve"), limit)
	}
	if (kind == "" || kind == "statuses") && strings.HasPrefix(query, "https://") && in.flag("resolve") {
		if note := mastodon_fetch(c, s, query); note != nil {
			if status := mastodon_note(c, s, note); status != nil {
				statuses = append(statuses, status)
			}
		}
	}
	if kind == "" || kind == "hashtags" {
		if tag := strings.ToLower(strings.TrimPrefix(query, "#")); tag != "" && !strings.ContainsAny(tag, " /@:") {
			hashtags = append(hashtags, map[string]any{"name": tag, "url": "https://" + s.domain + "/feeds/search?q=%23" + tag, "history": []any{}, "following": false})
		}
	}
	c.JSON(http.StatusOK, map[string]any{"accounts": accounts, "statuses": statuses, "hashtags": hashtags})
}

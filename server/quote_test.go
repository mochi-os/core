// Mochi server: Tests for quoting posts on other networks
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"testing"

	sl "go.starlark.net/starlark"
)

// TestQuoteActivityPub: a status is read as its object, with the content
// warning first and the text from its HTML, the first image attachment, the
// time it was published and the author resolved; a Lemmy page carries its
// title and its image property; an actor document is not a post.
func TestQuoteActivityPub(t *testing.T) {
	d := resolve_test_install(t, map[string]string{
		"https://mastodon.social/@alice/1":    `{"id":"https://mastodon.social/users/alice/statuses/1","type":"Note","summary":"<p>Spoilers</p>","content":"<p>Hello <b>world</b></p><p>Second</p>","attributedTo":"https://mastodon.social/users/alice","url":"https://mastodon.social/@alice/1","published":"2026-10-01T12:00:00Z","attachment":[{"type":"Document","mediaType":"video/mp4","url":"https://files.example/a.mp4"},{"type":"Document","mediaType":"image/png","url":"https://files.example/a.png"}]}`,
		"https://mastodon.social/users/alice": `{"id":"https://mastodon.social/users/alice","type":"Person","preferredUsername":"alice","name":"Alice","url":"https://mastodon.social/@alice","icon":{"type":"Image","url":"https://files.example/alice.png"}}`,
		"https://lemmy.world/post/7":          `{"id":"https://lemmy.world/post/7","type":"Page","name":"A title","content":"<p>Body</p>","attributedTo":{"type":"Person","id":"https://lemmy.world/u/bob"},"image":{"type":"Image","url":"https://lemmy.world/pictrs/7.png"},"published":"2026-10-02T00:00:00Z"}`,
	})
	quote, err := quote_activitypub(context.Background(), nil, "https://mastodon.social/@alice/1", true)
	if err != nil {
		t.Fatal(err)
	}
	if quote["text"] != "Spoilers\n\nHello world\nSecond" || quote["image"] != "https://files.example/a.png" || quote["created"] != int64(1790856000) || quote["id"] != "https://mastodon.social/users/alice/statuses/1" || quote["url"] != "https://mastodon.social/@alice/1" {
		t.Errorf("quote = %+v", quote)
	}
	author, _ := quote["author"].(map[string]any)
	if author["name"] != "Alice" || author["address"] != "@alice@mastodon.social" || author["avatar"] != "https://files.example/alice.png" || author["url"] != "https://mastodon.social/@alice" {
		t.Errorf("author = %+v", author)
	}
	if d.fetches != 2 {
		t.Errorf("fetches = %d, want the object and the actor", d.fetches)
	}

	quote, err = quote_activitypub(context.Background(), nil, "https://lemmy.world/post/7", true)
	if err != nil {
		t.Fatal(err)
	}
	if quote["text"] != "A title\n\nBody" || quote["image"] != "https://lemmy.world/pictrs/7.png" {
		t.Errorf("page quote = %+v", quote)
	}
	// The author did not resolve: the identifier stands in.
	if author, _ := quote["author"].(map[string]any); author["id"] != "https://lemmy.world/u/bob" || author["name"] != "" {
		t.Errorf("unresolved author = %+v", author)
	}

	if _, err := quote_activitypub(context.Background(), nil, "https://mastodon.social/users/alice", true); err == nil {
		t.Error("an actor document quoted as a post")
	}
}

// TestQuoteObjectHostMustMatch: an object whose id names another host is read
// from that host, once, which must serve it under its own id; a host that does
// not is refused, and the original is not followed further.
func TestQuoteObjectHostMustMatch(t *testing.T) {
	alice, _ := create_handles_test_env(t)
	d := resolve_test_install(t, map[string]string{
		"https://lemmy.world/post/9":     `{"id":"https://programming.dev/post/4","type":"Page","name":"Shown on another instance","content":"<p>stale copy</p>"}`,
		"https://programming.dev/post/4": `{"id":"https://programming.dev/post/4","type":"Page","name":"The original","content":"<p>Body</p>"}`,
		"https://evil.example/1":         `{"id":"https://mastodon.social/users/alice/statuses/1","type":"Note","content":"x"}`,
		"https://evil.example/2":         `{"id":"https://relay.example/x","type":"Note","content":"x"}`,
		"https://relay.example/x":        `{"id":"https://elsewhere.example/y","type":"Note","content":"x"}`,
	})
	quote, err := quote_fetch(context.Background(), alice, "https://lemmy.world/post/9")
	if err != nil {
		t.Fatal(err)
	}
	if quote["text"] != "The original\n\nBody" || quote["id"] != "https://programming.dev/post/4" || d.fetches != 2 {
		t.Errorf("quote = %+v, fetches %d", quote, d.fetches)
	}
	if _, err := quote_fetch(context.Background(), alice, "https://evil.example/1"); err == nil {
		t.Error("an object whose claimed host does not serve it was accepted")
	}
	if _, err := quote_fetch(context.Background(), alice, "https://evil.example/2"); err == nil {
		t.Error("a chain of claimed ids was followed past the first")
	}
}

// TestQuoteBluesky: a post link on the Bluesky app resolves the handle, reads
// the record from the data server and shows its first image as a blob.
func TestQuoteBluesky(t *testing.T) {
	alice, _ := create_handles_test_env(t)
	did := "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
	d := resolve_test_install(t, map[string]string{
		"https://bob.bsky.social/.well-known/atproto-did": did,
		"https://plc.directory/" + did:                    `{"alsoKnownAs":["at://bob.bsky.social"],"service":[{"id":"#atproto_pds","type":"AtprotoPersonalDataServer","serviceEndpoint":"https://pds.example"}]}`,
		"https://pds.example/xrpc/com.atproto.repo.getRecord?repo=did%3Aplc%3Aewvi7nxzyoun6zhxrhs64oiz&collection=app.bsky.actor.profile&rkey=self": `{"value":{"displayName":"Bob"}}`,
		"https://pds.example/xrpc/com.atproto.repo.getRecord?repo=did%3Aplc%3Aewvi7nxzyoun6zhxrhs64oiz&collection=app.bsky.feed.post&rkey=3abc":     `{"uri":"at://` + did + `/app.bsky.feed.post/3abc","cid":"bafypost","value":{"text":"Hi there","createdAt":"2026-10-03T00:00:00Z","embed":{"$type":"app.bsky.embed.images","images":[{"alt":"A cat","image":{"ref":{"$link":"bafyimage"}}}]}}}`,
	})
	saved := bluesky_lookup
	bluesky_lookup = func(ctx context.Context, name string) ([]string, error) { return nil, context.Canceled }
	t.Cleanup(func() { bluesky_lookup = saved })

	quote, err := quote_fetch(context.Background(), alice, "https://bsky.app/profile/bob.bsky.social/post/3abc")
	if err != nil {
		t.Fatal(err)
	}
	if quote["network"] != "bluesky" || quote["text"] != "Hi there" || quote["id"] != "at://"+did+"/app.bsky.feed.post/3abc" || quote["url"] != "https://bsky.app/profile/bob.bsky.social/post/3abc" || quote["created"] != int64(1790985600) {
		t.Errorf("quote = %+v", quote)
	}
	if quote["image"] != "https://pds.example/xrpc/com.atproto.sync.getBlob?did=did%3Aplc%3Aewvi7nxzyoun6zhxrhs64oiz&cid=bafyimage" {
		t.Errorf("image = %v", quote["image"])
	}
	if author, _ := quote["author"].(map[string]any); author["name"] != "Bob" || author["address"] != "@bob.bsky.social" || author["id"] != did {
		t.Errorf("author = %+v", author)
	}
	if d.fetches != 4 {
		t.Errorf("fetches = %d, want the handle, the identity, the profile and the record", d.fetches)
	}

	// Through the API, with the network off, the answer is None.
	thread := create_test_thread(alice, create_external_app("app1"))
	fn := sl.NewBuiltin("mochi.url.quote", api_url_quote)
	value, err := api_url_quote(thread, fn, sl.Tuple{sl.String("https://bsky.app/profile/bob.bsky.social/post/3abc")}, nil)
	if err != nil || value == sl.None {
		t.Errorf("quote through the API = %v, %v", value, err)
	}
	setting_set("bluesky", "false")
	value, err = api_url_quote(thread, fn, sl.Tuple{sl.String("https://bsky.app/profile/bob.bsky.social/post/3abc")}, nil)
	if err != nil || value != sl.None {
		t.Errorf("quote with Bluesky off = %v, %v", value, err)
	}
	setting_set("bluesky", "true")
	value, err = api_url_quote(thread, fn, sl.Tuple{sl.String("https://example.com/page")}, nil)
	if err != nil || value != sl.None {
		t.Errorf("quote of an unanswered page = %v, %v", value, err)
	}
}

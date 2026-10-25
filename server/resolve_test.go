// Mochi server: Tests for people on other networks
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestAuthorNetwork: each network's identifier form is recognised, a Mochi
// identity is a valid author, and anything else is neither.
func TestAuthorNetwork(t *testing.T) {
	for identifier, want := range map[string]string{
		"https://lemmy.world/u/bob":            "fediverse",
		"https://mastodon.social/users/alice":  "fediverse",
		"did:plc:ewvi7nxzyoun6zhxrhs64oiz":     "bluesky",
		"did:web:example.com":                  "bluesky",
		"@bob:matrix.org":                      "matrix",
		"xmpp:bob@example.org":                 "xmpp",
		"bob@example.org":                      "",
		"http://insecure.example/users/a":      "",
		"https://":                             "",
		"https://user:pw@host.example/x":       "",
		"did:plc:short":                        "",
		"@bob":                                 "",
		"xmpp:bob":                             "",
		"mailto:bob@example.org":               "",
		"https://lemmy.world/u/bob with space": "",
		"1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNO": "",
	} {
		if got := author_network(identifier); got != want {
			t.Errorf("author_network(%q) = %q, want %q", identifier, got, want)
		}
	}
	entity := strings.Repeat("a", 50)
	if !author_valid(entity) || !author_valid("https://lemmy.world/u/bob") || author_valid("bob") {
		t.Error("author_valid wrong")
	}
	if !valid("@bob:matrix.org", "author") || valid("bob@example.org", "author") {
		t.Error("the author validator kind is not wired")
	}
}

// resolve_test_documents answers the documents the resolvers fetch, by URL,
// and counts the fetches.
type resolve_test_documents struct {
	documents map[string]string
	fetches   int
}

func (d *resolve_test_documents) fetch(ctx context.Context, address string, accept string) ([]byte, error) {
	d.fetches++
	if body, found := d.documents[address]; found {
		// A document of "401" stands for a server that wants a signature.
		if body == "401" {
			return nil, &resolve_refusal{Status: 401}
		}
		return []byte(body), nil
	}
	return nil, &resolve_refusal{Status: 404}
}

func resolve_test_install(t *testing.T, documents map[string]string) *resolve_test_documents {
	t.Helper()
	d := &resolve_test_documents{documents: documents}
	saved := resolve_fetch
	resolve_fetch = d.fetch
	resolve_lock.Lock()
	resolve_cache = map[string]resolve_entry{}
	resolve_lock.Unlock()
	t.Cleanup(func() {
		resolve_fetch = saved
		resolve_lock.Lock()
		resolve_cache = map[string]resolve_entry{}
		resolve_lock.Unlock()
	})
	return d
}

// TestResolveActivityPub: a handle goes through WebFinger to the actor
// document, which gives the address, name and icon; a second lookup is served
// from the cache.
func TestResolveActivityPub(t *testing.T) {
	d := resolve_test_install(t, map[string]string{
		"https://lemmy.world/.well-known/webfinger?resource=acct%3Abob%40lemmy.world": `{"links":[{"rel":"self","type":"application/activity+json","href":"https://lemmy.world/u/bob"}]}`,
		"https://lemmy.world/u/bob": `{"id":"https://lemmy.world/u/bob","type":"Person","preferredUsername":"bob","name":"Bob","url":"https://lemmy.world/u/bob","icon":{"type":"Image","url":"https://lemmy.world/pictrs/bob.png"}}`,
	})
	r, err := author_resolve(context.Background(), "@bob@lemmy.world")
	if err != nil {
		t.Fatal(err)
	}
	if r.Network != "fediverse" || r.Address != "@bob@lemmy.world" || r.Name != "Bob" || r.Avatar != "https://lemmy.world/pictrs/bob.png" || r.URL != "https://lemmy.world/u/bob" {
		t.Errorf("resolved = %+v", r)
	}
	if d.fetches != 2 {
		t.Errorf("fetches = %d, want 2 (webfinger and actor)", d.fetches)
	}
	if _, err := author_resolve(context.Background(), "@bob@lemmy.world"); err != nil || d.fetches != 2 {
		t.Errorf("a second lookup should come from the cache: fetches = %d, %v", d.fetches, err)
	}
	// The actor URL itself resolves without WebFinger.
	if r, err := author_resolve(context.Background(), "https://lemmy.world/u/bob"); err != nil || r.Address != "@bob@lemmy.world" || d.fetches != 3 {
		t.Errorf("actor URL lookup: %+v, %v, fetches %d", r, err, d.fetches)
	}
}

// TestResolveTransient: a lookup that failed for a reason that may pass is
// kept a minute and answers the same error while kept, so a caller still
// sees it may pass; one that found nothing is kept an hour.
func TestResolveTransient(t *testing.T) {
	d := resolve_test_install(t, map[string]string{})
	down := &resolve_refusal{Status: 503}
	saved := resolve_fetch
	resolve_fetch = func(ctx context.Context, address string, accept string) ([]byte, error) {
		if strings.Contains(address, "down.example") {
			d.fetches++
			return nil, down
		}
		return saved(ctx, address, accept)
	}
	if _, err := author_resolve(context.Background(), "https://down.example/u/bob"); !resolve_transient(err) {
		t.Fatalf("first lookup: %v, want the host's failure", err)
	}
	fetches := d.fetches
	if _, err := author_resolve(context.Background(), "https://down.example/u/bob"); !resolve_transient(err) || d.fetches != fetches {
		t.Errorf("THE POINT: kept, the failure reads %v after %d more fetches, want the same error from the cache", err, d.fetches-fetches)
	}
	resolve_lock.Lock()
	kept := time.Until(resolve_cache["https://down.example/u/bob"].expires)
	missing := time.Now()
	resolve_lock.Unlock()
	if kept > resolve_cache_transient {
		t.Errorf("THE POINT: a failure that may pass is kept %v, want at most %v", kept, resolve_cache_transient)
	}
	if _, err := author_resolve(context.Background(), "https://gone.example/u/bob"); err == nil || resolve_transient(err) {
		t.Fatalf("a missing person: %v", err)
	}
	resolve_lock.Lock()
	gone := resolve_cache["https://gone.example/u/bob"].expires.Sub(missing)
	resolve_lock.Unlock()
	if gone < resolve_cache_failure-time.Minute {
		t.Errorf("a person not found is kept %v, want %v", gone, resolve_cache_failure)
	}
}

// TestResolveGroup: a community resolves as a group with its outbox, its
// summary and whether it is sensitive, which a forum joining it reads.
func TestResolveGroup(t *testing.T) {
	resolve_test_install(t, map[string]string{
		"https://lemmy.world/c/books": `{"id":"https://lemmy.world/c/books","type":"Group","preferredUsername":"books","name":"Books","summary":"<p>Reading</p>","sensitive":true,"inbox":"https://lemmy.world/c/books/inbox","outbox":"https://lemmy.world/c/books/outbox"}`,
	})
	r, err := author_resolve(context.Background(), "https://lemmy.world/c/books")
	if err != nil {
		t.Fatal(err)
	}
	if r.Type != "group" || r.Outbox != "https://lemmy.world/c/books/outbox" || r.Summary != "<p>Reading</p>" || !r.Sensitive || r.Address != "@books@lemmy.world" {
		t.Errorf("resolved = %+v", r)
	}
}

// TestResolveBluesky: the DID document gives the handle and the data server,
// whose profile record gives the name and avatar.
func TestResolveBluesky(t *testing.T) {
	did := "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
	resolve_test_install(t, map[string]string{
		"https://plc.directory/" + did: `{"alsoKnownAs":["at://bob.bsky.social"],"service":[{"id":"#atproto_pds","type":"AtprotoPersonalDataServer","serviceEndpoint":"https://pds.example"}]}`,
		"https://pds.example/xrpc/com.atproto.repo.getRecord?repo=did%3Aplc%3Aewvi7nxzyoun6zhxrhs64oiz&collection=app.bsky.actor.profile&rkey=self": `{"value":{"displayName":"Bob","avatar":{"ref":{"$link":"bafyavatar"}}}}`,
	})
	r, err := author_resolve(context.Background(), did)
	if err != nil {
		t.Fatal(err)
	}
	if r.Network != "bluesky" || r.Address != "@bob.bsky.social" || r.Name != "Bob" || r.URL != "https://bsky.app/profile/bob.bsky.social" {
		t.Errorf("resolved = %+v", r)
	}
	if r.Avatar != "https://pds.example/xrpc/com.atproto.sync.getBlob?did=did%3Aplc%3Aewvi7nxzyoun6zhxrhs64oiz&cid=bafyavatar" {
		t.Errorf("avatar = %q", r.Avatar)
	}
}

// TestResolveMatrix: the client well-known names the home server, whose
// profile endpoint gives the name and an mxc avatar.
func TestResolveMatrix(t *testing.T) {
	resolve_test_install(t, map[string]string{
		"https://matrix.org/.well-known/matrix/client":                               `{"m.homeserver":{"base_url":"https://matrix-client.matrix.org/"}}`,
		"https://matrix-client.matrix.org/_matrix/client/v3/profile/@bob:matrix.org": `{"displayname":"Bob","avatar_url":"mxc://matrix.org/abc123"}`,
	})
	r, err := author_resolve(context.Background(), "@bob:matrix.org")
	if err != nil {
		t.Fatal(err)
	}
	if r.Network != "matrix" || r.Name != "Bob" || r.Address != "@bob:matrix.org" || r.Avatar != "https://matrix-client.matrix.org/_matrix/client/v1/media/download/matrix.org/abc123" {
		t.Errorf("resolved = %+v", r)
	}
}

// TestResolveXMPPAndFailure: an XMPP address needs no fetch; a network that
// does not answer is a failure, cached as one.
func TestResolveXMPPAndFailure(t *testing.T) {
	d := resolve_test_install(t, map[string]string{})
	r, err := author_resolve(context.Background(), "xmpp:bob@example.org")
	if err != nil || r.Network != "xmpp" || r.Address != "bob@example.org" || r.Name != "bob" || d.fetches != 0 {
		t.Errorf("xmpp: %+v, %v, fetches %d", r, err, d.fetches)
	}
	if _, err := author_resolve(context.Background(), "https://gone.example/users/x"); err == nil {
		t.Error("an unanswered lookup should fail")
	}
	if _, err := author_resolve(context.Background(), "https://gone.example/users/x"); err == nil || d.fetches != 1 {
		t.Errorf("a failure should be cached: fetches = %d", d.fetches)
	}
	if _, err := author_resolve(context.Background(), "not an identifier"); err == nil {
		t.Error("a non-identifier should fail")
	}
}

// TestResolveSecureMode: a server that gives its actor documents to signed
// requests only still resolves a handle from WebFinger - the address, the
// username and the profile page - while an actor URL alone does not.
func TestResolveSecureMode(t *testing.T) {
	d := resolve_test_install(t, map[string]string{
		"https://mastodon.social/.well-known/webfinger?resource=acct%3AMastodon%40mastodon.social": `{"links":[{"rel":"http://webfinger.net/rel/profile-page","type":"text/html","href":"https://mastodon.social/@Mastodon"},{"rel":"self","type":"application/activity+json","href":"https://mastodon.social/users/Mastodon"}]}`,
		"https://mastodon.social/users/Mastodon":                                                   "401",
	})
	r, err := author_resolve(context.Background(), "@Mastodon@mastodon.social")
	if err != nil {
		t.Fatal(err)
	}
	if r.Network != "fediverse" || r.Type != "person" || r.Address != "@Mastodon@mastodon.social" || r.Username != "Mastodon" || r.Host != "mastodon.social" || r.URL != "https://mastodon.social/@Mastodon" || r.ID != "https://mastodon.social/users/Mastodon" || r.Name != "Mastodon" {
		t.Errorf("resolved = %+v", r)
	}
	if d.fetches != 2 {
		t.Errorf("fetches = %d, want WebFinger and the refused actor", d.fetches)
	}
	if _, err := author_resolve(context.Background(), "https://mastodon.social/users/Mastodon"); err == nil {
		t.Error("an actor URL with no WebFinger to fall back on resolved")
	}
	if !resolve_signed(&resolve_refusal{Status: 403}) || resolve_signed(&resolve_refusal{Status: 404}) || resolve_signed(fmt.Errorf("status 401")) {
		t.Error("resolve_signed reads the status of a refusal alone")
	}
}

// TestResolveActorFields: the type, username and host come with an actor, and
// a document that is not an actor's is refused.
func TestResolveActorFields(t *testing.T) {
	resolve_test_install(t, map[string]string{
		"https://lemmy.world/c/technology": `{"id":"https://lemmy.world/c/technology","type":"Group","preferredUsername":"technology","name":"Technology"}`,
		"https://lemmy.world/post/1":       `{"id":"https://lemmy.world/post/1","type":"Page","name":"A post"}`,
	})
	r, err := author_resolve(context.Background(), "https://lemmy.world/c/technology")
	if err != nil {
		t.Fatal(err)
	}
	if r.Type != "group" || r.Username != "technology" || r.Host != "lemmy.world" || r.ID != "https://lemmy.world/c/technology" || r.Address != "@technology@lemmy.world" {
		t.Errorf("resolved = %+v", r)
	}
	if _, err := author_resolve(context.Background(), "https://lemmy.world/post/1"); err == nil {
		t.Error("a post resolved as a person")
	}
}

// TestResolveHandle: a Bluesky handle is a lookup form, resolved to its DID by
// the DNS record first and the well-known file second; the account's data
// server comes with the identity.
func TestResolveHandle(t *testing.T) {
	did := "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
	d := resolve_test_install(t, map[string]string{
		"https://bob.bsky.social/.well-known/atproto-did": did + "\n",
		"https://plc.directory/" + did:                    `{"alsoKnownAs":["at://bob.bsky.social"],"service":[{"id":"#atproto_pds","type":"AtprotoPersonalDataServer","serviceEndpoint":"https://pds.example"}]}`,
	})
	saved := bluesky_lookup
	records := []string{}
	bluesky_lookup = func(ctx context.Context, name string) ([]string, error) { return records, nil }
	t.Cleanup(func() { bluesky_lookup = saved })

	for _, form := range []string{"bob.bsky.social", "Bob.bsky.social"} {
		if !resolve_handle_form(form) {
			t.Errorf("%q is a handle", form)
		}
	}
	for _, form := range []string{"@bob.bsky.social", "bob@bsky.social", "https://bsky.app/profile/bob.bsky.social", "bob", "did:plc:ewvi7nxzyoun6zhxrhs64oiz"} {
		if resolve_handle_form(form) {
			t.Errorf("%q is not a handle", form)
		}
	}
	if resolve_network_of("bob.bsky.social") != "bluesky" || resolve_network_of("bob@lemmy.world") != "fediverse" || resolve_network_of("bob") != "" {
		t.Error("the network of a lookup form")
	}
	if identifier_host("bob.bsky.social") != "bob.bsky.social" {
		t.Error("a handle's host is the handle")
	}

	r, err := author_resolve(context.Background(), "bob.bsky.social")
	if err != nil {
		t.Fatal(err)
	}
	if r.ID != did || r.Username != "bob.bsky.social" || r.Server != "https://pds.example" || r.Type != "person" {
		t.Errorf("resolved = %+v", r)
	}
	if d.fetches != 3 {
		t.Errorf("fetches = %d, want the well-known file, the identity and the profile", d.fetches)
	}
	records = []string{"did=" + did}
	resolve_lock.Lock()
	resolve_cache = map[string]resolve_entry{}
	resolve_lock.Unlock()
	if _, err := author_resolve(context.Background(), "bob.bsky.social"); err != nil || d.fetches != 5 {
		t.Errorf("with a DNS record the well-known file is not fetched: fetches = %d, %v", d.fetches, err)
	}
}

// TestBlueskyPublicPosts: an account's post records are read from its data
// server and shaped as its feed shows them, the author from the profile and
// the images as blobs; the cursor carries to the next page.
func TestBlueskyPublicPosts(t *testing.T) {
	did := "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
	resolve_test_install(t, map[string]string{
		"https://plc.directory/" + did: `{"alsoKnownAs":["at://bob.bsky.social"],"service":[{"id":"#atproto_pds","type":"AtprotoPersonalDataServer","serviceEndpoint":"https://pds.example"}]}`,
		"https://pds.example/xrpc/com.atproto.repo.getRecord?repo=did%3Aplc%3Aewvi7nxzyoun6zhxrhs64oiz&collection=app.bsky.actor.profile&rkey=self":         `{"value":{"displayName":"Bob","avatar":{"ref":{"$link":"bafyavatar"}}}}`,
		"https://pds.example/xrpc/com.atproto.repo.listRecords?repo=did%3Aplc%3Aewvi7nxzyoun6zhxrhs64oiz&collection=app.bsky.feed.post&limit=2":             `{"cursor":"next","records":[{"uri":"at://` + did + `/app.bsky.feed.post/1","cid":"c1","value":{"text":"One","createdAt":"2026-10-03T00:00:00Z","embed":{"$type":"app.bsky.embed.recordWithMedia","media":{"$type":"app.bsky.embed.images","images":[{"alt":"Alt","image":{"ref":{"$link":"bafyone"}}}]}}}},{"uri":"at://` + did + `/app.bsky.feed.post/2","cid":"c2","value":{"text":"Two","embed":{"$type":"app.bsky.embed.external","external":{"uri":"https://example.com","title":"Example","thumb":{"ref":{"$link":"bafythumb"}}}}}}]}`,
		"https://pds.example/xrpc/com.atproto.repo.listRecords?repo=did%3Aplc%3Aewvi7nxzyoun6zhxrhs64oiz&collection=app.bsky.feed.post&limit=2&cursor=next": `{"records":[]}`,
	})
	page, err := bluesky_public_posts(context.Background(), did, "", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 2 || page.Cursor != "next" {
		t.Fatalf("page = %+v", page)
	}
	first := page.Items[0].(map[string]any)["post"].(map[string]any)
	author := first["author"].(map[string]any)
	if author["handle"] != "bob.bsky.social" || author["displayName"] != "Bob" || author["did"] != did {
		t.Errorf("author = %+v", author)
	}
	if first["record"].(map[string]any)["text"] != "One" || first["cid"] != "c1" {
		t.Errorf("record = %+v", first)
	}
	images := first["embed"].(map[string]any)["images"].([]any)
	if len(images) != 1 || images[0].(map[string]any)["fullsize"] != "https://pds.example/xrpc/com.atproto.sync.getBlob?did=did%3Aplc%3Aewvi7nxzyoun6zhxrhs64oiz&cid=bafyone" || images[0].(map[string]any)["alt"] != "Alt" {
		t.Errorf("images = %+v", images)
	}
	second := page.Items[1].(map[string]any)["post"].(map[string]any)
	external := second["embed"].(map[string]any)["external"].(map[string]any)
	if external["title"] != "Example" || external["thumb"] != "https://pds.example/xrpc/com.atproto.sync.getBlob?did=did%3Aplc%3Aewvi7nxzyoun6zhxrhs64oiz&cid=bafythumb" {
		t.Errorf("external = %+v", external)
	}
	if page, err := bluesky_public_posts(context.Background(), did, "next", 2); err != nil || len(page.Items) != 0 || page.Cursor != "" {
		t.Errorf("last page = %+v, %v", page, err)
	}
}

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
		return []byte(body), nil
	}
	return nil, fmt.Errorf("status 404")
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

// Mochi server: no response stamps a pair-affinity cookie.
//
// web_sticky.go stamped a `mochi-server-id` cookie into every same-site
// response, naming the peer that served it. Its purpose was whole-server
// pairing: with DNS round-robin across a pair, a session-aware load balancer
// could read the cookie and pin a browser to the host it started on. Pairing
// went with multi-host replication in July 2026, so there is no pair, no second
// host to pin to, and nothing that could act on it.
//
// The file's own header already conceded half of this - "Nothing in the server
// reads it back: it is a marker for a downstream routing layer, and for an
// operator asking which host answered a request." The routing layer cannot
// exist any more, and the operator half had no implementation: a search of the
// whole monorepo (Go, TypeScript, Starlark, JS, shell, Python, config) for
// "mochi-server-id" returned three hits, all inside web_sticky.go itself - two
// comments and the constant. Nothing anywhere consumed it.
//
// Removing it is visible to browsers, which is why it is gated: every response
// used to carry Set-Cookie, and the local peer id was disclosed to every client
// that asked for anything.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestNoStickySessionCookie fails if the middleware, its cookie or its wiring
// comes back. A restored middleware would compile and pass every other test —
// nothing reads the cookie, so nothing else can notice it.
func TestNoStickySessionCookie(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("globbing: %v", err)
	}
	for _, name := range files {
		if name == "web_sticky_removed_test.go" {
			continue // the comment above is the one legitimate mention
		}
		source, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("reading %s: %v", name, err)
		}
		for _, dead := range []string{"mochi-server-id", "sticky_session_cookie", "web_sticky_session"} {
			if strings.Contains(string(source), dead) {
				t.Errorf("%s references %s; the cookie pinned a browser to one member of a server pair, and there is no pair — if a served-by marker is wanted, it should be a response header chosen for that purpose", name, dead)
			}
		}
	}
}

// TestWebSetupHasNoStickyMiddleware pins the wiring specifically. The gate
// above would catch a restored file, but the middleware could also be
// reintroduced under a different name; this checks the router's middleware
// chain is the one intended.
func TestWebSetupHasNoStickyMiddleware(t *testing.T) {
	source, err := os.ReadFile("web.go")
	if err != nil {
		t.Fatalf("reading web.go: %v", err)
	}
	text := string(source)
	if !strings.Contains(text, "r.Use(web_resource_guard)\n\tr.Use(web_body_limit)") {
		t.Error("the middleware chain between web_resource_guard and web_body_limit has changed; a pair-affinity cookie stamper sat there and was removed, so check what has been inserted")
	}
}

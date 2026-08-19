// Mochi server: the WebAuthn instance cache was keyed on the Host header.
//
// request_origin builds its key from c.Request.Host, which the client chooses
// freely, and POST /_/auth/passkey/begin reaches webauthn_for_origin as its
// first statement with no authentication. Every distinct header value inserted
// a permanent map entry - 346 bytes measured, no cap, no eviction - so an
// anonymous caller grew the map for as long as it cared to.
//
// The cache is gone rather than bounded. webauthn.New is a url.Parse, four
// default-filling checks and a two-word allocation (236ns measured) on a path
// that runs a handful of times per user session, so there was nothing worth
// caching and therefore no eviction policy worth tuning.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"fmt"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// TestOriginsAreNotRetained is the defect. Every distinct Host an anonymous
// caller sends used to be kept for the life of the process; nothing may
// accumulate per origin now.
func TestOriginsAreNotRetained(t *testing.T) {
	// Warm up first: the first calls pull in lazily-initialised library and
	// runtime state, which would otherwise be charged to the measurement.
	for i := 0; i < 1000; i++ {
		webauthn_for_origin(fmt.Sprintf("http://warmup-%d.example", i))
	}

	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)

	const origins = 50000
	for i := 0; i < origins; i++ {
		if webauthn_for_origin(fmt.Sprintf("http://attacker-chosen-%07d.example", i)) == nil {
			t.Fatalf("origin %d produced no instance", i)
		}
	}

	runtime.GC()
	runtime.ReadMemStats(&after)

	// 346 bytes per entry was the measured cost of the map that was removed,
	// so 50k distinct origins retained ~17MB. A budget of 32 bytes each is far
	// under anything a per-origin structure could cost and far above the noise
	// of an unrelated allocation landing in the same window.
	var retained int64
	if after.HeapAlloc > before.HeapAlloc {
		retained = int64(after.HeapAlloc - before.HeapAlloc)
	}
	if budget := int64(origins) * 32; retained > budget {
		t.Errorf("%d distinct origins retained %d bytes (%d each); nothing may be kept per origin, or an anonymous caller grows it by sending a fresh Host header",
			origins, retained, retained/origins)
	}
}

// TestEachOriginGetsItsOwnInstance: removing the cache must not start handing
// out a single shared instance, which would validate assertions against
// whichever origin happened to be seen first.
func TestEachOriginGetsItsOwnInstance(t *testing.T) {
	first := webauthn_for_origin("https://mochi-os.org")
	second := webauthn_for_origin("http://localhost:8081")
	if first == nil || second == nil {
		t.Fatal("webauthn_for_origin returned nil for a well-formed origin")
	}
	if first == second {
		t.Fatal("two origins share one instance, so a ceremony would be validated against the wrong origin")
	}
	if got := first.Config.RPOrigins; len(got) != 1 || got[0] != "https://mochi-os.org" {
		t.Errorf("RPOrigins = %v, want the caller's own origin", got)
	}
	if got := second.Config.RPOrigins; len(got) != 1 || got[0] != "http://localhost:8081" {
		t.Errorf("RPOrigins = %v, want the caller's own origin", got)
	}
}

// TestRelyingPartyIdentifierStripsSchemeAndPort: the browser only returns
// credentials whose stored RPID matches exactly, so the same site reached over
// http, https and any port must produce one identifier. A regression here
// silently stops existing passkeys from being offered.
func TestRelyingPartyIdentifierStripsSchemeAndPort(t *testing.T) {
	for _, origin := range []string{
		"https://mochi-os.org",
		"http://mochi-os.org",
		"https://mochi-os.org:8443",
		"http://mochi-os.org:8081",
	} {
		instance := webauthn_for_origin(origin)
		if instance == nil {
			t.Fatalf("no instance for %q", origin)
		}
		if got := instance.Config.RPID; got != "mochi-os.org" {
			t.Errorf("origin %q gave RPID %q, want mochi-os.org; a passkey registered at another port would not be offered", origin, got)
		}
	}
}

// TestEmptyOriginIsRefused: a request with no Host must not produce an
// instance configured for nothing.
func TestEmptyOriginIsRefused(t *testing.T) {
	if webauthn_for_origin("") != nil {
		t.Error("an empty origin produced a WebAuthn instance")
	}
}

// TestRequestOriginIsTheClientsHostHeader documents WHY nothing may be keyed
// on this value. It is the header verbatim - this is not a defect in itself
// (the browser, not the server, decides which credentials to release), but it
// means the string is attacker-chosen and unbounded in cardinality.
func TestRequestOriginIsTheClientsHostHeader(t *testing.T) {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/_/auth/passkey/begin", nil)
	c.Request.Host = "anything-the-client-likes.example"

	if got := request_origin(c); got != "http://anything-the-client-likes.example" {
		t.Errorf("request_origin = %q; if this is ever sanitised the comment above needs revisiting, but the value is still not a safe cache key", got)
	}
}

// TestNoWebauthnInstanceCacheRemains: a cache reintroduced "for performance"
// puts the unbounded map straight back, since the key is still the Host
// header. The saving it would buy is 236ns on a per-session path.
func TestNoWebauthnInstanceCacheRemains(t *testing.T) {
	body := function_body(t, "passkeys.go", "func webauthn_for_origin(")
	for _, marker := range []string{"webauthn_instances", "webauthn_mu"} {
		if strings.Contains(body, marker) {
			t.Errorf("webauthn_for_origin references %s again; an origin-keyed cache is an anonymous caller's memory leak", marker)
		}
	}
}

// Mochi server: tests for the auth and account hardening (#607-#622)
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pquerna/otp/totp"

	"github.com/gin-gonic/gin"
)

// TestSamesiteMiddlewareRefusesCrossSitePost is #607. gin's JSON binding ignores
// Content-Type, so a cross-origin form whose text/plain body is valid JSON reaches
// a login handler and its Set-Cookie is stored. Both signals must refuse.
func TestSamesiteMiddlewareRefusesCrossSitePost(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.POST("/probe", web_samesite_middleware, func(c *gin.Context) { c.String(http.StatusOK, "reached") })

	cases := []struct {
		name    string
		site    string
		content string
		want    int
	}{
		{"cross-site fetch metadata", "cross-site", "application/json", http.StatusForbidden},
		{"cross-origin fetch metadata", "cross-origin", "application/json", http.StatusForbidden},
		{"form post, no fetch metadata", "", "text/plain;charset=UTF-8", http.StatusForbidden},
		{"urlencoded, no fetch metadata", "", "application/x-www-form-urlencoded", http.StatusForbidden},
		{"same-origin json", "same-origin", "application/json", http.StatusOK},
		{"no metadata, json (curl, mobile)", "", "application/json", http.StatusOK},
		{"multipart, no metadata (restore)", "", "multipart/form-data; boundary=x", http.StatusOK},
	}
	for _, c := range cases {
		request := httptest.NewRequest("POST", "/probe", strings.NewReader("{}"))
		if c.site != "" {
			request.Header.Set("Sec-Fetch-Site", c.site)
		}
		request.Header.Set("Content-Type", c.content)
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		if response.Code != c.want {
			t.Errorf("%s: status %d, want %d", c.name, response.Code, c.want)
		}
	}
}

// TestReauthenticationBoundToSession is #608. Two sessions of one account share
// the row without this: the owner's email verification plus an attacker's TOTP
// completes a proof the attacker then spends.
func TestReauthenticationBoundToSession(t *testing.T) {
	test_data_directory(t)
	db_create()

	user := &User{UID: "u-split", Username: "split@example.com", Methods: "email,totp"}

	// The owner's browser verifies email; the attacker's session verifies totp.
	if token, remaining := reauthentication_advance(user, "browser", "email"); token != "" || len(remaining) != 1 {
		t.Fatalf("email advance = (%q, %v), want no token and one remaining", token, remaining)
	}
	token, remaining := reauthentication_advance(user, "stolen", "totp")
	if token != "" {
		t.Error("a factor verified in one session completed the other session's proof")
	}
	if len(remaining) != 1 || remaining[0] != "email" {
		t.Errorf("attacker remaining = %v, want [email]: its own accrual has only totp", remaining)
	}

	// The owner finishing in its own session still works.
	token, remaining = reauthentication_advance(user, "browser", "totp")
	if token == "" || len(remaining) != 0 {
		t.Fatalf("owner second factor = (%q, %v), want a token", token, remaining)
	}
	if reauthentication_consume(user, "stolen", token) {
		t.Error("a proof minted in one session was spent from another")
	}
	if !reauthentication_consume(user, "browser", token) {
		t.Error("the session that earned the proof could not spend it")
	}
}

// TestTotpCodeIsSingleUse is #616. RFC 6238 section 5.2: a code from a step
// already accepted must be refused, or a shoulder-surfed code replays for the
// rest of its window.
func TestTotpCodeIsSingleUse(t *testing.T) {
	test_data_directory(t)
	db_create()

	db := db_open("db/users.db")
	secret := "JBSWY3DPEHPK3PXP"
	// totp.user is a foreign key into users.
	db.exec("insert into users (uid, username, role, methods) values (?, ?, 'user', 'totp')", "u-totp", "totp@example.com")
	db.exec("insert into totp (user, secret, verified, created) values (?, ?, 1, ?)", "u-totp", secret, now())

	code, err := totp.GenerateCode(secret, time.Now())
	if err != nil {
		t.Fatalf("generating a code: %v", err)
	}
	if !totp_verify("u-totp", code) {
		t.Fatal("a fresh code was refused")
	}
	if totp_verify("u-totp", code) {
		t.Error("the same code was accepted twice inside its window")
	}
}

// TestEntityKeyMatches is #609. Restore is the one path that writes an entity id
// it did not derive, and only the primary is signature-checked - with a key that
// is itself in the bundle.
func TestEntityKeyMatches(t *testing.T) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generating a key: %v", err)
	}
	id := base58_encode(public)
	if !entity_key_matches(id, base58_encode(private)) {
		t.Error("a genuine id/key pair was refused")
	}

	// The planted case: a real key, but an id naming somebody else's entity.
	other, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generating a second key: %v", err)
	}
	if entity_key_matches(base58_encode(other), base58_encode(private)) {
		t.Error("an id that is not the public half of its key was accepted")
	}
	if entity_key_matches(id, "not-base58-at-all") {
		t.Error("a malformed private key was accepted")
	}
	if entity_key_matches("", "") {
		t.Error("an empty pair was accepted")
	}
}

// TestOauthIssuerPinnedUnlessMultiTenant is #622. The issuer check can only be
// skipped where it cannot be satisfied - Microsoft's shared endpoints.
func TestOauthIssuerPinnedUnlessMultiTenant(t *testing.T) {
	test_data_directory(t)
	db_create()

	for _, tenant := range []string{"common", "organizations", "consumers"} {
		setting_set("oauth_microsoft_tenant", tenant)
		if !oauth_issuer_unpinned() {
			t.Errorf("tenant %q: issuer check kept, but its tokens carry a per-tenant issuer", tenant)
		}
	}
	for _, tenant := range []string{"", "contoso.onmicrosoft.com", "72f988bf-86f1-41af-91ab-2d7cd011db47"} {
		setting_set("oauth_microsoft_tenant", tenant)
		if oauth_issuer_unpinned() {
			t.Errorf("tenant %q: issuer check skipped, but this deployment has one issuer", tenant)
		}
	}
}

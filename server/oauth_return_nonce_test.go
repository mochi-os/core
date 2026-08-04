// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// redirect_query runs oauth_mobile_redirect and returns the query it sent the
// app, so each case can assert on one parameter rather than a whole URL.
func redirect_query(t *testing.T, st *oauth_state, code, error_code string, extras map[string]string) url.Values {
	t.Helper()
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	// gin's redirect renderer reads the request to decide the status wording,
	// so a bare test context panics without one.
	c.Request = httptest.NewRequest("GET", "/_/auth/oauth/google/callback", nil)
	oauth_mobile_redirect(c, st, code, error_code, extras)

	location := recorder.Header().Get("Location")
	prefix := st.Scheme + ":oauth-return?"
	if !strings.HasPrefix(location, prefix) {
		t.Fatalf("Location = %q, want prefix %q", location, prefix)
	}
	query, err := url.ParseQuery(strings.TrimPrefix(location, prefix))
	if err != nil {
		t.Fatalf("parse query: %v", err)
	}
	return query
}

// The deep-link return must carry the ceremony's return nonce on EVERY branch.
//
// The activity that receives mochi:oauth-return is exported and BROWSABLE, so
// any installed app or web page can deliver one. Without a value the app can
// check, an injected return is indistinguishable from the real one: accepting
// it consumes the PKCE verifier before the exchange, so the genuine callback
// then fails with "missing verifier" and the login cannot complete.
//
// The error branch matters at least as much as the success branch. A forged
// error needs no plausible exchange code, so it is the cheaper forgery, and it
// was the branch that stayed unauthenticated when only success carried an
// identifier.
func TestMobileRedirectCarriesReturnNonce(t *testing.T) {
	gin.SetMode(gin.TestMode)
	st := &oauth_state{Scheme: "mochi", Return: oauth_return{Nonce: "nonce-abc"}}

	cases := []struct {
		name  string
		code  string
		error string
	}{
		{"success", "exchange-123", ""},
		{"error", "", "provider_error"},
	}
	for _, one := range cases {
		t.Run(one.name, func(t *testing.T) {
			query := redirect_query(t, st, one.code, one.error, nil)
			if got := query.Get("nonce"); got != "nonce-abc" {
				t.Errorf("nonce = %q, want %q — an unauthenticated %s return can be forged by any app that can deliver an intent", got, "nonce-abc", one.name)
			}
		})
	}
}

// A ceremony begun before this server carried return nonces has none stored.
// Those rows live for ten minutes, so a deploy mid-ceremony must not send the
// app the literal string "nonce=" to compare against — the parameter is simply
// absent, and the client treats absence as "this server does not send one".
func TestMobileRedirectOmitsAbsentNonce(t *testing.T) {
	gin.SetMode(gin.TestMode)
	st := &oauth_state{Scheme: "mochi"}

	query := redirect_query(t, st, "exchange-123", "", nil)
	if _, present := query["nonce"]; present {
		t.Errorf("nonce present for a ceremony that has none: %q", query.Get("nonce"))
	}
}

// The nonce must not collide with the extras the callers already send, and
// extras must not be able to overwrite it.
func TestMobileRedirectNonceSurvivesExtras(t *testing.T) {
	gin.SetMode(gin.TestMode)
	st := &oauth_state{Scheme: "mochi", Return: oauth_return{Nonce: "nonce-abc"}}

	query := redirect_query(t, st, "", "email_exists", map[string]string{
		"provider": "google",
		"email":    "someone@example.com",
	})
	if got := query.Get("nonce"); got != "nonce-abc" {
		t.Errorf("nonce = %q, want %q — extras displaced it", got, "nonce-abc")
	}
	if got := query.Get("provider"); got != "google" {
		t.Errorf("provider = %q, want google — the nonce displaced an extra", got)
	}
}

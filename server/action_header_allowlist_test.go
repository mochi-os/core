// Mochi server: a.header reads only an allowlisted request header.
//
// It returned any header verbatim, so a.header("Cookie") yielded the session -
// the same bearer credential the removed a.cookie API handed over - and
// a.header("Authorization") yielded whatever Bearer token the client sent. An
// allowlist, not a denylist: naming only those two would expose by default
// every sensitive header added later.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	sl "go.starlark.net/starlark"
)

// header_action builds an action whose request carries the given headers.
func header_action(t *testing.T, headers map[string]string) *Action {
	t.Helper()
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest("GET", "/", nil)
	for k, v := range headers {
		c.Request.Header.Set(k, v)
	}
	return &Action{web: c, active: &AppVersion{}}
}

// header_read calls a.header(name) with one argument.
func header_read(t *testing.T, a *Action, name string) (sl.Value, error) {
	t.Helper()
	fn := sl.NewBuiltin("header", a.sl_header)
	return a.sl_header(&sl.Thread{}, fn, sl.Tuple{sl.String(name)}, nil)
}

// TestHeaderRefusesCookie is the regression: the session must not come back.
func TestHeaderRefusesCookie(t *testing.T) {
	a := header_action(t, map[string]string{"Cookie": "session=secret"})

	value, err := header_read(t, a, "Cookie")
	if err == nil {
		t.Fatalf("a.header(\"Cookie\") returned %v with no error; that is the session credential", value)
	}
	if strings.Contains(err.Error(), "secret") {
		t.Errorf("the refusal quotes the header value: %v", err)
	}
}

// TestHeaderRefusesCookieWhateverTheCase. Header.Get canonicalises, so
// "cookie" reaches the same value as "Cookie" - an allowlist keyed on the
// caller's spelling would be bypassed by lowercasing it.
func TestHeaderRefusesCookieWhateverTheCase(t *testing.T) {
	a := header_action(t, map[string]string{"Cookie": "session=secret"})

	for _, spelling := range []string{"cookie", "COOKIE", "CoOkIe", "cOOKIE"} {
		value, err := header_read(t, a, spelling)
		if err == nil {
			t.Errorf("a.header(%q) returned %v; Header.Get canonicalises, so this is the Cookie header", spelling, value)
		}
	}
}

// TestHeaderRefusesAuthorization: the other credential-bearing header.
func TestHeaderRefusesAuthorization(t *testing.T) {
	a := header_action(t, map[string]string{"Authorization": "Bearer mochi-abc123"})

	for _, spelling := range []string{"Authorization", "authorization"} {
		if value, err := header_read(t, a, spelling); err == nil {
			t.Errorf("a.header(%q) returned %v; that is the client's bearer token", spelling, value)
		}
	}
}

// TestHeaderAllowsTheConventionalSet is the other direction. Refusing
// everything would be safe and useless; these carry no credential and the tree
// already reads two of them.
func TestHeaderAllowsTheConventionalSet(t *testing.T) {
	allowed := map[string]string{
		"Accept":           "application/json",
		"Accept-Language":  "en-GB",
		"Content-Type":     "application/json",
		"Referer":          "https://mochi-os.org/",
		"Sec-Fetch-Site":   "cross-site",
		"Stripe-Signature": "t=1,v1=abc",
		"User-Agent":       "Mochi/1.0",
	}
	a := header_action(t, allowed)

	for name, want := range allowed {
		value, err := header_read(t, a, name)
		if err != nil {
			t.Errorf("a.header(%q) refused: %v", name, err)
			continue
		}
		if got, _ := sl.AsString(value); got != want {
			t.Errorf("a.header(%q) = %q, want %q", name, got, want)
		}
	}
}

// TestHeaderAllowsTheTwoTheTreeReads pins the two live call sites by name:
// comptroller verifies the Stripe webhook signature, settings checks the
// fetch site. Dropping either from the list breaks a shipped app.
func TestHeaderAllowsTheTwoTheTreeReads(t *testing.T) {
	for _, name := range []string{"Stripe-Signature", "Sec-Fetch-Site"} {
		if !header_readable[name] {
			t.Errorf("%s is not readable, but an app reads it today", name)
		}
	}
}

// TestHeaderStillSetsResponseHeaders. The read and write arms share one
// function, split on len(args), so an allowlist on the read must not reach the
// write - apps set Content-Type and Cache-Control on every serve path.
func TestHeaderStillSetsResponseHeaders(t *testing.T) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest("GET", "/", nil)
	a := &Action{web: c, active: &AppVersion{}}

	fn := sl.NewBuiltin("header", a.sl_header)
	args := sl.Tuple{sl.String("Content-Type"), sl.String("application/json")}
	if _, err := a.sl_header(&sl.Thread{}, fn, args, nil); err != nil {
		t.Fatalf("setting a response header failed: %v", err)
	}
	if got := recorder.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", got)
	}
}

// TestHeaderWriteIsNotGatedByTheReadAllowlist: a response header outside the
// readable set must still be settable. Cookie is the sharp case - refusing to
// SET it is #166's business, not this allowlist's, and conflating the two
// would silently change behaviour the write arm never had.
func TestHeaderWriteIsNotGatedByTheReadAllowlist(t *testing.T) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest("GET", "/", nil)
	a := &Action{web: c, active: &AppVersion{}}

	fn := sl.NewBuiltin("header", a.sl_header)
	args := sl.Tuple{sl.String("Content-Disposition"), sl.String("attachment")}
	if _, err := a.sl_header(&sl.Thread{}, fn, args, nil); err != nil {
		t.Fatalf("Content-Disposition is not in the read allowlist but must still be settable: %v", err)
	}
	if got := recorder.Header().Get("Content-Disposition"); got != "attachment" {
		t.Errorf("Content-Disposition = %q, want attachment", got)
	}
}

// TestHeaderAllowlistIsCanonical guards the table itself: a key stored
// non-canonically can never match, so the entry would be silently dead and the
// header silently refused.
func TestHeaderAllowlistIsCanonical(t *testing.T) {
	for name := range header_readable {
		if canonical := http.CanonicalHeaderKey(name); canonical != name {
			t.Errorf("header_readable key %q is not canonical (%q); it can never match", name, canonical)
		}
	}
}

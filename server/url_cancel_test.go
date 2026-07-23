// Mochi server: outbound HTTP cancellation tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"time"

	"testing"
)

// TestURLRequestCancels pins that an outbound request stops when its context is
// cancelled, so an app's HTTP call abandoned at the Starlark compute timeout
// does not run on with no concurrency slot and no cancellation. Without the
// context the request would block until its own (app-supplied) timeout.
func TestURLRequestCancels(t *testing.T) {
	allow_private_for_test(t)

	// A server that never responds — the request only ends when cancelled or
	// timed out.
	block := make(chan struct{})
	defer close(block)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done() // hold until the client goes away
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	// A long app timeout — cancellation, not the timeout, must end this.
	_, err := url_request(ctx, "GET", server.URL, map[string]string{"timeout": "300"}, nil, nil)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("a cancelled request returned no error")
	}
	if elapsed > 5*time.Second {
		t.Errorf("request took %s to notice cancellation; it ignored the context", elapsed)
	}
}

// TestURLTimeout pins the timeout selection exactly: an app cannot name an
// absurd value, an unset or invalid one falls to the default, and a value
// within the cap is honoured.
func TestURLTimeout(t *testing.T) {
	if url_max_timeout <= 0 || url_max_timeout > time.Hour {
		t.Fatalf("url_max_timeout is %s, not a sane finite ceiling", url_max_timeout)
	}
	cases := []struct {
		name    string
		options map[string]string
		want    time.Duration
	}{
		{"unset", nil, 30 * time.Second},
		{"invalid", map[string]string{"timeout": "soon"}, 30 * time.Second},
		{"zero", map[string]string{"timeout": "0"}, 30 * time.Second},
		{"negative", map[string]string{"timeout": "-5"}, 30 * time.Second},
		{"within cap", map[string]string{"timeout": "60"}, 60 * time.Second},
		{"a week is clamped", map[string]string{"timeout": "604800"}, url_max_timeout},
	}
	for _, c := range cases {
		if got := url_timeout(c.options); got != c.want {
			t.Errorf("%s: url_timeout = %s, want %s", c.name, got, c.want)
		}
	}
}

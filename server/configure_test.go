// Mochi server: development overrides read from the configuration.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import "testing"

// The ini layer honours MOCHI_<SECTION>_<KEY> before any file, which lets
// these run without loading a configuration file over the other tests.

func TestUrlConfigurePrivate(t *testing.T) {
	previous := url_allow_private
	defer func() { url_allow_private = previous }()

	url_allow_private = true
	url_configure()
	if url_allow_private {
		t.Fatal("no configuration: outbound requests to private addresses must stay refused")
	}

	t.Setenv("MOCHI_DEVELOPMENT_PRIVATE", "true")
	url_configure()
	if !url_allow_private {
		t.Fatal("[development] private = true must allow loopback and private addresses")
	}
	if err := url_address_allowed("127.0.0.1:8080"); err != nil {
		t.Fatalf("loopback refused with the override on: %v", err)
	}
}

func TestLimitsConfigureRequests(t *testing.T) {
	previous := rate_limit_api.limit
	defer func() { rate_limit_api.limit = previous }()

	limits_configure()
	if rate_limit_api.limit != previous {
		t.Fatalf("no configuration: the request budget changed from %d to %d", previous, rate_limit_api.limit)
	}

	t.Setenv("MOCHI_WEB_REQUESTS", "50000")
	limits_configure()
	if rate_limit_api.limit != 50000 {
		t.Fatalf("[web] requests = 50000 gave a budget of %d", rate_limit_api.limit)
	}

	// Zero or negative is not a budget; the default stands.
	t.Setenv("MOCHI_WEB_REQUESTS", "0")
	rate_limit_api.limit = previous
	limits_configure()
	if rate_limit_api.limit != previous {
		t.Fatalf("[web] requests = 0 changed the budget to %d", rate_limit_api.limit)
	}
}

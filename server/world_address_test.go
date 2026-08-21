// Mochi server: a world listing's address is an endpoint, not any URL.
//
// world_validate used valid(address, "url"), a charset check whose class holds
// ":" and "%", so "javascript:alert%281%29" fits it. The listing arrives by
// gossip, is re-served through mochi.world.list, and is rendered on join
// pages.//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"strings"
	"testing"
)

// TestWorldAddressRefusesActiveSchemes is the regression. Each of these passes
// the charset check, which is what made the leading scheme the whole defect.
func TestWorldAddressRefusesActiveSchemes(t *testing.T) {
	for _, address := range []string{
		"javascript:alert%281%29",
		"JaVaScRiPt:alert%281%29",
		"javascript:fetch%28%27https%3A%2F%2Fevil%27%29",
		"vbscript:msgbox%281%29",
		"file:%2F%2F%2Fetc%2Fpasswd",
		"ftp:example.com",
		"ws://example.com:4433",
		"wss://example.com:4433",
	} {
		if world_address_valid(address) {
			t.Errorf("world_address_valid(%q) = true; only http, https and a bare host[:port] are reachable world addresses", address)
		}
		// And the charset check alone would have let the dangerous ones by,
		// which is the point: this is not a duplicate of valid().
		if strings.HasPrefix(strings.ToLower(address), "javascript:") && !valid(address, "url") {
			t.Errorf("valid(%q, \"url\") already refused it; this test's premise is gone", address)
		}
	}
}

// TestWorldAddressAcceptsRealEndpoints: the shapes normalize_server and
// default_server actually produce must survive, or a valid world stops being
// listed.
func TestWorldAddressAcceptsRealEndpoints(t *testing.T) {
	for _, address := range []string{
		"example.com",
		"example.com:4433",
		"worlds.mochi-os.org:4433",
		"https://example.com:4433",
		"http://example.com:4433",
		"HTTPS://example.com:4433",
		"https://192.0.2.10:4433",
		"192.0.2.10:4433",
		"localhost:4433",
		"https://example.com:4433/lobby",
	} {
		if !world_address_valid(address) {
			t.Errorf("world_address_valid(%q) = false; that is a shape a real world server is reached at", address)
		}
	}
}

// TestWorldAddressRejectsEmptyAndMalformed keeps the obvious cases pinned;
// the empty check used to live in world_validate beside the charset call.
func TestWorldAddressRejectsEmptyAndMalformed(t *testing.T) {
	for _, address := range []string{
		"",
		"example.com:",             // a colon with no port
		"example.com:44a33",        // not a port
		" example.com",             // the charset refuses whitespace
		strings.Repeat("a", 10001), // over the charset's length bound
	} {
		if world_address_valid(address) {
			t.Errorf("world_address_valid(%q) = true, want false", address)
		}
	}
}

// TestWorldValidateRefusesAnActiveSchemeAddress drives world_validate itself,
// so the check is wired into the path gossip and the local push both take -
// not merely present as a helper.
func TestWorldValidateRefusesAnActiveSchemeAddress(t *testing.T) {
	id := strings.Repeat("a", 32)
	services := `[{"service":"air","players":1}]`

	if _, ok := world_validate(id, "Test World", "javascript:alert%281%29", "1", services); ok {
		t.Error("world_validate accepted a javascript: address; the listing is gossiped to other servers and rendered on their join pages")
	}
	if _, ok := world_validate(id, "Test World", "example.com:4433", "1", services); !ok {
		t.Error("world_validate rejected a real world address")
	}
	if _, ok := world_validate(id, "Test World", "https://example.com:4433", "1", services); !ok {
		t.Error("world_validate rejected an explicit https world address")
	}
}

// TestWorldAddressDoesNotNarrowTheSharedUrlValidator: valid(s, "url") is
// exposed to apps as mochi.text.valid, so tightening the shared case would
// change what every app's own URL validation means. The world's rule belongs
// to the world.
func TestWorldAddressDoesNotNarrowTheSharedUrlValidator(t *testing.T) {
	if !valid("javascript:alert%281%29", "url") {
		t.Error(`valid(s, "url") now refuses a javascript: URL. That may well be right, but it is an app-visible change to mochi.text.valid and wants its own decision - world_address_valid exists so this one did not have to be made here`)
	}
}

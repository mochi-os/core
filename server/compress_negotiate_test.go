// Mochi server: Accept-Encoding is a list with weights, not a string to search.
//
// negotiate_encoding tested the header with strings.Contains, which is wrong in
// both directions. "br;q=0" contains "br", so a client explicitly refusing
// brotli - how a proxy that cannot pass it through says so - was sent brotli
// and could not decode the reply. "*" contains neither "br" nor "gzip", so a
// client accepting every coding was sent an uncompressed response.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import "testing"

// negotiate_with runs one header against one server setting.
func negotiate_with(t *testing.T, setting string, accept string) string {
	t.Helper()
	original := web_compress
	web_compress = setting
	defer func() { web_compress = original }()
	return negotiate_encoding(accept)
}

// TestNegotiateRefusedCodingIsNotSent is the defect. q=0 is the client saying
// "not acceptable", and answering with it anyway hands them bytes they told us
// they cannot decode.
func TestNegotiateRefusedCodingIsNotSent(t *testing.T) {
	cases := []struct {
		setting string
		accept  string
		want    string
		why     string
	}{
		{"auto", "br;q=0, gzip", "gzip", "the client refused brotli and offered gzip in the same header"},
		{"auto", "br;q=0", "", "the client refused brotli and offered nothing else"},
		{"auto", "gzip;q=0", "", "the client refused gzip and offered nothing else"},
		{"auto", "br;q=0, gzip;q=0", "", "the client refused both"},
		{"br", "br;q=0", "", "the server prefers brotli but the client refused it"},
		{"gzip", "gzip;q=0", "", "the server prefers gzip but the client refused it"},
		{"auto", "br;q=0.0", "", "q may be written with decimals"},
		{"auto", "br;q=0.000", "", "and to three places"},
	}
	for _, c := range cases {
		if got := negotiate_with(t, c.setting, c.accept); got != c.want {
			t.Errorf("compress=%s, Accept-Encoding %q gave %q, want %q: %s",
				c.setting, c.accept, got, c.want, c.why)
		}
	}
}

// TestNegotiateWildcardIsAccepted. "*" means every coding is acceptable, and
// answering uncompressed wastes the bandwidth the client just offered to save.
func TestNegotiateWildcardIsAccepted(t *testing.T) {
	if got := negotiate_with(t, "auto", "*"); got != "br" {
		t.Errorf("Accept-Encoding \"*\" gave %q, want br: the client accepts every coding", got)
	}
	if got := negotiate_with(t, "gzip", "*"); got != "gzip" {
		t.Errorf("compress=gzip with \"*\" gave %q, want gzip", got)
	}
	// A wildcard covers what the header does not name, and no more: a named
	// refusal is still a refusal.
	if got := negotiate_with(t, "auto", "br;q=0, *"); got != "gzip" {
		t.Errorf("Accept-Encoding %q gave %q, want gzip: the wildcard must not override an explicit refusal", "br;q=0, *", got)
	}
	// And a wildcard can itself be a refusal of everything unnamed.
	if got := negotiate_with(t, "auto", "identity, *;q=0"); got != "" {
		t.Errorf("Accept-Encoding %q gave %q, want none", "identity, *;q=0", got)
	}
}

// TestNegotiateHonoursPreference. Parsing q for the refusal case makes the
// ordering free, and ignoring it sent brotli to a client that asked for gzip.
func TestNegotiateHonoursPreference(t *testing.T) {
	if got := negotiate_with(t, "auto", "gzip;q=1.0, br;q=0.5"); got != "gzip" {
		t.Errorf("Accept-Encoding %q gave %q, want gzip: the client weighted gzip higher", "gzip;q=1.0, br;q=0.5", got)
	}
	if got := negotiate_with(t, "auto", "gzip;q=0.5, br;q=1.0"); got != "br" {
		t.Errorf("Accept-Encoding %q gave %q, want br", "gzip;q=0.5, br;q=1.0", got)
	}
	// Equal weights go to brotli, which compresses better.
	if got := negotiate_with(t, "auto", "gzip, br"); got != "br" {
		t.Errorf("Accept-Encoding %q gave %q, want br on a tie", "gzip, br", got)
	}
}

// TestNegotiateOrdinaryHeadersStillWork guards the change: the browsers are the
// traffic, and a stricter parser that stopped compressing for them would be a
// far worse defect than the one being fixed.
func TestNegotiateOrdinaryHeadersStillWork(t *testing.T) {
	for _, accept := range []string{
		"gzip, deflate, br",
		"gzip, deflate, br, zstd",
		"gzip,deflate,br",
		"GZIP, DEFLATE, BR",
		" gzip , deflate , br ",
	} {
		if got := negotiate_with(t, "auto", accept); got != "br" {
			t.Errorf("a browser sending %q got %q, want br", accept, got)
		}
	}
	if got := negotiate_with(t, "auto", "gzip"); got != "gzip" {
		t.Errorf("a gzip-only client got %q, want gzip", got)
	}
	for _, accept := range []string{"", "identity", "deflate", "zstd"} {
		if got := negotiate_with(t, "auto", accept); got != "" {
			t.Errorf("Accept-Encoding %q gave %q, want none: the server encodes neither", accept, got)
		}
	}
}

// TestNegotiateMalformedWeightIsNotARefusal. A header we cannot parse should
// read as an absent q, not as q=0 - treating a malformed value as a refusal
// would quietly stop compressing for whatever sends it.
func TestNegotiateMalformedWeightIsNotARefusal(t *testing.T) {
	for _, accept := range []string{"br;q=", "br;q=abc", "br;q", "br;level=1"} {
		if got := negotiate_with(t, "auto", accept); got != "br" {
			t.Errorf("Accept-Encoding %q gave %q, want br: an unreadable parameter is not a refusal", accept, got)
		}
	}
}

// TestNegotiateExplicitSettingSendsOnlyWhatItNames. web.compress=br is an
// operator choosing brotli, not asking for a fallback they did not pick.
func TestNegotiateExplicitSettingSendsOnlyWhatItNames(t *testing.T) {
	if got := negotiate_with(t, "br", "gzip"); got != "" {
		t.Errorf("compress=br with a gzip-only client gave %q, want none", got)
	}
	if got := negotiate_with(t, "gzip", "br"); got != "" {
		t.Errorf("compress=gzip with a brotli-only client gave %q, want none", got)
	}
	if got := negotiate_with(t, "none", "gzip, br"); got != "" {
		t.Errorf("compress=none gave %q, want none", got)
	}
}

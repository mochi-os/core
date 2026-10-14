// Mochi server: Tests for link cards read from a page's head
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"strings"
	"testing"
)

// TestURLCardReadsTheHead: Open Graph tags come first, Twitter card tags fill
// what they leave, the page's own title and description come last, a relative
// image resolves against the page, and the parser stops at the body.
func TestURLCardReadsTheHead(t *testing.T) {
	page := `<!doctype html><html><head>
<title>  Page   title </title>
<meta name="description" content="Meta description">
<meta property="og:title" content="OG title">
<meta name="twitter:description" content="Twitter description">
<meta property="og:image" content="/images/card.png">
<meta property="og:site_name" content="Example">
</head><body><meta property="og:description" content="in the body, ignored"></body></html>`
	card := url_extract_card(strings.NewReader(page), "https://example.com/articles/1")
	if card["title"] != "OG title" || card["description"] != "Twitter description" || card["image"] != "https://example.com/images/card.png" || card["site"] != "Example" {
		t.Errorf("card = %v", card)
	}

	bare := `<html><head><title>Only a title</title><meta name="description" content="Only a description"></head><body></body></html>`
	card = url_extract_card(strings.NewReader(bare), "https://example.com/")
	if card["title"] != "Only a title" || card["description"] != "Only a description" || card["image"] != "" || card["site"] != "" {
		t.Errorf("bare card = %v", card)
	}

	// The first of a repeated tag wins, and the image alone is what preview answered.
	twice := `<html><head><meta property="og:image" content="https://a.example/1.png"><meta property="og:image" content="https://a.example/2.png"></head>`
	if card := url_extract_card(strings.NewReader(twice), "https://example.com/"); card["image"] != "https://a.example/1.png" || card["title"] != "" {
		t.Errorf("repeated card = %v", card)
	}
	if card := url_extract_card(strings.NewReader("<html><body>no head</body></html>"), "https://example.com/"); card["title"] != "" || card["image"] != "" {
		t.Errorf("headless card = %v", card)
	}
}

// TestNetworkBase: a host is reached over https, except a loopback host where
// the development switch admits private addresses, which is reached over http
// so a test server can stand in for a remote one.
func TestNetworkBase(t *testing.T) {
	saved := url_allow_private
	t.Cleanup(func() { url_allow_private = saved })
	url_allow_private = false
	if network_base("lemmy.world") != network_scheme+"lemmy.world" || network_base("127.0.0.1:8080") != network_scheme+"127.0.0.1:8080" {
		t.Error("without the switch every host is https")
	}
	url_allow_private = true
	if network_base("127.0.0.1:8080") != "http://127.0.0.1:8080" || network_base("localhost") != "http://localhost" || network_base("lemmy.world") != network_scheme+"lemmy.world" {
		t.Error("with the switch loopback is http and the rest https")
	}
}

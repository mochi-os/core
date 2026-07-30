// Mochi server: making an app's og:image absolute.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import "testing"

// TestOpenGraphAbsolute covers the shapes apps actually emit. The entity-route
// case is the defect: people and feeds both emit "-/avatar" from a page served at
// /<app>/<fingerprint>, and ordinary relative resolution would drop the
// fingerprint and point at an asset route that does not exist.
func TestOpenGraphAbsolute(t *testing.T) {
	cases := []struct {
		name   string
		image  string
		scheme string
		host   string
		path   string
		want   string
	}{
		{
			"entity route without a trailing slash",
			"-/avatar", "https", "mochi-os.org", "/people/1abcdef",
			"https://mochi-os.org/people/1abcdef/-/avatar",
		},
		{
			"entity route with a trailing slash",
			"-/avatar", "https", "mochi-os.org", "/people/1abcdef/",
			"https://mochi-os.org/people/1abcdef/-/avatar",
		},
		{
			"direct entity route",
			"-/avatar", "https", "mochi-os.org", "/1abcdef",
			"https://mochi-os.org/1abcdef/-/avatar",
		},
		{
			"root-relative is host-relative, not path-relative",
			"/static/logo.png", "https", "mochi-os.org", "/people/1abcdef",
			"https://mochi-os.org/static/logo.png",
		},
		{
			"an absolute URL is left alone",
			"https://cdn.example.com/a.png", "https", "mochi-os.org", "/people/1abcdef",
			"https://cdn.example.com/a.png",
		},
		{
			"a protocol-relative URL is left alone",
			"//cdn.example.com/a.png", "https", "mochi-os.org", "/people/1abcdef",
			"//cdn.example.com/a.png",
		},
		{
			"http when the server is not serving https",
			"-/avatar", "http", "localhost:8081", "/people/1abcdef",
			"http://localhost:8081/people/1abcdef/-/avatar",
		},
		{
			"a domain route keeps the domain it was reached on",
			"-/avatar", "https", "alice.example.com", "/",
			"https://alice.example.com/-/avatar",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := opengraph_absolute(c.image, c.scheme, c.host, c.path)
			if got != c.want {
				t.Errorf("opengraph_absolute(%q, %q, %q, %q) = %q, want %q",
					c.image, c.scheme, c.host, c.path, got, c.want)
			}
		})
	}
}

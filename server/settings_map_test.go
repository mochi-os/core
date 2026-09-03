// Mochi server: the map tile settings every app's maps read.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import "testing"

// A server that has configured nothing renders OpenStreetMap's own tiles and
// credits them; both values are readable by the anonymous viewer of a public
// feed, so they must be Public.
func TestMapTileSettingsDefaults(t *testing.T) {
	url, ok := system_settings["map_tile_url"]
	if !ok {
		t.Fatal("map_tile_url is not declared")
	}
	if url.Default != "https://tile.openstreetmap.org/{z}/{x}/{y}.png" {
		t.Errorf("map_tile_url default = %q", url.Default)
	}
	if !url.Public {
		t.Error("map_tile_url must be Public: anonymous readers of a public feed render maps")
	}
	attribution, ok := system_settings["map_tile_attribution"]
	if !ok {
		t.Fatal("map_tile_attribution is not declared")
	}
	if attribution.Default != "© OpenStreetMap contributors" {
		t.Errorf("map_tile_attribution default = %q", attribution.Default)
	}
	if !attribution.Public {
		t.Error("map_tile_attribution must be Public")
	}
}

// The URL template keeps its Leaflet placeholders; a value spanning lines is
// refused, and so is an empty one (the map would then render nothing).
func TestMapTileSettingsValidation(t *testing.T) {
	pattern := system_settings["map_tile_url"].Pattern
	if !valid("https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png", pattern) {
		t.Error("a provider template with subdomain and retina placeholders must validate")
	}
	if valid("https://a.example/{z}\n/{x}/{y}.png", pattern) {
		t.Error("a template with a line break must not validate")
	}
	if valid("", pattern) {
		t.Error("an empty template must not validate")
	}
}

// Mochi server: theme manifest validation.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"strings"
	"testing"
)

// theme returns a valid theme in the shape the bundled themes use, which
// every case then perturbs. If this baseline ever fails validation, the
// bundled apps/themes manifest would be rejected at load.
func theme(mutate func(*AppTheme)) AppTheme {
	t := AppTheme{
		ID:           "terracotta",
		Label:        "theme_terracotta",
		Hue:          40,
		Chroma:       0.15,
		HueBG:        35,
		Spacing:      "comfortable",
		BorderRadius: "0.375rem",
		Overrides:    map[string]string{"--primary-l": "0.55"},
	}
	if mutate != nil {
		mutate(&t)
	}
	return t
}

func TestThemesValidate(t *testing.T) {
	cases := []struct {
		name   string
		themes []AppTheme
		icons  map[string]string
		want   string // substring of the error, "" for valid
	}{
		{"bundled shape", []AppTheme{theme(nil)}, nil, ""},
		{"no themes", nil, nil, ""},
		{"duplicate id", []AppTheme{theme(nil), theme(func(t *AppTheme) { t.Hue = 200 })}, nil, "duplicate theme id"},
		{"empty id", []AppTheme{theme(func(t *AppTheme) { t.ID = "" })}, nil, "bad theme id"},
		{"id with colon", []AppTheme{theme(func(t *AppTheme) { t.ID = "a:b" })}, nil, "bad theme id"},
		{"empty label", []AppTheme{theme(func(t *AppTheme) { t.Label = "" })}, nil, "bad theme label"},
		{"hue out of range", []AppTheme{theme(func(t *AppTheme) { t.Hue = 361 })}, nil, "bad theme hue"},
		{"negative background hue", []AppTheme{theme(func(t *AppTheme) { t.HueBG = -1 })}, nil, "bad theme hue"},
		{"chroma out of range", []AppTheme{theme(func(t *AppTheme) { t.Chroma = 0.6 })}, nil, "bad theme chroma"},
		{"radius not a length", []AppTheme{theme(func(t *AppTheme) { t.BorderRadius = "calc(1rem - 2px)" })}, nil, "bad theme border radius"},
		{"radius injection", []AppTheme{theme(func(t *AppTheme) { t.BorderRadius = `1rem"` })}, nil, "bad theme border radius"},
		{"bad spacing", []AppTheme{theme(func(t *AppTheme) { t.Spacing = "cosy" })}, nil, "bad theme spacing"},
		{"font stack", []AppTheme{theme(func(t *AppTheme) { t.FontSans = "Georgia, 'Times New Roman', serif" })}, nil, ""},
		{"font injection", []AppTheme{theme(func(t *AppTheme) { t.FontSans = `x</style>` })}, nil, "bad theme font"},
		{"bad icon mask", []AppTheme{theme(func(t *AppTheme) { t.IconMask = "hexagon" })}, nil, "bad theme icon mask"},
		{"icon mask shape", []AppTheme{theme(func(t *AppTheme) { t.IconMask = "squircle"; t.IconBackground = "oklch(0.5 0.1 250)" })}, nil, ""},
		{"ordinary property key", []AppTheme{theme(func(t *AppTheme) { t.Overrides = map[string]string{"display": "none"} })}, nil, "bad theme override"},
		{"font-size key", []AppTheme{theme(func(t *AppTheme) { t.Overrides = map[string]string{"font-size": "200%"} })}, nil, "bad theme override"},
		{"override value injection", []AppTheme{theme(func(t *AppTheme) { t.Overrides = map[string]string{"--primary-l": `0.5;display:none`} })}, nil, "bad theme override value"},
		{"external url", []AppTheme{theme(func(t *AppTheme) {
			t.Overrides = map[string]string{"--background-image": "url(https://evil.example/beacon)"}
		})}, nil, "bad theme override value"},
		{"external url quoted spaced", []AppTheme{theme(func(t *AppTheme) {
			t.Overrides = map[string]string{"--background-image": "URL( 'https://evil.example/x' )"}
		})}, nil, "bad theme override value"},
		{"protocol-relative url", []AppTheme{theme(func(t *AppTheme) { t.Overrides = map[string]string{"--background-image": "url(//evil.example/x)"} })}, nil, "bad theme override value"},
		{"relative url", []AppTheme{theme(func(t *AppTheme) { t.Overrides = map[string]string{"--background-image": "url(/themes/waves.svg)"} })}, nil, ""},
		{"gradient value", []AppTheme{theme(func(t *AppTheme) {
			t.Overrides = map[string]string{"--background-image": "radial-gradient(ellipse at top, color-mix(in oklch, var(--primary) 12%, transparent), transparent 70%)"}
		})}, nil, ""},
		{"long override value", []AppTheme{theme(func(t *AppTheme) { t.Overrides = map[string]string{"--x": strings.Repeat("a", 501)} })}, nil, "bad theme override value"},
		{"icon traversal", []AppTheme{theme(func(t *AppTheme) { t.Icons = map[string]string{"feeds": "../../secret.png"} })}, nil, "bad theme icon file"},
		{"icon file", []AppTheme{theme(func(t *AppTheme) { t.Icons = map[string]string{"feeds": "feeds-brutalist.png"} })}, nil, ""},
		{"icon empty path", []AppTheme{theme(func(t *AppTheme) { t.Icons = map[string]string{"": "x.png"} })}, nil, "bad theme icon path"},
		{"theme icon", nil, map[string]string{"themes:blue": "images/blue.png"}, ""},
		{"theme icon without app", nil, map[string]string{"blue": "images/blue.png"}, "bad theme icon id"},
		{"theme icon traversal", nil, map[string]string{"themes:blue": "../db/users.db"}, "bad theme icon file"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			av := AppVersion{Themes: c.themes, ThemeIcons: c.icons}
			err := themes_validate(&av)
			if c.want == "" {
				if err != nil {
					t.Fatalf("themes_validate = %v, want valid", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("themes_validate accepted the manifest, want error containing %q", c.want)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("themes_validate = %v, want error containing %q", err, c.want)
			}
		})
	}
}

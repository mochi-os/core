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
		// Every construct below was verified fetching in Chrome when set as
		// --background-image, which lib/web's base layer consumes as a real
		// background-image. A theme has no legitimate reason to fetch, so
		// same-origin and relative references are refused too rather than
		// leaving a shape for an attacker to aim at.
		{"external url", []AppTheme{theme(func(t *AppTheme) {
			t.Overrides = map[string]string{"--background-image": "url(https://evil.example/beacon)"}
		})}, nil, "bad theme override value"},
		{"external url quoted spaced", []AppTheme{theme(func(t *AppTheme) {
			t.Overrides = map[string]string{"--background-image": "URL( 'https://evil.example/x' )"}
		})}, nil, "bad theme override value"},
		{"protocol-relative url", []AppTheme{theme(func(t *AppTheme) { t.Overrides = map[string]string{"--background-image": "url(//evil.example/x)"} })}, nil, "bad theme override value"},
		{"relative url", []AppTheme{theme(func(t *AppTheme) { t.Overrides = map[string]string{"--background-image": "url(/themes/waves.svg)"} })}, nil, "bad theme override value"},
		{"escaped scheme", []AppTheme{theme(func(t *AppTheme) {
			t.Overrides = map[string]string{"--background-image": `url(h\74tp://evil.example/x)`}
		})}, nil, "bad theme override value"},
		{"escaped url token", []AppTheme{theme(func(t *AppTheme) {
			t.Overrides = map[string]string{"--background-image": `\75rl(http://evil.example/x)`}
		})}, nil, "bad theme override value"},
		{"image-set", []AppTheme{theme(func(t *AppTheme) {
			t.Overrides = map[string]string{"--background-image": "image-set('https://evil.example/x' 1x)"}
		})}, nil, "bad theme override value"},
		{"webkit image-set", []AppTheme{theme(func(t *AppTheme) {
			t.Overrides = map[string]string{"--background-image": "-webkit-image-set(url('https://evil.example/x') 1x)"}
		})}, nil, "bad theme override value"},
		{"cross-fade", []AppTheme{theme(func(t *AppTheme) {
			t.Overrides = map[string]string{"--background-image": "cross-fade(url('https://evil.example/x') 50%, red)"}
		})}, nil, "bad theme override value"},
		{"comment split", []AppTheme{theme(func(t *AppTheme) {
			t.Overrides = map[string]string{"--background-image": "u/*x*/rl(https://evil.example/x)"}
		})}, nil, "bad theme override value"},
		// The style attribute is HTML, so the parser decodes character
		// references before CSS parses the value: these spell url( and a
		// declaration separator without containing either literally.
		{"character reference in url", []AppTheme{theme(func(t *AppTheme) {
			t.Overrides = map[string]string{"--background-image": "u&#114;l(https://evil.example/x)"}
		})}, nil, "bad theme override value"},
		{"character reference separator", []AppTheme{theme(func(t *AppTheme) {
			t.Overrides = map[string]string{"--primary-l": "red&#59;background-&#105mage:u&#114l(https://evil.example/x)"}
		})}, nil, "bad theme override value"},
		{"character reference in font", []AppTheme{theme(func(t *AppTheme) {
			t.FontSans = "Georgia&#59;color:red"
		})}, nil, "bad theme font"},
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

// TestWebUserThemeStyleEscapes covers the boundary rather than the value: the
// style attribute is HTML, so whatever reaches it must leave escaped. The
// radius preference is the shortest path to a value the caller controls, and
// its own check (like every check that reasons about the raw string) does not
// stop a character reference — u&#114l( is url( only after the HTML parser has
// decoded it, which happens after every string check in the server has run.
func TestWebUserThemeStyleEscapes(t *testing.T) {
	user, cleanup := create_test_user(t)
	defer cleanup()

	// theme is set empty so the active-theme lookup is skipped: this test is
	// about the attribute, not about resolving a theme. The references are
	// deliberately unterminated — a trailing semicolon would be caught by the
	// existing literal-semicolon check, whereas `&#59` reaches the attribute
	// and was verified in Chrome decoding to `;` and injecting the declaration.
	user.Preferences = map[string]string{
		"theme":  "",
		"radius": "1rem&#59background-&#105mage:u&#114l(https://evil.example/x)",
	}

	style := web_user_theme_style(user)
	if style == "" {
		t.Fatal("web_user_theme_style returned nothing, so the test proves nothing")
	}
	if strings.Contains(style, "&#") {
		t.Errorf("style attribute carries an undecoded character reference, which the HTML parser will turn back into CSS: %s", style)
	}
	if !strings.Contains(style, "&amp;#") {
		t.Errorf("expected the ampersands to be escaped, got: %s", style)
	}
	// The quote in a font stack must survive as a quote once the parser
	// decodes the attribute, so escaping may not be lossy.
	user.Preferences = map[string]string{"theme": "", "font": "serif"}
	style = web_user_theme_style(user)
	if !strings.Contains(style, "&#39;Times New Roman&#39;") {
		t.Errorf("font stack quotes should survive as escaped quotes, got: %s", style)
	}
}

// The shell re-reads the theme from /_/shell when an app reports that the
// preference changed, and installs the result on the trusted root — so the
// declarations must arrive as CSS, not as the HTML attribute the page template
// wants. A style="..." wrapper or an escaped ampersand reaching setProperty is
// a value the browser drops, which would silently leave the chrome unthemed.
func TestWebUserThemeDeclarations(t *testing.T) {
	user, cleanup := create_test_user(t)
	defer cleanup()

	user.Preferences = map[string]string{"theme": "", "radius": "1rem"}

	declarations := web_user_theme_declarations(user)
	if declarations == "" {
		t.Fatal("web_user_theme_declarations returned nothing, so the test proves nothing")
	}
	if strings.Contains(declarations, `style="`) {
		t.Errorf("declarations should carry no attribute wrapper, got: %s", declarations)
	}
	if strings.Contains(declarations, "&") {
		t.Errorf("declarations are consumed as CSS, so they must not be HTML-escaped, got: %s", declarations)
	}
	if !strings.Contains(declarations, "--radius") {
		t.Errorf("expected the radius preference in the declarations, got: %s", declarations)
	}

	// The attribute form is the same content, escaped and wrapped — one source,
	// two encodings, so the page and the shell can never disagree about the
	// user's theme.
	style := web_user_theme_style(user)
	if !strings.HasPrefix(style, `style="`) || !strings.HasSuffix(style, `"`) {
		t.Errorf("expected an attribute wrapper on the style form, got: %s", style)
	}

	// A user with no preferences at all resolves to the system default theme;
	// whatever that yields, the two forms must still agree.
	empty, cleanup_empty := create_test_user(t)
	defer cleanup_empty()
	empty.Preferences = map[string]string{"theme": ""}
	if web_user_theme_declarations(empty) == "" && web_user_theme_style(empty) != "" {
		t.Error("empty declarations must produce an empty style attribute, not a bare style=\"\"")
	}
}

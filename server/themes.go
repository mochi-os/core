// Mochi server: User theme and appearance rendering
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"fmt"
	"html"
	"regexp"
	"sort"
	"strings"
)

var (
	match_theme_override_key = regexp.MustCompile(`^--[A-Za-z0-9_-]{1,64}$`)
	match_theme_radius       = regexp.MustCompile(`^[0-9]+(\.[0-9]+)?(rem|em|px)$`)
	match_theme_background   = regexp.MustCompile(`^[0-9a-zA-Z#(),.% -]{1,100}$`)
	// Function names that make the browser fetch. A custom property like
	// --background-image is consumed as a real background-image, so a value
	// naming any of these turns a theme into a beacon that reports every
	// page view to a third party. Matching the construct rather than the URL
	// shape is deliberate: a blocklist of URL shapes is bypassable, and
	// url(h\74tp://...), image-set('http://...' 1x), -webkit-image-set(...)
	// and even \75rl(...) were all verified fetching in Chrome against a
	// scheme-matching pattern. No legitimate theme value needs to fetch —
	// gradients, colours, lengths, shadows and font stacks name none of these.
	match_theme_fetch = regexp.MustCompile(`(?i)url|image|src|element|cross-fade|paint`)
)

// themes_validate checks an app manifest's themes and theme_icons blocks.
// Themes cross a trust boundary — any installed app may ship themes the user
// can apply platform-wide — so identifiers, numeric ranges, enums, paths and
// override properties are constrained at load, not at render. Override keys
// are restricted to CSS custom properties by shape, not by allowlist: the
// contract is "any variable theme.css defines", and the --* restriction is
// what stops a theme setting ordinary properties (display, pointer-events)
// on another app's root element.
func themes_validate(av *AppVersion) error {
	if len(av.Themes) > 100 {
		return fmt.Errorf("App has too many themes (%d)", len(av.Themes))
	}
	identifiers := map[string]bool{}
	for _, t := range av.Themes {
		if !valid(t.ID, "constant") {
			return fmt.Errorf("App bad theme id %q", t.ID)
		}
		if identifiers[t.ID] {
			return fmt.Errorf("App duplicate theme id %q", t.ID)
		}
		identifiers[t.ID] = true

		if !valid(t.Label, "constant") {
			return fmt.Errorf("App bad theme label %q", t.Label)
		}

		if t.Hue < 0 || t.Hue > 360 || t.HueBG < 0 || t.HueBG > 360 {
			return fmt.Errorf("App bad theme hue in theme %q", t.ID)
		}
		if t.Chroma < 0 || t.Chroma > 0.5 {
			return fmt.Errorf("App bad theme chroma in theme %q", t.ID)
		}

		if t.BorderRadius != "" && !match_theme_radius.MatchString(t.BorderRadius) {
			return fmt.Errorf("App bad theme border radius %q in theme %q", t.BorderRadius, t.ID)
		}

		switch t.Spacing {
		case "", "compact", "comfortable", "spacious":
		default:
			return fmt.Errorf("App bad theme spacing %q in theme %q", t.Spacing, t.ID)
		}

		for _, font := range []string{t.FontSans, t.FontMono} {
			if font != "" && (len(font) > 300 || strings.ContainsAny(font, `;<>"&`) || !match_non_controls.MatchString(font)) {
				return fmt.Errorf("App bad theme font %q in theme %q", font, t.ID)
			}
		}

		switch t.IconMask {
		case "", "circle", "square", "rounded", "squircle":
		default:
			return fmt.Errorf("App bad theme icon mask %q in theme %q", t.IconMask, t.ID)
		}
		// Same fetch blocklist as the override values below. The charset
		// above was written for colour syntax - # for hex, and (), and %
		// for rgb()/hsl() - and admitting parentheses admits function
		// calls generally: url(evil.example), element(#id), paint(name),
		// cross-fade() and the image-set family all fit it. The class
		// happens to exclude : and /, so no absolute URL can be written,
		// and the one consumer today assigns this to backgroundColor,
		// where the CSSOM drops a non-colour. Neither is a guarantee: a
		// consumer that used the background shorthand, which accepts an
		// <image>, would make every one of them live. The charset already
		// covers the rest of what the override check bans - ; < > " \ &
		// and /* cannot be written with it.
		if t.IconBackground != "" && (!match_theme_background.MatchString(t.IconBackground) ||
			match_theme_fetch.MatchString(t.IconBackground)) {
			return fmt.Errorf("App bad theme icon background %q in theme %q", t.IconBackground, t.ID)
		}

		if len(t.Overrides) > 50 {
			return fmt.Errorf("App has too many theme overrides (%d) in theme %q", len(t.Overrides), t.ID)
		}
		for key, value := range t.Overrides {
			if !match_theme_override_key.MatchString(key) {
				return fmt.Errorf("App bad theme override %q in theme %q", key, t.ID)
			}
			// Backslash, ampersand and comment syntax are refused outright:
			// each lets an identifier be written so it doesn't read as itself,
			// which is how a name-based check is evaded. \75rl( is url( to the
			// CSS tokenizer, and u&#114l( is url( to the HTML parser, which
			// decodes the style attribute before CSS sees it. A value carrying
			// none of them cannot name a function it doesn't spell. The style
			// attribute is escaped on output too (web_user_theme_style) — this
			// is so a manifest that tries fails loudly at load rather than
			// silently rendering something inert.
			if len(value) > 500 || strings.ContainsAny(value, `;<>"\&`) || strings.Contains(value, "/*") ||
				!match_non_controls.MatchString(value) || match_theme_fetch.MatchString(value) {
				return fmt.Errorf("App bad theme override value for %q in theme %q", key, t.ID)
			}
		}

		if len(t.Icons) > 100 {
			return fmt.Errorf("App has too many theme icons (%d) in theme %q", len(t.Icons), t.ID)
		}
		for path, file := range t.Icons {
			if path == "" || !valid(path, "apppath") {
				return fmt.Errorf("App bad theme icon path %q in theme %q", path, t.ID)
			}
			if !valid(file, "filepath") {
				return fmt.Errorf("App bad theme icon file %q in theme %q", file, t.ID)
			}
		}
	}

	if len(av.ThemeIcons) > 100 {
		return fmt.Errorf("App has too many theme icons (%d)", len(av.ThemeIcons))
	}
	for id, file := range av.ThemeIcons {
		parts := strings.SplitN(id, ":", 2)
		if len(parts) != 2 || !valid(parts[0], "constant") || !valid(parts[1], "constant") {
			return fmt.Errorf("App bad theme icon id %q", id)
		}
		if !valid(file, "filepath") {
			return fmt.Errorf("App bad theme icon file %q", file)
		}
	}

	return nil
}

// web_user_appearance_attrs returns the html-tag class attribute and an
// optional <script> that selects dark/light mode according to the user's
// "appearance" preference. The script is empty for explicit light/dark
// preferences and resolves system-prefers-dark when set to "auto".
// If nonce is non-empty it's added as a script nonce attribute (for
// callers serving under a strict CSP, like the shell page).
func web_user_appearance_attrs(user *User, nonce string) (string, string) {
	appearance := user_preference_get(user, "appearance", "auto")
	script_attrs := ""
	if nonce != "" {
		script_attrs = ` nonce="` + nonce + `"`
	}
	switch appearance {
	case "light":
		return `class="light"`, ""
	case "dark":
		return `class="dark"`, ""
	case "auto":
		// The script resolves the scheme before first paint so the page does not
		// flash. The attribute carries the PREFERENCE, which the resolved class
		// alone destroys: a client reading only the class sees "dark" and cannot
		// tell a user who chose dark from one who chose auto on a dark desktop,
		// so it stops following the system for exactly half the users who asked
		// it to.
		return `data-appearance="auto"`, `<script` + script_attrs + `>if(window.matchMedia('(prefers-color-scheme:dark)').matches)document.documentElement.classList.add('dark')</script>`
	default:
		return "", ""
	}
}

func append_radius_variables_from_base(style_parts *[]string, base_radius string) {
	*style_parts = append(*style_parts,
		fmt.Sprintf("--radius: %s", base_radius),
		fmt.Sprintf("--radius-sm: calc(%s - 4px)", base_radius),
		fmt.Sprintf("--radius-md: calc(%s - 2px)", base_radius),
		fmt.Sprintf("--radius-lg: %s", base_radius),
		fmt.Sprintf("--radius-xl: calc(%s + 4px)", base_radius),
	)
}

//lint:ignore U1000 the radius half of the theme preset helpers, alongside the colour ones that are used
func append_radius_preset(style_parts *[]string, preset string) {
	switch preset {
	case "none":
		*style_parts = append(*style_parts,
			"--radius: 0rem",
			"--radius-sm: 0rem",
			"--radius-md: 0rem",
			"--radius-lg: 0rem",
			"--radius-xl: 0rem",
		)
	case "small":
		*style_parts = append(*style_parts,
			"--radius: 0.375rem",
			"--radius-sm: 0.125rem",
			"--radius-md: 0.25rem",
			"--radius-lg: 0.375rem",
			"--radius-xl: 0.625rem",
		)
	case "medium":
		*style_parts = append(*style_parts,
			"--radius: 0.75rem",
			"--radius-sm: 0.5rem",
			"--radius-md: 0.625rem",
			"--radius-lg: 0.75rem",
			"--radius-xl: 1rem",
		)
	case "large":
		*style_parts = append(*style_parts,
			"--radius: 1.75rem",
			"--radius-sm: 1.5rem",
			"--radius-md: 1.625rem",
			"--radius-lg: 1.75rem",
			"--radius-xl: 2rem",
		)
	}
}

// font_stacks returns the (sans, mono) CSS font-family strings for a
// user's font preference. Empty strings mean "no override" — the density
// preset (or theme's font_sans/font_mono) keeps its value.
func font_stacks(pref string) (sans, mono string) {
	switch pref {
	case "system":
		return `-apple-system, BlinkMacSystemFont, 'Segoe UI', 'Helvetica Neue', Arial, sans-serif`,
			`ui-monospace, SFMono-Regular, Menlo, Consolas, monospace`
	case "serif":
		return `Georgia, 'Times New Roman', Cambria, 'Source Serif Pro', serif`, ""
	case "dyslexia":
		// Atkinson Hyperlegible is loaded as a web font in lib/web's
		// theme.css so this stack actually takes effect. OpenDyslexic
		// (preferred by some readers) isn't on Google Fonts and would
		// have to be self-hosted; Comic Sans is a last-resort local
		// fallback some dyslexic readers find legible.
		return `'Atkinson Hyperlegible', 'OpenDyslexic', 'Comic Sans MS', sans-serif`, ""
	}
	return "", ""
}

// style_preset_vars returns every CSS custom property a given density
// preset emits. Density only drives ergonomics (control heights, card
// padding, spacing base) — fonts, shadows, and border width are
// platform-wide defaults so changing density doesn't ripple into
// typography or visual depth. Single source of truth for both
// server-rendered inline styles (web_user_theme_style) and the
// mochi.app.presets() API the client consumes for live updates
// and theme previews.
func style_preset_vars(density string) map[string]string {
	const (
		font_sans    = `-apple-system, BlinkMacSystemFont, 'Segoe UI', 'Helvetica Neue', Arial, sans-serif`
		font_mono    = "'IBM Plex Mono', 'Geist Mono', monospace"
		shadow_color = "rgba(0, 0, 0, 0.1)"
		border_width = "1px"
	)

	var spacing_base string
	switch density {
	case "compact":
		spacing_base = "0.215rem"
	case "spacious":
		spacing_base = "0.285rem"
	default: // comfortable
		spacing_base = "0.27rem"
	}

	vars := map[string]string{
		"--spacing-base": spacing_base,
		"--spacing":      spacing_base,
		"--font-sans":    font_sans,
		"--font-mono":    font_mono,
		"--border-width": border_width,
		"--shadow-color": shadow_color,
		"--shadow-2xs":   fmt.Sprintf("0 1px 2px %s", shadow_color),
		"--shadow-xs":    fmt.Sprintf("0 1px 3px %s", shadow_color),
		"--shadow-sm":    fmt.Sprintf("0 1px 2px %s, 0 2px 6px %s", shadow_color, shadow_color),
		"--shadow":       fmt.Sprintf("0 2px 8px %s, 0 10px 28px %s", shadow_color, shadow_color),
		"--shadow-md":    fmt.Sprintf("0 4px 12px %s, 0 14px 36px %s", shadow_color, shadow_color),
		"--shadow-lg":    fmt.Sprintf("0 8px 20px %s, 0 20px 48px %s", shadow_color, shadow_color),
		"--shadow-xl":    fmt.Sprintf("0 12px 28px %s, 0 28px 56px %s", shadow_color, shadow_color),
		"--shadow-2xl":   fmt.Sprintf("0 16px 34px %s, 0 36px 72px %s", shadow_color, shadow_color),
	}

	switch density {
	case "compact":
		vars["--control-height-xs"] = "1.5rem"
		vars["--control-height-sm"] = "1.75rem"
		vars["--control-height-md"] = "2rem"
		vars["--control-height-lg"] = "2.25rem"
		vars["--input-h"] = "2rem"
		vars["--card-px"] = "1.25rem"
		vars["--card-py"] = "0.875rem"
	case "spacious":
		vars["--control-height-xs"] = "1.875rem"
		vars["--control-height-sm"] = "2.125rem"
		vars["--control-height-md"] = "2.5rem"
		vars["--control-height-lg"] = "2.75rem"
		vars["--input-h"] = "2.5rem"
		vars["--card-px"] = "1.75rem"
		vars["--card-py"] = "1.25rem"
	default: // comfortable
		vars["--control-height-xs"] = "1.75rem"
		vars["--control-height-sm"] = "2rem"
		vars["--control-height-md"] = "2.25rem"
		vars["--control-height-lg"] = "2.5rem"
		vars["--input-h"] = "2.25rem"
		vars["--card-px"] = "1.5rem"
		vars["--card-py"] = "1rem"
	}

	return vars
}

func append_style_preset(style_parts *[]string, density string) {
	vars := style_preset_vars(density)
	keys := make([]string, 0, len(vars))
	for k := range vars {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		*style_parts = append(*style_parts, fmt.Sprintf("%s: %s", k, vars[k]))
	}
}

// web_user_theme_declarations returns the user's resolved theme as CSS
// declaration text ("--hue: 250; --radius: 0.5rem"), unescaped and without an
// attribute wrapper. web_user_theme_style wraps it for HTML output; the shell
// init endpoint returns it as JSON so the shell can refresh the trusted root
// from the server rather than from an app's postMessage.
//
// Honours per-axis user overrides — density, radius, background, font_size —
// each defaulting to "theme" (inherit from the active theme). For anonymous
// requests (user == nil) the per-user overrides resolve to "theme" via
// user_preference_get's nil-guard, and the active theme falls through to the
// system-wide default_theme so login / landing / public-anon pages render
// branded.
func web_user_theme_declarations(user *User) string {
	user_density := user_preference_get(user, "density", "theme")
	user_radius := user_preference_get(user, "radius", "theme")
	user_card := user_preference_get(user, "card", "theme")
	user_background := user_preference_get(user, "background", "theme")
	user_font_size := user_preference_get(user, "font_size", "theme")
	user_font := user_preference_get(user, "font", "theme")

	var t *AppTheme
	if theme_pref := user_preference_get(user, "theme", setting_effective("default_theme")); theme_pref != "" {
		if parts := strings.SplitN(theme_pref, ":", 2); len(parts) == 2 {
			t = app_theme_get(user, parts[0], parts[1])
		}
	}

	style_parts := []string{}

	if t != nil {
		style_parts = append(style_parts,
			fmt.Sprintf("--hue: %g", t.Hue),
			fmt.Sprintf("--hue-chroma: %g", t.Chroma),
			fmt.Sprintf("--hue-bg: %g", t.HueBG),
		)
	}

	// Effective radius: user override wins, else theme's value.
	radius := ""
	if user_radius != "theme" {
		radius = user_radius
	} else if t != nil {
		radius = t.BorderRadius
	}
	if radius != "" && !strings.ContainsAny(radius, `;<>"`) {
		append_radius_variables_from_base(&style_parts, radius)
	}

	// Background: the standard gentle gradient, suppressed when the user opts
	// out. One design for every theme - it tints itself through var(--primary),
	// so it follows the active theme (and appearance) automatically. Replaces
	// the per-theme SVG backgrounds (decision 2026-07-05).
	if user_background != "off" {
		style_parts = append(style_parts,
			"--background-image: radial-gradient(ellipse at top, color-mix(in oklch, var(--primary) 12%, transparent), transparent 70%)",
			"--background-size: 100% 420px",
			"--background-position: top center",
			"--background-repeat: no-repeat")
	}

	if t != nil {
		for key, val := range t.Overrides {
			if strings.HasPrefix(key, "--") && !strings.ContainsAny(key, `;<>"`) && !strings.ContainsAny(val, `;<>"`) {
				style_parts = append(style_parts, fmt.Sprintf("%s: %s", key, val))
			}
		}
	}

	// Card surface treatment: user override wins over the theme's card vars.
	// Appended after the theme overrides above so it takes precedence. Each
	// option sets both axes (border width + shadow) explicitly.
	switch user_card {
	case "flat":
		style_parts = append(style_parts, "--card-border-width: var(--border-width)", "--card-shadow: none")
	case "raised":
		style_parts = append(style_parts, "--card-border-width: var(--border-width)", "--card-shadow: var(--shadow-sm)")
	}

	// Effective density: user override wins, else theme's spacing.
	density := ""
	if user_density != "theme" {
		density = user_density
	} else if t != nil {
		density = t.Spacing
	}
	if density != "" {
		append_style_preset(&style_parts, density)
	}

	// Font family overrides: theme's font_sans/font_mono override the
	// density preset's choice, then the user's font preference (if any)
	// overrides both. Density preset goes first so font specifications
	// here win.
	if t != nil {
		if t.FontSans != "" && !strings.ContainsAny(t.FontSans, `;<>"`) {
			style_parts = append(style_parts, fmt.Sprintf("--font-sans: %s", t.FontSans))
		}
		if t.FontMono != "" && !strings.ContainsAny(t.FontMono, `;<>"`) {
			style_parts = append(style_parts, fmt.Sprintf("--font-mono: %s", t.FontMono))
		}
	}
	if font_sans, font_mono := font_stacks(user_font); font_sans != "" {
		style_parts = append(style_parts, fmt.Sprintf("--font-sans: %s", font_sans))
		if font_mono != "" {
			style_parts = append(style_parts, fmt.Sprintf("--font-mono: %s", font_mono))
		}
	}

	// Font size scales the html root, so all rem-based sizing follows.
	switch user_font_size {
	case "small":
		style_parts = append(style_parts, "font-size: 87.5%")
	case "normal":
		style_parts = append(style_parts, "font-size: 100%")
	case "large":
		style_parts = append(style_parts, "font-size: 112.5%")
	case "extra-large":
		style_parts = append(style_parts, "font-size: 125%")
	}

	return strings.Join(style_parts, "; ")
}

// web_user_theme_style returns web_user_theme_declarations as an inline
// style="..." attribute for injection into an HTML document.
func web_user_theme_style(user *User) string {
	declarations := web_user_theme_declarations(user)
	if declarations == "" {
		return ""
	}
	// The attribute goes into HTML, and the parser decodes character
	// references before CSS ever parses the value — so a theme value carrying
	// `u&#114l(` or `red&#59;background-image:url(` reconstructs a fetching
	// function or a whole extra declaration on <html> that none of the checks
	// in web_user_theme_declarations, or the manifest validation, ever saw as
	// such. Escaping at the point of output is the boundary that holds
	// whatever reached here, and it costs nothing in fidelity: the parser
	// decodes each reference back to the character the theme meant, so the
	// quotes in a font stack still arrive as quotes.
	return `style="` + html.EscapeString(declarations) + `"`
}

// web_apply_user_document_theme injects user appearance/theme into a full
// HTML document — used when serving raw app HTML directly (no shell wrapper).
// Anonymous (user == nil) requests fall through to the system default_theme
// and "auto" appearance, so login / landing / public-anon pages render
// branded.
func web_apply_user_document_theme(content string, user *User) string {
	html_class, appearance_script := web_user_appearance_attrs(user, "")
	content = web_add_html_attribute(content, html_class)
	content = web_add_html_attribute(content, web_user_theme_style(user))
	if appearance_script != "" {
		content = strings.Replace(content, "<head>", "<head>"+appearance_script, 1)
	}
	return content
}

// web_add_html_attribute injects a class="..." or style="..." attribute into the
// first <html> tag. If the tag already carries the same attribute name the
// values are merged (space-joined for class, semicolon-joined for style)
// instead of creating an invalid duplicate attribute.
func web_add_html_attribute(content, attribute string) string {
	if attribute == "" {
		return content
	}

	start := strings.Index(content, "<html")
	if start == -1 {
		return content
	}
	end := strings.Index(content[start:], ">")
	if end == -1 {
		return content
	}
	end += start
	tag := content[start:end]

	// Extract the attribute name and value from the incoming attribute (e.g. class="dark")
	eq := strings.Index(attribute, "=")
	if eq == -1 {
		// Plain attribute without value — just append
		return content[:end] + " " + attribute + content[end:]
	}
	name := attribute[:eq]                     // "class" or "style"
	val := strings.Trim(attribute[eq+1:], `"`) // the value without quotes

	// Check if the <html> tag already has this attribute
	needle := name + `="`
	position := strings.Index(tag, needle)
	if position == -1 {
		// Attribute doesn't exist yet — append it
		return content[:end] + " " + attribute + content[end:]
	}

	// Find the closing quote of the existing attribute value
	val_start := start + position + len(needle)
	val_end := strings.Index(content[val_start:], `"`)
	if val_end == -1 {
		return content[:end] + " " + attribute + content[end:]
	}
	val_end += val_start

	// Merge: space for class, semicolon for style
	sep := " "
	if name == "style" {
		sep = "; "
	}
	existing := content[val_start:val_end]
	if existing == "" {
		existing = val
	} else {
		existing = existing + sep + val
	}
	return content[:val_start] + existing + content[val_end:]
}

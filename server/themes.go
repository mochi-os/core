// Mochi server: User theme and appearance rendering
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"fmt"
	"sort"
	"strings"
)

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
		return "", `<script` + script_attrs + `>if(window.matchMedia('(prefers-color-scheme:dark)').matches)document.documentElement.classList.add('dark')</script>`
	default:
		return "", ""
	}
}

func append_radius_variables_from_base(style_parts *[]string, baseRadius string) {
	*style_parts = append(*style_parts,
		fmt.Sprintf("--radius: %s", baseRadius),
		fmt.Sprintf("--radius-sm: calc(%s - 4px)", baseRadius),
		fmt.Sprintf("--radius-md: calc(%s - 2px)", baseRadius),
		fmt.Sprintf("--radius-lg: %s", baseRadius),
		fmt.Sprintf("--radius-xl: calc(%s + 4px)", baseRadius),
	)
}

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
		fontSans    = `-apple-system, BlinkMacSystemFont, 'Segoe UI', 'Helvetica Neue', Arial, sans-serif`
		fontMono    = "'IBM Plex Mono', 'Geist Mono', monospace"
		shadowColor = "rgba(0, 0, 0, 0.1)"
		borderWidth = "1px"
	)

	var spacingBase string
	switch density {
	case "compact":
		spacingBase = "0.215rem"
	case "spacious":
		spacingBase = "0.285rem"
	default: // comfortable
		spacingBase = "0.27rem"
	}

	vars := map[string]string{
		"--spacing-base": spacingBase,
		"--spacing":      spacingBase,
		"--font-sans":    fontSans,
		"--font-mono":    fontMono,
		"--border-width": borderWidth,
		"--shadow-color": shadowColor,
		"--shadow-2xs":   fmt.Sprintf("0 1px 2px %s", shadowColor),
		"--shadow-xs":    fmt.Sprintf("0 1px 3px %s", shadowColor),
		"--shadow-sm":    fmt.Sprintf("0 1px 2px %s, 0 2px 6px %s", shadowColor, shadowColor),
		"--shadow":       fmt.Sprintf("0 2px 8px %s, 0 10px 28px %s", shadowColor, shadowColor),
		"--shadow-md":    fmt.Sprintf("0 4px 12px %s, 0 14px 36px %s", shadowColor, shadowColor),
		"--shadow-lg":    fmt.Sprintf("0 8px 20px %s, 0 20px 48px %s", shadowColor, shadowColor),
		"--shadow-xl":    fmt.Sprintf("0 12px 28px %s, 0 28px 56px %s", shadowColor, shadowColor),
		"--shadow-2xl":   fmt.Sprintf("0 16px 34px %s, 0 36px 72px %s", shadowColor, shadowColor),
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

// web_user_theme_style returns an inline style="..." attribute carrying
// the user's resolved theme as CSS custom properties (hue, radius, fonts,
// shadows, density). Honours per-axis user overrides — density, radius,
// background, font_size — each defaulting to "theme" (inherit from the
// active theme). For anonymous requests (user == nil) the per-user
// overrides resolve to "theme" via user_preference_get's nil-guard, and
// the active theme falls through to the system-wide default_theme so
// login / landing / public-anon pages render branded.
func web_user_theme_style(user *User) string {
	user_density := user_preference_get(user, "density", "theme")
	user_radius := user_preference_get(user, "radius", "theme")
	user_card := user_preference_get(user, "card", "theme")
	user_background := user_preference_get(user, "background", "theme")
	user_font_size := user_preference_get(user, "font_size", "theme")
	user_font := user_preference_get(user, "font", "theme")

	var t *AppTheme
	var theme_app_id string
	if theme_pref := user_preference_get(user, "theme", setting_get("default_theme", system_settings["default_theme"].Default)); theme_pref != "" {
		if parts := strings.SplitN(theme_pref, ":", 2); len(parts) == 2 {
			t = app_theme_get(user, parts[0], parts[1])
			theme_app_id = parts[0]
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

	// Background image, suppressed when the user opts out.
	if t != nil && user_background != "off" && t.Background != "" && theme_app_id != "" {
		apps_lock.Lock()
		a := apps[theme_app_id]
		apps_lock.Unlock()
		if a != nil {
			av := a.active(user)
			if av != nil && len(av.Paths) > 0 {
				base := av.Paths[0]
				if !strings.ContainsAny(t.Background, `<>"`) {
					style_parts = append(style_parts, fmt.Sprintf("--background-image: url(/%s/backgrounds/%s)", base, t.Background))
				}
				if t.BackgroundDark != "" && !strings.ContainsAny(t.BackgroundDark, `<>"`) {
					style_parts = append(style_parts, fmt.Sprintf("--background-image-dark: url(/%s/backgrounds/%s)", base, t.BackgroundDark))
				}
			}
		}
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

	if len(style_parts) == 0 {
		return ""
	}
	return `style="` + strings.Join(style_parts, "; ") + `"`
}

// web_apply_user_document_theme injects user appearance/theme into a full
// HTML document — used when serving raw app HTML directly (no shell wrapper).
// Anonymous (user == nil) requests fall through to the system default_theme
// and "auto" appearance, so login / landing / public-anon pages render
// branded.
func web_apply_user_document_theme(content string, user *User) string {
	html_class, appearance_script := web_user_appearance_attrs(user, "")
	content = web_add_html_attr(content, html_class)
	content = web_add_html_attr(content, web_user_theme_style(user))
	if appearance_script != "" {
		content = strings.Replace(content, "<head>", "<head>"+appearance_script, 1)
	}
	return content
}

// web_add_html_attr injects a class="..." or style="..." attribute into the
// first <html> tag. If the tag already carries the same attribute name the
// values are merged (space-joined for class, semicolon-joined for style)
// instead of creating an invalid duplicate attribute.
func web_add_html_attr(content, attr string) string {
	if attr == "" {
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

	// Extract the attribute name and value from the incoming attr (e.g. class="dark")
	eq := strings.Index(attr, "=")
	if eq == -1 {
		// Plain attribute without value — just append
		return content[:end] + " " + attr + content[end:]
	}
	name := attr[:eq]                     // "class" or "style"
	val := strings.Trim(attr[eq+1:], `"`) // the value without quotes

	// Check if the <html> tag already has this attribute
	needle := name + `="`
	pos := strings.Index(tag, needle)
	if pos == -1 {
		// Attribute doesn't exist yet — append it
		return content[:end] + " " + attr + content[end:]
	}

	// Find the closing quote of the existing attribute value
	val_start := start + pos + len(needle)
	val_end := strings.Index(content[val_start:], `"`)
	if val_end == -1 {
		return content[:end] + " " + attr + content[end:]
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

// Mochi server: User theme and appearance rendering
// Copyright Alistair Cunningham 2026

package main

import (
	"fmt"
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

func appendRadiusVarsFromBase(styleParts *[]string, baseRadius string) {
	*styleParts = append(*styleParts,
		fmt.Sprintf("--radius: %s", baseRadius),
		fmt.Sprintf("--radius-sm: calc(%s - 4px)", baseRadius),
		fmt.Sprintf("--radius-md: calc(%s - 2px)", baseRadius),
		fmt.Sprintf("--radius-lg: %s", baseRadius),
		fmt.Sprintf("--radius-xl: calc(%s + 4px)", baseRadius),
	)
}

func appendRadiusPreset(styleParts *[]string, preset string) {
	switch preset {
	case "none":
		*styleParts = append(*styleParts,
			"--radius: 0rem",
			"--radius-sm: 0rem",
			"--radius-md: 0rem",
			"--radius-lg: 0rem",
			"--radius-xl: 0rem",
		)
	case "small":
		*styleParts = append(*styleParts,
			"--radius: 0.375rem",
			"--radius-sm: 0.125rem",
			"--radius-md: 0.25rem",
			"--radius-lg: 0.375rem",
			"--radius-xl: 0.625rem",
		)
	case "medium":
		*styleParts = append(*styleParts,
			"--radius: 0.75rem",
			"--radius-sm: 0.5rem",
			"--radius-md: 0.625rem",
			"--radius-lg: 0.75rem",
			"--radius-xl: 1rem",
		)
	case "large":
		*styleParts = append(*styleParts,
			"--radius: 1.75rem",
			"--radius-sm: 1.5rem",
			"--radius-md: 1.625rem",
			"--radius-lg: 1.75rem",
			"--radius-xl: 2rem",
		)
	}
}

func appendDensityPreset(styleParts *[]string, preset string) {
	switch preset {
	case "compact":
		*styleParts = append(*styleParts,
			"--control-height-xs: 1.5rem",
			"--control-height-sm: 1.75rem",
			"--control-height-md: 2rem",
			"--control-height-lg: 2.25rem",
			"--input-h: 2rem",
			"--card-px: 1.25rem",
			"--card-py: 0.875rem",
		)
	case "spacious":
		*styleParts = append(*styleParts,
			"--control-height-xs: 1.875rem",
			"--control-height-sm: 2.125rem",
			"--control-height-md: 2.5rem",
			"--control-height-lg: 2.75rem",
			"--input-h: 2.5rem",
			"--card-px: 1.75rem",
			"--card-py: 1.25rem",
		)
	default:
		*styleParts = append(*styleParts,
			"--control-height-xs: 1.75rem",
			"--control-height-sm: 2rem",
			"--control-height-md: 2.25rem",
			"--control-height-lg: 2.5rem",
			"--input-h: 2.25rem",
			"--card-px: 1.5rem",
			"--card-py: 1rem",
		)
	}
}

func appendStylePreset(styleParts *[]string, preset string) {
	appendPreset := func(spacingBase, fontSans, fontMono, shadowColor, density, borderWidth string) {
		*styleParts = append(*styleParts,
			fmt.Sprintf("--spacing-base: %s", spacingBase),
			fmt.Sprintf("--spacing: %s", spacingBase),
			fmt.Sprintf("--font-sans: %s", fontSans),
			fmt.Sprintf("--font-mono: %s", fontMono),
			fmt.Sprintf("--border-width: %s", borderWidth),
			fmt.Sprintf("--shadow-color: %s", shadowColor),
			fmt.Sprintf("--shadow-2xs: 0 1px 2px %s", shadowColor),
			fmt.Sprintf("--shadow-xs: 0 1px 3px %s", shadowColor),
			fmt.Sprintf("--shadow-sm: 0 1px 2px %s, 0 2px 6px %s", shadowColor, shadowColor),
			fmt.Sprintf("--shadow: 0 2px 8px %s, 0 10px 28px %s", shadowColor, shadowColor),
			fmt.Sprintf("--shadow-md: 0 4px 12px %s, 0 14px 36px %s", shadowColor, shadowColor),
			fmt.Sprintf("--shadow-lg: 0 8px 20px %s, 0 20px 48px %s", shadowColor, shadowColor),
			fmt.Sprintf("--shadow-xl: 0 12px 28px %s, 0 28px 56px %s", shadowColor, shadowColor),
			fmt.Sprintf("--shadow-2xl: 0 16px 34px %s, 0 36px 72px %s", shadowColor, shadowColor),
		)
		appendDensityPreset(styleParts, density)
	}

	switch preset {
	case "default", "maia":
		appendPreset(
			"0.3rem",
			"'Nunito Sans', 'Inter', sans-serif",
			"'Fira Code', 'Geist Mono', monospace",
			"rgba(0, 0, 0, 0.12)",
			"spacious",
			"1px",
		)
	case "vega":
		appendPreset(
			"0.215rem",
			"'Public Sans', 'Inter', sans-serif",
			"'IBM Plex Mono', 'Geist Mono', monospace",
			"rgba(0, 0, 0, 0.17)",
			"compact",
			"1px",
		)
	case "nova":
		appendPreset(
			"0.255rem",
			"'Poppins', 'Inter', sans-serif",
			"'JetBrains Mono', 'Geist Mono', monospace",
			"rgba(0, 0, 0, 0.18)",
			"comfortable",
			"1.25px",
		)
	case "lyra":
		appendPreset(
			"0.235rem",
			"'Inter Tight', 'Inter', sans-serif",
			"'JetBrains Mono', 'Geist Mono', monospace",
			"rgba(0, 0, 0, 0.22)",
			"compact",
			"1.5px",
		)
	case "mira":
		appendPreset(
			"0.285rem",
			"'DM Sans', 'Inter', sans-serif",
			"'Space Mono', 'Geist Mono', monospace",
			"rgba(0, 0, 0, 0.14)",
			"spacious",
			"1.25px",
		)
	case "luma":
		appendPreset(
			"0.27rem",
			"'Manrope', 'Inter', sans-serif",
			"'IBM Plex Mono', 'Geist Mono', monospace",
			"rgba(0, 0, 0, 0.1)",
			"comfortable",
			"1px",
		)
	}
}

// web_user_theme_style returns an inline style="..." attribute carrying
// the user's resolved theme as CSS custom properties (hue, radius, fonts,
// shadows, density). Honours per-axis user overrides — density, radius,
// background, font_size — each defaulting to "theme" (inherit from the
// active theme). Returns empty string when nothing is configured.
func web_user_theme_style(user *User) string {
	if user == nil {
		return ""
	}

	user_density := user_preference_get(user, "density", "theme")
	user_radius := user_preference_get(user, "radius", "theme")
	user_background := user_preference_get(user, "background", "theme")
	user_font_size := user_preference_get(user, "font_size", "theme")

	var t *AppTheme
	var theme_app_id string
	if theme_pref := user_preference_get(user, "theme", setting_get("default_theme", "")); theme_pref != "" {
		if parts := strings.SplitN(theme_pref, ":", 2); len(parts) == 2 {
			t = app_theme_get(user, parts[0], parts[1])
			theme_app_id = parts[0]
		}
	}

	styleParts := []string{}

	if t != nil {
		styleParts = append(styleParts,
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
		appendRadiusVarsFromBase(&styleParts, radius)
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
					styleParts = append(styleParts, fmt.Sprintf("--background-image: url(/%s/backgrounds/%s)", base, t.Background))
				}
				if t.BackgroundDark != "" && !strings.ContainsAny(t.BackgroundDark, `<>"`) {
					styleParts = append(styleParts, fmt.Sprintf("--background-image-dark: url(/%s/backgrounds/%s)", base, t.BackgroundDark))
				}
			}
		}
	}

	if t != nil {
		for key, val := range t.Overrides {
			if strings.HasPrefix(key, "--") && !strings.ContainsAny(key, `;<>"`) && !strings.ContainsAny(val, `;<>"`) {
				styleParts = append(styleParts, fmt.Sprintf("%s: %s", key, val))
			}
		}
	}

	// Effective density: user override wins, else theme's spacing.
	density := ""
	if user_density != "theme" {
		density = user_density
	} else if t != nil {
		density = t.Spacing
	}
	if density != "" {
		switch density {
		case "compact":
			appendStylePreset(&styleParts, "vega")
		case "spacious":
			appendStylePreset(&styleParts, "mira")
		default:
			appendStylePreset(&styleParts, "luma")
		}
	}

	// Font size scales the html root, so all rem-based sizing follows.
	switch user_font_size {
	case "small":
		styleParts = append(styleParts, "font-size: 87.5%")
	case "normal":
		styleParts = append(styleParts, "font-size: 100%")
	case "large":
		styleParts = append(styleParts, "font-size: 112.5%")
	case "extra-large":
		styleParts = append(styleParts, "font-size: 125%")
	}

	if len(styleParts) == 0 {
		return ""
	}
	return `style="` + strings.Join(styleParts, "; ") + `"`
}

// web_apply_user_document_theme injects user appearance/theme into a full
// HTML document — used when serving raw app HTML directly (no shell wrapper).
func web_apply_user_document_theme(content string, user *User) string {
	if user == nil {
		return content
	}

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

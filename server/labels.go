// Mochi server: Label resolution and ICU MessageFormat
// Copyright Alistair Cunningham 2026

package main

import (
	"embed"
	"io/fs"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/gotnospirit/messageformat"
	"gopkg.in/ini.v1"
)

// core_labels_fs embeds core server's own translatable strings. Compiled
// into the binary so the labels travel with the executable.
//
//go:embed labels/*.conf
var core_labels_fs embed.FS

// core_labels holds core server's own translatable strings, keyed by
// BCP 47 tag. Loaded once at startup from the embedded labels FS. Used by
// respond_error() for HTTP error responses returned by core itself.
var core_labels = map[string]map[string]string{}

// English variants that don't follow the default `<lang> -> en` fallback.
// Most Commonwealth English speakers (en-gb, en-au, en-nz, en-ie, en-za, en-in,
// en-sg, en-hk, en-ca) fall back directly to `en` because the source `en`
// catalog is already Commonwealth-flavoured. Only en-PH historically follows US
// conventions and so routes through en-us first.
var english_variants = map[string]string{
	"en-ph": "en-us",
}

// language_fallbacks returns the resolution chain for a BCP 47 language tag.
// `en` is always the final fallback. Intermediate parents (e.g. `zh` between
// `zh-hant` and `en`) are returned even if their catalog isn't installed; the
// resolver skips uninstalled entries at lookup time.
//
// Examples:
//
//	"en-us"      -> ["en-us", "en"]
//	"en-gb"      -> ["en-gb", "en"]
//	"en-ph"      -> ["en-ph", "en-us", "en"]
//	"zh-hant-hk" -> ["zh-hant-hk", "zh-hant", "zh", "en"]
//	"pt-br"      -> ["pt-br", "pt", "en"]
//	"fr"         -> ["fr", "en"]
//	"en"         -> ["en"]
func language_fallbacks(lang string) []string {
	lang = strings.ToLower(strings.TrimSpace(lang))
	if lang == "" || lang == "en" {
		return []string{"en"}
	}

	chain := []string{lang}

	if redirect, ok := english_variants[lang]; ok {
		// Explicit redirect (e.g. en-ph -> en-us). Walk the redirect's parents.
		chain = append(chain, redirect)
		for parent := strip_subtag(redirect); parent != "" && parent != "en"; parent = strip_subtag(parent) {
			chain = append(chain, parent)
		}
	} else {
		// Generic: strip subtags one by one until we reach the bare language.
		for parent := strip_subtag(lang); parent != "" && parent != "en"; parent = strip_subtag(parent) {
			chain = append(chain, parent)
		}
	}

	chain = append(chain, "en")
	return chain
}

// strip_subtag removes the final hyphen-separated subtag.
// "zh-hant-hk" -> "zh-hant" -> "zh" -> ""
func strip_subtag(lang string) string {
	i := strings.LastIndex(lang, "-")
	if i < 0 {
		return ""
	}
	return lang[:i]
}

// plural_locale extracts the language portion of a BCP 47 tag for plural-rule
// lookup. The makeplural library is keyed by 2-3 letter language codes only,
// so "en-gb" becomes "en", "zh-hant" becomes "zh", etc.
func plural_locale(lang string) string {
	lang = strings.ToLower(lang)
	if i := strings.Index(lang, "-"); i > 0 {
		return lang[:i]
	}
	return lang
}

// format_message applies ICU MessageFormat substitution to a label format
// string. If args is nil/empty or the format has no placeholders, returns the
// format unchanged. Errors are logged and the unformatted format string is
// returned (so a broken label degrades gracefully to source text).
func format_message(format, locale string, args map[string]any) string {
	if format == "" || len(args) == 0 {
		return format
	}

	parser, err := messageformat.NewWithCulture(plural_locale(locale))
	if err != nil {
		// Locale not in makeplural's table; fall back to English plural rules.
		parser, err = messageformat.New()
		if err != nil {
			info("MessageFormat init failed: %v", err)
			return format
		}
	}

	mf, err := parser.Parse(format)
	if err != nil {
		info("MessageFormat parse failed for %q: %v", format, err)
		return format
	}

	result, err := mf.FormatMap(normalize_args(args))
	if err != nil {
		info("MessageFormat format failed for %q: %v", format, err)
		return format
	}
	return result
}

// normalize_args coerces numeric types into the int/float64/string trio that
// the messageformat library accepts for plural and select keys. int64 values
// from Starlark and other Go callers are converted to int when they fit.
func normalize_args(args map[string]any) map[string]any {
	out := make(map[string]any, len(args))
	for k, v := range args {
		switch n := v.(type) {
		case int8:
			out[k] = int(n)
		case int16:
			out[k] = int(n)
		case int32:
			out[k] = int(n)
		case int64:
			out[k] = int(n)
		case uint:
			out[k] = int(n)
		case uint8:
			out[k] = int(n)
		case uint16:
			out[k] = int(n)
		case uint32:
			out[k] = int(n)
		case uint64:
			out[k] = int(n)
		case float32:
			out[k] = float64(n)
		default:
			out[k] = v
		}
	}
	return out
}

// parse_accept_language parses an HTTP Accept-Language header into a list of
// BCP 47 language tags ordered by preference (highest q first). Tags are
// lowercased; the wildcard "*" is dropped. Returns nil for an empty header.
func parse_accept_language(header string) []string {
	if header == "" {
		return nil
	}
	type entry struct {
		tag string
		q   float64
	}
	var entries []entry
	for _, raw := range strings.Split(header, ",") {
		parts := strings.SplitN(strings.TrimSpace(raw), ";", 2)
		tag := strings.ToLower(strings.TrimSpace(parts[0]))
		if tag == "" || tag == "*" {
			continue
		}
		q := 1.0
		if len(parts) > 1 {
			qstr := strings.TrimSpace(parts[1])
			if strings.HasPrefix(qstr, "q=") {
				if v, err := strconv.ParseFloat(qstr[2:], 64); err == nil {
					q = v
				}
			}
		}
		entries = append(entries, entry{tag, q})
	}
	sort.SliceStable(entries, func(i, j int) bool {
		return entries[i].q > entries[j].q
	})
	out := make([]string, len(entries))
	for i, e := range entries {
		out[i] = e.tag
	}
	return out
}

// request_language resolves the language for a request: the user's preference
// if logged in, else the best-priority Accept-Language tag, else "en". The
// returned tag flows into the label resolver where the fallback chain handles
// catalog-not-installed cases automatically.
func request_language(c *gin.Context, u *User) string {
	if u != nil {
		if lang := user_preference_get(u, "language", ""); lang != "" {
			return strings.ToLower(lang)
		}
	}
	if c != nil {
		tags := parse_accept_language(c.GetHeader("Accept-Language"))
		if len(tags) > 0 {
			return tags[0]
		}
	}
	return "en"
}

// installed_languages returns the union of BCP 47 tags across every loaded
// app version's labels map. Used by the picker UI to populate the language
// list. Locked-down read of apps_lock; callers tolerate transient stale
// results during hot-reload.
func installed_languages() []string {
	seen := map[string]struct{}{}
	apps_lock.Lock()
	for _, a := range apps {
		for _, av := range a.versions {
			for tag := range av.labels {
				seen[tag] = struct{}{}
			}
		}
	}
	apps_lock.Unlock()
	out := make([]string, 0, len(seen))
	for tag := range seen {
		out = append(out, tag)
	}
	sort.Strings(out)
	return out
}

// web_languages handles GET /_/languages — returns the union of installed
// language tags across all apps. Public; no auth required so anonymous and
// pre-login pages can populate their pickers.
func web_languages(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"languages": installed_languages()})
}

// web_serve_labels handles the built-in /<app>/-/labels and
// /<app>/-/labels/<tag> endpoints. With no tag, returns a sorted list of
// installed language tags for the app. With a tag, returns the {key: format}
// map for that catalog. Used by tooling (Translate Mochi, dev introspection)
// rather than the web SPA.
func web_serve_labels(c *gin.Context, av *AppVersion, suffix string) bool {
	if av == nil {
		respond_error(c, http.StatusNotFound, "labels_not_loaded", "errors.labels_not_loaded", nil)
		return true
	}
	suffix = strings.TrimPrefix(suffix, "/")
	if suffix == "" {
		out := make([]string, 0, len(av.labels))
		for tag := range av.labels {
			out = append(out, tag)
		}
		sort.Strings(out)
		c.JSON(http.StatusOK, gin.H{"languages": out})
		return true
	}
	tag := strings.ToLower(suffix)
	if !valid(tag, "locale") {
		respond_error(c, http.StatusBadRequest, "invalid_language_tag", "errors.invalid_language_tag", nil)
		return true
	}
	labels := av.labels[tag]
	if labels == nil {
		respond_error(c, http.StatusNotFound, "language_not_installed", "errors.language_not_installed", nil)
		return true
	}
	c.JSON(http.StatusOK, labels)
	return true
}

// resolve_label walks the fallback chain and returns the first matching label,
// substituted with args via MessageFormat. Returns the literal key if nothing
// resolves (developer bug — log it).
func resolve_label(av *AppVersion, language, key string, args map[string]any) string {
	if av == nil || av.labels == nil {
		return key
	}

	for _, tag := range language_fallbacks(language) {
		labels := av.labels[tag]
		if labels == nil {
			continue
		}
		format := labels[key]
		if format == "" {
			continue
		}
		return format_message(format, tag, args)
	}

	info("App label %q in language %q not set", key, language)
	return key
}

// resolve_core_label walks the fallback chain across core_labels and returns
// the first matching label, substituted with args. Returns the literal key
// if nothing resolves.
func resolve_core_label(language, key string, args map[string]any) string {
	for _, tag := range language_fallbacks(language) {
		labels := core_labels[tag]
		if labels == nil {
			continue
		}
		format := labels[key]
		if format == "" {
			continue
		}
		return format_message(format, tag, args)
	}
	info("Core label %q in language %q not set", key, language)
	return key
}

// load_core_labels populates the core_labels map from the embedded labels FS.
// Called once at startup.
func load_core_labels() {
	entries, err := fs.ReadDir(core_labels_fs, "labels")
	if err != nil {
		// No labels embedded — core falls back to literal keys.
		return
	}
	for _, ent := range entries {
		if ent.IsDir() || !strings.HasSuffix(ent.Name(), ".conf") {
			continue
		}
		tag := strings.ToLower(strings.TrimSuffix(ent.Name(), ".conf"))
		if !valid(tag, "locale") {
			info("Core labels: skipping %q (invalid locale tag)", ent.Name())
			continue
		}
		data, err := fs.ReadFile(core_labels_fs, "labels/"+ent.Name())
		if err != nil {
			info("Core labels: cannot read %q: %v", ent.Name(), err)
			continue
		}
		cfg, err := ini.Load(data)
		if err != nil {
			info("Core labels: cannot parse %q: %v", ent.Name(), err)
			continue
		}
		section := cfg.Section("labels")
		if section == nil {
			continue
		}
		entry := map[string]string{}
		for _, key := range section.KeyStrings() {
			entry[key] = section.Key(key).Value()
		}
		core_labels[tag] = entry
	}
}

// respond_error sends an HTTP error response with a translated `message` field
// and a stable machine-readable `code` field. The label key resolves through
// the user's language preference (or Accept-Language for anonymous requests),
// with the standard fallback chain.
//
//	respond_error(c, 404, "not_found", "errors.not_found", nil)
//	respond_error(c, 400, "invalid_email", "errors.invalid_email",
//	    map[string]any{"email": email})
func respond_error(c *gin.Context, status int, code, key string, args map[string]any) {
	lang := request_language(c, nil)
	msg := resolve_core_label(lang, key, args)
	c.JSON(status, gin.H{"error": code, "message": msg})
	c.Abort()
}

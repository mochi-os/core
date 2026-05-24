// Mochi server: Documents
// Copyright Alistair Cunningham 2026

// Operator-customisable Markdown documents (server rules, terms and
// conditions, privacy notice). Bundled defaults live in core/server/documents/
// and are embedded into the binary; operator overrides are stored in
// db/settings.db.documents keyed by (name, language). The resolver applies
// a four-step fallback chain (override → bundled, language → en) and
// renders {{operator.*}} and {{server.host}} placeholders at read time.

package main

import (
	"embed"
	"fmt"
	"sort"
	"strings"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

//go:embed documents/*.md
var documents_fs embed.FS

// document_names is the v1 allowlist of valid document names.
var document_names = []string{"rules", "terms", "privacy"}

// document_get returns the body that would be served for (name, language).
// Applies the four-step fallback chain and template interpolation.
func document_get(name, language string) string {
	if !document_name_valid(name) {
		return ""
	}
	body := document_lookup(name, language)
	if body == "" {
		return ""
	}
	return document_render(body, language)
}

// document_name_valid checks whether name is one of the allowlisted documents.
func document_name_valid(name string) bool {
	for _, n := range document_names {
		if n == name {
			return true
		}
	}
	return false
}

// document_lookup returns the raw (un-rendered) body via the four-step
// fallback chain: operator override in language, bundled default in language,
// operator override in en, bundled default in en. Returns "" if nothing is
// found (which shouldn't happen — bundled en is always shipped).
func document_lookup(name, language string) string {
	if language != "" && language != "en" {
		if body := document_override(name, language); body != "" {
			return body
		}
		if body := document_bundled(name, language); body != "" {
			return body
		}
	}
	if body := document_override(name, "en"); body != "" {
		return body
	}
	return document_bundled(name, "en")
}

// document_override reads an operator override from db/settings.db. Returns
// "" if no row exists for (name, language).
func document_override(name, language string) string {
	db := db_open("db/settings.db")
	row, err := db.row("select body from documents where name=? and language=?", name, language)
	if err != nil || row == nil {
		return ""
	}
	if v, ok := row["body"].(string); ok {
		return v
	}
	return ""
}

// document_bundled reads the embedded default for (name, language). Returns
// "" if no file exists for that pair.
func document_bundled(name, language string) string {
	if language == "" {
		language = "en"
	}
	data, err := documents_fs.ReadFile("documents/" + name + "." + language + ".md")
	if err != nil {
		return ""
	}
	return string(data)
}

// document_render performs literal placeholder substitution. Recognised
// placeholders are {{operator.name}}, {{operator.email}}, and
// {{operator.jurisdiction}}, all from system settings. Empty operator
// settings render as the localised `document.not_configured` core label
// (e.g. "[not configured]" in en, "[non configuré]" in fr) so it is
// visually obvious they have not been filled in. We use literal string
// replacement rather than text/template so the placeholder syntax in the
// markdown files is exactly what operators see and edit.
func document_render(body, language string) string {
	replacements := []struct {
		placeholder string
		value       string
	}{
		{"{{operator.name}}", document_setting("operator_name", language)},
		{"{{operator.email}}", document_setting("operator_email", language)},
		{"{{operator.jurisdiction}}", document_setting("operator_jurisdiction", language)},
	}
	for _, r := range replacements {
		body = strings.ReplaceAll(body, r.placeholder, r.value)
	}
	return body
}

// document_setting reads an operator-info setting and returns the
// localised "not configured" sentinel if empty.
func document_setting(name, language string) string {
	v := setting_get(name, "")
	if v == "" {
		return resolve_core_label(language, "document.not_configured", nil)
	}
	return v
}

// document_set writes an operator override into the documents table.
// Replicates to operator-paired hosts so a terms / rules / privacy
// edit on one side reaches the others; LWW per (name, language).
func document_set(name, language, body string) error {
	if !document_name_valid(name) {
		return fmt.Errorf("unknown document name %q", name)
	}
	if language == "" {
		return fmt.Errorf("language required")
	}
	updated := now()
	db := db_open("db/settings.db")
	db.exec("replace into documents ( name, language, body, updated ) values ( ?, ?, ?, ? )",
		name, language, body, updated)
	replication_emit_system_row("settings", "documents",
		map[string]string{"name": name, "language": language},
		map[string]string{"body": body, "updated": fmt.Sprintf("%d", updated)},
		false)
	return nil
}

// document_languages returns the sorted list of languages we ship bundled
// defaults for. Derived from the embedded FS (filenames are
// `<name>.<language>.md`).
func document_languages() []string {
	entries, err := documents_fs.ReadDir("documents")
	if err != nil {
		return nil
	}
	seen := map[string]bool{}
	for _, e := range entries {
		parts := strings.Split(e.Name(), ".")
		if len(parts) != 3 || parts[2] != "md" {
			continue
		}
		seen[parts[1]] = true
	}
	out := make([]string, 0, len(seen))
	for lang := range seen {
		out = append(out, lang)
	}
	sort.Strings(out)
	return out
}

// document_updated returns the unix timestamp of the operator's last edit
// for (name, language), or 0 if no override exists.
func document_updated(name, language string) int64 {
	db := db_open("db/settings.db")
	row, err := db.row("select updated from documents where name=? and language=?", name, language)
	if err != nil || row == nil {
		return 0
	}
	switch v := row["updated"].(type) {
	case int64:
		return v
	case int:
		return int64(v)
	}
	return 0
}

// === Starlark API ===

var api_document = sls.FromStringDict(sl.String("mochi.document"), sl.StringDict{
	"get":  sl.NewBuiltin("mochi.document.get", api_document_get),
	"list": sl.NewBuiltin("mochi.document.list", api_document_list),
	"set":  sl.NewBuiltin("mochi.document.set", api_document_set),
})

// mochi.document.get(name, language=None) -> string: Get the rendered body of a document.
// Public — no authentication required. Applies the four-step fallback chain
// (operator override → bundled default, in language → in en) and renders
// {{operator.*}} and {{server.host}} placeholders. Language defaults to the
// request's locale.
func api_document_get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return sl_error(fn, "syntax: <name: string>, [language: string]")
	}
	name, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid document name")
	}
	if !document_name_valid(name) {
		return sl_error(fn, "unknown document %q", name)
	}
	language := ""
	if len(args) == 2 {
		if l, ok := sl.AsString(args[1]); ok {
			language = l
		}
	}
	if language == "" {
		if l, ok := t.Local("language").(string); ok {
			language = l
		}
	}
	return sl.String(document_get(name, language)), nil
}

// mochi.document.set(name, language, body) -> bool: Write an operator override (admin only).
// Audited via audit_settings_changed.
func api_document_set(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 3 {
		return sl_error(fn, "syntax: <name: string>, <language: string>, <body: string>")
	}
	user, _ := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}
	if !user.administrator() {
		return sl_error(fn, "not administrator")
	}
	name, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid document name")
	}
	language, ok := sl.AsString(args[1])
	if !ok {
		return sl_error(fn, "invalid language")
	}
	body, ok := sl.AsString(args[2])
	if !ok {
		return sl_error(fn, "invalid body")
	}
	if err := document_set(name, language, body); err != nil {
		return sl_error(fn, "%v", err)
	}
	audit_settings_changed(user.Username, "document/"+name+"/"+language, fmt.Sprintf("%d bytes", len(body)))
	return sl.True, nil
}

// mochi.document.list() -> list: Returns one entry per (name × language)
// supported. Each entry has {name, language, body, default, updated}, where
// `body` is the raw current body (operator override if set, else bundled
// default) and `default` is always the raw bundled default. Both are
// un-rendered Markdown so the admin UI can show placeholder syntax verbatim.
// `updated` is 0 if no operator override exists. Admin only.
func api_document_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 0 {
		return sl_error(fn, "syntax: no arguments")
	}
	user, _ := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}
	if !user.administrator() {
		return sl_error(fn, "not administrator")
	}
	languages := document_languages()
	out := []map[string]any{}
	for _, name := range document_names {
		for _, lang := range languages {
			defaultBody := document_bundled(name, lang)
			if defaultBody == "" {
				continue
			}
			body := document_override(name, lang)
			if body == "" {
				body = defaultBody
			}
			out = append(out, map[string]any{
				"name":     name,
				"language": lang,
				"body":     body,
				"default":  defaultBody,
				"updated":  document_updated(name, lang),
			})
		}
	}
	return sl_encode(out), nil
}

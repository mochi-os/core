// Mochi server: Connected accounts
// Copyright Alistair Cunningham 2025-2026
//
// Connected accounts allow users to link external services (email, push notifications,
// AI providers, MCP servers) to their Mochi account. Apps access accounts via the
// mochi.account.* Starlark API. Secrets (API keys, tokens) are never exposed to apps;
// core handles all provider interactions internally.

package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"html"
	"io"
	"net/http"
	"strings"
	"time"

	webpush "github.com/SherClockHolmes/webpush-go"
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

// Provider defines an account provider type
type Provider struct {
	Type         string          `json:"type"`
	Capabilities []string        `json:"capabilities"`
	Flow         string          `json:"flow"` // "form", "browser", "oauth"
	Fields       []ProviderField `json:"fields"`
	Verify       bool            `json:"verify"`
}

// ProviderField defines a field in a provider's form
type ProviderField struct {
	Name        string `json:"name"`
	Label       string `json:"label"`
	Type        string `json:"type"` // "email", "text", "password", "url"
	Required    bool   `json:"required"`
	Placeholder string `json:"placeholder"`
}

// providers defines all available account providers (sorted alphabetically by type)
var providers = []Provider{
	{
		Type:         "browser",
		Capabilities: []string{"notify"},
		Flow:         "browser",
		Fields:       nil, // handled by JavaScript
		Verify:       false,
	},
	{
		Type:         "claude",
		Capabilities: []string{"ai"},
		Flow:         "form",
		Fields: []ProviderField{
			{Name: "api_key", Label: "API key", Type: "password", Required: true, Placeholder: "sk-ant-..."},
			{Name: "model", Label: "Model", Type: "text", Required: false, Placeholder: "default"},
			{Name: "label", Label: "Name", Type: "text", Required: false, Placeholder: ""},
		},
		Verify: false,
	},
	{
		Type:         "email",
		Capabilities: []string{"notify"},
		Flow:         "form",
		Fields: []ProviderField{
			{Name: "address", Label: "Email address", Type: "email", Required: true, Placeholder: "you@example.com"},
		},
		Verify: true,
	},
	{
		Type:         "mcp",
		Capabilities: []string{"mcp"},
		Flow:         "form",
		Fields: []ProviderField{
			{Name: "url", Label: "Server URL", Type: "url", Required: true, Placeholder: "https://mcp.example.com"},
			{Name: "token", Label: "Access token", Type: "password", Required: false, Placeholder: ""},
			{Name: "label", Label: "Name", Type: "text", Required: false, Placeholder: ""},
		},
		Verify: false,
	},
	{
		Type:         "ntfy",
		Capabilities: []string{"notify"},
		Flow:         "form",
		Fields: []ProviderField{
			{Name: "topic", Label: "Topic", Type: "text", Required: true, Placeholder: "my-notifications"},
			{Name: "server", Label: "Server URL", Type: "url", Required: false, Placeholder: "https://ntfy.sh"},
			{Name: "token", Label: "Access token", Type: "password", Required: false, Placeholder: ""},
			{Name: "label", Label: "Name", Type: "text", Required: false, Placeholder: ""},
		},
		Verify: false,
	},
	{
		Type:         "openai",
		Capabilities: []string{"ai"},
		Flow:         "form",
		Fields: []ProviderField{
			{Name: "api_key", Label: "API key", Type: "password", Required: true, Placeholder: "sk-..."},
			{Name: "model", Label: "Model", Type: "text", Required: false, Placeholder: "default"},
			{Name: "label", Label: "Name", Type: "text", Required: false, Placeholder: ""},
		},
		Verify: false,
	},
	{
		Type:         "pushbullet",
		Capabilities: []string{"notify"},
		Flow:         "form",
		Fields: []ProviderField{
			{Name: "token", Label: "Access token", Type: "password", Required: true, Placeholder: ""},
			{Name: "label", Label: "Name", Type: "text", Required: false, Placeholder: ""},
		},
		Verify: false,
	},
	{
		Type:         "unifiedpush",
		Capabilities: []string{"notify"},
		Flow:         "browser",
		Fields:       nil,
		Verify:       false,
	},
	{
		Type:         "fcm",
		Capabilities: []string{"notify"},
		Flow:         "browser",
		Fields:       nil,
		Verify:       false,
	},
	{
		Type:         "url",
		Capabilities: []string{"notify"},
		Flow:         "form",
		Fields: []ProviderField{
			{Name: "url", Label: "URL", Type: "url", Required: true, Placeholder: ""},
			{Name: "secret", Label: "Signing secret", Type: "password", Required: false, Placeholder: ""},
			{Name: "label", Label: "Name", Type: "text", Required: false, Placeholder: ""},
		},
		Verify: false,
	},
}

// Unambiguous character set for verification codes (no 0/O, 1/l/I confusion)
const verificationCharset = "23456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghjkmnpqrstuvwxyz"

// Starlark API module
var api_account = sls.FromStringDict(sl.String("mochi.account"), sl.StringDict{
	"add":       sl.NewBuiltin("mochi.account.add", api_account_add),
	"get":       sl.NewBuiltin("mochi.account.get", api_account_get),
	"list":      sl.NewBuiltin("mochi.account.list", api_account_list),
	"notify":    sl.NewBuiltin("mochi.account.notify", api_account_notify),
	"providers": sl.NewBuiltin("mochi.account.providers", api_account_providers),
	"remove":    sl.NewBuiltin("mochi.account.remove", api_account_remove),
	"test":      sl.NewBuiltin("mochi.account.test", api_account_test),
	"update":    sl.NewBuiltin("mochi.account.update", api_account_update),
	"verify":    sl.NewBuiltin("mochi.account.verify", api_account_verify),
})

// provider_get returns a provider by type, or nil if not found
func provider_get(ptype string) *Provider {
	for i := range providers {
		if providers[i].Type == ptype {
			return &providers[i]
		}
	}
	return nil
}

// providers_by_capability returns all providers with a given capability
func providers_by_capability(capability string) []Provider {
	var result []Provider
	for _, p := range providers {
		for _, c := range p.Capabilities {
			if c == capability {
				result = append(result, p)
				break
			}
		}
	}
	return result
}

// provider_has_capability checks if a provider has a given capability
func provider_has_capability(ptype, capability string) bool {
	p := provider_get(ptype)
	if p == nil {
		return false
	}
	for _, c := range p.Capabilities {
		if c == capability {
			return true
		}
	}
	return false
}

// account_generate_code generates a random verification code
func account_generate_code(length int) string {
	b := make([]byte, length)
	rand.Read(b)
	code := make([]byte, length)
	for i := range b {
		code[i] = verificationCharset[int(b[i])%len(verificationCharset)]
	}
	return string(code)
}

// account_redact returns an account map with secrets removed
func account_redact(row map[string]any) map[string]any {
	return map[string]any{
		"id":         row["id"],
		"type":       row["type"],
		"label":      row["label"],
		"identifier": row["identifier"],
		"created":    row["created"],
		"verified":   row["verified"],
		"enabled":    row["enabled"],
		"default":    row["default"],
	}
}

// mochi.account.providers(capability?) -> list: Get available providers
func api_account_providers(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var capability string
	if len(args) > 0 && args[0] != sl.None {
		cap, ok := sl.AsString(args[0])
		if !ok {
			return sl_error(fn, "invalid capability")
		}
		capability = cap
	}

	result := []map[string]any{}
	var list []Provider

	if capability != "" {
		list = providers_by_capability(capability)
	} else {
		list = providers
	}

	for _, p := range list {
		pm := map[string]any{
			"type":         p.Type,
			"capabilities": p.Capabilities,
			"flow":         p.Flow,
			"verify":       p.Verify,
		}

		if len(p.Fields) > 0 {
			fields := make([]map[string]any, len(p.Fields))
			for i, f := range p.Fields {
				fields[i] = map[string]any{
					"name":        f.Name,
					"label":       f.Label,
					"type":        f.Type,
					"required":    f.Required,
					"placeholder": f.Placeholder,
				}
			}
			pm["fields"] = fields
		} else {
			pm["fields"] = []map[string]any{}
		}

		result = append(result, pm)
	}

	return sl_encode(result), nil
}

// mochi.account.list(capability?) -> list: List user's accounts
func api_account_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := require_permission(t, fn, "accounts/read"); err != nil {
		return sl_error(fn, "%v", err)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	db := db_user(user, "user")

	var capability string
	if len(args) > 0 && args[0] != sl.None {
		cap, ok := sl.AsString(args[0])
		if !ok {
			return sl_error(fn, "invalid capability")
		}
		capability = cap
	}

	rows, err := db.rows("select id, type, label, identifier, created, verified, enabled, \"default\" from accounts order by created desc")
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}

	result := []map[string]any{}
	for _, row := range rows {
		// Filter by capability if specified
		if capability != "" {
			ptype, _ := row["type"].(string)
			if !provider_has_capability(ptype, capability) {
				continue
			}
		}
		result = append(result, account_redact(row))
	}

	return sl_encode(result), nil
}

// mochi.account.get(id) -> dict | None: Get a single account
func api_account_get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <id: integer>")
	}

	if err := require_permission(t, fn, "accounts/read"); err != nil {
		return sl_error(fn, "%v", err)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	id, err := sl.AsInt32(args[0])
	if err != nil {
		return sl_error(fn, "invalid id")
	}

	db := db_user(user, "user")
	row, err := db.row("select id, type, label, identifier, created, verified, enabled, \"default\" from accounts where id=?", id)
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}
	if row == nil {
		return sl.None, nil
	}

	return sl_encode(account_redact(row)), nil
}

// mochi.account.add(type, label=..., address=..., token=..., api_key=..., url=..., endpoint=..., auth=..., p256dh=..., secret=..., topic=..., server=...) -> dict: Add an account
func api_account_add(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <type: string>, [label=...], [address=...], [token=...], [api_key=...], [url=...], [endpoint=...], [auth=...], [p256dh=...], [secret=...], [topic=...], [server=...]")
	}

	if err := require_permission(t, fn, "accounts/manage"); err != nil {
		return sl_error(fn, "%v", err)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	ptype, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid type")
	}

	provider := provider_get(ptype)
	if provider == nil {
		return sl_error(fn, "unknown provider type %q", ptype)
	}

	// Extract fields from kwargs
	fields := make(map[string]any)
	for _, kv := range kwargs {
		key := string(kv[0].(sl.String))
		val, _ := sl.AsString(kv[1])
		fields[key] = val
	}

	// Validate required fields
	for _, f := range provider.Fields {
		if f.Required {
			val, ok := fields[f.Name]
			if !ok || val == nil || val == "" {
				return sl_error(fn, "required field %q is missing", f.Name)
			}
		}
	}

	db := db_user(user, "user")
	now := time.Now().Unix()

	// Extract standard fields
	label, _ := fields["label"].(string)
	identifier := ""
	data := make(map[string]any)

	switch ptype {
	case "email":
		address, _ := fields["address"].(string)
		if !email_valid(address) {
			return sl_error(fn, "invalid email address")
		}
		identifier = address

		// Generate verification code
		code := account_generate_code(10)
		expires := now + 3600 // 1 hour
		data["code"] = code
		data["expires"] = expires

		// Send verification email
		account_send_verification_email(address, code, user_language(user))

	case "browser":
		// Browser push - extract endpoint for uniqueness check
		endpoint, _ := fields["endpoint"].(string)
		if endpoint == "" {
			return sl_error(fn, "endpoint is required for browser push")
		}

		// Check for existing subscription with same endpoint
		existing, _ := db.row("select id from accounts where type='browser' and identifier=?", endpoint)
		if existing != nil {
			// Update existing instead of creating duplicate
			auth, _ := fields["auth"].(string)
			p256dh, _ := fields["p256dh"].(string)
			data["endpoint"] = endpoint
			data["auth"] = auth
			data["p256dh"] = p256dh
			data_json, _ := json.Marshal(data)

			db.exec("update accounts set data=?, created=? where id=?", string(data_json), now, existing["id"])
			row, _ := db.row("select id, type, label, identifier, created, verified from accounts where id=?", existing["id"])
			return sl_encode(account_redact(row)), nil
		}

		identifier = endpoint
		auth, _ := fields["auth"].(string)
		p256dh, _ := fields["p256dh"].(string)
		data["endpoint"] = endpoint
		data["auth"] = auth
		data["p256dh"] = p256dh

	case "pushbullet":
		token, _ := fields["token"].(string)
		data["token"] = token

	case "claude", "openai":
		api_key, _ := fields["api_key"].(string)
		data["api_key"] = api_key
		if model, _ := fields["model"].(string); model != "" {
			identifier = model
			data["model"] = model
		} else {
			identifier = "default"
		}

	case "mcp":
		url, _ := fields["url"].(string)
		token, _ := fields["token"].(string)
		identifier = url
		data["token"] = token

	case "ntfy":
		topic, _ := fields["topic"].(string)
		server, _ := fields["server"].(string)
		token, _ := fields["token"].(string)
		if server == "" {
			server = "https://ntfy.sh"
		}
		identifier = server + "/" + topic
		data["topic"] = topic
		data["server"] = server
		if token != "" {
			data["token"] = token
		}

	case "url":
		url, _ := fields["url"].(string)
		secret, _ := fields["secret"].(string)
		identifier = url
		if secret != "" {
			data["secret"] = secret
		}

	case "unifiedpush":
		// UnifiedPush subscription. endpoint may be either an absolute
		// URL (third-party distributor like ntfy.sh — used verbatim at
		// delivery time as RFC 8030 Web Push) OR a path-only string
		// like "/menu/-/push/inbound/<sub>" synthesised by
		// function_push_register for the local-distributor case (where
		// the deliver fast-path forwards via in-process WebSocket
		// instead of a self-HTTP-call). Either way, we store it
		// verbatim along with the auth + p256dh keys.
		endpoint, _ := fields["endpoint"].(string)
		auth, _ := fields["auth"].(string)
		p256dh, _ := fields["p256dh"].(string)

		// Re-registration is idempotent: each Android app re-runs
		// MochiPushClient.register at every launch, so the App side
		// hits /menu/-/push/register with the same endpoint each
		// time. If we kept inserting we'd accumulate duplicate
		// account rows + destinations forever. Mirror the browser
		// case: update the existing row in place when a row with the
		// same endpoint already exists.
		if endpoint != "" {
			existing, _ := db.row("select id from accounts where type='unifiedpush' and identifier=?", endpoint)
			if existing != nil {
				data["endpoint"] = endpoint
				data["auth"] = auth
				data["p256dh"] = p256dh
				data_json, _ := json.Marshal(data)
				db.exec("update accounts set label=?, data=? where id=?", label, string(data_json), existing["id"])
				row, _ := db.row("select id, type, label, identifier, created, verified from accounts where id=?", existing["id"])
				return sl_encode(account_redact(row)), nil
			}
		}

		identifier = endpoint
		data["endpoint"] = endpoint
		data["auth"] = auth
		data["p256dh"] = p256dh

	case "fcm":
		// Firebase Cloud Messaging device token. The Android client calls
		// /notifications/-/push/register/fcm after FirebaseMessaging.getToken
		// resolves (and again on onNewToken). The Firebase Installations ID
		// (install_id) is the stable per-install handle used as `identifier`
		// so token refreshes update the existing row in place, while a
		// second phone (different FID) creates a separate row — both can
		// receive pushes concurrently.
		token, _ := fields["token"].(string)
		install_id, _ := fields["install_id"].(string)
		if token == "" {
			return sl_error(fn, "required field %q is missing", "token")
		}
		if install_id == "" {
			return sl_error(fn, "required field %q is missing", "install_id")
		}
		existing, _ := db.row("select id from accounts where type='fcm' and identifier=?", install_id)
		if existing != nil {
			data["token"] = token
			data_json, _ := json.Marshal(data)
			db.exec("update accounts set label=?, data=? where id=?", label, string(data_json), existing["id"])
			row, _ := db.row("select id, type, label, identifier, created, verified from accounts where id=?", existing["id"])
			return sl_encode(account_redact(row)), nil
		}
		identifier = install_id
		data["token"] = token
	}

	// Serialize data to JSON
	data_json := ""
	if len(data) > 0 {
		b, _ := json.Marshal(data)
		data_json = string(b)
	}

	verified := int64(0)
	if !provider.Verify {
		verified = now
	}

	// Insert account
	result, err := db.internal.Exec(
		"insert into accounts (type, label, identifier, data, created, verified) values (?, ?, ?, ?, ?, ?)",
		ptype, label, identifier, data_json, now, verified,
	)
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}

	id, _ := result.LastInsertId()

	// Replicate user-facing account inserts so the other host has the
	// same row id (destinations.target references this integer id and
	// is replicated independently). Per-device types stay host-local —
	// each browser / phone registers its own push endpoint per host.
	switch ptype {
	case "browser", "unifiedpush", "fcm":
		// host-local
	default:
		replication_emit_user_core_exec(user,
			"insert or replace into accounts (id, type, label, identifier, data, created, verified) values (?, ?, ?, ?, ?, ?, ?)",
			[]any{id, ptype, label, identifier, data_json, now, verified})
	}

	return sl_encode(map[string]any{
		"id":         id,
		"type":       ptype,
		"label":      label,
		"identifier": identifier,
		"created":    now,
		"verified":   verified,
	}), nil
}

// mochi.account.update(id, label=..., enabled=...) -> bool: Update an account
func api_account_update(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <id: integer>, [label=...], [enabled=...]")
	}

	if err := require_permission(t, fn, "accounts/manage"); err != nil {
		return sl_error(fn, "%v", err)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	id, err := sl.AsInt32(args[0])
	if err != nil {
		return sl_error(fn, "invalid id")
	}

	db := db_user(user, "user")

	// Check account exists
	exists, _ := db.exists("select 1 from accounts where id=?", id)
	if !exists {
		return sl.False, nil
	}

	// Process kwargs
	for _, kv := range kwargs {
		key := string(kv[0].(sl.String))
		switch key {
		case "label":
			label, _ := sl.AsString(kv[1])
			db.exec_replicated("update accounts set label=? where id=?", label, id)

		case "enabled":
			var val int
			switch v := kv[1].(type) {
			case sl.Bool:
				if v {
					val = 1
				}
			case sl.Int:
				i, _ := v.Int64()
				val = int(i)
			}
			db.exec_replicated("update accounts set enabled=? where id=?", val, id)

		case "default":
			val, _ := sl.AsString(kv[1])
			if val != "" {
				db.exec_replicated("update accounts set \"default\"='' where \"default\"=?", val)
			}
			db.exec_replicated("update accounts set \"default\"=? where id=?", val, id)

		case "model":
			model, _ := sl.AsString(kv[1])
			row, err := db.row("select type, data from accounts where id=?", id)
			if err == nil && row != nil {
				raw, _ := row["data"].(string)
				var d map[string]any
				if raw != "" {
					json.Unmarshal([]byte(raw), &d)
				}
				if d == nil {
					d = make(map[string]any)
				}
				if model != "" {
					d["model"] = model
				} else {
					delete(d, "model")
				}
				j, _ := json.Marshal(d)
				// Keep identifier in sync for AI accounts
				ptype, _ := row["type"].(string)
				if ptype == "claude" || ptype == "openai" {
					ident := "default"
					if model != "" {
						ident = model
					}
					db.exec_replicated("update accounts set data=?, identifier=? where id=?", string(j), ident, id)
				} else {
					db.exec_replicated("update accounts set data=? where id=?", string(j), id)
				}
			}

		case "endpoint":
			// Used by function_push_register's local-distributor branch
			// to write back the canonical path-only endpoint that
			// embeds the just-allocated account id.
			endpoint, _ := sl.AsString(kv[1])
			row, err := db.row("select data from accounts where id=?", id)
			if err == nil && row != nil {
				raw, _ := row["data"].(string)
				var d map[string]any
				if raw != "" {
					json.Unmarshal([]byte(raw), &d)
				}
				if d == nil {
					d = make(map[string]any)
				}
				d["endpoint"] = endpoint
				j, _ := json.Marshal(d)
				db.exec("update accounts set data=?, identifier=? where id=?", string(j), endpoint, id)
			}
		}
	}

	return sl.True, nil
}

// mochi.account.remove(id) -> bool: Remove an account
func api_account_remove(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <id: integer>")
	}

	if err := require_permission(t, fn, "accounts/manage"); err != nil {
		return sl_error(fn, "%v", err)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	id, err := sl.AsInt32(args[0])
	if err != nil {
		return sl_error(fn, "invalid id")
	}

	db := db_user(user, "user")

	// Check account exists
	exists, _ := db.exists("select 1 from accounts where id=?", id)
	if !exists {
		return sl.False, nil
	}

	db.exec_replicated("delete from accounts where id=?", id)
	return sl.True, nil
}

// mochi.account.verify(id, code?) -> bool: Verify an account or resend code
func api_account_verify(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return sl_error(fn, "syntax: <id: integer>, [code: string]")
	}

	if err := require_permission(t, fn, "accounts/manage"); err != nil {
		return sl_error(fn, "%v", err)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	id, err := sl.AsInt32(args[0])
	if err != nil {
		return sl_error(fn, "invalid id")
	}

	var code string
	if len(args) > 1 && args[1] != sl.None {
		c, ok := sl.AsString(args[1])
		if !ok {
			return sl_error(fn, "invalid code")
		}
		code = c
	}

	db := db_user(user, "user")

	// Get account with data
	row, err := db.row("select id, type, identifier, data, verified from accounts where id=?", id)
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}
	if row == nil {
		return sl_error(fn, "account not found")
	}

	// Check if already verified
	verified, _ := row["verified"].(int64)
	if verified > 0 {
		return sl.True, nil
	}

	ptype, _ := row["type"].(string)
	provider := provider_get(ptype)
	if provider == nil || !provider.Verify {
		// Account type doesn't require verification
		return sl.True, nil
	}

	identifier, _ := row["identifier"].(string)
	raw, _ := row["data"].(string)

	var data map[string]any
	if raw != "" {
		json.Unmarshal([]byte(raw), &data)
	}
	if data == nil {
		data = make(map[string]any)
	}

	now := time.Now().Unix()

	if code == "" {
		// Resend verification code
		existing_code, _ := data["code"].(string)
		expires, _ := data["expires"].(float64)

		language := user_language(user)
		if existing_code != "" && int64(expires) > now {
			// Reuse existing code, extend expiration
			data["expires"] = now + 3600
			data_json, _ := json.Marshal(data)
			db.exec_replicated("update accounts set data=? where id=?", string(data_json), id)
			account_send_verification_email(identifier, existing_code, language)
		} else {
			// Generate new code
			new_code := account_generate_code(10)
			data["code"] = new_code
			data["expires"] = now + 3600

			data_json, _ := json.Marshal(data)
			db.exec_replicated("update accounts set data=? where id=?", string(data_json), id)

			account_send_verification_email(identifier, new_code, language)
		}
		return sl.True, nil
	}

	// Verify the code
	stored_code, _ := data["code"].(string)
	expires, _ := data["expires"].(float64)

	if stored_code == "" || code != stored_code {
		return sl.False, nil
	}

	if int64(expires) < now {
		return sl_error(fn, "verification code has expired")
	}

	// Code matches - mark as verified and clear code data
	delete(data, "code")
	delete(data, "expires")

	data_json, _ := json.Marshal(data)
	db.exec_replicated("update accounts set verified=?, data=? where id=?", now, string(data_json), id)

	return sl.True, nil
}

// account_send_verification_email sends a styled HTML email with a verification
// code, localised to the given language (BCP 47 tag) via the core label
// resolver's fallback chain.
func account_send_verification_email(to string, code string, language string) {
	subject := resolve_core_label(language, "email.verification.subject", nil)
	heading := resolve_core_label(language, "email.verification.heading", nil)
	tagline := resolve_core_label(language, "email.verification.tagline", nil)
	expiry := resolve_core_label(language, "email.verification.expiry", nil)
	ignore := resolve_core_label(language, "email.verification.ignore", nil)
	html_body := `<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="margin: 0; padding: 0; background-color: #f4f4f5; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;">
  <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="min-height: 100vh;">
    <tr>
      <td align="center" style="padding: 40px 20px;">
        <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="max-width: 440px; background-color: #ffffff; border-radius: 12px; box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);">
          <tr>
            <td style="padding: 40px 40px 32px 40px; text-align: center;">
              <h1 style="margin: 0 0 8px 0; font-size: 24px; font-weight: 600; color: #18181b;">` + html.EscapeString(heading) + `</h1>
              <p style="margin: 0; font-size: 15px; color: #71717a;">` + html.EscapeString(tagline) + `</p>
            </td>
          </tr>
          <tr>
            <td style="padding: 0 40px;">
              <div style="background-color: #f4f4f5; border-radius: 8px; padding: 24px; text-align: center;">
                <span style="font-family: 'SF Mono', Monaco, 'Cascadia Code', monospace; font-size: 32px; font-weight: 600; letter-spacing: 4px; color: #18181b;">` + html.EscapeString(code) + `</span>
              </div>
            </td>
          </tr>
          <tr>
            <td style="padding: 32px 40px 40px 40px; text-align: center;">
              <p style="margin: 0; font-size: 14px; color: #a1a1aa;">` + html.EscapeString(expiry+". "+ignore) + `</p>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>`
	email_send_html(to, subject, html_body)
}

// AccountTestResult represents the result of testing an account
type AccountTestResult struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

// account_display_label returns a friendly name for a connected account for
// use in test-notification bodies. Prefers the user-set label; falls back to
// a localised provider-typed string ("Android device", "Email", etc.), or to
// the identifier for email accounts when no label is set, or to the raw type
// as a last resort.
func account_display_label(row map[string]any, language string) string {
	if label, _ := row["label"].(string); label != "" {
		return label
	}
	ptype, _ := row["type"].(string)
	identifier, _ := row["identifier"].(string)
	if ptype == "email" && identifier != "" {
		return identifier
	}
	key := "account.display." + ptype
	resolved := resolve_core_label(language, key, nil)
	if resolved != key {
		return resolved
	}
	return ptype
}

// mochi.account.test(id) -> dict: Test an account connection
func api_account_test(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <id: integer>")
	}

	if err := require_permission(t, fn, "accounts/manage"); err != nil {
		return sl_error(fn, "%v", err)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	id, err := sl.AsInt32(args[0])
	if err != nil {
		return sl_error(fn, "invalid id")
	}

	db := db_user(user, "user")

	// Get account with data
	row, err := db.row("select id, type, identifier, label, data from accounts where id=?", id)
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}
	if row == nil {
		return sl_error(fn, "account not found")
	}

	ptype, _ := row["type"].(string)
	identifier, _ := row["identifier"].(string)
	raw, _ := row["data"].(string)

	var data map[string]any
	if raw != "" {
		json.Unmarshal([]byte(raw), &data)
	}

	var result AccountTestResult

	language := user_language(user)
	label := account_display_label(row, language)
	switch ptype {
	case "email":
		result = account_test_email(identifier, language, label)

	case "browser":
		result = account_test_browser(data, language, label)

	case "unifiedpush":
		result = account_test_unifiedpush(data, language, label)

	case "fcm":
		var retire bool
		result, retire = account_test_fcm(data, language, label)
		if !result.Success && retire {
			// Permanent FCM failure (UNREGISTERED / INVALID_ARGUMENT) —
			// drop the row here too so the next FcmRegistrar.connect from
			// the phone replaces it cleanly. Without this, every Test
			// click re-hits Google with the same dead token forever.
			db.exec("delete from accounts where id=?", id)
		}

	case "pushbullet":
		token, _ := data["token"].(string)
		result = account_test_pushbullet(token, language, label)

	case "claude":
		api_key, _ := data["api_key"].(string)
		result = account_test_claude(api_key)

	case "openai":
		api_key, _ := data["api_key"].(string)
		result = account_test_openai(api_key)

	case "mcp":
		url := identifier
		token, _ := data["token"].(string)
		result = account_test_mcp(url, token)

	case "ntfy":
		server, _ := data["server"].(string)
		topic, _ := data["topic"].(string)
		token, _ := data["token"].(string)
		result = account_test_ntfy(server, topic, token, language, label)

	case "url":
		url := identifier
		secret, _ := data["secret"].(string)
		result = account_test_url(url, secret)

	default:
		result = AccountTestResult{Success: false, Message: "Unknown account type"}
	}

	return sl_encode(map[string]any{
		"success": result.Success,
		"message": result.Message,
	}), nil
}

// account_test_email sends a test email, localised to the recipient user's
// language preference via the core label resolver.
func account_test_email(address string, language string, account_label string) AccountTestResult {
	subject := resolve_core_label(language, "email.test.subject", nil)
	heading := resolve_core_label(language, "email.test.heading", nil)
	body := resolve_core_label(language, "email.test.body", map[string]any{"account": account_label})
	html_body := `<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="margin: 0; padding: 0; background-color: #f4f4f5; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;">
  <table role="presentation" width="100%" cellspacing="0" cellpadding="0">
    <tr>
      <td align="center" style="padding: 20px;">
        <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="max-width: 440px; background-color: #ffffff; border-radius: 12px; box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);">
          <tr>
            <td style="padding: 40px; text-align: center;">
              <h1 style="margin: 0 0 12px 0; font-size: 24px; font-weight: 600; color: #18181b;">` + html.EscapeString(heading) + `</h1>
              <p style="margin: 0; font-size: 15px; color: #52525b;">` + html.EscapeString(body) + `</p>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>`
	email_send_html(address, subject, html_body)
	return AccountTestResult{Success: true, Message: "Test email sent"}
}

// account_test_browser sends a test browser push notification, localised to
// the recipient user's language preference.
func account_test_browser(data map[string]any, language string, account_label string) AccountTestResult {
	webpush_ensure()
	if webpush_public == "" || webpush_private == "" {
		return AccountTestResult{Success: false, Message: "Push notifications not configured"}
	}

	endpoint, _ := data["endpoint"].(string)
	auth, _ := data["auth"].(string)
	p256dh, _ := data["p256dh"].(string)

	if endpoint == "" || auth == "" || p256dh == "" {
		return AccountTestResult{Success: false, Message: "Invalid subscription data"}
	}

	payload, _ := json.Marshal(map[string]string{
		"title": resolve_core_label(language, "push.test.title", nil),
		"body":  resolve_core_label(language, "push.test.body", map[string]any{"account": account_label}),
	})

	sub := webpush.Subscription{
		Endpoint: endpoint,
		Keys: webpush.Keys{
			Auth:   auth,
			P256dh: p256dh,
		},
	}

	resp, err := webpush.SendNotification(payload, &sub, &webpush.Options{
		Subscriber:      "mailto:webpush@localhost",
		VAPIDPublicKey:  webpush_public,
		VAPIDPrivateKey: webpush_private,
		TTL:             60,
	})

	if err != nil {
		return AccountTestResult{Success: false, Message: "Push notification failed: " + err.Error()}
	}
	defer resp.Body.Close()

	if resp.StatusCode == 201 {
		return AccountTestResult{Success: true, Message: "Test notification sent"}
	} else if resp.StatusCode == 404 || resp.StatusCode == 410 {
		return AccountTestResult{Success: false, Message: "Subscription expired"}
	}

	return AccountTestResult{Success: false, Message: "Push notification failed"}
}

// account_test_unifiedpush sends a test push notification via UnifiedPush.
// Uses RFC 8030 Web Push to whatever endpoint URL the distributor allocated;
// works for our own distributor and for third-party distributors (ntfy etc.)
// alike — endpoint is opaque.
func account_test_unifiedpush(data map[string]any, language string, account_label string) AccountTestResult {
	webpush_ensure()
	if webpush_public == "" || webpush_private == "" {
		return AccountTestResult{Success: false, Message: "Push notifications not configured"}
	}

	endpoint, _ := data["endpoint"].(string)
	auth, _ := data["auth"].(string)
	p256dh, _ := data["p256dh"].(string)

	if endpoint == "" || auth == "" || p256dh == "" {
		return AccountTestResult{Success: false, Message: "Invalid subscription data"}
	}

	payload, _ := json.Marshal(map[string]string{
		"title": resolve_core_label(language, "push.test.title", nil),
		"body":  resolve_core_label(language, "push.test.body", map[string]any{"account": account_label}),
	})

	sub := webpush.Subscription{
		Endpoint: endpoint,
		Keys: webpush.Keys{
			Auth:   auth,
			P256dh: p256dh,
		},
	}

	resp, err := webpush.SendNotification(payload, &sub, &webpush.Options{
		Subscriber:      "mailto:webpush@localhost",
		VAPIDPublicKey:  webpush_public,
		VAPIDPrivateKey: webpush_private,
		TTL:             60,
	})

	if err != nil {
		return AccountTestResult{Success: false, Message: "Push notification failed: " + err.Error()}
	}
	defer resp.Body.Close()

	if resp.StatusCode == 201 {
		return AccountTestResult{Success: true, Message: "Test notification sent"}
	} else if resp.StatusCode == 404 || resp.StatusCode == 410 {
		return AccountTestResult{Success: false, Message: "Subscription expired"}
	}

	return AccountTestResult{Success: false, Message: "Push notification failed"}
}

// account_test_fcm sends a test notification via Firebase Cloud Messaging,
// reusing the same delivery path the production notification flow uses so
// any service-account / API-key misconfiguration surfaces with the same
// failure mode the user would otherwise hit. Returns the test result plus
// retire=true when the row holds a permanently-dead token (Google
// UNREGISTERED / INVALID_ARGUMENT) so the caller can drop it — same
// semantics as the production-push path in api_account_notify.
func account_test_fcm(data map[string]any, language string, account_label string) (AccountTestResult, bool) {
	if setting_get("fcm.service_account", "") == "" {
		return AccountTestResult{Success: false, Message: "FCM not configured"}, false
	}
	title := resolve_core_label(language, "push.test.title", nil)
	body := resolve_core_label(language, "push.test.body", map[string]any{"account": account_label})
	success, retire, detail := account_deliver_fcm(data, title, body, "", "notifications-test-", "notifications", "")
	if success {
		return AccountTestResult{Success: true, Message: "Test notification sent"}, false
	}
	msg := detail
	if msg == "" {
		msg = "Push notification failed"
	}
	return AccountTestResult{Success: false, Message: msg}, retire
}

// account_test_pushbullet sends a test notification via Pushbullet
func account_test_pushbullet(token string, language string, account_label string) AccountTestResult {
	if token == "" {
		return AccountTestResult{Success: false, Message: "No access token"}
	}

	payload, _ := json.Marshal(map[string]string{
		"type":  "note",
		"title": resolve_core_label(language, "push.test.title", nil),
		"body":  resolve_core_label(language, "push.test.body", map[string]any{"account": account_label}),
	})

	req, _ := http.NewRequest("POST", "https://api.pushbullet.com/v2/pushes", bytes.NewReader(payload))
	req.Header.Set("Access-Token", token)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return AccountTestResult{Success: false, Message: "Connection failed: " + err.Error()}
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		return AccountTestResult{Success: true, Message: "Test push sent"}
	}

	body, _ := io.ReadAll(resp.Body)
	var data map[string]any
	json.Unmarshal(body, &data)
	if error_map, ok := data["error"].(map[string]any); ok {
		if msg, ok := error_map["message"].(string); ok {
			return AccountTestResult{Success: false, Message: msg}
		}
	}

	return AccountTestResult{Success: false, Message: "Pushbullet error"}
}

// account_test_claude verifies a Claude API key
func account_test_claude(api_key string) AccountTestResult {
	if api_key == "" {
		return AccountTestResult{Success: false, Message: "No API key"}
	}

	// Use a minimal request to verify the key works
	payload, _ := json.Marshal(map[string]any{
		"model":      "claude-3-haiku-20240307",
		"max_tokens": 1,
		"messages": []map[string]string{
			{"role": "user", "content": "Hi"},
		},
	})

	req, _ := http.NewRequest("POST", "https://api.anthropic.com/v1/messages", bytes.NewReader(payload))
	req.Header.Set("x-api-key", api_key)
	req.Header.Set("anthropic-version", "2023-06-01")
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return AccountTestResult{Success: false, Message: "Connection failed: " + err.Error()}
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		return AccountTestResult{Success: true, Message: "API key verified"}
	}
	if resp.StatusCode == 401 {
		return AccountTestResult{Success: false, Message: "Invalid API key"}
	}

	body, _ := io.ReadAll(resp.Body)
	var data map[string]any
	json.Unmarshal(body, &data)
	if error_map, ok := data["error"].(map[string]any); ok {
		if msg, ok := error_map["message"].(string); ok {
			return AccountTestResult{Success: false, Message: msg}
		}
	}

	return AccountTestResult{Success: false, Message: "API verification failed"}
}

// account_test_openai verifies an OpenAI API key
func account_test_openai(api_key string) AccountTestResult {
	if api_key == "" {
		return AccountTestResult{Success: false, Message: "No API key"}
	}

	req, _ := http.NewRequest("GET", "https://api.openai.com/v1/models", nil)
	req.Header.Set("Authorization", "Bearer "+api_key)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return AccountTestResult{Success: false, Message: "Connection failed: " + err.Error()}
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		return AccountTestResult{Success: true, Message: "API key verified"}
	}
	if resp.StatusCode == 401 {
		return AccountTestResult{Success: false, Message: "Invalid API key"}
	}

	return AccountTestResult{Success: false, Message: "API verification failed"}
}

// account_test_mcp tests connection to an MCP server
func account_test_mcp(url, token string) AccountTestResult {
	if url == "" {
		return AccountTestResult{Success: false, Message: "No server URL"}
	}
	if url_is_cloud_metadata(url) {
		return AccountTestResult{Success: false, Message: "URL not allowed"}
	}

	// Send MCP initialize request
	payload, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "initialize",
		"params": map[string]any{
			"protocolVersion": "2024-11-05",
			"capabilities":    map[string]any{},
			"clientInfo": map[string]string{
				"name":    "mochi",
				"version": "1.0",
			},
		},
	})

	req, _ := http.NewRequest("POST", url, bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return AccountTestResult{Success: false, Message: "Connection failed: " + err.Error()}
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		body, _ := io.ReadAll(resp.Body)
		var result map[string]any
		if json.Unmarshal(body, &result) == nil {
			if _, ok := result["result"]; ok {
				return AccountTestResult{Success: true, Message: "Server connected"}
			}
			if error_map, ok := result["error"].(map[string]any); ok {
				if msg, ok := error_map["message"].(string); ok {
					return AccountTestResult{Success: false, Message: msg}
				}
			}
		}
		return AccountTestResult{Success: true, Message: "Server connected"}
	}
	if resp.StatusCode == 401 || resp.StatusCode == 403 {
		return AccountTestResult{Success: false, Message: "Authentication failed"}
	}

	return AccountTestResult{Success: false, Message: "Connection failed"}
}

// account_test_ntfy sends a test notification via ntfy
func account_test_ntfy(server, topic, token string, language string, account_label string) AccountTestResult {
	if topic == "" {
		return AccountTestResult{Success: false, Message: "No topic configured"}
	}
	if server == "" {
		server = "https://ntfy.sh"
	}

	url := server + "/" + topic
	if url_is_cloud_metadata(url) {
		return AccountTestResult{Success: false, Message: "URL not allowed"}
	}
	title := resolve_core_label(language, "push.test.title", nil)
	body := resolve_core_label(language, "push.test.body", map[string]any{"account": account_label})
	req, _ := http.NewRequest("POST", url, bytes.NewReader([]byte(body)))
	req.Header.Set("Title", title)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return AccountTestResult{Success: false, Message: "Connection failed: " + err.Error()}
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		return AccountTestResult{Success: true, Message: "Test notification sent"}
	}
	if resp.StatusCode == 401 || resp.StatusCode == 403 {
		return AccountTestResult{Success: false, Message: "Authentication failed"}
	}

	return AccountTestResult{Success: false, Message: "ntfy returned " + itoa(resp.StatusCode)}
}

// account_test_url tests an external URL
func account_test_url(url, secret string) AccountTestResult {
	if url == "" {
		return AccountTestResult{Success: false, Message: "No URL configured"}
	}
	if url_is_cloud_metadata(url) {
		return AccountTestResult{Success: false, Message: "URL not allowed"}
	}

	// Send a test notification
	payload := []byte(`{"test":true}`)
	req, _ := http.NewRequest("POST", url, bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Mochi-Event", "test")

	if secret != "" {
		timestamp := time.Now().Unix()
		signature := account_url_signature(timestamp, payload, secret)
		req.Header.Set("X-Mochi-Timestamp", itoa(int(timestamp)))
		req.Header.Set("X-Mochi-Signature", signature)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return AccountTestResult{Success: false, Message: "Connection failed: " + err.Error()}
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return AccountTestResult{Success: true, Message: "External URL test sent"}
	}

	return AccountTestResult{Success: false, Message: "External URL returned " + itoa(resp.StatusCode)}
}

// mochi.account.notify(app, category, object, title, body, link, urgency?, account?, id?) -> dict:
// Notify the user across one or all of their verified notification accounts. Iterates
// accounts with the "notify" capability and dispatches via the matching provider driver
// (browser push, email, pushbullet, ntfy, external URL). Returns {sent, failed} counts.
//
// id (optional): the notification row id from the notifications app. When set, it is
// echoed in unifiedpush / fcm push payloads so the device can call -/read on tap and
// clear the matching row from the user's web bell.
//
// Dispatch runs in core because account secrets (push subscriptions, API tokens, HMAC
// signing keys) are never exposed to apps. For policy-aware notification routing
// (topics, categories, per-user destinations), call the notifications app's service
// instead: mochi.service.call("notifications", "send", ...).
func api_account_notify(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := require_permission(t, fn, "accounts/notify"); err != nil {
		return sl_error(fn, "%v", err)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	var app, category, object, title, body, link, urgency, id string
	var account int
	if err := sl.UnpackArgs(fn.Name(), args, kwargs,
		"app", &app,
		"category", &category,
		"object", &object,
		"title", &title,
		"body", &body,
		"link?", &link,
		"urgency?", &urgency,
		"account?", &account,
		"id?", &id,
	); err != nil {
		return nil, err
	}

	db := db_user(user, "user")

	// Opportunistic TTL: drop unifiedpush rows that haven't been delivered
	// to in 366 days. We only support local distributors, so a "dead"
	// subscription costs nothing per attempt (websockets_send is a no-op
	// when no client subscribes), but the rows accumulate forever without
	// this. 366 days absorbs leap-year drift — a user pushing exactly once
	// a year on the same calendar date never trips the cleanup. The
	// last_delivered > 0 guard skips freshly-registered subscriptions
	// that haven't received their first push yet.
	db.exec(
		"delete from accounts where type='unifiedpush' and last_delivered > 0 and last_delivered < ?",
		time.Now().Unix()-366*86400,
	)

	// Get verified accounts with notify capability (optionally filtered by account ID)
	var rows []map[string]any
	var err error
	if account > 0 {
		rows, err = db.rows("select id, type, identifier, data from accounts where verified > 0 and id = ?", account)
	} else {
		rows, err = db.rows("select id, type, identifier, data from accounts where verified > 0")
	}
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}

	sent := 0
	failed := 0

	for _, row := range rows {
		account, _ := row["id"].(int64)
		ptype, _ := row["type"].(string)

		// Check if provider has notify capability
		if !provider_has_capability(ptype, "notify") {
			continue
		}

		// Deliver based on type
		identifier, _ := row["identifier"].(string)
		raw, _ := row["data"].(string)
		var data map[string]any
		if raw != "" {
			json.Unmarshal([]byte(raw), &data)
		}

		var success bool
		switch ptype {
		case "browser":
			success = account_deliver_browser(data, title, body, link, app+"-"+category+"-"+object)
		case "email":
			success = account_deliver_email(identifier, title, body, link)
		case "pushbullet":
			token, _ := data["token"].(string)
			success = account_deliver_pushbullet(token, title, body, link)
		case "ntfy":
			server, _ := data["server"].(string)
			topic, _ := data["topic"].(string)
			token, _ := data["token"].(string)
			success = account_deliver_ntfy(server, topic, token, title, body, link)
		case "unifiedpush":
			success = account_deliver_unifiedpush(user, account, data, title, body, link, app+"-"+category+"-"+object, app, id)
		case "fcm":
			var retire bool
			success, retire, _ = account_deliver_fcm(data, title, body, link, app+"-"+category+"-"+object, app, id)
			if !success && retire {
				// Google reported UNREGISTERED / INVALID_ARGUMENT — the
				// token is permanently dead. Drop the row so the next
				// FcmRegistrar.connect from the phone creates a fresh
				// one rather than the upsert resurrecting the dead
				// token. (Same one-strike semantics as browser below.)
				db.exec("delete from accounts where id=?", account)
				failed++
				continue
			}
		case "url":
			secret, _ := data["secret"].(string)
			success = account_deliver_url(identifier, secret, app, category, object, title, body, link)
		default:
			continue
		}

		if success {
			sent++
			// Update last_delivered for TTL sweep
			db.exec("update accounts set last_delivered=? where id=?", time.Now().Unix(), account)
		} else {
			failed++
			// Remove expired browser subscriptions on any failure (longstanding
			// behaviour). For unifiedpush, only the deliver function knows
			// whether the failure was permanent (404/410 → drop) or transient
			// (timeout, 5xx, network → keep) — until that signal is plumbed,
			// keep the row so we don't tear down phones that just temporarily
			// lost reachability. The TTL sweep handles long-term orphans.
			if ptype == "browser" {
				db.exec("delete from accounts where id=?", account)
			}
		}
	}

	return sl_encode(map[string]any{
		"sent":   sent,
		"failed": failed,
	}), nil
}

// account_deliver_browser sends a push notification to a browser
func account_deliver_browser(data map[string]any, title, body, link, tag string) bool {
	webpush_ensure()
	if webpush_public == "" || webpush_private == "" {
		return false
	}

	endpoint, _ := data["endpoint"].(string)
	auth, _ := data["auth"].(string)
	p256dh, _ := data["p256dh"].(string)

	if endpoint == "" || auth == "" || p256dh == "" {
		return false
	}

	payload, _ := json.Marshal(map[string]string{
		"title": title,
		"body":  body,
		"link":  link,
		"tag":   tag,
	})

	sub := webpush.Subscription{
		Endpoint: endpoint,
		Keys: webpush.Keys{
			Auth:   auth,
			P256dh: p256dh,
		},
	}

	resp, err := webpush.SendNotification(payload, &sub, &webpush.Options{
		// Without an explicit client, webpush-go falls back to a bare
		// http.Client with no timeout; a hanging push endpoint then
		// blocks the notifications commit hook until the Starlark
		// execution cap kills it (and the caller's service.call with it).
		HTTPClient:      &http.Client{Timeout: 15 * time.Second},
		Subscriber:      "mailto:webpush@localhost",
		VAPIDPublicKey:  webpush_public,
		VAPIDPrivateKey: webpush_private,
		TTL:             86400,
	})

	if err != nil {
		return false
	}
	resp.Body.Close()

	return resp.StatusCode == 201
}

// account_deliver_unifiedpush sends a notification to a UnifiedPush distributor.
// The endpoint URL belongs to whichever distributor the user picked: it may be
// on this Mochi server (when the user picked our distributor) or on a
// third-party push server like ntfy.sh.
//
// Two paths:
//   - **Local fast-path**: when the endpoint is path-only (starts with "/"),
//     it was synthesised by function_push_register for the Mochi-distributor
//     case. Skip the RFC 8030 wrap entirely and forward the cleartext payload
//     to the device via the existing per-user WebSocket (the Mochi distributor
//     subscribes to the well-known key "unifiedpush"). One in-process hop
//     instead of HTTP self-call + Web Push round-trip.
//   - **Remote (RFC 8030)**: any absolute URL — third-party distributors
//     (ntfy, NextPush, Mozilla autopush). Same code path as browser push.
func account_deliver_unifiedpush(user *User, accountID int64, data map[string]any, title, body, link, tag, app, id string) bool {
	endpoint, _ := data["endpoint"].(string)
	if endpoint == "" {
		return false
	}

	payload, _ := json.Marshal(map[string]string{
		"title": title,
		"body":  body,
		"link":  link,
		"tag":   tag,
		"app":   app,
		"id":    id,
	})

	// Local fast-path: path-only endpoint synthesised by our own register
	// flow. Forward to the Mochi distributor over the existing user WebSocket.
	// The envelope carries `account` (the integer accounts.id) so the on-device
	// distributor can ack the matching push_pending row on receipt — without
	// it, every live event leaves a stuck row until the 7-day TTL sweep.
	if strings.HasPrefix(endpoint, "/") {
		sub_id := endpoint
		if i := strings.LastIndex(endpoint, "/"); i >= 0 {
			sub_id = endpoint[i+1:]
		}
		websockets_send(user, "unifiedpush", map[string]any{
			"sub_id":   sub_id,
			"payload": string(payload),
			"account": accountID,
		})
		return true
	}

	webpush_ensure()
	if webpush_public == "" || webpush_private == "" {
		return false
	}

	auth, _ := data["auth"].(string)
	p256dh, _ := data["p256dh"].(string)
	if auth == "" || p256dh == "" {
		return false
	}

	sub := webpush.Subscription{
		Endpoint: endpoint,
		Keys: webpush.Keys{
			Auth:   auth,
			P256dh: p256dh,
		},
	}

	resp, err := webpush.SendNotification(payload, &sub, &webpush.Options{
		// Bounded client for the same reason as account_deliver_browser.
		HTTPClient:      &http.Client{Timeout: 15 * time.Second},
		Subscriber:      "mailto:webpush@localhost",
		VAPIDPublicKey:  webpush_public,
		VAPIDPrivateKey: webpush_private,
		TTL:             86400,
	})

	if err != nil {
		return false
	}
	resp.Body.Close()

	// 201 Created = success. 404/410 = subscription dead (caller drops the row).
	return resp.StatusCode == 201
}

// account_deliver_email sends a notification via email
func account_deliver_email(address, title, body, link string) bool {
	text := title + "\n\n" + body
	escaped_title := html.EscapeString(title)
	escaped_body := html.EscapeString(body)
	markup := `<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="margin: 0; padding: 0; background-color: #f4f4f5; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;">
  <table role="presentation" width="100%" cellspacing="0" cellpadding="0">
    <tr>
      <td align="center" style="padding: 20px;">
        <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="max-width: 440px; background-color: #ffffff; border-radius: 12px; box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);">
          <tr>
            <td style="padding: 32px 40px;">
              <h1 style="margin: 0 0 16px 0; font-size: 18px; font-weight: 600; color: #18181b;">` + escaped_title + `</h1>
              <p style="margin: 0; font-size: 15px; color: #3f3f46; line-height: 1.5;">` + escaped_body + `</p>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>`
	email_send_multipart(address, title, text, markup)
	return true
}

// account_deliver_pushbullet sends a notification via Pushbullet
func account_deliver_pushbullet(token, title, body, link string) bool {
	if token == "" {
		return false
	}

	payload, _ := json.Marshal(map[string]string{
		"type":  "note",
		"title": title,
		"body":  body,
	})

	req, _ := http.NewRequest("POST", "https://api.pushbullet.com/v2/pushes", bytes.NewReader(payload))
	req.Header.Set("Access-Token", token)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	return resp.StatusCode == 200
}

// account_deliver_ntfy sends a notification via ntfy
func account_deliver_ntfy(server, topic, token, title, body, link string) bool {
	if topic == "" {
		return false
	}
	if server == "" {
		server = "https://ntfy.sh"
	}

	url := server + "/" + topic
	if url_is_cloud_metadata(url) {
		return false
	}
	req, _ := http.NewRequest("POST", url, bytes.NewReader([]byte(body)))
	req.Header.Set("Title", title)
	if link != "" {
		req.Header.Set("Click", link)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	return resp.StatusCode == 200
}

// account_deliver_url sends a notification to an external URL
func account_deliver_url(url, secret, app, category, object, title, body, link string) bool {
	if url == "" || url_is_cloud_metadata(url) {
		return false
	}

	payload, _ := json.Marshal(map[string]any{
		"app":      app,
		"category": category,
		"object":   object,
		"title":    title,
		"body":     body,
		"link":     link,
		"created":  time.Now().Unix(),
	})

	req, _ := http.NewRequest("POST", url, bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Mochi-Event", "notification")

	if secret != "" {
		timestamp := time.Now().Unix()
		signature := account_url_signature(timestamp, payload, secret)
		req.Header.Set("X-Mochi-Timestamp", itoa(int(timestamp)))
		req.Header.Set("X-Mochi-Signature", signature)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	return resp.StatusCode >= 200 && resp.StatusCode < 300
}

// account_url_signature generates HMAC-SHA256 signature for external URL requests
func account_url_signature(timestamp int64, payload []byte, secret string) string {
	message := itoa(int(timestamp)) + "." + string(payload)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(message))
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

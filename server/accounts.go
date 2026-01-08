// Mochi server: Connected accounts
// Copyright Alistair Cunningham 2025
//
// Connected accounts allow users to link external services (email, push notifications,
// AI providers, MCP servers) to their Mochi account. Apps access accounts via the
// mochi.account.* Starlark API. Secrets (API keys, tokens) are never exposed to apps;
// core handles all provider interactions internally.

package main

import (
	"crypto/rand"
	"encoding/json"
	"time"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

// Provider defines an account provider type
type Provider struct {
	Type         string          `json:"type"`
	Label        string          `json:"label"`
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

// providers defines all available account providers
var providers = []Provider{
	{
		Type:         "browser",
		Label:        "Browser notifications",
		Capabilities: []string{"notify"},
		Flow:         "browser",
		Fields:       nil, // handled by JavaScript
		Verify:       false,
	},
	{
		Type:         "email",
		Label:        "Email",
		Capabilities: []string{"notify"},
		Flow:         "form",
		Fields: []ProviderField{
			{Name: "address", Label: "Email address", Type: "email", Required: true, Placeholder: "you@example.com"},
			{Name: "label", Label: "Label", Type: "text", Required: false, Placeholder: "Personal, Work, etc."},
		},
		Verify: true,
	},
	{
		Type:         "pushbullet",
		Label:        "Pushbullet",
		Capabilities: []string{"notify"},
		Flow:         "form",
		Fields: []ProviderField{
			{Name: "token", Label: "Access token", Type: "password", Required: true, Placeholder: ""},
			{Name: "label", Label: "Label", Type: "text", Required: false, Placeholder: ""},
		},
		Verify: false,
	},
	{
		Type:         "claude",
		Label:        "Claude (Anthropic)",
		Capabilities: []string{"ai"},
		Flow:         "form",
		Fields: []ProviderField{
			{Name: "api_key", Label: "API key", Type: "password", Required: true, Placeholder: "sk-ant-..."},
			{Name: "label", Label: "Label", Type: "text", Required: false, Placeholder: ""},
		},
		Verify: false,
	},
	{
		Type:         "openai",
		Label:        "OpenAI",
		Capabilities: []string{"ai"},
		Flow:         "form",
		Fields: []ProviderField{
			{Name: "api_key", Label: "API key", Type: "password", Required: true, Placeholder: "sk-..."},
			{Name: "label", Label: "Label", Type: "text", Required: false, Placeholder: ""},
		},
		Verify: false,
	},
	{
		Type:         "mcp",
		Label:        "MCP server",
		Capabilities: []string{"mcp"},
		Flow:         "form",
		Fields: []ProviderField{
			{Name: "url", Label: "Server URL", Type: "url", Required: true, Placeholder: "https://mcp.example.com"},
			{Name: "token", Label: "Access token", Type: "password", Required: false, Placeholder: ""},
			{Name: "label", Label: "Label", Type: "text", Required: false, Placeholder: ""},
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
	"providers": sl.NewBuiltin("mochi.account.providers", api_account_providers),
	"remove":    sl.NewBuiltin("mochi.account.remove", api_account_remove),
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
	}
}

// mochi.account.providers(capability?) -> list: Get available providers
func api_account_providers(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var capability string
	if len(args) > 0 {
		cap, ok := sl.AsString(args[0])
		if !ok {
			return sl_error(fn, "invalid capability")
		}
		capability = cap
	}

	var result []map[string]any
	var list []Provider

	if capability != "" {
		list = providers_by_capability(capability)
	} else {
		list = providers
	}

	for _, p := range list {
		pm := map[string]any{
			"type":         p.Type,
			"label":        p.Label,
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
	if err := require_permission(t, fn, "account/read"); err != nil {
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

	rows, err := db.rows("select id, type, label, identifier, created, verified from accounts order by created desc")
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}

	var result []map[string]any
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

	if err := require_permission(t, fn, "account/read"); err != nil {
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
	row, err := db.row("select id, type, label, identifier, created, verified from accounts where id=?", id)
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}
	if row == nil {
		return sl.None, nil
	}

	return sl_encode(account_redact(row)), nil
}

// mochi.account.add(type, fields) -> dict: Add an account
func api_account_add(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return sl_error(fn, "syntax: <type: string>, <fields: dict>")
	}

	if err := require_permission(t, fn, "account/manage"); err != nil {
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

	fields := sl_decode_map(args[1])
	if fields == nil {
		return sl_error(fn, "invalid fields")
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
		account_send_verification_email(address, code)

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
			dataJSON, _ := json.Marshal(data)

			db.exec("update accounts set data=?, created=? where id=?", string(dataJSON), now, existing["id"])
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
		apiKey, _ := fields["api_key"].(string)
		data["api_key"] = apiKey

	case "mcp":
		url, _ := fields["url"].(string)
		token, _ := fields["token"].(string)
		identifier = url
		data["token"] = token
	}

	// Serialize data to JSON
	dataJSON := ""
	if len(data) > 0 {
		b, _ := json.Marshal(data)
		dataJSON = string(b)
	}

	verified := int64(0)
	if !provider.Verify {
		verified = now
	}

	// Insert account
	result, err := db.handle.Exec(
		"insert into accounts (type, label, identifier, data, created, verified) values (?, ?, ?, ?, ?, ?)",
		ptype, label, identifier, dataJSON, now, verified,
	)
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}

	id, _ := result.LastInsertId()

	return sl_encode(map[string]any{
		"id":         id,
		"type":       ptype,
		"label":      label,
		"identifier": identifier,
		"created":    now,
		"verified":   verified,
	}), nil
}

// mochi.account.update(id, fields) -> bool: Update an account
func api_account_update(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return sl_error(fn, "syntax: <id: integer>, <fields: dict>")
	}

	if err := require_permission(t, fn, "account/manage"); err != nil {
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

	fields := sl_decode_map(args[1])
	if fields == nil {
		return sl_error(fn, "invalid fields")
	}

	db := db_user(user, "user")

	// Check account exists
	exists, _ := db.exists("select 1 from accounts where id=?", id)
	if !exists {
		return sl.False, nil
	}

	// Only label can be updated for now
	if label, ok := fields["label"].(string); ok {
		db.exec("update accounts set label=? where id=?", label, id)
	}

	return sl.True, nil
}

// mochi.account.remove(id) -> bool: Remove an account
func api_account_remove(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <id: integer>")
	}

	if err := require_permission(t, fn, "account/manage"); err != nil {
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

	db.exec("delete from accounts where id=?", id)
	return sl.True, nil
}

// mochi.account.verify(id, code?) -> bool: Verify an account or resend code
func api_account_verify(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return sl_error(fn, "syntax: <id: integer>, [code: string]")
	}

	if err := require_permission(t, fn, "account/manage"); err != nil {
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
	dataStr, _ := row["data"].(string)

	var data map[string]any
	if dataStr != "" {
		json.Unmarshal([]byte(dataStr), &data)
	}
	if data == nil {
		data = make(map[string]any)
	}

	now := time.Now().Unix()

	if code == "" {
		// Resend verification code
		existingCode, _ := data["code"].(string)
		expires, _ := data["expires"].(float64)

		if existingCode != "" && int64(expires) > now {
			// Reuse existing code
			account_send_verification_email(identifier, existingCode)
		} else {
			// Generate new code
			newCode := account_generate_code(10)
			data["code"] = newCode
			data["expires"] = now + 3600

			dataJSON, _ := json.Marshal(data)
			db.exec("update accounts set data=? where id=?", string(dataJSON), id)

			account_send_verification_email(identifier, newCode)
		}
		return sl.True, nil
	}

	// Verify the code
	storedCode, _ := data["code"].(string)
	expires, _ := data["expires"].(float64)

	if storedCode == "" || code != storedCode {
		return sl.False, nil
	}

	if int64(expires) < now {
		return sl_error(fn, "verification code has expired")
	}

	// Code matches - mark as verified and clear code data
	delete(data, "code")
	delete(data, "expires")

	dataJSON, _ := json.Marshal(data)
	db.exec("update accounts set verified=?, data=? where id=?", now, string(dataJSON), id)

	return sl.True, nil
}

// account_send_verification_email sends a styled HTML email with a verification code
func account_send_verification_email(to string, code string) {
	subject := "Verify your email address"
	html := `<!DOCTYPE html>
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
              <h1 style="margin: 0 0 8px 0; font-size: 24px; font-weight: 600; color: #18181b;">Verification Code</h1>
              <p style="margin: 0; font-size: 15px; color: #71717a;">Enter this code to verify your email address</p>
            </td>
          </tr>
          <tr>
            <td style="padding: 0 40px;">
              <div style="background-color: #f4f4f5; border-radius: 8px; padding: 24px; text-align: center;">
                <span style="font-family: 'SF Mono', Monaco, 'Cascadia Code', monospace; font-size: 32px; font-weight: 600; letter-spacing: 4px; color: #18181b;">` + code + `</span>
              </div>
            </td>
          </tr>
          <tr>
            <td style="padding: 32px 40px 40px 40px; text-align: center;">
              <p style="margin: 0; font-size: 14px; color: #a1a1aa;">This code expires in 1 hour</p>
            </td>
          </tr>
        </table>
        <p style="margin: 24px 0 0 0; font-size: 13px; color: #a1a1aa;">If you didn't request this verification, you can safely ignore this email.</p>
      </td>
    </tr>
  </table>
</body>
</html>`
	email_send_html(to, subject, html)
}

// Mochi server: Passkey/WebAuthn authentication
// Copyright Alistair Cunningham 2025

package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/go-webauthn/webauthn/protocol"
	"github.com/go-webauthn/webauthn/webauthn"
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

var webauthn_instance *webauthn.WebAuthn

var api_user_passkey = sls.FromStringDict(sl.String("mochi.user.passkey"), sl.StringDict{
	"count":  sl.NewBuiltin("mochi.user.passkey.count", api_user_passkey_count),
	"delete": sl.NewBuiltin("mochi.user.passkey.delete", api_user_passkey_delete),
	"list":   sl.NewBuiltin("mochi.user.passkey.list", api_user_passkey_list),
	"rename": sl.NewBuiltin("mochi.user.passkey.rename", api_user_passkey_rename),
	"register": sls.FromStringDict(sl.String("mochi.user.passkey.register"), sl.StringDict{
		"begin":  sl.NewBuiltin("mochi.user.passkey.register.begin", api_user_passkey_register_begin),
		"finish": sl.NewBuiltin("mochi.user.passkey.register.finish", api_user_passkey_register_finish),
	}),
})

// Initialize WebAuthn with server configuration
func passkey_init() {
	domain := ini_string("web", "domain", "localhost")
	origins := []string{"https://" + domain}
	if domain == "localhost" {
		origins = append(origins, "http://localhost", "http://localhost:8080", "http://localhost:8081")
	}

	cfg := &webauthn.Config{
		RPDisplayName: "Mochi",
		RPID:          domain,
		RPOrigins:     origins,
	}

	var err error
	webauthn_instance, err = webauthn.New(cfg)
	if err != nil {
		warn("Failed to initialize WebAuthn: %v", err)
	}
}

// WebAuthnUser adapts User for the go-webauthn library
type WebAuthnUser struct {
	user *User
}

// WebAuthnID returns the user ID as bytes
func (u *WebAuthnUser) WebAuthnID() []byte {
	return []byte(fmt.Sprintf("%d", u.user.ID))
}

// WebAuthnName returns the username (email)
func (u *WebAuthnUser) WebAuthnName() string {
	return u.user.Username
}

// WebAuthnDisplayName returns the user's display name
func (u *WebAuthnUser) WebAuthnDisplayName() string {
	if u.user.Identity != nil && u.user.Identity.Name != "" {
		return u.user.Identity.Name
	}
	return u.user.Username
}

// WebAuthnCredentials returns all credentials for this user
func (u *WebAuthnUser) WebAuthnCredentials() []webauthn.Credential {
	db := db_open("db/users.db")
	rows, _ := db.rows("select id, public_key, sign_count, transports, backup_eligible, backup_state from credentials where user=?", u.user.ID)

	var creds []webauthn.Credential
	for _, row := range rows {
		transports := []protocol.AuthenticatorTransport{}
		if t, ok := row["transports"].(string); ok && t != "" {
			for _, tr := range strings.Split(t, ",") {
				transports = append(transports, protocol.AuthenticatorTransport(tr))
			}
		}

		// Handle blob fields that may come back as string or []byte
		var id, public_key []byte
		switch v := row["id"].(type) {
		case []byte:
			id = v
		case string:
			id = []byte(v)
		}
		switch v := row["public_key"].(type) {
		case []byte:
			public_key = v
		case string:
			public_key = []byte(v)
		}

		creds = append(creds, webauthn.Credential{
			ID:              id,
			PublicKey:       public_key,
			AttestationType: "none",
			Transport:       transports,
			Flags: webauthn.CredentialFlags{
				BackupEligible: row["backup_eligible"].(int64) != 0,
				BackupState:    row["backup_state"].(int64) != 0,
			},
			Authenticator: webauthn.Authenticator{
				SignCount: uint32(row["sign_count"].(int64)),
			},
		})
	}
	return creds
}

// ============================================================================
// System Endpoints (unauthenticated login flows)
// ============================================================================

// POST /_/auth/passkey/begin - Start discoverable login
func web_passkey_login_begin(c *gin.Context) {
	if webauthn_instance == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "webauthn_not_configured"})
		return
	}

	if setting_get("auth_passkey_enabled", "true") != "true" {
		c.JSON(http.StatusForbidden, gin.H{"error": "passkey_disabled"})
		return
	}

	options, session, err := webauthn_instance.BeginDiscoverableLogin()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "webauthn_error"})
		return
	}

	// Store ceremony session
	ceremony := random_alphanumeric(32)
	data, _ := json.Marshal(session)
	db := db_open("db/sessions.db")
	db.exec("insert into ceremonies (id, type, challenge, data, expires) values (?, 'login', ?, ?, ?)",
		ceremony, session.Challenge, string(data), now()+300)

	c.JSON(http.StatusOK, gin.H{
		"options":  options.Response,
		"ceremony": ceremony,
	})
}

// POST /_/auth/passkey/finish - Complete discoverable login
func web_passkey_login_finish(c *gin.Context) {
	if webauthn_instance == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "webauthn_not_configured"})
		return
	}

	if setting_get("auth_passkey_enabled", "true") != "true" {
		c.JSON(http.StatusForbidden, gin.H{"error": "passkey_disabled"})
		return
	}

	// Read raw body first since we need to parse it twice
	body, err := c.GetRawData()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_request"})
		return
	}

	// Extract ceremony from request
	var input struct {
		Ceremony string `json:"ceremony"`
	}
	if err := json.Unmarshal(body, &input); err != nil || input.Ceremony == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_request"})
		return
	}

	// Load ceremony session
	db := db_open("db/sessions.db")
	row, _ := db.row("select data from ceremonies where id=? and type='login' and expires>?",
		input.Ceremony, now())
	if row == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "ceremony_expired"})
		return
	}

	// Delete ceremony session
	db.exec("delete from ceremonies where id=?", input.Ceremony)

	var session webauthn.SessionData
	json.Unmarshal([]byte(row["data"].(string)), &session)

	// Parse the credential response from the request body
	parsed, err := protocol.ParseCredentialRequestResponseBody(strings.NewReader(string(body)))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_credential"})
		return
	}

	// Handler to find user from credential
	handler := func(rawID, userHandle []byte) (webauthn.User, error) {
		user_id := int(atoi(string(userHandle), 0))
		user := user_by_id(user_id)
		if user == nil {
			return nil, errors.New("user not found")
		}
		return &WebAuthnUser{user: user}, nil
	}

	credential, err := webauthn_instance.ValidateDiscoverableLogin(handler, session, parsed)
	if err != nil {
		info("Passkey login failed: %v", err)
		c.JSON(http.StatusUnauthorized, gin.H{"error": "authentication_failed"})
		return
	}

	// Update credential usage
	users := db_open("db/users.db")
	users.exec("update credentials set sign_count=?, last_used=? where id=?",
		credential.Authenticator.SignCount, now(), credential.ID)

	// Find user from credential
	row, _ = users.row("select user from credentials where id=?", credential.ID)
	if row == nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "credential_not_found"})
		return
	}
	user := user_by_id(int(row["user"].(int64)))
	if user == nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "user_not_found"})
		return
	}
	if user.Status == "suspended" {
		c.JSON(http.StatusForbidden, gin.H{"error": "suspended", "message": "Your account has been suspended."})
		return
	}

	// Reset rate limit on successful login
	rate_limit_login.reset(rate_limit_client_ip(c))

	// Check for remaining MFA methods
	remaining := auth_remaining_methods(user, "passkey")
	if len(remaining) > 0 {
		// If email is required, send the code now
		for _, method := range remaining {
			if method == "email" {
				code_send(user.Username)
				break
			}
		}

		// Create partial session
		partial := random_alphanumeric(32)
		db.exec("insert into partial (id, user, completed, remaining, expires) values (?, ?, 'passkey', ?, ?)",
			partial, user.ID, strings.Join(remaining, ","), now()+300)
		c.JSON(http.StatusOK, gin.H{
			"mfa":       true,
			"partial":   partial,
			"remaining": remaining,
		})
		return
	}

	// Create full session
	auth_complete_login(c, user)
}

// ============================================================================
// Starlark APIs (authenticated passkey management)
// ============================================================================

// mochi.user.passkey.list() -> list: List user's passkeys
func api_user_passkey_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	db := db_open("db/users.db")
	rows, err := db.rows("select id, name, transports, created, last_used from credentials where user=? order by created desc", user.ID)
	if err != nil {
		return sl_error(fn, "database error")
	}

	// Convert blob IDs to base64 for Starlark
	credentials := make([]map[string]any, len(rows))
	for i, row := range rows {
		// Handle ID as either []byte or string (SQLite driver may return either)
		var idBase64 string
		switch id := row["id"].(type) {
		case []byte:
			idBase64 = base64.URLEncoding.EncodeToString(id)
		case string:
			idBase64 = base64.URLEncoding.EncodeToString([]byte(id))
		}
		credentials[i] = map[string]any{
			"id":         idBase64,
			"name":       row["name"],
			"transports": row["transports"],
			"created":    row["created"],
			"last_used":  row["last_used"],
		}
	}

	return sl_encode(credentials), nil
}

// mochi.user.passkey.count() -> int: Count user's passkeys
func api_user_passkey_count(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	db := db_open("db/users.db")
	row, _ := db.row("select count(*) as count from credentials where user=?", user.ID)
	if row == nil {
		return sl.MakeInt(0), nil
	}
	return sl.MakeInt(int(row["count"].(int64))), nil
}

// mochi.user.passkey.register.begin() -> dict: Start passkey registration
func api_user_passkey_register_begin(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if webauthn_instance == nil {
		return sl_error(fn, "webauthn not configured")
	}

	if setting_get("auth_passkey_enabled", "true") != "true" {
		return sl_error(fn, "passkey disabled")
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	wu := &WebAuthnUser{user: user}
	options, session, err := webauthn_instance.BeginRegistration(wu,
		webauthn.WithResidentKeyRequirement(protocol.ResidentKeyRequirementRequired),
	)
	if err != nil {
		return sl_error(fn, "webauthn error: %v", err)
	}

	// Store ceremony session
	ceremony := random_alphanumeric(32)
	data, _ := json.Marshal(session)
	db := db_open("db/sessions.db")
	db.exec("insert into ceremonies (id, type, user, challenge, data, expires) values (?, 'register', ?, ?, ?, ?)",
		ceremony, user.ID, session.Challenge, string(data), now()+300)

	return sl_encode(map[string]any{
		"options":  options.Response,
		"ceremony": ceremony,
	}), nil
}

// mochi.user.passkey.register.finish(ceremony, credential, name?) -> dict: Complete registration
func api_user_passkey_register_finish(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if webauthn_instance == nil {
		return sl_error(fn, "webauthn not configured")
	}

	if setting_get("auth_passkey_enabled", "true") != "true" {
		return sl_error(fn, "passkey disabled")
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	if len(args) < 2 {
		return sl_error(fn, "syntax: <ceremony: string>, <credential: dict>, [name: string]")
	}

	ceremony, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid ceremony")
	}

	// The credential can be a JSON string or a Starlark dict
	var credential_json string
	switch cred := args[1].(type) {
	case sl.String:
		credential_json = string(cred)
	case *sl.Dict:
		body, err := starlark_to_json(cred)
		if err != nil {
			return sl_error(fn, "invalid credential format")
		}
		credential_json = string(body)
	default:
		return sl_error(fn, "invalid credential")
	}

	name := ""
	if len(args) > 2 {
		name, _ = sl.AsString(args[2])
	}

	// Load ceremony session
	db := db_open("db/sessions.db")
	row, _ := db.row("select data from ceremonies where id=? and type='register' and user=? and expires>?",
		ceremony, user.ID, now())
	if row == nil {
		return sl_error(fn, "ceremony expired")
	}

	// Delete ceremony session
	db.exec("delete from ceremonies where id=?", ceremony)

	var session webauthn.SessionData
	json.Unmarshal([]byte(row["data"].(string)), &session)

	// Parse the credential response
	parsed, err := protocol.ParseCredentialCreationResponseBody(strings.NewReader(credential_json))
	if err != nil {
		return sl_error(fn, "invalid credential: %v", err)
	}

	wu := &WebAuthnUser{user: user}
	credential, err := webauthn_instance.CreateCredential(wu, session, parsed)
	if err != nil {
		return sl_error(fn, "registration failed: %v", err)
	}

	// Build transports string
	transports := ""
	for i, tr := range credential.Transport {
		if i > 0 {
			transports += ","
		}
		transports += string(tr)
	}

	// Default name if not provided
	if name == "" {
		name = "Passkey"
	}

	// Store credential
	users := db_open("db/users.db")
	users.exec(`insert into credentials (id, user, public_key, sign_count, name, transports, backup_eligible, backup_state, created, last_used)
             values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		credential.ID, user.ID, credential.PublicKey,
		credential.Authenticator.SignCount, name, transports,
		credential.Flags.BackupEligible, credential.Flags.BackupState, now(), 0)

	audit_password_changed(user.Username, "passkey_registered")
	return sl_encode(map[string]any{"status": "ok", "name": name}), nil
}

// mochi.user.passkey.rename(id, name) -> bool: Rename a passkey
func api_user_passkey_rename(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	if len(args) != 2 {
		return sl_error(fn, "syntax: <id: string>, <name: string>")
	}

	encoded, ok := sl.AsString(args[0])
	if !ok || encoded == "" {
		return sl_error(fn, "invalid id")
	}

	name, ok := sl.AsString(args[1])
	if !ok || name == "" {
		return sl_error(fn, "invalid name")
	}

	id, err := base64.URLEncoding.DecodeString(encoded)
	if err != nil {
		return sl_error(fn, "invalid id encoding")
	}

	db := db_open("db/users.db")
	exists, _ := db.exists("select id from credentials where id=? and user=?", id, user.ID)
	if !exists {
		return sl_error(fn, "credential not found")
	}

	db.exec("update credentials set name=? where id=? and user=?", name, id, user.ID)
	return sl.True, nil
}

// mochi.user.passkey.delete(id) -> bool: Delete a passkey
func api_user_passkey_delete(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	if len(args) != 1 {
		return sl_error(fn, "syntax: <id: string>")
	}

	encoded, ok := sl.AsString(args[0])
	if !ok || encoded == "" {
		return sl_error(fn, "invalid id")
	}

	id, err := base64.URLEncoding.DecodeString(encoded)
	if err != nil {
		return sl_error(fn, "invalid id encoding")
	}

	db := db_open("db/users.db")

	// Check if this would leave user without passkey when passkey is required
	if strings.Contains(user.Methods, "passkey") {
		row, _ := db.row("select count(*) as count from credentials where user=?", user.ID)
		if row != nil && row["count"].(int64) <= 1 {
			return sl_error(fn, "cannot delete last passkey while passkey authentication is required")
		}
	}

	exists, _ := db.exists("select id from credentials where id=? and user=?", id, user.ID)
	if !exists {
		return sl_error(fn, "credential not found")
	}

	db.exec("delete from credentials where id=? and user=?", id, user.ID)
	audit_password_changed(user.Username, "passkey_deleted")
	return sl.True, nil
}

// ============================================================================
// Helper functions
// ============================================================================

// starlark_to_json converts a Starlark dict to JSON bytes
func starlark_to_json(v sl.Value) ([]byte, error) {
	switch x := v.(type) {
	case *sl.Dict:
		m := make(map[string]any)
		for _, item := range x.Items() {
			key, _ := sl.AsString(item[0])
			val, err := starlark_to_go(item[1])
			if err != nil {
				return nil, err
			}
			m[key] = val
		}
		return json.Marshal(m)
	default:
		return nil, fmt.Errorf("expected dict, got %s", v.Type())
	}
}

// starlark_to_go converts a Starlark value to a Go value
func starlark_to_go(v sl.Value) (any, error) {
	switch x := v.(type) {
	case sl.NoneType:
		return nil, nil
	case sl.Bool:
		return bool(x), nil
	case sl.Int:
		i, _ := x.Int64()
		return i, nil
	case sl.Float:
		return float64(x), nil
	case sl.String:
		return string(x), nil
	case *sl.List:
		arr := make([]any, x.Len())
		for i := 0; i < x.Len(); i++ {
			val, err := starlark_to_go(x.Index(i))
			if err != nil {
				return nil, err
			}
			arr[i] = val
		}
		return arr, nil
	case *sl.Dict:
		m := make(map[string]any)
		for _, item := range x.Items() {
			key, _ := sl.AsString(item[0])
			val, err := starlark_to_go(item[1])
			if err != nil {
				return nil, err
			}
			m[key] = val
		}
		return m, nil
	default:
		return nil, fmt.Errorf("unsupported type: %s", v.Type())
	}
}

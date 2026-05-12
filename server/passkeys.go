// Mochi server: Passkey/WebAuthn authentication
// Copyright Alistair Cunningham 2025-2026

package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/go-webauthn/webauthn/protocol"
	"github.com/go-webauthn/webauthn/webauthn"
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

var (
	webauthn_instances map[string]*webauthn.WebAuthn
	webauthn_mu        sync.RWMutex
)

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

// Initialize the WebAuthn instance cache
func passkey_init() {
	webauthn_instances = make(map[string]*webauthn.WebAuthn)
}

// Return a WebAuthn instance for the given request host, creating one if needed
func webauthn_for_host(host string) *webauthn.WebAuthn {
	// Strip port from host
	domain := host
	if i := strings.IndexByte(domain, ':'); i != -1 {
		domain = domain[:i]
	}

	webauthn_mu.RLock()
	instance := webauthn_instances[domain]
	webauthn_mu.RUnlock()
	if instance != nil {
		return instance
	}

	origins := []string{"https://" + domain}
	if domain == "localhost" {
		origins = append(origins, "http://localhost", "http://localhost:8080", "http://localhost:8081")
	}

	instance, err := webauthn.New(&webauthn.Config{
		RPDisplayName: "Mochi",
		RPID:          domain,
		RPOrigins:     origins,
	})
	if err != nil {
		warn("Failed to initialize WebAuthn for %s: %v", domain, err)
		return nil
	}

	webauthn_mu.Lock()
	webauthn_instances[domain] = instance
	webauthn_mu.Unlock()
	return instance
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

// passkey_lasts returns last-used by credential id (as string of bytes) for
// every passkey registered to this user. Unknown credentials map to 0.
func passkey_lasts(user_id int) map[string]int64 {
	out := map[string]int64{}
	rows, err := db_open("db/sessions.db").rows("select credential, last from passkeys where user=?", user_id)
	if err != nil {
		return out
	}
	for _, r := range rows {
		var key []byte
		switch v := r["credential"].(type) {
		case []byte:
			key = v
		case string:
			key = []byte(v)
		}
		last, _ := r["last"].(int64)
		out[string(key)] = last
	}
	return out
}

// ============================================================================
// System Endpoints (unauthenticated login flows)
// ============================================================================

// POST /_/auth/passkey/begin - Start discoverable login
func web_passkey_login_begin(c *gin.Context) {
	wa := webauthn_for_host(c.Request.Host)
	if wa == nil {
		respond_error(c, http.StatusInternalServerError, "webauthn_not_configured", "errors.webauthn_not_configured", nil)
		return
	}

	if !auth_method_allowed("passkey") {
		respond_error(c, http.StatusForbidden, "passkey_disabled", "errors.passkey_disabled", nil)
		return
	}

	options, session, err := wa.BeginDiscoverableLogin()
	if err != nil {
		respond_error(c, http.StatusInternalServerError, "webauthn_error", "errors.webauthn_error", nil)
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
	wa := webauthn_for_host(c.Request.Host)
	if wa == nil {
		respond_error(c, http.StatusInternalServerError, "webauthn_not_configured", "errors.webauthn_not_configured", nil)
		return
	}

	if !auth_method_allowed("passkey") {
		respond_error(c, http.StatusForbidden, "passkey_disabled", "errors.passkey_disabled", nil)
		return
	}

	// Read raw body first since we need to parse it twice
	body, err := c.GetRawData()
	if err != nil {
		respond_error(c, http.StatusBadRequest, "invalid_request", "errors.invalid_request", nil)
		return
	}

	// Extract ceremony from request
	var input struct {
		Ceremony string `json:"ceremony"`
	}
	if err := json.Unmarshal(body, &input); err != nil || input.Ceremony == "" {
		respond_error(c, http.StatusBadRequest, "invalid_request", "errors.invalid_request", nil)
		return
	}

	// Load ceremony session
	db := db_open("db/sessions.db")
	row, _ := db.row("select data from ceremonies where id=? and type='login' and expires>?",
		input.Ceremony, now())
	if row == nil {
		respond_error(c, http.StatusBadRequest, "ceremony_expired", "errors.ceremony_expired", nil)
		return
	}

	// Delete ceremony session
	db.exec("delete from ceremonies where id=?", input.Ceremony)

	var session webauthn.SessionData
	json.Unmarshal([]byte(row["data"].(string)), &session)

	// Parse the credential response from the request body
	parsed, err := protocol.ParseCredentialRequestResponseBody(strings.NewReader(string(body)))
	if err != nil {
		respond_error(c, http.StatusBadRequest, "invalid_credential", "errors.invalid_credential", nil)
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

	credential, err := wa.ValidateDiscoverableLogin(handler, session, parsed)
	if err != nil {
		info("Passkey login failed: %v", err)
		respond_error(c, http.StatusUnauthorized, "authentication_failed", "errors.authentication_failed", nil)
		return
	}

	// Find user from credential first (we need the user id for the passkeys
	// upsert below, and an unknown credential should short-circuit anyway).
	users := db_open("db/users.db")
	row, _ = users.row("select user from credentials where id=?", credential.ID)
	if row == nil {
		respond_error(c, http.StatusUnauthorized, "credential_not_found", "errors.credential_not_found", nil)
		return
	}
	user := user_by_id(int(row["user"].(int64)))
	if user == nil {
		respond_error(c, http.StatusUnauthorized, "user_not_found", "errors.user_not_found", nil)
		return
	}

	// Update credential usage. Sign count (replay-prevention state) goes to
	// users.db so it survives sessions.db corruption; cosmetic last-used to
	// sessions.db so per-assertion stat writes don't touch the cold store.
	// Upsert so the row self-heals if sessions.db was wiped.
	users.exec("update credentials set sign_count=? where id=?",
		credential.Authenticator.SignCount, credential.ID)
	db_open("db/sessions.db").exec("insert into passkeys (credential, user, last) values (?, ?, ?) on conflict(credential) do update set last=excluded.last",
		credential.ID, user.ID, now())

	// Per-(user, credential) leadership claim. The host that processed
	// this assertion takes a 60s lease on `("credential", <id>)`; another
	// host in the user's set that receives a concurrent assertion before
	// the lease expires would lose its claim and (once the fence-aware
	// op-rejection path lands) drop the conflicting sign_count update.
	// For now the claim is informational — the existing sign_count
	// integer remains authoritative — but the leadership row is in place
	// so cross-host coordination can be turned on without touching this
	// site again. See claude/plans/replication.md pattern 1.4.
	replication_leader_claim("credential", base64.StdEncoding.EncodeToString(credential.ID))
	if user.Status == "suspended" {
		respond_error(c, http.StatusForbidden, "suspended", "errors.suspended", nil)
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
				code_send(user.Username, c)
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
	if err := require_permission(t, fn, "user/authentication/read"); err != nil {
		return sl_error(fn, "%v", err)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	db := db_open("db/users.db")
	rows, err := db.rows("select id, name, transports, created from credentials where user=? order by created desc", user.ID)
	if err != nil {
		return sl_error(fn, "database error")
	}

	lasts := passkey_lasts(user.ID)

	// Convert blob IDs to base64 for Starlark
	credentials := make([]map[string]any, len(rows))
	for i, row := range rows {
		// Handle ID as either []byte or string (SQLite driver may return either)
		var idBytes []byte
		switch id := row["id"].(type) {
		case []byte:
			idBytes = id
		case string:
			idBytes = []byte(id)
		}
		credentials[i] = map[string]any{
			"id":         base64.URLEncoding.EncodeToString(idBytes),
			"name":       row["name"],
			"transports": row["transports"],
			"created":    row["created"],
			"last_used":  lasts[string(idBytes)],
		}
	}

	return sl_encode(credentials), nil
}

// mochi.user.passkey.count() -> int: Count user's passkeys
func api_user_passkey_count(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := require_permission(t, fn, "user/authentication/read"); err != nil {
		return sl_error(fn, "%v", err)
	}

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
	if err := require_permission(t, fn, "user/authentication/write"); err != nil {
		return sl_error(fn, "%v", err)
	}

	host, _ := t.Local("host").(string)
	wa := webauthn_for_host(host)
	if wa == nil {
		return sl_error(fn, "webauthn not configured")
	}

	if !auth_method_allowed("passkey") {
		return sl_error(fn, "passkey disabled")
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	wu := &WebAuthnUser{user: user}
	options, session, err := wa.BeginRegistration(wu,
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
	if err := require_permission(t, fn, "user/authentication/write"); err != nil {
		return sl_error(fn, "%v", err)
	}

	host, _ := t.Local("host").(string)
	wa := webauthn_for_host(host)
	if wa == nil {
		return sl_error(fn, "webauthn not configured")
	}

	if !auth_method_allowed("passkey") {
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
	credential, err := wa.CreateCredential(wu, session, parsed)
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

	// Store credential + sign count (replay-prevention state) in users.db so
	// it survives sessions.db corruption. The cosmetic last-used row goes to
	// sessions.db, populated lazily on first assertion.
	users := db_open("db/users.db")
	users.exec(`insert into credentials (id, user, public_key, sign_count, name, transports, backup_eligible, backup_state, created)
             values (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		credential.ID, user.ID, credential.PublicKey,
		credential.Authenticator.SignCount, name, transports,
		credential.Flags.BackupEligible, credential.Flags.BackupState, now())
	db_open("db/sessions.db").exec("insert into passkeys (credential, user, last) values (?, ?, 0)",
		credential.ID, user.ID)

	audit_password_changed(user.Username, "passkey_registered")
	return sl_encode(map[string]any{"status": "ok", "name": name}), nil
}

// mochi.user.passkey.rename(id, name) -> bool: Rename a passkey
func api_user_passkey_rename(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := require_permission(t, fn, "user/authentication/write"); err != nil {
		return sl_error(fn, "%v", err)
	}

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
	if err := require_permission(t, fn, "user/authentication/write"); err != nil {
		return sl_error(fn, "%v", err)
	}

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
	db_open("db/sessions.db").exec("delete from passkeys where credential=?", id)
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

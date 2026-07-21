// Mochi authentication and login
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/pquerna/otp"
	"github.com/pquerna/otp/totp"
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
	"golang.org/x/crypto/bcrypt"
)

const recovery_code_count = 10

var (
	jwt_expiry = int64(365 * 86400) // 1 year, matching session cookie lifetime
)

var api_user_recovery = sls.FromStringDict(sl.String("mochi.user.recovery"), sl.StringDict{
	"count":    sl.NewBuiltin("mochi.user.recovery.count", api_user_recovery_count),
	"generate": sl.NewBuiltin("mochi.user.recovery.generate", api_user_recovery_generate),
})

var api_user_totp = sls.FromStringDict(sl.String("mochi.user.totp"), sl.StringDict{
	"disable": sl.NewBuiltin("mochi.user.totp.disable", api_user_totp_disable),
	"enabled": sl.NewBuiltin("mochi.user.totp.enabled", api_user_totp_enabled),
	"setup":   sl.NewBuiltin("mochi.user.totp.setup", api_user_totp_setup),
	"verify":  sl.NewBuiltin("mochi.user.totp.verify", api_user_totp_verify),
})

type mochi_claims struct {
	User string `json:"user"`
	App  string `json:"app,omitempty"`
	jwt.RegisteredClaims
}

// Exchange a login code for a JWT token and login cookie
func web_login_verify(c *gin.Context) {
	var body struct {
		Code string `json:"code"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		respond_error(c, http.StatusBadRequest, "invalid_request", "errors.invalid_request", nil)
		return
	}
	if body.Code == "" {
		respond_error(c, http.StatusBadRequest, "missing_code", "errors.missing_code", nil)
		return
	}
	user, reason := user_from_code(body.Code)
	if user == nil {
		audit_login_failed("", rate_limit_client_ip(c), reason)
		switch reason {
		case "signup_disabled":
			respond_error(c, http.StatusForbidden, "signup_disabled", "errors.signup_disabled", nil)
		case "suspended":
			respond_error(c, http.StatusForbidden, "suspended", "errors.suspended", nil)
		default:
			respond_error(c, http.StatusUnauthorized, "invalid_code", "errors.invalid_code", nil)
		}
		return
	}

	// Refuse if the user has turned email codes off as a login factor.
	// New signups never have email disabled, so this only blocks an
	// existing account that explicitly disabled it.
	if user_method_disabled(user, "email") {
		audit_login_failed(user.Username, rate_limit_client_ip(c), "email_disabled")
		respond_error(c, http.StatusUnauthorized, "invalid_code", "errors.invalid_code", nil)
		return
	}

	// Check for remaining MFA methods, folding this factor into any pending
	// partial (a passkey- or OAuth-first flow) rather than starting over.
	partial, remaining := partial_continue(c, user, "email")
	if len(remaining) > 0 {
		c.JSON(http.StatusOK, gin.H{
			"mfa":       true,
			"partial":   partial,
			"remaining": remaining,
		})
		return
	}

	// No MFA required - create full session
	auth_complete_login(c, user)
}

// auth_method_state returns the configured state of a login method: one of
// "required", "allowed", or "disabled". Reads the per-method setting (e.g.
// auth_email, auth_passkey) with the appropriate default. Only email can be
// required server-wide (it's the one method every account always has); a
// legacy "required" stored on any other method is treated as "allowed", since
// requiring a credential a user may not have would lock them out.
func auth_method_state(method string) string {
	state := setting_get("auth_"+method, "allowed")
	if method != "email" && state == "required" {
		return "allowed"
	}
	return state
}

// auth_method_allowed reports whether a login method is usable at all — i.e.
// its state is not "disabled".
func auth_method_allowed(method string) bool {
	return auth_method_state(method) != "disabled"
}

// auth_methods_required_list returns the list of methods every user must have
// configured. Recovery is excluded even if somehow set to "required" because
// its setting is allowed|disabled only.
func auth_methods_required_list() []string {
	var required []string
	for _, m := range []string{"email", "passkey", "totp", "oauth"} {
		if auth_method_state(m) == "required" {
			required = append(required, m)
		}
	}
	return required
}

// auth_methods_allowed_list returns the list of methods that are not disabled.
func auth_methods_allowed_list() []string {
	var allowed []string
	for _, m := range []string{"email", "passkey", "totp", "recovery", "oauth"} {
		if auth_method_allowed(m) {
			allowed = append(allowed, m)
		}
	}
	return allowed
}

// auth_remaining_methods returns the factors still required after completing
// the given method. The effective required set is the user's required methods
// plus email when the operator requires it server-wide — email is always
// available so it's always enforceable, and it's the only method that can be
// system-required, so it's the only policy addition here. Returned in
// canonical order.
func auth_remaining_methods(user *User, completed string) []string {
	return auth_remaining_after(user, map[string]bool{completed: true})
}

// auth_remaining_after returns the required factors still outstanding once
// every method in completed is done: the user's own required factors plus the
// system email floor, minus completed, in canonical order.
func auth_remaining_after(user *User, completed map[string]bool) []string {
	required := methods_parse(user.Methods)
	if auth_method_state("email") == "required" {
		required["email"] = true
	}

	var remaining []string
	for _, m := range auth_method_list {
		if required[m] && !completed[m] {
			remaining = append(remaining, m)
		}
	}
	return remaining
}

// auth_remaining_oauth returns the factors still required after an OAuth
// login. OAuth proves control of a linked third-party account, not the
// account's email inbox (the OAuth address may differ from the account
// email), so it satisfies no required factor: the user must still complete
// every method they require. OAuth alone signs in only when nothing is
// required (an all-allowed account, where any one factor suffices).
func auth_remaining_oauth(user *User) []string {
	return auth_remaining_methods(user, "oauth")
}

// auth_establish_session does the shared work of creating a login session: load
// identity if missing, create per-device session with its JWT secret, set the
// browser cookie, and audit the success. Used by both JSON and redirect finish
// paths.
func auth_establish_session(c *gin.Context, user *User) {
	if user != nil && user.Identity == nil {
		user.Identity = user.identity()
	}
	// A full login clears the per-IP counter. The per-account throttle is
	// settled by each guessable factor's own handler (account_login.done), so
	// there is nothing to clear here — and clearing would delete the entry out
	// from under any concurrent in-flight reservations.
	rate_limit_login.reset(rate_limit_client_ip(c))
	session := login_create(user.UID, c.ClientIP(), c.GetHeader("User-Agent"))
	web_cookie_set(c, "session", session)
	db_open("db/sessions.db").exec("replace into logins (user, last) values (?, ?)", user.UID, now())
	audit_login(user.Username, rate_limit_client_ip(c))
}

// auth_complete_login creates a full session and returns session info as JSON.
// Used by XHR-based login endpoints (email code, passkey, TOTP, MFA).
func auth_complete_login(c *gin.Context, user *User) {
	auth_establish_session(c, user)

	response := gin.H{
		"has_identity": user.Identity != nil && user.Identity.Name != "",
	}

	if user.Identity != nil && user.Identity.Name != "" {
		response["name"] = user.Identity.Name
	}

	c.JSON(http.StatusOK, response)
}

// redirect_local returns target only if it is a safe same-site relative path,
// otherwise "". The post-login redirect target is client-supplied (the OAuth
// begin body), so without this check an attacker could point the redirect at
// an external origin (open redirect / phishing) once the session is
// established. A single leading "/" that is not "//" or "/\" keeps it on this
// origin; anything else falls back to the caller's default.
func redirect_local(target string) string {
	if strings.HasPrefix(target, "/") && !strings.HasPrefix(target, "//") && !strings.HasPrefix(target, "/\\") {
		return target
	}
	return ""
}

// auth_redirect_login creates a full session and redirects the browser to the
// given target. Used by OAuth callback flows where the response is a browser
// redirect rather than an XHR JSON body.
func auth_redirect_login(c *gin.Context, user *User, target string) {
	auth_establish_session(c, user)
	target = redirect_local(target)
	if target == "" {
		if user.Identity == nil || user.Identity.Name == "" {
			target = "/login/identity"
		} else {
			target = "/"
		}
	}
	c.Redirect(http.StatusFound, target)
}

// auth_create_app_token creates an app-scoped JWT for a session
func auth_create_app_token(user_uid string, login string, app string) string {
	var s Session
	db := db_open("db/sessions.db")
	if !db.scan(&s, "select * from sessions where code=? and expires>=?", login, now()) {
		return ""
	}

	if s.Secret == "" {
		return ""
	}

	claims := mochi_claims{
		User: user_uid,
		App:  app,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Unix(now()+jwt_expiry, 0)),
			IssuedAt:  jwt.NewNumericDate(time.Unix(now(), 0)),
		},
	}
	t := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	t.Header["kid"] = login
	signed, err := t.SignedString([]byte(s.Secret))
	if err != nil {
		return ""
	}
	return signed
}

// GET /_/auth/methods - Get enabled auth methods for the system
func web_auth_methods(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"email":    auth_method_allowed("email"),
		"passkey":  auth_method_allowed("passkey"),
		"recovery": auth_method_allowed("recovery"),
		"signup":   setting_signup_enabled(),
		"oauth": gin.H{
			"google":    oauth_enabled("google"),
			"github":    oauth_enabled("github"),
			"microsoft": oauth_enabled("microsoft"),
			"facebook":  oauth_enabled("facebook"),
			"x":         oauth_enabled("x"),
		},
	})
}

// POST /_/auth/totp - Verify TOTP code for a user (initial login, not MFA)
// Used when a user has TOTP as their only or first auth method
func web_auth_totp(c *gin.Context) {
	var input struct {
		Email string `json:"email"`
		Code  string `json:"code"`
	}
	if err := c.ShouldBindJSON(&input); err != nil || input.Email == "" || input.Code == "" {
		respond_error(c, http.StatusBadRequest, "invalid_request", "errors.invalid_request", nil)
		return
	}

	user := user_by_username(input.Email)
	if user == nil {
		audit_login_failed(input.Email, rate_limit_client_ip(c), "user_not_found")
		respond_error(c, http.StatusUnauthorized, "invalid_credentials", "errors.invalid_credentials", nil)
		return
	}
	if user.Status == "suspended" {
		audit_login_failed(input.Email, rate_limit_client_ip(c), "suspended")
		respond_error(c, http.StatusForbidden, "suspended", "errors.suspended", nil)
		return
	}

	// Refuse if the user has turned the authenticator off as a login factor.
	if user_method_disabled(user, "totp") {
		audit_login_failed(input.Email, rate_limit_client_ip(c), "totp_disabled")
		respond_error(c, http.StatusUnauthorized, "invalid_credentials", "errors.invalid_credentials", nil)
		return
	}

	// Six-digit codes are guessable and the per-IP limiter alone is defeated
	// by rotating source addresses, so throttle per account (see account_gate).
	if !account_gate_guard(c, user.UID) {
		return
	}
	verified := false
	defer func() { account_login.done(user.UID, verified) }()

	// Verify TOTP code
	if !totp_verify(user.UID, input.Code) {
		audit_login_failed(input.Email, rate_limit_client_ip(c), "invalid_totp")
		respond_error(c, http.StatusUnauthorized, "invalid_code", "errors.invalid_code", nil)
		return
	}
	verified = true

	// Check for remaining MFA methods after TOTP, folding this factor into
	// any pending partial rather than starting over.
	partial, remaining := partial_continue(c, user, "totp")
	if len(remaining) > 0 {
		// If email is required, send the code now
		for _, method := range remaining {
			if method == "email" {
				code_send(user.Username, c)
				break
			}
		}
		c.JSON(http.StatusOK, gin.H{
			"mfa":       true,
			"partial":   partial,
			"remaining": remaining,
		})
		return
	}

	// TOTP was the only required method - complete login
	// Load identity for the response
	user.Identity = user.identity()
	auth_complete_login(c, user)
}

// Verify a JWT and return the user id and app claim, or -1 if invalid.
// If the token header contains a "kid" referencing a login code, attempt to verify
// using that login's secret. Otherwise fall back to the global secret.
func jwt_verify(token_string string) (string, string, error) {
	// First parse the token without verification to read header/kid
	token, _, err := new(jwt.Parser).ParseUnverified(token_string, &mochi_claims{})
	if err != nil {
		return "", "", err
	}

	// Require kid header (login code) to look up per-login secret
	kid, ok := token.Header["kid"].(string)
	if !ok || kid == "" {
		return "", "", errors.New("token missing kid header referencing login code")
	}
	var s Session
	db := db_open("db/sessions.db")
	if !db.scan(&s, "select * from sessions where code=? and expires>=?", kid, now()) {
		return "", "", errors.New("session not found for kid")
	}
	if s.Secret == "" {
		return "", "", errors.New("session has no secret")
	}
	secret := []byte(s.Secret)
	var claims mochi_claims
	tkn, err := jwt.ParseWithClaims(token_string, &claims, func(token *jwt.Token) (interface{}, error) {
		if token.Method != jwt.SigningMethodHS256 {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return secret, nil
	})
	if err != nil {
		return "", "", err
	}
	if !tkn.Valid {
		return "", "", errors.New("invalid token")
	}
	return claims.User, claims.App, nil
}

// ============================================================================
// Multi-factor authentication
// ============================================================================

var api_user_methods = sls.FromStringDict(sl.String("mochi.user.methods"), sl.StringDict{
	"get":       sl.NewBuiltin("mochi.user.methods.get", api_user_methods_get),
	"states":    sl.NewBuiltin("mochi.user.methods.states", api_user_methods_states),
	"set":       sl.NewBuiltin("mochi.user.methods.set", api_user_methods_set),
	"configure": sl.NewBuiltin("mochi.user.methods.configure", api_user_methods_configure),
	"reset":     sl.NewBuiltin("mochi.user.methods.reset", api_user_methods_reset),
})

// auth_method_list is the canonical ordering of login methods for the
// per-user state model: email, the three credential-backed factors, then
// recovery (break-glass — never "required").
var auth_method_list = []string{"email", "passkey", "totp", "oauth", "recovery"}

// auth_factor_list is the subset that can serve as a primary sign-in
// proof; recovery is excluded because it's single-use break-glass.
var auth_factor_list = []string{"email", "passkey", "totp", "oauth"}

// methods_parse splits a comma-separated method list into a set, trimming
// whitespace and dropping empties.
func methods_parse(csv string) map[string]bool {
	out := map[string]bool{}
	for _, m := range strings.Split(csv, ",") {
		if m = strings.TrimSpace(m); m != "" {
			out[m] = true
		}
	}
	return out
}

// methods_join renders a method set back to a comma-separated string in
// canonical (auth_method_list) order, so the stored value is deterministic
// regardless of update order.
func methods_join(set map[string]bool) string {
	var out []string
	for _, m := range auth_method_list {
		if set[m] {
			out = append(out, m)
		}
	}
	return strings.Join(out, ",")
}

// user_method_available reports whether the user holds the credential a
// method needs to be usable: a registered passkey, a verified TOTP secret,
// or a linked OAuth identity. Email and recovery need no stored credential.
func user_method_available(user *User, method string) bool {
	db := db_open("db/users.db")
	switch method {
	case "email", "recovery":
		return true
	case "passkey":
		row, _ := db.row("select count(*) as count from credentials where user=?", user.UID)
		return row != nil && row["count"].(int64) > 0
	case "totp":
		row, _ := db.row("select verified from totp where user=?", user.UID)
		return row != nil && row["verified"].(int64) == 1
	case "oauth":
		ok, _ := db.exists("select 1 from oauth where user=?", user.UID)
		return ok
	}
	return false
}

// user_method_disabled reports whether the user has explicitly turned a
// method off (it's in their disabled set).
func user_method_disabled(user *User, method string) bool {
	return methods_parse(user.Disabled)[method]
}

// user_method_state returns the effective state for a method as shown in the
// settings grid. Operator policy wins: disabled server-wide reads as "disabled"
// and email required server-wide reads as "required", whatever the user set.
// Otherwise it reflects the user's own setting: "required" if in their required
// set, "disabled" if they turned it off or its credential is missing, else
// "allowed".
func user_method_state(user *User, method string) string {
	switch auth_method_state(method) {
	case "disabled":
		return "disabled"
	case "required":
		return "required"
	}
	if methods_parse(user.Methods)[method] {
		return "required"
	}
	if user_method_disabled(user, method) || !user_method_available(user, method) {
		return "disabled"
	}
	return "allowed"
}

// user_method_usable reports whether a method can sign the user in right
// now: the operator permits it, the user hasn't disabled it, and the
// credential exists.
func user_method_usable(user *User, method string) bool {
	return auth_method_state(method) != "disabled" && !user_method_disabled(user, method) && user_method_available(user, method)
}

// user_login_factors returns the address-then-prove factors the login
// screen offers after the user enters their email — email code, passkey,
// authenticator — filtered to those usable for this account. OAuth is
// excluded because its buttons identify the user before any address is
// typed, so they're driven by the system settings, not per-user state.
func user_login_factors(user *User) []string {
	var out []string
	for _, m := range []string{"email", "passkey", "totp"} {
		if user_method_usable(user, m) {
			out = append(out, m)
		}
	}
	return out
}

// user_login_offered returns the factors the login screen should offer after
// the user enters their email. When the account requires specific factors,
// only those can complete the login, so a usable-but-not-required factor is
// omitted - offering it would imply an alternative that doesn't exist, since
// the required factors must be completed regardless. With nothing required
// (methods=''), any one usable factor suffices, so all are offered. Always
// non-nil.
func user_login_offered(user *User) []string {
	usable := user_login_factors(user)
	required := auth_remaining_methods(user, "")
	if len(required) == 0 {
		if usable == nil {
			return []string{}
		}
		return usable
	}
	want := map[string]bool{}
	for _, m := range required {
		want[m] = true
	}
	offered := []string{}
	for _, f := range usable {
		if want[f] {
			offered = append(offered, f)
		}
	}
	return offered
}

// user_has_login_factor reports whether at least one primary factor would
// remain usable given the proposed required/disabled sets — the guard that
// stops a user locking themselves out by disabling their last way in.
func user_has_login_factor(user *User, required, disabled map[string]bool) bool {
	for _, m := range auth_factor_list {
		if disabled[m] || auth_method_state(m) == "disabled" {
			continue
		}
		// A required factor was checked for availability when it was set;
		// otherwise confirm the credential exists.
		if required[m] || user_method_available(user, m) {
			return true
		}
	}
	return false
}

// user_factor_removal_blocked reports why removing the user's last credential
// for a factor must be refused, or "" if it is safe. "required" - the factor
// is still in the required set, so removing its credential would make login
// impossible (the user must un-require it first); "last" - it is the only
// factor that could still sign the user in (e.g. they disabled email and rely
// on this one). Shared by the passkey-delete and authenticator-disable guards.
func user_factor_removal_blocked(user *User, method string) string {
	required := methods_parse(user.Methods)
	if required[method] {
		return "required"
	}
	disabled := methods_parse(user.Disabled)
	disabled[method] = true
	if !user_has_login_factor(user, required, disabled) {
		return "last"
	}
	return ""
}

// user_methods_configure sets one login method to a state ("disabled",
// "allowed", or "required") for the user, enforcing the operator policy
// floor/ceiling, credential availability, and the at-least-one-factor
// rule. Returns "" on success, or a short error code the caller maps to a
// translated message: "invalid" (unknown method/state), "blocked" (policy
// forbids it), "credential" (nothing to enable), "last" (would remove the
// user's only way to sign in).
func user_methods_configure(user *User, method, state string) string {
	known := false
	for _, m := range auth_method_list {
		if m == method {
			known = true
			break
		}
	}
	if !known {
		return "invalid"
	}
	if state != "disabled" && state != "allowed" && state != "required" {
		return "invalid"
	}
	// Recovery can't be "required": it's single-use break-glass, so demanding it
	// every sign-in makes no sense. (OAuth was barred here too while it counted
	// as the email factor; now that it's an independent factor it can be
	// required like passkey or authenticator, subject to a linked provider.)
	if method == "recovery" && state == "required" {
		return "invalid"
	}

	// Operator policy bounds the per-user choice.
	system := auth_method_state(method)
	if system == "disabled" && state != "disabled" {
		return "blocked"
	}
	if system == "required" && state != "required" {
		return "blocked"
	}

	// Enabling a credential-backed factor needs the credential present.
	if state != "disabled" && !user_method_available(user, method) {
		return "credential"
	}

	required := methods_parse(user.Methods)
	disabled := methods_parse(user.Disabled)
	delete(required, method)
	delete(disabled, method)
	switch state {
	case "required":
		required[method] = true
	case "disabled":
		disabled[method] = true
	}

	if !user_has_login_factor(user, required, disabled) {
		return "last"
	}

	methods := methods_join(required)
	off := methods_join(disabled)
	db := db_open("db/users.db")
	db.exec("update users set methods=?, disabled=? where uid=?", methods, off, user.UID)
	audit_password_changed(user.Username, "methods_changed")
	return ""
}

// POST /_/auth/methods - Complete additional MFA factor
func web_auth_mfa(c *gin.Context) {
	var input struct {
		Partial   string   `json:"partial"`
		Method    string   `json:"method"`            // Single method (legacy)
		Code      string   `json:"code"`              // Single code (legacy)
		EmailCode string   `json:"email_code"`        // Email code for atomic validation
		TotpCode  string   `json:"totp_code"`         // TOTP code for atomic validation
		Methods   []string `json:"methods,omitempty"` // Multiple methods for atomic validation
	}
	if err := c.ShouldBindJSON(&input); err != nil {
		respond_error(c, http.StatusBadRequest, "invalid_request", "errors.invalid_request", nil)
		return
	}

	// Load partial session
	db := db_open("db/sessions.db")
	row, _ := db.row("select * from partial where id=? and expires>?", input.Partial, now())
	if row == nil {
		respond_error(c, http.StatusBadRequest, "session_expired", "errors.session_expired", nil)
		return
	}

	user := user_by_uid(row["user"].(string))
	if user == nil {
		respond_error(c, http.StatusBadRequest, "user_not_found", "errors.user_not_found", nil)
		return
	}
	if user.Status == "suspended" {
		respond_error(c, http.StatusForbidden, "suspended", "errors.suspended", nil)
		return
	}

	remaining := strings.Split(row["remaining"].(string), ",")
	completed := row["completed"].(string)

	// Determine which methods to validate
	var methods []string
	if len(input.Methods) > 0 {
		methods = input.Methods
	} else if input.Method != "" {
		methods = []string{input.Method}
	} else {
		// Auto-detect from provided codes
		if input.EmailCode != "" {
			methods = append(methods, "email")
		}
		if input.TotpCode != "" {
			methods = append(methods, "totp")
		}
	}

	if len(methods) == 0 {
		respond_error(c, http.StatusBadRequest, "no_methods", "errors.no_methods", nil)
		return
	}

	// Check all methods are in remaining list
	for _, method := range methods {
		found := false
		for _, m := range remaining {
			if m == method {
				found = true
				break
			}
		}
		if !found {
			respond_error(c, http.StatusBadRequest, "invalid_method", "errors.invalid_method", nil)
			return
		}
	}

	// Get codes for each method
	lookup := func(method string) string {
		switch method {
		case "email":
			if input.EmailCode != "" {
				return input.EmailCode
			}
			return input.Code
		case "totp":
			if input.TotpCode != "" {
				return input.TotpCode
			}
			return input.Code
		default:
			return input.Code
		}
	}

	// Throttle per account right before verification — guessable codes, and
	// holding a partial does not exempt the caller (see account_gate).
	if !account_gate_guard(c, user.UID) {
		return
	}
	verified := false
	defer func() { account_login.done(user.UID, verified) }()

	// Validate all methods WITHOUT consuming codes first
	// For email, we need to check without deleting; for TOTP, it's stateless
	for _, method := range methods {
		code := lookup(method)
		switch method {
		case "email":
			if !email_code_check(user.Username, code) {
				respond_error(c, http.StatusUnauthorized, "invalid_code", "errors.invalid_code", nil)
				return
			}
		case "totp":
			if !totp_verify(user.UID, code) {
				respond_error(c, http.StatusUnauthorized, "invalid_code", "errors.invalid_code", nil)
				return
			}
		default:
			respond_error(c, http.StatusBadRequest, "unsupported_method", "errors.unsupported_method", nil)
			return
		}
	}
	verified = true

	// All validations passed - now consume the codes
	for _, method := range methods {
		code := lookup(method)
		if method == "email" {
			email_code_consume(code)
		}
	}

	// Update completed methods
	for _, method := range methods {
		if completed != "" {
			completed += ","
		}
		completed += method
	}

	// Calculate new remaining
	var pending []string
	for _, m := range remaining {
		still_remaining := true
		for _, validated := range methods {
			if m == validated {
				still_remaining = false
				break
			}
		}
		if still_remaining {
			pending = append(pending, m)
		}
	}

	if len(pending) > 0 {
		// Still more methods required. Re-emit the full row so peers
		// observe the partial-state progression and can complete the
		// flow if the user lands on a different host for the next
		// factor.
		pending_string := strings.Join(pending, ",")
		db.exec("update partial set completed=?, remaining=? where id=?",
			completed, pending_string, input.Partial)
		c.JSON(http.StatusOK, gin.H{
			"mfa":       true,
			"partial":   input.Partial,
			"remaining": pending,
		})
		return
	}

	// All methods complete - delete partial session and create full session.
	partial_delete(db, input.Partial)

	// Load identity for the response
	user.Identity = user.identity()
	auth_complete_login(c, user)
}

// mochi.user.methods.get() -> list: Get user's required authentication methods
func api_user_methods_get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := require_permission(t, fn, "user/authentication/read"); err != nil {
		return sl_error(fn, "%v", err)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	if user.Methods == "" {
		return sl_encode([]string{}), nil
	}

	methods := strings.Split(user.Methods, ",")
	return sl_encode(methods), nil
}

// mochi.user.methods.states() -> dict: the user's per-method login state,
// keyed by method (email, passkey, totp, oauth, recovery). Each value is a
// dict {state, system, available}: state is the effective per-user setting
// ("disabled"|"allowed"|"required"), system is the operator policy, and
// available reports whether the credential the method needs exists.
func api_user_methods_states(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := require_permission(t, fn, "user/authentication/read"); err != nil {
		return sl_error(fn, "%v", err)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	out := map[string]any{}
	for _, m := range auth_method_list {
		out[m] = map[string]any{
			"state":     user_method_state(user, m),
			"system":    auth_method_state(m),
			"available": user_method_available(user, m),
		}
	}
	return sl_encode(out), nil
}

// mochi.user.methods.configure(method, state) -> string: set one login
// method's per-user state ("disabled" | "allowed" | "required"). Returns
// "" on success, or an error code ("invalid" | "blocked" | "credential" |
// "last") the calling action maps to a translated message.
func api_user_methods_configure(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := require_permission(t, fn, "user/authentication/write"); err != nil {
		return sl_error(fn, "%v", err)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	var method, state string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "method", &method, "state", &state); err != nil {
		return sl_error(fn, "%v", err)
	}

	return sl.String(user_methods_configure(user, method, state)), nil
}

// mochi.user.methods.set(methods) -> bool: Set user's required authentication methods
func api_user_methods_set(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := require_permission(t, fn, "user/authentication/write"); err != nil {
		return sl_error(fn, "%v", err)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	if len(args) != 1 {
		return sl_error(fn, "syntax: <methods: list>")
	}

	list, ok := args[0].(*sl.List)
	if !ok {
		return sl_error(fn, "methods must be a list")
	}

	var methods []string
	for i := 0; i < list.Len(); i++ {
		m, _ := sl.AsString(list.Index(i))
		if m != "" {
			methods = append(methods, m)
		}
	}

	if len(methods) == 0 {
		return sl_error(fn, "at least one method required")
	}

	// Validate against allowed methods
	allowed := auth_methods_allowed_list()
	for _, m := range methods {
		valid := false
		for _, a := range allowed {
			if m == a {
				valid = true
				break
			}
		}
		if !valid {
			return sl_error(fn, "method not allowed: %s", m)
		}
	}

	// Check required methods are included
	for _, r := range auth_methods_required_list() {
		found := false
		for _, m := range methods {
			if m == r {
				found = true
				break
			}
		}
		if !found {
			return sl_error(fn, "method required: %s", r)
		}
	}

	// Validate user has the required auth configured
	db := db_open("db/users.db")
	for _, m := range methods {
		switch m {
		case "passkey":
			row, _ := db.row("select count(*) as count from credentials where user=?", user.UID)
			if row == nil || row["count"].(int64) == 0 {
				return sl_error(fn, "no passkey registered")
			}
		case "totp":
			row, _ := db.row("select verified from totp where user=?", user.UID)
			if row == nil || row["verified"].(int64) != 1 {
				return sl_error(fn, "totp not configured")
			}
		}
	}

	// The list-based setter expresses only the required set; it carries no
	// "disabled" concept, so clear the per-user disabled list — everything
	// not required becomes allowed.
	csv := strings.Join(methods, ",")
	db.exec("update users set methods=?, disabled='' where uid=?", csv, user.UID)
	audit_password_changed(user.Username, "methods_changed")
	return sl.True, nil
}

// mochi.user.methods.reset(user) -> bool: Admin: reset user to email only
func api_user_methods_reset(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := require_permission(t, fn, "user/authentication/write"); err != nil {
		return sl_error(fn, "%v", err)
	}

	admin := t.Local("user").(*User)
	if admin == nil {
		return sl_error(fn, "no user")
	}
	if !admin.administrator() {
		return sl_error(fn, "admin required")
	}

	if len(args) != 1 {
		return sl_error(fn, "syntax: <user: string>")
	}

	id, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid user uid")
	}

	db := db_open("db/users.db")
	exists, _ := db.exists("select uid from users where uid=?", id)
	if !exists {
		return sl_error(fn, "user not found")
	}

	target := user_by_uid(id)
	target_name := id
	if target != nil {
		target_name = target.Username
	}
	db.exec("update users set methods='email', disabled='' where uid=?", id)
	audit_password_changed(target_name, "admin_reset")
	return sl.True, nil
}

// ============================================================================
// TOTP authentication
// ============================================================================

// email_code_check validates an email code without consuming it
func email_code_check(username string, code string) bool {
	sessions := db_open("db/sessions.db")
	row, _ := sessions.row("select username from codes where code=? and expires>=?", code, now())
	if row == nil {
		return false
	}
	return row["username"].(string) == username
}

// email_code_consume deletes an email code after successful validation
// and fans the delete out to peers so the code can't be replayed on
// another host in the user's host set.
func email_code_consume(code string) {
	sessions := db_open("db/sessions.db")
	// Look up the username before deleting so we can resolve the user
	// for the cross-host emit (codes is keyed on (code, username)).
	var username string
	if row, _ := sessions.row("select username from codes where code=?", code); row != nil {
		username, _ = row["username"].(string)
	}
	sessions.exec("delete from codes where code=?", code)
	if u := user_by_username(username); u != nil {
	}
}

// totp_verify checks a TOTP code for a user (used by MFA endpoint)
func totp_verify(user string, code string) bool {
	db := db_open("db/users.db")
	row, _ := db.row("select secret, verified from totp where user=?", user)
	if row == nil {
		return false
	}
	if row["verified"].(int64) != 1 {
		return false
	}
	return totp.Validate(code, row["secret"].(string))
}

// mochi.user.totp.setup() -> dict: Generate TOTP secret for user
func api_user_totp_setup(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := require_permission(t, fn, "user/authentication/write"); err != nil {
		return sl_error(fn, "%v", err)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	// Generate new TOTP key
	domain := ini_string("web", "domain", "localhost")
	key, err := totp.Generate(totp.GenerateOpts{
		Issuer:      "Mochi",
		AccountName: user.Username,
		Period:      30,
		Digits:      otp.DigitsSix,
		Algorithm:   otp.AlgorithmSHA1,
	})
	if err != nil {
		return sl_error(fn, "failed to generate TOTP: %v", err)
	}

	// Store secret (unverified)
	db := db_open("db/users.db")
	created := now()
	db.exec("replace into totp (user, secret, verified, created) values (?, ?, 0, ?)",
		user.UID, key.Secret(), created)

	// Return secret and otpauth URL for QR code
	return sl_encode(map[string]any{
		"secret": key.Secret(),
		"url":    key.URL(),
		"issuer": "Mochi",
		"domain": domain,
	}), nil
}

// mochi.user.totp.verify(code): during setup, verifies the code and marks
// TOTP enabled (returns bool). When TOTP is already enabled, this is a
// step-up re-verify: it advances the re-authentication accrual and returns
// the result dict ({"token": ...} or {"remaining": [...]}), or None on a
// bad code.
func api_user_totp_verify(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := require_permission(t, fn, "user/authentication/write"); err != nil {
		return sl_error(fn, "%v", err)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	if len(args) != 1 {
		return sl_error(fn, "syntax: <code: string>")
	}

	code, ok := sl.AsString(args[0])
	if !ok || code == "" {
		return sl_error(fn, "invalid code")
	}

	db := db_open("db/users.db")
	row, _ := db.row("select secret, verified, created from totp where user=?", user.UID)
	if row == nil {
		return sl_error(fn, "totp not set up")
	}

	secret := row["secret"].(string)
	verified, _ := row["verified"].(int64)

	// When TOTP is already enabled this call is a step-up re-verify, not
	// setup: validate the code and advance the re-authentication accrual,
	// returning the step-up result dict, or None on a bad code.
	if verified == 1 {
		if user_method_disabled(user, "totp") {
			return sl.None, nil
		}
		if !totp.Validate(code, secret) {
			return sl.None, nil
		}
		return reauthentication_result(user, "totp"), nil
	}

	if !totp.Validate(code, secret) {
		return sl.False, nil
	}

	// Mark as verified (setup completion)
	db.exec("update totp set verified=1 where user=?", user.UID)
	audit_password_changed(user.Username, "totp_enabled")
	return sl.True, nil
}

// mochi.user.totp.enabled() -> bool: Check if TOTP is enabled and verified
func api_user_totp_enabled(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := require_permission(t, fn, "user/authentication/read"); err != nil {
		return sl_error(fn, "%v", err)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	db := db_open("db/users.db")
	row, _ := db.row("select verified from totp where user=?", user.UID)
	if row == nil {
		return sl.False, nil
	}
	return sl.Bool(row["verified"].(int64) == 1), nil
}

// mochi.user.totp.disable() -> bool: Remove TOTP from user account
func api_user_totp_disable(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := require_permission(t, fn, "user/authentication/write"); err != nil {
		return sl_error(fn, "%v", err)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	// Refuse if disabling the authenticator would leave no way to sign in.
	switch user_factor_removal_blocked(user, "totp") {
	case "required":
		return sl_error(fn, "cannot disable the authenticator while it is a required method")
	case "last":
		return sl_error(fn, "cannot disable your only remaining sign-in method")
	}

	db := db_open("db/users.db")
	db.exec("delete from totp where user=?", user.UID)
	audit_password_changed(user.Username, "totp_disabled")
	return sl.True, nil
}

// ============================================================================
// Recovery code authentication
// ============================================================================

// POST /_/auth/recovery - Login with recovery code
func web_recovery_login(c *gin.Context) {
	if !auth_method_allowed("recovery") {
		respond_error(c, http.StatusForbidden, "recovery_disabled", "errors.recovery_disabled", nil)
		return
	}

	var input struct {
		Username string `json:"username"`
		Code     string `json:"code"`
	}
	if err := c.ShouldBindJSON(&input); err != nil {
		respond_error(c, http.StatusBadRequest, "invalid_request", "errors.invalid_request", nil)
		return
	}

	// Normalize code (remove dashes, case-sensitive)
	code := strings.ReplaceAll(input.Code, "-", "")

	db := db_open("db/users.db")
	row, _ := db.row("select uid from users where username=?", input.Username)
	if row == nil {
		// Timing-safe: always do bcrypt comparison even if user not found
		bcrypt.CompareHashAndPassword([]byte("$2a$10$xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), []byte(code))
		audit_login_failed(input.Username, rate_limit_client_ip(c), "user_not_found")
		respond_error(c, http.StatusUnauthorized, "invalid_credentials", "errors.invalid_credentials", nil)
		return
	}
	user_id, _ := row["uid"].(string)

	// Throttle guessing across all source addresses (see account_gate).
	if !account_gate_guard(c, user_id) {
		return
	}
	verified := false
	defer func() { account_login.done(user_id, verified) }()

	// Check recovery codes
	rows, _ := db.rows("select id, hash from recovery where user=?", user_id)
	var matched int64 = -1
	for _, row := range rows {
		hash, _ := row["hash"].(string)
		if bcrypt.CompareHashAndPassword([]byte(hash), []byte(code)) == nil {
			matched = row["id"].(int64)
			break
		}
	}

	if matched < 0 {
		audit_login_failed(input.Username, rate_limit_client_ip(c), "invalid_recovery_code")
		respond_error(c, http.StatusUnauthorized, "invalid_credentials", "errors.invalid_credentials", nil)
		return
	}
	verified = true

	// Load user with identity
	user := user_by_uid(user_id)
	if user == nil {
		respond_error(c, http.StatusInternalServerError, "user_error", "errors.user_error", nil)
		return
	}
	if user.Status == "suspended" {
		respond_error(c, http.StatusForbidden, "suspended", "errors.suspended", nil)
		return
	}

	// Refuse if the user has turned recovery codes off as a login factor.
	if user_method_disabled(user, "recovery") {
		audit_login_failed(input.Username, rate_limit_client_ip(c), "recovery_disabled")
		respond_error(c, http.StatusUnauthorized, "invalid_credentials", "errors.invalid_credentials", nil)
		return
	}

	// Delete used code (after suspension check to avoid consuming codes for suspended users)
	db.exec("delete from recovery where id=?", matched)

	// Recovery bypasses all MFA - create full session directly
	auth_complete_login(c, user)
}

// mochi.user.recovery.generate() -> list: Generate new recovery codes (replaces existing)
func api_user_recovery_generate(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := require_permission(t, fn, "user/authentication/write"); err != nil {
		return sl_error(fn, "%v", err)
	}

	if !auth_method_allowed("recovery") {
		return sl_error(fn, "recovery disabled")
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	codes := make([]string, recovery_code_count)
	db := db_open("db/users.db")

	// Delete existing codes
	db.exec("delete from recovery where user=?", user.UID)
	// Tell peers to wipe their existing codes too. The hash="*" sentinel
	// is recognised by the apply path as "delete all for user".
	audit_password_changed(user.Username, "recovery_regenerated")

	// Generate new codes
	for i := 0; i < recovery_code_count; i++ {
		// Format: XXXX-XXXX-XXXX (12 unambiguous mixed-case chars)
		raw := random_unambiguous(12)
		code := raw[:4] + "-" + raw[4:8] + "-" + raw[8:]
		codes[i] = code

		// Store bcrypt hash of normalized code (no dashes)
		hash, _ := bcrypt.GenerateFromPassword([]byte(raw), bcrypt.DefaultCost)
		created := now()
		db.exec("insert into recovery (user, hash, created) values (?, ?, ?)",
			user.UID, string(hash), created)
	}

	return sl_encode(codes), nil
}

// mochi.user.recovery.count() -> int: Get remaining recovery code count
func api_user_recovery_count(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := require_permission(t, fn, "user/authentication/read"); err != nil {
		return sl_error(fn, "%v", err)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	db := db_open("db/users.db")
	row, _ := db.row("select count(*) as count from recovery where user=?", user.UID)
	if row == nil {
		return sl.MakeInt(0), nil
	}
	return sl.MakeInt(int(row["count"].(int64))), nil
}

// partial_create inserts an MFA partial-login session. Wrapping keeps the
// insert SQL in one place across the five call sites.
func partial_create(sdb *DB, partialID, userUID, completed, remaining string, expires int64) {
	sdb.exec("insert into partial (id, user, completed, remaining, expires) values (?, ?, ?, ?, ?)",
		partialID, userUID, completed, remaining, expires)
}

// partial_delete removes an MFA partial-login session by id (the random
// 32-char id is globally unique).
func partial_delete(sdb *DB, partialID string) {
	sdb.exec("delete from partial where id=?", partialID)
}

// partial_continue records a just-completed login factor. When the caller's
// login_partial cookie names a live partial for the same user, the factor is
// folded into it — so factors complete in any order, and the passkey and
// OAuth endpoints continue a sequence begun by a code factor instead of
// minting a fresh partial that forgets the ones already done (which made an
// account requiring both a code factor and a passkey or OAuth impossible to
// sign in to). Otherwise a fresh partial is created. Returns the partial id
// and the factors still remaining; when none remain the partial is deleted
// and the empty id tells the caller to create the full session.
func partial_continue(c *gin.Context, user *User, method string) (string, []string) {
	sdb := db_open("db/sessions.db")
	completed := map[string]bool{method: true}
	partial := ""
	if cookie, err := c.Cookie("login_partial"); err == nil && cookie != "" {
		row, _ := sdb.row("select user, completed from partial where id=? and expires>?", cookie, now())
		if row != nil && row["user"].(string) == user.UID {
			partial = cookie
			for m := range methods_parse(row["completed"].(string)) {
				completed[m] = true
			}
		}
	}

	remaining := auth_remaining_after(user, completed)
	if len(remaining) == 0 {
		if partial != "" {
			partial_delete(sdb, partial)
		}
		return "", nil
	}

	if partial == "" {
		partial = random_alphanumeric(32)
		partial_create(sdb, partial, user.UID, methods_join(completed), strings.Join(remaining, ","), now()+300)
	} else {
		sdb.exec("update partial set completed=?, remaining=?, expires=? where id=?",
			methods_join(completed), strings.Join(remaining, ","), now()+300, partial)
	}
	// Cookie-mirror the partial for /codes recovery (see web_auth_partial).
	web_cookie_set(c, "login_partial", partial)
	return partial, remaining
}

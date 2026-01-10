// Mochi authentication and login
// Copyright Alistair Cunningham 2025

package main

import (
	"errors"
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
	jwt_expiry = int64(3600)
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
	User int `json:"user"`
	jwt.RegisteredClaims
}

// Exchange a login code for a JWT token and login cookie
func web_login_verify(c *gin.Context) {
	var body struct {
		Code string `json:"code"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return
	}
	if body.Code == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing code"})
		return
	}
	user, reason := user_from_code(body.Code)
	if user == nil {
		audit_login_failed("", rate_limit_client_ip(c), reason)
		switch reason {
		case "signup_disabled":
			c.JSON(http.StatusForbidden, gin.H{"error": "signup_disabled", "message": "New user signup is disabled."})
		case "suspended":
			c.JSON(http.StatusForbidden, gin.H{"error": "suspended", "message": "Your account has been suspended."})
		default:
			c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid code"})
		}
		return
	}

	// Reset rate limit on successful login
	rate_limit_login.reset(rate_limit_client_ip(c))

	// Check for remaining MFA methods
	remaining := auth_remaining_methods(user, "email")
	if len(remaining) > 0 {
		// Create partial session for MFA
		partial := random_alphanumeric(32)
		db := db_open("db/sessions.db")
		db.exec("insert into partial (id, user, completed, remaining, expires) values (?, ?, 'email', ?, ?)",
			partial, user.ID, strings.Join(remaining, ","), now()+300)
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

// auth_remaining_methods returns methods still required after completing the given method
func auth_remaining_methods(user *User, completed string) []string {
	if user.Methods == "" || user.Methods == completed {
		return nil
	}

	methods := strings.Split(user.Methods, ",")
	var remaining []string
	for _, m := range methods {
		m = strings.TrimSpace(m)
		if m != completed && m != "" {
			remaining = append(remaining, m)
		}
	}
	return remaining
}

// auth_complete_login creates a full session and returns token/session to client
func auth_complete_login(c *gin.Context, user *User) {
	// Create session entry (per-device) which stores a per-session secret
	session := login_create(user.ID, c.ClientIP(), c.GetHeader("User-Agent"))

	// Create a JWT signed with the per-session secret
	token := auth_create_token(user.ID, session)
	if token == "" {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "unable to create token"})
		return
	}

	// Set session cookie for web browser authentication
	web_cookie_set(c, "session", session)

	// Audit log successful login
	audit_login(user.Username, rate_limit_client_ip(c))

	response := gin.H{
		"token":   token,
		"session": session,
	}

	if user.Identity != nil && user.Identity.Name != "" {
		response["name"] = user.Identity.Name
	}

	c.JSON(http.StatusOK, response)
}

// auth_create_token creates a JWT for a session
func auth_create_token(user_id int, login string) string {
	var s Session
	db := db_open("db/sessions.db")
	if !db.scan(&s, "select * from sessions where code=? and expires>=?", login, now()) {
		return ""
	}

	if s.Secret == "" {
		return ""
	}

	claims := mochi_claims{
		User: user_id,
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
		"email":    setting_get("auth_email_enabled", "true") == "true",
		"passkey":  setting_get("auth_passkey_enabled", "true") == "true",
		"recovery": setting_get("auth_recovery_enabled", "true") == "true",
		"signup":   setting_signup_enabled(),
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
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_request"})
		return
	}

	user := user_by_username(input.Email)
	if user == nil {
		audit_login_failed(input.Email, rate_limit_client_ip(c), "user_not_found")
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid_credentials"})
		return
	}
	if user.Status == "suspended" {
		audit_login_failed(input.Email, rate_limit_client_ip(c), "suspended")
		c.JSON(http.StatusForbidden, gin.H{"error": "suspended", "message": "Your account has been suspended."})
		return
	}

	// Verify TOTP code
	if !totp_verify(user.ID, input.Code) {
		audit_login_failed(input.Email, rate_limit_client_ip(c), "invalid_totp")
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid_code"})
		return
	}

	// Reset rate limit on successful verification
	rate_limit_login.reset(rate_limit_client_ip(c))

	// Check for remaining MFA methods after TOTP
	remaining := auth_remaining_methods(user, "totp")
	if len(remaining) > 0 {
		// If email is required, send the code now
		for _, method := range remaining {
			if method == "email" {
				code_send(user.Username)
				break
			}
		}

		// Create partial session for remaining MFA
		partial := random_alphanumeric(32)
		db := db_open("db/sessions.db")
		db.exec("insert into partial (id, user, completed, remaining, expires) values (?, ?, 'totp', ?, ?)",
			partial, user.ID, strings.Join(remaining, ","), now()+300)
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

// Create a JWT using a specific HMAC secret
func jwt_create_with_secret(user_id int, secret []byte) (string, error) {
	claims := mochi_claims{
		User: user_id,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Unix(now()+jwt_expiry, 0)),
			IssuedAt:  jwt.NewNumericDate(time.Unix(now(), 0)),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString(secret)
	if err != nil {
		return "", err
	}
	return signed, nil
}

// Verify a JWT and return the user id, or -1 if invalid.
// If the token header contains a "kid" referencing a login code, attempt to verify
// using that login's secret. Otherwise fall back to the global secret.
func jwt_verify(token_string string) (int, error) {
	// First parse the token without verification to read header/kid
	token, _, err := new(jwt.Parser).ParseUnverified(token_string, &mochi_claims{})
	if err != nil {
		return -1, err
	}

	// Require kid header (login code) to look up per-login secret
	kid, ok := token.Header["kid"].(string)
	if !ok || kid == "" {
		return -1, errors.New("token missing kid header referencing login code")
	}
	var s Session
	db := db_open("db/sessions.db")
	if !db.scan(&s, "select * from sessions where code=? and expires>=?", kid, now()) {
		return -1, errors.New("session not found for kid")
	}
	if s.Secret == "" {
		return -1, errors.New("session has no secret")
	}
	secret := []byte(s.Secret)
	var claims mochi_claims
	tkn, err := jwt.ParseWithClaims(token_string, &claims, func(token *jwt.Token) (interface{}, error) {
		return secret, nil
	})
	if err != nil {
		return -1, err
	}
	if !tkn.Valid {
		return -1, errors.New("invalid token")
	}
	return claims.User, nil
}

// ============================================================================
// Multi-factor authentication
// ============================================================================

var api_user_methods = sls.FromStringDict(sl.String("mochi.user.methods"), sl.StringDict{
	"get":   sl.NewBuiltin("mochi.user.methods.get", api_user_methods_get),
	"set":   sl.NewBuiltin("mochi.user.methods.set", api_user_methods_set),
	"reset": sl.NewBuiltin("mochi.user.methods.reset", api_user_methods_reset),
})

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
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_request"})
		return
	}

	// Load partial session
	db := db_open("db/sessions.db")
	row, _ := db.row("select * from partial where id=? and expires>?", input.Partial, now())
	if row == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "session_expired"})
		return
	}

	user := user_by_id(int(row["user"].(int64)))
	if user == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "user_not_found"})
		return
	}
	if user.Status == "suspended" {
		c.JSON(http.StatusForbidden, gin.H{"error": "suspended", "message": "Your account has been suspended."})
		return
	}

	remaining := strings.Split(row["remaining"].(string), ",")
	completed := row["completed"].(string)

	// Determine which methods to validate
	var methodsToValidate []string
	if len(input.Methods) > 0 {
		methodsToValidate = input.Methods
	} else if input.Method != "" {
		methodsToValidate = []string{input.Method}
	} else {
		// Auto-detect from provided codes
		if input.EmailCode != "" {
			methodsToValidate = append(methodsToValidate, "email")
		}
		if input.TotpCode != "" {
			methodsToValidate = append(methodsToValidate, "totp")
		}
	}

	if len(methodsToValidate) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no_methods"})
		return
	}

	// Check all methods are in remaining list
	for _, method := range methodsToValidate {
		found := false
		for _, m := range remaining {
			if m == method {
				found = true
				break
			}
		}
		if !found {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_method"})
			return
		}
	}

	// Get codes for each method
	getCode := func(method string) string {
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

	// Validate all methods WITHOUT consuming codes first
	// For email, we need to check without deleting; for TOTP, it's stateless
	for _, method := range methodsToValidate {
		code := getCode(method)
		switch method {
		case "email":
			if !email_code_check(user.Username, code) {
				c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid_code"})
				return
			}
		case "totp":
			if !totp_verify(user.ID, code) {
				c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid_code"})
				return
			}
		default:
			c.JSON(http.StatusBadRequest, gin.H{"error": "unsupported_method"})
			return
		}
	}

	// All validations passed - now consume the codes
	for _, method := range methodsToValidate {
		code := getCode(method)
		if method == "email" {
			email_code_consume(code)
		}
	}

	// Update completed methods
	for _, method := range methodsToValidate {
		if completed != "" {
			completed += ","
		}
		completed += method
	}

	// Calculate new remaining
	var newRemaining []string
	for _, m := range remaining {
		stillRemaining := true
		for _, validated := range methodsToValidate {
			if m == validated {
				stillRemaining = false
				break
			}
		}
		if stillRemaining {
			newRemaining = append(newRemaining, m)
		}
	}

	if len(newRemaining) > 0 {
		// Still more methods required
		db.exec("update partial set completed=?, remaining=? where id=?",
			completed, strings.Join(newRemaining, ","), input.Partial)
		c.JSON(http.StatusOK, gin.H{
			"mfa":       true,
			"partial":   input.Partial,
			"remaining": newRemaining,
		})
		return
	}

	// All methods complete - delete partial session and create full session
	db.exec("delete from partial where id=?", input.Partial)

	// Load identity for the response
	user.Identity = user.identity()
	auth_complete_login(c, user)
}

// mochi.user.methods.get() -> list: Get user's required authentication methods
func api_user_methods_get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	if user.Methods == "" {
		return sl_encode([]string{"email"}), nil
	}

	methods := strings.Split(user.Methods, ",")
	return sl_encode(methods), nil
}

// mochi.user.methods.set(methods) -> bool: Set user's required authentication methods
func api_user_methods_set(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
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
	allowed := strings.Split(setting_get("auth_methods_allowed", "email,passkey,totp,recovery"), ",")
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
	required := setting_get("auth_methods_required", "")
	if required != "" {
		for _, r := range strings.Split(required, ",") {
			r = strings.TrimSpace(r)
			if r == "" {
				continue
			}
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
	}

	// Validate user has the required auth configured
	db := db_open("db/users.db")
	for _, m := range methods {
		switch m {
		case "passkey":
			row, _ := db.row("select count(*) as count from credentials where user=?", user.ID)
			if row == nil || row["count"].(int64) == 0 {
				return sl_error(fn, "no passkey registered")
			}
		case "totp":
			row, _ := db.row("select verified from totp where user=?", user.ID)
			if row == nil || row["verified"].(int64) != 1 {
				return sl_error(fn, "totp not configured")
			}
		}
	}

	db.exec("update users set methods=? where id=?", strings.Join(methods, ","), user.ID)
	return sl.True, nil
}

// mochi.user.methods.reset(user) -> bool: Admin: reset user to email only
func api_user_methods_reset(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	admin := t.Local("user").(*User)
	if admin == nil {
		return sl_error(fn, "no user")
	}
	if !admin.administrator() {
		return sl_error(fn, "admin required")
	}

	if len(args) != 1 {
		return sl_error(fn, "syntax: <user: int>")
	}

	id, err := sl.AsInt32(args[0])
	if err != nil {
		return sl_error(fn, "invalid user id")
	}

	db := db_open("db/users.db")
	exists, _ := db.exists("select id from users where id=?", id)
	if !exists {
		return sl_error(fn, "user not found")
	}

	db.exec("update users set methods='email' where id=?", id)
	return sl.True, nil
}

// ============================================================================
// TOTP authentication
// ============================================================================

// email_code_verify checks an email code for a user and consumes it (used by MFA endpoint)
func email_code_verify(username string, code string) bool {
	if !email_code_check(username, code) {
		return false
	}
	email_code_consume(code)
	return true
}

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
func email_code_consume(code string) {
	sessions := db_open("db/sessions.db")
	sessions.exec("delete from codes where code=?", code)
}

// totp_verify checks a TOTP code for a user (used by MFA endpoint)
func totp_verify(user int, code string) bool {
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
	db.exec("replace into totp (user, secret, verified, created) values (?, ?, 0, ?)",
		user.ID, key.Secret(), now())

	// Return secret and otpauth URL for QR code
	return sl_encode(map[string]any{
		"secret": key.Secret(),
		"url":    key.URL(),
		"issuer": "Mochi",
		"domain": domain,
	}), nil
}

// mochi.user.totp.verify(code) -> bool: Verify TOTP code and mark as verified
func api_user_totp_verify(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
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
	row, _ := db.row("select secret from totp where user=?", user.ID)
	if row == nil {
		return sl_error(fn, "totp not set up")
	}

	secret := row["secret"].(string)
	if !totp.Validate(code, secret) {
		return sl.False, nil
	}

	// Mark as verified
	db.exec("update totp set verified=1 where user=?", user.ID)
	return sl.True, nil
}

// mochi.user.totp.enabled() -> bool: Check if TOTP is enabled and verified
func api_user_totp_enabled(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	db := db_open("db/users.db")
	row, _ := db.row("select verified from totp where user=?", user.ID)
	if row == nil {
		return sl.False, nil
	}
	return sl.Bool(row["verified"].(int64) == 1), nil
}

// mochi.user.totp.disable() -> bool: Remove TOTP from user account
func api_user_totp_disable(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	// Check if totp is in user's required methods
	if strings.Contains(user.Methods, "totp") {
		return sl_error(fn, "cannot disable totp while it is a required method")
	}

	db := db_open("db/users.db")
	db.exec("delete from totp where user=?", user.ID)
	return sl.True, nil
}

// ============================================================================
// Recovery code authentication
// ============================================================================

// POST /_/auth/recovery - Login with recovery code
func web_recovery_login(c *gin.Context) {
	if setting_get("auth_recovery_enabled", "true") != "true" {
		c.JSON(http.StatusForbidden, gin.H{"error": "recovery_disabled"})
		return
	}

	var input struct {
		Username string `json:"username"`
		Code     string `json:"code"`
	}
	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_request"})
		return
	}

	// Normalize code (remove dashes, case-sensitive)
	code := strings.ReplaceAll(input.Code, "-", "")

	db := db_open("db/users.db")
	row, _ := db.row("select id from users where username=?", input.Username)
	if row == nil {
		// Timing-safe: always do bcrypt comparison even if user not found
		bcrypt.CompareHashAndPassword([]byte("$2a$10$xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), []byte(code))
		audit_login_failed(input.Username, rate_limit_client_ip(c), "user_not_found")
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid_credentials"})
		return
	}
	user_id := int(row["id"].(int64))

	// Check recovery codes
	rows, _ := db.rows("select id, hash from recovery where user=?", user_id)
	var matched int64 = -1
	for _, row := range rows {
		if bcrypt.CompareHashAndPassword([]byte(row["hash"].(string)), []byte(code)) == nil {
			matched = row["id"].(int64)
			break
		}
	}

	if matched < 0 {
		audit_login_failed(input.Username, rate_limit_client_ip(c), "invalid_recovery_code")
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid_credentials"})
		return
	}

	// Delete used code
	db.exec("delete from recovery where id=?", matched)

	// Reset rate limit on successful login
	rate_limit_login.reset(rate_limit_client_ip(c))

	// Load user with identity
	user := user_by_id(user_id)
	if user == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "user_error"})
		return
	}
	if user.Status == "suspended" {
		c.JSON(http.StatusForbidden, gin.H{"error": "suspended", "message": "Your account has been suspended."})
		return
	}

	// Recovery bypasses all MFA - create full session directly
	auth_complete_login(c, user)
}

// mochi.user.recovery.generate() -> list: Generate new recovery codes (replaces existing)
func api_user_recovery_generate(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if setting_get("auth_recovery_enabled", "true") != "true" {
		return sl_error(fn, "recovery disabled")
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	codes := make([]string, recovery_code_count)
	db := db_open("db/users.db")

	// Delete existing codes
	db.exec("delete from recovery where user=?", user.ID)

	// Generate new codes
	for i := 0; i < recovery_code_count; i++ {
		// Format: XXXX-XXXX-XXXX (12 unambiguous mixed-case chars)
		raw := random_unambiguous(12)
		code := raw[:4] + "-" + raw[4:8] + "-" + raw[8:]
		codes[i] = code

		// Store bcrypt hash of normalized code (no dashes)
		hash, _ := bcrypt.GenerateFromPassword([]byte(raw), bcrypt.DefaultCost)
		db.exec("insert into recovery (user, hash, created) values (?, ?, ?)",
			user.ID, string(hash), now())
	}

	return sl_encode(codes), nil
}

// mochi.user.recovery.count() -> int: Get remaining recovery code count
func api_user_recovery_count(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	db := db_open("db/users.db")
	row, _ := db.row("select count(*) as count from recovery where user=?", user.ID)
	if row == nil {
		return sl.MakeInt(0), nil
	}
	return sl.MakeInt(int(row["count"].(int64))), nil
}

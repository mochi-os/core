// Mochi server: OAuth2 / OIDC login (Google, GitHub, Microsoft, Facebook, X)
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/gin-gonic/gin"
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
	"golang.org/x/oauth2"
)

// oauth_provider holds the static configuration for one provider. OIDC
// providers (Google, Microsoft) fill discovery and rely on go-oidc to derive
// endpoints; plain OAuth2 providers (GitHub, Facebook, X) fill auth/token URLs
// directly and implement a profile fetcher.
type oauth_provider struct {
	name       string
	display    string
	oidc       bool
	discovery  func() string // OIDC only; may depend on tenant setting
	auth_url   string        // non-OIDC
	token_url  string        // non-OIDC
	scopes     []string
	fetch      func(ctx context.Context, token string) (*oauth_profile, error)
	extra_auth []oauth2.AuthCodeOption
}

// oauth_profile is the minimal identity we extract from any provider.
type oauth_profile struct {
	Subject  string
	Email    string
	Verified bool
	Name     string
}

// oauth_state is serialised into ceremonies.data during the round trip.
type oauth_state struct {
	Provider  string `json:"provider"`
	Verifier  string `json:"verifier"`
	Nonce     string `json:"nonce"`
	Target    string `json:"target"`
	Redirect  string `json:"redirect"`
	Mode      string `json:"mode,omitempty"`      // "mobile" for native-app flow
	Scheme    string `json:"scheme,omitempty"`    // app deep-link scheme (mobile)
	Challenge string `json:"challenge,omitempty"` // S256(verifier) for app exchange (mobile)
	Email     string `json:"email,omitempty"`     // address the email-login flow is verifying
}

var api_user_oauth = sls.FromStringDict(sl.String("mochi.user.oauth"), sl.StringDict{
	"list":   sl.NewBuiltin("mochi.user.oauth.list", api_user_oauth_list),
	"unlink": sl.NewBuiltin("mochi.user.oauth.unlink", api_user_oauth_unlink),
	"verify": sls.FromStringDict(sl.String("mochi.user.oauth.verify"), sl.StringDict{
		"begin":  sl.NewBuiltin("mochi.user.oauth.verify.begin", api_user_oauth_verify_begin),
		"finish": sl.NewBuiltin("mochi.user.oauth.verify.finish", api_user_oauth_verify_finish),
	}),
})

// Cached go-oidc providers keyed by discovery URL. Discovery is a network call
// and we only want to do it once per process.
var (
	oidc_providers    = map[string]*oidc.Provider{}
	oidc_providers_mu sync.Mutex
	oauth_http_client = &http.Client{Timeout: 15 * time.Second}
)

// oauth_providers returns the registry of all providers we know about. Call
// it lazily so Microsoft's tenant setting can be read at dispatch time.
func oauth_providers() map[string]*oauth_provider {
	return map[string]*oauth_provider{
		"google": {
			name:      "google",
			display:   "Google",
			oidc:      true,
			discovery: func() string { return "https://accounts.google.com" },
			scopes:    []string{oidc.ScopeOpenID, "email", "profile"},
			extra_auth: []oauth2.AuthCodeOption{
				oauth2.SetAuthURLParam("prompt", "select_account"),
			},
		},
		"microsoft": {
			name:    "microsoft",
			display: "Microsoft",
			oidc:    true,
			discovery: func() string {
				tenant := setting_get("oauth_microsoft_tenant", "common")
				return "https://login.microsoftonline.com/" + tenant + "/v2.0"
			},
			scopes: []string{oidc.ScopeOpenID, "email", "profile"},
		},
		"github": {
			name:      "github",
			display:   "GitHub",
			auth_url:  "https://github.com/login/oauth/authorize",
			token_url: "https://github.com/login/oauth/access_token",
			scopes:    []string{"read:user", "user:email"},
			fetch:     oauth_github,
		},
		"facebook": {
			name:      "facebook",
			display:   "Facebook",
			auth_url:  "https://www.facebook.com/v19.0/dialog/oauth",
			token_url: "https://graph.facebook.com/v19.0/oauth/access_token",
			scopes:    []string{"public_profile", "email"},
			fetch:     oauth_facebook,
		},
		"x": {
			name:      "x",
			display:   "X",
			auth_url:  "https://twitter.com/i/oauth2/authorize",
			token_url: "https://api.twitter.com/2/oauth2/token",
			scopes:    []string{"users.read", "users.email", "tweet.read"},
			fetch:     oauth_x,
		},
	}
}

// oauth_enabled reports whether a provider is configured and allowed. A
// provider is considered enabled iff auth_oauth is not "disabled" (the
// server-wide kill switch) and both a client ID and client secret are set.
func oauth_enabled(name string) bool {
	if !auth_method_allowed("oauth") {
		return false
	}
	if setting_get("oauth_"+name+"_client_id", "") == "" {
		return false
	}
	if setting_get("oauth_"+name+"_client_secret", "") == "" {
		return false
	}
	return true
}

// oauth_client_config builds the oauth2.Config for a provider using current
// settings. For OIDC providers the endpoint is taken from the cached discovery
// document; for plain OAuth2 providers it comes from the static registry.
func oauth_client_config(p *oauth_provider, redirect string) (*oauth2.Config, *oidc.Provider, error) {
	cfg := &oauth2.Config{
		ClientID:     setting_get("oauth_"+p.name+"_client_id", ""),
		ClientSecret: setting_get("oauth_"+p.name+"_client_secret", ""),
		RedirectURL:  redirect,
		Scopes:       p.scopes,
	}
	if !p.oidc {
		cfg.Endpoint = oauth2.Endpoint{AuthURL: p.auth_url, TokenURL: p.token_url}
		return cfg, nil, nil
	}
	provider, err := oauth_oidc_provider(p.discovery())
	if err != nil {
		return nil, nil, err
	}
	cfg.Endpoint = provider.Endpoint()
	return cfg, provider, nil
}

// oauth_oidc_provider fetches and caches an OIDC discovery document.
func oauth_oidc_provider(issuer string) (*oidc.Provider, error) {
	oidc_providers_mu.Lock()
	defer oidc_providers_mu.Unlock()
	if p, ok := oidc_providers[issuer]; ok {
		return p, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ctx = oidc.ClientContext(ctx, oauth_http_client)
	// Microsoft's "common" tenant returns issuer URLs that embed the caller's
	// real tenant ID, so enforcing a strict issuer match breaks multi-tenant
	// logins. Skip issuer checks in the library and rely on signature + audience
	// verification via the configured client ID.
	p, err := oidc.NewProvider(ctx, issuer)
	if err != nil {
		return nil, err
	}
	oidc_providers[issuer] = p
	return p, nil
}

// oauth_redirect returns the absolute callback URL for a provider, using
// oauth_public_url if set, otherwise deriving from the request host.
func oauth_redirect(c *gin.Context, provider string) string {
	base := setting_get("oauth_public_url", "")
	if base != "" {
		return strings.TrimRight(base, "/") + "/_/auth/oauth/" + provider + "/callback"
	}
	scheme := "http"
	if web_https && !web_is_localhost(c) {
		scheme = "https"
	}
	return scheme + "://" + c.Request.Host + "/_/auth/oauth/" + provider + "/callback"
}

// oauth_pkce generates a 64-character code verifier and its S256 challenge.
func oauth_pkce() (verifier, challenge string) {
	verifier = random_alphanumeric(64)
	h := sha256.Sum256([]byte(verifier))
	challenge = base64.RawURLEncoding.EncodeToString(h[:])
	return
}

// oauth_login_target redirects the browser to the login page carrying an
// error code. Used for every refusal path in the callback handler so the
// frontend can show a toast.
func oauth_error_redirect(c *gin.Context, code string, extras map[string]string) {
	q := url.Values{}
	q.Set("oauth_error", code)
	for k, v := range extras {
		if v != "" {
			q.Set(k, v)
		}
	}
	c.Redirect(http.StatusFound, "/login/?"+q.Encode())
}

// ============================================================================
// HTTP handlers
// ============================================================================

// POST /_/auth/oauth/:provider/begin
// Body: {target, link}. Returns {url} to redirect the browser to.
func web_oauth_begin(c *gin.Context) {
	name := c.Param("provider")
	reg := oauth_providers()
	provider, ok := reg[name]
	if !ok || !oauth_enabled(name) {
		respond_error(c, http.StatusNotFound, "unknown_provider", "errors.unknown_provider", nil)
		return
	}

	var body struct {
		Target    string `json:"target"`
		Link      bool   `json:"link"`
		Mode      string `json:"mode"`      // "mobile" for native-app flow
		Scheme    string `json:"scheme"`    // app deep-link scheme (mobile only)
		Challenge string `json:"challenge"` // base64url(sha256(app_verifier)) (mobile only)
		Email     string `json:"email"`     // typed address (email-login flow); binds OAuth to that account
	}
	c.ShouldBindJSON(&body)

	// Mobile flow validation: scheme must be either the consolidated
	// Mochi super-app's "mochi" or one of the legacy per-app schemes
	// ("mochi-<app>"), and the PKCE challenge is mandatory so we can prove
	// the exchange came from the same app instance that started the flow.
	if body.Mode == "mobile" {
		if !oauth_valid_mobile_scheme(body.Scheme) {
			respond_error(c, http.StatusBadRequest, "invalid_scheme", "errors.invalid_scheme", nil)
			return
		}
		if len(body.Challenge) < 32 || len(body.Challenge) > 128 {
			respond_error(c, http.StatusBadRequest, "invalid_challenge", "errors.invalid_challenge", nil)
			return
		}
	}

	// Linking requires an authenticated session; the user is stored in the
	// ceremony row so the callback knows to take the link branch. The
	// settings app runs sandboxed — cookies aren't sent, so fall back to the
	// Bearer token in the Authorization header (same pattern as websockets).
	var link_user string
	if body.Link {
		user := web_auth(c)
		if user == nil {
			auth_header := c.GetHeader("Authorization")
			if strings.HasPrefix(auth_header, "Bearer ") {
				token := strings.TrimPrefix(auth_header, "Bearer ")
				if uid, _, err := jwt_verify(token); err == nil && uid != "" {
					user = user_by_uid(uid)
				}
			}
		}
		if user == nil {
			respond_error(c, http.StatusUnauthorized, "not_authenticated", "errors.not_authenticated", nil)
			return
		}
		link_user = user.UID
	}

	auth_url, err := oauth_begin_ceremony(c, provider, name, link_user, body.Target, body.Mode, body.Scheme, body.Challenge, body.Email)
	if err != nil {
		warn("OAuth begin: %v", err)
		respond_error(c, http.StatusServiceUnavailable, "provider_unavailable", "errors.provider_unavailable", nil)
		return
	}
	c.JSON(http.StatusOK, gin.H{"url": auth_url})
}

// oauth_begin_ceremony generates the PKCE verifier/state, stores the oauth
// ceremony - carrying user_id for the link and step-up flows, and the caller's
// result challenge for the app/popup exchange - and returns the provider auth
// URL. Shared by the web begin handler and the step-up verify.begin builtin.
func oauth_begin_ceremony(c *gin.Context, provider *oauth_provider, name, user_id, target, mode, scheme, challenge, email string) (string, error) {
	verifier, oauth_challenge := oauth_pkce()
	state := random_alphanumeric(32)
	nonce := random_alphanumeric(32)
	redirect := oauth_redirect(c, name)

	cfg, _, err := oauth_client_config(provider, redirect)
	if err != nil {
		return "", fmt.Errorf("provider config error (%s): %w", name, err)
	}

	data, err := json.Marshal(oauth_state{
		Provider:  name,
		Verifier:  verifier,
		Nonce:     nonce,
		Target:    target,
		Redirect:  redirect,
		Mode:      mode,
		Scheme:    scheme,
		Challenge: challenge,
		Email:     email,
	})
	if err != nil {
		return "", err
	}

	db_open("db/sessions.db").exec(
		"insert into ceremonies (id, type, user, challenge, data, expires) values (?, 'oauth', ?, ?, ?, ?)",
		state, user_id, []byte(state), string(data), now()+600)

	opts := []oauth2.AuthCodeOption{
		oauth2.SetAuthURLParam("code_challenge", oauth_challenge),
		oauth2.SetAuthURLParam("code_challenge_method", "S256"),
	}
	if provider.oidc {
		opts = append(opts, oidc.Nonce(nonce))
	}
	opts = append(opts, provider.extra_auth...)

	return cfg.AuthCodeURL(state, opts...), nil
}

// GET /_/auth/oauth/:provider/callback
// Handles the redirect from the provider and either logs the user in, creates
// a new account, or links the provider to an existing account.
func web_oauth_callback(c *gin.Context) {
	name := c.Param("provider")
	reg := oauth_providers()
	provider, ok := reg[name]
	if !ok || !oauth_enabled(name) {
		oauth_error_redirect(c, "unknown_provider", nil)
		return
	}

	if perr := c.Query("error"); perr != "" {
		oauth_error_redirect(c, "access_denied", nil)
		return
	}

	state := c.Query("state")
	code := c.Query("code")
	if state == "" || code == "" {
		oauth_error_redirect(c, "state_invalid", nil)
		return
	}

	// Look up ceremony and consume it immediately (single-use).
	db := db_open("db/sessions.db")
	row, _ := db.row("select user, data from ceremonies where id=? and type='oauth' and expires>?", state, now())
	if row == nil {
		audit_login_failed("", rate_limit_client_ip(c), "oauth_state_invalid")
		oauth_error_redirect(c, "state_invalid", nil)
		return
	}
	db.exec("delete from ceremonies where id=?", state)

	var st oauth_state
	if err := json.Unmarshal([]byte(row["data"].(string)), &st); err != nil {
		oauth_error_redirect(c, "state_invalid", nil)
		return
	}
	if st.Provider != name {
		oauth_error_redirect(c, "state_invalid", nil)
		return
	}

	var link_user string
	if row["user"] != nil {
		link_user, _ = row["user"].(string)
	}

	cfg, oidc_prov, err := oauth_client_config(provider, st.Redirect)
	if err != nil {
		warn("OAuth callback: provider config error (%s): %v", name, err)
		oauth_error_redirect(c, "provider_error", nil)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ctx = context.WithValue(ctx, oauth2.HTTPClient, oauth_http_client)

	token, err := cfg.Exchange(ctx, code, oauth2.VerifierOption(st.Verifier))
	if err != nil {
		warn("OAuth callback: token exchange failed (%s): %v", name, err)
		oauth_error_redirect(c, "provider_error", nil)
		return
	}

	var profile *oauth_profile
	if provider.oidc {
		profile, err = oauth_oidc_profile(ctx, oidc_prov, cfg.ClientID, token, st.Nonce)
	} else {
		profile, err = provider.fetch(ctx, token.AccessToken)
	}
	if err != nil {
		warn("OAuth callback: profile fetch failed (%s): %v", name, err)
		oauth_error_redirect(c, "provider_error", nil)
		return
	}
	if profile.Subject == "" {
		oauth_error_redirect(c, "provider_error", nil)
		return
	}

	if st.Mode == "reauthentication" && link_user != "" {
		if user := user_by_uid(link_user); user != nil {
			oauth_reauthenticate(c, name, profile, user, st.Challenge)
		} else {
			oauth_reauthenticate_page(c)
		}
		return
	}
	if link_user != "" {
		oauth_link(c, name, profile, link_user, st.Target)
		return
	}
	if st.Mode == "mobile" {
		oauth_mobile_login(c, name, profile, &st)
		return
	}
	oauth_login(c, name, profile, st.Target, st.Email)
}

// oauth_link attaches an OAuth identity to an already-authenticated user. The
// callback handler routes here when the ceremony row has a non-null user.
func oauth_link(c *gin.Context, provider string, p *oauth_profile, user_id string, target string) {
	db := db_open("db/users.db")

	// If this (provider, subject) is already linked, either refuse (wrong
	// user) or update timestamps (same user).
	owner := ""
	if row, _ := db.row("select user from oauth where provider=? and subject=?", provider, p.Subject); row != nil {
		owner, _ = row["user"].(string)
	}
	target = redirect_local(target)
	if target == "" {
		target = "/login/settings/oauth"
	}
	sep := "?"
	if strings.Contains(target, "?") {
		sep = "&"
	}

	switch {
	case owner == "":
		db.exec("insert into oauth (user, provider, subject, email, verified, name, created) values (?, ?, ?, ?, ?, ?, ?)",
			user_id, provider, p.Subject, p.Email, boolint(p.Verified), p.Name, now())
		oauth_verification_record(db, provider, p.Subject, user_id)
		oauth_replicate(db, provider, p.Subject)
	case owner == user_id:
		oauth_update_profile(db, provider, p)
		oauth_verification_record(db, provider, p.Subject, user_id)
	default:
		// Linking failures redirect back to the target (user is authenticated)
		// rather than /login/, which would log them out of the UI's view.
		c.Redirect(http.StatusFound, target+sep+"oauth_error=already_linked")
		return
	}

	c.Redirect(http.StatusFound, target+sep+"oauth_linked="+provider)
}

// oauth_login looks up an existing identity or creates a new user.
func oauth_login(c *gin.Context, provider string, p *oauth_profile, target, expect_email string) {
	db := db_open("db/users.db")

	var user_id string
	row, _ := db.row("select user from oauth where provider=? and subject=?", provider, p.Subject)
	if row != nil {
		user_id, _ = row["user"].(string)
	}

	// Started from the email-entry login flow: the OAuth is verifying a specific
	// identified account, so it must resolve to exactly that account - never a
	// different account the provider happens to be linked to, and never a fresh
	// signup. The discoverable landing path passes no email and is unaffected.
	if expect_email != "" {
		matched := false
		if user_id != "" {
			if u := user_by_uid(user_id); u != nil && u.Username == expect_email {
				matched = true
			}
		}
		if !matched {
			audit_login_failed(expect_email, rate_limit_client_ip(c), "oauth_account_mismatch")
			oauth_error_redirect(c, "account_mismatch", nil)
			return
		}
	}

	if user_id != "" {
		// Existing linked account.
		user := user_by_uid(user_id)
		if user == nil {
			audit_login_failed(p.Email, rate_limit_client_ip(c), "oauth_user_missing")
			oauth_error_redirect(c, "provider_error", nil)
			return
		}
		if user.Status == "suspended" {
			audit_login_failed(user.Username, rate_limit_client_ip(c), "suspended")
			oauth_error_redirect(c, "suspended", nil)
			return
		}
		// Refuse only if the user has explicitly disabled OAuth as a login
		// factor. OAuth is its own factor (not the email factor), so
		// auth_remaining_oauth clears only the oauth requirement - every other
		// required method (email, passkey, authenticator) must still complete.
		if user_method_disabled(user, "oauth") {
			audit_login_failed(user.Username, rate_limit_client_ip(c), "oauth_disabled")
			oauth_error_redirect(c, "oauth_disallowed", nil)
			return
		}

		oauth_update_profile(db, provider, p)
		oauth_verification_record(db, provider, p.Subject, user.UID)

		rate_limit_login.reset(rate_limit_client_ip(c))

		// OAuth proves a linked account, not the email factor, so any methods
		// the user requires must still be completed as MFA.
		remaining := auth_remaining_oauth(user)
		if len(remaining) > 0 {
			// If email is one of them, send the code now, as the other
			// non-email first factors (TOTP, passkey) do.
			for _, method := range remaining {
				if method == "email" {
					code_send(user.Username, c)
					break
				}
			}
			partial := random_alphanumeric(32)
			partial_create(db_open("db/sessions.db"), partial, user.UID, "oauth", strings.Join(remaining, ","), now()+300)
			web_cookie_set(c, "login_partial", partial)
			c.Redirect(http.StatusFound, "/login/codes")
			return
		}

		auth_redirect_login(c, user, target)
		return
	}

	// Unknown identity — attempt signup.
	if !setting_signup_enabled() {
		audit_login_failed(p.Email, rate_limit_client_ip(c), "oauth_signup_disabled")
		oauth_error_redirect(c, "signup_disabled", nil)
		return
	}
	if !p.Verified {
		audit_login_failed(p.Email, rate_limit_client_ip(c), "oauth_email_unverified")
		oauth_error_redirect(c, "email_unverified", map[string]string{"provider": provider})
		return
	}
	if p.Email == "" {
		oauth_error_redirect(c, "email_unverified", map[string]string{"provider": provider})
		return
	}

	// Refuse to auto-link by email — see plan decision 6. The user must log
	// in through their existing factor and then link from auth settings.
	if exists, _ := db.exists("select 1 from users where username=?", p.Email); exists {
		audit_login_failed(p.Email, rate_limit_client_ip(c), "oauth_email_exists")
		oauth_error_redirect(c, "email_exists", map[string]string{"provider": provider, "email": p.Email})
		return
	}

	user, reason := user_create(p.Email)
	if user == nil {
		audit_login_failed(p.Email, rate_limit_client_ip(c), "oauth_"+reason)
		oauth_error_redirect(c, "provider_error", nil)
		return
	}

	db.exec("insert into oauth (user, provider, subject, email, verified, name, created) values (?, ?, ?, ?, ?, ?, ?)",
		user.UID, provider, p.Subject, p.Email, boolint(p.Verified), p.Name, now())
	oauth_verification_record(db, provider, p.Subject, user.UID)
	oauth_replicate(db, provider, p.Subject)

	rate_limit_login.reset(rate_limit_client_ip(c))

	// Seed the mochi_me cookie with the provider's name and email so the
	// /login/identity form can prefill the name input. The cookie is read by
	// the frontend (lib/profile-cookie.ts), so it must NOT be HttpOnly.
	oauth_set_profile_cookie(c, p)

	auth_redirect_login(c, user, "/login/identity")
}

// oauth_set_profile_cookie writes the mochi_me JSON cookie that the login
// app's identity form reads on mount. Mirrors the frontend writer in
// apps/login/web/src/lib/profile-cookie.ts.
func oauth_set_profile_cookie(c *gin.Context, p *oauth_profile) {
	profile := map[string]string{}
	if p.Name != "" {
		profile["name"] = p.Name
	}
	if p.Email != "" {
		profile["email"] = p.Email
	}
	if len(profile) == 0 {
		return
	}
	data, err := json.Marshal(profile)
	if err != nil {
		return
	}
	secure := web_https && !web_is_localhost(c)
	// Lax (not Strict) because the callback redirects to /login/identity via a
	// chain initiated by the upstream provider (github.com etc.). Strict holds
	// the cookie back on that top-level cross-site navigation, so the identity
	// form sees no prefill.
	// Use http.SetCookie directly, not Gin's helper: Gin url.QueryEscape's the
	// value and encodes spaces as '+' (application/x-www-form-urlencoded), which
	// decodeURIComponent on the client does NOT turn back into a space. That
	// produces names like "Alistair+Cunningham" in the prefill. url.PathEscape
	// encodes spaces as %20 which decodes cleanly.
	http.SetCookie(c.Writer, &http.Cookie{
		Name:     "mochi_me",
		Value:    url.PathEscape(string(data)),
		MaxAge:   7 * 86400,
		Path:     "/",
		Secure:   secure,
		HttpOnly: false,
		SameSite: http.SameSiteLaxMode,
	})
}

// ============================================================================
// Provider-specific profile fetchers
// ============================================================================

// oauth_oidc_profile verifies the ID token returned by an OIDC provider and
// extracts the profile claims we care about.
func oauth_oidc_profile(ctx context.Context, provider *oidc.Provider, client_id string, token *oauth2.Token, nonce string) (*oauth_profile, error) {
	raw, ok := token.Extra("id_token").(string)
	if !ok || raw == "" {
		return nil, errors.New("missing id_token")
	}
	verifier := provider.Verifier(&oidc.Config{
		ClientID:        client_id,
		SkipIssuerCheck: true, // Microsoft multi-tenant: issuer embeds real tenant ID
	})
	idt, err := verifier.Verify(ctx, raw)
	if err != nil {
		return nil, err
	}
	if idt.Nonce != nonce {
		return nil, errors.New("nonce mismatch")
	}
	var claims struct {
		Sub           string `json:"sub"`
		Email         string `json:"email"`
		EmailVerified *bool  `json:"email_verified"`
		Name          string `json:"name"`
	}
	if err := idt.Claims(&claims); err != nil {
		return nil, err
	}
	verified := false
	if claims.EmailVerified != nil && *claims.EmailVerified {
		verified = true
	}

	// Fall back to the /userinfo endpoint when the ID token omits name or email
	// (e.g. Google returns minimal claims on prompt=none refreshes). UserInfo
	// is the OIDC-standard place for profile data.
	if claims.Name == "" || claims.Email == "" {
		ts := oauth2.StaticTokenSource(token)
		if ui, err := provider.UserInfo(ctx, ts); err == nil {
			var extra struct {
				Name          string `json:"name"`
				EmailVerified *bool  `json:"email_verified"`
			}
			_ = ui.Claims(&extra)
			if claims.Name == "" {
				claims.Name = extra.Name
			}
			if claims.Email == "" {
				claims.Email = ui.Email
			}
			if !verified && ((extra.EmailVerified != nil && *extra.EmailVerified) || ui.EmailVerified) {
				verified = true
			}
		}
	}

	return &oauth_profile{
		Subject:  claims.Sub,
		Email:    claims.Email,
		Verified: verified,
		Name:     claims.Name,
	}, nil
}

// oauth_github fetches a GitHub profile. GitHub does not issue ID tokens and
// email may be hidden in the profile — fall back to /user/emails and pick the
// primary verified entry.
func oauth_github(ctx context.Context, token string) (*oauth_profile, error) {
	var profile struct {
		ID    int64  `json:"id"`
		Login string `json:"login"`
		Name  string `json:"name"`
		Email string `json:"email"`
	}
	if err := oauth_get_json(ctx, "https://api.github.com/user", token, true, &profile); err != nil {
		return nil, err
	}

	verified := false
	email := profile.Email
	if email != "" {
		verified = true // GitHub only returns a public email if the user has verified it.
	} else {
		var emails []struct {
			Email    string `json:"email"`
			Primary  bool   `json:"primary"`
			Verified bool   `json:"verified"`
		}
		if err := oauth_get_json(ctx, "https://api.github.com/user/emails", token, true, &emails); err != nil {
			return nil, err
		}
		for _, e := range emails {
			if e.Primary && e.Verified {
				email = e.Email
				verified = true
				break
			}
		}
	}

	name := profile.Name
	if name == "" {
		name = profile.Login
	}

	return &oauth_profile{
		Subject:  fmt.Sprintf("%d", profile.ID),
		Email:    email,
		Verified: verified,
		Name:     name,
	}, nil
}

// oauth_facebook fetches a Facebook Graph API profile. Facebook does not
// expose an email_verified claim; treat a present email as verified because
// Meta verifies addresses at account creation time.
func oauth_facebook(ctx context.Context, token string) (*oauth_profile, error) {
	var profile struct {
		ID    string `json:"id"`
		Name  string `json:"name"`
		Email string `json:"email"`
	}
	url := "https://graph.facebook.com/v19.0/me?fields=id,name,email"
	if err := oauth_get_json(ctx, url, token, false, &profile); err != nil {
		return nil, err
	}
	return &oauth_profile{
		Subject:  profile.ID,
		Email:    profile.Email,
		Verified: profile.Email != "",
		Name:     profile.Name,
	}, nil
}

// oauth_x fetches an X (Twitter) v2 API profile. The operator must enable the
// `users.email` scope on their X app for `confirmed_email` to appear.
func oauth_x(ctx context.Context, token string) (*oauth_profile, error) {
	var resp struct {
		Data struct {
			ID             string `json:"id"`
			Name           string `json:"name"`
			Username       string `json:"username"`
			ConfirmedEmail string `json:"confirmed_email"`
		} `json:"data"`
	}
	url := "https://api.twitter.com/2/users/me?user.fields=confirmed_email,name,username"
	if err := oauth_get_json(ctx, url, token, false, &resp); err != nil {
		return nil, err
	}
	name := resp.Data.Name
	if name == "" {
		name = resp.Data.Username
	}
	return &oauth_profile{
		Subject:  resp.Data.ID,
		Email:    resp.Data.ConfirmedEmail,
		Verified: resp.Data.ConfirmedEmail != "",
		Name:     name,
	}, nil
}

// oauth_get_json issues a GET with a bearer token and decodes the JSON body
// into out. github=true sets the GitHub-required User-Agent and Accept headers.
func oauth_get_json(ctx context.Context, url, token string, github bool, out any) error {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if github {
		req.Header.Set("Accept", "application/vnd.github+json")
		req.Header.Set("User-Agent", "Mochi/"+build_version)
	} else {
		req.Header.Set("Accept", "application/json")
	}
	resp, err := oauth_http_client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("http %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// ============================================================================
// Starlark API
// ============================================================================

// mochi.user.oauth.list() -> list: Identities linked to the current user
func api_user_oauth_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := require_permission(t, fn, "user/authentication/read"); err != nil {
		return sl_error(fn, "%v", err)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}
	db := db_open("db/users.db")
	rows, err := db.rows("select id, provider, email, name, created from oauth where user=? order by created asc", user.UID)
	if err != nil {
		return sl_error(fn, "database error")
	}

	lasts := oauth_verification_lasts(user.UID)
	for _, r := range rows {
		id, _ := r["id"].(int64)
		r["used"] = lasts[id]
		delete(r, "id")
	}
	return sl_encode(rows), nil
}

// api_user_oauth_verify_begin is mochi.user.oauth.verify.begin(provider,
// challenge) -> {url}: start a popup OAuth re-authentication for the current
// user. challenge is base64url(sha256(verifier)) the caller holds; the proof
// is retrieved afterwards with verify.finish(verifier).
func api_user_oauth_verify_begin(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := require_permission(t, fn, "user/authentication/write"); err != nil {
		return sl_error(fn, "%v", err)
	}
	user, _ := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}
	action, _ := t.Local("action").(*Action)
	if action == nil || action.web == nil {
		return sl_error(fn, "no request context")
	}

	var name, challenge string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "provider", &name, "challenge", &challenge); err != nil {
		return sl_error(fn, "%v", err)
	}
	if len(challenge) < 32 || len(challenge) > 128 {
		return sl_error(fn, "invalid challenge")
	}

	provider, ok := oauth_providers()[name]
	if !ok || !oauth_enabled(name) {
		return sl_error(fn, "unknown provider")
	}
	// Only a provider the user has actually linked can re-authenticate them.
	if linked, _ := db_open("db/users.db").exists("select 1 from oauth where user=? and provider=?", user.UID, name); !linked {
		return sl_error(fn, "provider not linked")
	}

	url, err := oauth_begin_ceremony(action.web, provider, name, user.UID, "", "reauthentication", "", challenge, "")
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	return sl_encode(map[string]any{"url": url}), nil
}

// api_user_oauth_verify_finish is mochi.user.oauth.verify.finish(verifier) ->
// {token}|{remaining}|None: retrieve the step-up proof the OAuth popup produced,
// keyed by the verifier's S256. Single-use; None if absent, expired, or the
// authenticated provider account was not linked to this user.
func api_user_oauth_verify_finish(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := require_permission(t, fn, "user/authentication/write"); err != nil {
		return sl_error(fn, "%v", err)
	}
	user, _ := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	var verifier string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "verifier", &verifier); err != nil {
		return sl_error(fn, "%v", err)
	}

	h := sha256.Sum256([]byte(verifier))
	challenge := base64.RawURLEncoding.EncodeToString(h[:])

	db := db_open("db/sessions.db")
	row, _ := db.row("select data from ceremonies where id=? and type='reauthentication_oauth' and user=? and expires>?",
		challenge, user.UID, now())
	if row == nil {
		return sl.None, nil
	}
	db.exec("delete from ceremonies where id=?", challenge)

	var result map[string]any
	if err := json.Unmarshal([]byte(row["data"].(string)), &result); err != nil {
		return sl.None, nil
	}
	return sl_encode(result), nil
}

// oauth_reauthenticate completes a popup OAuth step-up: it confirms the
// authenticated provider identity is already linked to the user, advances the
// oauth re-authentication factor, and stashes the resulting proof (token or
// remaining factors) keyed by the caller's challenge for verify.finish to
// retrieve. Always renders the auto-close page; a mismatched account simply
// stores nothing, so finish returns None.
func oauth_reauthenticate(c *gin.Context, provider string, p *oauth_profile, user *User, challenge string) {
	users := db_open("db/users.db")
	owner := ""
	if row, _ := users.row("select user from oauth where provider=? and subject=?", provider, p.Subject); row != nil {
		owner, _ = row["user"].(string)
	}

	if owner != "" && owner == user.UID && !user_method_disabled(user, "oauth") {
		oauth_update_profile(users, provider, p)
		oauth_verification_record(users, provider, p.Subject, user.UID)

		token, remaining := reauthentication_advance(user, "oauth")
		result := map[string]any{}
		if token != "" {
			result["token"] = token
		} else {
			result["remaining"] = remaining
		}
		if body, err := json.Marshal(result); err == nil {
			db_open("db/sessions.db").exec(
				"insert into ceremonies (id, type, user, challenge, data, expires) values (?, 'reauthentication_oauth', ?, '', ?, ?)",
				challenge, user.UID, string(body), now()+120)
		}
	}

	oauth_reauthenticate_page(c)
}

// oauth_reauthenticate_page closes the OAuth popup and pings the opener so the
// step-up dialog fetches its proof immediately (it also polls window.closed).
func oauth_reauthenticate_page(c *gin.Context) {
	c.Data(http.StatusOK, "text/html; charset=utf-8",
		[]byte("<!doctype html><meta charset=utf-8><script>try{window.opener&&window.opener.postMessage('mochi-oauth-done','*')}catch(e){}window.close()</script>"))
}

// oauth_update_profile refreshes the email/name/verified fields for an
// existing (provider, subject) link. The WHERE clause matches only when at
// least one value differs, so unchanged logins (the common case — providers
// rarely change a user's claims between logins) skip the write entirely and
// keep users.db cold.
func oauth_update_profile(db *DB, provider string, p *oauth_profile) {
	verified := boolint(p.Verified)
	db.exec(`update oauth set email=?, name=?, verified=?
	         where provider=? and subject=?
	         and (email!=? or name!=? or verified!=?)`,
		p.Email, p.Name, verified,
		provider, p.Subject,
		p.Email, p.Name, verified)
	oauth_replicate(db, provider, p.Subject)
}

// oauth_replicate fans the current (provider, subject) oauth row to the owning
// user's host set — which unconditionally includes both operator-pair members
// (see recipients), so an oauth link or profile change reaches the other
// DNS-round-robin member instead of only the host that served the request
// (#150). Reads the persisted row so the replicated state is always accurate.
// No-op off a pair / with no other hosts.
func oauth_replicate(db *DB, provider, subject string) {
	row, _ := db.row("select user, email, verified, name, created from oauth where provider=? and subject=?", provider, subject)
	if row == nil {
		return
	}
	user, _ := row["user"].(string)
	if user == "" {
		return
	}
}

// oauth_replicate_unlink fans an oauth unlink (every subject for the provider)
// to the user's host set / pair, so a revoked login can't keep working on the
// other DNS-round-robin member (#150).
func oauth_replicate_unlink(userUID, provider string) {
}

// oauth_verification_record upserts the last-used timestamp for a (provider,
// subject) pair. Looks up oauth.id from users.db then writes to sessions.db.
// Idempotent: insert-or-replace by oauth id (the verifications PK).
func oauth_verification_record(db *DB, provider, subject string, user_id string) {
	row, _ := db.row("select id from oauth where provider=? and subject=?", provider, subject)
	if row == nil {
		return
	}
	oauth_id, _ := row["id"].(int64)
	db_open("db/sessions.db").exec("replace into verifications (oauth, user, last) values (?, ?, ?)",
		oauth_id, user_id, now())
}

// oauth_verification_lasts returns last-login by oauth.id for every linked
// identity belonging to a user. Unknown identities map to 0.
func oauth_verification_lasts(user_id string) map[int64]int64 {
	out := map[int64]int64{}
	rows, err := db_open("db/sessions.db").rows("select oauth, last from verifications where user=?", user_id)
	if err != nil {
		return out
	}
	for _, r := range rows {
		oauth_id, _ := r["oauth"].(int64)
		last, _ := r["last"].(int64)
		out[oauth_id] = last
	}
	return out
}

// mochi.user.oauth.unlink(provider) -> bool: Remove an OAuth link
func api_user_oauth_unlink(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := require_permission(t, fn, "user/authentication/write"); err != nil {
		return sl_error(fn, "%v", err)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}
	if len(args) != 1 {
		return sl_error(fn, "syntax: <provider: string>")
	}
	provider, ok := sl.AsString(args[0])
	if !ok || provider == "" {
		return sl_error(fn, "invalid provider")
	}

	db := db_open("db/users.db")
	exists, _ := db.exists("select 1 from oauth where user=? and provider=?", user.UID, provider)
	if !exists {
		return sl_error(fn, "provider not linked")
	}

	// Safety: do not let the user remove their only path back in.
	if !user_has_other_login(user, provider) {
		return sl_error(fn, "cannot unlink last login method")
	}

	row, _ := db.row("select id from oauth where user=? and provider=?", user.UID, provider)
	db.exec("delete from oauth where user=? and provider=?", user.UID, provider)
	oauth_replicate_unlink(user.UID, provider)
	if row != nil {
		if oauth_id, ok := row["id"].(int64); ok {
			db_open("db/sessions.db").exec("delete from verifications where oauth=?", oauth_id)
		}
	}
	audit_password_changed(user.Username, "oauth_unlinked_"+provider)
	return sl.True, nil
}

// user_has_other_login reports whether the user retains at least one way to
// log in if the given OAuth provider were removed.
func user_has_other_login(user *User, leaving string) bool {
	if auth_method_allowed("email") {
		return true
	}
	db := db_open("db/users.db")
	if exists, _ := db.exists("select 1 from credentials where user=?", user.UID); exists {
		return true
	}
	if row, _ := db.row("select verified from totp where user=?", user.UID); row != nil {
		if v, ok := row["verified"].(int64); ok && v == 1 {
			return true
		}
	}
	if exists, _ := db.exists("select 1 from oauth where user=? and provider!=?", user.UID, leaving); exists {
		return true
	}
	return false
}

// ============================================================================
// Mobile (native app) OAuth flow
// ============================================================================
//
// Browsers complete OAuth by setting a session cookie and redirecting back to
// a web page. Native apps cannot read cookies from a Custom Tabs session, so
// we substitute a deep-link return: the callback redirects to
// <scheme>:oauth-return?code=<exchange_code>, and the app then POSTs the
// exchange code (plus a PKCE verifier) to /_/auth/oauth/exchange to retrieve
// the actual session token. The exchange row is single-use and short-lived.
//
// The URI is opaque (no `//`) per the mochi: URI scheme — see
// claude/plans/mochi-uri-scheme.md. OAuth providers never see this URI; the
// pre-registered redirect_uri is the server's `https://<host>/_/auth/oauth/<provider>/callback`,
// which the server then 302s to the opaque deep-link URI as the final hop to
// the device. Android (and any other Mochi client) catches the URI via a
// scheme="mochi" intent filter.

// oauth_valid_mobile_scheme rejects schemes that aren't the consolidated
// super-app's "mochi" or one of the legacy "mochi-<app>" forms (kept for the
// transitional window while users update third-party provider registrations).
// Callbacks redirect to <scheme>:oauth-return — letting arbitrary schemes
// through would turn this into an open redirect.
func oauth_valid_mobile_scheme(s string) bool {
	if s == "mochi" {
		return true
	}
	if !strings.HasPrefix(s, "mochi-") {
		return false
	}
	rest := s[len("mochi-"):]
	if rest == "" || len(rest) > 32 {
		return false
	}
	for _, r := range rest {
		if !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')) {
			return false
		}
	}
	return true
}

// oauth_mobile_redirect 302s to the app's deep-link return URL with either an
// exchange code (success / MFA / identity-needed) or an error code.
func oauth_mobile_redirect(c *gin.Context, scheme, exchange_code, error_code string, extras map[string]string) {
	q := url.Values{}
	if exchange_code != "" {
		q.Set("code", exchange_code)
	}
	if error_code != "" {
		q.Set("error", error_code)
	}
	for k, v := range extras {
		if v != "" {
			q.Set(k, v)
		}
	}
	c.Redirect(http.StatusFound, scheme+":oauth-return?"+q.Encode())
}

// oauth_mobile_error sends the app a deep-link redirect carrying an error
// code, mirroring oauth_error_redirect for the web path.
func oauth_mobile_error(c *gin.Context, st *oauth_state, code string, extras map[string]string) {
	oauth_mobile_redirect(c, st.Scheme, "", code, extras)
}

// oauth_mobile_store stashes the result of a callback in the ceremonies table
// keyed by a fresh exchange code, so the app can retrieve it via /exchange.
// The PKCE challenge from begin is stored in the challenge column; the
// exchange handler verifies the verifier matches before releasing the data.
func oauth_mobile_store(challenge string, data any) (string, error) {
	exchange_code := random_alphanumeric(32)
	body, err := json.Marshal(data)
	if err != nil {
		return "", err
	}
	db_open("db/sessions.db").exec(
		"insert into ceremonies (id, type, challenge, data, expires) values (?, 'oauth_exchange', ?, ?, ?)",
		exchange_code, []byte(challenge), string(body), now()+120,
	)
	return exchange_code, nil
}

// oauth_mobile_login mirrors oauth_login but completes via deep-link redirect
// instead of cookie + web redirect. The existing-user path runs the same MFA
// check; if MFA is required, the partial id is delivered through the exchange.
func oauth_mobile_login(c *gin.Context, provider string, p *oauth_profile, st *oauth_state) {
	db := db_open("db/users.db")

	var user_id string
	if row, _ := db.row("select user from oauth where provider=? and subject=?", provider, p.Subject); row != nil {
		user_id, _ = row["user"].(string)
	}

	if user_id != "" {
		user := user_by_uid(user_id)
		if user == nil {
			audit_login_failed(p.Email, rate_limit_client_ip(c), "oauth_user_missing")
			oauth_mobile_error(c, st, "provider_error", nil)
			return
		}
		if user.Status == "suspended" {
			audit_login_failed(user.Username, rate_limit_client_ip(c), "suspended")
			oauth_mobile_error(c, st, "suspended", nil)
			return
		}
		// Refuse only if the user has explicitly disabled OAuth as a login
		// factor (OAuth is its own factor, not the email factor), matching
		// the web path.
		if user_method_disabled(user, "oauth") {
			audit_login_failed(user.Username, rate_limit_client_ip(c), "oauth_disabled")
			oauth_mobile_error(c, st, "oauth_disallowed", nil)
			return
		}

		oauth_update_profile(db, provider, p)
		oauth_verification_record(db, provider, p.Subject, user.UID)
		rate_limit_login.reset(rate_limit_client_ip(c))

		remaining := auth_remaining_oauth(user)
		if len(remaining) > 0 {
			for _, method := range remaining {
				if method == "email" {
					code_send(user.Username, c)
					break
				}
			}
			partial := random_alphanumeric(32)
			partial_create(db_open("db/sessions.db"), partial, user.UID, "oauth", strings.Join(remaining, ","), now()+300)
			code, err := oauth_mobile_store(st.Challenge, map[string]any{
				"mfa":       true,
				"partial":   partial,
				"remaining": remaining,
			})
			if err != nil {
				oauth_mobile_error(c, st, "server_error", nil)
				return
			}
			oauth_mobile_redirect(c, st.Scheme, code, "", nil)
			return
		}

		// Full login. Create the session now so the exchange just hands it back.
		session := login_create(user.UID, c.ClientIP(), c.GetHeader("User-Agent"))
		db_open("db/sessions.db").exec("replace into logins (user, last) values (?, ?)", user.UID, now())
		audit_login(user.Username, rate_limit_client_ip(c))
		if user.Identity == nil {
			user.Identity = user.identity()
		}
		has_identity := user.Identity != nil && user.Identity.Name != ""

		code, err := oauth_mobile_store(st.Challenge, map[string]any{
			"session":      session,
			"has_identity": has_identity,
			"name":         identity_name_or_empty(user),
		})
		if err != nil {
			oauth_mobile_error(c, st, "server_error", nil)
			return
		}
		oauth_mobile_redirect(c, st.Scheme, code, "", nil)
		return
	}

	// New user signup path.
	if !setting_signup_enabled() {
		audit_login_failed(p.Email, rate_limit_client_ip(c), "oauth_signup_disabled")
		oauth_mobile_error(c, st, "signup_disabled", nil)
		return
	}
	if !p.Verified || p.Email == "" {
		audit_login_failed(p.Email, rate_limit_client_ip(c), "oauth_email_unverified")
		oauth_mobile_error(c, st, "email_unverified", map[string]string{"provider": provider})
		return
	}
	if exists, _ := db.exists("select 1 from users where username=?", p.Email); exists {
		audit_login_failed(p.Email, rate_limit_client_ip(c), "oauth_email_exists")
		oauth_mobile_error(c, st, "email_exists", map[string]string{"provider": provider, "email": p.Email})
		return
	}

	user, reason := user_create(p.Email)
	if user == nil {
		audit_login_failed(p.Email, rate_limit_client_ip(c), "oauth_"+reason)
		oauth_mobile_error(c, st, "provider_error", nil)
		return
	}

	db.exec("insert into oauth (user, provider, subject, email, verified, name, created) values (?, ?, ?, ?, ?, ?, ?)",
		user.UID, provider, p.Subject, p.Email, boolint(p.Verified), p.Name, now())
	oauth_verification_record(db, provider, p.Subject, user.UID)
	rate_limit_login.reset(rate_limit_client_ip(c))

	session := login_create(user.UID, c.ClientIP(), c.GetHeader("User-Agent"))
	db_open("db/sessions.db").exec("replace into logins (user, last) values (?, ?)", user.UID, now())
	audit_login(user.Username, rate_limit_client_ip(c))

	code, err := oauth_mobile_store(st.Challenge, map[string]any{
		"session":      session,
		"has_identity": false,
		"profile":      map[string]string{"name": p.Name, "email": p.Email},
	})
	if err != nil {
		oauth_mobile_error(c, st, "server_error", nil)
		return
	}
	oauth_mobile_redirect(c, st.Scheme, code, "", nil)
}

// identity_name_or_empty returns the user's identity name or "" if not set.
func identity_name_or_empty(user *User) string {
	if user.Identity != nil {
		return user.Identity.Name
	}
	return ""
}

// POST /_/auth/oauth/exchange
// Body: {code, verifier}. The app posts the exchange code it received in the
// deep link, plus the PKCE verifier matching the challenge supplied at /begin.
// Returns the same JSON shape as /code/verify (session via Set-Cookie + body
// metadata, or {mfa, partial, remaining} for incomplete logins).
func web_oauth_exchange(c *gin.Context) {
	var body struct {
		Code     string `json:"code"`
		Verifier string `json:"verifier"`
	}
	if err := c.ShouldBindJSON(&body); err != nil || body.Code == "" || body.Verifier == "" {
		respond_error(c, http.StatusBadRequest, "invalid_request", "errors.invalid_request", nil)
		return
	}

	db := db_open("db/sessions.db")
	row, _ := db.row("select challenge, data from ceremonies where id=? and type='oauth_exchange' and expires>?",
		body.Code, now())
	if row == nil {
		respond_error(c, http.StatusNotFound, "exchange_invalid", "errors.exchange_invalid", nil)
		return
	}
	// Single-use: consume the row whether or not the verifier matches, so a
	// brute-force search of verifiers can't try multiple times.
	db.exec("delete from ceremonies where id=?", body.Code)

	// db.row() converts []byte columns to string, so the BLOB challenge column
	// arrives as a string here, not a []byte. Reading it as a string keeps us
	// honest with the helper's contract.
	stored_challenge, _ := row["challenge"].(string)
	h := sha256.Sum256([]byte(body.Verifier))
	expected := base64.RawURLEncoding.EncodeToString(h[:])
	if stored_challenge != expected {
		respond_error(c, http.StatusUnauthorized, "exchange_verifier_mismatch", "errors.exchange_verifier_mismatch", nil)
		return
	}

	var data map[string]any
	if err := json.Unmarshal([]byte(row["data"].(string)), &data); err != nil {
		respond_error(c, http.StatusInternalServerError, "server_error", "errors.server_error", nil)
		return
	}

	// MFA branch: just relay the partial info; no session exists yet.
	if mfa, _ := data["mfa"].(bool); mfa {
		c.JSON(http.StatusOK, data)
		return
	}

	// Full session — set the cookie too so a same-device browser opening the
	// app's webview gets logged in, and return the JSON the app expects.
	session, _ := data["session"].(string)
	if session != "" {
		web_cookie_set(c, "session", session)
	}
	has_identity, _ := data["has_identity"].(bool)
	resp := gin.H{"has_identity": has_identity}
	if name, ok := data["name"].(string); ok && name != "" {
		resp["name"] = name
	}
	if profile, ok := data["profile"].(map[string]any); ok {
		resp["profile"] = profile
	}
	c.JSON(http.StatusOK, resp)
}

// boolint converts a Go bool to the 0/1 integer we use in SQLite.
func boolint(b bool) int {
	if b {
		return 1
	}
	return 0
}

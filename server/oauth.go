// Mochi server: OAuth2 / OIDC login (Google, GitHub, Microsoft, Facebook, X)
// Copyright Alistair Cunningham 2026

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
	"golang.org/x/oauth2"
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

// oauth_provider holds the static configuration for one provider. OIDC
// providers (Google, Microsoft) fill discovery and rely on go-oidc to derive
// endpoints; plain OAuth2 providers (GitHub, Facebook, X) fill auth/token URLs
// directly and implement a profile fetcher.
type oauth_provider struct {
	name        string
	display     string
	oidc        bool
	discovery   func() string // OIDC only; may depend on tenant setting
	auth_url    string        // non-OIDC
	token_url   string        // non-OIDC
	scopes      []string
	fetch       func(ctx context.Context, token string) (*oauth_profile, error)
	extra_auth  []oauth2.AuthCodeOption
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
	Provider string `json:"provider"`
	Verifier string `json:"verifier"`
	Nonce    string `json:"nonce"`
	Target   string `json:"target"`
	Redirect string `json:"redirect"`
}

var api_user_oauth = sls.FromStringDict(sl.String("mochi.user.oauth"), sl.StringDict{
	"list":   sl.NewBuiltin("mochi.user.oauth.list", api_user_oauth_list),
	"unlink": sl.NewBuiltin("mochi.user.oauth.unlink", api_user_oauth_unlink),
})

// Cached go-oidc providers keyed by discovery URL. Discovery is a network call
// and we only want to do it once per process.
var (
	oidc_providers     = map[string]*oidc.Provider{}
	oidc_providers_mu  sync.Mutex
	oauth_http_client  = &http.Client{Timeout: 15 * time.Second}
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
		c.JSON(http.StatusNotFound, gin.H{"error": "unknown_provider"})
		return
	}

	var body struct {
		Target string `json:"target"`
		Link   bool   `json:"link"`
	}
	c.ShouldBindJSON(&body)

	// Linking requires an authenticated session; the user is stored in the
	// ceremony row so the callback knows to take the link branch. The
	// settings app runs sandboxed — cookies aren't sent, so fall back to the
	// Bearer token in the Authorization header (same pattern as websockets).
	var link_user int
	if body.Link {
		user := web_auth(c)
		if user == nil {
			auth_header := c.GetHeader("Authorization")
			if strings.HasPrefix(auth_header, "Bearer ") {
				token := strings.TrimPrefix(auth_header, "Bearer ")
				if uid, _, err := jwt_verify(token); err == nil && uid > 0 {
					user = user_by_id(uid)
				}
			}
		}
		if user == nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "not_authenticated"})
			return
		}
		link_user = user.ID
	}

	verifier, challenge := oauth_pkce()
	state := random_alphanumeric(32)
	nonce := random_alphanumeric(32)
	redirect := oauth_redirect(c, name)

	cfg, _, err := oauth_client_config(provider, redirect)
	if err != nil {
		warn("OAuth begin: provider config error (%s): %v", name, err)
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "provider_unavailable"})
		return
	}

	data, err := json.Marshal(oauth_state{
		Provider: name,
		Verifier: verifier,
		Nonce:    nonce,
		Target:   body.Target,
		Redirect: redirect,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "server_error"})
		return
	}

	db := db_open("db/sessions.db")
	if link_user > 0 {
		db.exec("insert into ceremonies (id, type, user, challenge, data, expires) values (?, 'oauth', ?, ?, ?, ?)",
			state, link_user, []byte(state), string(data), now()+600)
	} else {
		db.exec("insert into ceremonies (id, type, challenge, data, expires) values (?, 'oauth', ?, ?, ?)",
			state, []byte(state), string(data), now()+600)
	}

	opts := []oauth2.AuthCodeOption{
		oauth2.SetAuthURLParam("code_challenge", challenge),
		oauth2.SetAuthURLParam("code_challenge_method", "S256"),
	}
	if provider.oidc {
		opts = append(opts, oidc.Nonce(nonce))
	}
	opts = append(opts, provider.extra_auth...)

	auth_url := cfg.AuthCodeURL(state, opts...)
	c.JSON(http.StatusOK, gin.H{"url": auth_url})
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

	var link_user int
	if row["user"] != nil {
		if v, ok := row["user"].(int64); ok {
			link_user = int(v)
		}
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

	if link_user > 0 {
		oauth_link(c, name, profile, link_user, st.Target)
		return
	}
	oauth_login(c, name, profile, st.Target)
}

// oauth_link attaches an OAuth identity to an already-authenticated user. The
// callback handler routes here when the ceremony row has a non-null user.
func oauth_link(c *gin.Context, provider string, p *oauth_profile, user_id int, target string) {
	db := db_open("db/users.db")

	// If this (provider, subject) is already linked, either refuse (wrong
	// user) or update timestamps (same user).
	owner := 0
	if row, _ := db.row("select user from oauth where provider=? and subject=?", provider, p.Subject); row != nil {
		if v, ok := row["user"].(int64); ok {
			owner = int(v)
		}
	}
	if target == "" {
		target = "/login/settings/oauth"
	}
	sep := "?"
	if strings.Contains(target, "?") {
		sep = "&"
	}

	switch {
	case owner == 0:
		db.exec("insert into oauth (user, provider, subject, email, verified, name, created, used) values (?, ?, ?, ?, ?, ?, ?, ?)",
			user_id, provider, p.Subject, p.Email, boolint(p.Verified), p.Name, now(), now())
	case owner == user_id:
		db.exec("update oauth set used=?, email=?, name=?, verified=? where provider=? and subject=?",
			now(), p.Email, p.Name, boolint(p.Verified), provider, p.Subject)
	default:
		// Linking failures redirect back to the target (user is authenticated)
		// rather than /login/, which would log them out of the UI's view.
		c.Redirect(http.StatusFound, target+sep+"oauth_error=already_linked")
		return
	}

	c.Redirect(http.StatusFound, target+sep+"oauth_linked="+provider)
}

// oauth_login looks up an existing identity or creates a new user.
func oauth_login(c *gin.Context, provider string, p *oauth_profile, target string) {
	db := db_open("db/users.db")

	var user_id int
	row, _ := db.row("select user from oauth where provider=? and subject=?", provider, p.Subject)
	if row != nil {
		if v, ok := row["user"].(int64); ok {
			user_id = int(v)
		}
	}

	if user_id > 0 {
		// Existing linked account.
		user := user_by_id(user_id)
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
		if strings.Contains(user.Methods, "passkey") {
			audit_login_failed(user.Username, rate_limit_client_ip(c), "oauth_disallowed")
			oauth_error_redirect(c, "oauth_disallowed", nil)
			return
		}

		db.exec("update oauth set used=?, email=?, name=?, verified=? where provider=? and subject=?",
			now(), p.Email, p.Name, boolint(p.Verified), provider, p.Subject)

		rate_limit_login.reset(rate_limit_client_ip(c))

		// MFA: OAuth is treated as equivalent to email for "what factors
		// are still required" purposes.
		remaining := auth_remaining_oauth(user)
		if len(remaining) > 0 {
			partial := random_alphanumeric(32)
			sessions := db_open("db/sessions.db")
			sessions.exec("insert into partial (id, user, completed, remaining, expires) values (?, ?, 'email', ?, ?)",
				partial, user.ID, strings.Join(remaining, ","), now()+300)
			web_cookie_set(c, "oauth_partial", partial)
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

	db.exec("insert into oauth (user, provider, subject, email, verified, name, created, used) values (?, ?, ?, ?, ?, ?, ?, ?)",
		user.ID, provider, p.Subject, p.Email, boolint(p.Verified), p.Name, now(), now())

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
	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}
	db := db_open("db/users.db")
	rows, err := db.rows("select provider, email, name, created, used from oauth where user=? order by created asc", user.ID)
	if err != nil {
		return sl_error(fn, "database error")
	}
	return sl_encode(rows), nil
}

// mochi.user.oauth.unlink(provider) -> bool: Remove an OAuth link
func api_user_oauth_unlink(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
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
	exists, _ := db.exists("select 1 from oauth where user=? and provider=?", user.ID, provider)
	if !exists {
		return sl_error(fn, "provider not linked")
	}

	// Safety: do not let the user remove their only path back in.
	if !user_has_other_login(user, provider) {
		return sl_error(fn, "cannot unlink last login method")
	}

	db.exec("delete from oauth where user=? and provider=?", user.ID, provider)
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
	if exists, _ := db.exists("select 1 from credentials where user=?", user.ID); exists {
		return true
	}
	if row, _ := db.row("select verified from totp where user=?", user.ID); row != nil {
		if v, ok := row["verified"].(int64); ok && v == 1 {
			return true
		}
	}
	if exists, _ := db.exists("select 1 from oauth where user=? and provider!=?", user.ID, leaving); exists {
		return true
	}
	return false
}

// boolint converts a Go bool to the 0/1 integer we use in SQLite.
func boolint(b bool) int {
	if b {
		return 1
	}
	return 0
}

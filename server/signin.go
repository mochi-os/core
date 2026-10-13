// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: Sign-in for other clients
//
// The OAuth server the Mastodon, Matrix and Bluesky client interfaces share.
// A client is known by a registration (a Mastodon app at /api/v1/apps, a
// Matrix client at /oauth/register) or, on the AT Protocol, by the document
// its id names, which core fetches. It asks for a sign-in at /oauth/authorize,
// directly or after pushing the request at /oauth/par; core checks the request
// and sends the browser to the consent page in the login app, which reads the
// request through mochi.oauth.request and answers through mochi.oauth.approve or
// refuse, naming the account the client signs in to. The code the browser
// carries back is exchanged at /oauth/token for a token of a new kind in the
// token store, bound to the client and the account, with a refresh token where
// the network's clients expect one and bound to the client's key where the
// protocol demands proof of possession (DPoP). /oauth/revoke ends one. The
// engines of each network check these tokens with signin_validate.
//
// Beside the sign-in, the two things the other protocols need to let their
// clients in: a minted token typed as a password where a protocol insists on
// one (decision 7), one per device, checked with password_check; and the
// numbers Lemmy clients want for every item (decision 8), kept per user by
// number_for and number_identifier.

package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
	"mellium.im/sasl"
)

// The token kinds the store holds beside an app's own: a sign-in by another
// client, and a minted password for a protocol that insists on one.
const token_signin = "signin"
const token_password = "password"

// signin_consent_lifetime is how long a pending request waits for the user;
// signin_code_lifetime how long a code may be exchanged; signin_push_lifetime
// how long a pushed request may be taken up; signin_proof_window how old a
// DPoP proof may be. Variables so tests can lower them.
var signin_consent_lifetime int64 = 600
var signin_code_lifetime int64 = 60
var signin_push_lifetime int64 = 90
var signin_proof_window int64 = 300

// signin_access_lifetime is how long an access token lasts on a network whose
// clients refresh: Matrix and Bluesky. Mastodon clients expect a token that
// lasts, and get one.
var signin_access_lifetime int64 = 3600

// signin_refresh_lifetime is how long a refresh token lasts unused.
var signin_refresh_lifetime int64 = 90 * 86400

// A client is one party that signs users in: registered on this server, or
// described by the document its id names.
type signin_client struct {
	ID        string   `json:"id"`
	Secret    string   `json:"-"`
	Name      string   `json:"name"`
	Website   string   `json:"website"`
	Redirects []string `json:"redirects"`
	Scopes    string   `json:"scopes"`
	Network   string   `json:"network"`
	// Method is how the client authenticates at the token endpoint: "none"
	// for a public client, "secret" for a registered one with a secret,
	// "key" for one that signs an assertion with a key its document names.
	Method string `json:"method"`
	// Keys is the client's JWK set, or where to fetch it, for "key".
	Keys    any    `json:"keys,omitempty"`
	KeysURL string `json:"keys_url,omitempty"`
	// Proof says the client's tokens are bound to its DPoP key.
	Proof bool `json:"proof"`
}

// A request is a pending sign-in: what the client asked for, waiting for the
// user's answer on the consent page, or pushed and waiting to be taken up.
type signin_request struct {
	ID        string `json:"id"`
	Client    string `json:"client"`
	Redirect  string `json:"redirect"`
	Scope     string `json:"scope"`
	State     string `json:"state"`
	Challenge string `json:"challenge"`
	Hint      string `json:"hint"`
	Network   string `json:"network"`
	Issuer    string `json:"issuer"`
	Key       string `json:"key"`   // the DPoP key's thumbprint the tokens bind to
	Nonce     string `json:"nonce"` // an OpenID nonce, echoed where asked
	Pushed    bool   `json:"pushed"`
}

var api_oauth = sls.FromStringDict(sl.String("mochi.oauth"), sl.StringDict{
	"approve": sl.NewBuiltin("mochi.oauth.approve", api_oauth_approve),
	"refuse":  sl.NewBuiltin("mochi.oauth.refuse", api_oauth_refuse),
	"request": sl.NewBuiltin("mochi.oauth.request", api_oauth_request),
})

// === The engine ===

// signin_engine answers the sign-in server's paths: the server's documents,
// registration, the pushed request, the authorisation, the token and the
// revocation endpoints. Open to browsers from anywhere, since the clients are
// often web pages; nothing here takes the session cookie. Served while the
// development switch is on and the system has a configured domain.
func signin_engine() *engine {
	return &engine{
		name:     "signin",
		prefixes: []string{"/oauth/", "/.well-known/oauth-authorization-server", "/.well-known/oauth-protected-resource", "/api/v1/apps"},
		handler:  signin_route,
	}
}

// signin_route dispatches one request.
func signin_route(c *gin.Context) bool {
	c.Header("Access-Control-Allow-Origin", "*")
	c.Header("Access-Control-Expose-Headers", "DPoP-Nonce, WWW-Authenticate")
	if c.Request.Method == "OPTIONS" {
		c.Header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Authorization, Content-Type, Accept, DPoP")
		c.Header("Access-Control-Max-Age", "86400")
		c.AbortWithStatus(http.StatusNoContent)
		return true
	}
	if !networks_development || signin_issuer(c) == "" {
		respond_error(c, http.StatusNotFound, "not_found", "errors.not_found", nil)
		return true
	}
	path, method := c.Request.URL.Path, c.Request.Method
	switch {
	case path == "/.well-known/oauth-authorization-server" && method == "GET":
		signin_metadata(c)
	case path == "/.well-known/oauth-protected-resource" && method == "GET":
		c.JSON(http.StatusOK, map[string]any{
			"resource":                 signin_issuer(c),
			"authorization_servers":    []string{signin_issuer(c)},
			"scopes_supported":         signin_scopes,
			"bearer_methods_supported": []string{"header"},
		})
	case path == "/api/v1/apps" && method == "POST":
		signin_register_mastodon(c)
	case path == "/oauth/register" && method == "POST":
		signin_register(c)
	case path == "/oauth/par" && method == "POST":
		signin_push(c)
	case path == "/oauth/authorize" && (method == "GET" || method == "POST"):
		signin_authorize(c)
	case path == "/oauth/token" && method == "POST":
		signin_token(c)
	case path == "/oauth/revoke" && method == "POST":
		signin_revoke(c)
	default:
		respond_error(c, http.StatusNotFound, "not_found", "errors.not_found", nil)
	}
	return true
}

// signin_scopes are the scopes the networks' clients ask for, listed for them.
var signin_scopes = []string{
	"read", "write", "follow", "push", "profile",
	"atproto", "transition:generic", "transition:chat.bsky", "transition:email",
	"urn:matrix:org.matrix.msc2967.client:api:*", "urn:matrix:client:api:*",
}

// signin_issuer is the sign-in server's identity for a request: HTTPS on the
// host the request came by, when that host is the server's own domain or a
// domain it serves accounts on; empty for any other host, which the server
// does not sign in for. Tests may use a plain scheme.
func signin_issuer(c *gin.Context) string {
	host := strings.ToLower(c.Request.Host)
	bare := host
	if h, _, found := strings.Cut(host, ":"); found && !strings.Contains(h, "]") {
		bare = h
	}
	known := bare == server_domain()
	if !known {
		for _, d := range domain_list() {
			if d.Domain == bare && d.Verified != 0 {
				known = true
			}
		}
	}
	if !known || bare == "" {
		return ""
	}
	return strings.TrimSuffix(network_scheme, "://") + "://" + host
}

// signin_metadata is the authorization server's document.
func signin_metadata(c *gin.Context) {
	issuer := signin_issuer(c)
	c.JSON(http.StatusOK, map[string]any{
		"issuer":                                           issuer,
		"authorization_endpoint":                           issuer + "/oauth/authorize",
		"token_endpoint":                                   issuer + "/oauth/token",
		"revocation_endpoint":                              issuer + "/oauth/revoke",
		"pushed_authorization_request_endpoint":            issuer + "/oauth/par",
		"registration_endpoint":                            issuer + "/oauth/register",
		"response_types_supported":                         []string{"code"},
		"response_modes_supported":                         []string{"query", "fragment"},
		"grant_types_supported":                            []string{"authorization_code", "refresh_token", "client_credentials"},
		"code_challenge_methods_supported":                 []string{"S256"},
		"token_endpoint_auth_methods_supported":            []string{"none", "client_secret_basic", "client_secret_post", "private_key_jwt"},
		"token_endpoint_auth_signing_alg_values_supported": []string{"ES256"},
		"dpop_signing_alg_values_supported":                []string{"ES256"},
		"scopes_supported":                                 signin_scopes,
		"authorization_response_iss_parameter_supported":   true,
		"require_pushed_authorization_requests":            false,
		"client_id_metadata_document_supported":            true,
		"require_request_uri_registration":                 false,
		"ui_locales_supported":                             []string{"en"},
	})
}

// signin_fail answers an OAuth error in the protocol's shape.
func signin_fail(c *gin.Context, status int, code string, description string) {
	c.JSON(status, map[string]any{"error": code, "error_description": description})
}

// === Clients ===

// client_registered reads a registered client by id.
func client_registered(id string) *signin_client {
	row, _ := db_open("db/networks.db").row("select id, secret, name, website, redirects, scopes, network from applications where id=?", id)
	if row == nil {
		return nil
	}
	cl := &signin_client{ID: id, Secret: row_string(row, "secret"), Name: row_string(row, "name"), Website: row_string(row, "website"), Scopes: row_string(row, "scopes"), Network: row_string(row, "network"), Method: "none"}
	json.Unmarshal([]byte(row_string(row, "redirects")), &cl.Redirects)
	if cl.Secret != "" {
		cl.Method = "secret"
	}
	return cl
}

// client_store keeps a registration and returns it.
func client_store(name string, website string, redirects []string, scopes string, network string, secret bool) *signin_client {
	cl := &signin_client{ID: random_alphanumeric(32), Name: name, Website: website, Redirects: redirects, Scopes: scopes, Network: network, Method: "none"}
	if secret {
		cl.Secret = random_alphanumeric(48)
		cl.Method = "secret"
	}
	db_open("db/networks.db").exec("insert into applications (id, secret, name, website, redirects, scopes, network, created) values (?, ?, ?, ?, ?, ?, ?, ?)", cl.ID, cl.Secret, name, website, json_encode(redirects), scopes, network, now())
	return cl
}

// client_name_maximum bounds what a client may call itself.
const client_name_maximum = 100

// signin_redirects_valid checks a client's redirect URIs: absolute, with a
// scheme, and not naming this server's own paths. A native app's custom
// scheme is allowed, as is Mastodon's out-of-band marker.
func signin_redirects_valid(redirects []string) bool {
	if len(redirects) == 0 || len(redirects) > 10 {
		return false
	}
	for _, r := range redirects {
		if r == "urn:ietf:wg:oauth:2.0:oob" {
			continue
		}
		u, err := url.Parse(r)
		if err != nil || u.Scheme == "" || len(r) > 2048 || strings.ContainsAny(r, " \n\r") {
			return false
		}
		if (u.Scheme == "http" || u.Scheme == "https") && u.Host == "" {
			return false
		}
	}
	return true
}

// signin_register_mastodon registers a Mastodon app: POST /api/v1/apps with
// client_name, redirect_uris (one per line or several values), scopes and
// website, answering the client's id and secret as Mastodon does.
func signin_register_mastodon(c *gin.Context) {
	var body struct {
		Name      string `form:"client_name" json:"client_name"`
		Redirects string `form:"redirect_uris" json:"redirect_uris"`
		Scopes    string `form:"scopes" json:"scopes"`
		Website   string `form:"website" json:"website"`
	}
	var list struct {
		Redirects []string `form:"redirect_uris" json:"redirect_uris"`
	}
	if strings.HasPrefix(c.ContentType(), "application/json") {
		raw, _ := c.GetRawData()
		json.Unmarshal(raw, &body)
		json.Unmarshal(raw, &list)
	} else {
		c.ShouldBind(&body)
		c.Request.ParseForm()
		list.Redirects = c.Request.PostForm["redirect_uris[]"]
	}
	redirects := list.Redirects
	if len(redirects) == 0 && body.Redirects != "" {
		redirects = strings.Fields(strings.ReplaceAll(body.Redirects, "\r", "\n"))
	}
	name := strings.TrimSpace(body.Name)
	if name == "" || len(name) > client_name_maximum || !signin_redirects_valid(redirects) {
		signin_fail(c, http.StatusUnprocessableEntity, "invalid_request", "client_name and redirect_uris are required")
		return
	}
	scopes := strings.TrimSpace(body.Scopes)
	if scopes == "" {
		scopes = "read"
	}
	cl := client_store(name, strings.TrimSpace(body.Website), redirects, scopes, "fediverse", true)
	c.JSON(http.StatusOK, map[string]any{
		"id":            cl.ID,
		"name":          cl.Name,
		"website":       cl.Website,
		"scopes":        strings.Fields(cl.Scopes),
		"redirect_uri":  strings.Join(cl.Redirects, "\n"),
		"redirect_uris": cl.Redirects,
		"client_id":     cl.ID,
		"client_secret": cl.Secret,
		"vapid_key":     "",
	})
}

// signin_register registers a client dynamically (RFC 7591), as Matrix
// clients do: a public client with PKCE unless it asks to authenticate with a
// secret. Answers the registration.
func signin_register(c *gin.Context) {
	var body struct {
		Name      string   `json:"client_name"`
		URI       string   `json:"client_uri"`
		Redirects []string `json:"redirect_uris"`
		Scope     string   `json:"scope"`
		Method    string   `json:"token_endpoint_auth_method"`
		Grants    []string `json:"grant_types"`
		Responses []string `json:"response_types"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		signin_fail(c, http.StatusBadRequest, "invalid_client_metadata", "the registration is not JSON")
		return
	}
	name := strings.TrimSpace(body.Name)
	if name == "" {
		name = body.URI
	}
	if name == "" || len(name) > client_name_maximum || !signin_redirects_valid(body.Redirects) {
		signin_fail(c, http.StatusBadRequest, "invalid_redirect_uri", "client_name and redirect_uris are required")
		return
	}
	for _, grant := range body.Grants {
		if grant != "authorization_code" && grant != "refresh_token" {
			signin_fail(c, http.StatusBadRequest, "invalid_client_metadata", "unsupported grant type "+grant)
			return
		}
	}
	secret := body.Method == "client_secret_basic" || body.Method == "client_secret_post"
	if body.Method != "" && body.Method != "none" && !secret {
		signin_fail(c, http.StatusBadRequest, "invalid_client_metadata", "unsupported token_endpoint_auth_method")
		return
	}
	network := "matrix"
	if strings.Contains(body.Scope, "atproto") {
		network = "bluesky"
	} else if body.Scope != "" && !strings.Contains(body.Scope, "matrix") {
		network = "fediverse"
	}
	cl := client_store(name, body.URI, body.Redirects, body.Scope, network, secret)
	answer := map[string]any{
		"client_id":                  cl.ID,
		"client_id_issued_at":        now(),
		"client_name":                cl.Name,
		"client_uri":                 cl.Website,
		"redirect_uris":              cl.Redirects,
		"token_endpoint_auth_method": "none",
		"grant_types":                []string{"authorization_code", "refresh_token"},
		"response_types":             []string{"code"},
		"scope":                      cl.Scopes,
	}
	if secret {
		answer["client_secret"] = cl.Secret
		answer["token_endpoint_auth_method"] = body.Method
	}
	c.JSON(http.StatusCreated, answer)
}

// client_document_maximum caps a client document.
const client_document_maximum = 64 << 10

// client_documents caches the documents of URL clients for an hour.
var client_documents = map[string]*signin_client{}
var client_documents_lock sync.Mutex

// client_fetch reads an AT Protocol client's document from the URL that is
// its id: the redirects, the name, how it authenticates and its keys.
func client_fetch(c *gin.Context, id string) (*signin_client, error) {
	u, err := url.Parse(id)
	if err != nil || u.Scheme+"://" != network_scheme || u.Host == "" || u.Fragment != "" || u.RawQuery != "" {
		return nil, fmt.Errorf("a client id is an https URL")
	}
	client_documents_lock.Lock()
	cached := client_documents[id]
	client_documents_lock.Unlock()
	if cached != nil {
		return cached, nil
	}
	request, err := http.NewRequestWithContext(c.Request.Context(), "GET", id, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Accept", "application/json")
	response, err := network_request(request.Context(), nil, "", request, 3)
	if err != nil {
		return nil, fmt.Errorf("the client document could not be fetched: %v", err)
	}
	defer response.Body.Close()
	body, err := read_limited(response.Body, client_document_maximum)
	if err != nil || response.StatusCode != 200 {
		return nil, fmt.Errorf("the client document could not be read")
	}
	var document struct {
		ID        string   `json:"client_id"`
		Name      string   `json:"client_name"`
		URI       string   `json:"client_uri"`
		Redirects []string `json:"redirect_uris"`
		Scope     string   `json:"scope"`
		Method    string   `json:"token_endpoint_auth_method"`
		Proof     bool     `json:"dpop_bound_access_tokens"`
		Keys      any      `json:"jwks"`
		KeysURL   string   `json:"jwks_uri"`
	}
	if json.Unmarshal(body, &document) != nil || document.ID != id || !signin_redirects_valid(document.Redirects) || !document.Proof {
		return nil, fmt.Errorf("the client document is not valid for %s", id)
	}
	cl := &signin_client{ID: id, Name: document.Name, Website: document.URI, Redirects: document.Redirects, Scopes: document.Scope, Network: "bluesky", Method: "none", Proof: true}
	if cl.Name == "" {
		cl.Name = u.Host
	}
	switch document.Method {
	case "", "none":
	case "private_key_jwt":
		cl.Method = "key"
		cl.Keys, cl.KeysURL = document.Keys, document.KeysURL
		if cl.Keys == nil && cl.KeysURL == "" {
			return nil, fmt.Errorf("the client document names no keys")
		}
	default:
		return nil, fmt.Errorf("the client document names an unsupported authentication method")
	}
	// The redirects of a web client must be on the client's own host.
	for _, redirect := range cl.Redirects {
		if r, err := url.Parse(redirect); err == nil && (r.Scheme == "https" || r.Scheme == "http") && r.Hostname() != u.Hostname() {
			return nil, fmt.Errorf("the client's redirects are not on its host")
		}
	}
	client_documents_lock.Lock()
	if len(client_documents) > 1000 {
		client_documents = map[string]*signin_client{}
	}
	client_documents[id] = cl
	client_documents_lock.Unlock()
	return cl, nil
}

// client_resolve finds a client by id: a registration, or a document.
func client_resolve(c *gin.Context, id string) (*signin_client, error) {
	if id == "" {
		return nil, fmt.Errorf("client_id is required")
	}
	if strings.HasPrefix(id, network_scheme) {
		return client_fetch(c, id)
	}
	if cl := client_registered(id); cl != nil {
		return cl, nil
	}
	return nil, fmt.Errorf("unknown client")
}

// client_authenticate checks the client's credential at the token, push and
// revocation endpoints: a secret in the body or the Basic header, an assertion
// signed with its key, or nothing for a public client. It answers the client,
// or writes the refusal.
func client_authenticate(c *gin.Context) *signin_client {
	id := c.PostForm("client_id")
	secret := c.PostForm("client_secret")
	if user, password, ok := c.Request.BasicAuth(); ok {
		if id != "" && id != user {
			signin_fail(c, http.StatusUnauthorized, "invalid_client", "two client ids")
			return nil
		}
		id, secret = user, password
	}
	if assertion := c.PostForm("client_assertion"); assertion != "" {
		if c.PostForm("client_assertion_type") != "urn:ietf:params:oauth:client-assertion-type:jwt-bearer" {
			signin_fail(c, http.StatusUnauthorized, "invalid_client", "unsupported client assertion type")
			return nil
		}
		if id == "" {
			if claims, _, err := jwt.NewParser().ParseUnverified(assertion, jwt.MapClaims{}); err == nil {
				id, _ = claims.Claims.(jwt.MapClaims)["iss"].(string)
			}
		}
		cl, err := client_resolve(c, id)
		if err != nil || cl.Method != "key" {
			signin_fail(c, http.StatusUnauthorized, "invalid_client", "the client does not authenticate with a key")
			return nil
		}
		if err := client_assertion_check(c, cl, assertion); err != nil {
			signin_fail(c, http.StatusUnauthorized, "invalid_client", err.Error())
			return nil
		}
		return cl
	}
	cl, err := client_resolve(c, id)
	if err != nil {
		signin_fail(c, http.StatusUnauthorized, "invalid_client", err.Error())
		return nil
	}
	switch cl.Method {
	case "secret":
		if secret == "" || cl.Secret != secret {
			signin_fail(c, http.StatusUnauthorized, "invalid_client", "the client secret is wrong")
			return nil
		}
	case "key":
		signin_fail(c, http.StatusUnauthorized, "invalid_client", "the client must send its assertion")
		return nil
	}
	return cl
}

// client_assertion_check verifies a client's assertion: signed by a key of its
// set, for this server, by the client, and fresh.
func client_assertion_check(c *gin.Context, cl *signin_client, assertion string) error {
	keys, err := client_keys(c, cl)
	if err != nil {
		return err
	}
	token, err := jwt.Parse(assertion, func(t *jwt.Token) (any, error) {
		kid, _ := t.Header["kid"].(string)
		for _, key := range keys {
			if kid == "" || key["kid"] == kid {
				if public, err := jwk_public(key); err == nil {
					return public, nil
				}
			}
		}
		return nil, fmt.Errorf("no key %q in the client's set", kid)
	}, jwt.WithValidMethods([]string{"ES256"}), jwt.WithIssuer(cl.ID), jwt.WithSubject(cl.ID), jwt.WithAudience(signin_issuer(c)), jwt.WithExpirationRequired())
	if err != nil || !token.Valid {
		return fmt.Errorf("the client assertion is not valid: %v", err)
	}
	return nil
}

// client_keys reads a client's JWK set, from its document or where it names.
func client_keys(c *gin.Context, cl *signin_client) ([]map[string]any, error) {
	var set struct {
		Keys []map[string]any `json:"keys"`
	}
	if cl.Keys != nil {
		json.Unmarshal([]byte(json_encode(cl.Keys)), &set)
	} else if cl.KeysURL != "" {
		request, err := http.NewRequestWithContext(c.Request.Context(), "GET", cl.KeysURL, nil)
		if err != nil {
			return nil, err
		}
		response, err := network_request(request.Context(), nil, "", request, 3)
		if err != nil {
			return nil, fmt.Errorf("the client's keys could not be fetched")
		}
		defer response.Body.Close()
		body, err := read_limited(response.Body, client_document_maximum)
		if err != nil || response.StatusCode != 200 || json.Unmarshal(body, &set) != nil {
			return nil, fmt.Errorf("the client's keys could not be read")
		}
	}
	if len(set.Keys) == 0 {
		return nil, fmt.Errorf("the client has no keys")
	}
	return set.Keys, nil
}

// jwk_public reads a P-256 public key from a JWK.
func jwk_public(jwk map[string]any) (*ecdsa.PublicKey, error) {
	if jwk["kty"] != "EC" || jwk["crv"] != "P-256" {
		return nil, fmt.Errorf("not a P-256 key")
	}
	x, err := base64.RawURLEncoding.DecodeString(fmt.Sprint(jwk["x"]))
	if err != nil {
		return nil, err
	}
	y, err := base64.RawURLEncoding.DecodeString(fmt.Sprint(jwk["y"]))
	if err != nil {
		return nil, err
	}
	public := &ecdsa.PublicKey{Curve: elliptic.P256(), X: new(big.Int).SetBytes(x), Y: new(big.Int).SetBytes(y)}
	if !public.Curve.IsOnCurve(public.X, public.Y) {
		return nil, fmt.Errorf("not a point on the curve")
	}
	return public, nil
}

// jwk_thumbprint is a JWK's thumbprint (RFC 7638): the hash of its required
// members in order.
func jwk_thumbprint(jwk map[string]any) string {
	canonical := fmt.Sprintf(`{"crv":%q,"kty":%q,"x":%q,"y":%q}`, fmt.Sprint(jwk["crv"]), fmt.Sprint(jwk["kty"]), fmt.Sprint(jwk["x"]), fmt.Sprint(jwk["y"]))
	sum := sha256.Sum256([]byte(canonical))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

// === DPoP ===
//
// A proof is a JWT the client signs with its key for each request, naming the
// method, the URL, the time, a nonce this server gave it and, when a token is
// sent, the token's hash. The nonce is made from the time, so it needs no
// storage and is good for its own and the previous window; the proof's id is
// remembered for the window against replay.

var signin_proofs = map[string]int64{}
var signin_proofs_lock sync.Mutex

// signin_nonce is the current DPoP nonce, or the previous window's when
// previous is set.
func signin_nonce(previous bool) string {
	window := now() / signin_proof_window
	if previous {
		window--
	}
	mac := hmac.New(sha256.New, []byte(networks_key("signin-nonce", key_p256)))
	mac.Write([]byte(strconv.FormatInt(window, 10)))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))[:24]
}

// signin_proof checks a request's DPoP proof against the method and URL it
// came by and, when given, the access token it carries, and answers the key's
// thumbprint. A proof without this server's nonce, or without a proof at all
// where required, is answered with the nonce to use. Writes the refusal.
func signin_proof(c *gin.Context, token string, required bool) (string, bool) {
	proof := c.GetHeader("DPoP")
	c.Header("DPoP-Nonce", signin_nonce(false))
	if proof == "" {
		if !required {
			return "", true
		}
		signin_fail(c, http.StatusBadRequest, "invalid_dpop_proof", "a DPoP proof is required")
		return "", false
	}
	var jwk map[string]any
	parsed, err := jwt.Parse(proof, func(t *jwt.Token) (any, error) {
		jwk, _ = t.Header["jwk"].(map[string]any)
		if t.Header["typ"] != "dpop+jwt" || jwk == nil {
			return nil, fmt.Errorf("not a DPoP proof")
		}
		return jwk_public(jwk)
	}, jwt.WithValidMethods([]string{"ES256"}))
	if err != nil || !parsed.Valid {
		signin_fail(c, http.StatusBadRequest, "invalid_dpop_proof", "the proof is not valid")
		return "", false
	}
	claims, _ := parsed.Claims.(jwt.MapClaims)
	issued, _ := claims["iat"].(float64)
	jti, _ := claims["jti"].(string)
	htu, _ := claims["htu"].(string)
	if claims["htm"] != c.Request.Method || htu != signin_issuer(c)+c.Request.URL.Path || jti == "" || int64(issued) < now()-signin_proof_window || int64(issued) > now()+60 {
		signin_fail(c, http.StatusBadRequest, "invalid_dpop_proof", "the proof names another request or another time")
		return "", false
	}
	nonce, _ := claims["nonce"].(string)
	if nonce != signin_nonce(false) && nonce != signin_nonce(true) {
		c.Header("WWW-Authenticate", `DPoP error="use_dpop_nonce"`)
		signin_fail(c, http.StatusBadRequest, "use_dpop_nonce", "a fresh nonce is needed")
		return "", false
	}
	if token != "" {
		sum := sha256.Sum256([]byte(token))
		if claims["ath"] != base64.RawURLEncoding.EncodeToString(sum[:]) {
			signin_fail(c, http.StatusBadRequest, "invalid_dpop_proof", "the proof is for another token")
			return "", false
		}
	}
	signin_proofs_lock.Lock()
	if _, seen := signin_proofs[jti]; seen {
		signin_proofs_lock.Unlock()
		signin_fail(c, http.StatusBadRequest, "invalid_dpop_proof", "the proof was already used")
		return "", false
	}
	if len(signin_proofs) > 100000 {
		signin_proofs = map[string]int64{}
	}
	signin_proofs[jti] = now()
	for id, when := range signin_proofs {
		if when < now()-2*signin_proof_window {
			delete(signin_proofs, id)
		}
	}
	signin_proofs_lock.Unlock()
	return jwk_thumbprint(jwk), true
}

// === Requests ===

// request_store keeps a pending request under a kind for a time.
func request_store(kind string, r *signin_request, lifetime int64) {
	db_open("db/sessions.db").exec("insert into ceremonies (id, type, user, challenge, data, expires) values (?, ?, '', ?, ?, ?)", r.ID, kind, []byte(r.ID), json_encode(r), now()+lifetime)
}

// request_take reads and consumes a pending request of a kind, or answers nil.
func request_take(kind string, id string) *signin_request {
	db := db_open("db/sessions.db")
	row, _ := db.row("select data from ceremonies where id=? and type=? and expires>?", id, kind, now())
	if row == nil {
		return nil
	}
	db.exec("delete from ceremonies where id=?", id)
	var r signin_request
	if json.Unmarshal([]byte(row_string(row, "data")), &r) != nil || r.ID != id {
		return nil
	}
	return &r
}

// request_read reads a pending request without consuming it.
func request_read(kind string, id string) *signin_request {
	row, _ := db_open("db/sessions.db").row("select data from ceremonies where id=? and type=? and expires>?", id, kind, now())
	if row == nil {
		return nil
	}
	var r signin_request
	if json.Unmarshal([]byte(row_string(row, "data")), &r) != nil || r.ID != id {
		return nil
	}
	return &r
}

// request_build reads a sign-in request's parameters against its client: the
// redirect must be one the client registered, PKCE S256 is required of a
// public client and accepted from any, and the response type is code.
func request_build(c *gin.Context, cl *signin_client, values url.Values) (*signin_request, string) {
	redirect := values.Get("redirect_uri")
	if redirect == "" && len(cl.Redirects) == 1 {
		redirect = cl.Redirects[0]
	}
	if !slices.Contains(cl.Redirects, redirect) {
		return nil, "the redirect is not one the client registered"
	}
	if values.Get("response_type") != "code" {
		return nil, "response_type must be code"
	}
	challenge := values.Get("code_challenge")
	if challenge == "" && cl.Method == "none" {
		return nil, "a code challenge is required"
	}
	if challenge != "" && values.Get("code_challenge_method") != "S256" {
		return nil, "code_challenge_method must be S256"
	}
	if len(challenge) > 128 || len(values.Get("state")) > 512 || len(values.Get("scope")) > 1024 || len(values.Get("login_hint")) > 255 {
		return nil, "a parameter is too long"
	}
	return &signin_request{
		ID:        random_alphanumeric(32),
		Client:    cl.ID,
		Redirect:  redirect,
		Scope:     strings.Join(strings.Fields(values.Get("scope")), " "),
		State:     values.Get("state"),
		Challenge: challenge,
		Hint:      strings.TrimSpace(values.Get("login_hint")),
		Network:   cl.Network,
		Issuer:    signin_issuer(c),
		Key:       values.Get("dpop_jkt"),
		Nonce:     values.Get("nonce"),
	}, ""
}

// signin_push takes a pushed authorisation request (PAR): the client
// authenticates, the request is checked and kept for a short time, and the
// client is told the URI that names it. A DPoP proof binds the tokens to come
// to the client's key.
func signin_push(c *gin.Context) {
	cl := client_authenticate(c)
	if cl == nil {
		return
	}
	key, ok := signin_proof(c, "", cl.Proof)
	if !ok {
		return
	}
	c.Request.ParseForm()
	r, fault := request_build(c, cl, c.Request.PostForm)
	if fault != "" {
		signin_fail(c, http.StatusBadRequest, "invalid_request", fault)
		return
	}
	if r.Key != "" && key != "" && r.Key != key {
		signin_fail(c, http.StatusBadRequest, "invalid_dpop_proof", "dpop_jkt names another key")
		return
	}
	if key != "" {
		r.Key = key
	}
	r.Pushed = true
	request_store("signin_pushed", r, signin_push_lifetime)
	c.JSON(http.StatusCreated, map[string]any{"request_uri": "urn:ietf:params:oauth:request_uri:" + r.ID, "expires_in": signin_push_lifetime})
}

// signin_authorize begins a sign-in: a pushed request taken up by its URI, or
// the parameters given here, checked against the client; the request then
// waits for the user on the consent page in the login app, where the browser
// is sent. A request that cannot be checked is answered here, since the
// client's redirect cannot be trusted yet; one the client registered
// correctly but that is otherwise wrong is answered at the redirect.
func signin_authorize(c *gin.Context) {
	values := c.Request.URL.Query()
	if c.Request.Method == "POST" {
		c.Request.ParseForm()
		values = c.Request.PostForm
	}
	var r *signin_request
	if uri := values.Get("request_uri"); uri != "" {
		id, ok := strings.CutPrefix(uri, "urn:ietf:params:oauth:request_uri:")
		if !ok {
			signin_fail(c, http.StatusBadRequest, "invalid_request", "unknown request_uri")
			return
		}
		r = request_take("signin_pushed", id)
		if r == nil {
			signin_fail(c, http.StatusBadRequest, "invalid_request", "the pushed request is unknown or has expired")
			return
		}
		if client := values.Get("client_id"); client != "" && client != r.Client {
			signin_fail(c, http.StatusBadRequest, "invalid_request", "the client is not the one that pushed the request")
			return
		}
	} else {
		cl, err := client_resolve(c, values.Get("client_id"))
		if err != nil {
			signin_fail(c, http.StatusBadRequest, "invalid_client", err.Error())
			return
		}
		var fault string
		r, fault = request_build(c, cl, values)
		if fault != "" {
			if !slices.Contains(cl.Redirects, values.Get("redirect_uri")) || values.Get("redirect_uri") == "" {
				signin_fail(c, http.StatusBadRequest, "invalid_request", fault)
				return
			}
			c.Redirect(http.StatusFound, signin_redirect(values.Get("redirect_uri"), map[string]string{"error": "invalid_request", "error_description": fault, "state": values.Get("state"), "iss": signin_issuer(c)}))
			return
		}
	}
	request_store("signin_consent", r, signin_consent_lifetime)
	c.Redirect(http.StatusFound, app_login_route("oauth/consent")+"?request="+r.ID)
}

// signin_redirect adds parameters to a client's redirect: in the fragment for
// a redirect that asks for it, else the query.
func signin_redirect(redirect string, parameters map[string]string) string {
	values := url.Values{}
	for key, value := range parameters {
		if value != "" {
			values.Set(key, value)
		}
	}
	if redirect == "urn:ietf:wg:oauth:2.0:oob" {
		return redirect + "?" + values.Encode()
	}
	sep := "?"
	if strings.Contains(redirect, "?") {
		sep = "&"
	}
	return redirect + sep + values.Encode()
}

// === The consent page's functions ===

// oauth_consent_login says whether an app is the login app, the one place
// the consent page lives. A variable so tests can stand one in.
var oauth_consent_login = app_is_login

// oauth_consent_app checks the caller is the login app with the permission.
func oauth_consent_app(t *sl.Thread, fn *sl.Builtin) (*User, error) {
	if err := require_permission(t, fn, "oauth/consent"); err != nil {
		return nil, err
	}
	if !oauth_consent_login(principal_app(t)) {
		return nil, fmt.Errorf("the consent page is the login app's")
	}
	user := principal_caller(t)
	if user == nil {
		return nil, fmt.Errorf("no user")
	}
	return user, nil
}

// signin_writes reports whether a request asks for permission to write: to
// post, on any of the networks.
func signin_writes(r *signin_request) bool {
	for _, scope := range strings.Fields(r.Scope) {
		switch {
		case scope == "write", strings.HasPrefix(scope, "write:"):
			return true
		case strings.HasPrefix(scope, "transition:"), scope == "atproto", strings.HasPrefix(scope, "repo:"), strings.HasPrefix(scope, "rpc:"):
			return true
		case strings.Contains(scope, "matrix"):
			return true
		}
	}
	return false
}

// mochi.oauth.request(id) -> dict | None: A sign-in another client is asking for, by the id the consent page was opened with: client (name, website, id), network, scope, the scopes as a list, writes (whether the client asks to post), hint (the account the client suggested), and accounts, the user's hosted accounts on the network the client may sign in to, each with id, identifier, name, home (the home feed chosen for it, or "") and hinted. Only the login app holds the permission
func api_oauth_request(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var id string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "id", &id); err != nil {
		return nil, err
	}
	user, err := oauth_consent_app(t, fn)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	r := request_read("signin_consent", id)
	if r == nil {
		return sl.None, nil
	}
	name, website := r.Client, ""
	if cl := client_registered(r.Client); cl != nil {
		name, website = cl.Name, cl.Website
	} else if strings.HasPrefix(r.Client, network_scheme) {
		client_documents_lock.Lock()
		if cl := client_documents[r.Client]; cl != nil {
			name, website = cl.Name, cl.Website
		}
		client_documents_lock.Unlock()
	}
	var accounts []map[string]any
	for _, row := range accounts_hosted(user, r.Network) {
		data := account_data(row)
		home, _ := data["home"].(string)
		identifier, _ := row["identifier"].(string)
		account := map[string]any{"id": row["id"], "identifier": identifier, "name": row["label"], "home": home, "hinted": false}
		if r.Hint != "" && (strings.EqualFold(strings.TrimPrefix(identifier, "@"), strings.TrimPrefix(r.Hint, "@")) || data["did"] == r.Hint || account_url(fmt.Sprint(data["domain"]), fmt.Sprint(row["id"])) == r.Hint) {
			account["hinted"] = true
		}
		accounts = append(accounts, account)
	}
	if accounts == nil {
		accounts = []map[string]any{}
	}
	return sl_encode(map[string]any{
		"id":       r.ID,
		"client":   map[string]any{"id": r.Client, "name": name, "website": website},
		"network":  r.Network,
		"scope":    r.Scope,
		"scopes":   strings.Fields(r.Scope),
		"writes":   signin_writes(r),
		"hint":     r.Hint,
		"accounts": accounts,
	}), nil
}

// mochi.oauth.approve(id, account, feed="") -> dict: Approve a sign-in for one of the user's hosted accounts on the request's network, naming the feed posts made from the client land in where the client asks to write and the account has no home feed yet (decision 42). Returns {url}, where the browser is sent: the client's redirect carrying the code. Only the login app holds the permission
func api_oauth_approve(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var id, account, feed string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "id", &id, "account", &account, "feed?", &feed); err != nil {
		return nil, err
	}
	user, err := oauth_consent_app(t, fn)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	r := request_read("signin_consent", id)
	if r == nil {
		return sl_error(fn, "no such request")
	}
	row := account_hosted_get(user, account)
	if row == nil || row["type"] != r.Network {
		return sl_error(fn, "the account is not the user's on %s", r.Network)
	}
	data := account_data(row)
	if feed != "" {
		if !valid(feed, "entity") {
			return sl_error(fn, "invalid feed")
		}
		data["home"] = feed
		db_user(user, "user").account_set(account, map[string]any{"data": json_encode(data)})
	}
	request_take("signin_consent", id)
	code := &signin_request{ID: random_alphanumeric(40), Client: r.Client, Redirect: r.Redirect, Scope: r.Scope, State: r.State, Challenge: r.Challenge, Network: r.Network, Issuer: r.Issuer, Key: r.Key, Nonce: r.Nonce, Hint: user.UID + "/" + account}
	request_store("signin_code", code, signin_code_lifetime)
	return sl_encode(map[string]any{"url": signin_redirect(r.Redirect, map[string]string{"code": code.ID, "state": r.State, "iss": r.Issuer})}), nil
}

// mochi.oauth.refuse(id) -> dict: Refuse a sign-in. Returns {url}, the client's redirect carrying the refusal. Only the login app holds the permission
func api_oauth_refuse(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var id string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "id", &id); err != nil {
		return nil, err
	}
	if _, err := oauth_consent_app(t, fn); err != nil {
		return sl_error(fn, "%v", err)
	}
	r := request_take("signin_consent", id)
	if r == nil {
		return sl_error(fn, "no such request")
	}
	return sl_encode(map[string]any{"url": signin_redirect(r.Redirect, map[string]string{"error": "access_denied", "state": r.State, "iss": r.Issuer})}), nil
}

// === Tokens ===

// signin_token issues tokens: for a code the consent page made, for a refresh
// token, or for a client's own credentials. The client authenticates as it
// registered; a client that proves possession sends its proof, and the tokens
// bind to its key.
func signin_token(c *gin.Context) {
	cl := client_authenticate(c)
	if cl == nil {
		return
	}
	key, ok := signin_proof(c, "", cl.Proof)
	if !ok {
		return
	}
	switch c.PostForm("grant_type") {
	case "authorization_code":
		signin_token_code(c, cl, key)
	case "refresh_token":
		signin_token_refresh(c, cl, key)
	case "client_credentials":
		signin_token_client(c, cl)
	default:
		signin_fail(c, http.StatusBadRequest, "unsupported_grant_type", "grant_type must be authorization_code, refresh_token or client_credentials")
	}
}

// signin_token_code exchanges a code: it must be the client's, the redirect
// and the verifier must match what the request named, and the key the request
// bound must be the one proving now.
func signin_token_code(c *gin.Context, cl *signin_client, key string) {
	code := request_take("signin_code", c.PostForm("code"))
	if code == nil || code.Client != cl.ID {
		signin_fail(c, http.StatusBadRequest, "invalid_grant", "the code is unknown, used or another client's")
		return
	}
	if redirect := c.PostForm("redirect_uri"); redirect != "" && redirect != code.Redirect {
		signin_fail(c, http.StatusBadRequest, "invalid_grant", "the redirect is not the request's")
		return
	}
	if code.Challenge != "" && oauth_challenge(c.PostForm("code_verifier")) != code.Challenge {
		signin_fail(c, http.StatusBadRequest, "invalid_grant", "the code verifier does not match")
		return
	}
	if code.Key != "" && code.Key != key {
		signin_fail(c, http.StatusBadRequest, "invalid_dpop_proof", "the proof is not by the key the request bound")
		return
	}
	uid, account, _ := strings.Cut(code.Hint, "/")
	user := user_by_uid(uid)
	if user == nil || account_hosted_get(user, account) == nil {
		signin_fail(c, http.StatusBadRequest, "invalid_grant", "the account is gone")
		return
	}
	signin_issue(c, cl, user, account, code.Scope, key, "")
}

// signin_issue mints the tokens for a sign-in and answers them: an access
// token that lasts on the fediverse and expires elsewhere, with a refresh
// token where it expires, bound to the client, the account and the key.
func signin_issue(c *gin.Context, cl *signin_client, user *User, account string, scope string, key string, device string) {
	var expires int64
	refresh := ""
	if cl.Network != "fediverse" {
		expires = now() + signin_access_lifetime
		refresh = token_generate()
	}
	if device == "" {
		device = random_alphanumeric(10)
	}
	token := token_generate()
	if token == "" {
		signin_fail(c, http.StatusInternalServerError, "server_error", "no token")
		return
	}
	db_open("db/users.db").exec("insert into tokens (hash, user, app, name, scopes, action, entity, created, expires, kind, client, account, network, jkt, refresh, device) values (?, ?, '', ?, ?, '', '', ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		token_hash(token), user.UID, cl.Name, json_encode(strings.Fields(scope)), now(), expires, token_signin, cl.ID, account, cl.Network, key, token_hash(refresh), device)
	db_open("db/sessions.db").exec("insert into accesses (hash, user, used) values (?, ?, ?)", token_hash(token), user.UID, now())
	answer := map[string]any{
		"access_token": token,
		"token_type":   "Bearer",
		"scope":        scope,
		"created_at":   now(),
	}
	if key != "" {
		answer["token_type"] = "DPoP"
	}
	if expires > 0 {
		answer["expires_in"] = expires - now()
		answer["refresh_token"] = refresh
	}
	data := account_data(account_hosted_get(user, account))
	if did, _ := data["did"].(string); did != "" {
		answer["sub"] = did
	} else if cl.Network == "bluesky" {
		answer["sub"] = account_url(fmt.Sprint(data["domain"]), account)
	}
	if cl.Network == "matrix" {
		answer["device_id"] = device
	}
	c.Header("Cache-Control", "no-store")
	c.JSON(http.StatusOK, answer)
}

// signin_token_refresh rotates a sign-in's tokens: the refresh token must be
// the client's, unused and within its life, and the proof by the bound key.
// The old tokens end.
func signin_token_refresh(c *gin.Context, cl *signin_client, key string) {
	db := db_open("db/users.db")
	var t Token
	if !db.scan(&t, "select hash, user, app, name, scopes, action, entity, created, expires, kind, client, account, network, jkt, refresh, device from tokens where refresh=? and kind=?", token_hash(c.PostForm("refresh_token")), token_signin) || t.Client != cl.ID {
		signin_fail(c, http.StatusBadRequest, "invalid_grant", "the refresh token is unknown or another client's")
		return
	}
	used := token_useds(t.User)[t.Hash]
	if used == 0 {
		used = t.Created
	}
	if now()-used > signin_refresh_lifetime {
		token_delete(t.Hash)
		signin_fail(c, http.StatusBadRequest, "invalid_grant", "the refresh token has expired")
		return
	}
	if t.JKT != "" && t.JKT != key {
		signin_fail(c, http.StatusBadRequest, "invalid_dpop_proof", "the proof is not by the token's key")
		return
	}
	user := user_by_uid(t.User)
	if user == nil || account_hosted_get(user, t.Account) == nil {
		token_delete(t.Hash)
		signin_fail(c, http.StatusBadRequest, "invalid_grant", "the account is gone")
		return
	}
	token_delete(t.Hash)
	json.Unmarshal([]byte(t.ScopesDB), &t.Scopes)
	signin_issue(c, cl, user, t.Account, strings.Join(t.Scopes, " "), t.JKT, t.Device)
}

// signin_token_client answers a client's own token, for the calls a Mastodon
// app makes before any user signs in: a token of the client alone, bound to
// no account, which the engines take for the public reads only.
func signin_token_client(c *gin.Context, cl *signin_client) {
	if cl.Method != "secret" {
		signin_fail(c, http.StatusUnauthorized, "invalid_client", "client credentials need a secret")
		return
	}
	token := token_generate()
	db_open("db/networks.db").exec("insert into tokens (hash, client, scopes, created) values (?, ?, ?, ?)", token_hash(token), cl.ID, c.PostForm("scope"), now())
	c.Header("Cache-Control", "no-store")
	c.JSON(http.StatusOK, map[string]any{"access_token": token, "token_type": "Bearer", "scope": c.PostForm("scope"), "created_at": now()})
}

// signin_revoke ends a token: an access or a refresh token of the client.
// Answers 200 whatever happened, as the protocol asks.
func signin_revoke(c *gin.Context) {
	cl := client_authenticate(c)
	if cl == nil {
		return
	}
	hash := token_hash(c.PostForm("token"))
	db := db_open("db/users.db")
	if row, _ := db.row("select hash from tokens where (hash=? or refresh=?) and kind=? and client=?", hash, hash, token_signin, cl.ID); row != nil {
		token_delete(row_string(row, "hash"))
	}
	db_open("db/networks.db").exec("delete from tokens where hash=? and client=?", hash, cl.ID)
	c.JSON(http.StatusOK, map[string]any{})
}

// signin_validate checks a token another client signed in with, for the
// engines: it must be a sign-in token within its life, and bound to the key
// proving possession when it is bound at all. The proof is checked against
// the request. Answers the token, with its use recorded, or nil with the
// refusal written.
func signin_validate(c *gin.Context) *Token {
	header := c.GetHeader("Authorization")
	scheme, raw, _ := strings.Cut(header, " ")
	raw = strings.TrimSpace(raw)
	if raw == "" || (scheme != "Bearer" && scheme != "DPoP") {
		c.Header("WWW-Authenticate", `Bearer, DPoP algs="ES256"`)
		signin_fail(c, http.StatusUnauthorized, "invalid_token", "a token is required")
		return nil
	}
	t := token_lookup(raw)
	if t == nil || t.Kind != token_signin {
		c.Header("WWW-Authenticate", scheme+` error="invalid_token"`)
		signin_fail(c, http.StatusUnauthorized, "invalid_token", "the token is not valid")
		return nil
	}
	if t.JKT != "" {
		key, ok := signin_proof(c, raw, true)
		if !ok {
			return nil
		}
		if key != t.JKT {
			signin_fail(c, http.StatusUnauthorized, "invalid_token", "the proof is not by the token's key")
			return nil
		}
	}
	db_open("db/sessions.db").exec("insert into accesses (hash, user, used) values (?, ?, ?) on conflict(hash) do update set used=excluded.used", t.Hash, t.User, now())
	return t
}

// === Minted passwords ===

// password_scram_iterations is the SCRAM iteration count a minted password's
// salted forms are made with.
const password_scram_iterations = 4096

// mochi.account.password(account, name) -> string: Mint a password for a hosted account, for a protocol that insists on one: a token the user types as the password in a Lemmy app, an XMPP client or a Matrix or Bluesky app that signs in the old way, named for the device it is for. Shown once and never stored; one per device. Revoked with mochi.token.delete. Requires accounts/write
func api_account_password(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var account, name string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "account", &account, "name", &name); err != nil {
		return nil, err
	}
	if err := require_permission(t, fn, "accounts/write"); err != nil {
		return sl_error(fn, "%v", err)
	}
	user := principal_caller(t)
	if user == nil {
		return sl_error(fn, "no user")
	}
	if token_carried(t) {
		return sl_error(fn, "a credential carried in a URL cannot mint passwords")
	}
	app := principal_app(t)
	if app == nil {
		return sl_error(fn, "no app")
	}
	name = strings.TrimSpace(name)
	if name == "" || len(name) > 100 {
		return sl_error(fn, "a name for the device is required")
	}
	row := account_hosted_get(user, account)
	if row == nil {
		return sl_error(fn, "no such hosted account")
	}
	password := password_mint(user, app.id, account, row_string(row, "type"), name)
	if password == "" {
		return sl_error(fn, "cannot mint a password")
	}
	return sl.String(password), nil
}

// password_mint makes a password token for an account: stored hashed, with the
// SCRAM forms an XMPP server needs kept from the start.
func password_mint(user *User, app string, account string, network string, name string) string {
	password := token_generate()
	if password == "" {
		return ""
	}
	salt := []byte(random_alphanumeric(16))
	scram := map[string]any{
		"salt":       base64.StdEncoding.EncodeToString(salt),
		"iterations": password_scram_iterations,
		"sha1":       base64.StdEncoding.EncodeToString(sasl.SCRAMSaltPassword(sha1.New, []byte(password), salt, password_scram_iterations)),
		"sha256":     base64.StdEncoding.EncodeToString(sasl.SCRAMSaltPassword(sha256.New, []byte(password), salt, password_scram_iterations)),
	}
	db_open("db/users.db").exec("insert into tokens (hash, user, app, name, scopes, action, entity, created, expires, kind, account, network, scram) values (?, ?, ?, ?, '[]', '', '', ?, 0, ?, ?, ?, ?)",
		token_hash(password), user.UID, app, name, now(), token_password, account, network, json_encode(scram))
	db_open("db/sessions.db").exec("insert into accesses (hash, user, used) values (?, ?, 0)", token_hash(password), user.UID)
	return password
}

// account_by_address finds the hosted account an address names on a network:
// user@domain or @user@domain on the fediverse and XMPP, @user:domain on
// Matrix, handle.domain or a DID on Bluesky, through the handle registry.
func account_by_address(network string, address string) (*User, map[string]any) {
	address = strings.ToLower(strings.TrimSpace(address))
	name, domain := "", ""
	switch network {
	case "fediverse", "xmpp":
		address = strings.TrimPrefix(strings.TrimPrefix(address, "xmpp:"), "@")
		name, domain, _ = strings.Cut(address, "@")
	case "matrix":
		if rest, ok := strings.CutPrefix(address, "@"); ok {
			name, domain, _ = strings.Cut(rest, ":")
		}
	case "bluesky":
		if strings.HasPrefix(address, "did:") {
			return account_by_did(address)
		}
		for _, d := range handle_domains(nil) {
			if rest, ok := strings.CutSuffix(address, "."+d); ok {
				name, domain = strings.ReplaceAll(rest, "-", "_"), d
			}
		}
	}
	if name == "" || domain == "" {
		return nil, nil
	}
	h := handle_lookup(domain, name)
	if h == nil || h.Kind != "handle" {
		return nil, nil
	}
	user := user_by_uid(h.User)
	if user == nil {
		return nil, nil
	}
	row := account_hosted(user, network, h.Domain, h.Name)
	if row == nil {
		return nil, nil
	}
	return user, row
}

// account_by_did finds the hosted Bluesky account with a DID, across the
// server's users.
func account_by_did(did string) (*User, map[string]any) {
	rows, _ := db_open("db/users.db").rows("select uid from users where status='active'")
	for _, row := range rows {
		user := user_by_uid(row_string(row, "uid"))
		if user == nil {
			continue
		}
		for _, account := range accounts_hosted(user, "bluesky") {
			if account_data(account)["did"] == did {
				return user, account
			}
		}
	}
	return nil, nil
}

// password_check verifies a password typed to a protocol against the minted
// passwords of the account an address names on a network, and answers the
// user, the account and the password's device name. The password's use is
// recorded.
func password_check(network string, address string, password string) (*User, map[string]any, string) {
	user, row := account_by_address(network, address)
	if user == nil || !strings.HasPrefix(password, "mochi-") {
		return nil, nil, ""
	}
	t := token_lookup(password)
	if t == nil || t.Kind != token_password || t.User != user.UID || t.Account != row["id"] {
		return nil, nil, ""
	}
	db_open("db/sessions.db").exec("insert into accesses (hash, user, used) values (?, ?, ?) on conflict(hash) do update set used=excluded.used", t.Hash, t.User, now())
	return user, row, t.Name
}

// password_scram answers the SCRAM forms of an account's minted passwords for
// an XMPP server's challenge: each with its salt, iterations and salted
// password for the hash named, "sha1" or "sha256". The server tries each,
// since the client names no device.
func password_scram(network string, address string, hash string) [][3]any {
	user, row := account_by_address(network, address)
	if user == nil {
		return nil
	}
	rows, _ := db_open("db/users.db").rows("select scram from tokens where user=? and kind=? and account=?", user.UID, token_password, row["id"])
	var out [][3]any
	for _, r := range rows {
		var scram struct {
			Salt       string `json:"salt"`
			Iterations int    `json:"iterations"`
			SHA1       string `json:"sha1"`
			SHA256     string `json:"sha256"`
		}
		if json.Unmarshal([]byte(row_string(r, "scram")), &scram) != nil {
			continue
		}
		salt, _ := base64.StdEncoding.DecodeString(scram.Salt)
		salted := scram.SHA256
		if hash == "sha1" {
			salted = scram.SHA1
		}
		key, _ := base64.StdEncoding.DecodeString(salted)
		out = append(out, [3]any{salt, scram.Iterations, key})
	}
	return out
}

// === Numbers for Lemmy clients ===
//
// Lemmy apps expect a whole number for every post, comment, community, person
// and message and send those numbers back. Each user's Lemmy engine numbers
// the items that user's apps see, in the user's own database, and translates
// both ways (decision 8).

// numbers_table makes the numbering table in a user database.
func numbers_table(db *DB) {
	db.exec("create table if not exists numbers (number integer primary key autoincrement, kind text not null, id text not null, created integer not null, unique (kind, id))")
}

// number_for gives an item its number for a user, the same one each time. The
// row is looked for before it is made, since an insert refused for the unique
// pair would still spend a number.
func number_for(user *User, kind string, id string) int64 {
	db := db_user(user, "user")
	row, _ := db.row("select number from numbers where kind=? and id=?", kind, id)
	if row == nil {
		db.exec("insert or ignore into numbers (kind, id, created) values (?, ?, ?)", kind, id, now())
		row, _ = db.row("select number from numbers where kind=? and id=?", kind, id)
	}
	if row == nil {
		return 0
	}
	number, _ := row["number"].(int64)
	return number
}

// number_identifier answers the item a number names for a user, of the kind
// expected, or empty.
func number_identifier(user *User, kind string, number int64) string {
	row, _ := db_user(user, "user").row("select id from numbers where kind=? and number=?", kind, number)
	if row == nil {
		return ""
	}
	return row_string(row, "id")
}

// signin_scopes_check reports whether a token holds a scope, for the engines:
// the scope itself, or a wider one that covers it on the network.
func signin_scopes_check(t *Token, scope string) bool {
	for _, held := range t.Scopes {
		switch {
		case held == scope, held == "*":
			return true
		case strings.HasPrefix(scope, held+":"):
			return true
		case held == "transition:generic" && (strings.HasPrefix(scope, "rpc:") || strings.HasPrefix(scope, "repo:")):
			return true
		case strings.HasSuffix(held, ":*") && strings.HasPrefix(scope, strings.TrimSuffix(held, "*")):
			return true
		}
	}
	return false
}

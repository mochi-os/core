// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: Bluesky client
//
// A linked Bluesky account on the AT Protocol. The account's identity is
// resolved from its handle to its DID and from the DID document to the server
// that hosts it. Where the system has a configured domain the account links
// by OAuth: Mochi publishes a client document and its keys at /oauth/ on that
// domain, which the remote server fetches, pushes the authorisation request
// (PAR) signed as a confidential client, and holds the tokens bound to a key
// made for the session (DPoP), proving possession on every request and taking
// up the nonce the server demands. Without a domain the account links by app
// password, with the session the server issues. Records go in and out through
// the repository interface (XRPC), and a session's tokens are renewed when the
// server says they have expired.

package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
)

// bluesky_plc is the did:plc directory identities are read from. A variable
// so tests can point it at a server of their own.
var bluesky_plc = "https://plc.directory"

// bluesky_scope is what Mochi asks of an account: the whole of the account,
// as the protocol's transitional scope allows.
const bluesky_scope = "atproto transition:generic"

// bluesky_handle_re is the shape of a handle: labels with dots between.
var bluesky_handle_re = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)+(:[0-9]+)?$`)

// bluesky_lookup reads the DNS TXT records of a name, for a handle's DID. A
// variable so tests need no DNS.
var bluesky_lookup = func(ctx context.Context, name string) ([]string, error) {
	return net.DefaultResolver.LookupTXT(ctx, name)
}

// A bluesky_identity is what the network says an account is: its DID, its
// handle and the server that hosts it.
type bluesky_identity struct {
	DID    string
	Handle string
	PDS    string
}

// bluesky_fetch reads a document for a user under the host policy, up to the
// profile document cap.
func bluesky_fetch(ctx context.Context, user *User, address string, accept string) (int, []byte, error) {
	request, err := http.NewRequestWithContext(ctx, "GET", address, nil)
	if err != nil {
		return 0, nil, err
	}
	request.Header.Set("Accept", accept)
	response, err := network_request(ctx, user, "", request, 5)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	body, err := read_limited(response.Body, resolve_document_maximum)
	return response.StatusCode, body, err
}

// bluesky_resolve finds an account from a handle or a DID.
func bluesky_resolve(ctx context.Context, user *User, identifier string) (*bluesky_identity, error) {
	identifier = strings.ToLower(strings.TrimPrefix(strings.TrimSpace(identifier), "@"))
	did := identifier
	if !strings.HasPrefix(identifier, "did:") {
		if !bluesky_handle_re.MatchString(identifier) {
			return nil, fmt.Errorf("invalid handle")
		}
		host := identifier
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}
		if err := host_allowed(user, "", host); err != nil {
			return nil, err
		}
		var err error
		if did, err = resolve_handle(ctx, identifier); err != nil {
			return nil, err
		}
	}
	if !resolve_did_re.MatchString(did) {
		return nil, fmt.Errorf("invalid identifier")
	}
	identity, err := bluesky_document(ctx, user, did)
	if err != nil {
		return nil, err
	}
	if identity.Handle == "" {
		identity.Handle = identifier
	}
	return identity, nil
}

// bluesky_document reads a DID document and takes the handle and the server
// from it.
func bluesky_document(ctx context.Context, user *User, did string) (*bluesky_identity, error) {
	address := bluesky_plc + "/" + did
	if rest, ok := strings.CutPrefix(did, "did:web:"); ok {
		host := strings.ReplaceAll(strings.SplitN(rest, ":", 2)[0], "%3A", ":")
		address = network_scheme + host + "/.well-known/did.json"
	}
	status, body, err := bluesky_fetch(ctx, user, address, "application/did+ld+json, application/json")
	if err != nil {
		return nil, err
	}
	if status != 200 {
		return nil, fmt.Errorf("the identity %s could not be read: status %d", did, status)
	}
	var document struct {
		ID          string   `json:"id"`
		AlsoKnownAs []string `json:"alsoKnownAs"`
		Service     []struct {
			ID       string `json:"id"`
			Type     string `json:"type"`
			Endpoint string `json:"serviceEndpoint"`
		} `json:"service"`
	}
	if err := json.Unmarshal(body, &document); err != nil || document.ID != did {
		return nil, fmt.Errorf("the identity %s has no document", did)
	}
	identity := &bluesky_identity{DID: did}
	for _, known := range document.AlsoKnownAs {
		if handle, ok := strings.CutPrefix(known, "at://"); ok && identity.Handle == "" {
			identity.Handle = handle
		}
	}
	for _, service := range document.Service {
		if service.ID == "#atproto_pds" && service.Type == "AtprotoPersonalDataServer" {
			identity.PDS = strings.TrimSuffix(service.Endpoint, "/")
		}
	}
	if u, err := url.Parse(identity.PDS); err != nil || identity.PDS == "" || u.Hostname() == "" {
		return nil, fmt.Errorf("the identity %s names no server", did)
	}
	return identity, nil
}

// bluesky_profile is what an account looks like, from its own server.
func bluesky_profile(ctx context.Context, l *linked) *linked_profile {
	did, handle := l.get("did"), l.get("handle")
	profile := &linked_profile{ID: did, Address: handle, Name: handle, URL: "https://bsky.app/profile/" + did, Server: l.get("server")}
	var record struct {
		Value struct {
			Name   string `json:"displayName"`
			Avatar struct {
				Ref struct {
					Link string `json:"$link"`
				} `json:"ref"`
			} `json:"avatar"`
		} `json:"value"`
	}
	status, _, err := bluesky_xrpc(ctx, l, "GET", "com.atproto.repo.getRecord", map[string]string{"repo": did, "collection": "app.bsky.actor.profile", "rkey": "self"}, nil, "", &record)
	if err == nil && status == 200 {
		if record.Value.Name != "" {
			profile.Name = record.Value.Name
		}
		if cid := record.Value.Avatar.Ref.Link; cid != "" {
			profile.Avatar = l.get("pds") + "/xrpc/com.atproto.sync.getBlob?did=" + url.QueryEscape(did) + "&cid=" + url.QueryEscape(cid)
		}
	}
	return profile
}

// === App password ===

// bluesky_link_password links an account by handle and app password: the
// session the server issues is kept with the password, so a session that has
// run out is made again without the user.
func bluesky_link_password(ctx context.Context, user *User, fields map[string]string) (*linked_profile, map[string]any, error) {
	identity, err := bluesky_resolve(ctx, user, fields["handle"])
	if err != nil {
		return nil, nil, err
	}
	l := bluesky_linked(user, identity)
	l.data["password"] = fields["password"]
	if err := bluesky_session(ctx, l); err != nil {
		return nil, nil, err
	}
	return bluesky_profile(ctx, l), l.data, nil
}

// bluesky_linked starts an account's data from its identity.
func bluesky_linked(user *User, identity *bluesky_identity) *linked {
	u, _ := url.Parse(identity.PDS)
	return &linked{user: user, kind: "bluesky", data: map[string]any{
		"did":    identity.DID,
		"handle": identity.Handle,
		"pds":    identity.PDS,
		"server": u.Hostname(),
	}}
}

// bluesky_session signs in with the app password.
func bluesky_session(ctx context.Context, l *linked) error {
	var session struct {
		Access  string `json:"accessJwt"`
		Refresh string `json:"refreshJwt"`
		DID     string `json:"did"`
		Handle  string `json:"handle"`
	}
	l.set("token", "")
	status, body, err := bluesky_xrpc(ctx, l, "POST", "com.atproto.server.createSession", nil, map[string]any{"identifier": l.get("did"), "password": l.get("password")}, "", &session)
	if err != nil {
		return err
	}
	if status == 400 || status == 401 {
		return fmt.Errorf("the server did not accept the sign-in: %v", linked_failure(status, body))
	}
	if status < 200 || status > 299 || session.Access == "" || session.DID != l.get("did") {
		return fmt.Errorf("the sign-in failed: %v", linked_failure(status, body))
	}
	l.set("token", session.Access)
	l.set("refresh", session.Refresh)
	if session.Handle != "" {
		l.set("handle", session.Handle)
	}
	return nil
}

// bluesky_refresh_session renews an app-password session, or signs in again.
func bluesky_refresh_session(ctx context.Context, l *linked) error {
	var session struct {
		Access  string `json:"accessJwt"`
		Refresh string `json:"refreshJwt"`
	}
	if refresh := l.get("refresh"); refresh != "" {
		request, err := http.NewRequestWithContext(ctx, "POST", l.get("pds")+"/xrpc/com.atproto.server.refreshSession", nil)
		if err == nil {
			request.Header.Set("Authorization", "Bearer "+refresh)
			request.Header.Set("Accept", "application/json")
			if response, err := network_send(ctx, l.user, "", l.get("server")+"|"+l.id, request, 0); err == nil {
				body, _ := read_limited(response.Body, linked_body_maximum)
				response.Body.Close()
				if response.StatusCode == 200 && json.Unmarshal(body, &session) == nil && session.Access != "" {
					l.set("token", session.Access)
					l.set("refresh", session.Refresh)
					l.save()
					return nil
				}
			}
		}
	}
	if l.get("password") == "" {
		return linked_signed_out
	}
	if err := bluesky_session(ctx, l); err != nil {
		return linked_signed_out
	}
	l.save()
	return nil
}

// === OAuth ===

// bluesky_client_base is where this server publishes its client document:
// the public URL the owner set for OAuth, else the configured domain.
func bluesky_client_base() string {
	return server_public_url()
}

// bluesky_client_id is the client's id: the URL of its document.
func bluesky_client_id() string {
	return bluesky_client_base() + "/oauth/client-metadata.json"
}

// bluesky_client_redirect is where the remote server sends the browser back.
func bluesky_client_redirect() string {
	return bluesky_client_base() + "/_/auth/oauth/bluesky/callback"
}

// bluesky_client_key is the server's P-256 key as a confidential client, made
// once and kept in networks.db.
func bluesky_client_key() (*ecdsa.PrivateKey, error) {
	private := networks_key("bluesky-client", key_p256)
	if private == "" {
		return nil, fmt.Errorf("no client key")
	}
	return key_p256_private(private)
}

// bluesky_jwk writes a P-256 public key as a JWK.
func bluesky_jwk(public *ecdsa.PublicKey) map[string]any {
	x := public.X.FillBytes(make([]byte, 32))
	y := public.Y.FillBytes(make([]byte, 32))
	return map[string]any{
		"kty": "EC",
		"crv": "P-256",
		"x":   base64.RawURLEncoding.EncodeToString(x),
		"y":   base64.RawURLEncoding.EncodeToString(y),
	}
}

// bluesky_kid names a key by a hash of its public point.
func bluesky_kid(public *ecdsa.PublicKey) string {
	sum := sha256.Sum256(append(public.X.FillBytes(make([]byte, 32)), public.Y.FillBytes(make([]byte, 32))...))
	return base64.RawURLEncoding.EncodeToString(sum[:16])
}

// bluesky_client_engine serves the client document and the keys the remote
// server fetches: at /oauth/client-metadata.json and /oauth/jwks.json, on a
// server with a configured domain while the development switch is on.
func bluesky_client_engine() *engine {
	return &engine{
		name:     "bluesky-client",
		prefixes: []string{"/oauth/client-metadata.json", "/oauth/jwks.json"},
		handler: func(c *gin.Context) bool {
			if !networks_development || bluesky_client_base() == "" {
				respond_error(c, http.StatusNotFound, "not_found", "errors.not_found", nil)
				return true
			}
			key, err := bluesky_client_key()
			if err != nil {
				respond_error(c, http.StatusInternalServerError, "server_error", "errors.server_error", nil)
				return true
			}
			jwk := bluesky_jwk(&key.PublicKey)
			jwk["kid"] = bluesky_kid(&key.PublicKey)
			jwk["use"] = "sig"
			jwk["alg"] = "ES256"
			if strings.HasSuffix(c.Request.URL.Path, "/jwks.json") {
				c.JSON(http.StatusOK, map[string]any{"keys": []any{jwk}})
				return true
			}
			c.JSON(http.StatusOK, map[string]any{
				"client_id":                       bluesky_client_id(),
				"client_name":                     "Mochi",
				"client_uri":                      bluesky_client_base(),
				"redirect_uris":                   []string{bluesky_client_redirect()},
				"grant_types":                     []string{"authorization_code", "refresh_token"},
				"response_types":                  []string{"code"},
				"scope":                           bluesky_scope,
				"application_type":                "web",
				"token_endpoint_auth_method":      "private_key_jwt",
				"token_endpoint_auth_signing_alg": "ES256",
				"dpop_bound_access_tokens":        true,
				"jwks_uri":                        bluesky_client_base() + "/oauth/jwks.json",
			})
			return true
		},
	}
}

// A bluesky_server is an authorization server's metadata, cached by issuer.
type bluesky_server struct {
	Issuer  string `json:"issuer"`
	Push    string `json:"pushed_authorization_request_endpoint"`
	Consent string `json:"authorization_endpoint"`
	Token   string `json:"token_endpoint"`
	Revoke  string `json:"revocation_endpoint"`
	expires time.Time
}

var bluesky_servers = map[string]*bluesky_server{}
var bluesky_nonces = map[string]string{}
var bluesky_lock sync.Mutex

// bluesky_authorization finds the authorization server of a hosting server:
// the protected resource document names it, and its own document names its
// endpoints. Cached for an hour.
func bluesky_authorization(ctx context.Context, user *User, pds string) (*bluesky_server, error) {
	var resource struct {
		Servers []string `json:"authorization_servers"`
	}
	status, body, err := bluesky_fetch(ctx, user, pds+"/.well-known/oauth-protected-resource", "application/json")
	if err != nil {
		return nil, err
	}
	if status != 200 || json.Unmarshal(body, &resource) != nil || len(resource.Servers) == 0 {
		return nil, fmt.Errorf("%s names no authorization server", pds)
	}
	issuer := strings.TrimSuffix(resource.Servers[0], "/")
	bluesky_lock.Lock()
	cached := bluesky_servers[issuer]
	bluesky_lock.Unlock()
	if cached != nil && time.Now().Before(cached.expires) {
		return cached, nil
	}
	status, body, err = bluesky_fetch(ctx, user, issuer+"/.well-known/oauth-authorization-server", "application/json")
	if err != nil {
		return nil, err
	}
	var server bluesky_server
	if status != 200 || json.Unmarshal(body, &server) != nil || strings.TrimSuffix(server.Issuer, "/") != issuer || server.Push == "" || server.Consent == "" || server.Token == "" {
		return nil, fmt.Errorf("%s has no usable authorization server document", issuer)
	}
	server.Issuer = issuer
	server.expires = time.Now().Add(time.Hour)
	bluesky_lock.Lock()
	bluesky_servers[issuer] = &server
	bluesky_lock.Unlock()
	return &server, nil
}

// bluesky_nonce_set keeps the nonce a server demanded for its DPoP proofs.
func bluesky_nonce_set(host string, nonce string) {
	bluesky_lock.Lock()
	bluesky_nonces[host] = nonce
	bluesky_lock.Unlock()
}

// bluesky_nonce reads the nonce a server last demanded.
func bluesky_nonce(host string) string {
	bluesky_lock.Lock()
	defer bluesky_lock.Unlock()
	return bluesky_nonces[host]
}

// bluesky_wants_nonce reports whether an answer asks for a fresh DPoP nonce,
// which the header beside it carries.
func bluesky_wants_nonce(response *http.Response, body []byte) bool {
	if response.StatusCode != 400 && response.StatusCode != 401 {
		return false
	}
	if strings.Contains(response.Header.Get("WWW-Authenticate"), "use_dpop_nonce") {
		return true
	}
	var answer struct {
		Error string `json:"error"`
	}
	return json.Unmarshal(body, &answer) == nil && answer.Error == "use_dpop_nonce"
}

// bluesky_proof makes a DPoP proof for a request: a JWT signed with the
// session's key, naming the method and the URL, the server's nonce where one
// is known, and the hash of the access token when one is sent.
func bluesky_proof(key *ecdsa.PrivateKey, method string, address string, token string) (string, error) {
	u, err := url.Parse(address)
	if err != nil {
		return "", err
	}
	u.RawQuery, u.Fragment = "", ""
	claims := jwt.MapClaims{
		"jti": random_alphanumeric(24),
		"htm": method,
		"htu": u.String(),
		"iat": time.Now().Unix(),
	}
	if nonce := bluesky_nonce(u.Host); nonce != "" {
		claims["nonce"] = nonce
	}
	if token != "" {
		sum := sha256.Sum256([]byte(token))
		claims["ath"] = base64.RawURLEncoding.EncodeToString(sum[:])
	}
	proof := jwt.NewWithClaims(jwt.SigningMethodES256, claims)
	proof.Header["typ"] = "dpop+jwt"
	proof.Header["jwk"] = bluesky_jwk(&key.PublicKey)
	return proof.SignedString(key)
}

// bluesky_assertion makes the client assertion a confidential client sends
// to the authorization server: a JWT signed with the client key.
func bluesky_assertion(issuer string) (string, error) {
	key, err := bluesky_client_key()
	if err != nil {
		return "", err
	}
	assertion := jwt.NewWithClaims(jwt.SigningMethodES256, jwt.MapClaims{
		"iss": bluesky_client_id(),
		"sub": bluesky_client_id(),
		"aud": issuer,
		"jti": random_alphanumeric(24),
		"iat": time.Now().Unix(),
		"exp": time.Now().Add(5 * time.Minute).Unix(),
	})
	assertion.Header["kid"] = bluesky_kid(&key.PublicKey)
	return assertion.SignedString(key)
}

// bluesky_session_key reads a session's DPoP key from its PEM.
func bluesky_session_key(text string) (*ecdsa.PrivateKey, error) {
	if text == "" {
		return nil, fmt.Errorf("the session has no key")
	}
	return key_p256_private(text)
}

// bluesky_post_signed sends a form to the authorization server with the
// client assertion and a DPoP proof, taking up a demanded nonce once, and
// decodes the answer.
func bluesky_post_signed(ctx context.Context, user *User, limiter string, server *bluesky_server, endpoint string, key *ecdsa.PrivateKey, values url.Values, into any) (int, []byte, error) {
	for attempt := 0; attempt < 2; attempt++ {
		assertion, err := bluesky_assertion(server.Issuer)
		if err != nil {
			return 0, nil, err
		}
		values.Set("client_id", bluesky_client_id())
		values.Set("client_assertion_type", "urn:ietf:params:oauth:client-assertion-type:jwt-bearer")
		values.Set("client_assertion", assertion)
		proof, err := bluesky_proof(key, "POST", endpoint, "")
		if err != nil {
			return 0, nil, err
		}
		request, err := http.NewRequestWithContext(ctx, "POST", endpoint, strings.NewReader(values.Encode()))
		if err != nil {
			return 0, nil, err
		}
		request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		request.Header.Set("Accept", "application/json")
		request.Header.Set("DPoP", proof)
		response, err := network_send(ctx, user, "", limiter, request, 0)
		if err != nil {
			return 0, nil, err
		}
		body, err := read_limited(response.Body, linked_body_maximum)
		response.Body.Close()
		if err != nil {
			return 0, nil, err
		}
		if nonce := response.Header.Get("DPoP-Nonce"); nonce != "" {
			bluesky_nonce_set(request.URL.Host, nonce)
			if attempt == 0 && bluesky_wants_nonce(response, body) {
				continue
			}
		}
		if into != nil && response.StatusCode >= 200 && response.StatusCode <= 299 {
			if err := json.Unmarshal(body, into); err != nil {
				return response.StatusCode, body, fmt.Errorf("the server's answer is not JSON")
			}
		}
		return response.StatusCode, body, nil
	}
	return 0, nil, fmt.Errorf("the server kept demanding a nonce")
}

// bluesky_prepare readies a link ceremony by OAuth: the account is resolved,
// its authorization server found, a key made for the session, and the
// authorisation request pushed; the consent URL names what was pushed.
func bluesky_prepare(ctx context.Context, user *User, st *oauth_state, handle string) error {
	if bluesky_client_base() == "" {
		return fmt.Errorf("linking by OAuth needs a configured domain")
	}
	identity, err := bluesky_resolve(ctx, user, handle)
	if err != nil {
		return err
	}
	server, err := bluesky_authorization(ctx, user, identity.PDS)
	if err != nil {
		return err
	}
	_, private, err := key_generate(key_p256)
	if err != nil {
		return err
	}
	key, err := key_p256_private(private)
	if err != nil {
		return err
	}
	st.Redirect = bluesky_client_redirect()
	var pushed struct {
		Request string `json:"request_uri"`
	}
	status, body, err := bluesky_post_signed(ctx, user, strings.ToLower(identifier_host(server.Push))+"|link", server, server.Push, key, url.Values{
		"response_type":         {"code"},
		"redirect_uri":          {st.Redirect},
		"scope":                 {bluesky_scope},
		"state":                 {st.State},
		"code_challenge":        {oauth_challenge(st.Verifier)},
		"code_challenge_method": {"S256"},
		"login_hint":            {identity.Handle},
	}, &pushed)
	if err != nil {
		return err
	}
	if status < 200 || status > 299 || pushed.Request == "" {
		return fmt.Errorf("the authorization server refused the request: %v", linked_failure(status, body))
	}
	st.Server = identity.PDS
	st.Identifier = identity.DID
	st.Issuer = server.Issuer
	st.Key = private
	st.Consent = server.Consent + "?" + url.Values{"client_id": {bluesky_client_id()}, "request_uri": {pushed.Request}}.Encode()
	return nil
}

// bluesky_callback completes a link by OAuth: the issuer must be the one the
// request went to, the code is exchanged with the session's key, and the
// tokens must be for the account resolved.
func bluesky_callback(ctx context.Context, user *User, st *oauth_state, code string, issuer string) (*linked_profile, map[string]any, error) {
	if strings.TrimSuffix(issuer, "/") != st.Issuer {
		return nil, nil, fmt.Errorf("the callback names another issuer")
	}
	server, err := bluesky_authorization(ctx, user, st.Server)
	if err != nil {
		return nil, nil, err
	}
	key, err := bluesky_session_key(st.Key)
	if err != nil {
		return nil, nil, err
	}
	var token struct {
		Access  string `json:"access_token"`
		Refresh string `json:"refresh_token"`
		Type    string `json:"token_type"`
		Subject string `json:"sub"`
		Expires int64  `json:"expires_in"`
		Scope   string `json:"scope"`
	}
	status, body, err := bluesky_post_signed(ctx, user, strings.ToLower(identifier_host(server.Token))+"|link", server, server.Token, key, url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {code},
		"redirect_uri":  {st.Redirect},
		"code_verifier": {st.Verifier},
	}, &token)
	if err != nil {
		return nil, nil, err
	}
	if status < 200 || status > 299 || token.Access == "" {
		return nil, nil, fmt.Errorf("the token exchange failed: %v", linked_failure(status, body))
	}
	if token.Subject != st.Identifier || !strings.EqualFold(token.Type, "DPoP") {
		return nil, nil, fmt.Errorf("the tokens are not for the account resolved")
	}
	identity, err := bluesky_document(ctx, user, st.Identifier)
	if err != nil {
		return nil, nil, err
	}
	l := bluesky_linked(user, identity)
	l.data["oauth"] = true
	l.data["issuer"] = server.Issuer
	l.data["key"] = st.Key
	l.data["token"] = token.Access
	l.data["refresh"] = token.Refresh
	l.data["expires"] = now() + token.Expires
	l.data["scopes"] = strings.Fields(token.Scope)
	return bluesky_profile(ctx, l), l.data, nil
}

// bluesky_renew_oauth renews the tokens with the refresh token.
func bluesky_renew_oauth(ctx context.Context, l *linked) error {
	refresh := l.get("refresh")
	if refresh == "" {
		return linked_signed_out
	}
	server, err := bluesky_authorization(ctx, l.user, l.get("pds"))
	if err != nil {
		return err
	}
	key, err := bluesky_session_key(l.get("key"))
	if err != nil {
		return linked_signed_out
	}
	var token struct {
		Access  string `json:"access_token"`
		Refresh string `json:"refresh_token"`
		Subject string `json:"sub"`
		Expires int64  `json:"expires_in"`
	}
	status, _, err := bluesky_post_signed(ctx, l.user, l.get("server")+"|"+l.id, server, server.Token, key, url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {refresh},
	}, &token)
	if err != nil {
		return err
	}
	if status == 400 || status == 401 {
		return linked_signed_out
	}
	if status < 200 || status > 299 || token.Access == "" || token.Subject != l.get("did") {
		return fmt.Errorf("the token refresh failed with status %d", status)
	}
	l.set("token", token.Access)
	l.set("refresh", token.Refresh)
	l.set("expires", now()+token.Expires)
	l.save()
	return nil
}

// bluesky_forget revokes an OAuth session when the account is unlinked, best
// effort.
func bluesky_forget(ctx context.Context, l *linked) {
	if l.data["oauth"] != true || l.get("token") == "" {
		return
	}
	server, err := bluesky_authorization(ctx, l.user, l.get("pds"))
	if err != nil || server.Revoke == "" {
		return
	}
	key, err := bluesky_session_key(l.get("key"))
	if err != nil {
		return
	}
	bluesky_post_signed(ctx, l.user, l.get("server")+"|"+l.id, server, server.Revoke, key, url.Values{"token": {l.get("token")}}, nil)
}

// === Requests ===

// bluesky_authorize adds the session's credential: the DPoP token with its
// proof for an OAuth session, the bearer for an app-password one.
func bluesky_authorize(l *linked, request *http.Request, body []byte) error {
	// No token yet, as at the sign-in: the request goes plain.
	token := l.get("token")
	if token == "" {
		return nil
	}
	if l.data["oauth"] != true {
		request.Header.Set("Authorization", "Bearer "+token)
		return nil
	}
	key, err := bluesky_session_key(l.get("key"))
	if err != nil {
		return linked_signed_out
	}
	proof, err := bluesky_proof(key, request.Method, request.URL.String(), token)
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "DPoP "+token)
	request.Header.Set("DPoP", proof)
	return nil
}

// bluesky_expired reports whether an answer says the session has run out.
func bluesky_expired(status int, body []byte) bool {
	if status == 401 {
		return true
	}
	var answer struct {
		Error string `json:"error"`
	}
	return status == 400 && json.Unmarshal(body, &answer) == nil && answer.Error == "ExpiredToken"
}

// bluesky_renew renews whichever session the account has.
func bluesky_renew(ctx context.Context, l *linked) error {
	if l.data["oauth"] == true {
		return bluesky_renew_oauth(ctx, l)
	}
	return bluesky_refresh_session(ctx, l)
}

// bluesky_xrpc sends one XRPC request to the account's server: a query by
// GET with its parameters, a procedure by POST with a JSON body, or raw bytes
// of the content type given.
func bluesky_xrpc(ctx context.Context, l *linked, method string, nsid string, query map[string]string, body any, content string, into any) (int, []byte, error) {
	address := l.get("pds") + "/xrpc/" + nsid
	if len(query) > 0 {
		values := url.Values{}
		for key, value := range query {
			values.Set(key, value)
		}
		address += "?" + values.Encode()
	}
	headers := map[string]string{}
	var encoded []byte
	switch value := body.(type) {
	case nil:
	case []byte:
		encoded = value
		headers["Content-Type"] = content
	default:
		encoded = []byte(json_encode(body))
		headers["Content-Type"] = "application/json"
	}
	status, data, _, err := l.request(ctx, method, address, headers, encoded)
	if err != nil {
		return status, data, err
	}
	if into != nil && status >= 200 && status <= 299 && len(data) > 0 {
		if err := json.Unmarshal(data, into); err != nil {
			return status, data, fmt.Errorf("the server's answer is not JSON")
		}
	}
	return status, data, nil
}

// bluesky_verify checks the session with the server.
func bluesky_verify(ctx context.Context, l *linked) error {
	var session struct {
		DID string `json:"did"`
	}
	status, body, err := bluesky_xrpc(ctx, l, "GET", "com.atproto.server.getSession", nil, nil, "", &session)
	if err != nil {
		return err
	}
	if status < 200 || status > 299 || session.DID != l.get("did") {
		return linked_failure(status, body)
	}
	return nil
}

// bluesky_uri reads an at:// record URI into its parts.
func bluesky_uri(id string) (string, string, string, error) {
	rest, ok := strings.CutPrefix(id, "at://")
	parts := strings.Split(rest, "/")
	if !ok || len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return "", "", "", fmt.Errorf("invalid record uri %q", id)
	}
	return parts[0], parts[1], parts[2], nil
}

// bluesky_result shapes a record's answer for an app: the URI as its id, the
// page on Bluesky for a post, and the answer.
func bluesky_result(answer map[string]any, did string) map[string]any {
	uri, _ := answer["uri"].(string)
	page := ""
	if _, collection, rkey, err := bluesky_uri(uri); err == nil && collection == "app.bsky.feed.post" {
		page = "https://bsky.app/profile/" + did + "/post/" + rkey
	}
	return map[string]any{"id": uri, "url": page, "data": answer}
}

// bluesky_create writes a record: the object is the record, with $type naming
// its collection (app.bsky.feed.post when absent) and createdAt filled in.
func bluesky_create(ctx context.Context, l *linked, object map[string]any) (map[string]any, error) {
	collection, _ := object["$type"].(string)
	if collection == "" {
		collection = "app.bsky.feed.post"
		object["$type"] = collection
	}
	if _, set := object["createdAt"]; !set {
		object["createdAt"] = time.Now().UTC().Format(time.RFC3339Nano)
	}
	var answer map[string]any
	status, body, err := bluesky_xrpc(ctx, l, "POST", "com.atproto.repo.createRecord", nil, map[string]any{"repo": l.get("did"), "collection": collection, "record": object}, "", &answer)
	if err != nil {
		return nil, err
	}
	if status < 200 || status > 299 {
		return nil, linked_failure(status, body)
	}
	return bluesky_result(answer, l.get("did")), nil
}

// bluesky_update writes a new version of a record the account holds.
func bluesky_update(ctx context.Context, l *linked, id string, object map[string]any) (map[string]any, error) {
	repo, collection, rkey, err := bluesky_uri(id)
	if err != nil {
		return nil, err
	}
	if repo != l.get("did") {
		return nil, fmt.Errorf("the record is not the account's")
	}
	if _, set := object["$type"]; !set {
		object["$type"] = collection
	}
	var answer map[string]any
	status, body, err := bluesky_xrpc(ctx, l, "POST", "com.atproto.repo.putRecord", nil, map[string]any{"repo": repo, "collection": collection, "rkey": rkey, "record": object}, "", &answer)
	if err != nil {
		return nil, err
	}
	if status < 200 || status > 299 {
		return nil, linked_failure(status, body)
	}
	return bluesky_result(answer, repo), nil
}

// bluesky_delete deletes a record the account holds.
func bluesky_delete(ctx context.Context, l *linked, id string) error {
	repo, collection, rkey, err := bluesky_uri(id)
	if err != nil {
		return err
	}
	if repo != l.get("did") {
		return fmt.Errorf("the record is not the account's")
	}
	status, body, err := bluesky_xrpc(ctx, l, "POST", "com.atproto.repo.deleteRecord", nil, map[string]any{"repo": repo, "collection": collection, "rkey": rkey}, "", nil)
	if err != nil {
		return err
	}
	if status < 200 || status > 299 {
		return linked_failure(status, body)
	}
	return nil
}

// bluesky_page reads one page of a feed query: the items under field and the
// cursor.
func bluesky_page(ctx context.Context, l *linked, nsid string, query map[string]string, field string, cursor string, limit int) (*linked_page, error) {
	if limit > 100 {
		limit = 100
	}
	query["limit"] = strconv.Itoa(limit)
	if cursor != "" {
		query["cursor"] = cursor
	}
	var answer map[string]any
	status, body, err := bluesky_xrpc(ctx, l, "GET", nsid, query, nil, "", &answer)
	if err != nil {
		return nil, err
	}
	if status < 200 || status > 299 {
		return nil, linked_failure(status, body)
	}
	items, _ := answer[field].([]any)
	page := &linked_page{Items: items}
	if len(items) > 0 {
		page.Cursor, _ = answer["cursor"].(string)
	}
	return page, nil
}

// bluesky_timeline reads the home timeline.
func bluesky_timeline(ctx context.Context, l *linked, cursor string, limit int) (*linked_page, error) {
	return bluesky_page(ctx, l, "app.bsky.feed.getTimeline", map[string]string{}, "feed", cursor, limit)
}

// bluesky_posts reads the account's own posts and replies.
func bluesky_posts(ctx context.Context, l *linked, cursor string, limit int) (*linked_page, error) {
	return bluesky_page(ctx, l, "app.bsky.feed.getAuthorFeed", map[string]string{"actor": l.get("did"), "filter": "posts_with_replies"}, "feed", cursor, limit)
}

// bluesky_notifications reads the account's notifications.
func bluesky_notifications(ctx context.Context, l *linked, cursor string, limit int) (*linked_page, error) {
	return bluesky_page(ctx, l, "app.bsky.notification.listNotifications", map[string]string{}, "notifications", cursor, limit)
}

// bluesky_upload uploads a blob, which a record embeds by the reference
// answered.
func bluesky_upload(ctx context.Context, l *linked, content []byte, kind string) (map[string]any, error) {
	var answer struct {
		Blob map[string]any `json:"blob"`
	}
	status, body, err := bluesky_xrpc(ctx, l, "POST", "com.atproto.repo.uploadBlob", nil, content, kind, &answer)
	if err != nil {
		return nil, err
	}
	if status < 200 || status > 299 || answer.Blob == nil {
		return nil, linked_failure(status, body)
	}
	id := ""
	if ref, ok := answer.Blob["ref"].(map[string]any); ok {
		id, _ = ref["$link"].(string)
	}
	return map[string]any{"id": id, "url": bluesky_blob(l.get("pds"), l.get("did"), id), "data": answer.Blob}, nil
}

// bluesky_request sends an app's own XRPC request: the path is the method's
// name.
func bluesky_request(ctx context.Context, l *linked, method string, nsid string, query map[string]string, body any) (int, []byte, error) {
	if strings.Contains(nsid, "/") {
		return 0, nil, fmt.Errorf("the path is an XRPC method name")
	}
	return bluesky_xrpc(ctx, l, method, nsid, query, body, "", nil)
}

// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	sl "go.starlark.net/starlark"
)

// A bluesky_fake is one server playing the handle's host, the DID document's
// host, the hosting server and the authorization server.
type bluesky_fake struct {
	server     *httptest.Server
	lock       sync.Mutex
	did        string
	handle     string
	pushed     map[string]string // the DPoP key each pushed request (by its code challenge) is bound to
	thumbprint string            // the key of the session that signed in
	pushes     int
	expired    bool
	refused    bool // a refresh is refused, so the app password is used again
	revokes    int
	records    []map[string]any
}

// dpop_check verifies a DPoP proof against the request and returns its claims
// and the key's thumbprint, or why it is wrong.
func dpop_check(r *http.Request, base string, token string, nonce string) (jwt.MapClaims, string, string) {
	proof := r.Header.Get("DPoP")
	if proof == "" {
		return nil, "", "no proof"
	}
	var jwk map[string]any
	parsed, err := jwt.Parse(proof, func(t *jwt.Token) (any, error) {
		jwk, _ = t.Header["jwk"].(map[string]any)
		if t.Header["typ"] != "dpop+jwt" || jwk == nil {
			return nil, fmt.Errorf("bad header")
		}
		return jwk_public(jwk)
	}, jwt.WithValidMethods([]string{"ES256"}))
	if err != nil || !parsed.Valid {
		return nil, "", "invalid proof: " + fmt.Sprint(err)
	}
	claims := parsed.Claims.(jwt.MapClaims)
	if claims["htm"] != r.Method || claims["htu"] != base+r.URL.Path || claims["jti"] == "" {
		return nil, "", "proof names another request"
	}
	if nonce != "" && claims["nonce"] != nonce {
		return claims, "", "use_dpop_nonce"
	}
	if token != "" {
		sum := sha256.Sum256([]byte(token))
		if claims["ath"] != base64.RawURLEncoding.EncodeToString(sum[:]) {
			return nil, "", "ath mismatch"
		}
	}
	return claims, fmt.Sprint(jwk["x"], jwk["y"]), ""
}

func create_bluesky_fake(t *testing.T) *bluesky_fake {
	t.Helper()
	f := &bluesky_fake{pushed: map[string]string{}}
	mux := http.NewServeMux()
	base := func() string { return f.server.URL }
	mux.HandleFunc("GET /.well-known/atproto-did", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(f.did + "\n"))
	})
	mux.HandleFunc("GET /.well-known/did.json", func(w http.ResponseWriter, r *http.Request) {
		answer(w, 200, map[string]any{"id": f.did, "alsoKnownAs": []string{"at://" + f.handle}, "service": []any{map[string]any{"id": "#atproto_pds", "type": "AtprotoPersonalDataServer", "serviceEndpoint": base()}}})
	})
	mux.HandleFunc("GET /.well-known/oauth-protected-resource", func(w http.ResponseWriter, r *http.Request) {
		answer(w, 200, map[string]any{"resource": base(), "authorization_servers": []string{base()}})
	})
	mux.HandleFunc("GET /.well-known/oauth-authorization-server", func(w http.ResponseWriter, r *http.Request) {
		answer(w, 200, map[string]any{"issuer": base(), "pushed_authorization_request_endpoint": base() + "/par", "authorization_endpoint": base() + "/authorize", "token_endpoint": base() + "/token", "revocation_endpoint": base() + "/revoke", "dpop_signing_alg_values_supported": []string{"ES256"}})
	})
	assertion := func(r *http.Request) string {
		if r.Form.Get("client_assertion_type") != "urn:ietf:params:oauth:client-assertion-type:jwt-bearer" || r.Form.Get("client_id") != "https://example.com/oauth/client-metadata.json" {
			return "no client assertion"
		}
		key, _ := bluesky_client_key()
		parsed, err := jwt.Parse(r.Form.Get("client_assertion"), func(t *jwt.Token) (any, error) { return &key.PublicKey, nil }, jwt.WithValidMethods([]string{"ES256"}), jwt.WithAudience(base()), jwt.WithIssuer("https://example.com/oauth/client-metadata.json"))
		if err != nil || !parsed.Valid || parsed.Header["kid"] != bluesky_kid(&key.PublicKey) {
			return "invalid client assertion: " + fmt.Sprint(err)
		}
		return ""
	}
	mux.HandleFunc("POST /par", func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		f.lock.Lock()
		defer f.lock.Unlock()
		f.pushes++
		w.Header().Set("DPoP-Nonce", "n1")
		_, thumbprint, fault := dpop_check(r, base(), "", "n1")
		if fault == "use_dpop_nonce" {
			answer(w, 400, map[string]any{"error": "use_dpop_nonce"})
			return
		}
		if fault != "" {
			answer(w, 400, map[string]any{"error": "invalid_dpop_proof", "error_description": fault})
			return
		}
		if fault := assertion(r); fault != "" {
			answer(w, 401, map[string]any{"error": "invalid_client", "error_description": fault})
			return
		}
		if r.Form.Get("redirect_uri") != "https://example.com/_/auth/oauth/bluesky/callback" || r.Form.Get("scope") != bluesky_scope || r.Form.Get("response_type") != "code" || r.Form.Get("code_challenge_method") != "S256" || r.Form.Get("login_hint") != f.handle || r.Form.Get("state") == "" {
			answer(w, 400, map[string]any{"error": "invalid_request", "error_description": r.Form.Encode()})
			return
		}
		f.pushed[r.Form.Get("code_challenge")] = thumbprint
		answer(w, 201, map[string]any{"request_uri": "urn:ietf:params:oauth:request_uri:req1", "expires_in": 60})
	})
	mux.HandleFunc("POST /token", func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		f.lock.Lock()
		defer f.lock.Unlock()
		w.Header().Set("DPoP-Nonce", "n1")
		_, thumbprint, fault := dpop_check(r, base(), "", "n1")
		if fault == "use_dpop_nonce" {
			answer(w, 400, map[string]any{"error": "use_dpop_nonce"})
			return
		}
		if fault != "" {
			answer(w, 400, map[string]any{"error": "invalid_dpop_proof", "error_description": fault})
			return
		}
		if fault := assertion(r); fault != "" {
			answer(w, 401, map[string]any{"error": "invalid_client", "error_description": fault})
			return
		}
		switch r.Form.Get("grant_type") {
		case "authorization_code":
			// The token is bound to the key that pushed the request.
			challenge := oauth_challenge(r.Form.Get("code_verifier"))
			if r.Form.Get("code") != "code-1" || f.pushed[challenge] == "" || f.pushed[challenge] != thumbprint || r.Form.Get("redirect_uri") != "https://example.com/_/auth/oauth/bluesky/callback" {
				answer(w, 400, map[string]any{"error": "invalid_grant", "error_description": "the code, the verifier or the key is wrong"})
				return
			}
			f.thumbprint = thumbprint
			answer(w, 200, map[string]any{"access_token": "at1", "refresh_token": "rt1", "token_type": "DPoP", "sub": f.did, "scope": bluesky_scope, "expires_in": 3600})
		case "refresh_token":
			if r.Form.Get("refresh_token") != "rt1" || thumbprint != f.thumbprint {
				answer(w, 400, map[string]any{"error": "invalid_grant"})
				return
			}
			answer(w, 200, map[string]any{"access_token": "at2", "refresh_token": "rt2", "token_type": "DPoP", "sub": f.did, "scope": bluesky_scope, "expires_in": 3600})
		default:
			answer(w, 400, map[string]any{"error": "unsupported_grant_type"})
		}
	})
	mux.HandleFunc("POST /revoke", func(w http.ResponseWriter, r *http.Request) {
		f.lock.Lock()
		f.revokes++
		f.lock.Unlock()
		w.WriteHeader(200)
	})
	// The hosting server: the session's credential is checked first, OAuth
	// with its proof or the app-password session as a bearer.
	authorized := func(w http.ResponseWriter, r *http.Request) bool {
		f.lock.Lock()
		defer f.lock.Unlock()
		header := r.Header.Get("Authorization")
		switch {
		case strings.HasPrefix(header, "DPoP "):
			token := strings.TrimPrefix(header, "DPoP ")
			w.Header().Set("DPoP-Nonce", "p1")
			_, thumbprint, fault := dpop_check(r, base(), token, "p1")
			if fault == "use_dpop_nonce" {
				w.Header().Set("WWW-Authenticate", `DPoP error="use_dpop_nonce"`)
				answer(w, 401, map[string]any{"error": "use_dpop_nonce"})
				return false
			}
			if fault != "" || thumbprint != f.thumbprint {
				answer(w, 401, map[string]any{"error": "invalid_dpop_proof", "error_description": fault})
				return false
			}
			if (token == "at1" && f.expired) || (token != "at1" && token != "at2") {
				w.Header().Set("WWW-Authenticate", `DPoP error="invalid_token"`)
				answer(w, 401, map[string]any{"error": "invalid_token"})
				return false
			}
			return true
		case strings.HasPrefix(header, "Bearer "):
			token := strings.TrimPrefix(header, "Bearer ")
			if token == "a1" && f.expired {
				answer(w, 400, map[string]any{"error": "ExpiredToken", "message": "Token has expired"})
				return false
			}
			if token != "a1" && token != "a2" {
				answer(w, 401, map[string]any{"error": "AuthenticationRequired"})
				return false
			}
			return true
		}
		answer(w, 401, map[string]any{"error": "AuthenticationRequired"})
		return false
	}
	mux.HandleFunc("POST /xrpc/com.atproto.server.createSession", func(w http.ResponseWriter, r *http.Request) {
		body := json_body(r)
		if body["identifier"] != f.did || body["password"] != "app-pass" {
			answer(w, 401, map[string]any{"error": "AuthenticationRequired", "message": "Invalid identifier or password"})
			return
		}
		f.lock.Lock()
		f.expired = false
		f.lock.Unlock()
		answer(w, 200, map[string]any{"accessJwt": "a1", "refreshJwt": "r1", "did": f.did, "handle": f.handle})
	})
	mux.HandleFunc("POST /xrpc/com.atproto.server.refreshSession", func(w http.ResponseWriter, r *http.Request) {
		f.lock.Lock()
		refused := f.refused
		f.lock.Unlock()
		if r.Header.Get("Authorization") != "Bearer r1" || refused {
			answer(w, 400, map[string]any{"error": "ExpiredToken"})
			return
		}
		f.lock.Lock()
		f.expired = false
		f.lock.Unlock()
		answer(w, 200, map[string]any{"accessJwt": "a2", "refreshJwt": "r2", "did": f.did, "handle": f.handle})
	})
	mux.HandleFunc("GET /xrpc/com.atproto.server.getSession", func(w http.ResponseWriter, r *http.Request) {
		if !authorized(w, r) {
			return
		}
		answer(w, 200, map[string]any{"did": f.did, "handle": f.handle})
	})
	mux.HandleFunc("GET /xrpc/com.atproto.repo.getRecord", func(w http.ResponseWriter, r *http.Request) {
		if !authorized(w, r) {
			return
		}
		if r.URL.Query().Get("collection") != "app.bsky.actor.profile" || r.URL.Query().Get("repo") != f.did {
			answer(w, 400, map[string]any{"error": "InvalidRequest"})
			return
		}
		answer(w, 200, map[string]any{"uri": "at://" + f.did + "/app.bsky.actor.profile/self", "value": map[string]any{"displayName": "Alice", "avatar": map[string]any{"ref": map[string]any{"$link": "bafyavatar"}}}})
	})
	mux.HandleFunc("POST /xrpc/com.atproto.repo.createRecord", func(w http.ResponseWriter, r *http.Request) {
		if !authorized(w, r) {
			return
		}
		body := json_body(r)
		record, _ := body["record"].(map[string]any)
		if body["repo"] != f.did || body["collection"] != "app.bsky.feed.post" || record["$type"] != "app.bsky.feed.post" || record["createdAt"] == nil {
			answer(w, 400, map[string]any{"error": "InvalidRequest", "message": fmt.Sprint(body)})
			return
		}
		f.lock.Lock()
		f.records = append(f.records, record)
		f.lock.Unlock()
		answer(w, 200, map[string]any{"uri": "at://" + f.did + "/app.bsky.feed.post/3k", "cid": "bafy1"})
	})
	mux.HandleFunc("POST /xrpc/com.atproto.repo.putRecord", func(w http.ResponseWriter, r *http.Request) {
		if !authorized(w, r) {
			return
		}
		body := json_body(r)
		if body["repo"] != f.did || body["collection"] != "app.bsky.feed.post" || body["rkey"] != "3k" {
			answer(w, 400, map[string]any{"error": "InvalidRequest"})
			return
		}
		answer(w, 200, map[string]any{"uri": "at://" + f.did + "/app.bsky.feed.post/3k", "cid": "bafy2"})
	})
	mux.HandleFunc("POST /xrpc/com.atproto.repo.deleteRecord", func(w http.ResponseWriter, r *http.Request) {
		if !authorized(w, r) {
			return
		}
		answer(w, 200, map[string]any{})
	})
	mux.HandleFunc("GET /xrpc/app.bsky.feed.getTimeline", func(w http.ResponseWriter, r *http.Request) {
		if !authorized(w, r) {
			return
		}
		if r.URL.Query().Get("cursor") == "" {
			answer(w, 200, map[string]any{"feed": []any{map[string]any{"post": map[string]any{"uri": "at://x/app.bsky.feed.post/1"}}}, "cursor": "c1"})
			return
		}
		answer(w, 200, map[string]any{"feed": []any{}})
	})
	mux.HandleFunc("GET /xrpc/app.bsky.feed.getAuthorFeed", func(w http.ResponseWriter, r *http.Request) {
		if !authorized(w, r) {
			return
		}
		if r.URL.Query().Get("actor") != f.did {
			answer(w, 400, map[string]any{"error": "InvalidRequest"})
			return
		}
		answer(w, 200, map[string]any{"feed": []any{map[string]any{"post": map[string]any{"uri": "at://" + f.did + "/app.bsky.feed.post/3k"}}}})
	})
	mux.HandleFunc("GET /xrpc/app.bsky.notification.listNotifications", func(w http.ResponseWriter, r *http.Request) {
		if !authorized(w, r) {
			return
		}
		answer(w, 200, map[string]any{"notifications": []any{map[string]any{"reason": "like"}}, "cursor": "n1"})
	})
	mux.HandleFunc("POST /xrpc/com.atproto.repo.uploadBlob", func(w http.ResponseWriter, r *http.Request) {
		if !authorized(w, r) {
			return
		}
		content, _ := io.ReadAll(r.Body)
		if r.Header.Get("Content-Type") != "image/png" || string(content) != "PNG" {
			answer(w, 400, map[string]any{"error": "InvalidRequest"})
			return
		}
		answer(w, 200, map[string]any{"blob": map[string]any{"$type": "blob", "ref": map[string]any{"$link": "bafyblob"}, "mimeType": "image/png", "size": 3}})
	})
	mux.HandleFunc("GET /xrpc/app.bsky.graph.getFollows", func(w http.ResponseWriter, r *http.Request) {
		if !authorized(w, r) {
			return
		}
		answer(w, 200, map[string]any{"follows": []any{map[string]any{"did": "did:plc:bob"}}})
	})
	f.server = httptest.NewServer(mux)
	t.Cleanup(f.server.Close)
	f.handle = fake_host(f.server)
	f.did = "did:web:" + strings.ReplaceAll(f.handle, ":", "%3A")
	return f
}

// bluesky_test_setup makes the fake, turns DNS off and points identities at it.
func bluesky_test_setup(t *testing.T) (*User, *bluesky_fake) {
	t.Helper()
	alice := create_linked_test_env(t)
	fake := create_bluesky_fake(t)
	saved := bluesky_lookup
	bluesky_lookup = func(ctx context.Context, name string) ([]string, error) { return nil, fmt.Errorf("no DNS in tests") }
	t.Cleanup(func() { bluesky_lookup = saved })
	return alice, fake
}

// TestBlueskyIdentity: a handle resolves through its well-known document or a
// DNS record to a DID whose document names the hosting server; a document for
// another identity, or without a server, is refused.
func TestBlueskyIdentity(t *testing.T) {
	alice, fake := bluesky_test_setup(t)
	identity, err := bluesky_resolve(context.Background(), alice, "@"+strings.ToUpper(fake.handle))
	if err != nil || identity.DID != fake.did || identity.PDS != fake.server.URL || identity.Handle != fake.handle {
		t.Fatalf("resolved %+v, %v", identity, err)
	}
	bluesky_lookup = func(ctx context.Context, name string) ([]string, error) {
		if name != "_atproto."+fake.handle {
			return nil, fmt.Errorf("unknown name")
		}
		return []string{"did=" + fake.did}, nil
	}
	if identity, err := bluesky_resolve(context.Background(), alice, fake.handle); err != nil || identity.DID != fake.did {
		t.Errorf("resolved through DNS: %+v, %v", identity, err)
	}
	if _, err := bluesky_resolve(context.Background(), alice, "did:web:127.0.0.1%3A1"); err == nil {
		t.Error("an identity nothing serves resolved")
	}
	if _, err := bluesky_resolve(context.Background(), alice, "not a handle"); err == nil {
		t.Error("a malformed handle resolved")
	}
	fake.lock.Lock()
	fake.did = "did:web:other.example"
	fake.lock.Unlock()
	if _, err := bluesky_resolve(context.Background(), alice, "did:web:"+strings.ReplaceAll(fake.handle, ":", "%3A")); err == nil {
		t.Error("a document for another identity was accepted")
	}
	block_add(db_user(alice, "user"), "127.0.0.1")
	if _, err := bluesky_resolve(context.Background(), alice, fake.handle); err == nil || !strings.Contains(err.Error(), "blocked") {
		t.Errorf("blocked host: %v", err)
	}
}

// TestBlueskyPassword: without a configured domain a Bluesky account links by
// app password; the session is kept with the password, a post goes out as a
// record with its page named, the feeds and notifications read, a blob
// uploads, and an expired session is refreshed, or made again with the
// password when the refresh is refused.
func TestBlueskyPassword(t *testing.T) {
	alice, fake := bluesky_test_setup(t)
	server_domain_name = ""
	thread := linked_thread(alice)
	fn := sl.NewBuiltin("test", nil)

	if _, err := api_account_link(thread, fn, sl.Tuple{sl.String("bluesky")}, linked_kwargs("handle", fake.handle, "password", "wrong")); !refused(err, "errors.link_failed", "did not accept") {
		t.Errorf("wrong password: %v", err)
	}
	if _, err := api_account_link(thread, fn, sl.Tuple{sl.String("bluesky")}, linked_kwargs("handle", fake.handle)); err == nil || !strings.Contains(err.Error(), "password") {
		t.Errorf("no password without a domain: %v", err)
	}
	account := decoded(t)(api_account_link(thread, fn, sl.Tuple{sl.String("bluesky")}, linked_kwargs("handle", fake.handle, "password", "app-pass")))
	id := account["id"].(string)
	if account["identifier"] != fake.handle || account["label"] != "Alice" || account["hosted"] != false || account["url"] != "https://bsky.app/profile/"+fake.did || account["avatar"] != fake.server.URL+"/xrpc/com.atproto.sync.getBlob?did="+url.QueryEscape(fake.did)+"&cid=bafyavatar" {
		t.Fatalf("linked account = %v", account)
	}
	l := linked_get(alice, id)
	if l.get("token") != "a1" || l.get("refresh") != "r1" || l.get("password") != "app-pass" || l.get("did") != fake.did || l.get("pds") != fake.server.URL || l.data["oauth"] != nil {
		t.Fatalf("stored data = %v", l.data)
	}

	created := decoded(t)(api_account_object_create(thread, fn, sl.Tuple{sl.String(id), object_dict("text", "Hello")}, linked_kwargs("key", "p1")))
	if created["id"] != "at://"+fake.did+"/app.bsky.feed.post/3k" || created["url"] != "https://bsky.app/profile/"+fake.did+"/post/3k" || created["data"].(map[string]any)["cid"] != "bafy1" {
		t.Errorf("create = %v", created)
	}
	decoded(t)(api_account_object_create(thread, fn, sl.Tuple{sl.String(id), object_dict("text", "Hello")}, linked_kwargs("key", "p1")))
	fake.lock.Lock()
	records := len(fake.records)
	fake.lock.Unlock()
	if records != 1 {
		t.Errorf("%d records for one key, want 1", records)
	}
	updated := decoded(t)(api_account_object_update(thread, fn, sl.Tuple{sl.String(id), sl.String(created["id"].(string)), object_dict("text", "Edited")}, nil))
	if updated["data"].(map[string]any)["cid"] != "bafy2" {
		t.Errorf("update = %v", updated)
	}
	if value, err := api_account_object_update(thread, fn, sl.Tuple{sl.String(id), sl.String("at://did:plc:other/app.bsky.feed.post/1"), object_dict("text", "x")}, nil); err != nil || value != sl.None {
		t.Errorf("another account's record was updated: %v, %v", value, err)
	}
	if _, err := api_account_object_delete(thread, fn, sl.Tuple{sl.String(id), sl.String(created["id"].(string))}, nil); err != nil {
		t.Errorf("delete: %v", err)
	}
	timeline := decoded(t)(api_account_timeline(thread, fn, sl.Tuple{sl.String(id)}, nil))
	if len(timeline["items"].([]any)) != 1 || timeline["cursor"] != "c1" {
		t.Errorf("timeline = %v", timeline)
	}
	if posts := decoded(t)(api_account_posts(thread, fn, sl.Tuple{sl.String(id)}, nil)); len(posts["items"].([]any)) != 1 || posts["cursor"] != "" {
		t.Errorf("posts = %v", posts)
	}
	if notifications := decoded(t)(api_account_notifications(thread, fn, sl.Tuple{sl.String(id)}, nil)); len(notifications["items"].([]any)) != 1 || notifications["cursor"] != "n1" {
		t.Errorf("notifications = %v", notifications)
	}
	upload := decoded(t)(api_account_upload(thread, fn, sl.Tuple{sl.String(id), sl.Bytes("PNG"), sl.String("image/png")}, nil))
	if upload["id"] != "bafyblob" || upload["data"].(map[string]any)["mimeType"] != "image/png" {
		t.Errorf("upload = %v", upload)
	}
	requested := decoded(t)(api_account_request(thread, fn, sl.Tuple{sl.String(id), sl.String("GET"), sl.String("app.bsky.graph.getFollows")}, nil))
	if data, _ := requested["data"].(map[string]any); data == nil || len(data["follows"].([]any)) != 1 {
		t.Errorf("request = %v", requested)
	}
	if _, err := api_account_request(thread, fn, sl.Tuple{sl.String(id), sl.String("GET"), sl.String("xrpc/app.bsky.graph.getFollows")}, nil); err == nil {
		t.Error("a path with a slash was accepted as a method name")
	}

	// The session expires: a refresh renews it and the request goes through.
	fake.lock.Lock()
	fake.expired = true
	fake.lock.Unlock()
	if tested := decoded(t)(api_account_test(thread, fn, sl.Tuple{sl.String(id)}, nil)); tested["success"] != true {
		t.Errorf("test across an expiry = %v", tested)
	}
	if l := linked_get(alice, id); l.get("token") != "a2" || l.get("refresh") != "r2" {
		t.Errorf("renewed data = %v", l.data)
	}
	// It expires again and the refresh is refused: the password signs in again.
	fake.lock.Lock()
	fake.expired = true
	fake.refused = true
	fake.lock.Unlock()
	db_user(alice, "user").account_set(id, map[string]any{"data": json_encode(map[string]any{"did": fake.did, "handle": fake.handle, "pds": fake.server.URL, "server": fake.handle, "token": "a1", "refresh": "r2", "password": "app-pass"})})
	if tested := decoded(t)(api_account_test(thread, fn, sl.Tuple{sl.String(id)}, nil)); tested["success"] != true {
		t.Errorf("test across a refused refresh = %v", tested)
	}
	if l := linked_get(alice, id); l.get("token") != "a1" || l.get("refresh") != "r1" {
		t.Errorf("re-signed data = %v", l.data)
	}
}

// TestBlueskyOAuth: with a configured domain a Bluesky account links by the
// protocol's OAuth - the request pushed with the client assertion and a DPoP
// proof, taking up the nonce the server demands, the consent at the
// authorization server, the tokens bound to the session's key and checked
// against the account resolved - and every request proves possession of the
// key with the token's hash, renews an expired token with the refresh token,
// and the session is revoked when the account is unlinked.
func TestBlueskyOAuth(t *testing.T) {
	alice, fake := bluesky_test_setup(t)
	thread := linked_thread(alice)
	fn := sl.NewBuiltin("test", nil)
	session := login_create(alice.UID, "", "")

	begun := decoded(t)(api_account_link(thread, fn, sl.Tuple{sl.String("bluesky")}, linked_kwargs("handle", fake.handle, "target", "/settings/networks")))
	fake.lock.Lock()
	pushes := fake.pushes
	fake.lock.Unlock()
	if pushes != 2 {
		t.Errorf("%d pushes, want 2: one refused for the nonce, one taken", pushes)
	}
	consent := oauth_hop(t, "bluesky", begun["url"].(string), session)
	if consent.Path != "/authorize" || consent.Query().Get("client_id") != "https://example.com/oauth/client-metadata.json" || consent.Query().Get("request_uri") != "urn:ietf:params:oauth:request_uri:req1" {
		t.Fatalf("consent = %v", consent)
	}
	_, st := oauth_only_ceremony(t)
	if st.Server != fake.server.URL || st.Identifier != fake.did || st.Issuer != fake.server.URL || st.Key == "" || st.Redirect != "https://example.com/_/auth/oauth/bluesky/callback" {
		t.Fatalf("ceremony = %+v", st)
	}
	state := consent.Query().Get("state")
	if state == "" {
		state, _ = oauth_only_ceremony(t)
	}

	// The callback must come from the issuer the request went to.
	wrong := decoded(t)(api_account_link(thread, fn, sl.Tuple{sl.String("bluesky")}, linked_kwargs("handle", fake.handle, "target", "/settings/networks")))
	wrong_state := oauth_hop(t, "bluesky", wrong["url"].(string), session).Query().Get("state")
	if wrong_state == "" {
		rows, _ := db_open("db/sessions.db").rows("select id from ceremonies where type='oauth' and id<>?", state)
		wrong_state = row_string(rows[0], "id")
	}
	if _, location := link_return("bluesky", "state="+wrong_state+"&code=code-1&iss=https://evil.example", session); !strings.Contains(location, "link_error=provider_error") {
		t.Errorf("callback from another issuer: %q", location)
	}

	code, location := link_return("bluesky", "state="+state+"&code=code-1&iss="+url.QueryEscape(fake.server.URL), session)
	if code != http.StatusFound || !strings.HasPrefix(location, "/settings/networks?linked=") {
		t.Fatalf("callback: %d %q", code, location)
	}
	id := strings.TrimPrefix(location, "/settings/networks?linked=")
	l := linked_get(alice, id)
	if l == nil || l.data["oauth"] != true || l.get("token") != "at1" || l.get("refresh") != "rt1" || l.get("key") == "" || l.get("issuer") != fake.server.URL || l.get("password") != "" || l.label != "Alice" {
		t.Fatalf("linked account = %v", l)
	}

	// A request proves possession, takes up the resource server's nonce, and
	// an expired token is renewed.
	if tested := decoded(t)(api_account_test(thread, fn, sl.Tuple{sl.String(id)}, nil)); tested["success"] != true {
		t.Errorf("test = %v", tested)
	}
	created := decoded(t)(api_account_object_create(thread, fn, sl.Tuple{sl.String(id), object_dict("text", "Hello")}, nil))
	if created["url"] != "https://bsky.app/profile/"+fake.did+"/post/3k" {
		t.Errorf("create = %v", created)
	}
	fake.lock.Lock()
	fake.expired = true
	fake.lock.Unlock()
	if tested := decoded(t)(api_account_test(thread, fn, sl.Tuple{sl.String(id)}, nil)); tested["success"] != true {
		t.Errorf("test across an expiry = %v", tested)
	}
	if l := linked_get(alice, id); l.get("token") != "at2" || l.get("refresh") != "rt2" {
		t.Errorf("renewed data = %v", l.data)
	}

	if value, err := api_account_remove(thread, fn, sl.Tuple{sl.String(id)}, nil); err != nil || value != sl.True {
		t.Fatalf("remove = %v, %v", value, err)
	}
	fake.lock.Lock()
	revokes := fake.revokes
	fake.lock.Unlock()
	if revokes != 1 {
		t.Errorf("%d revocations, want 1", revokes)
	}
}

// TestBlueskyClientDocument: the client document and the keys are served on a
// server with a configured domain while the development switch is on, naming
// the redirect the ceremony uses; without either they are not found.
func TestBlueskyClientDocument(t *testing.T) {
	create_linked_test_env(t)
	e := bluesky_client_engine()
	fetch := func(path string) (int, map[string]any) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("GET", path, nil)
		e.handler(c)
		var body map[string]any
		json_decode(w.Body.Bytes(), &body)
		return w.Code, body
	}
	status, document := fetch("/oauth/client-metadata.json")
	if status != 200 || document["client_id"] != "https://example.com/oauth/client-metadata.json" || document["token_endpoint_auth_method"] != "private_key_jwt" || document["dpop_bound_access_tokens"] != true || document["jwks_uri"] != "https://example.com/oauth/jwks.json" {
		t.Errorf("client document = %d %v", status, document)
	}
	if redirects, _ := document["redirect_uris"].([]any); len(redirects) != 1 || redirects[0] != "https://example.com/_/auth/oauth/bluesky/callback" {
		t.Errorf("redirects = %v", document["redirect_uris"])
	}
	status, keys := fetch("/oauth/jwks.json")
	list, _ := keys["keys"].([]any)
	if status != 200 || len(list) != 1 {
		t.Fatalf("jwks = %d %v", status, keys)
	}
	key := list[0].(map[string]any)
	client, _ := bluesky_client_key()
	if key["kty"] != "EC" || key["crv"] != "P-256" || key["kid"] != bluesky_kid(&client.PublicKey) || key["alg"] != "ES256" {
		t.Errorf("key = %v", key)
	}
	if again, _ := bluesky_client_key(); again.D.Cmp(client.D) != 0 {
		t.Error("the client key changed between reads")
	}
	server_domain_name = ""
	if status, _ := fetch("/oauth/client-metadata.json"); status != 404 {
		t.Errorf("without a domain: %d", status)
	}
	server_domain_name = "Example.com"
	networks_development = false
	if status, _ := fetch("/oauth/jwks.json"); status != 404 {
		t.Errorf("with the switch off: %d", status)
	}
	networks_development = true
}

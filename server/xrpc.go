// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: Bluesky clients
//
// What an AT Protocol app does with a hosted Bluesky account, as its data
// server (the federation plan, phase 28): it signs in - by OAuth, or the old
// way with a password Mochi minted for it - writes, changes and deletes
// records of any type in the account's repository, uploads blobs, keeps its
// preferences here, and has everything else it asks of the network - the
// index, the chat service, the labellers - forwarded with a token signed as
// the account. A post record it writes is mirrored into the account's home
// feed, a like or a repost of a post a feed holds is that post's reaction or
// boost, and a follow is a source of the home feed (decision 42), through
// the functions the feeds app declares (atproto/written). An account moves
// here from another server, and away again, by the protocol's own steps.

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/atproto/atdata"
	atrepo "github.com/bluesky-social/indigo/atproto/repo"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/gin-gonic/gin"
	"github.com/ipfs/go-cid"
)

// xrpc_access_lifetime and xrpc_refresh_lifetime bound an old-style
// session's tokens.
const xrpc_access_lifetime = 2 * 3600
const xrpc_refresh_lifetime = 90 * 86400

// xrpc_record_maximum bounds a record a client writes.
const xrpc_record_maximum = 1 << 20

// xrpc_import_maximum bounds a repository a client imports.
const xrpc_import_maximum = 512 << 20

// xrpc_commit_maximum is the most operations the stream carries in one
// commit.
const xrpc_commit_maximum = 200

// === Errors ===

// xrpc_fail answers an XRPC error with its message.
func xrpc_fail(c *gin.Context, status int, name string, message string) {
	c.JSON(status, map[string]any{"error": name, "message": message}) // i18n-ok: XRPC errors are protocol, read by clients
}

// === Sessions ===

// An xrpc_session is a request of an app's signed in to a hosted Bluesky
// account.
type xrpc_session struct {
	token *Token
	user  *User
	row   map[string]any
	id    string
	did   string
}

// xrpc_jwt is a token in the shape clients read a session's tokens in: a
// JWT naming the account, what it may do and when it expires. The server
// knows it by its hash, as any other token, so its signature is only
// shape.
func xrpc_jwt(did string, scope string, audience string, expires int64) string {
	encode := func(v map[string]any) string {
		return base64.RawURLEncoding.EncodeToString([]byte(json_encode(v)))
	}
	signature := make([]byte, 32)
	rand.Read(signature)
	return encode(map[string]any{"typ": "at+jwt", "alg": "HS256"}) + "." + encode(map[string]any{"scope": scope, "sub": did, "aud": audience, "iat": now(), "exp": expires, "jti": uid()}) + "." + base64.RawURLEncoding.EncodeToString(signature)
}

// xrpc_mint mints an old-style session for an account: an access token that
// expires within hours and the refresh token that renews it, both kept as
// any sign-in's tokens are.
func xrpc_mint(c *gin.Context, user *User, row map[string]any, name string) (string, string) {
	id, _ := row["id"].(string)
	did := atproto_did(row)
	audience := "did:web:" + activitypub_host(c)
	access := xrpc_jwt(did, "com.atproto.access", audience, now()+xrpc_access_lifetime)
	refresh := xrpc_jwt(did, "com.atproto.refresh", audience, now()+xrpc_refresh_lifetime)
	db_open("db/users.db").exec("insert into tokens (hash, user, app, name, scopes, action, entity, created, expires, kind, client, account, network, jkt, refresh, device) values (?, ?, '', ?, ?, '', '', ?, ?, ?, '', ?, 'bluesky', '', ?, '')",
		token_hash(access), user.UID, name, json_encode([]string{"atproto", "transition:generic", "transition:chat.bsky"}), now(), now()+xrpc_access_lifetime, token_signin, id, token_hash(refresh))
	db_open("db/sessions.db").exec("insert into accesses (hash, user, used) values (?, ?, ?)", token_hash(access), user.UID, now())
	return access, refresh
}

// xrpc_bearer is the token a request carries, and its scheme.
func xrpc_bearer(c *gin.Context) (string, string) {
	scheme, raw, _ := strings.Cut(c.GetHeader("Authorization"), " ")
	return strings.TrimSpace(scheme), strings.TrimSpace(raw)
}

// xrpc_token is the token a request carries: one OAuth gave, or an old-style
// session's, which is shaped as a JWT rather than as Mochi's tokens are and
// so is found by its hash alone.
func xrpc_token(raw string) *Token {
	if t := token_lookup(raw); t != nil {
		return t
	}
	if strings.Count(raw, ".") != 2 {
		return nil
	}
	var t Token
	if !db_open("db/users.db").scan(&t, "select hash, user, app, name, scopes, action, entity, created, expires, kind, client, account, network, jkt, refresh, device, scram from tokens where hash=? and network='bluesky' and client=''", token_hash(raw)) {
		return nil
	}
	if t.Expires > 0 && now() > t.Expires {
		return nil
	}
	json.Unmarshal([]byte(t.ScopesDB), &t.Scopes)
	return &t
}

// xrpc_authenticate checks a request's token is a Bluesky app's, signed in
// to a hosted account, with the scope to write where it writes, and writes
// the refusal. A token bound to a key needs its proof.
func xrpc_authenticate(c *gin.Context, writes bool) *xrpc_session {
	scheme, raw := xrpc_bearer(c)
	if raw == "" || (!strings.EqualFold(scheme, "Bearer") && !strings.EqualFold(scheme, "DPoP")) {
		xrpc_fail(c, http.StatusUnauthorized, "AuthenticationRequired", "Authentication Required")
		return nil
	}
	t := xrpc_token(raw)
	if t == nil {
		var held Token
		if db_open("db/users.db").scan(&held, "select hash, kind from tokens where hash=?", token_hash(raw)) && held.Kind == token_signin {
			xrpc_fail(c, http.StatusBadRequest, "ExpiredToken", "Token has expired")
			return nil
		}
		xrpc_fail(c, http.StatusUnauthorized, "InvalidToken", "Token could not be verified")
		return nil
	}
	if t.Kind != token_signin || t.Network != "bluesky" {
		xrpc_fail(c, http.StatusUnauthorized, "InvalidToken", "Token could not be verified")
		return nil
	}
	if t.JKT != "" {
		key, ok := signin_proof(c, raw, true)
		if !ok {
			return nil
		}
		if key != t.JKT {
			xrpc_fail(c, http.StatusUnauthorized, "InvalidToken", "The proof is not by the token's key")
			return nil
		}
	}
	if writes && !signin_scopes_check(t, "transition:generic") && !signin_scopes_check(t, "repo:*") {
		xrpc_fail(c, http.StatusForbidden, "InsufficientScope", "The token does not allow writing")
		return nil
	}
	user := user_by_uid(t.User)
	if user == nil {
		xrpc_fail(c, http.StatusUnauthorized, "InvalidToken", "Token could not be verified")
		return nil
	}
	row := account_hosted_get(user, t.Account)
	if row == nil || row["type"] != "bluesky" || atproto_did(row) == "" {
		xrpc_fail(c, http.StatusUnauthorized, "InvalidToken", "Token could not be verified")
		return nil
	}
	db_open("db/sessions.db").exec("insert into accesses (hash, user, used) values (?, ?, ?) on conflict(hash) do update set used=excluded.used", t.Hash, t.User, now())
	return &xrpc_session{token: t, user: user, row: row, id: t.Account, did: atproto_did(row)}
}

// xrpc_session_answer is what a session's sign-in and reads answer: the
// account, its identity document and its standing.
func xrpc_session_answer(c *gin.Context, user *User, row map[string]any) map[string]any {
	did := atproto_did(row)
	handle := row_string(row, "identifier")
	answer := map[string]any{"did": did, "handle": handle, "email": "", "emailConfirmed": true, "emailAuthFactor": false, "active": !atproto_deactivated(row)}
	if atproto_deactivated(row) {
		answer["status"] = "deactivated"
	}
	if document := xrpc_did_document(c, row); document != nil {
		answer["didDoc"] = document
	}
	return answer
}

// xrpc_did_document is the account's identity document as the directory
// would answer it, from its last operation.
func xrpc_did_document(c *gin.Context, row map[string]any) map[string]any {
	did := atproto_did(row)
	if !strings.HasPrefix(did, "did:plc:") {
		return nil
	}
	op, _, err := plc_last(c.Request.Context(), did)
	if err != nil {
		return nil
	}
	document := map[string]any{"@context": []string{"https://www.w3.org/ns/did/v1", "https://w3id.org/security/multikey/v1", "https://w3id.org/security/suites/secp256k1-2019/v1"}, "id": did, "alsoKnownAs": object_strings(op["alsoKnownAs"])}
	if methods, ok := op["verificationMethods"].(map[string]any); ok {
		if key, _ := methods["atproto"].(string); key != "" {
			document["verificationMethod"] = []any{map[string]any{"id": did + "#atproto", "type": "Multikey", "controller": did, "publicKeyMultibase": strings.TrimPrefix(key, "did:key:")}}
		}
	}
	if services, ok := op["services"].(map[string]any); ok {
		if pds, ok := services["atproto_pds"].(map[string]any); ok {
			document["service"] = []any{map[string]any{"id": "#atproto_pds", "type": "AtprotoPersonalDataServer", "serviceEndpoint": pds["endpoint"]}}
		}
	}
	return document
}

// xrpc_body reads a request's JSON body.
func xrpc_body(c *gin.Context, into any) bool {
	raw, err := io.ReadAll(io.LimitReader(c.Request.Body, xrpc_record_maximum+1))
	if err != nil || len(raw) > xrpc_record_maximum {
		xrpc_fail(c, http.StatusRequestEntityTooLarge, "PayloadTooLarge", "The request is too large")
		return false
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		raw = []byte("{}")
	}
	if err := json.Unmarshal(raw, into); err != nil {
		xrpc_fail(c, http.StatusBadRequest, "InvalidRequest", "The body is not JSON")
		return false
	}
	return true
}

// === Routing ===

// xrpc_dispatch answers what a signed-in app asks, reporting whether the
// request was one of its methods; the engine's own reads answer the rest.
func xrpc_dispatch(c *gin.Context, name string) bool {
	method := c.Request.Method
	switch method + " " + name {
	case "POST com.atproto.server.createSession":
		xrpc_create_session(c)
	case "POST com.atproto.server.refreshSession":
		xrpc_refresh_session(c)
	case "GET com.atproto.server.getSession":
		if s := xrpc_authenticate(c, false); s != nil {
			c.JSON(http.StatusOK, xrpc_session_answer(c, s.user, s.row))
		}
	case "POST com.atproto.server.deleteSession":
		_, raw := xrpc_bearer(c)
		if raw != "" {
			db_open("db/users.db").exec("delete from tokens where (hash=? or refresh=?) and network='bluesky' and kind=?", token_hash(raw), token_hash(raw), token_signin)
		}
		c.JSON(http.StatusOK, map[string]any{})
	case "GET com.atproto.server.getServiceAuth":
		xrpc_service_auth(c)
	case "GET com.atproto.server.checkAccountStatus":
		xrpc_account_status(c)
	case "POST com.atproto.server.activateAccount", "POST com.atproto.server.deactivateAccount":
		xrpc_activation(c, name == "com.atproto.server.activateAccount")
	case "POST com.atproto.server.createAccount":
		xrpc_create_account(c)
	case "GET com.atproto.server.listAppPasswords":
		c.JSON(http.StatusOK, map[string]any{"passwords": []any{}})
	case "POST com.atproto.server.createAppPassword", "POST com.atproto.server.requestEmailUpdate", "POST com.atproto.server.updateEmail", "POST com.atproto.server.requestAccountDelete", "POST com.atproto.server.deleteAccount":
		if s := xrpc_authenticate(c, true); s != nil {
			xrpc_fail(c, http.StatusBadRequest, "InvalidRequest", "This is done in Mochi")
		}
	case "POST com.atproto.identity.updateHandle":
		// A hosted account's handle is kept for good (the federation plan,
		// decision 13), changed only by moving the account elsewhere.
		if s := xrpc_authenticate(c, true); s != nil {
			xrpc_fail(c, http.StatusBadRequest, "InvalidRequest", "A handle on this server cannot be changed")
		}
	case "GET com.atproto.identity.getRecommendedDidCredentials":
		xrpc_recommended(c)
	case "POST com.atproto.identity.requestPlcOperationSignature":
		xrpc_plc_request(c)
	case "POST com.atproto.identity.signPlcOperation":
		xrpc_plc_sign(c)
	case "POST com.atproto.identity.submitPlcOperation":
		xrpc_plc_submit(c)
	case "POST com.atproto.repo.createRecord", "POST com.atproto.repo.putRecord", "POST com.atproto.repo.deleteRecord":
		xrpc_record(c, strings.TrimPrefix(name, "com.atproto.repo."))
	case "POST com.atproto.repo.applyWrites":
		xrpc_apply_writes(c)
	case "POST com.atproto.repo.uploadBlob":
		xrpc_upload_blob(c)
	case "POST com.atproto.repo.importRepo":
		xrpc_import(c)
	case "GET com.atproto.repo.listMissingBlobs":
		xrpc_missing_blobs(c)
	case "GET app.bsky.actor.getPreferences":
		if s := xrpc_authenticate(c, false); s != nil {
			preferences := []any{}
			if held := mastodon_snapshot_get(s.user, "preferences", s.id); held != nil {
				if list, ok := held["preferences"].([]any); ok {
					preferences = list
				}
			}
			c.JSON(http.StatusOK, map[string]any{"preferences": preferences})
		}
	case "POST app.bsky.actor.putPreferences":
		if s := xrpc_authenticate(c, true); s != nil {
			var body struct {
				Preferences []any `json:"preferences"`
			}
			if !xrpc_body(c, &body) {
				return true
			}
			if body.Preferences == nil {
				body.Preferences = []any{}
			}
			mastodon_snapshot(s.user, "preferences", s.id, map[string]any{"preferences": body.Preferences})
			c.JSON(http.StatusOK, map[string]any{})
		}
	default:
		// The network's methods are forwarded as the account: the index's,
		// the chat service's, a labeller's, whatever the app names.
		if name == "com.atproto.moderation.createReport" || strings.HasPrefix(name, "app.bsky.") || strings.HasPrefix(name, "chat.bsky.") || strings.HasPrefix(name, "tools.ozone.") || c.GetHeader("Atproto-Proxy") != "" {
			xrpc_forward(c, name)
			return true
		}
		return false
	}
	return true
}

// xrpc_create_session signs an app in the old way: the account's handle or
// identifier, and a password Mochi minted for it.
func xrpc_create_session(c *gin.Context) {
	var body struct {
		Identifier string `json:"identifier"`
		Password   string `json:"password"`
	}
	if !xrpc_body(c, &body) {
		return
	}
	user, row, name := password_check("bluesky", strings.TrimPrefix(body.Identifier, "@"), body.Password)
	if user == nil || row == nil || atproto_did(row) == "" {
		xrpc_fail(c, http.StatusUnauthorized, "AuthenticationRequired", "Invalid identifier or password")
		return
	}
	access, refresh := xrpc_mint(c, user, row, name)
	answer := xrpc_session_answer(c, user, row)
	answer["accessJwt"], answer["refreshJwt"] = access, refresh
	c.JSON(http.StatusOK, answer)
}

// xrpc_refresh_session renews an old-style session by its refresh token:
// the old tokens end.
func xrpc_refresh_session(c *gin.Context) {
	_, raw := xrpc_bearer(c)
	var t Token
	if raw == "" || !db_open("db/users.db").scan(&t, "select hash, user, name, account, created from tokens where refresh=? and kind=? and network='bluesky' and client=''", token_hash(raw), token_signin) {
		xrpc_fail(c, http.StatusBadRequest, "ExpiredToken", "Token has expired")
		return
	}
	token_delete(t.Hash)
	user := user_by_uid(t.User)
	row := account_hosted_get(user, t.Account)
	if user == nil || row == nil || now()-t.Created > xrpc_refresh_lifetime {
		xrpc_fail(c, http.StatusBadRequest, "ExpiredToken", "Token has expired")
		return
	}
	access, refresh := xrpc_mint(c, user, row, t.Name)
	answer := xrpc_session_answer(c, user, row)
	answer["accessJwt"], answer["refreshJwt"] = access, refresh
	c.JSON(http.StatusOK, answer)
}

// xrpc_service_auth answers a token signed as the account for another
// service: for a minute, or up to an hour where the app asks.
func xrpc_service_auth(c *gin.Context) {
	s := xrpc_authenticate(c, false)
	if s == nil {
		return
	}
	audience := c.Query("aud")
	if !strings.HasPrefix(audience, "did:") {
		xrpc_fail(c, http.StatusBadRequest, "InvalidRequest", "aud must be a DID")
		return
	}
	token, err := atproto_token(s.user, s.row, audience, c.Query("lxm"))
	if err != nil {
		xrpc_fail(c, http.StatusBadRequest, "InvalidRequest", err.Error())
		return
	}
	if exp, err := strconv.ParseInt(c.Query("exp"), 10, 64); err == nil && exp > now() {
		if exp > now()+3600 {
			xrpc_fail(c, http.StatusBadRequest, "BadExpiration", "The token may last an hour at most")
			return
		}
		token, err = xrpc_token_until(s, audience, c.Query("lxm"), exp)
		if err != nil {
			xrpc_fail(c, http.StatusBadRequest, "InvalidRequest", err.Error())
			return
		}
	}
	c.JSON(http.StatusOK, map[string]any{"token": token})
}

// xrpc_token_until is atproto_token lasting until a time.
func xrpc_token_until(s *xrpc_session, audience string, method string, expires int64) (string, error) {
	key := keypair_get(s.user, s.id, key_signing)
	if key == nil {
		return "", fmt.Errorf("account has no signing key")
	}
	encode := func(v map[string]any) string {
		return base64.RawURLEncoding.EncodeToString([]byte(json_encode(v)))
	}
	claims := map[string]any{"iss": s.did, "aud": audience, "exp": expires, "iat": now(), "jti": uid()}
	if method != "" {
		claims["lxm"] = method
	}
	data := encode(map[string]any{"typ": "JWT", "alg": "ES256"}) + "." + encode(claims)
	signature, err := key_sign_p256(key.Private, []byte(data))
	if err != nil {
		return "", err
	}
	return data + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}

// === Records ===

// xrpc_repository is the repository a request names, which must be the
// signed-in account's, and writes the refusal.
func xrpc_repository(c *gin.Context, s *xrpc_session, repo string) *repository {
	if repo != s.did && !strings.EqualFold(repo, row_string(s.row, "identifier")) {
		xrpc_fail(c, http.StatusBadRequest, "InvalidRequest", "The repository is not the account's")
		return nil
	}
	if atproto_deactivated(s.row) {
		xrpc_fail(c, http.StatusBadRequest, "AccountDeactivated", "The account is deactivated")
		return nil
	}
	r := repository_get(s.user, s.id)
	if r == nil {
		xrpc_fail(c, http.StatusBadRequest, "RepoNotFound", "The account has no repository")
		return nil
	}
	return r
}

// A xrpc_write is one write of a client's to a repository.
type xrpc_write struct {
	action     string
	collection string
	rkey       string
	record     map[string]any
}

// xrpc_check checks a write: its collection a valid name, its key a valid
// record key, its record of the collection's type.
func xrpc_check(w *xrpc_write) error {
	if _, err := syntax.ParseNSID(w.collection); err != nil {
		return fmt.Errorf("invalid collection %q", w.collection)
	}
	if w.action == "create" && w.rkey == "" {
		w.rkey = repository_clock.Next().String()
	}
	if _, err := syntax.ParseRecordKey(w.rkey); err != nil {
		return fmt.Errorf("invalid record key %q", w.rkey)
	}
	if w.action != "delete" {
		if w.record == nil {
			return fmt.Errorf("the record is missing")
		}
		kind, _ := w.record["$type"].(string)
		if kind == "" {
			w.record["$type"] = w.collection
		} else if kind != w.collection {
			return fmt.Errorf("the record's type %q is not its collection %q", kind, w.collection)
		}
	}
	return nil
}

// xrpc_apply makes writes in one commit, each told to the apps that mirror
// records, and answers each result.
func xrpc_apply(ctx context.Context, s *xrpc_session, r *repository, writes []*xrpc_write) ([]map[string]any, error) {
	db := db_user(s.user, "user")
	var ops []repository_operation
	var results []map[string]any
	var deleted []map[string]any
	for _, w := range writes {
		path := w.collection + "/" + w.rkey
		if w.action == "delete" {
			_, held := repository_record(s.user, s.id, w.collection, w.rkey)
			deleted = append(deleted, xrpc_plain(held))
			db.exec("delete from records where account=? and collection=? and rkey=?", s.id, w.collection, w.rkey)
			ops = append(ops, repository_operation{path: path})
			results = append(results, map[string]any{"$type": "com.atproto.repo.applyWrites#deleteResult"})
			continue
		}
		deleted = append(deleted, nil)
		_, data, c, err := repository_encode(w.record)
		if err != nil {
			return nil, err
		}
		db.exec("insert or ignore into nodes (account, cid, data) values (?, ?, ?)", s.id, c.String(), data)
		db.exec("insert into records (account, collection, rkey, cid, created, updated) values (?, ?, ?, ?, ?, 0) on conflict(account, collection, rkey) do update set cid=excluded.cid, updated=?", s.id, w.collection, w.rkey, c.String(), now(), now())
		cc := c
		ops = append(ops, repository_operation{path: path, cid: &cc, data: data})
		kind := "createResult"
		if w.action == "update" {
			kind = "updateResult"
		}
		results = append(results, map[string]any{"$type": "com.atproto.repo.applyWrites#" + kind, "uri": "at://" + s.did + "/" + path, "cid": c.String(), "validationStatus": "unknown"})
	}
	event, err := repository_commit(ctx, r, append(repository_waiting(r), ops...))
	if err != nil {
		return nil, err
	}
	for i, w := range writes {
		record := w.record
		if w.action == "delete" {
			record = deleted[i]
		}
		uri := "at://" + s.did + "/" + w.collection + "/" + w.rkey
		if w.collection == "app.bsky.graph.follow" {
			xrpc_following(s, w.action, uri, record)
		}
		xrpc_mirror(s, w.action, w.collection, w.rkey, uri, record)
		if event != nil {
			results[i]["commit"] = map[string]any{"cid": event.head, "rev": event.rev}
		}
	}
	return results, nil
}

// xrpc_record answers one write: createRecord, putRecord or deleteRecord.
func xrpc_record(c *gin.Context, kind string) {
	s := xrpc_authenticate(c, true)
	if s == nil {
		return
	}
	var body struct {
		Repo       string         `json:"repo"`
		Collection string         `json:"collection"`
		Rkey       string         `json:"rkey"`
		Record     map[string]any `json:"record"`
		SwapRecord *string        `json:"swapRecord"`
	}
	if !xrpc_body(c, &body) {
		return
	}
	r := xrpc_repository(c, s, body.Repo)
	if r == nil {
		return
	}
	w := &xrpc_write{collection: body.Collection, rkey: body.Rkey, record: body.Record}
	switch kind {
	case "createRecord":
		w.action = "create"
	case "putRecord":
		w.action = "update"
	case "deleteRecord":
		w.action = "delete"
		w.record = nil
	}
	if err := xrpc_check(w); err != nil {
		xrpc_fail(c, http.StatusBadRequest, "InvalidRequest", err.Error())
		return
	}
	held, _ := repository_record(s.user, s.id, w.collection, w.rkey)
	if body.SwapRecord != nil && *body.SwapRecord != held {
		xrpc_fail(c, http.StatusBadRequest, "InvalidSwap", "The record has changed")
		return
	}
	if w.action == "create" && held != "" {
		xrpc_fail(c, http.StatusBadRequest, "InvalidRequest", "The record exists")
		return
	}
	if w.action == "delete" && held == "" {
		c.JSON(http.StatusOK, map[string]any{})
		return
	}
	results, err := xrpc_apply(c.Request.Context(), s, r, []*xrpc_write{w})
	if err != nil {
		xrpc_fail(c, http.StatusBadRequest, "InvalidRequest", err.Error())
		return
	}
	answer := results[0]
	delete(answer, "$type")
	c.JSON(http.StatusOK, answer)
}

// xrpc_apply_writes answers several writes in one commit.
func xrpc_apply_writes(c *gin.Context) {
	s := xrpc_authenticate(c, true)
	if s == nil {
		return
	}
	var body struct {
		Repo   string           `json:"repo"`
		Writes []map[string]any `json:"writes"`
	}
	if !xrpc_body(c, &body) {
		return
	}
	r := xrpc_repository(c, s, body.Repo)
	if r == nil {
		return
	}
	if len(body.Writes) == 0 || len(body.Writes) > xrpc_commit_maximum {
		xrpc_fail(c, http.StatusBadRequest, "InvalidRequest", "Between 1 and 200 writes")
		return
	}
	var writes []*xrpc_write
	for _, raw := range body.Writes {
		kind, _ := raw["$type"].(string)
		w := &xrpc_write{action: strings.TrimPrefix(kind, "com.atproto.repo.applyWrites#")}
		w.collection, _ = raw["collection"].(string)
		w.rkey, _ = raw["rkey"].(string)
		w.record, _ = raw["value"].(map[string]any)
		if w.action != "create" && w.action != "update" && w.action != "delete" {
			xrpc_fail(c, http.StatusBadRequest, "InvalidRequest", "Unknown write "+kind)
			return
		}
		if w.action == "delete" {
			w.record = nil
		}
		if err := xrpc_check(w); err != nil {
			xrpc_fail(c, http.StatusBadRequest, "InvalidRequest", err.Error())
			return
		}
		writes = append(writes, w)
	}
	results, err := xrpc_apply(c.Request.Context(), s, r, writes)
	if err != nil {
		xrpc_fail(c, http.StatusBadRequest, "InvalidRequest", err.Error())
		return
	}
	answer := map[string]any{"results": results}
	if len(results) > 0 {
		answer["commit"] = results[0]["commit"]
	}
	c.JSON(http.StatusOK, answer)
}

// xrpc_upload_blob keeps a blob for the account.
func xrpc_upload_blob(c *gin.Context) {
	s := xrpc_authenticate(c, true)
	if s == nil {
		return
	}
	data, err := io.ReadAll(io.LimitReader(c.Request.Body, int64(repository_blob_maximum)+1))
	if err != nil || len(data) > repository_blob_maximum {
		xrpc_fail(c, http.StatusRequestEntityTooLarge, "BlobTooLarge", "The blob is too large")
		return
	}
	kind := c.ContentType()
	if kind == "" || kind == "*/*" {
		kind = http.DetectContentType(data)
	}
	uploaded, err := atproto_upload(s.user, s.row, data, kind)
	if err != nil {
		xrpc_fail(c, http.StatusBadRequest, "InvalidRequest", err.Error())
		return
	}
	c.JSON(http.StatusOK, map[string]any{"blob": uploaded["data"]})
}

// === Mirroring into feeds ===

// xrpc_mirror tells the apps that mirror records what a client wrote: a
// post kept in the home feed, a like or repost of a post a feed holds its
// reaction or boost, a follow a source of the home feed (decision 42), the
// profile's pinned post the post's pin. A post's images go with their bytes.
func xrpc_mirror(s *xrpc_session, action string, collection string, rkey string, uri string, record map[string]any) {
	switch collection {
	case "app.bsky.feed.post", "app.bsky.feed.like", "app.bsky.feed.repost", "app.bsky.graph.follow", "app.bsky.actor.profile":
	default:
		return
	}
	home, _ := account_data(s.row)["home"].(string)
	var media []any
	if collection == "app.bsky.feed.post" && action != "delete" {
		media = xrpc_record_media(s, record)
	}
	// One app keeps the posts, as for a Mastodon app: a development copy of
	// it beside the installed one would mirror each record twice.
	a := mastodon_app(s.user, "atproto/written")
	if a == nil {
		return
	}
	if _, err := xrpc_caller(s, a, "atproto/written", Map{"account": s.id, "home": home, "action": action, "collection": collection, "rkey": rkey, "uri": uri, "record": record, "media": media}); err != nil {
		debug("Bluesky account %s: %s of %s not mirrored by %s: %v", s.id, action, uri, a.id, err)
	}
}

// xrpc_following keeps the account's following rows, which the apps read, in
// step with the follow records an app writes.
func xrpc_following(s *xrpc_session, action string, uri string, record map[string]any) {
	did, _ := record["subject"].(string)
	if !strings.HasPrefix(did, "did:") {
		return
	}
	db := db_user(s.user, "user")
	if action == "delete" {
		db.exec("delete from following where account=? and actor=? and activity=?", s.id, did, uri)
		return
	}
	db.exec("insert into following (account, actor, inbox, activity, accepted, created) values (?, ?, '', ?, ?, ?) on conflict(account, actor) do update set activity=excluded.activity, accepted=excluded.accepted", s.id, did, uri, now(), now())
}

// xrpc_caller runs an app's function as the user; a test stands its own in.
var xrpc_caller = func(s *xrpc_session, a *App, function string, args Map) (any, error) {
	return app_call_as_server(s.user, a, function, args)
}

// xrpc_record_media is a post record's images, each with its bytes from the
// account's blobs and its description.
func xrpc_record_media(s *xrpc_session, record map[string]any) []any {
	embed, _ := record["embed"].(map[string]any)
	if kind, _ := embed["$type"].(string); kind == "app.bsky.embed.recordWithMedia" {
		embed, _ = embed["media"].(map[string]any)
	}
	images, _ := embed["images"].([]any)
	var media []any
	for _, raw := range images {
		image, _ := raw.(map[string]any)
		blob, _ := image["image"].(map[string]any)
		id := bluesky_blob_cid(blob)
		if id == "" {
			continue
		}
		kind, data, ok := repository_blob(s.user, s.id, id)
		if !ok {
			continue
		}
		alt, _ := image["alt"].(string)
		media = append(media, map[string]any{"name": id + xrpc_extension(kind), "type": kind, "description": alt, "data": data})
	}
	return media
}

// xrpc_extension is a file name's ending for a media type.
func xrpc_extension(kind string) string {
	switch kind {
	case "image/jpeg":
		return ".jpg"
	case "image/png":
		return ".png"
	case "image/gif":
		return ".gif"
	case "image/webp":
		return ".webp"
	case "video/mp4":
		return ".mp4"
	}
	return ""
}

// === Forwarding ===

// xrpc_services are the services a method goes to when the app names none.
var xrpc_services = map[string]string{
	"app.bsky.":                           "did:web:api.bsky.app#bsky_appview",
	"chat.bsky.":                          "did:web:api.bsky.chat#bsky_chat",
	"com.atproto.moderation.createReport": "did:plc:ar7c4by46qjdydhdevvrndac#atproto_labeler",
}

// xrpc_service_endpoint is where a service is reached: the index this
// server is set to for Bluesky's own, a did:web's host, or the endpoint a
// did:plc's document names for the service.
func xrpc_service_endpoint(ctx context.Context, did string, service string) (string, error) {
	if did == bluesky_appview_did {
		return strings.TrimSuffix(bluesky_appview, "/"), nil
	}
	if host, ok := strings.CutPrefix(did, "did:web:"); ok {
		if strings.ContainsAny(host, "/:%") {
			return "", fmt.Errorf("invalid service %s", did)
		}
		return "https://" + host, nil
	}
	if strings.HasPrefix(did, "did:plc:") {
		op, _, err := plc_last(ctx, did)
		if err != nil {
			return "", err
		}
		if services, ok := op["services"].(map[string]any); ok {
			if entry, ok := services[service].(map[string]any); ok {
				if endpoint, _ := entry["endpoint"].(string); strings.HasPrefix(endpoint, "https://") {
					return strings.TrimSuffix(endpoint, "/"), nil
				}
			}
		}
		return "", fmt.Errorf("%s names no %s", did, service)
	}
	return "", fmt.Errorf("unknown service %s", did)
}

// xrpc_forward sends a method an app asks of the network to the service it
// names, or the method's own, with a token signed as the account, and
// answers what the service answers.
func xrpc_forward(c *gin.Context, name string) {
	s := xrpc_authenticate(c, c.Request.Method == "POST")
	if s == nil {
		return
	}
	proxy := c.GetHeader("Atproto-Proxy")
	if proxy == "" {
		for prefix, service := range xrpc_services {
			if strings.HasPrefix(name, prefix) {
				proxy = service
			}
		}
	}
	did, service, found := strings.Cut(proxy, "#")
	if !found || !strings.HasPrefix(did, "did:") || service == "" {
		xrpc_fail(c, http.StatusBadRequest, "InvalidRequest", "No service to forward the method to")
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), network_timeout)
	defer cancel()
	endpoint, err := xrpc_service_endpoint(ctx, did, service)
	if err != nil {
		xrpc_fail(c, http.StatusBadRequest, "InvalidRequest", err.Error())
		return
	}
	if host_allowed(s.user, "bluesky", identifier_host(endpoint)) != nil {
		xrpc_fail(c, http.StatusBadRequest, "InvalidRequest", "The service is blocked")
		return
	}
	token, err := atproto_token(s.user, s.row, did, name)
	if err != nil {
		xrpc_fail(c, http.StatusBadRequest, "InvalidRequest", err.Error())
		return
	}
	address := endpoint + "/xrpc/" + url.PathEscape(name)
	if c.Request.URL.RawQuery != "" {
		address += "?" + c.Request.URL.RawQuery
	}
	var body io.Reader = http.NoBody
	if c.Request.Method == "POST" {
		data, err := io.ReadAll(io.LimitReader(c.Request.Body, int64(repository_blob_maximum)+1))
		if err != nil || len(data) > repository_blob_maximum {
			xrpc_fail(c, http.StatusRequestEntityTooLarge, "PayloadTooLarge", "The request is too large")
			return
		}
		body = bytes.NewReader(data)
	}
	request, err := http.NewRequestWithContext(ctx, c.Request.Method, address, body)
	if err != nil {
		xrpc_fail(c, http.StatusBadRequest, "InvalidRequest", err.Error())
		return
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Accept", "application/json")
	if accept := c.GetHeader("Accept"); accept != "" {
		request.Header.Set("Accept", accept)
	}
	for _, header := range []string{"Content-Type", "Accept-Language", "Atproto-Accept-Labelers"} {
		if value := c.GetHeader(header); value != "" {
			request.Header.Set(header, value)
		}
	}
	if request.Header.Get("Atproto-Accept-Labelers") == "" {
		request.Header.Set("Atproto-Accept-Labelers", appview_labelers(s.row))
	}
	response, err := network_send(ctx, s.user, "bluesky", strings.ToLower(request.URL.Hostname())+"|"+s.id, request, 3)
	if err != nil {
		xrpc_fail(c, http.StatusBadGateway, "UpstreamFailure", "The service could not be reached")
		return
	}
	defer response.Body.Close()
	data, err := read_limited(response.Body, linked_body_maximum)
	if err != nil {
		xrpc_fail(c, http.StatusBadGateway, "UpstreamFailure", "The service's answer could not be read")
		return
	}
	for _, header := range []string{"Atproto-Content-Labelers", "Content-Language"} {
		if value := response.Header.Get(header); value != "" {
			c.Header(header, value)
		}
	}
	kind := response.Header.Get("Content-Type")
	if kind == "" {
		kind = "application/json"
	}
	c.Data(response.StatusCode, kind, data)
}

// === Moving accounts ===

// xrpc_account_status answers how far an account's repository and blobs
// are, as a client moving it checks.
func xrpc_account_status(c *gin.Context) {
	s := xrpc_authenticate(c, false)
	if s == nil {
		return
	}
	db := db_user(s.user, "user")
	count := func(sql string) int64 {
		row, _ := db.row(sql, s.id)
		return row_int(row, "n")
	}
	r := repository_get(s.user, s.id)
	head, rev := "", ""
	if r != nil {
		head, rev = r.head, r.rev
	}
	records := count("select count(*) as n from records where account=?")
	blobs := count("select count(*) as n from blobs where account=?")
	expected := int64(len(xrpc_referenced(s)))
	c.JSON(http.StatusOK, map[string]any{
		"activated":          !atproto_deactivated(s.row),
		"validDid":           strings.HasPrefix(s.did, "did:plc:"),
		"repoCommit":         head,
		"repoRev":            rev,
		"repoBlocks":         count("select count(*) as n from nodes where account=?"),
		"indexedRecords":     records,
		"privateStateValues": 0,
		"expectedBlobs":      max(expected, blobs),
		"importedBlobs":      blobs,
	})
}

// xrpc_referenced is every blob the account's records name.
func xrpc_referenced(s *xrpc_session) map[string]string {
	db := db_user(s.user, "user")
	rows, _ := db.rows("select r.collection, r.rkey, n.data from records r join nodes n on n.account=r.account and n.cid=r.cid where r.account=?", s.id)
	out := map[string]string{}
	for _, row := range rows {
		uri := "at://" + s.did + "/" + row_string(row, "collection") + "/" + row_string(row, "rkey")
		record, err := atdata.UnmarshalCBOR(repository_bytes(row["data"]))
		if err != nil {
			continue
		}
		xrpc_blobs_in(xrpc_plain(record), func(id string) { out[id] = uri })
	}
	return out
}

// xrpc_plain is a record read from the repository in the form a client
// writes it: blobs and links as their JSON objects.
func xrpc_plain(record map[string]any) map[string]any {
	if record == nil {
		return nil
	}
	data, err := json.Marshal(record)
	if err != nil {
		return nil
	}
	var plain map[string]any
	if json.Unmarshal(data, &plain) != nil {
		return nil
	}
	return plain
}

// xrpc_blobs_in calls found for each blob a record names, however deep.
func xrpc_blobs_in(v any, found func(string)) {
	switch item := v.(type) {
	case map[string]any:
		if item["$type"] == "blob" {
			if id := bluesky_blob_cid(item); id != "" {
				found(id)
			}
			return
		}
		for _, inner := range item {
			xrpc_blobs_in(inner, found)
		}
	case []any:
		for _, inner := range item {
			xrpc_blobs_in(inner, found)
		}
	}
}

// xrpc_missing_blobs answers the blobs the account's records name that it
// does not hold, as a client moving it uploads them.
func xrpc_missing_blobs(c *gin.Context) {
	s := xrpc_authenticate(c, false)
	if s == nil {
		return
	}
	db := db_user(s.user, "user")
	blobs := []any{}
	limit := atproto_limit(c, 500, 1000)
	cursor := c.Query("cursor")
	referenced := xrpc_referenced(s)
	var ids []string
	for id := range referenced {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	last := ""
	for _, id := range ids {
		if cursor != "" && id <= cursor {
			continue
		}
		if held, _ := db.exists("select 1 from blobs where account=? and cid=?", s.id, id); held {
			continue
		}
		blobs = append(blobs, map[string]any{"cid": id, "recordUri": referenced[id]})
		last = id
		if len(blobs) == limit {
			break
		}
	}
	answer := map[string]any{"blobs": blobs}
	if len(blobs) == limit {
		answer["cursor"] = last
	}
	c.JSON(http.StatusOK, answer)
}

// xrpc_activation activates an account - one that has arrived, once its
// identity names this server - or deactivates one that is moving away.
func xrpc_activation(c *gin.Context, active bool) {
	s := xrpc_authenticate(c, false)
	if s == nil {
		return
	}
	ctx := c.Request.Context()
	data := account_data(s.row)
	if active {
		op, _, err := plc_last(ctx, s.did)
		if err != nil {
			xrpc_fail(c, http.StatusBadRequest, "InvalidRequest", err.Error())
			return
		}
		domain, _ := data["domain"].(string)
		if !xrpc_names_here(s, op, domain) {
			xrpc_fail(c, http.StatusBadRequest, "InvalidRequest", "The identity does not name this server and the account's key yet")
			return
		}
		delete(data, "deactivated")
		delete(data, "arriving")
	} else {
		data["deactivated"] = true
	}
	db_user(s.user, "user").account_set(s.id, map[string]any{"data": json_encode(data)})
	if active {
		stream_append(s.did, "#identity", map[string]any{"did": s.did, "handle": row_string(s.row, "identifier")})
		stream_append(s.did, "#account", map[string]any{"did": s.did, "active": true})
		// A commit signed with this server's key, which the identity now names.
		if r := repository_get(s.user, s.id); r != nil {
			repository_commit(ctx, r, nil)
		}
		domain, _ := data["domain"].(string)
		go atproto_crawl(domain)
	} else {
		stream_append(s.did, "#account", map[string]any{"did": s.did, "active": false, "status": "deactivated"})
	}
	c.JSON(http.StatusOK, map[string]any{})
}

// xrpc_names_here says whether an identity operation names this server for
// the account: its data server here, and the account's signing key here.
func xrpc_names_here(s *xrpc_session, op map[string]any, domain string) bool {
	signing := keypair_get(s.user, s.id, key_signing)
	if signing == nil {
		return false
	}
	methods, _ := op["verificationMethods"].(map[string]any)
	services, _ := op["services"].(map[string]any)
	pds, _ := services["atproto_pds"].(map[string]any)
	endpoint, _ := pds["endpoint"].(string)
	return methods["atproto"] == signing.Public && strings.TrimSuffix(endpoint, "/") == atproto_endpoint(domain)
}

// xrpc_recommended answers the identity this server would have the account
// name: its rotation keys, its handle, its signing key and this server.
func xrpc_recommended(c *gin.Context) {
	s := xrpc_authenticate(c, false)
	if s == nil {
		return
	}
	keys, _, err := plc_keys(s.user, s.id)
	signing := keypair_get(s.user, s.id, key_signing)
	if err != nil || signing == nil {
		xrpc_fail(c, http.StatusBadRequest, "InvalidRequest", "The account has no keys")
		return
	}
	domain, _ := account_data(s.row)["domain"].(string)
	c.JSON(http.StatusOK, map[string]any{
		"rotationKeys":        keys,
		"alsoKnownAs":         []string{"at://" + row_string(s.row, "identifier")},
		"verificationMethods": map[string]any{"atproto": signing.Public},
		"services":            map[string]any{"atproto_pds": map[string]any{"type": "AtprotoPersonalDataServer", "endpoint": atproto_endpoint(domain)}},
	})
}

// xrpc_plc_request sends the account's user the code a change of identity
// needs, as their notification, since a moving account's identity is the
// user's to give away.
func xrpc_plc_request(c *gin.Context) {
	s := xrpc_authenticate(c, false)
	if s == nil {
		return
	}
	code := strings.ToUpper(random_alphanumeric(5)) + "-" + strings.ToUpper(random_alphanumeric(5))
	mastodon_snapshot(s.user, "plc", s.id, map[string]any{"code": code, "expires": now() + 3600})
	language := user_language(s.user)
	title := resolve_core_label(language, "bluesky.identity_code.title", nil)
	body := resolve_core_label(language, "bluesky.identity_code.body", map[string]any{"account": row_string(s.row, "identifier"), "code": code})
	topic := resolve_core_label(language, "bluesky.identity_code.topic", nil)
	if err := xrpc_notify(s.user, Map{"topic": "bluesky/identity", "object": s.id, "title": title, "body": body, "url": "/settings/user/networks", "label": topic, "event": "identity:" + code}); err != nil {
		xrpc_fail(c, http.StatusBadRequest, "InvalidRequest", "The code could not be sent")
		return
	}
	c.JSON(http.StatusOK, map[string]any{})
}

// xrpc_notify sends the user a notification; a test stands its own in.
var xrpc_notify = func(user *User, args Map) error {
	return service_call_as_server(user.UID, "notifications", "send", args)
}

// xrpc_plc_sign signs a change of the account's identity with this server's
// rotation key: the keys, handle and server another server recommends, once
// the user gives the code sent them.
func xrpc_plc_sign(c *gin.Context) {
	s := xrpc_authenticate(c, false)
	if s == nil {
		return
	}
	var body struct {
		Token               string         `json:"token"`
		RotationKeys        []string       `json:"rotationKeys"`
		AlsoKnownAs         []string       `json:"alsoKnownAs"`
		VerificationMethods map[string]any `json:"verificationMethods"`
		Services            map[string]any `json:"services"`
	}
	if !xrpc_body(c, &body) {
		return
	}
	held := mastodon_snapshot_get(s.user, "plc", s.id)
	if held == nil || body.Token == "" || mastodon_string(held, "code") != body.Token || mastodon_integer(held, "expires") < now() {
		xrpc_fail(c, http.StatusBadRequest, "InvalidToken", "The code is wrong or has expired")
		return
	}
	ctx := c.Request.Context()
	last, previous, err := plc_last(ctx, s.did)
	if err != nil {
		xrpc_fail(c, http.StatusBadRequest, "InvalidRequest", err.Error())
		return
	}
	_, private, err := plc_keys(s.user, s.id)
	if err != nil {
		xrpc_fail(c, http.StatusBadRequest, "InvalidRequest", err.Error())
		return
	}
	op := map[string]any{"type": "plc_operation", "prev": previous, "rotationKeys": last["rotationKeys"], "verificationMethods": last["verificationMethods"], "alsoKnownAs": last["alsoKnownAs"], "services": last["services"]}
	if body.RotationKeys != nil {
		op["rotationKeys"] = body.RotationKeys
	}
	if body.AlsoKnownAs != nil {
		op["alsoKnownAs"] = body.AlsoKnownAs
	}
	if body.VerificationMethods != nil {
		op["verificationMethods"] = body.VerificationMethods
	}
	if body.Services != nil {
		op["services"] = body.Services
	}
	if err := plc_sign(op, private); err != nil {
		xrpc_fail(c, http.StatusBadRequest, "InvalidRequest", err.Error())
		return
	}
	db_user(s.user, "user").exec("delete from snapshots where kind='plc' and key=?", s.id)
	c.JSON(http.StatusOK, map[string]any{"operation": op})
}

// xrpc_plc_submit sends a signed change of an arriving account's identity to
// the directory: one that names this server and the account's key here.
func xrpc_plc_submit(c *gin.Context) {
	s := xrpc_authenticate(c, false)
	if s == nil {
		return
	}
	var body struct {
		Operation map[string]any `json:"operation"`
	}
	if !xrpc_body(c, &body) {
		return
	}
	domain, _ := account_data(s.row)["domain"].(string)
	if !xrpc_names_here(s, body.Operation, domain) {
		xrpc_fail(c, http.StatusBadRequest, "InvalidRequest", "The operation does not name this server and the account's key")
		return
	}
	if err := plc_submit(c.Request.Context(), s.did, body.Operation); err != nil {
		xrpc_fail(c, http.StatusBadRequest, "InvalidRequest", err.Error())
		return
	}
	c.JSON(http.StatusOK, map[string]any{})
}

// xrpc_create_account takes an account arriving from another server: the
// Mochi user made it ready in Settings, minting the password the moving
// client gives here; the client proves the identity is the account's with a
// token its old server signed, and the account waits deactivated until its
// repository is here and its identity names this server.
func xrpc_create_account(c *gin.Context) {
	var body struct {
		Handle     string `json:"handle"`
		DID        string `json:"did"`
		Password   string `json:"password"`
		InviteCode string `json:"inviteCode"`
	}
	if !xrpc_body(c, &body) {
		return
	}
	if !strings.HasPrefix(body.DID, "did:plc:") {
		xrpc_fail(c, http.StatusBadRequest, "InvalidRequest", "Accounts are made in Mochi; one moving here names its identity")
		return
	}
	password := body.Password
	if password == "" {
		password = body.InviteCode
	}
	user, row, name := password_check("bluesky", strings.ToLower(body.Handle), password)
	if user == nil || row == nil {
		xrpc_fail(c, http.StatusBadRequest, "InvalidInviteCode", "Make the account ready in Mochi first, and give the password it mints")
		return
	}
	data := account_data(row)
	if arriving, _ := data["arriving"].(bool); !arriving || atproto_did(row) != "" {
		xrpc_fail(c, http.StatusBadRequest, "InvalidRequest", "The account is not waiting for one to arrive")
		return
	}
	ctx := c.Request.Context()
	if err := xrpc_service_verify(ctx, c, body.DID, "com.atproto.server.createAccount"); err != nil {
		xrpc_fail(c, http.StatusUnauthorized, "InvalidToken", err.Error())
		return
	}
	if existing, _ := networks_database().exists("select 1 from identities where did=?", body.DID); existing {
		xrpc_fail(c, http.StatusBadRequest, "AlreadyExists", "The identity is held here already")
		return
	}
	id, _ := row["id"].(string)
	data["did"] = body.DID
	data["deactivated"] = true
	db_user(user, "user").account_set(id, map[string]any{"data": json_encode(data)})
	atproto_register(body.DID, id, user)
	repository_create(user, id, body.DID)
	row = account_hosted_get(user, id)
	access, refresh := xrpc_mint(c, user, row, name)
	c.JSON(http.StatusOK, map[string]any{"accessJwt": access, "refreshJwt": refresh, "handle": row_string(row, "identifier"), "did": body.DID})
}

// xrpc_service_verify checks a request carries a token signed by an
// identity's current key for this server and the method.
func xrpc_service_verify(ctx context.Context, c *gin.Context, did string, method string) error {
	_, raw := xrpc_bearer(c)
	parts := strings.Split(raw, ".")
	if len(parts) != 3 {
		return fmt.Errorf("a token from the account's old server is required")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return fmt.Errorf("unreadable token")
	}
	var claims struct {
		Issuer   string `json:"iss"`
		Audience string `json:"aud"`
		Expires  int64  `json:"exp"`
		Method   string `json:"lxm"`
	}
	if json.Unmarshal(payload, &claims) != nil || claims.Issuer != did || claims.Expires < now() {
		return fmt.Errorf("the token is not the identity's or has expired")
	}
	if claims.Audience != "did:web:"+activitypub_host(c) || (claims.Method != "" && claims.Method != method) {
		return fmt.Errorf("the token is not for this server")
	}
	op, _, err := plc_last(ctx, did)
	if err != nil {
		return err
	}
	methods, _ := op["verificationMethods"].(map[string]any)
	key, _ := methods["atproto"].(string)
	public, err := atcrypto.ParsePublicDIDKey(key)
	if err != nil {
		return fmt.Errorf("the identity's key is unreadable")
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return fmt.Errorf("unreadable signature")
	}
	if err := public.HashAndVerifyLenient([]byte(parts[0]+"."+parts[1]), signature); err != nil {
		return fmt.Errorf("the token is not signed by the identity's key")
	}
	return nil
}

// xrpc_import takes an arriving account's repository: its records, written
// here in one commit signed with this server's key.
func xrpc_import(c *gin.Context) {
	s := xrpc_authenticate(c, true)
	if s == nil {
		return
	}
	if !atproto_deactivated(s.row) {
		xrpc_fail(c, http.StatusBadRequest, "InvalidRequest", "A repository is imported only before the account is activated")
		return
	}
	data, err := io.ReadAll(io.LimitReader(c.Request.Body, xrpc_import_maximum+1))
	if err != nil || len(data) > xrpc_import_maximum {
		xrpc_fail(c, http.StatusRequestEntityTooLarge, "PayloadTooLarge", "The repository is too large")
		return
	}
	ctx := c.Request.Context()
	commit, imported, err := atrepo.LoadRepoFromCAR(ctx, bytes.NewReader(data))
	if err != nil || commit == nil || commit.DID != s.did {
		xrpc_fail(c, http.StatusBadRequest, "InvalidRequest", "The repository is not the account's")
		return
	}
	r := repository_get(s.user, s.id)
	if r == nil {
		xrpc_fail(c, http.StatusBadRequest, "RepoNotFound", "The account has no repository")
		return
	}
	db := db_user(s.user, "user")
	var ops []repository_operation
	walked := imported.MST.Walk(func(key []byte, value cid.Cid) error {
		block, err := imported.RecordStore.Get(ctx, value)
		if err != nil {
			return err
		}
		path := string(key)
		collection, rkey, found := strings.Cut(path, "/")
		if !found {
			return fmt.Errorf("invalid path %s", path)
		}
		db.exec("insert or ignore into nodes (account, cid, data) values (?, ?, ?)", s.id, value.String(), block.RawData())
		db.exec("insert into records (account, collection, rkey, cid, created, updated) values (?, ?, ?, ?, ?, 0) on conflict(account, collection, rkey) do update set cid=excluded.cid", s.id, collection, rkey, value.String(), now())
		v := value
		ops = append(ops, repository_operation{path: path, cid: &v, data: block.RawData()})
		return nil
	})
	if walked != nil {
		xrpc_fail(c, http.StatusBadRequest, "InvalidRequest", walked.Error())
		return
	}
	// In commits of the most operations the stream carries in one.
	for start := 0; start < len(ops); start += xrpc_commit_maximum {
		if _, err := repository_commit(ctx, r, ops[start:min(start+xrpc_commit_maximum, len(ops))]); err != nil {
			xrpc_fail(c, http.StatusBadRequest, "InvalidRequest", err.Error())
			return
		}
	}
	c.JSON(http.StatusOK, map[string]any{})
}

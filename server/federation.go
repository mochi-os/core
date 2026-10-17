// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: Matrix federation
//
// The engine that makes this server a Matrix home server for the hosted Matrix
// accounts on its domains (the federation plan, phase 24). Each domain with a
// Matrix account has an ed25519 signing key, made with its first account and
// published at /_matrix/key/v2/server for the domain the request names;
// /.well-known/matrix/server on the domain names the domain itself on port
// 443, so every federation request arrives under the domain it is for. A
// request from another server is checked against the X-Matrix signature in its
// Authorization header, with that server's keys fetched from it and cached,
// and its destination must be one of this server's Matrix domains.
//
// What the server sends goes through one client whose transport resolves a
// server name as the specification says - the well-known document, then the
// SRV records, then port 8448 - and sends under the host policy, the limiter
// and the private-address check, as every outbound request does. Events for
// other servers are queued per destination and sent in transactions, with a
// backoff for a server that keeps failing. The rooms themselves are in
// rooms.go.

package main

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/fclient"
	"github.com/matrix-org/gomatrixserverlib/spec"
	"github.com/sirupsen/logrus"
)

// federation_body_maximum caps a request body on the engine's paths: a
// transaction carries up to fifty events of up to 64 KiB each.
const federation_body_maximum = 8 << 20

// federation_key_validity is how long the keys this server publishes are
// declared valid, after which other servers fetch them again.
const federation_key_validity = 7 * 24 * time.Hour

// federation_timeout bounds one request to another server.
var federation_timeout = 30 * time.Second

// federation_scheme is how another server is reached: HTTPS, as the
// specification requires. A variable so tests can serve from plain servers.
var federation_scheme = "https"

// The database the engine keeps for every user together: the rooms (rooms.go),
// the other servers' keys, the queue to each other server and the
// transactions already taken. Its tables are made each time it is opened, so
// nothing uses it before they exist.
var federation_database_lock sync.Mutex

func federation_database() *DB {
	federation_database_lock.Lock()
	defer federation_database_lock.Unlock()
	db, _, reused := db_open_work("db/matrix.db")
	if db != nil && !reused {
		db.exec("create table if not exists servers (name text not null, key text not null, public text not null, valid integer not null, expired integer not null default 0, primary key (name, key))")
		db.exec("create table if not exists outbox (id integer primary key autoincrement, destination text not null, kind text not null, body text not null, created integer not null)")
		db.exec("create index if not exists outbox_destination on outbox(destination, id)")
		db.exec("create table if not exists destinations (name text not null primary key, failures integer not null default 0, retry integer not null default 0)")
		db.exec("create table if not exists transactions (origin text not null, id text not null, received integer not null, primary key (origin, id))")
		rooms_tables(db)
	}
	return db
}

// federation_engine is the engine's registration.
func federation_engine() *engine {
	return &engine{
		name:     "matrix",
		prefixes: []string{"/_matrix/", "/.well-known/matrix/server"},
		setting:  "matrix",
		body:     federation_body_maximum,
		handler:  federation_route,
	}
}

// === Identity ===

// A federation_identity is one of this server's Matrix domains with its
// signing key.
type federation_identity struct {
	domain  spec.ServerName
	key     gomatrixserverlib.KeyID
	private ed25519.PrivateKey
	public  ed25519.PublicKey
}

var federation_identities = map[spec.ServerName]*federation_identity{}
var federation_identities_lock sync.Mutex

// federation_local reports whether a server name is one of this server's
// Matrix domains: a domain it serves with a Matrix account on it.
func federation_local(name spec.ServerName) bool {
	domain := strings.ToLower(string(name))
	if domain == "" {
		return false
	}
	found, _ := networks_database().exists("select 1 from actors where network='matrix' and domain=? and deleted=0", domain)
	return found
}

// federation_identity_of is a Matrix domain's identity, its key made on first
// use; nil for a domain that is not one.
func federation_identity_of(name spec.ServerName) *federation_identity {
	domain := spec.ServerName(strings.ToLower(string(name)))
	if !federation_local(domain) {
		return nil
	}
	federation_identities_lock.Lock()
	defer federation_identities_lock.Unlock()
	if id := federation_identities[domain]; id != nil {
		return id
	}
	encoded := networks_key("matrix "+string(domain), key_ed25519)
	block, _ := pem.Decode([]byte(encoded))
	if block == nil {
		return nil
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	private, ok := parsed.(ed25519.PrivateKey)
	if err != nil || !ok {
		return nil
	}
	public := private.Public().(ed25519.PublicKey)
	sum := sha256.Sum256(public)
	id := &federation_identity{domain: domain, key: gomatrixserverlib.KeyID("ed25519:" + hex.EncodeToString(sum[:4])), private: private, public: public}
	federation_identities[domain] = id
	return id
}

// federation_signing lists every Matrix domain's identity, for the client to
// sign with whichever a request comes from.
func federation_signing() []*fclient.SigningIdentity {
	rows, _ := networks_database().rows("select distinct domain from actors where network='matrix' and deleted=0")
	var out []*fclient.SigningIdentity
	for _, row := range rows {
		if id := federation_identity_of(spec.ServerName(row_string(row, "domain"))); id != nil {
			out = append(out, &fclient.SigningIdentity{ServerName: id.domain, KeyID: id.key, PrivateKey: id.private})
		}
	}
	return out
}

// federation_user is the local account a Matrix user id names: the user and
// the account's id, or nil.
func federation_user(id string) (*User, string) {
	user, err := spec.NewUserID(id, true)
	if err != nil {
		return nil, ""
	}
	var a actor
	if !networks_database().scan(&a, "select * from actors where network='matrix' and domain=? and handle=? and deleted=0", strings.ToLower(string(user.Domain())), user.Local()) {
		return nil, ""
	}
	u := user_by_uid(a.User)
	if u == nil || user_pending(u) {
		return nil, ""
	}
	return u, a.ID
}

// federation_sender is the user a sender id names. Pseudo-identities (MSC4014)
// are not used, so a sender is its user.
func federation_sender(room spec.RoomID, sender spec.SenderID) (*spec.UserID, error) {
	return spec.NewUserID(string(sender), true)
}

// === Requests ===

// federation_error answers a request with a Matrix error.
func federation_error(c *gin.Context, status int, code string, message string) {
	c.JSON(status, map[string]any{"errcode": code, "error": message}) // i18n-ok: Matrix error codes and their text are protocol, read by servers
}

// federation_domain is the domain a request was made to, by its Host.
func federation_domain(c *gin.Context) spec.ServerName {
	host := c.Request.Host
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	return spec.ServerName(strings.ToLower(host))
}

// federation_host is a server name's host, without the port it may carry, as
// the host policy names servers.
func federation_host(name spec.ServerName) string {
	host := string(name)
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	return host
}

// federation_verify checks a request's X-Matrix signature and answers the
// signed request - its origin, destination and content - or nil having
// answered. The library refuses a destination that is not one of this
// server's Matrix domains.
func federation_verify(c *gin.Context) *fclient.FederationRequest {
	domain := federation_domain(c)
	request, failure := fclient.VerifyHTTPRequest(c.Request, time.Now(), domain, federation_local, federation_keyring())
	if request == nil {
		message := "the request's signature could not be checked"
		if text, ok := failure.JSON.(interface{ Error() string }); ok {
			message = text.Error()
		}
		federation_error(c, failure.Code, "M_UNAUTHORIZED", message)
		return nil
	}
	if err := host_allowed(nil, "matrix", federation_host(request.Origin())); err != nil {
		federation_error(c, http.StatusForbidden, "M_FORBIDDEN", "the server is blocked here")
		return nil
	}
	return request
}

// === The client ===

// A federation_target is where a server name is reached: the address dialled,
// the name its certificate must carry and the Host the request names.
type federation_target struct {
	address string
	tls     string
	host    string
	expires time.Time
}

var federation_targets = map[string]federation_target{}
var federation_targets_lock sync.Mutex

// federation_lookup finds a server's SRV records. A variable so tests can
// answer for names with none.
var federation_lookup = func(ctx context.Context, service string, name string) ([]*net.SRV, error) {
	_, records, err := net.DefaultResolver.LookupSRV(ctx, service, "tcp", name)
	return records, err
}

// federation_resolve finds where a server name is reached (the server-server
// specification's "Resolving server names").
func federation_resolve(ctx context.Context, name string) (federation_target, error) {
	federation_targets_lock.Lock()
	if t, found := federation_targets[name]; found && time.Now().Before(t.expires) {
		federation_targets_lock.Unlock()
		return t, nil
	}
	federation_targets_lock.Unlock()
	target, err := federation_resolve_fresh(ctx, name)
	if err != nil {
		return target, err
	}
	target.expires = time.Now().Add(24 * time.Hour)
	federation_targets_lock.Lock()
	federation_targets[name] = target
	federation_targets_lock.Unlock()
	return target, nil
}

func federation_resolve_fresh(ctx context.Context, name string) (federation_target, error) {
	host, port, err := net.SplitHostPort(name)
	if err != nil {
		host, port = name, ""
	}
	// An address, or a name with its port, is reached as it is.
	if net.ParseIP(host) != nil || port != "" {
		if port == "" {
			port = "8448"
		}
		return federation_target{address: net.JoinHostPort(host, port), tls: host, host: name}, nil
	}
	// The well-known document may delegate to another name.
	if delegated := federation_wellknown(ctx, host); delegated != "" {
		dhost, dport, err := net.SplitHostPort(delegated)
		if err != nil {
			dhost, dport = delegated, ""
		}
		if net.ParseIP(dhost) != nil || dport != "" {
			if dport == "" {
				dport = "8448"
			}
			return federation_target{address: net.JoinHostPort(dhost, dport), tls: dhost, host: delegated}, nil
		}
		if t, ok := federation_srv(ctx, dhost); ok {
			return t, nil
		}
		return federation_target{address: net.JoinHostPort(dhost, "8448"), tls: dhost, host: dhost}, nil
	}
	if t, ok := federation_srv(ctx, host); ok {
		return t, nil
	}
	return federation_target{address: net.JoinHostPort(host, "8448"), tls: host, host: host}, nil
}

// federation_srv reads a name's SRV records, the current service name and
// then the deprecated one: the target is dialled, and the certificate and
// Host are the name's own.
func federation_srv(ctx context.Context, name string) (federation_target, bool) {
	for _, service := range []string{"matrix-fed", "matrix"} {
		records, err := federation_lookup(ctx, service, name)
		if err == nil && len(records) > 0 {
			target := strings.TrimSuffix(records[0].Target, ".")
			return federation_target{address: net.JoinHostPort(target, strconv.Itoa(int(records[0].Port))), tls: name, host: name}, true
		}
	}
	return federation_target{}, false
}

// federation_wellknown reads a server name's /.well-known/matrix/server, the
// name it delegates to or "".
func federation_wellknown(ctx context.Context, name string) string {
	address := federation_scheme + "://" + name + "/.well-known/matrix/server"
	request, err := http.NewRequestWithContext(ctx, "GET", address, nil)
	if err != nil {
		return ""
	}
	request.Header.Set("Accept", "application/json")
	response, err := network_send(ctx, nil, "matrix", strings.ToLower(name)+"|matrix", request, 10)
	if err != nil {
		return ""
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return ""
	}
	var known struct {
		Server string `json:"m.server"`
	}
	data, err := read_limited(response.Body, 64<<10)
	if err != nil || json.Unmarshal(data, &known) != nil {
		return ""
	}
	return strings.TrimSpace(known.Server)
}

// A federation_transport carries the client's requests: a matrix:// address
// is resolved to the server's real one and sent under the host policy and the
// limiter, with the certificate checked against the server's name.
type federation_transport struct{}

func (federation_transport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Scheme != "matrix" {
		return nil, fmt.Errorf("not a Matrix address: %s", r.URL)
	}
	name := r.URL.Host
	hostname := federation_host(spec.ServerName(name))
	if err := host_allowed(nil, "matrix", hostname); err != nil {
		return nil, err
	}
	key := strings.ToLower(hostname) + "|matrix"
	if !rate_limit_host.allow(key) {
		return nil, rate_limit_refuse(rate_limit_host, key, "requests per minute to "+hostname)
	}
	target, err := federation_resolve(r.Context(), name)
	if err != nil {
		return nil, err
	}
	out := r.Clone(r.Context())
	out.URL = &url.URL{Scheme: federation_scheme, Host: target.address, Path: r.URL.Path, RawPath: r.URL.RawPath, RawQuery: r.URL.RawQuery}
	out.Host = target.host
	if out.Header.Get("User-Agent") == "" {
		out.Header.Set("User-Agent", "Mochi/"+build_version)
	}
	transport := federation_transports(target.tls)
	return transport.RoundTrip(out)
}

var federation_transports_cache = map[string]*http.Transport{}
var federation_transports_lock sync.Mutex

// federation_transports is the transport for a certificate name: the
// private-address check in its dialler as everywhere, and the certificate
// checked against the server's name whatever address is dialled.
func federation_transports(name string) *http.Transport {
	federation_transports_lock.Lock()
	defer federation_transports_lock.Unlock()
	if t := federation_transports_cache[name]; t != nil {
		return t
	}
	t := &http.Transport{
		Proxy: nil,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
			Control: func(network string, address string, _ syscall.RawConn) error {
				return url_address_allowed(address)
			},
		}).DialContext,
		TLSClientConfig:     &tls.Config{ServerName: name},
		TLSHandshakeTimeout: 10 * time.Second,
		IdleConnTimeout:     90 * time.Second,
		MaxIdleConnsPerHost: 4,
	}
	federation_transports_cache[name] = t
	return t
}

// federation_client is the client the engine sends with, signing as each of
// the server's Matrix domains. A variable so tests can hand over their own.
var federation_client = func() fclient.FederationClient {
	return fclient.NewFederationClient(federation_signing(), fclient.WithTransport(federation_transport{}), fclient.WithTimeout(federation_timeout))
}

// === Keys ===

// federation_keys is the cache of other servers' keys the key ring reads,
// answering this server's own keys itself.
type federation_keys struct{}

func (federation_keys) FetcherName() string { return "matrix.db" }

func (federation_keys) FetchKeys(ctx context.Context, requests map[gomatrixserverlib.PublicKeyLookupRequest]spec.Timestamp) (map[gomatrixserverlib.PublicKeyLookupRequest]gomatrixserverlib.PublicKeyLookupResult, error) {
	out := map[gomatrixserverlib.PublicKeyLookupRequest]gomatrixserverlib.PublicKeyLookupResult{}
	db := federation_database()
	for request := range requests {
		if id := federation_identity_of(request.ServerName); id != nil {
			if request.KeyID == id.key {
				out[request] = gomatrixserverlib.PublicKeyLookupResult{
					VerifyKey:    gomatrixserverlib.VerifyKey{Key: spec.Base64Bytes(id.public)},
					ExpiredTS:    gomatrixserverlib.PublicKeyNotExpired,
					ValidUntilTS: spec.AsTimestamp(time.Now().Add(federation_key_validity)),
				}
			}
			continue
		}
		row, _ := db.row("select public, valid, expired from servers where name=? and key=?", string(request.ServerName), string(request.KeyID))
		if row == nil {
			continue
		}
		public, err := base64.RawStdEncoding.DecodeString(row_string(row, "public"))
		if err != nil {
			continue
		}
		out[request] = gomatrixserverlib.PublicKeyLookupResult{
			VerifyKey:    gomatrixserverlib.VerifyKey{Key: spec.Base64Bytes(public)},
			ExpiredTS:    spec.Timestamp(row_int(row, "expired")),
			ValidUntilTS: spec.Timestamp(row_int(row, "valid")),
		}
	}
	return out, nil
}

func (federation_keys) StoreKeys(ctx context.Context, results map[gomatrixserverlib.PublicKeyLookupRequest]gomatrixserverlib.PublicKeyLookupResult) error {
	db := federation_database()
	for request, result := range results {
		if federation_local(request.ServerName) {
			continue
		}
		db.exec("insert into servers (name, key, public, valid, expired) values (?, ?, ?, ?, ?) on conflict(name, key) do update set public=excluded.public, valid=excluded.valid, expired=excluded.expired",
			string(request.ServerName), string(request.KeyID), base64.RawStdEncoding.EncodeToString(result.Key), int64(result.ValidUntilTS), int64(result.ExpiredTS))
	}
	return nil
}

// federation_keyring checks signatures: the keys from the cache, else fetched
// from the server itself.
func federation_keyring() *gomatrixserverlib.KeyRing {
	return &gomatrixserverlib.KeyRing{
		KeyFetchers: []gomatrixserverlib.KeyFetcher{&gomatrixserverlib.DirectKeyFetcher{Client: federation_client(), IsLocalServerName: federation_local}},
		KeyDatabase: federation_keys{},
	}
}

// federation_key_answer is the signed document of a domain's keys.
func federation_key_answer(id *federation_identity) ([]byte, error) {
	document := map[string]any{
		"server_name":     string(id.domain),
		"valid_until_ts":  int64(spec.AsTimestamp(time.Now().Add(federation_key_validity))),
		"verify_keys":     map[string]any{string(id.key): map[string]any{"key": base64.RawStdEncoding.EncodeToString(id.public)}},
		"old_verify_keys": map[string]any{},
	}
	unsigned, err := json.Marshal(document)
	if err != nil {
		return nil, err
	}
	return gomatrixserverlib.SignJSON(string(id.domain), id.key, id.private, unsigned)
}

// === Routes ===

// federation_route dispatches a request on the engine's paths.
func federation_route(c *gin.Context) bool {
	path := c.Request.URL.Path
	method := c.Request.Method
	domain := federation_domain(c)
	switch {
	case path == "/.well-known/matrix/server":
		if !federation_local(domain) {
			return false
		}
		c.JSON(http.StatusOK, map[string]any{"m.server": string(domain) + ":443"})
		return true
	case path == "/_matrix/key/v2/server" || strings.HasPrefix(path, "/_matrix/key/v2/server/"):
		id := federation_identity_of(domain)
		if id == nil {
			federation_error(c, http.StatusNotFound, "M_NOT_FOUND", "no Matrix server here")
			return true
		}
		signed, err := federation_key_answer(id)
		if err != nil {
			federation_error(c, http.StatusInternalServerError, "M_UNKNOWN", "the keys could not be signed")
			return true
		}
		c.Data(http.StatusOK, "application/json", signed)
		return true
	case path == "/_matrix/federation/v1/version":
		c.JSON(http.StatusOK, map[string]any{"server": map[string]any{"name": "Mochi", "version": build_version}})
		return true
	case strings.HasPrefix(path, "/_matrix/federation/"):
		if !federation_local(domain) {
			federation_error(c, http.StatusNotFound, "M_NOT_FOUND", "no Matrix server here")
			return true
		}
		request := federation_verify(c)
		if request == nil {
			return true
		}
		// Split as sent: an event id of version 3 may hold a slash, escaped.
		federation_dispatch(c, method, strings.Split(strings.TrimPrefix(c.Request.URL.EscapedPath(), "/_matrix/federation/"), "/"), request)
		return true
	}
	federation_error(c, http.StatusNotFound, "M_UNRECOGNIZED", "unrecognised request")
	return true
}

// federation_dispatch answers a signed federation request by its path's
// segments after /_matrix/federation/.
func federation_dispatch(c *gin.Context, method string, parts []string, r *fclient.FederationRequest) {
	segment := func(i int) string {
		if i < len(parts) {
			value, err := url.PathUnescape(parts[i])
			if err == nil {
				return value
			}
			return parts[i]
		}
		return ""
	}
	route := method + " " + segment(0) + "/" + segment(1)
	switch {
	case route == "PUT v1/send" && len(parts) == 3:
		federation_receive_transaction(c, r, segment(2))
	case route == "GET v1/make_join" && len(parts) == 4:
		rooms_make_join(c, r, segment(2), segment(3))
	case (route == "PUT v2/send_join" || route == "PUT v1/send_join") && len(parts) == 4:
		rooms_send_join(c, r, segment(2), segment(3), segment(0) == "v1")
	case route == "GET v1/make_leave" && len(parts) == 4:
		rooms_make_leave(c, r, segment(2), segment(3))
	case (route == "PUT v2/send_leave" || route == "PUT v1/send_leave") && len(parts) == 4:
		rooms_send_leave(c, r, segment(2), segment(3), segment(0) == "v1")
	case route == "PUT v2/invite" && len(parts) == 4:
		rooms_invite_received(c, r, segment(2), segment(3))
	case route == "GET v1/event" && len(parts) == 3:
		rooms_serve_event(c, r, segment(2))
	case route == "GET v1/state" && len(parts) == 3:
		rooms_serve_state(c, r, segment(2), c.Query("event_id"), false)
	case route == "GET v1/state_ids" && len(parts) == 3:
		rooms_serve_state(c, r, segment(2), c.Query("event_id"), true)
	case route == "GET v1/event_auth" && len(parts) == 4:
		rooms_serve_auth(c, r, segment(2), segment(3))
	case route == "POST v1/get_missing_events" && len(parts) == 3:
		rooms_serve_missing(c, r, segment(2))
	case route == "GET v1/backfill" && len(parts) == 3:
		rooms_serve_backfill(c, r, segment(2), c.QueryArray("v"), c.Query("limit"))
	case route == "GET v1/query" && segment(2) == "profile":
		rooms_serve_profile(c, c.Query("user_id"), c.Query("field"))
	case route == "GET v1/query" && segment(2) == "directory":
		federation_error(c, http.StatusNotFound, "M_NOT_FOUND", "no such room alias here")
	case route == "GET v1/user" && segment(2) == "devices" && len(parts) == 4:
		// No device keys yet: those come with Matrix clients signing in.
		c.JSON(http.StatusOK, map[string]any{"user_id": segment(3), "stream_id": 0, "devices": []any{}})
	case route == "POST v1/user" && segment(2) == "keys":
		switch segment(3) {
		case "query":
			c.JSON(http.StatusOK, map[string]any{"device_keys": map[string]any{}})
		case "claim":
			c.JSON(http.StatusOK, map[string]any{"one_time_keys": map[string]any{}})
		default:
			federation_error(c, http.StatusNotFound, "M_UNRECOGNIZED", "unrecognised request")
		}
	case route == "GET v1/publicRooms" || route == "POST v1/publicRooms":
		c.JSON(http.StatusOK, map[string]any{"chunk": []any{}, "total_room_count_estimate": 0})
	default:
		federation_error(c, http.StatusNotFound, "M_UNRECOGNIZED", "unrecognised request")
	}
}

// === Transactions in ===

// federation_receive_transaction takes a transaction from another server:
// each event is processed in turn and answered for. A transaction already
// taken is answered at once. Ephemeral events (typing, receipts, presence)
// come with a later part of the phase.
func federation_receive_transaction(c *gin.Context, r *fclient.FederationRequest, id string) {
	origin := string(r.Origin())
	db := federation_database()
	if seen, _ := db.exists("select 1 from transactions where origin=? and id=?", origin, id); seen {
		c.JSON(http.StatusOK, map[string]any{"pdus": map[string]any{}})
		return
	}
	var t struct {
		PDUs []json.RawMessage `json:"pdus"`
		EDUs []json.RawMessage `json:"edus"`
	}
	if err := json.Unmarshal(r.Content(), &t); err != nil || len(t.PDUs) > 50 || len(t.EDUs) > 100 {
		federation_error(c, http.StatusBadRequest, "M_BAD_JSON", "not a transaction")
		return
	}
	results := map[string]any{}
	for _, raw := range t.PDUs {
		id, err := rooms_receive(c.Request.Context(), r.Origin(), raw)
		if id == "" {
			continue
		}
		if err != nil {
			results[id] = map[string]any{"error": err.Error()}
		} else {
			results[id] = map[string]any{}
		}
	}
	db.exec("insert or ignore into transactions (origin, id, received) values (?, ?, ?)", origin, id, now())
	db.exec("delete from transactions where received<?", now()-7*86400)
	c.JSON(http.StatusOK, map[string]any{"pdus": results})
}

// === Transactions out ===

// federation_workers is each destination with a worker running, and what
// tells it more was queued.
var federation_workers = map[string]chan struct{}{}
var federation_workers_lock sync.Mutex

// federation_send queues an event for each server given, other than this
// one's own domains, and wakes their workers.
func federation_send(destinations []string, event gomatrixserverlib.PDU) {
	db := federation_database()
	for _, destination := range destinations {
		if destination == "" || federation_local(spec.ServerName(destination)) {
			continue
		}
		db.exec("insert into outbox (destination, kind, body, created) values (?, 'pdu', ?, ?)", destination, event.EventID(), now())
		federation_worker_start(destination)
	}
}

// federation_worker_start runs a destination's worker, or tells the running
// one there is more.
func federation_worker_start(destination string) {
	federation_workers_lock.Lock()
	wake, running := federation_workers[destination]
	if !running {
		wake = make(chan struct{}, 1)
		federation_workers[destination] = wake
	}
	federation_workers_lock.Unlock()
	if running {
		select {
		case wake <- struct{}{}:
		default:
		}
		return
	}
	go federation_worker(destination, wake)
}

// federation_worker_done ends a destination's worker where nothing was
// queued since it last looked, and answers whether it ended.
func federation_worker_done(destination string, wake chan struct{}) bool {
	federation_workers_lock.Lock()
	defer federation_workers_lock.Unlock()
	select {
	case <-wake:
		return false
	default:
	}
	delete(federation_workers, destination)
	return true
}

// federation_backoff is how long a destination waits after failures in a row:
// doubling from thirty seconds to a day.
func federation_backoff(failures int64) int64 {
	wait := int64(30)
	for i := int64(1); i < failures && wait < 86400; i++ {
		wait *= 2
	}
	return min(wait, 86400)
}

// federation_worker sends a destination's queue in transactions of up to
// fifty events until it is empty, waiting out its backoff after a failure;
// more queued meanwhile has it look again, but not before its backoff ends.
// What has waited a week for a server that never answers is dropped.
func federation_worker(destination string, wake chan struct{}) {
	db := federation_database()
	for {
		db.exec("delete from outbox where destination=? and created<?", destination, now()-7*86400)
		if row, _ := db.row("select retry from destinations where name=?", destination); row != nil {
			if wait := row_int(row, "retry") - now(); wait > 0 {
				select {
				case <-time.After(time.Duration(min(wait, 3600)) * time.Second):
				case <-wake:
				}
				continue
			}
		}
		rows, _ := db.rows("select id, body from outbox where destination=? and kind='pdu' order by id limit 50", destination)
		if len(rows) == 0 {
			if federation_worker_done(destination, wake) {
				return
			}
			continue
		}
		var pdus []json.RawMessage
		var ids []any
		for _, row := range rows {
			ids = append(ids, row["id"])
			if event := rooms_event(row_string(row, "body")); event != nil {
				pdus = append(pdus, event.JSON())
			}
		}
		origin := federation_origin(pdus)
		err := error(nil)
		if len(pdus) > 0 && origin != "" {
			ctx, cancel := context.WithTimeout(context.Background(), federation_timeout+10*time.Second)
			_, err = federation_client().SendTransaction(ctx, gomatrixserverlib.Transaction{
				TransactionID:  gomatrixserverlib.TransactionID(uid()),
				Origin:         spec.ServerName(origin),
				Destination:    spec.ServerName(destination),
				OriginServerTS: spec.AsTimestamp(time.Now()),
				PDUs:           pdus,
			})
			cancel()
		}
		if err != nil {
			var limited *RateLimitError
			if errors.As(err, &limited) {
				time.Sleep(time.Duration(max(limited.Retry, 1)) * time.Second)
				continue
			}
			debug("Matrix transaction to %s failed: %v", destination, err)
			db.exec("insert into destinations (name, failures, retry) values (?, 1, 0) on conflict(name) do update set failures=failures+1", destination)
			row, _ := db.row("select failures from destinations where name=?", destination)
			db.exec("update destinations set retry=? where name=?", now()+federation_backoff(row_int(row, "failures")), destination)
			continue
		}
		db.exec("delete from destinations where name=?", destination)
		db.exec("delete from outbox where id in ("+strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")+")", ids...)
	}
}

// federation_origin is the domain a transaction is sent from: the domain of
// the first event's sender, which is the server's own.
func federation_origin(pdus []json.RawMessage) string {
	for _, raw := range pdus {
		var event struct {
			Sender string `json:"sender"`
		}
		if json.Unmarshal(raw, &event) == nil {
			if user, err := spec.NewUserID(event.Sender, true); err == nil && federation_local(user.Domain()) {
				return string(user.Domain())
			}
		}
	}
	return ""
}

// federation_log carries the library's warnings to the debug log.
type federation_log struct{}

func (federation_log) Write(line []byte) (int, error) {
	debug("Matrix: %s", strings.TrimSpace(string(line)))
	return len(line), nil
}

// federation_manager starts a worker for every destination with events
// waiting at startup; federation_send starts the rest.
func federation_manager() {
	logrus.SetOutput(federation_log{})
	logrus.SetLevel(logrus.WarnLevel)
	if !networks_development {
		return
	}
	rows, _ := federation_database().rows("select distinct destination from outbox")
	for _, row := range rows {
		federation_worker_start(row_string(row, "destination"))
	}
}

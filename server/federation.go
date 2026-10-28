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
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/matrix-org/gomatrix"
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

// federation_database opens the database the engine keeps for every user
// together, whose tables federation_create lays.
func federation_database() *DB {
	return db_open("db/matrix.db")
}

// federation_create lays the engine's database, for db_create and
// db_upgrade_18: the rooms (rooms.go), the other servers' keys, the queue to
// each other server - each row with the domain it is sent from - and the
// transactions already taken. A change here is also a new db_upgrade_N step,
// and db_upgrade_18 keeps a copy of this as it was.
func federation_create(db *DB) {
	db.exec("create table if not exists servers (name text not null, key text not null, public text not null, valid integer not null, expired integer not null default 0, primary key (name, key))")
	db.exec("create table if not exists outbox (id integer primary key autoincrement, destination text not null, origin text not null default '', kind text not null, body text not null, created integer not null)")
	db.exec("create index if not exists outbox_destination on outbox(destination, id)")
	db.exec("create table if not exists destinations (name text not null primary key, failures integer not null default 0, retry integer not null default 0)")
	db.exec("create table if not exists transactions (origin text not null, id text not null, received integer not null, primary key (origin, id))")
	// Signing keys a domain has replaced: published as its old keys, with
	// when they stopped signing, so what they signed stays verifiable.
	db.exec("create table if not exists retired (domain text not null, key text not null, public text not null, expired integer not null, primary key (domain, key))")
	rooms_tables(db)
	keyring_tables(db)
	clients_tables(db)
	signals_tables(db)
	content_tables(db)
	bridge_tables(db)
	notices_tables(db)
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
// Matrix domains: a domain it serves with a Matrix account on it, or one it
// keeps a group's stand-ins on.
func federation_local(name spec.ServerName) bool {
	domain := strings.ToLower(string(name))
	if domain == "" {
		return false
	}
	// The domain registrations are taken on, in development, is a Matrix
	// domain before anyone registers (clients.go).
	if clients_registration && domain == server_domain() {
		return true
	}
	found, _ := networks_database().exists("select 1 from actors where network='matrix' and domain=? and deleted=0", domain)
	if !found {
		// A domain the server keeps a group's stand-ins on (bridge.go).
		found, _ = federation_database().exists("select 1 from standins where domain=?", domain)
	}
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

// federation_domains are the Matrix domains this server speaks for: those of
// its accounts and those it keeps stand-ins on, each once.
func federation_domains() []spec.ServerName {
	rows, _ := networks_database().rows("select distinct domain from actors where network='matrix' and deleted=0")
	standins, _ := federation_database().rows("select distinct domain from standins")
	seen := map[string]bool{}
	var out []spec.ServerName
	// The domain registrations are taken on, in development, before anyone
	// registers (clients.go).
	if clients_registration && server_domain() != "" {
		seen[server_domain()] = true
		out = append(out, spec.ServerName(server_domain()))
	}
	for _, row := range append(rows, standins...) {
		if domain := strings.ToLower(row_string(row, "domain")); domain != "" && !seen[domain] {
			seen[domain] = true
			out = append(out, spec.ServerName(domain))
		}
	}
	return out
}

// federation_signing lists every Matrix domain's identity, for the client to
// sign with whichever a request comes from.
func federation_signing() []*fclient.SigningIdentity {
	var out []*fclient.SigningIdentity
	for _, domain := range federation_domains() {
		if id := federation_identity_of(domain); id != nil {
			out = append(out, &fclient.SigningIdentity{ServerName: id.domain, KeyID: id.key, PrivateKey: id.private})
		}
	}
	return out
}

// federation_key_replace gives a Matrix domain of this server a new signing
// key and answers its id. The key it replaces is retired: published among the
// domain's old keys with the moment it stopped signing, and never signed with
// again.
func federation_key_replace(name string) (gomatrixserverlib.KeyID, error) {
	domain := spec.ServerName(strings.ToLower(name))
	old := federation_identity_of(domain)
	if old == nil {
		return "", fmt.Errorf("not a Matrix domain of this server")
	}
	federation_database().exec("insert or ignore into retired (domain, key, public, expired) values (?, ?, ?, ?)", string(domain), string(old.key), base64.RawStdEncoding.EncodeToString(old.public), time.Now().UnixMilli())
	federation_identities_lock.Lock()
	networks_keys_lock.Lock()
	db_open("db/networks.db").exec("delete from keys where name=?", "matrix "+string(domain))
	networks_keys_lock.Unlock()
	delete(federation_identities, domain)
	federation_identities_lock.Unlock()
	id := federation_identity_of(domain)
	if id == nil {
		return "", fmt.Errorf("no new key")
	}
	return id.key, nil
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
	if u == nil || user_quiet(u) {
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

// federation_get sends a signed GET to another server from one of this
// server's domains, decoding the answer as it came.
func federation_get(ctx context.Context, local spec.ServerName, server string, path string, into any) error {
	return federation_signed(ctx, local, "GET", server, path, nil, into)
}

// federation_ask sends a signed request to another server from the domain of
// the user it is for, decoding the answer as it came.
func federation_ask(ctx context.Context, user string, method string, server string, path string, content any, into any) error {
	id, err := spec.NewUserID(user, true)
	if err != nil {
		return err
	}
	return federation_signed(ctx, id.Domain(), method, server, path, content, into)
}

// federation_signed sends a request to another server signed as one of this
// server's domains, with content where there is some, decoding the answer.
func federation_signed(ctx context.Context, local spec.ServerName, method string, server string, path string, content any, into any) error {
	identity := federation_identity_of(local)
	if identity == nil {
		return fmt.Errorf("this server does not speak for %s", local)
	}
	request := fclient.NewFederationRequest(method, identity.domain, spec.ServerName(server), path)
	if content != nil {
		if err := request.SetContent(content); err != nil {
			return err
		}
	}
	if err := request.Sign(identity.domain, identity.key, identity.private); err != nil {
		return err
	}
	prepared, err := request.HTTPRequest()
	if err != nil {
		return err
	}
	bounded, cancel := context.WithTimeout(ctx, federation_timeout)
	defer cancel()
	var raw json.RawMessage
	if err := federation_client().DoRequestAndParseResponse(bounded, prepared.WithContext(bounded), &raw); err != nil {
		return err
	}
	return json.Unmarshal(raw, into)
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
			} else if row, _ := db.row("select public, expired from retired where domain=? and key=?", string(id.domain), string(request.KeyID)); row != nil {
				if public, err := base64.RawStdEncoding.DecodeString(row_string(row, "public")); err == nil {
					out[request] = gomatrixserverlib.PublicKeyLookupResult{
						VerifyKey:    gomatrixserverlib.VerifyKey{Key: spec.Base64Bytes(public)},
						ExpiredTS:    spec.Timestamp(row_int(row, "expired")),
						ValidUntilTS: spec.Timestamp(row_int(row, "expired")),
					}
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

// federation_keys_retired are a domain's old keys as its key document lists
// them: each with when it stopped signing.
func federation_keys_retired(domain spec.ServerName) map[string]any {
	out := map[string]any{}
	rows, _ := federation_database().rows("select key, public, expired from retired where domain=?", string(domain))
	for _, row := range rows {
		out[row_string(row, "key")] = map[string]any{"key": row_string(row, "public"), "expired_ts": row_int(row, "expired")}
	}
	return out
}

// federation_key_answer is the signed document of a domain's keys.
func federation_key_answer(id *federation_identity) ([]byte, error) {
	document := map[string]any{
		"server_name":     string(id.domain),
		"valid_until_ts":  int64(spec.AsTimestamp(time.Now().Add(federation_key_validity))),
		"verify_keys":     map[string]any{string(id.key): map[string]any{"key": base64.RawStdEncoding.EncodeToString(id.public)}},
		"old_verify_keys": federation_keys_retired(id.domain),
	}
	unsigned, err := json.Marshal(document)
	if err != nil {
		return nil, err
	}
	return gomatrixserverlib.SignJSON(string(id.domain), id.key, id.private, unsigned)
}

// === Routes ===

// federation_methods are the methods each federation route the engine
// answers takes, by its version and first segment.
var federation_methods = map[string][]string{
	"v1/send": {"PUT"}, "v1/make_join": {"GET"}, "v1/send_join": {"PUT"}, "v2/send_join": {"PUT"},
	"v1/make_leave": {"GET"}, "v1/send_leave": {"PUT"}, "v2/send_leave": {"PUT"}, "v2/invite": {"PUT"},
	"v1/event": {"GET"}, "v1/state": {"GET"}, "v1/state_ids": {"GET"}, "v1/event_auth": {"GET"},
	"v1/get_missing_events": {"POST"}, "v1/backfill": {"GET"}, "v1/query": {"GET"},
	"v1/make_knock": {"GET"}, "v1/send_knock": {"PUT"}, "v1/hierarchy": {"GET"}, "v1/user": {"GET", "POST"},
	"v1/media": {"GET"}, "v1/publicRooms": {"GET", "POST"}, "v1/timestamp_to_event": {"GET"},
}

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
		if method != "GET" {
			federation_error(c, http.StatusMethodNotAllowed, "M_UNRECOGNIZED", "unrecognised request")
			return true
		}
		c.JSON(http.StatusOK, map[string]any{"server": map[string]any{"name": "Mochi", "version": build_version}})
		return true
	case strings.HasPrefix(path, "/_matrix/federation/"):
		if !federation_local(domain) {
			federation_error(c, http.StatusNotFound, "M_NOT_FOUND", "no Matrix server here")
			return true
		}
		// A path the engine has no route for is unknown whoever asks, and
		// one asked with another method is told so, before any signature is
		// checked, as Synapse answers them.
		segments := strings.Split(strings.TrimPrefix(path, "/_matrix/federation/"), "/")
		methods, known := []string(nil), false
		if len(segments) >= 2 {
			methods, known = federation_methods[segments[0]+"/"+segments[1]]
		}
		if !known {
			federation_error(c, http.StatusNotFound, "M_UNRECOGNIZED", "unrecognised request")
			return true
		}
		if !slices.Contains(methods, method) {
			federation_error(c, http.StatusMethodNotAllowed, "M_UNRECOGNIZED", "unrecognised request")
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
	// A room's server access list is applied to the server asking at the
	// endpoints of its events and memberships, as the specification lists
	// them; a transaction's events are checked one by one (rooms_receive).
	switch route {
	case "GET v1/make_join", "PUT v1/send_join", "PUT v2/send_join", "GET v1/make_leave", "PUT v1/send_leave", "PUT v2/send_leave", "PUT v2/invite",
		"GET v1/make_knock", "PUT v1/send_knock", "GET v1/state", "GET v1/state_ids", "GET v1/event_auth", "POST v1/get_missing_events", "GET v1/backfill":
		if !rooms_acl_allows(segment(2), r.Origin()) {
			federation_error(c, http.StatusForbidden, "M_FORBIDDEN", "the room's server access list denies this server")
			return
		}
	}
	// Someone the owner blocks joins and knocks on no room here.
	if (route == "GET v1/make_join" || route == "GET v1/make_knock") && server_blocked(segment(3)) {
		federation_error(c, http.StatusForbidden, "M_FORBIDDEN", "the user is blocked")
		return
	}
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
		rooms_serve_alias(c, c.Query("room_alias"))
	case route == "GET v1/make_knock" && len(parts) == 4:
		rooms_make_knock(c, r, segment(2), segment(3))
	case route == "PUT v1/send_knock" && len(parts) == 4:
		rooms_send_knock(c, r, segment(2), segment(3))
	case route == "GET v1/timestamp_to_event" && len(parts) == 3:
		rooms_serve_nearest(c, r, segment(2))
	case route == "GET v1/hierarchy" && len(parts) == 3:
		rooms_serve_hierarchy(c, r, segment(2))
	case route == "GET v1/user" && segment(2) == "devices" && len(parts) == 4:
		keyring_serve_devices(c, segment(3))
	case route == "POST v1/user" && segment(2) == "keys":
		switch segment(3) {
		case "query":
			keyring_serve_query(c, r)
		case "claim":
			keyring_serve_claim(c, r)
		default:
			federation_error(c, http.StatusNotFound, "M_UNRECOGNIZED", "unrecognised request")
		}
	case route == "GET v1/media" && (segment(2) == "download" || segment(2) == "thumbnail") && len(parts) == 4:
		content_serve(c, r, segment(3))
	case route == "GET v1/publicRooms" || route == "POST v1/publicRooms":
		rooms_serve_published(c, r)
	default:
		federation_error(c, http.StatusNotFound, "M_UNRECOGNIZED", "unrecognised request")
	}
}

// === Transactions in ===

// federation_receive_transaction takes a transaction from another server:
// each room event is processed in turn and answered for, then each ephemeral
// one (signals.go). A transaction already taken is answered at once.
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
		// A fault in taking one event is that event's failure alone, as
		// Synapse answers any exception in one, and the rest are still taken.
		var id string
		var err error
		guard("Matrix event from "+origin, func() {
			id, err = rooms_identify(raw), fmt.Errorf("the event could not be taken")
		}, func() {
			id, err = rooms_receive(c.Request.Context(), r.Origin(), raw)
		})
		if id == "" {
			continue
		}
		// An event taken and kept as rejected was handled; only one that
		// could not be is an error.
		if rejection := (rooms_rejection{}); err != nil && !errors.As(err, &rejection) {
			results[id] = map[string]any{"error": federation_reason(err)}
		} else {
			results[id] = map[string]any{}
		}
	}
	var out rooms_outbox
	for _, raw := range t.EDUs {
		signals_receive(r.Origin(), raw, &out)
	}
	out.flush()
	db.exec("insert or ignore into transactions (origin, id, received) values (?, ?, ?)", origin, id, now())
	db.exec("delete from transactions where received<?", now()-7*86400)
	c.JSON(http.StatusOK, map[string]any{"pdus": results})
}

// federation_reason is why something failed, as the server it is told to
// reads it: where another server refused a request on the way, its own code
// and message in place of its answer's raw bytes.
func federation_reason(err error) string {
	var remote gomatrix.HTTPError
	if !errors.As(err, &remote) {
		return err.Error()
	}
	return strings.Replace(err.Error(), remote.Error(), federation_refusal(remote), 1)
}

// federation_refusal is what another server's refusal says: its code and
// message, or the status it answered where it gave neither.
func federation_refusal(remote gomatrix.HTTPError) string {
	var answer gomatrix.RespError
	if json.Unmarshal(remote.Contents, &answer) == nil && answer.ErrCode != "" {
		return answer.ErrCode + ": " + answer.Err
	}
	return fmt.Sprintf("the other server answered %d", remote.Code)
}

// === Transactions out ===

// federation_workers is each destination with a worker running, and what
// tells it more was queued.
var federation_workers = map[string]chan struct{}{}
var federation_workers_lock sync.Mutex

// federation_send queues a room event for each server given, other than this
// one's own domains and the server of its sender, which has it already: sent
// from its sender's domain where that is this server's, else from the domain
// this server speaks for the room as.
func federation_send(destinations []string, event gomatrixserverlib.PDU) {
	user, err := spec.NewUserID(string(event.SenderID()), true)
	if err != nil {
		return
	}
	origin := string(user.Domain())
	if !federation_local(user.Domain()) {
		origin = string(rooms_local(event.RoomID().String()))
	}
	var others []string
	for _, destination := range destinations {
		if !strings.EqualFold(destination, string(user.Domain())) {
			others = append(others, destination)
		}
	}
	federation_queue(others, origin, "pdu", event.EventID())
}

// federation_signal queues an ephemeral event - typing, a read receipt,
// presence - from one of this server's domains for each server given. One a
// server has not taken within ten minutes is dropped, since it says nothing
// worth knowing late.
func federation_signal(destinations []string, origin string, kind string, content any) {
	federation_queue(destinations, origin, "edu", json_encode(map[string]any{"edu_type": kind, "content": content}))
}

// federation_message queues an ephemeral event that must not be lost - an
// event sent to a device, a change to someone's devices or keys - for each
// server given: kept as long as a room event is.
func federation_message(destinations []string, origin string, kind string, content any) {
	federation_queue(destinations, origin, "message", json_encode(map[string]any{"edu_type": kind, "content": content}))
}

// federation_queue adds a row for each destination and wakes its worker.
func federation_queue(destinations []string, origin string, kind string, body string) {
	if origin == "" {
		return
	}
	db := federation_database()
	for _, destination := range destinations {
		if destination == "" || federation_local(spec.ServerName(destination)) {
			continue
		}
		db.exec("insert into outbox (destination, origin, kind, body, created) values (?, ?, ?, ?, ?)", destination, origin, kind, body, now())
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
// fifty room events and a hundred ephemeral ones, all from one domain of this
// server, until it is empty, waiting out its backoff after a failure; more
// queued meanwhile has it look again, but not before its backoff ends. What
// has waited a week for a server that never answers is dropped.
func federation_worker(destination string, wake chan struct{}) {
	db := federation_database()
	for {
		db.exec("delete from outbox where destination=? and (created<? or (kind='edu' and created<?) or origin='')", destination, now()-7*86400, now()-600)
		if row, _ := db.row("select retry from destinations where name=?", destination); row != nil {
			if wait := row_int(row, "retry") - now(); wait > 0 {
				select {
				case <-time.After(time.Duration(min(wait, 3600)) * time.Second):
				case <-wake:
				}
				continue
			}
		}
		first, _ := db.row("select origin from outbox where destination=? order by id limit 1", destination)
		if first == nil {
			if federation_worker_done(destination, wake) {
				return
			}
			continue
		}
		origin := row_string(first, "origin")
		// A row this server cannot sign for would hold up the rest.
		if !federation_local(spec.ServerName(origin)) {
			db.exec("delete from outbox where destination=? and origin=?", destination, origin)
			continue
		}
		var ids []any
		var pdus []json.RawMessage
		rows, _ := db.rows("select id, body from outbox where destination=? and origin=? and kind='pdu' order by id limit 50", destination, origin)
		for _, row := range rows {
			ids = append(ids, row["id"])
			if event := rooms_event(row_string(row, "body")); event != nil {
				pdus = append(pdus, event.JSON())
			}
		}
		var edus []gomatrixserverlib.EDU
		rows, _ = db.rows("select id, body from outbox where destination=? and origin=? and kind in ('edu', 'message') order by id limit 100", destination, origin)
		for _, row := range rows {
			ids = append(ids, row["id"])
			var edu gomatrixserverlib.EDU
			if json.Unmarshal([]byte(row_string(row, "body")), &edu) == nil {
				edu.Origin, edu.Destination = origin, destination
				edus = append(edus, edu)
			}
		}
		err := error(nil)
		// A transaction of ephemeral events alone still lists its room events,
		// as none: servers refuse one without the list.
		if pdus == nil {
			pdus = []json.RawMessage{}
		}
		if len(pdus)+len(edus) > 0 {
			ctx, cancel := context.WithTimeout(context.Background(), federation_timeout+10*time.Second)
			_, err = federation_client().SendTransaction(ctx, gomatrixserverlib.Transaction{
				TransactionID:  gomatrixserverlib.TransactionID(uid()),
				Origin:         spec.ServerName(origin),
				Destination:    spec.ServerName(destination),
				OriginServerTS: spec.AsTimestamp(time.Now()),
				PDUs:           pdus,
				EDUs:           edus,
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
		// An event kept only to be sent, of a room no one here is in any
		// longer, goes once every server has it (rooms_forget).
		db.exec("delete from events where room in (select id from rooms where state=-1) and id not in (select body from outbox where kind='pdu')")
	}
}

// federation_log carries the library's warnings to the debug log.
type federation_log struct{}

func (federation_log) Write(line []byte) (int, error) {
	debug("Matrix: %s", strings.TrimSpace(string(line)))
	return len(line), nil
}

// federation_manager starts a worker for every destination with events
// waiting at startup; federation_send starts the rest. Each day what waited
// past its time for the key store's devices goes (keyring.go).
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
	go func() {
		for range time.Tick(24 * time.Hour) {
			keyring_prune()
		}
	}()
	go func() {
		for range time.Tick(time.Minute) {
			clients_presence_lapse()
		}
	}()
}

// Mochi server: Other networks
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// The networks Mochi can host accounts on - the fediverse (ActivityPub, which
// Mastodon and Lemmy speak), Bluesky, XMPP and Matrix - each behind a system
// setting the owner may switch off, and all of them behind the development
// switch in mochi.conf that keeps the code dormant until every part is ready.
// A protocol's engine, the Networks page and the apps all ask here whether a
// network is on; with the switch off, or a setting off, nothing of that network
// is offered or answered and Mochi behaves as it did before any of it existed.

package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

// network_names are the networks, in the order the settings and the Networks
// page list them. Each is also the name of its system setting.
var network_names = []string{"fediverse", "bluesky", "xmpp", "matrix"}

// networks_development is the [development] networks switch in mochi.conf. Off
// by default: every engine, page and provider for the networks stays dormant
// until the whole of the federation work is ready, when the switch is removed.
var networks_development bool

// network_known reports whether a name is one of the networks.
func network_known(name string) bool {
	for _, n := range network_names {
		if n == name {
			return true
		}
	}
	return false
}

// network_enabled reports whether a network is on: the development switch is
// on and the owner has not switched the network's setting off.
func network_enabled(name string) bool {
	return networks_development && network_known(name) && setting_effective(name) == "true"
}

// networks_enabled lists the networks that are on, in their standard order.
func networks_enabled() []string {
	var on []string
	for _, n := range network_names {
		if network_enabled(n) {
			on = append(on, n)
		}
	}
	return on
}

// server_public_url is the address people and other servers reach this server
// at: the configured public URL, else https on the configured domain, else
// empty where the server has no public address.
func server_public_url() string {
	if base := strings.TrimRight(setting_effective("oauth_public_url"), "/"); base != "" {
		return base
	}
	if domain := server_domain(); domain != "" {
		return "https://" + domain
	}
	return ""
}

// domain_addresses are the addresses other servers reach a domain at, from
// what this machine's own lookup of it found. A loopback address there comes
// from the hosts file naming the machine itself - Debian and Ubuntu map the
// machine's name to 127.0.1.1 - and stands for the addresses on the machine's
// interfaces. A development instance that reaches private addresses
// ([development] private) talks to servers on the same machine, which reach
// it at the loopback address too.
func domain_addresses(found []string) []string {
	var out []string
	local := false
	for _, address := range found {
		if ip := net.ParseIP(address); ip != nil && ip.IsLoopback() {
			local = true
			if url_allow_private {
				out = append(out, address)
			}
			continue
		}
		out = append(out, address)
	}
	if local {
		out = append(out, domain_interfaces()...)
	}
	return out
}

// domain_interfaces lists the addresses on this machine's interfaces, other
// than loopback and link-local ones. A variable so tests can give their own.
var domain_interfaces = func() []string {
	addresses, err := net.InterfaceAddrs()
	if err != nil {
		return nil
	}
	var out []string
	for _, a := range addresses {
		network, ok := a.(*net.IPNet)
		if !ok || network.IP.IsLoopback() || network.IP.IsLinkLocalUnicast() {
			continue
		}
		out = append(out, network.IP.String())
	}
	return out
}

// mochi.server.url() -> string: The server's public address, https on its
// configured domain, or empty where it has none - what a link back to a page
// here is built from
func api_server_url(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	return sl.String(server_public_url()), nil
}

// network_address is the address a hosted account has on its network: the
// handle (with a suffix name's separator written as the network wants it) on
// the domain. The fediverse and XMPP write user@domain, Matrix @user:domain,
// and Bluesky makes the handle a host name under the domain.
func network_address(network string, name string, domain string) string {
	switch network {
	case "fediverse":
		// The form every fediverse server writes an address in, and the one
		// its people paste into a search box; WebFinger's acct: drops the
		// leading @ and the engine builds that from the handle itself.
		return "@" + name + "@" + domain
	case "xmpp":
		return name + "@" + domain
	case "matrix":
		return "@" + name + ":" + domain
	case "bluesky":
		return handle_bluesky(name) + "." + domain
	}
	return ""
}

// networks_setting reports whether a system setting belongs to the networks:
// the handle limit, the server description and the four protocol switches,
// which exist for the owner only once the development switch is on.
func networks_setting(name string) bool {
	return name == "handles" || name == "description" || name == "labelers" || network_known(name)
}

// network_state is an enabled network's state for the owner: unavailable with
// the reason "domain" while the server has no configured domain, since a
// hosted account needs a public address; otherwise working. Each engine adds
// its own checks here as it is built.
func network_state(name string) (string, string) {
	if server_domain() == "" && len(domain_list()) == 0 {
		return "unavailable", "domain"
	}
	if name == "xmpp" {
		if reason := xmpp_unavailable(); reason != "" {
			return "unavailable", reason
		}
	}
	return "working", ""
}

// mochi.server.networks() -> dict: Each network's state for the owner's Status
// page: "working", "unavailable" with a reason, or "disabled" (the setting or
// the development switch off); for Bluesky how many certificates the server
// has had issued this week against the authority's allowance and how much of
// the relay's hourly and daily budget of events it has used; for Matrix each
// domain's signing key; for XMPP each domain's room service, as {host, state,
// reason}, unavailable ("dns") where its name does not lead to this server;
// and whether the development switch is on.
func api_server_networks(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := require_permission(t, fn, "server/read"); err != nil {
		return sl_error(fn, "%v", err)
	}
	networks := map[string]any{}
	for _, n := range network_names {
		state := map[string]any{"state": "disabled", "reason": ""}
		if network_enabled(n) {
			state["state"], state["reason"] = network_state(n)
		}
		if n == "bluesky" {
			state["certificates"] = map[string]any{"issued": certificates_recent(), "limit": certificates_limit}
			hour, day := atproto_budget_used()
			state["budget"] = map[string]any{
				"hour": map[string]any{"used": hour, "limit": atproto_budget_hour},
				"day":  map[string]any{"used": day, "limit": atproto_budget_day},
			}
			// The server's standing with the relay: whether it follows this
			// server, and how many accounts it admits.
			if network_enabled(n) {
				if relay := atproto_relay_status(thread_context(t)); relay != nil {
					state["relay"] = relay
				}
			}
		}
		// Each Matrix domain's signing key, which the owner may replace.
		if n == "matrix" {
			keys := []any{}
			for _, domain := range federation_domains() {
				if id := federation_identity_of(domain); id != nil {
					keys = append(keys, map[string]any{"domain": string(domain), "key": string(id.key)})
				}
			}
			state["keys"] = keys
		}
		// Each XMPP domain's room service, unavailable where its name does not
		// lead to this server.
		if n == "xmpp" && network_enabled(n) {
			state["rooms"] = xmpp_rooms_state(thread_context(t))
		}
		networks[n] = state
	}
	return sl_encode(map[string]any{
		"development": networks_development,
		"networks":    networks,
	}), nil
}

// === Certificates ===
//
// Each Bluesky handle under the server's domain is a host name with its own
// certificate, and the authority issues a limited number of new certificates
// for one registered domain in a week (letsencrypt.org/docs/rate-limits, read
// 2026-10-04: 50, refilling one every 202 minutes), renewals exempt. The
// certificate cache (domains_cache) records each certificate issued for a new
// name so the owner's Status page can show the week's count.

const certificates_limit = 50
const certificates_window = 7 * 24 * time.Hour

// certificates_record notes a certificate newly issued for a host name.
func certificates_record(host string) {
	networks_database().exec("insert into certificates (host, issued) values (?, ?)", host, time.Now().Unix())
}

// certificates_recent counts the certificates issued in the last week.
func certificates_recent() int {
	row, _ := networks_database().row("select count(*) as issued from certificates where issued>?", time.Now().Add(-certificates_window).Unix())
	return int(row_int(row, "issued"))
}

// mochi.account.networks() -> list: The networks a user may host accounts on,
// in their standard order: those the owner has on, when the development switch
// is on. Empty when networks are not offered on this server.
func api_account_networks(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if principal_caller(t) == nil {
		return sl_error(fn, "no user")
	}
	on := networks_enabled()
	sort.SliceStable(on, func(i, j int) bool { return network_index(on[i]) < network_index(on[j]) })
	return sl_encode(on), nil
}

// network_index is a network's place in the standard order.
func network_index(name string) int {
	for i, n := range network_names {
		if n == name {
			return i
		}
	}
	return len(network_names)
}

// === Hosted accounts ===
//
// A hosted account is a row of the user's accounts table (accounts.go) whose
// type is a network and whose data carries hosted, handle and domain. Its id
// is its permanent identifier; its identifier column is its address.

// account_hosted returns a user's hosted account on a network for a handle, or
// nil.
func account_hosted(user *User, network string, domain string, name string) map[string]any {
	for _, row := range accounts_hosted(user, network) {
		data, _ := row["data"].(map[string]any)
		if data["domain"] == domain && data["handle"] == name {
			return row
		}
	}
	return nil
}

// accounts_hosted lists a user's hosted accounts, on one network or on every
// network when network is empty, each with its data decoded.
func accounts_hosted(user *User, network string) []map[string]any {
	var hosted []map[string]any
	for _, row := range accounts_network(user) {
		ptype, _ := row["type"].(string)
		if !account_hosted_flag(row) || (network != "" && ptype != network) {
			continue
		}
		hosted = append(hosted, row)
	}
	return hosted
}

// account_network_get returns a user's account on a network by its id, hosted
// or linked, with its data decoded, or nil.
func account_network_get(user *User, id string) map[string]any {
	for _, row := range accounts_network(user) {
		if row["id"] == id {
			return row
		}
	}
	return nil
}

// accounts_hosted_remove deletes a user's hosted accounts on a handle, on
// every network: the handle is being released or freed.
func accounts_hosted_remove(user *User, domain string, name string) {
	db := db_user(user, "user")
	for _, row := range accounts_hosted(user, "") {
		data, _ := row["data"].(map[string]any)
		if data["domain"] == domain && data["handle"] == name {
			id, _ := row["id"].(string)
			account_hosted_gone(user, row)
			if ptype := row_string(row, "type"); ptype == "matrix" || ptype == "xmpp" {
				encryption_forget(user, row)
			}
			keypairs_delete(db, id)
			db.row_remove(reg_accounts, map[string]any{"id": id})
		}
	}
}

// account_hosted_gone tells a hosted account's network it is gone, which
// cannot be taken back: the fediverse by a Delete of the actor to its
// followers' servers, Bluesky by tombstoning its identity, Matrix by its
// leaving every room it is in.
func account_hosted_gone(user *User, row map[string]any) {
	id := row_string(row, "id")
	activitypub_account_remove(user, id)
	switch row_string(row, "type") {
	case "bluesky":
		atproto_account_remove(user, id)
	case "matrix":
		address := row_string(row, "identifier")
		for _, room := range signals_rooms(address) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := rooms_leave(ctx, address, room); err != nil {
				info("Matrix %s leaving %s on going: %v", address, room, err)
			}
			cancel()
		}
	}
}

// account_hosted_get returns a user's hosted account by its id, or nil.
func account_hosted_get(user *User, id string) map[string]any {
	for _, row := range accounts_hosted(user, "") {
		if row["id"] == id {
			return row
		}
	}
	return nil
}

// account_url is a hosted account's permanent id on the fediverse,
// https://<domain>/~<id> (decision 40): the id, never the handle, so a released
// and reused handle never collides with the old account.
func account_url(domain string, id string) string {
	return "https://" + strings.ToLower(domain) + "/~" + id
}

// account_actor is a hosted account's id on its network, by the account's id,
// or empty when the user has no such account.
func account_actor(user *User, id string) string {
	row := account_hosted_get(user, id)
	if row == nil {
		return ""
	}
	data, _ := row["data"].(map[string]any)
	domain, _ := data["domain"].(string)
	if domain == "" {
		return ""
	}
	return account_url(domain, id)
}

// === Hosts ===
//
// What takes the place of per-host consent for a hosted account is a block
// list, since that is how these networks work: the owner blocks a host for the
// whole server, a user blocks one for themselves, and the owner's protocol
// setting turns a network off. Every request a network makes passes
// host_allowed and the limiter per destination host, and in the dialer the
// private-address check; a delivery past the limiter waits for the next window.

// rate_limit_host bounds the requests to one remote host from every account and
// app together: 60 a minute, the budget Mastodon gives one address. A variable
// so tests can lower it.
var rate_limit_host = &rate_limiter{entries: make(map[string]*rate_limit_entry), limit: 60, window: 60}

// network_timeout bounds one request to another server.
const network_timeout = 30 * time.Second

// block_domain_re is the shape of a blocked domain: lowercase labels, two or
// more.
var block_domain_re = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?(\.[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)+$`)

// networks_database opens the server-wide networks database, whose tables
// networks_create lays.
func networks_database() *DB {
	return db_open("db/networks.db")
}

// networks_create lays the server-wide networks database, for db_create and
// db_upgrade_18: the delivery queue (whose rows carry, in address, the actor
// they are for where its inbox was not known when queued), the hosts it has
// learned about, the owner's domain blocks, Mochi's registrations as a client
// on other servers, the server's own keys as a client, and the clients
// registered to sign users in with their own tokens. A change here is also a
// new db_upgrade_N step; db_upgrade_18 lays its own copy of this as it was
// then (db_upgrade_18_networks).
func networks_create(db *DB) {
	db.exec("create table if not exists deliveries (id text not null primary key, user text not null, account text not null, network text not null, inbox text not null, host text not null, body text not null, attempts integer not null default 0, next integer not null, claimed integer not null default 0, created integer not null, address text not null default '')")
	db.exec("create index if not exists deliveries_next on deliveries(next)")
	db.exec("create index if not exists deliveries_host on deliveries(host)")
	db.exec("create table if not exists hosts (host text not null primary key, failures integer not null default 0, success integer not null default 0, failure integer not null default 0, next integer not null default 0, error text not null default '')")
	db.exec("create table if not exists blocks (domain text not null primary key, created integer not null)")
	db.exec("create table if not exists clients (host text not null, network text not null, id text not null, secret text not null, redirect text not null default '', created integer not null, primary key (host, network))")
	db.exec("create table if not exists keys (name text not null primary key, algorithm text not null, public text not null, private text not null, created integer not null)")
	// The clients registered to sign users in, and the tokens of their own
	// (signin.go).
	db.exec("create table if not exists applications (id text not null primary key, secret text not null default '', name text not null default '', website text not null default '', redirects text not null default '[]', scopes text not null default '', network text not null, created integer not null)")
	db.exec("create table if not exists tokens (hash text not null primary key, client text not null, scopes text not null default '', created integer not null)")
	// The certificates issued for new host names, for the week's count.
	db.exec("create table if not exists certificates (host text not null, issued integer not null)")
	db.exec("create index if not exists certificates_issued on certificates(issued)")
	// The hosted accounts by permanent id, for a request that arrives with
	// no user; the activity ids the inbox has seen; and what the inbox queues
	// for the apps (activitypub.go).
	db.exec("create table if not exists actors (id text not null primary key, user text not null, domain text not null, handle text not null, network text not null, created integer not null, deleted integer not null default 0, key text not null default '')")
	db.exec("create index if not exists actors_user on actors(user)")
	// By domain and handle, for the lookups of an account by its address
	// (federation_local, federation_user, xmpp_served, reports).
	db.exec("create index if not exists actors_domain on actors(domain, handle)")
	// The activity ids seen by each account.
	db.exec("create table if not exists seen (id text not null, account text not null, created integer not null, primary key (id, account))")
	db.exec("create index if not exists seen_created on seen(created)")
	db.exec("create table if not exists inbound (id text not null primary key, user text not null, account text not null, actor text not null, type text not null, body text not null, created integer not null)")
	db.exec("create index if not exists inbound_account on inbound(account, created)")
	// The Bluesky identities by did and the update stream (atproto.go).
	atproto_tables(db)
	// Subscriptions to web feeds and to this server's, and the mentions
	// waiting (websub.go, webmention.go).
	websub_tables(db)
	webmention_tables(db)
	// The reports for the administrators and the people the owner blocks
	// (reports.go).
	report_tables(db)
	// The image store's uploads (lemmy_server.go).
	lemmy_tables(db)
}

var networks_keys_lock sync.Mutex

// networks_key is one of the server's own keys by name, made on first use with
// the algorithm given and kept in networks.db: the private half, PEM.
func networks_key(name string, algorithm string) string {
	networks_keys_lock.Lock()
	defer networks_keys_lock.Unlock()
	db := db_open("db/networks.db")
	if row, _ := db.row("select private from keys where name=?", name); row != nil {
		return row_string(row, "private")
	}
	public, private, err := key_generate(algorithm)
	if err != nil {
		warn("Cannot make the %s key: %v", name, err)
		return ""
	}
	db.exec("insert into keys (name, algorithm, public, private, created) values (?, ?, ?, ?, ?)", name, algorithm, public, private, now())
	return private
}

// A host_refusal is why the host policy refused a host: the network is off, or
// the owner or the user blocks the host.
type host_refusal struct {
	Host   string
	Reason string
	Off    bool
}

func (e *host_refusal) Error() string {
	return e.Reason
}

// host_allowed says whether this server may contact host on a network for a
// user: the network is on, and neither the owner nor the user blocks the host
// or a domain above it. The network is empty for a lookup any network makes, the
// user nil for a request on the server's own behalf.
func host_allowed(user *User, network string, host string) error {
	host = strings.ToLower(host)
	if host == "" {
		return &host_refusal{Host: host, Reason: "no host"}
	}
	if network != "" && !network_enabled(network) {
		return &host_refusal{Host: host, Reason: network + " is off", Off: true}
	}
	if blocked_by(db_open("db/networks.db"), host) {
		return &host_refusal{Host: host, Reason: host + " is blocked on this server"}
	}
	if user != nil && blocked_by(db_user(user, "user"), host) {
		return &host_refusal{Host: host, Reason: host + " is blocked"}
	}
	return nil
}

// blocked_by says whether a blocks table names host or a domain above it.
func blocked_by(db *DB, host string) bool {
	for h := host; h != ""; {
		if found, _ := db.exists("select 1 from blocks where domain=?", h); found {
			return true
		}
		i := strings.IndexByte(h, '.')
		if i < 0 {
			return false
		}
		h = h[i+1:]
	}
	return false
}

// network_base is the address a host is reached at: https, or http for a
// loopback host where the development switch admits private addresses, so a
// server on this machine can stand in for a remote one under test.
func network_base(host string) string {
	if url_allow_private {
		name := host
		if h, _, err := net.SplitHostPort(host); err == nil {
			name = h
		}
		if name == "127.0.0.1" || name == "localhost" || name == "::1" {
			return "http://" + host
		}
	}
	return network_scheme + host
}

// network_target checks one URL against the host policy: HTTPS, not the cloud
// metadata service, and host_allowed.
func network_target(user *User, network string, u *url.URL) error {
	if url_is_cloud_metadata(u.String()) {
		return &host_refusal{Host: u.Hostname(), Reason: "blocked outbound request to the cloud metadata service"}
	}
	if u.Scheme != "https" && !url_allow_private {
		return &host_refusal{Host: u.Hostname(), Reason: u.String() + " is not https"}
	}
	return host_allowed(user, network, u.Hostname())
}

// network_request sends a request to another server for a user under the host
// policy: network_target on the request and on each redirect, up to redirects
// of them, the limiter per host, and the private-address check in the dialer on
// every hop. The user may be nil; a *host_refusal or a *RateLimitError says why
// a request was not made.
func network_request(ctx context.Context, user *User, network string, r *http.Request, redirects int) (*http.Response, error) {
	return network_send(ctx, user, network, strings.ToLower(r.URL.Hostname()), r, redirects)
}

// network_send is network_request with the limiter's key given: a linked
// account's requests count against a budget of their own on its server, which
// is how that server limits them, rather than the host's. A context with a
// deadline bounds the request instead of network_timeout.
func network_send(ctx context.Context, user *User, network string, key string, r *http.Request, redirects int) (*http.Response, error) {
	if err := network_target(user, network, r.URL); err != nil {
		return nil, err
	}
	host := strings.ToLower(r.URL.Hostname())
	// The limiter spares other servers; a verified domain of this server's
	// own is none, and a community and its members here talk through it.
	if !domain_served(host) && !rate_limit_host.allow(key) {
		return nil, rate_limit_refuse(rate_limit_host, key, "requests per minute to "+host)
	}
	if r.Header.Get("User-Agent") == "" {
		r.Header.Set("User-Agent", "Mochi/"+build_version)
	}
	client := &http.Client{Timeout: network_timeout, Transport: url_transport}
	if _, bounded := ctx.Deadline(); bounded {
		client.Timeout = 0
	}
	client.CheckRedirect = func(request *http.Request, via []*http.Request) error {
		if len(via) >= redirects {
			return fmt.Errorf("too many redirects")
		}
		return network_target(user, network, request.URL)
	}
	return client.Do(r)
}

// === Domain blocks ===

var api_account_block = sls.FromStringDict(sl.String("mochi.account.block"), sl.StringDict{
	"add":    sl.NewBuiltin("mochi.account.block.add", api_account_block_add),
	"list":   sl.NewBuiltin("mochi.account.block.list", api_account_block_list),
	"remove": sl.NewBuiltin("mochi.account.block.remove", api_account_block_remove),
})

var api_server_block = sls.FromStringDict(sl.String("mochi.server.block"), sl.StringDict{
	"add":    sl.NewBuiltin("mochi.server.block.add", api_server_block_add),
	"check":  sl.NewBuiltin("mochi.server.block.check", api_server_block_check),
	"list":   sl.NewBuiltin("mochi.server.block.list", api_server_block_list),
	"remove": sl.NewBuiltin("mochi.server.block.remove", api_server_block_remove),
})

// blocks_list reads a blocks table, oldest first.
func blocks_list(db *DB) []map[string]any {
	rows, _ := db.rows("select domain, created from blocks order by created")
	if rows == nil {
		rows = []map[string]any{}
	}
	return rows
}

// block_add adds a domain to a blocks table.
func block_add(db *DB, domain string) error {
	domain = strings.ToLower(strings.TrimSpace(domain))
	if !block_domain_re.MatchString(domain) {
		return fmt.Errorf("invalid domain")
	}
	db.exec("insert or ignore into blocks (domain, created) values (?, ?)", domain, now())
	return nil
}

// block_remove removes a domain from a blocks table.
func block_remove(db *DB, domain string) {
	db.exec("delete from blocks where domain=?", strings.ToLower(strings.TrimSpace(domain)))
}

// block_call checks a block API's permission and returns the caller.
func block_call(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, permission string, syntax string) (*User, string, error) {
	domain := ""
	if syntax == "" {
		if len(args) != 0 {
			return nil, "", fmt.Errorf("syntax: no arguments")
		}
	} else {
		if len(args) != 1 {
			return nil, "", fmt.Errorf("syntax: %s", syntax)
		}
		ok := false
		if domain, ok = sl.AsString(args[0]); !ok || domain == "" {
			return nil, "", fmt.Errorf("invalid domain")
		}
	}
	if err := require_permission(t, fn, permission); err != nil {
		return nil, "", err
	}
	user := principal_caller(t)
	if user == nil {
		return nil, "", fmt.Errorf("no user")
	}
	return user, domain, nil
}

// mochi.account.block.list() -> list: What the user has blocked: domains, each
// with the domain and when, then people, each with the person's id and when.
// No account of the user sends to, follows on or fetches from a blocked domain
// or one under it, whatever an app is granted; a blocked person's activities
// are taken and dropped, and they are no longer a follower.
func api_account_block_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user, _, err := block_call(t, fn, args, "accounts/read", "")
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	db := db_user(user, "user")
	return sl_encode(append(blocks_list(db), actor_list(db, "blocked")...)), nil
}

// mochi.account.block.add(target) -> None: Block a domain, or a person by their
// id on their network, for the user.
func api_account_block_add(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user, target, err := block_call(t, fn, args, "accounts/write", "<target: string>")
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	if actor, ok := actor_target(target); ok {
		blocked_add(user, actor)
		return sl.None, nil
	}
	if err := block_add(db_user(user, "user"), target); err != nil {
		return sl_error(fn, "%v", err)
	}
	return sl.None, nil
}

// mochi.account.block.remove(target) -> None: Unblock a domain or a person for
// the user.
func api_account_block_remove(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user, target, err := block_call(t, fn, args, "accounts/write", "<target: string>")
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	db := db_user(user, "user")
	if actor, ok := actor_target(target); ok {
		blocked_remove(user, actor)
		return sl.None, nil
	}
	block_remove(db, target)
	return sl.None, nil
}

// mochi.server.block.list() -> list: What the owner has blocked for the whole
// server: domains, each with the domain and when, then people, each with the
// person's id and when. No account on this server sends to, follows on or
// fetches from a blocked domain or one under it; a blocked person's
// activities, events and stanzas are taken and dropped, and they are no longer
// anyone's follower.
func api_server_block_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if _, _, err := block_call(t, fn, args, "server/read", ""); err != nil {
		return sl_error(fn, "%v", err)
	}
	db := networks_database()
	return sl_encode(append(blocks_list(db), actor_list(db, "blocked")...)), nil
}

// mochi.server.key.replace(domain) -> string: Replace the Matrix signing key of one of the server's domains and return the new key's id. The old key is published among the domain's old keys, so what it signed stays verifiable. Requires settings/write
func api_server_key_replace(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	_, domain, err := block_call(t, fn, args, "settings/write", "<domain: string>")
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	key, err := federation_key_replace(domain)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	return sl.String(key), nil
}

// mochi.server.block.check(target) -> string: Whether the owner can block a target: "" if so, "invalid" for neither a domain nor a person on a network, "local" for a person or a domain of this server's own. Requires server/read
func api_server_block_check(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	_, target, err := block_call(t, fn, args, "server/read", "<target: string>")
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	return sl.String(block_refusal(target)), nil
}

// mochi.server.block.add(target) -> None: Block a domain for the whole server,
// or a person by their id on their network: a fediverse id, a Bluesky did, a
// Matrix user id or an XMPP address as xmpp:name@host. A person of this
// server's own is suspended instead.
func api_server_block_add(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	_, target, err := block_call(t, fn, args, "settings/write", "<target: string>")
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	if actor, ok := block_target(target); ok {
		if err := server_blocked_add(actor); err != nil {
			return sl_error(fn, "%v", err)
		}
		return sl.None, nil
	}
	if block_refusal(target) == "local" {
		return sl_error(fn, "a domain of this server's own is not blocked")
	}
	if err := block_add(networks_database(), target); err != nil {
		return sl_error(fn, "%v", err)
	}
	return sl.None, nil
}

// mochi.server.block.remove(target) -> None: Unblock a domain or a person for
// the whole server.
func api_server_block_remove(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	_, target, err := block_call(t, fn, args, "settings/write", "<target: string>")
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	db := networks_database()
	if actor, ok := block_target(target); ok {
		db.exec("delete from blocked where actor=?", actor)
		return sl.None, nil
	}
	block_remove(db, target)
	return sl.None, nil
}

// Mochi server: AT Protocol engine
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// The Bluesky side of a hosted account: this server as the account's data
// server. It answers the repository interface under /xrpc/ - the records, the
// repository as an archive, its blobs, its latest commit - and the update
// stream relays subscribe to, serves each handle's identifier at its host,
// registers the account's did:plc identity when the account is made (plc.go)
// and writes its profile into its repository (repository.go). An app
// publishes through mochi.account.object.create as it does to the fediverse,
// with the object already a record; core gives it its key, commits it and
// paces the commit under the relay's budget. Off with the bluesky setting.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/bluesky-social/indigo/atproto/atdata"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/gin-gonic/gin"
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
	"nhooyr.io/websocket"
)

// bluesky_relay is the relay told of this server's repositories; a
// development setting points it at one on this machine.
var bluesky_relay = "https://bsky.network"

// atproto_endpoint_override names this server's repository endpoint in place
// of https on the account's domain: a development setting for a reference
// relay on this machine, which reaches only localhost.
var atproto_endpoint_override = ""

// atproto_lookup resolves a host name to its addresses, for the check that
// a handle's host reaches this server. A variable so tests need no DNS.
var atproto_lookup = func(ctx context.Context, host string) ([]string, error) {
	return net.DefaultResolver.LookupHost(ctx, host)
}

// stream_age is how long the update stream keeps events for replay: the
// relay's own window is a day; three days covers a relay that was away.
var stream_age int64 = 3 * 86400

// atproto_engine is the engine's registration: the repository interface, the
// handle's identifier and the interface signed-in apps use (xrpc.go), whose
// blobs and imported repositories are the largest bodies; each method reads
// no more than its own bound. Off with the bluesky setting.
func atproto_engine() *engine {
	return &engine{
		name:     "atproto",
		prefixes: []string{"/xrpc/", "/.well-known/atproto-did", "/.well-known/did.json"},
		setting:  "bluesky",
		body:     xrpc_import_maximum,
		handler:  atproto_route,
	}
}

// atproto_tables lays the server-wide tables the engine keeps in networks.db,
// part of networks_create: the identities by did, for a request that names no
// user, and the update stream.
func atproto_tables(db *DB) {
	db.exec("create table if not exists identities (did text not null primary key, account text not null, user text not null, created integer not null)")
	// A relay resumes from the last sequence it read, so a number is never
	// given twice: AUTOINCREMENT keeps the last one even when the replay
	// window has emptied the table.
	db.exec("create table if not exists stream (sequence integer not null primary key autoincrement, did text not null, kind text not null, body blob not null, created integer not null)")
	db.exec("create index if not exists stream_created on stream(created)")
}

// atproto_stream_rebuild moves a stream table made before its sequence was
// AUTOINCREMENT onto one that is, keeping its events; their highest number
// becomes the last given. Run once, by db_upgrade_18.
func atproto_stream_rebuild(db *DB) {
	row, _ := db.row("select sql from sqlite_master where type='table' and name='stream'")
	if row == nil || strings.Contains(strings.ToLower(row_string(row, "sql")), "autoincrement") {
		return
	}
	db.exec("drop table if exists stream_rebuilt")
	db.exec("create table stream_rebuilt (sequence integer not null primary key autoincrement, did text not null, kind text not null, body blob not null, created integer not null)")
	db.exec("insert into stream_rebuilt (sequence, did, kind, body, created) select sequence, did, kind, body, created from stream")
	db.exec("drop table stream")
	db.exec("alter table stream_rebuilt rename to stream")
	db.exec("create index if not exists stream_created on stream(created)")
}

// atproto_endpoint is the address other services reach an account's
// repository at: the development override, else https on the account's
// domain.
func atproto_endpoint(domain string) string {
	if atproto_endpoint_override != "" {
		return strings.TrimSuffix(atproto_endpoint_override, "/")
	}
	return "https://" + strings.ToLower(domain)
}

// atproto_cut keeps the first n characters of a string.
func atproto_cut(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	runes := []rune(s)
	return strings.TrimSpace(string(runes[:n]))
}

// === Identities ===

// atproto_register records an account's identifier for requests that arrive
// with no user.
func atproto_register(did string, account string, user *User) {
	networks_database().exec("insert into identities (did, account, user, created) values (?, ?, ?, ?) on conflict(did) do update set account=excluded.account, user=excluded.user", did, account, user.UID, now())
}

// atproto_identity resolves an identifier to its user, account row and
// repository, or nil for one this server does not hold.
func atproto_identity(did string) (*User, map[string]any, *repository) {
	row, _ := networks_database().row("select account, user from identities where did=?", did)
	if row == nil {
		return nil, nil, nil
	}
	user := user_by_uid(row_string(row, "user"))
	if user == nil || user_quiet(user) {
		return nil, nil, nil
	}
	account := account_hosted_get(user, row_string(row, "account"))
	if account == nil {
		return nil, nil, nil
	}
	return user, account, repository_get(user, row_string(row, "account"))
}

// domain_owner is the user who runs a domain, through the route at its root,
// or nil.
func domain_owner(name string) *User {
	row, _ := db_open("db/domains.db").row("select owner from routes where domain=? and path=''", strings.ToLower(name))
	if row == nil {
		return nil
	}
	return user_by_uid(row_string(row, "owner"))
}

// atproto_host_account finds the hosted Bluesky account whose handle is a
// host: a name under a domain, through the handle registry, or a domain a
// user runs whose account took the domain itself as its handle.
func atproto_host_account(host string) (*User, map[string]any) {
	host = strings.ToLower(host)
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	if owner := domain_owner(host); owner != nil {
		for _, row := range accounts_hosted(owner, "bluesky") {
			data, _ := row["data"].(map[string]any)
			if apex, _ := data["apex"].(bool); apex && data["domain"] == host {
				return owner, row
			}
		}
	}
	i := strings.IndexByte(host, '.')
	if i <= 0 {
		return nil, nil
	}
	h := handle_lookup(host[i+1:], handle_normalise(host[:i]))
	if h == nil {
		return nil, nil
	}
	holder := user_by_uid(h.User)
	if holder == nil {
		return nil, nil
	}
	row := account_hosted(holder, "bluesky", h.Domain, h.Name)
	if row == nil {
		return nil, nil
	}
	return holder, row
}

// atproto_handle_base is the domain a hosted Bluesky account's handle host
// is under, which its certificate is asked under (domain_certified), or ""
// for a host that is no account's handle, or with Bluesky off.
func atproto_handle_base(host string) string {
	if !network_enabled("bluesky") {
		return ""
	}
	_, row := atproto_host_account(host)
	if row == nil {
		return ""
	}
	domain, _ := account_data(row)["domain"].(string)
	return domain
}

// atproto_repository resolves what a request names a repository by, an
// identifier or a handle, to its user, account and repository.
func atproto_repository(name string) (*User, map[string]any, *repository) {
	name = strings.TrimSpace(name)
	if strings.HasPrefix(name, "did:") {
		return atproto_identity(name)
	}
	user, row := atproto_host_account(name)
	if user == nil {
		return nil, nil, nil
	}
	id, _ := row["id"].(string)
	return user, row, repository_get(user, id)
}

// atproto_did is a hosted account's identifier, from its data.
func atproto_did(row map[string]any) string {
	did, _ := account_data(row)["did"].(string)
	return did
}

// atproto_apex_taken says whether one of a user's accounts on a domain
// already took the domain itself as its handle.
func atproto_apex_taken(user *User, domain string) bool {
	for _, row := range accounts_hosted(user, "bluesky") {
		data, _ := row["data"].(map[string]any)
		if apex, _ := data["apex"].(bool); apex && data["domain"] == domain {
			return true
		}
	}
	return false
}

// atproto_reaches checks that a handle's host reaches this server before
// Bluesky is switched on for it: the host resolves to an address the
// account's domain is reached at, which catches a missing wildcard record and
// a clashing one.
func atproto_reaches(ctx context.Context, host string, domain string) error {
	if host == domain {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	addresses, err := atproto_lookup(ctx, host)
	if err != nil || len(addresses) == 0 {
		return refuse(http.StatusConflict, "errors.host_unresolved", map[string]any{"host": host, "domain": domain})
	}
	own, err := atproto_lookup(ctx, domain)
	if err != nil || len(own) == 0 {
		return refuse(http.StatusConflict, "errors.domain_unresolved", map[string]any{"domain": domain})
	}
	own = domain_addresses(own)
	for _, a := range addresses {
		for _, b := range own {
			if a == b {
				return nil
			}
		}
	}
	return refuse(http.StatusConflict, "errors.host_elsewhere", map[string]any{"host": host, "addresses": strings.Join(addresses, ", "), "domain": domain, "own": strings.Join(own, ", ")})
}

// === Accounts ===

// atproto_account_create switches Bluesky on for a hosted account just made
// (accounts.go): checks the handle's host reaches this server, registers the
// identity, makes the repository with the account's profile in it and tells
// the relay. On a user's own domain the first account takes the domain
// itself as its handle. An error leaves the caller to undo the account.
func atproto_account_create(ctx context.Context, user *User, id string) error {
	row := account_hosted_get(user, id)
	if row == nil {
		return fmt.Errorf("no such account")
	}
	data := account_data(row)
	handle, _ := data["handle"].(string)
	domain, _ := data["domain"].(string)
	address := network_address("bluesky", handle, domain)
	apex := false
	if domain != server_domain() && domain_owned(user, domain) && !atproto_apex_taken(user, domain) {
		apex = true
		address = domain
	}
	if err := atproto_reaches(ctx, address, domain); err != nil {
		return err
	}
	did, err := plc_create(ctx, user, id, address, atproto_endpoint(domain))
	if err != nil {
		return err
	}
	data["did"] = did
	if apex {
		data["apex"] = true
	}
	encoded, _ := json.Marshal(data)
	db := db_user(user, "user")
	db.account_set(id, map[string]any{"data": string(encoded), "identifier": address})
	atproto_register(did, id, user)
	r := repository_create(user, id, did)

	stream_append(did, "#identity", map[string]any{"did": did, "handle": address})
	stream_append(did, "#account", map[string]any{"did": did, "active": true})
	row = account_hosted_get(user, id)
	record := atproto_profile_record(ctx, user, row)
	if _, _, err := repository_write(ctx, r, "app.bsky.actor.profile", "self", record, false); err != nil {
		return err
	}
	go atproto_crawl(domain)
	return nil
}

// atproto_account_remove ends a hosted Bluesky account: the stream says it is
// deleted, the identity is tombstoned where the directory answers, and the
// repository goes with its blobs.
func atproto_account_remove(user *User, id string) {
	row := account_hosted_get(user, id)
	if row == nil {
		return
	}
	did := atproto_did(row)
	if did != "" {
		stream_append(did, "#account", map[string]any{"did": did, "active": false, "status": "deleted"})
		ctx, cancel := context.WithTimeout(context.Background(), plc_timeout)
		defer cancel()
		if err := plc_tombstone(ctx, user, id, did); err != nil {
			warn("Bluesky account %s: identity not tombstoned: %v", did, err)
		}
		networks_database().exec("delete from identities where did=?", did)
	}
	repository_delete(user, id)
}

// atproto_profile_media asks the app that describes people (people, through
// profile/media) for the person's avatar and banner as bytes, each
// {data, type}, for the profile record's blobs.
func atproto_profile_media(user *User) map[string]any {
	person := ""
	if identity := user.identity(); identity != nil {
		person = identity.ID
	}
	for _, a := range apps_for_function(user, "profile/media") {
		result, err := activitypub_call(user, a, "profile/media", Map{"person": person})
		if err != nil {
			debug("Profile media from %s: %v", a.id, err)
			continue
		}
		if media, ok := result.(map[string]any); ok {
			return media
		}
	}
	return map[string]any{}
}

// atproto_profile_pinned is the post an account's profile pins, as the
// reference the record carries: the first of the posts the apps name through
// atproto/featured, newest pin first, that is a post in the account's own
// repository. Bluesky pins one post; nil when there is none.
func atproto_profile_pinned(user *User, id string, did string) map[string]any {
	prefix := "at://" + did + "/app.bsky.feed.post/"
	for _, a := range apps_for_function(user, "atproto/featured") {
		result, err := activitypub_call(user, a, "atproto/featured", Map{"account": id})
		if err != nil {
			debug("Bluesky featured from %s: %v", a.id, err)
			continue
		}
		for _, uri := range object_strings(result) {
			rkey, found := strings.CutPrefix(uri, prefix)
			if !found {
				continue
			}
			if c, _ := repository_record(user, id, "app.bsky.feed.post", rkey); c != "" {
				return map[string]any{"uri": uri, "cid": c}
			}
		}
	}
	return nil
}

// atproto_profile_record builds an account's profile record from the name it
// has, the person's profile text, their avatar and banner, stored as blobs of
// the account, and the post the apps pin.
func atproto_profile_record(ctx context.Context, user *User, row map[string]any) map[string]any {
	id, _ := row["id"].(string)
	profile := activitypub_profile(user, id)
	name, _ := row["label"].(string)
	if name == "" {
		name, _ = profile["name"].(string)
	}
	if name == "" {
		if identity := user.identity(); identity != nil {
			name = identity.Name
		}
	}
	record := map[string]any{"$type": "app.bsky.actor.profile", "displayName": atproto_cut(name, 64), "createdAt": time.Unix(row_int(row, "created"), 0).UTC().Format(time.RFC3339)}
	if summary, _ := profile["summary"].(string); summary != "" {
		if text := atproto_cut(html_text(summary), 256); text != "" {
			record["description"] = text
		}
	}
	media := atproto_profile_media(user)
	for _, slot := range []string{"avatar", "banner"} {
		item, _ := media[slot].(map[string]any)
		if item == nil {
			continue
		}
		data := repository_bytes(item["data"])
		kind, _ := item["type"].(string)
		if len(data) == 0 || !strings.HasPrefix(kind, "image/") {
			continue
		}
		blob, err := repository_blob_store(user, id, data, kind)
		if err != nil {
			debug("Profile %s of %s: %v", slot, id, err)
			continue
		}
		record[slot] = repository_blob_reference(blob, kind, len(data))
	}
	if pinned := atproto_profile_pinned(user, id, atproto_did(row)); pinned != nil {
		record["pinnedPost"] = pinned
	}
	return record
}

// atproto_profile_write puts the account's profile as it stands into its
// repository: after the account is renamed, or the person's profile changes.
func atproto_profile_write(ctx context.Context, user *User, id string) error {
	row := account_hosted_get(user, id)
	r := repository_get(user, id)
	if row == nil || r == nil {
		return fmt.Errorf("no such account")
	}
	_, _, err := repository_write(ctx, r, "app.bsky.actor.profile", "self", atproto_profile_record(ctx, user, row), false)
	return err
}

// atproto_keys_replaced announces an account's new signing key: the identity
// names it, a commit signed with it goes out, and the stream says the
// identity changed so relays read it again.
func atproto_keys_replaced(ctx context.Context, user *User, id string) error {
	row := account_hosted_get(user, id)
	r := repository_get(user, id)
	if row == nil || r == nil {
		return fmt.Errorf("no such account")
	}
	did := atproto_did(row)
	signing := keypair_get(user, id, key_signing)
	if did == "" || signing == nil {
		return fmt.Errorf("account has no identity")
	}
	if err := plc_update(ctx, user, id, did, func(op map[string]any) {
		op["verificationMethods"] = map[string]any{"atproto": signing.Public}
	}); err != nil {
		return err
	}
	if _, err := repository_commit(ctx, r, nil); err != nil {
		return err
	}
	stream_append(did, "#identity", map[string]any{"did": did, "handle": row_string(row, "identifier")})
	return nil
}

// atproto_recovery_added puts the recovery key the user has just made above
// the server's own among the identity's rotation keys.
func atproto_recovery_added(ctx context.Context, user *User, id string) error {
	row := account_hosted_get(user, id)
	if row == nil || atproto_did(row) == "" {
		return fmt.Errorf("account has no identity")
	}
	return plc_update(ctx, user, id, atproto_did(row), func(op map[string]any) {})
}

// atproto_notified is when a commit on each domain last asked the relays to
// crawl it: at most every atproto_notify_interval, as Bluesky's own data
// server asks. A relay that gave the server up as offline - after a restart
// it did not wait out - follows it again only when asked, so without this a
// server's posts would stop reaching the network for good; the first commit
// after a start always asks.
var atproto_notified = map[string]time.Time{}
var atproto_notified_lock sync.Mutex

const atproto_notify_interval = 20 * time.Minute

// atproto_notify asks the relays to crawl a domain when it is due.
func atproto_notify(domain string) {
	if domain == "" {
		return
	}
	atproto_notified_lock.Lock()
	last, found := atproto_notified[domain]
	due := !found || time.Since(last) >= atproto_notify_interval
	if due {
		atproto_notified[domain] = time.Now()
	}
	atproto_notified_lock.Unlock()
	if due {
		go atproto_crawl(domain)
	}
}

// atproto_crawl asks the relay to read this server's repositories, once an
// account is made on a domain and as commits are made (atproto_notify). Best effort: a relay that already knows the
// host, or refuses, costs nothing but a line in the log.
func atproto_crawl(domain string) {
	host := atproto_host(domain)
	if host == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), plc_timeout)
	defer cancel()
	body := strings.NewReader(json_encode(map[string]any{"hostname": host}))
	request, err := http.NewRequestWithContext(ctx, "POST", strings.TrimSuffix(bluesky_relay, "/")+"/xrpc/com.atproto.sync.requestCrawl", body)
	if err != nil {
		return
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := network_request(ctx, nil, "", request, 0)
	if err != nil {
		debug("Relay %s: crawl of %s not requested: %v", bluesky_relay, host, err)
		return
	}
	defer response.Body.Close()
	answer, _ := read_limited(response.Body, 4096)
	debug("Relay %s: crawl of %s requested: %d %s", bluesky_relay, host, response.StatusCode, strings.TrimSpace(string(answer)))
}

// atproto_host is the host name a relay knows a domain's repositories by, or
// empty.
func atproto_host(domain string) string {
	endpoint, err := url.Parse(atproto_endpoint(domain))
	if err != nil {
		return ""
	}
	return endpoint.Host
}

// atproto_crawl_all asks the relay to read every host this server keeps
// Bluesky repositories on: the server's own domain, where a first account is
// made, and each domain an account is on. At startup and when the owner
// switches Bluesky on, so the relay is listening before a first account's
// events go out rather than asked to after them.
func atproto_crawl_all() {
	if !network_enabled("bluesky") {
		return
	}
	domains := map[string]bool{}
	if domain := server_domain(); domain != "" {
		domains[domain] = true
	}
	for _, a := range atproto_active() {
		domains[a.domain] = true
	}
	for domain := range domains {
		atproto_crawl(domain)
	}
}

// atproto_held is a hosted Bluesky account whose repository this server
// serves as active.
type atproto_held struct {
	did        string
	domain     string
	identifier string
	r          *repository
}

// atproto_active lists the hosted Bluesky accounts that are active, with a
// commit made: the ones a relay should hold.
func atproto_active() []atproto_held {
	rows, _ := networks_database().rows("select did from identities")
	var out []atproto_held
	for _, row := range rows {
		did := row_string(row, "did")
		_, account, r := atproto_identity(did)
		if account == nil || r == nil || r.head == "" || atproto_status(account) != "" {
			continue
		}
		domain, _ := account_data(account)["domain"].(string)
		out = append(out, atproto_held{did: did, domain: domain, identifier: row_string(account, "identifier"), r: r})
	}
	return out
}

// atproto_repair announces again each active account the relay does not hold.
// Its first events went out before the relay was listening - a crawl is asked
// for only once an account is made - or while the relay was away, and nothing
// after them told it. Asked host by host, so a host whose every account the
// relay holds costs one request; on a host it is short of, each account is
// asked after, and the host is asked to be crawled. The announcement goes out
// only while a relay is listening, since one connecting later starts after it;
// the next pass announces once it is.
func atproto_repair(ctx context.Context) {
	if !network_enabled("bluesky") {
		return
	}
	hosts := map[string][]atproto_held{}
	for _, a := range atproto_active() {
		host := atproto_host(a.domain)
		hosts[host] = append(hosts[host], a)
	}
	for host, accounts := range hosts {
		if atproto_relay_count(ctx, host) >= len(accounts) {
			continue
		}
		atproto_crawl(accounts[0].domain)
		if !stream_listened() {
			continue
		}
		for _, a := range accounts {
			if held, known := atproto_relay_holds(ctx, a.did); known && !held {
				atproto_announce(a)
			}
		}
	}
}

// atproto_repair_delay is how long after a relay subscribes from the present
// the accounts it may have missed are looked for: time for it to settle in.
var atproto_repair_delay = time.Minute

var atproto_repair_timer *time.Timer
var atproto_repair_lock sync.Mutex

// atproto_repair_soon looks for missed accounts shortly, once however many
// relays subscribe meanwhile.
func atproto_repair_soon() {
	atproto_repair_lock.Lock()
	defer atproto_repair_lock.Unlock()
	if atproto_repair_timer != nil {
		return
	}
	atproto_repair_timer = time.AfterFunc(atproto_repair_delay, func() {
		atproto_repair_lock.Lock()
		atproto_repair_timer = nil
		atproto_repair_lock.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		atproto_repair(ctx)
	})
}

// atproto_announce puts an account on the stream again as it stands: its
// identity, that it is active, and its latest commit as a #sync, from which a
// relay that lacks it reads its repository.
func atproto_announce(a atproto_held) {
	blocks, err := repository_head(a.r)
	if err != nil {
		debug("Bluesky account %s not announced: %v", a.did, err)
		return
	}
	stream_append(a.did, "#identity", map[string]any{"did": a.did, "handle": a.identifier})
	stream_append(a.did, "#account", map[string]any{"did": a.did, "active": true})
	stream_append(a.did, "#sync", map[string]any{"did": a.did, "blocks": atdata.Bytes(blocks), "rev": a.r.rev})
	info("Bluesky account %s announced again: the relay did not hold it", a.did)
}

// atproto_relay_get asks the relay one of its sync methods, answering the
// status and the decoded answer.
func atproto_relay_get(ctx context.Context, method string, query url.Values) (int, map[string]any, error) {
	request, err := http.NewRequestWithContext(ctx, "GET", strings.TrimSuffix(bluesky_relay, "/")+"/xrpc/"+method+"?"+query.Encode(), nil)
	if err != nil {
		return 0, nil, err
	}
	response, err := network_request(ctx, nil, "", request, 0)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	data, _ := read_limited(response.Body, 65536)
	var answer map[string]any
	json.Unmarshal(data, &answer)
	return response.StatusCode, answer, nil
}

// atproto_relay_count is how many of a host's accounts the relay holds, or -1
// when it does not say.
func atproto_relay_count(ctx context.Context, host string) int {
	status, answer, err := atproto_relay_get(ctx, "com.atproto.sync.getHostStatus", url.Values{"hostname": {host}})
	if err != nil || status != http.StatusOK {
		return -1
	}
	count, ok := answer["accountCount"].(float64)
	if !ok {
		return -1
	}
	return int(count)
}

// atproto_relay_holds says whether the relay holds an account, and whether it
// said either way.
func atproto_relay_holds(ctx context.Context, did string) (bool, bool) {
	status, answer, err := atproto_relay_get(ctx, "com.atproto.sync.getRepoStatus", url.Values{"did": {did}})
	switch {
	case err != nil:
		return false, false
	case status == http.StatusOK:
		return true, true
	case answer["error"] == "RepoNotFound":
		return false, true
	}
	return false, false
}

// === Publishing ===

// request_remembered answers what an account's request under a key was
// answered with, if it was made: a post is made once however often an app
// asks with the same key.
func request_remembered(user *User, account string, key string) map[string]any {
	if key == "" {
		return nil
	}
	row, _ := db_user(user, "user").row("select result from requests where account=? and key=?", account, key)
	if row == nil {
		return nil
	}
	var result map[string]any
	json.Unmarshal([]byte(row_string(row, "result")), &result)
	return result
}

// request_remember keeps a key's answer for a day, and drops the day-old
// ones.
func request_remember(user *User, account string, key string, result map[string]any) {
	if key == "" {
		return
	}
	db := db_user(user, "user")
	db.exec("delete from requests where created<?", now()-86400)
	db.exec("insert or replace into requests (account, key, result, created) values (?, ?, ?, ?)", account, key, json_encode(result), now())
}

// atproto_result shapes what a record write answers for an app: the record's
// URI as its id, its page on Bluesky for a post, its content id, and whether
// the commit went out or waits for the budget.
func atproto_result(did string, collection string, rkey string, c string, committed bool) map[string]any {
	uri := "at://" + did + "/" + collection + "/" + rkey
	result := bluesky_result(map[string]any{"uri": uri, "cid": c}, did)
	result["data"] = map[string]any{"uri": uri, "cid": c, "committed": committed}
	return result
}

// atproto_create writes a record to a hosted account's repository: the object
// is the record, with $type naming its collection (app.bsky.feed.post when
// absent) and createdAt filled in; sourced says the write may wait for the
// budget. Once per key.
func atproto_create(ctx context.Context, user *User, row map[string]any, object map[string]any, key string, sourced bool) (map[string]any, error) {
	id, _ := row["id"].(string)
	if result := request_remembered(user, id, key); result != nil {
		return result, nil
	}
	if atproto_deactivated(row) {
		return nil, fmt.Errorf("account is deactivated")
	}
	r := repository_get(user, id)
	if r == nil {
		return nil, fmt.Errorf("account has no repository")
	}
	collection, _ := object["$type"].(string)
	if collection == "" {
		collection = "app.bsky.feed.post"
		object["$type"] = collection
	}
	if _, err := syntax.ParseNSID(collection); err != nil {
		return nil, fmt.Errorf("invalid collection %q", collection)
	}
	if _, set := object["createdAt"]; !set {
		object["createdAt"] = time.Now().UTC().Format(time.RFC3339Nano)
	}
	// A generator record names this server as the service unless the app
	// names another.
	if collection == "app.bsky.feed.generator" {
		if did, _ := object["did"].(string); did == "" {
			object["did"] = atproto_generator_did()
		}
	}
	rkey := repository_clock.Next().String()
	c, committed, err := repository_write(ctx, r, collection, rkey, object, sourced)
	if err != nil {
		return nil, err
	}
	result := atproto_result(r.did, collection, rkey, c, committed)
	request_remember(user, id, key, result)
	return result, nil
}

// atproto_record_of reads an at:// URI naming a record of the account.
func atproto_record_of(r *repository, uri string) (string, string, error) {
	did, collection, rkey, err := bluesky_uri(uri)
	if err != nil {
		return "", "", err
	}
	if did != r.did {
		return "", "", fmt.Errorf("the record is not the account's")
	}
	return collection, rkey, nil
}

// atproto_update writes a new version of a record the account holds.
func atproto_update(ctx context.Context, user *User, row map[string]any, uri string, object map[string]any) (map[string]any, error) {
	if atproto_deactivated(row) {
		return nil, fmt.Errorf("account is deactivated")
	}
	id, _ := row["id"].(string)
	r := repository_get(user, id)
	if r == nil {
		return nil, fmt.Errorf("account has no repository")
	}
	collection, rkey, err := atproto_record_of(r, uri)
	if err != nil {
		return nil, err
	}
	if _, set := object["$type"]; !set {
		object["$type"] = collection
	}
	c, committed, err := repository_write(ctx, r, collection, rkey, object, false)
	if err != nil {
		return nil, err
	}
	return atproto_result(r.did, collection, rkey, c, committed), nil
}

// atproto_delete deletes a record the account holds.
func atproto_delete(ctx context.Context, user *User, row map[string]any, uri string) error {
	if atproto_deactivated(row) {
		return fmt.Errorf("account is deactivated")
	}
	id, _ := row["id"].(string)
	r := repository_get(user, id)
	if r == nil {
		return fmt.Errorf("account has no repository")
	}
	collection, rkey, err := atproto_record_of(r, uri)
	if err != nil {
		return err
	}
	_, err = repository_remove(ctx, r, collection, rkey, false)
	return err
}

// atproto_upload stores a blob for a hosted account and answers the
// reference a record names it by, as the linked account's upload does.
func atproto_upload(user *User, row map[string]any, data []byte, kind string) (map[string]any, error) {
	id, _ := row["id"].(string)
	did := atproto_did(row)
	if did == "" {
		return nil, fmt.Errorf("account has no identity")
	}
	blob, err := repository_blob_store(user, id, data, kind)
	if err != nil {
		return nil, err
	}
	domain, _ := account_data(row)["domain"].(string)
	return map[string]any{"id": blob, "url": bluesky_blob(atproto_endpoint(domain), did, blob), "data": repository_blob_reference(blob, kind, len(data))}, nil
}

// === The update stream ===
//
// Every event this server sends - a commit, an identity or account change -
// is a row of the stream in networks.db with the server-wide sequence number
// relays resume from, kept for stream_age. A frame is the event's header and
// body as DAG-CBOR, one after the other, sent as one binary message.

var stream_lock sync.Mutex
var stream_subscribers = map[chan struct{}]bool{}

// stream_frame encodes an event for the wire.
func stream_frame(kind string, body map[string]any) ([]byte, error) {
	header, err := atdata.MarshalCBOR(map[string]any{"op": int64(1), "t": kind})
	if err != nil {
		return nil, err
	}
	encoded, err := atdata.MarshalCBOR(body)
	if err != nil {
		return nil, err
	}
	return append(header, encoded...), nil
}

// stream_error encodes an error frame, which ends a subscription.
func stream_error(name string, message string) []byte {
	header, _ := atdata.MarshalCBOR(map[string]any{"op": int64(-1)})
	body, _ := atdata.MarshalCBOR(map[string]any{"error": name, "message": message})
	return append(header, body...)
}

// stream_append gives an event the next sequence number, keeps its frame and
// wakes the subscribers. Answers the sequence.
func stream_append(did string, kind string, body map[string]any) int64 {
	stream_lock.Lock()
	defer stream_lock.Unlock()
	db := networks_database()
	// The frame carries its number, so it is chosen here: after the last
	// given, which the table keeps even when it holds no events, and never
	// below the time in milliseconds, so a server restored from an older
	// backup goes on past every number it gave before instead of giving them
	// again, which a relay holding a cursor past them would never read; the
	// relay then finds each repository's commits no longer follow on from
	// the one it holds, and reads the repository afresh.
	last, _ := db.row("select max(coalesce((select max(sequence) from stream), 0), coalesce((select seq from sqlite_sequence where name='stream'), 0)) as last")
	sequence := max(row_int(last, "last")+1, stream_clock())
	body["seq"] = sequence
	if _, set := body["time"]; !set {
		body["time"] = time.Now().UTC().Format(time.RFC3339Nano)
	}
	frame, err := stream_frame(kind, body)
	if err != nil {
		warn("Stream event %s not encoded: %v", kind, err)
		return 0
	}
	db.exec("insert into stream (sequence, did, kind, body, created) values (?, ?, ?, ?, ?)", sequence, did, kind, frame, now())
	for ch := range stream_subscribers {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	return sequence
}

// stream_clock is the floor stream_append numbers events from: the time in
// milliseconds. A variable so tests set the time.
var stream_clock = func() int64 { return time.Now().UnixMilli() }

// stream_last is the sequence of the last event, zero before any.
func stream_last() int64 {
	row, _ := networks_database().row("select coalesce(max(sequence), 0) as last from stream")
	return row_int(row, "last")
}

// stream_after reads up to limit frames after a sequence, in order.
func stream_after(sequence int64, limit int) []map[string]any {
	rows, _ := networks_database().rows("select sequence, body from stream where sequence>? order by sequence limit ?", sequence, limit)
	return rows
}

// stream_subscribe registers for wakeups on new events.
func stream_subscribe() chan struct{} {
	ch := make(chan struct{}, 1)
	stream_lock.Lock()
	stream_subscribers[ch] = true
	stream_lock.Unlock()
	return ch
}

// stream_unsubscribe ends the wakeups.
func stream_unsubscribe(ch chan struct{}) {
	stream_lock.Lock()
	delete(stream_subscribers, ch)
	stream_lock.Unlock()
}

// stream_listened says whether a relay is subscribed now.
func stream_listened() bool {
	stream_lock.Lock()
	defer stream_lock.Unlock()
	return len(stream_subscribers) > 0
}

// stream_cleanup drops events past the replay window.
func stream_cleanup() {
	networks_database().exec("delete from stream where created<?", now()-stream_age)
}

// atproto_subscribe serves com.atproto.sync.subscribeRepos: the frames after
// the cursor given, else from now, then each new one as it happens. A cursor
// ahead of the stream is answered with an error frame, as the protocol says.
func atproto_subscribe(c *gin.Context) {
	cursor := int64(-1)
	if text := c.Query("cursor"); text != "" {
		value, err := strconv.ParseInt(text, 10, 64)
		if err != nil || value < 0 {
			atproto_error(c, http.StatusBadRequest, "InvalidRequest")
			return
		}
		cursor = value
	}
	ws, err := websocket.Accept(c.Writer, c.Request, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	ctx, cancel := context.WithCancel(c.Request.Context())
	defer cancel()
	defer ws.Close(websocket.StatusNormalClosure, "")
	last := stream_last()
	if cursor > last {
		ws.Write(ctx, websocket.MessageBinary, stream_error("FutureCursor", "the cursor is ahead of the stream"))
		return
	}
	position := last
	if cursor >= 0 {
		position = cursor
	}
	wake := stream_subscribe()
	defer stream_unsubscribe(wake)
	// A relay starting from the present has missed whatever went before: an
	// account made before it was listening is looked for once it settles.
	if cursor < 0 {
		atproto_repair_soon()
	}
	// A relay sends nothing; reading tells when it has gone.
	go func() {
		for {
			if _, _, err := ws.Read(ctx); err != nil {
				cancel()
				return
			}
		}
	}()
	keepalive := time.NewTicker(30 * time.Second)
	defer keepalive.Stop()
	for {
		rows := stream_after(position, 100)
		for _, row := range rows {
			if err := ws.Write(ctx, websocket.MessageBinary, repository_bytes(row["body"])); err != nil {
				return
			}
			position = row_int(row, "sequence")
		}
		if len(rows) == 100 {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-wake:
		case <-keepalive.C:
			if err := ws.Ping(ctx); err != nil {
				return
			}
		}
	}
}

// === Routing ===

// atproto_error answers an XRPC error: the protocol's own name for it, which
// clients read, not a sentence for a person.
func atproto_error(c *gin.Context, status int, name string) {
	c.JSON(status, map[string]any{"error": name}) // i18n-ok: XRPC error names are protocol tokens, not text for people
}

// atproto_route dispatches one request on the engine's paths.
func atproto_route(c *gin.Context) bool {
	path, method := c.Request.URL.Path, c.Request.Method
	c.Header("Access-Control-Allow-Origin", "*")
	if method == "OPTIONS" {
		c.Header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Accept, Accept-Language, Content-Type, Authorization, DPoP, Atproto-Proxy, Atproto-Accept-Labelers")
		c.Header("Access-Control-Expose-Headers", "DPoP-Nonce, WWW-Authenticate, Atproto-Content-Labelers")
		c.AbortWithStatus(http.StatusNoContent)
		return true
	}
	debug("AT Protocol %s %s from %s (%s)", method, path, c.ClientIP(), c.Request.UserAgent())
	if path == "/.well-known/atproto-did" || path == "/.well-known/did.json" {
		if method != "GET" {
			atproto_error(c, http.StatusMethodNotAllowed, "InvalidRequest")
			return true
		}
		if path == "/.well-known/did.json" {
			atproto_generator_document(c)
		} else {
			atproto_well_known(c)
		}
		return true
	}
	name := strings.TrimPrefix(path, "/xrpc/")
	// What a signed-in app asks of its data server; the methods the engine
	// serves to the network itself are answered below.
	if !atproto_served[name] && xrpc_dispatch(c, name) {
		return true
	}
	if method != "GET" {
		atproto_error(c, http.StatusMethodNotAllowed, "InvalidRequest")
		return true
	}
	switch name {
	case "app.bsky.feed.getFeedSkeleton":
		atproto_skeleton(c)
	case "app.bsky.feed.describeFeedGenerator":
		atproto_describe_generator(c)
	case "com.atproto.server.describeServer":
		atproto_describe_server(c)
	case "com.atproto.identity.resolveHandle":
		atproto_resolve_handle(c)
	case "com.atproto.repo.describeRepo":
		atproto_describe_repo(c)
	case "com.atproto.repo.getRecord":
		atproto_get_record(c)
	case "com.atproto.repo.listRecords":
		atproto_list_records(c)
	case "com.atproto.sync.getRepo":
		atproto_sync_repo(c)
	case "com.atproto.sync.getLatestCommit":
		atproto_sync_commit(c)
	case "com.atproto.sync.getRepoStatus":
		atproto_sync_status(c)
	case "com.atproto.sync.getRecord":
		atproto_sync_record(c)
	case "com.atproto.sync.getBlob":
		atproto_sync_blob(c)
	case "com.atproto.sync.listBlobs":
		atproto_sync_blobs(c)
	case "com.atproto.sync.listRepos":
		atproto_sync_repos(c)
	case "com.atproto.sync.subscribeRepos":
		atproto_subscribe(c)
	default:
		atproto_error(c, http.StatusNotImplemented, "MethodNotImplemented")
	}
	return true
}

// atproto_served are the methods the engine answers anyone: the feed
// generator's, which the index asks, and the reads of repositories.
var atproto_served = map[string]bool{
	"app.bsky.feed.getFeedSkeleton": true, "app.bsky.feed.describeFeedGenerator": true,
	"com.atproto.server.describeServer": true, "com.atproto.repo.describeRepo": true, "com.atproto.repo.getRecord": true, "com.atproto.repo.listRecords": true,
	"com.atproto.sync.getRepo": true, "com.atproto.sync.getLatestCommit": true, "com.atproto.sync.getRepoStatus": true, "com.atproto.sync.getRecord": true,
	"com.atproto.sync.getBlob": true, "com.atproto.sync.listBlobs": true, "com.atproto.sync.listRepos": true, "com.atproto.sync.subscribeRepos": true,
	"com.atproto.identity.resolveHandle": true,
}

// atproto_well_known answers a handle's identifier at the handle's host.
func atproto_well_known(c *gin.Context) {
	_, row := atproto_host_account(c.Request.Host)
	if row == nil || atproto_did(row) == "" {
		atproto_error(c, http.StatusNotFound, "NotFound")
		return
	}
	c.Data(http.StatusOK, "text/plain; charset=utf-8", []byte(atproto_did(row)))
}

// atproto_describe_server describes this server as a data server: its own
// identifier on the host asked, the domain handles are made under, and that
// accounts are not made through the protocol.
func atproto_describe_server(c *gin.Context) {
	host := activitypub_host(c)
	c.JSON(http.StatusOK, map[string]any{
		"did":                  "did:web:" + host,
		"availableUserDomains": []string{"." + host},
		"inviteCodeRequired":   true,
		"links":                map[string]any{},
	})
}

// atproto_resolve_handle answers the identifier of a handle this server
// hosts.
func atproto_resolve_handle(c *gin.Context) {
	handle := strings.ToLower(strings.TrimSpace(c.Query("handle")))
	_, row := atproto_host_account(handle)
	if row != nil && atproto_did(row) != "" {
		c.JSON(http.StatusOK, map[string]any{"did": atproto_did(row)})
		return
	}
	// A handle elsewhere, as a signed-in app asks of its data server, is
	// resolved as the network resolves one.
	if _, raw := xrpc_bearer(c); raw != "" && handle != "" {
		if did, err := resolve_handle(c.Request.Context(), handle); err == nil && did != "" {
			c.JSON(http.StatusOK, map[string]any{"did": did})
			return
		}
	}
	atproto_error(c, http.StatusBadRequest, "HandleNotFound")
}

// atproto_refused answers for a repository the owner has taken down, and
// says so; false for one that is served.
func atproto_refused(c *gin.Context, row map[string]any) bool {
	if atproto_down(row) {
		atproto_error(c, http.StatusBadRequest, "RepoTakendown")
		return true
	}
	if atproto_deactivated(row) {
		atproto_error(c, http.StatusBadRequest, "RepoDeactivated")
		return true
	}
	return false
}

// atproto_describe_repo describes a repository: its handle, identifier and
// collections.
func atproto_describe_repo(c *gin.Context) {
	user, row, r := atproto_repository(c.Query("repo"))
	if r == nil {
		atproto_error(c, http.StatusBadRequest, "RepoNotFound")
		return
	}
	if atproto_refused(c, row) {
		return
	}
	collections := []string{}
	rows, _ := db_user(user, "user").rows("select distinct collection from records where account=? order by collection", r.account)
	for _, item := range rows {
		collections = append(collections, row_string(item, "collection"))
	}
	c.JSON(http.StatusOK, map[string]any{
		"handle":          row_string(row, "identifier"),
		"did":             r.did,
		"didDoc":          map[string]any{"id": r.did},
		"collections":     collections,
		"handleIsCorrect": true,
	})
}

// atproto_get_record answers one record.
func atproto_get_record(c *gin.Context) {
	user, row, r := atproto_repository(c.Query("repo"))
	if r == nil {
		atproto_error(c, http.StatusBadRequest, "RepoNotFound")
		return
	}
	if atproto_refused(c, row) {
		return
	}
	collection, rkey := c.Query("collection"), c.Query("rkey")
	id, value := repository_record(user, r.account, collection, rkey)
	if value == nil {
		atproto_error(c, http.StatusBadRequest, "RecordNotFound")
		return
	}
	c.JSON(http.StatusOK, map[string]any{"uri": "at://" + r.did + "/" + collection + "/" + rkey, "cid": id, "value": value})
}

// atproto_list_records answers a page of a collection.
func atproto_list_records(c *gin.Context) {
	user, row, r := atproto_repository(c.Query("repo"))
	if r == nil {
		atproto_error(c, http.StatusBadRequest, "RepoNotFound")
		return
	}
	if atproto_refused(c, row) {
		return
	}
	collection := c.Query("collection")
	if _, err := syntax.ParseNSID(collection); err != nil {
		atproto_error(c, http.StatusBadRequest, "InvalidRequest")
		return
	}
	limit := atproto_limit(c, 50, 100)
	items, cursor := repository_records(user, r.account, collection, limit, c.Query("cursor"), c.Query("reverse") == "true")
	records := []map[string]any{}
	for _, item := range items {
		records = append(records, map[string]any{"uri": "at://" + r.did + "/" + collection + "/" + item["rkey"].(string), "cid": item["cid"], "value": item["value"]})
	}
	answer := map[string]any{"records": records}
	if cursor != "" {
		answer["cursor"] = cursor
	}
	c.JSON(http.StatusOK, answer)
}

// atproto_limit reads a page size within bounds.
func atproto_limit(c *gin.Context, fallback int, maximum int) int {
	limit := fallback
	if text := c.Query("limit"); text != "" {
		if value, err := strconv.Atoi(text); err == nil && value > 0 {
			limit = value
		}
	}
	if limit > maximum {
		limit = maximum
	}
	return limit
}

// atproto_sync_repo answers the whole repository as an archive.
func atproto_sync_repo(c *gin.Context) {
	_, row, r := atproto_repository(c.Query("did"))
	if r == nil || r.head == "" {
		atproto_error(c, http.StatusNotFound, "RepoNotFound")
		return
	}
	if atproto_refused(c, row) {
		return
	}
	archive, err := repository_archive(c.Request.Context(), r)
	if err != nil {
		warn("Repository %s: %v", r.account, err)
		atproto_error(c, http.StatusInternalServerError, "InternalError")
		return
	}
	c.Data(http.StatusOK, "application/vnd.ipld.car", archive)
}

// atproto_sync_commit answers the latest commit's id and revision.
func atproto_sync_commit(c *gin.Context) {
	_, row, r := atproto_repository(c.Query("did"))
	if r == nil || r.head == "" {
		atproto_error(c, http.StatusNotFound, "RepoNotFound")
		return
	}
	if atproto_refused(c, row) {
		return
	}
	c.JSON(http.StatusOK, map[string]any{"cid": r.head, "rev": r.rev})
}

// atproto_sync_status answers whether a repository is active and its
// revision, or that the owner has taken it down.
func atproto_sync_status(c *gin.Context) {
	_, row, r := atproto_repository(c.Query("did"))
	if r == nil {
		atproto_error(c, http.StatusNotFound, "RepoNotFound")
		return
	}
	answer := map[string]any{"did": r.did, "active": true}
	if status := atproto_status(row); status != "" {
		answer["active"] = false
		answer["status"] = status
	}
	if r.rev != "" {
		answer["rev"] = r.rev
	}
	c.JSON(http.StatusOK, answer)
}

// atproto_sync_record answers a record with the commit and tree that prove
// it, as an archive.
func atproto_sync_record(c *gin.Context) {
	user, row, r := atproto_repository(c.Query("did"))
	if r == nil || r.head == "" {
		atproto_error(c, http.StatusNotFound, "RepoNotFound")
		return
	}
	if atproto_refused(c, row) {
		return
	}
	if id, _ := repository_record(user, r.account, c.Query("collection"), c.Query("rkey")); id == "" {
		atproto_error(c, http.StatusNotFound, "RecordNotFound")
		return
	}
	archive, err := repository_archive(c.Request.Context(), r)
	if err != nil {
		atproto_error(c, http.StatusInternalServerError, "InternalError")
		return
	}
	c.Data(http.StatusOK, "application/vnd.ipld.car", archive)
}

// atproto_sync_blob answers a blob's bytes.
func atproto_sync_blob(c *gin.Context) {
	user, row, r := atproto_repository(c.Query("did"))
	if r == nil {
		atproto_error(c, http.StatusNotFound, "RepoNotFound")
		return
	}
	if atproto_refused(c, row) {
		return
	}
	kind, data, found := repository_blob(user, r.account, c.Query("cid"))
	if !found {
		atproto_error(c, http.StatusNotFound, "BlobNotFound")
		return
	}
	c.Header("Cache-Control", "public, max-age=31536000, immutable")
	c.Data(http.StatusOK, kind, data)
}

// atproto_sync_blobs lists a repository's blobs.
func atproto_sync_blobs(c *gin.Context) {
	user, _, r := atproto_repository(c.Query("did"))
	if r == nil {
		atproto_error(c, http.StatusNotFound, "RepoNotFound")
		return
	}
	limit := atproto_limit(c, 500, 1000)
	query, args := "select cid from blobs where account=?", []any{r.account}
	if cursor := c.Query("cursor"); cursor != "" {
		query += " and cid>?"
		args = append(args, cursor)
	}
	query += " order by cid limit ?"
	args = append(args, limit+1)
	rows, _ := db_user(user, "user").rows(query, args...)
	ids := []string{}
	answer := map[string]any{}
	for i, row := range rows {
		if i == limit {
			answer["cursor"] = ids[len(ids)-1]
			break
		}
		ids = append(ids, row_string(row, "cid"))
	}
	answer["cids"] = ids
	c.JSON(http.StatusOK, answer)
}

// atproto_sync_repos lists the repositories this server holds.
func atproto_sync_repos(c *gin.Context) {
	limit := atproto_limit(c, 500, 1000)
	query, args := "select did from identities", []any{}
	if cursor := c.Query("cursor"); cursor != "" {
		query += " where did>?"
		args = append(args, cursor)
	}
	query += " order by did limit ?"
	args = append(args, limit+1)
	rows, _ := networks_database().rows(query, args...)
	repos := []map[string]any{}
	answer := map[string]any{}
	for i, row := range rows {
		if i == limit {
			answer["cursor"] = repos[len(repos)-1]["did"]
			break
		}
		_, account, r := atproto_identity(row_string(row, "did"))
		if r == nil {
			continue
		}
		repo := map[string]any{"did": r.did, "head": r.head, "rev": r.rev, "active": true}
		if status := atproto_status(account); status != "" {
			repo["active"] = false
			repo["status"] = status
		}
		repos = append(repos, repo)
	}
	answer["repos"] = repos
	c.JSON(http.StatusOK, answer)
}

// === Following and the graph (phase 19) ===
//
// A hosted account's follows, blocks, likes and reposts are records of its
// repository, public as everything on the network is; the apps write likes,
// reposts and replies through mochi.account.object.create, and core writes
// follows and blocks here so the following and blocked tables stay the one
// account of them.

// atproto_graph finds the account's record of a graph collection naming a
// person: the record key, or empty.
func atproto_graph(user *User, account string, collection string, did string) string {
	items, _ := repository_records(user, account, collection, 500, "", false)
	for _, item := range items {
		value, _ := item["value"].(map[string]any)
		if subject, _ := value["subject"].(string); subject == did {
			return item["rkey"].(string)
		}
	}
	return ""
}

// atproto_follow follows a person as a hosted Bluesky account: a follow record,
// and the following row the apps read, accepted at once since the network
// has no acceptance. Following them again writes nothing. Answers the
// record's URI.
func atproto_follow(ctx context.Context, user *User, row map[string]any, did string) (string, error) {
	id, _ := row["id"].(string)
	r := repository_get(user, id)
	if r == nil {
		return "", fmt.Errorf("account has no repository")
	}
	if rkey := atproto_graph(user, id, "app.bsky.graph.follow", did); rkey != "" {
		// A record an app wrote (xrpc.go) has its row already; one from
		// before is given it.
		uri := "at://" + r.did + "/app.bsky.graph.follow/" + rkey
		db_user(user, "user").exec("insert or ignore into following (account, actor, inbox, activity, accepted, created) values (?, ?, '', ?, ?, ?)", id, did, uri, now(), now())
		return uri, nil
	}
	rkey := repository_clock.Next().String()
	record := map[string]any{"$type": "app.bsky.graph.follow", "subject": did, "createdAt": time.Now().UTC().Format(time.RFC3339Nano)}
	if _, _, err := repository_write(ctx, r, "app.bsky.graph.follow", rkey, record, false); err != nil {
		return "", err
	}
	uri := "at://" + r.did + "/app.bsky.graph.follow/" + rkey
	db_user(user, "user").exec("insert into following (account, actor, inbox, activity, accepted, created) values (?, ?, '', ?, ?, ?) on conflict(account, actor) do update set activity=excluded.activity, accepted=excluded.accepted", id, did, uri, now(), now())
	return uri, nil
}

// atproto_unfollow deletes the follow record and the row.
func atproto_unfollow(ctx context.Context, user *User, row map[string]any, did string) error {
	id, _ := row["id"].(string)
	r := repository_get(user, id)
	if r == nil {
		return fmt.Errorf("account has no repository")
	}
	db_user(user, "user").exec("delete from following where account=? and actor=?", id, did)
	rkey := atproto_graph(user, id, "app.bsky.graph.follow", did)
	if rkey == "" {
		return nil
	}
	_, err := repository_remove(ctx, r, "app.bsky.graph.follow", rkey, false)
	return err
}

// atproto_block writes or deletes a block record for a person on each of the
// user's hosted Bluesky accounts: a block is public on the network.
func atproto_block(ctx context.Context, user *User, did string, on bool) {
	for _, row := range accounts_hosted(user, "bluesky") {
		id, _ := row["id"].(string)
		r := repository_get(user, id)
		if r == nil || atproto_did(row) == "" {
			continue
		}
		rkey := atproto_graph(user, id, "app.bsky.graph.block", did)
		var err error
		switch {
		case on && rkey == "":
			record := map[string]any{"$type": "app.bsky.graph.block", "subject": did, "createdAt": time.Now().UTC().Format(time.RFC3339Nano)}
			_, _, err = repository_write(ctx, r, "app.bsky.graph.block", repository_clock.Next().String(), record, false)
		case !on && rkey != "":
			_, err = repository_remove(ctx, r, "app.bsky.graph.block", rkey, false)
		}
		if err != nil {
			warn("Bluesky account %s: block of %s not written: %v", id, did, err)
		}
	}
}

// atproto_posts is a hosted account's own posts from its repository, newest
// first, in the form the index gives a feed item, so an app reads them as it
// reads a linked account's.
func atproto_posts(user *User, row map[string]any, cursor string, limit int) *linked_page {
	id, _ := row["id"].(string)
	did := atproto_did(row)
	handle := row_string(row, "identifier")
	name, _ := row["label"].(string)
	items, next := repository_records(user, id, "app.bsky.feed.post", limit, cursor, false)
	page := &linked_page{Items: []any{}, Cursor: next}
	author := map[string]any{"did": did, "handle": handle, "displayName": name}
	for _, item := range items {
		rkey, _ := item["rkey"].(string)
		page.Items = append(page.Items, map[string]any{"post": map[string]any{"uri": "at://" + did + "/app.bsky.feed.post/" + rkey, "cid": item["cid"], "author": author, "record": item["value"]}})
	}
	return page
}

// === Account status (phase 19) ===
//
// The owner takes a hosted account down: its repository stops being served,
// relays are told, and its identity stays, so restoring it is one event.

// atproto_down says whether the owner has taken an account down.
func atproto_down(row map[string]any) bool {
	status, _ := account_data(row)["status"].(string)
	return status == "takendown"
}

// atproto_deactivated says whether an account is deactivated: moved to
// another server, or arrived from one and not yet activated (xrpc.go).
func atproto_deactivated(row map[string]any) bool {
	deactivated, _ := account_data(row)["deactivated"].(bool)
	return deactivated
}

// atproto_status is why an account's repository is not active, as the sync
// methods answer it, or empty.
func atproto_status(row map[string]any) string {
	if atproto_down(row) {
		return "takendown"
	}
	if atproto_deactivated(row) {
		return "deactivated"
	}
	return ""
}

// atproto_takedown takes an account down or restores it.
func atproto_takedown(user *User, row map[string]any, down bool) {
	id, _ := row["id"].(string)
	did := atproto_did(row)
	data := account_data(row)
	if down {
		data["status"] = "takendown"
	} else {
		delete(data, "status")
	}
	encoded, _ := json.Marshal(data)
	db_user(user, "user").account_set(id, map[string]any{"data": string(encoded)})
	if did == "" {
		return
	}
	event := map[string]any{"did": did, "active": !down}
	if down {
		event["status"] = "takendown"
	}
	stream_append(did, "#account", event)
}

var api_server_account = sls.FromStringDict(sl.String("mochi.server.account"), sl.StringDict{
	"takedown": sl.NewBuiltin("mochi.server.account.takedown", api_server_account_takedown),
})

// mochi.server.account.takedown(did, down=True) -> None: Take a hosted Bluesky
// account on this server down by its identifier, or restore it with
// down=False: a taken-down repository is not served and relays are told.
// Requires settings/write
func api_server_account_takedown(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var did string
	down := true
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "did", &did, "down?", &down); err != nil {
		return nil, err
	}
	if err := require_permission(t, fn, "settings/write"); err != nil {
		return sl_error(fn, "%v", err)
	}
	user, row, _ := atproto_identity(strings.TrimSpace(did))
	if row == nil {
		return sl_error(fn, "no such account")
	}
	atproto_takedown(user, row, down)
	return sl.None, nil
}

// === Feed generators (phase 19) ===
//
// A feed an app ranks is published as a Bluesky feed generator: a record in
// the account's repository naming this server as the generator service, which
// the index asks for the feed's skeleton - the URIs of the posts, which the
// index fills in. The service's identity is did:web on the server's domain.

// atproto_generator_did is this server's identity as a feed generator
// service: did:web on the host the relay reaches it at.
func atproto_generator_did() string {
	host := server_domain()
	if endpoint, err := url.Parse(atproto_endpoint(host)); err == nil && endpoint.Host != "" {
		host = endpoint.Host
	}
	return "did:web:" + strings.ReplaceAll(host, ":", "%3A")
}

// atproto_generator_document answers the did:web document of this server as
// a feed generator service.
func atproto_generator_document(c *gin.Context) {
	host := activitypub_host(c)
	c.JSON(http.StatusOK, map[string]any{
		"@context": []string{"https://www.w3.org/ns/did/v1"},
		"id":       "did:web:" + host,
		"service": []map[string]any{{
			"id":              "#bsky_fg",
			"type":            "BskyFeedGenerator",
			"serviceEndpoint": "https://" + host,
		}},
	})
}

// atproto_generators lists the generator records of every account on this
// server, as describeFeedGenerator wants them.
func atproto_generators() []map[string]any {
	feeds := []map[string]any{}
	rows, _ := networks_database().rows("select did from identities order by did")
	for _, row := range rows {
		user, account, r := atproto_identity(row_string(row, "did"))
		if r == nil || atproto_status(account) != "" {
			continue
		}
		items, _ := repository_records(user, r.account, "app.bsky.feed.generator", 100, "", false)
		for _, item := range items {
			feeds = append(feeds, map[string]any{"uri": "at://" + r.did + "/app.bsky.feed.generator/" + item["rkey"].(string)})
		}
	}
	return feeds
}

// atproto_describe_generator answers what feeds this server generates.
func atproto_describe_generator(c *gin.Context) {
	c.JSON(http.StatusOK, map[string]any{"did": "did:web:" + activitypub_host(c), "feeds": atproto_generators()})
}

// atproto_skeleton answers a feed's skeleton: the app that published the
// generator record names the posts, through the manifest function
// atproto/skeleton, given the account, the feed's URI, the cursor and the
// limit, and answers {posts: [uri, ...], cursor}.
func atproto_skeleton(c *gin.Context) {
	feed := strings.TrimSpace(c.Query("feed"))
	did, collection, rkey, err := bluesky_uri(feed)
	if err != nil || collection != "app.bsky.feed.generator" {
		atproto_error(c, http.StatusBadRequest, "UnknownFeed")
		return
	}
	user, row, r := atproto_identity(did)
	if r == nil || atproto_status(row) != "" {
		atproto_error(c, http.StatusBadRequest, "UnknownFeed")
		return
	}
	if id, _ := repository_record(user, r.account, "app.bsky.feed.generator", rkey); id == "" {
		atproto_error(c, http.StatusBadRequest, "UnknownFeed")
		return
	}
	limit := atproto_limit(c, 50, 100)
	answer := map[string]any{"feed": []any{}}
	for _, a := range apps_for_function(user, "atproto/skeleton") {
		result, err := activitypub_call(user, a, "atproto/skeleton", Map{"account": r.account, "feed": feed, "cursor": c.Query("cursor"), "limit": limit})
		if err != nil {
			debug("Feed skeleton from %s: %v", a.id, err)
			continue
		}
		skeleton, ok := result.(map[string]any)
		if !ok {
			continue
		}
		posts := []any{}
		for _, uri := range object_strings(skeleton["posts"]) {
			posts = append(posts, map[string]any{"post": uri})
		}
		answer["feed"] = posts
		if cursor, _ := skeleton["cursor"].(string); cursor != "" {
			answer["cursor"] = cursor
		}
		break
	}
	c.JSON(http.StatusOK, answer)
}

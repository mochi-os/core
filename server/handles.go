// Mochi server: Handles
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// A handle is the short name behind a hosted account's address: alice in
// alice@example.com, @alice:example.com and alice.example.com. It is reserved
// once per domain and is valid on every network, each switched on separately
// by creating that network's hosted account on it. The registry lives in
// users.db beside users and entities, since discovery requests arrive with no
// user signed in and app databases are per user.
//
// The rules (the federation plan, "Handles"): the owner limits how many bare
// handles each user may reserve, counted across the domains everyone shares;
// administrators are unlimited and may take the protected role names; a name
// under a handle, written handle_suffix, is free and belongs to whoever holds
// the handle; a forum's community name is a reservation of the same kind under
// the same rules; a released handle is freed for anyone; names that would claim
// a host under the domain on Bluesky are closed to everyone. Each domain is its
// own namespace, run by its owner, and handles on a user's own domain are free
// of the limit.

package main

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

// handle is one registry row. Parent is the handle a suffix name sits under,
// empty for a bare name; Kind is "handle" or "community".
type handle struct {
	Domain  string `db:"domain"`
	Name    string `db:"name"`
	User    string `db:"user"`
	Parent  string `db:"parent"`
	Kind    string `db:"kind"`
	Created int64  `db:"created"`
}

// The length a name may have, counting a suffix and its separator: the
// intersection of what the networks accept, with room for a suffix.
const handle_minimum = 3
const handle_maximum = 20

// A bare handle is lowercase letters and digits, starting with a letter; a name
// under a handle adds one suffix after an underscore. Hyphens, which Bluesky
// uses in place of the underscore, are accepted on input and stored as the
// underscore, so the two spellings are one reservation.
var handle_bare_re = regexp.MustCompile(`^[a-z][a-z0-9]*$`)
var handle_suffix_re = regexp.MustCompile(`^[a-z][a-z0-9]*_[a-z0-9]+$`)

// handle_protected are the role names only an administrator may reserve. They
// are not passed on: when their holder is demoted or deleted the name is freed
// for another administrator to reserve afresh.
var handle_protected = map[string]bool{
	"admin": true, "administrator": true, "abuse": true, "support": true, "moderator": true,
	"moderation": true, "security": true, "staff": true, "team": true, "official": true,
	"help": true, "info": true, "contact": true, "press": true, "postmaster": true,
	"webmaster": true, "hostmaster": true, "root": true, "noreply": true,
}

// handle_blocked are closed to everyone, administrators included: a handle is
// a host name under the domain on Bluesky, and these name hosts a domain has or
// may need, or paths and services the server itself answers.
var handle_blocked = map[string]bool{
	"www": true, "mail": true, "smtp": true, "imap": true, "pop": true, "pop3": true,
	"ftp": true, "ns": true, "ns1": true, "ns2": true, "dns": true, "mx": true,
	"autoconfig": true, "autodiscover": true, "localhost": true, "rooms": true,
	"matrix": true, "xmpp": true, "conference": true, "upload": true, "cdn": true,
	"static": true, "assets": true, "media": true, "api": true, "oauth": true,
	"xrpc": true, "pictrs": true, "mochi": true,
}

// handle_normalise lowercases a name and writes a hyphen as the underscore it
// is stored as.
func handle_normalise(name string) string {
	return strings.ReplaceAll(strings.ToLower(strings.TrimSpace(name)), "-", "_")
}

// handle_bluesky writes a name as Bluesky wants it: the suffix separator is a
// hyphen, since an underscore is not valid in a host name.
func handle_bluesky(name string) string {
	return strings.ReplaceAll(name, "_", "-")
}

// handle_parent returns the handle a normalised name sits under, or empty for
// a bare name.
func handle_parent(name string) string {
	if i := strings.IndexByte(name, '_'); i > 0 {
		return name[:i]
	}
	return ""
}

// handle_name_valid reports whether a normalised name has the allowed shape.
func handle_name_valid(name string) bool {
	if len(name) < handle_minimum || len(name) > handle_maximum {
		return false
	}
	return handle_bare_re.MatchString(name) || handle_suffix_re.MatchString(name)
}

// server_domain_name is [web] domain as configured, read at startup.
var server_domain_name string

// server_domain is the server's own domain, lowercased, or empty when none is
// configured. A server with no domain hosts no accounts.
func server_domain() string {
	domain := strings.ToLower(strings.TrimSpace(server_domain_name))
	if domain == "localhost" {
		return ""
	}
	return domain
}

// domain_owned reports whether a user runs a domain: a route for the domain's
// root names them as owner.
func domain_owned(user *User, name string) bool {
	if user == nil || name == "" {
		return false
	}
	db := db_open("db/domains.db")
	owned, _ := db.exists("select 1 from routes where domain=? and path='' and owner=?", name, user.UID)
	return owned
}

// handle_domains lists the domains a user may reserve a handle on: the server's
// own domain, the domains the user runs, and for an administrator every
// registered domain. The server's domain comes first, as the default.
func handle_domains(user *User) []string {
	seen := map[string]bool{}
	var domains []string
	add := func(name string) {
		name = strings.ToLower(name)
		if name == "" || strings.HasPrefix(name, "*.") || seen[name] {
			return
		}
		seen[name] = true
		domains = append(domains, name)
	}
	add(server_domain())
	if user == nil {
		return domains
	}
	for _, d := range domain_list() {
		if user.administrator() || domain_owned(user, d.Domain) {
			add(d.Domain)
		}
	}
	return domains
}

// handle_domain_allowed reports whether a user may reserve on a domain.
func handle_domain_allowed(user *User, name string) bool {
	for _, d := range handle_domains(user) {
		if d == name {
			return true
		}
	}
	return false
}

// handle_get returns a registry row by domain and normalised name, or nil.
func handle_get(domain string, name string) *handle {
	db := db_open("db/users.db")
	var h handle
	if !db.scan(&h, "select * from handles where domain=? and name=?", domain, name) {
		return nil
	}
	return &h
}

// handle_lookup resolves a name as it arrives from a network or a URL - any
// case, either separator - to its registry row, or nil.
func handle_lookup(domain string, name string) *handle {
	return handle_get(strings.ToLower(domain), handle_normalise(name))
}

// handles_user lists a user's reservations, parents before their suffix names.
func handles_user(uid string) []handle {
	db := db_open("db/users.db")
	var rows []handle
	db.scans(&rows, "select * from handles where user=? order by domain, parent, name", uid)
	return rows
}

// handles_all lists every reservation on the server, by domain, parents before
// their suffix names.
func handles_all() []handle {
	db := db_open("db/users.db")
	var rows []handle
	db.scans(&rows, "select * from handles order by domain, parent, name")
	return rows
}

// handle_limit is the number of bare names a user may hold in the shared
// namespace, from the owner's setting. Zero means administrators only.
func handle_limit() int {
	limit := 0
	fmt.Sscanf(setting_effective("handles"), "%d", &limit)
	return limit
}

// handle_counted reports how many bare names a user holds that count against
// the limit: handles and community names on domains the user does not run.
func handle_counted(user *User) int {
	count := 0
	for _, h := range handles_user(user.UID) {
		if h.Parent == "" && !domain_owned(user, h.Domain) {
			count++
		}
	}
	return count
}

// handle_available says whether a user may reserve a name on a domain, and if
// not why: invalid, domain, blocked, protected, taken, parent or limit. The
// domain is the server's when empty. kind is "handle" or "community".
func handle_available(user *User, domain string, name string, kind string) (string, string, string) {
	name = handle_normalise(name)
	domain = strings.ToLower(strings.TrimSpace(domain))
	if domain == "" {
		domain = server_domain()
	}
	if kind != "handle" && kind != "community" {
		return domain, name, "invalid"
	}
	if !handle_name_valid(name) {
		return domain, name, "invalid"
	}
	if domain == "" || !handle_domain_allowed(user, domain) {
		return domain, name, "domain"
	}
	parent := handle_parent(name)
	if parent == "" {
		if handle_blocked[name] || engine_path_reserved(name) {
			return domain, name, "blocked"
		}
		// On a user's own domain the protected names are theirs to take.
		if handle_protected[name] && !user.administrator() && !domain_owned(user, domain) {
			return domain, name, "protected"
		}
	}
	if handle_get(domain, name) != nil {
		return domain, name, "taken"
	}
	if parent != "" {
		// A name under a handle belongs to whoever holds the handle; an
		// administrator may add one under any handle, as they may take any name.
		held := handle_get(domain, parent)
		if held == nil || (held.User != user.UID && !user.administrator()) {
			return domain, name, "parent"
		}
		return domain, name, ""
	}
	if !user.administrator() && !domain_owned(user, domain) && handle_counted(user) >= handle_limit() {
		return domain, name, "limit"
	}
	return domain, name, ""
}

// handle_reserve reserves a name for a user, under the rules above.
func handle_reserve(user *User, domain string, name string, kind string) (*handle, error) {
	domain, name, reason := handle_available(user, domain, name, kind)
	if reason != "" {
		return nil, fmt.Errorf("%s", reason)
	}
	h := &handle{Domain: domain, Name: name, User: user.UID, Parent: handle_parent(name), Kind: kind, Created: time.Now().Unix()}
	db := db_open("db/users.db")
	db.exec("insert into handles (domain, name, user, parent, kind, created) values (?, ?, ?, ?, ?, ?)", h.Domain, h.Name, h.User, h.Parent, h.Kind, h.Created)
	return h, nil
}

// handle_release frees a name: its hosted accounts and the names under it are
// deleted and the name may be reserved by anyone. The holder may release their
// own; an administrator may release any.
func handle_release(user *User, domain string, name string) error {
	domain = strings.ToLower(strings.TrimSpace(domain))
	if domain == "" {
		domain = server_domain()
	}
	name = handle_normalise(name)
	h := handle_get(domain, name)
	if h == nil {
		return fmt.Errorf("unknown")
	}
	if h.User != user.UID && !user.administrator() {
		return fmt.Errorf("not holder")
	}
	handle_delete(h)
	return nil
}

// handle_delete removes a row, the rows under it, and the hosted accounts on
// each, for whichever user holds them.
func handle_delete(h *handle) {
	db := db_open("db/users.db")
	var rows []handle
	db.scans(&rows, "select * from handles where domain=? and (name=? or parent=?)", h.Domain, h.Name, h.Name)
	for _, r := range rows {
		if holder := user_by_uid(r.User); holder != nil {
			accounts_hosted_remove(holder, r.Domain, r.Name)
		}
	}
	db.exec("delete from handles where domain=? and (name=? or parent=?)", h.Domain, h.Name, h.Name)
}

// handles_purge frees every name a deleted user held, with the accounts on
// them. The user's own database is going too, so only the registry is touched.
func handles_purge(uid string) {
	db := db_open("db/users.db")
	// db_create builds the table at startup; only a users database that never
	// went through it - a test's, built by hand - lacks it.
	if present, _ := db.exists("select 1 from sqlite_master where type='table' and name='handles'"); present {
		db.exec("delete from handles where user=?", uid)
	}
}

// handles_demoted frees the protected role names an administrator held when
// they are demoted, with the accounts on them; their ordinary names stay.
func handles_demoted(uid string) {
	for _, h := range handles_user(uid) {
		if h.Parent == "" && handle_protected[h.Name] {
			handle_delete(&h)
		}
	}
}

// handle_host_answers reports whether a host name under a domain is a reserved
// handle whose holder has a Bluesky account on it: the one case in which a
// certificate for a name under a wildcard domain is requested on demand.
func handle_host_answers(host string) bool {
	host = strings.ToLower(host)
	i := strings.IndexByte(host, '.')
	if i <= 0 {
		return false
	}
	h := handle_lookup(host[i+1:], host[:i])
	if h == nil {
		return false
	}
	holder := user_by_uid(h.User)
	if holder == nil {
		return false
	}
	return account_hosted(holder, "bluesky", h.Domain, h.Name) != nil
}

// === Starlark API ===

var api_handle = sls.FromStringDict(sl.String("mochi.handle"), sl.StringDict{
	"available": sl.NewBuiltin("mochi.handle.available", api_handle_available),
	"domains":   sl.NewBuiltin("mochi.handle.domains", api_handle_domains),
	"list":      sl.NewBuiltin("mochi.handle.list", api_handle_list),
	"registry":  sl.NewBuiltin("mochi.handle.registry", api_handle_registry),
	"release":   sl.NewBuiltin("mochi.handle.release", api_handle_release),
	"reserve":   sl.NewBuiltin("mochi.handle.reserve", api_handle_reserve),
})

// handle_arguments reads the name, domain and kind a handle call names.
func handle_arguments(fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (string, string, string, error) {
	if len(args) != 1 {
		return "", "", "", fmt.Errorf("syntax: <name: string>, [domain=...], [kind=...]")
	}
	name, ok := sl.AsString(args[0])
	if !ok {
		return "", "", "", fmt.Errorf("invalid name")
	}
	domain, kind := "", "handle"
	for _, kv := range kwargs {
		value, _ := sl.AsString(kv[1])
		switch string(kv[0].(sl.String)) {
		case "domain":
			domain = value
		case "kind":
			kind = value
		default:
			return "", "", "", fmt.Errorf("unknown argument %q", kv[0])
		}
	}
	return name, domain, kind, nil
}

// handle_present is a registry row as the API returns it, with the addresses
// the name would have on each network.
func handle_present(h handle) map[string]any {
	addresses := map[string]any{}
	for _, n := range network_names {
		addresses[n] = network_address(n, h.Name, h.Domain)
	}
	return map[string]any{
		"domain":    h.Domain,
		"name":      h.Name,
		"parent":    h.Parent,
		"kind":      h.Kind,
		"created":   h.Created,
		"addresses": addresses,
	}
}

// mochi.handle.available(name, domain="", kind="handle") -> dict: Whether the
// caller may reserve a name, with the reason when not: invalid, domain,
// blocked, protected, taken, parent or limit.
func api_handle_available(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := require_permission(t, fn, "accounts/read"); err != nil {
		return sl_error(fn, "%v", err)
	}
	user := principal_caller(t)
	if user == nil {
		return sl_error(fn, "no user")
	}
	name, domain, kind, err := handle_arguments(fn, args, kwargs)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	domain, name, reason := handle_available(user, domain, name, kind)
	return sl_encode(map[string]any{
		"available": reason == "",
		"reason":    reason,
		"domain":    domain,
		"name":      name,
	}), nil
}

// mochi.handle.domains() -> list: The domains the caller may reserve on, the
// server's own first.
func api_handle_domains(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := require_permission(t, fn, "accounts/read"); err != nil {
		return sl_error(fn, "%v", err)
	}
	user := principal_caller(t)
	if user == nil {
		return sl_error(fn, "no user")
	}
	return sl_encode(handle_domains(user)), nil
}

// mochi.handle.list() -> list: The caller's reservations, with the limit and
// how much of it is used.
func api_handle_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := require_permission(t, fn, "accounts/read"); err != nil {
		return sl_error(fn, "%v", err)
	}
	user := principal_caller(t)
	if user == nil {
		return sl_error(fn, "no user")
	}
	handles := []map[string]any{}
	for _, h := range handles_user(user.UID) {
		handles = append(handles, handle_present(h))
	}
	return sl_encode(map[string]any{
		"handles":   handles,
		"limit":     handle_limit(),
		"counted":   handle_counted(user),
		"unlimited": user.administrator(),
	}), nil
}

// mochi.handle.registry() -> list: Every reservation on the server with its
// holder's username and name, for the owner's table of handles. Administrators
// only.
func api_handle_registry(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := require_permission(t, fn, "server/read"); err != nil {
		return sl_error(fn, "%v", err)
	}
	user := principal_caller(t)
	if user == nil {
		return sl_error(fn, "no user")
	}
	if !user.administrator() {
		return sl_error(fn, "not administrator")
	}
	rows := []map[string]any{}
	for _, h := range handles_all() {
		row := handle_present(h)
		row["user"], row["username"], row["holder"] = h.User, "", ""
		if holder := user_by_uid(h.User); holder != nil {
			row["username"] = holder.Username
			if identity := holder.identity(); identity != nil {
				row["holder"] = identity.Name
			}
		}
		rows = append(rows, row)
	}
	return sl_encode(rows), nil
}

// mochi.handle.reserve(name, domain="", kind="handle") -> dict: Reserve a name
// for the caller. Refused with the reason handle.available would give.
func api_handle_reserve(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := require_permission(t, fn, "accounts/write"); err != nil {
		return sl_error(fn, "%v", err)
	}
	user := principal_caller(t)
	if user == nil {
		return sl_error(fn, "no user")
	}
	if !networks_development {
		return sl_error(fn, "networks are not available on this server")
	}
	name, domain, kind, err := handle_arguments(fn, args, kwargs)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	h, err := handle_reserve(user, domain, name, kind)
	if err != nil {
		return sl_error(fn, "handle not available: %v", err)
	}
	audit_handle(user.Username, "reserve", h.Domain, h.Name)
	return sl_encode(handle_present(*h)), nil
}

// mochi.handle.release(name, domain="") -> bool: Release a name the caller
// holds (an administrator may release any), deleting the hosted accounts on it
// and the names under it.
func api_handle_release(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := require_permission(t, fn, "accounts/write"); err != nil {
		return sl_error(fn, "%v", err)
	}
	user := principal_caller(t)
	if user == nil {
		return sl_error(fn, "no user")
	}
	name, domain, _, err := handle_arguments(fn, args, kwargs)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	if err := handle_release(user, domain, name); err != nil {
		return sl_error(fn, "cannot release handle: %v", err)
	}
	audit_handle(user.Username, "release", domain, handle_normalise(name))
	return sl.True, nil
}

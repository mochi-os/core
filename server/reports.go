// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: the administrator system
//
// Reports for the server's administrators (the federation plan, phase 40),
// kept in networks.db whichever network they came by: a Flag from another
// fediverse server about one of this server's people, which the reported
// person's apps never see; a report a user's Mastodon client sends; a Matrix
// client's report of an event, a room or a person; and an XMPP client's spam
// or abuse report on a blocking command (XEP-0377). A report sent to a
// community is its moderators', and stays with the app that keeps it. Each
// administrator is notified of a new report, and resolves it: dismissed, or
// with the person blocked or suspended. The reported person is told nothing,
// and no network carries a report's outcome back to whoever sent it.
//
// The owner's blocks are domains and people: a blocked person, on any of the
// networks, is nobody to every account here, as a person a user blocks is to
// that user's - their activities, events and stanzas are taken and dropped,
// they are no longer anyone's follower, and they are not looked up. A person
// of this server's own is suspended instead, as any user is.
//
// What the owner sees of the servers this one talks to is each network's own
// record: the fediverse delivery queue's per-host failures and the limit on
// requests to each host, Matrix's per-server backoff and outbox, and XMPP's
// links to other servers and the clients signed in.

package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/matrix-org/gomatrixserverlib/spec"
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

// report_open_maximum bounds the reports waiting from one other server, or
// from one person here; one past it is dropped. A variable so tests can lower
// it.
var report_open_maximum = 100

// The longest comment and reported text a report keeps, and the most objects.
const (
	report_comment_maximum = 2048
	report_content_maximum = 4096
	report_objects_maximum = 20
)

// report_outcomes are the ways an administrator resolves a report.
var report_outcomes = []string{"dismissed", "blocked", "suspended"}

// report_tables makes the reports and the people the owner blocks.
func report_tables(db *DB) {
	db.exec("create table if not exists reports (id text not null primary key, network text not null, reporter text not null, person text not null default '', user text not null default '', room text not null default '', objects text not null default '[]', category text not null default '', comment text not null default '', content text not null default '', names text not null default '{}', host text not null default '', created integer not null, resolved integer not null default 0, outcome text not null default '')")
	db.exec("create index if not exists reports_resolved on reports(resolved, created)")
	db.exec("create table if not exists blocked (actor text not null primary key, created integer not null)")
}

// A report is what one says, as the network carried it: who sent it, the
// person it is about and the local user that person is, the room it is in,
// the objects complained of, the kind of complaint, the reporter's words and
// the reported text where this server holds it, and the names each address
// is shown by. Host is the other server it came from, for the bound.
type report struct {
	Network  string
	Reporter string
	Person   string
	User     string
	Room     string
	Objects  []string
	Category string
	Comment  string
	Content  string
	Names    map[string]string
	Host     string
}

// report_clip cuts text to a number of bytes at a character boundary.
func report_clip(text string, maximum int) string {
	text = strings.TrimSpace(text)
	if len(text) <= maximum {
		return text
	}
	text = text[:maximum]
	for !utf8.ValidString(text) {
		text = text[:len(text)-1]
	}
	return text
}

// report_add keeps a report and tells the administrators, answering its id,
// or "" where the bound of reports waiting from its server or reporter is
// reached.
func report_add(r report) string {
	db := networks_database()
	var waiting map[string]any
	if r.Host != "" {
		waiting, _ = db.row("select count(*) as count from reports where host=? and resolved=0", strings.ToLower(r.Host))
	} else {
		waiting, _ = db.row("select count(*) as count from reports where host='' and reporter=? and resolved=0", r.Reporter)
	}
	if row_int(waiting, "count") >= int64(report_open_maximum) {
		debug("Report from %s dropped: %d waiting", r.Reporter, report_open_maximum)
		return ""
	}
	objects := []string{}
	for _, object := range r.Objects {
		if object = report_clip(object, 2048); object != "" && len(objects) < report_objects_maximum {
			objects = append(objects, object)
		}
	}
	if r.Names == nil {
		r.Names = map[string]string{}
	}
	id := uid()
	db.exec("insert into reports (id, network, reporter, person, user, room, objects, category, comment, content, names, host, created) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		id, r.Network, r.Reporter, r.Person, r.User, r.Room, json_encode(objects), r.Category, report_clip(r.Comment, report_comment_maximum), report_clip(r.Content, report_content_maximum), json_encode(r.Names), strings.ToLower(r.Host), now())
	report_notify(id, r)
	return id
}

// report_name is how an address is shown: the name given for it, else the
// address, an XMPP one without its scheme.
func report_name(names map[string]string, address string) string {
	if name := names[address]; name != "" {
		return name
	}
	return strings.TrimPrefix(address, "xmpp:")
}

// report_notify tells each active administrator of a new report.
func report_notify(id string, r report) {
	rows, _ := db_open("db/users.db").rows("select uid from users where role='administrator' and status='active'")
	for _, row := range rows {
		user := user_by_uid(row_string(row, "uid"))
		if user == nil {
			continue
		}
		language := user_language(user)
		body := resolve_core_label(language, "reports.notification.room", map[string]any{"reporter": report_name(r.Names, r.Reporter)})
		if r.Person != "" {
			body = resolve_core_label(language, "reports.notification.body", map[string]any{"reporter": report_name(r.Names, r.Reporter), "person": report_name(r.Names, r.Person)})
		}
		args := Map{
			"topic":  "reports/received",
			"object": id,
			"title":  resolve_core_label(language, "reports.notification.title", nil),
			"body":   body,
			"url":    "/settings/system/reports",
			"label":  resolve_core_label(language, "reports.notification.topic", nil),
			"event":  "report:" + id,
			"count":  int64(1),
		}
		if err := report_tell(user, args); err != nil {
			info("Report %s: notify %s: %v", id, user.UID, err)
		}
	}
}

// report_tell sends an administrator a notification; a test stands its own
// in.
var report_tell = func(user *User, args Map) error {
	return service_call_as_server(user.UID, "notifications", "send", args)
}

// report_view is a report as the API gives it: each address with the name it
// is shown by.
func report_view(row map[string]any) map[string]any {
	names := map[string]string{}
	json.Unmarshal([]byte(row_string(row, "names")), &names)
	objects := []string{}
	json.Unmarshal([]byte(row_string(row, "objects")), &objects)
	// A person is blockable who is on another server of a network, and
	// their server by its host.
	party := func(address string) any {
		if address == "" {
			return nil
		}
		_, person := block_target(address)
		ours := person_ours(address)
		host := identifier_host(address)
		if ours || !block_domain_re.MatchString(host) {
			host = ""
		}
		return map[string]any{"id": address, "name": report_name(names, address), "blockable": person && !ours, "host": host}
	}
	return map[string]any{
		"id":       row_string(row, "id"),
		"network":  row_string(row, "network"),
		"reporter": party(row_string(row, "reporter")),
		"person":   party(row_string(row, "person")),
		"user":     row_string(row, "user"),
		"room":     report_room(row_string(row, "room")),
		"objects":  objects,
		"category": row_string(row, "category"),
		"comment":  row_string(row, "comment"),
		"content":  row_string(row, "content"),
		"created":  row_int(row, "created"),
		"resolved": row_int(row, "resolved"),
		"outcome":  row_string(row, "outcome"),
	}
}

// report_room is a Matrix room as a report shows it: its name where it has
// one, else its id; nil for no room.
func report_room(room string) any {
	if room == "" {
		return nil
	}
	name := room
	if current, _ := rooms_current(room); current != nil {
		if event := rooms_event(current[rooms_key("m.room.name", "")]); event != nil {
			var content struct {
				Name string `json:"name"`
			}
			json.Unmarshal(event.Content(), &content)
			if content.Name != "" {
				name = content.Name
			}
		}
	}
	return map[string]any{"id": room, "name": name}
}

// person_local is the user of this server an address names, or nil: a
// Mochi identity here, a hosted fediverse account by its permanent id, a
// Matrix, XMPP or Bluesky account by its address.
func person_local(address string) *User {
	if id, found := strings.CutPrefix(address, "identity:"); found {
		if e := entity_by_any(id); e != nil && e.User != "" {
			return user_by_uid(e.User)
		}
		return nil
	}
	switch author_network(address) {
	case "fediverse":
		u, err := url.Parse(address)
		if err != nil || !domain_ours(strings.ToLower(u.Hostname())) || !strings.HasPrefix(u.Path, "/~") {
			return nil
		}
		user, _ := actor_account(strings.TrimPrefix(u.Path, "/~"))
		return user
	case "matrix":
		user, _ := federation_user(address)
		return user
	case "xmpp":
		user, _ := account_by_address("xmpp", address)
		return user
	case "bluesky":
		user, _ := account_by_did(address)
		return user
	}
	return nil
}

// domain_ours says whether a host is one of this server's own: its domain, a
// verified domain, or one its hosted accounts are on.
func domain_ours(host string) bool {
	host = strings.ToLower(host)
	if host == "" {
		return false
	}
	if host == server_domain() || domain_served(host) {
		return true
	}
	found, _ := networks_database().exists("select 1 from actors where domain=? and deleted=0", host)
	return found
}

// person_ours says whether an address is on one of this server's own
// domains, whether or not an account holds it.
func person_ours(address string) bool {
	host := identifier_host(address)
	switch author_network(address) {
	case "matrix":
		return federation_local(spec.ServerName(host)) || domain_ours(host)
	case "xmpp":
		return xmpp_local(host) || domain_ours(host)
	case "bluesky":
		return person_local(address) != nil || domain_ours(host)
	}
	return domain_ours(host)
}

// === The owner's blocks of people ===

// block_target reads a person from a block API's argument: anything
// actor_target takes, or a Matrix user id; false for a domain.
func block_target(target string) (string, bool) {
	target = strings.TrimSpace(target)
	if id, err := spec.NewUserID(target, true); err == nil && strings.HasPrefix(target, "@") {
		return id.String(), true
	}
	return actor_target(target)
}

// block_refusal says why the owner cannot block a target, or "": "invalid"
// for neither a domain nor a person, "local" for a person or domain of this
// server's own.
func block_refusal(target string) string {
	if actor, ok := block_target(target); ok {
		if person_ours(actor) {
			return "local"
		}
		return ""
	}
	domain := strings.ToLower(strings.TrimSpace(target))
	if !block_domain_re.MatchString(domain) {
		return "invalid"
	}
	if domain_ours(domain) || federation_local(spec.ServerName(domain)) || xmpp_local(domain) {
		return "local"
	}
	return ""
}

// server_blocked says whether the owner blocks a person, by their id on their
// network.
func server_blocked(actor string) bool {
	if actor == "" {
		return false
	}
	found, _ := networks_database().exists("select 1 from blocked where actor=?", actor)
	return found
}

// server_blocked_add blocks a person for the whole server: they are no longer
// a follower of any account here, nor followed by one. A person of this
// server's own is not blocked but suspended.
func server_blocked_add(actor string) error {
	if person_ours(actor) {
		return fmt.Errorf("a person on this server is suspended, not blocked")
	}
	networks_database().exec("insert or ignore into blocked (actor, created) values (?, ?)", actor, now())
	rows, _ := db_open("db/users.db").rows("select uid from users")
	for _, row := range rows {
		user := user_by_uid(row_string(row, "uid"))
		if user == nil {
			continue
		}
		db := db_user(user, "user")
		db.exec("delete from followers where actor=?", actor)
		db.exec("delete from following where actor=?", actor)
	}
	return nil
}

// === The API ===

var api_server_report = sls.FromStringDict(sl.String("mochi.server.report"), sl.StringDict{
	"get":     sl.NewBuiltin("mochi.server.report.get", api_server_report_get),
	"list":    sl.NewBuiltin("mochi.server.report.list", api_server_report_list),
	"resolve": sl.NewBuiltin("mochi.server.report.resolve", api_server_report_resolve),
})

// mochi.server.report.list(state="open") -> list: The reports for the server's administrators, each with its id, network, reporter and person (each {id, name}, the person None for a report of a room), the local user the person is, the room, the objects complained of, the category, the reporter's comment, the reported text where the server holds it, when it came, and when and how it was resolved. state "open" lists those waiting, oldest first; "resolved" the 200 last resolved, latest first. Requires server/read
func api_server_report_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	state := "open"
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "state?", &state); err != nil {
		return nil, err
	}
	if err := require_permission(t, fn, "server/read"); err != nil {
		return sl_error(fn, "%v", err)
	}
	var rows []map[string]any
	switch state {
	case "open":
		rows, _ = networks_database().rows("select * from reports where resolved=0 order by created, rowid")
	case "resolved":
		rows, _ = networks_database().rows("select * from reports where resolved>0 order by resolved desc, rowid desc limit 200")
	default:
		return sl_error(fn, "state must be open or resolved")
	}
	out := []any{}
	for _, row := range rows {
		out = append(out, report_view(row))
	}
	return sl_encode(out), nil
}

// mochi.server.report.get(id) -> dict | None: One report, as mochi.server.report.list gives it. Requires server/read
func api_server_report_get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var id string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "id", &id); err != nil {
		return nil, err
	}
	if err := require_permission(t, fn, "server/read"); err != nil {
		return sl_error(fn, "%v", err)
	}
	row, _ := networks_database().row("select * from reports where id=?", id)
	if row == nil {
		return sl.None, nil
	}
	return sl_encode(report_view(row)), nil
}

// mochi.server.report.resolve(id, outcome) -> None: Resolve a waiting report: "dismissed", or "blocked" or "suspended" for what was done about it. Requires settings/write
func api_server_report_resolve(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var id, outcome string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "id", &id, "outcome", &outcome); err != nil {
		return nil, err
	}
	if err := require_permission(t, fn, "settings/write"); err != nil {
		return sl_error(fn, "%v", err)
	}
	if !slices.Contains(report_outcomes, outcome) {
		return sl_error(fn, "outcome must be dismissed, blocked or suspended")
	}
	db := networks_database()
	if found, _ := db.exists("select 1 from reports where id=? and resolved=0", id); !found {
		return sl_error(fn, "no such report waiting")
	}
	db.exec("update reports set resolved=?, outcome=? where id=? and resolved=0", now(), outcome, id)
	return sl.None, nil
}

// mochi.server.hosts() -> dict: What the server knows of the servers it talks to. fediverse: each host it delivers to, with its failures in a row, its last success and failure, the next attempt, the last error, the deliveries waiting and whether the limit on requests to it is reached. matrix: each server with its failures, the next attempt and the transactions waiting. xmpp: each link to another server, from which of this server's domains, whether it is up, its failures, the next attempt and the stanzas waiting; and the clients signed in, with the accounts they are of. Requires server/read
func api_server_hosts(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) > 0 || len(kwargs) > 0 {
		return sl_error(fn, "syntax: no arguments")
	}
	if err := require_permission(t, fn, "server/read"); err != nil {
		return sl_error(fn, "%v", err)
	}
	return sl_encode(hosts_health()), nil
}

// hosts_health gathers each network's record of the servers it talks to.
func hosts_health() map[string]any {
	db := networks_database()
	waiting := map[string]int64{}
	counts, _ := db.rows("select host, count(*) as count from deliveries group by host")
	for _, row := range counts {
		waiting[row_string(row, "host")] = row_int(row, "count")
	}
	fediverse := []any{}
	seen := map[string]bool{}
	rows, _ := db.rows("select * from hosts order by host")
	for _, row := range rows {
		host := row_string(row, "host")
		seen[host] = true
		fediverse = append(fediverse, map[string]any{
			"host":     host,
			"failures": row_int(row, "failures"),
			"success":  row_int(row, "success"),
			"failure":  row_int(row, "failure"),
			"next":     row_int(row, "next"),
			"error":    row_string(row, "error"),
			"waiting":  waiting[host],
			"limited":  rate_limit_host.exhausted(host),
		})
	}
	for host, count := range waiting {
		if !seen[host] {
			fediverse = append(fediverse, map[string]any{"host": host, "failures": 0, "success": 0, "failure": 0, "next": 0, "error": "", "waiting": count, "limited": rate_limit_host.exhausted(host)})
		}
	}

	matrix := []any{}
	if network_enabled("matrix") {
		federation := federation_database()
		outbox := map[string]int64{}
		counts, _ := federation.rows("select destination, count(*) as count from outbox group by destination")
		for _, row := range counts {
			outbox[row_string(row, "destination")] = row_int(row, "count")
		}
		seen := map[string]bool{}
		rows, _ := federation.rows("select name, failures, retry from destinations order by name")
		for _, row := range rows {
			name := row_string(row, "name")
			seen[name] = true
			matrix = append(matrix, map[string]any{"server": name, "failures": row_int(row, "failures"), "next": row_int(row, "retry"), "waiting": outbox[name]})
		}
		for name, count := range outbox {
			if !seen[name] {
				matrix = append(matrix, map[string]any{"server": name, "failures": 0, "next": 0, "waiting": count})
			}
		}
	}

	links := []any{}
	xmpp_peers_lock.Lock()
	peers := make([]*xmpp_peer, 0, len(xmpp_peers))
	for _, l := range xmpp_peers {
		peers = append(peers, l)
	}
	xmpp_peers_lock.Unlock()
	for _, l := range peers {
		l.lock.Lock()
		next := int64(0)
		if !l.retry.IsZero() {
			next = l.retry.Unix()
		}
		links = append(links, map[string]any{"server": l.remote, "domain": l.local, "up": l.ready && l.conn != nil, "failures": l.failures, "next": next, "waiting": len(l.waiting)})
		l.lock.Unlock()
	}
	xmpp_resources_lock.Lock()
	clients, accounts := 0, 0
	for _, resources := range xmpp_resources {
		if len(resources) > 0 {
			clients += len(resources)
			accounts++
		}
	}
	xmpp_resources_lock.Unlock()
	return map[string]any{
		"fediverse": fediverse,
		"matrix":    matrix,
		"xmpp":      map[string]any{"links": links, "sessions": map[string]any{"clients": clients, "accounts": accounts}},
	}
}

// Mochi server: ActivityPub engine
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// The fediverse side of a hosted account: discovery (WebFinger, host-meta,
// NodeInfo), the account's document and collections under its permanent id
// https://<domain>/~<id> (decision 40 of the federation plan), /@<handle> as
// the human address, the inbox per account and the shared one, and the queue
// that hands what arrives to the apps. Core carries the transport and the
// identity; an app supplies the meaning through the functions it declares:
// activitypub/object answers for an object the app published,
// activitypub/profile describes the person behind the account, and
// activitypub/receive takes the activities the inbox queues, and
// activitypub/featured names the objects the account pins. Follow and Undo
// of a Follow are the engine's own: a follow is accepted at once and the
// follower recorded for delivery, and the Follow is then handed to the apps
// as well, so one can tell the person. The Accept and Reject of a Follow the
// account sent, and a Move by someone it follows, are the engine's too: the
// following table is kept, the follow of a moved person is sent again to
// where they went, and the apps hear of the Move to follow suit. What a
// followed person posts reaches the shared inbox addressed to their
// followers, and is routed to every account here that follows them.
//
// Every inbound request is checked before anything else sees it: the HTTP
// signature against the actor's key, fetched from the actor's own server and
// cached; the actor's host against the owner's and the user's domain blocks,
// and the actor against the people the user has blocked or muted
// (moderation.go); the activity's id against those already seen. Engine
// routes take no session cookie and no app token: nothing here acts as the
// signed-in user.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"html"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

const activitypub_type = "application/activity+json"
const activitypub_jrd = "application/jrd+json"
const activitypub_nodeinfo = "http://nodeinfo.diaspora.software/ns/schema/2.1"
const activitypub_subscribe = "http://ostatus.org/schema/1.0/subscribe"

// activitypub_context is the @context an actor document and the activities the
// engine makes carry: ActivityStreams, the security vocabulary for the key, and
// the Mastodon terms other servers read.
var activitypub_context = []any{
	activitystreams,
	"https://w3id.org/security/v1",
	map[string]any{
		"manuallyApprovesFollowers": "as:manuallyApprovesFollowers",
		"toot":                      "http://joinmastodon.org/ns#",
		"discoverable":              "toot:discoverable",
		"indexable":                 "toot:indexable",
		"schema":                    "http://schema.org#",
		"PropertyValue":             "schema:PropertyValue",
		"value":                     "schema:value",
	},
}

// activitypub_body_maximum caps an inbox body: an activity is a few kilobytes,
// a long post with its tags under a hundred.
const activitypub_body_maximum = 1 << 20

// activitypub_page is how many items a collection page carries.
const activitypub_page = 20

// inbound_batch is how many queued activities one app call takes, and
// inbound_poll how often the manager looks unprompted. Variables so tests can
// lower them.
var inbound_batch = 20
var inbound_poll = 10 * time.Second

// received_age is how long an activity id is remembered, so a replay is
// dropped.
var received_age int64 = 7 * 86400

// tombstone_age is how long a removed account answers gone, and keeps its key
// to sign the notice of its own deletion.
var tombstone_age int64 = 30 * 86400

// activitypub_call runs one of an app's functions as the server. A variable so
// tests substitute an app.
var activitypub_call = app_call_as_server

// activitypub_engine is the engine's registration: discovery under
// /.well-known, NodeInfo, every path under the two sigils. Off with the
// fediverse setting.
func activitypub_engine() *engine {
	return &engine{
		name:     "activitypub",
		prefixes: []string{"/.well-known/webfinger", "/.well-known/nodeinfo", "/nodeinfo/", "/~", "/@"},
		setting:  "fediverse",
		body:     activitypub_body_maximum,
		handler:  activitypub_route,
	}
}

// === Actors ===
//
// Accounts live in each user's own database, and a request for /~<id> arrives
// with no user: the actors table in networks.db maps an account's permanent id
// to its user, domain and handle. A removed account stays as a tombstone with
// its key, so the notice of its deletion can still be signed and its id
// answers gone rather than not found.

// actor is a row of the actors table.
type actor struct {
	ID      string `db:"id"`
	User    string `db:"user"`
	Domain  string `db:"domain"`
	Handle  string `db:"handle"`
	Network string `db:"network"`
	Created int64  `db:"created"`
	Deleted int64  `db:"deleted"`
	Key     string `db:"key"`
}

// actor_register records a hosted account in the actors table.
func actor_register(user *User, id string, network string, domain string, handle string) {
	networks_database().exec("insert into actors (id, user, domain, handle, network, created) values (?, ?, ?, ?, ?, ?) on conflict(id) do update set user=excluded.user, domain=excluded.domain, handle=excluded.handle, network=excluded.network, deleted=0, key=''", id, user.UID, strings.ToLower(domain), handle, network, now())
}

// actor_get reads an actor by id, tombstones included, or nil.
func actor_get(id string) *actor {
	var a actor
	if !networks_database().scan(&a, "select * from actors where id=?", id) {
		return nil
	}
	return &a
}

// actor_by_handle finds the fediverse actor of a handle on a domain, or nil.
func actor_by_handle(domain string, name string) *actor {
	h := handle_lookup(domain, name)
	if h == nil {
		return nil
	}
	var a actor
	if !networks_database().scan(&a, "select * from actors where user=? and domain=? and handle=? and network='fediverse' and deleted=0", h.User, h.Domain, h.Name) {
		return nil
	}
	return &a
}

// actor_remove turns an account's actor row into a tombstone: the row stays,
// marked deleted, holding the signing key so the Delete of the actor can go
// out, and is dropped after tombstone_age.
func actor_remove(user *User, id string) {
	key := ""
	if k := keypair_get(user, id, key_signing); k != nil {
		key = k.Private
	}
	networks_database().exec("update actors set deleted=?, key=? where id=? and deleted=0", now(), key, id)
}

// actors_rebuild makes the actors table agree with the handle registry and
// the users' accounts, for a server whose accounts predate the table or whose
// networks database was lost. Tombstones are left as they are.
func actors_rebuild() {
	db := networks_database()
	seen := map[string]bool{}
	for _, h := range handles_all() {
		user := user_by_uid(h.User)
		if user == nil || user_pending(user) {
			continue
		}
		for _, row := range accounts_hosted(user, "") {
			data, _ := row["data"].(map[string]any)
			if data["domain"] != h.Domain || data["handle"] != h.Name {
				continue
			}
			id, _ := row["id"].(string)
			ptype, _ := row["type"].(string)
			created := row_int(row, "created")
			if created == 0 {
				created = now()
			}
			db.exec("insert into actors (id, user, domain, handle, network, created) values (?, ?, ?, ?, ?, ?) on conflict(id) do update set user=excluded.user, domain=excluded.domain, handle=excluded.handle, network=excluded.network", id, h.User, h.Domain, h.Name, ptype, created)
			seen[id] = true
			// A Bluesky account's identifier is found by its registry too.
			if did := atproto_did(row); ptype == "bluesky" && did != "" {
				atproto_register(did, id, user)
			}
		}
	}
	rows, _ := db.rows("select id from actors where deleted=0")
	for _, row := range rows {
		if id := row_string(row, "id"); !seen[id] {
			db.exec("delete from actors where id=?", id)
		}
	}
}

// actor_account resolves a live fediverse actor to its user and account row,
// or nil for an unknown, deleted or non-fediverse id.
func actor_account(id string) (*User, map[string]any) {
	a := actor_get(id)
	if a == nil || a.Deleted != 0 || a.Network != "fediverse" {
		return nil, nil
	}
	user := user_by_uid(a.User)
	if user == nil || user_quiet(user) {
		return nil, nil
	}
	row := account_hosted_get(user, id)
	if row == nil {
		return nil, nil
	}
	return user, row
}

// tombstone_key is the signing key a removed account kept, with its actor id,
// for the delivery queue; empty when the account is live or long gone.
func tombstone_key(id string) (string, string) {
	a := actor_get(id)
	if a == nil || a.Deleted == 0 || a.Key == "" {
		return "", ""
	}
	return account_url(a.Domain, id), a.Key
}

// activitypub_cleanup drops old received ids and old tombstones. Called from
// the delivery manager's hourly pass.
func activitypub_cleanup() {
	db := networks_database()
	db.exec("delete from seen where created<?", now()-received_age)
	db.exec("delete from actors where deleted>0 and deleted<?", now()-tombstone_age)
}

// === Routing ===

// activitypub_route dispatches one request on the engine's paths.
func activitypub_route(c *gin.Context) bool {
	path, method := c.Request.URL.Path, c.Request.Method
	c.Header("Access-Control-Allow-Origin", "*")
	if method == "OPTIONS" {
		c.Header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Accept, Content-Type, Signature, Digest, Date")
		c.AbortWithStatus(http.StatusNoContent)
		return true
	}
	debug("ActivityPub %s %s from %s (%s)", method, path, c.ClientIP(), c.Request.UserAgent())
	switch {
	case path == "/.well-known/webfinger" && method == "GET":
		activitypub_webfinger(c)
	case path == "/.well-known/host-meta" && method == "GET":
		activitypub_host_meta(c)
	case path == "/.well-known/nodeinfo" && method == "GET":
		c.JSON(http.StatusOK, map[string]any{"links": []map[string]any{{"rel": activitypub_nodeinfo, "href": activitypub_base(c) + "/nodeinfo/2.1"}}})
	case path == "/nodeinfo/2.1" && method == "GET":
		activitypub_nodeinfo_document(c)
	case strings.HasPrefix(path, "/~"):
		activitypub_actor_route(c)
	case strings.HasPrefix(path, "/@"):
		activitypub_handle_route(c)
	default:
		respond_error(c, http.StatusNotFound, "not_found", "errors.not_found", nil)
	}
	return true
}

// activitypub_host is the host a request arrived on, lowercased and without a
// port: the domain whose accounts it asks about.
func activitypub_host(c *gin.Context) string {
	host := strings.ToLower(c.Request.Host)
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	return host
}

// activitypub_base is the https address of the host a request arrived on, as
// the links a document carries are built from it.
func activitypub_base(c *gin.Context) string {
	return "https://" + activitypub_host(c)
}

// activitypub_signing returns a context whose lookups are signed as a hosted
// account, for servers that give their documents to signed requests only, as
// GoToSocial and Mastodon in secure mode do. The context itself when the
// account has no key.
func activitypub_signing(ctx context.Context, user *User, account string) context.Context {
	actor_id := account_actor(user, account)
	key := keypair_get(user, account, key_signing)
	if actor_id == "" || key == nil {
		return ctx
	}
	return resolve_signing(ctx, func(r *http.Request) {
		signature_sign(r, actor_id+"#main-key", key.Private, nil)
	})
}

// activitypub_wants says whether a request asks for ActivityPub JSON rather
// than a page: Accept names activity+json or ld+json, or names no HTML.
func activitypub_wants(c *gin.Context) bool {
	accept := c.GetHeader("Accept")
	if strings.Contains(accept, "activity+json") || strings.Contains(accept, "ld+json") {
		return true
	}
	return accept != "" && !strings.Contains(accept, "text/html") && !strings.Contains(accept, "*/*")
}

// activitypub_json answers an ActivityPub document.
func activitypub_json(c *gin.Context, status int, document any) {
	c.Header("Content-Type", activitypub_type+"; charset=utf-8")
	c.Status(status)
	c.Writer.Write([]byte(json_encode(document)))
}

// activitypub_page_url is the person's public profile in the people app, the
// browser's destination for an actor or a handle.
func activitypub_page_url(user *User) string {
	if identity := user.identity(); identity != nil && identity.Fingerprint != "" {
		return "/people/" + identity.Fingerprint
	}
	return "/people"
}

// === Discovery ===

// activitypub_webfinger answers a WebFinger query for an account on a domain
// this server serves: acct:handle@domain, or the actor's id or handle page.
func activitypub_webfinger(c *gin.Context) {
	resource := strings.TrimSpace(c.Query("resource"))
	var a *actor
	switch {
	case strings.HasPrefix(resource, "acct:"):
		name, domain, found := strings.Cut(strings.TrimPrefix(resource, "acct:"), "@")
		if found {
			a = actor_by_handle(domain, name)
		}
	case strings.HasPrefix(resource, "https://"):
		u, err := url.Parse(resource)
		if err == nil {
			switch {
			case strings.HasPrefix(u.Path, "/~"):
				a = actor_get(strings.TrimPrefix(u.Path, "/~"))
			case strings.HasPrefix(u.Path, "/@"):
				a = actor_by_handle(u.Hostname(), strings.TrimPrefix(u.Path, "/@"))
			}
			if a != nil && (a.Domain != strings.ToLower(u.Hostname()) || a.Deleted != 0) {
				a = nil
			}
		}
	}
	if a == nil || a.Network != "fediverse" {
		respond_error(c, http.StatusNotFound, "not_found", "errors.not_found", nil)
		return
	}
	user, row := actor_account(a.ID)
	if user == nil {
		respond_error(c, http.StatusNotFound, "not_found", "errors.not_found", nil)
		return
	}
	id := account_url(a.Domain, a.ID)
	page := "https://" + a.Domain + "/@" + a.Handle
	jrd := map[string]any{
		"subject": "acct:" + a.Handle + "@" + a.Domain,
		"aliases": []string{id, page},
		"links": []map[string]any{
			{"rel": "self", "type": activitypub_type, "href": id},
			{"rel": "http://webfinger.net/rel/profile-page", "type": "text/html", "href": page},
			{"rel": activitypub_subscribe, "template": "https://" + a.Domain + "/~/authorize?uri={uri}"},
		},
	}
	// A community tells a server looking a name up what kind of actor it will
	// find, as Lemmy's WebFinger does, so a person and a community of the same
	// name on two domains are told apart.
	if account_group(row) {
		jrd["links"].([]map[string]any)[0]["properties"] = map[string]any{activitystreams + "#type": "Group"}
	}
	c.Header("Content-Type", activitypub_jrd+"; charset=utf-8")
	c.Status(http.StatusOK)
	c.Writer.Write([]byte(json_encode(jrd)))
}

// activitypub_host_meta answers host-meta: where WebFinger is.
func activitypub_host_meta(c *gin.Context) {
	host_meta_document(c, true)
}

// host_meta_engine answers host-meta (RFC 6415) for whichever network needs
// it: where WebFinger is while the fediverse is on, and the host's XMPP web
// transports while XMPP is (XEP-0156), as XML and as JSON, to any origin.
func host_meta_engine() *engine {
	return &engine{
		name:     "hostmeta",
		prefixes: []string{"/.well-known/host-meta", "/.well-known/host-meta.json"},
		handler:  host_meta_route,
	}
}

// host_meta_route answers host-meta where a network that uses it is on.
func host_meta_route(c *gin.Context) bool {
	c.Header("Access-Control-Allow-Origin", "*")
	if c.Request.Method != http.MethodGet || (!network_enabled("fediverse") && !network_enabled("xmpp")) {
		respond_error(c, http.StatusNotFound, "not_found", "errors.not_found", nil)
		return true
	}
	host_meta_document(c, network_enabled("fediverse"))
	return true
}

// host_meta_document writes host-meta: the WebFinger template where asked,
// and the XMPP links the host has.
func host_meta_document(c *gin.Context, lrdd bool) {
	template := activitypub_base(c) + "/.well-known/webfinger?resource={uri}"
	links := xmpp_host_links(activitypub_host(c))
	if strings.HasSuffix(c.Request.URL.Path, ".json") {
		list := []map[string]any{}
		if lrdd {
			list = append(list, map[string]any{"rel": "lrdd", "template": template})
		}
		for _, link := range links {
			list = append(list, map[string]any{"rel": link[0], "href": link[1]})
		}
		c.Header("Content-Type", "application/json; charset=utf-8")
		c.Status(http.StatusOK)
		c.Writer.Write([]byte(json_encode(map[string]any{"links": list})))
		return
	}
	text := `<?xml version="1.0" encoding="UTF-8"?>
<XRD xmlns="http://docs.oasis-open.org/ns/xri/xrd-1.0">
`
	if lrdd {
		text += `  <Link rel="lrdd" template="` + html.EscapeString(template) + `"/>
`
	}
	for _, link := range links {
		text += `  <Link rel="` + html.EscapeString(link[0]) + `" href="` + html.EscapeString(link[1]) + `"/>
`
	}
	c.Header("Content-Type", "application/xrd+xml; charset=utf-8")
	c.String(http.StatusOK, text+"</XRD>\n")
}

// activitypub_nodeinfo_document answers NodeInfo 2.1: the software, the
// protocol, whether sign-up is open, how many users, and the operator's name
// and the server's description from the system settings.
func activitypub_nodeinfo_document(c *gin.Context) {
	users := int64(0)
	if row, _ := db_open("db/users.db").row("select count(*) as n from users where status='active'"); row != nil {
		users = row_int(row, "n")
	}
	c.Header("Content-Type", `application/json; profile="`+activitypub_nodeinfo+`#"; charset=utf-8`)
	c.Status(http.StatusOK)
	c.Writer.Write([]byte(json_encode(map[string]any{
		"version":           "2.1",
		"software":          map[string]any{"name": "mochi", "version": build_version, "homepage": "https://mochi-os.org"},
		"protocols":         []string{"activitypub"},
		"services":          map[string]any{"inbound": []string{}, "outbound": []string{}},
		"openRegistrations": setting_signup_enabled(),
		"usage":             map[string]any{"users": map[string]any{"total": users}},
		"metadata": map[string]any{
			"nodeName":        setting_effective("operator_name"),
			"nodeDescription": setting_effective("description"),
		},
	})))
}

// === The handle address ===

// activitypub_handle_route answers /@<handle>: a redirect to the actor's id
// for a server, to the person's public profile for a browser.
func activitypub_handle_route(c *gin.Context) {
	name := strings.TrimPrefix(c.Request.URL.Path, "/@")
	if i := strings.IndexByte(name, '/'); i >= 0 {
		name = name[:i]
	}
	a := actor_by_handle(activitypub_host(c), name)
	if a == nil {
		respond_error(c, http.StatusNotFound, "not_found", "errors.not_found", nil)
		return
	}
	user, _ := actor_account(a.ID)
	if user == nil {
		respond_error(c, http.StatusNotFound, "not_found", "errors.not_found", nil)
		return
	}
	if activitypub_wants(c) {
		c.Redirect(http.StatusFound, account_url(a.Domain, a.ID))
		return
	}
	_, row := actor_account(a.ID)
	c.Redirect(http.StatusFound, activitypub_browser_url(user, row))
}

// activitypub_browser_url is where a browser is sent for an actor: a group's
// own page where it names one, else the person's profile.
func activitypub_browser_url(user *User, row map[string]any) string {
	if account_group(row) {
		if page, _ := account_data(row)["page"].(string); page != "" {
			return page
		}
	}
	return activitypub_page_url(user)
}

// === The actor and its collections ===

// activitypub_actor_route answers everything under /~: the shared inbox and
// the subscribe landing under /~/, and an actor's document, inbox, outbox,
// collections and objects under /~<id>/.
func activitypub_actor_route(c *gin.Context) {
	rest := strings.TrimPrefix(c.Request.URL.Path, "/~")
	id, tail, _ := strings.Cut(rest, "/")
	method := c.Request.Method
	if id == "" {
		switch {
		case tail == "inbox" && method == "POST":
			activitypub_inbox(c, nil, nil)
		case tail == "authorize" && method == "GET":
			activitypub_authorize(c)
		default:
			respond_error(c, http.StatusNotFound, "not_found", "errors.not_found", nil)
		}
		return
	}
	a := actor_get(id)
	if a == nil || a.Network != "fediverse" {
		respond_error(c, http.StatusNotFound, "not_found", "errors.not_found", nil)
		return
	}
	if a.Deleted != 0 {
		// Gone, not not-found: the other server stops asking and forgets. The
		// tombstone still carries the key, so the Delete of the actor that
		// went out signed with it can be checked by a server fetching it.
		if tail == "" && activitypub_wants(c) {
			id := account_url(a.Domain, a.ID)
			tombstone := map[string]any{"@context": activitypub_context, "id": id, "type": "Tombstone", "formerType": "Person", "deleted": time.Unix(a.Deleted, 0).UTC().Format(time.RFC3339)}
			if public := key_rsa_public_of(a.Key); public != "" {
				tombstone["publicKey"] = map[string]any{"id": id + "#main-key", "owner": id, "publicKeyPem": public}
			}
			activitypub_json(c, http.StatusGone, tombstone)
			return
		}
		c.AbortWithStatus(http.StatusGone)
		return
	}
	user, row := actor_account(id)
	if user == nil {
		respond_error(c, http.StatusNotFound, "not_found", "errors.not_found", nil)
		return
	}
	actor_id := account_url(a.Domain, a.ID)
	switch {
	case tail == "" && method == "GET":
		if !activitypub_wants(c) {
			c.Redirect(http.StatusFound, activitypub_browser_url(user, row))
			return
		}
		activitypub_json(c, http.StatusOK, activitypub_actor(user, a, row))
	case tail == "moderators" && method == "GET":
		// A group's moderators, the collection its document attributes it to.
		if !account_group(row) {
			respond_error(c, http.StatusNotFound, "not_found", "errors.not_found", nil)
			return
		}
		moderators := object_strings(account_data(row)["moderators"])
		items := make([]any, 0, len(moderators))
		for _, moderator := range moderators {
			items = append(items, moderator)
		}
		activitypub_json(c, http.StatusOK, map[string]any{"@context": activitystreams, "id": actor_id + "/moderators", "type": "OrderedCollection", "totalItems": len(items), "orderedItems": items})
	case tail == "inbox" && method == "POST":
		activitypub_inbox(c, user, a)
	case tail == "outbox" && method == "GET":
		activitypub_outbox(c, user, a, row)
	case tail == "followers" && method == "GET":
		n, _ := db_user(user, "user").row("select count(*) as n from followers where account=?", a.ID)
		activitypub_json(c, http.StatusOK, map[string]any{"@context": activitystreams, "id": actor_id + "/followers", "type": "OrderedCollection", "totalItems": row_int(n, "n")})
	case tail == "following" && method == "GET":
		n, _ := db_user(user, "user").row("select count(*) as n from following where account=? and accepted>0", a.ID)
		activitypub_json(c, http.StatusOK, map[string]any{"@context": activitystreams, "id": actor_id + "/following", "type": "OrderedCollection", "totalItems": row_int(n, "n")})
	case tail == "featured" && method == "GET":
		activitypub_featured(c, user, a, row)
	case method == "GET" && tail != "" && !strings.Contains(tail, "/"):
		activitypub_object(c, user, a, row, tail)
	case method == "GET" && strings.HasSuffix(tail, "/activity") && strings.Count(tail, "/") == 1:
		// The Create an object went out in, at the id the outbox and a
		// group's Announce give it: a server told of the activity fetches it
		// here before it takes the object.
		activitypub_create(c, user, a, row, strings.TrimSuffix(tail, "/activity"))
	case method == "GET" && strings.HasPrefix(tail, "media/") && strings.Count(tail, "/") == 1:
		// A file one of the account's objects carries (media.go).
		media_serve(c, user, a.ID, strings.TrimPrefix(tail, "media/"))
	default:
		respond_error(c, http.StatusNotFound, "not_found", "errors.not_found", nil)
	}
}

// activitypub_create answers /~<id>/<object>/activity: the Create activity of
// an object the account published, as the outbox lists it; gone with the
// object.
func activitypub_create(c *gin.Context, user *User, a *actor, row map[string]any, id string) {
	db := db_user(user, "user")
	object_row, _ := db.row("select id, app, type, addressing, created, updated, deleted from objects where id=? and account=?", id, a.ID)
	if object_row == nil || row_int(object_row, "deleted") != 0 || object_activity(object_row) != nil || row_string(object_row, "type") == "Announce" {
		respond_error(c, http.StatusNotFound, "not_found", "errors.not_found", nil)
		return
	}
	object := activitypub_object_document(user, a, row, object_row)
	if object == nil {
		respond_error(c, http.StatusNotFound, "not_found", "errors.not_found", nil)
		return
	}
	remote, _ := object["id"].(string)
	activitypub_json(c, http.StatusOK, map[string]any{
		"@context":  activitypub_context,
		"id":        remote + "/activity",
		"type":      "Create",
		"actor":     account_url(a.Domain, a.ID),
		"published": object["published"],
		"to":        object["to"],
		"cc":        object["cc"],
		"object":    object,
	})
}

// activitypub_authorize is the landing of this server's subscribe template:
// another server sends a Mochi user here with the address of someone to follow,
// and the feeds app's lookup takes it from there.
func activitypub_authorize(c *gin.Context) {
	uri := strings.TrimSpace(c.Query("uri"))
	if uri == "" || len(uri) > 2048 {
		respond_error(c, http.StatusBadRequest, "invalid", "errors.invalid_request", nil)
		return
	}
	c.Redirect(http.StatusFound, "/feeds/find?link="+url.QueryEscape(uri))
}

// activitypub_profile asks the app that describes people (people, through
// activitypub/profile) for what the actor document shows beyond the account:
// the summary as HTML, the avatar and banner addresses and the links. Empty
// when no app answers.
func activitypub_profile(user *User, account string) map[string]any {
	person := ""
	if identity := user.identity(); identity != nil {
		person = identity.ID
	}
	for _, a := range apps_for_function(user, "activitypub/profile") {
		result, err := activitypub_call(user, a, "activitypub/profile", Map{"account": account, "person": person})
		if err != nil {
			debug("ActivityPub profile from %s: %v", a.id, err)
			continue
		}
		if profile, ok := result.(map[string]any); ok {
			return profile
		}
	}
	return map[string]any{}
}

// activitypub_actor builds an account's document: a person's, or a group's
// for a community (phase 20).
func activitypub_actor(user *User, a *actor, row map[string]any) map[string]any {
	if account_group(row) {
		return activitypub_group(user, a, row)
	}
	id := account_url(a.Domain, a.ID)
	data := account_data(row)
	profile := activitypub_profile(user, a.ID)
	name, _ := row["label"].(string)
	if name == "" {
		name, _ = profile["name"].(string)
	}
	if name == "" {
		if identity := user.identity(); identity != nil {
			name = identity.Name
		}
	}
	kind := "Person"
	if automated, _ := data["automated"].(bool); automated {
		kind = "Service"
	}
	public := true
	if identity := user.identity(); identity != nil && identity.Privacy == "private" {
		public = false
	}
	document := map[string]any{
		"@context":                  activitypub_context,
		"id":                        id,
		"type":                      kind,
		"preferredUsername":         a.Handle,
		"name":                      name,
		"url":                       "https://" + a.Domain + "/@" + a.Handle,
		"inbox":                     id + "/inbox",
		"outbox":                    id + "/outbox",
		"followers":                 id + "/followers",
		"following":                 id + "/following",
		"featured":                  id + "/featured",
		"endpoints":                 map[string]any{"sharedInbox": "https://" + a.Domain + "/~/inbox"},
		"manuallyApprovesFollowers": false,
		"discoverable":              public,
		"indexable":                 public,
		"published":                 time.Unix(a.Created, 0).UTC().Format(time.RFC3339),
	}
	if key := keypair_get(user, a.ID, key_signing); key != nil {
		document["publicKey"] = map[string]any{"id": id + "#main-key", "owner": id, "publicKeyPem": key.Public}
	}
	// The person's other identities, which a Move from one of them to this
	// account must find here; and where this account has moved to.
	if aliases := object_strings(data["aliases"]); len(aliases) > 0 {
		document["alsoKnownAs"] = aliases
	}
	if moved, _ := data["moved"].(string); moved != "" {
		document["movedTo"] = moved
	}
	if summary, _ := profile["summary"].(string); summary != "" {
		document["summary"] = summary
	}
	if avatar, _ := profile["avatar"].(string); avatar != "" {
		document["icon"] = map[string]any{"type": "Image", "url": avatar}
	}
	if banner, _ := profile["banner"].(string); banner != "" {
		document["image"] = map[string]any{"type": "Image", "url": banner}
	}
	links, _ := profile["links"].([]any)
	var attachments []map[string]any
	for _, item := range links {
		link, _ := item.(map[string]any)
		label, _ := link["name"].(string)
		href, _ := link["url"].(string)
		if href == "" || !strings.HasPrefix(href, "https://") {
			continue
		}
		if label == "" {
			label = href
		}
		attachments = append(attachments, map[string]any{
			"type":  "PropertyValue",
			"name":  label,
			"value": `<a href="` + html.EscapeString(href) + `" rel="me nofollow noopener noreferrer" target="_blank">` + html.EscapeString(strings.TrimPrefix(href, "https://")) + `</a>`,
		})
	}
	if len(attachments) > 0 {
		document["attachment"] = attachments
	}
	return document
}

// activitypub_app_object asks the app that published an object for it, as it
// stands now, or nil.
func activitypub_app_object(user *User, app_id string, account string, remote string) map[string]any {
	a := app_by_id(app_id)
	if a == nil {
		return nil
	}
	result, err := activitypub_call(user, a, "activitypub/object", Map{"account": account, "id": remote})
	if err != nil {
		debug("ActivityPub object %s from %s: %v", remote, app_id, err)
		return nil
	}
	object, _ := result.(map[string]any)
	return object
}

// activitypub_object_document is an object as served: the app's body under its
// id, with what core keeps of it filled in.
func activitypub_object_document(user *User, a *actor, row map[string]any, object_row map[string]any) map[string]any {
	actor_id := account_url(a.Domain, a.ID)
	id := row_string(object_row, "id")
	remote := actor_id + "/" + id
	app_id := row_string(object_row, "app")
	object := activitypub_app_object(user, app_id, a.ID, remote)
	if object == nil {
		return nil
	}
	delete(object, "@context")
	object["id"] = remote
	object["attributedTo"] = actor_id
	if _, set := object["type"]; !set {
		object["type"] = row_string(object_row, "type")
	}
	object["published"] = time.Unix(row_int(object_row, "created"), 0).UTC().Format(time.RFC3339)
	if updated := row_int(object_row, "updated"); updated > 0 {
		object["updated"] = time.Unix(updated, 0).UTC().Format(time.RFC3339)
	}
	to, cc := object_stored_addressing(object_row)
	if _, set := object["to"]; !set && len(to)+len(cc) > 0 {
		object["to"], object["cc"] = to, cc
	}
	return object
}

// activitypub_announce is a boost row as the activity it is, or nil for any
// other row: an activity of the account's own, not an object an app holds,
// so the app is never asked for it.
func activitypub_announce(actor_id string, object_row map[string]any) map[string]any {
	if row_string(object_row, "type") != "Announce" {
		return nil
	}
	to, cc := object_stored_addressing(object_row)
	return map[string]any{
		"id":        actor_id + "/" + row_string(object_row, "id"),
		"type":      "Announce",
		"actor":     actor_id,
		"published": time.Unix(row_int(object_row, "created"), 0).UTC().Format(time.RFC3339),
		"to":        to,
		"cc":        cc,
		"object":    object_announced(object_row),
	}
}

// activitypub_object answers /~<id>/<object>: the object from its app, a
// boost as its Announce, a tombstone where it was deleted, a redirect to its
// page for a browser.
func activitypub_object(c *gin.Context, user *User, a *actor, row map[string]any, id string) {
	db := db_user(user, "user")
	object_row, _ := db.row("select id, app, type, addressing, created, updated, deleted from objects where id=? and account=?", id, a.ID)
	if object_row == nil {
		respond_error(c, http.StatusNotFound, "not_found", "errors.not_found", nil)
		return
	}
	remote := account_url(a.Domain, a.ID) + "/" + id
	if row_int(object_row, "deleted") != 0 {
		activitypub_json(c, http.StatusGone, map[string]any{"@context": activitystreams, "id": remote, "type": "Tombstone", "formerType": row_string(object_row, "type"), "deleted": time.Unix(row_int(object_row, "deleted"), 0).UTC().Format(time.RFC3339)})
		return
	}
	if activity := object_activity(object_row); activity != nil {
		activity["@context"] = activitypub_context
		activitypub_json(c, http.StatusOK, activity)
		return
	}
	if announce := activitypub_announce(account_url(a.Domain, a.ID), object_row); announce != nil {
		announce["@context"] = activitypub_context
		activitypub_json(c, http.StatusOK, announce)
		return
	}
	object := activitypub_object_document(user, a, row, object_row)
	if object == nil {
		respond_error(c, http.StatusNotFound, "not_found", "errors.not_found", nil)
		return
	}
	if !activitypub_wants(c) {
		if page, _ := object["url"].(string); page != "" {
			c.Redirect(http.StatusFound, page)
		} else {
			c.Redirect(http.StatusFound, activitypub_page_url(user))
		}
		return
	}
	object["@context"] = activitypub_context
	activitypub_json(c, http.StatusOK, object)
}

// activitypub_featured answers the account's pinned posts, which servers ask
// for on meeting an account: the objects each app that published for the
// account names through activitypub/featured, in the order the apps give
// them.
func activitypub_featured(c *gin.Context, user *User, a *actor, row map[string]any) {
	db := db_user(user, "user")
	actor_id := account_url(a.Domain, a.ID)
	items := []any{}
	apps_rows, _ := db.rows("select distinct app from objects where account=? and deleted=0 and app!=''", a.ID)
	for _, app_row := range apps_rows {
		app := app_by_id(row_string(app_row, "app"))
		if app == nil {
			continue
		}
		result, err := activitypub_call(user, app, "activitypub/featured", Map{"account": a.ID})
		if err != nil {
			debug("ActivityPub featured from %s: %v", app.id, err)
			continue
		}
		for _, remote := range object_strings(result) {
			id, object_row := object_get(db, actor_id, a.ID, remote)
			if id == "" {
				// A group pins its members' posts, which are objects of
				// theirs: named by id for the reader to fetch.
				if account_group(row) && activitypub_remote_ok(remote) {
					items = append(items, remote)
				}
				continue
			}
			if object_row["deleted"].(int64) != 0 {
				continue
			}
			object_row["app"] = app.id
			if object := activitypub_object_document(user, a, row, object_row); object != nil {
				items = append(items, object)
			}
		}
	}
	activitypub_json(c, http.StatusOK, map[string]any{"@context": activitypub_context, "id": actor_id + "/featured", "type": "OrderedCollection", "totalItems": len(items), "orderedItems": items})
}

// activitypub_outbox answers the account's outbox: the collection with its
// count and first page, and a page of Create activities wrapping the objects
// as the apps give them now, newest first.
func activitypub_outbox(c *gin.Context, user *User, a *actor, row map[string]any) {
	db := db_user(user, "user")
	actor_id := account_url(a.Domain, a.ID)
	outbox := actor_id + "/outbox"
	// The outbox shows what the account published for everyone, as
	// Mastodon's does: a direct message or a post for followers alone is not
	// listed, nor counted.
	public := "%" + json_encode(activitystreams_public) + "%"
	if c.Query("page") == "" {
		n, _ := db.row("select count(*) as n from objects where account=? and deleted=0 and addressing like ?", a.ID, public)
		activitypub_json(c, http.StatusOK, map[string]any{"@context": activitystreams, "id": outbox, "type": "OrderedCollection", "totalItems": row_int(n, "n"), "first": outbox + "?page=true"})
		return
	}
	before := int64(0)
	if s := c.Query("before"); s != "" {
		before, _ = strconv.ParseInt(s, 10, 64)
	}
	query := "select id, app, type, addressing, created, updated, deleted from objects where account=? and deleted=0 and addressing like ?"
	args := []any{a.ID, public}
	if before > 0 {
		query += " and created<?"
		args = append(args, before)
	}
	query += " order by created desc limit ?"
	args = append(args, activitypub_page)
	rows, _ := db.rows(query, args...)
	items := []map[string]any{}
	last := int64(0)
	for _, object_row := range rows {
		last = row_int(object_row, "created")
		if activity := object_activity(object_row); activity != nil {
			// An activity the account sent whole (mochi.account.activity),
			// kept as it went out.
			items = append(items, activity)
			continue
		}
		if announce := activitypub_announce(actor_id, object_row); announce != nil {
			items = append(items, announce)
			continue
		}
		object := activitypub_object_document(user, a, row, object_row)
		if object == nil {
			continue
		}
		remote, _ := object["id"].(string)
		items = append(items, map[string]any{
			"id":        remote + "/activity",
			"type":      "Create",
			"actor":     actor_id,
			"published": object["published"],
			"to":        object["to"],
			"cc":        object["cc"],
			"object":    object,
		})
	}
	page := map[string]any{"@context": activitypub_context, "id": outbox + "?page=true", "type": "OrderedCollectionPage", "partOf": outbox, "orderedItems": items}
	if before > 0 {
		page["id"] = outbox + "?page=true&before=" + strconv.FormatInt(before, 10)
	}
	if len(rows) == activitypub_page && last > 0 {
		page["next"] = outbox + "?page=true&before=" + strconv.FormatInt(last, 10)
	}
	activitypub_json(c, http.StatusOK, page)
}

// === The inbox ===

// activitypub_actor_of reads an activity's actor: a string, or an object with
// an id.
func activitypub_actor_of(v any) string {
	switch value := v.(type) {
	case string:
		return value
	case map[string]any:
		id, _ := value["id"].(string)
		return id
	}
	return ""
}

// activitypub_remote_ok says whether an identifier is a remote fediverse id:
// an https URL, or an http one on a loopback host under the development
// switch that admits private addresses, so a stand-in server under test can
// act.
func activitypub_remote_ok(id string) bool {
	if author_network(id) == "fediverse" {
		return true
	}
	if url_allow_private && strings.HasPrefix(id, "http://") {
		u, err := url.Parse(id)
		return err == nil && u.Hostname() != "" && u.User == nil
	}
	return false
}

// activitypub_host_of is the host a remote id or key URL names, lowercased.
func activitypub_host_of(id string) string {
	u, err := url.Parse(id)
	if err != nil {
		return ""
	}
	return strings.ToLower(u.Hostname())
}

// activitypub_received records an activity id as seen by an account, and says
// whether that account saw it before. An activity reaches the server once per
// inbox it is sent to - an account's own, and the shared inbox of each domain
// the server's accounts are on - so it is a duplicate only for an account it
// was already given to.
func activitypub_received(id string, account string) bool {
	db := networks_database()
	if seen, _ := db.exists("select 1 from seen where id=? and account=?", id, account); seen {
		return true
	}
	db.exec("insert or ignore into seen (id, account, created) values (?, ?, ?)", id, account, now())
	return false
}

// activitypub_inbox takes one activity for an account, or for the shared
// inbox (user and a nil), and answers 202 for whatever it takes and drops.
// Order: the body and its shape, the actor's host against the blocks, the
// signature, the duplicate check, then the dispatch.
func activitypub_inbox(c *gin.Context, user *User, a *actor) {
	body, err := io.ReadAll(io.LimitReader(c.Request.Body, activitypub_body_maximum+1))
	if err != nil || len(body) > activitypub_body_maximum {
		respond_error(c, http.StatusRequestEntityTooLarge, "too_large", "errors.body_too_large", nil)
		return
	}
	var activity map[string]any
	if err := json.Unmarshal(body, &activity); err != nil || activity == nil {
		respond_error(c, http.StatusBadRequest, "invalid", "errors.invalid_request", nil)
		return
	}
	kind, _ := activity["type"].(string)
	id, _ := activity["id"].(string)
	remote := activitypub_actor_of(activity["actor"])
	if kind == "" || !activitypub_remote_ok(remote) {
		respond_error(c, http.StatusBadRequest, "invalid", "errors.invalid_request", nil)
		return
	}
	host := activitypub_host_of(remote)

	// A server tells every server it knows when one of its accounts is
	// deleted. One whose actor no server here follows or is followed by is
	// nobody to us, and its key is gone with it, so the request is taken and
	// dropped without a fetch.
	if kind == "Delete" && activitypub_actor_of(activity["object"]) == remote && !activitypub_known(user, a, remote) {
		c.AbortWithStatus(http.StatusAccepted)
		return
	}

	if err := host_allowed(user, "fediverse", host); err != nil {
		// A blocked host learns nothing: its activity is taken and dropped.
		debug("ActivityPub inbox dropped %s from %s: %v", kind, remote, err)
		c.AbortWithStatus(http.StatusAccepted)
		return
	}

	// The accounts the activity is for: the one whose inbox it is, or for the
	// shared inbox those it addresses or follows. The first signs the fetches
	// the checks below make, since the sender's server may give its documents
	// to signed requests only.
	targets := []*actor{}
	if a != nil {
		targets = append(targets, a)
	} else {
		targets = activitypub_targets(remote, activity)
	}
	ctx := c.Request.Context()
	if len(targets) > 0 {
		if target_user, _ := actor_account(targets[0].ID); target_user != nil {
			ctx = activitypub_signing(ctx, target_user, targets[0].ID)
		}
	}

	key_id, err := signature_verify(ctx, c.Request, body, signature_key_fetch)
	if err != nil {
		debug("ActivityPub inbox refused %s from %s: %v", kind, remote, err)
		if !activitypub_deferred(c, err) {
			respond_error(c, http.StatusUnauthorized, "signature", "errors.signature_invalid", nil)
		}
		return
	}
	// The key must belong to the actor: the same host at least, and the actor
	// document the key came from must name this actor as its owner.
	if activitypub_host_of(key_id) != host || !activitypub_key_owner(ctx, key_id, remote) {
		debug("ActivityPub inbox refused %s: key %s is not %s's", kind, key_id, remote)
		respond_error(c, http.StatusUnauthorized, "signature", "errors.signature_invalid", nil)
		return
	}

	var later error
	for _, target := range targets {
		if id != "" && activitypub_received(id, target.ID) {
			continue
		}
		target_user, _ := actor_account(target.ID)
		if target_user == nil {
			continue
		}
		if err := host_allowed(target_user, "fediverse", host); err != nil {
			continue
		}
		// A blocked person is nobody to the user; a muted one may still
		// follow and unfollow, and nothing else of theirs is taken.
		if actor_blocked(target_user, remote) || (actor_muted(target_user, remote) && !activitypub_following(kind, activity)) {
			debug("ActivityPub inbox dropped %s from %s: blocked or muted by %s", kind, remote, target_user.UID)
			continue
		}
		if err := activitypub_dispatch(activitypub_signing(c.Request.Context(), target_user, target.ID), target_user, target, remote, kind, activity, body); err != nil {
			// Not taken for this account for now: forgotten as seen, so
			// the activity sent again is.
			networks_database().exec("delete from seen where id=? and account=?", id, target.ID)
			later = err
		}
	}
	if later != nil && activitypub_deferred(c, later) {
		return
	}
	c.AbortWithStatus(http.StatusAccepted)
}

// activitypub_deferred answers a request as one to send again later when what
// it needs could not be read for now - this server's own limit on requests to
// the sender's host, which a burst of new people from one large server
// reaches, or that host failing: 429 or 503 with Retry-After, which senders
// retry, where a 401 or a 202 that took nothing loses the activity. Says
// whether it answered.
func activitypub_deferred(c *gin.Context, err error) bool {
	var limited *RateLimitError
	switch {
	case errors.As(err, &limited):
		retry := limited.Retry
		if retry <= 0 {
			retry = int(resolve_cache_transient.Seconds())
		}
		c.Header("Retry-After", strconv.Itoa(retry))
		c.AbortWithStatus(http.StatusTooManyRequests)
	case resolve_transient(err):
		c.Header("Retry-After", strconv.Itoa(int(resolve_cache_transient.Seconds())))
		c.AbortWithStatus(http.StatusServiceUnavailable)
	default:
		return false
	}
	return true
}

// activitypub_following says whether an activity is a Follow or the Undo of
// one.
func activitypub_following(kind string, activity map[string]any) bool {
	if kind == "Follow" {
		return true
	}
	if kind == "Undo" {
		object, _ := activity["object"].(map[string]any)
		return object != nil && object["type"] == "Follow"
	}
	return false
}

// activitypub_known says whether a remote actor is anyone to the account, or
// to any account for the shared inbox: a follower, or someone followed.
func activitypub_known(user *User, a *actor, remote string) bool {
	if a != nil && user != nil {
		db := db_user(user, "user")
		follower, _ := db.exists("select 1 from followers where account=? and actor=?", a.ID, remote)
		followed, _ := db.exists("select 1 from following where account=? and actor=?", a.ID, remote)
		return follower || followed
	}
	rows, _ := networks_database().rows("select distinct user from actors where deleted=0")
	for _, row := range rows {
		u := user_by_uid(row_string(row, "user"))
		if u == nil || user_quiet(u) {
			continue
		}
		db := db_user(u, "user")
		follower, _ := db.exists("select 1 from followers where actor=?", remote)
		followed, _ := db.exists("select 1 from following where actor=?", remote)
		if follower || followed {
			return true
		}
	}
	return false
}

// activitypub_key_owner checks that the actor document a key came from is the
// actor's: the key's URL without its fragment is the actor id, as Mastodon and
// Lemmy publish keys, or the document at the key's URL names the actor as the
// key's owner, as GoToSocial does.
func activitypub_key_owner(ctx context.Context, key_id string, remote string) bool {
	address, _, _ := strings.Cut(key_id, "#")
	if address == remote {
		return true
	}
	owner, err := signature_key_owner(ctx, key_id)
	return err == nil && owner == remote
}

// activitypub_targets finds the accounts a shared-inbox activity is for: those
// named by its object (a Follow), to, cc or audience, directly or as their
// followers collection, and those following its actor, to whom a post
// addressed to the actor's followers is delivered this way.
func activitypub_targets(remote string, activity map[string]any) []*actor {
	seen := map[string]bool{}
	var targets []*actor
	for _, a := range activitypub_followers_of(remote) {
		seen[a.ID] = true
		targets = append(targets, a)
	}
	consider := func(address string) {
		u, err := url.Parse(address)
		if err != nil || !strings.HasPrefix(u.Path, "/~") {
			return
		}
		id := strings.TrimPrefix(u.Path, "/~")
		id, _, _ = strings.Cut(id, "/")
		if id == "" || seen[id] {
			return
		}
		seen[id] = true
		if a := actor_get(id); a != nil && a.Deleted == 0 && a.Network == "fediverse" && a.Domain == strings.ToLower(u.Hostname()) {
			targets = append(targets, a)
		}
	}
	consider(activitypub_actor_of(activity["object"]))
	for _, key := range []string{"to", "cc", "audience"} {
		for _, address := range object_strings(activity[key]) {
			consider(address)
		}
	}
	if object, ok := activity["object"].(map[string]any); ok {
		for _, key := range []string{"to", "cc", "audience", "object"} {
			for _, address := range object_strings(object[key]) {
				consider(address)
			}
		}
	}
	return targets
}

// activitypub_followers_of lists the live accounts here that follow a remote
// actor, across users.
func activitypub_followers_of(remote string) []*actor {
	var found []*actor
	rows, _ := networks_database().rows("select distinct user from actors where deleted=0 and network='fediverse'")
	for _, row := range rows {
		u := user_by_uid(row_string(row, "user"))
		if u == nil || user_quiet(u) {
			continue
		}
		accounts, _ := db_user(u, "user").rows("select account from following where actor=?", remote)
		for _, account := range accounts {
			if a := actor_get(row_string(account, "account")); a != nil && a.Deleted == 0 && a.Network == "fediverse" {
				found = append(found, a)
			}
		}
	}
	return found
}

// activitypub_dispatch acts on one verified activity for one account: a
// Follow is accepted, an Undo of a Follow forgets the follower, an Accept or
// Reject settles a follow the account sent, a Move by someone followed sends
// the follow on to where they went, and anything else is queued for the apps.
// An error is a failure that may pass, for the sender to send again.
func activitypub_dispatch(ctx context.Context, user *User, a *actor, remote string, kind string, activity map[string]any, body []byte) error {
	actor_id := account_url(a.Domain, a.ID)
	db := db_user(user, "user")
	switch kind {
	case "Follow":
		if activitypub_actor_of(activity["object"]) != actor_id {
			return nil
		}
		r, err := author_resolve(ctx, remote)
		if err != nil || r.Inbox == "" {
			debug("ActivityPub follow from %s not resolved: %v", remote, err)
			if resolve_transient(err) {
				return err
			}
			return nil
		}
		db.exec("insert into followers (account, actor, inbox, shared, created) values (?, ?, ?, ?, ?) on conflict(account, actor) do update set inbox=excluded.inbox, shared=excluded.shared", a.ID, remote, r.Inbox, r.Shared, now())
		follow := map[string]any{"id": activity["id"], "type": "Follow", "actor": remote, "object": actor_id}
		accept := map[string]any{
			"@context": activitystreams,
			"id":       actor_id + "#accepts/follows/" + uid(),
			"type":     "Accept",
			"actor":    actor_id,
			"to":       []string{remote},
			"object":   follow,
		}
		deliver(user, a.ID, "fediverse", []string{r.Inbox}, []byte(json_encode(accept)))
		// The apps hear of the follow too, to tell the person.
		inbound_add(user, a.ID, remote, kind, body)
	case "Undo":
		object, _ := activity["object"].(map[string]any)
		if object == nil {
			return nil
		}
		if object["type"] == "Follow" && activitypub_actor_of(object["actor"]) == remote && activitypub_actor_of(object["object"]) == actor_id {
			db.exec("delete from followers where account=? and actor=?", a.ID, remote)
			return nil
		}
		inbound_add(user, a.ID, remote, kind, body)
	case "Accept":
		object, _ := activity["object"].(map[string]any)
		if object != nil && object["type"] == "Follow" && activitypub_actor_of(object["actor"]) == actor_id {
			db.exec("update following set accepted=? where account=? and actor=?", now(), a.ID, remote)
			return nil
		}
		inbound_add(user, a.ID, remote, kind, body)
	case "Reject":
		object, _ := activity["object"].(map[string]any)
		if object != nil && object["type"] == "Follow" && activitypub_actor_of(object["actor"]) == actor_id {
			db.exec("delete from following where account=? and actor=?", a.ID, remote)
			return nil
		}
		inbound_add(user, a.ID, remote, kind, body)
	case "Move":
		// A person the account follows has moved: the follow goes to where
		// they went, once their new identity names the old one as its own,
		// and the apps hear of it to follow suit.
		target := activitypub_actor_of(activity["target"])
		if activitypub_actor_of(activity["object"]) != remote || target == "" || target == remote {
			return nil
		}
		if following_move(ctx, user, a.ID, remote, target) {
			inbound_add(user, a.ID, remote, kind, body)
		}
	default:
		inbound_add(user, a.ID, remote, kind, body)
	}
	return nil
}

// === The inbound queue ===
//
// What the inbox takes beyond follows is queued per account and handed to the
// apps declaring activitypub/receive in batches, one account at a time in
// turn, so one account's flood never starves another's.

var inbound_wake_ch = make(chan struct{}, 1)

// inbound_add queues an activity for an account's apps.
func inbound_add(user *User, account string, remote string, kind string, body []byte) {
	networks_database().exec("insert into inbound (id, user, account, actor, type, body, created) values (?, ?, ?, ?, ?, ?, ?)", uid(), user.UID, account, remote, kind, string(body), now())
	select {
	case inbound_wake_ch <- struct{}{}:
	default:
	}
}

// inbound_manager hands queued activities to the apps, woken by the inbox or
// every inbound_poll.
func inbound_manager() {
	for {
		if inbound_process() > 0 {
			continue
		}
		select {
		case <-time.After(inbound_poll):
		case <-inbound_wake_ch:
		}
	}
}

var inbound_lock sync.Mutex

// inbound_process takes one batch for each account with rows waiting, oldest
// account first, and returns how many rows it handled. A row is deleted once
// offered to the apps; an app's error is logged and does not hold the queue.
func inbound_process() int {
	inbound_lock.Lock()
	defer inbound_lock.Unlock()
	db := networks_database()
	accounts, _ := db.rows("select account, user, min(created) as oldest from inbound group by account order by oldest limit 100")
	handled := 0
	for _, row := range accounts {
		account, user_id := row_string(row, "account"), row_string(row, "user")
		rows, _ := db.rows("select id, actor, type, body from inbound where account=? order by created limit ?", account, inbound_batch)
		if len(rows) == 0 {
			continue
		}
		ids := make([]string, 0, len(rows))
		activities := make([]any, 0, len(rows))
		for _, r := range rows {
			ids = append(ids, row_string(r, "id"))
			var activity any
			if err := json.Unmarshal([]byte(row_string(r, "body")), &activity); err == nil {
				activities = append(activities, map[string]any{"actor": row_string(r, "actor"), "type": row_string(r, "type"), "activity": activity})
			}
		}
		user := user_by_uid(user_id)
		if user != nil && !user_quiet(user) && len(activities) > 0 {
			for _, a := range apps_for_function(user, "activitypub/receive") {
				if _, err := activitypub_call(user, a, "activitypub/receive", Map{"account": account, "activities": activities}); err != nil {
					info("ActivityPub receive in %s for account %s: %v", a.id, account, err)
				}
			}
		}
		for _, id := range ids {
			db.exec("delete from inbound where id=?", id)
		}
		handled += len(ids)
	}
	return handled
}

// === Removal ===

// activitypub_account_remove is the fediverse side of removing a hosted
// account: a Delete of the actor goes to every follower's server, signed with
// the key the tombstone keeps, and the id answers gone from now on. The
// account's own rows go with the account.
func activitypub_account_remove(user *User, id string) {
	a := actor_get(id)
	if a == nil || a.Network != "fediverse" || a.Deleted != 0 {
		return
	}
	actor_id := account_url(a.Domain, a.ID)
	db := db_user(user, "user")
	inboxes := []string{}
	seen := map[string]bool{}
	rows, _ := db.rows("select inbox, shared from followers where account=?", a.ID)
	for _, row := range rows {
		inbox := row_string(row, "shared")
		if inbox == "" {
			inbox = row_string(row, "inbox")
		}
		if inbox != "" && !seen[inbox] {
			seen[inbox] = true
			inboxes = append(inboxes, inbox)
		}
	}
	actor_remove(user, id)
	if len(inboxes) > 0 {
		activity := map[string]any{
			"@context": activitystreams,
			"id":       actor_id + "#delete",
			"type":     "Delete",
			"actor":    actor_id,
			"to":       []string{activitystreams_public},
			"object":   actor_id,
		}
		deliver(user, id, "fediverse", inboxes, []byte(json_encode(activity)))
	}
	db.exec("delete from followers where account=?", a.ID)
	db.exec("delete from following where account=?", a.ID)
	db.exec("delete from objects where account=?", a.ID)
	media_remove(user, a.ID)
	networks_database().exec("delete from inbound where account=?", a.ID)
}

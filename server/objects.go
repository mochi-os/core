// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: What an account publishes
//
// mochi.account.object.* publish, update and delete an object as a hosted
// account, and read one from another server; mochi.account.follow and
// unfollow follow a person, mochi.account.move moves the account to one
// elsewhere; and mochi.account.media fetches a file from another server as
// the account. Each is behind the account permission, and every request goes
// through the host policy, the limiter per host and the delivery queue. An object's id is
// <actor>/<id>: the engine serves it by asking the app that published it, so
// core keeps the id, the app, the type and the addressing, not the body. A
// Bluesky account publishes into its repository instead (atproto.go); a Matrix
// or XMPP account answers that its network cannot publish yet.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

const activitystreams = "https://www.w3.org/ns/activitystreams"
const activitystreams_public = activitystreams + "#Public"

// media_maximum caps a file fetched for an app. A variable so tests can lower it.
var media_maximum = 10 << 20

var api_account_object = sls.FromStringDict(sl.String("mochi.account.object"), sl.StringDict{
	"create": sl.NewBuiltin("mochi.account.object.create", api_account_object_create),
	"delete": sl.NewBuiltin("mochi.account.object.delete", api_account_object_delete),
	"read":   sl.NewBuiltin("mochi.account.object.read", api_account_object_read),
	"update": sl.NewBuiltin("mochi.account.object.update", api_account_object_update),
})

// networks_tables makes the tables beside a user's accounts: what each hosted
// account published, who follows it, whom it follows, and the user's own
// domain blocks.
func networks_tables(db *DB) {
	db.exec("create table if not exists objects (id text not null primary key, account text not null, app text not null default '', type text not null default '', addressing text not null default '', created integer not null, updated integer not null default 0, deleted integer not null default 0)")
	db.exec("create index if not exists objects_account on objects(account)")
	db.exec("create table if not exists followers (account text not null, actor text not null, inbox text not null, shared text not null default '', created integer not null, primary key (account, actor))")
	db.exec("create table if not exists following (account text not null, actor text not null, inbox text not null default '', activity text not null, accepted integer not null default 0, created integer not null, primary key (account, actor))")
	db.exec("create table if not exists blocks (domain text not null primary key, created integer not null)")
	// What a linked account was asked to post, by the app's key (linked.go).
	db.exec("create table if not exists requests (account text not null, key text not null, result text not null, created integer not null, primary key (account, key))")
	// People on other networks the user has blocked or muted, by their id
	// (moderation.go).
	db.exec("create table if not exists blocked (actor text not null primary key, created integer not null)")
	db.exec("create table if not exists muted (actor text not null primary key, created integer not null)")
	// The files a hosted fediverse account's objects carry, served under the
	// account (media.go).
	db.exec("create table if not exists media (id text not null primary key, account text not null, type text not null default '', name text not null default '', description text not null default '', size integer not null default 0, created integer not null)")
	db.exec("create index if not exists media_account on media(account)")
	// A hosted fediverse account's identifier is its address, written
	// @name@domain; accounts made before 2026-10-14 held name@domain.
	db.exec("update accounts set identifier='@'||identifier where type='fediverse' and identifier not like '@%' and data like '%\"hosted\":true%'")
	// A hosted Bluesky account's repository (repository.go).
	repository_tables(db)
}

// account_acting resolves the account an app acts through: the account
// permission, the user whose account it is, the account row with its data
// decoded, and for a hosted account its actor id. A hosted account acts only
// while its network is on, and only a fediverse or Bluesky one can publish
// through the functions that take it; a linked account acts while the
// development switch is on, since it only makes requests as the user.
func account_acting(t *sl.Thread, fn *sl.Builtin, id string) (*User, map[string]any, string, error) {
	return account_acting_on(t, fn, id, "fediverse", "bluesky")
}

// account_acting_on is account_acting for a function that takes the hosted
// accounts of the networks given: a hosted Matrix account's conversations
// beside the publishing of the others.
func account_acting_on(t *sl.Thread, fn *sl.Builtin, id string, networks ...string) (*User, map[string]any, string, error) {
	if err := require_permission_account(t, fn, id); err != nil {
		return nil, nil, "", err
	}
	user, err := principal_storage(t)
	if err != nil || user == nil {
		return nil, nil, "", fmt.Errorf("no user")
	}
	if user_quiet(user) {
		return nil, nil, "", fmt.Errorf("the account is closing")
	}
	row := account_network_get(user, id)
	if row == nil {
		return nil, nil, "", fmt.Errorf("no such account")
	}
	if !account_hosted_flag(row) {
		if !networks_development {
			return nil, nil, "", fmt.Errorf("linked accounts are not available on this server")
		}
		return user, row, "", nil
	}
	network, _ := row["type"].(string)
	if !slices.Contains(networks, network) {
		return nil, nil, "", fmt.Errorf("a %s account cannot publish yet", network)
	}
	if !network_enabled(network) {
		return nil, nil, "", fmt.Errorf("%s is off", network)
	}
	if network == "matrix" || network == "xmpp" {
		return user, row, "", nil
	}
	return user, row, account_actor(user, id), nil
}

// account_fediverse refuses a hosted account that is not on the fediverse,
// for the functions that speak ActivityPub: following, moving, boosting and
// reading as the account. Their Bluesky forms come with phase 19.
func account_fediverse(row map[string]any) error {
	if network, _ := row["type"].(string); network != "fediverse" {
		return fmt.Errorf("a %s account cannot do this yet", network)
	}
	return nil
}

// object_result shapes what a publish answers: the object's id, its page and
// the object as published.
func object_result(id string, page string, data any) sl.Value {
	return sl_encode(map[string]any{"id": id, "url": page, "data": data})
}

// thread_context is the request's context, for an outbound request made on a
// Starlark thread.
func thread_context(t *sl.Thread) context.Context {
	if ctx, ok := t.Local("context").(context.Context); ok && ctx != nil {
		return ctx
	}
	return context.Background()
}

// object_addressing reads an object's to and cc, each a string or a list, and
// fills in the public defaults when it has neither: to the public, cc the
// account's followers, as Mastodon addresses a public post.
func object_addressing(object map[string]any, actor string) (to []string, cc []string) {
	to = object_strings(object["to"])
	cc = object_strings(object["cc"])
	if len(to) == 0 && len(cc) == 0 {
		to = []string{activitystreams_public}
		cc = []string{actor + "/followers"}
	}
	object["to"] = to
	object["cc"] = cc
	return to, cc
}

// object_strings reads a string or a list of strings.
func object_strings(v any) []string {
	switch value := v.(type) {
	case string:
		if value != "" {
			return []string{value}
		}
	case []any:
		var out []string
		for _, item := range value {
			if s, ok := item.(string); ok && s != "" {
				out = append(out, s)
			}
		}
		return out
	case []string:
		return value
	}
	return nil
}

// object_recipients turns an activity's addressing into inboxes: the account's
// followers for its followers collection, a person's inbox for their id, nothing
// for the public. Each appears once. The ids whose lookup failed for a reason
// that may pass come back apart, to be queued by address.
func object_recipients(ctx context.Context, user *User, account string, actor string, addresses []string) ([]string, []string) {
	seen := map[string]bool{}
	var inboxes, unresolved []string
	add := func(inbox string) {
		if inbox != "" && !seen[inbox] {
			seen[inbox] = true
			inboxes = append(inboxes, inbox)
		}
	}
	db := db_user(user, "user")
	for _, address := range addresses {
		switch {
		case address == activitystreams_public:
		case address == actor+"/followers":
			rows, _ := db.rows("select inbox, shared from followers where account=?", account)
			for _, row := range rows {
				if shared, _ := row["shared"].(string); shared != "" {
					add(shared)
				} else if inbox, _ := row["inbox"].(string); inbox != "" {
					add(inbox)
				}
			}
		case activitypub_remote_ok(address):
			if r, err := author_resolve(ctx, address); err == nil {
				add(r.Inbox)
			} else if resolve_transient(err) && !seen[address] {
				seen[address] = true
				unresolved = append(unresolved, address)
			}
		}
	}
	return inboxes, unresolved
}

// object_deliver queues an activity to everyone it addresses: by inbox, or by
// address for anyone whose inbox could not be found now, which the delivery
// manager looks up again before sending.
func object_deliver(ctx context.Context, user *User, account string, actor string, activity map[string]any, to []string, cc []string) {
	addresses := append(append([]string{}, to...), cc...)
	inboxes, unresolved := object_recipients(ctx, user, account, actor, addresses)
	body := []byte(json_encode(activity))
	deliver(user, account, "fediverse", inboxes, body)
	deliver_unresolved(user, account, "fediverse", unresolved, body)
}

// object_get finds an object of an account by its id, which must be the
// account's own.
func object_get(db *DB, actor string, account string, remote string) (string, map[string]any) {
	id := strings.TrimPrefix(remote, actor+"/")
	if id == remote || id == "" || strings.Contains(id, "/") {
		return "", nil
	}
	row, _ := db.row("select id, type, addressing, created, updated, deleted from objects where id=? and account=?", id, account)
	if row == nil {
		return "", nil
	}
	return id, row
}

// object_stored_addressing reads the addressing an object was published with.
func object_stored_addressing(row map[string]any) (to []string, cc []string) {
	var stored struct{ To, Cc []string }
	if text, _ := row["addressing"].(string); text != "" {
		json.Unmarshal([]byte(text), &stored)
	}
	return stored.To, stored.Cc
}

// mochi.account.object.create(account, object, key="", sourced=False) -> dict: Publish an object as an account and return {id, url, data}: its id on the network, its page where the network has one, and the object as published. As a hosted fediverse account, the object is an ActivityStreams object the app has already put in the network's form (its type, HTML content, attachments, tags); core gives it its id under the account, attributedTo and published, and sends Create to everyone it addresses, which is the public and the account's followers unless the object says otherwise. As a hosted Bluesky account, the object is a record, with $type naming its collection (a post when absent); core writes it to the account's repository and commits, at once or, for a sourced post (sourced=True), when the relay's budget allows, and data says whether the commit went out. As a linked account, the object is in the network's own form - a Mastodon status, a Bluesky record, a Lemmy post or comment; as a linked XMPP or Matrix account or a hosted Matrix account, a message: its conversation, its content, and the message it replies to or the thread it is in. key makes the post once however often it is asked
func api_account_object_create(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var account, key string
	var value sl.Value
	var sourced bool
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "account", &account, "object", &value, "key?", &key, "sourced?", &sourced); err != nil {
		return nil, err
	}
	user, row, actor, err := account_acting_on(t, fn, account, "fediverse", "bluesky", "matrix", "xmpp")
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	object := sl_decode_map(value)
	if x := xmpp_hosted_of(user, row); x != nil {
		result, err := xmpp_object_create(x, object, key)
		if err != nil {
			return xmpp_failed(x, fn, err)
		}
		return sl_encode(result), nil
	}
	if l := linked_from(user, row); l != nil {
		result, err := linked_create(thread_context(t), l, object, key)
		if err != nil {
			return linked_failed(l, fn, err)
		}
		return sl_encode(result), nil
	}
	if h := matrix_hosted_of(user, row); h != nil {
		result, err := rooms_object_create(thread_context(t), h, object, key)
		if err != nil {
			return hosted_failed(h, fn, err)
		}
		return sl_encode(result), nil
	}
	if row["type"] == "bluesky" {
		result, err := atproto_create(thread_context(t), user, row, object, key, sourced)
		if err != nil {
			return sl_error(fn, "%v", err)
		}
		return sl_encode(result), nil
	}
	kind, _ := object["type"].(string)
	if kind == "" {
		return sl_error(fn, "object needs a type")
	}

	id := uid()
	remote := actor + "/" + id
	delete(object, "@context")
	object["id"] = remote
	object["attributedTo"] = actor
	if _, set := object["published"]; !set {
		object["published"] = time.Now().UTC().Format(time.RFC3339)
	}
	to, cc := object_addressing(object, actor)

	app_id := ""
	if app, _ := t.Local("app").(*App); app != nil {
		app_id = app.id
	}
	db := db_user(user, "user")
	db.exec("insert into objects (id, account, app, type, addressing, created) values (?, ?, ?, ?, ?, ?)", id, account, app_id, kind, json_encode(map[string]any{"to": to, "cc": cc}), now())

	activity := map[string]any{
		"@context":  activitystreams,
		"id":        remote + "/activity",
		"type":      "Create",
		"actor":     actor,
		"published": object["published"],
		"to":        to,
		"cc":        cc,
		"object":    object,
	}
	object_deliver(activitypub_signing(thread_context(t), user, account), user, account, actor, activity, to, cc)
	return object_result(remote, remote, object), nil
}

// mochi.account.object.update(account, id, object) -> dict: Publish a new version of an object the account published, and return {id, url, data} as create does. The object is the whole new form, as for create; as a hosted account core keeps the id and published, sets updated, and sends Update to everyone the object addresses
func api_account_object_update(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var account, remote string
	var value sl.Value
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "account", &account, "id", &remote, "object", &value); err != nil {
		return nil, err
	}
	user, found, actor, err := account_acting_on(t, fn, account, "fediverse", "bluesky", "matrix", "xmpp")
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	object := sl_decode_map(value)
	if x := xmpp_hosted_of(user, found); x != nil {
		conversation, _ := object["conversation"].(string)
		text, _ := object["content"].(string)
		if text == "" {
			return xmpp_failed(x, fn, fmt.Errorf("a correction needs the new text"))
		}
		if _, _, err := xmpp_hosted_send(x, conversation, uid(), text, "", "", remote); err != nil {
			return xmpp_failed(x, fn, err)
		}
		return sl_encode(map[string]any{"id": remote, "url": "", "data": map[string]any{}}), nil
	}
	if l := linked_from(user, found); l != nil {
		result, err := linked_update(thread_context(t), l, remote, object)
		if err != nil {
			return linked_failed(l, fn, err)
		}
		return sl_encode(result), nil
	}
	if h := matrix_hosted_of(user, found); h != nil {
		result, err := rooms_object_update(thread_context(t), h, remote, object)
		if err != nil {
			return hosted_failed(h, fn, err)
		}
		return sl_encode(result), nil
	}
	if found["type"] == "bluesky" {
		result, err := atproto_update(thread_context(t), user, found, remote, object)
		if err != nil {
			return sl_error(fn, "%v", err)
		}
		return sl_encode(result), nil
	}
	db := db_user(user, "user")
	id, row := object_get(db, actor, account, remote)
	if row == nil || row["deleted"].(int64) != 0 {
		return sl_error(fn, "no such object")
	}
	kind, _ := object["type"].(string)
	if kind == "" {
		return sl_error(fn, "object needs a type")
	}

	delete(object, "@context")
	object["id"] = remote
	object["attributedTo"] = actor
	object["published"] = time.Unix(row["created"].(int64), 0).UTC().Format(time.RFC3339)
	if _, set := object["updated"]; !set {
		object["updated"] = time.Now().UTC().Format(time.RFC3339)
	}
	to, cc := object_strings(object["to"]), object_strings(object["cc"])
	if len(to) == 0 && len(cc) == 0 {
		to, cc = object_stored_addressing(row)
	}
	object["to"], object["cc"] = to, cc
	db.exec("update objects set type=?, addressing=?, updated=? where id=?", kind, json_encode(map[string]any{"to": to, "cc": cc}), now(), id)

	activity := map[string]any{
		"@context": activitystreams,
		"id":       remote + "#updates/" + strconv.FormatInt(now(), 10),
		"type":     "Update",
		"actor":    actor,
		"to":       to,
		"cc":       cc,
		"object":   object,
	}
	object_deliver(activitypub_signing(thread_context(t), user, account), user, account, actor, activity, to, cc)
	return object_result(remote, remote, object), nil
}

// mochi.account.object.delete(account, id, conversation="") -> bool | None: Withdraw an object the account published, answering True, or None where a linked account's server would not. As a hosted account a Delete goes to everyone it addressed, and its id is kept so the engine answers for it with a tombstone; as a linked account the server is asked to delete it, and on XMPP or Matrix, linked or hosted, the message is withdrawn from the conversation named (a redaction, a retraction)
func api_account_object_delete(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var account, remote, conversation string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "account", &account, "id", &remote, "conversation?", &conversation); err != nil {
		return nil, err
	}
	user, found, actor, err := account_acting_on(t, fn, account, "fediverse", "bluesky", "matrix", "xmpp")
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	if x := xmpp_hosted_of(user, found); x != nil {
		if err := xmpp_hosted_retract(x, conversation, remote); err != nil {
			return xmpp_failed(x, fn, err)
		}
		return sl.True, nil
	}
	if l := linked_from(user, found); l != nil {
		if err := linked_delete(thread_context(t), l, remote, conversation); err != nil {
			return linked_failed(l, fn, err)
		}
		return sl.True, nil
	}
	if h := matrix_hosted_of(user, found); h != nil {
		if err := rooms_object_delete(thread_context(t), h, remote, conversation); err != nil {
			return hosted_failed(h, fn, err)
		}
		return sl.True, nil
	}
	if found["type"] == "bluesky" {
		if err := atproto_delete(thread_context(t), user, found, remote); err != nil {
			return sl_error(fn, "%v", err)
		}
		return sl.True, nil
	}
	db := db_user(user, "user")
	id, row := object_get(db, actor, account, remote)
	if row == nil {
		return sl_error(fn, "no such object")
	}
	if row["deleted"].(int64) != 0 {
		return sl.True, nil
	}
	db.exec("update objects set deleted=? where id=?", now(), id)

	to, cc := object_stored_addressing(row)
	if row["type"] == "Announce" {
		// A boost is taken back with an Undo of the Announce, not a Delete.
		undo := map[string]any{
			"@context": activitystreams,
			"id":       remote + "#undo",
			"type":     "Undo",
			"actor":    actor,
			"to":       to,
			"cc":       cc,
			"object":   map[string]any{"id": remote, "type": "Announce", "actor": actor, "object": object_announced(row)},
		}
		object_deliver(thread_context(t), user, account, actor, undo, to, cc)
		return sl.True, nil
	}
	activity := map[string]any{
		"@context": activitystreams,
		"id":       remote + "#delete",
		"type":     "Delete",
		"actor":    actor,
		"to":       to,
		"cc":       cc,
		"object":   map[string]any{"id": remote, "type": "Tombstone"},
	}
	object_deliver(thread_context(t), user, account, actor, activity, to, cc)
	return sl.True, nil
}

// mochi.account.follow(account, address) -> dict: Follow a person on the
// fediverse as a hosted account, by their handle or their id. The follow is
// pending until their server accepts it, which a locked account's owner does
// by hand; returns {id, locked, accepted} for the person followed.
func api_account_follow(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var account, address string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "account", &account, "address", &address); err != nil {
		return nil, err
	}
	user, row, actor, err := account_acting(t, fn, account)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	if actor == "" {
		return sl_error(fn, "a linked account follows through mochi.account.request")
	}
	// A Bluesky account follows with a record of its repository; the follow
	// needs no acceptance.
	if row["type"] == "bluesky" {
		r, err := author_resolve(thread_context(t), address)
		if err != nil || r.Network != "bluesky" || r.ID == "" {
			return sl_error(fn, "cannot resolve %s", address)
		}
		// The host policy applies to the person's data server, since a did:plc
		// names no host of its own.
		if err := host_allowed(user, "bluesky", resolved_host(r)); err != nil {
			return sl_error(fn, "%v", err)
		}
		if _, err := atproto_follow(thread_context(t), user, row, r.ID); err != nil {
			return sl_error(fn, "%v", err)
		}
		return sl_encode(map[string]any{"id": r.ID, "locked": false, "accepted": true}), nil
	}
	if err := account_fediverse(row); err != nil {
		return sl_error(fn, "%v", err)
	}
	ctx := activitypub_signing(thread_context(t), user, account)
	r, err := author_resolve(ctx, address)
	if err != nil || r.Network != "fediverse" || r.ID == "" || r.Inbox == "" {
		return sl_error(fn, "cannot resolve %s", address)
	}
	if err := host_allowed(user, "fediverse", identifier_host(r.ID)); err != nil {
		return sl_error(fn, "%v", err)
	}
	account_follow(user, account, actor, r)
	following, _ := db_user(user, "user").row("select accepted from following where account=? and actor=?", account, r.ID)
	return sl_encode(map[string]any{"id": r.ID, "locked": r.Locked, "accepted": following != nil && row_int(following, "accepted") > 0}), nil
}

// account_follow records a person as followed by a hosted account, pending
// their server's Accept, and sends the Follow: the same activity again where
// one was sent before, so a repeated follow never makes a second.
func account_follow(user *User, account string, actor string, r *resolved) string {
	db := db_user(user, "user")
	activity := actor + "/follows/" + uid()
	if row, _ := db.row("select activity from following where account=? and actor=?", account, r.ID); row != nil {
		activity = row["activity"].(string)
	}
	db.exec("insert into following (account, actor, inbox, activity, accepted, created) values (?, ?, ?, ?, 0, ?) on conflict(account, actor) do update set inbox=excluded.inbox", account, r.ID, r.Inbox, activity, now())
	body := map[string]any{"@context": activitystreams, "id": activity, "type": "Follow", "actor": actor, "object": r.ID}
	deliver(user, account, "fediverse", []string{r.Inbox}, []byte(json_encode(body)))
	return activity
}

// following_move sends a hosted account's follow of a person on to where
// they have moved, once the new identity names the old one among its own
// (alsoKnownAs): the old row goes, the new one is pending as any follow is.
// False when the account did not follow the person or the move does not
// check out.
func following_move(ctx context.Context, user *User, account string, from string, to string) bool {
	db := db_user(user, "user")
	if found, _ := db.exists("select 1 from following where account=? and actor=?", account, from); !found {
		return false
	}
	r, err := author_resolve(ctx, to)
	if err != nil || r.Network != "fediverse" || r.ID == "" || r.Inbox == "" {
		debug("ActivityPub move of %s to %s not resolved: %v", from, to, err)
		return false
	}
	named := false
	for _, alias := range r.Aliases {
		if alias == from {
			named = true
		}
	}
	if !named {
		debug("ActivityPub move of %s to %s refused: %s does not name %s", from, to, r.ID, from)
		return false
	}
	if err := host_allowed(user, "fediverse", identifier_host(r.ID)); err != nil {
		return false
	}
	db.exec("delete from following where account=? and actor=?", account, from)
	account_follow(user, account, account_actor(user, account), r)
	return true
}

// mochi.account.move(account, address) -> None: Move a hosted account to an
// account elsewhere, by its handle or its id: the other account must already
// name this one among its own (alsoKnownAs). The account's document names
// where it went, and its followers' servers are sent a Move so they follow
// the new account instead. Behind the account permission
func api_account_move(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var account, address string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "account", &account, "address", &address); err != nil {
		return nil, err
	}
	user, row, actor, err := account_acting(t, fn, account)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	if actor == "" {
		return sl_error(fn, "a linked account is moved from its own server")
	}
	if err := account_fediverse(row); err != nil {
		return sl_error(fn, "%v", err)
	}
	ctx := activitypub_signing(thread_context(t), user, account)
	r, err := author_resolve(ctx, address)
	if err != nil || r.Network != "fediverse" || r.ID == "" {
		return sl_error(fn, "cannot resolve %s", address)
	}
	if r.ID == actor {
		return sl_error(fn, "an account cannot move to itself")
	}
	named := false
	for _, alias := range r.Aliases {
		if alias == actor {
			named = true
		}
	}
	if !named {
		return sl_error(fn, "%s does not name this account among its own", r.Address)
	}
	data := account_data(row)
	data["moved"] = r.ID
	encoded, _ := json.Marshal(data)
	db := db_user(user, "user")
	db.account_set(account, map[string]any{"data": string(encoded)})

	inboxes := []string{}
	seen := map[string]bool{}
	followers, _ := db.rows("select inbox, shared from followers where account=?", account)
	for _, follower := range followers {
		inbox := row_string(follower, "shared")
		if inbox == "" {
			inbox = row_string(follower, "inbox")
		}
		if inbox != "" && !seen[inbox] {
			seen[inbox] = true
			inboxes = append(inboxes, inbox)
		}
	}
	if len(inboxes) > 0 {
		activity := map[string]any{
			"@context": activitystreams,
			"id":       actor + "#moves/" + uid(),
			"type":     "Move",
			"actor":    actor,
			"object":   actor,
			"target":   r.ID,
			"to":       []string{actor + "/followers"},
		}
		deliver(user, account, "fediverse", inboxes, []byte(json_encode(activity)))
	}
	return sl.None, nil
}

// mochi.account.announce(account, id) -> dict: Boost an object on another
// server as a hosted account: an Announce of it to the public and the
// account's followers, with its author told, which credits the author and
// copies nothing. Returns {id, url, data} as object.create does, the id being
// the Announce's, which object.delete undoes. Behind the account permission
func api_account_announce(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var account, remote string
	var announced sl.Value
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "account", &account, "id", &remote, "activity?", &announced); err != nil {
		return nil, err
	}
	user, row, actor, err := account_acting(t, fn, account)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	if actor == "" {
		return sl_error(fn, "a linked account boosts through mochi.account.request")
	}
	if err := account_fediverse(row); err != nil {
		return sl_error(fn, "%v", err)
	}
	remote = strings.TrimSpace(remote)
	if !activitypub_remote_ok(remote) {
		return sl_error(fn, "invalid id")
	}
	if err := host_allowed(user, "fediverse", identifier_host(remote)); err != nil {
		return sl_error(fn, "%v", err)
	}
	ctx := activitypub_signing(thread_context(t), user, account)
	to := []string{activitystreams_public}
	cc := []string{actor + "/followers"}
	// A group announces a member's activity whole, as communities do (phase
	// 20): the activity travels inside the Announce, its actor told. A boost
	// names the object alone, with its author told where it can be read.
	var inner map[string]any
	if announced != nil && announced != sl.None {
		inner = sl_decode_map(announced)
		if inner == nil || inner["id"] != remote || activitypub_actor_of(inner["actor"]) == "" {
			return sl_error(fn, "the activity must carry the id announced and its actor")
		}
		delete(inner, "@context")
		cc = append(cc, activitypub_actor_of(inner["actor"]))
	} else {
		var object map[string]any
		if err := resolve_json(ctx, remote, "application/activity+json, application/ld+json", &object); err == nil && object != nil {
			if author := resolve_first_url(object["attributedTo"]); author != "" {
				cc = append(cc, author)
			}
		}
	}
	id := uid()
	announce := actor + "/" + id
	db := db_user(user, "user")
	db.exec("insert into objects (id, account, app, type, addressing, created) values (?, ?, ?, 'Announce', ?, ?)", id, account, thread_app_id(t), json_encode(map[string]any{"to": to, "cc": cc, "object": remote}), now())
	activity := map[string]any{
		"@context":  activitystreams,
		"id":        announce,
		"type":      "Announce",
		"actor":     actor,
		"published": time.Now().UTC().Format(time.RFC3339),
		"to":        to,
		"cc":        cc,
		"object":    remote,
	}
	if inner != nil {
		activity["object"] = inner
	}
	object_deliver(ctx, user, account, actor, activity, to, cc)
	return object_result(announce, remote, map[string]any{"type": "Announce", "object": remote}), nil
}

// The activities an app may send as a hosted account through
// mochi.account.activity: what a member or a moderator does to another's
// object - a moderator's Delete of it among them. Creating, editing and
// deleting the account's own objects go through mochi.account.object.
var activity_kinds = map[string]bool{"Like": true, "Dislike": true, "Undo": true, "Lock": true, "Add": true, "Remove": true, "Block": true, "Flag": true, "Delete": true}

// mochi.account.activity(account, activity) -> dict: Send an activity as a hosted fediverse account and return {id, url, data}: a Like or Dislike of an object, the Undo of one, a Lock, an Add or Remove against a collection, a Block, a Flag, or a moderator's Delete of another's object, in the network's form with the object named; core gives it its id under the account, its actor and its time, and the public addressing with the followers told unless the activity carries its own. The activity is answered at its id, and listed in the outbox when it is addressed to the public. Behind the account permission
func api_account_activity(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var account string
	var value sl.Value
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "account", &account, "activity", &value); err != nil {
		return nil, err
	}
	user, row, actor, err := account_acting(t, fn, account)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	if actor == "" {
		return sl_error(fn, "a linked account acts through mochi.account.request")
	}
	if err := account_fediverse(row); err != nil {
		return sl_error(fn, "%v", err)
	}
	activity := sl_decode_map(value)
	kind, _ := activity["type"].(string)
	if !activity_kinds[kind] {
		return sl_error(fn, "activity type must be one of Like, Dislike, Undo, Lock, Add, Remove, Block or Flag")
	}
	if activity["object"] == nil {
		return sl_error(fn, "activity needs an object")
	}
	id := uid()
	remote := actor + "/" + id
	delete(activity, "@context")
	activity["id"] = remote
	activity["actor"] = actor
	if _, set := activity["published"]; !set {
		activity["published"] = time.Now().UTC().Format(time.RFC3339)
	}
	to, cc := object_addressing(activity, actor)
	db := db_user(user, "user")
	db.exec("insert into objects (id, account, app, type, addressing, created) values (?, ?, ?, ?, ?, ?)", id, account, thread_app_id(t), kind, json_encode(map[string]any{"to": to, "cc": cc, "activity": activity}), now())
	body := map[string]any{"@context": activitystreams}
	for key, item := range activity {
		body[key] = item
	}
	object_deliver(activitypub_signing(thread_context(t), user, account), user, account, actor, body, to, cc)
	return object_result(remote, "", activity), nil
}

// object_activity reads the activity an objects row keeps whole, as
// mochi.account.activity stores it, or nil for an object or a boost.
func object_activity(row map[string]any) map[string]any {
	var stored struct{ Activity map[string]any }
	if text, _ := row["addressing"].(string); text != "" {
		json.Unmarshal([]byte(text), &stored)
	}
	return stored.Activity
}

// thread_app_id is the id of the app a Starlark thread runs, or empty.
func thread_app_id(t *sl.Thread) string {
	if app, _ := t.Local("app").(*App); app != nil {
		return app.id
	}
	return ""
}

// object_announced reads the object an Announce row boosted, or empty for an
// object row.
func object_announced(row map[string]any) string {
	var stored struct{ Object string }
	if text, _ := row["addressing"].(string); text != "" {
		json.Unmarshal([]byte(text), &stored)
	}
	return stored.Object
}

// mochi.account.object.read(account, id) -> dict | None: Read an object on
// another server by its id, as a hosted account: a post someone boosted, the
// parent of a reply, signed so a server that gives its documents to signed
// requests only answers. The object as its server gives it, or None when the
// host is blocked or off, or did not answer with an object. Behind the
// account permission
func api_account_object_read(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var account, remote string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "account", &account, "id", &remote); err != nil {
		return nil, err
	}
	user, row, actor, err := account_acting(t, fn, account)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	if actor == "" {
		return sl_error(fn, "a linked account reads through mochi.account.request")
	}
	if err := account_fediverse(row); err != nil {
		return sl_error(fn, "%v", err)
	}
	remote = strings.TrimSpace(remote)
	if !activitypub_remote_ok(remote) {
		return sl_error(fn, "invalid id")
	}
	if err := host_allowed(user, "fediverse", identifier_host(remote)); err != nil {
		return sl.None, nil
	}
	var object map[string]any
	if err := resolve_json(activitypub_signing(thread_context(t), user, account), remote, "application/activity+json, application/ld+json", &object); err != nil || object == nil {
		debug("Object %s not read: %v", remote, err)
		return sl.None, nil
	}
	// An object is served by the host its id names, as an actor is.
	if id, _ := object["id"].(string); id != "" && identifier_host(id) != identifier_host(remote) {
		return sl.None, nil
	}
	return sl_encode(object), nil
}

// mochi.account.unfollow(account, address) -> None: Stop following a person,
// by their handle or their id, undoing the follow at their server.
func api_account_unfollow(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var account, address string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "account", &account, "address", &address); err != nil {
		return nil, err
	}
	user, acting, actor, err := account_acting(t, fn, account)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	if actor == "" {
		return sl_error(fn, "a linked account unfollows through mochi.account.request")
	}
	if acting["type"] == "bluesky" {
		did := strings.TrimSpace(address)
		if !strings.HasPrefix(did, "did:") {
			r, err := author_resolve(thread_context(t), address)
			if err != nil || r.Network != "bluesky" || r.ID == "" {
				return sl_error(fn, "cannot resolve %s", address)
			}
			did = r.ID
		}
		if err := atproto_unfollow(thread_context(t), user, acting, did); err != nil {
			return sl_error(fn, "%v", err)
		}
		return sl.None, nil
	}
	if err := account_fediverse(acting); err != nil {
		return sl_error(fn, "%v", err)
	}
	db := db_user(user, "user")
	row, _ := db.row("select actor, inbox, activity from following where account=? and actor=?", account, address)
	if row == nil {
		if r, err := author_resolve(thread_context(t), address); err == nil && r.ID != "" {
			row, _ = db.row("select actor, inbox, activity from following where account=? and actor=?", account, r.ID)
		}
	}
	if row == nil {
		return sl_error(fn, "not following %s", address)
	}
	followed, _ := row["actor"].(string)
	inbox, _ := row["inbox"].(string)
	activity, _ := row["activity"].(string)
	db.exec("delete from following where account=? and actor=?", account, followed)
	body := map[string]any{
		"@context": activitystreams,
		"id":       activity + "/undo",
		"type":     "Undo",
		"actor":    actor,
		"object":   map[string]any{"id": activity, "type": "Follow", "actor": actor, "object": followed},
	}
	deliver(user, account, "fediverse", []string{inbox}, []byte(json_encode(body)))
	return sl.None, nil
}

// mochi.account.media(account, url) -> dict: Fetch a file from another server
// as a hosted account, signed so a server that requires signed fetches answers;
// a hosted Matrix account fetches the address a message's file names, kept in
// core's evictable cache.
// Returns the status, the content type and the data, up to ten megabytes, or
// None when the request could not be made: the host is blocked or off, its
// budget for the minute is spent, or it did not answer. Nothing is stored.
func api_account_media(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var account, address string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "account", &account, "url", &address); err != nil {
		return nil, err
	}
	user, row, actor, err := account_acting_on(t, fn, account, "fediverse", "bluesky", "matrix", "xmpp")
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	if u, err := url.Parse(address); err != nil || u.Hostname() == "" {
		return sl_error(fn, "invalid url")
	}
	if h := matrix_hosted_of(user, row); h != nil {
		status, kind, data, err := content_fetch(thread_context(t), h, address)
		if err == nil && encryption_file(user, account, address) != nil {
			// A file sent encrypted is decrypted with the key its message
			// carried (encryption.go); its type is the one the message
			// gives.
			data, err = encryption_opened(user, account, address, data)
			kind = ""
		}
		if err != nil {
			debug("Media %s not fetched: %v", address, err)
			return sl.None, nil
		}
		result := sl.NewDict(3)
		result.SetKey(sl.String("status"), sl.MakeInt(status))
		result.SetKey(sl.String("type"), sl.String(kind))
		result.SetKey(sl.String("data"), sl.Bytes(data))
		return result, nil
	}
	if l := linked_from(user, row); l != nil {
		status, kind, data, err := linked_media(thread_context(t), l, address)
		if err == nil && (encryption_file(user, account, address) != nil || encryption_shared(user, account, address) != "") {
			data, err = encryption_opened(user, account, address, data)
			// Its type is the one its name gives; a Matrix address has none,
			// leaving the message's own.
			kind = mime.TypeByExtension(path.Ext(xmpp_file_name(address)))
		}
		if err != nil {
			debug("Media %s not fetched: %v", address, err)
			return sl.None, nil
		}
		result := sl.NewDict(3)
		result.SetKey(sl.String("status"), sl.MakeInt(status))
		result.SetKey(sl.String("type"), sl.String(kind))
		result.SetKey(sl.String("data"), sl.Bytes(data))
		return result, nil
	}
	request, err := http.NewRequestWithContext(thread_context(t), "GET", address, nil)
	if err != nil {
		return sl_error(fn, "invalid url")
	}
	request.Header.Set("Accept", "*/*")
	// A file shared on XMPP is fetched as any page is; on the fediverse the
	// request is signed as the account.
	network := "xmpp"
	if xmpp_hosted_of(user, row) == nil {
		network = "fediverse"
		key := keypair_get(user, account, key_signing)
		if key == nil {
			return sl_error(fn, "account has no key")
		}
		if err := signature_sign(request, actor+"#main-key", key.Private, nil); err != nil {
			return sl_error(fn, "%v", err)
		}
	}
	response, err := network_request(request.Context(), user, network, request, 5)
	if err != nil {
		debug("Media %s not fetched: %v", address, err)
		return sl.None, nil
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, int64(media_maximum)+1))
	if err != nil {
		debug("Media %s not read: %v", address, err)
		return sl.None, nil
	}
	if len(data) > media_maximum {
		debug("Media %s not read: over %d bytes", address, media_maximum)
		return sl.None, nil
	}
	kind := response.Header.Get("Content-Type")
	// A file shared encrypted is decrypted with the key its message carried
	// (omemo.go), its type the one its name gives.
	if network == "xmpp" && encryption_shared(user, account, address) != "" {
		if data, err = encryption_opened(user, account, address, data); err != nil {
			debug("Media %s not decrypted: %v", address, err)
			return sl.None, nil
		}
		kind = mime.TypeByExtension(path.Ext(xmpp_file_name(address)))
	}
	result := sl.NewDict(3)
	result.SetKey(sl.String("status"), sl.MakeInt(response.StatusCode))
	result.SetKey(sl.String("type"), sl.String(kind))
	result.SetKey(sl.String("data"), sl.Bytes(data))
	return result, nil
}

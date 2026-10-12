// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: What an account publishes
//
// mochi.account.object.* publish, update and delete an object as a hosted
// account; mochi.account.follow and unfollow follow a person; and
// mochi.account.media fetches a file from another server as the account. Each
// is behind the account permission, and every request goes through the host
// policy, the limiter per host and the delivery queue. An object's id is
// <actor>/<id>: the engine serves it by asking the app that published it, so
// core keeps the id, the app, the type and the addressing, not the body. Only
// fediverse accounts publish yet; a Bluesky, Matrix or XMPP account answers that
// its network cannot publish yet.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
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
}

// account_acting resolves the hosted account an app acts through: the account
// permission, the user whose account it is, the account row and its actor id.
// Only a fediverse account can act yet, and only while the network is on.
func account_acting(t *sl.Thread, fn *sl.Builtin, id string) (*User, map[string]any, string, error) {
	if err := require_permission_account(t, fn, id); err != nil {
		return nil, nil, "", err
	}
	user, err := principal_storage(t)
	if err != nil || user == nil {
		return nil, nil, "", fmt.Errorf("no user")
	}
	row := account_hosted_get(user, id)
	if row == nil {
		return nil, nil, "", fmt.Errorf("no such account")
	}
	network, _ := row["type"].(string)
	if network != "fediverse" {
		return nil, nil, "", fmt.Errorf("a %s account cannot publish yet", network)
	}
	if !network_enabled(network) {
		return nil, nil, "", fmt.Errorf("%s is off", network)
	}
	return user, row, account_actor(user, id), nil
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
// for the public. Each appears once.
func object_recipients(ctx context.Context, user *User, account string, actor string, addresses []string) []string {
	seen := map[string]bool{}
	var inboxes []string
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
		case strings.HasPrefix(address, "https://"):
			if r, err := author_resolve(ctx, address); err == nil {
				add(r.Inbox)
			}
		}
	}
	return inboxes
}

// object_deliver queues an activity to everyone it addresses.
func object_deliver(ctx context.Context, user *User, account string, actor string, activity map[string]any, to []string, cc []string) {
	addresses := append(append([]string{}, to...), cc...)
	inboxes := object_recipients(ctx, user, account, actor, addresses)
	deliver(user, account, "fediverse", inboxes, []byte(json_encode(activity)))
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

// mochi.account.object.create(account, object) -> string: Publish an object as
// a hosted account and return its id. The object is an ActivityStreams object
// the app has already put in the network's form (its type, HTML content,
// attachments, tags); core gives it its id under the account, attributedTo and
// published, and sends Create to everyone it addresses, which is the public and
// the account's followers unless the object says otherwise.
func api_account_object_create(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var account string
	var value sl.Value
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "account", &account, "object", &value); err != nil {
		return nil, err
	}
	user, _, actor, err := account_acting(t, fn, account)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	object := sl_decode_map(value)
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
	object_deliver(thread_context(t), user, account, actor, activity, to, cc)
	return sl.String(remote), nil
}

// mochi.account.object.update(account, id, object) -> string: Publish a new
// version of an object the account published, and return its id. The object is
// the whole new form, as for create; core keeps the id and published, sets
// updated, and sends Update to everyone the object addresses.
func api_account_object_update(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var account, remote string
	var value sl.Value
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "account", &account, "id", &remote, "object", &value); err != nil {
		return nil, err
	}
	user, _, actor, err := account_acting(t, fn, account)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	db := db_user(user, "user")
	id, row := object_get(db, actor, account, remote)
	if row == nil || row["deleted"].(int64) != 0 {
		return sl_error(fn, "no such object")
	}
	object := sl_decode_map(value)
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
	object_deliver(thread_context(t), user, account, actor, activity, to, cc)
	return sl.String(remote), nil
}

// mochi.account.object.delete(account, id) -> None: Withdraw an object the
// account published: a Delete goes to everyone it addressed, and its id is kept
// so the engine answers for it with a tombstone.
func api_account_object_delete(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var account, remote string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "account", &account, "id", &remote); err != nil {
		return nil, err
	}
	user, _, actor, err := account_acting(t, fn, account)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	db := db_user(user, "user")
	id, row := object_get(db, actor, account, remote)
	if row == nil {
		return sl_error(fn, "no such object")
	}
	if row["deleted"].(int64) != 0 {
		return sl.None, nil
	}
	db.exec("update objects set deleted=? where id=?", now(), id)

	to, cc := object_stored_addressing(row)
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
	return sl.None, nil
}

// mochi.account.follow(account, address) -> None: Follow a person on the
// fediverse as a hosted account, by their handle or their id. The follow is
// pending until their server accepts it.
func api_account_follow(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var account, address string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "account", &account, "address", &address); err != nil {
		return nil, err
	}
	user, _, actor, err := account_acting(t, fn, account)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	ctx := thread_context(t)
	r, err := author_resolve(ctx, address)
	if err != nil || r.Network != "fediverse" || r.ID == "" || r.Inbox == "" {
		return sl_error(fn, "cannot resolve %s", address)
	}
	if err := host_allowed(user, "fediverse", identifier_host(r.ID)); err != nil {
		return sl_error(fn, "%v", err)
	}

	db := db_user(user, "user")
	activity := actor + "/follows/" + uid()
	if row, _ := db.row("select activity from following where account=? and actor=?", account, r.ID); row != nil {
		activity = row["activity"].(string)
	}
	db.exec("insert into following (account, actor, inbox, activity, accepted, created) values (?, ?, ?, ?, 0, ?) on conflict(account, actor) do update set inbox=excluded.inbox", account, r.ID, r.Inbox, activity, now())
	body := map[string]any{"@context": activitystreams, "id": activity, "type": "Follow", "actor": actor, "object": r.ID}
	deliver(user, account, "fediverse", []string{r.Inbox}, []byte(json_encode(body)))
	return sl.None, nil
}

// mochi.account.unfollow(account, address) -> None: Stop following a person,
// by their handle or their id, undoing the follow at their server.
func api_account_unfollow(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var account, address string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "account", &account, "address", &address); err != nil {
		return nil, err
	}
	user, _, actor, err := account_acting(t, fn, account)
	if err != nil {
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
// as a hosted account, signed so a server that requires signed fetches answers.
// Returns the status, the content type and the data, up to ten megabytes, or
// None when the request could not be made: the host is blocked or off, its
// budget for the minute is spent, or it did not answer. Nothing is stored.
func api_account_media(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var account, address string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "account", &account, "url", &address); err != nil {
		return nil, err
	}
	user, _, actor, err := account_acting(t, fn, account)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	if u, err := url.Parse(address); err != nil || u.Hostname() == "" {
		return sl_error(fn, "invalid url")
	}
	key := keypair_get(user, account, key_signing)
	if key == nil {
		return sl_error(fn, "account has no key")
	}
	request, err := http.NewRequestWithContext(thread_context(t), "GET", address, nil)
	if err != nil {
		return sl_error(fn, "invalid url")
	}
	request.Header.Set("Accept", "*/*")
	if err := signature_sign(request, actor+"#main-key", key.Private, nil); err != nil {
		return sl_error(fn, "%v", err)
	}
	response, err := network_request(request.Context(), user, "fediverse", request, 5)
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
	result := sl.NewDict(3)
	result.SetKey(sl.String("status"), sl.MakeInt(response.StatusCode))
	result.SetKey(sl.String("type"), sl.String(response.Header.Get("Content-Type")))
	result.SetKey(sl.String("data"), sl.Bytes(data))
	return result, nil
}

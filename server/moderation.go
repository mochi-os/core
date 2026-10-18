// Mochi server: a user's own moderation on other networks
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// What a user does about people on other networks, kept in the user's own
// database beside their domain blocks: a blocked person is nobody to any of
// the user's accounts - their activities are taken and dropped, as a blocked
// domain's are, and they are no longer a follower - while a muted person may
// still follow and receive but nothing they send reaches the apps. A report
// goes out as a Flag from one of the user's accounts to the person's server,
// which is how a fediverse server tells another's moderators about one of
// its people.

package main

import (
	"context"
	"fmt"
	"strings"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

var api_account_mute = sls.FromStringDict(sl.String("mochi.account.mute"), sl.StringDict{
	"add":    sl.NewBuiltin("mochi.account.mute.add", api_account_mute_add),
	"list":   sl.NewBuiltin("mochi.account.mute.list", api_account_mute_list),
	"remove": sl.NewBuiltin("mochi.account.mute.remove", api_account_mute_remove),
})

// actor_target reads a person's id on another network from an API argument,
// trimmed - a fediverse id, a Bluesky identifier, or an XMPP address as
// xmpp:name@host, lowercased as the XMPP engine keeps it; false for anything
// that is not one, such as a domain.
func actor_target(target string) (string, bool) {
	target = strings.TrimSpace(target)
	if rest, found := strings.CutPrefix(target, "xmpp:"); found {
		address, err := xmpp_address(rest)
		if err != nil {
			return "", false
		}
		return "xmpp:" + strings.ToLower(address.String()), true
	}
	if !activitypub_remote_ok(target) && !resolve_did_re.MatchString(target) {
		return "", false
	}
	return target, true
}

// actor_blocked says whether the user has blocked a person.
func actor_blocked(user *User, actor string) bool {
	found, _ := db_user(user, "user").exists("select 1 from blocked where actor=?", actor)
	return found
}

// actor_muted says whether the user has muted a person.
func actor_muted(user *User, actor string) bool {
	found, _ := db_user(user, "user").exists("select 1 from muted where actor=?", actor)
	return found
}

// actor_list reads a table of people, oldest first.
func actor_list(db *DB, table string) []map[string]any {
	rows, _ := db.rows("select actor, created from " + table + " order by created")
	if rows == nil {
		rows = []map[string]any{}
	}
	return rows
}

// blocked_add blocks a person for the user: they are forgotten as a follower
// of every account of the user's, and as someone followed; a person on
// Bluesky is blocked by a record of each of the user's accounts there.
func blocked_add(user *User, actor string) {
	db := db_user(user, "user")
	db.exec("insert or ignore into blocked (actor, created) values (?, ?)", actor, now())
	db.exec("delete from followers where actor=?", actor)
	db.exec("delete from following where actor=?", actor)
	if strings.HasPrefix(actor, "did:") {
		atproto_block(context.Background(), user, actor, true)
	}
}

// blocked_remove lifts a block on a person, and the records that made it.
func blocked_remove(user *User, actor string) {
	db_user(user, "user").exec("delete from blocked where actor=?", actor)
	if strings.HasPrefix(actor, "did:") {
		atproto_block(context.Background(), user, actor, false)
	}
}

// muted_set mutes or unmutes a person for the user, and at Bluesky's index
// for each of the user's accounts there, which keeps mutes for an account.
func muted_set(ctx context.Context, user *User, actor string, on bool) {
	db := db_user(user, "user")
	if on {
		db.exec("insert or ignore into muted (actor, created) values (?, ?)", actor, now())
	} else {
		db.exec("delete from muted where actor=?", actor)
	}
	if !strings.HasPrefix(actor, "did:") {
		return
	}
	for _, row := range accounts_hosted(user, "bluesky") {
		if atproto_did(row) == "" {
			continue
		}
		if err := atproto_mute(ctx, user, row, actor, on); err != nil {
			debug("Bluesky account %s: mute of %s not sent: %v", row["id"], actor, err)
		}
	}
}

// mochi.account.mute.list() -> list: The people the user has muted, each with
// the person's id and when. A muted person may follow and receive, but nothing
// they send to the user's accounts reaches the apps.
func api_account_mute_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user, _, err := block_call(t, fn, args, "accounts/read", "")
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	return sl_encode(actor_list(db_user(user, "user"), "muted")), nil
}

// mochi.account.mute.add(actor) -> None: Mute a person, by their id on their
// network.
func api_account_mute_add(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user, target, err := block_call(t, fn, args, "accounts/write", "<actor: string>")
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	actor, ok := actor_target(target)
	if !ok {
		return sl_error(fn, "invalid actor")
	}
	muted_set(thread_context(t), user, actor, true)
	return sl.None, nil
}

// mochi.account.mute.remove(actor) -> None: Unmute a person.
func api_account_mute_remove(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user, target, err := block_call(t, fn, args, "accounts/write", "<actor: string>")
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	muted_set(thread_context(t), user, strings.TrimSpace(target), false)
	return sl.None, nil
}

// mochi.account.followers(account, actor="") -> list: The people following a
// hosted account, each with their id, their inbox and when they followed;
// with actor, only that person, so an app can ask whether someone follows.
// Behind the account permission
func api_account_followers(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var account, actor string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "account", &account, "actor?", &actor); err != nil {
		return nil, err
	}
	user, _, id, err := account_acting(t, fn, account)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	if id == "" {
		return sl_error(fn, "a linked account's followers are read through mochi.account.request")
	}
	db := db_user(user, "user")
	var rows []map[string]any
	if actor != "" {
		rows, _ = db.rows("select actor, inbox, created from followers where account=? and actor=?", account, actor)
	} else {
		rows, _ = db.rows("select actor, inbox, created from followers where account=? order by created", account)
	}
	if rows == nil {
		rows = []map[string]any{}
	}
	return sl_encode(rows), nil
}

// mochi.account.following(account, actor="") -> list: The people a hosted
// account follows, each with their id, when the follow was sent and when
// their server accepted it (0 while it waits, as a locked account's does);
// with actor, only that person. Behind the account permission
func api_account_following(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var account, actor string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "account", &account, "actor?", &actor); err != nil {
		return nil, err
	}
	user, _, id, err := account_acting(t, fn, account)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	if id == "" {
		return sl_error(fn, "a linked account's follows are read through mochi.account.request")
	}
	db := db_user(user, "user")
	var rows []map[string]any
	if actor != "" {
		rows, _ = db.rows("select actor, accepted, created from following where account=? and actor=?", account, actor)
	} else {
		rows, _ = db.rows("select actor, accepted, created from following where account=? order by created", account)
	}
	if rows == nil {
		rows = []map[string]any{}
	}
	return sl_encode(rows), nil
}

// mochi.account.report(account, actor, objects=[], content="") -> None: Report
// a person on another network to their server's moderators, as a hosted
// account: a Flag naming the person and the objects of theirs complained of,
// with the reason, sent to their inbox. Behind the account permission
func api_account_report(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var account, address, content string
	var objects *sl.List
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "account", &account, "actor", &address, "objects?", &objects, "content?", &content); err != nil {
		return nil, err
	}
	user, _, actor, err := account_acting(t, fn, account)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	if actor == "" {
		return sl_error(fn, "a linked account reports through mochi.account.request")
	}
	if len(content) > 2048 {
		return sl_error(fn, "content must be at most 2048 characters")
	}
	ctx := activitypub_signing(thread_context(t), user, account)
	r, err := author_resolve(ctx, address)
	if err != nil || r.Network != "fediverse" || r.ID == "" || r.Inbox == "" {
		return sl_error(fn, "cannot resolve %s", address)
	}
	if err := host_allowed(user, "fediverse", identifier_host(r.ID)); err != nil {
		return sl_error(fn, "%v", err)
	}
	named := []string{r.ID}
	if objects != nil {
		for i := 0; i < objects.Len(); i++ {
			object, ok := sl.AsString(objects.Index(i))
			if !ok || !activitypub_remote_ok(object) {
				return sl_error(fn, "objects must be ids on the person's network")
			}
			if activitypub_host_of(object) != activitypub_host_of(r.ID) {
				return sl_error(fn, "an object must be on the person's own server")
			}
			named = append(named, object)
		}
	}
	flag := map[string]any{
		"@context": activitystreams,
		"id":       actor + "#flags/" + uid(),
		"type":     "Flag",
		"actor":    actor,
		"to":       []string{r.ID},
		"object":   named,
		"content":  content,
	}
	inbox := r.Inbox
	if r.Shared != "" {
		inbox = r.Shared
	}
	if deliver(user, account, "fediverse", []string{inbox}, []byte(json_encode(flag))) == 0 {
		return sl_error(fn, fmt.Sprintf("cannot deliver to %s", inbox))
	}
	return sl.None, nil
}

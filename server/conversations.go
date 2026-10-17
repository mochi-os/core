// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: Conversations on linked accounts
//
// A linked XMPP or Matrix account's conversations and rooms, and a linked
// Mastodon account's direct messages, are chat's (the federation plan, phase
// 23). Each account's session reads what arrives in its protocol's own form
// and hands it to the apps declaring linked/receive as events of one shape,
// whatever the network:
//
//	message   conversation, room, from, name, id, archive, body or html,
//	          created, own, encrypted, reply, thread, files ({url, name,
//	          type, size}); a Mastodon message names its people instead of
//	          a conversation
//	edit      a new version of a message: conversation, from, id, body or html
//	delete    a message withdrawn: conversation, from, id
//	sent      the id a room gave a message the account sent: conversation,
//	          id, archive
//	typing    someone typing, or stopped: conversation, from, typing
//	receipt   how far someone has received or read: conversation, from, id,
//	          displayed
//	presence  someone's presence: from, status, message
//	room      what a sync says of a conversation: conversation, room, name,
//	          direct, encrypted, count, heroes, left
//	member    someone joining or leaving a room: conversation, id, name, left
//	invite    an invitation to a room: conversation, from, name, direct
//	session   the account's session began; the app answers by saying whether
//	          the user's presence is shared
//
// A conversation is a Matrix room's id, an XMPP person's or room's address
// (xmpp:name@host), and a person is known as the network knows them
// (@name:host, xmpp:name@host, acct:name@host). The functions below act the
// other way: typing, read markers, the user's presence and rooms; messages go
// through mochi.account.object like any other publishing. A hosted Matrix
// account's rooms are the engine's (rooms.go), which tells the apps of them
// in the same events and takes the same functions.

package main

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/matrix-org/gomatrixserverlib/spec"
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

// linked_inbound hands what an account's session heard to the apps declaring
// linked/receive, in one call per batch. A variable so tests can watch it.
var linked_inbound = func(user *User, account string, network string, events []map[string]any) {
	if user == nil || len(events) == 0 || user_pending(user) {
		return
	}
	list := make([]any, 0, len(events))
	for _, event := range events {
		list = append(list, event)
	}
	for _, a := range apps_for_function(user, "linked/receive") {
		if _, err := activitypub_call(user, a, "linked/receive", Map{"account": account, "network": network, "events": list}); err != nil {
			info("Linked %s account %s: %s did not take %d events: %v", network, account, a.id, len(events), err)
		}
	}
}

// === Presence ===

// linked_presence is what a linked chat account shows of the user: their
// status and its text while the app says the user shares it, else offline.
// The status is one of Mochi's: online, away, busy, offline.
func linked_presence(l *linked) (string, string) {
	fresh := linked_get(l.user, l.id)
	if fresh == nil {
		return "offline", ""
	}
	if hidden, _ := fresh.value("hidden").(bool); hidden {
		return "offline", ""
	}
	status, _, connections := presence_user(l.user.UID)
	message := ""
	for _, c := range connections {
		if c.Status == status && c.Message != "" {
			message = c.Message
		}
	}
	return status, message
}

// linked_presence_notify tells a user's sessions that their presence, or what
// they share of it, may have changed.
func linked_presence_notify(uid string) {
	linked_lock.Lock()
	defer linked_lock.Unlock()
	prefix := uid + "/"
	for key, s := range linked_sessions {
		if strings.HasPrefix(key, prefix) && s.presence != nil {
			select {
			case s.presence <- struct{}{}:
			default:
			}
		}
	}
}

// === Functions ===

var api_account_room = sls.FromStringDict(sl.String("mochi.account.room"), sl.StringDict{
	"create": sl.NewBuiltin("mochi.account.room.create", api_account_room_create),
	"join":   sl.NewBuiltin("mochi.account.room.join", api_account_room_join),
	"leave":  sl.NewBuiltin("mochi.account.room.leave", api_account_room_leave),
})

// A matrix_hosted is a hosted Matrix account an app holds conversations
// through: its user, its id, and its Matrix id.
type matrix_hosted struct {
	user    *User
	id      string
	address string
}

// matrix_hosted_of is an account row's hosted Matrix account, or nil.
func matrix_hosted_of(user *User, row map[string]any) *matrix_hosted {
	if row == nil || !account_hosted_flag(row) || row["type"] != "matrix" {
		return nil
	}
	return &matrix_hosted{user: user, id: row_string(row, "id"), address: row_string(row, "identifier")}
}

// hosted_failed is a hosted Matrix account's failure as the app sees it: None,
// with the reason in the log, so an app's action carries on as it does for a
// linked account (linked_failed).
func hosted_failed(h *matrix_hosted, fn *sl.Builtin, err error) (sl.Value, error) {
	info("Hosted Matrix account %s: %s: %v", h.id, fn.Name(), err)
	return sl.None, nil
}

// conversing resolves the account an app holds conversations through: a
// linked account of a kind given, or a hosted Matrix account.
func conversing(t *sl.Thread, fn *sl.Builtin, account string, kinds ...string) (*linked, *matrix_hosted, error) {
	user, row, _, err := account_acting_on(t, fn, account, "fediverse", "bluesky", "matrix")
	if err != nil {
		return nil, nil, err
	}
	if h := matrix_hosted_of(user, row); h != nil && slices.Contains(kinds, "matrix") {
		return nil, h, nil
	}
	l := linked_from(user, row)
	if l == nil || !slices.Contains(kinds, l.kind) {
		return nil, nil, fmt.Errorf("this account has no conversations here")
	}
	return l, nil, nil
}

// linked_client is the running XMPP session of an account.
func linked_client(l *linked) (*xmpp_client, error) {
	linked_lock.Lock()
	s := linked_sessions[linked_session_key(l.user.UID, l.id)]
	linked_lock.Unlock()
	if s != nil {
		if c := s.client.Load(); c != nil {
			return c, nil
		}
	}
	return nil, fmt.Errorf("the account is not connected")
}

// xmpp_create sends a message through an account's session: its text or a
// file by the address its upload answered, in a conversation, perhaps as a
// reply. The app's key is the message's id, so the same message is known
// again.
func xmpp_create(ctx context.Context, l *linked, object map[string]any, key string) (map[string]any, error) {
	c, err := linked_client(l)
	if err != nil {
		return nil, err
	}
	conversation, _ := object["conversation"].(string)
	text, _ := object["content"].(string)
	reply, _ := object["reply"].(string)
	file := ""
	if f, _ := object["file"].(map[string]any); f != nil {
		file, _ = f["url"].(string)
	}
	if text == "" && file == "" {
		return nil, fmt.Errorf("a message needs content or a file")
	}
	id := key
	if id == "" {
		id = uid()
	}
	if err := c.message_send(ctx, conversation, id, text, file, reply, ""); err != nil {
		return nil, err
	}
	return map[string]any{"id": id, "url": "", "data": map[string]any{}}, nil
}

// xmpp_update sends a correction of a message the account sent.
func xmpp_update(ctx context.Context, l *linked, id string, object map[string]any) (map[string]any, error) {
	c, err := linked_client(l)
	if err != nil {
		return nil, err
	}
	conversation, _ := object["conversation"].(string)
	text, _ := object["content"].(string)
	if text == "" {
		return nil, fmt.Errorf("a correction needs the new text")
	}
	if err := c.message_send(ctx, conversation, uid(), text, "", "", id); err != nil {
		return nil, err
	}
	return map[string]any{"id": id, "url": "", "data": map[string]any{}}, nil
}

// mochi.account.typing(account, conversation, typing=True) -> None: Tell a conversation of a linked XMPP or Matrix account that the user is typing, or stopped. The app calls it while the user types, as their switch allows; the other side is told they stopped once the typing ends. A hosted Matrix account's rooms are not told yet. Requires the account's permission
func api_account_typing(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var account, conversation string
	typing := true
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "account", &account, "conversation", &conversation, "typing?", &typing); err != nil {
		return nil, err
	}
	l, h, err := conversing(t, fn, account, "xmpp", "matrix")
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	if h != nil {
		return sl.None, nil
	}
	switch l.kind {
	case "xmpp":
		c, e := linked_client(l)
		if e == nil {
			e = c.type_state(thread_context(t), conversation, typing)
		}
		err = e
	case "matrix":
		err = matrix_typing(thread_context(t), l, conversation, typing)
	}
	if err != nil {
		return linked_failed(l, fn, err)
	}
	return sl.None, nil
}

// mochi.account.read(account, conversation, id, shared=True) -> None: Say the user has read a conversation of a linked XMPP or Matrix account up to a message, by its id on the network. Shared, the other members are told (a read marker); not shared, a Matrix server alone is told, to keep the unread count of the user's other clients, and XMPP is told nothing. A hosted Matrix account's rooms are not told yet. Requires the account's permission
func api_account_read(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var account, conversation, id string
	shared := true
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "account", &account, "conversation", &conversation, "id", &id, "shared?", &shared); err != nil {
		return nil, err
	}
	l, h, err := conversing(t, fn, account, "xmpp", "matrix")
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	if h != nil {
		return sl.None, nil
	}
	switch l.kind {
	case "xmpp":
		if !shared {
			return sl.None, nil
		}
		c, e := linked_client(l)
		if e == nil {
			e = c.display(thread_context(t), conversation, id)
		}
		err = e
	case "matrix":
		err = matrix_read(thread_context(t), l, conversation, id, shared)
	}
	if err != nil {
		return linked_failed(l, fn, err)
	}
	return sl.None, nil
}

// mochi.account.presence(account, shared) -> None: Say whether a linked XMPP or Matrix account shows the user's presence. Shared, it shows their status in Mochi and its text; not shared, Matrix shows them offline and XMPP extended away, since an XMPP session that said it was unavailable would be sent nothing. A hosted Matrix account shows no presence yet. Requires the account's permission
func api_account_presence(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var account string
	var shared bool
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "account", &account, "shared", &shared); err != nil {
		return nil, err
	}
	l, h, err := conversing(t, fn, account, "xmpp", "matrix")
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	if h != nil {
		return sl.None, nil
	}
	if shared {
		l.store("hidden", nil)
	} else {
		l.store("hidden", true)
	}
	linked_presence_notify(l.user.UID)
	return sl.None, nil
}

// mochi.account.room.create(account, people, name="") -> dict: Start a conversation on a linked XMPP or Matrix account, or a hosted Matrix account, with people by their addresses (@name:host on Matrix, name@host on XMPP), and return {conversation, room, people}: the conversation's id, whether it is a room, and each person as {id, name}. With one person it is a one-to-one conversation (on Matrix a private room marked so); with several, a members-only room named name that each is invited to. An address no one is at fails. Requires the account's permission
func api_account_room_create(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var account, name string
	var list *sl.List
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "account", &account, "people", &list, "name?", &name); err != nil {
		return nil, err
	}
	l, h, err := conversing(t, fn, account, "xmpp", "matrix")
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	var addresses []string
	for i := 0; i < list.Len(); i++ {
		address, ok := sl.AsString(list.Index(i))
		if !ok || strings.TrimSpace(address) == "" {
			return conversing_failed(l, h, fn, fmt.Errorf("invalid address"))
		}
		addresses = append(addresses, strings.TrimSpace(address))
	}
	if len(addresses) == 0 {
		return conversing_failed(l, h, fn, fmt.Errorf("no people"))
	}
	ctx := thread_context(t)
	var people []any
	var conversation string
	if h != nil {
		mine, _ := spec.NewUserID(h.address, true)
		var ids []string
		for _, address := range addresses {
			address = matrix_person(address)
			if !resolve_matrix_re.MatchString(address) {
				return hosted_failed(h, fn, fmt.Errorf("invalid address"))
			}
			found, err := rooms_profile(ctx, mine.Domain(), address)
			if err != nil {
				return hosted_failed(h, fn, err)
			}
			if address != h.address && !slices.Contains(ids, address) {
				ids = append(ids, address)
				people = append(people, map[string]any{"id": address, "name": found})
			}
		}
		if len(ids) == 0 {
			return hosted_failed(h, fn, fmt.Errorf("no people"))
		}
		room, err := rooms_create(ctx, h.address, ids, name)
		if err != nil {
			return hosted_failed(h, fn, err)
		}
		return sl_encode(map[string]any{"conversation": room, "room": len(people) > 1, "people": people}), nil
	}
	switch l.kind {
	case "xmpp":
		var bares []string
		for _, address := range addresses {
			person, err := xmpp_address(address)
			if err != nil {
				return linked_failed(l, fn, err)
			}
			if !slices.Contains(bares, person.String()) {
				bares = append(bares, person.String())
				people = append(people, map[string]any{"id": "xmpp:" + person.String(), "name": person.Localpart()})
			}
		}
		if len(bares) == 1 {
			conversation = "xmpp:" + bares[0]
			break
		}
		c, err := linked_client(l)
		if err != nil {
			return linked_failed(l, fn, err)
		}
		room, err := c.room_create(ctx, bares, name)
		if err != nil {
			return linked_failed(l, fn, err)
		}
		conversation = "xmpp:" + room
	case "matrix":
		var ids []string
		for _, address := range addresses {
			address = matrix_person(address)
			found, err := matrix_name(ctx, l, address)
			if err != nil {
				return linked_failed(l, fn, err)
			}
			if !slices.Contains(ids, address) {
				ids = append(ids, address)
				people = append(people, map[string]any{"id": address, "name": found})
			}
		}
		room, err := matrix_room_create(ctx, l, ids, name)
		if err != nil {
			return linked_failed(l, fn, err)
		}
		conversation = room
	}
	return sl_encode(map[string]any{"conversation": conversation, "room": len(people) > 1, "people": people}), nil
}

// mochi.account.room.join(account, address, person="") -> dict: Join a room on a linked XMPP or Matrix account, or a hosted Matrix account, by its address (#alias:host or !id:host on Matrix, name@rooms.host on XMPP), and return {conversation, name}. On Matrix it also accepts an invitation, and with person the room is marked in the account's data as the one-to-one conversation with them, as an invitation to one asks. A hosted account's apps are told of the room, who is in it and its recent past before the join returns. An address that is not a room fails. Requires the account's permission
func api_account_room_join(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var account, address, person string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "account", &account, "address", &address, "person?", &person); err != nil {
		return nil, err
	}
	l, h, err := conversing(t, fn, account, "xmpp", "matrix")
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	address = strings.TrimSpace(address)
	ctx := thread_context(t)
	if h != nil {
		// Marked one-to-one before the join, which tells the apps of the room.
		target := matrix_room(address)
		direct := resolve_matrix_re.MatchString(person) && strings.HasPrefix(target, "!")
		if direct {
			rooms_direct_add(h.address, target, person)
		}
		room, err := rooms_join(ctx, h.address, target)
		if err != nil {
			if direct {
				rooms_direct_remove(h.address, target)
			}
			return hosted_failed(h, fn, err)
		}
		return sl_encode(map[string]any{"conversation": room, "name": ""}), nil
	}
	switch l.kind {
	case "xmpp":
		c, err := linked_client(l)
		if err != nil {
			return linked_failed(l, fn, err)
		}
		room, name, err := c.room_join(ctx, address)
		if err != nil {
			return linked_failed(l, fn, err)
		}
		return sl_encode(map[string]any{"conversation": "xmpp:" + room, "name": name}), nil
	default:
		room, err := matrix_room_join(ctx, l, matrix_room(address))
		if err != nil {
			return linked_failed(l, fn, err)
		}
		if resolve_matrix_re.MatchString(person) {
			matrix_direct_add(ctx, l, person, room)
		}
		return sl_encode(map[string]any{"conversation": room, "name": ""}), nil
	}
}

// mochi.account.room.leave(account, conversation) -> None: Leave a room of a linked XMPP or Matrix account, or a hosted Matrix account, or decline an invitation to it, and stop keeping it. Requires the account's permission
func api_account_room_leave(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var account, conversation string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "account", &account, "conversation", &conversation); err != nil {
		return nil, err
	}
	l, h, err := conversing(t, fn, account, "xmpp", "matrix")
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	if h != nil {
		if err := rooms_leave(thread_context(t), h.address, conversation); err != nil {
			return hosted_failed(h, fn, err)
		}
		return sl.None, nil
	}
	switch l.kind {
	case "xmpp":
		c, err := linked_client(l)
		if err != nil {
			return linked_failed(l, fn, err)
		}
		room, err := xmpp_target(conversation)
		if err != nil {
			return linked_failed(l, fn, err)
		}
		c.room_leave(thread_context(t), room.Bare().String())
	case "matrix":
		if err := matrix_room_leave(thread_context(t), l, conversation); err != nil {
			return linked_failed(l, fn, err)
		}
	}
	return sl.None, nil
}

// conversing_failed is a failure of either kind of account as the app sees it.
func conversing_failed(l *linked, h *matrix_hosted, fn *sl.Builtin, err error) (sl.Value, error) {
	if h != nil {
		return hosted_failed(h, fn, err)
	}
	return linked_failed(l, fn, err)
}

// matrix_person is a Matrix person's id from an address as typed: @name:host,
// name:host or matrix:u/name:host.
func matrix_person(address string) string {
	address = strings.TrimPrefix(address, "matrix:u/")
	if !strings.HasPrefix(address, "@") {
		address = "@" + address
	}
	return address
}

// matrix_room is a Matrix room's id or alias from an address as typed:
// !id:host, #alias:host, matrix:roomid/id:host or matrix:r/alias:host.
func matrix_room(address string) string {
	if rest, ok := strings.CutPrefix(address, "matrix:r/"); ok {
		return "#" + rest
	}
	if rest, ok := strings.CutPrefix(address, "matrix:roomid/"); ok {
		return "!" + rest
	}
	return address
}

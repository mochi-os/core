// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: XMPP rosters
//
// A hosted XMPP account's roster is its contacts on XMPP, each with the state
// of the presence subscriptions between them (RFC 6121): whether the account
// sees the contact's presence (to), the contact the account's (from), both or
// neither, and the requests waiting either way. Requests and answers come
// from the account's clients, from other servers, and from people, where an
// XMPP contact is invited and accepted as a friend is; people is told of each
// change through xmpp/roster, so a request shows among its invitations.
//
// The user's Mochi friends who have an XMPP address - an account on this
// server, or one on their contact card - are on the roster too, both ways,
// for as long as they are friends.

package main

import (
	"fmt"
	"strings"
	"sync"
	"time"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
	"mellium.im/xmpp/jid"
)

// An xmpp_item is one of an account's contacts on XMPP.
type xmpp_item struct {
	address      string
	name         string
	subscription string
	ask          bool
	pending      bool
	listed       bool
	friend       bool
}

// xmpp_rows reads an account's stored contacts.
func xmpp_rows(a *xmpp_account) map[string]*xmpp_item {
	rows, _ := xmpp_database().rows("select * from roster where account=?", a.id)
	out := map[string]*xmpp_item{}
	for _, row := range rows {
		out[row_string(row, "address")] = &xmpp_item{
			address:      row_string(row, "address"),
			name:         row_string(row, "name"),
			subscription: row_string(row, "subscription"),
			ask:          row_int(row, "ask") == 1,
			pending:      row_int(row, "pending") == 1,
			listed:       row_int(row, "listed") == 1,
		}
	}
	return out
}

// xmpp_roster lists an account's contacts: those stored, and its user's
// friends with an XMPP address, who see each other's presence.
func xmpp_roster(a *xmpp_account) []*xmpp_item {
	items := xmpp_rows(a)
	for address, name := range xmpp_friends(a) {
		item := items[address]
		if item == nil {
			item = &xmpp_item{address: address, name: name}
			items[address] = item
		}
		if item.name == "" {
			item.name = name
		}
		item.subscription, item.friend, item.listed = "both", true, true
	}
	out := make([]*xmpp_item, 0, len(items))
	for _, item := range items {
		out = append(out, item)
	}
	return out
}

// xmpp_roster_item is one contact of an account's by address, or nil.
func xmpp_roster_item(a *xmpp_account, address string) *xmpp_item {
	address = strings.ToLower(address)
	item := xmpp_rows(a)[address]
	if name, friend := xmpp_friends(a)[address]; friend {
		if item == nil {
			item = &xmpp_item{address: address, name: name}
		}
		item.subscription, item.friend, item.listed = "both", true, true
	}
	return item
}

// xmpp_roster_save stores a contact, or forgets it once nothing is left
// between them and it is not on the roster.
func xmpp_roster_save(a *xmpp_account, item *xmpp_item) {
	db := xmpp_database()
	if item.subscription == "none" && !item.ask && !item.pending && !item.listed {
		db.exec("delete from roster where account=? and address=?", a.id, item.address)
		return
	}
	db.exec("insert into roster (account, address, name, subscription, ask, pending, listed, created, updated) values (?, ?, ?, ?, ?, ?, ?, ?, ?) on conflict(account, address) do update set name=excluded.name, subscription=excluded.subscription, ask=excluded.ask, pending=excluded.pending, listed=excluded.listed, updated=excluded.updated",
		a.id, item.address, item.name, item.subscription, xmpp_flag(item.ask), xmpp_flag(item.pending), xmpp_flag(item.listed), now(), now())
}

func xmpp_flag(on bool) int {
	if on {
		return 1
	}
	return 0
}

// xmpp_item_text writes a contact as a roster item.
func xmpp_item_text(item *xmpp_item) string {
	text := `<item jid="` + xmpp_escape(item.address) + `" subscription="` + item.subscription + `"`
	if item.name != "" {
		text += ` name="` + xmpp_escape(item.name) + `"`
	}
	if item.ask {
		text += ` ask="subscribe"`
	}
	return text + `/>`
}

// xmpp_shown reports whether a contact is on the roster a client sees: one
// added, a friend, or one with a subscription either way or asked for.
func xmpp_item_shown(item *xmpp_item) bool {
	return item.listed || item.friend || item.subscription != "none" || item.ask
}

// xmpp_roster_push tells each of an account's clients of a contact changed,
// or removed where it is no longer on the roster.
func xmpp_roster_push(a *xmpp_account, address string) {
	item := xmpp_roster_item(a, address)
	text := `<item jid="` + xmpp_escape(address) + `" subscription="remove"/>`
	if item != nil && xmpp_item_shown(item) {
		text = xmpp_item_text(item)
	}
	for _, r := range xmpp_resources_of(a.address.String()) {
		r.send(xmpp_make("iq", `<query xmlns="`+xmpp_ns_roster+`">`+text+`</query>`, "type", "set", "id", "push-"+uid(), "to", r.full.String()).text(xmpp_ns_client))
	}
}

// xmpp_roster_iq answers a client asking for the roster, or adding, renaming
// or removing a contact.
func xmpp_roster_iq(a *xmpp_account, r *xmpp_resource, s *xmpp_stanza) *xmpp_stanza {
	switch s.get("type") {
	case "get":
		var items strings.Builder
		for _, item := range xmpp_roster(a) {
			if xmpp_item_shown(item) {
				items.WriteString(xmpp_item_text(item))
			}
		}
		return xmpp_result(s, `<query xmlns="`+xmpp_ns_roster+`">`+items.String()+`</query>`)
	case "set":
		var request struct {
			Items []struct {
				JID          string `xml:"jid,attr"`
				Name         string `xml:"name,attr"`
				Subscription string `xml:"subscription,attr"`
			} `xml:"query>item"`
		}
		if s.decode(&request) != nil || len(request.Items) != 1 {
			return xmpp_failure(s, "bad-request", "modify", "")
		}
		entry := request.Items[0]
		address, err := jid.Parse(entry.JID)
		if err != nil || address.Localpart() == "" || address.Resourcepart() != "" {
			return xmpp_failure(s, "bad-request", "modify", "")
		}
		if address.Bare().Equal(a.address) {
			return xmpp_failure(s, "not-allowed", "cancel", "")
		}
		bare := strings.ToLower(address.String())
		item := xmpp_roster_item(a, bare)
		if entry.Subscription == "remove" {
			if item != nil && item.friend {
				return xmpp_failure(s, "not-allowed", "cancel", "")
			}
			xmpp_roster_remove(a, address)
			return xmpp_result(s, "")
		}
		if item == nil {
			item = &xmpp_item{address: bare, subscription: "none"}
		}
		item.name, item.listed = strings.TrimSpace(entry.Name), true
		if len(item.name) > 200 {
			item.name = item.name[:200]
		}
		xmpp_roster_save(a, item)
		xmpp_roster_push(a, bare)
		return xmpp_result(s, "")
	}
	return xmpp_failure(s, "bad-request", "modify", "")
}

// xmpp_roster_remove ends everything between an account and a contact: the
// subscriptions either way, or the requests, and takes it off the roster.
func xmpp_roster_remove(a *xmpp_account, contact jid.JID) {
	contact = contact.Bare()
	address := strings.ToLower(contact.String())
	item := xmpp_rows(a)[address]
	if item == nil {
		return
	}
	if item.subscription == "to" || item.subscription == "both" || item.ask {
		xmpp_route(xmpp_make("presence", "", "type", "unsubscribe"), a.address, contact)
	}
	if item.subscription == "from" || item.subscription == "both" || item.pending {
		xmpp_route(xmpp_make("presence", "", "type", "unsubscribed"), a.address, contact)
		xmpp_presence_withdraw(a, contact)
	}
	xmpp_database().exec("delete from roster where account=? and address=?", a.id, address)
	xmpp_roster_push(a, address)
	xmpp_people(a, address, "remove")
}

// xmpp_presence_send sends a contact the account's presence: each client's
// and Mochi's.
func xmpp_presence_send(a *xmpp_account, contact jid.JID) {
	for _, r := range xmpp_resources_of(a.address.String()) {
		r.lock.Lock()
		available, inner := r.available, r.presence
		r.lock.Unlock()
		if available {
			xmpp_route(xmpp_make("presence", inner), r.full, contact)
		}
	}
	if inner, shown := xmpp_mochi_presence(a); shown {
		xmpp_route(xmpp_make("presence", inner), a.mochi(), contact)
	}
}

// xmpp_presence_withdraw tells a contact no longer allowed to see the
// account's presence that each of its resources is unavailable.
func xmpp_presence_withdraw(a *xmpp_account, contact jid.JID) {
	for _, r := range xmpp_resources_of(a.address.String()) {
		r.lock.Lock()
		available := r.available
		r.lock.Unlock()
		if available {
			xmpp_route(xmpp_make("presence", "", "type", "unavailable"), r.full, contact)
		}
	}
	if _, shown := xmpp_mochi_presence(a); shown {
		xmpp_route(xmpp_make("presence", "", "type", "unavailable"), a.mochi(), contact)
	}
}

// xmpp_subscription_out takes the account's own subscription request or
// answer to a contact: the roster changed as it makes it, the contact told,
// and on approving, sent the account's presence.
func xmpp_subscription_out(a *xmpp_account, kind string, contact jid.JID) {
	contact = contact.Bare()
	if contact.Localpart() == "" || contact.Equal(a.address) || xmpp_blocked(a.user, contact) {
		return
	}
	address := strings.ToLower(contact.String())
	item := xmpp_rows(a)[address]
	if item == nil {
		item = &xmpp_item{address: address, subscription: "none"}
	}
	_, friend := xmpp_friends(a)[address]
	switch kind {
	case "subscribe":
		if item.subscription != "to" && item.subscription != "both" && !friend {
			item.ask, item.listed = true, true
		}
	case "subscribed":
		if !item.pending {
			return
		}
		item.pending, item.listed = false, true
		item.subscription = map[string]string{"none": "from", "to": "both"}[item.subscription]
		if item.subscription == "" {
			item.subscription = "from"
		}
	case "unsubscribe":
		item.ask = false
		item.subscription = map[string]string{"to": "none", "both": "from"}[item.subscription]
		if item.subscription == "" {
			item.subscription = "none"
		}
	case "unsubscribed":
		item.pending = false
		item.subscription = map[string]string{"from": "none", "both": "to"}[item.subscription]
		if item.subscription == "" {
			item.subscription = "none"
		}
	}
	xmpp_roster_save(a, item)
	xmpp_route(xmpp_make("presence", "", "type", kind), a.address, contact)
	switch kind {
	case "subscribed":
		xmpp_presence_send(a, contact)
	case "unsubscribed":
		xmpp_presence_withdraw(a, contact)
	}
	xmpp_roster_push(a, address)
	xmpp_people(a, address, "")
}

// xmpp_subscription_in takes a contact's subscription request or answer: a
// request from someone already allowed is approved at once, else it waits
// for the user in their clients and in people; an answer settles the account's
// own request, and an end takes the subscription away.
func xmpp_subscription_in(a *xmpp_account, kind string, contact jid.JID, s *xmpp_stanza) {
	address := strings.ToLower(contact.String())
	item := xmpp_rows(a)[address]
	_, friend := xmpp_friends(a)[address]
	var request struct {
		Nick string `xml:"http://jabber.org/protocol/nick nick"`
	}
	s.decode(&request)
	text := xmpp_make("presence", s.inner, "type", kind, "from", address, "to", a.address.String()).text(xmpp_ns_client)
	event := ""
	switch kind {
	case "subscribe":
		if friend || (item != nil && (item.subscription == "from" || item.subscription == "both")) {
			xmpp_route(xmpp_make("presence", "", "type", "subscribed"), a.address, contact)
			xmpp_presence_send(a, contact)
			return
		}
		if item == nil {
			item = &xmpp_item{address: address, subscription: "none"}
		}
		fresh := !item.pending
		item.pending = true
		if nick := strings.TrimSpace(request.Nick); nick != "" && item.name == "" && len(nick) <= 200 {
			item.name = nick
		}
		xmpp_roster_save(a, item)
		xmpp_resources_deliver(a, a.address, text, false)
		if !fresh {
			return
		}
		event = "invite"
	case "subscribed":
		if item == nil || !item.ask {
			return
		}
		item.ask, item.listed = false, true
		item.subscription = map[string]string{"none": "to", "from": "both"}[item.subscription]
		if item.subscription == "" {
			item.subscription = "to"
		}
		event = "accept"
	case "unsubscribe":
		if item == nil || (item.subscription != "from" && item.subscription != "both" && !item.pending) {
			return
		}
		item.pending = false
		item.subscription = map[string]string{"from": "none", "both": "to"}[item.subscription]
		if item.subscription == "" {
			item.subscription = "none"
		}
		event = "remove"
	case "unsubscribed":
		if item == nil || (item.subscription != "to" && item.subscription != "both" && !item.ask) {
			return
		}
		item.ask = false
		item.subscription = map[string]string{"to": "none", "both": "from"}[item.subscription]
		if item.subscription == "" {
			item.subscription = "none"
		}
		event = "remove"
		xmpp_contact_presence(a, contact, true, "", "")
	}
	xmpp_roster_save(a, item)
	if kind != "subscribe" {
		xmpp_resources_deliver(a, a.address, text, false)
	}
	xmpp_roster_push(a, address)
	xmpp_people(a, address, event)
}

// xmpp_people tells the apps declaring xmpp/roster of a contact as it now
// stands, and what just happened: an invitation, an acceptance, a removal,
// or nothing to tell the user of. They are told in order with what the
// account hears.
func xmpp_people(a *xmpp_account, address string, event string) {
	identity := a.user.identity()
	if identity == nil {
		return
	}
	args := Map{"identity": identity.ID, "account": a.id, "address": address, "name": "", "subscription": "none", "ask": false, "pending": false, "event": event}
	if item := xmpp_roster_item(a, address); item != nil {
		args["name"], args["subscription"], args["ask"], args["pending"] = item.name, item.subscription, item.ask, item.pending
	}
	user := a.user
	xmpp_queue(user, func() {
		for _, app := range apps_for_function(user, "xmpp/roster") {
			if _, err := activitypub_call(user, app, "xmpp/roster", args); err != nil {
				info("XMPP roster of %s: %s did not take %s: %v", a.id, app.id, address, err)
			}
		}
	})
}

// === Friends ===

type xmpp_friend_list struct {
	addresses map[string]string
	until     time.Time
}

// Each account's friends with an XMPP address, by address, for five minutes
// or until the user's contacts change.
var (
	xmpp_friend_lists      = map[string]xmpp_friend_list{}
	xmpp_friend_lists_lock sync.Mutex
)

// xmpp_friends is an account's user's friends with an XMPP address, by
// address with their name: an address on the friend's contact card, else the
// friend's hosted XMPP account where they are on this server.
func xmpp_friends(a *xmpp_account) map[string]string {
	xmpp_friend_lists_lock.Lock()
	cached, found := xmpp_friend_lists[a.id]
	xmpp_friend_lists_lock.Unlock()
	if found && time.Now().Before(cached.until) {
		return cached.addresses
	}
	out := map[string]string{}
	if identity := a.user.identity(); identity != nil {
		for _, app := range apps_for_function(a.user, "xmpp/friends") {
			result, err := activitypub_call(a.user, app, "xmpp/friends", Map{"identity": identity.ID})
			if err != nil {
				info("XMPP friends of %s: %s: %v", a.id, app.id, err)
				continue
			}
			list, _ := result.([]any)
			for _, entry := range list {
				friend, _ := entry.(map[string]any)
				if friend == nil {
					continue
				}
				name, _ := friend["name"].(string)
				var addresses []string
				for _, value := range object_strings(friend["addresses"]) {
					if address, err := xmpp_address(value); err == nil {
						addresses = append(addresses, strings.ToLower(address.String()))
					}
				}
				if person, _ := friend["person"].(string); len(addresses) == 0 && person != "" {
					if local := user_by_identity(person); local != nil && local.UID != a.user.UID {
						for _, row := range accounts_hosted(local, "xmpp") {
							if address, err := xmpp_address(row_string(row, "identifier")); err == nil {
								addresses = append(addresses, strings.ToLower(address.String()))
							}
						}
					}
				}
				for _, address := range addresses {
					if address != strings.ToLower(a.address.String()) {
						out[address] = name
					}
				}
			}
		}
	}
	xmpp_friend_lists_lock.Lock()
	xmpp_friend_lists[a.id] = xmpp_friend_list{addresses: out, until: time.Now().Add(5 * time.Minute)}
	xmpp_friend_lists_lock.Unlock()
	return out
}

// xmpp_friends_changed reads a user's friends again after their contacts
// changed, telling each account's clients of the contacts that came or went.
func xmpp_friends_changed(user *User) {
	if !network_enabled("xmpp") {
		return
	}
	for _, row := range accounts_hosted(user, "xmpp") {
		a := xmpp_hosted_of(user, row)
		if a == nil {
			continue
		}
		xmpp_friend_lists_lock.Lock()
		before := xmpp_friend_lists[a.id].addresses
		delete(xmpp_friend_lists, a.id)
		xmpp_friend_lists_lock.Unlock()
		after := xmpp_friends(a)
		for address := range after {
			if _, was := before[address]; !was {
				xmpp_roster_push(a, address)
			}
		}
		for address := range before {
			if _, is := after[address]; !is {
				xmpp_roster_push(a, address)
			}
		}
	}
}

// === The queue ===

// xmpp_queue runs work for a user after what came before it, on the user's
// queue to the apps.
func xmpp_queue(user *User, work func()) {
	xmpp_queues_lock.Lock()
	queue := xmpp_queues[user.UID]
	if queue == nil {
		queue = make(chan xmpp_batch, 1000)
		xmpp_queues[user.UID] = queue
		go xmpp_teller(user.UID, queue)
	}
	xmpp_queues_lock.Unlock()
	xmpp_running.Add(1)
	select {
	case queue <- xmpp_batch{work: work}:
	default:
		xmpp_running.Done()
		info("XMPP queue of %s is full; work dropped", user.UID)
	}
}

// === Functions ===

var api_account_roster = sls.FromStringDict(sl.String("mochi.account.roster"), sl.StringDict{
	"add":    sl.NewBuiltin("mochi.account.roster.add", api_account_roster_add),
	"list":   sl.NewBuiltin("mochi.account.roster.list", api_account_roster_list),
	"remove": sl.NewBuiltin("mochi.account.roster.remove", api_account_roster_remove),
})

// mochi.account.roster.list() -> dict: The user's hosted XMPP accounts and their contacts on XMPP: accounts, each {id, address}, and items, each {account, address, name, subscription, ask, pending} - subscription is none, to (the account sees their presence), from (they see the account's) or both; ask is the account's request waiting, pending theirs. Friends shown on the roster by their address are not listed. Requires roster/read
func api_account_roster_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := sl.UnpackArgs(fn.Name(), args, kwargs); err != nil {
		return nil, err
	}
	if err := require_permission(t, fn, "roster/read"); err != nil {
		return sl_error(fn, "%v", err)
	}
	user := principal_caller(t)
	if user == nil {
		return sl_error(fn, "no user")
	}
	accounts, items := []any{}, []any{}
	if network_enabled("xmpp") {
		for _, row := range accounts_hosted(user, "xmpp") {
			a := xmpp_hosted_of(user, row)
			if a == nil {
				continue
			}
			accounts = append(accounts, map[string]any{"id": a.id, "address": a.address.String()})
			for _, item := range xmpp_rows(a) {
				items = append(items, map[string]any{"account": a.id, "address": item.address, "name": item.name, "subscription": item.subscription, "ask": item.ask, "pending": item.pending})
			}
		}
	}
	return sl_encode(map[string]any{"accounts": accounts, "items": items}), nil
}

// roster_call resolves the account and the person a roster function names.
func roster_call(t *sl.Thread, fn *sl.Builtin, account string, address string) (*xmpp_account, jid.JID, error) {
	if err := require_permission(t, fn, "roster/write"); err != nil {
		return nil, jid.JID{}, err
	}
	user := principal_caller(t)
	if user == nil {
		return nil, jid.JID{}, fmt.Errorf("no user")
	}
	if !network_enabled("xmpp") {
		return nil, jid.JID{}, fmt.Errorf("xmpp is off")
	}
	a := xmpp_hosted_of(user, account_hosted_get(user, account))
	if a == nil {
		return nil, jid.JID{}, fmt.Errorf("no such hosted XMPP account")
	}
	contact, err := xmpp_address(address)
	if err != nil {
		return nil, jid.JID{}, err
	}
	if contact.Equal(a.address) {
		return nil, jid.JID{}, fmt.Errorf("the account itself")
	}
	return a, contact, nil
}

// mochi.account.roster.add(account, address, name="") -> None: Add a person to a hosted XMPP account's contacts by their address (name@host), named as given: their request approved where they asked, and the account's own sent, so that each sees the other's presence. Requires roster/write
func api_account_roster_add(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var account, address, name string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "account", &account, "address", &address, "name?", &name); err != nil {
		return nil, err
	}
	a, contact, err := roster_call(t, fn, account, address)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	if xmpp_blocked(a.user, contact) {
		return sl_error(fn, "the person is blocked")
	}
	bare := strings.ToLower(contact.String())
	item := xmpp_rows(a)[bare]
	if item == nil {
		item = &xmpp_item{address: bare, subscription: "none"}
	}
	if name = strings.TrimSpace(name); name != "" {
		item.name = name
		if len(item.name) > 200 {
			item.name = item.name[:200]
		}
	}
	item.listed = true
	xmpp_roster_save(a, item)
	if item.pending {
		xmpp_subscription_out(a, "subscribed", contact)
	}
	if current := xmpp_rows(a)[bare]; current == nil || (current.subscription != "to" && current.subscription != "both" && !current.ask) {
		xmpp_subscription_out(a, "subscribe", contact)
	}
	xmpp_roster_push(a, bare)
	return sl.None, nil
}

// mochi.account.roster.remove(account, address) -> None: End what a hosted XMPP account has with a person on XMPP: the subscriptions either way, a request refused or withdrawn, and the person taken off the account's contacts. Requires roster/write
func api_account_roster_remove(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var account, address string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "account", &account, "address", &address); err != nil {
		return nil, err
	}
	a, contact, err := roster_call(t, fn, account, address)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	xmpp_roster_remove(a, contact)
	return sl.None, nil
}

// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: what a hosted XMPP account publishes
//
// A hosted account's own publishing service (XEP-0163, the federation plan,
// phase 26): nodes of items it publishes, kept in xmpp.db, each saying who may
// read it - the account alone, its contacts, or anyone - and how many items it
// keeps. The account's clients are told of each item published or withdrawn.
// Its bookmarks (XEP-0402) are one such node: the rooms the account keeps,
// which Mochi's own sessions keep too, joining a room its clients bookmark to
// be joined and leaving one whose bookmark they withdraw.

package main

import (
	"strconv"
	"strings"

	"mellium.im/xmpp/jid"
)

const (
	xmpp_ns_pubsub       = "http://jabber.org/protocol/pubsub"
	xmpp_ns_pubsub_owner = "http://jabber.org/protocol/pubsub#owner"
	xmpp_ns_pubsub_event = "http://jabber.org/protocol/pubsub#event"
	xmpp_ns_bookmarks    = "urn:xmpp:bookmarks:1"
)

// The most items a node keeps, and the largest item.
const (
	xmpp_pep_items = 1000
	xmpp_pep_size  = 65536
)

// An xmpp_node is how a node is set up: who may read it, and how many items
// it keeps.
type xmpp_node_config struct {
	access  string
	maximum int
}

// xmpp_node_of is a node's configuration, or nil where the account has none.
func xmpp_node_of(a *xmpp_account, node string) *xmpp_node_config {
	row, _ := xmpp_database().row("select access, maximum from nodes where account=? and node=?", a.id, node)
	if row == nil {
		return nil
	}
	return &xmpp_node_config{access: row_string(row, "access"), maximum: int(row_int(row, "maximum"))}
}

// xmpp_pep_options reads what a publish asks of its node: who may read it
// and how many items it keeps.
func xmpp_pep_options(fields map[string]string, config *xmpp_node_config) {
	if access := fields["pubsub#access_model"]; access == "open" || access == "presence" || access == "roster" || access == "whitelist" || access == "authorize" {
		config.access = access
	}
	if maximum := fields["pubsub#max_items"]; maximum == "max" {
		config.maximum = xmpp_pep_items
	} else if n, err := strconv.Atoi(maximum); err == nil && n > 0 {
		config.maximum = min(n, xmpp_pep_items)
	}
}

// xmpp_pep_publish keeps an item of a node, made where it is new with the
// options given, and tells the account's clients. Only the newest items the
// node keeps stay.
func xmpp_pep_publish(a *xmpp_account, node string, item string, payload string, fields map[string]string) {
	config := xmpp_node_of(a, node)
	db := xmpp_database()
	if config == nil {
		config = &xmpp_node_config{access: "presence", maximum: 1}
		xmpp_pep_options(fields, config)
		db.exec("insert into nodes (account, node, access, maximum, created) values (?, ?, ?, ?, ?)", a.id, node, config.access, config.maximum, now())
	} else if len(fields) > 0 {
		xmpp_pep_options(fields, config)
		db.exec("update nodes set access=?, maximum=? where account=? and node=?", config.access, config.maximum, a.id, node)
	}
	db.exec("insert into items (account, node, item, payload, updated) values (?, ?, ?, ?, ?) on conflict(account, node, item) do update set payload=excluded.payload, updated=excluded.updated", a.id, node, item, payload, now())
	db.exec("delete from items where account=? and node=? and rowid not in (select rowid from items where account=? and node=? order by updated desc, rowid desc limit ?)", a.id, node, a.id, node, config.maximum)
	xmpp_pep_notify(a, `<items node="`+xmpp_escape(node)+`"><item id="`+xmpp_escape(item)+`">`+payload+`</item></items>`)
	if node == xmpp_ns_bookmarks {
		xmpp_bookmark_published(a, item, payload)
	}
}

// xmpp_pep_retract withdraws an item of a node and tells the account's
// clients.
func xmpp_pep_retract(a *xmpp_account, node string, item string) bool {
	db := xmpp_database()
	found, _ := db.exists("select 1 from items where account=? and node=? and item=?", a.id, node, item)
	if !found {
		return false
	}
	db.exec("delete from items where account=? and node=? and item=?", a.id, node, item)
	xmpp_pep_notify(a, `<items node="`+xmpp_escape(node)+`"><retract id="`+xmpp_escape(item)+`"/></items>`)
	if node == xmpp_ns_bookmarks {
		xmpp_bookmark_withdrawn(a, item)
	}
	return true
}

// xmpp_pep_notify tells each of the account's clients of a change to a node.
func xmpp_pep_notify(a *xmpp_account, inner string) {
	for _, r := range xmpp_resources_of(a.address.String()) {
		r.lock.Lock()
		available := r.available
		r.lock.Unlock()
		if available {
			r.send(xmpp_make("message", `<event xmlns="`+xmpp_ns_pubsub_event+`">`+inner+`</event>`, "from", a.address.String(), "to", r.full.String(), "type", "headline", "id", uid()).text(xmpp_ns_client))
		}
	}
}

// An xmpp_pep_request is a request to an account's publishing service.
type xmpp_pep_request struct {
	Pubsub *struct {
		Publish *struct {
			Node string `xml:"node,attr"`
			Item *struct {
				ID    string `xml:"id,attr"`
				Inner string `xml:",innerxml"`
			} `xml:"item"`
		} `xml:"publish"`
		Options *struct {
			Fields []struct {
				Var    string   `xml:"var,attr"`
				Values []string `xml:"value"`
			} `xml:"x>field"`
		} `xml:"publish-options"`
		Items *struct {
			Node    string `xml:"node,attr"`
			Maximum string `xml:"max_items,attr"`
			Item    []struct {
				ID string `xml:"id,attr"`
			} `xml:"item"`
		} `xml:"items"`
		Retract *struct {
			Node string `xml:"node,attr"`
			Item []struct {
				ID string `xml:"id,attr"`
			} `xml:"item"`
		} `xml:"retract"`
	} `xml:"http://jabber.org/protocol/pubsub pubsub"`
	Owner *struct {
		Delete *struct {
			Node string `xml:"node,attr"`
		} `xml:"delete"`
		Configure *struct {
			Node   string `xml:"node,attr"`
			Fields []struct {
				Var    string   `xml:"var,attr"`
				Values []string `xml:"value"`
			} `xml:"x>field"`
		} `xml:"configure"`
	} `xml:"http://jabber.org/protocol/pubsub#owner pubsub"`
}

// xmpp_pep_iq answers the account's own clients: publishing, reading,
// withdrawing, configuring and deleting its nodes.
func xmpp_pep_iq(a *xmpp_account, s *xmpp_stanza, own bool) *xmpp_stanza {
	var request xmpp_pep_request
	if s.decode(&request) != nil {
		return xmpp_failure(s, "bad-request", "modify", "")
	}
	kind := s.get("type")
	db := xmpp_database()
	switch {
	case request.Pubsub != nil && request.Pubsub.Publish != nil && kind == "set":
		p := request.Pubsub.Publish
		if p.Node == "" || len(p.Node) > 1024 || p.Item == nil {
			return xmpp_failure(s, "bad-request", "modify", "")
		}
		payload := strings.TrimSpace(p.Item.Inner)
		if len(payload) > xmpp_pep_size {
			return xmpp_failure(s, "not-acceptable", "modify", "")
		}
		if _, err := xmpp_parse(payload); err != nil {
			return xmpp_failure(s, "bad-request", "modify", "")
		}
		item := p.Item.ID
		if item == "" {
			item = uid()
		}
		fields := map[string]string{}
		if o := request.Pubsub.Options; o != nil {
			for _, f := range o.Fields {
				if len(f.Values) > 0 {
					fields[f.Var] = strings.TrimSpace(f.Values[0])
				}
			}
		}
		xmpp_pep_publish(a, p.Node, item, payload, fields)
		return xmpp_result(s, `<pubsub xmlns="`+xmpp_ns_pubsub+`"><publish node="`+xmpp_escape(p.Node)+`"><item id="`+xmpp_escape(item)+`"/></publish></pubsub>`)
	case request.Pubsub != nil && request.Pubsub.Items != nil && kind == "get":
		return xmpp_pep_items_answer(a, s, request.Pubsub.Items.Node, request.Pubsub.Items.Maximum, request.Pubsub.Items.Item)
	case request.Pubsub != nil && request.Pubsub.Retract != nil && kind == "set":
		r := request.Pubsub.Retract
		for _, item := range r.Item {
			xmpp_pep_retract(a, r.Node, item.ID)
		}
		return xmpp_result(s, "")
	case request.Owner != nil && request.Owner.Delete != nil && kind == "set":
		node := request.Owner.Delete.Node
		if xmpp_node_of(a, node) == nil {
			return xmpp_failure(s, "item-not-found", "cancel", "")
		}
		if node == xmpp_ns_bookmarks {
			rows, _ := db.rows("select item from items where account=? and node=?", a.id, node)
			for _, row := range rows {
				xmpp_bookmark_withdrawn(a, row_string(row, "item"))
			}
		}
		db.exec("delete from items where account=? and node=?", a.id, node)
		db.exec("delete from nodes where account=? and node=?", a.id, node)
		xmpp_pep_notify(a, `<delete node="`+xmpp_escape(node)+`"/>`)
		return xmpp_result(s, "")
	case request.Owner != nil && request.Owner.Configure != nil:
		c := request.Owner.Configure
		config := xmpp_node_of(a, c.Node)
		if config == nil {
			return xmpp_failure(s, "item-not-found", "cancel", "")
		}
		if kind == "get" {
			maximum := strconv.Itoa(config.maximum)
			return xmpp_result(s, `<pubsub xmlns="`+xmpp_ns_pubsub_owner+`"><configure node="`+xmpp_escape(c.Node)+`"><x xmlns="`+xmpp_ns_forms+`" type="form"><field var="FORM_TYPE" type="hidden"><value>http://jabber.org/protocol/pubsub#node_config</value></field><field var="pubsub#access_model" type="list-single"><value>`+config.access+`</value></field><field var="pubsub#max_items" type="text-single"><value>`+maximum+`</value></field></x></configure></pubsub>`)
		}
		fields := map[string]string{}
		for _, f := range c.Fields {
			if len(f.Values) > 0 {
				fields[f.Var] = strings.TrimSpace(f.Values[0])
			}
		}
		xmpp_pep_options(fields, config)
		db.exec("update nodes set access=?, maximum=? where account=? and node=?", config.access, config.maximum, a.id, c.Node)
		return xmpp_result(s, "")
	}
	return xmpp_failure(s, "feature-not-implemented", "cancel", `<unsupported xmlns="`+xmpp_ns_pubsub+`#errors" feature="subscribe"/>`)
}

// xmpp_pep_items_answer is a node's items: the newest, as many as asked, or
// those named.
func xmpp_pep_items_answer(a *xmpp_account, s *xmpp_stanza, node string, maximum string, named []struct {
	ID string `xml:"id,attr"`
}) *xmpp_stanza {
	if xmpp_node_of(a, node) == nil {
		return xmpp_failure(s, "item-not-found", "cancel", "")
	}
	db := xmpp_database()
	var rows []map[string]any
	if len(named) > 0 {
		for _, item := range named {
			if row, _ := db.row("select item, payload from items where account=? and node=? and item=?", a.id, node, item.ID); row != nil {
				rows = append(rows, row)
			}
		}
	} else {
		limit := xmpp_pep_items
		if n, err := strconv.Atoi(maximum); err == nil && n > 0 {
			limit = min(n, xmpp_pep_items)
		}
		rows, _ = db.rows("select item, payload from items where account=? and node=? order by updated desc, rowid desc limit ?", a.id, node, limit)
	}
	var text strings.Builder
	for _, row := range rows {
		text.WriteString(`<item id="` + xmpp_escape(row_string(row, "item")) + `">` + row_string(row, "payload") + `</item>`)
	}
	return xmpp_result(s, `<pubsub xmlns="`+xmpp_ns_pubsub+`"><items node="`+xmpp_escape(node)+`">`+text.String()+`</items></pubsub>`)
}

// xmpp_pep_iq_from answers someone else reading one of the account's nodes,
// where its access lets them: anyone, or a contact who may see the account's
// presence. A node they may not read is not found.
func xmpp_pep_iq_from(a *xmpp_account, s *xmpp_stanza, from jid.JID) *xmpp_stanza {
	if from.Bare().Equal(a.address) {
		return xmpp_pep_iq(a, s, true)
	}
	var request xmpp_pep_request
	if s.decode(&request) != nil || request.Pubsub == nil || request.Pubsub.Items == nil || s.get("type") != "get" {
		return xmpp_failure(s, "forbidden", "auth", "")
	}
	node := request.Pubsub.Items.Node
	config := xmpp_node_of(a, node)
	allowed := false
	if config != nil {
		switch config.access {
		case "open":
			allowed = true
		case "presence", "roster":
			item := xmpp_roster_item(a, from.Bare().String())
			allowed = item != nil && (item.subscription == "from" || item.subscription == "both")
		}
	}
	if !allowed {
		return xmpp_failure(s, "item-not-found", "cancel", "")
	}
	return xmpp_pep_items_answer(a, s, node, request.Pubsub.Items.Maximum, request.Pubsub.Items.Item)
}

// === Bookmarks ===

// xmpp_bookmark_keep keeps a room Mochi's own sessions joined in the account's
// bookmarks, to be joined by its clients too.
func xmpp_bookmark_keep(a *xmpp_account, room string, name string, nick string) {
	payload := `<conference xmlns="` + xmpp_ns_bookmarks + `" name="` + xmpp_escape(name) + `" autojoin="true"><nick>` + xmpp_escape(nick) + `</nick></conference>`
	xmpp_pep_publish(a, xmpp_ns_bookmarks, room, payload, map[string]string{"pubsub#access_model": "whitelist", "pubsub#max_items": "max"})
}

// xmpp_bookmark_drop withdraws a room from the account's bookmarks.
func xmpp_bookmark_drop(a *xmpp_account, room string) {
	xmpp_pep_retract(a, xmpp_ns_bookmarks, room)
}

// xmpp_bookmark_published joins a room a client bookmarked to be joined,
// where Mochi's own sessions are not in it yet; the apps hear of it as of any
// room joined.
func xmpp_bookmark_published(a *xmpp_account, item string, payload string) {
	var b struct {
		Name     string `xml:"name,attr"`
		Autojoin string `xml:"autojoin,attr"`
		Nick     string `xml:"nick"`
	}
	s, err := xmpp_parse(payload)
	if err != nil || s.space != xmpp_ns_bookmarks || s.name != "conference" || s.decode(&b) != nil {
		return
	}
	room, err := jid.Parse(item)
	if err != nil || room.Localpart() == "" || (b.Autojoin != "true" && b.Autojoin != "1") {
		return
	}
	room = room.Bare()
	if xmpp_joined_of(a, room.String()) != nil || xmpp_joining_of(a, room.String()) != nil {
		return
	}
	nick := strings.TrimSpace(b.Nick)
	if nick == "" {
		nick = a.address.Localpart()
	}
	name := strings.TrimSpace(b.Name)
	if name == "" {
		name = room.Localpart()
	}
	xmpp_running.Add(1)
	go func() {
		defer xmpp_running.Done()
		if err := xmpp_room_enter(a, room, nick, name, 0, false); err != nil {
			debug("XMPP account %s: bookmarked room %s not joined: %v", a.address, room, err)
		}
	}()
}

// xmpp_bookmark_withdrawn leaves a room whose bookmark a client withdrew;
// the apps hear of it as of a room left.
func xmpp_bookmark_withdrawn(a *xmpp_account, item string) {
	j := xmpp_joined_of(a, item)
	if j == nil {
		return
	}
	if full, err := jid.Parse(j.room + "/" + j.nick); err == nil {
		xmpp_route(xmpp_make("presence", "", "type", "unavailable"), a.mochi(), full)
	}
	xmpp_database().exec("delete from joined where account=? and room=?", a.id, j.room)
	xmpp_tell(a, []map[string]any{{"kind": "room", "conversation": "xmpp:" + j.room, "left": true}})
}

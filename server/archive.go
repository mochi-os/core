// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: a hosted XMPP account's archive
//
// The account's archive (XEP-0313) is chat's messages table (the federation
// plan, phase 26): a client's query is answered from the conversations chat
// holds for the account, through chat's linked/archive, so that an XMPP client
// sees the conversation Mochi's own clients show, with what was sent from
// either and as it stands after corrections and withdrawals. Each message of a
// conversation that comes to the account or goes from it is given an id in the
// archive as it passes (XEP-0359), which its clients are told and chat keeps.
// An encrypted message's content is kept by chat as it came, so the archive
// gives it back to the account's other devices; a message chat sent with
// files is one message for its text and one for each file, each with an id of
// its own.

package main

import (
	"encoding/xml"
	"strconv"
	"strings"
	"time"

	"mellium.im/xmpp/jid"
)

const (
	xmpp_ns_archive = "urn:xmpp:mam:2"
	xmpp_ns_paging  = "http://jabber.org/protocol/rsm"
	xmpp_ns_sid     = "urn:xmpp:sid:0"
	xmpp_ns_forward = "urn:xmpp:forward:0"
	xmpp_ns_delay   = "urn:xmpp:delay"
	xmpp_ns_forms   = "jabber:x:data"
)

// The most messages one archive query answers, and how many when it does not
// say.
const (
	xmpp_archive_maximum = 250
	xmpp_archive_default = 50
)

// The namespaces an encrypted message's content is carried in, which chat
// keeps for the archive.
var xmpp_encryptions = []string{"eu.siacs.conversations.axolotl", "urn:xmpp:omemo:2", "urn:xmpp:openpgp:0", "jabber:x:encrypted"}

// === A stanza's parts ===

// An xmpp_child is an element directly inside a stanza: its namespace, name
// and attributes, and where its text lies in the stanza's content.
type xmpp_child struct {
	space string
	name  string
	attrs []xml.Attr
	start int
	end   int
}

// children lists the elements directly inside the stanza, read in the
// namespaces the stanza declares.
func (s *xmpp_stanza) children() []xmpp_child {
	var head strings.Builder
	head.WriteString(`<w xmlns="` + xmpp_ns_client + `"`)
	for _, a := range s.attrs {
		if a.Name.Space == "xmlns" {
			head.WriteString(` xmlns:` + a.Name.Local + `="` + xmpp_escape(a.Value) + `"`)
		}
	}
	head.WriteString(">")
	prefix := head.String()
	d := xml.NewDecoder(strings.NewReader(prefix + s.inner + "</w>"))
	var out []xmpp_child
	depth := 0
	for {
		offset := int(d.InputOffset()) - len(prefix)
		t, err := d.Token()
		if err != nil {
			return out
		}
		switch t := t.(type) {
		case xml.StartElement:
			depth++
			if depth == 2 {
				out = append(out, xmpp_child{space: t.Name.Space, name: t.Name.Local, attrs: t.Attr, start: offset})
			}
		case xml.EndElement:
			if depth == 2 && len(out) > 0 {
				out[len(out)-1].end = int(d.InputOffset()) - len(prefix)
			}
			depth--
		}
	}
}

// attribute is one of a child's own attributes.
func (c *xmpp_child) attribute(name string) string {
	for _, a := range c.attrs {
		if a.Name.Space == "" && a.Name.Local == name {
			return a.Value
		}
	}
	return ""
}

// strip takes out of the stanza the elements inside it a test picks.
func (s *xmpp_stanza) strip(picked func(c *xmpp_child) bool) {
	children := s.children()
	inner := s.inner
	for i := len(children) - 1; i >= 0; i-- {
		c := children[i]
		if c.end > c.start && picked(&c) {
			inner = inner[:c.start] + inner[c.end:]
		}
	}
	s.inner = inner
}

// xmpp_stanza_id gives a message its id in an archive (XEP-0359), taking out
// any id it claims the archive gave it already, which only the archive may.
func xmpp_stanza_id(s *xmpp_stanza, by string, id string) {
	s.strip(func(c *xmpp_child) bool {
		return c.space == xmpp_ns_sid && c.name == "stanza-id" && strings.EqualFold(c.attribute("by"), by)
	})
	s.inner += `<stanza-id xmlns="` + xmpp_ns_sid + `" id="` + xmpp_escape(id) + `" by="` + xmpp_escape(by) + `"/>`
}

// xmpp_payload is an encrypted message's content as it came: the elements it
// is carried in, which chat keeps for the archive, or "" where it is not
// encrypted or too large to keep.
func xmpp_payload(s *xmpp_stanza) string {
	var out strings.Builder
	for _, c := range s.children() {
		for _, space := range xmpp_encryptions {
			if c.space == space && c.end > c.start {
				out.WriteString(s.inner[c.start:c.end])
			}
		}
	}
	if out.Len() > 65536 {
		return ""
	}
	return out.String()
}

// xmpp_archive_result reports whether a message is an archive's answer to a
// query.
func xmpp_archive_result(s *xmpp_stanza) bool {
	for _, c := range s.children() {
		if c.space == xmpp_ns_archive && c.name == "result" {
			return true
		}
	}
	return false
}

// xmpp_archivable reports whether a message is one the archive keeps: one of
// a conversation with content, rather than a correction, a withdrawal, a
// reaction or a marker, which chat applies to what it keeps.
func xmpp_archivable(kind string, m *xmpp_message) bool {
	if kind != "" && kind != "chat" && kind != "normal" {
		return false
	}
	if m.Replace != nil || m.Retract != nil || m.Fasten != nil || m.Reactions != nil || m.Result != nil || len(m.Carbons) > 0 || len(m.Sent) > 0 {
		return false
	}
	return strings.TrimSpace(m.Body) != "" || m.encrypted() || len(m.Links) > 0
}

// === Queries ===

// An xmpp_query_page is what a query asks: its id, the conversation, the
// times it is between, where it starts or ends, and how many it wants.
type xmpp_query_page struct {
	id      string
	with    string
	start   int64
	end     int64
	after   string
	before  string
	last    bool
	maximum int
}

// xmpp_archive_query reads a client's query of an archive, or reports it
// malformed.
func xmpp_archive_query(s *xmpp_stanza) (*xmpp_query_page, bool) {
	var request struct {
		Query struct {
			ID     string `xml:"queryid,attr"`
			Fields []struct {
				Var    string   `xml:"var,attr"`
				Values []string `xml:"value"`
			} `xml:"jabber:x:data x>field"`
			Set *struct {
				Maximum *string `xml:"max"`
				After   *string `xml:"after"`
				Before  *string `xml:"before"`
			} `xml:"http://jabber.org/protocol/rsm set"`
		} `xml:"urn:xmpp:mam:2 query"`
	}
	if s.decode(&request) != nil {
		return nil, false
	}
	q := &xmpp_query_page{id: request.Query.ID, maximum: xmpp_archive_default}
	for _, f := range request.Query.Fields {
		value := ""
		if len(f.Values) > 0 {
			value = strings.TrimSpace(f.Values[0])
		}
		switch f.Var {
		case "with":
			with, err := jid.Parse(value)
			if err != nil {
				return nil, false
			}
			q.with = with.Bare().String()
		case "start", "end":
			stamp, err := time.Parse(time.RFC3339, value)
			if err != nil {
				return nil, false
			}
			if f.Var == "start" {
				q.start = stamp.Unix()
			} else {
				q.end = stamp.Unix()
			}
		case "after-id":
			q.after = value
		case "before-id":
			q.before = value
		}
	}
	if set := request.Query.Set; set != nil {
		if set.Maximum != nil {
			maximum, err := strconv.Atoi(strings.TrimSpace(*set.Maximum))
			if err != nil || maximum < 0 {
				return nil, false
			}
			q.maximum = min(maximum, xmpp_archive_maximum)
		}
		if set.After != nil {
			q.after = strings.TrimSpace(*set.After)
		}
		if set.Before != nil {
			q.before = strings.TrimSpace(*set.Before)
			q.last = q.before == ""
		}
	}
	return q, true
}

// xmpp_archive_form is the form a query may fill.
func xmpp_archive_form(s *xmpp_stanza) *xmpp_stanza {
	return xmpp_result(s, `<query xmlns="`+xmpp_ns_archive+`"><x xmlns="`+xmpp_ns_forms+`" type="form"><field type="hidden" var="FORM_TYPE"><value>`+xmpp_ns_archive+`</value></field><field type="jid-single" var="with"/><field type="text-single" var="start"/><field type="text-single" var="end"/><field type="text-single" var="after-id"/><field type="text-single" var="before-id"/></x></query>`)
}

// An xmpp_entry is one message an archive answers with: its id there, when it
// came, and the message.
type xmpp_entry struct {
	archive string
	created int64
	text    string
}

// xmpp_item_id splits an item's id into the message chat keeps it under and
// its place among that message's items: a message chat sent with files is one
// item for its text and one for each file, the later ones as <id>+<n>.
func xmpp_item_id(id string) (string, int) {
	base, place, found := strings.Cut(id, "+")
	if !found {
		return id, 0
	}
	n, err := strconv.Atoi(place)
	if err != nil || n < 1 {
		return id, 0
	}
	return base, n
}

// xmpp_archive_iq answers a client's query of its account's archive: the
// form, or the messages a page holds, each sent to the client before the
// answer saying whether the page is the last. Chat is asked off the client's
// stream, so the stream is read meanwhile.
func xmpp_archive_iq(a *xmpp_account, r *xmpp_resource, s *xmpp_stanza) *xmpp_stanza {
	if s.get("type") == "get" {
		return xmpp_archive_form(s)
	}
	q, ok := xmpp_archive_query(s)
	if !ok {
		return xmpp_failure(s, "bad-request", "modify", "")
	}
	xmpp_running.Add(1)
	go func() {
		defer xmpp_running.Done()
		items, complete, found := xmpp_archive_answer(a, q)
		var reply *xmpp_stanza
		if !found {
			reply = xmpp_failure(s, "item-not-found", "cancel", "")
		} else {
			for _, item := range items {
				r.send(xmpp_make("message", `<result xmlns="`+xmpp_ns_archive+`" queryid="`+xmpp_escape(q.id)+`" id="`+xmpp_escape(item.archive)+`"><forwarded xmlns="`+xmpp_ns_forward+`"><delay xmlns="`+xmpp_ns_delay+`" stamp="`+time.Unix(item.created, 0).UTC().Format(time.RFC3339)+`"/>`+item.text+`</forwarded></result>`, "from", a.address.String(), "to", r.full.String()).text(xmpp_ns_client))
			}
			reply = xmpp_result(s, xmpp_archive_fin(items, complete))
		}
		reply.set("to", r.full.String())
		r.send(reply.text(xmpp_ns_client))
	}()
	return nil
}

// xmpp_archive_fin is the answer ending a page: whether it is the last, and
// the ids of its first and last items.
func xmpp_archive_fin(items []xmpp_entry, complete bool) string {
	text := `<fin xmlns="` + xmpp_ns_archive + `"`
	if complete {
		text += ` complete="true"`
	}
	text += `><set xmlns="` + xmpp_ns_paging + `">`
	if len(items) > 0 {
		text += `<first>` + xmpp_escape(items[0].archive) + `</first><last>` + xmpp_escape(items[len(items)-1].archive) + `</last>`
	} else {
		text += `<count>0</count>`
	}
	return text + `</set></fin>`
}

// xmpp_archive_answer is a page of the account's archive, as chat holds it:
// the items, whether the page reaches the archive's end in the direction it
// runs, and false where the place it starts from is not in the archive.
func xmpp_archive_answer(a *xmpp_account, q *xmpp_query_page) ([]xmpp_entry, bool, bool) {
	anchor, place := "", 0
	switch {
	case q.after != "":
		anchor, place = xmpp_item_id(q.after)
	case q.before != "":
		anchor, place = xmpp_item_id(q.before)
	}
	result := xmpp_archive_ask(a, Map{"account": a.id, "network": "xmpp", "conversation": xmpp_when(q.with != "", "xmpp:"+q.with), "start": q.start, "end": q.end, "after": xmpp_when(q.after != "", anchor), "before": xmpp_when(q.before != "", anchor), "last": q.last, "maximum": q.maximum})
	if result == nil {
		return nil, true, true
	}
	if found, _ := result["found"].(bool); !found && anchor != "" {
		return nil, true, false
	}
	more, _ := result["more"].(bool)
	list, _ := result["messages"].([]any)
	var items []xmpp_entry
	for _, entry := range list {
		message, _ := entry.(map[string]any)
		if message == nil {
			continue
		}
		expanded := xmpp_archive_items(a, message)
		archive, _ := message["archive"].(string)
		for i, item := range expanded {
			// The message a page starts after or before is given again so its
			// items can be told apart; the items up to the place are dropped.
			if archive == anchor && anchor != "" {
				if q.after != "" && i <= place {
					continue
				}
				if q.before != "" && i >= place {
					continue
				}
			}
			items = append(items, item)
		}
	}
	trimmed := false
	if len(items) > q.maximum {
		trimmed = true
		if q.before != "" || q.last {
			items = items[len(items)-q.maximum:]
		} else {
			items = items[:q.maximum]
		}
	}
	return items, !more && !trimmed, true
}

// xmpp_when is a value where a condition holds, else "".
func xmpp_when(condition bool, value string) string {
	if condition {
		return value
	}
	return ""
}

// xmpp_archive_ask asks the apps declaring linked/archive for a page of the
// account's conversations; the first to answer is the archive.
var xmpp_archive_ask = func(a *xmpp_account, arguments Map) map[string]any {
	for _, app := range apps_for_function(a.user, "linked/archive") {
		result, err := activitypub_call(a.user, app, "linked/archive", arguments)
		if err != nil {
			info("Linked archive from %s: %v", app.id, err)
			continue
		}
		if answer, ok := result.(map[string]any); ok {
			return answer
		}
	}
	return nil
}

// xmpp_archive_items is a message chat keeps as the items the archive gives:
// its text, carrying its files where it came as one message, else each file
// as a message of its own after it.
func xmpp_archive_items(a *xmpp_account, message map[string]any) []xmpp_entry {
	archive, _ := message["archive"].(string)
	person, _ := message["person"].(string)
	other, err := jid.Parse(strings.TrimPrefix(person, "xmpp:"))
	if archive == "" || err != nil {
		return nil
	}
	created := as_int64(message["created"])
	own, _ := message["own"].(bool)
	from, to := other.Bare().String(), a.address.String()
	if own {
		from, to = to, from
	}
	id, _ := message["id"].(string)
	body, _ := message["body"].(string)
	reply, _ := message["reply"].(string)
	payload, _ := message["payload"].(string)
	if payload != "" {
		if _, err := xmpp_parse("<x>" + payload + "</x>"); err != nil {
			payload = ""
		}
	}
	files, _ := message["files"].([]any)
	// An encrypted message is given as it came, with the text clients that
	// cannot read it show, never as the account's device read it.
	if payload != "" {
		body, files = omemo_fallback, nil
	}
	var links []string
	var separate [][2]string
	for _, f := range files {
		file, _ := f.(map[string]any)
		address, _ := file["url"].(string)
		if !strings.HasPrefix(address, "https://") && !strings.HasPrefix(address, "http://") {
			continue
		}
		if part, _ := file["id"].(string); part != "" {
			separate = append(separate, [2]string{part, address})
		} else {
			links = append(links, address)
		}
	}
	text := func(id string, body string, extra string) string {
		inner := ""
		if body != "" {
			inner += `<body>` + xmpp_escape(body) + `</body>`
		}
		return xmpp_make("message", inner+extra, "from", from, "to", to, "type", "chat", "id", id).text(xmpp_ns_client)
	}
	var items []xmpp_entry
	if body != "" || payload != "" || len(links) > 0 || len(separate) == 0 {
		extra := payload
		for _, link := range links {
			extra += `<x xmlns="jabber:x:oob"><url>` + xmpp_escape(link) + `</url></x>`
		}
		if body == "" && len(links) == 1 {
			body = links[0]
		}
		if reply != "" {
			extra += `<reply xmlns="urn:xmpp:reply:0" id="` + xmpp_escape(reply) + `"/>`
		}
		if body == "" && payload == "" && len(links) == 0 {
			return nil
		}
		items = append(items, xmpp_entry{archive: archive, created: created, text: text(id, body, extra)})
	}
	for i, file := range separate {
		item := archive
		if len(items) > 0 || i > 0 {
			item = archive + "+" + strconv.Itoa(len(items))
		}
		items = append(items, xmpp_entry{archive: item, created: created, text: text(file[0], file[1], `<x xmlns="jabber:x:oob"><url>`+xmpp_escape(file[1])+`</url></x>`)})
	}
	return items
}

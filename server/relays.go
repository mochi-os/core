// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: groups relayed to XMPP
//
// A Mochi group chat that takes people from XMPP (the federation plan, phase
// 26, decision 39, as phase 24 relays groups to Matrix). The server of the
// member who added the first of them hosts a room for the group on its room
// service: rooms.<domain> of the server's own domain, else of that member's
// first hosted XMPP account's. Each Mochi member is an occupant of the room
// under their name, with no address and no account of their own; the people
// from XMPP are its members, invited by the room, and it is for members only.
// The relaying member's app tells the room what the Mochi members say through
// mochi.bridge, and hears through bridge/receive what the people from XMPP
// say. The room is not end-to-end encrypted: the relaying server reads each
// message to pass it on.
//
// A room is named by its address with the prefix chat keeps it under,
// xmpp:<room>, which tells mochi.bridge's functions it is one of these.

package main

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"mellium.im/xmpp/jid"
)

// relay_domain is the domain a user's server hosts relayed rooms under: its
// own, else that of the user's first hosted XMPP account, or "".
func relay_domain(user *User) string {
	if domain := server_domain(); domain != "" && xmpp_served(domain) {
		return domain
	}
	row, _ := networks_database().row("select domain from actors where user=? and network='xmpp' and deleted=0 order by created limit 1", user.UID)
	return row_string(row, "domain")
}

// relay_available reports whether a user's server can relay a group to XMPP.
func relay_available(user *User) bool {
	domain := relay_domain(user)
	return network_enabled("xmpp") && domain != "" && xmpp_room_base("rooms."+domain) != ""
}

// relay_room is a relayed room's conference by the name chat keeps it under,
// where the user relays it.
func relay_room(user *User, room string) (*conference, error) {
	b := bridge_of(room)
	if b == nil || b.user != user.UID {
		return nil, fmt.Errorf("no such relay")
	}
	c := conference_of(strings.TrimPrefix(room, "xmpp:"))
	if c == nil {
		return nil, fmt.Errorf("no such room")
	}
	return c, nil
}

// relay_person is someone from XMPP by an address as chat keeps it or as
// typed.
func relay_person(address string) (jid.JID, error) {
	person, err := xmpp_address(strings.TrimPrefix(strings.TrimSpace(address), "xmpp:"))
	if err != nil || person.Localpart() == "" {
		return jid.JID{}, fmt.Errorf("invalid address")
	}
	return person.Bare(), nil
}

// relay_create makes a room for a group: named, for members only, each
// address shown to all, the Mochi members given seated, and each person
// invited. It answers the room as chat keeps it and each person as {id, name}.
func relay_create(user *User, chat string, name string, people []string, members [][2]string) (map[string]any, error) {
	if !relay_available(user) {
		return nil, fmt.Errorf("this server has no XMPP room service to relay on")
	}
	var found []any
	var invited []jid.JID
	for _, address := range people {
		person, err := relay_person(address)
		if err != nil {
			return nil, err
		}
		if !strings.Contains(fmt.Sprint(invited), person.String()) {
			invited = append(invited, person)
			found = append(found, map[string]any{"id": "xmpp:" + person.String(), "name": person.Localpart()})
		}
	}
	address := strings.ToLower(uid()) + "@rooms." + relay_domain(user)
	room := "xmpp:" + address
	federation_database().exec("insert into bridges (room, user, standin, chat, created) values (?, ?, '', ?, ?) on conflict(room) do update set user=excluded.user, chat=excluded.chat", room, user.UID, chat, now())
	c := conference_make(address, name, jid.JID{})
	out := &conference_outbox{}
	c.lock.Lock()
	c.relay = user.UID
	c.seat(user.Identity.ID, user.Identity.Name, out)
	for _, member := range members {
		c.seat(member[0], member[1], out)
	}
	for _, person := range invited {
		c.invite_send(address, person, "", out)
	}
	c.lock.Unlock()
	out.flush()
	return map[string]any{"room": room, "people": found}, nil
}

// relay_invite invites someone from XMPP to a relayed room, a member of it
// from then on, and answers them as {id, name}.
func relay_invite(user *User, room string, address string) (map[string]any, error) {
	c, err := relay_room(user, room)
	if err != nil {
		return nil, err
	}
	person, err := relay_person(address)
	if err != nil {
		return nil, err
	}
	out := &conference_outbox{}
	c.lock.Lock()
	c.invite_send(c.address, person, "", out)
	c.lock.Unlock()
	out.flush()
	return map[string]any{"id": "xmpp:" + person.String(), "name": person.Localpart()}, nil
}

// relay_member seats a Mochi member in a relayed room, and answers the nick
// they go by there.
func relay_member(user *User, room string, entity string, name string) (string, error) {
	c, err := relay_room(user, room)
	if err != nil {
		return "", err
	}
	out := &conference_outbox{}
	c.lock.Lock()
	nick := c.seat(entity, name, out)
	c.lock.Unlock()
	out.flush()
	return nick, nil
}

// relay_leave takes a Mochi member out of a relayed room.
func relay_leave(user *User, room string, entity string) error {
	c, err := relay_room(user, room)
	if err != nil {
		return err
	}
	out := &conference_outbox{}
	c.lock.Lock()
	c.unseat(entity, out)
	c.lock.Unlock()
	out.flush()
	return nil
}

// relay_remove puts someone from XMPP out of a relayed room: no longer a
// member, and out of it where they are in it.
func relay_remove(user *User, room string, address string) error {
	c, err := relay_room(user, room)
	if err != nil {
		return err
	}
	person, err := relay_person(address)
	if err != nil {
		return err
	}
	out := &conference_outbox{}
	c.lock.Lock()
	c.affiliate(person.String(), "none", "")
	c.affiliated(person.String(), "none", out)
	c.lock.Unlock()
	out.flush()
	return nil
}

// relay_rename renames a relayed room, which everyone in it is told.
func relay_rename(user *User, room string, name string) error {
	c, err := relay_room(user, room)
	if err != nil {
		return err
	}
	out := &conference_outbox{}
	c.lock.Lock()
	c.config.name = conference_cut(name, 200)
	c.save()
	for _, o := range c.occupants {
		for session := range o.sessions {
			out.add(xmpp_make("message", `<x xmlns="`+xmpp_ns_muc_user+`"><status code="104"/></x>`, "type", "groupchat", "id", uid()), c.address, session)
		}
	}
	c.lock.Unlock()
	out.flush()
	return nil
}

// The ids messages were sent under by the key the app gave, for a day, so
// the same message is sent once.
var (
	relay_keys      = map[string]relay_key{}
	relay_keys_lock sync.Mutex
)

type relay_key struct {
	id   string
	when time.Time
}

// relay_send sends what a Mochi member does to a relayed room as them, the
// object as mochi.bridge.send takes it - its content, perhaps a reply, a file
// by the address its upload answered, or a reaction to a message - and
// answers the id the room gave it. A key sends it once.
func relay_send(user *User, room string, entity string, object map[string]any, key string) (string, error) {
	c, err := relay_room(user, room)
	if err != nil {
		return "", err
	}
	if key != "" {
		relay_keys_lock.Lock()
		kept, found := relay_keys[room+" "+key]
		for k, v := range relay_keys {
			if time.Since(v.when) > 24*time.Hour {
				delete(relay_keys, k)
			}
		}
		relay_keys_lock.Unlock()
		if found {
			return kept.id, nil
		}
	}
	inner := ""
	emoji, reacting := object["reaction"].(string)
	message, _ := object["message"].(string)
	if reacting {
		if message == "" {
			return "", fmt.Errorf("a reaction needs its message")
		}
		inner = xmpp_reactions(message, emoji)
	} else {
		text, _ := object["content"].(string)
		file := ""
		if f, _ := object["file"].(map[string]any); f != nil {
			file, _ = f["url"].(string)
		}
		if text == "" && file == "" {
			return "", fmt.Errorf("a message needs content or a file")
		}
		body := text
		if body == "" {
			body = file
		}
		inner = `<body>` + xmpp_escape(body) + `</body>`
		if file != "" {
			inner += `<x xmlns="jabber:x:oob"><url>` + xmpp_escape(file) + `</url></x>`
		}
		if reply, _ := object["reply"].(string); reply != "" {
			inner += `<reply xmlns="urn:xmpp:reply:0" id="` + xmpp_escape(reply) + `"/>`
		}
	}
	out := &conference_outbox{}
	c.lock.Lock()
	id, err := c.speak(entity, inner, out)
	c.lock.Unlock()
	out.flush()
	if err != nil {
		return "", err
	}
	// A reaction is known by the message it is to, all of a member's
	// reactions to it replaced at once.
	if reacting {
		id = xmpp_reaction_prefix + message
	}
	if key != "" {
		relay_keys_lock.Lock()
		relay_keys[room+" "+key] = relay_key{id: id, when: time.Now()}
		relay_keys_lock.Unlock()
	}
	return id, nil
}

// relay_update sends a new version of a message a Mochi member sent to a
// relayed room.
func relay_update(user *User, room string, entity string, id string, text string) error {
	c, err := relay_room(user, room)
	if err != nil {
		return err
	}
	if text == "" {
		return fmt.Errorf("a correction needs the new text")
	}
	out := &conference_outbox{}
	c.lock.Lock()
	_, err = c.speak(entity, `<body>`+xmpp_escape(text)+`</body><replace xmlns="urn:xmpp:message-correct:0" id="`+xmpp_escape(id)+`"/>`, out)
	c.lock.Unlock()
	out.flush()
	return err
}

// relay_delete withdraws a message a Mochi member sent to a relayed room, or
// their reaction to one.
func relay_delete(user *User, room string, entity string, id string) error {
	c, err := relay_room(user, room)
	if err != nil {
		return err
	}
	inner := `<retract xmlns="urn:xmpp:message-retract:1" id="` + xmpp_escape(id) + `"/><store xmlns="urn:xmpp:hints"/>`
	if message, found := strings.CutPrefix(id, xmpp_reaction_prefix); found {
		inner = xmpp_reactions(message, "")
	}
	out := &conference_outbox{}
	c.lock.Lock()
	_, err = c.speak(entity, inner, out)
	c.lock.Unlock()
	out.flush()
	return err
}

// relay_upload keeps a file for a relayed room's messages, on its domain, and
// answers {url, name, type, size}.
func relay_upload(user *User, room string, data []byte, kind string, name string) (map[string]any, error) {
	c, err := relay_room(user, room)
	if err != nil {
		return nil, err
	}
	_, service, _ := strings.Cut(c.address, "@")
	return xmpp_upload_store(user, "", strings.TrimPrefix(service, "rooms."), data, kind, name)
}

// relay_media fetches a file someone from XMPP sent a relayed room, by its
// address, as any page is fetched: its status, type and bytes.
func relay_media(user *User, room string, address string) (int, string, []byte, error) {
	if _, err := relay_room(user, room); err != nil {
		return 0, "", nil, err
	}
	request, err := http.NewRequest("GET", address, nil)
	if err != nil || (request.URL.Scheme != "https" && request.URL.Scheme != "http") {
		return 0, "", nil, fmt.Errorf("invalid url")
	}
	request.Header.Set("Accept", "*/*")
	response, err := network_request(request.Context(), user, "xmpp", request, 5)
	if err != nil {
		return 0, "", nil, err
	}
	defer response.Body.Close()
	data, err := read_limited(response.Body, int64(media_maximum))
	if err != nil {
		return 0, "", nil, err
	}
	return response.StatusCode, response.Header.Get("Content-Type"), data, nil
}

// relay_close stops relaying a room: it ends, and everyone in it is told.
func relay_close(user *User, room string) error {
	c, err := relay_room(user, room)
	if err != nil {
		return err
	}
	out := &conference_outbox{}
	c.lock.Lock()
	c.destroy("", out)
	c.lock.Unlock()
	out.flush()
	federation_database().exec("delete from bridges where room=?", room)
	return nil
}

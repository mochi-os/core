// Mochi: Chat app
// Copyright Alistair Cunningham 2024-2025

package main

type Chat struct {
	ID       string        `cbor:"id" json:"id"`
	Identity string        `cbor:"-" json:"-"`
	Name     string        `cbor:"name" json:"name"`
	Updated  int64         `cbor:"-" json:"-"`
	Members  *[]ChatMember `cbor:"members,omitempty" json:"-"`
}

type ChatMember struct {
	Chat   string `cbor:"chat" json:"chat"`
	Member string `cbor:"member" json:"member"`
	Name   string `cbor:"name" json:"name"`
}

type ChatMessage struct {
	ID          string        `cbor:"id" json:"id"`
	Chat        string        `cbor:"chat" json:"chat"`
	Member      string        `cbor:"member" json:"member"`
	Name        string        `cbor:"name" json:"name"`
	Body        string        `cbor:"body" json:"body"`
	Created     int64         `cbor:"created" json:"created"`
	Attachments *[]Attachment `cbor:"attachments,omitempty" json:"attachments,omitempty"`
}

func init() {
	a := app("chat")
	a.home("chat", map[string]string{"en": "Chat"})
	a.db("chat/chat.db", chat_db_create)

	a.path("chat", chat_list)
	a.path("chat/create", chat_create)
	a.path("chat/new", chat_new)
	a.path("chat/:chat", chat_view)
	a.path("chat/:chat/:name", chat_view)
	a.path("chat/:chat/messages", chat_messages)
	a.path("chat/:chat/send", chat_message_send)

	a.service("chat")
	a.event("message", chat_message_event)
	a.event("new", chat_new_event)
}

// Create app database
func chat_db_create(db *DB) {
	db.exec("create table settings ( name text not null primary key, value text not null )")
	db.exec("replace into settings ( name, value ) values ( 'schema', 1 )")

	db.exec("create table chats ( id text not null primary key, identity text not null, name text not null, updated integer not null )")
	db.exec("create index chats_updated on chats( updated )")

	db.exec("create table members ( chat references chats( id ), member text not null, name text not null, primary key ( chat, member ) )")

	db.exec("create table messages ( id text not null primary key, chat references chats( id ), member text not null, name text not null, body text not null, created integer not null )")
	db.exec("create index messages_chat_created on messages( chat, created )")
}

// Get details of a chat
func chat_by_id(db *DB, chat string) *Chat {
	var c Chat
	if db.scan(&c, "select * from chats where id=?", chat) {
		return &c
	}
	return nil
}

// Create chat
func chat_create(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	chat := uid()
	name := a.input("name")
	if !valid(name, "name") {
		a.error(400, "Invalid name")
		return
	}

	a.user.db.exec("replace into chats ( id, identity, name, updated ) values ( ?, ?, ?, ? )", chat, a.user.Identity.ID, name, now())
	a.user.db.exec("replace into members ( chat, member, name ) values ( ?, ?, ? )", chat, a.user.Identity.ID, a.user.Identity.Name)
	members := []ChatMember{ChatMember{Chat: chat, Member: a.user.Identity.ID, Name: a.user.Identity.Name}}

	for _, f := range *friends(a.user) {
		if a.input(f.ID) != "" {
			a.user.db.exec("replace into members ( chat, member, name ) values ( ?, ?, ? )", chat, f.ID, f.Name)
			members = append(members, ChatMember{Chat: chat, Member: f.ID, Name: f.Name})
		}
	}

	for _, m := range members {
		if m.Member == a.user.Identity.ID {
			continue
		}
		message(a.user.Identity.ID, m.Member, "chat", "new").set("id", chat, "name", name).add(members).send()
	}

	a.redirect("/chat/" + chat)
}

// List existing chats
func chat_list(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	var cs []Chat
	a.user.db.scans(&cs, "select * from chats order by updated desc")

	a.write("chat/list", cs)
}

// Get details of a chat member
func chat_member(db *DB, chat string, member string) *ChatMember {
	var m ChatMember
	if db.scan(&m, "select * from members where chat=? and member=?", chat, member) {
		return &m
	}
	return nil
}

// Received a message event from another member
func chat_message_event(e *Event) {
	chat := e.get("chat", "")
	c := chat_by_id(e.db, chat)
	if c == nil {
		info("Chat dropping message to unknown chat '%s'", chat)
		return
	}

	m := chat_member(e.db, chat, e.from)
	if m == nil {
		info("Chat dropping message from unknown member '%s'", e.from)
		return
	}

	message := e.get("message", "")
	if !valid(message, "id") {
		info("Chat dropping message with invalid ID '%s'", message)
		return
	}

	body := e.get("body", "")
	if !valid(body, "text") {
		info("Chat dropping message with invalid body '%s'", body)
		return
	}

	e.db.exec("replace into messages ( id, chat, member, name, body, created ) values ( ?, ?, ?, ?, ?, ? )", message, chat, e.from, m.Name, body, now())

	var as []Attachment
	if e.decode(&as) {
		attachments_save(&as, e.user, e.from, "chat/%s/%s", chat, message)
	}

	websockets_send(e.user, "chat", json_encode(Map{"name": m.Name, "body": body, "attachments": &as}))
	notification(e.user, "chat", "message", chat, m.Name+": "+body, "/chat/"+chat)
}

// Send previous messages to client
func chat_messages(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	c := chat_by_id(a.user.db, a.input("chat"))
	if c == nil {
		a.error(404, "Chat not found")
		return
	}

	var ms []ChatMessage
	a.user.db.scans(&ms, "select * from messages where chat=? order by id", c.ID)

	for i, m := range ms {
		ms[i].Attachments = attachments(a.user, "chat/%s/%s", c.ID, m.ID)
		debug("Attachments for '%s': %+v", m.ID, ms[i].Attachments)
	}

	a.json(ms)
}

// Send a chat message
func chat_message_send(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	c := chat_by_id(a.user.db, a.input("chat"))
	if c == nil {
		a.error(404, "Chat not found")
		return
	}

	id := uid()
	body := a.input("body")
	a.user.db.exec("replace into messages ( id, chat, member, name, body, created ) values ( ?, ?, ?, ?, ?, ? )", id, c.ID, a.user.Identity.ID, a.user.Identity.Name, body, now())

	attachments := a.upload_attachments("attachments", a.user.Identity.ID, true, "chat/%s/%s", c.ID, id)

	var ms []ChatMember
	a.user.db.scans(&ms, "select * from members where chat=? and member!=?", c.ID, a.user.Identity.ID)
	for _, m := range ms {
		message(a.user.Identity.ID, m.Member, "chat", "message").set("chat", c.ID, "message", id, "body", body).add(attachments).send()
	}

	websockets_send(a.user, "chat", json_encode(Map{"name": a.user.Identity.Name, "body": body, "attachments": attachments}))
}

// Ask user who they'd like to chat with
func chat_new(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	a.write("chat/new", Map{"Friends": friends(a.user), "Name": a.user.Identity.Name})
}

// Received a new chat event from a friend
func chat_new_event(e *Event) {
	f := friend(e.user, e.from)
	if f == nil {
		info("Chat dropping new chat from unknown friend '%s'", e.from)
		return
	}

	chat := e.get("id", "")
	if !valid(chat, "id") {
		info("Chat dropping new chat with invalid ID '%s'", chat)
		return
	}

	c := chat_by_id(e.db, chat)
	if c != nil {
		info("Chat dropping duplicate new chat '%s'", chat)
		return
	}

	name := e.get("name", "")
	if !valid(name, "name") {
		info("Chat dropping new chat with invalid name '%s'", name)
		return
	}

	e.db.exec("replace into chats ( id, identity, name, updated ) values ( ?, ?, ?, ? )", chat, e.to, name, now())

	var ms []ChatMember
	if e.decode(&ms) {
		for _, cm := range ms {
			if !valid(cm.Member, "entity") {
				info("Chat dropping member with invalid ID '%s'", cm.Member)
				continue
			}

			if !valid(cm.Name, "name") {
				info("Chat dropping member with invalid name '%s'", cm.Name)
				continue
			}

			e.db.exec("replace into members ( chat, member, name ) values ( ?, ?, ? )", chat, cm.Member, cm.Name)
		}
	}

	notification(e.user, "chat", "new", chat, "New chat from "+f.Name+": "+c.Name, "/chat/"+c.ID)
}

// View a chat
func chat_view(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	c := chat_by_id(a.user.db, a.input("chat"))
	if c == nil {
		a.error(404, "Chat not found")
		return
	}

	notifications_clear_object(a.user, "chat", c.ID)
	a.write("chat/view", Map{"Chat": c})
}

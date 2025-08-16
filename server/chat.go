// Mochi: Chat app
// Copyright Alistair Cunningham 2024-2025

package main

type Chat struct {
	ID       string
	Identity string `cbor:"-"`
	Name     string
	Updated  int64 `cbor:"-"`
	Members  *[]ChatMember
}

type ChatMember struct {
	Chat   string
	Member string
	Name   string
}

type ChatMessage struct {
	ID          string
	Chat        string
	Time        int64
	Author      string
	Name        string
	Body        string
	Attachments *[]Attachment `cbor:",omitempty"`
}

func init() {
	a := app("chat")
	a.home("chat", map[string]string{"en": "Chat"})
	a.db("chat.db", chat_db_create)

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

	db.exec("create table messages ( id text not null primary key, chat references chats( id ), time integer not null, author text not null, name text not null, body text not null )")
	db.exec("create index messages_chat_time on messages( chat, time )")
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

	log_debug("Creating chat '%s' with name '%s'", chat, name)
	a.db.exec("replace into chats ( id, identity, name, updated ) values ( ?, ?, ?, ? )", chat, a.user.Identity.ID, name, now())
	a.db.exec("replace into members ( chat, member, name ) values ( ?, ?, ? )", chat, a.user.Identity.ID, a.user.Identity.Name)
	members := []ChatMember{ChatMember{Chat: chat, Member: a.user.Identity.ID, Name: a.user.Identity.Name}}

	for _, f := range *friends(a.user) {
		if a.input(f.ID) != "" {
			log_debug("Adding %s (%s) to new chat", f.ID, f.Name)
			a.db.exec("replace into members ( chat, member, name ) values ( ?, ?, ? )", chat, f.ID, f.Name)
			members = append(members, ChatMember{Chat: chat, Member: f.ID, Name: f.Name})
		}
	}

	for _, m := range members {
		if m.Member == a.user.Identity.ID {
			continue
		}
		log_debug("Chat sending new chat to '%s' (%s)", m.Member, m.Name)
		ev := event(a.user.Identity.ID, m.Member, "chat", "new")
		ev.set("id", chat, "name", name)
		ev.add(members)
		ev.send()
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
	a.db.scans(&cs, "select * from chats order by updated desc")

	a.template("chat/list", cs)
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
		log_info("Chat dropping message to unknown chat '%s'", chat)
		return
	}

	m := chat_member(e.db, chat, e.From)
	if m == nil {
		log_info("Chat dropping message from unknown member '%s'", e.From)
		return
	}

	body := e.get("body", "")
	if !valid(body, "text") {
		log_info("Chat dropping message with invalid body '%s'", body)
		return
	}

	e.db.exec("replace into messages ( id, chat, time, author, name, body ) values ( ?, ?, ?, ?, ?, ? )", e.ID, chat, now(), e.From, m.Name, body)

	var as []Attachment
	if e.decode(&as) {
		log_debug("Chat recieved attachments %v", as)
		attachments_save(&as, e.user, e.From, "chat/%s/%s", chat, e.ID)
	}

	websockets_send(e.user, "chat", json_encode(Map{"Name": m.Name, "Body": body, "Attachments": &as}))
	notification(e.user, "chat", "message", c.ID, m.Name+": "+body, "/chat/"+c.ID)
}

// Send previous messages to client
func chat_messages(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	c := chat_by_id(a.db, a.input("chat"))
	if c == nil {
		a.error(404, "Chat not found")
		return
	}

	var ms []ChatMessage
	a.db.scans(&ms, "select * from messages where chat=? order by id", c.ID)

	for i, m := range ms {
		ms[i].Attachments = attachments(a.user, "chat/%s/%s", c.ID, m.ID)
		log_debug("Attachments found for 'chat/%s/%s': %d", c.ID, m.ID, len(*ms[i].Attachments))
	}

	a.json(ms)
}

// Send a chat message
func chat_message_send(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	c := chat_by_id(a.db, a.input("chat"))
	if c == nil {
		a.error(404, "Chat not found")
		return
	}

	id := uid()
	body := a.input("body")
	log_debug("Chat sending message '%s'", body)
	a.db.exec("replace into messages ( id, chat, time, author, name, body ) values ( ?, ?, ?, ?, ?, ? )", id, c.ID, now(), a.user.Identity.ID, a.user.Identity.Name, body)

	attachments := a.upload_attachments("attachments", a.user.Identity.ID, true, "chat/%s/%s", c.ID, id)

	var ms []ChatMember
	a.db.scans(&ms, "select * from members where chat=? and member!=?", c.ID, a.user.Identity.ID)
	for _, m := range ms {
		log_debug("Sending chat message to '%s' (%s)", m.Member, m.Name)
		ev := event(a.user.Identity.ID, m.Member, "chat", "message")
		ev.set("chat", c.ID, "body", body)
		ev.add(attachments)
		ev.send()
	}

	websockets_send(a.user, "chat", json_encode(Map{"Name": a.user.Identity.Name, "Body": body, "Attachments": attachments}))
}

// Ask user who they'd like to chat with
func chat_new(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	a.template("chat/new", Map{"Friends": friends(a.user), "Name": a.user.Identity.Name})
}

// Received a new chat event from a friend
func chat_new_event(e *Event) {
	f := friend(e.user, e.From)
	if f == nil {
		log_info("Chat dropping new chat from unknown friend '%s'", e.From)
		return
	}

	chat := e.get("chat", "")
	if !valid(chat, "uid") {
		log_info("Chat dropping new chat with invalid ID '%s'", chat)
		return
	}

	c := chat_by_id(e.db, chat)
	if c != nil {
		log_info("Chat dropping duplicate new chat '%s'", chat)
		return
	}

	name := e.get("name", "")
	if !valid(name, "name") {
		log_info("Chat dropping new chat with invalid name '%s'", name)
		return
	}

	e.db.exec("replace into chats ( id, identity, name, updated ) values ( ?, ?, ?, ? )", chat, e.To, name, now())

	var ms []ChatMember
	if e.decode(&ms) {
		for _, cm := range ms {
			if !valid(cm.Member, "entity") {
				log_info("Chat dropping member with invalid ID '%s'", cm.Member)
				continue
			}

			if !valid(cm.Name, "name") {
				log_info("Chat dropping member with invalid name '%s'", cm.Name)
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

	c := chat_by_id(a.db, a.input("chat"))
	if c == nil {
		a.error(404, "Chat not found")
		return
	}

	notifications_clear_object(a.user, "chat", c.ID)
	a.template("chat/view", Map{"Chat": c})
}

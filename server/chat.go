// Comms: Chat app
// Copyright Alistair Cunningham 2024-2025

package main

type Chat struct {
	ID       string
	Identity string
	Name     string
	Updated  int64
}

type ChatMember struct {
	Chat   string
	Member string
	Name   string
	Role   string
}

type ChatMessage struct {
	ID     string
	Chat   string
	Time   int64
	Author string
	Name   string
	Body   string
}

func init() {
	a := app("chat")
	a.home("chat", map[string]string{"en": "Chat"})
	a.db("db/chat.db", chat_db_create)
	a.entity("chat")

	a.path("chat", chat_list)
	a.path("chat/create", chat_create)
	a.path("chat/new", chat_new)
	a.path("chat/:chat", chat_view)
	a.path("chat/:chat/messages", chat_messages)
	a.path("chat/:chat/send", chat_send)

	a.service("chat")
	a.event("message", chat_receive)
}

// Create app database
func chat_db_create(db *DB) {
	db.exec("create table settings ( name text not null primary key, value text not null )")
	db.exec("replace into settings ( name, value ) values ( 'schema', 1 )")

	db.exec("create table chats ( id text not null primary key, identity text not null, name text not null, updated integer not null )")
	db.exec("create index chats_updated on chats( updated )")

	db.exec("create table members ( chat references chats( id ), member text not null, name text not null, role text not null default 'admin', primary key ( chat, member ) )")

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
	a.db.exec("replace into members ( chat, member, name, role ) values ( ?, ?, ?, 'administrator' )", chat, a.user.Identity.ID, a.user.Identity.Name)

	for _, f := range *friends(a.user) {
		if a.input(f.ID) != "" {
			log_debug("Adding %s (%s) to new chat", f.ID, f.Name)
			a.db.exec("replace into members ( chat, member, name, role ) values ( ?, ?, ?, 'talker' )", chat, f.ID, f.Name)
		}
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
	if db.scan("select * from members where chat=? and member=?", chat, member) {
		return &m
	}
	return nil
}

// Send list of messages to client
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

	var m []ChatMessage
	a.db.scans(&m, "select * from messages where chat=? order by id", c.ID)
	a.json(m)
}

// Ask user who they'd like to chat with
func chat_new(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	a.template("chat/new", Map{"Friends": friends(a.user), "Name": a.user.Identity.Name})
}

// Received a chat event from another user
func chat_receive(e *Event) {
	var cm ChatMessage
	if !json_decode(&cm, e.Content) {
		log_info("Chat dropping message '%s' with malformed JSON", e.Content)
		return
	}

	c := chat_by_id(e.db, cm.Chat)
	if c == nil {
		log_info("Chat dropping message to unknown chat '%s'", cm.Chat)
		return
	}

	m := chat_member(e.db, cm.Chat, e.From)
	if m == nil {
		log_info("Chat dropping message from unknown member '%s'", e.From)
		return
	}

	if !valid(cm.Body, "text") {
		log_info("Chat dropping message with invalid body '%s'", cm.Body)
		return
	}

	e.db.exec("replace into messages ( id, chat, time, author, author, body ) values ( ?, ?, ?, ?, ?, ? )", e.ID, c.ID, now(), e.From, m.Name, cm.Body)
	websockets_send(e.user, "chat", json_encode(map[string]string{"from": e.From, "name": m.Name, "time": now_string(), "body": cm.Body}))
	notification(e.user, "chat", "message", c.ID, m.Name+": "+cm.Body, "/chat/"+c.ID)
}

// Send a chat message
func chat_send(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	c := chat_by_id(a.db, a.input("chat"))

	message := a.input("message")
	a.db.exec("replace into messages ( id, chat, time, author, name, body ) values ( ?, ?, ?, ?, ?, ? )", uid(), c.ID, now_string(), a.user.Identity.ID, a.user.Identity.Name, message)
	event := Event{ID: uid(), From: a.user.Identity.ID, To: c.ID, Service: "chat", Action: "message", Content: json_encode(map[string]string{"body": message})}
	event.send()

	j := json_encode(map[string]string{"from": a.user.Identity.ID, "name": a.user.Identity.Name, "time": now_string(), "body": message})
	websockets_send(a.user, "chat", j)
}

// View a chat
func chat_view(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	c := chat_by_id(a.db, a.input("chat"))
	notifications_clear_entity(a.user, "chat", c.ID)
	a.template("chat/view", Map{"Chat": c})
}

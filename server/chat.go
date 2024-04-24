// Comms: Chat app
// Copyright Alistair Cunningham 2024

package main

import (
	"net/http"
)

type Chat struct {
	ID      string
	Name    string
	Friend  string
	Updated int64
}

type ChatMember struct {
	Chat   string
	Member string
	Role   string
}

type ChatMessage struct {
	ID     string `json:"id"`
	Chat   string `json:"chat"`
	Time   int64  `json:"time"`
	Sender string `json:"from"`
	Name   string `json:"name"`
	Body   string `json:"body"`
}

func init() {
	register_app("chat")
	register_home("chat", "chat", map[string]string{"en": "Chat"})
	register_action("chat", "chat", chat_list, true)
	register_action("chat", "chat/messages", chat_messages, true)
	register_action("chat", "chat/new", chat_new, true)
	register_action("chat", "chat/send", chat_send, true)
	register_action("chat", "chat/view", chat_view, true)
	register_event("chat", "message", chat_receive)
}

// Create app database
func chat_db_create(db string) {
	db_exec(db, "create table chats ( id text not null primary key, name text not null, friend text not null default '', updated integer not null )")
	db_exec(db, "create index chats_friend on chats( friend )")
	db_exec(db, "create index chats_updated on chats( updated )")
	db_exec(db, "create table members ( chat references chats( id ), member text not null, role text not null default 'talker' )")
	db_exec(db, "create table messages ( id text not null primary key, chat references chats( id ), time integer not null, sender text not null, name text not null, body text not null )")
	db_exec(db, "create index messages_chat_time on messages( chat, time )")
}

// Find best chat for friend
func chat_for_friend(u *User, f *Friend) *Chat {
	var c Chat
	db := db_app(u, "chat", "data.db", chat_db_create)
	if db_struct(&c, db, "select * from chats where friend=? order by updated desc", f.ID) {
		db_exec(db, "update chats set updated=? where id=?", time_unix_string(), c.ID)
	} else {
		c = Chat{ID: uid(), Name: f.Name, Friend: f.ID, Updated: time_unix()}
		db_exec(db, "replace into chats ( id, name, friend, updated ) values ( ?, ?, ?, ? )", c.ID, c.Name, c.Friend, c.Updated)
	}
	return &c
}

// List existing chats
func chat_list(u *User, w http.ResponseWriter, r *http.Request) {
	var chats []Chat
	db := db_app(u, "chat", "data.db", chat_db_create)
	db_structs(&chats, db, "select * from chats order by updated desc")
	app_write(w, r.FormValue("format"), "chat/list", chats)
}

// Received a chat event from another user
func chat_receive(u *User, e *Event) {
	db := db_app(u, "chat", "data.db", chat_db_create)

	var m map[string]string
	if !json_decode([]byte(e.Content), &m) {
		log_info("Chat dropping chat message '%s' with malformed JSON", e.Content)
		return
	}
	body, found := m["body"]
	if !found {
		log_info("Chat dropping chat message '%s' without body", e.Content)
		return
	}

	f := friend(u, e.From)
	if f == nil {
		// Event from unkown sender. Send them an error reply and drop their message.
		event(u, e.From, "chat", "", "message", "The person you have contacted has not yet added you as a friend, so your message has not been delivered.")
		return
	}
	c := chat_for_friend(u, f)

	db_exec(db, "replace into messages ( id, chat, time, sender, name, body ) values ( ?, ?, ?, ?, ?, ? )", uid(), c.ID, time_unix_string(), e.From, f.Name, body)
	j := json_encode(map[string]string{"from": e.From, "name": f.Name, "time": time_unix_string(), "body": body})
	websockets_send(u, "chat", j)
	notification_create(u, "chat", "message", c.ID, f.Name+": "+body, "/chat/view/?friend="+f.ID)
}

// Send list of messages to client
func chat_messages(u *User, w http.ResponseWriter, r *http.Request) {
	db := db_app(u, "chat", "data.db", chat_db_create)
	f := friend(u, r.FormValue("friend"))
	if f == nil {
		app_error(w, 404, "Friend not found")
		return
	}
	c := chat_for_friend(u, f)

	var m []ChatMessage
	db_structs(&m, db, "select * from messages where chat=? order by id", c.ID)
	app_write(w, "json", "", m)
}

// Ask user who they'd like to chat with
func chat_new(u *User, w http.ResponseWriter, r *http.Request) {
	app_write(w, "html", "chat/list", friends(u))
}

// Send a chat message
func chat_send(u *User, w http.ResponseWriter, r *http.Request) {
	db := db_app(u, "chat", "data.db", chat_db_create)

	f := friend(u, r.FormValue("friend"))
	if f == nil {
		app_error(w, 404, "Friend not found")
		return
	}
	c := chat_for_friend(u, f)

	message := r.FormValue("message")
	db_exec(db, "replace into messages ( id, chat, time, sender, name, body ) values ( ?, ?, ?, ?, ?, ? )", uid(), c.ID, time_unix_string(), u.Public, u.Name, message)
	event(u, f.ID, "chat", "", "message", json_encode(map[string]string{"body": message}))
	j := json_encode(map[string]string{"from": u.Public, "name": u.Name, "time": time_unix_string(), "body": message})
	websockets_send(u, "chat", j)
}

// View a chat
func chat_view(u *User, w http.ResponseWriter, r *http.Request) {
	f := friend(u, r.FormValue("friend"))
	if f == nil {
		app_error(w, 404, "Friend not found")
		return
	}
	c := chat_for_friend(u, f)
	notifications_clear_entity(u, "chat", c.ID)
	app_write(w, "html", "chat/view", c)
}

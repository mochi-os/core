// Comms: Chat app
// Copyright Alistair Cunningham 2024

package main

type Chat struct {
	ID      string
	Name    string
	Friend  string
	Master  string
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
	a := register_app("chat")
	a.register_home("chat", map[string]string{"en": "Chat"})
	a.register_action("chat", chat_list, true)
	a.register_action("chat/messages", chat_messages, true)
	a.register_action("chat/new", chat_new, true)
	a.register_action("chat/send", chat_send, true)
	a.register_action("chat/view", chat_view, true)
	a.register_event("message", chat_receive)
}

// Create app database
func chat_db_create(db *DB) {
	db.exec("create table settings ( name text not null primary key, value text not null )")
	db.exec("replace into settings ( name, value ) values ( 'schema', 1 )")

	db.exec("create table chats ( id text not null primary key, name text not null, friend text not null default '', master text not null default '', updated integer not null )")
	db.exec("create index chats_friend on chats( friend )")
	db.exec("create index chats_updated on chats( updated )")

	db.exec("create table members ( chat references chats( id ), member text not null, role text not null default 'talker' )")

	db.exec("create table messages ( id text not null primary key, chat references chats( id ), time integer not null, sender text not null, name text not null, body text not null )")
	db.exec("create index messages_chat_time on messages( chat, time )")
}

// Find best chat for friend
func chat_for_friend(u *User, f *Friend) *Chat {
	var c Chat
	db := db_app(u, "chat", "data.db", chat_db_create)
	if db.scan(&c, "select * from chats where friend=? order by updated desc", f.ID) {
		db.exec("update chats set updated=? where id=?", time_unix_string(), c.ID)
	} else {
		c = Chat{ID: uid(), Name: f.Name, Friend: f.ID, Updated: time_unix()}
		db.exec("replace into chats ( id, name, friend, updated ) values ( ?, ?, ?, ? )", c.ID, c.Name, c.Friend, c.Updated)
	}
	return &c
}

// List existing chats
func chat_list(u *User, a *Action) {
	var chats []Chat
	db := db_app(u, "chat", "data.db", chat_db_create)
	db.scans(&chats, "select * from chats order by updated desc")
	a.write_format(a.input("format"), "chat/list", chats)
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

	db.exec("replace into messages ( id, chat, time, sender, name, body ) values ( ?, ?, ?, ?, ?, ? )", uid(), c.ID, time_unix_string(), e.From, f.Name, body)
	j := json_encode(map[string]string{"from": e.From, "name": f.Name, "time": time_unix_string(), "body": body})
	websockets_send(u, "chat", j)
	notification_create(u, "chat", "message", c.ID, f.Name+": "+body, "/chat/view/?friend="+f.ID)
}

// Send list of messages to client
func chat_messages(u *User, a *Action) {
	db := db_app(u, "chat", "data.db", chat_db_create)
	f := friend(u, a.input("friend"))
	if f == nil {
		a.error(404, "Friend not found")
		return
	}
	c := chat_for_friend(u, f)

	var m []ChatMessage
	db.scans(&m, "select * from messages where chat=? order by id", c.ID)
	a.write_json(m)
}

// Ask user who they'd like to chat with
func chat_new(u *User, a *Action) {
	a.write_template("chat/list", friends(u))
}

// Send a chat message
func chat_send(u *User, a *Action) {
	db := db_app(u, "chat", "data.db", chat_db_create)

	f := friend(u, a.input("friend"))
	if f == nil {
		a.error(404, "Friend not found")
		return
	}
	c := chat_for_friend(u, f)

	message := a.input("message")
	db.exec("replace into messages ( id, chat, time, sender, name, body ) values ( ?, ?, ?, ?, ?, ? )", uid(), c.ID, time_unix_string(), u.Public, u.Name, message)
	event(u, f.ID, "chat", "", "message", json_encode(map[string]string{"body": message}))
	j := json_encode(map[string]string{"from": u.Public, "name": u.Name, "time": time_unix_string(), "body": message})
	websockets_send(u, "chat", j)
}

// View a chat
func chat_view(u *User, a *Action) {
	f := friend(u, a.input("friend"))
	if f == nil {
		a.error(404, "Friend not found")
		return
	}
	c := chat_for_friend(u, f)
	notifications_clear_entity(u, "chat", c.ID)
	a.write_template("chat/view", c)
}

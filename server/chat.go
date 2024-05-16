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
	Sender string `json:"sender"`
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
	db := db_app(u, "chat", "data.db", chat_db_create)
	var c Chat
	if db.scan(&c, "select * from chats where friend=? order by updated desc", f.ID) {
		db.exec("update chats set updated=? where id=?", now_string(), c.ID)
	} else {
		c = Chat{ID: uid(), Name: f.Name, Friend: f.ID, Updated: now()}
		db.exec("replace into chats ( id, name, friend, updated ) values ( ?, ?, ?, ? )", c.ID, c.Name, c.Friend, c.Updated)
	}
	return &c
}

// List existing chats
func chat_list(u *User, a *Action) {
	db := db_app(u, "chat", "data.db", chat_db_create)
	defer db.close()

	var c []Chat
	db.scans(&c, "select * from chats order by updated desc")
	a.write(a.input("format"), "chat/list", c)
}

// Send list of messages to client
func chat_messages(u *User, a *Action) {
	db := db_app(u, "chat", "data.db", chat_db_create)
	defer db.close()

	f := friend(u, a.input("friend"))
	if f == nil {
		a.error(404, "Friend not found")
		return
	}
	c := chat_for_friend(u, f)

	var m []ChatMessage
	db.scans(&m, "select * from messages where chat=? order by id", c.ID)
	a.json(m)
}

// Ask user who they'd like to chat with
func chat_new(u *User, a *Action) {
	a.template("chat/new", friends(u))
}

// Received a chat event from another user
func chat_receive(u *User, e *Event) {
	db := db_app(u, "chat", "data.db", chat_db_create)

	var m map[string]string
	if !json_decode(&m, e.Content) {
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
		event := Event{ID: uid(), From: u.Identity.ID, To: e.From, App: "chat", Action: "message", Content: `{"body": "The person you have contacted has not yet added you as a friend, so your message has not been delivered."}`}
		event.send()
		return
	}
	c := chat_for_friend(u, f)

	db.exec("replace into messages ( id, chat, time, sender, name, body ) values ( ?, ?, ?, ?, ?, ? )", uid(), c.ID, now_string(), e.From, f.Name, body)
	j := json_encode(map[string]string{"from": e.From, "name": f.Name, "time": now_string(), "body": body})
	websockets_send(u, "chat", j)
	notification_create(u, "chat", "message", c.ID, f.Name+": "+body, "/chat/view/?friend="+f.ID)
}

// Send a chat message
func chat_send(u *User, a *Action) {
	db := db_app(u, "chat", "data.db", chat_db_create)
	defer db.close()

	f := friend(u, a.input("friend"))
	if f == nil {
		a.error(404, "Friend not found")
		return
	}
	c := chat_for_friend(u, f)

	i := u.identity()
	if i == nil {
		a.error(500, "User has no identity")
		return
	}

	message := a.input("message")
	db.exec("replace into messages ( id, chat, time, sender, name, body ) values ( ?, ?, ?, ?, ?, ? )", uid(), c.ID, now_string(), i.ID, i.Name, message)
	event := Event{ID: uid(), From: i.ID, To: f.ID, App: "chat", Action: "message", Content: json_encode(map[string]string{"body": message})}
	event.send()

	j := json_encode(map[string]string{"from": i.ID, "name": i.Name, "time": now_string(), "body": message})
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
	a.template("chat/view", c)
}

// Comms: Chat app
// Copyright Alistair Cunningham 2024-2025

package main

type Chat struct {
	ID       string
	Identity string
	Name     string
	Friend   string
	Master   string
	Updated  int64
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
	a := app("chat")
	a.home("chat", map[string]string{"en": "Chat"})
	a.db("db/chat.db", chat_db_create)

	a.path("chat", chat_list)
	a.path("chat/messages", chat_messages)
	a.path("chat/new", chat_new)
	a.path("chat/send", chat_send)
	a.path("chat/:entity", chat_view)

	a.service("chat")
	a.event("message", chat_receive)
}

// Create app database
func chat_db_create(db *DB) {
	db.exec("create table settings ( name text not null primary key, value text not null )")
	db.exec("replace into settings ( name, value ) values ( 'schema', 1 )")

	db.exec("create table chats ( id text not null primary key, identity text not null, name text not null, friend text not null default '', master text not null default '', updated integer not null )")
	db.exec("create index chats_friend on chats( friend )")
	db.exec("create index chats_updated on chats( updated )")

	db.exec("create table members ( chat references chats( id ), member text not null, role text not null default 'talker' )")

	db.exec("create table messages ( id text not null primary key, chat references chats( id ), time integer not null, sender text not null, name text not null, body text not null )")
	db.exec("create index messages_chat_time on messages( chat, time )")
}

// Find best chat for friend
func chat_for_friend(u *User, db *DB, f *Friend) *Chat {
	var c Chat
	if db.scan(&c, "select * from chats where friend=? order by updated desc", f.ID) {
		db.exec("update chats set updated=? where id=?", now_string(), c.ID)
	} else {
		c = Chat{ID: uid(), Identity: u.Identity.ID, Name: f.Name, Friend: f.ID, Updated: now()}
		db.exec("replace into chats ( id, identity, name, friend, updated ) values ( ?, ?, ?, ?, ? )", c.ID, c.Identity, c.Name, c.Friend, c.Updated)
	}
	return &c
}

// List existing chats
func chat_list(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	var c []Chat
	a.db.scans(&c, "select * from chats order by updated desc")
	a.write(a.input("format"), "chat/list", c)
}

// Send list of messages to client
func chat_messages(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	f := friend(a.user, a.input("friend"))
	if f == nil {
		a.error(404, "Friend not found")
		return
	}
	c := chat_for_friend(a.user, a.db, f)

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

	a.template("chat/new", friends(a.user))
}

// Received a chat event from another user
func chat_receive(e *Event) {
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

	f := friend(e.user, e.From)
	if f == nil {
		// Event from unknown sender. Send them an error reply and drop their message.
		event := Event{ID: uid(), From: e.user.Identity.ID, To: e.From, Service: "chat", Action: "message", Content: `{"body": "The person you have contacted has not yet added you as a friend, so your message has not been delivered."}`}
		event.send()
		return
	}
	c := chat_for_friend(e.user, e.db, f)

	e.db.exec("replace into messages ( id, chat, time, sender, name, body ) values ( ?, ?, ?, ?, ?, ? )", uid(), c.ID, now_string(), e.From, f.Name, body)
	j := json_encode(map[string]string{"from": e.From, "name": f.Name, "time": now_string(), "body": body})
	websockets_send(e.user, "chat", j)
	notification(e.user, "chat", "message", c.ID, f.Name+": "+body, "/chat/"+f.ID)
}

// Send a chat message
func chat_send(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	f := friend(a.user, a.input("friend"))
	if f == nil {
		a.error(404, "Friend not found")
		return
	}
	c := chat_for_friend(a.user, a.db, f)

	message := a.input("message")
	a.db.exec("replace into messages ( id, chat, time, sender, name, body ) values ( ?, ?, ?, ?, ?, ? )", uid(), c.ID, now_string(), a.user.Identity.ID, a.user.Identity.Name, message)
	event := Event{ID: uid(), From: a.user.Identity.ID, To: f.ID, Service: "chat", Action: "message", Content: json_encode(map[string]string{"body": message})}
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

	f := friend(a.user, a.id())
	if f == nil {
		a.error(404, "Friend not found")
		return
	}
	c := chat_for_friend(a.user, a.db, f)
	notifications_clear_entity(a.user, "chat", c.ID)
	a.template("chat/view", c)
}

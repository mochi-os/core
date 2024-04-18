// Comms sample internal app: Chat
// Copyright Alistair Cunningham 2024

package main

func init() {
	app_register("chat", map[string]string{"en": "Chat"})
	app_register_action("chat", "", chat_list)
	app_register_action("chat", "list", chat_list)
	app_register_action("chat", "new", chat_new)
	app_register_action("chat", "view", chat_view)
	app_register_event("chat", "message", chat_message_receive)
	app_register_path("chat", "chat")
}

// List existing chats
func chat_list(u *User, action string, format string, p app_parameters) string {
	return app_template("chat/"+format+"/list", objects_by_category(u, "chat", "friend", "updated desc"))
}

// Received a chat event from another user
func chat_message_receive(u *User, e *Event) {
	//TODO Fix pointers
	fp, _ := service_generic[*Object](u, "friends", "get", e.From)
	f := *fp
	if f == nil {
		// Event from unkown sender. Send them an error reply and drop their message.
		event(u, e.From, "chat", "", "message", "The person you have contacted has not yet added you as a friend, so your message has not been delivered.")
		return
	}

	c := object_by_name(u, "chat", "friend", f.Name)
	if c == nil {
		c = object_create(u, "chat", "friend", f.Name, f.Label)
		if c == nil {
			return
		}
	}
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

	j := json_encode(map[string]string{"from": e.From, "name": f.Label, "time": time_unix_string(), "body": body})
	object_value_append(u, c.ID, "messages", "\n"+j)
	service(u, "notifications", "create", c.ID, f.Label+": "+body, "/chat/view/?friend="+f.Name)
}

// Ask user who they'd like to chat with
func chat_new(u *User, action string, format string, p app_parameters) string {
	friends, _ := service_generic[*[]Object](u, "friends", "list")
	return app_template("chat/"+format+"/new", friends)
}

// View a chat
func chat_view(u *User, action string, format string, p app_parameters) string {
	//TODO Fix pointers
	fp, _ := service_generic[*Object](u, "friends", "get", app_parameter(p, "friend", ""))
	f := *fp
	if f == nil {
		return "Friend not found"
	}
	c := object_by_name(u, "chat", "friend", f.Name)
	if c == nil {
		c = object_create(u, "chat", "friend", f.Name, f.Label)
		if c == nil {
			return "Unable to create chat"
		}
	} else {
		object_touch(u, c.ID)
		service(u, "notifications", "clear/object", c.ID)
	}

	messages := object_value_get(u, c.ID, "messages", "")
	message := app_parameter(p, "message", "")
	if message != "" {
		// User sent a message
		event(u, f.Name, "chat", "", "message", json_encode(map[string]string{"body": message}))
		j := json_encode(map[string]string{"from": u.Public, "name": u.Name, "time": time_unix_string(), "body": message})
		object_value_append(u, c.ID, "messages", "\n"+j)
		messages = messages + "\n" + j
	}

	return app_template("chat/"+format+"/view", map[string]any{"Friend": f, "Messages": messages})
}

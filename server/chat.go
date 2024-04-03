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
	f := service(u, "friends", "get", e.From).(*Object)
	if f == nil {
		// Event from unkown sender. Send them an error reply and drop their message.
		event(u, e.From, "chat", "", "message", "The person you have contacted has not yet added you as a friend, so your message has not been delivered.")
		return
	}

	c := object_by_name(u, "chat", "friend", f.Name)
	if c == nil {
		c = object_create(u, "chat", "friend", f.Name, f.Name)
		if c == nil {
			return
		}
	}
	object_value_append(u, c.ID, "messages", "\n"+f.Name+": "+e.Content)
	service(u, "notifications", "create", c.ID, f.Name+": "+e.Content, "/chat/view/?friend="+f.Name)
}

// Ask user who they'd like to chat with
func chat_new(u *User, action string, format string, p app_parameters) string {
	return app_template("chat/"+format+"/new", service(u, "friends", "list"))
}

// View a chat
func chat_view(u *User, action string, format string, p app_parameters) string {
	f := service(u, "friends", "get", app_parameter(p, "friend", "")).(*Object)
	if f == nil {
		return "Friend not found"
	}
	c := object_by_name(u, "chat", "friend", f.Name)
	if c == nil {
		c = object_create(u, "chat", "friend", f.Name, f.Name)
		if c == nil {
			return "Unable to creat chat"
		}
	} else {
		object_touch(u, c.ID)
		service(u, "notifications", "clear/object", c.ID)
	}

	messages := object_value_get(u, c.ID, "messages", "")
	message := app_parameter(p, "message", "")
	if message != "" {
		// User sent a message
		event(u, f.Name, "chat", "", "message", message)
		object_value_append(u, c.ID, "messages", "\n"+u.Name+": "+message)
		messages = messages + "\n" + u.Name + ": " + message
	}

	return app_template("chat/"+format+"/view", map[string]any{"Friend": f, "Messages": messages})
}

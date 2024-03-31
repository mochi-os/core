// Comms sample internal service: Chat
// Copyright Alistair Cunningham 2024

package main

func init() {
	app_register("chat", map[string]string{"en": "Chat"})
	app_register_display("chat", chat_display)
	app_register_event("chat", "message", chat_message_receive)
	app_register_service("chat", "chat")
}

// Display app
func chat_display(u *User, p app_parameters, format string) string {
	action := app_parameter(p, "action", "")

	if action == "new" {
		// Ask user who they'd like to chat with
		return app_template("chat/"+format+"/new", service(u, "friends", "list"))

	} else if action == "view" {
		f := service(u, "friends", "get", app_parameter(p, "friend", "")).(*Object)
		if f == nil {
			return "Friend not found"
		}
		c := object_by_path(u, "chat", "friends/"+f.ID)
		if c == nil {
			c = object_create(u, "chat", "friends/"+f.ID, "friends", f.Name)
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
			event(u, f.ID, "chat", "", "message", message)
			object_value_append(u, c.ID, "messages", "\n"+u.Name+": "+message)
			messages = messages + "\n" + u.Name + ": " + message
		}

		return app_template("chat/"+format+"/view", map[string]any{"Friend": f, "Messages": messages})

	} else {
		// List existing chats
		return app_template("chat/"+format+"/list", objects_by_parent(u, "chat", "friends", "updated desc"))
	}
}

// Received a chat event from another user
func chat_message_receive(u *User, e *Event) {
	f := service(u, "friends", "get", e.From).(*Object)
	if f == nil {
		// Event from unkown sender. Send them an error reply and drop their message.
		event(u, e.From, "chat", "", "message", "The person you have contacted has not yet added you as a friend, so your message has not been delivered.")
		return
	}

	c := object_by_path(u, "chat", "friends/"+f.ID)
	if c == nil {
		c = object_create(u, "chat", "friends/"+f.ID, "friends", f.Name)
		if c == nil {
			return
		}
	}
	object_value_append(u, c.ID, "messages", "\n"+f.Name+": "+e.Content)
	service(u, "notifications", "create/object", c.ID, f.Name+": "+e.Content, "?app=chat&action=view&friend="+f.ID)
}

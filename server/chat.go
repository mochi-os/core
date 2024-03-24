// Comms sample internal service: Chat
// Copyright Alistair Cunningham 2024

package main

func init() {
	app_register_internal("chat", "Chat", []string{"chat"})
	app_register_function_display("chat", chat_display)
	app_register_function_event("chat", chat_event)
}

// Display app
func chat_display(u *User, p app_parameters, format string) string {
	action := app_parameter(p, "action", "")

	if action == "new" {
		// Ask user who they'd like to chat with
		return app_template("chat/"+format+"/new", service(u, "friends", "list"))

	} else if action == "view" {
		instance := app_parameter(p, "id", "")
		var f *Friend
		var messages string

		if instance == "" {
			// New chat
			f = service(u, "friends", "get", app_parameter(p, "friend", "")).(*Friend)
			if f == nil {
				return "Friend not found"
			}
			instance = uid()
			err := instance_create(u.ID, instance, f.Name, "chat")
			if err != nil {
				return app_error(err)
			}
			err = data_set(u.ID, "chat", instance, "friend", f.ID)
			if err != nil {
				return app_error(err)
			}
			_, err = event(u.Public, f.ID, "chat", instance, "invite", u.Name)
			if err != nil {
				return app_error(err)
			}

		} else {
			// Existing chat
			i := instance_by_id(u.ID, instance)
			if i == nil {
				return "Chat not found"
			}
			instance_touch(u.ID, instance)
			service(u, "notifications", "clear/instance", instance)
			f = service(u, "friends", "get", data_get(u.ID, "chat", instance, "friend", "")).(*Friend)
			if f == nil {
				return "Friend not found"
			}
			messages = data_get(u.ID, "chat", instance, "messages", "")
		}

		message := app_parameter(p, "message", "")
		if message != "" {
			// User sent a message
			_, err := event(u.Public, f.ID, "chat", instance, "message", message)
			if err != nil {
				return app_error(err)
			}
			err = data_append(u.ID, "chat", instance, "messages", "\n"+u.Name+": "+message)
			if err != nil {
				return app_error(err)
			}
			messages = messages + "\n" + u.Name + ": " + message
		}

		return app_template("chat/"+format+"/view", map[string]any{"Instance": instance, "Friend": f, "Messages": messages})

	} else {
		// List existing chats
		return app_template("chat/"+format+"/list", instances_by_service(u.ID, "chat", "updated desc"))
	}
}

// Received a chat event from another user
func chat_event(u *User, e *Event) {
	f := service(u, "friends", "get", e.From).(*Friend)
	if f == nil {
		// Event from unkown sender. Send them an error reply and drop their message.
		event(u.Public, e.From, "chat", e.Instance, "message", "The person you have contacted has not yet added you as a friend, so your message has not been delivered.")
		return
	}

	if e.Action == "invite" {
		// Remote party invited us to a new chat
		//TODO Check if we already have an instance
		err := instance_create(u.ID, e.Instance, e.Content, "chat")
		if err != nil {
			log_info(err.Error())
			return
		}
		err = data_set(u.ID, "chat", e.Instance, "friend", e.From)
		if err != nil {
			log_info(err.Error())
			return
		}
		service(u, "notifications", "create", e.Instance, "New chat with "+f.Name, "?app=chat&action=view&id="+e.Instance)

	} else if e.Action == "message" {
		// Remote party sent us a message in an existing chat
		//TODO Check instance exists
		err := data_append(u.ID, "chat", e.Instance, "messages", "\n"+f.Name+": "+e.Content)
		if err != nil {
			log_info(err.Error())
			return
		}
		service(u, "notifications", "create", e.Instance, f.Name+": "+e.Content, "?app=chat&action=view&id="+e.Instance)

	} else {
		log_info("Dropping received event due to unknown action '%s'", e.Action)
	}
}

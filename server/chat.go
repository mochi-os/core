// Comms sample internal service: Chat
// Copyright Alistair Cunningham 2024

package main

import (
	"strings"
)

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
		f := service(u, "friends", "get", app_parameter(p, "friend", "")).(*Friend)
		if f == nil {
			return "Friend not found"
		}
		instance := "chat-" + f.ID
		if instance_by_id(u.ID, instance) == nil {
			instance_create(u.ID, instance, f.Name, "chat")
		} else {
			instance_touch(u.ID, instance)
			service(u, "notifications", "clear/instance", instance)
		}

		messages := data_get(u.ID, "chat", instance, "messages", "")
		message := app_parameter(p, "message", "")
		if message != "" {
			// User sent a message
			event(u, f.ID, "chat", instance, "message", message)
			data_append(u.ID, "chat", instance, "messages", "\n"+u.Name+": "+message)
			messages = messages + "\n" + u.Name + ": " + message
		}

		return app_template("chat/"+format+"/view", map[string]any{"Friend": f, "Messages": messages})

	} else {
		// List existing chats
		var chats []Instance
		for _, c := range *instances_by_service(u.ID, "chat", "updated desc") {
			c.ID = strings.TrimLeft(c.ID, "chat-")
			chats = append(chats, c)
		}
		return app_template("chat/"+format+"/list", chats)
	}
}

// Received a chat event from another user
func chat_message_receive(u *User, e *Event) {
	f := service(u, "friends", "get", e.From).(*Friend)
	if f == nil {
		// Event from unkown sender. Send them an error reply and drop their message.
		event(u, e.From, "chat", e.Instance, "message", "The person you have contacted has not yet added you as a friend, so your message has not been delivered.")
		return
	}

	instance := "chat-" + f.ID
	if instance_by_id(u.ID, instance) == nil {
		instance_create(u.ID, instance, f.Name, "chat")
	}
	data_append(u.ID, "chat", instance, "messages", "\n"+f.Name+": "+e.Content)
	service(u, "notifications", "create/instance", instance, f.Name+": "+e.Content, "?app=chat&action=view&friend="+f.ID)
}

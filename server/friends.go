// Comms server: Friends
// Copyright Alistair Cunningham 2024

package main

func init() {
	app_register("friends", map[string]string{"en": "Friends"})
	app_register_display("friends", friends_display)
	app_register_event("friends", "accept", friends_event_accept)
	app_register_event("friends", "cancel", friends_event_cancel)
	app_register_event("friends", "invite", friends_event_invite)
	app_register_service("friends", "get", friends_get)
	app_register_service("friends", "list", friends_list)
}

// Accept a friend's invitation
func friend_accept(u *User, friend string) {
	i := object_by_tag(u, "friends", "invite/from", friend)
	if i == nil {
		return
	}
	f := object_by_tag(u, "friends", "friend", friend)
	if f == nil {
		friend_create(u, friend, i.Name, "person", false)
	}
	event(u, friend, "friends", "", "accept", "")
	object_delete_by_id(u, i.ID)

	// Cancel any invitation we had sent to them
	i = object_by_tag(u, "friends", "invite/to", friend)
	if i != nil {
		event(u, friend, "friends", "", "cancel", "")
		object_delete_by_id(u, i.ID)
	}
}

func friend_create(u *User, friend string, name string, class string, invite bool) error {
	if !valid(friend, "public") {
		return error_message("Invalid ID")
	}
	if !valid(name, "name") {
		return error_message("Invalid name")
	}
	if !valid(class, "^person$") {
		return error_message("Invalid class")
	}
	if object_by_tag(u, "friends", "friend", friend) != nil {
		return error_message("You are already friends")
	}

	f := object_create(u, "friends", "friend", friend, name)
	if f == nil {
		return error_message("Unable to create friend")
	}

	i := object_by_tag(u, "friends", "invite/from", friend)
	if i != nil {
		// We have an existing invitation from them, so accept it automatically
		event(u, friend, "friends", "", "accept", "")
		object_delete_by_id(u, i.ID)

	} else if invite {
		// Send invitation
		object_create(u, "friends", "invite/to", friend, name)
		event(u, friend, "friends", "", "invite", u.Name)
	}

	return nil
}

func friend_delete(u *User, friend string) {
	log_debug("Deleting friend '%s'", friend)

	i := object_by_tag(u, "friends", "invite/from", friend)
	if i != nil {
		event(u, friend, "friends", "", "ignore", "")
		object_delete_by_id(u, i.ID)
	}

	i = object_by_tag(u, "friends", "invite/to", friend)
	if i != nil {
		event(u, friend, "friends", "", "cancel", "")
		object_delete_by_id(u, i.ID)
	}

	object_delete_by_tag(u, "friends", "friend", friend)
}

func friends_display(u *User, p app_parameters, format string) string {
	action := app_parameter(p, "action", "")

	if action == "accept" {
		// Accept friend invitation
		friend_accept(u, app_parameter(p, "id", ""))
		return app_template("friends/" + format + "/accepted")

	} else if action == "create" {
		// Create new friend
		err := friend_create(u, app_parameter(p, "id", ""), app_parameter(p, "name", ""), "person", true)
		if err != nil {
			return app_error(err)
		}
		return app_template("friends/" + format + "/created")

	} else if action == "delete" {
		// Delete friend
		friend_delete(u, app_parameter(p, "id", ""))
		return app_template("friends/" + format + "/deleted")

	} else if action == "ignore" {
		friend_ignore(u, app_parameter(p, "id", ""))
		return app_template("friends/" + format + "/ignored")

	} else if action == "new" {
		// New friend selector
		return app_template("friends/" + format + "/new")

	} else if action == "search" {
		// Search the directory for potential friends
		search := app_parameter(p, "search", "")
		if search == "" {
			return "No search terms entered"
		}
		return app_template("friends/"+format+"/search", directory_search(search))
	}

	// No action specified; show list of friends
	return app_template("friends/"+format+"/list", map[string]any{"Friends": objects_by_category(u, "friends", "friend", "name"), "Invites": objects_by_category(u, "friends", "invite/from", "updated desc")})
}

// Remote party accepted our invitation
func friends_event_accept(u *User, e *Event) {
	i := object_by_tag(u, "friends", "invite/to", e.From)
	if i != nil {
		service(u, "notification", "create", e.From, object_value_get(u, i.ID, "name", "Unknown person")+" accepted your friend invitation", "?app=friends")
		object_delete_by_id(u, i.ID)
	}
}

// Remote party cancelled their existing invitation
func friends_event_cancel(u *User, e *Event) {
	i := object_by_tag(u, "friends", "invite/from", e.From)
	if i != nil {
		object_delete_by_id(u, i.ID)
	}
}

// Remote party sent us a new invitation
func friends_event_invite(u *User, e *Event) {
	p := object_by_tag(u, "friends", "invite/to", e.From)
	if p != nil {
		// We have an existing invitation to them, so accept theirs automatically, and cancel ours
		friend_accept(u, e.From)
	} else {
		// Store the invitation, but don't notify the user so we don't have notification spam
		i := object_create(u, "friends", "invite/from", e.From, e.Content)
		if i != nil {
			object_value_set(u, i.ID, "name", e.Content)
		}
	}
}

func friend_ignore(u *User, friend string) {
	i := object_by_tag(u, "friends", "invite/from", friend)
	if i != nil {
		object_delete_by_id(u, i.ID)
	}
}

func friends_get(u *User, service string, values ...any) any {
	if len(values) == 1 {
		return object_by_tag(u, "friends", "friend", values[0].(string))
	}
	return nil
}

func friends_list(u *User, service string, values ...any) any {
	return objects_by_category(u, "friends", "friend", "name")
}

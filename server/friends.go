// Comms server: Friends service
// Copyright Alistair Cunningham 2024

package main

type Friend struct {
	User     int
	ID       string
	Name     string
	Class    string
	Location string
}

func init() {
	app_register_internal("comms/friends", "Friends", []string{"friends"})
	app_register_function_call("comms/friends", friends_call)
	app_register_function_display("comms/friends", friends_display)
	app_register_function_event("comms/friends", friends_event)
}

func friend_accept(u *User, instance string) {
	i := instance_by_id(u.ID, instance)
	if i == nil {
		return
	}

	id := data_get(u.ID, "comms/friends", instance, "id", "")
	if friend_by_id(u, id) == nil {
		friend_create(u, id, i.Name, "person", false)
	}
	event(u.Public, id, "friends", instance, "accept", "")
	instance_delete(u.ID, instance)
}

func friend_by_id(u *User, id string) *Friend {
	var c Friend
	if db_struct(&c, "users", "select * from friends where user=? and id=?", u.ID, id) {
		return &c
	}
	return nil
}

func friends_by_user(u *User) *[]Friend {
	var c []Friend
	db_structs(&c, "users", "select * from friends where user=? order by name, id", u.ID)
	return &c
}

func friends_call(u *User, service string, function string, values ...any) any {
	if function == "get" {
		if len(values) == 0 {
			return nil
		}
		return friend_by_id(u, values[0].(string))

	} else if function == "list" {
		return friends_by_user(u)

	} else {
		return nil
	}
}

func friend_create(u *User, id string, name string, class string, invite bool) error {
	if db_exists("users", "select id from friends where user=? and id=?", u.ID, id) {
		return error_message("You are already friends")
	}
	if !valid(id, "public") {
		return error_message("Invalid ID")
	}
	if !valid(name, "name") {
		return error_message("Invalid name")
	}
	if !valid(class, "^person$") {
		return error_message("Invalid class")
	}

	db_exec("users", "insert into friends ( user, id, name, class ) values ( ?, ?, ?, ? )", u.ID, id, name, class)

	p := friend_previous_invite(u, id, "receive")
	if p != nil {
		// We have an existing invitation from them, so accept it automatically
		event(u.Public, id, "friends", p.ID, "accept", "")
		instance_delete(u.ID, p.ID)

	} else if invite {
		// Send invitation
		instance := uid()
		instance_create(u.ID, instance, name, "friends")
		data_set(u.ID, "comms/friends", instance, "id", id)
		data_set(u.ID, "comms/friends", instance, "direction", "send")
		event(u.Public, id, "friends", instance, "invite", u.Name)
	}

	return nil
}

func friend_delete(u *User, id string) error {
	if !db_exists("users", "select id from friends where user=? and id=?", u.ID, id) {
		return error_message("friend not found")
	}

	db_exec("users", "delete from friends where user=? and id=?", u.ID, id)
	return nil
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
		err := friend_delete(u, app_parameter(p, "id", ""))
		if err != nil {
			return app_error(err)
		}
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

	return app_template("friends/"+format+"/list", map[string]any{"Friends": friends_by_user(u), "Invitations": friend_invitations_received(u)})
}

func friends_event(u *User, e *Event) {
	i := instance_by_id(u.ID, e.Instance)
	id := data_get(u.ID, "comms/friends", e.Instance, "id", "")
	if i != nil && e.From != id {
		log_info("Dropping received friend event due to incorrect sender")
		return
	}

	if e.Action == "accept" {
		// Remote party accepted our invitation
		if i != nil {
			service(u, "notification", "create", i.ID, i.Name+" accepted your friend invitation", "?app=friends")
			instance_delete(u.ID, i.ID)
		}

	} else if e.Action == "cancel" {
		// Remote party cancelled their existing invitation
		if i != nil {
			instance_delete(u.ID, i.ID)
		}

	} else if e.Action == "invite" {
		// Remote party sent us a new invitation
		p := friend_previous_invite(u, e.From, "send")
		if p != nil {
			// We have an existing invitation to them, so accept theirs automatically, and cancel ours
			friend_accept(u, e.From)
			event(e.To, e.From, "friends", p.ID, "cancel", "")
			instance_delete(u.ID, p.ID)
		} else {
			// Store the invitation, but don't notify the user so we don't have notification spam
			instance_create(u.ID, e.Instance, e.Content, "friends")
			data_set(u.ID, "comms/friends", e.Instance, "id", e.From)
			data_set(u.ID, "comms/friends", e.Instance, "direction", "receive")
		}

	} else {
		log_info("Dropping received event due to unknown action '%s'", e.Action)
	}
}

func friend_invitations_received(u *User) *[]Instance {
	var invitations []Instance
	for _, i := range *instances_by_service(u.ID, "friends", "updated desc") {
		if data_get(u.ID, "comms/friends", i.ID, "direction", "") == "receive" {
			invitations = append(invitations, i)
		}
	}
	return &invitations
}

func friend_ignore(u *User, instance string) {
	i := instance_by_id(u.ID, instance)
	if i != nil {
		instance_delete(u.ID, instance)
	}
}

func friend_previous_invite(u *User, id string, direction string) *Instance {
	for _, i := range *instances_by_service(u.ID, "friends", "id") {
		r := data_get(u.ID, "comms/friends", i.ID, "id", "")
		if r == id {
			return &i
		}
	}
	return nil
}

// Comms server: Friends
// Copyright Alistair Cunningham 2024

package main

type Friend struct {
	ID    string
	Name  string
	Class string
}

type FriendInvite struct {
	ID        string
	Direction string
	Name      string
	Updated   int64
}

func init() {
	app_register("friends", map[string]string{"en": "Friends"})
	app_register_action("friends", "", friends_action_list)
	app_register_action("friends", "accept", friends_action_accept)
	app_register_action("friends", "create", friends_action_create)
	app_register_action("friends", "delete", friends_action_delete)
	app_register_action("friends", "ignore", friends_action_ignore)
	app_register_action("friends", "list", friends_action_list)
	app_register_action("friends", "new", friends_action_new)
	app_register_action("friends", "search", friends_action_search)
	app_register_event("friends", "accept", friends_event_accept)
	app_register_event("friends", "cancel", friends_event_cancel)
	app_register_event("friends", "invite", friends_event_invite)
	app_register_path("friends", "friends")
}

// Create app database
func friends_db_create(db string) {
	db_exec(db, "create table friends ( id text not null primary key, name text not null, class text not null )")
	db_exec(db, "create index friends_name on friends( name )")
	db_exec(db, "create table invites ( id text not null, direction text not null, name text not null, updated integer not null, primary key ( id, direction ) )")
	db_exec(db, "create index invites_direction on invites( direction )")
}

// Get a friend
func friend(u *User, id string) *Friend {
	db := db_app(u, "friends", "data.db", friends_db_create)
	var f Friend
	if db_struct(&f, db, "select * from friends where id=?", id) {
		return &f
	}
	return nil
}

// List friends
func friends(u *User) *[]Friend {
	db := db_app(u, "friends", "data.db", friends_db_create)
	var f []Friend
	db_structs(&f, db, "select * from friends order by name")
	return &f
}

// Accept friend invitation
func friends_action_accept(u *User, action string, format string, p app_parameters) string {
	friend_accept(u, app_parameter(p, "id", ""))
	return app_template("friends/accepted")
}

// Create new friend
func friends_action_create(u *User, action string, format string, p app_parameters) string {
	err := friend_create(u, app_parameter(p, "id", ""), app_parameter(p, "name", ""), "person", true)
	if err != nil {
		return app_error(err)
	}
	return app_template("friends/created")
}

// Delete friend
func friends_action_delete(u *User, action string, format string, p app_parameters) string {
	friend_delete(u, app_parameter(p, "id", ""))
	return app_template("friends/deleted")
}

// Ignore friend invitation
func friends_action_ignore(u *User, action string, format string, p app_parameters) string {
	friend_ignore(u, app_parameter(p, "id", ""))
	return app_template("friends/ignored")
}

// Show list of friends
func friends_action_list(u *User, action string, format string, p app_parameters) string {
	db := db_app(u, "friends", "data.db", friends_db_create)
	var f []Friend
	db_structs(&f, db, "select * from friends order by name")
	var i []FriendInvite
	db_structs(&i, db, "select * from invites where direction='from' order by updated desc")

	return app_template("friends/list", map[string]any{"Friends": f, "Invites": i})
}

// New friend selector
func friends_action_new(u *User, action string, format string, p app_parameters) string {
	return app_template("friends/new")
}

// Search the directory for potential friends
func friends_action_search(u *User, action string, format string, p app_parameters) string {
	search := app_parameter(p, "search", "")
	if search == "" {
		return "No search terms entered"
	}
	return app_template("friends/search", directory_search(u, search, false))
}

// Accept a friend's invitation
func friend_accept(u *User, friend string) {
	db := db_app(u, "friends", "data.db", friends_db_create)

	var i FriendInvite
	db_struct(&i, db, "select * from invites where id=? and direction='from'", friend)
	if i.ID == "" {
		return
	}

	if !db_exists(db, "select id from friends where id=?", friend) {
		friend_create(u, friend, i.Name, "person", false)
	}
	event(u, friend, "friends", "", "accept", "")
	db_exec(db, "delete from invites where id=? and direction='from'", friend)

	// Cancel any invitation we had sent to them
	if db_exists(db, "select id from invites where id=? and direction='to'", friend) {
		event(u, friend, "friends", "", "cancel", "")
		db_exec(db, "delete from invites where id=? and direction='to'", friend)
	}

	broadcast(u, "friends", "accept", friend, nil)
}

// Create new friend
func friend_create(u *User, friend string, name string, class string, invite bool) error {
	db := db_app(u, "friends", "data.db", friends_db_create)

	if !valid(friend, "public") {
		return error_message("Invalid ID")
	}
	if !valid(name, "name") {
		return error_message("Invalid name")
	}
	if !valid(class, "^person$") {
		return error_message("Invalid class")
	}
	if db_exists(db, "select id from friends where id=?", friend) {
		return error_message("You are already friends")
	}

	db_exec(db, "replace into friends ( id, name, class ) values ( ?, ?, ? )", friend, name, class)

	if db_exists(db, "select id from invites where id=? and direction='from'", friend) {
		// We have an existing invitation from them, so accept it automatically
		event(u, friend, "friends", "", "accept", "")
		db_exec(db, "delete from invites where id=? and direction='from'", friend)

	} else if invite {
		// Send invitation
		event(u, friend, "friends", "", "invite", u.Name)
		db_exec(db, "replace into invites ( id, direction, name, updated ) values ( ?, 'to', ?, ? )", friend, name, time_unix_string())
	}

	broadcast(u, "friends", "create", friend, nil)
	return nil
}

// Delete friend
func friend_delete(u *User, friend string) {
	log_debug("Deleting friend '%s'", friend)
	db := db_app(u, "friends", "data.db", friends_db_create)
	db_exec(db, "delete from invites where id=?", friend)
	db_exec(db, "delete from friends where id=?", friend)
	broadcast(u, "friends", "delete", friend, nil)
}

// Remote party accepted our invitation
func friends_event_accept(u *User, e *Event) {
	db := db_app(u, "friends", "data.db", friends_db_create)
	var i FriendInvite
	db_struct(&i, db, "select * from invites where id=? and direction='to'", e.From)
	if i.ID != "" {
		service(u, "notifications", "create", "friends", "accept", i.ID, i.Name+" accepted your friend invitation", "/friends/")
		db_exec(db, "delete from invites where id=? and direction='to'", e.From)
		broadcast(u, "friends", "accepted", e.From, nil)
	}
}

// Remote party cancelled their existing invitation
func friends_event_cancel(u *User, e *Event) {
	db := db_app(u, "friends", "data.db", friends_db_create)
	db_exec(db, "delete from invites where id=? and direction='from'", e.From)
	broadcast(u, "friends", "cancelled", e.From, nil)
}

// Remote party sent us a new invitation
func friends_event_invite(u *User, e *Event) {
	db := db_app(u, "friends", "data.db", friends_db_create)
	if db_exists(db, "select id from invites where id=? and direction='to'", e.From) {
		// We have an existing invitation to them, so accept theirs automatically and cancel ours
		friend_accept(u, e.From)
	} else {
		// Store the invitation, but don't notify the user so we don't have notification spam
		db_exec(db, "replace into invites ( id, direction, name, updated ) values ( ?, 'from', ?, ? )", e.From, e.Content, time_unix_string())
	}
	broadcast(u, "friends", "invited", e.From, nil)
}

// Ignore a friend invitation
func friend_ignore(u *User, friend string) {
	db := db_app(u, "friends", "data.db", friends_db_create)
	db_exec(db, "delete from invites where id=? and direction='from'", friend)
	broadcast(u, "friends", "ignore", friend, nil)
}

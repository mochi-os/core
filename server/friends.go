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
	a := register_app("friends")
	a.register_home("friends", map[string]string{"en": "Friends"})
	a.register_action("friends", friends_action_list, true)
	a.register_action("friends/accept", friends_action_accept, true)
	a.register_action("friends/create", friends_action_create, true)
	a.register_action("friends/delete", friends_action_delete, true)
	a.register_action("friends/ignore", friends_action_ignore, true)
	a.register_action("friends/new", friends_action_new, true)
	a.register_action("friends/search", friends_action_search, true)
	a.register_event("accept", friends_accept_event)
	a.register_event("cancel", friends_cancel_event)
	a.register_event("invite", friends_invite_event)
}

// Create app database
func friends_db_create(db *DB) {
	db.exec("create table settings ( name text not null primary key, value text not null )")
	db.exec("replace into settings ( name, value ) values ( 'schema', 1 )")

	db.exec("create table friends ( id text not null primary key, name text not null, class text not null )")
	db.exec("create index friends_name on friends( name )")

	db.exec("create table invites ( id text not null, direction text not null, name text not null, updated integer not null, primary key ( id, direction ) )")
	db.exec("create index invites_direction on invites( direction )")
}

// Get a friend
func friend(u *User, id string) *Friend {
	db := db_app(u, "friends", "data.db", friends_db_create)
	defer db.close()

	var f Friend
	if db.scan(&f, "select * from friends where id=?", id) {
		return &f
	}
	return nil
}

// List friends
func friends(u *User) *[]Friend {
	db := db_app(u, "friends", "data.db", friends_db_create)
	defer db.close()

	var f []Friend
	db.scans(&f, "select * from friends order by name")
	return &f
}

// Accept friend invitation
func friends_action_accept(u *User, a *Action) {
	friend_accept(u, a.input("id"))
	a.template("friends/accepted")
}

// Create new friend
func friends_action_create(u *User, a *Action) {
	err := friend_create(u, a.input("id"), a.input("name"), "person", true)
	if err != nil {
		a.error(500, "Unable to create friend: %s", err)
		return
	}
	a.template("friends/created")
}

// Delete friend
func friends_action_delete(u *User, a *Action) {
	friend_delete(u, a.input("id"))
	a.template("friends/deleted")
}

// Ignore friend invitation
func friends_action_ignore(u *User, a *Action) {
	friend_ignore(u, a.input("id"))
	a.template("friends/ignored")
}

// Show list of friends
func friends_action_list(u *User, a *Action) {
	db := db_app(u, "friends", "data.db", friends_db_create)
	defer db.close()

	var f []Friend
	db.scans(&f, "select * from friends order by name")
	var i []FriendInvite
	db.scans(&i, "select * from invites where direction='from' order by updated desc")

	switch a.input("format") {
	case "json":
		a.json(f)
	default:
		a.template("friends/list", map[string]any{"Friends": f, "Invites": i})
	}
}

// New friend selector
func friends_action_new(u *User, a *Action) {
	a.template("friends/new")
}

// Search the directory for potential friends
func friends_action_search(u *User, a *Action) {
	search := a.input("search")
	if search == "" {
		a.error(400, "No search entered")
		return
	}
	a.template("friends/search", directory_search(u, "person", search, false))
}

// Accept a friend's invitation
func friend_accept(u *User, friend string) {
	db := db_app(u, "friends", "data.db", friends_db_create)
	defer db.close()

	var fi FriendInvite
	db.scan(&fi, "select * from invites where id=? and direction='from'", friend)
	if fi.ID == "" {
		return
	}

	if !db.exists("select id from friends where id=?", friend) {
		friend_create(u, friend, fi.Name, "person", false)
	}
	event := Event{ID: uid(), From: u.Identity.ID, To: friend, App: "friends", Action: "accept"}
	event.send()
	db.exec("delete from invites where id=? and direction='from'", friend)

	// Cancel any invitation we had sent to them
	if db.exists("select id from invites where id=? and direction='to'", friend) {
		event := Event{ID: uid(), From: u.Identity.ID, To: friend, App: "friends", Action: "cancel"}
		event.send()
		db.exec("delete from invites where id=? and direction='to'", friend)
	}

	broadcast(u, "friends", "accept", friend, nil)
}

// Remote party accepted our invitation
func friends_accept_event(u *User, e *Event) {
	db := db_app(u, "friends", "data.db", friends_db_create)
	defer db.close()

	var fi FriendInvite
	db.scan(&fi, "select * from invites where id=? and direction='to'", e.From)
	if fi.ID != "" {
		notification_create(u, "friends", "accept", fi.ID, fi.Name+" accepted your friend invitation", "/friends/")
		db.exec("delete from invites where id=? and direction='to'", e.From)
		broadcast(u, "friends", "accepted", e.From, nil)
	}
}

// Remote party cancelled their existing invitation
func friends_cancel_event(u *User, e *Event) {
	db := db_app(u, "friends", "data.db", friends_db_create)
	defer db.close()

	db.exec("delete from invites where id=? and direction='from'", e.From)
	broadcast(u, "friends", "cancelled", e.From, nil)
}

// Create new friend
func friend_create(u *User, friend string, name string, class string, invite bool) error {
	db := db_app(u, "friends", "data.db", friends_db_create)
	defer db.close()

	if !valid(friend, "public") {
		return error_message("Invalid ID")
	}
	if !valid(name, "name") {
		return error_message("Invalid name")
	}
	if !valid(class, "^person$") {
		return error_message("Invalid class")
	}
	if db.exists("select id from friends where id=?", friend) {
		return error_message("You are already friends")
	}

	db.exec("replace into friends ( id, name, class ) values ( ?, ?, ? )", friend, name, class)

	if db.exists("select id from invites where id=? and direction='from'", friend) {
		// We have an existing invitation from them, so accept it automatically
		event := Event{ID: uid(), From: u.Identity.ID, To: friend, App: "friends", Action: "accept"}
		event.send()
		db.exec("delete from invites where id=? and direction='from'", friend)

	} else if invite {
		// Send invitation
		event := Event{ID: uid(), From: u.Identity.ID, To: friend, App: "friends", Action: "invite", Content: u.Identity.Name}
		event.send()
		db.exec("replace into invites ( id, direction, name, updated ) values ( ?, 'to', ?, ? )", friend, name, now_string())
	}

	broadcast(u, "friends", "create", friend, nil)
	return nil
}

// Delete friend
func friend_delete(u *User, friend string) {
	db := db_app(u, "friends", "data.db", friends_db_create)

	db.exec("delete from invites where id=?", friend)
	db.exec("delete from friends where id=?", friend)
	broadcast(u, "friends", "delete", friend, nil)
}

// Remote party sent us a new invitation
func friends_invite_event(u *User, e *Event) {
	db := db_app(u, "friends", "data.db", friends_db_create)

	if db.exists("select id from invites where id=? and direction='to'", e.From) {
		// We have an existing invitation to them, so accept theirs automatically and cancel ours
		friend_accept(u, e.From)
	} else {
		// Store the invitation, but don't notify the user so we don't have notification spam
		db.exec("replace into invites ( id, direction, name, updated ) values ( ?, 'from', ?, ? )", e.From, e.Content, now_string())
	}
	broadcast(u, "friends", "invited", e.From, nil)
}

// Ignore a friend invitation
func friend_ignore(u *User, friend string) {
	db := db_app(u, "friends", "data.db", friends_db_create)

	db.exec("delete from invites where id=? and direction='from'", friend)
	broadcast(u, "friends", "ignore", friend, nil)
}

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
	a := app("friends")
	a.home("friends", map[string]string{"en": "Friends"})
	a.db("data.db", friends_db_create)

	a.path("friends", friends_list)
	a.path("friends/accept", friends_accept)
	a.path("friends/create", friends_create)
	a.path("friends/delete", friends_delete)
	a.path("friends/ignore", friends_ignore)
	a.path("friends/new", friends_new)
	a.path("friends/search", friends_search)

	a.event("accept", friends_accept_event)
	a.event("cancel", friends_cancel_event)
	a.event("invite", friends_invite_event)
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
	if db.scan(&f, "select * from friends where id=?", u.Identity.ID) {
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

// Accept a friend's invitation
func friend_accept(u *User, db *DB, friend string) {
	var fi FriendInvite
	db.scan(&fi, "select * from invites where id=? and direction='from'", friend)
	if fi.ID == "" {
		return
	}

	if !db.exists("select id from friends where id=?", friend) {
		friend_create(u, db, friend, fi.Name, "person", false)
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

// Accept friend invitation
func friends_accept(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	friend_accept(a.user, a.db, a.input("id"))
	a.template("friends/accepted")
}

// Remote party accepted our invitation
func friends_accept_event(e *Event) {
	var fi FriendInvite
	e.db.scan(&fi, "select * from invites where id=? and direction='to'", e.From)
	if fi.ID != "" {
		notification(e.user, "friends", "accept", fi.ID, fi.Name+" accepted your friend invitation", "/friends/")
		e.db.exec("delete from invites where id=? and direction='to'", e.From)
		broadcast(e.user, "friends", "accepted", e.From, nil)
	}
}

// Remote party cancelled their existing invitation
func friends_cancel_event(e *Event) {
	e.db.exec("delete from invites where id=? and direction='from'", e.From)
	broadcast(e.user, "friends", "cancelled", e.From, nil)
}

// Create new friend
func friend_create(u *User, db *DB, friend string, name string, class string, invite bool) error {
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

// Create new friend
func friends_create(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	err := friend_create(a.user, a.db, a.input("id"), a.input("name"), "person", true)
	if err != nil {
		a.error(500, "Unable to create friend: %s", err)
		return
	}
	a.template("friends/created")
}

// Delete friend
func friends_delete(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	friend := a.input("id")
	a.db.exec("delete from invites where id=?", friend)
	a.db.exec("delete from friends where id=?", friend)
	broadcast(a.user, "friends", "delete", friend, nil)
	a.template("friends/deleted")
}

// Ignore friend invitation
func friends_ignore(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	friend := a.input("id")
	a.db.exec("delete from invites where id=? and direction='from'", friend)
	broadcast(a.user, "friends", "ignore", friend, nil)
	a.template("friends/ignored")
}

// Remote party sent us a new invitation
func friends_invite_event(e *Event) {
	if e.db.exists("select id from invites where id=? and direction='to'", e.From) {
		// We have an existing invitation to them, so accept theirs automatically and cancel ours
		friend_accept(e.user, e.db, e.From)
	} else {
		// Store the invitation, but don't notify the user so we don't have notification spam
		e.db.exec("replace into invites ( id, direction, name, updated ) values ( ?, 'from', ?, ? )", e.From, e.Content, now_string())
	}
	broadcast(e.user, "friends", "invited", e.From, nil)
}

// Show list of friends
func friends_list(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	var f []Friend
	a.db.scans(&f, "select * from friends order by name")
	var i []FriendInvite
	a.db.scans(&i, "select * from invites where direction='from' order by updated desc")

	switch a.input("format") {
	case "json":
		a.json(f)
	default:
		a.template("friends/list", M{"Friends": f, "Invites": i})
	}
}

// New friend selector
func friends_new(a *Action) {
	a.template("friends/new")
}

// Search the directory for potential friends
func friends_search(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	search := a.input("search")
	if search == "" {
		a.error(400, "No search entered")
		return
	}
	a.template("friends/search", directory_search(a.user, "person", search, false))
}

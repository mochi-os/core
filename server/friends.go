// Mochi server: Friends
// Copyright Alistair Cunningham 2024-2025

package main

type Friend struct {
	Identity string
	ID       string
	Name     string
	Class    string
}

type FriendInvite struct {
	Identity  string
	ID        string
	Direction string
	Name      string
	Updated   int64
}

func init() {
	a := app("friends")
	a.home("friends", map[string]string{"en": "Friends"})
	a.db("friends/friends.db", friends_db_create)

	a.path("friends", friends_list)
	a.path("friends/accept", friends_accept)
	a.path("friends/create", friends_create)
	a.path("friends/delete", friends_delete)
	a.path("friends/ignore", friends_ignore)
	a.path("friends/new", friends_new)
	a.path("friends/search", friends_search)

	a.service("friends")
	a.event("accept", friends_accept_event)
	a.event("cancel", friends_cancel_event)
	a.event("invite", friends_invite_event)
}

// Create app database
func friends_db_create(db *DB) {
	db.exec("create table settings ( name text not null primary key, value text not null )")
	db.exec("replace into settings ( name, value ) values ( 'schema', 1 )")

	db.exec("create table friends ( identity text not null, id text not null, name text not null, class text not null, primary key ( identity, id ) )")
	db.exec("create index friends_id on friends( id )")
	db.exec("create index friends_name on friends( name )")

	db.exec("create table invites ( identity text not null, id text not null, direction text not null, name text not null, updated integer not null, primary key ( identity, id, direction ) )")
	db.exec("create index invites_direction on invites( direction )")
}

// Get a friend
func friend(u *User, id string) *Friend {
	db := db_user(u, "friends/friends.db", friends_db_create)
	defer db.close()

	var f Friend
	if db.scan(&f, "select * from friends where identity=? and id=?", u.Identity.ID, id) {
		return &f
	}
	return nil
}

// List friends
func friends(u *User) *[]Friend {
	db := db_user(u, "friends/friends.db", friends_db_create)
	defer db.close()

	var f []Friend
	db.scans(&f, "select * from friends order by name, identity, id")
	return &f
}

// Accept a friend's invitation
func friend_accept(u *User, db *DB, id string) {
	var fi FriendInvite
	db.scan(&fi, "select * from invites where identity=? and id=? and direction='from'", u.Identity.ID, id)
	if fi.ID == "" {
		return
	}

	if !db.exists("select id from friends where identity=? and id=?", u.Identity.ID, id) {
		friend_create(u, db, id, fi.Name, "person", false)
	}
	message(u.Identity.ID, id, "friends", "accept").send()
	db.exec("delete from invites where identity=? and id=? and direction='from'", u.Identity.ID, id)

	// Cancel any invitation we had sent to them
	if db.exists("select id from invites where identity=? and id=? and direction='to'", u.Identity.ID, id) {
		message(u.Identity.ID, id, "friends", "cancel").send()
		db.exec("delete from invites where identity=? and id=? and direction='to'", u.Identity.ID, id)
	}

	broadcast(u, "friends", "accept", id, nil)
}

// Accept friend invitation
func friends_accept(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	friend_accept(a.user, a.user.db, a.input("id"))
	a.template("friends/accepted")
}

// Remote party accepted our invitation
func friends_accept_event(e *Event) {
	var fi FriendInvite
	if e.db.scan(&fi, "select * from invites where identity=? and id=? and direction='to'", e.to, e.from) {
		notification(e.user, "friends", "accept", fi.ID, fi.Name+" accepted your friend invitation", "/friends")
		e.db.exec("delete from invites where identity=? and id=? and direction='to'", e.to, e.from)
		broadcast(e.user, "friends", "accepted", e.from, nil)
	}
}

// Remote party cancelled their existing invitation
func friends_cancel_event(e *Event) {
	e.db.exec("delete from invites where identity=? and id=? and direction='from'", e.to, e.from)
	broadcast(e.user, "friends", "cancelled", e.from, nil)
}

// Create new friend
func friend_create(u *User, db *DB, id string, name string, class string, invite bool) error {
	if !valid(id, "entity") {
		return error_message("Invalid ID")
	}
	if !valid(name, "name") {
		return error_message("Invalid name")
	}
	if !valid(class, "^person$") {
		return error_message("Invalid class")
	}
	if db.exists("select id from friends where identity=? and id=?", u.Identity.ID, id) {
		return error_message("You are already friends")
	}

	db.exec("replace into friends ( identity, id, name, class ) values ( ?, ?, ?, ? )", u.Identity.ID, id, name, class)

	if db.exists("select id from invites where identity=? and id=? and direction='from'", u.Identity.ID, id) {
		// We have an existing invitation from them, so accept it automatically
		message(u.Identity.ID, id, "friends", "accept").send()
		db.exec("delete from invites where identity=? and id=? and direction='from'", u.Identity.ID, id)

	} else if invite {
		// Send invitation
		message(u.Identity.ID, id, "friends", "invite").set("name", u.Identity.Name).send()
		db.exec("replace into invites ( identity, id, direction, name, updated ) values ( ?, ?, 'to', ?, ? )", u.Identity.ID, id, name, now_string())
	}

	broadcast(u, "friends", "create", id, nil)
	return nil
}

// Create new friend
func friends_create(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	err := friend_create(a.user, a.user.db, a.input("id"), a.input("name"), "person", true)
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

	id := a.input("id")
	a.user.db.exec("delete from invites where identity=? and id=?", a.user.Identity.ID, id)
	a.user.db.exec("delete from friends where identity=? and id=?", a.user.Identity.ID, id)
	broadcast(a.user, "friends", "delete", id, nil)
	a.template("friends/deleted")
}

// Ignore friend invitation
func friends_ignore(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}

	id := a.input("id")
	a.user.db.exec("delete from invites where identity=? and id=? and direction='from'", a.user.Identity.ID, id)
	broadcast(a.user, "friends", "ignore", id, nil)
	a.template("friends/ignored")
}

// Remote party sent us a new invitation
func friends_invite_event(e *Event) {
	name := e.get("name", "")
	if !valid(name, "line") {
		info("Friends dropping invitation with invalid name '%s'", name)
		return
	}

	if e.db.exists("select id from invites where identity=? and id=? and direction='to'", e.to, e.from) {
		// We have an existing invitation to them, so accept theirs automatically and cancel ours
		friend_accept(e.user, e.db, e.from)
	} else {
		// Store the invitation, but don't notify the user so we don't have notification spam
		e.db.exec("replace into invites ( identity, id, direction, name, updated ) values ( ?, ?, 'from', ?, ? )", e.to, e.from, name, now_string())
	}
	broadcast(e.user, "friends", "invited", e.from, nil)
}

// Show list of friends
func friends_list(a *Action) {
	if a.user == nil {
		a.error(401, "Not logged in")
		return
	}
	notifications_clear_app(a.user, "friends")

	var f []Friend
	a.user.db.scans(&f, "select * from friends order by name, identity, id")
	var i []FriendInvite
	a.user.db.scans(&i, "select * from invites where direction='from' order by updated desc")

	switch a.input("format") {
	case "json":
		a.json(f)
	default:
		a.template("friends/list", Map{"Friends": f, "Invites": i})
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

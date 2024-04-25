// Comms server: Users
// Copyright Alistair Cunningham 2024

package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"time"
)

type Code struct {
	Code     string
	Username string
	Expires  int
}

type Login struct {
	User    int
	Code    string
	Name    string
	Expires int
}

type User struct {
	ID        int
	Username  string
	Name      string
	Role      string
	Private   string
	Public    string
	Language  string
	Published string
}

func code_send(email string) bool {
	if !email_valid(email) {
		return false
	}
	code := random_alphanumeric(12)
	db := db_open("db/users.db")
	db.exec("replace into codes ( code, username, expires ) values ( ?, ?, ? )", code, email, time_unix()+3600)
	email_send(email, "Comms login code", "Please copy and paste the code below into your web browser. This code is valid for one hour.\n\n"+code)
	return true
}

func login_create(user int) string {
	code := random_alphanumeric(20)
	db := db_open("db/users.db")
	db.exec("replace into logins ( user, code, expires ) values ( ?, ?, ? )", user, code, time_unix()+365*86400)
	return code
}

func login_delete(code string) {
	db := db_open("db/users.db")
	db.exec("delete from logins where code=?", code)
}

func user_by_id(id int) *User {
	var u User
	db := db_open("db/users.db")
	if db.scan(&u, "select * from users where id=?", id) {
		return &u
	}
	return nil
}

func user_by_login(login string) *User {
	var l Login
	db := db_open("db/users.db")
	if !db.scan(&l, "select * from logins where code=? and expires>=?", login, time_unix()) {
		return nil
	}

	var u User
	if db.scan(&u, "select * from users where id=?", l.User) {
		return &u
	}

	return nil
}

func user_from_code(code string) *User {
	var c Code
	db := db_open("db/users.db")
	if !db.scan(&c, "select * from codes where code=? and expires>=?", code, time_unix()) {
		return nil
	}
	db.exec("delete from codes where code=?", code)

	var u User
	if db.scan(&u, "select * from users where username=?", c.Username) {
		return &u
	}

	public, private, err := ed25519.GenerateKey(rand.Reader)
	check(err)

	db.exec("replace into users ( username, name, role, public, private, language ) values ( ?, '', 'user', ?, ?, 'en' )", c.Username, base64_encode(public), base64_encode(private))

	if db.scan(&u, "select * from users where username=?", c.Username) {
		return &u
	}

	log_warn("Unable to create user")
	return nil
}

func user_location(user string) (string, string, string, string) {
	// Check if user is local
	var u User
	dbu := db_open("db/users.db")
	if dbu.scan(&u, "select * from users where public=?", user) {
		return "local", u.Public, "user", u.Public
	}

	// Check in directory
	var d Directory
	dbd := db_open("db/directory.db")
	if dbd.scan(&d, "select location from directory where id=?", user) {
		address := peer_address(d.Location)
		if address != "" {
			return "libp2p", address, "peer", d.Location
		}
		peer_request(d.Location)
		return "peer", d.Location, "peer", d.Location
	}

	directory_request(user)
	return "user", user, "user", user
}

// Re-publish all our users every 30 days so the network knows they're still active
func users_manager() {
	db := db_open("db/users.db")

	for {
		time.Sleep(time.Minute)
		var users []User
		db.scans(&users, "select * from users where published<?", time_unix()-30*86400)
		for _, u := range users {
			db.exec("update users set published=? where id=?", time_unix(), u.ID)
			directory_publish(&u)
		}
	}
}

func user_set_name(u *User, name string) {
	db := db_open("db/users.db")
	db.exec("update users set name=? where id=?", name, u.ID)
}

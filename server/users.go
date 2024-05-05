// Comms server: Users
// Copyright Alistair Cunningham 2024

package main

import (
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
	ID       int
	Username string
	Identity *Identity
	Role     string
	Language string
	Timezone string
}

func code_send(email string) bool {
	if !email_valid(email) {
		return false
	}
	code := random_alphanumeric(12)
	db := db_open("db/users.db")
	db.exec("replace into codes ( code, username, expires ) values ( ?, ?, ? )", code, email, time_unix()+3600)
	email_send(email, "Comms login code", "Please copy and paste the code below into your web browser. This code is valid for one hour.\n\n"+code)
	log_debug("Code: %s", code)
	return true
}

func (u *User) identity() *Identity {
	db := db_open("db/users.db")
	var i Identity
	if db.scan(&i, "select * from identities where user=? and class='person' order by id limit 1", u.ID) {
		return &i
	}
	return nil
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

func (u *User) time_local(t int64) string {
	l, err := time.LoadLocation(u.Timezone)
	if err == nil {
		return time.Unix(t, 0).In(l).Format(time.DateTime)
	} else {
		log_warn("Invalid time zone '%s':", err)
		return time.Unix(t, 0).Format(time.DateTime)
	}
}

func user_by_id(id int) *User {
	db := db_open("db/users.db")
	var u User
	if !db.scan(&u, "select * from users where id=?", id) {
		return nil
	}

	u.Identity = u.identity()
	if u.Identity == nil {
		return nil
	}

	return &u
}

func user_by_identity(id string) *User {
	db := db_open("db/users.db")
	var i Identity
	if !db.scan(&i, "select * from identities where id=?", id) {
		return nil
	}

	var u User
	if !db.scan(&u, "select * from users where id=?", i.User) {
		return nil
	}

	u.Identity = &i
	return &u
}

func user_by_login(login string) *User {
	if login == "" {
		return nil
	}

	var l Login
	db := db_open("db/users.db")
	if !db.scan(&l, "select * from logins where code=? and expires>=?", login, time_unix()) {
		return nil
	}

	var u User
	if !db.scan(&u, "select * from users where id=?", l.User) {
		return nil
	}

	u.Identity = u.identity()
	return &u
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
		u.Identity = u.identity()
		return &u
	}

	db.exec("replace into users ( username, role, language, timezone ) values ( ?, 'user', 'en', '' )", c.Username)

	if db.scan(&u, "select * from users where username=?", c.Username) {
		return &u
	}

	log_warn("Unable to create user")
	return nil
}

func user_owning_identity(id string) *User {
	db := db_open("db/users.db")
	var i Identity
	if !db.scan(&i, "select * from identities where id=?", id) {
		return nil
	}

	var u User
	if !db.scan(&u, "select * from users where id=?", i.User) {
		return nil
	}

	u.Identity = u.identity()
	if u.Identity == nil {
		return nil
	}

	return &u
}

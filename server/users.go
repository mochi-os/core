// Comms server: Users
// Copyright Alistair Cunningham 2024

package main

import (
	"crypto/ed25519"
	"crypto/rand"
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
	Name     string
	Role     string
	Private  string
	Public   string
}

func code_send(email string) bool {
	if !email_valid(email) {
		return false
	}
	code := random_alphanumeric(12)
	db_exec("users", "insert into codes ( code, username, expires ) values ( ?, ?, ? )", code, email, time_unix()+3600)
	email_send(email, "Comms login code", "Please copy and paste the code below into your web browser. This code is valid for one hour.\n\n"+code)
	return true
}

func login_create(user int) string {
	code := random_alphanumeric(20)
	db_exec("users", "insert into logins ( user, code, expires ) values ( ?, ?, ? )", user, code, time_unix()+365*86400)
	return code
}

func login_delete(code string) {
	db_exec("users", "delete from logins where code=?", code)
}

func user_by_id(id int) *User {
	var u User
	if db_struct(&u, "users", "select * from users where id=?", id) {
		return &u
	}
	return nil
}

func user_by_login(login string) *User {
	var l Login
	if !db_struct(&l, "users", "select * from logins where code=? and expires>=?", login, time_unix()) {
		return nil
	}

	var u User
	if db_struct(&u, "users", "select * from users where id=?", l.User) {
		return &u
	}

	return nil
}

func user_from_code(code string) *User {
	var c Code
	if !db_struct(&c, "users", "select * from codes where code=? and expires>=?", code, time_unix()) {
		return nil
	}
	db_exec("users", "delete from codes where code=?", code)

	var u User
	if db_struct(&u, "users", "select * from users where username=?", c.Username) {
		return &u
	}

	public, private, err := ed25519.GenerateKey(rand.Reader)
	fatal(err)

	db_exec("users", "insert into users ( username, name, role, public, private ) values ( ?, '', 'user', ?, ? )", c.Username, base64_encode(public), base64_encode(private))

	if db_struct(&u, "users", "select * from users where username=?", c.Username) {
		return &u
	}

	log_warn("Unable to create user")
	return nil
}

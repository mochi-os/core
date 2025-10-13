// Mochi server: Users
// Copyright Alistair Cunningham 2024-2025

package main

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
	Role     string
	Language string
	Timezone string
	Identity *Entity
	db       *DB // Used by actions
}

func code_send(email string) bool {
	if !email_valid(email) {
		return false
	}
	code := random_alphanumeric(12)
	debug("Code '%s'", code)
	db := db_open("db/users.db")
	db.exec("replace into codes ( code, username, expires ) values ( ?, ?, ? )", code, email, now()+3600)
	email_send(email, "Mochi login code", "Please copy and paste the code below into your web browser. This code is valid for one hour.\n\n"+code)
	return true
}

func login_create(user int) string {
	code := random_alphanumeric(20)
	db := db_open("db/users.db")
	db.exec("replace into logins ( user, code, expires ) values ( ?, ?, ? )", user, code, now()+365*86400)
	return code
}

func login_delete(code string) {
	db := db_open("db/users.db")
	db.exec("delete from logins where code=?", code)
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
	var i Entity
	if !db.scan(&i, "select * from entities where id=?", id) {
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
	if !db.scan(&l, "select * from logins where code=? and expires>=?", login, now()) {
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
	if !db.scan(&c, "select * from codes where code=? and expires>=?", code, now()) {
		return nil
	}
	db.exec("delete from codes where code=?", code)

	var u User
	if db.scan(&u, "select * from users where username=?", c.Username) {
		u.Identity = u.identity()
		return &u
	}

	db.exec("replace into users ( username, role, language, timezone ) values ( ?, 'user', 'en', '' )", c.Username)

	// Remove once we have hooks
	admin := ini_string("email", "admin", "")
	if admin != "" {
		email_send(admin, "Mochi new user", "New user: "+c.Username)
	}

	if db.scan(&u, "select * from users where username=?", c.Username) {
		return &u
	}

	warn("Unable to create user")
	return nil
}

func user_owning_entity(id string) *User {
	if id == "" {
		return nil
	}

	db := db_open("db/users.db")
	var i Entity
	if !db.scan(&i, "select * from entities where id=?", id) {
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

func (u *User) administrator() bool {
	if u.Role == "administrator" {
		return true
	}
	return false
}

func (u *User) identity() *Entity {
	db := db_open("db/users.db")
	var i Entity
	if db.scan(&i, "select * from entities where user=? and class='person' order by id limit 1", u.ID) {
		return &i
	}
	return nil
}

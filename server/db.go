// Comms server: Database
// Copyright Alistair Cunningham 2024

package main

import (
	"database/sql"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

const latest_schema = 1

var db_handles = map[string]*sqlx.DB{}

func db_create() {
	log_info("Creating new database")

	// Settings
	db_exec("settings", "create table settings ( name text not null primary key, value text not null )")
	db_exec("settings", "insert into settings ( name, value ) values ( 'schema', ? )", latest_schema)

	// Users
	db_exec("users", "create table users ( id integer primary key, username text not null, name text not null, role text not null default 'user', private text not null, public text not null )")
	db_exec("users", "create unique index users_username on users( username )")
	db_exec("users", "create unique index users_private on users( private )")
	db_exec("users", "create unique index users_public on users( public )")

	// Login codes
	db_exec("users", "create table codes ( code text not null, username text not null, expires integer not null, primary key ( code, username ) )")
	db_exec("users", "create index codes_expires on codes( expires )")

	// Logins
	db_exec("users", "create table logins ( user references users( id ), code text not null, name text not null default '', expires integer not null, primary key ( user, code ) )")
	db_exec("users", "create unique index logins_code on logins( code )")
	db_exec("users", "create index logins_expires on logins( expires )")

	// Friends
	db_exec("users", "create table friends ( user references users( id ), id text not null, name text not null, class text not null, location text not null default '', primary key ( user, id ) )")
	db_exec("users", "create index friends_id on friends( id )")
	db_exec("users", "create index friends_name on friends( name )")
	db_exec("users", "create index friends_class on friends( class )")

	// Directory
	db_exec("directory", "create table directory ( id text not null primary key, fingerprint text not null, name text not null, class text not null, location text not null default '', updated integer )")
	db_exec("directory", "create index directory_fingerprint on directory( fingerprint )")
	db_exec("directory", "create index directory_name on directory( name )")
	db_exec("directory", "create index directory_class on directory( class )")
	db_exec("directory", "create index directory_updated on directory( updated )")

	// Instances
	db_exec("data", "create table instances ( user integer not null, id text not null, name text not null default '', service text not null default '', updated integer not null, primary key ( user, id ) )")
	db_exec("data", "create index instances_name on instances( name )")
	db_exec("data", "create index instances_service on instances( service )")
	db_exec("data", "create index instances_updated on instances( updated )")

	// User data
	db_exec("data", "create table data ( user integer not null, app text not null, instance text not null default '', name text not null, value text not null, primary key ( user, app, instance, name ) )")
	db_exec("data", "create index data_instance on data( instance )")

	// Notifications
	db_exec("notifications", "create table notifications ( id text not null primary key, user integer not null, instance text not null default '', content text not null, link text not null default '', updated integer not null )")
	db_exec("notifications", "create index notifications_user_instance on notifications( user, instance )")
	db_exec("notifications", "create index notifications_updated on notifications( updated )")
}

func db_exec(file string, query string, values ...any) {
	//log_debug("db_exec('%s', ...)", query)
	_, err := db_open(file).Exec(query, values...)
	fatal(err)
}

func db_exists(file string, query string, values ...any) bool {
	//log_debug("db_exists('%s', ...)", query)
	r, err := db_open(file).Query(query, values...)
	fatal(err)
	defer r.Close()

	for r.Next() {
		return true
	}
	return false
}

func db_init() {
	if file_exists("db/users.db") {
		db_upgrade()
	} else {
		db_create()
	}
}

func db_open(file string) *sqlx.DB {
	h, open := db_handles[file]
	if open {
		return h
	}

	path := "db/" + file + ".db"
	if !file_exists(path) {
		file_mkdir("db")
		file_create(path)
	}

	var err error
	db_handles[file], err = sqlx.Open("sqlite3", base_dir+path)
	fatal(err)
	return db_handles[file]
}

func db_struct(out any, file string, query string, values ...any) bool {
	//log_debug("db_struct('%s', ...)", query)
	err := db_open(file).QueryRowx(query, values...).StructScan(out)
	if err != nil {
		if err == sql.ErrNoRows {
			return false
		}
		fatal(err)
	}
	return true
}

func db_structs(out any, file string, query string, values ...any) {
	//log_debug("db_structs('%s', ...)", query)
	err := db_open(file).Select(out, query, values...)
	fatal(err)
}

func db_upgrade() {
	schema := atoi(setting_get("schema", ""), 1)
	for schema < latest_schema {
		schema++
		log_info("Upgrading database schema to version %d", schema)
		if schema == 2 {
		} else if schema == 3 {
		}
		setting_set("schema", string(schema))
	}
}

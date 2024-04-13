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
	db_exec("settings", "replace into settings ( name, value ) values ( 'schema', ? )", latest_schema)

	// Users
	db_exec("users", "create table users ( id integer primary key, username text not null, name text not null, role text not null default 'user', private text not null, public text not null, language text not null default 'en', published integer not null default 0 )")
	db_exec("users", "create unique index users_username on users( username )")
	db_exec("users", "create unique index users_private on users( private )")
	db_exec("users", "create unique index users_public on users( public )")
	db_exec("users", "create index users_published on users( published )")

	// Login codes
	db_exec("users", "create table codes ( code text not null, username text not null, expires integer not null, primary key ( code, username ) )")
	db_exec("users", "create index codes_expires on codes( expires )")

	// Logins
	db_exec("users", "create table logins ( user references users( id ), code text not null, name text not null default '', expires integer not null, primary key ( user, code ) )")
	db_exec("users", "create unique index logins_code on logins( code )")
	db_exec("users", "create index logins_expires on logins( expires )")

	// User apps
	db_exec("users", "create table user_apps ( user references users( id ), app text not null, track text not null, path text not null default '', installed integer not null default 0, primary key ( user, app ) )")
	db_exec("users", "create index user_apps_app on user_apps( app )")

	// Directory
	db_exec("directory", "create table directory ( id text not null primary key, fingerprint text not null, name text not null, class text not null, location text not null default '', updated integer not null )")
	db_exec("directory", "create index directory_fingerprint on directory( fingerprint )")
	db_exec("directory", "create index directory_name on directory( name )")
	db_exec("directory", "create index directory_class on directory( class )")
	db_exec("directory", "create index directory_location on directory( location )")
	db_exec("directory", "create index directory_updated on directory( updated )")

	// Peers
	db_exec("peers", "create table peers ( id text not null primary key, address text not null, updated integer not null )")

	// Queued outbound events
	db_exec("queue", "create table queue ( id text not null primary key, method text not null, location text not null, event text not null, updated integer not null )")
	db_exec("queue", "create index queue_method_location on queue( method, location )")
	db_exec("queue", "create index queue_updated on queue( updated )")

	// App data objects
	db_exec("data", "create table objects ( id text not null primary key, user integer not null, app text not null, category text not null default '', name text not null, label text not null default '', updated integer not null )")
	db_exec("data", "create unique index objects_user_app_category_name on objects( user, app, category, name )")
	db_exec("data", "create index objects_updated on objects( updated )")

	// App data key/values
	db_exec("data", "create table object_values ( object references objects( id ), name text not null, value text not null, primary key ( object, name ) )")
}

func db_exec(file string, query string, values ...any) {
	//log_debug("db_exec('%s', ...)", query)
	_, err := db_open(file).Exec(query, values...)
	check(err)
}

func db_exists(file string, query string, values ...any) bool {
	//log_debug("db_exists('%s', ...)", query)
	r, err := db_open(file).Query(query, values...)
	check(err)
	defer r.Close()

	for r.Next() {
		return true
	}
	return false
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
	db_handles[file], err = sqlx.Open("sqlite3", data_dir+"/"+path)
	check(err)
	return db_handles[file]
}

func db_start() bool {
	if file_exists("db/users.db") {
		db_upgrade()
	} else {
		db_create()
		return true
	}
	return false
}

func db_struct(out any, file string, query string, values ...any) bool {
	//log_debug("db_struct('%s', ...)", query)
	err := db_open(file).QueryRowx(query, values...).StructScan(out)
	if err != nil {
		if err == sql.ErrNoRows {
			return false
		}
		check(err)
	}
	return true
}

func db_structs(out any, file string, query string, values ...any) {
	//log_debug("db_structs('%s', ...)", query)
	err := db_open(file).Select(out, query, values...)
	check(err)
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

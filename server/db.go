// Comms server: Database
// Copyright Alistair Cunningham 2024

package main

import (
	"database/sql"
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"sync"
)

const latest_schema = 1

// TODO Clean up stale handles
var db_handles = map[string]*sqlx.DB{}
var db_handles_lock sync.Mutex

func db_create() {
	log_info("Creating new database")

	// Settings
	db_exec("db/settings.db", "create table settings ( name text not null primary key, value text not null )")
	db_exec("db/settings.db", "replace into settings ( name, value ) values ( 'schema', ? )", latest_schema)

	// Users
	db_exec("db/users.db", "create table users ( id integer primary key, username text not null, name text not null, role text not null default 'user', private text not null, public text not null, language text not null default 'en', published integer not null default 0 )")
	db_exec("db/users.db", "create unique index users_username on users( username )")
	db_exec("db/users.db", "create unique index users_private on users( private )")
	db_exec("db/users.db", "create unique index users_public on users( public )")
	db_exec("db/users.db", "create index users_published on users( published )")

	// Login codes
	db_exec("db/users.db", "create table codes ( code text not null, username text not null, expires integer not null, primary key ( code, username ) )")
	db_exec("db/users.db", "create index codes_expires on codes( expires )")

	// Logins
	db_exec("db/users.db", "create table logins ( user references users( id ), code text not null, name text not null default '', expires integer not null, primary key ( user, code ) )")
	db_exec("db/users.db", "create unique index logins_code on logins( code )")
	db_exec("db/users.db", "create index logins_expires on logins( expires )")

	// User apps
	db_exec("db/users.db", "create table user_apps ( user references users( id ), app text not null, track text not null, path text not null default '', installed integer not null default 0, primary key ( user, app ) )")
	db_exec("db/users.db", "create index user_apps_app on user_apps( app )")

	// Directory
	db_exec("db/directory.db", "create table directory ( id text not null primary key, fingerprint text not null, name text not null, class text not null, location text not null default '', updated integer not null )")
	db_exec("db/directory.db", "create index directory_fingerprint on directory( fingerprint )")
	db_exec("db/directory.db", "create index directory_name on directory( name )")
	db_exec("db/directory.db", "create index directory_class on directory( class )")
	db_exec("db/directory.db", "create index directory_location on directory( location )")
	db_exec("db/directory.db", "create index directory_updated on directory( updated )")

	// Peers
	db_exec("db/peers.db", "create table peers ( id text not null primary key, address text not null, updated integer not null )")

	// Queued outbound events
	db_exec("db/queue.db", "create table queue ( id text not null primary key, method text not null, location text not null, event text not null, updated integer not null )")
	db_exec("db/queue.db", "create index queue_method_location on queue( method, location )")
	db_exec("db/queue.db", "create index queue_updated on queue( updated )")
}

func db_app(u *User, app string, file string, create func(string)) string {
	path := fmt.Sprintf("users/%d/apps/%s/%s", u.ID, app, file)
	if !file_exists(path) {
		file_create(path)
		create(path)
	}
	return path
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

func db_open(path string) *sqlx.DB {
	db_handles_lock.Lock()
	h, open := db_handles[path]
	db_handles_lock.Unlock()
	if open {
		return h
	}

	if !file_exists(path) {
		file_create(path)
	}

	var err error
	h, err = sqlx.Open("sqlite3", data_dir+"/"+path)
	check(err)
	db_handles_lock.Lock()
	db_handles[path] = h
	db_handles_lock.Unlock()
	return h
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

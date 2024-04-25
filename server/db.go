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

type DB struct {
	path   string
	handle *sqlx.DB
}

const latest_schema = 1

// TODO Clean up stale handles
var databases = map[string]*DB{}
var databases_lock sync.Mutex

func db_create() {
	log_info("Creating new database")

	// Settings
	settings := db_open("db/settings.db")
	settings.Exec("create table settings ( name text not null primary key, value text not null )")
	settings.Exec("replace into settings ( name, value ) values ( 'schema', ? )", latest_schema)

	// Users
	users := db_open("db/users.db")
	users.Exec("create table users ( id integer primary key, username text not null, name text not null, role text not null default 'user', private text not null, public text not null, language text not null default 'en', published integer not null default 0 )")
	users.Exec("create unique index users_username on users( username )")
	users.Exec("create unique index users_private on users( private )")
	users.Exec("create unique index users_public on users( public )")
	users.Exec("create index users_published on users( published )")

	// Login codes
	users.Exec("create table codes ( code text not null, username text not null, expires integer not null, primary key ( code, username ) )")
	users.Exec("create index codes_expires on codes( expires )")

	// Logins
	users.Exec("create table logins ( user references users( id ), code text not null, name text not null default '', expires integer not null, primary key ( user, code ) )")
	users.Exec("create unique index logins_code on logins( code )")
	users.Exec("create index logins_expires on logins( expires )")

	// User apps
	users.Exec("create table user_apps ( user references users( id ), app text not null, track text not null, path text not null default '', installed integer not null default 0, primary key ( user, app ) )")
	users.Exec("create index user_apps_app on user_apps( app )")

	// Directory
	directory := db_open("db/directory.db")
	directory.Exec("create table directory ( id text not null primary key, fingerprint text not null, name text not null, class text not null, location text not null default '', updated integer not null )")
	directory.Exec("create index directory_fingerprint on directory( fingerprint )")
	directory.Exec("create index directory_name on directory( name )")
	directory.Exec("create index directory_class on directory( class )")
	directory.Exec("create index directory_location on directory( location )")
	directory.Exec("create index directory_updated on directory( updated )")

	// Peers
	peers := db_open("db/peers.db")
	peers.Exec("create table peers ( id text not null primary key, address text not null, updated integer not null )")

	// Queued outbound events
	queue := db_open("db.queue.db")
	queue.Exec("db/queue.db", "create table queue ( id text not null primary key, method text not null, location text not null, event text not null, updated integer not null )")
	queue.Exec("db/queue.db", "create index queue_method_location on queue( method, location )")
	queue.Exec("db/queue.db", "create index queue_updated on queue( updated )")
}

func db_app(u *User, app string, file string, create func(*DB)) *DB {
	path := fmt.Sprintf("users/%d/apps/%s/%s", u.ID, app, file)
	if file_exists(path) {
		return db_open(path)
	}

	db := db_open(path)
	create(db)
	return db
}

func (db *DB) Exec(query string, values ...any) {
	_, err := db.handle.Exec(query, values...)
	check(err)
}

func (db *DB) Exists(query string, values ...any) bool {
	r, err := db.handle.Query(query, values...)
	check(err)
	defer r.Close()

	for r.Next() {
		return true
	}
	return false
}

func db_open(path string) *DB {
	databases_lock.Lock()
	db, found := databases[path]
	databases_lock.Unlock()
	if found {
		return db
	}

	if !file_exists(path) {
		file_create(path)
	}

	h, err := sqlx.Open("sqlite3", data_dir+"/"+path)
	check(err)
	db = &DB{path: path, handle: h}

	databases_lock.Lock()
	databases[path] = db
	databases_lock.Unlock()

	return db
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

func (db *DB) Struct(out any, query string, values ...any) bool {
	err := db.handle.QueryRowx(query, values...).StructScan(out)
	if err != nil {
		if err == sql.ErrNoRows {
			return false
		}
		check(err)
	}
	return true
}

func (db *DB) Structs(out any, query string, values ...any) {
	err := db.handle.Select(out, query, values...)
	check(err)
}

// Does nothing yet
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

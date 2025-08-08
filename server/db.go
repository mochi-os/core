// Mochi server: Database
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"database/sql"
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"sync"
	"time"
)

type DB struct {
	path   string
	handle *sqlx.DB
	closed int64
}

const (
	schema_version = 1
)

var (
	databases      = map[string]*DB{}
	databases_lock sync.Mutex
)

func db_create() {
	log_info("Creating new database")

	// Settings
	settings := db_open("db/settings.db")
	settings.exec("create table settings ( name text not null primary key, value text not null )")
	settings.exec("replace into settings ( name, value ) values ( 'schema', ? )", schema_version)

	// Users
	users := db_open("db/users.db")
	users.exec("create table users ( id integer primary key, username text not null, role text not null default 'user', language text not null default 'en', timezone text not null default 'UTC' )")
	users.exec("create unique index users_username on users( username )")

	// Login codes
	users.exec("create table codes ( code text not null, username text not null, expires integer not null, primary key ( code, username ) )")
	users.exec("create index codes_expires on codes( expires )")

	// Logins
	users.exec("create table logins ( user references users( id ), code text not null, name text not null default '', expires integer not null, primary key ( user, code ) )")
	users.exec("create unique index logins_code on logins( code )")
	users.exec("create index logins_expires on logins( expires )")

	// Entities
	users.exec("create table entities ( id text not null primary key, private text not null, fingerprint text not null, user references users( id ), parent text not null default '', class text not null, name text not null, privacy text not null default 'public', data text not null default '', published integer not null default 0 )")
	users.exec("create index entities_fingerprint on entities( fingerprint )")
	users.exec("create index entities_user on entities( user )")
	users.exec("create index entities_parent on entities( parent )")
	users.exec("create index entities_class on entities( class )")
	users.exec("create index entities_name on entities( name )")
	users.exec("create index entities_privacy on entities( privacy )")
	users.exec("create index entities_published on entities( published )")

	// Directory
	directory := db_open("db/directory.db")
	directory.exec("create table directory ( id text not null primary key, fingerprint text not null, name text not null, class text not null, location text not null default '', data text not null default '', created integer not null, updated integer not null )")
	directory.exec("create index directory_fingerprint on directory( fingerprint )")
	directory.exec("create index directory_name on directory( name )")
	directory.exec("create index directory_class on directory( class )")
	directory.exec("create index directory_location on directory( location )")
	directory.exec("create index directory_created on directory( created )")
	directory.exec("create index directory_updated on directory( updated )")

	// Peers
	peers := db_open("db/peers.db")
	peers.exec("create table peers ( id text not null, address text not null, updated integer not null, primary key ( id, address ) )")

	// Queued outbound events
	queue := db_open("db/queue.db")
	queue.exec("create table events ( id text not null primary key, method text not null, location text not null, event text not null, updated integer not null )")
	queue.exec("create index events_method_location on events( method, location )")
	queue.exec("create index events_updated on events( updated )")

	// Queued broadcast events
	queue.exec("create table broadcast ( id text not null primary key, topic text not null, content text not null, updated integer not null )")
	queue.exec("create index broadcast_updated on broadcast( updated )")

	// Cache
	cache := db_open("db/cache.db")
	cache.exec("create table attachments ( user integer not null, identity text not null, entity text not null, id text not null, thumbnail integer not null default 0, path text not null, created integer not null, primary key ( user, identity, entity, id, thumbnail ) )")
	cache.exec("create index attachments_path on attachments( path )")
	cache.exec("create index attachments_created on attachments( created )")
}

func db_manager() {
	for {
		time.Sleep(time.Minute)
		now := now()
		var closers []*sqlx.DB

		databases_lock.Lock()
		for _, db := range databases {
			if db.closed > 0 && db.closed < now-60 {
				closers = append(closers, db.handle)
				delete(databases, db.path)
			}
		}
		databases_lock.Unlock()

		for _, h := range closers {
			h.Close()
		}
	}
}

func db_open(file string) *DB {
	path := data_dir + "/" + file
	//log_debug("db_open() using '%s'", path)

	databases_lock.Lock()
	db, found := databases[path]
	databases_lock.Unlock()
	if found {
		db.closed = 0
		return db
	}

	//log_debug("db_open() opening '%s'", path)
	if !file_exists(path) {
		file_create(path)
	}

	h, err := sqlx.Open("sqlite3", path)
	check(err)
	db = &DB{path: path, handle: h, closed: 0}

	databases_lock.Lock()
	databases[path] = db
	databases_lock.Unlock()

	db.exec("PRAGMA journal_mode=WAL")
	return db
}

func db_start() bool {
	if file_exists(data_dir + "/db/users.db") {
		db_upgrade()
		go db_manager()
	} else {
		db_create()
		go db_manager()
		return true
	}
	return false
}

// Does nothing yet
func db_upgrade() {
	schema := atoi(setting_get("schema", ""), 1)
	for schema < schema_version {
		schema++
		log_info("Upgrading database schema to version %d", schema)
		if schema == 2 {
		} else if schema == 3 {
		}
		setting_set("schema", string(schema))
	}
}

func db_user(u *User, file string, create func(*DB)) *DB {
	path := fmt.Sprintf("users/%d/db/%s", u.ID, file)
	if file_exists(data_dir + "/" + path) {
		return db_open(path)
	}

	db := db_open(path)
	create(db)
	return db
}

func (db *DB) close() {
	db.closed = now()
}

func (db *DB) exec(query string, values ...any) {
	_, err := db.handle.Exec(query, values...)
	check(err)
}

func (db *DB) exists(query string, values ...any) bool {
	r, err := db.handle.Query(query, values...)
	check(err)

	for r.Next() {
		return true
	}
	return false
}

func (db *DB) integer(query string, values ...any) int {
	var result int
	err := db.handle.QueryRow(query, values...).Scan(&result)
	check(err)
	return result
}

func (db *DB) scan(out any, query string, values ...any) bool {
	err := db.handle.QueryRowx(query, values...).StructScan(out)
	if err != nil {
		if err == sql.ErrNoRows {
			return false
		}
		check(err)
	}
	return true
}

func (db *DB) scans(out any, query string, values ...any) {
	err := db.handle.Select(out, query, values...)
	check(err)
}

func (a *App) db(file string, create func(*DB)) {
	a.db_file = file
	a.db_create = create
}

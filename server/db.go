// Mochi server: Database
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

type DB struct {
	path   string
	handle *sqlx.DB
	user   *User
	closed int64
}

const (
	schema_version = 2
)

var (
	databases      = map[string]*DB{}
	databases_lock sync.Mutex
)

func db_create() {
	info("Creating new database")

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
	// code: the login token string presented by clients
	// secret: a per-login secret used to sign JWTs for that specific device/login
	users.exec("create table logins ( user references users( id ), code text not null, secret text not null default '', name text not null default '', expires integer not null, primary key ( user, code ) )")
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

	// Queued outbound messages
	queue := db_open("db/queue.db")
	queue.exec("create table entities ( id text not null primary key, entity text not null, data blob not null, file text not null default '', created integer not null )")
	queue.exec("create index entities_entity on entities( entity )")
	queue.exec("create index entities_created on entities( created )")

	queue.exec("create table peers ( id text not null primary key, peer text not null, data blob not null, file text not null default '', created integer not null )")
	queue.exec("create index peers_peer on peers( peer )")
	queue.exec("create index peers_created on peers( created )")

	queue.exec("create table broadcasts ( id text not null primary key, data blob not null, created integer not null )")
	queue.exec("create index broadcasts_created on broadcasts( created )")

	// Cache
	cache := db_open("db/cache.db")
	cache.exec("create table attachments ( user integer not null, identity text not null, entity text not null, id text not null, thumbnail integer not null default 0, path text not null, created integer not null, primary key ( user, identity, entity, id, thumbnail ) )")
	cache.exec("create index attachments_path on attachments( path )")
	cache.exec("create index attachments_created on attachments( created )")
}

// Open a database file for an app, creating it if necessary
func db_app(u *User, a *App) *DB {
	if a.Database.File == "" {
		warn("App '%s' asked for database, but no database file specified", a.id)
		return nil
	}

	path := fmt.Sprintf("users/%d/%s/%s", u.ID, a.id, a.Database.File)
	if file_exists(data_dir + "/" + path) {
		db := db_open(path)
		db.user = u
		return db
	}

	if a.Database.Create != "" {
		db := db_open(path)
		db.user = u
		s := a.starlark()
		s.set("app", a)
		s.set("user", u)
		s.set("owner", u)
		version_var, _ := s.call(a.Database.Create, nil)
		version := s.int(version_var)
		if version == 0 {
			info("App '%s' database creation function '%s' did not return a schema version, assuming 1", a.id, a.Database.Create)
			version = 1
		}
		db.exec("create table _settings ( name text not null primary key, value text not null )")
		db.exec("replace into _settings ( name, value ) values ( 'schema', ? )", version)
		return db
	}

	if a.Database.CreateFunction != nil {
		db := db_user(u, a.Database.File, a.Database.CreateFunction)
		db.user = u
		return db
	}

	warn("App '%s' has no way to create database file '%s'", a.id, a.Database.File)
	return nil
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
	//debug("db_open() using '%s'", path)

	databases_lock.Lock()
	db, found := databases[path]
	databases_lock.Unlock()
	if found {
		db.closed = 0
		return db
	}

	//debug("db_open() opening '%s'", path)
	if !file_exists(path) {
		file_create(path)
	}

	h := must(sqlx.Open("sqlite3", path))
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

func db_upgrade() {
	schema := atoi(setting_get("schema", ""), 1)

	for schema < schema_version {
		schema++
		info("Upgrading database schema to version %d", schema)
		if schema == 2 {
			// Migration: ensure logins table has a 'secret' column for per-login JWT secrets.
			// This runs for existing deployments which were created before the column was added.
			{
				db := db_open("db/users.db")
				// Check pragma for logins table columns
				rows, err := db.handle.Query("PRAGMA table_info(logins)")
				if err == nil {
					defer rows.Close()
					has_secret := false
					for rows.Next() {
						var cid int
						var col_name string
						var col_type string
						var col_notnull int
						var dflt_value sql.NullString
						var pk int
						// pragma table_info returns: cid,name,type,notnull,dflt_value,pk
						if err := rows.Scan(&cid, &col_name, &col_type, &col_notnull, &dflt_value, &pk); err == nil {
							if col_name == "secret" {
								has_secret = true
								break
							}
						}
					}

					if !has_secret {
						// Ensure the logins table actually exists before attempting ALTER
						if !db.exists("select name from sqlite_master where type='table' and name='logins'") {
							info("DB migration: 'logins' table not present, skipping secret migration")
						} else {
							info("DB migration: adding 'secret' column to logins table")
							// Add the column with a safe default
							db.exec("alter table logins add column secret text not null default ''")

							// Backfill existing rows with generated secrets inside a transaction
							db.exec("BEGIN")
							rows2, err := db.handle.Query("select user, code, secret from logins")
							if err == nil {
								defer rows2.Close()
								for rows2.Next() {
									var user_id int
									var code string
									var secret_val sql.NullString
									if err := rows2.Scan(&user_id, &code, &secret_val); err != nil {
										continue
									}
									if secret_val.Valid && secret_val.String != "" {
										continue
									}
									new_secret := random_alphanumeric(32)
									db.exec("update logins set secret=? where user=? and code=?", new_secret, user_id, code)
								}
							}
							db.exec("COMMIT")
						}
					}
				}
			}
		} else if schema == 3 {
		}
		setting_set("schema", itoa(int(schema)))
	}
}

// Open a database file for a user, creating it if necessary
func db_user(u *User, file string, create func(*DB)) *DB {
	path := fmt.Sprintf("users/%d/%s", u.ID, file)
	if file_exists(data_dir + "/" + path) {
		db := db_open(path)
		db.user = u
		return db
	}

	// File does not exist, so create it
	db := db_open(path)
	create(db)
	db.user = u
	return db
}

func (db *DB) close() {
	db.closed = now()
}

func (db *DB) exec(query string, values ...any) {
	must(db.handle.Exec(query, values...))
}

func (db *DB) exists(query string, values ...any) bool {
	r := must(db.handle.Query(query, values...))

	for r.Next() {
		return true
	}
	return false
}

func (db *DB) integer(query string, values ...any) int {
	var result int
	must(db.handle.QueryRow(query, values...).Scan(&result))
	return result
}

func (db *DB) row(query string, values ...any) map[string]any {
	r, err := db.handle.Queryx(query, values...)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		info("DB map error: %v", err)
		return nil
	}
	defer r.Close()

	for r.Next() {
		row := make(map[string]any)
		err = r.MapScan(row)
		if err != nil {
			info("DB maps error: %v", err)
			return nil
		}

		for i, v := range row {
			bytes, ok := v.([]byte)
			if ok {
				row[i] = string(bytes)
			}
		}

		return row
	}

	return nil
}

func (db *DB) rows(query string, values ...any) []map[string]any {
	var results []map[string]any

	r, err := db.handle.Queryx(query, values...)
	if err != nil {
		if err == sql.ErrNoRows {
			return results
		}
		info("DB maps error: %v", err)
		return nil
	}
	defer r.Close()

	for r.Next() {
		row := make(map[string]any)
		err = r.MapScan(row)
		if err != nil {
			info("DB maps error: %v", err)
			return nil
		}

		for i, v := range row {
			bytes, ok := v.([]byte)
			if ok {
				row[i] = string(bytes)
			}
		}

		results = append(results, row)
	}

	return results
}

func (db *DB) scan(out any, query string, values ...any) bool {
	err := db.handle.QueryRowx(query, values...).StructScan(out)
	if err != nil {
		if err == sql.ErrNoRows {
			return false
		}
		info("DB scan error: %v", err)
		return false
	}
	return true
}

func (db *DB) scans(out any, query string, values ...any) {
	must(db.handle.Select(out, query, values...))
}

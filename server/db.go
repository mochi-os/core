// Mochi server: Database
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/mattn/go-sqlite3"
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

type DB struct {
	path   string
	handle *sqlx.DB
	user   *User
	closed int64
}

const (
	schema_version = 3
)

var (
	databases      = map[string]*DB{}
	databases_lock sync.Mutex

	api_db = sls.FromStringDict(sl.String("mochi.db"), sl.StringDict{
		"exists": sl.NewBuiltin("mochi.db.exists", api_db_query),
		"query":  sl.NewBuiltin("mochi.db.query", api_db_query),
		"row":    sl.NewBuiltin("mochi.db.row", api_db_query),
	})
)

func init() {
	// Register a SQLite driver that blocks ATTACH/DETACH to prevent sandbox escape
	sql.Register("sqlite3_noattach", &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			conn.RegisterAuthorizer(func(action int, arg1, arg2, arg3 string) int {
				// SQLITE_ATTACH=24, SQLITE_DETACH=25
				if action == 24 || action == 25 {
					return sqlite3.SQLITE_DENY
				}
				return sqlite3.SQLITE_OK
			})
			return nil
		},
	})
}

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
	// code: the login token string that can be used for API authentication
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

	// Message queue with reliability tracking
	queue := db_open("db/queue.db")

	// Drop old schema (one-time migration to new reliability system)
	queue.exec("drop table if exists entities")
	queue.exec("drop table if exists peers")
	queue.exec("drop table if exists broadcasts")
	queue.exec("drop table if exists seen_nonces")

	// Outgoing message queue
	queue.exec("create table if not exists queue ( id text primary key, type text not null default 'direct', target text not null, from_entity text not null, to_entity text not null, service text not null, event text not null, content blob, data blob, file text, expires integer not null default 0, status text not null default 'pending', attempts integer not null default 0, next_retry integer not null, last_error text, created integer not null )")
	queue.exec("create index if not exists queue_status_retry on queue (status, next_retry)")
	queue.exec("create index if not exists queue_target on queue (target)")
}

// Open a database file for an app version, creating, upgrading, or downgrading it as necessary
func db_app(u *User, av *AppVersion) *DB {
	if av.app == nil {
		warn("Attempt to create database for unloaded app version")
		return nil
	}

	if av.Database.File == "" {
		warn("App %q version %q asked for database, but no database file specified", av.app.id, av.Version)
		return nil
	}

	path := fmt.Sprintf("users/%d/%s/%s", u.ID, av.app.id, av.Database.File)
	db, created, reused := db_open_work(path)
	db.user = u

	if reused {
		debug("Database app reusing already open %q", path)
		return db
	}

	//Lock everything below here to prevent race conditions when modifying the schema
	l := lock(path)
	l.Lock()
	defer l.Unlock()

	if created {
		debug("Database app creating %q", path)

		if av.Database.Create.Function != "" {
			s := av.starlark()
			s.set("app", av.app)
			s.set("user", u)
			s.set("owner", u)
			_, err := s.call(av.Database.Create.Function, nil)
			if err != nil {
				warn("App %q version %q database create error: %v", av.app.id, av.Version, err)
				return nil
			}
			db.schema(av.Database.Schema)

		} else if av.Database.create_function != nil {
			av.Database.create_function(db)
			db.schema(av.Database.Schema)

		} else {
			warn("App %q version %q has no way to create database file %q", av.app.id, av.Version, av.Database.File)
			return nil
		}

	} else {
		debug("Database app opening %q", path)

		// Check if _settings table exists, if not create it with schema 0
		has_settings, _ := db.exists("select name from sqlite_master where type='table' and name='_settings'")
		if !has_settings {
			debug("Database %q missing _settings table; initializing with schema 0", path)
			db.schema(0)
		}

		schema := db.integer("select cast(value as integer) from _settings where name='schema'")

		// Check if app tables exist - if not, call database_create()
		if av.Database.Create.Function != "" {
			// Check if any user tables exist (excluding _settings)
			has_tables, _ := db.exists("select name from sqlite_master where type='table' and name!='_settings'")
			if !has_tables {
				debug("Database %q exists but has no app tables; calling database_create()", path)
				s := av.starlark()
				s.set("app", av.app)
				s.set("user", u)
				s.set("owner", u)
				_, err := s.call(av.Database.Create.Function, nil)
				if err != nil {
					warn("App %q version %q database create error: %v", av.app.id, av.Version, err)
					return db
				}
				db.schema(av.Database.Schema)
				schema = av.Database.Schema
			}
		}

		if schema < av.Database.Schema && av.Database.Upgrade.Function != "" {
			for version := schema + 1; version <= av.Database.Schema; version++ {
				debug("Database %q upgrading to schema version %d", path, version)
				s := av.starlark()
				s.set("app", av.app)
				s.set("user", u)
				s.set("owner", u)
				_, err := s.call(av.Database.Upgrade.Function, sl_encode_tuple(version))
				if err != nil {
					warn("App %q version %q database upgrade error: %v", av.app.id, av.Version, err)
				}
				db.schema(version)
			}

		} else if schema > av.Database.Schema && av.Database.Downgrade.Function != "" {
			for version := schema; version > av.Database.Schema; version-- {
				debug("Database %q downgrading from schema version %d", path, version)
				s := av.starlark()
				s.set("app", av.app)
				s.set("user", u)
				s.set("owner", u)
				_, err := s.call(av.Database.Downgrade.Function, sl_encode_tuple(version))
				if err != nil {
					warn("App %q version %q database downgrade error: %v", av.app.id, av.Version, err)
				}
				db.schema(version - 1)
			}
		}
	}

	// Set up any helpers
	for _, helper := range av.Database.Helpers {
		setup, ok := app_helpers[helper]
		if ok {
			setup(db)
		}
	}

	return db
}

func db_manager() {
	for range time.Tick(time.Minute) {
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
	db, _, _ := db_open_work(file)
	return db
}

func db_open_work(file string) (*DB, bool, bool) {
	path := data_dir + "/" + file

	databases_lock.Lock()
	db, found := databases[path]
	databases_lock.Unlock()
	if found {
		//debug("Database reusing already open %q", path)
		db.closed = 0
		return db, false, true
	}

	created := false
	if !file_exists(path) {
		//debug("Database creating %q", path)
		file_create(path)
		created = true
	}

	//debug("Database opening %q", path)
	h := must(sqlx.Open("sqlite3_noattach", path))
	db = &DB{path: path, handle: h, closed: 0}

	databases_lock.Lock()
	databases[path] = db
	databases_lock.Unlock()

	db.exec("PRAGMA journal_mode=WAL")
	return db, created, false
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
			//TODO Remove once wasabi is running 0.2
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
						has_logins, _ := db.exists("select name from sqlite_master where type='table' and name='logins'")
						if !has_logins {
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
			// Migration: new message queue with reliability tracking
			queue := db_open("db/queue.db")

			// Drop old schema (one-time migration to new reliability system)
			queue.exec("drop table if exists entities")
			queue.exec("drop table if exists peers")
			queue.exec("drop table if exists broadcasts")
			queue.exec("drop table if exists seen_nonces")

			// Outgoing message queue
			queue.exec("create table if not exists queue ( id text primary key, type text not null default 'direct', target text not null, from_entity text not null, to_entity text not null, service text not null, event text not null, content blob, data blob, file text, status text not null default 'pending', attempts integer not null default 0, next_retry integer not null, last_error text, created integer not null )")
			queue.exec("create index if not exists queue_status_retry on queue (status, next_retry)")
			queue.exec("create index if not exists queue_target on queue (target)")
		}
		setting_set("schema", itoa(int(schema)))
	}
}

// Open a database file for an internal app, creating it if necessary
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

func (db *DB) exists(query string, values ...any) (bool, error) {
	r, err := db.handle.Query(query, values...)
	if err != nil {
		return false, err
	}
	defer r.Close()
	return r.Next(), nil
}

func (db *DB) integer(query string, values ...any) int {
	var result int
	must(db.handle.QueryRow(query, values...).Scan(&result))
	return result
}

func (db *DB) row(query string, values ...any) (map[string]any, error) {
	r, err := db.handle.Queryx(query, values...)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	if !r.Next() {
		return nil, nil
	}

	row := make(map[string]any)
	if err = r.MapScan(row); err != nil {
		return nil, err
	}

	for i, v := range row {
		if bytes, ok := v.([]byte); ok {
			row[i] = string(bytes)
		}
	}
	return row, nil
}

func (db *DB) rows(query string, values ...any) ([]map[string]any, error) {
	var results []map[string]any

	r, err := db.handle.Queryx(query, values...)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	for r.Next() {
		row := make(map[string]any)
		if err = r.MapScan(row); err != nil {
			return nil, err
		}
		for i, v := range row {
			if bytes, ok := v.([]byte); ok {
				row[i] = string(bytes)
			}
		}
		results = append(results, row)
	}
	return results, nil
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

func (db *DB) scans(out any, query string, values ...any) error {
	return db.handle.Select(out, query, values...)
}

func (db *DB) schema(version int) {
	db.exec("create table if not exists _settings ( name text not null primary key, value text not null )")
	db.exec("replace into _settings ( name, value ) values ( 'schema', ? )", version)
}

// mochi.db.exists/row/query(sql, params...) -> bool/dict/list: Execute database query
func api_db_query(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 {
		return sl_error(fn, "syntax: <SQL statement: string>, [parameters: variadic strings]")
	}

	query, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid SQL statement %q", query)
	}

	as := sl_decode(args[1:]).([]any)

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	app := t.Local("app").(*App)
	if app == nil {
		return sl_error(fn, "unknown app")
	}

	db := db_app(user, app.active)

	switch fn.Name() {
	case "mochi.db.exists":
		exists, err := db.exists(query, as...)
		if err != nil {
			return sl_error(fn, "database error: %v", err)
		}
		if exists {
			return sl.True, nil
		}
		return sl.False, nil

	case "mochi.db.row":
		row, err := db.row(query, as...)
		if err != nil {
			return sl_error(fn, "database error: %v", err)
		}
		return sl_encode(row), nil

	case "mochi.db.query":
		rows, err := db.rows(query, as...)
		if err != nil {
			return sl_error(fn, "database error: %v", err)
		}
		return sl_encode(rows), nil
	}

	return sl_error(fn, "invalid database query %q", fn.Name())
}

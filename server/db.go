// Mochi server: Database
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"
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
	schema_version = 20
)

var (
	databases      = map[string]*DB{}
	databases_lock sync.Mutex

	api_db = sls.FromStringDict(sl.String("mochi.db"), sl.StringDict{
		"exists": sl.NewBuiltin("mochi.db.exists", api_db_query),
		"query":  sl.NewBuiltin("mochi.db.query", api_db_query),
		"row":    sl.NewBuiltin("mochi.db.row", api_db_query),
	})

	// Pattern to detect modifications to system tables (starting with _)
	system_table_pattern = regexp.MustCompile(`(?i)(insert\s+(or\s+\w+\s+)?into|replace\s+into|update|delete\s+from|drop\s+(table|index|trigger)|alter\s+table|create\s+(table|index|trigger))\s+_`)
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
	users.exec("create table users (id integer primary key, username text not null, role text not null default 'user', methods text not null default 'email', status text not null default 'active')")
	users.exec("create unique index users_username on users (username)")

	// Passkey credentials
	users.exec("create table credentials (id blob primary key, user integer not null references users(id) on delete cascade, public_key blob not null, sign_count integer not null default 0, name text not null default '', transports text not null default '', backup_eligible integer not null default 0, backup_state integer not null default 0, created integer not null, last_used integer not null default 0)")
	users.exec("create index credentials_user on credentials(user)")

	// Recovery codes
	users.exec("create table recovery (id integer primary key, user integer not null references users(id) on delete cascade, hash text not null, created integer not null)")
	users.exec("create index recovery_user on recovery(user)")

	// TOTP secrets
	users.exec("create table totp (user integer primary key references users(id) on delete cascade, secret text not null, verified integer not null default 0, created integer not null)")

	// Entities
	users.exec("create table entities ( id text not null primary key, private text not null, fingerprint text not null, user references users( id ), parent text not null default '', class text not null, name text not null, privacy text not null default 'public', data text not null default '', published integer not null default 0 )")
	users.exec("create index entities_fingerprint on entities( fingerprint )")
	users.exec("create index entities_user on entities( user )")
	users.exec("create index entities_parent on entities( parent )")
	users.exec("create index entities_class on entities( class )")
	users.exec("create index entities_name on entities( name )")
	users.exec("create index entities_privacy on entities( privacy )")
	users.exec("create index entities_published on entities( published )")

	// Sessions (login codes and sessions - transient auth data)
	sessions := db_open("db/sessions.db")
	sessions.exec("create table codes ( code text not null, username text not null, expires integer not null, primary key ( code, username ) )")
	sessions.exec("create index codes_expires on codes( expires )")
	sessions.exec("create table sessions (user integer not null, code text not null, secret text not null default '', expires integer not null, created integer not null default 0, accessed integer not null default 0, address text not null default '', agent text not null default '', primary key (user, code))")
	sessions.exec("create unique index sessions_code on sessions(code)")
	sessions.exec("create index sessions_expires on sessions(expires)")
	sessions.exec("create index sessions_user on sessions(user)")

	// WebAuthn ceremony sessions (temporary)
	sessions.exec("create table ceremonies (id text primary key, type text not null, user integer, challenge blob not null, data text not null default '', expires integer not null)")
	sessions.exec("create index ceremonies_expires on ceremonies(expires)")

	// Partial authentication sessions (for MFA)
	sessions.exec("create table partial (id text primary key, user integer not null, completed text not null default '', remaining text not null, expires integer not null)")
	sessions.exec("create index partial_expires on partial(expires)")

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
	queue.exec("create table if not exists queue ( id text primary key, type text not null default 'direct', target text not null, from_entity text not null, to_entity text not null, service text not null, event text not null, content blob not null default '', data blob not null default '', file text not null default '', expires integer not null default 0, status text not null default 'pending', attempts integer not null default 0, next_retry integer not null, last_error text not null default '', created integer not null )")
	queue.exec("create index if not exists queue_status_retry on queue (status, next_retry)")
	queue.exec("create index if not exists queue_target on queue (target)")

	// Domains
	domains := db_open("db/domains.db")
	domains.exec("create table if not exists domains (domain text primary key, verified integer not null default 0, token text not null default '', tls integer not null default 1, created integer not null, updated integer not null)")
	domains.exec("create table if not exists routes (domain text not null, path text not null default '', method text not null default 'app', target text not null, context text not null default '', priority integer not null default 0, enabled integer not null default 1, created integer not null, updated integer not null, primary key (domain, path), foreign key (domain) references domains(domain) on delete cascade)")
	domains.exec("create index if not exists routes_domain on routes(domain)")
	domains.exec("create table if not exists delegations (id integer primary key, domain text not null, path text not null, owner integer not null, created integer not null, updated integer not null, unique(domain, path, owner), foreign key (domain) references domains(domain) on delete cascade)")
	domains.exec("create index if not exists delegations_domain on delegations(domain)")
	domains.exec("create index if not exists delegations_owner on delegations(owner)")
}

// db_user opens a database in the user's directory
func db_user(u *User, name string) *DB {
	path := fmt.Sprintf("users/%d/%s.db", u.ID, name)
	db := db_open(path)

	// Create preferences table for settings.db
	if name == "settings" {
		db.exec("create table if not exists preferences (name text primary key, value text not null)")
	}

	return db
}

// Maximum database size per app per user (1GB / 4KB page size = 262144 pages)
const db_max_page_count = 262144

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

	// Limit database size to prevent misbehaving apps from filling storage
	db.exec(fmt.Sprintf("PRAGMA max_page_count = %d", db_max_page_count))

	if reused {
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
			// Note: logins table is renamed to sessions in schema 11.
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
			queue.exec("create table if not exists queue ( id text primary key, type text not null default 'direct', target text not null, from_entity text not null, to_entity text not null, service text not null, event text not null, content blob not null default '', data blob not null default '', file text not null default '', status text not null default 'pending', attempts integer not null default 0, next_retry integer not null, last_error text not null default '', created integer not null )")
			queue.exec("create index if not exists queue_status_retry on queue (status, next_retry)")
			queue.exec("create index if not exists queue_target on queue (target)")

		} else if schema == 4 {
			// Migration: previously added invites table, now removed in schema 15

		} else if schema == 5 {
			// Migration: add expires column to queue table for message expiration
			queue := db_open("db/queue.db")
			queue.exec("alter table queue add column expires integer not null default 0")

		} else if schema == 6 {
			// Migration: create domains.db and migrate config from mochi.conf
			domains := db_open("db/domains.db")
			domains.exec("create table if not exists domains (domain text primary key, verified integer not null default 0, token text not null default '', tls integer not null default 1, created integer not null, updated integer not null)")
			domains.exec("create table if not exists routes (domain text not null, path text not null default '', entity text not null, priority integer not null default 0, enabled integer not null default 1, created integer not null, updated integer not null, primary key (domain, path), foreign key (domain) references domains(domain) on delete cascade)")
			domains.exec("create index if not exists routes_domain on routes(domain)")
			domains.exec("create table if not exists delegations (id integer primary key, domain text not null, path text not null, owner integer not null, created integer not null, updated integer not null, unique(domain, path, owner), foreign key (domain) references domains(domain) on delete cascade)")
			domains.exec("create index if not exists delegations_domain on delegations(domain)")
			domains.exec("create index if not exists delegations_owner on delegations(owner)")
			domains_migrate_config()

		} else if schema == 7 {
			// Migration: add delegations table, simplify domains and routes tables
			db := db_open("db/domains.db")
			db.exec("create table if not exists delegations (id integer primary key, domain text not null, path text not null, owner integer not null, created integer not null, updated integer not null, unique(domain, path, owner), foreign key (domain) references domains(domain) on delete cascade)")
			db.exec("create index if not exists delegations_domain on delegations(domain)")
			db.exec("create index if not exists delegations_owner on delegations(owner)")
			// Simplify domains table (remove delegation columns)
			db.exec("create table domains_new (domain text primary key, verified integer not null default 0, token text not null default '', tls integer not null default 1, created integer not null, updated integer not null)")
			db.exec("insert into domains_new select domain, verified, token, tls, created, updated from domains")
			db.exec("drop table domains")
			db.exec("alter table domains_new rename to domains")
			// Simplify routes table (remove app, target columns)
			db.exec("create table routes_new (domain text not null, path text not null default '', entity text not null, priority integer not null default 0, enabled integer not null default 1, created integer not null, updated integer not null, primary key (domain, path), foreign key (domain) references domains(domain) on delete cascade)")
			db.exec("insert into routes_new select domain, path, entity, priority, enabled, created, updated from routes")
			db.exec("drop table routes")
			db.exec("alter table routes_new rename to routes")
			db.exec("create index if not exists routes_domain on routes(domain)")

		} else if schema == 8 {
			// Migration: make nullable columns in queue table not null
			queue := db_open("db/queue.db")
			queue.exec("update queue set content = '' where content is null")
			queue.exec("update queue set data = '' where data is null")
			queue.exec("update queue set file = '' where file is null")
			queue.exec("update queue set last_error = '' where last_error is null")
			queue.exec("create table queue_new ( id text primary key, type text not null default 'direct', target text not null, from_entity text not null, to_entity text not null, service text not null, event text not null, content blob not null default '', data blob not null default '', file text not null default '', expires integer not null default 0, status text not null default 'pending', attempts integer not null default 0, next_retry integer not null, last_error text not null default '', created integer not null )")
			queue.exec("insert into queue_new select * from queue")
			queue.exec("drop table queue")
			queue.exec("alter table queue_new rename to queue")
			queue.exec("create index queue_status_retry on queue (status, next_retry)")
			queue.exec("create index queue_target on queue (target)")

		} else if schema == 9 {
			// Add context field to routes
			domains := db_open("db/domains.db")
			domains.exec("alter table routes add column context text not null default ''")

		} else if schema == 10 {
			// Migrate language and timezone from users table to user preferences
			users := db_open("db/users.db")
			rows, _ := users.rows("select id, language, timezone from users where language != 'en' or timezone != 'UTC'")
			for _, row := range rows {
				id := int(row["id"].(int64))
				prefs := db_open(fmt.Sprintf("users/%d/settings.db", id))
				prefs.exec("create table if not exists preferences (name text primary key, value text not null)")
				if lang, ok := row["language"].(string); ok && lang != "en" {
					prefs.exec("replace into preferences (name, value) values ('language', ?)", lang)
				}
				if tz, ok := row["timezone"].(string); ok && tz != "UTC" {
					prefs.exec("replace into preferences (name, value) values ('timezone', ?)", tz)
				}
			}
			// Drop language and timezone columns by rebuilding table
			users.exec("create table users_new (id integer primary key, username text not null, role text not null default 'user')")
			users.exec("insert into users_new select id, username, role from users")
			users.exec("drop table users")
			users.exec("alter table users_new rename to users")
			users.exec("create unique index users_username on users (username)")

		} else if schema == 11 {
			// Migration: rename logins table to sessions, add session metadata columns
			users := db_open("db/users.db")
			has_logins, _ := users.exists("select name from sqlite_master where type='table' and name='logins'")
			if has_logins {
				users.exec("alter table logins rename to sessions")
				users.exec("drop index if exists logins_code")
				users.exec("drop index if exists logins_expires")
				users.exec("create unique index sessions_code on sessions(code)")
				users.exec("create index sessions_expires on sessions(expires)")
				users.exec("create index sessions_user on sessions(user)")
				users.exec("alter table sessions add column created integer not null default 0")
				users.exec("alter table sessions add column accessed integer not null default 0")
				users.exec("alter table sessions add column address text not null default ''")
				users.exec("alter table sessions add column agent text not null default ''")
				// Backfill created timestamp from expires (expires = created + 1 year)
				users.exec("update sessions set created = expires - 31536000 where created = 0")
			}

		} else if schema == 12 {
			// Migration: rename domains_registration to domains_signup
			settings := db_open("db/settings.db")
			settings.exec("update settings set name='domains_signup' where name='domains_registration'")

		} else if schema == 13 {
			// Migration: drop unused name column from sessions table
			users := db_open("db/users.db")
			users.exec("alter table sessions drop column name")

		} else if schema == 14 {
			// Migration: remove unused domains_signup setting
			settings := db_open("db/settings.db")
			settings.exec("delete from settings where name in ('domains_signup', 'domains_registration')")

		} else if schema == 15 {
			// Migration: move sessions and codes from users.db to sessions.db
			// Also removes unused invites table
			// This isolates hot auth tables from critical user/entity data
			users := db_open("db/users.db")
			sessions := db_open("db/sessions.db")

			// Create tables in sessions.db
			sessions.exec("create table codes ( code text not null, username text not null, expires integer not null, primary key ( code, username ) )")
			sessions.exec("create index codes_expires on codes( expires )")
			sessions.exec("create table sessions (user integer not null, code text not null, secret text not null default '', expires integer not null, created integer not null default 0, accessed integer not null default 0, address text not null default '', agent text not null default '', primary key (user, code))")
			sessions.exec("create unique index sessions_code on sessions(code)")
			sessions.exec("create index sessions_expires on sessions(expires)")
			sessions.exec("create index sessions_user on sessions(user)")

			// Copy codes
			codes, _ := users.rows("select code, username, expires from codes")
			for _, c := range codes {
				sessions.exec("insert into codes (code, username, expires) values (?, ?, ?)", c["code"], c["username"], c["expires"])
			}

			// Copy sessions
			sess, _ := users.rows("select user, code, secret, expires, created, accessed, address, agent from sessions")
			for _, s := range sess {
				sessions.exec("insert into sessions (user, code, secret, expires, created, accessed, address, agent) values (?, ?, ?, ?, ?, ?, ?, ?)",
					s["user"], s["code"], s["secret"], s["expires"], s["created"], s["accessed"], s["address"], s["agent"])
			}

			// Drop old tables (including unused invites)
			users.exec("drop table codes")
			users.exec("drop table sessions")
			users.exec("drop table if exists invites")

		} else if schema == 16 {
			// Migration: add multi-factor authentication tables
			users := db_open("db/users.db")
			sessions := db_open("db/sessions.db")

			// Add methods column to users table
			users.exec("alter table users add column methods text not null default 'email'")

			// Add passkey credentials table
			users.exec("create table credentials (id blob primary key, user integer not null references users(id) on delete cascade, public_key blob not null, sign_count integer not null default 0, name text not null default '', transports text not null default '', created integer not null, last_used integer not null default 0)")
			users.exec("create index credentials_user on credentials(user)")

			// Add recovery codes table
			users.exec("create table recovery (id integer primary key, user integer not null references users(id) on delete cascade, hash text not null, created integer not null)")
			users.exec("create index recovery_user on recovery(user)")

			// Add TOTP secrets table
			users.exec("create table totp (user integer primary key references users(id) on delete cascade, secret text not null, verified integer not null default 0, created integer not null)")

			// Add ceremony sessions table for WebAuthn
			sessions.exec("create table ceremonies (id text primary key, type text not null, user integer, challenge blob not null, data text not null default '', expires integer not null)")
			sessions.exec("create index ceremonies_expires on ceremonies(expires)")

			// Add partial sessions table for MFA
			sessions.exec("create table partial (id text primary key, user integer not null, completed text not null default '', remaining text not null, expires integer not null)")
			sessions.exec("create index partial_expires on partial(expires)")

		} else if schema == 17 {
			// Migration: add user status column for user management
			users := db_open("db/users.db")
			users.exec("alter table users add column status text not null default 'active'")
			// Note: mfa_required was added here but removed in schema 19

		} else if schema == 18 {
			// Migration: add backup flags to passkey credentials
			users := db_open("db/users.db")
			users.exec("alter table credentials add column backup_eligible integer not null default 0")
			users.exec("alter table credentials add column backup_state integer not null default 0")

		} else if schema == 19 {
			// Migration: remove unused mfa_required column
			users := db_open("db/users.db")
			users.exec("alter table users drop column mfa_required")

		} else if schema == 20 {
			// Migration: split routes entity column into method and target
			db := db_open("db/domains.db")
			db.exec("create table routes_new (domain text not null, path text not null default '', method text not null default 'app', target text not null, context text not null default '', priority integer not null default 0, enabled integer not null default 1, created integer not null, updated integer not null, primary key (domain, path), foreign key (domain) references domains(domain) on delete cascade)")
			// Migrate existing routes, parsing entity format (app:name, redirect:url, or entity id)
			rows, _ := db.rows("select domain, path, entity, context, priority, enabled, created, updated from routes")
			for _, r := range rows {
				entity := r["entity"].(string)
				method := "entity"
				target := entity
				if strings.HasPrefix(entity, "app:") {
					method = "app"
					target = strings.TrimPrefix(entity, "app:")
				} else if strings.HasPrefix(entity, "redirect:") {
					method = "redirect"
					target = strings.TrimPrefix(entity, "redirect:")
				}
				db.exec("insert into routes_new (domain, path, method, target, context, priority, enabled, created, updated) values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
					r["domain"], r["path"], method, target, r["context"], r["priority"], r["enabled"], r["created"], r["updated"])
			}
			db.exec("drop table routes")
			db.exec("alter table routes_new rename to routes")
			db.exec("create index routes_domain on routes(domain)")
		}

		setting_set("schema", itoa(int(schema)))
	}

	// Migrate email_from from config to system setting
	if setting_get("email_from", "") == "" {
		if from := ini_string("email", "from", ""); from != "" {
			info("Migrating email_from setting from config to system settings")
			setting_set("email_from", from)
		}
	}
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

	// Block modifications to system tables (starting with _)
	if system_table_pattern.MatchString(query) {
		return sl_error(fn, "cannot modify system tables")
	}

	as := sl_decode(args[1:]).([]any)

	// Get user or fall back to owner for anonymous entity-context access
	user := t.Local("user").(*User)
	if user == nil {
		user = t.Local("owner").(*User)
	}
	if user == nil {
		return sl_error(fn, "no user")
	}

	app := t.Local("app").(*App)
	if app == nil {
		return sl_error(fn, "unknown app")
	}

	db := db_app(user, app.active)
	if db == nil {
		return sl_error(fn, "app has no database configured")
	}

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

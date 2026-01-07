// Mochi server: Database
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"database/sql"
	"fmt"
	"regexp"
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
	schema_version = 25
)

var (
	databases      = map[string]*DB{}
	databases_lock sync.Mutex

	api_db = sls.FromStringDict(sl.String("mochi.db"), sl.StringDict{
		"execute": sl.NewBuiltin("mochi.db.execute", api_db_query),
		"exists":  sl.NewBuiltin("mochi.db.exists", api_db_query),
		"row":     sl.NewBuiltin("mochi.db.row", api_db_query),
		"rows":    sl.NewBuiltin("mochi.db.rows", api_db_query),
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

	// API tokens (app-scoped)
	users.exec("create table tokens (hash text primary key not null, user integer not null references users(id) on delete cascade, app text not null, name text not null default '', scopes text not null default '', created integer not null, used integer not null default 0, expires integer not null default 0)")
	users.exec("create index tokens_user on tokens(user)")
	users.exec("create index tokens_app on tokens(app)")

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
	directory.exec("create table directory ( id text not null primary key, name text not null, class text not null, location text not null default '', data text not null default '', created integer not null, updated integer not null )")
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
	// Outgoing message queue
	queue.exec("create table if not exists queue ( id text primary key, type text not null default 'direct', target text not null, from_entity text not null, to_entity text not null, service text not null, event text not null, content blob not null default '', data blob not null default '', file text not null default '', expires integer not null default 0, status text not null default 'pending', attempts integer not null default 0, next_retry integer not null, last_error text not null default '', created integer not null )")
	queue.exec("create index if not exists queue_status_retry on queue (status, next_retry)")
	queue.exec("create index if not exists queue_target on queue (target)")

	// Domains
	domains := db_open("db/domains.db")
	domains.exec("create table if not exists domains (domain text primary key, verified integer not null default 0, token text not null default '', tls integer not null default 1, created integer not null, updated integer not null)")
	domains.exec("create table if not exists routes (domain text not null, path text not null default '', method text not null default 'app', target text not null, context text not null default '', owner integer not null default 0, priority integer not null default 0, enabled integer not null default 1, created integer not null, updated integer not null, primary key (domain, path), foreign key (domain) references domains(domain) on delete cascade)")
	if exists, _ := domains.exists("select 1 from pragma_table_info('routes') where name='owner'"); !exists {
		domains.exec("alter table routes add column owner integer not null default 0")
	}
	domains.exec("create index if not exists routes_domain on routes(domain)")
	domains.exec("create table if not exists delegations (id integer primary key, domain text not null, path text not null, owner integer not null, created integer not null, updated integer not null, unique(domain, path, owner), foreign key (domain) references domains(domain) on delete cascade)")
	domains.exec("create index if not exists delegations_domain on delegations(domain)")
	domains.exec("create index if not exists delegations_owner on delegations(owner)")

	// Apps (for multi-version and user-configurable routing)
	apps := db_open("db/apps.db")
	apps.exec("create table classes (class text not null primary key, app text not null)")
	apps.exec("create table services (service text not null primary key, app text not null)")
	apps.exec("create table paths (path text not null primary key, app text not null)")
	apps.exec("create table versions (app text not null primary key, version text, track text)")
	apps.exec("create table tracks (app text not null, track text not null, version text not null, primary key (app, track))")
	apps.exec("create table apps (app text not null primary key, installed integer not null)")
}

// db_apps opens the apps.db database, creating tables if needed
func db_apps() *DB {
	db := db_open("db/apps.db")
	db.exec("create table if not exists classes (class text not null primary key, app text not null)")
	db.exec("create table if not exists services (service text not null primary key, app text not null)")
	db.exec("create table if not exists paths (path text not null primary key, app text not null)")
	db.exec("create table if not exists versions (app text not null primary key, version text not null default '', track text not null default '')")
	db.exec("create table if not exists tracks (app text not null, track text not null, version text not null, primary key (app, track))")
	db.exec("create table if not exists apps (app text not null primary key, installed integer not null)")
	return db
}

// db_user opens a database in the user's directory
func db_user(u *User, name string) *DB {
	path := fmt.Sprintf("users/%d/%s.db", u.ID, name)
	db := db_open(path)

	// Create tables for user.db
	if name == "user" {
		db.exec("create table if not exists preferences (name text primary key, value text not null)")
		db.groups_setup()
		db.permissions_setup()

		// App preferences (for multi-version and user-configurable routing)
		db.exec("create table if not exists classes (class text not null primary key, app text not null)")
		db.exec("create table if not exists services (service text not null primary key, app text not null)")
		db.exec("create table if not exists paths (path text not null primary key, app text not null)")
		db.exec("create table if not exists versions (app text not null primary key, version text not null default '', track text not null default '')")
	}

	return db
}

// Maximum database size per app per user (1GB / 4KB page size = 262144 pages)
const db_max_page_count = 262144

// Open a database file for an app, creating, upgrading, or downgrading it as necessary
func db_app(u *User, app *App) *DB {
	av := app.active(u)
	if av == nil {
		warn("Attempt to create database for app with no version loaded")
		return nil
	}

	if av.Database.File == "" {
		warn("App %q asked for database, but no database file specified", app.id)
		return nil
	}

	path := fmt.Sprintf("users/%d/%s/%s", u.ID, app.id, av.Database.File)
	db, _, reused := db_open_work(path)
	db.user = u

	// Limit database size to prevent misbehaving apps from filling storage
	db.exec(fmt.Sprintf("PRAGMA max_page_count = %d", db_max_page_count))

	if reused {
		return db
	}

	// Lock everything below here to prevent race conditions when modifying the schema
	l := lock(path)
	l.Lock()
	defer l.Unlock()

	// Check if _settings table exists, if not create it with schema 0
	has_settings, _ := db.exists("select name from sqlite_master where type='table' and name='_settings'")
	if !has_settings {
		db.schema(0)
	}

	schema := db.integer("select cast(value as integer) from _settings where name='schema'")

	// Check if app tables exist - if not, call database_create()
	// We always check actual database state rather than relying on file creation status,
	// because multiple goroutines may race to create the same database file.
	// Note: Use ESCAPE to treat underscore literally (it's a LIKE wildcard otherwise)
	has_tables, _ := db.exists("select name from sqlite_master where type='table' and name not like '\\_%' escape '\\'")
	if !has_tables {
		debug("Database app creating %q", path)

		if av.Database.Create.Function != "" {
			if err := av.starlark_db(u, av.Database.Create.Function, nil); err != nil {
				warn("App %q version %q database create error: %v", av.app.id, av.Version, err)
				return nil
			}
		} else if av.Database.create_function != nil {
			av.Database.create_function(db)
		} else {
			warn("App %q version %q has no way to create database file %q", av.app.id, av.Version, av.Database.File)
			return nil
		}
		db.schema(av.Database.Schema)
		schema = av.Database.Schema
	}

	if schema < av.Database.Schema && av.Database.Upgrade.Function != "" {
		for version := schema + 1; version <= av.Database.Schema; version++ {
			debug("Database %q upgrading to schema version %d", path, version)
			if err := av.starlark_db(u, av.Database.Upgrade.Function, sl_encode_tuple(version)); err != nil {
				warn("App %q version %q database upgrade error: %v", av.app.id, av.Version, err)
			}
			db.schema(version)
		}
	} else if schema > av.Database.Schema && av.Database.Downgrade.Function != "" {
		for version := schema; version > av.Database.Schema; version-- {
			debug("Database %q downgrading from schema version %d", path, version)
			if err := av.starlark_db(u, av.Database.Downgrade.Function, sl_encode_tuple(version)); err != nil {
				warn("App %q version %q database downgrade error: %v", av.app.id, av.Version, err)
			}
			db.schema(version - 1)
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

	if schema < 20 {
		panic(fmt.Sprintf("Database schema version %d is too old to upgrade. Minimum supported version is 20.", schema))
	}

	for schema < schema_version {
		schema++
		info("Upgrading database schema to version %d", schema)

		if schema == 21 {
			// Migration: rename per-user settings.db to user.db
			users := db_open("db/users.db")
			rows, _ := users.rows("select id from users")
			for _, r := range rows {
				id := int(r["id"].(int64))
				old_path := fmt.Sprintf("%s/users/%d/settings.db", data_dir, id)
				new_path := fmt.Sprintf("%s/users/%d/user.db", data_dir, id)
				if file_exists(old_path) && !file_exists(new_path) {
					file_move(old_path, new_path)
					// Also move WAL and SHM files if they exist
					if file_exists(old_path + "-wal") {
						file_move(old_path+"-wal", new_path+"-wal")
					}
					if file_exists(old_path + "-shm") {
						file_move(old_path+"-shm", new_path+"-shm")
					}
				}
			}
		}

		if schema == 22 {
			// Migration: add API tokens table
			users := db_open("db/users.db")
			users.exec("create table if not exists tokens (hash text primary key not null, user integer not null references users(id) on delete cascade, name text not null default '', scopes text not null default '', expires text not null default '', created text not null default '', last_used text not null default '')")
			users.exec("create index if not exists tokens_user on tokens(user)")
		}

		if schema == 23 {
			// Migration: default app permissions (now handled lazily by app_user_init)
		}

		if schema == 24 {
			// Migration: recreate tokens table with app scoping and integer timestamps
			users := db_open("db/users.db")
			users.exec("drop table if exists tokens")
			users.exec("create table tokens (hash text primary key not null, user integer not null references users(id) on delete cascade, app text not null, name text not null default '', scopes text not null default '', created integer not null, used integer not null default 0, expires integer not null default 0)")
			users.exec("create index tokens_user on tokens(user)")
			users.exec("create index tokens_app on tokens(app)")

			// Migration: rename permissions/manage to permission/manage in all user databases
			rows, _ := users.rows("select id from users")
			for _, row := range rows {
				if id, ok := row["id"].(int64); ok {
					udb := db_open(fmt.Sprintf("users/%d/user.db", id))
					udb.exec("update permissions set permission='permission/manage' where permission='permissions/manage'")
				}
			}
		}

		if schema == 25 {
			// Migration: recalculate entity fingerprints using SHA256 (was SHA1)
			users := db_open("db/users.db")
			erows, _ := users.rows("select id from entities")
			for _, erow := range erows {
				if eid, ok := erow["id"].(string); ok {
					fp := fingerprint(eid)
					users.exec("update entities set fingerprint=? where id=?", fp, eid)
				}
			}

			// Remove fingerprint column from directory (now computed on the fly)
			dir := db_open("db/directory.db")
			dir.exec("create table directory_new ( id text not null primary key, name text not null, class text not null, location text not null default '', data text not null default '', created integer not null, updated integer not null )")
			dir.exec("insert into directory_new (id, name, class, location, data, created, updated) select id, name, class, location, data, created, updated from directory")
			dir.exec("drop table directory")
			dir.exec("alter table directory_new rename to directory")
			dir.exec("create index directory_name on directory(name)")
			dir.exec("create index directory_class on directory(class)")
			dir.exec("create index directory_location on directory(location)")
			dir.exec("create index directory_created on directory(created)")
			dir.exec("create index directory_updated on directory(updated)")
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

// integer returns the first column as an integer, or 0 on error
func (db *DB) integer(query string, values ...any) int {
	var result int
	err := db.handle.QueryRow(query, values...).Scan(&result)
	if err != nil {
		return 0
	}
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

// mochi.db.execute/exists/query/row/rows(sql, params...) -> nil/bool/list/dict/list: Execute database query
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

	// For entity context, use owner's database (entity data lives there).
	// For class context, use authenticated user's database.
	owner := t.Local("owner").(*User)
	user := t.Local("user").(*User)
	db_user := owner
	if db_user == nil {
		db_user = user
	}
	if db_user == nil {
		return sl_error(fn, "no user")
	}

	app := t.Local("app").(*App)
	if app == nil {
		return sl_error(fn, "unknown app")
	}

	db := db_app(db_user, app)
	if db == nil {
		return sl_error(fn, "app has no database configured")
	}

	switch fn.Name() {
	case "mochi.db.execute":
		_, err := db.handle.Exec(query, as...)
		if err != nil {
			return sl_error(fn, "database error: %v", err)
		}
		return sl.None, nil

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

	case "mochi.db.rows":
		rows, err := db.rows(query, as...)
		if err != nil {
			return sl_error(fn, "database error: %v", err)
		}
		return sl_encode(rows), nil
	}

	return sl_error(fn, "invalid database query %q", fn.Name())
}

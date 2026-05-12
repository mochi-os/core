// Mochi server: Database
// Copyright Alistair Cunningham 2024-2026

package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/ncruces/go-sqlite3"
	sqlitedrv "github.com/ncruces/go-sqlite3/driver"
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

// DB carries two connection pools per SQLite file. internal has no
// authoriser and is used for all server-trusted queries (schema migrations,
// PRAGMA reads, mochi.db.table/indexes etc). starlark has a strict
// authoriser that denies ATTACH/DETACH/PRAGMA/triggers/vtables and is used
// only for SQL strings supplied by Starlark via api_db_query and
// api_db_transaction.
type DB struct {
	key      string
	path     string
	internal *sqlx.DB
	starlark *sqlx.DB
	user     *User
	closed   int64
}

const (
	schema_version = 52
)

var (
	databases      = map[string]*DB{}
	databases_lock sync.Mutex

	api_db = sls.FromStringDict(sl.String("mochi.db"), sl.StringDict{
		"commit":      api_commit,
		"execute":     sl.NewBuiltin("mochi.db.execute", api_db_query),
		"exists":      sl.NewBuiltin("mochi.db.exists", api_db_query),
		"row":         sl.NewBuiltin("mochi.db.row", api_db_query),
		"rows":        sl.NewBuiltin("mochi.db.rows", api_db_query),
		"indexes":     sl.NewBuiltin("mochi.db.indexes", api_db_indexes),
		"table":       sl.NewBuiltin("mochi.db.table", api_db_table),
		"tables":      sl.NewBuiltin("mochi.db.tables", api_db_tables),
		"transaction": sl.NewBuiltin("mochi.db.transaction", api_db_transaction),
	})
)

// db_setup_conn runs the per-connection PRAGMAs that configure WAL,
// foreign keys, and the per-DB size cap. It runs on every fresh
// connection in either pool, before any query.
func db_setup_conn(c *sqlite3.Conn) error {
	if err := c.Exec("PRAGMA journal_mode=WAL"); err != nil {
		return err
	}
	if err := c.Exec("PRAGMA foreign_keys=ON"); err != nil {
		return err
	}
	return c.Exec(fmt.Sprintf("PRAGMA max_page_count = %d", db_max_page_count))
}

// db_setup_conn_starlark wraps db_setup_conn and additionally installs
// the Starlark-pool authoriser, which blocks any operation an
// untrusted app shouldn't perform on its own DB.
func db_setup_conn_starlark(c *sqlite3.Conn) error {
	if err := db_setup_conn(c); err != nil {
		return err
	}
	return c.SetAuthorizer(db_authorise_starlark)
}

// db_authorise_starlark is the authoriser callback for connections that
// will execute SQL strings supplied by Starlark code. It denies the
// operations apps must not perform on their per-user DB:
//
//   - ATTACH / DETACH: would let an app peek at or write to other DBs
//     in the same process.
//   - PRAGMA *with argument*: would let an app override server quotas
//     (`max_page_count = N`), journal mode, schema version, etc.
//     Read-only PRAGMA queries (no argument, e.g. `PRAGMA query_only`)
//     are allowed because ncruces' database/sql connector runs
//     `PRAGMA query_only` after our ConnectHook to detect read-only
//     mode, and denying it would break every connection on this pool.
//     Apps gain a small info leak (they can read pragma values they
//     wouldn't otherwise see) but cannot change behaviour.
//   - Triggers (CREATE / DROP, persistent and TEMP): would silently
//     fire on every write and burn CPU. No current app needs them.
//   - Virtual tables (CREATE / DROP): wrap arbitrary modules outside
//     the sandbox model. Built-in pragma_* virtual table reads go via
//     SQLITE_READ, which is unaffected.
//
// VACUUM and ANALYZE have no authoriser action codes and are caught
// by the string-prefix check in api_db_query / transaction_args
// instead.
func db_authorise_starlark(action sqlite3.AuthorizerActionCode, _, name4th, _, _ string) sqlite3.AuthorizerReturnCode {
	switch action {
	case sqlite3.AUTH_ATTACH, sqlite3.AUTH_DETACH,
		sqlite3.AUTH_CREATE_TRIGGER, sqlite3.AUTH_CREATE_TEMP_TRIGGER,
		sqlite3.AUTH_DROP_TRIGGER, sqlite3.AUTH_DROP_TEMP_TRIGGER,
		sqlite3.AUTH_CREATE_VTABLE, sqlite3.AUTH_DROP_VTABLE:
		return sqlite3.AUTH_DENY
	case sqlite3.AUTH_PRAGMA:
		// For PRAGMA, name4th is the pragma's argument (empty string
		// when no argument — i.e. a read query). Allow reads, deny
		// writes/calls-with-args. The connector's `PRAGMA query_only`
		// check has no argument and so survives this rule.
		if name4th != "" {
			return sqlite3.AUTH_DENY
		}
	}
	return sqlite3.AUTH_OK
}

func db_create() {
	info("Creating new database")

	// Settings
	settings := db_open("db/settings.db")
	settings.exec("create table settings ( name text not null primary key, value text not null )")
	settings.exec("replace into settings ( name, value ) values ( 'schema', ? )", schema_version)

	// Documents: operator-customisable Markdown for server rules / terms / privacy.
	// Bundled defaults live in core/server/documents/ (embedded); this table
	// holds only operator overrides keyed by (name, language).
	settings.exec("create table documents ( name text not null, language text not null, body text not null, updated integer not null, primary key ( name, language ) )")

	// Users. `uid` is the globally-stable identifier used for replication
	// and cross-host data references. The integer `id` is the per-host
	// disk-path identifier (`users/<int>/`) and stays as the primary key.
	users := db_open("db/users.db")
	users.exec("create table users (id integer primary key, uid text not null default '', username text not null, role text not null default 'user', methods text not null default 'email', status text not null default 'active')")
	users.exec("create unique index users_username on users (username)")
	users.exec("create unique index users_uid on users (uid)")
	users.exec(`create trigger users_uid_insert after insert on users
		when new.uid is null or new.uid = ''
		begin
			update users set uid = lower(hex(randomblob(16))) where id = new.id;
		end`)

	// Passkey credential definitions and sign count. Sign count is WebAuthn
	// replay-prevention state and lives here so it survives sessions.db
	// corruption. Only the cosmetic last-used timestamp lives in sessions.db.
	users.exec("create table credentials (id blob primary key, user integer not null references users(id) on delete cascade, user_uid text not null default '', public_key blob not null, sign_count integer not null default 0, name text not null default '', transports text not null default '', backup_eligible integer not null default 0, backup_state integer not null default 0, created integer not null)")
	users.exec("create index credentials_user on credentials(user)")
	users.exec("create index credentials_user_uid on credentials(user_uid)")

	// Recovery codes
	users.exec("create table recovery (id integer primary key, user integer not null references users(id) on delete cascade, user_uid text not null default '', hash text not null, created integer not null)")
	users.exec("create index recovery_user on recovery(user)")
	users.exec("create index recovery_user_uid on recovery(user_uid)")

	// TOTP secrets
	users.exec("create table totp (user integer primary key references users(id) on delete cascade, user_uid text not null default '', secret text not null, verified integer not null default 0, created integer not null)")
	users.exec("create index totp_user_uid on totp(user_uid)")

	// OAuth identity definitions (Google, GitHub, Microsoft, Facebook, X).
	// Last-used timestamp lives in sessions.db.verifications so this cold
	// reference store doesn't take a write on every OAuth login.
	users.exec("create table oauth (id integer primary key, user integer not null references users(id) on delete cascade, user_uid text not null default '', provider text not null, subject text not null, email text not null default '', verified integer not null default 0, name text not null default '', created integer not null, unique(provider, subject))")
	users.exec("create index oauth_user on oauth(user)")
	users.exec("create index oauth_user_uid on oauth(user_uid)")

	// API token definitions. Hot per-request "used" timestamp lives in
	// sessions.db.accesses; here we keep just the definition so token loss
	// doesn't follow sessions.db corruption.
	users.exec("create table tokens (hash text primary key not null, user integer not null references users(id) on delete cascade, user_uid text not null default '', app text not null, name text not null default '', scopes text not null default '', created integer not null, expires integer not null default 0)")
	users.exec("create index tokens_user on tokens(user)")
	users.exec("create index tokens_user_uid on tokens(user_uid)")
	users.exec("create index tokens_app on tokens(app)")

	// Entities
	users.exec("create table entities ( id text not null primary key, private text not null, fingerprint text not null, user references users( id ), user_uid text not null default '', parent text not null default '', class text not null, name text not null, privacy text not null default 'public', data text not null default '', published integer not null default 0 )")
	users.exec("create index entities_fingerprint on entities( fingerprint )")
	users.exec("create index entities_user on entities( user )")
	users.exec("create index entities_user_uid on entities( user_uid )")
	users.exec("create index entities_parent on entities( parent )")
	users.exec("create index entities_class on entities( class )")
	users.exec("create index entities_name on entities( name )")
	users.exec("create index entities_privacy on entities( privacy )")
	users.exec("create index entities_published on entities( published )")

	// Dual-write triggers: every insert / FK update on a user-referencing
	// table copies users.uid into the parallel user_uid column. Lets the
	// codebase migrate to UID-based reads incrementally without splitting
	// every existing INSERT site.
	for _, tbl := range []string{"entities", "credentials", "recovery", "totp", "oauth", "tokens"} {
		users.exec(fmt.Sprintf(`create trigger %s_user_uid_insert after insert on %s
			when new.user is not null and (new.user_uid is null or new.user_uid = '')
			begin
				update %s set user_uid = coalesce((select uid from users where id = new.user), '') where rowid = new.rowid;
			end`, tbl, tbl, tbl))
		users.exec(fmt.Sprintf(`create trigger %s_user_uid_update after update of user on %s
			when new.user is not null
			begin
				update %s set user_uid = coalesce((select uid from users where id = new.user), '') where rowid = new.rowid;
			end`, tbl, tbl, tbl))
	}

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

	// Last-login timestamps (kept here, not in users.db, so the cold reference
	// store doesn't take a write on every login)
	sessions.exec("create table logins (user integer primary key, last integer not null)")

	// Per-request token access timestamps. Split out of users.db.tokens so the
	// every-request "used" write doesn't land on the cold reference store, but
	// the token definitions themselves stay in users.db so token loss doesn't
	// follow sessions.db corruption. `user` duplicated here for cascade.
	sessions.exec("create table accesses (hash text primary key not null, user integer not null, used integer not null default 0)")
	sessions.exec("create index accesses_user on accesses(user)")

	// Cosmetic last-used timestamp per passkey. Sign count (replay-prevention
	// state) stays in users.db.credentials; only the cosmetic stat lives here.
	sessions.exec("create table passkeys (credential blob primary key, user integer not null, last integer not null default 0)")
	sessions.exec("create index passkeys_user on passkeys(user)")

	// OAuth verification state (last time each linked identity was used to log
	// in). Split from users.db.oauth so per-login writes don't land on the cold
	// reference store. `oauth` references users.db.oauth(id); `user` duplicated
	// here for cascade.
	sessions.exec("create table verifications (oauth integer primary key, user integer not null, last integer not null default 0)")
	sessions.exec("create index verifications_user on verifications(user)")

	// Directory. `entities` holds per-entity metadata (one row); `locations`
	// holds per-(entity, peer) location claims so receivers know every host
	// that has announced itself as a replica. The legacy `location` column
	// on entities is kept one release for rollback safety.
	directory := db_open("db/directory.db")
	directory.exec("create table entities ( id text not null primary key, name text not null, class text not null, location text not null default '', data text not null default '', fingerprint text not null default '', created integer not null, updated integer not null )")
	directory.exec("create index entities_name on entities( name )")
	directory.exec("create index entities_class on entities( class )")
	directory.exec("create index entities_location on entities( location )")
	directory.exec("create index entities_fingerprint on entities( fingerprint )")
	directory.exec("create index entities_created on entities( created )")
	directory.exec("create index entities_updated on entities( updated )")
	directory.exec("create table locations ( entity text not null, peer text not null, seen integer not null, primary key ( entity, peer ) )")
	directory.exec("create index locations_peer on locations( peer )")
	directory.exec("create index locations_seen on locations( seen )")

	// Peers
	peers := db_open("db/peers.db")
	peers.exec("create table peers ( id text not null, address text not null, updated integer not null, primary key ( id, address ) )")

	// Message queue with reliability tracking
	queue := db_open("db/queue.db")
	// Outgoing message queue
	queue.exec("create table if not exists queue ( id text primary key, type text not null default 'direct', target text not null, from_entity text not null, to_entity text not null, service text not null, event text not null, from_app text not null default '', from_services text not null default '', content blob not null default '', data blob not null default '', file text not null default '', expires integer not null default 0, status text not null default 'pending', attempts integer not null default 0, next_retry integer not null, last_error text not null default '', created integer not null )")
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

	// Scheduled events
	schedule := db_open("db/schedule.db")
	schedule.exec("create table schedule (id integer primary key, user int not null, app text not null, due int not null, event text not null, data text not null, interval int not null, created int not null)")
	schedule.exec("create index schedule_due on schedule(due)")
	schedule.exec("create index schedule_app_event on schedule(app, event)")

	// Replication: per-origin-peer dedup, schema-coordination buffer,
	// per-user opt-in set, outbound sequence counters, server-pair members,
	// lease-based leadership with fencing, bulk-bootstrap progress, paired
	// server compatibility tracking. See claude/plans/replication.md.
	replication := db_open("db/replication.db")
	replication.exec("create table seen (peer text not null, scope text not null, user text not null default '', sequence integer not null, applied integer not null, primary key (peer, scope, user, sequence))")
	replication.exec("create index seen_applied on seen(applied)")
	replication.exec("create table pending (peer text not null, scope text not null, user text not null default '', sequence integer not null, schema integer not null default 0, payload blob not null, received integer not null, primary key (peer, scope, user, sequence))")
	replication.exec("create index pending_received on pending(received)")
	replication.exec("create table hosts (user text not null, peer text not null, added integer not null, ack integer not null default 0, primary key (user, peer))")
	replication.exec("create table sequence (user text not null default '', scope text not null, next integer not null default 0, primary key (user, scope))")
	replication.exec("create table pair (peer text primary key, added integer not null, role text not null default '')")
	replication.exec("create table leadership (scope text not null, key text not null, peer text not null, expires integer not null, fence integer not null default 0, primary key (scope, key))")
	replication.exec("create index leadership_expires on leadership(expires)")
	replication.exec("create table fence_witness (scope text not null, key text not null, fence integer not null default 0, peer text not null default '', seen integer not null default 0, primary key (scope, key))")
	replication.exec("create table bootstrap (scope text not null, peer text not null, position text not null default '', primary key (scope, peer))")
	replication.exec("create table schemas (peer text primary key, core integer not null default 0, apps text not null default '')")
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

		// Connected accounts (email, browser push, AI services, MCP)
		db.exec("create table if not exists accounts (id integer primary key, type text not null, label text not null default '', identifier text not null default '', data text not null default '', created integer not null, verified integer not null default 0, enabled integer not null default 1, \"default\" text not null default '', last_delivered integer not null default 0)")
		db.exec("create index if not exists accounts_type on accounts(type)")
		if exists, _ := db.exists("select 1 from pragma_table_info('accounts') where name='last_delivered'"); !exists {
			db.exec("alter table accounts add column last_delivered integer not null default 0")
		}

		// User interest profiles for personalised ranking
		db.exec("create table if not exists interests (qid text not null primary key, weight integer not null default 100, updated integer not null default 0)")

		// Internal key-value settings (Go-only, no Starlark API)
		db.exec("create table if not exists settings (key text not null primary key, text text not null default '', number integer not null default 0)")

	}

	return db
}

// Maximum database size per app per user (1GB / 4KB page size = 262144 pages)
const db_max_page_count = 262144

// db_app opens a database file for an app, creating, upgrading, or downgrading it as necessary.
// App databases are stored in users/{user_id}/{app_id}/db/{file.db}.
// Schema version is tracked using SQLite's user_version pragma.
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

	path := fmt.Sprintf("users/%d/%s/db/%s", u.ID, app.id, av.Database.File)
	key := fmt.Sprintf("%s|%s", filepath.Join(data_dir, path), av.Version)
	db, _, reused := db_open_work(path, key)
	if db == nil {
		return nil
	}
	db.user = u

	if reused {
		return db
	}

	// Lock everything below here to prevent race conditions when modifying the schema
	l := lock(path)
	l.Lock()
	defer l.Unlock()

	// Get schema version from user_version pragma
	schema := db_app_schema_get(db)

	// Check if app tables exist - if not, call database_create()
	// We always check actual database state rather than relying on file creation status,
	// because multiple goroutines may race to create the same database file.
	has_tables, _ := db.exists("select name from sqlite_master where type='table'")
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
		db_app_schema_set(db, av.Database.Schema)
		schema = av.Database.Schema
	}

	if schema < av.Database.Schema && av.Database.Upgrade.Function != "" {
		for version := schema + 1; version <= av.Database.Schema; version++ {
			debug("Database %q upgrading to schema version %d", path, version)
			if err := av.starlark_db(u, av.Database.Upgrade.Function, sl_encode_tuple(version)); err != nil {
				warn("App %q version %q database upgrade error: %v", av.app.id, av.Version, err)
			}
			db_app_schema_set(db, version)
			audit_app_schema_migrated(av.app.id, version-1, version)
		}
	} else if schema > av.Database.Schema && av.Database.Downgrade.Function != "" {
		for version := schema; version > av.Database.Schema; version-- {
			debug("Database %q downgrading from schema version %d", path, version)
			if err := av.starlark_db(u, av.Database.Downgrade.Function, sl_encode_tuple(version)); err != nil {
				warn("App %q version %q database downgrade error: %v", av.app.id, av.Version, err)
			}
			db_app_schema_set(db, version-1)
			audit_app_schema_migrated(av.app.id, version, version-1)
		}
	}

	return db
}

// db_app_system opens the system database (app.db) for an app.
// Contains access and attachments tables managed by the platform.
// Always available even if app has no declared database file.
func db_app_system(u *User, app *App) *DB {
	if u == nil || app == nil {
		return nil
	}

	path := fmt.Sprintf("users/%d/%s/app.db", u.ID, app.id)
	db, _, reused := db_open_work(path)
	if db == nil {
		return nil
	}
	db.user = u

	if reused {
		return db
	}

	l := lock(path)
	l.Lock()
	defer l.Unlock()

	// Create system tables
	db.access_setup()
	db.attachments_setup()

	return db
}

// db_app_schema_get reads the app database schema version from user_version pragma
func db_app_schema_get(db *DB) int {
	return db.integer("pragma user_version")
}

// db_app_schema_set writes the app database schema version to user_version pragma
func db_app_schema_set(db *DB, version int) {
	db.exec(fmt.Sprintf("pragma user_version=%d", version))
}

func db_manager() {
	for range time.Tick(time.Minute) {
		now := now()
		var closers []*sqlx.DB

		databases_lock.Lock()
		for _, db := range databases {
			if db.closed > 0 && db.closed < now-60 {
				closers = append(closers, db.internal, db.starlark)
				delete(databases, db.key)
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

func db_open_work(file string, cacheKeys ...string) (*DB, bool, bool) {
	path := filepath.Join(data_dir, file)
	key := path
	if len(cacheKeys) > 0 && cacheKeys[0] != "" {
		key = cacheKeys[0]
	}

	databases_lock.Lock()
	db, found := databases[key]
	databases_lock.Unlock()
	if found {
		//debug("Database reusing already open %q", path)
		db.closed = 0
		return db, false, true
	}

	created := false
	if !file_exists(path) {
		//debug("Database creating %q", path)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			warn("Database unable to create directory for %q: %v", path, err)
			return db, false, false
		}
		f, err := os.Create(path)
		if err != nil {
			warn("Database unable to create %q: %v", path, err)
			return db, false, false
		}
		f.Close()
		created = true
	}

	//debug("Database opening %q", path)
	internal_db, err := sqlitedrv.Open(path, db_setup_conn)
	if err != nil {
		warn("Database unable to open %q: %v", path, err)
		return nil, false, false
	}
	starlark_db, err := sqlitedrv.Open(path, db_setup_conn_starlark)
	if err != nil {
		internal_db.Close()
		warn("Database unable to open Starlark pool for %q: %v", path, err)
		return nil, false, false
	}
	db = &DB{
		key:      key,
		path:     path,
		internal: sqlx.NewDb(internal_db, "sqlite3"),
		starlark: sqlx.NewDb(starlark_db, "sqlite3"),
	}

	databases_lock.Lock()
	if existing, found := databases[key]; found {
		databases_lock.Unlock()
		db.internal.Close()
		db.starlark.Close()
		existing.closed = 0
		return existing, false, true
	}
	databases[key] = db
	databases_lock.Unlock()

	return db, created, false
}

func db_start() bool {
	if file_exists(filepath.Join(data_dir, "db", "users.db")) {
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

	if schema > schema_version {
		panic(fmt.Sprintf("Database schema version %d is newer than this server supports (version %d). Downgrade is not supported.", schema, schema_version))
	}

	for schema < schema_version {
		next := schema + 1
		info("Upgrading database schema from version %d to %d", schema, next)
		switch next {
		case 43:
			db_upgrade_43()
		case 44:
			db_upgrade_44()
		case 45:
			db_upgrade_45()
		case 46:
			db_upgrade_46()
		case 47:
			db_upgrade_47()
		case 48:
			db_upgrade_48()
		case 49:
			db_upgrade_49()
		case 50:
			db_upgrade_50()
		case 51:
			db_upgrade_51()
		case 52:
			db_upgrade_52()
		default:
			panic(fmt.Sprintf("No upgrade path for schema version %d", next))
		}
		setting_set("schema", fmt.Sprintf("%d", next))
		schema = next
	}
}

// db_upgrade_43 adds the oauth table for third-party login identities.
func db_upgrade_43() {
	users := db_open("db/users.db")
	if exists, _ := users.exists("select 1 from sqlite_master where type='table' and name='oauth'"); !exists {
		users.exec("create table oauth (id integer primary key, user integer not null references users(id) on delete cascade, provider text not null, subject text not null, email text not null default '', verified integer not null default 0, name text not null default '', created integer not null, used integer not null default 0, unique(provider, subject))")
		users.exec("create index oauth_user on oauth(user)")
	}
}

// db_upgrade_44 adds the logins table to sessions.db for last-login
// timestamps that survive logout (the previous lookup via sessions.accessed
// lost the value when login_delete removed the row).
func db_upgrade_44() {
	sessions := db_open("db/sessions.db")
	if exists, _ := sessions.exists("select 1 from sqlite_master where type='table' and name='logins'"); !exists {
		sessions.exec("create table logins (user integer primary key, last integer not null)")
	}
}

// db_upgrade_45 moves the tokens table from users.db to sessions.db so the
// per-request "used" write doesn't land on the cold reference store. Copies
// existing rows then drops the source. Cross-DB FK cascade is lost; user_delete
// now removes tokens explicitly.
func db_upgrade_45() {
	sessions := db_open("db/sessions.db")
	if exists, _ := sessions.exists("select 1 from sqlite_master where type='table' and name='tokens'"); !exists {
		sessions.exec("create table tokens (hash text primary key not null, user integer not null, app text not null, name text not null default '', scopes text not null default '', created integer not null, used integer not null default 0, expires integer not null default 0)")
		sessions.exec("create index tokens_user on tokens(user)")
		sessions.exec("create index tokens_app on tokens(app)")
	}

	users := db_open("db/users.db")
	if exists, _ := users.exists("select 1 from sqlite_master where type='table' and name='tokens'"); exists {
		rows, _ := users.rows("select hash, user, app, name, scopes, created, used, expires from tokens")
		for _, r := range rows {
			sessions.exec("insert or ignore into tokens (hash, user, app, name, scopes, created, used, expires) values (?, ?, ?, ?, ?, ?, ?, ?)",
				r["hash"], r["user"], r["app"], r["name"], r["scopes"], r["created"], r["used"], r["expires"])
		}
		users.exec("drop table tokens")
	}
}

// db_upgrade_46 splits passkey activity (sign count + last used) out of
// users.db.credentials into sessions.db.passkeys, so per-assertion writes
// don't land on the cold reference store. Copies existing values then drops
// the columns from credentials.
func db_upgrade_46() {
	sessions := db_open("db/sessions.db")
	if exists, _ := sessions.exists("select 1 from sqlite_master where type='table' and name='passkeys'"); !exists {
		sessions.exec("create table passkeys (credential blob primary key, user integer not null, count integer not null default 0, last integer not null default 0)")
		sessions.exec("create index passkeys_user on passkeys(user)")
	}

	users := db_open("db/users.db")
	if exists, _ := users.exists("select 1 from pragma_table_info('credentials') where name='sign_count'"); exists {
		rows, _ := users.rows("select id, user, sign_count, last_used from credentials")
		for _, r := range rows {
			sessions.exec("insert or ignore into passkeys (credential, user, count, last) values (?, ?, ?, ?)",
				r["id"], r["user"], r["sign_count"], r["last_used"])
		}
		users.exec("alter table credentials drop column sign_count")
		users.exec("alter table credentials drop column last_used")
	}
}

// db_upgrade_47 splits the OAuth used timestamp out of users.db.oauth into
// sessions.db.verifications, so per-login writes don't land on the cold
// reference store. Copies existing values then drops the column.
func db_upgrade_47() {
	sessions := db_open("db/sessions.db")
	if exists, _ := sessions.exists("select 1 from sqlite_master where type='table' and name='verifications'"); !exists {
		sessions.exec("create table verifications (oauth integer primary key, user integer not null, last integer not null default 0)")
		sessions.exec("create index verifications_user on verifications(user)")
	}

	users := db_open("db/users.db")
	if exists, _ := users.exists("select 1 from pragma_table_info('oauth') where name='used'"); exists {
		rows, _ := users.rows("select id, user, used from oauth")
		for _, r := range rows {
			sessions.exec("insert or ignore into verifications (oauth, user, last) values (?, ?, ?)",
				r["id"], r["user"], r["used"])
		}
		users.exec("alter table oauth drop column used")
	}
}

// db_upgrade_48 corrects two earlier moves. Tokens (whole table moved to
// sessions.db at v45) get split: definition goes back to users.db, only the
// per-request `used` timestamp stays in sessions.db (in a new `accesses`
// table). Passkey sign_count (moved to sessions.db.passkeys at v46) goes back
// to users.db.credentials so WebAuthn replay protection survives sessions.db
// corruption; only the cosmetic `last` stays in sessions.db.passkeys.
func db_upgrade_48() {
	sessions := db_open("db/sessions.db")
	users := db_open("db/users.db")

	// Tokens: recreate definition table in users.db
	if exists, _ := users.exists("select 1 from sqlite_master where type='table' and name='tokens'"); !exists {
		users.exec("create table tokens (hash text primary key not null, user integer not null references users(id) on delete cascade, app text not null, name text not null default '', scopes text not null default '', created integer not null, expires integer not null default 0)")
		users.exec("create index tokens_user on tokens(user)")
		users.exec("create index tokens_app on tokens(app)")
	}
	// Tokens: create new per-request access timestamp table in sessions.db
	if exists, _ := sessions.exists("select 1 from sqlite_master where type='table' and name='accesses'"); !exists {
		sessions.exec("create table accesses (hash text primary key not null, user integer not null, used integer not null default 0)")
		sessions.exec("create index accesses_user on accesses(user)")
	}
	// Tokens: split sessions.db.tokens → users.db.tokens (definition) + sessions.db.accesses (used)
	if exists, _ := sessions.exists("select 1 from sqlite_master where type='table' and name='tokens'"); exists {
		rows, _ := sessions.rows("select hash, user, app, name, scopes, created, used, expires from tokens")
		for _, r := range rows {
			users.exec("insert or ignore into tokens (hash, user, app, name, scopes, created, expires) values (?, ?, ?, ?, ?, ?, ?)",
				r["hash"], r["user"], r["app"], r["name"], r["scopes"], r["created"], r["expires"])
			sessions.exec("insert or ignore into accesses (hash, user, used) values (?, ?, ?)",
				r["hash"], r["user"], r["used"])
		}
		sessions.exec("drop table tokens")
	}

	// Passkeys: add sign_count back to users.db.credentials
	if exists, _ := users.exists("select 1 from pragma_table_info('credentials') where name='sign_count'"); !exists {
		users.exec("alter table credentials add column sign_count integer not null default 0")
	}
	// Passkeys: copy count back to users.db.credentials.sign_count, drop count from passkeys
	if exists, _ := sessions.exists("select 1 from pragma_table_info('passkeys') where name='count'"); exists {
		rows, _ := sessions.rows("select credential, count from passkeys")
		for _, r := range rows {
			users.exec("update credentials set sign_count=? where id=?", r["count"], r["credential"])
		}
		sessions.exec("alter table passkeys drop column count")
	}
}

// db_upgrade_49 adds the documents table to settings.db. Holds operator
// overrides for the bundled server-rules / terms / privacy markdown shipped
// in core/server/documents/. Bundled defaults are not copied in — empty
// override means "serve the bundled default".
func db_upgrade_49() {
	settings := db_open("db/settings.db")
	if exists, _ := settings.exists("select 1 from sqlite_master where type='table' and name='documents'"); !exists {
		settings.exec("create table documents ( name text not null, language text not null, body text not null, updated integer not null, primary key ( name, language ) )")
	}
}

// db_upgrade_52 reshapes directory.db for multi-location entities. The
// existing single-row-per-entity table (`directory.directory`) is renamed
// to `directory.entities`; a new `directory.locations(entity, peer, seen)`
// table holds per-peer location claims so a replicated entity can announce
// itself from every host independently and receivers track the active set.
// The legacy `location` column on entities is left for one release for
// rollback safety. See claude/plans/replication.md.
func db_upgrade_52() {
	db := db_open("db/directory.db")
	if exists, _ := db.exists("select 1 from sqlite_master where type='table' and name='directory'"); exists {
		db.exec("alter table directory rename to entities")
		for _, suffix := range []string{"name", "class", "location", "fingerprint", "created", "updated"} {
			db.exec("drop index if exists directory_" + suffix)
			db.exec("create index if not exists entities_" + suffix + " on entities(" + suffix + ")")
		}
	}
	if exists, _ := db.exists("select 1 from sqlite_master where type='table' and name='locations'"); !exists {
		db.exec("create table locations (entity text not null, peer text not null, seen integer not null, primary key (entity, peer))")
		db.exec("create index locations_peer on locations(peer)")
		db.exec("create index locations_seen on locations(seen)")
		// Backfill from the legacy location column on entities. Each
		// pre-existing row becomes one locations row keyed on the peer
		// it had claimed.
		db.exec("insert or ignore into locations (entity, peer, seen) select id, replace(location, 'p2p/', ''), updated from entities where location != ''")
	}
}

// db_upgrade_51 adds users.uid plus parallel TEXT user_uid columns on every
// table that referenced users.id, with SQLite triggers that keep user_uid in
// sync on insert / FK update. The integer users.id remains as the local
// disk-path identifier (`users/<int>/`); user_uid is the globally-stable
// data identifier used for replication. See claude/plans/replication.md
// "users.id — UID for all data references" and task #4.
//
// Additive only. The legacy integer FK columns stay populated and a follow-up
// migration drops them once production has run cleanly on the UID-only read
// path.
func db_upgrade_51() {
	users := db_open("db/users.db")

	if col_exists, _ := users.exists("select 1 from pragma_table_info('users') where name='uid'"); !col_exists {
		users.exec("alter table users add column uid text not null default ''")
		rows, _ := users.rows("select id from users where uid = ''")
		for _, r := range rows {
			if id, ok := r["id"].(int64); ok {
				users.exec("update users set uid = ? where id = ?", uid(), id)
			}
		}
		users.exec("create unique index if not exists users_uid on users(uid)")
		// Auto-populate uid for any future insert that doesn't supply one.
		// 32-char hex matches uid() format (UUIDv7 without hyphens) — same
		// width and character set, just different entropy source.
		users.exec(`create trigger if not exists users_uid_insert after insert on users
			when new.uid is null or new.uid = ''
			begin
				update users set uid = lower(hex(randomblob(16))) where id = new.id;
			end`)
	}

	for _, tbl := range []string{"entities", "credentials", "recovery", "totp", "oauth", "tokens"} {
		col_exists, _ := users.exists(fmt.Sprintf("select 1 from pragma_table_info('%s') where name='user_uid'", tbl))
		if col_exists {
			continue
		}
		users.exec(fmt.Sprintf("alter table %s add column user_uid text not null default ''", tbl))
		users.exec(fmt.Sprintf("update %s set user_uid = coalesce((select uid from users where id = %s.user), '') where user is not null and user_uid = ''", tbl, tbl))
		users.exec(fmt.Sprintf("create index if not exists %s_user_uid on %s(user_uid)", tbl, tbl))
		users.exec(fmt.Sprintf(`create trigger if not exists %s_user_uid_insert after insert on %s
			when new.user is not null and (new.user_uid is null or new.user_uid = '')
			begin
				update %s set user_uid = coalesce((select uid from users where id = new.user), '') where rowid = new.rowid;
			end`, tbl, tbl, tbl))
		users.exec(fmt.Sprintf(`create trigger if not exists %s_user_uid_update after update of user on %s
			when new.user is not null
			begin
				update %s set user_uid = coalesce((select uid from users where id = new.user), '') where rowid = new.rowid;
			end`, tbl, tbl, tbl))
	}
}

// db_upgrade_50 creates replication.db. Tables idempotent so re-running the
// migration on a partially-created database is safe.
func db_upgrade_50() {
	replication := db_open("db/replication.db")
	if exists, _ := replication.exists("select 1 from sqlite_master where type='table' and name='seen'"); !exists {
		replication.exec("create table seen (peer text not null, scope text not null, user text not null default '', sequence integer not null, applied integer not null, primary key (peer, scope, user, sequence))")
		replication.exec("create index seen_applied on seen(applied)")
	}
	if exists, _ := replication.exists("select 1 from sqlite_master where type='table' and name='pending'"); !exists {
		replication.exec("create table pending (peer text not null, scope text not null, user text not null default '', sequence integer not null, schema integer not null default 0, payload blob not null, received integer not null, primary key (peer, scope, user, sequence))")
		replication.exec("create index pending_received on pending(received)")
	}
	if exists, _ := replication.exists("select 1 from sqlite_master where type='table' and name='hosts'"); !exists {
		replication.exec("create table hosts (user text not null, peer text not null, added integer not null, ack integer not null default 0, primary key (user, peer))")
	}
	if exists, _ := replication.exists("select 1 from sqlite_master where type='table' and name='sequence'"); !exists {
		replication.exec("create table sequence (user text not null default '', scope text not null, next integer not null default 0, primary key (user, scope))")
	}
	if exists, _ := replication.exists("select 1 from sqlite_master where type='table' and name='pair'"); !exists {
		replication.exec("create table pair (peer text primary key, added integer not null, role text not null default '')")
	}
	if exists, _ := replication.exists("select 1 from sqlite_master where type='table' and name='leadership'"); !exists {
		replication.exec("create table leadership (scope text not null, key text not null, peer text not null, expires integer not null, fence integer not null default 0, primary key (scope, key))")
		replication.exec("create index leadership_expires on leadership(expires)")
	}
	if exists, _ := replication.exists("select 1 from sqlite_master where type='table' and name='bootstrap'"); !exists {
		replication.exec("create table bootstrap (scope text not null, peer text not null, position text not null default '', primary key (scope, peer))")
	}
	if exists, _ := replication.exists("select 1 from sqlite_master where type='table' and name='schemas'"); !exists {
		replication.exec("create table schemas (peer text primary key, core integer not null default 0, apps text not null default '')")
	}
}

func (db *DB) close() {
	db.closed = now()
}

// db_purge_prefix closes and evicts every cached DB whose on-disk path lives
// under the given directory. Use this before removing a directory (e.g. a
// user's data dir) so that stale handles can't be reused for I/O against
// files that no longer exist.
func db_purge_prefix(dir string) {
	prefix := filepath.Join(data_dir, dir)
	if !strings.HasSuffix(prefix, string(os.PathSeparator)) {
		prefix += string(os.PathSeparator)
	}
	var closers []*sqlx.DB
	databases_lock.Lock()
	for key, db := range databases {
		if strings.HasPrefix(db.path, prefix) {
			closers = append(closers, db.internal, db.starlark)
			delete(databases, key)
		}
	}
	databases_lock.Unlock()
	for _, h := range closers {
		h.Close()
	}
}

func (db *DB) exec(query string, values ...any) {
	must(db.internal.Exec(query, values...))
}

func (db *DB) exists(query string, values ...any) (bool, error) {
	r, err := db.internal.Query(query, values...)
	if err != nil {
		return false, err
	}
	defer r.Close()
	return r.Next(), nil
}

// integer returns the first column as an integer, or 0 on error
func (db *DB) integer(query string, values ...any) int {
	var result int
	err := db.internal.QueryRow(query, values...).Scan(&result)
	if err != nil {
		return 0
	}
	return result
}

func (db *DB) row(query string, values ...any) (map[string]any, error) {
	r, err := db.internal.Queryx(query, values...)
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

	r, err := db.internal.Queryx(query, values...)
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
	err := db.internal.QueryRowx(query, values...).StructScan(out)
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
	return db.internal.Select(out, query, values...)
}

// db_for_thread resolves the correct per-user database for the current Starlark
// thread, applying the same authentication-vs-routing rules used by
// mochi.db.execute and mochi.db.transaction. Returns the DB, or an error
// describing why the lookup failed.
func db_for_thread(t *sl.Thread) (*DB, error) {
	owner, _ := t.Local("owner").(*User)
	user, _ := t.Local("user").(*User)

	var db_user *User
	var domain_routing bool

	if action := t.Local("action"); action != nil {
		if a, ok := action.(*Action); ok && a.domain != nil && a.domain.route != nil {
			domain_routing = a.domain.route.context != ""
		}
	}

	if user == nil {
		if owner != nil {
			db_user = owner
		} else {
			return nil, fmt.Errorf("no user context available")
		}
	} else if owner != nil && owner.ID != user.ID {
		db_user = owner
	} else if domain_routing {
		if owner != nil {
			db_user = owner
		} else {
			return nil, fmt.Errorf("no owner context for domain routing")
		}
	} else {
		db_user = user
	}

	app, _ := t.Local("app").(*App)
	if app == nil {
		return nil, fmt.Errorf("unknown app")
	}

	db := db_app(db_user, app)
	if db == nil {
		return nil, fmt.Errorf("app has no database configured")
	}
	return db, nil
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

	if reason := db_starlark_sql_blocked(query); reason != "" {
		return sl_error(fn, "%s", reason)
	}

	as := sl_decode(args[1:]).([]any)

	// Flatten nested lists/tuples so Starlark can pass variable-length parameter lists.
	flat := make([]any, 0, len(as))
	for _, a := range as {
		if list, ok := a.([]any); ok {
			flat = append(flat, list...)
		} else {
			flat = append(flat, a)
		}
	}
	as = flat

	db, err := db_for_thread(t)
	if err != nil {
		return sl_error(fn, "%v", err)
	}

	// Check out a dedicated connection so a failed multi-statement
	// query (e.g. `BEGIN; bad-sql; COMMIT;` where bad-sql is denied by
	// the authoriser at prepare) can't return a half-open transaction
	// to the shared pool. On error we issue a defensive ROLLBACK on
	// the same connection before releasing it.
	ctx := context.Background()
	conn, err := db.starlark.Connx(ctx)
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}
	defer conn.Close()

	switch fn.Name() {
	case "mochi.db.execute":
		_, err := conn.ExecContext(ctx, query, as...)
		if err != nil {
			db_starlark_rollback(conn)
			return sl_error(fn, "database error: %v", err)
		}
		return sl.None, nil

	case "mochi.db.exists":
		r, err := conn.QueryContext(ctx, query, as...)
		if err != nil {
			db_starlark_rollback(conn)
			return sl_error(fn, "database error: %v", err)
		}
		defer r.Close()
		if r.Next() {
			return sl.True, nil
		}
		return sl.False, nil

	case "mochi.db.row":
		r, err := conn.QueryxContext(ctx, query, as...)
		if err != nil {
			db_starlark_rollback(conn)
			return sl_error(fn, "database error: %v", err)
		}
		defer r.Close()
		if !r.Next() {
			return sl.None, nil
		}
		row := make(map[string]any)
		if err := r.MapScan(row); err != nil {
			return sl_error(fn, "database error: %v", err)
		}
		for k, v := range row {
			if b, ok := v.([]byte); ok {
				row[k] = string(b)
			}
		}
		return sl_encode(row), nil

	case "mochi.db.rows":
		r, err := conn.QueryxContext(ctx, query, as...)
		if err != nil {
			db_starlark_rollback(conn)
			return sl_error(fn, "database error: %v", err)
		}
		defer r.Close()
		var results []map[string]any
		for r.Next() {
			row := make(map[string]any)
			if err := r.MapScan(row); err != nil {
				return sl_error(fn, "database error: %v", err)
			}
			for k, v := range row {
				if b, ok := v.([]byte); ok {
					row[k] = string(b)
				}
			}
			results = append(results, row)
		}
		return sl_encode(results), nil
	}

	return sl_error(fn, "invalid database query %q", fn.Name())
}

// db_starlark_rollback issues a best-effort ROLLBACK on the given
// connection. Used to clear any half-open transaction left behind by a
// multi-statement Exec whose middle statement was denied by the
// authoriser (e.g. `BEGIN; PRAGMA …; COMMIT;` — BEGIN runs, PRAGMA is
// denied at prepare, COMMIT never executes). On a connection without
// an active transaction the ROLLBACK errors and is silently dropped —
// that's expected and safe.
func db_starlark_rollback(conn *sqlx.Conn) {
	_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
}

// db_starlark_sql_blocked returns a non-empty error message if the query
// starts with a keyword that's blocked from Starlark. Belt-and-braces on
// top of the per-connection authoriser: the authoriser is the
// load-bearing layer (it sees parsed multi-statement input and can't
// be bypassed by `BEGIN; PRAGMA …; COMMIT;`), but a clean string-level
// rejection gives apps a friendlier error than an opaque authoriser
// denial. Also catches VACUUM and ANALYZE, which have no authoriser
// action codes.
func db_starlark_sql_blocked(query string) string {
	trimmed := strings.TrimSpace(query)
	first := trimmed
	if i := strings.IndexAny(trimmed, " \t\r\n;("); i >= 0 {
		first = trimmed[:i]
	}
	switch strings.ToUpper(first) {
	case "PRAGMA":
		return "PRAGMA statements are not allowed"
	case "VACUUM":
		return "VACUUM is not allowed"
	case "ANALYZE":
		return "ANALYZE is not allowed"
	}
	return ""
}

// TransactionHandle is the Starlark value returned by mochi.db.transaction(). It
// exposes execute/exists/row/rows that route through the underlying SQL
// transaction, plus commit and rollback. Forgetting to call commit() is safe —
// the cleanup hook in starlark.go rolls back any uncommitted handles when the
// Starlark thread tears down (script return, error, or timeout).
type TransactionHandle struct {
	tx     *sqlx.Tx
	closed bool
}

func (h *TransactionHandle) String() string { return "mochi.db.transaction" }
func (h *TransactionHandle) Type() string   { return "transaction" }
func (h *TransactionHandle) Freeze()        {}
func (h *TransactionHandle) Truth() sl.Bool { return sl.True }
func (h *TransactionHandle) Hash() (uint32, error) {
	return 0, fmt.Errorf("unhashable type: transaction")
}

func (h *TransactionHandle) AttrNames() []string {
	return []string{"commit", "execute", "exists", "rollback", "row", "rows"}
}

func (h *TransactionHandle) Attr(name string) (sl.Value, error) {
	switch name {
	case "commit":
		return sl.NewBuiltin("transaction.commit", h.sl_commit), nil
	case "execute":
		return sl.NewBuiltin("transaction.execute", h.sl_execute), nil
	case "exists":
		return sl.NewBuiltin("transaction.exists", h.sl_exists), nil
	case "rollback":
		return sl.NewBuiltin("transaction.rollback", h.sl_rollback), nil
	case "row":
		return sl.NewBuiltin("transaction.row", h.sl_row), nil
	case "rows":
		return sl.NewBuiltin("transaction.rows", h.sl_rows), nil
	}
	return nil, nil
}

// transaction_close rolls back any uncommitted transactions registered on the
// thread. Called from the Starlark execution wrapper at script tear-down so
// callers can't leak open transactions even if they forget to commit or the
// script errors out.
func transaction_close(t *sl.Thread) {
	handles, _ := t.Local("transactions").([]*TransactionHandle)
	for _, h := range handles {
		if !h.closed {
			h.tx.Rollback()
			h.closed = true
		}
	}
	t.SetLocal("transactions", nil)
}

// transaction_args validates the SQL argument shape shared by execute/exists/row/rows.
func transaction_args(fn *sl.Builtin, args sl.Tuple) (string, []any, error) {
	if len(args) < 1 {
		return "", nil, fmt.Errorf("syntax: <SQL statement: string>, [parameters: variadic]")
	}
	query, ok := sl.AsString(args[0])
	if !ok {
		return "", nil, fmt.Errorf("invalid SQL statement %q", query)
	}
	if reason := db_starlark_sql_blocked(query); reason != "" {
		return "", nil, fmt.Errorf("%s", reason)
	}
	as := sl_decode(args[1:]).([]any)
	flat := make([]any, 0, len(as))
	for _, a := range as {
		if list, ok := a.([]any); ok {
			flat = append(flat, list...)
		} else {
			flat = append(flat, a)
		}
	}
	return query, flat, nil
}

func (h *TransactionHandle) sl_execute(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if h.closed {
		return sl_error(fn, "transaction is closed")
	}
	query, params, err := transaction_args(fn, args)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	if _, err := h.tx.Exec(query, params...); err != nil {
		return sl_error(fn, "database error: %v", err)
	}
	return sl.None, nil
}

func (h *TransactionHandle) sl_exists(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if h.closed {
		return sl_error(fn, "transaction is closed")
	}
	query, params, err := transaction_args(fn, args)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	r, err := h.tx.Query(query, params...)
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}
	defer r.Close()
	return sl.Bool(r.Next()), nil
}

func (h *TransactionHandle) sl_row(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if h.closed {
		return sl_error(fn, "transaction is closed")
	}
	query, params, err := transaction_args(fn, args)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	r, err := h.tx.Queryx(query, params...)
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}
	defer r.Close()
	if !r.Next() {
		return sl.None, nil
	}
	row := make(map[string]any)
	if err := r.MapScan(row); err != nil {
		return sl_error(fn, "database error: %v", err)
	}
	for k, v := range row {
		if b, ok := v.([]byte); ok {
			row[k] = string(b)
		}
	}
	return sl_encode(row), nil
}

func (h *TransactionHandle) sl_rows(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if h.closed {
		return sl_error(fn, "transaction is closed")
	}
	query, params, err := transaction_args(fn, args)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	r, err := h.tx.Queryx(query, params...)
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}
	defer r.Close()
	var results []map[string]any
	for r.Next() {
		row := make(map[string]any)
		if err := r.MapScan(row); err != nil {
			return sl_error(fn, "database error: %v", err)
		}
		for k, v := range row {
			if b, ok := v.([]byte); ok {
				row[k] = string(b)
			}
		}
		results = append(results, row)
	}
	return sl_encode(results), nil
}

func (h *TransactionHandle) sl_commit(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if h.closed {
		return sl_error(fn, "transaction is closed")
	}
	if err := h.tx.Commit(); err != nil {
		h.closed = true
		return sl_error(fn, "commit failed: %v", err)
	}
	h.closed = true
	return sl.None, nil
}

func (h *TransactionHandle) sl_rollback(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if h.closed {
		return sl_error(fn, "transaction is closed")
	}
	h.tx.Rollback()
	h.closed = true
	return sl.None, nil
}

// mochi.db.transaction() -> transaction: Start a SQL transaction on the calling
// app's per-user database. Returns a handle whose execute/exists/row/rows methods
// run inside the transaction. Call .commit() to persist or .rollback() to discard.
// If the Starlark thread tears down (return, error, timeout) without commit, the
// transaction is rolled back automatically — forgetting commit is safe.
// Nested transactions error: SQLite doesn't support real nested transactions,
// and silent savepoint behaviour would surprise callers.
func api_db_transaction(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 0 {
		return sl_error(fn, "syntax: mochi.db.transaction()")
	}

	// Block nested transactions
	existing, _ := t.Local("transactions").([]*TransactionHandle)
	for _, h := range existing {
		if !h.closed {
			return sl_error(fn, "a transaction is already in progress")
		}
	}

	db, err := db_for_thread(t)
	if err != nil {
		return sl_error(fn, "%v", err)
	}

	tx, err := db.starlark.Beginx()
	if err != nil {
		return sl_error(fn, "begin failed: %v", err)
	}

	h := &TransactionHandle{tx: tx}
	t.SetLocal("transactions", append(existing, h))
	return h, nil
}

// mochi.db.table(name) -> list: Return column info for a table via PRAGMA table_info
func api_db_table(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: mochi.db.table(name)")
	}
	name, ok := sl.AsString(args[0])
	if !ok || !valid_sql_identifier(name) {
		return sl_error(fn, "invalid table name %q", name)
	}

	db, err := db_for_thread(t)
	if err != nil {
		return sl_error(fn, "%v", err)
	}

	rows, err := db.rows("PRAGMA table_info(" + name + ")")
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}
	return sl_encode(rows), nil
}

// mochi.db.tables() -> list: List user table names in the calling app's database, sorted
func api_db_tables(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 0 {
		return sl_error(fn, "syntax: mochi.db.tables()")
	}
	db, err := db_for_thread(t)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	rows, err := db.rows("select name from sqlite_schema where type='table' and name not like 'sqlite_%' and name not like '\\_%' escape '\\' order by name")
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}
	names := make([]any, 0, len(rows))
	for _, r := range rows {
		if n, ok := r["name"].(string); ok {
			names = append(names, n)
		}
	}
	return sl_encode(names), nil
}

// mochi.db.indexes(table) -> list: Return index info for a table via PRAGMA index_list
func api_db_indexes(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: mochi.db.indexes(table)")
	}
	name, ok := sl.AsString(args[0])
	if !ok || !valid_sql_identifier(name) {
		return sl_error(fn, "invalid table name %q", name)
	}
	db, err := db_for_thread(t)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	rows, err := db.rows("PRAGMA index_list(" + name + ")")
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}
	return sl_encode(rows), nil
}

// valid_sql_identifier returns true if name is alphanumeric/underscore only — safe to splice into a PRAGMA.
func valid_sql_identifier(name string) bool {
	if len(name) == 0 {
		return false
	}
	for _, c := range name {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_') {
			return false
		}
	}
	return true
}

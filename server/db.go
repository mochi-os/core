// Mochi server: Database
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/ncruces/go-sqlite3"
	sqlitedrv "github.com/ncruces/go-sqlite3/driver"
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

// MigrationAbort, raised by mochi.db.abort(), stops a database_upgrade without
// advancing the schema version, so the same step retries on the next request.
// For a migration blocked on something transient, not for a coding error.
type MigrationAbort struct {
	Reason string
}

func (e *MigrationAbort) Error() string { return "migration aborted: " + e.Reason }

// mochi.db.abort(reason) -> never returns: Abort the running database_upgrade
// without advancing the schema version, so the same migration step retries on
// the next request. For a transient precondition, not a coding error.
func api_db_abort(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	reason := ""
	if len(args) > 0 {
		reason, _ = sl.AsString(args[0])
	}
	return nil, &MigrationAbort{Reason: reason}
}

// DB carries two connection pools per SQLite file. internal has no authoriser
// and serves server-trusted queries; starlark carries the authoriser that
// denies ATTACH/DETACH/PRAGMA/triggers/vtables and runs only app-supplied SQL.
type DB struct {
	key      string
	path     string
	internal *sqlx.DB
	starlark *sqlx.DB
	user     *User
	app      *App
	// system_setup is set once db_app_system has run access_setup / journal_setup
	// on this handle. Gated on this rather than db_open_work's `reused`, so a
	// handle first cached by a raw db_open still gets its setups. Guarded by
	// lock(path).
	system_setup bool
	// ready is set once db_app has finished database_create/upgrade on this
	// handle; the reused fast-path requires it so a concurrent opener waits rather
	// than querying a schema that does not exist yet (#227). Guarded by
	// databases_lock.
	ready bool
	// closed is the unix timestamp when this handle was last marked
	// idle, or 0 while in use. Always read and written under
	// databases_lock - same primitive that guards the cache map this
	// DB lives in, so no new synchronisation primitive is introduced.
	closed int64

	// statement_cache holds prepared statements for the internal pool, keyed
	// by SQL text, populated lazily by prepared(). Guarded by statement_lock.
	// Closed on eviction (stmts_close).
	statement_lock  sync.Mutex
	statement_cache map[string]*sqlx.Stmt
}

const (
	schema_version = 10
)

// health_schema is the current shape of queue.db's health table, shared by
// db_create and the test harness so the two cannot drift. db_upgrade_2 must NOT
// use it: it reproduces the shape schema 2 shipped, later columns added by
// later steps.
const health_schema = "create table if not exists health ( recipient text not null primary key, failures integer not null default 0, denials integer not null default 0, success integer not null default 0, since integer not null default 0, suspended integer not null default 0, probed integer not null default 0, evicted integer not null default 0 )"

var (
	databases      = map[string]*DB{}
	databases_lock sync.Mutex

	api_db = sls.FromStringDict(sl.String("mochi.db"), sl.StringDict{
		"abort":       sl.NewBuiltin("mochi.db.abort", api_db_abort),
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
	// auto_vacuum must be set before journal_mode=WAL and before any table exists,
	// or it silently stays NONE. Fresh databases are then incremental from birth;
	// existing ones convert lazily in DB.vacuum. See claude/plans/vacuum.md.
	if err := c.Exec("PRAGMA auto_vacuum=INCREMENTAL"); err != nil {
		return err
	}
	if err := c.Exec("PRAGMA journal_mode=WAL"); err != nil {
		return err
	}
	if err := c.Exec("PRAGMA foreign_keys=ON"); err != nil {
		return err
	}
	return c.Exec(fmt.Sprintf("PRAGMA max_page_count = %d", db_page_count_maximum))
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

// db_authorise_starlark denies what an app must not do on its own database.
// PRAGMA only with an argument - bare reads must pass for the driver's `PRAGMA
// query_only`, and pragma_* vtables carry their target as one. ANALYZE needs
// its own case because the string check misses it; VACUUM attaches, so
// AUTH_ATTACH already covers it.
func db_authorise_starlark(action sqlite3.AuthorizerActionCode, _, name4th, _, _ string) sqlite3.AuthorizerReturnCode {
	switch action {
	case sqlite3.AUTH_ATTACH, sqlite3.AUTH_DETACH,
		sqlite3.AUTH_ANALYZE,
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

// db_setup_conn_lifecycle configures the connection that runs an app's database
// lifecycle functions: the Starlark rules plus `pragma user_version` (the
// server stamps the schema version in the same transaction) and read-only
// table_info / index_list, which must run here to see this transaction's
// uncommitted DDL.
func db_setup_conn_lifecycle(c *sqlite3.Conn) error {
	if err := db_setup_conn(c); err != nil {
		return err
	}
	return c.SetAuthorizer(db_authorise_lifecycle)
}

func db_authorise_lifecycle(action sqlite3.AuthorizerActionCode, name3rd, name4th, schema, inner string) sqlite3.AuthorizerReturnCode {
	if action == sqlite3.AUTH_PRAGMA {
		switch strings.ToLower(name3rd) {
		case "user_version", "table_info", "index_list":
			return sqlite3.AUTH_OK
		}
	}
	return db_authorise_starlark(action, name3rd, name4th, schema, inner)
}

func db_create() {
	db_migrating.Add(1)
	defer db_migrating.Add(-1)
	info("Creating new database")

	// Settings
	settings := db_open("db/settings.db")
	settings.exec("create table if not exists settings ( name text not null primary key, value text not null )")
	settings.exec("insert or ignore into settings ( name, value ) values ( 'schema', ? )", schema_version)

	// Documents: operator-customisable Markdown for server rules / terms / privacy.
	// Bundled defaults live in core/server/documents/ (embedded); this table
	// holds only operator overrides keyed by (name, language).
	settings.exec("create table if not exists documents ( name text not null, language text not null, body text not null, updated integer not null, primary key ( name, language ) )")

	// Users. `uid` is the globally-stable identifier used everywhere — for
	// replication, cross-host data references, FK joins, and the on-disk
	// `users/<uid>/` data directory. Callers supply the uid via the Go
	// uid() helper at INSERT time; no triggers.
	users := db_open("db/users.db")
	users.exec("create table if not exists users (uid text not null primary key, username text not null, role text not null default 'user', methods text not null default '', disabled text not null default '', status text not null default 'active', restore_source text not null default '', restore_passkeys integer not null default 0, purge integer not null default 0)")
	users.exec("create unique index if not exists users_username on users (username)")

	// Services the user must re-link after a server move (restore). Populated
	// at restore time from the bundle's linked.json; rows clear as the user
	// re-links each on the destination. Drives the post-restore banner.
	users.exec("create table if not exists relinks (user text not null references users(uid) on delete cascade, service text not null, identifier text not null default '', linked integer not null default 0, primary key (user, service))")

	// Passkey credential definitions and sign count. Sign count is WebAuthn
	// replay-prevention state and lives here so it survives sessions.db
	// corruption. Only the cosmetic last-used timestamp lives in sessions.db.
	users.exec("create table if not exists credentials (id blob primary key, user text not null references users(uid) on delete cascade, public_key blob not null, sign_count integer not null default 0, name text not null default '', transports text not null default '', backup_eligible integer not null default 0, backup_state integer not null default 0, created integer not null)")
	users.exec("create index if not exists credentials_user on credentials(user)")

	// Recovery codes
	users.exec("create table if not exists recovery (id integer primary key, user text not null references users(uid) on delete cascade, hash text not null, created integer not null)")
	users.exec("create index if not exists recovery_user on recovery(user)")

	// TOTP secrets. pending holds a secret from an unproven enrolment, kept
	// separate from secret so starting an enrolment cannot disturb the
	// authenticator the user is currently logging in with.
	users.exec("create table if not exists totp (user text primary key references users(uid) on delete cascade, secret text not null, verified integer not null default 0, pending text not null default '', created integer not null)")

	// OAuth identity definitions (Google, GitHub, Microsoft, Facebook, X).
	// Last-used timestamp lives in sessions.db.verifications so this cold
	// reference store doesn't take a write on every OAuth login.
	users.exec("create table if not exists oauth (id integer primary key, user text not null references users(uid) on delete cascade, provider text not null, subject text not null, email text not null default '', verified integer not null default 0, name text not null default '', created integer not null, unique(provider, subject))")
	users.exec("create index if not exists oauth_user on oauth(user)")

	// API token definitions. Hot per-request "used" timestamp lives in
	// sessions.db.accesses; here we keep just the definition so token loss
	// doesn't follow sessions.db corruption.
	users.exec("create table if not exists tokens (hash text primary key not null, user text not null references users(uid) on delete cascade, app text not null, name text not null default '', scopes text not null default '', action text not null default '', entity text not null default '', created integer not null, expires integer not null default 0)")
	users.exec("create index if not exists tokens_user on tokens(user)")
	users.exec("create index if not exists tokens_app on tokens(app)")

	// Entities
	users.exec("create table if not exists entities (id text not null primary key, private text not null, fingerprint text not null, user text not null references users(uid) on delete cascade, parent text not null default '', class text not null, name text not null, privacy text not null default 'public', data text not null default '', published integer not null default 0)")
	users.exec("create index if not exists entities_fingerprint on entities(fingerprint)")
	users.exec("create index if not exists entities_user on entities(user)")
	users.exec("create index if not exists entities_parent on entities(parent)")
	users.exec("create index if not exists entities_class on entities(class)")
	users.exec("create index if not exists entities_name on entities(name)")
	users.exec("create index if not exists entities_privacy on entities(privacy)")
	users.exec("create index if not exists entities_published on entities(published)")

	// Sessions (login codes and sessions - transient auth data)
	sessions := db_open("db/sessions.db")
	sessions.exec("create table if not exists codes ( code text not null, username text not null, expires integer not null, primary key ( code, username ) )")
	sessions.exec("create index if not exists codes_expires on codes( expires )")
	sessions.exec("create table if not exists sessions (user text not null, code text not null, secret text not null default '', expires integer not null, created integer not null default 0, accessed integer not null default 0, address text not null default '', agent text not null default '', primary key (user, code))")
	sessions.exec("create unique index if not exists sessions_code on sessions(code)")
	sessions.exec("create index if not exists sessions_expires on sessions(expires)")
	sessions.exec("create index if not exists sessions_user on sessions(user)")

	// WebAuthn ceremony sessions (temporary)
	sessions.exec("create table if not exists ceremonies (id text primary key, type text not null, user text not null default '', challenge blob not null, data text not null default '', expires integer not null)")
	sessions.exec("create index if not exists ceremonies_expires on ceremonies(expires)")

	// Partial authentication sessions (for MFA)
	sessions.exec("create table if not exists partial (id text primary key, user text not null, completed text not null default '', remaining text not null, expires integer not null)")
	sessions.exec("create index if not exists partial_expires on partial(expires)")

	// Step-up re-authentication proofs: short-lived single-use tokens
	// earned by re-verifying the user's login factor(s) before a
	// sensitive action. methods is the accrued set of factors verified.
	sessions.exec("create table if not exists reauthentication (id text primary key, user text not null, methods text not null default '', expires integer not null)")
	sessions.exec("create index if not exists reauthentication_expires on reauthentication(expires)")

	// Last-login timestamps (kept here, not in users.db, so the cold reference
	// store doesn't take a write on every login)
	sessions.exec("create table if not exists logins (user text primary key, last integer not null)")

	// Per-request token access timestamps. Split out of users.db.tokens so the
	// every-request "used" write doesn't land on the cold reference store, but
	// the token definitions themselves stay in users.db so token loss doesn't
	// follow sessions.db corruption. `user` duplicated here for cascade.
	sessions.exec("create table if not exists accesses (hash text primary key not null, user text not null, used integer not null default 0)")
	sessions.exec("create index if not exists accesses_user on accesses(user)")

	// Cosmetic last-used timestamp per passkey. Sign count (replay-prevention
	// state) stays in users.db.credentials; only the cosmetic stat lives here.
	sessions.exec("create table if not exists passkeys (credential blob primary key, user text not null, last integer not null default 0)")
	sessions.exec("create index if not exists passkeys_user on passkeys(user)")

	// OAuth verification state (last time each linked identity was used to log
	// in). Split from users.db.oauth so per-login writes don't land on the cold
	// reference store. `oauth` references users.db.oauth(id); `user` duplicated
	// here for cascade.
	sessions.exec("create table if not exists verifications (oauth integer primary key, user text not null, last integer not null default 0)")
	sessions.exec("create index if not exists verifications_user on verifications(user)")

	// Directory. One row per (entity, peer): one host's listing of one entity,
	// asserted by that host alone, and self-verifying - `signature` is the
	// entity's ed25519 signature over the whole row, peer included.
	directory := db_open("db/directory.db")
	directory.exec("create table if not exists entries ( entity text not null, peer text not null, name text not null, class text not null, data text not null default '', fingerprint text not null default '', version integer not null default 0, created integer not null, seen integer not null, message text not null default '', expires text not null default '', signature text not null default '', primary key ( entity, peer ) )")
	directory.exec("create index if not exists entries_name on entries( name )")
	directory.exec("create index if not exists entries_class on entries( class )")
	directory.exec("create index if not exists entries_fingerprint on entries( fingerprint )")
	directory.exec("create index if not exists entries_peer on entries( peer )")
	directory.exec("create index if not exists entries_seen on entries( seen )")
	directory.exec("create index if not exists entries_created on entries( created )")

	// Peers
	peers := db_open("db/peers.db")
	peers.exec("create table if not exists peers ( id text not null, address text not null, updated integer not null, success integer not null default 0, failure integer not null default 0, primary key ( id, address ) )")
	// Claimed display names per peer with their verification verdict
	peers.exec("create table if not exists names ( id text not null, name text not null, updated integer not null, primary key ( id, name ) )")
	// Latest signed peer record per peer: self-certifying addresses
	peers.exec("create table if not exists records ( id text not null primary key, record blob not null, sequence integer not null, updated integer not null )")

	// Message queue with reliability tracking
	queue := db_open("db/queue.db")
	// Outgoing message queue
	queue.exec("create table if not exists queue ( id text primary key, type text not null default 'direct', target text not null, from_entity text not null, to_entity text not null, service text not null, event text not null, from_app text not null default '', from_services text not null default '', content blob not null default '', data blob not null default '', file text not null default '', expires integer not null default 0, status text not null default 'pending', attempts integer not null default 0, next_retry integer not null, last_error text not null default '', created integer not null, priority integer not null default 20, claimed integer not null default 0 )")
	// (status, priority, next_retry) covers both queue_select queries. Without the
	// priority column the main query sorts the whole ready set and the bulk query
	// scans it.
	queue.exec("create index if not exists queue_status_priority_retry on queue (status, priority, next_retry)")
	queue.exec("create index if not exists queue_target on queue (target)")
	queue.exec("create index if not exists queue_target_priority_retry on queue (target, priority desc, next_retry)")
	// journal_inflight bridges send->ack for the per-peer journal delivery
	// cursor (#28): one row per shipped journal op, resolved when the
	// transport ACK lands so journal_delivery can advance. Co-located with
	// the ack delete so the resolve is a same-DB lookup.
	queue.exec("create table if not exists journal_inflight (id text primary key, user text not null, peer text not null, stream text not null, sequence integer not null, created integer not null)")
	// Per-recipient delivery health: failure memory per recipient entity, rows
	// only where there is a failure history. `evicted` stamps the drop-subscriber
	// dispatch; only a stamped row is residue the cleanup sweep may delete.
	queue.exec(health_schema)
	queue.exec(`create table if not exists pushes ( id text primary key, user text not null,
		account text not null, type text not null, identifier text not null default '',
		data text not null default '', app text not null default '', category text not null default '',
		object text not null default '', title text not null default '', body text not null default '',
		link text not null default '', event text not null default '', attempts integer not null default 0,
		next_retry integer not null, created integer not null )`)
	queue.exec("create index if not exists pushes_next_retry on pushes(next_retry)")

	// Domains
	domains := db_open("db/domains.db")
	domains.exec("create table if not exists domains (domain text primary key, verified integer not null default 0, token text not null default '', tls integer not null default 1, created integer not null, updated integer not null)")
	domains.exec("create table if not exists routes (domain text not null, path text not null default '', method text not null default 'app', target text not null, context text not null default '', owner text not null default '', priority integer not null default 0, enabled integer not null default 1, created integer not null, updated integer not null, primary key (domain, path), foreign key (domain) references domains(domain) on delete cascade)")
	if exists, _ := domains.exists("select 1 from pragma_table_info('routes') where name='owner'"); !exists {
		domains.exec("alter table routes add column owner text not null default ''")
	}
	domains.exec("create index if not exists routes_domain on routes(domain)")
	domains.exec("create table if not exists delegations (id integer primary key, domain text not null, path text not null, owner text not null, created integer not null, updated integer not null, unique(domain, path, owner), foreign key (domain) references domains(domain) on delete cascade)")
	domains.exec("create index if not exists delegations_domain on delegations(domain)")
	domains.exec("create index if not exists delegations_owner on delegations(owner)")

	// Apps (for multi-version and user-configurable routing)
	apps := db_open("db/apps.db")
	apps.exec("create table if not exists classes (class text not null primary key, app text not null)")
	apps.exec("create table if not exists services (service text not null primary key, app text not null)")
	apps.exec("create table if not exists paths (path text not null primary key, app text not null)")
	apps.exec("create table if not exists versions (app text not null primary key, version text, track text)")
	apps.exec("create table if not exists tracks (app text not null, track text not null, version text not null, primary key (app, track))")
	apps.exec("create table if not exists apps (app text not null primary key, installed integer not null)")

	// Scheduled events
	schedule := db_open("db/schedule.db")
	schedule.exec("create table if not exists schedule (id integer primary key, user text not null, app text not null, due int not null, event text not null, data text not null, interval int not null, created int not null)")
	schedule.exec("create index if not exists schedule_due on schedule(due)")
	schedule.exec("create index if not exists schedule_app_event on schedule(app, event)")

	// External-data caches (Wikidata qid labels + searches)
	external := db_open("db/external.db")
	external.exec("create table if not exists qids (qid text not null, lang text not null, label text not null, fetched integer not null, primary key (qid, lang))")
	external.exec("create table if not exists qid_searches (query text not null, lang text not null, results text not null, fetched integer not null, primary key (query, lang))")

}

// db_apps opens the apps.db database, creating tables if needed.
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
	path := fmt.Sprintf("users/%s/%s.db", u.UID, name)
	db := db_open(path)
	// Bind under databases_lock via db_bind: these fields live on the shared
	// cached handle, so assigning them directly races any concurrent opener.
	db_bind(db, u, nil)

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
		db.exec("create table if not exists accounts (id text not null primary key, type text not null, label text not null default '', identifier text not null default '', data text not null default '', created integer not null, verified integer not null default 0, enabled integer not null default 1, \"default\" text not null default '', last_delivered integer not null default 0)")
		db.exec("create index if not exists accounts_type on accounts(type)")
		if exists, _ := db.exists("select 1 from pragma_table_info('accounts') where name='last_delivered'"); !exists {
			db.exec("alter table accounts add column last_delivered integer not null default 0")
		}
		db.accounts_migrate()

		// User interest profiles for personalised ranking
		db.exec("create table if not exists interests (qid text not null primary key, weight integer not null default 100, updated integer not null default 0)")

		// Internal key-value settings (Go-only, no Starlark API)
		db.exec("create table if not exists settings (key text not null primary key, text text not null default '', number integer not null default 0)")

		// The user's learned directory: private routing memory (directory_user.go)
		directory_user_table(db)

		// Per-user app state (permission setup tracking)
		db.apps_setup()
	}

	// Per-user notification-delivery dedup (email + web push)
	if name == "notifications" {
		db.exec("create table if not exists email_delivered (address text not null, event_id text not null, ts integer not null, primary key (address, event_id))")
		db.exec("create index if not exists email_delivered_ts on email_delivered(ts)")
		db.exec("create table if not exists webpush_delivered (endpoint text not null, event_id text not null, ts integer not null, primary key (endpoint, event_id))")
		db.exec("create index if not exists webpush_delivered_ts on webpush_delivered(ts)")
	}

	return db
}

// Per-database page-count cap applied to every connection by db_setup_conn. At
// the cap SQLite returns SQLITE_FULL, which must() turns into a panic - a
// safety net against runaway growth, sized to clear a legitimately heavy
// per-user DB.
const db_page_count_maximum = 6_553_600

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

	path := fmt.Sprintf("users/%s/%s/db/%s", u.UID, app.id, av.Database.File)
	key := fmt.Sprintf("%s|%s", filepath.Join(data_dir, path), av.Version)
	db, _, reused := db_open_work(path, key)
	if db == nil {
		return nil
	}
	db_bind(db, u, app)

	// Fast path: a reused handle whose schema this process has verified. Gated on
	// ready, not reuse alone - a concurrent opener can get the handle mid-creation
	// (#227).
	if reused && db_ready(db) {
		return db
	}

	// Lock everything below here to prevent race conditions when modifying the schema
	l := lock(path)
	l.Lock()
	defer l.Unlock()

	// Re-check under the lock: the creator may have finished (or failed —
	// in which case this opener retries the creation itself) while we waited.
	if db_ready(db) {
		return db
	}

	// Get schema version from user_version pragma
	schema := db_app_schema_get(db)

	// Check if app tables exist - if not, call database_create()
	// We always check actual database state rather than relying on file creation status,
	// because multiple goroutines may race to create the same database file.
	has_tables, _ := db.exists("select name from sqlite_master where type='table'")
	if !has_tables {
		debug("Database app creating %q", path)

		if av.Database.Create.Function != "" {
			// starlark_db stamps user_version inside the same transaction as
			// the app's DDL, so a crash at any point leaves an empty file
			// (rolled back) rather than a partial schema the has_tables
			// check above would mistake for a complete one (#227).
			if err := av.starlark_db(db, u, av.Database.Create.Function, nil, av.Database.Schema); err != nil {
				warn("App %q version %q database create error: %v", av.app.id, av.Version, err)
				return nil
			}
		} else if av.Database.create_function != nil {
			// Go create functions are core-internal and idempotent; they keep
			// the plain pool path.
			av.Database.create_function(db)
			db_app_schema_set(db, av.Database.Schema)
		} else {
			warn("App %q version %q has no way to create database file %q", av.app.id, av.Version, av.Database.File)
			return nil
		}
		schema = av.Database.Schema
	}

	if schema < av.Database.Schema && av.Database.Upgrade.Function != "" {
		for version := schema + 1; version <= av.Database.Schema; version++ {
			debug("Database %q upgrading to schema version %d", path, version)
			if err := av.starlark_db(db, u, av.Database.Upgrade.Function, sl_encode_tuple(version), version); err != nil {
				// A migration that called mochi.db.abort() is blocked on
				// something transient, not broken: leave the version where it
				// was and stop, so the same step retries verbatim next time
				// rather than being consumed and needing a repair version.
				var abort *MigrationAbort
				if errors.As(err, &abort) {
					warn("App %q version %q database upgrade to %d aborted, version held: %s", av.app.id, av.Version, version, abort.Reason)
					break
				}
				warn("App %q version %q database upgrade error: %v", av.app.id, av.Version, err)
				// A failed migration still consumes its version number - the repair ships
				// as the next one - but its DDL rolled back with the transaction.
				db_app_schema_set(db, version)
			}
			audit_app_schema_migrated(av.app.id, version-1, version)
		}
	} else if schema > av.Database.Schema && av.Database.Downgrade.Function != "" {
		for version := schema; version > av.Database.Schema; version-- {
			debug("Database %q downgrading from schema version %d", path, version)
			if err := av.starlark_db(db, u, av.Database.Downgrade.Function, sl_encode_tuple(version), version-1); err != nil {
				warn("App %q version %q database downgrade error: %v", av.app.id, av.Version, err)
				db_app_schema_set(db, version-1)
			}
			audit_app_schema_migrated(av.app.id, version, version-1)
		}
	}

	// No infra tables on the data DB: the commit-hook and broadcast tables live on
	// the per-app system DB, and a copy here is app-writable and shadows the real
	// one. Never set on the error returns above, so a failed create is retried
	// (#227).
	db_ready_set(db)

	return db
}

// db_app_system opens the system database (app.db) for an app.
// Contains the access table managed by the platform.
// Always available even if app has no declared database file.
func db_app_system(u *User, app *App) *DB {
	if u == nil || app == nil {
		return nil
	}

	path := fmt.Sprintf("users/%s/%s/app.db", u.UID, app.id)
	db, _, _ := db_open_work(path)
	if db == nil {
		return nil
	}
	db_bind(db, u, app)

	// Run the platform system-table setups (and the access-table migration) the
	// first time THIS handle is used as an app-system DB — even if a raw db_open
	// cached it first (which leaves system_setup false). Idempotent. (#111)
	if db.system_setup {
		return db
	}

	l := lock(path)
	l.Lock()
	defer l.Unlock()
	if db.system_setup {
		return db
	}

	db.access_setup()
	// Broadcast state lives on the system DB; create its tables eagerly
	// here (#424) rather than only via the send/receive paths' defensive
	// creates.
	broadcast_sequence_table_create(db)
	broadcast_received_table_create(db)
	broadcast_log_table_create(db)
	broadcast_acknowledged_table_create(db)
	broadcast_pending_table_create(db)
	db.system_setup = true

	return db
}

// db_app_system_sweep runs the idempotent app-system setups (access / journal)
// over every existing app.db at startup, so one nothing touches is still
// brought up to date.
func db_app_system_sweep() {
	users_root := filepath.Join(data_dir, "users")
	users, err := os.ReadDir(users_root)
	if err != nil {
		return
	}
	count := 0
	for _, u := range users {
		if !u.IsDir() {
			continue
		}
		apps, err := os.ReadDir(filepath.Join(users_root, u.Name()))
		if err != nil {
			continue
		}
		for _, a := range apps {
			if !a.IsDir() {
				continue
			}
			path := fmt.Sprintf("users/%s/%s/app.db", u.Name(), a.Name())
			if !file_exists(filepath.Join(data_dir, path)) {
				continue
			}
			db, _, _ := db_open_work(path)
			if db == nil || db.system_setup {
				continue
			}
			l := lock(path)
			l.Lock()
			if !db.system_setup {
				db.access_setup()
				db.system_setup = true
				count++
			}
			l.Unlock()
		}
	}
	debug("App-system sweep: setups run on %d app.db files", count)
}

// attachment_export_file is the JSON list at the root of an app's file storage
// holding the attachment rows core's per-app store used to keep, written only
// where rows existed. Each entry adds "file", the own row's stored filename (""
// if remote).
const attachment_export_file = "attachments.json"

// attachment_export_sweep moves each app.db's attachments table out of core at
// startup, writing attachment_export_file first and keeping the table if it
// cannot. It runs before any request, so a migration may read "no file" as "no
// rows" - restore_apply must therefore export a restored user before the
// account activates.
func attachment_export_sweep() {
	users_root := filepath.Join(data_dir, "users")
	users, err := os.ReadDir(users_root)
	if err != nil {
		return
	}
	exported, dropped := 0, 0
	for _, u := range users {
		if !u.IsDir() {
			continue
		}
		e, d := attachment_export_user(u.Name())
		exported += e
		dropped += d
	}
	if dropped > 0 {
		info("Attachment export: %d stores exported, %d tables dropped", exported, dropped)
	}
}

// attachment_export_user runs the attachment export-and-drop over one user's
// stores, returning how many were exported and dropped. See
// attachment_export_sweep for the semantics of each step.
func attachment_export_user(uid string) (int, int) {
	exported, dropped := 0, 0
	apps, err := os.ReadDir(filepath.Join(data_dir, "users", uid))
	if err != nil {
		return 0, 0
	}
	for _, a := range apps {
		if !a.IsDir() {
			continue
		}
		path := fmt.Sprintf("users/%s/%s/app.db", uid, a.Name())
		if !file_exists(filepath.Join(data_dir, path)) {
			continue
		}
		db, _, _ := db_open_work(path)
		if db == nil {
			continue
		}
		present, _ := db.exists("select 1 from sqlite_master where type='table' and name='attachments'")
		if !present {
			continue
		}
		files := filepath.Join(data_dir, "users", uid, a.Name(), "files")
		rows, err := db.rows("select * from attachments order by rowid")
		if err != nil {
			warn("Attachment export: unable to read %s: %v", path, err)
			continue
		}
		if len(rows) > 0 {
			if err := attachment_export_write(files, rows); err != nil {
				warn("Attachment export: unable to write %s: %v", filepath.Join(files, attachment_export_file), err)
				continue
			}
			exported++
		}
		if err := db.exec_e("drop table if exists attachments"); err != nil {
			warn("Attachment export: unable to drop the attachments table in %s: %v", path, err)
			continue
		}
		os.RemoveAll(filepath.Join(files, "thumbnails"))
		os.RemoveAll(filepath.Join(files, "previews"))
		dropped++
	}
	return exported, dropped
}

// attachment_export_write writes the rows of one store as attachment_export_file
// under files, atomically, unless an export is already there. The stored
// filename is the one core used: the id, an underscore, and the base of the
// name ("file" when the name has no base).
func attachment_export_write(files string, rows []map[string]any) error {
	target := filepath.Join(files, attachment_export_file)
	if file_exists(target) {
		return nil
	}
	entries := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		entry := map[string]any{}
		for _, column := range []string{"id", "object", "entity", "name", "size", "content_type", "creator", "caption", "description", "rank", "created"} {
			if value, ok := row[column]; ok && value != nil {
				entry[column] = value
			}
		}
		entry["file"] = ""
		id, _ := row["id"].(string)
		name, _ := row["name"].(string)
		entity, _ := row["entity"].(string)
		if id != "" && entity == "" {
			base := filepath.Base(name)
			if base == "" || base == "." || base == ".." || base == string(filepath.Separator) {
				base = "file"
			}
			entry["file"] = id + "_" + base
		}
		entries = append(entries, entry)
	}
	data, err := json.Marshal(entries)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(files, 0755); err != nil {
		return err
	}
	temporary := target + ".tmp"
	if err := os.WriteFile(temporary, data, 0644); err != nil {
		return err
	}
	if err := os.Rename(temporary, target); err != nil {
		os.Remove(temporary)
		return err
	}
	return nil
}

// db_app_schema_get reads the app database schema version from user_version pragma
func db_app_schema_get(db *DB) int {
	return db.integer("pragma user_version")
}

// db_app_schema_set writes the app database schema version to user_version pragma
func db_app_schema_set(db *DB, version int) {
	db.exec(fmt.Sprintf("pragma user_version=%d", version))
}

// A database is vacuumed only when both gates hold: db_vacuum_ratio of its
// pages on the freelist and db_vacuum_minimum reclaimable bytes.
// db_vacuum_period throttles the periodic pass, which the per-minute db_manager
// tick would otherwise run.
const (
	db_vacuum_ratio   = 0.25
	db_vacuum_minimum = 8 * 1024 * 1024
	db_vacuum_period  = 3600
)

// db_vacuum_last is the unix time of the most recent periodic pass. Read
// and written only from the single db_manager goroutine, so no lock.
var db_vacuum_last int64

// vacuum reclaims free pages from one database past the churn gate. INCREMENTAL
// databases get PRAGMA incremental_vacuum; an auto_vacuum=NONE one converts
// once. Best-effort: errors log at debug, never warn (which emails the admin)
// and never panic.
func (db *DB) vacuum() int64 {
	pages := db.integer("pragma page_count")
	if pages == 0 {
		return 0
	}
	free := db.integer("pragma freelist_count")
	size := db.integer("pragma page_size")
	if float64(free)/float64(pages) < db_vacuum_ratio || free*size < db_vacuum_minimum {
		return 0
	}

	conn, err := db.internal.Conn(context.Background())
	if err != nil {
		debug("Database vacuum unable to get connection for %q: %v", db.path, err)
		return 0
	}
	defer conn.Close()

	run := func(query string) bool {
		if _, err := conn.ExecContext(context.Background(), query); err != nil {
			debug("Database vacuum %q on %q failed: %v", query, db.path, err)
			return false
		}
		return true
	}

	run("pragma busy_timeout=30000")
	if db.integer("pragma auto_vacuum") == 2 {
		if !run("pragma incremental_vacuum") {
			return 0
		}
	} else {
		if !run("pragma auto_vacuum=INCREMENTAL") || !run("vacuum") {
			return 0
		}
	}
	run("pragma wal_checkpoint(truncate)")

	reclaimed := int64(pages*size - db.integer("pragma page_count")*size)
	info("Database vacuum reclaimed %d bytes from %q", reclaimed, db.path)
	return reclaimed
}

// db_vacuum_all vacuums every currently-open database now, returning the count
// and bytes freed. Snapshots the open set under the lock, then vacuums outside
// it.
func db_vacuum_all() (int, int64) {
	databases_lock.Lock()
	open := make([]*DB, 0, len(databases))
	for _, db := range databases {
		if db.closed == 0 {
			open = append(open, db)
		}
	}
	databases_lock.Unlock()

	count := 0
	var total int64
	for _, db := range open {
		if n := db.vacuum(); n > 0 {
			count++
			total += n
		}
	}
	return count, total
}

// db_wal_warn_bytes is the WAL size past which the watchdog force-checkpoints
// and, failing that, warns. A healthy WAL is single-digit MB. var so tests can
// lower it.
var db_wal_warn_bytes int64 = 256 * 1024 * 1024

// db_wal_warn_strikes is how many consecutive over-threshold minutes precede a
// warning, so a transient spike the next checkpoint drains never warns.
const db_wal_warn_strikes = 3

var db_wal_strikes sync.Map // db.path -> consecutive over-threshold checks

// db_wal_watchdog force-checkpoints (TRUNCATE) any open DB whose -wal has grown
// past db_wal_warn_bytes, and warns once it stays oversized across
// db_wal_warn_strikes ticks. Best-effort: a reader can block the truncate,
// which the strikes surface.
func db_wal_watchdog() {
	databases_lock.Lock()
	open := make([]*DB, 0, len(databases))
	for _, db := range databases {
		if db.closed == 0 {
			open = append(open, db)
		}
	}
	databases_lock.Unlock()

	for _, db := range open {
		if st, err := os.Stat(db.path + "-wal"); err != nil || st.Size() < db_wal_warn_bytes {
			db_wal_strikes.Delete(db.path)
			continue
		}
		// Force a truncate checkpoint to reclaim it.
		if conn, err := db.internal.Conn(context.Background()); err == nil {
			// Short lock-wait: if a reader is starving the checkpoint, waiting
			// won't help (the strike + warn handle the persistent case); an
			// uncontended checkpoint still completes regardless.
			_, _ = conn.ExecContext(context.Background(), "PRAGMA busy_timeout=1000")
			_, _ = conn.ExecContext(context.Background(), "PRAGMA wal_checkpoint(TRUNCATE)")
			_ = conn.Close()
		}
		// A transient spike (e.g. a just-finished bootstrap land) drained above
		// and accrues no strike; only a WAL the checkpoint couldn't reclaim does.
		st, err := os.Stat(db.path + "-wal")
		if err != nil || st.Size() < db_wal_warn_bytes {
			db_wal_strikes.Delete(db.path)
			continue
		}
		n := 1
		if v, ok := db_wal_strikes.Load(db.path); ok {
			n = v.(int) + 1
		}
		db_wal_strikes.Store(db.path, n)
		if n == db_wal_warn_strikes {
			warn("Database WAL runaway: %q -wal is %d MB after %d min, checkpoint starved (a long-lived reader pinning an old frame).", db.path, st.Size()/(1024*1024), n)
		}
	}
}

// db_integrity_period is how often each open DB is re-checked for corruption.
var db_integrity_period int64 = 3600

// db_integrity_per_check_maximum bounds how many DBs the watchdog quick_checks per
// tick, so a host with many large DBs spreads the scan load instead of stalling
// on a thundering herd of full-DB checks. var so tests can lift the cap.
var db_integrity_per_check_maximum = 2

// db_integrity_state maps db.path -> last-ok unix time (int64), or the string
// "corrupt" once a DB has been flagged (so it isn't re-scanned or re-alerted).
// Both the proactive watchdog and a reactive background-write fault write
// the "corrupt" marker here, so they share one quarantine + one alert.
var db_integrity_state sync.Map

// db_quarantined reports whether a DB has been flagged corrupt, by the watchdog
// or by a background write. exec_bg skips such a DB so it cannot crash-loop.
// The flag is in-memory: it clears on restart and when a reseed swaps a fresh
// copy in.
func db_quarantined(path string) bool {
	v, ok := db_integrity_state.Load(path)
	return ok && v == "corrupt"
}

// db_quarantine flags a DB corrupt and alerts the admin ONCE (only on the
// transition into corrupt), sharing db_integrity_state with the watchdog so a
// reactive quarantine and the proactive scan never double-alert.
func db_quarantine(path, context string, err error) {
	previous, _ := db_integrity_state.Load(path)
	db_integrity_state.Store(path, "corrupt")
	if previous != "corrupt" {
		warn("Database %q corrupt during %s: %v — quarantined; further operations on it are skipped until it is repaired (recover from backup / reseed).", path, context, err)
	}
}

// db_quarantine_clear lifts a corruption flag — called when a bootstrap reseed
// has replaced the file with a fresh, verified copy, so background ops resume.
func db_quarantine_clear(path string) {
	if db_quarantined(path) {
		db_integrity_state.Delete(path)
		info("Database %q quarantine cleared (replaced by a fresh copy).", path)
	}
}

// db_error_is_corruption matches the sqlite errors that mean the file is
// structurally bad — the same set db_quick_check treats as definitive
// corruption (db_snapshot.go).
func db_error_is_corruption(err error) bool {
	if err == nil {
		return false
	}
	message := err.Error()
	return strings.Contains(message, "malformed") ||
		strings.Contains(message, "not a database") ||
		strings.Contains(message, "disk image is malformed") ||
		strings.Contains(message, "corrupt")
}

// db_error_is_transient reports whether err is a RETRYABLE write failure — lock
// contention or storage pressure — rather than a permanent one (schema drift,
// constraint, malformed SQL). It decides how exec_bg words its warning.
// Parallel-queue delivery applies N ops to one peer concurrently, so a lock
// timeout is a normal transient event under load, not a bug.
func db_error_is_transient(err error) bool {
	if err == nil {
		return false
	}
	message := err.Error()
	return strings.Contains(message, "database is locked") ||
		strings.Contains(message, "database table is locked") ||
		strings.Contains(message, "SQLITE_BUSY") ||
		strings.Contains(message, "SQLITE_LOCKED") ||
		strings.Contains(message, "disk I/O error") ||
		strings.Contains(message, "database or disk is full") ||
		strings.Contains(message, "disk is full")
}

// db_integrity_watchdog quick_checks a few due DBs each tick and warns the
// moment one is corrupt. The check is read-only, and a transient open/lock miss
// (ran=false) is ignored rather than read as corruption.
func db_integrity_watchdog() {
	databases_lock.Lock()
	open := make([]*DB, 0, len(databases))
	for _, db := range databases {
		if db.closed == 0 {
			open = append(open, db)
		}
	}
	databases_lock.Unlock()

	checked := 0
	for _, db := range open {
		if checked >= db_integrity_per_check_maximum {
			break
		}
		if v, ok := db_integrity_state.Load(db.path); ok {
			if v == "corrupt" {
				continue // already flagged + alerted
			}
			if t, ok := v.(int64); ok && now()-t < db_integrity_period {
				continue // checked clean recently
			}
		}
		result, ran := db_quick_check(db.path)
		if !ran {
			continue // couldn't run (locked/transient) — retry next cycle, no alert
		}
		checked++
		if result == "ok" {
			db_integrity_state.Store(db.path, now())
			continue
		}
		db_integrity_state.Store(db.path, "corrupt")
		warn("Database integrity: %q is corrupt — quick_check: %s. Recover from backup.", db.path, result)
	}
}

func db_manager() {
	for range time.Tick(time.Minute) {
		now := now()
		db_wal_watchdog()
		db_integrity_watchdog()
		queue_watchdog()
		qid_prune_due(now)
		pass := now-db_vacuum_last >= db_vacuum_period

		// Collect under the lock, but vacuum and close outside it: both
		// can hold a write lock on the file, and must not block every
		// other db_open while they run.
		var evicting, live []*DB
		databases_lock.Lock()
		for _, db := range databases {
			if db.closed > 0 && db.closed < now-60 {
				evicting = append(evicting, db)
				delete(databases, db.key)
			} else if pass {
				live = append(live, db)
			}
		}
		databases_lock.Unlock()

		// 2b: reclaim each idle database at the zero-contention moment
		// just before its handles close.
		for _, db := range evicting {
			db.vacuum()
			db.stmts_close()
			db.internal.Close()
			db.starlark.Close()
		}

		// 2a (primary): reclaim the still-open databases in place. Core
		// DBs and busy user DBs never go idle, so this periodic pass -
		// not the eviction path above - is what keeps them compact.
		if pass {
			for _, db := range live {
				db.vacuum()
			}
			db_vacuum_last = now
		}
	}
}

func db_open(file string) *DB {
	db, _, _ := db_open_work(file)
	return db
}

// db_path_contained reports whether an already-joined path is still inside the
// data directory. filepath.Join resolves ".." lexically, so a component
// carrying one escapes; every component is constrained elsewhere, and this is
// the one place every database path passes through. Lexical only - a symlink
// out of the directory resolves.
func db_path_contained(path string) bool {
	// Rel cleans both arguments, so an operator config carrying a trailing
	// slash or an interior ".." needs no separate tidying here.
	relative, err := filepath.Rel(data_dir, path)
	if err != nil {
		return false
	}
	return relative != "." && relative != ".." && !strings.HasPrefix(relative, ".."+string(os.PathSeparator))
}

func db_open_work(file string, keys ...string) (*DB, bool, bool) {
	path := filepath.Join(data_dir, file)
	if !db_path_contained(path) {
		warn("Database refusing to open %q: outside the data directory", file)
		return nil, false, false
	}
	key := path
	if len(keys) > 0 && keys[0] != "" {
		key = keys[0]
	}

	databases_lock.Lock()
	db, found := databases[key]
	if found {
		// Touch back to in-use while still under the lock the
		// pruning loop reads `closed` under.
		db.closed = 0
	}
	databases_lock.Unlock()
	if found {
		//debug("Database reusing already open %q", path)
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
		existing.closed = 0
		databases_lock.Unlock()
		db.internal.Close()
		db.starlark.Close()
		return existing, false, true
	}
	databases[key] = db
	databases_lock.Unlock()

	return db, created, false
}

// db_transient_dbs are the host-local core DBs whose contents are re-derived
// after loss (queue from journals, sessions by re-auth, peers by re-discovery),
// so a corrupt one is rebuilt fresh. Cold DBs are deliberately absent: those
// need operator restore.
var db_transient_dbs = []string{"queue", "sessions", "peers"}

// db_heal_transient deletes a corrupt transient core DB at startup so it
// rebuilds fresh, and returns true iff one is now missing - the only case in
// which the caller re-runs db_create. Never on a healthy start.
func db_heal_transient() bool {
	rebuild := false
	for _, name := range db_transient_dbs {
		path := filepath.Join(data_dir, "db", name+".db")
		if !file_exists(path) {
			rebuild = true // missing → its schema must be (re)created
			continue
		}
		if result, ran := db_quick_check(path); ran && result != "ok" {
			warn("Transient core DB %q corrupt at startup (%s) — deleting so it rebuilds fresh; its data is re-derived (queue from journals / sessions re-auth / peers re-discovery).", path, result)
			for _, suffix := range []string{"", "-wal", "-shm"} {
				_ = os.Remove(path + suffix)
			}
			rebuild = true
		}
	}
	return rebuild
}

func db_start() bool {
	fresh := !file_exists(filepath.Join(data_dir, "db", "users.db"))
	// db_create must NOT run on every start: it would recreate a missing migrated
	// DB with only its base schema, after which db_upgrade skips its migrations
	// (the version already reads current) and leaves the schema silently
	// incomplete.
	rebuild := db_heal_transient()
	switch {
	case fresh:
		db_create()
	case rebuild:
		db_create()
		db_upgrade()
	default:
		db_upgrade()
	}
	go db_manager()
	return fresh
}

func db_upgrade() {
	db_migrating.Add(1)
	defer db_migrating.Add(-1)
	schema := atoi(setting_get("schema", ""), 1)

	if schema > schema_version {
		panic(fmt.Sprintf("Database schema version %d is newer than this server supports (version %d). Downgrade is not supported.", schema, schema_version))
	}

	for schema < schema_version {
		next := schema + 1
		info("Upgrading database schema from version %d to %d", schema, next)
		switch next {
		// Future migrations: add `case N: db_upgrade_N()`, bump schema_version, and
		// provide the matching db_upgrade_N.
		case 2:
			db_upgrade_2()
		case 3:
			db_upgrade_3()
		case 4:
			db_upgrade_4()
		case 5:
			db_upgrade_5()
		case 6:
			db_upgrade_6()
		case 7:
			db_upgrade_7()
		case 8:
			db_upgrade_8()
		case 9:
			db_upgrade_9()
		case 10:
			db_upgrade_10()
		default:
			panic(fmt.Sprintf("No upgrade path for schema version %d", next))
		}
		setting_set("schema", fmt.Sprintf("%d", next))
		schema = next
	}
}

// db_upgrade_2 adds the per-recipient delivery health table to queue.db
// on existing installs; db_create carries the same table for fresh ones
// and transient rebuilds.
func db_upgrade_2() {
	queue := db_open("db/queue.db")
	queue.exec("create table if not exists health ( recipient text not null primary key, failures integer not null default 0, denials integer not null default 0, success integer not null default 0, since integer not null default 0, suspended integer not null default 0, probed integer not null default 0 )")
}

// db_upgrade_3 added a directory binding column that db_upgrade_4 drops again.
// The version shipped and ran on production, so it is spent: keep the no-op so
// a server still at schema 2 walks the same path production did.
func db_upgrade_3() {
}

// db_upgrade_4 collapses the directory row's three signatures into one over the
// whole row, peer included. Stored rows keep routing (verified only on
// arrival); rows from hosts that have not upgraded fail the check and age out.
func db_upgrade_4() {
	directory := db_open("db/directory.db")
	for _, column := range []string{"attestation", "binding"} {
		if have, _ := directory.exists("select 1 from pragma_table_info('entries') where name=?", column); have {
			directory.exec("alter table entries drop column " + column)
		}
	}
	// The row now stores the announcement it came from, so it can be
	// re-verified and re-served without a second signature.
	for _, column := range []string{"message", "expires"} {
		if have, _ := directory.exists("select 1 from pragma_table_info('entries') where name=?", column); !have {
			directory.exec("alter table entries add column " + column + " text not null default ''")
		}
	}
}

// db_upgrade_5 binds an API token to one action and entity: the recorded scopes
// were never enforced, so an RSS feed URL was a full-privilege credential for
// its app. Scoped tokens (the RSS family) are revoked and reminted bound on
// next use; unscoped ones are deliberate user-created API tokens and keep
// working.
func db_upgrade_5() {
	users := db_open("db/users.db")
	for _, column := range []string{"action", "entity"} {
		if have, _ := users.exists("select 1 from pragma_table_info('tokens') where name=?", column); !have {
			users.exec("alter table tokens add column " + column + " text not null default ''")
		}
	}

	rows, _ := users.rows("select hash from tokens where scopes not in ('', '[]', 'null')")
	if len(rows) == 0 {
		return
	}
	sessions := db_open("db/sessions.db")
	for _, row := range rows {
		hash, _ := row["hash"].(string)
		if hash == "" {
			continue
		}
		users.exec("delete from tokens where hash = ?", hash)
		sessions.exec("delete from accesses where hash = ?", hash)
	}
	info("Revoked %d scoped API token(s) that predate action binding; feed URLs will be reissued on next use", len(rows))
}

func (db *DB) close() {
	databases_lock.Lock()
	db.closed = now()
	databases_lock.Unlock()
}

// db_ready reports whether db_app has finished schema create/upgrade and the
// infra tables on this handle (#227). See the DB.ready field comment.
func db_ready(db *DB) bool {
	databases_lock.Lock()
	r := db.ready
	databases_lock.Unlock()
	return r
}

func db_ready_set(db *DB) {
	databases_lock.Lock()
	db.ready = true
	databases_lock.Unlock()
}

// db_bind associates a pooled handle with its user/app identity once. The pool
// key embeds both, so the values never change for a handle; binding under
// databases_lock keeps concurrent holders from racing (#227). Also heals an
// unbound cached handle.
func db_bind(db *DB, u *User, app *App) {
	databases_lock.Lock()
	if db.user == nil {
		db.user = u
		db.app = app
	}
	databases_lock.Unlock()
}

// db_purge_prefix closes and evicts every cached DB under the given directory.
// Call it before removing a directory so stale handles cannot be reused for
// I/O.
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

// db_statement_cache_maximum bounds the per-DB prepared-statement cache. On
// overflow the whole cache is flushed (closing a statement is safe even
// if one is mid-flight — database/sql reference-counts open uses), so
// dynamically-built SQL can't grow it without bound.
const db_statement_cache_maximum = 512

// prepared returns a cached prepared statement on the internal pool, or nil to
// fall back to the uncached path. These are pool-level and never used inside a
// transaction, so they cannot leak a write out of one.
func (db *DB) prepared(query string) *sqlx.Stmt {
	// Never cache while a migration runs, and never cache schema introspection: a
	// cached statement carries a stale schema view on a pooled connection, which
	// made a table_info guard miss a present column and re-run its ALTER (#10).
	if db_migrating.Load() > 0 || sql_is_introspection(query) {
		return nil
	}
	db.statement_lock.Lock()
	defer db.statement_lock.Unlock()
	if st, ok := db.statement_cache[query]; ok {
		return st
	}
	if db.statement_cache == nil {
		db.statement_cache = make(map[string]*sqlx.Stmt)
	}
	if len(db.statement_cache) >= db_statement_cache_maximum {
		for _, st := range db.statement_cache {
			st.Close()
		}
		db.statement_cache = make(map[string]*sqlx.Stmt)
	}
	st, err := db.internal.Preparex(query)
	if err != nil {
		return nil // fall back to the uncached path
	}
	db.statement_cache[query] = st
	return st
}

// stmts_close closes every cached prepared statement. Called from the
// db_manager eviction path before the pool is closed.
func (db *DB) stmts_close() {
	db.statement_lock.Lock()
	defer db.statement_lock.Unlock()
	for _, st := range db.statement_cache {
		st.Close()
	}
	db.statement_cache = nil
}

// statement_closed reports database/sql's "statement is closed" sentinel, which
// a cached statement hits when a concurrent stmts_close beats the caller. The
// helpers retry uncached. The sentinel is unexported, so match on its stable
// message.
func statement_closed(err error) bool {
	return err != nil && strings.Contains(err.Error(), "statement is closed")
}

// db_migrating is >0 while a schema migration runs (db_create/db_upgrade).
// prepared() returns nil during that window so every migration statement
// is prepared fresh, never carrying a stale schema view across the
// migration's DDL (#10). A counter so nested migrations compose.
var db_migrating atomic.Int32

// sql_is_introspection reports whether a query reads schema metadata. Such a
// query must never be cached: a stale schema view breaks migration idempotency
// guards (#10).
func sql_is_introspection(query string) bool {
	q := strings.ToLower(query)
	return strings.Contains(q, "pragma_") ||
		strings.Contains(q, "sqlite_master") ||
		strings.Contains(q, "sqlite_schema")
}

// sql_is_schema reports whether a statement changes the schema (DDL), which
// invalidates the prepared-statement cache: statements compiled against the old
// schema return stale or empty results afterwards, so the cache is flushed.
func sql_is_schema(query string) bool {
	verb, _ := sql_take_word(sql_strip_lead(query))
	switch strings.ToUpper(verb) {
	case "ALTER", "CREATE", "DROP", "REINDEX":
		return true
	}
	return false
}

// exec runs a write and panics (via must) on any sqlite error — the fail-fast
// contract for foreground/request and startup callers. Background goroutines
// must use exec_bg instead so one corrupt user DB can't crash the multi-user
// process.
func (db *DB) exec(query string, values ...any) {
	must(db.exec_e(query, values...))
}

// exec_e is exec that RETURNS the sqlite error instead of panicking. Same
// prepared-cache + DDL-flush behaviour; the caller decides how to handle the
// error. Used by exec_bg (and any other path that needs to recover rather than
// die on a DB fault).
func (db *DB) exec_e(query string, values ...any) error {
	// DDL changes the schema, which invalidates cached statements; run it
	// uncached and flush. (Migrations run DDL through db.exec.)
	if sql_is_schema(query) {
		if _, err := db.internal.Exec(query, values...); err != nil {
			return err
		}
		db.stmts_close()
		return nil
	}
	if st := db.prepared(query); st != nil {
		if _, err := st.Exec(values...); !statement_closed(err) {
			return err
		}
		// cached statement closed by a concurrent cache flush; retry uncached
	}
	_, err := db.internal.Exec(query, values...)
	return err
}

// exec_bg is the background-safe write: it never panics, so a corrupt user DB
// cannot take down the process. A quarantined DB is skipped; a corruption error
// quarantines it and alerts once. `context` names the operation for the alert.
//
// Returns nothing. It used to report a wrote/retryable/skipped tri-state for a
// consumer that decided whether to retry; that consumer went in July 2026, and
// every one of the callers left discards the result. A retryable failure is
// reported in the warning and nowhere else.
func (db *DB) exec_bg(context, query string, values ...any) {
	if db == nil || db_quarantined(db.path) {
		return
	}
	if err := db.exec_e(query, values...); err != nil {
		if db_error_is_corruption(err) {
			db_quarantine(db.path, context, err)
			return
		}
		if db_error_is_transient(err) {
			// Retryable: the failure is lock contention or storage pressure, so
			// the same statement could succeed later.
			warn("Background DB write failed (%s, retryable) on %q: %v", context, db.path, err)
			return
		}
		warn("Background DB write failed (%s) on %q: %v", context, db.path, err)
	}
}

func (db *DB) exists(query string, values ...any) (bool, error) {
	var r *sql.Rows
	var err error
	if st := db.prepared(query); st != nil {
		r, err = st.Query(values...)
		if statement_closed(err) {
			// cached statement closed by a concurrent cache flush; retry uncached
			r, err = db.internal.Query(query, values...)
		}
	} else {
		r, err = db.internal.Query(query, values...)
	}
	if err != nil {
		return false, err
	}
	defer r.Close()
	return r.Next(), nil
}

// integer returns the first column as an integer, or 0 on error
func (db *DB) integer(query string, values ...any) int {
	var result int
	var err error
	if st := db.prepared(query); st != nil {
		err = st.QueryRow(values...).Scan(&result)
		if statement_closed(err) {
			// cached statement closed by a concurrent cache flush; retry uncached
			err = db.internal.QueryRow(query, values...).Scan(&result)
		}
	} else {
		err = db.internal.QueryRow(query, values...).Scan(&result)
	}
	if err != nil {
		return 0
	}
	return result
}

// integer64 reads a single integer column as int64, so a value beyond the
// 32-bit range is not truncated on the 32-bit builds. Returns 0 on
// no-row/error.
//
// lint:ignore U1000 exists to stop a >2.1e9 column being truncated on the
// 32-bit armhf and armv7hl builds
func (db *DB) integer64(query string, values ...any) int64 {
	var result int64
	var err error
	if st := db.prepared(query); st != nil {
		err = st.QueryRow(values...).Scan(&result)
		if statement_closed(err) {
			// cached statement closed by a concurrent cache flush; retry uncached
			err = db.internal.QueryRow(query, values...).Scan(&result)
		}
	} else {
		err = db.internal.QueryRow(query, values...).Scan(&result)
	}
	if err != nil {
		return 0
	}
	return result
}

func (db *DB) row(query string, values ...any) (map[string]any, error) {
	var r *sqlx.Rows
	var err error
	if st := db.prepared(query); st != nil {
		r, err = st.Queryx(values...)
		if statement_closed(err) {
			// cached statement closed by a concurrent cache flush; retry uncached
			r, err = db.internal.Queryx(query, values...)
		}
	} else {
		r, err = db.internal.Queryx(query, values...)
	}
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

	var r *sqlx.Rows
	var err error
	if st := db.prepared(query); st != nil {
		r, err = st.Queryx(values...)
		if statement_closed(err) {
			// cached statement closed by a concurrent cache flush; retry uncached
			r, err = db.internal.Queryx(query, values...)
		}
	} else {
		r, err = db.internal.Queryx(query, values...)
	}
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
	var err error
	if st := db.prepared(query); st != nil {
		err = st.QueryRowx(values...).StructScan(out)
		if statement_closed(err) {
			// cached statement closed by a concurrent cache flush; retry uncached
			err = db.internal.QueryRowx(query, values...).StructScan(out)
		}
	} else {
		err = db.internal.QueryRowx(query, values...).StructScan(out)
	}
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
	if st := db.prepared(query); st != nil {
		if err := st.Select(out, values...); !statement_closed(err) {
			return err
		}
		// cached statement closed by a concurrent cache flush; retry uncached
	}
	return db.internal.Select(out, query, values...)
}

// starlark_db runs one of the app's database lifecycle functions and stamps
// user_version in a single transaction on a dedicated connection, so a crash
// cannot leave a partial schema (#227). The connection is the thread's
// "lifecycle" local, so mochi.db.* calls run on it and never re-enter db_app,
// whose lock this call holds.
func (av *AppVersion) starlark_db(db *DB, u *User, function string, args sl.Tuple, stamp int) error {
	pool, err := sqlitedrv.Open(db.path, db_setup_conn_lifecycle)
	if err != nil {
		return fmt.Errorf("lifecycle open: %w", err)
	}
	lifecycle := sqlx.NewDb(pool, "sqlite3")
	defer lifecycle.Close()

	ctx := context.Background()
	conn, err := lifecycle.Connx(ctx)
	if err != nil {
		return fmt.Errorf("lifecycle connection: %w", err)
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, "begin immediate"); err != nil {
		return fmt.Errorf("lifecycle begin: %w", err)
	}
	committed := false
	// Roll back on any failure or panic. A no-op error after a successful
	// commit, silently dropped.
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), "rollback")
		}
	}()

	s := av.starlark()
	s.set("app", av.app)
	s.set("user", u)
	s.set("owner", u)
	s.set("database", db)
	s.set("lifecycle", conn)
	if _, err := s.call(function, args); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("pragma user_version=%d", stamp)); err != nil {
		return fmt.Errorf("lifecycle stamp: %w", err)
	}
	if _, err := conn.ExecContext(ctx, "commit"); err != nil {
		return fmt.Errorf("lifecycle commit: %w", err)
	}
	committed = true
	return nil
}

// db_lifecycle_conn returns the dedicated lifecycle connection when the
// current Starlark call is a database lifecycle function, else nil. The
// mochi.db builtins prefer it so their statements join the lifecycle
// transaction (and see its uncommitted DDL) instead of running on the pool.
func db_lifecycle_conn(t *sl.Thread) *sqlx.Conn {
	conn, _ := t.Local("lifecycle").(*sqlx.Conn)
	return conn
}

// db_for_thread resolves the correct per-user database for the current Starlark
// thread, applying the same authentication-vs-routing rules used by
// mochi.db.execute and mochi.db.transaction. Returns the DB, or an error
// describing why the lookup failed.
func db_for_thread(t *sl.Thread) (*DB, error) {
	// Inside a database lifecycle function the handle is already resolved —
	// and this goroutine holds lock(path), so re-entering db_app below would
	// self-deadlock. The query builtins check db_lifecycle_conn first and
	// never reach this; the local is insurance for any other resolver caller.
	if db, ok := t.Local("database").(*DB); ok && db != nil {
		return db, nil
	}

	db_user, err := principal_storage(t)
	if err != nil {
		return nil, err
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

// mochi.db.execute/exists/query/row/rows(sql, params...) -> int/bool/list/dict/list: Execute database query (execute returns rows affected)
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

	// The read APIs must not smuggle a write: reads and writes have distinct
	// semantics (return values, hooks), so a mutation through row/rows/exists
	// is always a mistake. Reject it — writes go through mochi.db.execute (or
	// mochi.db.transaction).
	switch fn.Name() {
	case "mochi.db.exists", "mochi.db.row", "mochi.db.rows":
		if sql_is_mutating(query) {
			return sl_error(fn, "%s cannot run a mutating statement (INSERT/UPDATE/DELETE/REPLACE); use mochi.db.execute", fn.Name())
		}
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

	// Inside a database lifecycle function, statements run on the dedicated
	// lifecycle connection so they join its transaction (and don't re-enter
	// db_app, whose lock this goroutine holds — #227). Its rollback is owned
	// by starlark_db, not the per-call defensive rollback below.
	conn := db_lifecycle_conn(t)
	lifecycle := conn != nil
	// Migrations keep a background context deliberately: a schema rebuild on a
	// large database can legitimately outrun the call timeout, and interrupting
	// one halfway is far worse than letting it finish. Ordinary queries take
	// the call's context so a timed-out call stops its statement.
	ctx := context.Background()
	if !lifecycle {
		ctx = starlark_context(t)
		db, err := db_for_thread(t)
		if err != nil {
			return sl_error(fn, "%v", err)
		}

		// Check out a dedicated connection so a failed multi-statement query cannot
		// return a half-open transaction to the shared pool; on error a defensive
		// ROLLBACK runs on the same connection before it is released.
		pooled, err := db.starlark.Connx(ctx)
		if err != nil {
			return sl_error(fn, "database error: %v", err)
		}
		defer pooled.Close()
		conn = pooled
	}

	switch fn.Name() {
	case "mochi.db.execute":
		res, err := conn.ExecContext(ctx, query, as...)
		if err != nil {
			if !lifecycle {
				db_starlark_rollback(conn)
			}
			return sl_error(fn, "database error: %v", err)
		}
		// Return the number of rows the statement changed (insert/update/
		// delete count), so apps can branch on whether a conditional write
		// took effect.
		affected, _ := res.RowsAffected()
		return sl.MakeInt64(affected), nil

	case "mochi.db.exists":
		r, err := conn.QueryContext(ctx, query, as...)
		if err != nil {
			if !lifecycle {
				db_starlark_rollback(conn)
			}
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
			if !lifecycle {
				db_starlark_rollback(conn)
			}
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
			if !lifecycle {
				db_starlark_rollback(conn)
			}
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

// db_starlark_rollback clears any half-open transaction left by a
// multi-statement Exec whose middle statement the authoriser denied. On a
// connection with no active transaction the ROLLBACK errors and is dropped,
// which is expected.
func db_starlark_rollback(conn *sqlx.Conn) {
	_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
}

// db_starlark_sql_blocked returns a message when a query starts with a keyword
// blocked from Starlark. Courtesy only, and deliberately not hardened:
// splitting on ";" would refuse "select 'a; analyze'". The authoriser is what
// actually decides.
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

// TransactionHandle is the value mochi.db.transaction() returns: execute/exists/row/
// rows routed through the SQL transaction, plus commit and rollback. Forgetting
// commit is safe - starlark.go's cleanup hook rolls back any uncommitted
// handle.
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
	res, err := h.tx.ExecContext(starlark_context(t), query, params...)
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}
	// Return rows affected, matching mochi.db.execute, so conditional writes
	// inside a transaction can branch on whether they took effect.
	affected, _ := res.RowsAffected()
	return sl.MakeInt64(affected), nil
}

func (h *TransactionHandle) sl_exists(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if h.closed {
		return sl_error(fn, "transaction is closed")
	}
	query, params, err := transaction_args(fn, args)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	r, err := h.tx.QueryContext(starlark_context(t), query, params...)
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
	r, err := h.tx.QueryxContext(starlark_context(t), query, params...)
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
	r, err := h.tx.QueryxContext(starlark_context(t), query, params...)
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

// mochi.db.transaction() -> transaction: Start a SQL transaction on the calling app's
// per-user database. Call .commit() to persist or .rollback() to discard; a
// thread that tears down without commit rolls back. Nested transactions error -
// SQLite has no real nesting.
func api_db_transaction(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 0 {
		return sl_error(fn, "syntax: mochi.db.transaction()")
	}

	// Database lifecycle functions already run inside a transaction on the
	// lifecycle connection; a second one would block on its write lock.
	if db_lifecycle_conn(t) != nil {
		return sl_error(fn, "mochi.db.transaction is not available inside database create, upgrade, or downgrade functions (they already run in a transaction)")
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

	// Opened under the call's context. thread.Cancel is only seen between
	// interpreter steps, so a statement already inside SQLite runs on holding the
	// write lock; with a context, database/sql rolls the transaction back when the
	// call is cancelled.
	tx, err := db.starlark.BeginTxx(starlark_context(t), nil)
	if err != nil {
		return sl_error(fn, "begin failed: %v", err)
	}

	h := &TransactionHandle{tx: tx}
	t.SetLocal("transactions", append(existing, h))
	return h, nil
}

// db_conn_rows runs a read query on one connection, in DB.rows' shape. The
// introspection builtins need it during a lifecycle transaction to see
// uncommitted DDL, and must use the direct PRAGMA forms: pragma_* vtables
// silently return no rows there.
func db_conn_rows(conn *sqlx.Conn, query string, args ...any) ([]map[string]any, error) {
	r, err := conn.QueryxContext(context.Background(), query, args...)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	var results []map[string]any
	for r.Next() {
		row := make(map[string]any)
		if err := r.MapScan(row); err != nil {
			return nil, err
		}
		for k, v := range row {
			if b, ok := v.([]byte); ok {
				row[k] = string(b)
			}
		}
		results = append(results, row)
	}
	return results, nil
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

	if conn := db_lifecycle_conn(t); conn != nil {
		rows, err := db_conn_rows(conn, "PRAGMA table_info("+name+")")
		if err != nil {
			return sl_error(fn, "database error: %v", err)
		}
		return sl_encode(rows), nil
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
	const query = "select name from sqlite_schema where type='table' and name not like 'sqlite_%' and name not like '\\_%' escape '\\' order by name"
	var rows []map[string]any
	if conn := db_lifecycle_conn(t); conn != nil {
		var err error
		rows, err = db_conn_rows(conn, query)
		if err != nil {
			return sl_error(fn, "database error: %v", err)
		}
	} else {
		db, err := db_for_thread(t)
		if err != nil {
			return sl_error(fn, "%v", err)
		}
		rows, err = db.rows(query)
		if err != nil {
			return sl_error(fn, "database error: %v", err)
		}
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
	if conn := db_lifecycle_conn(t); conn != nil {
		rows, err := db_conn_rows(conn, "PRAGMA index_list("+name+")")
		if err != nil {
			return sl_error(fn, "database error: %v", err)
		}
		return sl_encode(rows), nil
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

// row_string / row_int unpack scalar SQL row values defensively. The
// nil checks let api_replication_* return an empty list cleanly when
// a row was scanned with an unexpected column type instead of
// panicking the action.
func row_string(r map[string]any, key string) string {
	if v, ok := r[key].(string); ok {
		return v
	}
	return ""
}

// row_int extracts a numeric field from a CBOR-decoded payload or a row scan.
// The cbor library decodes non-negative integers into uint64 when the target is
// any, so a missing uint64 case silently reads every value as zero.
func row_int(r map[string]any, key string) int64 {
	switch v := r[key].(type) {
	case int64:
		return v
	case uint64:
		return int64(v)
	case int:
		return int64(v)
	case uint:
		return int64(v)
	case int32:
		return int64(v)
	case uint32:
		return int64(v)
	}
	return 0
}

// sql_strip_lead skips over leading whitespace and line / block comments.
func sql_strip_lead(s string) string {
	for {
		s = strings.TrimLeft(s, " \t\r\n")
		if strings.HasPrefix(s, "--") {
			if i := strings.IndexByte(s, '\n'); i >= 0 {
				s = s[i+1:]
				continue
			}
			return ""
		}
		if strings.HasPrefix(s, "/*") {
			if i := strings.Index(s, "*/"); i >= 0 {
				s = s[i+2:]
				continue
			}
			return ""
		}
		return s
	}
}

// sql_take_word reads the next contiguous run of letters as a single
// keyword. Stops at the first non-letter, returning the word and the
// remainder.
func sql_take_word(s string) (string, string) {
	i := 0
	for i < len(s) {
		c := s[i]
		if (c < 'A' || c > 'Z') && (c < 'a' || c > 'z') {
			break
		}
		i++
	}
	return s[:i], s[i:]
}

// sql_is_mutating reports whether sql changes rows. Mutations are kept out of
// the read-only mochi.db.row/rows/exists APIs, which run the write but do not
// journal it. CTE-prefixed mutations (WITH ... DELETE) are not detected; the CI
// grep gate covers them.
func sql_is_mutating(sql string) bool {
	verb, _ := sql_take_word(sql_strip_lead(sql))
	switch strings.ToUpper(verb) {
	case "INSERT", "REPLACE", "UPDATE", "DELETE":
		return true
	}
	return false
}

// db_upgrade_6 gives the totp table a pending column: an unproven enrolment
// must not overwrite the verified secret the user is currently logging in with.
func db_upgrade_6() {
	users := db_open("db/users.db")
	if have, _ := users.exists("select 1 from pragma_table_info('totp') where name=?", "pending"); !have {
		users.exec("alter table totp add column pending text not null default ''")
	}
}

// db_upgrade_9 canonicalises usernames, since a username is an email address
// and the login paths now key on the parsed form. Two rows reducing to the same
// address are LEFT ALONE - they are two accounts, and merging gives one person
// the other's data.
func db_upgrade_9() {
	users := db_open("db/users.db")
	rows, err := users.rows("select uid, username from users")
	if err != nil {
		return
	}

	changed := 0
	var collisions, unparsed []string
	for _, row := range rows {
		id := row_string(row, "uid")
		username := row_string(row, "username")
		canonical := email_address(username)
		if canonical == "" {
			unparsed = append(unparsed, fmt.Sprintf("%s (%q)", id, username))
			continue
		}
		if canonical == username {
			continue
		}
		if taken, _ := users.exists("select 1 from users where username=? and uid<>?", canonical, id); taken {
			collisions = append(collisions, fmt.Sprintf("%s (%q -> %q)", id, username, canonical))
			continue
		}
		users.exec("update users set username=? where uid=?", canonical, id)
		changed++
	}

	if changed > 0 {
		info("Schema 9: canonicalised %d username(s)", changed)
	}
	// One warning rather than one per row: warn() emails the administrator.
	if len(collisions) > 0 {
		warn("Schema 9: %d account(s) share a mailbox with another account and were left as they are, since merging them would give one person the other's data. Resolve them by hand: %s",
			len(collisions), strings.Join(collisions, ", "))
	}
	if len(unparsed) > 0 {
		warn("Schema 9: %d account(s) have a username that is not a usable email address and cannot be signed in to: %s",
			len(unparsed), strings.Join(unparsed, ", "))
	}
}

// db_upgrade_10 adds health.evicted, the timestamp of the first drop-subscriber
// dispatch for a recipient: only a stamped row is residue the cleanup sweep may
// delete. Existing rows default to 0, which correctly reads as never
// dispatched.
func db_upgrade_10() {
	queue := db_open("db/queue.db")
	if exists, _ := queue.exists("select 1 from pragma_table_info('health') where name='evicted'"); !exists {
		queue.exec("alter table health add column evicted integer not null default 0")
	}
}

// db_upgrade_8 adds the push retry queue.
func db_upgrade_8() {
	queue := db_open("db/queue.db")
	queue.exec(`create table if not exists pushes ( id text primary key, user text not null,
		account text not null, type text not null, identifier text not null default '',
		data text not null default '', app text not null default '', category text not null default '',
		object text not null default '', title text not null default '', body text not null default '',
		link text not null default '', event text not null default '', attempts integer not null default 0,
		next_retry integer not null, created integer not null )`)
	queue.exec("create index if not exists pushes_next_retry on pushes(next_retry)")
}

// db_upgrade_7 adds queue.claimed: when a row was last marked 'sending'.
// The stuck-sending safety net keyed on `created`, which is the enqueue time
// and never changes, so any row that had ever been retried was swept the
// instant it was claimed - racing the sender still holding it.
func db_upgrade_7() {
	queue := db_open("db/queue.db")
	if have, _ := queue.exists("select 1 from pragma_table_info('queue') where name=?", "claimed"); !have {
		queue.exec("alter table queue add column claimed integer not null default 0")
	}
}

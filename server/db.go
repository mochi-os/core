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
	app      *App
	kind     string
	// closed is the unix timestamp when this handle was last marked
	// idle, or 0 while in use. Always read and written under
	// databases_lock - same primitive that guards the cache map this
	// DB lives in, so no new synchronisation primitive is introduced.
	closed int64
}

// db_kind_* tag a DB handle with the per-host file role so the
// matching emit/apply pair can route replicated writes back into the
// right file on the receiver.
//   - "app-system": users/<uid>/<app>/app.db (access, attachments)
//   - "user-core" : users/<uid>/user.db (groups, accounts, interests,
//     permissions, settings, classes/services/paths/versions)
//
// Everything else (per-app data DBs, sessions.db, users.db, …) goes
// through its own replication path and leaves kind unset.
const (
	db_kind_app_system = "app-system"
	db_kind_user_core  = "user-core"
)

const (
	schema_version = 67
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

	// Users. `uid` is the globally-stable identifier used everywhere — for
	// replication, cross-host data references, FK joins, and the on-disk
	// `users/<uid>/` data directory. Callers supply the uid via the Go
	// uid() helper at INSERT time; no triggers.
	users := db_open("db/users.db")
	users.exec("create table users (uid text not null primary key, username text not null, role text not null default 'user', methods text not null default 'email', status text not null default 'active')")
	users.exec("create unique index users_username on users (username)")

	// Passkey credential definitions and sign count. Sign count is WebAuthn
	// replay-prevention state and lives here so it survives sessions.db
	// corruption. Only the cosmetic last-used timestamp lives in sessions.db.
	users.exec("create table credentials (id blob primary key, user text not null references users(uid) on delete cascade, public_key blob not null, sign_count integer not null default 0, name text not null default '', transports text not null default '', backup_eligible integer not null default 0, backup_state integer not null default 0, created integer not null)")
	users.exec("create index credentials_user on credentials(user)")

	// Recovery codes
	users.exec("create table recovery (id integer primary key, user text not null references users(uid) on delete cascade, hash text not null, created integer not null)")
	users.exec("create index recovery_user on recovery(user)")

	// TOTP secrets
	users.exec("create table totp (user text primary key references users(uid) on delete cascade, secret text not null, verified integer not null default 0, created integer not null)")

	// OAuth identity definitions (Google, GitHub, Microsoft, Facebook, X).
	// Last-used timestamp lives in sessions.db.verifications so this cold
	// reference store doesn't take a write on every OAuth login.
	users.exec("create table oauth (id integer primary key, user text not null references users(uid) on delete cascade, provider text not null, subject text not null, email text not null default '', verified integer not null default 0, name text not null default '', created integer not null, unique(provider, subject))")
	users.exec("create index oauth_user on oauth(user)")

	// API token definitions. Hot per-request "used" timestamp lives in
	// sessions.db.accesses; here we keep just the definition so token loss
	// doesn't follow sessions.db corruption.
	users.exec("create table tokens (hash text primary key not null, user text not null references users(uid) on delete cascade, app text not null, name text not null default '', scopes text not null default '', created integer not null, expires integer not null default 0)")
	users.exec("create index tokens_user on tokens(user)")
	users.exec("create index tokens_app on tokens(app)")

	// Entities
	users.exec("create table entities (id text not null primary key, private text not null, fingerprint text not null, user text not null references users(uid) on delete cascade, parent text not null default '', class text not null, name text not null, privacy text not null default 'public', data text not null default '', published integer not null default 0)")
	users.exec("create index entities_fingerprint on entities(fingerprint)")
	users.exec("create index entities_user on entities(user)")
	users.exec("create index entities_parent on entities(parent)")
	users.exec("create index entities_class on entities(class)")
	users.exec("create index entities_name on entities(name)")
	users.exec("create index entities_privacy on entities(privacy)")
	users.exec("create index entities_published on entities(published)")

	// Sessions (login codes and sessions - transient auth data)
	sessions := db_open("db/sessions.db")
	sessions.exec("create table codes ( code text not null, username text not null, expires integer not null, primary key ( code, username ) )")
	sessions.exec("create index codes_expires on codes( expires )")
	sessions.exec("create table sessions (user text not null, code text not null, secret text not null default '', expires integer not null, created integer not null default 0, accessed integer not null default 0, address text not null default '', agent text not null default '', primary key (user, code))")
	sessions.exec("create unique index sessions_code on sessions(code)")
	sessions.exec("create index sessions_expires on sessions(expires)")
	sessions.exec("create index sessions_user on sessions(user)")

	// WebAuthn ceremony sessions (temporary)
	sessions.exec("create table ceremonies (id text primary key, type text not null, user text not null default '', challenge blob not null, data text not null default '', expires integer not null)")
	sessions.exec("create index ceremonies_expires on ceremonies(expires)")

	// Partial authentication sessions (for MFA)
	sessions.exec("create table partial (id text primary key, user text not null, completed text not null default '', remaining text not null, expires integer not null)")
	sessions.exec("create index partial_expires on partial(expires)")

	// Last-login timestamps (kept here, not in users.db, so the cold reference
	// store doesn't take a write on every login)
	sessions.exec("create table logins (user text primary key, last integer not null)")

	// Per-request token access timestamps. Split out of users.db.tokens so the
	// every-request "used" write doesn't land on the cold reference store, but
	// the token definitions themselves stay in users.db so token loss doesn't
	// follow sessions.db corruption. `user` duplicated here for cascade.
	sessions.exec("create table accesses (hash text primary key not null, user text not null, used integer not null default 0)")
	sessions.exec("create index accesses_user on accesses(user)")

	// Cosmetic last-used timestamp per passkey. Sign count (replay-prevention
	// state) stays in users.db.credentials; only the cosmetic stat lives here.
	sessions.exec("create table passkeys (credential blob primary key, user text not null, last integer not null default 0)")
	sessions.exec("create index passkeys_user on passkeys(user)")

	// OAuth verification state (last time each linked identity was used to log
	// in). Split from users.db.oauth so per-login writes don't land on the cold
	// reference store. `oauth` references users.db.oauth(id); `user` duplicated
	// here for cascade.
	sessions.exec("create table verifications (oauth integer primary key, user text not null, last integer not null default 0)")
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
	queue.exec("create table if not exists queue ( id text primary key, type text not null default 'direct', target text not null, from_entity text not null, to_entity text not null, service text not null, event text not null, from_app text not null default '', from_services text not null default '', content blob not null default '', data blob not null default '', file text not null default '', expires integer not null default 0, status text not null default 'pending', attempts integer not null default 0, next_retry integer not null, last_error text not null default '', created integer not null, priority integer not null default 20 )")
	queue.exec("create index if not exists queue_status_retry on queue (status, next_retry)")
	queue.exec("create index if not exists queue_target on queue (target)")

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
	apps.exec("create table classes (class text not null primary key, app text not null)")
	apps.exec("create table services (service text not null primary key, app text not null)")
	apps.exec("create table paths (path text not null primary key, app text not null)")
	apps.exec("create table versions (app text not null primary key, version text, track text)")
	apps.exec("create table tracks (app text not null, track text not null, version text not null, primary key (app, track))")
	apps.exec("create table apps (app text not null primary key, installed integer not null)")

	// Scheduled events
	schedule := db_open("db/schedule.db")
	schedule.exec("create table schedule (id integer primary key, user text not null, app text not null, due int not null, event text not null, data text not null, interval int not null, created int not null)")
	schedule.exec("create index schedule_due on schedule(due)")
	schedule.exec("create index schedule_app_event on schedule(app, event)")

	// Replication: per-origin-peer dedup, schema-coordination buffer,
	// per-user opt-in set, outbound sequence counters, server-pair members,
	// lease-based leadership with fencing, bulk-bootstrap progress, paired
	// server compatibility tracking. See claude/plans/replication.md.
	replication := db_open("db/replication.db")
	replication.exec("create table seen (peer text not null, scope text not null, user text not null default '', sequence integer not null, applied integer not null, primary key (peer, scope, user, sequence))")
	replication.exec("create index seen_applied on seen(applied)")
	replication.exec("create table pending (peer text not null, scope text not null, user text not null default '', db text not null default '', sequence integer not null, prev integer not null default 0, schema integer not null default 0, payload blob not null, received integer not null, primary key (peer, scope, user, sequence))")
	replication.exec("create index pending_received on pending(received)")
	replication.exec("create index pending_chain on pending(peer, scope, user, db, prev)")
	replication.exec("create table hosts (user text not null, peer text not null, added integer not null, ack integer not null default 0, primary key (user, peer))")
	replication.exec("create table sequence (user text not null default '', scope text not null, next integer not null default 0, primary key (user, scope))")
	// cursor: the contiguous in-order apply watermark per inbound
	// (peer, scope, user, db) stream. Each op chains onto its db
	// stream via op.Prev so a same-row op chain can't be reordered
	// by a backlog drain. See claude/plans/replication-test.md
	// Stage 19.
	replication.exec("create table cursor (peer text not null, scope text not null, user text not null default '', db text not null default '', sequence integer not null default 0, primary key (peer, scope, user, db))")
	// tail: sender-side last-emitted sequence per (user, scope, db),
	// stamped onto each outbound op as Prev — the per-db ordering chain.
	replication.exec("create table tail (user text not null default '', scope text not null, db text not null default '', last integer not null default 0, primary key (user, scope, db))")
	replication.exec("create table pair (peer text primary key, added integer not null, role text not null default '')")
	replication.exec("create table leadership (scope text not null, key text not null, peer text not null, expires integer not null, fence integer not null default 0, primary key (scope, key))")
	replication.exec("create index leadership_expires on leadership(expires)")
	replication.exec("create table fence_witness (scope text not null, key text not null, fence integer not null default 0, peer text not null default '', seen integer not null default 0, primary key (scope, key))")
	replication.exec("create table bootstrap (scope text not null, peer text not null, position text not null default '', state text not null default 'queued', failed integer not null default 0, primary key (scope, peer))")
	// bootstrap_served: source-side tracking of scopes we're currently
	// serving to each joined peer. Inserted on join approval (one row
	// per scope), deleted when the receiver acks `bootstrap/scope/done`.
	// Symmetry with the receiver's `bootstrap` table — the receiver
	// sees "syncing" while it pulls; the source sees "syncing" while
	// these rows exist.
	replication.exec("create table bootstrap_served (peer text not null, scope text not null, started integer not null, primary key (peer, scope))")
	replication.exec("create table schemas (peer text primary key, core integer not null default 0, apps text not null default '')")
	// Per-user link-requests awaiting Approve / Deny in Settings → Replication.
	// One row per (target user on this host, source peer); newest wins via
	// INSERT OR REPLACE. Expiry is 1h from receipt; periodic sweep emits
	// link-denied(reason="expired") to the source side. See "Per-user trigger"
	// in claude/plans/replication.md.
	replication.exec("create table links (user text not null, peer text not null, label text not null default '', placeholder text not null, received integer not null, expires integer not null, primary key (user, peer))")
	replication.exec("create index links_expires on links(expires)")
	// Whole-server pair join-requests awaiting Approve / Deny on the Pair
	// page. One row per source peer; newest wins via INSERT OR REPLACE.
	// Expiry is 10 minutes from receipt; periodic sweep emits
	// join-denied(reason="expired") to the replica. See "Operator UI" in
	// claude/plans/replication.md.
	replication.exec("create table joins (peer text not null primary key, label text not null default '', received integer not null, expires integer not null)")
	replication.exec("create index joins_expires on joins(expires)")
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
	db.user = u
	if name == "user" {
		db.kind = db_kind_user_core
	}

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

// Per-database page-count cap applied to every SQLite connection via
// the db_setup_conn PRAGMA. Acts as a safety net against runaway growth
// — at the cap SQLite returns SQLITE_FULL, which the must() wrapper
// turns into a panic; operator wakes up before the disk does.
//
// Bumped 2026-05-15 from 262_144 pages (1 GB) to 6_553_600 pages
// (25 GB) so legitimate per-user app DBs can grow past 1 GB. A heavy
// feeds.db hit ~2 GB in alpha testing; rough projection for a
// medium-sized server is a few users in the 10-25 GB range, the rest
// well under 100 MB. Headroom for the busy outliers without removing
// the safety net for actually-runaway code.
//
// Total max disk per server is approximately (number of DB files) ×
// 25 GB, but in practice only a handful of DBs ever approach the cap.
const db_max_page_count = 6_553_600

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
	db.user = u
	db.app = app

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

	// Opportunistic resync probe: kick the replication pending-buffer
	// drain for this (user, app) tuple in the background. If a sender's
	// ops were deferred for schema-skew and the migration above just
	// caught us up, the drain finds them and applies without waiting
	// for the 30s manager tick. No-op when there's nothing pending.
	//
	// Routed through a package-level variable so test harnesses that
	// mutate data_dir (integration_setup, harness) can install a
	// no-op stub - the production goroutine reads data_dir
	// asynchronously, which races with the harness's host switches.
	post_migration_drain_async(u.UID, app.id)

	return db
}

// db_app_system opens the system database (app.db) for an app.
// Contains access and attachments tables managed by the platform.
// Always available even if app has no declared database file.
func db_app_system(u *User, app *App) *DB {
	if u == nil || app == nil {
		return nil
	}

	path := fmt.Sprintf("users/%s/%s/app.db", u.UID, app.id)
	db, _, reused := db_open_work(path)
	if db == nil {
		return nil
	}
	db.user = u
	db.app = app
	db.kind = db_kind_app_system

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
		case 53:
			db_upgrade_53()
		case 54:
			db_upgrade_54()
		case 55:
			db_upgrade_55()
		case 56:
			db_upgrade_56()
		case 57:
			db_upgrade_57()
		case 58:
			db_upgrade_58()
		case 59:
			db_upgrade_59()
		case 60:
			db_upgrade_60()
		case 61:
			db_upgrade_61()
		case 62:
			db_upgrade_62()
		case 63:
			db_upgrade_63()
		case 64:
			db_upgrade_64()
		case 65:
			db_upgrade_65()
		case 66:
			db_upgrade_66()
		case 67:
			db_upgrade_67()
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

// db_upgrade_60 reverts the (ts, peer) columns added in v56–v59 across
// settings.db, apps.db and domains.db. The system-LWW conflict-resolution
// scheme they backed has been replaced with simpler op-replication where
// last-applier-by-arrival-order wins (acceptable for operator-managed
// system tables — concurrent same-row writes are rare). The columns
// are dropped to keep the schema clean. SQLite ≥ 3.35 supports DROP
// COLUMN natively; ncruces/go-sqlite3 ships a recent SQLite. Idempotent.
// db_upgrade_62 adds the `state` column to replication.db.bootstrap.
// The bulk-bootstrap protocol (#66) tracks per-(scope, peer) progress
// as one of three states:
//
//   - 'queued'  — entry exists but no transfer has started yet
//   - 'active'  — transfer in progress; `position` is the resume marker
//   - 'done'    — transfer complete; further ops arrive via the live op
//     channel
//
// Existing rows (which all carry position=” from db_upgrade_50) are
// inferred as 'queued' — they've never started a real transfer. New
// rows go through the bootstrap_set_* helpers. Idempotent.
func db_upgrade_62() {
	r := db_open("db/replication.db")
	if col, _ := r.exists("select 1 from pragma_table_info('bootstrap') where name='state'"); !col {
		r.exec("alter table bootstrap add column state text not null default 'queued'")
	}
}

// db_upgrade_63 adds the bootstrap_served table — source-side tracking
// of scopes a consumer hasn't acked as done yet. Symmetry with the
// receiver's `bootstrap` table; the UI uses both to compute the
// per-pair-member Synced/Syncing status. Idempotent.
func db_upgrade_63() {
	r := db_open("db/replication.db")
	if has, _ := r.exists("select 1 from sqlite_master where type='table' and name='bootstrap_served'"); !has {
		r.exec("create table bootstrap_served (peer text not null, scope text not null, started integer not null, primary key (peer, scope))")
	}
}

// db_upgrade_64 adds a `failed` counter to the bootstrap table so the
// state machine can distinguish "every entry was attempted" from "every
// entry was successfully transferred". Before this column, the file
// scope driver decremented the pending counter on both success AND
// failure so the scope could settle to 'done' even with chunks missing;
// the receiving user landed on their dashboard with silently incomplete
// data (caught live during Stage 17 per-user link signup: ~900 MB / 35%
// of files absent on mochi2 while bootstrap state said 'done'). Now we
// settle to a new 'incomplete' state instead, and a background retry
// manager drains the failures until 'done' is reachable.
func db_upgrade_64() {
	r := db_open("db/replication.db")
	if has, _ := r.exists("select 1 from pragma_table_info('bootstrap') where name='failed'"); !has {
		r.exec("alter table bootstrap add column failed integer not null default 0")
	}
}

// db_upgrade_65 adds a priority column to the outbound queue so
// queue_process can deliver replication coordination (link approvals,
// membership changes, key transfers) ahead of bulk replication data.
// Without it a large sync flood — e.g. subscribing to a big project,
// which emits one sql/op per inserted row — buries a pending
// link/approved behind thousands of messages and the destination sits
// on "waiting for approval". Existing rows default to 20 (interactive);
// queue_priority reclassifies each new row at enqueue.
func db_upgrade_65() {
	q := db_open("db/queue.db")
	if has, _ := q.exists("select 1 from pragma_table_info('queue') where name='priority'"); !has {
		q.exec("alter table queue add column priority integer not null default 20")
	}
}

// db_upgrade_66 adds the replication apply-cursor table — the
// contiguous in-order apply watermark per inbound (peer, scope, user)
// stream — and seeds it from the existing `seen` high-water so every
// pair/link with replication history is gated on the first restart
// after upgrade. Streams with no `seen` rows (a replica that
// bootstraps after the upgrade) get their cursor from the bootstrap
// DB manifest instead. See claude/plans/replication-test.md Stage 19.
func db_upgrade_66() {
	r := db_open("db/replication.db")
	if has, _ := r.exists("select 1 from sqlite_master where type='table' and name='cursor'"); !has {
		r.exec("create table cursor (peer text not null, scope text not null, user text not null default '', sequence integer not null default 0, primary key (peer, scope, user))")
		r.exec("insert or ignore into cursor (peer, scope, user, sequence) select peer, scope, user, max(sequence) from seen group by peer, scope, user")
	}
}

// db_upgrade_67 re-keys the in-order apply gate per DB. `pending` gains
// db + prev columns — the per-db ordering chain. `cursor` is re-keyed
// (peer, scope, user, db): dropped and recreated, since the gate
// re-anchors each stream from its first post-upgrade op (Prev==0) and
// the bootstrap manifest seeds fresh replicas. `tail` is new: the
// sender's last-emitted sequence per db-stream.
func db_upgrade_67() {
	r := db_open("db/replication.db")
	if has, _ := r.exists("select 1 from pragma_table_info('pending') where name='db'"); !has {
		r.exec("alter table pending add column db text not null default ''")
	}
	if has, _ := r.exists("select 1 from pragma_table_info('pending') where name='prev'"); !has {
		r.exec("alter table pending add column prev integer not null default 0")
	}
	r.exec("create index if not exists pending_chain on pending(peer, scope, user, db, prev)")
	r.exec("drop table if exists cursor")
	r.exec("create table cursor (peer text not null, scope text not null, user text not null default '', db text not null default '', sequence integer not null default 0, primary key (peer, scope, user, db))")
	r.exec("create table if not exists tail (user text not null default '', scope text not null, db text not null default '', last integer not null default 0, primary key (user, scope, db))")
}

// db_upgrade_61 heals replication.db installs whose db_upgrade_55 ran
// against an earlier build of this session in which the joins-table
// creation was added after links. Servers that took the first build
// landed at schema 55 with `links` present but `joins` missing; the
// schema_version moved past 55 so db_upgrade_55 never re-ran. This
// migration recreates `joins` + `joins_expires` idempotently. Servers
// that already have the table are no-ops.
func db_upgrade_61() {
	r := db_open("db/replication.db")
	r.exec("create table if not exists joins (peer text not null primary key, label text not null default '', received integer not null, expires integer not null)")
	r.exec("create index if not exists joins_expires on joins(expires)")
}

func db_upgrade_60() {
	for _, target := range []struct {
		path  string
		table string
	}{
		{"db/settings.db", "settings"},
		{"db/apps.db", "classes"},
		{"db/apps.db", "services"},
		{"db/apps.db", "paths"},
		{"db/apps.db", "apps"},
		{"db/domains.db", "domains"},
		{"db/domains.db", "routes"},
	} {
		db := db_open(target.path)
		if col, _ := db.exists(fmt.Sprintf("select 1 from pragma_table_info('%s') where name='ts'", target.table)); col {
			db.exec(fmt.Sprintf("alter table %s drop column ts", target.table))
		}
		if col, _ := db.exists(fmt.Sprintf("select 1 from pragma_table_info('%s') where name='peer'", target.table)); col {
			db.exec(fmt.Sprintf("alter table %s drop column peer", target.table))
		}
	}
}

// db_upgrade_59 adds the LWW conflict-resolution columns (ts, peer) to
// domains.db.routes — composite-key (domain, path) rows replicated via
// the SystemLWWRow shape. Idempotent.
func db_upgrade_59() {
	domains := db_open("db/domains.db")
	if col, _ := domains.exists("select 1 from pragma_table_info('routes') where name='ts'"); !col {
		domains.exec("alter table routes add column ts integer not null default 0")
	}
	if col, _ := domains.exists("select 1 from pragma_table_info('routes') where name='peer'"); !col {
		domains.exec("alter table routes add column peer text not null default ''")
	}
}

// db_upgrade_58 adds the LWW conflict-resolution columns (ts, peer) to
// the domains.db.domains table. Routes and delegations follow when the
// composite-key shape is wired (their primary keys span multiple
// columns and need the SystemLWWRow row-level shape rather than the
// field-level SystemLWWSet shape used here). Idempotent.
func db_upgrade_58() {
	domains := db_open("db/domains.db")
	if col, _ := domains.exists("select 1 from pragma_table_info('domains') where name='ts'"); !col {
		domains.exec("alter table domains add column ts integer not null default 0")
	}
	if col, _ := domains.exists("select 1 from pragma_table_info('domains') where name='peer'"); !col {
		domains.exec("alter table domains add column peer text not null default ''")
	}
}

// db_upgrade_57 adds the LWW conflict-resolution columns (ts, peer) to
// the apps.db two-column routing tables (classes, services, paths)
// and the install registry (apps). Same pattern as v56 for settings.
// The three-column versions/tracks tables follow in a later migration.
// Idempotent.
func db_upgrade_57() {
	apps := db_open("db/apps.db")
	for _, t := range []string{"classes", "services", "paths", "apps"} {
		if col, _ := apps.exists(fmt.Sprintf("select 1 from pragma_table_info('%s') where name='ts'", t)); !col {
			apps.exec(fmt.Sprintf("alter table %s add column ts integer not null default 0", t))
		}
		if col, _ := apps.exists(fmt.Sprintf("select 1 from pragma_table_info('%s') where name='peer'", t)); !col {
			apps.exec(fmt.Sprintf("alter table %s add column peer text not null default ''", t))
		}
	}
}

// db_upgrade_56 adds the LWW conflict-resolution columns (ts, peer) to
// the settings.db settings table. Subsequent system-LWW replication
// ops carry (ts, peer) per write and the receiver uses them to drop
// stale incoming writes (existing local ts > incoming ts, or equal ts
// with higher peer-id lexicographically). Idempotent.
func db_upgrade_56() {
	settings := db_open("db/settings.db")
	if col, _ := settings.exists("select 1 from pragma_table_info('settings') where name='ts'"); !col {
		settings.exec("alter table settings add column ts integer not null default 0")
	}
	if col, _ := settings.exists("select 1 from pragma_table_info('settings') where name='peer'"); !col {
		settings.exec("alter table settings add column peer text not null default ''")
	}
}

// db_upgrade_55 adds two replication.db tables:
//
//   - `links` — per-user inbound link-requests awaiting Approve / Deny
//     on Settings → Replication. Keyed on (user, peer); 1h expiry.
//   - `joins` — whole-server inbound join-requests awaiting Approve /
//     Deny on the Pair page. Keyed on peer; 10-minute expiry.
//
// Idempotent: running on a DB that already has either table is a no-op.
func db_upgrade_55() {
	r := db_open("db/replication.db")
	r.exec("create table if not exists links (user text not null, peer text not null, label text not null default '', placeholder text not null, received integer not null, expires integer not null, primary key (user, peer))")
	r.exec("create index if not exists links_expires on links(expires)")
	r.exec("create table if not exists joins (peer text not null primary key, label text not null default '', received integer not null, expires integer not null)")
	r.exec("create index if not exists joins_expires on joins(expires)")
}

// db_upgrade_54 renames every user's per-user repositories data dir from
// the literal `users/<uid>/repositories/` to `users/<uid>/<app-id>/`, where
// app-id is the calling repositories app's id. On dev the dir name is
// already "repositories" so nothing moves; on every published deployment
// the data moves to `users/<uid>/1SWnPXg9xpT2Cxemw2aw8CLZCP5yDatQ6ebF9dHoMTXQNFKLuw/`
// (the canonical repositories app entity id from apps_default) so per-user
// storage finally lines up with every other path-composing API.
//
// The migration runs before apps_start, so it can't ask app_by_id. Instead
// it checks for the published-app directory under apps_root and routes:
//   - published installed: target = the published entity id
//   - otherwise: dev, leave alone (the source name already matches dev app.id)
//
// If the target directory already exists, the source is merged in to avoid
// clobbering. Per-user errors are logged and skipped — failure here can't
// orphan FKs because the data is just on-disk git repos.
func db_upgrade_54() {
	const repositories_entity = "1SWnPXg9xpT2Cxemw2aw8CLZCP5yDatQ6ebF9dHoMTXQNFKLuw"

	published := filepath.Join(data_dir, "apps", repositories_entity)
	if !file_exists(published) {
		// Dev deployment — source dir name already matches dev app.id.
		return
	}

	root := filepath.Join(data_dir, "users")
	entries, err := os.ReadDir(root)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		from := filepath.Join(root, e.Name(), "repositories")
		if !file_exists(from) {
			continue
		}
		to := filepath.Join(root, e.Name(), repositories_entity)
		if !file_exists(to) {
			if err := os.Rename(from, to); err != nil {
				warn("Database upgrade 54: rename %q -> %q failed: %v", from, to, err)
			}
			continue
		}
		// Target already exists: move each child individually rather than
		// overwrite. A repo entity-id collision is vanishingly unlikely but
		// we err on the side of preserving data.
		children, err := os.ReadDir(from)
		if err != nil {
			warn("Database upgrade 54: read %q failed: %v", from, err)
			continue
		}
		for _, c := range children {
			src := filepath.Join(from, c.Name())
			dst := filepath.Join(to, c.Name())
			if file_exists(dst) {
				warn("Database upgrade 54: %q already exists at target, leaving source in place", dst)
				continue
			}
			if err := os.Rename(src, dst); err != nil {
				warn("Database upgrade 54: rename %q -> %q failed: %v", src, dst, err)
			}
		}
		os.Remove(from)
	}
}

// db_upgrade_53 replaces the integer `users.id` primary key with the
// globally-stable `uid` everywhere. Drops the v51 dual-write triggers and
// parallel `user_uid` columns, rebuilds every FK-bearing table with a TEXT
// `user` column referencing `users(uid)`, retypes every `user`/`owner` column
// in sessions.db, domains.db and schedule.db from integer to text, and
// renames on-disk `users/<int>/` directories to `users/<uid>/` so per-user
// data paths reflect the new identifier. See claude/plans/replication.md.
func db_upgrade_53() {
	users := db_open("db/users.db")
	sessions := db_open("db/sessions.db")
	domains := db_open("db/domains.db")
	schedule := db_open("db/schedule.db")

	// Build int id -> uid map for users. v51 already filled uid in every row.
	idmap := map[int64]string{}
	rows, _ := users.rows("select id, uid from users")
	for _, r := range rows {
		id, _ := r["id"].(int64)
		uid, _ := r["uid"].(string)
		if uid == "" {
			continue
		}
		idmap[id] = uid
	}

	// Drop the v51 triggers that maintained user_uid in parallel. They're
	// about to become wrong (the rebuild will keep `uid`/`user` as the only
	// columns).
	for _, tbl := range []string{"entities", "credentials", "recovery", "totp", "oauth", "tokens"} {
		users.exec(fmt.Sprintf("drop trigger if exists %s_user_uid_insert", tbl))
		users.exec(fmt.Sprintf("drop trigger if exists %s_user_uid_update", tbl))
	}
	users.exec("drop trigger if exists users_uid_insert")

	// Foreign keys must be off while we shuffle parents and children; SQLite
	// otherwise rejects the row copies that recreate the FK target.
	users.exec("pragma foreign_keys=off")
	defer users.exec("pragma foreign_keys=on")

	// Rebuild users: uid TEXT PK, no integer id.
	users.exec("create table users_new (uid text not null primary key, username text not null, role text not null default 'user', methods text not null default 'email', status text not null default 'active')")
	users.exec("insert into users_new (uid, username, role, methods, status) select uid, username, role, methods, status from users")
	users.exec("drop table users")
	users.exec("alter table users_new rename to users")
	users.exec("create unique index users_username on users (username)")

	// Rebuild entities with user TEXT FK to users(uid).
	users.exec("create table entities_new (id text not null primary key, private text not null, fingerprint text not null, user text not null references users(uid) on delete cascade, parent text not null default '', class text not null, name text not null, privacy text not null default 'public', data text not null default '', published integer not null default 0)")
	users.exec("insert into entities_new (id, private, fingerprint, user, parent, class, name, privacy, data, published) select id, private, fingerprint, coalesce(user_uid, ''), parent, class, name, privacy, data, published from entities")
	users.exec("drop table entities")
	users.exec("alter table entities_new rename to entities")
	users.exec("create index entities_fingerprint on entities(fingerprint)")
	users.exec("create index entities_user on entities(user)")
	users.exec("create index entities_parent on entities(parent)")
	users.exec("create index entities_class on entities(class)")
	users.exec("create index entities_name on entities(name)")
	users.exec("create index entities_privacy on entities(privacy)")
	users.exec("create index entities_published on entities(published)")

	// Rebuild credentials.
	users.exec("create table credentials_new (id blob primary key, user text not null references users(uid) on delete cascade, public_key blob not null, sign_count integer not null default 0, name text not null default '', transports text not null default '', backup_eligible integer not null default 0, backup_state integer not null default 0, created integer not null)")
	users.exec("insert into credentials_new (id, user, public_key, sign_count, name, transports, backup_eligible, backup_state, created) select id, coalesce(user_uid, ''), public_key, sign_count, name, transports, backup_eligible, backup_state, created from credentials")
	users.exec("drop table credentials")
	users.exec("alter table credentials_new rename to credentials")
	users.exec("create index credentials_user on credentials(user)")

	// Rebuild recovery.
	users.exec("create table recovery_new (id integer primary key, user text not null references users(uid) on delete cascade, hash text not null, created integer not null)")
	users.exec("insert into recovery_new (id, user, hash, created) select id, coalesce(user_uid, ''), hash, created from recovery")
	users.exec("drop table recovery")
	users.exec("alter table recovery_new rename to recovery")
	users.exec("create index recovery_user on recovery(user)")

	// Rebuild totp.
	users.exec("create table totp_new (user text primary key references users(uid) on delete cascade, secret text not null, verified integer not null default 0, created integer not null)")
	users.exec("insert into totp_new (user, secret, verified, created) select coalesce(user_uid, ''), secret, verified, created from totp")
	users.exec("drop table totp")
	users.exec("alter table totp_new rename to totp")

	// Rebuild oauth.
	users.exec("create table oauth_new (id integer primary key, user text not null references users(uid) on delete cascade, provider text not null, subject text not null, email text not null default '', verified integer not null default 0, name text not null default '', created integer not null, unique(provider, subject))")
	users.exec("insert into oauth_new (id, user, provider, subject, email, verified, name, created) select id, coalesce(user_uid, ''), provider, subject, email, verified, name, created from oauth")
	users.exec("drop table oauth")
	users.exec("alter table oauth_new rename to oauth")
	users.exec("create index oauth_user on oauth(user)")

	// Rebuild tokens.
	users.exec("create table tokens_new (hash text primary key not null, user text not null references users(uid) on delete cascade, app text not null, name text not null default '', scopes text not null default '', created integer not null, expires integer not null default 0)")
	users.exec("insert into tokens_new (hash, user, app, name, scopes, created, expires) select hash, coalesce(user_uid, ''), app, name, scopes, created, expires from tokens")
	users.exec("drop table tokens")
	users.exec("alter table tokens_new rename to tokens")
	users.exec("create index tokens_user on tokens(user)")
	users.exec("create index tokens_app on tokens(app)")

	// sessions.db: retype every user / owner column from integer to text and
	// remap rows through idmap. Each table is rebuilt so SQLite can change
	// the column type cleanly.
	db_upgrade_53_retype_user(sessions, "sessions",
		"user text not null, code text not null, secret text not null default '', expires integer not null, created integer not null default 0, accessed integer not null default 0, address text not null default '', agent text not null default '', primary key (user, code)",
		"user, code, secret, expires, created, accessed, address, agent",
		idmap)
	sessions.exec("create unique index if not exists sessions_code on sessions(code)")
	sessions.exec("create index if not exists sessions_expires on sessions(expires)")
	sessions.exec("create index if not exists sessions_user on sessions(user)")

	db_upgrade_53_retype_user(sessions, "ceremonies",
		"id text primary key, type text not null, user text not null default '', challenge blob not null, data text not null default '', expires integer not null",
		"id, type, user, challenge, data, expires",
		idmap)
	sessions.exec("create index if not exists ceremonies_expires on ceremonies(expires)")

	db_upgrade_53_retype_user(sessions, "partial",
		"id text primary key, user text not null, completed text not null default '', remaining text not null, expires integer not null",
		"id, user, completed, remaining, expires",
		idmap)
	sessions.exec("create index if not exists partial_expires on partial(expires)")

	db_upgrade_53_retype_user(sessions, "logins",
		"user text primary key, last integer not null",
		"user, last",
		idmap)

	db_upgrade_53_retype_user(sessions, "accesses",
		"hash text primary key not null, user text not null, used integer not null default 0",
		"hash, user, used",
		idmap)
	sessions.exec("create index if not exists accesses_user on accesses(user)")

	db_upgrade_53_retype_user(sessions, "passkeys",
		"credential blob primary key, user text not null, last integer not null default 0",
		"credential, user, last",
		idmap)
	sessions.exec("create index if not exists passkeys_user on passkeys(user)")

	db_upgrade_53_retype_user(sessions, "verifications",
		"oauth integer primary key, user text not null, last integer not null default 0",
		"oauth, user, last",
		idmap)
	sessions.exec("create index if not exists verifications_user on verifications(user)")

	// domains.db: retype routes.owner and delegations.owner from integer to
	// text. Rebuild both tables and remap.
	db_upgrade_53_retype_owner(domains, "routes",
		"domain text not null, path text not null default '', method text not null default 'app', target text not null, context text not null default '', owner text not null default '', priority integer not null default 0, enabled integer not null default 1, created integer not null, updated integer not null, primary key (domain, path), foreign key (domain) references domains(domain) on delete cascade",
		"domain, path, method, target, context, owner, priority, enabled, created, updated",
		idmap)
	domains.exec("create index if not exists routes_domain on routes(domain)")

	db_upgrade_53_retype_owner(domains, "delegations",
		"id integer primary key, domain text not null, path text not null, owner text not null, created integer not null, updated integer not null, unique(domain, path, owner), foreign key (domain) references domains(domain) on delete cascade",
		"id, domain, path, owner, created, updated",
		idmap)
	domains.exec("create index if not exists delegations_domain on delegations(domain)")
	domains.exec("create index if not exists delegations_owner on delegations(owner)")

	// schedule.db: retype schedule.user from integer to text.
	if exists, _ := schedule.exists("select 1 from sqlite_master where type='table' and name='schedule'"); exists {
		db_upgrade_53_retype_user(schedule, "schedule",
			"id integer primary key, user text not null, app text not null, due int not null, event text not null, data text not null, interval int not null, created int not null",
			"id, user, app, due, event, data, interval, created",
			idmap)
		schedule.exec("create index if not exists schedule_due on schedule(due)")
		schedule.exec("create index if not exists schedule_app_event on schedule(app, event)")
	}

	// Rename on-disk per-user data directories from users/<int>/ to
	// users/<uid>/ so the disk path reflects the new identifier.
	root := filepath.Join(data_dir, "users")
	if entries, err := os.ReadDir(root); err == nil {
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			id := atoi(e.Name(), 0)
			if id <= 0 {
				continue
			}
			uid, ok := idmap[int64(id)]
			if !ok || uid == "" {
				warn("Database upgrade 53: no uid for user dir %q, leaving untouched", e.Name())
				continue
			}
			from := filepath.Join(root, e.Name())
			to := filepath.Join(root, uid)
			if from == to {
				continue
			}
			if err := os.Rename(from, to); err != nil {
				warn("Database upgrade 53: rename %q -> %q failed: %v", from, to, err)
			}
		}
	}
}

// db_upgrade_53_retype_user rebuilds a table that has an integer `user`
// column into the same shape with `user text`. Existing rows are remapped
// through idmap (int id -> uid). Rows with no idmap entry are dropped.
func db_upgrade_53_retype_user(db *DB, table, schema, cols string, idmap map[int64]string) {
	if exists, _ := db.exists("select 1 from sqlite_master where type='table' and name=?", table); !exists {
		db.exec(fmt.Sprintf("create table %s (%s)", table, schema))
		return
	}
	// Snapshot existing rows before drop.
	rows, _ := db.rows(fmt.Sprintf("select %s from %s", cols, table))
	db.exec(fmt.Sprintf("drop table %s", table))
	db.exec(fmt.Sprintf("create table %s (%s)", table, schema))
	column_names := strings.Split(cols, ", ")
	placeholders := strings.Repeat("?, ", len(column_names))
	placeholders = strings.TrimSuffix(placeholders, ", ")
	for _, r := range rows {
		vals := make([]any, len(column_names))
		drop := false
		for i, n := range column_names {
			if n == "user" {
				switch v := r[n].(type) {
				case int64:
					uid, ok := idmap[v]
					if !ok {
						drop = true
					}
					vals[i] = uid
				case string:
					vals[i] = v
				default:
					vals[i] = ""
				}
			} else {
				vals[i] = r[n]
			}
		}
		if drop {
			continue
		}
		db.exec(fmt.Sprintf("insert or ignore into %s (%s) values (%s)", table, cols, placeholders), vals...)
	}
}

// db_upgrade_53_retype_owner is the same as db_upgrade_53_retype_user but
// remaps an `owner` column rather than `user`.
func db_upgrade_53_retype_owner(db *DB, table, schema, cols string, idmap map[int64]string) {
	if exists, _ := db.exists("select 1 from sqlite_master where type='table' and name=?", table); !exists {
		db.exec(fmt.Sprintf("create table %s (%s)", table, schema))
		return
	}
	rows, _ := db.rows(fmt.Sprintf("select %s from %s", cols, table))
	db.exec(fmt.Sprintf("drop table %s", table))
	db.exec(fmt.Sprintf("create table %s (%s)", table, schema))
	column_names := strings.Split(cols, ", ")
	placeholders := strings.Repeat("?, ", len(column_names))
	placeholders = strings.TrimSuffix(placeholders, ", ")
	for _, r := range rows {
		vals := make([]any, len(column_names))
		for i, n := range column_names {
			if n == "owner" {
				switch v := r[n].(type) {
				case int64:
					if v == 0 {
						vals[i] = ""
					} else if uid, ok := idmap[v]; ok {
						vals[i] = uid
					} else {
						vals[i] = ""
					}
				case string:
					vals[i] = v
				default:
					vals[i] = ""
				}
			} else {
				vals[i] = r[n]
			}
		}
		db.exec(fmt.Sprintf("insert or ignore into %s (%s) values (%s)", table, cols, placeholders), vals...)
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
		replication.exec("create table bootstrap (scope text not null, peer text not null, position text not null default '', state text not null default 'queued', failed integer not null default 0, primary key (scope, peer))")
	}
	if exists, _ := replication.exists("select 1 from sqlite_master where type='table' and name='schemas'"); !exists {
		replication.exec("create table schemas (peer text primary key, core integer not null default 0, apps text not null default '')")
	}
}

func (db *DB) close() {
	databases_lock.Lock()
	db.closed = now()
	databases_lock.Unlock()
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

// db_evict_path closes and evicts the single cached DB at `relpath`
// (relative to data_dir). Use it before rename(2)-replacing a DB file
// out from under the server — e.g. when bootstrap lands a snapshot —
// so the next db_open picks up the fresh inode instead of a handle
// pinned to the now-unlinked original. Returns true if a handle was
// actually evicted. No-op if the path isn't cached.
func db_evict_path(relpath string) bool {
	full := filepath.Join(data_dir, relpath)
	var closers []*sqlx.DB
	databases_lock.Lock()
	for key, db := range databases {
		if db.path == full {
			closers = append(closers, db.internal, db.starlark)
			delete(databases, key)
		}
	}
	databases_lock.Unlock()
	for _, h := range closers {
		if h != nil {
			h.Close()
		}
	}
	return len(closers) > 0
}

func (db *DB) exec(query string, values ...any) {
	must(db.internal.Exec(query, values...))
}

// exec_replicated runs a write against the local DB and, if the handle
// is tagged with a replicating kind (app-system, user-core), fans the
// statement out to the user's host set. Used by Go-side APIs
// (mochi.access.*, mochi.group.*, …) so their writes converge across
// hosts the same way mochi.db.execute does for app-defined tables.
//
// Schema DDL (create table, create index, alter table) must keep using
// the plain exec — receivers create their own tables on first open.
func (db *DB) exec_replicated(query string, values ...any) {
	must(db.internal.Exec(query, values...))
	if db.user == nil || db.user.UID == "" {
		return
	}
	switch db.kind {
	case db_kind_app_system:
		if db.app != nil {
			replication_emit_app_system_exec(db.user, db.app, query, values)
		}
	case db_kind_user_core:
		replication_emit_user_core_exec(db.user, query, values)
	}
}

// exec_app_user runs a write against a per-user app DB and emits a
// sql/op so the same statement re-executes on every paired host.
// Used by Go-side internals that need to mutate per-user app DBs and
// have those changes converge across hosts — currently the broadcast
// SENDER-side helpers (sequence / log / acknowledged), so a paired
// host can take over emission and serve resync requests with a
// consistent log.
//
// NOTE: broadcast RECEIVER-side state (received, pending) deliberately
// does NOT use this helper. Each paired host applies inbound
// broadcasts independently; pair-replicating received caused the
// gap detector on the partner to dedup events it never actually
// applied (task #91). receiver-side writes go through plain db.exec.
//
// Schema DDL stays on plain exec — receivers create their own tables
// on first open via the `create table if not exists` helpers.
func (db *DB) exec_app_user(query string, values ...any) {
	must(db.internal.Exec(query, values...))
	if db.user == nil || db.user.UID == "" || db.app == nil {
		return
	}
	av := db.app.active(db.user)
	if av == nil {
		return
	}
	replication_emit_sql_command(db.user, db.app, av, query, values)
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

// db_user_for_thread resolves which user's perspective the current Starlark
// thread is acting from, given the user/owner/domain-routing context.
//
// Rules:
//   - logged-in user, no domain routing: that user
//   - logged-in user with domain routing: the route owner (so apps under a
//     custom domain serve the route owner's data regardless of who's signed in)
//   - anonymous (no user) with a local entity owner: that owner (so visitors
//     can read public entity content)
//   - anonymous and no owner: error (no context to act in)
//   - logged-in under domain routing with no owner: error (the route has no
//     target user to act as)
//
// The helper is shared by mochi.db.* (which propagates the error) and
// mochi.entity.* (which treats it as "no entities to show"), so the two
// namespaces stay in sync.
func db_user_for_thread(t *sl.Thread) (*User, error) {
	owner, _ := t.Local("owner").(*User)
	user, _ := t.Local("user").(*User)

	var domain_routing bool
	if action := t.Local("action"); action != nil {
		if a, ok := action.(*Action); ok && a.domain != nil && a.domain.route != nil {
			domain_routing = a.domain.route.context != ""
		}
	}

	if user == nil {
		if owner == nil {
			return nil, fmt.Errorf("no user context available")
		}
		return owner, nil
	}
	if domain_routing {
		if owner == nil {
			return nil, fmt.Errorf("no owner context for domain routing")
		}
		return owner, nil
	}
	return user, nil
}

// db_replicate_after_exec emits a replication op for a successful local
// app-DB write. Called from api_db_query (mochi.db.execute) and from
// TransactionHandle's deferred-emit flush at commit. The decision on
// whether to actually emit (table not excluded, user has UID, app
// resolvable) lives in replication_emit_sql_command.
func db_replicate_after_exec(t *sl.Thread, sql string, args []any) {
	u, err := db_user_for_thread(t)
	if err != nil || u == nil {
		return
	}
	app, _ := t.Local("app").(*App)
	if app == nil {
		return
	}
	av := app.active(u)
	if av == nil {
		return
	}
	replication_emit_sql_command(u, app, av, sql, args)
}

// db_for_thread resolves the correct per-user database for the current Starlark
// thread, applying the same authentication-vs-routing rules used by
// mochi.db.execute and mochi.db.transaction. Returns the DB, or an error
// describing why the lookup failed.
func db_for_thread(t *sl.Thread) (*DB, error) {
	db_user, err := db_user_for_thread(t)
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
		db_replicate_after_exec(t, query, as)
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
//
// Pending replication ops accumulate in pending_emits as each execute()
// lands; commit() flushes them after the SQL commit succeeds, rollback()
// drops them. This way a discarded transaction never leaves emitted ops
// without matching local state on the source.
type TransactionHandle struct {
	tx            *sqlx.Tx
	closed        bool
	pending_emits []sql_pending_emit
	user          *User
	app           *App
	av            *AppVersion
}

// sql_pending_emit is one buffered (sql, args) tuple awaiting commit.
type sql_pending_emit struct {
	sql  string
	args []any
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
			h.pending_emits = nil
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
	h.pending_emits = append(h.pending_emits, sql_pending_emit{sql: query, args: params})
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
		h.pending_emits = nil
		return sl_error(fn, "commit failed: %v", err)
	}
	h.closed = true
	for _, e := range h.pending_emits {
		replication_emit_sql_command(h.user, h.app, h.av, e.sql, e.args)
	}
	h.pending_emits = nil
	return sl.None, nil
}

func (h *TransactionHandle) sl_rollback(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if h.closed {
		return sl_error(fn, "transaction is closed")
	}
	h.tx.Rollback()
	h.closed = true
	h.pending_emits = nil
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
	if user, _ := db_user_for_thread(t); user != nil {
		if app, _ := t.Local("app").(*App); app != nil {
			h.user = user
			h.app = app
			h.av = app.active(user)
		}
	}
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

// Mochi server: Database
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"database/sql"
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
	// system_setup is set once db_app_system has run access_setup /
	// attachments_setup / journal_setup on this handle. Gated on this — NOT on
	// db_open_work's `reused` — so a handle first cached by a raw db_open (the
	// convergence audit / sweep / bootstrap reading app.db's replicated tables)
	// still gets its setups, and the access-table migration, on the first
	// db_app_system call. Without it a passive replica's access table stays on
	// the legacy schema and inbound new-schema access ops fail + deadletter (#111).
	// Guarded by lock(path) at the setup site.
	system_setup bool
	// ready is set once db_app has finished database_create/upgrade and the
	// core infra tables on this handle. The reused fast-path requires it, so
	// a concurrent opener waits on lock(path) for the creator instead of
	// querying a schema that doesn't exist yet ("no such table" on a fresh
	// user's first concurrent requests, #227). The creating goroutine's own
	// mochi.db.* calls never reach db_app — starlark_db hands the lifecycle
	// connection to the Starlark thread — so waiting here cannot
	// self-deadlock. Guarded by databases_lock, like closed.
	ready bool
	// closed is the unix timestamp when this handle was last marked
	// idle, or 0 while in use. Always read and written under
	// databases_lock - same primitive that guards the cache map this
	// DB lives in, so no new synchronisation primitive is introduced.
	closed int64

	// stmt_cache holds prepared statements for the internal pool, keyed
	// by SQL text, populated lazily by prepared(). Guarded by stmt_lock.
	// Closed on eviction (stmts_close).
	stmt_lock  sync.Mutex
	stmt_cache map[string]*sqlx.Stmt
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
	schema_version = 1
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
	// auto_vacuum must be set before journal_mode=WAL and before any
	// table exists, or it silently stays NONE (setting it after WAL is a
	// no-op even on an empty database). On a fresh file this makes every
	// new database incremental-auto-vacuum from birth, so DB.vacuum can
	// reclaim freed pages with the cheap PRAGMA incremental_vacuum. On an
	// existing populated database it is a no-op; those convert lazily in
	// DB.vacuum. Runs before the Starlark authoriser is installed, so the
	// PRAGMA write is permitted on both pools. See claude/plans/vacuum.md.
	if err := c.Exec("PRAGMA auto_vacuum=INCREMENTAL"); err != nil {
		return err
	}
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

// db_setup_conn_lifecycle configures the dedicated connection that runs an
// app's database lifecycle functions (database_create / database_upgrade /
// database_downgrade). Same rules as the Starlark pool, with two additions:
//
//   - `pragma user_version = N`: starlark_db stamps the schema version inside
//     the same transaction as the app's DDL so creation and each migration
//     step are atomic on disk (#227). Apps cannot reach PRAGMA through
//     mochi.db.* (string check in api_db_query), and even a smuggled
//     multi-statement user_version write is overwritten by the server's own
//     stamp before commit, so the allowance grants apps nothing.
//   - `pragma table_info(x)` / `pragma index_list(x)`: read-only
//     introspection, needed because mochi.db.table/indexes must run on THIS
//     connection during a migration to see its uncommitted DDL (feeds and
//     wikis use them as idempotency guards). The pragma_* virtual-table forms
//     are no alternative: their internal prepare re-fires AUTH_PRAGMA, and a
//     denial there silently yields zero rows.
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

	// TOTP secrets
	users.exec("create table if not exists totp (user text primary key references users(uid) on delete cascade, secret text not null, verified integer not null default 0, created integer not null)")

	// OAuth identity definitions (Google, GitHub, Microsoft, Facebook, X).
	// Last-used timestamp lives in sessions.db.verifications so this cold
	// reference store doesn't take a write on every OAuth login.
	users.exec("create table if not exists oauth (id integer primary key, user text not null references users(uid) on delete cascade, provider text not null, subject text not null, email text not null default '', verified integer not null default 0, name text not null default '', created integer not null, unique(provider, subject))")
	users.exec("create index if not exists oauth_user on oauth(user)")

	// API token definitions. Hot per-request "used" timestamp lives in
	// sessions.db.accesses; here we keep just the definition so token loss
	// doesn't follow sessions.db corruption.
	users.exec("create table if not exists tokens (hash text primary key not null, user text not null references users(uid) on delete cascade, app text not null, name text not null default '', scopes text not null default '', created integer not null, expires integer not null default 0)")
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

	// Directory. One row per (entity, peer): each row is one host's listing
	// of one entity, asserted by that host alone. There are no global rows;
	// a host may only publish or delete rows naming itself. Rows are
	// self-verifying: `signature` is the entity's ed25519 signature over the
	// content facts, `attestation` is the asserting host's libp2p-key
	// signature over the claim, so any row can be re-served and verified
	// regardless of how it arrived.
	directory := db_open("db/directory.db")
	directory.exec("create table if not exists entries ( entity text not null, peer text not null, name text not null, class text not null, data text not null default '', fingerprint text not null default '', version integer not null default 0, created integer not null, seen integer not null, signature text not null default '', attestation text not null default '', primary key ( entity, peer ) )")
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
	queue.exec("create table if not exists queue ( id text primary key, type text not null default 'direct', target text not null, from_entity text not null, to_entity text not null, service text not null, event text not null, from_app text not null default '', from_services text not null default '', content blob not null default '', data blob not null default '', file text not null default '', expires integer not null default 0, status text not null default 'pending', attempts integer not null default 0, next_retry integer not null, last_error text not null default '', created integer not null, priority integer not null default 20 )")
	// (status, priority, next_retry) covers BOTH queue_select queries
	// (main priority-desc + bulk-floor priority-range). Without the
	// priority column, the main query's ORDER BY priority forces a
	// 1.7M-row sort and the bulk query's priority filter scans the
	// whole "ready" set looking for non-existent priority<=10 rows.
	// On wasabi 2026-05-26 the missing index pushed queue_select to
	// 1.3s per call, capping drain at ~50 rows/sec via queue_manager.
	queue.exec("create index if not exists queue_status_priority_retry on queue (status, priority, next_retry)")
	queue.exec("create index if not exists queue_target on queue (target)")
	queue.exec("create index if not exists queue_target_priority_retry on queue (target, priority desc, next_retry)")
	// journal_inflight bridges send->ack for the per-peer journal delivery
	// cursor (#28): one row per shipped journal op, resolved when the
	// transport ACK lands so journal_delivery can advance. Co-located with
	// the ack delete so the resolve is a same-DB lookup.
	queue.exec("create table if not exists journal_inflight (id text primary key, user text not null, peer text not null, stream text not null, sequence integer not null, created integer not null)")

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
	db_bind(db, u, app, "")

	// Fast path: a reused handle whose schema this process has already
	// verified or created. Gated on ready, not reuse alone — a concurrent
	// opener can be handed the pooled handle before the first goroutine has
	// run database_create, and would otherwise query a table that doesn't
	// exist yet (#227).
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
				warn("App %q version %q database upgrade error: %v", av.app.id, av.Version, err)
				// A failed migration still consumes the version number (the
				// established repair convention: the fix ships as the NEXT
				// version) — but its partial DDL rolled back with the
				// transaction, so the repair starts from the clean previous
				// shape rather than a half-applied step.
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

	// Create the core-managed commit-hook table on this data DB eagerly at
	// open — not lazily on first use (#424). Idempotent; covers both fresh
	// and pre-existing files, and runs once per process per (path, version).
	// The broadcast tables do NOT belong here: their live copies moved to
	// the per-app SYSTEM DB (db_app_system creates them eagerly), and
	// creating them on the data DB kept regenerating the stale duplicates
	// that misled the 2026-07 News wedge diagnosis — the per-app migrations
	// dropping those duplicates rely on this not re-creating them.
	commits_table_create(db)

	// Schema and infra tables are in place; open the reused fast-path. Never
	// set on the error returns above, so a failed create is retried by the
	// next opener instead of wedging the handle (#227).
	db_ready_set(db)

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
	db, _, _ := db_open_work(path)
	if db == nil {
		return nil
	}
	db_bind(db, u, app, db_kind_app_system)

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
	db.attachments_setup()
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

// db_app_system_sweep walks every existing app.db at startup and runs the
// idempotent app-system setups (access / attachments / journal, including the
// access-table register migration). Without it, a (user, app) pair's app.db
// migrates only when something calls db_app_system for it, and on a passive
// pair member a dormant pair keeps the legacy schema indefinitely — so the
// first register op emitted after the ACTIVE side migrated used to be
// deadlettered ("table access has no column named removed"). The apply path
// now migrates on demand (db_app_system + the system_setup gate), so this
// sweep is the proactive half: it clears the dormant backlog at startup
// instead of leaving each pair to heal on its next inbound op, and stops the
// convergence audit reporting those pairs as schema-skew in the meantime.
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
				db.attachments_setup()
				db.system_setup = true
				count++
			}
			l.Unlock()
		}
	}
	debug("App-system sweep: setups run on %d app.db files", count)
}

// db_app_schema_get reads the app database schema version from user_version pragma
func db_app_schema_get(db *DB) int {
	return db.integer("pragma user_version")
}

// db_app_schema_set writes the app database schema version to user_version pragma
func db_app_schema_set(db *DB, version int) {
	db.exec(fmt.Sprintf("pragma user_version=%d", version))
}

// db_vacuum_ratio is the minimum fraction of a database's pages that
// must be on the freelist before reclaim is worthwhile; db_vacuum_minimum
// is the minimum number of reclaimable bytes. Both must hold, so a
// database that has not meaningfully churned is left untouched.
// db_vacuum_period throttles the periodic pass: the db_manager tick is
// per-minute, but the vacuum pass over open handles runs at most this
// often (seconds).
const (
	db_vacuum_ratio   = 0.25
	db_vacuum_minimum = 8 * 1024 * 1024
	db_vacuum_period  = 3600
)

// db_vacuum_last is the unix time of the most recent periodic pass. Read
// and written only from the single db_manager goroutine, so no lock.
var db_vacuum_last int64

// vacuum reclaims free pages from one database when it has churned past
// the gate (ratio and minimum). It is host-local file maintenance, not a
// logical write: it runs independently on every replica and must NOT be
// leader-gated - gating it would leave non-leader replicas' files growing
// forever. See claude/plans/vacuum.md.
//
// auto_vacuum=INCREMENTAL databases (everything created since this landed)
// get the cheap PRAGMA incremental_vacuum. Older auto_vacuum=NONE
// databases convert once: set INCREMENTAL then full VACUUM, which both
// reclaims now and flips the mode for future churn. Either way the WAL is
// checkpointed so freed pages leave the file. The work runs on one pinned
// connection with a busy timeout, so it waits for rather than fights
// concurrent writers. Best-effort: any error is logged at debug and
// skipped - never warn (which emails the admin) and never panic.
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

// db_vacuum_all runs the reclaim pass over every currently-open database
// immediately and returns how many were reclaimed and the total bytes
// freed. Backs the on-demand admin vacuum endpoint; the routine path is
// the periodic pass in db_manager. Snapshots the open set under the lock,
// then vacuums outside it so it does not block db_open.
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
// and (if it can't reclaim) warns. A healthy WAL is single-digit MB
// (auto-checkpoint defaults to ~4 MB); the runaway that corrupted feeds.db
// reached 2.9 GB with NO alert. 256 MB catches a starved checkpoint ~10x
// earlier. var (not const) so tests can lower it.
var db_wal_warn_bytes int64 = 256 * 1024 * 1024

// db_wal_warn_strikes is how many consecutive over-threshold minutes before the
// watchdog warns. A transient spike (a just-finished bootstrap land dumps the
// DB into the WAL for a few seconds) is reclaimed by the per-minute checkpoint
// below and never warns; only a SUSTAINED runaway the checkpoint can't drain —
// a genuinely starved checkpoint (a long-lived reader pinning an old WAL frame,
// or writes outpacing it) — crosses the strike count.
const db_wal_warn_strikes = 3

var db_wal_strikes sync.Map // db.path -> consecutive over-threshold checks

// db_wal_watchdog runs every db_manager tick. For each open DB whose -wal has
// grown past db_wal_warn_bytes it force-checkpoints (TRUNCATE) to reclaim it,
// and warns once the WAL stays oversized across db_wal_warn_strikes ticks — the
// checkpoint-starvation that ballooned feeds.db's WAL to 2.9 GB and led to the
// corruption (#6). Best-effort and non-fatal: a reader can block the truncate,
// but the warning then surfaces the runaway early instead of silently.
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
// The 2026-06-23 feeds.db corruption ran silently for hours before anyone
// noticed; an hourly quick_check turns that into a prompt alert.
var db_integrity_period int64 = 3600

// db_integrity_max_per_check bounds how many DBs the watchdog quick_checks per
// tick, so a host with many large DBs spreads the scan load instead of stalling
// on a thundering herd of full-DB checks. var so tests can lift the cap.
var db_integrity_max_per_check = 2

// db_integrity_state maps db.path -> last-ok unix time (int64), or the string
// "corrupt" once a DB has been flagged (so it isn't re-scanned or re-alerted).
// Both the proactive watchdog and a reactive background-write fault write
// the "corrupt" marker here, so they share one quarantine + one alert.
var db_integrity_state sync.Map

// db_quarantined reports whether a DB has been flagged corrupt — by the
// integrity watchdog or by a background write that hit corruption. Background
// ops (exec_bg) skip a quarantined DB so it can't crash-loop. The flag is
// in-memory: it clears on restart (after the operator recovers the file) and is
// cleared eagerly when a bootstrap reseed swaps a fresh copy in.
func db_quarantined(path string) bool {
	v, ok := db_integrity_state.Load(path)
	return ok && v == "corrupt"
}

// db_quarantine flags a DB corrupt and alerts the admin ONCE (only on the
// transition into corrupt), sharing db_integrity_state with the watchdog so a
// reactive quarantine and the proactive scan never double-alert.
func db_quarantine(path, context string, err error) {
	prev, _ := db_integrity_state.Load(path)
	db_integrity_state.Store(path, "corrupt")
	if prev != "corrupt" {
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
	msg := err.Error()
	return strings.Contains(msg, "malformed") ||
		strings.Contains(msg, "not a database") ||
		strings.Contains(msg, "disk image is malformed") ||
		strings.Contains(msg, "corrupt")
}

// db_error_is_transient reports whether err is a RETRYABLE write failure — lock
// contention or storage pressure — rather than a permanent one (schema drift,
// constraint, malformed SQL). A replicated apply that hits one of these must
// retry (ApplyDeferred), not report success and drop the op, or the write is
// lost with no retry and the replica silently diverges (#159). Parallel-queue
// delivery applies N ops to one peer concurrently, so a lock timeout is a normal
// transient event under load, not a bug.
func db_error_is_transient(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "database is locked") ||
		strings.Contains(msg, "database table is locked") ||
		strings.Contains(msg, "SQLITE_BUSY") ||
		strings.Contains(msg, "SQLITE_LOCKED") ||
		strings.Contains(msg, "disk I/O error") ||
		strings.Contains(msg, "database or disk is full") ||
		strings.Contains(msg, "disk is full")
}

// ExecResult is the outcome of a background write (exec_bg), so a replicated
// apply can decide whether to retry. ExecRetryable must defer; ExecWrote and
// ExecSkipped must NOT retry (success is done; a skipped/quarantined or
// permanently-failing write won't succeed on retry — a quarantined DB reseeds).
type ExecResult int

const (
	ExecWrote     ExecResult = iota // the write executed successfully
	ExecRetryable                   // transient failure (lock / disk) — safe to retry
	ExecSkipped                     // nil / quarantined DB, or a permanent error — retry won't help
)

// db_recover_background is a deferred backstop for a long-lived background loop:
// a corruption panic that escaped exec_bg (a shared write still on db.exec, or
// any unconverted site) is logged + swallowed so the loop and the whole process
// survive; any OTHER panic re-fires so a genuine bug still crashes. The
// integrity watchdog flags + quarantines the corrupt DB within the hour.
func db_recover_background(context string) {
	r := recover()
	if r == nil {
		return
	}
	if e, ok := r.(error); ok && db_error_is_corruption(e) {
		warn("Background goroutine %s recovered from a corrupt-DB panic: %v — skipped to keep the server up; the integrity watchdog will flag the DB.", context, r)
		return
	}
	panic(r)
}

// db_integrity_watchdog quick_checks a few due DBs each tick and warns the
// moment one is found corrupt — proactive detection so corruption surfaces as
// an alert in minutes rather than as a silent multi-hour outage (#6). The check
// is read-only (db_quick_check opens its own ro handle), and a transient
// open/lock miss (ran=false) is ignored rather than mistaken for corruption.
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
		if checked >= db_integrity_max_per_check {
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

// db_transient_dbs are the host-local, self-healing core DBs: their contents are
// re-derived after loss (queue from per-app journals, sessions by re-auth, peers
// by re-discovery), so a corrupt or missing one can be rebuilt fresh instead of
// crash-looping the server. Cold/critical DBs (users, domains, replication,
// directory, apps, settings) are deliberately NOT in this set — a corrupt one is
// left for operator restore from backup.
var db_transient_dbs = []string{"queue", "sessions", "peers"}

// db_heal_transient checks the self-healing transient core DBs at startup: it
// deletes a corrupt one (so it rebuilds fresh) and reports whether any is now
// missing. It returns true iff a transient DB's schema needs (re)creating, so
// the caller re-runs the idempotent db_create ONLY then — never on a healthy
// start. This breaks the crash-loop a corrupt/missing queue.db/sessions.db/
// peers.db would otherwise cause; a rebuilt DB's data is re-derived (queue
// from journals, sessions by re-auth, peers by re-discovery). The per-start cost
// is a quick_check of these three (normally small) DBs only.
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
	// We do NOT run db_create on every start: re-running it touches every core DB
	// and would recreate a *missing migrated* DB with only its base schema, after
	// which db_upgrade skips its migrations (version reads current) — a silently
	// incomplete schema. Instead db_heal_transient heals the self-healing
	// transient DBs, and db_create re-runs only when one of those is actually
	// missing/corrupt (rare), restoring that DB's schema (a no-op for the present
	// DBs) and fixing the missing/corrupt-queue.db crash-loop.
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
		// Future migrations: add `case N: db_upgrade_N()` here, bump
		// schema_version, and provide the matching db_upgrade_N function.
		// History before the 2026-07 baseline squash is in git.
		default:
			panic(fmt.Sprintf("No upgrade path for schema version %d", next))
		}
		setting_set("schema", fmt.Sprintf("%d", next))
		schema = next
	}
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

// db_bind associates a pooled handle with its user/app identity once. The
// pool key embeds both, so the values never change for a given handle;
// binding under databases_lock replaces the old unconditional per-open field
// writes, which raced every other goroutine holding the handle (#227). Also
// heals handles first cached by a raw db_open or the app-system sweep, which
// carry no binding.
func db_bind(db *DB, u *User, app *App, kind string) {
	databases_lock.Lock()
	if db.user == nil {
		db.user = u
		db.app = app
	}
	if kind != "" && db.kind == "" {
		db.kind = kind
	}
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

// db_stmt_cache_max bounds the per-DB prepared-statement cache. On
// overflow the whole cache is flushed (closing a statement is safe even
// if one is mid-flight — database/sql reference-counts open uses), so
// dynamically-built SQL can't grow it without bound.
const db_stmt_cache_max = 512

// prepared returns a cached prepared statement on the internal pool for
// query, or nil to fall back to the uncached path. These statements are
// pool-level and are never used inside a transaction (the *DB query
// methods all run on the pool, not on a tx), so they cannot leak a write
// out of a transaction. Schema changes are handled by the driver: ncruces
// prepares with prepare_v3, so a cached statement auto-re-prepares on
// SQLITE_SCHEMA.
func (db *DB) prepared(query string) *sqlx.Stmt {
	// Never cache while a migration runs, and never cache schema
	// introspection. A cached statement can carry a stale schema view on a
	// pooled connection; that made a pragma_table_info idempotency guard
	// report a present column as absent, re-running an ALTER and crashing a
	// migration (#10). The uncached path prepares fresh each call, which
	// reloads the connection's schema.
	if db_migrating.Load() > 0 || sql_is_introspection(query) {
		return nil
	}
	db.stmt_lock.Lock()
	defer db.stmt_lock.Unlock()
	if st, ok := db.stmt_cache[query]; ok {
		return st
	}
	if db.stmt_cache == nil {
		db.stmt_cache = make(map[string]*sqlx.Stmt)
	}
	if len(db.stmt_cache) >= db_stmt_cache_max {
		for _, st := range db.stmt_cache {
			st.Close()
		}
		db.stmt_cache = make(map[string]*sqlx.Stmt)
	}
	st, err := db.internal.Preparex(query)
	if err != nil {
		return nil // fall back to the uncached path
	}
	db.stmt_cache[query] = st
	return st
}

// stmts_close closes every cached prepared statement. Called from the
// db_manager eviction path before the pool is closed.
func (db *DB) stmts_close() {
	db.stmt_lock.Lock()
	defer db.stmt_lock.Unlock()
	for _, st := range db.stmt_cache {
		st.Close()
	}
	db.stmt_cache = nil
}

// stmt_closed reports whether err is database/sql's "statement is closed"
// sentinel. A cached prepared statement can be closed by a concurrent
// stmts_close (DDL flush, 512-entry overflow, or eviction) in the window
// between prepared() handing it out and the caller executing it outside
// stmt_lock — under heavy concurrent load on one DB this surfaced as
// intermittent "sql: statement is closed" (e.g. group_memberships on the hot
// access-check path). The query helpers treat it as a transient cache miss and
// retry once on the uncached pool path, which re-prepares fresh. The sentinel
// is unexported in database/sql, so match on its stable message.
func stmt_closed(err error) bool {
	return err != nil && strings.Contains(err.Error(), "statement is closed")
}

// db_migrating is >0 while a schema migration runs (db_create/db_upgrade).
// prepared() returns nil during that window so every migration statement
// is prepared fresh, never carrying a stale schema view across the
// migration's DDL (#10). A counter so nested migrations compose.
var db_migrating atomic.Int32

// sql_is_introspection reports whether a query reads schema metadata
// (pragma_*, sqlite_master, sqlite_schema). These must not be cached: a
// cached introspection statement can report a stale schema on a pooled
// connection, which breaks migration idempotency guards (see #10 and
// prepared()).
func sql_is_introspection(query string) bool {
	q := strings.ToLower(query)
	return strings.Contains(q, "pragma_") ||
		strings.Contains(q, "sqlite_master") ||
		strings.Contains(q, "sqlite_schema")
}

// sql_is_schema reports whether a statement changes the database schema
// (DDL). Such a statement invalidates the prepared-statement cache:
// statements compiled against the old schema return stale or empty
// results afterwards (verified — ncruces' prepare_v3 auto-re-prepare does
// not save us through the database/sql + sqlx path), so the cache is
// flushed when one runs.
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
		if _, err := st.Exec(values...); !stmt_closed(err) {
			return err
		}
		// cached statement closed by a concurrent cache flush; retry uncached
	}
	_, err := db.internal.Exec(query, values...)
	return err
}

// exec_bg is the background-safe write: it NEVER panics, so a corrupt user DB
// can't take down the whole process. A DB already quarantined (flagged
// corrupt) is skipped without touching it. A corruption error quarantines the
// DB — skipping all further ops on it — and alerts the admin once; any other
// error is logged. The caller keeps serving every other user. `context` names
// the operation for the alert/log.
func (db *DB) exec_bg(context, query string, values ...any) ExecResult {
	if db == nil || db_quarantined(db.path) {
		return ExecSkipped
	}
	if err := db.exec_e(query, values...); err != nil {
		if db_error_is_corruption(err) {
			db_quarantine(db.path, context, err)
			return ExecSkipped
		}
		if db_error_is_transient(err) {
			// Retryable: a replicated apply defers on this so the op stays in
			// the pending buffer and re-applies next drain tick (#159).
			warn("Background DB write failed (%s, retryable) on %q: %v", context, db.path, err)
			return ExecRetryable
		}
		warn("Background DB write failed (%s) on %q: %v", context, db.path, err)
		return ExecSkipped
	}
	return ExecWrote
}


func (db *DB) exists(query string, values ...any) (bool, error) {
	var r *sql.Rows
	var err error
	if st := db.prepared(query); st != nil {
		r, err = st.Query(values...)
		if stmt_closed(err) {
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
		if stmt_closed(err) {
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

// integer64 reads a single integer column as int64, so a value beyond the 32-bit
// range (a timestamp, a large sequence, a generation epoch) is not truncated on
// the 32-bit builds (armhf/armv7hl) where integer()'s int return would be. Use
// this for any column whose value can exceed ~2.1e9. Returns 0 on no-row/error,
// matching integer().
func (db *DB) integer64(query string, values ...any) int64 {
	var result int64
	var err error
	if st := db.prepared(query); st != nil {
		err = st.QueryRow(values...).Scan(&result)
		if stmt_closed(err) {
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
		if stmt_closed(err) {
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
		if stmt_closed(err) {
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
		if stmt_closed(err) {
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
		if err := st.Select(out, values...); !stmt_closed(err) {
			return err
		}
		// cached statement closed by a concurrent cache flush; retry uncached
	}
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


// starlark_db runs one of the app's database lifecycle functions
// (database_create / database_upgrade / database_downgrade) and stamps
// user_version, both inside a single transaction on a dedicated connection.
// Either the function's DDL and the stamp commit together, or (error, panic,
// process death) nothing persists — a crash can no longer leave a partial
// schema that db_app's has_tables check mistakes for a complete one (#227).
// The connection is handed to the Starlark thread as the "lifecycle" local:
// mochi.db.* calls inside the function run on it directly, which both joins
// them to the transaction and keeps them from re-entering db_app, whose
// lock(path) this goroutine already holds (re-entry would self-deadlock).
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

	ctx := context.Background()
	// Inside a database lifecycle function, statements run on the dedicated
	// lifecycle connection so they join its transaction (and don't re-enter
	// db_app, whose lock this goroutine holds — #227). Its rollback is owned
	// by starlark_db, not the per-call defensive rollback below.
	conn := db_lifecycle_conn(t)
	lifecycle := conn != nil
	if !lifecycle {
		db, err := db_for_thread(t)
		if err != nil {
			return sl_error(fn, "%v", err)
		}

		// Check out a dedicated connection so a failed multi-statement
		// query (e.g. `BEGIN; bad-sql; COMMIT;` where bad-sql is denied by
		// the authoriser at prepare) can't return a half-open transaction
		// to the shared pool. On error we issue a defensive ROLLBACK on
		// the same connection before releasing it.
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
	res, err := h.tx.Exec(query, params...)
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

	tx, err := db.starlark.Beginx()
	if err != nil {
		return sl_error(fn, "begin failed: %v", err)
	}

	h := &TransactionHandle{tx: tx}
	t.SetLocal("transactions", append(existing, h))
	return h, nil
}

// db_conn_rows runs a read query on a specific connection and returns rows in
// the same map shape as DB.rows. Used by the introspection builtins when a
// database lifecycle transaction is active: they must run on the lifecycle
// connection to see its uncommitted DDL (feeds and wikis migrations use
// mochi.db.table as column-existence idempotency guards). The direct PRAGMA
// forms are used — the lifecycle authoriser allows table_info/index_list —
// because the pragma_* virtual-table forms silently return zero rows on an
// authorised connection (their internal prepare re-fires AUTH_PRAGMA).
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

// row_int extracts a numeric field from a map[string]any returned by
// a CBOR-decoded payload or a sqlite row scan. The cbor library
// decodes non-negative integers into uint64 (not int64) when the
// target is interface{}, so callers like the bootstrap chunk-fetch
// handler would see length=0 for every non-empty file because the
// uint64(903) case wasn't matched and fell through to the zero
// return. That broke file-chunk delivery — every file landed as a
// zero-byte file on the receiver, including 21,612 entity-id app
// files whose empty app.json then made the published-apps loader
// silently skip the entire installed-app set.
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

// sql_target_table extracts the target table from a mutating SQL
// statement. Returns "" for read-only statements (SELECT, PRAGMA …)
// and for schema statements (CREATE/DROP/ALTER) — neither replicates.
// The parser is intentionally simple: skip leading comments + whitespace,
// match the verb, then take the next identifier as the table name. CTE
// (WITH …) prefixes are not recognised and stay local; apps that need
// CTE writes to replicate should reshape to a plain INSERT/UPDATE/DELETE.
// sql_is_mutating reports whether sql is a row-changing statement
// (INSERT / REPLACE / UPDATE / DELETE, including the INSERT OR ... forms).
// Used to keep mutations out of the read-only mochi.db.row/rows/exists APIs,
// which run the write but do NOT journal it — so the change would never
// replicate (silent divergence). Such writes must go through mochi.db.execute.
// CTE-prefixed mutations (WITH ... DELETE) are not detected — no app uses them
// and the CI grep gate (#8) covers the literal case.
func sql_is_mutating(sql string) bool {
	verb, _ := sql_take_word(sql_strip_lead(sql))
	switch strings.ToUpper(verb) {
	case "INSERT", "REPLACE", "UPDATE", "DELETE":
		return true
	}
	return false
}


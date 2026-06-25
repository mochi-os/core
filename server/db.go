// Mochi server: Database
// Copyright © 2026 Mochi OÜ
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
	// closed is the unix timestamp when this handle was last marked
	// idle, or 0 while in use. Always read and written under
	// databases_lock - same primitive that guards the cache map this
	// DB lives in, so no new synchronisation primitive is introduced.
	closed int64

	// stmt_cache holds prepared statements for the internal pool, keyed
	// by SQL text, populated lazily by prepared() when the development
	// cache_prepare flag is set. Guarded by stmt_lock. Closed on
	// eviction (stmts_close). Nil/empty when the flag is off.
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
	schema_version = 89
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

func db_create() {
	db_migrating.Add(1)
	defer db_migrating.Add(-1)
	info("Creating new database")

	// Settings
	settings := db_open("db/settings.db")
	settings.exec("create table if not exists settings ( name text not null primary key, value text not null )")
	settings.exec("replace into settings ( name, value ) values ( 'schema', ? )", schema_version)

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

	// Replication: per-origin-peer dedup, schema-coordination buffer,
	// per-user opt-in set, outbound sequence counters, server-pair members,
	// lease-based leadership with fencing, bulk-bootstrap progress, paired
	// server compatibility tracking. See claude/plans/replication.md.
	replication := db_open("db/replication.db")
	replication.exec("create table if not exists seen (peer text not null, scope text not null, user text not null default '', sequence integer not null, applied integer not null, primary key (peer, scope, user, sequence))")
	replication.exec("create index if not exists seen_applied on seen(applied)")
	replication.exec("create table if not exists pending (peer text not null, scope text not null, user text not null default '', db text not null default '', sequence integer not null, prev integer not null default 0, schema integer not null default 0, payload blob not null, received integer not null, primary key (peer, scope, user, sequence))")
	replication.exec("create index if not exists pending_received on pending(received)")
	replication.exec("create index if not exists pending_chain on pending(peer, scope, user, db, prev)")
	// relayed: cross-hop dedup for the transit relay - one row per origin
	// op this host has applied-and-relayed, so a copy arriving by another
	// path (or bouncing back) is dropped instead of re-relayed forever.
	replication.exec("create table if not exists relayed (user text not null, origin text not null, seen integer not null, primary key (user, origin))")
	replication.exec("create index if not exists relayed_seen on relayed(seen)")
	// hosts: a user's per-user replica set, one row per hosting peer. Each
	// row is that peer's self-assertion that it hosts the user, carrying its
	// libp2p-key attestation and a `seen` refresh timestamp. A peer can only
	// add or remove its own membership; rows age out when un-refreshed.
	replication.exec("create table if not exists hosts (user text not null, peer text not null, added integer not null, ack integer not null default 0, seen integer not null default 0, attestation text not null default '', primary key (user, peer))")
	replication.exec("create table if not exists sequence (user text not null default '', scope text not null, next integer not null default 0, primary key (user, scope))")
	// cursor: the contiguous in-order apply watermark per inbound
	// (peer, scope, user, db) stream. Each op chains onto its db
	// stream via op.Prev so a same-row op chain can't be reordered
	// by a backlog drain. See claude/plans/replication-test.md
	// Stage 19.
	replication.exec("create table if not exists cursor (peer text not null, scope text not null, user text not null default '', db text not null default '', sequence integer not null default 0, primary key (peer, scope, user, db))")
	// (#65) Replication generation (epoch) tables. epoch: this host's outbound
	// generation — a 1-row counter bumped to now() when the host resets its
	// outbound sequence space (a replica reset / rejoin, detected at the
	// receiver's bootstrap completion). Stamped on every emitted op. peer_epoch:
	// the highest generation seen from each peer; a higher epoch than recorded
	// triggers a one-time inbound reset of that peer's stream state, so a restart
	// is self-announced to ANY host, not just the one that served the bootstrap
	// (#34 for N>=3). See claude/plans/replication-epoch.md.
	replication.exec("create table if not exists epoch (singleton integer primary key check (singleton = 1), value integer not null default 0)")
	// peer_epoch.pending=1 marks a peer we just bootstrapped FROM: its cursors are
	// freshly seeded at its current generation, so the first op from it adopts
	// that generation as our baseline WITHOUT an inbound reset (which would clear
	// the good seed and stall a mid-stream peer). Cleared on adoption.
	replication.exec("create table if not exists peer_epoch (peer text primary key, epoch integer not null default 0, pending integer not null default 0)")
	// tail: sender-side last-emitted sequence per (user, scope, db),
	// stamped onto each outbound op as Prev — the per-db ordering chain.
	replication.exec("create table if not exists tail (user text not null default '', scope text not null, db text not null default '', last integer not null default 0, primary key (user, scope, db))")
	replication.exec("create table if not exists pair (peer text primary key, added integer not null, role text not null default '')")
	replication.exec("create table if not exists leadership (scope text not null, key text not null, peer text not null, expires integer not null, fence integer not null default 0, primary key (scope, key))")
	replication.exec("create index if not exists leadership_expires on leadership(expires)")
	replication.exec("create table if not exists fence_witness (scope text not null, key text not null, fence integer not null default 0, peer text not null default '', seen integer not null default 0, primary key (scope, key))")
	replication.exec("create table if not exists bootstrap (scope text not null, peer text not null, position text not null default '', state text not null default 'queued', failed integer not null default 0, progress integer not null default 0, attempts integer not null default 0, primary key (scope, peer))")
	// bootstrap_served: source-side tracking of scopes we're currently
	// serving to each joined peer. Inserted on join approval (one row
	// per scope), deleted when the receiver acks `bootstrap/scope/done`.
	// Symmetry with the receiver's `bootstrap` table — the receiver
	// sees "syncing" while it pulls; the source sees "syncing" while
	// these rows exist.
	replication.exec("create table if not exists bootstrap_served (peer text not null, scope text not null, started integer not null, primary key (peer, scope))")
	replication.exec("create table if not exists schemas (peer text primary key, core integer not null default 0, apps text not null default '')")
	// Per-user link-requests awaiting Approve / Deny in Settings → Replication.
	// One row per (target user on this host, source peer); newest wins via
	// INSERT OR REPLACE. Expiry is 1h from receipt; periodic sweep emits
	// link-denied(reason="expired") to the source side. See "Per-user trigger"
	// in claude/plans/replication.md.
	replication.exec("create table if not exists links (user text not null, peer text not null, label text not null default '', placeholder text not null, received integer not null, expires integer not null, primary key (user, peer))")
	replication.exec("create index if not exists links_expires on links(expires)")
	// Whole-server pair join-requests awaiting Approve / Deny on the Pair
	// page. One row per source peer; newest wins via INSERT OR REPLACE.
	// Expiry is 10 minutes from receipt; periodic sweep emits
	// join-denied(reason="expired") to the replica. See "Operator UI" in
	// claude/plans/replication.md.
	replication.exec("create table if not exists joins (peer text not null primary key, label text not null default '', received integer not null, expires integer not null)")
	replication.exec("create index if not exists joins_expires on joins(expires)")
	// irreparable: a stream broken (stalled on an unfillable gap, or a
	// member offline) past T_forget, when no lossless recovery remains.
	// One row per (peer, scope, user, db); `notified` flips to 1 once the
	// dual-side notification has fired so the manager neither re-notifies
	// nor keeps warning. Cleared when the stream recovers or the operator
	// removes the relationship. See replication_irreparable.go.
	replication.exec("create table if not exists irreparable (peer text not null, scope text not null, user text not null default '', db text not null default '', reason text not null, since integer not null, notified integer not null default 0, primary key (peer, scope, user, db))")
	// unreachable: persisted "this peer's Sender has been failing to deliver
	// since `since`" — set when a peer crosses the stall threshold, cleared on
	// the next ack. notified guards the 24h offline notification. Survives
	// restarts so a member offline past T_forget is recognised even across
	// server bounces. See peer_progress.go + replication_irreparable.go.
	replication.exec("create table if not exists unreachable (peer text not null primary key, since integer not null, notified integer not null default 0)")
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
var db_integrity_state sync.Map

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
		case 68:
			db_upgrade_68()
		case 69:
			db_upgrade_69()
		case 70:
			db_upgrade_70()
		case 71:
			db_upgrade_71()
		case 72:
			db_upgrade_72()
		case 73:
			db_upgrade_73()
		case 74:
			db_upgrade_74()
		case 75:
			db_upgrade_75()
		case 76:
			db_upgrade_76()
		case 77:
			db_upgrade_77()
		case 78:
			db_upgrade_78()
		case 79:
			db_upgrade_79()
		case 80:
			db_upgrade_80()
		case 81:
			db_upgrade_81()
		case 82:
			db_upgrade_82()
		case 83:
			db_upgrade_83()
		case 84:
			db_upgrade_84()
		case 85:
			db_upgrade_85()
		case 86:
			db_upgrade_86()
		case 87:
			db_upgrade_87()
		case 88:
			db_upgrade_88()
		case 89:
			db_upgrade_89()
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

// db_upgrade_68 fixes the queue_select bottleneck observed on wasabi
// 2026-05-26 with a 1.77M-row queue. The existing (status, next_retry)
// index satisfied the WHERE clause but not the `priority desc,
// next_retry` ORDER BY, so each queue_select needed to sort 1.7M rows
// before picking the top 50 - 1.3 seconds per call, capping drain at
// 50/sec via queue_manager. The bulk-floor select had the same problem
// for `priority <= 10` range filtering. A single (status, priority,
// next_retry) index covers both queries; SQLite reverse-scans for the
// DESC order in the main query. After this migration the old index is
// redundant and gets dropped. ANALYZE refreshes stats so the planner
// picks the new index immediately rather than waiting for the next
// background analyze.
func db_upgrade_68() {
	q := db_open("db/queue.db")
	q.exec("create index if not exists queue_status_priority_retry on queue (status, priority, next_retry)")
	q.exec("drop index if exists queue_status_retry")
	q.exec("analyze queue")
}

// db_upgrade_69 adds queue_target_priority_retry so the
// /mochi/2/messages Sender's pull_loop can pick its peer's rows in
// (priority desc, next_retry asc) order without sorting. The existing
// queue_target index (target alone) finds rows quickly but the ORDER BY
// would force a temp-table sort over every row for the target — at
// wasabi's per-peer scale that's hundreds of thousands of rows per
// pull. The new composite lets SQLite walk the index in the requested
// order and stop at LIMIT. analyze refreshes stats so the planner picks
// the new index immediately.
func db_upgrade_69() {
	q := db_open("db/queue.db")
	q.exec("create index if not exists queue_target_priority_retry on queue (target, priority desc, next_retry)")
	q.exec("analyze queue")
}

// db_upgrade_70 adds the restore_source column to users (set when an
// account arrives via a server-move restore; drives the source-cleanup
// banner) and the relinks table (third-party services the user must
// re-link on the destination after a move).
func db_upgrade_70() {
	users := db_open("db/users.db")
	if exists, _ := users.exists("select 1 from pragma_table_info('users') where name='restore_source'"); !exists {
		users.exec("alter table users add column restore_source text not null default ''")
	}
	if exists, _ := users.exists("select 1 from sqlite_master where type='table' and name='relinks'"); !exists {
		users.exec("create table relinks (user text not null references users(uid) on delete cascade, service text not null, identifier text not null default '', linked integer not null default 0, primary key (user, service))")
	}
}

// db_upgrade_71 adds the reauthentication table to sessions.db: short-lived
// single-use step-up proofs earned by re-verifying the user's login
// factor(s) before a sensitive action (export, replication approval, and
// the account-security cluster).
func db_upgrade_71() {
	sessions := db_open("db/sessions.db")
	if exists, _ := sessions.exists("select 1 from sqlite_master where type='table' and name='reauthentication'"); !exists {
		sessions.exec("create table reauthentication (id text primary key, user text not null, methods text not null default '', expires integer not null)")
		sessions.exec("create index reauthentication_expires on reauthentication(expires)")
	}
}

// db_upgrade_72 adds the version column to directory.db entities. It
// carries the announcing host's last-edit time for the entity, used by
// directory_publish_event for version-based last-write-wins: a
// reordered or replayed older announcement no longer clobbers a newer
// description. Existing rows default to 0 (treated as "no version", so
// the first versioned announcement wins). Idempotent.
func db_upgrade_72() {
	directory := db_open("db/directory.db")
	if col, _ := directory.exists("select 1 from pragma_table_info('entities') where name='version'"); !col {
		directory.exec("alter table entities add column version integer not null default 0")
	}
}

// db_upgrade_73 adds the disabled column to users: a CSV of login methods
// the user has explicitly turned off, the complement of the required
// methods column. A method that is neither required nor disabled is
// "allowed" (a usable but optional sign-in factor). Existing rows default
// to ” (nothing explicitly disabled), so behaviour is unchanged on
// upgrade. Idempotent.
func db_upgrade_73() {
	users := db_open("db/users.db")
	if col, _ := users.exists("select 1 from pragma_table_info('users') where name='disabled'"); !col {
		users.exec("alter table users add column disabled text not null default ''")
	}
}

// db_upgrade_74 relaxes the historical email-required default to the tri-state
// redesign's "any registered factor signs you in" default. Until schema 73 a
// new account got methods='email' (email Required); the redesign makes the
// default "no required factor", and the OAuth/email decoupling means an account
// still on the old default that relies on a third-party provider can no longer
// sign in with it alone (and could be locked out if the provider email is
// unreachable). Relaxing every account still on the untouched default ('email'
// exactly) to ” aligns existing users with new signups and un-breaks OAuth
// login. Accounts that chose a stricter set (e.g. 'email,totp') or a non-email
// factor keep it; pure-email accounts are unaffected in practice, since the
// email code stays their only way in. Idempotent.
func db_upgrade_74() {
	users := db_open("db/users.db")
	users.exec("update users set methods='' where methods='email'")
}

// db_upgrade_75 adds the restore_passkeys flag to users: set on restore when
// the source account had registered passkeys, so the post-restore banner can
// prompt the user to re-register them (passkeys are bound to the source
// origin and don't travel in a backup). Idempotent.
func db_upgrade_75() {
	users := db_open("db/users.db")
	if col, _ := users.exists("select 1 from pragma_table_info('users') where name='restore_passkeys'"); !col {
		users.exec("alter table users add column restore_passkeys integer not null default 0")
	}
}

// db_upgrade_76 adds the irreparable marker table to replication.db: a
// stream broken past T_forget is recorded here so the manager can notify
// both sides once and surface the terminal state. Idempotent. See
// replication_irreparable.go.
func db_upgrade_76() {
	replication := db_open("db/replication.db")
	if exists, _ := replication.exists("select 1 from sqlite_master where type='table' and name='irreparable'"); !exists {
		replication.exec("create table irreparable (peer text not null, scope text not null, user text not null default '', db text not null default '', reason text not null, since integer not null, notified integer not null default 0, primary key (peer, scope, user, db))")
	}
}

// db_upgrade_77 adds the peer_unreachable table to replication.db: a
// persisted "Sender failing since" timestamp per peer, so a member offline
// past T_forget is recognised even across restarts. Idempotent. See
// peer_progress.go.
func db_upgrade_77() {
	replication := db_open("db/replication.db")
	if exists, _ := replication.exists("select 1 from sqlite_master where type='table' and name='peer_unreachable'"); !exists {
		replication.exec("create table peer_unreachable (peer text not null primary key, since integer not null)")
	}
}

// db_upgrade_78 renames peer_unreachable to unreachable (single word,
// consistent with the irreparable table) and adds the notified flag, which
// guards the 24h offline notification (fires once per offline episode; reset
// when the row is dropped on the peer's next ack). Idempotent. See
// replication_irreparable.go.
func db_upgrade_78() {
	replication := db_open("db/replication.db")
	old, _ := replication.exists("select 1 from sqlite_master where type='table' and name='peer_unreachable'")
	renamed, _ := replication.exists("select 1 from sqlite_master where type='table' and name='unreachable'")
	if old && !renamed {
		replication.exec("alter table peer_unreachable rename to unreachable")
	}
	if exists, _ := replication.exists("select 1 from sqlite_master where type='table' and name='unreachable'"); exists {
		if col, _ := replication.exists("select 1 from pragma_table_info('unreachable') where name='notified'"); !col {
			replication.exec("alter table unreachable add column notified integer not null default 0")
		}
	}
}

// db_upgrade_79 adds the purge column to users.db.users: the unix timestamp
// at which a self-closed account is hard-deleted. 0 means the account is not
// closing. Set alongside status='closing' by user_close; cleared on cancel;
// acted on by closure_manager once it passes. Idempotent.
func db_upgrade_79() {
	users := db_open("db/users.db")
	if col, _ := users.exists("select 1 from pragma_table_info('users') where name='purge'"); !col {
		users.exec("alter table users add column purge integer not null default 0")
	}
}

// db_upgrade_80 replaces the two-table directory (entities + locations) with
// the single self-verifying `entries` table. Legacy rows carry no signatures
// and could never be re-served under the self-verifying model, and
// directory.db is rebuildable network state — so drop and rebuild rather
// than migrate. Local public entities are marked unpublished so the
// directory manager's startup republish re-announces them immediately;
// third-party rows repopulate via sync. Idempotent.
// db_upgrade_81 adds the self-assertion columns to replication.db hosts:
// `seen` (each host's membership refresh timestamp) and `attestation` (its
// libp2p-key signature over the claim). Existing rows backfill seen=added
// and an empty attestation; upgraded peers re-assert within the hour, and a
// peer that never re-asserts ages out via the membership TTL. Idempotent.
func db_upgrade_81() {
	r := db_open("db/replication.db")
	if col, _ := r.exists("select 1 from pragma_table_info('hosts') where name='seen'"); !col {
		r.exec("alter table hosts add column seen integer not null default 0")
		r.exec("update hosts set seen=added where seen=0")
	}
	if col, _ := r.exists("select 1 from pragma_table_info('hosts') where name='attestation'"); !col {
		r.exec("alter table hosts add column attestation text not null default ''")
	}
}

// db_upgrade_82 adds the peers.db names table: hostname/domain claims
// from peers/publish with their DNS verification verdict. Idempotent.
func db_upgrade_82() {
	p := db_open("db/peers.db")
	p.exec("create table if not exists names ( id text not null, name text not null, verified integer not null default 0, checked integer not null default 0, updated integer not null, primary key ( id, name ) )")
}

// db_upgrade_83 adds usefulness evidence to peers.db addresses: when a
// connection last succeeded on each (`success`) and how many dial
// rounds have failed since (`failure`). Protects proven addresses from
// cap eviction and lets never-proven ones prune early. Idempotent.
func db_upgrade_83() {
	p := db_open("db/peers.db")
	if col, _ := p.exists("select 1 from pragma_table_info('peers') where name='success'"); !col {
		p.exec("alter table peers add column success integer not null default 0")
	}
	if col, _ := p.exists("select 1 from pragma_table_info('peers') where name='failure'"); !col {
		p.exec("alter table peers add column failure integer not null default 0")
	}
}

// db_upgrade_84 adds the peers.db records table: the latest signed peer
// record per peer, for replay rejection and relay. Idempotent.
func db_upgrade_84() {
	p := db_open("db/peers.db")
	p.exec("create table if not exists records ( id text not null primary key, record blob not null, sequence integer not null, updated integer not null )")
}

// db_upgrade_86 drops peer-name DNS verification: the names table loses its
// verified/checked columns and keeps only the announced name. peers.db is
// host-local cache repopulated from announcements, so the table is dropped
// and recreated rather than altered in place.
func db_upgrade_86() {
	p := db_open("db/peers.db")
	p.exec("drop table if exists names")
	p.exec("create table names ( id text not null, name text not null, updated integer not null, primary key ( id, name ) )")
}

// db_upgrade_87 adds the `progress` and `attempts` columns to
// replication.db.bootstrap. Together they let the retry driver re-fire
// EVERY non-done scope (queued, stalled-active, incomplete) — not just
// 'incomplete' — without disturbing a transfer that is still moving:
//
//   - progress — unix timestamp of the last forward progress (manifest
//     landed, a chunk landed). A scope sitting in 'active' with a fresh
//     progress is a live transfer and is left alone; one whose progress
//     has gone stale is treated as stalled and re-driven.
//   - attempts — consecutive retry attempts since the last forward
//     progress, used to back off (so an unreachable source isn't probed
//     every tick forever). Reset to 0 on any real progress.
//
// Existing rows default both to 0, which makes them immediately eligible
// for the first retry pass — correct, since a row already non-done at
// upgrade time has nothing in flight. Idempotent.
func db_upgrade_87() {
	r := db_open("db/replication.db")
	if has, _ := r.exists("select 1 from pragma_table_info('bootstrap') where name='progress'"); !has {
		r.exec("alter table bootstrap add column progress integer not null default 0")
	}
	if has, _ := r.exists("select 1 from pragma_table_info('bootstrap') where name='attempts'"); !has {
		r.exec("alter table bootstrap add column attempts integer not null default 0")
	}
}

// db_upgrade_88 adds the relayed table: cross-hop dedup for the transit
// relay, so an op that reaches a host by more than one path (or bounces
// back) is applied-and-relayed at most once. See claude/plans/replication.md.
func db_upgrade_88() {
	r := db_open("db/replication.db")
	r.exec("create table if not exists relayed (user text not null, origin text not null, seen integer not null, primary key (user, origin))")
	r.exec("create index if not exists relayed_seen on relayed(seen)")
}

// db_upgrade_89 adds the replication generation (epoch) tables (#65): the host's
// own outbound generation and the highest generation seen per peer. A host bumps
// its epoch when it resets its outbound sequence space; a peer seeing the higher
// epoch performs a one-time inbound reset, so a restart is self-announced to any
// host (a generalised #34 for N>=3). See claude/plans/replication-epoch.md.
func db_upgrade_89() {
	r := db_open("db/replication.db")
	r.exec("create table if not exists epoch (singleton integer primary key check (singleton = 1), value integer not null default 0)")
	r.exec("create table if not exists peer_epoch (peer text primary key, epoch integer not null default 0, pending integer not null default 0)")
}

// db_upgrade_85 re-keys replication.db cursor/tail/pending stream
// identifiers to the class-qualified scheme (app:/core:/system:) so an app
// named after a reserved core stream (e.g. a dev app "notifications") no
// longer shares one stream + cursor with the core DB of the same name. The
// bare keys map deterministically via repl_stream_migrate_key. Rows already
// containing ':' are skipped, so the migration is idempotent. A conflated
// bare "notifications"/"user" row resolves to the core stream; any colliding
// app-data stream re-anchors on its next Prev==0 op.
func db_upgrade_85() {
	r := db_open("db/replication.db")
	for _, table := range []string{"cursor", "tail", "pending"} {
		if has, _ := r.exists("select 1 from sqlite_master where type='table' and name=?", table); !has {
			continue
		}
		rows, _ := r.rows("select distinct db from " + table + " where db != '' and instr(db, ':') = 0")
		for _, row := range rows {
			old, _ := row["db"].(string)
			if old == "" {
				continue
			}
			r.exec("update "+table+" set db=? where db=?", repl_stream_migrate_key(old), old)
		}
	}
}

func db_upgrade_80() {
	d := db_open("db/directory.db")
	d.exec("drop table if exists entities")
	d.exec("drop table if exists locations")
	d.exec("create table if not exists entries ( entity text not null, peer text not null, name text not null, class text not null, data text not null default '', fingerprint text not null default '', version integer not null default 0, created integer not null, seen integer not null, signature text not null default '', attestation text not null default '', primary key ( entity, peer ) )")
	d.exec("create index if not exists entries_name on entries( name )")
	d.exec("create index if not exists entries_class on entries( class )")
	d.exec("create index if not exists entries_fingerprint on entries( fingerprint )")
	d.exec("create index if not exists entries_peer on entries( peer )")
	d.exec("create index if not exists entries_seen on entries( seen )")
	d.exec("create index if not exists entries_created on entries( created )")
	db_open("db/users.db").exec("update entities set published=0 where privacy='public'")
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
		replication.exec("create table bootstrap (scope text not null, peer text not null, position text not null default '', state text not null default 'queued', failed integer not null default 0, progress integer not null default 0, attempts integer not null default 0, primary key (scope, peer))")
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

// db_stmt_cache_max bounds the per-DB prepared-statement cache. On
// overflow the whole cache is flushed (closing a statement is safe even
// if one is mid-flight — database/sql reference-counts open uses), so
// dynamically-built SQL can't grow it without bound.
const db_stmt_cache_max = 512

// prepared returns a cached prepared statement on the internal pool for
// query, or nil to fall back to the uncached path. Active only under the
// development cache_prepare flag, so the default path is byte-for-byte
// unchanged. These statements are pool-level and are never used inside a
// transaction (the *DB query methods all run on the pool, not on a tx),
// so they cannot leak a write out of a transaction. Schema changes are
// handled by the driver: ncruces prepares with prepare_v3, so a cached
// statement auto-re-prepares on SQLITE_SCHEMA.
func (db *DB) prepared(query string) *sqlx.Stmt {
	if !cache_prepare {
		return nil
	}
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

func (db *DB) exec(query string, values ...any) {
	// DDL changes the schema, which invalidates cached statements; run it
	// uncached and flush. (Migrations run DDL through db.exec.)
	if cache_prepare && sql_is_schema(query) {
		must(db.internal.Exec(query, values...))
		db.stmts_close()
		return
	}
	if st := db.prepared(query); st != nil {
		must(st.Exec(values...))
		return
	}
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
	// App-system writes (access, attachments in app.db) journal the op in the
	// same transaction as the data, so it can't be lost relative to the data
	// (Gap A/B) — same guarantee as mochi.db.execute. user-core stays on the
	// legacy emit (core-scope signing isn't wired; #34).
	if db.kind == db_kind_app_system && db.user != nil && db.user.UID != "" && db.app != nil &&
		journal_table_replicates(sql_target_table(query)) {
		journal_ensure(db)
		if tx, err := db.internal.Beginx(); err == nil {
			if _, err := tx.Exec(query, values...); err != nil {
				tx.Rollback()
				must[any](nil, err)
				return
			}
			if err := journal_record_tx(tx, repl_op_exec_app_system, 0, query, values); err != nil {
				tx.Rollback()
				must[any](nil, err)
				return
			}
			if err := tx.Commit(); err != nil {
				must[any](nil, err)
				return
			}
			journal_wake(db)
			return
		}
		// tx open failed: fall through to the non-atomic write + legacy emit.
	}

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
	if db.user == nil || db.user.UID == "" || db.app == nil {
		must(db.internal.Exec(query, values...))
		return
	}
	av := db.app.active(db.user)
	if av == nil {
		must(db.internal.Exec(query, values...))
		return
	}
	if !journal_replicates(false, av, query) {
		must(db.internal.Exec(query, values...))
		return
	}

	// Record the op in the data DB's journal in the same transaction as the
	// write, then wake the drainer — same atomicity guarantee as
	// mochi.db.execute (claude/plans/replication-journal.md).
	journal_ensure(db)
	tx, err := db.internal.Beginx()
	if err != nil {
		// Can't open a tx: fall back to the non-atomic write so the broadcast
		// helper still makes progress (panics on a real SQL error, as before).
		must(db.internal.Exec(query, values...))
		return
	}
	if _, err := tx.Exec(query, values...); err != nil {
		tx.Rollback()
		must[any](nil, err) // preserve the panic-on-SQL-error contract
		return
	}
	if err := journal_record_tx(tx, repl_op_exec, av.Database.Schema, query, values); err != nil {
		tx.Rollback()
		must[any](nil, err)
		return
	}
	if err := tx.Commit(); err != nil {
		must[any](nil, err)
		return
	}
	journal_wake(db)
}

func (db *DB) exists(query string, values ...any) (bool, error) {
	var r *sql.Rows
	var err error
	if st := db.prepared(query); st != nil {
		r, err = st.Query(values...)
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
		return st.Select(out, values...)
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

// db_replicate_after_exec emits a replication op for a successful local
// app-DB write. Called from api_db_query (mochi.db.execute) and from
// TransactionHandle's deferred-emit flush at commit. The decision on
// whether to actually emit (table not excluded, user has UID, app
// resolvable) lives in replication_emit_sql_command.
func db_replicate_after_exec(t *sl.Thread, sql string, args []any) {
	// Migrations (database_create/upgrade/downgrade) run with this flag set so
	// their writes don't replicate — every replica migrates itself. See
	// (*AppVersion).starlark_db.
	if suppressed, _ := t.Local("replication_suppressed").(bool); suppressed {
		return
	}
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

	// The read APIs must not smuggle a write: a mutation run through
	// row/rows/exists executes but is never journaled, so it would never
	// replicate (silent divergence). Reject it — the write must go through
	// mochi.db.execute (or mochi.db.transaction), which journals the op.
	switch fn.Name() {
	case "mochi.db.exists", "mochi.db.row", "mochi.db.rows":
		if sql_is_mutating(query) {
			return sl_error(fn, "%s cannot run a mutating statement (INSERT/UPDATE/DELETE/REPLACE); use mochi.db.execute so the write replicates", fn.Name())
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
		// Replicated app-scope write: record the op in the data DB's journal
		// in the SAME transaction as the mutation (db_execute_journal), then
		// wake the drainer. The old direct emit (db_replicate_after_exec) is
		// retired for this path — replication is driven by the journal so a
		// crash can't leave the data committed with the op unrecorded, and a
		// write made with no live peer is still journaled. See
		// claude/plans/replication-journal.md.
		u, _ := db_user_for_thread(t)
		app, _ := t.Local("app").(*App)
		var av *AppVersion
		if u != nil && app != nil {
			av = app.active(u)
		}
		suppressed, _ := t.Local("replication_suppressed").(bool)
		affected, recorded, err := db_execute_journal(ctx, conn, db, av, suppressed, query, as)
		if err != nil {
			db_starlark_rollback(conn)
			return sl_error(fn, "database error: %v", err)
		}
		if recorded {
			journal_wake(db)
		}
		// Return the number of rows the statement changed (insert/update/
		// delete count), so apps can branch on whether a conditional write
		// took effect. Previously returned None.
		return sl.MakeInt64(affected), nil

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
	suppressed    bool // set when opened inside a migration: commit emits nothing
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
	res, err := h.tx.Exec(query, params...)
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}
	h.pending_emits = append(h.pending_emits, sql_pending_emit{sql: query, args: params})
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
	// Record journal rows for the buffered replicated writes INSIDE this
	// transaction, so the data and the ops it emits commit atomically. The
	// drainer assigns sequences and ships post-commit (see
	// claude/plans/replication-journal.md). Replaces the old post-commit
	// replication_emit_sql_command flush, whose emit could be lost if the
	// process died after the data committed.
	recorded := false
	if !h.suppressed {
		for _, e := range h.pending_emits {
			if !journal_replicates(false, h.av, e.sql) {
				continue
			}
			if err := journal_record_tx(h.tx, repl_op_exec, h.av.Database.Schema, e.sql, e.args); err != nil {
				h.tx.Rollback()
				h.closed = true
				h.pending_emits = nil
				return sl_error(fn, "commit failed: %v", err)
			}
			recorded = true
		}
	}

	if err := h.tx.Commit(); err != nil {
		h.closed = true
		h.pending_emits = nil
		return sl_error(fn, "commit failed: %v", err)
	}
	h.closed = true
	h.pending_emits = nil
	if recorded {
		journal_wake_app(h.user, h.app)
	}
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

	// Ensure the replication journal exists before the transaction opens, so
	// sl_commit can insert journal rows on this same connection's tx.
	journal_ensure(db)

	tx, err := db.starlark.Beginx()
	if err != nil {
		return sl_error(fn, "begin failed: %v", err)
	}

	h := &TransactionHandle{tx: tx}
	if suppressed, _ := t.Local("replication_suppressed").(bool); suppressed {
		h.suppressed = true
	}
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

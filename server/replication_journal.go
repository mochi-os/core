package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	cbor "github.com/fxamacker/cbor/v2"
	"github.com/jmoiron/sqlx"
)

// Durable transactional journal for app-scope replication.
//
// Every replicated app-scope write records the op to emit in an `journal`
// table inside the SAME transaction as the data mutation. Replication is then
// driven by draining the journal, never by the live peer set at write time.
// This closes two atomicity gaps that previously diverged replicas silently:
//
//   - Gap A: a write made with no live/reparable peer used to return before
//     allocating a sequence, so it was never journaled and never re-emitted.
//     Now it is recorded regardless of peers, its sequence is assigned, and
//     the tail advances — so a peer that pairs later receives the data via
//     bootstrap (cursor seeded to the consistent tail) and an already-paired
//     peer that was briefly absent is backfilled from the journal (#23).
//   - Gap B: a crash between the data commit and the queue enqueue used to
//     burn a sequence (or lose the op). Now the op is committed atomically
//     with the data, and the drainer (re-)assigns a sequence idempotently per
//     journal-id, so a re-drain after a crash reuses the same sequence and
//     never burns one.
//
// The receive gate dedups on seen(sequence) and chains per-stream on Prev, so
// re-shipping an already-assigned (sequence, prev, payload) is idempotent —
// which is what makes "ship at least once, possibly again later" converge.
//
// See claude/plans/replication-journal.md for the full design and rollout.

// ============================================================
// Write side: record the op atomically with the data mutation
// ============================================================

// journal_ensured caches which data-DB file paths have had their core-managed
// `journal` table created this process, so the DDL runs once per file rather
// than on every write. journal_ensure_mu serialises the create so concurrent
// first-time writers to a fresh DB don't all run the DDL at once.
var (
	journal_ensured   sync.Map // db.path -> struct{}
	journal_ensure_mu sync.Mutex
)

// journal_ensure lazily creates the per-DB replication journal on the internal
// (server-trusted) pool. The table is visible to the starlark pool's
// connections (same file), so the in-transaction insert on the write
// connection finds it.
func journal_ensure(db *DB) {
	if db == nil || db.internal == nil || db.path == "" {
		return
	}
	if _, done := journal_ensured.Load(db.path); done {
		return
	}
	journal_ensure_mu.Lock()
	defer journal_ensure_mu.Unlock()
	if _, done := journal_ensured.Load(db.path); done { // re-check under the lock
		return
	}
	db.exec(`create table if not exists journal (
		id text primary key,
		operation text not null,
		statement text not null,
		args blob not null,
		target text not null default '',
		uid text not null default '',
		schema integer not null default 0,
		created integer not null,
		state text not null default 'pending'
	)`)
	db.exec("create index if not exists journal_pending on journal(state, created)")
	journal_ensured.Store(db.path, struct{}{})
}

// journal_replicates reports whether an app-scope write should be journaled to
// the journal: not a migration write (replication_suppressed thread-local, set
// by (*AppVersion).starlark_db so every replica migrates itself), and its
// target table not excluded from replication.
func journal_replicates(suppressed bool, av *AppVersion, statement string) bool {
	if suppressed || av == nil {
		return false
	}
	return !sql_table_excluded(av, sql_target_table(statement))
}

// journal_record_tx inserts the replication op for one app-scope write into the
// journal within the caller's transaction. operation is repl_op_exec (per-app
// data DB) or repl_op_exec_app_system (app.db); schema is the op's Schema stamp
// (av.Database.Schema for the data path, 0 for app-system — matching the legacy
// emit). The (user, app) that own the stream are implied by which journal the
// drainer reads, so they are not stored on the row.
func journal_record_tx(tx *sqlx.Tx, operation string, schema int, statement string, args []any) error {
	_, err := tx.Exec(
		"insert into journal (id, operation, statement, args, target, uid, schema, created, state) "+
			"values (?, ?, ?, ?, ?, ?, ?, ?, 'pending')",
		uid(), operation, statement, cbor_encode(args),
		sql_target_table(statement), sql_target_uid(statement, args),
		schema, now())
	return err
}

// journal_table_replicates reports whether a parsed target table replicates on
// the app-system path: non-empty and not a default-excluded infra table. The
// app-system scope has no per-app exclude list, so this matches
// replication_emit_app_system_exec's gate exactly.
func journal_table_replicates(table string) bool {
	if table == "" {
		return false
	}
	for _, prefix := range sql_default_excluded {
		if strings.HasPrefix(table, prefix) {
			return false
		}
	}
	return true
}

// journal_app_dbs returns the relative paths of the DBs for one app that may
// carry a journal: every data DB under db/ plus the system app.db. Only
// existing files are returned, so callers can db_open each without creating
// junk 0-byte files.
func journal_app_dbs(userUID, appID string) []string {
	var out []string
	root := filepath.Join(data_dir, "users", userUID, appID)
	if files, err := os.ReadDir(filepath.Join(root, "db")); err == nil {
		for _, fe := range files {
			if !fe.IsDir() && strings.HasSuffix(fe.Name(), ".db") {
				out = append(out, fmt.Sprintf("users/%s/%s/db/%s", userUID, appID, fe.Name()))
			}
		}
	}
	if st, err := os.Stat(filepath.Join(root, "app.db")); err == nil && !st.IsDir() {
		out = append(out, fmt.Sprintf("users/%s/%s/app.db", userUID, appID))
	}
	return out
}

// db_execute_journal runs a single app-scope write (mochi.db.execute) and, when
// the write replicates, records its journal row in the same transaction so the
// data and the op commit atomically. Returns whether an journal row was written
// (so the caller wakes the drainer) and any data-write error. A non-replicated
// write keeps the cheap autocommit path. The caller supplies the already-
// checked-out write connection (from db.starlark) so the journal insert shares
// the data write's connection — required for the two to land in one
// transaction.
func db_execute_journal(ctx context.Context, conn *sqlx.Conn, db *DB, av *AppVersion, suppressed bool, query string, args []any) (bool, error) {
	if !journal_replicates(suppressed, av, query) {
		_, err := conn.ExecContext(ctx, query, args...)
		return false, err
	}

	journal_ensure(db)

	tx, err := conn.BeginTxx(ctx, nil)
	if err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		tx.Rollback()
		return false, err
	}
	if err := journal_record_tx(tx, repl_op_exec, av.Database.Schema, query, args); err != nil {
		tx.Rollback()
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// ============================================================
// Drain side: assign a sequence idempotently and ship
// ============================================================

// replication_journal_tables_ensure lazily creates the replication.db tables
// backing idempotent sequence assignment (journal_sequence binds an op id to
// its allocated sequence/prev). The `sequence`/`tail` creates are
// belt-and-suspenders (production creates them at
// init) so assignment is robust if it runs before that init and so tests with a
// fresh data_dir work. Keyed by the replication.db path so each test's temp
// data_dir initialises once rather than a process-wide sync.Once that would
// pin the first test's directory.
var (
	replication_journal_tables_ensured sync.Map // replication.db path -> struct{}
	replication_journal_tables_mu      sync.Mutex
)

func replication_journal_tables_ensure() {
	rdb := db_open("db/replication.db")
	if rdb == nil || rdb.path == "" {
		return
	}
	if _, done := replication_journal_tables_ensured.Load(rdb.path); done {
		return
	}
	replication_journal_tables_mu.Lock()
	defer replication_journal_tables_mu.Unlock()
	if _, done := replication_journal_tables_ensured.Load(rdb.path); done { // re-check under the lock
		return
	}
	rdb.exec("create table if not exists sequence (user text not null default '', scope text not null, next integer not null default 0, primary key (user, scope))")
	rdb.exec("create table if not exists tail (user text not null default '', scope text not null, db text not null default '', last integer not null default 0, primary key (user, scope, db))")
	rdb.exec("create table if not exists journal_sequence (id text primary key, user text not null, scope text not null, stream text not null, sequence integer not null, prev integer not null)")
	replication_journal_tables_ensured.Store(rdb.path, struct{}{})
}

// replication_journal_binding returns the (sequence, prev) already bound to an
// journal id, if any. A re-drain after a crash finds the binding and reuses it.
func replication_journal_binding(rdb *DB, journalID string) (int64, int64, bool) {
	row, err := rdb.row("select sequence, prev from journal_sequence where id=?", journalID)
	if err != nil || row == nil {
		return 0, 0, false
	}
	seq, _ := row["sequence"].(int64)
	prev, _ := row["prev"].(int64)
	return seq, prev, true
}

// replication_journal_assign allocates (sequence, prev) for an journal op and
// binds them to the journal id, all in one replication.db transaction so the
// counter bump, tail advance and binding insert are atomic. Re-running for the
// same id returns the existing binding without consuming a new sequence — the
// idempotency that closes Gap B's assign-then-crash window. Returns (0, 0) on
// failure; the caller leaves the row pending for the next drain.
func replication_journal_assign(userUID string, op *ReplicationOp, journalID string) (int64, int64) {
	replication_journal_tables_ensure()
	stream := repl_op_stream(op)
	rdb := db_open("db/replication.db")

	// Fast path: already bound (the common case on a post-crash re-drain).
	if seq, prev, ok := replication_journal_binding(rdb, journalID); ok {
		return seq, prev
	}

	// Serialise allocation per (user, scope, stream) against the live emit
	// path, which locks the same mutex around its sequence/tail helpers.
	mu := replication_emit_lock(userUID, op.Scope, stream)
	mu.Lock()
	defer mu.Unlock()

	if seq, prev, ok := replication_journal_binding(rdb, journalID); ok {
		return seq, prev
	}

	tx, err := rdb.internal.Beginx()
	if err != nil {
		return 0, 0
	}
	rollback := func() (int64, int64) { tx.Rollback(); return 0, 0 }

	if _, err := tx.Exec("insert or ignore into sequence (user, scope, next) values (?, ?, 0)", userUID, op.Scope); err != nil {
		return rollback()
	}
	if _, err := tx.Exec("update sequence set next = next + 1 where user=? and scope=?", userUID, op.Scope); err != nil {
		return rollback()
	}
	var seq int64
	if err := tx.Get(&seq, "select next from sequence where user=? and scope=?", userUID, op.Scope); err != nil {
		return rollback()
	}
	var prev int64
	if err := tx.Get(&prev, "select coalesce((select last from tail where user=? and scope=? and db=?), 0)", userUID, op.Scope, stream); err != nil {
		return rollback()
	}
	if _, err := tx.Exec("insert into tail (user, scope, db, last) values (?, ?, ?, ?) on conflict(user, scope, db) do update set last=excluded.last", userUID, op.Scope, stream, seq); err != nil {
		return rollback()
	}
	if _, err := tx.Exec("insert or ignore into journal_sequence (id, user, scope, stream, sequence, prev) values (?, ?, ?, ?, ?, ?)", journalID, userUID, op.Scope, stream, seq, prev); err != nil {
		return rollback()
	}
	if err := tx.Commit(); err != nil {
		return 0, 0
	}
	return seq, prev
}

// journal_row_to_op reconstructs the replication op from one journal row.
// Returns the row id and the op (without Sequence/Prev, which are assigned at
// ship time).
func journal_row_to_op(userUID, appID string, r map[string]any) (string, *ReplicationOp) {
	id, _ := r["id"].(string)
	operation, _ := r["operation"].(string)
	if operation == "" {
		operation = repl_op_exec
	}
	statement, _ := r["statement"].(string)
	target, _ := r["target"].(string)
	rowuid, _ := r["uid"].(string)
	schema := 0
	if s, ok := r["schema"].(int64); ok {
		schema = int(s)
	}
	var args []any
	if s, ok := r["args"].(string); ok && s != "" {
		_ = cbor.Unmarshal([]byte(s), &args)
	}
	return id, &ReplicationOp{
		Scope:     repl_scope_app,
		User:      userUID,
		Database:  appID,
		Table:     target,
		UID:       rowuid,
		Operation: operation,
		Payload:   cbor_encode(&SQLCommand{Statement: statement, Args: args}),
		Schema:    schema,
	}
}

// journal_ship signs and sends an op (with its Sequence/Prev already set) to
// the given peers via the transport queue. Shared by the live drain and the
// peer backfill. Skips irreparable peers and stamps origin/fence. A package
// var so tests can capture shipped ops without the net/message stack.
var journal_ship = journal_ship_real

func journal_ship_real(userUID string, op *ReplicationOp, peers []string) {
	if len(peers) == 0 {
		return
	}
	replication_origin_ensure(op)
	if op.LeaderScope != "" && op.LeaderKey != "" && op.Fence == 0 {
		op.Fence = replication_leader_fence(op.LeaderScope, op.LeaderKey)
	}

	// Signing entity for this user (the user column is the TEXT uid).
	udb := db_open("db/users.db")
	row, err := udb.row("select id from entities where user=? order by id limit 1", userUID)
	if err != nil || row == nil {
		return
	}
	from, _ := row["id"].(string)
	if from == "" {
		return
	}

	for _, peer := range peers {
		if irreparable_emit_skip(userUID, peer) {
			continue
		}
		m := message(from, from, "replication", "sql/op")
		m.add(op)
		m.send_peer(peer)
	}
}

// replication_emit_journal assigns the op its (sequence, prev) idempotently and
// ships it to the user's current recipients. Unlike the live emit path it does
// NOT bail out when there are no recipients: the op is still journaled into the
// stream (sequence assigned, tail advanced) so the stream stays consistent for
// a future bootstrap, and an absent-but-paired peer is backfilled later (#23).
// Returns true once a sequence is assigned (the row may then be marked
// shipped); false if assignment failed, so the caller retries on the next tick.
var replication_emit_journal = replication_emit_journal_real

func replication_emit_journal_real(userUID string, op *ReplicationOp, journalID string) bool {
	if op.Scope != repl_scope_app {
		return false
	}

	op.Sequence, op.Prev = replication_journal_assign(userUID, op, journalID)
	if op.Sequence == 0 {
		return false
	}
	if replication_op_self_anchoring(op) {
		op.Prev = 0
	}

	journal_ship(userUID, op, recipients(userUID))
	return true
}

// journal_drain ships every pending row of one data DB's journal in created
// order, marking each shipped once its sequence is assigned. It stops at the
// first row whose assignment fails, preserving in-stream order for the retry.
func journal_drain(userUID, appID string, db *DB) {
	if db == nil {
		return
	}
	rows, err := db.rows("select id, operation, statement, args, target, uid, schema from journal where state='pending' order by created, id")
	if err != nil || len(rows) == 0 {
		return
	}
	for _, r := range rows {
		id, op := journal_row_to_op(userUID, appID, r)
		if !replication_emit_journal(userUID, op, id) {
			return
		}
		db.exec("update journal set state='shipped' where id=?", id)
	}
}

// ============================================================
// Manager: wake on write, periodic safety sweep, startup recovery
// ============================================================

type journal_request struct {
	user *User
	app  *App
}

var (
	journal_dirty_mu sync.Mutex
	journal_dirty    = map[string]journal_request{}
)

// journal_wake marks (user, app) as having pending journal rows so the next
// manager tick drains it. Never drops: a map entry coalesces repeat wakes.
func journal_wake(db *DB) {
	if db == nil || db.user == nil || db.app == nil {
		return
	}
	journal_dirty_mu.Lock()
	journal_dirty[db.user.UID+"|"+db.app.id] = journal_request{user: db.user, app: db.app}
	journal_dirty_mu.Unlock()
}

// journal_wake_app is journal_wake for callers holding (user, app) rather than a
// *DB (the transaction-commit path).
func journal_wake_app(user *User, app *App) {
	if user == nil || app == nil {
		return
	}
	journal_dirty_mu.Lock()
	journal_dirty[user.UID+"|"+app.id] = journal_request{user: user, app: app}
	journal_dirty_mu.Unlock()
}

func journal_drain_app(user *User, app *App) {
	if user == nil || app == nil {
		return
	}
	for _, rel := range journal_app_dbs(user.UID, app.id) {
		db := db_open(rel)
		if db == nil {
			continue
		}
		journal_drain(user.UID, app.id, db)
	}
}

// journal_manager drains the dirty set every second (low latency, like the
// queue manager) and runs a full filesystem recovery sweep at startup and
// periodically thereafter as the correctness backstop for rows a crash left
// pending without an in-memory wake.
func journal_manager() {
	journal_sweep()
	ticks := 0
	for range time.Tick(time.Second) {
		journal_dirty_mu.Lock()
		batch := journal_dirty
		journal_dirty = map[string]journal_request{}
		journal_dirty_mu.Unlock()
		for _, req := range batch {
			journal_drain_app(req.user, req.app)
		}
		if ticks++; ticks >= 300 { // every ~5 minutes
			ticks = 0
			journal_sweep()
		}
	}
}

// journal_sweep walks every per-user app DB (data DBs + app.db) and drains any
// that carry a pending journal, pruning shipped ops as it goes. Bounded work:
// it only opens DBs that exist and only drains those with a pending row. Runs
// at startup (crash recovery) and periodically.
func journal_sweep() {
	base := filepath.Join(data_dir, "users")
	users, err := os.ReadDir(base)
	if err != nil {
		return
	}
	for _, ue := range users {
		if !ue.IsDir() {
			continue
		}
		userUID := ue.Name()
		apps, err := os.ReadDir(filepath.Join(base, userUID))
		if err != nil {
			continue
		}
		for _, ae := range apps {
			if !ae.IsDir() {
				continue
			}
			appID := ae.Name()
			for _, rel := range journal_app_dbs(userUID, appID) {
				db := db_open(rel)
				if db == nil {
					continue
				}
				if has, _ := db.exists("select 1 from journal where state='pending' limit 1"); has {
					journal_drain(userUID, appID, db)
				}
				journal_prune(db)
			}
		}
	}
}

// Retention bounds for shipped journal ops. A shipped op is kept so a
// transiently-absent peer can be backfilled (#23); past these bounds it is
// pruned and a peer that far behind falls back to bootstrap. Tunable later;
// generous defaults so normal outages are covered.
var (
	journal_retention_age     int64 = 7 * 24 * 60 * 60 // seconds
	journal_retention_minimum       = 1000             // most-recent rows kept regardless of age
)

// journal_prune deletes shipped journal ops older than the retention age,
// always keeping the most recent journal_retention_minimum rows so a recently
// active stream stays backfillable. Pending rows are never pruned. The
// matching idempotency binding in replication.db is dropped with each row.
func journal_prune(db *DB) {
	if db == nil {
		return
	}
	cutoff := now() - journal_retention_age
	ids, err := db.rows(
		"select id from journal where state='shipped' and created < ? "+
			"and id not in (select id from journal where state='shipped' order by created desc limit ?)",
		cutoff, journal_retention_minimum)
	if err != nil || len(ids) == 0 {
		return
	}
	rdb := db_open("db/replication.db")
	pruned := 0
	for _, r := range ids {
		id, _ := r["id"].(string)
		if id == "" {
			continue
		}
		db.exec("delete from journal where id=?", id)
		rdb.exec("delete from journal_sequence where id=?", id)
		pruned++
	}
	if pruned > 0 {
		info("Replication journal pruned %d shipped op(s) older than %dd from %s",
			pruned, journal_retention_age/86400, db.path)
	}
}

// ============================================================
// Backfill: re-ship retained ops to a peer that has (re)appeared
// ============================================================

// journal_backfill_to_peer re-ships every retained journal op to a peer that
// has just become reachable again, for each of this host's users that count
// the peer as a recipient. A transiently-absent peer thus receives ops written
// while it was gone WITHOUT a full re-bootstrap; the receiver dedups already-
// applied ops via `seen` and chains the rest. Fired from peer_reconnect on a
// successful reconnect. (A brand-new pair member is seeded by bootstrap
// instead, so this is a no-op there — its cursor is at the tail and the
// re-shipped ops are dropped below it.)
func journal_backfill_to_peer(peer string) {
	if peer == "" || peer == net_id {
		return
	}
	base := filepath.Join(data_dir, "users")
	users, err := os.ReadDir(base)
	if err != nil {
		return
	}
	for _, ue := range users {
		if !ue.IsDir() {
			continue
		}
		userUID := ue.Name()
		if !slices.Contains(recipients(userUID), peer) {
			continue
		}
		journal_backfill_peer(userUID, peer)
	}
}

// journal_backfill_peer re-ships one user's retained (shipped) journal ops to a
// single peer, in stream order, reusing each op's already-assigned (sequence,
// prev) so the receiver's chain lines up. Pending rows are left to the normal
// drain (which now counts this peer as a recipient).
func journal_backfill_peer(userUID, peer string) {
	base := filepath.Join(data_dir, "users", userUID)
	apps, err := os.ReadDir(base)
	if err != nil {
		return
	}
	rdb := db_open("db/replication.db")
	for _, ae := range apps {
		if !ae.IsDir() {
			continue
		}
		appID := ae.Name()
		for _, rel := range journal_app_dbs(userUID, appID) {
			db := db_open(rel)
			if db == nil {
				continue
			}
			rows, err := db.rows("select id, operation, statement, args, target, uid, schema from journal where state='shipped' order by created, id")
			if err != nil || len(rows) == 0 {
				continue
			}
			for _, r := range rows {
				id, op := journal_row_to_op(userUID, appID, r)
				seq, prev, ok := replication_journal_binding(rdb, id)
				if !ok {
					continue
				}
				op.Sequence, op.Prev = seq, prev
				if replication_op_self_anchoring(op) {
					op.Prev = 0
				}
				journal_ship(userUID, op, []string{peer})
			}
		}
	}
}

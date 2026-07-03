// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

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

// journal_setup creates the per-DB replication journal on the internal
// (server-trusted) pool. Called once per open from db_app / db_app_system (the
// per-app equivalent of db_create), alongside access/attachments setup — not
// lazily on first write. The table is visible to the starlark pool's
// connections (same file), so the in-transaction insert on the write
// connection finds it. Idempotent: a pre-existing DB gets the table on its next
// open.
func (db *DB) journal_setup() {
	if db == nil || db.internal == nil || db.path == "" {
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

// journal_guard opens rel and runs work against it, but never lets a corrupt
// per-user DB crash the journal_manager goroutine — which has no recover of its
// own and runs journal_sweep at startup, so a panic here was the corrupt-app-DB
// crash-loop. A quarantined DB is skipped; a corruption panic from any
// journal write quarantines the DB and is swallowed so the sweep/drain moves on
// to the next user; any other panic re-fires so a genuine bug still surfaces.
func journal_guard(rel string, work func(db *DB)) {
	if db_quarantined(rel) {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(error); ok && db_error_is_corruption(e) {
				db_quarantine(rel, "journal sweep", e)
				return
			}
			panic(r)
		}
	}()
	db := db_open(rel)
	if db == nil {
		return
	}
	work(db)
}

// db_execute_journal runs a single app-scope write (mochi.db.execute) and, when
// the write replicates, records its journal row in the same transaction so the
// data and the op commit atomically. Returns the number of rows the write
// affected (mochi.db.execute's return value), whether a journal row was written
// (so the caller wakes the drainer), and any data-write error. A non-replicated
// write keeps the cheap autocommit path. The caller supplies the already-
// checked-out write connection (from db.starlark) so the journal insert shares
// the data write's connection — required for the two to land in one
// transaction.
func db_execute_journal(ctx context.Context, conn *sqlx.Conn, db *DB, av *AppVersion, suppressed bool, query string, args []any) (int64, bool, error) {
	if !journal_replicates(suppressed, av, query) {
		res, err := conn.ExecContext(ctx, query, args...)
		if err != nil {
			return 0, false, err
		}
		affected, _ := res.RowsAffected()
		return affected, false, nil
	}

	tx, err := conn.BeginTxx(ctx, nil)
	if err != nil {
		return 0, false, err
	}
	res, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		tx.Rollback()
		return 0, false, err
	}
	if err := journal_record_tx(tx, repl_op_exec, av.Database.Schema, query, args); err != nil {
		tx.Rollback()
		return 0, false, err
	}
	if err := tx.Commit(); err != nil {
		return 0, false, err
	}
	affected, _ := res.RowsAffected()
	return affected, true, nil
}

// ============================================================
// Drain side: assign a sequence idempotently and ship
// ============================================================

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
	// (#65) Stamp our current outbound generation so a receiver re-anchors if we
	// have reset our sequence space since it last heard from us.
	op.Epoch = replication_epoch_current()

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

	stream := repl_op_stream(op)
	for _, peer := range peers {
		if irreparable_emit_skip(userUID, peer) {
			continue
		}
		m := message(from, from, "replication", "sql/op")
		m.add(op)
		m.send_peer(peer)
		// Record the in-flight op so the transport ack can advance this peer's
		// delivery cursor (#28) — lets the reconnect backfill ship only the
		// delta above what the peer has already confirmed.
		journal_inflight_record(m.ID, userUID, peer, stream, op.Sequence)
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
		journal_guard(rel, func(db *DB) {
			journal_drain(user.UID, app.id, db)
		})
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
				journal_guard(rel, func(db *DB) {
					// The startup sweep is the eager-creation pass for pre-existing
					// per-app journals: create the table here so a later write via
					// this same cached handle (app.db shares this cache key) never
					// hits a missing table (#424).
					db.journal_setup()
					if has, _ := db.exists("select 1 from journal where state='pending' limit 1"); has {
						journal_drain(userUID, appID, db)
					}
					journal_prune(db, userUID)
				})
			}
		}
	}
	journal_inflight_sweep() // drop inflight rows whose message never acked (#28)

	// #162: periodically re-ship unconfirmed ops to CONNECTED peers, not only on
	// reconnect. journal_drain marks a row shipped once its sequence is assigned
	// (even when journal_ship_real sent to nobody), and the sole re-ship trigger is
	// peer reconnect — so a send dropped to a peer that stays continuously connected
	// is otherwise never retried until it happens to reconnect. journal_backfill_to_peer
	// is delivery-cursor-gated (a caught-up peer gets nothing) and filters to users
	// the peer actually replicates, so a connected non-recipient is a cheap no-op.
	if net_me != nil {
		for _, p := range net_me.Network().Peers() {
			go journal_backfill_to_peer(p.String())
		}
	}
}

// Retention bounds for shipped journal ops. A shipped op is kept so a
// transiently-absent peer can be backfilled (#23); past these bounds it is
// pruned and a peer that far behind falls back to bootstrap. Tunable later;
// generous defaults so normal outages are covered.
var (
	journal_retention_age      int64 = 7 * 24 * 60 * 60  // seconds — a CONFIRMED op is pruned past this
	journal_retention_hard_age int64 = 30 * 24 * 60 * 60 // seconds — even an UNconfirmed op is pruned past this (#163); a peer that far behind re-bootstraps
	journal_retention_minimum        = 1000              // most-recent rows kept regardless of age
)

// journal_prune deletes shipped journal ops older than the retention age,
// always keeping the most recent journal_retention_minimum rows so a recently
// active stream stays backfillable. Pending rows are never pruned. The
// matching idempotency binding in replication.db is dropped with each row.
func journal_prune(db *DB, userUID string) {
	if db == nil {
		return
	}
	softCutoff := now() - journal_retention_age
	rows, err := db.rows(
		"select id, created from journal where state='shipped' and created < ? "+
			"and id not in (select id from journal where state='shipped' order by created desc limit ?)",
		softCutoff, journal_retention_minimum)
	if err != nil || len(rows) == 0 {
		return
	}
	rdb := db_open("db/replication.db")
	hardCutoff := now() - journal_retention_hard_age
	recips := recipients(userUID)
	// #163: don't prune a shipped op a still-paired peer hasn't confirmed. Per
	// stream, minConfirmed is the LOWEST delivery cursor across the user's current
	// recipients (#28); an op at or below it is delivered to everyone, so it's safe
	// to drop; above it a peer still needs it via the reconnect/periodic backfill,
	// and pruning it here would strand that peer with an unfillable gap. Past the
	// hard cap we prune anyway — a peer that far behind falls back to a full
	// bootstrap (accepting the intervening per-op deltas, fine for LWW-registers).
	// The binding's stream == the delivery cursor's stream (both repl_op_stream(op)).
	minConfirmed := map[string]int64{}
	pruned := 0
	for _, r := range rows {
		id, _ := r["id"].(string)
		if id == "" {
			continue
		}
		created, _ := r["created"].(int64)
		if len(recips) > 0 && created >= hardCutoff {
			if b, _ := rdb.row("select sequence, stream from journal_sequence where id=?", id); b != nil {
				seq, _ := b["sequence"].(int64)
				stream, _ := b["stream"].(string)
				mc, seen := minConfirmed[stream]
				if !seen {
					mc = -1
					for _, peer := range recips {
						if c := journal_delivery_cursor(rdb, userUID, peer, stream); mc < 0 || c < mc {
							mc = c
						}
					}
					minConfirmed[stream] = mc
				}
				if mc >= 0 && seq > mc {
					continue // a still-paired peer hasn't confirmed this op — keep it backfillable
				}
			}
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
	// Delivery cursor per stream, read once at the start of the backfill (#28):
	// ship only ops above what the peer has already confirmed, instead of the
	// whole retained window. The receiver still dedups, so a stale cursor only
	// costs a few redundant re-ships, never correctness.
	cursors := map[string]int64{}
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
				stream := repl_op_stream(op)
				cur, seen := cursors[stream]
				if !seen {
					cur = journal_delivery_cursor(rdb, userUID, peer, stream)
					cursors[stream] = cur
				}
				if op.Sequence <= cur {
					continue // peer already confirmed this op
				}
				if replication_op_self_anchoring(op) {
					op.Prev = 0
				}
				journal_ship(userUID, op, []string{peer})
			}
		}
	}
}

// --- Receiver-initiated gap-fill ------------------------------------------
//
// An anchored gap — a predecessor op that never arrived — wedges an inbound
// stream and does not self-heal: the drain only re-evaluates already-buffered
// ops, and the reconnect backfill (journal_backfill_peer) skips anything the
// peer's delivery cursor already marked confirmed. So an op the peer recorded as
// received but the receiver never APPLIED (lost/dropped after receipt) is never
// re-sent. The receiver alone knows the exact missing range, so it asks the peer
// to re-ship it. Purely additive — the ops land through the normal chain gate —
// so there is no reseed and none of the #42 wipe risk. A gap the peer can no
// longer fill (ops pruned past T_forget) stays stalled and falls through to the
// existing operator alert; an auto-reseed fallback is deliberately out of scope
// until a row-level subset gate exists (#70/#42).

// gapfill_min_stall_seconds is how long an anchored gap must persist before we
// ask the peer to re-ship — long enough for ordinary in-flight lag to resolve
// itself first. gapfill_backoff_seconds bounds how often a still-wedged stream
// is re-asked.
const gapfill_min_stall_seconds = 60
const gapfill_backoff_seconds = 60

// gapfill_attempt tracks per-stream gap-fill progress so the request loop can
// bound retries: the last request time (backoff), the apply-cursor seen at that
// request, and how many consecutive requests have NOT moved the cursor. Only the
// (single) manager goroutine touches the map — from replication_gapfill_request
// and the stall-alert's gapfill_reship_exhausted read.
type gapfill_attempt struct {
	last     int64 // unix-seconds of the last request
	cursor   int64 // apply-cursor observed at the last request
	attempts int   // consecutive requests with no cursor progress
}

var gapfill_requested = map[string]gapfill_attempt{}

// gapfill_max_attempts bounds re-ship requests for one wedged stream: after this
// many consecutive requests with no cursor progress, the peer evidently cannot
// supply the missing ops (pruned past retention, or a journal gap), so we stop
// asking and let the (sharpened) stall alert escalate it to an operator reseed.
// At gapfill_backoff_seconds apart that's ~5 min — well under the 15-min stall
// alert, so the gap is classified unfillable before the operator is paged.
const gapfill_max_attempts = 5

// gapfill_should_request advances the per-stream retry bookkeeping and reports
// whether to send a re-ship request now. It resets the no-progress counter the
// moment the cursor advances (re-ship or ordinary delivery made progress),
// returns false once gapfill_max_attempts no-progress requests have been sent
// (re-ship exhausted), and otherwise honours the backoff. nowSec is a parameter
// so the bounding logic is unit-testable without the wire/queue path.
func gapfill_should_request(key string, cursor, nowSec int64) bool {
	st := gapfill_requested[key]
	if cursor > st.cursor {
		st.attempts = 0
		st.cursor = cursor
		gapfill_requested[key] = st
	}
	if st.attempts >= gapfill_max_attempts {
		return false
	}
	if st.last != 0 && nowSec-st.last < gapfill_backoff_seconds {
		return false
	}
	st.last = nowSec
	st.cursor = cursor
	st.attempts++
	gapfill_requested[key] = st
	return true
}

// gapfill_reship_exhausted reports whether the gap-fill has given up re-ship
// requests for this stream (gapfill_max_attempts with no cursor progress) — the
// peer can't supply the missing ops, so an operator reseed is the remedy. Read by
// the stall alert to sharpen its message.
func gapfill_reship_exhausted(s StalledStream) bool {
	key := s.Peer + "|" + s.Scope + "|" + s.User + "|" + s.Database
	return gapfill_requested[key].attempts >= gapfill_max_attempts
}

// replication_gapfill_request asks each peer to re-ship the ops missing from a
// wedged inbound stream (the self-healing half of an anchored gap), bounded by
// gapfill_max_attempts. Called from the replication manager after the drain.
func replication_gapfill_request() {
	live := map[string]bool{}
	for _, s := range replication_pending_stalled() {
		// v1: app streams only (journal-replayable), and only true anchored gaps
		// (a real missing predecessor — not a fresh, un-seeded stream).
		if !s.Anchored || !strings.HasPrefix(s.Database, "app:") {
			continue
		}
		key := s.Peer + "|" + s.Scope + "|" + s.User + "|" + s.Database
		live[key] = true
		if now()-s.Oldest < gapfill_min_stall_seconds {
			continue // let ordinary in-flight lag resolve first
		}
		if !gapfill_should_request(key, s.Cursor, now()) {
			continue // within backoff, or re-ship exhausted (stall alert escalates)
		}
		m := message("", "", "replication", "replica/gapfill")
		m.content = gapfill_request_content(s)
		m.send_peer(s.Peer)
		info("Replication gap-fill requested: peer=%q user=%q db=%q range=[%d,%d] attempt=%d/%d (stalled %ds)",
			s.Peer, s.User, s.Database, s.Cursor+1, s.Predecessor.Maximum,
			gapfill_requested[key].attempts, gapfill_max_attempts, now()-s.Oldest)
	}
	// Forget streams no longer stalled (healed/drained) so the map stays bounded
	// and a future re-stall of the same stream starts its count fresh.
	for key := range gapfill_requested {
		if !live[key] {
			delete(gapfill_requested, key)
		}
	}
}

// gapfill_request_content builds the wire payload of a gap-fill request from a
// stalled stream: the stream identity plus the exact missing window
// [cursor+1 .. predecessor.max]. Factored out so the request->serve round-trip is
// testable through real cbor — the from/to integers decode as uint64 on the wire,
// which gapfill_seq must handle (the bug that made the serve handler a no-op).
func gapfill_request_content(s StalledStream) map[string]any {
	return map[string]any{
		"scope": s.Scope,
		"user":  s.User,
		"db":    s.Database,
		"from":  s.Cursor + 1,
		"to":    s.Predecessor.Maximum,
	}
}

// replication_gapfill_event serves a peer's gap-fill request: it has wedged on an
// anchored gap in its inbound stream from us and wants the missing op range
// re-shipped so it can self-heal without an operator reseed.
func replication_gapfill_event(e *Event) {
	scope, _ := e.content["scope"].(string)
	user, _ := e.content["user"].(string)
	database, _ := e.content["db"].(string)
	from := gapfill_seq(e.content["from"])
	to := gapfill_seq(e.content["to"])
	if scope == "" || user == "" || database == "" || from <= 0 || to < from {
		return
	}
	n := journal_reship_range(user, e.peer, database, from, to)
	info("Replication gap-fill served: peer=%q user=%q db=%q range=[%d,%d] reshipped=%d",
		e.peer, user, database, from, to, n)
}

// gapfill_seq coerces a JSON-decoded numeric field (float64 over the wire) to int64.
func gapfill_seq(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case uint64:
		// cbor decodes positive integers into interface{} as uint64, so the
		// wire-decoded from/to land here — not int64.
		return int64(n)
	case int:
		return int64(n)
	case float64:
		return int64(n)
	}
	return 0
}

// journal_reship_range re-ships this user's journal ops for one stream whose
// binding sequence is in [from, to] to the requesting peer, reusing each op's
// original (sequence, prev) so the receiver's chain lines up. Unlike
// journal_backfill_peer it is NOT delivery-cursor-gated — the receiver asked
// precisely because it never applied these ops, so the "already confirmed" skip
// must not apply. Additive only. Returns the number of ops re-shipped.
func journal_reship_range(userUID, peer, stream string, from, to int64) int {
	// Only a host that is genuinely in this user's host set may pull their ops.
	if peer == "" || peer == net_id || !slices.Contains(recipients(userUID), peer) {
		return 0
	}
	// v1: app-scope streams only (journal-shipped + replayable).
	if !strings.HasPrefix(stream, "app:") {
		return 0
	}
	appID := strings.TrimSuffix(strings.TrimPrefix(stream, "app:"), "/system")
	rdb := db_open("db/replication.db")
	if rdb == nil {
		return 0
	}
	shipped := 0
	for _, rel := range journal_app_dbs(userUID, appID) {
		db := db_open(rel)
		if db == nil {
			continue
		}
		rows, err := db.rows("select id, operation, statement, args, target, uid, schema from journal where state='shipped' order by created, id")
		if err != nil {
			continue
		}
		for _, r := range rows {
			id, op := journal_row_to_op(userUID, appID, r)
			seq, prev, ok := replication_journal_binding(rdb, id)
			if !ok {
				continue
			}
			op.Sequence, op.Prev = seq, prev
			if repl_op_stream(op) != stream || seq < from || seq > to {
				continue
			}
			if replication_op_self_anchoring(op) {
				op.Prev = 0
			}
			journal_ship(userUID, op, []string{peer})
			shipped++
		}
	}
	return shipped
}

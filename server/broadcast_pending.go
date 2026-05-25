// Mochi server: broadcast pending buffer
// Copyright Alistair Cunningham 2026
//
// Per-app `_broadcast_pending` table that buffers out-of-order
// broadcast events on the subscriber side. Replaces the previous
// "NACK on gap" behaviour with "buffer + drain in chain order",
// mirroring the per-row replication pending pattern in replication.go.
//
// Why: the queue.go per-(target, from_entity) bucket has cap=1 for
// FK ordering on sql/op replication. Broadcast events were caught in
// the same restriction even though their ordering is enforced
// receiver-side by _sequence. With this buffer, the sender can blast
// multiple events to a subscriber out of order; the receiver drains
// them in chain order via this table. Combined with task #80 (drop
// on gap NACK) and task #81 (one-in-flight resync gate), this closes
// the "subscriber permanently behind" failure mode documented in
// claude/sessions/2026-05-25-broadcast-resync-seq-643-investigation.md.
//
// Bounded per (peer, key) at broadcast_pending_max so a single
// misbehaving stream can't grow the table unbounded. Inserts above
// the cap are dropped (with log) - the subscriber's resync request
// re-fetches them.

package main

const broadcast_pending_max = 1000

// broadcast_pending_table_create lazily creates the table; the call
// is idempotent and the schema matches the comment block above.
func broadcast_pending_table_create(db *DB) {
	db.exec(`create table if not exists _broadcast_pending (
		peer text not null,
		key text not null,
		sequence integer not null,
		source text not null,
		target text not null,
		service text not null,
		event text not null,
		msg_id text not null default '',
		sender_app text not null default '',
		sender_services text not null default '',
		content blob not null,
		received integer not null,
		primary key (peer, key, sequence)
	)`)
}

// broadcast_pending_count returns the current row count for one
// (peer, key) stream. Used by the insert path to enforce the per-
// stream cap and by the operator visibility surface (task #83).
func broadcast_pending_count(db *DB, peer, key string) int {
	exists, _ := db.exists("select 1 from sqlite_master where type='table' and name='_broadcast_pending'")
	if !exists {
		return 0
	}
	return db.integer("select count(*) from _broadcast_pending where peer=? and key=?", peer, key)
}

// broadcast_pending_insert buffers one out-of-order event. Returns
// true if the row was stored, false if dropped because the per-stream
// cap was reached. The caller still fires the resync request - either
// way the gap-fill path is initiated; the buffer is the
// "we already received this, replay it in order when the gap closes"
// optimisation, not the primary mechanism for filling missed events.
func broadcast_pending_insert(db *DB, peer, key string, sequence int64, source, target, service, event, msg_id, sender_app, sender_services string, content []byte) bool {
	broadcast_pending_table_create(db)
	if broadcast_pending_count(db, peer, key) >= broadcast_pending_max {
		debug("Broadcast pending dropping seq=%d for (peer=%s, key=%s): per-stream buffer full at %d", sequence, peer, key, broadcast_pending_max)
		return false
	}
	db.exec_app_user(`insert or ignore into _broadcast_pending
		(peer, key, sequence, source, target, service, event, msg_id, sender_app, sender_services, content, received)
		values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		peer, key, sequence, source, target, service, event, msg_id, sender_app, sender_services, content, now())
	return true
}

// broadcast_pending_row holds one buffered event's identity + payload.
// All scalar columns plus the CBOR-encoded content blob.
type broadcast_pending_row struct {
	Peer           string `db:"peer"`
	Key            string `db:"key"`
	Sequence       int64  `db:"sequence"`
	Source         string `db:"source"`
	Target         string `db:"target"`
	Service        string `db:"service"`
	Event          string `db:"event"`
	MsgID          string `db:"msg_id"`
	SenderApp      string `db:"sender_app"`
	SenderServices string `db:"sender_services"`
	Content        []byte `db:"content"`
	Received       int64  `db:"received"`
}

// broadcast_pending_next returns the buffered row matching the
// requested chain-link (peer, key, sequence) or nil if absent. The
// drain loop in broadcast_pending_drain_chain calls this for
// last+1 after every advance.
func broadcast_pending_next(db *DB, peer, key string, sequence int64) *broadcast_pending_row {
	exists, _ := db.exists("select 1 from sqlite_master where type='table' and name='_broadcast_pending'")
	if !exists {
		return nil
	}
	var row broadcast_pending_row
	if !db.scan(&row, "select * from _broadcast_pending where peer=? and key=? and sequence=?", peer, key, sequence) {
		return nil
	}
	return &row
}

// broadcast_pending_delete removes one drained row. Caller deletes
// only after the handler ran successfully and _received was advanced.
func broadcast_pending_delete(db *DB, peer, key string, sequence int64) {
	db.exec_app_user("delete from _broadcast_pending where peer=? and key=? and sequence=?", peer, key, sequence)
}

// broadcast_pending_dispatch is the package-level callback the drain
// loop uses to re-run a buffered event's handler. events.go sets it
// in init() to a closure that synthesises an Event from the row and
// invokes the same run_handler the route() path uses. Decoupled this
// way so broadcast_pending.go doesn't depend on the routing graph.
//
// Returns true if the handler ran cleanly and the row should be
// considered applied (delete + advance). False on any error: caller
// stops draining; the row stays in pending and another drain attempt
// happens after the NEXT advance, or after a resync round-trip
// inserts an earlier-numbered event.
var broadcast_pending_dispatch func(row *broadcast_pending_row, db *DB) bool

// broadcast_pending_drain_chain walks the pending buffer for one
// (peer, key) stream starting at the current _received.last+1.
// Each row that dispatches cleanly is removed, _received advances,
// then the loop looks for the next chain link. Stops at the first
// missing link or first dispatch failure. Bounded by the per-stream
// cap so it can't run forever.
//
// Called from broadcast_advance_local on every advance, so the
// common case is "no pending rows" and the loop costs one indexed
// SELECT.
func broadcast_pending_drain_chain(db *DB, peer, key string) {
	if broadcast_pending_dispatch == nil {
		return
	}
	for i := 0; i < broadcast_pending_max; i++ {
		last := broadcast_received_get(db, peer, key)
		row := broadcast_pending_next(db, peer, key, last+1)
		if row == nil {
			return
		}
		if !broadcast_pending_dispatch(row, db) {
			return
		}
		// Dispatch succeeded; advance via the simple-path helper
		// (NOT broadcast_advance_local) so we don't re-enter this
		// drain loop on every iteration.
		broadcast_advance_local_simple(db, peer, key, row.Sequence)
		broadcast_pending_delete(db, peer, key, row.Sequence)
	}
}

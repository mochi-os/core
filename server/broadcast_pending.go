// Mochi server: broadcast pending buffer
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.
//
// Per-app `pending` table that buffers out-of-order
// broadcast events on the subscriber side. Replaces the previous
// "NACK on gap" behaviour with "buffer + drain in chain order",
// mirroring the per-row replication pending pattern in replication.go.
//
// Why: the queue.go per-(target, from_entity) bucket has cap=1 for
// FK ordering on sql/op replication. Broadcast events were caught in
// the same restriction even though their ordering is enforced
// receiver-side by sequence. With this buffer, the sender can blast
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

import (
	"os"
	"path/filepath"
	"strconv"
)

const broadcast_pending_max = 1000

// broadcast_pending_gc_default_ttl_days is the default age above which
// a stuck-stream gap gets skipped. Tuned to broadcast_log_age (7 days)
// - if the sender pruned the gap from its log, no amount of patience
// will fill it. Overridable via setting
// `broadcast.pending.unfillable_ttl_days`.
const broadcast_pending_gc_default_ttl_days = 7

// broadcast_pending_gc_period_seconds is how often broadcast_manager
// wakes to run the GC pass. Hourly - the TTL is days, no point
// checking more often than the staleness signal moves.
const broadcast_pending_gc_period_seconds = 60 * 60

// broadcast_pending_table_create lazily creates the table; the call
// is idempotent and the schema matches the comment block above.
func broadcast_pending_table_create(db *DB) {
	db.exec(`create table if not exists pending (
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
	exists, _ := db.exists("select 1 from sqlite_master where type='table' and name='pending'")
	if !exists {
		return 0
	}
	return db.integer("select count(*) from pending where peer=? and key=?", peer, key)
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
	// Plain db.exec - pending is receiver-side
	// apply-buffer state and each paired host must track its own.
	// Pair-replicating the buffer would cross-pollute drain
	// expectations between hosts that have applied different subsets
	// of their inbound streams. See task #91 for the related bug on
	// the received table.
	db.exec(`insert or ignore into pending
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
	exists, _ := db.exists("select 1 from sqlite_master where type='table' and name='pending'")
	if !exists {
		return nil
	}
	var row broadcast_pending_row
	if !db.scan(&row, "select * from pending where peer=? and key=? and sequence=?", peer, key, sequence) {
		return nil
	}
	return &row
}

// broadcast_pending_delete removes one drained row. Caller deletes
// only after the handler ran successfully and received was advanced.
// Plain db.exec - see broadcast_pending_insert above for why
// receiver-side buffer state stays host-local.
func broadcast_pending_delete(db *DB, peer, key string, sequence int64) {
	db.exec("delete from pending where peer=? and key=? and sequence=?", peer, key, sequence)
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
// (peer, key) stream starting at the current received.last+1.
// Each row that dispatches cleanly is removed, received advances,
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

// --- pending GC for unfillable gaps (task #101) -------------------
//
// Receivers can deadlock when the pending buffer fills with high-seq
// rows that won't apply (gap below them) and resync replies for the
// gap can't fit (buffer full). Sender log entries below the gap get
// pruned at broadcast_log_age (7 days), so resync can never fill the
// missing seqs no matter how patient the wait. Result: the stream
// sits forever at received.last, every new arrival drops or NACKs
// pending-full, every retry repeats the same failure.
//
// Same shape as replication_pending_gc (#68): walk the stuck streams,
// for any stuck longer than the TTL skip the gap by advancing
// received.last to just before the lowest pending sequence, then drain
// the chain that's now contiguous. Audit-log every skip. Lose events
// in the gap - acceptable: alternative is permanent loss of everything
// past the gap, which is worse.

// BroadcastStalledStream is one (user, app, peer, key) stream whose
// pending buffer cannot drain. Streams that would drain on the next
// contiguous arrival are excluded by the classifier.
type BroadcastStalledStream struct {
	User       string
	App        string
	DBPath     string
	Peer       string
	Key        string
	Last       int64 // received.last
	MinPending int64 // min(pending.sequence)
	Count      int64
	Oldest     int64 // min(pending.received), unix seconds
}

// broadcast_pending_stalled walks per-app system DBs and returns
// streams whose pending buffer has rows below the in-buffer minimum
// AND received.last hasn't reached the contiguous chain start.
// Mirrors broadcast_lag_scan's walk pattern (task #83). Walks
// users/<uid>/<app-id>/app.db; the broadcast tables live in the per-
// app system DB (task #90). Apps that don't use broadcasts have no
// `pending` table - skipped silently as the common case.
func broadcast_pending_stalled() []BroadcastStalledStream {
	var out []BroadcastStalledStream
	users_root := filepath.Join(data_dir, "users")
	users, err := os.ReadDir(users_root)
	if err != nil {
		return out
	}
	for _, u := range users {
		if !u.IsDir() {
			continue
		}
		user := u.Name()
		user_dir := filepath.Join(users_root, user)
		apps, err := os.ReadDir(user_dir)
		if err != nil {
			continue
		}
		for _, a := range apps {
			if !a.IsDir() {
				continue
			}
			app := a.Name()
			path := filepath.Join("users", user, app, "app.db")
			abs := filepath.Join(data_dir, path)
			if !file_exists(abs) {
				continue
			}
			out = append(out, broadcast_pending_stalled_db(user, app, path)...)
		}
	}
	return out
}

// broadcast_pending_stalled_db classifies one app DB's streams. A
// stream qualifies as stalled when its lowest RELEVANT buffered
// sequence is greater than received.last+1 - the next applyable
// sequence is missing entirely (neither buffered nor yet arrived) and
// the buffer can't drain until something fills it.
//
// The "relevant" filter (sequence > received.last) excludes orphan
// stale entries from old-buggy-code-era inserts. Without it, an
// orphan at sequence=11 with received.last=866 hides a genuinely
// stuck stream with min(relevant)=1310; the classifier would see
// min(sequence)=11 <= 867 and falsely report "would drain naturally."
func broadcast_pending_stalled_db(user, app, db_path string) []BroadcastStalledStream {
	var out []BroadcastStalledStream
	db := db_open(db_path)
	if db == nil {
		return out
	}
	has_pending, _ := db.exists("select 1 from sqlite_master where type='table' and name='pending'")
	if !has_pending {
		return out
	}
	has_received, _ := db.exists("select 1 from sqlite_master where type='table' and name='received'")
	// LEFT JOIN against received (coalesced to 0 if absent or no row
	// for this peer/key) lets us filter pending to entries above the
	// cursor in a single query. has_received determines whether the
	// JOIN target exists at all.
	var rows []map[string]any
	var err error
	if has_received {
		rows, err = db.rows(`select p.peer, p.key,
			count(*) as count,
			min(p.sequence) as min_seq,
			min(p.received) as oldest,
			coalesce(r.last, 0) as last
			from pending p
			left join received r on r.sender = p.peer and r.key = p.key
			where p.sequence > coalesce(r.last, 0)
			group by p.peer, p.key, coalesce(r.last, 0)`)
	} else {
		// No received table - treat every pending entry as above-cursor
		// (the receiver has never advanced, so any in-buffer seq is
		// relevant). Same shape so the loop below works uniformly.
		rows, err = db.rows(`select peer, key,
			count(*) as count,
			min(sequence) as min_seq,
			min(received) as oldest,
			0 as last
			from pending group by peer, key`)
	}
	if err != nil {
		return out
	}
	for _, r := range rows {
		peer, _ := r["peer"].(string)
		key, _ := r["key"].(string)
		count, _ := r["count"].(int64)
		min_seq, _ := r["min_seq"].(int64)
		oldest, _ := r["oldest"].(int64)
		last, _ := r["last"].(int64)
		// Drains naturally on the next arrival of received.last+1.
		if min_seq <= last+1 {
			continue
		}
		out = append(out, BroadcastStalledStream{
			User:       user,
			App:        app,
			DBPath:     db_path,
			Peer:       peer,
			Key:        key,
			Last:       last,
			MinPending: min_seq,
			Count:      count,
			Oldest:     oldest,
		})
	}
	return out
}

// broadcast_pending_gc skips the unfillable gap on every stalled
// stream whose pending buffer has been stuck longer than the TTL. The
// skip advances received.last to min(pending.sequence)-1; the standard
// chain-drain in broadcast_advance_local then picks up from there,
// applying every buffered chain link until it hits the next missing
// sequence or empties the buffer. Returns the number of gaps skipped
// (not sequences lost). Safe to call on demand (admin endpoint) as
// well as from broadcast_manager.
//
// force=true bypasses the TTL gate entirely - every classified-as-
// stalled stream gets its gap skipped right now. Operator opt-in only:
// the admin endpoint accepts ?force=true; the background manager
// always calls with force=false so it respects the configured window.
func broadcast_pending_gc(force bool) int {
	ttl_days := int64(broadcast_pending_gc_default_ttl_days)
	if s := setting_get("broadcast.pending.unfillable_ttl_days", ""); s != "" {
		if v, err := strconv.ParseInt(s, 10, 64); err == nil && v > 0 {
			ttl_days = v
		}
	}
	cutoff := now() - ttl_days*86400
	stalled := broadcast_pending_stalled()
	if len(stalled) == 0 {
		return 0
	}
	skipped := 0
	for _, s := range stalled {
		if !force && s.Oldest >= cutoff {
			// Still within the operator window; defer.
			continue
		}
		// Resolve user + app so broadcast_advance_local has the
		// context it needs (the pending dispatch path runs handlers
		// against the app's data DB, which requires both).
		u := user_by_uid(s.User)
		if u == nil {
			info("Broadcast pending GC: skipping orphan user=%q app=%q (user not found)", s.User, s.App)
			continue
		}
		a := app_by_id(s.App)
		if a == nil {
			info("Broadcast pending GC: skipping orphan user=%q app=%q (app not found)", s.User, s.App)
			continue
		}
		sysdb := db_app_system(u, a)
		if sysdb == nil {
			continue
		}
		skip_to := s.MinPending - 1
		gap_size := skip_to - s.Last
		if gap_size <= 0 {
			continue
		}
		broadcast_advance_local(sysdb, s.Peer, s.Key, skip_to)
		// Sweep any orphan pending rows below the new cursor. The
		// chain-drain only deletes rows it actually dispatched; rows
		// that were never re-dispatched (left over from older buggy
		// code paths) survive and distort the next GC pass's classifier
		// (the bug that caused the wasabi-self feeds stream to escape
		// detection on the first force-skip attempt).
		new_last := broadcast_received_get(sysdb, s.Peer, s.Key)
		sysdb.exec("delete from pending where peer=? and key=? and sequence<=?",
			s.Peer, s.Key, new_last)
		info("Broadcast pending GC: skipped gap user=%q app=%q peer=%q key=%q from_seq=%d to_seq=%d gap=%d age=%ds",
			s.User, s.App, s.Peer, s.Key, s.Last+1, skip_to, gap_size, now()-s.Oldest)
		audit_broadcast_pending_purged(s.User, s.App, s.Peer, s.Key, s.Last+1, skip_to, gap_size)

		// Tell the subscribing app it permanently lost events on this
		// stream, so it can do a full state re-fetch — broadcast/resync
		// can't fill a gap whose sequences are pruned from the owner's log.
		// entity = the stream key (the source entity). Best-effort,
		// host-local; no-op if the app declares no broadcast/gap handler.
		svc := ""
		if svcs := app_services(a, u); len(svcs) > 0 {
			svc = svcs[0]
		}
		peer, key, first, last := s.Peer, s.Key, s.Last+1, skip_to
		error_dispatch(u, a, error_code_broadcast_gap, "unfillable", svc, key, nil, func() map[string]any {
			return map[string]any{"peer": peer, "key": key, "first": first, "last": last}
		})
		skipped++
	}
	if skipped > 0 {
		info("Broadcast pending GC: skipped %d unfillable gap(s) older than %d days", skipped, ttl_days)
	}
	return skipped
}

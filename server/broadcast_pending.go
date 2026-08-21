// Mochi server: broadcast pending buffer
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.
//
// Per-app `pending` table buffering out-of-order broadcast events on the
// subscriber side, drained in chain order. Bounded per (peer, key) at
// broadcast_pending_maximum; inserts above the cap are dropped and the
// subscriber's resync request re-fetches them.

package main

import (
	"os"
	"path/filepath"
	"strconv"
)

const broadcast_pending_maximum = 1000

// broadcast_pending_streams_maximum bounds how many DISTINCT streams one peer
// may hold buffered for a user and app. broadcast_pending_maximum is per (peer,
// key), and the key is unsigned and buffered before any app handler runs, so a
// peer inventing a key per event draws a fresh budget every time.
const broadcast_pending_streams_maximum = 1000

// broadcast_pending_gc_default_ttl_days is the age above which a stuck-stream
// gap is skipped, the backstop for streams with no inbound traffic to trigger a
// resync floor skip. Overridable via `broadcast.pending.unfillable_ttl_days`.
const broadcast_pending_gc_default_ttl_days = 2

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
// stream cap and by the operator visibility surface.
func broadcast_pending_count(db *DB, peer, key string) int {
	exists, _ := db.exists("select 1 from sqlite_master where type='table' and name='pending'")
	if !exists {
		return 0
	}
	return db.integer("select count(*) from pending where peer=? and key=?", peer, key)
}

// broadcast_pending_streams counts the distinct streams this peer holds
// buffered. Served by the (peer, key, sequence) primary key.
func broadcast_pending_streams(db *DB, peer string) int {
	exists, _ := db.exists("select 1 from sqlite_master where type='table' and name='pending'")
	if !exists {
		return 0
	}
	return db.integer("select count(distinct key) from pending where peer=?", peer)
}

// broadcast_pending_insert buffers one out-of-order event, returning false if
// the per-stream cap dropped it. Either way the caller still fires the resync
// request - the buffer is an optimisation, not the gap-fill mechanism.
func broadcast_pending_insert(db *DB, peer, key string, sequence int64, source, target, service, event, message, sender_app, sender_services string, content []byte) bool {
	broadcast_pending_table_create(db)
	count := broadcast_pending_count(db, peer, key)
	if count >= broadcast_pending_maximum {
		debug("Broadcast pending dropping seq=%d for (peer=%s, key=%s): per-stream buffer full at %d", sequence, peer, key, broadcast_pending_maximum)
		return false
	}
	// Only when opening a stream, so an established one never pays for the
	// second query.
	if count == 0 && broadcast_pending_streams(db, peer) >= broadcast_pending_streams_maximum {
		debug("Broadcast pending dropping seq=%d for (peer=%s, key=%s): peer already holds %d buffered streams", sequence, peer, key, broadcast_pending_streams_maximum)
		return false
	}
	// pending is receiver-side apply-buffer state: it holds what THIS host has
	// received but cannot yet apply in order.
	db.exec(`insert or ignore into pending
		(peer, key, sequence, source, target, service, event, msg_id, sender_app, sender_services, content, received)
		values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		peer, key, sequence, source, target, service, event, message, sender_app, sender_services, content, now())
	return true
}

// broadcast_pending_row holds one buffered event's identity + payload.
// All scalar columns plus the CBOR-encoded content blob.
type broadcast_pending_row struct {
	Peer           string `db:"peer"`
	Key            string `db:"key"`
	Sequence       int64  `db:"sequence"`
	From           string `db:"source"`
	To             string `db:"target"`
	Service        string `db:"service"`
	Event          string `db:"event"`
	Message        string `db:"msg_id"`
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

// broadcast_pending_dispatch re-runs a buffered event's handler. events.go sets
// it in init(), so this file does not depend on the routing graph. True means
// applied (delete + advance); false stops the drain and leaves the row.
var broadcast_pending_dispatch func(row *broadcast_pending_row, db *DB) bool

// broadcast_pending_drain_chain applies buffered rows for one (peer, key)
// stream from received.last+1, stopping at the first missing link or dispatch
// failure.
func broadcast_pending_drain_chain(db *DB, peer, key string) {
	if broadcast_pending_dispatch == nil {
		return
	}
	for i := 0; i < broadcast_pending_maximum; i++ {
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

// --- pending GC for unfillable gaps -------------------------------
//
// A stream deadlocks when the sequences below its buffered rows are pruned from
// the sender's log: no wait can fill them. The GC skips such a gap by advancing
// received.last to just before the lowest pending sequence, then drains what is
// now contiguous. Losing the gap's events beats losing everything past it.

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

// broadcast_pending_stalled walks users/<uid>/<app>/app.db and returns streams
// whose pending buffer cannot drain. Apps that never broadcast have no
// `pending` table and are skipped silently.
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

// broadcast_pending_stalled_db classifies one app DB's streams: stalled when
// the lowest buffered sequence ABOVE received.last is greater than last+1. The
// filter matters - an orphan row below the cursor would make a genuinely stuck
// stream look like it drains naturally.
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
		minimum_sequence, _ := r["min_seq"].(int64)
		oldest, _ := r["oldest"].(int64)
		last, _ := r["last"].(int64)
		// Drains naturally on the next arrival of received.last+1.
		if minimum_sequence <= last+1 {
			continue
		}
		out = append(out, BroadcastStalledStream{
			User:       user,
			App:        app,
			DBPath:     db_path,
			Peer:       peer,
			Key:        key,
			Last:       last,
			MinPending: minimum_sequence,
			Count:      count,
			Oldest:     oldest,
		})
	}
	return out
}

// broadcast_pending_skip_stream loops skip + drain until the stream has no
// TTL-old unfillable hole left, returning the final received.last. A sparse
// buffer against a large gap means thousands of holes, so skipping one per
// hourly pass never converges. force bypasses the per-hole age gate.
func broadcast_pending_skip_stream(sysdb *DB, user, app, peer, key string, start, cutoff int64, force bool) int64 {
	last := start
	for iterations := 0; iterations < 10000; iterations++ {
		row, _ := sysdb.row("select min(sequence) as low, min(received) as oldest from pending where peer=? and key=? and sequence > ?", peer, key, last)
		if row == nil {
			break
		}
		low, ok := row["low"].(int64)
		if !ok || low == 0 {
			break // buffer empty above the cursor
		}
		if low <= last+1 {
			break // drains naturally on the next arrival
		}
		if oldest, ok := row["oldest"].(int64); !force && (!ok || oldest >= cutoff) {
			break // younger holes wait out their own TTL
		}
		skip_to := low - 1
		broadcast_advance_local(sysdb, peer, key, skip_to)
		// Sweep orphan pending rows below the new cursor: the chain-drain only
		// deletes rows it dispatched, and the survivors distort the next GC pass's
		// classifier.
		new_last := broadcast_received_get(sysdb, peer, key)
		sysdb.exec("delete from pending where peer=? and key=? and sequence<=?",
			peer, key, new_last)
		audit_broadcast_pending_purged(user, app, peer, key, last+1, skip_to, skip_to-last)
		if new_last <= last {
			break // no forward progress; avoid spinning
		}
		last = new_last
	}
	return last
}

// broadcast_pending_gc skips the unfillable gap on every stalled stream stuck
// longer than the TTL, advancing received.last to min(pending.sequence)-1 and
// letting the chain-drain take it from there. Returns gaps skipped, not
// sequences lost. force=true bypasses the TTL gate; operator opt-in only.
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
		last := broadcast_pending_skip_stream(sysdb, s.User, s.App, s.Peer, s.Key, s.Last, cutoff, force)
		if last <= s.Last {
			continue
		}
		info("Broadcast pending GC: skipped gap user=%q app=%q peer=%q key=%q from_seq=%d to_seq=%d age=%ds",
			s.User, s.App, s.Peer, s.Key, s.Last+1, last, now()-s.Oldest)
		broadcast_skip_warn(s.User, s.App, s.Peer, s.Key, s.Last+1, last)

		// Tell the subscribing app it permanently lost events so it can re-fetch;
		// resync cannot fill a gap pruned from the owner's log. Best-effort and
		// host-local; a no-op if the app declares no broadcast/gap handler.
		service := ""
		if svcs := app_services(a, u); len(svcs) > 0 {
			service = svcs[0]
		}
		peer, key, first, final := s.Peer, s.Key, s.Last+1, last
		error_dispatch(u, a, error_code_broadcast_gap, "unfillable", service, key, nil, func() map[string]any {
			return map[string]any{"peer": peer, "key": key, "first": first, "last": final}
		})
		skipped++
	}
	if skipped > 0 {
		info("Broadcast pending GC: skipped %d unfillable gap(s) older than %d days", skipped, ttl_days)
	}
	return skipped
}

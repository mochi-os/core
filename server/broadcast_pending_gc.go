// Mochi server: broadcast pending GC for unfillable gaps
// Copyright Alistair Cunningham 2026
//
// Receivers can deadlock when the pending buffer fills with high-seq
// rows that won't apply (gap below them) and resync replies for the
// gap can't fit (buffer full). The sender's log entries below the gap
// have been pruned at broadcast_log_age (7 days), so resync can never
// fill the missing seqs no matter how patient the wait. Result: the
// stream sits forever at received.last, every new arrival drops or
// NACKs pending-full, every retry repeats the same failure.
//
// This file is the analogue of replication_pending_gc (task #68) for
// the broadcast subsystem. Same shape: walk the stuck streams, for any
// stuck longer than the TTL skip the gap by advancing received.last to
// just before the lowest pending sequence, then drain the chain that's
// now contiguous. Audit-log every skip with how many sequences were
// lost. Acceptable tradeoff: the alternative is permanent loss of
// everything past the gap, which is worse.

package main

import (
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// broadcast_pending_gc_default_ttl_days is the default age above which
// a stuck-stream gap gets skipped. Tuned to broadcast_log_age (7
// days) - if the sender pruned the gap from its log, no amount of
// patience will fill it. Overridable via
// `broadcast.pending.unfillable_ttl_days`.
const broadcast_pending_gc_default_ttl_days = 7

// broadcast_pending_gc_period_seconds is how often broadcast_manager
// wakes to run the GC pass. Hourly - the TTL is measured in days, no
// point checking more often than the staleness signal moves.
const broadcast_pending_gc_period_seconds = 60 * 60

// BroadcastStalledStream is one (user, app, peer, key) stream whose
// pending buffer cannot drain. Returned by broadcast_pending_stalled
// and consumed by broadcast_pending_gc. Streams that would drain on
// the next contiguous arrival are excluded by the classifier.
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
// AND received.last hasn't reached the contiguous chain start. Mirrors
// broadcast_lag_scan's walk pattern (task #83).
//
// Walks users/<uid>/<app-id>/app.db; the broadcast tables live in the
// per-app system DB (task #90 architectural fix). Apps that don't use
// broadcasts have no `pending` table - skipped silently as the common
// case.
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
// stream qualifies as stalled when its lowest buffered sequence is
// greater than received.last+1 - the next applyable sequence is
// missing entirely (neither buffered nor yet arrived) and the buffer
// can't drain until something fills it.
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
	rows, err := db.rows(
		"select peer, key, count(*) as count, min(sequence) as min_seq, min(received) as oldest from pending group by peer, key")
	if err != nil {
		return out
	}
	for _, r := range rows {
		peer, _ := r["peer"].(string)
		key, _ := r["key"].(string)
		count, _ := r["count"].(int64)
		min_seq, _ := r["min_seq"].(int64)
		oldest, _ := r["oldest"].(int64)
		var last int64
		if has_received {
			received_row, _ := db.row("select last from received where sender=? and key=?", peer, key)
			if received_row != nil {
				last, _ = received_row["last"].(int64)
			}
		}
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
// sequence or empties the buffer.
//
// Returns the number of gaps skipped (not the number of sequences
// lost). Safe to call on demand (admin endpoint) as well as from the
// background manager.
func broadcast_pending_gc() int {
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
		if s.Oldest >= cutoff {
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
		info("Broadcast pending GC: skipped gap user=%q app=%q peer=%q key=%q from_seq=%d to_seq=%d gap=%d age=%ds",
			s.User, s.App, s.Peer, s.Key, s.Last+1, skip_to, gap_size, now()-s.Oldest)
		audit_broadcast_pending_purged(s.User, s.App, s.Peer, s.Key, s.Last+1, skip_to, gap_size)
		skipped++
	}
	if skipped > 0 {
		info("Broadcast pending GC: skipped %d unfillable gap(s) older than %d days", skipped, ttl_days)
	}
	return skipped
}

// broadcast_manager runs the periodic GC. Hourly cadence matches
// replication's GC interval: the TTL is days, so a tighter loop just
// burns CPU on the per-app DB walk without any operational benefit.
func broadcast_manager() {
	for range time.Tick(time.Duration(broadcast_pending_gc_period_seconds) * time.Second) {
		broadcast_pending_gc()
	}
}

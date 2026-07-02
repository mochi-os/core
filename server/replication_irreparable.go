// Mochi server: irreparable replication detection + dual-side notification.
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// A replication stream broken (stalled on an unfillable gap, or a member
// gone) for longer than T_forget can no longer be recovered without data
// loss: the predecessor ops are past retention on the sender, and a
// snapshot-over would silently discard any divergent writes on this side.
// Past that line the stream is IRREPARABLE - a terminal state only an
// operator can resolve (remove the relationship, or re-bootstrap accepting
// the loss). We deliberately do NOT auto-repair, because after T_forget
// there is no lossless repair to perform.
//
// When a stream crosses the line we record it in replication.db.irreparable
// (so we neither re-notify nor keep warning every tick) and notify the
// administrators on BOTH sides: the local admins / owning user via a Mochi
// notification, and the peer via a replica/irreparable event so its admins
// learn of it too - as long as that machine is still up. The local side is
// leader-gated so a multi-host set notifies once; the optimistic claim lets
// a sole survivor self-elect when its only co-member is the dead peer.

// irreparable_threshold is the broken-duration past which a stalled stream
// is declared irreparable. Tied to replication_op_retention so it can never
// be shorter than the window in which a returning peer's ops are still
// replayable: only once the ops are genuinely gone is the stream truly
// unrecoverable.
const irreparable_threshold = replication_op_retention

// offline_threshold is how long a replication member may be unreachable
// before its admins / owning user get a soft "offline" notification - the
// early heads-up that sits well below the 30-day irreparable line, after
// routine reboots / deploys / maintenance windows have had time to recover.
// The Pair / My-hosts pages surface the offline badge sooner (from
// unreachable.since directly); only the notification waits this long.
const offline_threshold = 24 * 3600

// replication_irreparable_scan reconciles the irreparable marker table
// against the currently-stalled streams, then fires the dual-side
// notification for any newly-marked stream. Called from the replication
// manager on the same slow cadence as the pending GC.
func replication_irreparable_scan() {
	db := db_open("db/replication.db")

	// Mark any stream stalled past T_forget. Run before the pending GC
	// (the caller orders it so) because the GC purges the aged pending
	// rows that prove the gap - once they are gone the stream drops out
	// of replication_pending_stalled and there is nothing left to detect.
	// A marker, once set, is NOT auto-cleared from the stalled set: past
	// T_forget the only recovery is an operator re-bootstrap, which clears
	// it via bootstrap_set_state(done); a natural drain can't happen
	// because the predecessor ops are gone from the sender too.
	cutoff := now() - irreparable_threshold
	for _, s := range replication_pending_stalled() {
		if s.Oldest > cutoff {
			continue // broken, but not yet past T_forget
		}
		// Stand down while a bootstrap from this peer is in flight: a large-DB
		// re-bootstrap takes hours, during which the old pending rows still look
		// stalled. Marking irreparable + notifying here would raise a false alarm
		// on a recovery that is actively landing (the same guard the wiped-replica
		// re-bootstrap added after the yuzu 2026-06-15 false alarm) (#147).
		if bootstrap_in_progress(s.Peer) {
			continue
		}
		exists, _ := db.exists("select 1 from irreparable where peer=? and scope=? and user=? and db=?",
			s.Peer, s.Scope, s.User, s.Database)
		if !exists {
			db.exec("insert into irreparable (peer, scope, user, db, reason, since, notified) values (?, ?, ?, ?, 'stalled', ?, 0)",
				s.Peer, s.Scope, s.User, s.Database, now())
			warn("Replication stream irreparable: peer=%q scope=%q user=%q db=%q age=%ds (broken past T_forget; operator action required)",
				s.Peer, s.Scope, s.User, s.Database, now()-s.Oldest)
		}
	}

	// Offline members: a peer whose Sender has been unreachable past
	// T_forget, for every replication relationship it participates in
	// (whole-server pair and/or per-user host). Unlike a stalled stream
	// this has no buffered pending - the member simply went silent - so
	// it's detected from the persisted unreachable mark instead. db is ''
	// (the whole relationship, not one stream), so these never collide
	// with the per-stream stalled rows above.
	gone, _ := db.rows("select peer from unreachable where since < ?", now()-irreparable_threshold)
	for _, g := range gone {
		peer, _ := g["peer"].(string)
		if peer == "" {
			continue
		}
		if paired, _ := db.exists("select 1 from pair where peer=?", peer); paired {
			replication_irreparable_offline_mark(db, peer, repl_scope_core, "")
		}
		users, _ := db.rows("select distinct user from hosts where peer=?", peer)
		for _, u := range users {
			user, _ := u["user"].(string)
			replication_irreparable_offline_mark(db, peer, repl_scope_app, user)
		}
	}

	// Notify both sides once per newly-marked stream.
	pending, _ := db.rows("select peer, scope, user, db, reason from irreparable where notified=0")
	for _, r := range pending {
		peer, _ := r["peer"].(string)
		scope, _ := r["scope"].(string)
		user, _ := r["user"].(string)
		database, _ := r["db"].(string)
		reason, _ := r["reason"].(string)
		if !replication_notify_leader(scope, user) {
			continue // a co-host holds the lease and will notify
		}
		urgent := replication_irreparable_last_copy(db, scope, user, peer)
		replication_irreparable_notify_local(scope, user, urgent)
		replication_irreparable_notify_remote(peer, scope, user, database, reason)
		db.exec("update irreparable set notified=1 where peer=? and scope=? and user=? and db=?",
			peer, scope, user, database)
	}
}

// replication_irreparable_offline_mark records a relationship as irreparable
// because its peer has been unreachable past T_forget (reason "offline",
// db='' for the whole relationship). Idempotent; the scan fires the
// notification for notified=0 rows.
func replication_irreparable_offline_mark(db *DB, peer, scope, user string) {
	exists, _ := db.exists("select 1 from irreparable where peer=? and scope=? and user=? and db=''", peer, scope, user)
	if exists {
		return
	}
	db.exec("insert into irreparable (peer, scope, user, db, reason, since, notified) values (?, ?, ?, '', 'offline', ?, 0)",
		peer, scope, user, now())
	warn("Replication member irreparable (offline): peer=%q scope=%q user=%q (unreachable past T_forget; operator action required)",
		peer, scope, user)
}

// peer_is_replication_member reports whether this peer participates in any
// replication relationship (whole-server pair or per-user host) - the only
// peers whose unreachability is worth persisting for the offline-irreparable
// detector. Gating the unreachable stamp on this keeps the table from
// accumulating rows for transient message recipients we happen never to ack
// again.
func peer_is_replication_member(id string) bool {
	if id == "" || id == net_id {
		return false
	}
	db := db_open("db/replication.db")
	if ok, _ := db.exists("select 1 from pair where peer=?", id); ok {
		return true
	}
	ok, _ := db.exists("select 1 from hosts where peer=?", id)
	return ok
}

// replication_member_unreachable persists the "unreachable since" mark for a
// replication member that has gone away - at the libp2p level (peer_disconnected,
// the event-driven signal that fires even for an idle member with no traffic)
// or whose sends keep failing (peer_mark_no_progress inflight timeouts /
// peer_mark_send_failed connect failures). INSERT OR IGNORE keeps the original
// timestamp across later signals. Non-members are ignored so the table stays
// scoped to replication relationships.
func replication_member_unreachable(id string) {
	if !peer_is_replication_member(id) {
		return
	}
	db_open("db/replication.db").exec("insert or ignore into unreachable (peer, since) values (?, ?)", id, now())
}

// replication_member_reachable clears the unreachable mark and any
// offline-irreparable marker for a peer that is reachable again - an ack
// arrived (peer_mark_progress) or libp2p reconnected (peer_reconnected).
// Existence-gated so the common healthy peer pays only a cheap read. The
// offline condition is resolved by reachability; a residual data gap, if any,
// re-surfaces as a stalled stream, so the stalled irreparable reason is NOT
// cleared here - a gap survives a reconnect.
func replication_member_reachable(id string) {
	if id == "" {
		return
	}
	db := db_open("db/replication.db")
	if seen, _ := db.exists("select 1 from unreachable where peer=?", id); seen {
		db.exec("delete from unreachable where peer=?", id)
		db.exec("delete from irreparable where peer=? and reason='offline'", id)
		// A source that just came back is the moment to re-drive any
		// bootstrap scope that stalled while it was gone. Clearing the
		// per-row backoff (attempts=0) and progress timestamp makes every
		// non-done scope for this peer immediately eligible on the retry
		// driver's next tick — instead of waiting out its exponential
		// backoff window. We only reach here on an unreachable→reachable
		// transition, so any 'active' row was genuinely stalled (its peer
		// was gone), making the re-drive correct rather than disruptive.
		// A plain UPDATE here (rather than calling bootstrap_resume_peer)
		// keeps the libp2p connect callback off the manifest-fetch code
		// path — both for speed and to avoid a package initialization
		// cycle through the stubbable fetch vars.
		db.exec("update bootstrap set attempts=0, progress=0 where peer=? and state != 'done'", id)
	}
}

// replication_irreparable_clear removes any irreparable markers for a peer
// (optionally narrowed to one scope). Called when an operator re-bootstraps
// the relationship (the only recovery past T_forget) or removes it
// entirely, so the terminal state and its UI badge clear cleanly.
func replication_irreparable_clear(peer, scope string) {
	db := db_open("db/replication.db")
	if scope == "" {
		db.exec("delete from irreparable where peer=?", peer)
		return
	}
	db.exec("delete from irreparable where peer=? and scope=?", peer, scope)
}

// replication_notify_leader gates a replication notification so a multi-host
// set notifies once. Core (whole-server) relationships elect among the pair
// members; app (per-user) relationships elect among that user's hosts. The
// claim is optimistic, so a sole survivor whose only co-member is the dead
// peer still wins and fires. Shared by the irreparable and offline scans.
func replication_notify_leader(scope, user string) bool {
	leader_scope := "platform"
	if scope == repl_scope_app && user != "" {
		leader_scope = "user:" + user
	}
	return replication_leader_claim(leader_scope, "notify", false)
}

// replication_irreparable_last_copy reports whether the irreparable peer held
// the LAST other copy of this data - i.e. no other healthy host/pair-member
// still has it, so losing this one leaves the data single-copy (only here).
// Drives the "no redundancy" urgency in the notification. Core scope counts
// other pair members; app scope counts the user's other hosts.
func replication_irreparable_last_copy(db *DB, scope, user, peer string) bool {
	var others []map[string]any
	if scope == repl_scope_app && user != "" {
		others, _ = db.rows("select peer from hosts where user=? and peer!=?", user, peer)
	} else {
		others, _ = db.rows("select peer from pair where peer!=?", peer)
	}
	for _, r := range others {
		other, _ := r["peer"].(string)
		if other == "" {
			continue
		}
		// A co-member that is itself irreparable doesn't count as a copy.
		bad, _ := db.exists("select 1 from irreparable where peer=? and (scope=? or (scope=? and user=?))",
			other, repl_scope_core, repl_scope_app, user)
		if !bad {
			return false // at least one other healthy copy survives
		}
	}
	return true // no other healthy copy - losing this peer leaves no redundancy
}

// replication_irreparable_count returns how many relationships are currently
// marked irreparable. Surfaced by /_/health as a degraded replication signal
// (without flipping liveness - a dead peer is not a dead server).
func replication_irreparable_count() int {
	return db_open("db/replication.db").integer("select count(*) from irreparable")
}

// Cached snapshot of the irreparable table, refreshed at most every
// irreparable_emit_cache_seconds, so the emit hot path filters out dead peers
// with an in-memory check instead of a per-write query. The common case (no
// irreparable rows) is an empty slice and a no-op loop.
const irreparable_emit_cache_seconds = 30

type irreparableEntry struct{ peer, scope, user string }

var (
	irreparable_snapshot      []irreparableEntry
	irreparable_snapshot_at   int64
	irreparable_snapshot_lock sync.Mutex
)

// irreparable_emit_skip reports whether this user's ops should be withheld
// from `peer` because that relationship is irreparable - emitting to it is
// wasted churn until an operator re-bootstraps. Skips on a whole-server (core)
// marker for the peer, or a per-user (app) marker for this user.
func irreparable_emit_skip(user, peer string) bool {
	irreparable_snapshot_lock.Lock()
	if now()-irreparable_snapshot_at > irreparable_emit_cache_seconds {
		rows, _ := db_open("db/replication.db").rows("select peer, scope, user from irreparable")
		snap := make([]irreparableEntry, 0, len(rows))
		for _, r := range rows {
			p, _ := r["peer"].(string)
			s, _ := r["scope"].(string)
			u, _ := r["user"].(string)
			snap = append(snap, irreparableEntry{p, s, u})
		}
		irreparable_snapshot = snap
		irreparable_snapshot_at = now()
	}
	snap := irreparable_snapshot
	irreparable_snapshot_lock.Unlock()

	for _, e := range snap {
		if e.peer != peer {
			continue
		}
		if e.scope == repl_scope_core {
			return true
		}
		if e.scope == repl_scope_app && e.user == user {
			return true
		}
	}
	return false
}

// rebootstrap_unanchored_seconds is how long an UNANCHORED stalled stream (no
// cursor - the receiver is missing the whole stream, i.e. wiped/fresh) must
// persist before we auto-request a re-bootstrap from the source. Long enough
// that ordinary out-of-order delivery has resolved, far short of the 30-day
// irreparable floor. Anchored gaps (existing state, possible divergence) are
// never auto-rebootstrapped - those stay operator-driven.
const rebootstrap_unanchored_seconds = 10 * 60

// rebootstrap auto-recovery must converge, not fire every 30s forever. Per-
// (peer, user) attempt count + last-attempt time gate exponential backoff;
// after rebootstrap_attempt_cap futile pulls the relationship escalates to
// irreparable (operator re-join) instead of retrying for the full 30-day
// floor. In-memory: a restart resets the counters, which at worst means a
// fresh round of attempts.
type rebootstrap_state struct {
	attempts int
	last     int64
	gaveup   bool
}

var (
	rebootstrap_attempts = map[string]rebootstrap_state{}
	rebootstrap_mutex    sync.Mutex
)

const (
	rebootstrap_attempt_cap = 5
	rebootstrap_backoff_cap = 6 * 60 * 60 // seconds between attempts, capped
)

// replication_wiped_rebootstrap is the fast path for the 2026-06-02 incident:
// a wiped replica that receives ops it can never chain (no cursor for the
// stream) and would otherwise sit stalled for 30 days. When such a stream has
// been unanchored past rebootstrap_unanchored_seconds, the receiver requests a
// re-bootstrap from the source - safe because an unanchored stream has no
// local state to lose. Debounced on any in-flight bootstrap for the peer, and
// gated by the setting replication.rebootstrap.wiped (default on). Called from
// the replication manager on its 30s tick.
func replication_wiped_rebootstrap() {
	if setting_get("replication.rebootstrap.wiped", "true") == "false" {
		return
	}
	stalled := replication_pending_stalled()

	// Prune attempt state for (peer, user) pairs no longer stalled — a
	// stream that anchored (drained or re-seeded) starts fresh if it ever
	// stalls again.
	live := map[string]bool{}
	for _, s := range stalled {
		if s.User != "" {
			live[s.Peer+"|"+s.User] = true
		}
	}
	rebootstrap_mutex.Lock()
	for k := range rebootstrap_attempts {
		// Keys are "peer|user" (whole-user re-pull) or "peer|user|stream"
		// (targeted reseed, #33); keep a key while its peer|user is still
		// stalled so its backoff + attempt count survive between ticks.
		pu := k
		if parts := strings.SplitN(k, "|", 3); len(parts) >= 2 {
			pu = parts[0] + "|" + parts[1]
		}
		if !live[pu] {
			delete(rebootstrap_attempts, k)
		}
	}
	rebootstrap_mutex.Unlock()

	cutoff := now() - rebootstrap_unanchored_seconds
	for _, s := range stalled {
		if s.Anchored || s.User == "" || s.Oldest > cutoff {
			continue // anchored gap (divergence risk), non-user, or still settling
		}

		// Stand down while a bootstrap from this peer is still running. A
		// fresh join's bulk transfer AND its keys-transfer backfill (which is
		// what seeds system-row streams) are still landing, so a stream that
		// hasn't anchored YET is expected, not broken. Escalating "operator
		// re-join", or re-pulling, mid-join is a false alarm — the join IS the
		// re-seed. Once the bootstrap settles, a still-unanchored stream is a
		// genuine gap and the checks below fire. (Observed live 2026-06-15:
		// yuzu emailed "auto-recovery gave up … system:schedule … re-join"
		// 11 min into an in-progress pair-join while files/apps were still
		// transferring.) This also subsumes the old userdbs-only debounce.
		if bootstrap_in_progress(s.Peer) {
			continue
		}

		// System-row streams (system:users / sessions / schedule) are seeded
		// only by a keys-transfer at join time, never by the per-user file pull
		// below nor by a per-DB reseed — so neither can anchor them, and
		// retrying was the infinite loop this function used to be. Escalate
		// straight to irreparable: the operator's re-join re-sends the keys (and
		// Seeds). Checked ahead of the populated branch so it applies whether or
		// not the user has local data.
		if strings.HasPrefix(s.Database, repl_stream_class_system+":") {
			replication_rebootstrap_give_up(s, "system-row stream needs a keys-transfer re-seed (operator re-join)")
			continue
		}

		// SAFETY GATE (#27, 2026-06-24 SEV1) + RE-ANCHOR (#33): the whole-user
		// re-pull below re-fetches the user's DBs and OVERWRITES the local
		// copies. "Unanchored" means a broken CURSOR, not an empty DB — under
		// cursor misalignment a fully populated host looks unanchored, and
		// re-pulling it overwrote real data with the peer's (possibly
		// incomplete) copy: the 28-DB wipe on the live primary. So a populated
		// host is NEVER whole-user-re-pulled. Instead re-anchor THIS stream
		// non-destructively via the targeted reseed, gated per-DB on the source
		// being authoritative (not missing any op we originated). A source that
		// is behind us on a DB is genuine divergence, never overwritten — left
		// for operator/merge resolution (#22).
		if replication_user_has_local_data(s.User) {
			replication_reanchor_misaligned(s)
			continue
		}

		key := s.Peer + "|" + s.User
		rebootstrap_mutex.Lock()
		state := rebootstrap_attempts[key]
		if state.gaveup {
			rebootstrap_mutex.Unlock()
			continue
		}
		// Exponential backoff between attempts so a stream that won't anchor
		// (source down, FK-deferred parent never arrives) isn't re-pulled
		// every tick. The first fire is already gated by s.Oldest > cutoff.
		delay := int64(rebootstrap_unanchored_seconds) << uint(state.attempts)
		if delay > rebootstrap_backoff_cap {
			delay = rebootstrap_backoff_cap
		}
		if state.last != 0 && now()-state.last < delay {
			rebootstrap_mutex.Unlock()
			continue
		}
		// Cap: after N futile pulls, stop and escalate rather than retrying
		// for the full 30-day irreparable floor.
		if state.attempts >= rebootstrap_attempt_cap {
			state.gaveup = true
			rebootstrap_attempts[key] = state
			rebootstrap_mutex.Unlock()
			replication_rebootstrap_give_up(s, fmt.Sprintf("re-bootstrap did not anchor after %d attempts", state.attempts))
			continue
		}
		state.attempts++
		state.last = now()
		rebootstrap_attempts[key] = state
		rebootstrap_mutex.Unlock()

		info("Replication wiped-replica recovery: peer=%q user=%q db=%q unanchored %ds - re-bootstrap attempt %d/%d",
			s.Peer, s.User, s.Database, now()-s.Oldest, state.attempts, rebootstrap_attempt_cap)
		// The surgical per-user pull: re-fetches only this user's files +
		// DBs (apps come from apps_default at boot), so we never atomic-
		// rename whole-server apps/files out from under the running receiver.
		bootstrap_start_user(s.Peer, s.User)
	}
}

// replication_rebootstrap_give_up escalates a stream the auto-recovery can't
// fix to the irreparable table (operator action required), idempotently —
// one marker + one warning per stream until it's cleared, by a successful
// re-bootstrap (bootstrap_set_state → replication_irreparable_clear) or
// operator removal.
func replication_rebootstrap_give_up(s StalledStream, why string) {
	db := db_open("db/replication.db")
	if exists, _ := db.exists("select 1 from irreparable where peer=? and scope=? and user=? and db=?",
		s.Peer, s.Scope, s.User, s.Database); exists {
		return
	}
	db.exec("insert into irreparable (peer, scope, user, db, reason, since, notified) values (?, ?, ?, ?, 'stalled', ?, 0)",
		s.Peer, s.Scope, s.User, s.Database, now())
	warn("Replication auto-recovery gave up: peer=%q user=%q db=%q - %s", s.Peer, s.User, s.Database, why)
}

// reanchor_inflight tracks targeted reseeds the auto-recovery has launched but
// not yet finished, keyed by data-dir-relative DB path. It both dedups (one
// reseed per DB at a time) and caps total concurrency: a freshly-misaligned
// pair can have many stalled streams, and each reseed is a full snapshot fetch,
// so firing dozens at once would swamp the link and the rebuild memory. The
// memory-heavy rebuild is already bounded by bootstrap_rebuild_sem; this bounds
// the whole fetch.
var (
	reanchor_inflight   = map[string]bool{}
	reanchor_mutex      sync.Mutex
	reanchor_concurrent = 2
)

// reanchor_acquire reserves a reseed slot for rel, or returns false if rel is
// already reseeding or the concurrency cap is reached. Paired with
// reanchor_release in the launched goroutine's defer.
func reanchor_acquire(rel string) bool {
	reanchor_mutex.Lock()
	defer reanchor_mutex.Unlock()
	if reanchor_inflight[rel] || len(reanchor_inflight) >= reanchor_concurrent {
		return false
	}
	reanchor_inflight[rel] = true
	return true
}

func reanchor_release(rel string) {
	reanchor_mutex.Lock()
	delete(reanchor_inflight, rel)
	reanchor_mutex.Unlock()
}

// replication_reanchor_misaligned re-anchors a populated host's unanchored
// (cursor-misaligned) stream non-destructively — the safe alternative to the
// whole-user re-pull that wiped 28 DBs (#27, #33). For each DB file backing the
// stream it runs the targeted reseed (the same primitive as `mochictl
// replication reseed`), but ONLY where reseed_source_missing_ops reports the
// source is authoritative — not missing any op this host originated. A source
// that is behind us on a DB is genuine bidirectional divergence, not a
// misalignment; overwriting it would lose local-origin rows, so it is left for
// operator/merge resolution (#22). Paced by the shared per-stream backoff +
// attempt cap (key "peer|user|stream"); after the cap it escalates to
// irreparable so an operator is told. bootstrap_db_reseed blocks on a full
// snapshot fetch, so it runs in a bounded background goroutine and the manager
// tick returns at once.
func replication_reanchor_misaligned(s StalledStream) {
	paths := replication_stream_db_paths(s.User, s.Database)
	if len(paths) == 0 {
		// No DB file on disk yet for this stream (the row can arrive before its
		// file). Nothing to reseed; the op stream anchors when the file lands.
		return
	}

	// Originator guard (#42): NEVER reseed a stream this host has emitted ops on
	// (tail>0). A reseed overwrites the local DB with the peer's copy; if we
	// originated data here, a misaligned cursor that made the peer look
	// authoritative would wipe our OWN data — the 2026-06-25 SEV1, where yuzu
	// (which originated 164k feed posts) reseeded its feeds.db from a near-empty
	// wasabi. Only a pure receiver (tail==0) has nothing of its own to lose. The
	// audit path already guards this; this covers the wiped-rebootstrap path too,
	// since both funnel through here. The no-shrink swap-guard is the final
	// backstop if this is ever bypassed.
	if replication_tail(s.User, s.Scope, s.Database) > 0 {
		info("Replication re-anchor: SKIP peer=%q user=%q db=%q — this host originated the stream (tail>0); a reseed would risk our own data",
			s.Peer, s.User, s.Database)
		return
	}

	key := s.Peer + "|" + s.User + "|" + s.Database
	rebootstrap_mutex.Lock()
	state := rebootstrap_attempts[key]
	if state.gaveup {
		rebootstrap_mutex.Unlock()
		return
	}
	delay := int64(rebootstrap_unanchored_seconds) << uint(state.attempts)
	if delay > rebootstrap_backoff_cap {
		delay = rebootstrap_backoff_cap
	}
	if state.last != 0 && now()-state.last < delay {
		rebootstrap_mutex.Unlock()
		return
	}
	if state.attempts >= rebootstrap_attempt_cap {
		state.gaveup = true
		rebootstrap_attempts[key] = state
		rebootstrap_mutex.Unlock()
		replication_rebootstrap_give_up(s, fmt.Sprintf("targeted reseed did not anchor after %d attempts", state.attempts))
		return
	}
	rebootstrap_mutex.Unlock()

	launched, diverged := false, false
	for _, rel := range paths {
		// #27 gate, per-DB: only overwrite from a source that is not missing any
		// op we originated. A source behind us here is divergence, not a wipe.
		if reseed_source_missing_ops(rel, s.Peer) {
			diverged = true
			continue
		}
		if !reanchor_acquire(rel) {
			continue // already reseeding this DB, or at the concurrency cap
		}
		launched = true
		info("Replication re-anchor: peer=%q user=%q db=%q path=%q targeted reseed attempt %d/%d",
			s.Peer, s.User, s.Database, rel, state.attempts+1, rebootstrap_attempt_cap)
		go func(rel string) {
			defer reanchor_release(rel)
			if err := bootstrap_db_reseed(s.Peer, bootstrap_scope_userdbs, rel); err != nil {
				warn("Replication re-anchor reseed %q from peer %q failed: %v", rel, s.Peer, err)
				return
			}
			info("Replication re-anchor: %q re-seeded from peer %q (cursor re-anchored)", rel, s.Peer)
		}(rel)
	}

	// Consume a backoff attempt only when we actually launched a reseed; a tick
	// blocked purely by the concurrency cap retries next window for free.
	if launched {
		rebootstrap_mutex.Lock()
		state = rebootstrap_attempts[key]
		state.attempts++
		state.last = now()
		rebootstrap_attempts[key] = state
		rebootstrap_mutex.Unlock()
		return
	}
	if diverged {
		info("Replication re-anchor: SKIP peer=%q user=%q db=%q — source is behind on this stream (divergence, not a wipe); operator/merge required",
			s.Peer, s.User, s.Database)
	}
}

// replication_stream_db_paths maps a stalled (user, stream-key) back to the
// data-dir-relative path(s) of the DB file(s) backing the stream — the inverse
// of bootstrap_stream_key — for the targeted reseed. Only files that exist on
// disk are returned.
//
//	app:<id>         → users/<u>/<id>/db/*.db   (app data DBs)
//	app:<id>/system  → users/<u>/<id>/app.db    (per-app config DB)
//	core:<file>      → users/<u>/<file>.db       (per-user infra DB)
func replication_stream_db_paths(user, stream string) []string {
	if user == "" || stream == "" {
		return nil
	}
	exists := func(rel string) bool {
		st, err := os.Stat(filepath.Join(data_dir, filepath.FromSlash(rel)))
		return err == nil && !st.IsDir()
	}
	switch {
	case strings.HasPrefix(stream, repl_stream_class_app+":"):
		rest := strings.TrimPrefix(stream, repl_stream_class_app+":")
		if app := strings.TrimSuffix(rest, "/system"); app != rest {
			rel := "users/" + user + "/" + app + "/app.db"
			if exists(rel) {
				return []string{rel}
			}
			return nil
		}
		entries, err := os.ReadDir(filepath.Join(data_dir, "users", user, rest, "db"))
		if err != nil {
			return nil
		}
		var out []string
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), ".db") {
				continue
			}
			out = append(out, "users/"+user+"/"+rest+"/db/"+e.Name())
		}
		return out
	case strings.HasPrefix(stream, repl_stream_class_core+":"):
		rel := "users/" + user + "/" + strings.TrimPrefix(stream, repl_stream_class_core+":") + ".db"
		if exists(rel) {
			return []string{rel}
		}
		return nil
	}
	return nil
}

// replication_irreparable_notify_local raises a Mochi notification on this
// host: to every administrator for a whole-server stream, or to the owning
// user for a per-user stream. Modelled on update_notify_admins - object=""
// so the sender shows as "Mochi server", and the title/body are resolved
// per-recipient against core labels.
// `urgent` is true when the dead peer held the last other copy of this data
// (no redundancy left) - it escalates the notification title.
func replication_irreparable_notify_local(scope, user string, urgent bool) {
	title := "replica.irreparable.title"
	if urgent {
		title = "replica.irreparable.urgent"
	}
	replication_notify_members(scope, user, "replica/irreparable", title, "replica.irreparable.body")
}

// replication_offline_scan fires the soft offline notification once for any
// member unreachable past offline_threshold, for every relationship the peer
// participates in (whole-server pair and/or per-user host). The notified flag
// dedups within an offline episode; it resets when the peer's next ack drops
// the unreachable row. Members only - a stranger we briefly failed to
// reach never gets a unreachable row. Leader-gated. Runs on the manager's
// hourly tick alongside the irreparable scan.
func replication_offline_scan() {
	db := db_open("db/replication.db")
	gone, _ := db.rows("select peer from unreachable where notified=0 and since < ?", now()-offline_threshold)
	for _, g := range gone {
		peer, _ := g["peer"].(string)
		if peer == "" {
			continue
		}
		if paired, _ := db.exists("select 1 from pair where peer=?", peer); paired {
			if replication_notify_leader(repl_scope_core, "") {
				replication_offline_notify_local(repl_scope_core, "")
			}
		}
		users, _ := db.rows("select distinct user from hosts where peer=?", peer)
		for _, u := range users {
			user, _ := u["user"].(string)
			if replication_notify_leader(repl_scope_app, user) {
				replication_offline_notify_local(repl_scope_app, user)
			}
		}
		db.exec("update unreachable set notified=1 where peer=?", peer)
	}
}

// replication_offline_notify_local raises the soft offline notification: a
// member has been unreachable past offline_threshold (24h) but not yet
// irreparable. Whole-server scope notifies every administrator; per-user
// scope notifies the owning user.
func replication_offline_notify_local(scope, user string) {
	replication_notify_members(scope, user, "replica/offline", "replica.offline.title", "replica.offline.body")
}

// replication_notify_members delivers a replication notification to whoever
// manages the relationship: the owning user for an app (per-user) scope, or
// every administrator for a core (whole-server) scope, each deep-linked to
// the page where they act on it. title and body are core-label keys.
func replication_notify_members(scope, user, topic, title, body string) {
	if scope == repl_scope_app && user != "" {
		replication_notify_user(user, topic, title, body, "/settings/user/replication")
		return
	}
	db := db_open("db/users.db")
	rows, _ := db.rows("select uid from users where role = ?", "administrator")
	for _, row := range rows {
		if id, _ := row["uid"].(string); id != "" {
			replication_notify_user(id, topic, title, body, "/settings/system/replication")
		}
	}
}

// replication_notify_user delivers one replication notification to a user
// uid, resolving the title and body label keys in that user's language and
// showing the sender as "Mochi server" (object=""). Shared by the
// irreparable and offline notifications.
func replication_notify_user(uid, topic, title, body, url string) {
	user := user_by_uid(uid)
	if user == nil {
		return
	}
	lang := user_language(user)
	args := Map{
		"topic":  topic,
		"object": "",
		"title":  resolve_core_label(lang, title, nil),
		"body":   resolve_core_label(lang, body, nil),
		"url":    url,
		"label":  resolve_core_label(lang, "replica.irreparable.topic", nil),
		"count":  int64(1),
	}
	if err := service_call_as_server(uid, "notifications", "send", args); err != nil {
		info("Replication notify user %q topic %q: %v", uid, topic, err)
	}
}

// replication_irreparable_notify_remote tells the other end of the stream
// that the relationship is irreparable, so its admins / owning user are
// notified too. Best-effort fire-and-forget: it only lands if that machine
// is up. The receiver notifies its local side but does NOT echo back, so no
// loop forms.
func replication_irreparable_notify_remote(peer, scope, user, database, reason string) {
	m := message("", "", "replication", "replica/irreparable")
	m.content = map[string]any{
		"scope":  scope,
		"user":   user,
		"db":     database,
		"reason": reason,
	}
	m.send_peer(peer)
}

// replication_irreparable_event is the inbound handler: a peer is telling us
// our replication relationship with it is irreparable from its side. We
// NOTIFY our local admins / owning user (this side may not be able to detect
// the problem locally - the peer can't reach us, but we can reach it) and
// do NOT echo the event back.
//
// Deliberately notify-only, no persistent marker: a mirrored marker here has
// no local recovery signal to clear it (our ack-based clear keys off our own
// unreachable, which a remote-reported marker never sets), so it would
// orphan once the relationship recovers. This side's own Pair / My-hosts
// badge is driven by its own detection (unreachable / stalled), which
// has a proper lifecycle. The notifications app dedups repeats on topic.
func replication_irreparable_event(e *Event) {
	scope, _ := e.content["scope"].(string)
	user, _ := e.content["user"].(string)
	if scope == "" {
		return
	}
	urgent := replication_irreparable_last_copy(db_open("db/replication.db"), scope, user, e.peer)
	replication_irreparable_notify_local(scope, user, urgent)
	info("Replication irreparable reported by peer: peer=%q scope=%q user=%q", e.peer, scope, user)
}

// replication_user_has_local_data reports whether uid has any populated DB on
// disk. It is the safety gate for replication_wiped_rebootstrap (#27): an
// "unanchored" stream means a broken cursor, NOT an empty DB, so a fully
// populated host can look unanchored under cursor misalignment. Re-bootstrapping
// such a host overwrites its real data with the peer's (possibly incomplete)
// copy — the 2026-06-24 incident that wiped 28 DBs on the live primary. Fails
// SAFE: any error returns true (assume data present; do not re-pull).
func replication_user_has_local_data(uid string) bool {
	if uid == "" {
		return false
	}
	root := filepath.Join(data_dir, "users", uid)
	has := false
	_ = filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || has {
			return nil
		}
		if !strings.HasSuffix(p, ".db") || strings.Contains(p, ".backup") {
			return nil
		}
		if db_file_has_rows(p) {
			has = true
		}
		return nil
	})
	return has
}

// db_file_has_rows opens a sqlite file read-only and reports whether any
// non-internal table holds at least one row. Fails SAFE: an open/query error
// returns true (treat as "has data").
func db_file_has_rows(path string) bool {
	// No file at this path → nothing to protect. A bootstrap placing an
	// authoritative source DB here (even an empty one — an app the user has
	// installed but never used) MUST be allowed; the swap creates the file. The
	// query path below fails-safe to "has rows" on any open/query error, which
	// for a NON-EXISTENT target made the swap-guard refuse to place every empty
	// source DB — leaving the receiver stuck incomplete with ~220 unused-app DBs
	// never transferred (#37, rig 2026-06-25). An existing-but-unreadable file
	// still fails safe (true) via the open/query errors below.
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false
	}
	d, err := sql.Open("sqlite3", "file:"+path+"?mode=ro")
	if err != nil {
		return true
	}
	defer d.Close()
	rows, err := d.Query("select name from sqlite_master where type='table' and name not like 'sqlite_%'")
	if err != nil {
		return true
	}
	var tables []string
	for rows.Next() {
		var t string
		if rows.Scan(&t) == nil {
			tables = append(tables, t)
		}
	}
	rows.Close()
	for _, t := range tables {
		var n int
		if d.QueryRow(`select count(*) from "`+t+`"`).Scan(&n) == nil && n > 0 {
			return true
		}
	}
	return false
}

// db_file_data_rows returns the total rows across a DB's replicated DATA tables
// — every non-internal table EXCEPT the host-local change-capture tables
// (journal, journal_delivery) that a freshly-rebuilt bootstrap scratch
// structurally omits — so the count is directly comparable between a live
// receiver (which has those tables) and a scratch (which doesn't). Returns -1
// on any open/query error so the caller treats the result as "unknown" rather
// than a false shrink verdict. Backs the swap-guard's no-shrink check (#42):
// unlike a cursor comparison it reads actual rows, so a misaligned cursor can't
// fool it.
func db_file_data_rows(path string) int64 {
	if st, err := os.Stat(path); err != nil || st.IsDir() {
		return -1
	}
	d, err := sql.Open("sqlite3", "file:"+path+"?mode=ro")
	if err != nil {
		return -1
	}
	defer d.Close()
	rows, err := d.Query("select name from sqlite_master where type='table' and name not like 'sqlite_%' and name not in ('journal', 'journal_delivery')")
	if err != nil {
		return -1
	}
	var tables []string
	for rows.Next() {
		var t string
		if rows.Scan(&t) == nil {
			tables = append(tables, t)
		}
	}
	rows.Close()
	var total int64
	for _, t := range tables {
		var n int64
		if d.QueryRow(`select count(*) from "`+t+`"`).Scan(&n) != nil {
			return -1 // can't count a table → unknown; don't risk a false verdict
		}
		total += n
	}
	return total
}

// Mochi server: bulk bootstrap protocol (#66) — V1 scaffolding
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.
//
// When a fresh replica B joins a populated pair member A via
// `mochictl replica join <A-id>` + source-side Approve, B's local
// state is empty. The per-user link-request keys-transfer in
// replication_link.go transfers one user's identity in a single
// payload — fine for a few MB. Whole-server bootstrap needs to move
// GB-to-TB across:
//
//   1. /var/lib/mochi/users/<id>/<app>/files/        — file trees
//   2. /var/lib/mochi/apps/<entity-id>/               — installed app code
//   3. /var/lib/mochi/users/<id>/<app>/db/<file>.db   — per-user SQLite DBs
//   4. /var/lib/mochi/db/{users,apps,settings,domains,…}.db — system DBs
//
// The protocol is split into two transfer flavours, both manifest-
// driven so the receiver can resume after interruption:
//
//   - File-tree sync: BootstrapFileManifest + BootstrapFileChunk. The
//     sender enumerates a path prefix and returns `(path, size,
//     sha256)` per entry; the receiver compares to its local copy
//     and requests chunks for files that are missing or differ.
//   - SQLite snapshot sync: BootstrapDBSnapshot + BootstrapDBChunk.
//     The sender takes a `VACUUM INTO` of the live DB and streams
//     the result; new writes during the transfer are buffered and
//     flushed as standard `op` events once the snapshot lands.
//
// Per-(scope, peer) progress is tracked in replication.db.bootstrap
// (state ∈ {'queued', 'active', 'done'}; position is the remaining
// EOF count while active, empty while done). Server restart
// re-fires manifest-requests for any non-done row via bootstrap_resume.
//
// What's landed (V1–V7):
//   - Wire types for all four payload pairs (file-tree + DB-snapshot,
//     both with manifest + chunk variants).
//   - Sender-side: file-tree walker (sha256-hashing each file), DB
//     manifest enumerator, file-chunk reader, SQLite online-backup
//     via the ncruces driver's sqlite3_backup_init.
//   - Receiver-side: .partial → atomic-rename chunk writer with
//     traversal-safe path resolution, manifest-diff orchestrator that
//     skips files whose local copy matches by size + sha256, and a
//     pending-counter state machine that auto-transitions each scope
//     to 'done' on the last EOF.
//   - Driver: bootstrap_start fires manifest-requests for all four
//     scopes (files, apps, userdbs, sysdbs) on join-approved and on
//     mochictl replication resync. bootstrap_resume re-fires on
//     server start for any row not yet 'done'.
//   - Operator visibility: mochi.replication.status() exposes
//     aggregate bootstrap_pending; mochi.replication.bootstrap.progress()
//     exposes the per-(peer, scope) drill-down. The Pair page renders
//     this; mochictl wraps the admin HTTP equivalents.
//   - Defensive: chunks from peers not actively bootstrapping the
//     given scope are silently dropped (bootstrap_is_active_source).
//
// Future polish (deferred to alpha-tuning):
//   - Pending-ops buffer during DB snapshot so writes that land mid-
//     snapshot are flushed after the snapshot transfers (rather than
//     trusting the live op-replication channel's idempotent apply).
//   - Multi-page file-tree manifest for TB-scale trees (the current
//     single-page approach reads the full manifest into memory).
//   - Retry policy on transient transport failures (currently
//     surfaced via mochictl replication resync).

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	sqlitedrv "github.com/ncruces/go-sqlite3/driver"
)

// Bootstrap scope names. Used as the `scope` key in
// replication.db.bootstrap and on the wire. Single-word per the
// project convention.
const (
	bootstrap_scope_files   = "files"   // /var/lib/mochi/users/<id>/<app>/files/
	bootstrap_scope_apps    = "apps"    // /var/lib/mochi/apps/<entity-id>/
	bootstrap_scope_userdbs = "userdbs" // per-user SQLite DBs
	bootstrap_scope_sysdbs  = "sysdbs"  // core system DBs
)

// bootstrap_sysdb_excluded names system DBs that must NOT be
// transferred during bulk bootstrap. These are receiver-local
// infrastructure state machines whose contents are meaningful only
// to the host that owns them — overwriting them on a running server
// causes file-handle / SQLite-lock confusion that crashes the daemon
// (observed live as 'database is locked' panic in queue_add_direct
// after a clobber). Both sender (manifest enumeration) and receiver
// (chunk-write apply) honour the deny-list defensively.
//
//	queue.db        — outbound message queue, server-local
//	replication.db  — replication state machine (seen/pending/hosts/
//	                  bootstrap rows), server-local
//	peers.db        — libp2p peer cache, server-local
//
// Other system DBs (users, settings, domains, apps, directory,
// sessions, schedule, external) are legitimately part of the
// bootstrap payload — apart from sessions.db which arguably should
// also be excluded, but the existing session-replication ops handle
// it correctly through the live channel, so leaving it as-is for V1.
var bootstrap_sysdb_excluded = map[string]bool{
	"queue.db":       true,
	"replication.db": true,
	"peers.db":       true,
}

// Bootstrap state values. Mirrors replication.db.bootstrap.state.
//
// Lifecycle: queued → active → (done | incomplete). A scope reaches
// 'done' only when every entry the source advertised in its manifest
// was successfully transferred locally. If any entry failed all retries
// the scope settles to 'incomplete' instead — bootstrap_retry_incomplete
// keeps re-running until the gap closes. Importantly, the per-user
// link signup flow waits for 'done' (not just 'pending counter == 0'),
// so a user with incomplete data stays on the /login/replicating page
// rather than landing on a half-empty dashboard.
const (
	bootstrap_state_queued     = "queued"
	bootstrap_state_active     = "active"
	bootstrap_state_done       = "done"
	bootstrap_state_incomplete = "incomplete"
)

// BootstrapFileManifestRequest is the receiver→sender request for a
// directory listing rooted at `prefix`. Sender enumerates the path
// and returns BootstrapFileManifestResult.
type BootstrapFileManifestRequest struct {
	Scope  string `cbor:"scope"`  // bootstrap_scope_files | bootstrap_scope_apps
	Prefix string `cbor:"prefix"` // relative to the scope root
}

// BootstrapFileManifestResult lists every regular file under the
// requested prefix. The receiver compares (path, size, sha256) to its
// local copy and requests chunks for any missing or differing file.
type BootstrapFileManifestResult struct {
	Scope   string               `cbor:"scope"`
	Prefix  string               `cbor:"prefix"`
	Entries []BootstrapFileEntry `cbor:"entries"`
	Done    bool                 `cbor:"done,omitempty"` // false → another result page follows
}

// BootstrapFileEntry is one (path, size, hash) tuple from a manifest.
// Path is relative to the scope root. Sha256 is hex-encoded.
type BootstrapFileEntry struct {
	Path   string `cbor:"path"`
	Size   int64  `cbor:"size"`
	Sha256 string `cbor:"sha256"`
}

// BootstrapFileChunkRequest is the receiver→sender request for bytes
// `[offset, offset+length)` of `path` under `scope`. Length is bounded
// by a sender-side max chunk size; the receiver issues sequential
// requests until size is reached.
type BootstrapFileChunkRequest struct {
	Scope  string `cbor:"scope"`
	Path   string `cbor:"path"`
	Offset int64  `cbor:"offset"`
	Length int64  `cbor:"length"`
}

// BootstrapFileChunk is the sender→receiver response carrying the
// requested byte range. EOF=true signals this was the final chunk
// (offset+len(data) == file size).
type BootstrapFileChunk struct {
	Scope  string `cbor:"scope"`
	Path   string `cbor:"path"`
	Offset int64  `cbor:"offset"`
	Data   []byte `cbor:"data"`
	EOF    bool   `cbor:"eof,omitempty"`
}

// BootstrapDBFetchRequest is the receiver→sender request opening a
// stream for one DB. The sender takes a SQLite online-backup snapshot
// to a tempfile and writes the bytes back as a sequence of
// BootstrapDBChunk segments on the SAME stream, terminated by EOF.
//
// Replaces the earlier queue-based snapshot-request + chunk events
// which fanned out N queue rows per chunk per DB and snowballed
// queue.db to multi-GB during a few-dozen-user bootstrap (caught live:
// instance 1's queue.db reached 835 MB + 743 MB WAL after a single
// recovery test, mmap'd as 1.5 GB of process RSS and triggered the
// OS low-memory notification).
//
// Mirrors the file-side `bootstrap/file/chunk/fetch` sync-stream RPC
// pattern, with the difference that one stream carries an entire DB
// (multiple chunks) rather than one chunk per RPC — the source's
// snapshot tempfile only lives for the lifetime of the single stream.
type BootstrapDBFetchRequest struct {
	Scope string `cbor:"scope"`          // bootstrap_scope_userdbs | bootstrap_scope_sysdbs
	Path  string `cbor:"path,omitempty"` // relative path under data_dir (modern); preferred when set
	User  string `cbor:"user,omitempty"` // legacy fallback (when Path empty)
	App   string `cbor:"app,omitempty"`  // legacy fallback (when Path empty)
	DB    string `cbor:"db,omitempty"`   // legacy fallback (when Path empty)
}

// BootstrapDBManifestRequest is the receiver→sender ask for the list
// of per-user app DBs + system DBs the source has. The receiver fires
// one of these after the file-tree scopes complete (so the
// /var/lib/mochi/users/<u>/<a>/db/ directory trees exist) and uses
// the response to drive snapshot-requests for each (user, app, db)
// triple. System DBs (`db/*.db`) are enumerated separately for the
// sysdbs scope.
//
// User is the per-user filter for the per-user link signup path: when
// set, the source only returns rows under users/<User>/ — never any
// other user's DBs. Empty User is the whole-server case used by
// pair-join, where the receiver is mirroring every user on the source.
type BootstrapDBManifestRequest struct {
	Scope string `cbor:"scope"`          // bootstrap_scope_userdbs | bootstrap_scope_sysdbs
	User  string `cbor:"user,omitempty"` // optional per-user filter (uid)
}

// BootstrapDBManifestResult lists every DB the source has at the
// time of the request. For userdbs the entries are
// (user, app, db); for sysdbs only the `db` field is populated.
type BootstrapDBManifestResult struct {
	Scope   string             `cbor:"scope"`
	Entries []BootstrapDBEntry `cbor:"entries"`
}

// BootstrapDBEntry is one DB the source has. Comparable to a row of
// the eventual file manifest but specialised: SQLite DBs go through
// the snapshot protocol, not the file-chunk protocol.
//
// Path is the source's relative location under data_dir (e.g.
// "users/<u>/<app>/db/<file>.db", "users/<u>/<app>/app.db",
// "users/<u>/user.db", "db/<file>.db"). The receiver mirrors it
// verbatim into its own data_dir. This single field replaces the
// older User/App/DB triple which assumed a fixed users/<u>/<a>/db/
// layout and missed per-app config DBs (users/<u>/<a>/app.db) and
// per-user infrastructure DBs (users/<u>/user.db). User/App/DB are
// retained for back-compat and audit log readability — the receiver
// trusts Path when set and falls back to the legacy triple when not.
type BootstrapDBEntry struct {
	Path string `cbor:"path,omitempty"`
	User string `cbor:"user,omitempty"`
	App  string `cbor:"app,omitempty"`
	DB   string `cbor:"db,omitempty"`
}

// BootstrapDBChunk is the sender→receiver chunk of a DB snapshot.
// Offset + len(Data) == EOF position when EOF=true. The EOF chunk also
// carries Seed — the sender's replication tail for this DB's stream,
// read just before the snapshot — so the receiver can seed the
// in-order apply cursor at the snapshot's exact sequence point.
type BootstrapDBChunk struct {
	Scope  string `cbor:"scope"`
	User   string `cbor:"user,omitempty"`
	App    string `cbor:"app,omitempty"`
	DB     string `cbor:"db"`
	Offset int64  `cbor:"offset"`
	Data   []byte `cbor:"data"`
	EOF    bool   `cbor:"eof,omitempty"`
	Seed   int64  `cbor:"seed,omitempty"`
}

// bootstrap_stream_key maps a bootstrapped DB file — by its relative
// path under data_dir — to the class-qualified replication stream key the
// in-order gate uses for that file, matching repl_op_stream:
//
//	users/<u>/<app>/db/<file>  → app:<app>          (app data DB)
//	users/<u>/<app>/app.db     → app:<app>/system   (per-app system DB)
//	users/<u>/<file>.db        → core:<file>        (per-user infra DB:
//	                                                core:user, core:notifications)
//
// Returns "" for a file no replication stream targets. Keyed off the
// path structure (not the basename) so an app whose data file is
// itself named app.db can't collide with the config DB. The class prefix
// keeps a dev app named after a reserved core DB (e.g. "notifications")
// from sharing a stream with that core DB.
func bootstrap_stream_key(path string) string {
	parts := strings.Split(filepath.ToSlash(path), "/")
	if len(parts) >= 5 && parts[0] == "users" && parts[3] == "db" {
		return repl_stream_key(repl_stream_class_app, parts[2])
	}
	if len(parts) == 4 && parts[0] == "users" && parts[3] == "app.db" {
		return repl_stream_key(repl_stream_class_app, parts[2]) + "/system"
	}
	if len(parts) == 3 && parts[0] == "users" {
		return repl_stream_key(repl_stream_class_core, strings.TrimSuffix(parts[2], ".db"))
	}
	return ""
}

// BootstrapScopeDone is the receiver→source ack that a bulk-bootstrap
// scope has fully transferred and flipped to `done` on the receiver.
// The source uses it to delete its `bootstrap_served (peer, scope)`
// row; the per-pair-member Syncing/Synced status in the operator UI
// reads this table on the source side so both sides settle to
// "Synced" together rather than the source displaying "Synced" the
// instant the join is approved.
type BootstrapScopeDone struct {
	Scope string `cbor:"scope"`
}

// bootstrap_set_state upserts a bootstrap progress row, recording the
// (scope, peer) pair's current state + opaque position cursor. Use
// bootstrap_state_active while transferring and bootstrap_state_done
// when complete; queued rows are created automatically on first
// reference but callers may also seed them explicitly.
func bootstrap_set_state(scope, peer, state, position string) {
	rdb := db_open("db/replication.db")
	if state == bootstrap_state_active {
		// Reaching (or staying) active means the manifest or a chunk just
		// landed — real forward progress. Stamp `progress` and clear the
		// retry-backoff counter so the universal-retry driver sees the
		// transfer as live and leaves it alone (bootstrap_retry_eligible).
		rdb.exec(
			"insert into bootstrap (scope, peer, state, position, progress, attempts) values (?, ?, ?, ?, ?, 0) "+
				"on conflict(scope, peer) do update set state=excluded.state, position=excluded.position, progress=excluded.progress, attempts=0",
			scope, peer, state, position, now())
	} else {
		rdb.exec(
			"insert into bootstrap (scope, peer, state, position) values (?, ?, ?, ?) "+
				"on conflict(scope, peer) do update set state=excluded.state, position=excluded.position",
			scope, peer, state, position)
	}
	// A completed (re-)bootstrap re-seeds this scope's streams from the
	// peer, which is the only recovery for a stream marked irreparable
	// past T_forget. Clear the terminal marker so its UI badge and the
	// notify-dedup reset. See replication_irreparable.go.
	if state == bootstrap_state_done {
		replication_irreparable_clear(peer, scope)
	}
}

// bootstrap_get_state reads the recorded (state, position) for a
// (scope, peer) pair. Returns (”, ”) if no row exists; callers
// should treat absence as "never started, queue if needed".
func bootstrap_get_state(scope, peer string) (string, string) {
	rdb := db_open("db/replication.db")
	row, _ := rdb.row("select state, position from bootstrap where scope=? and peer=?", scope, peer)
	if row == nil {
		return "", ""
	}
	state, _ := row["state"].(string)
	position, _ := row["position"].(string)
	return state, position
}

// bootstrap_get_failed reads the recorded failed-entry count for a
// (scope, peer) pair. Returns 0 if the row doesn't exist or the count
// is missing.
func bootstrap_get_failed(scope, peer string) int64 {
	rdb := db_open("db/replication.db")
	return int64(rdb.integer("select failed from bootstrap where scope=? and peer=?", scope, peer))
}

// bootstrap_failed_increment atomically bumps the (scope, peer) row's
// failed counter. Called from the file + db scope drivers when a
// transfer gives up after exhausting its retry budget. Must be paired
// with the pending counter decrement so the scope can still settle —
// settle path inspects `failed` and chooses 'done' vs 'incomplete'.
//
// Locks bootstrap_pending_lock so the failed bump + pending decrement
// can be observed together by the settle-check.
func bootstrap_failed_increment(scope, peer string) {
	bootstrap_pending_lock.Lock()
	defer bootstrap_pending_lock.Unlock()
	rdb := db_open("db/replication.db")
	rdb.exec(
		"update bootstrap set failed = failed + 1 where scope=? and peer=?",
		scope, peer)
}

// bootstrap_settled_state chooses between 'done' and 'incomplete' for
// a scope that's reached pending==0. Pure decision function — callers
// already hold bootstrap_pending_lock.
func bootstrap_settled_state(scope, peer string) string {
	rdb := db_open("db/replication.db")
	if rdb.integer("select failed from bootstrap where scope=? and peer=?", scope, peer) > 0 {
		return bootstrap_state_incomplete
	}
	return bootstrap_state_done
}

// bootstrap_clear removes the (scope, peer) row entirely. Called when
// the bootstrap completes successfully and we want to reclaim the
// state-machine slot (vs leaving a 'done' row forever). Callers must
// only invoke after every scope for the peer has reached 'done'.
func bootstrap_clear(scope, peer string) {
	rdb := db_open("db/replication.db")
	rdb.exec("delete from bootstrap where scope=? and peer=?", scope, peer)
}

// bootstrap_pending_lock serialises every read-modify-write of a
// (scope, peer) bootstrap row's `position` field. Paginated manifest
// pagination spawns one driver per page; each driver decrements the
// shared counter as its files land. Without this mutex the parallel
// decrements race (read position, subtract 1, write back) and lose
// updates — observed live: with the apps scope's 21,612 files split
// across 5 driver goroutines, the counter settled at ~2,100 instead
// of 0 despite every file landing on the receiver.
//
// One global mutex is fine: bootstrap traffic is rare (per-pair, not
// per-request) and the protected region is microseconds of SQLite
// work. A per-(scope, peer) mutex would scale better in theory but
// is unnecessary churn.
var bootstrap_pending_lock sync.Mutex

// bootstrap_set_pending sets the (scope, peer) row's pending-file
// counter and transitions to 'active'. Called when a manifest result
// arrives with N entries the receiver needs to fetch — that N is the
// number of files (or DBs) whose EOF chunk we expect to land before
// the scope is complete.
func bootstrap_set_pending(scope, peer string, count int64) {
	bootstrap_pending_lock.Lock()
	defer bootstrap_pending_lock.Unlock()
	bootstrap_set_state(scope, peer, bootstrap_state_active, strconv.FormatInt(count, 10))
}

// bootstrap_pending_add increases the (scope, peer) row's pending
// counter by `delta` and transitions to 'active'. Used by the
// manifest-result receiver when manifests are paginated — each page
// adds its own needed-files contribution rather than overwriting the
// total. Existing 'done' rows are left alone (a late page after the
// scope already settled is a no-op).
func bootstrap_pending_add(scope, peer string, delta int64) {
	bootstrap_pending_lock.Lock()
	defer bootstrap_pending_lock.Unlock()
	rdb := db_open("db/replication.db")
	row, _ := rdb.row("select position, state from bootstrap where scope=? and peer=?", scope, peer)
	var current int64
	if row != nil {
		if state, _ := row["state"].(string); state == bootstrap_state_done {
			return
		}
		position_string, _ := row["position"].(string)
		current, _ = strconv.ParseInt(position_string, 10, 64)
	}
	bootstrap_set_state(scope, peer, bootstrap_state_active, strconv.FormatInt(current+delta, 10))
}

// bootstrap_pending_decrement atomically subtracts 1 from the (scope,
// peer) row's pending counter. If the resulting count is 0 (or
// negative — defensive against unexpected over-decrement), the row
// transitions to state='done' AND triggers an immediate drain of the
// per-app `pending` op buffer (so any ops that arrived during the
// transfer window — and were marked ApplyDeferred because the target
// user / DB wasn't local yet — apply immediately instead of waiting
// up to 30 s for the next replication_manager tick). Called from the
// chunk handlers after a successful EOF write.
//
// Locks bootstrap_pending_lock so concurrent drivers don't race the
// read/modify/write of `position` and lose decrements.
//
// Returns the remaining count (or -1 if the row didn't exist). The
// returned value is mostly for tests; callers don't need it.
func bootstrap_pending_decrement(scope, peer string) int64 {
	bootstrap_pending_lock.Lock()
	rdb := db_open("db/replication.db")
	row, _ := rdb.row("select position, state from bootstrap where scope=? and peer=?", scope, peer)
	if row == nil {
		bootstrap_pending_lock.Unlock()
		return -1
	}
	state, _ := row["state"].(string)
	if state == bootstrap_state_done {
		// Already complete — nothing to decrement.
		bootstrap_pending_lock.Unlock()
		return 0
	}
	position_string, _ := row["position"].(string)
	count, _ := strconv.ParseInt(position_string, 10, 64)
	count--
	if count <= 0 {
		settled := bootstrap_settled_state(scope, peer)
		bootstrap_set_state(scope, peer, settled, "")
		if settled == bootstrap_state_done {
			audit_replication_bootstrap_scope_done(peer, scope)
		}
		bootstrap_pending_lock.Unlock()
		// Only signal scope-settled hook on 'done'; 'incomplete' means
		// the retry manager will re-fire and the receiver shouldn't
		// announce completion to the source yet.
		if settled == bootstrap_state_done {
			bootstrap_scope_settled(peer, scope)
		}
		bootstrap_progress_settle(peer, scope, settled)
		return 0
	}
	bootstrap_set_state(scope, peer, bootstrap_state_active, strconv.FormatInt(count, 10))
	bootstrap_pending_lock.Unlock()
	return count
}

// bootstrap_scope_settled is the side-effect hook fired when a
// (scope, peer) bootstrap row transitions to state='done'. Common to
// the pending-decrement, file-manifest-settle, and empty-db-manifest
// paths so the post-settle work is consistent regardless of which
// path got us there.
//
// Today: drain the deferred-op buffer (so ops that arrived during the
// transfer window apply immediately instead of waiting for the next
// replication_manager tick), re-scan the published apps directory
// when the apps scope settles (so newly-bootstrapped apps load into
// the in-memory registry — without this, the receiver only knows
// about its dev apps and the operator sees "No apps installed" until
// the next server restart), and send a `bootstrap/scope/done` ack to
// the source so it can clear its `bootstrap_served` row and the
// per-pair-member Syncing/Synced status settles symmetrically.
// Indirected through a package var so setup_replication_test can no-op the
// whole hook: it spawns goroutines (replication_pending_drain,
// apps_load_published) that read data_dir via db_open and would race a
// test's data_dir reset on cleanup. The scope's done-state is set before
// this hook fires, so no test depends on the hook's side effects; tests
// exercising them call bootstrap_scope_settled_impl directly.
var bootstrap_scope_settled = bootstrap_scope_settled_impl

func bootstrap_scope_settled_impl(peer, scope string) {
	go replication_pending_drain()
	if scope == bootstrap_scope_apps {
		go apps_load_published()
	}
	if peer != "" {
		go replication_bootstrap_emit_scope_done(peer, scope)
	}
}

// replication_bootstrap_emit_scope_done sends a `bootstrap/scope/done`
// ack to the source that served this scope. Pair-scoped, libp2p-signed
// — no entity context, this is a server-to-server message. Package-level
// var so unit tests can stub it out (the real send_peer touches queue.db,
// which isn't always set up under test isolation).
var replication_bootstrap_emit_scope_done = func(peer, scope string) {
	m := message("", "", "replication", "bootstrap/scope/done")
	m.add(&BootstrapScopeDone{Scope: scope})
	m.send_peer(peer)
}

// replication_bootstrap_scope_done_event is the source-side handler
// for `bootstrap/scope/done` acks. Removes the matching
// `bootstrap_served (peer, scope)` row so the Pair members status
// column flips this peer to "Synced" once every served scope has
// acked. Anonymous (libp2p-signed) — the sender is whichever peer
// holds the message channel; we trust it for this informational
// signal.
func replication_bootstrap_scope_done_event(e *Event) {
	var done BootstrapScopeDone
	if !e.segment(&done) {
		info("Replication bootstrap-scope-done dropping: cannot decode payload (from peer %q)", e.peer)
		return
	}
	if done.Scope == "" {
		return
	}
	rdb := db_open("db/replication.db")
	rdb.exec("delete from bootstrap_served where peer=? and scope=?", e.peer, done.Scope)
	replication_bootstrap_reconcile_on_complete(e.peer)
}

// replication_bootstrap_reconcile_on_complete re-runs the point-in-time
// pair-backfill (users + system DBs + schedule) to `peer` once its bulk
// bootstrap is fully acked (no bootstrap_served rows left) and it's a
// whole-server pair member.
//
// The join-time backfill is a point-in-time snapshot, so any row carried that
// way — schedule events, settings/apps/domains rows, a new user — CREATED
// during the (potentially hours-long) bootstrap can be missed. These paths
// are last-write-wins or emit-once, so unlike the sequenced per-app DB
// streams (a gap buffers and the stalled-stream recovery re-bootstraps) and
// the continuously-resynced file trees, a missed one never self-heals.
// Re-running the backfill once now — when the bulk transfer is done and the
// live op channel is stable — brackets the bootstrap window with a second
// point-in-time pass and closes it. Idempotent (creates-if-absent / REPLACE /
// insert-if-absent on the receiver). Pair members only; a per-user link would
// need a per-user reconcile.
//
// Observed live 2026-06-15: 2 of 16 schedule rows were absent on yuzu after a
// re-bootstrap until a manual backfill — the schedule case of this window.
func replication_bootstrap_reconcile_on_complete(peer string) {
	rdb := db_open("db/replication.db")
	if remaining, _ := rdb.exists("select 1 from bootstrap_served where peer=?", peer); remaining {
		return // bulk bootstrap still in progress
	}
	// We just served `peer` a full bootstrap, so it re-synced its data FROM us —
	// which means it wiped its replication.db and RESET its outbound sequence
	// counter to ~0 (a `replica reset` / fresh rejoin). Its resumed writes will
	// therefore arrive at LOW sequences. Our inbound cursor + seen for it are
	// stale-high from before its reset and would silently drop those ops as
	// "already applied" (sequence <= cursor) — the replica-reset sequence-space
	// misalignment that silently diverged wasabi->yuzu (2026-06-19). Reset our
	// inbound state for it so its fresh stream is accepted and idempotently
	// re-applied. No-op for a brand-new peer (we have no prior state for it).
	replication_inbound_reset(rdb, peer)
	if paired, _ := rdb.exists("select 1 from pair where peer=?", peer); !paired {
		return
	}
	go replication_pair_backfill(peer)
}

// replication_inbound_reset clears this host's inbound replication state for
// `peer` — apply cursors, the seen-dedup set, and any buffered pending ops.
// Called after we serve `peer` a full bootstrap (it has re-synced from us and
// reset its outbound sequence counter), so we must treat its incoming stream as
// fresh; otherwise its low-sequence resumed writes are dropped below our
// stale-high cursor and replication silently diverges. Safe because the peer's
// data now matches our snapshot, so re-applying its subsequent ops is idempotent.
func replication_inbound_reset(rdb *DB, peer string) {
	if rdb == nil || peer == "" {
		return
	}
	rdb.exec("delete from cursor where peer=?", peer)
	rdb.exec("delete from seen where peer=?", peer)
	rdb.exec("delete from pending where peer=?", peer)
	info("Replication inbound state reset for peer %q after serving its bootstrap — now accepts its fresh sequence space", peer)
}

// bootstrap_scopes_for_peer returns every (scope, state, position)
// row for the given peer, in stable order. Used by the receiver-side
// driver to find resumable work and by the admin / mochictl status
// readout.
func bootstrap_scopes_for_peer(peer string) []map[string]string {
	rdb := db_open("db/replication.db")
	rows, _ := rdb.rows("select scope, state, position from bootstrap where peer=? order by scope", peer)
	out := make([]map[string]string, 0, len(rows))
	for _, r := range rows {
		scope, _ := r["scope"].(string)
		state, _ := r["state"].(string)
		position, _ := r["position"].(string)
		out = append(out, map[string]string{
			"scope":    scope,
			"state":    state,
			"position": position,
		})
	}
	return out
}

// bootstrap_file_scope_root returns the absolute filesystem root for
// the named scope. Used by both sender (manifest walker, chunk reader)
// and receiver (.partial writer). Returns ("", error) for unknown
// scopes; the file-tree protocol only handles two scopes (files,
// apps), the DB-snapshot protocol handles the rest.
func bootstrap_file_scope_root(scope string) (string, error) {
	switch scope {
	case bootstrap_scope_files:
		return filepath.Join(data_dir, "users"), nil
	case bootstrap_scope_apps:
		return filepath.Join(data_dir, "apps"), nil
	default:
		return "", fmt.Errorf("bootstrap: scope %q is not file-tree-based", scope)
	}
}

// bootstrap_safe_path joins (root, relative) and confirms the result
// is contained within root. Rejects absolute paths, "..", and symlink
// escapes. Returns the absolute path on success. The receiver and
// sender both call this before any open() / write() so a malicious
// peer can't tunnel out of the scope root.
func bootstrap_safe_path(root, relative string) (string, error) {
	if strings.HasPrefix(relative, "/") {
		return "", fmt.Errorf("bootstrap: absolute path rejected: %q", relative)
	}
	for _, part := range strings.Split(relative, "/") {
		if part == ".." {
			return "", fmt.Errorf("bootstrap: parent-dir traversal rejected: %q", relative)
		}
	}
	candidate := filepath.Clean(filepath.Join(root, relative))
	resolved, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		// File may not yet exist on the receiver — fall back to the
		// candidate which already passed the textual checks above.
		// (EvalSymlinks of the candidate's parent + the basename is
		// what we'd want, but the candidate-only check is sufficient
		// for V2 since the receiver creates files freshly without
		// chasing existing symlinks.)
		resolved = candidate
	}
	root_resolved, err := filepath.EvalSymlinks(root)
	if err != nil {
		root_resolved = root
	}
	if !strings.HasPrefix(resolved+string(filepath.Separator), root_resolved+string(filepath.Separator)) && resolved != root_resolved {
		return "", fmt.Errorf("bootstrap: path %q escapes scope root", relative)
	}
	return candidate, nil
}

// file_is_sqlite_sidecar returns true for filenames that belong to
// SQLite databases — the main .db file (which the userdbs scope
// handles via online-backup snapshots) and its sidecar files
// (-wal, -shm, -journal) which are transient and reconstructed by
// SQLite on next open. We don't want the file-tree walker to pick
// any of these up: the userdbs and files scopes would race on the
// same target file, leaving corruption.
func file_is_sqlite_sidecar(name string) bool {
	for _, suffix := range []string{".db", ".db-wal", ".db-shm", ".db-journal", ".db.backup", ".db.snap"} {
		if strings.HasSuffix(name, suffix) {
			return true
		}
	}
	return false
}

// bootstrap_walk_manifest enumerates every regular file under
// `<scope-root>/<prefix>` and returns one BootstrapFileEntry per file
// with size + sha256. Skips symlinks (we never copy symlinks to a
// replica; if the source has one the operator must reconstruct it on
// the replica). The bootstrap file-manifest event handler streams the
// result in pages via bootstrap_walk_manifest_stream; this slice form is
// retained for tests and full-list callers.
//
// bootstrap_walk_manifest_stream walks the scope tree and invokes `emit`
// once per page of up to pageSize entries AS the walk progresses, plus once
// more for any trailing partial page. Streaming — rather than hashing the
// whole tree into one slice before returning — means the receiver gets its
// first page (and resets its per-read deadline) within seconds even on a
// huge tree (wasabi: 446k files / 12.7 GB took minutes to hash, far past
// the receiver's manifest-read deadline, so every fetch — and every retry —
// timed out before a single page arrived). `emit` must not retain the slice
// it is passed: the backing array is reused between pages, and the stream
// writer encodes it synchronously, so callers that need to keep entries
// copy them (append does).
func bootstrap_walk_manifest_stream(scope, prefix string, pageSize int, emit func([]BootstrapFileEntry) error) error {
	root, err := bootstrap_file_scope_root(scope)
	if err != nil {
		return err
	}
	start_dir, err := bootstrap_safe_path(root, prefix)
	if err != nil {
		return err
	}

	page := make([]BootstrapFileEntry, 0, pageSize)
	walk_error := filepath.Walk(start_dir, func(absPath string, info os.FileInfo, err error) error {
		if err != nil {
			// Missing prefix dir → empty manifest, not an error. Anything
			// else propagates so the caller can see filesystem trouble.
			if os.IsNotExist(err) && absPath == start_dir {
				return io.EOF
			}
			return err
		}
		if !info.Mode().IsRegular() {
			return nil // skip dirs, symlinks, devices
		}
		// Skip SQLite databases + their sidecar files. The userdbs scope
		// handles every users/*/*.db / users/*/*/app.db / users/*/*/db/*.db
		// via SQLite online-backup snapshots. If the file-tree walker
		// also picked them up, the two scopes' drivers would race on the
		// shared <target>.partial file, leaving the receiver with a
		// random mix of raw-file and snapshot bytes — caught live as a
		// 950 MB feeds.db that was 600 MB zeros + 300 MB real data
		// after the userdbs snapshot landed first and the files-scope
		// raw copy overwrote chunks 0..600 with whatever the live DB
		// (under concurrent writes) had at walk time.
		if file_is_sqlite_sidecar(info.Name()) {
			return nil
		}
		rel_path, err := filepath.Rel(root, absPath)
		if err != nil {
			return err
		}
		hash, err := bootstrap_file_sha256(absPath)
		if err != nil {
			return err
		}
		page = append(page, BootstrapFileEntry{
			Path:   filepath.ToSlash(rel_path),
			Size:   info.Size(),
			Sha256: hash,
		})
		if len(page) >= pageSize {
			if err := emit(page); err != nil {
				return err
			}
			page = page[:0]
		}
		return nil
	})
	if walk_error != nil && walk_error != io.EOF {
		return walk_error
	}
	if len(page) > 0 {
		return emit(page)
	}
	return nil
}

func bootstrap_walk_manifest(scope, prefix string) ([]BootstrapFileEntry, error) {
	var entries []BootstrapFileEntry
	err := bootstrap_walk_manifest_stream(scope, prefix, bootstrap_manifest_page_size, func(page []BootstrapFileEntry) error {
		entries = append(entries, page...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

// bootstrap_file_sha256 hashes the file at `path` and returns the hex
// digest. Helper for bootstrap_walk_manifest; small files are read in
// one shot, larger files stream through a 64KB buffer.
func bootstrap_file_sha256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// bootstrap_read_chunk reads up to `length` bytes from `path` at
// `offset` and returns the bytes plus an EOF flag. Path is relative
// to the scope root; bootstrap_safe_path validates against traversal.
// The receiver issues sequential chunk requests in order to assemble
// a file; this function is stateless across calls.
func bootstrap_read_chunk(scope, path string, offset, length int64) ([]byte, bool, error) {
	root, err := bootstrap_file_scope_root(scope)
	if err != nil {
		return nil, false, err
	}
	abs, err := bootstrap_safe_path(root, path)
	if err != nil {
		return nil, false, err
	}
	f, err := os.Open(abs)
	if err != nil {
		return nil, false, err
	}
	defer f.Close()
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return nil, false, err
	}
	buf := make([]byte, length)
	n, err := io.ReadFull(f, buf)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return buf[:n], true, nil
	}
	if err != nil {
		return nil, false, err
	}
	// Did we consume the whole file? Check by stat'ing total size.
	st, err := f.Stat()
	if err != nil {
		return nil, false, err
	}
	eof := offset+int64(n) >= st.Size()
	return buf[:n], eof, nil
}

// bootstrap_manifest_page_size caps the number of entries in any one
// BootstrapFileManifestResult page on the stream. The receiver's CBOR
// decoder rejects arrays over 10,000 elements (cbor_max_elements); the
// apps scope on a fully-populated server is 21k+ files (every published
// app's web/dist tree), so a single-page manifest is not viable. 5000
// entries per page gives ~3-5 pages for the apps scope, each ~500-900
// KB CBOR.
const bootstrap_manifest_page_size = 5000

// replication_bootstrap_file_manifest_event is the sender's stream
// handler for sync-RPC manifest fetch. Reads the request from the
// stream, then walks the requested path prefix and writes each
// BootstrapFileManifestResult page back on the same stream AS the walk
// produces it (up to bootstrap_manifest_page_size entries per page),
// followed by a final empty Done=true page. Streaming during the walk —
// not hashing the whole tree first — is what lets the receiver read its
// first page within seconds on a huge tree instead of timing out while
// the source hashes hundreds of thousands of files.
func replication_bootstrap_file_manifest_event(e *Event) {
	if e.stream == nil {
		info("Replication bootstrap-file-manifest: no stream (queued retry?) — dropping")
		return
	}
	// Stream-RPC payload arrives as e.content (already decoded as
	// map[string]any by stream_receive's read_content); pull fields
	// directly rather than calling e.segment which would EOF — only
	// one request segment was sent on the stream.
	scope, _ := e.content["scope"].(string)
	prefix, _ := e.content["prefix"].(string)

	walk_err := bootstrap_walk_manifest_stream(scope, prefix, bootstrap_manifest_page_size, func(page []BootstrapFileEntry) error {
		return e.stream.write(&BootstrapFileManifestResult{Scope: scope, Prefix: prefix, Entries: page})
	})
	if walk_err != nil {
		info("Replication bootstrap-file-manifest: walk failed (scope=%q prefix=%q from=%q): %v",
			scope, prefix, e.peer, walk_err)
		// Do NOT send Done=true: the manifest is incomplete, and marking the
		// scope done would silently drop the un-walked files. Returning closes
		// the stream; the receiver's next read errors, the scope stays
		// non-done, and the retry driver re-walks (the receiver-side diff
		// keeps whatever pages already landed). A write failure (receiver gone)
		// surfaces here too and is handled the same way.
		return
	}
	// Final empty page marks completion. An empty tree (or a missing prefix
	// dir, which the walker treats as empty) sends only this page, so the
	// receiver still transitions the scope to 'done'.
	if err := e.stream.write(&BootstrapFileManifestResult{Scope: scope, Prefix: prefix, Done: true}); err != nil {
		info("Replication bootstrap-file-manifest: write final page failed: %v", err)
	}
}

// bootstrap_diff_manifest returns the subset of `remote` entries whose
// local copy is missing or differs (size or sha256 mismatch). Local
// state is computed by re-walking the same scope/prefix on the
// receiver's filesystem; entries the receiver has but the sender
// doesn't are NOT removed locally — bulk bootstrap is additive at
// this layer, garbage collection is a follow-up concern.
//
// V3 caveat: the rehash of every local file under the prefix is O(N)
// in local bytes per manifest page. Acceptable for the per-user files
// scope; for the apps scope (where most files are unchanged binaries)
// the V4 follow-up adds an "ack-list" so the receiver only rehashes
// candidates it has previously cached as up-to-date.
func bootstrap_diff_manifest(scope, prefix string, remote []BootstrapFileEntry) ([]BootstrapFileEntry, error) {
	local_entries, err := bootstrap_walk_manifest(scope, prefix)
	if err != nil {
		// Walk failed → treat every remote entry as missing locally.
		// The receiver's chunk-write path will create the parent dirs
		// as it goes, so an empty / missing local tree just means we
		// fetch the lot.
		local_entries = nil
	}
	local := make(map[string]BootstrapFileEntry, len(local_entries))
	for _, e := range local_entries {
		local[e.Path] = e
	}

	var needed []BootstrapFileEntry
	for _, r := range remote {
		if l, ok := local[r.Path]; ok && l.Size == r.Size && l.Sha256 == r.Sha256 {
			continue // already-have, skip
		}
		needed = append(needed, r)
	}
	return needed, nil
}

// bootstrap_chunk_requests_for_entry yields the sequence of
// (offset, length) chunk requests needed to fetch the full file. Uses
// bootstrap_max_chunk_size as the chunk granularity; the last chunk
// is short. A zero-byte file gets one (0, 0) request — the sender
// responds with EOF=true and an empty Data, which is the explicit
// "create an empty file here" signal.
func bootstrap_chunk_requests_for_entry(entry BootstrapFileEntry) []BootstrapFileChunkRequest {
	if entry.Size == 0 {
		return []BootstrapFileChunkRequest{{Path: entry.Path, Offset: 0, Length: 0}}
	}
	var out []BootstrapFileChunkRequest
	var offset int64
	for offset < entry.Size {
		length := int64(bootstrap_max_chunk_size)
		if remaining := entry.Size - offset; remaining < length {
			length = remaining
		}
		out = append(out, BootstrapFileChunkRequest{Path: entry.Path, Offset: offset, Length: length})
		offset += length
	}
	return out
}

// bootstrap_file_chunk_fetch is the synchronous-stream chunk fetch.
// Opens a stream to the sender, writes the chunk-request as the first
// segment, reads the chunk-response (one segment carrying up to
// bootstrap_max_chunk_size bytes + EOF flag), closes the stream.
//
// Replaces the earlier queue-based chunk-request / chunk-response pair
// which fed every 1 MiB chunk through queue.db on both sides. With a
// few hundred files and a few hundred MiB per per-user-app-DB, the
// queue's 1 GB cap was tripped within seconds of bootstrap kickoff.
// Going synchronous makes each chunk a single round-trip with no
// queue involvement — the response IS the ACK.
//
// The caller is the manifest-diff orchestrator running in its own
// goroutine per scope; it fetches each needed (path, offset) pair
// sequentially, writing chunks to `.partial` as they arrive, and
// atomic-renaming on EOF. Failure on any chunk drops the file —
// the operator's resync re-fetches.
//
// Package-level alias so tests can stub the network call.
var bootstrap_file_chunk_fetch = bootstrap_file_chunk_fetch_impl

func bootstrap_file_chunk_fetch_impl(peer, scope, path string, offset, length int64) (*BootstrapFileChunk, error) {
	if peer == "" {
		return nil, fmt.Errorf("bootstrap-file-chunk-fetch: empty peer")
	}
	s, err := stream_to_peer(peer, "", "", "replication", "bootstrap/file/chunk/fetch", "", nil)
	if err != nil {
		return nil, fmt.Errorf("bootstrap-file-chunk-fetch: open stream: %w", err)
	}
	defer s.close()

	if err := s.write(&BootstrapFileChunkRequest{
		Scope: scope, Path: path, Offset: offset, Length: length,
	}); err != nil {
		return nil, fmt.Errorf("bootstrap-file-chunk-fetch: write request: %w", err)
	}

	var resp BootstrapFileChunk
	if err := s.read(&resp); err != nil {
		return nil, fmt.Errorf("bootstrap-file-chunk-fetch: read response: %w", err)
	}
	return &resp, nil
}

// replication_bootstrap_file_manifest_result_apply diffs the manifest
// against the local copy and spawns one driver goroutine that
// synchronously fetches each needed file's chunks via stream RPC.
// No queue involvement: the goroutine opens one stream per chunk,
// writes the request, reads the response, writes the chunk to
// .partial, and atomic-renames on EOF.
//
// Why per-scope driver vs per-file goroutine: serialising the fetches
// keeps a single stream-open-at-a-time pattern that doesn't trip the
// peer's per-second connection rate limit. Bandwidth is the limit,
// not concurrency. For per-file fan-out we'd need to throttle to
// stay under the rate limit anyway.
func replication_bootstrap_file_manifest_result_apply(originPeer string, res *BootstrapFileManifestResult) {
	needed, err := bootstrap_diff_manifest(res.Scope, res.Prefix, res.Entries)
	if err != nil {
		info("Replication bootstrap-file-manifest-result: diff failed (scope=%q prefix=%q from=%q): %v",
			res.Scope, res.Prefix, originPeer, err)
		return
	}
	// Manifests are paginated. Each page contributes its own needed-
	// files count to the pending counter; the per-scope driver
	// decrements it as each file lands. The final page (Done=true)
	// triggers a settle check: if no files were ever needed across all
	// pages, the scope transitions to 'done' here. Otherwise the last
	// pending decrement transitions it.
	if len(needed) > 0 {
		bootstrap_pending_add(res.Scope, originPeer, int64(len(needed)))
		// Defer the bulk chunk-fetch driver while `originPeer` is in its
		// manifest phase (bootstrap_start) so a multi-GB transfer can't
		// start mid-phase and starve another scope's manifest read. Capture
		// scope as a local — `res` (=&page) is reused across the manifest
		// pagination loop, so the closure must not read res lazily.
		scope := res.Scope
		bootstrap_phase_drive(originPeer, scope, func() {
			bootstrap_file_scope_driver(originPeer, scope, needed)
		})
	}
	if res.Done {
		// Read position + state under the same lock the decrement
		// uses, so a concurrent decrement in flight at the moment the
		// final page lands can't lead to a missed settle-to-done.
		bootstrap_pending_lock.Lock()
		rdb := db_open("db/replication.db")
		row, _ := rdb.row("select position, state from bootstrap where scope=? and peer=?", res.Scope, originPeer)
		var settle bool
		if row == nil {
			// No row at all — no work was ever queued.
			settle = true
		} else {
			state, _ := row["state"].(string)
			position_string, _ := row["position"].(string)
			count, _ := strconv.ParseInt(position_string, 10, 64)
			// Pending=0 + Done=true means every needed file across all
			// pages is already local (or nothing was needed at all).
			settle = state != bootstrap_state_done && count == 0
		}
		var settledState string
		if settle {
			settledState = bootstrap_settled_state(res.Scope, originPeer)
			bootstrap_set_state(res.Scope, originPeer, settledState, "")
			if settledState == bootstrap_state_done {
				audit_replication_bootstrap_scope_done(originPeer, res.Scope)
			}
		}
		bootstrap_pending_lock.Unlock()
		if settle && settledState == bootstrap_state_done {
			bootstrap_scope_settled(originPeer, res.Scope)
		}
		if settle {
			bootstrap_progress_settle(originPeer, res.Scope, settledState)
		}
	}
}

// bootstrap_chunk_fetch_with_retry wraps bootstrap_file_chunk_fetch
// with bounded exponential-backoff retry for transient stream errors.
// Backoff: 1s, 2s, 4s, capped at 4 attempts. Matches the plan's
// "5 s → 60 s cap" intent for whole-bootstrap retries, scaled down
// because chunk fetches are individual RPCs; total worst-case retry
// budget is ~7 s before giving up.
//
// The receiver's driver gives up on a file after this returns an
// error and decrements the pending counter (so the scope can settle),
// then the operator's `mochictl replication resync` picks up the
// remaining files on a re-run.
func bootstrap_chunk_fetch_with_retry(peer, scope, path string, offset, length int64) (*BootstrapFileChunk, error) {
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(1<<(attempt-1)) * time.Second)
		}
		resp, err := bootstrap_file_chunk_fetch(peer, scope, path, offset, length)
		if err == nil {
			return resp, nil
		}
		lastErr = err
		debug("Bootstrap chunk-fetch retry: scope=%q path=%q offset=%d attempt=%d err=%v",
			scope, path, offset, attempt+1, err)
	}
	return nil, lastErr
}

// bootstrap_file_scope_driver runs in its own goroutine for one
// (scope, peer) pair. Fetches each file's chunks sequentially via
// stream RPC, writes to .partial, atomic-renames on EOF, decrements
// the pending counter. Failure on any chunk skips the file (the
// pending counter still decrements so the scope can complete; the
// missing file gets re-fetched on the next operator-initiated resync,
// which re-runs the diff and re-drives this loop).
//
// Package-level variable so tests can stub the network drive entirely.
var bootstrap_file_scope_driver = bootstrap_file_scope_driver_impl

func bootstrap_file_scope_driver_impl(peer, scope string, needed []BootstrapFileEntry) {
	for _, entry := range needed {
		ok := true
		var size int64
		for _, req := range bootstrap_chunk_requests_for_entry(entry) {
			resp, err := bootstrap_chunk_fetch_with_retry(peer, scope, req.Path, req.Offset, req.Length)
			if err != nil {
				info("Bootstrap file-scope driver: fetch failed after retries (scope=%q path=%q offset=%d from=%q): %v",
					scope, req.Path, req.Offset, peer, err)
				ok = false
				break
			}
			if err := bootstrap_write_chunk(scope, resp.Path, resp.Offset, resp.Data, resp.EOF); err != nil {
				info("Bootstrap file-scope driver: write failed (scope=%q path=%q offset=%d): %v",
					scope, resp.Path, resp.Offset, err)
				ok = false
				break
			}
			size += int64(len(resp.Data))
			if resp.EOF {
				break
			}
		}
		if ok {
			bootstrap_progress_transfer(peer, "file", size)
		} else {
			// Failed file: bump the failed counter so the settle path
			// chooses 'incomplete' instead of 'done'. The pending
			// counter still decrements below so the scope can settle
			// — the retry manager will re-fire later, the diff will
			// match this file as still-needed, and it gets fetched
			// again with fresh retries.
			bootstrap_failed_increment(scope, peer)
		}
		bootstrap_pending_decrement(scope, peer)
	}
}

// replication_bootstrap_file_manifest_fetch is the receiver-side
// orchestrator for a file-manifest sync RPC. Opens a stream to peer,
// writes the request, reads paged manifest responses, applies each via
// replication_bootstrap_file_manifest_result_apply, and returns when
// the final Done=true page lands. Designed to run in a goroutine —
// the caller fires-and-forgets; this function blocks for the full
// manifest stream duration.
//
// Package-level alias so tests can stub the network call.
var replication_bootstrap_file_manifest_fetch = replication_bootstrap_file_manifest_fetch_impl

func replication_bootstrap_file_manifest_fetch_impl(peer, scope, prefix string) {
	if peer == "" {
		return
	}
	s, err := stream_to_peer(peer, "", "", "replication", "bootstrap/file/manifest", "", nil)
	if err != nil {
		info("Replication bootstrap-file-manifest-fetch: open stream (scope=%q peer=%q): %v", scope, peer, err)
		return
	}
	defer s.close()
	// The source streams a page per bootstrap_manifest_page_size files as it
	// walks + hashes the tree; on a large tree the gap before the first page
	// (and between pages) can exceed the framework's default 30s read
	// deadline, so use the same generous per-read deadline as the bulk
	// transfers. Per-read, so each streamed page resets it.
	s.timeout.read = bootstrap_stream_timeout

	if err := s.write(&BootstrapFileManifestRequest{Scope: scope, Prefix: prefix}); err != nil {
		info("Replication bootstrap-file-manifest-fetch: write request (scope=%q peer=%q): %v", scope, peer, err)
		return
	}

	for {
		var page BootstrapFileManifestResult
		if err := s.read(&page); err != nil {
			info("Replication bootstrap-file-manifest-fetch: read page (scope=%q peer=%q): %v", scope, peer, err)
			return
		}
		replication_bootstrap_file_manifest_result_apply(peer, &page)
		if page.Done {
			return
		}
	}
}

// replication_bootstrap_file_chunk_fetch_event is the sender's
// stream handler for synchronous chunk fetch. Reads the request from
// e.content (single-segment stream RPC, same pattern as user-lookup
// and freshness-probe), reads the file chunk, writes the response
// back on the same stream. No queue involvement — the response goes
// directly over the open stream the caller is reading from.
func replication_bootstrap_file_chunk_fetch_event(e *Event) {
	if e.stream == nil {
		info("Replication bootstrap-file-chunk-fetch: no stream (queued retry?) — dropping")
		return
	}
	// Stream-RPC payload arrives as e.content (already decoded as
	// map[string]any by read_content); pull fields out instead of
	// calling e.segment which would EOF (only one segment was sent).
	scope, _ := e.content["scope"].(string)
	path, _ := e.content["path"].(string)
	offset := row_int(e.content, "offset")
	length := row_int(e.content, "length")
	// Length=0 is the "empty file marker" produced by
	// bootstrap_chunk_requests_for_entry for zero-byte files — the
	// caller still wants the path created locally, so we reply with
	// EOF=true and empty Data instead of rejecting.
	if length < 0 || length > bootstrap_max_chunk_size {
		info("Replication bootstrap-file-chunk-fetch rejecting: length %d out of range (0..%d) from=%q",
			length, bootstrap_max_chunk_size, e.peer)
		return
	}
	var data []byte
	var eof bool
	if length == 0 {
		eof = true
	} else {
		var err error
		data, eof, err = bootstrap_read_chunk(scope, path, offset, length)
		if err != nil {
			info("Replication bootstrap-file-chunk-fetch: read failed (scope=%q path=%q offset=%d from=%q): %v",
				scope, path, offset, e.peer, err)
			return
		}
	}
	resp := &BootstrapFileChunk{
		Scope: scope, Path: path, Offset: offset, Data: data, EOF: eof,
	}
	if err := e.stream.write(resp); err != nil {
		info("Replication bootstrap-file-chunk-fetch: write response failed (scope=%q path=%q offset=%d from=%q): %v",
			scope, path, offset, e.peer, err)
		return
	}
}

// bootstrap_max_chunk_size caps a single chunk request at 1 MiB. The
// receiver issues sequential requests until file size is reached;
// larger chunks would just add latency on retries.
const bootstrap_max_chunk_size = 1 << 20

// bootstrap_stream_timeout is the per-{read,write} deadline used for
// the long-lived sync-stream RPCs that carry bulk-bootstrap chunks.
// The framework default (30s, see stream.read / stream.write) is
// per-call, not per-stream — but a single 1 MiB write on a long DB
// transfer can sit under flow-control backpressure for >30s when the
// receiver's disk lags behind the sender's network throughput. The
// 948 MB feeds.db transfer in live testing tripped this at offset
// 100 MiB. Five minutes is generous: per-call, not cumulative — each
// chunk gets a fresh 5-min window. If a write actually does block
// for 5 minutes the underlying transport is broken; failing then is
// correct.
const bootstrap_stream_timeout = 300

// bootstrap_stream_max_bytes caps the receiver's decoder LimitReader
// for a bulk-bootstrap DB transfer. Default cbor_max_size (100 MB) is
// cumulative across the stream's lifetime; multi-GB DB transfers hit
// it well before the snapshot ends. 50 GiB covers the per-DB cap
// (db_max_page_count × 4 KiB = 25 GiB at the current setting) with
// 2x headroom. Per-element CBOR limits (MaxMapPairs, MaxArrayElements,
// MaxNestedLevels) still bound any single message's structure.
const bootstrap_stream_max_bytes = 50 * 1024 * 1024 * 1024

// bootstrap_write_chunk writes a received chunk to disk on the
// receiver. Bytes go to `<final>.partial` so an interrupted transfer
// can be resumed without overwriting an intact file. On EOF the
// partial is atomically renamed to the final path. Creates any
// missing parent directories.
//
// Caller is responsible for matching the (scope, path) tuple to the
// expected file from the sender's manifest — bootstrap_write_chunk
// trusts the input and only guards against path traversal.
func bootstrap_write_chunk(scope, path string, offset int64, data []byte, eof bool) error {
	root, err := bootstrap_file_scope_root(scope)
	if err != nil {
		return err
	}
	final, err := bootstrap_safe_path(root, path)
	if err != nil {
		return err
	}
	partial := final + ".partial"

	if err := os.MkdirAll(filepath.Dir(final), 0o755); err != nil {
		return fmt.Errorf("bootstrap: mkdir for %q: %w", path, err)
	}

	f, err := os.OpenFile(partial, os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		return fmt.Errorf("bootstrap: open partial %q: %w", partial, err)
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		f.Close()
		return fmt.Errorf("bootstrap: seek %q to %d: %w", partial, offset, err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("bootstrap: write %q at %d: %w", partial, offset, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("bootstrap: close %q: %w", partial, err)
	}

	if eof {
		if err := os.Rename(partial, final); err != nil {
			return fmt.Errorf("bootstrap: rename %q -> %q: %w", partial, final, err)
		}
	}
	return nil
}

// bootstrap_db_source_path returns the absolute path of the source DB
// for a snapshot request.
//
// If `path` is non-empty it's the explicit relative path supplied by
// the walker (e.g. "users/<u>/<app>/app.db", "users/<u>/user.db",
// "db/queue.db"). It's validated against directory traversal and
// joined under data_dir. This is the modern code path that covers
// every user-tree layout (per-app data DBs in db/, per-app config DBs
// at the app root, per-user infrastructure DBs at the user root).
//
// If `path` is empty we fall back to the legacy User/App/DB triple
// shape — used by manifest entries from older senders. The legacy
// layout is users/<user>/<app>/db/<db> for userdbs and db/<db> for
// sysdbs.
//
// The file basename is validated to prevent directory traversal
// (only `^[A-Za-z0-9_.-]+\.db$` for the bare filename).
func bootstrap_db_source_path(scope, path, user, app, db string) (string, error) {
	if path != "" {
		return bootstrap_db_safe_path(path)
	}
	if !bootstrap_db_basename_safe(db) {
		return "", fmt.Errorf("bootstrap: invalid db basename %q", db)
	}
	switch scope {
	case bootstrap_scope_userdbs:
		if user == "" || app == "" {
			return "", fmt.Errorf("bootstrap: user-scope db needs user + app")
		}
		// user / app names: alphanumerics + a few extras, no /, no ..
		if strings.Contains(user, "/") || strings.Contains(user, "..") {
			return "", fmt.Errorf("bootstrap: invalid user %q", user)
		}
		if strings.Contains(app, "/") || strings.Contains(app, "..") {
			return "", fmt.Errorf("bootstrap: invalid app %q", app)
		}
		return filepath.Join(data_dir, "users", user, app, "db", db), nil
	case bootstrap_scope_sysdbs:
		return filepath.Join(data_dir, "db", db), nil
	default:
		return "", fmt.Errorf("bootstrap: scope %q is not db-snapshot-based", scope)
	}
}

// bootstrap_db_target_path mirrors bootstrap_db_source_path on the
// receiver side. Same layout; same validation.
func bootstrap_db_target_path(scope, path, user, app, db string) (string, error) {
	return bootstrap_db_source_path(scope, path, user, app, db)
}

// bootstrap_db_safe_path validates a relative DB path supplied by a
// peer manifest. Rejects empty, absolute, "..", or non-.db basenames.
// Joins under data_dir. Used by the new Path-bearing manifest entries
// to support per-app config DBs and per-user infrastructure DBs that
// don't fit the historical users/<u>/<a>/db/ layout.
func bootstrap_db_safe_path(rel string) (string, error) {
	if rel == "" {
		return "", fmt.Errorf("bootstrap: empty db path")
	}
	if filepath.IsAbs(rel) {
		return "", fmt.Errorf("bootstrap: absolute db path %q", rel)
	}
	clean := filepath.Clean(rel)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") || strings.Contains(clean, "/../") {
		return "", fmt.Errorf("bootstrap: traversal in db path %q", rel)
	}
	if !bootstrap_db_basename_safe(filepath.Base(clean)) {
		return "", fmt.Errorf("bootstrap: invalid db basename in path %q", rel)
	}
	// Whole-path validation: only allow the documented subtrees.
	if !strings.HasPrefix(clean, "users/") && !strings.HasPrefix(clean, "db/") {
		return "", fmt.Errorf("bootstrap: db path %q outside users/ or db/", rel)
	}
	return filepath.Join(data_dir, clean), nil
}

// bootstrap_db_basename_safe matches `^[A-Za-z0-9_.-]+\.db$` — the
// only DB filenames we accept on the wire. Rejects empty names, slash,
// "..", leading dot.
func bootstrap_db_basename_safe(name string) bool {
	if name == "" || name == "." || name == ".." || !strings.HasSuffix(name, ".db") {
		return false
	}
	for _, ch := range name {
		switch {
		case ch >= 'a' && ch <= 'z':
		case ch >= 'A' && ch <= 'Z':
		case ch >= '0' && ch <= '9':
		case ch == '.' || ch == '_' || ch == '-':
		default:
			return false
		}
	}
	return true
}

// replication_bootstrap_db_fetch_event is the source's stream handler.
// Reads the BootstrapDBFetchRequest from e.content (single-segment
// stream RPC, same pattern as user/lookup, freshness/probe, and
// bootstrap/file/chunk/fetch), takes a SQLite online-backup snapshot
// to a tempfile, and writes the contents as a sequence of
// BootstrapDBChunk segments on the same stream. Terminated by a
// chunk with EOF=true.
//
// V4 caveat: writes to the live DB during the snapshot are not
// buffered — they will be picked up by the standard replication op
// channel as long as the (scope, peer) tracker for the receiver is
// in place. High-write workloads during bootstrap may experience a
// brief inconsistency window that's resolved once live op replay
// catches up. The pending-ops-buffer design (plan line 644-646) is a
// V5 follow-up.
func replication_bootstrap_db_fetch_event(e *Event) {
	if e.stream == nil {
		info("Replication bootstrap-db-fetch: no stream (queued retry?) — dropping")
		return
	}
	// Stream-RPC payload arrives as e.content (already decoded as
	// map[string]any); pull fields directly rather than calling
	// e.segment which would EOF (only one request segment was sent).
	scope, _ := e.content["scope"].(string)
	path, _ := e.content["path"].(string)
	user, _ := e.content["user"].(string)
	app, _ := e.content["app"].(string)
	db, _ := e.content["db"].(string)

	// For sysdb exclusion the basename is what matters — path-bearing
	// (modern) requests have the basename embedded in path; legacy
	// requests have it in db.
	basename := db
	if path != "" {
		basename = filepath.Base(path)
	}
	if scope == bootstrap_scope_sysdbs && bootstrap_sysdb_excluded[basename] {
		info("Replication bootstrap-db-fetch rejecting: sysdb %q is server-local and must not be transferred", basename)
		return
	}
	source_path, err := bootstrap_db_source_path(scope, path, user, app, db)
	if err != nil {
		info("Replication bootstrap-db-fetch rejecting (scope=%q path=%q user=%q app=%q db=%q from=%q): %v",
			scope, path, user, app, db, e.peer, err)
		return
	}
	if !file_exists(source_path) {
		info("Replication bootstrap-db-fetch: source %q does not exist (from=%q)", source_path, e.peer)
		return
	}

	tmp, err := os.CreateTemp("", "mochi-bootstrap-*.db")
	if err != nil {
		info("Replication bootstrap-db-fetch: tempfile create failed (from=%q): %v", e.peer, err)
		return
	}
	tmp_path := tmp.Name()
	_ = tmp.Close()
	defer os.Remove(tmp_path)

	// Read this DB's replication tail before snapshotting, so the EOF
	// chunk can carry it as the receiver's apply-cursor seed. Reading
	// before the snapshot guarantees the seed is at — or just behind —
	// the snapshot's sequence point: a tiny idempotent re-apply window,
	// never a gap that would drop ops.
	rel := strings.TrimPrefix(source_path, data_dir+string(os.PathSeparator))
	var seedSeq int64
	if stream := bootstrap_stream_key(rel); stream != "" {
		// Derive the owning user from the path, NOT the request's `user` field. A
		// reseed fetch carries an empty user (bootstrap_db_reseed passes ""), so
		// using it would make replication_tail return 0 and seed the receiver's
		// cursor to 0 — harmless for a quiet stream (it re-syncs on the next write)
		// but a wedge for a heavily-written one whose pruned journal can't backfill
		// from zero. The path is authoritative and present for fresh and reseed
		// fetches alike.
		seedUser := user
		if parts := strings.Split(filepath.ToSlash(rel), "/"); len(parts) >= 2 && parts[0] == "users" {
			seedUser = parts[1]
		}
		seedSeq = replication_tail(seedUser, repl_scope_app, stream)
	}

	size, err := snapshot_copy_db(source_path, tmp_path)
	if err != nil {
		info("Replication bootstrap-db-fetch: backup %q failed (from=%q): %v", source_path, e.peer, err)
		return
	}

	f, err := os.Open(tmp_path)
	if err != nil {
		info("Replication bootstrap-db-fetch: reopen snapshot failed (from=%q): %v", e.peer, err)
		return
	}
	defer f.Close()

	// Bulk DB transfer can run for minutes on a large user DB; bump
	// the per-call write deadline so flow-control backpressure
	// from a slow-disk receiver doesn't trip the 30s default.
	e.stream.timeout.write = bootstrap_stream_timeout

	buf := make([]byte, bootstrap_max_chunk_size)
	var offset int64
	for {
		n, readErr := f.Read(buf)
		eof := readErr == io.EOF || offset+int64(n) >= size
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			c := &BootstrapDBChunk{
				Scope:  scope,
				User:   user,
				App:    app,
				DB:     db,
				Offset: offset,
				Data:   chunk,
				EOF:    eof,
			}
			if eof {
				c.Seed = seedSeq
			}
			if write_error := e.stream.write(c); write_error != nil {
				info("Replication bootstrap-db-fetch: write chunk failed at %d (from=%q): %v", offset, e.peer, write_error)
				return
			}
			offset += int64(n)
		}
		if eof {
			break
		}
		if readErr != nil {
			info("Replication bootstrap-db-fetch: read failed at %d (from=%q): %v", offset, e.peer, readErr)
			return
		}
	}
}

// bootstrap_db_fetch is the receiver-side synchronous-stream fetch
// for one DB. Opens a stream to `peer`, writes a single
// BootstrapDBFetchRequest as the first segment, then reads
// BootstrapDBChunk segments off the same stream until one arrives
// with EOF=true. Each chunk lands in <target>.partial; on EOF the
// partial is atomic-renamed to <target>.
//
// Package-level alias so tests can stub the network call.
var bootstrap_db_fetch = bootstrap_db_fetch_impl

func bootstrap_db_fetch_impl(peer, scope, path, user, app, db string) error {
	if peer == "" {
		return fmt.Errorf("bootstrap-db-fetch: empty peer")
	}
	target, err := bootstrap_db_target_path(scope, path, user, app, db)
	if err != nil {
		return fmt.Errorf("bootstrap-db-fetch: target path: %w", err)
	}
	partial := target + ".partial"
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return fmt.Errorf("bootstrap-db-fetch: mkdir for %q: %w", target, err)
	}
	// Truncate any pre-existing partial so a resumed fetch doesn't
	// preserve stale bytes past the new EOF (different snapshots may
	// differ in length).
	f, err := os.OpenFile(partial, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("bootstrap-db-fetch: open partial %q: %w", partial, err)
	}
	defer f.Close()

	s, err := stream_to_peer(peer, "", "", "replication", "bootstrap/db/fetch", "", nil)
	if err != nil {
		return fmt.Errorf("bootstrap-db-fetch: open stream: %w", err)
	}
	defer s.close()

	// Mirror the source-side bump: long DB transfers can have a gap
	// between chunks if the source is reading a slow disk or the
	// libp2p stream is under backpressure. The default 30s read
	// deadline trips spuriously on multi-GB DBs.
	s.timeout.read = bootstrap_stream_timeout
	// Cumulative-bytes cap: the default cbor_max_size (100 MB) is the
	// io.LimitReader wrapping the decoder for the stream's lifetime.
	// A multi-GB DB transfer hits it at offset 100 MB and the decoder
	// returns EOF. Bump to bootstrap_stream_max_bytes so the whole DB
	// fits in one stream.
	s.max_bytes = bootstrap_stream_max_bytes

	if err := s.write(&BootstrapDBFetchRequest{
		Scope: scope, Path: path, User: user, App: app, DB: db,
	}); err != nil {
		return fmt.Errorf("bootstrap-db-fetch: write request: %w", err)
	}

	var seed int64
	var received int64
	for {
		var chunk BootstrapDBChunk
		if err := s.read(&chunk); err != nil {
			return fmt.Errorf("bootstrap-db-fetch: read chunk: %w", err)
		}
		if _, err := f.Seek(chunk.Offset, io.SeekStart); err != nil {
			return fmt.Errorf("bootstrap-db-fetch: seek partial %q: %w", partial, err)
		}
		n, err := f.Write(chunk.Data)
		if err != nil {
			return fmt.Errorf("bootstrap-db-fetch: write partial %q: %w", partial, err)
		}
		if n != len(chunk.Data) {
			return fmt.Errorf("bootstrap-db-fetch: short write at offset %d: wrote %d of %d", chunk.Offset, n, len(chunk.Data))
		}
		received += int64(n)
		if chunk.EOF {
			seed = chunk.Seed
			break
		}
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("bootstrap-db-fetch: close partial %q: %w", partial, err)
	}
	if err := bootstrap_db_land(partial, target); err != nil {
		return err
	}
	bootstrap_db_seed_cursor(peer, target, seed)
	bootstrap_progress_transfer(peer, "database", received)
	return nil
}

// replication_cursor_force sets the apply cursor unconditionally, overriding the
// monotonic max() guard in replication_cursor_set. Used ONLY by the bootstrap /
// reseed snapshot seed, where the snapshot's sequence point is authoritative and
// may legitimately be BELOW a stale cursor — a reseed after the source did a
// replica reset (which restarts its outbound sequence counter near zero) lands a
// snapshot at a low sequence, and the receiver's pre-reset cursor must rewind to
// match or it keeps dropping the source's resumed writes as already-applied (the
// replica-reset misalignment). The live apply path keeps using the monotonic
// _set so an out-of-order op can never rewind a healthy cursor.
func replication_cursor_force(db *DB, peer, scope, user, database string, sequence int64) {
	db.exec(
		"insert into cursor (peer, scope, user, db, sequence) values (?, ?, ?, ?, ?) "+
			"on conflict(peer, scope, user, db) do update set sequence=excluded.sequence",
		peer, scope, user, database, sequence)
}

// bootstrap_db_seed_cursor seeds the in-order apply cursor for a
// just-landed bootstrap DB at the snapshot's sequence point, then
// drains any live ops that buffered for that stream while the
// transfer was in flight. `target` is the absolute landed path; the
// stream key and owning user are derived from it so source and
// receiver agree without trusting the request fields. Uses the forcing
// cursor set: the snapshot point is authoritative and must override a
// stale (higher) cursor left by a pre-reset source.
func bootstrap_db_seed_cursor(peer, target string, seed int64) {
	rel := strings.TrimPrefix(target, data_dir+string(os.PathSeparator))
	stream := bootstrap_stream_key(rel)
	parts := strings.Split(filepath.ToSlash(rel), "/")
	if stream == "" || len(parts) < 2 || parts[0] != "users" {
		return
	}
	user := parts[1]
	rdb := db_open("db/replication.db")
	replication_cursor_force(rdb, peer, repl_scope_app, user, stream, seed)
	replication_stream_drain(rdb, peer, repl_scope_app, user, stream)
}

// bootstrap_db_reseed re-seeds ONE DB stream from `peer` on a live,
// populated replica — the targeted alternative to a full `replica
// reset` when a single stream has wedged on an anchored gap that won't
// self-heal (the feeds-stall class). It reuses the bootstrap snapshot
// primitive (fetch + atomic land + cursor re-anchor + buffered-op
// drain), then clears the state a re-seed leaves stale. The caller (the
// admin handler) owns the safety gate — see reseed_source_missing_ops.
func bootstrap_db_reseed(peer, scope, path string) error {
	target, err := bootstrap_db_target_path(scope, path, "", "", "")
	if err != nil {
		return err
	}
	if err := bootstrap_db_fetch_impl(peer, scope, path, "", "", ""); err != nil {
		return err
	}
	reseed_finalize(peer, target)
	return nil
}

// reseed_source_missing_ops reports whether the SOURCE peer is missing any op
// THIS host originated for `rel` — the data a re-seed (page-copy overwrite from
// the source) would silently discard. It replaces an earlier count-all gate that
// counted RETAINED-SHIPPED journal rows the source already holds (kept for
// backfill up to journal_retention_minimum), and so false-refused every active
// stream. The source is missing our ops iff EITHER (1) the journal holds un-sent
// `pending` ops, OR (2) it has not acked up to our emitted tail for this stream
// (its delivery cursor lags — e.g. its inbound replication dropped our shipped
// ops). A pure-receiver stream (no journal) or one the source has fully acked is
// safe to re-seed. The stream key / user are derivable only for a per-user
// app/core DB; for anything else the pending check alone is the gate.
func reseed_source_missing_ops(rel, peer string) bool {
	full := filepath.Join(data_dir, filepath.FromSlash(rel))
	if info, err := os.Stat(full); err != nil || info.IsDir() {
		return false
	}
	db := db_open(rel)
	if db == nil {
		return false
	}
	if has, _ := db.exists("select 1 from sqlite_master where type = 'table' and name = 'journal'"); !has {
		return false
	}
	// (1) Un-sent local writes: the source definitely lacks them.
	if db.integer("select count(*) from journal where state = 'pending'") > 0 {
		return true
	}
	// (2) Shipped ops: missing at the source only if it has not acked up to our
	// emitted tail (its delivery cursor lags our emitted sequence).
	stream := bootstrap_stream_key(rel)
	parts := strings.Split(filepath.ToSlash(rel), "/")
	if stream == "" || len(parts) < 2 || parts[0] != "users" {
		return false
	}
	user := parts[1]
	rdb := db_open("db/replication.db")
	if rdb == nil {
		return false
	}
	tail := rdb.integer("select coalesce(last, 0) from tail where user = ? and scope = ? and db = ?", user, repl_scope_app, stream)
	if tail <= 0 {
		return false // we emitted nothing — the source can't be missing our ops
	}
	return journal_delivery_cursor(rdb, user, peer, stream) < int64(tail)
}

// reseed_finalize clears the two things a re-seed leaves stale. First,
// the inherited journal: the snapshot carries the SOURCE's sender-state,
// and the receiver must not drain and re-ship those ops as if it had
// originated them — so the landed journal is emptied (the receiver
// starts fresh, recording its own ops only if it later writes that DB).
// Second, the stream's pending rows at or below the new cursor — ops
// that buffered behind the gap we just jumped, now dead.
// bootstrap_db_fetch_impl has already re-anchored the cursor and drained
// anything that chains onto it.
func reseed_finalize(peer, target string) {
	rel := strings.TrimPrefix(target, data_dir+string(os.PathSeparator))
	if db := db_open(rel); db != nil {
		if has, _ := db.exists("select 1 from sqlite_master where type = 'table' and name = 'journal'"); has {
			db.exec("delete from journal")
		}
	}
	stream := bootstrap_stream_key(rel)
	parts := strings.Split(filepath.ToSlash(rel), "/")
	if stream == "" || len(parts) < 2 || parts[0] != "users" {
		return
	}
	user := parts[1]
	rdb := db_open("db/replication.db")
	cursor, _ := replication_cursor(rdb, peer, repl_scope_app, user, stream)
	rdb.exec("delete from pending where peer = ? and scope = ? and user = ? and db = ? and sequence <= ?",
		peer, repl_scope_app, user, stream, cursor)
}

// bootstrap_db_land atomically installs the completed snapshot at
// `partial` as `target`. The snapshot is a self-contained SQLite
// online-backup output — no WAL needed. But `target` may already
// exist with a `-wal`/`-shm` from the server's prior use of that
// path. Renaming only the `.db` would leave the new snapshot beside
// the OLD write-ahead log; the next connection replays a log that
// belongs to the previous database and SQLite reports "database disk
// image is malformed" — which, raised inside a Starlark goroutine,
// crashed the server outright (2026-05-21). So the order is:
//  1. evict the cached handle so the server isn't pinned to the
//     pre-swap inode,
//  2. drop the stale -wal/-shm/-journal sidecars,
//  3. rename the snapshot into place.
//
// A fresh open then sees the snapshot with no sidecars and creates
// its own clean WAL.
func bootstrap_db_land(partial, target string) error {
	rel := strings.TrimPrefix(target, data_dir+string(os.PathSeparator))
	// Page-copy the snapshot INTO the live connection rather than evicting
	// the cached handle and renaming the file under it. The old evict+rename
	// closed the pooled handle mid-use — a borrower's next query hit "database
	// is closed" and panicked the worker — and left an evict→rename gap where
	// a concurrent re-open saw the old file with its WAL already deleted
	// ("database disk image is malformed"). Restore keeps the handle and its
	// WAL intact: SQLite replaces the destination's pages under its own
	// locking, and other pool connections pick up the new content on their
	// next read. db_open creates the file if absent, so a first-time landing
	// works too.
	db := db_open(rel)
	if db == nil {
		return fmt.Errorf("bootstrap-db-fetch: open target %q for restore", rel)
	}
	conn, err := db.internal.Conn(context.Background())
	if err != nil {
		return fmt.Errorf("bootstrap-db-fetch: restore conn for %q: %w", rel, err)
	}
	defer conn.Close()
	// Wait through brief writer contention on the destination rather than
	// failing the whole landing (WAL readers don't block the restore writer).
	if _, err := conn.ExecContext(context.Background(), "PRAGMA busy_timeout=10000"); err != nil {
		return fmt.Errorf("bootstrap-db-fetch: busy_timeout for %q: %w", rel, err)
	}
	source := "file:" + partial + "?mode=ro"
	restore_error := conn.Raw(func(driverConn any) error {
		dc, ok := driverConn.(sqlitedrv.Conn)
		if !ok {
			return fmt.Errorf("driver conn does not implement sqlitedrv.Conn")
		}
		return dc.Raw().Restore("main", source)
	})
	if restore_error != nil {
		return fmt.Errorf("bootstrap-db-fetch: restore %q into %q: %w", partial, rel, restore_error)
	}
	// The Restore replaced every page of the destination, including its `journal`
	// table — and a pure-receiver source has none (the apply path never creates one,
	// replication.go), so B's journal is now gone. Drop the stale journal_ensured
	// entry so the next journaled write re-runs journal_ensure and re-creates B's own
	// journal, instead of early-returning on the stale cache and failing every write
	// with "no such table: journal" (#424). Verified live: with this line the post-
	// reseed write re-creates the journal (cached=false → MISS); without it the write
	// fails (cached=true → early-return, table gone). Covers BOTH write paths, since
	// both gate on journal_ensure: execute via db_execute_journal and transaction via
	// api_db_transaction. Matches reseed_finalize's "receiver starts journal fresh".
	journal_ensured.Delete(db.path)
	_ = os.Remove(partial)
	return nil
}

// bootstrap_db_scope_driver runs in its own goroutine per (scope, peer)
// pair. Fetches each DB sequentially via stream RPC, calling
// bootstrap_pending_decrement after each completes. Failures drop the
// pending DB (the next operator-initiated resync re-fires the manifest
// and re-drives this loop). Sequential rather than parallel: rate
// limits are no longer a concern for paired peers, but bandwidth is
// finite and one DB transfer at a time gives a clean progress signal.
//
// Package-level variable so tests can stub the network drive entirely.
var bootstrap_db_scope_driver = bootstrap_db_scope_driver_impl

func bootstrap_db_scope_driver_impl(peer, scope string, entries []BootstrapDBEntry) {
	for _, entry := range entries {
		var lastErr error
		// Bounded exponential-backoff retry — same shape as the file
		// chunk-fetch retry. DB transfers are bigger so each retry is
		// more expensive, but transient flow-control or libp2p stream
		// hiccups are recoverable and a single attempt's failure
		// shouldn't strand a whole DB until operator resync.
		for attempt := 0; attempt < 4; attempt++ {
			if attempt > 0 {
				time.Sleep(time.Duration(1<<(attempt-1)) * time.Second)
				debug("Bootstrap db-scope driver retry: scope=%q path=%q attempt=%d",
					scope, entry.Path, attempt+1)
			}
			if err := bootstrap_db_fetch(peer, scope, entry.Path, entry.User, entry.App, entry.DB); err != nil {
				lastErr = err
				continue
			}
			lastErr = nil
			break
		}
		if lastErr != nil {
			info("Bootstrap db-scope driver: fetch failed after retries (scope=%q path=%q user=%q app=%q db=%q from=%q): %v",
				scope, entry.Path, entry.User, entry.App, entry.DB, peer, lastErr)
			// Settle path consults this counter to choose
			// 'incomplete' instead of 'done'. Retry manager fires
			// the manifest again later; the diff naturally picks up
			// the still-missing DB and re-drives this loop.
			bootstrap_failed_increment(scope, peer)
		}
		bootstrap_pending_decrement(scope, peer)
	}
}

// bootstrap_is_active_source returns true if there's a (scope, peer)
// row in replication.db.bootstrap that hasn't reached 'done'. Used
// by the chunk handlers as a defensive check: a chunk for a scope
// we're not actively bootstrapping from this peer is silently
// dropped. Prevents a malicious or buggy peer from writing arbitrary
// data into our scope roots (subject to bootstrap_safe_path's
// existing traversal guard).
func bootstrap_is_active_source(scope, peer string) bool {
	rdb := db_open("db/replication.db")
	exists, _ := rdb.exists("select 1 from bootstrap where scope=? and peer=? and state != 'done'", scope, peer)
	return exists
}

// bootstrap_in_progress reports whether ANY scope is still bootstrapping
// from `peer` (queued or active). Used by the stalled-stream recovery to
// stand down while a join's bulk transfer + keys-transfer backfill are
// still landing — escalating "operator re-join" or re-pulling a stream
// mid-join is a false alarm, since the join itself is the re-seed.
func bootstrap_in_progress(peer string) bool {
	rdb := db_open("db/replication.db")
	exists, _ := rdb.exists("select 1 from bootstrap where peer=? and state in ('queued','active')", peer)
	return exists
}

// bootstrap_resume picks up where the receiver left off across every
// (peer, scope) row that hasn't reached state='done'. Called from
// the server's startup hook so a crash mid-bootstrap doesn't leave
// the replica stuck in 'active' forever — on next boot every
// non-done row gets a fresh manifest-request fired against its peer,
// and the receiver-side diff skips files that already match by
// size + sha256 so the resume is incremental.
//
// Idempotent: running on a server with no active bootstrap rows is
// a no-op. Running while bootstrap is mid-flight is harmless (the
// in-flight transfer's manifests + chunks will land normally; the
// extra manifest-request just adds one round-trip).
func bootstrap_resume() {
	rdb := db_open("db/replication.db")
	rows, err := rdb.rows("select peer, scope from bootstrap where state != 'done'")
	if err != nil || len(rows) == 0 {
		return
	}
	for _, r := range rows {
		peer, _ := r["peer"].(string)
		scope, _ := r["scope"].(string)
		if peer == "" || scope == "" {
			continue
		}
		uid := bootstrap_peer_user(peer)
		bootstrap_refire_manifest(peer, scope, uid)
	}
	info("Replication bootstrap_resume: re-fired manifest-requests for %d non-done rows", len(rows))
}

// bootstrap_peer_user returns the user-uid filter to apply for a peer's
// bootstrap, or empty if this is a pair-join peer (whole-server).
// Looks up `replication.pair` first; if the peer is in the pair set
// it's a whole-server source. Otherwise checks `replication.hosts` for
// the per-user link case where one user has this peer in their host
// set. Returns "" if peer is in neither table (orphan row — caller
// treats as whole-server, which is a no-op since the peer won't reply).
func bootstrap_peer_user(peer string) string {
	if peer == "" {
		return ""
	}
	rdb := db_open("db/replication.db")
	if is_pair, _ := rdb.exists("select 1 from pair where peer=?", peer); is_pair {
		return ""
	}
	row, _ := rdb.row("select user from hosts where peer=? order by added asc limit 1", peer)
	if row == nil {
		return ""
	}
	uid, _ := row["user"].(string)
	return uid
}

// bootstrap_refire_manifest re-fires the appropriate manifest fetch for
// (scope, peer) with the per-user filter set when applicable. Shared by
// bootstrap_resume (server start, picks up where a crash left off) and
// bootstrap_retry_incomplete (recurring drain of failed transfers).
func bootstrap_refire_manifest(peer, scope, uid string) {
	switch scope {
	case bootstrap_scope_files:
		prefix := ""
		if uid != "" {
			prefix = uid + "/"
		}
		go replication_bootstrap_file_manifest_fetch(peer, scope, prefix)
	case bootstrap_scope_apps:
		// apps is whole-server only — per-user link doesn't subscribe to it.
		go replication_bootstrap_file_manifest_fetch(peer, scope, "")
	case bootstrap_scope_userdbs:
		go replication_bootstrap_db_manifest_fetch(peer, scope, uid)
	case bootstrap_scope_sysdbs:
		// sysdbs is whole-server only — per-user link doesn't include
		// it (sessions/login/etc. are server-local).
		go replication_bootstrap_db_manifest_fetch(peer, scope, "")
	}
}

// bootstrap_retry_incomplete_manager runs forever, periodically re-driving
// EVERY (scope, peer) row that hasn't reached 'done' — not just the ones
// that settled to 'incomplete'. A scope can be stuck non-done in three
// ways, and all three must eventually heal without an operator or a
// restart:
//
//   - queued     — the manifest-request never landed (e.g. the small
//     manifest read was starved by a multi-GB bulk transfer over the same
//     connection, or the source was briefly unreachable at join time).
//     Before this driver covered queued rows, a starved manifest left the
//     scope queued forever (observed live 2026-06-14: yuzu's files+apps
//     scopes sat queued through a 16 GB userdbs transfer and never
//     recovered without a re-wipe).
//   - active     — a transfer that stalled (the source went away mid-
//     stream) without settling. Distinguished from a *live* transfer by
//     its `progress` timestamp: a live one refreshes progress on every
//     chunk and is left alone; a stalled one is re-driven.
//   - incomplete — every entry was attempted but some failed.
//
// Each re-drive backs off per-row (bootstrap_retry_backoff) keyed on the
// `attempts` counter so an unreachable source isn't probed every tick;
// any real forward progress resets attempts to 0 (see bootstrap_set_state
// for state=active). Re-firing a manifest against an unreachable peer
// simply fails fast and is retried next pass, so connectivity needs no
// separate gate here — the periodic retry IS the wait-for-connection.
//
// Per-user link signup blocks the user on /login/replicating until the
// scope reaches 'done' (see bootstrap_wait_then_activate), so this
// manager is what eventually lets a user with a flaky network land on a
// complete dashboard instead of a half-empty one.
const bootstrap_retry_interval = 30 * time.Second

// bootstrap_stall_seconds is how long a scope may sit in 'active' with no
// forward progress before the retry driver treats it as stalled and
// re-drives it. Comfortably longer than the gap between chunks on a slow
// link so a live-but-slow transfer is never disturbed.
const bootstrap_stall_seconds = 120

func bootstrap_retry_incomplete_manager() {
	for {
		time.Sleep(bootstrap_retry_interval)
		bootstrap_retry_incomplete_once()
	}
}

// bootstrap_retry_backoff returns the minimum seconds since last progress
// before a scope with `attempts` consecutive failed retries is eligible
// again: 30s, 60s, 120s, … capped at 30 minutes. The cap keeps a long-
// unreachable source probed roughly twice an hour rather than abandoned.
func bootstrap_retry_backoff(attempts int64) int64 {
	const base = 30
	const maximum = 1800
	backoff := int64(base)
	for i := int64(0); i < attempts && backoff < maximum; i++ {
		backoff *= 2
	}
	if backoff > maximum {
		backoff = maximum
	}
	return backoff
}

// bootstrap_retry_eligible decides whether a non-done row should be
// re-driven this pass. idle is now-progress (seconds since last forward
// progress). A live 'active' transfer (idle < bootstrap_stall_seconds) is
// never disturbed; everything else waits out its per-row backoff.
func bootstrap_retry_eligible(state string, idle, attempts int64) bool {
	if state == bootstrap_state_active && idle < bootstrap_stall_seconds {
		return false
	}
	return idle >= bootstrap_retry_backoff(attempts)
}

// bootstrap_retry_incomplete_once does one pass: re-drive every eligible
// non-done row. Pulled out so tests can drive a single iteration without
// spinning the manager loop. (Name kept for back-compat; it now covers
// queued + stalled-active + incomplete, not only incomplete.)
func bootstrap_retry_incomplete_once() {
	rdb := db_open("db/replication.db")
	rows, err := rdb.rows("select peer, scope, state, progress, attempts from bootstrap where state != ?", bootstrap_state_done)
	if err != nil || len(rows) == 0 {
		return
	}
	for _, r := range rows {
		peer, _ := r["peer"].(string)
		scope, _ := r["scope"].(string)
		state, _ := r["state"].(string)
		if peer == "" || scope == "" {
			continue
		}
		progress, _ := r["progress"].(int64)
		attempts, _ := r["attempts"].(int64)
		if !bootstrap_retry_eligible(state, now()-progress, attempts) {
			continue
		}
		// Reset to queued and clear the per-pass failed counter so the new
		// manifest pass starts fresh — the file scope's pending_add would
		// otherwise stack a fresh page count on top of the stale one and
		// the scope would never reach pending==0. Bump attempts and stamp
		// progress so the row backs off and isn't re-fired again until its
		// next window; a real chunk landing resets attempts to 0 via
		// bootstrap_set_state(active).
		rdb.exec("update bootstrap set state='queued', position='', failed=0, attempts=attempts+1, progress=? where scope=? and peer=?", now(), scope, peer)
		uid := bootstrap_peer_user(peer)
		debug("Replication bootstrap-retry: scope=%q peer=%q uid=%q state=%q attempts=%d re-firing manifest", scope, peer, uid, state, attempts+1)
		bootstrap_refire_manifest(peer, scope, uid)
	}
}

// bootstrap_resume_peer re-drives every non-done (scope, peer) bootstrap
// row for one peer immediately, ignoring backoff — the on-demand analogue
// of the periodic retry driver. Safe to call on a *populated* replica: it
// only re-fires scopes that are not yet 'done', so it never re-fetches
// (and rename-replaces) a DB the running server already holds open. This
// is the crucial difference from `resync`, which wipes ALL bootstrap rows
// and re-bootstraps every scope from scratch — including the done ones —
// and so is refused on a populated host. Resume is what lets an operator
// (or an automatic reconnect) finish a bootstrap that completed some
// scopes (e.g. userdbs) but left others stuck (e.g. files+apps queued),
// which `resync` could not do without a full re-wipe.
//
// Called by `mochictl replication resume` and fired automatically when a
// stalled source becomes reachable again (replication_member_reachable).
// No-op (returns 0) if the peer has no non-done rows.
func bootstrap_resume_peer(peer string) int {
	if peer == "" {
		return 0
	}
	rdb := db_open("db/replication.db")
	rows, err := rdb.rows("select scope from bootstrap where peer=? and state != ?", peer, bootstrap_state_done)
	if err != nil || len(rows) == 0 {
		return 0
	}
	uid := bootstrap_peer_user(peer)
	count := 0
	for _, r := range rows {
		scope, _ := r["scope"].(string)
		if scope == "" {
			continue
		}
		// Clear backoff (attempts=0) so the kick fires now; a clean recount
		// (position='') so the file scope's pending_add doesn't stack.
		rdb.exec("update bootstrap set state='queued', position='', failed=0, attempts=0, progress=? where scope=? and peer=?", now(), scope, peer)
		bootstrap_refire_manifest(peer, scope, uid)
		count++
	}
	info("Replication bootstrap resume: re-fired %d non-done scope(s) for peer=%q", count, peer)
	return count
}

// bootstrap_progress tracks one in-flight bootstrap (a bootstrap_start
// or bootstrap_start_user call) so the log carries a single starting
// line and a single finished/incomplete summary line instead of a line
// per manifest, DB, and chunk; per-transfer failures keep their own
// info lines. Keyed by source peer — one bootstrap per peer at a time,
// matching the (scope, peer) keying of the bootstrap state table.
// In-memory only: a restart mid-bootstrap loses the summary (the
// resume path's refires log through their own lines), and settles for
// untracked (peer, scope) pairs — resume, retry-manager refires — are
// no-ops here.
type bootstrap_progress struct {
	user      string
	started   int64
	scopes    map[string]string // scope -> "" (pending) | done | incomplete
	databases int64
	files     int64
	bytes     int64
	failures  int64
}

var (
	bootstrap_progresses     = map[string]*bootstrap_progress{}
	bootstrap_progress_mutex sync.Mutex
)

// bootstrap_progress_start registers a tracked bootstrap and logs the
// starting line. A re-start for the same peer supersedes the prior
// entry (its summary is abandoned).
func bootstrap_progress_start(peer, user string, scopes []string) {
	pending := map[string]string{}
	for _, scope := range scopes {
		pending[scope] = ""
	}
	bootstrap_progress_mutex.Lock()
	bootstrap_progresses[peer] = &bootstrap_progress{user: user, started: now(), scopes: pending}
	bootstrap_progress_mutex.Unlock()
	replication_health_record_bootstrap()
	info("Replication bootstrap starting: peer=%q user=%q scopes=%q", peer, user, strings.Join(scopes, " "))
}

// bootstrap_progress_transfer adds one completed transfer to the
// peer's tracked bootstrap. kind is "database" or "file".
func bootstrap_progress_transfer(peer, kind string, bytes int64) {
	bootstrap_progress_mutex.Lock()
	defer bootstrap_progress_mutex.Unlock()
	p, ok := bootstrap_progresses[peer]
	if !ok {
		return
	}
	if kind == "database" {
		p.databases++
	} else {
		p.files++
	}
	p.bytes += bytes
}

// bootstrap_progress_settle records a scope's settled state. When the
// last tracked scope settles it logs the summary line and drops the
// entry: "finished" when every scope reached done, "incomplete" when
// any scope gave up on entries (the retry manager re-fires those
// scopes later).
func bootstrap_progress_settle(peer, scope, state string) {
	bootstrap_progress_mutex.Lock()
	p, ok := bootstrap_progresses[peer]
	if !ok {
		bootstrap_progress_mutex.Unlock()
		return
	}
	if _, tracked := p.scopes[scope]; !tracked {
		bootstrap_progress_mutex.Unlock()
		return
	}
	p.scopes[scope] = state
	if state == bootstrap_state_incomplete {
		p.failures += bootstrap_get_failed(scope, peer)
	}
	for _, s := range p.scopes {
		if s == "" {
			bootstrap_progress_mutex.Unlock()
			return
		}
	}
	delete(bootstrap_progresses, peer)
	bootstrap_progress_mutex.Unlock()

	outcome := "finished"
	for _, s := range p.scopes {
		if s != bootstrap_state_done {
			outcome = "incomplete"
		}
	}
	info("Replication bootstrap %s: peer=%q user=%q duration=%ds databases=%d files=%d bytes=%d failures=%d",
		outcome, peer, p.user, now()-p.started, p.databases, p.files, p.bytes, p.failures)
}

// ---- §2: manifests-first bulk ordering -------------------------------
//
// bootstrap_start fetches every scope's manifest BEFORE any bulk transfer
// begins. Without this, the first scope whose manifest lands (userdbs,
// typically the largest) starts streaming GBs immediately and saturates
// the shared connection, so the other scopes' small manifest READS time
// out and those scopes sit at 'queued' forever (observed live 2026-06-14:
// yuzu's files+apps starved behind a 16 GB userdbs transfer). Reading all
// manifests first means no bulk stream can ever block a manifest read.
//
// Mechanism: while a peer is in its manifest phase (between
// bootstrap_phase_begin and bootstrap_phase_end), the per-scope bulk
// drivers the manifest results would spawn are DEFERRED into the phase
// instead of run. bootstrap_phase_end (fired once every manifest fetch has
// returned) releases them under a bounded worker count. Callers that
// re-drive a single scope on their own schedule (resume / retry) don't
// open a phase, so their drives run immediately as before — they're
// already backstopped by the universal retry driver and don't have the
// all-scopes-starting-at-once contention.

// bootstrap_bulk_concurrency caps concurrent bulk-scope drivers as an
// anti-runaway bound — a heavily paginated file manifest can otherwise
// spawn one driver per page. Generous: it stops pathological fan-out
// without throttling the handful of drivers a normal bootstrap runs.
const bootstrap_bulk_concurrency = 16

var (
	bootstrap_bulk_sem = make(chan struct{}, bootstrap_bulk_concurrency)
	bootstrap_phase_mu sync.Mutex
	// bootstrap_phases holds, per peer, the bulk drives deferred during
	// that peer's manifest phase. A peer absent from the map is not in a
	// manifest phase — its drives run immediately.
	bootstrap_phases = map[string]*[]bootstrap_drive{}
)

// bootstrap_drive is a deferred bulk-scope drive plus the scope it belongs
// to, so bootstrap_bulk_run can refresh the right (peer, scope) row's
// progress while it waits for a concurrency slot (#33).
type bootstrap_drive struct {
	scope string
	run   func()
}

// bootstrap_bulk_touch is how often bootstrap_bulk_run refreshes a waiting
// scope's progress timestamp — half the stall window, so a scope queued on
// the concurrency semaphore stays comfortably "live" to the retry driver.
// A var so tests can shorten it.
var bootstrap_bulk_touch = (bootstrap_stall_seconds / 2) * time.Second

// bootstrap_phase_begin opens a manifest phase for `peer`: bulk drives are
// collected rather than run until bootstrap_phase_end.
func bootstrap_phase_begin(peer string) {
	bootstrap_phase_mu.Lock()
	deferred := []bootstrap_drive{}
	bootstrap_phases[peer] = &deferred
	bootstrap_phase_mu.Unlock()
}

// bootstrap_phase_drive runs `drive` now (under the concurrency bound), or
// defers it if `peer` is in a manifest phase. scope names the (peer, scope)
// row so the concurrency wait can keep its progress fresh (#33).
func bootstrap_phase_drive(peer, scope string, drive func()) {
	bootstrap_phase_mu.Lock()
	if deferred, ok := bootstrap_phases[peer]; ok {
		*deferred = append(*deferred, bootstrap_drive{scope: scope, run: drive})
		bootstrap_phase_mu.Unlock()
		return
	}
	bootstrap_phase_mu.Unlock()
	go bootstrap_bulk_run(peer, scope, drive)
}

// bootstrap_phase_end closes `peer`'s manifest phase and releases every
// drive deferred during it. No-op if the peer wasn't in a phase.
func bootstrap_phase_end(peer string) {
	bootstrap_phase_mu.Lock()
	deferred := bootstrap_phases[peer]
	delete(bootstrap_phases, peer)
	bootstrap_phase_mu.Unlock()
	if deferred == nil {
		return
	}
	for _, d := range *deferred {
		go bootstrap_bulk_run(peer, d.scope, d.run)
	}
}

// bootstrap_bulk_run runs one bulk-scope drive under the global concurrency
// bound. While it waits for a slot — e.g. queued behind a much larger scope
// holding all bootstrap_bulk_concurrency slots — it refreshes the (peer,
// scope) row's progress every bootstrap_bulk_touch so the retry driver sees
// the scope as starved (correctly waiting), not stalled, and doesn't
// needlessly re-fire its manifest (#33). Waiting on a local semaphore is not
// a failure: a vanished source surfaces when drive() runs, not here.
func bootstrap_bulk_run(peer, scope string, drive func()) {
acquire:
	for {
		select {
		case bootstrap_bulk_sem <- struct{}{}:
			break acquire
		case <-time.After(bootstrap_bulk_touch):
			bootstrap_progress_touch(peer, scope)
		}
	}
	defer func() { <-bootstrap_bulk_sem }()
	drive()
}

// bootstrap_progress_touch refreshes a scope's progress timestamp without
// otherwise changing the row, so a scope correctly waiting for a concurrency
// slot isn't mistaken for stalled by bootstrap_retry_eligible (#33).
func bootstrap_progress_touch(peer, scope string) {
	rdb := db_open("db/replication.db")
	rdb.exec("update bootstrap set progress=? where scope=? and peer=?", now(), scope, peer)
}

// bootstrap_start kicks off a whole-replica bootstrap from `peer`.
// Called from the join-approved handler on a fresh replica once the
// source has accepted the pair join; also exposed via mochictl for
// manual resume in case of interruption.
//
// Three scopes are file-snapshot-based and safe to atomic-rename
// because the receiver doesn't hold them open at pair-join time:
//   - files: users/<u>/<app>/files/<path>
//   - apps: apps/<entity>/<path>
//   - userdbs: users/<u>/<app>/db/<file>.db (opened on demand)
//
// System DBs (users.db / settings.db / apps.db / domains.db) are NOT
// bootstrapped as file snapshots — the running receiver holds them
// open from boot, and rename(2)-replacing the file out from under a
// live SQLite connection leaves the connection pinned to the now-
// unlinked original inode. The source handles these via
// replication_pair_backfill (row-by-row, fired from join-approved).
//
// Bootstrap is fully asynchronous: bootstrap_start returns
// immediately after firing the entry-point emits.
func bootstrap_start(peer string) {
	if peer == "" {
		return
	}
	bootstrap_progress_start(peer, "", []string{bootstrap_scope_files, bootstrap_scope_apps, bootstrap_scope_userdbs})
	// Capture the manifest-fetch hooks into locals before spawning. These
	// are package-level test seams; reading them from inside the goroutines
	// would race a test that restores its stubs on cleanup while a goroutine
	// is still in flight. The local read happens-before the `go`, so the
	// goroutine sees a stable value. See setup_replication_test.
	file_fetch := replication_bootstrap_file_manifest_fetch
	db_fetch := replication_bootstrap_db_manifest_fetch
	// Manifests first: open a phase so the per-scope bulk drivers are held
	// until every manifest has been fetched, then released together. See
	// the §2 block above bootstrap_start.
	bootstrap_phase_begin(peer)
	var wg sync.WaitGroup
	for _, scope := range []string{bootstrap_scope_files, bootstrap_scope_apps} {
		bootstrap_set_state(scope, peer, bootstrap_state_queued, "")
		wg.Add(1)
		go func(scope string) {
			defer wg.Done()
			file_fetch(peer, scope, "")
		}(scope)
	}
	bootstrap_set_state(bootstrap_scope_userdbs, peer, bootstrap_state_queued, "")
	wg.Add(1)
	go func() {
		defer wg.Done()
		db_fetch(peer, bootstrap_scope_userdbs, "")
	}()
	audit_replication_bootstrap_started(peer)
	// Release the deferred bulk drivers once every manifest has landed.
	// Async so bootstrap_start stays non-blocking.
	go func() {
		wg.Wait()
		bootstrap_phase_end(peer)
	}()
}

// bootstrap_start_user is the per-user link signup analogue of
// bootstrap_start: it pulls *only* one user's data from `peer`,
// suitable for the case where this server has accepted hosting a
// single user (not joined a whole-server pair).
//
// Scopes used:
//   - files:   prefix = "<uid>/" so the file walk is rooted at
//     users/<uid>/ on the source and only that user's uploads / blobs
//     come over.
//   - userdbs: with the User filter set so the manifest only lists
//     DBs under users/<uid>/.
//
// Skipped vs bootstrap_start:
//   - apps: whole-server, handled by the destination's own
//     apps_default install at boot (this peer's catalogue is
//     independent of which users are hosted here).
//   - sysdbs: not transferred by pair-join either; per-server.
//
// State is recorded under the same (scope, peer) keys as pair-join.
// In the current model a peer is either a pair member or a per-user
// link source, not both, so the keys don't collide.
//
// Fully asynchronous: returns immediately after firing the entry-point
// goroutines.
func bootstrap_start_user(peer, uid string) {
	if peer == "" || uid == "" {
		return
	}
	bootstrap_progress_start(peer, uid, []string{bootstrap_scope_files, bootstrap_scope_userdbs})
	// Capture the manifest-fetch hooks into locals before spawning, so the
	// goroutines don't read these package-level test seams directly and race
	// a test's stub-restore on cleanup. See bootstrap_start / the comment in
	// setup_replication_test.
	file_fetch := replication_bootstrap_file_manifest_fetch
	db_fetch := replication_bootstrap_db_manifest_fetch
	// Manifests first (see §2 block above bootstrap_start).
	bootstrap_phase_begin(peer)
	var wg sync.WaitGroup
	bootstrap_set_state(bootstrap_scope_files, peer, bootstrap_state_queued, "")
	wg.Add(1)
	go func() {
		defer wg.Done()
		file_fetch(peer, bootstrap_scope_files, uid+"/")
	}()
	bootstrap_set_state(bootstrap_scope_userdbs, peer, bootstrap_state_queued, "")
	wg.Add(1)
	go func() {
		defer wg.Done()
		db_fetch(peer, bootstrap_scope_userdbs, uid)
	}()
	audit_replication_bootstrap_started(peer)
	go func() {
		wg.Wait()
		bootstrap_phase_end(peer)
	}()
}

// bootstrap_walk_db_manifest enumerates every DB the source has for
// the requested scope. For userdbs three layouts are covered:
//   - users/<u>/user.db          — per-user infrastructure DB
//   - users/<u>/<app>/app.db     — per-app config DB (attachments,
//     access lists, etc.)
//   - users/<u>/<app>/db/*.db    — per-app data DB (feeds.db etc.)
//
// For sysdbs: every db/*.db at the top level (excluding the
// server-local DBs in bootstrap_sysdb_excluded).
//
// Only files matching bootstrap_db_basename_safe are included — junk
// files in the db dir are ignored. Symlinks and non-regular entries
// are skipped on the same principle as the file-tree walker.
// bootstrap_walk_db_manifest enumerates every DB the source has for
// the requested scope, optionally filtered to a single user (uid).
//
// userFilter:
//   - empty string → return every user's DBs (pair-join's whole-server
//     mirror).
//   - non-empty → return only DBs under users/<userFilter>/. Used by the
//     per-user link signup path so a peer that hosts one user's data
//     never sees another user's DB list.
//
// userFilter is ignored when scope == bootstrap_scope_sysdbs (sysdbs
// aren't per-user, and a filtered sysdb request is meaningless).
func bootstrap_walk_db_manifest(scope, userFilter string) ([]BootstrapDBEntry, error) {
	var entries []BootstrapDBEntry
	switch scope {
	case bootstrap_scope_userdbs:
		users_root := filepath.Join(data_dir, "users")
		user_entries, err := os.ReadDir(users_root)
		if err != nil {
			if os.IsNotExist(err) {
				return entries, nil
			}
			return nil, err
		}
		for _, u := range user_entries {
			if !u.IsDir() {
				continue
			}
			user := u.Name()
			if userFilter != "" && user != userFilter {
				continue
			}
			user_dir := filepath.Join(users_root, user)
			// Per-user infrastructure DBs at the user root
			// (users/<u>/*.db — e.g. user.db).
			root_entries, err := os.ReadDir(user_dir)
			if err != nil {
				continue
			}
			for _, r := range root_entries {
				if !r.Type().IsRegular() {
					continue
				}
				name := r.Name()
				if !bootstrap_db_basename_safe(name) {
					continue
				}
				entries = append(entries, BootstrapDBEntry{
					Path: filepath.Join("users", user, name),
					User: user,
					DB:   name,
				})
			}
			for _, a := range root_entries {
				if !a.IsDir() {
					continue
				}
				app := a.Name()
				app_dir := filepath.Join(user_dir, app)
				// Per-app config DB at the app root
				// (users/<u>/<app>/app.db).
				app_root_entries, err := os.ReadDir(app_dir)
				if err != nil {
					continue
				}
				for _, ar := range app_root_entries {
					if !ar.Type().IsRegular() {
						continue
					}
					name := ar.Name()
					if !bootstrap_db_basename_safe(name) {
						continue
					}
					entries = append(entries, BootstrapDBEntry{
						Path: filepath.Join("users", user, app, name),
						User: user,
						App:  app,
						DB:   name,
					})
				}
				// Per-app data DBs in users/<u>/<app>/db/.
				db_dir := filepath.Join(app_dir, "db")
				db_files, err := os.ReadDir(db_dir)
				if err != nil {
					continue
				}
				for _, f := range db_files {
					if !f.Type().IsRegular() {
						continue
					}
					name := f.Name()
					if !bootstrap_db_basename_safe(name) {
						continue
					}
					entries = append(entries, BootstrapDBEntry{
						Path: filepath.Join("users", user, app, "db", name),
						User: user,
						App:  app,
						DB:   name,
					})
				}
			}
		}
	case bootstrap_scope_sysdbs:
		db_dir := filepath.Join(data_dir, "db")
		files, err := os.ReadDir(db_dir)
		if err != nil {
			if os.IsNotExist(err) {
				return entries, nil
			}
			return nil, err
		}
		for _, f := range files {
			if !f.Type().IsRegular() {
				continue
			}
			name := f.Name()
			if !bootstrap_db_basename_safe(name) {
				continue
			}
			if bootstrap_sysdb_excluded[name] {
				continue
			}
			entries = append(entries, BootstrapDBEntry{
				Path: filepath.Join("db", name),
				DB:   name,
			})
		}
	default:
		return nil, fmt.Errorf("bootstrap: scope %q is not db-manifest-based", scope)
	}
	return entries, nil
}

// replication_bootstrap_db_manifest_fetch is the receiver-side
// orchestrator for a DB-manifest sync RPC. Opens a stream, writes the
// request, reads one response, applies it via the result-apply path.
// Designed to run in a goroutine.
//
// Package-level alias so tests can stub the network call.
var replication_bootstrap_db_manifest_fetch = replication_bootstrap_db_manifest_fetch_impl

// replication_bootstrap_db_manifest_fetch_impl opens a stream to peer
// and asks for the DB manifest for `scope`. `user` is the optional
// per-user filter — empty for pair-join (every user), non-empty for
// per-user link signup (only that uid's DBs).
func replication_bootstrap_db_manifest_fetch_impl(peer, scope, user string) {
	if peer == "" {
		return
	}
	s, err := stream_to_peer(peer, "", "", "replication", "bootstrap/db/manifest", "", nil)
	if err != nil {
		info("Replication bootstrap-db-manifest-fetch: open stream (scope=%q peer=%q): %v", scope, peer, err)
		return
	}
	defer s.close()
	// Building the DB manifest (walk + size/hash every DB) can also exceed
	// the default 30s read deadline on a host with many or large DBs; use
	// the same generous deadline as the file manifest + bulk transfers.
	s.timeout.read = bootstrap_stream_timeout

	if err := s.write(&BootstrapDBManifestRequest{Scope: scope, User: user}); err != nil {
		info("Replication bootstrap-db-manifest-fetch: write request (scope=%q peer=%q): %v", scope, peer, err)
		return
	}

	var res BootstrapDBManifestResult
	if err := s.read(&res); err != nil {
		info("Replication bootstrap-db-manifest-fetch: read response (scope=%q peer=%q): %v", scope, peer, err)
		return
	}
	replication_bootstrap_db_manifest_result_apply(peer, &res)
}

// replication_bootstrap_db_manifest_event is the sender-side stream
// handler. Reads the request, walks the appropriate root, writes the
// response back on the same stream.
func replication_bootstrap_db_manifest_event(e *Event) {
	if e.stream == nil {
		info("Replication bootstrap-db-manifest: no stream (queued retry?) — dropping")
		return
	}
	// Stream-RPC payload arrives as e.content (same pattern as the
	// file manifest + chunk-fetch handlers — calling e.segment here
	// would EOF because only one request segment was written).
	scope, _ := e.content["scope"].(string)
	user, _ := e.content["user"].(string)

	entries, err := bootstrap_walk_db_manifest(scope, user)
	if err != nil {
		info("Replication bootstrap-db-manifest: walk failed (scope=%q user=%q from=%q): %v",
			scope, user, e.peer, err)
		_ = e.stream.write(&BootstrapDBManifestResult{Scope: scope})
		return
	}
	_ = e.stream.write(&BootstrapDBManifestResult{Scope: scope, Entries: entries})
}

// replication_bootstrap_db_manifest_result_apply seeds the pending-DB
// counter and spawns a driver goroutine that fetches each DB
// sequentially via stream RPC. Empty result → scope is immediately
// 'done'. Replaces the earlier "fire one snapshot-request per entry"
// fan-out which fed every DB through queue.db and snowballed (every
// chunk of every DB was a queue row).
func replication_bootstrap_db_manifest_result_apply(originPeer string, res *BootstrapDBManifestResult) {
	if len(res.Entries) == 0 {
		bootstrap_set_state(res.Scope, originPeer, bootstrap_state_done, "")
		audit_replication_bootstrap_scope_done(originPeer, res.Scope)
		bootstrap_scope_settled(originPeer, res.Scope)
		bootstrap_progress_settle(originPeer, res.Scope, bootstrap_state_done)
		return
	}
	bootstrap_set_pending(res.Scope, originPeer, int64(len(res.Entries)))
	// Defer the bulk DB-fetch driver while `originPeer` is in its manifest
	// phase (bootstrap_start) so a multi-GB DB transfer can't start mid-
	// phase and starve another scope's manifest read.
	scope := res.Scope
	entries := res.Entries
	bootstrap_phase_drive(originPeer, scope, func() {
		bootstrap_db_scope_driver(originPeer, scope, entries)
	})
}

// ============================================================
// pair-join backfill (system DBs)
// ============================================================
//
// Bulk bootstrap's atomic-rename-of-snapshot strategy is correct for
// files, installed app code, and per-user app DBs (none held open by
// the running receiver at pair-join time). It is fundamentally broken
// for system DBs (db/users.db, db/apps.db, etc.) - those are opened
// during server startup and held for the process lifetime. A rename(2)
// swaps the directory entry but the receiver's open FD still points to
// the old inode. Pair-backfill copies system DB rows over the wire
// after the receiver is running, so the writes land in the SAME open
// connection.

// replication_pair_backfill runs on the source after approving a pair
// join. Direct-emits every replicated row to the new peer via the
// existing op channel. Idempotent on the receiver (REPLACE INTO + INSERT
// OR IGNORE in the apply handlers); a re-run after partial delivery
// just re-sends rows the receiver already has.
//
// Async: returns immediately. The actual messages are queued via
// message().send_peer() which the queue manager drains in the
// background.
//
// Package-level variable so tests can stub the backfill out — useful
// when a test only cares about the local-DB effect of join-approve
// and doesn't want the side-effect emit traffic.
var replication_pair_backfill = replication_pair_backfill_impl

func replication_pair_backfill_impl(peer string) {
	if peer == "" || peer == net_id {
		return
	}
	replication_pair_backfill_users(peer)
	replication_pair_backfill_system(peer)
	replication_pair_backfill_schedule(peer)
	info("Replication pair-backfill: dispatched users + system rows to peer %q", peer)
}

// replication_pair_backfill_schedule re-sends every user's scheduled events
// to the joining peer. schedule.db is a shared core DB that isn't snapshot-
// bootstrapped, and schedule rows replicate as self-anchoring idempotent ops
// (see replication_emit_to_real), so a fresh replica receives the EXISTING
// schedule only if we re-emit it at join time; live changes thereafter flow
// over the normal op channel. System events (user=="") are host-local and
// never replicated, so they're skipped. Idempotent on the receiver
// (replication_schedule_row_apply inserts only if absent).
func replication_pair_backfill_schedule(peer string) {
	sdb := schedule_db()
	rows, err := sdb.rows("select user, app, due, event, data, interval, created from schedule")
	if err != nil {
		warn("Replication pair-backfill schedule: enumerate failed: %v", err)
		return
	}
	count := 0
	for _, r := range rows {
		user, _ := r["user"].(string)
		app, _ := r["app"].(string)
		event, _ := r["event"].(string)
		if user == "" || app == "" || event == "" {
			continue // system events are host-local, not replicated
		}
		due, _ := r["due"].(int64)
		interval, _ := r["interval"].(int64)
		created, _ := r["created"].(int64)
		data, _ := r["data"].(string)
		op := &ReplicationOp{
			Scope:     repl_scope_app,
			User:      user,
			Database:  "schedule",
			Table:     "schedule",
			Operation: "schedule-row.set",
			Payload: cbor_encode(&ScheduleRow{
				Key: map[string]string{
					"user":    user,
					"app":     app,
					"event":   event,
					"created": fmt.Sprintf("%d", created),
				},
				Cols: map[string]string{
					"due":      fmt.Sprintf("%d", due),
					"data":     data,
					"interval": fmt.Sprintf("%d", interval),
				},
			}),
		}
		replication_emit_to_peer(user, op, peer)
		count++
	}
	debug("Replication pair-backfill: schedule rows sent to peer %q: %d", peer, count)
}

// replication_pair_backfill_users enumerates active users.db.users and
// emits a keys-transfer for each user to `peer`. Each keys-transfer
// carries the user's username + every owned entity (including private
// keys) — the receiver's keys-transfer handler creates the user row
// fresh when no local user with that username exists, then inserts
// each entity.
func replication_pair_backfill_users(peer string) {
	udb := db_open("db/users.db")
	rows, err := udb.rows("select uid, username, role from users where status = 'active'")
	if err != nil {
		warn("Replication pair-backfill users: enumerate failed: %v", err)
		return
	}
	count := 0
	seeded := 0
	for _, r := range rows {
		uid, _ := r["uid"].(string)
		if uid == "" {
			continue
		}
		if replication_transfer_keys_var(uid, peer) {
			count++
			continue
		}
		// No signing entity (a signup that hasn't created an identity yet):
		// keys-transfer skips them, but the bare user row must still reach the
		// partner so a replicated session resolves there (#34). Seed it via the
		// unsigned, peer-targeted pair system-row path (create-or-update).
		username, _ := r["username"].(string)
		if username == "" {
			continue
		}
		role, _ := r["role"].(string)
		replication_system_row_to_peer_var(peer, "users", "users",
			map[string]string{"uid": uid},
			map[string]string{"username": username, "role": role}, false)
		seeded++
	}
	debug("Replication pair-backfill: keys-transfer queued for %d users, bare-row seeded %d entity-less users, to peer %q", count, seeded, peer)
}

// replication_pair_backfill_system enumerates every replicated row of
// every replicated system table and emits it to `peer` via the
// single-target send helpers. Order matters for some tables (e.g.
// `apps.apps` install registry typically lands before per-app
// `versions` / `tracks` references), but the receiver's apply handlers
// are tolerant — out-of-order rows insert successfully when the schema
// doesn't enforce a FK ordering constraint.
func replication_pair_backfill_system(peer string) {
	// settings.db.settings
	sdb := db_open("db/settings.db")
	if rows, err := sdb.rows("select name, value from settings"); err == nil {
		for _, r := range rows {
			name, _ := r["name"].(string)
			value, _ := r["value"].(string)
			if name == "" {
				continue
			}
			replication_system_set_to_peer_var(peer, "settings", "settings", name, "value", value)
		}
	}

	// settings.db.documents: operator-edited rules / terms / privacy
	// overrides. Composite key (name, language); body + updated as the
	// row data.
	if rows, err := sdb.rows("select name, language, body, updated from documents"); err == nil {
		for _, r := range rows {
			name, _ := r["name"].(string)
			language, _ := r["language"].(string)
			if name == "" || language == "" {
				continue
			}
			body, _ := r["body"].(string)
			updated, _ := r["updated"].(int64)
			replication_system_row_to_peer_var(peer, "settings", "documents",
				map[string]string{"name": name, "language": language},
				map[string]string{"body": body, "updated": strconv.FormatInt(updated, 10)}, false)
		}
	}

	// apps.db two-column key tables (classes / services / paths).
	adb := db_apps()
	for _, t := range []struct{ table, keyCol string }{
		{"classes", "class"},
		{"services", "service"},
		{"paths", "path"},
	} {
		if rows, err := adb.rows(fmt.Sprintf("select %s as k, app from %s", t.keyCol, t.table)); err == nil {
			for _, r := range rows {
				key, _ := r["k"].(string)
				app, _ := r["app"].(string)
				if key == "" {
					continue
				}
				replication_system_set_to_peer_var(peer, "apps", t.table, key, "app", app)
			}
		}
	}

	// apps.db.apps (install registry): row=app, field='installed', value=timestamp.
	if rows, err := adb.rows("select app, installed from apps"); err == nil {
		for _, r := range rows {
			app, _ := r["app"].(string)
			if app == "" {
				continue
			}
			installed, _ := r["installed"].(int64)
			replication_system_set_to_peer_var(peer, "apps", "apps", app, "installed",
				strconv.FormatInt(installed, 10))
		}
	}

	// apps.db.versions: row-level, key=(app), cols={version, track}.
	if rows, err := adb.rows("select app, version, track from versions"); err == nil {
		for _, r := range rows {
			app, _ := r["app"].(string)
			if app == "" {
				continue
			}
			version, _ := r["version"].(string)
			track, _ := r["track"].(string)
			replication_system_row_to_peer_var(peer, "apps", "versions",
				map[string]string{"app": app},
				map[string]string{"version": version, "track": track}, false)
		}
	}

	// apps.db.tracks: row-level, key=(app, track), cols={version}.
	if rows, err := adb.rows("select app, track, version from tracks"); err == nil {
		for _, r := range rows {
			app, _ := r["app"].(string)
			track, _ := r["track"].(string)
			if app == "" || track == "" {
				continue
			}
			version, _ := r["version"].(string)
			replication_system_row_to_peer_var(peer, "apps", "tracks",
				map[string]string{"app": app, "track": track},
				map[string]string{"version": version}, false)
		}
	}

	// domains.db.domains: row-level, key=(domain).
	ddb := db_open("db/domains.db")
	if rows, err := ddb.rows("select domain, verified, token, tls, created, updated from domains"); err == nil {
		for _, r := range rows {
			domain, _ := r["domain"].(string)
			if domain == "" {
				continue
			}
			verified, _ := r["verified"].(int64)
			token, _ := r["token"].(string)
			tls, _ := r["tls"].(int64)
			created, _ := r["created"].(int64)
			updated, _ := r["updated"].(int64)
			replication_system_row_to_peer_var(peer, "domains", "domains",
				map[string]string{"domain": domain},
				map[string]string{
					"verified": strconv.FormatInt(verified, 10),
					"token":    token,
					"tls":      strconv.FormatInt(tls, 10),
					"created":  strconv.FormatInt(created, 10),
					"updated":  strconv.FormatInt(updated, 10),
				}, false)
		}
	}

	// domains.db.routes: row-level, key=(domain, path).
	if rows, err := ddb.rows("select domain, path, method, target, context, owner, priority, enabled, created, updated from routes"); err == nil {
		for _, r := range rows {
			domain, _ := r["domain"].(string)
			path, _ := r["path"].(string)
			if domain == "" || path == "" {
				continue
			}
			method, _ := r["method"].(string)
			target, _ := r["target"].(string)
			context, _ := r["context"].(string)
			owner, _ := r["owner"].(string)
			priority, _ := r["priority"].(int64)
			enabled, _ := r["enabled"].(int64)
			created, _ := r["created"].(int64)
			updated, _ := r["updated"].(int64)
			replication_system_row_to_peer_var(peer, "domains", "routes",
				map[string]string{"domain": domain, "path": path},
				map[string]string{
					"method":   method,
					"target":   target,
					"context":  context,
					"owner":    owner,
					"priority": strconv.FormatInt(priority, 10),
					"enabled":  strconv.FormatInt(enabled, 10),
					"created":  strconv.FormatInt(created, 10),
					"updated":  strconv.FormatInt(updated, 10),
				}, false)
		}
	}

	// domains.db.delegations: row-level, key=(domain, path, owner).
	if rows, err := ddb.rows("select domain, path, owner, created from delegations"); err == nil {
		for _, r := range rows {
			domain, _ := r["domain"].(string)
			path, _ := r["path"].(string)
			owner, _ := r["owner"].(string)
			if domain == "" || path == "" || owner == "" {
				continue
			}
			created, _ := r["created"].(int64)
			replication_system_row_to_peer_var(peer, "domains", "delegations",
				map[string]string{"domain": domain, "path": path, "owner": owner},
				map[string]string{"created": strconv.FormatInt(created, 10)}, false)
		}
	}

	replication_pair_backfill_sessions(peer)
	replication_pair_backfill_accounts(peer)
}

// replication_pair_backfill_accounts sends every replicable accounts
// row (per user) to the new peer via the exec-user-core op channel.
// Skips per-device types (browser, unifiedpush, fcm) — each device
// registers separately on each host. Ids are preserved by emitting
// "insert or replace" with the explicit local id, so destinations
// table rows referencing the account by integer id stay valid.
func replication_pair_backfill_accounts(peer string) {
	udb := db_open("db/users.db")
	users, err := udb.rows("select uid from users where status = 'active'")
	if err != nil {
		warn("Replication pair-backfill accounts: enumerate users failed: %v", err)
		return
	}
	total := 0
	for _, u := range users {
		uid, _ := u["uid"].(string)
		if uid == "" {
			continue
		}
		user := &User{UID: uid}
		db := db_user(user, "user")
		rows, err := db.rows("select id, type, label, identifier, data, created, verified from accounts where type not in ('browser', 'unifiedpush', 'fcm')")
		if err != nil {
			continue
		}
		for _, r := range rows {
			id, _ := r["id"].(int64)
			ptype, _ := r["type"].(string)
			if id == 0 || ptype == "" {
				continue
			}
			label, _ := r["label"].(string)
			identifier, _ := r["identifier"].(string)
			data, _ := r["data"].(string)
			created, _ := r["created"].(int64)
			verified, _ := r["verified"].(int64)
			replication_emit_user_core_exec_to_peer_var(peer, user,
				"insert or replace into accounts (id, type, label, identifier, data, created, verified) values (?, ?, ?, ?, ?, ?, ?)",
				[]any{id, ptype, label, identifier, data, created, verified})
			total++
		}
	}
	debug("Replication pair-backfill: %d account rows queued to peer %q", total, peer)
}

// replication_emit_user_core_exec_to_peer_var is the per-peer variant
// of replication_emit_user_core_exec. Used by pair-backfill to deliver
// rows to one specific peer rather than fanning out.
var replication_emit_user_core_exec_to_peer_var = replication_emit_user_core_exec_to_peer_impl

func replication_emit_user_core_exec_to_peer_impl(peer string, user *User, sql string, args []any) {
	if peer == "" || peer == net_id || user == nil || user.UID == "" {
		return
	}
	table := sql_target_table(sql)
	if table == "" {
		return
	}
	for _, prefix := range sql_default_excluded {
		if strings.HasPrefix(table, prefix) {
			return
		}
	}
	payload := cbor_encode(&SQLCommand{Statement: sql, Args: args})
	replication_emit_to_peer(user.UID, &ReplicationOp{
		Scope:     repl_scope_app,
		User:      user.UID,
		Database:  repl_db_user_core_sentinel,
		Table:     table,
		Operation: repl_op_exec_user_core,
		Payload:   payload,
	}, peer)
}

// replication_pair_backfill_sessions sends every active (non-expired)
// session row to the new peer via the same op channel login_create
// uses. The receiver's apply path is idempotent (REPLACE INTO), so
// re-runs after partial delivery are safe.
func replication_pair_backfill_sessions(peer string) {
	sdb := db_open("db/sessions.db")
	rows, err := sdb.rows("select user, code, secret, expires, created, accessed, address, agent from sessions where expires >= ?", now())
	if err != nil {
		warn("Replication pair-backfill sessions: enumerate failed: %v", err)
		return
	}
	count := 0
	for _, r := range rows {
		user_uid, _ := r["user"].(string)
		code, _ := r["code"].(string)
		if user_uid == "" || code == "" {
			continue
		}
		secret, _ := r["secret"].(string)
		expires, _ := r["expires"].(int64)
		created, _ := r["created"].(int64)
		accessed, _ := r["accessed"].(int64)
		address, _ := r["address"].(string)
		agent, _ := r["agent"].(string)
		replication_emit_session_insert_to_peer_var(peer, user_uid, code, secret, expires, created, accessed, address, agent)
		count++
	}
	debug("Replication pair-backfill: %d session rows queued to peer %q", count, peer)
}

// replication_emit_session_insert_to_peer_var is the per-peer variant
// of the live session-insert emit. Same payload shape; sends only to
// the targeted peer instead of fan-out to all pair members.
//
// Package-level variable so tests can stub the wire emission.
var replication_emit_session_insert_to_peer_var = replication_emit_session_insert_to_peer_impl

func replication_emit_session_insert_to_peer_impl(peer, user_uid, code, secret string, expires, created, accessed int64, address, agent string) {
	if peer == "" || peer == net_id || user_uid == "" {
		return
	}
	payload := cbor_encode(&SessionInsert{
		UserUID: user_uid, Code: code, Secret: secret,
		Expires: expires, Created: created, Accessed: accessed,
		Address: address, Agent: agent,
	})
	replication_emit_to_peer(user_uid, &ReplicationOp{
		Scope:     repl_scope_app,
		User:      user_uid,
		Database:  "sessions",
		Table:     "sessions",
		Operation: repl_op_insert,
		Payload:   payload,
	}, peer)
}

// replication_system_set_to_peer_var emits a SystemSet to a single peer
// (vs replication_emit_system_set which fans out to every pair member).
// Used by pair-backfill to direct-send rows to the newly-joining peer
// without flooding existing pair members with already-have-data noise.
//
// Package-level variable so tests can stub the wire emission.
var replication_system_set_to_peer_var = replication_system_set_to_peer_impl

func replication_system_set_to_peer(peer, database, table, row, field, value string) {
	replication_system_set_to_peer_var(peer, database, table, row, field, value)
}

func replication_system_set_to_peer_impl(peer, database, table, row, field, value string) {
	if peer == "" || peer == net_id {
		return
	}
	m := message("", "", "replication", "system/set")
	m.add(&SystemSet{
		Database: database, Table: table, Row: row, Field: field, Value: value,
	})
	m.send_peer(peer)
}

// replication_system_row_to_peer_var is the row-level companion to
// replication_system_set_to_peer_var.
var replication_system_row_to_peer_var = replication_system_row_to_peer_impl

func replication_system_row_to_peer(peer, database, table string, key, cols map[string]string, del bool) {
	replication_system_row_to_peer_var(peer, database, table, key, cols, del)
}

func replication_system_row_to_peer_impl(peer, database, table string, key, cols map[string]string, del bool) {
	if peer == "" || peer == net_id {
		return
	}
	m := message("", "", "replication", "system/row")
	m.add(&SystemRow{
		Database: database, Table: table, Key: key, Cols: cols, Delete: del,
	})
	m.send_peer(peer)
}

// replication_transfer_keys_var is the package-level alias for
// replication_transfer_keys, exposed so pair-backfill tests can stub
// the per-user emit out and just record which users were transferred.
var replication_transfer_keys_var = replication_transfer_keys

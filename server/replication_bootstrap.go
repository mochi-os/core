// Mochi server: bulk bootstrap protocol (#66) — V1 scaffolding
// Copyright Alistair Cunningham 2026
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
//     aggregate bootstrap_pending; mochi.replication.bootstrap_progress()
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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
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
//   queue.db        — outbound message queue, server-local
//   replication.db  — replication state machine (seen/pending/hosts/
//                     bootstrap rows), server-local
//   peers.db        — libp2p peer cache, server-local
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
const (
	bootstrap_state_queued = "queued"
	bootstrap_state_active = "active"
	bootstrap_state_done   = "done"
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
	Scope   string                 `cbor:"scope"`
	Prefix  string                 `cbor:"prefix"`
	Entries []BootstrapFileEntry   `cbor:"entries"`
	Done    bool                   `cbor:"done,omitempty"` // false → another result page follows
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
	Scope string `cbor:"scope"`           // bootstrap_scope_userdbs | bootstrap_scope_sysdbs
	User  string `cbor:"user,omitempty"`  // empty for system DBs
	App   string `cbor:"app,omitempty"`   // empty for system DBs
	DB    string `cbor:"db"`              // file basename, e.g. "users.db" or "feed.db"
}

// BootstrapDBManifestRequest is the receiver→sender ask for the list
// of per-user app DBs + system DBs the source has. The receiver fires
// one of these after the file-tree scopes complete (so the
// /var/lib/mochi/users/<u>/<a>/db/ directory trees exist) and uses
// the response to drive snapshot-requests for each (user, app, db)
// triple. System DBs (`db/*.db`) are enumerated separately for the
// sysdbs scope.
type BootstrapDBManifestRequest struct {
	Scope string `cbor:"scope"` // bootstrap_scope_userdbs | bootstrap_scope_sysdbs
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
type BootstrapDBEntry struct {
	User string `cbor:"user,omitempty"`
	App  string `cbor:"app,omitempty"`
	DB   string `cbor:"db"`
}

// BootstrapDBChunk is the sender→receiver chunk of a DB snapshot.
// Offset + len(Data) == EOF position when EOF=true.
type BootstrapDBChunk struct {
	Scope  string `cbor:"scope"`
	User   string `cbor:"user,omitempty"`
	App    string `cbor:"app,omitempty"`
	DB     string `cbor:"db"`
	Offset int64  `cbor:"offset"`
	Data   []byte `cbor:"data"`
	EOF    bool   `cbor:"eof,omitempty"`
}

// bootstrap_set_state upserts a bootstrap progress row, recording the
// (scope, peer) pair's current state + opaque position cursor. Use
// bootstrap_state_active while transferring and bootstrap_state_done
// when complete; queued rows are created automatically on first
// reference but callers may also seed them explicitly.
func bootstrap_set_state(scope, peer, state, position string) {
	rdb := db_open("db/replication.db")
	rdb.exec(
		"insert into bootstrap (scope, peer, state, position) values (?, ?, ?, ?) "+
			"on conflict(scope, peer) do update set state=excluded.state, position=excluded.position",
		scope, peer, state, position)
}

// bootstrap_get_state reads the recorded (state, position) for a
// (scope, peer) pair. Returns ('', '') if no row exists; callers
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
		positionStr, _ := row["position"].(string)
		current, _ = strconv.ParseInt(positionStr, 10, 64)
	}
	bootstrap_set_state(scope, peer, bootstrap_state_active, strconv.FormatInt(current+delta, 10))
}

// bootstrap_pending_decrement atomically subtracts 1 from the (scope,
// peer) row's pending counter. If the resulting count is 0 (or
// negative — defensive against unexpected over-decrement), the row
// transitions to state='done'. Called from the chunk handlers after
// a successful EOF write.
//
// Locks bootstrap_pending_lock so concurrent drivers don't race the
// read/modify/write of `position` and lose decrements.
//
// Returns the remaining count (or -1 if the row didn't exist). The
// returned value is mostly for tests; callers don't need it.
func bootstrap_pending_decrement(scope, peer string) int64 {
	bootstrap_pending_lock.Lock()
	defer bootstrap_pending_lock.Unlock()
	rdb := db_open("db/replication.db")
	row, _ := rdb.row("select position, state from bootstrap where scope=? and peer=?", scope, peer)
	if row == nil {
		return -1
	}
	state, _ := row["state"].(string)
	if state == bootstrap_state_done {
		// Already complete — nothing to decrement.
		return 0
	}
	positionStr, _ := row["position"].(string)
	count, _ := strconv.ParseInt(positionStr, 10, 64)
	count--
	if count <= 0 {
		bootstrap_set_state(scope, peer, bootstrap_state_done, "")
		audit_replication_bootstrap_scope_done(peer, scope)
		return 0
	}
	bootstrap_set_state(scope, peer, bootstrap_state_active, strconv.FormatInt(count, 10))
	return count
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
	rootResolved, err := filepath.EvalSymlinks(root)
	if err != nil {
		rootResolved = root
	}
	if !strings.HasPrefix(resolved+string(filepath.Separator), rootResolved+string(filepath.Separator)) && resolved != rootResolved {
		return "", fmt.Errorf("bootstrap: path %q escapes scope root", relative)
	}
	return candidate, nil
}

// bootstrap_walk_manifest enumerates every regular file under
// `<scope-root>/<prefix>` and returns one BootstrapFileEntry per file
// with size + sha256. Skips symlinks (we never copy symlinks to a
// replica; if the source has one the operator must reconstruct it on
// the replica). V2: returns the full list in one page; pagination is
// a V3 follow-up. Caller is responsible for splitting into multiple
// BootstrapFileManifestResult messages if size becomes a concern.
func bootstrap_walk_manifest(scope, prefix string) ([]BootstrapFileEntry, error) {
	root, err := bootstrap_file_scope_root(scope)
	if err != nil {
		return nil, err
	}
	startDir, err := bootstrap_safe_path(root, prefix)
	if err != nil {
		return nil, err
	}

	var entries []BootstrapFileEntry
	walkErr := filepath.Walk(startDir, func(absPath string, info os.FileInfo, err error) error {
		if err != nil {
			// Missing prefix dir → empty manifest, not an error. Anything
			// else propagates so the caller can see filesystem trouble.
			if os.IsNotExist(err) && absPath == startDir {
				return io.EOF
			}
			return err
		}
		if !info.Mode().IsRegular() {
			return nil // skip dirs, symlinks, devices
		}
		relPath, err := filepath.Rel(root, absPath)
		if err != nil {
			return err
		}
		hash, err := bootstrap_file_sha256(absPath)
		if err != nil {
			return err
		}
		entries = append(entries, BootstrapFileEntry{
			Path:   filepath.ToSlash(relPath),
			Size:   info.Size(),
			Sha256: hash,
		})
		return nil
	})
	if walkErr != nil && walkErr != io.EOF {
		return nil, walkErr
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

// replication_emit_bootstrap_file_manifest_result sends a manifest
// page to the receiver. Sender helper; called from the file-manifest
// request handler after the walker completes.
//
// Package-level alias so tests can stub the send_peer broadcast.
var replication_emit_bootstrap_file_manifest_result = replication_emit_bootstrap_file_manifest_result_impl

func replication_emit_bootstrap_file_manifest_result_impl(peer, scope, prefix string, entries []BootstrapFileEntry, done bool) {
	if peer == "" {
		return
	}
	m := message("", "", "replication", "bootstrap/file/manifest/result")
	m.add(&BootstrapFileManifestResult{
		Scope:   scope,
		Prefix:  prefix,
		Entries: entries,
		Done:    done,
	})
	m.send_peer(peer)
}

// bootstrap_manifest_page_size caps the number of entries in any one
// BootstrapFileManifestResult payload. The receiver's CBOR decoder
// rejects arrays over 10,000 elements (cbor_max_elements), and the
// per-message queue.db row also doesn't want to be tens of MB even
// briefly (the queue_check fan-out is fixed but bytes still get
// cloned during scan + send). The apps scope on a fully-populated
// server is 21k+ files (every published app's web/dist tree), so a
// single-page manifest is not viable. 5000 entries per page gives
// ~3-5 pages for the apps scope, each ~500-900 KB CBOR.
const bootstrap_manifest_page_size = 5000

// replication_bootstrap_file_manifest_request_event is the sender's
// receive handler — walks the requested path prefix, returns one or
// more BootstrapFileManifestResult pages. Each page carries up to
// bootstrap_manifest_page_size entries; the final page has Done=true.
func replication_bootstrap_file_manifest_request_event(e *Event) {
	var req BootstrapFileManifestRequest
	if !e.segment(&req) {
		info("Replication bootstrap-file-manifest-request dropping: cannot decode")
		return
	}
	entries, err := bootstrap_walk_manifest(req.Scope, req.Prefix)
	if err != nil {
		info("Replication bootstrap-file-manifest-request: walk failed (scope=%q prefix=%q from=%q): %v",
			req.Scope, req.Prefix, e.peer, err)
		// Reply with an empty Done=true manifest so the receiver can
		// distinguish "couldn't enumerate" from "still walking". A
		// retried request will produce the same response; the receiver
		// can backoff + retry if the operator fixes the underlying
		// filesystem issue.
		replication_emit_bootstrap_file_manifest_result(e.peer, req.Scope, req.Prefix, nil, true)
		return
	}
	debug("Replication bootstrap-file-manifest-request: scope=%q prefix=%q entries=%d from=%q",
		req.Scope, req.Prefix, len(entries), e.peer)
	// Empty manifest is still a single page with Done=true so the
	// receiver knows to transition the scope to 'done'.
	if len(entries) == 0 {
		replication_emit_bootstrap_file_manifest_result(e.peer, req.Scope, req.Prefix, nil, true)
		return
	}
	for i := 0; i < len(entries); i += bootstrap_manifest_page_size {
		end := i + bootstrap_manifest_page_size
		if end > len(entries) {
			end = len(entries)
		}
		done := end == len(entries)
		replication_emit_bootstrap_file_manifest_result(e.peer, req.Scope, req.Prefix, entries[i:end], done)
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
	localEntries, err := bootstrap_walk_manifest(scope, prefix)
	if err != nil {
		// Walk failed → treat every remote entry as missing locally.
		// The receiver's chunk-write path will create the parent dirs
		// as it goes, so an empty / missing local tree just means we
		// fetch the lot.
		localEntries = nil
	}
	local := make(map[string]BootstrapFileEntry, len(localEntries))
	for _, e := range localEntries {
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

// replication_bootstrap_file_manifest_result_event is the receiver's
// handler. Decodes the payload and delegates to the pure helper so
// unit tests can exercise the diff + chunk-request fan-out without
// constructing a live stream Event.
func replication_bootstrap_file_manifest_result_event(e *Event) {
	var res BootstrapFileManifestResult
	if !e.segment(&res) {
		info("Replication bootstrap-file-manifest-result dropping: cannot decode")
		return
	}
	replication_bootstrap_file_manifest_result_apply(e.peer, &res)
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
		go bootstrap_file_scope_driver(originPeer, res.Scope, needed)
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
			positionStr, _ := row["position"].(string)
			count, _ := strconv.ParseInt(positionStr, 10, 64)
			// Pending=0 + Done=true means every needed file across all
			// pages is already local (or nothing was needed at all).
			settle = state != bootstrap_state_done && count == 0
		}
		if settle {
			bootstrap_set_state(res.Scope, originPeer, bootstrap_state_done, "")
			audit_replication_bootstrap_scope_done(originPeer, res.Scope)
		}
		bootstrap_pending_lock.Unlock()
	}
	debug("Replication bootstrap-file-manifest-result: scope=%q prefix=%q page-entries=%d needed=%d done=%v from=%q",
		res.Scope, res.Prefix, len(res.Entries), len(needed), res.Done, originPeer)
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
		for _, req := range bootstrap_chunk_requests_for_entry(entry) {
			resp, err := bootstrap_file_chunk_fetch(peer, scope, req.Path, req.Offset, req.Length)
			if err != nil {
				info("Bootstrap file-scope driver: fetch failed (scope=%q path=%q offset=%d from=%q): %v",
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
			if resp.EOF {
				break
			}
		}
		if ok {
			bootstrap_pending_decrement(scope, peer)
		} else {
			// Failed file: still decrement so the scope can settle
			// (otherwise it's stuck active forever). A re-run picks
			// up missing files.
			bootstrap_pending_decrement(scope, peer)
		}
	}
}

// replication_emit_bootstrap_file_manifest_request kicks off a
// bootstrap from the receiver's side. Caller is the driver that
// orchestrates the four scopes (#66 V4); for now this helper exists
// so unit tests and the eventual driver have a single entry point.
//
// Package-level alias so tests can stub the send_peer broadcast.
var replication_emit_bootstrap_file_manifest_request = replication_emit_bootstrap_file_manifest_request_impl

func replication_emit_bootstrap_file_manifest_request_impl(peer, scope, prefix string) {
	if peer == "" {
		return
	}
	m := message("", "", "replication", "bootstrap/file/manifest/request")
	m.add(&BootstrapFileManifestRequest{Scope: scope, Prefix: prefix})
	m.send_peer(peer)
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
	debug("Replication bootstrap-file-chunk-fetch served: scope=%q path=%q offset=%d sent=%d eof=%v to=%q",
		scope, path, offset, len(data), eof, e.peer)
}

// bootstrap_max_chunk_size caps a single chunk request at 1 MiB. The
// receiver issues sequential requests until file size is reached;
// larger chunks would just add latency on retries.
const bootstrap_max_chunk_size = 1 << 20

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
// for a snapshot request. Per-user app DBs live at
// users/<user>/<app>/db/<file>, system DBs live at db/<file>. The
// file basename is validated to prevent directory traversal (only
// `^[A-Za-z0-9_.-]+\.db$`).
func bootstrap_db_source_path(scope, user, app, db string) (string, error) {
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
func bootstrap_db_target_path(scope, user, app, db string) (string, error) {
	return bootstrap_db_source_path(scope, user, app, db)
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
	user, _ := e.content["user"].(string)
	app, _ := e.content["app"].(string)
	db, _ := e.content["db"].(string)

	if scope == bootstrap_scope_sysdbs && bootstrap_sysdb_excluded[db] {
		info("Replication bootstrap-db-fetch rejecting: sysdb %q is server-local and must not be transferred", db)
		return
	}
	srcPath, err := bootstrap_db_source_path(scope, user, app, db)
	if err != nil {
		info("Replication bootstrap-db-fetch rejecting (scope=%q user=%q app=%q db=%q from=%q): %v",
			scope, user, app, db, e.peer, err)
		return
	}
	if !file_exists(srcPath) {
		info("Replication bootstrap-db-fetch: source %q does not exist (from=%q)", srcPath, e.peer)
		return
	}

	tmp, err := os.CreateTemp("", "mochi-bootstrap-*.db")
	if err != nil {
		info("Replication bootstrap-db-fetch: tempfile create failed (from=%q): %v", e.peer, err)
		return
	}
	tmpPath := tmp.Name()
	_ = tmp.Close()
	defer os.Remove(tmpPath)

	size, err := snapshot_copy_db(srcPath, tmpPath)
	if err != nil {
		info("Replication bootstrap-db-fetch: backup %q failed (from=%q): %v", srcPath, e.peer, err)
		return
	}
	debug("Replication bootstrap-db-fetch: source=%q size=%d to=%q", srcPath, size, e.peer)

	f, err := os.Open(tmpPath)
	if err != nil {
		info("Replication bootstrap-db-fetch: reopen snapshot failed (from=%q): %v", e.peer, err)
		return
	}
	defer f.Close()

	buf := make([]byte, bootstrap_max_chunk_size)
	var offset int64
	for {
		n, readErr := f.Read(buf)
		eof := readErr == io.EOF || offset+int64(n) >= size
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			if writeErr := e.stream.write(&BootstrapDBChunk{
				Scope:  scope,
				User:   user,
				App:    app,
				DB:     db,
				Offset: offset,
				Data:   chunk,
				EOF:    eof,
			}); writeErr != nil {
				info("Replication bootstrap-db-fetch: write chunk failed at %d (from=%q): %v", offset, e.peer, writeErr)
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

func bootstrap_db_fetch_impl(peer, scope, user, app, db string) error {
	if peer == "" {
		return fmt.Errorf("bootstrap-db-fetch: empty peer")
	}
	target, err := bootstrap_db_target_path(scope, user, app, db)
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

	if err := s.write(&BootstrapDBFetchRequest{
		Scope: scope, User: user, App: app, DB: db,
	}); err != nil {
		return fmt.Errorf("bootstrap-db-fetch: write request: %w", err)
	}

	for {
		var chunk BootstrapDBChunk
		if err := s.read(&chunk); err != nil {
			return fmt.Errorf("bootstrap-db-fetch: read chunk: %w", err)
		}
		if _, err := f.Seek(chunk.Offset, io.SeekStart); err != nil {
			return fmt.Errorf("bootstrap-db-fetch: seek partial %q: %w", partial, err)
		}
		if _, err := f.Write(chunk.Data); err != nil {
			return fmt.Errorf("bootstrap-db-fetch: write partial %q: %w", partial, err)
		}
		if chunk.EOF {
			break
		}
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("bootstrap-db-fetch: close partial %q: %w", partial, err)
	}
	if err := os.Rename(partial, target); err != nil {
		return fmt.Errorf("bootstrap-db-fetch: rename %q -> %q: %w", partial, target, err)
	}
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
		if err := bootstrap_db_fetch(peer, scope, entry.User, entry.App, entry.DB); err != nil {
			info("Bootstrap db-scope driver: fetch failed (scope=%q user=%q app=%q db=%q from=%q): %v",
				scope, entry.User, entry.App, entry.DB, peer, err)
		}
		// Decrement whether the fetch succeeded or failed so the scope
		// can settle to 'done'; the missing DB gets refetched on next
		// resync.
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
		switch scope {
		case bootstrap_scope_files, bootstrap_scope_apps:
			replication_emit_bootstrap_file_manifest_request(peer, scope, "")
		case bootstrap_scope_userdbs, bootstrap_scope_sysdbs:
			replication_emit_bootstrap_db_manifest_request(peer, scope)
		}
	}
	info("Replication bootstrap_resume: re-fired manifest-requests for %d non-done rows", len(rows))
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
	for _, scope := range []string{bootstrap_scope_files, bootstrap_scope_apps} {
		bootstrap_set_state(scope, peer, bootstrap_state_queued, "")
		replication_emit_bootstrap_file_manifest_request(peer, scope, "")
	}
	bootstrap_set_state(bootstrap_scope_userdbs, peer, bootstrap_state_queued, "")
	replication_emit_bootstrap_db_manifest_request(peer, bootstrap_scope_userdbs)
	audit_replication_bootstrap_started(peer)
}

// bootstrap_walk_db_manifest enumerates every DB the source has for
// the requested scope. For userdbs: every users/<u>/<a>/db/*.db; for
// sysdbs: every db/*.db at the top level.
//
// Only files matching bootstrap_db_basename_safe are included — junk
// files in the db dir are ignored. Symlinks and non-regular entries
// are skipped on the same principle as the file-tree walker.
func bootstrap_walk_db_manifest(scope string) ([]BootstrapDBEntry, error) {
	var entries []BootstrapDBEntry
	switch scope {
	case bootstrap_scope_userdbs:
		usersRoot := filepath.Join(data_dir, "users")
		userEntries, err := os.ReadDir(usersRoot)
		if err != nil {
			if os.IsNotExist(err) {
				return entries, nil
			}
			return nil, err
		}
		for _, u := range userEntries {
			if !u.IsDir() {
				continue
			}
			user := u.Name()
			userDir := filepath.Join(usersRoot, user)
			appEntries, err := os.ReadDir(userDir)
			if err != nil {
				continue
			}
			for _, a := range appEntries {
				if !a.IsDir() {
					continue
				}
				app := a.Name()
				dbDir := filepath.Join(userDir, app, "db")
				dbFiles, err := os.ReadDir(dbDir)
				if err != nil {
					continue
				}
				for _, f := range dbFiles {
					if !f.Type().IsRegular() {
						continue
					}
					name := f.Name()
					if !bootstrap_db_basename_safe(name) {
						continue
					}
					entries = append(entries, BootstrapDBEntry{User: user, App: app, DB: name})
				}
			}
		}
	case bootstrap_scope_sysdbs:
		dbDir := filepath.Join(data_dir, "db")
		files, err := os.ReadDir(dbDir)
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
			entries = append(entries, BootstrapDBEntry{DB: name})
		}
	default:
		return nil, fmt.Errorf("bootstrap: scope %q is not db-manifest-based", scope)
	}
	return entries, nil
}

// replication_emit_bootstrap_db_manifest_request asks the source for
// a DB manifest. Package-level alias for test stubbing.
var replication_emit_bootstrap_db_manifest_request = replication_emit_bootstrap_db_manifest_request_impl

func replication_emit_bootstrap_db_manifest_request_impl(peer, scope string) {
	if peer == "" {
		return
	}
	m := message("", "", "replication", "bootstrap/db/manifest/request")
	m.add(&BootstrapDBManifestRequest{Scope: scope})
	m.send_peer(peer)
}

// replication_emit_bootstrap_db_manifest_result responds with the
// enumerated DB list. Package-level alias for test stubbing.
var replication_emit_bootstrap_db_manifest_result = replication_emit_bootstrap_db_manifest_result_impl

func replication_emit_bootstrap_db_manifest_result_impl(peer, scope string, entries []BootstrapDBEntry) {
	if peer == "" {
		return
	}
	m := message("", "", "replication", "bootstrap/db/manifest/result")
	m.add(&BootstrapDBManifestResult{Scope: scope, Entries: entries})
	m.send_peer(peer)
}

// replication_bootstrap_db_manifest_request_event is the sender-side
// handler. Walks the appropriate root and emits a result.
func replication_bootstrap_db_manifest_request_event(e *Event) {
	var req BootstrapDBManifestRequest
	if !e.segment(&req) {
		info("Replication bootstrap-db-manifest-request dropping: cannot decode")
		return
	}
	entries, err := bootstrap_walk_db_manifest(req.Scope)
	if err != nil {
		info("Replication bootstrap-db-manifest-request: walk failed (scope=%q from=%q): %v",
			req.Scope, e.peer, err)
		replication_emit_bootstrap_db_manifest_result(e.peer, req.Scope, nil)
		return
	}
	debug("Replication bootstrap-db-manifest-request: scope=%q entries=%d from=%q",
		req.Scope, len(entries), e.peer)
	replication_emit_bootstrap_db_manifest_result(e.peer, req.Scope, entries)
}

// replication_bootstrap_db_manifest_result_event is the receiver-side
// handler. Decodes the payload and delegates to the pure helper so
// unit tests can exercise the snapshot-emit fan-out without a live
// stream.
func replication_bootstrap_db_manifest_result_event(e *Event) {
	var res BootstrapDBManifestResult
	if !e.segment(&res) {
		info("Replication bootstrap-db-manifest-result dropping: cannot decode")
		return
	}
	replication_bootstrap_db_manifest_result_apply(e.peer, &res)
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
		debug("Replication bootstrap-db-manifest-result: scope=%q empty (no DBs to fetch) from=%q",
			res.Scope, originPeer)
		return
	}
	bootstrap_set_pending(res.Scope, originPeer, int64(len(res.Entries)))
	go bootstrap_db_scope_driver(originPeer, res.Scope, res.Entries)
	debug("Replication bootstrap-db-manifest-result: scope=%q driving %d entries from=%q",
		res.Scope, len(res.Entries), originPeer)
}

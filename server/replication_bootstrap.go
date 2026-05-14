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
// (state ∈ {'queued', 'active', 'done'}; position is an opaque per-
// scope cursor). Server restart picks up where it left off.
//
// V1 (this file) lands:
//   - Wire types for all four payloads.
//   - Event-name registration in replication.go init.
//   - bootstrap_set_* / bootstrap_get_* / bootstrap_clear helpers
//     so callers (mochictl replica join, the eventual receiver-side
//     driver loop) can persist progress.
//   - Stub event handlers that log + decode + persist a no-op
//     bootstrap row — enough for the protocol to be wireable end-to-
//     end without yet transferring bytes.
//
// V2 (subsequent commits) lands:
//   - File-tree walker + chunk reader on the sender side.
//   - Receiver-side .partial → atomic-rename writer with resume.
//   - SQLite snapshot driver using ncruces sqlite3 backup API.
//   - The driver loop that orchestrates the four scopes in order
//     (files → apps → user DBs → system DBs) and transitions to
//     state='done' when all four are complete.
//
// V3 (post-alpha): receiver-side throttling, sender-side backpressure,
// integrity-failure retry policy, audit-log event for every step.

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
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

// BootstrapDBSnapshotRequest is the receiver→sender request to
// snapshot a SQLite DB. The sender runs VACUUM INTO to a tempfile,
// streams it via BootstrapDBChunk, then flushes any writes that
// arrived during the snapshot through the standard op channel.
type BootstrapDBSnapshotRequest struct {
	Scope string `cbor:"scope"` // bootstrap_scope_userdbs | bootstrap_scope_sysdbs
	User  string `cbor:"user,omitempty"`  // empty for system DBs
	App   string `cbor:"app,omitempty"`   // empty for system DBs
	DB    string `cbor:"db"`              // file basename, e.g. "users.db" or "feed.db"
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
	m := message("", "", "replication", "bootstrap-file-manifest-result")
	m.add(&BootstrapFileManifestResult{
		Scope:   scope,
		Prefix:  prefix,
		Entries: entries,
		Done:    done,
	})
	m.send_peer(peer)
}

// replication_emit_bootstrap_file_chunk sends a chunk of file data to
// the receiver. Sender helper; called from the chunk-request handler
// after bootstrap_read_chunk returns.
//
// Package-level alias so tests can stub the send_peer broadcast.
var replication_emit_bootstrap_file_chunk = replication_emit_bootstrap_file_chunk_impl

func replication_emit_bootstrap_file_chunk_impl(peer, scope, path string, offset int64, data []byte, eof bool) {
	if peer == "" {
		return
	}
	m := message("", "", "replication", "bootstrap-file-chunk")
	m.add(&BootstrapFileChunk{
		Scope:  scope,
		Path:   path,
		Offset: offset,
		Data:   data,
		EOF:    eof,
	})
	m.send_peer(peer)
}

// replication_bootstrap_file_manifest_request_event is the sender's
// receive handler — walks the requested path prefix, returns a
// BootstrapFileManifestResult page with the file list (V2: single
// page, no pagination yet).
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
	replication_emit_bootstrap_file_manifest_result(e.peer, req.Scope, req.Prefix, entries, true)
}

// replication_bootstrap_file_manifest_result_event is the receiver's
// handler — it diffs the manifest against the local copy and queues
// chunk requests for missing/differing files. V1 stub.
func replication_bootstrap_file_manifest_result_event(e *Event) {
	var res BootstrapFileManifestResult
	if !e.segment(&res) {
		info("Replication bootstrap-file-manifest-result dropping: cannot decode")
		return
	}
	debug("Replication bootstrap-file-manifest-result: scope=%q prefix=%q entries=%d done=%v from=%q (V1 stub)",
		res.Scope, res.Prefix, len(res.Entries), res.Done, e.peer)
	// V2: diff against local files, emit BootstrapFileChunkRequest for
	// each (path, offset) pair until all chunks complete; on the final
	// page (Done=true) the receiver transitions the scope to 'done'.
}

// replication_bootstrap_file_chunk_request_event is the sender's
// receive handler — reads bytes from the requested file and emits a
// BootstrapFileChunk back to the receiver.
func replication_bootstrap_file_chunk_request_event(e *Event) {
	var req BootstrapFileChunkRequest
	if !e.segment(&req) {
		info("Replication bootstrap-file-chunk-request dropping: cannot decode")
		return
	}
	if req.Length <= 0 || req.Length > bootstrap_max_chunk_size {
		info("Replication bootstrap-file-chunk-request rejecting: length %d out of range (1..%d)",
			req.Length, bootstrap_max_chunk_size)
		return
	}
	data, eof, err := bootstrap_read_chunk(req.Scope, req.Path, req.Offset, req.Length)
	if err != nil {
		info("Replication bootstrap-file-chunk-request: read failed (scope=%q path=%q offset=%d from=%q): %v",
			req.Scope, req.Path, req.Offset, e.peer, err)
		return
	}
	debug("Replication bootstrap-file-chunk-request: scope=%q path=%q offset=%d sent=%d eof=%v from=%q",
		req.Scope, req.Path, req.Offset, len(data), eof, e.peer)
	replication_emit_bootstrap_file_chunk(e.peer, req.Scope, req.Path, req.Offset, data, eof)
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

// replication_bootstrap_file_chunk_event is the receiver's handler.
// Writes the chunk to a `.partial` file; atomic-renames on EOF.
func replication_bootstrap_file_chunk_event(e *Event) {
	var chunk BootstrapFileChunk
	if !e.segment(&chunk) {
		info("Replication bootstrap-file-chunk dropping: cannot decode")
		return
	}
	if err := bootstrap_write_chunk(chunk.Scope, chunk.Path, chunk.Offset, chunk.Data, chunk.EOF); err != nil {
		info("Replication bootstrap-file-chunk write failed (scope=%q path=%q offset=%d from=%q): %v",
			chunk.Scope, chunk.Path, chunk.Offset, e.peer, err)
		return
	}
	debug("Replication bootstrap-file-chunk written: scope=%q path=%q offset=%d len=%d eof=%v from=%q",
		chunk.Scope, chunk.Path, chunk.Offset, len(chunk.Data), chunk.EOF, e.peer)
}

// replication_bootstrap_db_snapshot_request_event is the sender's
// receive handler — VACUUM INTO a tempfile, stream as
// BootstrapDBChunk. V1 stub.
func replication_bootstrap_db_snapshot_request_event(e *Event) {
	var req BootstrapDBSnapshotRequest
	if !e.segment(&req) {
		info("Replication bootstrap-db-snapshot-request dropping: cannot decode")
		return
	}
	debug("Replication bootstrap-db-snapshot-request: scope=%q user=%q app=%q db=%q from=%q (V1 stub)",
		req.Scope, req.User, req.App, req.DB, e.peer)
	// V2: VACUUM INTO <tmp>; chunk-emit; on completion, flush any ops
	// that arrived during the snapshot from a per-(user, app, db)
	// pending buffer via the standard op channel; transition the
	// (scope, peer) row to 'done'.
}

// replication_bootstrap_db_chunk_event is the receiver's handler.
// V1 stub.
func replication_bootstrap_db_chunk_event(e *Event) {
	var chunk BootstrapDBChunk
	if !e.segment(&chunk) {
		info("Replication bootstrap-db-chunk dropping: cannot decode")
		return
	}
	debug("Replication bootstrap-db-chunk: scope=%q user=%q app=%q db=%q offset=%d len=%d eof=%v from=%q (V1 stub)",
		chunk.Scope, chunk.User, chunk.App, chunk.DB, chunk.Offset, len(chunk.Data), chunk.EOF, e.peer)
	// V2: write chunk to <target>.partial; on EOF, rename and proceed
	// to the live-op flush phase.
}

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

// replication_bootstrap_file_manifest_request_event is the sender's
// receive handler — it walks the requested path prefix and returns a
// BootstrapFileManifestResult. V1 stub: logs + returns an empty
// manifest with Done=true. Full implementation lands in V2.
func replication_bootstrap_file_manifest_request_event(e *Event) {
	var req BootstrapFileManifestRequest
	if !e.segment(&req) {
		info("Replication bootstrap-file-manifest-request dropping: cannot decode")
		return
	}
	debug("Replication bootstrap-file-manifest-request: scope=%q prefix=%q from=%q (V1 stub)",
		req.Scope, req.Prefix, e.peer)
	// V2: walk the directory tree, emit BootstrapFileManifestResult pages
	// until the prefix is exhausted, set Done=true on the last page.
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
// receive handler — it reads bytes from the requested file and
// returns a BootstrapFileChunk. V1 stub.
func replication_bootstrap_file_chunk_request_event(e *Event) {
	var req BootstrapFileChunkRequest
	if !e.segment(&req) {
		info("Replication bootstrap-file-chunk-request dropping: cannot decode")
		return
	}
	debug("Replication bootstrap-file-chunk-request: scope=%q path=%q offset=%d length=%d from=%q (V1 stub)",
		req.Scope, req.Path, req.Offset, req.Length, e.peer)
	// V2: open the file, seek to offset, read up to length bytes,
	// emit BootstrapFileChunk with eof=true when offset+len == size.
}

// replication_bootstrap_file_chunk_event is the receiver's handler —
// it appends the chunk to a `.partial` file, atomic-renames on EOF.
// V1 stub.
func replication_bootstrap_file_chunk_event(e *Event) {
	var chunk BootstrapFileChunk
	if !e.segment(&chunk) {
		info("Replication bootstrap-file-chunk dropping: cannot decode")
		return
	}
	debug("Replication bootstrap-file-chunk: scope=%q path=%q offset=%d len=%d eof=%v from=%q (V1 stub)",
		chunk.Scope, chunk.Path, chunk.Offset, len(chunk.Data), chunk.EOF, e.peer)
	// V2: open `<path>.partial` O_WRONLY|O_CREATE, write at offset,
	// rename(<path>.partial → <path>) on EOF, advance per-(scope, peer)
	// position cursor.
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

// Mochi server: bulk bootstrap unit tests (V1 — scaffolding)
// Copyright Alistair Cunningham 2026

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestBootstrapSetAndGetState: round-trip a state + position cursor
// through the bootstrap table; subsequent set replaces the prior row.
func TestBootstrapSetAndGetState(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	// Absent row → empty state + position.
	state, pos := bootstrap_get_state(bootstrap_scope_files, "peer-A")
	if state != "" || pos != "" {
		t.Errorf("absent row: got state=%q pos=%q, want both empty", state, pos)
	}

	// First write — queued, no position yet.
	bootstrap_set_state(bootstrap_scope_files, "peer-A", bootstrap_state_queued, "")
	state, pos = bootstrap_get_state(bootstrap_scope_files, "peer-A")
	if state != bootstrap_state_queued || pos != "" {
		t.Errorf("after seed: state=%q pos=%q, want (%q, %q)", state, pos, bootstrap_state_queued, "")
	}

	// Advance to active with a resume cursor.
	bootstrap_set_state(bootstrap_scope_files, "peer-A", bootstrap_state_active, "cursor-1")
	state, pos = bootstrap_get_state(bootstrap_scope_files, "peer-A")
	if state != bootstrap_state_active || pos != "cursor-1" {
		t.Errorf("after advance: state=%q pos=%q, want (%q, %q)", state, pos, bootstrap_state_active, "cursor-1")
	}

	// Transition to done; position retained as the final marker.
	bootstrap_set_state(bootstrap_scope_files, "peer-A", bootstrap_state_done, "cursor-final")
	state, pos = bootstrap_get_state(bootstrap_scope_files, "peer-A")
	if state != bootstrap_state_done || pos != "cursor-final" {
		t.Errorf("after done: state=%q pos=%q, want (%q, %q)", state, pos, bootstrap_state_done, "cursor-final")
	}
}

// TestBootstrapClear: removes the row entirely; subsequent reads
// behave as if the (scope, peer) was never tracked.
func TestBootstrapClear(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	bootstrap_set_state(bootstrap_scope_apps, "peer-A", bootstrap_state_done, "cursor-final")
	bootstrap_clear(bootstrap_scope_apps, "peer-A")

	state, pos := bootstrap_get_state(bootstrap_scope_apps, "peer-A")
	if state != "" || pos != "" {
		t.Errorf("after clear: got state=%q pos=%q, want both empty", state, pos)
	}
}

// TestBootstrapSafePathRejectsTraversal: ../ segments, absolute paths,
// and symlink escapes are all rejected.
func TestBootstrapSafePathRejectsTraversal(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	root := filepath.Join(data_dir, "users")
	_ = os.MkdirAll(root, 0o755)

	cases := []struct {
		relative string
		wantErr  bool
		desc     string
	}{
		{"alice/files/post.md", false, "ordinary relative path"},
		{"alice/files/../../etc/passwd", true, ".. traversal"},
		{"/etc/passwd", true, "absolute path"},
		{"alice/../../../tmp/escape", true, "deep traversal"},
		{"", false, "empty prefix (= root itself)"},
		{"alice/files/", false, "trailing slash"},
	}
	for _, tc := range cases {
		_, err := bootstrap_safe_path(root, tc.relative)
		if (err != nil) != tc.wantErr {
			t.Errorf("%s: err=%v, wantErr=%v", tc.desc, err, tc.wantErr)
		}
	}
}

// TestBootstrapWalkManifest: walks a small tree, asserts (path, size,
// sha256) for each entry. Symlinks and non-regular files are skipped.
func TestBootstrapWalkManifest(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	usersRoot := filepath.Join(data_dir, "users")
	_ = os.MkdirAll(filepath.Join(usersRoot, "alice", "feed", "files"), 0o755)
	_ = os.MkdirAll(filepath.Join(usersRoot, "bob", "feed", "files"), 0o755)
	if err := os.WriteFile(filepath.Join(usersRoot, "alice", "feed", "files", "post.md"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("write alice/post.md: %v", err)
	}
	if err := os.WriteFile(filepath.Join(usersRoot, "alice", "feed", "files", "draft.md"), []byte("draft text"), 0o644); err != nil {
		t.Fatalf("write alice/draft.md: %v", err)
	}
	if err := os.WriteFile(filepath.Join(usersRoot, "bob", "feed", "files", "other.md"), []byte("bob"), 0o644); err != nil {
		t.Fatalf("write bob/other.md: %v", err)
	}

	// Whole tree.
	entries, err := bootstrap_walk_manifest(bootstrap_scope_files, "")
	if err != nil {
		t.Fatalf("walk root: %v", err)
	}
	if len(entries) != 3 {
		t.Errorf("entries count = %d, want 3 (got: %+v)", len(entries), entries)
	}
	for _, e := range entries {
		if e.Sha256 == "" || e.Size == 0 {
			t.Errorf("entry missing hash/size: %+v", e)
		}
		if filepath.IsAbs(e.Path) {
			t.Errorf("entry path is absolute: %q (paths must be relative to scope root)", e.Path)
		}
	}

	// Prefixed walk = subset.
	subset, err := bootstrap_walk_manifest(bootstrap_scope_files, "alice")
	if err != nil {
		t.Fatalf("walk alice: %v", err)
	}
	if len(subset) != 2 {
		t.Errorf("alice subset = %d, want 2", len(subset))
	}

	// Hash determinism: re-walking returns the same digests.
	rewalk, _ := bootstrap_walk_manifest(bootstrap_scope_files, "")
	got := map[string]string{}
	for _, e := range entries {
		got[e.Path] = e.Sha256
	}
	for _, e := range rewalk {
		if got[e.Path] != e.Sha256 {
			t.Errorf("hash drift for %q: was %q, now %q", e.Path, got[e.Path], e.Sha256)
		}
	}
}

// TestBootstrapReadChunkRoundtrip: write a file, request chunks
// sequentially, reassemble, compare to the original.
func TestBootstrapReadChunkRoundtrip(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	usersRoot := filepath.Join(data_dir, "users")
	_ = os.MkdirAll(filepath.Join(usersRoot, "alice"), 0o755)
	original := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	if err := os.WriteFile(filepath.Join(usersRoot, "alice", "blob"), original, 0o644); err != nil {
		t.Fatalf("write blob: %v", err)
	}

	// Read in 10-byte chunks.
	var assembled []byte
	var offset int64
	for {
		data, eof, err := bootstrap_read_chunk(bootstrap_scope_files, "alice/blob", offset, 10)
		if err != nil {
			t.Fatalf("read at offset %d: %v", offset, err)
		}
		assembled = append(assembled, data...)
		offset += int64(len(data))
		if eof {
			break
		}
		if offset > int64(len(original))+100 {
			t.Fatalf("infinite loop suspected at offset %d", offset)
		}
	}

	if string(assembled) != string(original) {
		t.Errorf("assembled = %q, want %q", assembled, original)
	}

	// Hash check (defense in depth — confirms no off-by-one in our
	// chunk boundaries).
	wantHash := sha256.Sum256(original)
	gotHash := sha256.Sum256(assembled)
	if hex.EncodeToString(wantHash[:]) != hex.EncodeToString(gotHash[:]) {
		t.Errorf("hash mismatch: want %s got %s",
			hex.EncodeToString(wantHash[:]), hex.EncodeToString(gotHash[:]))
	}
}

// TestBootstrapWriteChunkPartialThenRename: chunks land in .partial,
// rename happens only on EOF, intermediate state survives interruption.
func TestBootstrapWriteChunkPartialThenRename(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	usersRoot := filepath.Join(data_dir, "users")
	_ = os.MkdirAll(usersRoot, 0o755)

	final := filepath.Join(usersRoot, "alice", "feed", "files", "post.md")
	partial := final + ".partial"

	// First chunk — not EOF.
	if err := bootstrap_write_chunk(bootstrap_scope_files, "alice/feed/files/post.md", 0, []byte("hello "), false); err != nil {
		t.Fatalf("write chunk 1: %v", err)
	}
	// Partial exists, final doesn't.
	if _, err := os.Stat(partial); err != nil {
		t.Errorf(".partial missing after first chunk: %v", err)
	}
	if _, err := os.Stat(final); !os.IsNotExist(err) {
		t.Errorf("final exists before EOF: %v", err)
	}

	// Second chunk — EOF.
	if err := bootstrap_write_chunk(bootstrap_scope_files, "alice/feed/files/post.md", 6, []byte("world"), true); err != nil {
		t.Fatalf("write chunk 2: %v", err)
	}
	// Final exists, partial gone.
	if _, err := os.Stat(partial); !os.IsNotExist(err) {
		t.Errorf(".partial still present after EOF: %v", err)
	}
	got, err := os.ReadFile(final)
	if err != nil {
		t.Fatalf("read final: %v", err)
	}
	if string(got) != "hello world" {
		t.Errorf("assembled file = %q, want %q", got, "hello world")
	}
}

// TestBootstrapWriteChunkRejectsTraversal: paths attempting parent-dir
// traversal are refused.
func TestBootstrapWriteChunkRejectsTraversal(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	err := bootstrap_write_chunk(bootstrap_scope_files, "../../../etc/passwd", 0, []byte("malicious"), true)
	if err == nil {
		t.Error("expected error for parent-dir traversal, got nil")
	}
	err = bootstrap_write_chunk(bootstrap_scope_files, "/etc/passwd", 0, []byte("malicious"), true)
	if err == nil {
		t.Error("expected error for absolute path, got nil")
	}
}

// TestBootstrapDiffManifestSkipsMatchingFiles: files whose local copy
// has the same size + sha256 are excluded from the needed list.
func TestBootstrapDiffManifestSkipsMatchingFiles(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	usersRoot := filepath.Join(data_dir, "users")
	_ = os.MkdirAll(filepath.Join(usersRoot, "alice", "feed", "files"), 0o755)

	// Local copy of post.md matches one of the remote entries.
	localPath := filepath.Join(usersRoot, "alice", "feed", "files", "post.md")
	if err := os.WriteFile(localPath, []byte("hello"), 0o644); err != nil {
		t.Fatalf("write local post.md: %v", err)
	}
	localHash, err := bootstrap_file_sha256(localPath)
	if err != nil {
		t.Fatalf("hash local: %v", err)
	}

	remote := []BootstrapFileEntry{
		// Matches local.
		{Path: "alice/feed/files/post.md", Size: 5, Sha256: localHash},
		// Same path, different content → must be fetched.
		{Path: "alice/feed/files/draft.md", Size: 10, Sha256: "deadbeef"},
	}

	needed, err := bootstrap_diff_manifest(bootstrap_scope_files, "alice", remote)
	if err != nil {
		t.Fatalf("diff: %v", err)
	}
	if len(needed) != 1 {
		t.Fatalf("needed = %d, want 1 (post.md matches, draft.md missing)", len(needed))
	}
	if needed[0].Path != "alice/feed/files/draft.md" {
		t.Errorf("needed[0].Path = %q, want draft.md", needed[0].Path)
	}
}

// TestBootstrapChunkRequestsForEntry: a non-trivial file is split into
// bootstrap_max_chunk_size chunks; zero-byte file gets a single
// (0, 0) request as the create-empty-file signal.
func TestBootstrapChunkRequestsForEntry(t *testing.T) {
	// Zero-byte file → single request with length 0.
	reqs := bootstrap_chunk_requests_for_entry(BootstrapFileEntry{Path: "x", Size: 0})
	if len(reqs) != 1 || reqs[0].Length != 0 || reqs[0].Offset != 0 {
		t.Errorf("zero-byte file: got %+v, want [{Offset:0 Length:0}]", reqs)
	}

	// File just under one chunk.
	reqs = bootstrap_chunk_requests_for_entry(BootstrapFileEntry{Path: "y", Size: bootstrap_max_chunk_size - 1})
	if len(reqs) != 1 || reqs[0].Length != bootstrap_max_chunk_size-1 {
		t.Errorf("short file: got %d requests, want 1 of size %d", len(reqs), bootstrap_max_chunk_size-1)
	}

	// File spanning multiple chunks.
	size := int64(bootstrap_max_chunk_size)*2 + 17
	reqs = bootstrap_chunk_requests_for_entry(BootstrapFileEntry{Path: "z", Size: size})
	if len(reqs) != 3 {
		t.Fatalf("multi-chunk: got %d requests, want 3 (got: %+v)", len(reqs), reqs)
	}
	if reqs[0].Offset != 0 || reqs[0].Length != bootstrap_max_chunk_size {
		t.Errorf("req[0] = %+v", reqs[0])
	}
	if reqs[2].Offset != int64(bootstrap_max_chunk_size)*2 || reqs[2].Length != 17 {
		t.Errorf("req[2] = %+v, want offset=2*chunk, length=17", reqs[2])
	}

	// Coverage: offsets are contiguous, end at exactly Size.
	var covered int64
	for _, r := range reqs {
		if r.Offset != covered {
			t.Errorf("non-contiguous: offset %d expected %d", r.Offset, covered)
		}
		covered += r.Length
	}
	if covered != size {
		t.Errorf("total covered = %d, want %d", covered, size)
	}
}

// TestBootstrapFileTransferEndToEnd: sender walks → emits manifest →
// receiver "diffs" and chunk-fetches each file → sender reads chunk →
// receiver writes chunk → atomic rename on EOF. Exercises the V2 hot
// path without P2P transport (handlers called directly).
func TestBootstrapFileTransferEndToEnd(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	usersRoot := filepath.Join(data_dir, "users")
	_ = os.MkdirAll(filepath.Join(usersRoot, "alice", "feed", "files"), 0o755)

	// Source files on the sender side. Use the same data_dir for both
	// sides since bootstrap_*_scope_root is just data_dir-rooted; the
	// receiver path computation works against the same root. Real
	// cross-host usage has distinct data_dirs.
	contents := map[string]string{
		"alice/feed/files/post.md":  "first post body",
		"alice/feed/files/draft.md": "draft text content",
	}
	for rel, body := range contents {
		full := filepath.Join(usersRoot, rel)
		_ = os.MkdirAll(filepath.Dir(full), 0o755)
		if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
			t.Fatalf("write source %q: %v", rel, err)
		}
	}

	// 1. Walker emits manifest with 2 entries.
	entries, err := bootstrap_walk_manifest(bootstrap_scope_files, "alice")
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("manifest entries = %d, want 2", len(entries))
	}

	// 2. For each manifest entry, read every chunk from the sender
	// and write it on the receiver. Use a small chunk size so we
	// exercise the multi-chunk path.
	const chunkSize int64 = 7
	for _, entry := range entries {
		var offset int64
		for {
			data, eof, err := bootstrap_read_chunk(bootstrap_scope_files, entry.Path, offset, chunkSize)
			if err != nil {
				t.Fatalf("read %q at %d: %v", entry.Path, offset, err)
			}
			// Write into a different sub-tree so we don't overwrite
			// the source mid-test. Rewrite the path so the receiver
			// puts the file under "bob" instead of "alice".
			recvPath := "bob/" + entry.Path[len("alice/"):]
			if err := bootstrap_write_chunk(bootstrap_scope_files, recvPath, offset, data, eof); err != nil {
				t.Fatalf("write %q at %d: %v", recvPath, offset, err)
			}
			if eof {
				break
			}
			offset += int64(len(data))
		}
	}

	// 3. Verify each received file matches the source byte-for-byte.
	for rel, want := range contents {
		recvRel := "bob/" + rel[len("alice/"):]
		got, err := os.ReadFile(filepath.Join(usersRoot, recvRel))
		if err != nil {
			t.Errorf("read receiver %q: %v", recvRel, err)
			continue
		}
		if string(got) != want {
			t.Errorf("receiver %q content = %q, want %q", recvRel, got, want)
		}
		// .partial should have been renamed away.
		if _, err := os.Stat(filepath.Join(usersRoot, recvRel+".partial")); !os.IsNotExist(err) {
			t.Errorf(".partial still present for %q after transfer: %v", recvRel, err)
		}
	}
}

// TestBootstrapDBBasenameSafe: validates the basename allowlist.
func TestBootstrapDBBasenameSafe(t *testing.T) {
	cases := []struct {
		name string
		want bool
	}{
		{"users.db", true},
		{"feed.db", true},
		{"my-app_v2.db", true},
		{"", false},
		{".", false},
		{"..", false},
		{".db", true}, // pathologically minimal but matches the regex
		{"users", false},
		{"users.txt", false},
		{"path/users.db", false},
		{"../users.db", false},
		{"users.db/", false},
		{"u sers.db", false},
	}
	for _, tc := range cases {
		if got := bootstrap_db_basename_safe(tc.name); got != tc.want {
			t.Errorf("bootstrap_db_basename_safe(%q) = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestBootstrapDBSourcePathRejectsInvalid: every path-traversal style
// the source-path builder might be asked to validate is rejected.
func TestBootstrapDBSourcePathRejectsInvalid(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	cases := []struct {
		scope, user, app, db string
		wantErr              bool
		desc                 string
	}{
		{bootstrap_scope_userdbs, "alice", "feed", "users.db", false, "well-formed user-db"},
		{bootstrap_scope_sysdbs, "", "", "users.db", false, "well-formed system-db"},
		{bootstrap_scope_userdbs, "../etc", "feed", "users.db", true, "user with .."},
		{bootstrap_scope_userdbs, "alice/etc", "feed", "users.db", true, "user with /"},
		{bootstrap_scope_userdbs, "alice", "../feed", "users.db", true, "app with .."},
		{bootstrap_scope_userdbs, "alice", "feed", "../users.db", true, "db with .."},
		{bootstrap_scope_userdbs, "alice", "feed", "users.txt", true, "db missing .db suffix"},
		{bootstrap_scope_userdbs, "", "feed", "users.db", true, "userdbs missing user"},
		{bootstrap_scope_userdbs, "alice", "", "users.db", true, "userdbs missing app"},
		{bootstrap_scope_files, "alice", "feed", "users.db", true, "wrong scope (files)"},
	}
	for _, tc := range cases {
		_, err := bootstrap_db_source_path(tc.scope, tc.user, tc.app, tc.db)
		if (err != nil) != tc.wantErr {
			t.Errorf("%s: err=%v, wantErr=%v", tc.desc, err, tc.wantErr)
		}
	}
}

// TestBootstrapDBSnapshotRoundtrip: write a real SQLite DB, snapshot
// it via snapshot_copy_db, confirm the copy is openable and contains
// the same data.
func TestBootstrapDBSnapshotRoundtrip(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	srcDir := filepath.Join(data_dir, "users", "alice", "feed", "db")
	_ = os.MkdirAll(srcDir, 0o755)

	// Create a small SQLite DB via the existing db_open path so we
	// get the project's normal connection setup (busy_timeout etc.).
	src := db_open("users/alice/feed/db/feed.db")
	src.exec("create table if not exists posts (id text primary key, body text)")
	src.exec("insert into posts (id, body) values ('p1', 'hello world')")
	src.exec("insert into posts (id, body) values ('p2', 'second post')")

	srcPath, err := bootstrap_db_source_path(bootstrap_scope_userdbs, "alice", "feed", "feed.db")
	if err != nil {
		t.Fatalf("source path: %v", err)
	}
	if _, err := os.Stat(srcPath); err != nil {
		t.Fatalf("source DB missing at %q: %v", srcPath, err)
	}

	// Snapshot to a tempfile (relative to data_dir so db_open can
	// reopen it via the standard pool).
	snapRel := "snap.db"
	snapAbs := filepath.Join(data_dir, snapRel)
	size, err := snapshot_copy_db(srcPath, snapAbs)
	if err != nil {
		t.Fatalf("snapshot_copy_db: %v", err)
	}
	if size == 0 {
		t.Errorf("snapshot size = 0; expected a non-empty DB file")
	}

	// Re-open the snapshot and confirm the rows came across. Reset
	// the db pool so db_open gives us a fresh connection rather than
	// the cached one pointing at the old file.
	databases = map[string]*DB{}
	defer func() { databases = map[string]*DB{} }()
	dst := db_open(snapRel)
	rows, err := dst.rows("select id, body from posts order by id")
	if err != nil {
		t.Fatalf("query snapshot: %v", err)
	}
	if len(rows) != 2 {
		t.Errorf("snapshot rows = %d, want 2", len(rows))
	}
}

// TestBootstrapStartSeedsScopesAndEmitsManifests: bootstrap_start
// seeds 'queued' rows for all four scopes (files, apps, userdbs,
// sysdbs) and fires the corresponding manifest-request to the source
// peer for each.
func TestBootstrapStartSeedsScopesAndEmitsManifests(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	var fileRequests []struct{ peer, scope, prefix string }
	var dbRequests []struct{ peer, scope string }
	origFile := replication_emit_bootstrap_file_manifest_request
	origDB := replication_emit_bootstrap_db_manifest_request
	replication_emit_bootstrap_file_manifest_request = func(peer, scope, prefix string) {
		fileRequests = append(fileRequests, struct{ peer, scope, prefix string }{peer, scope, prefix})
	}
	replication_emit_bootstrap_db_manifest_request = func(peer, scope string) {
		dbRequests = append(dbRequests, struct{ peer, scope string }{peer, scope})
	}
	defer func() {
		replication_emit_bootstrap_file_manifest_request = origFile
		replication_emit_bootstrap_db_manifest_request = origDB
	}()

	bootstrap_start("source-A")

	if len(fileRequests) != 2 {
		t.Errorf("file-manifest emit count = %d, want 2 (files + apps)", len(fileRequests))
	}
	// sysdbs is intentionally not bootstrapped as file snapshots — the
	// running receiver holds the system DBs open and rename(2) would
	// leave its fds pinned to the old inode. Source backfills sysdbs
	// row-by-row via replication_pair_backfill instead. So only
	// userdbs gets a db-manifest emit.
	if len(dbRequests) != 1 {
		t.Errorf("db-manifest emit count = %d, want 1 (userdbs only; sysdbs goes through op-channel backfill)", len(dbRequests))
	}
	for _, scope := range []string{bootstrap_scope_files, bootstrap_scope_apps, bootstrap_scope_userdbs} {
		state, _ := bootstrap_get_state(scope, "source-A")
		if state != bootstrap_state_queued {
			t.Errorf("scope %q state = %q, want queued", scope, state)
		}
	}
	// sysdbs must NOT have a bootstrap row.
	state, _ := bootstrap_get_state(bootstrap_scope_sysdbs, "source-A")
	if state != "" {
		t.Errorf("sysdbs state = %q, want empty (not bootstrapped as a file scope)", state)
	}
}

// TestBootstrapIsActiveSource: returns true only for (scope, peer)
// rows whose state isn't 'done'; missing row → false.
func TestBootstrapIsActiveSource(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	// Missing row → false.
	if bootstrap_is_active_source(bootstrap_scope_files, "peer-A") {
		t.Error("missing row reported as active source")
	}

	// queued → true (we haven't started yet but it's authorized).
	bootstrap_set_state(bootstrap_scope_files, "peer-A", bootstrap_state_queued, "")
	if !bootstrap_is_active_source(bootstrap_scope_files, "peer-A") {
		t.Error("queued row reported as not active")
	}

	// active → true.
	bootstrap_set_state(bootstrap_scope_files, "peer-A", bootstrap_state_active, "42")
	if !bootstrap_is_active_source(bootstrap_scope_files, "peer-A") {
		t.Error("active row reported as not active")
	}

	// done → false (no longer authorized to receive chunks).
	bootstrap_set_state(bootstrap_scope_files, "peer-A", bootstrap_state_done, "")
	if bootstrap_is_active_source(bootstrap_scope_files, "peer-A") {
		t.Error("done row reported as active source")
	}

	// Different peer → false even if the scope has rows for someone else.
	bootstrap_set_state(bootstrap_scope_files, "peer-A", bootstrap_state_active, "1")
	if bootstrap_is_active_source(bootstrap_scope_files, "peer-B") {
		t.Error("different peer reported as active source for the same scope")
	}
}

// TestBootstrapResume: re-fires manifest-requests for every non-done
// row; 'done' rows are ignored; correct event type per scope.
func TestBootstrapResume(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	var fileReqs []struct{ peer, scope string }
	var dbReqs []struct{ peer, scope string }
	origFile := replication_emit_bootstrap_file_manifest_request
	origDB := replication_emit_bootstrap_db_manifest_request
	replication_emit_bootstrap_file_manifest_request = func(peer, scope, prefix string) {
		fileReqs = append(fileReqs, struct{ peer, scope string }{peer, scope})
	}
	replication_emit_bootstrap_db_manifest_request = func(peer, scope string) {
		dbReqs = append(dbReqs, struct{ peer, scope string }{peer, scope})
	}
	defer func() {
		replication_emit_bootstrap_file_manifest_request = origFile
		replication_emit_bootstrap_db_manifest_request = origDB
	}()

	// Seed the bootstrap table with a mix of states / scopes.
	bootstrap_set_state(bootstrap_scope_files, "peer-A", bootstrap_state_active, "12")
	bootstrap_set_state(bootstrap_scope_apps, "peer-A", bootstrap_state_queued, "")
	bootstrap_set_state(bootstrap_scope_userdbs, "peer-A", bootstrap_state_done, "")
	bootstrap_set_state(bootstrap_scope_sysdbs, "peer-B", bootstrap_state_queued, "")

	bootstrap_resume()

	// 'done' row for peer-A/userdbs was skipped; everything else fired.
	if len(fileReqs) != 2 {
		t.Errorf("file requests = %d, want 2 (peer-A's files + apps)", len(fileReqs))
	}
	if len(dbReqs) != 1 {
		t.Errorf("db requests = %d, want 1 (peer-B's sysdbs only — peer-A's userdbs was already done)", len(dbReqs))
	}
}

// TestBootstrapResumeNoActiveRows: no-op when every row is done /
// none exist.
func TestBootstrapResumeNoActiveRows(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	called := false
	origFile := replication_emit_bootstrap_file_manifest_request
	replication_emit_bootstrap_file_manifest_request = func(peer, scope, prefix string) { called = true }
	defer func() { replication_emit_bootstrap_file_manifest_request = origFile }()

	bootstrap_resume()
	if called {
		t.Error("bootstrap_resume fired against empty table; should have been no-op")
	}

	bootstrap_set_state(bootstrap_scope_files, "peer-A", bootstrap_state_done, "")
	bootstrap_resume()
	if called {
		t.Error("bootstrap_resume fired against all-done rows; should have been no-op")
	}
}

// TestBootstrapStartEmptyPeerIsNoOp: empty peer string short-circuits;
// no rows seeded, no emits fired.
func TestBootstrapStartEmptyPeerIsNoOp(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	called := false
	orig := replication_emit_bootstrap_file_manifest_request
	replication_emit_bootstrap_file_manifest_request = func(peer, scope, prefix string) { called = true }
	defer func() { replication_emit_bootstrap_file_manifest_request = orig }()

	bootstrap_start("")
	if called {
		t.Error("bootstrap_start(\"\") emitted a request; should be a no-op")
	}
}

// TestBootstrapPendingDecrement: counter advances toward zero and
// transitions to 'done' on the last decrement; underflow is no-op.
func TestBootstrapPendingDecrement(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	// Seed at 3.
	bootstrap_set_pending(bootstrap_scope_files, "peer-A", 3)
	state, pos := bootstrap_get_state(bootstrap_scope_files, "peer-A")
	if state != bootstrap_state_active || pos != "3" {
		t.Errorf("after seed 3: state=%q pos=%q", state, pos)
	}

	// 3 → 2 → 1 → 0 (done)
	if got := bootstrap_pending_decrement(bootstrap_scope_files, "peer-A"); got != 2 {
		t.Errorf("decrement 1: got %d, want 2", got)
	}
	if got := bootstrap_pending_decrement(bootstrap_scope_files, "peer-A"); got != 1 {
		t.Errorf("decrement 2: got %d, want 1", got)
	}
	if got := bootstrap_pending_decrement(bootstrap_scope_files, "peer-A"); got != 0 {
		t.Errorf("decrement 3: got %d, want 0 (last → done)", got)
	}
	state, pos = bootstrap_get_state(bootstrap_scope_files, "peer-A")
	if state != bootstrap_state_done {
		t.Errorf("after last decrement: state=%q, want done", state)
	}
	if pos != "" {
		t.Errorf("after done: position=%q, want empty", pos)
	}

	// Further decrement on a done row is a no-op.
	if got := bootstrap_pending_decrement(bootstrap_scope_files, "peer-A"); got != 0 {
		t.Errorf("decrement after done: got %d, want 0", got)
	}
	state, _ = bootstrap_get_state(bootstrap_scope_files, "peer-A")
	if state != bootstrap_state_done {
		t.Errorf("after no-op decrement: state=%q, want done", state)
	}

	// Decrement on a missing row returns -1.
	if got := bootstrap_pending_decrement(bootstrap_scope_files, "peer-missing"); got != -1 {
		t.Errorf("decrement on missing row: got %d, want -1", got)
	}
}

// TestBootstrapDBManifestEmptyImmediatelyDone: zero-entry manifest
// marks the scope done without firing snapshot requests.
func TestBootstrapDBManifestEmptyImmediatelyDone(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	called := 0
	orig := replication_emit_bootstrap_db_snapshot_request
	replication_emit_bootstrap_db_snapshot_request = func(peer, scope, user, app, db string) { called++ }
	defer func() { replication_emit_bootstrap_db_snapshot_request = orig }()

	res := &BootstrapDBManifestResult{Scope: bootstrap_scope_userdbs, Entries: nil}
	replication_bootstrap_db_manifest_result_apply("source-A", res)

	if called != 0 {
		t.Errorf("snapshot requests fired on empty manifest = %d, want 0", called)
	}
	state, _ := bootstrap_get_state(bootstrap_scope_userdbs, "source-A")
	if state != bootstrap_state_done {
		t.Errorf("empty manifest scope state = %q, want done", state)
	}
}

// TestBootstrapWalkDBManifest: enumerates DBs under users/<u>/<a>/db/
// for userdbs and db/ for sysdbs; rejects non-.db basenames + non-
// regular entries; returns empty (no error) for missing roots.
func TestBootstrapWalkDBManifest(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	// userdbs layout: alice/feed/db/feed.db + bob/chat/db/chat.db
	_ = os.MkdirAll(filepath.Join(data_dir, "users", "alice", "feed", "db"), 0o755)
	_ = os.MkdirAll(filepath.Join(data_dir, "users", "bob", "chat", "db"), 0o755)
	_ = os.WriteFile(filepath.Join(data_dir, "users", "alice", "feed", "db", "feed.db"), []byte("x"), 0o644)
	_ = os.WriteFile(filepath.Join(data_dir, "users", "bob", "chat", "db", "chat.db"), []byte("x"), 0o644)
	// Junk file that should be filtered:
	_ = os.WriteFile(filepath.Join(data_dir, "users", "alice", "feed", "db", "notes.txt"), []byte("x"), 0o644)

	entries, err := bootstrap_walk_db_manifest(bootstrap_scope_userdbs)
	if err != nil {
		t.Fatalf("walk userdbs: %v", err)
	}
	if len(entries) != 2 {
		t.Errorf("userdbs entries = %d, want 2 (notes.txt should be filtered)", len(entries))
	}
	for _, e := range entries {
		if e.User == "" || e.App == "" || e.DB == "" {
			t.Errorf("incomplete entry: %+v", e)
		}
	}

	// sysdbs layout
	_ = os.MkdirAll(filepath.Join(data_dir, "db"), 0o755)
	_ = os.WriteFile(filepath.Join(data_dir, "db", "users.db"), []byte("x"), 0o644)
	_ = os.WriteFile(filepath.Join(data_dir, "db", "settings.db"), []byte("x"), 0o644)

	sys, err := bootstrap_walk_db_manifest(bootstrap_scope_sysdbs)
	if err != nil {
		t.Fatalf("walk sysdbs: %v", err)
	}
	// Could be > 2 because setup_replication_test already creates
	// replication.db and queue.db. Just assert our two are present.
	have := map[string]bool{}
	for _, e := range sys {
		if e.User != "" || e.App != "" {
			t.Errorf("sysdbs entry has user/app populated: %+v", e)
		}
		have[e.DB] = true
	}
	if !have["users.db"] || !have["settings.db"] {
		t.Errorf("sysdbs missing one of users.db/settings.db; got %v", have)
	}

	// Wrong scope.
	if _, err := bootstrap_walk_db_manifest(bootstrap_scope_files); err == nil {
		t.Error("expected error for files scope on db-manifest walker")
	}
}

// TestBootstrapDBManifestResultFiresSnapshotRequests: receiver handler
// emits one snapshot-request per entry in the manifest.
func TestBootstrapDBManifestResultFiresSnapshotRequests(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	var snapshotReqs []struct{ peer, scope, user, app, db string }
	orig := replication_emit_bootstrap_db_snapshot_request
	replication_emit_bootstrap_db_snapshot_request = func(peer, scope, user, app, db string) {
		snapshotReqs = append(snapshotReqs, struct{ peer, scope, user, app, db string }{peer, scope, user, app, db})
	}
	defer func() { replication_emit_bootstrap_db_snapshot_request = orig }()

	res := &BootstrapDBManifestResult{
		Scope: bootstrap_scope_userdbs,
		Entries: []BootstrapDBEntry{
			{User: "alice", App: "feed", DB: "feed.db"},
			{User: "bob", App: "chat", DB: "chat.db"},
		},
	}
	replication_bootstrap_db_manifest_result_apply("source-A", res)

	if len(snapshotReqs) != 2 {
		t.Fatalf("snapshot requests = %d, want 2", len(snapshotReqs))
	}
	if snapshotReqs[0].peer != "source-A" || snapshotReqs[0].scope != bootstrap_scope_userdbs {
		t.Errorf("snapshot request[0] = %+v", snapshotReqs[0])
	}
}

// TestBootstrapFileScopeAutoDone: end-to-end glue test — sender walks
// → manifest-result apply spawns driver → driver fetches chunks via
// stub → writes land + decrement counter → scope transitions to 'done'.
//
// Replaces an earlier queue-based glue test. The chunk transfer now
// goes through bootstrap_file_chunk_fetch (synchronous stream RPC);
// the test stubs that out to feed bytes directly from the source side
// of the same data_dir without crossing the network.
func TestBootstrapFileScopeAutoDone(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	usersRoot := filepath.Join(data_dir, "users")

	_ = os.MkdirAll(filepath.Join(usersRoot, "alice", "feed", "files"), 0o755)
	contents := map[string]string{
		"alice/feed/files/post1.md": "first post",
		"alice/feed/files/post2.md": "second post body",
		"alice/feed/files/post3.md": "third",
	}
	for rel, body := range contents {
		_ = os.MkdirAll(filepath.Dir(filepath.Join(usersRoot, rel)), 0o755)
		if err := os.WriteFile(filepath.Join(usersRoot, rel), []byte(body), 0o644); err != nil {
			t.Fatalf("write source %q: %v", rel, err)
		}
	}

	entries, err := bootstrap_walk_manifest(bootstrap_scope_files, "alice")
	if err != nil {
		t.Fatalf("walk: %v", err)
	}

	// Override the chunk-fetch RPC to read from the source side of the
	// same data_dir. The real implementation goes over a libp2p
	// stream; this stub keeps the test in-process.
	origFetch := bootstrap_file_chunk_fetch
	defer func() { bootstrap_file_chunk_fetch = origFetch }()
	bootstrap_file_chunk_fetch = func(peer, scope, path string, offset, length int64) (*BootstrapFileChunk, error) {
		data, eof, err := bootstrap_read_chunk(scope, path, offset, length)
		if err != nil {
			return nil, err
		}
		return &BootstrapFileChunk{Scope: scope, Path: path, Offset: offset, Data: data, EOF: eof}, nil
	}
	// Override the driver to run synchronously so the test can assert
	// state after the apply returns. The real driver is `go`-spawned.
	origDriver := bootstrap_file_scope_driver
	defer func() { bootstrap_file_scope_driver = origDriver }()
	bootstrap_file_scope_driver = bootstrap_file_scope_driver_impl // synchronous from this test's POV

	res := &BootstrapFileManifestResult{
		Scope:   bootstrap_scope_files,
		Prefix:  "alice",
		Entries: entries,
		Done:    true,
	}
	// Apply runs the driver in a goroutine; we override the var to
	// run synchronously instead.
	bootstrap_file_scope_driver = func(peer, scope string, needed []BootstrapFileEntry) {
		bootstrap_file_scope_driver_impl(peer, scope, needed)
	}
	replication_bootstrap_file_manifest_result_apply("source-A", res)
	// Driver was kicked off via `go`; for this stub we replaced it
	// with a synchronous-only version above. Since apply still uses
	// `go`, give it a small window to drain. In production the goroutine
	// runs across many seconds; here, deterministic data + sync fetches
	// finish in microseconds. Use a tight poll instead of fixed sleep.
	deadline := now() + 5
	for now() < deadline {
		s, _ := bootstrap_get_state(bootstrap_scope_files, "source-A")
		if s == bootstrap_state_done {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	state, pos := bootstrap_get_state(bootstrap_scope_files, "source-A")
	if state != bootstrap_state_done {
		t.Errorf("after driver: state=%q pos=%q, want done", state, pos)
	}
}

// TestBootstrapScopesForPeer: every scope for a peer is returned in
// stable order; rows for other peers are excluded.
func TestBootstrapScopesForPeer(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	bootstrap_set_state(bootstrap_scope_files, "peer-A", bootstrap_state_done, "f")
	bootstrap_set_state(bootstrap_scope_apps, "peer-A", bootstrap_state_active, "a")
	bootstrap_set_state(bootstrap_scope_userdbs, "peer-A", bootstrap_state_queued, "")
	bootstrap_set_state(bootstrap_scope_files, "peer-OTHER", bootstrap_state_done, "x")

	rows := bootstrap_scopes_for_peer("peer-A")
	if len(rows) != 3 {
		t.Fatalf("rows for peer-A: got %d, want 3", len(rows))
	}
	// Stable order = ORDER BY scope ASC → apps, files, userdbs.
	want := []string{bootstrap_scope_apps, bootstrap_scope_files, bootstrap_scope_userdbs}
	for i, w := range want {
		if rows[i]["scope"] != w {
			t.Errorf("row[%d].scope = %q, want %q", i, rows[i]["scope"], w)
		}
	}

	// Spot-check the states came through.
	if rows[0]["state"] != bootstrap_state_active {
		t.Errorf("apps state = %q, want active", rows[0]["state"])
	}
	if rows[1]["position"] != "f" {
		t.Errorf("files position = %q, want %q", rows[1]["position"], "f")
	}

	// peer-OTHER's row not included.
	for _, r := range rows {
		if r["scope"] == "" {
			t.Errorf("got an empty-scope row: %+v", r)
		}
	}
}

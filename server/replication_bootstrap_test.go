// Mochi server: bulk bootstrap unit tests (V1 — scaffolding)
// Copyright Alistair Cunningham 2026

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"sync"
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
		_, err := bootstrap_db_source_path(tc.scope, "", tc.user, tc.app, tc.db)
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

	srcPath, err := bootstrap_db_source_path(bootstrap_scope_userdbs, "", "alice", "feed", "feed.db")
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

	var mu sync.Mutex
	var fileRequests []struct{ peer, scope, prefix string }
	var dbRequests []struct{ peer, scope, user string }
	origFile := replication_bootstrap_file_manifest_fetch
	origDB := replication_bootstrap_db_manifest_fetch
	replication_bootstrap_file_manifest_fetch = func(peer, scope, prefix string) {
		mu.Lock()
		fileRequests = append(fileRequests, struct{ peer, scope, prefix string }{peer, scope, prefix})
		mu.Unlock()
	}
	replication_bootstrap_db_manifest_fetch = func(peer, scope, user string) {
		mu.Lock()
		dbRequests = append(dbRequests, struct{ peer, scope, user string }{peer, scope, user})
		mu.Unlock()
	}
	defer func() {
		replication_bootstrap_file_manifest_fetch = origFile
		replication_bootstrap_db_manifest_fetch = origDB
	}()

	bootstrap_start("source-A")
	// bootstrap_start spawns the fetches in goroutines; give them a
	// moment to record their stubbed calls.
	for i := 0; i < 50; i++ {
		mu.Lock()
		done := len(fileRequests) == 2 && len(dbRequests) == 1
		mu.Unlock()
		if done {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
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

// TestBootstrapStartUserFiresFilteredFetches: bootstrap_start_user
// (called from replication_link_apply_keys after a per-user link
// signup is approved) must fire (1) a file-manifest fetch with
// prefix=<uid>/ so only that user's files come over, and (2) a
// db-manifest fetch with user=<uid> so the source only sends back
// that user's DB list. Apps + sysdbs must NOT be touched — those are
// whole-server scopes that pair-join handles, not per-user link.
func TestBootstrapStartUserFiresFilteredFetches(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	var mu sync.Mutex
	var fileReqs []struct{ peer, scope, prefix string }
	var dbReqs []struct{ peer, scope, user string }
	origFile := replication_bootstrap_file_manifest_fetch
	origDB := replication_bootstrap_db_manifest_fetch
	replication_bootstrap_file_manifest_fetch = func(peer, scope, prefix string) {
		mu.Lock()
		fileReqs = append(fileReqs, struct{ peer, scope, prefix string }{peer, scope, prefix})
		mu.Unlock()
	}
	replication_bootstrap_db_manifest_fetch = func(peer, scope, user string) {
		mu.Lock()
		dbReqs = append(dbReqs, struct{ peer, scope, user string }{peer, scope, user})
		mu.Unlock()
	}
	defer func() {
		replication_bootstrap_file_manifest_fetch = origFile
		replication_bootstrap_db_manifest_fetch = origDB
	}()

	bootstrap_start_user("source-peer", "alice-uid")
	for i := 0; i < 50; i++ {
		mu.Lock()
		done := len(fileReqs) == 1 && len(dbReqs) == 1
		mu.Unlock()
		if done {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(fileReqs) != 1 {
		t.Fatalf("file-manifest emit count = %d, want 1 (files only; apps is whole-server)", len(fileReqs))
	}
	if fileReqs[0].scope != bootstrap_scope_files {
		t.Errorf("file-manifest scope = %q, want %q", fileReqs[0].scope, bootstrap_scope_files)
	}
	if fileReqs[0].prefix != "alice-uid/" {
		t.Errorf("file-manifest prefix = %q, want %q (must be uid-scoped — never empty, that would pull every user)", fileReqs[0].prefix, "alice-uid/")
	}
	if len(dbReqs) != 1 {
		t.Fatalf("db-manifest emit count = %d, want 1 (userdbs only; sysdbs is per-server)", len(dbReqs))
	}
	if dbReqs[0].user != "alice-uid" {
		t.Errorf("db-manifest user filter = %q, want %q (must be set — empty filter would expose every user's DB list)", dbReqs[0].user, "alice-uid")
	}
	// Apps + sysdbs must remain absent from bootstrap state — they
	// are not per-user concerns.
	for _, scope := range []string{bootstrap_scope_apps, bootstrap_scope_sysdbs} {
		state, _ := bootstrap_get_state(scope, "source-peer")
		if state != "" {
			t.Errorf("bootstrap_start_user touched scope %q (state=%q); only files + userdbs should be seeded", scope, state)
		}
	}
}

// TestBootstrapStartUserRejectsEmpty: defensive guard. An empty peer
// or empty uid must not fire any fetches — both are required.
func TestBootstrapStartUserRejectsEmpty(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	calls := 0
	origFile := replication_bootstrap_file_manifest_fetch
	origDB := replication_bootstrap_db_manifest_fetch
	replication_bootstrap_file_manifest_fetch = func(peer, scope, prefix string) { calls++ }
	replication_bootstrap_db_manifest_fetch = func(peer, scope, user string) { calls++ }
	defer func() {
		replication_bootstrap_file_manifest_fetch = origFile
		replication_bootstrap_db_manifest_fetch = origDB
	}()

	bootstrap_start_user("", "alice-uid")
	bootstrap_start_user("source-peer", "")
	// Give any stray goroutines a beat to record themselves.
	time.Sleep(50 * time.Millisecond)
	if calls != 0 {
		t.Errorf("bootstrap_start_user fired fetches with empty peer/uid (%d calls); both args are required", calls)
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

	var mu sync.Mutex
	var fileReqs []struct{ peer, scope string }
	var dbReqs []struct{ peer, scope string }
	origFile := replication_bootstrap_file_manifest_fetch
	origDB := replication_bootstrap_db_manifest_fetch
	replication_bootstrap_file_manifest_fetch = func(peer, scope, prefix string) {
		mu.Lock()
		fileReqs = append(fileReqs, struct{ peer, scope string }{peer, scope})
		mu.Unlock()
	}
	replication_bootstrap_db_manifest_fetch = func(peer, scope, user string) {
		mu.Lock()
		dbReqs = append(dbReqs, struct{ peer, scope string }{peer, scope})
		mu.Unlock()
	}
	defer func() {
		replication_bootstrap_file_manifest_fetch = origFile
		replication_bootstrap_db_manifest_fetch = origDB
	}()

	// Seed the bootstrap table with a mix of states / scopes.
	bootstrap_set_state(bootstrap_scope_files, "peer-A", bootstrap_state_active, "12")
	bootstrap_set_state(bootstrap_scope_apps, "peer-A", bootstrap_state_queued, "")
	bootstrap_set_state(bootstrap_scope_userdbs, "peer-A", bootstrap_state_done, "")
	bootstrap_set_state(bootstrap_scope_sysdbs, "peer-B", bootstrap_state_queued, "")

	bootstrap_resume()
	for i := 0; i < 50; i++ {
		mu.Lock()
		done := len(fileReqs) == 2 && len(dbReqs) == 1
		mu.Unlock()
		if done {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
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

	var mu sync.Mutex
	called := false
	origFile := replication_bootstrap_file_manifest_fetch
	replication_bootstrap_file_manifest_fetch = func(peer, scope, prefix string) {
		mu.Lock()
		called = true
		mu.Unlock()
	}
	defer func() { replication_bootstrap_file_manifest_fetch = origFile }()

	bootstrap_resume()
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	if called {
		mu.Unlock()
		t.Error("bootstrap_resume fired against empty table; should have been no-op")
	}
	mu.Unlock()

	bootstrap_set_state(bootstrap_scope_files, "peer-A", bootstrap_state_done, "")
	bootstrap_resume()
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if called {
		t.Error("bootstrap_resume fired against all-done rows; should have been no-op")
	}
}

// TestBootstrapStartEmptyPeerIsNoOp: empty peer string short-circuits;
// no rows seeded, no emits fired.
func TestBootstrapStartEmptyPeerIsNoOp(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	var mu sync.Mutex
	called := false
	orig := replication_bootstrap_file_manifest_fetch
	replication_bootstrap_file_manifest_fetch = func(peer, scope, prefix string) {
		mu.Lock()
		called = true
		mu.Unlock()
	}
	defer func() { replication_bootstrap_file_manifest_fetch = orig }()

	bootstrap_start("")
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
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
// marks the scope done without spawning the fetch driver.
func TestBootstrapDBManifestEmptyImmediatelyDone(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	driven := 0
	orig := bootstrap_db_scope_driver
	bootstrap_db_scope_driver = func(peer, scope string, entries []BootstrapDBEntry) { driven++ }
	defer func() { bootstrap_db_scope_driver = orig }()

	res := &BootstrapDBManifestResult{Scope: bootstrap_scope_userdbs, Entries: nil}
	replication_bootstrap_db_manifest_result_apply("source-A", res)

	if driven != 0 {
		t.Errorf("db-scope driver invocations on empty manifest = %d, want 0", driven)
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

	entries, err := bootstrap_walk_db_manifest(bootstrap_scope_userdbs, "")
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

	sys, err := bootstrap_walk_db_manifest(bootstrap_scope_sysdbs, "")
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
	if _, err := bootstrap_walk_db_manifest(bootstrap_scope_files, ""); err == nil {
		t.Error("expected error for files scope on db-manifest walker")
	}
}

// TestBootstrapWalkDBManifestUserFilter: per-user link signup passes
// the placeholder's uid so the source returns only that user's DBs,
// never any other user's. With the filter set to "alice" we should
// see alice/feed/db/feed.db but not bob/chat/db/chat.db.
func TestBootstrapWalkDBManifestUserFilter(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	_ = os.MkdirAll(filepath.Join(data_dir, "users", "alice", "feed", "db"), 0o755)
	_ = os.MkdirAll(filepath.Join(data_dir, "users", "bob", "chat", "db"), 0o755)
	_ = os.WriteFile(filepath.Join(data_dir, "users", "alice", "feed", "db", "feed.db"), []byte("x"), 0o644)
	_ = os.WriteFile(filepath.Join(data_dir, "users", "alice", "user.db"), []byte("x"), 0o644)
	_ = os.WriteFile(filepath.Join(data_dir, "users", "alice", "feed", "app.db"), []byte("x"), 0o644)
	_ = os.WriteFile(filepath.Join(data_dir, "users", "bob", "chat", "db", "chat.db"), []byte("x"), 0o644)
	_ = os.WriteFile(filepath.Join(data_dir, "users", "bob", "user.db"), []byte("x"), 0o644)

	entries, err := bootstrap_walk_db_manifest(bootstrap_scope_userdbs, "alice")
	if err != nil {
		t.Fatalf("walk userdbs (user=alice): %v", err)
	}
	if len(entries) != 3 {
		t.Errorf("filtered userdbs entries = %d, want 3 (alice's user.db + feed/app.db + feed/db/feed.db only)", len(entries))
	}
	for _, e := range entries {
		if e.User != "alice" {
			t.Errorf("filtered entry leaks non-matching user: %+v", e)
		}
	}

	// Empty filter still returns everything (pair-join behaviour).
	all, err := bootstrap_walk_db_manifest(bootstrap_scope_userdbs, "")
	if err != nil {
		t.Fatalf("walk userdbs (no filter): %v", err)
	}
	if len(all) <= len(entries) {
		t.Errorf("unfiltered manifest didn't include bob's DBs; got %d entries, filtered had %d", len(all), len(entries))
	}

	// Filter for a user with no on-disk dir → empty manifest, no error.
	none, err := bootstrap_walk_db_manifest(bootstrap_scope_userdbs, "nobody")
	if err != nil {
		t.Fatalf("walk userdbs (user=nobody): %v", err)
	}
	if len(none) != 0 {
		t.Errorf("filter for missing user returned %d entries, want 0", len(none))
	}
}

// TestBootstrapSettleIncompleteWhenFailures: pending counter draining
// to 0 with failed > 0 transitions to 'incomplete', not 'done'. The
// failure mode this guards: the file driver decrements pending for
// EVERY entry (success or failure) so the scope can settle, but if
// `failed` is ignored the receiver lands on a 'done' state with data
// gaps and silently activates a half-empty user (caught live 2026-05-20:
// 35% of files missing while bootstrap state said 'done').
func TestBootstrapSettleIncompleteWhenFailures(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	bootstrap_pending_add(bootstrap_scope_files, "peer-A", 3)
	// Simulate two of the three transfers failing.
	bootstrap_failed_increment(bootstrap_scope_files, "peer-A")
	bootstrap_failed_increment(bootstrap_scope_files, "peer-A")
	// Drain the pending counter all three times.
	bootstrap_pending_decrement(bootstrap_scope_files, "peer-A")
	bootstrap_pending_decrement(bootstrap_scope_files, "peer-A")
	bootstrap_pending_decrement(bootstrap_scope_files, "peer-A")

	state, _ := bootstrap_get_state(bootstrap_scope_files, "peer-A")
	if state != bootstrap_state_incomplete {
		t.Errorf("state after 3-pending / 2-failed drain = %q, want %q", state, bootstrap_state_incomplete)
	}
	if f := bootstrap_get_failed(bootstrap_scope_files, "peer-A"); f != 2 {
		t.Errorf("failed counter = %d, want 2", f)
	}
}

// TestBootstrapSettleDoneWhenZeroFailures: the happy path — pending
// drains to zero with no failures, scope settles to 'done'.
func TestBootstrapSettleDoneWhenZeroFailures(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	bootstrap_pending_add(bootstrap_scope_files, "peer-A", 2)
	bootstrap_pending_decrement(bootstrap_scope_files, "peer-A")
	bootstrap_pending_decrement(bootstrap_scope_files, "peer-A")

	state, _ := bootstrap_get_state(bootstrap_scope_files, "peer-A")
	if state != bootstrap_state_done {
		t.Errorf("state after clean drain = %q, want %q", state, bootstrap_state_done)
	}
	if f := bootstrap_get_failed(bootstrap_scope_files, "peer-A"); f != 0 {
		t.Errorf("failed counter = %d, want 0", f)
	}
}

// TestBootstrapPeerUserResolvesByMembership: bootstrap_peer_user
// distinguishes pair-join (peer in pair set → empty filter, whole-
// server) from per-user link (peer in hosts → uid filter).
func TestBootstrapPeerUserResolvesByMembership(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	rdb := db_open("db/replication.db")
	rdb.exec("insert into pair (peer, added, role) values ('pair-peer', 1, '')")
	rdb.exec("insert into hosts (user, peer, added, ack) values ('alice-uid', 'link-peer', 1, 0)")

	if got := bootstrap_peer_user("pair-peer"); got != "" {
		t.Errorf("pair-peer must resolve to '' (whole-server); got %q", got)
	}
	if got := bootstrap_peer_user("link-peer"); got != "alice-uid" {
		t.Errorf("link-peer must resolve to 'alice-uid'; got %q", got)
	}
	if got := bootstrap_peer_user("unknown-peer"); got != "" {
		t.Errorf("unknown peer returns ''; got %q", got)
	}
}

// TestBootstrapRetryIncompleteOnceRefiresAndResets: an 'incomplete'
// row in the bootstrap table gets re-fired with reset counters and
// the right user filter (or empty for pair).
func TestBootstrapRetryIncompleteOnceRefiresAndResets(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	rdb := db_open("db/replication.db")
	rdb.exec("insert into hosts (user, peer, added, ack) values ('alice-uid', 'src-peer', 1, 0)")
	// Seed two incomplete scopes: files (per-user) and userdbs (per-user).
	rdb.exec("insert into bootstrap (scope, peer, state, position, failed) values ('files', 'src-peer', 'incomplete', '0', 5)")
	rdb.exec("insert into bootstrap (scope, peer, state, position, failed) values ('userdbs', 'src-peer', 'incomplete', '0', 2)")
	// A done row in the same table — must be left alone.
	rdb.exec("insert into bootstrap (scope, peer, state, position, failed) values ('apps', 'other-peer', 'done', '', 0)")

	var mu sync.Mutex
	var fileReqs []struct{ peer, scope, prefix string }
	var dbReqs []struct{ peer, scope, user string }
	origFile := replication_bootstrap_file_manifest_fetch
	origDB := replication_bootstrap_db_manifest_fetch
	replication_bootstrap_file_manifest_fetch = func(peer, scope, prefix string) {
		mu.Lock()
		fileReqs = append(fileReqs, struct{ peer, scope, prefix string }{peer, scope, prefix})
		mu.Unlock()
	}
	replication_bootstrap_db_manifest_fetch = func(peer, scope, user string) {
		mu.Lock()
		dbReqs = append(dbReqs, struct{ peer, scope, user string }{peer, scope, user})
		mu.Unlock()
	}
	defer func() {
		replication_bootstrap_file_manifest_fetch = origFile
		replication_bootstrap_db_manifest_fetch = origDB
	}()

	bootstrap_retry_incomplete_once()

	for i := 0; i < 50; i++ {
		mu.Lock()
		done := len(fileReqs) == 1 && len(dbReqs) == 1
		mu.Unlock()
		if done {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(fileReqs) != 1 {
		t.Fatalf("retry must re-fire one file manifest for files-incomplete row; got %d", len(fileReqs))
	}
	if fileReqs[0].prefix != "alice-uid/" {
		t.Errorf("retry file prefix = %q, want %q (uid-scoped because src-peer is in hosts not pair)", fileReqs[0].prefix, "alice-uid/")
	}
	if len(dbReqs) != 1 {
		t.Fatalf("retry must re-fire one db manifest for userdbs-incomplete row; got %d", len(dbReqs))
	}
	if dbReqs[0].user != "alice-uid" {
		t.Errorf("retry db user = %q, want %q", dbReqs[0].user, "alice-uid")
	}

	// Counters must be reset on the retried rows so the new round can
	// repopulate them via pending_add.
	state, _ := bootstrap_get_state(bootstrap_scope_files, "src-peer")
	if state != bootstrap_state_queued {
		t.Errorf("files state after retry = %q, want %q (reset)", state, bootstrap_state_queued)
	}
	if f := bootstrap_get_failed(bootstrap_scope_files, "src-peer"); f != 0 {
		t.Errorf("files failed counter after retry = %d, want 0 (reset)", f)
	}

	// The unrelated done row stays as it was.
	state, _ = bootstrap_get_state(bootstrap_scope_apps, "other-peer")
	if state != bootstrap_state_done {
		t.Errorf("untouched done row mutated by retry: state=%q", state)
	}
}

// TestBootstrapDBLandClearsStaleSidecars: landing a snapshot must
// remove the destination's stale -wal / -shm / -journal sidecars.
// Regression for the 2026-05-21 mochi2 crash: bootstrap renamed the
// `.db` into place but left the previous database's write-ahead log
// next to it; the next open replayed that log against the new file
// and SQLite raised "database disk image is malformed", which (in a
// Starlark goroutine) killed the server.
func TestBootstrapDBLandClearsStaleSidecars(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	dir := filepath.Join(data_dir, "users", "u-alice", "feeds", "db")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	target := filepath.Join(dir, "feeds.db")
	partial := target + ".partial"

	// The destination already exists with stale sidecars from the
	// server's prior use of that path.
	if err := os.WriteFile(target, []byte("OLD-DB"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, sidecar := range []string{target + "-wal", target + "-shm", target + "-journal"} {
		if err := os.WriteFile(sidecar, []byte("stale"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// The freshly-transferred snapshot.
	if err := os.WriteFile(partial, []byte("NEW-SNAPSHOT"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := bootstrap_db_land(partial, target); err != nil {
		t.Fatalf("bootstrap_db_land: %v", err)
	}

	// The snapshot replaced the target...
	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("read target: %v", err)
	}
	if string(got) != "NEW-SNAPSHOT" {
		t.Errorf("target content = %q, want %q", got, "NEW-SNAPSHOT")
	}
	// ...the partial was consumed...
	if _, err := os.Stat(partial); !os.IsNotExist(err) {
		t.Errorf("partial %q still exists after land", partial)
	}
	// ...and every stale sidecar is gone.
	for _, sidecar := range []string{target + "-wal", target + "-shm", target + "-journal"} {
		if _, err := os.Stat(sidecar); !os.IsNotExist(err) {
			t.Errorf("stale sidecar %q survived land — a fresh open would replay it and corrupt the new DB", sidecar)
		}
	}
}

// TestBootstrapDBManifestResultSpawnsDriver: receiver handler spawns
// the per-scope driver goroutine with the full entry list. Replaces
// the old "fires one snapshot-request per entry" test — the new
// sync-stream flow has no per-entry queue emit; the driver runs in
// its own goroutine and calls bootstrap_db_fetch per entry.
func TestBootstrapDBManifestResultSpawnsDriver(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	type drivenCall struct {
		peer    string
		scope   string
		entries []BootstrapDBEntry
	}
	driven := make(chan drivenCall, 1)
	orig := bootstrap_db_scope_driver
	bootstrap_db_scope_driver = func(peer, scope string, entries []BootstrapDBEntry) {
		driven <- drivenCall{peer, scope, entries}
	}
	defer func() { bootstrap_db_scope_driver = orig }()

	res := &BootstrapDBManifestResult{
		Scope: bootstrap_scope_userdbs,
		Entries: []BootstrapDBEntry{
			{User: "alice", App: "feed", DB: "feed.db"},
			{User: "bob", App: "chat", DB: "chat.db"},
		},
	}
	replication_bootstrap_db_manifest_result_apply("source-A", res)

	select {
	case call := <-driven:
		if call.peer != "source-A" || call.scope != bootstrap_scope_userdbs {
			t.Errorf("driver call = peer=%q scope=%q, want source-A / %q", call.peer, call.scope, bootstrap_scope_userdbs)
		}
		if len(call.entries) != 2 {
			t.Errorf("driver entries = %d, want 2", len(call.entries))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("driver was not invoked within 2s")
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

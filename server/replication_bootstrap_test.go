// Mochi server: bulk bootstrap unit tests (V1 — scaffolding)
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strconv"
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

// TestBootstrapSafePathRejectsTraversal: ../ segments, absolute paths,
// and symlink escapes are all rejected.
func TestBootstrapSafePathRejectsTraversal(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	root := filepath.Join(data_dir, "users")
	_ = os.MkdirAll(root, 0o755)

	cases := []struct {
		relative string
		want_err bool
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
		if (err != nil) != tc.want_err {
			t.Errorf("%s: err=%v, want_err=%v", tc.desc, err, tc.want_err)
		}
	}
}

// TestBootstrapWalkManifest: walks a small tree, asserts (path, size,
// sha256) for each entry. Symlinks and non-regular files are skipped.
// TestBootstrapWalkManifestStreamPaginates: the streaming walker emits
// entries in pages of the given size AS it walks (so the receiver gets its
// first page before the whole tree is hashed), no page exceeds pageSize,
// and the slice wrapper still returns every entry.
func TestBootstrapWalkManifestStreamPaginates(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	files := filepath.Join(data_dir, "users", "alice", "feed", "files")
	_ = os.MkdirAll(files, 0o755)
	const n = 5
	for i := 0; i < n; i++ {
		if err := os.WriteFile(filepath.Join(files, "f"+strconv.Itoa(i)+".md"), []byte("body"+strconv.Itoa(i)), 0o644); err != nil {
			t.Fatalf("write f%d: %v", i, err)
		}
	}

	var pages [][]BootstrapFileEntry
	var total int
	err := bootstrap_walk_manifest_stream(bootstrap_scope_files, "", 2, func(page []BootstrapFileEntry) error {
		if len(page) > 2 {
			t.Errorf("page size %d exceeds pageSize 2", len(page))
		}
		// emit reuses the backing array, so copy before retaining.
		cp := append([]BootstrapFileEntry(nil), page...)
		pages = append(pages, cp)
		total += len(cp)
		return nil
	})
	if err != nil {
		t.Fatalf("stream walk: %v", err)
	}
	if total != n {
		t.Errorf("streamed entries total = %d, want %d", total, n)
	}
	if len(pages) < 2 {
		t.Errorf("expected multiple pages for %d files at pageSize 2; got %d page(s)", n, len(pages))
	}
	// Wrapper returns the full set.
	all, err := bootstrap_walk_manifest(bootstrap_scope_files, "")
	if err != nil || len(all) != n {
		t.Fatalf("bootstrap_walk_manifest = %d entries (err %v), want %d", len(all), err, n)
	}
}

func TestBootstrapWalkManifest(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	users_root := filepath.Join(data_dir, "users")
	_ = os.MkdirAll(filepath.Join(users_root, "alice", "feed", "files"), 0o755)
	_ = os.MkdirAll(filepath.Join(users_root, "bob", "feed", "files"), 0o755)
	if err := os.WriteFile(filepath.Join(users_root, "alice", "feed", "files", "post.md"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("write alice/post.md: %v", err)
	}
	if err := os.WriteFile(filepath.Join(users_root, "alice", "feed", "files", "draft.md"), []byte("draft text"), 0o644); err != nil {
		t.Fatalf("write alice/draft.md: %v", err)
	}
	if err := os.WriteFile(filepath.Join(users_root, "bob", "feed", "files", "other.md"), []byte("bob"), 0o644); err != nil {
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
	users_root := filepath.Join(data_dir, "users")
	_ = os.MkdirAll(filepath.Join(users_root, "alice"), 0o755)
	original := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	if err := os.WriteFile(filepath.Join(users_root, "alice", "blob"), original, 0o644); err != nil {
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
	want_hash := sha256.Sum256(original)
	got_hash := sha256.Sum256(assembled)
	if hex.EncodeToString(want_hash[:]) != hex.EncodeToString(got_hash[:]) {
		t.Errorf("hash mismatch: want %s got %s",
			hex.EncodeToString(want_hash[:]), hex.EncodeToString(got_hash[:]))
	}
}

// TestBootstrapWriteChunkPartialThenRename: chunks land in .partial,
// rename happens only on EOF, intermediate state survives interruption.
func TestBootstrapWriteChunkPartialThenRename(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	users_root := filepath.Join(data_dir, "users")
	_ = os.MkdirAll(users_root, 0o755)

	final := filepath.Join(users_root, "alice", "feed", "files", "post.md")
	partial := final + ".partial"

	// First chunk — not EOF.
	if err := bootstrap_write_chunk(bootstrap_scope_files, "alice/feed/files/post.md", 0, []byte("hello "), false, ""); err != nil {
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
	if err := bootstrap_write_chunk(bootstrap_scope_files, "alice/feed/files/post.md", 6, []byte("world"), true, ""); err != nil {
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

	err := bootstrap_write_chunk(bootstrap_scope_files, "../../../etc/passwd", 0, []byte("malicious"), true, "")
	if err == nil {
		t.Error("expected error for parent-dir traversal, got nil")
	}
	err = bootstrap_write_chunk(bootstrap_scope_files, "/etc/passwd", 0, []byte("malicious"), true, "")
	if err == nil {
		t.Error("expected error for absolute path, got nil")
	}
}

// TestBootstrapDiffManifestSkipsMatchingFiles: files whose local copy
// has the same size + sha256 are excluded from the needed list.
func TestBootstrapDiffManifestSkipsMatchingFiles(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	users_root := filepath.Join(data_dir, "users")
	_ = os.MkdirAll(filepath.Join(users_root, "alice", "feed", "files"), 0o755)

	// Local copy of post.md matches one of the remote entries.
	local_path := filepath.Join(users_root, "alice", "feed", "files", "post.md")
	if err := os.WriteFile(local_path, []byte("hello"), 0o644); err != nil {
		t.Fatalf("write local post.md: %v", err)
	}
	local_hash, err := bootstrap_file_sha256(local_path)
	if err != nil {
		t.Fatalf("hash local: %v", err)
	}

	remote := []BootstrapFileEntry{
		// Matches local.
		{Path: "alice/feed/files/post.md", Size: 5, Sha256: local_hash},
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
	orig := data_dir
	data_dir = t.TempDir() // no .partial files present → requests start at offset 0
	defer func() { data_dir = orig }()

	// Zero-byte file → single request with length 0.
	reqs := bootstrap_chunk_requests_for_entry("files", BootstrapFileEntry{Path: "x", Size: 0})
	if len(reqs) != 1 || reqs[0].Length != 0 || reqs[0].Offset != 0 {
		t.Errorf("zero-byte file: got %+v, want [{Offset:0 Length:0}]", reqs)
	}

	// File just under one chunk.
	reqs = bootstrap_chunk_requests_for_entry("files", BootstrapFileEntry{Path: "y", Size: bootstrap_max_chunk_size - 1})
	if len(reqs) != 1 || reqs[0].Length != bootstrap_max_chunk_size-1 {
		t.Errorf("short file: got %d requests, want 1 of size %d", len(reqs), bootstrap_max_chunk_size-1)
	}

	// File spanning multiple chunks.
	size := int64(bootstrap_max_chunk_size)*2 + 17
	reqs = bootstrap_chunk_requests_for_entry("files", BootstrapFileEntry{Path: "z", Size: size})
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

// TestBootstrapChunkRequestsResume: a partial file on disk makes the chunk
// requests resume from its size instead of restarting at offset 0, so a
// re-fetch after a dropped connection makes forward progress (#78).
func TestBootstrapChunkRequestsResume(t *testing.T) {
	orig := data_dir
	data_dir = t.TempDir()
	defer func() { data_dir = orig }()

	root, err := bootstrap_file_scope_root("files")
	if err != nil {
		t.Fatal(err)
	}
	final, err := bootstrap_safe_path(root, "bigfile")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(final), 0o755); err != nil {
		t.Fatal(err)
	}
	chunk := int64(bootstrap_max_chunk_size)
	size := chunk*4 + 7 // 5 chunks
	entry := BootstrapFileEntry{Path: "bigfile", Size: size}

	// No partial → full fetch from offset 0 (5 chunks).
	if reqs := bootstrap_chunk_requests_for_entry("files", entry); len(reqs) != 5 || reqs[0].Offset != 0 {
		t.Fatalf("no partial: want 5 reqs from 0, got %d from %d", len(reqs), reqs[0].Offset)
	}

	// 2 chunks already on disk → resume from offset 2*chunk (3 chunks left),
	// last request still reaching Size (its EOF triggers the rename).
	if err := os.WriteFile(final+".partial", make([]byte, chunk*2), 0o644); err != nil {
		t.Fatal(err)
	}
	reqs := bootstrap_chunk_requests_for_entry("files", entry)
	if len(reqs) != 3 || reqs[0].Offset != chunk*2 {
		t.Fatalf("resume: want 3 reqs from %d, got %d from %d", chunk*2, len(reqs), reqs[0].Offset)
	}
	if last := reqs[len(reqs)-1]; last.Offset+last.Length != size {
		t.Fatalf("resume: last req must reach Size, got %d+%d != %d", last.Offset, last.Length, size)
	}

	// Partial at full size but unrenamed (EOF lost) → re-fetch the final chunk.
	if err := os.WriteFile(final+".partial", make([]byte, size), 0o644); err != nil {
		t.Fatal(err)
	}
	if reqs := bootstrap_chunk_requests_for_entry("files", entry); len(reqs) != 1 || reqs[0].Offset != chunk*4 {
		t.Fatalf("complete-unrenamed: want 1 final chunk from %d, got %d from %d", chunk*4, len(reqs), reqs[0].Offset)
	}

	// Oversized stale partial → discarded, full re-fetch from 0.
	if err := os.WriteFile(final+".partial", make([]byte, size+chunk), 0o644); err != nil {
		t.Fatal(err)
	}
	if reqs := bootstrap_chunk_requests_for_entry("files", entry); len(reqs) != 5 || reqs[0].Offset != 0 {
		t.Fatalf("oversized partial: want full re-fetch from 0, got %d from %d", len(reqs), reqs[0].Offset)
	}
	if _, err := os.Stat(final + ".partial"); !os.IsNotExist(err) {
		t.Fatal("oversized stale partial should have been removed")
	}
}

// TestBootstrapFileTransferEndToEnd: sender walks → emits manifest →
// receiver "diffs" and chunk-fetches each file → sender reads chunk →
// receiver writes chunk → atomic rename on EOF. Exercises the V2 hot
// path without Net transport (handlers called directly).
func TestBootstrapFileTransferEndToEnd(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	users_root := filepath.Join(data_dir, "users")
	_ = os.MkdirAll(filepath.Join(users_root, "alice", "feed", "files"), 0o755)

	// Source files on the sender side. Use the same data_dir for both
	// sides since bootstrap_*_scope_root is just data_dir-rooted; the
	// receiver path computation works against the same root. Real
	// cross-host usage has distinct data_dirs.
	contents := map[string]string{
		"alice/feed/files/post.md":  "first post body",
		"alice/feed/files/draft.md": "draft text content",
	}
	for rel, body := range contents {
		full := filepath.Join(users_root, rel)
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
			recv_path := "bob/" + entry.Path[len("alice/"):]
			if err := bootstrap_write_chunk(bootstrap_scope_files, recv_path, offset, data, eof, ""); err != nil {
				t.Fatalf("write %q at %d: %v", recv_path, offset, err)
			}
			if eof {
				break
			}
			offset += int64(len(data))
		}
	}

	// 3. Verify each received file matches the source byte-for-byte.
	for rel, want := range contents {
		recv_rel := "bob/" + rel[len("alice/"):]
		got, err := os.ReadFile(filepath.Join(users_root, recv_rel))
		if err != nil {
			t.Errorf("read receiver %q: %v", recv_rel, err)
			continue
		}
		if string(got) != want {
			t.Errorf("receiver %q content = %q, want %q", recv_rel, got, want)
		}
		// .partial should have been renamed away.
		if _, err := os.Stat(filepath.Join(users_root, recv_rel+".partial")); !os.IsNotExist(err) {
			t.Errorf(".partial still present for %q after transfer: %v", recv_rel, err)
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
		want_err             bool
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
		if (err != nil) != tc.want_err {
			t.Errorf("%s: err=%v, want_err=%v", tc.desc, err, tc.want_err)
		}
	}
}

// TestBootstrapDBSnapshotRoundtrip: write a real SQLite DB, snapshot
// it via snapshot_copy_db, confirm the copy is openable and contains
// the same data.
func TestBootstrapDBSnapshotRoundtrip(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	source_dir := filepath.Join(data_dir, "users", "alice", "feed", "db")
	_ = os.MkdirAll(source_dir, 0o755)

	// Create a small SQLite DB via the existing db_open path so we
	// get the project's normal connection setup (busy_timeout etc.).
	src := db_open("users/alice/feed/db/feed.db")
	src.exec("create table if not exists posts (id text primary key, body text)")
	src.exec("insert into posts (id, body) values ('p1', 'hello world')")
	src.exec("insert into posts (id, body) values ('p2', 'second post')")

	source_path, err := bootstrap_db_source_path(bootstrap_scope_userdbs, "", "alice", "feed", "feed.db")
	if err != nil {
		t.Fatalf("source path: %v", err)
	}
	if _, err := os.Stat(source_path); err != nil {
		t.Fatalf("source DB missing at %q: %v", source_path, err)
	}

	// Snapshot to a tempfile (relative to data_dir so db_open can
	// reopen it via the standard pool).
	snap_rel := "snap.db"
	snap_abs := filepath.Join(data_dir, snap_rel)
	size, err := snapshot_copy_db(source_path, snap_abs)
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
	dst := db_open(snap_rel)
	rows, err := dst.rows("select id, body from posts order by id")
	if err != nil {
		t.Fatalf("query snapshot: %v", err)
	}
	if len(rows) != 2 {
		t.Errorf("snapshot rows = %d, want 2", len(rows))
	}
}

// TestBootstrapDBSnapshotShipsBroadcastLog: a per-app DB with rows in
// the broadcast subsystem's reserved tables (_log, _sequence,
// _received, _acknowledged) round-trips through snapshot_copy_db.
// Demonstrates the answer to task #21 in concrete code: bulk
// bootstrap of a new pair member carries the full broadcast log so
// the new member can serve resync requests for any (key, peer)
// stream the existing pair members had been logging - no separate
// backfill needed. Regression guard if the snapshot path ever
// changes to selectively skip tables.
func TestBootstrapDBSnapshotShipsBroadcastLog(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	source_dir := filepath.Join(data_dir, "users", "alice", "feed", "db")
	_ = os.MkdirAll(source_dir, 0o755)

	source := db_open("users/alice/feed/db/feed.db")
	// Match the production schema verbatim (broadcast_log_table_create
	// + siblings in broadcast.go). The test asserts the bootstrap path
	// preserves these tables and their rows; if the production schema
	// changes, update both places.
	source.exec("create table _log (key text not null, peer text not null, sequence integer not null, event text not null, data text not null, created integer not null, primary key (key, peer, sequence))")
	source.exec("create table _sequence (key text not null, peer text not null, last integer not null default 0, primary key (key, peer))")
	source.exec("create table _received (sender text not null, key text not null, last integer not null default 0, primary key (sender, key))")
	source.exec("create table _acknowledged (key text not null, peer text not null, subscriber text not null, last integer not null default 0, primary key (key, peer, subscriber))")
	source.exec("insert into _log (key, peer, sequence, event, data, created) values ('feed1', 'peerA', 1, 'post/create', '{}', 1700000000)")
	source.exec("insert into _log (key, peer, sequence, event, data, created) values ('feed1', 'peerA', 2, 'post/edit', '{}', 1700000005)")
	source.exec("insert into _log (key, peer, sequence, event, data, created) values ('feed1', 'peer_b', 1, 'comment/create', '{}', 1700000010)")
	source.exec("insert into _sequence (key, peer, last) values ('feed1', 'peerA', 2)")
	source.exec("insert into _sequence (key, peer, last) values ('feed1', 'peer_b', 1)")
	source.exec("insert into _received (sender, key, last) values ('peerC', 'feed1', 5)")
	source.exec("insert into _acknowledged (key, peer, subscriber, last) values ('feed1', 'peerA', 'subX', 2)")

	source_path, err := bootstrap_db_source_path(bootstrap_scope_userdbs, "", "alice", "feed", "feed.db")
	if err != nil {
		t.Fatalf("source path: %v", err)
	}

	snapshot_relative := "feed-snapshot.db"
	snapshot_absolute := filepath.Join(data_dir, snapshot_relative)
	if _, err := snapshot_copy_db(source_path, snapshot_absolute); err != nil {
		t.Fatalf("snapshot_copy_db: %v", err)
	}

	databases = map[string]*DB{}
	defer func() { databases = map[string]*DB{} }()
	destination := db_open(snapshot_relative)

	// _log rows: every (key, peer, sequence) round-trips.
	log_rows, _ := destination.rows("select key, peer, sequence, event from _log order by key, peer, sequence")
	if len(log_rows) != 3 {
		t.Errorf("_log rows = %d, want 3 (snapshot dropped or skipped _log)", len(log_rows))
	}
	for index, expected := range []struct {
		key, peer, event string
		sequence         int64
	}{
		{"feed1", "peerA", "post/create", 1},
		{"feed1", "peerA", "post/edit", 2},
		{"feed1", "peer_b", "comment/create", 1},
	} {
		if index >= len(log_rows) {
			break
		}
		row := log_rows[index]
		if row["key"] != expected.key || row["peer"] != expected.peer ||
			row["event"] != expected.event || row["sequence"].(int64) != expected.sequence {
			t.Errorf("_log row %d: got %v, want %+v", index, row, expected)
		}
	}

	// _sequence: per-(key, peer) last sequence preserved.
	if destination.integer("select last from _sequence where key='feed1' and peer='peerA'") != 2 {
		t.Errorf("_sequence (feed1, peerA) did not round-trip as 2")
	}
	if destination.integer("select last from _sequence where key='feed1' and peer='peer_b'") != 1 {
		t.Errorf("_sequence (feed1, peer_b) did not round-trip as 1")
	}

	// _received: per-(sender, key) cursor preserved.
	if destination.integer("select last from _received where sender='peerC' and key='feed1'") != 5 {
		t.Errorf("_received (peerC, feed1) did not round-trip as 5")
	}

	// _acknowledged: per-(key, peer, subscriber) progress preserved.
	if destination.integer("select last from _acknowledged where key='feed1' and peer='peerA' and subscriber='subX'") != 2 {
		t.Errorf("_acknowledged (feed1, peerA, subX) did not round-trip as 2")
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
	orig_file := replication_bootstrap_file_manifest_fetch
	orig_db := replication_bootstrap_db_manifest_fetch
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
		replication_bootstrap_file_manifest_fetch = orig_file
		replication_bootstrap_db_manifest_fetch = orig_db
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
	orig_file := replication_bootstrap_file_manifest_fetch
	orig_db := replication_bootstrap_db_manifest_fetch
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
		replication_bootstrap_file_manifest_fetch = orig_file
		replication_bootstrap_db_manifest_fetch = orig_db
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
	orig_file := replication_bootstrap_file_manifest_fetch
	orig_db := replication_bootstrap_db_manifest_fetch
	replication_bootstrap_file_manifest_fetch = func(peer, scope, prefix string) { calls++ }
	replication_bootstrap_db_manifest_fetch = func(peer, scope, user string) { calls++ }
	defer func() {
		replication_bootstrap_file_manifest_fetch = orig_file
		replication_bootstrap_db_manifest_fetch = orig_db
	}()

	bootstrap_start_user("", "alice-uid")
	bootstrap_start_user("source-peer", "")
	// Give any stray goroutines a beat to record themselves.
	time.Sleep(50 * time.Millisecond)
	if calls != 0 {
		t.Errorf("bootstrap_start_user fired fetches with empty peer/uid (%d calls); both args are required", calls)
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
	orig_file := replication_bootstrap_file_manifest_fetch
	orig_db := replication_bootstrap_db_manifest_fetch
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
		replication_bootstrap_file_manifest_fetch = orig_file
		replication_bootstrap_db_manifest_fetch = orig_db
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
	orig_file := replication_bootstrap_file_manifest_fetch
	replication_bootstrap_file_manifest_fetch = func(peer, scope, prefix string) {
		mu.Lock()
		called = true
		mu.Unlock()
	}
	defer func() { replication_bootstrap_file_manifest_fetch = orig_file }()

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
	orig_file := replication_bootstrap_file_manifest_fetch
	orig_db := replication_bootstrap_db_manifest_fetch
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
		replication_bootstrap_file_manifest_fetch = orig_file
		replication_bootstrap_db_manifest_fetch = orig_db
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

// TestBootstrapRetryRefiresQueuedAndStalledActive is the keystone of the
// universal-retry fix (#25). The retry driver used to re-fire only
// 'incomplete' rows, so a scope stuck at 'queued' (its manifest-request
// never landed — exactly yuzu's files+apps scopes on 2026-06-14) sat
// there forever. The driver must now also re-fire queued rows and
// stalled-active rows, while leaving a live (recently-progressing) active
// transfer alone.
func TestBootstrapRetryRefiresQueuedAndStalledActive(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	rdb := db_open("db/replication.db")
	// Pair member → whole-server (empty prefix / user filter), deterministic.
	rdb.exec("insert into pair (peer, added) values ('src-peer', 1)")

	// files: queued, never progressed (progress=0) → the yuzu case.
	rdb.exec("insert into bootstrap (scope, peer, state, position, progress, attempts) values ('files', 'src-peer', 'queued', '', 0, 0)")
	// apps: active and LIVE (progress just now) → must be left alone.
	rdb.exec("insert into bootstrap (scope, peer, state, position, progress, attempts) values ('apps', 'src-peer', 'active', '3', ?, 0)", now())
	// userdbs: active but STALLED (progress well past the stall window) → re-fire.
	rdb.exec("insert into bootstrap (scope, peer, state, position, progress, attempts) values ('userdbs', 'src-peer', 'active', '7', ?, 0)", now()-bootstrap_stall_seconds-10)
	// sysdbs on another peer: done → never touched.
	rdb.exec("insert into bootstrap (scope, peer, state, position, progress, attempts) values ('sysdbs', 'other-peer', 'done', '', 0, 0)")

	var mu sync.Mutex
	var fileScopes, dbScopes []string
	orig_file := replication_bootstrap_file_manifest_fetch
	orig_db := replication_bootstrap_db_manifest_fetch
	replication_bootstrap_file_manifest_fetch = func(peer, scope, prefix string) {
		mu.Lock()
		fileScopes = append(fileScopes, scope)
		mu.Unlock()
	}
	replication_bootstrap_db_manifest_fetch = func(peer, scope, user string) {
		mu.Lock()
		dbScopes = append(dbScopes, scope)
		mu.Unlock()
	}
	defer func() {
		replication_bootstrap_file_manifest_fetch = orig_file
		replication_bootstrap_db_manifest_fetch = orig_db
	}()

	bootstrap_retry_incomplete_once()

	// Give the refire goroutines a moment to record.
	for i := 0; i < 50; i++ {
		mu.Lock()
		done := len(fileScopes) >= 1 && len(dbScopes) >= 1
		mu.Unlock()
		if done {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	// files (queued) must be re-fired; apps (live active) must NOT be.
	if len(fileScopes) != 1 || fileScopes[0] != bootstrap_scope_files {
		t.Errorf("file refires = %v, want exactly [files] (queued re-fired, live-active apps left alone)", fileScopes)
	}
	// userdbs (stalled active) must be re-fired.
	if len(dbScopes) != 1 || dbScopes[0] != bootstrap_scope_userdbs {
		t.Errorf("db refires = %v, want exactly [userdbs] (stalled active re-fired)", dbScopes)
	}
	// The live-active apps row keeps its position (not reset to queued).
	if state, pos := bootstrap_get_state(bootstrap_scope_apps, "src-peer"); state != bootstrap_state_active || pos != "3" {
		t.Errorf("live-active apps row disturbed: state=%q position=%q, want active/3", state, pos)
	}
	// The done row on the other peer is untouched.
	if state, _ := bootstrap_get_state(bootstrap_scope_sysdbs, "other-peer"); state != bootstrap_state_done {
		t.Errorf("done row mutated: state=%q", state)
	}
}

// TestBootstrapRetryEligibility unit-tests the pure backoff + eligibility
// decision so the timing logic is verified without DB / goroutine churn.
func TestBootstrapRetryEligibility(t *testing.T) {
	backoffs := []struct {
		attempts int64
		want     int64
	}{
		{0, 30}, {1, 60}, {2, 120}, {3, 240}, {6, 1800}, {100, 1800},
	}
	for _, b := range backoffs {
		if got := bootstrap_retry_backoff(b.attempts); got != b.want {
			t.Errorf("bootstrap_retry_backoff(%d) = %d, want %d", b.attempts, got, b.want)
		}
	}

	cases := []struct {
		name     string
		state    string
		idle     int64
		attempts int64
		want     bool
	}{
		{"queued within backoff", bootstrap_state_queued, 5, 0, false},
		{"queued past backoff", bootstrap_state_queued, 40, 0, true},
		{"queued backoff escalated by attempts", bootstrap_state_queued, 40, 1, false},
		{"live active never disturbed", bootstrap_state_active, 5, 0, false},
		{"stalled active re-driven", bootstrap_state_active, bootstrap_stall_seconds + 10, 0, true},
		{"incomplete long idle", bootstrap_state_incomplete, 100000, 0, true},
	}
	for _, c := range cases {
		if got := bootstrap_retry_eligible(c.state, c.idle, c.attempts); got != c.want {
			t.Errorf("%s: bootstrap_retry_eligible(%q, idle=%d, attempts=%d) = %v, want %v",
				c.name, c.state, c.idle, c.attempts, got, c.want)
		}
	}
}

// TestBootstrapResumePeer covers the on-demand operator kick (#25): it
// re-fires every NOT-done scope for one peer, leaves done scopes and other
// peers alone, returns the count, and is a no-op for an unknown peer.
func TestBootstrapResumePeer(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	rdb := db_open("db/replication.db")
	rdb.exec("insert into pair (peer, added) values ('src-peer', 1)")
	rdb.exec("insert into bootstrap (scope, peer, state, position, progress, attempts) values ('files', 'src-peer', 'queued', '', 0, 4)")
	rdb.exec("insert into bootstrap (scope, peer, state, position, progress, attempts) values ('userdbs', 'src-peer', 'incomplete', '0', 0, 2)")
	rdb.exec("insert into bootstrap (scope, peer, state, position, progress, attempts) values ('apps', 'src-peer', 'done', '', 0, 0)")
	rdb.exec("insert into bootstrap (scope, peer, state, position, progress, attempts) values ('files', 'other-peer', 'queued', '', 0, 0)")

	var mu sync.Mutex
	var fileScopes, dbScopes []string
	orig_file := replication_bootstrap_file_manifest_fetch
	orig_db := replication_bootstrap_db_manifest_fetch
	replication_bootstrap_file_manifest_fetch = func(peer, scope, prefix string) {
		mu.Lock()
		fileScopes = append(fileScopes, peer+"/"+scope)
		mu.Unlock()
	}
	replication_bootstrap_db_manifest_fetch = func(peer, scope, user string) {
		mu.Lock()
		dbScopes = append(dbScopes, peer+"/"+scope)
		mu.Unlock()
	}
	defer func() {
		replication_bootstrap_file_manifest_fetch = orig_file
		replication_bootstrap_db_manifest_fetch = orig_db
	}()

	if n := bootstrap_resume_peer("src-peer"); n != 2 {
		t.Errorf("bootstrap_resume_peer returned %d, want 2 (files + userdbs; apps is done)", n)
	}

	for i := 0; i < 50; i++ {
		mu.Lock()
		done := len(fileScopes) >= 1 && len(dbScopes) >= 1
		mu.Unlock()
		if done {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(fileScopes) != 1 || fileScopes[0] != "src-peer/files" {
		t.Errorf("file refires = %v, want [src-peer/files] (other-peer must not be touched)", fileScopes)
	}
	if len(dbScopes) != 1 || dbScopes[0] != "src-peer/userdbs" {
		t.Errorf("db refires = %v, want [src-peer/userdbs]", dbScopes)
	}

	// The done scope is left as done; the retried scopes reset to queued
	// with backoff cleared (attempts=0).
	if state, _ := bootstrap_get_state(bootstrap_scope_apps, "src-peer"); state != bootstrap_state_done {
		t.Errorf("done scope mutated by resume: state=%q", state)
	}
	if got := bootstrap_get_failed(bootstrap_scope_files, "src-peer"); got != 0 {
		t.Errorf("files failed after resume = %d, want 0 (reset)", got)
	}
	if attempts := rdb.integer("select attempts from bootstrap where scope='files' and peer='src-peer'"); attempts != 0 {
		t.Errorf("files attempts after resume = %d, want 0 (backoff cleared)", attempts)
	}

	// Unknown peer is a no-op.
	if n := bootstrap_resume_peer("nobody"); n != 0 {
		t.Errorf("bootstrap_resume_peer(unknown) = %d, want 0", n)
	}
}

// TestBootstrapPhaseDefersBulk is the §2 manifests-first guarantee: a bulk
// drive enqueued while a peer is in its manifest phase must NOT run until
// the phase ends (so a multi-GB transfer can't start mid-phase and starve
// another scope's manifest read), and a drive enqueued with no phase
// active runs immediately (the resume / retry path).
func TestBootstrapPhaseDefersBulk(t *testing.T) {
	// In a phase: deferred until phase_end.
	ran := make(chan struct{}, 1)
	bootstrap_phase_begin("phase-peer")
	bootstrap_phase_drive("phase-peer", "files", func() { ran <- struct{}{} })

	select {
	case <-ran:
		t.Fatal("bulk drive ran during the manifest phase; it must be deferred")
	case <-time.After(100 * time.Millisecond):
		// Correct: still deferred.
	}

	bootstrap_phase_end("phase-peer")
	select {
	case <-ran:
		// Correct: released on phase end.
	case <-time.After(2 * time.Second):
		t.Fatal("bulk drive did not run after phase end")
	}

	// No phase active: runs immediately.
	ran2 := make(chan struct{}, 1)
	bootstrap_phase_drive("no-phase-peer", "files", func() { ran2 <- struct{}{} })
	select {
	case <-ran2:
		// Correct.
	case <-time.After(2 * time.Second):
		t.Fatal("bulk drive with no active phase did not run")
	}
}

// TestBootstrapBulkRunStarvationKeepsProgressFresh is the #33 fix: a bulk
// drive starved waiting for a concurrency slot (all slots held by a bigger
// scope) must refresh its scope's progress timestamp, so the retry driver
// treats it as starved (correctly waiting), not stalled, and doesn't
// needlessly re-fire its manifest.
func TestBootstrapBulkRunStarvationKeepsProgressFresh(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	// Shorten the touch interval, and always leave the global semaphore empty
	// for other tests.
	orig := bootstrap_bulk_touch
	bootstrap_bulk_touch = 15 * time.Millisecond
	defer func() { bootstrap_bulk_touch = orig }()
	defer func() {
		for len(bootstrap_bulk_sem) > 0 {
			<-bootstrap_bulk_sem
		}
	}()

	rdb := db_open("db/replication.db")
	old := now() - 1000
	rdb.exec("insert into bootstrap (scope, peer, state, position, progress, attempts) values ('apps', 'p', 'active', '5', ?, 0)", old)

	// Fill every slot so the drive cannot acquire and must wait.
	for i := 0; i < bootstrap_bulk_concurrency; i++ {
		bootstrap_bulk_sem <- struct{}{}
	}

	ran := make(chan struct{}, 1)
	go bootstrap_bulk_run("p", "apps", func() { ran <- struct{}{} })

	// While starved, progress must advance past the stale value (touch fired).
	time.Sleep(80 * time.Millisecond)
	if got := int64(rdb.integer("select progress from bootstrap where scope='apps' and peer='p'")); got <= old {
		t.Fatalf("starved drive did not refresh progress: got %d, want > %d", got, old)
	}

	// It must NOT have run yet — no slot was freed.
	select {
	case <-ran:
		t.Fatal("drive ran before a slot was freed")
	default:
	}

	// Free one slot; the drive acquires and runs.
	<-bootstrap_bulk_sem
	select {
	case <-ran:
	case <-time.After(2 * time.Second):
		t.Fatal("drive did not run after a slot was freed")
	}
}

// The 2026-05-21 "database disk image is malformed" crash (bootstrap
// renamed a new .db into place but left the previous WAL beside it, which
// the next open replayed against the new file) is now prevented by design:
// bootstrap_db_swap renames the rebuilt file in, removes the stale -wal/-shm
// sidecars, and reopens fresh pools under databases_lock, so no connection
// ever replays an orphaned WAL. TestBootstrapDbSwap
// (replication_bootstrap_swap_test.go) covers the cutover end to end.

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
	users_root := filepath.Join(data_dir, "users")

	_ = os.MkdirAll(filepath.Join(users_root, "alice", "feed", "files"), 0o755)
	contents := map[string]string{
		"alice/feed/files/post1.md": "first post",
		"alice/feed/files/post2.md": "second post body",
		"alice/feed/files/post3.md": "third",
	}
	for rel, body := range contents {
		_ = os.MkdirAll(filepath.Dir(filepath.Join(users_root, rel)), 0o755)
		if err := os.WriteFile(filepath.Join(users_root, rel), []byte(body), 0o644); err != nil {
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
	orig_fetch := bootstrap_file_chunk_fetch
	defer func() { bootstrap_file_chunk_fetch = orig_fetch }()
	bootstrap_file_chunk_fetch = func(peer, scope, path string, offset, length int64) (*BootstrapFileChunk, error) {
		data, eof, err := bootstrap_read_chunk(scope, path, offset, length)
		if err != nil {
			return nil, err
		}
		return &BootstrapFileChunk{Scope: scope, Path: path, Offset: offset, Data: data, EOF: eof}, nil
	}
	// Override the driver to run synchronously so the test can assert
	// state after the apply returns. The real driver is `go`-spawned.
	orig_driver := bootstrap_file_scope_driver
	defer func() { bootstrap_file_scope_driver = orig_driver }()
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

// ============================================================
// pair-join backfill tests
// (was replication_pair_backfill_test.go)
// ============================================================

// TestPairBackfillEmitsForEveryReplicatedRow: seed every replicated
// system table with a row, fire replication_pair_backfill_impl
// (bypassing the test-setup stub), and confirm a system-set or
// system-row emit fires for every row.
func TestPairBackfillEmitsForEveryReplicatedRow(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()

	// Source-side seed data.
	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username, status) values ('u-alice', 'alice@example.org', 'active')")
	udb.exec("insert into entities (id, private, fingerprint, user, class, name) values (?, ?, ?, ?, ?, ?)",
		test_entity_id('a'), "private-key", "fp", "u-alice", "identity", "Alice")

	sdb := db_open("db/settings.db")
	sdb.exec("create table if not exists settings (name text primary key, value text not null)")
	sdb.exec("insert into settings (name, value) values ('server_name', 'test-server')")
	sdb.exec("insert into settings (name, value) values ('locale', 'en')")

	adb := db_open("db/apps.db")
	adb.exec("create table if not exists classes (class text primary key, app text not null)")
	adb.exec("create table if not exists services (service text primary key, app text not null)")
	adb.exec("create table if not exists paths (path text primary key, app text not null)")
	adb.exec("create table if not exists apps (app text primary key, installed integer not null default 0)")
	adb.exec("create table if not exists versions (app text primary key, version text, track text)")
	adb.exec("create table if not exists tracks (app text not null, track text not null, version text not null, primary key (app, track))")
	adb.exec("insert into classes (class, app) values ('wiki', 'app-wiki-123')")
	adb.exec("insert into services (service, app) values ('feed', 'app-feed-456')")
	adb.exec("insert into paths (path, app) values ('feed', 'app-feed-456')")
	adb.exec("insert into apps (app, installed) values ('app-wiki-123', 100)")
	adb.exec("insert into versions (app, version, track) values ('app-wiki-123', '1.2', 'stable')")
	adb.exec("insert into tracks (app, track, version) values ('app-wiki-123', 'stable', '1.2')")

	ddb := db_open("db/domains.db")
	ddb.exec("create table if not exists domains (domain text primary key, verified integer not null default 0, token text not null default '', tls integer not null default 0, created integer not null default 0, updated integer not null default 0)")
	ddb.exec("create table if not exists routes (domain text not null, path text not null, method text not null default '', target text not null default '', context text not null default '', owner text not null default '', priority integer not null default 0, enabled integer not null default 1, created integer not null default 0, updated integer not null default 0, primary key (domain, path))")
	ddb.exec("create table if not exists delegations (domain text not null, path text not null, owner text not null, created integer not null default 0, primary key (domain, path, owner))")
	ddb.exec("insert into domains (domain, verified, token, tls, created, updated) values ('example.org', 1, 'tok', 1, 100, 100)")
	ddb.exec("insert into routes (domain, path, method, target, owner) values ('example.org', '/', 'GET', 'app-feed-456', 'u-alice')")
	ddb.exec("insert into delegations (domain, path, owner, created) values ('example.org', '/sub', 'u-alice', 100)")

	// Capture every emit.
	var setEmits []struct{ db, table, row, field, value string }
	var rowEmits []struct {
		db, table string
		key       map[string]string
		cols      map[string]string
		del       bool
	}
	var transferred []string

	orig_set := replication_system_set_to_peer_var
	orig_row := replication_system_row_to_peer_var
	orig_transfer := replication_transfer_keys_var
	replication_system_set_to_peer_var = func(peer, db, table, row, field, value string) {
		setEmits = append(setEmits, struct{ db, table, row, field, value string }{db, table, row, field, value})
	}
	replication_system_row_to_peer_var = func(peer, db, table string, key, cols map[string]string, del bool) {
		rowEmits = append(rowEmits, struct {
			db, table string
			key       map[string]string
			cols      map[string]string
			del       bool
		}{db, table, key, cols, del})
	}
	replication_transfer_keys_var = func(uid, peer string) bool {
		transferred = append(transferred, uid)
		return true
	}
	defer func() {
		replication_system_set_to_peer_var = orig_set
		replication_system_row_to_peer_var = orig_row
		replication_transfer_keys_var = orig_transfer
	}()

	replication_pair_backfill_impl("peer-NEW")

	// 1 user transferred
	if len(transferred) != 1 || transferred[0] != "u-alice" {
		t.Errorf("users transferred = %v, want [u-alice]", transferred)
	}

	// system-set emits: 2 settings + 1 class + 1 service + 1 path + 1 install = 6
	if len(setEmits) != 6 {
		t.Errorf("system-set emits = %d, want 6 (2 settings + 3 two-col + 1 install)", len(setEmits))
	}
	// system-row emits: 1 version + 1 track + 1 domain + 1 route + 1 delegation = 5
	if len(rowEmits) != 5 {
		t.Errorf("system-row emits = %d, want 5 (1 version + 1 track + 1 domain + 1 route + 1 delegation)", len(rowEmits))
	}

	// Spot-check a couple of the emit shapes.
	found_settings := false
	for _, e := range setEmits {
		if e.db == "settings" && e.table == "settings" && e.row == "server_name" && e.field == "value" && e.value == "test-server" {
			found_settings = true
		}
	}
	if !found_settings {
		t.Errorf("settings.server_name emit missing from %+v", setEmits)
	}

	found_route := false
	for _, e := range rowEmits {
		if e.db == "domains" && e.table == "routes" && e.key["domain"] == "example.org" && e.key["path"] == "/" {
			found_route = true
		}
	}
	if !found_route {
		t.Errorf("domains.routes emit missing from %+v", rowEmits)
	}
}

// TestPairBackfillSkipsEmptyPeer: empty peer is a no-op.
func TestPairBackfillSkipsEmptyPeer(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	called := false
	orig_transfer := replication_transfer_keys_var
	replication_transfer_keys_var = func(uid, peer string) bool { called = true; return true }
	defer func() { replication_transfer_keys_var = orig_transfer }()

	replication_pair_backfill_impl("")
	if called {
		t.Error("backfill ran on empty peer; should be no-op")
	}
}

// TestJournalSetupIdempotent (#424): journal_setup is memoless, so calling it
// again after the journal table has been wiped (e.g. a bootstrap page-copy
// replaced the file) re-creates the table — journaled writes never hit "no such
// table: journal". The old lazy journal_ensure cached "already ensured" and
// skipped the re-create on a stale cache; the eager memoless setup cannot.
func TestJournalSetupIdempotent(t *testing.T) {
	orig := data_dir
	data_dir = t.TempDir()
	defer func() { data_dir = orig }()
	if err := os.MkdirAll(filepath.Join(data_dir, "db"), 0o755); err != nil {
		t.Fatal(err)
	}
	db := db_open("db/x.db")
	db.journal_setup()
	if has, _ := db.exists("select 1 from sqlite_master where type='table' and name='journal'"); !has {
		t.Fatal("journal_setup must create the journal table")
	}

	// Simulate a bootstrap page-copy wiping the journal table.
	db.exec("drop table journal")

	// Memoless: the next setup re-creates it (the stale-cache #424 bug cannot recur).
	db.journal_setup()
	if has, _ := db.exists("select 1 from sqlite_master where type='table' and name='journal'"); !has {
		t.Fatal("journal_setup must re-create the journal table after it was dropped")
	}
}

// TestReplicationPairKeysTransfer (#69) checks that a user's identity is pushed
// to every whole-server pair member (skipping self) via the keys-transfer — the
// path that lets a first-entity signup on a paired host reach the partner
// instead of stranding behind the signed op channel it can't bootstrap.
func TestReplicationPairKeysTransfer(t *testing.T) {
	orig := data_dir
	data_dir = t.TempDir()
	defer func() { data_dir = orig }()
	if err := os.MkdirAll(filepath.Join(data_dir, "db"), 0o755); err != nil {
		t.Fatal(err)
	}
	rdb := db_open("db/replication.db")
	rdb.exec("create table pair (peer text primary key, added integer, role text)")
	rdb.exec("insert into pair (peer) values ('peerA'), ('peerB'), ('self')")

	savedNet := net_id
	net_id = "self"
	defer func() { net_id = savedNet }()

	origTransfer := replication_transfer_keys_var
	defer func() { replication_transfer_keys_var = origTransfer }()
	got := make(chan string, 8)
	replication_transfer_keys_var = func(uid, peer string) bool {
		got <- uid + "|" + peer
		return true
	}

	replication_pair_keys_transfer("u1")

	seen := map[string]bool{}
	deadline := time.After(2 * time.Second)
	for len(seen) < 2 {
		select {
		case c := <-got:
			seen[c] = true
		case <-deadline:
			t.Fatalf("timed out waiting for transfers; got %v", seen)
		}
	}
	if !seen["u1|peerA"] || !seen["u1|peerB"] {
		t.Errorf("expected transfers to peerA and peerB, got %v", seen)
	}
	// self must be skipped — no third call should arrive.
	select {
	case c := <-got:
		t.Errorf("unexpected transfer (self should be skipped): %s", c)
	case <-time.After(150 * time.Millisecond):
	}
}

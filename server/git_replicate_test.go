// Live replication of the repositories app's git trees (#105). The bare git repos live
// at users/<uid>/<app>/<repo>/{objects,refs,...} — OUTSIDE files/ — so the files/-rooted
// eager file push didn't carry them and they replicated only via the periodic bulk
// bootstrap. These cover the two halves of the fix: the file/push "app" root + its db/
// guard, and the post-write delta walk that emits the new objects/refs.
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFilePushRootingAndGuard(t *testing.T) {
	orig := data_dir
	data_dir = "/data"
	defer func() { data_dir = orig }()

	if got := file_push_base("u", "a", ""); got != "/data/users/u/a/files" {
		t.Errorf("files root = %q, want /data/users/u/a/files", got)
	}
	if got := file_push_base("u", "a", file_push_root_app); got != "/data/users/u/a" {
		t.Errorf("app root = %q, want /data/users/u/a", got)
	}

	// The app root's only extra gate: it must never let a push land in the app's db/
	// dir (that would corrupt a replicated SQLite DB out-of-band).
	if file_push_path_allowed(file_push_root_app, "db/repositories.db") {
		t.Error("app-rooted push to db/ must be refused")
	}
	if file_push_path_allowed(file_push_root_app, "db") {
		t.Error("app-rooted push to db must be refused")
	}
	if file_push_path_allowed(file_push_root_app, "../db/x.db") {
		t.Error("app-rooted push escaping via .. to db/ must be refused")
	}
	if !file_push_path_allowed(file_push_root_app, "12repo/objects/ab/cdef") {
		t.Error("app-rooted git object path must be allowed")
	}
	// The files root is already confined below files/, so it carries no db/ gate.
	if !file_push_path_allowed("", "db/whatever") {
		t.Error("files root needs no db gate")
	}
}

func TestGitReplicateRepoDelta(t *testing.T) {
	orig := data_dir
	data_dir = t.TempDir()
	defer func() { data_dir = orig }()

	owner := &User{UID: "u1"}
	app := &App{id: "12app"}
	entity := "12repo"
	repo := git_repo_path(owner, app, entity)

	mk := func(rel, content string) string {
		p := filepath.Join(repo, rel)
		os.MkdirAll(filepath.Dir(p), 0o755)
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	mk("HEAD", "ref: refs/heads/main\n")
	mk("config", "[core]\n")
	oldObj := mk("objects/aa/oldobject", "old")
	mk("objects/bb/newobject", "new")
	mk("refs/heads/main", "deadbeef\n")
	mk("hooks/post-receive", "#!/bin/sh\n")

	// HEAD/config + the old object predate the push; the new object + ref are written
	// during it.
	past := time.Now().Add(-1 * time.Hour)
	for _, p := range []string{filepath.Join(repo, "HEAD"), filepath.Join(repo, "config"), oldObj} {
		if err := os.Chtimes(p, past, past); err != nil {
			t.Fatal(err)
		}
	}
	since := time.Now().Add(-1 * time.Minute)

	var pushed []string
	origEmit := replication_emit_file_push_rooted
	replication_emit_file_push_rooted = func(uid, a, root, path string) {
		if uid != "u1" || a != "12app" || root != file_push_root_app {
			t.Errorf("emit args = (%q,%q,%q), want (u1,12app,app)", uid, a, root)
		}
		pushed = append(pushed, path)
	}
	defer func() { replication_emit_file_push_rooted = origEmit }()

	git_replicate_repo_delta(owner, app, entity, repo, since)

	has := func(p string) bool {
		for _, x := range pushed {
			if x == p {
				return true
			}
		}
		return false
	}
	// HEAD + config always ship, even though they predate `since` (a fresh repo's
	// pointers must reach the replica or the clone is broken).
	if !has("12repo/HEAD") || !has("12repo/config") {
		t.Errorf("HEAD/config must always ship; got %v", pushed)
	}
	// The objects/refs written during the push ship.
	if !has("12repo/objects/bb/newobject") {
		t.Errorf("new object must ship; got %v", pushed)
	}
	if !has("12repo/refs/heads/main") {
		t.Errorf("advanced ref must ship; got %v", pushed)
	}
	// The pre-existing object does NOT re-ship (it's already on the replica).
	if has("12repo/objects/aa/oldobject") {
		t.Errorf("unchanged object must not re-ship; got %v", pushed)
	}
	// Hooks are server-side executables, never git content — never ship.
	for _, p := range pushed {
		if strings.Contains(p, "hooks/") {
			t.Errorf("hooks must never ship; got %v", pushed)
		}
	}
}

// TestFilePushReceiverAppRooted drives the receiver over the in-memory stream harness
// with Root="app": the git object must land at <uid>/<app>/<entity>/..., NOT under
// files/. This is the inbound half of the wire path #105 adds.
func TestFilePushReceiverAppRooted(t *testing.T) {
	orig := data_dir
	data_dir = t.TempDir()
	defer func() { data_dir = orig }()

	udb := db_open("db/users.db")
	udb.exec("create table if not exists users (uid text, username text)")
	udb.exec("insert into users (uid, username) values ('u1', 'u1@example.com')")
	// Authorize the pushing peer for u1's files (#145 file/push gate).
	rdb := db_open("db/replication.db")
	rdb.exec("create table if not exists hosts (user text, peer text, added integer, ack integer)")
	rdb.exec("insert into hosts (user, peer, added, ack) values ('u1', 'peerX', 1, 0)")

	const size = int64(2000)
	body := bytes.Repeat([]byte("G"), int(size))
	header := cbor_encode(&FilePushHeader{User: "u1", App: "app1", Path: "12repo/objects/ab/cdef", Size: size, Root: file_push_root_app, Resume: true})
	var wire bytes.Buffer
	wire.Write(header)
	wire.Write(body)
	var reply bytes.Buffer
	s := &Stream{reader: io.NopCloser(bytes.NewReader(wire.Bytes())), writer: filePushTestWriteCloser{&reply}}
	replication_file_push_event(&Event{from: "signer", peer: "peerX", stream: s})

	appRooted := filepath.Join(data_dir, "users", "u1", "app1", "12repo", "objects", "ab", "cdef")
	got, err := os.ReadFile(appRooted)
	if err != nil {
		t.Fatalf("app-rooted file not written to %q: %v", appRooted, err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("app-rooted content mismatch: %d bytes, want %d", len(got), size)
	}
	if _, err := os.Stat(filepath.Join(data_dir, "users", "u1", "app1", "files", "12repo", "objects", "ab", "cdef")); err == nil {
		t.Fatal("file wrongly written under files/ for an app-rooted push")
	}
}

// TestFilePushReceiverAppRootedRejectsDB confirms the receiver refuses an app-rooted
// push aimed at the app's db/ directory, so a peer can never corrupt a replicated
// SQLite DB out-of-band.
func TestFilePushReceiverAppRootedRejectsDB(t *testing.T) {
	orig := data_dir
	data_dir = t.TempDir()
	defer func() { data_dir = orig }()

	udb := db_open("db/users.db")
	udb.exec("create table if not exists users (uid text, username text)")
	udb.exec("insert into users (uid, username) values ('u1', 'u1@example.com')")

	const size = int64(100)
	body := bytes.Repeat([]byte("X"), int(size))
	header := cbor_encode(&FilePushHeader{User: "u1", App: "app1", Path: "db/app1.db", Size: size, Root: file_push_root_app, Resume: true})
	var wire bytes.Buffer
	wire.Write(header)
	wire.Write(body)
	var reply bytes.Buffer
	s := &Stream{reader: io.NopCloser(bytes.NewReader(wire.Bytes())), writer: filePushTestWriteCloser{&reply}}
	replication_file_push_event(&Event{from: "signer", peer: "peerX", stream: s})

	if _, err := os.Stat(filepath.Join(data_dir, "users", "u1", "app1", "db", "app1.db")); err == nil {
		t.Fatal("app-rooted push to db/ must be refused, but the DB file was written")
	}
}

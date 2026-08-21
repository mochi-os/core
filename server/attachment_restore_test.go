// Mochi server: the restore path exports attachment stores before activation.
// restore_apply is the one path that adds user data to a running server, so it
// must export itself: otherwise the first request's migration reads "no rows"
// and consumes the schema version with the rows still in app.db.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestAttachmentExportUserScopesToOneUser: the per-user export handles exactly
// the named user's stores - the restore path must not touch anyone else's, and
// must leave the restored user with the same end state the startup sweep
// produces (export written, table dropped, generated variants removed).
func TestAttachmentExportUserScopesToOneUser(t *testing.T) {
	tmp_dir, err := os.MkdirTemp("", "mochi_restore_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmp_dir)

	orig_data_dir := data_dir
	data_dir = tmp_dir
	defer func() { data_dir = orig_data_dir }()

	create := "create table attachments ( id text not null primary key, object text not null, entity text not null default '', name text not null, size integer not null, content_type text not null default '', creator text not null default '', caption text not null default '', description text not null default '', rank integer not null default 0, created integer not null )"
	insert := "insert into attachments ( id, object, entity, name, size, content_type, creator, caption, description, rank, created ) values ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )"

	// The restored user, with one own row whose bytes are on disk.
	restored := db_open("users/restored/feeds/app.db")
	restored.exec(create)
	restored.exec(insert, "0123456789abcdef0123456789abcdef", "post/1", "", "photo.jpg", 8, "image/jpeg", "", "", "", 1, 1700000000)
	files := filepath.Join(tmp_dir, "users", "restored", "feeds", "files")
	os.MkdirAll(filepath.Join(files, "thumbnails"), 0755)
	os.WriteFile(filepath.Join(files, "0123456789abcdef0123456789abcdef_photo.jpg"), []byte("original"), 0644)
	os.WriteFile(filepath.Join(files, "thumbnails", "0123456789abcdef0123456789abcdef_photo_thumbnail.jpg"), []byte("x"), 0644)

	// A bystander whose store the restore must not touch.
	bystander := db_open("users/bystander/feeds/app.db")
	bystander.exec(create)
	bystander.exec(insert, "fedcba9876543210fedcba9876543210", "post/2", "", "other.png", 1, "image/png", "", "", "", 1, 1700000001)

	exported, dropped := attachment_export_user("restored")
	if exported != 1 || dropped != 1 {
		t.Fatalf("attachment_export_user = (%d, %d), want (1, 1)", exported, dropped)
	}

	// The restored user's store reached the swept end state.
	data, err := os.ReadFile(filepath.Join(files, attachment_export_file))
	if err != nil {
		t.Fatalf("export not written: %v", err)
	}
	var entries []map[string]any
	if err := json.Unmarshal(data, &entries); err != nil || len(entries) != 1 {
		t.Fatalf("export holds %d entries (err %v), want 1", len(entries), err)
	}
	if entries[0]["id"] != "0123456789abcdef0123456789abcdef" || entries[0]["file"] != "0123456789abcdef0123456789abcdef_photo.jpg" {
		t.Errorf("exported entry = %v", entries[0])
	}
	if present, _ := restored.exists("select 1 from sqlite_master where type='table' and name='attachments'"); present {
		t.Error("the restored user's attachments table survives")
	}
	if file_exists(filepath.Join(files, "thumbnails")) {
		t.Error("the restored user's generated variants survive")
	}
	if !file_exists(filepath.Join(files, "0123456789abcdef0123456789abcdef_photo.jpg")) {
		t.Error("the restored user's original bytes were removed")
	}

	// The bystander is untouched: table intact, no export.
	if present, _ := bystander.exists("select 1 from sqlite_master where type='table' and name='attachments'"); !present {
		t.Error("the bystander's attachments table was dropped")
	}
	if file_exists(filepath.Join(tmp_dir, "users", "bystander", "feeds", "files", attachment_export_file)) {
		t.Error("the bystander grew an export")
	}
}

// TestRestoreExportsBeforeActivation pins the ordering that IS the fix: the
// export must run inside restore_apply after the swap and before the account
// flips active, because activation is what releases the gates and lets the
// first request run the migration that would otherwise read "no rows".
func TestRestoreExportsBeforeActivation(t *testing.T) {
	body := function_body(t, "auth_restore.go", "func restore_apply(")

	export := strings.Index(body, "attachment_export_user(")
	if export < 0 {
		t.Fatal("restore_apply does not export the restored user's attachment stores; a pre-migration bundle's rows are stranded by the first request's migration")
	}
	swap := strings.Index(body, "restore_swap(")
	if swap < 0 {
		t.Fatal("restore_apply no longer calls restore_swap; this test is reading the wrong function")
	}
	active := strings.Index(body, "status='active'")
	if active < 0 {
		t.Fatal("restore_apply no longer flips the account active; this test is reading the wrong function")
	}
	if export < swap {
		t.Error("the export runs before the swap, so it walks the placeholder files rather than the restored ones")
	}
	if export > active {
		t.Error("the export runs after activation, so a request can race it into the stranding this exists to prevent")
	}
}

// Mochi server: attachment metadata as a versioned register. A caption/description
// edit must bump the row's revision so concurrent edits from the user's hosts
// converge, and a delete must tombstone the row (hidden from the live view but
// retained) so a concurrent edit can't resurrect it.
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import "testing"

func TestAttachmentMetaVersioned(t *testing.T) {
	cleanup, user_uid, app_id := setup_sql_replication_test(t)
	defer cleanup()
	db := db_app_system(&User{UID: user_uid}, app_by_id(app_id))
	if db == nil {
		t.Fatal("no app-system db")
	}
	db.attachments_setup()
	db.exec("insert into attachments (id, object, entity, name, size, caption, description, rank, created, revision) values ('a1', 'o1', '', 'f.png', 10, 'old', '', 0, 100, 1)")

	db.attachment_meta_set("a1", "new", "desc")
	var r struct {
		Caption  string
		Revision int64
	}
	db.scan(&r, "select caption, revision from attachments where id='a1'")
	if r.Caption != "new" {
		t.Errorf("caption not updated: %q", r.Caption)
	}
	if r.Revision != 2 {
		t.Errorf("edit should bump revision to 2, got %d", r.Revision)
	}

	db.register_remove(reg_attachments, map[string]any{"id": "a1"})
	if n := db.integer("select count(*) from attachments where id='a1' and deleted=0"); n != 0 {
		t.Errorf("tombstoned attachment still visible in the live view")
	}
	if n := db.integer("select count(*) from attachments where id='a1'"); n != 1 {
		t.Errorf("tombstone row should remain (deleted=1), got %d rows", n)
	}
}

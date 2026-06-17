// Mochi server: Attachment unit tests
// Copyright Alistair Cunningham 2025-2026

package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/fxamacker/cbor/v2"
)

// TestAttachmentRecordWriteReplicatesMetadata locks in the fix for the
// replica-blank-photos bug: attachment metadata lives in the per-app system
// DB and used to be written with plain db.exec, so it never reached a paired
// host (the attachments table was empty on the replica and images rendered
// blank). attachment_record_write must now record an app-system exec op in the
// app.db journal for every attachment — owner-owned (entity="") and foreign
// cached reference (entity set) alike — carrying the row verbatim, including
// the entity column the receiver branches on. The journal drainer ships it
// (async) so it converges. (The owner-only eager byte push is exercised by
// file_push_test.go; here we assert the metadata convergence that was missing.)
func TestAttachmentRecordWriteReplicatesMetadata(t *testing.T) {
	cleanup, user_uid, app_id := setup_sql_replication_test(t)
	defer cleanup()

	u := &User{UID: user_uid}
	a := app_by_id(app_id)
	db := db_app_system(u, a)
	if db == nil {
		t.Fatal("no app-system db")
	}

	attachment_record_write(db, &Attachment{ID: "att-own", Object: "post-1", Entity: "", Name: "photo.jpg", Size: 3, Rank: 1, Created: 1})
	attachment_record_write(db, &Attachment{ID: "att-foreign", Object: "post-2", Entity: "entity-99", Name: "remote.jpg", Size: 5, Rank: 1, Created: 2})

	// Each write must journal an attachments app-system exec op (durably in
	// app.db, drained + shipped async by journal_manager) carrying the row's
	// id and entity value.
	rows, err := db.rows("select operation, target, args from journal where state='pending'")
	if err != nil {
		t.Fatalf("read journal: %v", err)
	}
	seen := map[string]string{} // id -> entity carried in the op
	for _, r := range rows {
		op, _ := r["operation"].(string)
		target, _ := r["target"].(string)
		if op != repl_op_exec_app_system || target != "attachments" {
			continue
		}
		var args []any
		if s, ok := r["args"].(string); ok {
			if err := cbor.Unmarshal([]byte(s), &args); err != nil {
				t.Fatalf("decode journal args: %v", err)
			}
		}
		// Args order matches the insert: id, object, entity, ...
		if len(args) < 3 {
			continue
		}
		id, _ := args[0].(string)
		entity, _ := args[2].(string)
		seen[id] = entity
	}

	if e, ok := seen["att-own"]; !ok {
		t.Error("owner attachment metadata was not replicated")
	} else if e != "" {
		t.Errorf("owner attachment op carried entity=%q, want empty", e)
	}
	if e, ok := seen["att-foreign"]; !ok {
		t.Error("foreign attachment metadata was not replicated")
	} else if e != "entity-99" {
		t.Errorf("foreign attachment op carried entity=%q, want entity-99", e)
	}

	// Sanity: the rows also landed in the local app-system DB.
	if n := db.integer("select count(*) from attachments"); n != 2 {
		t.Errorf("local attachments rows = %d, want 2", n)
	}
}

// setup_attachment_move_test opens a fresh attachments DB under a
// throwaway data_dir, seeds three rows (id=a,b,c with ranks 1,2,3)
// scoped to the given entity, and returns the DB plus a cleanup
// closure. Used by the attachment_event_move convergence tests.
func setup_attachment_move_test(t *testing.T, entity string) (*DB, func()) {
	t.Helper()
	tmp, err := os.MkdirTemp("", "mochi_attach_move_test")
	if err != nil {
		t.Fatalf("mkdir temp: %v", err)
	}
	orig_data_dir := data_dir
	data_dir = tmp

	db := db_open("db/attachments.db")
	db.exec("create table if not exists attachments ( id text not null primary key, object text not null, entity text not null default '', name text not null, size integer not null, content_type text not null default '', creator text not null default '', caption text not null default '', description text not null default '', rank integer not null default 0, created integer not null )")
	for i, id := range []string{"a", "b", "c"} {
		db.exec("insert into attachments (id, object, entity, name, size, rank, created) values (?, ?, ?, ?, ?, ?, ?)", id, "obj1", entity, fmt.Sprintf("%s.txt", id), int64(10), int64(i+1), int64(1700000000))
	}

	cleanup := func() {
		// Mirror the setup_replication_test pattern: leave cached DB
		// handles in the map (their files vanish when the temp dir
		// is removed) and reset data_dir last.
		os.RemoveAll(tmp)
		data_dir = orig_data_dir
	}
	return db, cleanup
}

func ranks_by_id(t *testing.T, db *DB, entity string) map[string]int64 {
	t.Helper()
	rows, err := db.rows("select id, rank from attachments where entity = ? order by id", entity)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	out := map[string]int64{}
	for _, r := range rows {
		id, _ := r["id"].(string)
		rank, _ := r["rank"].(int64)
		out[id] = rank
	}
	return out
}

// TestAttachmentMoveEventPreferredRanks drives the receiver with the
// new absolute-rank payload. Per-id UPDATE; no legacy header needed,
// and the result is independent of arrival order.
func TestAttachmentMoveEventPreferredRanks(t *testing.T) {
	entity := test_entity_id('a')
	db, cleanup := setup_attachment_move_test(t, entity)
	defer cleanup()

	// Caller is moving b from rank 2 to rank 1 (and a from 1 to 2).
	e := &Event{
		from: entity,
		db:   db,
		content: map[string]any{
			"ranks": []any{
				map[string]any{"id": "a", "rank": int64(2)},
				map[string]any{"id": "b", "rank": int64(1)},
				map[string]any{"id": "c", "rank": int64(3)},
			},
		},
	}
	e.attachment_event_move()

	got := ranks_by_id(t, db, entity)
	want := map[string]int64{"a": 2, "b": 1, "c": 3}
	for id, w := range want {
		if got[id] != w {
			t.Errorf("rank for %q: got %d, want %d", id, got[id], w)
		}
	}
}

// TestAttachmentMoveEventConvergesUnderRepeat applies the same
// absolute-rank payload twice. The new path is idempotent — second
// apply changes nothing because each row is already at the target
// rank.
func TestAttachmentMoveEventConvergesUnderRepeat(t *testing.T) {
	entity := test_entity_id('b')
	db, cleanup := setup_attachment_move_test(t, entity)
	defer cleanup()

	payload := []any{
		map[string]any{"id": "a", "rank": int64(3)},
		map[string]any{"id": "b", "rank": int64(1)},
		map[string]any{"id": "c", "rank": int64(2)},
	}
	e := &Event{
		from:    entity,
		db:      db,
		content: map[string]any{"ranks": payload},
	}
	e.attachment_event_move()
	first := ranks_by_id(t, db, entity)
	e.attachment_event_move()
	second := ranks_by_id(t, db, entity)
	for id := range first {
		if first[id] != second[id] {
			t.Errorf("repeat apply for %q changed: first=%d second=%d", id, first[id], second[id])
		}
	}
	want := map[string]int64{"a": 3, "b": 1, "c": 2}
	for id, w := range want {
		if first[id] != w {
			t.Errorf("rank for %q: got %d, want %d", id, first[id], w)
		}
	}
}

// TestAttachmentMoveEventLegacyFallback exercises the rank-relative
// shift handler that runs when an older peer omits the ranks list.
// Required for backward compatibility during the cross-release
// rollout window: the new sender emits BOTH the legacy old_rank
// header AND the new ranks list in the same message, so old
// receivers (which don't know about ranks) fall through here.
func TestAttachmentMoveEventLegacyFallback(t *testing.T) {
	entity := test_entity_id('d')
	db, cleanup := setup_attachment_move_test(t, entity)
	defer cleanup()

	// Encode the attachment dict the legacy sender put in the
	// segment: id "b" being moved from rank 2 -> rank 1.
	var buf bytes.Buffer
	enc := cbor.NewEncoder(&buf)
	if err := enc.Encode(map[string]any{
		"id":     "b",
		"object": "obj1",
		"rank":   int64(1),
	}); err != nil {
		t.Fatalf("cbor encode: %v", err)
	}

	stream := &Stream{
		reader:  io.NopCloser(&buf),
		decoder: cbor_decode_mode.NewDecoder(&buf),
	}
	e := &Event{
		from:    entity,
		db:      db,
		stream:  stream,
		content: map[string]any{"old_rank": "2"},
	}
	e.attachment_event_move()

	got := ranks_by_id(t, db, entity)
	// a was at 1, b was at 2, c was at 3. Move b 2->1 shifts a down to 2.
	want := map[string]int64{"a": 2, "b": 1, "c": 3}
	for id, w := range want {
		if got[id] != w {
			t.Errorf("legacy fallback rank for %q: got %d, want %d", id, got[id], w)
		}
	}
}

// TestAttachmentMoveEventConcurrentApplyConverges fires two
// independent move events against the same DB in different orders
// and asserts that the final state matches the last-applied payload
// regardless of interleaving. This is the property the legacy
// rank-relative shift handler did NOT satisfy and that finding A of
// the #69 audit called out.
func TestAttachmentMoveEventConcurrentApplyConverges(t *testing.T) {
	entity := test_entity_id('c')

	// Two concurrent moves on the same object:
	//   host-1 moves a from 1 -> 3 (shifts b,c down to 1,2)
	//   host-2 moves c from 3 -> 1 (shifts a,b down to 2,3)
	move_one := []any{
		map[string]any{"id": "a", "rank": int64(3)},
		map[string]any{"id": "b", "rank": int64(1)},
		map[string]any{"id": "c", "rank": int64(2)},
	}
	move_two := []any{
		map[string]any{"id": "a", "rank": int64(2)},
		map[string]any{"id": "b", "rank": int64(3)},
		map[string]any{"id": "c", "rank": int64(1)},
	}

	// Replica order one-then-two.
	db, cleanup := setup_attachment_move_test(t, entity)
	e1 := &Event{from: entity, db: db, content: map[string]any{"ranks": move_one}}
	e1.attachment_event_move()
	e2 := &Event{from: entity, db: db, content: map[string]any{"ranks": move_two}}
	e2.attachment_event_move()
	got_a := ranks_by_id(t, db, entity)
	cleanup()

	// Replica order two-then-one.
	db, cleanup = setup_attachment_move_test(t, entity)
	e2b := &Event{from: entity, db: db, content: map[string]any{"ranks": move_two}}
	e2b.attachment_event_move()
	e1b := &Event{from: entity, db: db, content: map[string]any{"ranks": move_one}}
	e1b.attachment_event_move()
	got_b := ranks_by_id(t, db, entity)
	cleanup()

	// Last write wins per id is the contract. Either replica's final
	// state must equal the LAST-applied payload exactly.
	want_a := map[string]int64{"a": 2, "b": 3, "c": 1}
	want_b := map[string]int64{"a": 3, "b": 1, "c": 2}
	for id, w := range want_a {
		if got_a[id] != w {
			t.Errorf("order one-then-two, rank for %q: got %d, want %d", id, got_a[id], w)
		}
	}
	for id, w := range want_b {
		if got_b[id] != w {
			t.Errorf("order two-then-one, rank for %q: got %d, want %d", id, got_b[id], w)
		}
	}
}

// move_locally runs the same SQL the api_attachment_move builtin does -
// shifts the affected siblings, rewrites the moved row, then queries
// the post-shift absolute-rank snapshot the federation event would
// carry. Bypasses Starlark and message.send so two-host concurrent-
// move scenarios can be exercised in-process without the libp2p / queue
// stack. Returns (ranks, old_rank) the way attachment_notify_move would
// see them.
func move_locally(t *testing.T, db *DB, entity, object, id string, position int) ([]map[string]any, int) {
	t.Helper()
	row, err := db.row("select rank from attachments where id = ? and entity = ?", id, entity)
	if err != nil || row == nil {
		t.Fatalf("read pre-move rank for %q: row=%v err=%v", id, row, err)
	}
	old_rank := int(row["rank"].(int64))
	new_rank := position
	if old_rank != new_rank {
		if new_rank < old_rank {
			db.exec("update attachments set rank = rank + 1 where object = ? and entity = ? and rank >= ? and rank < ?", object, entity, new_rank, old_rank)
		} else {
			db.exec("update attachments set rank = rank - 1 where object = ? and entity = ? and rank > ? and rank <= ?", object, entity, old_rank, new_rank)
		}
		db.exec("update attachments set rank = ? where id = ? and entity = ?", new_rank, id, entity)
	}
	rows, err := db.rows("select id, rank from attachments where object = ? and entity = ?", object, entity)
	if err != nil {
		t.Fatalf("read post-move snapshot: %v", err)
	}
	ranks := make([]map[string]any, 0, len(rows))
	for _, r := range rows {
		ranks = append(ranks, map[string]any{
			"id":   r["id"].(string),
			"rank": r["rank"].(int64),
		})
	}
	return ranks, old_rank
}

// TestAttachmentMoveRoundTripConvergesOnSubscriber is the Path-A
// integration test for task #79 / validation of #76. Models a
// two-host federated entity B (hosts h1, h2) where both replicas
// independently call api_attachment_move on the same object, AND a
// downstream subscriber C that receives both notify events. Asserts
// C's final rank state matches the last-applied event exactly (LWW
// per id) and that swapping the arrival order at C changes C to the
// other producer's snapshot. That's the contract Option B promises:
// no rank divergence under concurrent producers; per-replica order of
// arrival determines whose snapshot wins.
func TestAttachmentMoveRoundTripConvergesOnSubscriber(t *testing.T) {
	// Seed three independent DBs sharing the same entity owner key.
	// Entity owner identity is what attachment_event_move keys writes
	// against (the `entity = ?` predicate on the UPDATE), so all three
	// DBs must agree on the entity id for h1 and h2's payloads to
	// land on C's rows.
	entity := test_entity_id('e')

	h1, cleanup1 := setup_attachment_move_test(t, entity)
	defer cleanup1()
	h2, cleanup2 := setup_attachment_move_test(t, entity)
	defer cleanup2()
	c, cleanup3 := setup_attachment_move_test(t, entity)
	defer cleanup3()

	// h1: move b 2 -> 1 (a 1->2, b 2->1, c 3->3).
	ranks_h1, _ := move_locally(t, h1, entity, "obj1", "b", 1)
	// h2: move c 3 -> 1 (a 1->2, b 2->3, c 3->1).
	ranks_h2, _ := move_locally(t, h2, entity, "obj1", "c", 1)

	// Verify producer-side snapshots are what we'd expect.
	want_h1 := map[string]int64{"a": 2, "b": 1, "c": 3}
	want_h2 := map[string]int64{"a": 2, "b": 3, "c": 1}
	for _, r := range ranks_h1 {
		if w := want_h1[r["id"].(string)]; r["rank"].(int64) != w {
			t.Errorf("h1 snapshot %q: got %d, want %d", r["id"], r["rank"], w)
		}
	}
	for _, r := range ranks_h2 {
		if w := want_h2[r["id"].(string)]; r["rank"].(int64) != w {
			t.Errorf("h2 snapshot %q: got %d, want %d", r["id"], r["rank"], w)
		}
	}

	// Cross-apply both events at C in arrival order h1-then-h2. Final
	// state must equal h2's snapshot.
	deliver := func(db *DB, ranks []map[string]any) {
		entries := make([]any, 0, len(ranks))
		for _, r := range ranks {
			entries = append(entries, map[string]any{"id": r["id"], "rank": r["rank"]})
		}
		e := &Event{from: entity, db: db, content: map[string]any{"ranks": entries}}
		e.attachment_event_move()
	}
	deliver(c, ranks_h1)
	deliver(c, ranks_h2)
	got_c1 := ranks_by_id(t, c, entity)
	for id, w := range want_h2 {
		if got_c1[id] != w {
			t.Errorf("C after h1->h2 delivery, rank for %q: got %d, want %d (last-applied = h2)", id, got_c1[id], w)
		}
	}

	// Swap arrival order on a fresh C. h2-then-h1 must leave C at h1.
	c.exec("update attachments set rank = ? where id = 'a' and entity = ?", int64(1), entity)
	c.exec("update attachments set rank = ? where id = 'b' and entity = ?", int64(2), entity)
	c.exec("update attachments set rank = ? where id = 'c' and entity = ?", int64(3), entity)
	deliver(c, ranks_h2)
	deliver(c, ranks_h1)
	got_c2 := ranks_by_id(t, c, entity)
	for id, w := range want_h1 {
		if got_c2[id] != w {
			t.Errorf("C after h2->h1 delivery, rank for %q: got %d, want %d (last-applied = h1)", id, got_c2[id], w)
		}
	}
}

// TestAttachmentNotifyMoveStubCaptures is the wiring check for the
// new package-level var that the test harness (Path B / task #79) and
// future federation tests use to intercept the federation emit. If
// this test breaks, attachment_notify_move was inlined back to a
// regular func and the harness lost its hook point.
func TestAttachmentNotifyMoveStubCaptures(t *testing.T) {
	original := attachment_notify_move
	defer func() { attachment_notify_move = original }()

	var captured struct {
		called bool
		ranks  []map[string]any
		notify []string
	}
	attachment_notify_move = func(app *App, owner *User, attachment map[string]any, old_rank int, ranks []map[string]any, notify []string) {
		captured.called = true
		captured.ranks = ranks
		captured.notify = notify
	}

	ranks := []map[string]any{{"id": "a", "rank": int64(2)}, {"id": "b", "rank": int64(1)}}
	attachment_notify_move(nil, nil, map[string]any{"id": "b"}, 2, ranks, []string{test_entity_id('f')})

	if !captured.called {
		t.Fatal("stub didn't capture the call")
	}
	if len(captured.ranks) != 2 || captured.ranks[1]["id"].(string) != "b" {
		t.Errorf("stub captured wrong ranks: %v", captured.ranks)
	}
	if len(captured.notify) != 1 {
		t.Errorf("stub captured wrong notify: %v", captured.notify)
	}
}

// Test attachment_content_type detection
func TestAttachmentContentType(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		expected string
	}{
		{"text file", "document.txt", "text/plain; charset=utf-8"},
		{"PNG image", "image.png", "image/png"},
		{"JPEG image", "photo.jpg", "image/jpeg"},
		{"JPEG alt", "photo.jpeg", "image/jpeg"},
		{"GIF image", "animation.gif", "image/gif"},
		{"PDF document", "report.pdf", "application/pdf"},
		{"JSON file", "data.json", "application/json"},
		{"HTML file", "page.html", "text/html; charset=utf-8"},
		{"CSS file", "style.css", "text/css; charset=utf-8"},
		{"JavaScript", "script.js", "text/javascript; charset=utf-8"},
		{"no extension", "README", "application/octet-stream"},
		{"unknown ext", "file.xyz", "application/octet-stream"},
		{"empty name", "", "application/octet-stream"},
		{"dot only", ".", "application/octet-stream"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := attachment_content_type(tt.filename)
			if result != tt.expected {
				t.Errorf("attachment_content_type(%q) = %q, want %q", tt.filename, result, tt.expected)
			}
		})
	}
}

// Test Attachment.to_map conversion
func TestAttachmentToMap(t *testing.T) {
	att := &Attachment{
		ID:          "test123",
		Object:      "post/abc",
		Entity:      "entity456",
		Name:        "document.pdf",
		Size:        1024,
		ContentType: "application/pdf",
		Creator:     "user789",
		Caption:     "Test caption",
		Description: "Test description",
		Rank:        1,
		Created:     1700000000,
	}

	m := att.to_map()

	// Verify all fields are present and correct
	if m["id"] != "test123" {
		t.Errorf("id = %v, want test123", m["id"])
	}
	if m["object"] != "post/abc" {
		t.Errorf("object = %v, want post/abc", m["object"])
	}
	if m["entity"] != "entity456" {
		t.Errorf("entity = %v, want entity456", m["entity"])
	}
	if m["name"] != "document.pdf" {
		t.Errorf("name = %v, want document.pdf", m["name"])
	}
	if m["size"] != int64(1024) {
		t.Errorf("size = %v, want 1024", m["size"])
	}
	if m["content_type"] != "application/pdf" {
		t.Errorf("content_type = %v, want application/pdf", m["content_type"])
	}
	if m["type"] != "application/pdf" {
		t.Errorf("type = %v, want application/pdf", m["type"])
	}
	if m["creator"] != "user789" {
		t.Errorf("creator = %v, want user789", m["creator"])
	}
	if m["caption"] != "Test caption" {
		t.Errorf("caption = %v, want Test caption", m["caption"])
	}
	if m["description"] != "Test description" {
		t.Errorf("description = %v, want Test description", m["description"])
	}
	if m["rank"] != 1 {
		t.Errorf("rank = %v, want 1", m["rank"])
	}
	if m["created"] != int64(1700000000) {
		t.Errorf("created = %v, want 1700000000", m["created"])
	}
	if m["image"] != false {
		t.Errorf("image = %v, want false for pdf", m["image"])
	}
	// Without app_path, url should not be set
	if _, ok := m["url"]; ok {
		t.Errorf("url should not be set without app_path, got %v", m["url"])
	}
}

// Test Attachment.to_map with app_path for URL generation
func TestAttachmentToMapWithURL(t *testing.T) {
	// Test non-image attachment with default action_path
	att := &Attachment{
		ID:   "abc123",
		Name: "document.pdf",
	}

	m := att.to_map("chat")

	if m["url"] != "/chat/attachments/abc123" {
		t.Errorf("url = %v, want /chat/attachments/abc123", m["url"])
	}
	if m["image"] != false {
		t.Errorf("image = %v, want false", m["image"])
	}
	if _, ok := m["thumbnail_url"]; ok {
		t.Errorf("thumbnail_url should not be set for non-image")
	}

	// Test image attachment with default action_path
	img_att := &Attachment{
		ID:   "img456",
		Name: "photo.jpg",
	}

	img_m := img_att.to_map("feeds")

	if img_m["url"] != "/feeds/attachments/img456" {
		t.Errorf("url = %v, want /feeds/attachments/img456", img_m["url"])
	}
	if img_m["image"] != true {
		t.Errorf("image = %v, want true", img_m["image"])
	}
	if img_m["thumbnail_url"] != "/feeds/attachments/img456/thumbnail" {
		t.Errorf("thumbnail_url = %v, want /feeds/attachments/img456/thumbnail", img_m["thumbnail_url"])
	}

	// Test with custom action_path
	custom_att := &Attachment{
		ID:   "custom789",
		Name: "file.txt",
	}

	custom_m := custom_att.to_map("myapp", "files")

	if custom_m["url"] != "/myapp/files/custom789" {
		t.Errorf("url = %v, want /myapp/files/custom789", custom_m["url"])
	}

	// Test image with custom action_path
	custom_img := &Attachment{
		ID:   "img999",
		Name: "photo.png",
	}

	custom_img_m := custom_img.to_map("gallery", "media")

	if custom_img_m["url"] != "/gallery/media/img999" {
		t.Errorf("url = %v, want /gallery/media/img999", custom_img_m["url"])
	}
	if custom_img_m["thumbnail_url"] != "/gallery/media/img999/thumbnail" {
		t.Errorf("thumbnail_url = %v, want /gallery/media/img999/thumbnail", custom_img_m["thumbnail_url"])
	}
}

// Test Attachment.attachment_url
func TestAttachmentURL(t *testing.T) {
	att := &Attachment{ID: "test123"}

	tests := []struct {
		name        string
		app_path    string
		action_path string
		entity      string
		expected    string
	}{
		{"chat default", "chat", "attachments", "", "/chat/attachments/test123"},
		{"feeds default", "feeds", "attachments", "", "/feeds/attachments/test123"},
		{"forums default", "forums", "attachments", "", "/forums/attachments/test123"},
		{"custom files", "myapp", "files", "", "/myapp/files/test123"},
		{"custom media", "gallery", "media", "", "/gallery/media/test123"},
		{"with entity", "feeds", "attachments", "alice@example.com", "/feeds/alice@example.com/-/attachments/test123"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := att.attachment_url(tt.app_path, tt.action_path, tt.entity)
			if result != tt.expected {
				t.Errorf("attachment_url(%q, %q, %q) = %q, want %q", tt.app_path, tt.action_path, tt.entity, result, tt.expected)
			}
		})
	}
}

// Test attachment_path sanitization
func TestAttachmentPath(t *testing.T) {
	tests := []struct {
		name     string
		id       string
		filename string
		expected string
	}{
		{"normal file", "abc123", "document.pdf", "users/42/wiki/files/abc123_document.pdf"},
		{"with spaces", "abc123", "my document.pdf", "users/42/wiki/files/abc123_my document.pdf"},
		{"path traversal attempt", "abc123", "../../../etc/passwd", "users/42/wiki/files/abc123_passwd"},
		{"absolute path attempt", "abc123", "/etc/passwd", "users/42/wiki/files/abc123_passwd"},
		{"empty name", "abc123", "", "users/42/wiki/files/abc123_file"},
		{"dot only", "abc123", ".", "users/42/wiki/files/abc123_file"},
		{"dot dot", "abc123", "..", "users/42/wiki/files/abc123_file"},
		{"nested path", "abc123", "subdir/file.txt", "users/42/wiki/files/abc123_file.txt"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := attachment_path("42", "wiki", tt.id, tt.filename)
			if result != tt.expected {
				t.Errorf("attachment_path(\"42\", \"wiki\", %q, %q) = %q, want %q", tt.id, tt.filename, result, tt.expected)
			}
		})
	}
}

// Test attachment_files_base helper function
func TestAttachmentFilesBase(t *testing.T) {
	orig_data_dir := data_dir
	data_dir = "/var/lib/mochi"
	defer func() { data_dir = orig_data_dir }()

	tests := []struct {
		name     string
		user_id  string
		app_id   string
		expected string
	}{
		{"basic", "42", "chat", "/var/lib/mochi/users/42/chat/files"},
		{"user 1", "1", "forums", "/var/lib/mochi/users/1/forums/files"},
		{"large user id", "999999", "feeds", "/var/lib/mochi/users/999999/feeds/files"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := attachment_files_base(tt.user_id, tt.app_id)
			if result != tt.expected {
				t.Errorf("attachment_files_base(%q, %q) = %q, want %q", tt.user_id, tt.app_id, result, tt.expected)
			}
		})
	}
}

// Test attachment_filename helper function
func TestAttachmentFilename(t *testing.T) {
	tests := []struct {
		name     string
		id       string
		filename string
		expected string
	}{
		{"normal file", "abc123", "document.pdf", "abc123_document.pdf"},
		{"with spaces", "xyz789", "my file.txt", "xyz789_my file.txt"},
		{"path traversal blocked", "id1", "../../../etc/passwd", "id1_passwd"},
		{"absolute path blocked", "id2", "/etc/shadow", "id2_shadow"},
		{"empty name", "id3", "", "id3_file"},
		{"dot only", "id4", ".", "id4_file"},
		{"dot dot", "id5", "..", "id5_file"},
		{"nested path", "id6", "subdir/nested/file.txt", "id6_file.txt"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := attachment_filename(tt.id, tt.filename)
			if result != tt.expected {
				t.Errorf("attachment_filename(%q, %q) = %q, want %q", tt.id, tt.filename, result, tt.expected)
			}
		})
	}
}

// Benchmark attachment_content_type
func BenchmarkAttachmentContentType(b *testing.B) {
	filenames := []string{
		"document.pdf",
		"image.png",
		"README",
		"script.js",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		attachment_content_type(filenames[i%len(filenames)])
	}
}

// Benchmark Attachment.to_map
func BenchmarkAttachmentToMap(b *testing.B) {
	att := &Attachment{
		ID:          "test123",
		Object:      "post/abc",
		Entity:      "entity456",
		Name:        "document.pdf",
		Size:        1024,
		ContentType: "application/pdf",
		Creator:     "user789",
		Caption:     "Test caption",
		Description: "Test description",
		Rank:        1,
		Created:     1700000000,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		att.to_map()
	}
}

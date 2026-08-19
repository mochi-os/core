// Mochi server: Attachments
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.
//
// The transition bridge for attachments, which apps own now
// (lib/starlark/attachments.star). This file no longer implements storage,
// federation or synchronisation between users - it is what a migration reads
// its old rows out through, plus the degradations an app published before the
// move still calls. See the comment on api_attachment below for why each
// surviving entry point survives.

package main

import (
	"fmt"
	"os"
	"sync"
	"time"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
	"io"
	"mime"
	"path/filepath"
)

const (
	// The largest attachment the platform stores. Equal to object_maximum, and
	// held there by test rather than by hope: an attachment above the transfer
	// cap is stored whole by its owner and received truncated by every
	// subscriber, silently.
	attachment_max_size_default = object_maximum
	cache_max_age               = 7 * 24 * time.Hour // 7 days
)

type Attachment struct {
	ID          string `db:"id"`
	Object      string `db:"object"`
	Entity      string `db:"entity"`
	Name        string `db:"name"`
	Size        int64  `db:"size"`
	ContentType string `db:"content_type"`
	Creator     string `db:"creator"`
	Caption     string `db:"caption"`
	Description string `db:"description"`
	Rank        int    `db:"rank"`
	Created     int64  `db:"created"`
}

// What survives of the attachment bridge, and why each one does. Attachments
// belong to the app that owns them now (lib/starlark/attachments.star); this is
// the residue that cannot be deleted yet.
//
// export and path are what MIGRATIONS need. A database still at an old schema
// runs its upgrade step whenever it is next opened - next week, or in two years
// after a restore - and reads its rows out through here. Deleting these does not
// delay such a database, it strands it permanently, on a host nobody can inspect.
//
// create and clear exist so the migration above can be tested. A permanent code
// path with no way to build a fixture is worse than one small function.
//
// data and fetch are deliberate degradations for an app published before the
// move and not yet updated: fetch answers an empty list and data answers None
// for a remote row, rather than aborting the handler. The write and management
// surface was deleted instead - a stale app failing to save an attachment fails
// visibly, and is fixed by updating the app.
var api_attachment = sls.FromStringDict(sl.String("mochi.attachment"), sl.StringDict{
	"create": &attachment_create_module{},
	"clear":  sl.NewBuiltin("mochi.attachment.clear", api_attachment_clear),
	"data":   sl.NewBuiltin("mochi.attachment.data", api_attachment_data),
	"path":   sl.NewBuiltin("mochi.attachment.path", api_attachment_path),
	"fetch":  sl.NewBuiltin("mochi.attachment.fetch", api_attachment_fetch),
	"export": sl.NewBuiltin("mochi.attachment.export", api_attachment_export),
})

var attachment_bridge_warned sync.Map

// attachment_bridge_notice logs when a legacy mochi.attachment.* call runs, so
// apps still on the old API surface show in the logs instead of lingering
// silently until the bridge is removed. Once per app and function per process:
// a stale chat app would otherwise write a line per message.
//
// A migration is not a stale app. The attachments library's own
// attachment_migrate reads its rows out through mochi.attachment.export from
// database_upgrade - the bridge's whole reason for surviving - so warning
// there tells an operator to update an app that is already current, once for
// every database still to migrate. That is also what makes the absence of
// these lines worth something: while migrations wrote them too, a quiet log
// could not distinguish "no stale apps" from "no migrations ran", and the
// quiet is the only evidence that would ever justify deleting the bridge.
func attachment_bridge_notice(t *sl.Thread, function string) {
	if db_lifecycle_conn(t) != nil {
		return
	}
	identifier := ""
	if app, ok := t.Local("app").(*App); ok && app != nil {
		identifier = app.id
	}
	if _, seen := attachment_bridge_warned.LoadOrStore(identifier+" "+function, true); !seen {
		warn("Legacy attachment API %s called by app %q; update the app to the attachments library", function, identifier)
	}
}

// mochi.attachment.export() -> list: Return every attachment row for the
// calling user and app, each carrying object, entity (provenance: ” for the
// app's own rows, an entity id for a remote copy), name, size, content_type,
// creator, caption, description, rank, created. The transition bridge for the
// attachments-library migration: an app's database_upgrade reads its rows out
// of the core-managed app.db (which mochi.db cannot reach) and into its own
// table. No URLs - the migration writes storage rows, not display data.
func api_attachment_export(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	attachment_bridge_notice(t, "mochi.attachment.export")
	app := t.Local("app").(*App)
	if app == nil {
		return sl_error(fn, "no app")
	}
	owner := attachment_user(t)
	if owner == nil {
		return sl_error(fn, "no owner")
	}
	db := db_app_system(owner, app)
	if db == nil {
		return sl_error(fn, "no database")
	}
	db.attachments_setup()

	var attachments []Attachment
	if err := db.scans(&attachments, "select * from attachments order by object, rank"); err != nil {
		return sl.None, fmt.Errorf("database error: %v", err)
	}

	results := []map[string]any{}
	for _, att := range attachments {
		results = append(results, map[string]any{
			"id": att.ID, "object": att.Object, "entity": att.Entity,
			"name": att.Name, "size": att.Size, "content_type": att.ContentType,
			"creator": att.Creator, "caption": att.Caption,
			"description": att.Description, "rank": att.Rank, "created": att.Created,
		})
	}
	return sl_encode(results), nil
}

// attachment_create_module is a callable module that also has a .stream method.
// Usage: mochi.attachment.create(object, name, data, ...) or mochi.attachment.create.stream(object, name, stream, ...)
type attachment_create_module struct{}

func (m *attachment_create_module) String() string { return "mochi.attachment.create" }
func (m *attachment_create_module) Type() string   { return "module" }
func (m *attachment_create_module) Freeze()        {}
func (m *attachment_create_module) Truth() sl.Bool { return sl.True }
func (m *attachment_create_module) Hash() (uint32, error) {
	return 0, fmt.Errorf("unhashable type: module")
}

func (m *attachment_create_module) AttrNames() []string { return []string{"stream"} }

func (m *attachment_create_module) Attr(name string) (sl.Value, error) {
	if name == "stream" {
		return sl.NewBuiltin("mochi.attachment.create.stream", api_attachment_create_stream), nil
	}
	return nil, nil
}

func (m *attachment_create_module) Name() string { return "mochi.attachment.create" }

func (m *attachment_create_module) CallInternal(thread *sl.Thread, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	return api_attachment_create(thread, nil, args, kwargs)
}

// attachment_user resolves whose store an attachment builtin operates on. It
// deliberately matches mochi.db's resolution (principal_storage): the
// requesting user when there is one, otherwise the entity owner. Before this,
// the attachment builtins read t.Local("owner") directly, so a user acting on
// an entity someone else owns had mochi.db.* and mochi.attachment.* pointing at
// two DIFFERENT users' databases inside one handler - a subscriber's resync
// reconciliation then read the owner's attachment rows while deleting its own
// objects, and serve_attachment validated an owner-store row against
// requesting-user objects. In event context nothing changes: events set only
// "owner", which principal_storage falls back to.
//
// Metadata rows and their files must always resolve to the SAME user, so every
// builtin here uses this for the database, the files base, and the URL path.
// Byte serving (a.write.attachment) stays entity/owner-based on purpose, so a
// subscriber's remote-attachment URLs still fetch from the owning host.
func attachment_user(t *sl.Thread) *User {
	user, err := principal_storage(t)
	if err != nil {
		return nil
	}
	return user
}

// reg_attachments is the upsert definition for attachment metadata rows in the
// app-system app.db. `entity` (the owned-vs-foreign pointer) and `rank`
// (maintained by reorder arithmetic) are kept out of the payload so whole-row
// writes leave them untouched.
var reg_attachments = upsert_def{"attachments", []string{"id"}, []string{"object", "name", "size", "content_type", "creator", "caption", "description", "created"}}

// Create attachments table in the system database (app.db)
func (db *DB) attachments_setup() {
	db.exec("create table if not exists attachments ( id text not null primary key, object text not null, entity text not null default '', name text not null, size integer not null, content_type text not null default '', creator text not null default '', caption text not null default '', description text not null default '', rank integer not null default 0, created integer not null )")
	db.exec("create index if not exists attachments_object on attachments( object )")

	// Add rank column if missing (for databases created before rank was added)
	has_rank, _ := db.exists("select 1 from pragma_table_info('attachments') where name='rank'")
	if !has_rank {
		db.exec("alter table attachments add column rank integer not null default 0")
	}
}

// Get the file path for an attachment (relative to data_dir)
func attachment_path(user_uid string, app_id string, id string, name string) string {
	safe_name := filepath.Base(name)
	if safe_name == "" || safe_name == "." || safe_name == ".." {
		safe_name = "file"
	}
	return fmt.Sprintf("users/%s/%s/files/%s_%s", user_uid, app_id, id, safe_name)
}

// Get the base directory for attachment files (for use with os.Root)
func attachment_files_base(user_uid string, app_id string) string {
	return fmt.Sprintf("%s/users/%s/%s/files", data_dir, user_uid, app_id)
}

// Get just the filename for an attachment (for use with os.Root)
func attachment_filename(id string, name string) string {
	safe_name := filepath.Base(name)
	if safe_name == "" || safe_name == "." || safe_name == ".." {
		safe_name = "file"
	}
	return fmt.Sprintf("%s_%s", id, safe_name)
}

// Remove an attachment's file and any generated image variants (thumbnail,
// preview) from the app's files root.
func attachment_files_remove(root *os.Root, id string, name string) {
	filename := attachment_filename(id, name)
	root.Remove(filename)
	ext := filepath.Ext(filename)
	stem := filename[:len(filename)-len(ext)]
	root.Remove("thumbnails/" + stem + "_thumbnail" + ext)
	root.Remove("previews/" + stem + "_preview" + ext)
}

// Get the next rank for an object
func (db *DB) attachment_next_rank(object string) int {
	var max_rank int
	row, _ := db.row("select max(rank) as max_rank from attachments where object=?", object)
	if row != nil && row["max_rank"] != nil {
		switch v := row["max_rank"].(type) {
		case int64:
			max_rank = int(v)
		case int:
			max_rank = v
		}
	}
	return max_rank + 1
}

// attachment_record_write inserts an attachment row in the per-app system DB.
func attachment_record_write(db *DB, att *Attachment) {
	db.exec("insert into attachments (id, object, entity, name, size, content_type, creator, caption, description, rank, created) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		att.ID, att.Object, att.Entity, att.Name, att.Size, att.ContentType, att.Creator, att.Caption, att.Description, att.Rank, att.Created)
}

// Create attachment record for file already at final path.
// Shared logic used by create_from_stream.
func attachment_create_record(db *DB, app *App, owner *User, object, name, id string, size int64, content_type, creator, caption, description string) map[string]any {
	rank := db.attachment_next_rank(object)

	att := Attachment{
		ID:          id,
		Object:      object,
		Entity:      "",
		Name:        name,
		Size:        size,
		ContentType: content_type,
		Creator:     creator,
		Caption:     caption,
		Description: description,
		Rank:        rank,
		Created:     now(),
	}

	attachment_record_write(db, &att)

	result := att.to_map(app.url_path(owner))

	return result
}

// Convert Attachment struct to map for Starlark
// paths: [app_path, action_path (default "attachments"), entity (optional for public URLs)]
// to_map renders a row as the response dict the create builtins return. The
// path is the calling app's URL prefix; empty omits the URLs entirely. It took
// a variadic of up to three path components - prefix, action, entity - and
// every caller passed one, the other two exercised only by tests written for
// the capability rather than for any use of it.
func (a *Attachment) to_map(app_path string) map[string]any {
	m := map[string]any{
		"id":           a.ID,
		"object":       a.Object,
		"entity":       a.Entity,
		"name":         a.Name,
		"size":         a.Size,
		"content_type": a.ContentType,
		"type":         a.ContentType,
		"creator":      a.Creator,
		"caption":      a.Caption,
		"description":  a.Description,
		"rank":         a.Rank,
		"created":      a.Created,
		"image":        is_image(a.Name),
	}
	if app_path != "" {
		url := a.attachment_url(app_path, "attachments", "")
		m["url"] = url
		if is_image(a.Name) {
			m["thumbnail_url"] = url + "/thumbnail"
			m["preview_url"] = url + "/preview"
		}
	}
	return m
}

// Generate URL for attachment
// If entity is provided, generates public URL format: /app/entity/-/action/id
func (a *Attachment) attachment_url(app_path, action_path, entity string) string {
	if entity != "" {
		return fmt.Sprintf("/%s/%s/-/%s/%s", app_path, entity, action_path, a.ID)
	}
	return fmt.Sprintf("/%s/%s/%s", app_path, action_path, a.ID)
}

// Detect content type from filename
func attachment_content_type(name string) string {
	ext := filepath.Ext(name)
	if ext == "" {
		return "application/octet-stream"
	}
	ct := mime.TypeByExtension(ext)
	if ct == "" {
		return "application/octet-stream"
	}
	return ct
}

// mochi.attachment.create(object, name, data, content_type?, caption?, description?) -> dict: Create an attachment from data
func api_attachment_create(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	attachment_bridge_notice(t, "mochi.attachment.create")
	if len(args) < 3 || len(args) > 7 {
		return sl_error(fn, "syntax: <object: string>, <name: string>, <data: bytes>, [content_type: string], [caption: string], [description: string]")
	}

	object, ok := sl.AsString(args[0])
	if !ok || !valid(object, "path") {
		return sl_error(fn, "invalid object")
	}

	name, ok := sl.AsString(args[1])
	if !ok || name == "" {
		return sl_error(fn, "invalid name")
	}

	data := sl_decode(args[2])
	var bytes []byte
	switch v := data.(type) {
	case []byte:
		bytes = v
	case string:
		bytes = []byte(v)
	default:
		return sl_error(fn, "data must be bytes or string")
	}

	content_type := ""
	if len(args) > 3 {
		content_type, _ = sl.AsString(args[3])
	}
	if content_type == "" {
		content_type = attachment_content_type(name)
	}

	caption := ""
	if len(args) > 4 {
		caption, _ = sl.AsString(args[4])
	}

	description := ""
	if len(args) > 5 {
		description, _ = sl.AsString(args[5])
	}

	app := t.Local("app").(*App)
	if app == nil {
		return sl_error(fn, "no app")
	}

	owner := attachment_user(t)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	user := principal_caller(t)
	creator := ""
	if user != nil && user.Identity != nil {
		creator = user.Identity.ID
	}

	db := db_app_system(owner, app)
	if db == nil {
		return sl_error(fn, "no database")
	}
	db.attachments_setup()

	// Check size
	if int64(len(bytes)) > attachment_max_size_default {
		return sl_error(fn, "file too large: %d bytes", len(bytes))
	}

	// Check storage limit (10GB per user across all apps; admins exempt)
	remaining, err := user_storage_remaining(owner)
	if err != nil {
		return sl_error(fn, "unable to measure storage: %v", err)
	}
	if int64(len(bytes)) > remaining {
		return sl_error(fn, "storage limit exceeded")
	}

	// Create attachment record
	id := uid()
	rank := db.attachment_next_rank(object)

	att := Attachment{
		ID:          id,
		Object:      object,
		Entity:      "",
		Name:        name,
		Size:        int64(len(bytes)),
		ContentType: content_type,
		Creator:     creator,
		Caption:     caption,
		Description: description,
		Rank:        rank,
		Created:     now(),
	}

	// Save file using os.Root for traversal protection
	base := attachment_files_base(owner.UID, app.id)
	if err := os.MkdirAll(base, 0755); err != nil {
		return sl_error(fn, "unable to create files directory: %v", err)
	}
	root, err := os.OpenRoot(base)
	if err != nil {
		return sl_error(fn, "unable to access files directory")
	}
	defer root.Close()

	filename := attachment_filename(id, name)
	f, err := root.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return sl_error(fn, "unable to write file")
	}
	_, err = f.Write(bytes)
	f.Close()
	if err != nil {
		return sl_error(fn, "unable to write file")
	}

	// Insert record
	attachment_record_write(db, &att)

	result := att.to_map(app.url_path(owner))

	return sl_encode(result), nil
}

// mochi.attachment.create.stream(object, name, stream, content_type?, caption?, description?, id?) -> dict: Create an attachment by streaming directly to storage
// This avoids the need for a temp file by streaming directly to the final attachment location.
func api_attachment_create_stream(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	attachment_bridge_notice(t, "mochi.attachment.create.stream")
	if len(args) < 3 || len(args) > 7 {
		return sl_error(fn, "syntax: <object: string>, <name: string>, <stream: Stream>, [content_type: string], [caption: string], [description: string], [id: string]")
	}

	object, ok := sl.AsString(args[0])
	if !ok || !valid(object, "path") {
		return sl_error(fn, "invalid object")
	}

	name, ok := sl.AsString(args[1])
	if !ok || name == "" {
		return sl_error(fn, "invalid name")
	}

	stream, ok := args[2].(*Stream)
	if !ok || stream == nil {
		return sl_error(fn, "invalid stream")
	}

	content_type := ""
	if len(args) > 3 {
		content_type, _ = sl.AsString(args[3])
	}
	if content_type == "" {
		content_type = attachment_content_type(name)
	}

	caption := ""
	if len(args) > 4 {
		caption, _ = sl.AsString(args[4])
	}

	description := ""
	if len(args) > 5 {
		description, _ = sl.AsString(args[5])
	}

	// Optional attachment ID (use existing ID for federation sync). Checked
	// against the shape uid() produces - 32 lowercase hex - because it becomes
	// both the filename stem and the row's primary key. os.Root refuses a
	// traversal downstream, but "" and ".." are accepted as filenames and
	// stored as the key regardless, and a value refused at the boundary is one
	// no later reader has to reason about.
	provided_id := ""
	if len(args) > 6 {
		provided_id, _ = sl.AsString(args[6])
		if provided_id != "" && !valid(provided_id, "id") {
			return sl_error(fn, "invalid id")
		}
	}

	app := t.Local("app").(*App)
	if app == nil {
		return sl_error(fn, "no app")
	}

	owner := attachment_user(t)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	user := principal_caller(t)
	creator := ""
	if user != nil && user.Identity != nil {
		creator = user.Identity.ID
	}

	db := db_app_system(owner, app)
	if db == nil {
		return sl_error(fn, "no database")
	}
	db.attachments_setup()

	// Check storage limit and calculate remaining space (admins exempt)
	remaining, err := user_storage_remaining(owner)
	if err != nil {
		stream.close_read()
		return sl_error(fn, "unable to measure storage: %v", err)
	}
	if remaining <= 0 {
		stream.close_read()
		return sl_error(fn, "storage limit exceeded")
	}

	// Generate attachment ID
	id := provided_id
	if id == "" {
		id = uid()
	}

	// Use os.Root for traversal protection
	base := attachment_files_base(owner.UID, app.id)
	if err := os.MkdirAll(base, 0755); err != nil {
		return sl_error(fn, "unable to create files directory: %v", err)
	}
	root, err := os.OpenRoot(base)
	if err != nil {
		stream.close_read()
		return sl_error(fn, "unable to access files directory")
	}
	defer root.Close()

	// Before the truncating open below. A repeated id used to reach
	// OpenFile(O_TRUNC), which emptied the existing attachment's file, and only
	// then fail on the primary key - and db.exec panics on a constraint
	// violation, so the caller got a failed handler and the original bytes were
	// already gone.
	if existing, _ := db.exists("select 1 from attachments where id=?", id); existing {
		stream.close_read()
		return sl_error(fn, "attachment %q already exists", id)
	}

	filename := attachment_filename(id, name)

	// Use raw_reader() to include any bytes buffered by the CBOR decoder
	reader := stream.raw_reader()

	// Bounded by whichever of the two caps binds: the platform's object
	// maximum, or what the user has left. Which one it was decides the error
	// below, because they mean different things to whoever sees it - free some
	// space, versus this file can never be stored here.
	//
	// The bound is max_size+1, the idiom api_cache_write documents: a source
	// that overruns must be distinguishable from one that fits exactly, so one
	// byte beyond the cap survives for the check after the copy to find.
	// Cutting at max_size stored a prefix of an oversized attachment, wrote a
	// row claiming that truncated length, and told the app it had succeeded -
	// which is the silent truncation attachment_max_size_default is described
	// as being held against.
	max_size := int64(attachment_max_size_default)
	quota := false
	if remaining < max_size {
		max_size = remaining
		quota = true
	}
	limited := io.LimitReader(reader, max_size+1)

	// Write to file within root
	f, err := root.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		stream.close_read()
		return sl_error(fn, "failed to write attachment")
	}

	size, err := io.Copy(f, limited)
	f.Close()
	stream.close_read()

	if err != nil {
		root.Remove(filename)
		return sl_error(fn, "failed to write attachment")
	}

	if size == 0 {
		root.Remove(filename)
		return sl_error(fn, "empty attachment")
	}

	// The extra byte the limit allowed for. Same two messages the non-stream
	// create answers with, so both paths say the same thing about the same
	// condition.
	if size > max_size {
		root.Remove(filename)
		if quota {
			return sl_error(fn, "storage limit exceeded")
		}
		return sl_error(fn, "file too large: exceeds %d bytes", max_size)
	}

	// Create record using shared helper
	result := attachment_create_record(db, app, owner, object, name, id, size, content_type, creator, caption, description)
	return sl_encode(result), nil
}

// mochi.attachment.clear(object) -> None: Delete all attachments for an object
func api_attachment_clear(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	attachment_bridge_notice(t, "mochi.attachment.clear")
	if len(args) < 1 || len(args) > 2 {
		return sl_error(fn, "syntax: <object: string>")
	}

	object, ok := sl.AsString(args[0])
	if !ok || !valid(object, "path") {
		return sl_error(fn, "invalid object")
	}

	app := t.Local("app").(*App)
	if app == nil {
		return sl_error(fn, "no app")
	}

	owner := attachment_user(t)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	db := db_app_system(owner, app)
	if db == nil {
		return sl_error(fn, "no database")
	}
	db.attachments_setup()

	// Get all attachments for object
	var attachments []Attachment
	err := db.scans(&attachments, "select * from attachments where object = ?", object)
	if err != nil {
		warn("Database error loading attachments for deletion: %v", err)
	}

	// Delete files and image variants using os.Root for traversal protection
	base := attachment_files_base(owner.UID, app.id)
	root, err := os.OpenRoot(base)
	if err == nil {
		for _, att := range attachments {
			attachment_files_remove(root, att.ID, att.Name)
		}
		root.Close()
	}

	// Delete the records.
	for _, att := range attachments {
		db.row_remove(reg_attachments, map[string]any{"id": att.ID})
	}

	return sl.None, nil
}

// mochi.attachment.data(id) -> bytes or None: Get attachment file data
func api_attachment_data(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	attachment_bridge_notice(t, "mochi.attachment.data")
	if len(args) != 1 {
		return sl_error(fn, "syntax: <id: string>")
	}

	id, ok := sl.AsString(args[0])
	if !ok || id == "" {
		return sl_error(fn, "invalid id")
	}

	app := t.Local("app").(*App)
	if app == nil {
		return sl_error(fn, "no app")
	}

	owner := attachment_user(t)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	db := db_app_system(owner, app)
	if db == nil {
		return sl_error(fn, "no database")
	}
	db.attachments_setup()

	var att Attachment
	if !db.scan(&att, "select * from attachments where id = ?", id) {
		return sl.None, nil
	}

	// A row carrying an entity is a reference to bytes another host holds.
	// Core no longer pulls them: the peer-facing _attachment/* transfer is
	// gone, and an app that wants a remote copy asks for it through its own
	// declared event, where it can authorise the exchange. Nothing local to
	// return, so this reads as absent.
	if att.Entity != "" {
		return sl.None, nil
	}

	// Local file - read using os.Root for traversal protection
	base := attachment_files_base(owner.UID, app.id)
	root, err := os.OpenRoot(base)
	if err != nil {
		return sl_error(fn, "file not found")
	}
	defer root.Close()

	filename := attachment_filename(att.ID, att.Name)
	f, err := root.Open(filename)
	if err != nil {
		return sl_error(fn, "file not found")
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		return sl_error(fn, "unable to read file")
	}
	return sl_encode(data), nil
}

// mochi.attachment.path(id) -> string or None: Get relative file path for use with stream file operations
// Returns the filename relative to the app's files directory, suitable for write_from_file/read_to_file
func api_attachment_path(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	attachment_bridge_notice(t, "mochi.attachment.path")
	if len(args) != 1 {
		return sl_error(fn, "syntax: <id: string>")
	}

	id, ok := sl.AsString(args[0])
	if !ok || id == "" {
		return sl_error(fn, "invalid id")
	}

	app := t.Local("app").(*App)
	if app == nil {
		return sl_error(fn, "no app")
	}

	owner := attachment_user(t)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	db := db_app_system(owner, app)
	if db == nil {
		return sl_error(fn, "no database")
	}
	db.attachments_setup()

	var att Attachment
	if !db.scan(&att, "select * from attachments where id = ?", id) {
		return sl.None, nil
	}

	// Remote attachments not available locally - served by web layer via feature:"attachment"
	if att.Entity != "" {
		return sl.None, nil
	}

	// Return just the filename (id_name) relative to the app's files directory
	// This works with write_from_file/read_to_file which prepend the full path
	safe_name := filepath.Base(att.Name)
	if safe_name == "" || safe_name == "." || safe_name == ".." {
		safe_name = "file"
	}
	return sl_encode(fmt.Sprintf("%s_%s", att.ID, safe_name)), nil
}

// mochi.attachment.fetch(object, entity) -> list: Retained for app versions
// published before attachments moved into the apps; always returns an empty
// list. Its counterpart, the built-in _attachment/fetch responder, is gone, so
// no peer answers this any more. It stays a no-op rather than an error because
// an installed app runs the version it was published with: an old app that
// calls this shows no attachments, where a removed builtin would abort the
// whole action. The response-parsing it used to do - which trusted the object
// each row claimed and could be made to repoint an id we already held - goes
// with it. Apps fetch remote attachments through their own declared events now.
func api_attachment_fetch(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	attachment_bridge_notice(t, "mochi.attachment.fetch")
	if len(args) != 2 {
		return sl_error(fn, "syntax: <object: string>, <entity: string>")
	}
	return sl_encode([]map[string]any{}), nil
}

// Mochi server: Attachments
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.
//
// Provides app-level file attachments with federation support.
// Attachments are associated with objects and can be synced between users.

package main

import (
	"fmt"
	"os"
	"time"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
	"io"
	"mime"
	"path/filepath"
)

const (
	attachment_max_size_default = 1073741824         // 1GB
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

var api_attachment = sls.FromStringDict(sl.String("mochi.attachment"), sl.StringDict{
	"save":      sl.NewBuiltin("mochi.attachment.save", api_attachment_save),
	"create":    &attachment_create_module{},
	"insert":    sl.NewBuiltin("mochi.attachment.insert", api_attachment_insert),
	"update":    sl.NewBuiltin("mochi.attachment.update", api_attachment_update),
	"move":      sl.NewBuiltin("mochi.attachment.move", api_attachment_move),
	"delete":    sl.NewBuiltin("mochi.attachment.delete", api_attachment_delete),
	"clear":     sl.NewBuiltin("mochi.attachment.clear", api_attachment_clear),
	"list":      sl.NewBuiltin("mochi.attachment.list", api_attachment_list),
	"get":       sl.NewBuiltin("mochi.attachment.get", api_attachment_get),
	"exists":    sl.NewBuiltin("mochi.attachment.exists", api_attachment_exists),
	"data":      sl.NewBuiltin("mochi.attachment.data", api_attachment_data),
	"path":      sl.NewBuiltin("mochi.attachment.path", api_attachment_path),
	"thumbnail": sl.NewBuiltin("mochi.attachment.thumbnail", api_attachment_thumbnail),
	"preview":   sl.NewBuiltin("mochi.attachment.preview", api_attachment_preview),
	"store":     sl.NewBuiltin("mochi.attachment.store", api_attachment_store),
	"fetch":     sl.NewBuiltin("mochi.attachment.fetch", api_attachment_fetch),
})

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
// deliberately matches mochi.db's resolution (db_user_for_thread): the
// requesting user when there is one, otherwise the entity owner. Before this,
// the attachment builtins read t.Local("owner") directly, so a user acting on
// an entity someone else owns had mochi.db.* and mochi.attachment.* pointing at
// two DIFFERENT users' databases inside one handler - a subscriber's resync
// reconciliation then read the owner's attachment rows while deleting its own
// objects, and serve_attachment validated an owner-store row against
// requesting-user objects. In event context nothing changes: events set only
// "owner", which db_user_for_thread falls back to.
//
// Metadata rows and their files must always resolve to the SAME user, so every
// builtin here uses this for the database, the files base, and the URL path.
// Byte serving (a.write.attachment) stays entity/owner-based on purpose, so a
// subscriber's remote-attachment URLs still fetch from the owning host.
func attachment_user(t *sl.Thread) *User {
	user, err := db_user_for_thread(t)
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

// Shift ranks up from a position.
func (db *DB) attachment_shift_up(object string, from_rank int) {
	db.exec("update attachments set rank = rank + 1 where object = ? and rank >= ?", object, from_rank)
}

// Shift ranks down from a position. Replicated (see attachment_shift_up).
func (db *DB) attachment_shift_down(object string, from_rank int) {
	db.exec("update attachments set rank = rank - 1 where object = ? and rank > ?", object, from_rank)
}

// attachment_record_write inserts an attachment row in the per-app system DB.
func attachment_record_write(db *DB, att *Attachment) {
	db.exec("insert into attachments (id, object, entity, name, size, content_type, creator, caption, description, rank, created) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		att.ID, att.Object, att.Entity, att.Name, att.Size, att.ContentType, att.Creator, att.Caption, att.Description, att.Rank, att.Created)
}

// attachment_meta_set applies a caption/description edit as a whole-row
// upsert. entity (owned-vs-foreign pointer) and rank (reorder-managed) are
// not in the payload, so the write leaves them untouched. No-op if the
// attachment is absent.
func (db *DB) attachment_meta_set(id, caption, description string) {
	var att Attachment
	if !db.scan(&att, "select id, object, name, size, content_type, creator, caption, description, created from attachments where id = ?", id) {
		return
	}
	db.row_write(reg_attachments, map[string]any{
		"id": id, "object": att.Object, "name": att.Name, "size": att.Size,
		"content_type": att.ContentType, "creator": att.Creator,
		"caption": caption, "description": description, "created": att.Created,
	})
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
func (a *Attachment) to_map(paths ...string) map[string]any {
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
	if len(paths) > 0 && paths[0] != "" {
		app_path := paths[0]
		action_path := "attachments"
		entity := ""
		if len(paths) > 1 && paths[1] != "" {
			action_path = paths[1]
		}
		if len(paths) > 2 && paths[2] != "" {
			entity = paths[2]
		}
		m["url"] = a.attachment_url(app_path, action_path, entity)
		if is_image(a.Name) {
			m["thumbnail_url"] = a.attachment_url(app_path, action_path, entity) + "/thumbnail"
			m["preview_url"] = a.attachment_url(app_path, action_path, entity) + "/preview"
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

// mochi.attachment.save(object, field, captions?, descriptions?) -> list: Save uploaded files as attachments
func api_attachment_save(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 2 || len(args) > 5 {
		return sl_error(fn, "syntax: <object: string>, <field: string>, [captions: array], [descriptions: array]")
	}

	object, ok := sl.AsString(args[0])
	if !ok || !valid(object, "path") {
		return sl_error(fn, "invalid object")
	}

	field, ok := sl.AsString(args[1])
	if !ok || !valid(field, "constant") {
		return sl_error(fn, "invalid field")
	}

	var captions []string
	if len(args) > 2 {
		captions = sl_decode_string_list(args[2])
	}

	var descriptions []string
	if len(args) > 3 {
		descriptions = sl_decode_string_list(args[3])
	}

	action := t.Local("action").(*Action)
	if action == nil {
		return sl_error(fn, "called from non-action")
	}

	app := t.Local("app").(*App)
	if app == nil {
		return sl_error(fn, "no app")
	}

	owner := attachment_user(t)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	user := t.Local("user").(*User)
	creator := ""
	if user != nil && user.Identity != nil {
		creator = user.Identity.ID
	}

	db := db_app_system(owner, app)
	if db == nil {
		return sl_error(fn, "no database")
	}
	db.attachments_setup()

	// Get multipart form
	form, err := action.web.MultipartForm()
	if err != nil {
		return sl_error(fn, "unable to parse form: %v", err)
	}

	files := form.File[field]
	if len(files) == 0 {
		return sl_encode([]map[string]any{}), nil
	}

	// Open root once for all files (traversal protection)
	base := attachment_files_base(owner.UID, app.id)
	if err := os.MkdirAll(base, 0755); err != nil {
		return sl_error(fn, "unable to create files directory: %v", err)
	}
	root, err := os.OpenRoot(base)
	if err != nil {
		return sl_error(fn, "unable to access files directory")
	}
	defer root.Close()

	var results []map[string]any
	for i, fh := range files {
		// Check size
		if fh.Size > attachment_max_size_default {
			return sl_error(fn, "file too large: %d bytes", fh.Size)
		}

		// Check storage limit (10GB per user across all apps; admins exempt)
		remaining, err := user_storage_remaining(owner)
		if err != nil {
			return sl_error(fn, "unable to measure storage: %v", err)
		}
		if fh.Size > remaining {
			return sl_error(fn, "storage limit exceeded")
		}

		// Open uploaded file
		src, err := fh.Open()
		if err != nil {
			return sl_error(fn, "unable to open uploaded file: %v", err)
		}
		defer src.Close()

		// Create attachment record
		id := uid()
		rank := db.attachment_next_rank(object)
		content_type := fh.Header.Get("Content-Type")
		if content_type == "" {
			content_type = attachment_content_type(fh.Filename)
		}

		caption := ""
		if i < len(captions) {
			caption = captions[i]
		}

		description := ""
		if i < len(descriptions) {
			description = descriptions[i]
		}

		att := Attachment{
			ID:          id,
			Object:      object,
			Entity:      "",
			Name:        fh.Filename,
			Size:        fh.Size,
			ContentType: content_type,
			Creator:     creator,
			Caption:     caption,
			Description: description,
			Rank:        rank,
			Created:     now(),
		}

		// Save file (stream directly to disk without buffering)
		filename := attachment_filename(id, fh.Filename)
		f, err := root.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			return sl_error(fn, "unable to write file")
		}
		_, err = io.Copy(f, src)
		f.Close()
		if err != nil {
			return sl_error(fn, "unable to write file: %v", err)
		}

		// Insert record
		attachment_record_write(db, &att)

		results = append(results, att.to_map(app.url_path(owner)))
	}

	return sl_encode(results), nil
}

// mochi.attachment.create(object, name, data, content_type?, caption?, description?) -> dict: Create an attachment from data
func api_attachment_create(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
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

	user := t.Local("user").(*User)
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

	// Optional attachment ID (use existing ID for federation sync)
	provided_id := ""
	if len(args) > 6 {
		provided_id, _ = sl.AsString(args[6])
	}

	app := t.Local("app").(*App)
	if app == nil {
		return sl_error(fn, "no app")
	}

	owner := attachment_user(t)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	user := t.Local("user").(*User)
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

	filename := attachment_filename(id, name)

	// Use raw_reader() to include any bytes buffered by the CBOR decoder
	reader := stream.raw_reader()

	// Limit reader to remaining storage space and max attachment size
	max_size := remaining
	if max_size > attachment_max_size_default {
		max_size = attachment_max_size_default
	}
	limited := io.LimitReader(reader, max_size)

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

	// Create record using shared helper
	result := attachment_create_record(db, app, owner, object, name, id, size, content_type, creator, caption, description)
	return sl_encode(result), nil
}

// mochi.attachment.insert(object, name, data, position, content_type?, caption?, description?) -> dict: Insert an attachment at position
func api_attachment_insert(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 4 || len(args) > 8 {
		return sl_error(fn, "syntax: <object: string>, <name: string>, <data: bytes>, <position: int>, [content_type: string], [caption: string], [description: string]")
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

	position, err := sl.AsInt32(args[3])
	if err != nil || position < 1 {
		return sl_error(fn, "invalid position")
	}

	content_type := ""
	if len(args) > 4 {
		content_type, _ = sl.AsString(args[4])
	}
	if content_type == "" {
		content_type = attachment_content_type(name)
	}

	caption := ""
	if len(args) > 5 {
		caption, _ = sl.AsString(args[5])
	}

	description := ""
	if len(args) > 6 {
		description, _ = sl.AsString(args[6])
	}

	app := t.Local("app").(*App)
	if app == nil {
		return sl_error(fn, "no app")
	}

	owner := attachment_user(t)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	user := t.Local("user").(*User)
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

	// Shift existing attachments
	db.attachment_shift_up(object, int(position))

	// Create attachment record
	id := uid()

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
		Rank:        int(position),
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

// mochi.attachment.update(id, caption, description) -> dict or None: Update attachment metadata
func api_attachment_update(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 3 || len(args) > 4 {
		return sl_error(fn, "syntax: <id: string>, <caption: string>, <description: string>")
	}

	id, ok := sl.AsString(args[0])
	if !ok || id == "" {
		return sl_error(fn, "invalid id")
	}

	caption, ok := sl.AsString(args[1])
	if !ok {
		return sl_error(fn, "invalid caption")
	}

	description, ok := sl.AsString(args[2])
	if !ok {
		return sl_error(fn, "invalid description")
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

	// Update record
	db.attachment_meta_set(id, caption, description)

	// Get updated record
	var att Attachment
	if !db.scan(&att, "select * from attachments where id = ?", id) {
		return sl.None, nil
	}

	result := att.to_map(app.url_path(owner))

	return sl_encode(result), nil
}

// mochi.attachment.move(id, position) -> dict: Move an attachment to a new position
func api_attachment_move(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 2 || len(args) > 3 {
		return sl_error(fn, "syntax: <id: string>, <position: int>")
	}

	id, ok := sl.AsString(args[0])
	if !ok || id == "" {
		return sl_error(fn, "invalid id")
	}

	position, err := sl.AsInt32(args[1])
	if err != nil || position < 1 {
		return sl_error(fn, "invalid position")
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

	// Get current attachment
	var att Attachment
	if !db.scan(&att, "select * from attachments where id = ?", id) {
		return sl_error(fn, "attachment not found")
	}

	old_rank := att.Rank
	new_rank := int(position)

	if old_rank != new_rank {
		if new_rank < old_rank {
			// Moving up: shift items in [new_rank, old_rank) up by 1
			db.exec("update attachments set rank = rank + 1 where object = ? and rank >= ? and rank < ?", att.Object, new_rank, old_rank)
		} else {
			// Moving down: shift items in (old_rank, new_rank] down by 1
			db.exec("update attachments set rank = rank - 1 where object = ? and rank > ? and rank <= ?", att.Object, old_rank, new_rank)
		}
		db.exec("update attachments set rank = ? where id = ?", new_rank, id)
	}

	// Get updated record
	db.scan(&att, "select * from attachments where id = ?", id)
	result := att.to_map(app.url_path(owner))

	return sl_encode(result), nil
}

// mochi.attachment.delete(id) -> None: Delete an attachment
func api_attachment_delete(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 || len(args) > 2 {
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

	// Get attachment to delete
	var att Attachment
	if !db.scan(&att, "select * from attachments where id = ?", id) {
		debug("attachment_delete: attachment %s not found in user %q database", id, owner.UID)
		return sl.False, nil
	}

	// Delete file and image variants using os.Root for traversal protection
	base := attachment_files_base(owner.UID, app.id)
	root, err := os.OpenRoot(base)
	if err == nil {
		attachment_files_remove(root, att.ID, att.Name)
		root.Close()
	}

	// Delete the record and shift ranks.
	db.row_remove(reg_attachments, map[string]any{"id": id})
	db.attachment_shift_down(att.Object, att.Rank)

	return sl.True, nil
}

// mochi.attachment.clear(object) -> None: Delete all attachments for an object
func api_attachment_clear(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
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

// mochi.attachment.list(object, entity="") -> list: List attachments for an object
// If entity is provided, URLs will include the entity for public access
func api_attachment_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return sl_error(fn, "syntax: <object: string>, [entity: string]")
	}

	object, ok := sl.AsString(args[0])
	if !ok || !valid(object, "path") {
		return sl_error(fn, "invalid object")
	}

	entity := ""
	if len(args) > 1 && args[1] != sl.None {
		entity, ok = sl.AsString(args[1])
		if !ok || (entity != "" && !valid(entity, "entity")) {
			return sl_error(fn, "invalid entity")
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

	db := db_app_system(owner, app)
	if db == nil {
		return sl_error(fn, "no database")
	}
	db.attachments_setup()

	var attachments []Attachment
	err := db.scans(&attachments, "select * from attachments where object = ? order by rank", object)
	if err != nil {
		return sl.None, fmt.Errorf("database error: %v", err)
	}

	var results []map[string]any
	for _, att := range attachments {
		results = append(results, att.to_map(app.url_path(owner), "attachments", entity))
	}

	return sl_encode(results), nil
}

// mochi.attachment.get(id) -> dict or None: Get an attachment by ID
func api_attachment_get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
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

	return sl_encode(att.to_map(app.url_path(owner))), nil
}

// mochi.attachment.exists(id) -> bool: Check if an attachment exists
func api_attachment_exists(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
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

	exists, _ := db.exists("select 1 from attachments where id = ?", id)
	return sl.Bool(exists), nil
}

// mochi.attachment.data(id) -> bytes or None: Get attachment file data
func api_attachment_data(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
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

	// If entity is set, this is a cached reference - fetch from remote
	if att.Entity != "" {
		from := ""
		if owner.Identity != nil {
			from = owner.Identity.ID
		}
		path := attachment_fetch_remote(app, from, att.Entity, id, "")
		if path != "" {
			data, err := os.ReadFile(path)
			if err != nil {
				return sl_error(fn, "unable to read attachment: %v", err)
			}
			return sl_encode(data), nil
		}
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

// mochi.attachment.thumbnail(id) -> string or None: Get the thumbnail path for an
// attachment, creating the thumbnail on demand if it doesn't exist yet. The
// returned path is relative to the app's files directory. Returns None for
// non-image attachments, remote attachments (served by the web layer instead),
// or on errors.
func api_attachment_thumbnail(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	return api_attachment_variant(t, fn, args, "thumbnail")
}

// mochi.attachment.preview(id) -> string or None: As mochi.attachment.thumbnail,
// for the larger preview variant.
func api_attachment_preview(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	return api_attachment_variant(t, fn, args, "preview")
}

func api_attachment_variant(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, variant string) (sl.Value, error) {
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

	// Only images have variants; decoding a video/PDF/etc would fail
	if !is_image(att.Name) {
		return sl.None, nil
	}

	path := filepath.Join(data_dir, attachment_path(owner.UID, app.id, att.ID, att.Name))
	thumb, err := variant_create(path, variant)
	if err != nil || thumb == "" {
		return sl.None, nil
	}

	// Return relative path from app's files directory
	// The variant is at: data_dir/users/{user}/app/files/thumbnails/id_name_thumbnail.ext
	// (previews/id_name_preview.ext for previews); we return that subdirectory path.
	base := filepath.Join(data_dir, "users", owner.UID, app.id, "files")
	rel, err := filepath.Rel(base, thumb)
	if err != nil {
		return sl.None, nil
	}
	return sl_encode(rel), nil
}

// Federation: fetch attachment data from remote entity, returns cache file
// path. variant is "" for the original bytes, "thumbnail" or "preview" for a
// downscaled image variant (generated on the remote side).
func attachment_fetch_remote(app *App, from string, entity string, id string, variant string) string {
	//debug("attachment_fetch_remote: fetching %s from entity %s via app %s", id, entity, app.id)

	// Cache path for remote attachments (variants cached separately; ".thumb"
	// predates the preview variant and is kept so existing caches stay valid)
	cache_path := fmt.Sprintf("%s/attachments/%s/%s/%s", cache_dir, entity, app.id, id)
	switch variant {
	case "thumbnail":
		cache_path += ".thumb"
	case "preview":
		cache_path += ".preview"
	}
	if fi, err := os.Stat(cache_path); err == nil {
		if time.Since(fi.ModTime()) > cache_max_age {
			os.Remove(cache_path) // expired, will refetch below
		} else {
			//debug("attachment_fetch_remote: returning cached file %s", cache_path)
			return cache_path
		}
	}

	// Fetch from remote — use declared service name (not app.id which may be an entity ID for published apps)
	service := app.id
	av := app.active(nil)
	if av != nil && len(av.Services) > 0 {
		service = av.Services[0]
	}
	s, err := stream(from, entity, service, "_attachment/data", app.id, app_services(app, nil))
	if err != nil {
		warn("attachment_fetch_remote: stream error: %v", err)
		return ""
	}
	defer s.close()

	//debug("attachment_fetch_remote: sending id=%s variant=%q", id, variant)
	// The wire flags are separate booleans rather than a variant field so old
	// receivers keep honouring thumbnail requests; a receiver that predates
	// previews ignores the preview flag and answers with the original bytes,
	// which still displays correctly.
	content := map[string]string{"id": id}
	switch variant {
	case "thumbnail":
		content["thumbnail"] = "true"
	case "preview":
		content["preview"] = "true"
	}
	s.write(content)

	//debug("attachment_fetch_remote: waiting for status response...")
	status, err := s.read_content()
	//debug("attachment_fetch_remote: received status=%v err=%v", status, err)
	if err != nil || status["status"] != "200" {
		debug("attachment_fetch_remote: bad status: %v", status)
		return ""
	}

	// Stream directly to cache file (use raw_reader to include any buffered data from CBOR decoder)
	if err := os.MkdirAll(filepath.Dir(cache_path), 0755); err != nil {
		warn("attachment_fetch_remote: failed to create cache dir: %v", err)
		return ""
	}
	if !file_write_from_reader(cache_path, s.raw_reader()) {
		//debug("attachment_fetch_remote: failed to write cache file")
		return ""
	}

	return cache_path
}

// Decode a Starlark value to a string list
// Accepts strings, or dicts (extracts first string value from each dict)
func sl_decode_string_list(v sl.Value) []string {
	var result []string
	switch x := v.(type) {
	case *sl.List:
		for i := 0; i < x.Len(); i++ {
			if s := sl_extract_string(x.Index(i)); s != "" {
				result = append(result, s)
			}
		}
	case sl.Tuple:
		for _, item := range x {
			if s := sl_extract_string(item); s != "" {
				result = append(result, s)
			}
		}
	}
	return result
}

// Extract a string from a Starlark value (string or first value of a dict)
func sl_extract_string(v sl.Value) string {
	if s, ok := sl.AsString(v); ok {
		return s
	}
	if d, ok := v.(*sl.Dict); ok {
		for _, kv := range d.Items() {
			if s, ok := sl.AsString(kv[1]); ok {
				return s
			}
		}
	}
	return ""
}

// Event handler: _attachment/data (responds with file bytes)
func (e *Event) attachment_event_data() {
	//debug("attachment_event_data: called with content=%v", e.content)

	if e.db == nil {
		// No app DB for this (user, app) on this host. A peer can
		// legitimately request an attachment for a user/app not present
		// here — that's "I don't have it", not a server fault. Answer 404
		// like the not-found case below, at debug, so it doesn't
		// warn-email the admin on every such request.
		debug("attachment_event_data: no database for this context, returning 404")
		e.stream.write(map[string]string{"status": "404"})
		return
	}

	id := e.get("id", "")
	if id == "" {
		warn("attachment_event_data: no id, returning 400")
		e.stream.write(map[string]string{"status": "400"})
		return
	}
	variant := ""
	if e.get("thumbnail", "") == "true" {
		variant = "thumbnail"
	}
	if e.get("preview", "") == "true" {
		variant = "preview"
	}

	//debug("attachment_event_data: looking up attachment id=%s", id)
	var att Attachment
	if !e.db.scan(&att, "select * from attachments where id = ?", id) {
		debug("attachment_event_data: attachment not found in db, returning 404")
		e.stream.write(map[string]string{"status": "404"})
		return
	}

	//debug("attachment_event_data: found attachment entity=%q name=%q", att.Entity, att.Name)

	// Resolve the file path — fetch from the original uploader if needed
	base := attachment_files_base(e.user.UID, e.app.id)
	filename := attachment_filename(att.ID, att.Name)
	path := filepath.Join(base, filename)

	if att.Entity != "" {
		// We don't own this attachment — file may not be local.
		// Try to fetch from the entity that uploaded it (e.g., subscriber
		// uploaded to a project we own; we have metadata but not the file).
		if !file_exists(path) {
			from := ""
			if e.user.Identity != nil {
				from = e.user.Identity.ID
			}
			cached := attachment_fetch_remote(e.app, from, att.Entity, id, "")
			if cached == "" {
				e.stream.write(map[string]string{"status": "404"})
				return
			}
			// Store locally so future requests don't need the uploader online
			if err := os.MkdirAll(base, 0755); err != nil {
				warn("Unable to create attachment base dir: %v", err)
				e.stream.write(map[string]string{"status": "500"})
				return
			}
			if err := file_copy(cached, path); err != nil {
				warn("Unable to cache attachment locally: %v", err)
				e.stream.write(map[string]string{"status": "500"})
				return
			}
			// Cache promotion: this host fetched the bytes and now owns a
			// local copy — entity="" means "bytes are local".
			e.db.exec("update attachments set entity = '' where id = ?", id)
			info("Attachment %s fetched from uploader and stored locally", id)
		}
		// File now exists locally — serve it below
	}

	// Open file using os.Root for traversal protection
	root, err := os.OpenRoot(base)
	if err != nil {
		warn("attachment_event_data: unable to open root, returning 404")
		e.stream.write(map[string]string{"status": "404"})
		return
	}
	defer root.Close()

	// Serve the requested image variant if the file is an image
	if variant != "" && is_image(att.Name) {
		thumb, err := variant_create(path, variant)
		if err == nil && thumb != "" {
			f, err := os.Open(thumb)
			if err == nil {
				defer f.Close()
				e.stream.write(map[string]string{"status": "200"})
				io.Copy(e.stream.writer, f)
				e.stream.close_write()
				return
			}
		}
	}

	f, err := root.Open(filename)
	if err != nil {
		debug("attachment_event_data: file not found, returning 404")
		e.stream.write(map[string]string{"status": "404"})
		return
	}
	defer f.Close()

	//debug("attachment_event_data: sending file %s", filename)
	e.stream.write(map[string]string{"status": "200"})
	io.Copy(e.stream.writer, f)
	e.stream.close_write()
	//debug("attachment_event_data: done")
}

// attachment_conflict reports whether id already exists bound to a different
// object, returning the object currently holding it.
//
// The store path takes ids from the sending peer and writes them with
// `replace`, so an id that already exists would otherwise be overwritten -
// object and entity included. A member of one container can read the
// attachment ids it holds and quote one back in a message to another,
// repointing the row and detaching the attachment from the message it really
// belongs to. Re-storing an id under its OWN object stays allowed: that is an
// ordinary metadata update, and the sync path relies on it.
func attachment_conflict(db *DB, id string, object string) (string, bool) {
	existing, _ := db.row("select object from attachments where id = ?", id)
	if existing == nil {
		return "", false
	}
	held, _ := existing["object"].(string)
	return held, held != object
}

// mochi.attachment.store(attachments, entity, object?) -> int: Store remote attachment metadata without downloading files
func api_attachment_store(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 2 || len(args) > 3 {
		return sl_error(fn, "syntax: <attachments: list>, <entity: string>, [object: string]")
	}

	entity, ok := sl.AsString(args[1])
	if !ok || !valid(entity, "entity") {
		return sl_error(fn, "invalid entity")
	}

	object_override := ""
	if len(args) > 2 {
		object_override, _ = sl.AsString(args[2])
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

	// Decode attachments list
	attachments := sl_decode(args[0])
	list, ok := attachments.([]any)
	if !ok {
		return sl_error(fn, "attachments must be a list")
	}

	count := 0
	for _, item := range list {
		att, ok := item.(map[string]any)
		if !ok {
			continue
		}

		id, _ := att["id"].(string)
		if !valid(id, "id") {
			continue
		}

		object := object_override
		if object == "" {
			object, _ = att["object"].(string)
		}

		// A collision with a different object is skipped, not fatal, so one
		// hostile entry can't discard a legitimate message's attachments.
		if held, conflict := attachment_conflict(db, id, object); conflict {
			warn("Attachment %q already belongs to %q; refusing to repoint it at %q", id, held, object)
			continue
		}

		name, _ := att["name"].(string)
		content_type, _ := att["content_type"].(string)
		creator, _ := att["creator"].(string)
		caption, _ := att["caption"].(string)
		description, _ := att["description"].(string)

		var size int64
		switch v := att["size"].(type) {
		case int:
			size = int64(v)
		case int64:
			size = v
		case float64:
			size = int64(v)
		}

		var rank int
		switch v := att["rank"].(type) {
		case int:
			rank = v
		case int64:
			rank = int(v)
		case float64:
			rank = int(v)
		}

		var created int64
		switch v := att["created"].(type) {
		case int:
			created = int64(v)
		case int64:
			created = v
		case float64:
			created = int64(v)
		}
		if created == 0 {
			created = now()
		}

		db.exec(`replace into attachments (id, object, entity, name, size, content_type, creator, caption, description, rank, created) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			id, object, entity, name, size, content_type, creator, caption, description, rank, created)
		count++
	}

	return sl_encode(count), nil
}

// mochi.attachment.fetch(object, entity) -> list: Fetch attachments from a remote entity
func api_attachment_fetch(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return sl_error(fn, "syntax: <object: string>, <entity: string>")
	}

	object, ok := sl.AsString(args[0])
	if !ok || !valid(object, "path") {
		return sl_error(fn, "invalid object")
	}

	entity, ok := sl.AsString(args[1])
	if !ok || !valid(entity, "entity") {
		return sl_error(fn, "invalid entity")
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

	from := ""
	if owner.Identity != nil {
		from = owner.Identity.ID
	}
	if from == "" {
		return sl_error(fn, "no identity")
	}

	// Open stream to remote entity
	s, err := stream(from, entity, app.id, "_attachment/fetch", app.id, app_services(app, nil))
	if err != nil {
		return sl_encode([]map[string]any{}), nil
	}
	defer s.close()

	s.write_content("object", object)

	// Read response
	var attachments []map[string]any
	if err := s.read(&attachments); err != nil {
		return sl_encode([]map[string]any{}), nil
	}

	// Store attachments locally
	for _, att := range attachments {
		id, _ := att["id"].(string)
		if !valid(id, "id") {
			continue
		}
		obj, _ := att["object"].(string)
		name, _ := att["name"].(string)
		size, _ := att["size"].(float64)
		content_type, _ := att["content_type"].(string)
		creator, _ := att["creator"].(string)
		caption, _ := att["caption"].(string)
		description, _ := att["description"].(string)
		rank, _ := att["rank"].(float64)
		created, _ := att["created"].(float64)

		db.exec(`replace into attachments (id, object, entity, name, size, content_type, creator, caption, description, rank, created)
			values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			id, obj, entity, name, int64(size), content_type, creator, caption, description, int(rank), int64(created))
	}

	return sl_encode(attachments), nil
}

// Event handler: _attachment/fetch (responds with attachments for object via stream)
func (e *Event) attachment_event_fetch() {
	object := e.get("object", "")
	if object == "" {
		e.stream.write([]map[string]any{})
		return
	}

	if e.db == nil {
		e.stream.write([]map[string]any{})
		return
	}

	// Get attachments for this object that we own (entity is empty)
	var attachments []Attachment
	err := e.db.scans(&attachments, "select * from attachments where object = ? and entity = '' order by rank", object)
	if err != nil {
		warn("Database error loading attachments: %v", err)
		e.stream.write([]map[string]any{})
		return
	}

	if len(attachments) == 0 {
		e.stream.write([]map[string]any{})
		return
	}

	// Convert to maps and send back via stream (no URL since this is Net)
	var results []map[string]any
	for _, att := range attachments {
		results = append(results, att.to_map())
	}

	e.stream.write(results)
}

// Mochi server: Attachments
// Copyright Alistair Cunningham 2025
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
	attachment_max_size_default = 104857600          // 100MB
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
	"save":             sl.NewBuiltin("mochi.attachment.save", api_attachment_save),
	"create":           sl.NewBuiltin("mochi.attachment.create", api_attachment_create),
	"create_from_file": sl.NewBuiltin("mochi.attachment.create_from_file", api_attachment_create_from_file),
	"insert":           sl.NewBuiltin("mochi.attachment.insert", api_attachment_insert),
	"update":           sl.NewBuiltin("mochi.attachment.update", api_attachment_update),
	"move":             sl.NewBuiltin("mochi.attachment.move", api_attachment_move),
	"delete":           sl.NewBuiltin("mochi.attachment.delete", api_attachment_delete),
	"clear":            sl.NewBuiltin("mochi.attachment.clear", api_attachment_clear),
	"list":             sl.NewBuiltin("mochi.attachment.list", api_attachment_list),
	"get":              sl.NewBuiltin("mochi.attachment.get", api_attachment_get),
	"exists":           sl.NewBuiltin("mochi.attachment.exists", api_attachment_exists),
	"data":             sl.NewBuiltin("mochi.attachment.data", api_attachment_data),
	"path":             sl.NewBuiltin("mochi.attachment.path", api_attachment_path),
	"thumbnail_path":   sl.NewBuiltin("mochi.attachment.thumbnail_path", api_attachment_thumbnail_path),
	"sync":             sl.NewBuiltin("mochi.attachment.sync", api_attachment_sync),
	"fetch":            sl.NewBuiltin("mochi.attachment.fetch", api_attachment_fetch),
})

// Create attachments table
func (db *DB) attachments_setup() {
	db.exec("create table if not exists _attachments ( id text not null primary key, object text not null, entity text not null default '', name text not null, size integer not null, content_type text not null default '', creator text not null default '', caption text not null default '', description text not null default '', rank integer not null default 0, created integer not null )")
	db.exec("create index if not exists _attachments_object on _attachments( object )")
}

// Get the file path for an attachment
func attachment_path(user_id int, app_id string, id string, name string) string {
	safe_name := filepath.Base(name)
	if safe_name == "" || safe_name == "." || safe_name == ".." {
		safe_name = "file"
	}
	return fmt.Sprintf("users/%d/%s/files/%s_%s", user_id, app_id, id, safe_name)
}

// Get the next rank for an object
func (db *DB) attachment_next_rank(object string) int {
	var max_rank int
	row, _ := db.row("select max(rank) as max_rank from _attachments where object=?", object)
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

// Shift ranks up from a position
func (db *DB) attachment_shift_up(object string, from_rank int) {
	db.exec("update _attachments set rank = rank + 1 where object = ? and rank >= ?", object, from_rank)
}

// Shift ranks down from a position
func (db *DB) attachment_shift_down(object string, from_rank int) {
	db.exec("update _attachments set rank = rank - 1 where object = ? and rank > ?", object, from_rank)
}

// Convert Attachment struct to map for Starlark
// If paths are provided: first is app_path, second is action_path (defaults to "attachments")
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
		if len(paths) > 1 && paths[1] != "" {
			action_path = paths[1]
		}
		m["url"] = a.attachment_url(app_path, action_path)
		if is_image(a.Name) {
			m["thumbnail_url"] = a.attachment_url(app_path, action_path) + "/thumbnail"
		}
	}
	return m
}

// Generate URL for attachment
func (a *Attachment) attachment_url(app_path, action_path string) string {
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

// mochi.attachment.save(object, field, captions?, descriptions?, notify?) -> list: Save uploaded files as attachments
func api_attachment_save(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 2 || len(args) > 5 {
		return sl_error(fn, "syntax: <object: string>, <field: string>, [captions: array], [descriptions: array], [notify: array]")
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

	var notify []string
	if len(args) > 4 {
		notify = sl_decode_string_list(args[4])
	}

	action := t.Local("action").(*Action)
	if action == nil {
		return sl_error(fn, "called from non-action")
	}

	app := t.Local("app").(*App)
	if app == nil {
		return sl_error(fn, "no app")
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	user := t.Local("user").(*User)
	creator := ""
	if user != nil && user.Identity != nil {
		creator = user.Identity.ID
	}

	db := db_app(owner, app.active)
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

	var results []map[string]any
	for i, fh := range files {
		// Check size
		if fh.Size > attachment_max_size_default {
			return sl_error(fn, "file too large: %d bytes", fh.Size)
		}

		// Check storage limit (10GB per user across all apps)
		current := dir_size(user_storage_dir(owner))
		if current+fh.Size > file_max_storage {
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

		// Save file
		path := attachment_path(owner.ID, app.id, id, fh.Filename)
		data, err := io.ReadAll(src)
		if err != nil {
			return sl_error(fn, "unable to read uploaded file: %v", err)
		}
		file_write(data_dir+"/"+path, data)

		// Insert record
		db.exec("insert into _attachments (id, object, entity, name, size, content_type, creator, caption, description, rank, created) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
			att.ID, att.Object, att.Entity, att.Name, att.Size, att.ContentType, att.Creator, att.Caption, att.Description, att.Rank, att.Created)

		results = append(results, att.to_map(app.url_path()))
	}

	// Handle federation notify
	if len(notify) > 0 {
		attachment_notify_create(app, owner, object, results, notify)
	}

	return sl_encode(results), nil
}

// mochi.attachment.create(object, name, data, content_type?, caption?, description?, notify?) -> dict: Create an attachment from data
func api_attachment_create(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 3 || len(args) > 7 {
		return sl_error(fn, "syntax: <object: string>, <name: string>, <data: bytes>, [content_type: string], [caption: string], [description: string], [notify: array]")
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

	var notify []string
	if len(args) > 6 {
		notify = sl_decode_string_list(args[6])
	}

	app := t.Local("app").(*App)
	if app == nil {
		return sl_error(fn, "no app")
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	user := t.Local("user").(*User)
	creator := ""
	if user != nil && user.Identity != nil {
		creator = user.Identity.ID
	}

	db := db_app(owner, app.active)
	if db == nil {
		return sl_error(fn, "no database")
	}
	db.attachments_setup()

	// Check size
	if int64(len(bytes)) > attachment_max_size_default {
		return sl_error(fn, "file too large: %d bytes", len(bytes))
	}

	// Check storage limit (10GB per user across all apps)
	current := dir_size(user_storage_dir(owner))
	if current+int64(len(bytes)) > file_max_storage {
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

	// Save file
	path := attachment_path(owner.ID, app.id, id, name)
	file_write(data_dir+"/"+path, bytes)

	// Insert record
	db.exec("insert into _attachments (id, object, entity, name, size, content_type, creator, caption, description, rank, created) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		att.ID, att.Object, att.Entity, att.Name, att.Size, att.ContentType, att.Creator, att.Caption, att.Description, att.Rank, att.Created)

	result := att.to_map(app.url_path())

	// Handle federation notify
	if len(notify) > 0 {
		attachment_notify_create(app, owner, object, []map[string]any{result}, notify)
	}

	return sl_encode(result), nil
}

// mochi.attachment.create_from_file(object, name, path, content_type?, caption?, description?, notify?, id?) -> dict: Create an attachment from a file
func api_attachment_create_from_file(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 3 || len(args) > 8 {
		return sl_error(fn, "syntax: <object: string>, <name: string>, <path: string>, [content_type: string], [caption: string], [description: string], [notify: array], [id: string]")
	}

	object, ok := sl.AsString(args[0])
	if !ok || !valid(object, "path") {
		return sl_error(fn, "invalid object")
	}

	name, ok := sl.AsString(args[1])
	if !ok || name == "" {
		return sl_error(fn, "invalid name")
	}

	src_path, ok := sl.AsString(args[2])
	if !ok || src_path == "" {
		return sl_error(fn, "invalid path")
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

	var notify []string
	if len(args) > 6 {
		notify = sl_decode_string_list(args[6])
	}

	// Optional attachment ID (use existing ID for federation sync)
	provided_id := ""
	if len(args) > 7 {
		provided_id, _ = sl.AsString(args[7])
	}

	app := t.Local("app").(*App)
	if app == nil {
		return sl_error(fn, "no app")
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	user := t.Local("user").(*User)
	creator := ""
	if user != nil && user.Identity != nil {
		creator = user.Identity.ID
	}

	db := db_app(owner, app.active)
	if db == nil {
		return sl_error(fn, "no database")
	}
	db.attachments_setup()

	// Resolve source path (relative to app data dir)
	full_src_path := api_file_path(owner, app, src_path)

	// Get file size
	fi, err := os.Stat(full_src_path)
	if err != nil {
		return sl_error(fn, "unable to read file: %v", err)
	}
	size := fi.Size()

	// Check size
	if size > attachment_max_size_default {
		return sl_error(fn, "file too large: %d bytes", size)
	}

	// Check storage limit (10GB per user across all apps)
	current := dir_size(user_storage_dir(owner))
	if current+size > file_max_storage {
		return sl_error(fn, "storage limit exceeded")
	}

	// Create attachment record - use provided ID or generate new one
	id := provided_id
	if id == "" {
		id = uid()
	}
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

	// Move file to attachment location
	dest_path := data_dir + "/" + attachment_path(owner.ID, app.id, id, name)
	file_move(full_src_path, dest_path)

	// Insert record
	db.exec("insert into _attachments (id, object, entity, name, size, content_type, creator, caption, description, rank, created) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		att.ID, att.Object, att.Entity, att.Name, att.Size, att.ContentType, att.Creator, att.Caption, att.Description, att.Rank, att.Created)

	result := att.to_map(app.url_path())

	// Handle federation notify
	if len(notify) > 0 {
		attachment_notify_create(app, owner, object, []map[string]any{result}, notify)
	}

	return sl_encode(result), nil
}

// mochi.attachment.insert(object, name, data, position, content_type?, caption?, description?, notify?) -> dict: Insert an attachment at position
func api_attachment_insert(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 4 || len(args) > 8 {
		return sl_error(fn, "syntax: <object: string>, <name: string>, <data: bytes>, <position: int>, [content_type: string], [caption: string], [description: string], [notify: array]")
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

	var notify []string
	if len(args) > 7 {
		notify = sl_decode_string_list(args[7])
	}

	app := t.Local("app").(*App)
	if app == nil {
		return sl_error(fn, "no app")
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	user := t.Local("user").(*User)
	creator := ""
	if user != nil && user.Identity != nil {
		creator = user.Identity.ID
	}

	db := db_app(owner, app.active)
	if db == nil {
		return sl_error(fn, "no database")
	}
	db.attachments_setup()

	// Check size
	if int64(len(bytes)) > attachment_max_size_default {
		return sl_error(fn, "file too large: %d bytes", len(bytes))
	}

	// Check storage limit (10GB per user across all apps)
	current := dir_size(user_storage_dir(owner))
	if current+int64(len(bytes)) > file_max_storage {
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

	// Save file
	path := attachment_path(owner.ID, app.id, id, name)
	file_write(data_dir+"/"+path, bytes)

	// Insert record
	db.exec("insert into _attachments (id, object, entity, name, size, content_type, creator, caption, description, rank, created) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		att.ID, att.Object, att.Entity, att.Name, att.Size, att.ContentType, att.Creator, att.Caption, att.Description, att.Rank, att.Created)

	result := att.to_map(app.url_path())

	// Handle federation notify
	if len(notify) > 0 {
		attachment_notify_insert(app, owner, object, result, notify)
	}

	return sl_encode(result), nil
}

// mochi.attachment.update(id, caption, description, notify?) -> dict or None: Update attachment metadata
func api_attachment_update(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 3 || len(args) > 4 {
		return sl_error(fn, "syntax: <id: string>, <caption: string>, <description: string>, [notify: array]")
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

	var notify []string
	if len(args) > 3 {
		notify = sl_decode_string_list(args[3])
	}

	app := t.Local("app").(*App)
	if app == nil {
		return sl_error(fn, "no app")
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	db := db_app(owner, app.active)
	if db == nil {
		return sl_error(fn, "no database")
	}
	db.attachments_setup()

	// Update record
	db.exec("update _attachments set caption = ?, description = ? where id = ?", caption, description, id)

	// Get updated record
	var att Attachment
	if !db.scan(&att, "select * from _attachments where id = ?", id) {
		return sl.None, nil
	}

	result := att.to_map(app.url_path())

	// Handle federation notify
	if len(notify) > 0 {
		attachment_notify_update(app, owner, result, notify)
	}

	return sl_encode(result), nil
}

// mochi.attachment.move(id, position, notify?) -> dict: Move an attachment to a new position
func api_attachment_move(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 2 || len(args) > 3 {
		return sl_error(fn, "syntax: <id: string>, <position: int>, [notify: array]")
	}

	id, ok := sl.AsString(args[0])
	if !ok || id == "" {
		return sl_error(fn, "invalid id")
	}

	position, err := sl.AsInt32(args[1])
	if err != nil || position < 1 {
		return sl_error(fn, "invalid position")
	}

	var notify []string
	if len(args) > 2 {
		notify = sl_decode_string_list(args[2])
	}

	app := t.Local("app").(*App)
	if app == nil {
		return sl_error(fn, "no app")
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	db := db_app(owner, app.active)
	if db == nil {
		return sl_error(fn, "no database")
	}
	db.attachments_setup()

	// Get current attachment
	var att Attachment
	if !db.scan(&att, "select * from _attachments where id = ?", id) {
		return sl_error(fn, "attachment not found")
	}

	old_rank := att.Rank
	new_rank := int(position)

	if old_rank != new_rank {
		if new_rank < old_rank {
			// Moving up: shift items in [new_rank, old_rank) up by 1
			db.exec("update _attachments set rank = rank + 1 where object = ? and rank >= ? and rank < ?", att.Object, new_rank, old_rank)
		} else {
			// Moving down: shift items in (old_rank, new_rank] down by 1
			db.exec("update _attachments set rank = rank - 1 where object = ? and rank > ? and rank <= ?", att.Object, old_rank, new_rank)
		}
		db.exec("update _attachments set rank = ? where id = ?", new_rank, id)
	}

	// Get updated record
	db.scan(&att, "select * from _attachments where id = ?", id)
	result := att.to_map(app.url_path())

	// Handle federation notify
	if len(notify) > 0 {
		attachment_notify_move(app, owner, result, old_rank, notify)
	}

	return sl_encode(result), nil
}

// mochi.attachment.delete(id, notify?) -> None: Delete an attachment
func api_attachment_delete(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return sl_error(fn, "syntax: <id: string>, [notify: array]")
	}

	id, ok := sl.AsString(args[0])
	if !ok || id == "" {
		return sl_error(fn, "invalid id")
	}

	var notify []string
	if len(args) > 1 {
		notify = sl_decode_string_list(args[1])
	}

	app := t.Local("app").(*App)
	if app == nil {
		return sl_error(fn, "no app")
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	db := db_app(owner, app.active)
	if db == nil {
		return sl_error(fn, "no database")
	}
	db.attachments_setup()

	// Get attachment to delete
	var att Attachment
	if !db.scan(&att, "select * from _attachments where id = ?", id) {
		debug("attachment_delete: attachment %s not found in user %d database", id, owner.ID)
		return sl.False, nil
	}

	// Delete file and thumbnail
	path := data_dir + "/" + attachment_path(owner.ID, app.id, att.ID, att.Name)
	file_delete(path)
	file_delete(thumbnail_path(path))

	// Delete record and shift ranks
	db.exec("delete from _attachments where id = ?", id)
	db.attachment_shift_down(att.Object, att.Rank)

	// Handle federation notify
	if len(notify) > 0 {
		attachment_notify_delete(app, owner, att.Object, id, notify)
	}

	return sl.True, nil
}

// mochi.attachment.clear(object, notify?) -> None: Delete all attachments for an object
func api_attachment_clear(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return sl_error(fn, "syntax: <object: string>, [notify: array]")
	}

	object, ok := sl.AsString(args[0])
	if !ok || !valid(object, "path") {
		return sl_error(fn, "invalid object")
	}

	var notify []string
	if len(args) > 1 {
		notify = sl_decode_string_list(args[1])
	}

	app := t.Local("app").(*App)
	if app == nil {
		return sl_error(fn, "no app")
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	db := db_app(owner, app.active)
	if db == nil {
		return sl_error(fn, "no database")
	}
	db.attachments_setup()

	// Get all attachments for object
	var attachments []Attachment
	err := db.scans(&attachments, "select * from _attachments where object = ?", object)
	if err != nil {
		warn("Database error loading attachments for deletion: %v", err)
	}

	// Delete files
	for _, att := range attachments {
		path := attachment_path(owner.ID, app.id, att.ID, att.Name)
		file_delete(data_dir + "/" + path)
	}

	// Delete records
	db.exec("delete from _attachments where object = ?", object)

	// Handle federation notify
	if len(notify) > 0 {
		attachment_notify_clear(app, owner, object, notify)
	}

	return sl.None, nil
}

// mochi.attachment.list(object) -> list: List attachments for an object
func api_attachment_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
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

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	db := db_app(owner, app.active)
	if db == nil {
		return sl_error(fn, "no database")
	}
	db.attachments_setup()

	var attachments []Attachment
	err := db.scans(&attachments, "select * from _attachments where object = ? order by rank", object)
	if err != nil {
		return sl.None, fmt.Errorf("database error: %v", err)
	}

	var results []map[string]any
	for _, att := range attachments {
		results = append(results, att.to_map(app.url_path()))
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

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	db := db_app(owner, app.active)
	if db == nil {
		return sl_error(fn, "no database")
	}
	db.attachments_setup()

	var att Attachment
	if !db.scan(&att, "select * from _attachments where id = ?", id) {
		return sl.None, nil
	}

	return sl_encode(att.to_map(app.url_path())), nil
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

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	db := db_app(owner, app.active)
	if db == nil {
		return sl_error(fn, "no database")
	}
	db.attachments_setup()

	exists, _ := db.exists("select 1 from _attachments where id = ?", id)
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

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	db := db_app(owner, app.active)
	if db == nil {
		return sl_error(fn, "no database")
	}
	db.attachments_setup()

	var att Attachment
	if !db.scan(&att, "select * from _attachments where id = ?", id) {
		return sl.None, nil
	}

	// If entity is set, this is a cached reference - fetch from remote
	if att.Entity != "" {
		data := attachment_fetch_remote(app, att.Entity, id)
		if data != nil {
			return sl_encode(data), nil
		}
		return sl.None, nil
	}

	// Local file
	path := attachment_path(owner.ID, app.id, att.ID, att.Name)
	data := file_read(data_dir + "/" + path)
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

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	db := db_app(owner, app.active)
	if db == nil {
		return sl_error(fn, "no database")
	}
	db.attachments_setup()

	var att Attachment
	if !db.scan(&att, "select * from _attachments where id = ?", id) {
		return sl.None, nil
	}

	// If entity is set, this is a remote attachment - not available locally
	// Use mochi.attachment.fetch() for remote attachments
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

// mochi.attachment.thumbnail_path(id) -> string or None: Get thumbnail path, creating thumbnail if needed
// Returns the thumbnail filename relative to the app's files directory
func api_attachment_thumbnail_path(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
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

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	db := db_app(owner, app.active)
	if db == nil {
		return sl_error(fn, "no database")
	}
	db.attachments_setup()

	var att Attachment
	if !db.scan(&att, "select * from _attachments where id = ?", id) {
		return sl.None, nil
	}

	// If entity is set, this is a remote attachment - not available locally
	if att.Entity != "" {
		return sl.None, nil
	}

	// Get full path and create thumbnail
	path := data_dir + "/" + attachment_path(owner.ID, app.id, att.ID, att.Name)
	thumb, err := thumbnail_create(path)
	if err != nil || thumb == "" {
		return sl.None, nil
	}

	// Return relative path from app's files directory
	// The thumbnail is at: data_dir/users/{user}/app/files/thumbnails/id_name_thumbnail.ext
	// We need to return: thumbnails/id_name_thumbnail.ext
	base := data_dir + "/" + fmt.Sprintf("users/%d/%s/files/", owner.ID, app.id)
	rel, err := filepath.Rel(base, thumb)
	if err != nil {
		return sl.None, nil
	}
	return sl_encode(rel), nil
}

// Federation: notify entities of new attachments
func attachment_notify_create(app *App, owner *User, object string, attachments []map[string]any, notify []string) {
	for _, entity := range notify {
		if !valid(entity, "entity") {
			continue
		}
		from := ""
		if owner != nil && owner.Identity != nil {
			from = owner.Identity.ID
		}
		if from == "" {
			continue
		}

		m := message(from, entity, "app/"+app.id, "_attachment/create")
		m.content = map[string]any{
			"object": object,
		}
		m.add(attachments)
		m.send()
	}
}

// Federation: notify entities of inserted attachment
func attachment_notify_insert(app *App, owner *User, object string, attachment map[string]any, notify []string) {
	for _, entity := range notify {
		if !valid(entity, "entity") {
			continue
		}
		from := ""
		if owner != nil && owner.Identity != nil {
			from = owner.Identity.ID
		}
		if from == "" {
			continue
		}

		m := message(from, entity, "app/"+app.id, "_attachment/insert")
		m.content = map[string]any{
			"object": object,
		}
		m.add(attachment)
		m.send()
	}
}

// Federation: notify entities of updated attachment
func attachment_notify_update(app *App, owner *User, attachment map[string]any, notify []string) {
	for _, entity := range notify {
		if !valid(entity, "entity") {
			continue
		}
		from := ""
		if owner != nil && owner.Identity != nil {
			from = owner.Identity.ID
		}
		if from == "" {
			continue
		}

		m := message(from, entity, "app/"+app.id, "_attachment/update")
		m.add(attachment)
		m.send()
	}
}

// Federation: notify entities of moved attachment
func attachment_notify_move(app *App, owner *User, attachment map[string]any, old_rank int, notify []string) {
	for _, entity := range notify {
		if !valid(entity, "entity") {
			continue
		}
		from := ""
		if owner != nil && owner.Identity != nil {
			from = owner.Identity.ID
		}
		if from == "" {
			continue
		}

		m := message(from, entity, "app/"+app.id, "_attachment/move")
		m.content = map[string]any{
			"old_rank": fmt.Sprintf("%d", old_rank),
		}
		m.add(attachment)
		m.send()
	}
}

// Federation: notify entities of deleted attachment
func attachment_notify_delete(app *App, owner *User, object string, id string, notify []string) {
	for _, entity := range notify {
		if !valid(entity, "entity") {
			continue
		}
		from := ""
		if owner != nil && owner.Identity != nil {
			from = owner.Identity.ID
		}
		if from == "" {
			continue
		}

		m := message(from, entity, "app/"+app.id, "_attachment/delete")
		m.content = map[string]any{
			"object": object,
			"id":     id,
		}
		m.send()
	}
}

// Federation: notify entities of cleared attachments
func attachment_notify_clear(app *App, owner *User, object string, notify []string) {
	for _, entity := range notify {
		if !valid(entity, "entity") {
			continue
		}
		from := ""
		if owner != nil && owner.Identity != nil {
			from = owner.Identity.ID
		}
		if from == "" {
			continue
		}

		m := message(from, entity, "app/"+app.id, "_attachment/clear")
		m.content = map[string]any{
			"object": object,
		}
		m.send()
	}
}

// Federation: fetch attachment data from remote entity
func attachment_fetch_remote(app *App, entity string, id string) []byte {
	debug("attachment_fetch_remote: fetching %s from entity %s via app %s", id, entity, app.id)

	// Check cache first
	cache_path := fmt.Sprintf("%s/attachments/%s/%s/%s", cache_dir, entity, app.id, id)
	if fi, err := os.Stat(cache_path); err == nil {
		if time.Since(fi.ModTime()) > cache_max_age {
			os.Remove(cache_path) // expired, will refetch below
		} else {
			debug("attachment_fetch_remote: returning cached file %s", cache_path)
			return file_read(cache_path)
		}
	}

	// Fetch from remote
	debug("attachment_fetch_remote: opening stream to %s service app/%s event _attachment/data", entity, app.id)
	s, err := stream("", entity, "app/"+app.id, "_attachment/data")
	if err != nil {
		warn("attachment_fetch_remote: stream error: %v", err)
		return nil
	}

	debug("attachment_fetch_remote: sending id=%s", id)
	s.write_content("id", id)

	debug("attachment_fetch_remote: waiting for status response...")
	status, err := s.read_content()
	debug("attachment_fetch_remote: received status=%v err=%v", status, err)
	if err != nil || status["status"] != "200" {
		warn("attachment_fetch_remote: bad status: %v", status)
		return nil
	}

	// Stream directly to cache file (use raw_reader to include any buffered data from CBOR decoder)
	file_mkdir(filepath.Dir(cache_path))
	if !file_write_from_reader(cache_path, s.raw_reader()) {
		debug("attachment_fetch_remote: failed to write cache file")
		return nil
	}

	return file_read(cache_path)
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

// Event handler: attachment/create
func (e *Event) attachment_event_create() {
	object := e.get("object", "")
	if object == "" {
		return
	}

	source := e.from
	if source == "" || !valid(source, "entity") {
		return
	}

	if e.db == nil {
		return
	}
	e.db.attachments_setup()

	var attachments []map[string]any
	if !e.segment(&attachments) {
		return
	}

	for _, att := range attachments {
		id, _ := att["id"].(string)
		if !valid(id, "id") {
			continue
		}
		name, _ := att["name"].(string)

		e.db.exec(`replace into _attachments (id, object, entity, name, size, content_type, creator, caption, description, rank, created) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			id, att["object"], object, name, att["size"], att["content_type"], att["creator"], att["caption"], att["description"], att["rank"], att["created"])

		// Fetch the file immediately to create a full local copy
		if e.user != nil && e.app != nil && name != "" {
			data := attachment_fetch_remote(e.app, object, id)
			if data != nil {
				path := data_dir + "/" + attachment_path(e.user.ID, e.app.id, id, name)
				file_write(path, data)
				e.db.exec(`update _attachments set entity = '' where id = ?`, id)
				info("Attachment %s fetched and stored locally", id)
			}
		}
	}
}

// Event handler: attachment/insert
func (e *Event) attachment_event_insert() {
	object := e.get("object", "")
	if object == "" {
		return
	}

	source := e.from
	if source == "" || !valid(source, "entity") {
		return
	}

	if e.db == nil {
		return
	}
	e.db.attachments_setup()

	var att map[string]any
	if !e.segment(&att) {
		return
	}

	id, _ := att["id"].(string)
	if !valid(id, "id") {
		return
	}

	// Shift existing attachments
	rank := 1
	if r, ok := att["rank"].(float64); ok {
		rank = int(r)
	} else if r, ok := att["rank"].(int); ok {
		rank = r
	}
	if rank < 1 {
		rank = 1
	}
	e.db.attachment_shift_up(object, rank)

	name, _ := att["name"].(string)

	e.db.exec(`insert into _attachments (id, object, entity, name, size, content_type, creator, caption, description, rank, created) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, att["object"], object, name, att["size"], att["content_type"], att["creator"], att["caption"], att["description"], att["rank"], att["created"])

	// Fetch the file immediately to create a full local copy
	if e.user != nil && e.app != nil && name != "" {
		data := attachment_fetch_remote(e.app, object, id)
		if data != nil {
			path := data_dir + "/" + attachment_path(e.user.ID, e.app.id, id, name)
			file_write(path, data)
			// File stored locally, clear entity so it's served from local storage
			e.db.exec(`update _attachments set entity = '' where id = ?`, id)
			info("Attachment %s fetched and stored locally", id)
		}
	}
}

// Event handler: attachment/update
func (e *Event) attachment_event_update() {
	source := e.from
	if source == "" || !valid(source, "entity") {
		return
	}

	if e.db == nil {
		return
	}

	var att map[string]any
	if !e.segment(&att) {
		return
	}

	id, _ := att["id"].(string)
	if id == "" {
		return
	}

	// Only update if we have this attachment and it's from this source
	e.db.exec(`update _attachments set caption = ?, description = ? where id = ? and entity = ?`,
		att["caption"], att["description"], id, source)
}

// Event handler: attachment/move
func (e *Event) attachment_event_move() {
	source := e.from
	if source == "" || !valid(source, "entity") {
		return
	}

	if e.db == nil {
		return
	}

	var att map[string]any
	if !e.segment(&att) {
		return
	}

	id, _ := att["id"].(string)
	if id == "" {
		return
	}

	object, _ := att["object"].(string)

	new_rank := 1
	if r, ok := att["rank"].(float64); ok {
		new_rank = int(r)
	} else if r, ok := att["rank"].(int); ok {
		new_rank = r
	}

	old_rank := int(atoi(e.get("old_rank", ""), 0))

	if old_rank > 0 && new_rank > 0 && old_rank != new_rank {
		if new_rank < old_rank {
			e.db.exec("update _attachments set rank = rank + 1 where object = ? and entity = ? and rank >= ? and rank < ?", object, source, new_rank, old_rank)
		} else {
			e.db.exec("update _attachments set rank = rank - 1 where object = ? and entity = ? and rank > ? and rank <= ?", object, source, old_rank, new_rank)
		}
		e.db.exec("update _attachments set rank = ? where id = ? and entity = ?", new_rank, id, source)
	}
}

// Event handler: attachment/delete
func (e *Event) attachment_event_delete() {
	source := e.from
	if source == "" || !valid(source, "entity") {
		return
	}

	if e.db == nil {
		return
	}

	id := e.get("id", "")
	object := e.get("object", "")
	if id == "" {
		return
	}

	// Get attachment before deleting (may have empty entity if stored locally)
	var att Attachment
	if e.db.scan(&att, "select * from _attachments where id = ?", id) {
		e.db.exec("delete from _attachments where id = ?", id)
		e.db.attachment_shift_down(object, att.Rank)

		// Delete local file and thumbnail if exists
		if e.user != nil && e.app != nil {
			local_path := data_dir + "/" + attachment_path(e.user.ID, e.app.id, att.ID, att.Name)
			file_delete(local_path)
			file_delete(thumbnail_path(local_path))
		}

		// Delete cached file if exists
		cache_path := fmt.Sprintf("%s/attachments/%s/%s/%s", cache_dir, source, e.app.id, id)
		file_delete(cache_path)
	}
}

// Event handler: attachment/clear
func (e *Event) attachment_event_clear() {
	source := e.from
	if source == "" || !valid(source, "entity") {
		return
	}

	if e.db == nil {
		return
	}

	object := e.get("object", "")
	if object == "" {
		return
	}

	// Get all attachments to delete cached files
	var attachments []Attachment
	err := e.db.scans(&attachments, "select * from _attachments where object = ? and entity = ?", object, source)
	if err != nil {
		warn("Database error loading attachments for cache deletion: %v", err)
	}

	for _, att := range attachments {
		cache_path := fmt.Sprintf("%s/attachments/%s/%s", cache_dir, source, att.ID)
		file_delete(cache_path)
	}

	e.db.exec("delete from _attachments where object = ? and entity = ?", object, source)
}

// Event handler: attachment/data (responds with file bytes)
func (e *Event) attachment_event_data() {
	debug("attachment_event_data: called with content=%v", e.content)

	if e.db == nil {
		warn("attachment_event_data: no database, returning 500")
		e.stream.write(map[string]string{"status": "500"})
		return
	}

	id := e.get("id", "")
	if id == "" {
		warn("attachment_event_data: no id, returning 400")
		e.stream.write(map[string]string{"status": "400"})
		return
	}

	debug("attachment_event_data: looking up attachment id=%s", id)
	var att Attachment
	if !e.db.scan(&att, "select * from _attachments where id = ?", id) {
		warn("attachment_event_data: attachment not found in db, returning 404")
		e.stream.write(map[string]string{"status": "404"})
		return
	}

	debug("attachment_event_data: found attachment entity=%q name=%q", att.Entity, att.Name)

	// Only serve if we own this attachment (entity is empty)
	if att.Entity != "" {
		warn("attachment_event_data: not owner (entity=%s), returning 403", att.Entity)
		e.stream.write(map[string]string{"status": "403"})
		return
	}

	path := data_dir + "/" + attachment_path(e.user.ID, e.app.id, att.ID, att.Name)
	debug("attachment_event_data: checking file path=%s", path)
	if !file_exists(path) {
		warn("attachment_event_data: file not found at %s, returning 404", path)
		e.stream.write(map[string]string{"status": "404"})
		return
	}

	debug("attachment_event_data: sending file %s", path)
	e.stream.write(map[string]string{"status": "200"})
	e.stream.write_file(path)
	e.stream.close_write()
	debug("attachment_event_data: done")
}

// mochi.attachment.sync(object, recipients) -> int: Sync attachments to recipients, returns count
func api_attachment_sync(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return sl_error(fn, "syntax: <object: string>, <recipients: array>")
	}

	object, ok := sl.AsString(args[0])
	if !ok || !valid(object, "path") {
		return sl_error(fn, "invalid object")
	}

	recipients := sl_decode_string_list(args[1])
	if len(recipients) == 0 {
		return sl.None, nil
	}

	app := t.Local("app").(*App)
	if app == nil {
		return sl_error(fn, "no app")
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	db := db_app(owner, app.active)
	if db == nil {
		return sl_error(fn, "no database")
	}
	db.attachments_setup()

	// Get existing attachments for object
	var attachments []Attachment
	err := db.scans(&attachments, "select * from _attachments where object = ? order by rank", object)
	if err != nil {
		return sl.None, fmt.Errorf("database error: %v", err)
	}

	if len(attachments) == 0 {
		return sl_encode(0), nil
	}

	// Convert to maps for notification
	var results []map[string]any
	for _, att := range attachments {
		results = append(results, att.to_map(app.url_path()))
	}

	// Send to recipients using existing notify infrastructure
	attachment_notify_create(app, owner, object, results, recipients)

	return sl_encode(len(attachments)), nil
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

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	db := db_app(owner, app.active)
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
	s, err := stream(from, entity, "app/"+app.id, "_attachment/fetch")
	if err != nil {
		return sl_encode([]map[string]any{}), nil
	}

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

		db.exec(`replace into _attachments (id, object, entity, name, size, content_type, creator, caption, description, rank, created)
			values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			id, obj, entity, name, int64(size), content_type, creator, caption, description, int(rank), int64(created))
	}

	return sl_encode(attachments), nil
}

// Event handler: attachment/fetch (responds with attachments for object via stream)
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
	err := e.db.scans(&attachments, "select * from _attachments where object = ? and entity = '' order by rank", object)
	if err != nil {
		warn("Database error loading attachments: %v", err)
		e.stream.write([]map[string]any{})
		return
	}

	if len(attachments) == 0 {
		e.stream.write([]map[string]any{})
		return
	}

	// Convert to maps and send back via stream (no URL since this is P2P)
	var results []map[string]any
	for _, att := range attachments {
		results = append(results, att.to_map())
	}

	e.stream.write(results)
}

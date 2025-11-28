// Mochi server: Attachments
// Copyright Alistair Cunningham 2025
//
// Provides app-level file attachments with federation support.
// Attachments are associated with objects and can be synced between users.

package main

import (
	"fmt"
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
	"io"
	"mime"
	"path/filepath"
	"strings"
)

const (
	attachment_max_size_default = 104857600 // 100MB
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
	"save":   sl.NewBuiltin("mochi.attachment.save", api_attachment_save),
	"create": sl.NewBuiltin("mochi.attachment.create", api_attachment_create),
	"insert": sl.NewBuiltin("mochi.attachment.insert", api_attachment_insert),
	"update": sl.NewBuiltin("mochi.attachment.update", api_attachment_update),
	"move":   sl.NewBuiltin("mochi.attachment.move", api_attachment_move),
	"delete": sl.NewBuiltin("mochi.attachment.delete", api_attachment_delete),
	"clear":  sl.NewBuiltin("mochi.attachment.clear", api_attachment_clear),
	"list":   sl.NewBuiltin("mochi.attachment.list", api_attachment_list),
	"get":    sl.NewBuiltin("mochi.attachment.get", api_attachment_get),
	"data":   sl.NewBuiltin("mochi.attachment.data", api_attachment_data),
	"path":   sl.NewBuiltin("mochi.attachment.path", api_attachment_path),
})

func init() {
	app_helper("attachments", (*DB).attachments_setup)
}

// Create attachments table
func (db *DB) attachments_setup() {
	db.exec("create table if not exists _attachments ( id text not null primary key, object text not null, entity text not null default '', name text not null, size integer not null, content_type text not null default '', creator text not null default '', caption text not null default '', description text not null default '', rank integer not null default 0, created integer not null )")
	db.exec("create index if not exists _attachments_object on _attachments( object )")
}

// Get the file path for an attachment
func (db *DB) attachment_path(id string, name string) string {
	safe_name := filepath.Base(name)
	if safe_name == "" || safe_name == "." || safe_name == ".." {
		safe_name = "file"
	}
	return fmt.Sprintf("users/%d/files/%s_%s", db.user.ID, id, safe_name)
}

// Get the next rank for an object
func (db *DB) attachment_next_rank(object string) int {
	var max_rank int
	row := db.row("select max(rank) as max_rank from _attachments where object=?", object)
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
func (a *Attachment) to_map() map[string]any {
	return map[string]any{
		"id":           a.ID,
		"object":       a.Object,
		"entity":       a.Entity,
		"name":         a.Name,
		"size":         a.Size,
		"content_type": a.ContentType,
		"creator":      a.Creator,
		"caption":      a.Caption,
		"description":  a.Description,
		"rank":         a.Rank,
		"created":      a.Created,
	}
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

// mochi.attachment.save(object, field, captions?, descriptions?, notify?)
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
		// TODO: Get max size from app config
		if fh.Size > attachment_max_size_default {
			return sl_error(fn, "file too large: %d bytes", fh.Size)
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
		path := db.attachment_path(id, fh.Filename)
		data, err := io.ReadAll(src)
		if err != nil {
			return sl_error(fn, "unable to read uploaded file: %v", err)
		}
		file_write(data_dir+"/"+path, data)

		// Insert record
		db.exec("insert into _attachments (id, object, entity, name, size, content_type, creator, caption, description, rank, created) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
			att.ID, att.Object, att.Entity, att.Name, att.Size, att.ContentType, att.Creator, att.Caption, att.Description, att.Rank, att.Created)

		results = append(results, att.to_map())
	}

	// Handle federation notify
	if len(notify) > 0 {
		attachment_notify_create(app, owner, object, results, notify)
	}

	return sl_encode(results), nil
}

// mochi.attachment/create(object, name, data, content_type?, caption?, description?, notify?)
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

	// Check size
	if int64(len(bytes)) > attachment_max_size_default {
		return sl_error(fn, "file too large: %d bytes", len(bytes))
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
	path := db.attachment_path(id, name)
	file_write(data_dir+"/"+path, bytes)

	// Insert record
	db.exec("insert into _attachments (id, object, entity, name, size, content_type, creator, caption, description, rank, created) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		att.ID, att.Object, att.Entity, att.Name, att.Size, att.ContentType, att.Creator, att.Caption, att.Description, att.Rank, att.Created)

	result := att.to_map()

	// Handle federation notify
	if len(notify) > 0 {
		attachment_notify_create(app, owner, object, []map[string]any{result}, notify)
	}

	return sl_encode(result), nil
}

// mochi.attachment/insert(object, name, data, position, content_type?, caption?, description?, notify?)
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

	// Check size
	if int64(len(bytes)) > attachment_max_size_default {
		return sl_error(fn, "file too large: %d bytes", len(bytes))
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
	path := db.attachment_path(id, name)
	file_write(data_dir+"/"+path, bytes)

	// Insert record
	db.exec("insert into _attachments (id, object, entity, name, size, content_type, creator, caption, description, rank, created) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		att.ID, att.Object, att.Entity, att.Name, att.Size, att.ContentType, att.Creator, att.Caption, att.Description, att.Rank, att.Created)

	result := att.to_map()

	// Handle federation notify
	if len(notify) > 0 {
		attachment_notify_insert(app, owner, object, result, notify)
	}

	return sl_encode(result), nil
}

// mochi.attachment/update(id, caption, description, notify?)
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

	// Update record
	db.exec("update _attachments set caption = ?, description = ? where id = ?", caption, description, id)

	// Get updated record
	var att Attachment
	if !db.scan(&att, "select * from _attachments where id = ?", id) {
		return sl.None, nil
	}

	result := att.to_map()

	// Handle federation notify
	if len(notify) > 0 {
		attachment_notify_update(app, owner, result, notify)
	}

	return sl_encode(result), nil
}

// mochi.attachment/move(id, position, notify?)
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
	result := att.to_map()

	// Handle federation notify
	if len(notify) > 0 {
		attachment_notify_move(app, owner, result, old_rank, notify)
	}

	return sl_encode(result), nil
}

// mochi.attachment/delete(id, notify?)
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

	// Get attachment to delete
	var att Attachment
	if !db.scan(&att, "select * from _attachments where id = ?", id) {
		return sl.None, nil
	}

	// Delete file
	path := db.attachment_path(att.ID, att.Name)
	file_delete(data_dir + "/" + path)

	// Delete record and shift ranks
	db.exec("delete from _attachments where id = ?", id)
	db.attachment_shift_down(att.Object, att.Rank)

	// Handle federation notify
	if len(notify) > 0 {
		attachment_notify_delete(app, owner, att.Object, id, notify)
	}

	return sl.None, nil
}

// mochi.attachment/clear(object, notify?)
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

	// Get all attachments for object
	var attachments []Attachment
	db.scans(&attachments, "select * from _attachments where object = ?", object)

	// Delete files
	for _, att := range attachments {
		path := db.attachment_path(att.ID, att.Name)
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

// mochi.attachment.list(object)
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

	var attachments []Attachment
	db.scans(&attachments, "select * from _attachments where object = ? order by rank", object)

	var results []map[string]any
	for _, att := range attachments {
		results = append(results, att.to_map())
	}

	return sl_encode(results), nil
}

// mochi.attachment.get(id)
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

	var att Attachment
	if !db.scan(&att, "select * from _attachments where id = ?", id) {
		return sl.None, nil
	}

	return sl_encode(att.to_map()), nil
}

// mochi.attachment/data(id)
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
	path := db.attachment_path(att.ID, att.Name)
	data := file_read(data_dir + "/" + path)
	return sl_encode(data), nil
}

// mochi.attachment.path(id) - returns file path for direct serving
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

	var att Attachment
	if !db.scan(&att, "select * from _attachments where id = ?", id) {
		return sl.None, nil
	}

	// If entity is set, this is a remote attachment - fetch and cache first
	if att.Entity != "" {
		cache_path := fmt.Sprintf("%s/attachments/%s/%s/%s", cache_dir, att.Entity, app.id, id)
		if !file_exists(cache_path) {
			data := attachment_fetch_remote(app, att.Entity, id)
			if data == nil {
				return sl.None, nil
			}
		}
		return sl_encode(cache_path), nil
	}

	// Local file
	path := data_dir + "/" + db.attachment_path(att.ID, att.Name)
	return sl_encode(path), nil
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
		m.content = map[string]string{
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
		m.content = map[string]string{
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
		m.content = map[string]string{
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
		m.content = map[string]string{
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
		m.content = map[string]string{
			"object": object,
		}
		m.send()
	}
}

// Federation: fetch attachment data from remote entity
func attachment_fetch_remote(app *App, entity string, id string) []byte {
	// Check cache first
	cache_path := fmt.Sprintf("%s/attachments/%s/%s/%s", cache_dir, entity, app.id, id)
	if file_exists(cache_path) {
		return file_read(cache_path)
	}

	// Fetch from remote
	s, err := stream("", entity, "app/"+app.id, "_attachment/data")
	if err != nil {
		debug("attachment_fetch_remote: stream error: %v", err)
		return nil
	}

	s.write_content("id", id)

	status, err := s.read_content()
	if err != nil || status["status"] != "200" {
		debug("attachment_fetch_remote: bad status: %v", status)
		return nil
	}

	// Stream directly to cache file
	file_mkdir(filepath.Dir(cache_path))
	if !file_write_from_reader(cache_path, s.reader) {
		debug("attachment_fetch_remote: failed to write cache file")
		return nil
	}

	return file_read(cache_path)
}

// Decode a Starlark value to a string list
func sl_decode_string_list(v sl.Value) []string {
	var result []string
	switch x := v.(type) {
	case *sl.List:
		for i := 0; i < x.Len(); i++ {
			if s, ok := sl.AsString(x.Index(i)); ok {
				result = append(result, s)
			}
		}
	case sl.Tuple:
		for _, item := range x {
			if s, ok := sl.AsString(item); ok {
				result = append(result, s)
			}
		}
	}
	return result
}

// Check if content type matches allowed patterns
func attachment_type_allowed(content_type string, allowed []string) bool {
	if len(allowed) == 0 {
		return true
	}

	for _, pattern := range allowed {
		if pattern == "*" || pattern == "*/*" {
			return true
		}
		if strings.HasSuffix(pattern, "/*") {
			prefix := strings.TrimSuffix(pattern, "/*")
			if strings.HasPrefix(content_type, prefix+"/") {
				return true
			}
		} else if pattern == content_type {
			return true
		}
	}

	return false
}

// Event handler: attachment/create
func attachment_event_create(e *Event) {
	object := e.content["object"]
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

	var attachments []map[string]any
	if !e.segment(&attachments) {
		return
	}

	for _, att := range attachments {
		e.db.exec(`replace into _attachments (id, object, entity, name, size, content_type, creator, caption, description, rank, created) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			att["id"], att["object"], source, att["name"], att["size"], att["content_type"], att["creator"], att["caption"], att["description"], att["rank"], att["created"])
	}
}

// Event handler: attachment/insert
func attachment_event_insert(e *Event) {
	object := e.content["object"]
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

	var att map[string]any
	if !e.segment(&att) {
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

	e.db.exec(`insert into _attachments (id, object, entity, name, size, content_type, creator, caption, description, rank, created) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		att["id"], att["object"], source, att["name"], att["size"], att["content_type"], att["creator"], att["caption"], att["description"], att["rank"], att["created"])
}

// Event handler: attachment/update
func attachment_event_update(e *Event) {
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
func attachment_event_move(e *Event) {
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

	old_rank := int(atoi(e.content["old_rank"], 0))

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
func attachment_event_delete(e *Event) {
	source := e.from
	if source == "" || !valid(source, "entity") {
		return
	}

	if e.db == nil {
		return
	}

	id := e.content["id"]
	object := e.content["object"]
	if id == "" {
		return
	}

	// Get rank before deleting
	var att Attachment
	if e.db.scan(&att, "select * from _attachments where id = ? and entity = ?", id, source) {
		e.db.exec("delete from _attachments where id = ? and entity = ?", id, source)
		e.db.attachment_shift_down(object, att.Rank)

		// Delete cached file if exists
		cache_path := fmt.Sprintf("%s/attachments/%s/%s", cache_dir, source, id)
		file_delete(cache_path)
	}
}

// Event handler: attachment/clear
func attachment_event_clear(e *Event) {
	source := e.from
	if source == "" || !valid(source, "entity") {
		return
	}

	if e.db == nil {
		return
	}

	object := e.content["object"]
	if object == "" {
		return
	}

	// Get all attachments to delete cached files
	var attachments []Attachment
	e.db.scans(&attachments, "select * from _attachments where object = ? and entity = ?", object, source)

	for _, att := range attachments {
		cache_path := fmt.Sprintf("%s/attachments/%s/%s", cache_dir, source, att.ID)
		file_delete(cache_path)
	}

	e.db.exec("delete from _attachments where object = ? and entity = ?", object, source)
}

// Event handler: attachment/data (responds with file bytes)
func attachment_event_data(e *Event) {
	if e.db == nil {
		e.stream.write(map[string]string{"status": "500"})
		return
	}

	id := e.content["id"]
	if id == "" {
		e.stream.write(map[string]string{"status": "400"})
		return
	}

	var att Attachment
	if !e.db.scan(&att, "select * from _attachments where id = ?", id) {
		e.stream.write(map[string]string{"status": "404"})
		return
	}

	// Only serve if we own this attachment (entity is empty)
	if att.Entity != "" {
		e.stream.write(map[string]string{"status": "403"})
		return
	}

	path := data_dir + "/" + e.db.attachment_path(att.ID, att.Name)
	if !file_exists(path) {
		e.stream.write(map[string]string{"status": "404"})
		return
	}

	e.stream.write(map[string]string{"status": "200"})
	e.stream.write_file(path)
}

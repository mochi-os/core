// Mochi: Attachments app
// Copyright Alistair Cunningham 2025

package main

import (
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"
)

type Attachment struct {
	Entity  string `cbor:"entity,omitempty"`
	ID      string `cbor:"id"`
	Object  string `cbor:"object,omitempty"`
	Rank    int    `cbor:"rank,omitempty"`
	Name    string `cbor:"name,omitempty"`
	Path    string `cbor:"-"`
	Size    int64  `cbor:"size,omitempty"`
	Created int64  `cbor:"-"`
	Image   bool   `cbor:"image,omitempty"`
	Data    []byte `cbor:"-"`
}

type AttachmentRequest struct {
	identity  string
	entity    string
	id        string
	thumbnail bool
	response  chan *Event
	time      int64
}

var (
	attachments_requested []*AttachmentRequest
	attachments_lock      = &sync.Mutex{}
)

func init() {
	a := app("attachments")
	a.db("attachments/attachments.db", attachments_db_create)

	a.path("attachments/:entity/:id", attachments_get)
	a.path("attachments/:entity/:id/:name", attachments_get)
	a.path("attachments/:entity/:id/thumbnail", attachments_get_thumbnail)

	a.service("attachments")
	a.event("get", attachments_get_event)
	a.event("get/thumbnail", attachments_get_thumbnail_event)
	a.event("send", attachments_send_event)
	a.event("send/thumbnail", attachments_send_thumbnail_event)
}

// Create app database
func attachments_db_create(db *DB) {
	db.exec("create table settings ( name text not null primary key, value text not null )")
	db.exec("replace into settings ( name, value ) values ( 'schema', 1 )")

	db.exec("create table attachments ( entity text not null, id text not null, object text not null, rank integer not null default 1, name text not null, path text not null default '', size integer not null default 0, created integer not null, primary key ( entity, id ) )")
	db.exec("create index attachments_object on attachments( object )")
	db.exec("create index attachments_name on attachments( name )")
	db.exec("create index attachments_path on attachments( path )")
	db.exec("create index attachments_created on attachments( created )")
}

// Get list of attachments for an object
func attachments(u *User, format string, values ...any) *[]Attachment {
	object := fmt.Sprintf(format, values...)
	db := db_user(u, "attachments/attachments.db", attachments_db_create)
	defer db.close()

	var as []Attachment
	db.scans(&as, "select * from attachments where object=? order by rank, name", object)

	for i, at := range as {
		as[i].Image = is_image(at.Name)
	}

	return &as
}

// Get data of an attachment
func attachments_get(a *Action) {
	attachments_get_work(a, false)
}

// Get thumbnail of an attachment
func attachments_get_thumbnail(a *Action) {
	attachments_get_work(a, true)
}

// Do the work for the above two functions
func attachments_get_work(a *Action, thumbnail bool) {
	var err error

	entity := a.input("entity")
	if !valid(entity, "entity") {
		a.error(400, "Invalid attachment entity")
		return
	}

	id := a.input("id")
	if !valid(id, "id") {
		a.error(400, "Invalid attachment ID")
		return
	}

	user := 0
	identity := "anonymous"
	if a.user != nil {
		user = a.user.ID
		identity = a.user.Identity.ID
	}

	var at Attachment
	if !a.db.scan(&at, "select * from attachments where entity=? and id=?", entity, id) {
		a.error(404, "Attachment not found")
		return
	}

	// If stored locally
	if at.Path != "" {
		path := fmt.Sprintf("%s/users/%d/%s", data_dir, a.owner.ID, at.Path)
		name := at.Name
		size := at.Size

		if thumbnail {
			path, err = thumbnail_create(path)
			if err != nil {
				a.error(500, "Unable to generate thumbnail: %s", err)
				return
			}
			name = thumbnail_name(name)
			size = file_size(path)
		}

		f, err := os.Open(path)
		defer f.Close()
		if err != nil {
			a.error(500, "Attachment '%s' unable to read local file: %s", err)
			return
		}

		a.web.DataFromReader(http.StatusOK, size, file_name_type(at.Name), f, map[string]string{"Content-Disposition": "inline; filename=\"" + file_name_safe(name) + "\""})
		return
	}

	// Not local, must be remote
	if a.user == nil {
		a.error(404, "Attachment not found locally and not logged in")
		return
	}

	// Check cache
	db_cache := db_open("db/cache.db")
	var c CacheAttachment
	if db_cache.scan(&c, "select * from attachments where user=? and identity=? and entity=? and id=? and thumbnail=?", user, identity, at.Entity, id, thumbnail) {
		file := cache_dir + "/" + c.Path
		f, err := os.Open(file)
		defer f.Close()
		if err == nil {
			a.web.DataFromReader(http.StatusOK, file_size(file), file_name_type(at.Name), f, map[string]string{"Content-Disposition": "inline; filename=\"" + file_name_safe(at.Name) + "\""})
			return
		}
	}

	// Check if we already have a request in progress, and if not add to list
	found := false
	attachments_lock.Lock()
	for _, ar := range attachments_requested {
		if ar.identity == identity && ar.entity == at.Entity && ar.id == id && ar.thumbnail == thumbnail {
			found = true
			break
		}
	}
	ar := AttachmentRequest{identity: identity, entity: at.Entity, id: id, thumbnail: thumbnail, response: make(chan *Event), time: now()}
	attachments_requested = append(attachments_requested, &ar)
	attachments_lock.Unlock()

	// Send request to entity storing file
	if !found {
		action := "get"
		if thumbnail {
			action = "get/thumbnail"
		}

		ev := event(identity, at.Entity, "attachments", action)
		ev.set("id", id)
		ev.send()
	}

	// Wait up to 1 minute for response
	var e *Event
	select {
	case e = <-ar.response:
	case <-time.After(time.Minute):
		a.error(500, "Timeout fetching remote file")
		return
	}
	status := e.get("status", "")
	if status != "200" {
		info("Attachment received negative response for '%s': %s", id, status)
		a.error(500, "Unable to fetch remote file: %s", status)
		return
	}

	// Write data to cache
	name := at.Name
	if thumbnail {
		name = thumbnail_name(name)
	}
	safe := file_name_safe(name)
	thumbnail_dir := ""
	if thumbnail {
		thumbnail_dir = "/thumbnails"
	}
	path := fmt.Sprintf("%d/%s/%s%s/%s_%s", user, identity, at.Entity, thumbnail_dir, id, safe)
	full := cache_dir + "/" + path
	if !file_write_from_reader(full, e.reader) {
		a.error(500, "Unable to write file to cache")
		return
	}

	debug("Creating cache entry '%s' at '%s'", path, full)
	db_cache.exec("replace into attachments ( user, identity, entity, id, thumbnail, path, created ) values ( ?, ?, ?, ?, ?, ?, ? )", user, identity, entity, id, thumbnail, path, now())

	// Write data to browser from cache
	f, err := os.Open(full)
	defer f.Close()
	if err != nil {
		warn("Unable to read newly cached file '%s': %v", full, err)
		a.error(500, "Unable to read newly cached file")
		return
	}
	a.web.DataFromReader(http.StatusOK, file_size(full), file_name_type(at.Name), f, map[string]string{"Content-Disposition": "inline; filename=\"" + safe + "\""})
}

// Request to get a file
func attachments_get_event(e *Event) {
	id := e.get("id", "")

	if !valid(id, "id") {
		info("Request for attachment with invalid ID '%s'", id)
		return
	}

	db := db_user(e.user, "attachments/attachments.db", attachments_db_create)
	defer db.close()

	var at Attachment
	if db.scan(&at, "select * from attachments where entity=? and id=?", e.To, id) {
		file := fmt.Sprintf("%s/users/%d/%s", data_dir, e.user.ID, at.Path)
		if !file_exists(data_dir + "/" + file) {
			ev := event(e.To, e.From, "attachments", "send")
			ev.set("status", "200", "id", id)
			ev.file = file
			ev.send()
			return

		} else {
			warn("Attachment file '%s' not found", data_dir+"/"+file)
		}

	} else {
		info("Request for unknown attachment '%s'", e.ID)
	}

	ev := event(e.To, e.From, "attachments", "send")
	ev.set("status", "404", "id", id)
	ev.send()
}

// Request to get a thumbnail
func attachments_get_thumbnail_event(e *Event) {
	id := e.get("id", "")

	if !valid(id, "id") {
		info("Request for thumbnail with invalid ID '%s'", id)
		return
	}

	db := db_user(e.user, "attachments/attachments.db", attachments_db_create)
	defer db.close()

	var at Attachment
	if db.scan(&at, "select * from attachments where entity=? and id=?", e.To, id) {
		path, err := thumbnail_create(fmt.Sprintf("%s/users/%d/%s", data_dir, e.user.ID, at.Path))
		if err == nil {
			ev := event(e.To, e.From, "attachments", "send/thumbnail")
			ev.set("status", "200", "id", id)
			ev.file = path
			ev.send()
			return
		}
	}

	info("Request for unknown attachment thumbnail '%s'", e.ID)
	ev := event(e.To, e.From, "attachments", "send/thumbnail")
	ev.set("status", "404", "id", id)
	ev.send()
}

// Clean up list of attachments waiting to be received
func attachments_manager() {
	for {
		var survivors []*AttachmentRequest
		time.Sleep(time.Hour)
		now := now()
		attachments_lock.Lock()
		for _, r := range attachments_requested {
			if r.time >= now-86400 {
				survivors = append(survivors, r)
			}
		}
		attachments_requested = survivors
		attachments_lock.Unlock()
	}
}

// Take an array of attachment objects, and save them locally
func attachments_save(as *[]Attachment, u *User, entity string, format string, values ...any) {
	if as == nil {
		return
	}

	object := fmt.Sprintf(format, values...)
	db := db_user(u, "attachments/attachments.db", attachments_db_create)
	defer db.close()

	for _, at := range *as {
		if !valid(at.ID, "id") {
			info("Skipping attachment with invalid ID '%s'", at.ID)
			continue
		}
		if !valid(at.Object, "path") {
			info("Skipping attachment with invalid object '%s'", at.Object)
			continue
		}
		if !valid(at.Name, "filename") {
			info("Skipping attachment with invalid name '%s'", at.Name)
			continue
		}

		path := ""
		if len(at.Data) > 0 {
			path = fmt.Sprintf("%s/%s_%s", object, at.ID, file_name_safe(at.Name))
			file_write(fmt.Sprintf("%s/users/%d/%s", data_dir, u.ID, path), at.Data)
		}

		debug("Attachments creating '%s'", path)
		db.exec("replace into attachments ( entity, id, object, rank, name, path, size, created ) values ( ?, ?, ?, ?, ?, ?, ?, ? )", entity, at.ID, at.Object, at.Rank, at.Name, path, at.Size, at.Created)
	}
}

// Receive a send event, and send it to the channel of any actions waiting for it
func attachments_send_event(e *Event) {
	attachments_send_event_work(e, false)
}

// Receive a thumbnail event, and send it to the channel of any actions waiting for it
func attachments_send_thumbnail_event(e *Event) {
	attachments_send_event_work(e, true)
}

// Do the work for the above two functions
func attachments_send_event_work(e *Event, thumbnail bool) {
	id := e.get("id", "")

	var survivors []*AttachmentRequest
	attachments_lock.Lock()
	for _, r := range attachments_requested {
		if r.identity == e.To && r.entity == e.From && r.id == id && r.thumbnail == thumbnail {
			r.response <- e
		} else {
			survivors = append(survivors, r)
		}
	}
	attachments_requested = survivors
	attachments_lock.Unlock()
}

// Get list of uploaded attachments, save them, and optionally save their data
func (a *Action) upload_attachments(field string, entity string, local bool, format string, values ...any) *[]Attachment {
	object := fmt.Sprintf(format, values...)
	db := db_user(a.user, "attachments/attachments.db", attachments_db_create)
	defer db.close()

	created := now()
	form, err := a.web.MultipartForm()
	check(err)
	var results []Attachment

	for i, f := range form.File[field] {
		if !valid(f.Filename, "filename") {
			info("Skipping uploaded file with invalid name '%s'", f.Filename)
			continue
		}

		id := uid()

		if local {
			// Save attachment and its data locally
			path := fmt.Sprintf("%s/%s_%s", object, id, file_name_safe(f.Filename))
			full := fmt.Sprintf("%s/users/%d/%s", data_dir, a.user.ID, path)
			file_mkdir_for_file(full)
			a.web.SaveUploadedFile(f, full)
			size := file_size(full)

			debug("Attachment creating local '%s' at '%s'", path, full)
			db.exec("replace into attachments ( entity, id, object, rank, name, path, size, created ) values ( ?, ?, ?, ?, ?, ?, ?, ? )", entity, id, object, i+1, f.Filename, path, size, created)
			results = append(results, Attachment{Entity: entity, ID: id, Object: object, Rank: i + 1, Name: f.Filename, Path: path, Size: size, Created: created, Image: is_image(f.Filename)})

		} else {
			// Save information about the attachment locally, but not its data because we're sending that to the owning entity

			// Temporarily save the file and read it back in again. I don't want to
			// do this, but it appears that Gin does not offer any alternative.
			tmp := fmt.Sprintf("%s/tmp/%s_%s", cache_dir, id, file_name_safe(f.Filename))
			a.web.SaveUploadedFile(f, tmp)
			size := file_size(tmp)

			debug("Attachment creating remote '%s' '%s'", object, id)
			db.exec("replace into attachments ( entity, id, object, rank, name, size, created ) values ( ?, ?, ?, ?, ?, ?, ? )", entity, id, object, i+1, f.Filename, size, created)
			results = append(results, Attachment{Entity: entity, ID: id, Object: object, Rank: i + 1, Name: f.Filename, Size: size, Created: created, Data: file_read(tmp), Image: is_image(f.Filename)})

			file_delete(tmp)
		}
	}

	return &results
}

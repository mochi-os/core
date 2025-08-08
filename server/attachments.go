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
	Entity  string
	ID      string
	Object  string
	Rank    int
	Name    string
	Path    string `json:"-"`
	Size    int64
	Created int64  `json:"-"`
	Data    []byte `json:",omitempty"`
	Image   bool
}

type AttachmentRequest struct {
	identity  string
	entity    string
	id        string
	thumbnail bool
	response  chan Attachment
	time      int64
}

var (
	attachments_requested []*AttachmentRequest
	mu                    = &sync.Mutex{}
)

func init() {
	a := app("attachments")
	a.db("attachments.db", attachments_db_create)

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
	db := db_user(u, "attachments.db", attachments_db_create)
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
	var c CacheAttachment
	db_cache := db_open("db/cache.db")
	if db_cache.scan(&c, "select * from attachments where user=? and identity=? and entity=? and id=? and thumbnail=?", user, identity, at.Entity, id, thumbnail) {
		file := cache_dir + "/" + c.Path
		f, err := os.Open(file)
		defer f.Close()
		if err == nil {
			a.web.DataFromReader(http.StatusOK, file_size(file), file_name_type(at.Name), f, map[string]string{"Content-Disposition": "inline; filename=\"" + file_name_safe(at.Name) + "\""})
			return
		}
	}

	// Check if we already have a request in progress
	found := false
	mu.Lock()
	for _, ar := range attachments_requested {
		if ar.identity == identity && ar.entity == at.Entity && ar.id == id && ar.thumbnail == thumbnail {
			found = true
			break
		}
	}
	mu.Unlock()

	// Send request to entity storing file
	if !found {
		action := "get"
		if thumbnail {
			action = "get/thumbnail"
		}

		//TODO Structure content?
		e := Event{ID: uid(), From: identity, To: at.Entity, Service: "attachments", Action: action, Content: id}
		e.send()
	}

	// Add to list of requested files
	mu.Lock()
	ar := AttachmentRequest{identity: identity, entity: at.Entity, id: id, thumbnail: thumbnail, response: make(chan Attachment), time: now()}
	attachments_requested = append(attachments_requested, &ar)
	mu.Unlock()

	// Wait for response
	r := <-ar.response
	if len(r.Data) == 0 {
		a.error(500, "Unable to fetch remote attachment")
		return
	}

	// Write data to web browser
	name := at.Name
	if thumbnail {
		name = thumbnail_name(name)
	}
	safe := file_name_safe(name)
	a.web.Header("Content-Disposition", "inline; filename=\""+safe+"\"")
	a.web.Data(http.StatusOK, file_name_type(name), r.Data)

	// Add to cache
	thumbnail_dir := ""
	if thumbnail {
		thumbnail_dir = "thumbnail/"
	}
	path := fmt.Sprintf("attachments/%d/%s/%s/%s%s_%s", user, identity, at.Entity, thumbnail_dir, id, safe)
	file_write(cache_dir+"/"+path, r.Data)
	db_cache.exec("replace into attachments ( user, identity, entity, id, thumbnail, path, created ) values ( ?, ?, ?, ?, ?, ?, ? )", user, identity, entity, id, thumbnail, path, now())
}

// Request to get a file
func attachments_get_event(e *Event) {
	log_debug("Request for attachment '%s' '%s'", e.To, e.Content)
	db := db_user(e.user, "attachments.db", attachments_db_create)
	defer db.close()

	if !valid(e.Content, "id") {
		log_info("Request for attachment with invalid ID '%s'", e.Content)
		return
	}

	var at Attachment
	if db.scan(&at, "select * from attachments where entity=? and id=?", e.To, e.Content) {
		file := fmt.Sprintf("%s/users/%d/%s", data_dir, e.user.ID, at.Path)
		if file_exists(file) {
			at.Data = file_read(file)
			ev := Event{ID: e.ID, From: e.To, To: e.From, Service: "attachments", Action: "send", Content: json_encode(at)}
			ev.send()
			return
		}
	}

	//TODO Return more meaningful error status
	log_info("Request for unknown attachment '%s'", e.ID)
	ev := Event{ID: e.ID, From: e.To, To: e.From, Service: "attachments", Action: "send"}
	ev.send()
}

// Request to get a thumbnail
func attachments_get_thumbnail_event(e *Event) {
	log_debug("Request for thumbnail '%s' '%s'", e.To, e.Content)
	db := db_user(e.user, "attachments.db", attachments_db_create)
	defer db.close()

	if !valid(e.Content, "id") {
		log_info("Request for thumbnail with invalid ID '%s'", e.Content)
		return
	}

	var at Attachment
	if db.scan(&at, "select * from attachments where entity=? and id=?", e.To, e.Content) {
		path, err := thumbnail_create(fmt.Sprintf("%s/users/%d/%s", data_dir, e.user.ID, at.Path))
		if err == nil {
			at.Data = file_read(path)
			ev := Event{ID: e.ID, From: e.To, To: e.From, Service: "attachments", Action: "send/thumbnail", Content: json_encode(at)}
			ev.send()
			return
		}
	}

	log_info("Request for unknown attachment thumbnail '%s'", e.ID)
	ev := Event{ID: e.ID, From: e.To, To: e.From, Service: "attachments", Action: "send/thumbnail"}
	ev.send()
}

// Clean up list of attachments waiting to be received
func attachments_manager() {
	for {
		var survivors []*AttachmentRequest
		time.Sleep(time.Hour)
		now := now()
		mu.Lock()
		for _, r := range attachments_requested {
			if r.time >= now-86400 {
				survivors = append(survivors, r)
			}
		}
		attachments_requested = survivors
		mu.Unlock()
	}
}

// Take an array of attachment objects, and save them locally
func attachments_save(as *[]Attachment, u *User, entity string, format string, values ...any) {
	if as == nil {
		return
	}

	object := fmt.Sprintf(format, values...)
	db := db_user(u, "attachments.db", attachments_db_create)
	defer db.close()

	for _, at := range *as {
		if !valid(at.ID, "id") {
			log_info("Skipping attachment with invalid ID '%s'", at.ID)
			continue
		}
		if !valid(at.Object, "path") {
			log_info("Skipping attachment with invalid object '%s'", at.Object)
			continue
		}
		if !valid(at.Name, "filename") {
			log_info("Skipping attachment with invalid name '%s'", at.Name)
			continue
		}

		path := ""
		if len(at.Data) > 0 {
			path = fmt.Sprintf("attachments/%s/%s_%s", object, at.ID, file_name_safe(at.Name))
			file_write(fmt.Sprintf("%s/users/%d/%s", data_dir, u.ID, path), at.Data)
		}

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
	if len(e.Content) == 0 {
		log_debug("Received negative response for attachment '%s'", e.ID)
		return
	}

	var at Attachment
	if !json_decode(&at, e.Content) {
		log_info("Dropping attachment send vent with invalid JSON content '%s'", e.Content)
		return
	}

	var survivors []*AttachmentRequest
	mu.Lock()
	for _, r := range attachments_requested {
		if r.identity == e.To && r.entity == e.From && r.id == at.ID && r.thumbnail == thumbnail {
			r.response <- at
		} else {
			survivors = append(survivors, r)
		}
	}
	attachments_requested = survivors
	mu.Unlock()
}

// Get list of uploaded attachments, save them, and optionally save their data
func (a *Action) upload_attachments(field string, entity string, local bool, format string, values ...any) *[]Attachment {
	object := fmt.Sprintf(format, values...)
	db := db_user(a.user, "attachments.db", attachments_db_create)
	defer db.close()

	created := now()
	form, err := a.web.MultipartForm()
	check(err)
	var results []Attachment

	for i, f := range form.File[field] {
		log_debug("Attachment uploading '%s'", f.Filename)
		if !valid(f.Filename, "filename") {
			log_info("Skipping uploaded file with invalid name '%s'", f.Filename)
			continue
		}

		id := uid()

		if local {
			// Save attachment and its data locally
			// Consider putting files in subdirectories?
			path := fmt.Sprintf("attachments/%s/%s_%s", object, id, file_name_safe(f.Filename))
			dir := fmt.Sprintf("%s/users/%d/", data_dir, a.user.ID)
			file_mkdir(dir + "/attachments/" + object)
			log_debug("Attachment writing file '%s'", dir+"/"+path)
			a.web.SaveUploadedFile(f, dir+"/"+path)
			size := file_size(dir + "/" + path)

			db.exec("replace into attachments ( entity, id, object, rank, name, path, size, created ) values ( ?, ?, ?, ?, ?, ?, ?, ? )", entity, id, object, i+1, f.Filename, path, size, created)
			results = append(results, Attachment{Entity: entity, ID: id, Object: object, Rank: i + 1, Name: f.Filename, Path: path, Size: size, Created: created, Image: is_image(f.Filename)})

		} else {
			// Save information about the attachment locally, but not its data because we're sending that to the owning entity

			// Temporarily save the file and read it back in again. I don't want to
			// do this, but it appears that Gin does not offer any alternative.
			tmp := fmt.Sprintf("%s/tmp/%s_%s", cache_dir, id, file_name_safe(f.Filename))
			a.web.SaveUploadedFile(f, tmp)
			size := file_size(tmp)

			db.exec("replace into attachments ( entity, id, object, rank, name, size, created ) values ( ?, ?, ?, ?, ?, ?, ? )", entity, id, object, i+1, f.Filename, size, created)
			results = append(results, Attachment{Entity: entity, ID: id, Object: object, Rank: i + 1, Name: f.Filename, Size: size, Created: created, Data: file_read(tmp), Image: is_image(f.Filename)})

			file_delete(tmp)
		}
	}

	return &results
}

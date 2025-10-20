// Mochi: Attachments app
// Copyright Alistair Cunningham 2025

package main

import (
	"fmt"
	"net/http"
	"os"
)

type Attachment struct {
	Entity  string `cbor:"entity,omitempty" json:"entity" map:"-"`
	ID      string `cbor:"id" json:"id" map:"id"`
	Object  string `cbor:"object,omitempty" json:"object" map:"object"`
	Rank    int    `cbor:"rank,omitempty" json:"rank" map:"rank"`
	Name    string `cbor:"name,omitempty" json:"name" map:"name"`
	Path    string `cbor:"-" json:"-" map:"-"`
	Size    int64  `cbor:"size,omitempty" json:"size" map:"size"`
	Created int64  `cbor:"-" json:"created" map:"created"`
	Image   bool   `cbor:"image,omitempty" json:"image" map:"image"`
	Data    []byte `cbor:"-" json:"-" map:"-"`
}

func init() {
	a := app("attachments")
	a.db("attachments/attachments.db", attachments_db_create)

	a.path("attachments/:entity/:id", attachments_get)
	a.path("attachments/:entity/:id/:name", attachments_get)
	a.path("attachments/:entity/:id/thumbnail", attachments_get_thumbnail)

	a.service("attachments")
	a.event("get", attachments_get_event)
	a.event("get/thumbnail", attachments_get_thumbnail_event)
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
func attachments(u *User, object string) *[]Attachment {
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
	if !a.owner.db.scan(&at, "select * from attachments where entity=? and id=?", entity, id) {
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

	// Request from owning entity
	event := "get"
	if thumbnail {
		event = "get/thumbnail"
	}
	s := stream(identity, at.Entity, "attachments", event)
	s.write_content("id", id)

	response, err := s.read_content()
	if err != nil || response["status"] != "200" {
		a.error(500, "Unable to fetch remote file: %s", response["status"])
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
	if !file_write_from_reader(full, s.reader) {
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

// Receieved a request to get a file
func attachments_get_event(e *Event) {
	id := e.get("id", "")
	s := e.stream

	if !valid(id, "id") {
		info("Request for attachment with invalid ID '%s'", id)
		s.write_content("status", "400")
		return
	}

	db := db_user(e.user, "attachments/attachments.db", attachments_db_create)
	defer db.close()

	var at Attachment
	if db.scan(&at, "select * from attachments where entity=? and id=?", e.to, id) {
		file := fmt.Sprintf("%s/users/%d/%s", data_dir, e.user.ID, at.Path)
		if file_exists(file) {
			s.write_content("status", "200")
			s.write_file(file)
			return

		} else {
			warn("Attachment file '%s' not found", data_dir+"/"+file)
		}

	} else {
		info("Request for unknown attachment '%s'", id)
	}

	s.write_content("status", "404")
}

// Request to get a thumbnail
func attachments_get_thumbnail_event(e *Event) {
	id := e.get("id", "")
	s := e.stream

	if !valid(id, "id") {
		info("Request for thumbnail with invalid ID '%s'", id)
		return
	}

	db := db_user(e.user, "attachments/attachments.db", attachments_db_create)
	defer db.close()

	var at Attachment
	if db.scan(&at, "select * from attachments where entity=? and id=?", e.to, id) {
		file, err := thumbnail_create(fmt.Sprintf("%s/users/%d/%s", data_dir, e.user.ID, at.Path))
		if err == nil {
			s.write_content("status", "200")
			s.write_file(file)
			return
		}
	}

	info("Request for unknown attachment thumbnail '%s'", id)
	s.write_content("status", "404")
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

// Take an array of attachment maps, and save them locally
func attachments_save_maps(as *[]map[string]string, u *User, entity string, format string, values ...any) {
	if as == nil {
		return
	}

	object := fmt.Sprintf(format, values...)
	db := db_user(u, "attachments/attachments.db", attachments_db_create)
	defer db.close()

	for _, a := range *as {
		if !valid(a["id"], "id") {
			info("Skipping attachment with invalid ID '%s'", a["id"])
			continue
		}

		if !valid(a["object"], "path") {
			info("Skipping attachment with invalid object '%s'", a["object"])
			continue
		}

		if !valid(a["rank"], "integer") {
			info("Skipping attachment with invalid rank '%s'", a["rank"])
			continue
		}

		if !valid(a["name"], "filename") {
			info("Skipping attachment with invalid name '%s'", a["name"])
			continue
		}

		if !valid(a["size"], "integer") {
			info("Skipping attachment with invalid size '%s'", a["size"])
			continue
		}

		if !valid(a["created"], "integer") {
			info("Skipping attachment with invalid created time '%s'", a["created"])
			continue
		}

		path := ""
		if len(a["data"]) > 0 {
			path = fmt.Sprintf("%s/%s_%s", object, a["id"], file_name_safe(a["name"]))
			file_write(fmt.Sprintf("%s/users/%d/%s", data_dir, u.ID, path), []byte(a["data"]))
		}

		debug("Attachments creating '%s'", path)
		db.exec("replace into attachments ( entity, id, object, rank, name, path, size, created ) values ( ?, ?, ?, ?, ?, ?, ?, ? )", entity, a["id"], a["object"], a["rank"], a["name"], path, a["size"], a["created"])
	}
}

// Get list of uploaded attachments, save them, and optionally save their data
func (a *Action) upload_attachments(field string, entity string, object string, local bool) *[]Attachment {
	db := db_user(a.user, "attachments/attachments.db", attachments_db_create)
	defer db.close()

	created := now()
	form := must(a.web.MultipartForm())
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

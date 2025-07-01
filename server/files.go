// Comms: Files app
// Copyright Alistair Cunningham 2025

package main

import (
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"
)

type File struct {
	ID      string
	Name    string
	Path    string `json:"-"`
	Size    int64
	Created int64 `json:"-"`
	Rank    int
}

type FileRequest struct {
	Identity string
	Entity   string
	ID       string
	Extra    string
	Response chan FileResponse
	Time     int64
}

type FileResponse struct {
	ID     string
	Extra  string
	Status int
	Name   string
	Size   int64
	Data   []byte
}

var (
	mu              = &sync.Mutex{}
	files_requested []*FileRequest
)

func init() {
	a := app("files")
	a.home("files", map[string]string{"en": "Files"})
	a.db("files.db", files_db_create)

	a.path("files", files_list)
	a.path("files/create", files_create)
	a.path("files/:entity/:id", files_view)
	a.path("files/:entity/:id/:name", files_view)
	a.path("files/thumbnail/:entity/:id", files_thumbnail)
	a.path("files/thumbnail/:entity/:id/:name", files_thumbnail)

	a.service("files")
	a.event("get", files_get_event)
	a.event("send", files_send_event)
}

// Create app database
func files_db_create(db *DB) {
	db.exec("create table settings ( name text not null primary key, value text not null )")
	db.exec("replace into settings ( name, value ) values ( 'schema', 1 )")

	db.exec("create table files ( id text not null primary key, name text not null, path text not null, size integer default 0, created integer not null )")
	db.exec("create index files_name on files( name )")
	db.exec("create index files_path on files( path )")
	db.exec("create index files_created on files( created )")
}

// Create a new file
func files_create(a *Action) {
	a.template("files/create")
}

// Request to get a file
func files_get_event(e *Event) {
	var r FileRequest
	if !json_decode(&r, e.Content) {
		log_info("Files dropping get event with invalid JSON '%s'", e.Content)
		return
	}

	if !valid(r.ID, "uid") {
		log_info("Files dropping get event with invalid file id '%s'", r.ID)
		return
	}

	var f File
	if e.db.scan(&f, "select * from files where id=?", r.ID) {
		file := fmt.Sprintf("%s/users/%d/files/%s", data_dir, e.user.ID, f.Path)

		if r.Extra == "thumbnail" {
			thumb, err := thumbnail_create(file)
			if err == nil {
				a := Event{ID: e.ID, From: e.To, To: e.From, Service: "files", Action: "send", Content: json_encode(FileResponse{ID: r.ID, Extra: r.Extra, Status: 200, Name: thumbnail_name(f.Name), Size: file_size(thumb), Data: file_read(thumb)})}
				a.send()
				return
			}

		} else if file_exists(file) {
			a := Event{ID: e.ID, From: e.To, To: e.From, Service: "files", Action: "send", Content: json_encode(FileResponse{ID: r.ID, Extra: r.Extra, Status: 200, Name: f.Name, Size: f.Size, Data: file_read(file)})}
			a.send()
			return
		}
	}

	log_info("Files received request for unknown file '%s'", r.ID)
	a := Event{ID: e.ID, From: e.To, To: e.From, Service: "files", Action: "send", Content: json_encode(FileResponse{ID: r.ID, Extra: r.Extra, Status: 404})}
	a.send()
}

// List existing files
func files_list(a *Action) {
	var fs []File
	a.db.scans(&fs, "select * from files order by name")
	a.template("files/list", Map{"Files": fs})
}

// Clean up list of files to be received
func files_manager() {
	for {
		var survivors []*FileRequest
		time.Sleep(time.Hour)
		now := now()
		mu.Lock()
		for _, fr := range files_requested {
			if fr.Time >= now-86400 {
				survivors = append(survivors, fr)
			}
		}
		files_requested = survivors
		mu.Unlock()
	}
}

// Receive a send event, and send it to the channel of any actions waiting for it
func files_send_event(e *Event) {
	var survivors []*FileRequest

	var r FileResponse
	if !json_decode(&r, e.Content) {
		log_info("Files dropping send event with invalid JSON content '%s'", e.Content)
		return
	}

	mu.Lock()
	for _, fr := range files_requested {
		if fr.Identity == e.To && fr.Entity == e.From && fr.ID == r.ID {
			fr.Response <- r
		} else {
			survivors = append(survivors, fr)
		}
	}
	files_requested = survivors
	mu.Unlock()
}

// View the thumbnail of a file
func files_thumbnail(a *Action) {
	files_view_action(a, "thumbnail")
}

// View a file
func files_view(a *Action) {
	files_view_action(a, "")
}

// Do the work of viewing a file
func files_view_action(a *Action, extra string) {
	var err error

	id := a.input("id")
	if !valid(id, "id") {
		a.error(400, "Invalid ID for file")
		return
	}

	// Check local
	var f File
	if a.db.scan(&f, "select * from files where id=?", id) {
		name := f.Name

		path := fmt.Sprintf("%s/users/%d/files/%s", data_dir, a.user.ID, f.Path)
		if !file_exists(path) {
			a.error(500, "File exists in database but no file found")
			return
		}

		if extra == "thumbnail" {
			path, err = thumbnail_create(path)
			if err != nil {
				a.error(500, "Unable to generate thumbnail: %s", err)
				return
			}
			name = thumbnail_name(name)
		}

		r, err := os.Open(path)
		defer r.Close()
		if err == nil {
			a.web.DataFromReader(http.StatusOK, f.Size, file_name_type(f.Name), r, map[string]string{"Content-Disposition": "inline; filename=\"" + file_name_safe(name) + "\""})
			return
		}
	}

	// Not local, must be remote
	entity := a.input("entity")
	if !valid(entity, "public") {
		a.error(400, "Invalid entity for file")
		return
	}

	// Check cache
	var c CacheFile
	db_cache := db_open("db/cache.db")
	if db_cache.scan(&c, "select * from files where user=? and identity=? and entity=? and id=? and extra=?", a.user.ID, a.user.Identity.ID, entity, id, extra) {
		r, err := os.Open(cache_dir + "/" + c.Path)
		defer r.Close()
		if err == nil {
			a.web.DataFromReader(http.StatusOK, c.Size, file_name_type(c.Name), r, map[string]string{"Content-Disposition": "inline; filename=\"" + file_name_safe(c.Name) + "\""})
			return
		}
	}

	// Send event asking for remote file, but only if we don't already have a request for the same file
	found := false
	mu.Lock()
	for _, fr := range files_requested {
		if fr.Identity == a.user.Identity.ID && fr.Entity == entity && fr.ID == id && fr.Extra == extra {
			found = true
			break
		}
	}
	mu.Unlock()
	if !found {
		e := Event{ID: uid(), From: a.user.Identity.ID, To: entity, Service: "files", Action: "get", Content: json_encode(Map{"ID": id, "Extra": extra})}
		e.send()
	}

	// Add to list of requested files
	mu.Lock()
	fr := FileRequest{Identity: a.user.Identity.ID, Entity: entity, ID: id, Extra: extra, Response: make(chan FileResponse), Time: now()}
	files_requested = append(files_requested, &fr)
	mu.Unlock()

	// Wait for response
	r := <-fr.Response
	if r.Status == 200 {
		safe := file_name_safe(r.Name)
		a.web.Header("Content-Disposition", "inline; filename=\""+safe+"\"")
		a.web.Data(http.StatusOK, file_name_type(r.Name), r.Data)

		// Add to cache
		path := fmt.Sprintf("files/%d/%s/%s/%s_%s", a.user.ID, a.user.Identity.ID, entity, id, safe)
		file_mkdir(fmt.Sprintf("%s/files/%d/%s/%s", cache_dir, a.user.ID, a.user.Identity.ID, entity))
		file_write(cache_dir+"/"+path, r.Data)
		db_cache.exec("replace into files ( user, identity, entity, id, extra, name, path, size, created ) values ( ?, ?, ?, ?, ?, ?, ?, ?, ? )", a.user.ID, a.user.Identity.ID, entity, id, extra, r.Name, path, r.Size, now())

	} else {
		a.error(500, "Unable to fetch remote file")
	}
}

// Upload one or more files for an action
func (a *Action) upload(name string) []File {
	created := now()
	db := db_user(a.user, "db/files.db", files_db_create)
	defer db.close()

	var results []File
	form, err := a.web.MultipartForm()
	check(err)

	for i, file := range form.File[name] {
		id := uid()
		dir := fmt.Sprintf("%s/users/%d/files", data_dir, a.user.ID)
		file_mkdir(dir)
		path := id + "_" + file_name_safe(file.Filename)
		a.web.SaveUploadedFile(file, dir+"/"+path)
		size := file_size(dir + "/" + path)

		db.exec("replace into files ( id, name, path, size, created ) values ( ?, ?, ?, ?, ? )", id, file.Filename, path, size, created)
		results = append(results, File{ID: id, Name: file.Filename, Path: path, Size: size, Created: created, Rank: i + 1})
	}

	return results
}

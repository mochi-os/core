// Comms: Files app
// Copyright Alistair Cunningham 2025

package main

import (
	"fmt"
	"net/http"
	"sync"
	"time"
)

type File struct {
	ID      string
	Name    string
	Path    string `json:"-"`
	Size    int64
	Rank    int
	Updated int64 `json:"-"`
}

type FileRequest struct {
	Entity   string
	ID       string
	Response chan FileResponse
	Time     int64
}

type FileResponse struct {
	ID     string
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

	a.service("files")
	a.event("get", files_get_event)
	a.event("send", files_send_event)
}

// Create app database
func files_db_create(db *DB) {
	db.exec("create table settings ( name text not null primary key, value text not null )")
	db.exec("replace into settings ( name, value ) values ( 'schema', 1 )")

	db.exec("create table files ( id text not null primary key, name text not null, path text not null, size integer default 0, rank integer not null default 1, updated integer not null )")
	db.exec("create index files_name on files( name )")
	db.exec("create index files_path on files( path )")
	db.exec("create index files_updated on files( updated )")
}

// Create a new file
func files_create(a *Action) {
	a.template("files/create")
}

// Request to get a file
func files_get_event(e *Event) {
	id := e.Content
	if !valid(id, "uid") {
		log_info("Files dropping get event with invalid file id '%s'", id)
		return
	}

	var f File
	if e.db.scan(&f, "select * from files where id=?", id) {
		file := fmt.Sprintf("users/%d/files/%s", e.user.ID, f.Path)
		if file_exists(file) {
			r := Event{ID: e.ID, From: e.To, To: e.From, Service: "files", Action: "send", Content: json_encode(FileResponse{ID: id, Status: 200, Name: f.Name, Size: f.Size, Data: file_read(file)})}
			r.send()
			return
		}
	}

	log_info("Files received request for unknown file '%s'", id)
	r := Event{ID: e.ID, From: e.To, To: e.From, Service: "files", Action: "send", Content: json_encode(FileResponse{ID: id, Status: 404})}
	r.send()
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
		if fr.Entity == e.From && fr.ID == r.ID {
			fr.Response <- r
		} else {
			survivors = append(survivors, fr)
		}
	}
	files_requested = survivors
	mu.Unlock()
}

// View a file
func files_view(a *Action) {
	id := a.input("id")

	var f File
	if a.db.scan(&f, "select * from files where id=?", id) {
		file := fmt.Sprintf("users/%d/files/%s", a.user.ID, f.Path)
		if !file_exists(file) {
			a.error(500, "File data not found")
			return
		}
		a.web.Header("Content-Disposition", "inline; filename=\""+file_name_safe(f.Name)+"\"")
		a.web.Data(http.StatusOK, file_name_type(f.Name), file_read(file))

	} else {
		entity := a.input("entity")
		if entity == "" || entity == "local" {
			a.error(404, "File not found")
			return
		}

		//TODO Check cache

		//TODO Don't send another request if we already have an active request from this identity
		e := Event{ID: uid(), From: a.user.Identity.ID, To: entity, Service: "files", Action: "get", Content: id}
		e.send()

		mu.Lock()
		fr := FileRequest{Entity: entity, ID: id, Response: make(chan FileResponse), Time: now()}
		files_requested = append(files_requested, &fr)
		mu.Unlock()

		r := <-fr.Response
		if r.Status == 200 {
			a.web.Header("Content-Disposition", "inline; filename=\""+file_name_safe(r.Name)+"\"")
			a.web.Data(http.StatusOK, file_name_type(r.Name), r.Data)
			//TODO Store in cache
		} else {
			a.error(500, "Unable to fetch remote file")
		}
	}
}

// Upload one or more files for an action
func (a *Action) upload(name string) []File {
	updated := now()
	db := db_user(a.user, "db/files.db", files_db_create)
	defer db.close()

	var results []File
	form, err := a.web.MultipartForm()
	check(err)

	for i, file := range form.File[name] {
		id := uid()
		dir := fmt.Sprintf("users/%d/files", a.user.ID)
		file_mkdir(data_dir + "/" + dir)
		path := id + "_" + file_name_safe(file.Filename)
		a.web.SaveUploadedFile(file, data_dir+"/"+dir+"/"+path)
		size := file_size(dir + "/" + path)

		db.exec("replace into files ( id, name, path, size, rank, updated ) values ( ?, ?, ?, ?, ?, ? )", id, file.Filename, path, size, i+1, updated)
		results = append(results, File{ID: id, Name: file.Filename, Path: path, Size: size, Rank: i + 1, Updated: updated})
	}

	return results
}

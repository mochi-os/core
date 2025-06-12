// Comms: Files app
// Copyright Alistair Cunningham 2025

package main

import (
	"fmt"
	"net/http"
)

type File struct {
	ID      string
	Name    string
	Path    string `json:"-"`
	Rank    int
	Updated int64 `json:"-"`
}

func init() {
	a := app("files")
	a.home("files", map[string]string{"en": "Files"})
	a.db("files.db", files_db_create)

	a.path("files", files_list)
	a.path("files/create", files_create)
	a.path("files/:entity", files_view)

	a.service("files")
	a.event("get", files_get_event)
}

// Create app database
func files_db_create(db *DB) {
	db.exec("create table settings ( name text not null primary key, value text not null )")
	db.exec("replace into settings ( name, value ) values ( 'schema', 1 )")

	db.exec("create table files ( id text not null primary key, name text not null, path text not null, rank integer not null default 1, updated integer not null )")
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
}

// List existing files
func files_list(a *Action) {
	var fs []File
	a.db.scans(&fs, "select * from files order by name")
	a.template("files/list", Map{"Files": fs})
}

// View a file
func files_view(a *Action) {
	var f File
	if !a.db.scan(&f, "select * from files where id=?", a.input("entity")) {
		a.error(404, "File not found")
		return
	}

	file := fmt.Sprintf("users/%d/files/data/%s", a.user.ID, f.Path)
	if !file_exists(file) {
		a.error(500, "File data not found")
		return
	}
	data := file_read(file)

	a.web.Header("Content-Disposition", "attachment; filename="+file_safe_name(f.Name))
	a.web.Data(http.StatusOK, "application/octet-stream", data)
}

// Upload one or more files for an action
func (a *Action) upload(name string) []File {
	updated := now()
	db := db_user(a.user, "files/files.db", files_db_create)
	defer db.close()

	var results []File
	form, err := a.web.MultipartForm()
	check(err)

	for i, file := range form.File[name] {
		id := uid()
		dir := fmt.Sprintf("%s/users/%d/files/data", data_dir, a.user.ID)
		file_mkdir(dir)
		path := id + "_" + file_safe_name(file.Filename)
		a.web.SaveUploadedFile(file, dir+"/"+path)
		db.exec("replace into files ( id, name, path, rank, updated ) values ( ?, ?, ?, ?, ? )", id, file.Filename, path, i+1, updated)
		results = append(results, File{ID: id, Name: file.Filename, Path: path, Rank: i + 1, Updated: updated})
	}

	return results
}

// Comms server: Directory
// Copyright Alistair Cunningham 2024

package main

type Directory struct {
	ID          string
	Fingerprint string
	Name        string
	Class       string
	Location    string
	Updated     int
}

func directory_create(id string, name string, class string, location string) {
	db_exec("directory", "insert into directory ( id, fingerprint, name, class, location, updated ) values ( ?, ?, ?, ?, ?, ? )", id, fingerprint(id), name, class, location, time_unix())
}

func directory_delete(id string) {
	db_exec("directory", "delete from directory where id=?", id)
}

func directory_search(search string) *[]Directory {
	var d []Directory
	db_structs(&d, "directory", "select * from directory where name like ? order by name", "%"+search+"%")
	return &d
}

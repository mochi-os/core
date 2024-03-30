// Comms server: Directory
// Copyright Alistair Cunningham 2024

package main

import (
	"encoding/json"
)

type Directory struct {
	ID          string `json:"id"`
	Fingerprint string
	Name        string `json:"name"`
	Class       string `json:"class"`
	Location    string `json:"location"`
	Updated     int
}

func init() {
	app_register("directory", map[string]string{"en": "Directory"})
	//TODO Directory update event
	app_register_pubsub("directory", "directory")
}

func directory_create(id string, name string, class string, location string) {
	db_exec("directory", "insert into directory ( id, fingerprint, name, class, location, updated ) values ( ?, ?, ?, ?, ?, ? )", id, fingerprint(id), name, class, location, time_unix())
	d := Directory{ID: id, Name: name, Class: class, Location: location}
	j, err := json.Marshal(d)
	fatal(err)
	libp2p_topics["directory"].Publish(libp2p_context, j)
}

func directory_delete(id string) {
	db_exec("directory", "delete from directory where id=?", id)
}

func directory_search(search string) *[]Directory {
	var d []Directory
	db_structs(&d, "directory", "select * from directory where name like ? order by name", "%"+search+"%")
	return &d
}

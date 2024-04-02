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
	app_register_event("directory", "publish", directory_event)
	app_register_pubsub("directory", "directory", nil)
}

func directory_create(u *User, location string) {
	db_exec("directory", "replace into directory ( id, fingerprint, name, class, location, updated ) values ( ?, ?, ?, 'person', ?, ? )", u.Public, fingerprint(u.Public), u.Name, location, time_unix())
	jd, err := json.Marshal(map[string]string{"id": u.Public, "name": u.Name, "class": "person", "location": location})
	fatal(err)
	e := event(u, "", "directory", "", "publish", string(jd))
	je, err := json.Marshal(e)
	fatal(err)
	libp2p_topics["directory"].Publish(libp2p_context, je)

	//TODO Check queue for events to this user
}

func directory_delete(id string) {
	db_exec("directory", "delete from directory where id=?", id)
}

func directory_event(u *User, e *Event) {
	log_debug("Received directory event '%#v'", e)
	var d Directory
	err := json.Unmarshal([]byte(e.Content), &d)
	if err != nil {
		log_info("Dropping directory event with malformed directory JSON '%s': %s", e.Content, err.Error())
		return
	}
	if d.ID != e.From {
		log_info("Dropping directory event with incorrect ID: '%s'!='%s'", d.ID, e.From)
		return
	}

	db_exec("directory", "replace into directory ( id, fingerprint, name, class, location, updated ) values ( ?, ?, ?, ?, ?, ? )", d.ID, fingerprint(d.ID), d.Name, d.Class, d.Location, time_unix())

	//TODO Check queue for events to this user
}

func directory_search(search string) *[]Directory {
	var d []Directory
	db_structs(&d, "directory", "select * from directory where name like ? order by name", "%"+search+"%")
	return &d
}

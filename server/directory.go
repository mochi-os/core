// Mochi server: Directory
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"time"
)

type Directory struct {
	ID          string
	Fingerprint string
	Name        string
	Class       string
	Location    string
	Data        string
	Created     int64
	Updated     int64
}

func init() {
	a := app("directory")
	a.service("directory")
	a.event_broadcast("download", directory_download_event)
	a.event_broadcast("request", directory_request_event)
	a.event_broadcast("publish", directory_publish_event)
}

// Get a directory entry
func directory_by_id(id string) *Directory {
	db := db_open("db/directory.db")
	var d Directory
	if db.scan(&d, "select * from directory where id=?", id) {
		return &d
	}
	return nil
}

// Create a new directory entry for a local entity
func directory_create(e *Entity) {
	debug("Directory creating entry '%s' (%s)", e.ID, e.Name)
	now := now()

	db := db_open("db/directory.db")
	db.exec("replace into directory ( id, fingerprint, name, class, location, data, created, updated ) values ( ?, ?, ?, ?, ?, ?, ?, ? )", e.ID, e.Fingerprint, e.Name, e.Class, p2p_id, e.Data, now, now)
}

// Delete a directory entry
func directory_delete(id string) {
	db := db_open("db/directory.db")
	db.exec("delete from directory where id=?", id)
}

// Ask known peers to send us a full copy of the directory, after a short delay to give time to connect to them
// TODO Test directory download
func directory_download() {
	time.Sleep(10 * time.Second)
	for _, p := range peers_bootstrap {
		if p.ID != p2p_id {
			debug("Directory requesting download from peer '%s'", p.ID)
			ev := event("", p.ID, "directory", "download")
			ev.send()
		}
	}
}

// Reply to a directory download request
func directory_download_event(e *Event) {
	debug("Directory received download event '%#v'", e)
	time.Sleep(time.Second)

	var results []Directory
	db := db_open("db/directory.db")
	db.scans(&results, "select * from directory order by id")
	for _, d := range results {
		ev := event(e.To, e.From, "directory", "publish")
		ev.add(d)
		ev.send()
		time.Sleep(time.Millisecond)
	}
}

// Publish a directory entry to the entire network
func directory_publish(e *Entity, allow_queue bool) {
	ev := event(e.ID, "", "directory", "publish")
	ev.set("id", e.ID, "name", e.Name, "class", e.Class, "location", "p2p/"+p2p_id, "data", e.Data)
	ev.publish(allow_queue)
}

// Received a directory publish event from another server
func directory_publish_event(e *Event) {
	debug("Directory received publish event '%#v'", e)
	now := now()

	//TODO Validate fields
	id := e.get("id", "")
	name := e.get("name", "")
	class := e.get("class", "")
	location := e.get("location", "")
	data := e.get("data", "")

	if e.From == "" {
		found := false
		for _, p := range peers_bootstrap {
			if e.p2p_peer == p.ID {
				found = true
				break
			}
		}
		if !found {
			info("Directory dropping anonymous event from untrusted peer")
			return
		}

	} else if e.From != id {
		info("Directory dropping event from incorrect sender: '%s'!='%s'", id, e.From)
		return
	}

	db := db_open("db/directory.db")
	db.exec("replace into directory ( id, fingerprint, name, class, location, data, created, updated ) values ( ?, ?, ?, ?, ?, ?, ?, ? )", id, fingerprint(id), name, class, location, data, now, now)

	go queue_check_entity(id)
}

// Request that another server publish a directory event
// TODO Test directory publish request
func directory_request(id string) {
	ev := event("", "", "directory", "request")
	ev.set("id", id)
	ev.publish(false)
}

// Reply to a directory request if we have the requested entity
func directory_request_event(e *Event) {
	debug("Directory received request event '%#v'", e)

	var r Entity
	db := db_open("db/users.db")
	if db.scan(&r, "select * from entities where id=?", e.get("id", "")) {
		directory_publish(&r, false)
	}
}

// Search the directory
func directory_search(u *User, class string, search string, include_self bool) *[]Directory {
	dbd := db_open("db/directory.db")
	var ds []Directory
	dbd.scans(&ds, "select * from directory where class=? and name like ? order by name, created", class, "%"+search+"%")

	for i, _ := range ds {
		ds[i].Fingerprint = fingerprint_hyphens(ds[i].Fingerprint)
	}

	if u == nil || include_self || class != "person" {
		return &ds
	}

	dbu := db_open("db/users.db")
	var es []Entity
	dbu.scans(&es, "select id from entities where user=?", u.ID)
	im := map[string]bool{}
	for _, e := range es {
		im[e.ID] = true
	}

	var o []Directory
	for _, d := range ds {
		_, found := im[d.ID]
		if !found {
			o = append(o, d)
		}
	}
	return &o
}

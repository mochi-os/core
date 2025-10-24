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
	a.event("request", directory_request_event)
	a.event_broadcast("download", directory_download_event)
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
	db.exec("replace into directory ( id, fingerprint, name, class, location, data, created, updated ) values ( ?, ?, ?, ?, ?, ?, ?, ? )", e.ID, e.Fingerprint, e.Name, e.Class, "p2p/"+p2p_id, e.Data, now, now)
}

// Delete a directory entry
func directory_delete(id string) {
	db := db_open("db/directory.db")
	db.exec("delete from directory where id=?", id)
}

// Ask known peers to send us any updates since the newest update in our copy of the directory
// TODO Test directory downloads once wasabi is running 0.2
func directory_download() {
	time.Sleep(3 * time.Second)
	for _, p := range peers_bootstrap {
		debug("Directory downloading from peer '%s'", p.ID)

		s := peer_stream(p.ID)
		if s == nil {
			debug("Stream %d unable to open to peer '%s'", s.id, p.ID)
			continue
		}
		debug("Stream %d open to peer '%s': from '', to '', service 'directory', event 'download'", s.id, p.ID)

		err := s.write(Headers{Service: "directory", Event: "download"})
		if err != nil {
			continue
		}

		start := int64(0)
		var u Directory
		db := db_open("db/directory.db")
		if db.scan(&u, "select updated from directory order by updated desc limit 1") {
			start = u.Updated
		}
		debug("Directory asking for directory updates since %d", start)
		s.write_content("start", i64toa(start))

		for {
			var d Directory
			debug("Directory reading update")
			err := s.read(&d)
			if err != nil {
				debug("Directory read error: %v", err)
				return
			}

			debug("Directory got update %#v", d)
			db.exec("replace into directory ( id, fingerprint, name, class, location, data, created, updated ) values ( ?, ?, ?, ?, ?, ?, ?, ? )", d.ID, fingerprint(d.ID), d.Name, d.Class, d.Location, d.Data, d.Created, d.Updated)
			go queue_check_entity(d.ID)
		}
	}

	debug("Directory download finished")
}

// Reply to a directory download request
func directory_download_event(e *Event) {
	debug("Directory received download request")

	start := atoi(e.content["start"], 0)
	debug("Directory sending updates since %d", start)

	var results []Directory
	db := db_open("db/directory.db")
	db.scans(&results, "select * from directory where updated>=? order by created, id", start)
	for _, d := range results {
		debug("Directory sending update %#v", d)
		e.stream.write(d)
	}

	debug("Directory finished sending updates")
}

// Publish a directory entry to the entire network
func directory_publish(e *Entity, allow_queue bool) {
	m := message(e.ID, "", "directory", "publish")
	m.set("id", e.ID, "name", e.Name, "class", e.Class, "location", "p2p/"+p2p_id, "data", e.Data)
	m.publish(allow_queue)
}

// Received a directory publish event from another server
func directory_publish_event(e *Event) {
	debug("Directory received publish event '%+v', content '%+v'", e, e.content)
	now := now()

	id := e.get("id", "")
	if !valid(id, "entity") {
		debug("Directory dropping event with invalid entity id '%s'", id)
		return
	}

	name := e.get("name", "")
	if !valid(name, "line") {
		debug("Directory dropping event with invalid name '%s'", name)
		return
	}

	class := e.get("class", "")
	if !valid(class, "constant") {
		debug("Directory dropping event with invalid class '%s'", class)
		return
	}

	location := e.get("location", "")
	if !valid(location, "line") {
		debug("Directory dropping event with invalid location '%s'", location)
		return
	}

	data := e.get("data", "")
	if !valid(data, "text") {
		debug("Directory dropping event with invalid data '%s'", data)
		return
	}

	created := now

	if e.from == "" {
		found := false
		for _, p := range peers_bootstrap {
			if e.peer == p.ID {
				found = true
				break
			}
		}
		if !found {
			info("Directory dropping anonymous event from untrusted peer")
			return
		}

		created = atoi(e.get("created", ""), created)

	} else if e.from != id {
		info("Directory dropping event from incorrect sender: '%s'!='%s'", id, e.from)
		return
	}

	db := db_open("db/directory.db")
	db.exec("replace into directory ( id, fingerprint, name, class, location, data, created, updated ) values ( ?, ?, ?, ?, ?, ?, ?, ? )", id, fingerprint(id), name, class, location, data, created, now)

	go queue_check_entity(id)
}

// Reply to a directory request if we have the requested entity
func directory_request_event(e *Event) {
	debug("Directory received request event '%+v'", e)

	var r Entity
	db := db_open("db/users.db")
	if db.scan(&r, "select * from entities where id=?", e.to) {
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

// Comms server: Directory
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"time"
)

type Directory struct {
	ID          string `json:"id"`
	Fingerprint string `json:"fingerprint"`
	Name        string `json:"name"`
	Class       string `json:"class"`
	Location    string `json:"location"`
	Data        string `json:"data"`
	Created     int64  `json:"created"`
	Updated     int64  `json:"updated"`
}

func init() {
	a := app("directory")
	a.service("directory")
	a.event_broadcast("download", directory_download_event)
	a.event_broadcast("request", directory_request_event)
	a.event_broadcast("publish", directory_publish_event)
	a.pubsub("directory", nil)
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
	log_debug("Creating directory entry '%s' (%s)", e.ID, e.Name)
	now := now()

	db := db_open("db/directory.db")
	db.exec("replace into directory ( id, fingerprint, name, class, location, data, created, updated ) values ( ?, ?, ?, ?, ?, ?, ?, ? )", e.ID, e.Fingerprint, e.Name, e.Class, libp2p_id, e.Data, now, now)
	go events_check_queue("entity", e.ID)
}

// Delete a directory entry
func directory_delete(id string) {
	db := db_open("db/directory.db")
	db.exec("delete from directory where id=?", id)
}

// Ask known peers to send us a full copy of the directory, after a short delay to give time to connect to them
func directory_download() {
	time.Sleep(10 * time.Second)
	j := json_encode(Event{ID: uid(), Service: "directory", Action: "download"})
	for peer, _ := range peers_known {
		if peer != libp2p_id {
			log_debug("Requesting directory download from peer '%s'", peer)
			peer_send(peer, j)
		}
	}
}

// Reply to a directory download request
func directory_download_event(e *Event) {
	log_debug("Received directory download event '%#v'", e)
	time.Sleep(time.Second)

	var results []Directory
	db := db_open("db/directory.db")
	db.scans(&results, "select * from directory order by id")
	for _, d := range results {
		peer_send(e.source, json_encode(Event{ID: uid(), Service: "directory", Action: "publish", Content: json_encode(d)}))
		time.Sleep(time.Millisecond)
	}
}

// Publish a directory entry to the entire network
func directory_publish(e *Entity, allow_queue bool) {
	// Send to libp2p broadcast
	ev := Event{ID: uid(), From: e.ID, Service: "directory", Action: "publish", Content: json_encode(map[string]string{"id": e.ID, "name": e.Name, "class": e.Class, "location": libp2p_id, "data": e.Data})}
	ev.sign()
	j := []byte(json_encode(ev))
	if len(peers_connected) >= peers_minimum {
		libp2p_topics["directory"].Publish(libp2p_context, j)
	} else if allow_queue {
		db := db_open("db/queue.db")
		db.exec("replace into broadcast ( id, topic, content, updated ) values ( ?, 'directory', ?, ? )", ev.ID, j, now())
	}
}

// Received a directory publish event from another server
func directory_publish_event(e *Event) {
	log_debug("Received directory publish event '%#v'", e)
	now := now()

	var d Directory
	if !json_decode(&d, e.Content) {
		log_info("Dropping directory event '%s' with malformed JSON", e.Content)
		return
	}

	if e.From == "" {
		found := false
		for peer, _ := range peers_known {
			if e.source == peer {
				found = true
				break
			}
		}
		if !found {
			log_info("Dropping anonymous directory event from untrusted peer")
			return
		}

	} else if e.From != d.ID {
		log_info("Dropping directory event from incorrect sender: '%s'!='%s'", d.ID, e.From)
		return
	}

	db := db_open("db/directory.db")
	db.exec("replace into directory ( id, fingerprint, name, class, location, data, created, updated ) values ( ?, ?, ?, ?, ?, ?, ?, ? )", d.ID, fingerprint(d.ID), d.Name, d.Class, d.Location, d.Data, now, now)

	go events_check_queue("entity", d.ID)
}

// Request that another server publish a directory event
func directory_request(id string) {
	e := Event{ID: uid(), Service: "directory", Action: "request", Content: id}
	libp2p_topics["directory"].Publish(libp2p_context, []byte(json_encode(e)))
}

// Reply to a directory request if we have the requested entity
func directory_request_event(e *Event) {
	log_debug("Received directory request event '%#v'", e)
	var r Entity
	db := db_open("db/users.db")
	if db.scan(&r, "select * from entities where id=?", e.Content) {
		directory_publish(&r, true)
	}
}

// Search the directory
func directory_search(u *User, class string, search string, include_self bool) *[]Directory {
	dbd := db_open("db/directory.db")
	var ds []Directory
	dbd.scans(&ds, "select * from directory where class=? and name like ? order by name, created", class, "%"+search+"%")

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

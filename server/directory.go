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
	Updated     int    `json:"updated"`
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

// Create a new directory entry for a local identity
func directory_create(i *Identity) {
	log_debug("Creating directory entry '%s' (%s)", i.ID, i.Name)
	db := db_open("db/directory.db")
	db.exec("replace into directory ( id, fingerprint, name, class, location, data, updated ) values ( ?, ?, ?, ?, ?, ?, ? )", i.ID, i.Fingerprint, i.Name, i.Class, libp2p_id, i.Data, now())
	go events_check_queue("identity", i.ID)
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

// Publish a directory entry on the libp2p pubsub
func directory_publish(i *Identity) {
	log_debug("Publishing identity '%s' (%s) to pubsub", i.ID, i.Name)
	e := Event{ID: uid(), From: i.ID, Service: "directory", Action: "publish", Content: json_encode(map[string]string{"id": i.ID, "name": i.Name, "class": i.Class, "location": libp2p_id, "data": i.Data})}
	e.sign()
	//TODO Queue publish for later if we're not connected to any/enough peers
	libp2p_topics["directory"].Publish(libp2p_context, []byte(json_encode(e)))
}

// Received a directory publish event from another server
func directory_publish_event(e *Event) {
	log_debug("Received directory publish event '%#v'", e)
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
	db.exec("replace into directory ( id, fingerprint, name, class, location, data, updated ) values ( ?, ?, ?, ?, ?, ?, ? )", d.ID, fingerprint(d.ID), d.Name, d.Class, d.Location, d.Data, now())

	go events_check_queue("identity", d.ID)
}

// Request that another server publish a directory event
func directory_request(id string) {
	e := Event{ID: uid(), Service: "directory", Action: "request", Content: id}
	libp2p_topics["directory"].Publish(libp2p_context, []byte(json_encode(e)))
}

// Reply to a directory request if we have the requested identity
func directory_request_event(e *Event) {
	log_debug("Received directory request event '%#v'", e)
	var r Identity
	db := db_open("db/users.db")
	if db.scan(&r, "select * from identities where id=?", e.Content) {
		directory_publish(&r)
	}
}

// Search the directory
func directory_search(u *User, class string, search string, include_self bool) *[]Directory {
	dbd := db_open("db/directory.db")
	var ds []Directory
	dbd.scans(&ds, "select * from directory where class=? and name like ? order by name", class, "%"+search+"%")

	if u == nil || include_self || class != "person" {
		return &ds
	}

	dbu := db_open("db/users.db")
	var is []Identity
	dbu.scans(&is, "select id from identities where user=?", u.ID)
	im := map[string]bool{}
	for _, i := range is {
		im[i.ID] = true
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

// Comms server: Directory
// Copyright Alistair Cunningham 2024

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
	Updated     int    `json:"updated"`
}

func init() {
	a := register_app("directory")
	a.register_event("download", directory_event_download)
	a.register_event("request", directory_event_request)
	a.register_event("publish", directory_event_publish)
	a.register_pubsub("directory", nil)
}

// Create a new directory entry for a local user
func directory_create(u *User) {
	db := db_open("db/directory.db")
	db.exec("replace into directory ( id, fingerprint, name, class, location, updated ) values ( ?, ?, ?, 'person', ?, ? )", u.Public, fingerprint(u.Public), u.Name, libp2p_id, time_unix())
	go events_check_queue("user", u.Public)
}

// Delete a directory entry
func directory_delete(id string) {
	db := db_open("db/directory.db")
	db.exec("delete from directory where id=?", id)
}

// Ask known peers to send us a full copy of the directory, after a short delay to give time to connect to them
func directory_download() {
	time.Sleep(10 * time.Second)
	j := json_encode(event(nil, "", "directory", libp2p_id, "download", ""))
	for peer, _ := range peers_known {
		if peer != libp2p_id {
			log_debug("Requesting directory download from peer '%s'", peer)
			peer_send(peer, j)
		}
	}
}

// Reply to a directory download request
func directory_event_download(u *User, e *Event) {
	log_debug("Received directory download event '%#v'", e)
	time.Sleep(time.Second)

	var results []Directory
	db := db_open("db/directory.db")
	db.scans(&results, "select * from directory order by id")
	for _, d := range results {
		peer_send(e.Source, json_encode(event(u, "", "directory", "", "publish", json_encode(d))))
		time.Sleep(time.Millisecond)
	}
}

// Reply to a directory request if we have the requested user
func directory_event_request(u *User, e *Event) {
	log_debug("Received directory request event '%#v'", e)
	var r User
	db := db_open("db/users.db")
	if db.scan(&r, "select * from users where public=?", e.Content) {
		directory_publish(&r)
	}
}

// Received a directory publish event from another server
func directory_event_publish(u *User, e *Event) {
	log_debug("Received directory publish event '%#v'", e)
	var d Directory
	if !json_decode([]byte(e.Content), &d) {
		log_info("Dropping directory event '%s' with malformed JSON", e.Content)
		return
	}
	if e.From == "" {
		found := false
		for peer, _ := range peers_known {
			if e.Source == peer {
				found = true
				break
			}
		}
		if !found {
			log_info("Dropping unsigned directory event from untrusted peer")
			return
		}
	} else if e.From != d.ID {
		log_info("Dropping directory event from incorrect sender: '%s'!='%s'", d.ID, e.From)
		return
	}

	db := db_open("db/directory.db")
	db.exec("replace into directory ( id, fingerprint, name, class, location, updated ) values ( ?, ?, ?, ?, ?, ? )", d.ID, fingerprint(d.ID), d.Name, d.Class, d.Location, time_unix())

	go events_check_queue("user", d.ID)
}

// Publish a directory entry on the libp2p pubsub
func directory_publish(u *User) {
	libp2p_topics["directory"].Publish(libp2p_context, []byte(json_encode(event(u, "", "directory", "", "publish", json_encode(map[string]string{"id": u.Public, "name": u.Name, "class": "person", "location": libp2p_id})))))
}

// Request that another server publish a directory event
func directory_request(user string) {
	libp2p_topics["directory"].Publish(libp2p_context, []byte(json_encode(event(nil, "", "directory", "", "request", user))))
}

// Search the directory
func directory_search(u *User, search string, include_self bool) *[]Directory {
	var d []Directory
	db := db_open("db/directory.db")
	if u == nil || include_self {
		db.scans(&d, "select * from directory where name like ? order by name", "%"+search+"%")
	} else {
		db.scans(&d, "select * from directory where name like ? and id!=? order by name", "%"+search+"%", u.Public)
	}
	return &d
}

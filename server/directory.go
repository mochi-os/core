// Comms server: Directory
// Copyright Alistair Cunningham 2024

package main

import (
	"encoding/json"
	"time"
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
	app_register_event("directory", "download", directory_event_download)
	app_register_event("directory", "request", directory_event_request)
	app_register_event("directory", "publish", directory_event_publish)
	app_register_pubsub("directory", "directory", nil)
}

// Create a new directory entry for a local user
func directory_create(u *User) {
	db_exec("directory", "replace into directory ( id, fingerprint, name, class, location, updated ) values ( ?, ?, ?, 'person', ?, ? )", u.Public, fingerprint(u.Public), u.Name, libp2p_id, time_unix())
	go events_check_queue("user", u.Public)
}

// Delete a directory entry
func directory_delete(id string) {
	db_exec("directory", "delete from directory where id=?", id)
}

// Ask known peers to send us a full copy of the directory, after a short delay to give time to connect to peers
func directory_download() {
	time.Sleep(10 * time.Second)
	log_debug("Requesting directory download")
	j, err := json.Marshal(event(nil, "", "directory", libp2p_id, "download", ""))
	check(err)
	libp2p_topics["directory"].Publish(libp2p_context, j)
}

// Reply to a directory download request, but only from immediate peers
func directory_event_download(u *User, e *Event) {
	log_debug("Received directory download event '%#v'", e)
	if e.Entity != e.Source {
		return
	}
	log_debug("Directory download request is from immediate peer; sending directory entries")
	time.Sleep(time.Second)

	var results []Directory
	db_structs(&results, "directory", "select * from directory order by id")
	for _, d := range results {
		jd, err := json.Marshal(d)
		check(err)
		je, err := json.Marshal(event(u, "", "directory", "", "publish", string(jd)))
		check(err)
		peer_send(e.Source, je)
		time.Sleep(time.Millisecond)
	}
}

// Reply to a directory request if we have the requested user
func directory_event_request(u *User, e *Event) {
	log_debug("Received directory request event '%#v'", e)
	var r User
	if db_struct(&r, "users", "select * from users where public=?", e.Content) {
		directory_publish(&r)
	}
}

// Received a directory publish event from another server
func directory_event_publish(u *User, e *Event) {
	log_debug("Received directory publish event '%#v'", e)
	var d Directory
	err := json.Unmarshal([]byte(e.Content), &d)
	if err != nil {
		log_info("Dropping directory event with malformed directory JSON '%s': %s", e.Content, err.Error())
		return
	}
	if e.From == "" {
		found := false
		for _, trusted := range peers_trusted {
			if e.Source == trusted {
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

	db_exec("directory", "replace into directory ( id, fingerprint, name, class, location, updated ) values ( ?, ?, ?, ?, ?, ? )", d.ID, fingerprint(d.ID), d.Name, d.Class, d.Location, time_unix())

	go events_check_queue("user", d.ID)
}

// Publish a directory entry on the libp2p pubsub
func directory_publish(u *User) {
	jd, err := json.Marshal(map[string]string{"id": u.Public, "name": u.Name, "class": "person", "location": libp2p_id})
	check(err)
	je, err := json.Marshal(event(u, "", "directory", "", "publish", string(jd)))
	check(err)
	libp2p_topics["directory"].Publish(libp2p_context, je)
}

// Request that another server publish a directory event
func directory_request(user string) {
	log_debug("Requesting directory user '%s' via pubsub", user)
	j, err := json.Marshal(event(nil, "", "directory", "", "request", user))
	check(err)
	libp2p_topics["directory"].Publish(libp2p_context, j)
}

// Search the directory
func directory_search(search string) *[]Directory {
	var d []Directory
	db_structs(&d, "directory", "select * from directory where name like ? order by name", "%"+search+"%")
	return &d
}

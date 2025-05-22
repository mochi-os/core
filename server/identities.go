// Comms server: Identities
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"time"
)

type Identity struct {
	ID          string `json:"id"`
	Private     string `json:"-"`
	Fingerprint string `json:"-"`
	User        int    `json:"-"`
	Parent      string `json:"-"`
	Class       string `json:"class"`
	Name        string `json:"name"`
	Privacy     string `json:"-"`
	Data        string `json:"data"`
	Published   int64  `json:"-"`
}

func identity_by_fingerprint(fingerprint string) *Identity {
	db := db_open("db/users.db")
	var i Identity
	if !db.scan(&i, "select * from identities where fingerprint=?", fingerprint) {
		return nil
	}
	return &i
}

func identity_by_id(id string) *Identity {
	db := db_open("db/users.db")
	var i Identity
	if !db.scan(&i, "select * from identities where id=?", id) {
		return nil
	}
	return &i
}

func identity_by_user_id(u *User, id string) *Identity {
	db := db_open("db/users.db")
	var i Identity
	if !db.scan(&i, "select * from identities where id=? and user=?", id, u.ID) {
		return nil
	}
	return &i
}

func identity_create(u *User, class string, name string, privacy string, data string) (*Identity, error) {
	db := db_open("db/users.db")
	if !valid(name, "name") {
		return nil, error_message("Invalid name")
	}
	if !db.exists("select id from users where id=?", u.ID) {
		return nil, error_message("User not found")
	}
	if !valid(class, "constant") {
		return nil, error_message("Invalid class")
	}
	if !valid(privacy, "privacy") {
		return nil, error_message("Invalid privacy")
	}

	parent := ""
	if u.Identity != nil {
		parent = u.Identity.ID
	}

	for j := 0; j < 1000; j++ {
		public, private, err := ed25519.GenerateKey(rand.Reader)
		check(err)
		id := base58_encode(public)
		fingerprint := fingerprint(string(public))
		if !db.exists("select id from identities where id=? or fingerprint=?", id, fingerprint) {
			db.exec("replace into identities ( id, private, fingerprint, user, parent, class, name, privacy, data, published ) values ( ?, ?, ?, ?, ?, ?, ?, ?, ?, 0 )", id, base58_encode(private), fingerprint, u.ID, parent, class, name, privacy, data)
			i := Identity{ID: id, Fingerprint: fingerprint, User: u.ID, Parent: parent, Class: class, Name: name, Privacy: privacy, Data: data, Published: 0}
			if privacy == "public" {
				directory_create(&i)
				directory_publish(&i)
			}
			return &i, nil
		}
		log_debug("Identity '%s', fingerprint '%s' already in use. Trying another...", id, fingerprint)
	}

	return nil, error_message("Unable to find spare entity ID or fingerprint")
}

func identity_location(id string) (string, string, string, string) {
	// Check if local
	var i Identity
	dbu := db_open("db/users.db")
	if dbu.scan(&i, "select * from identities where id=?", id) {
		return "local", id, "", ""
	}

	// Check in directory
	var d Directory
	dbd := db_open("db/directory.db")
	if dbd.scan(&d, "select location from directory where id=?", id) {
		address := peer_address(d.Location)
		if address != "" {
			return "libp2p", address, "peer", d.Location
		}
		peer_request(d.Location)
		return "peer", d.Location, "peer", d.Location
	}

	directory_request(id)
	return "entity", id, "entity", id
}

// Re-publish all our identities every 30 days so the network knows they're still active
func identities_manager() {
	db := db_open("db/users.db")

	for {
		time.Sleep(time.Minute)
		var identities []Identity
		db.scans(&identities, "select * from identities where privacy='public' and published<?", now()-30*86400)
		for _, i := range identities {
			db.exec("update identities set published=? where id=?", now(), i.ID)
			directory_publish(&i)
		}
	}
}

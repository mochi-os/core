// Comms server: Identities
// Copyright Alistair Cunningham 2024

package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"time"
)

type Identity struct {
	ID          string
	Private     string
	Fingerprint string
	User        int
	Class       string
	Name        string
	Privacy     string
	Published   int64
}

func identity_by_id(id string) *Identity {
	db := db_open("db/users.db")
	var i Identity
	if !db.scan(&i, "select * from identities where id=?", id) {
		return nil
	}
	return &i
}

func identity_create(u *User, class string, name string, privacy string) (*Identity, error) {
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

	for j := 0; j < 1000; j++ {
		public, private, err := ed25519.GenerateKey(rand.Reader)
		check(err)
		id := base64_encode(public)
		fingerprint := fingerprint(string(public))
		if !db.exists("select id from identities where id=? or fingerprint=?", id, fingerprint) {
			db.exec("replace into identities ( id, private, fingerprint, user, class, name, privacy, published ) values ( ?, ?, ?, ?, ?, ?, ?, 0 )", id, base64_encode(private), fingerprint, u.ID, class, name, privacy)
			i := Identity{ID: id, Fingerprint: fingerprint, User: u.ID, Class: class, Name: name, Privacy: privacy, Published: 0}
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
		return i.Class, i.ID, "", ""
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
		db.scans(&identities, "select * from identities where privacy='public' and published<?", time_unix()-30*86400)
		for _, i := range identities {
			db.exec("update identities set published=? where id=?", time_unix(), i.ID)
			directory_publish(&i)
		}
	}
}

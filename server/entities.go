// Mochi server: Entities
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	sl "go.starlark.net/starlark"
	"strings"
	"time"
)

type Entity struct {
	ID          string `cbor:"id" json:"id"`
	Private     string `cbor:"-" json:"-"`
	Fingerprint string `cbor:"-" json:"fingerprint"`
	User        int    `cbor:"-" json:"-"`
	Parent      string `cbor:"-" json:"-"`
	Class       string `cbor:"class,omitempty" json:"class"`
	Name        string `cbor:"name,omitempty" json:"name"`
	Privacy     string `cbor:"-" json:"privacy"`
	Data        string `cbor:"data,omitempty" json:"data"`
	Published   int64  `cbor:"-" json:"-"`
}

// Get an entity by fingerprint
func entity_by_fingerprint(in string) *Entity {
	db := db_open("db/users.db")
	var e Entity
	if db.scan(&e, "select * from entities where fingerprint=?", in) {
		return &e
	}
	return nil
}

// Get an entity by id
func entity_by_id(id string) *Entity {
	db := db_open("db/users.db")
	var e Entity
	if db.scan(&e, "select * from entities where id=?", id) {
		return &e
	}
	return nil
}

// Get an entity for a user
func entity_by_user_id(u *User, id string) *Entity {
	db := db_open("db/users.db")
	var e Entity
	if db.scan(&e, "select * from entities where id=? and user=?", id, u.ID) {
		return &e
	}
	return nil
}

// Create a new entity in the database
func entity_create(u *User, class string, name string, privacy string, data string) (*Entity, error) {
	db := db_open("db/users.db")
	if !valid(name, "name") {
		return nil, fmt.Errorf("Invalid name")
	}
	if !db.exists("select id from users where id=?", u.ID) {
		return nil, fmt.Errorf("User not found")
	}
	if !valid(class, "constant") {
		return nil, fmt.Errorf("Invalid class")
	}
	if !valid(privacy, "privacy") {
		return nil, fmt.Errorf("Invalid privacy")
	}

	parent := ""
	if u.Identity != nil {
		parent = u.Identity.ID
	}

	public, private, fingerprint := entity_id()
	if public == "" {
		return nil, fmt.Errorf("Unable to find spare entity ID or fingerprint")
	}

	db.exec("replace into entities ( id, private, fingerprint, user, parent, class, name, privacy, data, published ) values ( ?, ?, ?, ?, ?, ?, ?, ?, ?, 0 )", public, private, fingerprint, u.ID, parent, class, name, privacy, data)

	e := Entity{ID: public, Fingerprint: fingerprint, User: u.ID, Parent: parent, Class: class, Name: name, Privacy: privacy, Data: data, Published: 0}

	if privacy == "public" {
		directory_create(&e)
		directory_publish(&e, true)
	}

	return &e, nil
}

// Get a public/private key pair for a new entity
func entity_id() (string, string, string) {
	db := db_open("db/users.db")

	for j := 0; j < 10000; j++ {
		public, private, err := ed25519.GenerateKey(rand.Reader)
		check(err)
		id := base58_encode(public)
		fingerprint := fingerprint(id)
		if !db.exists("select id from entities where id=? or fingerprint=?", id, fingerprint) {
			return id, base58_encode(private), fingerprint
		}
		debug("Identity '%s', fingerprint '%s' already in use. Trying another...", id, fingerprint)
	}

	return "", "", ""
}

// Re-publish all our entities periodically so the network knows they're still active
// Increase this interval in future versions, especially once the directory gets recent updates
func entities_manager() {
	db := db_open("db/users.db")

	for {
		time.Sleep(time.Hour)
		if peers_sufficient() {
			var es []Entity
			db.scans(&es, "select * from entities where privacy='public' and published<?", now()-86400)
			for _, e := range es {
				db.exec("update entities set published=? where id=?", now(), e.ID)
				directory_publish(&e, false)
			}
		}
	}
}

// Get the peer an entity is at
func entity_peer(id string) string {
	// Check if local
	var e Entity
	if db_open("db/users.db").scan(&e, "select * from entities where id=?", id) {
		return p2p_id
	}

	// Check in directory
	var d Directory
	if db_open("db/directory.db").scan(&d, "select location from directory where id=?", id) {
		d.Location, _ = strings.CutPrefix(d.Location, "p2p/")
		return d.Location
	}

	// Not found. Send a directory request and return failure.
	message("", id, "directory", "request").publish(false)
	return ""
}

// Sign a string using an entity's private key
func entity_sign(entity string, s string) string {
	if entity == "" {
		return ""
	}

	db := db_open("db/users.db")
	var e Entity
	if !db.scan(&e, "select private from entities where id=?", entity) {
		warn("Signature entity '%s' not found", entity)
		return ""
	}

	private := base58_decode(e.Private, "")
	if string(private) == "" {
		warn("Signature entity '%s' empty private key", entity)
		return ""
	}

	return base58_encode(ed25519.Sign(private, []byte(s)))
}

// Starlark methods
func (e *Entity) AttrNames() []string {
	return []string{"id", "name"}
}

func (e *Entity) Attr(name string) (sl.Value, error) {
	switch name {
	case "id":
		return sl.String(e.ID), nil
	case "name":
		return sl.String(e.Name), nil
	default:
		return nil, nil
	}
}

func (e *Entity) Freeze() {}

func (e *Entity) Hash() (uint32, error) {
	return sl.String(e.ID).Hash()
}

func (e *Entity) String() string {
	return fmt.Sprintf("Entity %s", e.ID)
}

func (e *Entity) Truth() sl.Bool {
	return sl.True
}

func (e *Entity) Type() string {
	return "Entity"
}

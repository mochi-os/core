// Mochi server: Entities
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"strings"
	"time"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
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

var api_entity = sls.FromStringDict(sl.String("mochi.entity"), sl.StringDict{
	"create":      sl.NewBuiltin("mochi.entity.create", api_entity_create),
	"delete":      sl.NewBuiltin("mochi.entity.delete", api_entity_delete),
	"fingerprint": sl.NewBuiltin("mochi.entity.fingerprint", api_entity_fingerprint),
	"get":         sl.NewBuiltin("mochi.entity.get", api_entity_get),
	"info":        sl.NewBuiltin("mochi.entity.info", api_entity_info),
	"name":        sl.NewBuiltin("mochi.entity.name", api_entity_name),
	"owned":       sl.NewBuiltin("mochi.entity.owned", api_entity_owned),
	"privacy":     api_entity_privacy,
})

var api_entity_privacy = sls.FromStringDict(sl.String("mochi.entity.privacy"), sl.StringDict{
	"set": sl.NewBuiltin("mochi.entity.privacy.set", api_entity_privacy_set),
})

// Get an entity by id or fingerprint
func entity_by_any(s string) *Entity {
	db := db_open("db/users.db")
	var e Entity
	if db.scan(&e, "select * from entities where id=?", s) {
		return &e
	}
	if db.scan(&e, "select * from entities where fingerprint=?", s) {
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
	user_exists, _ := db.exists("select id from users where id=?", u.ID)
	if !user_exists {
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
		entity_exists, _ := db.exists("select id from entities where id=? or fingerprint=?", id, fingerprint)
		if !entity_exists {
			return id, base58_encode(private), fingerprint
		}
		debug("Identity %q, fingerprint %q already in use. Trying another...", id, fingerprint)
	}

	return "", "", ""
}

// Re-publish all our entities periodically so the network knows they're still active
func entities_manager() {
	db := db_open("db/users.db")

	for range time.Tick(time.Hour) {
		if peers_sufficient() {
			var es []Entity
			err := db.scans(&es, "select * from entities where privacy='public' and published<?", now()-86400)
			if err != nil {
				warn("Database error loading entities for republish: %v", err)
				continue
			}
			for _, e := range es {
				db.exec("update entities set published=? where id=?", now(), e.ID)
				directory_publish(&e, false)
			}
		}
	}
}

// Delete an entity: broadcast deletion to network, remove from directory and entities table
func (e *Entity) delete() {
	// Broadcast deletion (signs synchronously, dispatches async)
	m := message(e.ID, "", "directory", "delete")
	m.set("entity", e.ID)
	go m.publish(false)

	// Remove from local directory
	db_open("db/directory.db").exec("delete from directory where id=?", e.ID)

	// Remove entity
	db_open("db/users.db").exec("delete from entities where id=?", e.ID)
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
		warn("Signature entity %q not found", entity)
		return ""
	}

	private := base58_decode(e.Private, "")
	if string(private) == "" {
		warn("Signature entity %q empty private key", entity)
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

// mochi.entity.create(class, name, privacy, data?) -> string: Create a new entity, returns ID
func api_entity_create(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 3 || len(args) > 4 {
		return sl_error(fn, "syntax: <class: string>, <name: string>, <privacy: string>, [data: string]")
	}

	class, ok := sl.AsString(args[0])
	if !ok || !valid(class, "constant") {
		return sl_error(fn, "invalid class %q", class)
	}

	name, ok := sl.AsString(args[1])
	if !ok || !valid(name, "name") {
		return sl_error(fn, "invalid name %q", name)
	}

	privacy, ok := sl.AsString(args[2])
	if !ok || !valid(privacy, "^(private|public)$") {
		return sl_error(fn, "invalid privacy %q", privacy)
	}

	data := ""
	if len(args) > 3 {
		data, ok = sl.AsString(args[3])
		if !ok || !valid(data, "text") {
			return sl_error(fn, "invalid data %q", data)
		}
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	e, err := entity_create(user, class, name, privacy, data)
	if err != nil {
		return sl_error(fn, "unable to create entity: ", err)
	}

	return sl_encode(e.ID), nil
}

// mochi.entity.delete(id) -> bool: Delete an entity owned by the current user
func api_entity_delete(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <id: string>")
	}

	id, ok := sl.AsString(args[0])
	if !ok || !valid(id, "entity") {
		return sl_error(fn, "invalid id %q", id)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	// Verify entity exists and is owned by the current user
	db := db_open("db/users.db")
	var e Entity
	if !db.scan(&e, "select * from entities where id=?", id) {
		return sl_error(fn, "entity not found")
	}
	if e.User != user.ID {
		return sl_error(fn, "not authorized to delete this entity")
	}

	// Verify the calling app controls the entity's class
	app := t.Local("app").(*App)
	if app == nil {
		return sl_error(fn, "no app")
	}
	if e.Class != "" && apps_class_get(e.Class) != app.id {
		return sl_error(fn, "app does not control class %q", e.Class)
	}

	// Delete the entity
	e.delete()
	return sl.True, nil
}

// mochi.entity.fingerprint(id, hyphens?) -> string: Get the fingerprint of an entity
func api_entity_fingerprint(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return sl_error(fn, "syntax: <id: string>, [include hyphens: boolean]")
	}

	id, ok := sl.AsString(args[0])
	if !ok || !valid(id, "entity") {
		return sl_error(fn, "invalid id %q", id)
	}

	if len(args) > 1 && bool(args[1].Truth()) {
		return sl_encode(fingerprint_hyphens(fingerprint(id))), nil
	} else {
		return sl_encode(fingerprint(id)), nil
	}
}

// mochi.entity.get(id) -> list: Get an entity owned by the current user
func api_entity_get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <id: string>")
	}

	id, ok := sl.AsString(args[0])
	if !ok || !valid(id, "entity") {
		return sl_error(fn, "invalid id %q", id)
	}

	user, _ := t.Local("user").(*User)
	if user == nil {
		// No user means no entities owned by them
		return sl_encode([]any{}), nil
	}

	db := db_open("db/users.db")
	e, err := db.rows("select id, fingerprint, parent, class, name, data, published from entities where id=? and user=?", id, user.ID)
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}

	return sl_encode(e), nil
}

// mochi.entity.name(id) -> string or None: Get the name of any entity (local or directory)
func api_entity_name(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <id: string>")
	}

	id, ok := sl.AsString(args[0])
	if !ok || !valid(id, "entity") {
		return sl_error(fn, "invalid id %q", id)
	}

	// Check local entities first
	db := db_open("db/users.db")
	row, err := db.row("select name from entities where id=?", id)
	if err == nil && row != nil {
		if name, ok := row["name"].(string); ok {
			return sl.String(name), nil
		}
	}

	// Check directory
	db = db_open("db/directory.db")
	row, err = db.row("select name from directory where id=?", id)
	if err == nil && row != nil {
		if name, ok := row["name"].(string); ok {
			return sl.String(name), nil
		}
	}

	return sl.None, nil
}

// mochi.entity.info(id) -> dict or None: Get info for any local entity (no user restriction)
// Accepts either entity ID or fingerprint.
// Returns: id, fingerprint, parent, class, name, privacy, creator (owner's identity ID)
func api_entity_info(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <id: string>")
	}

	id, ok := sl.AsString(args[0])
	if !ok || !valid(id, "entity") {
		return sl_error(fn, "invalid id %q", id)
	}

	// Look up by ID or fingerprint
	e := entity_by_any(id)
	if e == nil {
		return sl.None, nil
	}

	row := map[string]any{
		"id":          e.ID,
		"fingerprint": e.Fingerprint,
		"parent":      e.Parent,
		"class":       e.Class,
		"name":        e.Name,
		"privacy":     e.Privacy,
	}

	// Resolve user ID to their identity (person entity)
	if e.User > 0 {
		db := db_open("db/users.db")
		identity, err := db.row("select id from entities where user=? and class='person' limit 1", e.User)
		if err == nil && identity != nil {
			if identityID, ok := identity["id"].(string); ok {
				row["creator"] = identityID
			}
		}
	}

	return sl_encode(row), nil
}

// mochi.entity.owned() -> list: Get all entities owned by the current user
func api_entity_owned(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	db := db_open("db/users.db")
	entities, err := db.rows("select id, fingerprint, class, name from entities where user=? order by name", user.ID)
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}

	return sl_encode(entities), nil
}

// mochi.entity.privacy.set(id, privacy) -> bool: Update entity privacy setting
func api_entity_privacy_set(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return sl_error(fn, "syntax: <id: string>, <privacy: string>")
	}

	id, ok := sl.AsString(args[0])
	if !ok || !valid(id, "entity") {
		return sl_error(fn, "invalid id %q", id)
	}

	privacy, ok := sl.AsString(args[1])
	if !ok || (privacy != "public" && privacy != "private") {
		return sl_error(fn, "privacy must be 'public' or 'private'")
	}

	// Get user from context
	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	// Verify ownership
	db := db_open("db/users.db")
	row, err := db.row("select user from entities where id=?", id)
	if err != nil || row == nil {
		return sl.False, nil
	}

	owner_id, _ := row["user"].(int64)
	if owner_id != int64(user.ID) {
		return sl_error(fn, "not owner")
	}

	// Update privacy
	db.exec("update entities set privacy=? where id=?", privacy, id)

	return sl.True, nil
}

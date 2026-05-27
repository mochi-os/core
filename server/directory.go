// Mochi server: Directory
// Copyright Alistair Cunningham 2024-2026

package main

import (
	"time"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
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

var api_directory = sls.FromStringDict(sl.String("mochi.directory"), sl.StringDict{
	"get":    sl.NewBuiltin("mochi.directory.get", api_directory_get),
	"search": sl.NewBuiltin("mochi.directory.search", api_directory_search),
})

func init() {
	a := app("directory")
	a.service("directory")
	a.event("delete", directory_delete_event)               // Requires signature (from == entity being deleted)
	a.event_anonymous("download", directory_download_event) // Unsigned request for directory sync
	a.event_anonymous("publish", directory_publish_event)   // Allows anonymous from bootstrap peers
	a.event_anonymous("request", directory_request_event)   // Unsigned pubsub broadcast
}

// Handle incoming delete events from the network
func directory_delete_event(e *Event) {
	debug("Directory received delete event '%+v'", e)

	id := e.get("entity", "")
	if !valid(id, "entity") {
		debug("Directory dropping delete event with invalid entity id %q", id)
		return
	}

	// Verify the message was signed by the entity being deleted
	if e.from != id {
		info("Directory dropping delete event from incorrect sender: %q!=%q", id, e.from)
		return
	}

	db := db_open("db/directory.db")
	db.exec("delete from entities where id=?", id)

	debug("Removed entity %s from entities (deletion announcement)", id)
}

// Create or update a directory entry for a local entity.
// Preserves the original created timestamp if entry already exists.
func directory_create(e *Entity) {
	debug("Directory creating entry %q %q", e.ID, e.Name)
	now := now()

	fp := fingerprint(e.ID)
	db := db_open("db/directory.db")
	exists, _ := db.exists("select 1 from entities where id=?", e.ID)
	if exists {
		db.exec("update entities set name=?, class=?, location=?, data=?, fingerprint=?, updated=? where id=?", e.Name, e.Class, "p2p/"+net_id, e.Data, fp, now, e.ID)
	} else {
		db.exec("insert into entities (id, name, class, location, data, fingerprint, created, updated) values (?, ?, ?, ?, ?, ?, ?, ?)", e.ID, e.Name, e.Class, "p2p/"+net_id, e.Data, fp, now, now)
	}
	db.exec("insert or replace into locations (entity, peer, seen) values (?, ?, ?)", e.ID, net_id, now)
}

// Ask known peers to send us any updates since the newest update in our copy of the directory
func directory_download() {
	for _, p := range peers_bootstrap {
		if p.ID == net_id {
			continue // Don't download from self
		}
		if directory_download_from_peer(p.ID) {
			return
		}
	}
}

// Download directory updates from a specific peer
func directory_download_from_peer(peer string) bool {
	s := peer_stream(peer)
	if s == nil {
		debug("Stream unable to open to peer %q", peer)
		return false
	}
	defer s.close()

	// Read challenge from receiver (required before sending headers)
	_, err := s.read_challenge()
	if err != nil {
		debug("Stream unable to read challenge: %v", err)
		return false
	}

	err = s.write(Headers{Service: "directory", Event: "download"})
	if err != nil {
		return false
	}

	start := int64(0)
	var u Directory
	db := db_open("db/directory.db")
	if db.scan(&u, "select updated from entities order by updated desc limit 1") {
		start = u.Updated
	}
	debug("Directory downloading updates since %s from peer %q", time_local(nil, start), peer)
	s.write_content("start", i64toa(start), "version", build_version)

	users := db_open("db/users.db")
	for {
		var d Directory
		err := s.read(&d)
		if err != nil {
			debug("Directory download finished")
			return true
		}

		debug("Directory got update %s %q", d.ID, d.Name)

		// Don't let remote peers override location for local entities
		local, _ := users.exists("select 1 from entities where id=?", d.ID)
		if local {
			d.Location = "p2p/" + net_id
		}

		fp := d.Fingerprint
		if fp == "" {
			fp = fingerprint(d.ID)
		}
		db.exec("replace into entities (id, name, class, location, data, fingerprint, created, updated) values (?, ?, ?, ?, ?, ?, ?, ?)", d.ID, d.Name, d.Class, d.Location, d.Data, fp, d.Created, d.Updated)

		// Also record the location claim in `locations` — the routing
		// table that entity_peer / entity_peers_failover read. The
		// per-entity live handler (directory_publish_event) does this,
		// but the bulk download previously wrote `entities` only, so a
		// freshly-wiped server ended up knowing every entity yet able
		// to route to almost none until each one happened to republish.
		// `seen` is the record's own `Updated` (not now()) so the
		// active/stale failover tiering stays honest about freshness.
		directory_record_location(db, d.ID, d.Location, d.Updated)

		go queue_check_entity(d.ID)
	}
}

// Reply to a directory download request
func directory_download_event(e *Event) {
	version := e.get("version", "unknown")
	remote := ""
	if e.stream != nil {
		remote = e.stream.remote
	}
	start := atoi(e.get("start", ""), 0)
	debug("Directory received download request from peer %q at %q, version %q, since %s", e.peer, remote, version, time_local(nil, start))

	var results []Directory
	db := db_open("db/directory.db")
	err := db.scans(&results, "select * from entities where updated>=? order by created, id", start)
	if err != nil {
		warn("Database error loading directory updates: %v", err)
		return
	}
	for _, d := range results {
		debug("Directory sending %q %q", d.ID, d.Name)
		err = e.stream.write(d)
		if err != nil {
			warn("Directory write error for %q: %v", d.ID, err)
			return
		}
	}
}

// Manage the directory
func directory_manager() {
	time.Sleep(3 * time.Second)
	directory_download()

	cleanup := now()
	for range time.Tick(5 * time.Minute) {
		directory_download()
		if now()-cleanup > 24*60*60 {
			cleanup = now()
			debug("Directory deleting stale entries")
			db := db_open("db/directory.db")
			db.exec("delete from entities where updated<?", now()-30*86400)
			db.exec("delete from locations where seen<?", now()-30*86400)

			// Clean up local directory entries for deleted entities
			location := "p2p/" + net_id
			users := db_open("db/users.db")
			rows, _ := db.rows("select id from entities where location=?", location)
			for _, row := range rows {
				id := row["id"].(string)
				exists, _ := users.exists("select 1 from entities where id=?", id)
				if !exists {
					debug("Directory removing orphaned local entry %q", id)
					db.exec("delete from entities where id=?", id)
					db.exec("delete from locations where entity=? and peer=?", id, net_id)
				}
			}
		}
	}
}

// Publish a directory entry to the entire network
func directory_publish(e *Entity, allow_queue bool) {
	m := message(e.ID, "", "directory", "publish")

	// Include created timestamp so bootstrap peers can propagate it
	created := now()
	db := db_open("db/directory.db")
	var d Directory
	if db.scan(&d, "select created from entities where id=?", e.ID) {
		created = d.Created
	}

	m.set("id", e.ID, "name", e.Name, "class", e.Class, "location", "p2p/"+net_id, "data", e.Data, "created", i64toa(created))
	m.publish(allow_queue)
}

// Received a directory publish event from another server
func directory_publish_event(e *Event) {
	//debug("Directory received publish event '%+v', content '%+v'", e, e.content)
	now := now()

	id := e.get("id", "")
	if !valid(id, "entity") {
		debug("Directory dropping publish event with invalid id %q", id)
		return
	}

	name := e.get("name", "")
	if !valid(name, "line") {
		debug("Directory dropping publish event with invalid name %q", name)
		return
	}

	class := e.get("class", "")
	if !valid(class, "constant") {
		debug("Directory dropping publish event with invalid class %q", class)
		return
	}

	location := e.get("location", "")
	if !valid(location, "line") {
		debug("Directory dropping publish event with invalid location %q", location)
		return
	}

	data := e.get("data", "")
	if !valid(data, "text") {
		debug("Directory dropping publish event with invalid data")
		return
	}

	db := db_open("db/directory.db")
	var created int64

	if e.from == "" {
		if !peer_is_bootstrap(e.peer) {
			info("Directory dropping anonymous event from untrusted peer")
			return
		}

		// Trust created from bootstrap peer
		created = atoi(e.get("created", ""), now)

	} else if e.from != id {
		info("Directory dropping event from incorrect sender: %q!=%q", id, e.from)
		return
	} else {
		// Non-bootstrap peer: preserve created unless name changed or entry is new
		var existing Directory
		if db.scan(&existing, "select created, name from entities where id=?", id) {
			if name != existing.Name {
				created = now
			} else {
				created = existing.Created
			}
		} else {
			created = now
		}
	}

	// Don't let remote peers override location for local entities
	users := db_open("db/users.db")
	local, _ := users.exists("select 1 from entities where id=?", id)
	if local {
		location = "p2p/" + net_id
	}

	db.exec("replace into entities (id, name, class, location, data, fingerprint, created, updated) values (?, ?, ?, ?, ?, ?, ?, ?)", id, name, class, location, data, fingerprint(id), created, now)

	directory_record_location(db, id, location, now)

	go queue_check_entity(id)
}

// directory_record_location records an entity's location claim into the
// `locations` routing table — the table entity_peer / entity_peers /
// entity_peers_failover read to resolve an entity to its host peer(s).
// The `location` field carries "p2p/<peer-id>"; the prefix is stripped,
// and an empty peer or our own self-claim is skipped (a server isn't a
// routable remote peer for its own entities).
//
// `seen` is the freshness timestamp the failover tiering uses: pass the
// live now() for a fresh first-hand claim (directory_publish_event), or
// the record's own `updated` for a bulk-download record so a stale
// directory entry isn't misreported as just-seen.
func directory_record_location(db *DB, id, location string, seen int64) {
	if peer := strings_trim_prefix(location, "p2p/"); peer != "" && peer != net_id {
		db.exec("insert or replace into locations (entity, peer, seen) values (?, ?, ?)", id, peer, seen)
	}
}

// strings_trim_prefix is a tiny helper that avoids importing the strings
// package solely for TrimPrefix.
func strings_trim_prefix(s, prefix string) string {
	if len(s) >= len(prefix) && s[:len(prefix)] == prefix {
		return s[len(prefix):]
	}
	return s
}

// Reply to a directory request if we have the requested public entity
func directory_request_event(e *Event) {
	debug("Directory received request event '%+v'", e)

	var r Entity
	db := db_open("db/users.db")
	if db.scan(&r, "select * from entities where id=? and privacy='public'", e.to) {
		directory_publish(&r, false)
	}
}

// mochi.directory.get(id) -> dict or None: Get a directory entry
func api_directory_get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <id: string>")
	}

	id, ok := sl.AsString(args[0])
	if !ok || !valid(id, "entity") {
		return sl_error(fn, "invalid ID %q", id)
	}

	db := db_open("db/directory.db")
	d, err := db.row("select * from entities where id=?", id)
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}
	if d == nil {
		return sl.None, nil
	}
	fp, _ := d["fingerprint"].(string)
	if fp == "" {
		fp = fingerprint(d["id"].(string))
	}
	d["fingerprint"] = fp
	d["fingerprint_hyphens"] = fingerprint_hyphens(fp)

	return sl_encode(d), nil
}

// mochi.directory.search(class, search, include_self, fingerprint="") -> list: Search the directory
func api_directory_search(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 3 {
		return sl_error(fn, "syntax: <class: string>, <search: string>, <include self: boolean>, [fingerprint: string]")
	}

	class, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid class %q", class)
	}

	search, ok := sl.AsString(args[1])
	if !ok {
		return sl_error(fn, "invalid search %q", search)
	}

	include_self := bool(args[2].Truth())

	// Optional fingerprint kwarg for indexed lookup
	fp_search := ""
	for _, kv := range kwargs {
		k, _ := sl.AsString(kv[0])
		if k == "fingerprint" {
			fp_search, _ = sl.AsString(kv[1])
		}
	}

	u := t.Local("user").(*User)

	db := db_open("db/directory.db")
	var ds []map[string]any
	var err error
	if fp_search != "" {
		ds, err = db.rows("select * from entities where class=? and fingerprint=? order by name, created", class, fp_search)
	} else {
		ds, err = db.rows("select * from entities where class=? and name like ? escape '\\' order by name, created", class, "%"+like_escape(search)+"%")
	}
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}

	for _, d := range ds {
		fp, _ := d["fingerprint"].(string)
		if fp == "" {
			fp = fingerprint(d["id"].(string))
		}
		d["fingerprint"] = fp
		d["fingerprint_hyphens"] = fingerprint_hyphens(fp)
	}

	if u == nil || include_self || class != "person" {
		return sl_encode(ds), nil
	}

	dbu := db_open("db/users.db")
	var es []Entity
	err = dbu.scans(&es, "select id from entities where user=?", u.UID)
	if err != nil {
		warn("Database error loading user entities: %v", err)
		return sl_encode(ds), nil
	}
	me := map[string]bool{}
	for _, e := range es {
		me[e.ID] = true
	}

	var o []map[string]any
	for _, d := range ds {
		_, found := me[d["id"].(string)]
		if !found {
			o = append(o, d)
		}
	}
	return sl_encode(&o), nil
}

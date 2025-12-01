// Mochi server: Directory
// Copyright Alistair Cunningham 2024-2025

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
	a.event("download", directory_download_event)
	a.event("publish", directory_publish_event)
	a.event("request", directory_request_event)
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
	debug("Directory creating entry %q %q", e.ID, e.Name)
	now := now()

	db := db_open("db/directory.db")
	db.exec("replace into directory ( id, fingerprint, name, class, location, data, created, updated ) values ( ?, ?, ?, ?, ?, ?, ?, ? )", e.ID, e.Fingerprint, e.Name, e.Class, "p2p/"+p2p_id, e.Data, now, now)
}

// Ask known peers to send us any updates since the newest update in our copy of the directory
func directory_download() {
	for _, p := range peers_bootstrap {
		debug("Directory downloading from peer %q", p.ID)

		s := peer_stream(p.ID)
		if s == nil {
			debug("Stream unable to open to peer %q", p.ID)
			continue
		}
		debug("Stream %d open to peer %q: from '', to '', service 'directory', event 'download'", s.id, p.ID)

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
}

// Reply to a directory download request
func directory_download_event(e *Event) {
	debug("Directory received download request")

	start := atoi(e.get("start", ""), 0)
	debug("Directory sending updates since %d", start)

	var results []Directory
	db := db_open("db/directory.db")
	err := db.scans(&results, "select * from directory where updated>=? order by created, id", start)
	if err != nil {
		warn("Database error loading directory updates: %v", err)
		return
	}
	for _, d := range results {
		debug("Directory sending update %#v", d)
		e.stream.write(d)
	}

	debug("Directory finished sending updates")
}

// Manage the directory
func directory_manager() {
	time.Sleep(3 * time.Second)
	directory_download()
	time.Sleep(30 * time.Minute)

	for {
		debug("Directory deleting stale entries")
		db := db_open("db/directory.db")
		db.exec("delete from directory where updated<?", now()-30*86400)
		time.Sleep(24 * time.Hour)
	}
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
		debug("Directory dropping event with invalid entity id %q", id)
		return
	}

	name := e.get("name", "")
	if !valid(name, "line") {
		debug("Directory dropping event with invalid name %q", name)
		return
	}

	class := e.get("class", "")
	if !valid(class, "constant") {
		debug("Directory dropping event with invalid class %q", class)
		return
	}

	location := e.get("location", "")
	if !valid(location, "line") {
		debug("Directory dropping event with invalid location %q", location)
		return
	}

	data := e.get("data", "")
	if !valid(data, "text") {
		debug("Directory dropping event with invalid data %q", data)
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
		info("Directory dropping event from incorrect sender: %q!=%q", id, e.from)
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
	err := dbd.scans(&ds, "select * from directory where class=? and name like ? escape '\\' order by name, created", class, "%"+like_escape(search)+"%")
	if err != nil {
		warn("Database error searching directory: %v", err)
		return &ds
	}

	for i, _ := range ds {
		ds[i].Fingerprint = fingerprint_hyphens(ds[i].Fingerprint)
	}

	if u == nil || include_self || class != "person" {
		return &ds
	}

	dbu := db_open("db/users.db")
	var es []Entity
	err = dbu.scans(&es, "select id from entities where user=?", u.ID)
	if err != nil {
		warn("Database error loading user entities: %v", err)
		return &ds
	}
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
	d, err := db.row("select * from directory where id=?", id)
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}
	if d == nil {
		return sl.None, nil
	}
	d["fingerprint_hyphens"] = fingerprint_hyphens(d["fingerprint"].(string))

	return sl_encode(d), nil
}

// mochi.directory.search(class, search, include_self) -> list: Search the directory
func api_directory_search(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 3 {
		return sl_error(fn, "syntax: <class: string>, <search: string>, <include self: boolean>")
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
	u := t.Local("user").(*User)

	db := db_open("db/directory.db")
	ds, err := db.rows("select * from directory where class=? and name like ? escape '\\' order by name, created", class, "%"+like_escape(search)+"%")
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}

	for _, d := range ds {
		d["fingerprint_hyphens"] = fingerprint_hyphens(d["fingerprint"].(string))
	}

	if u == nil || include_self || class != "person" {
		return sl_encode(ds), nil
	}

	dbu := db_open("db/users.db")
	var es []Entity
	err = dbu.scans(&es, "select id from entities where user=?", u.ID)
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

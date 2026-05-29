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

// Download directory updates from a specific peer over /mochi/2/stream.
// The wire content after the handshake is a series of CBOR-encoded
// Directory rows until EOF.
func directory_download_from_peer(peer string) bool {
	start := int64(0)
	var u Directory
	db := db_open("db/directory.db")
	if db.scan(&u, "select updated from entities order by updated desc limit 1") {
		start = u.Updated
	}
	debug("Directory downloading updates since %s from peer %q", time_local(nil, start), peer)

	// Build the content map the receiver's directory_download_event
	// reads via e.get("start", ...).
	content := map[string]any{
		"start":   i64toa(start),
		"version": build_version,
	}

	s, err := stream_open_or_self(peer, "", "", "directory", "download", "", nil, content)
	if err != nil || s == nil {
		debug("Directory stream unable to open to peer %q: %v", peer, err)
		return false
	}
	defer s.close()

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

// directory_location_max_age is how long a directory location row may
// remain un-refreshed before a silenced peer's directory entries get
// forgotten. Live peers re-publish on every libp2p connect and via
// peers_publish far more often than this; only a peer that's been
// dark to us for the full window gets considered.
const directory_location_max_age = 14 * 86400 // 14 days

// directory_cleanup_manager runs once per hour and forgets peers that
// have been demonstrably unreachable for a long time. See
// directory_forget_peer for the cleanup action and
// directory_cleanup_dead_peers for the selection criteria.
//
// Decoupled from directory_manager (which handles downloads + a daily
// row-level expiry) because this sweep operates at peer granularity:
// when ONE peer is dead, every entity it hosted needs cleaning, and
// the queue.db rows targeting it need clearing in the same step.
func directory_cleanup_manager() {
	// Stagger the first sweep so it doesn't pile on startup work.
	time.Sleep(5 * time.Minute)
	directory_cleanup_dead_peers()
	for range time.Tick(time.Hour) {
		directory_cleanup_dead_peers()
	}
}

// directory_cleanup_dead_peers scans the locations table grouped by
// peer and forgets peers that meet ALL of the following:
//
//   - Not net_id (self never silenced).
//   - Not a bootstrap peer (trusted infrastructure; never forget).
//   - Not a pair-set member (whole-server replication partner; its
//     lifecycle is operator-controlled via replica join/leave).
//   - Most recent `seen` for this peer < now - directory_location_max_age.
//   - peer_is_silent(peer) == true: the in-memory silent-cache has
//     confirmed via repeated failed stream opens that the peer is
//     genuinely unreachable, not just absent from the directory due
//     to a transient announcement gap.
//
// Both the time and silent criteria matter — `seen` alone would
// prematurely forget a peer that's only briefly offline; silent-cache
// alone would forget a peer that's down for an hour. Together they
// catch only peers that have been gone long enough to be considered
// dead, AND that we've recently tried to reach.
func directory_cleanup_dead_peers() {
	cutoff := now() - directory_location_max_age
	ddb := db_open("db/directory.db")
	rows, err := ddb.rows(
		"select peer, max(seen) as latest from locations where peer != ? group by peer having latest < ?",
		net_id, cutoff,
	)
	if err != nil {
		warn("Directory cleanup: locations query: %v", err)
		return
	}
	for _, row := range rows {
		peer, _ := row["peer"].(string)
		if peer == "" {
			continue
		}
		if peer_is_bootstrap(peer) {
			continue
		}
		if peer_is_pair(peer) {
			continue
		}
		if !peer_is_silent(peer) {
			// Stale `seen` but the silent-cache hasn't confirmed
			// unreachable. Could be transient (we just restarted and
			// the cache is cold; or the peer hasn't been pinged
			// recently). Skip this round; the next hourly sweep will
			// re-evaluate.
			continue
		}
		directory_forget_peer(peer)
	}
}

// directory_forget_peer deletes every trace of `peer` from this
// server's per-host state: directory locations rows that name it as
// a host, queue rows targeting it, peers.db addresses for it, and
// the in-memory peer/reachability/reconnect caches.
//
// Purely local — directory.db, queue.db, and peers.db are per-host
// state, not replicated across pair members, so no leader/coordination
// is needed. The peer can come back later: a fresh libp2p connect
// drives the directory re-exchange + peers_publish, and the location
// rows + peers.db addresses re-populate with current data.
//
// Logs row counts at info so the operator can see what happened.
func directory_forget_peer(peer string) {
	if peer == "" || peer == net_id {
		return
	}
	ddb := db_open("db/directory.db")
	qdb := db_open("db/queue.db")
	pdb := db_open("db/peers.db")

	loc_n := count_rows(ddb, "select count(*) from locations where peer=?", peer)
	queue_n := count_rows(qdb, "select count(*) from queue where target=?", peer)
	addr_n := count_rows(pdb, "select count(*) from peers where id=?", peer)

	ddb.exec("delete from locations where peer=?", peer)
	qdb.exec("delete from queue where target=?", peer)
	pdb.exec("delete from peers where id=?", peer)

	// In-memory caches.
	peer_mark_reachable(peer)
	peer_reconnect_lock.Lock()
	delete(peer_reconnects, peer)
	peer_reconnect_lock.Unlock()
	peers_lock.Lock()
	delete(peers, peer)
	peers_lock.Unlock()

	info("Directory forgot dead peer %q: %d locations, %d queue rows, %d addresses",
		peer, loc_n, queue_n, addr_n)
}

// count_rows is a small helper for directory_forget_peer's row-count
// logging. Returns 0 on error rather than panicking — the cleanup
// proceeds either way; the count is just diagnostic.
func count_rows(db *DB, query string, args ...any) int64 {
	row, err := db.row(query, args...)
	if err != nil || row == nil {
		return 0
	}
	for _, v := range row {
		if n, ok := v.(int64); ok {
			return n
		}
	}
	return 0
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

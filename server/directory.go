// Mochi server: Directory
//
// One row per (entity, peer): a host publishes and deletes only rows naming
// itself, and every row carries the entity's ed25519 signature over the whole
// row, peer and seen included, so no arrival path has to be trusted.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"time"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

// Entry is one directory row and the sync stream's wire shape.
type Entry struct {
	Entity      string
	Peer        string
	Name        string
	Class       string
	Data        string
	Fingerprint string
	Version     int64
	Created     int64
	Seen        int64
	Message     string // the announcement's frame id
	Expires     string // the announcement's signed freshness bound
	Signature   string // the entity's signature over the whole announcement
}

// A row's own signature is the pubsub announcement's, so it carries
// pubsub_domain. Only the host-key withdrawal needs a domain of its own.
const entry_delete_domain = "mochi/2/entry/delete"

var api_directory = sls.FromStringDict(sl.String("mochi.directory"), sl.StringDict{
	"get":    sl.NewBuiltin("mochi.directory.get", api_directory_get),
	"search": sl.NewBuiltin("mochi.directory.search", api_directory_search),
})

// directory_init registers the directory app and its handlers; called from
// main_serve.
func directory_init() {
	a := app("directory")
	a.service("directory")
	// All payloads are self-verifying; the message envelope is anonymous.
	a.event_anonymous("publish", directory_publish_event)
	a.event_anonymous("delete", directory_delete_event)
	a.event_anonymous("request", directory_request_event)
	a.event_anonymous("sync", directory_sync_event)
	a.event_anonymous("push", directory_push_event)
}

// entry_content projects a row back to the content map its announcement
// carried. The entity is NOT in content — it is the announcement's from,
// which is what makes the row self-signing rather than self-asserted.
func entry_content(en *Entry) map[string]string {
	return map[string]string{
		"peer":    en.Peer,
		"name":    en.Name,
		"class":   en.Class,
		"data":    en.Data,
		"version": i64toa(en.Version),
		"created": i64toa(en.Created),
		"seen":    i64toa(en.Seen),
	}
}

// entry_verify checks a row against the entity signature its announcement
// carried. The row stores the whole signed announcement, so this works
// identically for one that arrived by flood and one re-served from the
// database over a sync stream — there is no second signature.
func entry_verify(en *Entry) bool {
	return pubsub_verify(en.Message, en.Entity, "directory", "publish",
		en.Expires, entry_content(en), base58_decode(en.Signature, "")) == nil
}

// entry_delete_signable returns the canonical CBOR a host signs to delete
// its own row.
func entry_delete_signable(entity, peer string, time int64) ([]byte, error) {
	return canonical_encoder.Marshal(map[string]any{
		"v":      entry_delete_domain,
		"entity": entity,
		"peer":   peer,
		"time":   i64toa(time),
	})
}

// entry_store is the single gate every received row passes through —
// live publish and sync alike: validate fields, verify both signatures,
// apply the ordering rules, upsert. Returns whether the row was stored.
func entry_store(en *Entry, source string) bool {
	if !valid(en.Entity, "entity") || !valid(en.Name, "display") || !valid(en.Class, "constant") || !valid(en.Data, "text") {
		debug("Directory dropping invalid row for %q from %s", en.Entity, source)
		return false
	}
	if en.Version <= 0 || en.Created <= 0 || en.Seen <= 0 || en.Seen > now()+3600 {
		debug("Directory dropping row with bad timestamps for %q from %s", en.Entity, source)
		return false
	}
	// Verify before the branches below act on the row: the ghost branch answers
	// with a host-signed broadcast, so an unverified entity id read off the wire
	// would buy a signature and a publish from us.
	if !entry_verify(en) {
		info("Directory dropping row with bad signature: entity=%q peer=%q from %s", en.Entity, en.Peer, source)
		return false
	}
	// A row naming this host for an entity gone locally is a pre-wipe ghost only
	// this host's key can withdraw: answer the echo with a deletion. A replayed
	// copy never overrides local state, which directory_create alone rebuilds.
	if en.Peer == net_id {
		users := db_open("db/users.db")
		exists, _ := users.exists("select 1 from entities where id=?", en.Entity)
		if !exists && rate_limit_entry_withdraw.allow(en.Entity) {
			info("Directory withdrawing ghost row for %q echoed by %s: entity no longer exists here", en.Entity, source)
			entry_delete_self(en.Entity)
		}
		return false
	}
	// Owner-authoritative: a row naming a DIFFERENT peer for a locally-owned
	// entity is stale. A clone or restored backup holds the entity's keys, so its
	// rows VERIFY; ownership, not the signature, is what makes them wrong.
	if local, ok := entity_local(en.Entity); !ok || local {
		if local {
			debug("Directory dropping foreign row for locally-owned %q (peer=%q) from %s", en.Entity, en.Peer, source)
		}
		return false
	}

	db := db_open("db/directory.db")
	row, _ := db.row("select version, seen from entries where entity=? and peer=?", en.Entity, en.Peer)
	if row != nil {
		version, _ := row["version"].(int64)
		seen, _ := row["seen"].(int64)
		newer := en.Version > version || (en.Version == version && en.Seen > seen)
		if !newer {
			return false
		}
	}

	// Fingerprint is derived locally, never trusted from the wire.
	db.exec("replace into entries (entity, peer, name, class, data, fingerprint, version, created, seen, message, expires, signature) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		en.Entity, en.Peer, en.Name, en.Class, en.Data, fingerprint(en.Entity), en.Version, en.Created, en.Seen, en.Message, en.Expires, en.Signature)

	go queue_check_entity(en.Entity)
	return true
}

// directory_create builds or refreshes this host's row for a local entity.
// Unchanged content keeps its version; a change takes version = now() and a
// rename also resets created, so an impersonator cannot inherit seniority.
func directory_create(e *Entity) {
	debug("Directory creating entry %q %q", e.ID, e.Name)
	now := now()

	db := db_open("db/directory.db")
	var existing Entry
	have := db.scan(&existing, "select * from entries where entity=? and peer=?", e.ID, net_id)

	version := now
	created := now
	if have && existing.Name == e.Name && existing.Class == e.Class && existing.Data == e.Data {
		version = existing.Version
		created = existing.Created
	} else if have && existing.Name == e.Name {
		created = existing.Created
	}

	// Mint the announcement this row IS: the same signed artifact the network
	// receives, kept so the sync stream can re-serve it without re-signing.
	en := Entry{Entity: e.ID, Peer: net_id, Name: e.Name, Class: e.Class, Data: e.Data,
		Version: version, Created: created, Seen: now,
		Message: uid(), Expires: i64toa(now + pubsub_expires_ttl)}
	sig := pubsub_sign(en.Message, en.Entity, "directory", "publish", en.Expires, entry_content(&en))
	if sig == nil {
		if !entity_present(e.ID) {
			debug("Directory skipping entry for %q: the entity is gone", e.ID)
			return
		}
		warn("Directory unable to sign entry for %q", e.ID)
		return
	}
	en.Signature = base58_encode(sig)

	db.exec("replace into entries (entity, peer, name, class, data, fingerprint, version, created, seen, message, expires, signature) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		en.Entity, en.Peer, en.Name, en.Class, en.Data, fingerprint(en.Entity), en.Version, en.Created, en.Seen, en.Message, en.Expires, en.Signature)
}

// directory_publish broadcasts this host's row for a local entity to the
// network. The row must already exist locally (directory_create).
func directory_publish(e *Entity, allow_queue bool) {
	db := db_open("db/directory.db")
	var en Entry
	if !db.scan(&en, "select * from entries where entity=? and peer=?", e.ID, net_id) {
		directory_create(e)
		if !db.scan(&en, "select * from entries where entity=? and peer=?", e.ID, net_id) {
			return
		}
	}

	// The entity signs its own announcement, so it rides as the from — an
	// identity the receiver proves, not a field it reads out of content.
	m := message(en.Entity, "", "directory", "publish")
	for k, v := range entry_content(&en) {
		m.set(k, v)
	}
	m.publish(allow_queue)
}

// entry_delete_self removes this host's row for an entity, locally and
// network-wide. Host-key signed, so it has no dependency on the entity's
// keys — it works during and after account teardown, with no ordering
// constraint against key deletion.
func entry_delete_self(entity string) {
	db := db_open("db/directory.db")
	db.exec("delete from entries where entity=? and peer=?", entity, net_id)

	t := now()
	signable, err := entry_delete_signable(entity, net_id, t)
	if err != nil {
		warn("Directory delete canonical encode failed for %q: %v", entity, err)
		return
	}
	m := message("", "", "directory", "delete")
	m.set("entity", entity, "peer", net_id, "time", i64toa(t), "attestation", base58_encode(server_sign(signable)))
	m.publish(false)
}

// Received a directory publish from the network: one host's row for one
// entity, verified entirely from the payload.
func directory_publish_event(e *Event) {
	// from is the entity, already proven by pubsub_receive. It is never read
	// out of content: an identity you read is asserted, one you verify is
	// proven, and that difference is the whole point of the signature.
	en := Entry{
		Entity:    e.from,
		Peer:      e.get("peer", ""),
		Name:      e.get("name", ""),
		Class:     e.get("class", ""),
		Data:      e.get("data", ""),
		Version:   atoi(e.get("version", ""), 0),
		Created:   atoi(e.get("created", ""), 0),
		Seen:      atoi(e.get("seen", ""), 0),
		Message:   e.message,
		Expires:   e.expires,
		Signature: base58_encode(e.signature),
	}
	entry_store(&en, "publish")
}

// Received a directory delete from the network: a host withdrawing its own
// row. Only rows naming the signing peer can be affected, so the worst any
// sender can do is withdraw its own listings.
func directory_delete_event(e *Event) {
	entity := e.get("entity", "")
	peer := e.get("peer", "")
	t := atoi(e.get("time", ""), 0)
	if !valid(entity, "entity") || peer == "" || t <= 0 {
		return
	}
	if peer == net_id {
		return // we are authoritative for our own rows
	}
	signable, err := entry_delete_signable(entity, peer, t)
	if err != nil {
		return
	}
	if !server_verify(peer, signable, base58_decode(e.get("attestation", ""), "")) {
		info("Directory dropping delete with bad attestation: entity=%q peer=%q", entity, peer)
		return
	}
	db := db_open("db/directory.db")
	db.exec("delete from entries where entity=? and peer=? and seen<=?", entity, peer, t)
	debug("Directory removed row entity=%q peer=%q (withdrawal)", entity, peer)
}

// Reply to a directory request if we hold the requested public entity.
func directory_request_event(e *Event) {
	id := e.get("entity", "")
	if !valid(id, "entity") {
		return
	}
	var r Entity
	db := db_open("db/users.db")
	if db.scan(&r, "select * from entities where id=? and privacy='public'", id) {
		directory_publish(&r, false)
	}
}

// directory_sync pulls rows from one reachable bootstrap peer (a bootstrap
// peer accumulates the wider fleet). The first success wins.
func directory_sync() {
	for _, p := range peers_bootstrap {
		if p.ID == net_id {
			continue // Don't sync from self
		}
		if directory_sync_from_peer(p.ID) {
			directory_push_to_peer(p.ID)
			break
		}
	}
}

// directory_push_watermark is the highest self-row `seen` already pushed to
// each sync peer. In-memory: a restart repeats one full push, which entry_store
// dedups. Touched only from the directory_manager goroutine.
var directory_push_watermark = map[string]int64{}

// directory_push_rows returns this host's own rows re-attested after the
// watermark, oldest first so the watermark can advance monotonically.
func directory_push_rows(watermark int64) []Entry {
	var rows []Entry
	db := db_open("db/directory.db")
	if err := db.scans(&rows, "select * from entries where peer=? and seen>? order by seen", net_id, watermark); err != nil {
		warn("Database error loading directory rows for push: %v", err)
		return nil
	}
	return rows
}

// directory_push_to_peer delivers this host's own rows to one sync peer over a
// stream. Pubsub republish is the low-latency path but silently truncates
// bursts larger than gossipsub's per-peer outbound queue, so correctness rides
// on this.
func directory_push_to_peer(peer string) {
	rows := directory_push_rows(directory_push_watermark[peer])
	if len(rows) == 0 {
		return
	}
	s, err := stream_open_or_self(peer, "", "", "directory", "push", "", nil, map[string]any{"version": build_version})
	if err != nil || s == nil {
		debug("Directory push stream unable to open to peer %q: %v", peer, err)
		return
	}
	defer s.close()
	for _, en := range rows {
		if err := s.write(en); err != nil {
			debug("Directory push write error to peer %q: %v", peer, err)
			return
		}
	}
	directory_push_watermark[peer] = rows[len(rows)-1].Seen
	debug("Directory pushed %d rows to peer %q", len(rows), peer)
}

// directory_push_rows_maximum bounds one push stream. The stream's own cap is
// cumulative bytes; the cost here is per ROW - four validators, up to three
// SQLite queries and an ed25519 verification each - and far above any
// legitimate push.
const directory_push_rows_maximum = 100000

// directory_push_event receives a peer's directory rows over a stream. Every
// row passes the same verification gate as a live publish, so the sender is
// only a carrier. Anonymous, so bounded on both axes: push rate and rows per
// push.
func directory_push_event(e *Event) {
	if e.peer != "" && !rate_limit_directory_push.allow(e.peer) {
		debug("Directory push refused: peer %q over the push rate limit", e.peer)
		return
	}
	stored := 0
	for read := 0; read < directory_push_rows_maximum; read++ {
		var en Entry
		if err := e.stream.read(&en); err != nil {
			debug("Directory push from peer %q finished: %d rows stored", e.peer, stored)
			return
		}
		if entry_store(&en, "push") {
			stored++
		}
	}
	// Counted on rows READ, not stored: a row rejected by entry_store still
	// cost the validation that rejected it.
	debug("Directory push from peer %q truncated at %d rows: %d stored", e.peer, directory_push_rows_maximum, stored)
}

// directory_sync_from_peer pulls rows updated since our watermark from one peer
// over /mochi/2/stream. The watermark is max(seen) minus an hour of overlap;
// entry_store makes the overlap idempotent and verifies every row.
func directory_sync_from_peer(peer string) bool {
	start := int64(0)
	db := db_open("db/directory.db")
	if row, err := db.row("select max(seen) as seen from entries"); err == nil && row != nil {
		if v, ok := row["seen"].(int64); ok {
			start = v
		}
	}
	if start > 3600 {
		start -= 3600
	}
	debug("Directory syncing rows since %s from peer %q", time_local(nil, start), peer)

	content := map[string]any{
		"start":   i64toa(start),
		"version": build_version,
	}
	s, err := stream_open_or_self(peer, "", "", "directory", "sync", "", nil, content)
	if err != nil || s == nil {
		debug("Directory stream unable to open to peer %q: %v", peer, err)
		return false
	}
	defer s.close()

	for {
		var en Entry
		if err := s.read(&en); err != nil {
			debug("Directory sync finished")
			return true
		}
		entry_store(&en, "sync")
	}
}

// Serve a directory sync request: stream every row at or after the
// requester's watermark. Rows carry their signatures, so the requester
// verifies each one itself.
func directory_sync_event(e *Event) {
	start := atoi(e.get("start", ""), 0)
	remote := ""
	if e.stream != nil {
		remote = e.stream.remote
	}
	debug("Directory received sync request from peer %q at %q since %s", e.peer, remote, time_local(nil, start))

	// Anonymous by design, and answering costs a full read of every row at or
	// after the watermark. Nothing bounds how often a peer may ask, so one
	// small frame repeated is an amplifier. Keyed on the requesting peer.
	if e.peer != "" && !rate_limit_directory_sync.allow(e.peer) {
		debug("Directory sync refused: peer %q over the request rate limit", e.peer)
		return
	}

	var results []Entry
	db := db_open("db/directory.db")
	err := db.scans(&results, "select * from entries where seen>=? order by seen", start)
	if err != nil {
		warn("Database error loading directory rows: %v", err)
		return
	}
	for _, en := range results {
		if err := e.stream.write(en); err != nil {
			// The requesting peer closed the stream early: a transient, so debug rather
			// than an admin-emailing warn.
			debug("Directory sync to %q interrupted (peer closed stream): %v", en.Entity, err)
			return
		}
	}
}

// directory_location_age_maximum is how long a directory row may remain
// un-refreshed before a silenced peer's rows get forgotten. Live peers
// re-attest hourly; only a peer that's been dark for the full window gets
// considered.
const directory_location_age_maximum = 14 * 86400 // 14 days

// directory_cleanup_manager runs hourly and forgets peers unreachable for a
// long time. Separate from directory_manager because this sweep is per-peer:
// one dead peer means all its rows and its queue rows go together.
func directory_cleanup_manager() {
	// Stagger the first sweep so it doesn't pile on startup work.
	time.Sleep(5 * time.Minute)
	directory_cleanup_dead_peers()
	for range time.Tick(time.Hour) {
		directory_cleanup_dead_peers()
	}
}

// directory_cleanup_dead_peers forgets peers that are neither self nor
// bootstrap, whose newest `seen` predates directory_location_age_maximum AND
// that peer_is_silent confirms unreachable. Either alone forgets a
// merely-offline peer.
func directory_cleanup_dead_peers() {
	cutoff := now() - directory_location_age_maximum
	ddb := db_open("db/directory.db")
	rows, err := ddb.rows(
		"select peer, max(seen) as latest from entries where peer != ? group by peer having latest < ?",
		net_id, cutoff,
	)
	if err != nil {
		warn("Directory cleanup: entries query: %v", err)
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
		if !peer_is_silent(peer) {
			// Stale `seen` but the silent-cache has not confirmed unreachable - a cold
			// cache after a restart looks the same. Retry next sweep.
			continue
		}
		directory_forget_peer(peer)
	}
}

// directory_forget_peer deletes every trace of `peer` from this host's local
// state. Not permanent: a fresh libp2p connect drives republish and sync, which
// re-populate the rows and addresses.
func directory_forget_peer(peer string) {
	if peer == "" || peer == net_id {
		return
	}
	ddb := db_open("db/directory.db")
	qdb := db_open("db/queue.db")
	pdb := db_open("db/peers.db")

	row_n := count_rows(ddb, "select count(*) from entries where peer=?", peer)
	queue_n := count_rows(qdb, "select count(*) from queue where target=?", peer)
	addr_n := count_rows(pdb, "select count(*) from peers where id=?", peer)

	ddb.exec("delete from entries where peer=?", peer)
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

	info("Directory forgot dead peer %q: %d rows, %d queue rows, %d addresses",
		peer, row_n, queue_n, addr_n)
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
	// directory.db is rebuildable network state: an operator may wipe it
	// and let republish + sync repopulate. Self-heal the schema so a wiped
	// file doesn't strand the server until the next migration.
	db := db_open("db/directory.db")
	db.exec("create table if not exists entries ( entity text not null, peer text not null, name text not null, class text not null, data text not null default '', fingerprint text not null default '', version integer not null default 0, created integer not null, seen integer not null, message text not null default '', expires text not null default '', signature text not null default '', primary key ( entity, peer ) )")
	db.exec("create index if not exists entries_name on entries( name )")
	db.exec("create index if not exists entries_class on entries( class )")
	db.exec("create index if not exists entries_fingerprint on entries( fingerprint )")
	db.exec("create index if not exists entries_peer on entries( peer )")
	db.exec("create index if not exists entries_seen on entries( seen )")
	db.exec("create index if not exists entries_created on entries( created )")

	time.Sleep(3 * time.Second)

	// Republish every local public entity before the first sync. After a
	// schema rebuild (db_upgrade_80) or a wiped directory this is what
	// repopulates the network's rows for this host; the queued broadcast
	// survives peers that aren't up yet.
	users := db_open("db/users.db")
	var locals []Entity
	if err := users.scans(&locals, "select * from entities where privacy='public'"); err == nil {
		for _, e := range locals {
			directory_create(&e)
			directory_publish(&e, true)
			// A tight burst overflows gossipsub's per-peer outbound queue
			// and the excess is silently dropped; spread the broadcasts.
			time.Sleep(50 * time.Millisecond)
		}
	}

	directory_sync()

	// Zero, not now(): a host restarted daily would never reach the 24h mark and
	// never clean at all. The sweep is idempotent and cheap.
	cleanup := int64(0)
	for range time.Tick(5 * time.Minute) {
		directory_sync()
		if now()-cleanup > 24*60*60 {
			cleanup = now()
			debug("Directory deleting stale entries")
			db := db_open("db/directory.db")
			db.exec("delete from entries where seen<?", now()-30*86400)

			// Withdraw rows for local entities that no longer exist.
			rows, _ := db.rows("select entity from entries where peer=?", net_id)
			for _, row := range rows {
				id := row["entity"].(string)
				exists, _ := users.exists("select 1 from entities where id=?", id)
				if !exists {
					debug("Directory withdrawing orphaned local row %q", id)
					entry_delete_self(id)
				}
			}

			// Owner-authoritative purge: clones and backups sign valid rows for our
			// entities, so ownership decides. Clears any stored before entry_store began
			// refusing them.
			rows, _ = db.rows("select distinct entity from entries where peer<>?", net_id)
			for _, row := range rows {
				id := row["entity"].(string)
				if local, ok := entity_local(id); ok && local {
					debug("Directory dropping foreign rows for locally-owned %q", id)
					db.exec("delete from entries where entity=? and peer<>?", id, net_id)
				}
			}
		}
	}
}

// entry_legacy maps a row to the dictionary shape mochi.directory.get and
// mochi.directory.search have always returned — apps depend on these keys,
// including parsing the "p2p/" location prefix.
func entry_legacy(row map[string]any) map[string]any {
	entity, _ := row["entity"].(string)
	peer, _ := row["peer"].(string)
	fp, _ := row["fingerprint"].(string)
	if fp == "" {
		fp = fingerprint(entity)
	}
	return map[string]any{
		"id":                  entity,
		"name":                row["name"],
		"class":               row["class"],
		"location":            "p2p/" + peer,
		"data":                row["data"],
		"fingerprint":         fp,
		"fingerprint_hyphens": fingerprint_hyphens(fp),
		"created":             row["created"],
		"updated":             row["seen"],
		"version":             row["version"],
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
	d, err := db.row("select * from entries where entity=? order by version desc, seen desc limit 1", id)
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}
	if d == nil {
		return sl.None, nil
	}
	return sl_encode(entry_legacy(d)), nil
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

	u := principal_caller(t)

	// One result per entity: the row with the newest content, freshest
	// claim breaking ties.
	db := db_open("db/directory.db")
	var rows []map[string]any
	var err error
	if fp_search != "" {
		rows, err = db.rows("select * from (select *, row_number() over (partition by entity order by version desc, seen desc) ranked from entries where class=? and fingerprint=?) where ranked=1 order by name, created", class, fp_search)
	} else {
		rows, err = db.rows("select * from (select *, row_number() over (partition by entity order by version desc, seen desc) ranked from entries where class=? and name like ? escape '\\') where ranked=1 order by name, created", class, "%"+like_escape(search)+"%")
	}
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}

	ds := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		ds = append(ds, entry_legacy(row))
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

// Mochi server: Directory
//
// One row per (entity, peer): each row is one host's listing of one entity,
// asserted by that host alone. There are no global rows — a host may only
// publish or delete rows naming itself, so no key holder can suppress
// another host's listing or de-list an entity network-wide. Account
// deletion converges per host: each host deletes its own row as it purges,
// the same way the data layer converges.
//
// Rows are self-verifying. `signature` is the entity's ed25519 signature
// over the content facts (the entity id IS the public key); `attestation`
// is the asserting host's libp2p-key signature over the claim (the peer id
// self-certifies). Receivers verify both from the payload, so trust never
// depends on the arrival path — pubsub relays, the sync stream, and
// bootstrap peers are all untrusted carriers.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"crypto/ed25519"
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
	Signature   string
	Attestation string
}

// Domain separators for the three signables. Any schema change MUST bump
// the corresponding domain, same rule as pubsub_domain.
const (
	entry_domain        = "mochi/2/entry"
	entry_attest_domain = "mochi/2/entry/attest"
	entry_delete_domain = "mochi/2/entry/delete"
)

var api_directory = sls.FromStringDict(sl.String("mochi.directory"), sl.StringDict{
	"get":    sl.NewBuiltin("mochi.directory.get", api_directory_get),
	"search": sl.NewBuiltin("mochi.directory.search", api_directory_search),
})

func init() {
	a := app("directory")
	a.service("directory")
	// All payloads are self-verifying; the message envelope is anonymous.
	a.event_anonymous("publish", directory_publish_event)
	a.event_anonymous("delete", directory_delete_event)
	a.event_anonymous("request", directory_request_event)
	a.event_anonymous("sync", directory_sync_event)
	a.event_anonymous("push", directory_push_event)
}

// entry_signable returns the canonical CBOR the entity signs over a row's
// content facts. Excludes peer/seen: content is host-independent, so every
// host serving the entity carries the same signature for the same version.
func entry_signable(entity, name, class, data string, version int64) ([]byte, error) {
	return canonical_encoder.Marshal(map[string]any{
		"v":       entry_domain,
		"entity":  entity,
		"name":    name,
		"class":   class,
		"data":    data,
		"version": i64toa(version),
	})
}

// entry_sign produces the content signature with the entity's key.
// Empty on failure (entity not local or key unavailable).
func entry_sign(entity, name, class, data string, version int64) string {
	signable, err := entry_signable(entity, name, class, data, version)
	if err != nil {
		warn("Directory entry canonical encode failed for %q: %v", entity, err)
		return ""
	}
	return entity_sign(entity, string(signable))
}

// entry_verify checks a row's content signature against the entity id,
// which is the base58 ed25519 public key.
func entry_verify(en *Entry) bool {
	public := base58_decode(en.Entity, "")
	if len(public) != ed25519.PublicKeySize {
		return false
	}
	sig := base58_decode(en.Signature, "")
	if len(sig) != ed25519.SignatureSize {
		return false
	}
	signable, err := entry_signable(en.Entity, en.Name, en.Class, en.Data, en.Version)
	if err != nil {
		return false
	}
	return ed25519.Verify(public, signable, sig)
}

// entry_attest_signable returns the canonical CBOR the asserting host signs
// over a row's claim. Includes version (a relay can't graft a fresh
// attestation onto stale content) and created (the anti-impersonation
// ordering must not ride unsigned).
func entry_attest_signable(entity, peer string, version, created, seen int64) ([]byte, error) {
	return canonical_encoder.Marshal(map[string]any{
		"v":       entry_attest_domain,
		"entity":  entity,
		"peer":    peer,
		"version": i64toa(version),
		"created": i64toa(created),
		"seen":    i64toa(seen),
	})
}

// entry_attest produces this host's claim attestation for a row.
func entry_attest(entity string, version, created, seen int64) string {
	signable, err := entry_attest_signable(entity, net_id, version, created, seen)
	if err != nil {
		warn("Directory attestation canonical encode failed for %q: %v", entity, err)
		return ""
	}
	return base58_encode(server_sign(signable))
}

// entry_attest_verify checks a row's claim attestation against the peer id
// it names.
func entry_attest_verify(en *Entry) bool {
	signable, err := entry_attest_signable(en.Entity, en.Peer, en.Version, en.Created, en.Seen)
	if err != nil {
		return false
	}
	return server_verify(en.Peer, signable, base58_decode(en.Attestation, ""))
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
	if !valid(en.Entity, "entity") || !valid(en.Name, "line") || !valid(en.Class, "constant") || !valid(en.Data, "text") {
		debug("Directory dropping invalid row for %q from %s", en.Entity, source)
		return false
	}
	if en.Version <= 0 || en.Created <= 0 || en.Seen <= 0 || en.Seen > now()+3600 {
		debug("Directory dropping row with bad timestamps for %q from %s", en.Entity, source)
		return false
	}
	// This host is authoritative for its own rows; they are rebuilt only by
	// directory_create. A replayed copy must not override local state — but
	// it is evidence: a row naming this host for an entity that no longer
	// exists locally is a pre-wipe ghost other servers still hold, invisible
	// to the daily orphan sweep (which scans only the local table), and only
	// this host's key can withdraw it. Answer the echo with a deletion. The
	// existence check protects every live entity, and a row naming this peer
	// cannot be forged (it carries this host's attestation), so the echo is
	// always a claim a previous incarnation of this server really made.
	if en.Peer == net_id {
		users := db_open("db/users.db")
		exists, _ := users.exists("select 1 from entities where id=?", en.Entity)
		if !exists && rate_limit_entry_withdraw.allow(en.Entity) {
			info("Directory withdrawing ghost row for %q echoed by %s: entity no longer exists here", en.Entity, source)
			entry_delete_self(en.Entity)
		}
		return false
	}
	// Owner-authoritative: this host is the single home of the entities in
	// its users.db, so a row naming a DIFFERENT peer for a locally-owned
	// entity is stale — a restored backup, a cloned test instance, or a
	// pre-migration host. A clone holds the entity's keys, so its rows
	// VERIFY; ownership, not the signature, is what makes them wrong.
	// Storing one offers delivery fan-out a foreign route for a local
	// subscriber (the 2026-07-06 News feed wedge trigger class). The
	// fail-safe entity_local refuses the store when the check itself
	// errors; the row is re-gossiped later.
	if local, ok := entity_local(en.Entity); !ok || local {
		if local {
			debug("Directory dropping foreign row for locally-owned %q (peer=%q) from %s", en.Entity, en.Peer, source)
		}
		return false
	}
	if !entry_verify(en) {
		info("Directory dropping row with bad content signature: entity=%q peer=%q from %s", en.Entity, en.Peer, source)
		return false
	}
	if !entry_attest_verify(en) {
		info("Directory dropping row with bad attestation: entity=%q peer=%q from %s", en.Entity, en.Peer, source)
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
	db.exec("replace into entries (entity, peer, name, class, data, fingerprint, version, created, seen, signature, attestation) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		en.Entity, en.Peer, en.Name, en.Class, en.Data, fingerprint(en.Entity), en.Version, en.Created, en.Seen, en.Signature, en.Attestation)

	go queue_check_entity(en.Entity)
	return true
}

// directory_create builds or refreshes this host's row for a local entity.
// Unchanged content keeps its version and signature and re-issues only the
// attestation with a fresh seen — the cheap hourly heartbeat. Changed
// content takes version = now() and a new content signature; a rename also
// resets created, so an impersonator can't inherit an old entity's seniority
// in search ordering.
func directory_create(e *Entity) {
	debug("Directory creating entry %q %q", e.ID, e.Name)
	now := now()

	db := db_open("db/directory.db")
	var existing Entry
	have := db.scan(&existing, "select * from entries where entity=? and peer=?", e.ID, net_id)

	version := now
	created := now
	signature := ""
	if have && existing.Name == e.Name && existing.Class == e.Class && existing.Data == e.Data {
		version = existing.Version
		created = existing.Created
		signature = existing.Signature
	} else if have && existing.Name == e.Name {
		created = existing.Created
	}
	if signature == "" {
		signature = entry_sign(e.ID, e.Name, e.Class, e.Data, version)
		if signature == "" {
			warn("Directory unable to sign entry for %q", e.ID)
			return
		}
	}

	db.exec("replace into entries (entity, peer, name, class, data, fingerprint, version, created, seen, signature, attestation) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		e.ID, net_id, e.Name, e.Class, e.Data, fingerprint(e.ID), version, created, now, signature, entry_attest(e.ID, version, created, now))
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

	m := message("", "", "directory", "publish")
	m.set("entity", en.Entity, "peer", en.Peer, "name", en.Name, "class", en.Class, "data", en.Data,
		"version", i64toa(en.Version), "created", i64toa(en.Created), "seen", i64toa(en.Seen),
		"signature", en.Signature, "attestation", en.Attestation)
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
	en := Entry{
		Entity:      e.get("entity", ""),
		Peer:        e.get("peer", ""),
		Name:        e.get("name", ""),
		Class:       e.get("class", ""),
		Data:        e.get("data", ""),
		Version:     atoi(e.get("version", ""), 0),
		Created:     atoi(e.get("created", ""), 0),
		Seen:        atoi(e.get("seen", ""), 0),
		Signature:   e.get("signature", ""),
		Attestation: e.get("attestation", ""),
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

// directory_push_watermark tracks, per sync peer, the highest self-row
// `seen` already delivered over a push stream, so only rows re-attested
// since the last successful push are sent — steady-state one push per
// hourly re-attest cycle, not one per 5-minute sync tick. In-memory by
// design: a restart repeats one full push, which the receiver's
// entry_store ordering rules dedup; and because every self-row's seen
// advances on the hourly re-attest, a receiver that lost rows (wiped
// directory) is made whole within an hour regardless of the watermark.
// Touched only from the directory_manager goroutine.
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

// directory_push_to_peer delivers this host's own rows to one sync peer
// over a stream. Pubsub republish remains the low-latency path, but a
// republish burst larger than gossipsub's per-peer outbound queue is
// silently truncated (observed live: only ~40 of a 154-row burst
// survived), so correctness rides on this reliable push to the same
// peers directory_sync pulls from; the rest of the fleet picks the rows
// up from there.
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

// Receive a directory push: a peer delivering rows over a stream. Each
// row passes the same verification gate as a live publish, so the worst
// a malicious pusher can do is deliver valid rows; the sender is just a
// carrier.
func directory_push_event(e *Event) {
	stored := 0
	for {
		var en Entry
		if err := e.stream.read(&en); err != nil {
			debug("Directory push from peer %q finished: %d rows stored", e.peer, stored)
			return
		}
		if entry_store(&en, "push") {
			stored++
		}
	}
}

// directory_sync_from_peer pulls rows updated since our watermark from one
// peer over /mochi/2/stream. The watermark is max(seen) minus an hour of
// overlap — seen moves on every re-attestation, and entry_store makes
// re-delivery of the overlap idempotent. Every received row passes the same
// verification gate as a live publish; the sender is just a carrier.
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

	var results []Entry
	db := db_open("db/directory.db")
	err := db.scans(&results, "select * from entries where seen>=? order by seen", start)
	if err != nil {
		warn("Database error loading directory rows: %v", err)
		return
	}
	for _, en := range results {
		if err := e.stream.write(en); err != nil {
			// Routine: the peer that requested this sync closed the stream
			// early (it reconnected, already had what it needed, or the
			// connection flapped). Same class of transient as the
			// open-failure and read-EOF paths above, so debug — not an
			// admin-emailing warn.
			debug("Directory sync to %q interrupted (peer closed stream): %v", en.Entity, err)
			return
		}
	}
}

// directory_location_max_age is how long a directory row may remain
// un-refreshed before a silenced peer's rows get forgotten. Live peers
// re-attest hourly; only a peer that's been dark for the full window gets
// considered.
const directory_location_max_age = 14 * 86400 // 14 days

// directory_cleanup_manager runs once per hour and forgets peers that have
// been demonstrably unreachable for a long time. See directory_forget_peer
// for the cleanup action and directory_cleanup_dead_peers for the selection
// criteria.
//
// Decoupled from directory_manager (which handles sync + a daily row-level
// expiry) because this sweep operates at peer granularity: when ONE peer is
// dead, every row it asserted needs cleaning, and the queue.db rows
// targeting it need clearing in the same step.
func directory_cleanup_manager() {
	// Stagger the first sweep so it doesn't pile on startup work.
	time.Sleep(5 * time.Minute)
	directory_cleanup_dead_peers()
	for range time.Tick(time.Hour) {
		directory_cleanup_dead_peers()
	}
}

// directory_cleanup_dead_peers scans entries grouped by peer and forgets
// peers that meet ALL of the following:
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

// directory_forget_peer deletes every trace of `peer` from this server's
// per-host state: directory rows it asserted, queue rows targeting it,
// peers.db addresses for it, and the in-memory peer/reachability/reconnect
// caches.
//
// Purely local — directory.db, queue.db, and peers.db are per-host state,
// not replicated across pair members, so no leader/coordination is needed.
// The peer can come back later: a fresh libp2p connect drives republish +
// sync, and the rows + peers.db addresses re-populate with current data.
//
// Logs row counts at info so the operator can see what happened.
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
	db.exec("create table if not exists entries ( entity text not null, peer text not null, name text not null, class text not null, data text not null default '', fingerprint text not null default '', version integer not null default 0, created integer not null, seen integer not null, signature text not null default '', attestation text not null default '', primary key ( entity, peer ) )")
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

	// Zero, not now(): the cleanup must run on the FIRST tick after start
	// and then daily. Anchoring it to process start meant a host restarted
	// daily (yuzu, during active release periods) never reached the 24h
	// mark and never cleaned at all — stale-entry expiry, ghost
	// withdrawal, and the owner-authoritative foreign-claim purge all sit
	// behind this gate. The sweep is idempotent and cheap; running it
	// minutes after boot is strictly better than maybe-never.
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

			// Owner-authoritative purge: drop foreign-peer rows for
			// entities this host owns. Clones and restored backups sign
			// valid rows for our entities (they hold the keys); ownership,
			// not the signature, is what makes them wrong. entry_store
			// refuses new ones; this clears any stored before that guard
			// existed (e.g. the 2026-07 failover-drill ghosts).
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

	u := t.Local("user").(*User)

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

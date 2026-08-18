// Mochi server: Entities
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"time"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

type Entity struct {
	ID          string `cbor:"id" json:"id"`
	Private     string `cbor:"-" json:"-"`
	Fingerprint string `cbor:"-" json:"fingerprint"`
	User        string `cbor:"-" json:"-"`
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
	"sign":        sl.NewBuiltin("mochi.entity.sign", api_entity_sign),
	"update":      sl.NewBuiltin("mochi.entity.update", api_entity_update),
	"verify":      sl.NewBuiltin("mochi.entity.verify", api_entity_verify),
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
	user_exists, _ := db.exists("select uid from users where uid=?", u.UID)
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

	db.exec("replace into entities ( id, private, fingerprint, user, parent, class, name, privacy, data, published ) values ( ?, ?, ?, ?, ?, ?, ?, ?, ?, 0 )", public, private, fingerprint, u.UID, parent, class, name, privacy, data)

	e := Entity{ID: public, Private: private, Fingerprint: fingerprint, User: u.UID, Parent: parent, Class: class, Name: name, Privacy: privacy, Data: data, Published: 0}

	// Audit log entity/identity creation
	audit_identity_created(u.Username, public, class)
	audit_key_generated(public, class)

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
			// Hourly: the republish is what refreshes our directory rows'
			// attestation (seen), which peers' freshness tiering and the
			// dead-peer cleanup key on. directory_create keeps the content
			// signature when nothing changed, so this is cheap.
			var es []Entity
			err := db.scans(&es, "select * from entities where privacy='public' and published<?", now()-3600)
			if err != nil {
				warn("Database error loading entities for republish: %v", err)
				continue
			}
			for _, e := range es {
				db.exec("update entities set published=? where id=?", now(), e.ID)
				directory_create(&e)
				directory_publish(&e, false)
				// A tight burst overflows gossipsub's per-peer outbound
				// queue and the excess is silently dropped (observed live:
				// only ~40 of a 154-row burst survived); spread the
				// broadcasts. Reliable delivery to sync peers additionally
				// rides directory_push_to_peer.
				time.Sleep(50 * time.Millisecond)
			}
		}
	}
}

// entity_privacy_set updates an entity's privacy and synchronises the
// directory: removing and broadcasting deletion when going to private,
// republishing when staying or becoming public.
func entity_privacy_set(e *Entity, privacy string) error {
	if privacy != "public" && privacy != "private" {
		return fmt.Errorf("privacy must be 'public' or 'private'")
	}
	if privacy == e.Privacy {
		return nil
	}

	db_open("db/users.db").exec("update entities set privacy=? where id=?", privacy, e.ID)
	e.Privacy = privacy

	if privacy == "private" {
		entry_delete_self(e.ID)
	} else {
		directory_create(e)
		directory_publish(e, true)
	}
	return nil
}

// entity_name_set updates an entity's name and republishes the directory
// row when public. directory_create resets the row's created timestamp on
// rename to prevent impersonation: an attacker can't squat a name early and
// then later rename to it to appear first in search results.
func entity_name_set(e *Entity, name string) error {
	if !valid(name, "name") {
		return fmt.Errorf("invalid name")
	}
	if name == e.Name {
		return nil
	}

	db_open("db/users.db").exec("update entities set name=? where id=?", name, e.ID)
	e.Name = name

	if e.Privacy == "public" {
		directory_create(e)
		directory_publish(e, true)
	}
	return nil
}

// Delete an entity: withdraw this host's directory row (locally and
// network-wide) and remove the entity. The withdrawal is host-key signed,
// so there is no ordering constraint against the entity key's deletion.
func (e *Entity) delete() {
	// Get user for audit logging
	db := db_open("db/users.db")
	row, _ := db.row("select u.username from users u, entities en where en.id=? and en.user=u.uid", e.ID)
	username := ""
	if row != nil {
		username = row["username"].(string)
	}

	entry_delete_self(e.ID)

	// Remove queued messages from/to this entity
	queue := db_open("db/queue.db")
	queue.exec("delete from queue where from_entity=?", e.ID)
	queue.exec("delete from queue where to_entity=?", e.ID)

	// Remove from group memberships
	if e.User != "" {
		udb := db_open(fmt.Sprintf("users/%s/user.db", e.User))
		udb.exec("delete from group_members where member=? and type='user'", e.ID)
	}

	// Remove the entity row.
	db.exec("delete from entities where id=?", e.ID)

	// Audit log entity deletion
	audit_identity_deleted(username, e.ID)
}

// delete_local removes this entity's local rows WITHOUT broadcasting the
// directory tombstone or replicating the deletion. Used when this host stops
// hosting the user (leave-set / "remove my account from this server") while
// the account and its entities survive on other hosts: only THIS host's
// directory row is withdrawn (locally and network-wide); the other hosts'
// rows keep the entity resolvable.
func (e *Entity) delete_local() {
	db := db_open("db/users.db")
	row, _ := db.row("select u.username from users u, entities en where en.id=? and en.user=u.uid", e.ID)
	username := ""
	if row != nil {
		username, _ = row["username"].(string)
	}

	queue := db_open("db/queue.db")
	queue.exec("delete from queue where from_entity=?", e.ID)
	queue.exec("delete from queue where to_entity=?", e.ID)

	if e.User != "" {
		udb := db_open(fmt.Sprintf("users/%s/user.db", e.User))
		udb.exec("delete from group_members where member=? and type='user'", e.ID)
	}

	entry_delete_self(e.ID)
	db.exec("delete from entities where id=?", e.ID)

	audit_identity_deleted(username, e.ID)
}

// Get the peer an entity is at
func entity_peer(id string) string {
	// Check if local
	var e Entity
	if db_open("db/users.db").scan(&e, "select * from entities where id=?", id) {
		return net_id
	}

	// Check the directory for an active peer claim. `seen` ages out via
	// the 30-day cleanup in directory_manager.
	row, _ := db_open("db/directory.db").row("select peer from entries where entity=? and peer!=? and seen > ? order by seen desc limit 1", id, net_id, now()-30*86400)
	if row != nil {
		if peer, ok := row["peer"].(string); ok && peer != "" {
			return peer
		}
	}

	// Not found. Send a directory request and return failure.
	m := message("", "", "directory", "request")
	m.set("entity", id)
	m.publish(false)
	return ""
}

// entity_local reports whether `id` is owned by a user on this host, with
// ok=false when the check itself failed. Callers MUST fail their resolution
// on !ok rather than fall through to remote routes: a transient users.db
// error swallowed here silently demotes a locally-owned entity to remote
// resolution, sending its events to whatever stale peer the directory still
// advertises — and for sequenced broadcasts, permanently skipping those
// sequences on the real, local subscriber (the probable trigger of the
// 2026-07-06 News feed wedge: a ~16-event window that never healed).
func entity_local(id string) (local bool, ok bool) {
	exists, err := db_open("db/users.db").exists("select 1 from entities where id=?", id)
	if err != nil {
		info("Entity resolution: ownership check failed for %q: %v", id, err)
		return false, false
	}
	return exists, true
}

// entity_peers returns every active peer hosting `id`, ordered most-recent
// first. Empty for local entities or unknown ones (the caller should fall
// back to a broadcast directory request).
func entity_peers(id string) []string {
	local, ok := entity_local(id)
	if !ok {
		return nil
	}
	if local {
		return []string{net_id}
	}
	rows, _ := db_open("db/directory.db").rows("select peer from entries where entity=? and peer!=? and seen > ? order by seen desc", id, net_id, now()-30*86400)
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		if peer, ok := r["peer"].(string); ok && peer != "" {
			out = append(out, peer)
		}
	}
	return out
}

// entity_peers_for is entity_peers plus the sending user's learned
// directory, for delivery to private entities (never directory-listed).
// `from` is the sending entity; its owner's directory is consulted. The
// merge is ordered freshest-seen first across both sources, deduplicated
// per peer — so a stale public entry loses to yesterday's verified
// contact, and vice versa. User rows carry no age filter (see
// directory_user.go). Sends with no resolvable owner (system, anonymous)
// degrade to the public directory alone.
func entity_peers_for(from string, id string) []string {
	local, ok := entity_local(id)
	if !ok {
		return nil // check failed — never fall through to remote routes
	}
	if local {
		return []string{net_id}
	}
	type located struct {
		peer string
		seen int64
	}
	candidates := []located{}
	rows, _ := db_open("db/directory.db").rows("select peer, seen from entries where entity=? and peer!=? and seen > ? order by seen desc", id, net_id, now()-30*86400)
	for _, r := range rows {
		if peer, ok := r["peer"].(string); ok && peer != "" {
			candidates = append(candidates, located{peer, row_int(r, "seen")})
		}
	}
	if from != "" {
		if user := user_owning_entity(from); user != nil {
			public := len(candidates)
			for _, r := range directory_user_peers(user, id) {
				if peer, ok := r["peer"].(string); ok && peer != "" {
					candidates = append(candidates, located{peer, row_int(r, "seen")})
				}
			}
			if public == 0 && len(candidates) > 0 {
				debug("Directory user rows resolved %q for %q (not publicly listed)", id, from)
			}
		}
	}
	out := make([]string, 0, len(candidates))
	seen_peer := map[string]bool{}
	for {
		best := -1
		for i, c := range candidates {
			if seen_peer[c.peer] {
				continue
			}
			if best < 0 || c.seen > candidates[best].seen {
				best = i
			}
		}
		if best < 0 {
			break
		}
		seen_peer[candidates[best].peer] = true
		out = append(out, candidates[best].peer)
	}
	return out
}

// entity_peers_failover returns peers hosting `id` ordered for
// stream / RPC failover.
//
// Tier 1 — "active": peers whose `seen` is within 2× the entity
// republish interval (2 hours). They're presumed alive; among them the
// MOST-recently-attested comes first — freshest liveness, hence the most
// likely to be both up and running current code/data. (Preferring the
// oldest `seen` here once steered clients onto a lagging replica that had
// stopped updating; browser affinity is handled by the sticky-session
// cookie, not this order.)
//
// Tier 2 — "stale but not aged out": peers with seen older than the
// active window but still within the 30-day directory retention.
// Used as a last-ditch fallback when no active peer accepts a stream.
//
// Local entity short-circuits to [self].
func entity_peers_failover(id string) []string {
	local, ok := entity_local(id)
	if !ok {
		return nil // check failed — never fall through to remote routes
	}
	if local {
		return []string{net_id}
	}
	db := db_open("db/directory.db")
	now_ts := now()
	active_cutoff := now_ts - directory_active_window
	stale_cutoff := now_ts - 30*86400

	active, _ := db.rows("select peer from entries where entity=? and peer!=? and seen > ? order by seen desc", id, net_id, active_cutoff)
	stale, _ := db.rows("select peer from entries where entity=? and peer!=? and seen > ? and seen <= ? order by seen desc", id, net_id, stale_cutoff, active_cutoff)

	seen_peer := map[string]bool{}
	out := make([]string, 0, len(active)+len(stale))
	for _, r := range append(active, stale...) {
		if p, ok := r["peer"].(string); ok && p != "" && !seen_peer[p] {
			seen_peer[p] = true
			out = append(out, p)
		}
	}
	return out
}

// entity_private_local_foreign reports whether `id` is a PRIVATE entity local
// to this server owned by a DIFFERENT user than the caller `from`. Used to
// gate mochi.remote.ping: a private entity is unlisted precisely so its
// existence is unknowable, but the bare local self-loop let any co-tenant
// confirm one it knew the id of via a liveness probe. Only ping is gated —
// request/stream run the receiving app's handler, which does its own access
// control, so they stay reachable for legitimate cross-user calls (a member's
// access/check on a private forum). The entity's owner is never blocked.
func entity_private_local_foreign(from string, id string) bool {
	ent := entity_by_any(id)
	if ent == nil || ent.Privacy != "private" {
		return false
	}
	if from == "" {
		return true
	}
	sender := entity_by_any(from)
	return sender == nil || sender.User != ent.User
}

// entity_peers_failover_for is entity_peers_failover plus the sending
// user's learned directory as a final tier — for streams and RPC to
// private entities (a subscriber's sync pull from a private project).
// Public tiers first: they carry live attestation the learned rows
// lack. Learned rows are the only lead for a private entity, so they
// are tried regardless of age.
func entity_peers_failover_for(from string, id string) []string {
	// A locally-owned entity's only correct route is the self-loop;
	// appending the caller's learned rows would offer ghost peers that
	// advertised it (drill/clone residue), and remote_reach can pick one
	// when the self-dial doesn't instantly win — a stream to a dead peer
	// returns EOF (the 2026-07-17 "unable to read segment: EOF" viewing a
	// local feed). entity_peers_failover already returns [net_id] here;
	// stop before the learned-row merge. Mirrors the entity_peers_for
	// guard on the delivery path. The fail-safe local check refuses to
	// fall through on a users.db error.
	if local, ok := entity_local(id); !ok || local {
		return entity_peers_failover(id)
	}
	out := entity_peers_failover(id)
	if from == "" {
		return out
	}
	user := user_owning_entity(from)
	if user == nil {
		return out
	}
	seen_peer := map[string]bool{}
	for _, p := range out {
		seen_peer[p] = true
	}
	for _, r := range directory_user_peers(user, id) {
		if peer, ok := r["peer"].(string); ok && peer != "" && !seen_peer[peer] {
			if len(out) == 0 {
				debug("Directory user rows resolved %q for %q (not publicly listed)", id, from)
			}
			out = append(out, peer)
			seen_peer[peer] = true
		}
	}
	return out
}

// directory_active_window is the freshness window used by the
// stream/RPC failover policy. Set to 2× the hourly attestation refresh
// (entities_manager) so a peer that missed one refresh tick still counts
// as active. Peers outside this window fall to the stale-fallback tier.
const directory_active_window = 2 * 60 * 60

// Sign a string using an entity's private key
// entity_domain_application prefixes what mochi.entity.sign produces, so an
// app-minted signature can never validate where core expects one of its own.
// Core signs export manifests (api_user_export.go) and pubsub frames
// (pubsub.go) with the same keys and no tag, and an app can emit those exact
// bytes - without separation the two are indistinguishable.
//
// Deliberately applied to the APP side, not to core's: tagging what core signs
// would change what remote peers verify, which is a wire break. This way core's
// signing is untouched and only mochi.entity.verify has to learn the tag.
const entity_domain_application = "mochi/application/1\n"

// entity_present reports whether an entity row still exists on this host.
//
// Signing fails for two unrelated reasons: the entity is gone, or it is present
// with an unusable key. entity_sign already draws that line - info for the
// first, warn for the second - and only the second is a fault an operator
// should be mailed about. A caller that warns on any nil signature undoes that
// distinction, and an entity deleted while a publish was already in flight
// then mails the operator about a race it handled correctly.
func entity_present(id string) bool {
	if id == "" {
		return false
	}
	have, _ := db_open("db/users.db").exists("select 1 from entities where id=?", id)
	return have
}

func entity_sign(entity string, s string) string {
	if entity == "" {
		return ""
	}

	db := db_open("db/users.db")
	var e Entity
	if !db.scan(&e, "select private from entities where id=?", entity) {
		info("Signature entity %q not found", entity)
		return ""
	}

	private := base58_decode(e.Private, "")
	if string(private) == "" {
		warn("Signature entity %q empty private key", entity)
		return ""
	}

	return base58_encode(ed25519.Sign(private, []byte(s)))
}

// Verify a signature over a string against an entity's public key. An entity id
// IS its base58 ed25519 public key, so no lookup is needed and the entity does
// not have to exist on this host - which is what lets a receiving host check the
// authorship of a replicated object written by someone it has never met.
func entity_verify(entity string, s string, signature string) bool {
	if entity == "" || signature == "" {
		return false
	}

	public := base58_decode(entity, "")
	if len(public) != ed25519.PublicKeySize {
		return false
	}

	sig := base58_decode(signature, "")
	if len(sig) != ed25519.SignatureSize {
		return false
	}

	return ed25519.Verify(ed25519.PublicKey(public), []byte(s), sig)
}

// Starlark methods
func (e *Entity) AttrNames() []string {
	return []string{"id", "name", "privacy"}
}

func (e *Entity) Attr(name string) (sl.Value, error) {
	switch name {
	case "id":
		return sl.String(e.ID), nil
	case "name":
		return sl.String(e.Name), nil
	case "privacy":
		return sl.String(e.Privacy), nil
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

// entity_class_identity is the class of the account's login identity.
const entity_class_identity = "person"

// entity_class_allowed reports whether the calling app may act on this class.
// app_declares_class reads the app's own manifest, so it is a self-assertion -
// any app can add a class to its app.json. For the login identity that is not
// enough: mochi.user.identity.update makes the same mutation on the same row
// behind user/identity/write, so entity.* must not be the ungated way in.
func entity_class_allowed(t *sl.Thread, fn *sl.Builtin, app *App, user *User, class string) error {
	if class == entity_class_identity {
		if err := require_permission(t, fn, "user/identity/write"); err != nil {
			return err
		}
	}
	if !app_declares_class(app, user, class) {
		return fmt.Errorf("app does not control class %q", class)
	}
	return nil
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

	user := principal_caller(t)
	if user == nil {
		return sl_error(fn, "no user")
	}

	app := t.Local("app").(*App)
	if app == nil {
		return sl_error(fn, "no app")
	}
	if err := entity_class_allowed(t, fn, app, user, class); err != nil {
		return sl_error(fn, "%v", err)
	}

	// Charged here rather than in entity_create, so signup's own identity
	// creation (web.go) is not subject to an app's budget, and after the class
	// check, so a call that was going to be refused anyway costs nothing.
	if !rate_limit_entity_create.allow(user.UID) {
		return sl_error(fn, rate_limit_refuse(rate_limit_entity_create, user.UID, "entities created per minute"))
	}

	e, err := entity_create(user, class, name, privacy, data)
	if err != nil {
		return sl_error(fn, "unable to create entity: %v", err)
	}

	return sl_encode(e.ID), nil
}

// mochi.entity.delete(id) -> bool: Delete an entity owned by the current user.
// Accepts either an entity ID or a 9-character fingerprint.
func api_entity_delete(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <id: string>")
	}

	id, ok := sl.AsString(args[0])
	if !ok || (!valid(id, "entity") && !valid(id, "fingerprint")) {
		return sl_error(fn, "invalid id %q", id)
	}

	user := principal_caller(t)
	if user == nil {
		return sl_error(fn, "no user")
	}

	// Verify entity exists and is owned by the current user. Accepts either
	// an entity ID or a 9-character fingerprint.
	db := db_open("db/users.db")
	var e Entity
	if !db.scan(&e, "select * from entities where id=? or fingerprint=?", id, id) {
		return sl_error(fn, "entity not found")
	}
	if e.User != user.UID {
		return sl_error(fn, "not allowed to delete this entity")
	}

	app := t.Local("app").(*App)
	if app == nil {
		return sl_error(fn, "no app")
	}
	if e.Class != "" {
		if err := entity_class_allowed(t, fn, app, user, e.Class); err != nil {
			return sl_error(fn, "%v", err)
		}
	}

	// Delete the entity
	e.delete()
	return sl.True, nil
}

// mochi.entity.fingerprint(id) -> string: Get the 9-character fingerprint of an
// entity. Returns the bare form. For display, slice and join with hyphens at the
// call site: fp[:3] + "-" + fp[3:6] + "-" + fp[6:].
func api_entity_fingerprint(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <id: string>")
	}

	id, ok := sl.AsString(args[0])
	if !ok || !valid(id, "entity") {
		return sl_error(fn, "invalid id %q", id)
	}

	return sl_encode(fingerprint(id)), nil
}

// mochi.entity.sign(id, text) -> string: Sign text with an entity's private key,
// returning a base58 ed25519 signature ("" if the entity has no usable key).
//
// Only entities belonging to the effective user can be signed with, so an app
// cannot mint an assertion in someone else's name. Note that "effective user"
// carries the usual anonymous-runs-as-owner caveat: an anonymous request to a
// public action resolves to the entity owner, so a caller must establish a real
// authenticated user (a.user) before signing anything attributed to a person.
//
// Signatures verify against the entity id alone, because a Mochi entity id IS
// its base58 ed25519 public key. That is what makes person-level authorship
// checkable on a remote host with no key exchange, directory lookup or network
// round trip - see mochi.entity.verify.
func api_entity_sign(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := require_permission(t, fn, "entity/sign"); err != nil {
		return sl_error(fn, "%v", err)
	}

	if len(args) != 2 {
		return sl_error(fn, "syntax: <id: string>, <text: string>")
	}

	id, ok := sl.AsString(args[0])
	if !ok || (!valid(id, "entity") && !valid(id, "fingerprint")) {
		return sl_error(fn, "invalid id %q", id)
	}

	text, ok := sl.AsString(args[1])
	if !ok {
		return sl_error(fn, "invalid text")
	}

	user := principal_caller(t)
	if user == nil {
		return sl_error(fn, "no user")
	}

	db := db_open("db/users.db")
	var e Entity
	if !db.scan(&e, "select * from entities where id=? or fingerprint=?", id, id) {
		return sl_error(fn, "entity not found")
	}
	if e.User != user.UID {
		return sl_error(fn, "not allowed to sign as this entity")
	}

	return sl_encode(entity_sign(e.ID, entity_domain_application+text)), nil
}

// mochi.entity.verify(id, text, signature) -> bool: Check that signature is a
// valid signature over text by the entity id.
//
// Pure and self-contained: an entity id is its base58 ed25519 public key, so
// this needs neither the entity to exist locally nor any network access. That
// is the point - a host receiving a replicated object can check who really
// wrote it without trusting the peer that relayed it.
func api_entity_verify(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 3 {
		return sl_error(fn, "syntax: <id: string>, <text: string>, <signature: string>")
	}

	id, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid id")
	}

	text, ok := sl.AsString(args[1])
	if !ok {
		return sl_error(fn, "invalid text")
	}

	signature, ok := sl.AsString(args[2])
	if !ok {
		return sl_error(fn, "invalid signature")
	}

	// Both forms during the migration: signatures minted before the domain tag
	// existed are still stored in apps (wikis comments) and still arrive from
	// peers running older builds. Drop the untagged arm once those have aged
	// out - it is what lets a core-minted signature be presented here.
	if entity_verify(id, entity_domain_application+text, signature) {
		return sl.True, nil
	}
	return sl.Bool(entity_verify(id, text, signature)), nil
}

// mochi.entity.get(id) -> list: Get an entity owned by the AUTHENTICATED user,
// empty for an anonymous caller. Accepts either an entity ID or a fingerprint.
//
// Every caller uses this to answer "does the caller own this", so it resolves
// the caller and nothing else. It used to ask principal_storage, which answers
// a different question - which per-user database to open - and returns the
// OWNER for an anonymous caller or a domain route carrying a context. Reading a
// storage-routing decision as an identity claim meant that on such a route every
// logged-in visitor was reported as the owner of the owner's entities, and six
// apps inferred ownership from exactly that (feeds/forums owned(), wikis comment
// deletion, publisher's 403 gate, repositories, people). Core keeps the two
// apart everywhere else - access_check takes owner AND user as separate
// arguments - and principal_storage stays as it is for the two consumers that
// genuinely want storage: opening the app database, and resolving attachments.
func api_entity_get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := require_permission_acting(t, fn, "entity/read"); err != nil {
		return sl_error(fn, "%v", err)
	}

	if len(args) != 1 {
		return sl_error(fn, "syntax: <id: string>")
	}

	id, ok := sl.AsString(args[0])
	if !ok || (!valid(id, "entity") && !valid(id, "fingerprint")) {
		return sl_error(fn, "invalid id %q", id)
	}

	// No authenticated caller owns nothing, rather than owning the owner's
	// entities. This is also what makes the anonymous half of the same mistake
	// impossible rather than a convention apps have to remember.
	user := principal_caller(t)
	if user == nil {
		return sl_encode([]any{}), nil
	}

	db := db_open("db/users.db")
	e, err := db.rows("select id, fingerprint, parent, class, name, data, published from entities where (id=? or fingerprint=?) and user=?", id, id, user.UID)
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}

	return sl_encode(e), nil
}

// mochi.entity.name(id) -> string or None: Get the name of any entity (local or directory)
func api_entity_name(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	// Deliberately not behind entity/read, unlike owned, get and info. This
	// resolves one display string for an id the caller already holds, and an
	// entity id is an unguessable ed25519 public key - the same exemption
	// mochi.group.get relies on and groups.go documents. owned enumerates with
	// no argument, get returns the data blob and info returns class, privacy
	// and creator for any local entity; none of those need an id you were
	// already given.
	//
	// It is also the only one of the four reachable with no way to hold the
	// grant: comptroller resolves names inside event_staff_accounts_list, an
	// inbound P2P handler, and comptroller is not a default app, so nothing
	// seeds it a permission and no consent dialog can (see task #135).

	if len(args) != 1 {
		return sl_error(fn, "syntax: <id: string>")
	}

	id, ok := sl.AsString(args[0])
	if !ok || (!valid(id, "entity") && !valid(id, "fingerprint")) {
		return sl_error(fn, "invalid id %q", id)
	}

	// Check local entities first (by id or fingerprint)
	db := db_open("db/users.db")
	row, err := db.row("select name from entities where id=? or fingerprint=?", id, id)
	if err == nil && row != nil {
		if name, ok := row["name"].(string); ok {
			return sl.String(name), nil
		}
	}

	// Check directory (by id or fingerprint), newest content first
	db = db_open("db/directory.db")
	row, err = db.row("select name from entries where entity=? or fingerprint=? order by version desc, seen desc limit 1", id, id)
	if err == nil && row != nil {
		if name, ok := row["name"].(string); ok {
			return sl.String(name), nil
		}
	}

	return sl.None, nil
}

// mochi.entity.info(id) -> dict or None: Get info for any local entity (no user restriction)
// Accepts either an entity ID or a 9-character fingerprint.
// Returns: id, fingerprint, parent, class, name, privacy, creator (owner's identity ID)
func api_entity_info(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := require_permission_acting(t, fn, "entity/read"); err != nil {
		return sl_error(fn, "%v", err)
	}

	if len(args) != 1 {
		return sl_error(fn, "syntax: <id: string>")
	}

	id, ok := sl.AsString(args[0])
	if !ok || (!valid(id, "entity") && !valid(id, "fingerprint")) {
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

	// Resolve user UID to their identity (person entity)
	if e.User != "" {
		db := db_open("db/users.db")
		identity, err := db.row("select id from entities where user=? and class='person' limit 1", e.User)
		if err == nil && identity != nil {
			if identity_id, ok := identity["id"].(string); ok {
				row["creator"] = identity_id
			}
		}
	}

	return sl_encode(row), nil
}

// mochi.entity.owned() -> list: Get all entities owned by the current user
func api_entity_owned(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := require_permission_acting(t, fn, "entity/read"); err != nil {
		return sl_error(fn, "%v", err)
	}

	user := principal_caller(t)
	if user == nil {
		return sl_error(fn, "no user")
	}

	db := db_open("db/users.db")
	entities, err := db.rows("select id, fingerprint, class, name from entities where user=? order by name", user.UID)
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}

	return sl_encode(entities), nil
}

// mochi.entity.update(id, name=..., data=..., privacy=...) -> bool: Update entity fields
func api_entity_update(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <id: string>, [name=string], [data=string], [privacy=string]")
	}

	id, ok := sl.AsString(args[0])
	if !ok || (!valid(id, "entity") && !valid(id, "fingerprint")) {
		return sl_error(fn, "invalid id %q", id)
	}

	// Get user from context
	user := principal_caller(t)
	if user == nil {
		return sl_error(fn, "no user")
	}

	// Verify entity exists and is owned by the current user. Accepts either an
	// entity ID or a 9-character fingerprint as input; normalise to the
	// canonical entity ID so subsequent SQL works against the right key.
	db := db_open("db/users.db")
	var e Entity
	if !db.scan(&e, "select * from entities where id=? or fingerprint=?", id, id) {
		return sl_error(fn, "entity not found")
	}
	id = e.ID
	if e.User != user.UID {
		return sl_error(fn, "not allowed to update this entity")
	}

	app := t.Local("app").(*App)
	if app == nil {
		return sl_error(fn, "no app")
	}
	if e.Class != "" {
		if err := entity_class_allowed(t, fn, app, user, e.Class); err != nil {
			return sl_error(fn, "%v", err)
		}
	}

	old_privacy := e.Privacy
	changed_to_private := false

	// Process kwargs - validate and apply database updates
	for _, kv := range kwargs {
		key := string(kv[0].(sl.String))
		switch key {
		case "name":
			name, ok := sl.AsString(kv[1])
			if !ok || !valid(name, "name") {
				return sl_error(fn, "invalid name %q", name)
			}
			if name != e.Name {
				db.exec("update entities set name=? where id=?", name, id)
			}

		case "data":
			data, ok := sl.AsString(kv[1])
			if !ok || !valid(data, "text") {
				return sl_error(fn, "invalid data %q", data)
			}
			db.exec("update entities set data=? where id=?", data, id)

		case "privacy":
			privacy, ok := sl.AsString(kv[1])
			if !ok || (privacy != "public" && privacy != "private") {
				return sl_error(fn, "privacy must be 'public' or 'private'")
			}
			if privacy != old_privacy {
				db.exec("update entities set privacy=? where id=?", privacy, id)
				if privacy == "private" {
					changed_to_private = true
				}
			}

		default:
			return sl_error(fn, "unknown parameter %q", key)
		}
	}

	// Update directory after all changes are applied
	if changed_to_private {
		entry_delete_self(id)
	} else {
		// Reload entity to get all updated fields
		db.scan(&e, "select * from entities where id=?", id)
		if e.Privacy == "public" {
			// directory_create resets the row's created timestamp on rename
			// (anti-impersonation) and refreshes the attestation.
			directory_create(&e)
			directory_publish(&e, true)
		}
	}

	return sl.True, nil
}

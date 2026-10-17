// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: Matrix rooms
//
// The rooms this server's hosted Matrix accounts are in, kept for all of them
// together in matrix.db (the federation plan, phase 24). A room has its
// version and its current state; each of its events is kept with the state
// before it, a snapshot shared by every event between two state changes; and
// the events at its edge, its forward extremities, are what the next event
// follows. An event from another server is checked as the server-server
// specification says: its signatures and content hash, its auth events
// (fetched where missing), the state before it from its prev events (those
// missing fetched, or the state at them asked for), and then softly against
// the room's current state, so one the room has moved past is kept but
// neither shown nor built on. Where the state before an event comes from
// several branches, or the room's edge has several events, the state is
// resolved by the room version's algorithm.
//
// A hosted account's rooms reach chat as phase 23's conversation events, the
// same a linked Matrix account's sync brings (matrix.go), so chat's handling
// of linked rooms serves hosted ones. What the user does - joining, leaving,
// inviting, sending - is built here as the account, signed with its domain's
// key, and sent to every other server in the room through the queue
// (federation.go).

package main

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/fclient"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

// rooms_version_default is the version of the rooms this server makes. A
// variable so tests can make rooms of every version.
var rooms_version_default = gomatrixserverlib.RoomVersionV10

// rooms_history is how many events of a room's past are fetched on joining it.
const rooms_history = 50

// rooms_tables makes the room store in matrix.db.
func rooms_tables(db *DB) {
	db.exec("create table if not exists rooms (id text not null primary key, version text not null, state integer not null default 0, created integer not null)")
	db.exec("create table if not exists events (id text not null primary key, room text not null, type text not null, key text, membership text not null default '', sender text not null, depth integer not null, body text not null, rejected integer not null default 0, outlier integer not null default 0, redacted integer not null default 0, before integer not null default 0, received integer not null)")
	db.exec("create index if not exists events_room on events(room, depth)")
	db.exec("create table if not exists snapshots (id integer primary key autoincrement, room text not null, state text not null)")
	db.exec("create table if not exists extremities (room text not null, event text not null, primary key (room, event))")
	db.exec("create table if not exists invites (room text not null, user text not null, version text not null, sender text not null, body text not null, state text not null, created integer not null, primary key (room, user))")
	db.exec("create table if not exists directs (user text not null, room text not null, person text not null, primary key (user, room))")
	db.exec("create table if not exists sent (account text not null, key text not null, event text not null, created integer not null, primary key (account, key))")
}

// === State ===

// A rooms_state is a room's state: each state event's id by its type and
// state key.
type rooms_state map[string]string

func rooms_key(kind string, key string) string {
	return kind + "\x00" + key
}

// rooms_lock holds a room while an event is taken into it, so its edge and
// state move one event at a time.
var rooms_locks sync.Map

func rooms_lock(room string) func() {
	value, _ := rooms_locks.LoadOrStore(room, &sync.Mutex{})
	lock := value.(*sync.Mutex)
	lock.Lock()
	return lock.Unlock
}

// rooms_version is a room's version as the store has it, or nil for a room
// this server is not in.
func rooms_version(room string) gomatrixserverlib.IRoomVersion {
	row, _ := federation_database().row("select version from rooms where id=?", room)
	if row == nil {
		return nil
	}
	version, err := gomatrixserverlib.GetRoomVersion(gomatrixserverlib.RoomVersion(row_string(row, "version")))
	if err != nil {
		return nil
	}
	return version
}

// rooms_snapshot loads a stored state.
func rooms_snapshot(id int64) rooms_state {
	state := rooms_state{}
	if id <= 0 {
		return state
	}
	row, _ := federation_database().row("select state from snapshots where id=?", id)
	if row != nil {
		json.Unmarshal([]byte(row_string(row, "state")), &state)
	}
	return state
}

// rooms_snapshot_of is a state's snapshot: one of those given where it is the
// same state, which it is for every event between two state changes, else a
// new one.
func rooms_snapshot_of(room string, state rooms_state, candidates ...int64) int64 {
	for _, id := range candidates {
		if id > 0 && maps.Equal(rooms_snapshot(id), state) {
			return id
		}
	}
	return rooms_snapshot_save(room, state)
}

// rooms_snapshot_save stores a state and answers its id.
func rooms_snapshot_save(room string, state rooms_state) int64 {
	db := federation_database()
	result, err := db.internal.Exec("insert into snapshots (room, state) values (?, ?)", room, json_encode(state))
	if err != nil {
		warn("Matrix room %s: cannot keep a state: %v", room, err)
		return 0
	}
	id, _ := result.LastInsertId()
	return id
}

// rooms_current is a room's current state and its snapshot's id.
func rooms_current(room string) (rooms_state, int64) {
	row, _ := federation_database().row("select state from rooms where id=?", room)
	if row == nil {
		return rooms_state{}, 0
	}
	id := row_int(row, "state")
	return rooms_snapshot(id), id
}

// rooms_events loads stored events by id, those not stored left out.
func rooms_events(ids []string) map[string]gomatrixserverlib.PDU {
	out := map[string]gomatrixserverlib.PDU{}
	db := federation_database()
	for start := 0; start < len(ids); start += 500 {
		batch := ids[start:min(start+500, len(ids))]
		values := make([]any, len(batch))
		for i, id := range batch {
			values[i] = id
		}
		rows, _ := db.rows("select e.id, e.body, e.redacted, r.version from events e join rooms r on r.id=e.room where e.id in ("+strings.TrimSuffix(strings.Repeat("?,", len(batch)), ",")+")", values...)
		for _, row := range rows {
			version, err := gomatrixserverlib.GetRoomVersion(gomatrixserverlib.RoomVersion(row_string(row, "version")))
			if err != nil {
				continue
			}
			event, err := version.NewEventFromTrustedJSONWithEventID(row_string(row, "id"), []byte(row_string(row, "body")), row_int(row, "redacted") != 0)
			if err == nil {
				out[event.EventID()] = event
			}
		}
	}
	return out
}

// rooms_event loads one stored event, or nil.
func rooms_event(id string) gomatrixserverlib.PDU {
	return rooms_events([]string{id})[id]
}

// rooms_list is a state's events.
func rooms_list(state rooms_state) []gomatrixserverlib.PDU {
	ids := make([]string, 0, len(state))
	for _, id := range state {
		ids = append(ids, id)
	}
	loaded := rooms_events(ids)
	out := make([]gomatrixserverlib.PDU, 0, len(loaded))
	for _, id := range ids {
		if event := loaded[id]; event != nil {
			out = append(out, event)
		}
	}
	return out
}

// rooms_rejected says whether a stored event was rejected.
func rooms_rejected(id string) bool {
	found, _ := federation_database().exists("select 1 from events where id=? and rejected=1", id)
	return found
}

// rooms_after is the state after a stored event, or false where the state
// before it is not known.
func rooms_after(id string) (rooms_state, bool) {
	row, _ := federation_database().row("select type, key, rejected, before from events where id=?", id)
	if row == nil || row_int(row, "before") <= 0 {
		return nil, false
	}
	state := rooms_snapshot(row_int(row, "before"))
	if key, ok := row["key"].(string); ok && row_int(row, "rejected") == 0 {
		state[rooms_key(row_string(row, "type"), key)] = id
	}
	return state, true
}

// rooms_chain is the auth chain of events: their auth events and theirs in
// turn, as far as the store has them.
func rooms_chain(events []gomatrixserverlib.PDU) []gomatrixserverlib.PDU {
	seen := map[string]bool{}
	var queue []string
	for _, event := range events {
		queue = append(queue, event.AuthEventIDs()...)
	}
	var out []gomatrixserverlib.PDU
	for len(queue) > 0 {
		var next []string
		for _, id := range queue {
			if !seen[id] {
				seen[id] = true
				next = append(next, id)
			}
		}
		queue = nil
		loaded := rooms_events(next)
		for _, id := range next {
			if event := loaded[id]; event != nil {
				out = append(out, event)
				queue = append(queue, event.AuthEventIDs()...)
			}
		}
	}
	return out
}

// rooms_resolve is the state several branches of a room come to, by the
// room version's resolution algorithm.
func rooms_resolve(version gomatrixserverlib.IRoomVersion, sets []rooms_state) (rooms_state, error) {
	if len(sets) == 0 {
		return rooms_state{}, nil
	}
	same := true
	for _, set := range sets[1:] {
		if !maps.Equal(set, sets[0]) {
			same = false
			break
		}
	}
	if same {
		return maps.Clone(sets[0]), nil
	}
	var lists [][]gomatrixserverlib.PDU
	var all []gomatrixserverlib.PDU
	for _, set := range sets {
		list := rooms_list(set)
		lists = append(lists, list)
		all = append(all, list...)
	}
	resolved, err := gomatrixserverlib.ResolveConflictsNew(version.Version(), lists, rooms_chain(all), federation_sender, rooms_rejected)
	if err != nil {
		return nil, err
	}
	out := rooms_state{}
	for _, event := range resolved {
		if event.StateKey() != nil {
			out[rooms_key(event.Type(), *event.StateKey())] = event.EventID()
		}
	}
	return out, nil
}

// rooms_provider is the part of a state an event's auth needs.
func rooms_provider(state rooms_state, tuples []gomatrixserverlib.StateKeyTuple) ([]gomatrixserverlib.PDU, *gomatrixserverlib.AuthEvents, error) {
	var ids []string
	for _, tuple := range tuples {
		if id := state[rooms_key(tuple.EventType, tuple.StateKey)]; id != "" {
			ids = append(ids, id)
		}
	}
	loaded := rooms_events(ids)
	events := make([]gomatrixserverlib.PDU, 0, len(loaded))
	for _, id := range ids {
		if event := loaded[id]; event != nil {
			events = append(events, event)
		}
	}
	provider, err := gomatrixserverlib.NewAuthEvents(events)
	return events, provider, err
}

// rooms_allowed checks an event against a state.
func rooms_allowed(event gomatrixserverlib.PDU, state rooms_state) error {
	_, provider, err := rooms_provider(state, gomatrixserverlib.StateNeededForAuth([]gomatrixserverlib.PDU{event}).Tuples())
	if err != nil {
		return err
	}
	return gomatrixserverlib.Allowed(event, provider, federation_sender)
}

// rooms_members is who a state names in a room and as what: join, invite,
// leave, ban or knock.
func rooms_members(state rooms_state) map[string]string {
	var ids []any
	for key, id := range state {
		if strings.HasPrefix(key, spec.MRoomMember+"\x00") {
			ids = append(ids, id)
		}
	}
	out := map[string]string{}
	db := federation_database()
	for start := 0; start < len(ids); start += 500 {
		batch := ids[start:min(start+500, len(ids))]
		rows, _ := db.rows("select key, membership from events where id in ("+strings.TrimSuffix(strings.Repeat("?,", len(batch)), ",")+")", batch...)
		for _, row := range rows {
			out[row_string(row, "key")] = row_string(row, "membership")
		}
	}
	return out
}

// rooms_servers are the servers with someone joined in a state, other than
// this one's own domains.
func rooms_servers(states ...rooms_state) []string {
	seen := map[string]bool{}
	var out []string
	for _, state := range states {
		for member, membership := range rooms_members(state) {
			if membership != spec.Join {
				continue
			}
			user, err := spec.NewUserID(member, true)
			if err != nil {
				continue
			}
			domain := string(user.Domain())
			if !seen[domain] && !federation_local(user.Domain()) {
				seen[domain] = true
				out = append(out, domain)
			}
		}
	}
	sort.Strings(out)
	return out
}

// rooms_local is a domain of this server's with someone joined to a room:
// the server name it speaks for the room as, or "".
func rooms_local(room string) spec.ServerName {
	state, _ := rooms_current(room)
	var found []string
	for member, membership := range rooms_members(state) {
		if membership != spec.Join {
			continue
		}
		if user, err := spec.NewUserID(member, true); err == nil && federation_local(user.Domain()) {
			found = append(found, string(user.Domain()))
		}
	}
	if len(found) == 0 {
		return ""
	}
	sort.Strings(found)
	return spec.ServerName(found[0])
}

// rooms_edge is where a room's next event goes: the events it follows, and
// its depth.
func rooms_edge(room string) ([]string, int64) {
	rows, _ := federation_database().rows("select x.event, e.depth from extremities x join events e on e.id=x.event where x.room=? order by e.depth desc limit 20", room)
	var prevs []string
	depth := int64(0)
	for _, row := range rows {
		prevs = append(prevs, row_string(row, "event"))
		depth = max(depth, row_int(row, "depth"))
	}
	sort.Strings(prevs)
	return prevs, depth + 1
}

// === Storing ===

// rooms_keep stores an event: an outlier, kept to check others by and
// outside the room's timeline, or one of the timeline with the state before
// it. An outlier taken into the timeline later is updated.
func rooms_keep(event gomatrixserverlib.PDU, before int64, rejected bool, outlier bool) {
	var key any
	membership := ""
	if event.StateKey() != nil {
		key = *event.StateKey()
		if event.Type() == spec.MRoomMember {
			membership, _ = event.Membership()
		}
	}
	federation_database().exec("insert into events (id, room, type, key, membership, sender, depth, body, rejected, outlier, redacted, before, received) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) on conflict(id) do update set outlier=excluded.outlier, before=excluded.before, rejected=excluded.rejected where events.outlier=1 and excluded.outlier=0",
		event.EventID(), event.RoomID().String(), event.Type(), key, membership, string(event.SenderID()), event.Depth(), string(event.JSON()), rejected, outlier, event.Redacted(), before, now())
}

// rooms_known says whether an event is in a room's timeline, as against
// unknown or kept only as an outlier.
func rooms_known(id string) bool {
	found, _ := federation_database().exists("select 1 from events where id=? and outlier=0", id)
	return found
}

// rooms_point says whether the state after an event is known, so an event
// can follow from it.
func rooms_point(id string) bool {
	found, _ := federation_database().exists("select 1 from events where id=? and before>0", id)
	return found
}

// rooms_settle moves a room's current state to what its edge comes to.
func rooms_settle(version gomatrixserverlib.IRoomVersion, room string) error {
	rows, _ := federation_database().rows("select event from extremities where room=?", room)
	var sets []rooms_state
	for _, row := range rows {
		if after, ok := rooms_after(row_string(row, "event")); ok {
			sets = append(sets, after)
		}
	}
	state, err := rooms_resolve(version, sets)
	if err != nil {
		return err
	}
	current, id := rooms_current(room)
	if !maps.Equal(state, current) || id == 0 {
		id = rooms_snapshot_save(room, state)
	}
	federation_database().exec("update rooms set state=? where id=?", id, room)
	return nil
}

// rooms_advance makes an event the room's edge in place of those it follows.
func rooms_advance(event gomatrixserverlib.PDU) {
	db := federation_database()
	room := event.RoomID().String()
	for _, prev := range event.PrevEventIDs() {
		db.exec("delete from extremities where room=? and event=?", room, prev)
	}
	db.exec("insert or ignore into extremities (room, event) values (?, ?)", room, event.EventID())
}

// rooms_redact applies a redaction the room accepted to the event it names,
// where that event is in the same room.
func rooms_redact(version gomatrixserverlib.IRoomVersion, redaction gomatrixserverlib.PDU) {
	target := redaction.Redacts()
	if target == "" {
		var content struct {
			Redacts string `json:"redacts"`
		}
		json.Unmarshal(redaction.Content(), &content)
		target = content.Redacts
	}
	event := rooms_event(target)
	if event == nil || event.RoomID().String() != redaction.RoomID().String() || event.Redacted() {
		return
	}
	redacted, err := version.RedactEventJSON(event.JSON())
	if err != nil {
		return
	}
	federation_database().exec("update events set body=?, redacted=1 where id=?", string(redacted), target)
}

// === Receiving ===

// A rooms_outbox collects what the engine tells each account's apps, sent
// once the room is no longer held, since an app may answer by acting in the
// room. Apps are told before the function that acted returns: an app's
// action does not hold its database while it waits, and so a conversation a
// join makes is the app's before the join answers.
type rooms_outbox struct {
	order  []string
	users  map[string]*User
	events map[string][]map[string]any
}

func (o *rooms_outbox) add(user *User, account string, events ...map[string]any) {
	if len(events) == 0 {
		return
	}
	if o.users == nil {
		o.users = map[string]*User{}
		o.events = map[string][]map[string]any{}
	}
	if _, found := o.users[account]; !found {
		o.order = append(o.order, account)
		o.users[account] = user
	}
	o.events[account] = append(o.events[account], events...)
}

func (o *rooms_outbox) flush() {
	for _, account := range o.order {
		linked_inbound(o.users[account], account, "matrix", o.events[account])
	}
	o.order = nil
	o.users = nil
	o.events = nil
}

// rooms_receive takes an event another server sent in a transaction, and
// answers its id and why it was refused. An event of a room this server is
// not in has no version to read it by, and is passed over.
func rooms_receive(ctx context.Context, origin spec.ServerName, raw json.RawMessage) (string, error) {
	var head struct {
		Room string `json:"room_id"`
	}
	if json.Unmarshal(raw, &head) != nil || head.Room == "" {
		return "", nil
	}
	version := rooms_version(head.Room)
	if version == nil {
		return "", nil
	}
	event, err := version.NewEventFromUntrustedJSON(raw)
	if err != nil {
		return "", nil
	}
	if rooms_known(event.EventID()) {
		return event.EventID(), nil
	}
	if err := gomatrixserverlib.VerifyEventSignatures(ctx, event, federation_keyring(), federation_sender); err != nil {
		return event.EventID(), fmt.Errorf("signature: %w", err)
	}
	local := rooms_local(head.Room)
	if local == "" {
		return event.EventID(), fmt.Errorf("no one here is in the room")
	}
	var out rooms_outbox
	unlock := rooms_lock(head.Room)
	err = rooms_accept(ctx, local, origin, version, event, &out, true)
	unlock()
	out.flush()
	return event.EventID(), err
}

// rooms_auth_ids are the events an event is authorised by: its auth events,
// and from version 12, where a room's id is its create event's, that event,
// which is no longer named among them.
func rooms_auth_ids(version gomatrixserverlib.IRoomVersion, event gomatrixserverlib.PDU) []string {
	ids := event.AuthEventIDs()
	if version.DomainlessRoomIDs() && !(event.Type() == spec.MRoomCreate && event.StateKeyEquals("")) {
		create := "$" + strings.TrimPrefix(event.RoomID().String(), "!")
		if !slices.Contains(ids, create) {
			ids = append(slices.Clone(ids), create)
		}
	}
	return ids
}

// rooms_auth makes sure an event's auth events are stored, fetching its auth
// chain from the server it came from where they are not, and answers them.
func rooms_auth(ctx context.Context, local spec.ServerName, origin spec.ServerName, version gomatrixserverlib.IRoomVersion, event gomatrixserverlib.PDU) ([]gomatrixserverlib.PDU, error) {
	ids := rooms_auth_ids(version, event)
	have := rooms_events(ids)
	if len(have) < len(ids) {
		if origin == "" || federation_local(origin) {
			return nil, fmt.Errorf("auth events missing")
		}
		chain, err := federation_client().GetEventAuth(ctx, local, origin, version.Version(), event.RoomID().String(), event.EventID())
		if err != nil {
			return nil, fmt.Errorf("auth events: %w", err)
		}
		rooms_outliers(ctx, version, chain.AuthEvents.UntrustedEvents(version.Version()))
		have = rooms_events(ids)
		if len(have) < len(ids) {
			return nil, fmt.Errorf("auth events missing")
		}
	}
	out := make([]gomatrixserverlib.PDU, 0, len(ids))
	for _, id := range ids {
		out = append(out, have[id])
	}
	return out, nil
}

// rooms_outliers stores events fetched to check others by: each with good
// signatures, in auth order, rejected where its own auth events do not allow
// it.
func rooms_outliers(ctx context.Context, version gomatrixserverlib.IRoomVersion, events []gomatrixserverlib.PDU) map[string]bool {
	failures := gomatrixserverlib.VerifyAllEventSignatures(ctx, events, federation_keyring(), federation_sender)
	good := make([]gomatrixserverlib.PDU, 0, len(events))
	for i, event := range events {
		if failures[i] == nil && event.Version() == version.Version() {
			good = append(good, event)
		}
	}
	kept := map[string]bool{}
	for _, event := range gomatrixserverlib.ReverseTopologicalOrdering(good, gomatrixserverlib.TopologicalOrderByAuthEvents) {
		if stored := rooms_event(event.EventID()); stored != nil {
			kept[event.EventID()] = !rooms_rejected(event.EventID())
			continue
		}
		ids := rooms_auth_ids(version, event)
		auth := rooms_events(ids)
		rejected := len(auth) < len(ids)
		if !rejected {
			list := make([]gomatrixserverlib.PDU, 0, len(ids))
			for _, id := range ids {
				list = append(list, auth[id])
			}
			provider, err := gomatrixserverlib.NewAuthEvents(list)
			rejected = err != nil || gomatrixserverlib.Allowed(event, provider, federation_sender) != nil
		}
		rooms_keep(event, 0, rejected, true)
		kept[event.EventID()] = !rejected
	}
	return kept
}

// rooms_accept takes a checked event into a room's timeline: the state before
// it from its prev events, those missing fetched; its auth checked against
// that state; and then, softly, against the room's current state, so an event
// the room has moved past is kept but not built on or shown. The room is
// held by the caller.
func rooms_accept(ctx context.Context, local spec.ServerName, origin spec.ServerName, version gomatrixserverlib.IRoomVersion, event gomatrixserverlib.PDU, out *rooms_outbox, fill bool) error {
	room := event.RoomID().String()
	if rooms_known(event.EventID()) {
		return nil
	}
	auth, err := rooms_auth(ctx, local, origin, version, event)
	if err != nil {
		return err
	}
	provider, err := gomatrixserverlib.NewAuthEvents(auth)
	if err == nil {
		err = gomatrixserverlib.Allowed(event, provider, federation_sender)
	}
	if err != nil {
		rooms_keep(event, 0, true, false)
		return fmt.Errorf("not allowed by its auth events: %w", err)
	}

	// The events it follows, those missing asked for, and where they cannot
	// all be had, the state at each one still missing.
	missing := func() []string {
		var list []string
		for _, prev := range event.PrevEventIDs() {
			if !rooms_point(prev) {
				list = append(list, prev)
			}
		}
		return list
	}
	if len(missing()) > 0 && fill && origin != "" && !federation_local(origin) {
		rooms_fill(ctx, local, origin, version, event, out)
	}
	for _, prev := range missing() {
		if origin == "" || federation_local(origin) {
			return fmt.Errorf("prev event %s missing", prev)
		}
		if err := rooms_gap(ctx, local, origin, version, room, prev); err != nil {
			return fmt.Errorf("prev event %s: %w", prev, err)
		}
	}

	var sets []rooms_state
	var candidates []int64
	for _, prev := range event.PrevEventIDs() {
		if after, ok := rooms_after(prev); ok {
			sets = append(sets, after)
		}
		if row, _ := federation_database().row("select before from events where id=?", prev); row != nil {
			candidates = append(candidates, row_int(row, "before"))
		}
	}
	before, err := rooms_resolve(version, sets)
	if err != nil {
		return err
	}
	current, id := rooms_current(room)
	snapshot := rooms_snapshot_of(room, before, append(candidates, id)...)
	if err := rooms_allowed(event, before); err != nil {
		rooms_keep(event, snapshot, true, false)
		return fmt.Errorf("not allowed by the state before it: %w", err)
	}
	if err := rooms_allowed(event, current); err != nil {
		rooms_keep(event, snapshot, false, true)
		debug("Matrix room %s: event %s soft-failed: %v", room, event.EventID(), err)
		return nil
	}
	rooms_keep(event, snapshot, false, false)
	rooms_advance(event)
	if err := rooms_settle(version, room); err != nil {
		return err
	}
	if event.Type() == spec.MRoomRedaction {
		rooms_redact(version, event)
	}
	rooms_tell(event, out)
	return nil
}

// rooms_fill asks the server an event came from for the events between the
// room's edge and it, and takes each in, oldest first.
func rooms_fill(ctx context.Context, local spec.ServerName, origin spec.ServerName, version gomatrixserverlib.IRoomVersion, event gomatrixserverlib.PDU, out *rooms_outbox) {
	room := event.RoomID().String()
	edge, _ := rooms_edge(room)
	answer, err := federation_client().LookupMissingEvents(ctx, local, origin, room, fclient.MissingEvents{Limit: 20, EarliestEvents: edge, LatestEvents: []string{event.EventID()}}, version.Version())
	if err != nil {
		debug("Matrix room %s: missing events from %s: %v", room, origin, err)
		return
	}
	events := answer.Events.UntrustedEvents(version.Version())
	failures := gomatrixserverlib.VerifyAllEventSignatures(ctx, events, federation_keyring(), federation_sender)
	var good []gomatrixserverlib.PDU
	for i, e := range events {
		if failures[i] == nil && e.RoomID().String() == room && e.EventID() != event.EventID() {
			good = append(good, e)
		}
	}
	for _, e := range gomatrixserverlib.ReverseTopologicalOrdering(good, gomatrixserverlib.TopologicalOrderByPrevEvents) {
		if err := rooms_accept(ctx, local, origin, version, e, out, false); err != nil {
			debug("Matrix room %s: missing event %s: %v", room, e.EventID(), err)
		}
	}
}

// rooms_gap makes a prev event no one could send a point to follow from: the
// state at it asked for, and it stored outside the timeline with that state.
func rooms_gap(ctx context.Context, local spec.ServerName, origin spec.ServerName, version gomatrixserverlib.IRoomVersion, room string, prev string) error {
	client := federation_client()
	answer, err := client.LookupState(ctx, local, origin, room, prev, version.Version())
	if err != nil {
		return err
	}
	events := append(answer.AuthEvents.UntrustedEvents(version.Version()), answer.StateEvents.UntrustedEvents(version.Version())...)
	kept := rooms_outliers(ctx, version, events)
	state := rooms_state{}
	for _, e := range answer.StateEvents.UntrustedEvents(version.Version()) {
		if kept[e.EventID()] && e.StateKey() != nil && e.RoomID().String() == room {
			state[rooms_key(e.Type(), *e.StateKey())] = e.EventID()
		}
	}
	if _, found := state[rooms_key(spec.MRoomCreate, "")]; !found {
		return fmt.Errorf("the state has no create event")
	}
	transaction, err := client.GetEvent(ctx, local, origin, prev)
	if err != nil || len(transaction.PDUs) == 0 {
		return fmt.Errorf("cannot fetch it: %v", err)
	}
	event, err := version.NewEventFromUntrustedJSON(transaction.PDUs[0])
	if err != nil || event.EventID() != prev || event.RoomID().String() != room {
		return fmt.Errorf("not the event asked for")
	}
	if err := gomatrixserverlib.VerifyEventSignatures(ctx, event, federation_keyring(), federation_sender); err != nil {
		return err
	}
	if _, err := rooms_auth(ctx, local, origin, version, event); err != nil {
		return err
	}
	rejected := rooms_allowed(event, state) != nil
	snapshot := rooms_snapshot_save(room, state)
	rooms_keep(event, snapshot, rejected, true)
	federation_database().exec("update events set before=? where id=? and before=0", snapshot, prev)
	return nil
}

// === Telling the apps ===

// rooms_direct says whether a room is a user's one-to-one conversation.
func rooms_direct(user string, room string) bool {
	found, _ := federation_database().exists("select 1 from directs where user=? and room=?", user, room)
	return found
}

// rooms_direct_add marks a room as a user's one-to-one conversation with a
// person.
func rooms_direct_add(user string, room string, person string) {
	federation_database().exec("insert or ignore into directs (user, room, person) values (?, ?, ?)", user, room, person)
}

// rooms_direct_remove takes away a room's mark as a user's one-to-one
// conversation.
func rooms_direct_remove(user string, room string) {
	federation_database().exec("delete from directs where user=? and room=?", user, room)
}

// rooms_matrix_event is an event as the sync reader reads one.
func rooms_matrix_event(event gomatrixserverlib.PDU) matrix_event {
	var e matrix_event
	json.Unmarshal(event.JSON(), &e)
	e.ID = event.EventID()
	e.Redacts = event.Redacts()
	return e
}

// rooms_names is the name each member goes by in a state.
func rooms_names(room string, state rooms_state) map[string]string {
	names := map[string]string{}
	var ids []string
	for key, id := range state {
		if strings.HasPrefix(key, spec.MRoomMember+"\x00") {
			ids = append(ids, id)
		}
	}
	for _, event := range rooms_events(ids) {
		var content struct {
			Name string `json:"displayname"`
		}
		json.Unmarshal(event.Content(), &content)
		if content.Name != "" && event.StateKey() != nil {
			names[room+" "+*event.StateKey()] = content.Name
		}
	}
	return names
}

// rooms_summary is what chat is told of a room on joining it: its name, or
// the members an unnamed one is called after, how many are in it, and
// whether it is encrypted.
func rooms_summary(user string, room string, state rooms_state) map[string]any {
	direct := rooms_direct(user, room)
	info := map[string]any{"kind": "room", "conversation": room, "room": !direct, "direct": direct}
	if id := state[rooms_key("m.room.name", "")]; id != "" {
		if event := rooms_event(id); event != nil {
			var content struct {
				Name string `json:"name"`
			}
			json.Unmarshal(event.Content(), &content)
			if content.Name != "" {
				info["name"] = content.Name
			}
		}
	}
	if state[rooms_key("m.room.encryption", "")] != "" {
		info["encrypted"] = true
	}
	names := rooms_names(room, state)
	members := rooms_members(state)
	count := 0
	var heroes []string
	for member, membership := range members {
		if membership == spec.Join {
			count++
		}
		if member != user && (membership == spec.Join || membership == spec.Invite) {
			heroes = append(heroes, member)
		}
	}
	info["count"] = count
	sort.Strings(heroes)
	var list []any
	for _, hero := range heroes[:min(len(heroes), 5)] {
		list = append(list, map[string]any{"id": hero, "name": names[room+" "+hero]})
	}
	if len(list) > 0 {
		info["heroes"] = list
	}
	return info
}

// rooms_conversation is an event of a room as a user's conversation events.
func rooms_conversation(user string, event gomatrixserverlib.PDU, state rooms_state) []map[string]any {
	room := event.RoomID().String()
	e := rooms_matrix_event(event)
	if e.Key != nil {
		switch e.Type {
		case spec.MRoomMember:
			membership, _ := e.Content["membership"].(string)
			name, _ := e.Content["displayname"].(string)
			if *e.Key == user {
				switch membership {
				case spec.Join:
					return []map[string]any{rooms_summary(user, room, state)}
				case spec.Invite:
					direct, _ := e.Content["is_direct"].(bool)
					if direct {
						rooms_direct_add(user, room, e.Sender)
					}
					summary := rooms_summary(user, room, state)
					name, _ := summary["name"].(string)
					return []map[string]any{{"kind": "invite", "conversation": room, "from": e.Sender, "name": name, "direct": direct}}
				default:
					return []map[string]any{{"kind": "room", "conversation": room, "left": true}}
				}
			}
			return []map[string]any{{"kind": "member", "conversation": room, "id": *e.Key, "name": name, "left": membership != spec.Join && membership != spec.Invite}}
		case "m.room.name", "m.room.encryption":
			return []map[string]any{rooms_summary(user, room, state)}
		}
		return nil
	}
	reader := &matrix_state{user: user, typing: map[string][]string{}, direct: map[string]bool{room: rooms_direct(user, room)}, names: rooms_names(room, state), download: func(string) string { return "" }}
	if message := reader.timeline(room, &e); message != nil {
		return []map[string]any{message}
	}
	return nil
}

// rooms_tell queues an event of a room for the apps of each account here in
// it, other than the one that sent it, and of an account it invites or
// removes.
func rooms_tell(event gomatrixserverlib.PDU, out *rooms_outbox) {
	after, ok := rooms_after(event.EventID())
	if !ok || out == nil {
		return
	}
	members := rooms_members(after)
	recipients := map[string]bool{}
	for member, membership := range members {
		if membership == spec.Join {
			recipients[member] = true
		}
	}
	if event.Type() == spec.MRoomMember && event.StateKey() != nil {
		recipients[*event.StateKey()] = true
	}
	sender := string(event.SenderID())
	for member := range recipients {
		if member == sender {
			continue
		}
		user, account := federation_user(member)
		if user == nil {
			continue
		}
		out.add(user, account, rooms_conversation(member, event, after)...)
	}
}

// === Building ===

// rooms_build makes an event of a room as one of this server's users, on the
// room's current state, and checks the state allows it.
func rooms_build(version gomatrixserverlib.IRoomVersion, room string, sender string, kind string, key *string, content any) (gomatrixserverlib.PDU, error) {
	user, err := spec.NewUserID(sender, true)
	if err != nil {
		return nil, err
	}
	id := federation_identity_of(user.Domain())
	if id == nil {
		return nil, fmt.Errorf("%s is not a Matrix domain here", user.Domain())
	}
	proto := gomatrixserverlib.ProtoEvent{SenderID: sender, RoomID: room, Type: kind, StateKey: key}
	if err := proto.SetContent(content); err != nil {
		return nil, err
	}
	// A redaction names its event in its content from version 11, and beside
	// it before.
	if kind == spec.MRoomRedaction {
		target, _ := content.(map[string]any)["redacts"].(string)
		if slices.Contains([]gomatrixserverlib.RoomVersion{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}, version.Version()) {
			proto.Redacts = target
			proto.SetContent(map[string]any{})
		}
	}
	if kind == spec.MRoomCreate && version.DomainlessRoomIDs() {
		proto.RoomID = ""
	}
	prevs, depth := rooms_edge(room)
	if prevs == nil {
		prevs = []string{}
	}
	proto.PrevEvents = prevs
	proto.Depth = depth
	return rooms_finish(version, room, &proto, id)
}

// rooms_finish gives an event its auth events from the room's current state,
// builds it signed as a domain, and checks the state allows it.
func rooms_finish(version gomatrixserverlib.IRoomVersion, room string, proto *gomatrixserverlib.ProtoEvent, id *federation_identity) (gomatrixserverlib.PDU, error) {
	needed, err := gomatrixserverlib.StateNeededForProtoEvent(proto)
	if err != nil {
		return nil, err
	}
	current, _ := rooms_current(room)
	_, provider, err := rooms_provider(current, needed.Tuples())
	if err != nil {
		return nil, err
	}
	builder := version.NewEventBuilderFromProtoEvent(proto)
	if err := builder.AddAuthEvents(provider); err != nil {
		return nil, err
	}
	proto.AuthEvents = builder.AuthEvents
	event, err := builder.Build(time.Now(), id.domain, id.key, id.private)
	if err != nil {
		return nil, err
	}
	if err := gomatrixserverlib.Allowed(event, provider, federation_sender); err != nil {
		return nil, err
	}
	return event, nil
}

// rooms_commit takes an event this server built into its room, tells the
// other accounts here, and sends it to every other server in the room before
// and after it. The room is held by the caller.
func rooms_commit(version gomatrixserverlib.IRoomVersion, event gomatrixserverlib.PDU, out *rooms_outbox) error {
	room := event.RoomID().String()
	before, snapshot := rooms_current(room)
	if snapshot == 0 {
		snapshot = rooms_snapshot_save(room, before)
	}
	rooms_keep(event, snapshot, false, false)
	rooms_advance(event)
	if err := rooms_settle(version, room); err != nil {
		return err
	}
	if event.Type() == spec.MRoomRedaction {
		rooms_redact(version, event)
	}
	after, _ := rooms_current(room)
	rooms_tell(event, out)
	federation_send(rooms_servers(before, after), event)
	return nil
}

// rooms_send builds and commits an event as a user.
func rooms_send(ctx context.Context, room string, sender string, kind string, key *string, content any) (gomatrixserverlib.PDU, error) {
	version := rooms_version(room)
	if version == nil {
		return nil, fmt.Errorf("not in the room")
	}
	var out rooms_outbox
	unlock := rooms_lock(room)
	event, err := rooms_build(version, room, sender, kind, key, content)
	if err == nil {
		err = rooms_commit(version, event, &out)
	}
	unlock()
	out.flush()
	return event, err
}

// rooms_send_redaction redacts an event of a room as a user.
func rooms_send_redaction(ctx context.Context, room string, sender string, target string) (gomatrixserverlib.PDU, error) {
	return rooms_send(ctx, room, sender, spec.MRoomRedaction, nil, map[string]any{"redacts": target})
}

// rooms_membership is a user's membership of a room as its current state
// has it, or "".
func rooms_membership(room string, user string) string {
	state, _ := rooms_current(room)
	id := state[rooms_key(spec.MRoomMember, user)]
	if id == "" {
		return ""
	}
	row, _ := federation_database().row("select membership from events where id=?", id)
	return row_string(row, "membership")
}

// rooms_display is the name a hosted account goes by: its label, else its
// user's.
func rooms_display(user *User, account string) string {
	if row := account_hosted_get(user, account); row != nil {
		if label := row_string(row, "label"); label != "" {
			return label
		}
	}
	if identity := user.identity(); identity != nil {
		return identity.Name
	}
	return ""
}

// rooms_member_content is a user's member event content: the membership and
// the name they go by.
func rooms_member_content(address string, membership string) map[string]any {
	content := map[string]any{"membership": membership}
	if user, account := federation_user(address); user != nil {
		if name := rooms_display(user, account); name != "" {
			content["displayname"] = name
		}
	}
	return content
}

// rooms_stripped is the part of a room's state an invitation carries, so the
// invited can tell what room it is.
func rooms_stripped(room string, inviter string) []gomatrixserverlib.InviteStrippedState {
	state, _ := rooms_current(room)
	var ids []string
	for _, key := range []string{rooms_key(spec.MRoomCreate, ""), rooms_key(spec.MRoomJoinRules, ""), rooms_key("m.room.name", ""), rooms_key("m.room.avatar", ""), rooms_key("m.room.canonical_alias", ""), rooms_key("m.room.encryption", ""), rooms_key(spec.MRoomMember, inviter)} {
		if id := state[key]; id != "" {
			ids = append(ids, id)
		}
	}
	loaded := rooms_events(ids)
	var out []gomatrixserverlib.InviteStrippedState
	for _, id := range ids {
		if event := loaded[id]; event != nil {
			out = append(out, gomatrixserverlib.NewInviteStrippedState(event))
		}
	}
	return out
}

// === What the user does ===

// rooms_create makes a room as a user with people invited: a one-to-one
// conversation, marked so, for one person; a named group for several. Each
// person's server must know them.
func rooms_create(ctx context.Context, creator string, people []string, name string) (string, error) {
	version, err := gomatrixserverlib.GetRoomVersion(rooms_version_default)
	if err != nil {
		return "", err
	}
	user, err := spec.NewUserID(creator, true)
	if err != nil {
		return "", err
	}
	direct := len(people) == 1
	room := ""
	if !version.DomainlessRoomIDs() {
		room = "!" + strings.ReplaceAll(uid(), "-", "") + ":" + string(user.Domain())
	}
	empty := ""
	create := map[string]any{"room_version": string(version.Version())}
	if version.CreatorInCreateEvent() {
		create["creator"] = creator
	}
	var out rooms_outbox
	defer out.flush()
	// The create event comes first: a room without domains takes its id from
	// it.
	if room != "" {
		federation_database().exec("insert into rooms (id, version, state, created) values (?, ?, 0, ?)", room, string(version.Version()), now())
	}
	event, err := rooms_build(version, room, creator, spec.MRoomCreate, &empty, create)
	if err != nil {
		return "", err
	}
	if room == "" {
		room = "!" + strings.TrimPrefix(event.EventID(), "$")
		federation_database().exec("insert into rooms (id, version, state, created) values (?, ?, 0, ?)", room, string(version.Version()), now())
	}
	unlock := rooms_lock(room)
	defer unlock()
	if err := rooms_commit(version, event, &out); err != nil {
		return "", err
	}
	// From version 12 the creator outranks everyone and is not listed.
	users := map[string]any{}
	if !version.PrivilegedCreators() {
		users[creator] = 100
	}
	if direct {
		users[people[0]] = 100
	}
	powers := map[string]any{
		"users": users, "users_default": 0, "events_default": 0, "state_default": 50,
		"ban": 50, "kick": 50, "redact": 50, "invite": 0,
		"events": map[string]any{"m.room.name": 50, "m.room.power_levels": 100, "m.room.history_visibility": 100, "m.room.encryption": 100, "m.room.tombstone": 100},
	}
	steps := []struct {
		kind    string
		key     string
		content any
	}{
		{spec.MRoomMember, creator, rooms_member_content(creator, spec.Join)},
		{spec.MRoomPowerLevels, "", powers},
		{spec.MRoomJoinRules, "", map[string]any{"join_rule": spec.Invite}},
		{spec.MRoomHistoryVisibility, "", map[string]any{"history_visibility": "shared"}},
		{"m.room.guest_access", "", map[string]any{"guest_access": "forbidden"}},
	}
	if !direct && name != "" {
		steps = append(steps, struct {
			kind    string
			key     string
			content any
		}{"m.room.name", "", map[string]any{"name": name}})
	}
	for _, step := range steps {
		key := step.key
		event, err := rooms_build(version, room, creator, step.kind, &key, step.content)
		if err != nil {
			return "", err
		}
		if err := rooms_commit(version, event, &out); err != nil {
			return "", err
		}
	}
	if direct {
		rooms_direct_add(creator, room, people[0])
	}
	for _, person := range people {
		if err := rooms_invite_locked(ctx, version, room, creator, person, direct, &out); err != nil {
			return room, err
		}
	}
	return room, nil
}

// rooms_invite_locked invites a person to a room the caller holds: a person
// here by the room's own event, one elsewhere through their server, which
// countersigns the invitation before the room takes it.
func rooms_invite_locked(ctx context.Context, version gomatrixserverlib.IRoomVersion, room string, inviter string, person string, direct bool, out *rooms_outbox) error {
	invitee, err := spec.NewUserID(person, true)
	if err != nil {
		return fmt.Errorf("invalid address")
	}
	content := map[string]any{"membership": spec.Invite}
	if direct {
		content["is_direct"] = true
	}
	event, err := rooms_build(version, room, inviter, spec.MRoomMember, &person, content)
	if err != nil {
		return err
	}
	if !federation_local(invitee.Domain()) {
		request, err := fclient.NewInviteV2Request(event, rooms_stripped(room, inviter))
		if err != nil {
			return err
		}
		inviting, _ := spec.NewUserID(inviter, true)
		answer, err := federation_client().SendInviteV2(ctx, inviting.Domain(), invitee.Domain(), request)
		if err != nil {
			return fmt.Errorf("invitation refused: %w", err)
		}
		signed, err := version.NewEventFromUntrustedJSON(answer.Event)
		if err != nil || signed.EventID() != event.EventID() {
			return fmt.Errorf("the invitation came back changed")
		}
		if err := gomatrixserverlib.VerifyEventSignatures(ctx, signed, federation_keyring(), federation_sender); err != nil {
			return fmt.Errorf("the invitation came back unsigned: %w", err)
		}
		event = signed
	}
	return rooms_commit(version, event, out)
}

// rooms_joiner adapts the federation client to what the library's join asks
// of one.
type rooms_joiner struct {
	client fclient.FederationClient
}

func (j rooms_joiner) MakeJoin(ctx context.Context, origin spec.ServerName, s spec.ServerName, room string, user string) (gomatrixserverlib.MakeJoinResponse, error) {
	answer, err := j.client.MakeJoin(ctx, origin, s, room, user)
	return &answer, err
}

func (j rooms_joiner) SendJoin(ctx context.Context, origin spec.ServerName, s spec.ServerName, event gomatrixserverlib.PDU) (gomatrixserverlib.SendJoinResponse, error) {
	answer, err := j.client.SendJoin(ctx, origin, s, event)
	return &answer, err
}

// rooms_join joins a user to a room by its id or an alias, answering its id:
// by the room's own event where someone here is in it, else through a server
// in it - the alias's, the one the invitation came from, the room id's.
func rooms_join(ctx context.Context, user string, address string) (string, error) {
	joining, err := spec.NewUserID(user, true)
	if err != nil {
		return "", err
	}
	local := joining.Domain()
	var servers []spec.ServerName
	room := address
	if strings.HasPrefix(address, "#") {
		_, domain, err := gomatrixserverlib.SplitID('#', address)
		if err != nil {
			return "", fmt.Errorf("not a room")
		}
		if federation_local(domain) {
			return "", fmt.Errorf("no such room")
		}
		directory, err := federation_client().LookupRoomAlias(ctx, local, domain, address)
		if err != nil || directory.RoomID == "" {
			return "", fmt.Errorf("no such room: %v", err)
		}
		room = directory.RoomID
		servers = append(servers, directory.Servers...)
		servers = append(servers, domain)
	} else if !strings.HasPrefix(address, "!") {
		return "", fmt.Errorf("not a room")
	}
	roomID, err := spec.NewRoomID(room)
	if err != nil {
		return "", fmt.Errorf("not a room")
	}
	switch rooms_membership(room, user) {
	case spec.Join:
		return room, nil
	case spec.Ban:
		return "", fmt.Errorf("banned from the room")
	}
	// Someone here is in the room: the join is the room's own event.
	if version := rooms_version(room); version != nil && rooms_local(room) != "" {
		event, err := rooms_send(ctx, room, user, spec.MRoomMember, &user, rooms_member_content(user, spec.Join))
		if err != nil {
			return "", err
		}
		federation_database().exec("delete from invites where room=? and user=?", room, user)
		rooms_welcome(user, room, rooms_recent(user, room, event))
		return room, nil
	}
	if row, _ := federation_database().row("select sender from invites where room=? and user=?", room, user); row != nil {
		if inviter, err := spec.NewUserID(row_string(row, "sender"), true); err == nil {
			servers = append([]spec.ServerName{inviter.Domain()}, servers...)
		}
	}
	// A room id of the versions before 12 names the server that made it.
	if i := strings.IndexByte(room, ':'); i > 0 {
		servers = append(servers, spec.ServerName(room[i+1:]))
	}
	id := federation_identity_of(local)
	if id == nil {
		return "", fmt.Errorf("%s is not a Matrix domain here", local)
	}
	var failure error = fmt.Errorf("no server to join through")
	tried := map[spec.ServerName]bool{}
	for _, server := range servers {
		if tried[server] || federation_local(server) {
			continue
		}
		tried[server] = true
		joined, ferr := gomatrixserverlib.PerformJoin(ctx, rooms_joiner{federation_client()}, gomatrixserverlib.PerformJoinInput{
			UserID:     joining,
			RoomID:     roomID,
			ServerName: server,
			Content:    rooms_member_content(user, spec.Join),
			PrivateKey: id.private,
			KeyID:      id.key,
			KeyRing:    federation_keyring(),
			EventProvider: func(version gomatrixserverlib.RoomVersion, ids []string) ([]gomatrixserverlib.PDU, error) {
				return rooms_fetch(ctx, local, server, version, ids)
			},
			UserIDQuerier: federation_sender,
			GetOrCreateSenderID: func(ctx context.Context, user spec.UserID, room spec.RoomID, version string) (spec.SenderID, ed25519.PrivateKey, error) {
				return spec.SenderIDFromUserID(user), nil, nil
			},
			StoreSenderIDFromPublicID: func(ctx context.Context, sender spec.SenderID, user string, room spec.RoomID) error { return nil },
		})
		if ferr != nil {
			failure = ferr.Err
			debug("Matrix join of %s through %s: %v", room, server, ferr.Err)
			continue
		}
		if err := rooms_joined(joined); err != nil {
			return "", err
		}
		federation_database().exec("delete from invites where room=? and user=?", room, user)
		rooms_welcome(user, room, rooms_backfill(ctx, local, server, room, user, joined.JoinEvent))
		return room, nil
	}
	return "", failure
}

// rooms_fetch fetches events by id from a server, for the library's join to
// check the state it was sent by.
func rooms_fetch(ctx context.Context, local spec.ServerName, server spec.ServerName, version gomatrixserverlib.RoomVersion, ids []string) ([]gomatrixserverlib.PDU, error) {
	verImpl, err := gomatrixserverlib.GetRoomVersion(version)
	if err != nil {
		return nil, err
	}
	var out []gomatrixserverlib.PDU
	for _, id := range ids {
		if event := rooms_event(id); event != nil {
			out = append(out, event)
			continue
		}
		transaction, err := federation_client().GetEvent(ctx, local, server, id)
		if err != nil || len(transaction.PDUs) == 0 {
			return nil, fmt.Errorf("event %s: %v", id, err)
		}
		event, err := verImpl.NewEventFromUntrustedJSON(transaction.PDUs[0])
		if err != nil || event.EventID() != id {
			return nil, fmt.Errorf("event %s: not the event asked for", id)
		}
		out = append(out, event)
	}
	return out, nil
}

// rooms_joined takes in what a join through another server answered: the
// room's state and auth chain, checked by the library, and the join event
// following them.
func rooms_joined(joined *gomatrixserverlib.PerformJoinResponse) error {
	event := joined.JoinEvent
	room := event.RoomID().String()
	version, err := gomatrixserverlib.GetRoomVersion(event.Version())
	if err != nil {
		return err
	}
	federation_database().exec("insert into rooms (id, version, state, created) values (?, ?, 0, ?) on conflict(id) do update set version=excluded.version", room, string(version.Version()), now())
	unlock := rooms_lock(room)
	defer unlock()
	snapshot := gomatrixserverlib.LineariseStateResponse(version.Version(), joined.StateSnapshot)
	state := rooms_state{}
	states := map[string]bool{}
	for _, e := range joined.StateSnapshot.GetStateEvents().TrustedEvents(version.Version(), false) {
		states[e.EventID()] = true
	}
	for _, e := range snapshot {
		if e.RoomID().String() != room {
			continue
		}
		rooms_keep(e, 0, false, true)
		if states[e.EventID()] && e.StateKey() != nil {
			state[rooms_key(e.Type(), *e.StateKey())] = e.EventID()
		}
	}
	before := rooms_snapshot_save(room, state)
	rooms_keep(event, before, false, false)
	rooms_advance(event)
	return rooms_settle(version, room)
}

// rooms_welcome tells a user's apps of a room they joined, as one batch so
// it arrives in order: the room, who is in it, and its recent past.
func rooms_welcome(user string, room string, history []map[string]any) {
	found, account := federation_user(user)
	if found == nil {
		return
	}
	state, _ := rooms_current(room)
	events := []map[string]any{rooms_summary(user, room, state)}
	names := rooms_names(room, state)
	members := rooms_members(state)
	var people []string
	for member, membership := range members {
		if member != user && (membership == spec.Join || membership == spec.Invite) {
			people = append(people, member)
		}
	}
	sort.Strings(people)
	for _, member := range people {
		events = append(events, map[string]any{"kind": "member", "conversation": room, "id": member, "name": names[room+" "+member], "left": false})
	}
	linked_inbound(found, account, "matrix", append(events, history...))
}

// rooms_shared says whether a room shows its past to those who join it.
func rooms_shared(room string) bool {
	state, _ := rooms_current(room)
	visibility := gomatrixserverlib.HistoryVisibility("shared")
	if id := state[rooms_key(spec.MRoomHistoryVisibility, "")]; id != "" {
		if event := rooms_event(id); event != nil {
			if value, err := event.HistoryVisibility(); err == nil {
				visibility = value
			}
		}
	}
	return visibility == "shared" || visibility == "world_readable"
}

// rooms_recent is a room's recent messages before a user joined it, from
// what this server keeps, as the user's conversation events.
func rooms_recent(user string, room string, join gomatrixserverlib.PDU) []map[string]any {
	if join == nil || !rooms_shared(room) {
		return nil
	}
	rows, _ := federation_database().rows("select id from events where room=? and outlier=0 and rejected=0 and key is null and depth<? order by depth desc, rowid desc limit ?", room, join.Depth(), rooms_history)
	var ids []string
	for _, row := range rows {
		ids = append(ids, row_string(row, "id"))
	}
	slices.Reverse(ids)
	loaded := rooms_events(ids)
	var out []map[string]any
	for _, id := range ids {
		event := loaded[id]
		if event == nil || string(event.SenderID()) == user {
			continue
		}
		after, ok := rooms_after(id)
		if !ok {
			continue
		}
		out = append(out, rooms_conversation(user, event, after)...)
	}
	return out
}

// rooms_backfill fetches a room's recent past on joining it, and answers its
// messages as the joining user's conversation events, oldest first. The
// events are kept outside the timeline: the room's state is what the join
// brought.
func rooms_backfill(ctx context.Context, local spec.ServerName, server spec.ServerName, room string, user string, join gomatrixserverlib.PDU) []map[string]any {
	version := rooms_version(room)
	if version == nil {
		return nil
	}
	transaction, err := federation_client().Backfill(ctx, local, server, room, rooms_history, join.PrevEventIDs())
	if err != nil {
		debug("Matrix room %s: history from %s: %v", room, server, err)
		return nil
	}
	var events []gomatrixserverlib.PDU
	for _, raw := range transaction.PDUs {
		if event, err := version.NewEventFromUntrustedJSON(raw); err == nil && event.RoomID().String() == room {
			events = append(events, event)
		}
	}
	failures := gomatrixserverlib.VerifyAllEventSignatures(ctx, events, federation_keyring(), federation_sender)
	var good []gomatrixserverlib.PDU
	for i, event := range events {
		if failures[i] == nil {
			good = append(good, event)
		}
	}
	unlock := rooms_lock(room)
	current, _ := rooms_current(room)
	var told []map[string]any
	for _, event := range gomatrixserverlib.ReverseTopologicalOrdering(good, gomatrixserverlib.TopologicalOrderByPrevEvents) {
		if rooms_event(event.EventID()) != nil {
			continue
		}
		auth, err := rooms_auth(ctx, local, server, version, event)
		if err != nil {
			continue
		}
		provider, err := gomatrixserverlib.NewAuthEvents(auth)
		if err != nil || gomatrixserverlib.Allowed(event, provider, federation_sender) != nil {
			rooms_keep(event, 0, true, true)
			continue
		}
		rooms_keep(event, 0, false, true)
		if event.StateKey() == nil && string(event.SenderID()) != user {
			told = append(told, rooms_conversation(user, event, current)...)
		}
	}
	unlock()
	return told
}

// rooms_leave takes a user out of a room, or declines an invitation to one
// this server is not in through the server it came from.
func rooms_leave(ctx context.Context, user string, room string) error {
	content := map[string]any{"membership": spec.Leave}
	membership := rooms_membership(room, user)
	if membership == spec.Join || (membership == spec.Invite && rooms_local(room) != "") {
		_, err := rooms_send(ctx, room, user, spec.MRoomMember, &user, content)
		return err
	}
	row, _ := federation_database().row("select sender from invites where room=? and user=?", room, user)
	if row == nil {
		return nil
	}
	leaving, err := spec.NewUserID(user, true)
	if err != nil {
		return err
	}
	inviter, err := spec.NewUserID(row_string(row, "sender"), true)
	if err != nil {
		return err
	}
	id := federation_identity_of(leaving.Domain())
	if id == nil {
		return fmt.Errorf("not a Matrix domain here")
	}
	client := federation_client()
	template, err := client.MakeLeave(ctx, leaving.Domain(), inviter.Domain(), room, user)
	if err != nil {
		return fmt.Errorf("declining: %w", err)
	}
	version, err := gomatrixserverlib.GetRoomVersion(template.RoomVersion)
	if err != nil {
		return err
	}
	proto := template.LeaveEvent
	if proto.Type != spec.MRoomMember || proto.StateKey == nil || *proto.StateKey != user || proto.SenderID != user {
		return fmt.Errorf("declining: not a leave for the user")
	}
	if err := proto.SetContent(content); err != nil {
		return err
	}
	event, err := version.NewEventBuilderFromProtoEvent(&proto).Build(time.Now(), id.domain, id.key, id.private)
	if err != nil {
		return err
	}
	if err := client.SendLeave(ctx, leaving.Domain(), inviter.Domain(), event); err != nil {
		return fmt.Errorf("declining: %w", err)
	}
	federation_database().exec("delete from invites where room=? and user=?", room, user)
	return nil
}

// rooms_message sends a message event to a room as a user, once per key.
func rooms_message(ctx context.Context, account string, user string, room string, kind string, content map[string]any, key string) (string, error) {
	db := federation_database()
	if key != "" {
		if row, _ := db.row("select event from sent where account=? and key=?", account, key); row != nil {
			return row_string(row, "event"), nil
		}
	}
	if rooms_membership(room, user) != spec.Join {
		return "", fmt.Errorf("not in the room")
	}
	event, err := rooms_send(ctx, room, user, kind, nil, content)
	if err != nil {
		return "", err
	}
	if key != "" {
		db.exec("insert or ignore into sent (account, key, event, created) values (?, ?, ?, ?)", account, key, event.EventID(), now())
	}
	return event.EventID(), nil
}

// rooms_profile is the name a person goes by, which fails for an address
// their server knows no one at.
func rooms_profile(ctx context.Context, local spec.ServerName, person string) (string, error) {
	address, err := spec.NewUserID(person, true)
	if err != nil {
		return "", fmt.Errorf("invalid address")
	}
	if federation_local(address.Domain()) {
		user, account := federation_user(person)
		if user == nil {
			return "", fmt.Errorf("no one is at %s", person)
		}
		return rooms_display(user, account), nil
	}
	profile, err := federation_client().LookupProfile(ctx, local, address.Domain(), person, "")
	if err != nil {
		return "", fmt.Errorf("no one is at %s: %w", person, err)
	}
	if profile.DisplayName == "" {
		return address.Local(), nil
	}
	return profile.DisplayName, nil
}

// === Answering other servers ===

// rooms_refuse answers a request the library refused, with its own error
// where it gave one.
func rooms_refuse(c *gin.Context, err error) {
	var matrix spec.MatrixError
	if errors.As(err, &matrix) {
		status := http.StatusBadRequest
		switch matrix.ErrCode {
		case spec.ErrorForbidden:
			status = http.StatusForbidden
		case spec.ErrorNotFound:
			status = http.StatusNotFound
		}
		federation_error(c, status, string(matrix.ErrCode), matrix.Err)
		return
	}
	var internal spec.InternalServerError
	if errors.As(err, &internal) {
		federation_error(c, http.StatusInternalServerError, "M_UNKNOWN", "internal error")
		return
	}
	federation_error(c, http.StatusBadRequest, "M_UNKNOWN", err.Error())
}

// rooms_store answers the library's questions about a room from its current
// state.
type rooms_store struct{}

func (rooms_store) IsKnownRoom(ctx context.Context, room spec.RoomID) (bool, error) {
	return rooms_local(room.String()) != "", nil
}

func (rooms_store) CurrentMembership(ctx context.Context, room spec.RoomID, sender spec.SenderID) (string, error) {
	membership := rooms_membership(room.String(), string(sender))
	if membership == "" {
		membership = spec.Leave
	}
	return membership, nil
}

func (rooms_store) GetAuthEvents(ctx context.Context, event gomatrixserverlib.PDU) (gomatrixserverlib.AuthEventProvider, error) {
	state, _ := rooms_current(event.RoomID().String())
	_, provider, err := rooms_provider(state, gomatrixserverlib.StateNeededForAuth([]gomatrixserverlib.PDU{event}).Tuples())
	return provider, err
}

func (rooms_store) GetState(ctx context.Context, room spec.RoomID, wanted []gomatrixserverlib.StateKeyTuple) ([]gomatrixserverlib.PDU, error) {
	state, _ := rooms_current(room.String())
	events, _, err := rooms_provider(state, wanted)
	return events, err
}

func (rooms_store) CurrentStateEvent(ctx context.Context, room spec.RoomID, kind string, key string) (gomatrixserverlib.PDU, error) {
	state, _ := rooms_current(room.String())
	id := state[rooms_key(kind, key)]
	if id == "" {
		return nil, nil
	}
	return rooms_event(id), nil
}

func (rooms_store) InvitePending(ctx context.Context, room spec.RoomID, sender spec.SenderID) (bool, error) {
	return rooms_membership(room.String(), string(sender)) == spec.Invite, nil
}

func (rooms_store) RestrictedRoomJoinInfo(ctx context.Context, room spec.RoomID, sender spec.SenderID, local spec.ServerName) (*gomatrixserverlib.RestrictedRoomJoinInfo, error) {
	state, _ := rooms_current(room.String())
	members := rooms_members(state)
	info := &gomatrixserverlib.RestrictedRoomJoinInfo{LocalServerInRoom: rooms_local(room.String()) != "", UserJoinedToRoom: members[string(sender)] == spec.Join}
	var ids []string
	for member, membership := range members {
		if user, err := spec.NewUserID(member, true); err == nil && membership == spec.Join && user.Domain() == local {
			ids = append(ids, state[rooms_key(spec.MRoomMember, member)])
		}
	}
	for _, event := range rooms_events(ids) {
		info.JoinedUsers = append(info.JoinedUsers, event)
	}
	return info, nil
}

// rooms_template fills a join or leave event's template for another server
// on the room's current state, answering it built and the state that
// authorises it.
func rooms_template(version gomatrixserverlib.IRoomVersion, room string, local spec.ServerName, proto *gomatrixserverlib.ProtoEvent) (gomatrixserverlib.PDU, []gomatrixserverlib.PDU, error) {
	id := federation_identity_of(local)
	if id == nil {
		return nil, nil, spec.NotFound("no such server here")
	}
	prevs, depth := rooms_edge(room)
	proto.PrevEvents = prevs
	proto.Depth = depth
	needed, err := gomatrixserverlib.StateNeededForProtoEvent(proto)
	if err != nil {
		return nil, nil, err
	}
	current, _ := rooms_current(room)
	events, provider, err := rooms_provider(current, needed.Tuples())
	if err != nil {
		return nil, nil, err
	}
	builder := version.NewEventBuilderFromProtoEvent(proto)
	if err := builder.AddAuthEvents(provider); err != nil {
		return nil, nil, err
	}
	proto.AuthEvents = builder.AuthEvents
	event, err := builder.Build(time.Now(), id.domain, id.key, id.private)
	if err != nil {
		return nil, nil, err
	}
	return event, events, nil
}

// rooms_versions are the room versions a make_join names, version 1 alone
// where it names none.
func rooms_versions(names []string) []gomatrixserverlib.RoomVersion {
	if len(names) == 0 {
		return []gomatrixserverlib.RoomVersion{gomatrixserverlib.RoomVersionV1}
	}
	out := make([]gomatrixserverlib.RoomVersion, 0, len(names))
	for _, name := range names {
		out = append(out, gomatrixserverlib.RoomVersion(name))
	}
	return out
}

// rooms_make_join answers another server's request for a join event to fill
// for one of its users.
func rooms_make_join(c *gin.Context, r *fclient.FederationRequest, room string, user string) {
	joining, err := spec.NewUserID(user, true)
	roomID, rerr := spec.NewRoomID(room)
	if err != nil || rerr != nil {
		federation_error(c, http.StatusBadRequest, "M_INVALID_PARAM", "invalid user or room")
		return
	}
	version := rooms_version(room)
	local := rooms_local(room)
	if version == nil || local == "" {
		federation_error(c, http.StatusNotFound, "M_NOT_FOUND", "no such room here")
		return
	}
	answer, err := gomatrixserverlib.HandleMakeJoin(gomatrixserverlib.HandleMakeJoinInput{
		Context:           c.Request.Context(),
		UserID:            *joining,
		SenderID:          spec.SenderID(user),
		RoomID:            *roomID,
		RoomVersion:       version.Version(),
		RemoteVersions:    rooms_versions(c.QueryArray("ver")),
		RequestOrigin:     r.Origin(),
		LocalServerName:   local,
		LocalServerInRoom: true,
		RoomQuerier:       rooms_store{},
		UserIDQuerier:     federation_sender,
		BuildEventTemplate: func(proto *gomatrixserverlib.ProtoEvent) (gomatrixserverlib.PDU, []gomatrixserverlib.PDU, error) {
			return rooms_template(version, room, local, proto)
		},
	})
	if err != nil {
		rooms_refuse(c, err)
		return
	}
	c.JSON(http.StatusOK, map[string]any{"event": answer.JoinTemplateEvent, "room_version": answer.RoomVersion})
}

// rooms_send_join takes another server's join event into a room and answers
// the state the joining server needs.
func rooms_send_join(c *gin.Context, r *fclient.FederationRequest, room string, id string, v1 bool) {
	roomID, err := spec.NewRoomID(room)
	if err != nil {
		federation_error(c, http.StatusBadRequest, "M_INVALID_PARAM", "invalid room")
		return
	}
	version := rooms_version(room)
	local := rooms_local(room)
	identity := federation_identity_of(local)
	if version == nil || local == "" || identity == nil {
		federation_error(c, http.StatusNotFound, "M_NOT_FOUND", "no such room here")
		return
	}
	ctx := c.Request.Context()
	answer, err := gomatrixserverlib.HandleSendJoin(gomatrixserverlib.HandleSendJoinInput{
		Context:                   ctx,
		RoomID:                    *roomID,
		EventID:                   id,
		JoinEvent:                 r.Content(),
		RoomVersion:               version.Version(),
		RequestOrigin:             r.Origin(),
		LocalServerName:           local,
		KeyID:                     identity.key,
		PrivateKey:                identity.private,
		Verifier:                  federation_keyring(),
		MembershipQuerier:         rooms_store{},
		UserIDQuerier:             federation_sender,
		StoreSenderIDFromPublicID: func(ctx context.Context, sender spec.SenderID, user string, room spec.RoomID) error { return nil },
	})
	if err != nil {
		rooms_refuse(c, err)
		return
	}
	event := answer.JoinEvent
	if !answer.AlreadyJoined {
		var out rooms_outbox
		unlock := rooms_lock(room)
		err = rooms_accept(ctx, local, r.Origin(), version, event, &out, false)
		if err == nil && !rooms_known(event.EventID()) {
			err = fmt.Errorf("the room has moved past it")
		}
		if err == nil {
			before, _ := rooms_current(room)
			federation_send(rooms_servers(before), event)
		}
		unlock()
		out.flush()
		if err != nil {
			federation_error(c, http.StatusForbidden, "M_FORBIDDEN", err.Error())
			return
		}
	}
	row, _ := federation_database().row("select before from events where id=?", event.EventID())
	state := rooms_list(rooms_snapshot(row_int(row, "before")))
	chain := rooms_chain(append(slices.Clone(state), event))
	before := rooms_snapshot(row_int(row, "before"))
	servers := append(rooms_servers(before), string(local))
	body := map[string]any{
		"origin":          string(local),
		"auth_chain":      rooms_raw(chain),
		"state":           rooms_raw(state),
		"event":           json.RawMessage(event.JSON()),
		"members_omitted": false,
		"servers_in_room": servers,
	}
	if v1 {
		c.JSON(http.StatusOK, []any{200, body})
		return
	}
	c.JSON(http.StatusOK, body)
}

// rooms_raw is events as their JSON.
func rooms_raw(events []gomatrixserverlib.PDU) []json.RawMessage {
	out := make([]json.RawMessage, 0, len(events))
	for _, event := range events {
		out = append(out, event.JSON())
	}
	return out
}

// rooms_make_leave answers another server's request for a leave event to
// fill for one of its users.
func rooms_make_leave(c *gin.Context, r *fclient.FederationRequest, room string, user string) {
	leaving, err := spec.NewUserID(user, true)
	roomID, rerr := spec.NewRoomID(room)
	if err != nil || rerr != nil {
		federation_error(c, http.StatusBadRequest, "M_INVALID_PARAM", "invalid user or room")
		return
	}
	version := rooms_version(room)
	local := rooms_local(room)
	if version == nil || local == "" {
		federation_error(c, http.StatusNotFound, "M_NOT_FOUND", "no such room here")
		return
	}
	answer, err := gomatrixserverlib.HandleMakeLeave(gomatrixserverlib.HandleMakeLeaveInput{
		UserID:            *leaving,
		SenderID:          spec.SenderID(user),
		RoomID:            *roomID,
		RoomVersion:       version.Version(),
		RequestOrigin:     r.Origin(),
		LocalServerName:   local,
		LocalServerInRoom: true,
		UserIDQuerier:     federation_sender,
		BuildEventTemplate: func(proto *gomatrixserverlib.ProtoEvent) (gomatrixserverlib.PDU, []gomatrixserverlib.PDU, error) {
			return rooms_template(version, room, local, proto)
		},
	})
	if err != nil {
		rooms_refuse(c, err)
		return
	}
	c.JSON(http.StatusOK, map[string]any{"event": answer.LeaveTemplateEvent, "room_version": answer.RoomVersion})
}

// rooms_send_leave takes another server's leave event, for one of its own
// users, into a room.
func rooms_send_leave(c *gin.Context, r *fclient.FederationRequest, room string, id string, v1 bool) {
	version := rooms_version(room)
	local := rooms_local(room)
	if version == nil || local == "" {
		federation_error(c, http.StatusNotFound, "M_NOT_FOUND", "no such room here")
		return
	}
	ctx := c.Request.Context()
	event, err := version.NewEventFromUntrustedJSON(r.Content())
	if err != nil || event.EventID() != id || event.RoomID().String() != room {
		federation_error(c, http.StatusBadRequest, "M_BAD_JSON", "not the leave event named")
		return
	}
	sender, err := spec.NewUserID(string(event.SenderID()), true)
	membership, _ := event.Membership()
	if err != nil || event.Type() != spec.MRoomMember || !event.StateKeyEquals(string(event.SenderID())) || sender.Domain() != r.Origin() || membership != spec.Leave {
		federation_error(c, http.StatusBadRequest, "M_BAD_JSON", "not a leave of the server's own user")
		return
	}
	if err := gomatrixserverlib.VerifyEventSignatures(ctx, event, federation_keyring(), federation_sender); err != nil {
		federation_error(c, http.StatusForbidden, "M_FORBIDDEN", "bad signature")
		return
	}
	var out rooms_outbox
	unlock := rooms_lock(room)
	err = rooms_accept(ctx, local, r.Origin(), version, event, &out, false)
	if err == nil && rooms_known(event.EventID()) {
		after, _ := rooms_current(room)
		federation_send(rooms_servers(after), event)
	}
	unlock()
	out.flush()
	if err != nil {
		federation_error(c, http.StatusForbidden, "M_FORBIDDEN", err.Error())
		return
	}
	if v1 {
		c.JSON(http.StatusOK, []any{200, map[string]any{}})
		return
	}
	c.JSON(http.StatusOK, map[string]any{})
}

// rooms_invite_received takes another server's invitation of one of this
// server's users: checked and countersigned, kept until the user accepts or
// declines it, and the user's apps told.
func rooms_invite_received(c *gin.Context, r *fclient.FederationRequest, room string, id string) {
	var request fclient.InviteV2Request
	if err := json.Unmarshal(r.Content(), &request); err != nil || request.Event() == nil {
		federation_error(c, http.StatusBadRequest, "M_BAD_JSON", "not an invitation")
		return
	}
	event := request.Event()
	roomID, err := spec.NewRoomID(room)
	if err != nil || event.EventID() != id || event.Type() != spec.MRoomMember || event.StateKey() == nil {
		federation_error(c, http.StatusBadRequest, "M_BAD_JSON", "not the invitation named")
		return
	}
	membership, _ := event.Membership()
	invited := *event.StateKey()
	user, account := federation_user(invited)
	invitee, uerr := spec.NewUserID(invited, true)
	if membership != spec.Invite || user == nil || uerr != nil {
		federation_error(c, http.StatusNotFound, "M_NOT_FOUND", "no such user here")
		return
	}
	// The library checks the invitation is signed by its sender's server;
	// the user's own blocks are checked here, the server's with the request.
	if err := host_allowed(user, "matrix", federation_host(r.Origin())); err != nil {
		federation_error(c, http.StatusForbidden, "M_FORBIDDEN", "the server is blocked")
		return
	}
	identity := federation_identity_of(invitee.Domain())
	signed, err := gomatrixserverlib.HandleInvite(c.Request.Context(), gomatrixserverlib.HandleInviteInput{
		RoomID:            *roomID,
		RoomVersion:       request.RoomVersion(),
		InvitedUser:       *invitee,
		InvitedSenderID:   spec.SenderID(invited),
		InviteEvent:       event,
		StrippedState:     request.InviteRoomState(),
		KeyID:             identity.key,
		PrivateKey:        identity.private,
		Verifier:          federation_keyring(),
		RoomQuerier:       rooms_store{},
		MembershipQuerier: rooms_store{},
		StateQuerier:      rooms_store{},
		UserIDQuerier:     federation_sender,
	})
	if err != nil {
		rooms_refuse(c, err)
		return
	}
	stripped := request.InviteRoomState()
	name := ""
	for i := range stripped {
		if stripped[i].Type() == "m.room.name" {
			var content struct {
				Name string `json:"name"`
			}
			json.Unmarshal(stripped[i].Content(), &content)
			name = content.Name
		}
	}
	var content struct {
		Direct bool `json:"is_direct"`
	}
	json.Unmarshal(event.Content(), &content)
	federation_database().exec("insert into invites (room, user, version, sender, body, state, created) values (?, ?, ?, ?, ?, ?, ?) on conflict(room, user) do update set version=excluded.version, sender=excluded.sender, body=excluded.body, state=excluded.state, created=excluded.created",
		room, invited, string(request.RoomVersion()), string(event.SenderID()), string(signed.JSON()), json_encode(stripped), now())
	if content.Direct {
		rooms_direct_add(invited, room, string(event.SenderID()))
	}
	// A room someone here is in learns of the invitation as its own event;
	// one this server is not in is the user's to accept.
	if rooms_local(room) == "" {
		linked_inbound(user, account, "matrix", []map[string]any{{"kind": "invite", "conversation": room, "from": string(event.SenderID()), "name": name, "direct": content.Direct}})
	}
	c.JSON(http.StatusOK, map[string]any{"event": json.RawMessage(signed.JSON())})
}

// rooms_visible says whether a server may see an event: a room readable by
// anyone, or one the server had someone joined to then, or has now where the
// room shares its history.
func rooms_visible(server spec.ServerName, event gomatrixserverlib.PDU) bool {
	room := event.RoomID().String()
	current, _ := rooms_current(room)
	at, ok := rooms_after(event.EventID())
	if !ok {
		at = current
	}
	visibility := "shared"
	if id := at[rooms_key(spec.MRoomHistoryVisibility, "")]; id != "" {
		if e := rooms_event(id); e != nil {
			if value, err := e.HistoryVisibility(); err == nil {
				visibility = string(value)
			}
		}
	}
	if visibility == "world_readable" {
		return true
	}
	present := func(state rooms_state, memberships ...string) bool {
		for member, membership := range rooms_members(state) {
			if user, err := spec.NewUserID(member, true); err == nil && user.Domain() == server && slices.Contains(memberships, membership) {
				return true
			}
		}
		return false
	}
	if present(at, spec.Join) || (visibility == "invited" && present(at, spec.Join, spec.Invite)) {
		return true
	}
	return visibility == "shared" && present(current, spec.Join)
}

// rooms_serve_event answers one event of a room for a server that may see it.
func rooms_serve_event(c *gin.Context, r *fclient.FederationRequest, id string) {
	event := rooms_event(id)
	if event == nil || !rooms_visible(r.Origin(), event) {
		federation_error(c, http.StatusNotFound, "M_NOT_FOUND", "no such event")
		return
	}
	c.JSON(http.StatusOK, map[string]any{"origin": string(r.Destination()), "origin_server_ts": int64(spec.AsTimestamp(time.Now())), "pdus": []json.RawMessage{event.JSON()}})
}

// rooms_serve_state answers the state of a room before an event, as events or
// their ids, with its auth chain.
func rooms_serve_state(c *gin.Context, r *fclient.FederationRequest, room string, id string, ids bool) {
	event := rooms_event(id)
	row, _ := federation_database().row("select before from events where id=? and room=?", id, room)
	if event == nil || row == nil || row_int(row, "before") <= 0 || !rooms_visible(r.Origin(), event) {
		federation_error(c, http.StatusNotFound, "M_NOT_FOUND", "no such event")
		return
	}
	state := rooms_list(rooms_snapshot(row_int(row, "before")))
	chain := rooms_chain(state)
	if ids {
		c.JSON(http.StatusOK, map[string]any{"pdu_ids": rooms_ids(state), "auth_chain_ids": rooms_ids(chain)})
		return
	}
	c.JSON(http.StatusOK, map[string]any{"pdus": rooms_raw(state), "auth_chain": rooms_raw(chain)})
}

// rooms_ids is events' ids.
func rooms_ids(events []gomatrixserverlib.PDU) []string {
	out := make([]string, 0, len(events))
	for _, event := range events {
		out = append(out, event.EventID())
	}
	return out
}

// rooms_serve_auth answers an event's auth chain.
func rooms_serve_auth(c *gin.Context, r *fclient.FederationRequest, room string, id string) {
	event := rooms_event(id)
	if event == nil || event.RoomID().String() != room || !rooms_visible(r.Origin(), event) {
		federation_error(c, http.StatusNotFound, "M_NOT_FOUND", "no such event")
		return
	}
	c.JSON(http.StatusOK, map[string]any{"auth_chain": rooms_raw(rooms_chain([]gomatrixserverlib.PDU{event}))})
}

// rooms_walk goes back through a room from events by their prev events,
// deepest first, to as many as asked for, stopping at those named.
func rooms_walk(server spec.ServerName, room string, from []string, stop map[string]bool, limit int, depth int64, include bool) []gomatrixserverlib.PDU {
	seen := map[string]bool{}
	var out []gomatrixserverlib.PDU
	queue := slices.Clone(from)
	first := true
	for len(queue) > 0 && len(out) < limit {
		loaded := rooms_events(queue)
		var level []gomatrixserverlib.PDU
		for _, id := range queue {
			if seen[id] || stop[id] {
				continue
			}
			seen[id] = true
			if event := loaded[id]; event != nil && event.RoomID().String() == room && event.Depth() >= depth {
				level = append(level, event)
			}
		}
		sort.Slice(level, func(i int, j int) bool { return level[i].Depth() > level[j].Depth() })
		queue = nil
		for _, event := range level {
			if !rooms_visible(server, event) {
				continue
			}
			if !first || include {
				if len(out) >= limit {
					break
				}
				out = append(out, event)
			}
			queue = append(queue, event.PrevEventIDs()...)
		}
		first = false
	}
	return out
}

// rooms_serve_missing answers the events between what a server has of a room
// and what it was sent.
func rooms_serve_missing(c *gin.Context, r *fclient.FederationRequest, room string) {
	var request fclient.MissingEvents
	if err := json.Unmarshal(r.Content(), &request); err != nil {
		federation_error(c, http.StatusBadRequest, "M_BAD_JSON", "not a request for missing events")
		return
	}
	limit := request.Limit
	if limit <= 0 {
		limit = 10
	}
	stop := map[string]bool{}
	for _, id := range request.EarliestEvents {
		stop[id] = true
	}
	events := rooms_walk(r.Origin(), room, request.LatestEvents, stop, min(limit, 100), int64(request.MinDepth), false)
	slices.Reverse(events)
	c.JSON(http.StatusOK, map[string]any{"events": rooms_raw(events)})
}

// rooms_serve_backfill answers a room's past before the events named.
func rooms_serve_backfill(c *gin.Context, r *fclient.FederationRequest, room string, from []string, limit string) {
	count, _ := strconv.Atoi(limit)
	if count <= 0 {
		count = 10
	}
	events := rooms_walk(r.Origin(), room, from, map[string]bool{}, min(count, 100), 0, true)
	c.JSON(http.StatusOK, map[string]any{"origin": string(r.Destination()), "origin_server_ts": int64(spec.AsTimestamp(time.Now())), "pdus": rooms_raw(events)})
}

// rooms_serve_profile answers the name one of this server's users goes by.
func rooms_serve_profile(c *gin.Context, address string, field string) {
	user, account := federation_user(address)
	if user == nil {
		federation_error(c, http.StatusNotFound, "M_NOT_FOUND", "no such user")
		return
	}
	switch field {
	case "", "displayname":
		c.JSON(http.StatusOK, map[string]any{"displayname": rooms_display(user, account)})
	default:
		c.JSON(http.StatusOK, map[string]any{})
	}
}

// === Messages as objects ===

// rooms_object_create sends a message as a hosted account to the room the
// object names, once per key.
func rooms_object_create(ctx context.Context, h *matrix_hosted, object map[string]any, key string) (map[string]any, error) {
	room, _ := object["conversation"].(string)
	if !strings.HasPrefix(room, "!") {
		return nil, fmt.Errorf("a message needs the room it goes to")
	}
	if _, file := object["file"]; file {
		return nil, fmt.Errorf("files are not sent from a hosted Matrix account yet")
	}
	content, err := matrix_content(object)
	if err != nil {
		return nil, err
	}
	id, err := rooms_message(ctx, h.id, h.address, room, "m.room.message", content, key)
	if err != nil {
		return nil, err
	}
	return map[string]any{"id": id, "url": "https://matrix.to/#/" + room + "/" + id, "data": content}, nil
}

// rooms_object_update sends a new version of a message the hosted account
// sent.
func rooms_object_update(ctx context.Context, h *matrix_hosted, id string, object map[string]any) (map[string]any, error) {
	room, _ := object["conversation"].(string)
	text, _ := object["content"].(string)
	if !strings.HasPrefix(room, "!") || text == "" {
		return nil, fmt.Errorf("an edit needs the room and the new text")
	}
	original := rooms_event(id)
	if original == nil || original.RoomID().String() != room || string(original.SenderID()) != h.address {
		return nil, fmt.Errorf("not a message the account sent there")
	}
	content := map[string]any{
		"msgtype":       "m.text",
		"body":          "* " + text,
		"m.new_content": map[string]any{"msgtype": "m.text", "body": text},
		"m.relates_to":  map[string]any{"rel_type": "m.replace", "event_id": id},
	}
	edit, err := rooms_message(ctx, h.id, h.address, room, "m.room.message", content, "")
	if err != nil {
		return nil, err
	}
	return map[string]any{"id": id, "url": "https://matrix.to/#/" + room + "/" + id, "data": map[string]any{"edit": edit}}, nil
}

// rooms_object_delete redacts a message of a room as the hosted account.
func rooms_object_delete(ctx context.Context, h *matrix_hosted, id string, room string) error {
	if !strings.HasPrefix(room, "!") {
		return fmt.Errorf("a redaction needs the room")
	}
	target := rooms_event(id)
	if target == nil || target.RoomID().String() != room {
		return nil
	}
	if target.Redacted() {
		return nil
	}
	if rooms_membership(room, h.address) != spec.Join {
		return fmt.Errorf("not in the room")
	}
	_, err := rooms_send_redaction(ctx, room, h.address, id)
	return err
}

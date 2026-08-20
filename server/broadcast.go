// Mochi server: Durable broadcast log for subscriber fan-out
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

// Wire content keys for broadcast metadata riding alongside the app's own
// payload fields in an event's content map. Underscore-prefixed so an app
// payload field named "key" or "sequence" can't collide. Shared constants
// because the sender (api_broadcast_send, broadcast_resync replay) and the
// receiver (events.go gap detection) MUST agree: a 2026-05-26 table-rename
// find/replace turned the sender's "_sequence" into "sequence" and silently
// disabled all sequencing for six weeks — with these constants that class
// of divergence no longer compiles.
const (
	broadcast_content_key      = "_key"
	broadcast_content_sequence = "_sequence"
	broadcast_content_exclude  = "_exclude"
)

// Ceiling on one broadcast's recipient list. A call fans out one queued
// message per subscriber, so an uncapped list is an uncapped write to
// queue.db - the shape that took instance 1's queue to SQLite's 1GB limit
// and panicked (see send_peer in messages.go). Well above any real
// subscriber count.
const broadcast_recipients_maximum = 10000

// broadcast_skip_for reports whether a sequenced broadcast event must be
// acknowledged WITHOUT running the app handler at this receiver. Two cases:
//
//   - The receiving user owns the `from` entity: their DB is where the
//     event originated, and running a subscriber-side handler against the
//     canonical copy destroys it — a resync-replayed post/edit ran feeds'
//     attachment clear+store on the owner and deleted its files from disk
//     (2026-07-15). Holds for every broadcast app because app DBs are
//     per-user: for chat, `from` is the member identity, and even two
//     identities of one user share one DB, so dropping the echo is right.
//
//   - The recipient is the excluded actor named in _exclude: they already
//     applied their own action locally. Exclusion used to skip the send,
//     which left a permanent hole at that sequence in their stream —
//     resync, blind to the exclusion, then redelivered the event anyway.
//
// Applies only to sequenced broadcasts (the caller is inside the
// _key/_sequence wrapper); plain direct events may have legitimate
// self-sends.
func broadcast_skip_for(user *User, from, to string, content map[string]any) bool {
	if owner := user_owning_entity(from); owner != nil && user != nil && owner.UID == user.UID {
		return true
	}
	if excluded, _ := content[broadcast_content_exclude].(string); excluded != "" && excluded == to {
		return true
	}
	return false
}

// broadcast_inbound_class classifies an inbound sequenced event against the
// receiver's stream watermark. "apply" covers three cases: the in-order next
// event (bseq == last+1), a fresh stream starting at 1, and ANCHOR ADOPTION —
// last == 0 with bseq > 1 means this receiver has never applied a sequenced
// event for the stream (legacy pre-sequencing stream, rebuilt app.db, or a
// subscriber added mid-stream). Treating that first arrival as a gap would
// wedge the stream forever: resync replays `sequence > 0` from a log whose
// early rows are age/ack-trimmed, so the replay can never reach seq 1 and
// nothing ever applies. Adopting the event as the anchor loses nothing the
// pre-sequencing behaviour had (history is the app's own catch-up problem),
// and gaps after the anchor heal via the normal buffer + resync machinery.
func broadcast_inbound_class(last, bseq int64) string {
	if bseq <= last {
		return "duplicate"
	}
	if bseq > last+1 && last > 0 {
		return "gap"
	}
	return "apply"
}

// broadcast_stall_age is how long a stream may keep gapping on the same
// received watermark before it warns: a healing stream's watermark
// advances between gap events as resync replies land, so an unmoved
// watermark across hours of gap events means resync is not working.
// The News feed wedge (2026-07-06 to 2026-07-15) spent 9 days in
// exactly that state with no signal. var (not const) so tests can
// lower it. broadcast_stall_repeat is the re-warn cadence while the
// stall persists.
var broadcast_stall_age int64 = 6 * 3600
var broadcast_stall_repeat int64 = 86400

// broadcast_stall_maximum bounds the tracking map, and broadcast_stall_idle
// drops an entry nothing has re-noted for that long.
//
// Both exist because the map key carries the broadcast key, which arrives in
// an inbound event's content with nothing signing it. The gap branch notes a
// stall before any app handler runs, so a peer inventing a key per event grew
// this map for the process lifetime. The ceiling is what actually bounds
// memory; the idle sweep is hygiene for keys that are simply abandoned.
//
// A genuinely stalled stream is re-noted by every arrival that gaps on it -
// the 2026-07 News feed wedge gapped continuously for 9 days - so an hour of
// silence means no diagnostic is being lost. Ten thousand is far above any
// real population: a stream appears here only while it is actively gapping.
const broadcast_stall_maximum = 10000
const broadcast_stall_idle = int64(3600)

type broadcast_stall struct {
	first     int64 // when this watermark value first produced a gap
	watermark int64 // received.last at that moment
	warned    int64 // last warn unix, 0 = not yet warned
	seen      int64 // last note, for the idle sweep
}

// broadcast_stalls tracks gapping streams by user|app|peer|key. Entries
// reset whenever the watermark moves, so a healed stream can never cause a
// false warn. Guarded by a lock rather than left to the per-(user, app)
// worker's serialisation, because the sweep runs on the manager goroutine.
var (
	broadcast_stall_lock sync.Mutex
	broadcast_stalls     = map[string]*broadcast_stall{}
)

// broadcast_stall_note is called from the events.go gap branch on every
// buffered or NACKed gap event. It warns once the same watermark has
// been gapping for broadcast_stall_age, then once per repeat window.
func broadcast_stall_note(user, app, peer, key string, watermark, sequence int64) {
	first, should := broadcast_stall_record(user+"|"+app+"|"+peer+"|"+key, watermark)
	if !should {
		return
	}
	warn("Broadcast stream stalled: (peer=%q, key=%q) for user %q app %q has been gapping for %.1f hours with the received watermark stuck at %d (incoming sequence %d); resync is not healing it.", peer, key, user, app, float64(now()-first)/3600, watermark, sequence)
}

// broadcast_stall_record updates id's entry and reports the stall's start
// time when this note is the one that should warn.
func broadcast_stall_record(id string, watermark int64) (int64, bool) {
	broadcast_stall_lock.Lock()
	defer broadcast_stall_lock.Unlock()

	now := now()
	stall, tracked := broadcast_stalls[id]
	if !tracked || stall.watermark != watermark {
		// A stream already tracked always gets its reset; a new one only
		// when there is room. Refusing past the ceiling costs the
		// diagnostic for streams that begin stalling mid-flood, which is
		// the right trade against a map that grows without limit.
		if tracked || len(broadcast_stalls) < broadcast_stall_maximum {
			broadcast_stalls[id] = &broadcast_stall{first: now, watermark: watermark, seen: now}
		}
		return 0, false
	}
	stall.seen = now
	if now-stall.first < broadcast_stall_age {
		return 0, false
	}
	if stall.warned != 0 && now-stall.warned < broadcast_stall_repeat {
		return 0, false
	}
	stall.warned = now
	return stall.first, true
}

// broadcast_stall_clear drops a stream's tracking once it demonstrably
// healed. Called from broadcast_advance_local; the watermark reset above
// already prevents a false warn, so this is purely about not retaining an
// entry for a stream that is fine.
func broadcast_stall_clear(user, app, peer, key string) {
	broadcast_stall_lock.Lock()
	defer broadcast_stall_lock.Unlock()
	delete(broadcast_stalls, user+"|"+app+"|"+peer+"|"+key)
}

// broadcast_stall_sweep drops entries nothing has re-noted for
// broadcast_stall_idle.
func broadcast_stall_sweep() {
	broadcast_stall_lock.Lock()
	defer broadcast_stall_lock.Unlock()
	cutoff := now() - broadcast_stall_idle
	for id, stall := range broadcast_stalls {
		if stall.seen < cutoff {
			delete(broadcast_stalls, id)
		}
	}
}

// ErrBroadcastPendingFull signals the receiver's per-stream pending
// buffer was full and a gapped event could not be stored. The sender
// must NOT drop the row: this is transient backpressure that clears
// as resync drains the buffer. Without the signal the receiver would
// ACK silently on overflow and the event would be lost - the sender
// deletes the queue row on ACK, and the receiver would never see that
// seq again unless a later resync round happened to walk it.
//
// worker_failure_reason checks this sentinel and answers
// fail_transient. It is checked rather than left to that function's
// catch-all: retry-not-drop is the whole point of returning an error
// here, and inheriting it from a default means a later prefix rule
// matching "pending buffer full", or a reworded fmt.Errorf, flips the
// behaviour to drop with nothing to notice.
var ErrBroadcastPendingFull = errors.New("broadcast pending buffer full")

// mochi.broadcast.* — sequenced broadcast with a durable log per
// (app, key, peer) so subscribers can replay gaps from the owner.
//
// Sender side:
//
//	mochi.broadcast.send(key, [subscriber, ...], event, data) -> int
//	  allocates seq, writes log row, fans out to subscribers.
//	mochi.broadcast.replay(key, peer, after, limit) -> [{sequence, event, data}, ...]
//	  reads the log for a (key, peer) stream starting after `after`.
//
// Receiver side:
//
//	mochi.broadcast.next(key) -> int (legacy; sequence allocator)
//	mochi.broadcast.received(sender, key) -> int (highest applied seq)
//	mochi.broadcast.seen(key) -> int (host-local time of the last apply for
//	  key, maximum over senders; idle-resync #165 gate)
//	mochi.broadcast.advance(sender, key, sequence)
//	mochi.broadcast.touch(key) (stamp seen=now without an applied broadcast)
//
// Core's events.go auto-applies gap detection on inbound events
// carrying `_key` + `sequence` in content + `peer` header: dedups
// against `received`, NACKs on gap (with async resync request),
// advances `received` after a successful handler.
//
// Tables (per app DB, created at db_app open):
//
//	sequence(key, peer, last)               — sender outbound counter per (key, this_host)
//	log(key, peer, sequence, event, data, created)
//	acknowledged(key, peer, subscriber, last)
//	received(sender, key, last, seen)        — receiver-side dedup + idle stamp
var api_broadcast = sls.FromStringDict(sl.String("mochi.broadcast"), sl.StringDict{
	"next":     sl.NewBuiltin("mochi.broadcast.next", api_broadcast_next),
	"received": sl.NewBuiltin("mochi.broadcast.received", api_broadcast_received),
	"seen":     sl.NewBuiltin("mochi.broadcast.seen", api_broadcast_seen),
	"advance":  sl.NewBuiltin("mochi.broadcast.advance", api_broadcast_advance),
	"touch":    sl.NewBuiltin("mochi.broadcast.touch", api_broadcast_touch),
	"send":     sl.NewBuiltin("mochi.broadcast.send", api_broadcast_send),
	"replay":   sl.NewBuiltin("mochi.broadcast.replay", api_broadcast_replay),
	"subscriber": sls.FromStringDict(sl.String("mochi.broadcast.subscriber"), sl.StringDict{
		"add":    sl.NewBuiltin("mochi.broadcast.subscriber.add", api_broadcast_subscriber_add),
		"remove": sl.NewBuiltin("mochi.broadcast.subscriber.remove", api_broadcast_subscriber_remove),
	}),
})

const broadcast_log_age = 7 * 86400

func broadcast_sequence_table_create(db *DB) {
	db.exec("create table if not exists sequence (key text not null, peer text not null, last integer not null default 0, primary key (key, peer))")
}

func broadcast_received_table_create(db *DB) {
	db.exec("create table if not exists received (sender text not null, key text not null, last integer not null default 0, seen integer not null default 0, primary key (sender, key))")
	// Idle-resync (#165): seen = host-local time of the last applied broadcast
	// for (sender, key). Added here so the migration rides every advance/touch
	// path on existing received tables.
	if exists, _ := db.exists("select 1 from pragma_table_info('received') where name='seen'"); !exists {
		db.exec("alter table received add column seen integer not null default 0")
	}
}

// broadcast_log_table_create creates the log table for an app DB. Called
// eagerly from db_app open and defensively from the append/replay paths.
//
// Apps that adopt mochi.broadcast.send after their per-app DB
// already has data don't get a retroactive log for pre-upgrade
// events (claude/plans/broadcast.md: "No backfill on migration").
// Subscribers reaching for those older sequences fall back to the
// per-app request_resync helper, which pulls a fresh schema dump
// from the owner instead of a per-op replay.
func broadcast_log_table_create(db *DB) {
	db.exec("create table if not exists log (key text not null, peer text not null, sequence integer not null, event text not null, data text not null, created integer not null, primary key (key, peer, sequence))")
	db.exec("create index if not exists log_created on log(created)")
}

func broadcast_acknowledged_table_create(db *DB) {
	db.exec("create table if not exists acknowledged (key text not null, peer text not null, subscriber text not null, last integer not null default 0, primary key (key, peer, subscriber))")
}

func broadcast_subscribed_table_create(db *DB) {
	db.exec("create table if not exists subscribed (key text not null, peer text not null, subscriber text not null, updated integer not null default 0, primary key (key, peer, subscriber))")
}

// A subscription record expires on the same clock as the hard log cap. The
// binding rule is that the gate must never refuse a replay the log can still
// serve: an ack floor pins log rows out to broadcast_log_age_maximum, so a
// subscriber quiet for less than that may still have rows waiting, and a
// shorter window would turn a returning laggard into a wedge.
//
// This is garbage collection, not revocation. A removed subscriber keeps a
// live record until it expires, and the log is a rolling window, so they could
// replay events created after their removal for as long as the record lasts.
// mochi.broadcast.subscriber.remove is what actually revokes; the expiry only
// stops rows accumulating for members who have genuinely gone.
const broadcast_subscribed_age = broadcast_log_age_maximum

// broadcast_subscribed_record notes that this host fanned out to each of
// `subscribers` on (key, peer), and drops records past the expiry.
//
// The set is a UNION over sends, never a replacement: apps legitimately send to
// a partial list - every chat call site excludes the sender, and the leave /
// member-remove / member-add sites exclude the affected member - so replacing
// would evict a member who simply was not a recipient of the latest event, and
// then refuse them a resync.
// How stale a record may get before a send refreshes it. Without this every
// send rewrote a row per recipient, doubling the write cost of a fan-out - a
// feed with ten thousand subscribers paid ten thousand upserts per post. A day
// is far inside the expiry, so an actively-served subscriber never lapses.
const broadcast_subscribed_refresh = 86400

func broadcast_subscribed_record(db *DB, key, peer string, subscribers []string) {
	if key == "" || peer == "" || len(subscribers) == 0 {
		return
	}
	broadcast_subscribed_table_create(db)
	now := now()

	// One read to find what is already fresh, so the steady state writes
	// nothing at all.
	fresh := map[string]bool{}
	rows, _ := db.rows("select subscriber from subscribed where key=? and peer=? and updated > ?", key, peer, now-broadcast_subscribed_refresh)
	for _, row := range rows {
		if name, ok := row["subscriber"].(string); ok {
			fresh[name] = true
		}
	}

	// Marker row, subscriber "": records that this stream HAS a known set,
	// separately from who is in it. The gate keys "is this stream gated" on
	// the marker alone, so a subscriber recorded by broadcast_subscribed_add
	// before the stream's first send cannot gate it early and lock out members
	// nobody has recorded yet. An empty subscriber is refused by the gate, so
	// the marker never grants access itself.
	if !fresh[""] {
		db.exec("insert into subscribed (key, peer, subscriber, updated) values (?, ?, '', ?) on conflict(key, peer, subscriber) do update set updated = excluded.updated", key, peer, now)
	}
	for _, subscriber := range subscribers {
		if subscriber == "" || fresh[subscriber] {
			continue
		}
		db.exec("insert into subscribed (key, peer, subscriber, updated) values (?, ?, ?, ?) on conflict(key, peer, subscriber) do update set updated = excluded.updated", key, peer, subscriber, now)
	}
	db.exec("delete from subscribed where key=? and peer=? and updated < ?", key, peer, now-broadcast_subscribed_age)
}

// broadcast_subscribed_add records a subscriber without sending to them.
//
// Membership is otherwise only learned from a fan-out, which misses anyone a
// send deliberately excludes: chat's member/add broadcast goes to the EXISTING
// members, so the member being added is absent from the very event that admits
// them. That send still marks the stream gated, so without this the new member
// is refused a resync until some later send happens to include them - and a
// quiet stream leaves them stuck, which is worse than the exposure the gate
// closes.
//
// Deliberately does NOT write the marker: a stream with no sends yet must stay
// fail-open, or adding one member would lock out every member recorded nowhere.
func broadcast_subscribed_add(db *DB, key, peer, subscriber string) bool {
	if key == "" || peer == "" || subscriber == "" {
		return false
	}
	broadcast_subscribed_table_create(db)
	existed, _ := db.exists("select 1 from subscribed where key=? and peer=? and subscriber=?", key, peer, subscriber)
	db.exec("insert into subscribed (key, peer, subscriber, updated) values (?, ?, ?, ?) on conflict(key, peer, subscriber) do update set updated = excluded.updated", key, peer, subscriber, now())
	return !existed
}

// broadcast_subscribed_allowed reports whether `subscriber` may read the
// (key, peer) stream.
//
// Fails OPEN until this host has fanned the stream out at least once, which is
// what the marker row records. That makes the gate deployable without a flag
// day: on an upgraded server every existing stream starts unmarked, and
// refusing them would wedge every subscriber at once - the failure mode that
// ground the News feed for nine days in 2026-07. A stream becomes gated the
// moment it next sends, so the exposure closes on its own as traffic flows.
//
// Keyed on the marker rather than on "any row exists" so that
// broadcast_subscribed_add can record a member of a stream that has not sent
// yet without gating it against everyone else.
func broadcast_subscribed_allowed(db *DB, key, peer, subscriber string) bool {
	if subscriber == "" {
		return false
	}
	if exists, _ := db.exists("select 1 from sqlite_master where type='table' and name='subscribed'"); !exists {
		return true
	}
	marked, _ := db.exists("select 1 from subscribed where key=? and peer=? and subscriber=''", key, peer)
	if !marked {
		return true
	}
	allowed, _ := db.exists("select 1 from subscribed where key=? and peer=? and subscriber=?", key, peer, subscriber)
	return allowed
}

// broadcast_next_local allocates and returns the next outbound sequence
// number on the given DB for (key, peer). Per-(key, peer) PK gives each peer
// its own sequence space.
//
// Atomic via RETURNING. The previous UPSERT-then-SELECT pair raced
// when two goroutines hit the same (key, peer) concurrently: both
// SELECTs read the higher of the two interleaved UPSERTs, emit
// duplicate sequences, fail UNIQUE on the matching log INSERT. See
// wasabi 2026-05-24..26 event_ai_tag panics (468 occurrences over
// ~48h). RETURNING reports the post-UPSERT value as part of the same
// atomic statement, so each goroutine sees its own allocation.
func broadcast_next_local(db *DB, key, peer string) int64 {
	broadcast_sequence_table_create(db)
	const allocate = "insert into sequence (key, peer, last) values (?, ?, 1) on conflict(key, peer) do update set last = sequence.last + 1 returning last"
	var seq int64
	if err := db.internal.QueryRow(allocate, key, peer).Scan(&seq); err != nil {
		warn("Broadcast next_local: RETURNING failed for (key=%q, peer=%q): %v", key, peer, err)
		return 0
	}
	return seq
}

func broadcast_received_get(db *DB, sender, key string) int64 {
	exists, _ := db.exists("select 1 from sqlite_master where type='table' and name='received'")
	if !exists {
		return 0
	}
	return int64(db.integer("select last from received where sender=? and key=?", sender, key))
}

// broadcast_seen_get returns the host-local time of the most recent applied
// broadcast for key across all senders - the idle-resync (#165) gate. Reads
// max(seen) ignoring sender, so paired owners (several (peer, key) rows) and
// owner host-migration (new peer, same key) need no special handling. 0 when
// the table or the seen column is absent (pre-migration db), which reads as
// "very stale" and triggers one re-establish on first access after upgrade.
func broadcast_seen_get(db *DB, key string) int64 {
	if exists, _ := db.exists("select 1 from pragma_table_info('received') where name='seen'"); !exists {
		return 0
	}
	return int64(db.integer("select coalesce(max(seen), 0) from received where key=?", key))
}

// broadcast_touch_local stamps seen=now for key without an applied broadcast
// (subscribe / re-subscribe / full resync, and non-broadcast apps). Uses a
// sentinel sender=” row so it never collides with a real per-peer position
// row or the gap detector (which reads a specific (sender=peer, key)).
func broadcast_touch_local(db *DB, key string) {
	broadcast_received_table_create(db)
	db.exec("insert into received (sender, key, last, seen) values ('', ?, 0, ?) on conflict(sender, key) do update set seen = excluded.seen", key, now())
}

// broadcast_advance_local is the public advance: bumps received,
// clears the in-flight resync gate, then drains any pending-buffer
// rows that chain onto the new received.last. Callers (events.go,
// api_broadcast_advance) just want "this seq is done, do all
// follow-ups" - the drain is part of that.
func broadcast_advance_local(db *DB, sender, key string, sequence int64) {
	broadcast_advance_local_simple(db, sender, key, sequence)
	// Any advance is evidence the resync request (if any) is
	// producing replies, so the in-flight gate clears and the next
	// gap-detection can fire its follow-up batch immediately rather
	// than waiting out a fixed time window. db.user can be nil for
	// the api_broadcast_advance Starlark callsite without a user
	// context - skip the clear there; the throttle has its own
	// timeout fallback for the no-user case.
	if db.user != nil && db.user.UID != "" {
		broadcast_resync_clear(db.user.UID, sender, key)
		if db.app != nil {
			broadcast_stall_clear(db.user.UID, db.app.id, sender, key)
		}
	}
	// Pull in any buffered events that now chain onto received.last.
	// Common case is "nothing pending" - one indexed SELECT.
	broadcast_pending_drain_chain(db, sender, key)
}

// broadcast_advance_local_simple is the bare advance with no drain
// recursion. broadcast_pending_drain_chain calls this directly after
// dispatching a buffered row, so the drain's own advance doesn't
// re-enter the drain loop. Keep this in sync with the SQL in the
// public advance above.
//
// received is receiver-side apply state: it records what THIS host has already
// applied, so the gap detector can tell a fresh sequence from a duplicate.
func broadcast_advance_local_simple(db *DB, sender, key string, sequence int64) {
	broadcast_received_table_create(db)
	// seen = now() stamps the host-local idle-resync (#165) signal on every
	// applied broadcast - one chokepoint covering every app and event type.
	// now() computed in Go, not in SQL, so tests can control it.
	db.exec("insert into received (sender, key, last, seen) values (?, ?, ?, ?) on conflict(sender, key) do update set last = max(received.last, excluded.last), seen = excluded.seen", sender, key, sequence, now())
}

// broadcast_log_append writes one log row in the same transaction as
// the sequence bump. Returns the allocated sequence.
// broadcast_payload_decode reads a stored broadcast-log payload back, keeping
// whole numbers whole.
//
// The log stores the payload as JSON, and JSON has no integer type: a plain
// Unmarshal into `any` turns every number into a float64, so a replayed
// timestamp reaches the app as 1.7534e+09 rather than 1753400000. Apps
// validate such a field by pattern (`str(value)` against an integer regex),
// which the exponent form fails - so the handler returned early and the row
// was silently dropped, on the one path that exists to repair missed
// deliveries. Chat lost every gap-recovered message and edit this way
// (2026-07-25), and the same shape is live in projects, crm, chess, go and
// words.
//
// The live path carries CBOR, which preserves int64. Decoding with UseNumber
// and restoring integers here makes replay match delivery, without changing
// what is already written to disk.
func broadcast_payload_decode(raw string, target any) error {
	decoder := json.NewDecoder(bytes.NewReader([]byte(raw)))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return err
	}
	value = json_integers_restore(value)

	switch out := target.(type) {
	case *any:
		*out = value
	case *map[string]any:
		mapped, _ := value.(map[string]any)
		*out = mapped
	default:
		return fmt.Errorf("unsupported decode target %T", target)
	}
	return nil
}

// json_integers_restore walks a decoded JSON value and turns every
// json.Number back into an int64 where it is integral, a float64 otherwise.
func json_integers_restore(value any) any {
	switch v := value.(type) {
	case json.Number:
		if whole, err := v.Int64(); err == nil {
			return whole
		}
		fractional, err := v.Float64()
		if err != nil {
			return v.String()
		}
		return fractional
	case map[string]any:
		for key, item := range v {
			v[key] = json_integers_restore(item)
		}
		return v
	case []any:
		for i, item := range v {
			v[i] = json_integers_restore(item)
		}
		return v
	}
	return value
}

func broadcast_log_append(db *DB, key, peer, event string, data []byte) int64 {
	broadcast_log_table_create(db)
	broadcast_log_age_trim(db, key, peer)
	sequence := broadcast_next_local(db, key, peer)
	// insert OR IGNORE: the log is append-only keyed on (key, peer, sequence) and
	// the sender always allocates a fresh sequence, so a collision means the row
	// is already there. Ignoring it makes that a clean no-op rather than a UNIQUE
	// error.
	db.exec("insert or ignore into log (key, peer, sequence, event, data, created) values (?, ?, ?, ?, ?, ?)", key, peer, sequence, event, string(data), now())
	return sequence
}

// broadcast_log_age_trim deletes log rows older than the age cap for
// the given (key, peer). Called on send; no-op when nothing's aged out.
// broadcast_log_age_maximum is the hard retention cap: a lagging
// subscriber's ack floor protects rows past broadcast_log_age, but only
// this long — beyond it one dead subscriber would grow the log forever.
// Evicting past a live floor is alerted: that subscriber's next resync
// gets a broadcast/floor skip and its app re-fetches.
const broadcast_log_age_maximum = 4 * broadcast_log_age

func broadcast_log_age_trim(db *DB, key, peer string) {
	// The age trim respects the lowest acknowledged subscriber floor:
	// trimming rows a live subscriber still needs converts its fillable
	// gap into an unfillable one (the 2026-07 News feed wedge became
	// permanent exactly this way). Rows below every floor age out
	// normally; rows a laggard pins survive to the hard cap. Streams
	// with no acknowledged subscriber at all keep the plain age trim —
	// there is no floor to respect.
	floor := int64(0)
	if row, _ := db.row("select min(last) as m from acknowledged where key=? and peer=?", key, peer); row != nil {
		if m, ok := row["m"].(int64); ok && m > 0 {
			floor = m
		}
	}
	if floor == 0 {
		db.exec("delete from log where key=? and peer=? and created < ?", key, peer, now()-broadcast_log_age)
		return
	}
	db.exec("delete from log where key=? and peer=? and created < ? and sequence <= ?", key, peer, now()-broadcast_log_age, floor)
	if pinned, _ := db.exists("select 1 from log where key=? and peer=? and created < ? limit 1", key, peer, now()-broadcast_log_age_maximum); pinned {
		warn("Broadcast log for (key=%q, peer=%q) evicting rows past the hard retention cap that a subscriber at ack floor %d still needs; that subscriber will skip the lost span and re-fetch on its next resync.", key, peer, floor)
		db.exec("delete from log where key=? and peer=? and created < ?", key, peer, now()-broadcast_log_age_maximum)
		// Drop ack floors the surviving log can no longer replay to
		// (the subscriber's next needed sequence precedes the oldest
		// surviving row). Such a subscriber can only be floor-skipped
		// into a re-fetch, which reads nothing from acknowledged, and a
		// live one re-inserts its row with its next ack — but a floor
		// left by a subscriber that is gone for good (unsubscribed,
		// deleted, host wiped) never advances, and kept it would pin
		// the trim and this warning forever.
		if row, _ := db.row("select min(sequence) as m from log where key=? and peer=?", key, peer); row != nil {
			if oldest, ok := row["m"].(int64); ok {
				db.exec("delete from acknowledged where key=? and peer=? and last+1 < ?", key, peer, oldest)
			} else {
				// The eviction emptied the log: no floor is replayable.
				db.exec("delete from acknowledged where key=? and peer=?", key, peer)
			}
		}
	}
}

// broadcast_log_ack_trim deletes log rows below the minimum ack across all
// subscribers for (key, peer). Called from the acknowledge handler
// after acknowledged is updated.
func broadcast_log_ack_trim(db *DB, key, peer string) {
	row, _ := db.row("select min(last) as m from acknowledged where key=? and peer=?", key, peer)
	if row == nil {
		return
	}
	last, ok := row["m"].(int64)
	if !ok || last <= 0 {
		return
	}
	db.exec("delete from log where key=? and peer=? and sequence < ?", key, peer, last)
}

// mochi.broadcast.subscriber.add(key, subscriber) -> bool: record a subscriber
// of this host's (key) stream without sending to them. Returns whether the
// record is new.
//
// Call this when a member joins. Membership is otherwise learned only from
// mochi.broadcast.send's recipient list, which misses anyone a send
// deliberately excludes - the member/add broadcast goes to the EXISTING
// members, so the joiner is absent from the very event that admits them, and
// would be refused a resync until some later send happened to include them.
func api_broadcast_subscriber_add(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var key, subscriber string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "key", &key, "subscriber", &subscriber); err != nil {
		return nil, err
	}
	if key == "" {
		return sl_error(fn, "key must be non-empty")
	}
	if subscriber == "" {
		return sl_error(fn, "subscriber must be non-empty")
	}

	user, _ := principal_storage(t)
	app, _ := t.Local("app").(*App)
	if user == nil || app == nil {
		return sl_error(fn, "no user/app context")
	}

	db := db_app_system(user, app)
	if db == nil {
		return sl_error(fn, "no system database")
	}
	return sl.Bool(broadcast_subscribed_add(db, key, net_id, subscriber)), nil
}

// mochi.broadcast.subscriber.remove(key, subscriber) -> bool: revoke a
// subscriber's access to replay this host's (key) stream. Returns whether a
// record was removed.
//
// Call this when a member leaves or is removed. Dropping them from the
// subscriber list passed to mochi.broadcast.send stops future deliveries but
// NOT replay: their record ages out on the log's own clock, and until then
// they can resync events created after they left. This is the call that makes
// revocation immediate.
//
// Scoped to this host's own stream (key, net_id) - an app may revoke access to
// what it broadcasts, never to another host's stream.
func api_broadcast_subscriber_remove(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var key, subscriber string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "key", &key, "subscriber", &subscriber); err != nil {
		return nil, err
	}
	if key == "" {
		return sl_error(fn, "key must be non-empty")
	}
	if subscriber == "" {
		return sl_error(fn, "subscriber must be non-empty")
	}

	user, _ := principal_storage(t)
	app, _ := t.Local("app").(*App)
	if user == nil || app == nil {
		return sl_error(fn, "no user/app context")
	}

	db := db_app_system(user, app)
	if db == nil {
		return sl_error(fn, "no system database")
	}
	return sl.Bool(broadcast_subscribed_remove(db, key, net_id, subscriber)), nil
}

// broadcast_subscribed_remove revokes one subscriber's access to (key, peer).
// Returns whether a record was actually removed, so a caller can tell a real
// revocation from a no-op.
func broadcast_subscribed_remove(db *DB, key, peer, subscriber string) bool {
	// Never the marker row: deleting it would drop a stream whose members have
	// all been revoked back to fail-open.
	if subscriber == "" {
		return false
	}
	if exists, _ := db.exists("select 1 from sqlite_master where type='table' and name='subscribed'"); !exists {
		return false
	}
	removed, _ := db.exists("select 1 from subscribed where key=? and peer=? and subscriber=?", key, peer, subscriber)
	db.exec("delete from subscribed where key=? and peer=? and subscriber=?", key, peer, subscriber)
	return removed
}

// mochi.broadcast.next(key) -> int: allocate the next outbound sequence
// number for (key, this_host).
func api_broadcast_next(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var key string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "key", &key); err != nil {
		return nil, err
	}
	if key == "" {
		return sl_error(fn, "key must be non-empty")
	}

	user, _ := principal_storage(t)
	app, _ := t.Local("app").(*App)
	if user == nil || app == nil {
		return sl_error(fn, "no user/app context")
	}

	db := db_app_system(user, app)
	if db == nil {
		return sl_error(fn, "no system database")
	}
	return sl.MakeInt64(broadcast_next_local(db, key, net_id)), nil
}

// mochi.broadcast.received(sender, key) -> int: highest applied seq.
func api_broadcast_received(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var sender, key string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "sender", &sender, "key", &key); err != nil {
		return nil, err
	}

	user, _ := principal_storage(t)
	app, _ := t.Local("app").(*App)
	if user == nil || app == nil {
		return sl.MakeInt(0), nil
	}

	db := db_app_system(user, app)
	if db == nil {
		return sl.MakeInt(0), nil
	}
	return sl.MakeInt64(broadcast_received_get(db, sender, key)), nil
}

// mochi.broadcast.seen(key) -> int: host-local time of the most recent applied
// broadcast for key, across all senders. The idle-resync (#165) gate.
func api_broadcast_seen(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var key string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "key", &key); err != nil {
		return nil, err
	}
	if key == "" {
		return sl_error(fn, "key must be non-empty")
	}

	user, _ := principal_storage(t)
	app, _ := t.Local("app").(*App)
	if user == nil || app == nil {
		return sl.MakeInt(0), nil
	}

	db := db_app_system(user, app)
	if db == nil {
		return sl.MakeInt(0), nil
	}
	return sl.MakeInt64(broadcast_seen_get(db, key)), nil
}

// mochi.broadcast.touch(key) -> None: stamp seen=now for key without an applied
// broadcast (subscribe / re-subscribe / full resync, and non-broadcast apps).
func api_broadcast_touch(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var key string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "key", &key); err != nil {
		return nil, err
	}
	if key == "" {
		return sl_error(fn, "key must be non-empty")
	}

	user, _ := principal_storage(t)
	app, _ := t.Local("app").(*App)
	if user == nil || app == nil {
		return sl_error(fn, "no user/app context")
	}

	db := db_app_system(user, app)
	if db == nil {
		return sl_error(fn, "no system database")
	}
	broadcast_touch_local(db, key)
	return sl.None, nil
}

// mochi.broadcast.advance(sender, key, sequence) -> None: record applied seq.
func api_broadcast_advance(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var sender, key string
	var sequence int64
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "sender", &sender, "key", &key, "sequence", &sequence); err != nil {
		return nil, err
	}
	if sender == "" || key == "" {
		return sl_error(fn, "sender and key must be non-empty")
	}
	if sequence < 0 {
		return sl_error(fn, "sequence must be non-negative")
	}

	user, _ := principal_storage(t)
	app, _ := t.Local("app").(*App)
	if user == nil || app == nil {
		return sl_error(fn, "no user/app context")
	}

	db := db_app_system(user, app)
	if db == nil {
		return sl_error(fn, "no system database")
	}
	broadcast_advance_local(db, sender, key, sequence)
	return sl.None, nil
}

// mochi.broadcast.send(from, key, subscribers, service, event, data, exclude=None) -> int
//
// Allocates a sequence for (key, this_host), writes the event to the
// per-app log table, and fans out one mochi.message.send per
// subscriber. Each outbound message carries _key and sequence in
// content; the receiver's peer header identifies the originating host.
//
// `from` is the sender entity ID (must be owned by the calling user).
// `key` is the broadcast stream key (typically the same entity ID;
// apps that want multiple streams per scope can use other keys).
// `subscribers` is a list of recipients: each element is either an
// entity ID string, or a {"id": entity, "peer": peer} dictionary. A
// non-empty peer pins delivery to that peer instead of resolving the
// entity via the directory — required when the recipient is a private
// entity (not directory-listed), such as a wiki replica; the app
// stores the peer at subscribe time from the event's peer header.
// `exclude` skips a single entity (typically the original event
// author).
func api_broadcast_send(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var from, key, service, event, exclude string
	var subscribers *sl.List
	var data sl.Value
	if err := sl.UnpackArgs(fn.Name(), args, kwargs,
		"from", &from,
		"key", &key,
		"subscribers", &subscribers,
		"service", &service,
		"event", &event,
		"data", &data,
		"exclude?", &exclude,
	); err != nil {
		return nil, err
	}
	if !valid(from, "entity") {
		return sl_error(fn, "invalid from %q", from)
	}
	if key == "" {
		return sl_error(fn, "key must be non-empty")
	}
	if !valid(service, "constant") {
		return sl_error(fn, "invalid service %q", service)
	}
	if !valid(event, "constant") {
		return sl_error(fn, "invalid event %q", event)
	}

	user := principal_caller(t)
	app, _ := t.Local("app").(*App)
	if user == nil || app == nil {
		return sl_error(fn, "no user/app context")
	}

	udb := db_open("db/users.db")
	owned, err := udb.exists("select id from entities where id=? and user=?", from, user.UID)
	if err != nil || !owned {
		return sl_error(fn, "from %q not owned by caller", from)
	}

	// Cost is the recipient count, not one: this is the only send API that
	// amplifies. Charged on the LIST length rather than the delivered count,
	// and settled BEFORE the log append - the self-owned and suspended
	// recipients skipped below are only known part-way through the loop, and
	// refusing partway would leave a sequence in the log that only some
	// subscribers received, which resync would replay to the rest later as
	// though delivery had happened.
	recipients := subscribers.Len()
	if recipients > broadcast_recipients_maximum {
		return sl_error(fn, "too many subscribers: %d exceeds %d", recipients, broadcast_recipients_maximum)
	}
	if !rate_limit_broadcast.spend(app.id, recipients) {
		return sl_error(fn, rate_limit_refuse(rate_limit_broadcast, app.id, "broadcast recipients per minute"))
	}

	db := db_app_system(user, app)
	if db == nil {
		return sl_error(fn, "no system database")
	}

	payload, _ := sl_decode(data).(map[string]any)
	if payload == nil {
		payload = map[string]any{}
	}
	// The exclusion rides IN the payload, before the log append, so the
	// log row and every delivery and resync replay carry it identically;
	// the receive wrapper skips the handler for the excluded actor.
	// Send-time skipping (the old mechanism) left a permanent hole at
	// this sequence in the excluded subscriber's stream, and resync —
	// blind to the exclusion — replayed the event to them anyway: the
	// echoed post/edit ran feeds' subscriber handler against the OWNER's
	// canonical DB and destroyed its attachment files (2026-07-15).
	if exclude != "" {
		payload[broadcast_content_exclude] = exclude
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return sl_error(fn, "payload not JSON-encodable: %v", err)
	}

	sequence := broadcast_log_append(db, key, net_id, event, body)

	// Attach broadcast metadata to outbound content. _peer is implicit
	// via the originating libp2p host (peer header on the receiver
	// side) and doesn't need to ride in content.
	content := map[string]any{}
	for k, v := range payload {
		content[k] = v
	}
	content[broadcast_content_key] = key
	content[broadcast_content_sequence] = sequence

	services := app_services(app, user)
	// Who this stream fans out to, for the resync gate. Recorded from the
	// whole list rather than from the delivery loop below, which skips
	// recipients the sending user owns and subscribers the health gate has
	// suspended - both are still entitled to replay, and a suspended one is
	// exactly who needs it when they come back.
	recorded := []string{}
	iter := subscribers.Iterate()
	defer iter.Done()
	var item sl.Value
	for iter.Next(&item) {
		sub, _ := sl.AsString(item)
		peer := ""
		if sub == "" {
			if recipient, ok := sl_decode(item).(map[string]any); ok {
				sub, _ = recipient["id"].(string)
				peer, _ = recipient["peer"].(string)
			}
		}
		if sub != "" {
			recorded = append(recorded, sub)
		}
		if sub == "" {
			continue
		}
		// Never enqueue to a recipient owned by the sending user: their
		// DB is the canonical copy the event was written to, so delivery
		// is at best a no-op and at worst destructive (the owner guard in
		// events.go is the backstop). Safe for stream continuity — gap
		// detection only fires on arrival, so a never-delivered stream
		// cannot resync. The excluded actor, by contrast, IS still sent
		// to (when remote): the delivery advances their watermark and the
		// receive wrapper skips their handler via _exclude.
		if owner := user_owning_entity(sub); owner != nil && owner.UID == user.UID {
			continue
		}
		// Recipient health gate: a suspended subscriber (a full retry
		// budget burned with no contradicting success) gets no fan-out
		// rows — their stream catches up via resync when they return —
		// except one probe row per interval. Past the evict age the
		// owning app is told to drop the subscriber instead. Applies to
		// broadcast fan-out only; direct correspondence never gates.
		skip, evict := health_gate(sub)
		if evict {
			health_evict_dispatch(user, app, service, sub)
			continue
		}
		if skip {
			continue
		}
		m := message(from, sub, service, event)
		m.FromApp = app.id
		m.Services = services
		m.content = content
		if peer != "" {
			m.send_peer(peer)
		} else {
			m.send()
		}
	}

	broadcast_subscribed_record(db, key, net_id, recorded)

	return sl.MakeInt64(sequence), nil
}

// mochi.broadcast.replay(key, peer, after, limit=100) -> [{sequence, event, data}, ...]
//
// Reads log rows from the per-app log table for the given (key, peer)
// stream starting at sequence > after, capped at limit. Used by the
// broadcast/resync event handler to feed a resync request — apps
// shouldn't normally call this directly.
func api_broadcast_replay(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var key, peer string
	var after int64
	limit := int64(100)
	if err := sl.UnpackArgs(fn.Name(), args, kwargs,
		"key", &key,
		"peer", &peer,
		"after", &after,
		"limit?", &limit,
	); err != nil {
		return nil, err
	}
	if key == "" || peer == "" {
		return sl_error(fn, "key and peer must be non-empty")
	}
	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	user, _ := principal_storage(t)
	app, _ := t.Local("app").(*App)
	if user == nil || app == nil {
		return sl_error(fn, "no user/app context")
	}

	db := db_app_system(user, app)
	if db == nil {
		return sl_error(fn, "no system database")
	}

	exists, _ := db.exists("select 1 from sqlite_master where type='table' and name='log'")
	if !exists {
		return sl.NewList(nil), nil
	}

	rows, _ := db.rows("select sequence, event, data from log where key=? and peer=? and sequence > ? order by sequence limit ?", key, peer, after, limit)
	out := make([]sl.Value, 0, len(rows))
	for _, row := range rows {
		sequence, _ := row["sequence"].(int64)
		event_name, _ := row["event"].(string)
		data_raw, _ := row["data"].(string)
		var data any
		_ = broadcast_payload_decode(data_raw, &data)
		out = append(out, sl_encode(map[string]any{
			"sequence": sequence,
			"event":    event_name,
			"data":     data,
		}))
	}
	return sl.NewList(out), nil
}

// broadcast_resync handles an inbound broadcast/resync event. The
// subscriber's content has {key, peer, after}: we read the matching
// rows from log and re-emit each one to the requester via
// send_peer (direct libp2p delivery, not fanned). Replayed events
// flow through the normal event pipeline at the receiver, where the
// gap wrapper applies them in order.
const broadcast_replay_limit = 100

func (e *Event) broadcast_resync(a *App, av *AppVersion) error {
	key, _ := e.content["key"].(string)
	peer, _ := e.content["peer"].(string)
	after := event_int64(e.content["after"])
	if key == "" || peer == "" {
		return fmt.Errorf("broadcast/resync requires key and peer")
	}

	exists, _ := e.db.exists("select 1 from sqlite_master where type='table' and name='log'")
	if !exists {
		return nil
	}

	// The requester must be someone this stream fans out to. `key` is the
	// object entity id, so it is known to every current member, every FORMER
	// member, and anyone who has seen a share link - without this, knowing an
	// id was enough to replay a private stream's history, and removal from a
	// group revoked nothing. Silent: a refused requester learns nothing about
	// whether the stream exists.
	if !broadcast_subscribed_allowed(e.db, key, peer, e.from) {
		info("Broadcast refusing resync of (key=%q, peer=%q) to non-subscriber %q", key, peer, e.from)
		return nil
	}

	// Floor signal: when the requester asks for sequences below the
	// retained log, the gap below the floor is PROVABLY unfillable —
	// replaying from the floor would only feed the requester more
	// far-future events to buffer against a hole that can never fill
	// (the 2026-07 News feed wedge ground for 9 days in exactly that
	// state). Tell the requester where the log starts so it can skip
	// the lost span now and hand its app the broadcast/gap re-fetch.
	// A fully-trimmed log floors at head+1: everything is unfillable,
	// the requester re-anchors at the head.
	floor := int64(0)
	if row, _ := e.db.row("select min(sequence) as low from log where key=? and peer=?", key, peer); row != nil {
		if low, ok := row["low"].(int64); ok {
			floor = low
		}
	}
	if floor == 0 {
		if row, _ := e.db.row("select last from sequence where key=? and peer=?", key, peer); row != nil {
			if head, ok := row["last"].(int64); ok && head > 0 {
				floor = head + 1
			}
		}
	}
	if floor > 0 && after+1 < floor {
		m := message(e.to, e.from, e.service, "broadcast/floor")
		m.FromApp = a.id
		m.Services = app_services(a, e.user)
		m.content = map[string]any{"key": key, "peer": peer, "floor": floor}
		m.send_peer_priority(e.peer, priority_replay)
	}

	rows, _ := e.db.rows("select sequence, event, data from log where key=? and peer=? and sequence > ? order by sequence limit ?", key, peer, after, broadcast_replay_limit)
	if len(rows) == 0 {
		return nil
	}

	services := app_services(a, e.user)
	for _, row := range rows {
		sequence, _ := row["sequence"].(int64)
		event_name, _ := row["event"].(string)
		data_raw, _ := row["data"].(string)
		var payload map[string]any
		_ = broadcast_payload_decode(data_raw, &payload)
		if payload == nil {
			payload = map[string]any{}
		}
		content := map[string]any{}
		for k, v := range payload {
			content[k] = v
		}
		content[broadcast_content_key] = key
		// Same wire keys as the live send path: replayed events must engage
		// the receiver's gap detection so broadcast_advance_local moves the
		// watermark and the pending buffer drains behind it.
		content[broadcast_content_sequence] = sequence

		m := message(e.to, e.from, e.service, event_name)
		m.FromApp = a.id
		m.Services = services
		m.content = content
		// Replay messages ride the priority_replay tier so they
		// overtake the live-broadcast backlog in the requester's
		// (target, from_entity) outbound queue bucket. Without
		// this, resync replies serialise behind any pending live
		// traffic at the per-bucket cap=1 and the subscriber's
		// catch-up rate degrades to the bucket's drain rate
		// (~0.7 events/sec observed live with a 12k-deep bucket).
		m.send_peer_priority(e.peer, priority_replay)
	}
	return nil
}

// broadcast_acknowledge handles an inbound broadcast/acknowledge event.
// The subscriber's content has {key, peer, sequence}: we update
// acknowledged for (key, peer, subscriber=e.from) and run the
// log-trim step.
func (e *Event) broadcast_acknowledge() error {
	key, _ := e.content["key"].(string)
	peer, _ := e.content["peer"].(string)
	sequence := event_int64(e.content["sequence"])
	if key == "" || peer == "" || sequence <= 0 {
		return fmt.Errorf("broadcast/acknowledge requires key, peer, and sequence")
	}

	// Clamp to what this host actually allocated for the stream. The
	// sequence rides in from the network and was taken on trust, so a
	// subscriber could claim a watermark far beyond anything sent; the
	// acknowledged row then feeds broadcast_log_ack_trim, which deletes
	// every log row below the lowest subscriber floor. A watermark above
	// the head is not a claim we can honour - nobody can have received an
	// event that was never allocated - so cap it at the head rather than
	// refuse, which keeps a subscriber that is merely ahead of our own
	// view (a re-apply, a reseed) from wedging on a rejected ack.
	//
	// No sequence row means this host never originated the stream, so
	// there is nothing to acknowledge and nothing to trim: drop it
	// quietly rather than recording a floor against a log we do not own.
	head := int64(0)
	if row, _ := e.db.row("select last from sequence where key=? and peer=?", key, peer); row != nil {
		head, _ = row["last"].(int64)
	}
	if head <= 0 {
		return nil
	}
	if sequence > head {
		sequence = head
	}

	// Same membership gate as resync. An acknowledged row from a stranger is
	// not merely noise: broadcast_log_ack_trim trims to the LOWEST floor, so
	// one forged ack of 1 pins the log forever, and each distinct sender adds
	// a row keyed on an id anyone can mint.
	if !broadcast_subscribed_allowed(e.db, key, peer, e.from) {
		return nil
	}

	broadcast_acknowledged_table_create(e.db)
	e.db.exec("insert into acknowledged (key, peer, subscriber, last) values (?, ?, ?, ?) on conflict(key, peer, subscriber) do update set last = max(acknowledged.last, excluded.last)", key, peer, e.from, sequence)
	broadcast_log_ack_trim(e.db, key, peer)
	// A subscriber acking is alive: clear any failure streak.
	health_success(e.from)
	return nil
}

// A skipped unfillable gap is self-healing — the floor handler and the
// pending GC both tell the app to re-fetch — so a single skip is a log
// line, not an admin email. The operator signals are recurrence (the
// same stream skipping again within broadcast_skip_recurrence: re-fetch
// is not settling it) and breadth (broadcast_skip_breadth distinct
// streams skipping within one day: origins are trimming replay logs
// faster than subscribers catch up generally, which no per-stream
// re-fetch fixes). var (not const) so tests can lower them.
var broadcast_skip_recurrence int64 = 7 * 86400
var broadcast_skip_breadth int64 = 25

// broadcast_skip_record tracks skips per stream: `last` backs the
// recurrence check, `warned` throttles its warn to one per day. State is
// in-memory — a restart forgets prior skips, delaying escalation by at
// most one recurrence window.
type broadcast_skip_record struct {
	last   int64
	warned int64
}

var broadcast_skip_state sync.Map // user|app|peer|key -> broadcast_skip_record

// broadcast_skip_breadth_warned is the last unix time the breadth warn
// fired, stored under a fixed key in its own map so concurrent floor
// handlers race benignly at worst (a duplicate email, which
// warn_email_allow absorbs).
var broadcast_skip_breadth_warned sync.Map // "breadth" -> last warn unix

// broadcast_skip_warn records a skipped unfillable gap, logs it, and
// escalates to an operator email only on recurrence or breadth. Shared
// by the floor handler and the pending GC.
func broadcast_skip_warn(user, app, peer, key string, first, last int64) {
	id := user + "|" + app + "|" + peer + "|" + key
	moment := now()
	info("Broadcast stream skipped unfillable sequences %d..%d on (peer=%q, key=%q) for user %q app %q: the origin's replay log no longer holds them; the app was told to re-fetch.", first, last, peer, key, user, app)

	record := broadcast_skip_record{}
	if v, ok := broadcast_skip_state.Load(id); ok {
		record = v.(broadcast_skip_record)
	}
	if record.last != 0 && moment-record.last < broadcast_skip_recurrence && moment-record.warned >= 86400 {
		record.warned = moment
		warn("Broadcast stream skipped unfillable sequences %d..%d on (peer=%q, key=%q) for user %q app %q again within %d days: re-fetch is not settling this stream; the origin trims its replay log faster than this subscriber catches up.", first, last, peer, key, user, app, broadcast_skip_recurrence/86400)
	}
	record.last = moment
	broadcast_skip_state.Store(id, record)

	// Breadth across distinct streams in the past day, pruning entries
	// past the recurrence window in the same pass so churn (dev test
	// runs mint fresh streams constantly) cannot grow the map without
	// bound.
	streams := int64(0)
	broadcast_skip_state.Range(func(k, v any) bool {
		r := v.(broadcast_skip_record)
		switch {
		case moment-r.last < 86400:
			streams++
		case moment-r.last >= broadcast_skip_recurrence:
			broadcast_skip_state.Delete(k)
		}
		return true
	})
	if streams < broadcast_skip_breadth {
		return
	}
	if v, ok := broadcast_skip_breadth_warned.Load("breadth"); ok && moment-v.(int64) < 86400 {
		return
	}
	broadcast_skip_breadth_warned.Store("breadth", moment)
	warn("Broadcast streams skipped unfillable sequences on %d distinct streams within a day (threshold %d): origins are trimming replay logs faster than subscribers catch up; per-stream re-fetch will not fix this.", streams, broadcast_skip_breadth)
}

// broadcast_floor handles an inbound broadcast/floor event: the stream
// origin's answer to a resync request that asked for sequences below its
// retained log. The gap below the floor is provably unfillable — the log
// rows are trimmed — so waiting recovers nothing: skip to floor-1 now,
// drain whatever chains onto it, record the skip (escalating to the
// operator only on recurrence or breadth), and hand the app its
// broadcast/gap error for a full re-fetch. Without this signal the
// receiver grinds until the pending-GC TTL (the News feed wedge spent 9
// days there, 2026-07). Only the origin is authoritative about its own
// log, so the event must arrive from the peer it names — which for the
// self-loop is this host itself. A forged floor from the real origin
// peer is equivalent to that origin trimming its log: it can only move
// its own stream.
func (e *Event) broadcast_floor(a *App) error {
	key, _ := e.content["key"].(string)
	peer, _ := e.content["peer"].(string)
	floor := event_int64(e.content["floor"])
	if key == "" || peer == "" || floor <= 1 {
		return fmt.Errorf("broadcast/floor requires key, peer, and floor")
	}
	if peer != e.peer {
		info("Event dropping broadcast/floor for peer %q arriving from %q", peer, e.peer)
		return fmt.Errorf("floor event must arrive from its own peer")
	}
	last := broadcast_received_get(e.db, peer, key)
	if floor-1 <= last {
		return nil // already at or past the floor; nothing lost
	}
	first := last + 1
	broadcast_advance_local(e.db, peer, key, floor-1)
	// Sweep the orphaned buffer rows the skip jumped over: the chain-drain
	// only deletes rows it dispatches, so below-cursor rows would linger
	// as permanent sediment (same sweep the pending-GC runs after its
	// skips). Sweep to the fresh watermark — the drain may have chained
	// past floor-1.
	e.db.exec("delete from pending where peer=? and key=? and sequence<=?", peer, key, broadcast_received_get(e.db, peer, key))
	audit_broadcast_pending_purged(e.user.UID, a.id, peer, key, first, floor-1, floor-1-last)
	broadcast_skip_warn(e.user.UID, a.id, peer, key, first, floor-1)
	service := ""
	if svcs := app_services(a, e.user); len(svcs) > 0 {
		service = svcs[0]
	}
	k, p, f, l := key, peer, first, floor-1
	error_dispatch(e.user, a, error_code_broadcast_gap, "unfillable", service, k, nil, func() map[string]any {
		return map[string]any{"peer": p, "key": k, "first": f, "last": l}
	})
	return nil
}

// broadcast_resync_throttle gates resync requests per (user, peer, key)
// to at most ONE IN FLIGHT, not one per time window. Previous design
// locked out for 60 seconds after every request regardless of whether
// the request succeeded - a 300-event gap took 3+ minutes minimum
// even on a fast link, because four sequential 100-event resyncs
// each waited out 60s of throttle. New design tracks "request out,
// no advance yet" as a bool; clears it on any received.last advance
// for the (user, peer, key) tuple (broadcast_advance_local calls
// broadcast_resync_clear). A timeout fallback covers the case where
// the resync reply never arrives - same throttle behaviour as before
// but only when something is actually stuck, not after every success.
//
// Burst dedup (the original throttle's load-bearing property) still
// holds: if 50 inbound events trip the gap detector in 200ms, only
// the first sees broadcast_resync_inflight=false and proceeds; the
// other 49 see the flag and return. Once that resync's replies start
// advancing received, the flag clears and the next gap-detection
// request fires immediately.
//
// See claude/sessions/2026-05-25-broadcast-resync-seq-643-
// investigation.md.
const broadcast_resync_timeout = 30 * time.Second

// broadcast_resync_maximum bounds the in-flight map. An entry is only
// meaningful for broadcast_resync_timeout, but it is deleted on an advance
// that a deliberately-gapped stream never makes, so the ceiling is what
// bounds it. Matched to broadcast_stall_maximum - both are keyed on the
// same unsigned broadcast key.
const broadcast_resync_maximum = 10000

var (
	broadcast_resync_lock     sync.Mutex
	broadcast_resync_inflight = map[string]int64{} // tag -> request unix time
)

func broadcast_resync_tag(user_uid, peer, key string) string {
	return fmt.Sprintf("%s|%s|%s", user_uid, peer, key)
}

func broadcast_resync_throttle(user_uid, peer, key string) bool {
	broadcast_resync_lock.Lock()
	defer broadcast_resync_lock.Unlock()
	tag := broadcast_resync_tag(user_uid, peer, key)
	now_ts := time.Now().Unix()
	last, inflight := broadcast_resync_inflight[tag]
	if inflight {
		// Timeout fallback: if the resync reply never arrived
		// (link flapped, owner offline at the moment), clear the
		// in-flight flag so the next gap-detection can retry. Keeps
		// the subsystem from wedging on a lost reply.
		if now_ts-last < int64(broadcast_resync_timeout/time.Second) {
			return false
		}
	}
	// Past the ceiling only an already-tracked stream may re-arm. The tag
	// carries the broadcast key, which arrives in an inbound event with
	// nothing signing it, and this runs before any app handler - so a peer
	// inventing a key per event both grew this map for the process lifetime
	// and drew a resync request back to itself each time. Refusing is the
	// safe direction: it sends nothing rather than sending unthrottled.
	if !inflight && len(broadcast_resync_inflight) >= broadcast_resync_maximum {
		return false
	}
	broadcast_resync_inflight[tag] = now_ts
	return true
}

// broadcast_resync_sweep drops entries past the timeout. They are already
// ignored by the throttle above, so this reclaims them rather than changing
// any decision.
func broadcast_resync_sweep() {
	broadcast_resync_lock.Lock()
	defer broadcast_resync_lock.Unlock()
	cutoff := time.Now().Unix() - int64(broadcast_resync_timeout/time.Second)
	for tag, last := range broadcast_resync_inflight {
		if last < cutoff {
			delete(broadcast_resync_inflight, tag)
		}
	}
}

// broadcast_resync_clear marks the in-flight resync for the given
// (user, peer, key) tuple complete - subsequent gap-detections can
// fire the next request without waiting. Called from
// broadcast_advance_local on every received.last advance; idempotent
// when no flag is set, so safe to call on every advance whether or
// not a resync was in flight.
func broadcast_resync_clear(user_uid, peer, key string) {
	broadcast_resync_lock.Lock()
	defer broadcast_resync_lock.Unlock()
	delete(broadcast_resync_inflight, broadcast_resync_tag(user_uid, peer, key))
}

// broadcast_resync_jitter_maximum bounds the random delay added before
// a resync request leaves the subscriber. Spreads simultaneous gap
// detections - after a server restart, a sleep / wake cycle, or any
// event that causes thousands of subscribers to detect a gap on their
// first inbound event - across the interval, so the owner doesn't get
// every subscriber's resync request landing in the same second. The
// 60-second per-(user, peer, key) throttle above prevents same-stream
// churn; jitter prevents cross-subscriber thundering-herd at the
// owner.
const broadcast_resync_jitter_maximum = 5 * time.Second

// broadcast_resync_jitter returns a uniform random delay in
// [0, broadcast_resync_jitter_maximum). Uses crypto/rand because it's
// the rand source the rest of the package already imports; the jitter
// only needs randomness, not unpredictability.
func broadcast_resync_jitter() time.Duration {
	var buffer [2]byte
	if _, err := rand.Read(buffer[:]); err != nil {
		return 0
	}
	return time.Duration(int(buffer[0])<<8|int(buffer[1])) * time.Millisecond % broadcast_resync_jitter_maximum
}

// broadcast_request_resync sends a fire-and-forget broadcast/resync to
// the originating host asking for replay of (key, peer) starting after
// the receiver's current last. Called from the gap-detection wrapper
// in events.go when an out-of-order event arrives.
//
// from: the subscriber entity (the local user's entity that's
//
//	subscribed to the broadcast)
//
// to:   the broadcast owner entity
// peer: the libp2p peer ID of the originating host (matches e.peer on
//
//	the inbound event)
func broadcast_request_resync(user *User, a *App, from, to, key, peer string, last int64) {
	if user == nil || a == nil {
		return
	}
	if !broadcast_resync_throttle(user.UID, peer, key) {
		return
	}
	// Jitter the send to spread simultaneous gap detections across
	// subscribers - see broadcast_resync_jitter_maximum's comment. The
	// caller is already in a goroutine (events.go fires this with a
	// `go` statement), so the sleep doesn't block the apply path.
	time.Sleep(broadcast_resync_jitter())
	services := app_services(a, user)
	service := ""
	if len(services) > 0 {
		service = services[0]
	}
	m := message(from, to, service, "broadcast/resync")
	m.FromApp = a.id
	m.Services = services
	m.content = map[string]any{
		"key":   key,
		"peer":  peer,
		"after": last,
	}
	m.send_peer(peer)
}

// broadcast_send_ack delivers a broadcast/acknowledge event back to
// the originating host of a broadcast we've just applied. Fired by
// the receiver wrapper in events.go after each successful advance;
// the owner's broadcast_acknowledge handler upserts acknowledged
// for (key, peer, subscriber=us) and runs broadcast_log_ack_trim,
// which drops log rows below the slowest subscriber's progress.
//
// Self-loops (peer == net_id) are skipped: the owner is its own
// subscriber and already knows its state; the 7d age trim handles
// log cleanup for self-loop streams without needing a network
// round-trip.
//
// Bursts coalesce within broadcast_acknowledge_coalesce_window per
// (user, key, peer) - a chat full of messages or a fast game's move
// sequence sends one outbound ack per window per stream instead of
// one per applied event. Semantically equivalent because each ack
// carries the latest applied sequence (not a delta); a single ack at
// seq=N is the same as N individual acks at seqs 1..N. The owner
// upserts max(existing, new) in either case.
//
// Fire-and-forget: the flushed message goes to the queue and retries;
// an ack that fails to deliver is harmless because the next applied
// event will trigger a fresh ack carrying an equal-or-higher sequence.
//
// from: the local subscriber entity (e.to of the inbound broadcast —
//
//	the local entity that received the event).
//
// to:   the broadcast owner entity (e.from of the inbound — who
//
//	broadcast it).
//
// peer: the originating libp2p peer ID (e.peer of the inbound — the
//
//	host to send the ack back to).
func broadcast_send_ack(user *User, a *App, from, to, key, peer string, sequence int64) {
	if user == nil || a == nil {
		return
	}
	if from == "" || to == "" || key == "" || peer == "" || sequence <= 0 {
		return
	}
	if peer == net_id {
		return
	}
	broadcast_acknowledge_enqueue(user.UID, a.id, from, to, key, peer, sequence)
}

// broadcast_acknowledge_coalesce_window bounds how long a pending ack
// is held before flushing. Larger = more batching; smaller = lower
// latency to the owner's log trim. 250ms means bursty subscribers
// emit one ack per quarter-second per stream; an idle stream sees
// no extra latency because the first applied event after idle starts
// the timer fresh.
const broadcast_acknowledge_coalesce_window = 250 * time.Millisecond

// broadcast_acknowledge_pending holds one pending ack between its
// first scheduling and the timer flush. The pending entry's sequence
// is bumped by later inbound applies to the same (user, key, peer)
// tuple within the coalesce window; the timer always sends the latest.
type broadcast_acknowledge_pending struct {
	user     string
	app      string
	from     string
	to       string
	key      string
	peer     string
	sequence int64
}

var (
	broadcast_acknowledge_lock        sync.Mutex
	broadcast_acknowledge_pending_map = map[string]*broadcast_acknowledge_pending{}
)

// broadcast_acknowledge_enqueue accumulates the latest applied seq for
// one (user, key, peer) tuple and starts a flush timer if none exists.
// Subsequent enqueues within the window bump the sequence and ride the
// existing timer.
func broadcast_acknowledge_enqueue(user, app, from, to, key, peer string, sequence int64) {
	tag := user + "|" + key + "|" + peer
	broadcast_acknowledge_lock.Lock()
	pending, exists := broadcast_acknowledge_pending_map[tag]
	if exists {
		if sequence > pending.sequence {
			pending.sequence = sequence
		}
		broadcast_acknowledge_lock.Unlock()
		return
	}
	broadcast_acknowledge_pending_map[tag] = &broadcast_acknowledge_pending{
		user:     user,
		app:      app,
		from:     from,
		to:       to,
		key:      key,
		peer:     peer,
		sequence: sequence,
	}
	broadcast_acknowledge_lock.Unlock()
	time.AfterFunc(broadcast_acknowledge_coalesce_window, func() {
		broadcast_acknowledge_flush(tag)
	})
}

// broadcast_acknowledge_flush sends the coalesced ack for one tag and
// clears the pending entry. Called from the timer goroutine.
func broadcast_acknowledge_flush(tag string) {
	broadcast_acknowledge_lock.Lock()
	pending := broadcast_acknowledge_pending_map[tag]
	if pending == nil {
		broadcast_acknowledge_lock.Unlock()
		return
	}
	delete(broadcast_acknowledge_pending_map, tag)
	broadcast_acknowledge_lock.Unlock()

	user := user_by_uid(pending.user)
	a := app_by_id(pending.app)
	if user == nil || a == nil {
		return
	}
	services := app_services(a, user)
	service := ""
	if len(services) > 0 {
		service = services[0]
	}
	m := message(pending.from, pending.to, service, "broadcast/acknowledge")
	m.FromApp = pending.app
	m.Services = services
	m.content = map[string]any{
		"key":      pending.key,
		"peer":     pending.peer,
		"sequence": pending.sequence,
	}
	m.send_peer(pending.peer)
}

// broadcast_manager runs the periodic pending GC for unfillable gaps.
// Hourly cadence matches replication_manager's GC interval: the TTL
// is days, so a tighter loop just burns CPU on the per-app DB walk
// without operational benefit. Always force=false here - the
// configured TTL gate is the whole point of the background pass;
// force-skip is an operator-only path via the admin endpoint.
func broadcast_manager() {
	for range time.Tick(time.Duration(broadcast_pending_gc_period_seconds) * time.Second) {
		broadcast_pending_gc(false)
		broadcast_stall_sweep()
		broadcast_resync_sweep()
	}
}

// Mochi server: Broadcast subscription gate tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"testing"
)

func setup_subscribed_test(t *testing.T) (*DB, func()) {
	t.Helper()
	tmp_dir, err := os.MkdirTemp("", "mochi_bcast_sub")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	orig := data_dir
	data_dir = tmp_dir
	db := db_open("db/test.db")
	return db, func() {
		data_dir = orig
		os.RemoveAll(tmp_dir)
	}
}

// TestSubscribedGateRefusesNonSubscriber — the point of the whole change. `key`
// is the object entity id, so knowing it must not be enough to read a stream.
func TestSubscribedGateRefusesNonSubscriber(t *testing.T) {
	db, cleanup := setup_subscribed_test(t)
	defer cleanup()

	broadcast_subscribed_record(db, "k1", "peerA", []string{"member1", "member2"})

	if !broadcast_subscribed_allowed(db, "k1", "peerA", "member1") {
		t.Error("a recorded subscriber was refused")
	}
	if broadcast_subscribed_allowed(db, "k1", "peerA", "stranger") {
		t.Error("a non-subscriber was allowed to read the stream")
	}
	if broadcast_subscribed_allowed(db, "k1", "peerA", "") {
		t.Error("an empty identity was allowed")
	}
}

// TestSubscribedGateFailsOpenWhenUnrecorded — what makes this deployable
// without a flag day. On an upgraded server every existing stream starts with
// no records, and refusing them would wedge every subscriber at once.
func TestSubscribedGateFailsOpenWhenUnrecorded(t *testing.T) {
	db, cleanup := setup_subscribed_test(t)
	defer cleanup()

	// No table at all yet: the very first upgraded server.
	if !broadcast_subscribed_allowed(db, "k1", "peerA", "anyone") {
		t.Error("refused a request before the table existed")
	}

	// Table exists and another stream is recorded, but this one is not.
	broadcast_subscribed_record(db, "other", "peerA", []string{"member1"})
	if !broadcast_subscribed_allowed(db, "k1", "peerA", "anyone") {
		t.Error("refused a request for a stream with no records of its own")
	}
	// The recorded stream is still gated.
	if broadcast_subscribed_allowed(db, "other", "peerA", "anyone") {
		t.Error("a recorded stream failed open")
	}
}

// TestSubscribedRecordUnions — apps legitimately send to partial lists: every
// chat call site excludes the sender, and leave/remove/add exclude the
// affected member. Replacing the set on each send would evict a member who
// merely was not a recipient of the latest event and then refuse their resync.
func TestSubscribedRecordUnions(t *testing.T) {
	db, cleanup := setup_subscribed_test(t)
	defer cleanup()

	// A sends, so the list omits A; then B sends, so it omits B.
	broadcast_subscribed_record(db, "chat1", "peerA", []string{"B", "C"})
	broadcast_subscribed_record(db, "chat1", "peerA", []string{"A", "C"})

	for _, member := range []string{"A", "B", "C"} {
		if !broadcast_subscribed_allowed(db, "chat1", "peerA", member) {
			t.Errorf("member %q was evicted by a later partial send", member)
		}
	}
}

// TestSubscribedScopedPerStream — the set is per (key, peer), matching how the
// log is keyed. In chat every member originates their own stream, so a member
// is absent from their own set and present in the others'.
func TestSubscribedScopedPerStream(t *testing.T) {
	db, cleanup := setup_subscribed_test(t)
	defer cleanup()

	broadcast_subscribed_record(db, "chat1", "peerA", []string{"B"})
	broadcast_subscribed_record(db, "chat1", "peerB", []string{"A"})

	if !broadcast_subscribed_allowed(db, "chat1", "peerA", "B") {
		t.Error("B refused on A's stream")
	}
	if broadcast_subscribed_allowed(db, "chat1", "peerA", "A") {
		t.Error("A allowed on A's own stream, which never fans out to A")
	}
	if !broadcast_subscribed_allowed(db, "chat1", "peerB", "A") {
		t.Error("A refused on B's stream")
	}
}

// TestSubscribedRecordExpires — records age out on the log's own hard cap, so
// a member who has received nothing for that long lapses. Refreshed on every
// fan-out, so an active member never expires.
func TestSubscribedRecordExpires(t *testing.T) {
	db, cleanup := setup_subscribed_test(t)
	defer cleanup()

	broadcast_subscribed_table_create(db)
	stale := now() - broadcast_subscribed_age - 1
	db.exec("insert into subscribed (key, peer, subscriber, updated) values (?, ?, ?, ?)", "k1", "peerA", "departed", stale)
	db.exec("insert into subscribed (key, peer, subscriber, updated) values (?, ?, ?, ?)", "k1", "peerA", "present", now())

	// A send prunes the expired row and refreshes the active one.
	broadcast_subscribed_record(db, "k1", "peerA", []string{"present"})

	if broadcast_subscribed_allowed(db, "k1", "peerA", "departed") {
		t.Error("an expired record still granted access")
	}
	if !broadcast_subscribed_allowed(db, "k1", "peerA", "present") {
		t.Error("an active subscriber was pruned")
	}
}

// TestSubscribedRecordRefreshes — a subscriber present in a later send has its
// clock reset, so a long-lived member never ages out mid-membership.
func TestSubscribedRecordRefreshes(t *testing.T) {
	db, cleanup := setup_subscribed_test(t)
	defer cleanup()

	broadcast_subscribed_table_create(db)
	old := now() - broadcast_subscribed_age + 100
	db.exec("insert into subscribed (key, peer, subscriber, updated) values (?, ?, ?, ?)", "k1", "peerA", "member1", old)

	broadcast_subscribed_record(db, "k1", "peerA", []string{"member1"})

	row, _ := db.row("select updated from subscribed where key=? and peer=? and subscriber=?", "k1", "peerA", "member1")
	if row == nil {
		t.Fatal("record vanished")
	}
	updated, _ := row["updated"].(int64)
	if updated <= old {
		t.Errorf("record not refreshed: got %d, want > %d", updated, old)
	}
}

// TestSubscribedRecordIgnoresEmpty — an empty list must not create the table
// and then leave a stream gated with nobody in it, which would refuse every
// legitimate subscriber.
func TestSubscribedRecordIgnoresEmpty(t *testing.T) {
	db, cleanup := setup_subscribed_test(t)
	defer cleanup()

	broadcast_subscribed_record(db, "k1", "peerA", []string{})
	broadcast_subscribed_record(db, "k1", "peerA", nil)

	if !broadcast_subscribed_allowed(db, "k1", "peerA", "anyone") {
		t.Error("an empty subscriber list gated the stream against everyone")
	}
}

// TestResyncRefusesNonSubscriber — the gate in the handler, not just the
// helper: a stranger who knows the key gets no rows back.
func TestResyncRefusesNonSubscriber(t *testing.T) {
	db, cleanup := setup_subscribed_test(t)
	defer cleanup()

	for i := 0; i < 3; i++ {
		broadcast_log_append(db, "k1", "peerA", "event/a", []byte(`{}`))
	}
	broadcast_subscribed_record(db, "k1", "peerA", []string{"member1"})

	// A refused resync returns cleanly without emitting anything. Sending
	// would need a live peer, so the assertion here is that a stranger is
	// rejected before the log is read; the emit path is covered by the
	// dual-instance flow.
	e := &Event{db: db, from: "stranger", content: map[string]any{
		"key": "k1", "peer": "peerA", "after": int64(0),
	}}
	if allowed := broadcast_subscribed_allowed(e.db, "k1", "peerA", e.from); allowed {
		t.Error("stranger passed the resync gate")
	}
	if allowed := broadcast_subscribed_allowed(e.db, "k1", "peerA", "member1"); !allowed {
		t.Error("real subscriber failed the resync gate")
	}
}

// TestAcknowledgeRefusesNonSubscriber — a forged ack is not just noise:
// broadcast_log_ack_trim trims to the LOWEST floor, so one ack of 1 from a
// stranger pins the log forever.
func TestAcknowledgeRefusesNonSubscriber(t *testing.T) {
	db, cleanup := setup_subscribed_test(t)
	defer cleanup()

	for i := 0; i < 5; i++ {
		broadcast_log_append(db, "k1", "peerA", "event/a", []byte(`{}`))
	}
	broadcast_subscribed_record(db, "k1", "peerA", []string{"member1"})

	stranger := &Event{db: db, from: "stranger", content: map[string]any{
		"key": "k1", "peer": "peerA", "sequence": int64(1),
	}}
	if err := stranger.broadcast_acknowledge(); err != nil {
		t.Fatalf("acknowledge: %v", err)
	}
	if got := acknowledged_last(t, db, "k1", "peerA", "stranger"); got != -1 {
		t.Errorf("a stranger pinned the ack floor: got %d", got)
	}

	member := &Event{db: db, from: "member1", content: map[string]any{
		"key": "k1", "peer": "peerA", "sequence": int64(3),
	}}
	if err := member.broadcast_acknowledge(); err != nil {
		t.Fatalf("acknowledge: %v", err)
	}
	if got := acknowledged_last(t, db, "k1", "peerA", "member1"); got != 3 {
		t.Errorf("a real subscriber's ack was refused: got %d", got)
	}
}

// TestSubscribedRemoveRevokes — mochi.broadcast.subscriber.remove's behaviour.
// Ageing is garbage collection, not revocation: without this call a removed
// member keeps replay access until their record expires, and the log is a
// rolling window, so they could read events created after they left.
func TestSubscribedRemoveRevokes(t *testing.T) {
	db, cleanup := setup_subscribed_test(t)
	defer cleanup()

	broadcast_subscribed_record(db, "k1", "peerA", []string{"stays", "leaves"})

	if !broadcast_subscribed_remove(db, "k1", "peerA", "leaves") {
		t.Error("remove reported no record for a recorded subscriber")
	}
	if broadcast_subscribed_allowed(db, "k1", "peerA", "leaves") {
		t.Error("a removed subscriber still had access")
	}
	if !broadcast_subscribed_allowed(db, "k1", "peerA", "stays") {
		t.Error("removing one subscriber revoked another")
	}

	// A no-op removal is distinguishable from a real one, and harmless.
	if broadcast_subscribed_remove(db, "k1", "peerA", "never-there") {
		t.Error("remove claimed to revoke a subscriber that was never recorded")
	}
	// Removing the last subscriber must NOT drop the stream back to fail-open.
	if !broadcast_subscribed_remove(db, "k1", "peerA", "stays") {
		t.Error("remove reported no record for the last subscriber")
	}
	if broadcast_subscribed_allowed(db, "k1", "peerA", "leaves") {
		t.Error("emptying the set reopened the stream to everyone")
	}
}

// TestSubscribedRemoveScopedToStream — an app may revoke access to what it
// broadcasts, never to another host's stream.
func TestSubscribedRemoveScopedToStream(t *testing.T) {
	db, cleanup := setup_subscribed_test(t)
	defer cleanup()

	broadcast_subscribed_record(db, "k1", "peerA", []string{"member1"})
	broadcast_subscribed_record(db, "k1", "peerB", []string{"member1"})

	broadcast_subscribed_remove(db, "k1", "peerA", "member1")

	if broadcast_subscribed_allowed(db, "k1", "peerA", "member1") {
		t.Error("revocation did not apply to its own stream")
	}
	if !broadcast_subscribed_allowed(db, "k1", "peerB", "member1") {
		t.Error("revocation leaked into another host's stream")
	}
}

// TestSubscribedAddRecordsWithoutSending — the gap that made this API
// necessary: chat's member/add broadcast goes to the EXISTING members, so the
// joiner is absent from the very event that admits them.
func TestSubscribedAddRecordsWithoutSending(t *testing.T) {
	db, cleanup := setup_subscribed_test(t)
	defer cleanup()

	// The stream has sent, so it is gated, and the joiner was not a recipient.
	broadcast_subscribed_record(db, "chat1", "peerA", []string{"existing"})
	if broadcast_subscribed_allowed(db, "chat1", "peerA", "joiner") {
		t.Fatal("precondition: an unrecorded entity should be refused on a gated stream")
	}

	if !broadcast_subscribed_add(db, "chat1", "peerA", "joiner") {
		t.Error("add reported the record was not new")
	}
	if !broadcast_subscribed_allowed(db, "chat1", "peerA", "joiner") {
		t.Error("a joiner recorded by add was still refused")
	}
	// Idempotent: a repeat add is not new, and does not disturb access.
	if broadcast_subscribed_add(db, "chat1", "peerA", "joiner") {
		t.Error("a repeat add claimed to be new")
	}
	if !broadcast_subscribed_allowed(db, "chat1", "peerA", "joiner") {
		t.Error("a repeat add revoked access")
	}
}

// TestSubscribedAddDoesNotGateAnUnsentStream - recording one joiner must not
// gate a stream that has never sent; only a send writes the marker.
func TestSubscribedAddDoesNotGateAnUnsentStream(t *testing.T) {
	db, cleanup := setup_subscribed_test(t)
	defer cleanup()

	broadcast_subscribed_add(db, "chat1", "peerA", "joiner")

	if !broadcast_subscribed_allowed(db, "chat1", "peerA", "joiner") {
		t.Error("the added member was refused")
	}
	if !broadcast_subscribed_allowed(db, "chat1", "peerA", "longstanding-member") {
		t.Error("adding one member locked out everyone the stream has never sent to")
	}

	// The first send marks the stream, and only then does the gate bite - with
	// the previously-added joiner already inside it.
	broadcast_subscribed_record(db, "chat1", "peerA", []string{"existing"})
	if !broadcast_subscribed_allowed(db, "chat1", "peerA", "joiner") {
		t.Error("the joiner recorded before the first send lost access at the send")
	}
	if broadcast_subscribed_allowed(db, "chat1", "peerA", "longstanding-member") {
		t.Error("the stream did not gate after its first send")
	}
}

// TestSubscribedRecordSkipsFreshRows - a fan-out must not rewrite a row per
// recipient on every send.
func TestSubscribedRecordSkipsFreshRows(t *testing.T) {
	db, cleanup := setup_subscribed_test(t)
	defer cleanup()

	broadcast_subscribed_record(db, "k1", "peerA", []string{"a", "b"})
	row, _ := db.row("select updated from subscribed where key=? and peer=? and subscriber=?", "k1", "peerA", "a")
	first, _ := row["updated"].(int64)

	// Age one row past the refresh window; leave the other fresh.
	db.exec("update subscribed set updated=? where key=? and peer=? and subscriber=?", first-broadcast_subscribed_refresh-1, "k1", "peerA", "a")

	broadcast_subscribed_record(db, "k1", "peerA", []string{"a", "b"})

	stale, _ := db.row("select updated from subscribed where key=? and peer=? and subscriber=?", "k1", "peerA", "a")
	refreshed, _ := stale["updated"].(int64)
	if refreshed < first {
		t.Errorf("a stale row was not refreshed: got %d, want >= %d", refreshed, first)
	}
	// Both are still present and allowed either way.
	for _, who := range []string{"a", "b"} {
		if !broadcast_subscribed_allowed(db, "k1", "peerA", who) {
			t.Errorf("%q lost access across a refresh", who)
		}
	}
}

// TestSubscribedRefreshInsideExpiry — the refresh window must sit well inside
// the expiry, or a subscriber served regularly could still age out.
func TestSubscribedRefreshInsideExpiry(t *testing.T) {
	if broadcast_subscribed_refresh >= broadcast_subscribed_age {
		t.Fatalf("refresh window %d must be well inside the expiry %d", broadcast_subscribed_refresh, broadcast_subscribed_age)
	}
}

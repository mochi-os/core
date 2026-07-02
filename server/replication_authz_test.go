// Inbound replication authorization gates (threat-model findings A/A2,
// claude/plans/replication-threat-model.md):
//   - per-user sql/op  -> replication_op_authorized (peer in the user's host set
//     or a server-pair member); gated in replication_op_event (#90).
//   - server-global system/set + system/row -> peer_is_pair; gated in the
//     system event handlers (#89).
// The gates live at the event boundary (the authenticated e.peer); the apply
// machinery below is the trusted layer that tests drive directly.
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"io"
	"testing"
)

func authz_event(peer string, payload []byte) *Event {
	return &Event{peer: peer, stream: &Stream{reader: io.NopCloser(bytes.NewReader(payload))}}
}

// The per-user gate: a server-pair member may carry any user's ops; any other
// peer must hold an explicit hosts row for the user; unknown peers and the empty
// user are rejected.
func TestReplicationOpAuthorized(t *testing.T) {
	defer journal_test_dir(t, "u1", "testapp")()
	rdb := db_open("db/replication.db")
	if rdb == nil {
		t.Fatal("db_open replication.db")
	}
	rdb.exec("create table if not exists hosts (user text not null, peer text not null, added integer not null, primary key (user, peer))")
	rdb.exec("insert into hosts (user, peer, added) values ('u1', 'peer-host', 0)")

	pair_members_lock.Lock()
	orig := pair_members
	pair_members = map[string]bool{"peer-pair": true}
	pair_members_lock.Unlock()
	defer func() {
		pair_members_lock.Lock()
		pair_members = orig
		pair_members_lock.Unlock()
	}()

	for _, c := range []struct {
		why  string
		peer string
		user string
		want bool
	}{
		{"pair member carries any user", "peer-pair", "u1", true},
		{"pair member, other user too", "peer-pair", "u-other", true},
		{"explicit host of the user", "peer-host", "u1", true},
		{"host of u1 not authorized for u2", "peer-host", "u2", false},
		{"unknown peer rejected", "peer-attacker", "u1", false},
		{"empty user rejected for non-pair", "peer-host", "", false},
	} {
		if got := replication_op_authorized(c.peer, c.user); got != c.want {
			t.Errorf("%s: replication_op_authorized(%q, %q) = %v, want %v", c.why, c.peer, c.user, got, c.want)
		}
	}
}

// End-to-end wiring (#90): replication_op_event drops a per-user op from a peer
// outside the user's host set before it reaches the apply machinery, and admits
// it once the peer is authorized.
func TestReplicationOpEventAuthzGate(t *testing.T) {
	cleanup := setup_framework_test(t)
	defer cleanup()

	rdb := db_open("db/replication.db")
	rdb.exec("create table if not exists hosts (user text not null, peer text not null, added integer not null, primary key (user, peer))")

	// No pair members — the attacker has no relationship to fwUID at all.
	pair_members_lock.Lock()
	orig := pair_members
	pair_members = map[string]bool{}
	pair_members_lock.Unlock()
	defer func() {
		pair_members_lock.Lock()
		pair_members = orig
		pair_members_lock.Unlock()
	}()

	op := build_schedule_op(1, 0, "authztest", 100) // op.User == fwUID
	applied := func() int64 {
		row, _ := schedule_db().row("select count(*) as n from schedule where user=? and event='authztest'", fwUID)
		n, _ := row["n"].(int64)
		return n
	}

	// Unauthorized peer -> dropped at the event gate -> never applied.
	replication_op_event(authz_event("peer-attacker", cbor_encode(op)))
	if n := applied(); n != 0 {
		t.Fatalf("unauthorized op applied %d rows; gate failed to drop it", n)
	}

	// Authorize the peer for this user -> the same op now applies.
	rdb.exec("insert into hosts (user, peer, added) values (?, 'peer-attacker', 0)", fwUID)
	replication_op_event(authz_event("peer-attacker", cbor_encode(op)))
	if n := applied(); n != 1 {
		t.Fatalf("authorized op applied %d rows; want 1 (gate over-blocked)", n)
	}
}

// #156: the convergence-audit RPCs (audit/manifest, audit/hash) are
// event_anonymous but the manifest is the host's full user roster + row counts +
// write-activity tails, and the hash is a content confirmation / change-detection
// oracle. Only a pair member ever audits, so the handlers must answer a pair
// member and drop everyone else (no bytes written back).
func TestReplicationAuditHandlersRequirePair(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	set_pair := func(m map[string]bool) func() {
		pair_members_lock.Lock()
		orig := pair_members
		pair_members = m
		pair_members_lock.Unlock()
		return func() {
			pair_members_lock.Lock()
			pair_members = orig
			pair_members_lock.Unlock()
		}
	}
	hash_stream := func(reply *bytes.Buffer) *Stream {
		req := cbor_encode(&AuditHashRequest{Keys: []AuditKey{{User: "u1", Stream: "app"}}})
		return &Stream{reader: io.NopCloser(bytes.NewReader(req)), writer: filePushTestWriteCloser{reply}}
	}

	// Non-pair peer: both handlers drop without writing anything.
	restore := set_pair(map[string]bool{})
	var mReply, hReply bytes.Buffer
	replication_audit_manifest_event(&Event{peer: "attacker", stream: &Stream{writer: filePushTestWriteCloser{&mReply}}})
	if mReply.Len() != 0 {
		t.Errorf("audit-manifest answered a non-pair peer (%d bytes) — user roster leaked", mReply.Len())
	}
	replication_audit_hash_event(&Event{peer: "attacker", stream: hash_stream(&hReply)})
	if hReply.Len() != 0 {
		t.Errorf("audit-hash answered a non-pair peer (%d bytes) — content-hash oracle leaked", hReply.Len())
	}
	restore()

	// Pair member: the manifest handler responds (bytes written), proving the
	// gate doesn't over-block the legitimate audit.
	restore = set_pair(map[string]bool{"peer-pair": true})
	defer restore()
	var okReply bytes.Buffer
	replication_audit_manifest_event(&Event{peer: "peer-pair", stream: &Stream{writer: filePushTestWriteCloser{&okReply}}})
	if okReply.Len() == 0 {
		t.Error("audit-manifest gave a pair member nothing; gate over-blocks")
	}
}

// End-to-end wiring (#89): the server-global system event handlers drop ops from a
// non-pair peer and admit them from a pair member.
func TestReplicationSystemEventRequiresPair(t *testing.T) {
	cleanup := setup_framework_test(t)
	defer cleanup()
	db_open("db/settings.db").exec("create table if not exists settings (name text primary key, value text not null)")

	pair_members_lock.Lock()
	orig := pair_members
	pair_members = map[string]bool{} // attacker is not a pair member
	pair_members_lock.Unlock()
	defer func() {
		pair_members_lock.Lock()
		pair_members = orig
		pair_members_lock.Unlock()
	}()

	set := &SystemSet{Database: "settings", Table: "settings", Row: "authz_probe", Field: "value", Value: "x"}
	read := func() string {
		row, _ := db_open("db/settings.db").row("select value from settings where name='authz_probe'")
		v, _ := row["value"].(string)
		return v
	}

	// Non-pair peer -> dropped before apply.
	replication_system_set_event(authz_event("peer-attacker", cbor_encode(set)))
	if v := read(); v != "" {
		t.Fatalf("system-set from non-pair peer applied (value=%q); gate failed", v)
	}

	// Pair member -> applied.
	pair_members_lock.Lock()
	pair_members = map[string]bool{"peer-pair": true}
	pair_members_lock.Unlock()
	replication_system_set_event(authz_event("peer-pair", cbor_encode(set)))
	if v := read(); v != "x" {
		t.Fatalf("system-set from pair member not applied (value=%q); gate over-blocked", v)
	}
}

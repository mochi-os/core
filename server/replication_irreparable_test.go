// Mochi server: irreparable replication detection + marker lifecycle tests.
// Copyright Alistair Cunningham 2026

package main

import (
	"testing"

	sl "go.starlark.net/starlark"
)

// stall_pending inserts one unanchored gapped pending row (prev>0, no
// Prev==0 start) for a stream, received `ageDays` ago, so the stream shows
// up as stalled in replication_pending_stalled with that age.
func stall_pending(db *DB, peer, user, database string, ageDays int64) {
	db.exec(
		"insert into pending (peer, scope, user, db, sequence, prev, schema, payload, received) values (?, 'app', ?, ?, 7, 6, 1, ?, ?)",
		peer, user, database, []byte{0x00}, now()-ageDays*86400)
}

// TestIrreparableMarksPastForget: a stream stalled past T_forget gets an
// irreparable row; one stalled only a few days does not.
func TestIrreparableMarksPastForget(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db := db_open("db/replication.db")

	stall_pending(db, "peer-old", "u-old", "dbA", irreparable_threshold/86400+1) // > 30d
	stall_pending(db, "peer-young", "u-young", "dbB", 3)                          // < 30d

	replication_irreparable_scan()

	if marked, _ := db.exists("select 1 from irreparable where peer='peer-old'"); !marked {
		t.Error("a stream stalled past T_forget should be marked irreparable")
	}
	if marked, _ := db.exists("select 1 from irreparable where peer='peer-young'"); marked {
		t.Error("a stream stalled only 3 days must not be marked irreparable")
	}
}

// TestIrreparableMarkSurvivesPendingPurge: once marked, the row must NOT be
// cleared just because the pending evidence is gone (the GC runs right after
// the scan). Past T_forget the only recovery is an operator re-bootstrap.
func TestIrreparableMarkSurvivesPendingPurge(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db := db_open("db/replication.db")

	stall_pending(db, "peer-gone", "u-gone", "dbA", irreparable_threshold/86400+1)
	replication_irreparable_scan()
	if marked, _ := db.exists("select 1 from irreparable where peer='peer-gone'"); !marked {
		t.Fatal("stream should be marked irreparable")
	}

	// Simulate the pending GC purging the aged rows, then re-scan.
	db.exec("delete from pending where peer='peer-gone'")
	replication_irreparable_scan()

	if marked, _ := db.exists("select 1 from irreparable where peer='peer-gone'"); !marked {
		t.Error("irreparable mark must survive the pending purge — recovery is operator-only past T_forget")
	}
}

// TestIrreparableNotifiedFlipsOnce: the first scan marks notified=0, and a
// scan where this host wins the (sole-member) leader claim flips it to 1 so
// the notification fires exactly once.
func TestIrreparableNotifiedFlipsOnce(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db := db_open("db/replication.db")

	stall_pending(db, "peer-n", "u-n", "dbA", irreparable_threshold/86400+1)
	replication_irreparable_scan()

	// No co-members configured -> the optimistic leader claim succeeds and
	// notified flips to 1 in the same scan.
	if n := db.integer("select notified from irreparable where peer='peer-n'"); n != 1 {
		t.Errorf("notified = %d, want 1 (sole survivor notifies in-scan)", n)
	}
}

// TestIrreparableClearedOnBootstrapDone: a completed re-bootstrap for the
// (scope, peer) clears the terminal marker.
func TestIrreparableClearedOnBootstrapDone(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db := db_open("db/replication.db")

	stall_pending(db, "peer-b", "u-b", "dbA", irreparable_threshold/86400+1)
	replication_irreparable_scan()
	if marked, _ := db.exists("select 1 from irreparable where peer='peer-b'"); !marked {
		t.Fatal("stream should be marked irreparable")
	}

	bootstrap_set_state(repl_scope_app, "peer-b", bootstrap_state_done, "")

	if marked, _ := db.exists("select 1 from irreparable where peer='peer-b' and scope=?", repl_scope_app); marked {
		t.Error("a completed re-bootstrap must clear the irreparable marker")
	}
}

// TestIrreparableOfflineMember: a peer unreachable past T_forget that is a
// configured pair member and hosts a user gets one offline-irreparable row
// per relationship; a peer unreachable for only a few days does not.
func TestIrreparableOfflineMember(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db := db_open("db/replication.db")

	// peer-dead: paired + hosts user u1, unreachable 31 days.
	db.exec("insert into unreachable (peer, since) values ('peer-dead', ?)", now()-(irreparable_threshold+86400))
	db.exec("insert into pair (peer, added, role) values ('peer-dead', ?, '')", now())
	db.exec("insert into hosts (user, peer, added, ack) values ('u1', 'peer-dead', ?, 0)", now())
	// peer-flaky: paired, unreachable only 3 days -> not yet irreparable.
	db.exec("insert into unreachable (peer, since) values ('peer-flaky', ?)", now()-3*86400)
	db.exec("insert into pair (peer, added, role) values ('peer-flaky', ?, '')", now())

	replication_irreparable_scan()

	if c := db.integer("select count(*) from irreparable where peer='peer-dead' and reason='offline'"); c != 2 {
		t.Errorf("peer-dead offline rows = %d, want 2 (pair + one host)", c)
	}
	if ok, _ := db.exists("select 1 from irreparable where peer='peer-dead' and scope=? and user=''", repl_scope_core); !ok {
		t.Error("missing whole-server (core) offline-irreparable row for peer-dead")
	}
	if ok, _ := db.exists("select 1 from irreparable where peer='peer-dead' and scope=? and user='u1'", repl_scope_app); !ok {
		t.Error("missing per-user (app) offline-irreparable row for peer-dead/u1")
	}
	if c := db.integer("select count(*) from irreparable where peer='peer-flaky'"); c != 0 {
		t.Errorf("peer-flaky (3d) must not be irreparable, got %d rows", c)
	}
}

// TestPeerUnreachablePersistAndClear: crossing the stall threshold stamps a
// unreachable row once (preserving the original timestamp), and an ack
// clears it.
func TestPeerUnreachablePersistAndClear(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db := db_open("db/replication.db")
	peer_progress = map[string]PeerProgress{}

	peer := "peer-unreach"
	db.exec("insert into pair (peer, added, role) values (?, 0, '')", peer) // a replication member
	for i := 0; i < peer_stall_threshold; i++ {
		peer_mark_no_progress(peer)
	}
	since := db.integer("select since from unreachable where peer=?", peer)
	if since == 0 {
		t.Fatal("crossing the stall threshold must stamp unreachable")
	}
	// More timeouts must not move the original `since`.
	peer_mark_no_progress(peer)
	if s := db.integer("select since from unreachable where peer=?", peer); s != since {
		t.Errorf("unreachable.since changed on later timeout: %d -> %d", since, s)
	}
	// An ack clears it.
	peer_mark_progress(peer)
	if ok, _ := db.exists("select 1 from unreachable where peer=?", peer); ok {
		t.Error("an ack must clear unreachable")
	}
}

// TestPeerUnreachableConnectFailureStamps: the connect-failure path
// (peer_mark_send_failed, a powered-off / partitioned member) also stamps
// unreachable for a replication member, and a non-member does not.
func TestPeerUnreachableConnectFailureStamps(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db := db_open("db/replication.db")
	peer_reachability = map[string]PeerReachability{}

	member := "peer-poweredoff"
	db.exec("insert into hosts (user, peer, added, ack) values ('u1', ?, 0, 0)", member)
	stranger := "peer-stranger" // not a replication member

	for i := 0; i < peer_silent_failure_threshold; i++ {
		peer_mark_send_failed(member)
		peer_mark_send_failed(stranger)
	}

	if ok, _ := db.exists("select 1 from unreachable where peer=?", member); !ok {
		t.Error("a replication member we can't connect to must stamp unreachable")
	}
	if ok, _ := db.exists("select 1 from unreachable where peer=?", stranger); ok {
		t.Error("a non-member must not stamp unreachable (table stays scoped)")
	}
}

// TestIrreparableClearedOnRemove: removing a relationship clears its badge.
func TestIrreparableClearedOnRemove(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db := db_open("db/replication.db")

	db.exec("insert into irreparable (peer, scope, user, db, reason, since, notified) values ('peer-x', ?, '', '', 'offline', ?, 1)", repl_scope_core, now())
	replication_irreparable_clear("peer-x", repl_scope_core)
	if ok, _ := db.exists("select 1 from irreparable where peer='peer-x'"); ok {
		t.Error("replication_irreparable_clear must remove the marker")
	}
}

// TestIrreparableLastCopy: last_copy is true only when no OTHER healthy copy
// of the data survives (drives the no-redundancy urgent notification).
func TestIrreparableLastCopy(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db := db_open("db/replication.db")
	db.exec("insert into hosts (user, peer, added, ack) values ('u1', 'peerA', 0, 0)")
	db.exec("insert into hosts (user, peer, added, ack) values ('u1', 'peerB', 0, 0)")
	db.exec("insert into irreparable (peer, scope, user, db, reason, since, notified) values ('peerA', ?, 'u1', '', 'offline', 0, 1)", repl_scope_app)

	// peerB is still a healthy copy -> losing peerA is not the last copy.
	if replication_irreparable_last_copy(db, repl_scope_app, "u1", "peerA") {
		t.Error("with a healthy peerB, peerA must not be the last copy")
	}
	// peerB also dies -> peerA is now the last copy.
	db.exec("insert into irreparable (peer, scope, user, db, reason, since, notified) values ('peerB', ?, 'u1', '', 'offline', 0, 1)", repl_scope_app)
	if !replication_irreparable_last_copy(db, repl_scope_app, "u1", "peerA") {
		t.Error("with peerB also irreparable, peerA is the last copy (urgent)")
	}
}

// TestIrreparableEmitSkip: ops are withheld from a peer marked irreparable for
// the whole server (core) or for that user (app), and not from others.
func TestIrreparableEmitSkip(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db := db_open("db/replication.db")
	db.exec("insert into irreparable (peer, scope, user, db, reason, since, notified) values ('peerApp', ?, 'u1', '', 'offline', 0, 1)", repl_scope_app)
	db.exec("insert into irreparable (peer, scope, user, db, reason, since, notified) values ('peerCore', ?, '', '', 'offline', 0, 1)", repl_scope_core)
	irreparable_snapshot_at = 0 // force the cache to refresh

	if !irreparable_emit_skip("u1", "peerApp") {
		t.Error("must skip a peer irreparable for this user")
	}
	if irreparable_emit_skip("u2", "peerApp") {
		t.Error("must NOT skip for a different user (app-scope marker is per-user)")
	}
	if !irreparable_emit_skip("anyuser", "peerCore") {
		t.Error("must skip a core-scope-irreparable peer for any user")
	}
	if irreparable_emit_skip("u1", "peerHealthy") {
		t.Error("must NOT skip a healthy peer")
	}
}

// TestWipedRebootstrapTriggers: an UNANCHORED stalled stream aged past the
// threshold triggers a re-bootstrap; an anchored one and a fresh one do not.
func TestWipedRebootstrapTriggers(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db := db_open("db/replication.db")
	old := now() - int64(rebootstrap_unanchored_seconds) - 60

	// Wiped stream: unanchored (no cursor), prev>0, aged.
	db.exec("insert into pending (peer, scope, user, db, sequence, prev, schema, payload, received) values ('peer-wiped', 'app', 'u1', 'dbA', 7, 6, 1, ?, ?)", []byte{0x00}, old)
	// Anchored gap (has cursor) — divergence risk, must NOT auto-rebootstrap.
	db.exec("insert into cursor (peer, scope, user, db, sequence) values ('peer-anchored', 'app', 'u2', 'dbB', 10)")
	db.exec("insert into pending (peer, scope, user, db, sequence, prev, schema, payload, received) values ('peer-anchored', 'app', 'u2', 'dbB', 16, 15, 1, ?, ?)", []byte{0x00}, old)
	// Young unanchored — still settling, must NOT trigger yet.
	db.exec("insert into pending (peer, scope, user, db, sequence, prev, schema, payload, received) values ('peer-young', 'app', 'u3', 'dbC', 7, 6, 1, ?, ?)", []byte{0x00}, now())

	replication_wiped_rebootstrap()

	if st, _ := bootstrap_get_state(bootstrap_scope_userdbs, "peer-wiped"); st != bootstrap_state_queued {
		t.Errorf("wiped stream should trigger re-bootstrap; bootstrap state = %q, want queued", st)
	}
	if st, _ := bootstrap_get_state(bootstrap_scope_userdbs, "peer-anchored"); st != "" {
		t.Errorf("anchored gap must NOT auto-rebootstrap (divergence risk); state = %q", st)
	}
	if st, _ := bootstrap_get_state(bootstrap_scope_userdbs, "peer-young"); st != "" {
		t.Errorf("a still-settling unanchored stream must NOT trigger yet; state = %q", st)
	}
}

// TestWipedRebootstrapDisabled: the setting gate turns it off.
func TestWipedRebootstrapDisabled(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db_open("db/settings.db").exec("create table if not exists settings (name text primary key, value text not null)")
	setting_set("replication.rebootstrap.wiped", "false")
	db := db_open("db/replication.db")
	old := now() - int64(rebootstrap_unanchored_seconds) - 60
	db.exec("insert into pending (peer, scope, user, db, sequence, prev, schema, payload, received) values ('peer-off', 'app', 'u1', 'dbA', 7, 6, 1, ?, ?)", []byte{0x00}, old)

	replication_wiped_rebootstrap()
	if st, _ := bootstrap_get_state(bootstrap_scope_userdbs, "peer-off"); st != "" {
		t.Errorf("disabled gate must not trigger; state = %q", st)
	}
}

// TestWipedRebootstrapSystemRowEscalates: a system-row stream (system:sessions)
// can't be re-seeded by the per-user file pull, so the loop must NOT pull it —
// it escalates straight to irreparable for an operator re-join.
func TestWipedRebootstrapSystemRowEscalates(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	rebootstrap_attempts = map[string]rebootstrap_state{}
	db := db_open("db/replication.db")
	old := now() - int64(rebootstrap_unanchored_seconds) - 60
	sys := repl_stream_key(repl_stream_class_system, "sessions")
	db.exec("insert into pending (peer, scope, user, db, sequence, prev, schema, payload, received) values ('peer-sys', 'app', 'u1', ?, 7, 6, 1, ?, ?)",
		sys, []byte{0x00}, old)

	replication_wiped_rebootstrap()

	if st, _ := bootstrap_get_state(bootstrap_scope_userdbs, "peer-sys"); st != "" {
		t.Errorf("system-row stream must NOT trigger a file pull; state = %q", st)
	}
	if n := db.integer("select count(*) from irreparable where peer='peer-sys' and db=?", sys); n != 1 {
		t.Errorf("system-row stream must escalate to irreparable; markers = %d, want 1", n)
	}
}

// TestWipedRebootstrapCapEscalates: after rebootstrap_attempt_cap futile pulls
// the loop gives up — escalates to irreparable and stops re-pulling.
func TestWipedRebootstrapCapEscalates(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	rebootstrap_attempts = map[string]rebootstrap_state{"peer-cap|u1": {attempts: rebootstrap_attempt_cap}}
	db := db_open("db/replication.db")
	old := now() - int64(rebootstrap_unanchored_seconds) - 60
	db.exec("insert into pending (peer, scope, user, db, sequence, prev, schema, payload, received) values ('peer-cap', 'app', 'u1', 'app:feeds', 7, 6, 1, ?, ?)", []byte{0x00}, old)

	replication_wiped_rebootstrap()

	if st, _ := bootstrap_get_state(bootstrap_scope_userdbs, "peer-cap"); st != "" {
		t.Errorf("capped stream must NOT pull again; state = %q", st)
	}
	if n := db.integer("select count(*) from irreparable where peer='peer-cap' and db='app:feeds'"); n != 1 {
		t.Errorf("capped stream must escalate to irreparable; markers = %d, want 1", n)
	}
	if !rebootstrap_attempts["peer-cap|u1"].gaveup {
		t.Error("capped stream must be flagged gaveup so it stops retrying")
	}
}

// TestWipedRebootstrapBacksOff: a second tick immediately after a pull must not
// re-fire — exponential backoff keeps the attempt count at 1.
func TestWipedRebootstrapBacksOff(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	rebootstrap_attempts = map[string]rebootstrap_state{}
	db := db_open("db/replication.db")
	old := now() - int64(rebootstrap_unanchored_seconds) - 60
	db.exec("insert into pending (peer, scope, user, db, sequence, prev, schema, payload, received) values ('peer-bo', 'app', 'u1', 'app:feeds', 7, 6, 1, ?, ?)", []byte{0x00}, old)

	replication_wiped_rebootstrap() // attempt 1
	replication_wiped_rebootstrap() // immediate retry — gated by backoff

	if got := rebootstrap_attempts["peer-bo|u1"].attempts; got != 1 {
		t.Errorf("backoff must prevent a second immediate attempt; attempts = %d, want 1", got)
	}
}

// TestIrreparableCount: the health-endpoint count reflects the marker rows.
func TestIrreparableCount(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db := db_open("db/replication.db")
	if replication_irreparable_count() != 0 {
		t.Error("fresh count should be 0")
	}
	db.exec("insert into irreparable (peer, scope, user, db, reason, since, notified) values ('p1', ?, '', '', 'offline', 0, 1)", repl_scope_core)
	db.exec("insert into irreparable (peer, scope, user, db, reason, since, notified) values ('p2', ?, 'u1', '', 'offline', 0, 1)", repl_scope_app)
	if c := replication_irreparable_count(); c != 2 {
		t.Errorf("count = %d, want 2", c)
	}
}

// TestApiExposesOffline: status() lists offline pair members with their
// since, and hosts() carries a per-host offline timestamp (drives the badge).
func TestApiExposesOffline(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db := db_open("db/replication.db")
	db.exec("insert into pair (peer, added, role) values ('peer-off', 0, '')")
	db.exec("insert into unreachable (peer, since, notified) values ('peer-off', 12345, 0)")
	db.exec("insert into hosts (user, peer, added, ack) values ('u-a', 'host-off', 0, 0)")
	db.exec("insert into unreachable (peer, since, notified) values ('host-off', 54321, 0)")

	v, err := api_replication_status(&sl.Thread{}, nil, sl.Tuple{}, nil)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	off, _, _ := v.(*sl.Dict).Get(sl.String("offline"))
	list, ok := off.(*sl.List)
	if !ok || list.Len() != 1 {
		t.Fatalf("status offline not a 1-element list: %v", off)
	}
	d := list.Index(0).(*sl.Dict)
	peer, _, _ := d.Get(sl.String("peer"))
	since, _, _ := d.Get(sl.String("since"))
	if string(peer.(sl.String)) != "peer-off" {
		t.Errorf("offline peer = %v, want peer-off", peer)
	}
	if s, _ := since.(sl.Int).Int64(); s != 12345 {
		t.Errorf("offline since = %d, want 12345", s)
	}

	with_user_thread(&User{UID: "u-a"}, func(th *sl.Thread) {
		hv, _ := api_replication_hosts(th, nil, sl.Tuple{}, nil)
		hd := hv.(*sl.List).Index(0).(*sl.Dict)
		ho, _, _ := hd.Get(sl.String("offline"))
		if s, _ := ho.(sl.Int).Int64(); s != 54321 {
			t.Errorf("host offline = %d, want 54321", s)
		}
	})
}

// TestPeerDisconnectStampsUnreachable: a member dropping at the libp2p level
// (peer_disconnected) stamps unreachable even with no outbound traffic - the
// idle-member gap a stopped mochi2 exposed; a reconnect clears it; a
// non-member disconnect is ignored.
func TestPeerDisconnectStampsUnreachable(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db := db_open("db/replication.db")
	member := "peer-dropped"
	db.exec("insert into pair (peer, added, role) values (?, 0, '')", member)

	peer_disconnected(member)
	peer_disconnected("peer-stranger-disc") // not a member

	if ok, _ := db.exists("select 1 from unreachable where peer=?", member); !ok {
		t.Error("a member disconnecting must stamp unreachable (the idle-offline case)")
	}
	if ok, _ := db.exists("select 1 from unreachable where peer='peer-stranger-disc'"); ok {
		t.Error("a non-member disconnect must not stamp unreachable")
	}
	peer_reconnected(member)
	if ok, _ := db.exists("select 1 from unreachable where peer=?", member); ok {
		t.Error("a reconnect must clear the offline mark")
	}
}

// TestOfflineNotifyScan: a member unreachable past offline_threshold gets the
// soft offline notification once (notified flips to 1); a member offline only
// a few hours does not.
func TestOfflineNotifyScan(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db := db_open("db/replication.db")
	db.exec("insert into unreachable (peer, since, notified) values ('peer-day', ?, 0)", now()-offline_threshold-3600)
	db.exec("insert into pair (peer, added, role) values ('peer-day', 0, '')")
	db.exec("insert into unreachable (peer, since, notified) values ('peer-hour', ?, 0)", now()-3600)
	db.exec("insert into pair (peer, added, role) values ('peer-hour', 0, '')")

	replication_offline_scan()

	if n := db.integer("select notified from unreachable where peer='peer-day'"); n != 1 {
		t.Errorf("member offline past threshold should be notified once; notified=%d", n)
	}
	if n := db.integer("select notified from unreachable where peer='peer-hour'"); n != 0 {
		t.Errorf("member offline only an hour must not notify yet; notified=%d", n)
	}
}

// TestApiStatusExposesIrreparable: a core-scope marker surfaces in
// mochi.replication.status()'s irreparable list (drives the Pair page badge).
func TestApiStatusExposesIrreparable(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db := db_open("db/replication.db")
	db.exec("insert into pair (peer, added, role) values ('peer-broken', 0, '')")
	db.exec("insert into irreparable (peer, scope, user, db, reason, since, notified) values ('peer-broken', ?, '', '', 'offline', ?, 1)", repl_scope_core, now())

	v, err := api_replication_status(&sl.Thread{}, nil, sl.Tuple{}, nil)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	val, _, _ := v.(*sl.Dict).Get(sl.String("irreparable"))
	list, ok := val.(*sl.List)
	if !ok {
		t.Fatalf("irreparable is not a list: %T", val)
	}
	if list.Len() != 1 {
		t.Fatalf("irreparable len = %d, want 1", list.Len())
	}
	if s, _ := list.Index(0).(sl.String); string(s) != "peer-broken" {
		t.Errorf("irreparable[0] = %v, want peer-broken", list.Index(0))
	}
}

// TestApiHostsExposesIrreparable: an app-scope marker surfaces as the
// per-host irreparable=True flag in mochi.replication.hosts() (My-hosts badge).
func TestApiHostsExposesIrreparable(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db := db_open("db/replication.db")
	db.exec("insert into hosts (user, peer, added, ack) values ('u-a', 'peer-ok', 100, 0)")
	db.exec("insert into hosts (user, peer, added, ack) values ('u-a', 'peer-bad', 200, 0)")
	db.exec("insert into irreparable (peer, scope, user, db, reason, since, notified) values ('peer-bad', ?, 'u-a', '', 'offline', ?, 1)", repl_scope_app, now())

	with_user_thread(&User{UID: "u-a"}, func(th *sl.Thread) {
		v, err := api_replication_hosts(th, nil, sl.Tuple{}, nil)
		if err != nil {
			t.Fatalf("hosts: %v", err)
		}
		list := v.(*sl.List)
		seen := map[string]bool{}
		for i := 0; i < list.Len(); i++ {
			d := list.Index(i).(*sl.Dict)
			peer, _, _ := d.Get(sl.String("peer"))
			broken, _, _ := d.Get(sl.String("irreparable"))
			seen[string(peer.(sl.String))] = bool(broken.(sl.Bool))
		}
		if seen["peer-bad"] != true {
			t.Error("peer-bad host should be flagged irreparable")
		}
		if seen["peer-ok"] != false {
			t.Error("peer-ok host should not be flagged irreparable")
		}
	})
}

// TestAdminIrreparableEndpoint: the admin endpoint runs the scan and lists
// every reason. Drives a stalled stream, an offline pair member, and an
// offline host through one call and asserts the JSON.
func TestAdminIrreparableEndpoint(t *testing.T) {
	cleanup := setup_admin_replication_test(t)
	defer cleanup()
	db := db_open("db/replication.db")

	old := now() - (irreparable_threshold + 86400)
	// Stalled stream (unfillable gap), aged past T_forget.
	db.exec("insert into pending (peer, scope, user, db, sequence, prev, schema, payload, received) values ('peer-stall', 'app', 'u1', 'dbA', 7, 6, 1, ?, ?)", []byte{0x00}, old)
	// Offline member: paired + hosts u2, unreachable past T_forget.
	db.exec("insert into unreachable (peer, since) values ('peer-off', ?)", old)
	db.exec("insert into pair (peer, added, role) values ('peer-off', 0, '')")
	db.exec("insert into hosts (user, peer, added, ack) values ('u2', 'peer-off', 0, 0)")

	_, resp := admin_replication_call(t, "GET", "/_/admin/replication/irreparable", nil, admin_replication_irreparable)
	rows, ok := resp["irreparable"].([]any)
	if !ok {
		t.Fatalf("irreparable not a list: %T", resp["irreparable"])
	}

	var stalled, offlinePair, offlineHost bool
	for _, r := range rows {
		m := r.(map[string]any)
		peer, _ := m["peer"].(string)
		reason, _ := m["reason"].(string)
		scope, _ := m["scope"].(string)
		switch {
		case peer == "peer-stall" && reason == "stalled":
			stalled = true
		case peer == "peer-off" && reason == "offline" && scope == repl_scope_core:
			offlinePair = true
		case peer == "peer-off" && reason == "offline" && scope == repl_scope_app:
			offlineHost = true
		}
	}
	if !stalled {
		t.Error("missing stalled-stream irreparable entry")
	}
	if !offlinePair {
		t.Error("missing offline pair-member irreparable entry")
	}
	if !offlineHost {
		t.Error("missing offline host irreparable entry")
	}
}

// TestIrreparableEventNotifiesWithoutPersisting: the inbound
// replica/irreparable handler notifies but must NOT persist a marker - a
// mirrored marker would have no local recovery signal to clear it and would
// orphan once the relationship recovers (the bug the live two-instance run
// caught). This side's own badge is driven by its own detection.
func TestIrreparableEventNotifiesWithoutPersisting(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db := db_open("db/replication.db")

	e := &Event{peer: "peer-remote", content: map[string]any{
		"scope": repl_scope_core, "user": "", "db": "", "reason": "offline",
	}}
	replication_irreparable_event(e) // must not panic; notify-only

	if ok, _ := db.exists("select 1 from irreparable where peer='peer-remote'"); ok {
		t.Error("remote irreparable event must not persist a marker (it has no local clear signal)")
	}
}

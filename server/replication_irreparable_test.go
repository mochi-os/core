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
	db.exec("insert into peer_unreachable (peer, since) values ('peer-dead', ?)", now()-(irreparable_threshold+86400))
	db.exec("insert into pair (peer, added, role) values ('peer-dead', ?, '')", now())
	db.exec("insert into hosts (user, peer, added, ack) values ('u1', 'peer-dead', ?, 0)", now())
	// peer-flaky: paired, unreachable only 3 days -> not yet irreparable.
	db.exec("insert into peer_unreachable (peer, since) values ('peer-flaky', ?)", now()-3*86400)
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
// peer_unreachable row once (preserving the original timestamp), and an ack
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
	since := db.integer("select since from peer_unreachable where peer=?", peer)
	if since == 0 {
		t.Fatal("crossing the stall threshold must stamp peer_unreachable")
	}
	// More timeouts must not move the original `since`.
	peer_mark_no_progress(peer)
	if s := db.integer("select since from peer_unreachable where peer=?", peer); s != since {
		t.Errorf("peer_unreachable.since changed on later timeout: %d -> %d", since, s)
	}
	// An ack clears it.
	peer_mark_progress(peer)
	if ok, _ := db.exists("select 1 from peer_unreachable where peer=?", peer); ok {
		t.Error("an ack must clear peer_unreachable")
	}
}

// TestPeerUnreachableConnectFailureStamps: the connect-failure path
// (peer_mark_send_failed, a powered-off / partitioned member) also stamps
// peer_unreachable for a replication member, and a non-member does not.
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

	if ok, _ := db.exists("select 1 from peer_unreachable where peer=?", member); !ok {
		t.Error("a replication member we can't connect to must stamp peer_unreachable")
	}
	if ok, _ := db.exists("select 1 from peer_unreachable where peer=?", stranger); ok {
		t.Error("a non-member must not stamp peer_unreachable (table stays scoped)")
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
	db.exec("insert into peer_unreachable (peer, since) values ('peer-off', ?)", old)
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

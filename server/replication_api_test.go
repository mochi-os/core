// Mochi server: mochi.replication.* API unit tests
// Copyright Alistair Cunningham 2026

package main

import (
	"testing"

	sl "go.starlark.net/starlark"
)

// TestApiReplicationStatusEmpty: with no pair / no hosts / no pending
// requests, status returns zeros for the counts and the local peer-id.
func TestApiReplicationStatusEmpty(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	thread := &sl.Thread{}
	v, err := api_replication_status(thread, nil, sl.Tuple{}, nil)
	if err != nil {
		t.Fatalf("api_replication_status error: %v", err)
	}
	d, ok := v.(*sl.Dict)
	if !ok {
		t.Fatalf("result is not a dict: %T", v)
	}

	peer, _, _ := d.Get(sl.String("peer"))
	if s, _ := peer.(sl.String); string(s) != "self" {
		t.Errorf("peer = %v, want self", peer)
	}

	for _, key := range []string{"hosts_count", "links_pending", "joins_pending"} {
		v, _, _ := d.Get(sl.String(key))
		n, ok := v.(sl.Int)
		if !ok {
			t.Errorf("%s is not an Int: %T", key, v)
			continue
		}
		count, _ := n.Int64()
		if count != 0 {
			t.Errorf("%s = %d, want 0", key, count)
		}
	}
}

// TestApiReplicationStatusPopulated: rows in each table reflect in the
// returned dict.
func TestApiReplicationStatusPopulated(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	rdb := db_open("db/replication.db")
	rdb.exec("insert into pair (peer, added, role) values ('peer-A', 0, '')")
	rdb.exec("insert into pair (peer, added, role) values ('peer-B', 0, '')")
	rdb.exec("insert into hosts (user, peer, added, ack) values ('u1', 'peer-X', 0, 0)")
	rdb.exec("insert into hosts (user, peer, added, ack) values ('u2', 'peer-Y', 0, 0)")
	rdb.exec("insert into links (user, peer, label, placeholder, received, expires) values ('u1', 'peer-K', '', 'ph-1', 0, 9999999999)")
	rdb.exec("insert into joins (peer, label, received, expires) values ('peer-J', '', 0, 9999999999)")
	rdb.exec("insert into joins (peer, label, received, expires) values ('peer-K', '', 0, 9999999999)")

	thread := &sl.Thread{}
	v, err := api_replication_status(thread, nil, sl.Tuple{}, nil)
	if err != nil {
		t.Fatalf("api_replication_status error: %v", err)
	}
	d := v.(*sl.Dict)

	pairValue, _, _ := d.Get(sl.String("pair"))
	pairList, ok := pairValue.(*sl.List)
	if !ok {
		t.Fatalf("pair is not a list: %T", pairValue)
	}
	if pairList.Len() != 2 {
		t.Errorf("pair list len = %d, want 2", pairList.Len())
	}

	want := map[string]int64{
		"hosts_count":   2,
		"links_pending": 1,
		"joins_pending": 2,
	}
	for k, expected := range want {
		v, _, _ := d.Get(sl.String(k))
		n, _ := v.(sl.Int).Int64()
		if n != expected {
			t.Errorf("%s = %d, want %d", k, n, expected)
		}
	}
}

// withUserThread runs fn with t.Local("user") set to u.
func withUserThread(u *User, fn func(*sl.Thread)) {
	th := &sl.Thread{}
	th.SetLocal("user", u)
	fn(th)
}

// TestApiReplicationBootstrapProgress: returns per-(peer, scope) rows;
// peer-filtered arg narrows to one peer.
func TestApiReplicationBootstrapProgress(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	bootstrap_set_state("files", "peer-A", "done", "")
	bootstrap_set_state("apps", "peer-A", "active", "12")
	bootstrap_set_state("files", "peer-B", "queued", "")

	th := &sl.Thread{}

	// No filter — every row.
	v, err := api_replication_bootstrap_progress(th, nil, sl.Tuple{}, nil)
	if err != nil {
		t.Fatalf("progress: %v", err)
	}
	all := v.(*sl.List)
	if all.Len() != 3 {
		t.Errorf("all rows = %d, want 3", all.Len())
	}

	// Filtered to peer-A.
	v, err = api_replication_bootstrap_progress(th, sl.NewBuiltin("bootstrap_progress", api_replication_bootstrap_progress), sl.Tuple{sl.String("peer-A")}, nil)
	if err != nil {
		t.Fatalf("progress filtered: %v", err)
	}
	filtered := v.(*sl.List)
	if filtered.Len() != 2 {
		t.Errorf("peer-A rows = %d, want 2", filtered.Len())
	}

	// Check entry shape — iterate the first row and look up keys.
	it := filtered.Iterate()
	defer it.Done()
	var first sl.Value
	if !it.Next(&first) {
		t.Fatalf("filtered list is empty")
	}
	d := first.(*sl.Dict)
	for _, key := range []string{"peer", "scope", "state", "position"} {
		v, ok, _ := d.Get(sl.String(key))
		if !ok {
			t.Errorf("entry missing key %q", key)
			continue
		}
		if _, ok := v.(sl.String); !ok {
			t.Errorf("entry %q value is not a string: %T", key, v)
		}
	}
}

// TestApiReplicationLinksAndHosts: per-user link/host queries scope to
// the calling user. Inserts rows for two users and asserts the API
// returns only the calling user's rows.
func TestApiReplicationLinksAndHosts(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	rdb := db_open("db/replication.db")
	rdb.exec("insert into links (user, peer, label, placeholder, received, expires) values ('u-alice', 'peer-A', 'a.example', 'ph-1', 0, 9999999999)")
	rdb.exec("insert into links (user, peer, label, placeholder, received, expires) values ('u-alice', 'peer-B', 'b.example', 'ph-2', 0, 9999999999)")
	rdb.exec("insert into links (user, peer, label, placeholder, received, expires) values ('u-bob', 'peer-Z', 'z.example', 'ph-9', 0, 9999999999)")
	rdb.exec("insert into hosts (user, peer, added, ack) values ('u-alice', 'peer-A', 100, 0)")
	rdb.exec("insert into hosts (user, peer, added, ack) values ('u-bob', 'peer-Z', 200, 1)")

	alice := &User{UID: "u-alice"}
	withUserThread(alice, func(th *sl.Thread) {
		v, err := api_replication_links(th, nil, sl.Tuple{}, nil)
		if err != nil {
			t.Fatalf("links: %v", err)
		}
		links := v.(*sl.List)
		if links.Len() != 2 {
			t.Errorf("links len = %d, want 2 (alice has 2 pending)", links.Len())
		}

		v, err = api_replication_hosts(th, nil, sl.Tuple{}, nil)
		if err != nil {
			t.Fatalf("hosts: %v", err)
		}
		hosts := v.(*sl.List)
		if hosts.Len() != 1 {
			t.Errorf("hosts len = %d, want 1 (alice has 1 host)", hosts.Len())
		}
	})

	// No user — both APIs should error.
	th := &sl.Thread{}
	if _, err := api_replication_links(th, sl.NewBuiltin("links", api_replication_links), sl.Tuple{}, nil); err == nil {
		t.Error("links: expected error for no user")
	}
	if _, err := api_replication_hosts(th, sl.NewBuiltin("hosts", api_replication_hosts), sl.Tuple{}, nil); err == nil {
		t.Error("hosts: expected error for no user")
	}
}

// TestApiReplicationLinkDeny: deny removes the link row for the calling
// user but leaves rows for other users untouched.
func TestApiReplicationLinkDeny(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	rdb := db_open("db/replication.db")
	rdb.exec("insert into links (user, peer, label, placeholder, received, expires) values ('u-alice', 'peer-A', '', 'ph-1', 0, 9999999999)")
	rdb.exec("insert into links (user, peer, label, placeholder, received, expires) values ('u-bob', 'peer-A', '', 'ph-2', 0, 9999999999)")

	alice := &User{UID: "u-alice"}
	withUserThread(alice, func(th *sl.Thread) {
		v, err := api_replication_link_deny(th, sl.NewBuiltin("link_deny", api_replication_link_deny), sl.Tuple{sl.String("peer-A")}, nil)
		if err != nil {
			t.Fatalf("link_deny: %v", err)
		}
		if s, _ := v.(sl.String); string(s) != "denied" {
			t.Errorf("link_deny first call = %v, want denied", v)
		}

		// Idempotent: second call returns already-handled.
		v, _ = api_replication_link_deny(th, sl.NewBuiltin("link_deny", api_replication_link_deny), sl.Tuple{sl.String("peer-A")}, nil)
		if s, _ := v.(sl.String); string(s) != "already-handled" {
			t.Errorf("link_deny repeat = %v, want already-handled", v)
		}
	})

	// Bob's row must be untouched.
	exists, _ := rdb.exists("select 1 from links where user='u-bob' and peer='peer-A'")
	if !exists {
		t.Error("bob's link row was incorrectly removed by alice's deny")
	}
}

// TestApiReplicationJoinsAndDeny: joins() lists all pending whole-server
// join-requests (no user filter; system-wide). join_deny removes one
// row idempotently.
func TestApiReplicationJoinsAndDeny(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	orig_emit_join_denied := replication_emit_join_denied
	replication_emit_join_denied = func(peer, reason string) {}
	defer func() { replication_emit_join_denied = orig_emit_join_denied }()

	rdb := db_open("db/replication.db")
	rdb.exec("insert into joins (peer, label, received, expires) values ('peer-A', 'a.example', 0, 9999999999)")
	rdb.exec("insert into joins (peer, label, received, expires) values ('peer-B', 'b.example', 1, 9999999999)")

	th := &sl.Thread{}
	v, err := api_replication_joins(th, nil, sl.Tuple{}, nil)
	if err != nil {
		t.Fatalf("joins: %v", err)
	}
	joins := v.(*sl.List)
	if joins.Len() != 2 {
		t.Errorf("joins len = %d, want 2", joins.Len())
	}

	// Deny peer-A.
	v, err = api_replication_join_deny(th, sl.NewBuiltin("join_deny", api_replication_join_deny), sl.Tuple{sl.String("peer-A")}, nil)
	if err != nil {
		t.Fatalf("join_deny: %v", err)
	}
	if s, _ := v.(sl.String); string(s) != "denied" {
		t.Errorf("join_deny first call = %v, want denied", v)
	}

	// Idempotent.
	v, _ = api_replication_join_deny(th, sl.NewBuiltin("join_deny", api_replication_join_deny), sl.Tuple{sl.String("peer-A")}, nil)
	if s, _ := v.(sl.String); string(s) != "already-handled" {
		t.Errorf("join_deny repeat = %v, want already-handled", v)
	}

	// peer-B still pending.
	v, _ = api_replication_joins(th, nil, sl.Tuple{}, nil)
	if v.(*sl.List).Len() != 1 {
		t.Errorf("after denying A, joins len = %d, want 1", v.(*sl.List).Len())
	}
}

// TestApiReplicationPairRemove: removing a pair member drops the row
// and informs remaining members via the emit hook.
func TestApiReplicationPairRemove(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	emitted := false
	orig_emit := admin_replication_emit_pair_membership
	admin_replication_emit_pair_membership = func(full, recipients []string) { emitted = true }
	defer func() { admin_replication_emit_pair_membership = orig_emit }()

	rdb := db_open("db/replication.db")
	rdb.exec("insert into pair (peer, added, role) values ('peer-A', 0, '')")
	rdb.exec("insert into pair (peer, added, role) values ('peer-B', 0, '')")

	th := &sl.Thread{}
	v, err := api_replication_pair_remove(th, sl.NewBuiltin("pair_remove", api_replication_pair_remove), sl.Tuple{sl.String("peer-A")}, nil)
	if err != nil {
		t.Fatalf("pair_remove: %v", err)
	}
	if s, _ := v.(sl.String); string(s) != "removed" {
		t.Errorf("pair_remove = %v, want removed", v)
	}
	if !emitted {
		t.Error("admin_replication_emit_pair_membership was not called for remaining members")
	}

	// peer-B still present.
	if exists, _ := rdb.exists("select 1 from pair where peer='peer-B'"); !exists {
		t.Error("peer-B was incorrectly removed alongside peer-A")
	}

	// not-found path.
	v, _ = api_replication_pair_remove(th, sl.NewBuiltin("pair_remove", api_replication_pair_remove), sl.Tuple{sl.String("peer-unknown")}, nil)
	if s, _ := v.(sl.String); string(s) != "not-found" {
		t.Errorf("pair_remove unknown = %v, want not-found", v)
	}
}

// TestApiReplicationHostRemove: removing a host removes only the calling
// user's row, leaves other users untouched, and returns not-found when
// the peer wasn't a host.
func TestApiReplicationHostRemove(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()

	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username) values ('u-alice', 'alice')")
	udb.exec("insert into entities (id, private, fingerprint, user, class, name) values ('e-alice', '', 'fpa', 'u-alice', 'identity', 'Alice')")

	rdb := db_open("db/replication.db")
	rdb.exec("insert into hosts (user, peer, added, ack) values ('u-alice', 'peer-A', 100, 0)")
	rdb.exec("insert into hosts (user, peer, added, ack) values ('u-alice', 'peer-B', 200, 0)")
	rdb.exec("insert into hosts (user, peer, added, ack) values ('u-bob', 'peer-A', 300, 0)")

	alice := &User{UID: "u-alice"}
	withUserThread(alice, func(th *sl.Thread) {
		v, err := api_replication_host_remove(th, sl.NewBuiltin("host_remove", api_replication_host_remove), sl.Tuple{sl.String("peer-A")}, nil)
		if err != nil {
			t.Fatalf("host_remove: %v", err)
		}
		if s, _ := v.(sl.String); string(s) != "removed" {
			t.Errorf("host_remove = %v, want removed", v)
		}

		// not-found path.
		v, _ = api_replication_host_remove(th, sl.NewBuiltin("host_remove", api_replication_host_remove), sl.Tuple{sl.String("peer-unknown")}, nil)
		if s, _ := v.(sl.String); string(s) != "not-found" {
			t.Errorf("host_remove unknown peer = %v, want not-found", v)
		}
	})

	// Alice's other host and Bob's row must be intact.
	if exists, _ := rdb.exists("select 1 from hosts where user='u-alice' and peer='peer-B'"); !exists {
		t.Error("alice's peer-B host was incorrectly removed")
	}
	if exists, _ := rdb.exists("select 1 from hosts where user='u-bob' and peer='peer-A'"); !exists {
		t.Error("bob's host was incorrectly removed by alice's call")
	}
}

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

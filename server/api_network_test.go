// Mochi server: api_server_network unit tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.
//
// Broadcasts (target='pubsub') count under "queued" and messages with an
// unknown recipient host (empty target) under "unresolved"; neither has a
// target peer.

package main

import (
	"os"
	"testing"

	sl "go.starlark.net/starlark"
)

func TestApiServerNetworkQueueCounts(t *testing.T) {
	tmp, err := os.MkdirTemp("", "mochi_api_network_test")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	orig := data_dir
	data_dir = tmp
	defer func() { data_dir = orig; os.RemoveAll(tmp) }()

	q := db_open("db/queue.db")
	q.exec("create table if not exists queue ( id text primary key, type text not null default 'direct', target text not null, from_entity text not null, to_entity text not null, service text not null, event text not null, from_app text not null default '', from_services text not null default '', content blob not null default '', data blob not null default '', file text not null default '', expires integer not null default 0, status text not null default 'pending', attempts integer not null default 0, next_retry integer not null, last_error text not null default '', created integer not null, priority integer not null default 20, claimed integer not null default 0 )")
	// One broadcast, two unresolved-target direct rows, one direct row with a
	// known target. unresolved must count only the two empty-target directs;
	// queued must count only the broadcast; the resolved direct counts as
	// neither (it would appear in the per-peer peers() rollup instead).
	q.exec("insert into queue (id, type, target, from_entity, to_entity, service, event, next_retry, created) values ('b1','broadcast','pubsub','e','t','s','ev',0,0)")
	q.exec("insert into queue (id, type, target, from_entity, to_entity, service, event, next_retry, created) values ('u1','direct','','e','t1','s','ev',0,0)")
	q.exec("insert into queue (id, type, target, from_entity, to_entity, service, event, next_retry, created) values ('u2','direct','','e','t2','s','ev',0,0)")
	q.exec("insert into queue (id, type, target, from_entity, to_entity, service, event, next_retry, created) values ('d1','direct','12D3KooWPeer','e','t3','s','ev',0,0)")

	// mochi.server.network is behind server/read, so drive it the way an app
	// does: a thread carrying a granted app rather than a bare nil.
	os.MkdirAll(data_dir+"/users/u1", 0755)
	user := &User{UID: "u1", Username: "user1@example.com", Role: "administrator"}
	app := create_external_app("status")
	udb := db_user(user, "user")
	udb.permissions_setup()
	udb.permissions_upsert(app.id, "server/read", "", 1)

	result, err := api_server_network(create_test_thread(user, app),
		sl.NewBuiltin("mochi.server.network", nil), nil, nil)
	if err != nil {
		t.Fatalf("api_server_network: %v", err)
	}
	m := sl_decode_map(result)
	if got, _ := m["unresolved"].(int64); got != 2 {
		t.Errorf("unresolved = %v, want 2", m["unresolved"])
	}
	if got, _ := m["queued"].(int64); got != 1 {
		t.Errorf("queued = %v, want 1", m["queued"])
	}
}

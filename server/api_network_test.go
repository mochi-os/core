// Mochi server: api_server_network unit tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.
//
// Verifies the outbound-queue counts surfaced by mochi.server.network():
// broadcasts (target='pubsub') under "queued", and direct messages whose
// recipient host is not yet known (empty target) under "unresolved".
// Neither has a target peer, so neither appears in the peers() rollup.

package main

import (
	"os"
	"testing"
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
	q.exec("create table if not exists queue ( id text primary key, type text not null default 'direct', target text not null, from_entity text not null, to_entity text not null, service text not null, event text not null, from_app text not null default '', from_services text not null default '', content blob not null default '', data blob not null default '', file text not null default '', expires integer not null default 0, status text not null default 'pending', attempts integer not null default 0, next_retry integer not null, last_error text not null default '', created integer not null, priority integer not null default 20 )")
	// One broadcast, two unresolved-target direct rows, one direct row with a
	// known target. unresolved must count only the two empty-target directs;
	// queued must count only the broadcast; the resolved direct counts as
	// neither (it would appear in the per-peer peers() rollup instead).
	q.exec("insert into queue (id, type, target, from_entity, to_entity, service, event, next_retry, created) values ('b1','broadcast','pubsub','e','t','s','ev',0,0)")
	q.exec("insert into queue (id, type, target, from_entity, to_entity, service, event, next_retry, created) values ('u1','direct','','e','t1','s','ev',0,0)")
	q.exec("insert into queue (id, type, target, from_entity, to_entity, service, event, next_retry, created) values ('u2','direct','','e','t2','s','ev',0,0)")
	q.exec("insert into queue (id, type, target, from_entity, to_entity, service, event, next_retry, created) values ('d1','direct','12D3KooWPeer','e','t3','s','ev',0,0)")

	result, err := api_server_network(nil, nil, nil, nil)
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

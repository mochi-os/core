// Mochi server: the world socket's growth is bounded.
//
// db/world.db and the outbound gossip both scale with the number of distinct
// world ids. The per-world debounce is keyed on the id, so a caller whose id
// varies writes a fresh row per push and floods every peer, which is where the
// cost is paid.//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

// world_bounds_id builds a distinct, validly-shaped id per index: world_validate
// bounds the shape, so a test that pushed short ids would be measuring the
// validator rather than the cap.
func world_bounds_id(n int) string {
	return fmt.Sprintf("%032d", n)
}

func world_bounds_rows(peer string) int {
	return db_open("db/world.db").integer("select count(*) from worlds where peer=?", peer)
}

// TestWorldIdsAreCappedPerPeer is the finding. replace into keeps a repeated id
// free, so only new ids grow the table - and nothing counted them.
func TestWorldIdsAreCappedPerPeer(t *testing.T) {
	defer setup_world_test(t)()

	for i := 0; i < world_ids_most+50; i++ {
		world_store("peer1", world_bounds_id(i), "World", "https://world.example:4433", 1, world_test_services(1))
	}

	if got := world_bounds_rows("peer1"); got != world_ids_most {
		t.Errorf("peer holds %d rows after %d distinct ids, want the cap of %d; the table grows with whatever the caller invents",
			got, world_ids_most+50, world_ids_most)
	}
}

// TestWorldIdCapEvictsTheLeastRecentlySeen. Refusing past the cap would let one
// caller's churn lock every later world out; evicting the coldest row keeps the
// table bounded while a legitimate world can always list.
func TestWorldIdCapEvictsTheLeastRecentlySeen(t *testing.T) {
	defer setup_world_test(t)()
	db := db_open("db/world.db")

	for i := 0; i < world_ids_most; i++ {
		world_store("peer1", world_bounds_id(i), "World", "https://world.example:4433", 1, world_test_services(1))
	}
	// Age the first row so it is unambiguously the coldest, and refresh another
	// so recency rather than insertion order decides.
	db.exec("update worlds set seen=? where peer='peer1' and world=?", now()-9999, world_bounds_id(0))
	db.exec("update worlds set seen=? where peer='peer1' and world=?", now(), world_bounds_id(1))

	world_store("peer1", world_bounds_id(9999), "Newcomer", "https://world.example:4433", 1, world_test_services(1))

	if have, _ := db.exists("select 1 from worlds where peer='peer1' and world=?", world_bounds_id(0)); have {
		t.Error("the least recently seen row survived; the cap evicted something else, so a cold row can pin the table")
	}
	if have, _ := db.exists("select 1 from worlds where peer='peer1' and world=?", world_bounds_id(1)); !have {
		t.Error("a recently refreshed row was evicted ahead of a colder one")
	}
	if have, _ := db.exists("select 1 from worlds where peer='peer1' and world=?", world_bounds_id(9999)); !have {
		t.Error("the new world was not stored; past the cap a legitimate listing must displace a cold row, not be refused")
	}
	if got := world_bounds_rows("peer1"); got != world_ids_most {
		t.Errorf("peer holds %d rows, want %d", got, world_ids_most)
	}
}

// TestWorldIdCapIsPerPeer. Rows are keyed (peer, world), and a peer flooding ids
// must not evict another peer's listings - on a mesh that would let one broken
// server clear everyone else off the join page.
func TestWorldIdCapIsPerPeer(t *testing.T) {
	defer setup_world_test(t)()

	world_store("quiet", world_bounds_id(1), "Quiet", "https://world.example:4433", 1, world_test_services(1))
	for i := 0; i < world_ids_most+20; i++ {
		world_store("noisy", world_bounds_id(i), "Noisy", "https://world.example:4433", 1, world_test_services(1))
	}

	if got := world_bounds_rows("quiet"); got != 1 {
		t.Errorf("the quiet peer holds %d rows, want 1; one peer's churn evicted another's listing", got)
	}
	if got := world_bounds_rows("noisy"); got != world_ids_most {
		t.Errorf("the noisy peer holds %d rows, want the cap of %d", got, world_ids_most)
	}
}

// TestWorldRecentIsPruned. The debounce map had nothing deleting from it, so an
// id seen once stayed for the life of the process while the table aged the same
// world out after world_seen_expiry - the two disagreed about which worlds
// exist, and the map only ever grew.
func TestWorldRecentIsPruned(t *testing.T) {
	defer setup_world_test(t)()

	world_lock.Lock()
	world_recent = map[string]struct {
		content   string
		published int64
	}{
		"fresh": {content: "x", published: now()},
		"stale": {content: "x", published: now() - world_seen_expiry - 1},
	}
	world_lock.Unlock()

	world_recent_prune()

	world_lock.Lock()
	_, fresh := world_recent["fresh"]
	_, stale := world_recent["stale"]
	size := len(world_recent)
	world_lock.Unlock()

	if !fresh {
		t.Error("a world still inside the expiry lost its debounce state, so its next push floods early")
	}
	if stale {
		t.Error("a world past the expiry kept its debounce state; the table drops the row while the map keeps it for the life of the process")
	}
	if size != 1 {
		t.Errorf("world_recent holds %d entries, want 1", size)
	}

	// And the sweep has to be wired to the manager. Asserted on the source
	// because world_manager ticks every five minutes and cannot be driven from a
	// test - and without this the check above passes against a prune function
	// nothing ever calls, which is exactly how it read before.
	source, err := os.ReadFile("world.go")
	if err != nil {
		t.Fatalf("read world.go: %v", err)
	}
	manager := string(source)
	start := strings.Index(manager, "func world_manager()")
	end := strings.Index(manager[start:], "\n}\n")
	if start < 0 || end < 0 {
		t.Fatal("could not isolate world_manager")
	}
	if !strings.Contains(manager[start:start+end], "world_recent_prune()") {
		t.Error("world_manager does not call world_recent_prune, so the map is never swept in a running server")
	}
}

// TestWorldGossipHasAnAggregateFloor. world_publish_minimum is per id, so it
// cannot see a caller whose id varies. The aggregate limiter is what actually
// bounds what this server floods outward, and it is matched to the inbound
// limiter a receiving peer will apply anyway.
func TestWorldGossipHasAnAggregateFloor(t *testing.T) {
	previous := rate_limit_world_gossip.entries
	rate_limit_world_gossip.lock.Lock()
	rate_limit_world_gossip.entries = make(map[string]*rate_limit_entry)
	rate_limit_world_gossip.lock.Unlock()
	defer func() {
		rate_limit_world_gossip.lock.Lock()
		rate_limit_world_gossip.entries = previous
		rate_limit_world_gossip.lock.Unlock()
	}()

	allowed := 0
	for i := 0; i < rate_limit_world_gossip.limit*3; i++ {
		if rate_limit_world_gossip.allow("self") {
			allowed++
		}
	}
	if allowed != rate_limit_world_gossip.limit {
		t.Errorf("%d of %d publishes were allowed, want the limit of %d; a varying id would flood on every push",
			allowed, rate_limit_world_gossip.limit*3, rate_limit_world_gossip.limit)
	}
	if rate_limit_world_gossip.limit > rate_limit_world_publish.limit {
		t.Errorf("this server floods outward at %d per window while a receiving peer accepts %d; the sender must not exceed what the receiver will take",
			rate_limit_world_gossip.limit, rate_limit_world_publish.limit)
	}

	// And the limiter has to gate the publish. Asserted on the source: the
	// handler needs a live gin context and a real pubsub to drive, and without
	// this the checks above pass against a limiter nothing consults.
	source, err := os.ReadFile("world.go")
	if err != nil {
		t.Fatalf("read world.go: %v", err)
	}
	handler := string(source)
	start := strings.Index(handler, "func world_status_handler(")
	end := strings.Index(handler[start:], "\n}\n")
	if start < 0 || end < 0 {
		t.Fatal("could not isolate world_status_handler")
	}
	if !strings.Contains(handler[start:start+end], "rate_limit_world_gossip.allow(") {
		t.Error("the publish path does not consult rate_limit_world_gossip, so a varying id still floods on every push")
	}
}

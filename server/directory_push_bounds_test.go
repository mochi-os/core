// Mochi server: the anonymous directory push is bounded on both axes - how
// often a peer may push, and how many rows one push may carry. Each row costs
// four validators, up to three SQLite queries and an ed25519 verification.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"io"
	"testing"
	"time"

	"github.com/fxamacker/cbor/v2"
)

// TestDirectoryPushRateLimitedPerPeer. One valid row per push, from one peer,
// more times than the budget allows: the pushes past it must not be read.
func TestDirectoryPushRateLimitedPerPeer(t *testing.T) {
	cleanup := setup_directory_test(t)
	defer cleanup()

	peer, hk := test_host(t)
	rate_limit_directory_push.reset(peer)
	defer rate_limit_directory_push.reset(peer)

	attempts := rate_limit_directory_push.limit + 3
	entities := make([]string, attempts)
	for i := 0; i < attempts; i++ {
		entity, ek := test_identity(t)
		entities[i] = entity
		row := test_entry(t, entity, ek, peer, hk, "Alice", 100, 50, now())

		r, w := io.Pipe()
		go func() {
			enc := cbor.NewEncoder(w)
			_ = enc.Encode(row)
			w.Close()
		}()
		directory_push_event(&Event{peer: peer, stream: &Stream{reader: r}})
		r.Close()
	}

	db := db_open("db/directory.db")
	stored := 0
	for _, entity := range entities {
		stored += db.integer("select count(*) from entries where entity=?", entity)
	}
	if stored > rate_limit_directory_push.limit {
		t.Errorf("%d of %d pushes were accepted; the budget is %d per peer",
			stored, attempts, rate_limit_directory_push.limit)
	}
	if stored == 0 {
		t.Error("no push was accepted at all; the limit must admit the legitimate cadence")
	}
}

// TestDirectoryPushRateLimitIsPerPeer. One flooding peer must not deny the
// push path to every other peer - the fleet's whole directory rides on it.
func TestDirectoryPushRateLimitIsPerPeer(t *testing.T) {
	cleanup := setup_directory_test(t)
	defer cleanup()

	flooder, _ := test_host(t)
	rate_limit_directory_push.reset(flooder)
	defer rate_limit_directory_push.reset(flooder)
	for i := 0; i < rate_limit_directory_push.limit+3; i++ {
		directory_push_event(&Event{peer: flooder, stream: &Stream{}})
	}

	honest, hk := test_host(t)
	rate_limit_directory_push.reset(honest)
	defer rate_limit_directory_push.reset(honest)
	entity, ek := test_identity(t)
	row := test_entry(t, entity, ek, honest, hk, "Bob", 100, 50, now())

	r, w := io.Pipe()
	go func() {
		enc := cbor.NewEncoder(w)
		_ = enc.Encode(row)
		w.Close()
	}()
	directory_push_event(&Event{peer: honest, stream: &Stream{reader: r}})
	r.Close()

	db := db_open("db/directory.db")
	if n := db.integer("select count(*) from entries where entity=?", entity); n != 1 {
		t.Error("an unrelated peer's push was refused because another peer flooded")
	}
}

// TestDirectoryPushRowCap. The handler must stop reading at the cap rather
// than draining whatever the peer chooses to send. Driven with rows that fail
// validation cheaply: the cap counts rows READ, because a row rejected by
// entry_store still cost the validation that rejected it.
func TestDirectoryPushRowCap(t *testing.T) {
	cleanup := setup_directory_test(t)
	defer cleanup()

	peer, _ := test_host(t)
	rate_limit_directory_push.reset(peer)
	defer rate_limit_directory_push.reset(peer)

	// Far more rows than the cap admits. Invalid entity, so each is rejected
	// at entry_store's first validator - no signing needed to make the point.
	offered := directory_push_rows_maximum + 5000
	written := make(chan int, 1)
	r, w := io.Pipe()
	go func() {
		enc := cbor.NewEncoder(w)
		n := 0
		for i := 0; i < offered; i++ {
			if err := enc.Encode(&Entry{Entity: "not-an-entity", Peer: peer, Name: "x", Class: "person", Version: 1, Created: 1, Seen: 1}); err != nil {
				break
			}
			n++
		}
		w.Close()
		written <- n
	}()

	done := make(chan struct{})
	go func() {
		directory_push_event(&Event{peer: peer, stream: &Stream{reader: r}})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("push handler did not return; it is still draining the peer's rows")
	}

	// Unblock the writer, which is parked on a pipe nobody is reading.
	r.Close()
	if n := <-written; n >= offered {
		t.Errorf("the peer got all %d rows through; the cap of %d did not stop the read",
			n, directory_push_rows_maximum)
	}
}

// TestDirectoryPushLimiterMatchesItsSibling. The two anonymous directory
// handlers must stay bounded together. Push was the one left open, and it is
// the more expensive of the pair, so a future retune must not quietly drop it
// back below its sibling.
func TestDirectoryPushLimiterMatchesItsSibling(t *testing.T) {
	if rate_limit_directory_push == rate_limit_directory_sync {
		t.Fatal("push shares the sync bucket; a push flood would deny syncing and vice versa")
	}
	if rate_limit_directory_push.limit <= 0 || rate_limit_directory_push.window <= 0 {
		t.Fatal("push limiter is not configured")
	}
	// directory_sync drives at most one push per 5-minute tick, so anything at
	// or above the sync budget is already generous headroom.
	if rate_limit_directory_push.limit > rate_limit_directory_sync.limit {
		t.Errorf("push budget %d exceeds sync's %d, though push is the costlier side",
			rate_limit_directory_push.limit, rate_limit_directory_sync.limit)
	}
}

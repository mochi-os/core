// Mochi server: peer address-arrival waiter unit tests
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.
//
// Tests covering peer_await_addresses / peer_addresses_arrived — the
// bounded inline wait that lets synchronous request paths
// (remote_reach) recover from first contact with a never-seen peer.

package main

import (
	"testing"
	"time"
)

// TestPeerAwaitTimeout confirms a waiter with no arrival reports false
// after the timeout, and deregisters itself.
func TestPeerAwaitTimeout(t *testing.T) {
	peer := "12D3KooWAwaitTestTimeout"
	start := time.Now()
	if peer_await_addresses(peer, 20*time.Millisecond) {
		t.Error("waiter with no arrival must report false")
	}
	if time.Since(start) < 20*time.Millisecond {
		t.Error("waiter returned before the timeout elapsed")
	}
	peer_waiters_lock.Lock()
	_, found := peer_waiters[peer]
	peer_waiters_lock.Unlock()
	if found {
		t.Error("timed-out waiter must deregister itself")
	}
}

// TestPeerAwaitArrival confirms peer_addresses_arrived wakes a blocked
// waiter well before its timeout.
func TestPeerAwaitArrival(t *testing.T) {
	peer := "12D3KooWAwaitTestArrival"
	woken := make(chan bool, 1)
	go func() {
		woken <- peer_await_addresses(peer, 5*time.Second)
	}()

	// Give the waiter time to register before signalling.
	deadline := time.Now().Add(time.Second)
	for {
		peer_waiters_lock.Lock()
		registered := len(peer_waiters[peer]) > 0
		peer_waiters_lock.Unlock()
		if registered {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("waiter never registered")
		}
		time.Sleep(time.Millisecond)
	}

	start := time.Now()
	peer_addresses_arrived(peer)
	select {
	case ok := <-woken:
		if !ok {
			t.Error("signalled waiter must report true")
		}
		if time.Since(start) > time.Second {
			t.Error("signalled waiter took too long to wake")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("signalled waiter never woke")
	}
	peer_waiters_lock.Lock()
	_, found := peer_waiters[peer]
	peer_waiters_lock.Unlock()
	if found {
		t.Error("signalled waiter must be removed from the registry")
	}
}

// TestPeerArrivedNoWaiters confirms signalling with no one blocked is a
// no-op rather than a panic or a stuck registry entry.
func TestPeerArrivedNoWaiters(t *testing.T) {
	peer_addresses_arrived("12D3KooWAwaitTestNobody")
}

// TestPeerAwaitMultipleWaiters confirms one arrival wakes every waiter
// blocked on the same peer.
func TestPeerAwaitMultipleWaiters(t *testing.T) {
	peer := "12D3KooWAwaitTestMany"
	woken := make(chan bool, 3)
	for i := 0; i < 3; i++ {
		go func() {
			woken <- peer_await_addresses(peer, 5*time.Second)
		}()
	}

	deadline := time.Now().Add(time.Second)
	for {
		peer_waiters_lock.Lock()
		registered := len(peer_waiters[peer])
		peer_waiters_lock.Unlock()
		if registered == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("waiters never all registered")
		}
		time.Sleep(time.Millisecond)
	}

	peer_addresses_arrived(peer)
	for i := 0; i < 3; i++ {
		select {
		case ok := <-woken:
			if !ok {
				t.Error("signalled waiter must report true")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("a signalled waiter never woke")
		}
	}
}

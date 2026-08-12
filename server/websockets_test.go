// Mochi server: WebSocket registry termination
//
// A broken connection drives two goroutines at once: the reader's ws.Read
// fails and terminates it, and any concurrent send's ws.Write fails and
// terminates it too. Both then touch the same registry entry, so termination
// has to be safe to reach twice and from either side.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"nhooyr.io/websocket"
)

// websocket_pair returns a live server-side connection and a function that
// breaks it, so a send against it fails the way a vanished client does.
func websocket_pair(t *testing.T) (*websocket.Conn, func()) {
	t.Helper()
	accepted := make(chan *websocket.Conn, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		accepted <- ws
		<-r.Context().Done()
	}))

	client, _, err := websocket.Dial(context.Background(), "ws"+server.URL[len("http"):], nil)
	if err != nil {
		server.Close()
		t.Fatalf("dial: %v", err)
	}
	ws := <-accepted

	break_it := func() {
		client.CloseNow()
		server.CloseClientConnections()
	}
	t.Cleanup(func() {
		client.CloseNow()
		server.Close()
	})
	return ws, break_it
}

// websocket_register puts a connection into the registry the way
// websocket_connection does, and clears the user's entry afterwards.
func websocket_register(t *testing.T, u *User, key, id string, ws *websocket.Conn) {
	t.Helper()
	websockets_lock.Lock()
	if websockets[u.UID] == nil {
		websockets[u.UID] = map[string]map[string]*websocket.Conn{}
	}
	if websockets[u.UID][key] == nil {
		websockets[u.UID][key] = map[string]*websocket.Conn{}
	}
	websockets[u.UID][key][id] = ws
	websockets_lock.Unlock()

	t.Cleanup(func() {
		websockets_lock.Lock()
		delete(websockets, u.UID)
		websockets_lock.Unlock()
	})
}

func websocket_registered(u *User, key, id string) bool {
	websockets_lock.RLock()
	defer websockets_lock.RUnlock()
	_, present := websockets[u.UID][key][id]
	return present
}

// TestTerminateToleratesNilConnection — websockets_send used to re-look-up the
// connection in its termination pass, which yields nil once the reader
// goroutine has already removed it. CloseNow dereferences its receiver, so
// that nil was a panic on the ordinary disconnect path.
func TestTerminateToleratesNilConnection(t *testing.T) {
	u := &User{UID: "user-nil"}
	websocket_register(t, u, "key", "other", nil)

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("terminating an already-removed connection panicked: %v", r)
		}
	}()
	websocket_terminate(nil, u, "key", "gone")
}

// TestSendTerminatesBrokenConnection — the behaviour the termination pass
// exists for: a connection that cannot be written to is closed and removed.
func TestSendTerminatesBrokenConnection(t *testing.T) {
	u := &User{UID: "user-broken"}
	ws, break_it := websocket_pair(t)
	websocket_register(t, u, "key", "id", ws)
	break_it()

	// The first write may land in the socket buffer even though the peer has
	// gone; the second reliably fails. Sending twice matches what a live
	// server does anyway.
	websockets_send(u, "key", map[string]any{"a": 1})
	websockets_send(u, "key", map[string]any{"a": 2})

	if websocket_registered(u, "key", "id") {
		t.Error("a connection that could not be written to is still registered")
	}
}

// TestSendRacesReaderTermination — the race itself. The reader goroutine
// terminates the connection while a send is doing the same, which is what one
// broken connection actually produces. Run under -race this also covers the
// registry access, not just the panic.
func TestSendRacesReaderTermination(t *testing.T) {
	u := &User{UID: "user-race"}

	for round := 0; round < 25; round++ {
		ws, break_it := websocket_pair(t)
		websocket_register(t, u, "key", "id", ws)
		break_it()

		var wg sync.WaitGroup
		wg.Add(2)
		// The reader goroutine's path: Read fails, terminate.
		go func() {
			defer wg.Done()
			websocket_terminate(ws, u, "key", "id")
		}()
		// A concurrent send's path: Write fails, terminate.
		go func() {
			defer wg.Done()
			websockets_send(u, "key", map[string]any{"round": round})
		}()
		wg.Wait()

		if websocket_registered(u, "key", "id") {
			t.Fatalf("round %d: the connection survived termination from both sides", round)
		}
	}
}

// TestTerminateIsIdempotent — the read loop's deferred terminate and any
// explicit one must not compound. Closing twice and deleting twice has to be
// harmless, since which of the two paths gets there first is a race.
func TestTerminateIsIdempotent(t *testing.T) {
	u := &User{UID: "user-twice"}
	ws, _ := websocket_pair(t)
	websocket_register(t, u, "key", "id", ws)

	websocket_terminate(ws, u, "key", "id")
	websocket_terminate(ws, u, "key", "id")

	if websocket_registered(u, "key", "id") {
		t.Error("still registered after termination")
	}
	websockets_lock.RLock()
	_, user_present := websockets[u.UID]
	websockets_lock.RUnlock()
	if user_present {
		t.Error("the user's empty entry was left behind")
	}
}

// TestSendLeavesLiveConnectionsRegistered — the guard against over-correcting:
// a working connection must survive a send that terminates a broken sibling.
func TestSendLeavesLiveConnectionsRegistered(t *testing.T) {
	u := &User{UID: "user-mixed"}

	live, _ := websocket_pair(t)
	broken, break_broken := websocket_pair(t)
	websocket_register(t, u, "key", "live", live)
	websockets_lock.Lock()
	websockets[u.UID]["key"]["broken"] = broken
	websockets_lock.Unlock()
	break_broken()

	websockets_send(u, "key", map[string]any{"a": 1})
	websockets_send(u, "key", map[string]any{"a": 2})

	if websocket_registered(u, "key", "broken") {
		t.Error("the broken connection is still registered")
	}
	if !websocket_registered(u, "key", "live") {
		t.Error("a working connection was terminated alongside the broken one")
	}
}

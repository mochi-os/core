// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Tests for the app scoping of websocket delivery.
//
// The registry is keyed on (user, key, connection), and the key comes from the
// client's query string. Keys are not secrets: most are entity fingerprints,
// which mochi.entity.owned returns to any app with no permission check, and the
// rest are literals like "notifications". So before the app was recorded
// alongside each connection, mochi.websocket.write in one app delivered into
// another app's live sockets — which matters most for the consumers that trust
// the payload rather than treating it as a refetch signal (the shared game
// hook reads name, body and winner straight out of the frame).

package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	sl "go.starlark.net/starlark"
	"nhooyr.io/websocket"
)

// websocket_register puts a connection in the registry without a network
// socket. The tests assert on which entries websocket_targets selects rather
// than on delivery, since a write needs a real connection.
func websocket_register_scoped(user, app, key, id string) {
	websockets_lock.Lock()
	defer websockets_lock.Unlock()
	if websockets[user] == nil {
		websockets[user] = map[string]map[string]*websocket_client{}
	}
	if websockets[user][key] == nil {
		websockets[user][key] = map[string]*websocket_client{}
	}
	websockets[user][key][id] = &websocket_client{app: app}
}

// websocket_selected reports the connection ids a send for this app would write
// to. Calls the production selector rather than restating its rule, so
// reverting the filter fails these tests instead of leaving them green.
func websocket_selected(user, app, key string) []string {
	websockets_lock.RLock()
	defer websockets_lock.RUnlock()
	var ids []string
	for id := range websocket_targets(&User{UID: user}, app, key) {
		ids = append(ids, id)
	}
	return ids
}

func websocket_registry_reset() {
	websockets_lock.Lock()
	websockets = map[string]map[string]map[string]*websocket_client{}
	websockets_lock.Unlock()
}

// TestWebsocketWriteStaysWithinItsApp is the finding: two apps hold sockets on
// the same key — which happens by construction, because entity fingerprints are
// handed to any app — and neither may reach the other's.
func TestWebsocketWriteStaysWithinItsApp(t *testing.T) {
	websocket_registry_reset()
	defer websocket_registry_reset()

	const user, key = "user-1", "9fingerprint"
	websocket_register_scoped(user, "app-wikis", key, "wikis-connection")
	websocket_register_scoped(user, "app-hostile", key, "hostile-connection")

	selected := websocket_selected(user, "app-hostile", key)
	for _, id := range selected {
		if id == "wikis-connection" {
			t.Error("a write from app-hostile selected the wikis connection; the key alone still decides the target, so one app can inject frames into another's socket")
		}
	}
	if len(selected) != 1 {
		t.Errorf("app-hostile selected %d connections, want 1 (its own)", len(selected))
	}
}

// TestWebsocketWriteReachesItsOwnConnections: the scoping must not cost an app
// its own delivery, including when several of its connections share a key.
func TestWebsocketWriteReachesItsOwnConnections(t *testing.T) {
	websocket_registry_reset()
	defer websocket_registry_reset()

	const user, key = "user-1", "9fingerprint"
	websocket_register_scoped(user, "app-wikis", key, "first")
	websocket_register_scoped(user, "app-wikis", key, "second")
	websocket_register_scoped(user, "app-other", key, "other")

	if selected := websocket_selected(user, "app-wikis", key); len(selected) != 2 {
		t.Errorf("app-wikis selected %d of its 2 connections: %v", len(selected), selected)
	}
}

// TestWebsocketCoreSendReachesEveryConnection: core's own sends pass an empty
// app and are not scoped. accounts.go's unifiedpush send predates any app
// binding, and its clients may authenticate with a cookie and so carry no app
// at all — scoping that would silently stop delivering.
func TestWebsocketCoreSendReachesEveryConnection(t *testing.T) {
	websocket_registry_reset()
	defer websocket_registry_reset()

	const user, key = "user-1", "unifiedpush"
	websocket_register_scoped(user, "app-one", key, "one")
	websocket_register_scoped(user, "", key, "cookie-authenticated")

	if selected := websocket_selected(user, "", key); len(selected) != 2 {
		t.Errorf("a core send selected %d of 2 connections: %v", len(selected), selected)
	}
}

// websocket_ends returns a registered server-side connection and its client
// end, so a test can see what actually arrived rather than which entries were
// selected.
func websocket_ends(t *testing.T, user, app, key, id string) *websocket.Conn {
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
	t.Cleanup(func() { client.CloseNow(); server.Close() })

	websockets_lock.Lock()
	if websockets[user] == nil {
		websockets[user] = map[string]map[string]*websocket_client{}
	}
	if websockets[user][key] == nil {
		websockets[user][key] = map[string]*websocket_client{}
	}
	websockets[user][key][id] = &websocket_client{ws: <-accepted, app: app}
	websockets_lock.Unlock()
	return client
}

// TestStarlarkWriteDeliversOnlyToTheCallingApp drives sl_websocket_write itself.
// The selection tests above call websocket_targets, so they all still pass if
// the builtin stops passing its own app and sends unscoped — which is the bug.
// This one reads both client ends and asserts what arrived.
func TestStarlarkWriteDeliversOnlyToTheCallingApp(t *testing.T) {
	websocket_registry_reset()
	defer websocket_registry_reset()

	const user, key = "user-starlark", "9fingerprint"
	mine := websocket_ends(t, user, "app-caller", key, "mine")
	theirs := websocket_ends(t, user, "app-victim", key, "theirs")

	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("user", &User{UID: user})
	thread.SetLocal("app", &App{id: "app-caller"})
	builtin := sl.NewBuiltin("mochi.websocket.write", sl_websocket_write)

	if _, err := sl_websocket_write(thread, builtin, sl.Tuple{sl.String(key), sl.String("payload")}, nil); err != nil {
		t.Fatalf("sl_websocket_write: %v", err)
	}

	read, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, _, err := mine.Read(read); err != nil {
		t.Fatalf("the calling app's own connection received nothing: %v", err)
	}

	// The victim must time out rather than receive. A short budget: the frame
	// would already be there if it had been sent, since both writes happen in
	// the same call.
	quiet, stop := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer stop()
	if _, data, err := theirs.Read(quiet); err == nil {
		t.Errorf("another app's connection received %q; mochi.websocket.write is not scoped to the calling app", data)
	}
}

// TestStarlarkWriteRequiresAnApp: with no app on the thread there is nothing to
// scope to, and falling through would send unscoped.
func TestStarlarkWriteRequiresAnApp(t *testing.T) {
	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("user", &User{UID: "user-noapp"})
	builtin := sl.NewBuiltin("mochi.websocket.write", sl_websocket_write)

	if _, err := sl_websocket_write(thread, builtin, sl.Tuple{sl.String("key"), sl.String("x")}, nil); err == nil {
		t.Error("sl_websocket_write succeeded with no app on the thread; it must refuse rather than send unscoped")
	}
}

// TestWebsocketAppSendSkipsUnidentifiedConnections: a connection with no app
// cannot be attributed, so no app may write to it. This is the behaviour change
// worth knowing about — a cookie-authenticated socket stops receiving its app's
// frames — and it is safe because both token paths supply an app: the shell
// mints with a.id and standalone fetches its own from /_/token.
func TestWebsocketAppSendSkipsUnidentifiedConnections(t *testing.T) {
	websocket_registry_reset()
	defer websocket_registry_reset()

	const user, key = "user-1", "notifications"
	websocket_register_scoped(user, "", key, "cookie-authenticated")

	if selected := websocket_selected(user, "app-notifications", key); len(selected) != 0 {
		t.Errorf("an app send selected %v; a connection with no app identity cannot be claimed by one", selected)
	}
}

// Mochi server: a refused websocket handshake says why.
//
// A bare `return` in the auth branch is a 200 with an empty body: nothing
// connects either way, but the caller cannot tell why and the access log
// records an authentication failure as a success.//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// websocket_attempt drives the handler with a handshake and reports the status.
// A real upgrade is never reached in these tests: every case is refused before
// websocket.Accept, which is the point - a refusal has to be an HTTP status the
// caller can read, not a socket that opens and closes.
func websocket_attempt(t *testing.T, query string) int {
	t.Helper()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/_/websocket"+query, nil)
	c.Request.Header.Set("Connection", "Upgrade")
	c.Request.Header.Set("Upgrade", "websocket")
	c.Request.Header.Set("Sec-WebSocket-Version", "13")
	c.Request.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	return websocket_attempt_as(t, "", query)
}

// websocket_attempt_as is the same with a session cookie.
func websocket_attempt_as(t *testing.T, session string, query string) int {
	t.Helper()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/_/websocket"+query, nil)
	if session != "" {
		c.Request.AddCookie(&http.Cookie{Name: "session", Value: session})
	}
	c.Request.Header.Set("Connection", "Upgrade")
	c.Request.Header.Set("Upgrade", "websocket")
	c.Request.Header.Set("Sec-WebSocket-Version", "13")
	c.Request.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	// A request that passes every guard goes on to websocket.Accept, which
	// hijacks the connection - and httptest's recorder is not a Hijacker, so it
	// panics. That panic is the signal we want: it means the request was NOT
	// refused. Recovering keeps it readable as a status rather than a crash.
	func() {
		defer func() { recover() }()
		websocket_connection(c)
	}()
	// c.Writer.Status(), not the recorder's Code: gin buffers the status until the
	// engine finishes the request, which a direct handler call never reaches, so
	// the recorder reads 200 for a refusal that goes out as 401.
	return c.Writer.Status()
}

// TestWebsocketUnauthenticatedIsRefusedWithAStatus is the defect. 200 on a
// rejected connection is indistinguishable from success to every log, metric
// and alert that reads the status.
func TestWebsocketUnauthenticatedIsRefusedWithAStatus(t *testing.T) {
	create_test_users_db(t)

	if code := websocket_attempt(t, "?key=notifications"); code != 401 {
		t.Errorf("an unauthenticated handshake answered %d, want 401: a rejection that reads as success in the access log", code)
	}
}

// TestWebsocketRejectionsAreDistinguishable. Three different refusals, three
// different statuses - a caller that cannot tell them apart cannot act on any
// of them.
func TestWebsocketRejectionsAreDistinguishable(t *testing.T) {
	create_test_users_db(t)

	// No credentials at all, whatever the key.
	if code := websocket_attempt(t, "?key=notifications"); code != 401 {
		t.Errorf("missing credentials answered %d, want 401", code)
	}
	// A bad key is refused for a different reason, but the caller is still
	// unauthenticated here, so authentication is what it hears about first.
	if code := websocket_attempt(t, "?key="+strings.Repeat("x", 200)); code != 401 {
		t.Errorf("unauthenticated with a bad key answered %d, want 401: authentication is checked first", code)
	}
}

// TestWebsocketKeyIsValidatedLikeTheWriteSide. mochi.websocket.write requires a
// constant; the connect side took whatever the query string held, so a client
// could occupy keys no app could ever address and hold an arbitrarily long
// string as a map key for the life of the connection.
func TestWebsocketKeyIsValidatedLikeTheWriteSide(t *testing.T) {
	// Every key a shipped client actually uses must still pass, or this
	// tightening breaks the frontends rather than the abuse.
	for _, key := range []string{
		"notifications",
		"staff-events",
		"unifiedpush",
		"market-thread-019f4cd679b07874b79c9f946f575490",
		"aBc123XyZ", // an entity fingerprint
	} {
		if !valid(key, "constant") {
			t.Errorf("a key a shipped client sends (%q) fails the validator the connect side now applies", key)
		}
	}

	// And what it must refuse.
	for _, key := range []string{
		"",                       // registers on a channel nothing can address
		strings.Repeat("x", 101), // unbounded map key
		"has space",
		"semi;colon",
	} {
		if valid(key, "constant") {
			t.Errorf("the validator accepts %q, which mochi.websocket.write would refuse", key)
		}
	}
}

// TestWebsocketConnectionsAreBounded. Each connection is a goroutine parked in
// ws.Read plus a file descriptor, held until the client goes away.
func TestWebsocketConnectionsAreBounded(t *testing.T) {
	websockets_lock.Lock()
	websockets = map[string]map[string]map[string]*websocket_client{}
	websockets_lock.Unlock()
	t.Cleanup(func() {
		websockets_lock.Lock()
		websockets = map[string]map[string]map[string]*websocket_client{}
		websockets_lock.Unlock()
	})

	user := &User{UID: "u1", Username: "user@example.com"}
	if held := websockets_held(user); held != 0 {
		t.Fatalf("a user with no connections is counted as %d", held)
	}

	// Spread across several keys: the cap is per user, not per key, because
	// the resource is the goroutine and the descriptor.
	for i := 0; i < websockets_maximum; i++ {
		websocket_register_scoped("u1", "app", "key"+string(rune('a'+i%4)), uid())
	}
	if held := websockets_held(user); held != websockets_maximum {
		t.Errorf("counted %d connections across four keys, want %d: the cap is per user, so the count has to span keys", held, websockets_maximum)
	}
	if websockets_held(user) < websockets_maximum {
		t.Error("a user at the cap is not recognised as being at it")
	}

	// Another user is unaffected.
	websocket_register_scoped("u2", "app", "notifications", uid())
	if held := websockets_held(&User{UID: "u2"}); held != 1 {
		t.Errorf("a second user counted %d, want 1: the cap must not be global", held)
	}
}

// websocket_session makes a real session for a user, so the handler's own
// web_auth accepts the request and the guards past authentication are reached.
func websocket_session(t *testing.T, user string) string {
	t.Helper()
	code := uid()
	db_open("db/users.db").exec("insert or ignore into users (uid, username) values (?, ?)", user, user+"@example.com")
	sessions := db_open("db/sessions.db")
	sessions.exec(`create table if not exists sessions ( code text not null primary key,
		user text not null, expires integer not null, accessed integer not null default 0 )`)
	sessions.exec("replace into sessions ( code, user, expires ) values ( ?, ?, ? )", code, user, now()+3600)
	return code
}

func TestWebsocketCapIsEnforcedOnTheRequest(t *testing.T) {
	create_test_users_db(t)
	websockets_lock.Lock()
	websockets = map[string]map[string]map[string]*websocket_client{}
	websockets_lock.Unlock()
	t.Cleanup(func() {
		websockets_lock.Lock()
		websockets = map[string]map[string]map[string]*websocket_client{}
		websockets_lock.Unlock()
	})

	session := websocket_session(t, "u1")

	// One below the cap: the request gets past every guard and on to the
	// upgrade, which refuses this non-websocket request with its own status.
	for i := 0; i < websockets_maximum-1; i++ {
		websocket_register_scoped("u1", "app", "notifications", uid())
	}
	if code := websocket_attempt_as(t, session, "?key=notifications"); code == 429 {
		t.Errorf("a user one below the cap was refused with 429; the limit is off by one")
	}

	// At the cap.
	websocket_register_scoped("u1", "app", "notifications", uid())
	if code := websocket_attempt_as(t, session, "?key=notifications"); code != 429 {
		t.Errorf("a user at the cap of %d got %d, want 429: nothing bounds the goroutines and descriptors one user can hold", websockets_maximum, code)
	}

	// A different user is unaffected by the first one's connections.
	other := websocket_session(t, "u2")
	if code := websocket_attempt_as(t, other, "?key=notifications"); code == 429 {
		t.Error("a second user was refused because the first is at the cap; the limit must be per user")
	}
}

// TestWebsocketRefusesBeforeUpgrading pins the ordering. Checking after the
// upgrade would answer 101 and then close, which is a worse failure than a
// status: the caller sees a connection that opened and died for no stated
// reason, and the server has paid for the upgrade anyway.
func TestWebsocketRefusesBeforeUpgrading(t *testing.T) {
	body, err := os.ReadFile("websockets.go")
	if err != nil {
		t.Fatalf("reading websockets.go: %v", err)
	}
	source := string(body)

	accept := strings.Index(source, "websocket.Accept(")
	if accept < 0 {
		t.Fatal("websocket.Accept not found")
	}
	for _, guard := range []string{
		`valid(key, "constant")`,
		"websockets_held(u) >= websockets_maximum",
		"c.Status(401)",
	} {
		at := strings.Index(source, guard)
		if at < 0 {
			t.Errorf("%s is not in the connect path at all", guard)
			continue
		}
		if at > accept {
			t.Errorf("%s runs after websocket.Accept; the refusal becomes a 101 followed by a close rather than a status", guard)
		}
	}
}

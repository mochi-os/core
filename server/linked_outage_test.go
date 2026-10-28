// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sl "go.starlark.net/starlark"
	"mellium.im/xmpp/jid"
	"nhooyr.io/websocket"
)

// TestLinkedCause: why a session ended is sorted into the reason the
// account's page translates, with the status a server answered.
func TestLinkedCause(t *testing.T) {
	dial := &url.Error{Op: "Get", URL: "https://example.org/", Err: &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}}
	for _, c := range []struct {
		name   string
		err    error
		reason string
		status int
	}{
		{"a session that ended with no error", nil, "closed", 0},
		{"a connection that ended", fmt.Errorf("%w: %v", linked_closed, io.ErrUnexpectedEOF), "closed", 0},
		{"a server's error answer", linked_failure(503, []byte(`{"error":"busy"}`)), "status", 503},
		{"a host the owner blocks", &host_refusal{Host: "example.org", Reason: "blocked"}, "blocked", 0},
		{"a redirect to a blocked host", &url.Error{Op: "Get", URL: "https://example.org/", Err: &host_refusal{Host: "example.net", Reason: "blocked"}}, "blocked", 0},
		{"a request whose dial failed", dial, "unreachable", 0},
		{"a connection that could not be made", fmt.Errorf("%w: %w", linked_unreachable, errors.New("no route")), "unreachable", 0},
		{"a request that timed out", fmt.Errorf("the stream could not be opened: %w", context.DeadlineExceeded), "unreachable", 0},
		{"anything else", errors.New("the server's answer is not JSON"), "failed", 0},
	} {
		if reason, status := linked_cause(c.err); reason != c.reason || status != c.status {
			t.Errorf("THE POINT: %s (%v) = %s %d, want %s %d", c.name, c.err, reason, status, c.reason, c.status)
		}
	}
	if text := linked_failure(503, []byte(`{"error":"busy"}`)).Error(); text != "the server answered 503: busy" {
		t.Errorf("a server's error answer reads %q", text)
	}
}

// TestLinkedFailureShown: a linked account answers why it is not working as
// a reason, with the status and when it tries again where there are any; a
// sign-in the server no longer accepts outranks why its session is down.
func TestLinkedFailureShown(t *testing.T) {
	alice := create_linked_test_env(t)
	retry := time.Now().Add(time.Minute)
	s := &linked_session{}
	s.outage.Store(&linked_outage{reason: "status", status: 502, retry: retry, err: linked_failure(502, nil)})
	key := linked_session_key(alice.UID, "d1")
	linked_lock.Lock()
	linked_sessions[key] = s
	linked_lock.Unlock()
	defer func() {
		linked_lock.Lock()
		delete(linked_sessions, key)
		linked_lock.Unlock()
	}()

	present := account_present(alice, map[string]any{"id": "d1", "type": "mastodon", "data": json_encode(map[string]any{"server": "example.org"})})
	failure, _ := present["failure"].(map[string]any)
	if failure["reason"] != "status" || failure["status"] != 502 || failure["retry"] != retry.Unix() || present["error"] != "the server answered 502: " || present["connected"] != false {
		t.Errorf("THE POINT: a session that is down = %v", present)
	}
	present = account_present(alice, map[string]any{"id": "d1", "type": "mastodon", "data": json_encode(map[string]any{"server": "example.org", "error": linked_signed_out.Error()})})
	if failure, _ := present["failure"].(map[string]any); len(failure) != 1 || failure["reason"] != "signed_out" {
		t.Errorf("THE POINT: a sign-in no longer accepted, with the session down = %v", present["failure"])
	}
	present = account_present(alice, map[string]any{"id": "b1", "type": "bluesky", "data": json_encode(map[string]any{"server": "example.org", "error": linked_signed_out.Error()})})
	if failure, _ := present["failure"].(map[string]any); failure["reason"] != "signed_out" || present["error"] != linked_signed_out.Error() {
		t.Errorf("THE POINT: a linked account with no session whose sign-in is no longer accepted = %v", present)
	}
}

// TestMastodonOutage: a Mastodon account whose stream the server refuses
// answers the status and when it tries again, and no time while it tries;
// once the stream is open it answers no failure, and when the stream ends,
// that it ended.
func TestMastodonOutage(t *testing.T) {
	alice := create_linked_test_env(t)
	saved := linked_retry
	linked_retry = 1500 * time.Millisecond
	defer func() { linked_retry = saved }()
	var lock sync.Mutex
	instances, streams := 0, 0
	hold := make(chan struct{})
	drop := make(chan struct{})
	var release, dropped sync.Once
	mux := http.NewServeMux()
	var server *httptest.Server
	mux.HandleFunc("GET /api/v1/instance", func(w http.ResponseWriter, r *http.Request) {
		lock.Lock()
		instances++
		n := instances
		lock.Unlock()
		if n == 2 {
			<-hold
		}
		answer(w, 200, map[string]any{"urls": map[string]any{"streaming_api": "ws://" + fake_host(server)}})
	})
	mux.HandleFunc("GET /api/v1/conversations", func(w http.ResponseWriter, r *http.Request) {
		answer(w, 200, []any{})
	})
	mux.HandleFunc("GET /api/v1/streaming", func(w http.ResponseWriter, r *http.Request) {
		lock.Lock()
		streams++
		n := streams
		lock.Unlock()
		if n == 1 {
			w.WriteHeader(http.StatusGatewayTimeout)
			return
		}
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{"tok"}})
		if err != nil {
			return
		}
		go func() {
			for {
				if _, _, err := conn.Read(context.Background()); err != nil {
					return
				}
			}
		}()
		<-drop
		conn.Close(websocket.StatusGoingAway, "")
	})
	server = httptest.NewServer(mux)
	defer server.Close()
	defer release.Do(func() { close(hold) })
	defer dropped.Do(func() { close(drop) })
	host := fake_host(server)
	db_user(alice, "user").exec("insert into accounts (id, type, label, identifier, data, created, verified, enabled) values ('d1', 'mastodon', 'Alice', ?, ?, 1, 1, 1)", "@alice@"+host, json_encode(map[string]any{"id": "https://" + host + "/@alice", "server": host, "token": "tok", "account": "a1"}))
	thread := linked_thread(alice)
	fn := sl.NewBuiltin("test", nil)
	present := func() map[string]any {
		return decoded(t)(api_account_get(thread, fn, sl.Tuple{sl.String("d1")}, nil))
	}
	linked_start(linked_get(alice, "d1"))
	defer linked_stop(alice.UID, "d1")

	var outage *linked_outage
	wait(t, "the refused stream", func() bool { _, outage = linked_connected(alice.UID, "d1"); return outage != nil })
	if outage.reason != "status" || outage.status != http.StatusGatewayTimeout {
		t.Errorf("THE POINT: a refused stream = %s %d (%v)", outage.reason, outage.status, outage)
	}
	if left := time.Until(outage.retry); left < linked_retry/2 {
		t.Errorf("THE POINT: the retry is %v away, want about %v", left, linked_retry)
	}
	shown := present()
	failure, _ := shown["failure"].(map[string]any)
	if failure["reason"] != "status" || failure["status"] != int64(http.StatusGatewayTimeout) || failure["retry"] != outage.retry.Unix() {
		t.Errorf("THE POINT: failure = %v", shown["failure"])
	}
	if text, _ := shown["error"].(string); !strings.Contains(text, "504") {
		t.Errorf("THE POINT: the error text older clients print = %q", text)
	}

	wait(t, "the second attempt", func() bool {
		lock.Lock()
		defer lock.Unlock()
		return instances == 2
	})
	_, outage = linked_connected(alice.UID, "d1")
	if outage == nil || outage.reason != "status" || !outage.retry.IsZero() {
		t.Errorf("THE POINT: while trying again = %v", outage)
	}
	if failure, _ := present()["failure"].(map[string]any); failure["reason"] != "status" || failure["retry"] != nil {
		t.Errorf("THE POINT: failure while trying again = %v", failure)
	}
	release.Do(func() { close(hold) })

	wait(t, "the stream", func() bool { connected, _ := linked_connected(alice.UID, "d1"); return connected })
	if shown := present(); shown["failure"] != nil || shown["error"] != nil {
		t.Errorf("THE POINT: an open stream answers failure %v, error %v", shown["failure"], shown["error"])
	}
	dropped.Do(func() { close(drop) })

	wait(t, "the stream to end", func() bool {
		_, outage = linked_connected(alice.UID, "d1")
		return outage != nil && outage.reason == "closed"
	})
	if failure, _ := present()["failure"].(map[string]any); failure["reason"] != "closed" || failure["status"] != nil || failure["retry"] == nil {
		t.Errorf("THE POINT: failure once the stream ended = %v", failure)
	}
}

// TestXMPPOutage: an XMPP account whose server cannot be reached answers
// that, a connected one no failure, and one whose server ends the stream,
// cleanly or not, that it ended.
func TestXMPPOutage(t *testing.T) {
	alice := create_linked_test_env(t)
	saved := linked_retry
	linked_retry = 500 * time.Millisecond
	defer func() { linked_retry = saved }()
	fake := create_xmpp_fake(t, "secret")
	var up atomic.Bool
	xmpp_dial = func(ctx context.Context, address jid.JID) (net.Conn, error) {
		if !up.Load() {
			return nil, errors.New("no route to the server")
		}
		return fake.dial(ctx, address)
	}
	db_user(alice, "user").exec("insert into accounts (id, type, label, identifier, data, created, verified, enabled) values ('x1', 'xmpp', 'alice', 'alice@example.org', ?, 1, 1, 1)", json_encode(map[string]any{"id": "xmpp:alice@example.org", "address": "alice@example.org", "server": "example.org", "password": "secret"}))
	linked_start(linked_get(alice, "x1"))
	defer linked_stop(alice.UID, "x1")

	var outage *linked_outage
	wait(t, "the failed dial", func() bool { _, outage = linked_connected(alice.UID, "x1"); return outage != nil })
	if outage.reason != "unreachable" {
		t.Errorf("THE POINT: a server that cannot be reached = %s (%v)", outage.reason, outage)
	}

	up.Store(true)
	wait(t, "the connection", func() bool { connected, _ := linked_connected(alice.UID, "x1"); return connected })
	if _, outage := linked_connected(alice.UID, "x1"); outage != nil {
		t.Errorf("THE POINT: a connected session = %v", outage)
	}
	fake.lock.Lock()
	session := fake.sessions[len(fake.sessions)-1]
	fake.lock.Unlock()
	session.Close()
	wait(t, "the closed stream", func() bool {
		connected, outage := linked_connected(alice.UID, "x1")
		return !connected && outage != nil
	})
	if _, outage = linked_connected(alice.UID, "x1"); outage.reason != "closed" {
		t.Errorf("THE POINT: a stream the server ended = %s (%v)", outage.reason, outage)
	}

	wait(t, "the connection again", func() bool { connected, _ := linked_connected(alice.UID, "x1"); return connected })
	fake.lock.Lock()
	session = fake.sessions[len(fake.sessions)-1]
	fake.lock.Unlock()
	session.Conn().Write([]byte("<<not xml"))
	wait(t, "the broken stream", func() bool { connected, _ := linked_connected(alice.UID, "x1"); return !connected })
	wait(t, "the reason", func() bool { _, outage = linked_connected(alice.UID, "x1"); return outage != nil })
	if outage.reason != "closed" {
		t.Errorf("THE POINT: a stream that broke = %s (%v)", outage.reason, outage)
	}
}

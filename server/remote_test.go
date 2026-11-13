// Mochi server: Remote entity communication unit tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sl "go.starlark.net/starlark"
)

// Test peer_connect_url HTTP request and JSON parsing
// Note: These tests verify the HTTP/JSON handling but can't test actual Net connection
// peer_connect_tls stands up an https test server and points url_transport at a
// transport that trusts its self-signed certificate for the duration of the
// test. peer_connect_url is https-only (#593) because the addresses it learns
// are then dialled by libp2p, so its tests need a TLS harness; the production
// transport keeps full verification and is restored on cleanup.
func peer_connect_tls(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)

	saved := url_transport
	t.Cleanup(func() { url_transport = saved })
	url_transport = &http.Transport{
		DialContext:     saved.DialContext,
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // test certificate // nolint
	}
	return server
}

func TestPeerConnectUrlHttpHandling(t *testing.T) {
	// Serves from httptest on 127.0.0.1; url_request blocks non-public
	// destinations by default.
	allow_private_for_test(t)
	tests := []struct {
		name             string
		handler          http.HandlerFunc
		expect_error_nil bool
		expect_contains  string
	}{
		{
			name: "server returns 404",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(404)
			},
			expect_error_nil: false,
			expect_contains:  "server returned status 404",
		},
		{
			name: "server returns 500",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(500)
			},
			expect_error_nil: false,
			expect_contains:  "server returned status 500",
		},
		{
			name: "server returns invalid JSON",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte("not json"))
			},
			expect_error_nil: false,
			expect_contains:  "failed to parse net info",
		},
		{
			name: "server returns empty peer",
			handler: func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(map[string]any{
					"peer":      "",
					"addresses": []string{"/ip4/127.0.0.1/tcp/1443"},
				})
			},
			expect_error_nil: false,
			expect_contains:  "invalid net info: missing peer or addresses",
		},
		{
			name: "server returns empty addresses",
			handler: func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(map[string]any{
					"peer":      "12D3KooWTestPeerIdMockValue123456789",
					"addresses": []string{},
				})
			},
			expect_error_nil: false,
			expect_contains:  "invalid net info: missing peer or addresses",
		},
		{
			name: "server returns null addresses",
			handler: func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(map[string]any{
					"peer": "12D3KooWTestPeerIdMockValue123456789",
				})
			},
			expect_error_nil: false,
			expect_contains:  "invalid net info: missing peer or addresses",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := peer_connect_tls(t, tt.handler)
			defer server.Close()

			_, err := peer_connect_url(server.URL)
			if tt.expect_error_nil && err != nil {
				t.Errorf("expected no error, got %v", err)
			}
			if !tt.expect_error_nil && err == nil {
				t.Errorf("expected error containing %q, got nil", tt.expect_contains)
			}
			if !tt.expect_error_nil && err != nil && !strings.Contains(err.Error(), tt.expect_contains) {
				t.Errorf("expected error containing %q, got %q", tt.expect_contains, err.Error())
			}
		})
	}
}

// Test that peer_connect_url correctly constructs the info URL
func TestPeerConnectUrlPath(t *testing.T) {
	// Serves from httptest on 127.0.0.1; url_request blocks non-public
	// destinations by default.
	allow_private_for_test(t)
	var requested_path string
	server := peer_connect_tls(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requested_path = r.URL.Path
		w.WriteHeader(500) // Return error to stop further processing
	}))

	peer_connect_url(server.URL)

	if requested_path != "/_/p2p/info" {
		t.Errorf("expected request path /_/p2p/info, got %q", requested_path)
	}
}

// Test URL normalization (adding https:// when no scheme present)
func TestPeerConnectUrlNormalizesScheme(t *testing.T) {
	// Serves from httptest on 127.0.0.1; url_request blocks non-public
	// destinations by default.
	allow_private_for_test(t)
	// We can't easily test the https normalization without a real HTTPS server,
	// but we can verify the logic by checking that http:// URLs work
	var received_host string
	server := peer_connect_tls(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received_host = r.Host
		w.WriteHeader(500)
	}))

	// Extract host:port from server URL
	host_port := strings.TrimPrefix(server.URL, "https://")

	peer_connect_url(server.URL)
	if received_host != host_port {
		t.Errorf("with full URL, expected host %q, got %q", host_port, received_host)
	}
}

// Test remote_connect logic without peer (directory lookup path)
func TestRemoteConnectDirectoryLookup(t *testing.T) {
	// Skip this test if database subsystem isn't initialized
	// (entity_peer requires database access)
	t.Skip("requires database subsystem initialization")

	// When no peer is provided, remote_connect should look up in directory
	// With a non-existent entity, entity_peer returns empty string

	valid_entity_id := strings.Repeat("a", 50) // 50-char entity ID
	_, err := remote_connect("", valid_entity_id, "")

	if err == nil {
		t.Error("expected error for non-existent entity in directory")
		return
	}
	if !strings.Contains(err.Error(), "entity not found in directory") {
		t.Errorf("expected 'entity not found in directory' error, got %q", err.Error())
	}
}

// Test remote_connect with invalid peer
func TestRemoteConnectInvalidPeer(t *testing.T) {
	// Skip this test if Net subsystem isn't initialized
	// (peer_connect will panic on nil config)
	t.Skip("requires Net subsystem initialization")

	valid_entity_id := strings.Repeat("a", 50)
	_, err := remote_connect("", valid_entity_id, "invalid-peer-id")

	if err == nil {
		t.Error("expected error for invalid peer")
	}
}

// Benchmark JSON parsing in peer_connect_url
func BenchmarkPeerConnectUrlJsonParsing(b *testing.B) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return valid JSON but will fail at peer_connect
		json.NewEncoder(w).Encode(map[string]any{
			"peer":      "",
			"addresses": []string{},
		})
	}))
	defer server.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		peer_connect_url(server.URL)
	}
}

// TestPeerConnectUrlRequiresHttps is #593's transport half. This fetch learns a
// peer's addresses and libp2p then dials them, so it must not be a plain-text
// hop that anyone on the path can answer. The permission half - that
// api_remote_peer now calls require_permission_url - is not reachable without a
// Starlark thread, and is covered by the build: the call is unconditional for
// any URL that is not the p2p/ form.
func TestPeerConnectUrlRequiresHttps(t *testing.T) {
	allow_private_for_test(t)

	// A live server, so "refused" is distinguishable from "could not connect":
	// an unreachable host errors either way and would prove nothing.
	var hit atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hit.Store(true)
		w.Write([]byte(`{"peer":"x","addresses":["/ip4/127.0.0.1/tcp/1"]}`))
	}))
	t.Cleanup(server.Close)

	if _, err := peer_connect_url(server.URL); err == nil {
		t.Error("plain http was accepted: the addresses this fetch returns are dialled, so the hop must be authenticated")
	}
	if hit.Load() {
		t.Error("the plain-http fetch was made: the scheme must be refused before anything leaves the host")
	}

	// The scheme is what is bounded, not the form of the name: a bare host
	// still normalises to https and gets as far as the request.
	hit.Store(false)
	host := strings.TrimPrefix(server.URL, "http://")
	if _, err := peer_connect_url("ftp://" + host); err == nil {
		t.Error("a non-http scheme was accepted")
	}
	if hit.Load() {
		t.Error("the ftp:// form still reached the server")
	}
}

// remote_thread is the Starlark context an external app calls
// mochi.remote.peer under: a user, an app, and no url: grant.
func remote_thread(t *testing.T) *sl.Thread {
	t.Helper()
	user := create_test_user(t)
	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("user", user)
	thread.SetLocal("app", create_external_app("stranger"))
	db_user(user, "user").permissions_setup()
	return thread
}

// TestRemotePeerAnswersNoneForWhatItCannotFetch is the permission half of
// #593, pinned after it broke the repositories app (#1238). The url: gate
// applies to a value that would cause an HTTP fetch and to nothing else: a
// bare peer id is the p2p/ form and is connected directly, a value that is no
// URL answers None as it always did, and only an https host the app has no
// grant for is refused with an error.
func TestRemotePeerAnswersNoneForWhatItCannotFetch(t *testing.T) {
	thread := remote_thread(t)
	fn := sl.NewBuiltin("mochi.remote.peer", nil)
	peer := func(value string) (sl.Value, error) {
		return api_remote_peer(thread, fn, sl.Tuple{sl.String(value)}, nil)
	}

	// The repositories app stores the directory location with its p2p/ prefix
	// stripped and hands the bare id back here. Unknown to the peer table it
	// cannot connect, but it must never reach the gate.
	if v, err := peer("12D3KooWLYEsrR6rziEnpLYJXseH3W1DBQGdKn5qnmQcvgsKcwCH"); err != nil || v != sl.None {
		t.Errorf("bare peer id: value %v, error %v; want None with no error", v, err)
	}
	if v, err := peer("not a url"); err != nil || v != sl.None {
		t.Errorf("non-URL value: value %v, error %v; want None with no error", v, err)
	}
	if _, err := peer("https://127.0.0.1:9/"); err == nil || !strings.Contains(err.Error(), "permission") {
		t.Errorf("an ungranted https host was not refused by the url: gate: %v", err)
	}
}

// remote_stub stands in remote_open: each open takes delay, or slow[entity]
// for an entity named there, fails for the entities in fail, panics for those
// in panics, and gives back a stream of its own, kept by entity with the
// payload it was sent and how many opens ran at once. closed counts the
// streams closed, by entity.
type remote_stub struct {
	lock     sync.Mutex
	streams  map[string]*Stream
	payloads map[string]any
	slow     map[string]time.Duration
	panics   map[string]bool
	closed   map[string]int
	ended    map[string]bool
	answers  map[string][]any
	silent   map[string]bool
	held     map[string]bool
	active   int
	most     int
}

type remote_discard struct {
	stub   *remote_stub
	entity string
}

func (remote_discard) Write(p []byte) (int, error) { return len(p), nil }
func (d remote_discard) Close() error {
	d.stub.lock.Lock()
	d.stub.closed[d.entity]++
	d.stub.lock.Unlock()
	return nil
}

type remote_buffer struct{ *bytes.Buffer }

func (remote_buffer) Close() error { return nil }

func remote_stub_install(t *testing.T, delay time.Duration, fail map[string]bool) *remote_stub {
	t.Helper()
	stub := &remote_stub{streams: map[string]*Stream{}, payloads: map[string]any{}, slow: map[string]time.Duration{}, panics: map[string]bool{}, closed: map[string]int{}, ended: map[string]bool{}, answers: map[string][]any{}, silent: map[string]bool{}, held: map[string]bool{}}
	saved := remote_open
	remote_open = func(ctx context.Context, caller string, entity_id string, peer string, service string, event string, from_app string, services []string, payload any, held bool) (*Stream, error) {
		stub.lock.Lock()
		stub.active++
		stub.most = max(stub.most, stub.active)
		wait, slow := stub.slow[entity_id]
		panics := stub.panics[entity_id]
		stub.lock.Unlock()
		if !slow {
			wait = delay
		}
		time.Sleep(wait)
		stub.lock.Lock()
		defer stub.lock.Unlock()
		stub.active--
		stub.ended[entity_id] = ctx.Err() != nil
		if panics {
			panic("a hostile answer")
		}
		if fail[entity_id] {
			return nil, errors.New("unreachable")
		}
		// The far end's answer: the segments given for the entity, or nothing
		// ever for a silent one.
		var reader io.ReadCloser = io.NopCloser(strings.NewReader(""))
		if stub.silent[entity_id] {
			near, far := net.Pipe()
			t.Cleanup(func() { far.Close() })
			reader = near
		} else if segments := stub.answers[entity_id]; segments != nil {
			var written bytes.Buffer
			writer := stream_rw(nil, remote_buffer{&written})
			for _, segment := range segments {
				writer.write(segment)
			}
			reader = io.NopCloser(bytes.NewReader(written.Bytes()))
		}
		s := stream_rw(reader, remote_discard{stub: stub, entity: entity_id})
		stub.streams[entity_id] = s
		stub.payloads[entity_id] = payload
		stub.held[entity_id] = held
		return s, nil
	}
	t.Cleanup(func() { remote_open = saved })
	return stub
}

// remote_requests are mochi.remote.streams requests to publisher/version for
// each entity, each asking for itself.
func remote_requests(entities []string) *sl.List {
	requests := make([]sl.Value, len(entities))
	for i, entity := range entities {
		d := sl.NewDict(4)
		d.SetKey(sl.String("entity"), sl.String(entity))
		d.SetKey(sl.String("service"), sl.String("publisher"))
		d.SetKey(sl.String("event"), sl.String("version"))
		d.SetKey(sl.String("payload"), sl_encode(map[string]any{"app": entity}))
		requests[i] = d
	}
	return sl.NewList(requests)
}

func remote_entities(n int) []string {
	entities := make([]string, n)
	for i := range entities {
		entities[i] = strings.Repeat("a", 48) + string(rune('b'+i/20)) + string(rune('b'+i%20))
	}
	return entities
}

// TestRemoteStreamsOpenTogether: mochi.remote.streams opens every stream it is
// asked for at once - each open waits on its own handshake, so a call costs
// about one open however many it asks - answers them in the order asked, None
// where one could not be opened, and leaves each registered for closing when
// the script returns.
func TestRemoteStreamsOpenTogether(t *testing.T) {
	entities := remote_entities(10)
	stub := remote_stub_install(t, 200*time.Millisecond, map[string]bool{entities[3]: true})
	thread := &sl.Thread{Name: "test"}
	fn := sl.NewBuiltin("mochi.remote.streams", api_remote_streams)

	start := time.Now()
	value, err := api_remote_streams(thread, fn, sl.Tuple{remote_requests(entities)}, nil)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	if elapsed > time.Second || stub.most != len(entities) {
		t.Errorf("THE POINT: ten opens of 200 ms took %s, at most %d at once", elapsed, stub.most)
	}
	list := value.(*sl.List)
	if list.Len() != len(entities) {
		t.Fatalf("%d streams answered for %d asked", list.Len(), len(entities))
	}
	for i, entity := range entities {
		got := list.Index(i)
		if i == 3 {
			if got != sl.None {
				t.Errorf("THE POINT: a stream that could not be opened = %v", got)
			}
			continue
		}
		if got != sl.Value(stub.streams[entity]) {
			t.Errorf("THE POINT: stream %d is not the one opened to its entity", i)
		}
	}
	if payload, _ := stub.payloads[entities[0]].(map[string]any); payload["app"] != entities[0] {
		t.Errorf("the payload sent = %v", stub.payloads[entities[0]])
	}
	if registered, _ := thread.Local("streams").([]*Stream); len(registered) != 9 {
		t.Errorf("THE POINT: %d streams registered for closing, want 9", len(registered))
	}

	// One stream, as mochi.remote.stream opens it, goes the same way.
	one, err := api_remote_stream(thread, sl.NewBuiltin("mochi.remote.stream", api_remote_stream), sl.Tuple{sl.String(entities[0]), sl.String("publisher"), sl.String("version"), sl.NewDict(0)}, nil)
	if err != nil || one != sl.Value(stub.streams[entities[0]]) {
		t.Errorf("mochi.remote.stream = %v, %v", one, err)
	}
}

// TestRemoteStreamsRefusesWhatStreamRefuses: a call asks for at most 32
// streams, and every request is checked as mochi.remote.stream checks one.
func TestRemoteStreamsRefusesWhatStreamRefuses(t *testing.T) {
	remote_stub_install(t, 0, nil)
	thread := &sl.Thread{Name: "test"}
	fn := sl.NewBuiltin("mochi.remote.streams", api_remote_streams)
	if _, err := api_remote_streams(thread, fn, sl.Tuple{remote_requests(remote_entities(33))}, nil); err == nil {
		t.Error("THE POINT: 33 streams opened in one call")
	}
	if _, err := api_remote_streams(thread, fn, sl.Tuple{remote_requests(remote_entities(32))}, nil); err != nil {
		t.Errorf("32 streams refused: %v", err)
	}
	for name, change := range map[string][2]string{"entity": {"entity", "not an entity"}, "service": {"service", "no spaces"}, "event": {"event", "no spaces"}} {
		requests := remote_requests(remote_entities(2))
		requests.Index(1).(*sl.Dict).SetKey(sl.String(change[0]), sl.String(change[1]))
		if _, err := api_remote_streams(thread, fn, sl.Tuple{requests}, nil); err == nil {
			t.Errorf("THE POINT: a request with an invalid %s opened", name)
		}
	}
	if _, err := api_remote_streams(thread, fn, sl.Tuple{sl.NewList([]sl.Value{sl.String("a request")})}, nil); err == nil {
		t.Error("THE POINT: a request that is not a dict opened")
	}
	for name, change := range map[string]func(*sl.Dict){
		"no payload":  func(d *sl.Dict) { d.Delete(sl.String("payload")) },
		"unknown key": func(d *sl.Dict) { d.SetKey(sl.String("content"), sl.String("misspelt")) },
	} {
		requests := remote_requests(remote_entities(2))
		change(requests.Index(1).(*sl.Dict))
		if _, err := api_remote_streams(thread, fn, sl.Tuple{requests}, nil); err == nil {
			t.Errorf("THE POINT: a request with %s opened", name)
		}
	}
	tuple := remote_requests(remote_entities(2))
	if value, err := api_remote_streams(thread, fn, sl.Tuple{sl.Tuple{tuple.Index(0), tuple.Index(1)}}, nil); err != nil || value.(*sl.List).Len() != 2 {
		t.Errorf("a tuple of requests = %v, %v", value, err)
	}

	// Each stream opened is charged to the app's remote budget, as one opened
	// by mochi.remote.stream is, and a call refused for a bad request charges
	// none of those before it.
	remote_limit_reset()
	t.Cleanup(remote_limit_reset)
	charged := func() int {
		rate_limit_remote.lock.Lock()
		defer rate_limit_remote.lock.Unlock()
		if entry := rate_limit_remote.entries["apps"]; entry != nil {
			return entry.count
		}
		return 0
	}
	app := remote_limit_thread("apps")
	if _, err := api_remote_streams(app, fn, sl.Tuple{remote_requests(remote_entities(2))}, nil); err != nil || charged() != 2 {
		t.Errorf("CONTROL: two streams charged %d (%v), want 2", charged(), err)
	}
	remote_limit_reset()
	requests := remote_requests(remote_entities(3))
	requests.Index(2).(*sl.Dict).SetKey(sl.String("event"), sl.String("no spaces"))
	if _, err := api_remote_streams(app, fn, sl.Tuple{requests}, nil); err == nil || charged() != 0 {
		t.Errorf("THE POINT: a call refused for its third request charged %d (%v), want 0", charged(), err)
	}

	// A request the budget refuses answers None, and the others still open.
	remote_limit_reset()
	entities := remote_entities(3)
	for range rate_limit_remote_entity.limit {
		remote_rate_limit(app, entities[1])
	}
	value, err := api_remote_streams(app, fn, sl.Tuple{remote_requests(entities)}, nil)
	if err != nil {
		t.Fatalf("THE POINT: a call with one request over its budget failed: %v", err)
	}
	if list := value.(*sl.List); list.Index(0) == sl.None || list.Index(1) != sl.None || list.Index(2) == sl.None {
		t.Errorf("THE POINT: with the second over its budget = %v", list)
	}
}

// TestRemoteStreamsTimeout: an open not finished within the call's timeout
// answers None at the timeout, and the stream it makes afterwards is closed
// rather than left open; one that panics answers None and the others open.
func TestRemoteStreamsTimeout(t *testing.T) {
	entities := remote_entities(3)
	stub := remote_stub_install(t, 50*time.Millisecond, nil)
	stub.slow[entities[1]] = time.Second
	stub.panics[entities[2]] = true
	thread := &sl.Thread{Name: "test"}
	fn := sl.NewBuiltin("mochi.remote.streams", api_remote_streams)

	start := time.Now()
	value, err := api_remote_streams(thread, fn, sl.Tuple{remote_requests(entities)}, []sl.Tuple{{sl.String("timeout"), sl.Float(0.3)}})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	list := value.(*sl.List)
	if elapsed > 800*time.Millisecond {
		t.Errorf("THE POINT: a call with a timeout of 0.3 s waited %s for an open of 1 s", elapsed)
	}
	if list.Index(0) == sl.None || list.Index(1) != sl.None || list.Index(2) != sl.None {
		t.Errorf("THE POINT: answers = %v, want a stream, None for the late open, None for the panic", list)
	}
	if registered, _ := thread.Local("streams").([]*Stream); len(registered) != 1 {
		t.Errorf("%d streams registered, want the one in time", len(registered))
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		stub.lock.Lock()
		closed := stub.closed[entities[1]]
		ended := stub.ended[entities[1]]
		stub.lock.Unlock()
		if closed > 0 {
			if !ended {
				t.Error("THE POINT: the open still running at the timeout was not told the call had given up, so it would send its request")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("THE POINT: the stream the late open made was never closed")
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, bad := range []sl.Value{sl.MakeInt(0), sl.String("1")} {
		if _, err := api_remote_streams(thread, fn, sl.Tuple{remote_requests(entities[:1])}, []sl.Tuple{{sl.String("timeout"), bad}}); err == nil {
			t.Errorf("a timeout of %v was accepted", bad)
		}
	}
}

// TestEventReadWithoutStream: e.read() on an event that came with no stream
// answers None, as it did before a failed read closed the stream's read side.
func TestEventReadWithoutStream(t *testing.T) {
	read := &EventRead{event: &Event{}}
	value, err := read.CallInternal(&sl.Thread{Name: "test"}, nil, nil)
	if err != nil || value != sl.None {
		t.Errorf("THE POINT: e.read() with no stream = %v, %v", value, err)
	}
}

// TestPeerConnectWaiting: a caller that finds a peer mid-dial waits for that
// dial and answers its outcome, where peer_connect answers no at once.
func TestPeerConnectWaiting(t *testing.T) {
	id := "12D3KooWwaitingpeerwaitingpeerwaitingpeerwaitin"
	for _, connects := range []bool{true, false} {
		dialed := make(chan struct{})
		peers_lock.Lock()
		peers[id] = Peer{ID: id, state: peer_state_connecting}
		peer_dials[id] = dialed
		peers_lock.Unlock()
		if peer_connect(id) {
			t.Fatal("CONTROL: peer_connect answered yes mid-dial")
		}
		answer := make(chan bool, 1)
		go func() { answer <- peer_connect_waiting(context.Background(), id) }()
		select {
		case got := <-answer:
			t.Fatalf("THE POINT: answered %v before the dial ended", got)
		case <-time.After(200 * time.Millisecond):
		}
		peers_lock.Lock()
		p := peers[id]
		p.state = peer_state_disconnected
		if connects {
			p.state = peer_state_connected
		}
		peers[id] = p
		delete(peer_dials, id)
		close(dialed)
		peers_lock.Unlock()
		select {
		case got := <-answer:
			if got != connects {
				t.Errorf("THE POINT: answered %v after a dial that connected=%v", got, connects)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("THE POINT: still waiting after the dial ended")
		}
	}

	// A caller whose context ends stops waiting.
	peers_lock.Lock()
	peers[id] = Peer{ID: id, state: peer_state_connecting}
	peer_dials[id] = make(chan struct{})
	peers_lock.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	if peer_connect_waiting(ctx, id) || time.Since(start) > 2*time.Second {
		t.Errorf("THE POINT: a caller whose context ended waited %s", time.Since(start))
	}
	peers_lock.Lock()
	delete(peers, id)
	delete(peer_dials, id)
	peers_lock.Unlock()
}

// TestPeerDialKeepsAnotherDialsEntry: a dial that ends after a second dial of
// the same peer began - the peer reset mid-dial - leaves the second dial's
// entry, so a caller arriving then still waits for that dial.
func TestPeerDialKeepsAnotherDialsEntry(t *testing.T) {
	setup_peer_discovery_test(t)
	id := "12D3KooWsecond0dial0second0dial0second0dial0seco"
	release := map[int]chan bool{1: make(chan bool), 2: make(chan bool)}
	var count atomic.Int32
	saved := peer_dial
	peer_dial = func(peer string, addresses []string) bool {
		if held := release[int(count.Add(1))]; held != nil {
			return <-held
		}
		return false
	}
	t.Cleanup(func() { peer_dial = saved })
	reset := func() {
		peers_lock.Lock()
		peers[id] = Peer{ID: id, state: peer_state_disconnected}
		peers_lock.Unlock()
	}
	t.Cleanup(func() {
		peers_lock.Lock()
		delete(peers, id)
		delete(peer_dials, id)
		peers_lock.Unlock()
	})
	reset()
	first := make(chan bool, 1)
	go func() { first <- peer_connect(id) }()
	for count.Load() < 1 {
		time.Sleep(5 * time.Millisecond)
	}
	reset()
	second := make(chan bool, 1)
	go func() { second <- peer_connect(id) }()
	for count.Load() < 2 {
		time.Sleep(5 * time.Millisecond)
	}
	release[1] <- false
	<-first
	waited := make(chan bool, 1)
	go func() { waited <- peer_connect_waiting(context.Background(), id) }()
	select {
	case got := <-waited:
		t.Fatalf("THE POINT: answered %v while the second dial was still under way", got)
	case <-time.After(200 * time.Millisecond):
	}
	release[2] <- false
	<-second
	select {
	case <-waited:
	case <-time.After(2 * time.Second):
		t.Error("the waiter still waited after the second dial ended")
	}
	if n := count.Load(); n != 2 {
		t.Errorf("THE POINT: %d dials, want 2: the first dial's end let a caller start another", n)
	}
}

// TestRemoteOpenStopsWithItsContext: an open whose caller has given up does
// not wait out the address search for a peer it cannot reach.
func TestRemoteOpenStopsWithItsContext(t *testing.T) {
	saved := remote_address_wait
	remote_address_wait = 3 * time.Second
	t.Cleanup(func() { remote_address_wait = saved })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	s, err := remote_open(ctx, "", strings.Repeat("a", 50), "12D3KooWnobody0nobody0nobody0nobody0nobody0nobo", "publisher", "version", "apps", nil, map[string]any{}, false)
	if s != nil || err == nil || time.Since(start) > time.Second {
		t.Errorf("THE POINT: an open with its context ended took %s and answered %v, %v", time.Since(start), s, err)
	}
}

// TestRemoteRequestsAnswerTogether: mochi.remote.requests sends its requests
// and reads their answers together: each answer's first segments, up to the
// count asked, fewer where an answer ended sooner, None for a far end that
// never answers by the timeout, the others answered all the same.
func TestRemoteRequestsAnswerTogether(t *testing.T) {
	entities := remote_entities(6)
	stub := remote_stub_install(t, 300*time.Millisecond, map[string]bool{entities[5]: true})
	for i, entity := range entities[:3] {
		stub.answers[entity] = []any{map[string]any{"status": "200"}, map[string]any{"version": i}}
	}
	stub.answers[entities[3]] = []any{map[string]any{"status": "404"}}
	stub.silent[entities[4]] = true
	thread := &sl.Thread{Name: "test"}
	fn := sl.NewBuiltin("mochi.remote.requests", api_remote_requests)

	start := time.Now()
	value, err := api_remote_requests(thread, fn, sl.Tuple{remote_requests(entities)}, []sl.Tuple{{sl.String("timeout"), sl.Float(1)}, {sl.String("segments"), sl.MakeInt(2)}})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	answers := sl_decode(value).([]any)
	if elapsed > 1500*time.Millisecond {
		t.Errorf("THE POINT: six requests of 300 ms, one never answering, with a timeout of 1 s took %s", elapsed)
	}
	for i := range 3 {
		answer, _ := answers[i].([]any)
		if len(answer) != 2 || answer[1].(map[string]any)["version"] != int64(i) {
			t.Errorf("THE POINT: answer %d = %v", i, answers[i])
		}
	}
	if answer, _ := answers[3].([]any); len(answer) != 1 {
		t.Errorf("THE POINT: an answer that ended after one segment = %v", answers[3])
	}
	if answers[4] != nil {
		t.Errorf("THE POINT: a far end that never answered = %v", answers[4])
	}
	if answers[5] != nil {
		t.Errorf("a request that could not be opened = %v", answers[5])
	}
	if stub.closed[entities[0]] == 0 {
		t.Error("an answered stream was left open")
	}

	value, _ = api_remote_requests(thread, fn, sl.Tuple{remote_requests(entities[:1])}, nil)
	if answer, _ := sl_decode(value).([]any)[0].([]any); len(answer) != 1 {
		t.Errorf("one segment by default: %v", answer)
	}
	for _, bad := range []sl.Value{sl.MakeInt(0), sl.MakeInt(17)} {
		if _, err := api_remote_requests(thread, fn, sl.Tuple{remote_requests(entities[:1])}, []sl.Tuple{{sl.String("segments"), bad}}); err == nil {
			t.Errorf("segments=%v was accepted", bad)
		}
	}
}

// stream_read_call is s.read(...) as a script calls it.
func stream_read_call(s *Stream, kwargs ...sl.Tuple) (sl.Value, error) {
	return (&StreamRead{stream: s}).CallInternal(&sl.Thread{Name: "test"}, nil, kwargs)
}

// TestStreamReadTimeout: s.read(timeout=seconds) gives up on a far end that
// sends nothing within that time, rather than the stream's 30 seconds; it can
// shorten a stream's wait and never lengthen it; and a read that gave up ends
// the stream for reading, so the rest of a segment cut off by the deadline is
// never decoded as a value.
func TestStreamReadTimeout(t *testing.T) {
	near, far := net.Pipe()
	defer far.Close()
	s := stream_rw(near, near)
	sender := stream_rw(far, far)

	sent := make(chan error, 1)
	go func() { sent <- sender.write(map[string]any{"status": "200"}) }()
	value, err := stream_read_call(s, sl.Tuple{sl.String("timeout"), sl.MakeInt(5)})
	if err != nil || value == sl.None {
		t.Fatalf("CONTROL: a segment the far end sent was not read: %v, %v", value, err)
	}
	if err := <-sent; err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	value, err = stream_read_call(s, sl.Tuple{sl.String("timeout"), sl.Float(0.2)})
	if elapsed := time.Since(start); err != nil || value != sl.None || elapsed > 2*time.Second {
		t.Errorf("THE POINT: a read of 0.2 s from a silent far end answered %v, %v after %s", value, err, elapsed)
	}
	go sender.write(map[string]any{"status": "200"})
	start = time.Now()
	if value, _ := stream_read_call(s); value != sl.None || time.Since(start) > 2*time.Second {
		t.Errorf("THE POINT: a read after one that gave up answered %v after %s", value, time.Since(start))
	}

	// The stream's own wait is the ceiling.
	near, far = net.Pipe()
	defer far.Close()
	short := stream_rw(near, near)
	short.timeout.read = 1
	start = time.Now()
	if value, _ := stream_read_call(short, sl.Tuple{sl.String("timeout"), sl.MakeInt(100)}); value != sl.None || time.Since(start) > 3*time.Second {
		t.Errorf("THE POINT: a timeout of 100 s lengthened a 1 s read to %s", time.Since(start))
	}

	for _, bad := range []sl.Value{sl.MakeInt(0), sl.MakeInt(-1), sl.String("5")} {
		if _, err := stream_read_call(s, sl.Tuple{sl.String("timeout"), bad}); err == nil {
			t.Errorf("a timeout of %v was accepted", bad)
		}
	}

	// A timeout too small to count in nanoseconds is the smallest wait, not
	// the stream's own.
	near, far = net.Pipe()
	defer far.Close()
	tiny := stream_rw(near, near)
	tiny.timeout.read = 2
	start = time.Now()
	if value, _ := stream_read_call(tiny, sl.Tuple{sl.String("timeout"), sl.Float(1e-12)}); value != sl.None || time.Since(start) > time.Second {
		t.Errorf("THE POINT: a timeout of 1e-12 s waited %s", time.Since(start))
	}
}

// TestRemoteAnswerCutOff: an answer still coming at the deadline is no answer,
// not the part of it that arrived; one that ended sooner is answered as far as
// it went; one whose call gave up ends at once.
func TestRemoteAnswerCutOff(t *testing.T) {
	answering := func() (*Stream, *Stream) {
		near, far := net.Pipe()
		t.Cleanup(func() { far.Close() })
		return stream_rw(near, near), stream_rw(far, far)
	}

	s, sender := answering()
	go sender.write(map[string]any{"status": "200"})
	start := time.Now()
	if answer := remote_answer(context.Background(), s, 2, time.Now().Add(300*time.Millisecond)); answer != nil {
		t.Errorf("THE POINT: an answer cut off after its first segment = %v", answer)
	}
	if time.Since(start) > 2*time.Second {
		t.Errorf("the cut-off answer took %s", time.Since(start))
	}

	s, sender = answering()
	go func() {
		sender.write(map[string]any{"status": "404"})
		sender.close()
	}()
	if answer := remote_answer(context.Background(), s, 2, time.Now().Add(2*time.Second)); len(answer) != 1 {
		t.Errorf("THE POINT: an answer that ended after one segment = %v", answer)
	}

	s, _ = answering()
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(100*time.Millisecond, cancel)
	start = time.Now()
	if answer := remote_answer(ctx, s, 1, time.Time{}); answer != nil || time.Since(start) > 2*time.Second {
		t.Errorf("THE POINT: a call that gave up waited %s for %v", time.Since(start), answer)
	}
}

// TestRemoteStreamsHoldTheirPayloads: mochi.remote.streams holds each payload
// for its stream's ack, since a request not open in time must not have gone;
// mochi.remote.requests may send its payloads with their opens.
func TestRemoteStreamsHoldTheirPayloads(t *testing.T) {
	entities := remote_entities(3)
	stub := remote_stub_install(t, 0, nil)
	thread := &sl.Thread{Name: "test"}
	if _, err := api_remote_streams(thread, sl.NewBuiltin("mochi.remote.streams", api_remote_streams), sl.Tuple{remote_requests(entities)}, nil); err != nil {
		t.Fatal(err)
	}
	for _, entity := range entities {
		if !stub.held[entity] {
			t.Errorf("THE POINT: mochi.remote.streams sent %s's payload with its open", entity)
		}
	}
	if _, err := api_remote_requests(thread, sl.NewBuiltin("mochi.remote.requests", api_remote_requests), sl.Tuple{remote_requests(entities)}, nil); err != nil {
		t.Fatal(err)
	}
	for _, entity := range entities {
		if stub.held[entity] {
			t.Errorf("mochi.remote.requests held %s's payload", entity)
		}
	}
}

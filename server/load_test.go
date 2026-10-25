// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Load (the federation plan, phase 38): each test meets one kind of load the
// engines face on a small server, logs what it measured and fails well past
// it (each bound some ten times the figure measured on the development
// machine, 2026-10-24). Being slow, they
// are skipped unless MOCHI_LOAD is set, as make test-load sets it; the
// application side of the same loads runs against a dev instance in
// p2p-test.py's load-fediverse flow.

package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

// load_only skips a load test unless the load suite was asked for.
func load_only(t *testing.T) {
	t.Helper()
	if os.Getenv("MOCHI_LOAD") == "" {
		t.Skip("a load test: run with make test-load")
	}
}

// TestLoadFediverseFanout: one post of a hosted account goes to 500 follower
// servers, each its own host, so the per-host limiter holds none back, every
// one reached once and the queue drained.
func TestLoadFediverseFanout(t *testing.T) {
	load_only(t)
	alice, _ := create_activitypub_test_env(t)
	const servers = 500
	var reached atomic.Int64
	var hosts sync.Map
	box := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.ReadAll(r.Body)
		hosts.Store(r.Host, true)
		reached.Add(1)
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(box.Close)
	// Every follower's host is answered by the one server.
	address := box.Listener.Addr().String()
	saved := url_transport
	url_transport = &http.Transport{DialContext: func(ctx context.Context, network string, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}}
	t.Cleanup(func() { url_transport = saved })
	db := db_user(alice, "user")
	for i := range servers {
		inbox := fmt.Sprintf("http://follower%d.load.example/inbox", i)
		db.exec("insert into followers (account, actor, inbox, shared, created) values ('f1', ?, ?, ?, 1)", fmt.Sprintf("http://follower%d.load.example/users/p", i), inbox, inbox)
	}
	activity := map[string]any{"@context": activitystreams, "id": "https://example.com/~f1/o1/activity", "type": "Create", "actor": "https://example.com/~f1",
		"object": map[string]any{"id": "https://example.com/~f1/o1", "type": "Note", "content": "<p>Spreading</p>"}}
	started := time.Now()
	object_deliver(context.Background(), alice, "f1", "https://example.com/~f1", activity, []string{activitystreams_public}, []string{"https://example.com/~f1/followers"})
	queued := time.Since(started)
	for delivery_process() > 0 {
	}
	sent := time.Since(started)
	distinct := 0
	hosts.Range(func(any, any) bool { distinct++; return true })
	t.Logf("LOAD: one post to %d follower servers: queued in %v, all sent in %v (%.0f a second)", servers, queued, sent, float64(servers)/sent.Seconds())
	if reached.Load() != servers || distinct != servers {
		t.Errorf("THE POINT: %d deliveries reached %d servers, want %d each once", reached.Load(), distinct, servers)
	}
	if left := delivery_rows(t); len(left) != 0 {
		t.Errorf("THE POINT: %d deliveries left in the queue", len(left))
	}
	if sent > 30*time.Second {
		t.Errorf("THE POINT: the fan-out took %v", sent)
	}
}

// TestLoadFediverseBurst: 500 activities from 50 people on as many servers
// arrive at one hosted account at once, as when a post spreads; each is
// verified and taken into the account's queue, which then drains in
// batches.
func TestLoadFediverseBurst(t *testing.T) {
	load_only(t)
	create_activitypub_test_env(t)
	const people, each = 50, 10
	persons := make([]*remote_person, people)
	documents := map[string][]byte{}
	for i := range persons {
		public, private, err := key_generate(key_rsa)
		if err != nil {
			t.Fatal(err)
		}
		id := fmt.Sprintf("https://person%d.load.example/users/p", i)
		persons[i] = &remote_person{id: id, key_id: id + "#main-key", inbox: id + "/inbox", public: public, private: private}
		documents[id] = []byte(`{"id":"` + id + `","type":"Person","preferredUsername":"p","inbox":"` + id + `/inbox","publicKey":{"id":"` + id + `#main-key","owner":"` + id + `","publicKeyPem":` + json_encode(public) + `}}`)
	}
	saved := resolve_fetch
	resolve_fetch = func(ctx context.Context, address string, accept string) ([]byte, error) {
		if document, found := documents[address]; found {
			return document, nil
		}
		return nil, &resolve_refusal{Status: 404}
	}
	t.Cleanup(func() { resolve_fetch = saved })
	var refused atomic.Int64
	work := make(chan int)
	var group sync.WaitGroup
	started := time.Now()
	for range 16 {
		group.Add(1)
		go func() {
			defer group.Done()
			for n := range work {
				p := persons[n%people]
				create := map[string]any{"@context": activitystreams, "id": fmt.Sprintf("%s/creates/%d", p.id, n), "type": "Create", "actor": p.id,
					"object": map[string]any{"id": fmt.Sprintf("%s/notes/%d", p.id, n), "type": "Note", "attributedTo": p.id, "content": "<p>Reply</p>", "inReplyTo": "https://example.com/~f1/o1"}}
				if w := signed_post(t, p, "/~f1/inbox", create, nil); w.Code != http.StatusAccepted {
					refused.Add(1)
				}
			}
		}()
	}
	for n := range people * each {
		work <- n
	}
	close(work)
	group.Wait()
	taken := time.Since(started)
	queued, _ := networks_database().row("select count(*) as n from inbound")
	started = time.Now()
	for inbound_process() > 0 {
	}
	drained := time.Since(started)
	t.Logf("LOAD: %d activities from %d servers at one account: taken in %v (%.0f a second), the queue drained in %v", people*each, people, taken, float64(people*each)/taken.Seconds(), drained)
	if refused.Load() != 0 || row_int(queued, "n") != people*each {
		t.Errorf("THE POINT: %d refused, %d queued, want all %d taken", refused.Load(), row_int(queued, "n"), people*each)
	}
	if left, _ := networks_database().row("select count(*) as n from inbound"); row_int(left, "n") != 0 {
		t.Errorf("THE POINT: %d activities left in the queue", row_int(left, "n"))
	}
	if taken > 10*time.Second {
		t.Errorf("THE POINT: the burst took %v", taken)
	}
}

// TestLoadMatrixJoin: joining a room of 2,000 members on another server takes
// in its whole state, every member's event checked, and the room then goes on
// as any other.
func TestLoadMatrixJoin(t *testing.T) {
	load_only(t)
	alice, log := create_matrix_test_env(t)
	r := create_matrix_remote(t, gomatrixserverlib.RoomVersionV10)
	room := r.room("Large", "public")
	const members = 2000
	for i := range members {
		user := r.user(fmt.Sprintf("member%d", i))
		r.event(room, user, spec.MRoomMember, &user, map[string]any{"membership": "join"})
	}
	started := time.Now()
	matrix_join(t, r, alice, room)
	joined := time.Since(started)
	current, _ := rooms_current(room)
	count := 0
	for _, membership := range rooms_members(current) {
		if membership == spec.Join {
			count++
		}
	}
	message := r.event(room, r.user("bob"), "m.room.message", nil, map[string]any{"msgtype": "m.text", "body": "in a large room"})
	started = time.Now()
	r.push(t, "large", message)
	taken := time.Since(started)
	t.Logf("LOAD: a room of %d members joined in %v; a message in it taken in %v", count, joined, taken)
	if count < members+2 {
		t.Errorf("THE POINT: %d members held, want %d", count, members+2)
	}
	if len(log.matching(func(e map[string]any) bool { return e["kind"] == "message" && e["body"] == "in a large room" })) == 0 {
		t.Error("THE POINT: the message in the large room was not taken")
	}
	if joined > 30*time.Second || taken > time.Second {
		t.Errorf("THE POINT: the join took %v, a message %v", joined, taken)
	}
}

// TestLoadBlueskyBudget: a person's own writes go out at the relay's rate for
// a new server, 1,500 in an hour, each a commit, and the next waits for the
// budget, as decision 30 has it.
func TestLoadBlueskyBudget(t *testing.T) {
	load_only(t)
	alice, _ := create_atproto_test_env(t)
	ctx := context.Background()
	if err := atproto_account_create(ctx, alice, "b1"); err != nil {
		t.Fatal(err)
	}
	row := account_hosted_get(alice, "b1")
	used, _ := atproto_budget_used()
	writes := int(atproto_budget_hour - used)
	started := time.Now()
	for i := range writes {
		if _, err := atproto_create(ctx, alice, row, map[string]any{"text": fmt.Sprintf("Post %d", i)}, "", false); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	took := time.Since(started)
	hour, _ := atproto_budget_used()
	t.Logf("LOAD: %d writes, each a commit, in %v (%.0f a second)", writes, took, float64(writes)/took.Seconds())
	if hour != atproto_budget_hour {
		t.Errorf("THE POINT: %d events in the hour, want the budget's %d", hour, atproto_budget_hour)
	}
	if atproto_budget_allows(false) {
		t.Error("THE POINT: the budget spent, a write may still go out")
	}
	if took > time.Minute {
		t.Errorf("THE POINT: the writes took %v", took)
	}
}

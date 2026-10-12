// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
)

// create_delivery_test_env is create_handles_test_env with the networks
// database, a hosted fediverse account f1 for alice with its keys, private
// addresses allowed (the inboxes are httptest servers), a fresh host limiter
// and a fresh resolve cache. Restores the globals it changes.
func create_delivery_test_env(t *testing.T) *User {
	t.Helper()
	alice, _ := create_handles_test_env(t)
	networks_database()
	db := db_user(alice, "user")
	db.exec("insert into accounts (id, type, label, identifier, data, created, verified) values ('f1', 'fediverse', '', 'alice@example.com', '{\"hosted\":true,\"handle\":\"alice\",\"domain\":\"example.com\"}', 1, 1)")
	db.exec("insert into accounts (id, type, label, identifier, data, created, verified) values ('b1', 'bluesky', '', 'alice.example.com', '{\"hosted\":true,\"handle\":\"alice\",\"domain\":\"example.com\"}', 1, 1)")
	if err := keypairs_create(alice, "f1", "fediverse"); err != nil {
		t.Fatal(err)
	}
	if !network_enabled("fediverse") {
		t.Fatal("fediverse is off in the test environment")
	}

	saved_private, saved_limiter, saved_cache := url_allow_private, rate_limit_host, resolve_cache
	url_allow_private = true
	rate_limit_host = &rate_limiter{entries: make(map[string]*rate_limit_entry), limit: 60, window: 60}
	resolve_cache = map[string]resolve_entry{}
	t.Cleanup(func() {
		url_allow_private, rate_limit_host, resolve_cache = saved_private, saved_limiter, saved_cache
	})
	return alice
}

// An inbox is an httptest server recording what it is sent, answering with the
// status set, and checking each request's signature against alice's key.
type inbox struct {
	server   *httptest.Server
	lock     sync.Mutex
	requests []*http.Request
	bodies   [][]byte
	errors   []error
	status   int
}

func create_inbox(t *testing.T, alice *User) *inbox {
	t.Helper()
	box := &inbox{status: 202}
	key := keypair_get(alice, "f1", key_signing)
	fetch := func(ctx context.Context, key_id string) (string, error) {
		if key_id != "https://example.com/~f1#main-key" {
			return "", errors.New("unknown key " + key_id)
		}
		return key.Public, nil
	}
	box.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_, err := signature_verify(r.Context(), r, body, fetch)
		box.lock.Lock()
		box.requests = append(box.requests, r)
		box.bodies = append(box.bodies, body)
		box.errors = append(box.errors, err)
		status := box.status
		box.lock.Unlock()
		w.WriteHeader(status)
	}))
	t.Cleanup(box.server.Close)
	return box
}

// inbox_host is the host the queue records for an inbox server: its address
// without the port.
func inbox_host(box *inbox) string {
	u, _ := url.Parse(box.server.URL)
	return u.Hostname()
}

func (box *inbox) count() int {
	box.lock.Lock()
	defer box.lock.Unlock()
	return len(box.requests)
}

func (box *inbox) set(status int) {
	box.lock.Lock()
	defer box.lock.Unlock()
	box.status = status
}

// delivery_rows reads the queue, oldest first.
func delivery_rows(t *testing.T) []map[string]any {
	t.Helper()
	rows, err := db_open("db/networks.db").rows("select id, inbox, host, attempts, next, claimed from deliveries order by created, id")
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

// host_row reads the record of a host, or nil.
func host_row(host string) map[string]any {
	row, _ := db_open("db/networks.db").row("select failures, success, failure, next from hosts where host=?", host)
	return row
}

// TestDeliverySends: a queued body reaches the inbox as a signed POST of
// ActivityPub JSON, the row is gone and the host is recorded alive.
func TestDeliverySends(t *testing.T) {
	alice := create_delivery_test_env(t)
	box := create_inbox(t, alice)
	before := delivery_sent.Load()

	if n := deliver(alice, "f1", "fediverse", []string{box.server.URL + "/inbox"}, []byte(`{"type":"Create"}`)); n != 1 {
		t.Fatalf("deliver queued %d, want 1", n)
	}
	if acted := delivery_process(); acted != 1 {
		t.Errorf("delivery_process acted on %d rows, want 1", acted)
	}
	if box.count() != 1 {
		t.Fatalf("inbox got %d requests, want 1", box.count())
	}
	r, body, err := box.requests[0], box.bodies[0], box.errors[0]
	if err != nil {
		t.Errorf("signature did not verify: %v", err)
	}
	if r.Method != "POST" || r.URL.Path != "/inbox" {
		t.Errorf("request %s %s, want POST /inbox", r.Method, r.URL.Path)
	}
	if ct := r.Header.Get("Content-Type"); ct != "application/activity+json" {
		t.Errorf("Content-Type %q", ct)
	}
	if !strings.HasPrefix(r.Header.Get("User-Agent"), "Mochi/") {
		t.Errorf("User-Agent %q", r.Header.Get("User-Agent"))
	}
	if string(body) != `{"type":"Create"}` {
		t.Errorf("body %q", body)
	}
	if rows := delivery_rows(t); len(rows) != 0 {
		t.Errorf("%d rows left after delivery", len(rows))
	}
	host := host_row(inbox_host(box))
	if host == nil || host["failures"].(int64) != 0 || host["success"].(int64) == 0 {
		t.Errorf("host record after success = %v", host)
	}
	if delivery_sent.Load() != before+1 {
		t.Error("delivery_sent did not count the send")
	}
}

// TestDeliveryBackoffAndDeadHost: a failing inbox backs its rows off and counts
// against its host; from delivery_dead_failures the host is dead, its rows are
// not claimed until the probe time, one row then probes, and the rest wait for
// the outcome; a success ends the spell and the rest follow.
func TestDeliveryBackoffAndDeadHost(t *testing.T) {
	alice := create_delivery_test_env(t)
	box := create_inbox(t, alice)
	box.set(500)
	saved := delivery_dead_failures
	delivery_dead_failures = 2
	t.Cleanup(func() { delivery_dead_failures = saved })
	db := db_open("db/networks.db")
	host := inbox_host(box)

	deliver(alice, "f1", "fediverse", []string{box.server.URL + "/a", box.server.URL + "/b"}, []byte(`{}`))
	delivery_process()
	if box.count() != 2 {
		t.Fatalf("first pass made %d requests, want 2", box.count())
	}
	for _, row := range delivery_rows(t) {
		if row["attempts"].(int64) != 1 || row["next"].(int64) <= now() || row["claimed"].(int64) != 0 {
			t.Errorf("row after failure = %v, want attempts 1, next in the future, released", row)
		}
	}
	record := host_row(host)
	if record == nil || record["failures"].(int64) != 2 || record["next"].(int64) <= now() {
		t.Fatalf("host after two failures = %v, want dead with a probe time", record)
	}

	// Due rows on a dead host are not claimed before the probe time.
	db.exec("update deliveries set next=?", now())
	delivery_process()
	if box.count() != 2 {
		t.Errorf("a dead host was contacted before its probe time: %d requests", box.count())
	}

	// At the probe time one row probes; it fails, the other is released untouched.
	db.exec("update hosts set next=? where host=?", now(), host)
	delivery_process()
	if box.count() != 3 {
		t.Errorf("the probe made %d requests in all, want 3", box.count())
	}
	attempts := []int64{}
	for _, row := range delivery_rows(t) {
		attempts = append(attempts, row["attempts"].(int64))
		if row["claimed"].(int64) != 0 {
			t.Errorf("row %v left claimed after the probe", row)
		}
	}
	if len(attempts) != 2 || attempts[0]+attempts[1] != 3 {
		t.Errorf("attempts after the probe = %v, want one row at 2 and one at 1", attempts)
	}
	if record := host_row(host); record["failures"].(int64) != 3 || record["next"].(int64) <= now() {
		t.Errorf("host after a failed probe = %v", record)
	}

	// A probe that succeeds ends the spell: the rest go in the same pass.
	box.set(202)
	db.exec("update deliveries set next=?", now())
	db.exec("update hosts set next=? where host=?", now(), host)
	delivery_process()
	if box.count() != 5 {
		t.Errorf("after the host came back %d requests in all, want 5", box.count())
	}
	if rows := delivery_rows(t); len(rows) != 0 {
		t.Errorf("%d rows left after the host came back", len(rows))
	}
	if record := host_row(host); record["failures"].(int64) != 0 || record["next"].(int64) != 0 {
		t.Errorf("host after recovery = %v", record)
	}
}

// TestDeliveryClientErrorDrops: an inbox that refuses a body with a client
// error will not take it later; the row is dropped and the host stays alive.
func TestDeliveryClientErrorDrops(t *testing.T) {
	alice := create_delivery_test_env(t)
	box := create_inbox(t, alice)
	box.set(404)
	before := delivery_failed.Load()

	deliver(alice, "f1", "fediverse", []string{box.server.URL + "/inbox"}, []byte(`{}`))
	delivery_process()
	if box.count() != 1 {
		t.Fatalf("%d requests, want 1", box.count())
	}
	if rows := delivery_rows(t); len(rows) != 0 {
		t.Errorf("row kept after a client error: %v", rows)
	}
	host := host_row(inbox_host(box))
	if host == nil || host["failures"].(int64) != 0 {
		t.Errorf("host after a client error = %v, want alive", host)
	}
	if delivery_failed.Load() != before+1 {
		t.Error("delivery_failed did not count the refusal")
	}
}

// TestDeliveryGivesUp: a row past delivery_attempts_maximum is dropped.
func TestDeliveryGivesUp(t *testing.T) {
	alice := create_delivery_test_env(t)
	box := create_inbox(t, alice)
	box.set(500)
	saved := delivery_attempts_maximum
	delivery_attempts_maximum = 1
	t.Cleanup(func() { delivery_attempts_maximum = saved })

	deliver(alice, "f1", "fediverse", []string{box.server.URL + "/inbox"}, []byte(`{}`))
	delivery_process()
	if rows := delivery_rows(t); len(rows) != 0 {
		t.Errorf("row kept past the attempt limit: %v", rows)
	}
}

// TestDeliveryHostPolicy: a blocked host is not queued, a row whose host is
// blocked by the time it is sent is dropped unsent, and a row whose network is
// off waits rather than failing; the account's own blocks count as the
// owner's do.
func TestDeliveryHostPolicy(t *testing.T) {
	alice := create_delivery_test_env(t)
	box := create_inbox(t, alice)
	db := db_open("db/networks.db")
	bare := inbox_host(box)

	block_add(db, "blocked.example")
	if n := deliver(alice, "f1", "fediverse", []string{"https://blocked.example/inbox", "https://sub.blocked.example/inbox"}, []byte(`{}`)); n != 0 {
		t.Errorf("deliver queued %d to a blocked host, want 0", n)
	}

	deliver(alice, "f1", "fediverse", []string{box.server.URL + "/inbox"}, []byte(`{}`))
	db.exec("insert into blocks (domain, created) values (?, ?)", bare, now())
	delivery_process()
	if box.count() != 0 {
		t.Errorf("a blocked host was contacted")
	}
	if rows := delivery_rows(t); len(rows) != 0 {
		t.Errorf("row to a blocked host kept: %v", rows)
	}
	db.exec("delete from blocks")

	deliver(alice, "f1", "fediverse", []string{box.server.URL + "/inbox"}, []byte(`{}`))
	block_add(db_user(alice, "user"), bare)
	delivery_process()
	if box.count() != 0 || len(delivery_rows(t)) != 0 {
		t.Errorf("the user's own block was not applied: %d requests, %d rows", box.count(), len(delivery_rows(t)))
	}
	block_remove(db_user(alice, "user"), bare)

	deliver(alice, "f1", "fediverse", []string{box.server.URL + "/inbox"}, []byte(`{}`))
	setting_set("fediverse", "false")
	delivery_process()
	if box.count() != 0 {
		t.Errorf("a delivery was made with the network off")
	}
	rows := delivery_rows(t)
	if len(rows) != 1 || rows[0]["attempts"].(int64) != 0 || rows[0]["next"].(int64) <= now() {
		t.Errorf("row with the network off = %v, want deferred with no attempt counted", rows)
	}
	setting_set("fediverse", "true")
	db.exec("update deliveries set next=?", now())
	delivery_process()
	if box.count() != 1 || len(delivery_rows(t)) != 0 {
		t.Errorf("the deferred row did not go once the network was on: %d requests, %d rows", box.count(), len(delivery_rows(t)))
	}
}

// TestDeliveryLimiter: past the host's budget for the minute a row waits for
// the next window, with no attempt counted.
func TestDeliveryLimiter(t *testing.T) {
	alice := create_delivery_test_env(t)
	box := create_inbox(t, alice)
	rate_limit_host.limit = 1

	deliver(alice, "f1", "fediverse", []string{box.server.URL + "/a", box.server.URL + "/b"}, []byte(`{}`))
	delivery_process()
	if box.count() != 1 {
		t.Errorf("%d requests under a budget of 1", box.count())
	}
	rows := delivery_rows(t)
	if len(rows) != 1 || rows[0]["attempts"].(int64) != 0 || rows[0]["next"].(int64) <= now() {
		t.Errorf("row past the budget = %v, want deferred with no attempt counted", rows)
	}
}

// TestDeliveryAccountGone: a row whose account no longer exists is dropped
// without a request.
func TestDeliveryAccountGone(t *testing.T) {
	alice := create_delivery_test_env(t)
	box := create_inbox(t, alice)

	deliver(alice, "f1", "fediverse", []string{box.server.URL + "/inbox"}, []byte(`{}`))
	db_user(alice, "user").exec("delete from accounts where id='f1'")
	delivery_process()
	if box.count() != 0 || len(delivery_rows(t)) != 0 {
		t.Errorf("a row of a removed account was sent or kept: %d requests, %d rows", box.count(), len(delivery_rows(t)))
	}
}

// TestNetworkRequestPolicy: the checked client refuses plain http outside
// development, a blocked host, and a host past its budget, each with its own
// error.
func TestNetworkRequestPolicy(t *testing.T) {
	create_delivery_test_env(t)
	db := db_open("db/networks.db")

	url_allow_private = false
	request, _ := http.NewRequest("GET", "http://plain.example/x", nil)
	var refusal *host_refusal
	if _, err := network_request(context.Background(), nil, "", request, 0); !errors.As(err, &refusal) {
		t.Errorf("plain http = %v, want a host refusal", err)
	}
	url_allow_private = true

	block_add(db, "blocked.example")
	request, _ = http.NewRequest("GET", "https://a.blocked.example/x", nil)
	if _, err := network_request(context.Background(), nil, "", request, 0); !errors.As(err, &refusal) || refusal.Off {
		t.Errorf("blocked host = %v, want a block refusal", err)
	}

	request, _ = http.NewRequest("GET", "https://off.example/x", nil)
	setting_set("fediverse", "false")
	if _, err := network_request(context.Background(), nil, "fediverse", request, 0); !errors.As(err, &refusal) || !refusal.Off {
		t.Errorf("network off = %v, want an off refusal", err)
	}
	setting_set("fediverse", "true")

	// The budget is spent by the request that fails to resolve, so the next is
	// refused before it is tried.
	rate_limit_host.limit = 1
	request, _ = http.NewRequest("GET", "https://busy.example/x", nil)
	network_request(context.Background(), nil, "", request, 0)
	request, _ = http.NewRequest("GET", "https://busy.example/x", nil)
	var limited *RateLimitError
	if _, err := network_request(context.Background(), nil, "", request, 0); !errors.As(err, &limited) {
		t.Errorf("exhausted budget = %v, want a rate limit error", err)
	}
}

// TestIdentifierHost: the host each identifier form names.
func TestIdentifierHost(t *testing.T) {
	cases := map[string]string{
		"https://lemmy.world/u/bob":        "lemmy.world",
		"@bob@Mastodon.social":             "mastodon.social",
		"bob@mastodon.social":              "mastodon.social",
		"@bob:matrix.org":                  "matrix.org",
		"@bob:matrix.org:8448":             "matrix.org",
		"xmpp:bob@example.org":             "example.org",
		"did:web:example.org":              "example.org",
		"did:web:example.org%3A3000:alice": "example.org",
		"did:plc:ewvi7nxzyoun6zhxrhs64oiz": "",
	}
	for identifier, want := range cases {
		if got := identifier_host(identifier); got != want {
			t.Errorf("identifier_host(%q) = %q, want %q", identifier, got, want)
		}
	}
}

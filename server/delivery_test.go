// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"errors"
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
)

// delivery_process sends one claim of each due host's rows, hosts in parallel,
// and waits for them: the workers' pass, made at once so a test can look at
// what it did. Answers how many rows were tried.
func delivery_process() int {
	var group sync.WaitGroup
	var tried atomic.Int64
	for _, host := range delivery_due(delivery_workers) {
		group.Add(1)
		go func(host string) {
			defer group.Done()
			tried.Add(int64(delivery_host(host)))
		}(host)
	}
	group.Wait()
	return int(tried.Load())
}

// create_delivery_test_env is create_handles_test_env with the networks
// database, a hosted fediverse account f1 for alice with its keys, private
// addresses allowed (the inboxes are httptest servers), a fresh host limiter
// and a fresh resolve cache. Restores the globals it changes.
func create_delivery_test_env(t *testing.T) *User {
	t.Helper()
	alice, _ := create_handles_test_env(t)
	db := db_user(alice, "user")
	db.exec("insert into accounts (id, type, label, identifier, data, created, verified) values ('f1', 'fediverse', '', '@alice@example.com', '{\"hosted\":true,\"handle\":\"alice\",\"domain\":\"example.com\"}', 1, 1)")
	db.exec("insert into accounts (id, type, label, identifier, data, created, verified) values ('b1', 'bluesky', '', 'alice.example.com', '{\"hosted\":true,\"handle\":\"alice\",\"domain\":\"example.com\"}', 1, 1)")
	if err := keypairs_create(alice, "f1", "fediverse"); err != nil {
		t.Fatal(err)
	}
	if !network_enabled("fediverse") {
		t.Fatal("fediverse is off in the test environment")
	}

	saved_private, saved_limiter, saved_cache, saved_handles := url_allow_private, rate_limit_host, resolve_cache, resolve_handles
	url_allow_private = true
	rate_limit_host = &rate_limiter{entries: make(map[string]*rate_limit_entry), limit: 60, window: 60}
	resolve_cache, resolve_handles = map[string]resolve_entry{}, map[string]resolve_entry{}
	t.Cleanup(func() {
		url_allow_private, rate_limit_host, resolve_cache, resolve_handles = saved_private, saved_limiter, saved_cache, saved_handles
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
	fetch := func(ctx context.Context, key_id string, fresh bool) (string, error) {
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

// TestDeliveryOwnDomain: requests to a verified domain this server serves are
// not counted against the host limiter, which spares other servers; an
// unverified one is counted.
func TestDeliveryOwnDomain(t *testing.T) {
	alice := create_delivery_test_env(t)
	box := create_inbox(t, alice)
	rate_limit_host.limit = 1
	domains := db_open("db/domains.db")
	domains.exec("create table if not exists domains (domain text primary key, verified integer not null default 0, token text not null default '', tls integer not null default 1, created integer not null, updated integer not null)")
	domains.exec("insert or replace into domains (domain, verified, created, updated) values (?, 1, 1, 1)", inbox_host(box))
	t.Cleanup(func() { domains.exec("delete from domains where domain=?", inbox_host(box)) })

	deliver(alice, "f1", "fediverse", []string{box.server.URL + "/a", box.server.URL + "/b", box.server.URL + "/c"}, []byte(`{}`))
	delivery_process()
	if box.count() != 3 || len(delivery_rows(t)) != 0 {
		t.Errorf("%d of 3 requests to the server's own domain under a budget of 1, %d rows left", box.count(), len(delivery_rows(t)))
	}

	domains.exec("update domains set verified=0 where domain=?", inbox_host(box))
	deliver(alice, "f1", "fediverse", []string{box.server.URL + "/d", box.server.URL + "/e"}, []byte(`{}`))
	delivery_process()
	if box.count() != 4 || len(delivery_rows(t)) != 1 {
		t.Errorf("an unverified domain was not limited to its budget of 1: %d requests, %d rows", box.count(), len(delivery_rows(t)))
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

// TestNetworkRequestRedirectSignature: a signed request redirected keeps its
// signature on its own host and drops it on the way to another, which would
// learn whose request it was.
func TestNetworkRequestRedirectSignature(t *testing.T) {
	create_delivery_test_env(t)
	other := map[string]string{}
	elsewhere := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		other[r.URL.Path] = r.Header.Get("Signature")
	}))
	t.Cleanup(elsewhere.Close)
	own := map[string]string{}
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		own[r.URL.Path] = r.Header.Get("Signature")
		switch r.URL.Path {
		case "/away":
			http.Redirect(w, r, elsewhere.URL+"/landed", http.StatusFound)
		case "/along":
			http.Redirect(w, r, "/landed", http.StatusFound)
		}
	}))
	t.Cleanup(origin.Close)

	for _, path := range []string{"/away", "/along"} {
		request, _ := http.NewRequest("GET", origin.URL+path, nil)
		request.Header.Set("Signature", `keyId="https://example.com/~f1#main-key",signature="x"`)
		response, err := network_request(context.Background(), nil, "", request, 5)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		response.Body.Close()
	}
	if own["/away"] == "" || own["/along"] == "" {
		t.Errorf("the signature did not reach its own host: %v", own)
	}
	if own["/landed"] == "" {
		t.Error("a redirect on the same host dropped the signature")
	}
	if found, ok := other["/landed"]; !ok || found != "" {
		t.Errorf("another host was sent the signature, or never reached: %q %v", found, ok)
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

// TestDeliveryResolvesLater: an addressee whose actor document the limiter
// would not let us fetch is queued by address rather than lost, the refusal is
// not remembered as a failure, and the delivery manager looks the actor up
// again and sends to the inbox it finds; a lookup that fails outright backs
// the row off like any other failure.
func TestDeliveryResolvesLater(t *testing.T) {
	alice := create_delivery_test_env(t)
	box := create_inbox(t, alice)
	carol := "https://remote.example/u/carol"
	document := `{"id":"` + carol + `","type":"Person","preferredUsername":"carol","inbox":"` + box.server.URL + `/inbox"}`
	saved := resolve_fetch
	t.Cleanup(func() { resolve_fetch = saved })
	answer := func(ctx context.Context, address string, accept string) ([]byte, error) {
		return nil, &RateLimitError{Retry: 30, detail: "60 requests per minute to remote.example"}
	}
	resolve_fetch = func(ctx context.Context, address string, accept string) ([]byte, error) {
		return answer(ctx, address, accept)
	}

	// Dave answers that there is no such person: he is left out, as before.
	dave := "https://remote.example/u/dave"
	resolve_fetch = func(ctx context.Context, address string, accept string) ([]byte, error) {
		if address == dave {
			return nil, &resolve_refusal{Status: 404}
		}
		return answer(ctx, address, accept)
	}

	object_deliver(context.Background(), alice, "f1", "https://example.com/~f1", map[string]any{"type": "Create", "id": "https://example.com/~f1/o9/activity"}, []string{carol, dave}, nil)
	if rows := delivery_rows(t); len(rows) != 1 {
		t.Fatalf("queued %d rows, want carol's alone", len(rows))
	}
	row, _ := db_open("db/networks.db").row("select inbox, address, host from deliveries")
	if row == nil || row["inbox"] != "" || row["address"] != carol || row["host"] != "remote.example" {
		t.Fatalf("queued row = %v, want one by carol's address", row)
	}
	resolve_lock.Lock()
	_, cached := resolve_cache[carol]
	resolve_lock.Unlock()
	if cached {
		t.Error("the limiter's refusal was remembered as a failed lookup")
	}

	answer = func(ctx context.Context, address string, accept string) ([]byte, error) {
		return []byte(document), nil
	}
	delivery_process()
	if box.count() != 1 || box.errors[0] != nil || string(box.bodies[0]) == "" {
		t.Fatalf("inbox got %d requests (%v), want the activity once, signed", box.count(), box.errors)
	}
	if rows := delivery_rows(t); len(rows) != 0 {
		t.Errorf("%d rows left after the lookup and the send", len(rows))
	}

	// A lookup that fails outright counts an attempt and waits.
	resolve_lock.Lock()
	resolve_cache = map[string]resolve_entry{}
	resolve_lock.Unlock()
	answer = func(ctx context.Context, address string, accept string) ([]byte, error) {
		return nil, &resolve_refusal{Status: 500}
	}
	deliver_unresolved(alice, "f1", "fediverse", []string{carol}, []byte(`{}`))
	delivery_process()
	rows := delivery_rows(t)
	if len(rows) != 1 || rows[0]["attempts"].(int64) != 1 || rows[0]["next"].(int64) <= now() {
		t.Errorf("row after a failed lookup = %v, want one backed off", rows)
	}
}

// TestDeliveryStopsOnceTheHostIsDead. Whether a host was dead was read once,
// before its rows, so a server that had just stopped answering had every one
// of its rows tried, 30 s each, though it was dead after the first few.
func TestDeliveryStopsOnceTheHostIsDead(t *testing.T) {
	alice := create_delivery_test_env(t)
	box := create_inbox(t, alice)
	box.set(500)
	saved := delivery_dead_failures
	delivery_dead_failures = 2
	t.Cleanup(func() { delivery_dead_failures = saved })

	deliver(alice, "f1", "fediverse", []string{box.server.URL + "/a", box.server.URL + "/b", box.server.URL + "/c"}, []byte(`{}`))
	delivery_process()
	if box.count() != 2 {
		t.Errorf("THE POINT: %d requests to a host dead after its second failure, want 2", box.count())
	}
	probe := host_row(inbox_host(box))["next"].(int64)
	if probe <= now() {
		t.Fatalf("the host has no probe time: %d", probe)
	}
	untouched := 0
	for _, row := range delivery_rows(t) {
		if row["attempts"].(int64) == 0 && row["claimed"].(int64) == 0 {
			untouched++
		}
		if row["next"].(int64) != probe {
			t.Errorf("THE POINT: a dead host's row is not held to its probe time %d, so every look for due hosts reads it: %v", probe, row)
		}
	}
	if untouched != 1 {
		t.Errorf("rows left for the probe: %d, want 1 released untouched: %v", untouched, delivery_rows(t))
	}

	// A row queued for the dead host meanwhile waits for the same probe.
	db_open("db/networks.db").exec("delete from deliveries")
	deliver(alice, "f1", "fediverse", []string{box.server.URL + "/d"}, []byte(`{}`))
	if rows := delivery_rows(t); len(rows) != 1 || rows[0]["next"].(int64) != probe {
		t.Errorf("THE POINT: a row queued for a dead host is due before its probe time %d: %v", probe, rows)
	}
}

// TestDeliveryGivenUpCountsTowardDead: a failure on a row's last attempt is a
// failure of its host as much as any other.
func TestDeliveryGivenUpCountsTowardDead(t *testing.T) {
	alice := create_delivery_test_env(t)
	box := create_inbox(t, alice)
	box.set(500)
	saved_dead, saved_attempts := delivery_dead_failures, delivery_attempts_maximum
	delivery_dead_failures, delivery_attempts_maximum = 2, 1
	t.Cleanup(func() { delivery_dead_failures, delivery_attempts_maximum = saved_dead, saved_attempts })

	deliver(alice, "f1", "fediverse", []string{box.server.URL + "/a", box.server.URL + "/b", box.server.URL + "/c"}, []byte(`{}`))
	delivery_process()
	if box.count() != 2 {
		t.Errorf("THE POINT: %d requests to a host dead after two rows were given up, want 2", box.count())
	}
}

// TestDeliveryDueReadsOnlyDueRows. The look for due hosts walked the whole
// queue by host, dead hosts' parked rows included, on every call.
func TestDeliveryDueReadsOnlyDueRows(t *testing.T) {
	create_delivery_test_env(t)
	rows, err := db_open("db/networks.db").rows("explain query plan "+delivery_due_query, now(), now(), now(), 64)
	if err != nil {
		t.Fatal(err)
	}
	var plan []string
	for _, row := range rows {
		plan = append(plan, row_string(row, "detail"))
	}
	joined := strings.Join(plan, "\n")
	if !strings.Contains(joined, "deliveries USING INDEX deliveries_next (next<?)") || strings.Contains(joined, "deliveries_host") {
		t.Errorf("THE POINT: the look for due hosts does not read by due time:\n%s", joined)
	}
}

// silent is an inbox on its own loopback address, so another host to the
// queue, that holds every request until released and then answers 500.
type silent struct {
	server   *httptest.Server
	hits     atomic.Int64
	open     atomic.Int64
	most     atomic.Int64
	hit      chan struct{}
	released chan struct{}
	once     sync.Once
}

func (s *silent) release() { s.once.Do(func() { close(s.released) }) }

func create_silent(t *testing.T) *silent {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.2:0")
	if err != nil {
		t.Skipf("no second loopback address: %v", err)
	}
	s := &silent{hit: make(chan struct{}, 100), released: make(chan struct{})}
	s.server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		s.hits.Add(1)
		open := s.open.Add(1)
		for {
			most := s.most.Load()
			if open <= most || s.most.CompareAndSwap(most, open) {
				break
			}
		}
		s.hit <- struct{}{}
		<-s.released
		s.open.Add(-1)
		w.WriteHeader(500)
	}))
	s.server.Listener = listener
	s.server.Start()
	// Run last to first: release, wait for the workers, then close.
	t.Cleanup(s.server.Close)
	t.Cleanup(func() { delivery_idle(t) })
	t.Cleanup(s.release)
	return s
}

// delivery_running_count is how many hosts have a worker.
func delivery_running_count() int {
	delivery_running_lock.Lock()
	defer delivery_running_lock.Unlock()
	return len(delivery_running)
}

// delivery_within waits up to a deadline for a condition.
func delivery_within(limit time.Duration, condition func() bool) bool {
	deadline := time.Now().Add(limit)
	for !condition() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(10 * time.Millisecond)
	}
	return true
}

// delivery_idle waits for every worker to end.
func delivery_idle(t *testing.T) {
	if !delivery_within(10*time.Second, func() bool { return delivery_live.Load() == 0 }) {
		t.Errorf("%d delivery workers still running", delivery_live.Load())
	}
}

// delivery_woken takes the manager's wake-up, answering whether there was one.
func delivery_woken() bool {
	select {
	case <-delivery_wake_ch:
		return true
	default:
		return false
	}
}

// delivery_silent_first queues rows for the silent host due before anything
// else, so it is the first host the workers take.
func delivery_silent_first(alice *User, s *silent, rows int) {
	var inboxes []string
	for i := 0; i < rows; i++ {
		inboxes = append(inboxes, s.server.URL+"/inbox")
	}
	deliver(alice, "f1", "fediverse", inboxes, []byte(`{}`))
	db_open("db/networks.db").exec("update deliveries set next=? where host='127.0.0.2'", now()-10)
}

// TestDeliveryHostsDoNotWaitForASilentOne. A batch waited for its slowest host
// before the next was claimed, so a server that stopped answering held every
// other server's deliveries for 30 s per row it had queued.
func TestDeliveryHostsDoNotWaitForASilentOne(t *testing.T) {
	alice := create_delivery_test_env(t)
	box := create_inbox(t, alice)
	quiet := create_silent(t)
	delivery_silent_first(alice, quiet, 3)
	deliver(alice, "f1", "fediverse", []string{box.server.URL + "/inbox"}, []byte(`{}`))

	go delivery_start()
	select {
	case <-quiet.hit:
	case <-time.After(5 * time.Second):
		t.Fatal("the silent host was never tried")
	}
	if !delivery_within(3*time.Second, func() bool { return box.count() == 1 }) {
		t.Fatalf("THE POINT: the live host waited for the silent one: %d requests while it held", box.count())
	}
	quiet.release()
	delivery_idle(t)
	if quiet.hits.Load() != 3 {
		t.Errorf("the silent host had %d of its 3 rows tried", quiet.hits.Load())
	}
}

// TestDeliveryWorkersAreBounded: past delivery_workers a host waits for a
// place, and the manager is woken to give it the one a finishing worker frees.
func TestDeliveryWorkersAreBounded(t *testing.T) {
	alice := create_delivery_test_env(t)
	box := create_inbox(t, alice)
	saved := delivery_workers
	delivery_workers = 2
	t.Cleanup(func() { delivery_workers = saved })
	quiet := create_silent(t)
	listener, err := net.Listen("tcp", "127.0.0.3:0")
	if err != nil {
		t.Skipf("no third loopback address: %v", err)
	}
	var other atomic.Int64
	third := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		other.Add(1)
		w.WriteHeader(202)
	}))
	third.Listener = listener
	third.Start()
	t.Cleanup(third.Close)

	delivery_silent_first(alice, quiet, 1)
	delivery_start()
	select {
	case <-quiet.hit:
	case <-time.After(5 * time.Second):
		t.Fatal("the silent host was never tried")
	}
	// One place left, two hosts due.
	deliver(alice, "f1", "fediverse", []string{box.server.URL + "/inbox", third.URL + "/inbox"}, []byte(`{}`))
	delivery_woken()
	delivery_start()
	if !delivery_within(3*time.Second, func() bool { return int64(box.count())+other.Load() >= 1 && delivery_running_count() == 1 }) {
		t.Fatal("the host given the free place was not sent to")
	}
	time.Sleep(200 * time.Millisecond)
	if sent := int64(box.count()) + other.Load(); sent != 1 {
		t.Fatalf("THE POINT: %d hosts ran in the one free place", sent)
	}
	if !delivery_woken() {
		t.Fatal("THE POINT: the place a worker freed was not offered to the host waiting for one")
	}
	delivery_start() // as the manager does when woken
	if !delivery_within(3*time.Second, func() bool { return int64(box.count())+other.Load() == 2 }) {
		t.Fatalf("the waiting host was not sent to: %d and %d requests", box.count(), other.Load())
	}
	quiet.release()
	delivery_idle(t)
}

// TestDeliveryClaimFailureWaitsForThePoll. A claim that failed while reads
// still worked ended the worker, whose own look for due hosts found the same
// host and started it again at once: thousands of failed claims a second.
func TestDeliveryClaimFailureWaitsForThePoll(t *testing.T) {
	alice := create_delivery_test_env(t)
	box := create_inbox(t, alice)
	deliver(alice, "f1", "fediverse", []string{box.server.URL + "/inbox"}, []byte(`{}`))
	db := db_open("db/networks.db")
	db.exec("create trigger refuse before update on deliveries begin select raise(fail, 'refused'); end")
	t.Cleanup(func() { db.exec("drop trigger if exists refuse") })

	if tried := delivery_host(inbox_host(box)); tried != -1 {
		t.Errorf("a failed claim answered %d, want -1", tried)
	}
	delivery_running_lock.Lock()
	delivery_waiting = true
	delivery_running_lock.Unlock()
	t.Cleanup(func() {
		delivery_running_lock.Lock()
		delivery_waiting = false
		delivery_running_lock.Unlock()
	})
	delivery_woken()
	delivery_worker_start(inbox_host(box))
	delivery_idle(t)
	if delivery_woken() {
		t.Error("THE POINT: a worker that could not claim woke the manager to look again at once")
	}
	if box.count() != 0 {
		t.Errorf("%d requests with the queue refusing claims", box.count())
	}
}

// TestDeliveryOneWorkerPerHost: a host's rows go one at a time, in turn,
// however often it is woken.
func TestDeliveryOneWorkerPerHost(t *testing.T) {
	alice := create_delivery_test_env(t)
	saved := delivery_batch
	delivery_batch = 1
	t.Cleanup(func() { delivery_batch = saved })
	quiet := create_silent(t)
	delivery_silent_first(alice, quiet, 3)

	delivery_start()
	select {
	case <-quiet.hit:
	case <-time.After(5 * time.Second):
		t.Fatal("the silent host was never tried")
	}
	delivery_start()
	delivery_start()
	time.Sleep(300 * time.Millisecond)
	if most := quiet.most.Load(); most != 1 {
		t.Errorf("THE POINT: %d of the host's rows were sent at once, want 1", most)
	}
	quiet.release()
	delivery_idle(t)
	if quiet.hits.Load() != 3 {
		t.Errorf("the host had %d of its 3 rows tried", quiet.hits.Load())
	}
}

// TestDeliveryParksDeadHostsAtStart: rows queued for a host before it was
// found dead, under a core that did not park them, wait for its probe once the
// manager starts.
func TestDeliveryParksDeadHostsAtStart(t *testing.T) {
	alice := create_delivery_test_env(t)
	box := create_inbox(t, alice)
	deliver(alice, "f1", "fediverse", []string{box.server.URL + "/a", "https://alive.example/inbox"}, []byte(`{}`))
	db := db_open("db/networks.db")
	probe := now() + 3600
	db.exec("insert into hosts (host, failures, failure, next) values (?, 5, ?, ?)", inbox_host(box), now(), probe)

	delivery_park_all()
	for _, row := range delivery_rows(t) {
		want := now()
		if row["host"] == inbox_host(box) {
			want = probe
		}
		if next := row["next"].(int64); next < want-1 || next > want {
			t.Errorf("THE POINT: row for %v due at %d, want %d", row["host"], next, want)
		}
	}
}

// TestDeliveryFullPlacesSkipTheScan: with every place taken the manager does
// not read the queue; it marks a host waiting, for the next worker to end.
func TestDeliveryFullPlacesSkipTheScan(t *testing.T) {
	alice := create_delivery_test_env(t)
	saved := delivery_workers
	delivery_workers = 1
	t.Cleanup(func() { delivery_workers = saved })
	quiet := create_silent(t)
	delivery_silent_first(alice, quiet, 1)

	delivery_start()
	select {
	case <-quiet.hit:
	case <-time.After(5 * time.Second):
		t.Fatal("the silent host was never tried")
	}
	// A row for the running host, which its worker claims before it ends.
	deliver(alice, "f1", "fediverse", []string{quiet.server.URL + "/inbox"}, []byte(`{}`))
	delivery_start()
	delivery_running_lock.Lock()
	woken, waiting := len(delivery_running["127.0.0.2"]), delivery_waiting
	delivery_running_lock.Unlock()
	if woken != 0 {
		t.Error("THE POINT: with every place taken the manager read the queue and woke the running host")
	}
	if !waiting {
		t.Error("with every place taken no host was marked waiting")
	}
	quiet.release()
	delivery_idle(t)
	if quiet.hits.Load() != 2 {
		t.Errorf("the running host had %d of its 2 rows tried", quiet.hits.Load())
	}
	delivery_running_lock.Lock()
	delivery_waiting = false
	delivery_running_lock.Unlock()
}

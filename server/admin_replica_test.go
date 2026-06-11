// Mochi server: /_/admin/replica/* handler unit tests
// Copyright Alistair Cunningham 2026

//go:build linux

package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

// setup_admin_replica_test prepares data_dir + the DBs the admin
// handlers expect: users.db (for the zero-users gate), replication.db
// (pair + joins tables), settings.db (for replica.join.* keys). Stubs
// out the emit-function variables so tests don't spawn goroutines that
// outlive cleanup and panic on missing queue.db at the production path.
// Returns a cleanup function.
func setup_admin_replica_test(t *testing.T) func() {
	repl_cleanup := setup_replication_test(t)
	setup_users_test_schema()
	settings := db_open("db/settings.db")
	settings.exec("create table if not exists settings (name text primary key, value text)")
	gin.SetMode(gin.TestMode)

	orig_emit_join := admin_replica_emit_join
	orig_emit_pair := admin_replica_emit_pair_membership
	admin_replica_emit_join = func(string, string) {}
	admin_replica_emit_pair_membership = func([]string, []string) {}

	return func() {
		admin_replica_emit_join = orig_emit_join
		admin_replica_emit_pair_membership = orig_emit_pair
		repl_cleanup()
	}
}

func admin_replica_post(t *testing.T, path string, body any) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}
	c.Request = httptest.NewRequest("POST", path, &buf)
	c.Request.Header.Set("Content-Type", "application/json")
	switch path {
	case "/_/admin/replica/join":
		admin_replica_join(c)
	case "/_/admin/replica/leave":
		admin_replica_leave(c)
	default:
		t.Fatalf("unknown path %q", path)
	}
	var resp map[string]any
	_ = json.NewDecoder(w.Body).Decode(&resp)
	return w, resp
}

func admin_replica_get_status(t *testing.T) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/_/admin/replica/status", nil)
	admin_replica_status(c)
	var resp map[string]any
	_ = json.NewDecoder(w.Body).Decode(&resp)
	return w, resp
}

// TestAdminReplicaJoinRefusesWhenUsersPresent: the zero-users rule
// rejects the request with 403 if any user exists.
func TestAdminReplicaJoinRefusesWhenUsersPresent(t *testing.T) {
	cleanup := setup_admin_replica_test(t)
	defer cleanup()

	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username) values ('u1', 'someone@example.com')")

	w, _ := admin_replica_post(t, "/_/admin/replica/join", map[string]string{"source": "peer-A"})
	if w.Code != http.StatusForbidden {
		t.Errorf("status = %d, want %d", w.Code, http.StatusForbidden)
	}
	// Pending state must not be set.
	if peer := setting_get("replica.join.peer", ""); peer != "" {
		t.Errorf("pending peer should not be set; got %q", peer)
	}
}

// TestAdminReplicaJoinRequiresSource: empty body / missing source 400s.
func TestAdminReplicaJoinRequiresSource(t *testing.T) {
	cleanup := setup_admin_replica_test(t)
	defer cleanup()

	w, _ := admin_replica_post(t, "/_/admin/replica/join", map[string]string{})
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

// TestAdminReplicaJoinSetsPendingState: a valid join writes the four
// `replica.join.*` settings rows.
func TestAdminReplicaJoinSetsPendingState(t *testing.T) {
	cleanup := setup_admin_replica_test(t)
	defer cleanup()

	w, _ := admin_replica_post(t, "/_/admin/replica/join", map[string]string{"source": "peer-A"})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if peer := setting_get("replica.join.peer", ""); peer != "peer-A" {
		t.Errorf("replica.join.peer = %q, want %q", peer, "peer-A")
	}
	if state := setting_get("replica.join.state", ""); state != "waiting" {
		t.Errorf("replica.join.state = %q, want %q", state, "waiting")
	}
}

// TestAdminReplicaJoinConflictOnDifferentSource: starting a second join
// for a different source while one is in flight returns 409.
func TestAdminReplicaJoinConflictOnDifferentSource(t *testing.T) {
	cleanup := setup_admin_replica_test(t)
	defer cleanup()

	admin_replica_post(t, "/_/admin/replica/join", map[string]string{"source": "peer-A"})

	w, _ := admin_replica_post(t, "/_/admin/replica/join", map[string]string{"source": "peer-B"})
	if w.Code != http.StatusConflict {
		t.Errorf("second join with different source = %d, want %d", w.Code, http.StatusConflict)
	}
}

// TestAdminReplicaJoinIdempotentOnSameSource: starting again with the
// same source while one is in flight is allowed (re-emits the request,
// pending state unchanged).
func TestAdminReplicaJoinIdempotentOnSameSource(t *testing.T) {
	cleanup := setup_admin_replica_test(t)
	defer cleanup()

	admin_replica_post(t, "/_/admin/replica/join", map[string]string{"source": "peer-A"})
	w, _ := admin_replica_post(t, "/_/admin/replica/join", map[string]string{"source": "peer-A"})
	if w.Code != http.StatusOK {
		t.Errorf("second join with same source = %d, want 200", w.Code)
	}
}

// TestAdminReplicaStatusIdle: no pending join, no pair members → "idle".
func TestAdminReplicaStatusIdle(t *testing.T) {
	cleanup := setup_admin_replica_test(t)
	defer cleanup()

	_, resp := admin_replica_get_status(t)
	if state, _ := resp["state"].(string); state != "idle" {
		t.Errorf("state = %q, want %q", state, "idle")
	}
}

// TestAdminReplicaStatusWaiting: after a join, status reports "waiting"
// until the source approves.
func TestAdminReplicaStatusWaiting(t *testing.T) {
	cleanup := setup_admin_replica_test(t)
	defer cleanup()

	admin_replica_post(t, "/_/admin/replica/join", map[string]string{"source": "peer-A"})

	_, resp := admin_replica_get_status(t)
	if state, _ := resp["state"].(string); state != "waiting" {
		t.Errorf("state = %q, want %q", state, "waiting")
	}
	if source, _ := resp["source"].(string); source != "peer-A" {
		t.Errorf("source = %q, want %q", source, "peer-A")
	}
}

// TestAdminReplicaStatusApprovedClearsPending: when the source lands in
// the pair table (simulating the join-approved event applying), status
// reports "approved" and the pending state self-clears.
func TestAdminReplicaStatusApprovedClearsPending(t *testing.T) {
	cleanup := setup_admin_replica_test(t)
	defer cleanup()

	admin_replica_post(t, "/_/admin/replica/join", map[string]string{"source": "peer-A"})

	// Simulate join-approved arriving: pair table gets peer-A.
	rdb := db_open("db/replication.db")
	rdb.exec("insert into pair (peer, added, role) values ('peer-A', 0, '')")

	_, resp := admin_replica_get_status(t)
	if state, _ := resp["state"].(string); state != "approved" {
		t.Errorf("state = %q, want %q", state, "approved")
	}
	// Pending state should be cleared by the status read.
	if peer := setting_get("replica.join.peer", ""); peer != "" {
		t.Errorf("pending peer should be cleared after approved; got %q", peer)
	}

	// A subsequent status read while pair has members but no pending
	// still reports "approved" (we're in a pair).
	_, resp2 := admin_replica_get_status(t)
	if state, _ := resp2["state"].(string); state != "approved" {
		t.Errorf("subsequent state = %q, want %q", state, "approved")
	}
}

// TestAdminReplicaStatusDenied: setting replica.join.state="denied"
// reports it.
func TestAdminReplicaStatusDenied(t *testing.T) {
	cleanup := setup_admin_replica_test(t)
	defer cleanup()

	admin_replica_post(t, "/_/admin/replica/join", map[string]string{"source": "peer-A"})
	setting_set("replica.join.state", "denied")
	setting_set("replica.join.reason", "operator declined")

	_, resp := admin_replica_get_status(t)
	if state, _ := resp["state"].(string); state != "denied" {
		t.Errorf("state = %q, want %q", state, "denied")
	}
	if reason, _ := resp["reason"].(string); reason != "operator declined" {
		t.Errorf("reason = %q, want %q", reason, "operator declined")
	}
}

// TestAdminReplicaLeaveClearsPair: leave wipes the pair table and the
// pending-join settings.
func TestAdminReplicaLeaveClearsPair(t *testing.T) {
	cleanup := setup_admin_replica_test(t)
	defer cleanup()

	rdb := db_open("db/replication.db")
	rdb.exec("insert into pair (peer, added, role) values ('peer-A', 0, '')")
	rdb.exec("insert into pair (peer, added, role) values ('peer-B', 0, '')")
	setting_set("replica.join.peer", "peer-X")

	w, _ := admin_replica_post(t, "/_/admin/replica/leave", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("leave status = %d, want 200", w.Code)
	}
	rows, _ := rdb.rows("select 1 from pair")
	if len(rows) != 0 {
		t.Errorf("pair should be empty after leave; got %d rows", len(rows))
	}
	if peer := setting_get("replica.join.peer", ""); peer != "" {
		t.Errorf("pending peer should be cleared after leave; got %q", peer)
	}
}

// TestReplicationJoinDeniedApplyUpdatesPendingState: when a join-denied
// event arrives for the peer we're currently pending against, the apply
// path updates the settings.db state so admin_replica_status reports it.
func TestReplicationJoinDeniedApplyUpdatesPendingState(t *testing.T) {
	cleanup := setup_admin_replica_test(t)
	defer cleanup()

	setting_set("replica.join.peer", "peer-A")
	setting_set("replica.join.state", "waiting")

	replication_join_denied_apply("peer-A", "no thanks")

	if state := setting_get("replica.join.state", ""); state != "denied" {
		t.Errorf("state after denied event = %q, want %q", state, "denied")
	}
	if reason := setting_get("replica.join.reason", ""); reason != "no thanks" {
		t.Errorf("reason after denied event = %q, want %q", reason, "no thanks")
	}
}

// TestReplicationJoinDeniedApplyIgnoresOtherPeer: a denial from a peer
// we're not currently pending against doesn't disturb the state.
func TestReplicationJoinDeniedApplyIgnoresOtherPeer(t *testing.T) {
	cleanup := setup_admin_replica_test(t)
	defer cleanup()

	setting_set("replica.join.peer", "peer-A")
	setting_set("replica.join.state", "waiting")

	replication_join_denied_apply("peer-X", "stray denial")

	if state := setting_get("replica.join.state", ""); state != "waiting" {
		t.Errorf("state after stray denial = %q, want unchanged %q", state, "waiting")
	}
}

// setup_admin_replica_peers adds the peers.db schema and an empty
// in-memory registry on top of setup_admin_replica_test, for the
// --address seeding tests. Returns a combined cleanup.
func setup_admin_replica_peers(t *testing.T) func() {
	t.Helper()
	cleanup := setup_admin_replica_test(t)

	pdb := db_open("db/peers.db")
	pdb.exec("create table if not exists peers ( id text not null, address text not null, updated integer not null, primary key ( id, address ) )")

	peers_lock.Lock()
	saved := peers
	peers = map[string]Peer{}
	peers_lock.Unlock()

	return func() {
		peers_lock.Lock()
		peers = saved
		peers_lock.Unlock()
		cleanup()
	}
}

// TestAdminReplicaJoinSeedsAddresses: operator-supplied addresses (the
// --address escape hatch) land in the peer registry before the emit,
// with or without the /p2p/ suffix.
func TestAdminReplicaJoinSeedsAddresses(t *testing.T) {
	cleanup := setup_admin_replica_peers(t)
	defer cleanup()

	source, _ := test_host(t)
	w, _ := admin_replica_post(t, "/_/admin/replica/join", map[string]any{
		"source": source,
		"addresses": []string{
			"/ip4/198.51.100.20/tcp/1443",
			"/ip4/198.51.100.20/udp/1443/quic-v1/p2p/" + source,
		},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if n := peer_addresses_count(source); n != 2 {
		t.Errorf("seeded addresses = %d, want 2", n)
	}
}

// TestAdminReplicaJoinRejectsBadAddress: an unparseable address (or one
// suffixed with a different peer) 400s before any pending state is set.
func TestAdminReplicaJoinRejectsBadAddress(t *testing.T) {
	cleanup := setup_admin_replica_peers(t)
	defer cleanup()

	source, _ := test_host(t)
	other, _ := test_host(t)

	for _, address := range []string{"junk", "/ip4/198.51.100.20/tcp/1443/p2p/" + other} {
		w, body := admin_replica_post(t, "/_/admin/replica/join", map[string]any{
			"source":    source,
			"addresses": []string{address},
		})
		if w.Code != http.StatusBadRequest {
			t.Errorf("address %q: status = %d, want 400", address, w.Code)
		}
		if code, _ := body["error"].(string); code != "address_invalid" {
			t.Errorf("address %q: error = %q, want address_invalid", address, code)
		}
		if peer := setting_get("replica.join.peer", ""); peer != "" {
			t.Errorf("address %q: pending peer should not be set; got %q", address, peer)
		}
	}
}

// TestAdminReplicaStatusReportsDelivery: while a join is pending, the
// status payload carries the delivery diagnostics mochictl renders —
// queued row, attempt count, last error, known-address count.
func TestAdminReplicaStatusReportsDelivery(t *testing.T) {
	cleanup := setup_admin_replica_peers(t)
	defer cleanup()

	source, _ := test_host(t)
	setting_set("replica.join.peer", source)
	setting_set("replica.join.state", "waiting")

	qdb := db_open("db/queue.db")
	qdb.exec("insert into queue (id, target, from_entity, to_entity, service, event, attempts, last_error, next_retry, created) values ('q1', ?, '', '', 'replication', 'join/request', 3, 'connect timeout', 0, ?)",
		source, now())

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/_/admin/replica/status", nil)
	admin_replica_status(c)

	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("bad status body: %v", err)
	}
	if state, _ := body["state"].(string); state != "waiting" {
		t.Fatalf("state = %q, want waiting", state)
	}
	delivery, _ := body["delivery"].(map[string]any)
	if delivery == nil {
		t.Fatal("delivery block missing while waiting")
	}
	if queued, _ := delivery["queued"].(bool); !queued {
		t.Error("delivery.queued = false, want true")
	}
	if attempts, _ := delivery["attempts"].(float64); attempts != 3 {
		t.Errorf("delivery.attempts = %v, want 3", delivery["attempts"])
	}
	if errtext, _ := delivery["error"].(string); errtext != "connect timeout" {
		t.Errorf("delivery.error = %q, want %q", errtext, "connect timeout")
	}
	if addresses, _ := delivery["addresses"].(float64); addresses != 0 {
		t.Errorf("delivery.addresses = %v, want 0", delivery["addresses"])
	}
}

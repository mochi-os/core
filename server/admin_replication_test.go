// Mochi server: /_/admin/replication/* handler unit tests
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

func setup_admin_replication_test(t *testing.T) func() {
	cleanup := setup_replication_test(t)
	gin.SetMode(gin.TestMode)

	orig_emit := admin_replication_emit_pair_membership
	admin_replication_emit_pair_membership = func([]string, []string) {}
	return func() {
		admin_replication_emit_pair_membership = orig_emit
		cleanup()
	}
}

func admin_replication_call(t *testing.T, method, path string, body any, handler gin.HandlerFunc) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}
	c.Request = httptest.NewRequest(method, path, &buf)
	if body != nil {
		c.Request.Header.Set("Content-Type", "application/json")
	}
	handler(c)
	var resp map[string]any
	_ = json.NewDecoder(w.Body).Decode(&resp)
	return w, resp
}

// TestAdminReplicationStatusEmpty: a server with no pair, no per-user
// hosts, no pending requests reports zero counts.
func TestAdminReplicationStatusEmpty(t *testing.T) {
	cleanup := setup_admin_replication_test(t)
	defer cleanup()

	_, resp := admin_replication_call(t, "GET", "/_/admin/replication/status", nil, admin_replication_status)
	if got, _ := resp["hosts_count"].(float64); got != 0 {
		t.Errorf("hosts_count = %v, want 0", got)
	}
	if got, _ := resp["links_pending"].(float64); got != 0 {
		t.Errorf("links_pending = %v, want 0", got)
	}
	if got, _ := resp["joins_pending"].(float64); got != 0 {
		t.Errorf("joins_pending = %v, want 0", got)
	}
}

// TestAdminReplicationStatusPopulated: counts reflect rows in each
// table.
func TestAdminReplicationStatusPopulated(t *testing.T) {
	cleanup := setup_admin_replication_test(t)
	defer cleanup()

	rdb := db_open("db/replication.db")
	rdb.exec("insert into pair (peer, added, role) values ('peer-A', 0, '')")
	rdb.exec("insert into pair (peer, added, role) values ('peer-B', 0, '')")
	rdb.exec("insert into hosts (user, peer, added, ack) values ('u1', 'peer-X', 0, 0)")
	rdb.exec("insert into hosts (user, peer, added, ack) values ('u2', 'peer-Y', 0, 0)")
	rdb.exec("insert into hosts (user, peer, added, ack) values ('u2', 'peer-Z', 0, 0)")
	rdb.exec("insert into links (user, peer, label, placeholder, received, expires) values ('u1', 'peer-K', '', 'ph-1', 0, 9999999999)")
	rdb.exec("insert into joins (peer, label, received, expires) values ('peer-J', '', 0, 9999999999)")

	_, resp := admin_replication_call(t, "GET", "/_/admin/replication/status", nil, admin_replication_status)

	pair, _ := resp["pair"].([]any)
	if len(pair) != 2 {
		t.Errorf("pair = %v, want 2 members", pair)
	}
	if got, _ := resp["hosts_count"].(float64); got != 3 {
		t.Errorf("hosts_count = %v, want 3", got)
	}
	if got, _ := resp["links_pending"].(float64); got != 1 {
		t.Errorf("links_pending = %v, want 1", got)
	}
	if got, _ := resp["joins_pending"].(float64); got != 1 {
		t.Errorf("joins_pending = %v, want 1", got)
	}
}

// TestAdminReplicationPairListsMembers: GET /pair returns the current
// members with their metadata.
func TestAdminReplicationPairListsMembers(t *testing.T) {
	cleanup := setup_admin_replication_test(t)
	defer cleanup()

	rdb := db_open("db/replication.db")
	rdb.exec("insert into pair (peer, added, role) values ('peer-A', 100, '')")
	rdb.exec("insert into pair (peer, added, role) values ('peer-B', 200, '')")

	_, resp := admin_replication_call(t, "GET", "/_/admin/replication/pair", nil, admin_replication_pair)
	members, _ := resp["members"].([]any)
	if len(members) != 2 {
		t.Fatalf("members = %v, want 2", members)
	}
}

// TestAdminReplicationOpsEmpty: a fresh server has no rows in any
// op-replication table; the endpoint returns empty maps + zero
// aggregates.
func TestAdminReplicationOpsEmpty(t *testing.T) {
	cleanup := setup_admin_replication_test(t)
	defer cleanup()

	_, resp := admin_replication_call(t, "GET", "/_/admin/replication/ops", nil, admin_replication_ops)
	if got, _ := resp["pending_total"].(float64); got != 0 {
		t.Errorf("pending_total = %v, want 0", got)
	}
	if got, _ := resp["pending_oldest_age_s"].(float64); got != 0 {
		t.Errorf("pending_oldest_age_s = %v, want 0", got)
	}
}

// TestAdminReplicationOpsAggregateView: with rows in sequence, seen,
// and pending, the no-user-arg endpoint groups by (user, peer, scope).
func TestAdminReplicationOpsAggregateView(t *testing.T) {
	cleanup := setup_admin_replication_test(t)
	defer cleanup()

	rdb := db_open("db/replication.db")
	// Local emit high-water marks
	rdb.exec("insert into sequence (user, scope, next) values ('u1', 'app', 50)")
	rdb.exec("insert into sequence (user, scope, next) values ('u2', 'app', 7)")
	// Applied ops from peers
	rdb.exec("insert into seen (peer, scope, user, sequence, applied) values ('peer-A', 'app', 'u1', 40, 100)")
	rdb.exec("insert into seen (peer, scope, user, sequence, applied) values ('peer-A', 'app', 'u1', 41, 101)")
	rdb.exec("insert into seen (peer, scope, user, sequence, applied) values ('peer-B', 'app', 'u1', 30, 102)")
	// Pending ops
	rdb.exec("insert into pending (peer, scope, user, sequence, schema, payload, received) values ('peer-C', 'app', 'u1', 99, 1, x'00', ?)", now()-30)
	rdb.exec("insert into pending (peer, scope, user, sequence, schema, payload, received) values ('peer-C', 'app', 'u2', 5, 1, x'00', ?)", now()-5)

	_, resp := admin_replication_call(t, "GET", "/_/admin/replication/ops", nil, admin_replication_ops)

	if got, _ := resp["pending_total"].(float64); got != 2 {
		t.Errorf("pending_total = %v, want 2", got)
	}
	if got, _ := resp["pending_oldest_age_s"].(float64); got < 30 {
		t.Errorf("pending_oldest_age_s = %v, want >= 30", got)
	}

	emitted, _ := resp["emitted"].(map[string]any)
	u1, _ := emitted["u1"].(map[string]any)
	if v, _ := u1["app"].(float64); v != 50 {
		t.Errorf("emitted u1/app = %v, want 50", v)
	}
}

// TestAdminReplicationOpsUserFilter: ?user=uX scopes every table to
// that one user.
func TestAdminReplicationOpsUserFilter(t *testing.T) {
	cleanup := setup_admin_replication_test(t)
	defer cleanup()

	rdb := db_open("db/replication.db")
	rdb.exec("insert into sequence (user, scope, next) values ('alice', 'app', 12)")
	rdb.exec("insert into sequence (user, scope, next) values ('bob', 'app', 99)")
	rdb.exec("insert into seen (peer, scope, user, sequence, applied) values ('peerB', 'app', 'alice', 10, 0)")
	rdb.exec("insert into seen (peer, scope, user, sequence, applied) values ('peerB', 'app', 'bob', 50, 0)")

	_, resp := admin_replication_call(t, "GET", "/_/admin/replication/ops?user=alice", nil, admin_replication_ops)

	if got, _ := resp["user"].(string); got != "alice" {
		t.Errorf("user = %q, want alice", got)
	}
	emitted, _ := resp["emitted"].(map[string]any)
	if got, _ := emitted["app"].(float64); got != 12 {
		t.Errorf("emitted/app = %v, want 12", got)
	}
	if _, has_bob := emitted["bob"]; has_bob {
		t.Error("user filter must not leak other users' emitted state")
	}
	applied, _ := resp["applied"].(map[string]any)
	peerB, _ := applied["peerB"].(map[string]any)
	if got, _ := peerB["app"].(float64); got != 10 {
		t.Errorf("applied/peerB/app = %v, want 10", got)
	}
}

// TestAdminReplicationPairRemoveRequiresPeer: empty peer 400s.
func TestAdminReplicationPairRemoveRequiresPeer(t *testing.T) {
	cleanup := setup_admin_replication_test(t)
	defer cleanup()

	w, _ := admin_replication_call(t, "POST", "/_/admin/replication/pair/remove",
		map[string]string{}, admin_replication_pair_remove)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

// TestAdminReplicationPairRemoveNotFound: removing a peer that isn't
// in the pair set 404s.
func TestAdminReplicationPairRemoveNotFound(t *testing.T) {
	cleanup := setup_admin_replication_test(t)
	defer cleanup()

	w, _ := admin_replication_call(t, "POST", "/_/admin/replication/pair/remove",
		map[string]string{"peer": "peer-Z"}, admin_replication_pair_remove)
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
}

// TestAdminReplicationPairRemoveDeletes: removing an existing peer
// deletes it from the pair table and returns the remaining set.
func TestAdminReplicationPairRemoveDeletes(t *testing.T) {
	cleanup := setup_admin_replication_test(t)
	defer cleanup()

	rdb := db_open("db/replication.db")
	rdb.exec("insert into pair (peer, added, role) values ('peer-A', 0, '')")
	rdb.exec("insert into pair (peer, added, role) values ('peer-B', 0, '')")
	rdb.exec("insert into pair (peer, added, role) values ('peer-C', 0, '')")

	emit_called := false
	admin_replication_emit_pair_membership = func(full, targets []string) {
		emit_called = true
		// `targets` is the set the announcement goes to: the kicked peer
		// (peer-B) plus the remaining members (peer-A, peer-C). The kicked
		// peer needs to learn it was removed so it can clear its pair.
		if len(targets) != 3 {
			t.Errorf("targets = %v, want 3 (kicked + 2 remaining)", targets)
		}
		// `full` includes self (filtered to p2p_id="self" + remaining).
		if len(full) != 3 {
			t.Errorf("full = %v, want 3 (self + 2 remaining)", full)
		}
	}

	w, resp := admin_replication_call(t, "POST", "/_/admin/replication/pair/remove",
		map[string]string{"peer": "peer-B"}, admin_replication_pair_remove)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if got, _ := resp["removed"].(string); got != "peer-B" {
		t.Errorf("removed = %q, want peer-B", got)
	}

	exists, _ := rdb.exists("select 1 from pair where peer='peer-B'")
	if exists {
		t.Error("peer-B should be deleted")
	}
	exists, _ = rdb.exists("select 1 from pair where peer='peer-A'")
	if !exists {
		t.Error("peer-A should still exist")
	}

	if !emit_called {
		t.Error("pair-membership-change should have been emitted")
	}
}

// TestAdminReplicationPairRemoveLastMember: removing the last member
// still announces to the kicked peer so it can clear its own pair
// table. Without this, an N=2 unpair leaves the other side believing
// the pair still exists.
func TestAdminReplicationPairRemoveLastMember(t *testing.T) {
	cleanup := setup_admin_replication_test(t)
	defer cleanup()

	rdb := db_open("db/replication.db")
	rdb.exec("insert into pair (peer, added, role) values ('peer-A', 0, '')")

	var got_full, got_targets []string
	admin_replication_emit_pair_membership = func(full, targets []string) {
		got_full = full
		got_targets = targets
	}

	w, _ := admin_replication_call(t, "POST", "/_/admin/replication/pair/remove",
		map[string]string{"peer": "peer-A"}, admin_replication_pair_remove)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	rows, _ := rdb.rows("select 1 from pair")
	if len(rows) != 0 {
		t.Errorf("pair should be empty; got %d rows", len(rows))
	}
	if len(got_targets) != 1 || got_targets[0] != "peer-A" {
		t.Errorf("targets = %v, want [peer-A] (the kicked peer)", got_targets)
	}
	if len(got_full) != 1 || got_full[0] != "self" {
		t.Errorf("full = %v, want [self] (kicked peer not in new set)", got_full)
	}
}

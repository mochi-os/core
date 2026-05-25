// Mochi server: POST /_/auth/replicate handler unit tests
// Copyright Alistair Cunningham 2026

package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// setup_auth_replicate_test prepares the DBs the handler reads / writes
// (users.db, sessions.db, settings.db) and stubs the synchronous P2P
// helpers so tests don't spawn goroutines that outlive cleanup. Returns
// a cleanup function.
func setup_auth_replicate_test(t *testing.T) func() {
	cleanup := setup_replication_test(t)
	setup_users_test_schema()
	setup_sessions_test_schema()

	settings := db_open("db/settings.db")
	settings.exec("create table if not exists settings (name text primary key, value text)")
	setting_set("signup_enabled", "true")

	gin.SetMode(gin.TestMode)

	orig_resolve := admin_replica_resolve_user
	orig_emit := admin_replica_emit_link_request

	// Default stubs: source returns "alice exists with uid=u-alice" for
	// username "alice", not-found for everything else. emit no-ops.
	admin_replica_resolve_user = func(peer, username string) (string, bool, error) {
		if username == "alice" {
			return "u-alice", true, nil
		}
		return "", false, nil
	}
	admin_replica_emit_link_request = func(sourcePeer, targetUser, label, placeholder string) {}

	return func() {
		admin_replica_resolve_user = orig_resolve
		admin_replica_emit_link_request = orig_emit
		cleanup()
	}
}

func auth_replicate_post(t *testing.T, body any) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	var buf bytes.Buffer
	_ = json.NewEncoder(&buf).Encode(body)
	c.Request = httptest.NewRequest("POST", "/_/auth/replicate", &buf)
	c.Request.Header.Set("Content-Type", "application/json")
	web_auth_replicate(c)
	var resp map[string]any
	_ = json.NewDecoder(w.Body).Decode(&resp)
	return w, resp
}

// TestAuthReplicateMissingFields: empty email / source / source_username
// each 400s.
func TestAuthReplicateMissingFields(t *testing.T) {
	cleanup := setup_auth_replicate_test(t)
	defer cleanup()

	cases := []map[string]string{
		{"email": "", "source": "peer-A", "source_username": "alice"},
		{"email": "new@example.com", "source": "", "source_username": "alice"},
		{"email": "new@example.com", "source": "peer-A", "source_username": ""},
	}
	for _, c := range cases {
		w, _ := auth_replicate_post(t, c)
		if w.Code != http.StatusBadRequest {
			t.Errorf("%v: status = %d, want 400", c, w.Code)
		}
	}
}

// TestAuthReplicateInvalidEmail: a malformed email 400s.
func TestAuthReplicateInvalidEmail(t *testing.T) {
	cleanup := setup_auth_replicate_test(t)
	defer cleanup()

	w, _ := auth_replicate_post(t, map[string]string{
		"email": "not-an-email", "source": "peer-A", "source_username": "alice",
	})
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

// TestAuthReplicateSignupDisabled: signup off → 403.
func TestAuthReplicateSignupDisabled(t *testing.T) {
	cleanup := setup_auth_replicate_test(t)
	defer cleanup()

	setting_set("signup_enabled", "false")

	w, _ := auth_replicate_post(t, map[string]string{
		"email": "new@example.com", "source": "peer-A", "source_username": "alice",
	})
	if w.Code != http.StatusForbidden {
		t.Errorf("status = %d, want 403", w.Code)
	}
}

// TestAuthReplicateLocalEmailTaken: a local username conflict 409s with
// the conflict error code.
func TestAuthReplicateLocalEmailTaken(t *testing.T) {
	cleanup := setup_auth_replicate_test(t)
	defer cleanup()

	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username) values ('u-existing', 'taken@example.com')")

	w, _ := auth_replicate_post(t, map[string]string{
		"email": "taken@example.com", "source": "peer-A", "source_username": "alice",
	})
	if w.Code != http.StatusConflict {
		t.Errorf("status = %d, want 409", w.Code)
	}
}

// TestAuthReplicateSourceUnreachable: a P2P lookup failure 502s.
func TestAuthReplicateSourceUnreachable(t *testing.T) {
	cleanup := setup_auth_replicate_test(t)
	defer cleanup()

	admin_replica_resolve_user = func(peer, username string) (string, bool, error) {
		return "", false, err_stub_unreachable
	}

	w, _ := auth_replicate_post(t, map[string]string{
		"email": "new@example.com", "source": "peer-A", "source_username": "alice",
	})
	if w.Code != http.StatusBadGateway {
		t.Errorf("status = %d, want 502", w.Code)
	}
}

// TestAuthReplicateSourceUserNotFound: a P2P lookup that returns
// exists=false 404s.
func TestAuthReplicateSourceUserNotFound(t *testing.T) {
	cleanup := setup_auth_replicate_test(t)
	defer cleanup()

	w, _ := auth_replicate_post(t, map[string]string{
		"email": "new@example.com", "source": "peer-A", "source_username": "nobody",
	})
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
}

// TestAuthReplicateAlreadyReplicated: the source uid already exists on
// this server → 409 with the already-replicated error code.
func TestAuthReplicateAlreadyReplicated(t *testing.T) {
	cleanup := setup_auth_replicate_test(t)
	defer cleanup()

	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username) values ('u-alice', 'alice@local.example.com')")

	w, _ := auth_replicate_post(t, map[string]string{
		"email": "new@example.com", "source": "peer-A", "source_username": "alice",
	})
	if w.Code != http.StatusConflict {
		t.Errorf("status = %d, want 409", w.Code)
	}
}

// TestAuthReplicateSuccess: a valid request creates the placeholder
// with status='pending-replication' and the resolved uid, returns 200.
func TestAuthReplicateSuccess(t *testing.T) {
	cleanup := setup_auth_replicate_test(t)
	defer cleanup()

	emit_calls := 0
	admin_replica_emit_link_request = func(sourcePeer, targetUser, label, placeholder string) {
		emit_calls++
		if sourcePeer != "peer-A" || targetUser != "alice" || placeholder != "u-alice" {
			t.Errorf("emit called with (%q, %q, %q, %q); want (peer-A, alice, _, u-alice)",
				sourcePeer, targetUser, label, placeholder)
		}
	}

	w, resp := auth_replicate_post(t, map[string]string{
		"email": "new@example.com", "source": "peer-A", "source_username": "alice",
	})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if s, _ := resp["status"].(string); s != "pending" {
		t.Errorf("status = %q, want %q", s, "pending")
	}
	if u, _ := resp["uid"].(string); u != "u-alice" {
		t.Errorf("uid = %q, want %q", u, "u-alice")
	}

	udb := db_open("db/users.db")
	var u User
	if !udb.scan(&u, "select uid, username, role, methods, status from users where uid=?", "u-alice") {
		t.Fatal("placeholder row should exist")
	}
	if u.Status != "pending-replication" {
		t.Errorf("status = %q, want pending-replication", u.Status)
	}
	if u.Username != "new@example.com" {
		t.Errorf("username = %q, want new@example.com", u.Username)
	}
	// First user on a fresh server — even though created via per-user
	// replication — must get the administrator role, or the server
	// ends up with no admin and no way to mint one.
	if u.Role != "administrator" {
		t.Errorf("role = %q, want administrator (first user on an empty server)", u.Role)
	}

	if emit_calls != 1 {
		t.Errorf("emit_calls = %d, want 1", emit_calls)
	}

	// Session cookie should be set (look for the Set-Cookie header).
	if got := w.Header().Get("Set-Cookie"); got == "" || !strings.Contains(got, "session=") {
		t.Errorf("Set-Cookie header should include session; got %q", got)
	}
}

// TestAuthReplicateSecondUserNotAdmin: when the server already has a
// user, a per-user replication placeholder is created as a plain user
// — the first-user-becomes-administrator rule has already been spent.
func TestAuthReplicateSecondUserNotAdmin(t *testing.T) {
	cleanup := setup_auth_replicate_test(t)
	defer cleanup()

	// An existing user already holds the server.
	db_open("db/users.db").exec(
		"insert into users (uid, username, role, status) values ('u-existing', 'boss@example.com', 'administrator', 'active')")

	w, _ := auth_replicate_post(t, map[string]string{
		"email": "new@example.com", "source": "peer-A", "source_username": "alice",
	})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}

	udb := db_open("db/users.db")
	var u User
	if !udb.scan(&u, "select uid, username, role, methods, status from users where uid=?", "u-alice") {
		t.Fatal("placeholder row should exist")
	}
	if u.Role != "user" {
		t.Errorf("role = %q, want user (server already had a user — admin rule spent)", u.Role)
	}
}

// err_stub_unreachable is a sentinel for the unreachable-source test case.
var err_stub_unreachable = &stub_error{msg: "stub: unreachable"}

type stub_error struct{ msg string }

func (e *stub_error) Error() string { return e.msg }

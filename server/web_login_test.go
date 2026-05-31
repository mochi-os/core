package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// TestWebLoginBeginMethods guards web_login_begin's response shape: `methods`
// must marshal to a JSON array, never null, even when the user requires nothing
// (the login client calls .includes() on it, so null throws "can't access
// property includes"), and it must fold in the system email floor so a
// server-wide email=Required shows up at login even when the user requires
// nothing personally.
func TestWebLoginBeginMethods(t *testing.T) {
	cleanup := create_test_users_db(t)
	defer cleanup()

	users := db_open("db/users.db")
	users.exec("create table credentials (id blob primary key, user text not null, public_key blob not null, sign_count integer not null default 0, name text not null default '', transports text not null default '', backup_eligible integer not null default 0, backup_state integer not null default 0, created integer not null)")
	users.exec("create table totp (user text primary key, secret text not null, verified integer not null default 0, created integer not null)")
	users.exec("create table oauth (id integer primary key, user text not null, provider text not null, subject text not null, email text not null default '', verified integer not null default 0, name text not null default '', created integer not null, unique(provider, subject))")
	settings := db_open("db/settings.db")
	settings.exec("create table settings (name text primary key, value text not null)")

	// A user who requires nothing (methods='') — the all-allowed login case that
	// produced a nil slice -> JSON null before the fix.
	users.exec("insert into users (uid, username, methods) values ('u1', 'a@example.com', '')")

	begin := func() map[string]any {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("POST", "/_/auth/begin", strings.NewReader(`{"email":"a@example.com"}`))
		c.Request.Header.Set("Content-Type", "application/json")
		web_login_begin(c)
		var resp map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode begin response: %v (body=%s)", err, w.Body.String())
		}
		return resp
	}

	// methods must be a JSON array, never null.
	if resp := begin(); resp["methods"] == nil {
		t.Errorf("methods is null for a no-required-methods user — the login client's .includes() would crash")
	}

	// The system email floor must surface at login even when the user requires
	// nothing.
	setting_set("auth_email", "required")
	resp := begin()
	methods, _ := resp["methods"].([]any)
	found := false
	for _, m := range methods {
		if m == "email" {
			found = true
		}
	}
	if !found {
		t.Errorf("system email=required not reflected in login methods: %v", resp["methods"])
	}
}

// TestWebAuthPartial guards the endpoint that lets the /codes page resume after
// a full-page OAuth redirect: with the oauth_partial cookie it returns the
// partial id and its remaining factors; without it, empty (so the page falls
// through to the login redirect rather than dead-ending).
func TestWebAuthPartial(t *testing.T) {
	cleanup := create_test_sessions_db(t)
	defer cleanup()

	sessions := db_open("db/sessions.db")
	sessions.exec("insert into partial (id, user, completed, remaining, expires) values ('p1', 'u1', 'oauth', 'email', ?)", now()+300)

	call := func(withCookie bool) map[string]any {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("GET", "/_/auth/partial", nil)
		if withCookie {
			c.Request.AddCookie(&http.Cookie{Name: "login_partial", Value: "p1"})
		}
		web_auth_partial(c)
		var resp map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode: %v (body=%s)", err, w.Body.String())
		}
		return resp
	}

	resp := call(true)
	if resp["partial"] != "p1" {
		t.Errorf("partial with cookie = %v, want p1", resp["partial"])
	}
	if remaining, _ := resp["remaining"].([]any); len(remaining) != 1 || remaining[0] != "email" {
		t.Errorf("remaining = %v, want [email]", resp["remaining"])
	}

	if resp := call(false); resp["partial"] != "" {
		t.Errorf("partial without cookie = %v, want empty", resp["partial"])
	}
}

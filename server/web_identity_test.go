package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

// identity_request runs GET /_/identity with a session cookie or a Bearer
// token and returns the decoded user object.
func identity_request(t *testing.T, session string, authorization string) (int, map[string]any) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/_/identity", nil)
	if session != "" {
		c.Request.AddCookie(&http.Cookie{Name: "session", Value: session})
	}
	if authorization != "" {
		c.Request.Header.Set("Authorization", authorization)
	}
	web_identity_get(c)
	var body struct {
		User map[string]any `json:"user"`
	}
	if w.Code == http.StatusOK {
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatalf("identity body: %v", err)
		}
	}
	return w.Code, body.User
}

// login_stub registers an app on the login path for the test's duration, so
// app_is_login resolves it, and drops the cached resolution either side.
func login_stub(t *testing.T) string {
	t.Helper()
	id := "login-stub"
	forget := func() {
		resolution_paths.lock.Lock()
		delete(resolution_paths.entries, resolution_key{resolution_user_key(nil), app_login_path()})
		resolution_paths.lock.Unlock()
	}
	apps_lock.Lock()
	apps[id] = &App{id: id, internal: &AppVersion{Version: "1.0", Paths: []string{app_login_path()}}}
	apps_lock.Unlock()
	forget()
	t.Cleanup(func() {
		apps_lock.Lock()
		delete(apps, id)
		apps_lock.Unlock()
		forget()
	})
	return id
}

// The account email leaves /_/identity only for the callers that may hold
// it: the session cookie and the login app's token. Any other app's token
// gets the identity and the account status, but not the address, which
// Starlark gates behind accounts/read.
func TestIdentityEmailOnlyForTheLoginApp(t *testing.T) {
	app, user, session := web_bearer_env(t)

	code, account := identity_request(t, session, "")
	if code != http.StatusOK || account["email"] != "bearer@example.com" {
		t.Fatalf("cookie caller: status %d, user %v", code, account)
	}

	token := auth_create_app_token(user.UID, session, app.id)
	code, account = identity_request(t, "", "Bearer "+token)
	if code != http.StatusOK {
		t.Fatalf("app token: status %d", code)
	}
	if _, present := account["email"]; present {
		t.Errorf("a %s token read the account email: %v", app.id, account)
	}
	if account["name"] != "Bearer Tester" || account["status"] != "active" {
		t.Errorf("a %s token lost the identity or status: %v", app.id, account)
	}

	login := login_stub(t)
	token = auth_create_app_token(user.UID, session, login)
	code, account = identity_request(t, "", "Bearer "+token)
	if code != http.StatusOK || account["email"] != "bearer@example.com" {
		t.Errorf("the login app's token: status %d, user %v", code, account)
	}
}

// The default-permission seed runs once the request is known to be allowed:
// a user refused by the app's role requirement leaves no row behind, an
// allowed user gets the seed as before.
func TestPermissionSeedWaitsForTheGate(t *testing.T) {
	app, user, session := web_bearer_env(t)
	token := auth_create_app_token(user.UID, session, app.id)
	db := db_user(user, "user")
	rows := func() int {
		return db.integer("select count(*) from apps where app=?", app.id)
	}

	// The version cache is keyed by app id, so the version web_action will
	// use is whatever it resolves for this user, not necessarily app.latest.
	av := app.active(user)
	av.Require.Role = "administrator"
	if code, body := web_bearer_status(t, app, session, "Bearer "+token); code != http.StatusForbidden {
		t.Fatalf("refused request: status %d, body %s", code, body)
	}
	if n := rows(); n != 0 {
		t.Errorf("a refused request seeded %d apps row(s)", n)
	}

	av.Require.Role = ""
	web_bearer_status(t, app, session, "Bearer "+token)
	if n := rows(); n != 1 {
		t.Errorf("an allowed request seeded %d apps row(s), want 1", n)
	}
}

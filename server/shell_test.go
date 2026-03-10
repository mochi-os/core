// Mochi server: Shell isolation unit tests
// Copyright Alistair Cunningham 2026

package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// Test web_should_serve_shell requires Sec-Fetch-Dest: document
func TestShellRequiresDocumentDest(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/feeds/", nil)
	c.Request.Header.Set("Accept", "text/html")

	// No Sec-Fetch-Dest header
	if web_should_serve_shell(c) {
		t.Error("web_should_serve_shell should return false without Sec-Fetch-Dest: document")
	}

	// Wrong Sec-Fetch-Dest
	c.Request.Header.Set("Sec-Fetch-Dest", "iframe")
	if web_should_serve_shell(c) {
		t.Error("web_should_serve_shell should return false for Sec-Fetch-Dest: iframe")
	}

	c.Request.Header.Set("Sec-Fetch-Dest", "script")
	if web_should_serve_shell(c) {
		t.Error("web_should_serve_shell should return false for Sec-Fetch-Dest: script")
	}
}

// Test web_should_serve_shell requires Accept: text/html
func TestShellRequiresHtmlAccept(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/feeds/", nil)
	c.Request.Header.Set("Sec-Fetch-Dest", "document")
	c.Request.Header.Set("Accept", "application/json")

	if web_should_serve_shell(c) {
		t.Error("web_should_serve_shell should return false for Accept: application/json")
	}
}

// Test web_should_serve_shell skips /_/ system endpoints
func TestShellSkipsSystemEndpoints(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/_/ping", nil)
	c.Request.Header.Set("Sec-Fetch-Dest", "document")
	c.Request.Header.Set("Accept", "text/html")

	if web_should_serve_shell(c) {
		t.Error("web_should_serve_shell should return false for /_/ paths")
	}
}

// Test web_should_serve_shell requires authenticated user
func TestShellRequiresAuth(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/feeds/", nil)
	c.Request.Header.Set("Sec-Fetch-Dest", "document")
	c.Request.Header.Set("Accept", "text/html")
	// No session cookie

	if web_should_serve_shell(c) {
		t.Error("web_should_serve_shell should return false without session cookie")
	}
}

// Test web_is_iframe_request
func TestIsIframeRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// Case 1: shell sets iframe.src → Sec-Fetch-Dest: iframe
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/feeds/", nil)
	c.Request.Header.Set("Sec-Fetch-Dest", "iframe")

	if !web_is_iframe_request(c) {
		t.Error("web_is_iframe_request should return true for Sec-Fetch-Dest: iframe")
	}

	// Case 2: link click inside sandboxed iframe → document + cross-site
	c.Request.Header.Set("Sec-Fetch-Dest", "document")
	c.Request.Header.Set("Sec-Fetch-Site", "cross-site")
	if !web_is_iframe_request(c) {
		t.Error("web_is_iframe_request should return true for document + cross-site (sandboxed iframe navigation)")
	}

	// Case 3: normal top-level navigation → document + same-origin
	c.Request.Header.Set("Sec-Fetch-Dest", "document")
	c.Request.Header.Set("Sec-Fetch-Site", "same-origin")
	if web_is_iframe_request(c) {
		t.Error("web_is_iframe_request should return false for document + same-origin")
	}

	// Case 4: document without Sec-Fetch-Site
	c.Request.Header.Set("Sec-Fetch-Dest", "document")
	c.Request.Header.Del("Sec-Fetch-Site")
	if web_is_iframe_request(c) {
		t.Error("web_is_iframe_request should return false for document without Sec-Fetch-Site")
	}
}

// Test security headers
func TestSecurityHeaders(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(web_security_headers)
	r.GET("/test", func(c *gin.Context) {
		c.String(200, "ok")
	})

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	xfo := w.Header().Get("X-Frame-Options")
	if xfo != "SAMEORIGIN" {
		t.Errorf("Expected X-Frame-Options SAMEORIGIN, got %q", xfo)
	}

	acao := w.Header().Get("Access-Control-Allow-Origin")
	if acao != "*" {
		t.Errorf("Expected Access-Control-Allow-Origin *, got %q", acao)
	}
}

// Test CORS preflight handling
func TestCorsPreflightHandling(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(web_security_headers)
	r.OPTIONS("/test", func(c *gin.Context) {
		c.String(200, "should not reach here")
	})

	req := httptest.NewRequest("OPTIONS", "/test", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != 204 {
		t.Errorf("Expected 204 for OPTIONS preflight, got %d", w.Code)
	}

	methods := w.Header().Get("Access-Control-Allow-Methods")
	if !strings.Contains(methods, "GET") || !strings.Contains(methods, "POST") {
		t.Errorf("Expected Allow-Methods to include GET and POST, got %q", methods)
	}

	headers := w.Header().Get("Access-Control-Allow-Headers")
	if !strings.Contains(headers, "Authorization") {
		t.Errorf("Expected Allow-Headers to include Authorization, got %q", headers)
	}
}

// Test /_/token endpoint requires authentication
func TestShellTokenRequiresAuth(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/_/token", web_shell_token)

	body := `{"app":"feeds"}`
	req := httptest.NewRequest("POST", "/_/token", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("Expected 401 without auth, got %d", w.Code)
	}
}

// Test /_/token endpoint requires app parameter
func TestShellTokenRequiresApp(t *testing.T) {
	cleanup := create_web_test_env(t)
	defer cleanup()

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/_/token", web_shell_token)

	// Empty body
	req := httptest.NewRequest("POST", "/_/token", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest && w.Code != http.StatusUnauthorized {
		t.Errorf("Expected 400 or 401 without app param, got %d", w.Code)
	}
}

// Test shell HTML template has expected placeholders
func TestShellHtmlTemplate(t *testing.T) {
	required_placeholders := []string{
		"{{IFRAME_SRC}}",
		"{{APP_ID}}",
		"{{NOTIF_TOKEN}}",
		"{{USER_NAME}}",
		"{{SHELL_JS}}",
		"{{MENU_JS}}",
		"{{MENU_CSS}}",
	}

	for _, placeholder := range required_placeholders {
		if !strings.Contains(shell_html, placeholder) {
			t.Errorf("shell_html missing required placeholder %q", placeholder)
		}
	}
}

// Test shell HTML has sandboxed iframe with correct attributes
func TestShellHtmlSandboxedIframe(t *testing.T) {
	if !strings.Contains(shell_html, `sandbox="allow-scripts allow-forms allow-popups allow-popups-to-escape-sandbox"`) {
		t.Error("shell_html should contain sandboxed iframe with allow-scripts allow-forms allow-popups allow-popups-to-escape-sandbox")
	}

	// Must NOT contain allow-same-origin (that would defeat the purpose)
	if strings.Contains(shell_html, "allow-same-origin") {
		t.Error("shell_html MUST NOT contain allow-same-origin (defeats iframe isolation)")
	}
}

// Test shell HTML does not allow top navigation
func TestShellHtmlNoTopNavigation(t *testing.T) {
	if strings.Contains(shell_html, "allow-top-navigation") {
		t.Error("shell_html MUST NOT contain allow-top-navigation (iframe could escape)")
	}
}

// Test shell JS contains required postMessage handlers
func TestShellJsPostMessageHandlers(t *testing.T) {
	required_handlers := []string{
		"'ready'",
		"'navigate'",
		"'navigate-external'",
		"'title'",
		"'storage.get'",
		"'storage.set'",
		"'storage.remove'",
		"'init'",
		"'token-refresh'",
	}

	for _, handler := range required_handlers {
		if !strings.Contains(shell_js, handler) {
			t.Errorf("shell_js missing handler for message type %s", handler)
		}
	}
}

// Test shell JS validates message source
func TestShellJsValidatesMessageSource(t *testing.T) {
	if !strings.Contains(shell_js, "event.source !== iframe.contentWindow") {
		t.Error("shell_js must validate event.source === iframe.contentWindow")
	}
}

// Test shell JS registers service worker
func TestShellJsRegistersServiceWorker(t *testing.T) {
	if !strings.Contains(shell_js, "serviceWorker.register") {
		t.Error("shell_js should register the service worker")
	}
}

// Test shell JS namespaces localStorage keys
func TestShellJsNamespacesStorage(t *testing.T) {
	if !strings.Contains(shell_js, "storagePrefix") {
		t.Error("shell_js should namespace localStorage keys with storagePrefix")
	}
	if !strings.Contains(shell_js, "'app:'") {
		t.Error("shell_js storage prefix should start with 'app:'")
	}
}

// Test escape_attr properly escapes HTML attributes
func TestEscapeAttr(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"hello", "hello"},
		{`say "hi"`, `say &quot;hi&quot;`},
		{"a & b", "a &amp; b"},
		{"<script>", "&lt;script&gt;"},
		{`"<&>"`, `&quot;&lt;&amp;&gt;&quot;`},
		{"", ""},
	}

	for _, tt := range tests {
		result := escape_attr(tt.input)
		if result != tt.expected {
			t.Errorf("escape_attr(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

// Test JWT token includes app claim
func TestJwtAppClaim(t *testing.T) {
	cleanup := create_web_test_env(t)
	defer cleanup()

	// Create a session with a secret
	db := db_open("db/sessions.db")
	db.exec("create table if not exists sessions (user integer not null, code text not null, secret text not null default '', expires integer not null, created integer not null default 0, accessed integer not null default 0, address text not null default '', agent text not null default '', primary key (user, code))")
	db.exec("create unique index if not exists sessions_code on sessions(code)")
	n := now()
	db.exec("insert into sessions (user, code, secret, expires, created, address, agent) values (1, ?, 'test-secret-1234567890123456', ?, ?, '127.0.0.1', 'test')", "test-session-code", n+86400, n)

	token := auth_create_app_token(1, "test-session-code", "feeds")
	if token == "" {
		t.Fatal("auth_create_app_token returned empty token")
	}

	// Verify the token has the app claim
	uid, app, err := jwt_verify(token)
	if err != nil {
		t.Fatalf("jwt_verify failed: %v", err)
	}
	if uid != 1 {
		t.Errorf("jwt_verify user = %d, want 1", uid)
	}
	if app != "feeds" {
		t.Errorf("jwt_verify app = %q, want %q", app, "feeds")
	}
}

// Test JWT token with different app claims
func TestJwtAppClaimDifferentApps(t *testing.T) {
	cleanup := create_web_test_env(t)
	defer cleanup()

	db := db_open("db/sessions.db")
	db.exec("create table if not exists sessions (user integer not null, code text not null, secret text not null default '', expires integer not null, created integer not null default 0, accessed integer not null default 0, address text not null default '', agent text not null default '', primary key (user, code))")
	db.exec("create unique index if not exists sessions_code on sessions(code)")
	n := now()
	db.exec("insert into sessions (user, code, secret, expires, created, address, agent) values (1, ?, 'test-secret-app-claim-diff-1234', ?, ?, '127.0.0.1', 'test')", "session-app-test", n+86400, n)

	apps_to_test := []string{"feeds", "wikis", "notifications", "chat", ""}
	for _, appName := range apps_to_test {
		token := auth_create_app_token(1, "session-app-test", appName)
		if token == "" {
			t.Fatalf("auth_create_app_token returned empty for app %q", appName)
		}

		_, app, err := jwt_verify(token)
		if err != nil {
			t.Fatalf("jwt_verify failed for app %q: %v", appName, err)
		}
		if app != appName {
			t.Errorf("jwt_verify app = %q, want %q", app, appName)
		}
	}
}

// Test /_/token endpoint JSON response format
func TestShellTokenResponseFormat(t *testing.T) {
	cleanup := create_web_test_env(t)
	defer cleanup()

	// Fix table schemas to match production (create_web_test_env uses simplified schemas)
	db_users := db_open("db/users.db")
	db_users.exec("alter table users add column methods text not null default ''")
	db_users.exec("alter table users add column status text not null default 'active'")
	// Recreate entities table without created/updated (Entity struct doesn't have those fields)
	db_users.exec("drop table entities")
	db_users.exec("create table entities (id text not null primary key, private text not null default '', fingerprint text not null, user integer, parent text not null default '', class text not null, name text not null, privacy text not null default 'public', data text not null default '', published integer not null default 0)")
	n := now()
	db_users.exec("insert into users (id, username, role, created, updated) values (1, 'test@example.com', 'user', ?, ?)", n, n)
	db_users.exec("insert into entities (id, fingerprint, user, class, name, privacy) values ('identity1', 'abcde1234', 1, 'person', 'Test User', 'private')")

	db_sessions := db_open("db/sessions.db")
	db_sessions.exec("create table if not exists sessions (user integer not null, code text not null, secret text not null default '', expires integer not null, created integer not null default 0, accessed integer not null default 0, address text not null default '', agent text not null default '', primary key (user, code))")
	db_sessions.exec("create unique index if not exists sessions_code on sessions(code)")
	db_sessions.exec("insert into sessions (user, code, secret, expires, created, address, agent) values (1, 'token-test-session', 'secret-for-token-test-12345678', ?, ?, '127.0.0.1', 'test')", n+86400, n)

	// Set up the app with path binding
	apps_lock.Lock()
	apps["feeds"] = &App{id: "feeds"}
	apps_lock.Unlock()
	apps_path_set("feeds", "feeds")
	defer func() {
		apps_lock.Lock()
		delete(apps, "feeds")
		apps_lock.Unlock()
	}()

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/_/token", web_shell_token)

	body := `{"app":"feeds"}`
	req := httptest.NewRequest("POST", "/_/token", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(&http.Cookie{Name: "session", Value: "token-test-session"})
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Failed to parse response JSON: %v", err)
	}

	token, ok := resp["token"].(string)
	if !ok || token == "" {
		t.Error("Response should contain non-empty 'token' field")
	}

	// Verify the returned token is valid
	uid, app, err := jwt_verify(token)
	if err != nil {
		t.Fatalf("Returned token failed verification: %v", err)
	}
	if uid != 1 {
		t.Errorf("Token user = %d, want 1", uid)
	}
	if app != "feeds" {
		t.Errorf("Token app = %q, want 'feeds'", app)
	}
}

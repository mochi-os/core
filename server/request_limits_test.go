// Mochi server: request-handling and upload bounds
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// The limits an app action inherits from the request layer: the write deadline
// on a served file, which methods a URL credential may carry, how much of an
// upload reaches memory, how much reaches disk, and which entity the action is
// pointed at.

package main

import (
	"bytes"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	jwt "github.com/golang-jwt/jwt/v5"
	sl "go.starlark.net/starlark"
)

// deadline_recorder is a ResponseWriter that accepts a write deadline, standing
// in for the real *http.response. httptest's recorder does not, and without one
// every SetWriteDeadline answers ErrNotSupported whether the chain is whole or
// broken - which is the difference this test has to measure.
type deadline_recorder struct {
	*httptest.ResponseRecorder
	deadline time.Time
}

func (d *deadline_recorder) SetWriteDeadline(t time.Time) error {
	d.deadline = t
	return nil
}

// TestWriteDeadlineReachesThroughResponseWrappers covers the deadline
// starlark_serving_set sets before serving a file. Both wrappers embed the
// gin.ResponseWriter INTERFACE, which does not declare Unwrap, so the
// ResponseController's walk stopped at the wrapper and the download ran with no
// deadline at all - a slow reader held a Starlark slot for as long as it liked.
func TestWriteDeadlineReachesThroughResponseWrappers(t *testing.T) {
	gin.SetMode(gin.TestMode)
	when := time.Now().Add(time.Minute)

	cases := []struct {
		name string
		wrap func(gin.ResponseWriter) http.ResponseWriter
	}{
		{"bare", func(w gin.ResponseWriter) http.ResponseWriter { return w }},
		{"compression", func(w gin.ResponseWriter) http.ResponseWriter {
			return &compress_writer{ResponseWriter: w}
		}},
		{"resource guard", func(w gin.ResponseWriter) http.ResponseWriter {
			return &resource_writer{ResponseWriter: w}
		}},
		// Both are installed as middleware, so a served file is normally
		// behind the pair. A fix to one alone leaves this case broken.
		{"both", func(w gin.ResponseWriter) http.ResponseWriter {
			return &resource_writer{ResponseWriter: &compress_writer{ResponseWriter: w}}
		}},
	}

	for _, tt := range cases {
		base := &deadline_recorder{ResponseRecorder: httptest.NewRecorder()}
		c, _ := gin.CreateTestContext(base)
		writer := tt.wrap(c.Writer)

		if err := http.NewResponseController(writer).SetWriteDeadline(when); err != nil {
			t.Errorf("%s: SetWriteDeadline returned %v; want nil - the Unwrap chain does not reach the connection, so the served file has no deadline", tt.name, err)
			continue
		}
		if !base.deadline.Equal(when) {
			t.Errorf("%s: deadline on the connection = %v, want %v", tt.name, base.deadline, when)
		}
	}
}

// query_jwt_environment builds a user, session and app for a request
// authenticated by a JWT in the query string, and returns that token.
func query_jwt_environment(t *testing.T) (*App, string, string) {
	t.Helper()
	app, _, session := web_bearer_env(t)

	token, err := jwt_app_token("bearer-user", app.id, session, "bearer-test-secret-0123456789")
	if err != nil {
		t.Fatalf("signing the query token: %v", err)
	}
	return app, session, token
}

// TestQueryTokenIsReadOnly covers the URL credential. A JWT in the query string
// lands in history, bookmarks, "copy image address" and any pasted link, and it
// lives a year; unlike the API-token branch it binds only to the app, so a POST
// carrying one reached every mutating action the app has.
func TestQueryTokenIsReadOnly(t *testing.T) {
	app, session, token := query_jwt_environment(t)

	// GET is what the credential exists for - sandboxed iframes fetching
	// images and RSS readers. It must still work, or the fix has broken the
	// feature instead of bounding it.
	if status, body := query_token_status(t, app, session, token, "GET"); status == 403 && strings.Contains(body, "app_token_required") {
		t.Errorf("GET with a query token got %d %s; want the token accepted - this credential exists for resource URLs", status, body)
	}

	for _, method := range []string{"POST", "PUT", "PATCH", "DELETE"} {
		status, body := query_token_status(t, app, session, token, method)
		if status != 403 || !strings.Contains(body, "app_token_required") {
			t.Errorf("%s with a query token got %d %s; want 403 app_token_required - a URL credential must not authorise a state-changing method",
				method, status, body)
		}
	}
}

// query_token_status runs one request whose only app credential is ?token=.
// The session cookie is present because that is the real shape: a signed-in
// browser following a link, whose cookie identifies the user but does not
// satisfy the per-app gate.
func query_token_status(t *testing.T, app *App, session string, token string, method string) (int, string) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(method, "/bearertest/-/private?token="+token, nil)
	c.Request.Header.Set("Accept", "application/json")
	c.Request.AddCookie(&http.Cookie{Name: "session", Value: session})
	web_action(c, app, "-/private", nil, routing_class)
	return w.Code, w.Body.String()
}

// TestRoutedEntityWinsOverBodyKey covers the input a.input(class) answers. A
// body key named after the routed entity's class repointed it, so a.input
// disagreed with a.entity, a.owner and the database opened for the URL: an app
// authorising on one and querying the other operates on two entities under one
// set of checks.
func TestRoutedEntityWinsOverBodyKey(t *testing.T) {
	_, _, session := web_bearer_env(t)

	routed := test_entity_id('r')
	other := test_entity_id('o')

	// A fresh app id, not the bearer fixture's: the version-resolution cache
	// is keyed on (user, app id), so reusing it hands back whatever version an
	// earlier test in this package cached.
	var seen string
	version := &AppVersion{
		Actions: map[string]AppAction{
			":wiki/-/private": {
				Function: "action_private",
				internal_function: func(a *Action) {
					seen = a.inputs["wiki"]
				},
			},
		},
	}
	app := &App{id: "entitytest", latest: version, versions: map[string]*AppVersion{"1.0": version}}
	version.app = app

	entity := &Entity{ID: routed, Class: "wiki", Fingerprint: fingerprint(routed)}

	token, err := jwt_app_token("bearer-user", app.id, session, "bearer-test-secret-0123456789")
	if err != nil {
		t.Fatalf("signing the token: %v", err)
	}

	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	body := `{"wiki":"` + other + `"}`
	c.Request = httptest.NewRequest("POST", "/entitytest/-/private", strings.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Request.Header.Set("Accept", "application/json")
	c.Request.Header.Set("Authorization", "Bearer "+token)
	c.Request.AddCookie(&http.Cookie{Name: "session", Value: session})

	web_action(c, app, "-/private", entity, routing_path)

	if seen == "" {
		t.Fatalf("the action never ran: %d %s", w.Code, w.Body.String())
	}
	if seen != routed {
		t.Errorf("a.input(\"wiki\") = %q, want the routed entity %q - a body key repointed the action at %q", seen, routed, other)
	}
}

// TestFileRefusesPartAboveMaximum covers a.file's ceiling. Nothing but the
// caller's storage quota bounded it, so one request could hand a multi-gigabyte
// part to io.ReadAll; refusing answers None, matching mochi.file.read.
func TestFileRefusesPartAboveMaximum(t *testing.T) {
	action := multipart_action(t, "upload", make([]byte, 4096))

	cases := []struct {
		name    string
		maximum sl.Value
		want    bool // want the bytes
	}{
		{"under the maximum", sl.MakeInt64(8192), true},
		{"over the maximum", sl.MakeInt64(1024), false},
		// The part is exactly at the ceiling, which is not over it.
		{"exactly at the maximum", sl.MakeInt64(4096), true},
		// 0 disables the ceiling for a caller that has its own bound.
		{"disabled", sl.MakeInt64(0), true},
	}

	for _, tt := range cases {
		value, err := action.sl_file(&sl.Thread{Name: "test"}, sl.NewBuiltin("file", nil),
			sl.Tuple{sl.String("upload")}, []sl.Tuple{{sl.String("maximum"), tt.maximum}})
		if err != nil {
			t.Fatalf("%s: a.file returned %v", tt.name, err)
		}
		_, got := value.(*sl.Dict)
		if got != tt.want {
			t.Errorf("%s: a.file returned %v; want data=%v - a part over the ceiling must answer None rather than allocate",
				tt.name, value.Type(), tt.want)
		}
	}
}

// TestFileDefaultMaximumRefusesOversizePart pins the default, which is what an
// app that passes no maximum actually gets.
func TestFileDefaultMaximumRefusesOversizePart(t *testing.T) {
	action := multipart_action(t, "upload", make([]byte, 1024))
	// A part cannot be built at 32MB in a test cheaply, so shrink the
	// declared size instead: sl_file reads FileHeader.Size, which is what a
	// real oversize part presents.
	form, err := action.web.MultipartForm()
	if err != nil {
		t.Fatalf("parsing the form: %v", err)
	}
	form.File["upload"][0].Size = file_inline_maximum + 1

	value, err := action.sl_file(&sl.Thread{Name: "test"}, sl.NewBuiltin("file", nil),
		sl.Tuple{sl.String("upload")}, nil)
	if err != nil {
		t.Fatalf("a.file returned %v", err)
	}
	if value != sl.None {
		t.Errorf("a.file on a %d-byte part returned %s; want None - the default ceiling is %d",
			file_inline_maximum+1, value.Type(), file_inline_maximum)
	}
}

// multipart_action builds an Action whose request carries one uploaded part.
func multipart_action(t *testing.T, field string, content []byte) *Action {
	t.Helper()
	gin.SetMode(gin.TestMode)

	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	part, err := form.CreateFormFile(field, "upload.bin")
	if err != nil {
		t.Fatalf("building the form: %v", err)
	}
	if _, err := part.Write(content); err != nil {
		t.Fatalf("writing the part: %v", err)
	}
	form.Close()

	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/test/-/upload", &body)
	c.Request.Header.Set("Content-Type", form.FormDataContentType())
	return &Action{web: c}
}

// TestMultipartCeilingCountsBytesInFlight covers the concurrent-upload bound.
// The ceiling is derived from STORED bytes, which bounds one request and says
// nothing about several at once - and multipart parsing happens before the
// Starlark semaphore, so N connections each received the whole remaining quota
// and together filled the cache filesystem.
func TestMultipartCeilingCountsBytesInFlight(t *testing.T) {
	test_data_directory(t)
	user := &User{UID: "inflight-user"}
	if err := os.MkdirAll(user_storage_dir(user), 0755); err != nil {
		t.Fatalf("creating the storage directory: %v", err)
	}

	baseline := web_multipart_maximum(user)
	if baseline <= 0 {
		t.Fatalf("baseline ceiling = %d, want a positive quota-derived bound", baseline)
	}

	const flight = 4 * 1024 * 1024
	multipart_inflight_add(user.UID, flight)
	defer multipart_inflight_add(user.UID, -flight)

	if total := multipart_inflight_total(user.UID); total != flight {
		t.Fatalf("in-flight total = %d, want %d", total, flight)
	}
	if got := web_multipart_maximum(user); got != baseline-flight {
		t.Errorf("ceiling with %d bytes in flight = %d, want %d - a second upload was handed the whole quota again",
			flight, got, baseline-flight)
	}

	// A different user is unaffected: the budget is per account, not global.
	other := &User{UID: "inflight-other"}
	if err := os.MkdirAll(user_storage_dir(other), 0755); err != nil {
		t.Fatalf("creating the storage directory: %v", err)
	}
	if got := web_multipart_maximum(other); got != baseline {
		t.Errorf("another user's ceiling = %d, want the unreduced %d", got, baseline)
	}
}

// TestMultipartCounterChargesAndReleases covers the metering itself. It counts
// reads rather than trusting Content-Length because a chunked body declares
// none, and that is exactly the request the length pre-check cannot refuse.
func TestMultipartCounterChargesAndReleases(t *testing.T) {
	const uid = "counter-user"
	if before := multipart_inflight_total(uid); before != 0 {
		t.Fatalf("in-flight total started at %d, want 0", before)
	}

	payload := make([]byte, 3000)
	counter := &multipart_counter{reader: io.NopCloser(bytes.NewReader(payload)), user: uid}

	buffer := make([]byte, 1000)
	if _, err := counter.Read(buffer); err != nil {
		t.Fatalf("reading: %v", err)
	}
	if total := multipart_inflight_total(uid); total != 1000 {
		t.Errorf("after one 1000-byte read the total = %d, want 1000 - bytes are charged as they arrive, not on completion", total)
	}
	if _, err := counter.Read(buffer); err != nil {
		t.Fatalf("reading: %v", err)
	}
	if total := multipart_inflight_total(uid); total != 2000 {
		t.Errorf("after two reads the total = %d, want 2000", total)
	}

	counter.release()
	if total := multipart_inflight_total(uid); total != 0 {
		t.Errorf("after release the total = %d, want 0 - a finished request must return its bytes to the budget", total)
	}
	// The deferred release in web_action can run after an explicit one.
	counter.release()
	if total := multipart_inflight_total(uid); total != 0 {
		t.Errorf("after a second release the total = %d, want 0", total)
	}
}

// TestServePathRefusesDirectory covers the enumeration hole on the routes that
// serve a bundle file and on a.write.asset. gin's Context.File is
// http.ServeFile, which renders an HTML index for a directory holding no
// index.html, and the path validator refuses empty components but not a
// component that happens to name a directory.
func TestServePathRefusesDirectory(t *testing.T) {
	base := t.TempDir()
	if err := os.MkdirAll(base+"/assets", 0755); err != nil {
		t.Fatalf("creating the directory: %v", err)
	}
	if err := os.WriteFile(base+"/assets/private.txt", []byte("x"), 0600); err != nil {
		t.Fatalf("writing the file: %v", err)
	}

	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/test/assets", nil)
	web_serve_path(c, base+"/assets")

	if w.Code != http.StatusNotFound {
		t.Errorf("serving a directory returned %d, want 404", w.Code)
	}
	if body := w.Body.String(); strings.Contains(body, "private.txt") {
		t.Errorf("directory listing disclosed filenames: %q", body)
	}

	// A real file still serves, or the guard has broken the route.
	w = httptest.NewRecorder()
	c, _ = gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/test/assets/private.txt", nil)
	web_serve_path(c, base+"/assets/private.txt")
	if w.Code != http.StatusOK || w.Body.String() != "x" {
		t.Errorf("serving a file returned %d %q, want 200 \"x\"", w.Code, w.Body.String())
	}
}

// jwt_app_token signs a token the way auth_create_app_token does: the session
// code as the kid header, the session secret as the key, and an app claim,
// which is what the per-app gate compares.
func jwt_app_token(uid string, app string, session string, secret string) (string, error) {
	claims := mochi_claims{
		User: uid,
		App:  app,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Unix(now()+jwt_expiry, 0)),
			IssuedAt:  jwt.NewNumericDate(time.Unix(now(), 0)),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	token.Header["kid"] = session
	return token.SignedString([]byte(secret))
}

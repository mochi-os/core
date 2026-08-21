// Mochi server: /_/health endpoint tests.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jmoiron/sqlx"
)

func init() {
	gin.SetMode(gin.TestMode)
}

// reset_state clears the global state web_health reads so tests don't
// inherit leftover values across runs.
func reset_state(t *testing.T) {
	t.Helper()
	databases_lock.Lock()
	for k := range databases {
		delete(databases, k)
	}
	databases_lock.Unlock()
	net_me = nil
	build_version = "test"
	server_started_at = time.Now().Add(-42 * time.Second)
}

func decode(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("decode: %v (body=%s)", err, body)
	}
	return got
}

// TestHealthDegradedWhenDbAndP2pMissing covers the cold-start case: neither
// users.db nor net_me is wired up. Should be 503 with both subsystems flagged.
func TestHealthDegradedWhenDbAndP2pMissing(t *testing.T) {
	reset_state(t)

	r := gin.New()
	r.GET("/_/health", web_health)

	req := httptest.NewRequest("GET", "/_/health", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status: got %d, want 503", w.Code)
	}
	got := decode(t, w.Body.Bytes())
	if got["status"] != "degraded" {
		t.Errorf("status field: got %v, want degraded", got["status"])
	}
	if got["database"] != "not started" {
		t.Errorf("database field: got %v, want 'not started'", got["database"])
	}
	if got["network"] != "not started" {
		t.Errorf("network field: got %v, want 'not started'", got["network"])
	}
	if got["version"] != "test" {
		t.Errorf("version field: got %v, want 'test'", got["version"])
	}
	// uptime is a float in JSON; check it's roughly the 42 seconds we set
	if u, ok := got["uptime"].(float64); !ok || u < 40 || u > 60 {
		t.Errorf("uptime: got %v, want ~42", got["uptime"])
	}
}

// TestHealthScrubsTheDatabaseError: the body is served unauthenticated, so a
// driver failure must not name an absolute path under data_dir. The fixture
// makes users.db a DIRECTORY on purpose - a merely closed handle reports "sql:
// database is closed", which carries no path and would pass against unscrubbed
// code.
func TestHealthScrubsTheDatabaseError(t *testing.T) {
	reset_state(t)

	data_dir = t.TempDir()
	users_key := filepath.Join(data_dir, "db", "users.db")
	if err := os.MkdirAll(users_key, 0700); err != nil {
		t.Fatalf("fixture: %v", err)
	}

	internal, err := sqlx.Open("sqlite3", users_key)
	if err != nil {
		t.Fatalf("fixture open: %v", err)
	}
	defer internal.Close()

	databases_lock.Lock()
	databases[users_key] = &DB{key: users_key, path: users_key, internal: internal}
	databases_lock.Unlock()

	body, code := health_status()

	if code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503: a database that cannot be pinged is degraded", code)
	}
	status, _ := body["database"].(string)
	if !strings.Contains(status, "unable to open") {
		t.Fatalf("database = %q, want the driver's open failure: the fixture did not reproduce a path-bearing Ping error, so what follows would pass vacuously", status)
	}
	if strings.Contains(status, data_dir) {
		t.Errorf("database = %q still carries the data directory %q; an anonymous caller learns the server's layout", status, data_dir)
	}

	// Scrubbed, not emptied: the man page promises "503 with detail otherwise",
	// and an operator reading mochictl health needs to know which database.
	if !strings.Contains(status, "users.db") {
		t.Errorf("database = %q names no database; the detail the field exists for is gone", status)
	}
}

// TestHealthKeepsItsDocumentedFields. The body is a published contract: the man
// page prints it field for field, mochictl mirrors the field set, and the
// inter-instance tests read version from it. Narrowing it breaks all three.
func TestHealthKeepsItsDocumentedFields(t *testing.T) {
	reset_state(t)

	body, _ := health_status()
	for _, field := range []string{"status", "version", "uptime", "database", "network"} {
		if _, ok := body[field]; !ok {
			t.Errorf("the health body has no %q; mochi.7 documents it, mochictl renders it, and the field set is mirrored on both routes", field)
		}
	}
}

// Mochi server: /_/health endpoint tests.
// Copyright Alistair Cunningham 2026

package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
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

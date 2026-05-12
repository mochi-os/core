package main

import (
	"crypto/ecdh"
	"crypto/rand"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	webpush "github.com/SherClockHolmes/webpush-go"
)

// generateSubscriptionKeys returns a valid (auth, p256dh) pair for tests.
// auth is 16 random bytes; p256dh is the public half of an ephemeral P-256
// keypair in uncompressed form. Both base64url-encoded, matching the
// format webpush-go expects.
func generateSubscriptionKeys(t *testing.T) (auth, p256dh string) {
	t.Helper()
	authBytes := make([]byte, 16)
	if _, err := rand.Read(authBytes); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	priv, err := ecdh.P256().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("ecdh.GenerateKey: %v", err)
	}
	return base64.RawURLEncoding.EncodeToString(authBytes),
		base64.RawURLEncoding.EncodeToString(priv.PublicKey().Bytes())
}

// TestUnifiedPushProviderRegistered verifies the unifiedpush provider entry
// is in the providers slice with the right capability and flow.
func TestUnifiedPushProviderRegistered(t *testing.T) {
	p := provider_get("unifiedpush")
	if p == nil {
		t.Fatal("unifiedpush provider not registered")
	}
	if p.Type != "unifiedpush" {
		t.Errorf("Type = %q, want %q", p.Type, "unifiedpush")
	}
	hasNotify := false
	for _, c := range p.Capabilities {
		if c == "notify" {
			hasNotify = true
		}
	}
	if !hasNotify {
		t.Error("unifiedpush provider should have notify capability")
	}
	if p.Flow != "browser" {
		t.Errorf("Flow = %q, want %q (distributor-driven, no manual form)", p.Flow, "browser")
	}
	if p.Verify {
		t.Error("unifiedpush provider should not require verification")
	}
}

// TestUnifiedPushProviderHasNotifyCapability cross-checks via the helper used
// by the dispatch code in api_account_notify.
func TestUnifiedPushProviderHasNotifyCapability(t *testing.T) {
	if !provider_has_capability("unifiedpush", "notify") {
		t.Error("unifiedpush should be reported as having notify capability")
	}
}

// TestUnifiedPushDeliverLocalFastPath verifies that a path-only endpoint
// (the form synthesised by function_push_register for the Mochi-distributor
// case) doesn't trigger an HTTP self-call. We point a sentinel httptest
// server at a URL that should NOT be hit, and assert it stays untouched.
func TestUnifiedPushDeliverLocalFastPath(t *testing.T) {
	var hits int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	user := &User{UID: "u1"}
	data := map[string]any{
		// Path-only endpoint — local fast-path should fire
		"endpoint": "/menu/-/push/inbound/abc123",
	}

	ok := account_deliver_unifiedpush(user, 42, data, "Title", "Body", "", "tag", "")
	if !ok {
		t.Error("account_deliver_unifiedpush returned false for local fast-path; want true")
	}
	if got := atomic.LoadInt32(&hits); got != 0 {
		t.Errorf("local fast-path triggered %d HTTP request(s); want 0", got)
	}
}

// TestUnifiedPushDeliverRemote verifies that an absolute endpoint URL
// (third-party distributor like ntfy.sh) routes via RFC 8030 Web Push.
func TestUnifiedPushDeliverRemote(t *testing.T) {
	var hits int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		// 201 Created = success per RFC 8030
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	setupVAPID(t)
	auth, p256dh := generateSubscriptionKeys(t)

	user := &User{UID: "u1"}
	data := map[string]any{
		"endpoint": server.URL + "/push/abc",
		"auth":     auth,
		"p256dh":   p256dh,
	}

	ok := account_deliver_unifiedpush(user, 42, data, "Title", "Body", "", "tag", "")
	if !ok {
		t.Errorf("account_deliver_unifiedpush returned false for remote endpoint; want true")
	}
	if got := atomic.LoadInt32(&hits); got != 1 {
		t.Errorf("remote endpoint POSTed %d time(s); want 1", got)
	}
}

// TestUnifiedPushDeliverRemoteGone verifies that 410 Gone (the standard
// "subscription dead" response from RFC 8030 push services) returns false,
// so the caller's outer loop drops the account row.
func TestUnifiedPushDeliverRemoteGone(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusGone)
	}))
	defer server.Close()

	setupVAPID(t)
	auth, p256dh := generateSubscriptionKeys(t)

	user := &User{UID: "u1"}
	data := map[string]any{
		"endpoint": server.URL + "/push/abc",
		"auth":     auth,
		"p256dh":   p256dh,
	}

	if account_deliver_unifiedpush(user, 42, data, "T", "B", "", "tag", "") {
		t.Error("delivery should fail when push service returns 410 Gone")
	}
}

// setupVAPID injects a test VAPID keypair into the package globals and
// consumes webpush_once so the lazy initialiser doesn't later try to
// write through to a non-existent settings DB. Restored on test cleanup.
func setupVAPID(t *testing.T) {
	t.Helper()
	priv, pub, err := webpush.GenerateVAPIDKeys()
	if err != nil {
		t.Fatalf("GenerateVAPIDKeys: %v", err)
	}
	origPub, origPriv := webpush_public, webpush_private
	webpush_public, webpush_private = pub, priv
	webpush_once.Do(func() {}) // mark consumed
	t.Cleanup(func() { webpush_public, webpush_private = origPub, origPriv })
}

// TestUnifiedPushDeliverEmptyEndpoint guards against silent success when an
// account row is missing its endpoint.
func TestUnifiedPushDeliverEmptyEndpoint(t *testing.T) {
	user := &User{UID: "u1"}
	data := map[string]any{} // no endpoint
	if account_deliver_unifiedpush(user, 42, data, "T", "B", "", "tag", "") {
		t.Error("delivery should fail when endpoint is empty")
	}
}

// TestUnifiedPushDeliverRoutesToStoredEndpoint verifies that the endpoint
// from the account's data column is the one the deliver function POSTs to —
// not some hardcoded URL or default. Catches a regression where the path
// logic accidentally rewrites foreign endpoints.
func TestUnifiedPushDeliverRoutesToStoredEndpoint(t *testing.T) {
	expectedHost := ""
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Capture the host so the test can assert the request landed at our
		// sentinel and not some default.
		if expectedHost == "" {
			expectedHost = r.Host
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	setupVAPID(t)
	auth, p256dh := generateSubscriptionKeys(t)

	user := &User{UID: "u1"}
	data := map[string]any{
		"endpoint": server.URL + "/some/path",
		"auth":     auth,
		"p256dh":   p256dh,
	}

	if !account_deliver_unifiedpush(user, 42, data, "T", "B", "", "tag", "") {
		t.Fatal("delivery failed")
	}
	if expectedHost == "" {
		t.Fatal("sentinel server received no requests")
	}
	// Strip the http:// off server.URL to extract the host:port for comparison.
	wantHost := server.URL[len("http://"):]
	if expectedHost != wantHost {
		t.Errorf("delivered to host %q, want %q", expectedHost, wantHost)
	}
}

// TestAccountsHasLastDeliveredColumn verifies the schema migration that
// added the TTL-sweep column. Mirrors the existing
// TestDBUserCreatesAccountsWithDefault pattern.
func TestAccountsHasLastDeliveredColumn(t *testing.T) {
	tmp_dir, err := os.MkdirTemp("", "mochi_unifiedpush_test")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	defer os.RemoveAll(tmp_dir)

	orig_data_dir := data_dir
	data_dir = tmp_dir
	defer func() { data_dir = orig_data_dir }()

	os.MkdirAll(filepath.Join(tmp_dir, "users", "42"), 0755)
	user := &User{UID: "u42"}
	db := db_user(user, "user")

	has, err := db.exists(
		"select 1 from pragma_table_info('accounts') where name='last_delivered'",
	)
	if err != nil {
		t.Fatalf("pragma_table_info query failed: %v", err)
	}
	if !has {
		t.Error("accounts table should have last_delivered column")
	}

	// Clean up from the databases cache so subsequent tests don't reuse
	path := filepath.Join(tmp_dir, "users", "42", "user.db")
	databases_lock.Lock()
	delete(databases, path)
	databases_lock.Unlock()
	db.internal.Close()
	db.starlark.Close()
}


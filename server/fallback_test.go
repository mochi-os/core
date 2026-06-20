// Mochi server: 443 hostile-network fallback unit tests
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	multiaddr "github.com/multiformats/go-multiaddr"
)

func TestFallbackIsWebsocketUpgrade(t *testing.T) {
	upgrade := func(conn, up string) *http.Request {
		r := httptest.NewRequest("GET", "/", nil)
		if conn != "" {
			r.Header.Set("Connection", conn)
		}
		if up != "" {
			r.Header.Set("Upgrade", up)
		}
		return r
	}
	yes := []*http.Request{
		upgrade("Upgrade", "websocket"),
		upgrade("upgrade", "WebSocket"),
		upgrade("keep-alive, Upgrade", "websocket"),
	}
	for _, r := range yes {
		if !fallback_is_websocket_upgrade(r) {
			t.Errorf("Connection=%q Upgrade=%q: not detected as WS upgrade", r.Header.Get("Connection"), r.Header.Get("Upgrade"))
		}
	}
	no := []*http.Request{
		upgrade("", ""),
		upgrade("keep-alive", ""),
		upgrade("Upgrade", "h2c"),     // upgrade, but not websocket
		upgrade("", "websocket"),      // upgrade header without Connection
		upgrade("close", "websocket"), // websocket header without Connection: upgrade
	}
	for _, r := range no {
		if fallback_is_websocket_upgrade(r) {
			t.Errorf("Connection=%q Upgrade=%q: wrongly detected as WS upgrade", r.Header.Get("Connection"), r.Header.Get("Upgrade"))
		}
	}
}

func TestFallbackIsLoopbackWs(t *testing.T) {
	loopback := []string{"/ip4/127.0.0.1/tcp/40001/ws", "/ip6/::1/tcp/40001/ws"}
	for _, s := range loopback {
		if !fallback_is_loopback_ws(multiaddr.StringCast(s)) {
			t.Errorf("%q not recognised as loopback ws", s)
		}
	}
	other := []string{"/ip4/127.0.0.1/tcp/1443", "/dns4/mochi-os.org/tcp/443/tls/ws", "/ip4/192.0.2.1/tcp/443/tls/ws"}
	for _, s := range other {
		if fallback_is_loopback_ws(multiaddr.StringCast(s)) {
			t.Errorf("%q wrongly recognised as loopback ws", s)
		}
	}
}

func TestFallbackListenAddresses(t *testing.T) {
	if got := fallback_listen_addresses(); got != nil {
		t.Errorf("fallback off: listen addresses = %v, want nil", got)
	}
	t.Setenv("MOCHI_P2P_HTTPS", "true")
	got := fallback_listen_addresses()
	if len(got) != 3 {
		t.Fatalf("fallback on: %d listen addresses, want 3", len(got))
	}
	joined := strings.Join(got, " ")
	for _, want := range []string{"/udp/443/quic-v1", "/127.0.0.1/tcp/0/ws"} {
		if !strings.Contains(joined, want) {
			t.Errorf("listen addresses missing %q: %v", want, got)
		}
	}
}

func TestFallbackAddrsFactory(t *testing.T) {
	cleanup := setup_peer_discovery_test(t)
	defer cleanup()
	db_open("db/domains.db").exec("create table if not exists domains (domain text primary key, verified integer not null default 0, token text not null default '', tls integer not null default 1, created integer not null, updated integer not null)")

	in := []multiaddr.Multiaddr{
		multiaddr.StringCast("/ip4/127.0.0.1/tcp/40001/ws"), // loopback ws, must drop
		multiaddr.StringCast("/ip4/198.51.100.4/tcp/1443"),  // keep
		multiaddr.StringCast("/ip4/198.51.100.4/udp/443/quic-v1"),
	}

	// Off: unchanged.
	if got := fallback_addrs_factory(in); len(got) != 3 {
		t.Errorf("fallback off: factory changed addresses (%d)", len(got))
	}

	t.Setenv("MOCHI_P2P_HTTPS", "true")
	// The server hosts a user's content domain. It must NEVER be advertised
	// as a server address: the factory publishes no domain-based address at
	// all (a server's identity is its peer ID + IPs, never a name it serves).
	db_open("db/domains.db").exec("insert into domains (domain, verified, created, updated) values ('someuser.example', 1, 1, 1)")
	got := fallback_addrs_factory(in)
	joined := ""
	for _, a := range got {
		joined += a.String() + " "
	}
	if strings.Contains(joined, "/127.0.0.1/tcp/40001/ws") {
		t.Error("loopback ws was not dropped from advertised addresses")
	}
	// No domain-based address is ever advertised: no /dns, no WSS, and above
	// all no served (user) domain.
	if strings.Contains(joined, "/dns") || strings.Contains(joined, "/tls/ws") || strings.Contains(joined, "someuser.example") {
		t.Errorf("a domain-based address was advertised (must be none): %s", joined)
	}
	// IP-based addresses are kept (the QUIC/443 and 1443 paths still work).
	if !strings.Contains(joined, "/ip4/198.51.100.4/tcp/1443") {
		t.Error("normal address was dropped")
	}
}

// TestFallbackAutoEnable: with no explicit [p2p] https, the fallback
// follows the web server's HTTPS — on when serving 443 with a domain,
// off otherwise — and the explicit setting overrides either way.
func TestFallbackAutoEnable(t *testing.T) {
	cleanup := setup_peer_discovery_test(t)
	defer cleanup()
	db_open("db/domains.db").exec("create table if not exists domains (domain text primary key, verified integer not null default 0, token text not null default '', tls integer not null default 1, created integer not null, updated integer not null)")

	// No 443, no domain → off.
	if fallback_enabled() {
		t.Error("enabled with no HTTPS configured")
	}

	// 443 but still no domain → off (no certificate to share).
	t.Setenv("MOCHI_WEB_PORTS", "443")
	if fallback_enabled() {
		t.Error("enabled on 443 with no domain")
	}

	// 443 + a domain → HTTPS is enabled → fallback auto-on.
	db_open("db/domains.db").exec("insert into domains (domain, verified, created, updated) values ('mochi-os.org', 1, 1, 1)")
	if !fallback_enabled() {
		t.Error("not auto-enabled while serving HTTPS on 443 with a domain")
	}

	// Explicit opt-out wins even when HTTPS is on.
	t.Setenv("MOCHI_P2P_HTTPS", "false")
	if fallback_enabled() {
		t.Error("explicit [p2p] https=false did not opt out")
	}
}

// TestFallbackMiddleware: only a root-path WebSocket upgrade is bridged
// to the libp2p loopback listener; everything else passes through to the
// web app. The off case (port 0) never bridges.
func TestFallbackMiddleware(t *testing.T) {
	gin.SetMode(gin.TestMode)

	backendHit := false
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendHit = true
		w.WriteHeader(200)
	}))
	defer backend.Close()
	backendPort := backend.Listener.Addr().(interface{ String() string }).String()
	// Extract the numeric port from "127.0.0.1:NNNN".
	if i := strings.LastIndex(backendPort, ":"); i >= 0 {
		fallback_ws_port = int(atoi(backendPort[i+1:], 0))
	}
	defer func() { fallback_ws_port = 0 }()

	passedThrough := false
	engine := gin.New()
	engine.Use(fallback_middleware)
	engine.NoRoute(func(c *gin.Context) { passedThrough = true; c.Status(200) })
	front := httptest.NewServer(engine)
	defer front.Close()

	do := func(path string, ws bool) {
		backendHit, passedThrough = false, false
		req, _ := http.NewRequest("GET", front.URL+path, nil)
		if ws {
			req.Header.Set("Connection", "Upgrade")
			req.Header.Set("Upgrade", "websocket")
			req.Header.Set("Sec-WebSocket-Version", "13")
			req.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
		}
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			resp.Body.Close()
		}
	}

	// Root WebSocket upgrade → bridged to the libp2p backend.
	do("/", true)
	if !backendHit {
		t.Error("root WS upgrade was not bridged to the libp2p listener")
	}
	if passedThrough {
		t.Error("root WS upgrade also reached the web app")
	}

	// Root plain GET → web app, not bridged.
	do("/", false)
	if backendHit {
		t.Error("plain root GET was bridged")
	}
	if !passedThrough {
		t.Error("plain root GET did not reach the web app")
	}

	// App-path WebSocket upgrade (an app's own socket) → web app.
	do("/feeds/abc/-/socket", true)
	if backendHit {
		t.Error("app-path WS upgrade was bridged")
	}
	if !passedThrough {
		t.Error("app-path WS upgrade did not reach the web app")
	}

	// Off (port 0): even a root WS upgrade passes through.
	fallback_ws_port = 0
	do("/", true)
	if backendHit {
		t.Error("root WS upgrade bridged while fallback off")
	}
	if !passedThrough {
		t.Error("root WS upgrade did not reach the web app while fallback off")
	}
}

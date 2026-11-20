// Mochi server: 443 hostile-network fallback unit tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	p2p "github.com/libp2p/go-libp2p"
	p2p_network "github.com/libp2p/go-libp2p/core/network"
	p2p_peer "github.com/libp2p/go-libp2p/core/peer"
	p2p_websocket "github.com/libp2p/go-libp2p/p2p/transport/websocket"
	multiaddr "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
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
	setup_peer_discovery_test(t)
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
	setup_peer_discovery_test(t)
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

// fallback_test_listener installs a bridge listener for the rest of the test and
// collects what it is handed.
func fallback_test_listener(t *testing.T) chan manet.Conn {
	t.Helper()
	l := &fallback_listener{address: multiaddr.StringCast(fallback_bridge_address), incoming: make(chan manet.Conn), closed: make(chan struct{})}
	fallback_bridge.Store(l)
	t.Cleanup(func() { l.Close() })
	bridged := make(chan manet.Conn, 8)
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			bridged <- c
		}
	}()
	return bridged
}

// TestFallbackMiddleware: only a root-path WebSocket upgrade is handed to the
// bridge's libp2p listener; everything else passes through to the web app. With
// no bridge listener the fallback is off and nothing is bridged.
func TestFallbackMiddleware(t *testing.T) {
	gin.SetMode(gin.TestMode)
	bridged := fallback_test_listener(t)

	passed_through := false
	engine := gin.New()
	engine.Use(fallback_middleware)
	engine.NoRoute(func(c *gin.Context) { passed_through = true; c.Status(200) })
	front := httptest.NewServer(engine)
	defer front.Close()

	do := func(path string, ws bool) bool {
		passed_through = false
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
		select {
		case c := <-bridged:
			c.Close()
			return true
		case <-time.After(300 * time.Millisecond):
			return false
		}
	}

	// Root WebSocket upgrade → handed to libp2p.
	if !do("/", true) {
		t.Error("root WS upgrade was not handed to the libp2p listener")
	}
	if passed_through {
		t.Error("root WS upgrade also reached the web app")
	}

	// Root plain GET → web app, not bridged.
	if do("/", false) {
		t.Error("plain root GET was bridged")
	}
	if !passed_through {
		t.Error("plain root GET did not reach the web app")
	}

	// App-path WebSocket upgrade (an app's own socket) → web app.
	if do("/feeds/abc/-/socket", true) {
		t.Error("app-path WS upgrade was bridged")
	}
	if !passed_through {
		t.Error("app-path WS upgrade did not reach the web app")
	}

	// Off (no bridge listener): even a root WS upgrade passes through.
	fallback_bridge.Load().Close()
	if do("/", true) {
		t.Error("root WS upgrade bridged while fallback off")
	}
	if !passed_through {
		t.Error("root WS upgrade did not reach the web app while fallback off")
	}
}

// TestFallbackBridgeCarriesTheClientAddress is #136. The bridge reverse-proxied
// to a loopback libp2p listener, so libp2p saw every bridged peer at
// 127.0.0.1 and the proxy's own port: loopback is exempt from libp2p's
// per-address connection limits and rate limit, the relay counted every bridged
// reservation against one address, and the address recorded for the peer was
// this host's own loopback. libp2p must see the client socket itself.
func TestFallbackBridgeCarriesTheClientAddress(t *testing.T) {
	gin.SetMode(gin.TestMode)
	server, err := p2p.New(p2p.ListenAddrStrings(fallback_bridge_address), p2p.Transport(fallback_transport_new))
	if err != nil {
		t.Fatalf("server host: %v", err)
	}
	defer server.Close()
	if fallback_bridge.Load() == nil {
		t.Fatal("libp2p did not ask the bridge transport for the bridge's listener")
	}

	engine := gin.New()
	engine.Use(fallback_middleware)
	front := httptest.NewServer(engine)
	defer front.Close()

	client, err := p2p.New(p2p.NoListenAddrs, p2p.Transport(p2p_websocket.New))
	if err != nil {
		t.Fatalf("client host: %v", err)
	}
	defer client.Close()

	port := front.Listener.Addr().(*net.TCPAddr).Port
	target := multiaddr.StringCast(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d/ws", port))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := client.Connect(ctx, p2p_peer.AddrInfo{ID: server.ID(), Addrs: []multiaddr.Multiaddr{target}}); err != nil {
		t.Fatalf("the client could not connect through the bridge: %v", err)
	}

	var inbound []p2p_network.Conn
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if inbound = server.Network().ConnsToPeer(client.ID()); len(inbound) > 0 {
			break
		}
	}
	if len(inbound) == 0 {
		t.Fatal("the server holds no connection from the client")
	}
	outbound := client.Network().ConnsToPeer(server.ID())
	if len(outbound) == 0 {
		t.Fatal("the client holds no connection to the server")
	}
	remote := inbound[0].RemoteMultiaddr()
	local := outbound[0].LocalMultiaddr()
	remote_port, _ := remote.ValueForProtocol(multiaddr.P_TCP)
	local_port, _ := local.ValueForProtocol(multiaddr.P_TCP)
	if remote_port == "" || remote_port != local_port {
		t.Errorf("THE POINT: libp2p sees the bridged peer at %s, not at the client's own socket %s", remote, local)
	}
	if _, err := remote.ValueForProtocol(multiaddr.P_WS); err != nil {
		t.Errorf("the bridged connection's address %s is not a WebSocket one", remote)
	}
}

// fallback_test_conn is a connection seen from a given remote address.
type fallback_test_conn struct {
	p2p_network.Conn
	remote multiaddr.Multiaddr
}

func (c *fallback_test_conn) RemoteMultiaddr() multiaddr.Multiaddr { return c.remote }

// TestBridgedPeerAddressIsNotRecorded: a bridged connection's remote address is
// the client's socket, which no one can dial back, and was recorded for the
// peer - as this host's own loopback, while the bridge was a reverse proxy.
func TestBridgedPeerAddressIsNotRecorded(t *testing.T) {
	setup_peer_discovery_test(t)

	id, _ := test_host(t)
	peer_observed(id, &fallback_test_conn{remote: multiaddr.StringCast("/ip4/198.51.100.5/tcp/50123/wss")})
	if peer_held(id) {
		t.Error("THE POINT: a WebSocket client's socket address was recorded for the peer")
	}
	peer_observed(id, &fallback_test_conn{remote: multiaddr.StringCast("/ip4/198.51.100.5/tcp/1443")})
	if !peer_held(id) {
		t.Error("control: a dialable inbound address was not recorded")
	}
}

// TestNetStartInstallsTheBridgeAndTheRelayCandidates pins net_start's options:
// libp2p's default transports with the WebSocket one replaced by the bridge's,
// so the 443 bridge has its listener and no default transport is lost; and
// AutoRelay waiting for, and holding, the relays it wants rather than its
// defaults, which net_relay_candidates' configured-first rule relies on.
func TestNetStartInstallsTheBridgeAndTheRelayCandidates(t *testing.T) {
	source, err := os.ReadFile("net.go")
	if err != nil {
		t.Fatalf("reading net.go: %v", err)
	}
	for _, want := range []string{
		"p2p.Transport(p2p_tcp.NewTCPTransport)",
		"p2p.Transport(p2p_quic.NewTransport)",
		"p2p.Transport(fallback_transport_new)",
		"p2p.Transport(p2p_webtransport.New)",
		"p2p.Transport(p2p_webrtc.New)",
		"autorelay.WithNumRelays(relay_wanted)",
		"autorelay.WithMinCandidates(relay_wanted)",
		"autorelay.WithMaxCandidates(relay_candidates_maximum)",
	} {
		if !strings.Contains(string(source), want) {
			t.Errorf("net.go no longer contains %q", want)
		}
	}
}

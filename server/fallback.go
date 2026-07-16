// Mochi server: hostile-network reachability on port 443.
//
// Hostile networks — hotel WiFi, corporate firewalls, captive portals —
// block outbound traffic to all but well-known ports, in practice TCP
// and UDP 443. A server reachable only on the libp2p port (1443) is
// invisible to peers on such networks. This module lets a public server
// also accept libp2p connections on 443, two ways that between them
// cover both firewall styles:
//
//   - QUIC on UDP/443. libp2p already speaks QUIC; a UDP/443 listener
//     does not conflict with the web server's TCP/443. Covers networks
//     that allow UDP/443.
//
//   - WebSocket-Secure on TCP/443, shared with the web server. A WSS
//     connection is indistinguishable from HTTPS (a TLS handshake then
//     a WebSocket upgrade), so it passes any firewall that allows the
//     web. TCP/443 is a single socket owned by the web server, so the
//     two are demuxed: libp2p's WebSocket transport ignores the path
//     component and always upgrades at "/", while every other Mochi
//     WebSocket and request targets an app or /_/ path — so a WebSocket
//     upgrade to exactly "/" is libp2p and is bridged to a loopback
//     libp2p listener; everything else is the web app. The outer TLS
//     terminates at the web server (its certificate); libp2p runs its
//     own Noise handshake end-to-end inside the WebSocket stream, so
//     the bridge is not a trust boundary.
//
// Enabled automatically whenever the web server serves HTTPS, which in
// Mochi is exactly "serving port 443 with a domain configured" (web.go
// serves HTTPS only on 443). That is precisely the condition under which
// the fallback is both useful (a publicly-reachable HTTPS endpoint) and
// safe (it already holds the privilege to bind 443 and the certificate
// to share). `[p2p] https` overrides: `true` forces it on, `false` opts
// out (e.g. a server fronted by a CDN that wouldn't pass WSS through).
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"github.com/gin-gonic/gin"
	multiaddr "github.com/multiformats/go-multiaddr"
)

// fallback_ws_port is the loopback TCP port the libp2p WebSocket
// transport listens on; the web server's 443 listener bridges root
// WebSocket upgrades to it. Zero until fallback_capture runs.
var fallback_ws_port int

// fallback_enabled reports whether this server offers libp2p on 443.
// Defaults to following the web server's HTTPS (see web_https_enabled);
// the `[p2p] https` setting overrides either way.
func fallback_enabled() bool {
	switch strings.ToLower(strings.TrimSpace(ini_string("p2p", "https", ""))) {
	case "true":
		return true
	case "false":
		return false
	}
	return web_https_enabled()
}

// web_https_enabled mirrors web.go's own condition for serving HTTPS:
// port 443 is among the configured web ports and at least one domain is
// configured (the web server serves HTTPS only on 443). This is the
// signal that a usable certificate exists for the WSS half to share.
func web_https_enabled() bool {
	ports := ini_ints_commas("web", "ports")
	if len(ports) == 0 {
		ports = []int{ini_int("web", "port", 80)}
	}
	served := false
	for _, p := range ports {
		if p == 443 {
			served = true
			break
		}
	}
	return served && len(domain_list()) > 0
}

// fallback_listen_addresses are the extra libp2p listen multiaddrs for
// the 443 fallback: QUIC on UDP/443, and a loopback plain-WebSocket
// listener the web server bridges to (the public TLS terminates at the
// web server, so the libp2p listener itself is plaintext on loopback).
// Empty when the fallback is off.
func fallback_listen_addresses() []string {
	if !fallback_enabled() {
		return nil
	}
	return []string{
		"/ip4/0.0.0.0/udp/443/quic-v1",
		"/ip6/::/udp/443/quic-v1",
		"/ip4/127.0.0.1/tcp/0/ws",
	}
}

// fallback_addrs_factory rewrites the host's advertised addresses for the
// 443 fallback: it drops the loopback WebSocket listener (an internal
// bridge address — no remote peer can reach 127.0.0.1).
//
// It deliberately advertises NO domain-based (WSS) address. A domain is a
// name a server *serves*, never its own identity, and a server may have
// none — so there is no correct domain to publish as a system address, and
// auto-publishing a served one leaks to the whole network which users'
// domains a server hosts. Hostile-network reach over TCP/443 is provided by
// the curated bootstrap entries (which carry the project's own name), not
// by each server advertising a name for itself. The IP-based QUIC/443
// fallback still propagates from the identify-observed address like any
// other.
func fallback_addrs_factory(addrs []multiaddr.Multiaddr) []multiaddr.Multiaddr {
	if !fallback_enabled() {
		return addrs
	}
	out := make([]multiaddr.Multiaddr, 0, len(addrs))
	for _, a := range addrs {
		if fallback_is_loopback_ws(a) {
			continue
		}
		out = append(out, a)
	}
	return out
}

// fallback_is_loopback_ws reports whether a multiaddress is a loopback
// WebSocket listener (the bridged-from address that must not be
// advertised).
func fallback_is_loopback_ws(a multiaddr.Multiaddr) bool {
	s := a.String()
	if !strings.HasSuffix(s, "/ws") {
		return false
	}
	return strings.HasPrefix(s, "/ip4/127.") || strings.HasPrefix(s, "/ip6/::1/")
}

// fallback_capture records the loopback port the WebSocket transport
// bound, after the host has started, for the web bridge to target.
func fallback_capture() {
	if !fallback_enabled() || net_me == nil {
		return
	}
	for _, a := range net_me.Network().ListenAddresses() {
		if !fallback_is_loopback_ws(a) {
			continue
		}
		if p, err := a.ValueForProtocol(multiaddr.P_TCP); err == nil {
			fallback_ws_port = int(atoi(p, 0))
			info("Net 443 fallback active: libp2p WebSocket bridged from loopback port %d", fallback_ws_port)
			return
		}
	}
}

// fallback_middleware bridges a root-path WebSocket upgrade on the web
// server's 443 listener to the loopback libp2p WebSocket transport. A
// libp2p WSS client always upgrades at "/" (its transport ignores the
// path); every other Mochi WebSocket and request targets an app or /_/
// path. So a WebSocket upgrade to exactly "/" is libp2p and nothing
// else. When the fallback is off, or the request is not a root
// WebSocket upgrade, the request passes through untouched — so this is
// a no-op for every existing code path.
func fallback_middleware(c *gin.Context) {
	if fallback_ws_port == 0 || c.Request.URL.Path != "/" || !fallback_is_websocket_upgrade(c.Request) {
		c.Next()
		return
	}
	target := &url.URL{Scheme: "http", Host: fmt.Sprintf("127.0.0.1:%d", fallback_ws_port)}
	httputil.NewSingleHostReverseProxy(target).ServeHTTP(c.Writer, c.Request)
	c.Abort()
}

// fallback_is_websocket_upgrade reports whether a request is a WebSocket
// upgrade (an Upgrade: websocket header with upgrade in Connection).
func fallback_is_websocket_upgrade(r *http.Request) bool {
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		return false
	}
	for _, token := range strings.Split(r.Header.Get("Connection"), ",") {
		if strings.EqualFold(strings.TrimSpace(token), "upgrade") {
			return true
		}
	}
	return false
}

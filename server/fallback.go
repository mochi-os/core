// Mochi server: hostile-network reachability on port 443.
//
// Networks that block all but 443 make a server on the libp2p port invisible,
// so this module also accepts libp2p on QUIC/UDP 443 and on WebSocket-Secure
// over the web server's TCP/443. The two share that socket by path: libp2p's
// WebSocket transport always upgrades at "/" while every other Mochi WebSocket
// targets an app or /_/ path, so a root upgrade is bridged to a loopback libp2p
// listener. The outer TLS terminates at the web server, but libp2p runs its own
// Noise handshake inside, so the bridge is not a trust boundary.
//
// On whenever the web server serves HTTPS; `[p2p] https` forces it on or off.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

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

// fallback_listen_addresses are the extra libp2p listen multiaddrs for the 443
// fallback: QUIC on UDP/443, and a loopback plaintext WebSocket listener the
// web server bridges to. Empty when the fallback is off.
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

// fallback_addrs_factory drops the loopback WebSocket listener from the host's
// advertised addresses - no remote peer can reach 127.0.0.1. No domain-based
// (WSS) address is advertised either: a domain is a name a server serves, not
// its identity, and publishing one leaks which users' domains a server hosts.
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

// fallback_middleware bridges a root-path WebSocket upgrade on the web server's
// 443 listener to the loopback libp2p WebSocket transport: a libp2p WSS client
// always upgrades at "/", every other Mochi WebSocket targets an app or /_/
// path. Anything else passes through untouched.
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

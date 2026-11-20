// Mochi server: hostile-network reachability on port 443.
//
// Networks that block all but 443 make a server on the libp2p port invisible,
// so this module also accepts libp2p on QUIC/UDP 443 and on WebSocket-Secure
// over the web server's TCP/443. The two share that socket by path: libp2p's
// WebSocket transport always upgrades at "/" while every other Mochi WebSocket
// targets an app or /_/ path, so the web server accepts a root upgrade itself
// and hands the connection to libp2p's listener for the bridge. The outer TLS
// terminates at the web server, but libp2p runs its own Noise handshake inside,
// so the bridge is not a trust boundary.
//
// libp2p sees each bridged connection at the client's own address. A reverse
// proxy to a loopback libp2p listener made every bridged peer 127.0.0.1, which
// libp2p's per-address connection limits and rate limit exempt, so one host
// could hold every inbound connection slot; the relay counted every bridged
// reservation against that one address; and the address recorded for each peer
// was this host's own loopback.
//
// On whenever the web server serves HTTPS; `[p2p] https` forces it on or off.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	p2p_network "github.com/libp2p/go-libp2p/core/network"
	p2p_transport "github.com/libp2p/go-libp2p/core/transport"
	"github.com/libp2p/go-libp2p/p2p/transport/tcpreuse"
	p2p_websocket "github.com/libp2p/go-libp2p/p2p/transport/websocket"
	multiaddr "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"nhooyr.io/websocket"
)

// fallback_bridge_address is the listen address of the bridge's libp2p
// listener. No socket is bound to it: fallback_transport recognises it and
// answers with a listener fallback_middleware feeds. Loopback, so
// fallback_addrs_factory keeps it out of what this host advertises.
const fallback_bridge_address = "/ip4/127.0.0.1/tcp/0/ws"

// fallback_offer_timeout bounds how long an accepted WebSocket waits for
// libp2p's accept loop, as libp2p's own WebSocket listener bounds a handshake.
const fallback_offer_timeout = 15 * time.Second

// fallback_bridge is the bridge's listener once libp2p has asked for it; nil
// while the fallback is off.
var fallback_bridge atomic.Pointer[fallback_listener]

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
// fallback: QUIC on UDP/443, and the bridge's listener, which the web server
// feeds. Empty when the fallback is off.
func fallback_listen_addresses() []string {
	if !fallback_enabled() {
		return nil
	}
	return []string{
		"/ip4/0.0.0.0/udp/443/quic-v1",
		"/ip6/::/udp/443/quic-v1",
		fallback_bridge_address,
	}
}

// fallback_addrs_factory drops the bridge's loopback listener from the host's
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

// fallback_transport is libp2p's WebSocket transport, dialling as it does, but
// listening on fallback_bridge_address through the web server's own listener.
// Installed in place of the plain WebSocket transport whether or not the
// fallback is on.
type fallback_transport struct {
	*p2p_websocket.WebsocketTransport
	upgrader p2p_transport.Upgrader
}

// fallback_transport_new is the transport's constructor, which libp2p calls
// with what it calls the WebSocket transport's own with.
func fallback_transport_new(upgrader p2p_transport.Upgrader, resources p2p_network.ResourceManager, shared *tcpreuse.ConnMgr) (*fallback_transport, error) {
	inner, err := p2p_websocket.New(upgrader, resources, shared)
	if err != nil {
		return nil, err
	}
	return &fallback_transport{WebsocketTransport: inner, upgrader: upgrader}, nil
}

// Listen answers the bridge's address with a listener fallback_middleware
// feeds, and any other WebSocket address as libp2p's transport does. Its
// connections pass the gate libp2p's own listeners do - the connection gater
// and the resource manager's per-address limits - at the client's address.
func (t *fallback_transport) Listen(address multiaddr.Multiaddr) (p2p_transport.Listener, error) {
	if address.String() != fallback_bridge_address {
		return t.WebsocketTransport.Listen(address)
	}
	l := &fallback_listener{address: address, incoming: make(chan manet.Conn), closed: make(chan struct{})}
	fallback_bridge.Store(l)
	info("Net 443 fallback active: libp2p WebSocket accepted by the web server")
	return t.upgrader.UpgradeGatedMaListener(t, t.upgrader.GateMaListener(l)), nil
}

// fallback_listener is the libp2p side of the bridge: a listener with no
// socket, whose connections are the WebSockets fallback_middleware accepted.
type fallback_listener struct {
	address  multiaddr.Multiaddr
	incoming chan manet.Conn
	closed   chan struct{}
	once     sync.Once
}

func (l *fallback_listener) Accept() (manet.Conn, error) {
	select {
	case c := <-l.incoming:
		return c, nil
	case <-l.closed:
		return nil, p2p_transport.ErrListenerClosed
	}
}

func (l *fallback_listener) Close() error {
	l.once.Do(func() { close(l.closed) })
	fallback_bridge.CompareAndSwap(l, nil)
	return nil
}

func (l *fallback_listener) Multiaddr() multiaddr.Multiaddr {
	return l.address
}

func (l *fallback_listener) Addr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)}
}

// offer hands an accepted connection to libp2p's accept loop, reporting whether
// it was taken.
func (l *fallback_listener) offer(c manet.Conn) bool {
	timer := time.NewTimer(fallback_offer_timeout)
	defer timer.Stop()
	select {
	case l.incoming <- c:
		return true
	case <-l.closed:
	case <-timer.C:
	}
	return false
}

// fallback_connection is a bridged WebSocket as libp2p reads it: a stream over
// binary messages, at the client socket's own address.
type fallback_connection struct {
	net.Conn
	local  multiaddr.Multiaddr
	remote multiaddr.Multiaddr
}

func (c *fallback_connection) LocalMultiaddr() multiaddr.Multiaddr {
	return c.local
}

func (c *fallback_connection) RemoteMultiaddr() multiaddr.Multiaddr {
	return c.remote
}

// fallback_websocket_address renders a socket address as a WebSocket multiaddr,
// /wss when the web server terminated TLS on it, as libp2p's own WebSocket
// listener renders its connections'.
func fallback_websocket_address(address net.Addr, secure bool) (multiaddr.Multiaddr, error) {
	return manet.FromNetAddr(p2p_websocket.NewAddrWithScheme(address.String(), secure))
}

// fallback_accept accepts a root WebSocket upgrade on the web server's listener
// as a connection for libp2p. Accept answers a refused upgrade itself.
func fallback_accept(w http.ResponseWriter, r *http.Request) (*fallback_connection, error) {
	socket, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// libp2p's own listener accepts every origin: the client is a libp2p
		// host, which carries no cookie or credential an origin check would
		// protect, and Noise authenticates it inside.
		InsecureSkipVerify: true,
	})
	if err != nil {
		return nil, err
	}
	stream := websocket.NetConn(net_context, socket, websocket.MessageBinary)
	secure := r.TLS != nil
	local, err := fallback_websocket_address(stream.LocalAddr(), secure)
	if err != nil {
		stream.Close()
		return nil, err
	}
	remote, err := fallback_websocket_address(stream.RemoteAddr(), secure)
	if err != nil {
		stream.Close()
		return nil, err
	}
	return &fallback_connection{Conn: stream, local: local, remote: remote}, nil
}

// fallback_middleware hands a root-path WebSocket upgrade on the web server to
// libp2p's bridge listener: a libp2p WSS client always upgrades at "/", every
// other Mochi WebSocket targets an app or /_/ path. Anything else passes
// through untouched.
func fallback_middleware(c *gin.Context) {
	l := fallback_bridge.Load()
	if l == nil || c.Request.URL.Path != "/" || !fallback_is_websocket_upgrade(c.Request) {
		c.Next()
		return
	}
	c.Abort()
	connection, err := fallback_accept(c.Writer, c.Request)
	if err != nil {
		debug("Net 443 bridge refused an upgrade from %q: %v", c.Request.RemoteAddr, err)
		return
	}
	if !l.offer(connection) {
		connection.Close()
	}
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

// Mochi server: IPv6 / address-routability filter tests
// Copyright Alistair Cunningham 2026

package main

import (
	"testing"

	"github.com/multiformats/go-multiaddr"
)

// TestNetUnroutable pins which addresses are rejected as undialable across
// hosts (loopback, unspecified, link-local) versus kept. Private RFC1918
// and IPv6 ULA addresses MUST be kept — they're legitimately dialable
// between peers on the same LAN via mDNS. dns* addresses (no IP) are kept.
func TestNetUnroutable(t *testing.T) {
	cases := []struct {
		addr string
		want bool // true = unroutable (rejected)
	}{
		// Rejected: no other host could ever dial these.
		{"/ip4/127.0.0.1/tcp/1443", true},  // loopback v4
		{"/ip6/::1/tcp/1443", true},        // loopback v6
		{"/ip4/0.0.0.0/tcp/1443", true},    // unspecified v4
		{"/ip6/::/tcp/1443", true},         // unspecified v6
		{"/ip4/169.254.3.7/tcp/1443", true}, // link-local v4
		{"/ip6/fe80::1/tcp/1443", true},    // link-local v6

		// Kept: globally routable.
		{"/ip4/203.0.113.9/tcp/1443", false},
		{"/ip6/2606:4700::1111/tcp/1443", false},
		{"/ip6/2606:4700::1111/udp/443/quic-v1", false}, // v6 QUIC kept

		// Kept: LAN-dialable (mDNS) — must NOT be rejected.
		{"/ip4/192.168.1.5/tcp/1443", false}, // RFC1918
		{"/ip4/10.0.0.4/tcp/1443", false},    // RFC1918
		{"/ip6/fc00::5/tcp/1443", false},     // ULA

		// Kept: no IP component to judge — resolved at dial time.
		{"/dns/mochi-os.org/tcp/443/tls/ws", false},
		{"/dns6/example.com/tcp/443/tls/ws", false},
	}

	for _, c := range cases {
		ma, err := multiaddr.NewMultiaddr(c.addr)
		if err != nil {
			t.Fatalf("bad test multiaddr %q: %v", c.addr, err)
		}
		if got := net_unroutable(ma); got != c.want {
			t.Errorf("net_unroutable(%q) = %v, want %v", c.addr, got, c.want)
		}
	}
}

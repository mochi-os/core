// Mochi server: the published address list carries only addresses another host
// could dial, and there is one rendering of it - net_addresses_render. Loopback
// matters most in peers_publish, which truncates its address budget.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"strings"
	"testing"

	"github.com/multiformats/go-multiaddr"
)

func addresses_test_parse(t *testing.T, texts ...string) []multiaddr.Multiaddr {
	t.Helper()
	var out []multiaddr.Multiaddr
	for _, text := range texts {
		a, err := multiaddr.NewMultiaddr(text)
		if err != nil {
			t.Fatalf("parse %q: %v", text, err)
		}
		out = append(out, a)
	}
	return out
}

// TestUndialableAddressesAreNotPublished. Loopback, unspecified and link-local
// cannot leave the machine or the segment, so no remote peer can use them.
func TestUndialableAddressesAreNotPublished(t *testing.T) {
	for _, text := range []string{
		"/ip4/127.0.0.1/tcp/1443",
		"/ip4/127.0.0.53/udp/443/quic-v1",
		"/ip6/::1/tcp/1443",
		"/ip4/0.0.0.0/tcp/1443",
		"/ip6/::/tcp/1443",
		"/ip4/169.254.7.7/tcp/1443",
		"/ip6/fe80::1/tcp/1443",
	} {
		a := addresses_test_parse(t, text)[0]
		if net_address_dialable(a) {
			t.Errorf("%s is published; no other host can reach us there, and in peers_publish it displaces one that can", text)
		}
	}
}

// TestDialableAddressesSurvive. The filter must not be a blanket "drop anything
// that is not a public IPv4": private addresses are how two servers on one
// network find each other, and a name has no IP to judge.
func TestDialableAddressesSurvive(t *testing.T) {
	for _, text := range []string{
		"/ip4/51.178.97.142/tcp/1443",
		"/ip6/2001:41d0:30f:8e00::1/udp/443/quic-v1",
		"/ip4/10.26.1.181/tcp/1444",
		"/ip4/192.168.1.10/tcp/1444",
		"/ip4/172.20.0.5/udp/1444/quic-v1",
		"/dns/mochi-os.org/tcp/443/tls/ws",
		"/dns4/example.org/tcp/443",
	} {
		a := addresses_test_parse(t, text)[0]
		if !net_address_dialable(a) {
			t.Errorf("%s was dropped; a peer on the same network dials private addresses, and a name carries no IP to judge", text)
		}
	}
}

// TestPrivateAddressesAreDeliberatelyKept states the decision on its own, so
// tightening the filter to "public only" fails here with the reason rather than
// silently breaking same-network discovery.
func TestPrivateAddressesAreDeliberatelyKept(t *testing.T) {
	original := net_id
	net_id = "12D3KooWPd68TanRD1mgWmPZJ3iRantH8z3nFBFpsFSTodsxyMu7"
	defer func() { net_id = original }()

	rendered := net_addresses_render(addresses_test_parse(t,
		"/ip4/10.26.1.181/tcp/1444",
		"/ip4/127.0.0.1/tcp/1444",
	))
	if len(rendered) != 1 || !strings.Contains(rendered[0], "10.26.1.181") {
		t.Fatalf("rendered %v; the private address must survive and the loopback one must not", rendered)
	}
}

// TestRenderStampsThePeerIdOnce: a relay circuit already ends in the target
// peer id, so an unconditional /p2p/<id> append doubles it.
func TestRenderStampsThePeerIdOnce(t *testing.T) {
	original := net_id
	net_id = "12D3KooWPd68TanRD1mgWmPZJ3iRantH8z3nFBFpsFSTodsxyMu7"
	defer func() { net_id = original }()

	circuit := "/ip4/51.178.97.142/tcp/1443/p2p/12D3KooWELMRq3U9TrJE2FJs8pcXSQotDrtXwhajTNV2CN7fWdyR" +
		"/p2p-circuit/p2p/12D3KooWPd68TanRD1mgWmPZJ3iRantH8z3nFBFpsFSTodsxyMu7"
	rendered := net_addresses_render(addresses_test_parse(t, circuit, "/ip4/51.178.97.142/tcp/1444"))
	if len(rendered) != 2 {
		t.Fatalf("rendered %d addresses, want 2: %v", len(rendered), rendered)
	}
	for _, got := range rendered {
		if n := strings.Count(got, "/p2p/"+net_id); n != 1 {
			t.Errorf("%s carries the peer id %d times, want 1", got, n)
		}
	}
}

// TestRenderDeduplicates. Two listen addresses can normalise to one string;
// publishing it twice spends two of peers_publish's sixteen slots on one route.
func TestRenderDeduplicates(t *testing.T) {
	original := net_id
	net_id = "12D3KooWPd68TanRD1mgWmPZJ3iRantH8z3nFBFpsFSTodsxyMu7"
	defer func() { net_id = original }()

	rendered := net_addresses_render(addresses_test_parse(t,
		"/ip4/51.178.97.142/tcp/1443",
		"/ip4/51.178.97.142/tcp/1443",
	))
	if len(rendered) != 1 {
		t.Errorf("rendered %d addresses for one route: %v", len(rendered), rendered)
	}
}

// TestPublishedListLeavesRoomForRoutes. The concrete consequence: yuzu
// advertises six dialable addresses and six loopback ones, so an unfiltered
// list spends half the publish cap on addresses no peer can use.
func TestPublishedListLeavesRoomForRoutes(t *testing.T) {
	original := net_id
	net_id = "12D3KooWPd68TanRD1mgWmPZJ3iRantH8z3nFBFpsFSTodsxyMu7"
	defer func() { net_id = original }()

	var texts []string
	for _, host := range []string{"51.178.97.142", "51.178.97.143", "51.178.97.144"} {
		texts = append(texts, "/ip4/"+host+"/tcp/1443", "/ip4/"+host+"/udp/443/quic-v1")
	}
	for i := 0; i < peers_publish_addresses_maximum; i++ {
		texts = append(texts, "/ip4/127.0.0.1/tcp/"+strings.Repeat("1", 1)+string(rune('0'+i%10))+"44")
	}
	rendered := net_addresses_render(addresses_test_parse(t, texts...))
	if len(rendered) > peers_publish_addresses_maximum {
		t.Fatalf("rendered %d addresses, over the publish cap of %d", len(rendered), peers_publish_addresses_maximum)
	}
	for _, got := range rendered {
		if strings.Contains(got, "/ip4/127.0.0.1/") {
			t.Errorf("%s survived; it would occupy one of the %d publish slots", got, peers_publish_addresses_maximum)
		}
	}
	if len(rendered) != 6 {
		t.Errorf("rendered %d dialable addresses, want the 6 real routes: %v", len(rendered), rendered)
	}
}

// TestPeerInfoUsesTheSharedRendering. The endpoint is anonymous by design -
// mochi.remote.peer() fetches it from other servers to learn how to dial them -
// so it cannot be gated, which makes using the one filtered rendering the whole
// of the fix.
func TestPeerInfoUsesTheSharedRendering(t *testing.T) {
	body, err := os.ReadFile("web.go")
	if err != nil {
		t.Fatalf("read web.go: %v", err)
	}
	text := string(body)
	fn := text[strings.Index(text, "func web_p2p_info("):]
	fn = fn[:strings.Index(fn, "\n}")]
	// Comments stripped: the handler's own comment names the call it replaced.
	var code strings.Builder
	for _, line := range strings.Split(fn, "\n") {
		if i := strings.Index(line, "//"); i >= 0 {
			line = line[:i]
		}
		code.WriteString(line + "\n")
	}
	fn = code.String()

	if !strings.Contains(fn, "net_addresses()") {
		t.Error("web_p2p_info does not use net_addresses, so the anonymous endpoint publishes an unfiltered list again")
	}
	if strings.Contains(fn, "net_me.Addrs()") {
		t.Error("web_p2p_info loops over net_me.Addrs() itself; that is the duplicate rendering that skipped every filter")
	}
}

// TestRealCaptureRendersCorrectly runs the renderer over an address set copied
// from a running server. A circuit's leading IP is the RELAY's, so an
// over-eager filter drops exactly the addresses a NAT-ed server depends on.
func TestRealCaptureRendersCorrectly(t *testing.T) {
	original := net_id
	net_id = "12D3KooWPd68TanRD1mgWmPZJ3iRantH8z3nFBFpsFSTodsxyMu7"
	defer func() { net_id = original }()

	const relay = "/p2p/12D3KooWELMRq3U9TrJE2FJs8pcXSQotDrtXwhajTNV2CN7fWdyR/p2p-circuit"
	keep := []string{
		"/ip4/10.26.1.181/tcp/1444",
		"/ip4/10.26.1.181/udp/1444/quic-v1",
		"/ip4/51.178.97.142/tcp/1443" + relay,
		"/ip4/51.178.97.142/udp/443/quic-v1" + relay,
		"/ip6/2001:41d0:30f:8e00::1/tcp/1443" + relay,
		"/dns/mochi-os.org/tcp/443/tls/ws" + relay,
	}
	drop := []string{
		"/ip4/127.0.0.1/tcp/1444",
		"/ip6/::1/tcp/1444",
	}
	rendered := net_addresses_render(addresses_test_parse(t, append(append([]string{}, keep...), drop...)...))

	joined := strings.Join(rendered, " ")
	for _, want := range keep {
		if !strings.Contains(joined, want) {
			t.Errorf("%s was dropped; a NAT-ed server reaches the mesh only through addresses like this", want)
		}
	}
	for _, gone := range drop {
		if strings.Contains(joined, gone) {
			t.Errorf("%s survived", gone)
		}
	}
	if len(rendered) != len(keep) {
		t.Errorf("rendered %d addresses, want %d: %v", len(rendered), len(keep), rendered)
	}
}

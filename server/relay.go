// Mochi server: circuit-relay participation.
//
// Most home servers are behind NAT and not directly dialable. libp2p's
// circuit relay lets a publicly-reachable server act as a hop: a NAT'd
// peer reserves a slot on it and advertises a /p2p-circuit/ address
// through it. The relay network is therefore what makes NAT'd servers
// reachable at all.
//
// Two halves live here:
//
//   - Serve. A server that AutoNAT has found to be publicly reachable
//     auto-enables its own relay service, growing the relay pool with
//     the network instead of funnelling every NAT'd peer through the
//     handful of hand-configured bootstrap relays. The relay service
//     is started and stopped on the running host as the reachability
//     verdict changes; the `relay` system setting (default on) is the
//     operator opt-out for those who don't want to donate the
//     bandwidth.
//
//   - Discover. AutoRelay's candidate relays come from a dynamic
//     source (net_relay_candidates) rather than a static list: the
//     bootstrap relays plus any peer that has announced it relays. A
//     relaying server sets a `relay` flag in its peers/publish; that
//     flag is not security-sensitive (a peer falsely claiming it just
//     fails to grant reservations), so it rides the plain content, not
//     the signed record.
//
// Copyright Alistair Cunningham 2026

package main

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	p2p_peer "github.com/libp2p/go-libp2p/core/peer"
	pbv2 "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/pb"
	relay "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
	multiaddr "github.com/multiformats/go-multiaddr"
)

// peer_relay_maximum_age — a peer counts as a relay candidate only if
// it announced relay within this window. A relay refreshes the flag on
// every hourly publish; one that stops being public stops announcing
// and ages out.
const peer_relay_maximum_age = 3600

var (
	relay_service      *relay.Relay
	relay_service_lock = &sync.Mutex{}

	// peer_relays maps a peer id to when it last announced it relays.
	peer_relays      = map[string]int64{}
	peer_relays_lock = &sync.Mutex{}

	relay_reservations atomic.Int64 // reservations the relay service currently holds (gauge)
	relay_circuits     atomic.Int64 // relayed connections currently open (gauge)
	relay_rejected     atomic.Int64 // reservation requests refused since startup (cumulative)
)

// relay_metrics implements libp2p's relay MetricsTracer so the relay
// service's live load can be surfaced (mochi.server.network().relaying).
// The relay calls these from its own goroutines, so the counters are
// atomic. A saturated relay (reservations near the cap, rejected climbing)
// is what leaves NAT'd peers Unreachable with no other signal.
type relay_metrics struct{}

func (relay_metrics) RelayStatus(bool)                     {}
func (relay_metrics) ConnectionOpened()                    { relay_circuits.Add(1) }
func (relay_metrics) ConnectionClosed(time.Duration)       { relay_circuits.Add(-1) }
func (relay_metrics) ConnectionRequestHandled(pbv2.Status) {}
func (relay_metrics) ReservationAllowed(renewal bool) {
	if !renewal {
		relay_reservations.Add(1)
	}
}
func (relay_metrics) ReservationClosed(count int) { relay_reservations.Add(-int64(count)) }
func (relay_metrics) ReservationRequestHandled(status pbv2.Status) {
	if status != pbv2.Status_OK {
		relay_rejected.Add(1)
	}
}
func (relay_metrics) BytesTransferred(int) {}

// relay_utilization reports the relay service's live load for
// mochi.server.network(). The reservation/circuit gauges read 0 when no
// relay is running; rejected is a since-startup total.
func relay_utilization() map[string]any {
	held := relay_reservations.Load()
	if held < 0 {
		held = 0
	}
	circuits := relay_circuits.Load()
	if circuits < 0 {
		circuits = 0
	}
	return map[string]any{
		"active": relay_enabled(),
		"reservations": map[string]any{
			"held":    held,
			"maximum": int64(relay.DefaultResources().MaxReservations),
		},
		"circuits": circuits,
		"rejected": relay_rejected.Load(),
	}
}

// relay_offered reports whether the operator permits this server to
// serve as a relay. Default on; the `relay` system setting is the
// opt-out.
func relay_offered() bool {
	return setting_get("relay", "true") == "true"
}

// relay_enabled reports whether our relay service is currently running —
// the condition under which we announce the relay flag.
func relay_enabled() bool {
	relay_service_lock.Lock()
	defer relay_service_lock.Unlock()
	return relay_service != nil
}

// relay_service_update starts or stops the relay service to match the
// current state: serve iff the operator permits it and AutoNAT has
// found us publicly reachable. Called from the reachability watcher and
// when the `relay` setting changes. Announces the transition so NAT'd
// peers learn we became (or stopped being) a relay.
func relay_service_update() {
	if net_me == nil {
		return
	}
	want := relay_offered() && net_reachable() == "public"

	relay_service_lock.Lock()
	defer relay_service_lock.Unlock()
	switch {
	case want && relay_service == nil:
		relay_reservations.Store(0)
		relay_circuits.Store(0)
		r, err := relay.New(net_me, relay.WithMetricsTracer(relay_metrics{}))
		if err != nil {
			warn("Net unable to start relay service: %v", err)
			return
		}
		relay_service = r
		info("Net relay service enabled (publicly reachable)")
		peers_publish_request()
	case !want && relay_service != nil:
		relay_service.Close()
		relay_service = nil
		relay_reservations.Store(0)
		relay_circuits.Store(0)
		info("Net relay service disabled")
		peers_publish_request()
	}
}

// relay_shutdown closes the relay service on server shutdown.
func relay_shutdown() {
	relay_service_lock.Lock()
	defer relay_service_lock.Unlock()
	if relay_service != nil {
		relay_service.Close()
		relay_service = nil
	}
}

// peer_relay_seen records that a peer announced it relays.
func peer_relay_seen(id string) {
	if id == "" || id == net_id {
		return
	}
	peer_relays_lock.Lock()
	peer_relays[id] = now()
	peer_relays_lock.Unlock()
}

// peer_relays_fresh returns the peers that announced relay recently
// enough to still be candidates.
func peer_relays_fresh() []string {
	cutoff := now() - peer_relay_maximum_age
	peer_relays_lock.Lock()
	defer peer_relays_lock.Unlock()
	var out []string
	for id, t := range peer_relays {
		if t >= cutoff {
			out = append(out, id)
		}
	}
	return out
}

// peer_relays_sweep drops stale relay flags. Called from the daily
// maintenance pass.
func peer_relays_sweep() {
	cutoff := now() - peer_relay_maximum_age
	peer_relays_lock.Lock()
	for id, t := range peer_relays {
		if t < cutoff {
			delete(peer_relays, id)
		}
	}
	peer_relays_lock.Unlock()
}

// relay_addrinfo builds the dial target for a relay candidate from its
// stored addresses, dropping the /p2p/ suffix and any circuit address —
// a relay must be reachable directly, not through another relay.
// Returns a zero AddrInfo (no addresses) when none qualify.
func relay_addrinfo(id string, addresses []string) p2p_peer.AddrInfo {
	pid, err := p2p_peer.Decode(id)
	if err != nil {
		return p2p_peer.AddrInfo{}
	}
	ai := p2p_peer.AddrInfo{ID: pid}
	for _, a := range addresses {
		ma, err := multiaddr.NewMultiaddr(strings.TrimSuffix(a, "/p2p/"+id))
		if err != nil {
			continue
		}
		if _, err := ma.ValueForProtocol(multiaddr.P_CIRCUIT); err == nil {
			continue
		}
		ai.Addrs = append(ai.Addrs, ma)
	}
	return ai
}

// net_relay_candidates is AutoRelay's peer source: it yields up to num
// relay candidates — the bootstrap relays first (always-available
// anchors), then peers that have announced they relay. Switching from a
// static bootstrap-only list to this lets a NAT'd server reserve a slot
// on any public peer in the network, not just the bootstraps.
func net_relay_candidates(ctx context.Context, num int) <-chan p2p_peer.AddrInfo {
	out := make(chan p2p_peer.AddrInfo, num)
	go func() {
		defer close(out)
		sent := 0
		send := func(ai p2p_peer.AddrInfo) bool {
			if len(ai.Addrs) == 0 {
				return sent < num
			}
			select {
			case out <- ai:
				sent++
			case <-ctx.Done():
				return false
			}
			return sent < num
		}
		for _, bp := range peers_bootstrap {
			if bp.ID == net_id {
				continue
			}
			if !send(relay_addrinfo(bp.ID, peer_address_strings(bp.addresses))) {
				return
			}
		}
		for _, id := range peer_relays_fresh() {
			peers_lock.Lock()
			addresses := peer_address_strings(peers[id].addresses)
			peers_lock.Unlock()
			if !send(relay_addrinfo(id, addresses)) {
				return
			}
		}
	}()
	return out
}

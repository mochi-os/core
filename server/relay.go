// Mochi server: circuit-relay participation.
//
// Serve: a server AutoNAT has found publicly reachable auto-enables its relay
// service; the `relay` setting is the operator opt-out. Discover: AutoRelay
// candidates come from net_relay_candidates - bootstrap relays plus peers whose
// plain (unsigned) publish carries a `relay` flag, not security-sensitive.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"math/rand/v2"
	"sort"
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

// relay_reservations_default is the relay's MaxReservations when [relay]
// reservations is unset — far above libp2p's conservative 128, since a
// reservation is just a cheap idle registered connection and a dedicated
// relay should not be a bottleneck.
const relay_reservations_default = 2048

var (
	relay_service      *relay.Relay
	relay_service_lock = &sync.Mutex{}

	// peer_relays maps a peer id to its last relay announcement: when it was
	// seen, and whether the relay advertised itself full so candidate
	// selection can avoid it.
	peer_relays      = map[string]peer_relay_record{}
	peer_relays_lock = &sync.Mutex{}

	relay_reservations atomic.Int64 // reservations the relay service currently holds (gauge)
	relay_circuits     atomic.Int64 // relayed connections currently open (gauge)
	relay_rejected     atomic.Int64 // reservation requests refused since startup (cumulative)
)

// relay_metrics implements libp2p's relay MetricsTracer, surfacing live relay
// load through mochi.server.network().relaying. The relay calls these from its
// own goroutines, so the counters are atomic.
type relay_metrics struct{}

func (relay_metrics) RelayStatus(bool)                     {}
func (relay_metrics) ConnectionOpened()                    { relay_circuits.Add(1) }
func (relay_metrics) ConnectionClosed(time.Duration)       { relay_circuits.Add(-1) }
func (relay_metrics) ConnectionRequestHandled(pbv2.Status) {}
func (relay_metrics) ReservationAllowed(renewal bool) {
	if !renewal {
		relay_reservations.Add(1)
		relay_load_changed()
	}
}
func (relay_metrics) ReservationClosed(count int) {
	relay_reservations.Add(-int64(count))
	relay_load_changed()
}
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
			"maximum": int64(ini_int("relay", "reservations", relay_reservations_default)),
		},
		"circuits": circuits,
		"rejected": relay_rejected.Load(),
	}
}

// relay_resources sizes the relay service for a dedicated relay rather than
// libp2p's bootstrap defaults. Per-connection transfer is unbounded by default:
// a ceiling below the app's own limits (1 GB per file) truncates file sharing.
func relay_resources() relay.Resources {
	rc := relay.DefaultResources()
	rc.MaxReservations = ini_int("relay", "reservations", relay_reservations_default)
	// CGNAT/ISP headroom, well above libp2p's 8/32: many home servers
	// behind one carrier-grade-NAT IP or ISP can still reserve, while one
	// actor still cannot monopolise the relay.
	rc.MaxReservationsPerIP = 64
	rc.MaxReservationsPerASN = 256

	data := atoi(ini_string("relay", "data", "0"), 0)
	duration := ini_int("relay", "duration", 0)
	if data <= 0 && duration <= 0 {
		rc.Limit = nil // unbounded per-connection transfer
		return rc
	}
	limit := relay.DefaultLimit()
	if data > 0 {
		limit.Data = data
	} else {
		limit.Data = 1 << 60 // bounded only by duration
	}
	if duration > 0 {
		limit.Duration = time.Duration(duration) * time.Second
	} else {
		limit.Duration = time.Duration(1 << 60) // bounded only by data
	}
	rc.Limit = limit
	return rc
}

// relay_offered reports whether the operator permits this server to
// serve as a relay. Default on; the `relay` system setting is the
// opt-out.
func relay_offered() bool {
	return setting_effective("relay") == "true"
}

// relay_enabled reports whether our relay service is currently running —
// the condition under which we announce the relay flag.
func relay_enabled() bool {
	relay_service_lock.Lock()
	defer relay_service_lock.Unlock()
	return relay_service != nil
}

// relay_load_percent is our relay's current reservation utilisation as a
// percentage (0-100), advertised in peers/publish so NAT'd peers can prefer
// a relay with more headroom. 0 when we run no relay.
func relay_load_percent() int {
	if !relay_enabled() {
		return 0
	}
	maximum := ini_int("relay", "reservations", relay_reservations_default)
	if maximum <= 0 {
		return 0
	}
	held := int(relay_reservations.Load())
	if held < 0 {
		held = 0
	}
	pct := held * 100 / maximum
	if pct > 100 {
		pct = 100
	}
	return pct
}

// relay_load_tier buckets a utilisation percentage into coarse tiers
// (0 comfortable, 1 busy, 2 high, 3 full) so candidate ordering is stable
// against small load changes and stale announcements.
func relay_load_tier(load int) int {
	switch {
	case load >= 95:
		return 3
	case load >= 80:
		return 2
	case load >= 50:
		return 1
	default:
		return 0
	}
}

// relay_announced_tier is the load tier our last publish reflected; only
// relay_load_changed touches it.
var relay_announced_tier atomic.Int32

// relay_load_changed requests a republish when our advertised load tier moves,
// so peers learn it without waiting for the hourly publish. Cheap on every
// reservation change: only tier boundaries republish.
func relay_load_changed() {
	tier := int32(relay_load_tier(relay_load_percent()))
	if relay_announced_tier.Swap(tier) != tier {
		peers_publish_request()
	}
}

func relay_service_update() {
	if net_me == nil {
		return
	}
	want := relay_offered() && net_reachable_public.Load()

	relay_service_lock.Lock()
	defer relay_service_lock.Unlock()
	switch {
	case want && relay_service == nil:
		relay_reservations.Store(0)
		relay_circuits.Store(0)
		r, err := relay.New(net_me, relay.WithResources(relay_resources()), relay.WithMetricsTracer(relay_metrics{}))
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

// relay_rejected_alerted is the rejection count relay_saturation_check last
// warned at; only that function (a single goroutine) touches it.
var relay_rejected_alerted int64

// relay_manager periodically alerts when the running relay is turning peers
// away, so the operator can add capacity before NAT'd peers go unreachable.
func relay_manager() {
	for range time.Tick(15 * time.Minute) {
		relay_saturation_check()
	}
}

// relay_saturation_threshold: at or above this fraction of MaxReservations a
// refusal means the relay is genuinely full. Below it, refusals come from
// libp2p's per-peer/IP/ASN sub-limits and are not actionable.
const relay_saturation_threshold = 0.8

func relay_saturation_alert(rejected, alerted, held, maximum int64) bool {
	if rejected <= alerted || maximum <= 0 {
		return false
	}
	return float64(held) >= relay_saturation_threshold*float64(maximum)
}

// relay_saturation_check warns the admin about refused reservations while near
// capacity. The watermark advances either way, so a benign low-utilisation
// refusal is remembered but never alerts; warn() throttles the email hourly.
func relay_saturation_check() {
	if !relay_enabled() {
		relay_rejected_alerted = relay_rejected.Load()
		return
	}
	rejected := relay_rejected.Load()
	held := relay_reservations.Load()
	maximum := int64(ini_int("relay", "reservations", relay_reservations_default))
	if relay_saturation_alert(rejected, relay_rejected_alerted, held, maximum) {
		warn("Relay service refused %d reservation(s) since startup and is near capacity (%d of %d slots held) — it is turning NAT'd peers away; raise [relay] reservations or stand up more relays",
			rejected, held, maximum)
	}
	relay_rejected_alerted = rejected
}

// peer_relay_record is one peer's last relay announcement.
type peer_relay_record struct {
	seen int64
	load int // advertised reservation utilisation, 0-100 (100 = full)
}

// peer_relay_seen records that a peer announced it relays.
func peer_relay_seen(id string, load int) {
	if id == "" || id == net_id {
		return
	}
	if load < 0 {
		load = 0
	} else if load > 100 {
		load = 100
	}
	peer_relays_lock.Lock()
	peer_relays[id] = peer_relay_record{seen: now(), load: load}
	peer_relays_lock.Unlock()
}

// relay_candidate_load returns a relay's last-advertised reservation
// utilisation (0-100); 0 when unknown, so an un-announced relay is treated
// as having capacity.
func relay_candidate_load(id string) int {
	peer_relays_lock.Lock()
	defer peer_relays_lock.Unlock()
	return peer_relays[id].load
}

// peer_relays_fresh returns the peers that announced relay recently
// enough to still be candidates.
func peer_relays_fresh() []string {
	cutoff := now() - peer_relay_maximum_age
	peer_relays_lock.Lock()
	defer peer_relays_lock.Unlock()
	var out []string
	for id, r := range peer_relays {
		if r.seen >= cutoff {
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
	for id, r := range peer_relays {
		if r.seen < cutoff {
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

// relay_candidate_latency returns the libp2p-measured RTT to a peer (0 when
// there is no sample yet), used to prefer the closest relay.
func relay_candidate_latency(id string) time.Duration {
	if net_me == nil {
		return 0
	}
	pid, err := p2p_peer.Decode(id)
	if err != nil {
		return 0
	}
	return net_me.Peerstore().LatencyEWMA(pid)
}

// relay_latency_bucket coarse-buckets a measured RTT (50 ms granularity) so
// relays of similar distance tie and the shuffle spreads peers across them;
// an unmeasured relay (0) sorts after all measured ones.
func relay_latency_bucket(latency time.Duration) int {
	if latency <= 0 {
		return 1 << 30
	}
	return int(latency / (50 * time.Millisecond))
}

// net_relay_candidates is the AutoRelay candidate source: bootstrap relays plus
// peers that recently advertised they relay, minus known-unreachable ones,
// ordered by load tier then latency. Full relays sort last rather than dropping
// out.
func net_relay_candidates(ctx context.Context, num int) <-chan p2p_peer.AddrInfo {
	out := make(chan p2p_peer.AddrInfo, num)
	go func() {
		defer close(out)

		type candidate struct {
			ai     p2p_peer.AddrInfo
			tier   int
			bucket int
		}
		var cands []candidate
		seen := map[string]bool{}
		add := func(id string, addresses []string) {
			if id == "" || id == net_id || seen[id] || peer_is_silent(id) {
				return
			}
			ai := relay_addrinfo(id, addresses)
			if len(ai.Addrs) == 0 {
				return
			}
			seen[id] = true
			cands = append(cands, candidate{
				ai:     ai,
				tier:   relay_load_tier(relay_candidate_load(id)),
				bucket: relay_latency_bucket(relay_candidate_latency(id)),
			})
		}

		for _, bp := range peers_bootstrap {
			add(bp.ID, peer_address_strings(bp.addresses))
		}
		for _, id := range peer_relays_fresh() {
			peers_lock.Lock()
			addresses := peer_address_strings(peers[id].addresses)
			peers_lock.Unlock()
			add(id, addresses)
		}

		// Shuffle first so relays that tie on (load tier, latency bucket)
		// land in random order — light jitter that stops every peer piling
		// onto the same one. Then prefer the lowest load tier, then the
		// nearest latency bucket.
		rand.Shuffle(len(cands), func(i, j int) { cands[i], cands[j] = cands[j], cands[i] })
		sort.SliceStable(cands, func(i, j int) bool {
			if cands[i].tier != cands[j].tier {
				return cands[i].tier < cands[j].tier
			}
			return cands[i].bucket < cands[j].bucket
		})

		for i, c := range cands {
			if i >= num {
				return
			}
			select {
			case out <- c.ai:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

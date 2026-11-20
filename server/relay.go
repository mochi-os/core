// Mochi server: circuit-relay participation.
//
// Serve: a server AutoNAT has found publicly reachable auto-enables its relay
// service; the `relay` setting is the operator opt-out. It serves only peers
// that have completed a /mochi/2 hello with this host. Discover: AutoRelay
// candidates come from net_relay_candidates - the configured (bootstrap)
// relays, and peers whose publish carries a `relay` flag only to make up for
// configured relays that are unreachable, full or not relaying.
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

	p2p_network "github.com/libp2p/go-libp2p/core/network"
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

const (
	// relay_wanted is how many relays AutoRelay holds reservations on, and how
	// many candidates it waits for before reserving: its default number of
	// relays, set explicitly because net_relay_candidates offers no more than
	// that while the configured relays have room.
	relay_wanted = 2

	// relay_candidates_maximum is how many candidates AutoRelay buffers and
	// asks the source for.
	relay_candidates_maximum = 20

	// relay_probe_timeout bounds a relay admission probe.
	relay_probe_timeout = 5 * time.Second

	// peer_greetings_maximum bounds the record of peers that completed a
	// /mochi/2 hello with this host; past it the oldest is dropped. A dropped
	// peer is probed again when it next asks the relay for anything.
	peer_greetings_maximum = 10000
)

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

	// peer_greetings maps a peer to the last time it completed a /mochi/2
	// hello with this host, either way round: the evidence the relay admits
	// on. Its own record rather than membership of peers, which ids named by
	// announcements and mDNS answers, and any libp2p client that opens a
	// stream, can join.
	peer_greetings      = map[string]int64{}
	peer_greetings_lock = &sync.Mutex{}
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

// relay_access admits only peers that have shown themselves Mochi servers, by
// completing a /mochi/2 hello with this host. libp2p installs no ACL by
// default, so without this every circuit-v2 client on the internet can reserve
// a slot and tunnel third-party traffic over the operator's bandwidth. The
// unbounded per-circuit limit above is written for Mochi-to-Mochi file
// sharing; this is what makes that the only traffic it applies to. Membership
// of peers is no such evidence: any libp2p node that opens a stream joins it.
type relay_access struct{}

func (relay_access) AllowReserve(id p2p_peer.ID, address multiaddr.Multiaddr) bool {
	return relay_admitted(id)
}

func (relay_access) AllowConnect(source p2p_peer.ID, address multiaddr.Multiaddr, destination p2p_peer.ID) bool {
	return relay_admitted(source) && relay_admitted(destination)
}

// relay_admitted reports whether the relay may serve a peer: one that has
// completed a hello with this host, or that completes one now, over the
// connection it holds to this host to ask. A server that reserves on a relay,
// or dials through one, has no other reason to have spoken /mochi/2 with it
// first.
func relay_admitted(id p2p_peer.ID) bool {
	return peer_greeted(id.String()) || relay_probe(id)
}

// relay_probe asks a connected peer for a /mochi/2 hello: a messages stream
// opened over its connection, its hello read, caps written to complete the
// handshake, and the stream closed with nothing sent. Never dials. Reports
// whether the peer answered as a Mochi server.
func relay_probe(id p2p_peer.ID) bool {
	if net_me == nil || !rate_limit_relay_probe.allow(id.String()) {
		return false
	}
	ctx, cancel := context.WithTimeout(net_context, relay_probe_timeout)
	defer cancel()
	stream, err := net_me.NewStream(p2p_network.WithNoDial(ctx, "relay admission probe"), id, protocol_messages)
	if err != nil {
		debug("Relay probe of %q refused: %v", id, err)
		return false
	}
	_ = stream.SetDeadline(time.Now().Add(relay_probe_timeout))
	hello, err := hello_read(stream, 2)
	if err != nil {
		stream.Reset()
		debug("Relay probe of %q found no Mochi server: %v", id, err)
		return false
	}
	challenge, err := hello_challenge()
	if err != nil {
		stream.Reset()
		return false
	}
	if err := caps_write(stream, codec_intersect(receiver_codecs(), hello.Codecs), features_intersect(receiver_features(), hello.Features), challenge); err != nil {
		stream.Reset()
		return false
	}
	stream.Close()
	peer_greet(id.String())
	return true
}

// peer_greet records that a peer completed a /mochi/2 hello with this host.
// At the bound the oldest record goes.
func peer_greet(id string) {
	if id == "" || id == net_id {
		return
	}
	t := now()
	peer_greetings_lock.Lock()
	defer peer_greetings_lock.Unlock()
	if _, found := peer_greetings[id]; !found && len(peer_greetings) >= peer_greetings_maximum {
		oldest, at := "", int64(0)
		for other, seen := range peer_greetings {
			if oldest == "" || seen < at {
				oldest, at = other, seen
			}
		}
		delete(peer_greetings, oldest)
	}
	peer_greetings[id] = t
}

// peer_greeted reports whether a peer completed a /mochi/2 hello with this host
// within peer_expiry.
func peer_greeted(id string) bool {
	peer_greetings_lock.Lock()
	defer peer_greetings_lock.Unlock()
	seen, found := peer_greetings[id]
	return found && seen >= now()-peer_expiry
}

// peer_greetings_sweep drops greetings older than peer_expiry. Called from
// the daily maintenance pass.
func peer_greetings_sweep() {
	cutoff := now() - peer_expiry
	peer_greetings_lock.Lock()
	for id, seen := range peer_greetings {
		if seen < cutoff {
			delete(peer_greetings, id)
		}
	}
	peer_greetings_lock.Unlock()
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
		r, err := relay.New(net_me, relay.WithResources(relay_resources()),
			relay.WithACL(relay_access{}), relay.WithMetricsTracer(relay_metrics{}))
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

// relay_reachable reports whether a relay is not known to be unreachable: this
// host is connected to it, or holds an address for it that has not failed a
// dial since it last connected, or knows none to have tried.
func relay_reachable(id string) bool {
	if net_me != nil {
		if pid, err := p2p_peer.Decode(id); err == nil && net_me.Network().Connectedness(pid) == p2p_network.Connected {
			return true
		}
	}
	peers_lock.Lock()
	defer peers_lock.Unlock()
	addresses := peers[id].addresses
	if len(addresses) == 0 {
		return true
	}
	for _, a := range addresses {
		if a.Failure == 0 {
			return true
		}
	}
	return false
}

// net_relay_candidates is the AutoRelay candidate source. AutoRelay reserves on
// candidates picked at random from what it is given, so while at least
// relay_wanted of the configured relays are reachable and announcing a relay
// with room, those are all it is given; a relay that merely announces itself
// could otherwise land both reservations and decide whether this server can be
// reached at all. Only to make up the shortfall are peers that
// announced a relay flag added, best first - lowest load tier, then nearest
// latency bucket - and after them the configured relays left out.
func net_relay_candidates(ctx context.Context, num int) <-chan p2p_peer.AddrInfo {
	out := make(chan p2p_peer.AddrInfo, num)
	go func() {
		defer close(out)

		type candidate struct {
			ai     p2p_peer.AddrInfo
			tier   int
			bucket int
		}
		seen := map[string]bool{}
		make_candidate := func(id string, addresses []string) (candidate, bool) {
			if id == "" || id == net_id || seen[id] {
				return candidate{}, false
			}
			ai := relay_addrinfo(id, addresses)
			if len(ai.Addrs) == 0 {
				return candidate{}, false
			}
			seen[id] = true
			return candidate{
				ai:     ai,
				tier:   relay_load_tier(relay_candidate_load(id)),
				bucket: relay_latency_bucket(relay_candidate_latency(id)),
			}, true
		}
		// Shuffle first so relays that tie on (load tier, latency bucket) land
		// in random order — light jitter that stops every peer piling onto the
		// same one. Then prefer the lowest load tier, then the nearest latency
		// bucket.
		order := func(list []candidate) {
			rand.Shuffle(len(list), func(i, j int) { list[i], list[j] = list[j], list[i] })
			sort.SliceStable(list, func(i, j int) bool {
				if list[i].tier != list[j].tier {
					return list[i].tier < list[j].tier
				}
				return list[i].bucket < list[j].bucket
			})
		}

		// Configured relays that announce a relay with room (below the full
		// tier) and are not known unreachable; the rest - full, unreachable, or
		// running no relay this host has heard of - wait behind the announced
		// ones.
		fresh := map[string]bool{}
		for _, id := range peer_relays_fresh() {
			fresh[id] = true
		}
		var configured, announced, others []candidate
		for _, bp := range peers_bootstrap {
			c, ok := make_candidate(bp.ID, peer_address_strings(bp.addresses))
			if !ok {
				continue
			}
			if fresh[bp.ID] && c.tier < relay_load_tier(100) && relay_reachable(bp.ID) {
				configured = append(configured, c)
			} else {
				others = append(others, c)
			}
		}
		for id := range fresh {
			if peer_is_silent(id) {
				continue
			}
			peers_lock.Lock()
			addresses := peer_address_strings(peers[id].addresses)
			peers_lock.Unlock()
			if c, ok := make_candidate(id, addresses); ok {
				announced = append(announced, c)
			}
		}

		order(configured)
		candidates := configured
		if shortfall := relay_wanted - len(configured); shortfall > 0 {
			order(announced)
			order(others)
			extras := append(announced, others...)
			candidates = append(candidates, extras[:min(shortfall, len(extras))]...)
		}

		for i, c := range candidates {
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

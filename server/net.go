// Mochi server: p2p
// Copyright Alistair Cunningham 2024-2026

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	p2p "github.com/libp2p/go-libp2p"
	p2p_pubsub "github.com/libp2p/go-libp2p-pubsub"
	p2p_crypto "github.com/libp2p/go-libp2p/core/crypto"
	p2p_event "github.com/libp2p/go-libp2p/core/event"
	p2p_host "github.com/libp2p/go-libp2p/core/host"
	p2p_network "github.com/libp2p/go-libp2p/core/network"
	p2p_peer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	p2p_eventbus "github.com/libp2p/go-libp2p/p2p/host/eventbus"
	p2p_rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	p2p_ping "github.com/libp2p/go-libp2p/p2p/protocol/ping"
	multiaddr "github.com/multiformats/go-multiaddr"
)

type mdns_notifee struct {
	h p2p_host.Host
}

var (
	net_context  = context.Background()
	net_id       string
	net_private  p2p_crypto.PrivKey
	net_me       p2p_host.Host
	net_pubsub_1 *p2p_pubsub.Topic
	net_pinger   *p2p_ping.PingService
)

// Peer discovered using multicast DNS
func (n *mdns_notifee) HandlePeerFound(p p2p_peer.AddrInfo) {
	for _, pa := range p.Addrs {
		debug("Net received mDNS event from %q at %q", p.ID.String(), pa.String()+"/p2p/"+p.ID.String())
		peer_discovered_address(p.ID.String(), pa.String()+"/p2p/"+p.ID.String())
		peer_connect(p.ID.String())
	}
}

// Connect to a peer
func net_connect(peer string, addresses []string) bool {
	//debug("Net connecting to peer %q at %v", peer, addresses)

	// Defensive: a send_peer goroutine spawned before net_start
	// initialized net_me would panic on the Connect() call below.
	// Pre-p2p emit sites should be reordered (see main.go ordering),
	// but guard here too so the class of race is robust against
	// future regressions.
	if net_me == nil {
		return false
	}

	var err error

	var ai p2p_peer.AddrInfo
	ai.ID, err = p2p_peer.Decode(peer)
	if err != nil {
		warn("Net ignoring invalid peer ID %q: %v", peer, err)
		return false
	}

	for _, address := range addresses {
		ma, err := multiaddr.NewMultiaddr(address)
		if err != nil {
			warn("Net ignoring invalid peer address %q: %v", address, err)
			continue
		}

		i, err := p2p_peer.AddrInfoFromP2pAddr(ma)
		if err != nil {
			warn("Net ignoring invalid multiaddress: %v", err)
			continue
		}

		ai.Addrs = append(ai.Addrs, i.Addrs...)
	}

	if len(ai.Addrs) == 0 {
		warn("Net peer %q has no valid addresses", peer)
		return false
	}

	err = net_me.Connect(net_context, ai)
	if err != nil {
		//debug("Net error connecting to %q: %s", peer, err)
		return false
	}

	//debug("Net connected to peer %q", peer)
	return true
}

// Join pubsubs
func net_pubsubs() {
	s := must(net_pubsub_1.Subscribe())

	for {
		m, err := s.Next(net_context)
		if err != nil {
			warn("Net pubsub error: %v", err)
			continue
		}
		peer := m.ReceivedFrom.String()
		if peer != net_id {
			// Rate limit inbound pubsub messages per peer (skip
			// bootstrap and paired peers — both are trusted).
			if !peer_is_bootstrap(peer) && !peer_is_pair(peer) && !rate_limit_pubsub_in.allow(peer) {
				debug("Net pubsub rate limited peer %q", peer)
				continue
			}
			//debug("Net received pubsub event from peer %q", peer)
			stream_receive(stream_rw(io.NopCloser(bytes.NewReader(m.Data)), nil), 1, peer)
			peer_discovered(peer)
			peer_connect(peer)
		}
	}
}

// Receive event with protocol version 1 from p2p stream
func net_receive_1(s p2p_network.Stream) {
	defer s.Close()
	peer := s.Conn().RemotePeer().String()

	// Rate limit incoming streams per peer (skip bootstrap and paired
	// peers — both are trusted infrastructure, not anonymous senders).
	if !peer_is_bootstrap(peer) && !peer_is_pair(peer) && !rate_limit_p2p.allow(peer) {
		debug("Net rate limited peer %q", peer)
		return
	}

	address := s.Conn().RemoteMultiaddr().String() + "/p2p/" + peer
	//debug("Net stream from %q at %q", peer, address)

	st := stream_rw(s, s)
	st.remote = s.Conn().RemoteMultiaddr().String()
	stream_receive(st, 1, peer)
	peer_discovered_address(peer, address)
}

// Start p2p
func net_start() {
	// Read or create private/public key pair
	net_dir := filepath.Join(data_dir, "p2p")
	key_path := filepath.Join(net_dir, "private.key")
	if file_exists(key_path) {
		key_bytes, err := os.ReadFile(key_path)
		if err != nil {
			panic(fmt.Sprintf("Net failed to read private key: %v", err))
		}
		net_private = must(p2p_crypto.UnmarshalPrivateKey(key_bytes))
	} else {
		var err error
		net_private, _, err = p2p_crypto.GenerateKeyPairWithReader(p2p_crypto.Ed25519, 256, rand.Reader)
		if err != nil {
			panic(fmt.Sprintf("Net failed to generate key pair: %v", err))
		}
		p, err := p2p_crypto.MarshalPrivateKey(net_private)
		if err != nil {
			panic(fmt.Sprintf("Net failed to marshal private key: %v", err))
		}
		if err := os.MkdirAll(net_dir, 0755); err != nil {
			panic(fmt.Sprintf("Net failed to create directory: %v", err))
		}
		if err := os.WriteFile(key_path, p, 0600); err != nil {
			panic(fmt.Sprintf("Net failed to write private key: %v", err))
		}
	}

	// Configure resource manager with higher limits
	limits := p2p_rcmgr.DefaultLimits
	limits.SystemBaseLimit.Streams = 4096
	limits.SystemBaseLimit.StreamsInbound = 2048
	limits.SystemBaseLimit.StreamsOutbound = 2048
	limits.SystemBaseLimit.Conns = 256
	limits.SystemBaseLimit.ConnsInbound = 128
	limits.SystemBaseLimit.ConnsOutbound = 128
	limits.ServiceBaseLimit.Streams = 1024
	limits.ServiceBaseLimit.StreamsInbound = 512
	limits.ServiceBaseLimit.StreamsOutbound = 512
	limits.ProtocolBaseLimit.Streams = 1024
	limits.ProtocolBaseLimit.StreamsInbound = 512
	limits.ProtocolBaseLimit.StreamsOutbound = 512
	limits.PeerBaseLimit.Streams = 256
	limits.PeerBaseLimit.StreamsInbound = 128
	limits.PeerBaseLimit.StreamsOutbound = 128
	limiter := p2p_rcmgr.NewFixedLimiter(limits.AutoScale())
	rm, err := p2p_rcmgr.NewResourceManager(limiter)
	if err != nil {
		panic(fmt.Sprintf("Net failed to create resource manager: %v", err))
	}

	// Create p2p instance
	port := ini_int("p2p", "port", 1443)
	opts := []p2p.Option{
		p2p.ListenAddrStrings(
			fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", port),
			fmt.Sprintf("/ip6/::/tcp/%d", port),
			fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic-v1", port),
			fmt.Sprintf("/ip6/::/udp/%d/quic-v1", port)),
		p2p.Identity(net_private),
		p2p.ResourceManager(rm),
		p2p.NATPortMap(),
		p2p.EnableAutoNATv2(),
		p2p.EnableNATService(),
		p2p.EnableHolePunching(),
	}

	// AutoRelay: use bootstrap peers as relays when behind NAT
	var relayPeers []p2p_peer.AddrInfo
	for _, bp := range peers_bootstrap {
		pid, err := p2p_peer.Decode(bp.ID)
		if err != nil {
			continue
		}
		ai := p2p_peer.AddrInfo{ID: pid}
		for _, a := range bp.addresses {
			ma, err := multiaddr.NewMultiaddr(strings.TrimSuffix(a.Address, "/p2p/"+bp.ID))
			if err != nil {
				continue
			}
			ai.Addrs = append(ai.Addrs, ma)
		}
		if len(ai.Addrs) > 0 {
			relayPeers = append(relayPeers, ai)
		}
	}
	if len(relayPeers) > 0 {
		opts = append(opts, p2p.EnableAutoRelayWithStaticRelays(relayPeers))
	}

	// Relay server: serve as relay for NAT peers when configured
	if ini_bool("p2p", "relay", false) {
		opts = append(opts, p2p.EnableRelayService())
		info("Net relay service enabled")
	}

	net_me = must(p2p.New(opts...))
	net_id = net_me.ID().String()
	info("Net listening on port %d with id %q", port, net_id)

	// Listen for connecting peers
	net_me.SetStreamHandler("/mochi/1", net_receive_1)

	// Initialize ping service for keepalive
	net_pinger = p2p_ping.NewPingService(net_me)

	// Watch event bus for disconnecting peers
	go net_watch_disconnect()

	// Start keepalive ping manager
	go net_ping_manager()

	// Add bootstrap peers
	for _, p := range peers_bootstrap {
		if p.ID != net_id {
			debug("Adding bootstrap peer %q at %v", p.ID, peer_address_strings(p.addresses))
			peer_add_known(p.ID, peer_address_strings(p.addresses))
			go peer_connect(p.ID)
		}
	}

	// Add peers from database
	peers_add_from_db(100)

	// Listen via multicast DNS. Best-effort: hosts without a usable IPv4/IPv6
	// multicast interface (containers under qemu, certain k8s CNI plugins,
	// firewalled networks) still reach peers via the DHT and bootstrap nodes,
	// so a startup failure here shouldn't take the server down.
	if err := mdns.NewMdnsService(net_me, "mochi", &mdns_notifee{h: net_me}).Start(); err != nil {
		warn("mDNS peer discovery disabled: %v", err)
	}

	// Start pubsubs
	gs := must(p2p_pubsub.NewGossipSub(net_context, net_me))
	net_pubsub_1 = must(gs.Join("mochi/1"))
	go net_pubsubs()
}

// Create stream to an already connected peer
func net_stream(peer string) *Stream {
	p, err := p2p_peer.Decode(peer)
	if err != nil {
		warn("Net invalid peer %q: %v", peer, err)
		return nil
	}

	s, err := net_me.NewStream(net_context, p, "/mochi/1")
	if err != nil {
		info("Net unable to create stream to %q: %v", peer, err)
		return nil
	}

	return stream_rw(io.ReadCloser(s), io.WriteCloser(s))
}

// Watch event bus for disconnecting peers
func net_watch_disconnect() {
	sub, err := net_me.EventBus().Subscribe(&p2p_event.EvtPeerConnectednessChanged{}, p2p_eventbus.Name("disconnect"))
	if err != nil {
		warn("Net unable to subscribe to event bus: %v", err)
		return
	}
	defer sub.Close()

	for e := range sub.Out() {
		c := e.(p2p_event.EvtPeerConnectednessChanged)
		if c.Connectedness == p2p_network.NotConnected {
			peer_disconnected(c.Peer.String())
		}
	}
}

// Ping connected peers periodically to detect dead connections
func net_ping_manager() {
	for range time.Tick(30 * time.Second) {
		peers_lock.Lock()
		connected := []string{}
		for id, p := range peers {
			if p.connected {
				connected = append(connected, id)
			}
		}
		peers_lock.Unlock()

		for _, id := range connected {
			go net_ping_peer(id)
		}
	}
}

// Ping a single peer and mark disconnected if failed
func net_ping_peer(id string) {
	p, err := p2p_peer.Decode(id)
	if err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(net_context, 10*time.Second)
	defer cancel()

	result := <-net_pinger.Ping(ctx, p)
	if result.Error != nil {
		debug("Net ping failed for peer %q: %v", id, result.Error)
		peer_disconnected(id)
	} else {
		//debug("Net ping ok for peer %q: %v", id, result.RTT)
	}
}

// Sign data with this server's libp2p host key. Used for core-scope replication
// ops that aren't tied to a user identity (apps.db / settings.db / domains.db).
// User-scoped ops sign with entity_sign() instead.
func server_sign(data []byte) []byte {
	sig, err := net_private.Sign(data)
	if err != nil {
		warn("server_sign failed: %v", err)
		return nil
	}
	return sig
}

// Verify data signed by another server's libp2p host key. `peer` is the
// base58-encoded libp2p peer ID (whose public key is recoverable from it for
// ed25519 keys, which is what Mochi uses).
func server_verify(peer string, data, sig []byte) bool {
	id, err := p2p_peer.Decode(peer)
	if err != nil {
		return false
	}
	pub, err := id.ExtractPublicKey()
	if err != nil {
		return false
	}
	ok, err := pub.Verify(data, sig)
	return err == nil && ok
}

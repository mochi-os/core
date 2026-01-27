// Mochi server: p2p
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
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
	"io"
	"os"
	"time"
)

type mdns_notifee struct {
	h p2p_host.Host
}

var (
	p2p_context  = context.Background()
	p2p_id       string
	p2p_me       p2p_host.Host
	p2p_pubsub_1 *p2p_pubsub.Topic
	p2p_pinger   *p2p_ping.PingService
)

// Peer discovered using multicast DNS
func (n *mdns_notifee) HandlePeerFound(p p2p_peer.AddrInfo) {
	for _, pa := range p.Addrs {
		debug("P2P received mDNS event from %q at %q", p.ID.String(), pa.String()+"/p2p/"+p.ID.String())
		peer_discovered_address(p.ID.String(), pa.String()+"/p2p/"+p.ID.String())
		peer_connect(p.ID.String())
	}
}

// Connect to a peer
func p2p_connect(peer string, addresses []string) bool {
	debug("P2P connecting to peer %q at %v", peer, addresses)
	var err error

	var ai p2p_peer.AddrInfo
	ai.ID, err = p2p_peer.Decode(peer)
	if err != nil {
		warn("P2P ignoring invalid peer ID %q: %v", peer, err)
		return false
	}

	for _, address := range addresses {
		ma, err := multiaddr.NewMultiaddr(address)
		if err != nil {
			warn("P2P ignoring invalid peer address %q: %v", address, err)
			continue
		}

		i, err := p2p_peer.AddrInfoFromP2pAddr(ma)
		if err != nil {
			warn("P2P ignoring invalid multiaddress: %v", err)
			continue
		}

		ai.Addrs = append(ai.Addrs, i.Addrs...)
	}

	if len(ai.Addrs) == 0 {
		warn("P2P peer %q has no valid addresses", peer)
		return false
	}

	err = p2p_me.Connect(p2p_context, ai)
	if err != nil {
		info("P2P error connecting to %q: %v", peer, err)
		return false
	}

	debug("P2P connected to peer %q", peer)
	return true
}

// Join pubsubs
func p2p_pubsubs() {
	s := must(p2p_pubsub_1.Subscribe())

	for {
		m, err := s.Next(p2p_context)
		if err != nil {
			warn("P2P pubsub error: %v", err)
			continue
		}
		peer := m.ReceivedFrom.String()
		if peer != p2p_id {
			// Rate limit inbound pubsub messages per peer (skip bootstrap peers)
			if !peer_is_bootstrap(peer) && !rate_limit_pubsub_in.allow(peer) {
				debug("P2P pubsub rate limited peer %q", peer)
				continue
			}
			debug("P2P received pubsub event from peer %q", peer)
			stream_receive(stream_rw(io.NopCloser(bytes.NewReader(m.Data)), nil), 1, peer)
			peer_discovered(peer)
			peer_connect(peer)
		}
	}
}

// Receive event with protocol version 1 from p2p stream
func p2p_receive_1(s p2p_network.Stream) {
	defer s.Close()
	peer := s.Conn().RemotePeer().String()

	// Rate limit incoming streams per peer (skip bootstrap peers)
	if !peer_is_bootstrap(peer) && !rate_limit_p2p.allow(peer) {
		debug("P2P rate limited peer %q", peer)
		return
	}

	address := s.Conn().RemoteMultiaddr().String() + "/p2p/" + peer
	//debug("P2P stream from %q at %q", peer, address)

	st := stream_rw(s, s)
	st.remote = s.Conn().RemoteMultiaddr().String()
	stream_receive(st, 1, peer)
	peer_discovered_address(peer, address)
}

// Start p2p
func p2p_start() {
	// Read or create private/public key pair
	var private p2p_crypto.PrivKey
	if file_exists(data_dir + "/p2p/private.key") {
		private = must(p2p_crypto.UnmarshalPrivateKey(file_read(data_dir + "/p2p/private.key")))
	} else {
		private, _, err := p2p_crypto.GenerateKeyPairWithReader(p2p_crypto.Ed25519, 256, rand.Reader)
		if err != nil {
			panic(fmt.Sprintf("P2P failed to generate key pair: %v", err))
		}
		p, err := p2p_crypto.MarshalPrivateKey(private)
		if err != nil {
			panic(fmt.Sprintf("P2P failed to marshal private key: %v", err))
		}
		file_mkdir(data_dir + "/p2p")
		if err := os.WriteFile(data_dir+"/p2p/private.key", p, 0600); err != nil {
			panic(fmt.Sprintf("P2P failed to write private key: %v", err))
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
		panic(fmt.Sprintf("P2P failed to create resource manager: %v", err))
	}

	// Create p2p instance
	port := ini_int("p2p", "port", 1443)
	p2p_me = must(p2p.New(p2p.ListenAddrStrings(
		fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", port),
		fmt.Sprintf("/ip6/::/tcp/%d", port),
		fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic-v1", port),
		fmt.Sprintf("/ip6/::/udp/%d/quic-v1", port)), p2p.Identity(private), p2p.ResourceManager(rm)))
	p2p_id = p2p_me.ID().String()
	info("P2P listening on port %d with id %q", port, p2p_id)

	// Listen for connecting peers
	p2p_me.SetStreamHandler("/mochi/1", p2p_receive_1)

	// Initialize ping service for keepalive
	p2p_pinger = p2p_ping.NewPingService(p2p_me)

	// Watch event bus for disconnecting peers
	go p2p_watch_disconnect()

	// Start keepalive ping manager
	go p2p_ping_manager()

	// Add bootstrap peers
	for _, p := range peers_bootstrap {
		if p.ID != p2p_id {
			debug("Adding bootstrap peer %q at %v", p.ID, p.addresses)
			peer_add_known(p.ID, p.addresses)
			go peer_connect(p.ID)
		}
	}

	// Add peers from database
	peers_add_from_db(100)

	// Listen via multicast DNS
	must(mdns.NewMdnsService(p2p_me, "mochi", &mdns_notifee{h: p2p_me}).Start())

	// Start pubsubs
	gs := must(p2p_pubsub.NewGossipSub(p2p_context, p2p_me))
	p2p_pubsub_1 = must(gs.Join("mochi/1"))
	go p2p_pubsubs()
}

// Create stream to an already connected peer
func p2p_stream(peer string) *Stream {
	p, err := p2p_peer.Decode(peer)
	if err != nil {
		warn("P2P invalid peer %q: %v", peer, err)
		return nil
	}

	s, err := p2p_me.NewStream(p2p_context, p, "/mochi/1")
	if err != nil {
		warn("P2P unable to create stream to %q: %v'", peer, err)
		return nil
	}

	return stream_rw(io.ReadCloser(s), io.WriteCloser(s))
}

// Watch event bus for disconnecting peers
func p2p_watch_disconnect() {
	sub, err := p2p_me.EventBus().Subscribe(&p2p_event.EvtPeerConnectednessChanged{}, p2p_eventbus.Name("disconnect"))
	if err != nil {
		warn("P2P unable to subscribe to event bus: %v", err)
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
func p2p_ping_manager() {
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
			go p2p_ping_peer(id)
		}
	}
}

// Ping a single peer and mark disconnected if failed
func p2p_ping_peer(id string) {
	p, err := p2p_peer.Decode(id)
	if err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(p2p_context, 10*time.Second)
	defer cancel()

	result := <-p2p_pinger.Ping(ctx, p)
	if result.Error != nil {
		debug("P2P ping failed for peer %q: %v", id, result.Error)
		peer_disconnected(id)
	} else {
		//debug("P2P ping ok for peer %q: %v", id, result.RTT)
	}
}

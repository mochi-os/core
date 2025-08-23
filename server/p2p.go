// Mochi server: p2p
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	p2p "github.com/libp2p/go-libp2p"
	p2p_crypto "github.com/libp2p/go-libp2p/core/crypto"
	p2p_event "github.com/libp2p/go-libp2p/core/event"
	p2p_eventbus "github.com/libp2p/go-libp2p/p2p/host/eventbus"
	p2p_host "github.com/libp2p/go-libp2p/core/host"
	p2p_network "github.com/libp2p/go-libp2p/core/network"
	p2p_peer "github.com/libp2p/go-libp2p/core/peer"
	p2p_pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	multiaddr "github.com/multiformats/go-multiaddr"
)

type mdns_notifee struct {
	h p2p_host.Host
}

var (
	p2p_context         = context.Background()
	p2p_id              string
	p2p_me              p2p_host.Host
	p2p_pubsub_events_1 *p2p_pubsub.Topic
)

// Peer discovered using multicast DNS
func (n *mdns_notifee) HandlePeerFound(p p2p_peer.AddrInfo) {
	for _, pa := range p.Addrs {
		debug("P2P received multicast DNS peer event from '%s' at '%s'", p.ID.String(), pa.String()+"/p2p/"+p.ID.String())
		peer_discovered(p.ID.String(), pa.String()+"/p2p/"+p.ID.String())
		peer_connect(p.ID.String())
	}
}

// Connect to a peer
func p2p_connect(peer string, addresses []string) bool {
	debug("P2P connecting to peer '%s' at %v", peer, addresses)
	var err error

	var ai p2p_peer.AddrInfo
	ai.ID, err = p2p_peer.Decode(peer)
	if err != nil {
		warn("P2P ignoring invalid peer ID '%s': %v", peer, err)
		return false
	}

	for _, address := range addresses {
		ma, err := multiaddr.NewMultiaddr(address)
		if err != nil {
			warn("P2P ignoring invalid peer address '%s': %v", address, err)
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
		warn("P2P peer '%s' has no valid addresses", peer)
		return false
	}

	err = p2p_me.Connect(p2p_context, ai)
	if err != nil {
		info("P2P error connecting to '%s': %v", peer, err)
		return false
	}

	debug("P2P connected to peer '%s'", peer)
	return true
}

// Join pubsubs
func p2p_pubsubs() {
	s, err := p2p_pubsub_events_1.Subscribe()
	check(err)

	for {
		m, err := s.Next(p2p_context)
		check(err)
		peer := m.ReceivedFrom.String()
		if peer != p2p_id {
			debug("P2P received pubsub event from peer '%s', length=%d", peer, len(m.Data))
			//TODO Provide source address
			event_receive(bytes.NewReader(m.Data), 1, peer, "")
			//TODO Add peer for source at address
			//peer_discovered(peer, address)
			//peer_connect(peer)
		}
	}
}

// Receive event with protocol version 1 from p2p stream
func p2p_receive_event_1(s p2p_network.Stream) {
	peer := s.Conn().RemotePeer().String()
	address := s.Conn().RemoteMultiaddr().String() + "/p2p/" + peer
	debug("P2P event from '%s' at '%s'", peer, address)

	event_receive(bufio.NewReader(s), 1, peer, address)
	peer_discovered(peer, address)
}

// Start p2p
func p2p_start() {
	var err error

	// Read or create private/public key pair
	var private p2p_crypto.PrivKey
	if file_exists(data_dir + "/p2p/private.key") {
		private, err = p2p_crypto.UnmarshalPrivateKey(file_read(data_dir + "/p2p/private.key"))
		check(err)
	} else {
		private, _, err = p2p_crypto.GenerateKeyPairWithReader(p2p_crypto.Ed25519, 256, rand.Reader)
		check(err)
		var p []byte
		p, err = p2p_crypto.MarshalPrivateKey(private)
		check(err)
		file_mkdir(data_dir + "/p2p")
		file_write(data_dir+"/p2p/private.key", p)
	}

	// Create p2p instance
	port := ini_int("p2p", "port", 1443)
	p2p_me, err = p2p.New(p2p.ListenAddrStrings(fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", port), fmt.Sprintf("/ip6/::/tcp/%d", port)), p2p.Identity(private))
	check(err)
	p2p_id = p2p_me.ID().String()
	info("P2P listening on port %d with id '%s'", port, p2p_id)

	// Listen for connecting peers
	p2p_me.SetStreamHandler("/mochi/events/1", p2p_receive_event_1)

	// Watch event bus for disconnecting peers
	go p2p_watch_disconnect()

	// Add bootstrap peers
	for _, p := range peers_bootstrap {
		if p.ID != p2p_id {
			debug("Adding bootstrap peer '%s' at %v", p.ID, p.addresses)
			peer_add_known(p.ID, p.addresses)
			go peer_connect(p.ID)
		}
	}

	// Add peers from database
	peers_add_from_db(100)

	// Listen via multicast DNS
	dns := mdns.NewMdnsService(p2p_me, "mochi", &mdns_notifee{h: p2p_me})
	err = dns.Start()
	check(err)

	// Start pubsubs
	gs, err := p2p_pubsub.NewGossipSub(p2p_context, p2p_me)
	check(err)
	p2p_pubsub_events_1, err = gs.Join("mochi/events/1")
	check(err)
	go p2p_pubsubs()
}

// Create stream to an already connected peer
func p2p_stream(peer string) p2p_network.Stream {
	p, err := p2p_peer.Decode(peer)
	if err != nil {
		warn("P2P invalid peer '%s'", peer)
		return nil
	}

	s, err := p2p_me.NewStream(p2p_context, p, "/mochi/events/1")
	if err != nil {
		warn("P2P unable to create stream to '%s': %v'", peer, err)
		return nil
	}
	return s
}

// Watch event bus for disconnecting peers
func p2p_watch_disconnect() {
	sub, err := p2p_me.EventBus().Subscribe(p2p_event.EvtPeerConnectednessChanged{}, p2p_eventbus.Name("disconnect"))
	if err != nil {
		warn("P2P unable to subscribe to event bus: %v", err)
		return
	}
	defer sub.Close()

	for e := range sub.Out() {
		ev := e.(p2p_event.EvtPeerConnectednessChanged)
		if ev.Connectedness == p2p_network.NotConnected {
			peer_disconnected(ev.Peer)
		}
	}
}

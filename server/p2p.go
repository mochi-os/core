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
	p2p_host "github.com/libp2p/go-libp2p/core/host"
	p2p_network "github.com/libp2p/go-libp2p/core/network"
	p2p_peer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	multiaddr "github.com/multiformats/go-multiaddr"
)

type mdns_notifee struct {
	h p2p_host.Host
}

var (
	p2p_context = context.Background()
	p2p_id      string
	p2p_me      p2p_host.Host
	p2p_topics  = map[string]*p2p_pubsub.Topic{}
)

// Peer discovered using multicast DNS
func (n *mdns_notifee) HandlePeerFound(p p2p_peer.AddrInfo) {
	for _, pa := range p.Addrs {
		log_debug("p2p received multicast DNS peer event from '%s' at '%s'", p.ID.String(), pa.String()+"/p2p/"+p.ID.String())
		peer_update(p.ID.String(), pa.String()+"/p2p/"+p.ID.String())
	}
}

// Connect to a peer
func p2p_connect(peer string, addresses ...string) bool {
	log_debug("p2p connecting to peer '%s' at %v", peer, addresses)
	var err error

	var info p2p_peer.AddrInfo
	info.ID, err = p2p_peer.Decode(peer)
	if err != nil {
		log_warn("p2p ignoring invalid peer ID '%s': %v", peer, err)
		return false
	}

	for _, address := range addresses {
		ma, err := multiaddr.NewMultiaddr(address)
		if err != nil {
			log_warn("p2p ignoring invalid peer address '%s': %v", address, err)
			continue
		}

		i, err := p2p_peer.AddrInfoFromP2pAddr(ma)
		if err != nil {
			log_warn("p2p ignoring invalid multiaddress: %v", err)
			continue
		}

		info.Addrs = append(info.Addrs, i.Addrs...)
	}

	if len(info.Addrs) == 0 {
		log_warn("p2p peer '%s' has no valid addresses", peer)
		return false
	}

	err = p2p_me.Connect(p2p_context, info)
	if err != nil {
		log_info("p2p error connecting to '%s': %v", peer, err)
		return false
	}

	log_debug("p2p connected to peer '%s'", peer)
	return true
}

// Listen for  on a pubsub
func p2p_pubsub_listen(s *p2p_pubsub.Subscription) {
	for {
		m, err := s.Next(p2p_context)
		check(err)
		peer := m.ReceivedFrom.String()
		if peer != p2p_id {
			log_debug("p2p received pubsub event from peer '%s'", peer)
			//TODO Provide source address
			event_receive_reader(bytes.NewReader(m.Data), peer, "")
			//TODO Add peer for source at address
			//peer_update(peer, address)
		}
	}
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

	// Listen for connecting peers
	p2p_me.SetStreamHandler("/mochi/events/1", event_receive_stream)

	// Add bootstrap peers
	for _, p := range peers_bootstrap {
		if p.ID != p2p_id {
			log_debug("Adding bootstrap peer '%s' at %v", p.ID, p.addresses_as_slice())
			peer_update(p.ID, p.addresses_as_slice()...)
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
	for _, ps := range pubsubs {
		t, err := gs.Join(ps.Topic)
		check(err)
		p2p_topics[ps.Topic] = t
		s, err := t.Subscribe()
		check(err)
		go p2p_pubsub_listen(s)

		if ps.Publish != nil {
			go ps.Publish(t)
		}
	}

	log_info("p2p listening on port %d with id '%s'", port, p2p_id)
}

// Create stream to an already connected peer
func p2p_stream(peer string) p2p_network.Stream {
	p, err := p2p_peer.Decode(peer)
	if err != nil {
		log_warn("p2p invalid peer '%s'", peer)
		return nil
	}

	s, err := p2p_me.NewStream(p2p_context, p, "/mochi/events/1")
	if err != nil {
		log_warn("p2p unable to create stream to '%s': %v'", peer, err)
		return nil
	}
	return s
}

// Mochi server: libp2p
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"github.com/libp2p/go-libp2p"
	libp2p_pubsub "github.com/libp2p/go-libp2p-pubsub"
	libp2p_crypto "github.com/libp2p/go-libp2p/core/crypto"
	libp2p_host "github.com/libp2p/go-libp2p/core/host"
	libp2p_network "github.com/libp2p/go-libp2p/core/network"
	libp2p_peer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	multiaddr "github.com/multiformats/go-multiaddr"
)

type mdns_notifee struct {
	h libp2p_host.Host
}

var (
	libp2p_context = context.Background()
	libp2p_id      string
	libp2p_me      libp2p_host.Host
	libp2p_topics  = map[string]*libp2p_pubsub.Topic{}
)

// Peer discovered using multicast DNS
func (n *mdns_notifee) HandlePeerFound(p libp2p_peer.AddrInfo) {
	for _, pa := range p.Addrs {
		log_debug("libp2p received multicast DNS peer event from '%s' at '%s'", p.ID.String(), pa.String()+"/p2p/"+p.ID.String())
		peer_update(p.ID.String(), pa.String()+"/p2p/"+p.ID.String())
	}
}

// Connect to a peer
func libp2p_connect(peer string, addresses ...string) bool {
	log_debug("libp2p connecting to peer '%s' at %v", peer, addresses)
	var err error

	var info libp2p_peer.AddrInfo
	info.ID, err = libp2p_peer.Decode(peer)
	if err != nil {
		log_warn("libp2p ignoring invalid peer ID '%s': %v", peer, err)
		return false
	}

	for _, address := range addresses {
		ma, err := multiaddr.NewMultiaddr(address)
		if err != nil {
			log_warn("libp2p ignoring invalid peer address '%s': %v", address, err)
			continue
		}

		i, err := libp2p_peer.AddrInfoFromP2pAddr(ma)
		if err != nil {
			log_warn("libp2p ignoring invalid multiaddress: %v", err)
			continue
		}

		info.Addrs = append(info.Addrs, i.Addrs...)
	}

	if len(info.Addrs) == 0 {
		log_warn("libp2p peer '%s' has no valid addresses", peer)
		return false
	}

	err = libp2p_me.Connect(libp2p_context, info)
	if err != nil {
		log_info("libp2p error connecting to '%s': %v", peer, err)
		return false
	}

	log_debug("libp2p connected to peer '%s'", peer)
	return true
}

// Listen for  on a pubsub
func libp2p_pubsub_listen(s *libp2p_pubsub.Subscription) {
	for {
		m, err := s.Next(libp2p_context)
		check(err)
		peer := m.ReceivedFrom.String()
		if peer != libp2p_id {
			log_debug("libp2p received pubsub event from peer '%s'", peer)
			//TODO Provide source address
			event_receive_reader(bytes.NewReader(m.Data), peer, "")
			//TODO Add peer for source at address
			//peer_update(peer, address)
		}
	}
}

// Start libp2p
func libp2p_start() {
	var err error

	// Read or create private/public key pair
	var private libp2p_crypto.PrivKey
	if file_exists(data_dir + "/libp2p/private.key") {
		private, err = libp2p_crypto.UnmarshalPrivateKey(file_read(data_dir + "/libp2p/private.key"))
		check(err)
	} else {
		private, _, err = libp2p_crypto.GenerateKeyPairWithReader(libp2p_crypto.Ed25519, 256, rand.Reader)
		check(err)
		var p []byte
		p, err = libp2p_crypto.MarshalPrivateKey(private)
		check(err)
		file_mkdir(data_dir + "/libp2p")
		file_write(data_dir+"/libp2p/private.key", p)
	}

	// Create libp2p instance
	port := ini_int("libp2p", "port", 1443)
	libp2p_me, err = libp2p.New(libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", port), fmt.Sprintf("/ip6/::/tcp/%d", port)), libp2p.Identity(private))
	check(err)
	libp2p_id = libp2p_me.ID().String()

	// Listen for connecting peers
	libp2p_me.SetStreamHandler("/mochi/events/1", event_receive_stream)

	// Add bootstrap peers
	for _, p := range peers_bootstrap {
		if p.ID != libp2p_id {
			log_debug("Adding bootstrap peer '%s' at %v", p.ID, p.addresses_as_slice())
			peer_update(p.ID, p.addresses_as_slice()...)
		}
	}

	// Add peers from database
	peers_add_from_db(100)

	// Listen via multicast DNS
	dns := mdns.NewMdnsService(libp2p_me, "mochi", &mdns_notifee{h: libp2p_me})
	err = dns.Start()
	check(err)

	// Start pubsubs
	gs, err := libp2p_pubsub.NewGossipSub(libp2p_context, libp2p_me)
	check(err)
	for _, ps := range pubsubs {
		t, err := gs.Join(ps.Topic)
		check(err)
		libp2p_topics[ps.Topic] = t
		s, err := t.Subscribe()
		check(err)
		go libp2p_pubsub_listen(s)

		if ps.Publish != nil {
			go ps.Publish(t)
		}
	}

	log_info("libp2p listening on port %d with id '%s'", port, libp2p_id)
}

// Create stream to an already connected peer
func libp2p_stream(peer string) libp2p_network.Stream {
	p, err := libp2p_peer.Decode(peer)
	if err != nil {
		log_warn("libp2p invalid peer '%s'", peer)
		return nil
	}

	s, err := libp2p_me.NewStream(libp2p_context, p, "/mochi/events/1")
	if err != nil {
		log_warn("libp2p unable to create stream to '%s': %v'", peer, err)
		return nil
	}
	return s
}

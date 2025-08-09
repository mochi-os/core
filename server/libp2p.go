// Mochi server: libp2p
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"bufio"
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
	"strings"
)

type mdns_notifee struct {
	h libp2p_host.Host
}

var (
	libp2p_me     libp2p_host.Host
	libp2p_id     string
	libp2p_topics = map[string]*libp2p_pubsub.Topic{}
)

// Peer discovered using multicast DNS
func (n *mdns_notifee) HandlePeerFound(p libp2p_peer.AddrInfo) {
	for _, pa := range p.Addrs {
		log_debug("libp2p received multicast DNS peer event from '%s' at '%s'", p.ID.String(), pa.String()+"/p2p/"+p.ID.String())
		peer_update(p.ID.String(), nil, pa.String()+"/p2p/"+p.ID.String())
	}
}

// Connect to a peer
func libp2p_connect(peer string, addresses ...string) *libp2p_network.Stream {
	log_debug("libp2p connecting to peer '%s' at %v", peer, addresses)
	var err error
	ctx := context.Background()

	var info libp2p_peer.AddrInfo
	info.ID, err = libp2p_peer.Decode(peer)
	if err != nil {
		log_warn("libp2p ignoring invalid peer ID '%s': %v", peer, err)
		return nil
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
		return nil
	}

	err = libp2p_me.Connect(ctx, info)
	if err != nil {
		log_info("libp2p error connecting to '%s': %v", peer, err)
		return nil
	}

	s, err := libp2p_me.NewStream(ctx, info.ID, "/mochi/1.0.0")
	if err != nil {
		log_warn("libp2p unable to create stream to '%s': %v'", peer, err)
		return nil
	}

	log_debug("libp2p connected to peer '%s'", peer)
	return &s
}

// Peer connected to us
func libp2p_handle(s libp2p_network.Stream) {
	peer := s.Conn().RemotePeer().String()
	address := s.Conn().RemoteMultiaddr().String() + "/p2p/" + peer
	log_debug("libp2p peer '%s' connected from '%s'", peer, address)
	peer_update(peer, &s, address)
	go libp2p_read(&s, peer)
}

// Listen for updates on a pubsub
func libp2p_pubsub_listen(s *libp2p_pubsub.Subscription) {
	for {
		m, err := s.Next(context.Background())
		check(err)
		peer := m.ReceivedFrom.String()
		if peer != libp2p_id {
			log_debug("libp2p received pubsub event from peer '%s': %s", peer, string(m.Data))
			//TODO Set source address
			address := peer
			event_receive_json(string(m.Data), peer, address)
			//TODO Add peer for source
		}
	}
}

// Read from a connected peer
// TODO Make work with Go streaming
func libp2p_read(s *libp2p_network.Stream, peer string) {
	log_debug("libp2p reading events from peer '%s'", peer)
	r := bufio.NewReader(*s)
	for {
		in, err := r.ReadString('\n')
		if err != nil {
			log_info("libp2p error reading from peer '%s': %v", peer, err)
			return
		}
		in = strings.TrimSuffix(in, "\n")
		if in != "" {
			log_debug("libp2p received event from peer '%s': %s", peer, in)
			//TODO Set source address
			address := peer
			event_receive_json(in, peer, address)
			//TODO Add peer for source
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
	libp2p_me.SetStreamHandler("/mochi/1.0.0", libp2p_handle)

	// Add bootstrap peers
	for _, p := range peers_bootstrap {
		if p.ID != libp2p_id {
			log_debug("Adding bootstrap peer '%s' at %v", p.ID, p.addresses_as_slice())
			peer_update(p.ID, nil, p.addresses_as_slice()...)
		}
	}

	// Add peers from database
	peers_add_from_db(100)

	// Listen via multicast DNS
	dns := mdns.NewMdnsService(libp2p_me, "mochi", &mdns_notifee{h: libp2p_me})
	err = dns.Start()
	check(err)

	// Start pubsubs
	gs, err := libp2p_pubsub.NewGossipSub(context.Background(), libp2p_me)
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

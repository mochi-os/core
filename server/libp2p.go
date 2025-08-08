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
		peer_add(p.ID.String(), pa.String()+"/p2p/"+p.ID.String(), nil)
	}
}

// Connect to a peer
func libp2p_connect(peer string, address string) *libp2p_network.Stream {
	log_debug("libp2p connecting to peer '%s' at '%s'", peer, address)
	ctx := context.Background()

	info, err := libp2p_peer.AddrInfoFromString(address)
	if err != nil {
		log_warn("libp2p invalid peer address '%s': %v", address, err)
		return nil
	}

	err = libp2p_me.Connect(ctx, *info)
	if err != nil {
		log_info("libp2p error connecting to '%s': %v", address, err)
		return nil
	}

	s, err := libp2p_me.NewStream(ctx, info.ID, "/mochi/1.0.0")
	if err != nil {
		log_warn("libp2p unable to create stream to '%s': %v'", peer, err)
		return nil
	}

	log_debug("libp2p connected to peer '%s' at '%s'", peer, address)
	return &s
}

// Peer connected to us
func libp2p_handle(s libp2p_network.Stream) {
	peer := s.Conn().RemotePeer().String()
	address := s.Conn().RemoteMultiaddr().String() + "/p2p/" + peer
	log_debug("libp2p peer '%s' connected from '%s'", peer, address)
	peer_add(peer, address, &s)
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
			//TODO Set source
			source := peer
			event_receive_json(string(m.Data), peer, source)
			//TODO Add peer for source?
		}
	}
}

// Read from a connected peer
func libp2p_read(s *libp2p_network.Stream, peer string) {
	log_debug("libp2p reading events from peer '%s'", peer)
	//TODO Make work with Go streaming
	r := bufio.NewReader(bufio.NewReader(*s))
	for {
		in, err := r.ReadString('\n')
		if err != nil {
			log_info("libp2p error reading from peer '%s': %v", peer, err)
			return
		}
		in = strings.TrimSuffix(in, "\n")
		if in != "" {
			log_debug("libp2p received event from peer '%s': %s", peer, in)
			//TODO Set source
			source := peer
			event_receive_json(in, peer, source)
			//TODO Add peer for source?
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

	// Listen for connecting peers
	port := ini_int("libp2p", "port", 1443)
	//TODO Re-enable IPv6
	//libp2p_me, err = libp2p.New(libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", port), fmt.Sprintf("/ip6/::/tcp/%d", port)), libp2p.Identity(private))
	libp2p_me, err = libp2p.New(libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", port)), libp2p.Identity(private))
	check(err)
	libp2p_id = libp2p_me.ID().String()
	libp2p_me.SetStreamHandler("/mochi/1.0.0", libp2p_handle)
	log_info("libp2p listening on port %d with id '%s'", port, libp2p_id)

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

	// Add peers from database
	peers_add_from_db(100)

	// Add bootstrap peers
	for _, p := range peers_known {
		if p.ID != libp2p_id {
			log_debug("Adding libp2p bootstrap peer '%s' at '%s'", p.ID, p.Address)
			peer_add(p.ID, p.Address+"/p2p/"+p.ID, nil)
		}
	}
}

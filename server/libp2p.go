// Comms server: libp2p
// Copyright Alistair Cunningham 2024

package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"fmt"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	"github.com/multiformats/go-multiaddr"
	"strings"
)

type mdns_notifee struct {
	H host.Host
}

var libp2p_known_addresses = []string{"/ip4/145.239.9.209/tcp/1443/p2p/12D3KooWRbpjpRmFiK7v6wRXA6yvAtTXXfvSE6xjbHVFFSaxN8SH"}
var libp2p_listen string
var libp2p_port int
var libp2p_address string
var libp2p_context context.Context
var libp2p_host host.Host
var libp2p_id string
var libp2p_topics = map[string]*pubsub.Topic{}

// Peer discovered using multicast DNS
func (n *mdns_notifee) HandlePeerFound(p peer.AddrInfo) {
	for _, pa := range p.Addrs {
		log_debug("Found multicast DNS peer '%s' at '%s'", p.ID.String(), pa.String()+"/p2p/"+p.ID.String())
		peer_add_chan <- Peer{ID: p.ID.String(), Address: pa.String() + "/p2p/" + p.ID.String()}
	}
}

// Connect to a peer
func libp2p_connect(address string) error {
	ai, err := peer.AddrInfoFromString(address)
	if err != nil {
		return error_message("Invalid peer address '%s': %s", address, err)
	}
	log_debug("Connecting to peer at '%s'", address)
	err = libp2p_host.Connect(context.Background(), *ai)
	if err != nil {
		return error_message("Error connecting to peer at '%s': %s", address, err)
	}
	log_debug("Connected to peer at '%s'", address)
	return nil
}

// Handle connected peer
func libp2p_handle(s network.Stream) {
	address := s.Conn().RemoteMultiaddr().String()
	log_debug("Peer connected from libp2p '%s'", address)
	peer_add_by_address(address)
	rw := bufio.NewReadWriter(bufio.NewReader(s), bufio.NewWriter(s))
	go libp2p_read(rw)
}

// Listen for updates on a pubsub
func libp2p_pubsub_listen(s *pubsub.Subscription) {
	for {
		m, err := s.Next(libp2p_context)
		fatal(err)
		if m.ReceivedFrom.String() != libp2p_id {
			log_debug("Got event from pubsub: %s", string(m.Data))
			event_receive_json(m.Data, true)
		}
	}
}

// Read from a connected peer
func libp2p_read(r *bufio.ReadWriter) {
	log_debug("Reading events from new peer")
	for {
		in, _ := r.ReadString('\n')
		if in == "" {
			return
		}
		if in != "\n" {
			in = strings.TrimSuffix(in, "\n")
			log_debug("Got event from read peer: %s", in)
			event_receive_json([]byte(in), true)
		}
	}
}

// Send a message to an address
func libp2p_send(to string, content []byte) {
	//TODO Send event via libp2p
}

// Start libp2p
func libp2p_start() {
	var private crypto.PrivKey
	var err error

	if file_exists("libp2p/private.key") {
		private, err = crypto.UnmarshalPrivateKey(file_read("libp2p/private.key"))
		fatal(err)
	} else {
		private, _, err = crypto.GenerateKeyPairWithReader(crypto.Ed25519, 256, rand.Reader)
		fatal(err)
		var p []byte
		p, err = crypto.MarshalPrivateKey(private)
		fatal(err)
		file_mkdir("libp2p")
		file_write("libp2p/private.key", p)
	}

	libp2p_context = context.Background()
	ma, err := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/%d", libp2p_listen, libp2p_port))
	fatal(err)
	h, err := libp2p.New(libp2p.ListenAddrs(ma), libp2p.Identity(private))
	fatal(err)
	libp2p_host = h
	libp2p_id = h.ID().String()
	h.SetStreamHandler("/comms/1.0.0", libp2p_handle)
	libp2p_address = fmt.Sprintf("/ip4/%s/tcp/%d/p2p/%s", libp2p_listen, libp2p_port, libp2p_id)
	log_info("libp2p listening on '%s'", libp2p_address)

	dns := mdns.NewMdnsService(h, "comms", &mdns_notifee{H: h})
	err = dns.Start()
	fatal(err)

	gs, err := pubsub.NewGossipSub(libp2p_context, h)
	fatal(err)
	for _, ps := range app_pubsubs {
		t, err := gs.Join(ps.Topic)
		fatal(err)
		libp2p_topics[ps.Topic] = t
		s, err := t.Subscribe()
		fatal(err)
		go libp2p_pubsub_listen(s)

		if ps.Publish != nil {
			go ps.Publish(t)
		}
	}

	for _, address := range libp2p_known_addresses {
		if address != libp2p_id {
			peer_add_by_address(address)
		}
	}
}

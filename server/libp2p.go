// Comms server: libp2p
// Copyright Alistair Cunningham 2024

package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/json"
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
	"time"
)

type mdns_notifee struct {
	H host.Host
}

var libp2p_listen string
var libp2p_port int
var libp2p_address string
var libp2p_context context.Context
var libp2p_host host.Host
var libp2p_id string
var libp2p_topics map[string]*pubsub.Topic = map[string]*pubsub.Topic{}

// Peer discovered using mDNS
func (n *mdns_notifee) HandlePeerFound(p peer.AddrInfo) {
	for _, pa := range p.Addrs {
		address := pa.String() + "/p2p/" + p.ID.String()
		if pf, found := peers[address]; found {
			log_debug("Re-discovered existing multicast DNS peer '%s'", address)
			pf.Seen = time_unix()
			peers[address] = pf
		} else {
			log_debug("Discovered new multicast DNS peer '%s'", address)
			libp2p_connect(address, n.H)
		}
	}
}

// Connect to a peer
func libp2p_connect(address string, h host.Host) {
	log_debug("Connecting to peer at '%s'", address)
	ai, err := peer.AddrInfoFromString(address)
	fatal(err)
	err = h.Connect(context.Background(), *ai)
	if err != nil {
		log_info("Error connecting to peer at '%s': %s\n", address, err)
	}
	peers[address] = Peer{Address: address, Seen: time_unix()}
	log_debug("Connected to peer at '%s'", address)
}

// Handle peer connecting to us
func libp2p_handle(s network.Stream) {
	address := s.Conn().RemoteMultiaddr().String()
	log_debug("Peer connected from libp2p '%s'", address)
	rw := bufio.NewReadWriter(bufio.NewReader(s), bufio.NewWriter(s))
	if p, found := peers[address]; found {
		p.Seen = time_unix()
		peers[address] = p
	} else {
		peers[address] = Peer{Address: address, Seen: time_unix()}
	}
	go libp2p_read(rw)
	go libp2p_write(rw)
}

// Listen for updates on a pubsub
func libp2p_pubsub_listen(s *pubsub.Subscription) {
	for {
		m, err := s.Next(libp2p_context)
		fatal(err)
		if m.ReceivedFrom.String() != libp2p_id {
			event_receive_json(m.Data)
		}
	}
}

// Publish our own peer information to the peers pubsub once an hour
func libp2p_peers_publish(t *pubsub.Topic) {
	for {
		j, err := json.Marshal(Event{ID: uid(), From: libp2p_id, Service: "peers", Instance: libp2p_id, Action: "update", Content: libp2p_address})
		fatal(err)
		t.Publish(libp2p_context, j)
		//TODO Increase interval
		time.Sleep(5 * time.Second)
	}
}

// Read from a newly connected peer
func libp2p_read(r *bufio.ReadWriter) {
	for {
		in, _ := r.ReadString('\n')
		if in == "" {
			return
		}
		if in != "\n" {
			in = strings.TrimSuffix(in, "\n")
			event_receive_json([]byte(in))
		}
	}
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
	libp2p_id = h.ID().String()
	h.SetStreamHandler("/comms/1.0.0", libp2p_handle)
	libp2p_address = fmt.Sprintf("/ip4/%s/tcp/%d/p2p/%s", libp2p_listen, libp2p_port, libp2p_id)
	log_info("libp2p listening on '%s'", libp2p_address)

	dns := mdns.NewMdnsService(h, "comms", &mdns_notifee{H: h})
	err = dns.Start()
	fatal(err)

	//TODO
	//for _, peer := range libp2p_well_known_peers {
	//	libp2p_connect(peer)
	//}

	gs, err := pubsub.NewGossipSub(libp2p_context, h)
	fatal(err)

	for _, topic := range app_pubsubs {
		t, err := gs.Join(topic)
		fatal(err)
		s, err := t.Subscribe()
		fatal(err)
		go libp2p_pubsub_listen(s)
		libp2p_topics[topic] = t

		if topic == "peers" {
			go libp2p_peers_publish(t)
		}
	}

	libp2p_host = h
}

// Write to a newly connected peer. Currently not used.
func libp2p_write(w *bufio.ReadWriter) {
	//TODO Send it a list of current peers
	//for {
	//	w.WriteString(fmt.Sprintf("%s\n", data))
	//	w.Flush()
	//}
}

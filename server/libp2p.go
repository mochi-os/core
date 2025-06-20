// Comms server: libp2p
// Copyright Alistair Cunningham 2024

// The code in this file and peers.go could probably be improved by someone highly familiar with libp2p

package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"fmt"
	"github.com/libp2p/go-libp2p"
	libp2p_pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	libp2p_peer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	"github.com/multiformats/go-multiaddr"
	"strings"
)

type mdns_notifee struct {
	h host.Host
}

var (
	libp2p_listen  string
	libp2p_port    int
	libp2p_address string
	libp2p_context context.Context
	libp2p_host    host.Host
	libp2p_id      string
	libp2p_topics  = map[string]*libp2p_pubsub.Topic{}
)

// Peer discovered using multicast DNS
func (n *mdns_notifee) HandlePeerFound(p libp2p_peer.AddrInfo) {
	for _, pa := range p.Addrs {
		log_debug("libp2p received multicast DNS peer event from '%s' at '%s'", p.ID.String(), pa.String()+"/p2p/"+p.ID.String())
		peer_add(pa.String()+"/p2p/"+p.ID.String(), true)
	}
}

// Connect to a peer
func libp2p_connect(address string) bool {
	info, err := libp2p_peer.AddrInfoFromString(address)
	if err != nil {
		log_warn("libp2p invalid peer address '%s': %s", address, err)
		return false
	}
	log_debug("libp2p connecting to '%s'", address)
	err = libp2p_host.Connect(context.Background(), *info)
	if err != nil {
		log_info("libp2p error connecting to '%s': %s", address, err)
		return false
	}
	log_debug("libp2p connected to '%s'", address)
	return true
}

// Peer connected to us
func libp2p_handle(s network.Stream) {
	peer := s.Conn().RemotePeer().String()
	address := s.Conn().RemoteMultiaddr().String() + "/p2p/" + s.Conn().RemotePeer().String()
	log_debug("libp2p peer connected from '%s'", address)
	peer_add(address, false)
	r := bufio.NewReader(bufio.NewReader(s))
	go libp2p_read(r, peer)
}

// Listen for updates on a pubsub
func libp2p_pubsub_listen(s *libp2p_pubsub.Subscription) {
	for {
		m, err := s.Next(libp2p_context)
		check(err)
		peer := m.ReceivedFrom.String()
		if peer != libp2p_id {
			log_debug("libp2p received pubsub event: %s", string(m.Data))
			event_receive_json(string(m.Data), peer)
		}
	}
}

// Read from a connected peer
func libp2p_read(r *bufio.Reader, peer string) {
	log_debug("libp2p reading events from new peer '%s'", peer)
	for {
		in, err := r.ReadString('\n')
		if err != nil {
			return
		}
		in = strings.TrimSuffix(in, "\n")
		if in != "" {
			log_debug("libp2p received event from peer: %s", in)
			event_receive_json(in, peer)
		}
	}
}

// Send a message to an address
func libp2p_send(address string, content string) bool {
	log_debug("libp2p sending '%s' to '%s'", content, address)

	info, err := libp2p_peer.AddrInfoFromString(address)
	if err != nil {
		log_warn("libp2p invalid peer address '%s': %s", address, err)
		return false
	}
	s, err := libp2p_host.NewStream(context.Background(), info.ID, "/comms/1.0.0")
	if err != nil {
		log_warn("libp2p unable to create stream to '%s': %s'", address, err)
		return false
	}
	w := bufio.NewWriter(bufio.NewWriter(s))
	_, err = w.WriteString(content + "\n")
	if err != nil {
		log_debug("libp2p unable to write event: %s", err)
		return false
	}
	err = w.Flush()
	if err != nil {
		log_debug("libp2p unable to flush event: %s", err)
		return false
	}
	return true
}

// Start libp2p
func libp2p_start() {
	var private crypto.PrivKey
	var err error

	if file_exists(data_dir + "/libp2p/private.key") {
		private, err = crypto.UnmarshalPrivateKey(file_read(data_dir + "/libp2p/private.key"))
		check(err)
	} else {
		private, _, err = crypto.GenerateKeyPairWithReader(crypto.Ed25519, 256, rand.Reader)
		check(err)
		var p []byte
		p, err = crypto.MarshalPrivateKey(private)
		check(err)
		file_mkdir(data_dir + "/libp2p")
		file_write(data_dir+"/libp2p/private.key", p)
	}

	libp2p_context = context.Background()
	ma, err := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/%d", libp2p_listen, libp2p_port))
	check(err)
	h, err := libp2p.New(libp2p.ListenAddrs(ma), libp2p.Identity(private))
	check(err)
	libp2p_host = h
	libp2p_id = h.ID().String()
	h.SetStreamHandler("/comms/1.0.0", libp2p_handle)
	libp2p_address = fmt.Sprintf("/ip4/%s/tcp/%d/p2p/%s", libp2p_listen, libp2p_port, libp2p_id)
	log_info("libp2p listening on '%s'", libp2p_address)

	dns := mdns.NewMdnsService(h, "comms", &mdns_notifee{h: h})
	err = dns.Start()
	check(err)

	gs, err := libp2p_pubsub.NewGossipSub(libp2p_context, h)
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

	peers_add_from_db(10)

	for peer, location := range peers_known {
		if peer != libp2p_id {
			log_debug("Adding well known libp2p peer '%s' at '%s'", peer, location)
			peer_add(location+"/p2p/"+peer, true)
		}
	}
}

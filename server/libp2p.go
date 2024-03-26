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
	"time"
)

type Peer struct {
	Address string
	Seen    int64
}

type discovery_notifee struct {
	h host.Host
}

var libp2p_listen string
var libp2p_port int
var libp2p_address string
var libp2p_context context.Context
var libp2p_directory_subscription *pubsub.Topic
var libp2p_peers_subscription *pubsub.Topic
var libp2p_peers map[string]Peer = map[string]Peer{}

func (n *discovery_notifee) HandlePeerFound(p peer.AddrInfo) {
	for _, pa := range p.Addrs {
		address := pa.String() + "/p2p/" + p.ID.String()
		if pf, found := libp2p_peers[address]; found {
			log_debug("Re-discovered existing peer '%s'", address)
			pf.Seen = time_unix()
			libp2p_peers[address] = pf

		} else {
			log_debug("Discovered new peer '%s', and connecting to it", address)
			err := n.h.Connect(context.Background(), p)
			fatal(err)
			log_debug("Connected to peer at '%s'", address)
			libp2p_peers[address] = Peer{Address: address, Seen: time_unix()}
		}
	}
}

func libp2p_connect(h host.Host, address string) {
	log_debug("Connecting to peer at '%s'", address)
	ai, err := peer.AddrInfoFromString(address)
	fatal(err)
	err = h.Connect(context.Background(), *ai)
	if err != nil {
		log_info("Error connecting to peer at '%s': %s\n", address, err)
	}
	libp2p_peers[address] = Peer{Address: address, Seen: time_unix()}
	log_debug("Connected to peer at '%s'", address)
}

func libp2p_directory_listen(h host.Host, d *pubsub.Subscription) {
	for {
		msg, err := d.Next(libp2p_context)
		fatal(err)
		if msg.ReceivedFrom != h.ID() {
			log_debug("Got directory update '%s' from '%s'", string(msg.Data), msg.ReceivedFrom)
			//TODO
		}
	}
}

func libp2p_handle(s network.Stream) {
	address := s.Conn().RemoteMultiaddr().String()
	log_debug("Peer connected from libp2p '%s'", address)
	rw := bufio.NewReadWriter(bufio.NewReader(s), bufio.NewWriter(s))
	if p, found := libp2p_peers[address]; found {
		p.Seen = time_unix()
		libp2p_peers[address] = p
	} else {
		libp2p_peers[address] = Peer{Address: address, Seen: time_unix()}
	}
	go libp2p_read(rw)
	go libp2p_write(rw)
}

func libp2p_peers_publish(myself string) {
	for {
		libp2p_peers_subscription.Publish(libp2p_context, []byte(myself))
		time.Sleep(time.Hour)
	}
}

func libp2p_peers_listen(h host.Host, p *pubsub.Subscription) {
	for {
		m, err := p.Next(libp2p_context)
		fatal(err)
		if m.ReceivedFrom != h.ID() {
			address := string(m.Data)
			log_debug("Got peers update '%s' from '%s'", address, m.ReceivedFrom)
			if p, found := libp2p_peers[address]; found {
				p.Seen = time_unix()
			} else {
				libp2p_connect(h, address)
				libp2p_peers[address] = Peer{Address: address, Seen: time_unix()}
			}
		}
	}
}

func libp2p_read(r *bufio.ReadWriter) {
	for {
		in, _ := r.ReadString('\n')
		if in == "" {
			return
		}
		if in != "\n" {
			in = strings.TrimSuffix(in, "\n")
			event_receive_json(in)
		}
	}
}

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
	h.SetStreamHandler("/comms/1.0.0", libp2p_handle)
	libp2p_address = fmt.Sprintf("/ip4/%s/tcp/%d/p2p/%s", libp2p_listen, libp2p_port, h.ID())
	log_info("libp2p listening on '%s'", libp2p_address)

	dns := mdns.NewMdnsService(h, "comms", &discovery_notifee{h: h})
	err = dns.Start()
	fatal(err)

	//TODO
	//for _, peer := range libp2p_well_known_peers {
	//	libp2p_connect(h, peer)
	//}

	gs, err := pubsub.NewGossipSub(libp2p_context, h)
	fatal(err)

	libp2p_directory_subscription, err = gs.Join("directory")
	fatal(err)
	d, err := libp2p_directory_subscription.Subscribe()
	fatal(err)
	go libp2p_directory_listen(h, d)

	libp2p_peers_subscription, err = gs.Join("peers")
	fatal(err)
	p, err := libp2p_peers_subscription.Subscribe()
	fatal(err)
	go libp2p_peers_listen(h, p)
	go libp2p_peers_publish(libp2p_address)
}

func libp2p_write(w *bufio.ReadWriter) {
	//TODO Send it a list of current peers
	//for {
	//	w.WriteString(fmt.Sprintf("%s\n", data))
	//	w.Flush()
	//}
}

// Comms server: libp2p
// Copyright Alistair Cunningham 2024

package main

import (
	"bufio"
	"crypto/rand"
	"fmt"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/multiformats/go-multiaddr"
	"strings"
)

func libp2p_handle(s network.Stream) {
	log_debug("Peer connected from libp2p '%s'", s.Conn().RemoteMultiaddr())
	rw := bufio.NewReadWriter(bufio.NewReader(s), bufio.NewWriter(s))
	go libp2p_read(rw)
	// Enable if we ever want to push messages to a peer that connects to us
	//go libp2p_write(rw)
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

func libp2p_start(listen string, port int) {
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

	addr, _ := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/%d", listen, port))
	host, err := libp2p.New(libp2p.ListenAddrs(addr), libp2p.Identity(private))
	fatal(err)
	host.SetStreamHandler("/comms/1.0.0", libp2p_handle)
	log_info("libp2p listening on '/ip4/%s/tcp/%d/p2p/%s'", listen, port, host.ID())
}

/*func libp2p_write(w *bufio.ReadWriter) {
	for {
		w.WriteString(fmt.Sprintf("%s\n", data))
		w.Flush()
	}
}*/

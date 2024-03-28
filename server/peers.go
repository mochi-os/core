// Comms server: Peers
// Copyright Alistair Cunningham 2024

package main

import (
	"strings"
)

type Peer struct {
	ID      string
	Address string
	Seen    int64
}

var peers map[string]Peer = map[string]Peer{}
var peer_add_chan = make(chan Peer)

func init() {
	app_register("peers", map[string]string{"en": "Peers"})
	app_register_event("peers", "update", peer_update)
	app_register_pubsub("peers", "peers")
	app_register_service("peers", "peers")
}

func peer_add_by_address(address string) {
	log_debug("Adding peer by address '%s'", address)
	peer_add_chan <- Peer{ID: strings.TrimLeft(address, "/"), Address: address}
}

func peers_manager() {
	log_debug("Peers manager running")
	for p := range peer_add_chan {
		if p.ID == libp2p_id {
			continue
		}
		log_debug("Adding new peer '%s' at '%s'", p.ID, p.Address)
		p.Seen = time_unix()
		err := libp2p_connect(p.Address)
		if err != nil {
			log_info(err.Error())
			continue
		}
		peers[p.ID] = p
	}
}

func peer_update(u *User, e *Event) {
	if valid(e.Instance, "^[\\w]{1,100}$") && valid(e.Content, "^[\\w/.]{1,100}$") {
		if e.Instance == libp2p_id {
			return
		}
		peer_add_chan <- Peer{ID: e.Instance, Address: e.Content}
	} else {
		log_debug("Invalid peer update")
	}
}

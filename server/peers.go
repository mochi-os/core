// Comms server: libp2p
// Copyright Alistair Cunningham 2024

package main

type Peer struct {
	ID      string
	Address string
	Seen    int64
}

var peers map[string]Peer = map[string]Peer{}

func init() {
	app_register("peers", map[string]string{"en": "Peers"})
	app_register_event("peers", "update", peer_update)
	app_register_pubsub("peers", "peers")
	app_register_service("peers", "peers")
}

func peer_update(u *User, e *Event) {
	//TODO Validate instance
	libp2p_connect_chan <- Peer{ID: e.Instance, Address: e.Content}
}

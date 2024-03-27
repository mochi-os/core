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
	app_register("peers", "Peers")
	app_register_event("peers", "update", peer_update)
	app_register_service("peers", "peers")
}

func peer_update(u *User, e *Event) {
	log_debug("Got peer event from '%s'", e.From)
	if p, found := peers[e.Instance]; found {
		p.Seen = time_unix()
	} else {
		//TODO Validate everything
		libp2p_connect(e.Content)
		peers[e.Instance] = Peer{ID: e.Instance, Address: e.Content, Seen: time_unix()}
	}
}

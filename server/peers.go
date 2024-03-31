// Comms server: Peers
// Copyright Alistair Cunningham 2024

package main

import (
	"encoding/json"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"strings"
	"time"
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
	app_register_pubsub("peers", "peers", peers_publish)
	app_register_service("peers", "peers")
}

func peer_add_by_address(address string) {
	peer_add_chan <- Peer{ID: strings.TrimLeft(address, "/"), Address: address}
}

func peers_manager() {
	for p := range peer_add_chan {
		if p.ID == libp2p_id {
			continue
		}
		p.Seen = time_unix()
		_, found := peers[p.ID]
		if found {
			peers[p.ID] = p
			continue
		}
		err := libp2p_connect(p.Address)
		if err != nil {
			log_info(err.Error())
			continue
		}
		peers[p.ID] = p
	}
}

// Publish our own information to the pubsub regularly
func peers_publish(t *pubsub.Topic) {
	for {
		j, err := json.Marshal(Event{ID: uid(), Service: "peers", Entity: libp2p_id, Action: "update", Content: libp2p_address})
		fatal(err)
		t.Publish(libp2p_context, j)
		time.Sleep(time.Minute)
	}
}

func peer_update(u *User, e *Event) {
	if valid(e.Entity, "^[\\w]{1,100}$") && valid(e.Content, "^[\\w/.]{1,100}$") {
		if e.Entity == libp2p_id {
			return
		}
		peer_add_chan <- Peer{ID: e.Entity, Address: e.Content}
	} else {
		log_info("Invalid peer update")
	}
}

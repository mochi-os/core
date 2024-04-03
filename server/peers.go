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
	Connect bool
	Updated int64
}

var peers_connected map[string]Peer = map[string]Peer{}
var peer_add_chan = make(chan Peer)

func init() {
	app_register("peers", map[string]string{"en": "Peers"})
	app_register_event("peers", "publish", peer_event)
	app_register_pubsub("peers", "peers", peers_publish)
}

// Add a (possibly existing) peer
func peer_add(address string, connect bool) {
	parts := strings.Split(address, "/")
	if len(parts) > 1 {
		peer_add_chan <- Peer{ID: parts[len(parts)-1], Address: address, Connect: connect}
	}
}

// Peer event received
func peer_event(u *User, e *Event) {
	if valid(e.Entity, "^[\\w]{1,100}$") && valid(e.Content, "^[\\w/.]{1,100}$") {
		if e.Entity == libp2p_id {
			return
		}
		peer_add(e.Content, true)
	} else {
		log_info("Invalid peer update")
	}
}

// Manage list of known peers, and connect to them if necessary
func peers_manager() {
	for p := range peer_add_chan {
		if p.ID == libp2p_id {
			continue
		}

		p.Updated = time_unix()
		e, found := peers_connected[p.ID]
		if found && p.Address == e.Address {
			// We're already connected to this peer and it's at the same address as before, so just update its updated time
			peers_connected[p.ID] = p
			db_exec("peers", "update peers set updated=? where id=?", p.Updated, p.ID)

		} else {
			// New peer, peer not seen since we started, or peer changed address
			if p.Connect {
				err := libp2p_connect(p.Address)
				if err != nil {
					log_info(err.Error())
					continue
				}
			}
			peers_connected[p.ID] = p
			db_exec("peers", "replace into peers ( id, address, updated ) values ( ?, ?, ? )", p.ID, p.Address, p.Updated)
			go events_check_queue("peer", p.ID)
		}
	}
}

// Publish our own information to the pubsub regularly
func peers_publish(t *pubsub.Topic) {
	for {
		j, err := json.Marshal(Event{ID: uid(), App: "peers", Entity: libp2p_id, Action: "publish", Content: libp2p_address})
		fatal(err)
		t.Publish(libp2p_context, j)
		time.Sleep(time.Hour)
	}
}

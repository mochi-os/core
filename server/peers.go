// Mochi server: Peers
// Copyright Alistair Cunningham 2024-2025

package main

import (
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

const (
	peers_minimum = 1
)

var (
	peers_connected   map[string]Peer = map[string]Peer{}
	peer_add_chan                     = make(chan Peer)
	peers_known                       = map[string]string{"12D3KooWRbpjpRmFiK7v6wRXA6yvAtTXXfvSE6xjbHVFFSaxN8SH": "/ip4/217.182.75.108/tcp/1443"}
	peer_publish_chan                 = make(chan bool)
)

func init() {
	a := app("peers")
	a.service("peers")
	a.event_broadcast("request", peer_request_event)
	a.event_broadcast("publish", peer_publish_event)
	a.pubsub("peers", peers_publish)
}

// Add a (possibly existing) peer
func peer_add(address string, connect bool) {
	parts := strings.Split(address, "/")
	if len(parts) > 1 {
		peer_add_chan <- Peer{ID: parts[len(parts)-1], Address: address, Connect: connect}
	}
}

// Add some peers we already know about from the database
func peers_add_from_db(limit int) {
	var peers []Peer
	db := db_open("db/peers.db")
	db.scans(&peers, "select * from peers order by updated desc limit ?", limit)
	for _, p := range peers {
		log_debug("Adding peer from database at '%s'", p.Address)
		peer_add(p.Address, true)
	}
}

// Get address of peer
func peer_address(peer string) string {
	var p Peer
	db := db_open("db/peers.db")
	if db.scan(&p, "select address from peers where id=?", peer) {
		return p.Address
	}
	return ""
}

// Manage list of known peers, and connect to them if necessary
func peers_manager() {
	db := db_open("db/peers.db")

	for p := range peer_add_chan {
		if p.ID == libp2p_id {
			continue
		}

		o, found := peers_connected[p.ID]
		p.Updated = now()
		peers_connected[p.ID] = p

		if found {
			if p.Address == o.Address {
				log_debug("Peer '%s' already connected", p.ID)
				db.exec("update peers set updated=? where id=?", p.Updated, p.ID)
			} else {
				log_debug("Peer '%s' changed address to '%s'", o.Address, p.Address)
				db.exec("update peers set address=?, updated=? where id=?", p.Address, p.Updated, p.ID)
			}

		} else if p.Connect && libp2p_connect(p.Address) {
			log_debug("New peer '%s' connected", p.ID)
			db.exec("replace into peers ( id, address, updated ) values ( ?, ?, ? )", p.ID, p.Address, p.Updated)
			go events_check_queue("peer", p.ID)
		}
	}
}

// Publish our own information to the pubsub regularly or when requested
func peers_publish(t *pubsub.Topic) {
	jc := json_encode(map[string]string{"id": libp2p_id, "address": libp2p_address})
	after := time.After(time.Hour)
	for {
		select {
		case <-peer_publish_chan:
			log_debug("Peer publish requested")
		case <-after:
			log_debug("Peer routine publish")
		}
		log_debug("Publishing peer")
		t.Publish(libp2p_context, []byte(json_encode(Event{ID: uid(), Service: "peers", Action: "publish", Content: jc})))
	}
}

// Received a peer publish event from another server
// If the peer publishes its address as /ip4/0.0.0.0 or similar, we should probably
// use the received address, but I don't know how to do this.
func peer_publish_event(e *Event) {
	var m map[string]string
	if json_decode(&m, e.Content) && valid(m["id"], "^[\\w]{1,100}$") && valid(m["address"], "^[\\w/.]{1,100}$") {
		if m["id"] == libp2p_id {
			return
		}
		log_debug("Adding peer due to publish event at '%s'", m["address"])
		peer_add(m["address"], true)
	} else {
		log_info("Invalid peer update")
	}
}

// Ask the peers pubsub for a peer
func peer_request(peer string) {
	libp2p_topics["peers"].Publish(libp2p_context, []byte(json_encode(Event{ID: uid(), Service: "peers", Action: "request", Content: peer})))
}

// Reply to a peer request if for us
func peer_request_event(e *Event) {
	log_debug("Received peer request event '%#v'", e)
	if e.Content == libp2p_id {
		log_debug("Peer request is for us; requesting a re-publish")
		peer_publish_chan <- true
	}
}

// Send a message to a peer
func peer_send(peer string, content string) bool {
	address := peer_address(peer)
	if address != "" {
		return libp2p_send(address, content)
	}
	return false
}

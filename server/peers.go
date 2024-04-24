// Comms server: Peers
// Copyright Alistair Cunningham 2024

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

var peers_known = map[string]string{
	//	"12D3KooWRbpjpRmFiK7v6wRXA6yvAtTXXfvSE6xjbHVFFSaxN8SH": "/ip4/145.239.9.209/tcp/1443",
	"12D3KooWHrYrMabQw6HdWjKS5FcYMYGgMjKGYPGZpeZxUD3gmvvs": "/ip4/127.0.0.1/tcp/1443",
}

var peers_connected map[string]Peer = map[string]Peer{}
var peer_add_chan = make(chan Peer)
var peer_publish_chan = make(chan bool)

func init() {
	register_app("peers")
	register_event("peers", "request", peer_event_request)
	register_event("peers", "publish", peer_event_publish)
	register_pubsub("peers", "peers", peers_publish)
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
	db_structs(&peers, "db/peers.db", "select * from peers order by updated desc limit ?", limit)
	for _, p := range peers {
		peer_add(p.Address, true)
	}
}

// Get address of peer
func peer_address(peer string) string {
	var p Peer
	if db_struct(&p, "db/peers.db", "select address from peers where id=?", peer) {
		return p.Address
	}
	return ""
}

// Reply to a peer request if for us
func peer_event_request(u *User, e *Event) {
	log_debug("Received peer request event '%#v'", e)
	if e.Content == libp2p_id {
		log_debug("Peer request is for us; requesting a re-publish")
		peer_publish_chan <- true
	}
}

// Received a peer publish event from another server
func peer_event_publish(u *User, e *Event) {
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
			db_exec("db/peers.db", "update peers set updated=? where id=?", p.Updated, p.ID)

		} else if p.Connect && libp2p_connect(p.Address) {
			// New peer
			peers_connected[p.ID] = p
			db_exec("db/peers.db", "replace into peers ( id, address, updated ) values ( ?, ?, ? )", p.ID, p.Address, p.Updated)
			go events_check_queue("peer", p.ID)
		}
	}
}

// Publish our own information to the pubsub regularly or when requested
func peers_publish(t *pubsub.Topic) {
	after := time.After(time.Hour)
	for {
		select {
		case <-peer_publish_chan:
			log_debug("Peer publish requested")
		case <-after:
			log_debug("Peer routine publish")
		}
		log_debug("Publishing peer")
		t.Publish(libp2p_context, []byte(json_encode(Event{ID: uid(), App: "peers", Entity: libp2p_id, Action: "publish", Content: libp2p_address})))
	}
}

// Ask the peers pubsub for a peer
func peer_request(peer string) {
	libp2p_topics["peers"].Publish(libp2p_context, []byte(json_encode(event(nil, "", "peers", "", "request", peer))))
}

// Send a message to a peer
func peer_send(peer string, content string) bool {
	address := peer_address(peer)
	if address != "" {
		return libp2p_send(address, content)
	}
	return false
}

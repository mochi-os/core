// Mochi server: Peers
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"bufio"
	"context"
	libp2p_pubsub "github.com/libp2p/go-libp2p-pubsub"
	libp2p_network "github.com/libp2p/go-libp2p/core/network"
	"time"
)

type Peer struct {
	ID      string
	Address string `json:",omitempty"`
	Updated int64
	stream  *libp2p_network.Stream
}

const (
	peers_minimum = 1
)

var (
	peers_known = []Peer{
		Peer{ID: "12D3KooWRbpjpRmFiK7v6wRXA6yvAtTXXfvSE6xjbHVFFSaxN8SH", Address: "/ip4/217.182.75.108/tcp/1443"},
		Peer{ID: "12D3KooWRbpjpRmFiK7v6wRXA6yvAtTXXfvSE6xjbHVFFSaxN8SH", Address: "/ip6/2001:41d0:601:1100::61f7/tcp/1443"},
	}
	peers_connected   map[string]Peer = map[string]Peer{}
	peer_add_chan                     = make(chan Peer)
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
// If the peer connected to us stream will be the stream libp2p opened for us. Otherwise stream will be nil.
func peer_add(id string, address string, stream *libp2p_network.Stream) {
	peer_add_chan <- Peer{ID: id, Address: address, Stream: stream}
}

// Add some peers we already know about from the database
func peers_add_from_db(limit int) {
	var peers []Peer
	db := db_open("db/peers.db")
	db.scans(&peers, "select * from peers order by updated desc limit ?", limit)
	for _, p := range peers {
		log_debug("Adding peer from database at '%s'", p.Address)
		peer_add(p.ID, p.Address, nil)
	}
}

// Get details of a peer, connecting to it if not already connected
// TODO THIS IS NEXT
func peer_connect(peer string) *Peer {
	return nil
}

// Manage list of known peers, and connect to them if necessary
func peers_manager() {
	db := db_open("db/peers.db")

	for p := range peer_add_chan {
		log_debug("Peer request to add '%s' at '%s'", p.ID, p.Address)
		p.Updated = now()

		if p.ID == libp2p_id {
			log_debug("Peer ignoring request to add ourself")
			continue
		}

		_, found := peers_connected[p.ID]
		if found {
			log_debug("Peer '%s' already connected", p.ID)

		} else if p.stream != nil {
			log_debug("Peer '%s' connected to us and has stream; using their stream'", p.ID)
			peers_connected[p.ID] = p

		} else {
			log_debug("Peer '%s' is not connected; connecting...", p.ID)
			p.stream = libp2p_connect(p.ID, p.Address)
			if p.stream != nil {
				log_debug("New peer '%s' connected", p.ID)
				peers_connected[p.ID] = p
				go events_check_queue("peer", p.ID)

			} else {
				log_debug("Unable to connect to peer '%s'", p.ID)
				continue
			}
		}

		db.exec("replace into peers ( id, address, updated ) values ( ?, ?, ? )", p.ID, p.Address, p.Updated)
	}
}

// Publish our own information to the pubsub regularly or when requested
func peers_publish(t *libp2p_pubsub.Topic) {
	jc := json_encode(PeerUpdate{ID: libp2p_id, Updated: now()})
	after := time.After(time.Hour)
	for {
		select {
		case <-peer_publish_chan:
			log_debug("Peer publish requested")
		case <-after:
			log_debug("Peer routine publish")
		}
		log_debug("Publishing peer")
		t.Publish(context.Background(), []byte(json_encode(Event{ID: uid(), Service: "peers", Action: "publish", Content: jc})))
	}
}

// Received a peer publish event from another server
// TODO Use received address
// TODO Check timestamp
func peer_publish_event(e *Event) {
	var m map[string]string
	if json_decode(&m, e.Content) && valid(m["id"], "^[\\w]{1,100}$") && valid(m["address"], "^[\\w/.]{1,100}$") {
		if m["id"] == libp2p_id {
			return
		}
		log_debug("Adding peer '%s' due to publish event at '%s'", m["id"], m["address"])
		peer_add(m["id"], m["address"], nil)
	} else {
		log_info("Invalid peer update")
	}
}

// Ask the peers pubsub for a peer
func peer_request(peer string) {
	//TODO Structure content?
	pubsub_publish("peers", []byte(json_encode(Event{ID: uid(), Service: "peers", Action: "request", Content: peer})))
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
// TODO Make work with Go streams
func peer_send(peer string, content string) bool {
	p := peer_connect(peer)
	if p == nil {
		log_debug("Unable to connect to peer '%s'", peer)
		return false
	}

	w := bufio.NewWriter(bufio.NewWriter(*p.stream))
	_, err := w.WriteString(content + "\n")
	if err != nil {
		log_debug("libp2p unable to write event: %s", err)
		return false
	}

	err = w.Flush()
	if err != nil {
		log_debug("libp2p unable to flush event: %s", err)
		return false
	}

	return false
}

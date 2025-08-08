// Mochi server: Peers
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"bufio"
	"context"
	libp2p_pubsub "github.com/libp2p/go-libp2p-pubsub"
	libp2p_network "github.com/libp2p/go-libp2p/core/network"
	"sync"
	"time"
)

type Peer struct {
	ID      string `json:"id"`
	Address string `json:"address,omitempty"`
	Updated int64  `json:"updated"`
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
	peer_publish_chan                 = make(chan bool)
	peers_lock                        = &sync.Mutex{}
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
// TODO Better handle peers with multiple addresses
func peer_add(id string, address string, stream *libp2p_network.Stream) *Peer {
	if id == libp2p_id {
		log_debug("Peer ignoring request to add ourself")
		return nil
	}

	now := now()
	p := Peer{ID: id, Address: address, Updated: now, stream: stream}

	peers_lock.Lock()
	o, found := peers_connected[id]
	peers_lock.Unlock()

	if found {
		p.stream = o.stream

	} else if stream == nil {
		p.stream = libp2p_connect(id, address)
		if p.stream == nil {
			return nil
		}
	}

	peers_lock.Lock()
	peers_connected[id] = p
	peers_lock.Unlock()
	db := db_open("db/peers.db")
	db.exec("replace into peers ( id, address, updated ) values ( ?, ?, ? )", id, address, now)

	go events_check_queue("peer", id)
	return &p
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
func peer_connect(id string) *Peer {
	peers_lock.Lock()
	p, found := peers_connected[id]
	peers_lock.Unlock()
	if found {
		return &p
	}

	//TODO Better handle peers with multiple addresses
	var dp Peer
	db := db_open("db/peers.db")
	if !db.scan(&dp, "select * from peers where id=? order by updated desc limit 1", id) {
		return nil
	}
	return peer_add(id, dp.Address, nil)
}

// Publish our own information to the pubsub regularly or when requested
func peers_publish(t *libp2p_pubsub.Topic) {
	for {
		//TODO Change to hourly
		after := time.After(time.Minute)
		select {
		case <-peer_publish_chan:
			log_debug("Peer publish requested")
		case <-after:
			log_debug("Peer routine publish")
		}
		log_debug("Publishing peer")
		t.Publish(context.Background(), []byte(json_encode(Event{ID: uid(), Service: "peers", Action: "publish"})))
	}
}

// Received a peer publish event from another server
func peer_publish_event(e *Event) {
	//TODO Enable once libp2p_address is set
	//log_debug("Adding peer '%s' due to publish event from '%s'", e.libp2p_peer, e.libp2p_address)
	//peer_add(e.libp2p_peer, e.libp2p_address, nil)
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
	log_debug("Sending to peer '%s': %s", peer, content)
	p := peer_connect(peer)
	if p == nil {
		log_debug("Unable to connect to peer '%s'", peer)
		return false
	}
	if p.stream == nil {
		log_warn("Peer '%s' has no stream", peer)
		return false
	}

	w := bufio.NewWriter(*p.stream)
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

	return true
}

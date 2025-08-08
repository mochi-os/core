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
	ID        string           `json:"id"`
	Address   string           `json:"-"`
	Updated   int64            `json:"updated,omitempty"`
	addresses map[string]int64 `json:"-"`
	stream    *libp2p_network.Stream
}

const (
	peers_minimum = 1
)

var (
	peers_bootstrap = []Peer{
		Peer{ID: "12D3KooWRbpjpRmFiK7v6wRXA6yvAtTXXfvSE6xjbHVFFSaxN8SH", addresses: map[string]int64{"/ip4/217.182.75.108/tcp/1443/p2p/12D3KooWRbpjpRmFiK7v6wRXA6yvAtTXXfvSE6xjbHVFFSaxN8SH": now(), "/ip6/2001:41d0:601:1100::61f7/tcp/1443/p2p/12D3KooWRbpjpRmFiK7v6wRXA6yvAtTXXfvSE6xjbHVFFSaxN8SH": now()}},
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

// Add some peers we already know about from the database
func peers_add_from_db(limit int) {
	var ps []Peer
	db := db_open("db/peers.db")
	db.scans(&ps, "select id from peers group by id order by updated desc limit ?", limit)
	for _, p := range ps {
		var addresses []string
		var as []Peer
		db.scans(&as, "select address from peers where id=?", p.ID)
		for _, a := range as {
			addresses = append(addresses, a.Address)
		}
		log_debug("Adding database peer '%s' at %v", p.ID, addresses)
		peer_update(p.ID, nil, addresses...)
	}
}

// Get addresses of a peer as a slice
func (p Peer) addresses_as_slice() []string {
	var results []string
	for address, _ := range p.addresses {
		results = append(results, address)
	}
	return results
}

// Connect to a peer, unless already connected
func peer_connect(id string) *Peer {
	// Check if we're already connected
	peers_lock.Lock()
	p, found := peers_connected[id]
	peers_lock.Unlock()
	if found {
		return &p
	}

	// Load from database
	now := now()
	var as []Peer
	db := db_open("db/peers.db")
	db.scans(&as, "select * from peers where id=?", id)
	if len(as) > 0 {
		var addresses []string
		for _, a := range as {
			addresses = append(addresses, a.Address)
		}
		log_debug("Adding database peer '%s' at %v", p.ID, addresses)
		p = Peer{ID: id, Updated: now, stream: libp2p_connect(id, addresses...)}
		if p.stream != nil {
			for _, address := range addresses {
				p.addresses[address] = now
			}
			peers_lock.Lock()
			peers_connected[id] = p
			peers_lock.Unlock()
			return &p
		}
	}

	// Can't connect to it at this time. Send a publish request to the pubsub and give up.
	log_debug("Peer '%s' unknown. Sending pubsub request for it.", id)
	pubsub_publish("peers", []byte(json_encode(Event{ID: uid(), Service: "peers", Action: "request", Content: json_encode(Peer{ID: id})})))
	return nil
}

// Publish our own information to the pubsub regularly or when requested
func peers_publish(t *libp2p_pubsub.Topic) {
	for {
		after := time.After(time.Hour)
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
	//peer_update(e.libp2p_peer, nil, e.libp2p_address)
}

// Reply to a peer request if for us
func peer_request_event(e *Event) {
	log_debug("Received peer request event '%#v'", e)
	var p Peer
	if json_decode(&p, e.Content) && p.ID == libp2p_id {
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

// Add or update a peer
// If the peer connected to us stream will be the stream libp2p opened for us, otherwise stream will be nil.
func peer_update(id string, stream *libp2p_network.Stream, addresses ...string) *Peer {
	log_debug("Peer updating '%s' at %v", id, addresses)
	if id == libp2p_id {
		log_debug("Peer ignoring request to add ourself")
		return nil
	}

	peers_lock.Lock()
	p, found := peers_connected[id]
	peers_lock.Unlock()

	now := now()
	if found {
		for _, address := range addresses {
			p.addresses[address] = now
		}
		p.Updated = now

	} else {
		if stream == nil {
			stream = libp2p_connect(id, addresses...)
			if stream == nil {
				return nil
			}
		}
		p = Peer{ID: id, addresses: map[string]int64{}, Updated: now, stream: stream}
		for _, address := range addresses {
			p.addresses[address] = now
		}
	}

	peers_lock.Lock()
	peers_connected[id] = p
	peers_lock.Unlock()

	db := db_open("db/peers.db")
	for _, address := range addresses {
		db.exec("replace into peers ( id, address, updated ) values ( ?, ?, ? )", id, address, now)
	}

	go events_check_queue("peer", id)
	return &p
}

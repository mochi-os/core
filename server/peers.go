// Mochi server: Peers
// Copyright Alistair Cunningham 2024-2025

package main

import (
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
	connected bool
}

const (
	peers_minimum = 1
)

var (
	peers_bootstrap = []Peer{
		Peer{ID: "12D3KooWRbpjpRmFiK7v6wRXA6yvAtTXXfvSE6xjbHVFFSaxN8SH", addresses: map[string]int64{"/ip4/217.182.75.108/tcp/1443/p2p/12D3KooWRbpjpRmFiK7v6wRXA6yvAtTXXfvSE6xjbHVFFSaxN8SH": now(), "/ip6/2001:41d0:601:1100::61f7/tcp/1443/p2p/12D3KooWRbpjpRmFiK7v6wRXA6yvAtTXXfvSE6xjbHVFFSaxN8SH": now()}},
	}
	peers             map[string]Peer = map[string]Peer{}
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
		peer_update(p.ID, addresses...)
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

// Get details of a peer, either from memory, or from database
func peer_by_id(id string) *Peer {
	peers_lock.Lock()
	p, found := peers[id]
	peers_lock.Unlock()
	if found {
		log_debug("Already have peer '%s' at %v", id, p.addresses)
		return &p
	}

	// Load from database
	now := now()
	p = Peer{ID: id, Updated: now, connected: false}

	var ps []Peer
	db := db_open("db/peers.db")
	db.scans(&ps, "select * from peers where id=?", id)
	if len(ps) == 0 {
		log_debug("Peer '%s' not found in database", id)
		return nil
	}
	for _, a := range ps {
		log_debug("Peer '%s' adding address '%s' from database", id, a.Address)
		p.addresses[a.Address] = now
	}

	peers_lock.Lock()
	peers[id] = p
	peers_lock.Unlock()

	log_debug("Adding database peer '%s' at %v", id, p.addresses)
	return &p
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
		ev := event("", "", "", "publish")
		ev.publish("peers", false)
	}
}

// Received a peer publish event from another server
func peer_publish_event(e *Event) {
	//TODO Enable once libp2p_address is set
	//log_debug("Adding peer '%s' due to publish event from '%s'", e.libp2p_peer, e.libp2p_address)
	//peer_update(e.libp2p_peer, e.libp2p_address)
}

// Reply to a peer request if for us
func peer_request_event(e *Event) {
	log_debug("Received peer request event '%#v'", e)
	if e.get("id", "") == libp2p_id {
		log_debug("Peer request is for us; requesting a re-publish")
		peer_publish_chan <- true
	}
}

// Get a stream to a peer, connecting if necessary
func peer_stream(id string) libp2p_network.Stream {
	if id == "" {
		return nil
	}

	p := peer_by_id(id)
	if p == nil {
		log_debug("Peer '%s' unknown, sending pubsub request for it", id)
		ev := event("", "", "peers", "request")
		ev.set("id", id)
		ev.publish("peers", false)
		return nil
	}

	if !p.connected {
		p.connected = libp2p_connect(id, p.addresses_as_slice()...)
		if !p.connected {
			return nil
		}
	}

	return libp2p_stream(id)
}

// Check whether we have enough peers
func peers_sufficient() bool {
	peers_lock.Lock()
	count := len(peers)
	peers_lock.Unlock()

	if count >= peers_minimum {
		return true
	}
	return false
}

// Add or update a peer
func peer_update(id string, addresses ...string) {
	log_debug("Peer updating '%s' at %v", id, addresses)
	now := now()
	update_db := false

	peers_lock.Lock()
	p, found := peers[id]
	peers_lock.Unlock()

	if found && p.Updated < now-int64(3600) {
		update_db = true

	} else if !found {
		p = Peer{ID: id, addresses: map[string]int64{}, connected: false}
		update_db = true
	}
	for _, address := range addresses {
		_, address_found := p.addresses[address]
		if !address_found {
			update_db = true
		}
		p.addresses[address] = now
	}

	p.Updated = now
	peers_lock.Lock()
	peers[id] = p
	peers_lock.Unlock()

	if update_db {
		log_debug("Updating peer '%s' in database", id)
		db := db_open("db/peers.db")
		for _, address := range addresses {
			db.exec("replace into peers ( id, address, updated ) values ( ?, ?, ? )", id, address, now)
		}
	}

	go queue_check_peer(id)
}

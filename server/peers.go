// Mochi server: Peers
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"io"
	"sync"
	"time"
)

type Peer struct {
	ID        string
	Address   string
	Updated   int64
	addresses []string
	connected bool
}

const (
	peers_minimum = 1
)

var (
	peers_bootstrap = []Peer{
		Peer{ID: "12D3KooWRbpjpRmFiK7v6wRXA6yvAtTXXfvSE6xjbHVFFSaxN8SH", addresses: []string{"/ip4/217.182.75.108/tcp/1443/p2p/12D3KooWRbpjpRmFiK7v6wRXA6yvAtTXXfvSE6xjbHVFFSaxN8SH", "/ip6/2001:41d0:601:1100::61f7/tcp/1443/p2p/12D3KooWRbpjpRmFiK7v6wRXA6yvAtTXXfvSE6xjbHVFFSaxN8SH"}},
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
		debug("Adding database peer '%s' at %v", p.ID, addresses)
		peer_add_known(p.ID, addresses)
		go peer_connect(p.ID)
	}
}

// Add already known peer to memory if not already present
func peer_add_known(id string, addresses []string) {
	debug("Peer adding known '%s' at %v", id, addresses)

	peers_lock.Lock()
	_, found := peers[id]
	peers_lock.Unlock()
	if found {
		return
	}

	peers_lock.Lock()
	peers[id] = Peer{ID: id, addresses: addresses, connected: false}
	peers_lock.Unlock()
}

// Get details of a peer, either from memory, or from database
func peer_by_id(id string) *Peer {
	peers_lock.Lock()
	p, found := peers[id]
	peers_lock.Unlock()
	if found {
		return &p
	}

	// Load from database
	p = Peer{ID: id, connected: false}

	var ps []Peer
	db := db_open("db/peers.db")
	db.scans(&ps, "select * from peers where id=?", id)
	if len(ps) == 0 {
		debug("Peer '%s' not found in database", id)
		return nil
	}
	for _, a := range ps {
		debug("Peer '%s' adding address '%s' from database", id, a.Address)
		p.addresses = append(p.addresses, a.Address)
	}

	peers_lock.Lock()
	peers[id] = p
	peers_lock.Unlock()

	debug("Adding database peer '%s' at %v", id, p.addresses)
	return &p
}

// Connect to a peer if possible
// Call peer_add_known() or peer_discovered() before calling peer_connect()
func peer_connect(id string) bool {
	peers_lock.Lock()
	p, found := peers[id]
	peers_lock.Unlock()

	if !found {
		return false
	}

	//TODO Detect when peer becomes disconnected and mark it as such
	if p.connected {
		return true
	}
	p.connected = p2p_connect(id, p.addresses)

	peers_lock.Lock()
	peers[id] = p
	peers_lock.Unlock()

	return p.connected
}

// Peer has become disconnected
//TODO Test peer disconnected
func peer_disconnected(id string) {
	debug("Peer '%s' disconnected", id)

	peers_lock.Lock()
	p, found := peers[id]
	peers_lock.Unlock()

	if found {
		p.connected = false
		peers_lock.Lock()
		peers[id] = p
		peers_lock.Unlock()
	}
}

// New or existing peer discovered or re-discovered
func peer_discovered(id string, address string) {
	now := now()
	save := false

	peers_lock.Lock()
	p, found := peers[id]
	peers_lock.Unlock()

	if found {
		debug("Peer '%s' rediscovered at '%s'", id, address)
		exists := false
		for _, a := range p.addresses {
			if a == address {
				exists = true
				break
			}
		}
		if !exists {
			p.addresses = append(p.addresses, address)
		}

		if p.Updated < now-int64(3600) {
			save = true
		}

	} else {
		debug("Peer '%s' discovered at '%s'", id, address)
		p = Peer{ID: id, addresses: []string{address}}
		save = true
	}

	if save {
		db := db_open("db/peers.db")
		debug("Peer saving in database: '%s' at '%s'", id, address)
		db.exec("replace into peers ( id, address, updated ) values ( ?, ?, ? )", id, address, now)
		p.Updated = now
	}

	peers_lock.Lock()
	peers[id] = p
	peers_lock.Unlock()

	go queue_check_peer(id)
}

// Clean up stale peers
func peers_manager() {
	for {
		time.Sleep(24 * time.Hour)
		db := db_open("db/peers.db")
		db.exec("delete from peers where updated<?", now()-100*86400)
	}
}

// Publish our own information to the pubsub regularly or when requested
func peers_publish() {
	for {
		ev := event("", "", "peers", "publish")
		ev.publish(false)

		select {
		case <-peer_publish_chan:
			debug("Peer publish requested")
		case <-time.After(time.Hour):
			debug("Peer routine publish")
		}
	}
}

// Received a peer publish event from another server
func peer_publish_event(e *Event) {
	debug("Peer '%s' sent publish event from '%s'", e.p2p_peer, e.p2p_address)
	//TODO Enable and test once p2p_address is set
	//peer_discovered(e.p2p_peer, e.p2p_address)
	//peer_connect(e.p2p_peer)
}

// Reply to a peer request if for us
func peer_request_event(e *Event) {
	debug("Received peer request event '%#v'", e)
	if e.get("id", "") == p2p_id {
		debug("Peer request is for us; requesting a re-publish")
		peer_publish_chan <- true
	}
}

// Get a writer to a peer, connecting if necessary
func peer_writer(id string) io.WriteCloser {
	if id == "" {
		return nil
	}

	if id == p2p_id {
		debug("Sending event to ourself")
		r, w := io.Pipe()
		go event_receive(r, 1, p2p_id, "")
		return w
	}

	p := peer_by_id(id)
	if p == nil {
		// In a future version, rate limit this
		//TODO Test once p2p_address is set
		debug("Peer '%s' unknown, sending pubsub request for it", id)
		ev := event("", "", "peers", "request")
		ev.set("id", id)
		ev.publish(false)
		return nil
	}
	if !peer_connect(id) {
		return nil
	}
	return p2p_stream(id)
}

// Check whether we have enough peers to send broadcast events to, or whether to queue them
func peers_sufficient() bool {
	total := 0
	peers_lock.Lock()
	for _, p := range peers {
		if p.connected {
			total++
		}
	}
	peers_lock.Unlock()

	if total >= peers_minimum {
		return true
	}
	return false
}

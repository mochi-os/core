// Mochi server: Peers
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"encoding/json"
	"io"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	p2p_peer "github.com/libp2p/go-libp2p/core/peer"
	sl "go.starlark.net/starlark"
)

type Peer struct {
	ID        string
	Address   string
	Updated   int64
	addresses []string
	connected bool
}

const (
	peers_minimum          = 1
	peer_maximum_addresses = 20
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
	a.event("request", peer_request_event)
	a.event("publish", peer_publish_event)

	rand.Shuffle(len(peers_bootstrap), func(i, j int) {
		peers_bootstrap[i], peers_bootstrap[j] = peers_bootstrap[j], peers_bootstrap[i]
	})
}

// Add some peers we already know about from the database
func peers_add_from_db(limit int) {
	var ps []Peer
	db := db_open("db/peers.db")
	err := db.scans(&ps, "select id from peers group by id order by updated desc limit ?", limit)
	if err != nil {
		warn("Database error loading peers: %v", err)
		return
	}
	for _, p := range ps {
		var addresses []string
		var as []Peer
		err := db.scans(&as, "select address from peers where id=?", p.ID)
		if err != nil {
			warn("Database error loading addresses for peer %q: %v", p.ID, err)
			continue
		}
		for _, a := range as {
			addresses = append(addresses, a.Address)
		}
		debug("Adding database peer %q at %v", p.ID, addresses)
		peer_add_known(p.ID, addresses)
		go peer_connect(p.ID)
	}
}

// Add already known peer to memory if not already present
func peer_add_known(id string, addresses []string) {
	peers_lock.Lock()
	defer peers_lock.Unlock()

	if _, found := peers[id]; found {
		return
	}
	peers[id] = Peer{ID: id, addresses: addresses, connected: false}
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
	err := db.scans(&ps, "select * from peers where id=?", id)
	if err != nil {
		warn("Database error looking up peer %q: %v", id, err)
		return nil
	}
	if len(ps) == 0 {
		debug("Peer %q not found in database", id)
		return nil
	}
	for _, a := range ps {
		debug("Peer %q adding address %q from database", id, a.Address)
		p.addresses = append(p.addresses, a.Address)
	}

	peers_lock.Lock()
	peers[id] = p
	peers_lock.Unlock()

	debug("Adding database peer %q at %v", id, p.addresses)
	return &p
}

// Connect to a peer if possible
// Call peer_add_known(), peer_discovered(), or peer_discovered_address() before calling peer_connect()
func peer_connect(id string) bool {
	if id == p2p_id {
		return true
	}

	peers_lock.Lock()
	p, found := peers[id]
	peers_lock.Unlock()

	if !found {
		return false
	}

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
func peer_disconnected(id string) {
	if id == "" {
		return
	}
	debug("Peer %q disconnected", id)

	peers_lock.Lock()
	defer peers_lock.Unlock()

	if p, found := peers[id]; found {
		p.connected = false
		peers[id] = p
	}
}

// New or existing peer discovered or re-discovered at unknown address
func peer_discovered(id string) {
	p, err := p2p_peer.Decode(id)
	if err != nil {
		return
	}

	for _, a := range p2p_me.Peerstore().Addrs(p) {
		peer_discovered_work(id, a.String()+"/p2p/"+id)
	}

	go queue_check_peer(id)
}

// New or existing peer discovered or re-discovered at known address
func peer_discovered_address(id string, address string) {
	peer_discovered_work(id, address)
	go queue_check_peer(id)
}

// Do the work for the above two function
func peer_discovered_work(id string, address string) {
	now := now()
	save := false

	peers_lock.Lock()
	p, found := peers[id]

	if found {
		exists := false
		for _, a := range p.addresses {
			if a == address {
				exists = true
				break
			}
		}
		if !exists && len(p.addresses) < peer_maximum_addresses {
			p.addresses = append(p.addresses, address)
		}

		if p.Updated < now-int64(3600) {
			save = true
			p.Updated = now
		}
	} else {
		p = Peer{ID: id, addresses: []string{address}, Updated: now}
		save = true
	}

	peers[id] = p
	peers_lock.Unlock()

	if save {
		db := db_open("db/peers.db")
		db.exec("replace into peers ( id, address, updated ) values ( ?, ?, ? )", id, address, now)
	}
}

// Clean up stale peers
func peers_manager() {
	for range time.Tick(24 * time.Hour) {
		db := db_open("db/peers.db")
		db.exec("delete from peers where updated<?", now()-30*86400)
	}
}

// Publish our own information to the pubsub regularly or when requested
func peers_publish() {
	for {
		message("", "", "peers", "publish").publish(false)

		select {
		case <-peer_publish_chan:
			debug("Peer publish requested")
		case <-time.After(time.Hour):
			debug("Peer routine publish")
		}
	}
}

// Received a peer publish event from another server
// We don't need to do anything here because we've already
// marked the peer as discovered in p2p_pubsubs()
func peer_publish_event(e *Event) {
}

// Reply to a peer request if for us
func peer_request_event(e *Event) {
	if e.get("id", "") == p2p_id {
		peer_publish_chan <- true
	}
}

// Get a reader and writer to a peer, connecting if necessary
func peer_stream(id string) *Stream {
	if id == "" {
		return nil
	}

	if id == p2p_id {
		debug("Sending event to ourself")
		r1, w1 := io.Pipe()
		r2, w2 := io.Pipe()
		go stream_receive(&Stream{id: stream_id(), reader: &pipe_reader{PipeReader: r1}, writer: &pipe_writer{PipeWriter: w2}}, 1, p2p_id)
		return &Stream{id: stream_id(), reader: &pipe_reader{PipeReader: r2}, writer: &pipe_writer{PipeWriter: w1}}
	}

	p := peer_by_id(id)
	if p == nil {
		// In a future version, rate limit this
		debug("Peer %q unknown, sending pubsub request for it", id)
		message("", "", "peers", "request").set("id", id).publish(false)
		return nil
	}

	if !peer_connect(id) {
		return nil
	}

	return p2p_stream(id)
}

// Check whether we have enough peers to send broadcast messages to, or whether to queue them
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

// Notify peers of shutdown (best effort)
func peers_shutdown() {
	peers_lock.Lock()
	connected := []string{}
	for id, p := range peers {
		if p.connected {
			connected = append(connected, id)
		}
	}
	peers_lock.Unlock()

	info("Notifying %d connected peers of shutdown", len(connected))
	for _, id := range connected {
		s := p2p_stream(id)
		if s != nil && s.writer != nil {
			s.write(Headers{Type: "bye"})
			s.writer.Close()
		}
	}
}

// mochi.peer.connect.url(url) -> string: Connect to a peer by fetching P2P info from a URL
func api_peer_connect_url(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <url: string>")
	}

	url, ok := sl.AsString(args[0])
	if !ok || url == "" {
		return sl_error(fn, "invalid url")
	}

	// Normalize URL: add https:// if no scheme present
	if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
		url = "https://" + url
	}

	// Fetch P2P info from the server
	infoUrl := strings.TrimSuffix(url, "/") + "/_/p2p/info"
	resp, err := url_request("GET", infoUrl, nil, nil, nil)
	if err != nil {
		return sl_error(fn, "failed to fetch p2p info: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return sl_error(fn, "server returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return sl_error(fn, "failed to read response: %v", err)
	}

	// Parse JSON response
	var info struct {
		Peer      string   `json:"peer"`
		Addresses []string `json:"addresses"`
	}
	if err := json.Unmarshal(body, &info); err != nil {
		return sl_error(fn, "failed to parse p2p info: %v", err)
	}

	if info.Peer == "" || len(info.Addresses) == 0 {
		return sl_error(fn, "invalid p2p info: missing peer or addresses")
	}

	// Add peer and connect
	peer_add_known(info.Peer, info.Addresses)
	if !peer_connect(info.Peer) {
		return sl_error(fn, "failed to connect to peer %s", info.Peer)
	}

	return sl.String(info.Peer), nil
}

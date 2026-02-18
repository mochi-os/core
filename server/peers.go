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
	addresses []PeerAddress
	connected bool
}

// PeerAddress tracks a peer's address with a last-seen timestamp
type PeerAddress struct {
	Address string
	Updated int64
}

// peer_address_strings extracts address strings from a slice of PeerAddress
func peer_address_strings(addrs []PeerAddress) []string {
	out := make([]string, len(addrs))
	for i, a := range addrs {
		out[i] = a.Address
	}
	return out
}

const (
	peers_minimum          = 1
	peer_maximum_addresses = 20
)

// Reconnection state for a disconnected peer
type PeerReconnect struct {
	NextRetry int64
	Attempts  int
}

var (
	peer_default_publisher = "12D3KooWRbpjpRmFiK7v6wRXA6yvAtTXXfvSE6xjbHVFFSaxN8SH"
	peers_bootstrap        = []Peer{
		Peer{ID: "12D3KooWRbpjpRmFiK7v6wRXA6yvAtTXXfvSE6xjbHVFFSaxN8SH", addresses: []PeerAddress{
			{Address: "/ip4/217.182.75.108/tcp/1443/p2p/12D3KooWRbpjpRmFiK7v6wRXA6yvAtTXXfvSE6xjbHVFFSaxN8SH"},
			{Address: "/ip6/2001:41d0:601:1100::61f7/tcp/1443/p2p/12D3KooWRbpjpRmFiK7v6wRXA6yvAtTXXfvSE6xjbHVFFSaxN8SH"},
		}},
	}
	peers               map[string]Peer = map[string]Peer{}
	peer_publish_chan                   = make(chan bool)
	peers_lock                          = &sync.Mutex{}
	peer_reconnects                     = map[string]PeerReconnect{}
	peer_reconnect_lock                 = &sync.Mutex{}
)

func init() {
	a := app("peers")
	a.service("peers")
	a.event_anonymous("request", peer_request_event) // Unsigned pubsub broadcast
	a.event_anonymous("publish", peer_publish_event) // Unsigned pubsub broadcast

	rand.Shuffle(len(peers_bootstrap), func(i, j int) {
		peers_bootstrap[i], peers_bootstrap[j] = peers_bootstrap[j], peers_bootstrap[i]
	})
}

// peer_is_bootstrap returns true if the peer ID is a bootstrap peer
func peer_is_bootstrap(id string) bool {
	for _, p := range peers_bootstrap {
		if p.ID == id {
			return true
		}
	}
	return false
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
	t := now()
	pas := make([]PeerAddress, len(addresses))
	for i, a := range addresses {
		pas[i] = PeerAddress{Address: a, Updated: t}
	}
	peers[id] = Peer{ID: id, addresses: pas, connected: false}
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
		p.addresses = append(p.addresses, PeerAddress{Address: a.Address, Updated: a.Updated})
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
	p.connected = p2p_connect(id, peer_address_strings(p.addresses))

	// Refresh the timestamp of the address we actually connected on
	if p.connected {
		peer_refresh_connected_address(id)
		peer_reconnected(id)
	}

	peers_lock.Lock()
	peers[id] = p
	peers_lock.Unlock()

	return p.connected
}

// Refresh the timestamp of the address we actually connected on
func peer_refresh_connected_address(id string) {
	pid, err := p2p_peer.Decode(id)
	if err != nil {
		return
	}

	conns := p2p_me.Network().ConnsToPeer(pid)
	if len(conns) == 0 {
		return
	}

	t := now()
	addr := conns[0].RemoteMultiaddr().String() + "/p2p/" + id

	peers_lock.Lock()
	if p, found := peers[id]; found {
		for i, a := range p.addresses {
			if a.Address == addr {
				p.addresses[i].Updated = t
				peers[id] = p
				break
			}
		}
	}
	peers_lock.Unlock()

	db := db_open("db/peers.db")
	db.exec("replace into peers ( id, address, updated ) values ( ?, ?, ? )", id, addr, t)
}

// Peer has become disconnected
func peer_disconnected(id string) {
	if id == "" {
		return
	}
	debug("Peer %q disconnected", id)

	peers_lock.Lock()
	if p, found := peers[id]; found {
		p.connected = false
		peers[id] = p
	}
	peers_lock.Unlock()

	// Schedule reconnection if not already scheduled
	peer_reconnect_lock.Lock()
	if _, scheduled := peer_reconnects[id]; !scheduled {
		delay := int64(10) + rand.Int64N(5) // 10-14 seconds initial delay with jitter
		peer_reconnects[id] = PeerReconnect{NextRetry: now() + delay, Attempts: 0}
		debug("Peer %q scheduled for reconnection in %ds", id, delay)
	}
	peer_reconnect_lock.Unlock()
}

// Clear reconnection state for a peer (called when peer connects by any means)
func peer_reconnected(id string) {
	peer_reconnect_lock.Lock()
	delete(peer_reconnects, id)
	peer_reconnect_lock.Unlock()
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

// Do the work for the above two functions
func peer_discovered_work(id string, address string) {
	t := now()
	save := false

	peers_lock.Lock()
	p, found := peers[id]

	if found {
		exists := false
		for i, a := range p.addresses {
			if a.Address == address {
				exists = true
				p.addresses[i].Updated = t
				break
			}
		}
		if !exists {
			pa := PeerAddress{Address: address, Updated: t}
			if len(p.addresses) < peer_maximum_addresses {
				p.addresses = append(p.addresses, pa)
			} else {
				// Replace the oldest address
				oldest := 0
				for i, a := range p.addresses {
					if a.Updated < p.addresses[oldest].Updated {
						oldest = i
					}
				}
				p.addresses[oldest] = pa
			}
		}

		if p.Updated < t-int64(3600) {
			save = true
			p.Updated = t
		}
	} else {
		p = Peer{ID: id, addresses: []PeerAddress{{Address: address, Updated: t}}, Updated: t}
		save = true
	}

	peers[id] = p
	peers_lock.Unlock()

	if save {
		db := db_open("db/peers.db")
		db.exec("replace into peers ( id, address, updated ) values ( ?, ?, ? )", id, address, t)
	}
}

// Clean up stale peers
func peers_manager() {
	for range time.Tick(24 * time.Hour) {
		expiry := now() - 14*86400

		// Prune stale addresses from the database
		db := db_open("db/peers.db")
		db.exec("delete from peers where updated<?", expiry)

		// Prune stale addresses from memory
		peers_lock.Lock()
		for id, p := range peers {
			// Collect bootstrap addresses for this peer so we never prune them
			bootstrap := map[string]bool{}
			for _, bp := range peers_bootstrap {
				if bp.ID == id {
					for _, a := range bp.addresses {
						bootstrap[a.Address] = true
					}
					break
				}
			}

			// Keep addresses that are recent or are bootstrap hardcoded
			kept := []PeerAddress{}
			for _, a := range p.addresses {
				if a.Updated >= expiry || bootstrap[a.Address] {
					kept = append(kept, a)
				}
			}
			p.addresses = kept

			// Remove peer from memory if no addresses remain and not connected
			if len(p.addresses) == 0 && !p.connected {
				delete(peers, id)
			} else {
				peers[id] = p
			}
		}
		peers_lock.Unlock()
	}
}

// Reconnect to disconnected peers with exponential backoff
func peer_reconnect_manager() {
	for range time.Tick(10 * time.Second) {
		t := now()
		var ready []string

		// Collect peers ready for reconnection (max 3 per tick)
		peer_reconnect_lock.Lock()
		for id, r := range peer_reconnects {
			if r.NextRetry <= t {
				ready = append(ready, id)
				if len(ready) >= 3 {
					break
				}
			}
		}
		peer_reconnect_lock.Unlock()

		for _, id := range ready {
			if peer_connect(id) {
				debug("Peer %q reconnected successfully", id)
				continue
			}

			// Backoff: 10s, 20s, 40s, 80s, 160s, 300s (capped at 5 minutes)
			peer_reconnect_lock.Lock()
			r := peer_reconnects[id]
			r.Attempts++
			delay := int64(10) << min(r.Attempts, 5) // 20, 40, 80, 160, 320
			if delay > 300 {
				delay = 300
			}
			delay += rand.Int64N(delay/4 + 1) // 0-25% jitter
			r.NextRetry = now() + delay
			peer_reconnects[id] = r
			peer_reconnect_lock.Unlock()
			debug("Peer %q reconnect attempt %d failed, next retry in %ds", id, r.Attempts, delay)
		}
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

// Check whether we have enough peers in the pubsub mesh to send broadcast messages to
func peers_sufficient() bool {
	return len(p2p_pubsub_1.ListPeers()) >= peers_minimum
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
	info_url := strings.TrimSuffix(url, "/") + "/_/p2p/info"
	resp, err := url_request("GET", info_url, nil, nil, nil)
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

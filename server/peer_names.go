// Mochi server: Peer display names — hostname/domain claims carried in
// peers/publish, their DNS verification, and the display selection rule
// shared by every surface that shows a peer to a human.
//
// Trust model: GossipSub StrictSign proves which peer made a claim
// (Event.origin); DNS proves who owns a name. A claim verifies iff the
// name resolves to an IP we hold an authenticated, non-relayed libp2p
// connection to for that peer — the handshake has already proven the
// peer holds the key for its id, so name → IP → key closes the loop
// with zero configuration on the claimant's side: any server whose
// hostname already points at it verifies automatically. Accepted
// residual risk: servers sharing one inbound IP can claim each other's
// names — typically the same operator. Relayed connections are excluded
// as evidence (their remote address is the relay's IP, which would
// falsely verify any name pointing at the relay).
//
// Verification persists across disconnects and restarts — it proves the
// name↔key binding at check time, not connection liveness. Staleness is
// bounded by re-verification on reconnect, the daily sweep over
// connected peers, demotion on mismatch or claim change, and the 14-day
// prune that removes a dead peer's names entirely.
//
// Names are display-only. Nothing anywhere keys logic off them, and a
// dotted (domain-shaped) claim is never displayed unless verified — an
// impostor claiming someone's domain gets no UI credibility from it.
//
// Copyright Alistair Cunningham 2026

package main

import (
	"net"
	"os"
	"strings"
	"sync"

	p2p_peer "github.com/libp2p/go-libp2p/core/peer"
	multiaddr "github.com/multiformats/go-multiaddr"
	sl "go.starlark.net/starlark"
)

// PeerName is one claimed name for a peer. Claims keep their publish
// order (hostname first, then domains), so the first dotless claim is
// the announced machine hostname.
type PeerName struct {
	Name     string
	Verified bool
	Checked  int64 // last verification attempt; throttles lookups
	Updated  int64 // last time the claim appeared in a publish
}

// peer_name_row is the sqlx scan target for the peers.db names table.
type peer_name_row struct {
	ID       string
	Name     string
	Verified int64
	Checked  int64
	Updated  int64
}

const (
	peer_names_maximum = 5    // claims stored per peer: 1 hostname + 4 domains
	peer_names_recheck = 3600 // claim verdicts older than this re-verify on reconnect
)

var (
	peer_names      = map[string][]PeerName{}
	peer_names_lock = &sync.Mutex{}
	peer_names_chan = make(chan string, 256)

	// Resolver and connection-evidence hooks behind vars so tests
	// inject verdicts without DNS or a live libp2p host.
	peer_names_resolve  = net.LookupIP
	peer_names_evidence = peer_connected_ips
)

// peer_name_valid reports whether a claimed name is acceptable: ASCII
// RFC-1123 hostname grammar only, lowercase, 253 chars or fewer. The
// strictness is anti-spoofing — Unicode homoglyphs, RTL overrides and
// control characters never reach a screen.
func peer_name_valid(name string) bool {
	if name == "" || len(name) > 253 {
		return false
	}
	for _, label := range strings.Split(name, ".") {
		if label == "" || len(label) > 63 {
			return false
		}
		if label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for i := 0; i < len(label); i++ {
			c := label[i]
			if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '-' {
				return false
			}
		}
	}
	return true
}

// peer_names_announce composes this server's own announcement for
// peers/publish: the hostname (the `hostname` setting, defaulting to the
// OS hostname) and its served domains from the domain registry
// (wildcards collapse to their base). Both empty when the administrator
// has turned `hostname_publish` off.
func peer_names_announce() (string, string) {
	if setting_get("hostname_publish", "true") != "true" {
		return "", ""
	}

	name := strings.ToLower(strings.TrimSpace(setting_get("hostname", "")))
	if name == "" {
		if h, err := os.Hostname(); err == nil {
			name = strings.ToLower(strings.TrimSpace(h))
		}
	}
	if !peer_name_valid(name) {
		name = ""
	}

	var domains []string
	for _, d := range domain_list() {
		n := strings.ToLower(strings.TrimPrefix(d.Domain, "*."))
		if n == name || !peer_name_valid(n) {
			continue
		}
		duplicate := false
		for _, existing := range domains {
			if existing == n {
				duplicate = true
				break
			}
		}
		if duplicate {
			continue
		}
		domains = append(domains, n)
		if len(domains) == peer_names_maximum-1 {
			break
		}
	}

	return name, strings.Join(domains, ",")
}

// peer_names_apply merges a publish's claimed names for a peer. Claims
// keep publish order; existing claims keep their verification verdict;
// dropped claims are deleted (a peer that stops announcing wants
// anonymity — honor it). New or stale dotted claims queue verification.
func peer_names_apply(id string, names []string) {
	if id == "" || id == net_id {
		return
	}
	if len(names) > peer_names_maximum {
		names = names[:peer_names_maximum]
	}

	t := now()
	changed := false
	verify := false

	peer_names_lock.Lock()
	existing := peer_names[id]
	var next []PeerName
	for _, n := range names {
		duplicate := false
		for _, c := range next {
			if c.Name == n {
				duplicate = true
				break
			}
		}
		if duplicate {
			continue
		}
		kept := false
		for _, c := range existing {
			if c.Name == n {
				c.Updated = t
				next = append(next, c)
				kept = true
				break
			}
		}
		if !kept {
			next = append(next, PeerName{Name: n, Updated: t})
			changed = true
		}
	}
	if len(next) != len(existing) {
		changed = true
	}
	if len(next) == 0 {
		delete(peer_names, id)
	} else {
		peer_names[id] = next
	}
	for _, c := range next {
		if strings.Contains(c.Name, ".") && !c.Verified && t-c.Checked > peer_names_recheck {
			verify = true
		}
	}
	peer_names_lock.Unlock()

	if changed {
		debug("Peer %q announced names %v", id, names)
		peer_names_save(id)
	}
	if verify {
		peer_names_enqueue(id)
	}
}

// peer_name returns the display name for a peer and whether it is
// verified. Selection: the first verified claim wins; otherwise the
// first dotless claim (the announced machine hostname), unverified.
// A dotted claim is never returned unverified.
func peer_name(id string) (string, bool) {
	peer_names_lock.Lock()
	defer peer_names_lock.Unlock()
	for _, c := range peer_names[id] {
		if c.Verified {
			return c.Name, true
		}
	}
	for _, c := range peer_names[id] {
		if !strings.Contains(c.Name, ".") {
			return c.Name, false
		}
	}
	return "", false
}

// peer_names_load fills the in-memory claim registry from peers.db at
// startup. Dotless claims order first so the hostname-first publish
// order survives the round trip.
func peer_names_load() {
	var rows []peer_name_row
	db := db_open("db/peers.db")
	if err := db.scans(&rows, "select id, name, verified, checked, updated from names order by id, (name like '%.%'), name"); err != nil {
		warn("Database error loading peer names: %v", err)
		return
	}
	peer_names_lock.Lock()
	for _, r := range rows {
		peer_names[r.ID] = append(peer_names[r.ID], PeerName{Name: r.Name, Verified: r.Verified != 0, Checked: r.Checked, Updated: r.Updated})
	}
	peer_names_lock.Unlock()
}

// peer_names_save persists a peer's current claims, replacing whatever
// peers.db held for it.
func peer_names_save(id string) {
	peer_names_lock.Lock()
	claims := append([]PeerName{}, peer_names[id]...)
	peer_names_lock.Unlock()

	db := db_open("db/peers.db")
	db.exec("delete from names where id=?", id)
	for _, c := range claims {
		verified := 0
		if c.Verified {
			verified = 1
		}
		db.exec("replace into names ( id, name, verified, checked, updated ) values ( ?, ?, ?, ?, ? )", id, c.Name, verified, c.Checked, c.Updated)
	}
}

// peer_names_enqueue hands a peer to the verification worker.
// Non-blocking — a full queue drops the nudge; the reconnect hook and
// daily sweep re-raise it.
func peer_names_enqueue(id string) {
	select {
	case peer_names_chan <- id:
	default:
	}
}

// peer_names_connected queues a re-verification when a peer we just
// connected to has dotted claims with stale verdicts — freshness checked
// exactly when fresh evidence is available.
func peer_names_connected(id string) {
	t := now()
	need := false
	peer_names_lock.Lock()
	for _, c := range peer_names[id] {
		if strings.Contains(c.Name, ".") && t-c.Checked > peer_names_recheck {
			need = true
			break
		}
	}
	peer_names_lock.Unlock()
	if need {
		peer_names_enqueue(id)
	}
}

// peer_names_manager is the verification worker. Serial by design —
// verification volume is one peer per publish/reconnect and a daily
// sweep, and a slow DNS resolver must never fan out.
func peer_names_manager() {
	for id := range peer_names_chan {
		peer_names_verify(id)
	}
}

// peer_names_verify checks a peer's dotted claims against DNS: a claim
// verifies iff a resolved IP matches an authenticated, non-relayed
// connection to the peer. Without such a connection it dials the peer's
// known addresses once; failing that, verdicts stand until better
// evidence (the disconnected case learns nothing new).
func peer_names_verify(id string) {
	peer_names_lock.Lock()
	claims := append([]PeerName{}, peer_names[id]...)
	peer_names_lock.Unlock()

	dotted := false
	for _, c := range claims {
		if strings.Contains(c.Name, ".") {
			dotted = true
			break
		}
	}
	if !dotted {
		return
	}

	ips := peer_names_evidence(id)
	if len(ips) == 0 {
		if !peer_connect(id) {
			return
		}
		ips = peer_names_evidence(id)
		if len(ips) == 0 {
			return
		}
	}

	for i, c := range claims {
		if !strings.Contains(c.Name, ".") {
			continue
		}
		t := now()
		resolved, err := peer_names_resolve(c.Name)
		verified := c.Verified
		if err != nil {
			// A definitive "name does not exist" demotes; a transient
			// resolver failure keeps the last verdict.
			if e, ok := err.(*net.DNSError); ok && e.IsNotFound {
				verified = false
			}
		} else {
			verified = false
			for _, r := range resolved {
				for _, ip := range ips {
					if r.String() == ip {
						verified = true
						break
					}
				}
				if verified {
					break
				}
			}
		}
		if verified != c.Verified {
			debug("Peer %q name %q verification now %v", id, c.Name, verified)
		}
		claims[i].Verified = verified
		claims[i].Checked = t
	}

	peer_names_lock.Lock()
	current := peer_names[id]
	for i := range current {
		for _, c := range claims {
			if current[i].Name == c.Name {
				current[i].Verified = c.Verified
				current[i].Checked = c.Checked
				break
			}
		}
	}
	peer_names_lock.Unlock()

	peer_names_save(id)
}

// peer_name_dict adds the display fields for a peer to a starlark dict:
// name, verified, fingerprint. In approval contexts (a human deciding
// whether to trust the peer) unverified names are withheld entirely —
// only DNS-verified names may influence a trust decision's reader.
func peer_name_dict(entry *sl.Dict, id string, approval bool) {
	name, verified := peer_name(id)
	if approval && !verified {
		name = ""
	}
	_ = entry.SetKey(sl.String("name"), sl.String(name))
	_ = entry.SetKey(sl.String("verified"), sl.Bool(verified && name != ""))
	_ = entry.SetKey(sl.String("fingerprint"), sl.String(fingerprint(id)))
}

// peer_name_fields is peer_name_dict for JSON-bound maps (gin handlers).
func peer_name_fields(m map[string]any, id string, approval bool) {
	name, verified := peer_name(id)
	if approval && !verified {
		name = ""
	}
	m["name"] = name
	m["verified"] = verified && name != ""
	m["fingerprint"] = fingerprint(id)
}

// peer_connected_ips returns the remote IPs of the authenticated direct
// connections to a peer — the evidence set for name verification.
// Relayed connections are skipped: their remote address is the relay's.
func peer_connected_ips(id string) []string {
	if net_me == nil {
		return nil
	}
	pid, err := p2p_peer.Decode(id)
	if err != nil {
		return nil
	}
	var out []string
	for _, conn := range net_me.Network().ConnsToPeer(pid) {
		ma := conn.RemoteMultiaddr()
		if _, err := ma.ValueForProtocol(multiaddr.P_CIRCUIT); err == nil {
			continue
		}
		if v, err := ma.ValueForProtocol(multiaddr.P_IP4); err == nil {
			out = append(out, v)
		}
		if v, err := ma.ValueForProtocol(multiaddr.P_IP6); err == nil {
			out = append(out, v)
		}
	}
	return out
}

// peer_names_sweep is the daily maintenance pass, called from
// peers_manager with the same 14-day expiry as the peer prune: drops
// claims whose peer has gone quiet, and re-queues verification for
// connected peers whose verdicts are older than a day.
func peer_names_sweep(expiry int64) {
	db := db_open("db/peers.db")
	db.exec("delete from names where updated<?", expiry)

	stale := now() - 86400
	var recheck []string
	peer_names_lock.Lock()
	for id, claims := range peer_names {
		kept := claims[:0]
		due := false
		for _, c := range claims {
			if c.Updated >= expiry {
				kept = append(kept, c)
				if strings.Contains(c.Name, ".") && c.Checked < stale {
					due = true
				}
			}
		}
		if len(kept) == 0 {
			delete(peer_names, id)
			continue
		}
		peer_names[id] = kept
		if due {
			recheck = append(recheck, id)
		}
	}
	peer_names_lock.Unlock()

	for _, id := range recheck {
		if len(peer_connected_ips(id)) > 0 {
			peer_names_enqueue(id)
		}
	}
}

// Mochi server: Signed peer records - self-certifying address claims. Trust is
// in the record's own libp2p signature rather than the transport, so any holder
// can verify it and relay it on the subject's behalf, and the monotonic
// sequence rejects replays. We announce the envelope libp2p already keeps for
// our host.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"encoding/base64"
	"math/rand/v2"
	"sync"
	"time"

	p2p_peer "github.com/libp2p/go-libp2p/core/peer"
	p2p_peerstore "github.com/libp2p/go-libp2p/core/peerstore"
	p2p_record "github.com/libp2p/go-libp2p/core/record"
)

// SignedRecord is a peer's latest self-certifying address record: the
// marshalled libp2p envelope (relayable verbatim) and the record's
// monotonic sequence number, which gates replays.
type SignedRecord struct {
	Envelope []byte
	Sequence uint64
	Updated  int64
}

// peer_record_row is the sqlx scan target for the peers.db records table.
// Sequence is a libp2p uint64; sqlite's signed integer holds its
// nanosecond-timestamp values losslessly.
type peer_record_row struct {
	ID       string
	Record   []byte
	Sequence int64
	Updated  int64
}

var (
	peer_records      = map[string]SignedRecord{}
	peer_records_lock = &sync.Mutex{}

	// peer_records_answered tracks when we last saw any server relay a
	// record for a given peer, so a thundering herd of holders answering
	// one peers/request can suppress itself down to a handful.
	peer_records_answered      = map[string]int64{}
	peer_records_answered_lock = &sync.Mutex{}
)

const (
	// peer_record_relay_maximum_age — only relay a record fresh enough
	// that its addresses are likely still good. A live peer refreshes
	// its record hourly, so an hour bounds relay to currently-active
	// peers.
	peer_record_relay_maximum_age = 3600

	// peer_record_relay_jitter — relaying waits a random fraction of
	// this (seconds) before answering, so simultaneous holders spread
	// out and the suppression below can take effect.
	peer_record_relay_jitter = 3

	// peer_record_answered_window — treat a peer as already answered if
	// another server relayed its record within this many seconds.
	peer_record_answered_window = 5
)

// peer_record_announce returns this server's own signed peer record as
// a base64 string for the peers/publish payload, or "" when libp2p has
// not yet produced one (briefly true at startup before identify
// settles, in which case the plain address list carries the publish).
func peer_record_announce() string {
	if net_me == nil {
		return ""
	}
	cab, ok := p2p_peerstore.GetCertifiedAddrBook(net_me.Peerstore())
	if !ok {
		return ""
	}
	pid, err := p2p_peer.Decode(net_id)
	if err != nil {
		return ""
	}
	env := cab.GetPeerRecord(pid)
	if env == nil {
		return ""
	}
	data, err := env.Marshal()
	if err != nil {
		return ""
	}
	return base64.StdEncoding.EncodeToString(data)
}

// peer_record_verify returns a signed record's self-asserted peer id, its
// /p2p-suffixed addresses, sequence and envelope bytes. The signature must
// validate and its key derive to the embedded PeerID, which is what makes a
// third party's record safe to relay. ok=false on any failure.
func peer_record_verify(encoded string) (id string, addresses []string, sequence uint64, data []byte, ok bool) {
	if encoded == "" {
		return "", nil, 0, nil, false
	}
	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return "", nil, 0, nil, false
	}
	env, raw, err := p2p_record.ConsumeEnvelope(data, p2p_peer.PeerRecordEnvelopeDomain)
	if err != nil {
		return "", nil, 0, nil, false
	}
	rec, ok := raw.(*p2p_peer.PeerRecord)
	if !ok || env.PublicKey == nil {
		return "", nil, 0, nil, false
	}
	derived, err := p2p_peer.IDFromPublicKey(env.PublicKey)
	if err != nil || derived != rec.PeerID {
		return "", nil, 0, nil, false
	}
	id = rec.PeerID.String()
	suffix := "/p2p/" + id
	addresses = make([]string, 0, len(rec.Addrs))
	for _, a := range rec.Addrs {
		addresses = append(addresses, a.String()+suffix)
	}
	return id, addresses, rec.Seq, data, true
}

// peer_record_store keeps a verified record only when its sequence is strictly
// newer than the stored one, and reports whether it was - the replay guard.
func peer_record_store(id string, sequence uint64, data []byte) bool {
	peer_records_lock.Lock()
	if stored, found := peer_records[id]; found && sequence <= stored.Sequence {
		peer_records_lock.Unlock()
		return false
	}
	peer_records[id] = SignedRecord{Envelope: data, Sequence: sequence, Updated: now()}
	peer_records_lock.Unlock()
	peer_record_save(id, data, sequence)
	return true
}

// peer_record_apply verifies a record announced by its own subject: the
// record's PeerID must equal the GossipSub-verified origin. That binding is
// what separates this from a relayed record, which any peer may carry.
func peer_record_apply(origin string, encoded string) ([]string, bool) {
	id, addresses, sequence, data, ok := peer_record_verify(encoded)
	if !ok || id != origin {
		return nil, false
	}
	if !peer_record_store(id, sequence, data) {
		return nil, false
	}
	return addresses, true
}

// peer_record_relay answers a peers/request for another peer by broadcasting
// that peer's signed record - safe because the record self-certifies. Bounded
// by a rate limit, a freshness floor, and jitter-plus-suppression.
func peer_record_relay(id string) {
	if !peer_record_relayable(id) {
		return
	}
	if !rate_limit_record_relay.allow(id) {
		return
	}

	peer_records_lock.Lock()
	encoded := base64.StdEncoding.EncodeToString(peer_records[id].Envelope)
	peer_records_lock.Unlock()

	go func() {
		time.Sleep(time.Duration(rand.Int64N(peer_record_relay_jitter*1000)) * time.Millisecond)
		// Suppress if another server already answered for this peer
		// during the wait.
		peer_records_answered_lock.Lock()
		seen := peer_records_answered[id]
		peer_records_answered_lock.Unlock()
		if seen >= now()-peer_record_answered_window {
			return
		}
		debug("Relaying signed record for peer %q", id)
		message("", "", "peers", "record").set("record", encoded).publish(false)
	}()
}

// peer_record_relayable reports whether we hold a record for id fresh
// enough to relay on its behalf — never our own id, and only a record
// refreshed within the freshness floor so we don't reintroduce a peer
// at a stale address.
func peer_record_relayable(id string) bool {
	if id == "" || id == net_id {
		return false
	}
	peer_records_lock.Lock()
	defer peer_records_lock.Unlock()
	r, found := peer_records[id]
	return found && r.Updated >= now()-peer_record_relay_maximum_age
}

// peer_record_current reports whether a relayed record is at least as new as
// the one we hold. peer_record_store's return cannot answer this: it is false
// for an EQUAL sequence too, and equal is the ordinary case. Holding no record
// reports current, since we cannot relay what we do not have.
func peer_record_current(id string, sequence uint64) bool {
	peer_records_lock.Lock()
	defer peer_records_lock.Unlock()
	stored, found := peer_records[id]
	return !found || sequence >= stored.Sequence
}

// peer_record_seen marks that we observed a relayed record for a peer,
// feeding peer_record_relay's suppression.
func peer_record_seen(id string) {
	peer_records_answered_lock.Lock()
	peer_records_answered[id] = now()
	peer_records_answered_lock.Unlock()
}

// peer_record_save persists a peer's latest record, replacing any
// earlier one. Upsert so a refresh keeps the row's identity.
func peer_record_save(id string, data []byte, sequence uint64) {
	db := db_open("db/peers.db")
	db.exec("insert into records ( id, record, sequence, updated ) values ( ?, ?, ?, ? ) on conflict ( id ) do update set record=excluded.record, sequence=excluded.sequence, updated=excluded.updated", id, data, int64(sequence), now())
}

// peer_records_load fills the in-memory record cache from peers.db at
// startup so replay rejection survives restarts.
func peer_records_load() {
	var rows []peer_record_row
	db := db_open("db/peers.db")
	if err := db.scans(&rows, "select id, record, sequence, updated from records"); err != nil {
		warn("Database error loading peer records: %v", err)
		return
	}
	peer_records_lock.Lock()
	for _, r := range rows {
		peer_records[r.ID] = SignedRecord{Envelope: r.Record, Sequence: uint64(r.Sequence), Updated: r.Updated}
	}
	peer_records_lock.Unlock()
}

// peer_records_sweep drops records for peers gone quiet, with the same
// expiry as the address prune. A live peer republishes hourly, keeping
// its record fresh.
func peer_records_sweep(expiry int64) {
	db := db_open("db/peers.db")
	db.exec("delete from records where updated<?", expiry)

	peer_records_lock.Lock()
	for id, r := range peer_records {
		if r.Updated < expiry {
			delete(peer_records, id)
		}
	}
	peer_records_lock.Unlock()

	// The suppression map only matters within its few-second window;
	// drop everything older so it can't grow unbounded.
	stale := now() - peer_record_answered_window
	peer_records_answered_lock.Lock()
	for id, t := range peer_records_answered {
		if t < stale {
			delete(peer_records_answered, id)
		}
	}
	peer_records_answered_lock.Unlock()
}

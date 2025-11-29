// Mochi server: Queue
// Copyright Alistair Cunningham 2025

package main

import (
	"time"
)

type QueueBroadcast struct {
	ID      string
	Data    []byte
	Created int64
}

type QueueEntity struct {
	ID         string `db:"id"`
	Entity     string `db:"entity"`
	FromEntity string `db:"from_entity"`
	Service    string `db:"service"`
	Event      string `db:"event"`
	Content    []byte `db:"content"`
	Data       []byte `db:"data"`
	File       string `db:"file"`
	Created    int64  `db:"created"`
}

type QueuePeer struct {
	ID         string `db:"id"`
	Peer       string `db:"peer"`
	FromEntity string `db:"from_entity"`
	ToEntity   string `db:"to_entity"`
	Service    string `db:"service"`
	Event      string `db:"event"`
	Content    []byte `db:"content"`
	Data       []byte `db:"data"`
	File       string `db:"file"`
	Created    int64  `db:"created"`
}

const (
	maximum_queue_time = 7 * 86400
)

// Check if there any queued messages to an entity, and if so try resending them
func queue_check_entity(entity string) {
	var qs []QueueEntity
	db := db_open("db/queue.db")
	db.scans(&qs, "select * from entities where entity=?", entity)
	for _, q := range qs {
		debug("Trying to send queued event %q to entity %q", q.ID, q.Entity)
		peer := entity_peer(q.Entity)
		if peer != "" {
			debug("Entity %q is at peer %q", q.ID, peer)
			if queue_entity_send(peer, &q) {
				debug("Removing sent event from queue")
				db.exec("delete from entities where id=?", q.ID)
			}
		}
		time.Sleep(time.Millisecond)
	}
}

// Check if there any queued messages to a peer, and if so try resending them
func queue_check_peer(peer string) {
	var qs []QueuePeer
	db := db_open("db/queue.db")
	db.scans(&qs, "select * from peers where peer=?", peer)
	for _, q := range qs {
		debug("Trying to send queued event %q to peer %q", q.ID, q.Peer)
		if queue_peer_send(&q) {
			debug("Removing sent event from queue")
			db.exec("delete from peers where id=?", q.ID)
		}
		time.Sleep(time.Millisecond)
	}
}

// Send a queued entity message (sign at send time)
func queue_entity_send(peer string, q *QueueEntity) bool {
	s := peer_stream(peer)
	if s == nil {
		debug("Unable to create stream to peer, keeping in queue")
		return false
	}

	// Sign with fresh timestamp and nonce
	timestamp := now()
	nonce := uid()
	signature := entity_sign(q.FromEntity, string(signable_headers(q.FromEntity, q.Entity, q.Service, q.Event, timestamp, nonce)))

	headers := cbor_encode(Headers{
		From: q.FromEntity, To: q.Entity, Service: q.Service, Event: q.Event,
		Timestamp: timestamp, Nonce: nonce, Signature: signature,
	})

	if s.write_raw(headers) != nil {
		return false
	}
	if len(q.Content) > 0 && s.write_raw(q.Content) != nil {
		return false
	}
	if len(q.Data) > 0 && s.write_raw(q.Data) != nil {
		return false
	}
	if q.File != "" && s.write_file(q.File) != nil {
		return false
	}

	return true
}

// Send a queued peer message (sign at send time)
func queue_peer_send(q *QueuePeer) bool {
	s := peer_stream(q.Peer)
	if s == nil {
		debug("Unable to create stream to peer, keeping in queue")
		return false
	}

	// Sign with fresh timestamp and nonce
	timestamp := now()
	nonce := uid()
	signature := entity_sign(q.FromEntity, string(signable_headers(q.FromEntity, q.ToEntity, q.Service, q.Event, timestamp, nonce)))

	headers := cbor_encode(Headers{
		From: q.FromEntity, To: q.ToEntity, Service: q.Service, Event: q.Event,
		Timestamp: timestamp, Nonce: nonce, Signature: signature,
	})

	if s.write_raw(headers) != nil {
		return false
	}
	if len(q.Content) > 0 && s.write_raw(q.Content) != nil {
		return false
	}
	if len(q.Data) > 0 && s.write_raw(q.Data) != nil {
		return false
	}
	if q.File != "" && s.write_file(q.File) != nil {
		return false
	}

	return true
}

// Manage queued messages, nudging them or deleting them if they time out
func queue_manager() {
	db := db_open("db/queue.db")

	for {
		time.Sleep(time.Minute)
		if peers_sufficient() {
			var qe QueueEntity
			if db.scan(&qe, "select * from entities limit 1 offset abs(random()) % max((select count(*) from entities), 1)") {
				debug("Queue manager nudging messages to entity %q", qe.Entity)
				queue_check_entity(qe.Entity)
			}

			var qp QueuePeer
			if db.scan(&qp, "select * from peers limit 1 offset abs(random()) % max((select count(*) from peers), 1)") {
				debug("Queue manager nudging messages to peer %q", qp.Peer)
				queue_check_peer(qp.Peer)
			}

			var qbs []QueueBroadcast
			db.scans(&qbs, "select * from broadcasts")
			for _, qb := range qbs {
				debug("Queue manager sending broadcast event %q", qb.ID)
				p2p_pubsub_messages_1.Publish(p2p_context, qb.Data)
				db.exec("delete from broadcasts where id=?", qb.ID)
			}
		}

		now := now()
		db.exec("delete from entities where created<?", now-maximum_queue_time)
		db.exec("delete from peers where created<?", now-maximum_queue_time)
		db.exec("delete from broadcasts where created<?", now-maximum_queue_time)
	}
}

// Mochi server: Queue
// Copyright Alistair Cunningham 2025

package main

import (
	"io"
	"os"
	"time"
)

type QueueBroadcast struct {
	ID      string
	Topic   string
	Data    []byte
	Created int64
}

type QueueEntity struct {
	ID      string
	Entity  string
	Data    []byte
	File    string
	Created int64
}

type QueuePeer struct {
	ID      string
	Peer    string
	Data    []byte
	File    string
	Created int64
}

const (
	maximum_queue_time = 7 * 86400
)

// Check if there any queued events to an entity, and if so try resending them
func queue_check_entity(entity string) {
	var qs []QueueEntity
	db := db_open("db/queue.db")
	db.scans(&qs, "select * from queue_entities where entity=?", entity)
	for _, q := range qs {
		log_debug("Trying to send queued event '%s' to entity '%s': %s", q.ID, q.Entity)
		peer := entity_peer(q.Entity)
		if peer != "" {
			log_debug("Entity '%s' is at peer '%s'", q.ID, peer)
			if queue_event_send(db, peer, &q.Data, q.File) {
				log_debug("Removing sent event from queue")
				db.exec("delete from queue_entities where id=?", q.ID)
			}
		}
	}
}

// Check if there any queued events to a peer, and if so try resending them
func queue_check_peer(peer string) {
	var qs []QueuePeer
	db := db_open("db/queue.db")
	db.scans(&qs, "select * from queue_peers where peer=?", peer)
	for _, q := range qs {
		log_debug("Trying to send queued event '%s' to peer '%s': %s", q.ID, q.Peer)
		if queue_event_send(db, peer, &q.Data, q.File) {
			log_debug("Removing sent event from queue")
			db.exec("delete from queue_peers where id=?", q.ID)
		}
	}
}

// Send a queue event
func queue_event_send(db *DB, peer string, data *[]byte, file string) bool {
	s := peer_stream(peer)
	if s == nil {
		log_debug("Unable to create stream to peer, keeping in queue")
		return false
	}

	if len(*data) > 0 {
		log_debug("Sending combined data segment")
		_, err := s.Write(*data)
		if err != nil {
			log_debug("Error sending combined data segment: %v", err)
			return false
		}
	}

	if file != "" {
		log_debug("Sending file segment: %s", file)
		f, err := os.Open(file)
		if err != nil {
			log_warn("Unable to read file '%s'", file)
			return false
		}
		defer f.Close()
		n, err := io.Copy(s, f)
		if err != nil {
			log_debug("Error sending file segment: %v", err)
			return false
		}
		log_debug("Finished sending file segment, length %d", n)
	}

	s.Close()
	log_debug("Queued event sent")
	return true
}

// Manage queued events, nudging them or deleting them if they time out
func queue_manager() {
	db := db_open("db/queue.db")

	for {
		time.Sleep(time.Minute)
		if peers_sufficient() {
			var qe QueueEntity
			if db.scan(&qe, "select * from queue_entities limit 1 offset abs(random()) % max((select count(*) from queue_entities), 1)") {
				log_debug("Queue manager nudging events to entity '%s'", qe.Entity)
				queue_check_entity(qe.Entity)
			}

			var qp QueuePeer
			if db.scan(&qp, "select * from queue_peers limit 1 offset abs(random()) % max((select count(*) from queue_peers), 1)") {
				log_debug("Queue manager nudging events to peer '%s'", qp.Peer)
				queue_check_peer(qp.Peer)
			}

			var qbs []QueueBroadcast
			db.scans(&qbs, "select * from queue_broadcasts")
			for _, qb := range qbs {
				log_debug("Queue manager sending broadcast event '%s'", qb.ID)
				p2p_topics[qb.Topic].Publish(p2p_context, qb.Data)
				db.exec("delete from broadcast where id=?", qb.ID)
			}
		}

		now := now()
		db.exec("delete from queue_entities where created<?", now-maximum_queue_time)
		db.exec("delete from queue_peers where created<?", now-maximum_queue_time)
		db.exec("delete from queue_broadcasts where created<?", now-maximum_queue_time)
	}
}

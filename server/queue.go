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

// Check if there any queued messages to an entity, and if so try resending them
func queue_check_entity(entity string) {
	var qs []QueueEntity
	db := db_open("db/queue.db")
	db.scans(&qs, "select * from entities where entity=?", entity)
	for _, q := range qs {
		debug("Trying to send queued event '%s' to entity '%s'", q.ID, q.Entity)
		peer := entity_peer(q.Entity)
		if peer != "" {
			debug("Entity '%s' is at peer '%s'", q.ID, peer)
			if queue_event_send(db, peer, &q.Data, q.File) {
				debug("Removing sent event from queue")
				db.exec("delete from entities where id=?", q.ID)
			}
		}
	}
}

// Check if there any queued messages to a peer, and if so try resending them
func queue_check_peer(peer string) {
	var qs []QueuePeer
	db := db_open("db/queue.db")
	db.scans(&qs, "select * from peers where peer=?", peer)
	for _, q := range qs {
		debug("Trying to send queued event '%s' to peer '%s'", q.ID, q.Peer)
		if queue_event_send(db, peer, &q.Data, q.File) {
			debug("Removing sent event from queue")
			db.exec("delete from peers where id=?", q.ID)
		}
	}
}

// Send a queue event
func queue_event_send(db *DB, peer string, data *[]byte, file string) bool {
	w := peer_writer(peer)
	if w == nil {
		debug("Unable to create stream to peer, keeping in queue")
		return false
	}
	defer w.Close()

	if len(*data) > 0 {
		debug("Sending combined data segment")
		_, err := w.Write(*data)
		if err != nil {
			debug("Error sending combined data segment: %v", err)
			return false
		}
	}

	if file != "" {
		debug("Sending file segment: %s", file)
		f, err := os.Open(file)
		if err != nil {
			warn("Unable to read file '%s'", file)
			return false
		}
		defer f.Close()
		n, err := io.Copy(w, f)
		if err != nil {
			debug("Error sending file segment: %v", err)
			return false
		}
		debug("Finished sending file segment, length %d", n)
	}

	debug("Queued event sent")
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
				debug("Queue manager nudging messages to entity '%s'", qe.Entity)
				queue_check_entity(qe.Entity)
			}

			var qp QueuePeer
			if db.scan(&qp, "select * from peers limit 1 offset abs(random()) % max((select count(*) from peers), 1)") {
				debug("Queue manager nudging messages to peer '%s'", qp.Peer)
				queue_check_peer(qp.Peer)
			}

			var qbs []QueueBroadcast
			db.scans(&qbs, "select * from broadcasts")
			for _, qb := range qbs {
				debug("Queue manager sending broadcast event '%s'", qb.ID)
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

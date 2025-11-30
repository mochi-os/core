// Mochi server: Message queue with reliable delivery
// Copyright Alistair Cunningham 2025

package main

import (
	"math/rand"
	"time"
)

// Queue entry for outgoing messages
type QueueEntry struct {
	Nonce      string `db:"nonce"`
	Type       string `db:"type"`
	Target     string `db:"target"`
	FromEntity string `db:"from_entity"`
	ToEntity   string `db:"to_entity"`
	Service    string `db:"service"`
	Event      string `db:"event"`
	Content    []byte `db:"content"`
	Data       []byte `db:"data"`
	File       string `db:"file"`
	Status     string `db:"status"`
	Attempts   int    `db:"attempts"`
	NextRetry  int64  `db:"next_retry"`
	LastError  string `db:"last_error"`
	Created    int64  `db:"created"`
}

const (
	queue_max_attempts = 10
	queue_max_age      = 7 * 86400 // 7 days
)

// Retry delays: 1m, 2m, 4m, 8m, 15m, 30m, 1h
var retry_delays = []int64{60, 120, 240, 480, 900, 1800, 3600}

// Calculate next retry time with exponential backoff and jitter
func queue_next_retry(attempts int) int64 {
	idx := attempts
	if idx >= len(retry_delays) {
		idx = len(retry_delays) - 1
	}
	delay := retry_delays[idx]
	jitter := rand.Int63n(delay / 4)
	return now() + delay + jitter
}

// Add a direct message to the queue
func queue_add_direct(nonce, target, from_entity, to_entity, service, event string, content, data []byte, file string) {
	db := db_open("db/queue.db")
	db.exec(`insert or replace into queue
		(nonce, type, target, from_entity, to_entity, service, event, content, data, file, status, attempts, next_retry, created)
		values (?, 'direct', ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?)`,
		nonce, target, from_entity, to_entity, service, event, content, data, file, now(), now())
}

// Add a broadcast message to the queue
func queue_add_broadcast(nonce, from_entity, to_entity, service, event string, content, data []byte) {
	db := db_open("db/queue.db")
	db.exec(`insert or replace into queue
		(nonce, type, target, from_entity, to_entity, service, event, content, data, file, status, attempts, next_retry, created)
		values (?, 'broadcast', 'pubsub', ?, ?, ?, ?, ?, ?, '', 'pending', 0, ?, ?)`,
		nonce, from_entity, to_entity, service, event, content, data, now(), now())
}

// Mark a message as acknowledged (remove from queue)
func queue_ack(nonce string) {
	db := db_open("db/queue.db")
	db.exec("delete from queue where nonce = ?", nonce)
	debug("Queue ACK received for %q", nonce)
}

// Mark a message as failed and schedule retry or move to dead letters
func queue_fail(nonce string, err string) {
	db := db_open("db/queue.db")

	var q QueueEntry
	if !db.scan(&q, "select * from queue where nonce = ?", nonce) {
		return
	}

	attempts := q.Attempts + 1

	if attempts >= queue_max_attempts {
		// Move to dead letters
		db.exec(`insert into dead_letters
			(nonce, type, target, from_entity, to_entity, service, event, content, data, attempts, last_error, created, died)
			values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			q.Nonce, q.Type, q.Target, q.FromEntity, q.ToEntity, q.Service, q.Event, q.Content, q.Data, attempts, err, q.Created, now())
		db.exec("delete from queue where nonce = ?", nonce)
		warn("Queue message %q moved to dead letters after %d attempts: %s", nonce, attempts, err)
	} else {
		// Schedule retry
		next := queue_next_retry(attempts)
		db.exec("update queue set status = 'pending', attempts = ?, next_retry = ?, last_error = ? where nonce = ?",
			attempts, next, err, nonce)
		debug("Queue message %q scheduled for retry %d at %d: %s", nonce, attempts, next, err)
	}
}

// Send a queued direct message
func queue_send_direct(q *QueueEntry) bool {
	peer := q.Target
	if peer == "" {
		peer = entity_peer(q.ToEntity)
	}
	if peer == "" {
		return false
	}

	s := peer_stream(peer)
	if s == nil {
		return false
	}

	timestamp := now()
	signature := entity_sign(q.FromEntity, string(signable_headers("msg", q.FromEntity, q.ToEntity, q.Service, q.Event, timestamp, q.Nonce)))

	headers := cbor_encode(Headers{
		Type: "msg", From: q.FromEntity, To: q.ToEntity, Service: q.Service, Event: q.Event,
		Timestamp: timestamp, Nonce: q.Nonce, Signature: signature,
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

	if s.writer != nil {
		s.writer.Close()
	}

	return true
}

// Send a queued broadcast message
func queue_send_broadcast(q *QueueEntry) bool {
	if !peers_sufficient() {
		return false
	}

	timestamp := now()
	signature := entity_sign(q.FromEntity, string(signable_headers("msg", q.FromEntity, q.ToEntity, q.Service, q.Event, timestamp, q.Nonce)))

	msg := Message{
		ID: q.Nonce, From: q.FromEntity, To: q.ToEntity, Service: q.Service, Event: q.Event,
		Timestamp: timestamp, Nonce: q.Nonce, Signature: signature,
	}
	data := cbor_encode(msg)
	if len(q.Content) > 0 {
		data = append(data, q.Content...)
	}

	p2p_pubsub_1.Publish(p2p_context, data)
	return true
}

// Process pending queue entries
func queue_process() {
	db := db_open("db/queue.db")

	var entries []QueueEntry
	_ = db.scans(&entries, "select * from queue where status = 'pending' and next_retry <= ? limit 50", now())

	for _, q := range entries {
		var ok bool
		if q.Type == "broadcast" {
			ok = queue_send_broadcast(&q)
		} else {
			ok = queue_send_direct(&q)
		}

		if ok {
			// Mark as sent, waiting for ACK (for direct) or remove (for broadcast)
			if q.Type == "broadcast" {
				// Broadcasts don't get ACKs, just remove
				db.exec("delete from queue where nonce = ?", q.Nonce)
				debug("Queue broadcast %q sent", q.Nonce)
			} else {
				db.exec("update queue set status = 'sent' where nonce = ?", q.Nonce)
				debug("Queue direct %q sent, awaiting ACK", q.Nonce)
			}
		} else {
			queue_fail(q.Nonce, "send failed")
		}

		time.Sleep(time.Millisecond)
	}
}

// Check for sent messages that haven't received ACK (timeout)
func queue_check_ack_timeout() {
	db := db_open("db/queue.db")
	// Messages sent more than 30 seconds ago without ACK
	timeout := now() - 30
	db.exec("update queue set status = 'pending', next_retry = ? where status = 'sent' and created < ?",
		queue_next_retry(0), timeout)
}

// Check queue for messages to a specific entity (called when entity location discovered)
func queue_check_entity(entity string) {
	db := db_open("db/queue.db")

	var entries []QueueEntry
	_ = db.scans(&entries, "select * from queue where type = 'direct' and to_entity = ? and status = 'pending' limit 10", entity)

	for _, q := range entries {
		if queue_send_direct(&q) {
			db.exec("update queue set status = 'sent' where nonce = ?", q.Nonce)
			debug("Queue direct %q sent to entity %q, awaiting ACK", q.Nonce, entity)
		}
	}
}

// Check queue for messages to a specific peer (called when peer discovered)
func queue_check_peer(peer string) {
	db := db_open("db/queue.db")

	var entries []QueueEntry
	_ = db.scans(&entries, "select * from queue where type = 'direct' and target = ? and status = 'pending' limit 10", peer)

	for _, q := range entries {
		if queue_send_direct(&q) {
			db.exec("update queue set status = 'sent' where nonce = ?", q.Nonce)
			debug("Queue direct %q sent to peer %q, awaiting ACK", q.Nonce, peer)
		}
	}
}

// Clean up old entries
func queue_cleanup() {
	db := db_open("db/queue.db")
	cutoff := now() - queue_max_age

	// Move very old pending messages to dead letters
	var old []QueueEntry
	_ = db.scans(&old, "select * from queue where created < ?", cutoff)
	for _, q := range old {
		db.exec(`insert into dead_letters
			(nonce, type, target, from_entity, to_entity, service, event, content, data, attempts, last_error, created, died)
			values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			q.Nonce, q.Type, q.Target, q.FromEntity, q.ToEntity, q.Service, q.Event, q.Content, q.Data, q.Attempts, "expired", q.Created, now())
	}
	db.exec("delete from queue where created < ?", cutoff)

	// Clean old dead letters (keep for 30 days)
	db.exec("delete from dead_letters where died < ?", now()-30*86400)

	// Clean old seen nonces
	nonce_cleanup_persistent()
}

// Queue manager goroutine
func queue_manager() {
	// Process queue every 10 seconds
	go func() {
		for range time.Tick(10 * time.Second) {
			queue_process()
			queue_check_ack_timeout()
		}
	}()

	// Cleanup runs less frequently
	for range time.Tick(time.Hour) {
		queue_cleanup()
	}
}

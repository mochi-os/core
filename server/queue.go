// Mochi server: Message queue with reliable delivery
// Copyright Alistair Cunningham 2025

package main

import (
	"math/rand"
	"time"
)

// Queue entry for outgoing messages
type QueueEntry struct {
	ID         string `db:"id"`
	Type       string `db:"type"`
	Target     string `db:"target"`
	FromEntity string `db:"from_entity"`
	ToEntity   string `db:"to_entity"`
	Service    string `db:"service"`
	Event      string `db:"event"`
	App        string `db:"app"`
	Content    []byte `db:"content"`
	Data       []byte `db:"data"`
	File       string `db:"file"`
	Expires    int64  `db:"expires"`
	Status     string `db:"status"`
	Attempts   int    `db:"attempts"`
	NextRetry  int64  `db:"next_retry"`
	LastError  string `db:"last_error"`
	Created    int64  `db:"created"`
}

const (
	queue_max_age = 7 * 86400 // 7 days
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
func queue_add_direct(id, target, from_entity, to_entity, service, event, app string, content, data []byte, file string, expires int64) {
	db := db_open("db/queue.db")
	db.exec(`insert or replace into queue
		(id, type, target, from_entity, to_entity, service, event, app, content, data, file, expires, status, attempts, next_retry, created)
		values (?, 'direct', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?)`,
		id, target, from_entity, to_entity, service, event, app, content, data, file, expires, now(), now())
}

// Add a broadcast message to the queue
func queue_add_broadcast(id, from_entity, to_entity, service, event, app string, content, data []byte, expires int64) {
	db := db_open("db/queue.db")
	db.exec(`insert or replace into queue
		(id, type, target, from_entity, to_entity, service, event, app, content, data, file, expires, status, attempts, next_retry, created)
		values (?, 'broadcast', 'pubsub', ?, ?, ?, ?, ?, ?, ?, '', ?, 'pending', 0, ?, ?)`,
		id, from_entity, to_entity, service, event, app, content, data, expires, now(), now())
}

// Mark a message as acknowledged (remove from queue)
func queue_ack(id string) {
	db := db_open("db/queue.db")
	db.exec("delete from queue where id = ?", id)
	//debug("Queue ACK received for %q", id)
}

// Mark a message as being sent (prevents other processors from picking it up)
func queue_sending(id string) {
	db := db_open("db/queue.db")
	db.exec("update queue set status='sending' where id=?", id)
}

// Mark a message as failed and schedule retry or drop
func queue_fail(id string, err string) {
	db := db_open("db/queue.db")

	var q QueueEntry
	if !db.scan(&q, "select * from queue where id = ?", id) {
		return
	}

	attempts := q.Attempts + 1
	age := time.Now().Unix() - q.Created

	if age > queue_max_age {
		// Log and drop after max age
		warn("Queue dropping message after %d attempts: id=%q type=%q from=%q to=%q service=%q event=%q error=%q",
			attempts, q.ID, q.Type, q.FromEntity, q.ToEntity, q.Service, q.Event, err)
		db.exec("delete from queue where id = ?", id)
	} else {
		// Schedule retry
		next := queue_next_retry(attempts)
		db.exec("update queue set status = 'pending', attempts = ?, next_retry = ?, last_error = ? where id = ?",
			attempts, next, err, id)
		debug("Queue message %q scheduled for retry %d at %d: %s", id, attempts, next, err)
	}
}

// Send a queued direct message (reads challenge before sending, waits for ACK)
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
	defer s.close()

	// Read challenge from receiver
	challenge, err := s.read_challenge()
	if err != nil {
		return false
	}

	signature := entity_sign(q.FromEntity, string(signable_headers("msg", q.FromEntity, q.ToEntity, q.Service, q.Event, q.App, q.ID, "", challenge)))

	headers := cbor_encode(Headers{
		Type: "msg", From: q.FromEntity, To: q.ToEntity, Service: q.Service, Event: q.Event,
		App: q.App, ID: q.ID, Signature: signature,
	})

	// Batch headers + content + data into single write
	data := headers
	if len(q.Content) > 0 {
		data = append(data, q.Content...)
	}
	if len(q.Data) > 0 {
		data = append(data, q.Data...)
	}

	if s.write_raw(data) != nil {
		return false
	}
	if q.File != "" {
		_, err := s.write_file(q.File)
		if err != nil {
			return false
		}
	}

	// Close write direction to signal we're done sending (keeps read open for ACK)
	s.close_write()

	// Read ACK from stream
	var h Headers
	if s.read_headers(&h) != nil {
		debug("Queue direct %q failed to read ACK", q.ID)
		return false
	}

	if h.msg_type() == "ack" && h.AckID == q.ID {
		debug("Queue direct %q received ACK", q.ID)
		return true
	}

	if h.msg_type() == "nack" && h.AckID == q.ID {
		debug("Queue direct %q received NACK", q.ID)
		return false
	}

	debug("Queue direct %q received unexpected response type=%q ack=%q", q.ID, h.msg_type(), h.AckID)
	return false
}

// Send a queued broadcast message (no challenge for broadcasts)
func queue_send_broadcast(q *QueueEntry) bool {
	if !peers_sufficient() {
		return false
	}

	signature := entity_sign(q.FromEntity, string(signable_headers("msg", q.FromEntity, q.ToEntity, q.Service, q.Event, q.App, q.ID, "", nil)))

	msg := Message{
		ID: q.ID, From: q.FromEntity, To: q.ToEntity, Service: q.Service, Event: q.Event,
		App: q.App, Signature: signature,
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
	err := db.scans(&entries, "select * from queue where status = 'pending' and next_retry <= ? limit 50", now())
	if err != nil {
		info("Queue process scan error: %v", err)
	}
	if len(entries) > 0 {
		info("Queue processing %d entries", len(entries))
	}

	for _, q := range entries {
		// Skip expired messages
		if q.Expires > 0 && q.Expires < now() {
			debug("Queue message %q expired", q.ID)
			db.exec("delete from queue where id = ?", q.ID)
			continue
		}

		var ok bool
		if q.Type == "broadcast" {
			ok = queue_send_broadcast(&q)
		} else {
			ok = queue_send_direct(&q)
		}

		if ok {
			// Message sent and ACK received (or broadcast sent), remove from queue
			db.exec("delete from queue where id = ?", q.ID)
			debug("Queue %s %q completed", q.Type, q.ID)
		} else {
			queue_fail(q.ID, "send failed")
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
	// Messages stuck in 'sending' for more than 60 seconds (safety net)
	stuck := now() - 60
	db.exec("update queue set status = 'pending', next_retry = ? where status = 'sending' and created < ?",
		queue_next_retry(0), stuck)
}

// Check queue for messages to a specific entity (called when entity location discovered)
func queue_check_entity(entity string) {
	db := db_open("db/queue.db")

	var entries []QueueEntry
	err := db.scans(&entries, "select * from queue where type = 'direct' and to_entity = ? and status = 'pending' limit 10", entity)
	if err != nil {
		warn("Database error checking queue for entity %q: %v", entity, err)
		return
	}

	for _, q := range entries {
		if queue_send_direct(&q) {
			db.exec("delete from queue where id = ?", q.ID)
			debug("Queue direct %q sent to entity %q", q.ID, entity)
		}
	}
}

// Check queue for messages to a specific peer (called when peer discovered)
func queue_check_peer(peer string) {
	db := db_open("db/queue.db")

	var entries []QueueEntry
	err := db.scans(&entries, "select * from queue where type = 'direct' and target = ? and status = 'pending' limit 10", peer)
	if err != nil {
		warn("Database error checking queue for peer %q: %v", peer, err)
		return
	}

	for _, q := range entries {
		if queue_send_direct(&q) {
			db.exec("delete from queue where id = ?", q.ID)
			debug("Queue direct %q sent to peer %q", q.ID, peer)
		}
	}
}

// Clean up old entries
func queue_cleanup() {
	db := db_open("db/queue.db")
	cutoff := now() - queue_max_age

	// Log and delete expired messages
	var old []QueueEntry
	err := db.scans(&old, "select * from queue where created < ?", cutoff)
	if err != nil {
		warn("Database error loading expired queue entries: %v", err)
		return
	}
	for _, q := range old {
		warn("Queue dropping expired message: id=%q type=%q from=%q to=%q service=%q event=%q attempts=%d",
			q.ID, q.Type, q.FromEntity, q.ToEntity, q.Service, q.Event, q.Attempts)
	}
	db.exec("delete from queue where created < ?", cutoff)
}

// Drain queue before shutdown (wait for pending sends to complete)
func queue_drain(timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	db := db_open("db/queue.db")

	for time.Now().Before(deadline) {
		count := db.integer("select count(*) from queue where status = 'sent'")
		if count == 0 {
			info("Queue drained")
			return
		}
		info("Waiting for %d pending messages...", count)
		time.Sleep(time.Second)
	}

	remaining := db.integer("select count(*) from queue")
	if remaining > 0 {
		info("Queue drain timeout, %d messages still pending", remaining)
	}
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
		message_seen_cleanup()
	}
}

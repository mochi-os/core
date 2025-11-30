// Mochi server: Protocol
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"crypto/ed25519"
	"sync"
	"time"
)

var (
	nonces      = map[string]int64{}
	nonces_lock sync.Mutex
)

const (
	nonce_retention = 24 * 3600 // keep seen nonces for 24 hours
)

// Signable portion of headers (excludes signature and ack fields)
type SignableHeaders struct {
	Type      string `cbor:"type,omitempty"`
	From      string `cbor:"from,omitempty"`
	To        string `cbor:"to,omitempty"`
	Service   string `cbor:"service,omitempty"`
	Event     string `cbor:"event,omitempty"`
	Timestamp int64  `cbor:"timestamp,omitempty"`
	Nonce     string `cbor:"nonce,omitempty"`
}

// Create signable headers for signing
func signable_headers(msg_type, from, to, service, event string, timestamp int64, nonce string) []byte {
	return cbor_encode(SignableHeaders{
		Type: msg_type, From: from, To: to, Service: service, Event: event,
		Timestamp: timestamp, Nonce: nonce,
	})
}

// Message headers
type Headers struct {
	Type      string `cbor:"type,omitempty"`
	From      string `cbor:"from,omitempty"`
	To        string `cbor:"to,omitempty"`
	Service   string `cbor:"service,omitempty"`
	Event     string `cbor:"event,omitempty"`
	Timestamp int64  `cbor:"timestamp,omitempty"`
	Nonce     string `cbor:"nonce,omitempty"`
	AckNonce  string `cbor:"ack,omitempty"`
	Signature string `cbor:"signature,omitempty"`
}

// Get message type, defaulting to "msg"
func (h *Headers) msg_type() string {
	if h.Type == "" {
		return "msg"
	}
	return h.Type
}

// Check if headers are valid and signature verifies
func (h *Headers) valid() bool {
	// Validate type
	t := h.msg_type()
	if t != "msg" && t != "ack" && t != "nack" {
		info("Invalid message type %q", t)
		return false
	}

	// ACK/NACK must have ack nonce
	if (t == "ack" || t == "nack") && h.AckNonce == "" {
		info("ACK/NACK missing ack nonce")
		return false
	}

	if h.From != "" && !valid(h.From, "entity") {
		info("Invalid from header %q", h.From)
		return false
	}

	if h.To != "" && !valid(h.To, "entity") {
		info("Invalid to header %q", h.To)
		return false
	}

	if h.Service != "" && !valid(h.Service, "constant") {
		info("Invalid service header %q", h.Service)
		return false
	}

	// Event is optional for ACK messages
	if t == "msg" && !valid(h.Event, "constant") {
		info("Invalid event header %q", h.Event)
		return false
	}

	if h.From != "" {
		public := base58_decode(h.From, "")
		if len(public) != ed25519.PublicKeySize {
			info("Invalid from header length %d!=%d", len(public), ed25519.PublicKeySize)
			return false
		}
		// Require timestamp and nonce on signed headers
		if h.Timestamp == 0 || h.Nonce == "" {
			info("Missing timestamp or nonce")
			return false
		}
		// Reject stale timestamps (> 5 minutes)
		age := now() - h.Timestamp
		if age < 0 {
			age = -age
		}
		if age > 300 {
			info("Timestamp too old or too far in future")
			return false
		}
		// Reject replayed nonces
		if nonce_seen(h.Nonce) {
			info("Replayed nonce")
			return false
		}
		if !ed25519.Verify(public, signable_headers(h.msg_type(), h.From, h.To, h.Service, h.Event, h.Timestamp, h.Nonce), base58_decode(h.Signature, "")) {
			info("Incorrect signature header")
			return false
		}
	}

	return true
}

// Check if nonce was already seen (in-memory, for replay attack prevention)
func nonce_seen(nonce string) bool {
	nonces_lock.Lock()
	defer nonces_lock.Unlock()

	if _, exists := nonces[nonce]; exists {
		return true
	}
	nonces[nonce] = now() + 600 // expire 10 minutes from now
	return false
}

// Check if nonce was already processed (persistent, for deduplication)
func nonce_processed(nonce string) bool {
	db := db_open("db/queue.db")
	exists, _ := db.exists("select 1 from seen_nonces where nonce = ?", nonce)
	return exists
}

// Record a nonce as processed (persistent)
func nonce_record(nonce string) {
	db := db_open("db/queue.db")
	db.exec("insert or ignore into seen_nonces (nonce, created) values (?, ?)", nonce, now())
}

// Clean up expired nonces (in-memory)
func nonce_cleanup() {
	nonces_lock.Lock()
	defer nonces_lock.Unlock()

	t := now()
	for k, expires := range nonces {
		if expires < t {
			delete(nonces, k)
		}
	}
}

// Clean up old seen nonces (persistent) - called from queue_manager
func nonce_cleanup_persistent() {
	db := db_open("db/queue.db")
	db.exec("delete from seen_nonces where created < ?", now()-nonce_retention)
}

func init() {
	go func() {
		for range time.Tick(time.Minute) {
			nonce_cleanup()
		}
	}()
}

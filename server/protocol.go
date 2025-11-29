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

type SignableHeaders struct {
	From      string `cbor:"from,omitempty"`
	To        string `cbor:"to,omitempty"`
	Service   string `cbor:"service,omitempty"`
	Event     string `cbor:"event,omitempty"`
	Timestamp int64  `cbor:"timestamp,omitempty"`
	Nonce     string `cbor:"nonce,omitempty"`
}

func signable_headers(from, to, service, event string, timestamp int64, nonce string) []byte {
	return cbor_encode(SignableHeaders{
		From: from, To: to, Service: service, Event: event,
		Timestamp: timestamp, Nonce: nonce,
	})
}

type Headers struct {
	From      string `cbor:"from,omitempty"`
	To        string `cbor:"to,omitempty"`
	Service   string `cbor:"service,omitempty"`
	Event     string `cbor:"event,omitempty"`
	Timestamp int64  `cbor:"timestamp,omitempty"`
	Nonce     string `cbor:"nonce,omitempty"`
	Signature string `cbor:"signature,omitempty"`
}

// Check if headers are valid
func (h *Headers) valid() bool {
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

	if !valid(h.Event, "constant") {
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
		if !ed25519.Verify(public, signable_headers(h.From, h.To, h.Service, h.Event, h.Timestamp, h.Nonce), base58_decode(h.Signature, "")) {
			info("Incorrect signature header")
			return false
		}
	}

	return true
}

// Check if nonce was already seen; if not, record it
func nonce_seen(nonce string) bool {
	nonces_lock.Lock()
	defer nonces_lock.Unlock()

	if _, exists := nonces[nonce]; exists {
		return true
	}
	nonces[nonce] = now() + 600 // expire 10 minutes from now
	return false
}

// Clean up expired nonces periodically
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

func init() {
	go func() {
		for range time.Tick(time.Minute) {
			nonce_cleanup()
		}
	}()
}

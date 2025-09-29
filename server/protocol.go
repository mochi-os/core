// Mochi server: Protocol
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"crypto/ed25519"
)

type Headers struct {
	From    string `cbor:"from,omitempty"`
	To      string `cbor:"to,omitempty"`
	Service string `cbor:"service,omitempty"`
	//TODO Rename event header?
	Event     string `cbor:"event,omitempty"`
	Signature string `cbor:"signature,omitempty"`
}

// Check if headers are valid
func (h *Headers) valid() bool {
	if h.From != "" && !valid(h.From, "entity") {
		info("Invalid from header '%s'", h.From)
		return false
	}

	if h.To != "" && !valid(h.To, "entity") {
		info("Invalid to header '%s'", h.To)
		return false
	}

	if h.Service != "" && !valid(h.Service, "constant") {
		info("Invalid service header '%s'", h.Service)
		return false
	}

	if !valid(h.Event, "constant") {
		info("Invalid event header '%s'", h.Event)
		return false
	}

	if h.From != "" {
		public := base58_decode(h.From, "")
		if len(public) != ed25519.PublicKeySize {
			info("Invalid from header length %d!=%d", len(public), ed25519.PublicKeySize)
			return false
		}
		if !ed25519.Verify(public, []byte(h.From+h.To+h.Service+h.Event), base58_decode(h.Signature, "")) {
			info("Incorrect signature header")
			return false
		}
	}

	return true
}

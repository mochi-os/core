// Mochi server: Protocol
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"crypto/ed25519"
)

type SignableHeaders struct {
	From    string `cbor:"from,omitempty"`
	To      string `cbor:"to,omitempty"`
	Service string `cbor:"service,omitempty"`
	Event   string `cbor:"event,omitempty"`
}

func signable_headers(from, to, service, event string) []byte {
	return cbor_encode(SignableHeaders{From: from, To: to, Service: service, Event: event})
}

type Headers struct {
	From      string `cbor:"from,omitempty"`
	To        string `cbor:"to,omitempty"`
	Service   string `cbor:"service,omitempty"`
	Event     string `cbor:"event,omitempty"`
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
		if !ed25519.Verify(public, signable_headers(h.From, h.To, h.Service, h.Event), base58_decode(h.Signature, "")) {
			info("Incorrect signature header")
			return false
		}
	}

	return true
}

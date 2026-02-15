// Mochi server: Protocol
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"crypto/ed25519"
)

const max_id_length = 64

// Signable portion of headers
type SignableHeaders struct {
	Type      string `cbor:"type,omitempty"`
	From      string `cbor:"from,omitempty"`
	To        string `cbor:"to,omitempty"`
	Service   string `cbor:"service,omitempty"`
	Event     string `cbor:"event,omitempty"`
	ID        string `cbor:"id,omitempty"`
	AckID     string `cbor:"ack,omitempty"`
	Challenge []byte `cbor:"challenge,omitempty"`
}

// Create signable headers
func signable_headers(msg_type, from, to, service, event, id, ack_id string, challenge []byte) []byte {
	return cbor_encode(SignableHeaders{
		Type: msg_type, From: from, To: to, Service: service, Event: event,
		ID: id, AckID: ack_id, Challenge: challenge,
	})
}

// Message headers
type Headers struct {
	Type      string `cbor:"type,omitempty"`
	From      string `cbor:"from,omitempty"`
	To        string `cbor:"to,omitempty"`
	Service   string `cbor:"service,omitempty"`
	Event     string `cbor:"event,omitempty"`
	ID        string `cbor:"id,omitempty"`
	AckID     string `cbor:"ack,omitempty"`
	Signature string `cbor:"signature,omitempty"`
}

// Get message type, defaulting to "msg"
func (h *Headers) msg_type() string {
	if h.Type == "" {
		return "msg"
	}
	return h.Type
}

// Check if headers are valid (basic validation - signature verified separately with challenge)
func (h *Headers) valid() bool {
	t := h.msg_type()
	if t != "msg" && t != "ack" && t != "nack" {
		info("Invalid message type %q", t)
		audit_message_rejected(h.From, "invalid_type")
		return false
	}

	if (t == "ack" || t == "nack") && h.AckID == "" {
		info("ACK/NACK missing ack ID")
		audit_message_rejected(h.From, "missing_ack_id")
		return false
	}

	if h.ID != "" && len(h.ID) > max_id_length {
		info("Message ID too long: %d > %d", len(h.ID), max_id_length)
		audit_message_rejected(h.From, "id_too_long")
		return false
	}

	if h.AckID != "" && len(h.AckID) > max_id_length {
		info("Ack ID too long: %d > %d", len(h.AckID), max_id_length)
		audit_message_rejected(h.From, "ack_id_too_long")
		return false
	}

	if h.From != "" && !valid(h.From, "entity") {
		info("Invalid from header %q", h.From)
		audit_message_rejected(h.From, "invalid_from")
		return false
	}

	if h.To != "" && !valid(h.To, "entity") && !valid(h.To, "fingerprint") {
		info("Invalid to header %q", h.To)
		audit_message_rejected(h.From, "invalid_to")
		return false
	}

	if h.Service != "" && !valid(h.Service, "constant") {
		info("Invalid service header %q", h.Service)
		audit_message_rejected(h.From, "invalid_service")
		return false
	}

	if t == "msg" && h.Service != "" && !valid(h.Event, "constant") {
		info("Invalid event header %q", h.Event)
		audit_message_rejected(h.From, "invalid_event")
		return false
	}

	return true
}

// Verify signature with challenge
func (h *Headers) verify(challenge []byte) bool {
	if h.From == "" {
		return true // Unsigned message (e.g., broadcast)
	}

	public := base58_decode(h.From, "")
	if len(public) != ed25519.PublicKeySize {
		info("Invalid from header length %d!=%d", len(public), ed25519.PublicKeySize)
		audit_signature_failed(h.From, "invalid_key_length")
		return false
	}

	signable := signable_headers(h.msg_type(), h.From, h.To, h.Service, h.Event, h.ID, h.AckID, challenge)
	if !ed25519.Verify(public, signable, base58_decode(h.Signature, "")) {
		info("Incorrect signature")
		audit_signature_failed(h.From, "invalid_signature")
		return false
	}

	return true
}

// Mochi server: GossipSub pubsub.
//
// Server-level peer-discovery and directory announcements ride one
// GossipSub topic. This file owns the subscribe loop, the receive-side
// decode/route, and the single publish path shared by every producer
// (directory / peer announcements via Message.publish, and the queue's
// broadcast re-flood via queue_send_broadcast). See claude/plans/pubsub.md.
//
// Pubsub is best-effort and one-way: no per-message challenge, no
// ack/nack, no reply writer. GossipSub's StrictSign authenticates the
// relaying peer at the mesh layer; the entity signature (verified
// against a nil challenge) authenticates signed announcements.
//
// Copyright Alistair Cunningham 2024-2026

package main

import (
	"bytes"
	"io"
)

// pubsub_manager subscribes to the pubsub topic and dispatches each
// inbound message. One goroutine for the process, started from
// net_start once the topic is joined.
func pubsub_manager() {
	s := must(net_pubsub_1.Subscribe())

	for {
		m, err := s.Next(net_context)
		if err != nil {
			warn("Pubsub error: %v", err)
			continue
		}
		peer := m.ReceivedFrom.String()
		if peer == net_id {
			continue
		}
		// Rate limit inbound per peer. Bootstrap and paired peers are
		// trusted and skip the limit.
		if !peer_is_bootstrap(peer) && !peer_is_pair(peer) && !rate_limit_pubsub_in.allow(peer) {
			debug("Pubsub rate limited peer %q", peer)
			continue
		}
		pubsub_receive(m.Data, peer)
		peer_discovered(peer)
		peer_connect(peer)
	}
}

// pubsub_receive decodes one pubsub message — a Headers envelope
// followed by a content segment — and routes it to the matching event
// handler. Best-effort and one-way, so unlike stream_receive there is
// no challenge, no ack/nack, and no reply stream.
func pubsub_receive(data []byte, peer string) {
	s := stream_rw(io.NopCloser(bytes.NewReader(data)), nil)

	var h Headers
	if err := s.read_headers(&h); err != nil {
		info("Pubsub error reading headers from peer %q: %v", peer, err)
		return
	}
	if !h.valid() {
		info("Pubsub received invalid headers from peer %q", peer)
		return
	}
	// Pubsub only ever carries broadcast messages; ack/nack belong to
	// bidirectional streams. Drop anything else.
	if h.msg_type() != "msg" {
		return
	}

	// Verify the entity signature against a nil challenge (broadcasts
	// sign without one). On failure, clear From so the event is treated
	// as anonymous — the handler's Anonymous gate decides whether to
	// accept it.
	if !h.verify(nil) {
		h.From = ""
	}

	// Deduplicate. Also coalesces the same message arriving via multiple
	// mesh paths.
	if h.ID != "" && message_seen(h.ID) {
		return
	}

	content, err := s.read_content()
	if err != nil {
		info("Pubsub error reading content from peer %q: %v", peer, err)
		return
	}

	e := Event{id: event_id(), msg_id: h.ID, from: h.From, to: h.To, service: h.Service, event: h.Event, sender_app: h.FromApp, sender_services: h.Services, peer: peer, content: content}
	if err := e.route(); err != nil {
		debug("Pubsub route error for service %q event %q from peer %q: %v", h.Service, h.Event, peer, err)
	}

	if h.ID != "" {
		message_mark_seen(h.ID)
	}
}

// pubsub_publish signs, encodes, and floods one message to the pubsub
// topic. The single encode path for every producer, so the wire
// envelope — including the message ID that drives receiver dedup — is
// identical regardless of origin. (Before this path existed,
// queue_send_broadcast encoded a Message whose ID is never serialised,
// so re-flooded announcements carried no wire ID and bypassed the
// receiver's message_seen dedup.)
func pubsub_publish(from, to, service, event, from_app string, services []string, id string, content, data []byte) {
	signature := entity_sign(from, string(signable_headers("msg", from, to, service, event, from_app, id, "", "", services, nil)))
	out := cbor_encode(Headers{
		Type: "msg", From: from, To: to, Service: service, Event: event,
		FromApp: from_app, Services: services, ID: id, Signature: signature,
	})
	out = append(out, content...)
	if len(data) > 0 {
		out = append(out, data...)
	}
	net_pubsub_1.Publish(net_context, out)
}

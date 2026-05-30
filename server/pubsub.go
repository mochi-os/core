// Mochi server: GossipSub pubsub.
//
// Server-level peer-discovery and directory announcements ride GossipSub
// on the /mochi/2 topic. This file owns the subscribe loop, the
// receive-side decode/route, and the single publish path shared by every
// producer (directory / peer announcements via Message.publish, and the
// queue's broadcast re-flood via queue_send_broadcast). See
// claude/plans/pubsub.md.
//
// Each message is a self-contained protocol-2 Frame carrying an Expires
// freshness bound and — for signed announcements (directory) — a
// domain-separated entity signature over the canonical
// {v, from, service, event, expires, content}. pubsub_publish floods one
// Frame; receivers dedup a re-flood or multi-path delivery via
// message_seen_mark.
//
// Pubsub is best-effort and one-way: no per-message challenge, no
// ack/nack, no reply writer. GossipSub's StrictSign authenticates the
// relaying peer at the mesh layer; the entity signature authenticates
// signed announcements (directory). Anonymous announcements (peer
// discovery) are unsigned and trusted via the GossipSub source.
//
// Copyright Alistair Cunningham 2024-2026

package main

import (
	"bytes"
	"crypto/ed25519"
	"errors"
	"fmt"
	"sync/atomic"
)

// Operator counters surfaced by `mochictl pubsub status`
// (admin_pubsub_status): outbound publish volume, inbound message volume,
// and the last time a message was received. The live mesh peer count is
// read from ListPeers at report time, not counted here.
var (
	pubsub_published atomic.Int64
	pubsub_received  atomic.Int64
	pubsub_last      atomic.Int64
)

// Domain separator and freshness window for /mochi/2 pubsub entity
// signatures.
const (
	// pubsub_domain is the by-construction domain separator baked into the
	// signed payload, mirroring claim_domain. Any change to the signed
	// schema MUST bump it.
	pubsub_domain = "mochi/2/pubsub"

	// pubsub_expires_ttl is how long after flooding an announcement stays
	// valid. Lower-bounded by the max queue-broadcast retry interval
	// (retry_delays' 3600s) so a queue-held re-flood is never already
	// expired, and kept above the hourly peers_publish cadence so a peer
	// announcement stays valid until the next one. Asserted by
	// TestPubsubExpiresTTLExceedsMaxRetry. Upper bound is the replay-window
	// vs re-announce-cadence tradeoff; bulk directory download
	// (directory_download) is the catch-up backstop for entries whose
	// flood has expired.
	pubsub_expires_ttl = 2 * 3600 // 2 hours

	// pubsub_expires_max bounds how far in the future an Expires may sit
	// before a receiver treats it as absurd — without it, a captured
	// message carrying a far-future Expires would replay long past the
	// intended window. 2x the TTL leaves generous clock-skew slack.
	pubsub_expires_max = 2 * pubsub_expires_ttl
)

// pubsub_manager subscribes to the /mochi/2 topic and dispatches each
// inbound message. One goroutine for the process, started from net_start
// once the topic is joined.
func pubsub_manager() {
	s := must(net_pubsub.Subscribe())

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
		pubsub_received.Add(1)
		pubsub_last.Store(now())
		pubsub_receive(m.Data, peer)
		peer_discovered(peer)
		peer_connect(peer)
	}
}

// pubsub_receive decodes one /mochi/2 pubsub Frame and routes it. The
// frame is self-contained: routing envelope, an Expires freshness bound,
// and (for signed announcements) the entity signature all travel in the
// one message — there is no stream or handshake context. Best-effort and
// one-way, so there is no challenge, no ack/nack, and no reply stream.
func pubsub_receive(data []byte, peer string) {
	f, err := frame_read(bytes.NewReader(data))
	if err != nil {
		info("Pubsub frame read error from peer %q: %v", peer, err)
		return
	}
	if f.Type != frame_type_message {
		return // pubsub carries only message frames
	}
	if !pubsub_frame_valid(f) {
		info("Pubsub received invalid frame from peer %q", peer)
		return
	}

	// Freshness bounds replay within the signed window.
	if !pubsub_fresh(f.Expires) {
		debug("Pubsub dropping frame with out-of-window expires %q from peer %q", f.Expires, peer)
		return
	}

	// Entity signature (signed announcements only). On failure, clear From
	// so the event is treated as anonymous — the handler's Anonymous gate
	// (e.g. directory_publish_event's bootstrap-peer trust, which reads the
	// entity id from content) decides whether to keep it.
	if f.From != "" {
		strcontent, ok := pubsub_string_content(f.Content)
		if !ok || pubsub_verify(f.From, f.Service, f.Event, f.Expires, strcontent, f.Signature) != nil {
			f.From = ""
		}
	}

	// Deduplicate atomically, coalescing a re-flooded or multi-path
	// delivery without racing the direct-stream workers that share the
	// dedup map.
	if f.ID != "" && message_seen_mark(f.ID) {
		return
	}

	e := Event{id: event_id(), msg_id: f.ID, from: f.From, to: f.To, service: f.Service, event: f.Event, peer: peer, content: f.Content}
	if err := e.route(); err != nil {
		debug("Pubsub frame route error for service %q event %q from peer %q: %v", f.Service, f.Event, peer, err)
	}
}

// pubsub_frame_valid runs the envelope-level checks on a received frame:
// well-formed from / to / service / event / id. Content is validated by
// the event handler (valid(id,"entity") etc.).
func pubsub_frame_valid(f *Frame) bool {
	if f.From != "" && !valid(f.From, "entity") {
		return false
	}
	if f.To != "" && !valid(f.To, "entity") && !valid(f.To, "fingerprint") {
		return false
	}
	if f.Service != "" && !valid(f.Service, "constant") {
		return false
	}
	if f.Event != "" && !valid(f.Event, "constant") {
		return false
	}
	if f.ID != "" && len(f.ID) > max_id_length {
		return false
	}
	return true
}

// pubsub_fresh reports whether an Expires timestamp (absolute Unix
// seconds, decimal string) is within the acceptance window: present, not
// yet expired, and not absurdly far in the future.
func pubsub_fresh(expires string) bool {
	exp := atoi(expires, 0)
	return exp > 0 && now() < exp && exp <= now()+pubsub_expires_max
}

// pubsub_publish floods one message to the /mochi/2 topic as a
// self-contained Frame. Producers (directory / peer announcements via
// Message.publish, the queue's broadcast re-flood via
// queue_send_broadcast) call this. The Frame carries the routing
// envelope, an Expires freshness bound, and — for a signed announcement
// (from != "") — a domain-separated entity signature over the canonical
// {v, from, service, event, expires, content}. Expires and the signature
// are recomputed on every (re-)flood, so a queue-held broadcast re-floods
// with a fresh, still-valid window.
func pubsub_publish(from, to, service, event, id string, content, data []byte) {
	if net_pubsub == nil {
		return
	}

	var cmap map[string]any
	if len(content) > 0 {
		if err := cbor_decode_mode.Unmarshal(content, &cmap); err != nil {
			warn("Pubsub unable to decode content for frame: %v", err)
			return
		}
	}

	expires := i64toa(now() + pubsub_expires_ttl)

	var sig []byte
	if from != "" {
		strcontent, ok := pubsub_string_content(cmap)
		if !ok {
			warn("Pubsub refusing to sign non-string content for %q", from)
			return
		}
		sig = pubsub_sign(from, service, event, expires, strcontent)
	}

	f := &Frame{
		Type: frame_type_message, From: from, To: to,
		Service: service, Event: event, ID: id,
		Expires: expires, Content: cmap, Signature: sig,
	}
	if len(data) > 0 {
		f.Data = data
	}

	var buf bytes.Buffer
	if err := frame_write(&buf, f); err != nil {
		warn("Pubsub frame write failed: %v", err)
		return
	}
	net_pubsub.Publish(net_context, buf.Bytes())
	pubsub_published.Add(1)
}

// --- Entity signature (signed announcements) --------------------------

// pubsub_string_content projects a content map to map[string]string. ok
// is false if any value isn't a string: signed announcements are
// all-string by construction (numbers ride as decimal strings), so a
// non-string value on receipt means a tampered or malformed frame and the
// caller rejects it. All-string content is also what makes the canonical
// CBOR reconstruct byte-identically on the receiver — a map[string]any of
// mixed types would not round-trip reliably.
func pubsub_string_content(content map[string]any) (map[string]string, bool) {
	out := make(map[string]string, len(content))
	for k, v := range content {
		s, ok := v.(string)
		if !ok {
			return nil, false
		}
		out[k] = s
	}
	return out, true
}

// pubsub_signable returns the canonical CBOR an entity signs for a pubsub
// announcement: {v, from, service, event, expires, content} sorted
// bytewise-lexical. Mirrors claim_signable; any schema change MUST bump
// pubsub_domain.
func pubsub_signable(from, service, event, expires string, content map[string]string) ([]byte, error) {
	payload := map[string]any{
		"v":       pubsub_domain,
		"from":    from,
		"service": service,
		"event":   event,
		"expires": expires,
		"content": content,
	}
	out, err := canonical_encoder.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("pubsub: canonical encode failed: %w", err)
	}
	return out, nil
}

// pubsub_sign produces the entity signature for a signed announcement.
// Returns nil if the entity isn't local or its key can't be loaded — the
// caller then floods unsigned and receivers treat it as anonymous.
func pubsub_sign(from, service, event, expires string, content map[string]string) []byte {
	signable, err := pubsub_signable(from, service, event, expires, content)
	if err != nil {
		warn("pubsub_sign canonical encode failed for %q: %v", from, err)
		return nil
	}
	sig := entity_sign(from, string(signable))
	if sig == "" {
		return nil
	}
	return base58_decode(sig, "")
}

// pubsub_verify reconstructs the signable from a received frame and checks
// the entity signature. The entity id IS the base58 ed25519 public key —
// no directory lookup, as in claim_verify. Returns nil on success.
func pubsub_verify(from, service, event, expires string, content map[string]string, signature []byte) error {
	if from == "" {
		return errors.New("pubsub: empty from")
	}
	public := base58_decode(from, "")
	if len(public) != ed25519.PublicKeySize {
		return fmt.Errorf("pubsub: invalid from pubkey length %d", len(public))
	}
	if len(signature) != ed25519.SignatureSize {
		return fmt.Errorf("pubsub: invalid signature length %d", len(signature))
	}
	signable, err := pubsub_signable(from, service, event, expires, content)
	if err != nil {
		return err
	}
	if !ed25519.Verify(public, signable, signature) {
		return errors.New("pubsub: signature mismatch")
	}
	return nil
}

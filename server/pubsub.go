// Mochi server: GossipSub pubsub.
//
// Server-level peer-discovery and directory announcements ride GossipSub
// on the /mochi/2 topic. This file owns the subscribe loop, the
// receive-side decode/route, and the single publish path shared by every
// producer (directory / peer announcements via Message.publish, and the
// queue's broadcast re-flood via queue_send_broadcast). See
// claude/plans/pubsub.md.
//
// Each message is a self-contained Announcement. When it carries a from —
// the entity the message is ABOUT — that entity signs the whole thing:
// canonical {v, id, from, service, event, expires, content}, everything but
// the signature itself. A from that fails to verify is dropped, never
// downgraded to anonymous, because an identity is either proven or it is a
// claim by an attacker. Receivers dedup a re-flood or multi-path delivery
// via message_seen_mark.
//
// Every field a receiver acts on is signed or refused. An addressee is
// refused outright (a broadcast has none, and an unsigned one would let a
// relay re-target routing), and segment data cannot ride at all. That is
// what lets a directory row's subject be its from rather than a field read
// out of content: the row is proven, not asserted, and because the row
// stores the announcement it arrived as, the same signature re-verifies
// when it is re-served over the sync stream.
//
// Pubsub is best-effort and one-way: no per-message challenge, no
// ack/nack, no reply writer. GossipSub's StrictSign authenticates the
// relaying peer at the mesh layer, which is a different question from who
// the message is about — hence the entity signature above.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
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
	// pubsub_dropped counts inbound messages discarded by the rate limiter.
	// Counted rather than logged because the drop log line is repeat-
	// suppressed after log_repeat_threshold occurrences per window, so the
	// log undercounts a burst by design — and because the limiter runs
	// before the frame is parsed, so naming the discarded topic would mean
	// parsing every excess message, making a flood cost more rather than
	// less. Read as a snapshot either side of an operation to tell whether
	// drops coincided with it. Process-wide, not per peer: a non-zero delta
	// means messages were being discarded, not necessarily from the peer the
	// caller cared about.
	pubsub_dropped atomic.Int64
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

// Announcement is the /mochi/2 pubsub wire shape. Pubsub carries exactly one
// kind of message and GossipSub delivers it whole, so unlike the stream
// protocol's Frame it needs neither a type discriminator nor a length prefix:
// the topic says what this is, and the transport says where it ends.
type Announcement struct {
	ID        string         `cbor:"id,omitempty"`
	From      string         `cbor:"from,omitempty"`
	Service   string         `cbor:"service,omitempty"`
	Event     string         `cbor:"event,omitempty"`
	Expires   string         `cbor:"expires,omitempty"`
	Content   map[string]any `cbor:"content,omitempty"`
	Signature []byte         `cbor:"signature,omitempty"`
}

// announcement_read decodes one flooded announcement, bounding the payload
// before allocating so a flooder cannot force a large decode.
func announcement_read(data []byte) (*Announcement, error) {
	if len(data) > frame_maximum {
		return nil, fmt.Errorf("announcement: oversized %d > %d", len(data), frame_maximum)
	}
	var a Announcement
	if err := cbor_decode_mode.Unmarshal(data, &a); err != nil {
		return nil, fmt.Errorf("announcement: cbor decode failed: %w", err)
	}
	return &a, nil
}

// announcement_valid runs the envelope-level checks: well-formed from,
// service, event and id. Content is validated by the event handler.
func announcement_valid(a *Announcement) bool {
	if a.From != "" && !valid(a.From, "entity") {
		return false
	}
	if a.Service != "" && !valid(a.Service, "constant") {
		return false
	}
	if a.Event != "" && !valid(a.Event, "constant") {
		return false
	}
	if a.ID != "" && len(a.ID) > max_id_length {
		return false
	}
	return true
}

// pubsub_limiter chooses which inbound budget a message is charged against.
//
// The peers service is the control plane: the messages by which hosts learn
// each other's addresses, and which a synchronous remote request blocks on
// for remote_address_wait. It gets its own budget so the application plane —
// directory announcements and lookups, whose volume follows user activity and
// is effectively unbounded — cannot starve it, which it previously did
// exactly when a host was busiest and address resolution mattered most.
//
// Separated from pubsub_manager so the routing decision is testable; the loop
// itself only runs against a live subscription.
func pubsub_limiter(service string) *rate_limiter {
	if service == "peers" {
		return rate_limit_pubsub_control
	}
	return rate_limit_pubsub_in
}

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
		// ReceivedFrom is the last-hop mesh peer, not the originator — the
		// right identity for rate limiting and mesh-neighbour discovery
		// below. Nothing here treats it as the author: directory payloads
		// self-verify, and envelope signatures name their own entity.
		peer := m.ReceivedFrom.String()
		if peer == net_id {
			continue
		}
		// Decode before rate limiting, so the limit can tell peer control
		// traffic apart from an application flood. Only the cheap half of
		// the work moves ahead of the limit: announcement_read bounds the
		// payload before allocating and then CBOR-decodes, and
		// announcement_valid is a handful of string checks. The expensive
		// part — the entity signature verification in pubsub_receive —
		// stays behind it, so what an unauthenticated flooder can force is
		// one bounded decode rather than a public-key operation.
		f, err := announcement_read(m.Data)
		if err != nil {
			pubsub_dropped.Add(1)
			// Name the ORIGINATOR, not the relay: GossipSub forwards at the
			// mesh layer whether or not we can decode, so peer here is just
			// the last hop and blaming it sends an operator after the wrong
			// server — during a format change every relay looks guilty.
			info("Pubsub read error, originator %q via peer %q: %v", m.GetFrom().String(), peer, err)
			continue
		}
		if !announcement_valid(f) {
			pubsub_dropped.Add(1)
			info("Pubsub received invalid announcement from peer %q", peer)
			continue
		}

		// Rate limit inbound per peer, against the budget for this
		// message's plane. Bootstrap and paired peers are trusted and skip
		// the limit.
		limiter := pubsub_limiter(f.Service)
		if !peer_is_bootstrap(peer) && !limiter.allow(peer) {
			pubsub_dropped.Add(1)
			debug("Pubsub rate limited peer %q service %q", peer, f.Service)
			continue
		}
		pubsub_received.Add(1)
		pubsub_last.Store(now())
		// GetFrom is the originating peer, authenticated by GossipSub's
		// StrictSign policy (the message signature is verified against
		// this id before delivery). Handlers that act on the *author* of
		// a flooded message (peers/publish address announcements) read it
		// from Event.origin; ReceivedFrom stays the identity for rate
		// limiting and neighbour discovery.
		pubsub_receive(f, peer, m.GetFrom().String())
		peer_discovered(peer)
		peer_connect(peer)
	}
}

// pubsub_receive routes one decoded /mochi/2 announcement. The
// frame is self-contained: routing envelope, an Expires freshness bound,
// and (for signed announcements) the entity signature all travel in the
// one message — there is no stream or handshake context. Best-effort and
// one-way, so there is no challenge, no ack/nack, and no reply stream.
//
// origin is the GossipSub-authenticated originating peer (may equal
// peer when the originator is a direct mesh neighbour); "" when the
// caller has no authenticated originator.
// The frame arrives already decoded and shape-checked: pubsub_manager needs
// the service to choose a rate limit, so it decodes first and passes the
// result rather than having it parsed twice.
func pubsub_receive(f *Announcement, peer, origin string) {
	// Freshness bounds replay within the signed window.
	if !pubsub_fresh(f.Expires) {
		debug("Pubsub dropping frame with out-of-window expires %q from peer %q", f.Expires, peer)
		return
	}

	// A from names the entity the message is ABOUT and is an identity claim,
	// so it must be proven, not downgraded: a frame that claims one and fails
	// verification is hostile or corrupt and is dropped outright. An absent
	// from is simply anonymous, and the handler's Anonymous gate decides.
	if f.From != "" {
		strcontent, ok := pubsub_string_content(f.Content)
		if !ok || pubsub_verify(f.ID, f.From, f.Service, f.Event, f.Expires, strcontent, f.Signature) != nil {
			info("Pubsub dropping frame with bad signature: from=%q service=%q event=%q peer=%q", f.From, f.Service, f.Event, peer)
			return
		}
	}

	// Deduplicate atomically, coalescing a re-flooded or multi-path
	// delivery without racing the direct-stream workers that share the
	// dedup map.
	if f.ID != "" && message_seen_mark(f.ID) {
		return
	}

	e := Event{id: event_id(), msg_id: f.ID, from: f.From, service: f.Service, event: f.Event,
		peer: peer, origin: origin, content: f.Content, expires: f.Expires, signature: f.Signature}
	if err := e.route(); err != nil {
		debug("Pubsub frame route error for service %q event %q from peer %q: %v", f.Service, f.Event, peer, err)
	}
}

// pubsub_fresh reports whether an Expires timestamp (absolute Unix
// seconds, decimal string) is within the acceptance window: present, not
// yet expired, and not absurdly far in the future.
func pubsub_fresh(expires string) bool {
	exp := atoi(expires, 0)
	return exp > 0 && now() < exp && exp <= now()+pubsub_expires_max
}

// pubsub_publish floods one message to the /mochi/2 topic as a
// self-contained announcement. Producers (directory / peer announcements via
// Message.publish, the queue's broadcast re-flood via
// queue_send_broadcast) call this. The announcement carries the routing
// envelope, an Expires freshness bound, and — for a signed announcement
// (from != "") — a domain-separated entity signature over the canonical
// {v, from, service, event, expires, content}. Expires and the signature
// are recomputed on every (re-)flood, so a queue-held broadcast re-floods
// with a fresh, still-valid window.
func pubsub_publish(from, service, event, id string, content []byte) {
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
		sig = pubsub_sign(id, from, service, event, expires, strcontent)
		if sig == nil {
			warn("Pubsub refusing to flood unsigned announcement for %q", from)
			return
		}
	}

	body := cbor_encode(&Announcement{
		ID: id, From: from, Service: service, Event: event,
		Expires: expires, Content: cmap, Signature: sig,
	})
	if len(body) > frame_maximum {
		warn("Pubsub refusing to flood oversized announcement %d > %d", len(body), frame_maximum)
		return
	}
	net_pubsub.Publish(net_context, body)
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
// announcement: every frame field except the signature itself, sorted
// bytewise-lexical. Mirrors claim_signable; any schema change MUST bump
// pubsub_domain.
//
// Nothing meaningful is left out. id is covered so a captured message cannot
// be re-flooded under fresh ids to defeat dedup, and expires is covered so the
// sender owns the replay window rather than whoever relays it — an unsigned
// expires makes pubsub_expires_max decorative, since an attacker simply
// rewrites it every window. The frame type is NOT covered: pubsub carries only
// message frames, so v already implies it.
func pubsub_signable(id, from, service, event, expires string, content map[string]string) ([]byte, error) {
	payload := map[string]any{
		"v":       pubsub_domain,
		"id":      id,
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
func pubsub_sign(id, from, service, event, expires string, content map[string]string) []byte {
	signable, err := pubsub_signable(id, from, service, event, expires, content)
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
func pubsub_verify(id, from, service, event, expires string, content map[string]string, signature []byte) error {
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
	signable, err := pubsub_signable(id, from, service, event, expires, content)
	if err != nil {
		return err
	}
	if !ed25519.Verify(public, signable, signature) {
		return errors.New("pubsub: signature mismatch")
	}
	return nil
}

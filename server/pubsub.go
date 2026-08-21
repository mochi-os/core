// Mochi server: GossipSub pubsub.
//
// Peer-discovery and directory announcements ride GossipSub on the /mochi/2
// topic. This file owns the subscribe loop, the receive-side decode/route, and
// the single publish path. See claude/plans/pubsub.md.
//
// Each message is a self-contained Announcement. A from - the entity the
// message is ABOUT - signs canonical {v, id, from, service, event, expires,
// content}. A from that fails to verify is dropped, never downgraded to
// anonymous. Receivers dedup via message_seen_mark.
//
// Every field a receiver acts on is signed or refused: an addressee is refused
// outright and segment data cannot ride at all, which is what lets a directory
// row's subject be its from rather than a field read out of content.
//
// Best-effort and one-way: no per-message challenge, no ack, no reply writer.
// GossipSub's StrictSign authenticates the relaying peer, which is a different
// question from who the message is about.//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"sync/atomic"

	p2p_pubsub "github.com/libp2p/go-libp2p-pubsub"
	p2p_peer "github.com/libp2p/go-libp2p/core/peer"
)

// pubsub_topic is the GossipSub topic every Mochi server joins. Named
// because both the Join and the validator registration must use the same
// string, and a mismatch registers a validator that never runs.
const pubsub_topic = "/mochi/2"

// Operator counters surfaced by `mochictl pubsub status`
// (admin_pubsub_status): outbound publish volume, inbound message volume,
// and the last time a message was received. The live mesh peer count is
// read from ListPeers at report time, not counted here.
var (
	pubsub_published atomic.Int64
	pubsub_received  atomic.Int64
	pubsub_last      atomic.Int64
	// pubsub_dropped counts inbound messages discarded by the rate limiter.
	// Counted, not logged: the drop log is repeat-suppressed, and the limiter runs
	// before the frame is parsed. Process-wide, not per peer.
	pubsub_dropped atomic.Int64
)

// Domain separator and freshness window for /mochi/2 pubsub entity
// signatures.
const (
	// pubsub_domain is the by-construction domain separator baked into the
	// signed payload, mirroring claim_domain. Any change to the signed
	// schema MUST bump it.
	pubsub_domain = "mochi/2/pubsub"

	// pubsub_expires_ttl is how long a flooded announcement stays valid. Must
	// exceed the longest queue-broadcast retry interval (retry_delays' 3600s) and
	// the hourly peers_publish cadence; asserted by
	// TestPubsubExpiresTTLExceedsMaxRetry.
	pubsub_expires_ttl = 2 * 3600 // 2 hours

	// pubsub_expires_maximum bounds how far in the future an Expires may sit
	// before a receiver treats it as absurd — without it, a captured
	// message carrying a far-future Expires would replay long past the
	// intended window. 2x the TTL leaves generous clock-skew slack.
	pubsub_expires_maximum = 2 * pubsub_expires_ttl
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

// announcement_valid runs the envelope-level checks; content is the event
// handler's business. Delegates so the pubsub and /mochi/2/messages paths
// cannot disagree about what a well-formed envelope is.
func announcement_valid(a *Announcement) bool {
	return envelope_valid(a.From, a.Service, a.Event, a.ID)
}

// pubsub_limiter chooses which inbound budget a message is charged against. The
// peers service is the control plane - a synchronous remote request blocks on
// it for remote_address_wait - so it gets a budget the unbounded application
// plane cannot starve.
func pubsub_limiter(service string) *rate_limiter {
	if service == "peers" {
		return rate_limit_pubsub_control
	}
	return rate_limit_pubsub_in
}

// pubsub_validate is the GossipSub topic validator for /mochi/2, registered by
// net_start. It decodes, shape-checks and rate limits BEFORE the message is
// relayed; the same checks after s.Next() would already have amplified a flood.
//
// The cost ordering is deliberate: announcement_read bounds the payload before
// decoding, and signature verification stays behind the rate limit in
// pubsub_receive.
//
// Every refusal is Ignore, never Reject - Reject penalises the sender under
// peer scoring, and what is refused here is exactly what a version skew
// produces.
func pubsub_validate(ctx context.Context, from p2p_peer.ID, m *p2p_pubsub.Message) p2p_pubsub.ValidationResult {
	// ReceivedFrom is the last-hop mesh peer, not the originator — the right
	// identity for rate limiting. Nothing here treats it as the author:
	// directory payloads self-verify, and envelope signatures name their own
	// entity.
	peer := m.ReceivedFrom.String()

	// Our own publishes are validated too (Topic.Publish -> ValidateLocal),
	// and refusing them would stop this server flooding anything at all.
	if peer == net_id {
		return p2p_pubsub.ValidationAccept
	}

	f, err := announcement_read(m.Data)
	if err != nil {
		pubsub_dropped.Add(1)
		// Name the ORIGINATOR, not the relay: GossipSub forwards at the
		// mesh layer whether or not we can decode, so peer here is just
		// the last hop and blaming it sends an operator after the wrong
		// server — during a format change every relay looks guilty.
		info("Pubsub read error, originator %q via peer %q: %v", m.GetFrom().String(), peer, err)
		return p2p_pubsub.ValidationIgnore
	}
	if !announcement_valid(f) {
		pubsub_dropped.Add(1)
		info("Pubsub received invalid announcement from peer %q", peer)
		return p2p_pubsub.ValidationIgnore
	}

	// Rate limit inbound per peer, against the budget for this message's
	// plane. Bootstrap and paired peers are trusted and skip the limit.
	limiter := pubsub_limiter(f.Service)
	if !peer_is_bootstrap(peer) && !limiter.allow(peer) {
		pubsub_dropped.Add(1)
		debug("Pubsub rate limited peer %q service %q", peer, f.Service)
		return p2p_pubsub.ValidationIgnore
	}

	// Hand the decoded announcement to the subscribe loop rather than have
	// it parse the same bytes a second time.
	m.ValidatorData = f
	return p2p_pubsub.ValidationAccept
}

// pubsub_manager subscribes to the /mochi/2 topic and dispatches each inbound
// message. One goroutine for the process, started from net_start. Decode, shape
// and rate-limit checks have already run in pubsub_validate.
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
		// Set by pubsub_validate. Absent only if the validator did not run
		// (it accepts our own messages without decoding, and those are
		// skipped above), so treat a miss as a message to drop rather than
		// silently re-deriving what the gate was supposed to have checked.
		f, ok := m.ValidatorData.(*Announcement)
		if !ok {
			pubsub_dropped.Add(1)
			warn("Pubsub message from peer %q reached the loop unvalidated", peer)
			continue
		}
		pubsub_received.Add(1)
		pubsub_last.Store(now())
		// GetFrom is the originating peer, authenticated by StrictSign; handlers that
		// act on the author read it from Event.origin. ReceivedFrom stays the
		// identity for rate limiting and neighbour discovery.
		pubsub_receive(f, peer, m.GetFrom().String())
		peer_discovered(peer)
		peer_connect(peer)
	}
}

// pubsub_receive routes one decoded /mochi/2 announcement. origin is the
// GossipSub-authenticated originating peer, "" when the caller has none; the
// frame arrives already decoded and shape-checked by pubsub_validate. Guarded:
// this runs on pubsub_manager's goroutine, so an escaping panic ends pubsub for
// the process.
func pubsub_receive(f *Announcement, peer, origin string) {
	guard("pubsub_receive", nil, func() { pubsub_receive_guarded(f, peer, origin) })
}

func pubsub_receive_guarded(f *Announcement, peer, origin string) {
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

	e := Event{id: event_id(), message: f.ID, from: f.From, service: f.Service, event: f.Event,
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
	return exp > 0 && now() < exp && exp <= now()+pubsub_expires_maximum
}

// pubsub_publish floods one self-contained announcement to the /mochi/2 topic.
// For a signed announcement (from != "") it carries a domain-separated entity
// signature over {v, from, service, event, expires, content}. Expires and the
// signature are recomputed on every re-flood.
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
			if !entity_present(from) {
				debug("Pubsub skipping announcement for %q: the entity is gone", from)
				return
			}
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

// pubsub_string_content projects a content map to map[string]string; ok is
// false if any value is not a string. Signed announcements are all-string by
// construction, which is what makes the canonical CBOR round-trip
// byte-identically.
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

// pubsub_signable returns the canonical CBOR an entity signs: every frame field
// except the signature, sorted bytewise-lexical. Any schema change MUST bump
// pubsub_domain. id is covered so a captured message cannot be re-flooded under
// fresh ids, and expires so the sender owns the replay window, not the relay.
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

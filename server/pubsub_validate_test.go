// Mochi server: /mochi/2 topic validator unit tests
//
// The validator is what stops this node RELAYING junk. Every check it makes
// used to live in pubsub_manager, after s.Next() — by which point
// go-libp2p-pubsub had already forwarded the message to the mesh, so the node
// amplified a flood and only then decided it was junk. These pin both halves:
// that refusals really are refusals, and that the two things which would
// silently break the mesh (refusing our own publishes, penalising a peer on
// an older wire format) do not happen.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"

	p2p_pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	p2p_peer "github.com/libp2p/go-libp2p/core/peer"
)

// pubsub_message builds the Message shape go-libp2p-pubsub hands a validator.
//
// Takes a peer.ID rather than a string on purpose: peer.ID holds RAW
// multihash bytes and String() base58-encodes them, so peer.ID("12D3KooW...")
// is not the peer whose String() is "12D3KooW...". The validator compares
// ReceivedFrom.String() against net_id, and production sets net_id from
// net_me.ID().String(), so both sides encode identically - a test that builds
// the id from a display string is comparing two different encodings and will
// fail for a reason the production path does not have.
func pubsub_message(data []byte, received p2p_peer.ID) *p2p_pubsub.Message {
	return &p2p_pubsub.Message{
		Message:      &pb.Message{Data: data},
		ReceivedFrom: received,
	}
}

// pubsub_validate_isolated runs the validator against fresh, generous rate
// limiters so an unrelated test's budget cannot decide the outcome.
func pubsub_validate_isolated(t *testing.T, m *p2p_pubsub.Message) p2p_pubsub.ValidationResult {
	t.Helper()
	in, control := rate_limit_pubsub_in, rate_limit_pubsub_control
	t.Cleanup(func() { rate_limit_pubsub_in, rate_limit_pubsub_control = in, control })
	rate_limit_pubsub_in = &rate_limiter{entries: map[string]*rate_limit_entry{}, limit: 1000, window: 60}
	rate_limit_pubsub_control = &rate_limiter{entries: map[string]*rate_limit_entry{}, limit: 1000, window: 60}
	return pubsub_validate(context.Background(), p2p_peer.ID("origin"), m)
}

func pubsub_test_announcement(t *testing.T) []byte {
	t.Helper()
	return cbor_encode(&Announcement{
		ID: uid(), Service: "directory", Event: "publish",
		Expires: i64toa(now() + pubsub_expires_ttl),
	})
}

// A well-formed announcement is accepted, and the decoded form is handed to
// the subscribe loop rather than parsed a second time.
func TestPubsubValidateAcceptsAndPassesTheDecodedAnnouncement(t *testing.T) {
	m := pubsub_message(pubsub_test_announcement(t), p2p_peer.ID("peerA"))
	if got := pubsub_validate_isolated(t, m); got != p2p_pubsub.ValidationAccept {
		t.Fatalf("well-formed announcement got %v, want Accept", got)
	}
	f, ok := m.ValidatorData.(*Announcement)
	if !ok {
		t.Fatal("validator did not attach the decoded announcement; the loop would drop it as unvalidated")
	}
	if f.Service != "directory" {
		t.Errorf("attached announcement service = %q, want %q", f.Service, "directory")
	}
}

// Our own publishes run through the validator too (Topic.Publish ->
// ValidateLocal). Refusing them would stop this server flooding anything at
// all - the failure would show up as silence, not an error.
func TestPubsubValidateAcceptsOurOwnPublishes(t *testing.T) {
	saved := net_id
	t.Cleanup(func() { net_id = saved })

	// Derive net_id the way net_start does - from the peer id's String() -
	// so both sides of the comparison use the same encoding.
	self := p2p_peer.ID("self-peer-identity-for-test")
	net_id = self.String()

	// Deliberately undecodable: self must be accepted without even reading it.
	m := pubsub_message([]byte("not cbor at all"), self)
	if got := pubsub_validate_isolated(t, m); got != p2p_pubsub.ValidationAccept {
		t.Fatalf("own publish got %v, want Accept - this server would flood nothing", got)
	}
}

// An oversized payload is refused before it can be relayed.
func TestPubsubValidateRefusesOversized(t *testing.T) {
	m := pubsub_message(bytes.Repeat([]byte{0x41}, frame_maximum+1), p2p_peer.ID("peerA"))
	if got := pubsub_validate_isolated(t, m); got == p2p_pubsub.ValidationAccept {
		t.Error("oversized announcement was accepted, so the node would relay it")
	}
}

// A shape-invalid envelope is refused. announcement_valid rejects a from that
// is not a well-formed entity.
func TestPubsubValidateRefusesInvalidEnvelope(t *testing.T) {
	body := cbor_encode(&Announcement{
		ID: uid(), From: "not-a-valid-entity-id!!", Service: "directory", Event: "publish",
	})
	m := pubsub_message(body, p2p_peer.ID("peerA"))
	if got := pubsub_validate_isolated(t, m); got == p2p_pubsub.ValidationAccept {
		t.Error("announcement with a malformed from was accepted")
	}
}

// Over-budget traffic is refused at the validator, so a flood is not relayed.
func TestPubsubValidateRateLimitsBeforeRelaying(t *testing.T) {
	in, control := rate_limit_pubsub_in, rate_limit_pubsub_control
	t.Cleanup(func() { rate_limit_pubsub_in, rate_limit_pubsub_control = in, control })
	rate_limit_pubsub_in = &rate_limiter{entries: map[string]*rate_limit_entry{}, limit: 2, window: 60}
	rate_limit_pubsub_control = &rate_limiter{entries: map[string]*rate_limit_entry{}, limit: 2, window: 60}

	ctx := context.Background()
	for i := 0; i < 2; i++ {
		m := pubsub_message(pubsub_test_announcement(t), p2p_peer.ID("flooder"))
		if got := pubsub_validate(ctx, p2p_peer.ID("origin"), m); got != p2p_pubsub.ValidationAccept {
			t.Fatalf("message %d within budget got %v, want Accept", i, got)
		}
	}
	m := pubsub_message(pubsub_test_announcement(t), p2p_peer.ID("flooder"))
	if got := pubsub_validate(ctx, p2p_peer.ID("origin"), m); got == p2p_pubsub.ValidationAccept {
		t.Error("over-budget message was accepted, so the node would relay a flood")
	}
}

// Nothing the validator refuses may return Reject.
//
// Reject penalises the sender under GossipSub peer scoring. Everything
// refused here - an undecodable payload, an envelope whose shape we do not
// recognise - is exactly what a peer on an older wire format produces, and
// pubsub.go already notes that "during a format change every relay looks
// guilty". The 2026-07-25 flag day partitioned old peers; a Reject would have
// had every new node graylist the entire old fleet on top of it. Ignore stops
// the relay without turning a rollout into a mesh split.
func TestPubsubValidateNeverRejects(t *testing.T) {
	cases := []struct {
		name string
		data []byte
	}{
		{"undecodable", []byte("not cbor at all")},
		{"oversized", bytes.Repeat([]byte{0x41}, frame_maximum+1)},
		{"malformed from", cbor_encode(&Announcement{ID: uid(), From: "nope!!", Service: "directory"})},
		{"malformed service", cbor_encode(&Announcement{ID: uid(), Service: "not a constant!"})},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := pubsub_validate_isolated(t, pubsub_message(c.data, p2p_peer.ID("oldpeer")))
			if got == p2p_pubsub.ValidationReject {
				t.Errorf("%s returned Reject; an out-of-date peer would be penalised and "+
					"a wire-format rollout would graylist the old fleet", c.name)
			}
			if got == p2p_pubsub.ValidationAccept {
				t.Errorf("%s was accepted; the node would relay it", c.name)
			}
		})
	}
}

// The rate limiter must not be reached by a message we could not decode:
// charging a peer's budget for a payload we never understood would let a
// version skew exhaust the budget of every honest relay.
func TestPubsubValidateDoesNotChargeUndecodableToTheBudget(t *testing.T) {
	in, control := rate_limit_pubsub_in, rate_limit_pubsub_control
	t.Cleanup(func() { rate_limit_pubsub_in, rate_limit_pubsub_control = in, control })
	rate_limit_pubsub_in = &rate_limiter{entries: map[string]*rate_limit_entry{}, limit: 2, window: 60}
	rate_limit_pubsub_control = &rate_limiter{entries: map[string]*rate_limit_entry{}, limit: 2, window: 60}

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		pubsub_validate(ctx, p2p_peer.ID("origin"), pubsub_message([]byte("junk"), p2p_peer.ID("skewed")))
	}
	// Budget intact: a well-formed message from the same peer still passes.
	m := pubsub_message(pubsub_test_announcement(t), p2p_peer.ID("skewed"))
	if got := pubsub_validate(ctx, p2p_peer.ID("origin"), m); got != p2p_pubsub.ValidationAccept {
		t.Error("undecodable messages consumed the peer's rate budget")
	}
}

// The validator and the Join must name the same topic. A mismatch registers a
// validator that never runs, and every check silently moves back to being
// post-propagation - the exact defect this file exists to prevent, with no
// visible symptom.
func TestPubsubTopicConstantIsUsedForBothJoinAndValidator(t *testing.T) {
	source, err := os.ReadFile("net.go")
	if err != nil {
		t.Fatalf("reading net.go: %v", err)
	}
	for _, want := range []string{
		"gs.RegisterTopicValidator(pubsub_topic, pubsub_validate)",
		"gs.Join(pubsub_topic)",
	} {
		if !strings.Contains(string(source), want) {
			t.Errorf("net.go no longer contains %q", want)
		}
	}
}

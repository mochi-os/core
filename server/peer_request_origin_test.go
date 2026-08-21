// Mochi server: only a signature-verified broadcast can make us republish.
//
// e.origin is set in one place - pubsub.go, from GossipSub's
// StrictSign-verified GetFrom - so a direct-stream event always carries "".
// peer_request_event must check it before either branch, not only on the relay
// branch.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"strings"
	"testing"
)

// publish_pending drains and reports whether the publish loop was nudged.
// The channel is buffer-1, so a nudge is exactly one readable value.
func publish_pending() bool {
	select {
	case <-peer_publish_chan:
		return true
	default:
		return false
	}
}

// peer_request_env pins net_id for the duration of one test and leaves the
// publish channel empty.
func peer_request_env(t *testing.T, id string) {
	t.Helper()
	previous := net_id
	net_id = id
	t.Cleanup(func() { net_id = previous })
	publish_pending()
}

// TestADirectStreamCannotMakeUsRepublish is the defect. A direct stream has no
// origin, and this event names us, so the unguarded branch fired.
func TestADirectStreamCannotMakeUsRepublish(t *testing.T) {
	peer_request_env(t, "12D3KooWOurOwnPeerIdentifier")

	peer_request_event(&Event{origin: "", content: map[string]any{"id": net_id}})

	if publish_pending() {
		t.Error("an unverified direct-stream request made us republish; a caller who proved nothing decides when we announce ourselves to the mesh")
	}
}

// TestOurOwnBroadcastDoesNotMakeUsRepublish: our own pubsub message loops back
// to us, and answering it would be a publish that triggers a publish.
func TestOurOwnBroadcastDoesNotMakeUsRepublish(t *testing.T) {
	peer_request_env(t, "12D3KooWOurOwnPeerIdentifier")

	peer_request_event(&Event{origin: net_id, content: map[string]any{"id": net_id}})

	if publish_pending() {
		t.Error("our own broadcast made us republish")
	}
}

// TestAVerifiedRequestStillMakesUsRepublish is the property being protected.
// The guard must not silence the answer path a peer legitimately depends on to
// find us.
func TestAVerifiedRequestStillMakesUsRepublish(t *testing.T) {
	peer_request_env(t, "12D3KooWOurOwnPeerIdentifier")

	peer_request_event(&Event{origin: "12D3KooWSomeOtherPeer", content: map[string]any{"id": net_id}})

	if !publish_pending() {
		t.Error("a signature-verified request naming us did not nudge the publish loop; a peer asking where we are gets no answer")
	}
}

// TestARequestNamingNobodyIsIgnored covers the empty id, which used to be
// screened before the origin check and now sits after it.
func TestARequestNamingNobodyIsIgnored(t *testing.T) {
	peer_request_env(t, "12D3KooWOurOwnPeerIdentifier")

	peer_request_event(&Event{origin: "12D3KooWSomeOtherPeer", content: map[string]any{"id": ""}})

	if publish_pending() {
		t.Error("a request naming no peer made us republish")
	}
}

// TestEveryPeersHandlerChecksOriginFirst is the invariant, stated across the
// three handlers rather than in one. peer_record_event is exempt because it
// verifies the record's own libp2p signature instead - the carrier's identity
// is genuinely irrelevant there, which its comment says.
func TestEveryPeersHandlerChecksOriginFirst(t *testing.T) {
	for _, function := range []string{"func peer_request_event(", "func peer_publish_event("} {
		body := function_body(t, "peer_connect.go", function)
		guard := strings.Index(body, `e.origin == ""`)
		if guard < 0 {
			t.Errorf("%s no longer checks the origin at all, so a direct-stream message reaches it", function)
			continue
		}
		// Nothing may act before the guard. The first statement of the body is
		// the only safe place for it.
		opening := strings.Index(body, "{\n")
		if opening < 0 {
			t.Fatalf("%s: could not find the function body", function)
		}
		before := strings.TrimSpace(body[opening+2 : guard])
		before = strings.TrimSpace(strings.TrimSuffix(before, "if"))
		if before != "" {
			t.Errorf("%s runs %q before checking the origin; every branch above the guard is reachable unverified", function, before)
		}
	}
}

// TestPeersMessagesAreBroadcastOnly is why the guard costs nothing: all three
// peers/* messages go out over pubsub, so refusing a direct stream removes no
// legitimate sender.
func TestPeersMessagesAreBroadcastOnly(t *testing.T) {
	for _, target := range []struct{ file, function string }{
		{"peer_connect.go", "func peer_request_addresses("},
		{"peer_records.go", "func peer_record_relay("},
	} {
		body := function_body(t, target.file, target.function)
		if !strings.Contains(body, `"peers"`) {
			t.Errorf("%s %s no longer sends a peers message", target.file, target.function)
			continue
		}
		if !strings.Contains(body, ".publish(") {
			t.Errorf("%s %s sends its peers message by some route other than publish; a direct-stream sender would now be refused by the origin guard", target.file, target.function)
		}
	}
}

// Mochi server: a stale record replay does not silence the holders.
//
// peer_record_seen stamps the suppression map that peer_record_relay consults
// after its jitter sleep, and it ran before the monotonic gate - so replaying
// an old but validly signed record marked the peer answered and every holder
// mid-jitter dropped its relay. A signed record never expires and is broadcast
// on the open mesh, so anyone can keep one forever; peers/request is a
// broadcast too, so an attacker sees the same request the holders do and
// replays within their 0-3s jitter. The casualty is the address-book exchange
// that finds a peer which is offline or never heard the request.
//
// The obvious fix - move the stamp below the gate - would delete the feature.
// peer_record_store returns false for an EQUAL sequence as well as an older
// one, and equal is the ordinary case: every holder stores the same envelope
// at the same sequence, and one of them relaying it is exactly what the others
// are meant to suppress on.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"testing"
)

// record_answered reports whether the suppression map holds a live stamp for
// a peer - what peer_record_relay checks after its jitter.
func record_answered(id string) bool {
	peer_records_answered_lock.Lock()
	defer peer_records_answered_lock.Unlock()
	return peer_records_answered[id] >= now()-peer_record_answered_window
}

// record_forget clears any stamp so each case starts clean.
func record_forget(id string) {
	peer_records_answered_lock.Lock()
	delete(peer_records_answered, id)
	peer_records_answered_lock.Unlock()
}

// TestAStaleRecordReplayDoesNotSuppressRelays is the defect.
func TestAStaleRecordReplayDoesNotSuppressRelays(t *testing.T) {
	defer setup_peer_discovery_test(t)()

	subject, key := test_host(t)
	current := test_signed_record(t, key, subject, []string{"/ip4/192.0.2.90/tcp/1443"}, 5)
	if _, ok := peer_record_apply(subject, current); !ok {
		t.Fatal("seed record rejected")
	}
	record_forget(subject)

	// A validly signed record the holder has already superseded.
	stale := test_signed_record(t, key, subject, []string{"/ip4/192.0.2.90/tcp/1443"}, 2)
	peer_record_event(&Event{origin: "12D3KooWSomeCarrier", content: map[string]any{"record": stale}})

	if record_answered(subject) {
		t.Error("a replayed stale record marked the peer answered, so every holder suppresses its own relay and the asker gets no addresses")
	}
}

// TestAnEqualSequenceStillSuppresses is the feature the naive fix destroys:
// every holder carries the same envelope at the same sequence, so the herd
// collapse depends on an equal sequence counting as a real answer.
func TestAnEqualSequenceStillSuppresses(t *testing.T) {
	defer setup_peer_discovery_test(t)()

	subject, key := test_host(t)
	record := test_signed_record(t, key, subject, []string{"/ip4/192.0.2.91/tcp/1443"}, 5)
	if _, ok := peer_record_apply(subject, record); !ok {
		t.Fatal("seed record rejected")
	}
	record_forget(subject)

	// Another holder relays the very record we hold.
	peer_record_event(&Event{origin: "12D3KooWSomeCarrier", content: map[string]any{"record": record}})

	if !record_answered(subject) {
		t.Error("another server relaying the record we hold did not suppress our own relay; every holder answers the same request and the herd is back")
	}
}

// TestANewerRecordSuppresses: a record ahead of ours is plainly a real answer.
func TestANewerRecordSuppresses(t *testing.T) {
	defer setup_peer_discovery_test(t)()

	subject, key := test_host(t)
	if _, ok := peer_record_apply(subject, test_signed_record(t, key, subject, []string{"/ip4/192.0.2.92/tcp/1443"}, 5)); !ok {
		t.Fatal("seed record rejected")
	}
	record_forget(subject)

	newer := test_signed_record(t, key, subject, []string{"/ip4/192.0.2.93/tcp/1443"}, 9)
	peer_record_event(&Event{origin: "12D3KooWSomeCarrier", content: map[string]any{"record": newer}})

	if !record_answered(subject) {
		t.Error("a newer relayed record did not suppress our own relay")
	}
}

// TestAnUnknownPeerSuppresses: holding nothing, we cannot relay anyway
// (peer_record_relayable requires a stored record), so the stamp is harmless
// and treating "no record" as stale would be a needless special case.
func TestAnUnknownPeerSuppresses(t *testing.T) {
	defer setup_peer_discovery_test(t)()

	subject, key := test_host(t)
	record := test_signed_record(t, key, subject, []string{"/ip4/192.0.2.94/tcp/1443"}, 3)
	record_forget(subject)

	peer_record_event(&Event{origin: "12D3KooWSomeCarrier", content: map[string]any{"record": record}})

	if !record_answered(subject) {
		t.Error("a record for a peer we hold nothing for did not stamp the suppression map")
	}
	if peer_record_relayable(subject) != true {
		t.Error("the record was not stored, so this test proves nothing about the stamp being harmless")
	}
}

// TestPeerRecordCurrentSeparatesTheThreeCases pins the helper directly. The
// store's boolean cannot make this distinction, which is why the fix is not a
// reordering.
func TestPeerRecordCurrentSeparatesTheThreeCases(t *testing.T) {
	defer setup_peer_discovery_test(t)()

	subject, key := test_host(t)
	if _, ok := peer_record_apply(subject, test_signed_record(t, key, subject, []string{"/ip4/192.0.2.95/tcp/1443"}, 5)); !ok {
		t.Fatal("seed record rejected")
	}

	for _, c := range []struct {
		sequence uint64
		current  bool
		what     string
	}{
		{2, false, "older than ours - a replay"},
		{5, true, "equal to ours - the herd case"},
		{9, true, "newer than ours"},
	} {
		if got := peer_record_current(subject, c.sequence); got != c.current {
			t.Errorf("peer_record_current(sequence %d) = %v, want %v (%s)", c.sequence, got, c.current, c.what)
		}
		// The store's boolean cannot stand in: it is false for the replay and
		// for the herd case alike, which is the whole reason for this helper.
		if stored := peer_record_current(subject, c.sequence); c.sequence <= 5 && stored != (c.sequence == 5) {
			t.Errorf("sequence %d: current=%v, but the store refuses both 2 and 5 identically", c.sequence, stored)
		}
	}
	if peer_record_current("12D3KooWNeverHeardOf", 1) != true {
		t.Error("a peer we hold no record for must count as current; we cannot relay what we do not have")
	}
}

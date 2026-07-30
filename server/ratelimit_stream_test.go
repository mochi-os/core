// Mochi server: byte accounting for relayed streams.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"io"
	"strings"
	"testing"
)

func stream_bytes_reset() {
	for _, limiter := range []*rate_limiter{rate_limit_stream_client, rate_limit_stream_app} {
		limiter.lock.Lock()
		limiter.entries = make(map[string]*rate_limit_entry)
		limiter.lock.Unlock()
	}
}

// TestStreamBytesBoundsRepeatedRelays is the point of the change. The per-call cap
// bounds ONE relay; without accounting, a caller could take a capped relay as
// often as the call limit allowed - 600 fetches of a 10MB banner is 6GB, which is
// not a bandwidth bound in any useful sense.
func TestStreamBytesBoundsRepeatedRelays(t *testing.T) {
	stream_bytes_reset()

	// Charge in megabyte units until the client budget refuses.
	const megabyte = 1024 * 1024
	relayed := 0
	for i := 0; i < 100000; i++ {
		if stream_bytes_refusal("people/198.51.100.7", "people") != nil {
			break
		}
		stream_bytes_charge("people/198.51.100.7", "people", megabyte)
		relayed++
	}

	if relayed == 0 {
		t.Fatalf("the first relay was refused; the budget must clear ordinary use")
	}
	want := rate_limit_stream_client.limit / 1024 // kilobytes -> megabytes
	if relayed > want+1 {
		t.Errorf("relayed %dMB before refusal, want about %dMB - the budget is not bounding repeated relays", relayed, want)
	}
}

// TestStreamBytesIsolatesClients is why the key is the client and not the target
// entity. A per-target byte budget would be shared by everyone viewing a popular
// profile, so it would meter popularity rather than abuse and start refusing
// innocent viewers.
func TestStreamBytesIsolatesClients(t *testing.T) {
	stream_bytes_reset()

	// Exhaust one client against a target.
	for stream_bytes_refusal("people/198.51.100.7", "people") == nil {
		stream_bytes_charge("people/198.51.100.7", "people", 8*1024*1024)
	}

	if stream_bytes_refusal("people/203.0.113.9", "people") != nil {
		t.Errorf("a second client was refused because the first exhausted its budget; viewers of the same entity must not share fate")
	}
}

// TestStreamBytesAppCircuitBreaker covers the flood spread across many addresses,
// which per-client keying cannot see.
func TestStreamBytesAppCircuitBreaker(t *testing.T) {
	stream_bytes_reset()

	// Each address stays under its own budget; only the per-app total can stop it.
	blocked := false
	for i := 0; i < 4000; i++ {
		client := "people/198.51.100." + strings.Repeat("0", i%3) + itoa_test(i)
		if stream_bytes_refusal(client, "people") != nil {
			blocked = true
			break
		}
		stream_bytes_charge(client, "people", 8*1024*1024)
	}

	if !blocked {
		t.Errorf("a distributed flood was never refused; the per-app circuit breaker is not enforced")
	}
}

func itoa_test(i int) string {
	if i == 0 {
		return "0"
	}
	out := []byte{}
	for i > 0 {
		out = append([]byte{byte('0' + i%10)}, out...)
		i /= 10
	}
	return string(out)
}

// TestStreamBytesClearsALargeLegitimateTransfer is the constraint that stops this
// from being tuned into a bug. repositories' archive route is PUBLIC, so an
// anonymous caller downloads a whole repository through this path; a budget that
// cannot pass one relay of the maximum permitted size would truncate honest
// clones. One relay is capped at stream_maximum_default, so the budget has to
// clear at least that.
func TestStreamBytesClearsALargeLegitimateTransfer(t *testing.T) {
	stream_bytes_reset()

	const chunk = 4 * 1024 * 1024
	relayed := 0
	for relayed < int(stream_maximum_default) {
		if stream_bytes_refusal("repositories/198.51.100.7", "repositories") != nil {
			t.Fatalf("an anonymous archive download was refused after %d of %d bytes; a single maximum-size relay must complete",
				relayed, int64(stream_maximum_default))
		}
		stream_bytes_charge("repositories/198.51.100.7", "repositories", chunk)
		relayed += chunk
	}
}

// TestStreamLimitReaderChargesWhatItReads ties the accounting to the relay: the
// reader must charge the bytes it actually takes from the peer, or the budget
// measures nothing.
func TestStreamLimitReaderChargesWhatItReads(t *testing.T) {
	stream_bytes_reset()

	body := strings.Repeat("x", 200*1024)
	limited := &stream_limit_reader{
		reader:    strings.NewReader(body),
		remaining: int64(len(body)),
		client:    "people/198.51.100.7",
		app:       "people",
	}
	if _, err := io.Copy(io.Discard, limited); err != nil {
		t.Fatalf("copy returned %v", err)
	}

	rate_limit_stream_client.lock.Lock()
	charged := rate_limit_stream_client.entries["people/198.51.100.7"].count
	rate_limit_stream_client.lock.Unlock()

	// Kilobytes, rounded up per read.
	if charged < 200 {
		t.Errorf("charged %dKB for a 200KB relay; the budget is not being charged what was read", charged)
	}
}

// TestStreamLimitReaderStopsWhenBudgetSpent covers exhaustion DURING a relay. A
// single relay can be a gigabyte, so a budget checked only when it starts would
// bound how many relays may begin rather than how much traffic they cause.
func TestStreamLimitReaderStopsWhenBudgetSpent(t *testing.T) {
	stream_bytes_reset()

	// Spend the client's budget before the relay begins its second read.
	for stream_bytes_refusal("people/198.51.100.7", "people") == nil {
		stream_bytes_charge("people/198.51.100.7", "people", 16*1024*1024)
	}

	endless := &stream_counting_reader{}
	limited := &stream_limit_reader{
		reader:    endless,
		remaining: int64(stream_maximum_default),
		client:    "people/198.51.100.7",
		app:       "people",
	}
	written, err := io.Copy(io.Discard, limited)
	if err != nil {
		t.Fatalf("copy returned %v", err)
	}

	if written != 0 {
		t.Errorf("relayed %d bytes on a spent budget, want 0", written)
	}
	if !limited.spent {
		t.Errorf("the relay did not record that the budget was spent, so it would be reported as a complete body")
	}
}

// TestStreamLimitReaderUnmeteredWithoutClient keeps core's own internal relays
// working: with no HTTP caller to attribute, there is no budget to charge.
func TestStreamLimitReaderUnmeteredWithoutClient(t *testing.T) {
	stream_bytes_reset()

	body := strings.Repeat("x", 64*1024)
	limited := &stream_limit_reader{reader: strings.NewReader(body), remaining: int64(len(body))}
	written, err := io.Copy(io.Discard, limited)
	if err != nil {
		t.Fatalf("copy returned %v", err)
	}
	if written != int64(len(body)) {
		t.Errorf("relayed %d of %d bytes with no client set", written, len(body))
	}
	if limited.spent {
		t.Errorf("an unattributed relay was charged against a budget")
	}
}

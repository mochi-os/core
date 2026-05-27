// Tests for protocol2_cache.go — per-(peer, protocol) cache and
// multistream error detection.
//
// Phase 3f per claude/plans/protocol2.md → Testing strategy.

package main

import (
	"errors"
	"fmt"
	"testing"

	p2p_protocol "github.com/libp2p/go-libp2p/core/protocol"
	multistream "github.com/multiformats/go-multistream"
)

func reset_protocol_cache(t *testing.T) {
	t.Helper()
	protocol_known_lock.Lock()
	for k := range protocol_known {
		delete(protocol_known, k)
	}
	protocol_known_lock.Unlock()
}

func TestProtocolKnownUnknownIsZero(t *testing.T) {
	reset_protocol_cache(t)
	if got := protocol_known_get("never-seen", protocol_messages); got != protocol_state_unknown {
		t.Errorf("get on empty cache: got %d, want %d", got, protocol_state_unknown)
	}
}

func TestProtocolKnownSetGet(t *testing.T) {
	reset_protocol_cache(t)
	protocol_known_set("p1", protocol_messages, protocol_state_supported)
	protocol_known_set("p1", protocol_stream, protocol_state_unsupported)

	if got := protocol_known_get("p1", protocol_messages); got != protocol_state_supported {
		t.Errorf("messages: got %d, want supported", got)
	}
	if got := protocol_known_get("p1", protocol_stream); got != protocol_state_unsupported {
		t.Errorf("stream: got %d, want unsupported", got)
	}
	// Independent per-peer state.
	if got := protocol_known_get("p2", protocol_messages); got != protocol_state_unknown {
		t.Errorf("unknown peer leaked from another: got %d", got)
	}
}

func TestProtocolKnownClearDropsAllProtocols(t *testing.T) {
	reset_protocol_cache(t)
	protocol_known_set("p1", protocol_messages, protocol_state_supported)
	protocol_known_set("p1", protocol_stream, protocol_state_unsupported)
	protocol_known_set("p2", protocol_messages, protocol_state_supported)

	protocol_known_clear("p1")
	if got := protocol_known_get("p1", protocol_messages); got != protocol_state_unknown {
		t.Errorf("p1 messages: got %d after clear, want unknown", got)
	}
	if got := protocol_known_get("p1", protocol_stream); got != protocol_state_unknown {
		t.Errorf("p1 stream: got %d after clear, want unknown", got)
	}
	// p2 untouched.
	if got := protocol_known_get("p2", protocol_messages); got != protocol_state_supported {
		t.Errorf("p2 was wrongly cleared: got %d", got)
	}
}

func TestProtocolKnownOverwriteIsCheap(t *testing.T) {
	reset_protocol_cache(t)
	for i := 0; i < 100; i++ {
		state := protocol_state_supported
		if i%2 == 0 {
			state = protocol_state_unsupported
		}
		protocol_known_set("p", protocol_messages, state)
	}
	// Final state should reflect the last write (i=99 → odd → supported).
	if got := protocol_known_get("p", protocol_messages); got != protocol_state_supported {
		t.Errorf("after 100 overwrites: got %d, want supported", got)
	}
}

// --- is_protocol_not_supported ----------------------------------------

func TestIsProtocolNotSupportedRecognisesMultistream(t *testing.T) {
	// The function uses errors.As so a wrapped error is also matched.
	err := multistream.ErrNotSupported[string]{Protos: []string{protocol_messages}}
	if !is_protocol_not_supported(err) {
		t.Error("plain ErrNotSupported not recognised")
	}
	wrapped := fmt.Errorf("protocol open: %w", err)
	if !is_protocol_not_supported(wrapped) {
		t.Error("wrapped ErrNotSupported not recognised")
	}
}

func TestIsProtocolNotSupportedRecognisesProtocolIDSpecialisation(t *testing.T) {
	// Regression: libp2p's basic_host returns
	// ErrNotSupported[protocol.ID] (a typed-string alias), not
	// ErrNotSupported[string]. errors.As only matches exact types;
	// missing this specialisation broke /mochi/2/stream fallback
	// against /mochi/1-only peers — the publisher's "directory_download
	// stream_open failed" never cached as unsupported, so every
	// retry re-attempted /mochi/2/stream from scratch.
	err := multistream.ErrNotSupported[p2p_protocol.ID]{Protos: []p2p_protocol.ID{p2p_protocol.ID(protocol_stream)}}
	if !is_protocol_not_supported(err) {
		t.Error("ErrNotSupported[protocol.ID] not recognised")
	}
	wrapped := fmt.Errorf("failed to negotiate protocol: %w", err)
	if !is_protocol_not_supported(wrapped) {
		t.Error("wrapped ErrNotSupported[protocol.ID] not recognised")
	}
}

func TestIsProtocolNotSupportedStringFallback(t *testing.T) {
	// Belt-and-braces: a future libp2p wrapping that strips the typed
	// error chain should still be caught by the string-match fallback.
	plain := errString("multistream: protocols not supported: [/mochi/2/stream]")
	if !is_protocol_not_supported(plain) {
		t.Error("string fallback missed the standard phrasing")
	}
}

func TestIsProtocolNotSupportedIgnoresUnrelated(t *testing.T) {
	if is_protocol_not_supported(nil) {
		t.Error("nil error treated as unsupported")
	}
	if is_protocol_not_supported(errors.New("connection refused")) {
		t.Error("unrelated error treated as unsupported")
	}
}

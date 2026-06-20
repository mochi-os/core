// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Tests for multistream not-supported error detection
// (is_protocol_not_supported in protocol2_sender.go).

package main

import (
	"errors"
	"fmt"
	"testing"

	p2p_protocol "github.com/libp2p/go-libp2p/core/protocol"
	multistream "github.com/multiformats/go-multistream"
)

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
	// Regression: libp2p's basic_host returns ErrNotSupported[protocol.ID]
	// (a typed-string alias), not ErrNotSupported[string]. errors.As only
	// matches exact types; missing this specialisation meant a peer that
	// doesn't speak /mochi/2/stream wasn't recognised as not-supported, so
	// peer_protocol_open mishandled it (the publisher's "directory_download
	// stream_open failed" was retried as a transient error rather than
	// silenced).
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

// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Tests that each inbound P2P entry point is guarded. guard_test.go covers
// guard() itself; these cover the wiring, which is where the defect was.
// recover() only sees panics on its own goroutine, so the guard has to be at
// the entry point and cannot be hoisted to the dispatch they share.

package main

import (
	"testing"

	p2p_network "github.com/libp2p/go-libp2p/core/network"
)

// panicking_stream is a libp2p stream whose very first use panics.
// receive_messages and receive_stream both open with s.Conn().RemotePeer(), so
// this faults inside the guarded body rather than before it. The embedded nil
// interface satisfies the type; only the two methods below are ever called.
type panicking_stream struct {
	p2p_network.Stream
	reset bool
}

func (s *panicking_stream) Conn() p2p_network.Conn { panic("connection blew up") }
func (s *panicking_stream) Reset() error           { s.reset = true; return nil }

// TestReceiveMessagesContainsPanic is the regression. Without the guard the
// panic escapes to the goroutine top, which in production is the whole server.
func TestReceiveMessagesContainsPanic(t *testing.T) {
	stream := &panicking_stream{}
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("a panic escaped receive_messages: %v — in production this reaches the top of a libp2p goroutine and takes the process down", r)
		}
	}()

	receive_messages(stream)

	if !stream.reset {
		t.Error("receive_messages contained the panic but did not reset the stream; the peer is left waiting on a handler that has already died")
	}
}

// TestReceiveStreamContainsPanic pins the other stream handler;
// pubsub_receive's equivalent lives in guard_test.go.
func TestReceiveStreamContainsPanic(t *testing.T) {
	stream := &panicking_stream{}
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("a panic escaped receive_stream: %v", r)
		}
	}()

	receive_stream(stream)

	if !stream.reset {
		t.Error("receive_stream contained the panic but did not reset the stream")
	}
}

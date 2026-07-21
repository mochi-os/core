// Mochi server: listener connection limit tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"net"
	"testing"
	"time"
)

// TestWebListenBoundsConnections pins that the listener stops accepting past
// its ceiling.
//
// web_server's timeouts bound one connection's header phase and its gap
// between requests; nothing bounded how many could exist at once, and each one
// costs a goroutine, a file descriptor and read and write buffers.
func TestWebListenBoundsConnections(t *testing.T) {
	listener, err := web_listen("127.0.0.1:0", 2)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	// Accept in the background so the limiter, not a missing Accept call, is
	// what governs.
	accepted := make(chan net.Conn, 8)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			accepted <- conn
		}
	}()

	// Two connections fit the ceiling.
	held := []net.Conn{}
	defer func() {
		for _, c := range held {
			c.Close()
		}
	}()
	for i := 0; i < 2; i++ {
		conn, err := net.DialTimeout("tcp", listener.Addr().String(), 5*time.Second)
		if err != nil {
			t.Fatalf("connection %d was refused below the ceiling: %v", i+1, err)
		}
		held = append(held, conn)
		select {
		case server := <-accepted:
			held = append(held, server)
		case <-time.After(5 * time.Second):
			t.Fatalf("connection %d was never accepted", i+1)
		}
	}

	// The third must not be accepted. The TCP handshake still completes — it
	// waits in the kernel backlog, holding no descriptor of ours — so the
	// check is that the server side never sees it.
	extra, err := net.DialTimeout("tcp", listener.Addr().String(), 5*time.Second)
	if err == nil {
		defer extra.Close()
	}
	select {
	case <-accepted:
		t.Error("a connection past the ceiling was accepted, so the limit does not bind")
	case <-time.After(500 * time.Millisecond):
		// Correct: left queued rather than accepted.
	}

	// Releasing one must free a slot, or the limit would be a permanent cap on
	// the server's life rather than on simultaneous use.
	held[0].Close()
	held[1].Close()
	select {
	case server := <-accepted:
		server.Close()
	case <-time.After(5 * time.Second):
		t.Error("closing a connection did not free a slot for the queued one")
	}
}

// TestWebListenUnlimited pins the documented escape hatch: a configured zero
// disables the ceiling rather than refusing every connection.
func TestWebListenUnlimited(t *testing.T) {
	listener, err := web_listen("127.0.0.1:0", 0)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			conn.Close()
		}
	}()

	for i := 0; i < 4; i++ {
		conn, err := net.DialTimeout("tcp", listener.Addr().String(), 5*time.Second)
		if err != nil {
			t.Fatalf("connection %d refused with the limit disabled: %v", i+1, err)
		}
		conn.Close()
	}
}

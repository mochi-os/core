// Mochi server: Panic containment on the inbound P2P paths
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"testing"
)

// TestGuardContainsPanic — the whole point: a panic must not reach the
// goroutine's top, because on the two inbound P2P paths that ends the process
// rather than the frame.
func TestGuardContainsPanic(t *testing.T) {
	survived := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panic escaped guard: %v", r)
			}
		}()
		guard("test", nil, func() { panic("boom") })
		survived = true
	}()
	if !survived {
		t.Error("guard did not return after containing the panic")
	}
}

// TestGuardRunsCleanup — `after` exists to shut the faulted subject down, e.g.
// resetting the libp2p stream so the peer is not left waiting on a handler
// that has already died.
func TestGuardRunsCleanup(t *testing.T) {
	cleaned := false
	guard("test", func() { cleaned = true }, func() { panic("boom") })
	if !cleaned {
		t.Error("cleanup did not run during recovery")
	}
}

// TestGuardSurvivesCleanupPanic — a second panic while cleaning up must not
// defeat the first. Resetting a stream that the panic already tore down is
// exactly the case.
func TestGuardSurvivesCleanupPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("a panic in cleanup escaped: %v", r)
		}
	}()
	guard("test", func() { panic("cleanup blew up too") }, func() { panic("boom") })
}

// TestGuardSkipsCleanupWithoutPanic — cleanup is recovery-only. Running it on
// the happy path would reset every healthy stream.
func TestGuardSkipsCleanupWithoutPanic(t *testing.T) {
	cleaned := false
	ran := false
	guard("test", func() { cleaned = true }, func() { ran = true })
	if !ran {
		t.Error("guard did not run the body")
	}
	if cleaned {
		t.Error("cleanup ran even though nothing panicked")
	}
}

// TestGuardIsPerGoroutine — the reason each entry point needs its own guard
// rather than one around the code that spawns them. recover() only sees panics
// raised on its OWN goroutine, so a guard on the parent catches nothing.
func TestGuardIsPerGoroutine(t *testing.T) {
	contained := false
	// The guard is INSIDE the goroutine, which is the arrangement that works.
	// Completion is signalled after guard RETURNS, not from the body: a defer
	// inside the body runs while the guard is still unwinding, so waiting on it
	// would read `contained` before recovery had set it.
	done := make(chan struct{})
	go func() {
		guard("test", func() { contained = true }, func() { panic("boom") })
		close(done)
	}()
	<-done
	if !contained {
		t.Error("a panic on its own goroutine was not contained by a guard on that goroutine")
	}
}

// TestPubsubReceiveContainsPanic — the real path, not just the helper. A nil
// Announcement dereferences inside the handler; before the guard that killed
// pubsub for the whole process, since pubsub_receive runs on pubsub_manager's
// long-lived goroutine.
func TestPubsubReceiveContainsPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("pubsub_receive let a panic escape: %v", r)
		}
	}()
	pubsub_receive(nil, "peerZ", "")
}

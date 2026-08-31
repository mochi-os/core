// Mochi server: the sender side of both /mochi/2 handshakes must be bounded.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
)

// fake_timeout is a net.Error, the shape yamux returns on a read deadline. QUIC
// returns os.ErrDeadlineExceeded instead, so deadline_exceeded must accept both.
type fake_timeout struct{ timeout bool }

func (f fake_timeout) Error() string { return "i/o deadline reached" }
func (f fake_timeout) Timeout() bool { return f.timeout }
func (f fake_timeout) Temporary() bool {
	return f.timeout
}

func TestDeadlineExceededRecognisesBothTransports(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"QUIC sentinel", os.ErrDeadlineExceeded, true},
		{"yamux net.Error", fake_timeout{timeout: true}, true},
		// hello_read wraps with %w, and frame_read wraps the body read again,
		// so the real error arrives two or three layers down.
		{"wrapped as hello_read wraps", fmt.Errorf("hello: %w", os.ErrDeadlineExceeded), true},
		{"wrapped twice", fmt.Errorf("hello: %w",
			fmt.Errorf("frame: truncated body (wanted 4): %w", os.ErrDeadlineExceeded)), true},
		{"wrapped net.Error", fmt.Errorf("hello: %w", fake_timeout{timeout: true}), true},

		// Negatives: these are real failures and must stay real failures, or a
		// protocol violation would be reported to the queue as "unreachable"
		// and retried forever.
		{"clean close", io.EOF, false},
		{"truncated stream", io.ErrUnexpectedEOF, false},
		{"protocol violation", errors.New("hello: version mismatch want 2 got 1"), false},
		{"non-timeout net.Error", fake_timeout{timeout: false}, false},
		{"nil", nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := deadline_exceeded(c.err); got != c.want {
				t.Errorf("deadline_exceeded(%v) = %v, want %v", c.err, got, c.want)
			}
		})
	}
}

// Both senders open a real libp2p stream inside the function under test, so the
// deadline itself cannot be driven from a unit test. Pin its shape at the source
// instead, as TestStreamPreOpenBoundsArePinned does for the receiving side.
func TestSenderHandshakeDeadlinesArePinned(t *testing.T) {
	for _, file := range []string{"protocol2_sender.go", "protocol2_stream.go"} {
		source, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		body := string(source)

		// stream_open_timeout, not a fresh literal: the two sides of the
		// handshake are bounded by the same number.
		set := strings.Index(body, "SetReadDeadline(time.Now().Add(stream_open_timeout))")
		if set < 0 {
			t.Errorf("%s: the outbound handshake no longer sets a read deadline", file)
			continue
		}
		clear := strings.LastIndex(body, "SetReadDeadline(time.Time{})")
		if clear < 0 {
			t.Errorf("%s: the handshake deadline is never cleared, so an established stream would inherit it", file)
			continue
		}
		if clear < set {
			t.Errorf("%s: the deadline is cleared before it is set", file)
		}
		// A timeout must degrade to unreachable, not to a hard error: the queue
		// retries the former and gives up on the latter.
		if !strings.Contains(body, "deadline_exceeded(err)") {
			t.Errorf("%s: a handshake timeout is not mapped to error_sender_unreachable", file)
		}
	}
}

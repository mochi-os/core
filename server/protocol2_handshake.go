// Mochi server: Protocol 2 — shared handshake.
//
// /mochi/2/messages and /mochi/2/stream both begin with the same
// sequence:
//
//   1. Receiver writes a `hello` frame with a fresh per-stream
//      challenge, the protocol version, a session ID for log
//      correlation, and its supported codecs + features.
//   2. Sender writes a `caps` frame with its own codecs + features.
//   3. Sender writes one or more `claim` frames — one per local entity
//      that will send on the stream. Receiver verifies the per-(stream,
//      entity) signature and caches `claimed[From]=true`.
//
// After step 3 the two protocols diverge: /mochi/2/messages enters the
// multiplexed dispatch loop; /mochi/2/stream waits for an `open` frame
// then transitions to raw bytes.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	crand "crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
)

// session_id returns a fresh 8-byte hex log-correlation token, logged on both
// sides at stream open and close.
func session_id() string {
	var b [8]byte
	if _, err := crand.Read(b[:]); err != nil {
		// crypto/rand failing is "the OS is broken" — return a marker
		// that's visibly wrong so logs make the cause obvious.
		return "00000000-rand-fail"
	}
	return hex.EncodeToString(b[:])
}

// hello_challenge fills a fresh challenge_size_v2 buffer from the OS
// entropy pool. See claude/plans/protocol2.md Hello handshake → Challenge
// generation for the hard rules (no derivation, no reuse, no logging).
func hello_challenge() ([]byte, error) {
	b := make([]byte, challenge_size_v2)
	if _, err := crand.Read(b); err != nil {
		return nil, fmt.Errorf("hello: challenge entropy failed: %w", err)
	}
	return b, nil
}

func hello_write(w io.Writer, version int, session string, challenge []byte,
	codecs, features []string) error {
	return frame_write(w, &Frame{
		Type:      frame_type_hello,
		Version:   version,
		Session:   session,
		Challenge: challenge,
		Codecs:    codecs,
		Features:  features,
	})
}

// hello_read decodes the receiver's hello frame and runs the
// rejection tests demanded by Challenge generation. The sender treats
// a rejected hello (bad challenge, wrong version) as a protocol
// negotiation failure.
func hello_read(r io.Reader, version int) (*Frame, error) {
	f, err := frame_read(r)
	if err != nil {
		return nil, fmt.Errorf("hello: %w", err)
	}
	if f.Type != frame_type_hello {
		return nil, fmt.Errorf("hello: unexpected type %q", f.Type)
	}
	if f.Version != version {
		return nil, fmt.Errorf("hello: version mismatch want %d got %d", version, f.Version)
	}
	if err := frame_reject_challenge(f.Challenge); err != nil {
		return nil, err
	}
	return f, nil
}

// caps_write writes the sender's caps frame. MUST be the sender's first frame;
// the receiver closes the stream on anything else. The challenge is mandatory:
// the answering side signs it to prove it holds the entity being addressed.
func caps_write(w io.Writer, codecs, features []string, challenge []byte) error {
	return frame_write(w, &Frame{
		Type:      frame_type_caps,
		Codecs:    codecs,
		Features:  features,
		Challenge: challenge,
	})
}

// caps_read decodes the sender's caps frame. Receiver MUST call this
// for the first frame after writing hello; receiving anything else
// before caps is a protocol violation.
func caps_read(r io.Reader) (*Frame, error) {
	f, err := frame_read(r)
	if err != nil {
		return nil, fmt.Errorf("caps: %w", err)
	}
	if f.Type != frame_type_caps {
		return nil, fmt.Errorf("caps: first frame was %q not caps", f.Type)
	}
	return f, nil
}

// claim_write writes one `claim` frame for `entity`. receiver must be the
// authenticated peer this stream runs to.
func claim_write(w io.Writer, entity string, challenge []byte, receiver string, protocol string) error {
	sig := claim_sign(entity, challenge, receiver, protocol)
	if sig == nil {
		return fmt.Errorf("claim: signing failed for entity %q", entity)
	}
	return frame_write(w, &Frame{
		Type:      frame_type_claim,
		From:      entity,
		Signature: sig,
	})
}

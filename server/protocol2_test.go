// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Tests for protocol2.go — framing, codec, canonical CBOR, helpers.
//
// Phase 3a per claude/plans/protocol2.md → Testing strategy.

package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	cbor "github.com/fxamacker/cbor/v2"
)

func init() {
	// All protocol2 tests rely on the canonical-CBOR encoder and zstd
	// singletons. protocol2_init is idempotent, safe to call here.
	protocol2_init()
}

// --- Framing -----------------------------------------------------------

func TestFrameWriteReadRoundTrip(t *testing.T) {
	cases := []*Frame{
		{Type: frame_type_hello, Version: 2, Session: "abcd1234", Challenge: bytes.Repeat([]byte{0x01}, challenge_size_v2), Codecs: []string{"zstd"}, Features: nil},
		{Type: frame_type_caps, Codecs: []string{"zstd"}, Features: []string{"batch"}},
		{Type: frame_type_claim, From: test_entity_id('a'), Signature: bytes.Repeat([]byte{0x02}, ed25519.SignatureSize)},
		{Type: frame_type_message, ID: "msg-1", From: test_entity_id('b'), To: test_entity_id('c'),
			Service: "feeds", Event: "post/novelty", FromApp: "feeds",
			Services: []string{"feeds"}, Priority: frame_priority_interactive,
			Content: map[string]any{"k": "v"}, Data: []byte{0x09, 0x0a, 0x0b}},
		{Type: frame_type_ack, Replies: []string{"id-1", "id-2", "id-3"}},
		{Type: frame_type_fail, Replies: []string{"id-9"}, Reason: fail_unclaimed},
		{Type: frame_type_ping, ID: "ping-1"},
		{Type: frame_type_pong, ID: "ping-1"},
		{Type: frame_type_bye},
		{Type: frame_type_open, ID: "open-1", From: test_entity_id('d'), Service: "files", Event: "download"},
	}
	for _, want := range cases {
		t.Run(want.Type, func(t *testing.T) {
			var buf bytes.Buffer
			if err := frame_write(&buf, want); err != nil {
				t.Fatalf("frame_write: %v", err)
			}
			got, err := frame_read(&buf)
			if err != nil {
				t.Fatalf("frame_read: %v", err)
			}
			if got.Type != want.Type {
				t.Errorf("Type: got %q want %q", got.Type, want.Type)
			}
			if got.ID != want.ID {
				t.Errorf("ID: got %q want %q", got.ID, want.ID)
			}
			if !bytes.Equal(got.Signature, want.Signature) {
				t.Errorf("Signature: got %x want %x", got.Signature, want.Signature)
			}
			if !bytes.Equal(got.Data, want.Data) {
				t.Errorf("Data: got %x want %x", got.Data, want.Data)
			}
			if !bytes.Equal(got.Challenge, want.Challenge) {
				t.Errorf("Challenge: got %x want %x", got.Challenge, want.Challenge)
			}
		})
	}
}

func TestFrameOversizedLengthRejected(t *testing.T) {
	// Construct a length prefix > 16 MB and verify frame_read rejects
	// it BEFORE allocating the buffer.
	var buf bytes.Buffer
	var lenbuf [frame_length_size]byte
	binary.BigEndian.PutUint32(lenbuf[:], frame_maximum+1)
	buf.Write(lenbuf[:])
	// No body written — frame_read shouldn't get that far.

	_, err := frame_read(&buf)
	if err == nil {
		t.Fatal("frame_read accepted oversized length")
	}
	if !errors.Is(err, err) /*tautology*/ {
		// Just confirm we got an error before reading the body.
	}
	if buf.Len() > 0 {
		t.Errorf("frame_read consumed body bytes (%d remaining) on oversized length", buf.Len())
	}
}

func TestFrameZeroLengthRejected(t *testing.T) {
	var buf bytes.Buffer
	buf.Write([]byte{0, 0, 0, 0})
	_, err := frame_read(&buf)
	if err == nil {
		t.Fatal("frame_read accepted zero length")
	}
}

func TestFrameTruncatedLengthIsEOF(t *testing.T) {
	// Short read on the length prefix is treated as stream death.
	var buf bytes.Buffer
	buf.Write([]byte{0x00, 0x00}) // only 2 of 4 length bytes
	_, err := frame_read(&buf)
	if err == nil {
		t.Fatal("frame_read accepted truncated length")
	}
	// Should be io.EOF or io.ErrUnexpectedEOF (both are "stream died").
	if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("frame_read on truncated length: got %v, want EOF/ErrUnexpectedEOF", err)
	}
}

func TestFrameTruncatedBodyRejected(t *testing.T) {
	// Write length=100 but only 10 bytes of body.
	var buf bytes.Buffer
	var lenbuf [frame_length_size]byte
	binary.BigEndian.PutUint32(lenbuf[:], 100)
	buf.Write(lenbuf[:])
	buf.Write(bytes.Repeat([]byte{0xaa}, 10))
	_, err := frame_read(&buf)
	if err == nil {
		t.Fatal("frame_read accepted truncated body")
	}
}

func TestFrameMalformedCBORRejected(t *testing.T) {
	// Length 10, then 10 bytes of nonsense CBOR.
	var buf bytes.Buffer
	var lenbuf [frame_length_size]byte
	binary.BigEndian.PutUint32(lenbuf[:], 10)
	buf.Write(lenbuf[:])
	buf.Write([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})
	_, err := frame_read(&buf)
	if err == nil {
		t.Fatal("frame_read accepted malformed CBOR")
	}
}

func TestFrameUnknownTypeRejected(t *testing.T) {
	// Encode a frame with Type="not-a-real-type" — should be rejected.
	body, err := cbor.Marshal(map[string]any{"type": "not-a-real-type"})
	if err != nil {
		t.Fatalf("cbor.Marshal: %v", err)
	}
	var buf bytes.Buffer
	var lenbuf [frame_length_size]byte
	binary.BigEndian.PutUint32(lenbuf[:], uint32(len(body)))
	buf.Write(lenbuf[:])
	buf.Write(body)

	_, err = frame_read(&buf)
	if err == nil {
		t.Fatal("frame_read accepted unknown Type")
	}
}

func TestFrameMissingTypeRejected(t *testing.T) {
	body, err := cbor.Marshal(map[string]any{"id": "no-type"})
	if err != nil {
		t.Fatalf("cbor.Marshal: %v", err)
	}
	var buf bytes.Buffer
	var lenbuf [frame_length_size]byte
	binary.BigEndian.PutUint32(lenbuf[:], uint32(len(body)))
	buf.Write(lenbuf[:])
	buf.Write(body)

	_, err = frame_read(&buf)
	if err == nil {
		t.Fatal("frame_read accepted frame with no Type")
	}
}

func TestFrameForwardsCompatIgnoresUnknownFields(t *testing.T) {
	// A future sender adds a `foo` field; current receiver should
	// silently ignore it and decode the rest cleanly. CBOR struct
	// decoder skips unknown map keys by default.
	body, err := cbor.Marshal(map[string]any{
		"type":           frame_type_message,
		"id":             "test-id",
		"from":           test_entity_id('e'),
		"to":             test_entity_id('f'),
		"foo":            "unknown future field",
		"another-future": 42,
	})
	if err != nil {
		t.Fatalf("cbor.Marshal: %v", err)
	}
	var buf bytes.Buffer
	var lenbuf [frame_length_size]byte
	binary.BigEndian.PutUint32(lenbuf[:], uint32(len(body)))
	buf.Write(lenbuf[:])
	buf.Write(body)

	got, err := frame_read(&buf)
	if err != nil {
		t.Fatalf("frame_read with unknown fields: %v", err)
	}
	if got.Type != frame_type_message || got.ID != "test-id" {
		t.Errorf("unexpected decode: %+v", got)
	}
}

// --- Codec -------------------------------------------------------------

func TestCodecZstdRoundTrip(t *testing.T) {
	// Payload large enough to compress (over threshold) and compressible
	// enough that zstd will actually reduce its size.
	payload := bytes.Repeat([]byte("hello world "), 1000)
	codec, compressed, err := frame_compress(payload, codec_zstd)
	if err != nil {
		t.Fatalf("frame_compress: %v", err)
	}
	if codec != codec_zstd {
		t.Fatalf("frame_compress returned codec=%d, want %d", codec, codec_zstd)
	}
	if len(compressed) >= len(payload) {
		t.Errorf("zstd didn't compress: original=%d compressed=%d", len(payload), len(compressed))
	}
	round, err := frame_decompress(compressed, codec_zstd)
	if err != nil {
		t.Fatalf("frame_decompress: %v", err)
	}
	if !bytes.Equal(round, payload) {
		t.Errorf("round-trip mismatch: original=%d round=%d", len(payload), len(round))
	}
}

func TestCodecSmallPayloadSkipsCompression(t *testing.T) {
	// Under the threshold — should return codec_none even if zstd
	// requested.
	payload := bytes.Repeat([]byte("x"), codec_threshold-1)
	codec, out, err := frame_compress(payload, codec_zstd)
	if err != nil {
		t.Fatalf("frame_compress: %v", err)
	}
	if codec != codec_none {
		t.Errorf("small payload: codec=%d, want %d", codec, codec_none)
	}
	if !bytes.Equal(out, payload) {
		t.Errorf("small payload: output mutated")
	}
}

func TestCodecInflationDowngrades(t *testing.T) {
	// Random bytes don't compress; frame_compress should detect the
	// inflation and downgrade to codec_none rather than ship a
	// larger frame.
	payload := make([]byte, codec_threshold*4)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	codec, out, err := frame_compress(payload, codec_zstd)
	if err != nil {
		t.Fatalf("frame_compress: %v", err)
	}
	if codec == codec_zstd && len(out) >= len(payload) {
		t.Errorf("frame_compress shipped inflated zstd payload (%d -> %d)", len(payload), len(out))
	}
}

func TestCodecNoneIsIdentity(t *testing.T) {
	payload := []byte("anything")
	codec, out, err := frame_compress(payload, codec_none)
	if err != nil {
		t.Fatalf("frame_compress: %v", err)
	}
	if codec != codec_none {
		t.Errorf("codec_none input returned codec=%d", codec)
	}
	if !bytes.Equal(out, payload) {
		t.Errorf("codec_none mutated payload")
	}
	round, err := frame_decompress(payload, codec_none)
	if err != nil {
		t.Fatalf("frame_decompress: %v", err)
	}
	if !bytes.Equal(round, payload) {
		t.Errorf("codec_none decompress mismatch")
	}
}

func TestCodecUnknownRejected(t *testing.T) {
	_, err := frame_decompress([]byte("x"), 99)
	if err == nil {
		t.Fatal("frame_decompress accepted unknown codec")
	}
}

// --- Canonical CBOR + claim signable -----------------------------------

func TestClaimSignableDeterministic(t *testing.T) {
	// Two runs of claim_signable with the same inputs MUST yield
	// byte-identical output (canonical CBOR with SortBytewiseLexical).
	challenge := bytes.Repeat([]byte{0x42}, challenge_size_v2)
	entity := test_entity_id('s')
	out1, err := claim_signable(challenge, entity)
	if err != nil {
		t.Fatalf("claim_signable 1: %v", err)
	}
	out2, err := claim_signable(challenge, entity)
	if err != nil {
		t.Fatalf("claim_signable 2: %v", err)
	}
	if !bytes.Equal(out1, out2) {
		t.Fatalf("claim_signable non-deterministic: %x vs %x", out1, out2)
	}
}

func TestClaimSignableDomainSeparator(t *testing.T) {
	// The canonical encoding MUST contain the literal "mochi/2/claim"
	// domain separator string. Without it, a signature from this
	// schema could be replayed against a different signed payload that
	// happened to share the same field shape.
	challenge := bytes.Repeat([]byte{0x42}, challenge_size_v2)
	entity := test_entity_id('d')
	out, err := claim_signable(challenge, entity)
	if err != nil {
		t.Fatalf("claim_signable: %v", err)
	}
	if !bytes.Contains(out, []byte("mochi/2/claim")) {
		t.Errorf("claim signable missing domain separator: %x", out)
	}
}

// --- Challenge rejection -----------------------------------------------

func TestRejectChallengeWrongLength(t *testing.T) {
	if err := frame_reject_challenge(nil); err == nil {
		t.Error("frame_reject_challenge accepted nil")
	}
	if err := frame_reject_challenge(make([]byte, 16)); err == nil {
		t.Error("frame_reject_challenge accepted 16-byte challenge")
	}
	if err := frame_reject_challenge(make([]byte, 64)); err == nil {
		t.Error("frame_reject_challenge accepted 64-byte challenge")
	}
}

func TestRejectChallengeAllZero(t *testing.T) {
	if err := frame_reject_challenge(make([]byte, challenge_size_v2)); err == nil {
		t.Error("frame_reject_challenge accepted all-zero challenge")
	}
}

func TestRejectChallengeValid(t *testing.T) {
	b := make([]byte, challenge_size_v2)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	if err := frame_reject_challenge(b); err != nil {
		t.Errorf("frame_reject_challenge rejected valid challenge: %v", err)
	}
}

// --- Capability intersection -------------------------------------------

func TestCodecIntersectKeepsZstdBaseline(t *testing.T) {
	// zstd is the v2 baseline — must always appear in the result
	// regardless of advertisement.
	got := codec_intersect([]string{}, []string{})
	if !contains_string(got, "zstd") {
		t.Errorf("codec_intersect dropped zstd baseline: %v", got)
	}
}

func TestCodecIntersectIntersects(t *testing.T) {
	got := codec_intersect([]string{"zstd", "snappy"}, []string{"zstd", "gzip"})
	// Only zstd is in both; snappy and gzip drop. Plus zstd baseline.
	if !contains_string(got, "zstd") {
		t.Errorf("codec_intersect missing zstd: %v", got)
	}
	if contains_string(got, "snappy") {
		t.Errorf("codec_intersect kept sender-only codec: %v", got)
	}
	if contains_string(got, "gzip") {
		t.Errorf("codec_intersect kept receiver-only codec: %v", got)
	}
}

func TestFeaturesIntersectStrict(t *testing.T) {
	// Features have no baseline — empty in / empty out.
	got := features_intersect([]string{"a", "b"}, []string{"b", "c"})
	if !contains_string(got, "b") {
		t.Errorf("features_intersect dropped common element: %v", got)
	}
	if contains_string(got, "a") || contains_string(got, "c") {
		t.Errorf("features_intersect kept non-common elements: %v", got)
	}
	if len(features_intersect(nil, nil)) != 0 {
		t.Errorf("features_intersect on empty input should be empty")
	}
}

// --- Priority mapping --------------------------------------------------

func TestFramePriorityForMapsQueueTiers(t *testing.T) {
	cases := []struct {
		queue int
		want  byte
	}{
		{priority_control, frame_priority_control},
		{priority_replay, frame_priority_control},
		{priority_interactive, frame_priority_interactive},
		{priority_bulk, frame_priority_bulk},
		{0, frame_priority_interactive}, // default
	}
	for _, c := range cases {
		if got := frame_priority_for(c.queue); got != c.want {
			t.Errorf("frame_priority_for(%d): got %d, want %d", c.queue, got, c.want)
		}
	}
}

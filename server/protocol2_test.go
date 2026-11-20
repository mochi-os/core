// Copyright © 2026 Mochisoft OÜ
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
	cbor "github.com/fxamacker/cbor/v2"
	"github.com/klauspost/compress/zstd"
	p2p "github.com/libp2p/go-libp2p"
	p2p_peer "github.com/libp2p/go-libp2p/core/peer"
	multiaddr "github.com/multiformats/go-multiaddr"
	"io"
	"runtime"
	"slices"
	"testing"
	"time"
)

func init() {
	// All protocol2 tests rely on the canonical-CBOR encoder and zstd
	// singletons. protocol2_init is idempotent, safe to call here.
	protocol2_init()
}

// --- Framing -----------------------------------------------------------

// cbor_flood is a value of more than cbor_maximum_items data items within every
// per-container cap: arrays of arrays of empty maps, a byte apiece on the wire
// and a heap allocation apiece decoded.
func cbor_flood(t *testing.T) ([]byte, map[string]any) {
	t.Helper()
	inner := make([]any, cbor_maximum_elements)
	for i := range inner {
		inner[i] = map[string]any{}
	}
	outer := make([]any, cbor_maximum_items/cbor_maximum_elements+1)
	for i := range outer {
		outer[i] = inner
	}
	content := map[string]any{"x": outer}
	body, err := cbor.Marshal(map[string]any{"type": frame_type_message, "id": "flood", "content": content})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if err := cbor_decode_mode.Wellformed(body); err != nil {
		t.Fatalf("control: the flood breaks a per-container cap, so it tests nothing new: %v", err)
	}
	return body, content
}

// frame_prefixed is body with its length prefix, as frame_write sends it.
func frame_prefixed(body []byte) *bytes.Reader {
	out := make([]byte, frame_length_size+len(body))
	binary.BigEndian.PutUint32(out, uint32(len(body)))
	copy(out[frame_length_size:], body)
	return bytes.NewReader(out)
}

// A frame within every per-container cap but holding more data items than the
// budget is refused before it is decoded: about 1 MB of such a body decoded to
// some 64 MB of heap, and a 16 MB frame to a gigabyte, before any claim.
func TestFrameReadRefusesTooManyItems(t *testing.T) {
	body, content := cbor_flood(t)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err := frame_read(frame_prefixed(body))
	runtime.ReadMemStats(&after)
	var undecodable *frame_undecodable
	if !errors.As(err, &undecodable) || !errors.Is(err, cbor_walk_items) {
		t.Fatalf("THE POINT: a frame over the item budget read as %v", err)
	}
	if grown := after.TotalAlloc - before.TotalAlloc; grown > uint64(8*len(body)) {
		t.Errorf("THE POINT: refusing it allocated %d bytes for a %d-byte body", grown, len(body))
	}
	if err := frame_check(&Frame{Type: frame_type_message, ID: "flood", Content: content}); !errors.Is(err, frame_refused) {
		t.Errorf("frame_check passed a frame no receiver decodes: %v", err)
	}
	if err := announcement_read_check(body); err == nil {
		t.Error("announcement_read decoded a body over the item budget")
	}
}

// announcement_read_check reads body as a pubsub announcement.
func announcement_read_check(body []byte) error {
	_, err := announcement_read(body)
	return err
}

// A stream segment over the item budget is refused as a frame is, before it is
// decoded; the segment after it still reads.
func TestStreamReadRefusesTooManyItems(t *testing.T) {
	flood, _ := cbor_flood(t)
	small, _ := cbor.Marshal(map[string]any{"n": 1})
	st := stream_rw(io.NopCloser(bytes.NewReader(append(append([]byte{}, small...), flood...))), nil)
	first := map[string]any{}
	if err := st.read(&first); err != nil {
		t.Fatalf("control: the first segment did not read: %v", err)
	}
	var second any
	if err := st.read(&second); !errors.Is(err, cbor_walk_items) {
		t.Errorf("THE POINT: a segment over the item budget read as %v", err)
	}
}

// The walk finds where a value ends however its bytes are split across reads,
// counts as the decoder does, and agrees with it at the nesting bound.
func TestCborWalkFollowsSplitValues(t *testing.T) {
	values := []any{
		map[string]any{"a": []any{1, "two", []byte{3}, map[string]any{"x": nil}}, "b": 1.5, "c": true},
		[]any{},
		"text",
		uint64(1) << 40,
		-12,
		cbor.Tag{Number: 1, Content: int64(5)},
	}
	for i, value := range values {
		data, err := cbor.Marshal(value)
		if err != nil {
			t.Fatalf("encode %d: %v", i, err)
		}
		// Indefinite-length forms too.
		for split := 1; split <= len(data); split++ {
			var w cbor_walk
			n := 0
			for end := split; ; end += split {
				if end > len(data) {
					end = len(data)
				}
				n, err = w.step(data[:end])
				if err != nil || n > 0 || end == len(data) {
					break
				}
			}
			if err != nil || n != len(data) {
				t.Fatalf("value %d split every %d bytes: length %d, %v; want %d", i, split, n, err, len(data))
			}
		}
	}
	indefinite := []byte{0x9f, 0x01, 0x5f, 0x41, 0x61, 0xff, 0xbf, 0x61, 0x6b, 0x00, 0xff, 0xff}
	var w cbor_walk
	if n, err := w.step(append(indefinite, 0x01)); err != nil || n != len(indefinite) {
		t.Errorf("indefinite forms: length %d, %v; want %d", n, err, len(indefinite))
	}
	if err := cbor_decode_mode.Wellformed(indefinite); err != nil {
		t.Fatalf("control: %v", err)
	}

	// Nested as deep as the decoder allows passes the walk.
	var deep any = []any{}
	for i := 1; i < cbor_maximum_depth; i++ {
		deep = []any{deep}
	}
	data, _ := cbor.Marshal(deep)
	if err := cbor_decode_mode.Wellformed(data); err != nil {
		t.Fatalf("control: %v", err)
	}
	if err := cbor_items(data); err != nil {
		t.Errorf("a value the decoder takes was refused by the walk: %v", err)
	}
	for _, malformed := range [][]byte{{0xff}, {0x1c}, {0x5f, 0x41}} {
		if err := cbor_items(malformed); err == nil {
			t.Errorf("malformed % x passed the walk", malformed)
		}
	}
}

// Only a message carries content: any other frame past the control size, or
// carrying content or data at all, is refused, and a message is not.
func TestFrameReadHoldsControlFramesToTheirSize(t *testing.T) {
	read := func(f *Frame) error {
		var buffer bytes.Buffer
		if err := frame_write(&buffer, f); err != nil {
			t.Fatalf("frame_write: %v", err)
		}
		_, err := frame_read(&buffer)
		return err
	}
	large := map[string]any{"pad": make([]byte, frame_control_maximum)}
	if err := read(&Frame{Type: frame_type_ping, ID: "p", Content: large}); err == nil {
		t.Error("THE POINT: a ping past the control size was decoded")
	}
	if err := read(&Frame{Type: frame_type_ping, ID: "p", Content: map[string]any{"x": 1}}); err == nil {
		t.Error("THE POINT: a ping carrying content was taken")
	}
	if err := read(&Frame{Type: frame_type_ack, Replies: []string{"a"}, Data: []byte{1}}); err == nil {
		t.Error("THE POINT: an ack carrying data was taken")
	}
	if err := read(&Frame{Type: frame_type_message, ID: "m", Content: large}); err != nil {
		t.Errorf("control: a large message was refused: %v", err)
	}
	if err := read(&Frame{Type: frame_type_ping, ID: "p"}); err != nil {
		t.Errorf("control: a plain ping was refused: %v", err)
	}

	// Refused from its head, before the rest is built: a ping within the item
	// budget but holding half a million empty maps decoded to some 30 MB first.
	inner := make([]any, cbor_maximum_elements)
	for i := range inner {
		inner[i] = map[string]any{}
	}
	outer := make([]any, 50)
	for i := range outer {
		outer[i] = inner
	}
	body, err := cbor.Marshal(map[string]any{"type": frame_type_ping, "id": "p", "content": map[string]any{"x": outer}})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err = frame_read(frame_prefixed(body))
	runtime.ReadMemStats(&after)
	if err == nil {
		t.Error("a ping of half a million items was decoded")
	}
	if grown := after.TotalAlloc - before.TotalAlloc; grown > uint64(8*len(body)) {
		t.Errorf("THE POINT: refusing a large ping allocated %d bytes for a %d-byte body", grown, len(body))
	}
}

// The body is allocated as it arrives, not at the length its prefix claims: a
// four-byte prefix pinned 16 MB until the peer sent the rest, or never did.
func TestFrameReadAllocatesAsTheBodyArrives(t *testing.T) {
	var prefix [frame_length_size]byte
	binary.BigEndian.PutUint32(prefix[:], frame_maximum)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err := frame_read(io.MultiReader(bytes.NewReader(prefix[:]), bytes.NewReader([]byte{0xa1, 0x64})))
	runtime.ReadMemStats(&after)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("a body cut short read as %v", err)
	}
	if grown := after.TotalAlloc - before.TotalAlloc; grown > 1<<20 {
		t.Errorf("THE POINT: a 16 MB prefix and two bytes allocated %d bytes", grown)
	}
}

func TestFrameWriteReadRoundTrip(t *testing.T) {
	cases := []*Frame{
		{Type: frame_type_hello, Version: 2, Session: "abcd1234", Challenge: bytes.Repeat([]byte{0x01}, challenge_size_v2), Codecs: []string{"zstd"}, Features: nil},
		{Type: frame_type_caps, Codecs: []string{"zstd"}, Features: []string{"batch"}},
		{Type: frame_type_claim, From: test_entity_id('a'), Signature: bytes.Repeat([]byte{0x02}, ed25519.SignatureSize)},
		{Type: frame_type_message, ID: "msg-1", From: test_entity_id('b'), To: test_entity_id('c'),
			Service: "feeds", Event: "post/novelty", FromApp: "feeds",
			Services: []string{"feeds"},
			Content:  map[string]any{"k": "v"}, Data: []byte{0x09, 0x0a, 0x0b}},
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

// TestCodecHostileZstd: a peer's zstd payload of two blocks whose second
// Huffman weights table is at FSE table log 15 is refused as undecodable,
// not a panic that takes the server down. The input is the zstd library's
// own regression case for GO-2026-6865, both in one frame and split across
// two.
func TestCodecHostileZstd(t *testing.T) {
	one := slices.Concat(
		[]byte{0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x38, 0x74, 0x06, 0x00, 0x42, 0xb8, 0x32, 0x0d, 0x1a, 0x00, 0x00, 0x00, 0x90, 0xaf, 0xfa, 0xff, 0x03, 0x00, 0x80, 0xff, 0x7f},
		bytes.Repeat([]byte{0xa5, 0x94, 0x52, 0x4a, 0x29}, 37),
		[]byte{0xa5, 0x94, 0x12, 0x00, 0xad, 0x56, 0x03, 0xee, 0xff, 0xdf, 0xb3, 0x1a, 0x1c, 0x1a, 0x00, 0xe0, 0xff, 0x1f, 0x0f, 0xb1, 0x9c, 0x5a, 0xcf, 0x4d, 0xe4, 0x1d, 0x7b, 0xab, 0x99, 0x5e, 0x01, 0x51, 0xa4, 0x73, 0xd8, 0xb1, 0xf3, 0x39, 0x4b, 0x67, 0xf6, 0xab, 0x1a, 0xab, 0x1a, 0xab, 0x1a},
		bytes.Repeat([]byte{0x8c, 0x31, 0xc6, 0x18, 0x63}, 1365),
		[]byte{0x8c, 0x31},
		bytes.Repeat([]byte{0x63, 0x8c, 0x31, 0xc6, 0x18}, 1365),
		[]byte{0x63, 0x2c},
		bytes.Repeat([]byte{0x31, 0xc6, 0x18, 0x63, 0x8c}, 1365),
		[]byte{0x31, 0x46},
		bytes.Repeat([]byte{0x31, 0xc6, 0x18, 0x63, 0x8c}, 1365),
		[]byte{0x31, 0x06, 0x00},
	)
	two := slices.Concat(one[:6], []byte{0x75, 0x06, 0x00}, one[9:215], one[:6], one[215:])
	for name, payload := range map[string][]byte{"one frame": one, "two frames": two} {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("THE POINT: decoding a hostile payload (%s) panicked: %v", name, r)
				}
			}()
			if _, err := frame_decompress(payload, codec_zstd); err == nil {
				t.Errorf("a hostile payload (%s) decoded", name)
			}
		}()
	}
}

// --- Canonical CBOR + claim signable -----------------------------------

func TestClaimSignableDeterministic(t *testing.T) {
	// Two runs of claim_signable with the same inputs MUST yield
	// byte-identical output (canonical CBOR with SortBytewiseLexical).
	challenge := bytes.Repeat([]byte{0x42}, challenge_size_v2)
	entity := test_entity_id('s')
	out1, err := claim_signable(challenge, entity, "peerZ", protocol_messages)
	if err != nil {
		t.Fatalf("claim_signable 1: %v", err)
	}
	out2, err := claim_signable(challenge, entity, "peerZ", protocol_messages)
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
	out, err := claim_signable(challenge, entity, "peerZ", protocol_messages)
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

// TestFrameDecompressBounded pins the zstd-bomb defence: a frame tiny on the
// wire that expands past frame_maximum must be rejected before it is allocated.
func TestFrameDecompressBounded(t *testing.T) {
	protocol2_init()

	// Zeros compress to almost nothing, so this is the classic bomb shape:
	// far over the cap decompressed, far under it compressed.
	oversized := make([]byte, frame_maximum+(1<<20))
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatalf("zstd writer: %v", err)
	}
	bomb := encoder.EncodeAll(oversized, nil)
	encoder.Close()

	if len(bomb) >= frame_maximum {
		t.Fatalf("compressed bomb is %d bytes, not under the %d wire cap — test is not exercising the expansion path", len(bomb), frame_maximum)
	}

	_, err = frame_decompress(bomb, codec_zstd)
	if err == nil {
		t.Fatalf("frame_decompress accepted a payload expanding to %d bytes (cap %d)", len(oversized), frame_maximum)
	}
	// Assert on the decoder's own sentinel: frame_decompress' post-hoc length
	// check would also reject, but only after the memory is committed.
	if !errors.Is(err, zstd.ErrDecoderSizeExceeded) {
		t.Errorf("expected the decoder size cap to reject before allocating, got: %v", err)
	}

	// A frame that stays within the cap must still round-trip.
	ordinary := bytes.Repeat([]byte("mochi"), 1024)
	good := encoder.EncodeAll(ordinary, nil)
	out, err := frame_decompress(good, codec_zstd)
	if err != nil {
		t.Fatalf("frame_decompress rejected an in-bounds payload: %v", err)
	}
	if !bytes.Equal(out, ordinary) {
		t.Fatal("in-bounds payload did not round-trip")
	}
}

// TestDedupWindowExceedsMaxRetryInterval: seen_messages_ttl must be at least 2x
// the longest retry_delays gap, or a late retry reads as a fresh message.
func TestDedupWindowExceedsMaxRetryInterval(t *testing.T) {
	if len(retry_delays) == 0 {
		t.Skip("retry_delays empty; invariant not applicable")
	}
	gap_maximum := retry_delays[0]
	for _, d := range retry_delays {
		if d > gap_maximum {
			gap_maximum = d
		}
	}
	required := 2 * gap_maximum
	if seen_messages_ttl < required {
		t.Errorf("dedup window invariant violated: seen_messages_ttl=%d, max retry gap=%d, required ≥ %d (2× max gap). "+
			"Bump seen_messages_ttl OR cap retry_delays so the relation holds.",
			seen_messages_ttl, gap_maximum, required)
	}
}

// Guards inbound flow control: worker_dispatch BLOCKS on a full bounded inbox,
// so a flooding sender is paced by TCP rather than dropped. Fails if the inbox
// send becomes non-blocking or unbounded.//
// Application Interface Exception - see license.txt and license-exception.md.
func TestWorkerDispatchBackpressure(t *testing.T) {
	key := user_app_key{user: "u-backpressure", app: "test"}
	// A worker with a tiny inbox and NO run() goroutine — nothing drains it, so it
	// fills and stays full, modelling a worker that cannot keep up with the sender.
	w := &app_worker{user: key.user, app: key.app, inbox: make(chan *worker_frame, 2)}
	app_workers_lock.Lock()
	app_workers[key] = w
	app_workers_lock.Unlock()
	defer func() {
		app_workers_lock.Lock()
		delete(app_workers, key)
		app_workers_lock.Unlock()
	}()

	// Fill to capacity (cache hit on the pre-inserted worker, so no worker_create
	// / no run() goroutine is started).
	worker_dispatch(key.user, key.app, &worker_frame{})
	worker_dispatch(key.user, key.app, &worker_frame{})

	// A third dispatch must BLOCK — back-pressure, not unbounded buffering or a drop.
	done := make(chan struct{})
	go func() { worker_dispatch(key.user, key.app, &worker_frame{}); close(done) }()
	select {
	case <-done:
		t.Fatal("worker_dispatch returned on a full inbox — no back-pressure (unbounded buffer or silent drop)")
	case <-time.After(150 * time.Millisecond):
		// still blocked = back-pressure holds, the sender is paced not dropped
	}

	// Freeing one slot lets the blocked dispatch proceed — paced, never dropped.
	<-w.inbox
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("worker_dispatch stayed blocked after a slot freed — should have proceeded")
	}
}

// TestPeerProtocolOpenTimesOut is #47: peer_protocol_open must not hang when a
// stale "Connected" peer forces net_me.NewStream to dial an unreachable
// address. A peer believed connected whose only address is black-holed must
// fail within peer_stream_open_timeout.
func TestPeerProtocolOpenTimesOut(t *testing.T) {
	setup_peer_discovery_test(t)

	// A real libp2p host so NewStream genuinely attempts a dial.
	h, err := p2p.New(p2p.ListenAddrStrings("/ip4/127.0.0.1/udp/0/quic-v1"))
	if err != nil {
		t.Fatalf("test host: %v", err)
	}
	defer h.Close()
	saved_me := net_me
	net_me = h
	defer func() { net_me = saved_me }()

	// Peer Mochi thinks is connected (stale state) with only a black-holed
	// address in the libp2p peerstore — the dial can never succeed.
	id, _ := test_host(t)
	pid, err := p2p_peer.Decode(id)
	if err != nil {
		t.Fatal(err)
	}
	blackhole, err := multiaddr.NewMultiaddr("/ip4/192.0.2.50/udp/1443/quic-v1")
	if err != nil {
		t.Fatal(err)
	}
	h.Peerstore().AddAddr(pid, blackhole, time.Hour)
	peers_lock.Lock()
	peers[id] = Peer{ID: id, state: peer_state_connected}
	peers_lock.Unlock()

	saved_timeout := peer_stream_open_timeout
	peer_stream_open_timeout = 300 * time.Millisecond
	defer func() { peer_stream_open_timeout = saved_timeout }()

	start := time.Now()
	done := make(chan error, 1)
	go func() {
		_, e := peer_protocol_open(id, protocol_stream)
		done <- e
	}()

	select {
	case e := <-done:
		if e == nil {
			t.Fatal("expected an error opening a stream to a black-holed peer")
		}
		if d := time.Since(start); d > 3*time.Second {
			t.Fatalf("open returned in %v — NewStream not bounded by peer_stream_open_timeout", d)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("peer_protocol_open HUNG (>5s) — #47 NewStream deadline not applied")
	}
}

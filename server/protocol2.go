// Mochi server: Protocol 2 — framing, codec, and canonical-CBOR helpers.
//
// /mochi/2/messages and /mochi/2/stream share the wire framing defined
// here. See claude/plans/protocol2.md for the full design.
//
// Frame layout on the wire:
//
//   +--------+--------+--------+--------+
//   |     Length (uint32, big-endian)   |   4 bytes
//   +--------+--------+--------+--------+
//   |                                   |
//   |   CBOR envelope (Length bytes)    |   ≤ frame_maximum
//   |                                   |
//   +-----------------------------------+
//
// Both protocols loop on the same stream — frames bracket the message;
// the stream stays open across many frames.
//
// Copyright Alistair Cunningham 2026

package main

import (
	"bytes"
	"crypto/ed25519"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	cbor "github.com/fxamacker/cbor/v2"
	zstd "github.com/klauspost/compress/zstd"
)

const (
	// Protocol IDs registered with libp2p.
	protocol_messages = "/mochi/2/messages"
	protocol_stream   = "/mochi/2/stream"

	// Wire / framing limits.
	frame_maximum         = 16 * 1024 * 1024 // 16 MB defensive cap
	frame_length_size     = 4                // big-endian uint32 prefix
	frame_diagnostic_size = 256              // hex log on CBOR decode failure
	challenge_size_v2     = 32               // hello.Challenge length
	max_id_length         = 64               // max Frame.ID / message id length

	// Codec byte values (Frame.Codec).
	codec_none = 0
	codec_zstd = 1

	// Compression threshold — payloads smaller than this skip zstd.
	codec_threshold = 512

	// Priority tiers carried on Frame.Priority. Higher is more urgent.
	frame_priority_control     = 40
	frame_priority_interactive = 20
	frame_priority_bulk        = 10

	// Domain separators for the per-stream claim signature.
	claim_domain    = "mochi/2/claim"
	claim_version_2 = 2
)

// Type vocabulary for Frame.Type. Receivers MUST close the stream on an
// unknown Type per the framing-errors policy.
const (
	frame_type_hello   = "hello"   // shared
	frame_type_caps    = "caps"    // shared
	frame_type_claim   = "claim"   // shared
	frame_type_ack     = "ack"     // shared
	frame_type_fail    = "fail"    // shared
	frame_type_message = "message" // /mochi/2/messages only
	frame_type_ping    = "ping"    // /mochi/2/messages only
	frame_type_pong    = "pong"    // /mochi/2/messages only
	frame_type_bye     = "bye"     // /mochi/2/messages only
	frame_type_open    = "open"    // /mochi/2/stream only
)

// Failure reason vocabulary carried on Frame.Reason for fail frames.
// See claude/plans/protocol2.md for resolver semantics.
const (
	fail_unsupported       = "unsupported"
	fail_unknown_user      = "unknown_user"
	fail_signature_invalid = "signature_invalid"
	fail_expired           = "expired"
	fail_rate_limited      = "rate_limited"
	fail_buffer_full       = "buffer_full"
	fail_handler_panic     = "handler_panic"
	fail_dedup             = "dedup"
	fail_transient         = "transient"
	fail_unclaimed         = "unclaimed"
)

// Frame is the on-wire envelope shared by /mochi/2/messages and
// /mochi/2/stream. The routing field names (`from`, `to`, etc.) keep
// their original wire keys; new fields use single-word names per
// project convention.
type Frame struct {
	Type      string   `cbor:"type"`
	ID        string   `cbor:"id,omitempty"`
	Replies   []string `cbor:"replies,omitempty"`
	From      string   `cbor:"from,omitempty"`
	To        string   `cbor:"to,omitempty"`
	Service   string   `cbor:"service,omitempty"`
	Event     string   `cbor:"event,omitempty"`
	FromApp   string   `cbor:"from-app,omitempty"`
	Services  []string `cbor:"from-services,omitempty"`
	Signature []byte   `cbor:"signature,omitempty"` // claim frames + signed pubsub announcements
	Reason    string   `cbor:"reason,omitempty"`
	Expires   string   `cbor:"expires,omitempty"` // pubsub message frames: absolute Unix seconds (decimal string) after which receivers reject the announcement

	// New per-message fields.
	Codec    byte           `cbor:"codec,omitempty"`
	Priority byte           `cbor:"priority,omitempty"`
	Content  map[string]any `cbor:"content,omitempty"` // message frames only — /mochi/2/stream ships content as the first post-ack CBOR segment instead
	Data     []byte         `cbor:"data,omitempty"`    // message frames only — packed post-content CBOR segments (handler reads via e.segment)

	// Handshake-only fields.
	Challenge []byte   `cbor:"challenge,omitempty"`
	Version   int      `cbor:"version,omitempty"`
	Session   string   `cbor:"session,omitempty"`
	Codecs    []string `cbor:"codecs,omitempty"`
	Features  []string `cbor:"features,omitempty"`
}

// frame_codec_supported reports whether codec b is one this build can
// decode. Used for capability validation; sender MUST NOT set a Codec
// the receiver didn't advertise, so this only catches buggy/hostile
// senders.
func frame_codec_supported(b byte) bool {
	switch b {
	case codec_none, codec_zstd:
		return true
	}
	return false
}

// frame_type_known reports whether t is a Type this build handles. An
// unknown Type is a protocol violation; the receiver closes the stream.
func frame_type_known(t string) bool {
	switch t {
	case frame_type_hello, frame_type_caps, frame_type_claim,
		frame_type_ack, frame_type_fail,
		frame_type_message, frame_type_ping, frame_type_pong, frame_type_bye,
		frame_type_open:
		return true
	}
	return false
}

// CBOR encoding: a single permissive decoder is shared across the
// process. The canonical encoder for signed payloads lives separately
// (canonical_encoder) so SortBytewiseLexical is applied to signed bytes
// only, not to every frame on the wire.
var (
	canonical_encoder cbor.EncMode
	zstd_encoder      *zstd.Encoder
	zstd_decoder      *zstd.Decoder
	codec_init        sync.Once
)

// protocol2_init wires the canonical CBOR encoder and zstd singletons.
// Called from main() ordering before net_start. Idempotent.
func protocol2_init() {
	codec_init.Do(func() {
		var err error
		canonical_encoder, err = cbor.EncOptions{
			Sort: cbor.SortBytewiseLexical,
		}.EncMode()
		if err != nil {
			panic(fmt.Sprintf("protocol2: canonical encoder init failed: %v", err))
		}
		zstd_encoder, err = zstd.NewWriter(nil,
			zstd.WithEncoderLevel(zstd.SpeedDefault))
		if err != nil {
			panic(fmt.Sprintf("protocol2: zstd encoder init failed: %v", err))
		}
		zstd_decoder, err = zstd.NewReader(nil,
			zstd.WithDecoderConcurrency(0))
		if err != nil {
			panic(fmt.Sprintf("protocol2: zstd decoder init failed: %v", err))
		}
	})
}

// frame_read decodes one Frame from r. Returns io.EOF on clean stream
// close before the first byte of a new frame. Any other error means the
// caller MUST close the stream — frame boundaries are ambiguous after
// a partial read or decode failure.
func frame_read(r io.Reader) (*Frame, error) {
	var lenbuf [frame_length_size]byte
	if _, err := io.ReadFull(r, lenbuf[:]); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(lenbuf[:])
	if length == 0 {
		return nil, fmt.Errorf("frame: zero length")
	}
	if length > frame_maximum {
		return nil, fmt.Errorf("frame: oversized length %d > %d", length, frame_maximum)
	}
	body := make([]byte, length)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, fmt.Errorf("frame: truncated body (wanted %d): %w", length, err)
	}
	var f Frame
	if err := cbor_decode_mode.Unmarshal(body, &f); err != nil {
		head := body
		if len(head) > frame_diagnostic_size {
			head = head[:frame_diagnostic_size]
		}
		return nil, fmt.Errorf("frame: cbor decode failed (%d bytes, first %d hex=%x): %w",
			length, len(head), head, err)
	}
	if f.Type == "" {
		return nil, fmt.Errorf("frame: missing Type")
	}
	if !frame_type_known(f.Type) {
		return nil, fmt.Errorf("frame: unknown Type %q", f.Type)
	}
	return &f, nil
}

// frame_write encodes f and writes it to w as a length-prefixed CBOR
// frame. Buffers the encoded body so the prefix can carry the exact
// length; returns the buffer to the caller's stream in one Write so a
// concurrent writer on the same stream can't interleave.
func frame_write(w io.Writer, f *Frame) error {
	body, err := cbor.Marshal(f)
	if err != nil {
		return fmt.Errorf("frame: cbor encode failed: %w", err)
	}
	if len(body) > frame_maximum {
		return fmt.Errorf("frame: outbound oversized %d > %d", len(body), frame_maximum)
	}
	out := make([]byte, frame_length_size+len(body))
	binary.BigEndian.PutUint32(out[:frame_length_size], uint32(len(body)))
	copy(out[frame_length_size:], body)
	if _, err := w.Write(out); err != nil {
		return fmt.Errorf("frame: write failed: %w", err)
	}
	return nil
}

// frame_compress applies the chosen codec to payload. Returns the
// codec byte actually used (may downgrade to codec_none on tiny payloads
// or if zstd would inflate). Caller is responsible for ensuring the
// receiver advertised the codec in its hello.Codecs.
func frame_compress(payload []byte, want byte) (byte, []byte, error) {
	if want == codec_none || len(payload) < codec_threshold {
		return codec_none, payload, nil
	}
	switch want {
	case codec_zstd:
		out := zstd_encoder.EncodeAll(payload, make([]byte, 0, len(payload)/2))
		// Don't ship a "compressed" body that grew. The 512-byte
		// threshold catches most of these, but small structured CBOR
		// can sometimes inflate even at 512+.
		if len(out) >= len(payload) {
			return codec_none, payload, nil
		}
		return codec_zstd, out, nil
	}
	return 0, nil, fmt.Errorf("frame: unsupported codec %d", want)
}

// frame_decompress reverses frame_compress. Unknown codec → error;
// caller replies fail{Reason=unsupported}.
func frame_decompress(payload []byte, codec byte) ([]byte, error) {
	switch codec {
	case codec_none:
		return payload, nil
	case codec_zstd:
		out, err := zstd_decoder.DecodeAll(payload, make([]byte, 0, len(payload)*2))
		if err != nil {
			return nil, fmt.Errorf("frame: zstd decode failed: %w", err)
		}
		return out, nil
	}
	return nil, fmt.Errorf("frame: unsupported codec %d", codec)
}

// claim_signable returns the canonical CBOR bytes signed by an entity
// for a per-stream claim. Schema is fixed: {v, stream, entity} sorted
// bytewise-lexical per RFC 8949 §4.2. Any change to the schema MUST
// bump claim_domain.
//
// Map keys are deliberately one-letter — they're never decoded by
// anyone (only the signer and verifier ever materialise the bytes);
// keeping them short shaves a few bytes off every claim signature
// input. The canonical encoder sorts them bytewise.
func claim_signable(challenge []byte, entity string) ([]byte, error) {
	payload := map[string]any{
		"v":      claim_domain,
		"stream": challenge,
		"entity": entity,
	}
	out, err := canonical_encoder.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("claim: canonical encode failed: %w", err)
	}
	return out, nil
}

// claim_sign produces the signature for the given (challenge, entity)
// pair using the entity's private key. Returns nil if the entity isn't
// local or its key can't be loaded — caller logs and skips the claim.
func claim_sign(entity string, challenge []byte) []byte {
	if entity == "" || len(challenge) != challenge_size_v2 {
		return nil
	}
	signable, err := claim_signable(challenge, entity)
	if err != nil {
		warn("claim_sign canonical encode failed for %q: %v", entity, err)
		return nil
	}
	db := db_open("db/users.db")
	var e Entity
	if !db.scan(&e, "select private from entities where id=?", entity) {
		info("claim_sign entity %q not found", entity)
		return nil
	}
	private := base58_decode(e.Private, "")
	if len(private) != ed25519.PrivateKeySize {
		warn("claim_sign entity %q invalid private key length %d", entity, len(private))
		return nil
	}
	return ed25519.Sign(private, signable)
}

// claim_verify checks an inbound claim. The entity ID IS the base58-
// encoded ed25519 public key — no directory lookup needed. Returns nil
// on success, descriptive error on failure (logged by caller).
func claim_verify(entity string, challenge, signature []byte) error {
	if entity == "" {
		return errors.New("claim: empty entity")
	}
	if len(challenge) != challenge_size_v2 {
		return fmt.Errorf("claim: invalid challenge length %d", len(challenge))
	}
	if len(signature) != ed25519.SignatureSize {
		return fmt.Errorf("claim: invalid signature length %d", len(signature))
	}
	public := base58_decode(entity, "")
	if len(public) != ed25519.PublicKeySize {
		return fmt.Errorf("claim: invalid entity pubkey length %d", len(public))
	}
	signable, err := claim_signable(challenge, entity)
	if err != nil {
		return fmt.Errorf("claim: canonical encode failed: %w", err)
	}
	if !ed25519.Verify(public, signable, signature) {
		return errors.New("claim: signature mismatch")
	}
	return nil
}

// frame_reject_challenge runs the rejection tests demanded by the
// hello-handshake spec: length must equal challenge_size_v2 and must
// not be all-zero. A buggy/hostile receiver sending either of these
// → sender treats it as a protocol negotiation failure.
func frame_reject_challenge(challenge []byte) error {
	if len(challenge) != challenge_size_v2 {
		return fmt.Errorf("hello: bad challenge length %d", len(challenge))
	}
	if bytes.Equal(challenge, make([]byte, challenge_size_v2)) {
		return errors.New("hello: zero challenge")
	}
	return nil
}

// frame_priority_for maps the queue's existing priority constant to the
// per-frame Priority byte. The bulk/interactive/control split mirrors
// the queue lane discipline so the receiver knows which tier the
// message belonged to (currently informational; future ordering work
// may use it).
func frame_priority_for(queue_priority int) byte {
	switch queue_priority {
	case priority_control, priority_replay:
		return frame_priority_control
	case priority_bulk:
		return frame_priority_bulk
	}
	return frame_priority_interactive
}

// codec_intersect returns the codecs the sender should consider after
// intersecting its supported list with the receiver's advertised list
// from hello. Order follows the receiver's preference. zstd is always
// in the result because both sides MUST decode it (the advertised list
// is for additional codecs; an empty intersection still yields zstd).
func codec_intersect(sender, receiver []string) []string {
	have := map[string]bool{}
	for _, c := range receiver {
		have[c] = true
	}
	out := []string{}
	for _, c := range sender {
		if have[c] {
			out = append(out, c)
		}
	}
	if !contains_string(out, "zstd") {
		out = append(out, "zstd")
	}
	return out
}

// features_intersect: same shape as codec_intersect but for capability
// flags. The intersection has no baseline — empty is the v2 default.
func features_intersect(a, b []string) []string {
	have := map[string]bool{}
	for _, f := range b {
		have[f] = true
	}
	out := []string{}
	for _, f := range a {
		if have[f] {
			out = append(out, f)
		}
	}
	return out
}

func contains_string(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

// --- Operator-tunable settings ----------------------------------------
//
// All /mochi/2 timing and buffering defaults live in the matching
// `*_default` constants above and in protocol2_sender.go /
// protocol2_worker.go. The functions below expose those defaults as
// ini-tunable getters under the [peer] section, so an operator can
// adjust them via mochi.conf without recompiling. Names match the
// plan's "peer.*" namespace; unset settings fall back to the
// compile-time default.

// peer_window returns the per-peer inflight cap. Local memory bound on
// the Sender.inflight map; wire-level back-pressure rides on libp2p.
func peer_window() int { return ini_int("peer", "window", sender_window_default) }

// peer_outbox returns the per-Sender outbox channel depth.
func peer_outbox() int { return ini_int("peer", "outbox", sender_outbox_default) }

// peer_inflight_timeout returns the per-message ack timeout (seconds).
// Stale inflight entries past this are queue_failed and retried.
func peer_inflight_timeout() int {
	return ini_int("peer", "timeout", sender_inflight_timeout)
}

// peer_ping_interval_seconds is the idle-period before a Sender emits
// a ping frame. Active streams (any inbound frame within the period)
// don't ping — the receive-side activity resets the timer.
func peer_ping_interval_seconds() int {
	return ini_int("peer", "ping_interval", int(sender_ping_interval.Seconds()))
}

// peer_ping_timeout_seconds is the pong wait. No pong within this
// window → stream treated as dead and standard cleanup runs.
func peer_ping_timeout_seconds() int {
	return ini_int("peer", "ping_timeout", sender_ping_timeout)
}

// peer_worker_idle_seconds is the no-activity window after which an
// idle (user, app) worker is reaped. Active workers stay alive.
func peer_worker_idle_seconds() int {
	return ini_int("peer", "worker_idle", worker_idle_default)
}

// peer_worker_inbox is the per-(user, app) channel depth. Smaller =
// back-pressure propagates into libp2p faster; larger = more memory
// and more head-of-line tolerance.
func peer_worker_inbox() int {
	return ini_int("peer", "worker_inbox", worker_inbox_default)
}

// peer_rate is the per-Sender outbound message rate cap in
// messages/second. Default 0 (unlimited) per claude/plans/protocol2.md
// Decision points — operators set this only after observing a runaway
// producer, since the inflight cap (peer_window) is the natural
// back-pressure mechanism.
func peer_rate() int { return ini_int("peer", "rate", 0) }

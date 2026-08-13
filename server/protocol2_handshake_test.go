// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Tests for protocol2_handshake.go — hello / caps / claim helpers, plus
// the surrounding authentication semantics: per-(stream, entity) claim
// signing, cross-stream replay rejection, multi-entity claims, and the
// entity-key rotation hook.
//
// Phase 3b per claude/plans/protocol2.md → Testing strategy.

package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"io"
	"strings"
	"testing"
)

// test_receiver stands in for the peer ID a claim is addressed to. In
// production this comes from the authenticated libp2p connection at the
// signer and from net_id at the verifier; the tests only need the two
// ends to agree, except where they deliberately disagree.
const test_receiver = "12D3KooWReceiverUnderTest"

// test_opener stands in for the peer ID that OPENED a stream — the one
// a responder proof is addressed to. Deliberately distinct from
// test_receiver so a test cannot pass by confusing the two directions.
const test_opener = "12D3KooWOpenerUnderTest"

// must_challenge returns a fresh well-formed challenge, failing the test
// if the entropy source does. Tests use it wherever production code
// would call hello_challenge().
func must_challenge(t *testing.T) []byte {
	t.Helper()
	c, err := hello_challenge()
	if err != nil {
		t.Fatalf("hello_challenge: %v", err)
	}
	return c
}

// new_entity_keys generates a fresh ed25519 keypair, base58-encodes the
// public key as the entity ID (matching entity_id() in entities.go),
// and inserts the row into the test users.db so claim_sign can find the
// private key. Returns the entity ID + the raw private key for tests
// that need to forge or replay-test signatures.
//
// Inserts the owning user row first if it isn't already present, since
// entities.user has a FK to users.uid.
func new_entity_keys(t *testing.T) (id string, private ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("ed25519.GenerateKey: %v", err)
	}
	id = base58_encode(pub)
	private_b58 := base58_encode(priv)

	const owner = "uid-handshake-test"
	users := db_open("db/users.db")
	users.exec(`insert or ignore into users (uid, username, role) values (?, ?, 'user')`,
		owner, owner)
	users.exec(`insert into entities (id, private, fingerprint, user, class, name, privacy, data) values
		(?, ?, ?, ?, 'person', 'tester', 'public', '')`,
		id, private_b58, fingerprint(id), owner)
	return id, priv
}

// --- hello -------------------------------------------------------------

func TestHelloRoundTrip(t *testing.T) {
	challenge, err := hello_challenge()
	if err != nil {
		t.Fatalf("hello_challenge: %v", err)
	}
	if len(challenge) != challenge_size_v2 {
		t.Fatalf("challenge length %d, want %d", len(challenge), challenge_size_v2)
	}

	var buf bytes.Buffer
	if err := hello_write(&buf, 2, "sess-1", challenge, []string{"zstd"}, []string{"flag-a"}); err != nil {
		t.Fatalf("hello_write: %v", err)
	}
	got, err := hello_read(&buf, 2)
	if err != nil {
		t.Fatalf("hello_read: %v", err)
	}
	if got.Type != frame_type_hello {
		t.Errorf("Type: %q", got.Type)
	}
	if got.Version != 2 {
		t.Errorf("Version: %d", got.Version)
	}
	if got.Session != "sess-1" {
		t.Errorf("Session: %q", got.Session)
	}
	if !bytes.Equal(got.Challenge, challenge) {
		t.Errorf("Challenge mismatch")
	}
	if !contains_string(got.Codecs, "zstd") {
		t.Errorf("Codecs: %v", got.Codecs)
	}
	if !contains_string(got.Features, "flag-a") {
		t.Errorf("Features: %v", got.Features)
	}
}

func TestHelloVersionMismatchRejected(t *testing.T) {
	var buf bytes.Buffer
	challenge, _ := hello_challenge()
	if err := hello_write(&buf, 3, "s", challenge, nil, nil); err != nil {
		t.Fatalf("hello_write: %v", err)
	}
	if _, err := hello_read(&buf, 2); err == nil {
		t.Fatal("hello_read accepted version mismatch")
	}
}

func TestHelloChallengeFresh(t *testing.T) {
	// Plan: "No reuse. One challenge per stream, never regenerated."
	// Two calls to hello_challenge MUST yield distinct values.
	a, err := hello_challenge()
	if err != nil {
		t.Fatalf("hello_challenge a: %v", err)
	}
	b, err := hello_challenge()
	if err != nil {
		t.Fatalf("hello_challenge b: %v", err)
	}
	if bytes.Equal(a, b) {
		t.Error("hello_challenge returned identical bytes on two calls")
	}
}

func TestHelloRejectsZeroChallenge(t *testing.T) {
	// Plan: sender MUST reject all-zero challenge from buggy/hostile
	// receiver.
	var buf bytes.Buffer
	if err := hello_write(&buf, 2, "s", make([]byte, challenge_size_v2), nil, nil); err != nil {
		t.Fatalf("hello_write: %v", err)
	}
	if _, err := hello_read(&buf, 2); err == nil {
		t.Fatal("hello_read accepted all-zero challenge")
	}
}

func TestHelloWrongChallengeLengthRejected(t *testing.T) {
	// Build a hello frame by hand with a 16-byte challenge.
	bad := &Frame{
		Type:      frame_type_hello,
		Version:   2,
		Session:   "s",
		Challenge: make([]byte, 16),
	}
	var buf bytes.Buffer
	if err := frame_write(&buf, bad); err != nil {
		t.Fatalf("frame_write: %v", err)
	}
	if _, err := hello_read(&buf, 2); err == nil {
		t.Fatal("hello_read accepted 16-byte challenge")
	}
}

// --- caps --------------------------------------------------------------

func TestCapsRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	challenge, err := hello_challenge()
	if err != nil {
		t.Fatalf("hello_challenge: %v", err)
	}
	if err := caps_write(&buf, []string{"zstd", "snappy"}, []string{"batch"}, challenge); err != nil {
		t.Fatalf("caps_write: %v", err)
	}
	got, err := caps_read(&buf)
	if err != nil {
		t.Fatalf("caps_read: %v", err)
	}
	if got.Type != frame_type_caps {
		t.Errorf("Type: %q", got.Type)
	}
	if !contains_string(got.Codecs, "snappy") {
		t.Errorf("Codecs: %v", got.Codecs)
	}
	if !contains_string(got.Features, "batch") {
		t.Errorf("Features: %v", got.Features)
	}
	// The opener's challenge must survive the round trip: the answering
	// side signs exactly these bytes to prove it holds the addressed
	// entity, so a caps frame that drops or mangles it silently disables
	// the proof rather than failing it.
	if !bytes.Equal(got.Challenge, challenge) {
		t.Errorf("Challenge: got %x want %x", got.Challenge, challenge)
	}
}

func TestCapsRejectsNonCapsFrame(t *testing.T) {
	// Receiver MUST close stream if first sender frame isn't caps.
	var buf bytes.Buffer
	if err := frame_write(&buf, &Frame{Type: frame_type_message, ID: "x", From: test_entity_id('a')}); err != nil {
		t.Fatalf("frame_write: %v", err)
	}
	if _, err := caps_read(&buf); err == nil {
		t.Fatal("caps_read accepted non-caps frame")
	}
}

// --- claim sign / verify -----------------------------------------------

func TestClaimSignVerifyRoundTrip(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()
	id, _ := new_entity_keys(t)

	challenge := make([]byte, challenge_size_v2)
	if _, err := rand.Read(challenge); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	sig := claim_sign(id, challenge, test_receiver, protocol_messages)
	if sig == nil {
		t.Fatal("claim_sign returned nil for valid entity")
	}
	if err := claim_verify(id, challenge, sig, test_receiver, protocol_messages); err != nil {
		t.Errorf("claim_verify rejected own signature: %v", err)
	}
}

func TestClaimSignMissingEntityReturnsNil(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()

	challenge := make([]byte, challenge_size_v2)
	rand.Read(challenge)
	// entity not in users.db
	if sig := claim_sign(test_entity_id('z'), challenge, test_receiver, protocol_messages); sig != nil {
		t.Error("claim_sign for unknown entity returned non-nil")
	}
}

func TestClaimSignBadChallengeReturnsNil(t *testing.T) {
	if sig := claim_sign(test_entity_id('a'), make([]byte, 16), test_receiver, protocol_messages); sig != nil {
		t.Error("claim_sign with wrong-length challenge returned non-nil")
	}
	if sig := claim_sign("", make([]byte, challenge_size_v2), test_receiver, protocol_messages); sig != nil {
		t.Error("claim_sign with empty entity returned non-nil")
	}
}

func TestClaimVerifyRejectsForgedSignature(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()
	id, _ := new_entity_keys(t)

	challenge := make([]byte, challenge_size_v2)
	rand.Read(challenge)

	// Random garbage of correct length.
	bogus := make([]byte, ed25519.SignatureSize)
	rand.Read(bogus)
	if err := claim_verify(id, challenge, bogus, test_receiver, protocol_messages); err == nil {
		t.Error("claim_verify accepted random garbage signature")
	}
}

func TestClaimVerifyRejectsCrossStreamReplay(t *testing.T) {
	// Plan: "A captured claim from stream A cannot be replayed on
	// stream B because the signed input binds the per-stream
	// challenge, which is fresh per stream."
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()
	id, _ := new_entity_keys(t)

	chA := make([]byte, challenge_size_v2)
	chB := make([]byte, challenge_size_v2)
	rand.Read(chA)
	rand.Read(chB)
	if bytes.Equal(chA, chB) {
		t.Fatal("test setup: challenges collided")
	}
	sigA := claim_sign(id, chA, test_receiver, protocol_messages)
	if sigA == nil {
		t.Fatal("claim_sign A failed")
	}
	// Same signature, different stream challenge → must fail.
	if err := claim_verify(id, chB, sigA, test_receiver, protocol_messages); err == nil {
		t.Error("claim_verify accepted replay from a different stream's challenge")
	}
}

func TestClaimVerifyRejectsCrossEntityReplay(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()
	idA, _ := new_entity_keys(t)
	idB, _ := new_entity_keys(t)
	if idA == idB {
		t.Fatal("setup: entity IDs collided")
	}

	challenge := make([]byte, challenge_size_v2)
	rand.Read(challenge)
	sigA := claim_sign(idA, challenge, test_receiver, protocol_messages)
	if sigA == nil {
		t.Fatal("claim_sign A failed")
	}
	if err := claim_verify(idB, challenge, sigA, test_receiver, protocol_messages); err == nil {
		t.Errorf("claim_verify accepted entity A's signature for entity B")
	}
}

// TestClaimVerifyRejectsRelayedClaim is the attack the receiver binding
// exists for, and the one the cross-stream replay test above cannot
// express: it uses two INDEPENDENTLY RANDOM challenges, so it never
// exercises a challenge the attacker chose.
//
// M opens a stream to victim V and captures V's fresh challenge. M then
// induces the key holder to open a stream to M, plays receiver, and hands
// back that same challenge. The signature it gets is genuine and covers
// exactly the bytes V is expecting, so with only {challenge, entity}
// signed, relaying it to V used to succeed — and M could then speak as
// the entity and poison the directory route for it.
func TestClaimVerifyRejectsRelayedClaim(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()
	id, _ := new_entity_keys(t)

	challenge := make([]byte, challenge_size_v2) // V's, captured by M
	rand.Read(challenge)

	const victim = "12D3KooWVictimPeer"
	const attacker = "12D3KooWAttackerPeer"

	// The key holder signs for the peer it is actually talking to: M.
	sig := claim_sign(id, challenge, attacker, protocol_messages)
	if sig == nil {
		t.Fatal("claim_sign failed")
	}
	// M relays it verbatim; V checks it against its own identity.
	if err := claim_verify(id, challenge, sig, victim, protocol_messages); err == nil {
		t.Error("claim_verify accepted a claim signed for a different receiver")
	}
	// Control: the same claim is still good on the stream it was made
	// for, so this is rejecting the relay rather than everything.
	if err := claim_verify(id, challenge, sig, attacker, protocol_messages); err != nil {
		t.Errorf("claim_verify rejected the claim on its own stream: %v", err)
	}
}

// TestClaimVerifyRejectsCrossProtocolReplay: the protocol is signed too,
// so a claim made on the messages protocol cannot be lifted onto the
// stream protocol even between the same two peers on the same challenge.
func TestClaimVerifyRejectsCrossProtocolReplay(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()
	id, _ := new_entity_keys(t)

	challenge := make([]byte, challenge_size_v2)
	rand.Read(challenge)

	sig := claim_sign(id, challenge, test_receiver, protocol_messages)
	if sig == nil {
		t.Fatal("claim_sign failed")
	}
	if err := claim_verify(id, challenge, sig, test_receiver, protocol_stream); err == nil {
		t.Error("claim_verify accepted a claim made for a different protocol")
	}
}

// TestClaimRequiresReceiverAndProtocol: both must be supplied. An empty
// value would silently collapse the binding back to challenge-only, so
// signing and verifying fail closed rather than proceeding.
func TestClaimRequiresReceiverAndProtocol(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()
	id, _ := new_entity_keys(t)

	challenge := make([]byte, challenge_size_v2)
	rand.Read(challenge)

	if sig := claim_sign(id, challenge, "", protocol_messages); sig != nil {
		t.Error("claim_sign produced a signature with no receiver")
	}
	if sig := claim_sign(id, challenge, test_receiver, ""); sig != nil {
		t.Error("claim_sign produced a signature with no protocol")
	}
	sig := claim_sign(id, challenge, test_receiver, protocol_messages)
	if sig == nil {
		t.Fatal("claim_sign failed")
	}
	if err := claim_verify(id, challenge, sig, "", protocol_messages); err == nil {
		t.Error("claim_verify accepted an empty receiver")
	}
	if err := claim_verify(id, challenge, sig, test_receiver, ""); err == nil {
		t.Error("claim_verify accepted an empty protocol")
	}
}

func TestClaimVerifyShortInputs(t *testing.T) {
	if err := claim_verify("", make([]byte, challenge_size_v2), make([]byte, ed25519.SignatureSize), test_receiver, protocol_messages); err == nil {
		t.Error("claim_verify accepted empty entity")
	}
	if err := claim_verify(test_entity_id('a'), make([]byte, 16), make([]byte, ed25519.SignatureSize), test_receiver, protocol_messages); err == nil {
		t.Error("claim_verify accepted wrong-length challenge")
	}
	if err := claim_verify(test_entity_id('a'), make([]byte, challenge_size_v2), make([]byte, 32), test_receiver, protocol_messages); err == nil {
		t.Error("claim_verify accepted wrong-length signature")
	}
}

// --- claim_write framing ----------------------------------------------

func TestClaimWriteEmitsClaimFrame(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()
	id, _ := new_entity_keys(t)

	challenge := make([]byte, challenge_size_v2)
	rand.Read(challenge)

	var buf bytes.Buffer
	if err := claim_write(&buf, id, challenge, test_receiver, protocol_messages); err != nil {
		t.Fatalf("claim_write: %v", err)
	}
	f, err := frame_read(&buf)
	if err != nil {
		t.Fatalf("frame_read: %v", err)
	}
	if f.Type != frame_type_claim {
		t.Errorf("Type: %q want %q", f.Type, frame_type_claim)
	}
	if f.From != id {
		t.Errorf("From: %q want %q", f.From, id)
	}
	if err := claim_verify(f.From, challenge, f.Signature, test_receiver, protocol_messages); err != nil {
		t.Errorf("verify on round-tripped claim: %v", err)
	}
}

func TestClaimWriteFailsForUnknownEntity(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()

	challenge := make([]byte, challenge_size_v2)
	rand.Read(challenge)

	var buf bytes.Buffer
	err := claim_write(&buf, test_entity_id('q'), challenge, test_receiver, protocol_messages) // not in db
	if err == nil {
		t.Error("claim_write succeeded for unknown entity")
	}
}

// --- senders_entity_invalidate (key-rotation hook) --------------------

func TestSendersEntityInvalidateClosesMatching(t *testing.T) {
	// Plan: rotation handler closes any open Sender holding a claim
	// for the rotated entity. We can't easily spin up a real Sender
	// without libp2p, so install a synthetic one by hand into the
	// senders registry and verify the helper finds + tears it down.
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()
	const peer = "peer-rotate-test"
	id, _ := new_entity_keys(t)

	// Stash any existing entry under peer so we restore it on cleanup
	// and don't disturb other tests.
	senders_lock.Lock()
	prev, hadPrev := senders[peer]
	senders[peer] = &Sender{
		peer:     peer,
		outbox:   make(chan *outbound, 1),
		inflight: map[string]*pending{},
		pings:    map[string]int64{},
		claimed:  map[string]bool{id: true},
	}
	s := senders[peer]
	senders_lock.Unlock()
	defer func() {
		senders_lock.Lock()
		if hadPrev {
			senders[peer] = prev
		} else {
			delete(senders, peer)
		}
		senders_lock.Unlock()
	}()

	// Sanity: it's there.
	if s.closed.Load() {
		t.Fatal("setup: Sender is already closed")
	}

	senders_entity_invalidate(id)

	if !s.closed.Load() {
		t.Error("senders_entity_invalidate did not mark Sender closed")
	}
}

func TestSendersEntityInvalidateNoOpForEmptyEntity(t *testing.T) {
	// MUST be a no-op so a buggy rotation handler can't sweep
	// everything by accident.
	const peer = "peer-noop-test"
	senders_lock.Lock()
	prev, hadPrev := senders[peer]
	senders[peer] = &Sender{
		peer:     peer,
		outbox:   make(chan *outbound, 1),
		inflight: map[string]*pending{},
		pings:    map[string]int64{},
		claimed:  map[string]bool{"abc": true},
	}
	s := senders[peer]
	senders_lock.Unlock()
	defer func() {
		senders_lock.Lock()
		if hadPrev {
			senders[peer] = prev
		} else {
			delete(senders, peer)
		}
		senders_lock.Unlock()
	}()

	senders_entity_invalidate("")
	if s.closed.Load() {
		t.Error("senders_entity_invalidate(\"\") closed unrelated Sender")
	}
}

// --- Hello-on-stream end-to-end ---------------------------------------

func TestHelloReadFromHelloWriteOverPipe(t *testing.T) {
	// Models the real wire: one side writes hello, the other reads
	// via the same pipe. Exercises buffered length-prefix reads too.
	pr, pw := io.Pipe()
	defer pr.Close()
	defer pw.Close()

	challenge, _ := hello_challenge()
	done := make(chan error, 1)
	go func() {
		done <- hello_write(pw, 2, "sess", challenge, []string{"zstd"}, nil)
	}()

	got, err := hello_read(pr, 2)
	if err != nil {
		t.Fatalf("hello_read: %v", err)
	}
	if !bytes.Equal(got.Challenge, challenge) {
		t.Error("challenge mismatch over pipe")
	}
	if werr := <-done; werr != nil {
		t.Fatalf("hello_write: %v", werr)
	}
}

// --- session_id sanity -------------------------------------------------

func TestSessionIDIsHex(t *testing.T) {
	s := session_id()
	if len(s) != 16 {
		t.Errorf("session_id length %d, want 16 (8 bytes hex)", len(s))
	}
	if strings.ContainsAny(s, "ghijklmnopqrstuvwxyz") {
		t.Errorf("session_id has non-hex chars: %q", s)
	}
}

func TestSessionIDFresh(t *testing.T) {
	a := session_id()
	b := session_id()
	if a == b {
		t.Error("session_id returned same value twice")
	}
}

// --- responder proof ---------------------------------------------------
//
// The answering side's half of the mutual authentication: proof that the
// host which answered actually holds the entity the opener addressed.
// Without it an opener knows only which host replied, so any peer it was
// told to use could answer for any entity.

func TestResponderProofRoundTrip(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()
	id, _ := new_entity_keys(t)
	challenge := must_challenge(t)

	sig := responder_sign(id, challenge, test_opener, protocol_stream)
	if sig == nil {
		t.Fatal("responder_sign returned nil for a local entity")
	}
	if err := responder_verify(id, challenge, sig, test_opener, protocol_stream); err != nil {
		t.Errorf("responder_verify rejected a valid proof: %v", err)
	}
}

func TestResponderProofRejectsOtherEntity(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()
	// The core property: a host that does not hold an entity's key cannot
	// produce a proof for it. This is what makes a caller-supplied peer
	// harmless — it can answer, but it cannot answer AS the entity.
	holder, _ := new_entity_keys(t)
	impostor, _ := new_entity_keys(t)
	challenge := must_challenge(t)

	sig := responder_sign(impostor, challenge, test_opener, protocol_stream)
	if sig == nil {
		t.Fatal("responder_sign returned nil for a local entity")
	}
	if err := responder_verify(holder, challenge, sig, test_opener, protocol_stream); err == nil {
		t.Error("responder_verify accepted a proof signed by a different entity")
	}
}

func TestResponderProofRejectsReplayOnAnotherStream(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()
	// Each stream mints a fresh challenge, so a proof captured from one
	// stream must not authenticate another.
	id, _ := new_entity_keys(t)
	first := must_challenge(t)
	second := must_challenge(t)

	sig := responder_sign(id, first, test_opener, protocol_stream)
	if err := responder_verify(id, second, sig, test_opener, protocol_stream); err == nil {
		t.Error("responder_verify accepted a proof replayed onto a different challenge")
	}
}

func TestResponderProofRejectsReplayToAnotherOpener(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()
	// The opener binding stops a host collecting a proof on its own
	// connection and relaying it to a victim as though it were the
	// entity's answer.
	id, _ := new_entity_keys(t)
	challenge := must_challenge(t)

	sig := responder_sign(id, challenge, test_opener, protocol_stream)
	if err := responder_verify(id, challenge, sig, "12D3KooWSomeoneElse", protocol_stream); err == nil {
		t.Error("responder_verify accepted a proof addressed to a different opener")
	}
}

func TestResponderProofRejectsCrossProtocol(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()
	id, _ := new_entity_keys(t)
	challenge := must_challenge(t)

	sig := responder_sign(id, challenge, test_opener, protocol_stream)
	if err := responder_verify(id, challenge, sig, test_opener, protocol_messages); err == nil {
		t.Error("responder_verify accepted a stream proof on the messages protocol")
	}
}

func TestResponderProofIsNotAClaim(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()
	// The two directions use separate signing domains, so a claim
	// harvested from a sender can never be presented as a responder's
	// proof (or the reverse) even if the same challenge were in play on
	// both — which is the whole reason responder_domain exists.
	id, _ := new_entity_keys(t)
	challenge := must_challenge(t)

	claim := claim_sign(id, challenge, test_opener, protocol_stream)
	if claim == nil {
		t.Fatal("claim_sign returned nil for a local entity")
	}
	if err := responder_verify(id, challenge, claim, test_opener, protocol_stream); err == nil {
		t.Error("responder_verify accepted a sender claim as a responder proof")
	}

	proof := responder_sign(id, challenge, test_receiver, protocol_stream)
	if err := claim_verify(id, challenge, proof, test_receiver, protocol_stream); err == nil {
		t.Error("claim_verify accepted a responder proof as a sender claim")
	}
}

func TestResponderSignUnknownEntity(t *testing.T) {
	// An entity this host does not hold yields no proof, so the caller
	// answers fail_unproven rather than acking unauthenticated.
	challenge := must_challenge(t)
	if sig := responder_sign(test_entity_id('z'), challenge, test_opener, protocol_stream); sig != nil {
		t.Error("responder_sign produced a proof for an entity this host does not hold")
	}
}

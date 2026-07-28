// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
)

// entity_verify underpins person-level authorship on replicated objects (the
// wikis comment path), so the properties worth pinning are the ones an attacker
// would probe: a signature that verifies only under the exact signer, only over
// the exact text, and never under a malformed or truncated key or signature.

// TestEntityVerifyRoundTrip signs with a real key and checks the signature
// verifies against the entity id alone - no database row, no directory entry.
// That self-containment is the whole reason authorship is checkable on a host
// that has never met the author.
func TestEntityVerifyRoundTrip(t *testing.T) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	entity := base58_encode(public)
	text := "the payload a comment binds its author to"
	signature := base58_encode(ed25519.Sign(private, []byte(text)))

	if !entity_verify(entity, text, signature) {
		t.Error("signature did not verify against the signing entity")
	}
}

// TestEntityVerifyRejectsForgery is the actual threat: a peer that relays a
// comment claiming someone else wrote it. Signing with one key and attributing
// to another must fail, as must any edit to the signed text.
func TestEntityVerifyRejectsForgery(t *testing.T) {
	_, alice_private, _ := ed25519.GenerateKey(rand.Reader)
	bob_public, _, _ := ed25519.GenerateKey(rand.Reader)
	bob := base58_encode(bob_public)

	text := "a comment attributed to Bob"
	alice_signature := base58_encode(ed25519.Sign(alice_private, []byte(text)))

	if entity_verify(bob, text, alice_signature) {
		t.Error("a signature by one entity verified as another - authorship is forgeable")
	}

	// Same signer, tampered body: the classic relay-side edit.
	alice_public, alice_key, _ := ed25519.GenerateKey(rand.Reader)
	alice := base58_encode(alice_public)
	signature := base58_encode(ed25519.Sign(alice_key, []byte(text)))
	if entity_verify(alice, text+" (edited in transit)", signature) {
		t.Error("a signature verified over text it was not made over")
	}
}

// TestEntityVerifyRejectsMalformed covers the inputs that reach this function
// straight off the wire. ed25519.Verify panics on a wrong-sized public key, so
// the length guards are load-bearing, not defensive decoration: without them a
// peer could halt the event handler by sending a short id.
func TestEntityVerifyRejectsMalformed(t *testing.T) {
	public, private, _ := ed25519.GenerateKey(rand.Reader)
	entity := base58_encode(public)
	text := "payload"
	signature := base58_encode(ed25519.Sign(private, []byte(text)))

	cases := []struct {
		name      string
		entity    string
		text      string
		signature string
	}{
		{"empty entity", "", text, signature},
		{"empty signature", entity, text, ""},
		{"short entity", base58_encode(public[:16]), text, signature},
		{"long entity", base58_encode(append(public, 0x00)), text, signature},
		{"short signature", entity, text, base58_encode([]byte("too short"))},
		{"non base58 entity", "not-a-key!!!", text, signature},
		{"non base58 signature", entity, text, "not-a-signature!!!"},
	}

	for _, c := range cases {
		if entity_verify(c.entity, c.text, c.signature) {
			t.Errorf("%s: verified when it should not have", c.name)
		}
	}
}

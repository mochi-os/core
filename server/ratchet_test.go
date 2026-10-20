// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"testing"

	"filippo.io/edwards25519"
)

// omemo_test_device is one side of a session in a test: its identity, its
// signed pre-key and one one-time pre-key, and the bundle it publishes.
type omemo_test_device struct {
	identity omemo_pair
	signed   omemo_pair
	once     omemo_pair
}

func omemo_test_new() *omemo_test_device {
	return &omemo_test_device{identity: omemo_generate(), signed: omemo_generate(), once: omemo_generate()}
}

func (d *omemo_test_device) bundle(version string) *omemo_bundle {
	data := omemo_typed(d.signed.Public)
	if version == omemo_current {
		data = d.signed.Public
	}
	b := &omemo_bundle{identity: d.identity.Public, signed: 7, key: d.signed.Public, signature: omemo_sign(d.identity.Private, data), prekey: 9, once: d.once.Public}
	if version == omemo_current {
		b.edwards = omemo_identity(d.identity.Public)
	}
	return b
}

// accept is the device taking a key exchange, as the device does.
func (d *omemo_test_device) accept(t *testing.T, version string, data []byte) (*omemo_session, []byte) {
	t.Helper()
	k, err := omemo_kex_parse(version, data)
	if err != nil {
		t.Fatalf("key exchange not read: %v", err)
	}
	if k.signed != 7 || k.prekey != 9 {
		t.Fatalf("key exchange names %d and %d", k.signed, k.prekey)
	}
	s, err := omemo_accept(version, d.identity, d.signed, &d.once, k)
	if err != nil {
		t.Fatalf("session not accepted: %v", err)
	}
	plain, err := s.decrypt(d.identity.Public, k.message)
	if err != nil {
		t.Fatalf("first message not decrypted: %v", err)
	}
	return s, plain
}

// The signature XEdDSA makes verifies under the Ed25519 form OMEMO 2
// publishes, and under the Curve25519 key as the original version checks it;
// the form with the sign bit carried in the signature verifies too, and a
// changed message does not.
func TestOmemoSignature(t *testing.T) {
	for i := 0; i < 20; i++ {
		key := omemo_generate()
		message := []byte("signed pre-key")
		signature := omemo_sign(key.Private, message)
		if !ed25519.Verify(omemo_identity(key.Public), message, signature) {
			t.Fatalf("XEdDSA signature does not verify under the Ed25519 form")
		}
		if !omemo_verify(key.Public, message, signature) {
			t.Fatalf("XEdDSA signature does not verify under the Curve25519 key")
		}
		if omemo_verify(key.Public, []byte("signed pre-kez"), signature) {
			t.Fatalf("a changed message verifies")
		}
		if omemo_verify(omemo_generate().Public, message, signature) {
			t.Fatalf("another key verifies")
		}
	}
	// A native Ed25519 key's signature, its sign bit in the signature's last
	// byte, as the Signal protocol's first libraries made them.
	for i := 0; i < 20; i++ {
		public, private, _ := ed25519.GenerateKey(rand.Reader)
		montgomery := omemo_montgomery(public)
		signature := ed25519.Sign(private, []byte("m"))
		signature[63] |= public[31] & 0x80
		if !omemo_verify(montgomery, []byte("m"), signature) {
			t.Fatalf("a signature carrying its sign bit does not verify")
		}
	}
}

// The two forms of a key convert to each other.
func TestOmemoForms(t *testing.T) {
	key := omemo_generate()
	if !bytes.Equal(omemo_montgomery(omemo_identity(key.Public)), key.Public) {
		t.Fatalf("Ed25519 form does not convert back")
	}
	// A y with no x on the curve is not a key.
	for y := byte(2); ; y++ {
		encoded := append([]byte{y}, make([]byte, 31)...)
		if _, err := new(edwards25519.Point).SetBytes(encoded); err != nil {
			if omemo_montgomery(encoded) != nil {
				t.Fatalf("a non-point converts")
			}
			break
		}
	}
	if !bytes.Equal(omemo_untyped(omemo_typed(key.Public)), key.Public) || omemo_untyped(append([]byte{0x06}, key.Public...)) != nil || omemo_untyped([]byte{1, 2}) != nil {
		t.Fatalf("type byte not handled")
	}
}

// A session begun from a bundle carries messages both ways in each version:
// the first ones with the key exchange until an answer comes, messages out of
// order, and a changed message refused without spoiling the session.
func TestOmemoSession(t *testing.T) {
	for _, version := range []string{omemo_legacy, omemo_current} {
		t.Run(version, func(t *testing.T) {
			alice, bob := omemo_test_new(), omemo_test_new()
			a, err := omemo_begin(version, alice.identity, bob.bundle(version))
			if err != nil {
				t.Fatalf("session not begun: %v", err)
			}
			first, kex, err := a.encrypt(alice.identity.Public, []byte("one"), 5)
			if err != nil || !kex {
				t.Fatalf("first message: %v, key exchange %v", err, kex)
			}
			second, kex, _ := a.encrypt(alice.identity.Public, []byte("two"), 5)
			if !kex {
				t.Fatalf("second message before an answer carries no key exchange")
			}
			b, plain := bob.accept(t, version, first)
			if string(plain) != "one" {
				t.Fatalf("first message read %q", plain)
			}
			if version == omemo_legacy {
				if f, _ := omemo_parse(first[1:]); f.numbers[5] != 5 {
					t.Fatalf("registration %d", f.numbers[5])
				}
			}
			// The second, its key exchange the same, is read by the session.
			k, _ := omemo_kex_parse(version, second)
			if !bytes.Equal(k.base, b.Base) {
				t.Fatalf("second message's base key differs")
			}
			if plain, err := b.decrypt(bob.identity.Public, k.message); err != nil || string(plain) != "two" {
				t.Fatalf("second message: %q %v", plain, err)
			}
			// Bob answers; Alice's later messages carry no key exchange.
			answer, kex, err := b.encrypt(bob.identity.Public, []byte("three"), 6)
			if err != nil || kex {
				t.Fatalf("answer: %v, key exchange %v", err, kex)
			}
			if plain, err := a.decrypt(alice.identity.Public, answer); err != nil || string(plain) != "three" {
				t.Fatalf("answer read %q %v", plain, err)
			}
			if a.Exchange != nil {
				t.Fatalf("key exchange kept after the answer")
			}
			var later [][]byte
			for _, text := range []string{"four", "five", "six"} {
				m, kex, _ := a.encrypt(alice.identity.Public, []byte(text), 5)
				if kex {
					t.Fatalf("key exchange sent after the answer")
				}
				later = append(later, m)
			}
			// Out of order, and a message of an earlier chain after a new one.
			for _, i := range []int{2, 0} {
				if plain, err := b.decrypt(bob.identity.Public, later[i]); err != nil || string(plain) != []string{"four", "five", "six"}[i] {
					t.Fatalf("message %d read %q %v", i, plain, err)
				}
			}
			reply, _, _ := b.encrypt(bob.identity.Public, []byte("seven"), 6)
			if plain, err := a.decrypt(alice.identity.Public, reply); err != nil || string(plain) != "seven" {
				t.Fatalf("reply read %q %v", plain, err)
			}
			if plain, err := b.decrypt(bob.identity.Public, later[1]); err != nil || string(plain) != "five" {
				t.Fatalf("late message read %q %v", plain, err)
			}
			// A message read once is not read again.
			if _, err := b.decrypt(bob.identity.Public, later[1]); err == nil {
				t.Fatalf("a message read twice")
			}
			// A changed message is refused and the session goes on, a wrong
			// MAC on its own too.
			m, _, _ := a.encrypt(alice.identity.Public, []byte("eight"), 5)
			changed := bytes.Clone(m)
			changed[len(changed)/2] ^= 1
			if _, err := b.decrypt(bob.identity.Public, changed); err == nil {
				t.Fatalf("a changed message decrypts")
			}
			mac := bytes.Clone(m)
			if version == omemo_legacy {
				mac[len(mac)-1] ^= 1
			} else {
				mac[2] ^= 1
			}
			if _, err := b.decrypt(bob.identity.Public, mac); err == nil {
				t.Fatalf("THE POINT: a message with a wrong MAC decrypts")
			}
			if plain, err := b.decrypt(bob.identity.Public, m); err != nil || string(plain) != "eight" {
				t.Fatalf("message after a changed one read %q %v", plain, err)
			}
			// A message from too far ahead is refused.
			for i := 0; i < omemo_skip+2; i++ {
				m, _, _ = a.encrypt(alice.identity.Public, []byte("x"), 5)
			}
			if _, err := b.decrypt(bob.identity.Public, m); err == nil {
				t.Fatalf("a message too far ahead decrypts")
			}
		})
	}
}

// A bundle whose signed pre-key its identity did not sign begins no session.
func TestOmemoBundleSignature(t *testing.T) {
	for _, version := range []string{omemo_legacy, omemo_current} {
		alice, bob := omemo_test_new(), omemo_test_new()
		b := bob.bundle(version)
		b.signature = omemo_sign(omemo_generate().Private, b.key)
		if _, err := omemo_begin(version, alice.identity, b); err == nil {
			t.Fatalf("%s: a bundle signed by another key begins a session", version)
		}
	}
}

// OMEMO 2 takes an identity made as an Ed25519 key, whose sign bit may be
// set: its bundle's signature verifies under that key as given, and the
// session's associated data names it so.
func TestOmemoEdwardsIdentity(t *testing.T) {
	alice := omemo_test_new()
	for {
		public, private, _ := ed25519.GenerateKey(rand.Reader)
		if public[31]&0x80 == 0 {
			continue
		}
		signed, once := omemo_generate(), omemo_generate()
		b := &omemo_bundle{identity: omemo_montgomery(public), edwards: public, signed: 7, key: signed.Public, signature: ed25519.Sign(private, signed.Public), prekey: 9, once: once.Public}
		a, err := omemo_begin(omemo_current, alice.identity, b)
		if err != nil {
			t.Fatalf("session not begun: %v", err)
		}
		if want := append(omemo_identity(alice.identity.Public), public...); !bytes.Equal(a.associated(alice.identity.Public), want) {
			t.Fatalf("associated data does not name the identity as given")
		}
		return
	}
}

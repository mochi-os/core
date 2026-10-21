// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: OMEMO's sessions
//
// The cryptography of OMEMO's sessions between two devices (the federation
// plan, phase 27), on Go's standard cryptography and the edwards25519 group:
// XEdDSA signatures made with a Curve25519 identity key, the X3DH key
// agreement that begins a session from the other device's bundle, and the
// Double Ratchet that carries it on. Both versions are spoken: the original
// (eu.siacs.conversations.axolotl), whose messages are the Signal protocol's,
// and OMEMO 2 (urn:xmpp:omemo:2), with its own labels, messages and
// associated data. Keys are Curve25519 keys of 32 bytes throughout; the
// original version's messages carry them with a type byte before them, and
// OMEMO 2 names an identity by its Ed25519 form.

package main

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/subtle"
	"errors"
	"fmt"
	"slices"

	"filippo.io/edwards25519"
	"filippo.io/edwards25519/field"
)

// The two versions of OMEMO.
const (
	omemo_legacy  = "eu.siacs.conversations.axolotl"
	omemo_current = "urn:xmpp:omemo:2"
)

// The labels each version derives its keys under.
var omemo_labels = map[string][3]string{
	omemo_legacy:  {"WhisperText", "WhisperRatchet", "WhisperMessageKeys"},
	omemo_current: {"OMEMO X3DH", "OMEMO Root Chain", "OMEMO Message Key Material"},
}

// How far ahead of the last message of a chain a message may be, and how
// many chains of the other device's a session keeps for messages still to
// come.
const (
	omemo_skip   = 2000
	omemo_chains = 5
)

// The original version's message version byte: version 3 of the protocol.
const omemo_version = 0x33

var omemo_refused = errors.New("the message could not be decrypted")

// === Keys ===

// An omemo_pair is a Curve25519 key pair.
type omemo_pair struct {
	Private []byte `json:"private"`
	Public  []byte `json:"public"`
}

// omemo_generate is a new key pair.
func omemo_generate() omemo_pair {
	key, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		panic(err)
	}
	return omemo_pair{Private: key.Bytes(), Public: key.PublicKey().Bytes()}
}

// omemo_agree is the Diffie-Hellman agreement of a private key and a public
// one.
func omemo_agree(private []byte, public []byte) ([]byte, error) {
	key, err := ecdh.X25519().NewPrivateKey(private)
	if err != nil {
		return nil, err
	}
	other, err := ecdh.X25519().NewPublicKey(public)
	if err != nil {
		return nil, err
	}
	return key.ECDH(other)
}

// omemo_typed is a public key with the type byte the original version
// writes it with.
func omemo_typed(public []byte) []byte {
	return append([]byte{0x05}, public...)
}

// omemo_untyped is a public key without its type byte, or nil where it is not
// one.
func omemo_untyped(key []byte) []byte {
	switch {
	case len(key) == 33 && key[0] == 0x05:
		return key[1:]
	case len(key) == 32:
		return key
	}
	return nil
}

// omemo_edwards is a Curve25519 public key's Ed25519 form, its sign bit the
// one given: y = (u - 1) / (u + 1).
func omemo_edwards(public []byte, sign byte) []byte {
	u, err := new(field.Element).SetBytes(public)
	if err != nil {
		return nil
	}
	one := new(field.Element).One()
	numerator := new(field.Element).Subtract(u, one)
	denominator := new(field.Element).Add(u, one)
	y := new(field.Element).Multiply(numerator, new(field.Element).Invert(denominator))
	out := y.Bytes()
	out[31] = out[31]&0x7f | sign&0x80
	return out
}

// omemo_montgomery is an Ed25519 public key's Curve25519 form, or nil where
// it is not a point.
func omemo_montgomery(public []byte) []byte {
	point, err := new(edwards25519.Point).SetBytes(public)
	if err != nil {
		return nil
	}
	return point.BytesMontgomery()
}

// omemo_sign is an XEdDSA signature of a message by a Curve25519 private
// key, which verifies under the key's Ed25519 form with sign bit zero.
func omemo_sign(private []byte, message []byte) []byte {
	k, err := edwards25519.NewScalar().SetBytesWithClamping(private)
	if err != nil {
		panic(err)
	}
	a := k
	public := new(edwards25519.Point).ScalarBaseMult(k).Bytes()
	if public[31]&0x80 != 0 {
		a = edwards25519.NewScalar().Negate(k)
		public[31] &= 0x7f
	}
	random := make([]byte, 64)
	rand.Read(random)
	prefix := bytes.Repeat([]byte{0xff}, 32)
	prefix[0] = 0xfe
	h := sha512.New()
	h.Write(prefix)
	h.Write(a.Bytes())
	h.Write(message)
	h.Write(random)
	r, _ := edwards25519.NewScalar().SetUniformBytes(h.Sum(nil))
	R := new(edwards25519.Point).ScalarBaseMult(r).Bytes()
	h = sha512.New()
	h.Write(R)
	h.Write(public)
	h.Write(message)
	challenge, _ := edwards25519.NewScalar().SetUniformBytes(h.Sum(nil))
	s := edwards25519.NewScalar().MultiplyAdd(challenge, a, r)
	return append(R, s.Bytes()...)
}

// omemo_verify reports whether a signature of a message was made by a
// Curve25519 key: by XEdDSA, or as the Signal protocol's libraries first
// signed, with the Ed25519 form's sign bit in the signature's last byte.
func omemo_verify(public []byte, message []byte, signature []byte) bool {
	if len(public) != 32 || len(signature) != 64 {
		return false
	}
	key := omemo_edwards(public, signature[63])
	if key == nil {
		return false
	}
	plain := bytes.Clone(signature)
	plain[63] &= 0x7f
	return ed25519.Verify(key, message, plain)
}

// omemo_identity is a device's identity as OMEMO 2 names it: the Ed25519 form
// of its Curve25519 key, its sign bit zero.
func omemo_identity(public []byte) []byte {
	return omemo_edwards(public, 0)
}

// === Derivation ===

func omemo_derive(secret []byte, salt []byte, information string, length int) []byte {
	if salt == nil {
		salt = make([]byte, 32)
	}
	out, err := hkdf.Key(sha256.New, secret, salt, information, length)
	if err != nil {
		panic(err)
	}
	return out
}

func omemo_mac(key []byte, parts ...[]byte) []byte {
	h := hmac.New(sha256.New, key)
	for _, p := range parts {
		h.Write(p)
	}
	return h.Sum(nil)
}

// An omemo_keys is what one message is encrypted with.
type omemo_keys struct {
	cipher []byte
	mac    []byte
	iv     []byte
}

// omemo_message_keys are the keys of a message from its chain's key.
func omemo_message_keys(version string, chain []byte) omemo_keys {
	seed := omemo_mac(chain, []byte{0x01})
	derived := omemo_derive(seed, nil, omemo_labels[version][2], 80)
	return omemo_keys{cipher: derived[:32], mac: derived[32:64], iv: derived[64:80]}
}

// omemo_next is a chain's key after a message.
func omemo_next(chain []byte) []byte {
	return omemo_mac(chain, []byte{0x02})
}

func omemo_cbc_seal(key []byte, iv []byte, plain []byte) []byte {
	block, _ := aes.NewCipher(key)
	padding := aes.BlockSize - len(plain)%aes.BlockSize
	padded := append(bytes.Clone(plain), bytes.Repeat([]byte{byte(padding)}, padding)...)
	out := make([]byte, len(padded))
	cipher.NewCBCEncrypter(block, iv).CryptBlocks(out, padded)
	return out
}

func omemo_cbc_open(key []byte, iv []byte, sealed []byte) ([]byte, error) {
	if len(sealed) == 0 || len(sealed)%aes.BlockSize != 0 {
		return nil, omemo_refused
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, omemo_refused
	}
	out := make([]byte, len(sealed))
	cipher.NewCBCDecrypter(block, iv).CryptBlocks(out, sealed)
	padding := int(out[len(out)-1])
	if padding == 0 || padding > aes.BlockSize || padding > len(out) {
		return nil, omemo_refused
	}
	for _, b := range out[len(out)-padding:] {
		if int(b) != padding {
			return nil, omemo_refused
		}
	}
	return out[:len(out)-padding], nil
}

// === Protocol buffers ===

// The few protocol buffer messages OMEMO's are, written and read by hand:
// fields of varints and of bytes.

func omemo_varint(out []byte, value uint64) []byte {
	for value >= 0x80 {
		out = append(out, byte(value)|0x80)
		value >>= 7
	}
	return append(out, byte(value))
}

func omemo_field_number(out []byte, field int, value uint32) []byte {
	return omemo_varint(omemo_varint(out, uint64(field)<<3), uint64(value))
}

func omemo_field_bytes(out []byte, field int, value []byte) []byte {
	out = omemo_varint(out, uint64(field)<<3|2)
	out = omemo_varint(out, uint64(len(value)))
	return append(out, value...)
}

// An omemo_fields is a protocol buffer message read: its numbers and its
// bytes by field.
type omemo_fields struct {
	numbers map[int]uint32
	bytes   map[int][]byte
}

func omemo_parse(data []byte) (*omemo_fields, error) {
	f := &omemo_fields{numbers: map[int]uint32{}, bytes: map[int][]byte{}}
	read := func() (uint64, error) {
		var value uint64
		for shift := 0; shift < 64; shift += 7 {
			if len(data) == 0 {
				return 0, omemo_refused
			}
			b := data[0]
			data = data[1:]
			value |= uint64(b&0x7f) << shift
			if b < 0x80 {
				return value, nil
			}
		}
		return 0, omemo_refused
	}
	for len(data) > 0 {
		tag, err := read()
		if err != nil {
			return nil, err
		}
		field := int(tag >> 3)
		switch tag & 7 {
		case 0:
			value, err := read()
			if err != nil {
				return nil, err
			}
			f.numbers[field] = uint32(value)
		case 2:
			length, err := read()
			if err != nil || length > uint64(len(data)) {
				return nil, omemo_refused
			}
			f.bytes[field] = data[:length]
			data = data[length:]
		default:
			return nil, omemo_refused
		}
	}
	return f, nil
}

// === Sessions ===

// An omemo_chain is one direction's chain: its key and how many messages it
// has carried.
type omemo_chain struct {
	Key   []byte `json:"key"`
	Index uint32 `json:"index"`
}

// An omemo_receiving is a chain of the other device's, by the ratchet key it
// sends under, with the keys of its messages that have not yet come.
type omemo_receiving struct {
	Ratchet []byte            `json:"ratchet"`
	Chain   omemo_chain       `json:"chain"`
	Skipped map[uint32][]byte `json:"skipped,omitempty"`
}

// An omemo_exchange is what a session's first messages carry until the other
// device answers: the bundle keys it was begun from and its base key.
type omemo_exchange struct {
	Prekey uint32 `json:"prekey"`
	Signed uint32 `json:"signed"`
	Base   []byte `json:"base"`
}

// An omemo_session is a session with another device in one version.
type omemo_session struct {
	Version   string            `json:"version"`
	Root      []byte            `json:"root"`
	Ratchet   omemo_pair        `json:"ratchet"`
	Sending   *omemo_chain      `json:"sending,omitempty"`
	Previous  uint32            `json:"previous"`
	Receiving []omemo_receiving `json:"receiving,omitempty"`
	// The other device's identity, in OMEMO 2 its Ed25519 form as it gives
	// it too, and whether this device began the session, which orders OMEMO
	// 2's associated data.
	Identity  []byte `json:"identity"`
	Edwards   []byte `json:"edwards,omitempty"`
	Initiator bool   `json:"initiator"`
	// The base key the session was begun with, by which a repeated key
	// exchange is told from a new one.
	Base     []byte          `json:"base"`
	Exchange *omemo_exchange `json:"exchange,omitempty"`
}

// An omemo_bundle is what a device publishes for sessions to be begun with
// it: its identity, in OMEMO 2 its Ed25519 form as given too, its signed
// pre-key and one of its one-time pre-keys.
type omemo_bundle struct {
	identity  []byte
	edwards   []byte
	signed    uint32
	key       []byte
	signature []byte
	prekey    uint32
	once      []byte
}

// omemo_begin begins a session with another device from its bundle, whose
// signed pre-key is checked against its identity first.
func omemo_begin(version string, identity omemo_pair, b *omemo_bundle) (*omemo_session, error) {
	data := omemo_typed(b.key)
	if version == omemo_current {
		data = b.key
	}
	// OMEMO 2 signs with the identity's Ed25519 form as the bundle gives it,
	// whose sign bit an identity made as an Ed25519 key may have set.
	signed := version == omemo_current && len(b.edwards) == 32 && ed25519.Verify(b.edwards, data, b.signature)
	if !signed && !omemo_verify(b.identity, data, b.signature) {
		return nil, fmt.Errorf("the device's signed pre-key is not signed by its identity")
	}
	base := omemo_generate()
	secret := bytes.Repeat([]byte{0xff}, 32)
	for _, pair := range [][2][]byte{{identity.Private, b.key}, {base.Private, b.identity}, {base.Private, b.key}} {
		shared, err := omemo_agree(pair[0], pair[1])
		if err != nil {
			return nil, err
		}
		secret = append(secret, shared...)
	}
	if b.once != nil {
		shared, err := omemo_agree(base.Private, b.once)
		if err != nil {
			return nil, err
		}
		secret = append(secret, shared...)
	}
	derived := omemo_derive(secret, nil, omemo_labels[version][0], 64)
	s := &omemo_session{Version: version, Root: derived[:32], Identity: b.identity, Edwards: b.edwards, Initiator: true, Base: base.Public}
	s.Receiving = []omemo_receiving{{Ratchet: b.key, Chain: omemo_chain{Key: derived[32:]}}}
	s.Ratchet = omemo_generate()
	if err := s.step_sending(b.key); err != nil {
		return nil, err
	}
	s.Exchange = &omemo_exchange{Prekey: b.prekey, Signed: b.signed, Base: base.Public}
	return s, nil
}

// omemo_accept begins a session another device began with this one, from
// its key exchange: this device's identity, the signed pre-key and the
// one-time pre-key it named.
func omemo_accept(version string, identity omemo_pair, signed omemo_pair, once *omemo_pair, k *omemo_kex) (*omemo_session, error) {
	theirs, base := k.identity, k.base
	secret := bytes.Repeat([]byte{0xff}, 32)
	for _, pair := range [][2][]byte{{signed.Private, theirs}, {identity.Private, base}, {signed.Private, base}} {
		shared, err := omemo_agree(pair[0], pair[1])
		if err != nil {
			return nil, err
		}
		secret = append(secret, shared...)
	}
	if once != nil {
		shared, err := omemo_agree(once.Private, base)
		if err != nil {
			return nil, err
		}
		secret = append(secret, shared...)
	}
	derived := omemo_derive(secret, nil, omemo_labels[version][0], 64)
	return &omemo_session{Version: version, Root: derived[:32], Ratchet: signed, Sending: &omemo_chain{Key: derived[32:]}, Identity: theirs, Edwards: k.edwards, Base: base}, nil
}

// step_sending turns the root to a new sending chain toward the other
// device's ratchet key.
func (s *omemo_session) step_sending(theirs []byte) error {
	shared, err := omemo_agree(s.Ratchet.Private, theirs)
	if err != nil {
		return err
	}
	derived := omemo_derive(shared, s.Root, omemo_labels[s.Version][1], 64)
	s.Root = derived[:32]
	s.Sending = &omemo_chain{Key: derived[32:]}
	return nil
}

// step_receiving takes a new ratchet key of the other device's: a receiving
// chain under it, and a new key pair of this device's to send under.
func (s *omemo_session) step_receiving(theirs []byte) (*omemo_receiving, error) {
	shared, err := omemo_agree(s.Ratchet.Private, theirs)
	if err != nil {
		return nil, err
	}
	derived := omemo_derive(shared, s.Root, omemo_labels[s.Version][1], 64)
	s.Root = derived[:32]
	s.Receiving = append(s.Receiving, omemo_receiving{Ratchet: theirs, Chain: omemo_chain{Key: derived[32:]}})
	if len(s.Receiving) > omemo_chains {
		s.Receiving = s.Receiving[len(s.Receiving)-omemo_chains:]
	}
	if s.Sending != nil {
		s.Previous = s.Sending.Index
	}
	s.Ratchet = omemo_generate()
	if err := s.step_sending(theirs); err != nil {
		return nil, err
	}
	return &s.Receiving[len(s.Receiving)-1], nil
}

// associated is OMEMO 2's associated data: the Ed25519 identities of the
// device that began the session and of the other.
func (s *omemo_session) associated(own []byte) []byte {
	ours, theirs := omemo_identity(own), s.Edwards
	if len(theirs) != 32 {
		theirs = omemo_identity(s.Identity)
	}
	if s.Initiator {
		return slices.Concat(ours, theirs)
	}
	return slices.Concat(theirs, ours)
}

// encrypt encrypts a key for the other device: the message as it is sent,
// and whether it carries the key exchange that begins the session.
func (s *omemo_session) encrypt(own []byte, plain []byte, registration uint32) ([]byte, bool, error) {
	if s.Sending == nil {
		return nil, false, fmt.Errorf("the session cannot send")
	}
	keys := omemo_message_keys(s.Version, s.Sending.Key)
	index := s.Sending.Index
	s.Sending.Key = omemo_next(s.Sending.Key)
	s.Sending.Index++
	sealed := omemo_cbc_seal(keys.cipher, keys.iv, plain)
	var framed []byte
	if s.Version == omemo_legacy {
		var inner []byte
		inner = omemo_field_bytes(inner, 1, omemo_typed(s.Ratchet.Public))
		inner = omemo_field_number(inner, 2, index)
		inner = omemo_field_number(inner, 3, s.Previous)
		inner = omemo_field_bytes(inner, 4, sealed)
		mac := omemo_mac(keys.mac, omemo_typed(own), omemo_typed(s.Identity), []byte{omemo_version}, inner)[:8]
		framed = append(append([]byte{omemo_version}, inner...), mac...)
	} else {
		var inner []byte
		inner = omemo_field_number(inner, 1, index)
		inner = omemo_field_number(inner, 2, s.Previous)
		inner = omemo_field_bytes(inner, 3, s.Ratchet.Public)
		inner = omemo_field_bytes(inner, 4, sealed)
		mac := omemo_mac(keys.mac, s.associated(own), inner)[:16]
		framed = omemo_field_bytes(omemo_field_bytes(nil, 1, mac), 2, inner)
	}
	if s.Exchange == nil {
		return framed, false, nil
	}
	if s.Version == omemo_legacy {
		var outer []byte
		if s.Exchange.Prekey != 0 {
			outer = omemo_field_number(outer, 1, s.Exchange.Prekey)
		}
		outer = omemo_field_bytes(outer, 2, omemo_typed(s.Exchange.Base))
		outer = omemo_field_bytes(outer, 3, omemo_typed(own))
		outer = omemo_field_bytes(outer, 4, framed)
		outer = omemo_field_number(outer, 5, registration)
		outer = omemo_field_number(outer, 6, s.Exchange.Signed)
		return append([]byte{omemo_version}, outer...), true, nil
	}
	var outer []byte
	outer = omemo_field_number(outer, 1, s.Exchange.Prekey)
	outer = omemo_field_number(outer, 2, s.Exchange.Signed)
	outer = omemo_field_bytes(outer, 3, omemo_identity(own))
	outer = omemo_field_bytes(outer, 4, s.Exchange.Base)
	outer = omemo_field_bytes(outer, 5, framed)
	return outer, true, nil
}

// An omemo_kex is a key exchange as it came: the bundle keys of this
// device's it names, the other device's identity and base key, and the
// message it carries.
type omemo_kex struct {
	prekey   uint32
	signed   uint32
	identity []byte
	edwards  []byte
	base     []byte
	message  []byte
}

// omemo_kex_parse reads a key exchange.
func omemo_kex_parse(version string, data []byte) (*omemo_kex, error) {
	if version == omemo_legacy {
		if len(data) < 1 || data[0]>>4 != 3 {
			return nil, omemo_refused
		}
		f, err := omemo_parse(data[1:])
		if err != nil {
			return nil, err
		}
		k := &omemo_kex{prekey: f.numbers[1], signed: f.numbers[6], identity: omemo_untyped(f.bytes[3]), base: omemo_untyped(f.bytes[2]), message: f.bytes[4]}
		if k.identity == nil || k.base == nil || k.message == nil {
			return nil, omemo_refused
		}
		return k, nil
	}
	f, err := omemo_parse(data)
	if err != nil {
		return nil, err
	}
	identity := omemo_montgomery(f.bytes[3])
	if identity == nil || len(f.bytes[4]) != 32 || f.bytes[5] == nil {
		return nil, omemo_refused
	}
	return &omemo_kex{prekey: f.numbers[1], signed: f.numbers[2], identity: identity, edwards: f.bytes[3], base: f.bytes[4], message: f.bytes[5]}, nil
}

// decrypt decrypts a message of the session's, which is changed only where
// it decrypts.
func (s *omemo_session) decrypt(own []byte, data []byte) ([]byte, error) {
	var ratchet, sealed, mac, inner []byte
	var index uint32
	if s.Version == omemo_legacy {
		if len(data) < 9 || data[0]>>4 != 3 {
			return nil, omemo_refused
		}
		inner, mac = data[1:len(data)-8], data[len(data)-8:]
		f, err := omemo_parse(inner)
		if err != nil {
			return nil, err
		}
		ratchet, index, sealed = omemo_untyped(f.bytes[1]), f.numbers[2], f.bytes[4]
	} else {
		outer, err := omemo_parse(data)
		if err != nil {
			return nil, err
		}
		mac, inner = outer.bytes[1], outer.bytes[2]
		f, err := omemo_parse(inner)
		if err != nil || len(mac) != 16 {
			return nil, omemo_refused
		}
		index, ratchet, sealed = f.numbers[1], f.bytes[3], f.bytes[4]
	}
	if len(ratchet) != 32 || sealed == nil {
		return nil, omemo_refused
	}
	// A message of an earlier chain still to come is read from that chain,
	// which the session keeps, so the count of the chain before is not
	// needed.
	work := s.copy()
	var chain *omemo_receiving
	for i := range work.Receiving {
		if bytes.Equal(work.Receiving[i].Ratchet, ratchet) {
			chain = &work.Receiving[i]
		}
	}
	if chain == nil {
		var err error
		if chain, err = work.step_receiving(ratchet); err != nil {
			return nil, err
		}
	}
	var key []byte
	switch {
	case index < chain.Chain.Index:
		key = chain.Skipped[index]
		if key == nil {
			return nil, omemo_refused
		}
		delete(chain.Skipped, index)
	case index-chain.Chain.Index > omemo_skip:
		return nil, omemo_refused
	default:
		for chain.Chain.Index < index {
			if chain.Skipped == nil {
				chain.Skipped = map[uint32][]byte{}
			}
			chain.Skipped[chain.Chain.Index] = chain.Chain.Key
			chain.Chain.Key = omemo_next(chain.Chain.Key)
			chain.Chain.Index++
		}
		key = chain.Chain.Key
		chain.Chain.Key = omemo_next(chain.Chain.Key)
		chain.Chain.Index++
	}
	keys := omemo_message_keys(s.Version, key)
	var expected []byte
	if s.Version == omemo_legacy {
		expected = omemo_mac(keys.mac, omemo_typed(s.Identity), omemo_typed(own), []byte{omemo_version}, inner)[:8]
	} else {
		expected = omemo_mac(keys.mac, work.associated(own), inner)[:16]
	}
	if subtle.ConstantTimeCompare(expected, mac) != 1 {
		return nil, omemo_refused
	}
	plain, err := omemo_cbc_open(keys.cipher, keys.iv, sealed)
	if err != nil {
		return nil, err
	}
	// A message from the other device means it holds the session: the key
	// exchange is no longer sent.
	work.Exchange = nil
	*s = *work
	return plain, nil
}

// copy is a deep copy of the session, worked on until a message decrypts.
func (s *omemo_session) copy() *omemo_session {
	out := *s
	if s.Sending != nil {
		sending := *s.Sending
		out.Sending = &sending
	}
	out.Receiving = make([]omemo_receiving, len(s.Receiving))
	for i, r := range s.Receiving {
		out.Receiving[i] = r
		if r.Skipped != nil {
			out.Receiving[i].Skipped = make(map[uint32][]byte, len(r.Skipped))
			for k, v := range r.Skipped {
				out.Receiving[i].Skipped[k] = v
			}
		}
	}
	if s.Exchange != nil {
		exchange := *s.Exchange
		out.Exchange = &exchange
	}
	return &out
}

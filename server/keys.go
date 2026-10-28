// Mochi server: Keys of hosted accounts
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// A hosted account on another network signs as itself: on the fediverse with
// an RSA key over HTTP signatures, on Bluesky with a P-256 key over its
// repository commits, beside a rotation key that lets the account move
// servers. Core holds the private keys and signs on request, as it does with an
// entity's ed25519 key (decision 5 of the federation plan); an app never reads
// a key. The keys live in a table beside the accounts table in the user's own
// database, travel in the export bundle under its passphrase, and are replaced
// from the account's row on the Networks page. A Bluesky recovery key is the
// one key the user holds: core keeps its public half only.

package main

import (
	"bytes"
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/btcsuite/btcutil/base58"
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

// keypair is one key of a hosted account. Public is a PEM public key for
// RSA and a did:key for P-256; Private is a PEM private key, or empty for a
// recovery key, whose private half the user keeps.
type keypair struct {
	Account   string `db:"account"`
	Kind      string `db:"kind"`
	Algorithm string `db:"algorithm"`
	Public    string `db:"public"`
	Private   string `db:"private"`
	Created   int64  `db:"created"`
}

const key_signing = "signing"
const key_rotation = "rotation"
const key_recovery = "recovery"

const key_rsa = "rsa"
const key_p256 = "p256"
const key_ed25519 = "ed25519"

// keys_table creates the keys table in a user database.
func keys_table(db *DB) {
	db.exec("create table if not exists keys (account text not null, kind text not null, algorithm text not null, public text not null, private text not null default '', created integer not null, primary key (account, kind))")
}

// network_keys lists the keys a network's hosted account is created with, as
// kind and algorithm pairs.
func network_keys(network string) [][2]string {
	switch network {
	case "fediverse":
		return [][2]string{{key_signing, key_rsa}}
	case "bluesky":
		return [][2]string{{key_signing, key_p256}, {key_rotation, key_p256}}
	}
	return nil
}

// key_generate makes a key pair of an algorithm: PEM public and private for
// RSA, did:key public and PEM private for P-256.
func key_generate(algorithm string) (string, string, error) {
	switch algorithm {
	case key_rsa:
		private, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			return "", "", err
		}
		public, err := x509.MarshalPKIXPublicKey(&private.PublicKey)
		if err != nil {
			return "", "", err
		}
		return string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: public})),
			string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(private)})), nil
	case key_p256:
		private, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return "", "", err
		}
		encoded, err := x509.MarshalPKCS8PrivateKey(private)
		if err != nil {
			return "", "", err
		}
		return key_did(&private.PublicKey), string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: encoded})), nil
	case key_ed25519:
		// A Matrix server's signing key: the public half as Matrix writes it,
		// unpadded base64.
		public, private, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			return "", "", err
		}
		encoded, err := x509.MarshalPKCS8PrivateKey(private)
		if err != nil {
			return "", "", err
		}
		return base64.RawStdEncoding.EncodeToString(public), string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: encoded})), nil
	}
	return "", "", fmt.Errorf("unknown key algorithm %q", algorithm)
}

// key_did writes a P-256 public key as a did:key: multibase base58btc of the
// multicodec prefix for p256-pub (0x1200, as a varint) and the compressed
// point, the form the AT Protocol uses in DID documents. Plain base58, not the
// checksummed form entity ids use.
func key_did(public *ecdsa.PublicKey) string {
	point := elliptic.MarshalCompressed(elliptic.P256(), public.X, public.Y)
	return "did:key:z" + base58.Encode(append([]byte{0x80, 0x24}, point...))
}

// key_did_parse reads a did:key for a P-256 public key.
func key_did_parse(did string) (*ecdsa.PublicKey, error) {
	encoded, ok := strings.CutPrefix(did, "did:key:z")
	if !ok {
		return nil, fmt.Errorf("not a did:key")
	}
	raw := base58.Decode(encoded)
	if len(raw) != 35 || raw[0] != 0x80 || raw[1] != 0x24 {
		return nil, fmt.Errorf("not a P-256 did:key")
	}
	x, y := elliptic.UnmarshalCompressed(elliptic.P256(), raw[2:])
	if x == nil {
		return nil, fmt.Errorf("invalid P-256 point")
	}
	return &ecdsa.PublicKey{Curve: elliptic.P256(), X: x, Y: y}, nil
}

// key_rsa_private reads a PEM RSA private key.
func key_rsa_private(text string) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode([]byte(text))
	if block == nil {
		return nil, fmt.Errorf("no PEM block")
	}
	return x509.ParsePKCS1PrivateKey(block.Bytes)
}

// key_rsa_public_of is the PEM public half of a PEM RSA private key, or empty.
func key_rsa_public_of(private string) string {
	key, err := key_rsa_private(private)
	if err != nil {
		return ""
	}
	public, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		return ""
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: public}))
}

// key_public reads a PEM public key, as an actor document carries one: RSA,
// in the PKIX or the PKCS#1 form, or Ed25519.
func key_public(text string) (crypto.PublicKey, error) {
	block, _ := pem.Decode([]byte(text))
	if block == nil {
		return nil, fmt.Errorf("no PEM block")
	}
	parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		// Some servers publish the PKCS#1 form.
		if public, err1 := x509.ParsePKCS1PublicKey(block.Bytes); err1 == nil {
			return public, nil
		}
		return nil, err
	}
	return parsed, nil
}

// key_rsa_public reads a PEM RSA public key.
func key_rsa_public(text string) (*rsa.PublicKey, error) {
	parsed, err := key_public(text)
	if err != nil {
		return nil, err
	}
	public, ok := parsed.(*rsa.PublicKey)
	if !ok {
		return nil, fmt.Errorf("not an RSA key")
	}
	return public, nil
}

// key_p256_private reads a PEM P-256 private key.
func key_p256_private(text string) (*ecdsa.PrivateKey, error) {
	block, _ := pem.Decode([]byte(text))
	if block == nil {
		return nil, fmt.Errorf("no PEM block")
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	private, ok := parsed.(*ecdsa.PrivateKey)
	if !ok || private.Curve != elliptic.P256() {
		return nil, fmt.Errorf("not a P-256 key")
	}
	return private, nil
}

// key_sign_rsa signs data with RSA PKCS#1 v1.5 over SHA-256, the signature
// HTTP signatures and ActivityPub use.
func key_sign_rsa(private string, data []byte) ([]byte, error) {
	key, err := key_rsa_private(private)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(data)
	return rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
}

// key_verify_rsa checks an RSA PKCS#1 v1.5 SHA-256 signature against a PEM
// public key.
func key_verify_rsa(public string, data []byte, signature []byte) error {
	key, err := key_rsa_public(public)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(data)
	return rsa.VerifyPKCS1v15(key, crypto.SHA256, digest[:], signature)
}

// key_sign_p256 signs data with ECDSA P-256 over SHA-256 as the AT Protocol
// requires: the raw 64-byte r || s, with s in its low form.
func key_sign_p256(private string, data []byte) ([]byte, error) {
	key, err := key_p256_private(private)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(data)
	r, s, err := ecdsa.Sign(rand.Reader, key, digest[:])
	if err != nil {
		return nil, err
	}
	order := elliptic.P256().Params().N
	half := new(big.Int).Rsh(order, 1)
	if s.Cmp(half) > 0 {
		s = new(big.Int).Sub(order, s)
	}
	out := make([]byte, 64)
	r.FillBytes(out[:32])
	s.FillBytes(out[32:])
	return out, nil
}

// key_verify_p256 checks a raw r || s P-256 signature against a did:key,
// refusing a high s as the AT Protocol does.
func key_verify_p256(did string, data []byte, signature []byte) error {
	public, err := key_did_parse(did)
	if err != nil {
		return err
	}
	if len(signature) != 64 {
		return fmt.Errorf("signature is %d bytes, not 64", len(signature))
	}
	r := new(big.Int).SetBytes(signature[:32])
	s := new(big.Int).SetBytes(signature[32:])
	order := elliptic.P256().Params().N
	if s.Cmp(new(big.Int).Rsh(order, 1)) > 0 {
		return fmt.Errorf("signature s is not low")
	}
	digest := sha256.Sum256(data)
	if !ecdsa.Verify(public, digest[:], r, s) {
		return fmt.Errorf("signature does not verify")
	}
	return nil
}

// === Keys of an account ===

// keypairs lists an account's keys.
func keypairs(user *User, account string) []keypair {
	db := db_user(user, "user")
	var keys []keypair
	db.scans(&keys, "select * from keys where account=? order by kind", account)
	return keys
}

// keypair_get returns one key of an account, or nil.
func keypair_get(user *User, account string, kind string) *keypair {
	db := db_user(user, "user")
	var key keypair
	if !db.scan(&key, "select * from keys where account=? and kind=?", account, kind) {
		return nil
	}
	return &key
}

// keypairs_create makes the keys a network's hosted account is created
// with. An existing key of a kind is kept.
func keypairs_create(user *User, account string, network string) error {
	db := db_user(user, "user")
	now := time.Now().Unix()
	for _, pair := range network_keys(network) {
		kind, algorithm := pair[0], pair[1]
		if keypair_get(user, account, kind) != nil {
			continue
		}
		public, private, err := key_generate(algorithm)
		if err != nil {
			return err
		}
		db.exec("insert into keys (account, kind, algorithm, public, private, created) values (?, ?, ?, ?, ?, ?)", account, kind, algorithm, public, private, now)
	}
	return nil
}

// keypairs_replace makes a new signing key for an account, keeping the
// algorithm. The fediverse learns the new key from the account document;
// Bluesky from the identity operation and the commit atproto_keys_replaced
// issues after this. The rotation key itself is not replaced here: on Bluesky
// that is an identity operation, and the recovery key the user holds must
// outrank whatever replaces it.
func keypairs_replace(user *User, account string) error {
	key := keypair_get(user, account, key_signing)
	if key == nil {
		return refuse(http.StatusBadRequest, "errors.account_no_keys", nil)
	}
	public, private, err := key_generate(key.Algorithm)
	if err != nil {
		return err
	}
	db := db_user(user, "user")
	db.exec("update keys set public=?, private=?, created=? where account=? and kind=?", public, private, time.Now().Unix(), account, key_signing)
	return nil
}

// keypair_recovery makes a recovery key for a Bluesky account: core keeps
// the public half as a rotation key above its own, and the private half is
// returned once, as hex, for the user to keep. A new one replaces the last.
func keypair_recovery(user *User, account string) (string, string, error) {
	private, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return "", "", err
	}
	public := key_did(&private.PublicKey)
	db := db_user(user, "user")
	db.exec("delete from keys where account=? and kind=?", account, key_recovery)
	db.exec("insert into keys (account, kind, algorithm, public, private, created) values (?, ?, ?, ?, '', ?)", account, key_recovery, key_p256, public, time.Now().Unix())
	return public, hex.EncodeToString(private.D.FillBytes(make([]byte, 32))), nil
}

// keypairs_delete removes an account's keys when the account goes.
func keypairs_delete(db *DB, account string) {
	db.exec("delete from keys where account=?", account)
}

// account_sign signs data with an account's signing key, in the form its
// algorithm's network expects.
func account_sign(user *User, account string, data []byte) ([]byte, error) {
	key := keypair_get(user, account, key_signing)
	if key == nil {
		return nil, fmt.Errorf("account has no signing key")
	}
	switch key.Algorithm {
	case key_rsa:
		return key_sign_rsa(key.Private, data)
	case key_p256:
		return key_sign_p256(key.Private, data)
	}
	return nil, fmt.Errorf("unknown key algorithm %q", key.Algorithm)
}

// === HTTP signatures ===
//
// The draft-cavage form Mastodon, Lemmy and the rest of the fediverse use: a
// Signature header naming the key, the headers signed and the RSA-SHA256
// signature over them, with (request-target), host and date always signed and
// digest when there is a body. Requests are signed in this form; the RFC 9421
// form is accepted inbound as well (below).

// signature_window is how far a signed request's Date may be from now. Mastodon
// accepts twelve hours, Lemmy one; the generous end is taken inbound, since a
// server with a slow clock is a reason to warn, not to drop its posts.
const signature_window = 12 * time.Hour

// signature_timely says whether a signature's time is within the window.
func signature_timely(at time.Time) bool {
	skew := time.Since(at)
	return skew <= signature_window && skew >= -signature_window
}

// signature_sign signs a request as an account: the Date, Host, Digest (when
// there is a body) and Signature headers are set. key_id is the URL the actor
// document publishes the key under.
func signature_sign(r *http.Request, key_id string, private string, body []byte) error {
	r.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	if r.Header.Get("Host") == "" {
		r.Header.Set("Host", r.URL.Host)
	}
	names := []string{"(request-target)", "host", "date"}
	if body != nil {
		digest := sha256.Sum256(body)
		r.Header.Set("Digest", "SHA-256="+base64.StdEncoding.EncodeToString(digest[:]))
		names = append(names, "digest")
	}
	signature, err := key_sign_rsa(private, []byte(signature_string(r, names)))
	if err != nil {
		return err
	}
	r.Header.Set("Signature", fmt.Sprintf(`keyId="%s",algorithm="rsa-sha256",headers="%s",signature="%s"`,
		key_id, strings.Join(names, " "), base64.StdEncoding.EncodeToString(signature)))
	return nil
}

// signature_string builds the signing string: one line per signed header,
// lowercase name, colon, space, value.
func signature_string(r *http.Request, names []string) string {
	lines := make([]string, 0, len(names))
	for _, name := range names {
		value := ""
		switch name {
		case "(request-target)":
			value = strings.ToLower(r.Method) + " " + r.URL.RequestURI()
		case "host":
			value = r.Header.Get("Host")
			if value == "" {
				value = r.Host
			}
		default:
			value = r.Header.Get(name)
		}
		lines = append(lines, name+": "+value)
	}
	return strings.Join(lines, "\n")
}

// signature_parameters reads the Signature header's quoted parameters.
func signature_parameters(header string) map[string]string {
	parameters := map[string]string{}
	for _, part := range strings.Split(header, ",") {
		name, value, found := strings.Cut(strings.TrimSpace(part), "=")
		if !found {
			continue
		}
		parameters[strings.ToLower(name)] = strings.Trim(value, `"`)
	}
	return parameters
}

// A signature_fetcher reads the public key a keyId names. fresh asks for the
// key the actor publishes now rather than the one remembered.
type signature_fetcher func(ctx context.Context, key_id string, fresh bool) (string, error)

// signature_verify checks a signed request: the headers signed cover the
// request target, host and date (and the digest, when there is a body), the
// date is within the window, the digest matches the body, and the signature
// verifies against the key the keyId names, fetched through fetch. A request
// carrying Signature-Input is signed in the RFC 9421 form instead, and is
// checked by signature_message_verify. Returns the keyId, which identifies the
// actor.
func signature_verify(ctx context.Context, r *http.Request, body []byte, fetch signature_fetcher) (string, error) {
	if len(r.Header.Values("Signature-Input")) > 0 {
		return signature_message_verify(ctx, r, body, fetch)
	}
	parameters := signature_parameters(r.Header.Get("Signature"))
	key_id, signature := parameters["keyid"], parameters["signature"]
	if key_id == "" || signature == "" {
		return "", fmt.Errorf("no signature")
	}
	if algorithm := parameters["algorithm"]; algorithm != "" && algorithm != "rsa-sha256" && algorithm != "hs2019" {
		return "", fmt.Errorf("unsupported signature algorithm %q", algorithm)
	}
	names := strings.Fields(strings.ToLower(parameters["headers"]))
	if len(names) == 0 {
		names = []string{"date"}
	}
	signed := map[string]bool{}
	for _, name := range names {
		signed[name] = true
	}
	if !signed["(request-target)"] || !signed["host"] || !signed["date"] {
		return "", fmt.Errorf("signature does not cover the request target, host and date")
	}
	date, err := http.ParseTime(r.Header.Get("Date"))
	if err != nil {
		return "", fmt.Errorf("no date")
	}
	if !signature_timely(date) {
		return "", fmt.Errorf("date outside the signature window")
	}
	if len(body) > 0 {
		if !signed["digest"] {
			return "", fmt.Errorf("signature does not cover the body")
		}
		digest := sha256.Sum256(body)
		if r.Header.Get("Digest") != "SHA-256="+base64.StdEncoding.EncodeToString(digest[:]) {
			return "", fmt.Errorf("digest does not match the body")
		}
	}
	decoded, err := base64.StdEncoding.DecodeString(signature)
	if err != nil {
		return "", fmt.Errorf("signature is not base64")
	}
	message := []byte(signature_string(r, names))
	if err := signature_key_verify(ctx, fetch, key_id, func(public string) error {
		return key_verify_rsa(public, message, decoded)
	}); err != nil {
		return "", err
	}
	return key_id, nil
}

// signature_key_verify checks a signature against the key a keyId names,
// fetched through fetch. A signature that fails against the key fetch
// remembers is tried once more against the key read afresh, since the actor
// may have replaced it.
func signature_key_verify(ctx context.Context, fetch signature_fetcher, key_id string, check func(public string) error) error {
	public, err := fetch(ctx, key_id, false)
	if err != nil {
		return fmt.Errorf("key %s: %w", key_id, err)
	}
	if check(public) == nil {
		return nil
	}
	current, err := fetch(ctx, key_id, true)
	if err != nil || check(current) != nil {
		return fmt.Errorf("signature does not verify")
	}
	return nil
}

// === HTTP message signatures ===
//
// The RFC 9421 form WordPress and NodeBB sign with before falling back to
// draft-cavage: Signature-Input lists each signature's covered components and
// its parameters (keyid, alg, created, expires) under a label, and Signature
// carries the signature under the same label, over a base built from those
// components in order.

// signature_message_maximum is how many of a request's signatures are tried:
// each may name a key to fetch.
const signature_message_maximum = 4

// signature_message_verify checks a request signed in the RFC 9421 form, and
// returns the keyid of the first of its signatures that verifies.
func signature_message_verify(ctx context.Context, r *http.Request, body []byte, fetch signature_fetcher) (string, error) {
	inputs, err := structured_dictionary(strings.Join(r.Header.Values("Signature-Input"), ", "))
	if err != nil {
		return "", fmt.Errorf("signature input: %w", err)
	}
	signatures, err := structured_dictionary(strings.Join(r.Header.Values("Signature"), ", "))
	if err != nil {
		return "", fmt.Errorf("signature: %w", err)
	}
	if len(inputs) == 0 {
		return "", fmt.Errorf("no signature")
	}
	var failures []error
	for i, input := range inputs {
		if i == signature_message_maximum {
			break
		}
		var signature []byte
		for _, member := range signatures {
			if member.name == input.name {
				signature, _ = member.item.value.([]byte)
			}
		}
		key_id, err := signature_message_check(ctx, r, body, input, signature, fetch)
		if err == nil {
			return key_id, nil
		}
		failures = append(failures, fmt.Errorf("%s: %w", input.name, err))
	}
	return "", errors.Join(failures...)
}

// signature_message_check checks one RFC 9421 signature: it covers the method,
// the target and, when there is a body, the body's digest, which must match;
// its created time (or its Date, when it has none) is within the window and
// its expiry has not passed; and it verifies against the key its keyid names,
// by its alg or the key's own. Returns the keyid.
func signature_message_check(ctx context.Context, r *http.Request, body []byte, input structured_member, signature []byte, fetch signature_fetcher) (string, error) {
	if !input.inner {
		return "", fmt.Errorf("signature input is not a list of components")
	}
	if len(signature) == 0 {
		return "", fmt.Errorf("no signature")
	}
	parameters := input.item.parameters
	key_id, _ := parameters["keyid"].(string)
	if key_id == "" {
		return "", fmt.Errorf("no keyid")
	}
	algorithm := ""
	if value, set := parameters["alg"]; set {
		algorithm, _ = value.(string)
		if algorithm != "rsa-v1_5-sha256" && algorithm != "rsa-pss-sha512" && algorithm != "ed25519" {
			return "", fmt.Errorf("unsupported signature algorithm %v", value)
		}
	}

	covered := map[string]bool{}
	for _, component := range input.list {
		if name, ok := component.value.(string); ok {
			covered[name] = true
		}
	}
	target := covered["@target-uri"] || ((covered["@authority"] || covered["host"]) && (covered["@request-target"] || covered["@path"]))
	if !covered["@method"] || !target {
		return "", fmt.Errorf("signature does not cover the method and target")
	}
	if len(body) > 0 && !covered["content-digest"] {
		return "", fmt.Errorf("signature does not cover the body")
	}

	if value, set := parameters["created"]; set {
		created, ok := value.(int64)
		if !ok {
			return "", fmt.Errorf("created is not an integer")
		}
		if !signature_timely(time.Unix(created, 0)) {
			return "", fmt.Errorf("created outside the signature window")
		}
	} else if covered["date"] {
		date, err := http.ParseTime(r.Header.Get("Date"))
		if err != nil {
			return "", fmt.Errorf("no date")
		}
		if !signature_timely(date) {
			return "", fmt.Errorf("date outside the signature window")
		}
	} else {
		return "", fmt.Errorf("signature has no time")
	}
	if value, set := parameters["expires"]; set {
		expires, ok := value.(int64)
		if !ok {
			return "", fmt.Errorf("expires is not an integer")
		}
		if time.Now().After(time.Unix(expires, 0)) {
			return "", fmt.Errorf("signature expired")
		}
	}

	if covered["content-digest"] {
		if err := signature_digest(r, body); err != nil {
			return "", err
		}
	}
	base, err := signature_base(r, input)
	if err != nil {
		return "", err
	}
	if err := signature_key_verify(ctx, fetch, key_id, func(public string) error {
		return signature_check(public, algorithm, []byte(base), signature)
	}); err != nil {
		return "", err
	}
	return key_id, nil
}

// signature_base builds an RFC 9421 signature base: a line per covered
// component, its identifier, colon, space and value, then the signature
// parameters as Signature-Input carries them.
func signature_base(r *http.Request, input structured_member) (string, error) {
	lines := make([]string, 0, len(input.list)+1)
	seen := map[string]bool{}
	for _, component := range input.list {
		name, ok := component.value.(string)
		if !ok || len(component.parameters) > 0 {
			return "", fmt.Errorf("unsupported component %v", component.value)
		}
		if seen[name] {
			return "", fmt.Errorf("component %s covered twice", name)
		}
		seen[name] = true
		value, err := signature_component(r, name)
		if err != nil {
			return "", err
		}
		lines = append(lines, structured_string(name)+": "+value)
	}
	lines = append(lines, `"@signature-params": `+input.text)
	return strings.Join(lines, "\n"), nil
}

// signature_component is a covered component's value: a derived component, or
// a header's values with surrounding whitespace trimmed, joined by a comma and
// a space. The scheme is https, as every address this server publishes is,
// whatever terminated TLS before the request reached it.
func signature_component(r *http.Request, name string) (string, error) {
	switch name {
	case "@method":
		return r.Method, nil
	case "@target-uri":
		return "https://" + r.Host + r.URL.RequestURI(), nil
	case "@authority":
		return strings.TrimSuffix(strings.ToLower(r.Host), ":443"), nil
	case "@scheme":
		return "https", nil
	case "@request-target":
		return r.URL.RequestURI(), nil
	case "@path":
		if path := r.URL.EscapedPath(); path != "" {
			return path, nil
		}
		return "/", nil
	case "@query":
		return "?" + r.URL.RawQuery, nil
	}
	if strings.HasPrefix(name, "@") || name != strings.ToLower(name) {
		return "", fmt.Errorf("unsupported component %s", name)
	}
	values := r.Header.Values(name)
	if len(values) == 0 && name == "host" {
		values = []string{r.Host}
	}
	if len(values) == 0 {
		return "", fmt.Errorf("request carries no %s", name)
	}
	trimmed := make([]string, len(values))
	for i, value := range values {
		trimmed[i] = strings.Trim(value, " \t")
	}
	return strings.Join(trimmed, ", "), nil
}

// signature_digest checks the body against its Content-Digest (RFC 9530): each
// SHA-256 and SHA-512 digest given must match, and there must be one.
func signature_digest(r *http.Request, body []byte) error {
	digests, err := structured_dictionary(strings.Join(r.Header.Values("Content-Digest"), ", "))
	if err != nil {
		return fmt.Errorf("content digest: %w", err)
	}
	checked := false
	for _, digest := range digests {
		var expected []byte
		switch digest.name {
		case "sha-256":
			sum := sha256.Sum256(body)
			expected = sum[:]
		case "sha-512":
			sum := sha512.Sum512(body)
			expected = sum[:]
		default:
			continue
		}
		if value, _ := digest.item.value.([]byte); !bytes.Equal(value, expected) {
			return fmt.Errorf("content digest does not match the body")
		}
		checked = true
	}
	if !checked {
		return fmt.Errorf("no SHA-256 or SHA-512 content digest")
	}
	return nil
}

// signature_check verifies an RFC 9421 signature against a PEM public key by
// the algorithm named, or when none is named by the one the key's type
// implies: rsa-v1_5-sha256 for RSA, ed25519 for Ed25519.
func signature_check(public string, algorithm string, message []byte, signature []byte) error {
	key, err := key_public(public)
	if err != nil {
		return err
	}
	switch key := key.(type) {
	case *rsa.PublicKey:
		switch algorithm {
		case "", "rsa-v1_5-sha256":
			return key_verify_rsa(public, message, signature)
		case "rsa-pss-sha512":
			digest := sha512.Sum512(message)
			return rsa.VerifyPSS(key, crypto.SHA512, digest[:], signature, &rsa.PSSOptions{SaltLength: 64})
		}
	case ed25519.PublicKey:
		if algorithm == "" || algorithm == "ed25519" {
			if !ed25519.Verify(key, message, signature) {
				return fmt.Errorf("signature does not verify")
			}
			return nil
		}
	}
	return fmt.Errorf("signature algorithm %q does not suit the key", algorithm)
}

// === Structured fields ===
//
// Enough of RFC 8941 to read RFC 9421 signatures and RFC 9530 digests:
// dictionaries whose members are items or inner lists of items, each with
// parameters. Integers, decimals, strings, tokens, byte sequences and booleans
// are read; the dates and display strings RFC 9651 adds are not.

// A structured_token is a token, kept apart from a string.
type structured_token string

// structured_item is a bare item and its parameters.
type structured_item struct {
	value      any
	parameters map[string]any
}

// structured_member is a dictionary member: an item, or an inner list when
// inner is set, list then holding its items and item its parameters. text is
// the member's value as the field carries it.
type structured_member struct {
	name  string
	inner bool
	list  []structured_item
	item  structured_item
	text  string
}

// structured is a structured field being read.
type structured struct {
	text     string
	position int
}

// structured_dictionary reads a dictionary field. A repeated name keeps its
// first place and takes its last value.
func structured_dictionary(text string) ([]structured_member, error) {
	s := &structured{text: text}
	s.spaces()
	var members []structured_member
	index := map[string]int{}
	for !s.done() {
		name, err := s.key()
		if err != nil {
			return nil, err
		}
		member := structured_member{name: name}
		if s.next('=') {
			start := s.position
			if s.peek() == '(' {
				member.inner = true
				member.list, member.item.parameters, err = s.inner()
			} else {
				member.item, err = s.item()
			}
			if err != nil {
				return nil, err
			}
			member.text = s.text[start:s.position]
		} else {
			member.item.value = true
			if member.item.parameters, err = s.parameters(); err != nil {
				return nil, err
			}
		}
		if i, found := index[name]; found {
			members[i] = member
		} else {
			index[name] = len(members)
			members = append(members, member)
		}
		s.whitespace()
		if s.done() {
			break
		}
		if !s.next(',') {
			return nil, fmt.Errorf("expected a comma at %d", s.position)
		}
		s.whitespace()
		if s.done() {
			return nil, fmt.Errorf("trailing comma")
		}
	}
	return members, nil
}

// done says whether the field is read to its end.
func (s *structured) done() bool {
	return s.position >= len(s.text)
}

// peek is the next character, or zero at the end.
func (s *structured) peek() byte {
	if s.done() {
		return 0
	}
	return s.text[s.position]
}

// next reads the next character if it is the one given.
func (s *structured) next(c byte) bool {
	if !s.done() && s.text[s.position] == c {
		s.position++
		return true
	}
	return false
}

// spaces skips spaces.
func (s *structured) spaces() {
	for s.peek() == ' ' {
		s.position++
	}
}

// whitespace skips spaces and tabs.
func (s *structured) whitespace() {
	for c := s.peek(); c == ' ' || c == '\t'; c = s.peek() {
		s.position++
	}
}

// key reads a dictionary or parameter name.
func (s *structured) key() (string, error) {
	start := s.position
	if c := s.peek(); (c < 'a' || c > 'z') && c != '*' {
		return "", fmt.Errorf("expected a name at %d", s.position)
	}
	for c := s.peek(); (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '-' || c == '.' || c == '*'; c = s.peek() {
		s.position++
	}
	return s.text[start:s.position], nil
}

// inner reads an inner list and its parameters.
func (s *structured) inner() ([]structured_item, map[string]any, error) {
	s.position++
	list := []structured_item{}
	for !s.done() {
		s.spaces()
		if s.next(')') {
			parameters, err := s.parameters()
			return list, parameters, err
		}
		item, err := s.item()
		if err != nil {
			return nil, nil, err
		}
		list = append(list, item)
		if c := s.peek(); c != ' ' && c != ')' {
			return nil, nil, fmt.Errorf("expected a space or ) at %d", s.position)
		}
	}
	return nil, nil, fmt.Errorf("unterminated inner list")
}

// item reads a bare item and its parameters.
func (s *structured) item() (structured_item, error) {
	value, err := s.bare()
	if err != nil {
		return structured_item{}, err
	}
	parameters, err := s.parameters()
	return structured_item{value: value, parameters: parameters}, err
}

// parameters reads the parameters that follow an item or inner list.
func (s *structured) parameters() (map[string]any, error) {
	parameters := map[string]any{}
	for s.next(';') {
		s.spaces()
		name, err := s.key()
		if err != nil {
			return nil, err
		}
		var value any = true
		if s.next('=') {
			if value, err = s.bare(); err != nil {
				return nil, err
			}
		}
		parameters[name] = value
	}
	return parameters, nil
}

// bare reads a bare item: an int64 for an integer, a float64 for a decimal, a
// string, a structured_token, a []byte for a byte sequence, or a bool.
func (s *structured) bare() (any, error) {
	c := s.peek()
	switch {
	case c == '-' || (c >= '0' && c <= '9'):
		return s.number()
	case c == '"':
		return s.quoted()
	case c == ':':
		return s.binary()
	case c == '?':
		return s.boolean()
	case c == '*' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'):
		return s.token()
	}
	return nil, fmt.Errorf("unexpected %q at %d", c, s.position)
}

// number reads an integer of up to fifteen digits, or a decimal of up to
// twelve before the point and three after.
func (s *structured) number() (any, error) {
	negative := s.next('-')
	if c := s.peek(); c < '0' || c > '9' {
		return nil, fmt.Errorf("expected a digit at %d", s.position)
	}
	start := s.position
	decimal := false
	for !s.done() {
		c := s.peek()
		if c == '.' && !decimal {
			if s.position-start > 12 {
				return nil, fmt.Errorf("decimal too long at %d", start)
			}
			decimal = true
		} else if c < '0' || c > '9' {
			break
		}
		s.position++
		if length := s.position - start; (!decimal && length > 15) || (decimal && length > 16) {
			return nil, fmt.Errorf("number too long at %d", start)
		}
	}
	text := s.text[start:s.position]
	if !decimal {
		value, err := strconv.ParseInt(text, 10, 64)
		if negative {
			value = -value
		}
		return value, err
	}
	if _, fraction, _ := strings.Cut(text, "."); fraction == "" || len(fraction) > 3 {
		return nil, fmt.Errorf("invalid decimal at %d", start)
	}
	value, err := strconv.ParseFloat(text, 64)
	if negative {
		value = -value
	}
	return value, err
}

// quoted reads a string, whose only escapes are \" and \\.
func (s *structured) quoted() (any, error) {
	s.position++
	var text strings.Builder
	for !s.done() {
		c := s.text[s.position]
		s.position++
		switch {
		case c == '\\':
			if s.done() || (s.peek() != '"' && s.peek() != '\\') {
				return nil, fmt.Errorf("invalid escape at %d", s.position)
			}
			text.WriteByte(s.peek())
			s.position++
		case c == '"':
			return text.String(), nil
		case c < 0x20 || c > 0x7e:
			return nil, fmt.Errorf("invalid character in string at %d", s.position)
		default:
			text.WriteByte(c)
		}
	}
	return nil, fmt.Errorf("unterminated string")
}

// token reads a token.
func (s *structured) token() (any, error) {
	start := s.position
	s.position++
	for !s.done() && (strings.IndexByte("!#$%&'*+-.^_`|~:/", s.peek()) >= 0 || (s.peek() >= '0' && s.peek() <= '9') || (s.peek() >= 'a' && s.peek() <= 'z') || (s.peek() >= 'A' && s.peek() <= 'Z')) {
		s.position++
	}
	return structured_token(s.text[start:s.position]), nil
}

// binary reads a byte sequence: base64 between colons, its padding optional.
func (s *structured) binary() (any, error) {
	s.position++
	end := strings.IndexByte(s.text[s.position:], ':')
	if end < 0 {
		return nil, fmt.Errorf("unterminated byte sequence")
	}
	encoded := s.text[s.position : s.position+end]
	s.position += end + 1
	decoded, err := base64.RawStdEncoding.DecodeString(strings.TrimRight(encoded, "="))
	if err != nil {
		return nil, fmt.Errorf("byte sequence is not base64")
	}
	return decoded, nil
}

// boolean reads ?1 or ?0.
func (s *structured) boolean() (any, error) {
	s.position++
	switch {
	case s.next('1'):
		return true, nil
	case s.next('0'):
		return false, nil
	}
	return nil, fmt.Errorf("invalid boolean at %d", s.position)
}

// structured_string writes a string as a structured field carries it.
func structured_string(value string) string {
	return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(value) + `"`
}

// The fetched-key cache: an actor's key is read once a day, and again when a
// signature fails against it, at most once a minute, so an actor that replaces
// its key is heard at once and a stream of forged signatures costs its server
// one fetch a minute. A failed fetch is remembered for an hour, except a
// refusal by our own limiter, which says nothing about the key. Bounded as the
// resolve cache is.
const signature_key_maximum = 10000
const signature_key_refresh = time.Minute

type signature_key_entry struct {
	public    string
	expires   time.Time
	refreshed time.Time
	signed    bool
	failure   error
}

var signature_keys = map[string]signature_key_entry{}
var signature_keys_lock sync.Mutex

// signature_key_fetch fetches the PEM public key an ActivityPub keyId names:
// the actor document at the keyId's URL (fragment aside), whose publicKey
// carries it. Goes through resolve_fetch, so tests substitute documents. fresh
// reads past a remembered key unless it was read afresh within the last
// minute. A failure remembered from an unsigned fetch does not bind a signed
// one: a server in secure mode answers the second where it refused the first.
func signature_key_fetch(ctx context.Context, key_id string, fresh bool) (string, error) {
	signed := resolve_signed_by(ctx) != nil
	now := time.Now()
	signature_keys_lock.Lock()
	entry, found := signature_keys[key_id]
	signature_keys_lock.Unlock()
	if found && now.Before(entry.expires) {
		if entry.public != "" && (!fresh || now.Sub(entry.refreshed) < signature_key_refresh) {
			return entry.public, nil
		}
		if entry.public == "" && (entry.signed || !signed) {
			if entry.failure != nil {
				return "", entry.failure
			}
			return "", fmt.Errorf("key not found")
		}
	}

	public, err := signature_key_read(ctx, key_id)

	signature_keys_lock.Lock()
	defer signature_keys_lock.Unlock()
	if len(signature_keys) >= signature_key_maximum {
		signature_keys = map[string]signature_key_entry{}
	}
	if fresh {
		entry.refreshed = now
	}
	var limited *RateLimitError
	switch {
	case err == nil:
		signature_keys[key_id] = signature_key_entry{public: public, expires: now.Add(resolve_cache_success), refreshed: entry.refreshed}
	case errors.As(err, &limited):
		// Refused by our own limiter: nothing was learnt about the key.
	case found && entry.public != "" && now.Before(entry.expires):
		// A re-read that failed keeps the key the actor had.
		signature_keys[key_id] = entry
	default:
		signature_keys[key_id] = signature_key_entry{expires: now.Add(resolve_failure(err)), signed: signed, failure: err}
	}
	return public, err
}

// signature_key_read reads the key from the actor document, uncached.
func signature_key_read(ctx context.Context, key_id string) (string, error) {
	key, err := signature_key_document(ctx, key_id)
	if err != nil {
		return "", err
	}
	public, _ := key["publicKeyPem"].(string)
	if public == "" {
		return "", fmt.Errorf("actor document carries no key %s", key_id)
	}
	return public, nil
}

// signature_key_owner reads the owner the actor document names for a key: the
// actor the key signs for. Uncached beyond the document fetch; asked only when
// the key's URL is not the actor's own.
func signature_key_owner(ctx context.Context, key_id string) (string, error) {
	key, err := signature_key_document(ctx, key_id)
	if err != nil {
		return "", err
	}
	owner, _ := key["owner"].(string)
	if owner == "" {
		return "", fmt.Errorf("key %s names no owner", key_id)
	}
	return owner, nil
}

// signature_key_document fetches the document a keyId names - the actor
// document, or the key's own document as some servers serve it - and returns
// the key entry whose id is the keyId, or the only one. An http keyId is
// accepted on a loopback host under the development switch that admits private
// addresses, for a stand-in server under test.
func signature_key_document(ctx context.Context, key_id string) (map[string]any, error) {
	if !strings.HasPrefix(key_id, "https://") && !(url_allow_private && strings.HasPrefix(key_id, "http://")) {
		return nil, fmt.Errorf("keyId is not an https URL")
	}
	address, _, _ := strings.Cut(key_id, "#")
	body, err := resolve_fetch(ctx, address, "application/activity+json, application/ld+json")
	if err != nil {
		return nil, err
	}
	var document struct {
		ID        string `json:"id"`
		Owner     string `json:"owner"`
		Pem       string `json:"publicKeyPem"`
		PublicKey any    `json:"publicKey"`
	}
	if err := json.Unmarshal(body, &document); err != nil {
		return nil, err
	}
	// The document may be the key itself.
	if document.Pem != "" && (document.ID == key_id || document.ID == "") {
		return map[string]any{"id": document.ID, "owner": document.Owner, "publicKeyPem": document.Pem}, nil
	}
	var keys []any
	switch value := document.PublicKey.(type) {
	case map[string]any:
		keys = []any{value}
	case []any:
		keys = value
	}
	for _, item := range keys {
		key, _ := item.(map[string]any)
		if id, _ := key["id"].(string); id == key_id || (id == "" && len(keys) == 1) {
			if public, _ := key["publicKeyPem"].(string); public != "" {
				if _, set := key["owner"]; !set && document.ID != "" {
					key["owner"] = document.ID
				}
				return key, nil
			}
		}
	}
	return nil, fmt.Errorf("actor document carries no key %s", key_id)
}

// === Starlark API ===

var api_account_key = sls.FromStringDict(sl.String("mochi.account.key"), sl.StringDict{
	"list":     sl.NewBuiltin("mochi.account.key.list", api_account_key_list),
	"recovery": sl.NewBuiltin("mochi.account.key.recovery", api_account_key_recovery),
	"replace":  sl.NewBuiltin("mochi.account.key.replace", api_account_key_replace),
})

// keypair_call resolves the caller and the hosted account a key call
// names, checking the permission.
func keypair_call(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, permission string) (*User, map[string]any, error) {
	if len(args) != 1 {
		return nil, nil, fmt.Errorf("syntax: <id: string>")
	}
	if err := require_permission(t, fn, permission); err != nil {
		return nil, nil, err
	}
	user := principal_caller(t)
	if user == nil {
		return nil, nil, fmt.Errorf("no user")
	}
	id, ok := account_id_arg(args[0])
	if !ok {
		return nil, nil, fmt.Errorf("invalid id")
	}
	for _, row := range accounts_hosted(user, "") {
		if row["id"] == id {
			return user, row, nil
		}
	}
	return nil, nil, fmt.Errorf("no such hosted account")
}

// mochi.account.key.list(id) -> list: A hosted account's keys: kind, algorithm,
// public and created. Private keys never leave core.
func api_account_key_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user, row, err := keypair_call(t, fn, args, "accounts/read")
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	keys := []map[string]any{}
	for _, key := range keypairs(user, row["id"].(string)) {
		keys = append(keys, map[string]any{"kind": key.Kind, "algorithm": key.Algorithm, "public": key.Public, "created": key.Created})
	}
	return sl_encode(keys), nil
}

// mochi.account.key.replace(id) -> bool: Replace a hosted account's signing key.
func api_account_key_replace(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user, row, err := keypair_call(t, fn, args, "accounts/write")
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	if err := keypairs_replace(user, row["id"].(string)); err != nil {
		return sl_error(fn, "%v", err)
	}
	if row["type"] == "bluesky" {
		if err := atproto_keys_replaced(thread_context(t), user, row["id"].(string)); err != nil {
			return sl_error(fn, "%v", refuse(http.StatusBadGateway, "errors.key_not_announced", map[string]any{"reason": refusal_reason(err)}))
		}
	}
	// The fediverse learns the new key from the document, sent to the
	// followers' servers.
	if row["type"] == "fediverse" {
		account_changed(user, row["id"].(string))
	}
	return sl.True, nil
}

// mochi.account.key.recovery(id) -> dict: Make a recovery key for a Bluesky
// account. Returns public (a did:key, which core keeps) and private (hex, which
// the user keeps and core never stores); a new one replaces the last.
func api_account_key_recovery(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user, row, err := keypair_call(t, fn, args, "accounts/write")
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	if row["type"] != "bluesky" {
		return sl_error(fn, "only a Bluesky account has a recovery key")
	}
	public, private, err := keypair_recovery(user, row["id"].(string))
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	// The identity lists the new key above the server's own, so the user
	// can act on the account without this server.
	if err := atproto_recovery_added(thread_context(t), user, row["id"].(string)); err != nil {
		return sl_error(fn, "%v", refuse(http.StatusBadGateway, "errors.recovery_not_listed", map[string]any{"reason": refusal_reason(err)}))
	}
	return sl_encode(map[string]any{"public": public, "private": private}), nil
}

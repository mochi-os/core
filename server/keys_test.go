// Mochi server: Tests for the keys of hosted accounts
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"context"
	"crypto"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestKeyGenerateAndSign: each algorithm's key signs and verifies, a signature
// by another key does not, and a P-256 signature is 64 bytes with a low s and
// its public key round-trips through did:key.
func TestKeyGenerateAndSign(t *testing.T) {
	data := []byte("the quick brown fox")

	public, private, err := key_generate(key_rsa)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(public, "-----BEGIN PUBLIC KEY-----") || !strings.HasPrefix(private, "-----BEGIN RSA PRIVATE KEY-----") {
		t.Errorf("RSA keys are not PEM: %q / %q", public[:30], private[:30])
	}
	signature, err := key_sign_rsa(private, data)
	if err != nil {
		t.Fatal(err)
	}
	if err := key_verify_rsa(public, data, signature); err != nil {
		t.Errorf("RSA signature should verify: %v", err)
	}
	if err := key_verify_rsa(public, []byte("other data"), signature); err == nil {
		t.Error("RSA signature over other data should not verify")
	}
	other_public, _, _ := key_generate(key_rsa)
	if err := key_verify_rsa(other_public, data, signature); err == nil {
		t.Error("RSA signature should not verify with another key")
	}

	did, ec_private, err := key_generate(key_p256)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(did, "did:key:zDn") {
		t.Errorf("a P-256 did:key begins did:key:zDn, got %q", did)
	}
	parsed, err := key_did_parse(did)
	if err != nil {
		t.Fatalf("did:key does not parse: %v", err)
	}
	if key_did(parsed) != did {
		t.Error("did:key does not round-trip")
	}
	signature, err = key_sign_p256(ec_private, data)
	if err != nil {
		t.Fatal(err)
	}
	if len(signature) != 64 {
		t.Errorf("P-256 signature is %d bytes, want 64", len(signature))
	}
	if err := key_verify_p256(did, data, signature); err != nil {
		t.Errorf("P-256 signature should verify: %v", err)
	}
	if err := key_verify_p256(did, []byte("other"), signature); err == nil {
		t.Error("P-256 signature over other data should not verify")
	}
	// Flipping s to its high form must be refused, as the AT Protocol does.
	high := append([]byte{}, signature...)
	order := elliptic.P256().Params().N
	s := new(big.Int).SetBytes(high[32:])
	s.Sub(order, s)
	s.FillBytes(high[32:])
	if err := key_verify_p256(did, data, high); err == nil {
		t.Error("a high-s signature should be refused")
	}
	if _, _, err := key_generate("dsa"); err == nil {
		t.Error("an unknown algorithm should be refused")
	}
}

// TestAccountKeys: a hosted account is created with its network's keys, which
// sign; replacing changes the signing key alone; a recovery key is stored
// public-only and returned once; deleting the account's keys clears them.
func TestAccountKeys(t *testing.T) {
	alice, _ := create_handles_test_env(t)
	db := db_user(alice, "user")
	db.exec("insert into accounts (id, type, label, identifier, data, created, verified) values ('f1', 'fediverse', '', '@alice@example.com', '{\"hosted\":true,\"handle\":\"alice\",\"domain\":\"example.com\"}', 1, 1)")
	db.exec("insert into accounts (id, type, label, identifier, data, created, verified) values ('b1', 'bluesky', '', 'alice.example.com', '{\"hosted\":true,\"handle\":\"alice\",\"domain\":\"example.com\"}', 1, 1)")

	if err := keypairs_create(alice, "f1", "fediverse"); err != nil {
		t.Fatal(err)
	}
	if err := keypairs_create(alice, "b1", "bluesky"); err != nil {
		t.Fatal(err)
	}
	fediverse := keypairs(alice, "f1")
	if len(fediverse) != 1 || fediverse[0].Kind != key_signing || fediverse[0].Algorithm != key_rsa {
		t.Errorf("a fediverse account has one RSA signing key: %+v", fediverse)
	}
	bluesky := keypairs(alice, "b1")
	if len(bluesky) != 2 || bluesky[0].Kind != key_rotation || bluesky[1].Kind != key_signing || bluesky[0].Algorithm != key_p256 {
		t.Errorf("a Bluesky account has P-256 rotation and signing keys: %+v", bluesky)
	}
	if keypairs_create(alice, "f1", "fediverse") != nil || len(keypairs(alice, "f1")) != 1 {
		t.Error("creating again keeps the existing key")
	}

	data := []byte("signed as the account")
	signature, err := account_sign(alice, "f1", data)
	if err != nil {
		t.Fatal(err)
	}
	if err := key_verify_rsa(fediverse[0].Public, data, signature); err != nil {
		t.Errorf("the account's signature verifies against its public key: %v", err)
	}
	signature, err = account_sign(alice, "b1", data)
	if err != nil {
		t.Fatal(err)
	}
	if err := key_verify_p256(bluesky[1].Public, data, signature); err != nil {
		t.Errorf("the Bluesky account's signature verifies against its signing key: %v", err)
	}

	before := keypair_get(alice, "b1", key_signing).Public
	rotation := keypair_get(alice, "b1", key_rotation).Public
	if err := keypairs_replace(alice, "b1"); err != nil {
		t.Fatal(err)
	}
	if keypair_get(alice, "b1", key_signing).Public == before {
		t.Error("replacing should change the signing key")
	}
	if keypair_get(alice, "b1", key_rotation).Public != rotation {
		t.Error("replacing should keep the rotation key")
	}
	if err := keypairs_replace(alice, "nothing"); !refused(err, "errors.account_no_keys", "") {
		t.Errorf("an account without keys cannot be replaced: %v", err)
	}

	public, private, err := keypair_recovery(alice, "b1")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(public, "did:key:z") || len(private) != 64 {
		t.Errorf("recovery key: public %q, private %d hex characters", public, len(private))
	}
	recovery := keypair_get(alice, "b1", key_recovery)
	if recovery == nil || recovery.Private != "" || recovery.Public != public {
		t.Errorf("the recovery key is kept public-only: %+v", recovery)
	}
	public2, _, _ := keypair_recovery(alice, "b1")
	if keypair_get(alice, "b1", key_recovery).Public != public2 || len(keypairs(alice, "b1")) != 3 {
		t.Error("a new recovery key replaces the last")
	}

	keypairs_delete(db, "b1")
	if len(keypairs(alice, "b1")) != 0 || len(keypairs(alice, "f1")) != 1 {
		t.Error("deleting an account's keys clears its keys alone")
	}
	accounts_hosted_remove(alice, "example.com", "alice")
	if len(keypairs(alice, "f1")) != 0 {
		t.Error("removing hosted accounts removes their keys")
	}
}

// TestKeysExportRestore: private keys leave the staged user.db for the secrets
// and come back on restore.
func TestKeysExportRestore(t *testing.T) {
	alice, _ := create_handles_test_env(t)
	db := db_user(alice, "user")
	db.exec("insert into accounts (id, type, label, identifier, data, created, verified) values ('f1', 'fediverse', '', '@alice@example.com', '{\"hosted\":true}', 1, 1)")
	if err := keypairs_create(alice, "f1", "fediverse"); err != nil {
		t.Fatal(err)
	}
	private := keypair_get(alice, "f1", key_signing).Private

	extracted := export_keys_extract(db)
	if len(extracted) != 1 || extracted[0].Account != "f1" || extracted[0].Kind != key_signing || extracted[0].Private != private {
		t.Fatalf("extracted = %+v", extracted)
	}
	if keypair_get(alice, "f1", key_signing).Private != "" {
		t.Error("the staged copy should carry no private key")
	}
	for _, k := range extracted {
		db.exec("update keys set private=? where account=? and kind=?", k.Private, k.Account, k.Kind)
	}
	if keypair_get(alice, "f1", key_signing).Private != private {
		t.Error("the private key should be back after restore")
	}
}

// TestSignatureSignVerify: a request signed as an account verifies against the
// key its keyId names; a tampered body, a stale date, a wrong key, an
// unsigned header set and a missing signature are refused.
func TestSignatureSignVerify(t *testing.T) {
	public, private, err := key_generate(key_rsa)
	if err != nil {
		t.Fatal(err)
	}
	key_id := "https://example.com/~abc#main-key"
	fetch := func(ctx context.Context, id string, fresh bool) (string, error) {
		if id == key_id {
			return public, nil
		}
		return "", fmt.Errorf("unknown key")
	}
	body := []byte(`{"type":"Follow"}`)
	sign := func(body []byte) *http.Request {
		r := httptest.NewRequest("POST", "https://remote.example/inbox", bytes.NewReader(body))
		r.Host = "remote.example"
		if err := signature_sign(r, key_id, private, body); err != nil {
			t.Fatal(err)
		}
		return r
	}

	r := sign(body)
	if got, err := signature_verify(context.Background(), r, body, fetch); err != nil || got != key_id {
		t.Errorf("a signed request should verify: %q, %v", got, err)
	}
	if !strings.Contains(r.Header.Get("Signature"), `headers="(request-target) host date digest"`) {
		t.Errorf("signed headers: %s", r.Header.Get("Signature"))
	}

	if _, err := signature_verify(context.Background(), sign(body), []byte(`{"type":"Delete"}`), fetch); err == nil {
		t.Error("a body that does not match the digest should be refused")
	}

	// Signed correctly, but thirteen hours ago: the date is part of what is
	// signed, so the stale request is re-signed with it rather than edited.
	stale := httptest.NewRequest("POST", "https://remote.example/inbox", bytes.NewReader(body))
	stale.Host = "remote.example"
	stale.Header.Set("Host", "remote.example")
	stale.Header.Set("Date", time.Now().Add(-13*time.Hour).UTC().Format(http.TimeFormat))
	stale_digest := sha256.Sum256(body)
	stale.Header.Set("Digest", "SHA-256="+base64.StdEncoding.EncodeToString(stale_digest[:]))
	stale_names := []string{"(request-target)", "host", "date", "digest"}
	stale_signature, _ := key_sign_rsa(private, []byte(signature_string(stale, stale_names)))
	stale.Header.Set("Signature", fmt.Sprintf(`keyId="%s",algorithm="rsa-sha256",headers="%s",signature="%s"`, key_id, strings.Join(stale_names, " "), base64.StdEncoding.EncodeToString(stale_signature)))
	if _, err := signature_verify(context.Background(), stale, body, fetch); err == nil {
		t.Error("a date outside the window should be refused")
	}

	wrong := sign(body)
	wrong.Header.Set("Signature", strings.Replace(wrong.Header.Get("Signature"), key_id, "https://example.com/~other#main-key", 1))
	if _, err := signature_verify(context.Background(), wrong, body, fetch); err == nil {
		t.Error("an unknown key should be refused")
	}

	other_public, _, _ := key_generate(key_rsa)
	if _, err := signature_verify(context.Background(), sign(body), body, func(context.Context, string, bool) (string, error) { return other_public, nil }); err == nil {
		t.Error("a signature by another key should be refused")
	}

	partial := httptest.NewRequest("GET", "https://remote.example/~abc", nil)
	partial.Host = "remote.example"
	partial.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	signature, _ := key_sign_rsa(private, []byte(signature_string(partial, []string{"date"})))
	partial.Header.Set("Signature", fmt.Sprintf(`keyId="%s",algorithm="rsa-sha256",headers="date",signature="%s"`, key_id, base64.StdEncoding.EncodeToString(signature)))
	if _, err := signature_verify(context.Background(), partial, nil, fetch); err == nil {
		t.Error("a signature that does not cover the request target and host should be refused")
	}

	none := httptest.NewRequest("GET", "https://remote.example/~abc", nil)
	if _, err := signature_verify(context.Background(), none, nil, fetch); err == nil {
		t.Error("a request with no signature should be refused")
	}
}

// signature_message_test_request is a POST of a body to
// https://remote.example/inbox, with its Content-Digest.
func signature_message_test_request(body []byte) *http.Request {
	r := httptest.NewRequest("POST", "https://remote.example/inbox", bytes.NewReader(body))
	r.Host = "remote.example"
	digest := sha256.Sum256(body)
	r.Header.Set("Content-Digest", "sha-256=:"+base64.StdEncoding.EncodeToString(digest[:])+":")
	return r
}

// signature_message_test_sign signs a request in the RFC 9421 form under a
// label, covering the components given, with the values given, in order, and
// the parameters given. The base is written out here as RFC 9421 section 2.5
// lays it out, not built by signature_base. The signature is added to any the
// request already carries.
func signature_message_test_sign(r *http.Request, label string, components [][2]string, parameters string, sign func([]byte) []byte) {
	names := make([]string, 0, len(components))
	lines := make([]string, 0, len(components)+1)
	for _, component := range components {
		names = append(names, `"`+component[0]+`"`)
		lines = append(lines, `"`+component[0]+`": `+component[1])
	}
	input := "(" + strings.Join(names, " ") + ")" + parameters
	lines = append(lines, `"@signature-params": `+input)
	r.Header.Add("Signature-Input", label+"="+input)
	r.Header.Add("Signature", label+"=:"+base64.StdEncoding.EncodeToString(sign([]byte(strings.Join(lines, "\n"))))+":")
}

// TestSignatureMessageVerify: a request signed in the RFC 9421 form, as
// WordPress and NodeBB sign, verifies against the key its keyid names by
// rsa-v1_5-sha256, by the key's type when no alg is given, by rsa-pss-sha512
// and by ed25519, its time from created or a covered Date, its digest SHA-256
// or SHA-512. A tampered signature, a tampered body, an expired signature, one
// created outside the window, one with no time, one not covering the body or
// the method and target, a repeated component, an alg the key does not suit
// and an unsupported alg (refused before any fetch) are refused; of several
// signatures, one that verifies admits the request.
func TestSignatureMessageVerify(t *testing.T) {
	public, private, err := key_generate(key_rsa)
	if err != nil {
		t.Fatal(err)
	}
	curve_public, curve_private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := x509.MarshalPKIXPublicKey(curve_public)
	curve_pem := string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: encoded}))
	key_id := "https://remote.example/users/bob#main-key"
	curve_id := "https://remote.example/users/bob#ed25519-key"
	fetches := 0
	fetch := func(ctx context.Context, id string, fresh bool) (string, error) {
		fetches++
		switch id {
		case key_id:
			return public, nil
		case curve_id:
			return curve_pem, nil
		}
		return "", fmt.Errorf("unknown key")
	}
	ctx := context.Background()
	body := []byte(`{"type":"Follow"}`)
	now := time.Now().Unix()
	fresh := fmt.Sprintf(`;created=%d;keyid="%s";alg="rsa-v1_5-sha256"`, now, key_id)
	pkcs := func(base []byte) []byte {
		signature, err := key_sign_rsa(private, base)
		if err != nil {
			t.Fatal(err)
		}
		return signature
	}
	standard := func(r *http.Request) [][2]string {
		return [][2]string{{"@method", "POST"}, {"@target-uri", "https://remote.example/inbox"}, {"content-digest", r.Header.Get("Content-Digest")}}
	}
	signed := func(components func(*http.Request) [][2]string, parameters string, sign func([]byte) []byte) *http.Request {
		r := signature_message_test_request(body)
		signature_message_test_sign(r, "sig1", components(r), parameters, sign)
		return r
	}
	refused := func(r *http.Request, body []byte, reason string, message string) {
		t.Helper()
		if _, err := signature_verify(ctx, r, body, fetch); err == nil || !strings.Contains(err.Error(), reason) {
			t.Errorf("%s: %v", message, err)
		}
	}

	r := signed(standard, fresh, pkcs)
	if got, err := signature_verify(ctx, r, body, fetch); err != nil || got != key_id {
		t.Errorf("an rsa-v1_5-sha256 signature over @method, @target-uri and content-digest should verify: %q, %v", got, err)
	}

	refused(signed(standard, fresh, func(base []byte) []byte {
		signature := pkcs(base)
		signature[0] ^= 1
		return signature
	}), body, "signature does not verify", "a tampered signature should be refused")

	refused(signed(standard, fresh, pkcs), []byte(`{"type":"Delete"}`), "content digest does not match the body", "a body that does not match the content digest should be refused")

	refused(signed(standard, fmt.Sprintf(`;created=%d;expires=%d;keyid="%s";alg="rsa-v1_5-sha256"`, now-60, now-30, key_id), pkcs), body, "signature expired", "an expired signature should be refused")

	refused(signed(standard, fmt.Sprintf(`;created=%d;keyid="%s";alg="rsa-v1_5-sha256"`, now-13*3600, key_id), pkcs), body, "created outside the signature window", "a signature created thirteen hours ago should be refused")

	refused(signed(standard, fmt.Sprintf(`;keyid="%s";alg="rsa-v1_5-sha256"`, key_id), pkcs), body, "signature has no time", "a signature with no created time and no Date should be refused")

	dated := signature_message_test_request(body)
	dated.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	signature_message_test_sign(dated, "sig1", append(standard(dated), [2]string{"date", dated.Header.Get("Date")}), fmt.Sprintf(`;keyid="%s";alg="rsa-v1_5-sha256"`, key_id), pkcs)
	if _, err := signature_verify(ctx, dated, body, fetch); err != nil {
		t.Errorf("a signature with no created time but a covered Date should verify: %v", err)
	}

	refused(signed(func(r *http.Request) [][2]string { return standard(r)[:2] }, fresh, pkcs), body, "signature does not cover the body", "a signature not covering the body's digest should be refused")

	refused(signed(func(r *http.Request) [][2]string { return standard(r)[2:] }, fresh, pkcs), body, "signature does not cover the method and target", "a signature not covering the method and target should be refused")

	refused(signed(func(r *http.Request) [][2]string { return append(standard(r), [2]string{"@method", "POST"}) }, fresh, pkcs), body, "component @method covered twice", "a component covered twice should be refused")

	if _, err := signature_verify(ctx, signed(standard, fmt.Sprintf(`;created=%d;keyid="%s"`, now, key_id), pkcs), body, fetch); err != nil {
		t.Errorf("a signature with no alg should verify by the RSA key's rsa-v1_5-sha256: %v", err)
	}

	refused(signed(standard, fmt.Sprintf(`;created=%d;keyid="%s";alg="ed25519"`, now, key_id), pkcs), body, "signature does not verify", "an alg the key does not suit should be refused")

	before := fetches
	refused(signed(standard, fmt.Sprintf(`;created=%d;keyid="%s";alg="hmac-sha256"`, now, key_id), pkcs), body, "unsupported signature algorithm", "an unsupported alg should be refused")
	if fetches != before {
		t.Errorf("an unsupported alg should be refused before the key is fetched: %d fetches", fetches-before)
	}

	pss := func(base []byte) []byte {
		key, err := key_rsa_private(private)
		if err != nil {
			t.Fatal(err)
		}
		digest := sha512.Sum512(base)
		signature, err := rsa.SignPSS(rand.Reader, key, crypto.SHA512, digest[:], &rsa.PSSOptions{SaltLength: 64})
		if err != nil {
			t.Fatal(err)
		}
		return signature
	}
	if _, err := signature_verify(ctx, signed(standard, fmt.Sprintf(`;created=%d;keyid="%s";alg="rsa-pss-sha512"`, now, key_id), pss), body, fetch); err != nil {
		t.Errorf("an rsa-pss-sha512 signature should verify: %v", err)
	}

	curve := func(base []byte) []byte { return ed25519.Sign(curve_private, base) }
	if got, err := signature_verify(ctx, signed(standard, fmt.Sprintf(`;created=%d;keyid="%s";alg="ed25519"`, now, curve_id), curve), body, fetch); err != nil || got != curve_id {
		t.Errorf("an ed25519 signature should verify: %q, %v", got, err)
	}

	long := signature_message_test_request(body)
	digest := sha512.Sum512(body)
	long.Header.Set("Content-Digest", "sha-512=:"+base64.StdEncoding.EncodeToString(digest[:])+":")
	signature_message_test_sign(long, "sig1", standard(long), fresh, pkcs)
	if _, err := signature_verify(ctx, long, body, fetch); err != nil {
		t.Errorf("a SHA-512 content digest should be checked and verify: %v", err)
	}

	several := signature_message_test_request(body)
	signature_message_test_sign(several, "sig0", standard(several), fmt.Sprintf(`;created=%d;keyid="https://remote.example/users/nobody#main-key"`, now), pkcs)
	signature_message_test_sign(several, "sig1", standard(several), fresh, pkcs)
	if got, err := signature_verify(ctx, several, body, fetch); err != nil || got != key_id {
		t.Errorf("of several signatures, one that verifies should admit the request: %q, %v", got, err)
	}
}

// TestSignatureMessageBase: the signature base and the derived components are
// as RFC 9421 gives them (sections 2.1, 2.2 and 2.5), and the RFC's own
// ed25519 example (appendix B.2.6) verifies against its published key.
func TestSignatureMessageBase(t *testing.T) {
	r := httptest.NewRequest("POST", "https://example.com/foo?param=Value&Pet=dog", strings.NewReader(`{"hello": "world"}`))
	r.Host = "example.com"
	r.Header.Set("Date", "Tue, 20 Apr 2021 02:07:55 GMT")
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Content-Digest", "sha-512=:WZDPaVn/7XgHaAy8pmojAkGWoRx2UFChF41A2svX+TaPm+AbwAgBWnrIiYllu7BNNyealdVLvRwEmTHWXvJwew==:")
	r.Header.Set("Content-Length", "18")
	base := func(input string) string {
		t.Helper()
		members, err := structured_dictionary(input)
		if err != nil || len(members) != 1 {
			t.Fatalf("signature input %q: %v", input, err)
		}
		text, err := signature_base(r, members[0])
		if err != nil {
			t.Fatalf("signature base: %v", err)
		}
		return text
	}

	want := `"@method": POST
"@authority": example.com
"@path": /foo
"content-digest": sha-512=:WZDPaVn/7XgHaAy8pmojAkGWoRx2UFChF41A2svX+TaPm+AbwAgBWnrIiYllu7BNNyealdVLvRwEmTHWXvJwew==:
"content-length": 18
"content-type": application/json
"@signature-params": ("@method" "@authority" "@path" "content-digest" "content-length" "content-type");created=1618884473;keyid="test-key-rsa-pss"`
	if got := base(`sig1=("@method" "@authority" "@path" "content-digest" "content-length" "content-type");created=1618884473;keyid="test-key-rsa-pss"`); got != want {
		t.Errorf("signature base:\n%s\nwant:\n%s", got, want)
	}
	if err := signature_digest(r, []byte(`{"hello": "world"}`)); err != nil {
		t.Errorf("the RFC's SHA-512 content digest should match its body: %v", err)
	}

	curve := `sig-b26=("date" "@method" "@path" "@authority" "content-type" "content-length");created=1618884473;keyid="test-key-ed25519"`
	signature, _ := base64.StdEncoding.DecodeString("wqcAqbmYJ2ji2glfAMaRy4gruYYnx2nEFN2HN6jrnDnQCK1u02Gb04v9EDgwUPiu4A0w6vuQv5lIp5WPpBKRCw==")
	published := "-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEAJrQLj5P/89iXES9+vFgrIy29clF9CC/oPPsw3c5D0bs=\n-----END PUBLIC KEY-----\n"
	if err := signature_check(published, "", []byte(base(curve)), signature); err != nil {
		t.Errorf("the RFC's ed25519 example should verify: %v", err)
	}

	derived := httptest.NewRequest("POST", "https://www.example.com/path?param=value", nil)
	derived.Host = "www.example.com"
	for name, want := range map[string]string{
		"@method":         "POST",
		"@target-uri":     "https://www.example.com/path?param=value",
		"@authority":      "www.example.com",
		"@scheme":         "https",
		"@request-target": "/path?param=value",
		"@path":           "/path",
		"@query":          "?param=value",
	} {
		if got, err := signature_component(derived, name); err != nil || got != want {
			t.Errorf("%s = %q, %v; want %q", name, got, err, want)
		}
	}
	bare := httptest.NewRequest("GET", "https://Example.COM:443/", nil)
	bare.Host = "Example.COM:443"
	if got, _ := signature_component(bare, "@authority"); got != "example.com" {
		t.Errorf("@authority should be lowercased without the default port: %q", got)
	}
	if got, _ := signature_component(bare, "@query"); got != "?" {
		t.Errorf("@query with no query should be ?: %q", got)
	}
	bare.Header.Add("Cache-Control", "max-age=60")
	bare.Header.Add("Cache-Control", "   must-revalidate ")
	if got, _ := signature_component(bare, "cache-control"); got != "max-age=60, must-revalidate" {
		t.Errorf("a header's values should be trimmed and joined: %q", got)
	}
	if _, err := signature_component(bare, "@status"); err == nil || !strings.Contains(err.Error(), "unsupported component @status") {
		t.Errorf("an unsupported derived component should be refused: %v", err)
	}
	if _, err := signature_component(bare, "content-digest"); err == nil {
		t.Error("a header the request does not carry should be refused")
	}
}

// TestStructuredDictionary: a dictionary's members are read with their inner
// lists, parameters and serialised text, a comma or escaped quote inside a
// string does not end a member, a repeated name takes its last value, a byte
// sequence may leave off its padding, and a malformed field is refused.
func TestStructuredDictionary(t *testing.T) {
	members, err := structured_dictionary(`sig1=("@method" "@target-uri");created=1618884473;keyid="https://remote.example/a,b\"c";alg=token, sig2=:AAEC:, sig2=:AAECAw==:,flag`)
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 3 || members[0].name != "sig1" || members[1].name != "sig2" || members[2].name != "flag" {
		t.Fatalf("members: %+v", members)
	}
	input := members[0]
	if !input.inner || len(input.list) != 2 || input.list[0].value != "@method" || input.list[1].value != "@target-uri" {
		t.Errorf("inner list: %+v", input.list)
	}
	if input.item.parameters["created"] != int64(1618884473) || input.item.parameters["keyid"] != `https://remote.example/a,b"c` || input.item.parameters["alg"] != structured_token("token") {
		t.Errorf("parameters: %+v", input.item.parameters)
	}
	if input.text != `("@method" "@target-uri");created=1618884473;keyid="https://remote.example/a,b\"c";alg=token` {
		t.Errorf("text: %q", input.text)
	}
	if value, _ := members[1].item.value.([]byte); !bytes.Equal(value, []byte{0, 1, 2, 3}) {
		t.Errorf("a repeated name should take its last value: %v", members[1].item.value)
	}
	if members, err := structured_dictionary(`a=:AAECAw:`); err != nil || !bytes.Equal(members[0].item.value.([]byte), []byte{0, 1, 2, 3}) {
		t.Errorf("a byte sequence without its padding should be read: %v", err)
	}
	if members[2].item.value != true {
		t.Errorf("a name with no value is true: %v", members[2].item.value)
	}
	for _, field := range []string{`a=1,`, `a="unterminated`, `A=1`, `=1`, `a=(1 2`, `a=1 b=2`, `a=:not base64!:`} {
		if _, err := structured_dictionary(field); err == nil {
			t.Errorf("%q should be refused", field)
		}
	}
}

// TestSignatureMessageRefresh: an RFC 9421 signature that fails against the
// remembered key is tried against the key the actor publishes now, as a
// draft-cavage one is.
func TestSignatureMessageRefresh(t *testing.T) {
	actor := "https://remote.example/users/bob"
	key_id := actor + "#main-key"
	document := func(public string) string {
		encoded, _ := json.Marshal(map[string]any{
			"id":        actor,
			"publicKey": map[string]any{"id": key_id, "owner": actor, "publicKeyPem": public},
		})
		return string(encoded)
	}
	old_public, _, _ := key_generate(key_rsa)
	new_public, new_private, _ := key_generate(key_rsa)
	d := resolve_test_install(t, map[string]string{actor: document(old_public)})
	signature_keys_test_reset(t)
	ctx := context.Background()
	body := []byte(`{"type":"Follow"}`)

	if public, err := signature_key_fetch(ctx, key_id, false); err != nil || public != old_public {
		t.Fatalf("the key is not remembered: %v", err)
	}
	d.documents[actor] = document(new_public)

	r := signature_message_test_request(body)
	signature_message_test_sign(r, "sig1", [][2]string{{"@method", "POST"}, {"@target-uri", "https://remote.example/inbox"}, {"content-digest", r.Header.Get("Content-Digest")}},
		fmt.Sprintf(`;created=%d;keyid="%s";alg="rsa-v1_5-sha256"`, time.Now().Unix(), key_id), func(base []byte) []byte {
			signature, _ := key_sign_rsa(new_private, base)
			return signature
		})
	if _, err := signature_verify(ctx, r, body, signature_key_fetch); err != nil || d.fetches != 2 {
		t.Errorf("a request signed with the actor's new key should verify after one fetch: %v, fetches = %d", err, d.fetches)
	}
}

// TestSignatureKeyFetch: the key is read from the actor document the keyId
// names, by key id or as the document's only key, and cached.
func TestSignatureKeyFetch(t *testing.T) {
	public, _, _ := key_generate(key_rsa)
	encoded, _ := json.Marshal(map[string]any{
		"id":        "https://remote.example/users/bob",
		"publicKey": map[string]any{"id": "https://remote.example/users/bob#main-key", "owner": "https://remote.example/users/bob", "publicKeyPem": public},
	})
	d := resolve_test_install(t, map[string]string{"https://remote.example/users/bob": string(encoded)})
	signature_keys_lock.Lock()
	signature_keys = map[string]signature_key_entry{}
	signature_keys_lock.Unlock()

	got, err := signature_key_fetch(context.Background(), "https://remote.example/users/bob#main-key", false)
	if err != nil || got != public {
		t.Fatalf("fetched key: %v", err)
	}
	if _, err := signature_key_fetch(context.Background(), "https://remote.example/users/bob#main-key", false); err != nil || d.fetches != 1 {
		t.Errorf("a second fetch should come from the cache: fetches = %d", d.fetches)
	}
	if _, err := signature_key_fetch(context.Background(), "https://remote.example/users/nobody#main-key", false); err == nil {
		t.Error("an actor that does not exist has no key")
	}
	if _, err := signature_key_fetch(context.Background(), "http://remote.example/users/bob#main-key", false); err == nil {
		t.Error("a keyId that is not https is refused")
	}
}

// signature_keys_test_reset empties the fetched-key cache for a test and again
// after it.
func signature_keys_test_reset(t *testing.T) {
	reset := func() {
		signature_keys_lock.Lock()
		signature_keys = map[string]signature_key_entry{}
		signature_keys_lock.Unlock()
	}
	reset()
	t.Cleanup(reset)
}

// TestSignatureKeyRefresh: a signature that fails against the remembered key
// is tried against the key the actor publishes now, so an actor that replaced
// its key is heard at once; forged signatures cost its server one fetch a
// minute, and a re-read that fails keeps the key the actor had.
func TestSignatureKeyRefresh(t *testing.T) {
	actor := "https://remote.example/users/bob"
	key_id := actor + "#main-key"
	document := func(public string) string {
		encoded, _ := json.Marshal(map[string]any{
			"id":        actor,
			"publicKey": map[string]any{"id": key_id, "owner": actor, "publicKeyPem": public},
		})
		return string(encoded)
	}
	old_public, _, _ := key_generate(key_rsa)
	new_public, new_private, _ := key_generate(key_rsa)
	_, forged_private, _ := key_generate(key_rsa)
	d := resolve_test_install(t, map[string]string{actor: document(old_public)})
	signature_keys_test_reset(t)
	ctx := context.Background()
	body := []byte(`{"type":"Follow"}`)
	sign := func(private string) *http.Request {
		r := httptest.NewRequest("POST", "https://here.example/inbox", bytes.NewReader(body))
		r.Host = "here.example"
		if err := signature_sign(r, key_id, private, body); err != nil {
			t.Fatal(err)
		}
		return r
	}
	// age makes the last fresh read of the key a refresh interval old.
	age := func() {
		signature_keys_lock.Lock()
		entry := signature_keys[key_id]
		entry.refreshed = time.Now().Add(-signature_key_refresh)
		signature_keys[key_id] = entry
		signature_keys_lock.Unlock()
	}

	if public, err := signature_key_fetch(ctx, key_id, false); err != nil || public != old_public {
		t.Fatalf("the key is not remembered: %v", err)
	}
	d.documents[actor] = document(new_public)

	if _, err := signature_verify(ctx, sign(new_private), body, signature_key_fetch); err != nil || d.fetches != 2 {
		t.Fatalf("a request signed with the actor's new key should verify after one fetch: %v, fetches = %d", err, d.fetches)
	}
	if _, err := signature_verify(ctx, sign(new_private), body, signature_key_fetch); err != nil || d.fetches != 2 {
		t.Errorf("the new key should be remembered: %v, fetches = %d", err, d.fetches)
	}

	if _, err := signature_verify(ctx, sign(forged_private), body, signature_key_fetch); err == nil || d.fetches != 2 {
		t.Errorf("a forged signature within a minute of the last re-read should be refused with no fetch: %v, fetches = %d", err, d.fetches)
	}
	age()
	if _, err := signature_verify(ctx, sign(forged_private), body, signature_key_fetch); err == nil || d.fetches != 3 {
		t.Errorf("a forged signature should be refused after one re-read: %v, fetches = %d", err, d.fetches)
	}

	age()
	delete(d.documents, actor)
	if _, err := signature_verify(ctx, sign(forged_private), body, signature_key_fetch); err == nil || d.fetches != 4 {
		t.Errorf("a forged signature should be refused, the actor's document gone: %v, fetches = %d", err, d.fetches)
	}
	if _, err := signature_verify(ctx, sign(new_private), body, signature_key_fetch); err != nil || d.fetches != 4 {
		t.Errorf("a re-read that failed should keep the key the actor had: %v, fetches = %d", err, d.fetches)
	}
}

// TestSignatureKeyFailures: a refusal by our own limiter is not remembered, and
// a failure remembered from an unsigned fetch does not bind a signed one, while
// a signed one's failure does.
func TestSignatureKeyFailures(t *testing.T) {
	public, _, _ := key_generate(key_rsa)
	encoded, _ := json.Marshal(map[string]any{
		"id":        "https://remote.example/users/bob",
		"publicKey": map[string]any{"id": "https://remote.example/users/bob#main-key", "owner": "https://remote.example/users/bob", "publicKeyPem": public},
	})
	d := resolve_test_install(t, map[string]string{"https://remote.example/users/bob": string(encoded)})
	signature_keys_test_reset(t)
	limited := true
	secure := map[string]bool{}
	resolve_fetch = func(ctx context.Context, address string, accept string) ([]byte, error) {
		if limited {
			limited = false
			return nil, &RateLimitError{Retry: 60}
		}
		if secure[address] && resolve_signed_by(ctx) == nil {
			d.fetches++
			return nil, &resolve_refusal{Status: 401}
		}
		return d.fetch(ctx, address, accept)
	}
	ctx := context.Background()
	key_id := "https://remote.example/users/bob#main-key"

	if _, err := signature_key_fetch(ctx, key_id, false); err == nil {
		t.Fatal("the limiter's refusal should fail the fetch")
	}
	if got, err := signature_key_fetch(ctx, key_id, false); err != nil || got != public {
		t.Errorf("a refusal by our own limiter should not be remembered: %v", err)
	}

	secured := "https://secure.example/users/carol"
	secure[secured] = true
	d.documents[secured] = strings.Replace(string(encoded), "https://remote.example/users/bob", secured, -1)
	if _, err := signature_key_fetch(ctx, secured+"#main-key", false); err == nil {
		t.Fatal("a server in secure mode should refuse an unsigned fetch")
	}
	signing := resolve_signing(ctx, func(r *http.Request) {})
	if got, err := signature_key_fetch(signing, secured+"#main-key", false); err != nil || got != public {
		t.Errorf("an unsigned fetch's failure should not bind a signed one: %v", err)
	}

	before := d.fetches
	if _, err := signature_key_fetch(signing, "https://remote.example/users/nobody#main-key", false); err == nil {
		t.Fatal("an actor that does not exist has no key")
	}
	if _, err := signature_key_fetch(signing, "https://remote.example/users/nobody#main-key", false); err == nil || d.fetches != before+1 {
		t.Errorf("a signed fetch's failure should be remembered: %v, fetches = %d", err, d.fetches-before)
	}
}

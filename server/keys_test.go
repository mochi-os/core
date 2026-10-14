// Mochi server: Tests for the keys of hosted accounts
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"context"
	"crypto/elliptic"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
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
	if err := keypairs_replace(alice, "nothing"); err == nil {
		t.Error("an account without keys cannot be replaced")
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
	fetch := func(ctx context.Context, id string) (string, error) {
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
	if _, err := signature_verify(context.Background(), sign(body), body, func(context.Context, string) (string, error) { return other_public, nil }); err == nil {
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

	got, err := signature_key_fetch(context.Background(), "https://remote.example/users/bob#main-key")
	if err != nil || got != public {
		t.Fatalf("fetched key: %v", err)
	}
	if _, err := signature_key_fetch(context.Background(), "https://remote.example/users/bob#main-key"); err != nil || d.fetches != 1 {
		t.Errorf("a second fetch should come from the cache: fetches = %d", d.fetches)
	}
	if _, err := signature_key_fetch(context.Background(), "https://remote.example/users/nobody#main-key"); err == nil {
		t.Error("an actor that does not exist has no key")
	}
	if _, err := signature_key_fetch(context.Background(), "http://remote.example/users/bob#main-key"); err == nil {
		t.Error("a keyId that is not https is refused")
	}
}

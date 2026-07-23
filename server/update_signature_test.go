// Mochi server: update manifest signature tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"crypto/ed25519"
	"encoding/base64"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// verify_against checks a manifest and signature against an arbitrary public
// key, mirroring update_manifest_verify's logic so a test can supply its own
// key rather than the pinned one. Kept in step with update_manifest_verify.
func verify_against(public ed25519.PublicKey, manifest, signature []byte) bool {
	sig, err := base64.StdEncoding.DecodeString(string(signature))
	if err != nil {
		return false
	}
	return ed25519.Verify(public, manifest, sig)
}

// TestManifestVerifyRejectsSubstitution pins the property that matters: a
// manifest edited after signing, or signed by a key other than the release
// key, must not verify. The digests inside a manifest only bind an artifact to
// the manifest, so a host compromise could rewrite both together; the
// signature is what a compromised host cannot forge.
func TestManifestVerifyRejectsSubstitution(t *testing.T) {
	public, private, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	manifest := []byte(`{"tracks":{"production":"0.4.217"}}`)
	signature := []byte(base64.StdEncoding.EncodeToString(ed25519.Sign(private, manifest)))

	if !verify_against(public, manifest, signature) {
		t.Fatal("a correctly signed manifest did not verify")
	}

	tampered := []byte(`{"tracks":{"production":"6.6.6"}}`)
	if verify_against(public, tampered, signature) {
		t.Error("a manifest altered after signing verified anyway")
	}

	_, attacker, _ := ed25519.GenerateKey(nil)
	wrong := []byte(base64.StdEncoding.EncodeToString(ed25519.Sign(attacker, manifest)))
	if verify_against(public, manifest, wrong) {
		t.Error("a manifest signed by the wrong key verified against the release key")
	}
}

// TestManifestVerifyFailsClosed pins that malformed input to the real
// verifier fails rather than panics or passes. update_manifest_verify uses the
// pinned key, so these must be rejections independent of any signer.
func TestManifestVerifyFailsClosed(t *testing.T) {
	manifest := []byte(`{"tracks":{"production":"0.4.217"}}`)
	if update_manifest_verify(manifest, []byte("not base64 !!!")) == nil {
		t.Error("a non-base64 signature was accepted")
	}
	if update_manifest_verify(manifest, []byte(base64.StdEncoding.EncodeToString([]byte("short")))) == nil {
		t.Error("a signature of the wrong length was accepted")
	}
	if update_manifest_verify(manifest, nil) == nil {
		t.Error("an empty signature was accepted")
	}
}

// TestPinnedKeyIsValid pins that the key compiled into the binary is a usable
// ed25519 public key. A typo would make every update check on every server
// fail closed — safe, but total.
func TestPinnedKeyIsValid(t *testing.T) {
	key, err := base64.StdEncoding.DecodeString(update_manifest_public_key)
	if err != nil {
		t.Fatalf("pinned key is not base64: %v", err)
	}
	if len(key) != ed25519.PublicKeySize {
		t.Fatalf("pinned key is %d bytes, want %d", len(key), ed25519.PublicKeySize)
	}
}

// TestReleaseSigningRoundTrip is the real end-to-end check: the private key in
// core/local signs a manifest through openssl exactly as the release does, and
// the pinned public key verifies it through the server's own code. If signer
// and verifier ever disagree on format, every real update check breaks; this
// catches it here rather than in production. Skipped where the key or openssl
// is absent, which is anywhere but the release machine.
func TestReleaseSigningRoundTrip(t *testing.T) {
	key := filepath.Join("..", "local", "update-signing.key")
	if _, err := exec.LookPath("openssl"); err != nil {
		t.Skip("openssl not available")
	}
	if _, err := os.Stat(key); err != nil {
		t.Skip("release signing key not present (expected off the release machine)")
	}

	manifest := []byte(`{"tracks":{"production":"0.4.217"},"releases":{}}`)
	path := filepath.Join(t.TempDir(), "versions.json")
	if err := os.WriteFile(path, manifest, 0o600); err != nil {
		t.Fatal(err)
	}

	raw, err := exec.Command("openssl", "pkeyutl", "-sign", "-rawin", "-inkey", key, "-in", path).Output()
	if err != nil {
		t.Fatalf("openssl signing failed: %v", err)
	}
	signature := []byte(base64.StdEncoding.EncodeToString(raw))

	if err := update_manifest_verify(manifest, signature); err != nil {
		t.Errorf("a manifest signed by the real release key did not verify with the pinned key: %v", err)
	}
}

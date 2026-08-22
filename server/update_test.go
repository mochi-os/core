// Mochi server: Update tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestUpdateInstallDownloadVerifies: the artifact goes straight to msiexec as
// LocalSystem, so anything not matching the manifest must be rejected and the
// partial removed.
func TestUpdateInstallDownloadVerifies(t *testing.T) {
	body := []byte("pretend this is an MSI")
	sum := sha256.Sum256(body)
	digest := hex.EncodeToString(sum[:])

	serve := func(payload []byte) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Write(payload)
		}))
	}

	tests := []struct {
		name    string
		payload []byte
		release update_release
		wants   bool // download should succeed
	}{
		{
			name:    "matching size and digest",
			payload: body,
			release: update_release{File: "x.msi", Size: int64(len(body)), Sha256: digest},
			wants:   true,
		},
		{
			name:    "digest mismatch",
			payload: []byte("substituted body!!!!!!"), // same length, different bytes
			release: update_release{File: "x.msi", Size: int64(len(body)), Sha256: digest},
		},
		{
			name:    "body shorter than the manifest",
			payload: body[:5],
			release: update_release{File: "x.msi", Size: int64(len(body)), Sha256: digest},
		},
		{
			name:    "body longer than the manifest",
			payload: append(append([]byte{}, body...), []byte("extra")...),
			release: update_release{File: "x.msi", Size: int64(len(body)), Sha256: digest},
		},
		{
			name:    "manifest size above the ceiling",
			payload: body,
			release: update_release{File: "x.msi", Size: update_artifact_maximum + 1, Sha256: digest},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := serve(test.payload)
			defer server.Close()

			dest := filepath.Join(t.TempDir(), "mochi-server.msi")
			err := update_install_download(server.URL, dest, test.release)

			if test.wants {
				if err != nil {
					t.Fatalf("expected success, got: %v", err)
				}
				got, read_error := os.ReadFile(dest)
				if read_error != nil {
					t.Fatalf("read destination: %v", read_error)
				}
				if string(got) != string(body) {
					t.Errorf("destination holds %q, want %q", got, body)
				}
			} else {
				if err == nil {
					t.Fatal("expected rejection, got success")
				}
				if _, stat_error := os.Stat(dest); !os.IsNotExist(stat_error) {
					t.Errorf("rejected artifact was left at %s", dest)
				}
			}

			// The partial must never survive, on either path.
			if _, stat_error := os.Stat(dest + ".part"); !os.IsNotExist(stat_error) {
				t.Errorf("partial download was left at %s.part", dest)
			}
		})
	}
}

// TestUpdatePermission pins server/update as restricted and administrator-only.
// It replaces the running binary and restarts the service, so an app must not
// reach it just because a user happens to have that app installed.
func TestUpdatePermission(t *testing.T) {
	if !permission_restricted("server/update") {
		t.Error("server/update should be restricted")
	}
	if !permission_administrator("server/update") {
		t.Error("server/update should require an administrator")
	}
}

// Mochi server: RPM repository definition tests
// TestRPMRepoRequiresVerification pins signature verification in the canonical
// RPM repo definition. release-publish copies this file from source, so it is
// the single source of truth for whether dnf verifies Mochi packages.
func TestRPMRepoRequiresVerification(t *testing.T) {
	path := filepath.Join("..", "build", "rpm", "mochi.repo")
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	text := string(body)

	for _, required := range []string{
		"gpgcheck=1",      // verify each package's signature
		"repo_gpgcheck=1", // verify the signed metadata
		"gpgkey=https://packages.mochi-os.org/mochi.asc", // against the published key
	} {
		if !strings.Contains(text, required) {
			t.Errorf("mochi.repo is missing %q: RPM signature verification would be off", required)
		}
	}

	// Guard the inverse explicitly: a stray gpgcheck=0 anywhere disables it
	// regardless of the line above.
	if strings.Contains(text, "gpgcheck=0") {
		t.Error("mochi.repo contains gpgcheck=0, which disables signature verification")
	}
}

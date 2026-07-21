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
	"testing"
)

// TestUpdateInstallDownloadVerifies pins the integrity check on the
// self-install download. The artifact goes straight to msiexec, which runs it
// as LocalSystem with no verification of its own, so anything that does not
// match the manifest exactly must be rejected AND removed — a rejected
// artifact left on disk could be mistaken for a good download later.
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
				got, readErr := os.ReadFile(dest)
				if readErr != nil {
					t.Fatalf("read destination: %v", readErr)
				}
				if string(got) != string(body) {
					t.Errorf("destination holds %q, want %q", got, body)
				}
			} else {
				if err == nil {
					t.Fatal("expected rejection, got success")
				}
				if _, statErr := os.Stat(dest); !os.IsNotExist(statErr) {
					t.Errorf("rejected artifact was left at %s", dest)
				}
			}

			// The partial must never survive, on either path.
			if _, statErr := os.Stat(dest + ".part"); !os.IsNotExist(statErr) {
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

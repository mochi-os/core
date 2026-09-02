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
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
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

// TestUpdateManifestFreshness. The signature binds the manifest to the release
// key; these checks bind it to now, so a host that can only replay old signed
// manifests cannot hold a server on a release it has already moved past. Age
// alone is reported, not refused: nothing re-signs a manifest between releases.
func TestUpdateManifestFreshness(t *testing.T) {
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	day := 24 * time.Hour
	manifest := func(generated time.Time, platform string) *update_versions {
		return &update_versions{Generated: generated.Unix(), Platform: platform, Tracks: map[string]string{"production": "0.4.250"}}
	}
	cases := []struct {
		name     string
		manifest *update_versions
		accepted int64
		stale    bool
		refused  string
	}{
		{"fresh", manifest(now.Add(-day), "apt"), 0, false, ""},
		{"newer than the last accepted", manifest(now.Add(-day), "apt"), now.Add(-2 * day).Unix(), false, ""},
		{"same as the last accepted", manifest(now.Add(-day), "apt"), now.Add(-day).Unix(), false, ""},
		{"within clock skew", manifest(now.Add(time.Hour), "apt"), 0, false, ""},
		{"old is reported, not refused", manifest(now.Add(-61*day), "apt"), 0, true, ""},
		{"no generation time", &update_versions{Platform: "apt"}, 0, false, "no generation time"},
		{"another platform", manifest(now.Add(-day), "rpm"), 0, false, "platform"},
		{"older than the last accepted", manifest(now.Add(-3*day), "apt"), now.Add(-day).Unix(), false, "older than the last accepted"},
		{"from the future", manifest(now.Add(2*day), "apt"), 0, false, "in the future"},
	}
	for _, c := range cases {
		stale, err := update_manifest_fresh(c.manifest, "apt", now, c.accepted)
		if c.refused == "" {
			if err != nil {
				t.Errorf("%s: refused: %v", c.name, err)
				continue
			}
			if stale != c.stale {
				t.Errorf("%s: stale = %v, want %v", c.name, stale, c.stale)
			}
			continue
		}
		if err == nil || !strings.Contains(err.Error(), c.refused) {
			t.Errorf("%s: err = %v, want %q", c.name, err, c.refused)
		}
	}
}

// TestUpdateInstallCommandRestartsTheService. The MSI declares no failure
// actions, so nothing but this command line brings the server back when an
// upgrade rolls back; it must start the service after msiexec, unconditionally.
func TestUpdateInstallCommandRestartsTheService(t *testing.T) {
	msi := `C:\ProgramData\Mochi\data\tmp\mochi-server-0.4.250.msi`
	command := update_install_command(msi, msi+".log")
	wait := "ping -n " + strconv.Itoa(update_install_pre_wait+1) + " 127.0.0.1"
	install := `msiexec /i "` + msi + `" /quiet /norestart /l*v "` + msi + `.log"`
	restart := " & sc start mochi-server"
	for _, part := range []string{wait, install, restart} {
		if !strings.Contains(command, part) {
			t.Errorf("command lacks %q:\n%s", part, command)
		}
	}
	if !strings.HasSuffix(command, restart) {
		t.Errorf("the service start is not the last step:\n%s", command)
	}
	if strings.Contains(command, "&&") {
		t.Errorf("the service start is conditional on msiexec succeeding, so a rolled-back install leaves the server stopped:\n%s", command)
	}
}

// TestUpdateInstallLaunchRefusesATamperedArtifact. msiexec reads the file
// seconds after this service has exited, so the launch re-hashes it against
// the manifest and refuses anything else before the spawn. Off Windows the
// spawn stub refuses, which is how the test sees that every check passed.
func TestUpdateInstallLaunchRefusesATamperedArtifact(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the spawn would run msiexec")
	}
	path := filepath.Join(t.TempDir(), "mochi-server-0.4.250.msi")
	body := []byte("pretend this is an MSI")
	sum := sha256.Sum256(body)
	release := update_release{File: "mochi-server-0.4.250.msi", Size: int64(len(body)), Sha256: hex.EncodeToString(sum[:])}
	launch := func(content []byte) error {
		if content == nil {
			os.Remove(path)
		} else if err := os.WriteFile(path, content, 0o600); err != nil {
			t.Fatal(err)
		}
		return update_install_launch(path, path+".log", release)
	}
	if err := launch(body); err == nil || !strings.Contains(err.Error(), "self-install not supported") {
		t.Fatalf("intact artifact: err = %v, want the platform stub's refusal after the checks", err)
	}
	tampered := append([]byte{}, body...)
	tampered[0] ^= 1
	if err := launch(tampered); err == nil || !strings.Contains(err.Error(), "sha256") {
		t.Errorf("same-length tamper: err = %v, want a digest refusal", err)
	}
	if err := launch(append(append([]byte{}, body...), '!')); err == nil || !strings.Contains(err.Error(), "size") {
		t.Errorf("longer file: err = %v, want a size refusal", err)
	}
	if err := launch(nil); err == nil || strings.Contains(err.Error(), "self-install not supported") {
		t.Errorf("missing file: err = %v, want a refusal before the spawn", err)
	}
}

// Mochi server: RPM repository definition tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestRPMRepoRequiresVerification pins the security-relevant settings in the
// canonical RPM repo definition, so a change that silently disables signature
// verification is caught in review rather than in a package a user installs.
//
// The file is published from source at release (release-publish copies it into
// the untracked packages tree), so this is now the single source of truth for
// whether dnf verifies Mochi packages.
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

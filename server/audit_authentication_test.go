// Mochi server: the audit event for a sign-in change is named for what changed.
// Mochi has no password login, so an event named "password_changed" names a
// credential that does not exist and hides every real change behind it.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestNoPasswordLoginExists is the premise. If Mochi ever grows a password
// factor, the rename below stops being obviously right and this fires first.
func TestNoPasswordLoginExists(t *testing.T) {
	for _, method := range auth_method_list {
		if method == "password" {
			t.Fatal("auth_method_list now contains a password factor, so an audit event named for one would be meaningful again")
		}
	}
	if len(auth_method_list) == 0 {
		t.Fatal("auth_method_list is empty; this test is reading the wrong thing")
	}
}

// TestNoAuditEventNamesAPassword is the defect: an operator-facing event name
// for a credential that does not exist.
func TestNoAuditEventNamesAPassword(t *testing.T) {
	sources, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("listing sources: %v", err)
	}
	for _, path := range sources {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		text, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading %s: %v", path, err)
		}
		// The token, not the bare word: accounts.go legitimately declares
		// Type: "password" for connected-account fields, which is the HTML
		// input type and is the correct external name for that.
		for _, token := range []string{"password_changed", "audit_password"} {
			if strings.Contains(string(text), token) {
				t.Errorf("%s still carries %q; Mochi has no password login, so the audit trail names a credential that does not exist", path, token)
			}
		}
	}
}

// audit_event_string extracts the format literal a named audit helper emits.
func audit_event_string(t *testing.T, path, function string) string {
	t.Helper()
	text, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	at := strings.Index(string(text), "func "+function+"(")
	if at < 0 {
		t.Fatalf("%s does not define %s", path, function)
	}
	rest := string(text)[at:]
	if end := strings.Index(rest, "\n}\n"); end > 0 {
		rest = rest[:end]
	}
	match := regexp.MustCompile(`"([a-z_]+ user=[^"]*)"`).FindStringSubmatch(rest)
	if match == nil {
		t.Fatalf("%s %s emits no recognisable audit line", path, function)
	}
	return match[1]
}

// TestBothPlatformsEmitTheSameAuthenticationEvent is the drift guard:
// audit_windows.go is behind //go:build windows, so no Linux build ever
// compiles it and the two files drift unnoticed.
func TestBothPlatformsEmitTheSameAuthenticationEvent(t *testing.T) {
	unix := audit_event_string(t, "audit_unix.go", "audit_authentication_changed")
	windows := audit_event_string(t, "audit_windows.go", "audit_authentication_changed")

	if unix != windows {
		t.Errorf("the platforms emit different audit lines:\n  unix:    %q\n  windows: %q\nan operator's search finds only one of them", unix, windows)
	}
	if !strings.HasPrefix(unix, "authentication_changed ") {
		t.Errorf("the audit line is %q; it must lead with the event name an operator searches for", unix)
	}
}

// TestEveryAuthenticationChangeIsStillAudited pins the ten call sites. A rename
// that quietly drops one leaves that change unrecorded, which is worse than the
// misleading name it replaced.
func TestEveryAuthenticationChangeIsStillAudited(t *testing.T) {
	expected := map[string][]string{
		"authentication.go": {
			"methods_changed", "methods_changed", "admin_reset",
			"totp_enabled", "totp_enabled", "totp_disabled",
			"recovery_regenerated",
		},
		"oauth.go":    {`"oauth_unlinked_"+provider`},
		"passkeys.go": {"passkey_registered", "passkey_deleted"},
	}

	total := 0
	for path, methods := range expected {
		text, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading %s: %v", path, err)
		}
		calls := strings.Count(string(text), "audit_authentication_changed(")
		if calls != len(methods) {
			t.Errorf("%s makes %d audit calls, want %d", path, calls, len(methods))
		}
		total += calls
	}
	if total != 10 {
		t.Errorf("%d authentication changes are audited across the server, want 10", total)
	}
}

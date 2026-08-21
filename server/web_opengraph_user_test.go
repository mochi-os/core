// Mochi server: OpenGraph rendering runs as the viewer, on the owner's data.
//
// Who is asking and whose data is read are separate questions; `storage` is
// what answers the second, and these tests pin both halves.//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"strings"
	"testing"

	sl "go.starlark.net/starlark"
)

// opengraph_test_users returns an owner and an unrelated authenticated
// viewer. The viewer owns nothing in the app under test.
func opengraph_test_users() (*User, *User) {
	return &User{UID: "u-owner", Username: "owner@example.com"},
		&User{UID: "u-viewer", Username: "viewer@example.com"}
}

func TestStorageOverridesTheCallerForDatabaseReads(t *testing.T) {
	owner, viewer := opengraph_test_users()

	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("owner", owner)
	thread.SetLocal("user", viewer)
	thread.SetLocal("storage", owner)

	resolved, err := principal_storage(thread)
	if err != nil {
		t.Fatalf("principal_storage: %v", err)
	}
	if resolved.UID != owner.UID {
		t.Errorf("reads resolved to %q, want the storage account %q - the OpenGraph handler would read the viewer's own database and find nothing",
			resolved.UID, owner.UID)
	}
}

// TestStorageAppliesWithNoCallerAtAll. The anonymous crawler is the primary
// audience; it must reach the owner's data with no user bound whatsoever.
func TestStorageAppliesWithNoCallerAtAll(t *testing.T) {
	owner, _ := opengraph_test_users()

	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("owner", owner)
	thread.SetLocal("storage", owner)

	resolved, err := principal_storage(thread)
	if err != nil {
		t.Fatalf("principal_storage: %v", err)
	}
	if resolved.UID != owner.UID {
		t.Errorf("anonymous read resolved to %q, want %q", resolved.UID, owner.UID)
	}
}

// TestStorageAbsentLeavesResolutionUnchanged. Every other caller must be
// untouched: with no storage local the caller still wins, which is what the
// action path relies on.
func TestStorageAbsentLeavesResolutionUnchanged(t *testing.T) {
	owner, viewer := opengraph_test_users()

	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("owner", owner)
	thread.SetLocal("user", viewer)

	resolved, err := principal_storage(thread)
	if err != nil {
		t.Fatalf("principal_storage: %v", err)
	}
	if resolved.UID != viewer.UID {
		t.Errorf("with no storage local, reads resolved to %q, want the caller %q", resolved.UID, viewer.UID)
	}
}

// TestOpenGraphDoesNotImpersonateTheOwner is the finding. The handler must
// bind the real requester, falling back to the owner only when there is no
// requester - the same rule web_action applies to a public action.
func TestOpenGraphDoesNotImpersonateTheOwner(t *testing.T) {
	body := opengraph_handler_source(t)

	if strings.Contains(body, `s.set("user", owner)`) {
		t.Error("OpenGraph still binds user to the entity owner: an authenticated stranger is handed the owner's identity, and any handler gating on ownership grants")
	}
	if !strings.Contains(body, `s.set("user", user)`) {
		t.Error("OpenGraph does not bind the real caller")
	}
	if !strings.Contains(body, `s.set("storage", owner)`) {
		t.Error("OpenGraph does not pin reads to the owner's storage; a logged-in viewer would render from their own database")
	}
	if !strings.Contains(body, "web_auth(c)") {
		t.Error("OpenGraph never resolves the requesting user, so it cannot tell a stranger from the owner")
	}
}

// opengraph_handler_source returns the body of web_serve_file_with_opengraph.
// The identity binding has no observable output of its own - the meta tags
// look the same either way - so the ordering is pinned at the source.
func opengraph_handler_source(t *testing.T) string {
	t.Helper()
	source, err := os.ReadFile("web.go")
	if err != nil {
		t.Fatalf("read web.go: %v", err)
	}
	body := string(source)
	start := strings.Index(body, "func web_serve_file_with_opengraph(")
	if start < 0 {
		t.Fatal("web_serve_file_with_opengraph not found")
	}
	body = body[start:]
	end := strings.Index(body, "\n}\n")
	if end < 0 {
		t.Fatal("could not find the end of web_serve_file_with_opengraph")
	}
	return body[:end]
}

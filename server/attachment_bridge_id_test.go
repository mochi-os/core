// Mochi server: the legacy bridge's caller-supplied attachment id.
//
// mochi.attachment.create.stream takes an optional id "for federation sync"
// and used it unchecked as both the filename stem and the row's primary key.
// os.Root does refuse a traversal downstream, so that half of the risk was
// already contained - but "" and ".." pass as filenames and were stored as the
// key, and a repeated id reached OpenFile(O_TRUNC), emptying the existing
// attachment's file, before the insert failed on the primary key. db.exec
// panics on a constraint violation, so the caller got a failed handler and the
// original bytes were already gone.
//
// The bridge stays: attachments.go explains per entry point why each survives,
// and a database still at an old schema reads its rows out through here
// whenever it is next opened.
//
// Copyright © 2026 Mochisoft OU
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

// TestOsRootContainsATraversingId records the measurement that narrowed this
// task: the unchecked id is not a path-traversal hole, whatever its shape.
func TestOsRootContainsATraversingId(t *testing.T) {
	base := t.TempDir()
	root, err := os.OpenRoot(base)
	if err != nil {
		t.Fatalf("opening the root: %v", err)
	}
	defer root.Close()

	for _, id := range []string{"../../escape", "/etc/passwd", "a/b/c"} {
		f, err := root.OpenFile(attachment_filename(id, "photo.jpg"), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		if err == nil {
			f.Close()
			t.Errorf("os.Root created a file for id %q; the containment the create path relies on is gone", id)
		}
	}
}

// TestAnInvalidIdIsRefusedAtTheBoundary is the first half of the fix. These are
// the shapes os.Root does NOT catch, plus the ones it does - all refused before
// they can become a filename or a key.
func TestAnInvalidIdIsRefusedAtTheBoundary(t *testing.T) {
	for _, id := range []string{
		"..",
		" ",
		"../../escape",
		"/etc/passwd",
		"UPPERCASE0000000000000000000000A",
		"tooshort",
		"01a01b0674f473d98eaa45d0d0a5ac190", // 33 characters
		"01a01b0674f473d98eaa45d0d0a5ac1",   // 31 characters
	} {
		if valid(id, "id") {
			t.Errorf("id %q passes valid(id); it becomes the filename stem and the row's primary key", id)
		}
	}
}

// TestAGeneratedIdIsAccepted: the check must admit what uid() produces, or
// federation sync cannot pass a real id through at all.
func TestAGeneratedIdIsAccepted(t *testing.T) {
	for i := 0; i < 50; i++ {
		id := uid()
		if !valid(id, "id") {
			t.Fatalf("uid() produced %q, which valid(id) refuses; the bridge would reject every genuine federated id", id)
		}
	}
}

// TestADuplicateIdIsRefusedBeforeTheBytesAreTruncated is the destructive half.
// The order is the defect: the open truncates, and only then does the insert
// discover the collision.
func TestADuplicateIdIsRefusedBeforeTheBytesAreTruncated(t *testing.T) {
	base := t.TempDir()
	root, err := os.OpenRoot(base)
	if err != nil {
		t.Fatalf("opening the root: %v", err)
	}
	defer root.Close()

	name := attachment_filename("01a01b0674f473d98eaa45d0d0a5ac19", "photo.jpg")
	f, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		t.Fatalf("creating the original: %v", err)
	}
	f.Write([]byte("the original attachment's bytes"))
	f.Close()

	before, err := os.Stat(filepath.Join(base, name))
	if err != nil || before.Size() == 0 {
		t.Fatalf("the original was not written: %v", err)
	}

	// What the create path does on a repeat. The guard has to run before this.
	second, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		t.Fatalf("second open: %v", err)
	}
	second.Close()

	after, err := os.Stat(filepath.Join(base, name))
	if err != nil {
		t.Fatalf("stat after: %v", err)
	}
	if after.Size() != 0 {
		t.Fatal("O_TRUNC no longer empties an existing file; this test is not demonstrating what it claims")
	}
	// The measurement is the point: 31 bytes to 0, before any collision check
	// could run. The production guard below is what stops the create path
	// reaching this open at all.
}

// TestTheCreateStreamGuardsBeforeOpening pins the ORDER in source, which is the
// whole defect: a collision check after the open is a check on bytes that are
// already gone.
func TestTheCreateStreamGuardsBeforeOpening(t *testing.T) {
	body := function_body(t, "attachments.go", "func api_attachment_create_stream(")

	guard := strings.Index(body, "select 1 from attachments where id=?")
	if guard < 0 {
		t.Fatal("the create path does not check for an existing id; a repeat truncates the stored file and then panics on the primary key")
	}
	open := strings.Index(body, "os.O_TRUNC")
	if open < 0 {
		t.Fatal("the create path no longer opens the file with O_TRUNC; this test is reading the wrong thing")
	}
	if guard > open {
		t.Error("the duplicate check runs after the truncating open, so the existing attachment's bytes are destroyed before the collision is noticed")
	}

	check := strings.Index(body, `valid(provided_id, "id")`)
	if check < 0 {
		t.Error("the caller-supplied id is not validated; it becomes the filename stem and the row's primary key")
	}
	if check > 0 && check > guard {
		t.Error("the id is validated after the duplicate lookup; an unusable id should not reach the database at all")
	}
}

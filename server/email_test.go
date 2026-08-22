// Mochi server: Email tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	gm "github.com/wneessen/go-mail"
	"strings"
	"testing"
)

func TestEmailIdentifier(t *testing.T) {
	first := email_identifier("mochi-server@mail.example")
	if !strings.HasSuffix(first, "@mail.example") {
		t.Errorf("identifier %q does not carry the sender domain", first)
	}
	if len(strings.TrimSuffix(first, "@mail.example")) < 16 {
		t.Errorf("identifier %q has too short a unique part", first)
	}
	if second := email_identifier("mochi-server@mail.example"); second == first {
		t.Errorf("identifier %q not unique across calls", first)
	}

	// A display-name sender contributes only its address's domain.
	if id := email_identifier("Mochi <mochi-server@mail.example>"); !strings.HasSuffix(id, "@mail.example") {
		t.Errorf("identifier %q does not carry the sender domain", id)
	}

	// An unparseable sender falls back rather than panicking; the message
	// itself would already have failed to send.
	if id := email_identifier("not-an-address"); !strings.HasSuffix(id, "@localhost") {
		t.Errorf("identifier %q for unparseable sender should fall back to localhost", id)
	}
}

// The identifier must reach the rendered Message-ID header, angle-bracketed,
// in place of the library's hostname-derived default.
func TestEmailIdentifierHeader(t *testing.T) {
	m := gm.NewMsg()
	if err := m.From("mochi-server@mail.example"); err != nil {
		t.Fatalf("From: %v", err)
	}
	m.SetMessageIDWithValue(email_identifier("mochi-server@mail.example"))

	var rendered bytes.Buffer
	if _, err := m.WriteTo(&rendered); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	for _, line := range strings.Split(rendered.String(), "\r\n") {
		if !strings.HasPrefix(line, "Message-ID:") {
			continue
		}
		if !strings.HasSuffix(strings.TrimSpace(line), "@mail.example>") {
			t.Errorf("Message-ID %q right-hand side is not the sender domain", line)
		}
		return
	}
	t.Error("rendered message has no Message-ID header")
}

// Mochi server: email deliverability guard - never attempt delivery to the
// RFC 2606 / 6761 reserved domains, whose mail can only bounce.
func TestEmailDeliverable(t *testing.T) {
	blocked := []string{
		"p2ptest-owner-123@example.com", "x@example.net", "y@example.org",
		"a@sub.example.com", "b@anything.test", "c@host.invalid",
		"d@foo.localhost", "e@bar.example", "nodomain",
	}
	for _, a := range blocked {
		if email_deliverable(a) {
			t.Errorf("email_deliverable(%q) = true, want false (reserved/undeliverable)", a)
		}
	}
	allowed := []string{
		"alistair@acunningham.org", "duc@gmail.com", "user@mochi-os.org",
		"a@example.company", // NOT example.com - a real-looking TLD
		"b@testcorp.com",    // contains 'test' but not the .test TLD
	}
	for _, a := range allowed {
		if !email_deliverable(a) {
			t.Errorf("email_deliverable(%q) = false, want true (real address)", a)
		}
	}
}

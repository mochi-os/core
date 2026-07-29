// Mochi server: scope of built-in attachment event dispatch.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"strings"
	"testing"
)

// TestAttachmentMetadataEventsNotDispatched pins the removal of the built-in
// metadata handlers. They ran before the app.json event lookup, so an app could
// not decline them or check the sender, and their only gate was a service name
// the sender writes about itself - so any peer could place an attachment row
// against any object. Apps carry this metadata in their own domain events and
// authorize it there; nothing may reintroduce a path that bypasses them.
func TestAttachmentMetadataEventsNotDispatched(t *testing.T) {
	source, err := os.ReadFile("events.go")
	if err != nil {
		t.Fatalf("reading events.go: %v", err)
	}
	for _, event := range []string{"create", "insert", "update", "move", "delete", "clear"} {
		if strings.Contains(string(source), `"_attachment/`+event+`"`) {
			t.Errorf("_attachment/%s is dispatched again; it must be handled by the app's own declared event", event)
		}
	}
}

// TestAttachmentNotifyRemoved pins the send side. The notify argument fanned
// metadata out to peers who had no say in receiving it, and reinstating it
// would recreate the receiver-side hole above from the other end.
func TestAttachmentNotifyRemoved(t *testing.T) {
	source, err := os.ReadFile("attachments.go")
	if err != nil {
		t.Fatalf("reading attachments.go: %v", err)
	}
	for _, symbol := range []string{"attachment_notify_", "attachment_message(", "api_attachment_sync"} {
		if strings.Contains(string(source), symbol) {
			t.Errorf("%s is back; attachment fan-out belongs to the app that owns the data", symbol)
		}
	}
}

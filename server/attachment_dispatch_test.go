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
	// data and fetch went the same way as the metadata events: a peer asking
	// for bytes, or for an object's attachment list, now reaches the app's own
	// declared event, where the handler authorises the requester against its
	// own state before answering.
	for _, event := range []string{"create", "insert", "update", "move", "delete", "clear", "data", "fetch"} {
		if strings.Contains(string(source), `"_attachment/`+event+`"`) {
			t.Errorf("_attachment/%s is dispatched again; it must be handled by the app's own declared event", event)
		}
	}
}

// TestAttachmentByteTransferRemoved pins the responders and the remote pull.
// attachment_event_data and attachment_event_fetch answered any signed peer -
// the sender gate was a service name the sender asserts about itself - so a peer
// who knew an object id could list its attachments and pull their bytes whatever
// the app's own rules said. attachment_fetch_remote was the matching requester,
// which core drove automatically from serve and read paths, so the exchange
// happened with no app involved at either end.
func TestAttachmentByteTransferRemoved(t *testing.T) {
	for _, file := range []string{"attachments.go", "web.go"} {
		source, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("reading %s: %v", file, err)
		}
		for _, symbol := range []string{
			"attachment_event_data", "attachment_event_fetch",
			"attachment_fetch_remote", "web_serve_attachment_remote",
		} {
			if strings.Contains(string(source), symbol+"(") {
				t.Errorf("%s is back in %s; byte transfer between hosts belongs to the app that owns the data", symbol, file)
			}
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

// TestAttachmentFetchParsesNoResponse pins that api_attachment_fetch consumes
// nothing from a peer. It is retained only so an app published before the move
// keeps loading; it answers with an empty list and opens no stream.
//
// This replaces a pair of narrower pins - that the function filed rows under the
// object the app asked for rather than the one each reply claimed, and that it
// refused an id already bound elsewhere - which mattered while it still parsed a
// responder's answer. Writing nothing is the stronger property: a hostile
// responder has no reachable path here at all. Asserted against the source
// because the old function opened a real P2P stream, which a unit test cannot
// stand up.
func TestAttachmentFetchParsesNoResponse(t *testing.T) {
	source, err := os.ReadFile("attachments.go")
	if err != nil {
		t.Fatalf("reading attachments.go: %v", err)
	}
	start := strings.Index(string(source), "func api_attachment_fetch(")
	if start < 0 {
		t.Fatal("api_attachment_fetch not found")
	}
	body := string(source)[start:]
	if end := strings.Index(body, "\n}\n"); end > 0 {
		body = body[:end]
	}

	for _, symbol := range []string{"stream(", "s.read", "replace into attachments", "db.exec"} {
		if strings.Contains(body, symbol) {
			t.Errorf("api_attachment_fetch uses %s again: it must consume nothing from a peer, so a responder has no path into the app's database", symbol)
		}
	}
}

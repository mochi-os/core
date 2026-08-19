// Mochi server: attachments stay out of core.
//
// The attachment subsystem left the server in stages - built-in _attachment/*
// events first, then the whole mochi.attachment.* bridge and the app.db
// attachments table (exported to each app's file storage by
// attachment_export_sweep, which is what the apps' migrations read). These
// pins are what remains of the tests that walked it out: core must not grow
// an attachment surface again, because it cannot authorise one - "may this
// sender attach to, or read, this object" is app state.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"strings"
	"testing"

	sls "go.starlark.net/starlarkstruct"
)

// TestAttachmentEventsNotDispatched: the built-in _attachment/* events ran
// before the app.json event lookup, so an app could not decline them or check
// the sender. Apps carry attachment metadata and bytes in their own declared
// events and authorise them there.
func TestAttachmentEventsNotDispatched(t *testing.T) {
	source, err := os.ReadFile("events.go")
	if err != nil {
		t.Fatalf("reading events.go: %v", err)
	}
	if strings.Contains(string(source), `"_attachment/`) {
		t.Error(`events.go dispatches "_attachment/..." again; attachment traffic belongs to the app's own declared events`)
	}
}

// TestAttachmentApiRemoved: the mochi.attachment.* bridge is gone. An app's
// migration reads the rows core's store held from the attachments.json export
// in its file storage, written by attachment_export_sweep before the table was
// dropped.
func TestAttachmentApiRemoved(t *testing.T) {
	table := api_table()
	mochi, ok := table["mochi"].(*sls.Struct)
	if !ok {
		t.Fatalf("table[\"mochi\"] is %T, want *starlarkstruct.Struct", table["mochi"])
	}
	if v, _ := mochi.Attr("attachment"); v != nil {
		t.Error("mochi.attachment is back in the API table; attachments belong to the apps (lib/starlark/attachments.star)")
	}
}

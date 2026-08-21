// Mochi server: symbols removed as dead, and the behaviour that outlived them.
//
// The gate refuses their return; the behavioural tests below pin what the tests
// that used them were actually asserting.
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

// removed names each deleted symbol, where it lived, and why it went - the
// message a future reader gets if they put it back.
var removed = []struct {
	directory string
	symbol    string
	reason    string
}{
	{"../common/adminclient", "func (c *Client) Socket()",
		"#79: no callers. Its comment claimed error messages and `mochictl --help` used it; connect_error reads c.socket directly and --help prints a static line."},
	{"../mochictl", "func self_invocation(",
		"#80: no callers. It reconstructed a `mochictl -s <socket>` prefix for a follow-up command nothing prints."},
	{"../mochictl", "func post_with_body(",
		"#81: no callers, kept behind the repo's only //nolint:unused for v2 endpoints that do not exist. Write it when an endpoint needs it."},
	{".", "func file_content_type(",
		"#82: went with the legacy attachment bridge; its comment described the removed remote-attachment path."},
	{".", "func message_mark_seen(",
		"#83: test-only. message_seen_mark is the atomic check-and-mark production uses; this was the non-atomic half it replaced."},
	{".", "func peer_record_get(",
		"#84: test-only duplicate reader. peer_record_relay reads peer_records[id].Envelope directly."},
	{".", "type UserPurge struct",
		"#85: payload of a user/purge replication op. Multi-host replication was removed in July 2026."},
}

func TestRemovedSymbolsStayRemoved(t *testing.T) {
	for _, r := range removed {
		entries, err := os.ReadDir(r.directory)
		if err != nil {
			t.Fatalf("reading %s: %v", r.directory, err)
		}
		for _, entry := range entries {
			// Skip this file: the table above names every symbol it is
			// looking for.
			if !strings.HasSuffix(entry.Name(), ".go") || entry.Name() == "dead_code_test.go" {
				continue
			}
			path := filepath.Join(r.directory, entry.Name())
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("reading %s: %v", path, err)
			}
			if strings.Contains(string(data), r.symbol) {
				t.Errorf("%s defines %s again.\n    %s", path, r.symbol, r.reason)
			}
		}
	}
}

// TestErrorEventCarriesNoUnreadFields (#86): the handler gets user and app as
// thread locals, so struct fields for them would be written and never read.
func TestErrorEventCarriesNoUnreadFields(t *testing.T) {
	data, err := os.ReadFile("error_event.go")
	if err != nil {
		t.Fatalf("reading error_event.go: %v", err)
	}
	source := string(data)

	declaration := regexp.MustCompile(`(?m)^\t(user|app)\s+\*(User|App)$`)
	for _, m := range declaration.FindAllString(source, -1) {
		t.Errorf("ErrorEvent declares %q again; nothing reads it, and the handler gets the same value through s.set", strings.TrimSpace(m))
	}
	for _, assignment := range []string{"\t\tuser:     user,", "\t\tapp:      app,"} {
		if strings.Contains(source, assignment) {
			t.Errorf("the ErrorEvent constructor assigns %q again", strings.TrimSpace(assignment))
		}
	}

	// The Starlark surface is what apps see, and it never included these.
	e := &ErrorEvent{}
	for _, name := range e.AttrNames() {
		if name == "user" || name == "app" {
			t.Errorf("ErrorEvent exposes %q to Starlark; the struct comment calls the top-level shape frozen and these were never part of it", name)
		}
	}
}

// TestDedupStillMarksSeenMessages (#83) pins the pair production relies on:
// message_seen_mark marks, and message_seen then reports it.
func TestDedupStillMarksSeenMessages(t *testing.T) {
	const id = "dead-code-test-dedup-id"
	defer func() {
		seen_messages_lock.Lock()
		delete(seen_messages, id)
		seen_messages_lock.Unlock()
	}()

	if message_seen(id) {
		t.Fatalf("%q was already seen before the test marked it", id)
	}
	if message_seen_mark(id) {
		t.Error("message_seen_mark reported a first sighting as a duplicate")
	}
	if !message_seen(id) {
		t.Error("message_seen does not report an id message_seen_mark just marked; the read-only fast path in dispatch_message would stop deduplicating")
	}
	if !message_seen_mark(id) {
		t.Error("message_seen_mark did not report the second sighting as a duplicate")
	}
}

// TestRelayReadsStoredRecordsWithoutTheAccessor is #84's behaviour. The
// envelope store is live - peer_record_relay reads it directly - so what had
// to survive the deletion is that a stored record is still findable and still
// relayable, not the accessor itself.
func TestRelayReadsStoredRecordsWithoutTheAccessor(t *testing.T) {
	const id = "dead-code-test-peer"
	peer_records_lock.Lock()
	peer_records[id] = SignedRecord{Envelope: []byte("envelope"), Sequence: 1, Updated: now()}
	peer_records_lock.Unlock()
	defer func() {
		peer_records_lock.Lock()
		delete(peer_records, id)
		peer_records_lock.Unlock()
	}()

	if !peer_record_relayable(id) {
		t.Error("a freshly stored record is not relayable; the store peer_record_get used to read is what the relay path reads too")
	}

	peer_records_lock.Lock()
	held := peer_records[id].Envelope
	peer_records_lock.Unlock()
	if string(held) != "envelope" {
		t.Errorf("stored envelope reads back as %q, want %q", held, "envelope")
	}
}

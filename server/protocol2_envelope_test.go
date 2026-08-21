// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Tests for envelope validation on the /mochi/2/messages path: Frame.Service
// and Frame.ID are peer-chosen map keys, so both must be bounded.

package main

import (
	"strings"
	"testing"
)

// receiver_for_test returns a Receiver whose stream is an in-memory shim, so
// handle() can be driven without a network. reset reports whether the receiver
// tore the stream down.
type recording_stream struct {
	written strings.Builder
	reset   bool
}

func (s *recording_stream) Read(p []byte) (int, error)  { return 0, nil }
func (s *recording_stream) Write(p []byte) (int, error) { return s.written.Write(p) }
func (s *recording_stream) Reset() error                { s.reset = true; return nil }

func receiver_for_test() (*Receiver, *recording_stream) {
	stream := &recording_stream{}
	r := &Receiver{
		peer:    "peer-test",
		stream:  stream,
		session: "session-test",
		replies: make(chan *Frame, receiver_replies_buffer),
		claimed: map[string]bool{},
	}
	r.caps_seen.Store(true)
	return r, stream
}

// TestHandleRejectsOversizedID: the bound the constant declares, on the path
// that never applied it. An id of any length reached seen_messages, whose cap
// counts entries rather than bytes.
func TestHandleRejectsOversizedID(t *testing.T) {
	r, stream := receiver_for_test()

	long := strings.Repeat("a", id_length_maximum+1)
	if r.handle(&Frame{Type: frame_type_message, ID: long, Service: "feeds", Event: "post/create"}) {
		t.Error("handle accepted an id longer than max_id_length; it becomes a seen_messages key for the next 8 hours")
	}
	if !stream.reset {
		t.Error("a malformed envelope did not reset the stream")
	}
}

// TestHandleAcceptsIDAtTheLimit: the boundary is inclusive, so a legitimate
// id of exactly the documented length still works.
func TestHandleAcceptsIDAtTheLimit(t *testing.T) {
	if !envelope_valid("", "feeds", "post/create", strings.Repeat("a", id_length_maximum)) {
		t.Errorf("an id of exactly max_id_length (%d) was rejected; the bound is a maximum, not an exclusive limit", id_length_maximum)
	}
}

// TestHandleRejectsMalformedService: Frame.Service keys app_workers, where a
// miss creates a goroutine and a buffered channel rather than just an entry.
func TestHandleRejectsMalformedService(t *testing.T) {
	for _, service := range []string{
		strings.Repeat("s", 4096), // past the 100-char bound in "constant"
		"feeds; drop table",       // spaces and punctuation
		"\x00\x01binary",          // control bytes
	} {
		r, stream := receiver_for_test()
		if r.handle(&Frame{Type: frame_type_message, ID: "abc", Service: service, Event: "post/create"}) {
			t.Errorf("handle accepted service %.32q; it becomes an app_workers key with a goroutine behind it", service)
		}
		if !stream.reset {
			t.Errorf("service %.32q did not reset the stream", service)
		}
	}
}

// The shared "constant" validator permits "/" and ".", so a traversal-shaped
// service name is well-formed by its rules. Harmless here - Frame.Service
// becomes a map key, never a path - but five other call sites share the
// validator.
func TestServiceValidatorAdmitsSlashesAndDots(t *testing.T) {
	if !envelope_valid("", "../../etc/passwd", "", "") {
		t.Skip("\"constant\" has been tightened to reject traversal shapes; re-check every caller that relies on slashes, e.g. nested event names")
	}
	if !envelope_valid("", "post/create", "", "") {
		t.Error("a slash-bearing service was rejected; nested names are ordinary here")
	}
}

// TestHandleAcceptsAWellFormedEnvelope guards the other direction: the check
// must not reject ordinary traffic. A bye frame is the cheapest well-formed
// frame to drive, since it needs no worker or user.
func TestHandleAcceptsAWellFormedEnvelope(t *testing.T) {
	r, stream := receiver_for_test()

	// bye returns false (it ends the read loop) but must not reset the stream:
	// that is the difference between an orderly close and a violation.
	r.handle(&Frame{Type: frame_type_bye, ID: "01a0013", Service: "feeds", Event: "post/create"})
	if stream.reset {
		t.Error("a well-formed envelope reset the stream; ordinary traffic is being refused")
	}
}

// TestEnvelopeAllowsEmptyFields: plenty of frames legitimately carry no from,
// service, event or id — acks and pongs among them — so the check must reject
// only what is present and malformed.
func TestEnvelopeAllowsEmptyFields(t *testing.T) {
	if !envelope_valid("", "", "", "") {
		t.Error("an empty envelope was rejected; ack, pong and bye frames carry no addressing")
	}
}

// TestAnnouncementValidSharesTheEnvelopeCheck pins the delegation. The two
// paths having separate copies is how the id bound came to be enforced on one
// of them only; if pubsub re-inlines its own, this fails.
func TestAnnouncementValidSharesTheEnvelopeCheck(t *testing.T) {
	long := strings.Repeat("a", id_length_maximum+1)
	if announcement_valid(&Announcement{ID: long}) {
		t.Error("announcement_valid accepted an oversized id")
	}
	if envelope_valid("", "", "", long) {
		t.Error("envelope_valid accepted an oversized id")
	}
	// Same verdict for the same input, which is the property that matters.
	for _, service := range []string{"feeds", "not a service", strings.Repeat("s", 4096)} {
		shared := envelope_valid("", service, "", "")
		pubsub := announcement_valid(&Announcement{Service: service})
		if shared != pubsub {
			t.Errorf("service %.32q: envelope_valid=%v but announcement_valid=%v; the paths have diverged again", service, shared, pubsub)
		}
	}
}

// Mochi server: a frame with no segments gets no segment stream.
//
// The decision keys on length, not nil: an empty non-nil slice must behave as a
// nil one, so the self-loop and a round trip (where cbor omitempty turns an
// empty slice back into nil) agree.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"strings"
	"testing"

	cbor "github.com/fxamacker/cbor/v2"
)

// TestNoSegmentsGivesNoStream covers both ways a frame can carry nothing. The
// empty-but-not-nil case is the one the old condition got wrong.
func TestNoSegmentsGivesNoStream(t *testing.T) {
	for _, c := range []struct {
		name string
		data []byte
	}{
		{"nil", nil},
		{"empty non-nil", []byte{}},
	} {
		if stream := frame_segment_stream(c.data); stream != nil {
			t.Errorf("%s: a frame carrying no segments got a stream; every e.segment() call on it builds a decoder, hits EOF and logs before returning the false a nil stream returns for free", c.name)
		}
	}
}

// TestSegmentsGiveAReadableStream is the half that must keep working: the
// packed segments have to come back out in order.
func TestSegmentsGiveAReadableStream(t *testing.T) {
	type sample struct {
		Foo string `cbor:"foo"`
		Bar int    `cbor:"bar"`
	}
	first := sample{Foo: "hello", Bar: 42}
	second := sample{Foo: "again", Bar: 7}

	packed, err := cbor.Marshal(first)
	if err != nil {
		t.Fatalf("cbor.Marshal: %v", err)
	}
	more, err := cbor.Marshal(second)
	if err != nil {
		t.Fatalf("cbor.Marshal: %v", err)
	}
	packed = append(packed, more...)

	stream := frame_segment_stream(packed)
	if stream == nil {
		t.Fatal("a frame carrying segments got no stream, so a handler calling e.segment() would read nothing")
	}

	e := &Event{stream: stream}
	var got sample
	if !e.segment(&got) || got != first {
		t.Fatalf("first segment: got %+v, want %+v", got, first)
	}
	if !e.segment(&got) || got != second {
		t.Fatalf("second segment: got %+v, want %+v", got, second)
	}
	// Past the end, the stream reports exhaustion rather than repeating.
	if e.segment(&got) {
		t.Error("a third segment decoded from a two-segment stream")
	}
}

// TestSegmentOnNoStreamIsSilentlyFalse pins what a handler sees when the frame
// carried nothing. This is the behaviour that makes returning nil safe.
func TestSegmentOnNoStreamIsSilentlyFalse(t *testing.T) {
	e := &Event{stream: frame_segment_stream(nil)}
	var v map[string]any
	if e.segment(&v) {
		t.Error("e.segment reported a segment on an event whose frame carried none")
	}
}

// TestFrameDataIsOmitEmpty is the fact the length test rests on. If the tag
// ever loses omitempty, a sender's empty slice starts arriving as an empty
// slice rather than as nil, and the two spellings stop agreeing.
func TestFrameDataIsOmitEmpty(t *testing.T) {
	source, err := os.ReadFile("protocol2.go")
	if err != nil {
		t.Fatalf("reading protocol2.go: %v", err)
	}
	if !strings.Contains(string(source), "`cbor:\"data,omitempty\"`") {
		t.Error("Frame.Data is no longer cbor:\"data,omitempty\"; empty and absent were the same value across the wire, and frame_segment_stream's length test assumes it")
	}
}

// TestHandleDoesNotRestateTheCondition is the gate. The defect was a condition
// written inline where a reader could not check it against the wire format, so
// the rule now lives in one named function and handle must keep using it.
func TestHandleDoesNotRestateTheCondition(t *testing.T) {
	source, err := os.ReadFile("protocol2_worker.go")
	if err != nil {
		t.Fatalf("reading protocol2_worker.go: %v", err)
	}
	text := string(source)
	at := strings.Index(text, "func (w *app_worker) handle(")
	if at < 0 {
		t.Fatal("protocol2_worker.go no longer defines app_worker.handle")
	}
	body := text[at:]
	if end := strings.Index(body, "\n}\n"); end > 0 {
		body = body[:end]
	}

	if strings.Contains(body, "f.Data != nil") {
		t.Error("handle tests Frame.Data against nil again; len > 0 already implies non-nil, so a `len(...) > 0 || ... != nil` pair reduces to the nil test alone - the opposite of what it reads as")
	}
	if !strings.Contains(body, "frame_segment_stream(") {
		t.Error("handle builds the segment stream itself again rather than calling frame_segment_stream; the condition and the wire format's omitempty tag have to be readable together")
	}
}

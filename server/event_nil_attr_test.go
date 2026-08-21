// Mochi server: a nil field reaches Starlark as None, not as a typed nil.
//
// A nil *Stream or *User returned as an sl.Value is a non-nil interface
// wrapping a nil pointer: Truth() is unconditionally true, so `if e.stream:`
// passes for an event that has none, and String() dereferences and panics when
// printed. e.stream is nil for any frame carrying no packed segments, the
// ordinary remote case.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"testing"

	sl "go.starlark.net/starlark"
)

// nil_attribute fetches one attribute from an event with neither stream nor user.
func nil_attribute(t *testing.T, name string) sl.Value {
	t.Helper()
	value, err := (&Event{}).Attr(name)
	if err != nil {
		t.Fatalf("Attr(%q): %v", name, err)
	}
	return value
}

// TestNilAttrIsNone is the regression: the value handed to Starlark must be
// None rather than a typed nil pointer.
func TestNilAttrIsNone(t *testing.T) {
	for _, name := range []string{"stream", "user"} {
		value := nil_attribute(t, name)
		if value != sl.None {
			t.Errorf("e.%s on an event with none returned %T, want sl.None; a typed nil is truthy and panics when printed", name, value)
		}
	}
}

// TestNilAttrIsFalsy is the half that fails silently. A handler writing
// `if e.stream:` must not take the branch for an event that has no stream.
func TestNilAttrIsFalsy(t *testing.T) {
	for _, name := range []string{"stream", "user"} {
		if value := nil_attribute(t, name); bool(value.Truth()) {
			t.Errorf("e.%s is truthy on an event with none, so `if e.%s:` takes the branch for every event that has none", name, name)
		}
	}
}

// TestNilAttrPrintsWithoutPanicking is the loud half. String() on the typed nil
// dereferences; a handler doing print(e.stream) or string interpolation took
// the worker's recover() path and the message was dropped.
func TestNilAttrPrintsWithoutPanicking(t *testing.T) {
	for _, name := range []string{"stream", "user"} {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("printing e.%s on an event with none panicked: %v", name, r)
				}
			}()
			_ = nil_attribute(t, name).String()
		}()
	}
}

// TestPresentAttrIsUnchanged guards the other direction: the None is for the
// nil case only, and a real stream or user must still arrive as itself.
func TestPresentAttrIsUnchanged(t *testing.T) {
	stream := &Stream{id: 7}
	user := &User{UID: "01a0226400000000"}
	e := &Event{stream: stream, user: user}

	value, _ := e.Attr("stream")
	if value != sl.Value(stream) {
		t.Errorf("e.stream returned %v, want the stream itself", value)
	}
	if !bool(value.Truth()) {
		t.Error("a present stream is falsy")
	}

	value, _ = e.Attr("user")
	if value != sl.Value(user) {
		t.Errorf("e.user returned %v, want the user itself", value)
	}
	if !bool(value.Truth()) {
		t.Error("a present user is falsy")
	}
}

// TestUnknownAttrStillReturnsNilNotNone. Starlark distinguishes "no such
// attribute" (nil, nil -> AttributeError) from "the attribute is None". The
// change above must not turn an unknown name into a valid None.
func TestUnknownAttrStillReturnsNilNotNone(t *testing.T) {
	value, err := (&Event{}).Attr("no-such-attribute")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if value != nil {
		t.Errorf("an unknown attribute returned %v, want a nil sl.Value so Starlark raises AttributeError", value)
	}
}

// Mochi server: e.read and e.write refuse an event that carries no stream.
//
// er.event.stream is nil for any frame that carried no packed segments, and
// binding a method value to a nil receiver is legal and silent - the
// dereference is deferred to the call. The write builtins are worse: all three
// open with `defer s.close_write()`, which dereferences before the arguments
// are checked.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"strings"
	"testing"

	sl "go.starlark.net/starlark"
)

// stream_attribute resolves e.<group>.<name> on an event with no stream.
func stream_attribute(t *testing.T, group, name string) (sl.Value, error) {
	t.Helper()
	outer, err := (&Event{}).Attr(group)
	if err != nil {
		t.Fatalf("e.%s: %v", group, err)
	}
	holder, ok := outer.(sl.HasAttrs)
	if !ok {
		t.Fatalf("e.%s is %T, which has no attributes", group, outer)
	}
	return holder.Attr(name)
}

// TestNilStreamBuiltinsRefuse is the regression: resolving the attribute must
// fail with a readable error rather than handing back a builtin bound to nil.
func TestNilStreamBuiltinsRefuse(t *testing.T) {
	for _, c := range []struct{ group, name string }{
		{"read", "file"},
		{"write", "file"},
		{"write", "cache"},
		{"write", "asset"},
	} {
		value, err := stream_attribute(t, c.group, c.name)
		if err == nil {
			t.Errorf("e.%s.%s returned %T with no error; it is bound to a nil stream and panics when called", c.group, c.name, value)
			continue
		}
		if !strings.Contains(err.Error(), "no stream") {
			t.Errorf("e.%s.%s error %q does not say the event carries no stream", c.group, c.name, err)
		}
		if value != nil {
			t.Errorf("e.%s.%s returned both a value and an error", c.group, c.name)
		}
	}
}

// TestNilStreamBuiltinsNameTheAttribute. The four failures are otherwise
// identical, and an operator reading a handler error needs to know which call
// produced it.
func TestNilStreamBuiltinsNameTheAttribute(t *testing.T) {
	for _, c := range []struct{ group, name string }{
		{"read", "file"},
		{"write", "file"},
		{"write", "cache"},
		{"write", "asset"},
	} {
		_, err := stream_attribute(t, c.group, c.name)
		if err == nil {
			t.Fatalf("e.%s.%s did not fail", c.group, c.name)
		}
		want := "e." + c.group + "." + c.name
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %s", err, want)
		}
	}
}

// TestPresentStreamBuiltinsStillBind is the other direction: an event that has
// a stream must still resolve all four to callable builtins.
func TestPresentStreamBuiltinsStillBind(t *testing.T) {
	original := (&Event{stream: &Stream{id: 3}})
	for _, c := range []struct{ group, name string }{
		{"read", "file"},
		{"write", "file"},
		{"write", "cache"},
		{"write", "asset"},
	} {
		outer, _ := original.Attr(c.group)
		value, err := outer.(sl.HasAttrs).Attr(c.name)
		if err != nil {
			t.Errorf("e.%s.%s on an event WITH a stream failed: %v", c.group, c.name, err)
			continue
		}
		if _, ok := value.(*sl.Builtin); !ok {
			t.Errorf("e.%s.%s returned %T, want a builtin", c.group, c.name, value)
		}
	}
}

// TestUnknownStreamAttrIsStillUnknown. Starlark distinguishes "no such
// attribute" (nil, nil -> AttributeError) from an error, and the guard must not
// turn an unknown name into a failure that reads like a missing stream.
func TestUnknownStreamAttrIsStillUnknown(t *testing.T) {
	for _, group := range []string{"read", "write"} {
		value, err := stream_attribute(t, group, "no-such-attribute")
		if err != nil {
			t.Errorf("e.%s.no-such-attribute errored (%v); it should be a plain unknown attribute", group, err)
		}
		if value != nil {
			t.Errorf("e.%s.no-such-attribute returned %v, want nil so Starlark raises AttributeError", group, value)
		}
	}
}

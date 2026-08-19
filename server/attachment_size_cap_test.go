// Mochi server: an oversized attachment is refused, not stored truncated.
//
// The create path bounded its copy with io.LimitReader(reader, max_size) and
// never asked whether the source had overrun, so an attachment past the cap
// was stored as a prefix, with a row claiming that truncated length, and the
// app was told it succeeded. attachment_max_size_default is described in this
// very file as "held there by test rather than by hope" precisely because a
// truncated attachment is received silently.
//
// api_cache_write documents the idiom: bound at maximum+1, so a source that
// overruns is distinguishable from one that fits exactly. The non-stream
// create already refuses both caps outright; the two paths disagreed about the
// same condition.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

// size_cap_copy models the create path's bound-then-copy, so a test can show
// what each bound does with a source of a given length.
func size_cap_copy(t *testing.T, source_length int, max_size int64) int64 {
	t.Helper()
	reader := bytes.NewReader(bytes.Repeat([]byte("a"), source_length))
	written, err := io.Copy(io.Discard, io.LimitReader(reader, max_size))
	if err != nil {
		t.Fatalf("copying: %v", err)
	}
	return written
}

// TestABoundOfMaximumCannotDetectAnOverrun is the defect, stated as the
// arithmetic it rests on: at max_size the oversized source and the one that
// fits exactly produce the same count, so no check afterwards can separate
// them.
func TestABoundOfMaximumCannotDetectAnOverrun(t *testing.T) {
	const cap = 100

	exact := size_cap_copy(t, cap, cap)
	over := size_cap_copy(t, cap*10, cap)

	if exact != over {
		t.Fatalf("a bound of max_size gave %d for an exact fit and %d for an overrun; this test is not modelling the old behaviour", exact, over)
	}
	if exact != cap {
		t.Errorf("an exact fit copied %d bytes, want %d", exact, cap)
	}
}

// TestABoundOfMaximumPlusOneSeparatesThem is the idiom the fix adopts.
func TestABoundOfMaximumPlusOneSeparatesThem(t *testing.T) {
	const cap = 100

	if got := size_cap_copy(t, cap, cap+1); got != cap {
		t.Errorf("a source that fits exactly copied %d bytes, want %d; the extra byte must not make a legitimate attachment look oversized", got, cap)
	}
	if got := size_cap_copy(t, cap*10, cap+1); got != cap+1 {
		t.Errorf("an oversized source copied %d bytes, want %d so the check after the copy can see the overrun", got, cap+1)
	}
}

// TestTheCreatePathRefusesAnOverrun pins the whole shape in source: the bound
// is max_size+1, the check compares against max_size, and the file is removed
// rather than left as a truncated prefix with a row pointing at it.
func TestTheCreatePathRefusesAnOverrun(t *testing.T) {
	body := function_body(t, "attachments.go", "func api_attachment_create_stream(")

	if !strings.Contains(body, "io.LimitReader(reader, max_size+1)") {
		t.Error("the copy is not bounded at max_size+1, so an overrun cannot be told from an exact fit and the attachment is stored truncated")
	}
	check := strings.Index(body, "if size > max_size {")
	if check < 0 {
		t.Fatal("nothing checks whether the source overran the cap")
	}
	record := strings.Index(body, "attachment_create_record(")
	if record < 0 {
		t.Fatal("the create path no longer writes a record; this test is reading the wrong function")
	}
	if check > record {
		t.Error("the overrun check runs after the row is written, so a truncated attachment is already recorded at its truncated length")
	}
	if !strings.Contains(body[check:record], "root.Remove(filename)") {
		t.Error("an overrun leaves its truncated file behind")
	}
}

// TestTheTwoCapsAnswerDifferently: max_size conflates the user's remaining
// quota with the platform's object maximum, and they mean different things to
// whoever reads the error - free some space, versus this file can never be
// stored here. The non-stream create already distinguishes them.
func TestTheTwoCapsAnswerDifferently(t *testing.T) {
	body := function_body(t, "attachments.go", "func api_attachment_create_stream(")

	for _, message := range []string{"storage limit exceeded", "file too large"} {
		if !strings.Contains(body, message) {
			t.Errorf("the create path never answers %q; the two caps are reported as one", message)
		}
	}
	if !strings.Contains(body, "quota") {
		t.Error("nothing records which cap bound the copy, so the error cannot say which one was hit")
	}

	// The same two messages the sibling path uses, so both answer alike.
	sibling := function_body(t, "attachments.go", "func api_attachment_create(")
	for _, message := range []string{"storage limit exceeded", "file too large"} {
		if !strings.Contains(sibling, message) {
			t.Errorf("the non-stream create no longer answers %q; the two paths have diverged again", message)
		}
	}
}

// TestTheObjectMaximumIsStillTheCeiling: the cap being enforced has to be the
// one the file documents, not a looser local constant.
func TestTheObjectMaximumIsStillTheCeiling(t *testing.T) {
	if attachment_max_size_default != object_maximum {
		t.Errorf("attachment_max_size_default is %d and object_maximum is %d; an attachment above the transfer cap is stored whole by its owner and received truncated by every subscriber",
			attachment_max_size_default, object_maximum)
	}
}

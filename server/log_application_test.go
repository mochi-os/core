// Mochi server: an app cannot forge a journal line, or drown one.
//
// mochi.log.* lets an app choose its format string, so the per-format window
// never suppresses it, and a newline in app text forged a second line the
// writer never stamped. The fixes: escaping, and a budget keyed on the app.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"testing"

	sl "go.starlark.net/starlark"
)

// TestLogEscapeRemovesLineBreaks is the forgery fix. Whatever the app writes,
// one call must yield one line.
func TestLogEscapeRemovesLineBreaks(t *testing.T) {
	for _, c := range []struct{ name, input string }{
		{"newline", "upload failed\n2026-08-20 14:00:00.000000 Server shutting down"},
		{"carriage return", "ok\rServer restarting"},
		{"both", "ok\r\n2026-08-20 14:00:00.000000 Fatal"},
		{"trailing", "done\n"},
		{"many", "a\nb\nc\nd"},
	} {
		got := log_escape(c.input)
		if strings.ContainsAny(got, "\n\r") {
			t.Errorf("%s: log_escape(%+q) = %+q, which still spans lines - the extra one carries whatever the app wrote and the writer never stamps it", c.name, c.input, got)
		}
	}
}

// TestLogEscapeKeepsTheTextReadable. Escaping, not stripping: a multi-line
// message stays legible as \n rather than silently losing its structure.
func TestLogEscapeKeepsTheTextReadable(t *testing.T) {
	if got := log_escape("first\nsecond"); got != `first\nsecond` {
		t.Errorf(`log_escape("first\nsecond") = %q, want "first\\nsecond"`, got)
	}
	if got := log_escape("a\tb"); got != "a\tb" {
		t.Errorf("log_escape ate a tab: %q - a tab is legitimate spacing and starts no line", got)
	}
	// Ordinary text, including non-ASCII, is untouched.
	for _, plain := range []string{"upload failed", "Café Olé", "東京", "100% done", `C:\path\to\file`} {
		if got := log_escape(plain); got != plain {
			t.Errorf("log_escape(%q) = %q; ordinary text must survive unchanged", plain, got)
		}
	}
}

// TestLogEscapeRemovesTerminalControls: the journal is read in a terminal, so
// an escape sequence there is the same class of problem as the bidirectional
// controls refused from display names.
func TestLogEscapeRemovesTerminalControls(t *testing.T) {
	for _, c := range []struct{ name, input string }{
		{"ansi escape", "Server \x1b[31mFAILED\x1b[0m"},
		{"bell", "alert\x07"},
		{"backspace", "hidden\x08\x08\x08"},
		{"null", "a\x00b"},
		{"delete", "a\x7fb"},
	} {
		got := log_escape(c.input)
		for _, character := range got {
			if character < 0x20 && character != '\t' {
				t.Errorf("%s: log_escape(%+q) = %+q, which still carries a control character", c.name, c.input, got)
				break
			}
			if character == 0x7f {
				t.Errorf("%s: log_escape(%+q) left DEL in place", c.name, c.input)
				break
			}
		}
	}
}

// TestAppLineBudgetBoundsAVaryingFormat is the flood fix, stated against the
// thing that defeated the old one: text the app changes every call.
func TestAppLineBudgetBoundsAVaryingFormat(t *testing.T) {
	log_tables_reset(t)
	defer log_tables_reset(t)

	allowed := 0
	for i := 0; i < log_app_lines_maximum*5; i++ {
		if log_app_allow("app-one") {
			allowed++
		}
	}
	if allowed != log_app_lines_maximum {
		t.Errorf("an app wrote %d lines in one window, want at most %d; the per-format window cannot bound this because the app chooses the format", allowed, log_app_lines_maximum)
	}
}

// TestAppLineBudgetSeparatesApplications: one noisy app must not silence
// another. This is why the key is the app rather than a single global counter.
func TestAppLineBudgetSeparatesApplications(t *testing.T) {
	log_tables_reset(t)
	defer log_tables_reset(t)

	for i := 0; i < log_app_lines_maximum*2; i++ {
		log_app_allow("noisy")
	}
	if !log_app_allow("quiet") {
		t.Error("a second app was silenced by the first's flood")
	}
}

// TestAppLineBudgetExemptsCore. A call with no app bound - core's own logging
// through this path - keeps the per-format window and no app budget, because
// there is no app to charge and core's formats are fixed call sites.
func TestAppLineBudgetExemptsCore(t *testing.T) {
	log_tables_reset(t)
	defer log_tables_reset(t)

	for i := 0; i < log_app_lines_maximum*3; i++ {
		if !log_app_allow("") {
			t.Fatalf("an unattributed line was refused after %d; the budget charges apps, and there is no app here", i)
		}
	}
}

// TestAppLineBudgetKeySpaceIsBounded is the property that lets this table go
// without eviction, unlike log_repeat_state. The key is the app id, which the
// app does not choose; the format key it replaces is chosen per call, which is
// why that table needed a ceiling and an eviction pass.
func TestAppLineBudgetKeySpaceIsBounded(t *testing.T) {
	log_tables_reset(t)
	defer log_tables_reset(t)

	for i := 0; i < 50; i++ {
		log_app_allow("app-one")
		log_app_allow("app-two")
	}
	log_app_mutex.Lock()
	size := len(log_app_state)
	log_app_mutex.Unlock()

	if size != 2 {
		t.Errorf("the budget table holds %d entries for two apps; it is keyed on something the app can vary, and then it needs eviction like log_repeat_state does", size)
	}
}

// TestApplicationLoggingIsEscapedAndBudgeted is the gate on sl_log, the one
// choke point every mochi.log.* call passes through.
func TestApplicationLoggingIsEscapedAndBudgeted(t *testing.T) {
	source, err := os.ReadFile("log.go")
	if err != nil {
		t.Fatalf("reading log.go: %v", err)
	}
	text := string(source)
	at := strings.Index(text, "func sl_log(")
	if at < 0 {
		t.Fatal("log.go no longer defines sl_log")
	}
	body := text[at:]
	if end := strings.Index(body, "\n}\n"); end > 0 {
		body = body[:end]
	}
	// Strip line comments: the explanation above the code names both calls.
	var code []string
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "//") {
			code = append(code, line)
		}
	}
	body = strings.Join(code, "\n")

	if !strings.Contains(body, "log_app_allow(app)") {
		t.Error("sl_log does not charge the line against the app, so an app varying its format writes without limit - the per-format window it falls back on is keyed on a string the app chooses")
	}
	if !strings.Contains(body, "log_escape(fmt.Sprintf(format, values...))") {
		t.Error("sl_log does not escape the FORMATTED line; escaping the format alone leaves a newline one argument away, and sl_decode yields lists, maps and bytes that can carry one too")
	}
}

// TestEscapingHappensAfterFormatting states why, in a form a reader can check
// without reasoning about sl_decode: a newline arriving through a value is
// caught, and one arriving through a nested value is caught too.
func TestEscapingHappensAfterFormatting(t *testing.T) {
	for _, c := range []struct {
		name   string
		format string
		values []any
	}{
		{"through a string value", "upload of %s failed", []any{"a\n2026-01-01 00:00:00.000000 Fake"}},
		{"through a list", "items %v", []any{[]any{"a\nFake", "b"}}},
		{"through bytes", "payload %s", []any{[]byte("a\nFake")}},
		{"through a map", "state %v", []any{map[string]any{"k": "a\nFake"}}},
	} {
		line := log_escape(fmt.Sprintf(c.format, c.values...))
		if strings.ContainsAny(line, "\n\r") {
			t.Errorf("%s: %q still spans lines", c.name, line)
		}
	}
}

// log_capture redirects the package logger into a buffer for the duration of
// one test, and restores the real writer afterwards. init() installs
// log_writer; swapping it back is what keeps the rest of the binary's output
// intact.
type log_capture struct{ lines []string }

func (c *log_capture) Write(b []byte) (int, error) {
	for _, line := range strings.Split(strings.TrimRight(string(b), "\n"), "\n") {
		c.lines = append(c.lines, line)
	}
	return len(b), nil
}

func log_captured(t *testing.T) *log_capture {
	t.Helper()
	capture := &log_capture{}
	log.SetOutput(capture)
	t.Cleanup(func() { log.SetOutput(new(log_writer)) })
	return capture
}

// application_log calls mochi.log.<level> the way an app does, through the real
// builtin, with a real app bound to the thread.
func application_log(t *testing.T, app string, level string, format string, values ...string) {
	t.Helper()
	thread := &sl.Thread{}
	thread.SetLocal("app", &App{id: app})
	thread.SetLocal("function", "handler")
	args := sl.Tuple{sl.String(format)}
	for _, v := range values {
		args = append(args, sl.String(v))
	}
	builtin := sl.NewBuiltin(level, sl_log)
	if _, err := sl_log(thread, builtin, args, nil); err != nil {
		t.Fatalf("%s: %v", level, err)
	}
}

// TestAppFloodIsBoundedThroughTheRealCall is the flood regression end to end.
// It varies the FORMAT every call - the shape that defeated the per-format
// window entirely - and counts what actually reaches the journal.
func TestAppFloodIsBoundedThroughTheRealCall(t *testing.T) {
	log_tables_reset(t)
	defer log_tables_reset(t)
	capture := log_captured(t)

	for i := 0; i < log_app_lines_maximum*3; i++ {
		application_log(t, "noisy-app", "mochi.log.debug", fmt.Sprintf("processing item %d", i))
	}

	written := 0
	for _, line := range capture.lines {
		if strings.Contains(line, "noisy-app") {
			written++
		}
	}
	if written > log_app_lines_maximum {
		t.Errorf("%d lines reached the journal from one app in one window, want at most %d; a format built from data opens a fresh key every call, so the per-format window never suppresses it",
			written, log_app_lines_maximum)
	}
	if written == 0 {
		t.Error("no lines reached the journal at all; the budget is not meant to silence an app, only to bound it")
	}
}

// TestAppCannotForgeALineThroughTheRealCall is the forgery regression end to
// end: one call, one line, whatever the app writes.
func TestAppCannotForgeALineThroughTheRealCall(t *testing.T) {
	log_tables_reset(t)
	defer log_tables_reset(t)
	capture := log_captured(t)

	// Through the format...
	application_log(t, "sneaky-app", "mochi.log.debug",
		"upload failed\n2026-08-20 14:00:00.000000 Server shutting down for maintenance")
	// ...and through a value, which Sprintf splices in after the format is read.
	application_log(t, "sneaky-app", "mochi.log.info",
		"upload of %s failed", "photo.png\n2026-08-20 14:00:01.000000 Disk failure imminent")

	if len(capture.lines) != 2 {
		t.Fatalf("two calls produced %d journal lines: %q", len(capture.lines), capture.lines)
	}
	for _, line := range capture.lines {
		if !strings.Contains(line, "sneaky-app") {
			t.Errorf("a line reached the journal without the app attribution sl_log forces: %q", line)
		}
		for _, forged := range []string{"Server shutting down", "Disk failure imminent"} {
			if strings.HasPrefix(strings.TrimSpace(line), "2026-08-20") && strings.Contains(line, forged) {
				t.Errorf("the app emitted a line that reads as core output: %q", line)
			}
		}
	}
}

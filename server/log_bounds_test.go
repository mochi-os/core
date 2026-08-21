// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Tests for the two tables the log package keys on caller-supplied strings:
// log_repeat_state, keyed per format an app may vary, and warn_email_state,
// which warn_application keys on the app so it is bounded by installed apps.

package main

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

// log_tables_reset clears both tables so a test starts from a known size.
func log_tables_reset(t *testing.T) {
	t.Helper()
	log_repeat_mutex.Lock()
	log_repeat_state = map[string]*log_repeat_record{}
	log_repeat_mutex.Unlock()
	warn_email_mutex.Lock()
	warn_email_state = map[string]warn_email_record{}
	warn_email_mutex.Unlock()
	log_app_mutex.Lock()
	log_app_state = map[string]*log_repeat_record{}
	log_app_mutex.Unlock()
}

// TestLogRepeatTableIsBounded: a distinct format per call must not grow the
// table without limit. This is the part that applies to every server, since
// log_repeat_allow runs from debug() and info() with no level or email gate.
func TestLogRepeatTableIsBounded(t *testing.T) {
	log_tables_reset(t)
	defer log_tables_reset(t)

	for i := 0; i < log_repeat_maximum*2; i++ {
		log_repeat_allow(fmt.Sprintf("format variant %d", i))
	}

	log_repeat_mutex.Lock()
	size := len(log_repeat_state)
	log_repeat_mutex.Unlock()

	if size > log_repeat_maximum {
		t.Errorf("log_repeat_state holds %d entries after %d distinct formats, above its %d ceiling; an app varying its debug format grows it without bound",
			size, log_repeat_maximum*2, log_repeat_maximum)
	}
}

// TestLogRepeatStillSuppressesAfterEviction: the table is also the mechanism
// that suppresses a flooding format, so bounding it must not cost that. A
// format that keeps logging is recent, so eviction (oldest first) must leave it
// in place and still counting.
func TestLogRepeatStillSuppressesAfterEviction(t *testing.T) {
	log_tables_reset(t)
	defer log_tables_reset(t)

	const flooding = "the flooding format %d"
	for i := 0; i < log_repeat_threshold; i++ {
		if !log_repeat_allow(flooding) {
			t.Fatalf("log_repeat_allow refused call %d, below the threshold of %d", i, log_repeat_threshold)
		}
	}
	if log_repeat_allow(flooding) {
		t.Fatal("log_repeat_allow permitted a line past the threshold; suppression is not working before eviction is involved")
	}

	// Push the table over its ceiling with unrelated formats.
	for i := 0; i < log_repeat_maximum*2; i++ {
		log_repeat_allow(fmt.Sprintf("unrelated %d", i))
	}

	if log_repeat_allow(flooding) {
		t.Error("the flooding format was permitted again after eviction; its window was dropped despite being one of the newest, so a flood can reset its own suppression by opening fresh formats")
	}
}

func TestWarnEmailKeyIsPerApplication(t *testing.T) {
	log_tables_reset(t)
	defer log_tables_reset(t)

	send, _ := warn_email_allow("app:example")
	if !send {
		t.Fatal("the first warning from an app did not send; an admin who configured email.admin must still receive app warnings")
	}

	for i := 0; i < 1000; i++ {
		if send, _ := warn_email_allow("app:example"); send {
			t.Fatalf("a second admin mail was sent for the same app on call %d; keying on the app is what bounds the flood", i)
		}
	}

	warn_email_mutex.Lock()
	size := len(warn_email_state)
	warn_email_mutex.Unlock()
	if size != 1 {
		t.Errorf("warn_email_state holds %d entries for one app, want 1; the key still varies with the message", size)
	}
}

// TestWarnApplicationKeysOnTheApp checks the production path uses that key: the
// tests above call warn_email_allow directly and pass either way. Asserted on
// the source because ini only loads from a file, so email.admin cannot be set.
func TestWarnApplicationKeysOnTheApp(t *testing.T) {
	source, err := os.ReadFile("log.go")
	if err != nil {
		t.Fatalf("reading log.go: %v", err)
	}
	body := string(source)
	// The throttle lives in warn_application_email, which warn_application and
	// sl_log both call. sl_log needs it separately because it formats and
	// escapes the line itself - the app supplies the values as well as the
	// format - so it cannot hand text back to something that formats again.
	start := strings.Index(body, "func warn_application_email(")
	if start < 0 {
		t.Fatal("warn_application_email not found in log.go")
	}
	end := strings.Index(body[start:], "\n}\n")
	if end < 0 {
		t.Fatal("could not find the end of warn_application_email")
	}
	function := body[start : start+end]

	if !strings.Contains(function, `warn_email_allow("app:" + app)`) {
		t.Error("the app warning email does not key its throttle on the app; keyed on the message, an app varying its text sends a mail per call because every fresh key is a first occurrence")
	}

	// And both entry points must route through it, or one of them regains the
	// unbounded behaviour on its own.
	for _, caller := range []string{"func warn_application(", "func sl_log("} {
		at := strings.Index(body, caller)
		if at < 0 {
			t.Fatalf("%s not found in log.go", caller)
		}
		tail := body[at:]
		if stop := strings.Index(tail, "\n}\n"); stop > 0 {
			tail = tail[:stop]
		}
		if !strings.Contains(tail, "warn_application_email(") {
			t.Errorf("%s does not send its app warning through warn_application_email, so that path is not throttled per app", strings.TrimPrefix(strings.TrimSuffix(caller, "("), "func "))
		}
	}
}

// TestWarnEmailSeparatesApplications: one app exhausting its window must not
// silence another's first warning.
func TestWarnEmailSeparatesApplications(t *testing.T) {
	log_tables_reset(t)
	defer log_tables_reset(t)

	warn_email_allow("app:noisy")
	if send, _ := warn_email_allow("app:noisy"); send {
		t.Fatal("the noisy app sent twice inside one window")
	}
	if send, _ := warn_email_allow("app:quiet"); !send {
		t.Error("a second app's first warning was suppressed by the first app's window")
	}
}

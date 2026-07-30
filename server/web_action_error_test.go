// Mochi server: mapping an aborted action onto an HTTP response.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// action_error_response runs one error through the mapping and returns what the
// client would receive.
func action_error_response(err error) *httptest.ResponseRecorder {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/people/1person/-/avatar", nil)
	web_action_error(c, "people", err)
	return recorder
}

// TestActionErrorRateLimitIsA429 is the point of the change. A refused call used
// to arrive as a 500, which a client is entitled to retry - and the retry
// recharges the budget the limiter exists to protect.
func TestActionErrorRateLimitIsA429(t *testing.T) {
	recorder := action_error_response(&RateLimitError{Retry: 42, detail: "600 remote calls per minute per target"})

	if recorder.Code != http.StatusTooManyRequests {
		t.Errorf("status = %d, want %d - a refusal is not a server fault", recorder.Code, http.StatusTooManyRequests)
	}
	if got := recorder.Header().Get("Retry-After"); got != "42" {
		t.Errorf("Retry-After = %q, want %q, so a client need not guess how long to wait", got, "42")
	}
}

// TestActionErrorRateLimitWithheldDetail pins what must NOT reach the caller. The
// old 500 body named the internal Starlark function and the exact budget, to an
// anonymous caller on a public asset route.
func TestActionErrorRateLimitWithheldDetail(t *testing.T) {
	err := fmt.Errorf("mochi.remote.stream() %w", &RateLimitError{
		Retry:  30,
		detail: "600 remote calls per minute per target",
	})
	body := action_error_response(err).Body.String()

	for _, leak := range []string{"600", "mochi.remote.stream", "per target"} {
		if strings.Contains(body, leak) {
			t.Errorf("response body contains %q; body was %s", leak, body)
		}
	}
}

// TestActionErrorRateLimitUnknownRetry covers a refusal whose window has already
// lapsed: Retry-After must be absent rather than "0", which would invite an
// instant retry.
func TestActionErrorRateLimitUnknownRetry(t *testing.T) {
	recorder := action_error_response(&RateLimitError{Retry: 0})

	if recorder.Code != http.StatusTooManyRequests {
		t.Errorf("status = %d, want %d", recorder.Code, http.StatusTooManyRequests)
	}
	if _, present := recorder.Header()["Retry-After"]; present {
		t.Errorf("Retry-After = %q, want the header omitted when the wait is unknown", recorder.Header().Get("Retry-After"))
	}
}

// TestActionErrorSurvivesWrapping is the assumption the whole design rests on: the
// typed error is raised deep inside a builtin and has to survive sl_error's
// wrapping and the Starlark unwind. sl_error wraps with %w for exactly this, and
// PermissionError already relies on it - but if that ever stops holding, every
// refusal silently reverts to a 500, which is the bug this change fixes.
func TestActionErrorSurvivesWrapping(t *testing.T) {
	limit := &RateLimitError{Retry: 7}

	// Mirrors sl_error: "%w: %v" with the builtin name folded into the text.
	wrapped := fmt.Errorf("%w: %v", limit, "mochi.remote.stream() rate limit exceeded")
	// And one more layer, as the interpreter may add.
	wrapped = fmt.Errorf("action failed: %w", wrapped)

	var found *RateLimitError
	if !errors.As(wrapped, &found) {
		t.Fatalf("errors.As lost the *RateLimitError through wrapping; every refusal would fall through to a 500")
	}
	if found.Retry != 7 {
		t.Errorf("Retry = %d, want 7", found.Retry)
	}

	if code := action_error_response(wrapped).Code; code != http.StatusTooManyRequests {
		t.Errorf("wrapped error mapped to %d, want %d", code, http.StatusTooManyRequests)
	}
}

// TestActionErrorRateLimitLogGateDoesNotAffectTheResponse separates the two jobs
// the 429 branch does. The log line is throttled to one per app per minute so a
// flood cannot drive our disk, but throttling the LOG must never throttle the
// RESPONSE: every refused caller still needs its 429 and its Retry-After.
func TestActionErrorRateLimitLogGateDoesNotAffectTheResponse(t *testing.T) {
	rate_limit_refusal_log.lock.Lock()
	rate_limit_refusal_log.entries = make(map[string]*rate_limit_entry)
	rate_limit_refusal_log.lock.Unlock()

	// Well past the single log slot for this app.
	for i := 0; i < 50; i++ {
		recorder := action_error_response(&RateLimitError{Retry: 12, detail: "600 per minute"})
		if recorder.Code != http.StatusTooManyRequests {
			t.Fatalf("refusal %d answered %d, want %d - the log gate must not suppress responses", i+1, recorder.Code, http.StatusTooManyRequests)
		}
		if got := recorder.Header().Get("Retry-After"); got != "12" {
			t.Fatalf("refusal %d had Retry-After %q, want %q", i+1, got, "12")
		}
	}
}

// TestActionErrorPermissionUnchanged guards the branch that was already here, so
// extracting this function out of web_action cannot have altered it.
func TestActionErrorPermissionUnchanged(t *testing.T) {
	recorder := action_error_response(&PermissionError{Permission: "groups/read", Restricted: false})

	if recorder.Code != http.StatusForbidden {
		t.Errorf("status = %d, want %d", recorder.Code, http.StatusForbidden)
	}
	body := recorder.Body.String()
	for _, want := range []string{"permission_required", "groups/read", "people"} {
		if !strings.Contains(body, want) {
			t.Errorf("body is missing %q; body was %s", want, body)
		}
	}
}

// TestActionErrorGenericStaysA500 keeps a real fault reported as a fault. Turning
// every abort into a 429 would hide genuine breakage, which is the opposite
// failure to the one being fixed.
func TestActionErrorGenericStaysA500(t *testing.T) {
	recorder := action_error_response(errors.New("division by zero"))

	if recorder.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want %d - an ordinary fault must stay a fault", recorder.Code, http.StatusInternalServerError)
	}
	if !strings.Contains(recorder.Body.String(), "division by zero") {
		t.Errorf("the generic branch dropped the error text; body was %s", recorder.Body.String())
	}
}

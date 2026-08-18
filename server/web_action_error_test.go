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
	"os"
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
//
// It also holds the detail back from the caller. err carries the internal
// Starlark function name, and from a database failure the driver's own message;
// the body says only that the server failed, and the log carries the rest.
func TestActionErrorGenericStaysA500(t *testing.T) {
	recorder := action_error_response(errors.New("division by zero"))

	if recorder.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want %d - an ordinary fault must stay a fault", recorder.Code, http.StatusInternalServerError)
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "server_error") {
		t.Errorf("the generic branch did not report a server error; body was %s", body)
	}
	if strings.Contains(body, "division by zero") {
		t.Errorf("the generic branch leaked the error detail to the caller; body was %s", body)
	}
}

// TestActionErrorRateLimitDoesNotMailTheOperator. warn() mails the address in
// [email] admin; info() does not. A limiter refusing a caller is the limiter
// working - the caller already has a 429 with Retry-After and there is nothing
// for an operator to do - so mailing them is wrong, and on a public server the
// mail arrives for every distinct app a flood happens to touch. Every other
// rate-limit refusal in the server logs at debug; this one line was at warn,
// and mailed albatross for a test suite exercising its own limiter.
//
// Asserted on the source. warn only reaches the mail when [email] admin is set,
// the ini package loads from a file only, and setting it would mutate global
// config for every other test in this binary - the same reason
// TestWarnApplicationKeysOnTheApp asserts this way. Scoped to the branch, so a
// warn elsewhere in web_action_error is not caught by accident.
func TestActionErrorRateLimitDoesNotMailTheOperator(t *testing.T) {
	source, err := os.ReadFile("web.go")
	if err != nil {
		t.Fatalf("reading web.go: %v", err)
	}
	body := string(source)
	start := strings.Index(body, "func web_action_error(")
	if start < 0 {
		t.Fatal("web_action_error not found in web.go")
	}
	end := strings.Index(body[start:], "\n}\n")
	if end < 0 {
		t.Fatal("could not find the end of web_action_error")
	}
	function := body[start : start+end]

	branch_start := strings.Index(function, "errors.As(err, &limit)")
	branch_end := strings.Index(function[branch_start:], "\n\t}\n")
	if branch_start < 0 || branch_end < 0 {
		t.Fatal("could not isolate the rate-limit branch of web_action_error")
	}
	branch := function[branch_start : branch_start+branch_end]

	if strings.Contains(branch, "warn(") {
		t.Error("the rate-limit branch logs at warn, which mails the operator for a refusal they cannot act on")
	}
	if !strings.Contains(branch, "info(") {
		t.Error("the rate-limit branch no longer logs the refusal at all; the line is the only record that a budget was tripped")
	}

	// The generic branch below it must stay at warn: an action that genuinely
	// broke is exactly what the operator should hear about.
	if !strings.Contains(function[branch_start+branch_end:], "warn(") {
		t.Error("the generic failure branch no longer warns, so a real fault would stop reaching the operator")
	}
}

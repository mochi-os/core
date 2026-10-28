// Mochi server: mapping an aborted action onto an HTTP response.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	sl "go.starlark.net/starlark"
)

// action_error_response runs one error through the mapping and returns what the
// client would receive.
func action_error_response(err error) *httptest.ResponseRecorder {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/people/1person/-/avatar", nil)
	web_action_error(c, "people", "en", err)
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

// The typed error is raised inside a builtin and must survive sl_error's %w
// wrapping and the Starlark unwind, or every refusal falls back to a 500.
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

// TestActionErrorAbsentStorageIsA401. An anonymous request that resolved no
// storage account and then touched storage is a caller who needs to sign in,
// not a server fault: 401 with the usual code, and none of the internal text.
func TestActionErrorAbsentStorageIsA401(t *testing.T) {
	recorder := action_error_response(storage_absent)

	if recorder.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d - an anonymous caller with no storage account needs to sign in, not retry", recorder.Code, http.StatusUnauthorized)
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "authentication_required") {
		t.Errorf("body is missing the authentication_required code; body was %s", body)
	}
	if strings.Contains(body, "storage account") {
		t.Errorf("the internal error text reached the caller; body was %s", body)
	}
}

// The sentinel is raised inside a builtin (mochi.db, mochi.file, ...) and must
// survive sl_error's "%w: %v" wrapping and the Starlark unwind, as the typed
// errors above do, or every anonymous probe of a public route is a 500 and a
// mail to the operator.
func TestActionErrorAbsentStorageSurvivesWrapping(t *testing.T) {
	wrapped := fmt.Errorf("%w: %v", storage_absent, "mochi.db.query() no storage account for an anonymous request")
	wrapped = fmt.Errorf("action failed: %w", wrapped)

	if code := action_error_response(wrapped).Code; code != http.StatusUnauthorized {
		t.Errorf("wrapped error mapped to %d, want %d", code, http.StatusUnauthorized)
	}
}

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

// warn() mails the address in [email] admin; info() does not, and a refused
// caller needs no operator. Asserted on the source because warn only mails when
// [email] admin is set, and the ini package loads that from a file only.
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

	// The absent-storage branch logs nothing: an anonymous probe of a public
	// route is routine, and at warn it would mail the operator per probe.
	storage_start := strings.Index(function, "errors.Is(err, storage_absent)")
	if storage_start < 0 {
		t.Fatal("could not isolate the absent-storage branch of web_action_error")
	}
	storage_end := strings.Index(function[storage_start:], "\n\t}\n")
	if storage_end < 0 {
		t.Fatal("could not find the end of the absent-storage branch")
	}
	storage_branch := function[storage_start : storage_start+storage_end]
	if strings.Contains(storage_branch, "warn(") || strings.Contains(storage_branch, "info(") {
		t.Error("the absent-storage branch logs a refusal that needs no operator")
	}
	if !strings.Contains(function[storage_start+storage_end:], "warn(") {
		t.Error("the absent-storage branch sits after the generic warn, so it never answers")
	}
}

// refused reports whether err carries a refusal under key, and, where reason is
// given, whether the other server's reason it passes on contains it.
func refused(err error, key string, reason string) bool {
	var refusal *Refusal
	if !errors.As(err, &refusal) || refusal.Key != key {
		return false
	}
	if reason == "" {
		return true
	}
	given, _ := refusal.Args["reason"].(string)
	return strings.Contains(given, reason)
}

// TestActionErrorRefusal: a refusal raised in a builtin reaches the user with
// its own status and its reason in their language, the network it names in
// that language too, where it used to be a server error that mailed the
// administrator.
func TestActionErrorRefusal(t *testing.T) {
	load_core_labels()
	refusal := refuse(http.StatusConflict, "errors.account_exists", nil).named("network", "account.display.bluesky")
	_, inner := sl_error(sl.NewBuiltin("mochi.account.add", nil), "%v", refusal)
	err := fmt.Errorf("settings action failed: %w", inner)
	answer := func(language string) (int, map[string]any) {
		recorder := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(recorder)
		c.Request = httptest.NewRequest(http.MethodPost, "/settings/-/user/networks/account/create", nil)
		web_action_error(c, "settings", language, err)
		var body map[string]any
		json.Unmarshal(recorder.Body.Bytes(), &body)
		return recorder.Code, body
	}

	status, body := answer("en")
	if status != http.StatusConflict {
		t.Errorf("THE POINT: status = %d, want %d", status, http.StatusConflict)
	}
	if body["error"] != "account_exists" || body["message"] != "This handle already has a Bluesky account" {
		t.Errorf("THE POINT: the reason reaches the user: %v", body)
	}

	_, german := answer("de")
	want := resolve_core_label("de", "errors.account_exists", map[string]any{"network": resolve_core_label("de", "account.display.bluesky", nil)})
	if german["message"] != want || want == body["message"] || !strings.Contains(want, "Bluesky") {
		t.Errorf("the reason is in the user's language: %v, want %q", german, want)
	}
}

// TestActionErrorRefusalDoesNotMailTheOperator: a refusal is logged at info,
// since warn mails the administrator and nothing broke.
func TestActionErrorRefusalDoesNotMailTheOperator(t *testing.T) {
	source, err := os.ReadFile("web.go")
	if err != nil {
		t.Fatal(err)
	}
	body := string(source)
	start := strings.Index(body, "errors.As(err, &refusal)")
	if start < 0 {
		t.Fatal("the refusal branch of web_action_error is gone")
	}
	branch := body[start : start+strings.Index(body[start:], "\n\t}\n")]
	if strings.Contains(branch, "warn(") || !strings.Contains(branch, "info(") {
		t.Errorf("the refusal branch must log at info and not warn:\n%s", branch)
	}
}

// TestRefusalLabelsInEveryLocale: each refusal a network account gets is
// worded in every locale core carries, read back as written.
func TestRefusalLabelsInEveryLocale(t *testing.T) {
	load_core_labels()
	keys := []string{"errors.network_unavailable", "errors.handle_not_held", "errors.handle_not_community", "errors.account_exists", "errors.host_unresolved", "errors.domain_unresolved", "errors.host_elsewhere", "errors.directory_unreachable", "errors.directory_refused", "errors.linking_unavailable", "errors.link_failed", "errors.move_unresolved", "errors.move_to_itself", "errors.move_not_named", "errors.account_no_keys", "errors.key_not_announced", "errors.recovery_not_listed"}
	for _, key := range keys {
		if core_labels["en"][key] == "" {
			t.Errorf("%s has no English", key)
		}
	}
	if got := core_labels["af"]["errors.move_to_itself"]; !strings.HasPrefix(got, "'n ") {
		t.Errorf("af errors.move_to_itself read back as %q, its leading 'n lost", got)
	}
	// A regional variant inherits from its parent, so each is looked for along
	// the locale's own chain short of English.
	for locale := range core_labels {
		if locale == "en" || strings.HasPrefix(locale, "en-") {
			continue
		}
		for _, key := range keys {
			found := false
			for _, tag := range language_fallbacks(locale) {
				if tag != "en" && core_labels[tag][key] != "" {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("%s has no %s", locale, key)
			}
		}
	}
}

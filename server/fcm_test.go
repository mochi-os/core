// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import "testing"

// Real FCM v1 response body for a dead token, as received in production
// (2026-07-07): 404 whose details carry errorCode UNREGISTERED.
const fcm_body_unregistered = `{
  "error": {
    "code": 404,
    "message": "Requested entity was not found.",
    "status": "NOT_FOUND",
    "details": [
      {
        "@type": "type.googleapis.com/google.firebase.fcm.v1.FcmError",
        "errorCode": "UNREGISTERED"
      }
    ]
  }
}`

// 404 without the UNREGISTERED detail — the shape a wrong project ID in
// fcm.service_account produces. Must NOT retire: the token is fine, the
// request path is wrong, and retiring would delete live registrations.
const fcm_body_project_missing = `{
  "error": {
    "code": 404,
    "message": "The requested URL was not found on this server.",
    "status": "NOT_FOUND"
  }
}`

// 400 INVALID_ARGUMENT blaming the token itself — malformed token, retire.
const fcm_body_token_invalid = `{
  "error": {
    "code": 400,
    "message": "The registration token is not a valid FCM registration token",
    "status": "INVALID_ARGUMENT"
  }
}`

// 400 INVALID_ARGUMENT blaming a payload field — a server-side envelope
// bug, not a dead token. Must NOT retire.
const fcm_body_payload_invalid = `{
  "error": {
    "code": 400,
    "message": "Invalid value at 'message.data[0].value' (TYPE_STRING)",
    "status": "INVALID_ARGUMENT"
  }
}`

func TestFcmRetire(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		retire bool
	}{
		{"404 UNREGISTERED retires", 404, fcm_body_unregistered, true},
		{"404 without UNREGISTERED does not retire", 404, fcm_body_project_missing, false},
		{"404 unparseable body does not retire", 404, "not found", false},
		{"400 INVALID_ARGUMENT on token retires", 400, fcm_body_token_invalid, true},
		{"400 INVALID_ARGUMENT on payload does not retire", 400, fcm_body_payload_invalid, false},
		{"500 does not retire", 500, `{"error":{"status":"INTERNAL"}}`, false},
		{"401 does not retire", 401, `{"error":{"status":"UNAUTHENTICATED"}}`, false},
	}
	for _, c := range cases {
		if got := fcm_retire(c.status, []byte(c.body)); got != c.retire {
			t.Errorf("%s: fcm_retire(%d, ...) = %v, want %v", c.name, c.status, got, c.retire)
		}
	}
}

func TestFcmErrorCode(t *testing.T) {
	cases := []struct {
		name string
		body string
		code string
	}{
		{"details errorCode preferred", fcm_body_unregistered, "UNREGISTERED"},
		{"falls back to status", fcm_body_project_missing, "NOT_FOUND"},
		{"unparseable is empty", "not json", ""},
	}
	for _, c := range cases {
		if got := fcm_error_code([]byte(c.body)); got != c.code {
			t.Errorf("%s: fcm_error_code(...) = %q, want %q", c.name, got, c.code)
		}
	}
}

func TestFcmSummariseError(t *testing.T) {
	if got := fcm_summarise_error(404, []byte(fcm_body_unregistered)); got != "FCM 404 UNREGISTERED" {
		t.Errorf("fcm_summarise_error = %q, want %q", got, "FCM 404 UNREGISTERED")
	}
	if got := fcm_summarise_error(502, []byte("bad gateway")); got != "FCM 502" {
		t.Errorf("fcm_summarise_error = %q, want %q", got, "FCM 502")
	}
}

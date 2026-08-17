// Mochi server: a push gets two attempts, and the second one covers transport
// failures as well as 5xx.
//
// A push has no queue behind it. account_deliver_fcm's caller keeps the account
// row on a transport failure (the token is fine) and then moves on, so whatever
// fcm_post returns is the only attempt the notification gets - a failure is a
// phone that never buzzes. The retry existed for exactly that reason but only
// covered a 5xx *response*; a transport error returned on the spot. yuzu showed
// both arms in one fortnight: a 500 on 2026-08-12 was retried and recovered, and
// a TLS handshake timeout on 2026-08-17 was dropped.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// retry_test_no_backoff removes the two-second pause for the duration of a
// test, so exercising the retry costs nothing.
func retry_test_no_backoff(t *testing.T) {
	t.Helper()
	original := fcm_retry_backoff
	fcm_retry_backoff = 0
	t.Cleanup(func() { fcm_retry_backoff = original })
}

// retry_test_transport fails the first n attempts at the transport layer -
// before any response exists, which is what a TLS handshake timeout is - then
// delegates to the real transport.
type retry_test_transport struct {
	fail  int32
	tries int32
	inner http.RoundTripper
}

func (r *retry_test_transport) RoundTrip(req *http.Request) (*http.Response, error) {
	n := atomic.AddInt32(&r.tries, 1)
	if n <= atomic.LoadInt32(&r.fail) {
		return nil, errors.New("net/http: TLS handshake timeout")
	}
	return r.inner.RoundTrip(req)
}

// TestTransportFailureIsRetried is the finding. One TLS handshake timeout must
// not cost the notification.
func TestTransportFailureIsRetried(t *testing.T) {
	retry_test_no_backoff(t)

	var served int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&served, 1)
		w.WriteHeader(200)
	}))
	defer server.Close()

	transport := &retry_test_transport{fail: 1, inner: http.DefaultTransport}
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}

	resp, err := fcm_post(client, server.URL, "token", []byte(`{"message":{}}`))
	if err != nil {
		t.Fatalf("a single transport failure lost the push: %v\nthe retry does not cover transport errors, so a TLS handshake timeout drops the notification with no queue behind it", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("status %d, want 200", resp.StatusCode)
	}
	if got := atomic.LoadInt32(&transport.tries); got != 2 {
		t.Errorf("%d attempts, want 2", got)
	}
	if got := atomic.LoadInt32(&served); got != 1 {
		t.Errorf("the server saw %d requests, want 1 - the retry did not reach it", got)
	}
}

// TestTransportFailureGivesUpAfterTwo. Retrying is not the same as retrying for
// ever: a genuinely unreachable FCM must return, not spin, and it must return
// the transport error so the caller says "Network error" rather than treating
// a nil response as success.
func TestTransportFailureGivesUpAfterTwo(t *testing.T) {
	retry_test_no_backoff(t)

	transport := &retry_test_transport{fail: 99, inner: http.DefaultTransport}
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}

	done := make(chan struct{})
	var resp *http.Response
	var err error
	go func() {
		resp, err = fcm_post(client, "http://127.0.0.1:1/x", "token", []byte(`{}`))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("fcm_post did not return; the retry has no attempt ceiling")
	}

	if err == nil {
		t.Fatal("a permanently failing transport returned no error")
	}
	if resp != nil {
		t.Error("an error was returned alongside a response; the caller dereferences the response on a nil error")
	}
	if errors.Is(err, fcm_request_error) {
		t.Error("a send failure was reported as a request-build failure")
	}
	if got := atomic.LoadInt32(&transport.tries); got != fcm_send_attempts {
		t.Errorf("%d attempts, want %d - a push must not retry for ever", got, fcm_send_attempts)
	}
}

// TestServerErrorIsStillRetried. The behaviour that already worked, pinned so
// widening the retry did not cost it.
func TestServerErrorIsStillRetried(t *testing.T) {
	retry_test_no_backoff(t)

	var served int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&served, 1) == 1 {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(200)
		io.WriteString(w, `{"name":"ok"}`)
	}))
	defer server.Close()

	resp, err := fcm_post(server.Client(), server.URL, "token", []byte(`{}`))
	if err != nil {
		t.Fatalf("fcm_post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("status %d after a retried 500, want 200", resp.StatusCode)
	}
	if got := atomic.LoadInt32(&served); got != 2 {
		t.Errorf("%d requests, want 2", got)
	}
}

// TestPermanentStatusIsNotRetried. A 404 UNREGISTERED and a 400 are the token's
// own end of life; retrying them wastes two seconds per dead phone and delays
// the row being dropped.
func TestPermanentStatusIsNotRetried(t *testing.T) {
	retry_test_no_backoff(t)

	for _, status := range []int{200, 400, 401, 404, 429} {
		var served int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&served, 1)
			w.WriteHeader(status)
		}))

		resp, err := fcm_post(server.Client(), server.URL, "token", []byte(`{}`))
		if err != nil {
			t.Fatalf("status %d: %v", status, err)
		}
		resp.Body.Close()
		if got := atomic.LoadInt32(&served); got != 1 {
			t.Errorf("status %d was sent %d times; only a 5xx or a transport failure is retriable", status, got)
		}
		server.Close()
	}
}

// retry_test_body records whether it was closed, so the assertion below is on
// the close itself rather than on a side effect that a leak would not disturb.
type retry_test_body struct {
	io.Reader
	closed *int32
}

func (b retry_test_body) Close() error {
	atomic.AddInt32(b.closed, 1)
	return nil
}

// retry_test_responder answers each attempt from a list of statuses, handing
// back an instrumented body.
type retry_test_responder struct {
	statuses []int
	tries    int32
	closed   int32
}

func (r *retry_test_responder) RoundTrip(req *http.Request) (*http.Response, error) {
	n := int(atomic.AddInt32(&r.tries, 1))
	status := r.statuses[len(r.statuses)-1]
	if n <= len(r.statuses) {
		status = r.statuses[n-1]
	}
	return &http.Response{
		StatusCode: status,
		Body:       retry_test_body{Reader: strings.NewReader("body"), closed: &r.closed},
		Header:     make(http.Header),
		Request:    req,
	}, nil
}

// TestRetriedBodyIsClosed. The 5xx response is discarded inside fcm_post, so
// nothing outside can close it; leaking it holds a connection per retry, and
// under a burst of FCM 5xx that is a file descriptor leak on the push path.
func TestRetriedBodyIsClosed(t *testing.T) {
	retry_test_no_backoff(t)

	responder := &retry_test_responder{statuses: []int{503, 200}}
	client := &http.Client{Transport: responder}

	resp, err := fcm_post(client, "http://example.invalid/send", "token", []byte(`{}`))
	if err != nil {
		t.Fatalf("fcm_post: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("status %d, want 200", resp.StatusCode)
	}
	if got := atomic.LoadInt32(&responder.closed); got != 1 {
		t.Errorf("%d bodies closed before the returned one; the discarded 5xx response was leaked", got)
	}
	resp.Body.Close()
	if got := atomic.LoadInt32(&responder.closed); got != 2 {
		t.Errorf("%d bodies closed after the caller closed its own, want 2", got)
	}
}

// TestSendPayloadSurvivesTheRetry. The body is an io.Reader and is consumed by
// the first attempt; a retry that reuses the spent reader posts an empty body,
// which FCM answers with a 400 that fcm_retire reads as a dead token - so the
// phone's registration is deleted because Google had a bad second.
func TestSendPayloadSurvivesTheRetry(t *testing.T) {
	retry_test_no_backoff(t)

	payload := []byte(`{"message":{"token":"abc","data":{"title":"hello"}}}`)
	bodies := make(chan string, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		read, _ := io.ReadAll(r.Body)
		bodies <- string(read)
		if len(bodies) == 1 {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(200)
	}))
	defer server.Close()

	resp, err := fcm_post(server.Client(), server.URL, "token", payload)
	if err != nil {
		t.Fatalf("fcm_post: %v", err)
	}
	defer resp.Body.Close()
	close(bodies)

	count := 0
	for body := range bodies {
		count++
		if body != string(payload) {
			t.Errorf("attempt %d posted %q, want the full payload", count, body)
		}
	}
	if count != 2 {
		t.Errorf("%d attempts recorded, want 2", count)
	}
}

// TestRequestBuildFailureIsNotRetried. A malformed URL fails identically every
// time, so retrying it only doubles the wait, and it must stay distinguishable:
// the caller reports it as a build failure, not a network one.
func TestRequestBuildFailureIsNotRetried(t *testing.T) {
	retry_test_no_backoff(t)

	transport := &retry_test_transport{fail: 0, inner: http.DefaultTransport}
	client := &http.Client{Transport: transport}

	resp, err := fcm_post(client, "://not a url", "token", []byte(`{}`))
	if err == nil {
		resp.Body.Close()
		t.Fatal("a malformed URL built a request")
	}
	if !errors.Is(err, fcm_request_error) {
		t.Errorf("build failure reported as %v; the caller cannot tell it from a network error", err)
	}
	if got := atomic.LoadInt32(&transport.tries); got != 0 {
		t.Errorf("the transport was reached %d times for an unbuildable request", got)
	}
}

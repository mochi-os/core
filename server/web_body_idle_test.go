// Mochi server: request bodies that stop arriving, and the static refund
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// body_idle_server serves POST / through web_body_limit, reading the whole
// body and reporting how the read ended.
func body_idle_server(t *testing.T) (string, chan error) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	ended := make(chan error, 4)
	engine := gin.New()
	engine.Use(web_body_limit)
	engine.POST("/", func(c *gin.Context) {
		_, err := io.ReadAll(c.Request.Body)
		ended <- err
		c.Status(http.StatusOK)
	})
	server := httptest.NewServer(engine)
	t.Cleanup(server.Close)
	return strings.TrimPrefix(server.URL, "http://"), ended
}

// body_idle_send opens a connection and sends the headers of a POST declaring
// length bytes and the first part of its body.
func body_idle_send(t *testing.T, address string, length int, first string) net.Conn {
	t.Helper()
	connection, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connection.Close() })
	fmt.Fprintf(connection, "POST / HTTP/1.1\r\nHost: test\r\nContent-Type: application/octet-stream\r\nContent-Length: %d\r\n\r\n%s", length, first)
	return connection
}

// A body that stops arriving is cut once it has been idle for web_body_idle.
// Before, nothing bounded it: a signed-in client could hold a goroutine and a
// descriptor for as long as it kept the connection open.
func TestRequestBodyThatStopsIsCut(t *testing.T) {
	previous := web_body_idle
	web_body_idle = 300 * time.Millisecond
	t.Cleanup(func() { web_body_idle = previous })

	address, ended := body_idle_server(t)
	body_idle_send(t, address, 100, "abc")
	select {
	case err := <-ended:
		if err == nil {
			t.Error("the body read ended without an error though 97 bytes never came")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("THE POINT: a body that stopped arriving was still being waited for after 5 s, with a 300 ms idle bound")
	}
}

// A body that keeps arriving, however slowly, is never cut: each read moves the
// deadline on. Its whole run is several times the idle bound.
func TestRequestBodyThatKeepsMovingIsRead(t *testing.T) {
	previous := web_body_idle
	web_body_idle = 300 * time.Millisecond
	t.Cleanup(func() { web_body_idle = previous })

	address, ended := body_idle_server(t)
	connection := body_idle_send(t, address, 10, "a")
	for i := 0; i < 9; i++ {
		time.Sleep(150 * time.Millisecond)
		if _, err := connection.Write([]byte("b")); err != nil {
			t.Fatalf("writing byte %d: %v", i, err)
		}
	}
	select {
	case err := <-ended:
		if err != nil {
			t.Errorf("a body that kept arriving, 1.35 s in all, was cut: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the slow body was never read to its end")
	}
}

// An upgrade carries no body and gets no deadline, which would otherwise
// outlive the request on the websocket.
func TestUpgradeGetsNoBodyDeadline(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/_/websocket", nil)
	c.Request.Header.Set("Upgrade", "websocket")
	web_body_limit(c)
	if _, wrapped := c.Request.Body.(*web_idle_body); wrapped {
		t.Error("an upgrade request's body was given the idle deadline")
	}
}

// The default budget is 10,000 requests a minute per address.
func TestRateLimitDefaultBudget(t *testing.T) {
	if rate_limit_api.limit != 10000 && os.Getenv("MOCHI_CONFIG") == "" {
		t.Errorf("the default request budget is %d a minute, want 10,000", rate_limit_api.limit)
	}
}

// A refund gives one request back within the window, so the next one is
// allowed, and never takes the count below nothing.
func TestRateLimitRefund(t *testing.T) {
	limiter := &rate_limiter{entries: map[string]*rate_limit_entry{}, limit: 3, window: 60}
	for i := 0; i < 3; i++ {
		if !limiter.allow("a") {
			t.Fatalf("request %d refused under the limit", i)
		}
	}
	if limiter.allow("a") {
		t.Fatal("CONTROL: a fourth request was allowed at a limit of 3")
	}
	limiter.refund("a")
	if !limiter.allow("a") {
		t.Error("THE POINT: a refunded request did not give its place back")
	}
	limiter.refund("b")
	if _, found := limiter.entries["b"]; found {
		t.Error("a refund for an address with no window made one")
	}
}

// Every branch of web_action that answers a static file from a revalidation or
// a stored compressed copy refunds its budget, and nothing else does: pinned at
// source level, as web_action is not driven by a unit test.
func TestStaticFilesAnsweredCheaplyAreRefunded(t *testing.T) {
	source, err := os.ReadFile("web.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	if n := strings.Count(text, "web_static_refund(c)"); n != 4 {
		t.Errorf("web_static_refund is called %d times, want 4: after each revalidation and each compressed copy, in both static branches", n)
	}
	for _, guard := range []string{
		"if web_cache_static(c, file, aa.Cache) {\n\t\t\tweb_static_refund(c)",
		"if web_cache_static(c, file, aa.Cache) {\n\t\t\t\tweb_static_refund(c)",
		"if web_serve_compressed(c, a, av.base, file) {\n\t\t\tweb_static_refund(c)",
		"} else if web_serve_compressed(c, a, av.base, file) {\n\t\t\t\tweb_static_refund(c)",
	} {
		if !strings.Contains(text, guard) {
			t.Errorf("THE POINT: no refund where the static file is answered cheaply: %q", guard)
		}
	}
}

// A refunded static file is charged to the static budget instead, so refunds
// stop once that is spent: a client asking for cheap static answers cannot
// make every request free.
func TestStaticRefundIsBounded(t *testing.T) {
	gin.SetMode(gin.TestMode)
	api, static := rate_limit_api, rate_limit_static
	t.Cleanup(func() { rate_limit_api, rate_limit_static = api, static })
	rate_limit_api = &rate_limiter{entries: map[string]*rate_limit_entry{}, limit: 10, window: 60}
	rate_limit_static = &rate_limiter{entries: map[string]*rate_limit_entry{}, limit: 2, window: 60}

	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodGet, "/feeds/assets/a.js", nil)
	ip := rate_limit_client_ip(c)
	for i := 0; i < 5; i++ {
		rate_limit_api.allow(ip)
		web_static_refund(c)
	}
	if count := rate_limit_api.entries[ip].count; count != 3 {
		t.Errorf("THE POINT: after 5 refunded files with a static budget of 2 the request budget holds %d, want 3", count)
	}
}

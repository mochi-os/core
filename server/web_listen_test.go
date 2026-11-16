// Mochi server: listener connection limit and connection close tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestWebListenBoundsConnections(t *testing.T) {
	listener, err := web_listen("127.0.0.1:0", 2)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	// Accept in the background so the limiter, not a missing Accept call, is
	// what governs.
	accepted := make(chan net.Conn, 8)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			accepted <- conn
		}
	}()

	// Two connections fit the ceiling.
	held := []net.Conn{}
	defer func() {
		for _, c := range held {
			c.Close()
		}
	}()
	for i := 0; i < 2; i++ {
		conn, err := net.DialTimeout("tcp", listener.Addr().String(), 5*time.Second)
		if err != nil {
			t.Fatalf("connection %d was refused below the ceiling: %v", i+1, err)
		}
		held = append(held, conn)
		select {
		case server := <-accepted:
			held = append(held, server)
		case <-time.After(5 * time.Second):
			t.Fatalf("connection %d was never accepted", i+1)
		}
	}

	// The third must not be accepted. The TCP handshake still completes — it
	// waits in the kernel backlog, holding no descriptor of ours — so the
	// check is that the server side never sees it.
	extra, err := net.DialTimeout("tcp", listener.Addr().String(), 5*time.Second)
	if err == nil {
		defer extra.Close()
	}
	select {
	case <-accepted:
		t.Error("a connection past the ceiling was accepted, so the limit does not bind")
	case <-time.After(500 * time.Millisecond):
		// Correct: left queued rather than accepted.
	}

	// Releasing one must free a slot, or the limit would be a permanent cap on
	// the server's life rather than on simultaneous use.
	held[0].Close()
	held[1].Close()
	select {
	case server := <-accepted:
		server.Close()
	case <-time.After(5 * time.Second):
		t.Error("closing a connection did not free a slot for the queued one")
	}
}

// TestWebListenUnlimited pins the documented escape hatch: a configured zero
// disables the ceiling rather than refusing every connection.
func TestWebListenUnlimited(t *testing.T) {
	listener, err := web_listen("127.0.0.1:0", 0)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			conn.Close()
		}
	}()

	for i := 0; i < 4; i++ {
		conn, err := net.DialTimeout("tcp", listener.Addr().String(), 5*time.Second)
		if err != nil {
			t.Fatalf("connection %d refused with the limit disabled: %v", i+1, err)
		}
		conn.Close()
	}
}

// TestWebAnswerSurvivesAnUnreadBodyOnAClosingConnection: a client that asks to
// close the connection sends a body the handler never reads, as a forums view
// whose input comes from its path ignores the form sent with it. net/http drains
// such a body only on a connection it keeps, so it closed this one with the
// body unread, the kernel answered the unread bytes with a reset, and the
// client lost the end of a large answer: about one forums view of 10,500 rows
// in twenty, though the server logged a 200.
func TestWebAnswerSurvivesAnUnreadBodyOnAClosingConnection(t *testing.T) {
	answer := bytes.Repeat([]byte("0123456789abcdef"), 1<<18)
	listener, err := web_listen("127.0.0.1:0", 0)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := web_server(listener.Addr().String(), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(answer)
	}))
	go server.Serve(listener)
	defer server.Close()

	body := "forum=" + strings.Repeat("x", 51)
	for round := range 5 {
		conn, err := net.DialTimeout("tcp", listener.Addr().String(), 5*time.Second)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		// The head and the body in two writes, as Python's http.client sends
		// them: the server reads the head alone, and the body arrives after.
		fmt.Fprintf(conn, "POST /forums/x/-/restrictions HTTP/1.1\r\nHost: test\r\nConnection: close\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: %d\r\n\r\n", len(body))
		time.Sleep(20 * time.Millisecond)
		io.WriteString(conn, body)
		// The server answers and closes while the client has still to read.
		time.Sleep(200 * time.Millisecond)
		conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		response, err := http.ReadResponse(bufio.NewReader(conn), nil)
		if err != nil {
			conn.Close()
			t.Fatalf("round %d: reading the answer's head: %v", round, err)
		}
		data, err := io.ReadAll(response.Body)
		conn.Close()
		if err != nil || len(data) != len(answer) {
			t.Fatalf("THE POINT: round %d: the client read %d of %d bytes: %v", round, len(data), len(answer), err)
		}
	}
}

// TestWebDrainAnswersAnExpectingClientAtOnce: a client that asks to be told to
// continue sends no body until it is. Answered without being told - an upload
// refused before its body was read - it waited out the drain for a body that
// was never coming.
func TestWebDrainAnswersAnExpectingClientAtOnce(t *testing.T) {
	listener, err := web_listen("127.0.0.1:0", 0)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := web_server(listener.Addr().String(), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		io.WriteString(w, "refused")
	}))
	go server.Serve(listener)
	defer server.Close()

	conn, err := net.DialTimeout("tcp", listener.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	io.WriteString(conn, "POST /upload HTTP/1.1\r\nHost: test\r\nExpect: 100-continue\r\nContent-Length: 1000000\r\n\r\n")
	conn.SetReadDeadline(time.Now().Add(time.Second))
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("THE POINT: the refusal did not come within a second: %v", err)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil || response.StatusCode != http.StatusUnauthorized || string(body) != "refused" {
		t.Errorf("THE POINT: the refusal came as %d %q (%v), within a second", response.StatusCode, body, err)
	}
}

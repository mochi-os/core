// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"nhooyr.io/websocket"
)

// xmpp_web is the engine's web paths on a test server.
func xmpp_web(t *testing.T) *httptest.Server {
	t.Helper()
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Any("/*path", func(c *gin.Context) { xmpp_web_route(c) })
	server := httptest.NewServer(router)
	t.Cleanup(server.Close)
	return server
}

// An xmpp_socket is a WebSocket client of the engine's.
type xmpp_socket struct {
	conn *websocket.Conn
}

func (w *xmpp_socket) send(t *testing.T, text string) {
	t.Helper()
	if err := w.conn.Write(context.Background(), websocket.MessageText, []byte(text)); err != nil {
		t.Fatalf("send: %v", err)
	}
}

// read waits for the next message.
func (w *xmpp_socket) read(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, data, err := w.conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return string(data)
}

// until reads messages until one holds every part, and answers it.
func (w *xmpp_socket) until(t *testing.T, what string, parts ...string) string {
	t.Helper()
	for range 20 {
		text := w.read(t)
		all := true
		for _, part := range parts {
			all = all && strings.Contains(text, part)
		}
		if all {
			return text
		}
	}
	t.Fatalf("%s: never came", what)
	return ""
}

// TestXMPPWebSocket: a browser's client signs in over a WebSocket (RFC 7395),
// with no STARTTLS offered since the web server's TLS secures it, a stream
// restarted by a new open; it binds, and its messages go to and come from a
// client on the client port; it ends the stream with close. A WebSocket not
// for the xmpp subprotocol is refused.
func TestXMPPWebSocket(t *testing.T) {
	env := create_xmpp_server_env(t)
	fake := env.apps(t)
	fake.allow(env.alice, "bob@example.com")
	fake.allow(env.bob, "alice@example.com")
	web := xmpp_web(t)
	address := "ws" + strings.TrimPrefix(web.URL, "http") + "/xmpp/websocket"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if conn, _, err := websocket.Dial(ctx, address, nil); err == nil {
		_, _, err = conn.Read(ctx)
		if websocket.CloseStatus(err) != websocket.StatusPolicyViolation {
			t.Errorf("a WebSocket for no subprotocol was taken: %v", err)
		}
	}

	conn, _, err := websocket.Dial(ctx, address, &websocket.DialOptions{Subprotocols: []string{"xmpp"}})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")
	w := &xmpp_socket{conn: conn}
	w.send(t, `<open xmlns="urn:ietf:params:xml:ns:xmpp-framing" to="example.com" version="1.0"/>`)
	if opened := w.read(t); !strings.Contains(opened, `<open xmlns="urn:ietf:params:xml:ns:xmpp-framing"`) || !strings.Contains(opened, `from="example.com"`) {
		t.Fatalf("THE POINT: the stream opened as %s", opened)
	}
	features := w.read(t)
	if !strings.Contains(features, "<mechanism>PLAIN</mechanism>") || strings.Contains(features, "starttls") || !strings.Contains(features, `xmlns:stream=`) {
		t.Fatalf("the features: %s", features)
	}
	password := env.mint(t, env.alice, "x1", "Browser")
	w.send(t, `<auth xmlns="urn:ietf:params:xml:ns:xmpp-sasl" mechanism="PLAIN">`+base64.StdEncoding.EncodeToString([]byte("\x00alice\x00"+password))+`</auth>`)
	if answer := w.read(t); !strings.Contains(answer, "<success") {
		t.Fatalf("the sign-in: %s", answer)
	}
	w.send(t, `<open xmlns="urn:ietf:params:xml:ns:xmpp-framing" to="example.com" version="1.0"/>`)
	w.until(t, "the stream opened again", "<open")
	if features := w.read(t); !strings.Contains(features, "urn:ietf:params:xml:ns:xmpp-bind") || !strings.Contains(features, "urn:xmpp:sm:3") {
		t.Fatalf("the features after signing in: %s", features)
	}
	w.send(t, `<iq xmlns="jabber:client" type="set" id="b1"><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"><resource>web</resource></bind></iq>`)
	w.until(t, "the resource bound", "alice@example.com/web</jid>")
	w.send(t, `<presence xmlns="jabber:client"/>`)

	_, b := env.pair(t)
	w.send(t, `<message xmlns="jabber:client" to="bob@example.com" type="chat" id="w1"><body>From the browser</body></message>`)
	b.expect(t, "THE POINT: a message from the browser", "From the browser")
	b.send(t, `<message xmlns="jabber:client" to="alice@example.com/web" type="chat" id="w2"><body>To the browser</body></message>`)
	w.until(t, "THE POINT: a message to the browser", "To the browser")

	w.send(t, `<close xmlns="urn:ietf:params:xml:ns:xmpp-framing"/>`)
	w.until(t, "the stream closed", "<close")
	wait(t, "the resource to go", func() bool { return xmpp_resource_get("alice@example.com", "web") == nil })
}

// A xmpp_bosh_client is a BOSH client of the engine's.
type xmpp_bosh_client struct {
	address string
	sid     string
	rid     int
}

// post sends a body and answers the one that comes back.
func (b *xmpp_bosh_client) post(t *testing.T, attrs string, inner string) string {
	t.Helper()
	b.rid++
	text := `<body xmlns="http://jabber.org/protocol/httpbind" rid="` + itoa(b.rid) + `"`
	if b.sid != "" {
		text += ` sid="` + b.sid + `"`
	}
	text += ` ` + attrs + `>` + inner + `</body>`
	response, err := http.Post(b.address, "text/xml; charset=utf-8", strings.NewReader(text))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, _ := io.ReadAll(response.Body)
	return string(data)
}

// TestXMPPBosh: a browser's client signs in over BOSH (XEP-0124, XEP-0206):
// the session's first answer carries its id and the stream's features, a
// restart opens the stream again, and a request is held until something
// comes - a message from a client on the client port - or the wait passes.
// A request of an unknown session is answered with the session's end, as is
// one ending it, after which its resource goes.
func TestXMPPBosh(t *testing.T) {
	env := create_xmpp_server_env(t)
	fake := env.apps(t)
	fake.allow(env.alice, "bob@example.com")
	fake.allow(env.bob, "alice@example.com")
	saved := xmpp_bosh_wait
	xmpp_bosh_wait = 2 * time.Second
	t.Cleanup(func() { xmpp_bosh_wait = saved })
	web := xmpp_web(t)
	b := &xmpp_bosh_client{address: web.URL + "/xmpp/bosh", rid: 1000}

	stranger := &xmpp_bosh_client{address: b.address, sid: "nobody", rid: 5}
	if answer := stranger.post(t, "", ""); !strings.Contains(answer, `type="terminate"`) || !strings.Contains(answer, "item-not-found") {
		t.Errorf("a request of no session: %s", answer)
	}
	if answer := (&xmpp_bosh_client{address: b.address}).post(t, `to="elsewhere.test" wait="2" hold="1" xmlns:xmpp="urn:xmpp:xbosh" xmpp:version="1.0"`, ""); !strings.Contains(answer, "host-unknown") {
		t.Errorf("a session for another domain: %s", answer)
	}

	first := b.post(t, `to="example.com" wait="2" hold="1" ver="1.6" xmlns:xmpp="urn:xmpp:xbosh" xmpp:version="1.0"`, "")
	match := regexp.MustCompile(`sid="([^"]+)"`).FindStringSubmatch(first)
	if match == nil || !strings.Contains(first, "<mechanism>PLAIN</mechanism>") || strings.Contains(first, "starttls") {
		t.Fatalf("THE POINT: the session's first answer: %s", first)
	}
	b.sid = match[1]
	password := env.mint(t, env.alice, "x1", "Browser")
	if answer := b.post(t, "", `<auth xmlns="urn:ietf:params:xml:ns:xmpp-sasl" mechanism="PLAIN">`+base64.StdEncoding.EncodeToString([]byte("\x00alice\x00"+password))+`</auth>`); !strings.Contains(answer, "<success") {
		t.Fatalf("the sign-in: %s", answer)
	}
	if answer := b.post(t, `to="example.com" xmlns:xmpp="urn:xmpp:xbosh" xmpp:restart="true"`, ""); !strings.Contains(answer, "urn:ietf:params:xml:ns:xmpp-bind") {
		t.Fatalf("the restart: %s", answer)
	}
	if answer := b.post(t, "", `<iq xmlns="jabber:client" type="set" id="b1"><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"><resource>bosh</resource></bind></iq>`); !strings.Contains(answer, "alice@example.com/bosh</jid>") {
		t.Fatalf("the binding: %s", answer)
	}
	b.post(t, "", `<presence xmlns="jabber:client"/>`)

	_, bob := env.pair(t)
	b.post(t, "", `<message xmlns="jabber:client" to="bob@example.com" type="chat" id="h1"><body>Over BOSH</body></message>`)
	bob.expect(t, "THE POINT: a message over BOSH", "Over BOSH")
	go func() {
		time.Sleep(300 * time.Millisecond)
		bob.send(t, `<message xmlns="jabber:client" to="alice@example.com/bosh" type="chat" id="h2"><body>Held for you</body></message>`)
	}()
	held := ""
	asked := time.Now()
	for range 5 {
		if held = b.post(t, "", ""); strings.Contains(held, "Held for you") {
			break
		}
	}
	if !strings.Contains(held, "Held for you") || time.Since(asked) > 1500*time.Millisecond {
		t.Errorf("THE POINT: the held request was not answered with the message as it came (%v): %s", time.Since(asked), held)
	}
	started := time.Now()
	if idle := b.post(t, "", ""); strings.Contains(idle, "<message") || time.Since(started) < time.Second {
		t.Errorf("a request with nothing for it came back at once: %s", idle)
	}
	if ended := b.post(t, `type="terminate"`, `<presence xmlns="jabber:client" type="unavailable"/>`); !strings.Contains(ended, `type="terminate"`) {
		t.Errorf("the session's end: %s", ended)
	}
	wait(t, "the resource to go", func() bool { return xmpp_resource_get("alice@example.com", "bosh") == nil })
}

// TestXMPPHostMeta: host-meta names a domain's WebSocket and BOSH addresses
// (XEP-0156), as XML and JSON, beside WebFinger's template while the
// fediverse is on; a host with no XMPP accounts has no XMPP links.
func TestXMPPHostMeta(t *testing.T) {
	create_xmpp_server_env(t)
	answer := func(host string, path string) string {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("GET", "https://"+host+path, nil)
		host_meta_route(c)
		return w.Body.String()
	}
	text := answer("example.com", "/.well-known/host-meta")
	if !strings.Contains(text, `rel="urn:xmpp:alt-connections:websocket" href="wss://example.com/xmpp/websocket"`) || !strings.Contains(text, `rel="urn:xmpp:alt-connections:xbosh" href="https://example.com/xmpp/bosh"`) {
		t.Errorf("THE POINT: host-meta: %s", text)
	}
	if json := answer("example.com", "/.well-known/host-meta.json"); !strings.Contains(json, `"href":"wss://example.com/xmpp/websocket"`) {
		t.Errorf("host-meta as JSON: %s", json)
	}
	if other := answer("elsewhere.test", "/.well-known/host-meta"); strings.Contains(other, "xmpp") {
		t.Errorf("a host with no XMPP accounts: %s", other)
	}
}

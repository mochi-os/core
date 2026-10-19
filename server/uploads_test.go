// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"testing"

	sl "go.starlark.net/starlark"
)

// xmpp_web_request asks the engine's web paths for an address as if on a
// host, with a token where one is given.
func xmpp_web_request(t *testing.T, web string, method string, address string, host string, token string, body string, kind string) (int, string, http.Header) {
	t.Helper()
	path := address[strings.Index(address, "/xmpp/"):]
	request, _ := http.NewRequest(method, web+path, strings.NewReader(body))
	request.Host = host
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	if kind != "" {
		request.Header.Set("Content-Type", kind)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, _ := io.ReadAll(response.Body)
	return response.StatusCode, string(data), response.Header
}

// TestXMPPUploads: the server offers its upload service (XEP-0363) and its
// largest file; a hosted account's client is given a slot, refused for a
// file larger than that; the file is put to the slot with its token, once,
// within its lifetime, and fetched by anyone from the same address on the
// account's domain - shown where it is an image, saved otherwise. A file chat
// sends through the account is kept the same way and sent as its address.
func TestXMPPUploads(t *testing.T) {
	env := create_xmpp_server_env(t)
	fake := env.apps(t)
	fake.allow(env.bob, "alice@example.com")
	web := xmpp_web(t).URL
	c := env.raw(t, "alice@example.com", env.mint(t, env.alice, "x1", "Phone"), "phone")

	c.send(`<iq xmlns="jabber:client" type="get" id="d1" to="example.com"><query xmlns="http://jabber.org/protocol/disco#info"/></iq>`)
	c.expect(t, "the upload service offered", `id="d1"`, `var="urn:xmpp:http:upload:0"`, "<value>"+strconv.Itoa(media_maximum)+"</value>")

	c.send(`<iq xmlns="jabber:client" type="get" id="s0" to="example.com"><request xmlns="urn:xmpp:http:upload:0" filename="huge.bin" size="` + strconv.Itoa(media_maximum+1) + `" content-type="application/octet-stream"/></iq>`)
	c.expect(t, "THE POINT: a file too large refused", `id="s0"`, "file-too-large")

	c.send(`<iq xmlns="jabber:client" type="get" id="s1" to="example.com"><request xmlns="urn:xmpp:http:upload:0" filename="photo one.png" size="9" content-type="image/png"/></iq>`)
	slot := c.expect(t, "a slot", `id="s1"`, "<slot")
	put := regexp.MustCompile(`<put url="([^"]+)">`).FindStringSubmatch(slot)
	token := regexp.MustCompile(`Bearer ([A-Za-z0-9]+)`).FindStringSubmatch(slot)
	get := regexp.MustCompile(`<get url="([^"]+)"`).FindStringSubmatch(slot)
	if put == nil || token == nil || get == nil || !strings.HasPrefix(put[1], "https://example.com/xmpp/upload/") || !strings.HasSuffix(put[1], "/photo%20one.png") {
		t.Fatalf("THE POINT: the slot: %s", slot)
	}
	address := strings.ReplaceAll(put[1], "&amp;", "&")

	if status, _, _ := xmpp_web_request(t, web, "GET", address, "example.com", "", "", ""); status != http.StatusNotFound {
		t.Errorf("a file fetched before it was put: %d", status)
	}
	if status, _, _ := xmpp_web_request(t, web, "PUT", address, "example.com", "wrong", "png bytes", "image/png"); status != http.StatusForbidden {
		t.Errorf("THE POINT: a put with the wrong token: %d", status)
	}
	if status, _, _ := xmpp_web_request(t, web, "PUT", address, "example.com", token[1], "too many bytes", "image/png"); status != http.StatusBadRequest {
		t.Errorf("a put of another size: %d", status)
	}
	if status, _, _ := xmpp_web_request(t, web, "PUT", address, "example.com", token[1], "png bytes", "image/png"); status != http.StatusCreated {
		t.Fatalf("THE POINT: the put: %d", status)
	}
	if status, _, _ := xmpp_web_request(t, web, "PUT", address, "example.com", token[1], "png other", "image/png"); status != http.StatusForbidden {
		t.Errorf("THE POINT: a second put taken: %d", status)
	}
	status, body, header := xmpp_web_request(t, web, "GET", address, "example.com", "", "", "")
	if status != http.StatusOK || body != "png bytes" || header.Get("Content-Type") != "image/png" || !strings.HasPrefix(header.Get("Content-Disposition"), "inline") || header.Get("X-Content-Type-Options") != "nosniff" {
		t.Errorf("THE POINT: the file fetched: %d %q %v", status, body, header)
	}
	if status, _, _ := xmpp_web_request(t, web, "GET", address, "elsewhere.test", "", "", ""); status != http.StatusNotFound {
		t.Errorf("a file fetched on another host: %d", status)
	}

	c.send(`<iq xmlns="jabber:client" type="get" id="s2" to="example.com"><request xmlns="urn:xmpp:http:upload:0" filename="page.html" size="4" content-type="text/html"/></iq>`)
	slot = c.expect(t, "a second slot", `id="s2"`, "<slot")
	address = regexp.MustCompile(`<put url="([^"]+)">`).FindStringSubmatch(slot)[1]
	token = regexp.MustCompile(`Bearer ([A-Za-z0-9]+)`).FindStringSubmatch(slot)
	xmpp_web_request(t, web, "PUT", address, "example.com", token[1], "<b/>", "text/html")
	if _, _, header := xmpp_web_request(t, web, "GET", address, "example.com", "", "", ""); !strings.HasPrefix(header.Get("Content-Disposition"), "attachment") || !strings.Contains(header.Get("Content-Security-Policy"), "sandbox") {
		t.Errorf("THE POINT: a page is saved, never shown: %v", header)
	}

	c.send(`<iq xmlns="jabber:client" type="get" id="s3" to="example.com"><request xmlns="urn:xmpp:http:upload:0" filename="late.txt" size="4" content-type="text/plain"/></iq>`)
	slot = c.expect(t, "a third slot", `id="s3"`, "<slot")
	address = regexp.MustCompile(`<put url="([^"]+)">`).FindStringSubmatch(slot)[1]
	token = regexp.MustCompile(`Bearer ([A-Za-z0-9]+)`).FindStringSubmatch(slot)
	xmpp_database().exec("update uploads set created=created-3600 where name='late.txt'")
	if status, _, _ := xmpp_web_request(t, web, "PUT", address, "example.com", token[1], "late", "text/plain"); status != http.StatusGone {
		t.Errorf("THE POINT: a slot used past its lifetime: %d", status)
	}

	// From chat: kept as the account's upload, sent as its address.
	b := env.bob_online(t)
	uploaded := sl_decode(xmpp_call(t, env.alice, api_account_upload, sl.Tuple{sl.String("x1"), sl.Bytes("chat file"), sl.String("text/plain")}, sl.Tuple{sl.String("name"), sl.String("notes.txt")})).(map[string]any)
	link, _ := uploaded["url"].(string)
	if !strings.HasPrefix(link, "https://example.com/xmpp/upload/") || !strings.HasSuffix(link, "/notes.txt") {
		t.Fatalf("THE POINT: chat's upload: %v", uploaded)
	}
	if status, body, _ := xmpp_web_request(t, web, "GET", link, "example.com", "", "", ""); status != http.StatusOK || body != "chat file" {
		t.Errorf("chat's file fetched: %d %q", status, body)
	}
	file := sl.NewDict(1)
	file.SetKey(sl.String("url"), sl.String(link))
	object := sl.NewDict(2)
	object.SetKey(sl.String("conversation"), sl.String("xmpp:bob@example.com"))
	object.SetKey(sl.String("file"), file)
	xmpp_call(t, env.alice, api_account_object_create, sl.Tuple{sl.String("x1"), object})
	b.expect(t, "THE POINT: chat's file sent as its address", link+"</body>", "jabber:x:oob", link+"</url>")
}

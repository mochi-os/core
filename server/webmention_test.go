// Mochi server: Webmention tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	sl "go.starlark.net/starlark"
)

// create_webmention_test_env is the WebSub environment with a feed entity of
// alice's whose class the test app handles.
func create_webmention_test_env(t *testing.T) (*User, *App, *websub_calls) {
	t.Helper()
	alice, a, calls := create_websub_test_env(t)
	db_open("db/users.db").exec("insert into entities (id, private, fingerprint, user, parent, class, name, privacy, data, published) values ('wmfeed1', 'private', 'wmfp00001', ?, '', 'wmfeed', 'Notes', 'public', '', 0)", alice.UID)
	networks_database().exec("delete from webmentions")
	return alice, a, calls
}

// A webmention_pages serves the pages a test names, by path, and records the
// mentions its endpoint is sent.
type webmention_pages struct {
	server   *httptest.Server
	lock     sync.Mutex
	pages    map[string]webmention_page
	mentions []url.Values
	status   int
}

type webmention_page struct {
	status  int
	kind    string
	headers map[string]string
	body    string
}

func create_webmention_pages(t *testing.T) *webmention_pages {
	p := &webmention_pages{pages: map[string]webmention_page{}, status: http.StatusAccepted}
	p.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.lock.Lock()
		defer p.lock.Unlock()
		if r.Method == http.MethodPost {
			r.ParseForm()
			r.PostForm.Set("endpoint", r.URL.Path)
			p.mentions = append(p.mentions, r.PostForm)
			w.WriteHeader(p.status)
			return
		}
		page, found := p.pages[r.URL.Path]
		if !found {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		for k, v := range page.headers {
			w.Header().Add(k, strings.ReplaceAll(v, "{base}", p.server.URL))
		}
		kind := page.kind
		if kind == "" {
			kind = "text/html; charset=utf-8"
		}
		w.Header().Set("Content-Type", kind)
		if page.status != 0 {
			w.WriteHeader(page.status)
		}
		io.WriteString(w, strings.ReplaceAll(page.body, "{base}", p.server.URL))
	}))
	t.Cleanup(p.server.Close)
	return p
}

func (p *webmention_pages) set(path string, page webmention_page) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.pages[path] = page
}

func (p *webmention_pages) received() []url.Values {
	p.lock.Lock()
	defer p.lock.Unlock()
	return append([]url.Values{}, p.mentions...)
}

// webmention_mention sends a mention to alice's feed's endpoint.
func webmention_mention(source string, target string) int {
	return websub_request("POST", "/webmention/wmfeed1", url.Values{"source": {source}, "target": {target}}, nil, "").Code
}

// TestWebmentionReceive: a mention of a page here is accepted and checked
// afterwards; a source that links to the target arrives with its h-entry's
// author, text, address, time and kind; one that no longer links, or is gone,
// arrives as a deletion; one that cannot be reached waits and is tried again;
// a mention of a page elsewhere, of a page by itself, or for no entity, is
// refused, as is every mention with the networks off.
func TestWebmentionReceive(t *testing.T) {
	_, _, calls := create_webmention_test_env(t)
	blog := create_webmention_pages(t)
	target := "https://example.com/feeds/wmfp00001/post1"
	blog.set("/reply", webmention_page{body: `<html><head><title>A reply</title></head><body>
<article class="h-entry">
  <p class="p-author h-card"><a class="u-url p-name" href="/about">Carol</a><img class="u-photo" src="/carol.jpg"></p>
  <p>In reply to <a class="u-in-reply-to" href="` + target + `#top">the post</a></p>
  <div class="e-content">I   agree
  entirely.</div>
  <a class="u-url" href="{base}/reply"><time class="dt-published" datetime="2026-10-20T12:00:00Z">today</time></a>
</article></body></html>`})
	source := blog.server.URL + "/reply"
	if code := webmention_mention(source, target); code != http.StatusAccepted {
		t.Fatalf("THE POINT: the mention = %d", code)
	}
	if calls.count("webmention/receive") != 0 {
		t.Fatal("THE POINT: the mention was believed before it was checked")
	}
	webmention_work()
	got := calls.last("webmention/receive")
	if got == nil {
		t.Fatal("THE POINT: the checked mention never reached the app")
	}
	for k, v := range got {
		if sl_encode(v) == nil {
			t.Errorf("THE POINT: the mention's %s (%T) cannot reach the app's Starlark", k, v)
		}
	}
	author, _ := got["author"].(map[string]any)
	if got["source"] != source || got["target"] != target || got["entity"] != "wmfeed1" || got["deleted"] != false || got["kind"] != "reply" || got["content"] != "I agree entirely." || got["url"] != source || got["published"] != int64(1792497600) {
		t.Errorf("THE POINT: the mention = %v", got)
	}
	if author["name"] != "Carol" || author["url"] != blog.server.URL+"/about" || author["photo"] != blog.server.URL+"/carol.jpg" {
		t.Errorf("THE POINT: the author = %v", author)
	}
	if held, _ := networks_database().exists("select 1 from webmentions"); held {
		t.Error("THE POINT: a checked mention stayed queued")
	}

	blog.set("/like", webmention_page{body: `<div class="h-entry"><a class="u-like-of" href="` + target + `">liked</a></div>`})
	webmention_mention(blog.server.URL+"/like", target)
	webmention_work()
	if got := calls.last("webmention/receive"); got["kind"] != "like" || got["author"].(map[string]any)["name"] != "127.0.0.1" {
		t.Errorf("THE POINT: a like with no author = %v", got)
	}

	blog.set("/plain", webmention_page{body: `<html><head><title>Notes on things</title></head><body><p>See <a href="` + target + `">this</a>.</p></body></html>`})
	webmention_mention(blog.server.URL+"/plain", target)
	webmention_work()
	if got := calls.last("webmention/receive"); got["kind"] != "mention" || got["content"] != "Notes on things" || got["deleted"] != false {
		t.Errorf("THE POINT: a page with no h-entry = %v", got)
	}

	blog.set("/reply", webmention_page{body: `<div class="h-entry"><div class="e-content">Changed my mind.</div></div>`})
	webmention_mention(source, target)
	webmention_work()
	if got := calls.last("webmention/receive"); got["deleted"] != true || got["source"] != source {
		t.Errorf("THE POINT: a source that no longer links = %v", got)
	}
	blog.set("/reply", webmention_page{status: http.StatusGone})
	gone := calls.count("webmention/receive")
	webmention_mention(source, target)
	webmention_work()
	if got := calls.last("webmention/receive"); calls.count("webmention/receive") != gone+1 || got["deleted"] != true {
		t.Errorf("THE POINT: a source that is gone = %v", got)
	}

	blog.set("/plain-author", webmention_page{body: `<div class="h-entry"><span class="p-author"><a class="u-url" href="/dave">Dave</a></span><p>See <a href="` + target + `">this</a></p></div>`})
	webmention_mention(blog.server.URL+"/plain-author", target)
	webmention_work()
	if got := calls.last("webmention/receive"); got["url"] != blog.server.URL+"/plain-author" || got["author"].(map[string]any)["name"] != "Dave" {
		t.Errorf("THE POINT: an author's own address was taken for the entry's: %v", got)
	}

	before := calls.count("webmention/receive")
	blog.set("/down", webmention_page{status: http.StatusServiceUnavailable})
	webmention_mention(blog.server.URL+"/down", target)
	webmention_work()
	row, _ := networks_database().row("select attempts, next from webmentions where source=?", blog.server.URL+"/down")
	if calls.count("webmention/receive") != before || row == nil || row_int(row, "attempts") != 1 || row_int(row, "next") <= now() {
		t.Errorf("THE POINT: a source that could not be reached = %v, %d calls", row, calls.count("webmention/receive")-before)
	}

	if code := webmention_mention(source, "https://elsewhere.example/post"); code != http.StatusBadRequest {
		t.Errorf("THE POINT: a mention of a page elsewhere = %d", code)
	}
	if code := webmention_mention(target+"#a", target); code != http.StatusBadRequest {
		t.Errorf("THE POINT: a page mentioning itself = %d", code)
	}
	if code := websub_request("POST", "/webmention/nobody", url.Values{"source": {source}, "target": {target}}, nil, "").Code; code != http.StatusNotFound {
		t.Errorf("THE POINT: a mention for no entity = %d", code)
	}
	if code := websub_request("GET", "/webmention/wmfeed1", nil, nil, "").Code; code != http.StatusMethodNotAllowed {
		t.Errorf("a GET of the endpoint = %d", code)
	}
	networks_development = false
	if code := webmention_mention(source, target); code != http.StatusNotFound {
		t.Errorf("THE POINT: a mention with the networks off = %d", code)
	}
	networks_development = true
}

// TestWebmentionSend: a page here names the pages it links to; each target's
// endpoint is found in its Link header, else its first webmention link or
// anchor, resolved against it (an empty one the page itself), and told; a
// target with no endpoint is told nothing; an endpoint that fails waits and
// is tried again; a source not on this server is refused.
func TestWebmentionSend(t *testing.T) {
	alice, a, _ := create_webmention_test_env(t)
	thread := create_test_thread(alice, a)
	sites := create_webmention_pages(t)
	sites.set("/headed", webmention_page{headers: map[string]string{"Link": `</endpoint/one>; rel="webmention"`}, body: `<html></html>`})
	sites.set("/linked", webmention_page{body: `<html><head><link rel="stylesheet" href="/s.css"><link rel="webmention" href="/endpoint/two"></head><body><a rel="webmention" href="/wrong">x</a></body></html>`})
	sites.set("/anchored", webmention_page{body: `<html><body><a rel="nofollow webmention" href="">here</a></body></html>`})
	sites.set("/silent", webmention_page{body: `<html><body>no endpoint</body></html>`})
	source := "https://example.com/feeds/wmfp00001/post1"
	targets := sl.NewList([]sl.Value{sl.String(sites.server.URL + "/headed"), sl.String(sites.server.URL + "/linked"), sl.String(sites.server.URL + "/anchored"), sl.String(sites.server.URL + "/silent"), sl.String(sites.server.URL + "/headed#again"), sl.String(source)})
	value, err := api_webmention_send(thread, sl.NewBuiltin("mochi.webmention.send", nil), sl.Tuple{sl.String(source), targets}, nil)
	if err != nil || value != sl.MakeInt(4) {
		t.Fatalf("THE POINT: queued %v %v, not each page once", value, err)
	}
	webmention_work()
	told := map[string]string{}
	for _, m := range sites.received() {
		if m.Get("source") != source {
			t.Errorf("THE POINT: a mention named %q as its source", m.Get("source"))
		}
		told[m.Get("target")] = m.Get("endpoint")
	}
	for page, endpoint := range map[string]string{"/headed": "/endpoint/one", "/linked": "/endpoint/two", "/anchored": "/anchored"} {
		if told[sites.server.URL+page] != endpoint {
			t.Errorf("THE POINT: %s was not told at %s; told %v", page, endpoint, told)
		}
	}
	if len(told) != 3 {
		t.Errorf("THE POINT: the page with no endpoint was told: %v", told)
	}

	sites.lock.Lock()
	sites.status = http.StatusInternalServerError
	sites.lock.Unlock()
	api_webmention_send(thread, sl.NewBuiltin("mochi.webmention.send", nil), sl.Tuple{sl.String(source), sl.NewList([]sl.Value{sl.String(sites.server.URL + "/headed")})}, nil)
	webmention_work()
	if row, _ := networks_database().row("select attempts from webmentions where direction='sent'"); row == nil || row_int(row, "attempts") != 1 {
		t.Errorf("THE POINT: a mention its endpoint failed was not kept to try again: %v", row)
	}

	if _, err := api_webmention_send(thread, sl.NewBuiltin("mochi.webmention.send", nil), sl.Tuple{sl.String("https://elsewhere.example/page"), targets}, nil); err == nil {
		t.Error("THE POINT: a mention was sent for a page elsewhere")
	}
}

// TestWebmentionEndpoint: an entity's endpoint is under the server's address,
// for its owner's app alone.
func TestWebmentionEndpoint(t *testing.T) {
	alice, a, _ := create_webmention_test_env(t)
	value, err := api_webmention_endpoint(create_test_thread(alice, a), sl.NewBuiltin("mochi.webmention.endpoint", nil), sl.Tuple{sl.String("wmfp00001")}, nil)
	if err != nil || value != sl.String("https://example.com/webmention/wmfeed1") {
		t.Errorf("THE POINT: the endpoint = %v %v", value, err)
	}
	if value, err := api_webmention_endpoint(create_test_thread(&User{UID: "u-bob"}, a), sl.NewBuiltin("mochi.webmention.endpoint", nil), sl.Tuple{sl.String("wmfeed1")}, nil); err != nil || value != sl.String("") {
		t.Errorf("THE POINT: another user was given alice's endpoint: %v %v", value, err)
	}
	if _, err := api_webmention_endpoint(create_test_thread(alice, apps["bare1"]), sl.NewBuiltin("mochi.webmention.endpoint", nil), sl.Tuple{sl.String("wmfeed1")}, nil); err == nil {
		t.Error("THE POINT: an app with nowhere for mentions to go was given an endpoint")
	}
}

// TestOpengraphEntry: a page's entry is an h-entry with its author, its text
// sanitised, and its address and time; the links a page declares go out as
// Link headers too.
func TestOpengraphEntry(t *testing.T) {
	entry := opengraph_entry(map[string]any{"content": `<p>See <a href="https://blog.example/">this</a></p><script>alert(1)</script>`, "author": "Alice & co", "url": "https://example.com/feeds/x/p1", "published": int64(1792497600)})
	for _, want := range []string{`<article class="h-entry">`, `<span class="p-name">Alice &amp; co</span>`, `<a href="https://blog.example/"`, `<a class="u-url" href="https://example.com/feeds/x/p1">`, `datetime="2026-10-20T12:00:00Z"`} {
		if !strings.Contains(entry, want) {
			t.Errorf("THE POINT: the entry lacks %s: %s", want, entry)
		}
	}
	if strings.Contains(entry, "<script") {
		t.Errorf("THE POINT: the entry carries a script: %s", entry)
	}
	if opengraph_entry(map[string]any{"author": "x"}) != "" {
		t.Error("an entry with no text was drawn")
	}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	opengraph_headers(c, []any{map[string]any{"rel": "webmention", "href": "https://example.com/webmention/e1"}, map[string]any{"rel": "me", "href": "javascript:alert(1)"}})
	if got := w.Header().Values("Link"); len(got) != 1 || got[0] != `<https://example.com/webmention/e1>; rel="webmention"` {
		t.Errorf("THE POINT: the Link headers = %v", got)
	}
}

// TestLinkValues: a Link header's links, each with its relations.
func TestLinkValues(t *testing.T) {
	links := link_values(`<https://a.example/>; rel="hub", <https://b.example/x?y=1;z>; rel="self alternate"; type="application/rss+xml"`)
	if len(links) != 2 || links[0].href != "https://a.example/" || !links[0].relation("hub") || links[1].href != "https://b.example/x?y=1;z" || !links[1].relation("self") || !links[1].relation("alternate") {
		t.Errorf("THE POINT: the links = %+v", links)
	}
}

// TestTextLinks: the addresses a text links to, written as links or bare, in
// order and each once, and nothing that is not a web address.
func TestTextLinks(t *testing.T) {
	got := text_links("See [the post](https://blog.example/a) and https://other.example/b, then [again](https://blog.example/a) and [mail](mailto:x@example.com) and [here](/relative).")
	if len(got) != 2 || got[0] != "https://blog.example/a" || got[1] != "https://other.example/b" {
		t.Errorf("THE POINT: the links = %v", got)
	}
}

// TestWebmentionResent: a mention sent again while it is being checked stays
// queued, to be checked afresh, rather than going with the check before it.
func TestWebmentionResent(t *testing.T) {
	_, _, calls := create_webmention_test_env(t)
	target := "https://example.com/feeds/wmfp00001/post1"
	release := make(chan struct{})
	held := make(chan struct{}, 1)
	blog := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		held <- struct{}{}
		<-release
		io.WriteString(w, `<a href="`+target+`">x</a>`)
	}))
	t.Cleanup(blog.Close)
	webmention_mention(blog.URL+"/page", target)
	done := make(chan struct{})
	go func() {
		webmention_work()
		close(done)
	}()
	<-held
	webmention_mention(blog.URL+"/page", target)
	close(release)
	<-done
	if calls.count("webmention/receive") != 1 {
		t.Fatalf("the first check reached the app %d times", calls.count("webmention/receive"))
	}
	if row, _ := networks_database().row("select attempts, next from webmentions where source=?", blog.URL+"/page"); row == nil || row_int(row, "next") > now() {
		t.Errorf("THE POINT: the mention sent again during its check went with it: %v", row)
	}
}

// TestWebmentionDelay: a mention to send waits webmention_delay, so the page
// the app has just changed is in place when the target fetches it.
func TestWebmentionDelay(t *testing.T) {
	alice, a, _ := create_webmention_test_env(t)
	sites := create_webmention_pages(t)
	sites.set("/headed", webmention_page{headers: map[string]string{"Link": `</endpoint>; rel="webmention"`}, body: `<html></html>`})
	webmention_delay = 2 * time.Second
	api_webmention_send(create_test_thread(alice, a), sl.NewBuiltin("mochi.webmention.send", nil), sl.Tuple{sl.String("https://example.com/feeds/wmfp00001/post1"), sl.NewList([]sl.Value{sl.String(sites.server.URL + "/headed")})}, nil)
	webmention_work()
	if len(sites.received()) != 0 {
		t.Fatal("THE POINT: a mention went out before the delay")
	}
	time.Sleep(2100 * time.Millisecond)
	webmention_work()
	if len(sites.received()) != 1 {
		t.Errorf("THE POINT: the mention did not go out after the delay: %d", len(sites.received()))
	}
}

// Mochi server: WebSub tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
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

// A websub_calls records the app functions the engines call, answering each
// with what the test sets.
type websub_calls struct {
	lock    sync.Mutex
	calls   []string
	args    []Map
	answers map[string]any
}

func (w *websub_calls) call(user *User, a *App, function string, args Map) (any, error) {
	w.lock.Lock()
	defer w.lock.Unlock()
	w.calls = append(w.calls, function)
	w.args = append(w.args, args)
	return w.answers[function], nil
}

// count is how many calls of a function were made.
func (w *websub_calls) count(function string) int {
	w.lock.Lock()
	defer w.lock.Unlock()
	n := 0
	for _, c := range w.calls {
		if c == function {
			n++
		}
	}
	return n
}

// last is the arguments of a function's last call.
func (w *websub_calls) last(function string) Map {
	w.lock.Lock()
	defer w.lock.Unlock()
	for i := len(w.calls) - 1; i >= 0; i-- {
		if w.calls[i] == function {
			return w.args[i]
		}
	}
	return nil
}

// create_websub_test_env is alice with an app declaring the engines'
// functions, the calls they make recorded.
func create_websub_test_env(t *testing.T) (*User, *App, *websub_calls) {
	t.Helper()
	alice := create_delivery_test_env(t)
	functions := map[string]AppFunction{}
	for _, name := range []string{"websub/notify", "websub/topic", "websub/content", "webmention/receive"} {
		functions[name] = AppFunction{Function: "function_" + strings.ReplaceAll(name, "/", "_")}
	}
	a := &App{id: "feeds1", internal: &AppVersion{Functions: functions, Classes: []string{"wmfeed"}}}
	bare := &App{id: "bare1", internal: &AppVersion{Functions: map[string]AppFunction{}}}
	apps_lock.Lock()
	apps["feeds1"], apps["bare1"] = a, bare
	apps_lock.Unlock()
	calls := &websub_calls{answers: map[string]any{}}
	saved_websub, saved_webmention := websub_caller, webmention_caller
	saved_quiet, saved_gather, saved_retries, saved_delay := websub_quiet, websub_gather, websub_retries, webmention_delay
	webmention_delay = 0
	websub_caller, webmention_caller = calls.call, calls.call
	websub_quiet, websub_gather, websub_retries = 300*time.Millisecond, 50*time.Millisecond, []time.Duration{10 * time.Millisecond}
	rate_limit_websub = &rate_limiter{entries: make(map[string]*rate_limit_entry), limit: 1000, window: 60}
	rate_limit_webmention = &rate_limiter{entries: make(map[string]*rate_limit_entry), limit: 1000, window: 60}
	t.Cleanup(func() {
		websub_caller, webmention_caller = saved_websub, saved_webmention
		websub_quiet, websub_gather, websub_retries, webmention_delay = saved_quiet, saved_gather, saved_retries, saved_delay
		apps_lock.Lock()
		delete(apps, "feeds1")
		delete(apps, "bare1")
		apps_lock.Unlock()
	})
	return alice, a, calls
}

// websub_request sends one request to the engines, answering the recorder.
func websub_request(method string, target string, form url.Values, headers map[string]string, body string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	if form != nil {
		body = form.Encode()
	}
	c.Request = httptest.NewRequest(method, target, strings.NewReader(body))
	if form != nil {
		c.Request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	for k, v := range headers {
		c.Request.Header.Set(k, v)
	}
	c.Request.Host = "example.com"
	c.Request.RemoteAddr = "192.0.2.1:1234"
	if strings.HasPrefix(c.Request.URL.Path, "/webmention/") {
		webmention_route(c)
	} else {
		websub_route(c)
	}
	return w
}

// websub_eventually waits up to five seconds for a condition.
func websub_eventually(t *testing.T, what string, done func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if done() {
			return
		}
	}
	t.Fatalf("THE POINT: %s", what)
}

// A websub_hub is another server's hub, recording the requests it is sent.
type websub_hub struct {
	server *httptest.Server
	lock   sync.Mutex
	forms  []url.Values
	status int
}

func create_websub_hub(t *testing.T) *websub_hub {
	h := &websub_hub{status: http.StatusAccepted}
	h.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		h.lock.Lock()
		h.forms = append(h.forms, r.PostForm)
		status := h.status
		h.lock.Unlock()
		w.WriteHeader(status)
	}))
	t.Cleanup(h.server.Close)
	return h
}

func (h *websub_hub) form(n int) url.Values {
	h.lock.Lock()
	defer h.lock.Unlock()
	if n >= len(h.forms) {
		return nil
	}
	return h.forms[n]
}

// websub_signature signs a body as a hub does.
func websub_signature(secret string, body string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(body))
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

// TestWebsubSubscriber: a source subscribes at the hub its feed names with a
// callback of its own and a secret; the hub's check of intent is answered
// only for what was asked; a push signed with the secret prompts one poll,
// and pushes inside the quiet time join the next; an unsigned or wrongly
// signed push prompts nothing; a push to a callback that is gone answers
// gone; unsubscribing tells the hub and ends once it checks; a hub's refusal
// is kept and not asked again.
func TestWebsubSubscriber(t *testing.T) {
	alice, a, calls := create_websub_test_env(t)
	thread := create_test_thread(alice, a)
	hub := create_websub_hub(t)
	topic := "https://blog.example/feed.xml"
	subscribe := func(source string) sl.Value {
		value, err := api_websub_subscribe(thread, sl.NewBuiltin("mochi.websub.subscribe", nil), sl.Tuple{sl.String(source), sl.String(topic), sl.String(hub.server.URL)}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	if subscribe("s1") != sl.True {
		t.Fatal("THE POINT: the subscription was not asked for")
	}
	websub_eventually(t, "the hub was not asked to subscribe", func() bool { return hub.form(0) != nil })
	asked := hub.form(0)
	callback := asked.Get("hub.callback")
	if asked.Get("hub.mode") != "subscribe" || asked.Get("hub.topic") != topic || !strings.HasPrefix(callback, "https://example.com/websub/") || len(asked.Get("hub.secret")) < 32 || asked.Get("hub.lease_seconds") != "864000" {
		t.Fatalf("THE POINT: the hub was asked %v", asked)
	}
	path := strings.TrimPrefix(callback, "https://example.com")
	secret := asked.Get("hub.secret")
	subscribe("s1")
	time.Sleep(100 * time.Millisecond)
	if hub.form(1) != nil {
		t.Error("THE POINT: subscribing again to the same topic asked the hub again")
	}

	if w := websub_request("GET", path+"?hub.mode=subscribe&hub.topic="+url.QueryEscape("https://other.example/feed")+"&hub.challenge=c1", nil, nil, ""); w.Code != http.StatusNotFound {
		t.Errorf("THE POINT: a check naming another topic = %d", w.Code)
	}
	if w := websub_request("GET", path+"?hub.mode=unsubscribe&hub.topic="+url.QueryEscape(topic)+"&hub.challenge=c1", nil, nil, ""); w.Code != http.StatusNotFound {
		t.Errorf("THE POINT: a check of an unsubscription never asked for = %d", w.Code)
	}
	w := websub_request("GET", path+"?hub.mode=subscribe&hub.topic="+url.QueryEscape(topic)+"&hub.challenge=c2&hub.lease_seconds=3600", nil, nil, "")
	if w.Code != http.StatusOK || w.Body.String() != "c2" {
		t.Fatalf("THE POINT: the check of intent = %d %q", w.Code, w.Body.String())
	}
	held, _ := api_websub_get(thread, sl.NewBuiltin("mochi.websub.get", nil), sl.Tuple{sl.String("s1")}, nil)
	got := sl_decode_map(held)
	if got["state"] != "active" || got["hub"] != hub.server.URL || got["topic"] != topic {
		t.Errorf("THE POINT: the subscription = %v", got)
	}
	if expires, _ := got["expires"].(int64); expires < now()+3500 || expires > now()+3700 {
		t.Errorf("THE POINT: the lease ends at %v, not an hour on", got["expires"])
	}

	body := "<rss/>"
	if w := websub_request("POST", path, nil, nil, body); w.Code != http.StatusAccepted {
		t.Errorf("unsigned push = %d", w.Code)
	}
	if w := websub_request("POST", path, nil, map[string]string{"X-Hub-Signature": websub_signature("wrong", body)}, body); w.Code != http.StatusAccepted {
		t.Errorf("wrongly signed push = %d", w.Code)
	}
	time.Sleep(50 * time.Millisecond)
	if calls.count("websub/notify") != 0 {
		t.Fatal("THE POINT: a push without the secret's signature prompted a poll")
	}
	signed := func() {
		websub_request("POST", path, nil, map[string]string{"X-Hub-Signature": websub_signature(secret, body)}, body)
	}
	signed()
	websub_eventually(t, "a signed push prompted no poll", func() bool { return calls.count("websub/notify") == 1 })
	if calls.last("websub/notify")["source"] != "s1" {
		t.Errorf("THE POINT: the poll was for %v", calls.last("websub/notify"))
	}
	signed()
	time.Sleep(100 * time.Millisecond)
	if calls.count("websub/notify") != 1 {
		t.Error("THE POINT: a push inside the quiet time prompted a poll at once")
	}
	websub_eventually(t, "a push inside the quiet time was not taken once it ended", func() bool { return calls.count("websub/notify") == 2 })
	time.Sleep(400 * time.Millisecond)
	for i := 0; i < 3; i++ {
		signed()
	}
	time.Sleep(450 * time.Millisecond)
	if n := calls.count("websub/notify") - 2; n < 1 || n > 2 {
		t.Errorf("THE POINT: three pushes in a row prompted %d polls", n)
	}
	if w := websub_request("POST", "/websub/nobody", nil, nil, body); w.Code != http.StatusGone {
		t.Errorf("THE POINT: a push to a callback that is gone = %d", w.Code)
	}

	api_websub_unsubscribe(thread, sl.NewBuiltin("mochi.websub.unsubscribe", nil), sl.Tuple{sl.String("s1")}, nil)
	websub_eventually(t, "the hub was not asked to unsubscribe", func() bool { return hub.form(1) != nil })
	if f := hub.form(1); f.Get("hub.mode") != "unsubscribe" || f.Get("hub.callback") != callback {
		t.Errorf("THE POINT: the hub was asked %v", f)
	}
	if w := websub_request("GET", path+"?hub.mode=unsubscribe&hub.topic="+url.QueryEscape(topic)+"&hub.challenge=c3", nil, nil, ""); w.Code != http.StatusOK || w.Body.String() != "c3" {
		t.Errorf("THE POINT: the check of the unsubscription = %d %q", w.Code, w.Body.String())
	}
	if gone, _ := networks_database().exists("select 1 from websub where source='s1'"); gone {
		t.Error("THE POINT: the subscription outlived its unsubscription")
	}

	subscribe("s2")
	websub_eventually(t, "the second source was not subscribed", func() bool { return hub.form(2) != nil })
	refused := strings.TrimPrefix(hub.form(2).Get("hub.callback"), "https://example.com")
	websub_request("GET", refused+"?hub.mode=denied&hub.topic="+url.QueryEscape(topic), nil, nil, "")
	if subscribe("s2") != sl.False || hub.form(3) != nil {
		t.Error("THE POINT: a subscription the hub refused was asked for again")
	}

	hub.lock.Lock()
	hub.status = http.StatusBadRequest
	hub.lock.Unlock()
	subscribe("s3")
	websub_eventually(t, "a subscription the hub refuses at once is not failed", func() bool {
		row, _ := networks_database().row("select state from websub where source='s3'")
		return row != nil && row_string(row, "state") == "failed"
	})

	if _, err := api_websub_subscribe(create_test_thread(alice, apps["bare1"]), sl.NewBuiltin("mochi.websub.subscribe", nil), sl.Tuple{sl.String("s9"), sl.String(topic), sl.String(hub.server.URL)}, nil); err == nil {
		t.Error("THE POINT: an app with nowhere for pushes to go subscribed")
	}
	hub.lock.Lock()
	hub.status = http.StatusAccepted
	hub.lock.Unlock()
	subscribe("s5")
	websub_eventually(t, "the fifth source was not subscribed", func() bool {
		hub.lock.Lock()
		defer hub.lock.Unlock()
		return len(hub.forms) >= 5
	})
	fresh := strings.TrimPrefix(hub.form(4).Get("hub.callback"), "https://example.com")
	networks_development = false
	if w := websub_request("GET", fresh+"?hub.mode=subscribe&hub.topic="+url.QueryEscape(topic)+"&hub.challenge=c9", nil, nil, ""); w.Code != http.StatusNotFound {
		t.Errorf("THE POINT: the callback answered with the networks off: %d", w.Code)
	}
	if subscribe("s4") != sl.False {
		t.Error("THE POINT: a source subscribed with the networks off")
	}
	networks_development = true
}

// A websub_reader is a feed reader's server subscribing at the hub here: it
// echoes the hub's challenges when told to and records what it is sent.
type websub_reader struct {
	server  *httptest.Server
	lock    sync.Mutex
	echo    bool
	status  int
	checks  []url.Values
	bodies  []string
	headers []http.Header
}

func create_websub_reader(t *testing.T) *websub_reader {
	r := &websub_reader{echo: true, status: http.StatusOK}
	r.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		r.lock.Lock()
		defer r.lock.Unlock()
		if request.Method == http.MethodGet {
			r.checks = append(r.checks, request.URL.Query())
			if r.echo {
				io.WriteString(w, request.URL.Query().Get("hub.challenge"))
			}
			return
		}
		body, _ := io.ReadAll(request.Body)
		r.bodies = append(r.bodies, string(body))
		r.headers = append(r.headers, request.Header.Clone())
		w.WriteHeader(r.status)
	}))
	t.Cleanup(r.server.Close)
	return r
}

func (r *websub_reader) received() int {
	r.lock.Lock()
	defer r.lock.Unlock()
	return len(r.bodies)
}

// TestWebsubHub: an app names a feed of its own as a topic and is given the
// hub's address; a subscription to it is taken once the app says the topic is
// still published, and kept once the subscriber echoes the challenge; a
// subscriber that does not echo it is not kept; a change goes out once, as the
// document the app gives, signed with the subscriber's secret and naming the
// hub and the topic; a subscriber that answers gone is dropped; a topic not on
// this server, or another's, is refused.
func TestWebsubHub(t *testing.T) {
	alice, a, calls := create_websub_test_env(t)
	thread := create_test_thread(alice, a)
	topic := "https://example.com/feeds/abc/rss?token=t1"
	value, err := api_websub_hub(thread, sl.NewBuiltin("mochi.websub.hub", nil), sl.Tuple{sl.String(topic), sl.String("t1")}, nil)
	if err != nil || value != sl.String("https://example.com/websub") {
		t.Fatalf("THE POINT: the hub's address = %v %v", value, err)
	}
	if _, err := api_websub_hub(thread, sl.NewBuiltin("mochi.websub.hub", nil), sl.Tuple{sl.String("https://elsewhere.example/feed"), sl.String("t2")}, nil); err == nil {
		t.Error("THE POINT: a topic on another server was taken")
	}
	bob := &User{UID: "u-bob"}
	if _, err := api_websub_hub(create_test_thread(bob, a), sl.NewBuiltin("mochi.websub.hub", nil), sl.Tuple{sl.String(topic), sl.String("t1")}, nil); err == nil {
		t.Error("THE POINT: another user took alice's topic")
	}

	reader := create_websub_reader(t)
	callback := reader.server.URL + "/cb?feed=1"
	ask := func(mode string, topic string, callback string) int {
		return websub_request("POST", "/websub", url.Values{"hub.mode": {mode}, "hub.topic": {topic}, "hub.callback": {callback}, "hub.secret": {"s3cret"}, "hub.lease_seconds": {"200000"}}, nil, "").Code
	}
	if code := ask("subscribe", "https://example.com/feeds/abc/rss?token=none", callback); code != http.StatusNotFound {
		t.Errorf("THE POINT: a subscription to a topic not published here = %d", code)
	}
	if code := ask("subscribe", topic, "ftp://reader.example/cb"); code != http.StatusBadRequest {
		t.Errorf("THE POINT: a subscription with no web callback = %d", code)
	}
	if code := ask("subscribe", topic, callback); code != http.StatusNotFound {
		t.Errorf("THE POINT: a subscription to a topic the app no longer publishes = %d", code)
	}
	calls.answers["websub/topic"] = true
	if code := ask("subscribe", topic, callback); code != http.StatusAccepted {
		t.Fatalf("THE POINT: the subscription = %d", code)
	}
	if args := calls.last("websub/topic"); args["key"] != "t1" || args["topic"] != topic {
		t.Errorf("THE POINT: the app was asked about %v", args)
	}
	websub_eventually(t, "the subscription was not kept once the subscriber confirmed it", func() bool {
		held, _ := networks_database().exists("select 1 from subscribers where topic=? and callback=?", topic, callback)
		return held
	})
	reader.lock.Lock()
	check := reader.checks[0]
	reader.lock.Unlock()
	if check.Get("hub.mode") != "subscribe" || check.Get("hub.topic") != topic || check.Get("hub.lease_seconds") != "200000" || check.Get("feed") != "1" {
		t.Errorf("THE POINT: the check of intent = %v", check)
	}

	reader.lock.Lock()
	reader.echo = false
	reader.lock.Unlock()
	silent := reader.server.URL + "/silent"
	ask("subscribe", topic, silent)
	websub_eventually(t, "the silent subscriber was not checked", func() bool {
		reader.lock.Lock()
		defer reader.lock.Unlock()
		return len(reader.checks) == 2
	})
	time.Sleep(50 * time.Millisecond)
	if held, _ := networks_database().exists("select 1 from subscribers where callback=?", silent); held {
		t.Error("THE POINT: a subscriber that did not echo the challenge was kept")
	}

	calls.answers["websub/content"] = map[string]any{"content": "<rss>one</rss>", "type": "application/rss+xml"}
	for i := 0; i < 3; i++ {
		api_websub_publish(thread, sl.NewBuiltin("mochi.websub.publish", nil), sl.Tuple{sl.String("t1")}, nil)
	}
	websub_eventually(t, "the change was not sent", func() bool { return reader.received() >= 1 })
	time.Sleep(150 * time.Millisecond)
	if reader.received() != 1 || calls.count("websub/content") != 1 {
		t.Errorf("THE POINT: three changes in a row went out %d times", reader.received())
	}
	reader.lock.Lock()
	body, headers := reader.bodies[0], reader.headers[0]
	reader.lock.Unlock()
	if body != "<rss>one</rss>" || headers.Get("Content-Type") != "application/rss+xml" || headers.Get("X-Hub-Signature") != websub_signature("s3cret", body) {
		t.Errorf("THE POINT: the delivery = %q %v", body, headers)
	}
	if link := headers.Get("Link"); !strings.Contains(link, "<https://example.com/websub>; rel=\"hub\"") || !strings.Contains(link, "<"+topic+">; rel=\"self\"") {
		t.Errorf("THE POINT: the delivery's links = %q", link)
	}

	reader.lock.Lock()
	reader.status = http.StatusGone
	reader.lock.Unlock()
	api_websub_publish(thread, sl.NewBuiltin("mochi.websub.publish", nil), sl.Tuple{sl.String("t1")}, nil)
	websub_eventually(t, "a subscriber that answered gone was kept", func() bool {
		held, _ := networks_database().exists("select 1 from subscribers where callback=?", callback)
		return !held
	})
}

// TestRssHub: a feed names its hub and itself in its Link headers or its
// channel's link elements, resolved against the feed; an item's links are
// not the channel's.
func TestRssHub(t *testing.T) {
	page, _ := url.Parse("https://blog.example/feeds/main.xml")
	headers := http.Header{}
	headers.Add("Link", `<https://hub.example/>; rel="hub", </feeds/main.xml>; rel="self"`)
	if hub, self := rss_hub(page, headers, nil); hub != "https://hub.example/" || self != "https://blog.example/feeds/main.xml" {
		t.Errorf("THE POINT: from the headers, hub %q self %q", hub, self)
	}
	body := []byte(`<rss xmlns:atom="http://www.w3.org/2005/Atom"><channel><link>https://blog.example/</link><atom:link rel='hub' href='/hub'/><atom:link href="https://blog.example/feed" rel="self" type="application/rss+xml"/><item><atom:link rel="hub" href="https://wrong.example/"/></item></channel></rss>`)
	if hub, self := rss_hub(page, http.Header{}, body); hub != "https://blog.example/hub" || self != "https://blog.example/feed" {
		t.Errorf("THE POINT: from the channel, hub %q self %q", hub, self)
	}
	atom := []byte(`<feed xmlns="http://www.w3.org/2005/Atom"><link rel="self" href="https://blog.example/atom"/><entry><link rel="hub" href="https://wrong.example/"/></entry></feed>`)
	if hub, self := rss_hub(page, http.Header{}, atom); hub != "" || self != "https://blog.example/atom" {
		t.Errorf("THE POINT: an entry's link was taken for the feed's: hub %q self %q", hub, self)
	}
}

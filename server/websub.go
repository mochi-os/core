// Mochi server: WebSub
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// WebSub (a W3C Recommendation) pushes a web feed's changes to the readers
// subscribed to it, through a hub the feed names. Core speaks both sides, for
// the apps.
//
// As a subscriber, core subscribes at the hub a source's feed names, with a
// callback under /websub/ that only that subscription knows, answers the
// hub's check of intent there, and on each push signed with the
// subscription's secret asks the app to poll the source now. The push's own
// body is not read: the app's poll fetches the feed as it always does, so
// nothing about a source's items changes, and its timer stays as a safety
// net.
//
// As a hub, core takes subscriptions at /websub to the feeds an app has
// named as topics, checks each subscriber's intent at its callback, and on
// each change the app announces sends the feed's current document to every
// subscriber, signed with the subscriber's secret where it gave one.

package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

// websub_lease is the life, in seconds, core asks for and grants a
// subscription; websub_lease_minimum the shortest it grants.
const websub_lease = 10 * 24 * 3600
const websub_lease_minimum = 24 * 3600

// websub_quiet is the least time between two polls a hub's pushes prompt for
// one subscription; a push inside it is folded into the next.
var websub_quiet = time.Minute

// websub_gather is how long a hub waits after a change before sending, so a
// run of changes goes out as one document.
var websub_gather = 10 * time.Second

// websub_retries are the waits before a delivery is tried again.
var websub_retries = []time.Duration{30 * time.Second, 5 * time.Minute}

// websub_push_maximum is the most of a push read to check its signature.
const websub_push_maximum = 10 << 20

// websub_url_maximum is the longest topic, hub or callback taken.
const websub_url_maximum = 2000

// rate_limit_websub bounds the subscription requests one address makes.
var rate_limit_websub = &rate_limiter{entries: make(map[string]*rate_limit_entry), limit: 30, window: 60}

// websub_caller calls an app's function as a user; tests replace it.
var websub_caller = app_call_as_server

// websub_tables makes the engine's tables in networks.db: this server's
// subscriptions at other hubs, by the callback's private id; the topics the
// apps publish here; and those subscribed to them.
func websub_tables(db *DB) {
	db.exec("create table if not exists websub (id text not null primary key, user text not null, app text not null, source text not null, topic text not null, hub text not null, secret text not null, state text not null, expires integer not null default 0, attempts integer not null default 0, asked integer not null default 0, created integer not null, unique (user, app, source))")
	db.exec("create table if not exists topics (topic text not null primary key, user text not null, app text not null, key text not null, created integer not null)")
	db.exec("create index if not exists topics_key on topics(user, app, key)")
	db.exec("create table if not exists subscribers (topic text not null, callback text not null, secret text not null default '', expires integer not null, created integer not null, primary key (topic, callback))")
}

// websub_engine is the engine answering hubs and subscribers.
func websub_engine() *engine {
	return &engine{name: "websub", prefixes: []string{"/websub"}, handler: websub_route}
}

// websub_route answers /websub, the hub, and /websub/<id>, a subscription's
// callback. Like every engine of the federation work it answers nothing while
// the development switch is off.
func websub_route(c *gin.Context) bool {
	if !networks_development {
		respond_error(c, http.StatusNotFound, "not_found", "errors.not_found", nil)
		return true
	}
	rest := strings.TrimPrefix(c.Request.URL.Path, "/websub")
	switch {
	case rest == "" || rest == "/":
		if c.Request.Method != http.MethodPost {
			c.AbortWithStatus(http.StatusMethodNotAllowed)
			return true
		}
		websub_intake(c)
	case strings.HasPrefix(rest, "/") && !strings.Contains(rest[1:], "/"):
		switch c.Request.Method {
		case http.MethodGet:
			websub_verify(c, rest[1:])
		case http.MethodPost:
			websub_push(c, rest[1:])
		default:
			c.AbortWithStatus(http.StatusMethodNotAllowed)
		}
	default:
		c.AbortWithStatus(http.StatusNotFound)
	}
	return true
}

// websub_web reports whether a value is an absolute http or https address of
// a length core takes.
func websub_web(value string) bool {
	if value == "" || len(value) > websub_url_maximum {
		return false
	}
	u, err := url.Parse(value)
	return err == nil && (u.Scheme == "https" || u.Scheme == "http") && u.Host != ""
}

// websub_declares reports whether an app's active version declares every
// function named.
func websub_declares(user *User, a *App, functions ...string) bool {
	if a == nil {
		return false
	}
	av := a.active(user)
	if av == nil {
		return false
	}
	for _, f := range functions {
		if _, found := av.Functions[f]; !found {
			return false
		}
	}
	return true
}

// websub_send makes one request on the protocol's behalf, through the host
// policy, and answers its status and the start of its body.
func websub_send(user *User, method string, target string, kind string, body []byte, headers map[string]string) (int, string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var reader io.Reader
	if body != nil {
		reader = strings.NewReader(string(body))
	}
	r, err := http.NewRequestWithContext(ctx, method, target, reader)
	if err != nil {
		return 0, "", err
	}
	if kind != "" {
		r.Header.Set("Content-Type", kind)
	}
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	response, err := network_request(ctx, user, "", r, 3)
	if err != nil {
		return 0, "", err
	}
	defer response.Body.Close()
	answer, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
	return response.StatusCode, string(answer), nil
}

// === Subscribing at other hubs ===

// websub_ask asks a subscription's hub to subscribe or to unsubscribe; the
// hub then checks with the callback.
func websub_ask(row map[string]any, mode string) error {
	user := user_by_uid(row_string(row, "user"))
	base := server_public_url()
	if user == nil || base == "" {
		return fmt.Errorf("no callback")
	}
	form := url.Values{}
	form.Set("hub.mode", mode)
	form.Set("hub.topic", row_string(row, "topic"))
	form.Set("hub.callback", base+"/websub/"+row_string(row, "id"))
	if mode == "subscribe" {
		form.Set("hub.secret", row_string(row, "secret"))
		form.Set("hub.lease_seconds", strconv.Itoa(websub_lease))
	}
	status, answer, err := websub_send(user, http.MethodPost, row_string(row, "hub"), "application/x-www-form-urlencoded", []byte(form.Encode()), nil)
	if err != nil {
		return err
	}
	if status < 200 || status >= 300 {
		return fmt.Errorf("the hub answered %d: %s", status, strings.TrimSpace(answer))
	}
	return nil
}

// websub_subscribe asks a pending subscription's hub, counting the attempt;
// one the hub refuses is failed.
func websub_subscribe(id string) {
	db := networks_database()
	row, _ := db.row("select * from websub where id=?", id)
	if row == nil {
		return
	}
	db.exec("update websub set attempts=attempts+1, asked=? where id=?", now(), id)
	if err := websub_ask(row, "subscribe"); err != nil {
		info("WebSub subscription to %s at %s failed: %v", row_string(row, "topic"), row_string(row, "hub"), err)
		db.exec("update websub set state='failed' where id=? and state='pending'", id)
	}
}

// websub_verify answers a hub's check of intent at a subscription's callback:
// the challenge echoed where the hub confirms what this server asked for, and
// not found otherwise. A hub's refusal is kept, so the source is polled.
func websub_verify(c *gin.Context, id string) {
	mode, topic, challenge := c.Query("hub.mode"), c.Query("hub.topic"), c.Query("hub.challenge")
	db := networks_database()
	row, _ := db.row("select * from websub where id=?", id)
	if row == nil || row_string(row, "topic") != topic {
		c.AbortWithStatus(http.StatusNotFound)
		return
	}
	state := row_string(row, "state")
	switch {
	case mode == "subscribe" && challenge != "" && (state == "pending" || state == "active"):
		lease, _ := strconv.ParseInt(c.Query("hub.lease_seconds"), 10, 64)
		if lease <= 0 {
			lease = websub_lease
		}
		db.exec("update websub set state='active', expires=?, attempts=0 where id=?", now()+lease, id)
		c.Data(http.StatusOK, "text/plain; charset=utf-8", []byte(challenge))
	case mode == "unsubscribe" && challenge != "" && state == "leaving":
		db.exec("delete from websub where id=?", id)
		c.Data(http.StatusOK, "text/plain; charset=utf-8", []byte(challenge))
	case mode == "denied":
		db.exec("update websub set state='denied' where id=?", id)
		c.AbortWithStatus(http.StatusOK)
	default:
		c.AbortWithStatus(http.StatusNotFound)
	}
}

// websub_signed reports whether a push carries the signature its body has
// under the subscription's secret, by any of the methods the protocol names.
func websub_signed(header string, secret string, body []byte) bool {
	method, signature, found := strings.Cut(strings.TrimSpace(header), "=")
	if !found || secret == "" {
		return false
	}
	var digest func() hash.Hash
	switch strings.ToLower(method) {
	case "sha1":
		digest = sha1.New
	case "sha256":
		digest = sha256.New
	case "sha384":
		digest = sha512.New384
	case "sha512":
		digest = sha512.New
	default:
		return false
	}
	mac := hmac.New(digest, []byte(secret))
	mac.Write(body)
	given, err := hex.DecodeString(signature)
	return err == nil && hmac.Equal(given, mac.Sum(nil))
}

// websub_push takes a hub's push to a subscription's callback. A push to a
// subscription that is gone answers gone, which tells the hub to stop; one
// that is not active, or not signed with its secret, is acknowledged and
// ignored, as the protocol asks; a signed one prompts a poll.
func websub_push(c *gin.Context, id string) {
	body, err := io.ReadAll(io.LimitReader(c.Request.Body, websub_push_maximum))
	if err != nil {
		c.AbortWithStatus(http.StatusBadRequest)
		return
	}
	row, _ := networks_database().row("select * from websub where id=?", id)
	if row == nil {
		c.AbortWithStatus(http.StatusGone)
		return
	}
	c.AbortWithStatus(http.StatusAccepted)
	if row_string(row, "state") != "active" || !websub_signed(c.GetHeader("X-Hub-Signature"), row_string(row, "secret"), body) {
		return
	}
	websub_prompt(id, row_string(row, "user"), row_string(row, "app"), row_string(row, "source"))
}

// A websub_prompting is one subscription's prompting: when the app was last
// told to poll, and whether a prompt waits.
type websub_prompting struct {
	last    time.Time
	waiting bool
}

var websub_prompts = map[string]*websub_prompting{}
var websub_prompts_lock sync.Mutex

// websub_prompt tells the app to poll a source, at most once in
// websub_quiet: a push inside it waits for its end, and pushes meanwhile join
// it.
func websub_prompt(id string, uid string, app string, source string) {
	websub_prompts_lock.Lock()
	p := websub_prompts[id]
	if p == nil {
		p = &websub_prompting{}
		websub_prompts[id] = p
	}
	if p.waiting {
		websub_prompts_lock.Unlock()
		return
	}
	p.waiting = true
	wait := time.Until(p.last.Add(websub_quiet))
	websub_prompts_lock.Unlock()
	if wait < 0 {
		wait = 0
	}
	time.AfterFunc(wait, func() {
		websub_prompts_lock.Lock()
		p.waiting = false
		p.last = time.Now()
		websub_prompts_lock.Unlock()
		user, a := user_by_uid(uid), app_by_id(app)
		if user == nil || a == nil {
			return
		}
		if _, err := websub_caller(user, a, "websub/notify", Map{"source": source}); err != nil {
			info("WebSub push for source %s not taken: %v", source, err)
		}
	})
}

// === Hub ===

// websub_intake takes a request to subscribe to, or unsubscribe from, a topic
// published here: refused at once where the topic is none the app still
// publishes, accepted otherwise and checked with the subscriber afterwards.
func websub_intake(c *gin.Context) {
	if !rate_limit_websub.allow(c.ClientIP()) {
		c.AbortWithStatus(http.StatusTooManyRequests)
		return
	}
	mode, topic, callback, secret := c.PostForm("hub.mode"), c.PostForm("hub.topic"), c.PostForm("hub.callback"), c.PostForm("hub.secret")
	if (mode != "subscribe" && mode != "unsubscribe") || !websub_web(callback) || len(secret) > 200 {
		c.AbortWithStatus(http.StatusBadRequest)
		return
	}
	lease, _ := strconv.ParseInt(c.PostForm("hub.lease_seconds"), 10, 64)
	if lease <= 0 || lease > websub_lease {
		lease = websub_lease
	}
	if lease < websub_lease_minimum {
		lease = websub_lease_minimum
	}
	row, _ := networks_database().row("select * from topics where topic=?", topic)
	if row == nil {
		c.AbortWithStatus(http.StatusNotFound)
		return
	}
	user, a := user_by_uid(row_string(row, "user")), app_by_id(row_string(row, "app"))
	if user == nil || a == nil {
		c.AbortWithStatus(http.StatusNotFound)
		return
	}
	if mode == "subscribe" {
		published, err := websub_caller(user, a, "websub/topic", Map{"key": row_string(row, "key"), "topic": topic})
		if err != nil || published != true {
			c.AbortWithStatus(http.StatusNotFound)
			return
		}
	}
	c.AbortWithStatus(http.StatusAccepted)
	go websub_intent(user, mode, topic, callback, secret, lease)
}

// websub_intent checks with a subscriber that it asked for what the hub was
// asked, by the challenge it must echo, and only then keeps or ends its
// subscription.
func websub_intent(user *User, mode string, topic string, callback string, secret string, lease int64) {
	challenge := random_alphanumeric(32)
	u, err := url.Parse(callback)
	if err != nil {
		return
	}
	query := u.Query()
	query.Set("hub.mode", mode)
	query.Set("hub.topic", topic)
	query.Set("hub.challenge", challenge)
	if mode == "subscribe" {
		query.Set("hub.lease_seconds", strconv.FormatInt(lease, 10))
	}
	u.RawQuery = query.Encode()
	status, answer, err := websub_send(user, http.MethodGet, u.String(), "", nil, nil)
	if err != nil || status < 200 || status >= 300 || answer != challenge {
		debug("WebSub subscriber %s did not confirm %s of %s", callback, mode, topic)
		return
	}
	db := networks_database()
	if mode == "unsubscribe" {
		db.exec("delete from subscribers where topic=? and callback=?", topic, callback)
		return
	}
	db.exec("insert into subscribers (topic, callback, secret, expires, created) values (?, ?, ?, ?, ?) on conflict(topic, callback) do update set secret=excluded.secret, expires=excluded.expires", topic, callback, secret, now()+lease, now())
}

var websub_waiting = map[string]bool{}
var websub_waiting_lock sync.Mutex

// websub_announce sends a topic's current document to its subscribers once
// websub_gather has passed, so a run of changes goes out once.
func websub_announce(topic string) {
	websub_waiting_lock.Lock()
	if websub_waiting[topic] {
		websub_waiting_lock.Unlock()
		return
	}
	websub_waiting[topic] = true
	websub_waiting_lock.Unlock()
	time.AfterFunc(websub_gather, func() {
		websub_waiting_lock.Lock()
		delete(websub_waiting, topic)
		websub_waiting_lock.Unlock()
		websub_distribute(topic)
	})
}

// websub_distribute asks the app for a topic's document and sends it to each
// subscriber whose lease runs.
func websub_distribute(topic string) {
	db := networks_database()
	row, _ := db.row("select * from topics where topic=?", topic)
	if row == nil {
		return
	}
	subscribers, _ := db.rows("select callback, secret from subscribers where topic=? and expires>?", topic, now())
	if len(subscribers) == 0 {
		return
	}
	user, a := user_by_uid(row_string(row, "user")), app_by_id(row_string(row, "app"))
	if user == nil || a == nil {
		return
	}
	answer, err := websub_caller(user, a, "websub/content", Map{"key": row_string(row, "key"), "topic": topic})
	document, _ := answer.(map[string]any)
	if err != nil || document == nil {
		return
	}
	content, _ := document["content"].(string)
	kind, _ := document["type"].(string)
	if content == "" {
		return
	}
	if kind == "" {
		kind = "application/rss+xml"
	}
	links := "<" + server_public_url() + "/websub>; rel=\"hub\", <" + topic + ">; rel=\"self\""
	for _, s := range subscribers {
		go websub_deliver(user, topic, row_string(s, "callback"), row_string(s, "secret"), []byte(content), kind, links)
	}
}

// websub_deliver sends a topic's document to one subscriber, signed with its
// secret, trying again after each of websub_retries. A subscriber that
// answers gone is dropped, and one that refuses it otherwise is not asked
// again.
func websub_deliver(user *User, topic string, callback string, secret string, body []byte, kind string, links string) {
	headers := map[string]string{"Link": links}
	if secret != "" {
		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write(body)
		headers["X-Hub-Signature"] = "sha256=" + hex.EncodeToString(mac.Sum(nil))
	}
	for attempt := 0; ; attempt++ {
		status, _, err := websub_send(user, http.MethodPost, callback, kind, body, headers)
		switch {
		case err == nil && status >= 200 && status < 300:
			return
		case status == http.StatusGone:
			networks_database().exec("delete from subscribers where topic=? and callback=?", topic, callback)
			return
		case err == nil && status >= 400 && status < 500 && status != http.StatusTooManyRequests:
			return
		}
		if attempt >= len(websub_retries) {
			debug("WebSub delivery of %s to %s abandoned: %d %v", topic, callback, status, err)
			return
		}
		time.Sleep(websub_retries[attempt])
	}
}

// === Upkeep ===

// websub_manager renews the subscriptions here before their leases end, asks
// again of hubs that never checked, and lets go of what has lapsed.
func websub_manager() {
	for range time.Tick(time.Hour) {
		websub_tend()
	}
}

// websub_tend is one round of websub_manager.
func websub_tend() {
	if !networks_development {
		return
	}
	db := networks_database()
	t := now()
	db.exec("delete from subscribers where expires<?", t)
	db.exec("delete from websub where state='leaving' and asked<?", t-24*3600)
	db.exec("update websub set state='failed' where state='pending' and attempts>=5")
	renewing, _ := db.rows("select id from websub where (state='active' and expires<?) or (state='pending' and asked<?)", t+24*3600, t-3600)
	for _, row := range renewing {
		db.exec("update websub set state='pending' where id=?", row_string(row, "id"))
		websub_subscribe(row_string(row, "id"))
	}
}

// === Starlark API ===

var api_websub = sls.FromStringDict(sl.String("mochi.websub"), sl.StringDict{
	"get":         sl.NewBuiltin("mochi.websub.get", api_websub_get),
	"hub":         sl.NewBuiltin("mochi.websub.hub", api_websub_hub),
	"publish":     sl.NewBuiltin("mochi.websub.publish", api_websub_publish),
	"subscribe":   sl.NewBuiltin("mochi.websub.subscribe", api_websub_subscribe),
	"unsubscribe": sl.NewBuiltin("mochi.websub.unsubscribe", api_websub_unsubscribe),
})

// websub_calling is the user and app a call is made for.
func websub_calling(t *sl.Thread) (*User, *App) {
	user, _ := principal_storage(t)
	a, _ := t.Local("app").(*App)
	return user, a
}

// mochi.websub.subscribe(source, topic, hub) -> bool: Subscribe a source of
// the app's to a web feed through the hub the feed names, so the hub's pushes
// call the app's websub/notify function with the source; answers whether the
// subscription is asked for or held. A source already subscribed to the same
// topic at the same hub stays as it is; one that moves to another lets the
// old subscription go. False where the server has no public address for the
// hub to reach, or a hub refused the same subscription within the day
func api_websub_subscribe(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var source, topic, hub string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "source", &source, "topic", &topic, "hub", &hub); err != nil {
		return sl_error(fn, "%v", err)
	}
	user, a := websub_calling(t)
	if user == nil || a == nil {
		return sl_error(fn, "no user")
	}
	if !websub_declares(user, a, "websub/notify") {
		return sl_error(fn, "the app declares no websub/notify function")
	}
	if source == "" || !websub_web(topic) || !websub_web(hub) {
		return sl_error(fn, "invalid source, topic or hub")
	}
	if !networks_development || server_public_url() == "" {
		return sl.False, nil
	}
	db := networks_database()
	if row, _ := db.row("select * from websub where user=? and app=? and source=?", user.UID, a.id, source); row != nil {
		state := row_string(row, "state")
		if row_string(row, "topic") == topic && row_string(row, "hub") == hub {
			switch {
			case state == "pending" || state == "active":
				return sl.True, nil
			case state == "failed" && row_int(row, "asked") > now()-24*3600:
				return sl.False, nil
			case state == "denied":
				return sl.False, nil
			}
		}
		db.exec("delete from websub where id=?", row_string(row, "id"))
		if state == "active" || state == "pending" {
			go websub_ask(row, "unsubscribe")
		}
	}
	id := random_alphanumeric(32)
	db.exec("insert into websub (id, user, app, source, topic, hub, secret, state, created) values (?, ?, ?, ?, ?, ?, ?, 'pending', ?)", id, user.UID, a.id, source, topic, hub, random_alphanumeric(40), now())
	go websub_subscribe(id)
	return sl.True, nil
}

// mochi.websub.unsubscribe(source) -> None: End a source's subscription,
// telling its hub
func api_websub_unsubscribe(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var source string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "source", &source); err != nil {
		return sl_error(fn, "%v", err)
	}
	user, a := websub_calling(t)
	if user == nil || a == nil {
		return sl_error(fn, "no user")
	}
	db := networks_database()
	row, _ := db.row("select * from websub where user=? and app=? and source=?", user.UID, a.id, source)
	if row == nil {
		return sl.None, nil
	}
	state := row_string(row, "state")
	if state != "active" && state != "pending" {
		db.exec("delete from websub where id=?", row_string(row, "id"))
		return sl.None, nil
	}
	db.exec("update websub set state='leaving', asked=? where id=?", now(), row_string(row, "id"))
	go func() {
		if err := websub_ask(row, "unsubscribe"); err != nil {
			db.exec("delete from websub where id=?", row_string(row, "id"))
		}
	}()
	return sl.None, nil
}

// mochi.websub.get(source) -> dict: A source's subscription as {topic, hub,
// state, expires}, its state pending, active, leaving, failed or denied, or
// None
func api_websub_get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var source string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "source", &source); err != nil {
		return sl_error(fn, "%v", err)
	}
	user, a := websub_calling(t)
	if user == nil || a == nil {
		return sl_error(fn, "no user")
	}
	row, _ := networks_database().row("select topic, hub, state, expires from websub where user=? and app=? and source=?", user.UID, a.id, source)
	if row == nil {
		return sl.None, nil
	}
	return sl_encode(row), nil
}

// mochi.websub.hub(topic, key) -> string: Name a web feed the app serves as a
// topic this server's hub publishes, under a key of the app's own, and answer
// the hub's address for the feed to name; "" where the server has no public
// address. The topic must be an address on this server. The hub asks the
// app's websub/topic function whether the topic is still published before it
// takes a subscription, and its websub/content function for the document
func api_websub_hub(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var topic, key string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "topic", &topic, "key", &key); err != nil {
		return sl_error(fn, "%v", err)
	}
	user, a := websub_calling(t)
	if user == nil || a == nil {
		return sl_error(fn, "no user")
	}
	if !websub_declares(user, a, "websub/topic", "websub/content") {
		return sl_error(fn, "the app declares no websub/topic and websub/content functions")
	}
	if !websub_web(topic) || key == "" {
		return sl_error(fn, "invalid topic or key")
	}
	base := server_public_url()
	if !networks_development || base == "" {
		return sl.String(""), nil
	}
	u, _ := url.Parse(topic)
	host := strings.ToLower(u.Hostname())
	if host != server_domain() && !domain_served(host) {
		return sl_error(fn, "the topic is not on this server")
	}
	db := networks_database()
	if row, _ := db.row("select user, app from topics where topic=?", topic); row != nil && (row_string(row, "user") != user.UID || row_string(row, "app") != a.id) {
		return sl_error(fn, "the topic is another's")
	}
	db.exec("insert into topics (topic, user, app, key, created) values (?, ?, ?, ?, ?) on conflict(topic) do update set key=excluded.key", topic, user.UID, a.id, key, now())
	return sl.String(base + "/websub"), nil
}

// mochi.websub.publish(key) -> int: Tell the subscribers of the app's topics
// under a key that they have changed: shortly after, the hub asks the app's
// websub/content function for each one's document and sends it to them.
// Answers how many topics are announced
func api_websub_publish(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var key string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "key", &key); err != nil {
		return sl_error(fn, "%v", err)
	}
	user, a := websub_calling(t)
	if user == nil || a == nil {
		return sl_error(fn, "no user")
	}
	if !networks_development {
		return sl.MakeInt(0), nil
	}
	rows, _ := networks_database().rows("select topic from topics where user=? and app=? and key=?", user.UID, a.id, key)
	for _, row := range rows {
		websub_announce(row_string(row, "topic"))
	}
	return sl.MakeInt(len(rows)), nil
}

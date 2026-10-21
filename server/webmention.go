// Mochi server: Webmention
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Webmention (a W3C Recommendation) tells a page that another links to it:
// the sender finds the page's endpoint and posts both addresses there, and
// the receiver fetches the linking page and confirms the link before it
// believes it. Core speaks both sides, for the apps.
//
// Receiving, an entity's pages name /webmention/<entity> as their endpoint
// (mochi.webmention.endpoint). A mention there is accepted at once and
// checked afterwards: core fetches the source, confirms it links to the
// target, reads the parts of its h-entry a reply needs, and hands the result
// to the app's webmention/receive function as the entity's owner. A source
// that is gone, or no longer links, arrives as a deletion.
//
// Sending, an app names a page of its own and the pages it links to
// (mochi.webmention.send); core finds each one's endpoint and tells it. Both
// directions run from one queue that survives a restart, and every request
// goes through the host policy.

package main

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
	"golang.org/x/net/html"
)

// webmention_attempts is how many times a mention is tried before it is let
// go; webmention_page_maximum the most of a page read.
const webmention_attempts = 5
const webmention_page_maximum = 1 << 20

// webmention_targets_maximum is the most pages one send names.
const webmention_targets_maximum = 100

// webmention_content_maximum is the most of a mention's text kept.
const webmention_content_maximum = 5000

// rate_limit_webmention bounds the mentions one address sends.
var rate_limit_webmention = &rate_limiter{entries: make(map[string]*rate_limit_entry), limit: 30, window: 60}

// webmention_caller calls an app's function as a user; tests replace it.
var webmention_caller = app_call_as_server

// webmention_tables makes the queue in networks.db: each mention received
// and not yet checked, and each to send, by direction, source and target.
func webmention_tables(db *DB) {
	db.exec("create table if not exists webmentions (id integer primary key autoincrement, direction text not null, source text not null, target text not null, user text not null default '', app text not null default '', entity text not null default '', attempts integer not null default 0, next integer not null, created integer not null, unique (direction, source, target))")
	db.exec("create index if not exists webmentions_next on webmentions(next)")
}

// webmention_engine is the engine answering senders.
func webmention_engine() *engine {
	return &engine{name: "webmention", prefixes: []string{"/webmention/"}, handler: webmention_route}
}

// webmention_route answers /webmention/<entity>. Like every engine of the
// federation work it answers nothing while the development switch is off.
func webmention_route(c *gin.Context) bool {
	if !networks_development {
		respond_error(c, http.StatusNotFound, "not_found", "errors.not_found", nil)
		return true
	}
	entity := strings.TrimPrefix(c.Request.URL.Path, "/webmention/")
	if entity == "" || strings.Contains(entity, "/") {
		c.AbortWithStatus(http.StatusNotFound)
		return true
	}
	if c.Request.Method != http.MethodPost {
		c.AbortWithStatus(http.StatusMethodNotAllowed)
		return true
	}
	webmention_accept(c, entity)
	return true
}

// webmention_bare is an address without its fragment, as two addresses are
// compared.
func webmention_bare(value string) string {
	u, err := url.Parse(value)
	if err != nil {
		return value
	}
	u.Fragment = ""
	u.RawFragment = ""
	return u.String()
}

// webmention_here reports whether an address is on a domain this server
// serves.
func webmention_here(value string) bool {
	u, err := url.Parse(value)
	if err != nil {
		return false
	}
	host := strings.ToLower(u.Hostname())
	return host != "" && (host == server_domain() || domain_served(host))
}

// webmention_accept takes a mention for an entity's page: refused where the
// addresses are not web pages or the target is not here, accepted otherwise
// and queued to be checked. A mention sent again replaces the one waiting.
func webmention_accept(c *gin.Context, id string) {
	if !rate_limit_webmention.allow(c.ClientIP()) {
		c.AbortWithStatus(http.StatusTooManyRequests)
		return
	}
	source, target := c.PostForm("source"), c.PostForm("target")
	if !websub_web(source) || !websub_web(target) || webmention_bare(source) == webmention_bare(target) || !webmention_here(target) {
		c.AbortWithStatus(http.StatusBadRequest)
		return
	}
	e := entity_by_any(id)
	if e == nil {
		c.AbortWithStatus(http.StatusNotFound)
		return
	}
	user := user_by_uid(e.User)
	a := class_app_for(user, e.Class)
	if user == nil || !websub_declares(user, a, "webmention/receive") {
		c.AbortWithStatus(http.StatusNotFound)
		return
	}
	webmention_queue("received", source, target, user.UID, a.id, e.ID)
	c.AbortWithStatus(http.StatusAccepted)
}

// webmention_delay is how long a mention to send waits, so the change that
// prompted it - a page made, edited or deleted in the app's action - is in
// place by the time the target fetches the page.
var webmention_delay = 5 * time.Second

// webmention_queue adds a mention to the queue, or brings one waiting forward
// to be tried afresh: one received at once, one to send after
// webmention_delay.
func webmention_queue(direction string, source string, target string, user string, app string, entity string) {
	due := now()
	if direction == "sent" {
		due += int64(webmention_delay / time.Second)
	}
	networks_database().exec("insert into webmentions (direction, source, target, user, app, entity, attempts, next, created) values (?, ?, ?, ?, ?, ?, 0, ?, ?) on conflict(direction, source, target) do update set user=excluded.user, app=excluded.app, entity=excluded.entity, attempts=0, next=excluded.next",
		direction, source, target, user, app, entity, due, now())
	if direction == "sent" {
		time.AfterFunc(webmention_delay+time.Second, webmention_wake)
		return
	}
	webmention_wake()
}

var webmention_wakeup = make(chan struct{}, 1)

// webmention_wake has the queue looked at now rather than at the next tick.
func webmention_wake() {
	select {
	case webmention_wakeup <- struct{}{}:
	default:
	}
}

// webmention_manager works the queue each minute and whenever a mention
// joins it.
func webmention_manager() {
	tick := time.NewTicker(time.Minute)
	for {
		select {
		case <-tick.C:
		case <-webmention_wakeup:
		}
		webmention_work()
	}
}

var webmention_working sync.Mutex

// webmention_work takes the mentions due, four at a time, one round at a
// time. Each is moved on before it is tried, so one a restart interrupts is
// tried again later. A mention that fails for a reason that may pass waits
// twice as long each time, up to webmention_attempts; any other is done with.
func webmention_work() {
	if !networks_development {
		return
	}
	webmention_working.Lock()
	defer webmention_working.Unlock()
	db := networks_database()
	rows, _ := db.rows("select * from webmentions where next<=? order by next limit 20", now())
	var wait sync.WaitGroup
	slots := make(chan struct{}, 4)
	for _, row := range rows {
		id := row_int(row, "id")
		claimed := now() + 600
		db.exec("update webmentions set next=? where id=?", claimed, id)
		wait.Add(1)
		slots <- struct{}{}
		go func(row map[string]any) {
			defer wait.Done()
			defer func() { <-slots }()
			var again bool
			if row_string(row, "direction") == "received" {
				again = webmention_check(row)
			} else {
				again = webmention_tell(row)
			}
			attempts := row_int(row, "attempts") + 1
			// A mention sent again meanwhile moved its time back: it stays,
			// to be tried afresh.
			if again && attempts < webmention_attempts {
				db.exec("update webmentions set attempts=?, next=? where id=? and next=?", attempts, now()+int64(60<<attempts), id, claimed)
				return
			}
			db.exec("delete from webmentions where id=? and next=?", id, claimed)
		}(row)
	}
	wait.Wait()
}

// webmention_fetch reads a page for a user through the host policy: its
// status, the address it ended at, its headers and the start of its body.
func webmention_fetch(user *User, page string) (int, string, http.Header, []byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	r, err := http.NewRequestWithContext(ctx, http.MethodGet, page, nil)
	if err != nil {
		return 0, "", nil, nil, err
	}
	r.Header.Set("Accept", "text/html, application/xhtml+xml;q=0.9, */*;q=0.1")
	response, err := network_request(ctx, user, "", r, 5)
	if err != nil {
		return 0, "", nil, nil, err
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(response.Body, webmention_page_maximum))
	return response.StatusCode, response.Request.URL.String(), response.Header, body, nil
}

// webmention_check confirms a received mention and hands it to the app:
// a source that is gone, or no longer links to the target, as a deletion.
// Answers whether to try again, which a failure to reach the source or a
// server error asks for.
func webmention_check(row map[string]any) bool {
	user, a := user_by_uid(row_string(row, "user")), app_by_id(row_string(row, "app"))
	if user == nil || a == nil {
		return false
	}
	source, target := row_string(row, "source"), row_string(row, "target")
	status, final, _, body, err := webmention_fetch(user, source)
	if err != nil || status >= 500 || status == http.StatusTooManyRequests {
		return true
	}
	mention := Map{"source": source, "target": target, "entity": row_string(row, "entity"), "deleted": false}
	switch {
	case status == http.StatusGone || status == http.StatusNotFound:
		mention["deleted"] = true
	case status < 200 || status >= 300:
		return false
	default:
		document, err := html.Parse(strings.NewReader(string(body)))
		if err != nil {
			return false
		}
		base := webmention_base(document, final)
		if !webmention_links(document, base, target) {
			mention["deleted"] = true
		} else {
			for k, v := range webmention_entry(document, base, source, target) {
				mention[k] = v
			}
		}
	}
	if _, err := webmention_caller(user, a, "webmention/receive", mention); err != nil {
		info("Webmention from %s to %s not taken: %v", source, target, err)
	}
	return false
}

// webmention_tell sends a mention: finds the target's endpoint and posts the
// two addresses there. Answers whether to try again.
func webmention_tell(row map[string]any) bool {
	user := user_by_uid(row_string(row, "user"))
	if user == nil {
		return false
	}
	source, target := row_string(row, "source"), row_string(row, "target")
	endpoint, again := webmention_discover(user, target)
	if endpoint == "" {
		return again
	}
	form := url.Values{}
	form.Set("source", source)
	form.Set("target", target)
	status, _, err := websub_send(user, http.MethodPost, endpoint, "application/x-www-form-urlencoded", []byte(form.Encode()), nil)
	if err != nil || status >= 500 || status == http.StatusTooManyRequests {
		return true
	}
	if status >= 400 {
		debug("Webmention endpoint %s refused %s: %d", endpoint, source, status)
	}
	return false
}

// webmention_discover finds a page's endpoint: the first webmention link in
// its headers, else the first link or anchor with that relation in its
// document, resolved against the page; "" where it names none. Answers too
// whether the page should be asked again.
func webmention_discover(user *User, target string) (string, bool) {
	status, final, headers, body, err := webmention_fetch(user, target)
	if err != nil || status >= 500 || status == http.StatusTooManyRequests {
		return "", true
	}
	if status < 200 || status >= 300 {
		return "", false
	}
	base, _ := url.Parse(final)
	for _, value := range headers.Values("Link") {
		for _, link := range link_values(value) {
			if link.relation("webmention") {
				return webmention_resolve(base, link.href), false
			}
		}
	}
	if !strings.Contains(strings.ToLower(headers.Get("Content-Type")), "html") {
		return "", false
	}
	document, err := html.Parse(strings.NewReader(string(body)))
	if err != nil {
		return "", false
	}
	if b := webmention_base(document, final); b != nil {
		base = b
	}
	var found string
	var walk func(n *html.Node) bool
	walk = func(n *html.Node) bool {
		if n.Type == html.ElementNode && (n.Data == "link" || n.Data == "a") && webmention_has(webmention_attribute(n, "rel"), "webmention") {
			if href, present := webmention_attribute_present(n, "href"); present {
				found = webmention_resolve(base, href)
				return true
			}
		}
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			if walk(child) {
				return true
			}
		}
		return false
	}
	walk(document)
	return found, false
}

// === Links ===

// A link_value is one link of an HTTP Link header: its address and its
// relations.
type link_value struct {
	href      string
	relations []string
}

// relation reports whether a link has a relation.
func (l link_value) relation(name string) bool {
	for _, r := range l.relations {
		if strings.EqualFold(r, name) {
			return true
		}
	}
	return false
}

// link_values reads one Link header value: `<address>; rel="a b", <...>`.
func link_values(value string) []link_value {
	var out []link_value
	for value != "" {
		start := strings.Index(value, "<")
		end := strings.Index(value, ">")
		if start < 0 || end < start {
			break
		}
		link := link_value{href: strings.TrimSpace(value[start+1 : end])}
		rest := value[end+1:]
		next := strings.Index(rest, "<")
		parameters := rest
		if next >= 0 {
			parameters = rest[:next]
		}
		for _, p := range strings.Split(parameters, ";") {
			name, v, found := strings.Cut(strings.TrimSpace(p), "=")
			if found && strings.EqualFold(strings.TrimSpace(name), "rel") {
				link.relations = strings.Fields(strings.Trim(strings.TrimRight(strings.TrimSpace(v), ", "), `"`))
			}
		}
		out = append(out, link)
		if next < 0 {
			break
		}
		value = rest[next:]
	}
	return out
}

// webmention_resolve resolves an address against a page's.
func webmention_resolve(base *url.URL, reference string) string {
	r, err := url.Parse(strings.TrimSpace(reference))
	if err != nil || base == nil {
		return reference
	}
	return base.ResolveReference(r).String()
}

// webmention_attribute is an element's attribute, or "".
func webmention_attribute(n *html.Node, name string) string {
	value, _ := webmention_attribute_present(n, name)
	return value
}

// webmention_attribute_present is an element's attribute and whether it has
// one at all, as an empty href names the page itself.
func webmention_attribute_present(n *html.Node, name string) (string, bool) {
	for _, a := range n.Attr {
		if a.Namespace == "" && strings.EqualFold(a.Key, name) {
			return a.Val, true
		}
	}
	return "", false
}

// webmention_has reports whether a space-separated list holds a word.
func webmention_has(list string, word string) bool {
	for _, w := range strings.Fields(list) {
		if strings.EqualFold(w, word) {
			return true
		}
	}
	return false
}

// webmention_base is the address a document's links resolve against: its
// <base> where it has one, else the page's own.
func webmention_base(document *html.Node, page string) *url.URL {
	base, _ := url.Parse(page)
	var walk func(n *html.Node) bool
	walk = func(n *html.Node) bool {
		if n.Type == html.ElementNode && n.Data == "base" {
			if href := webmention_attribute(n, "href"); href != "" {
				if r, err := url.Parse(href); err == nil && base != nil {
					base = base.ResolveReference(r)
				}
			}
			return true
		}
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			if walk(child) {
				return true
			}
		}
		return false
	}
	walk(document)
	return base
}

// webmention_linking are the attributes an element links by.
var webmention_linking = map[string]string{"a": "href", "area": "href", "link": "href", "img": "src", "audio": "src", "video": "src", "source": "src", "iframe": "src"}

// webmention_links reports whether a document links to the target.
func webmention_links(document *html.Node, base *url.URL, target string) bool {
	want := webmention_bare(target)
	var walk func(n *html.Node) bool
	walk = func(n *html.Node) bool {
		if n.Type == html.ElementNode {
			if attribute, linking := webmention_linking[n.Data]; linking {
				if value := webmention_attribute(n, attribute); value != "" && webmention_bare(webmention_resolve(base, value)) == want {
					return true
				}
			}
		}
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			if walk(child) {
				return true
			}
		}
		return false
	}
	return walk(document)
}

// === Entries ===

// webmention_classed is the first element at or below n with a class, not
// looking inside another item (an element with an h-* class) below n.
func webmention_classed(n *html.Node, class string) *html.Node {
	var walk func(n *html.Node, top bool) *html.Node
	walk = func(n *html.Node, top bool) *html.Node {
		if n.Type == html.ElementNode {
			classes := webmention_attribute(n, "class")
			if webmention_has(classes, class) {
				return n
			}
			if !top && webmention_item(classes) {
				return nil
			}
		}
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			if found := walk(child, false); found != nil {
				return found
			}
		}
		return nil
	}
	return walk(n, true)
}

// webmention_item reports whether a class list makes an element an item of
// its own.
func webmention_item(classes string) bool {
	for _, c := range strings.Fields(classes) {
		if strings.HasPrefix(c, "h-") {
			return true
		}
	}
	return false
}

// webmention_text is an element's text, its white space folded.
func webmention_text(n *html.Node) string {
	var b strings.Builder
	var walk func(n *html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.TextNode {
			b.WriteString(n.Data)
			b.WriteString(" ")
		}
		if n.Type == html.ElementNode && (n.Data == "script" || n.Data == "style") {
			return
		}
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(n)
	return strings.Join(strings.Fields(b.String()), " ")
}

// webmention_url is an address an element gives a property: its href, its
// src, or its text.
func webmention_url(n *html.Node, base *url.URL) string {
	for _, attribute := range []string{"href", "src"} {
		if value := webmention_attribute(n, attribute); value != "" {
			return webmention_resolve(base, value)
		}
	}
	return webmention_resolve(base, webmention_text(n))
}

// webmention_entry reads what a reply needs from a source page's h-entry:
// its author's name, address and photo, its text, its address and when it
// was published, and what kind of mention it makes of the target - a reply,
// a like, a repost, a bookmark or a mention. A page with no h-entry gives
// its title as its text and its host as its author.
func webmention_entry(document *html.Node, base *url.URL, source string, target string) Map {
	out := Map{"kind": "mention", "url": source, "content": "", "published": int64(0)}
	author := map[string]any{"name": "", "url": "", "photo": ""}
	entry := webmention_classed(document, "h-entry")
	if entry == nil {
		if title := webmention_classed(document, "title"); title != nil {
			out["content"] = webmention_text(title)
		}
		var walk func(n *html.Node) bool
		walk = func(n *html.Node) bool {
			if n.Type == html.ElementNode && n.Data == "title" {
				out["content"] = webmention_text(n)
				return true
			}
			for child := n.FirstChild; child != nil; child = child.NextSibling {
				if walk(child) {
					return true
				}
			}
			return false
		}
		walk(document)
	} else {
		if a := webmention_classed(entry, "p-author"); a != nil {
			if webmention_has(webmention_attribute(a, "class"), "h-card") {
				if name := webmention_classed(a, "p-name"); name != nil {
					author["name"] = webmention_text(name)
				} else {
					author["name"] = webmention_text(a)
				}
				if u := webmention_classed(a, "u-url"); u != nil {
					author["url"] = webmention_url(u, base)
				} else if href := webmention_attribute(a, "href"); href != "" {
					author["url"] = webmention_resolve(base, href)
				}
				if photo := webmention_classed(a, "u-photo"); photo != nil {
					author["photo"] = webmention_url(photo, base)
				}
			} else {
				author["name"] = webmention_text(a)
				if href := webmention_attribute(a, "href"); href != "" {
					author["url"] = webmention_resolve(base, href)
				}
			}
		}
		for _, property := range []string{"e-content", "p-summary", "p-name"} {
			if n := webmention_classed(entry, property); n != nil {
				if text := webmention_text(n); text != "" {
					out["content"] = text
					break
				}
			}
		}
		if u := webmention_classed(entry, "u-url"); u != nil && !webmention_inside(u, "p-author") {
			out["url"] = webmention_url(u, base)
		}
		if p := webmention_classed(entry, "dt-published"); p != nil {
			stamp := webmention_attribute(p, "datetime")
			if stamp == "" {
				stamp = webmention_text(p)
			}
			if when, err := time.Parse(time.RFC3339, strings.TrimSpace(stamp)); err == nil {
				out["published"] = when.Unix()
			}
		}
		want := webmention_bare(target)
		for _, kind := range [][2]string{{"u-in-reply-to", "reply"}, {"u-like-of", "like"}, {"u-repost-of", "repost"}, {"u-bookmark-of", "bookmark"}} {
			if webmention_names(entry, kind[0], base, want) {
				out["kind"] = kind[1]
				break
			}
		}
	}
	if author["name"] == "" {
		if u, err := url.Parse(source); err == nil {
			author["name"] = u.Hostname()
		}
	}
	if content, _ := out["content"].(string); len([]rune(content)) > webmention_content_maximum {
		out["content"] = string([]rune(content)[:webmention_content_maximum]) + "…"
	}
	out["author"] = author
	return out
}

// webmention_inside reports whether an element lies within one with a class.
func webmention_inside(n *html.Node, class string) bool {
	for p := n.Parent; p != nil; p = p.Parent {
		if p.Type == html.ElementNode && webmention_has(webmention_attribute(p, "class"), class) {
			return true
		}
	}
	return false
}

// webmention_names reports whether an entry gives a property the target: an
// element with the property's class whose own address is the target, or
// which cites it (an h-cite with the target as its u-url).
func webmention_names(entry *html.Node, property string, base *url.URL, want string) bool {
	var walk func(n *html.Node) bool
	walk = func(n *html.Node) bool {
		if n.Type == html.ElementNode && webmention_has(webmention_attribute(n, "class"), property) {
			if webmention_bare(webmention_url(n, base)) == want {
				return true
			}
			if u := webmention_classed(n, "u-url"); u != nil && webmention_bare(webmention_url(u, base)) == want {
				return true
			}
		}
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			if walk(child) {
				return true
			}
		}
		return false
	}
	return walk(entry)
}

// === Starlark API ===

var api_webmention = sls.FromStringDict(sl.String("mochi.webmention"), sl.StringDict{
	"endpoint": sl.NewBuiltin("mochi.webmention.endpoint", api_webmention_endpoint),
	"send":     sl.NewBuiltin("mochi.webmention.send", api_webmention_send),
})

// mochi.webmention.endpoint(entity) -> string: The endpoint an entity of the
// user's names on its pages, where mentions of them are taken and, once
// checked, handed to the app's webmention/receive function; "" where the
// server has no public address or the entity is not the user's
func api_webmention_endpoint(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var id string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "entity", &id); err != nil {
		return sl_error(fn, "%v", err)
	}
	user, a := websub_calling(t)
	if user == nil || a == nil {
		return sl_error(fn, "no user")
	}
	if !websub_declares(user, a, "webmention/receive") {
		return sl_error(fn, "the app declares no webmention/receive function")
	}
	base := server_public_url()
	if !networks_development || base == "" {
		return sl.String(""), nil
	}
	e := entity_by_any(id)
	if e == nil || e.User != user.UID {
		return sl.String(""), nil
	}
	return sl.String(base + "/webmention/" + e.ID), nil
}

// mochi.webmention.send(source, targets) -> int: Tell the pages a page of
// this server's links to that it does: each one's endpoint is found and told,
// in the background and tried again where a page cannot be reached. Send
// again when the page changes or is deleted, naming the pages it linked to
// before as well as those it links to now. Answers how many are queued
func api_webmention_send(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var source string
	var targets *sl.List
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "source", &source, "targets", &targets); err != nil {
		return sl_error(fn, "%v", err)
	}
	user, a := websub_calling(t)
	if user == nil || a == nil {
		return sl_error(fn, "no user")
	}
	if !websub_web(source) || !webmention_here(source) {
		return sl_error(fn, "the source is not a page on this server")
	}
	if !networks_development {
		return sl.MakeInt(0), nil
	}
	seen := map[string]bool{webmention_bare(source): true}
	count := 0
	for i := 0; i < targets.Len() && count < webmention_targets_maximum; i++ {
		target, ok := sl.AsString(targets.Index(i))
		if !ok || !websub_web(target) || seen[webmention_bare(target)] {
			continue
		}
		seen[webmention_bare(target)] = true
		webmention_queue("sent", source, target, user.UID, a.id, "")
		count++
	}
	return sl.MakeInt(count), nil
}

// === Pages ===

// opengraph_headers sends the links an OpenGraph function declares as Link
// headers too, where a sender looks for an endpoint first.
func opengraph_headers(c *gin.Context, value any) {
	links, _ := value.([]any)
	for _, item := range links {
		link, _ := item.(map[string]any)
		rel, _ := link["rel"].(string)
		href, _ := link["href"].(string)
		if opengraph_rel_re.MatchString(rel) && websub_web(href) && !strings.ContainsAny(href, "<>\r\n") {
			c.Writer.Header().Add("Link", "<"+href+">; rel=\""+rel+"\"")
		}
	}
}

// opengraph_entry renders the entry an OpenGraph function declares for a
// page - {content, author, url, published}, the content HTML - as an h-entry,
// so a page the app draws in the browser carries its text and links for
// those that read it without running it: a server checking a mention, a
// reader, a search engine. The content is sanitised as rendered Markdown is.
func opengraph_entry(value any) string {
	entry, _ := value.(map[string]any)
	if entry == nil {
		return ""
	}
	content, _ := entry["content"].(string)
	author, _ := entry["author"].(string)
	page, _ := entry["url"].(string)
	var published int64
	switch v := entry["published"].(type) {
	case int64:
		published = v
	case int:
		published = int64(v)
	case float64:
		published = int64(v)
	}
	if content == "" {
		return ""
	}
	var b strings.Builder
	b.WriteString(`<article class="h-entry">`)
	if author != "" {
		b.WriteString(`<p class="p-author h-card"><span class="p-name">` + html.EscapeString(author) + `</span></p>`)
	}
	b.WriteString(`<div class="e-content">` + markdown_policy.Sanitize(content) + `</div>`)
	if websub_web(page) {
		b.WriteString(`<p><a class="u-url" href="` + escape_attribute(page) + `">`)
		if published > 0 {
			stamp := time.Unix(published, 0).UTC().Format(time.RFC3339)
			b.WriteString(`<time class="dt-published" datetime="` + stamp + `">` + stamp + `</time>`)
		} else {
			b.WriteString(html.EscapeString(page))
		}
		b.WriteString(`</a></p>`)
	}
	b.WriteString(`</article>`)
	return b.String()
}

// text_links are the web addresses a markdown text links to, in order and
// each once: the anchors of its rendered HTML, bare addresses included as the
// renderer links them.
func text_links(markdown_text string) []string {
	out := []string{}
	document, err := html.Parse(strings.NewReader(string(markdown([]byte(markdown_text)))))
	if err != nil {
		return out
	}
	seen := map[string]bool{}
	var walk func(n *html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "a" {
			if href := webmention_attribute(n, "href"); websub_web(href) && !seen[href] {
				seen[href] = true
				out = append(out, href)
			}
		}
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(document)
	return out
}

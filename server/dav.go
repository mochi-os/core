// Mochi server: CardDAV and CalDAV served on behalf of apps
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/emersion/go-ical"
	"github.com/emersion/go-vcard"
	"github.com/emersion/go-webdav"
	"github.com/emersion/go-webdav/caldav"
	"github.com/emersion/go-webdav/carddav"
	"github.com/gin-gonic/gin"
)

// A manifest action declaring a DAV feature ("carddav/*path", "caldav/*path")
// hands its requests to the go-webdav server. The server's backend is the app's
// own dav/* functions, called as the server the way a scheduled event is: the
// app never sees HTTP, XML, vCard or iCalendar syntax. Cards travel as property
// lists and calendar objects as iCalendar text; this file converts at the
// boundary.
//
// The route is class-level and public, so core resolves no entity and would
// run an anonymous request as the first administrator. The handler therefore
// authenticates before touching anything, refuses every unauthenticated
// request, and takes the owner from the credential alone.
//
// Paths follow the shape the library expects under the route prefix:
//
//	<prefix>/                                  root: names the principal
//	<prefix>/<principal>/                      the identity's fingerprint
//	<prefix>/<principal>/books/                home set (calendars/ for CalDAV)
//	<prefix>/<principal>/books/<slug>/         one collection
//	<prefix>/<principal>/books/<slug>/<name>.vcf  one object (.ics for CalDAV)
//
// Collection and object names are the app's slugs: a client creates both at a
// name of its own choosing and expects to find them there afterwards.

// dav_body_maximum caps a vCard or iCalendar body a client PUTs. Cards carry
// photos, so the general request cap is too small; nothing legitimate is larger.
const dav_body_maximum = 2 << 20

// dav_realm is the Basic challenge. The username is ignored; the password is a
// token minted by the app with the dav scope.
const dav_realm = `Basic realm="Mochi"`

// dav_feature describes what a feature serves: the home-set segment under the
// principal and the object suffix.
type dav_feature struct {
	home   string
	suffix string
}

var dav_features = map[string]dav_feature{
	"carddav": {"books", ".vcf"},
	"caldav":  {"calendars", ".ics"},
}

// dav_served reports whether a manifest feature is one this file handles.
func dav_served(feature string) bool {
	_, ok := dav_features[feature]
	return ok
}

// dav_name_re bounds the collection and object names a client may choose.
// Names arrive percent-decoded, so this is the character set of the name
// itself, and it is what a UUID, a fingerprint or a contact id needs.
var dav_name_re = regexp.MustCompile(`^[A-Za-z0-9._~@+=-]{1,128}$`)

func dav_name_valid(name string) bool {
	return dav_name_re.MatchString(name)
}

// Resource kinds along a request path.
const (
	dav_kind_root = iota
	dav_kind_principal
	dav_kind_home
	dav_kind_collection
	dav_kind_object
)

// dav_caller runs one of the app's dav/* functions and returns its decoded
// result. The server calls Starlark; tests substitute a map.
type dav_caller func(function string, args Map) (any, error)

// dav_backend implements the carddav and caldav Backend interfaces for one
// request: one authenticated identity, one app, one route prefix.
type dav_backend struct {
	feature   string
	prefix    string // the request path up to and including the feature segment, no trailing slash
	principal string // the identity's fingerprint: the DAV principal segment
	call      dav_caller
	// The request's preconditions, which the library passes to PUT but not
	// to DELETE.
	match webdav.ConditionalMatch
	none  webdav.ConditionalMatch
	// calls counts the app calls this request has made; see dav_calls_maximum.
	calls int
	// data says a listing must carry each object's content: the request
	// named address-data or calendar-data, or asked for every property. An
	// ordinary listing wants etags, and costs the app no card at all.
	data bool
	// prefetch holds the object names a report's hrefs ask for, by collection,
	// so each collection is fetched once with exactly those names, and cache
	// what came back. A missing collection caches as empty.
	prefetch map[string][]string
	cache    map[string]map[string]*dav_object
	// retained counts the bytes of object content this request holds for its
	// answer; see dav_retained_maximum.
	retained int
}

// dav_calls_maximum bounds the app calls one request may make. A report's
// hrefs are fetched one call per collection; this refuses a body naming
// hundreds of different ones. A depth-infinity listing makes one call per
// collection, and a query one per page.
const dav_calls_maximum = 500

// dav_hrefs_maximum bounds the hrefs one report may name. Clients ask in
// batches of tens to a few hundred.
const dav_hrefs_maximum = 1000

// dav_retained_maximum bounds the object content one answer carries. The
// library builds the whole multistatus in memory before writing it, so an
// answer that would hold more is refused with 507 and the client asks for
// less at a time.
const dav_retained_maximum = 64 << 20

// A query walks a collection this many objects per app call, keeping only the
// matches, and stops at dav_matches_maximum of them, which the protocol lets a
// server do.
const dav_page = 100
const dav_matches_maximum = 1000

// dav_collection is what dav/collections returns per collection.
type dav_collection struct {
	slug        string
	name        string
	description string
	readonly    bool
	components  []string
}

// dav_object is what dav/objects returns per object: a property list for a
// card, iCalendar text for a calendar object.
type dav_object struct {
	name    string
	etag    string
	updated time.Time
	card    any
	ics     string
	size    int // bytes of content, for dav_retained_maximum
}

var dav_not_found = webdav.NewHTTPError(http.StatusNotFound, errors.New("not found"))

// dav_http_handler serves one request on a DAV feature route. The credential
// is read here and nowhere else: web_action's app-token gate does not run on
// a public route, so the user it resolved - from a cookie, a query token or
// any app's JWT - is never trusted.
func dav_http_handler(c *gin.Context, a *App, aa *AppAction) bool {
	user := dav_authenticate(c, a, aa)
	if user == nil {
		// A client may probe the capabilities before it has a credential to
		// offer; the answer names the protocol and nothing of anyone's.
		if c.Request.Method == http.MethodOptions {
			dav_options_anonymous(c, aa.Feature)
			return true
		}
		c.Header("WWW-Authenticate", dav_realm)
		c.String(http.StatusUnauthorized, "Authentication required") // i18n-ok: DAV protocol, read by the client not a person
		return true
	}
	if user_pending(user) {
		c.String(http.StatusServiceUnavailable, "Account is being restored") // i18n-ok: DAV protocol, read by the client not a person
		return true
	}
	identity := user.identity()
	if identity == nil {
		c.String(http.StatusForbidden, "No identity") // i18n-ok: DAV protocol, read by the client not a person
		return true
	}
	// The route is public, so web_action skipped the app's own requirements
	// (require.role, require.function). A token minted before the app stopped
	// being offered to this account must stop working with it.
	av := a.active(user)
	if av == nil || !av.user_allowed(user) || !app_visible(av, user) {
		c.String(http.StatusForbidden, "Forbidden") // i18n-ok: DAV protocol, read by the client not a person
		return true
	}
	prefix := dav_prefix(c.Request.URL.Path, aa.name)
	if prefix == "" {
		c.String(http.StatusNotFound, "Not found") // i18n-ok: DAV protocol, read by the client not a person
		return true
	}

	b := &dav_backend{
		feature:   aa.Feature,
		prefix:    prefix,
		principal: identity.Fingerprint,
		match:     webdav.ConditionalMatch(c.GetHeader("If-Match")),
		none:      webdav.ConditionalMatch(c.GetHeader("If-None-Match")),
		call: func(function string, args Map) (any, error) {
			// The app's functions take the identity they act for, as the
			// friends service always has.
			args["identity"] = identity.ID
			return app_call_as_server(user, a, function, args)
		},
	}
	var handler http.Handler
	switch aa.Feature {
	case "carddav":
		handler = &carddav.Handler{Backend: b, Prefix: prefix}
	case "caldav":
		handler = &caldav.Handler{Backend: b, Prefix: prefix}
	default:
		c.String(http.StatusNotFound, "Not found") // i18n-ok: DAV protocol, read by the client not a person
		return true
	}
	// The router reached web_path through NoRoute, which pre-sets 404; a GET
	// the library answers with a body alone would carry it. Recorded, not
	// written: the library's own status still wins.
	c.Writer.WriteHeader(http.StatusOK)
	dav_body_settle(c.Request)
	if status := b.survey(c.Request); status != 0 {
		c.String(status, http.StatusText(status))
		return true
	}
	dav_mkcalendar(aa.Feature, c.Request)
	handler.ServeHTTP(c.Writer, c.Request)
	debug("DAV %s %s %s -> %d user=%s agent=%q", aa.Feature, c.Request.Method, c.Request.URL.Path, c.Writer.Status(), user.UID, c.GetHeader("User-Agent"))
	return true
}

// dav_body_settle makes an empty request body read as empty. The library
// asks by reading zero bytes and expecting end of file, and web_body_limit's
// wrapper answers a zero-length read with no error at all, so a PROPFIND with
// no body - which means all properties, and which clients send - was refused
// as carrying an unsupported one.
func dav_body_settle(r *http.Request) {
	if r.Body == nil || r.Body == http.NoBody {
		r.Body = http.NoBody
		return
	}
	reader := bufio.NewReader(r.Body)
	if _, err := reader.Peek(1); err == io.EOF {
		r.Body.Close()
		r.Body = http.NoBody
		return
	}
	r.Body = struct {
		io.Reader
		io.Closer
	}{reader, r.Body}
}

// survey reads a PROPFIND or REPORT body before the library does, to learn
// what the answer will need: whether listings must carry content, and which
// objects a report's hrefs name. The body is put back for the library. It is
// XML under the general request cap; a body that does not parse is left for
// the library to refuse. Returns a status to answer with instead, or 0.
func (b *dav_backend) survey(r *http.Request) int {
	if r.Method != "PROPFIND" && r.Method != "REPORT" {
		return 0
	}
	if r.Body == nil || r.Body == http.NoBody {
		// An empty PROPFIND asks for every property.
		b.data = true
		return 0
	}
	body, err := io.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		return http.StatusRequestEntityTooLarge
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	decoder := xml.NewDecoder(bytes.NewReader(body))
	inside := false
	var text strings.Builder
	hrefs := 0
	for {
		token, err := decoder.Token()
		if err != nil {
			return 0
		}
		switch element := token.(type) {
		case xml.StartElement:
			switch element.Name.Local {
			case "address-data", "calendar-data", "allprop":
				b.data = true
			case "href":
				inside = element.Name.Space == "DAV:"
				text.Reset()
			}
		case xml.CharData:
			if inside {
				text.Write(element)
			}
		case xml.EndElement:
			if !inside || element.Name.Local != "href" {
				continue
			}
			inside = false
			hrefs++
			if hrefs > dav_hrefs_maximum {
				return http.StatusRequestEntityTooLarge
			}
			target, err := url.Parse(strings.TrimSpace(text.String()))
			if err != nil {
				continue
			}
			kind, collection, name, err := b.parse(target.Path)
			if err != nil || kind != dav_kind_object {
				continue
			}
			if b.prefetch == nil {
				b.prefetch = map[string][]string{}
			}
			b.prefetch[collection] = append(b.prefetch[collection], name)
		}
	}
}

// retain accounts for object content kept for the answer.
func (b *dav_backend) retain(size int) error {
	b.retained += size
	if b.retained > dav_retained_maximum {
		return webdav.NewHTTPError(http.StatusInsufficientStorage, errors.New("answer too large"))
	}
	return nil
}

// dav_content_size estimates the bytes a stored card occupies: the text of
// its names, parameters and values.
func dav_content_size(v any) int {
	switch x := v.(type) {
	case string:
		return len(x)
	case []any:
		size := 0
		for _, item := range x {
			size += dav_content_size(item) + 1
		}
		return size
	case map[string]any:
		size := 0
		for k, item := range x {
			size += len(k) + dav_content_size(item) + 1
		}
		return size
	}
	return 8
}

// dav_options_anonymous answers an OPTIONS probe with the capabilities the
// library would report for a collection, without a backend call.
func dav_options_anonymous(c *gin.Context, feature string) {
	capability := "addressbook"
	allow := "OPTIONS, PROPFIND, REPORT, DELETE, MKCOL"
	if feature == "caldav" {
		capability = "calendar-access"
		allow += ", MKCALENDAR"
	}
	c.Header("DAV", "1, 3, "+capability)
	c.Header("Allow", allow)
	c.Status(http.StatusOK)
}

// dav_mkcalendar turns a MKCALENDAR request into the MKCOL the library
// serves. Every calendar client creates a calendar with MKCALENDAR (RFC
// 4791), which the library does not know; its MKCOL reads the same display
// name and answers with the same statuses (201, 405 when the collection
// exists, 403 away from the home set). The description a client sends is
// dropped, as the library's MKCOL drops it. A body over dav_mkcalendar_maximum
// is treated as empty: a display name needs a few hundred bytes.
func dav_mkcalendar(feature string, r *http.Request) {
	if feature != "caldav" || r.Method != "MKCALENDAR" {
		return
	}
	name := ""
	if r.Body != nil {
		body, _ := io.ReadAll(io.LimitReader(r.Body, dav_mkcalendar_maximum+1))
		r.Body.Close()
		if len(body) <= dav_mkcalendar_maximum {
			decoder := xml.NewDecoder(bytes.NewReader(body))
			for {
				token, err := decoder.Token()
				if err != nil {
					break
				}
				if start, ok := token.(xml.StartElement); ok && start.Name.Local == "displayname" {
					var text string
					if decoder.DecodeElement(&text, &start) == nil {
						name = text
					}
					break
				}
			}
		}
	}
	var escaped bytes.Buffer
	xml.EscapeText(&escaped, []byte(name))
	mkcol := `<?xml version="1.0" encoding="utf-8"?><D:mkcol xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav"><D:set><D:prop><D:resourcetype><D:collection/><C:calendar/></D:resourcetype><D:displayname>` + escaped.String() + `</D:displayname></D:prop></D:set></D:mkcol>`
	r.Method = "MKCOL"
	r.Body = io.NopCloser(strings.NewReader(mkcol))
	r.ContentLength = int64(len(mkcol))
	r.Header.Set("Content-Type", "application/xml; charset=utf-8")
}

const dav_mkcalendar_maximum = 65536

// dav_authenticate resolves the caller from the Authorization header alone: a
// Basic password or a Bearer value that is a mochi- token minted by this app
// with the dav scope, bound to this route or unbound. Nothing else counts. A
// token in the URL is refused because a DAV client never needs one and a URL
// is logged and copied; a JWT because it is any app's; a session cookie
// because no DAV client has one and a browser that does is the one party that
// must not be able to write here.
func dav_authenticate(c *gin.Context, a *App, aa *AppAction) *User {
	if c.Query("token") != "" {
		return nil
	}
	if _, password, ok := c.Request.BasicAuth(); ok {
		return dav_token_user(password, a, aa)
	}
	header := c.GetHeader("Authorization")
	if strings.HasPrefix(header, "Bearer ") {
		return dav_token_user(strings.TrimPrefix(header, "Bearer "), a, aa)
	}
	return nil
}

// dav_token_user validates a token the way git_authenticate does: this app,
// the dav scope, bound to this route if bound at all, and an active account.
// user_by_uid filters only suspended accounts, and a closing account is gone
// on every other surface.
func dav_token_user(secret string, a *App, aa *AppAction) *User {
	token := token_validate(secret)
	if token == nil || token.App != a.id {
		return nil
	}
	if !token_has_scope(token, "dav") || !token_allows(token, aa.name, "", "") {
		return nil
	}
	user := user_by_uid(token.User)
	if user == nil || user.Status != "active" {
		return nil
	}
	return user
}

// dav_route_literal returns the literal segments of a feature route before its
// wildcard ("carddav" for "carddav/*path"), or "" when a segment is a
// parameter: such a route needs an entity and has no fixed root.
func dav_route_literal(name string) string {
	segments := strings.Split(name, "/")
	if len(segments) < 2 || !strings.HasPrefix(segments[len(segments)-1], "*") {
		return ""
	}
	for _, s := range segments[:len(segments)-1] {
		if s == "" || strings.HasPrefix(s, ":") || strings.HasPrefix(s, "*") {
			return ""
		}
	}
	return strings.Join(segments[:len(segments)-1], "/")
}

// dav_prefix finds the route prefix inside the request path: everything up to
// and including the feature's literal segment. The app may be mounted at its
// path or at a domain's root, so the prefix is read from the request rather
// than assembled.
func dav_prefix(request string, name string) string {
	literal := dav_route_literal(name)
	if literal == "" {
		return ""
	}
	marker := "/" + literal
	if strings.HasSuffix(request, marker) {
		return request
	}
	if i := strings.Index(request, marker+"/"); i >= 0 {
		return request[:i+len(marker)]
	}
	return ""
}

// dav_root returns the plain root of the installed app serving a feature,
// derived from manifests: "/people/carddav/". Empty when no app declares it.
// Only an app that is the server's own choice for the path it is mounted at
// counts, so an app merely installed cannot capture discovery; among those,
// development apps win, then the lowest id, so the answer is stable.
func dav_root(feature string) string {
	apps_lock.Lock()
	list := make([]*App, 0, len(apps))
	for _, a := range apps {
		list = append(list, a)
	}
	apps_lock.Unlock()
	sort.Slice(list, func(i, j int) bool {
		if list[i].development != list[j].development {
			return list[i].development
		}
		return list[i].id < list[j].id
	})
	for _, a := range list {
		av := a.active(nil)
		if av == nil || app_for_path(nil, a.url_path(nil)) != a {
			continue
		}
		for name, aa := range av.Actions {
			if aa.Feature != feature {
				continue
			}
			if literal := dav_route_literal(name); literal != "" {
				return "/" + a.url_path(nil) + "/" + literal + "/"
			}
		}
	}
	return ""
}

// web_well_known_dav answers /.well-known/carddav and /.well-known/caldav with
// a redirect to the root of whichever installed app serves the feature. 308
// keeps the client's method, which is PROPFIND as often as GET.
func web_well_known_dav(c *gin.Context) {
	feature := strings.TrimPrefix(c.Request.URL.Path, "/.well-known/")
	root := dav_root(feature)
	if root == "" {
		c.String(http.StatusNotFound, "Not found") // i18n-ok: DAV protocol, read by the client not a person
		return
	}
	c.Redirect(http.StatusPermanentRedirect, root)
}

// === Paths ===

func (b *dav_backend) principal_path() string {
	return b.prefix + "/" + b.principal + "/"
}

func (b *dav_backend) home_path() string {
	return b.principal_path() + dav_features[b.feature].home + "/"
}

func (b *dav_backend) collection_path(slug string) string {
	return b.home_path() + slug + "/"
}

func (b *dav_backend) object_path(slug string, name string) string {
	return b.collection_path(slug) + name + dav_features[b.feature].suffix
}

// parse resolves a request path to its kind and the collection and object it
// names. A path under another principal is not found: the credential names
// one identity and nothing else is served here.
func (b *dav_backend) parse(request string) (kind int, collection string, object string, err error) {
	p := path.Clean(request)
	if p != b.prefix && !strings.HasPrefix(p, b.prefix+"/") {
		return 0, "", "", dav_not_found
	}
	rest := strings.Trim(strings.TrimPrefix(p, b.prefix), "/")
	if rest == "" {
		return dav_kind_root, "", "", nil
	}
	segments := strings.Split(rest, "/")
	if segments[0] != b.principal {
		return 0, "", "", dav_not_found
	}
	if len(segments) == 1 {
		return dav_kind_principal, "", "", nil
	}
	feature := dav_features[b.feature]
	if segments[1] != feature.home {
		return 0, "", "", dav_not_found
	}
	if len(segments) == 2 {
		return dav_kind_home, "", "", nil
	}
	collection = segments[2]
	if !dav_name_valid(collection) {
		return 0, "", "", dav_not_found
	}
	if len(segments) == 3 {
		return dav_kind_collection, collection, "", nil
	}
	if len(segments) > 4 {
		return 0, "", "", dav_not_found
	}
	object = strings.TrimSuffix(segments[3], feature.suffix)
	if object == segments[3] || !dav_name_valid(object) {
		return 0, "", "", dav_not_found
	}
	return dav_kind_object, collection, object, nil
}

// === Calls into the app ===

// dav_error maps an app's error code to the HTTP status a DAV client acts on.
func dav_error(code string) error {
	switch code {
	case "not_found":
		return dav_not_found
	case "conflict":
		return webdav.NewHTTPError(http.StatusPreconditionFailed, errors.New("precondition failed"))
	case "forbidden", "readonly":
		return webdav.NewHTTPError(http.StatusForbidden, errors.New("forbidden"))
	case "exists":
		return webdav.NewHTTPError(http.StatusMethodNotAllowed, errors.New("already exists"))
	case "duplicate":
		return webdav.NewHTTPError(http.StatusConflict, errors.New("another object in the collection has this uid"))
	case "too_large":
		return webdav.NewHTTPError(http.StatusRequestEntityTooLarge, errors.New("too large"))
	case "full":
		return webdav.NewHTTPError(http.StatusInsufficientStorage, errors.New("collection full"))
	case "invalid":
		return webdav.NewHTTPError(http.StatusBadRequest, errors.New("invalid"))
	}
	return webdav.NewHTTPError(http.StatusInternalServerError, errors.New("server error"))
}

// invoke runs an app function and turns its {"error": code} answer into the
// matching HTTP error. A failure inside the app goes to the log with its
// detail and to the client as a bare 500.
func (b *dav_backend) invoke(function string, args Map) (any, error) {
	b.calls++
	if b.calls > dav_calls_maximum {
		return nil, webdav.NewHTTPError(http.StatusServiceUnavailable, errors.New("too many operations in one request"))
	}
	result, err := b.call(function, args)
	if err != nil {
		info("DAV %s: %v", function, path_scrub(err.Error()))
		return nil, dav_error("")
	}
	if m, ok := result.(map[string]any); ok {
		if code := dav_string(m["error"]); code != "" {
			return nil, dav_error(code)
		}
	}
	return result, nil
}

func dav_string(v any) string {
	s, _ := v.(string)
	return s
}

func dav_bool(v any) bool {
	switch x := v.(type) {
	case bool:
		return x
	case int64:
		return x != 0
	case int:
		return x != 0
	}
	return false
}

func dav_int64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case float64:
		return int64(x)
	}
	return 0
}

func dav_strings(v any) []string {
	list, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(list))
	for _, item := range list {
		if s := dav_string(item); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// collections lists the app's collections, or the one named.
func (b *dav_backend) collections(slug string) ([]dav_collection, error) {
	args := Map{}
	if slug != "" {
		args["collection"] = slug
	}
	result, err := b.invoke("dav/collections", args)
	if err != nil {
		return nil, err
	}
	list, _ := result.([]any)
	out := make([]dav_collection, 0, len(list))
	for _, item := range list {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		c := dav_collection{
			slug:        dav_string(m["slug"]),
			name:        dav_string(m["name"]),
			description: dav_string(m["description"]),
			readonly:    dav_bool(m["readonly"]),
			components:  dav_strings(m["components"]),
		}
		if !dav_name_valid(c.slug) {
			continue
		}
		out = append(out, c)
	}
	return out, nil
}

func (b *dav_backend) collection(slug string) (*dav_collection, error) {
	list, err := b.collections(slug)
	if err != nil {
		return nil, err
	}
	for i := range list {
		if list[i].slug == slug {
			return &list[i], nil
		}
	}
	return nil, dav_not_found
}

// objects lists a collection's objects, or the ones named. extra carries a
// query's time range, so a calendar app can answer with a superset from its
// shadow columns rather than every object it holds, or a page (offset,
// limit). data asks for each object's content; without it the app answers
// names, etags and times alone.
func (b *dav_backend) objects(collection string, names []string, extra Map, data bool) ([]dav_object, error) {
	args := Map{"collection": collection, "data": data}
	if names != nil {
		args["names"] = names
	}
	for k, v := range extra {
		args[k] = v
	}
	result, err := b.invoke("dav/objects", args)
	if err != nil {
		return nil, err
	}
	list, _ := result.([]any)
	out := make([]dav_object, 0, len(list))
	for _, item := range list {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		o := dav_object{
			name: dav_string(m["name"]),
			etag: dav_string(m["etag"]),
			card: m["card"],
			ics:  dav_string(m["ics"]),
		}
		o.size = len(o.ics) + dav_content_size(o.card)
		if updated := dav_int64(m["updated"]); updated > 0 {
			o.updated = time.Unix(updated, 0)
		}
		if !dav_name_valid(o.name) {
			continue
		}
		out = append(out, o)
	}
	return out, nil
}

// dav_conditions reduces If-Match and If-None-Match to what the app checks:
// match is the etag the stored object must carry ("" for none, "*" for "must
// exist"), absent says it must not exist yet. A specific If-None-Match etag
// has no meaning on a PUT that no client sends, and is refused.
func dav_conditions(match webdav.ConditionalMatch, none webdav.ConditionalMatch) (string, bool, error) {
	required := ""
	if match.IsSet() {
		if match.IsWildcard() {
			required = "*"
		} else {
			etag, err := match.ETag()
			if err != nil {
				return "", false, webdav.NewHTTPError(http.StatusBadRequest, err)
			}
			required = etag
		}
	}
	absent := false
	if none.IsSet() {
		if !none.IsWildcard() {
			return "", false, webdav.NewHTTPError(http.StatusBadRequest, errors.New("If-None-Match must be *"))
		}
		absent = true
	}
	return required, absent, nil
}

// put writes one object and returns its new etag and time.
func (b *dav_backend) put(args Map) (string, time.Time, error) {
	result, err := b.invoke("dav/put", args)
	if err != nil {
		return "", time.Time{}, err
	}
	m, _ := result.(map[string]any)
	updated := time.Time{}
	if stamp := dav_int64(m["updated"]); stamp > 0 {
		updated = time.Unix(stamp, 0)
	}
	return dav_string(m["etag"]), updated, nil
}

// === Shared backend ===

func (b *dav_backend) CurrentUserPrincipal(ctx context.Context) (string, error) {
	return b.principal_path(), nil
}

func (b *dav_backend) create_collection(p string, name string, description string) error {
	kind, slug, _, err := b.parse(p)
	if err != nil {
		return err
	}
	if kind != dav_kind_collection {
		return webdav.NewHTTPError(http.StatusForbidden, errors.New("collections live under the home set"))
	}
	_, err = b.invoke("dav/collections/create", Map{"collection": slug, "name": name, "description": description})
	return err
}

func (b *dav_backend) delete_collection(p string) error {
	kind, slug, _, err := b.parse(p)
	if err != nil {
		return err
	}
	if kind != dav_kind_collection {
		return dav_not_found
	}
	_, err = b.invoke("dav/collections/delete", Map{"collection": slug})
	return err
}

// delete_object removes one object, honouring If-Match: a device holding a
// stale copy must not delete what another device has just changed, and for a
// friend's card the deletion also ends the friendship.
func (b *dav_backend) delete_object(p string) error {
	kind, collection, name, err := b.parse(p)
	if err != nil {
		return err
	}
	if kind != dav_kind_object {
		return dav_not_found
	}
	c, err := b.collection(collection)
	if err != nil {
		return err
	}
	if c.readonly {
		return dav_error("readonly")
	}
	match, absent, err := dav_conditions(b.match, b.none)
	if err != nil {
		return err
	}
	_, err = b.invoke("dav/delete", Map{"collection": collection, "name": name, "match": match, "absent": absent})
	return err
}

// object finds one object with its content. A collection a report's hrefs
// name is fetched once with all of them; any other lookup asks for the one.
func (b *dav_backend) object(collection string, name string) (*dav_object, error) {
	if b.cache == nil {
		b.cache = map[string]map[string]*dav_object{}
	}
	objects, cached := b.cache[collection]
	if !cached || (objects[name] == nil && !slices.Contains(b.prefetch[collection], name)) {
		names := b.prefetch[collection]
		if !cached && len(names) == 0 || cached {
			names = []string{name}
		}
		list, err := b.objects(collection, names, nil, true)
		if err != nil {
			if errors.Is(err, dav_not_found) && !cached {
				b.cache[collection] = map[string]*dav_object{}
			}
			return nil, err
		}
		if !cached {
			objects = map[string]*dav_object{}
			b.cache[collection] = objects
		}
		for i := range list {
			objects[list[i].name] = &list[i]
		}
	}
	o := objects[name]
	if o == nil {
		return nil, dav_not_found
	}
	if err := b.retain(o.size); err != nil {
		return nil, err
	}
	return o, nil
}

// === CardDAV ===

func (b *dav_backend) AddressBookHomeSetPath(ctx context.Context) (string, error) {
	return b.home_path(), nil
}

func (b *dav_backend) address_book(c *dav_collection) carddav.AddressBook {
	return carddav.AddressBook{
		Path:            b.collection_path(c.slug),
		Name:            c.name,
		Description:     c.description,
		MaxResourceSize: dav_body_maximum,
		SupportedAddressData: []carddav.AddressDataType{
			{ContentType: vcard.MIMEType, Version: "3.0"},
			{ContentType: vcard.MIMEType, Version: "4.0"},
		},
	}
}

func (b *dav_backend) ListAddressBooks(ctx context.Context) ([]carddav.AddressBook, error) {
	list, err := b.collections("")
	if err != nil {
		return nil, err
	}
	out := make([]carddav.AddressBook, 0, len(list))
	for i := range list {
		out = append(out, b.address_book(&list[i]))
	}
	return out, nil
}

func (b *dav_backend) GetAddressBook(ctx context.Context, p string) (*carddav.AddressBook, error) {
	kind, slug, _, err := b.parse(p)
	if err != nil {
		return nil, err
	}
	if kind != dav_kind_collection {
		return nil, dav_not_found
	}
	c, err := b.collection(slug)
	if err != nil {
		return nil, err
	}
	ab := b.address_book(c)
	return &ab, nil
}

func (b *dav_backend) CreateAddressBook(ctx context.Context, ab *carddav.AddressBook) error {
	return b.create_collection(ab.Path, ab.Name, ab.Description)
}

func (b *dav_backend) DeleteAddressBook(ctx context.Context, p string) error {
	return b.delete_collection(p)
}

// address_object converts an object for the library. Without content (a
// listing that asked for none) the card stays empty; the library reads it only
// to answer address-data, which such a request did not name.
func (b *dav_backend) address_object(collection string, o *dav_object) (*carddav.AddressObject, error) {
	var card vcard.Card
	if o.card != nil {
		var err error
		card, err = dav_card_from_properties(o.card)
		if err != nil {
			info("DAV %s/%s: stored card unreadable: %v", collection, o.name, err)
			return nil, dav_error("")
		}
	}
	return &carddav.AddressObject{
		Path:    b.object_path(collection, o.name),
		ModTime: o.updated,
		ETag:    o.etag,
		Card:    card,
	}, nil
}

func (b *dav_backend) GetAddressObject(ctx context.Context, p string, req *carddav.AddressDataRequest) (*carddav.AddressObject, error) {
	kind, collection, name, err := b.parse(p)
	if err != nil {
		return nil, err
	}
	if kind != dav_kind_object {
		return nil, dav_not_found
	}
	o, err := b.object(collection, name)
	if err != nil {
		return nil, err
	}
	return b.address_object(collection, o)
}

func (b *dav_backend) ListAddressObjects(ctx context.Context, p string, req *carddav.AddressDataRequest) ([]carddav.AddressObject, error) {
	kind, collection, _, err := b.parse(p)
	if err != nil {
		return nil, err
	}
	if kind != dav_kind_collection {
		return nil, dav_not_found
	}
	list, err := b.objects(collection, nil, nil, b.data)
	if err != nil {
		return nil, err
	}
	out := make([]carddav.AddressObject, 0, len(list))
	for i := range list {
		if b.data {
			if err := b.retain(list[i].size); err != nil {
				return nil, err
			}
		}
		ao, err := b.address_object(collection, &list[i])
		if err != nil {
			return nil, err
		}
		out = append(out, *ao)
	}
	return out, nil
}

// QueryAddressObjects answers an addressbook-query by walking the collection
// a page at a time and keeping the matches: the library's matcher knows the
// vCard property semantics and the app holds nothing it could search faster.
// The match is case-insensitive, the collation the protocol defaults to, which
// the library's own comparison is not: it runs over lowered copies and the
// originals are what is served.
func (b *dav_backend) QueryAddressObjects(ctx context.Context, p string, query *carddav.AddressBookQuery) ([]carddav.AddressObject, error) {
	kind, collection, _, err := b.parse(p)
	if err != nil {
		return nil, err
	}
	if kind != dav_kind_collection {
		return nil, dav_not_found
	}
	lowered := *query
	lowered.PropFilters = make([]carddav.PropFilter, len(query.PropFilters))
	for i, pf := range query.PropFilters {
		lowered.PropFilters[i] = pf
		lowered.PropFilters[i].TextMatches = make([]carddav.TextMatch, len(pf.TextMatches))
		for j, tm := range pf.TextMatches {
			tm.Text = strings.ToLower(tm.Text)
			lowered.PropFilters[i].TextMatches[j] = tm
		}
	}
	lowered.Limit = 0
	limit := dav_matches_maximum
	if query.Limit > 0 && query.Limit < limit {
		limit = query.Limit
	}
	out := []carddav.AddressObject{}
	for offset := 0; ; offset += dav_page {
		list, err := b.objects(collection, nil, Map{"offset": offset, "limit": dav_page}, true)
		if err != nil {
			return nil, err
		}
		for i := range list {
			ao, err := b.address_object(collection, &list[i])
			if err != nil {
				return nil, err
			}
			matched, err := carddav.Match(&lowered, &carddav.AddressObject{Path: ao.Path, Card: dav_card_lowered(ao.Card)})
			if err != nil {
				return nil, webdav.NewHTTPError(http.StatusBadRequest, err)
			}
			if !matched {
				continue
			}
			if err := b.retain(list[i].size); err != nil {
				return nil, err
			}
			out = append(out, *ao)
			if len(out) >= limit {
				return out, nil
			}
		}
		if len(list) < dav_page {
			return out, nil
		}
	}
}

// dav_card_lowered copies a card with every value lowercased, for matching.
func dav_card_lowered(card vcard.Card) vcard.Card {
	out := vcard.Card{}
	for k, fields := range card {
		for _, f := range fields {
			if f == nil {
				continue
			}
			out[k] = append(out[k], &vcard.Field{Value: strings.ToLower(f.Value), Params: f.Params, Group: f.Group})
		}
	}
	return out
}

func (b *dav_backend) PutAddressObject(ctx context.Context, p string, card vcard.Card, opts *carddav.PutAddressObjectOptions) (*carddav.AddressObject, error) {
	kind, collection, name, err := b.parse(p)
	if err != nil {
		return nil, err
	}
	if kind != dav_kind_object {
		return nil, webdav.NewHTTPError(http.StatusForbidden, errors.New("objects live in a collection"))
	}
	c, err := b.collection(collection)
	if err != nil {
		return nil, err
	}
	if c.readonly {
		return nil, dav_error("readonly")
	}
	match, absent, err := dav_conditions(opts.IfMatch, opts.IfNoneMatch)
	if err != nil {
		return nil, err
	}
	etag, updated, err := b.put(Map{
		"collection": collection,
		"name":       name,
		"card":       dav_properties_from_card(card),
		"match":      match,
		"absent":     absent,
	})
	if err != nil {
		return nil, err
	}
	return &carddav.AddressObject{Path: b.object_path(collection, name), ETag: etag, ModTime: updated, Card: card}, nil
}

func (b *dav_backend) DeleteAddressObject(ctx context.Context, p string) error {
	return b.delete_object(p)
}

// === CalDAV ===

func (b *dav_backend) CalendarHomeSetPath(ctx context.Context) (string, error) {
	return b.home_path(), nil
}

func (b *dav_backend) calendar(c *dav_collection) caldav.Calendar {
	return caldav.Calendar{
		Path:                  b.collection_path(c.slug),
		Name:                  c.name,
		Description:           c.description,
		MaxResourceSize:       dav_body_maximum,
		SupportedComponentSet: c.components,
	}
}

func (b *dav_backend) ListCalendars(ctx context.Context) ([]caldav.Calendar, error) {
	list, err := b.collections("")
	if err != nil {
		return nil, err
	}
	out := make([]caldav.Calendar, 0, len(list))
	for i := range list {
		out = append(out, b.calendar(&list[i]))
	}
	return out, nil
}

func (b *dav_backend) GetCalendar(ctx context.Context, p string) (*caldav.Calendar, error) {
	kind, slug, _, err := b.parse(p)
	if err != nil {
		return nil, err
	}
	if kind != dav_kind_collection {
		return nil, dav_not_found
	}
	c, err := b.collection(slug)
	if err != nil {
		return nil, err
	}
	cal := b.calendar(c)
	return &cal, nil
}

func (b *dav_backend) CreateCalendar(ctx context.Context, cal *caldav.Calendar) error {
	return b.create_collection(cal.Path, cal.Name, cal.Description)
}

// calendar_object converts an object for the library; without content the
// calendar stays empty, as for cards.
func (b *dav_backend) calendar_object(collection string, o *dav_object) (*caldav.CalendarObject, error) {
	var cal *ical.Calendar
	if o.ics != "" {
		var err error
		cal, err = ical.NewDecoder(strings.NewReader(o.ics)).Decode()
		if err != nil {
			info("DAV %s/%s: stored calendar object unreadable: %v", collection, o.name, err)
			return nil, dav_error("")
		}
	}
	return &caldav.CalendarObject{
		Path:    b.object_path(collection, o.name),
		ModTime: o.updated,
		ETag:    o.etag,
		Data:    cal,
	}, nil
}

func (b *dav_backend) GetCalendarObject(ctx context.Context, p string, req *caldav.CalendarCompRequest) (*caldav.CalendarObject, error) {
	kind, collection, name, err := b.parse(p)
	if err != nil {
		return nil, err
	}
	if kind != dav_kind_object {
		return nil, dav_not_found
	}
	o, err := b.object(collection, name)
	if err != nil {
		return nil, err
	}
	return b.calendar_object(collection, o)
}

func (b *dav_backend) calendar_objects(p string, extra Map, data bool) ([]caldav.CalendarObject, error) {
	kind, collection, _, err := b.parse(p)
	if err != nil {
		return nil, err
	}
	if kind != dav_kind_collection {
		return nil, dav_not_found
	}
	list, err := b.objects(collection, nil, extra, data)
	if err != nil {
		return nil, err
	}
	out := make([]caldav.CalendarObject, 0, len(list))
	for i := range list {
		if data {
			if err := b.retain(list[i].size); err != nil {
				return nil, err
			}
		}
		co, err := b.calendar_object(collection, &list[i])
		if err != nil {
			return nil, err
		}
		out = append(out, *co)
	}
	return out, nil
}

func (b *dav_backend) ListCalendarObjects(ctx context.Context, p string, req *caldav.CalendarCompRequest) ([]caldav.CalendarObject, error) {
	return b.calendar_objects(p, nil, b.data)
}

// dav_query_range finds the time range a calendar-query asks about: the
// filter is VCALENDAR at the top, and the range sits on a component filter
// beneath it or on a property filter beneath that.
func dav_query_range(filter *caldav.CompFilter) (start time.Time, finish time.Time) {
	if !filter.Start.IsZero() || !filter.End.IsZero() {
		return filter.Start, filter.End
	}
	for i := range filter.Comps {
		if s, f := dav_query_range(&filter.Comps[i]); !s.IsZero() || !f.IsZero() {
			return s, f
		}
	}
	for i := range filter.Props {
		if !filter.Props[i].Start.IsZero() || !filter.Props[i].End.IsZero() {
			return filter.Props[i].Start, filter.Props[i].End
		}
	}
	return time.Time{}, time.Time{}
}

// QueryCalendarObjects asks the app for the candidates in the query's time
// range, a superset it answers from its shadow columns, then applies the
// query here, which is where recurrences expand.
func (b *dav_backend) QueryCalendarObjects(ctx context.Context, p string, query *caldav.CalendarQuery) ([]caldav.CalendarObject, error) {
	extra := Map{}
	if start, finish := dav_query_range(&query.CompFilter); !start.IsZero() || !finish.IsZero() {
		if !start.IsZero() {
			extra["start"] = start.Unix()
		}
		if !finish.IsZero() {
			extra["finish"] = finish.Unix()
		}
	}
	list, err := b.calendar_objects(p, extra, true)
	if err != nil {
		return nil, err
	}
	// The library expands every recurrence from its first occurrence up to
	// the query's end and holds the result, so a secondly rule from 1970
	// would take the server down. Objects whose expansion would outrun the
	// budget are left out of time-range answers; a client still lists and
	// fetches them by etag.
	if start, finish := dav_query_range(&query.CompFilter); !start.IsZero() || !finish.IsZero() {
		budget := ical_budget_query
		bounded := list[:0]
		for _, co := range list {
			if !ical_expansion_heavy(co.Data, finish, &budget) {
				bounded = append(bounded, co)
			}
		}
		list = bounded
	}
	return caldav.Filter(query, list)
}

func (b *dav_backend) PutCalendarObject(ctx context.Context, p string, cal *ical.Calendar, opts *caldav.PutCalendarObjectOptions) (*caldav.CalendarObject, error) {
	kind, collection, name, err := b.parse(p)
	if err != nil {
		return nil, err
	}
	if kind != dav_kind_object {
		return nil, webdav.NewHTTPError(http.StatusForbidden, errors.New("objects live in a collection"))
	}
	c, err := b.collection(collection)
	if err != nil {
		return nil, err
	}
	if c.readonly {
		return nil, dav_error("readonly")
	}
	match, absent, err := dav_conditions(opts.IfMatch, opts.IfNoneMatch)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	if err := ical.NewEncoder(&buf).Encode(cal); err != nil {
		return nil, webdav.NewHTTPError(http.StatusBadRequest, err)
	}
	args := Map{
		"collection": collection,
		"name":       name,
		"ics":        buf.String(),
		"match":      match,
		"absent":     absent,
	}
	summary := ical_summary(cal)
	if summary == nil {
		return nil, webdav.NewHTTPError(http.StatusBadRequest, errors.New("the object's start cannot be read"))
	}
	for k, v := range summary {
		args[k] = v
	}
	etag, updated, err := b.put(args)
	if err != nil {
		return nil, err
	}
	return &caldav.CalendarObject{Path: b.object_path(collection, name), ETag: etag, ModTime: updated, Data: cal}, nil
}

// DeleteCalendarObject also removes a calendar: the library routes every
// DELETE here, where the address book side has its own DeleteAddressBook.
func (b *dav_backend) DeleteCalendarObject(ctx context.Context, p string) error {
	if kind, _, _, err := b.parse(p); err == nil && kind == dav_kind_collection {
		return b.delete_collection(p)
	}
	return b.delete_object(p)
}

// === Cards ===

// dav_properties_from_card turns a parsed card into the property list apps
// store: [{name, params, value, group?}, ...] in property-name order, every
// parameter a list of strings. Lists, not tuples, so Starlark sees the shape
// the JSON in its column has.
func dav_properties_from_card(card vcard.Card) []any {
	keys := make([]string, 0, len(card))
	for k := range card {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]any, 0, len(keys))
	for _, k := range keys {
		for _, f := range card[k] {
			if f == nil {
				continue
			}
			params := map[string]any{}
			for pk, pv := range f.Params {
				values := make([]any, len(pv))
				for i, v := range pv {
					values[i] = v
				}
				params[pk] = values
			}
			p := map[string]any{"name": k, "params": params, "value": f.Value}
			if f.Group != "" {
				p["group"] = f.Group
			}
			out = append(out, p)
		}
	}
	return out
}

// dav_card_from_properties is the inverse: a property list to the card the
// library encodes. A card without VERSION is served as 3.0, and a 3.0 card
// without N gets an empty one, since that version requires it and a card
// built from a friend's name alone has none.
func dav_card_from_properties(list any) (vcard.Card, error) {
	items, ok := list.([]any)
	if !ok {
		return nil, errors.New("card is not a list")
	}
	card := vcard.Card{}
	for _, item := range items {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		name := strings.ToUpper(strings.TrimSpace(dav_string(m["name"])))
		if name == "" {
			continue
		}
		f := &vcard.Field{Value: dav_string(m["value"]), Params: vcard.Params{}, Group: dav_string(m["group"])}
		if params, ok := m["params"].(map[string]any); ok {
			for k, v := range params {
				key := strings.ToUpper(k)
				switch x := v.(type) {
				case []any:
					for _, e := range x {
						f.Params[key] = append(f.Params[key], dav_string(e))
					}
				case string:
					f.Params[key] = append(f.Params[key], x)
				}
			}
		}
		card[name] = append(card[name], f)
	}
	if card.Get(vcard.FieldVersion) == nil {
		card.SetValue(vcard.FieldVersion, "3.0")
	}
	if card.Value(vcard.FieldVersion) == "3.0" && card.Get(vcard.FieldName) == nil {
		card.SetValue(vcard.FieldName, ";;;;")
	}
	return card, nil
}

// dav_card_text encodes a property list as vCard text, for mochi.vcard.format.
func dav_card_text(list any) (string, error) {
	card, err := dav_card_from_properties(list)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	if err := vcard.NewEncoder(&buf).Encode(card); err != nil {
		return "", fmt.Errorf("vcard: %v", err)
	}
	return buf.String(), nil
}

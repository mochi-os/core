// Mochi server: CalDAV client for apps
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"golang.org/x/oauth2"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

// An app syncs a calendar it holds with one on another CalDAV server through a
// connected account: a caldav or apple account carries a server address and a
// password, a google account carries the refresh token its grant left, and
// core makes every request itself so the app never sees a secret. The functions
// answer dicts, never raise: a failure is {"error": code}, with the codes
// unauthorised (the credential was refused), conflict (a precondition failed,
// so the object changed elsewhere), missing (no such object), large (an object
// past the size cap), transport (the server could not be reached) and status
// (an answer this client does not understand, with the status beside it).
//
// Every href the functions return is absolute, since a server may name its
// collections on another host than the one the account addresses (iCloud
// does), and a request to one of them re-sends the credential to that host.
var api_caldav = sls.FromStringDict(sl.String("mochi.caldav"), sl.StringDict{
	"calendars": sl.NewBuiltin("mochi.caldav.calendars", api_caldav_calendars),
	"delete":    sl.NewBuiltin("mochi.caldav.delete", api_caldav_delete),
	"get":       sl.NewBuiltin("mochi.caldav.get", api_caldav_get),
	"list":      sl.NewBuiltin("mochi.caldav.list", api_caldav_list),
	"put":       sl.NewBuiltin("mochi.caldav.put", api_caldav_put),
	"status":    sl.NewBuiltin("mochi.caldav.status", api_caldav_status),
})

// dav_client_body_maximum caps a listing or multiget answer; an object of
// its own is capped at dav_client_object_maximum, the app's own ceiling.
const dav_client_body_maximum = 16 << 20
const dav_client_object_maximum = 1 << 20

// dav_client_multiget_maximum bounds the objects one multiget asks for.
const dav_client_multiget_maximum = 100

// dav_client_timeout bounds one request; a listing of a large calendar is the
// slowest, and the app fetches objects in batches beneath it.
const dav_client_timeout = 60 * time.Second

// dav_client_hops bounds the redirects one request follows, each re-sending
// the credential, since Go's client drops it on a change of host.
const dav_client_hops = 5

const dav_client_google_root = "https://apidata.googleusercontent.com/caldav/v2/"
const dav_client_apple_root = "https://caldav.icloud.com/"

// dav_client is one account's view of a CalDAV server.
type dav_client struct {
	base     *url.URL
	http     *http.Client
	username string
	password string
	// The principal as the provider documents it, for a server whose root
	// and well-known path name none: Google's is <root><email>/user.
	principal string
}

// dav_client_error is a failure named by one of the codes above.
type dav_client_error struct {
	code   string
	status int
}

func (e *dav_client_error) Error() string {
	if e.status != 0 {
		return fmt.Sprintf("%s (%d)", e.code, e.status)
	}
	return e.code
}

func dav_client_fail(code string, status int) error {
	return &dav_client_error{code: code, status: status}
}

// dav_client_result turns an error into the dict an app reads.
func dav_client_result(err error) map[string]any {
	var failure *dav_client_error
	if errors.As(err, &failure) {
		out := map[string]any{"error": failure.code}
		if failure.status != 0 {
			out["status"] = failure.status
		}
		return out
	}
	return map[string]any{"error": "transport"}
}

// dav_client_for opens the client an account row describes. The row carries
// its data column; the caller has checked the account belongs to the user.
func dav_client_for(user *User, row map[string]any) (*dav_client, error) {
	ptype, _ := row["type"].(string)
	raw, _ := row["data"].(string)
	var data map[string]any
	if raw != "" {
		json.Unmarshal([]byte(raw), &data)
	}
	c := &dav_client{}
	address := ""
	switch ptype {
	case "google":
		if !oauth_account_granted(user, row, "calendar") {
			return nil, dav_client_fail("unauthorised", 0)
		}
		client, err := account_oauth_client(user, row)
		if err != nil {
			return nil, dav_client_fail("unauthorised", 0)
		}
		c.http = client
		address = dav_client_google_root
		c.principal = dav_client_google_principal(row)
	case "apple", "caldav":
		address, _ = data["url"].(string)
		if ptype == "apple" {
			address = dav_client_apple_root
		}
		c.username, _ = data["username"].(string)
		c.password, _ = data["password"].(string)
		c.http = &http.Client{Timeout: dav_client_timeout, Transport: url_transport}
	default:
		return nil, dav_client_fail("unauthorised", 0)
	}
	base, err := url.Parse(address)
	if err != nil || (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" {
		return nil, dav_client_fail("transport", 0)
	}
	if base.Path == "" {
		base.Path = "/"
	}
	c.base = base
	// Redirects are followed by hand below, so the credential reaches the
	// host redirected to.
	c.http.CheckRedirect = func(req *http.Request, via []*http.Request) error { return http.ErrUseLastResponse }
	return c, nil
}

// dav_client_resolve makes an href absolute against the URL it came from.
func dav_client_resolve(from *url.URL, href string) string {
	ref, err := url.Parse(strings.TrimSpace(href))
	if err != nil {
		return ""
	}
	return from.ResolveReference(ref).String()
}

// dav_client_same reports whether two hrefs name one resource, ignoring a trailing slash.
func dav_client_same(a string, b string) bool {
	return strings.TrimRight(a, "/") == strings.TrimRight(b, "/")
}

// request issues one request, following redirects with the credential and
// reading the answer up to limit bytes. A transport failure is reported as
// such; any status comes back for the caller to judge.
func (c *dav_client) request(ctx context.Context, method string, target string, headers map[string]string, body string, limit int64) (int, http.Header, []byte, *url.URL, error) {
	current, err := url.Parse(target)
	if err != nil || (current.Scheme != "http" && current.Scheme != "https") {
		return 0, nil, nil, nil, dav_client_fail("transport", 0)
	}
	for hop := 0; ; hop++ {
		r, err := http.NewRequestWithContext(ctx, method, current.String(), strings.NewReader(body))
		if err != nil {
			return 0, nil, nil, nil, dav_client_fail("transport", 0)
		}
		for k, v := range headers {
			r.Header.Set(k, v)
		}
		if c.username != "" || c.password != "" {
			r.SetBasicAuth(c.username, c.password)
		}
		r.Header.Set("User-Agent", "Mochi")
		response, err := c.http.Do(r)
		if err != nil {
			return 0, nil, nil, nil, dav_client_fail("transport", 0)
		}
		if response.StatusCode >= 300 && response.StatusCode < 400 && response.Header.Get("Location") != "" && hop < dav_client_hops {
			next, err := current.Parse(response.Header.Get("Location"))
			io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
			response.Body.Close()
			if err != nil {
				return 0, nil, nil, nil, dav_client_fail("transport", 0)
			}
			current = next
			continue
		}
		data, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
		response.Body.Close()
		if err != nil {
			return 0, nil, nil, nil, dav_client_fail("transport", 0)
		}
		if int64(len(data)) > limit {
			return 0, nil, nil, nil, dav_client_fail("large", 0)
		}
		return response.StatusCode, response.Header, data, current, nil
	}
}

// dav_client_multistatus is the answer to a PROPFIND or REPORT.
type dav_client_multistatus struct {
	XMLName   xml.Name              `xml:"DAV: multistatus"`
	Responses []dav_client_response `xml:"DAV: response"`
	Token     string                `xml:"DAV: sync-token"`
}

type dav_client_response struct {
	Href      string                `xml:"DAV: href"`
	Status    string                `xml:"DAV: status"`
	Propstats []dav_client_propstat `xml:"DAV: propstat"`
}

type dav_client_propstat struct {
	Status string          `xml:"DAV: status"`
	Prop   dav_client_prop `xml:"DAV: prop"`
}

type dav_client_prop struct {
	Etag        string                `xml:"DAV: getetag"`
	Ctag        string                `xml:"http://calendarserver.org/ns/ getctag"`
	Token       string                `xml:"DAV: sync-token"`
	Name        string                `xml:"DAV: displayname"`
	Description string                `xml:"urn:ietf:params:xml:ns:caldav calendar-description"`
	Colour      string                `xml:"http://apple.com/ns/ical/ calendar-color"`
	Data        string                `xml:"urn:ietf:params:xml:ns:caldav calendar-data"`
	Type        dav_client_type       `xml:"DAV: resourcetype"`
	Principal   dav_client_hrefs      `xml:"DAV: current-user-principal"`
	Home        dav_client_hrefs      `xml:"urn:ietf:params:xml:ns:caldav calendar-home-set"`
	Components  dav_client_components `xml:"urn:ietf:params:xml:ns:caldav supported-calendar-component-set"`
	Privileges  dav_client_privileges `xml:"DAV: current-user-privilege-set"`
}

type dav_client_type struct {
	Calendar   *struct{} `xml:"urn:ietf:params:xml:ns:caldav calendar"`
	Collection *struct{} `xml:"DAV: collection"`
}

type dav_client_hrefs struct {
	Hrefs []string `xml:"DAV: href"`
}

type dav_client_components struct {
	Components []struct {
		Name string `xml:"name,attr"`
	} `xml:"urn:ietf:params:xml:ns:caldav comp"`
}

type dav_client_privileges struct {
	Privileges []dav_client_privilege `xml:"DAV: privilege"`
}

type dav_client_privilege struct {
	All     *struct{} `xml:"DAV: all"`
	Write   *struct{} `xml:"DAV: write"`
	Content *struct{} `xml:"DAV: write-content"`
	Bind    *struct{} `xml:"DAV: bind"`
}

// found is the property set the response reports as 200.
func (r *dav_client_response) found() *dav_client_prop {
	for i := range r.Propstats {
		if strings.Contains(r.Propstats[i].Status, " 200") || r.Propstats[i].Status == "" {
			return &r.Propstats[i].Prop
		}
	}
	return nil
}

// gone reports a response naming a resource that no longer exists.
func (r *dav_client_response) gone() bool {
	return strings.Contains(r.Status, " 404")
}

const dav_client_namespaces = `xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav" xmlns:CS="http://calendarserver.org/ns/" xmlns:A="http://apple.com/ns/ical/"`

// propfind asks for props at the depth and parses the multistatus.
func (c *dav_client) propfind(ctx context.Context, target string, depth string, props string) (*dav_client_multistatus, *url.URL, error) {
	body := `<?xml version="1.0" encoding="utf-8"?><D:propfind ` + dav_client_namespaces + `><D:prop>` + props + `</D:prop></D:propfind>`
	return c.query(ctx, "PROPFIND", target, depth, body)
}

func (c *dav_client) query(ctx context.Context, method string, target string, depth string, body string) (*dav_client_multistatus, *url.URL, error) {
	headers := map[string]string{"Content-Type": "application/xml; charset=utf-8", "Depth": depth}
	status, _, data, from, err := c.request(ctx, method, target, headers, body, dav_client_body_maximum)
	if err != nil {
		return nil, nil, err
	}
	if err := dav_client_status(status); err != nil {
		return nil, nil, err
	}
	if status != http.StatusMultiStatus {
		return nil, nil, dav_client_fail("status", status)
	}
	var out dav_client_multistatus
	if err := xml.Unmarshal(data, &out); err != nil {
		return nil, nil, dav_client_fail("status", status)
	}
	return &out, from, nil
}

// dav_client_status maps the statuses every operation reads alike.
func dav_client_status(status int) error {
	switch {
	case status == http.StatusUnauthorized, status == http.StatusForbidden:
		return dav_client_fail("unauthorised", status)
	case status == http.StatusNotFound, status == http.StatusGone:
		return dav_client_fail("missing", status)
	case status == http.StatusPreconditionFailed:
		return dav_client_fail("conflict", status)
	case status == http.StatusRequestEntityTooLarge, status == http.StatusInsufficientStorage:
		return dav_client_fail("large", status)
	case status >= 500:
		return dav_client_fail("status", status)
	}
	return nil
}

// dav_client_calendar is one collection a server offers.
type dav_client_calendar struct {
	Href        string `json:"href"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Colour      string `json:"colour"`
	Readonly    bool   `json:"readonly"`
}

// dav_client_calendar_of reads a collection's own answer from a listing.
func dav_client_calendar_of(from *url.URL, r *dav_client_response) *dav_client_calendar {
	prop := r.found()
	if prop == nil || prop.Type.Calendar == nil {
		return nil
	}
	if len(prop.Components.Components) > 0 {
		events := false
		for _, comp := range prop.Components.Components {
			if strings.EqualFold(comp.Name, "VEVENT") {
				events = true
			}
		}
		if !events {
			return nil
		}
	}
	href := dav_client_resolve(from, r.Href)
	if href == "" {
		return nil
	}
	if !strings.HasSuffix(href, "/") {
		href += "/"
	}
	readonly := false
	if len(prop.Privileges.Privileges) > 0 {
		readonly = true
		for _, p := range prop.Privileges.Privileges {
			if p.All != nil || p.Write != nil || p.Content != nil || p.Bind != nil {
				readonly = false
			}
		}
	}
	name := strings.TrimSpace(prop.Name)
	if name == "" {
		name = path.Base(strings.TrimRight(href, "/"))
	}
	colour := strings.TrimSpace(prop.Colour)
	if len(colour) == 9 && colour[0] == '#' {
		// Apple writes #RRGGBBAA; the app's colours are #RRGGBB.
		colour = colour[:7]
	}
	return &dav_client_calendar{Href: href, Name: name, Description: strings.TrimSpace(prop.Description), Colour: strings.ToLower(colour), Readonly: readonly}
}

const dav_client_calendar_props = `<D:resourcetype/><D:displayname/><C:calendar-description/><A:calendar-color/><C:supported-calendar-component-set/><D:current-user-privilege-set/>`

// dav_client_google_principal is the principal Google documents for an
// account, <root><email>/user, from the email the grant recorded as the
// account's label; "" when the label is no address.
func dav_client_google_principal(row map[string]any) string {
	email, _ := row["label"].(string)
	if !strings.Contains(email, "@") {
		return ""
	}
	return dav_client_google_root + url.PathEscape(email) + "/user"
}

// dav_client_refused reports a failure that no other path would get past: the
// credential refused, or the server out of reach. Any other answer to one
// address leaves the next address worth trying.
func dav_client_refused(err error) bool {
	var failure *dav_client_error
	if errors.As(err, &failure) {
		return failure.code == "unauthorised" || failure.code == "transport"
	}
	return true
}

// calendars discovers the collections the account may sync: the address's own
// collection when it names one; else the principal it names, the one the
// well-known path leads to, or the one the provider documents, and the home
// set beneath the principal.
func (c *dav_client) calendars(ctx context.Context) ([]dav_client_calendar, error) {
	principal := ""
	own, from, err := c.propfind(ctx, c.base.String(), "0", dav_client_calendar_props+`<D:current-user-principal/>`)
	if err != nil && dav_client_refused(err) {
		return nil, err
	}
	if err == nil {
		for i := range own.Responses {
			if cal := dav_client_calendar_of(from, &own.Responses[i]); cal != nil {
				return []dav_client_calendar{*cal}, nil
			}
			if prop := own.Responses[i].found(); prop != nil && len(prop.Principal.Hrefs) > 0 {
				principal = dav_client_resolve(from, prop.Principal.Hrefs[0])
			}
		}
	}
	if principal == "" {
		known := &url.URL{Scheme: c.base.Scheme, Host: c.base.Host, Path: "/.well-known/caldav"}
		answer, at, err := c.propfind(ctx, known.String(), "0", `<D:current-user-principal/>`)
		if err != nil && dav_client_refused(err) {
			return nil, err
		}
		if err == nil {
			for i := range answer.Responses {
				if prop := answer.Responses[i].found(); prop != nil && len(prop.Principal.Hrefs) > 0 {
					principal = dav_client_resolve(at, prop.Principal.Hrefs[0])
				}
			}
		}
	}
	if principal == "" {
		principal = c.principal
	}
	if principal == "" {
		return nil, dav_client_fail("status", 0)
	}
	answer, at, err := c.propfind(ctx, principal, "0", `<C:calendar-home-set/>`)
	if err != nil {
		return nil, err
	}
	home := ""
	for i := range answer.Responses {
		if prop := answer.Responses[i].found(); prop != nil && len(prop.Home.Hrefs) > 0 {
			home = dav_client_resolve(at, prop.Home.Hrefs[0])
		}
	}
	if home == "" {
		return nil, dav_client_fail("status", 0)
	}
	listing, at, err := c.propfind(ctx, home, "1", dav_client_calendar_props)
	if err != nil {
		return nil, err
	}
	out := []dav_client_calendar{}
	for i := range listing.Responses {
		if cal := dav_client_calendar_of(at, &listing.Responses[i]); cal != nil {
			out = append(out, *cal)
		}
	}
	return out, nil
}

// status reads the collection's change tokens: the ctag, which moves on any
// change within, and the sync token where the server keeps one.
func (c *dav_client) status(ctx context.Context, collection string) (string, string, error) {
	answer, _, err := c.propfind(ctx, collection, "0", `<CS:getctag/><D:sync-token/><D:resourcetype/>`)
	if err != nil {
		return "", "", err
	}
	for i := range answer.Responses {
		if prop := answer.Responses[i].found(); prop != nil {
			return strings.TrimSpace(prop.Ctag), strings.TrimSpace(prop.Token), nil
		}
	}
	return "", "", dav_client_fail("status", 0)
}

// dav_client_object is one object's name and version, with its text when read.
type dav_client_object struct {
	Href string `json:"href"`
	Etag string `json:"etag"`
	Ics  string `json:"ics,omitempty"`
}

// list names every object in the collection with its etag.
func (c *dav_client) list(ctx context.Context, collection string) ([]dav_client_object, error) {
	answer, from, err := c.propfind(ctx, collection, "1", `<D:getetag/><D:resourcetype/>`)
	if err != nil {
		return nil, err
	}
	out := []dav_client_object{}
	for i := range answer.Responses {
		r := &answer.Responses[i]
		href := dav_client_resolve(from, r.Href)
		if href == "" || dav_client_same(href, from.String()) || dav_client_same(href, collection) {
			continue
		}
		prop := r.found()
		if prop == nil || prop.Type.Collection != nil || prop.Type.Calendar != nil {
			continue
		}
		out = append(out, dav_client_object{Href: href, Etag: strings.TrimSpace(prop.Etag)})
	}
	return out, nil
}

// get reads the named objects of the collection in one multiget. An object
// past the size cap comes back without text and with an error of its own; one
// the server no longer holds is left out.
func (c *dav_client) get(ctx context.Context, collection string, hrefs []string) ([]map[string]any, error) {
	if len(hrefs) > dav_client_multiget_maximum {
		hrefs = hrefs[:dav_client_multiget_maximum]
	}
	var body bytes.Buffer
	body.WriteString(`<?xml version="1.0" encoding="utf-8"?><C:calendar-multiget ` + dav_client_namespaces + `><D:prop><D:getetag/><C:calendar-data/></D:prop>`)
	for _, href := range hrefs {
		u, err := url.Parse(href)
		if err != nil {
			continue
		}
		body.WriteString("<D:href>")
		xml.EscapeText(&body, []byte(u.EscapedPath()))
		body.WriteString("</D:href>")
	}
	body.WriteString(`</C:calendar-multiget>`)
	answer, from, err := c.query(ctx, "REPORT", collection, "1", body.String())
	if err != nil {
		return nil, err
	}
	out := []map[string]any{}
	for i := range answer.Responses {
		r := &answer.Responses[i]
		href := dav_client_resolve(from, r.Href)
		if href == "" || r.gone() {
			continue
		}
		prop := r.found()
		if prop == nil {
			continue
		}
		if len(prop.Data) > dav_client_object_maximum {
			out = append(out, map[string]any{"href": href, "etag": strings.TrimSpace(prop.Etag), "error": "large"})
			continue
		}
		out = append(out, map[string]any{"href": href, "etag": strings.TrimSpace(prop.Etag), "ics": prop.Data})
	}
	return out, nil
}

// etag reads one object's etag, for a server whose PUT answers without one.
func (c *dav_client) etag(ctx context.Context, href string) (string, error) {
	answer, _, err := c.propfind(ctx, href, "0", `<D:getetag/>`)
	if err != nil {
		return "", err
	}
	for i := range answer.Responses {
		if prop := answer.Responses[i].found(); prop != nil {
			return strings.TrimSpace(prop.Etag), nil
		}
	}
	return "", nil
}

// put writes an object: over the version named by etag, or as a new object
// when etag is empty, which fails on conflict should one exist already.
func (c *dav_client) put(ctx context.Context, href string, ics string, etag string) (string, error) {
	if len(ics) > dav_client_object_maximum {
		return "", dav_client_fail("large", 0)
	}
	headers := map[string]string{"Content-Type": "text/calendar; charset=utf-8"}
	if etag != "" {
		headers["If-Match"] = etag
	} else {
		headers["If-None-Match"] = "*"
	}
	status, header, _, _, err := c.request(ctx, "PUT", href, headers, ics, dav_client_body_maximum)
	if err != nil {
		return "", err
	}
	if err := dav_client_status(status); err != nil {
		return "", err
	}
	if status < 200 || status >= 300 {
		return "", dav_client_fail("status", status)
	}
	if got := strings.TrimSpace(header.Get("ETag")); got != "" {
		return got, nil
	}
	return c.etag(ctx, href)
}

// delete removes an object, the version named by etag when one is given. An
// object already gone counts as deleted.
func (c *dav_client) delete(ctx context.Context, href string, etag string) error {
	headers := map[string]string{}
	if etag != "" {
		headers["If-Match"] = etag
	}
	status, _, _, _, err := c.request(ctx, "DELETE", href, headers, "", 65536)
	if err != nil {
		return err
	}
	if status == http.StatusNotFound || status == http.StatusGone {
		return nil
	}
	if err := dav_client_status(status); err != nil {
		return err
	}
	if status < 200 || status >= 300 {
		return dav_client_fail("status", status)
	}
	return nil
}

// === OAuth-backed accounts ===

// oauth_grants names the provider scopes each capability of an OAuth account
// needs. A capability absent here can only be the sign-in the link gives.
var oauth_grants = map[string]map[string][]string{
	"google": {"calendar": {"https://www.googleapis.com/auth/calendar"}},
}

// oauth_account_scopes reads the scopes an account's grants have gathered.
func oauth_account_scopes(row map[string]any) []string {
	raw, _ := row["data"].(string)
	var data struct {
		Scopes []string `json:"scopes"`
	}
	if raw != "" {
		json.Unmarshal([]byte(raw), &data)
	}
	return data.Scopes
}

// oauth_account_granted reports whether the account's grants cover the
// capability's scopes.
func oauth_account_granted(user *User, row map[string]any, capability string) bool {
	ptype, _ := row["type"].(string)
	needed := oauth_grants[ptype][capability]
	if len(needed) == 0 {
		return false
	}
	held := oauth_account_scopes(row)
	for _, scope := range needed {
		found := false
		for _, h := range held {
			if h == scope {
				found = true
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// oauth_sources keeps one refreshing token source per account and refresh
// token, so a sync does not exchange the refresh token on every request.
var oauth_sources = struct {
	sync.Mutex
	sources map[string]oauth2.TokenSource
}{sources: map[string]oauth2.TokenSource{}}

// account_oauth_client returns an HTTP client that sends the account's bearer
// token, refreshing it from the stored refresh token as it expires.
func account_oauth_client(user *User, row map[string]any) (*http.Client, error) {
	ptype, _ := row["type"].(string)
	provider, ok := oauth_providers()[ptype]
	if !ok || !oauth_enabled(ptype) {
		return nil, errors.New("provider not enabled")
	}
	raw, _ := row["data"].(string)
	var data struct {
		Refresh string `json:"refresh"`
	}
	if raw != "" {
		json.Unmarshal([]byte(raw), &data)
	}
	if data.Refresh == "" {
		return nil, errors.New("no grant")
	}
	cfg, _, err := oauth_client_config(provider, "")
	if err != nil {
		return nil, err
	}
	id, _ := row["id"].(string)
	key := id + ":" + data.Refresh
	oauth_sources.Lock()
	source := oauth_sources.sources[key]
	if source == nil {
		if len(oauth_sources.sources) > 1000 {
			oauth_sources.sources = map[string]oauth2.TokenSource{}
		}
		ctx := context.WithValue(context.Background(), oauth2.HTTPClient, oauth_http_client)
		source = oauth2.ReuseTokenSource(nil, cfg.TokenSource(ctx, &oauth2.Token{RefreshToken: data.Refresh, Expiry: time.Now().Add(-time.Hour)}))
		oauth_sources.sources[key] = source
	}
	oauth_sources.Unlock()
	return &http.Client{Timeout: dav_client_timeout, Transport: &oauth2.Transport{Source: source, Base: url_transport}}, nil
}

// oauth_sources_forget drops the cached source of an account whose grant was
// removed or replaced.
func oauth_sources_forget(id string) {
	oauth_sources.Lock()
	for key := range oauth_sources.sources {
		if strings.HasPrefix(key, id+":") {
			delete(oauth_sources.sources, key)
		}
	}
	oauth_sources.Unlock()
}

// === Starlark ===

// caldav_account resolves the account an app names to a client, checking the
// permission, the caller and that the account can hold a calendar.
func caldav_account(t *sl.Thread, fn *sl.Builtin, args sl.Tuple) (*dav_client, sl.Value, error) {
	if err := require_permission(t, fn, "accounts/calendar"); err != nil {
		return nil, nil, fmt.Errorf("%v", err)
	}
	user := principal_caller(t)
	if user == nil {
		return nil, nil, errors.New("no user")
	}
	if len(args) < 1 {
		return nil, nil, errors.New("syntax: <account: string>, ...")
	}
	id, ok := account_id_arg(args[0])
	if !ok {
		return nil, nil, errors.New("invalid account")
	}
	row, err := db_user(user, "user").row("select id, type, label, identifier, data from accounts where id=?", id)
	if err != nil {
		return nil, nil, fmt.Errorf("database error: %v", err)
	}
	if row == nil {
		return nil, sl_encode(map[string]any{"error": "missing"}), nil
	}
	if ptype, _ := row["type"].(string); !provider_has_capability(ptype, "calendar") {
		return nil, sl_encode(map[string]any{"error": "unauthorised"}), nil
	}
	client, err := dav_client_for(user, row)
	if err != nil {
		return nil, sl_encode(dav_client_result(err)), nil
	}
	return client, nil, nil
}

// caldav_context bounds a call by the action's own context when there is one.
func caldav_context(t *sl.Thread) context.Context {
	if ctx, ok := t.Local("context").(context.Context); ok && ctx != nil {
		return ctx
	}
	return context.Background()
}

// mochi.caldav.calendars(account) -> dict: The calendars the account's server offers, as {calendars: [{href, name, description, colour, readonly}]}, or {error}
func api_caldav_calendars(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	client, answer, err := caldav_account(t, fn, args)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	if answer != nil {
		return answer, nil
	}
	list, err := client.calendars(caldav_context(t))
	if err != nil {
		return sl_encode(dav_client_result(err)), nil
	}
	out := make([]map[string]any, 0, len(list))
	for _, cal := range list {
		out = append(out, map[string]any{"href": cal.Href, "name": cal.Name, "description": cal.Description, "colour": cal.Colour, "readonly": cal.Readonly})
	}
	return sl_encode(map[string]any{"calendars": out}), nil
}

// mochi.caldav.status(account, collection) -> dict: The collection's change tokens as {ctag, token}, or {error}
func api_caldav_status(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	client, answer, err := caldav_account(t, fn, args)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	if answer != nil {
		return answer, nil
	}
	var account, collection string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "account", &account, "collection", &collection); err != nil {
		return sl_error(fn, "%v", err)
	}
	ctag, token, err := client.status(caldav_context(t), collection)
	if err != nil {
		return sl_encode(dav_client_result(err)), nil
	}
	return sl_encode(map[string]any{"ctag": ctag, "token": token}), nil
}

// mochi.caldav.list(account, collection) -> dict: Every object in the collection as {objects: [{href, etag}]}, or {error}
func api_caldav_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	client, answer, err := caldav_account(t, fn, args)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	if answer != nil {
		return answer, nil
	}
	var account, collection string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "account", &account, "collection", &collection); err != nil {
		return sl_error(fn, "%v", err)
	}
	list, err := client.list(caldav_context(t), collection)
	if err != nil {
		return sl_encode(dav_client_result(err)), nil
	}
	out := make([]map[string]any, 0, len(list))
	for _, o := range list {
		out = append(out, map[string]any{"href": o.Href, "etag": o.Etag})
	}
	return sl_encode(map[string]any{"objects": out}), nil
}

// mochi.caldav.get(account, collection, hrefs) -> dict: The named objects with their text as {objects: [{href, etag, ics}]}, or {error}
func api_caldav_get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	client, answer, err := caldav_account(t, fn, args)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	if answer != nil {
		return answer, nil
	}
	var account, collection string
	var wanted *sl.List
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "account", &account, "collection", &collection, "hrefs", &wanted); err != nil {
		return sl_error(fn, "%v", err)
	}
	hrefs := []string{}
	for i := 0; i < wanted.Len(); i++ {
		if s, ok := sl.AsString(wanted.Index(i)); ok && s != "" {
			hrefs = append(hrefs, s)
		}
	}
	if len(hrefs) == 0 {
		return sl_encode(map[string]any{"objects": []map[string]any{}}), nil
	}
	objects, err := client.get(caldav_context(t), collection, hrefs)
	if err != nil {
		return sl_encode(dav_client_result(err)), nil
	}
	return sl_encode(map[string]any{"objects": objects}), nil
}

// mochi.caldav.put(account, href, ics, etag="") -> dict: Write an object, over the version etag names or as a new one, answering {etag}, or {error}
func api_caldav_put(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	client, answer, err := caldav_account(t, fn, args)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	if answer != nil {
		return answer, nil
	}
	var account, href, ics, etag string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "account", &account, "href", &href, "ics", &ics, "etag?", &etag); err != nil {
		return sl_error(fn, "%v", err)
	}
	got, err := client.put(caldav_context(t), href, ics, etag)
	if err != nil {
		return sl_encode(dav_client_result(err)), nil
	}
	return sl_encode(map[string]any{"etag": got}), nil
}

// mochi.caldav.delete(account, href, etag="") -> dict: Delete an object, the version etag names when given, answering {} or {error}
func api_caldav_delete(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	client, answer, err := caldav_account(t, fn, args)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	if answer != nil {
		return answer, nil
	}
	var account, href, etag string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "account", &account, "href", &href, "etag?", &etag); err != nil {
		return sl_error(fn, "%v", err)
	}
	if err := client.delete(caldav_context(t), href, etag); err != nil {
		return sl_encode(dav_client_result(err)), nil
	}
	return sl_encode(map[string]any{}), nil
}

// caldav_test probes an account's server for the settings page's Test button.
func caldav_test(user *User, row map[string]any, language string) AccountTestResult {
	client, err := dav_client_for(user, row)
	if err != nil {
		if ptype, _ := row["type"].(string); ptype == "google" {
			return AccountTestResult{Success: false, Message: resolve_core_label(language, "accounts.test.calendar_none", nil)}
		}
		return AccountTestResult{Success: false, Message: resolve_core_label(language, "accounts.test.connection_failed", nil)}
	}
	ctx, cancel := context.WithTimeout(context.Background(), dav_client_timeout)
	defer cancel()
	list, err := client.calendars(ctx)
	if err != nil {
		var failure *dav_client_error
		if errors.As(err, &failure) && failure.code == "unauthorised" {
			return AccountTestResult{Success: false, Message: resolve_core_label(language, "accounts.test.authentication_failed", nil)}
		}
		return account_failure(language, "accounts.test.connection_failed", "caldav", err)
	}
	return AccountTestResult{Success: true, Message: resolve_core_label(language, "accounts.test.calendar_connected", map[string]any{"count": len(list)})}
}

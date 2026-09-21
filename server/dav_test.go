// Mochi server: CardDAV and CalDAV tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/emersion/go-ical"
	"github.com/emersion/go-vcard"
	"github.com/emersion/go-webdav"
	"github.com/emersion/go-webdav/caldav"
	"github.com/emersion/go-webdav/carddav"
	"github.com/gin-gonic/gin"
)

// dav_fake_app answers the dav/* contract in memory the way the people app
// does: collections and objects addressed by slug, property lists for cards,
// iCalendar text for calendar objects, an etag per write, and the error codes
// dav_error maps. It is the contract's executable description.
type dav_fake_app struct {
	collections []map[string]any
	objects     map[string]map[string]map[string]any
	sequence    int
	calls       []string
	arguments   []Map
}

func new_dav_fake_app(slugs ...string) *dav_fake_app {
	f := &dav_fake_app{objects: map[string]map[string]map[string]any{}}
	for _, slug := range slugs {
		f.collections = append(f.collections, map[string]any{"slug": slug, "name": "Book " + slug, "description": "", "readonly": false})
		f.objects[slug] = map[string]map[string]any{}
	}
	return f
}

func (f *dav_fake_app) call(function string, args Map) (any, error) {
	f.calls = append(f.calls, function)
	f.arguments = append(f.arguments, args)
	slug, _ := args["collection"].(string)
	switch function {
	case "dav/collections":
		out := []any{}
		for _, c := range f.collections {
			if slug == "" || c["slug"] == slug {
				out = append(out, c)
			}
		}
		return out, nil
	case "dav/collections/create":
		for _, c := range f.collections {
			if c["slug"] == slug {
				return map[string]any{"error": "exists"}, nil
			}
		}
		f.collections = append(f.collections, map[string]any{"slug": slug, "name": args["name"], "description": args["description"]})
		f.objects[slug] = map[string]map[string]any{}
		return map[string]any{"slug": slug}, nil
	case "dav/collections/delete":
		if slug == "default" {
			return map[string]any{"error": "forbidden"}, nil
		}
		kept := f.collections[:0]
		found := false
		for _, c := range f.collections {
			if c["slug"] == slug {
				found = true
				continue
			}
			kept = append(kept, c)
		}
		if !found {
			return map[string]any{"error": "not_found"}, nil
		}
		f.collections = kept
		delete(f.objects, slug)
		return map[string]any{}, nil
	case "dav/objects":
		objects, ok := f.objects[slug]
		if !ok {
			return map[string]any{"error": "not_found"}, nil
		}
		wanted := map[string]bool{}
		names, _ := args["names"].([]string)
		for _, n := range names {
			wanted[n] = true
		}
		keys := make([]string, 0, len(objects))
		for k := range objects {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		if offset, ok := args["offset"].(int); ok {
			limit, _ := args["limit"].(int)
			keys = keys[min(offset, len(keys)):]
			keys = keys[:min(limit, len(keys))]
		}
		data, _ := args["data"].(bool)
		out := []any{}
		for _, k := range keys {
			if names != nil && !wanted[k] {
				continue
			}
			row := map[string]any{}
			for field, value := range objects[k] {
				if !data && (field == "card" || field == "ics") {
					continue
				}
				row[field] = value
			}
			out = append(out, row)
		}
		return out, nil
	case "dav/put":
		objects, ok := f.objects[slug]
		if !ok {
			return map[string]any{"error": "not_found"}, nil
		}
		name, _ := args["name"].(string)
		match, _ := args["match"].(string)
		absent, _ := args["absent"].(bool)
		existing := objects[name]
		if absent && existing != nil {
			return map[string]any{"error": "conflict"}, nil
		}
		if match == "*" && existing == nil {
			return map[string]any{"error": "conflict"}, nil
		}
		if match != "" && match != "*" && (existing == nil || existing["etag"] != match) {
			return map[string]any{"error": "conflict"}, nil
		}
		f.sequence++
		etag := fmt.Sprintf("etag-%d", f.sequence)
		row := map[string]any{"name": name, "etag": etag, "updated": int64(1700000000 + f.sequence)}
		if card, ok := args["card"]; ok {
			row["card"] = card
		}
		if ics, ok := args["ics"]; ok {
			row["ics"] = ics
			row["summary"] = args["summary"]
			row["start"] = args["start"]
			row["recurring"] = args["recurring"]
		}
		objects[name] = row
		return map[string]any{"name": name, "etag": etag, "updated": row["updated"]}, nil
	case "dav/delete":
		objects, ok := f.objects[slug]
		name, _ := args["name"].(string)
		if !ok || objects[name] == nil {
			return map[string]any{"error": "not_found"}, nil
		}
		match, _ := args["match"].(string)
		absent, _ := args["absent"].(bool)
		if absent || (match != "" && match != "*" && objects[name]["etag"] != match) {
			return map[string]any{"error": "conflict"}, nil
		}
		delete(objects, name)
		return map[string]any{}, nil
	}
	return nil, fmt.Errorf("unknown function %q", function)
}

// dav_test_server mounts the engine for one feature the way web_action does,
// with the fake app behind it and no authentication in the way.
func dav_test_server(t *testing.T, feature string, fake *dav_fake_app) *httptest.Server {
	t.Helper()
	prefix := "/people/" + feature
	// One backend per request, as dav_http_handler builds it: it carries the
	// request's preconditions, its call budget and its object cache.
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b := &dav_backend{feature: feature, prefix: prefix, principal: "fp1", call: fake.call,
			match: webdav.ConditionalMatch(r.Header.Get("If-Match")),
			none:  webdav.ConditionalMatch(r.Header.Get("If-None-Match"))}
		dav_body_settle(r)
		if status := b.survey(r); status != 0 {
			http.Error(w, http.StatusText(status), status)
			return
		}
		dav_mkcalendar(feature, r)
		if feature == "carddav" {
			(&carddav.Handler{Backend: b, Prefix: prefix}).ServeHTTP(w, r)
		} else {
			(&caldav.Handler{Backend: b, Prefix: prefix}).ServeHTTP(w, r)
		}
	})
	server := httptest.NewServer(h)
	t.Cleanup(server.Close)
	return server
}

func dav_raw(t *testing.T, server *httptest.Server, method string, target string, body string, headers map[string]string) *http.Response {
	t.Helper()
	request, err := http.NewRequest(method, server.URL+target, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range headers {
		request.Header.Set(k, v)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { response.Body.Close() })
	return response
}

func dav_test_card(name string, email string) vcard.Card {
	card := vcard.Card{}
	card.SetValue(vcard.FieldVersion, "3.0")
	card.SetValue(vcard.FieldFormattedName, name)
	card.Add(vcard.FieldEmail, &vcard.Field{Value: email, Params: vcard.Params{vcard.ParamType: {"home"}}})
	return card
}

func TestDavCardDavDiscoveryAndObjects(t *testing.T) {
	fake := new_dav_fake_app("default")
	server := dav_test_server(t, "carddav", fake)
	ctx := context.Background()

	client, err := carddav.NewClient(nil, server.URL+"/people/carddav/")
	if err != nil {
		t.Fatal(err)
	}
	principal, err := client.FindCurrentUserPrincipal(ctx)
	if err != nil {
		t.Fatalf("principal: %v", err)
	}
	if principal != "/people/carddav/fp1/" {
		t.Fatalf("principal = %q", principal)
	}
	home, err := client.FindAddressBookHomeSet(ctx, principal)
	if err != nil {
		t.Fatalf("home set: %v", err)
	}
	if home != "/people/carddav/fp1/books/" {
		t.Fatalf("home set = %q", home)
	}
	books, err := client.FindAddressBooks(ctx, home)
	if err != nil {
		t.Fatalf("books: %v", err)
	}
	if len(books) != 1 || books[0].Path != "/people/carddav/fp1/books/default/" || books[0].Name != "Book default" {
		t.Fatalf("books = %+v", books)
	}

	// Create at a client-chosen name, then read it back by that name.
	object := home + "default/ADA-1.vcf"
	put, err := client.PutAddressObject(ctx, object, dav_test_card("Ada Lovelace", "ada@example.org"))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if put.ETag != "etag-1" {
		t.Fatalf("put etag = %q", put.ETag)
	}
	got, err := client.GetAddressObject(ctx, object)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.ETag != "etag-1" || got.Card.Value(vcard.FieldFormattedName) != "Ada Lovelace" {
		t.Fatalf("get = %+v", got)
	}
	if got.Card.Get(vcard.FieldEmail).Params.Get(vcard.ParamType) != "home" {
		t.Fatalf("email parameter lost: %+v", got.Card.Get(vcard.FieldEmail))
	}
	if got.Card.Get(vcard.FieldName) == nil {
		t.Fatal("a 3.0 card served without N")
	}
	stored := fake.objects["default"]["ADA-1"]["card"].([]any)
	names := []string{}
	for _, p := range stored {
		names = append(names, p.(map[string]any)["name"].(string))
	}
	if strings.Join(names, ",") != "EMAIL,FN,VERSION" {
		t.Fatalf("stored property list = %v", names)
	}

	// The second card lets the query and the multiget pick one of two.
	if _, err := client.PutAddressObject(ctx, home+"default/grace.vcf", dav_test_card("Grace Hopper", "grace@example.org")); err != nil {
		t.Fatalf("put 2: %v", err)
	}
	query := &carddav.AddressBookQuery{
		DataRequest: carddav.AddressDataRequest{AllProp: true},
		PropFilters: []carddav.PropFilter{{Name: vcard.FieldFormattedName, TextMatches: []carddav.TextMatch{{Text: "grace"}}}},
	}
	hits, err := client.QueryAddressBook(ctx, home+"default/", query)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(hits) != 1 || hits[0].Path != home+"default/grace.vcf" {
		t.Fatalf("query hits = %+v", hits)
	}
	multi, err := client.MultiGetAddressBook(ctx, home+"default/", &carddav.AddressBookMultiGet{Paths: []string{object}, DataRequest: carddav.AddressDataRequest{AllProp: true}})
	if err != nil {
		t.Fatalf("multiget: %v", err)
	}
	if len(multi) != 1 || multi[0].Card.Value(vcard.FieldFormattedName) != "Ada Lovelace" {
		t.Fatalf("multiget = %+v", multi)
	}

	// Depth 1 on the collection lists both objects with their etags.
	response := dav_raw(t, server, "PROPFIND", home+"default/", `<?xml version="1.0"?><D:propfind xmlns:D="DAV:"><D:prop><D:getetag/></D:prop></D:propfind>`, map[string]string{"Depth": "1", "Content-Type": "application/xml"})
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusMultiStatus || strings.Count(string(body), "<href>") != 3 || !strings.Contains(string(body), "etag-2") {
		t.Fatalf("propfind depth 1: %d %s", response.StatusCode, body)
	}

	if err := client.RemoveAll(ctx, object); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := client.GetAddressObject(ctx, object); err == nil {
		t.Fatal("deleted object still served")
	}
	if fake.objects["default"]["ADA-1"] != nil {
		t.Fatal("delete did not reach the app")
	}
}

func TestDavPreconditionsReachTheApp(t *testing.T) {
	fake := new_dav_fake_app("default")
	server := dav_test_server(t, "carddav", fake)
	object := "/people/carddav/fp1/books/default/x.vcf"
	var card bytes.Buffer
	vcard.NewEncoder(&card).Encode(dav_test_card("X", "x@example.org"))
	headers := func(extra map[string]string) map[string]string {
		h := map[string]string{"Content-Type": "text/vcard; charset=utf-8"}
		for k, v := range extra {
			h[k] = v
		}
		return h
	}

	// If-Match on an object that does not exist yet: 412.
	if r := dav_raw(t, server, "PUT", object, card.String(), headers(map[string]string{"If-Match": `"etag-0"`})); r.StatusCode != http.StatusPreconditionFailed {
		t.Fatalf("If-Match on a missing object: %d", r.StatusCode)
	}
	// If-None-Match: * creates.
	if r := dav_raw(t, server, "PUT", object, card.String(), headers(map[string]string{"If-None-Match": "*"})); r.StatusCode != http.StatusCreated || r.Header.Get("ETag") != `"etag-1"` {
		t.Fatalf("create: %d etag=%q", r.StatusCode, r.Header.Get("ETag"))
	}
	// ...and refuses to overwrite.
	if r := dav_raw(t, server, "PUT", object, card.String(), headers(map[string]string{"If-None-Match": "*"})); r.StatusCode != http.StatusPreconditionFailed {
		t.Fatalf("If-None-Match on an existing object: %d", r.StatusCode)
	}
	// A stale If-Match is refused; the current one is honoured.
	if r := dav_raw(t, server, "PUT", object, card.String(), headers(map[string]string{"If-Match": `"etag-0"`})); r.StatusCode != http.StatusPreconditionFailed {
		t.Fatalf("stale If-Match: %d", r.StatusCode)
	}
	if r := dav_raw(t, server, "PUT", object, card.String(), headers(map[string]string{"If-Match": `"etag-1"`})); r.StatusCode != http.StatusCreated || r.Header.Get("ETag") != `"etag-2"` {
		t.Fatalf("current If-Match: %d etag=%q", r.StatusCode, r.Header.Get("ETag"))
	}
	// A card at a name without the suffix cannot be created: the href a client
	// would see back differs from the one it used.
	if r := dav_raw(t, server, "PUT", "/people/carddav/fp1/books/default/plain", card.String(), headers(nil)); r.StatusCode != http.StatusNotFound {
		t.Fatalf("suffix-less name: %d", r.StatusCode)
	}
	// Another principal's path is not served.
	if r := dav_raw(t, server, "PUT", "/people/carddav/fp2/books/default/y.vcf", card.String(), headers(nil)); r.StatusCode != http.StatusNotFound {
		t.Fatalf("other principal: %d", r.StatusCode)
	}
}

func TestDavCollectionsCreateAndDelete(t *testing.T) {
	fake := new_dav_fake_app("default")
	server := dav_test_server(t, "carddav", fake)
	mkcol := `<?xml version="1.0" encoding="utf-8" ?><D:mkcol xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav"><D:set><D:prop><D:resourcetype><D:collection/><C:addressbook/></D:resourcetype><D:displayname>Work</D:displayname></D:prop></D:set></D:mkcol>`
	if r := dav_raw(t, server, "MKCOL", "/people/carddav/fp1/books/work/", mkcol, map[string]string{"Content-Type": "application/xml"}); r.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(r.Body)
		t.Fatalf("mkcol: %d %s", r.StatusCode, body)
	}
	if len(fake.collections) != 2 || fake.collections[1]["slug"] != "work" || fake.collections[1]["name"] != "Work" {
		t.Fatalf("collections after mkcol = %+v", fake.collections)
	}
	// Creating it again is refused with the status MKCOL defines.
	if r := dav_raw(t, server, "MKCOL", "/people/carddav/fp1/books/work/", mkcol, map[string]string{"Content-Type": "application/xml"}); r.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("mkcol twice: %d", r.StatusCode)
	}
	// A collection anywhere but under the home set is refused.
	if r := dav_raw(t, server, "MKCOL", "/people/carddav/fp1/work/", "", nil); r.StatusCode != http.StatusForbidden {
		t.Fatalf("mkcol outside the home set: %d", r.StatusCode)
	}
	if r := dav_raw(t, server, "DELETE", "/people/carddav/fp1/books/work/", "", nil); r.StatusCode != http.StatusNoContent {
		t.Fatalf("delete collection: %d", r.StatusCode)
	}
	if len(fake.collections) != 1 {
		t.Fatalf("collections after delete = %+v", fake.collections)
	}
	// The app's refusal reaches the client as 403.
	if r := dav_raw(t, server, "DELETE", "/people/carddav/fp1/books/default/", "", nil); r.StatusCode != http.StatusForbidden {
		t.Fatalf("delete the default book: %d", r.StatusCode)
	}
	// OPTIONS on the route reports the protocol.
	r := dav_raw(t, server, "OPTIONS", "/people/carddav/fp1/books/default/", "", nil)
	if !strings.Contains(r.Header.Get("DAV"), "addressbook") {
		t.Fatalf("OPTIONS DAV header = %q", r.Header.Get("DAV"))
	}
}

// A calendar is made with MKCALENDAR, which the engine serves as the
// library's MKCOL, and removed with DELETE on the collection, which the
// library routes through the object delete.
func TestDavCalDavCollectionsCreateAndDelete(t *testing.T) {
	fake := new_dav_fake_app("default")
	server := dav_test_server(t, "caldav", fake)
	mkcalendar := `<?xml version="1.0" encoding="utf-8" ?><C:mkcalendar xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav"><D:set><D:prop><D:displayname>Work &amp; play</D:displayname><C:calendar-description>Team</C:calendar-description></D:prop></D:set></C:mkcalendar>`
	if r := dav_raw(t, server, "MKCALENDAR", "/people/caldav/fp1/calendars/work/", mkcalendar, map[string]string{"Content-Type": "application/xml"}); r.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(r.Body)
		t.Fatalf("mkcalendar: %d %s", r.StatusCode, body)
	}
	if len(fake.collections) != 2 || fake.collections[1]["slug"] != "work" || fake.collections[1]["name"] != "Work & play" {
		t.Fatalf("collections after mkcalendar = %+v", fake.collections)
	}
	if r := dav_raw(t, server, "MKCALENDAR", "/people/caldav/fp1/calendars/work/", mkcalendar, map[string]string{"Content-Type": "application/xml"}); r.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("mkcalendar twice: %d", r.StatusCode)
	}
	// Without a body the calendar takes the client's path segment as its name.
	if r := dav_raw(t, server, "MKCALENDAR", "/people/caldav/fp1/calendars/plain/", "", nil); r.StatusCode != http.StatusCreated {
		t.Fatalf("mkcalendar without a body: %d", r.StatusCode)
	}
	if len(fake.collections) != 3 || fake.collections[2]["slug"] != "plain" || fake.collections[2]["name"] != "" {
		t.Fatalf("collections after a bare mkcalendar = %+v", fake.collections)
	}
	if r := dav_raw(t, server, "MKCALENDAR", "/people/caldav/fp1/work/", "", nil); r.StatusCode != http.StatusForbidden {
		t.Fatalf("mkcalendar outside the home set: %d", r.StatusCode)
	}
	if r := dav_raw(t, server, "DELETE", "/people/caldav/fp1/calendars/work/", "", nil); r.StatusCode != http.StatusNoContent {
		t.Fatalf("delete calendar: %d", r.StatusCode)
	}
	if len(fake.collections) != 2 || fake.collections[1]["slug"] != "plain" {
		t.Fatalf("collections after delete = %+v", fake.collections)
	}
	if r := dav_raw(t, server, "DELETE", "/people/caldav/fp1/calendars/default/", "", nil); r.StatusCode != http.StatusForbidden {
		t.Fatalf("delete the fixed calendar: %d", r.StatusCode)
	}
	if r := dav_raw(t, server, "DELETE", "/people/caldav/fp1/calendars/missing/", "", nil); r.StatusCode != http.StatusNotFound {
		t.Fatalf("delete a missing calendar: %d", r.StatusCode)
	}
	// The address book route knows no MKCALENDAR.
	books := dav_test_server(t, "carddav", new_dav_fake_app("default"))
	if r := dav_raw(t, books, "MKCALENDAR", "/people/carddav/fp1/books/work/", "", nil); r.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("mkcalendar on carddav: %d", r.StatusCode)
	}
}

const dav_test_event = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Test//EN\r\nBEGIN:VEVENT\r\nUID:ev1\r\nDTSTAMP:20260901T000000Z\r\nDTSTART:20260908T090000Z\r\nDTEND:20260908T100000Z\r\nSUMMARY:Standup\r\nRRULE:FREQ=WEEKLY;COUNT=4\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"

func TestDavCalDavObjectsAndQuery(t *testing.T) {
	fake := new_dav_fake_app("main")
	server := dav_test_server(t, "caldav", fake)
	ctx := context.Background()
	client, err := caldav.NewClient(nil, server.URL+"/people/caldav/")
	if err != nil {
		t.Fatal(err)
	}
	principal, err := client.FindCurrentUserPrincipal(ctx)
	if err != nil {
		t.Fatalf("principal: %v", err)
	}
	home, err := client.FindCalendarHomeSet(ctx, principal)
	if err != nil {
		t.Fatalf("home: %v", err)
	}
	if home != "/people/caldav/fp1/calendars/" {
		t.Fatalf("home = %q", home)
	}
	calendars, err := client.FindCalendars(ctx, home)
	if err != nil {
		t.Fatalf("calendars: %v", err)
	}
	if len(calendars) != 1 || calendars[0].Path != home+"main/" {
		t.Fatalf("calendars = %+v", calendars)
	}

	cal, err := ical.NewDecoder(strings.NewReader(dav_test_event)).Decode()
	if err != nil {
		t.Fatal(err)
	}
	object := home + "main/ev1.ics"
	if _, err := client.PutCalendarObject(ctx, object, cal); err != nil {
		t.Fatalf("put: %v", err)
	}
	stored := fake.objects["main"]["ev1"]
	if stored["summary"] != "Standup" || stored["start"] != int64(1788858000) || stored["recurring"] != true {
		t.Fatalf("shadow columns = %+v", stored)
	}
	got, err := client.GetCalendarObject(ctx, object)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(got.Data.Events()) != 1 || got.ETag != "etag-1" {
		t.Fatalf("get = %+v", got)
	}

	// A time-range query in the third week finds the recurrence's instance
	// and passes the range to the app.
	query := &caldav.CalendarQuery{
		CompRequest: caldav.CalendarCompRequest{Name: ical.CompCalendar, AllProps: true, AllComps: true},
		CompFilter: caldav.CompFilter{Name: ical.CompCalendar, Comps: []caldav.CompFilter{{
			Name:  ical.CompEvent,
			Start: time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC),
			End:   time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC),
		}}},
	}
	hits, err := client.QueryCalendar(ctx, home+"main/", query)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(hits) != 1 {
		t.Fatalf("query hits = %d", len(hits))
	}
	query.CompFilter.Comps[0].Start = time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	query.CompFilter.Comps[0].End = time.Date(2026, 11, 8, 0, 0, 0, 0, time.UTC)
	if hits, err = client.QueryCalendar(ctx, home+"main/", query); err != nil || len(hits) != 0 {
		t.Fatalf("query past the last occurrence: %d %v", len(hits), err)
	}
}

func TestDavParseAndPrefix(t *testing.T) {
	b := &dav_backend{feature: "carddav", prefix: "/people/carddav", principal: "fp1"}
	cases := []struct {
		path       string
		kind       int
		collection string
		object     string
		ok         bool
	}{
		{"/people/carddav/", dav_kind_root, "", "", true},
		{"/people/carddav", dav_kind_root, "", "", true},
		{"/people/carddav/fp1/", dav_kind_principal, "", "", true},
		{"/people/carddav/fp1/books/", dav_kind_home, "", "", true},
		{"/people/carddav/fp1/books/abc/", dav_kind_collection, "abc", "", true},
		{"/people/carddav/fp1/books/abc/x-1.vcf", dav_kind_object, "abc", "x-1", true},
		{"/people/carddav/fp2/books/abc/", 0, "", "", false},
		{"/people/carddav/fp1/calendars/abc/", 0, "", "", false},
		{"/people/carddav/fp1/books/abc/x.ics", 0, "", "", false},
		{"/people/carddav/fp1/books/a b/", 0, "", "", false},
		{"/people/carddav/fp1/books/abc/x.vcf/more", 0, "", "", false},
		{"/other/carddav/fp1/", 0, "", "", false},
	}
	for _, c := range cases {
		kind, collection, object, err := b.parse(c.path)
		if (err == nil) != c.ok || kind != c.kind || collection != c.collection || object != c.object {
			t.Errorf("parse(%q) = %d %q %q %v", c.path, kind, collection, object, err)
		}
	}
	if b.object_path("abc", "x") != "/people/carddav/fp1/books/abc/x.vcf" {
		t.Errorf("object_path = %q", b.object_path("abc", "x"))
	}

	prefixes := []struct{ request, name, want string }{
		{"/people/carddav/fp1/books/", "carddav/*path", "/people/carddav"},
		{"/people/carddav", "carddav/*path", "/people/carddav"},
		{"/carddav/fp1/", "carddav/*path", "/carddav"},
		{"/calendars/caldav/x/", "caldav/*path", "/calendars/caldav"},
		{"/people/x/", ":person/carddav/*path", ""},
		{"/people/other/", "carddav/*path", ""},
	}
	for _, c := range prefixes {
		if got := dav_prefix(c.request, c.name); got != c.want {
			t.Errorf("dav_prefix(%q, %q) = %q, want %q", c.request, c.name, got, c.want)
		}
	}
	if dav_route_literal("carddav/*path") != "carddav" || dav_route_literal(":person/carddav/*path") != "" || dav_route_literal("carddav") != "" {
		t.Error("dav_route_literal")
	}
}

func TestDavCardConversionRoundTrips(t *testing.T) {
	text := "BEGIN:VCARD\r\nVERSION:4.0\r\nFN:Ada Lovelace\r\nN:Lovelace;Ada;;;\r\nitem1.URL:https://example.org\r\nitem1.X-ABLabel:Home Page\r\nTEL;TYPE=cell,voice;PREF=1:+44 20 7946 0000\r\nEND:VCARD\r\n"
	card, err := vcard.NewDecoder(strings.NewReader(text)).Decode()
	if err != nil {
		t.Fatal(err)
	}
	list := dav_properties_from_card(card)
	var url map[string]any
	for _, p := range list {
		if m := p.(map[string]any); m["name"] == "URL" {
			url = m
		}
	}
	if url == nil || url["group"] != "item1" {
		t.Fatalf("group lost: %+v", url)
	}
	back, err := dav_card_from_properties(list)
	if err != nil {
		t.Fatal(err)
	}
	if back.Value(vcard.FieldVersion) != "4.0" || back.Get("URL").Group != "item1" {
		t.Fatalf("round trip: %+v", back)
	}
	types := back.Get(vcard.FieldTelephone).Params[vcard.ParamType]
	if len(types) != 2 || types[0] != "cell" || types[1] != "voice" {
		t.Fatalf("parameters: %v", types)
	}
	var buf bytes.Buffer
	if err := vcard.NewEncoder(&buf).Encode(back); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "item1.X-ABLABEL:Home Page\r\n") {
		t.Fatalf("encoded: %s", buf.String())
	}

	// A list with no VERSION and no N, as a friend's migrated card, serves as
	// a valid 3.0 card.
	minimal, err := dav_card_from_properties([]any{map[string]any{"name": "FN", "params": map[string]any{}, "value": "Bob"}})
	if err != nil {
		t.Fatal(err)
	}
	if minimal.Value(vcard.FieldVersion) != "3.0" || minimal.Value(vcard.FieldName) != ";;;;" {
		t.Fatalf("minimal card: %+v", minimal)
	}
	out, err := dav_card_text([]any{map[string]any{"name": "FN", "params": map[string]any{}, "value": "Bob"}})
	if err != nil || !strings.HasPrefix(out, "BEGIN:VCARD\r\nVERSION:3.0\r\n") {
		t.Fatalf("dav_card_text: %q %v", out, err)
	}
}

// === Authentication ===

func basic_auth_encode(username string, password string) string {
	return base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
}

func dav_auth_env(t *testing.T) (*App, *AppAction) {
	t.Helper()
	test_data_directory(t)
	setup_users_test_schema()
	db_open("db/sessions.db").exec("create table accesses (hash text primary key not null, user text not null, used integer not null default 0)")
	users := db_open("db/users.db")
	users.exec("insert into users (uid, username, methods, status) values ('u-dav', 'dav@example.com', 'email', 'active')")
	users.exec("insert into entities (id, private, fingerprint, user, class, name) values ('e-dav', '', 'fp-dav', 'u-dav', 'person', 'Dav')")
	a := &App{id: "people"}
	a.internal = &AppVersion{app: a}
	return a, &AppAction{Feature: "carddav", Public: true, name: "carddav/*path"}
}

func dav_test_context(method string, target string, headers map[string]string) (*gin.Context, *httptest.ResponseRecorder) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(method, target, nil)
	for k, v := range headers {
		c.Request.Header.Set(k, v)
	}
	return c, recorder
}

func TestDavAuthenticate(t *testing.T) {
	a, aa := dav_auth_env(t)
	dav := token_create("u-dav", "people", "iPad", []string{"dav"}, 0, "carddav/*path", "")
	unbound := token_create("u-dav", "people", "api", nil, 0, "", "")
	git := token_create("u-dav", "people", "git", []string{"git"}, 0, "", "")
	other := token_create("u-dav", "feeds", "dav", []string{"dav"}, 0, "", "")
	elsewhere := token_create("u-dav", "people", "bound", []string{"dav"}, 0, ":repository/git/*path", "")
	basic := func(secret string) map[string]string {
		return map[string]string{"Authorization": "Basic " + basic_auth_encode("dav@example.com", secret)}
	}

	cases := []struct {
		name    string
		headers map[string]string
		query   string
		want    bool
	}{
		{"no credential", nil, "", false},
		{"dav token over Basic", basic(dav), "", true},
		{"unbound token with every scope", basic(unbound), "", true},
		{"git-scoped token", basic(git), "", false},
		{"another app's token", basic(other), "", false},
		{"token bound to another route", basic(elsewhere), "", false},
		{"garbage password", basic("mochi-nope"), "", false},
		{"dav token as Bearer", map[string]string{"Authorization": "Bearer " + dav}, "", true},
		// A credential in the URL is logged and copied, and no client needs one.
		{"dav token in the query", nil, "?token=" + dav, false},
		{"a valid Basic credential beside a query token", basic(dav), "?token=" + dav, false},
		{"a JWT", map[string]string{"Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.e30.x"}, "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx, _ := dav_test_context("PROPFIND", "/people/carddav/"+c.query, c.headers)
			user := dav_authenticate(ctx, a, aa)
			if (user != nil) != c.want {
				t.Fatalf("authenticated = %v, want %v", user != nil, c.want)
			}
			if user != nil && user.UID != "u-dav" {
				t.Fatalf("user = %q", user.UID)
			}
		})
	}
}

func TestDavHandlerChallengesAndServes(t *testing.T) {
	a, aa := dav_auth_env(t)
	ctx, recorder := dav_test_context("PROPFIND", "/people/carddav/", nil)
	if !dav_http_handler(ctx, a, aa) {
		t.Fatal("handler did not claim the request")
	}
	if recorder.Code != http.StatusUnauthorized || !strings.HasPrefix(recorder.Header().Get("WWW-Authenticate"), "Basic ") {
		t.Fatalf("unauthenticated: %d %q", recorder.Code, recorder.Header().Get("WWW-Authenticate"))
	}

	token := token_create("u-dav", "people", "iPad", []string{"dav"}, 0, "carddav/*path", "")
	ctx, recorder = dav_test_context("PROPFIND", "/people/carddav/", map[string]string{
		"Authorization": "Basic " + basic_auth_encode("x", token),
		"Depth":         "0",
	})
	dav_http_handler(ctx, a, aa)
	if recorder.Code != http.StatusMultiStatus || !strings.Contains(recorder.Body.String(), "/people/carddav/fp-dav/") {
		t.Fatalf("root propfind: %d %s", recorder.Code, recorder.Body.String())
	}
}

// === Middleware ===

func TestWebOptionsProbeReachesTheRoute(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(web_security_headers)
	r.Any("/x", func(c *gin.Context) { c.String(http.StatusOK, "reached") })

	preflight := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodOptions, "/x", nil)
	request.Header.Set("Origin", "null")
	request.Header.Set("Access-Control-Request-Method", "POST")
	r.ServeHTTP(preflight, request)
	if preflight.Code != http.StatusNoContent || preflight.Header().Get("Access-Control-Allow-Methods") == "" {
		t.Fatalf("preflight: %d", preflight.Code)
	}

	probe := httptest.NewRecorder()
	r.ServeHTTP(probe, httptest.NewRequest(http.MethodOptions, "/x", nil))
	if probe.Code != http.StatusOK || probe.Body.String() != "reached" {
		t.Fatalf("probe: %d %q", probe.Code, probe.Body.String())
	}
}

func TestWebBodyLimitRaisesTheCeilingForCards(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(web_body_limit)
	r.PUT("/x", func(c *gin.Context) {
		if _, err := io.ReadAll(c.Request.Body); err != nil {
			c.String(http.StatusRequestEntityTooLarge, "too large")
			return
		}
		c.String(http.StatusOK, "ok")
	})
	body := bytes.Repeat([]byte("x"), web_body_maximum+1)
	for _, c := range []struct {
		content string
		want    int
	}{
		{"text/vcard; charset=utf-8", http.StatusOK},
		{"text/calendar", http.StatusOK},
		{"application/json", http.StatusRequestEntityTooLarge},
	} {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPut, "/x", bytes.NewReader(body))
		request.Header.Set("Content-Type", c.content)
		r.ServeHTTP(recorder, request)
		if recorder.Code != c.want {
			t.Errorf("%s: %d, want %d", c.content, recorder.Code, c.want)
		}
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPut, "/x", bytes.NewReader(bytes.Repeat([]byte("x"), dav_body_maximum+1)))
	request.Header.Set("Content-Type", "text/vcard")
	r.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("a card past the DAV ceiling was accepted")
	}
}

func TestDavRootFromManifests(t *testing.T) {
	apps_lock.Lock()
	previous := apps
	apps = map[string]*App{}
	apps_lock.Unlock()
	t.Cleanup(func() {
		apps_lock.Lock()
		apps = previous
		apps_lock.Unlock()
	})
	if dav_root("carddav") != "" {
		t.Fatal("a root with no apps")
	}
	av := &AppVersion{Paths: []string{"people"}, Actions: map[string]AppAction{
		"carddav/*path": {Feature: "carddav", Public: true},
		"-/contacts":    {Function: "action_contacts"},
	}}
	a := &App{id: "people", development: true, internal: av}
	av.app = a
	apps_lock.Lock()
	apps["people"] = a
	apps_lock.Unlock()
	if got := dav_root("carddav"); got != "/people/carddav/" {
		t.Fatalf("root = %q", got)
	}
	if dav_root("caldav") != "" {
		t.Fatal("caldav root from an app that declares only carddav")
	}
}

func TestDavFeatureRouteMatchesItsRoot(t *testing.T) {
	av := &AppVersion{Actions: map[string]AppAction{
		"":              {File: "web/dist/index.html"},
		"carddav/*path": {Feature: "carddav", Public: true},
		"-/files/*path": {Function: "action_files"},
	}, app: &App{id: "people"}}
	for _, c := range []struct{ name, want, path string }{
		{"carddav", "carddav/*path", ""},
		{"carddav/fp1/books/x/", "carddav/*path", "fp1/books/x/"},
		{"-/files/a/b", "-/files/*path", "a/b"},
		// A plain wildcard still needs a segment: the catch-all answers.
		{"-/files", "", ""},
	} {
		aa := av.find_action(c.name)
		if aa == nil || aa.name != c.want || aa.parameters["path"] != c.path {
			t.Errorf("find_action(%q) = %+v, want %q with path %q", c.name, aa, c.want, c.path)
		}
	}
}

func TestDavOptionsWithoutCredentialNamesTheProtocol(t *testing.T) {
	a, aa := dav_auth_env(t)
	ctx, recorder := dav_test_context(http.MethodOptions, "/people/carddav/", nil)
	dav_http_handler(ctx, a, aa)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Header().Get("DAV"), "addressbook") {
		t.Fatalf("anonymous OPTIONS: %d DAV=%q", recorder.Code, recorder.Header().Get("DAV"))
	}
}

// A PROPFIND with no body asks for every property. The body arrives wrapped by
// web_body_limit, whose zero-length read reports no end of file.
func TestDavEmptyPropfindThroughTheBodyLimit(t *testing.T) {
	a, aa := dav_auth_env(t)
	token := token_create("u-dav", "people", "iPad", []string{"dav"}, 0, "carddav/*path", "")
	ctx, recorder := dav_test_context("PROPFIND", "/people/carddav/", map[string]string{
		"Authorization": "Basic " + basic_auth_encode("x", token),
		"Depth":         "0",
	})
	ctx.Request.Body = http.MaxBytesReader(recorder, ctx.Request.Body, web_body_maximum)
	dav_http_handler(ctx, a, aa)
	if recorder.Code != http.StatusMultiStatus || !strings.Contains(recorder.Body.String(), "current-user-principal") {
		t.Fatalf("empty PROPFIND: %d %s", recorder.Code, recorder.Body.String())
	}

	// A body that is present still reaches the library whole.
	body := `<?xml version="1.0"?><D:propfind xmlns:D="DAV:"><D:prop><D:current-user-principal/></D:prop></D:propfind>`
	ctx, recorder = dav_test_context("PROPFIND", "/people/carddav/", map[string]string{
		"Authorization": "Basic " + basic_auth_encode("x", token),
		"Depth":         "0",
		"Content-Type":  "application/xml",
	})
	ctx.Request.Body = http.MaxBytesReader(recorder, io.NopCloser(strings.NewReader(body)), web_body_maximum)
	dav_http_handler(ctx, a, aa)
	if recorder.Code != http.StatusMultiStatus || !strings.Contains(recorder.Body.String(), "/people/carddav/fp-dav/") {
		t.Fatalf("PROPFIND with a body: %d %s", recorder.Code, recorder.Body.String())
	}
}

// A route's closing state: a closed or restricted app refuses a token minted
// while it was open.
func TestDavHandlerAppliesTheAppsRequirements(t *testing.T) {
	a, aa := dav_auth_env(t)
	a.internal.Require.Role = "administrator"
	token := token_create("u-dav", "people", "iPad", []string{"dav"}, 0, "carddav/*path", "")
	ctx, recorder := dav_test_context("PROPFIND", "/people/carddav/", map[string]string{
		"Authorization": "Basic " + basic_auth_encode("x", token),
		"Depth":         "0",
	})
	dav_http_handler(ctx, a, aa)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("a user the app is not offered to: %d", recorder.Code)
	}
}

func TestDavDeleteHonoursIfMatch(t *testing.T) {
	fake := new_dav_fake_app("default")
	server := dav_test_server(t, "carddav", fake)
	object := "/people/carddav/fp1/books/default/x.vcf"
	var card bytes.Buffer
	vcard.NewEncoder(&card).Encode(dav_test_card("X", "x@example.org"))
	if r := dav_raw(t, server, "PUT", object, card.String(), map[string]string{"Content-Type": "text/vcard"}); r.StatusCode != http.StatusCreated {
		t.Fatalf("create: %d", r.StatusCode)
	}
	if r := dav_raw(t, server, "DELETE", object, "", map[string]string{"If-Match": `"etag-0"`}); r.StatusCode != http.StatusPreconditionFailed {
		t.Fatalf("DELETE with a stale If-Match: %d", r.StatusCode)
	}
	if fake.objects["default"]["x"] == nil {
		t.Fatal("a stale DELETE removed the object")
	}
	if r := dav_raw(t, server, "DELETE", object, "", map[string]string{"If-Match": `"etag-1"`}); r.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE with the current If-Match: %d", r.StatusCode)
	}
}

// A multiget of many hrefs in one collection costs two app calls, not one
// each; hrefs spread over many collections stop at the call budget.
func TestDavMultigetIsBounded(t *testing.T) {
	fake := new_dav_fake_app("default")
	for i := 0; i < 300; i++ {
		name := fmt.Sprintf("c%03d", i)
		fake.objects["default"][name] = map[string]any{"name": name, "etag": "e", "updated": int64(1), "card": []any{map[string]any{"name": "FN", "params": map[string]any{}, "value": name}}}
	}
	server := dav_test_server(t, "carddav", fake)
	var hrefs strings.Builder
	for i := 0; i < 300; i++ {
		fmt.Fprintf(&hrefs, "<D:href>/people/carddav/fp1/books/default/c%03d.vcf</D:href>", i)
	}
	body := `<?xml version="1.0"?><C:addressbook-multiget xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav"><D:prop><D:getetag/><C:address-data/></D:prop>` + hrefs.String() + `</C:addressbook-multiget>`
	fake.calls = nil
	r := dav_raw(t, server, "REPORT", "/people/carddav/fp1/books/default/", body, map[string]string{"Content-Type": "application/xml", "Depth": "1"})
	all, _ := io.ReadAll(r.Body)
	if r.StatusCode != http.StatusMultiStatus || strings.Count(string(all), "c299") == 0 {
		t.Fatalf("multiget: %d", r.StatusCode)
	}
	if len(fake.calls) != 1 {
		t.Fatalf("300 hrefs in one collection made %d app calls, want 1", len(fake.calls))
	}

	// Hrefs over many collections stop at the call budget.
	hrefs.Reset()
	for i := 0; i < dav_calls_maximum+100; i++ {
		fmt.Fprintf(&hrefs, "<D:href>/people/carddav/fp1/books/b%d/x.vcf</D:href>", i)
	}
	body = `<?xml version="1.0"?><C:addressbook-multiget xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav"><D:prop><D:getetag/></D:prop>` + hrefs.String() + `</C:addressbook-multiget>`
	fake.calls = nil
	dav_raw(t, server, "REPORT", "/people/carddav/fp1/books/default/", body, map[string]string{"Content-Type": "application/xml", "Depth": "1"})
	if len(fake.calls) > dav_calls_maximum {
		t.Fatalf("hrefs over many collections made %d app calls, budget %d", len(fake.calls), dav_calls_maximum)
	}

	// Past the href cap the report is refused before any work.
	hrefs.Reset()
	for i := 0; i < dav_hrefs_maximum+1; i++ {
		fmt.Fprintf(&hrefs, "<D:href>/people/carddav/fp1/books/default/c%d.vcf</D:href>", i)
	}
	body = `<?xml version="1.0"?><C:addressbook-multiget xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav"><D:prop><D:getetag/></D:prop>` + hrefs.String() + `</C:addressbook-multiget>`
	fake.calls = nil
	a, aa := dav_auth_env(t)
	token := token_create("u-dav", "people", "iPad", []string{"dav"}, 0, "carddav/*path", "")
	ctx, recorder := dav_test_context("REPORT", "/people/carddav/fp-dav/books/default/", map[string]string{
		"Authorization": "Basic " + basic_auth_encode("x", token),
		"Content-Type":  "application/xml",
	})
	ctx.Request.Body = io.NopCloser(strings.NewReader(body))
	dav_http_handler(ctx, a, aa)
	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("a report past the href cap: %d", recorder.Code)
	}
}

// A listing asks the app for etags alone unless the request names content;
// a query walks pages and still finds a match past the first.
func TestDavListingsAskForContentOnlyWhenNamed(t *testing.T) {
	fake := new_dav_fake_app("default")
	for i := 0; i < 250; i++ {
		name := fmt.Sprintf("c%03d", i)
		fake.objects["default"][name] = map[string]any{"name": name, "etag": "e" + name, "updated": int64(1), "card": []any{
			map[string]any{"name": "VERSION", "params": map[string]any{}, "value": "3.0"},
			map[string]any{"name": "FN", "params": map[string]any{}, "value": "Person " + name},
		}}
	}
	server := dav_test_server(t, "carddav", fake)
	listing := `<?xml version="1.0"?><D:propfind xmlns:D="DAV:"><D:prop><D:getetag/></D:prop></D:propfind>`
	fake.arguments = nil
	r := dav_raw(t, server, "PROPFIND", "/people/carddav/fp1/books/default/", listing, map[string]string{"Content-Type": "application/xml", "Depth": "1"})
	body, _ := io.ReadAll(r.Body)
	if r.StatusCode != http.StatusMultiStatus || strings.Count(string(body), "<href>") != 251 {
		t.Fatalf("listing: %d, %d hrefs", r.StatusCode, strings.Count(string(body), "<href>"))
	}
	for _, args := range fake.arguments {
		if args["data"] == true {
			t.Fatalf("an etag listing asked the app for content: %+v", args)
		}
	}
	data := `<?xml version="1.0"?><D:propfind xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav"><D:prop><D:getetag/><C:address-data/></D:prop></D:propfind>`
	fake.arguments = nil
	r = dav_raw(t, server, "PROPFIND", "/people/carddav/fp1/books/default/", data, map[string]string{"Content-Type": "application/xml", "Depth": "1"})
	body, _ = io.ReadAll(r.Body)
	if r.StatusCode != http.StatusMultiStatus || !strings.Contains(string(body), "FN:Person c249") {
		t.Fatalf("listing with address-data: %d", r.StatusCode)
	}

	ctx := context.Background()
	client, err := carddav.NewClient(nil, server.URL+"/people/carddav/")
	if err != nil {
		t.Fatal(err)
	}
	fake.calls = nil
	hits, err := client.QueryAddressBook(ctx, "/people/carddav/fp1/books/default/", &carddav.AddressBookQuery{
		DataRequest: carddav.AddressDataRequest{AllProp: true},
		PropFilters: []carddav.PropFilter{{Name: vcard.FieldFormattedName, TextMatches: []carddav.TextMatch{{Text: "PERSON C24"}}}},
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(hits) != 10 || len(fake.calls) != 3 {
		t.Fatalf("query over three pages: %d hits, %d calls", len(hits), len(fake.calls))
	}
}

// A rule that recurs every second from 1970 is two billion steps from a query
// about this week. The query leaves it out instead of expanding it.
func TestDavCalendarQuerySkipsRunawayRecurrences(t *testing.T) {
	fake := new_dav_fake_app("main")
	server := dav_test_server(t, "caldav", fake)
	ctx := context.Background()
	client, err := caldav.NewClient(nil, server.URL+"/people/caldav/")
	if err != nil {
		t.Fatal(err)
	}
	put := func(name string, text string) {
		cal, err := ical.NewDecoder(strings.NewReader(text)).Decode()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.PutCalendarObject(ctx, "/people/caldav/fp1/calendars/main/"+name+".ics", cal); err != nil {
			t.Fatalf("put %s: %v", name, err)
		}
	}
	put("runaway", "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Test//EN\r\nBEGIN:VEVENT\r\nUID:r1\r\nDTSTAMP:20260901T000000Z\r\nDTSTART:19700101T000000Z\r\nDTEND:19700101T000001Z\r\nRRULE:FREQ=SECONDLY\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n")
	put("weekly", dav_test_event)
	query := &caldav.CalendarQuery{
		CompRequest: caldav.CalendarCompRequest{Name: ical.CompCalendar, AllProps: true, AllComps: true},
		CompFilter: caldav.CompFilter{Name: ical.CompCalendar, Comps: []caldav.CompFilter{{
			Name:  ical.CompEvent,
			Start: time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC),
			End:   time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC),
		}}},
	}
	began := time.Now()
	hits, err := client.QueryCalendar(ctx, "/people/caldav/fp1/calendars/main/", query)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if elapsed := time.Since(began); elapsed > 5*time.Second {
		t.Fatalf("query took %s", elapsed)
	}
	if len(hits) != 1 || !strings.HasSuffix(hits[0].Path, "/weekly.ics") {
		t.Fatalf("hits = %d", len(hits))
	}
}

func TestDavGitRouteStillNeedsASegment(t *testing.T) {
	av := &AppVersion{Actions: map[string]AppAction{
		"":                      {File: "web/dist/index.html"},
		":repository/git/*path": {Feature: "git", Public: true},
		":repository":           {File: "web/dist/index.html"},
	}, app: &App{id: "repositories"}}
	if aa := av.find_action("abcdefghi/git"); aa == nil || aa.name == ":repository/git/*path" {
		t.Fatalf("a bare git URL matched the git route: %+v", aa)
	}
	if aa := av.find_action("abcdefghi/git/info/refs"); aa == nil || aa.name != ":repository/git/*path" {
		t.Fatalf("git/info/refs did not match the git route: %+v", aa)
	}
}

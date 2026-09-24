// Mochi server: the CalDAV client against the engine and against fixtures
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// dav_client_test_account is a caldav account row pointed at a server.
func dav_client_test_account(address string) map[string]any {
	return map[string]any{
		"id": "acc-1", "type": "caldav", "label": "Test", "identifier": address,
		"data": json_encode(map[string]any{"url": address, "username": "u", "password": "p"}),
	}
}

// TestDavClientSyncsWithTheEngine drives the whole sync surface against the
// engine serving the fake app, the way the calendars app syncs with another
// Mochi server: discovery from the root, the empty listing, a new object, its
// etag in the listing and its text in a multiget, a write over the wrong
// version refused and over the right one taken, and the same for a delete.
func TestDavClientSyncsWithTheEngine(t *testing.T) {
	private_endpoints_allowed(t)
	fake := new_dav_fake_app("main")
	server := dav_test_server(t, "caldav", fake)
	ctx := context.Background()

	client, err := dav_client_for(&User{UID: "u-sync"}, dav_client_test_account(server.URL+"/people/caldav/"))
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	calendars, err := client.calendars(ctx)
	if err != nil {
		t.Fatalf("calendars: %v", err)
	}
	collection := server.URL + "/people/caldav/fp1/calendars/main/"
	if len(calendars) != 1 || calendars[0].Href != collection || calendars[0].Name != "Book main" || calendars[0].Readonly {
		t.Fatalf("calendars = %+v", calendars)
	}

	if _, _, err := client.status(ctx, collection); err != nil {
		t.Fatalf("status: %v", err)
	}
	listing, err := client.list(ctx, collection)
	if err != nil || len(listing) != 0 {
		t.Fatalf("empty listing = %+v, %v", listing, err)
	}

	// Etags travel as the server writes them, quotes included: the listing,
	// the multiget and the PUT answer all read the same string, and it goes
	// back verbatim in If-Match.
	object := collection + "ev1.ics"
	etag, err := client.put(ctx, object, dav_test_event, "")
	if err != nil || etag != `"etag-1"` {
		t.Fatalf("put new: %q, %v", etag, err)
	}
	if _, err := client.put(ctx, object, dav_test_event, ""); err == nil || !strings.HasPrefix(err.Error(), "conflict") {
		t.Errorf("a second create over an existing object was not a conflict: %v", err)
	}
	listing, err = client.list(ctx, collection)
	if err != nil || len(listing) != 1 || listing[0].Href != object || listing[0].Etag != `"etag-1"` {
		t.Fatalf("listing = %+v, %v", listing, err)
	}
	objects, err := client.get(ctx, collection, []string{object, collection + "absent.ics"})
	if err != nil || len(objects) != 1 {
		t.Fatalf("multiget = %+v, %v", objects, err)
	}
	if objects[0]["href"] != object || objects[0]["etag"] != `"etag-1"` || !strings.Contains(objects[0]["ics"].(string), "SUMMARY:Standup") {
		t.Fatalf("multiget object = %+v", objects[0])
	}

	changed := strings.Replace(dav_test_event, "SUMMARY:Standup", "SUMMARY:Moved", 1)
	if _, err := client.put(ctx, object, changed, `"etag-stale"`); err == nil || !strings.HasPrefix(err.Error(), "conflict") {
		t.Errorf("a write over a stale version was not a conflict: %v", err)
	}
	etag, err = client.put(ctx, object, changed, `"etag-1"`)
	if err != nil || etag != `"etag-2"` {
		t.Fatalf("put over the current version: %q, %v", etag, err)
	}
	if fake.objects["main"]["ev1"]["summary"] != "Moved" {
		t.Errorf("the write did not reach the app: %+v", fake.objects["main"]["ev1"])
	}

	if err := client.delete(ctx, object, `"etag-1"`); err == nil || !strings.HasPrefix(err.Error(), "conflict") {
		t.Errorf("a delete of a stale version was not a conflict: %v", err)
	}
	if err := client.delete(ctx, object, `"etag-2"`); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if fake.objects["main"]["ev1"] != nil {
		t.Error("the delete did not reach the app")
	}
	if err := client.delete(ctx, object, ""); err != nil {
		t.Errorf("deleting an object already gone: %v, want nil", err)
	}
}

// TestDavClientRefusesInternalAddress: the account's address is the user's,
// but the request is the server's, so the outbound guard applies to it as to
// every other caller-supplied address.
func TestDavClientRefusesInternalAddress(t *testing.T) {
	fake := new_dav_fake_app("main")
	server := dav_test_server(t, "caldav", fake)
	ctx := context.Background()
	client, err := dav_client_for(&User{UID: "u-sync"}, dav_client_test_account(server.URL+"/people/caldav/"))
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	with_private_allowed(t, func() {
		if _, err := client.calendars(ctx); err != nil {
			t.Fatalf("unreachable with the guard off, so the case below proves nothing: %v", err)
		}
	})
	if _, err := client.calendars(ctx); err == nil || err.Error() != "transport" {
		t.Errorf("a loopback calendar server was contacted with the guard on: %v", err)
	}
}

// TestDavClientFollowsRedirectsWithTheCredential: iCloud answers the
// discovery on one host and keeps the calendars on another, and Go's client
// drops the credential on a change of host, so the client follows by hand.
func TestDavClientFollowsRedirectsWithTheCredential(t *testing.T) {
	private_endpoints_allowed(t)
	var seen []string
	var mux *http.ServeMux
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { mux.ServeHTTP(w, r) }))
	defer second.Close()
	mux = http.NewServeMux()
	mux.HandleFunc("/moved/", func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.Method+" "+r.Header.Get("Authorization"))
		w.Header().Set("ETag", "\"e9\"")
		w.WriteHeader(http.StatusCreated)
	})
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, second.URL+"/moved/"+strings.TrimPrefix(r.URL.Path, "/"), http.StatusTemporaryRedirect)
	}))
	defer first.Close()

	client, err := dav_client_for(&User{UID: "u-sync"}, dav_client_test_account(first.URL+"/"))
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	etag, err := client.put(context.Background(), first.URL+"/ev.ics", dav_test_event, "")
	if err != nil || etag != "\"e9\"" {
		t.Fatalf("put through a redirect: %q, %v", etag, err)
	}
	if len(seen) != 1 || !strings.HasPrefix(seen[0], "PUT Basic ") {
		t.Errorf("the redirected request carried no credential: %v", seen)
	}
}

// TestDavClientReadsCollections pins how a home set listing is read: a
// collection that is not a calendar is skipped, one holding only tasks is
// skipped, Apple's eight-digit colour is cut to the app's six, a privilege set
// without write makes the calendar read-only, and a nameless collection is
// named by its path.
func TestDavClientReadsCollections(t *testing.T) {
	answer := `<?xml version="1.0"?>
<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav" xmlns:A="http://apple.com/ns/ical/">
 <D:response><D:href>/home/</D:href><D:propstat><D:prop><D:resourcetype><D:collection/></D:resourcetype></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>
 <D:response><D:href>/home/work/</D:href><D:propstat><D:prop><D:resourcetype><D:collection/><C:calendar/></D:resourcetype><D:displayname>Work</D:displayname><A:calendar-color>#FF8800FF</A:calendar-color><C:supported-calendar-component-set><C:comp name="VEVENT"/><C:comp name="VTODO"/></C:supported-calendar-component-set><D:current-user-privilege-set><D:privilege><D:read/></D:privilege></D:current-user-privilege-set></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>
 <D:response><D:href>/home/tasks/</D:href><D:propstat><D:prop><D:resourcetype><D:collection/><C:calendar/></D:resourcetype><D:displayname>Tasks</D:displayname><C:supported-calendar-component-set><C:comp name="VTODO"/></C:supported-calendar-component-set></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>
 <D:response><D:href>https://other.example.com/home/nameless/</D:href><D:propstat><D:prop><D:resourcetype><D:collection/><C:calendar/></D:resourcetype><D:current-user-privilege-set><D:privilege><D:read/></D:privilege><D:privilege><D:write/></D:privilege></D:current-user-privilege-set></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>
</D:multistatus>`
	private_endpoints_allowed(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusMultiStatus)
		w.Write([]byte(answer))
	}))
	defer server.Close()
	client, err := dav_client_for(&User{UID: "u-sync"}, dav_client_test_account(server.URL+"/home/"))
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	from, _ := url.Parse(server.URL + "/home/")
	answered, _, err := client.propfind(context.Background(), server.URL+"/home/", "1", dav_client_calendar_props)
	if err != nil {
		t.Fatalf("propfind: %v", err)
	}
	var calendars []dav_client_calendar
	for i := range answered.Responses {
		if cal := dav_client_calendar_of(from, &answered.Responses[i]); cal != nil {
			calendars = append(calendars, *cal)
		}
	}
	if len(calendars) != 2 {
		t.Fatalf("calendars = %+v, want the two event calendars", calendars)
	}
	work := calendars[0]
	if work.Href != server.URL+"/home/work/" || work.Name != "Work" || work.Colour != "#ff8800" || !work.Readonly {
		t.Errorf("work = %+v", work)
	}
	other := calendars[1]
	if other.Href != "https://other.example.com/home/nameless/" || other.Name != "nameless" || other.Readonly {
		t.Errorf("nameless = %+v", other)
	}
}

// TestDavClientStatuses pins the answer codes an app reads.
func TestDavClientStatuses(t *testing.T) {
	cases := map[int]string{401: "unauthorised", 403: "unauthorised", 404: "missing", 412: "conflict", 413: "large", 507: "large", 500: "status", 502: "status"}
	for status, code := range cases {
		err := dav_client_status(status)
		if err == nil || !strings.HasPrefix(err.Error(), code) {
			t.Errorf("status %d = %v, want %s", status, err, code)
		}
	}
	for _, status := range []int{200, 201, 204, 207} {
		if err := dav_client_status(status); err != nil {
			t.Errorf("status %d = %v, want nil", status, err)
		}
	}
	if out := dav_client_result(dav_client_fail("status", 502)); out["error"] != "status" || out["status"] != 502 {
		t.Errorf("result = %v", out)
	}
	if out := dav_client_result(context.DeadlineExceeded); out["error"] != "transport" {
		t.Errorf("a plain error reads as %v, want transport", out)
	}
}

// TestDavClientNeedsAGrant: a Google account without the calendar grant has
// nothing to sync with, and answers unauthorised before any request.
func TestDavClientNeedsAGrant(t *testing.T) {
	row := map[string]any{"id": "acc-g", "type": "google", "identifier": "sub-1", "data": json_encode(map[string]any{"refresh": "r", "scopes": []string{"openid"}})}
	if _, err := dav_client_for(&User{UID: "u-g"}, row); err == nil || err.Error() != "unauthorised" {
		t.Errorf("client for an ungranted google account: %v, want unauthorised", err)
	}
	if oauth_account_granted(&User{UID: "u-g"}, row, "calendar") {
		t.Error("granted without the calendar scope")
	}
	row["data"] = json_encode(map[string]any{"refresh": "r", "scopes": []string{"openid", "https://www.googleapis.com/auth/calendar"}})
	if !oauth_account_granted(&User{UID: "u-g"}, row, "calendar") {
		t.Error("not granted with the calendar scope")
	}
}

// TestDavClientDiscoveryFallsThrough: a server whose root answers nothing
// useful (Google's does not name a principal there) is asked at the well-known
// path, and one that answers nothing there either is asked at the principal
// the provider documents, which a Google account derives from its email.
func TestDavClientDiscoveryFallsThrough(t *testing.T) {
	private_endpoints_allowed(t)
	multistatus := func(body string) string {
		return `<?xml version="1.0"?><D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">` + body + `</D:multistatus>`
	}
	var served []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		served = append(served, r.URL.Path)
		switch r.URL.Path {
		case "/.well-known/caldav":
			http.Redirect(w, r, "/principals/me/", http.StatusTemporaryRedirect)
		case "/principals/me/":
			w.WriteHeader(http.StatusMultiStatus)
			w.Write([]byte(multistatus(`<D:response><D:href>/principals/me/</D:href><D:propstat><D:prop><D:current-user-principal><D:href>/principals/me/</D:href></D:current-user-principal><C:calendar-home-set><D:href>/home/</D:href></C:calendar-home-set></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>`)))
		case "/home/":
			w.WriteHeader(http.StatusMultiStatus)
			w.Write([]byte(multistatus(`<D:response><D:href>/home/work/</D:href><D:propstat><D:prop><D:resourcetype><D:collection/><C:calendar/></D:resourcetype><D:displayname>Work</D:displayname></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>`)))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	// The root answers 404: the well-known path leads to the principal.
	client, err := dav_client_for(&User{UID: "u-sync"}, dav_client_test_account(server.URL+"/caldav/v2/"))
	if err != nil {
		t.Fatal(err)
	}
	calendars, err := client.calendars(context.Background())
	if err != nil || len(calendars) != 1 || calendars[0].Name != "Work" {
		t.Fatalf("discovery through the well-known path: %+v, %v (served %v)", calendars, err, served)
	}

	// Neither the root nor the well-known path answers: the documented
	// principal is used, here the one a Google account derives.
	served = nil
	client.principal = server.URL + "/principals/me/"
	client.base, _ = url.Parse(server.URL + "/nowhere/")
	blind := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.NotFound(w, r) }))
	defer blind.Close()
	client.base, _ = url.Parse(blind.URL + "/")
	calendars, err = client.calendars(context.Background())
	if err != nil || len(calendars) != 1 {
		t.Fatalf("discovery through the documented principal: %+v, %v", calendars, err)
	}

	// A refused credential stops discovery at once rather than being retried
	// at the next address.
	refusing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusUnauthorized) }))
	defer refusing.Close()
	client.base, _ = url.Parse(refusing.URL + "/")
	if _, err := client.calendars(context.Background()); err == nil || !strings.HasPrefix(err.Error(), "unauthorised") {
		t.Errorf("a refused credential: %v, want unauthorised", err)
	}

	// A Google account derives its documented principal from its email.
	if got := dav_client_google_principal(map[string]any{"label": "someone@example.test"}); got != dav_client_google_root+"someone@example.test/user" {
		t.Errorf("google principal = %q", got)
	}
	if got := dav_client_google_principal(map[string]any{"label": "Someone"}); got != "" {
		t.Errorf("a label that is no address gave %q", got)
	}
}

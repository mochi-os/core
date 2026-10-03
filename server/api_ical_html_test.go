// Mochi server: iCalendar descriptions written as HTML, tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/emersion/go-ical"
	"github.com/emersion/go-webdav/caldav"
	sl "go.starlark.net/starlark"
)

// A flight booking as a Google calendar writes it: the description is HTML.
const ical_test_html = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Google Inc//Google Calendar 70.9054//EN\r\n" +
	"BEGIN:VEVENT\r\nUID:flight@google.com\r\nDTSTAMP:20260901T000000Z\r\n" +
	"DTSTART:20260915T090000Z\r\nDTEND:20260915T120000Z\r\nSUMMARY:Flight to Lisbon\r\n" +
	"DESCRIPTION:Class: Economy<br>PNR: <span class=\"record-locator-value\">IQSZXC<br>Seats: Pat 1F\\, Sam 1D</span><br>Fish &amp\\; chips\r\n" +
	"END:VEVENT\r\nEND:VCALENDAR\r\n"

// The same event with its description written as text, as Mochi writes it.
const ical_test_text = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Mochi//Calendars//EN\r\n" +
	"BEGIN:VEVENT\r\nUID:plain@mochi\r\nDTSTAMP:20260901T000000Z\r\n" +
	"DTSTART:20260915T090000Z\r\nDTEND:20260915T120000Z\r\nSUMMARY:Lunch\r\n" +
	"DESCRIPTION:Bring a < b and <3 to the table\r\n" +
	"END:VEVENT\r\nEND:VCALENDAR\r\n"

const ical_test_html_text = "Class: Economy\nPNR: IQSZXC\nSeats: Pat 1F, Sam 1D\nFish & chips"

func ical_test_event(t *testing.T, cal *ical.Calendar) *ical.Component {
	t.Helper()
	for _, child := range cal.Children {
		if child.Name == ical.CompEvent {
			return child
		}
	}
	t.Fatal("no event")
	return nil
}

func ical_test_value(t *testing.T, comp *ical.Component, name string) string {
	t.Helper()
	prop := comp.Props.Get(name)
	if prop == nil {
		return ""
	}
	text, err := prop.Text()
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return text
}

func TestHtmlText(t *testing.T) {
	cases := []struct{ html, text string }{
		{"Class: Economy<br>PNR: <span class=\"x\">IQSZXC<br/>Seat: 12F</span>", "Class: Economy\nPNR: IQSZXC\nSeat: 12F"},
		{"<p>One</p><p>Two</p>", "One\nTwo"},
		{"<div>Kept</div><script>alert(1)</script><style>p{}</style>", "Kept"},
		{"Tom &amp; Jerry&nbsp;&lt;3 &#8364;5", "Tom & Jerry <3 €5"},
		{"<b>a</b><br><br><br><br>b", "a\n\nb"},
		{"<!-- note --><i>shown</i>", "shown"},
	}
	for _, c := range cases {
		if got := html_text(c.html); got != c.text {
			t.Errorf("html_text(%q) = %q, want %q", c.html, got, c.text)
		}
	}
	for _, text := range []string{"a < b", "<3 you", "Meet at <the usual place>", "plain text"} {
		if html_is(text) {
			t.Errorf("html_is(%q) = true, want plain text", text)
		}
	}
	if !html_is("Line one<br>Line two") || !html_is("<P>Upper</P>") {
		t.Error("html_is missed real markup")
	}
}

// A description written as HTML is served as its text, the HTML beside it in
// X-ALT-DESC, and the calendar handed in is left as it was.
func TestIcalPlainServesAnHtmlDescriptionAsText(t *testing.T) {
	cal, err := ical_decode(ical_test_html)
	if err != nil {
		t.Fatal(err)
	}
	plain := ical_plain(cal)
	if plain == cal {
		t.Fatal("ical_plain returned the calendar unchanged")
	}
	event := ical_test_event(t, plain)
	if got := ical_test_value(t, event, ical.PropDescription); got != ical_test_html_text {
		t.Fatalf("DESCRIPTION = %q, want %q", got, ical_test_html_text)
	}
	alternative := event.Props.Get(ical_alternative)
	if alternative == nil {
		t.Fatal("no X-ALT-DESC")
	}
	if got := alternative.Params.Get(ical.ParamFormatType); got != "text/html" {
		t.Fatalf("FMTTYPE = %q", got)
	}
	original := "Class: Economy<br>PNR: <span class=\"record-locator-value\">IQSZXC<br>Seats: Pat 1F, Sam 1D</span><br>Fish &amp; chips"
	if got := ical_test_value(t, event, ical_alternative); got != original {
		t.Fatalf("X-ALT-DESC = %q, want the HTML as stored", got)
	}
	if got := ical_test_value(t, ical_test_event(t, cal), ical.PropDescription); got != original {
		t.Fatalf("the calendar passed in was changed: %q", got)
	}
	if ical_test_event(t, cal).Props.Get(ical_alternative) != nil {
		t.Fatal("the calendar passed in gained an X-ALT-DESC")
	}
}

// A description that is already text, with angle brackets that are not
// markup, goes out as it came, and the calendar is not copied.
func TestIcalPlainLeavesTextAlone(t *testing.T) {
	cal, err := ical_decode(ical_test_text)
	if err != nil {
		t.Fatal(err)
	}
	if ical_plain(cal) != cal {
		t.Fatal("a text description was rewritten")
	}
}

// A component that already carries its HTML in X-ALT-DESC keeps that one.
func TestIcalPlainKeepsAnExistingAlternative(t *testing.T) {
	text := strings.Replace(ical_test_html, "SUMMARY:", "X-ALT-DESC;FMTTYPE=text/html:<p>Kept</p>\r\nSUMMARY:", 1)
	cal, err := ical_decode(text)
	if err != nil {
		t.Fatal(err)
	}
	event := ical_test_event(t, ical_plain(cal))
	if n := len(event.Props.Values(ical_alternative)); n != 1 {
		t.Fatalf("%d X-ALT-DESC properties, want 1", n)
	}
	if got := ical_test_value(t, event, ical_alternative); got != "<p>Kept</p>" {
		t.Fatalf("X-ALT-DESC = %q, want the one the event carried", got)
	}
	if got := ical_test_value(t, event, ical.PropDescription); got != ical_test_html_text {
		t.Fatalf("DESCRIPTION = %q", got)
	}
}

func TestIcalPlainApi(t *testing.T) {
	plain := sl.NewBuiltin("mochi.ical.plain", api_ical_plain)
	thread := &sl.Thread{}
	for _, text := range []string{ical_test_text, "not a calendar"} {
		got, err := sl.Call(thread, plain, sl.Tuple{sl.String(text)}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if string(got.(sl.String)) != text {
			t.Fatalf("plain(%q...) changed the text", text[:10])
		}
	}
	got, err := sl.Call(thread, plain, sl.Tuple{sl.String(ical_test_html)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	cal, err := ical_decode(string(got.(sl.String)))
	if err != nil {
		t.Fatalf("plain answered unreadable text: %v", err)
	}
	event := ical_test_event(t, cal)
	if got := ical_test_value(t, event, ical.PropDescription); got != ical_test_html_text {
		t.Fatalf("DESCRIPTION = %q", got)
	}
	if event.Props.Get(ical_alternative) == nil {
		t.Fatal("no X-ALT-DESC")
	}
}

// Over CalDAV, what Thunderbird and phones read, an HTML description is
// served as text with the HTML in X-ALT-DESC, by a fetch and by a query
// alike, while the app keeps the HTML it was given.
func TestDavServesAnHtmlDescriptionAsText(t *testing.T) {
	fake := new_dav_fake_app("main")
	server := dav_test_server(t, "caldav", fake)
	ctx := context.Background()
	client, err := caldav.NewClient(nil, server.URL+"/people/caldav/")
	if err != nil {
		t.Fatal(err)
	}
	cal, err := ical.NewDecoder(strings.NewReader(ical_test_html)).Decode()
	if err != nil {
		t.Fatal(err)
	}
	object := "/people/caldav/fp1/calendars/main/flight.ics"
	if _, err := client.PutCalendarObject(ctx, object, cal); err != nil {
		t.Fatalf("put: %v", err)
	}
	if stored, _ := fake.objects["main"]["flight"]["ics"].(string); !strings.Contains(stored, "<br>") || strings.Contains(stored, ical_alternative) {
		t.Fatalf("stored text was rewritten: %q", stored)
	}
	got, err := client.GetCalendarObject(ctx, object)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	event := ical_test_event(t, got.Data)
	if text := ical_test_value(t, event, ical.PropDescription); text != ical_test_html_text {
		t.Fatalf("served DESCRIPTION = %q", text)
	}
	if !strings.Contains(ical_test_value(t, event, ical_alternative), "<span") {
		t.Fatal("served without the HTML in X-ALT-DESC")
	}
	query := &caldav.CalendarQuery{
		CompRequest: caldav.CalendarCompRequest{Name: ical.CompCalendar, AllProps: true, AllComps: true},
		CompFilter: caldav.CompFilter{Name: ical.CompCalendar, Comps: []caldav.CompFilter{{
			Name:  ical.CompEvent,
			Start: time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC),
			End:   time.Date(2026, 9, 16, 0, 0, 0, 0, time.UTC),
		}}},
	}
	hits, err := client.QueryCalendar(ctx, "/people/caldav/fp1/calendars/main/", query)
	if err != nil || len(hits) != 1 {
		t.Fatalf("query: %d %v", len(hits), err)
	}
	if text := ical_test_value(t, ical_test_event(t, hits[0].Data), ical.PropDescription); text != ical_test_html_text {
		t.Fatalf("queried DESCRIPTION = %q", text)
	}
}

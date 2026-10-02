// Mochi server: iCalendar API tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"fmt"
	"strings"
	"testing"
	"time"

	sl "go.starlark.net/starlark"
)

const ical_test_weekly = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Test//EN\r\n" +
	"BEGIN:VEVENT\r\nUID:w1\r\nDTSTAMP:20260901T000000Z\r\nDTSTART;TZID=Europe/London:20260901T090000\r\nDTEND;TZID=Europe/London:20260901T100000\r\nSUMMARY:Weekly\r\nRRULE:FREQ=WEEKLY\r\nEXDATE;TZID=Europe/London:20260915T090000\r\nEND:VEVENT\r\n" +
	"BEGIN:VEVENT\r\nUID:w1\r\nRECURRENCE-ID;TZID=Europe/London:20260922T090000\r\nDTSTAMP:20260901T000000Z\r\nDTSTART;TZID=Europe/London:20260922T100000\r\nDTEND;TZID=Europe/London:20260922T110000\r\nSUMMARY:Weekly (moved)\r\nEND:VEVENT\r\n" +
	"BEGIN:VEVENT\r\nUID:d1\r\nDTSTAMP:20260901T000000Z\r\nDTSTART;VALUE=DATE:20260910\r\nSUMMARY:Holiday\r\nEND:VEVENT\r\n" +
	"END:VCALENDAR\r\n"

func ical_test_thread() *sl.Thread {
	return &sl.Thread{Name: "test"}
}

func TestIcalInstancesExpandWithExceptionsAndOverrides(t *testing.T) {
	cal, err := ical_decode(ical_test_weekly)
	if err != nil {
		t.Fatal(err)
	}
	london, _ := time.LoadLocation("Europe/London")
	from := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	instances := ical_instances(cal, from, until, london, false)

	var got []string
	for _, item := range instances {
		m := item.(map[string]any)
		got = append(got, m["uid"].(string)+"@"+time.Unix(m["start"].(int64), 0).In(london).Format("01-02 15:04")+" "+m["summary"].(string))
	}
	want := []string{
		"w1@09-01 09:00 Weekly",
		"w1@09-08 09:00 Weekly",
		"d1@09-10 00:00 Holiday",
		"w1@09-22 10:00 Weekly (moved)",
		"w1@09-29 09:00 Weekly",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("instances:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	holiday := instances[2].(map[string]any)
	if holiday["allday"] != true || holiday["date"] != "2026-09-10" || holiday["finish"].(int64)-holiday["start"].(int64) != 86400 {
		t.Fatalf("all-day instance: %+v", holiday)
	}
	moved := instances[3].(map[string]any)
	if moved["exception"] != true || moved["recurring"] != true {
		t.Fatalf("override instance: %+v", moved)
	}

	// A range that starts inside an occurrence still sees it.
	partial := ical_instances(cal, time.Date(2026, 9, 1, 8, 30, 0, 0, time.UTC), time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC), london, false)
	if len(partial) != 1 {
		t.Fatalf("overlapping range: %d instances", len(partial))
	}
}

func TestIcalSummaryReadsTheShadowColumns(t *testing.T) {
	cal, err := ical_decode(ical_test_weekly)
	if err != nil {
		t.Fatal(err)
	}
	summary := ical_summary(cal)
	if summary["uid"] != "w1" || summary["summary"] != "Weekly" || summary["recurring"] != true || summary["finish"] != int64(0) || summary["allday"] != false {
		t.Fatalf("summary = %+v", summary)
	}
	if summary["start"] != time.Date(2026, 9, 1, 8, 0, 0, 0, time.UTC).Unix() {
		t.Fatalf("start = %v", summary["start"])
	}
}

func TestIcalParseAndFormatRoundTrip(t *testing.T) {
	parse := sl.NewBuiltin("mochi.ical.parse", api_ical_parse)
	tree, err := api_ical_parse(ical_test_thread(), parse, sl.Tuple{sl.String(ical_test_weekly)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	top := sl_decode(tree).(map[string]any)
	if top["name"] != "VCALENDAR" || len(top["components"].([]any)) != 3 {
		t.Fatalf("tree = %+v", top)
	}
	event := top["components"].([]any)[0].(map[string]any)
	var tzid any
	for _, p := range event["properties"].([]any) {
		m := p.(map[string]any)
		if m["name"] == "DTSTART" {
			tzid = m["params"].(map[string]any)["TZID"]
		}
	}
	if list, ok := tzid.([]any); !ok || len(list) != 1 || list[0] != "Europe/London" {
		t.Fatalf("DTSTART TZID = %v", tzid)
	}

	format := sl.NewBuiltin("mochi.ical.format", api_ical_format)
	text, err := api_ical_format(ical_test_thread(), format, sl.Tuple{tree}, nil)
	if err != nil {
		t.Fatal(err)
	}
	out := string(text.(sl.String))
	if !strings.Contains(out, "DTSTART;TZID=Europe/London:20260901T090000\r\n") || !strings.Contains(out, "RRULE:FREQ=WEEKLY\r\n") {
		t.Fatalf("formatted:\n%s", out)
	}
	if _, err := api_ical_parse(ical_test_thread(), parse, sl.Tuple{sl.String("not a calendar")}, nil); err != nil {
		t.Fatalf("unparsable text should yield None, not an error: %v", err)
	}

	instances := sl.NewBuiltin("mochi.ical.instances", api_ical_instances)
	value, err := api_ical_instances(ical_test_thread(), instances, sl.Tuple{sl.String(ical_test_weekly)}, []sl.Tuple{
		{sl.String("start"), sl.MakeInt64(time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC).Unix())},
		{sl.String("finish"), sl.MakeInt64(time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC).Unix())},
	})
	if err != nil {
		t.Fatal(err)
	}
	list := sl_decode(value).([]any)
	if len(list) != 1 || list[0].(map[string]any)["summary"] != "Weekly (moved)" {
		t.Fatalf("instances = %+v", list)
	}
}

func TestVcardFormat(t *testing.T) {
	format := sl.NewBuiltin("mochi.vcard.format", api_vcard_format)
	properties := sl.NewList([]sl.Value{sl_encode(map[string]any{"name": "FN", "params": map[string]any{}, "value": "Ada"})})
	value, err := api_vcard_format(ical_test_thread(), format, sl.Tuple{properties}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(value.(sl.String)), "FN:Ada\r\n") {
		t.Fatalf("formatted: %q", value)
	}
}

func TestTimeIcalFormat(t *testing.T) {
	stamp := time.Date(2026, 9, 18, 9, 30, 0, 0, time.UTC).Unix()
	local := sl.NewBuiltin("mochi.time.local", api_time_local)
	value, err := api_time_local(ical_test_thread(), local, sl.Tuple{sl.MakeInt64(stamp), sl.String("ical")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(value.(sl.String)) != "20260918T093000Z" {
		t.Fatalf("local ical = %q", value)
	}
	parse := sl.NewBuiltin("mochi.time.parse", api_time_parse)
	for _, c := range []struct {
		text string
		want int64
	}{
		{"20260918T093000Z", stamp},
		{"20260918", time.Date(2026, 9, 18, 0, 0, 0, 0, time.UTC).Unix()},
	} {
		value, err := api_time_parse(ical_test_thread(), parse, sl.Tuple{sl.String(c.text), sl.String("ical")}, nil)
		if err != nil {
			t.Fatal(err)
		}
		got, _ := value.(sl.Int).Int64()
		if got != c.want {
			t.Errorf("parse(%q) = %d, want %d", c.text, got, c.want)
		}
	}
	value, _ = api_time_parse(ical_test_thread(), parse, sl.Tuple{sl.String("tomorrow"), sl.String("ical")}, nil)
	if value != sl.None {
		t.Errorf("parse of a non-time should be None, got %v", value)
	}
}

func TestIcalInstancesStopOnARunawayRule(t *testing.T) {
	cal, err := ical_decode("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Test//EN\r\nBEGIN:VEVENT\r\nUID:r1\r\nDTSTAMP:20260901T000000Z\r\nDTSTART:19700101T000000Z\r\nDTEND:19700101T000001Z\r\nRRULE:FREQ=SECONDLY\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n")
	if err != nil {
		t.Fatal(err)
	}
	began := time.Now()
	ical_instances(cal, time.Time{}, time.Time{}, time.UTC, false)
	ical_instances(cal, time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC), time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC), time.UTC, false)
	if elapsed := time.Since(began); elapsed > 5*time.Second {
		t.Fatalf("expansion took %s", elapsed)
	}
	budget := ical_budget_query
	if !ical_expansion_heavy(cal, time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC), &budget) {
		t.Fatal("a secondly rule from 1970 was not reported heavy")
	}
	weekly, _ := ical_decode(ical_test_weekly)
	budget = ical_budget_query
	if ical_expansion_heavy(weekly, time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC), &budget) {
		t.Fatal("a weekly rule was reported heavy")
	}
}

func TestIcalSummaryApi(t *testing.T) {
	summary := sl.NewBuiltin("mochi.ical.summary", api_ical_summary)
	value, err := api_ical_summary(ical_test_thread(), summary, sl.Tuple{sl.String(ical_test_weekly)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	m := sl_decode(value).(map[string]any)
	if m["uid"] != "w1" || m["recurring"] != true || m["component"] != "VEVENT" {
		t.Fatalf("summary = %+v", m)
	}
	if value, _ := api_ical_summary(ical_test_thread(), summary, sl.Tuple{sl.String("nope")}, nil); value != sl.None {
		t.Fatalf("unparsable text should give None, got %v", value)
	}
	// A start the format cannot read is refused rather than stored as zero,
	// where the event would never appear in a listing.
	broken := strings.Replace(ical_test_weekly, "DTSTART;TZID=Europe/London:20260901T090000", "DTSTART:{start_ical}", 1)
	if broken == ical_test_weekly {
		t.Fatal("the fixture no longer carries the start the test breaks")
	}
	if value, _ := api_ical_summary(ical_test_thread(), summary, sl.Tuple{sl.String(broken)}, nil); value != sl.None {
		t.Fatalf("an unreadable start should give None, got %v", value)
	}
}

// An occurrence says it has a reminder when its own event carries one, so a
// changed occurrence without the series' reminder answers for itself.
func TestIcalInstancesCarryTheirOwnAlarm(t *testing.T) {
	text := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Test//EN\r\n" +
		"BEGIN:VEVENT\r\nUID:a1\r\nDTSTAMP:20260901T000000Z\r\nDTSTART:20260901T090000Z\r\nDTEND:20260901T100000Z\r\nSUMMARY:Standup\r\nRRULE:FREQ=WEEKLY;COUNT=3\r\n" +
		"BEGIN:VALARM\r\nACTION:DISPLAY\r\nDESCRIPTION:Standup\r\nTRIGGER:-PT15M\r\nEND:VALARM\r\nEND:VEVENT\r\n" +
		"BEGIN:VEVENT\r\nUID:a1\r\nRECURRENCE-ID:20260908T090000Z\r\nDTSTAMP:20260901T000000Z\r\nDTSTART:20260908T100000Z\r\nDTEND:20260908T110000Z\r\nSUMMARY:Standup (late)\r\nEND:VEVENT\r\n" +
		"BEGIN:VEVENT\r\nUID:b1\r\nDTSTAMP:20260901T000000Z\r\nDTSTART:20260902T090000Z\r\nDTEND:20260902T100000Z\r\nSUMMARY:Lunch\r\nEND:VEVENT\r\n" +
		"END:VCALENDAR\r\n"
	cal, err := ical_decode(text)
	if err != nil {
		t.Fatal(err)
	}
	instances := ical_instances(cal, time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), time.UTC, false)
	got := map[string]bool{}
	for _, item := range instances {
		m := item.(map[string]any)
		got[m["summary"].(string)+"@"+time.Unix(m["start"].(int64), 0).UTC().Format("01-02")] = m["alarm"] == true
	}
	want := map[string]bool{
		"Standup@09-01":        true,
		"Standup (late)@09-08": false,
		"Standup@09-15":        true,
		"Lunch@09-02":          false,
	}
	for key, alarm := range want {
		if value, ok := got[key]; !ok || value != alarm {
			t.Errorf("%s: alarm %v, want %v (instances %v)", key, value, alarm, got)
		}
	}
}

func TestIcalInstancesCarryTheirOwnColour(t *testing.T) {
	event := func(uid, colour string) string {
		line := ""
		if colour != "" {
			line = "COLOR:" + colour + "\r\n"
		}
		return "BEGIN:VEVENT\r\nUID:" + uid + "\r\nDTSTAMP:20260901T000000Z\r\nDTSTART:20260901T090000Z\r\nDTEND:20260901T100000Z\r\nSUMMARY:" + uid + "\r\n" + line + "END:VEVENT\r\n"
	}
	text := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Test//EN\r\n" +
		event("named", "Turquoise") +
		event("hex", "#FF8800") +
		event("short", "#0af") +
		event("unknown", "not-a-colour") +
		event("broken", "#12345g") +
		event("plain", "") +
		"END:VCALENDAR\r\n"
	cal, err := ical_decode(text)
	if err != nil {
		t.Fatal(err)
	}
	instances := ical_instances(cal, time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC), time.UTC, false)
	got := map[string]any{}
	for _, item := range instances {
		m := item.(map[string]any)
		got[m["summary"].(string)] = m["colour"]
	}
	want := map[string]any{
		"named":   "#40e0d0",
		"hex":     "#ff8800",
		"short":   "#00aaff",
		"unknown": nil,
		"broken":  nil,
		"plain":   nil,
	}
	for key, colour := range want {
		if value, ok := got[key]; !ok || value != colour {
			t.Errorf("%s: colour %v, want %v (instances %v)", key, value, colour, got)
		}
	}
}

func TestIcalTreeCarriesTextUnescaped(t *testing.T) {
	text := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Test//EN\r\n" +
		"BEGIN:VEVENT\r\nUID:a1\r\nDTSTAMP:20260901T000000Z\r\nDTSTART:20261001T090000Z\r\n" +
		"SUMMARY:Trailhead Lodge\\, Estes Park\r\n" +
		"LOCATION:Trailhead Lodge\\, 130 Stanley Ave\\; rear\r\n" +
		"DESCRIPTION:Payment taken\\nKing bed room\\\\suite\r\n" +
		"COMMENT:Lunch, Bob\r\n" +
		"CATEGORIES:Travel,Hotel\\, lodge\r\n" +
		"END:VEVENT\r\nEND:VCALENDAR\r\n"
	parse := sl.NewBuiltin("mochi.ical.parse", api_ical_parse)
	tree, err := api_ical_parse(ical_test_thread(), parse, sl.Tuple{sl.String(text)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	event := sl_decode(tree).(map[string]any)["components"].([]any)[0].(map[string]any)
	got := map[string]any{}
	for _, p := range event["properties"].([]any) {
		m := p.(map[string]any)
		got[m["name"].(string)] = m["value"]
	}
	want := map[string]string{
		"SUMMARY":     "Trailhead Lodge, Estes Park",
		"LOCATION":    "Trailhead Lodge, 130 Stanley Ave; rear",
		"DESCRIPTION": "Payment taken\nKing bed room\\suite",
		// Written loosely, without the backslash, and still read whole.
		"COMMENT": "Lunch, Bob",
		// A list keeps its written form: the comma separates its values.
		"CATEGORIES": "Travel,Hotel\\, lodge",
	}
	for name, value := range want {
		if got[name] != value {
			t.Errorf("%s = %q, want %q", name, got[name], value)
		}
	}

	format := sl.NewBuiltin("mochi.ical.format", api_ical_format)
	out, err := api_ical_format(ical_test_thread(), format, sl.Tuple{tree}, nil)
	if err != nil {
		t.Fatal(err)
	}
	written := string(out.(sl.String))
	for _, line := range []string{
		"SUMMARY:Trailhead Lodge\\, Estes Park\r\n",
		"LOCATION:Trailhead Lodge\\, 130 Stanley Ave\\; rear\r\n",
		"DESCRIPTION:Payment taken\\nKing bed room\\\\suite\r\n",
		"COMMENT:Lunch\\, Bob\r\n",
		"CATEGORIES:Travel,Hotel\\, lodge\r\n",
	} {
		if !strings.Contains(written, line) {
			t.Errorf("formatted text lacks %q:\n%s", line, written)
		}
	}

	cal, err := ical_decode(text)
	if err != nil {
		t.Fatal(err)
	}
	instances := ical_instances(cal, time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC), time.UTC, false)
	if len(instances) != 1 {
		t.Fatalf("instances = %v", instances)
	}
	instance := instances[0].(map[string]any)
	if instance["summary"] != "Trailhead Lodge, Estes Park" || instance["description"] != "Payment taken\nKing bed room\\suite" {
		t.Errorf("instance = %v", instance)
	}

	// A summary another client wrote without escaping its comma reads whole.
	loose, err := ical_decode(strings.Replace(text, "SUMMARY:Trailhead Lodge\\, Estes Park", "SUMMARY:Lunch, Bob", 1))
	if err != nil {
		t.Fatal(err)
	}
	instances = ical_instances(loose, time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC), time.UTC, false)
	if len(instances) != 1 || instances[0].(map[string]any)["summary"] != "Lunch, Bob" {
		t.Errorf("loose instances = %v", instances)
	}
}

func TestIcalFormatWritesALineBreakAsEscapedText(t *testing.T) {
	tree := map[string]any{"name": "VCALENDAR", "properties": []any{
		map[string]any{"name": "VERSION", "params": map[string]any{}, "value": "2.0"},
		map[string]any{"name": "PRODID", "params": map[string]any{}, "value": "-//Test//EN"},
	}, "components": []any{map[string]any{"name": "VEVENT", "properties": []any{
		map[string]any{"name": "UID", "params": map[string]any{}, "value": "a1"},
		map[string]any{"name": "DTSTAMP", "params": map[string]any{}, "value": "20260901T000000Z"},
		map[string]any{"name": "DTSTART", "params": map[string]any{}, "value": "20261001T090000Z"},
		map[string]any{"name": "SUMMARY", "params": map[string]any{}, "value": "Dinner, then drinks"},
		map[string]any{"name": "DESCRIPTION", "params": map[string]any{}, "value": "Line one\nLine two"},
	}, "components": []any{}}}}
	format := sl.NewBuiltin("mochi.ical.format", api_ical_format)
	out, err := api_ical_format(ical_test_thread(), format, sl.Tuple{sl_encode(tree)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	written := string(out.(sl.String))
	if !strings.Contains(written, "SUMMARY:Dinner\\, then drinks\r\n") || !strings.Contains(written, "DESCRIPTION:Line one\\nLine two\r\n") {
		t.Fatalf("formatted:\n%s", written)
	}
}

// Each occurrence carries its own component's alarms: the series' for its
// own occurrences, an override's for the one it changes, none for an override
// that has none, which replaces the occurrence whole.
func TestIcalInstancesCarryEachOccurrencesOwnAlarms(t *testing.T) {
	text := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Test//EN\r\n" +
		"BEGIN:VEVENT\r\nUID:alarms@test\r\nDTSTART;TZID=Europe/London:20260901T090000\r\nDTEND;TZID=Europe/London:20260901T100000\r\n" +
		"RRULE:FREQ=DAILY;COUNT=4\r\nSUMMARY:Daily\r\n" +
		"BEGIN:VALARM\r\nACTION:DISPLAY\r\nTRIGGER:-PT15M\r\nEND:VALARM\r\n" +
		"BEGIN:VALARM\r\nACTION:DISPLAY\r\nTRIGGER;RELATED=END:-PT5M\r\nEND:VALARM\r\n" +
		"BEGIN:VALARM\r\nACTION:DISPLAY\r\nTRIGGER;VALUE=DATE-TIME:20260901T070000Z\r\nEND:VALARM\r\n" +
		"BEGIN:VALARM\r\nACTION:DISPLAY\r\nTRIGGER:-PT15M\r\nEND:VALARM\r\n" +
		"END:VEVENT\r\n" +
		"BEGIN:VEVENT\r\nUID:alarms@test\r\nRECURRENCE-ID;TZID=Europe/London:20260902T090000\r\n" +
		"DTSTART;TZID=Europe/London:20260902T110000\r\nDTEND;TZID=Europe/London:20260902T120000\r\nSUMMARY:Own reminder\r\n" +
		"BEGIN:VALARM\r\nACTION:DISPLAY\r\nTRIGGER:-PT1H\r\nEND:VALARM\r\nEND:VEVENT\r\n" +
		"BEGIN:VEVENT\r\nUID:alarms@test\r\nRECURRENCE-ID;TZID=Europe/London:20260903T090000\r\n" +
		"DTSTART;TZID=Europe/London:20260903T090000\r\nDTEND;TZID=Europe/London:20260903T100000\r\nSUMMARY:No reminder\r\nEND:VEVENT\r\n" +
		"END:VCALENDAR\r\n"
	cal, err := ical_decode(text)
	if err != nil {
		t.Fatal(err)
	}
	london, _ := time.LoadLocation("Europe/London")
	series := []any{
		map[string]any{"related": "start", "offset": int64(-900)},
		map[string]any{"related": "end", "offset": int64(-300)},
		map[string]any{"at": time.Date(2026, 9, 1, 7, 0, 0, 0, time.UTC).Unix()},
	}
	want := map[string][]any{
		"09-01 Daily":        series,
		"09-02 Own reminder": {map[string]any{"related": "start", "offset": int64(-3600)}},
		"09-03 No reminder":  {},
		"09-04 Daily":        series,
	}
	instances := ical_instances(cal, time.Time{}, time.Time{}, london, true)
	if len(instances) != len(want) {
		t.Fatalf("%d occurrences, want %d", len(instances), len(want))
	}
	for _, item := range instances {
		m := item.(map[string]any)
		key := time.Unix(m["start"].(int64), 0).In(london).Format("01-02") + " " + m["summary"].(string)
		expected, ok := want[key]
		if !ok {
			t.Fatalf("unexpected occurrence %s", key)
		}
		if got := fmt.Sprint(m["alarms"]); got != fmt.Sprint(expected) {
			t.Errorf("%s alarms = %s, want %s", key, got, fmt.Sprint(expected))
		}
	}
	for _, item := range ical_instances(cal, time.Time{}, time.Time{}, london, false) {
		if _, ok := item.(map[string]any)["alarms"]; ok {
			t.Fatal("alarms listed without being asked for")
		}
	}
}

// An event Outlook or Exchange writes names its zone the Windows way, with
// the VTIMEZONE inline; the zone database knows only IANA names.
const ical_test_windows = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Microsoft Corporation//Outlook 16.0 MIMEDIR//EN\r\n" +
	"BEGIN:VTIMEZONE\r\nTZID:W. Europe Standard Time\r\nBEGIN:STANDARD\r\nDTSTART:16011028T030000\r\nRRULE:FREQ=YEARLY;BYDAY=-1SU;BYMONTH=10\r\nTZOFFSETFROM:+0200\r\nTZOFFSETTO:+0100\r\nEND:STANDARD\r\n" +
	"BEGIN:DAYLIGHT\r\nDTSTART:16010325T020000\r\nRRULE:FREQ=YEARLY;BYDAY=-1SU;BYMONTH=3\r\nTZOFFSETFROM:+0100\r\nTZOFFSETTO:+0200\r\nEND:DAYLIGHT\r\nEND:VTIMEZONE\r\n" +
	"BEGIN:VEVENT\r\nUID:o1\r\nDTSTAMP:20260101T000000Z\r\nDTSTART;TZID=W. Europe Standard Time:20260115T090000\r\nDTEND;TZID=W. Europe Standard Time:20260115T100000\r\nSUMMARY:Winter\r\nEND:VEVENT\r\n" +
	"END:VCALENDAR\r\n"

func ical_test_summary_start(t *testing.T, text string) Map {
	t.Helper()
	cal, err := ical_decode(text)
	if err != nil {
		t.Fatal(err)
	}
	return ical_summary(cal)
}

func TestIcalSummaryReadsAWindowsZoneAsItsIanaZone(t *testing.T) {
	winter := ical_test_summary_start(t, ical_test_windows)
	if winter == nil || winter["start"] != time.Date(2026, 1, 15, 8, 0, 0, 0, time.UTC).Unix() {
		t.Fatalf("winter start = %v, want 08:00 UTC (Europe/Berlin at +01:00)", winter)
	}
	summer := ical_test_summary_start(t, strings.ReplaceAll(ical_test_windows, "20260115T", "20260715T"))
	if summer == nil || summer["start"] != time.Date(2026, 7, 15, 7, 0, 0, 0, time.UTC).Unix() {
		t.Fatalf("summer start = %v, want 07:00 UTC (Europe/Berlin keeps daylight time)", summer)
	}
}

func TestIcalSummaryFallsBackToTheZonesOwnStandardOffset(t *testing.T) {
	text := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Test//EN\r\n" +
		"BEGIN:VTIMEZONE\r\nTZID:(UTC+05:30) Chennai\\, Kolkata\r\nBEGIN:STANDARD\r\nDTSTART:16010101T000000\r\nTZOFFSETFROM:+0530\r\nTZOFFSETTO:+0530\r\nEND:STANDARD\r\nEND:VTIMEZONE\r\n" +
		"BEGIN:VEVENT\r\nUID:c1\r\nDTSTAMP:20260101T000000Z\r\nDTSTART;TZID=\"(UTC+05:30) Chennai, Kolkata\":20260115T090000\r\nDTEND;TZID=\"(UTC+05:30) Chennai, Kolkata\":20260115T100000\r\nSUMMARY:Chennai\r\nEND:VEVENT\r\n" +
		"END:VCALENDAR\r\n"
	summary := ical_test_summary_start(t, text)
	if summary == nil || summary["start"] != time.Date(2026, 1, 15, 3, 30, 0, 0, time.UTC).Unix() {
		t.Fatalf("start = %v, want 03:30 UTC from the zone's +05:30", summary)
	}
}

func TestIcalSummaryStillRefusesAZoneNothingDefines(t *testing.T) {
	text := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Test//EN\r\n" +
		"BEGIN:VEVENT\r\nUID:n1\r\nDTSTAMP:20260101T000000Z\r\nDTSTART;TZID=Nowhere/Special:20260115T090000\r\nSUMMARY:Nowhere\r\nEND:VEVENT\r\n" +
		"END:VCALENDAR\r\n"
	if summary := ical_test_summary_start(t, text); summary != nil {
		t.Fatalf("summary = %v, want nil for a zone neither the database nor the text defines", summary)
	}
}

func TestIcalInstancesReportTheResolvedZoneAndLeaveTheTextAlone(t *testing.T) {
	cal, err := ical_decode(ical_test_windows)
	if err != nil {
		t.Fatal(err)
	}
	instances := ical_instances(cal, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC), time.UTC, false)
	if len(instances) != 1 {
		t.Fatalf("instances = %d, want 1", len(instances))
	}
	zone := instances[0].(map[string]any)["zone"].(map[string]any)
	if zone["start"] != "Europe/Berlin" || zone["finish"] != "Europe/Berlin" {
		t.Fatalf("zone = %v, want Europe/Berlin, a name clients can format in", zone)
	}
	if got := cal.Children[1].Props.Get("DTSTART").Params.Get("TZID"); got != "W. Europe Standard Time" {
		t.Fatalf("the calendar passed in now names %q; reading its times must not rewrite it", got)
	}
}

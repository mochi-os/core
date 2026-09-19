// Mochi server: iCalendar API tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
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
	instances := ical_instances(cal, from, until, london)

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
	partial := ical_instances(cal, time.Date(2026, 9, 1, 8, 30, 0, 0, time.UTC), time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC), london)
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
	ical_instances(cal, time.Time{}, time.Time{}, time.UTC)
	ical_instances(cal, time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC), time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC), time.UTC)
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

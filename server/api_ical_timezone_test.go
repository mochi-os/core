// Mochi server: VTIMEZONE components from the zone database.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"strings"
	"testing"
	"time"

	"github.com/emersion/go-ical"
	sl "go.starlark.net/starlark"
)

// A fixed "now", so the window the tests read is the same on every run.
var ical_timezone_test_now = time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)

func ical_timezone_block(t *testing.T, zone *ical.Component, name string) *ical.Component {
	t.Helper()
	for _, child := range zone.Children {
		if child.Name == name {
			return child
		}
	}
	t.Fatalf("no %s block in %s", name, ical_timezone_text(t, zone))
	return nil
}

func ical_timezone_text(t *testing.T, zone *ical.Component) string {
	t.Helper()
	cal := ical.NewCalendar()
	cal.Props.SetText(ical.PropVersion, "2.0")
	cal.Props.SetText(ical.PropProductID, "-//Test//EN")
	cal.Children = append(cal.Children, zone)
	text, err := ical_encode(cal)
	if err != nil {
		t.Fatal(err)
	}
	return text
}

func ical_timezone_value(t *testing.T, block *ical.Component, name string) string {
	t.Helper()
	value, err := block.Props.Text(name)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func TestIcalTimezoneWritesYearlyRulesForLondonAndNewYork(t *testing.T) {
	london, err := ical_timezone("Europe/London", ical_timezone_test_now)
	if err != nil {
		t.Fatal(err)
	}
	if got := ical_timezone_value(t, london, ical.PropTimezoneID); got != "Europe/London" {
		t.Errorf("TZID = %q", got)
	}
	daylight := ical_timezone_block(t, london, ical.CompTimezoneDaylight)
	standard := ical_timezone_block(t, london, ical.CompTimezoneStandard)
	// Clocks go forward on the last Sunday of March at 01:00 GMT and back on
	// the last Sunday of October at 02:00 BST.
	for _, c := range []struct {
		block          *ical.Component
		property, want string
	}{
		{daylight, ical.PropRecurrenceRule, "FREQ=YEARLY;BYMONTH=3;BYDAY=-1SU"},
		{daylight, ical.PropTimezoneOffsetFrom, "+0000"},
		{daylight, ical.PropTimezoneOffsetTo, "+0100"},
		{daylight, ical.PropTimezoneName, "BST"},
		{standard, ical.PropRecurrenceRule, "FREQ=YEARLY;BYMONTH=10;BYDAY=-1SU"},
		{standard, ical.PropTimezoneOffsetFrom, "+0100"},
		{standard, ical.PropTimezoneOffsetTo, "+0000"},
		{standard, ical.PropTimezoneName, "GMT"},
	} {
		if got := ical_timezone_value(t, c.block, c.property); got != c.want {
			t.Errorf("%s %s = %q, want %q", c.block.Name, c.property, got, c.want)
		}
	}
	if got := ical_timezone_value(t, daylight, ical.PropDateTimeStart); !strings.HasSuffix(got, "T010000") {
		t.Errorf("DAYLIGHT DTSTART = %q, want a 01:00 clock time", got)
	}
	if got := ical_timezone_value(t, standard, ical.PropDateTimeStart); !strings.HasSuffix(got, "T020000") {
		t.Errorf("STANDARD DTSTART = %q, want a 02:00 clock time", got)
	}
	if london.Children[0].Name != ical.CompTimezoneStandard {
		t.Errorf("standard time is not written first")
	}

	newYork, err := ical_timezone("America/New_York", ical_timezone_test_now)
	if err != nil {
		t.Fatal(err)
	}
	if got := ical_timezone_value(t, ical_timezone_block(t, newYork, ical.CompTimezoneDaylight), ical.PropRecurrenceRule); got != "FREQ=YEARLY;BYMONTH=3;BYDAY=2SU" {
		t.Errorf("New York DAYLIGHT rule = %q", got)
	}
	if got := ical_timezone_value(t, ical_timezone_block(t, newYork, ical.CompTimezoneStandard), ical.PropRecurrenceRule); got != "FREQ=YEARLY;BYMONTH=11;BYDAY=1SU" {
		t.Errorf("New York STANDARD rule = %q", got)
	}
}

func TestIcalTimezoneWritesOneBlockForAZoneWithoutDaylightTime(t *testing.T) {
	for _, c := range []struct{ zone, offset, name string }{
		{"Asia/Tokyo", "+0900", "JST"},
		{"Asia/Kolkata", "+0530", "IST"},
		{"Etc/GMT+5", "-0500", "-05"},
	} {
		zone, err := ical_timezone(c.zone, ical_timezone_test_now)
		if err != nil {
			t.Fatal(err)
		}
		if len(zone.Children) != 1 || zone.Children[0].Name != ical.CompTimezoneStandard {
			t.Fatalf("%s: %s", c.zone, ical_timezone_text(t, zone))
		}
		block := zone.Children[0]
		if got := ical_timezone_value(t, block, ical.PropTimezoneOffsetTo); got != c.offset {
			t.Errorf("%s offset = %q, want %q", c.zone, got, c.offset)
		}
		if got := ical_timezone_value(t, block, ical.PropTimezoneName); got != c.name {
			t.Errorf("%s name = %q, want %q", c.zone, got, c.name)
		}
		if block.Props.Get(ical.PropRecurrenceRule) != nil {
			t.Errorf("%s carries a rule", c.zone)
		}
	}
}

func TestIcalRuleFallsBackToDatesWhenTransitionsFollowNoRule(t *testing.T) {
	at := func(year int, month time.Month, day int) ical_transition {
		return ical_transition{at: time.Date(year, month, day, 2, 0, 0, 0, time.UTC), from: 0, to: 3600}
	}
	// Second Sundays of March: a rule.
	if got := ical_rule([]ical_transition{at(2025, 3, 9), at(2026, 3, 8), at(2027, 3, 14)}); got != "FREQ=YEARLY;BYMONTH=3;BYDAY=2SU" {
		t.Errorf("second Sundays: %q", got)
	}
	// Last Sundays, some of them also a fourth Sunday: still a rule.
	if got := ical_rule([]ical_transition{at(2025, 3, 30), at(2026, 3, 29), at(2027, 3, 28)}); got != "FREQ=YEARLY;BYMONTH=3;BYDAY=-1SU" {
		t.Errorf("last Sundays: %q", got)
	}
	// A Sunday chosen by date rather than by count is no rule: the first
	// Sunday on or after 2 April is the first Sunday of the month until 2029,
	// when it is the second.
	if got := ical_rule([]ical_transition{at(2025, 4, 6), at(2026, 4, 5), at(2027, 4, 4), at(2028, 4, 2), at(2029, 4, 8)}); got != "" {
		t.Errorf("the first Sunday on or after a date: %q, want none", got)
	}
	if got := ical_rule([]ical_transition{at(2025, 3, 9)}); got != "" {
		t.Errorf("one transition: %q, want none", got)
	}

	// Through the builder: explicit dates when a zone's changes follow no
	// rule. Chile moves its clocks on the first Sunday on or after a date, so
	// the count from the start of the month varies year by year.
	zone, err := ical_timezone("America/Santiago", ical_timezone_test_now)
	if err != nil {
		t.Fatal(err)
	}
	for _, block := range zone.Children {
		if block.Props.Get(ical.PropRecurrenceRule) != nil {
			if block.Props.Get(ical.PropRecurrenceDates) != nil {
				t.Errorf("%s carries both a rule and dates", block.Name)
			}
			continue
		}
		dates := ical_timezone_value(t, block, ical.PropRecurrenceDates)
		if strings.Count(dates, ",") < 5 {
			t.Errorf("%s carries neither a rule nor a run of dates: %q", block.Name, dates)
		}
	}
}

func TestIcalTimezoneEncodesAndExpandsWithAnEvent(t *testing.T) {
	zone, err := ical_timezone("Europe/London", ical_timezone_test_now)
	if err != nil {
		t.Fatal(err)
	}
	text := ical_timezone_text(t, zone)
	if !strings.Contains(text, "BEGIN:VTIMEZONE\r\nTZID:Europe/London") || !strings.Contains(text, "BEGIN:DAYLIGHT") {
		t.Fatalf("encoded:\n%s", text)
	}
	// An event beside it still expands at the instant its zone says.
	object := strings.Replace(text, "END:VCALENDAR", "BEGIN:VEVENT\r\nUID:z1\r\nDTSTAMP:20260901T000000Z\r\nDTSTART;TZID=Europe/London:20260925T100000\r\nDTEND;TZID=Europe/London:20260925T110000\r\nSUMMARY:Zoned\r\nEND:VEVENT\r\nEND:VCALENDAR", 1)
	cal, err := ical_decode(object)
	if err != nil {
		t.Fatal(err)
	}
	instances := ical_instances(cal, time.Date(2026, 9, 24, 0, 0, 0, 0, time.UTC), time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC), time.UTC)
	if len(instances) != 1 {
		t.Fatalf("instances: %d", len(instances))
	}
	if got := time.Unix(instances[0].(map[string]any)["start"].(int64), 0).UTC().Format("15:04"); got != "09:00" {
		t.Errorf("10:00 London expanded to %s UTC, want 09:00", got)
	}
}

func TestIcalTimezoneBuiltinAnswersNoneForAnUnknownZone(t *testing.T) {
	builtin := sl.NewBuiltin("mochi.ical.timezone", api_ical_timezone)
	value, err := api_ical_timezone(&sl.Thread{Name: "test"}, builtin, sl.Tuple{sl.String("Nowhere/Invalid")}, nil)
	if err != nil || value != sl.None {
		t.Errorf("unknown zone: %v, %v", value, err)
	}
	value, err = api_ical_timezone(&sl.Thread{Name: "test"}, builtin, sl.Tuple{sl.String("Europe/London")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	tree := value.(*sl.Dict)
	name, _, _ := tree.Get(sl.String("name"))
	if name != sl.String("VTIMEZONE") {
		t.Errorf("tree: %v", tree)
	}
}

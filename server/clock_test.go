// Mochi server: tests for times and dates as a user reads them.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	sl "go.starlark.net/starlark"
)

// testdata/clock.json is what ICU writes for each language, from the same run
// of build/scripts/clock.mjs that wrote the table: its 24-hour and 12-hour
// clocks at four times, and its long date, across a century for the calendars
// converted here.
type clock_vectors struct {
	Vectors map[string]struct {
		Full   [][2]any `json:"full"`
		Twelve [][2]any `json:"twelve"`
		Day    [][2]any `json:"day"`
	} `json:"vectors"`
}

func clock_test_vectors(t *testing.T) clock_vectors {
	data, err := os.ReadFile("testdata/clock.json")
	if err != nil {
		t.Fatal(err)
	}
	var v clock_vectors
	if err := json.Unmarshal(data, &v); err != nil {
		t.Fatal(err)
	}
	return v
}

func TestClockWritesWhatICUWritesForEveryLanguage(t *testing.T) {
	v := clock_test_vectors(t)
	if len(v.Vectors) != len(clock_languages) {
		t.Fatalf("%d languages have vectors, %d have formats", len(v.Vectors), len(clock_languages))
	}
	for tag, cases := range v.Vectors {
		for _, c := range cases.Full {
			at := time.Unix(int64(c[0].(float64)), 0).UTC()
			if got := time_clock(at, tag, "24h"); got != c[1].(string) {
				t.Errorf("%s at %s on a 24-hour clock: %q, ICU writes %q", tag, at.Format("15:04"), got, c[1])
			}
		}
		for _, c := range cases.Twelve {
			at := time.Unix(int64(c[0].(float64)), 0).UTC()
			if got := time_clock(at, tag, "12h"); got != c[1].(string) {
				t.Errorf("%s at %s on a 12-hour clock: %q, ICU writes %q", tag, at.Format("15:04"), got, c[1])
			}
		}
		for _, c := range cases.Day {
			at := time.Unix(int64(c[0].(float64)), 0).UTC()
			if got := time_day(at, tag, "auto"); got != c[1].(string) {
				t.Errorf("%s on %s: %q, ICU writes %q", tag, at.Format("2006-01-02"), got, c[1])
			}
		}
	}
}

// Every language core has labels for has formats of its own, so a language
// added without running the generator fails here rather than reading English.
func TestClockCoversEveryLanguage(t *testing.T) {
	entries, err := os.ReadDir("labels")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		tag := strings.TrimSuffix(e.Name(), ".conf")
		if _, ok := clock_languages[tag]; !ok {
			t.Errorf("%s has labels but no clock and date formats: run node build/scripts/clock.mjs", tag)
		}
	}
}

func TestClockFollowsThePreference(t *testing.T) {
	afternoon := time.Date(2026, 9, 29, 15, 4, 0, 0, time.UTC)
	midnight := time.Date(2026, 9, 29, 0, 30, 0, 0, time.UTC)
	cases := []struct {
		language, preference string
		at                   time.Time
		want                 string
	}{
		{"en", "auto", afternoon, "15:04"},
		{"en", "auto", midnight, "00:30"},
		{"en-us", "auto", afternoon, "3:04 PM"},
		{"en-us", "auto", midnight, "12:30 AM"},
		{"en-us", "24h", afternoon, "15:04"},
		{"en", "12h", afternoon, "03:04 pm"},
		{"de", "12h", midnight, "12:30 AM"},
		{"da", "auto", afternoon, "15.04"},
		{"fr-ca", "auto", afternoon, "15 h 04"},
		{"ja", "12h", afternoon, "午後3:04"},
		{"ko", "auto", afternoon, "PM 3:04"},
		{"zh-hant", "auto", afternoon, "下午3:04"},
		// A regional variant with no formats of its own reads its parent's.
		{"en-ph", "auto", afternoon, "3:04 PM"},
		{"xx", "auto", afternoon, "15:04"},
	}
	for _, c := range cases {
		if got := time_clock(c.at, c.language, c.preference); got != c.want {
			t.Errorf("%s with %s at %s: %q, want %q", c.language, c.preference, c.at.Format("15:04"), got, c.want)
		}
	}
}

func TestDayFollowsThePreference(t *testing.T) {
	at := time.Date(2026, 9, 29, 15, 4, 0, 0, time.UTC)
	cases := []struct {
		language, preference, want string
	}{
		{"en", "auto", "29 September 2026"},
		{"en-us", "auto", "September 29, 2026"},
		{"de", "auto", "29. September 2026"},
		{"fa", "auto", "۷ مهر ۱۴۰۵"},
		{"th", "auto", "29 กันยายน 2569"},
		{"en", "YYYY-MM-DD", "2026-09-29"},
		{"en", "DD/MM/YYYY", "29/09/2026"},
		{"de", "DD.MM.YYYY", "29.09.2026"},
		{"en-us", "MM/DD/YYYY", "09/29/2026"},
		{"de", "D MMM YYYY", "29 Sep 2026"},
		// The numbered preferences are Gregorian whatever the language's own
		// calendar, as the calendar shows them.
		{"fa", "YYYY-MM-DD", "2026-09-29"},
		// A language with borrowed formats reads the date in numbers.
		{"gn", "auto", "2026-09-29"},
		{"gn", "D MMM YYYY", "2026-09-29"},
	}
	for _, c := range cases {
		if got := time_day(at, c.language, c.preference); got != c.want {
			t.Errorf("%s with %s: %q, want %q", c.language, c.preference, got, c.want)
		}
	}
}

func TestTimeLocalReadsTheClockAndDayAsTheUserDoes(t *testing.T) {
	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("user", timezone_test_user(map[string]string{"timezone": "Europe/London", "language": "en-us"}))
	local := sl.NewBuiltin("mochi.time.local", api_time_local)
	stamp := int64(1_790_694_240) // 2026-09-29 15:04 UTC, 16:04 in London
	call := func(format string, kwargs ...sl.Tuple) string {
		value, err := api_time_local(thread, local, sl.Tuple{sl.MakeInt64(stamp), sl.String(format)}, kwargs)
		if err != nil {
			t.Fatal(err)
		}
		return string(value.(sl.String))
	}
	if got := call("clock"); got != "4:04 PM" {
		t.Errorf("clock in the user's zone and language: %q, want 4:04 PM", got)
	}
	if got := call("clock", sl.Tuple{sl.String("timezone"), sl.String("Asia/Tokyo")}); got != "12:04 AM" {
		t.Errorf("clock in an explicit zone: %q, want 12:04 AM", got)
	}
	if got := call("day"); got != "September 29, 2026" {
		t.Errorf("day: %q, want September 29, 2026", got)
	}
	thread.SetLocal("user", timezone_test_user(map[string]string{"timezone": "Europe/London", "language": "en-us", "time_format": "24h", "date_format": "DD/MM/YYYY"}))
	if got := call("clock"); got != "16:04" {
		t.Errorf("clock with a 24-hour preference: %q, want 16:04", got)
	}
	if got := call("day"); got != "29/09/2026" {
		t.Errorf("day with a date format preference: %q, want 29/09/2026", got)
	}
	if got := call("time"); got != "16:04:00" {
		t.Errorf("the time format is unchanged: %q, want 16:04:00", got)
	}
}

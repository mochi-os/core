// Mochi server: the zone a user's clocks read in, and the shell's report of it.
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

	sl "go.starlark.net/starlark"
)

// A user whose preferences are only in memory: user_preference_get reads the
// map, so no user database is touched.
func timezone_test_user(preferences map[string]string) *User {
	return &User{UID: "tz-test", Preferences: preferences}
}

func TestUserTimezoneFollowsTheDeviceWhenAuto(t *testing.T) {
	cases := []struct {
		name        string
		preferences map[string]string
		want        string
	}{
		{"auto follows the reported zone", map[string]string{"timezone": "auto", "last_timezone": "Asia/Tokyo"}, "Asia/Tokyo"},
		{"unset follows the reported zone", map[string]string{"last_timezone": "Europe/London"}, "Europe/London"},
		{"a chosen zone beats the reported one", map[string]string{"timezone": "Asia/Tokyo", "last_timezone": "Europe/London"}, "Asia/Tokyo"},
		{"auto with nothing reported is UTC", map[string]string{"timezone": "auto"}, "UTC"},
		{"an unloadable reported zone is UTC", map[string]string{"timezone": "auto", "last_timezone": "Nowhere/Invalid"}, "UTC"},
		{"nothing at all is UTC", map[string]string{}, "UTC"},
	}
	for _, c := range cases {
		if got := user_timezone(timezone_test_user(c.preferences)).String(); got != c.want {
			t.Errorf("%s: user_timezone = %q, want %q", c.name, got, c.want)
		}
	}
	if got := user_timezone(nil).String(); got != "UTC" {
		t.Errorf("no user: user_timezone = %q, want UTC", got)
	}
	// 2023-11-14 22:13:20 UTC is 07:13 the next day in Tokyo.
	user := timezone_test_user(map[string]string{"timezone": "auto", "last_timezone": "Asia/Tokyo"})
	if got, want := time_local(user, 1_700_000_000), "2023-11-15 07:13:20"; got != want {
		t.Errorf("time_local through the reported zone = %q, want %q", got, want)
	}
}

func TestTimeLocalTakesAnExplicitZone(t *testing.T) {
	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("user", timezone_test_user(map[string]string{"timezone": "Europe/London"}))
	local := sl.NewBuiltin("mochi.time.local", api_time_local)
	stamp := int64(1_700_000_000) // 22:13:20 UTC, 17:13 in New York
	call := func(kwargs ...sl.Tuple) string {
		value, err := api_time_local(thread, local, sl.Tuple{sl.MakeInt64(stamp), sl.String("time")}, kwargs)
		if err != nil {
			t.Fatal(err)
		}
		return string(value.(sl.String))
	}
	if got := call(); got != "22:13:20" {
		t.Errorf("the user's zone: %q, want 22:13:20", got)
	}
	if got := call(sl.Tuple{sl.String("timezone"), sl.String("America/New_York")}); got != "17:13:20" {
		t.Errorf("an explicit zone: %q, want 17:13:20", got)
	}
	if got := call(sl.Tuple{sl.String("timezone"), sl.String("Nowhere/Invalid")}); got != "22:13:20" {
		t.Errorf("an unloadable zone keeps the user's: %q, want 22:13:20", got)
	}
	if got := call(sl.Tuple{sl.String("timezone"), sl.String("")}); got != "22:13:20" {
		t.Errorf("an empty zone keeps the user's: %q, want 22:13:20", got)
	}
	if _, err := api_time_local(thread, local, sl.Tuple{sl.MakeInt64(stamp)}, []sl.Tuple{{sl.String("zone"), sl.String("UTC")}}); err == nil {
		t.Error("an unknown keyword is accepted")
	}
}

func TestShellTimezoneReadsAValidZoneFromTheBody(t *testing.T) {
	cases := []struct{ body, want string }{
		{`{"timezone":"Europe/London"}`, "Europe/London"},
		{`{"timezone":"America/Argentina/Buenos_Aires"}`, "America/Argentina/Buenos_Aires"},
		{`{"timezone":"Nowhere/Invalid"}`, ""},
		{`{"timezone":"Local"}`, ""},
		{`{"timezone":""}`, ""},
		{`{}`, ""},
		{``, ""},
		{`not json`, ""},
	}
	for _, c := range cases {
		if got := shell_timezone(strings.NewReader(c.body)); got != c.want {
			t.Errorf("shell_timezone(%q) = %q, want %q", c.body, got, c.want)
		}
	}
	if got := shell_timezone(nil); got != "" {
		t.Errorf("shell_timezone(nil) = %q, want empty", got)
	}
}

const ical_test_zones = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Test//EN\r\n" +
	"BEGIN:VEVENT\r\nUID:flight\r\nDTSTAMP:20260901T000000Z\r\nDTSTART;TZID=Europe/London:20260925T100000\r\nDTEND;TZID=America/New_York:20260925T130000\r\nSUMMARY:Flight\r\nEND:VEVENT\r\n" +
	"BEGIN:VEVENT\r\nUID:utc\r\nDTSTAMP:20260901T000000Z\r\nDTSTART:20260925T142500Z\r\nDTEND:20260926T001500Z\r\nSUMMARY:Google\r\nEND:VEVENT\r\n" +
	"BEGIN:VEVENT\r\nUID:lasting\r\nDTSTAMP:20260901T000000Z\r\nDTSTART;TZID=Asia/Tokyo:20260925T090000\r\nDURATION:PT1H\r\nSUMMARY:Lasting\r\nEND:VEVENT\r\n" +
	"BEGIN:VEVENT\r\nUID:floating\r\nDTSTAMP:20260901T000000Z\r\nDTSTART:20260925T090000\r\nDTEND:20260925T100000\r\nSUMMARY:Floating\r\nEND:VEVENT\r\n" +
	"BEGIN:VEVENT\r\nUID:day\r\nDTSTAMP:20260901T000000Z\r\nDTSTART;VALUE=DATE:20260925\r\nSUMMARY:Day\r\nEND:VEVENT\r\n" +
	"END:VCALENDAR\r\n"

func TestIcalInstancesCarryEachEndsZone(t *testing.T) {
	cal, err := ical_decode(ical_test_zones)
	if err != nil {
		t.Fatal(err)
	}
	london, _ := time.LoadLocation("Europe/London")
	from := time.Date(2026, 9, 24, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC)
	zones := map[string][2]string{}
	var day map[string]any
	for _, item := range ical_instances(cal, from, until, london) {
		m := item.(map[string]any)
		uid := m["uid"].(string)
		if uid == "day" {
			day = m
			continue
		}
		zone, ok := m["zone"].(map[string]any)
		if !ok {
			t.Fatalf("%s carries no zone: %+v", uid, m)
		}
		zones[uid] = [2]string{zone["start"].(string), zone["finish"].(string)}
	}
	want := map[string][2]string{
		"flight":   {"Europe/London", "America/New_York"},
		"utc":      {"", ""},
		"lasting":  {"Asia/Tokyo", "Asia/Tokyo"},
		"floating": {"", ""},
	}
	for uid, w := range want {
		if zones[uid] != w {
			t.Errorf("%s: zone = %v, want %v", uid, zones[uid], w)
		}
	}
	if day == nil {
		t.Fatal("the all-day event was not expanded")
	}
	if _, present := day["zone"]; present {
		t.Errorf("an all-day occurrence carries a zone: %+v", day)
	}
	// The flight's instants are what its two zones say: 10:00 London is
	// 09:00 UTC, 13:00 New York is 17:00 UTC.
	for _, item := range ical_instances(cal, from, until, london) {
		m := item.(map[string]any)
		if m["uid"] == "flight" {
			if got := time.Unix(m["start"].(int64), 0).UTC().Format("15:04"); got != "09:00" {
				t.Errorf("flight start %s, want 09:00 UTC", got)
			}
			if got := time.Unix(m["finish"].(int64), 0).UTC().Format("15:04"); got != "17:00" {
				t.Errorf("flight finish %s, want 17:00 UTC", got)
			}
		}
	}
}

func TestIcalInstancesReadFloatingTimesInTheReportedZone(t *testing.T) {
	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("user", timezone_test_user(map[string]string{"timezone": "auto", "last_timezone": "Asia/Tokyo"}))
	builtin := sl.NewBuiltin("mochi.ical.instances", api_ical_instances)
	value, err := api_ical_instances(thread, builtin, sl.Tuple{sl.String(ical_test_zones)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// A Go slice reaches Starlark as a tuple.
	list := value.(sl.Tuple)
	for i := 0; i < list.Len(); i++ {
		m := list.Index(i).(*sl.Dict)
		uid, _, _ := m.Get(sl.String("uid"))
		if uid != sl.String("floating") {
			continue
		}
		start, _, _ := m.Get(sl.String("start"))
		seconds, _ := start.(sl.Int).Int64()
		// 09:00 floating, read in Tokyo, is 00:00 UTC.
		if got := time.Unix(seconds, 0).UTC().Format("2006-01-02 15:04"); got != "2026-09-25 00:00" {
			t.Errorf("floating start read as %s, want 2026-09-25 00:00 UTC", got)
		}
		return
	}
	t.Fatal("the floating event was not expanded")
}

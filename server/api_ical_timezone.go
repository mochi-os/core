// Mochi server: a VTIMEZONE component from the zone database.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/emersion/go-ical"
	sl "go.starlark.net/starlark"
)

// The years a VTIMEZONE describes: the rule in force now and its near future,
// which is what a client applies to the events it holds. Transitions before
// the window are not the zone's current rule and would only mislead a client
// that reads the block literally.
const (
	ical_timezone_before = 1
	ical_timezone_after  = 10
)

// ical_transition is one change of offset in a zone: the instant, the offsets
// either side in seconds east of UTC, the abbreviation and whether daylight
// time is in force after it.
type ical_transition struct {
	at   time.Time
	from int
	to   int
	name string
	dst  bool
}

// ical_transitions finds a zone's offset changes between two instants by
// stepping a day at a time and narrowing each change to the second.
func ical_transitions(loc *time.Location, from, until time.Time) []ical_transition {
	var out []ical_transition
	offset := func(t time.Time) int {
		_, o := t.In(loc).Zone()
		return o
	}
	previous := from
	for day := from.AddDate(0, 0, 1); !day.After(until); day = day.AddDate(0, 0, 1) {
		if offset(day) == offset(previous) {
			previous = day
			continue
		}
		low, high := previous, day
		for high.Sub(low) > time.Second {
			middle := low.Add(high.Sub(low) / 2)
			if offset(middle) == offset(low) {
				low = middle
			} else {
				high = middle
			}
		}
		after := high.In(loc)
		name, _ := after.Zone()
		out = append(out, ical_transition{at: high, from: offset(low), to: offset(high), name: name, dst: after.IsDST()})
		previous = day
	}
	return out
}

// ical_offset writes seconds east of UTC as iCalendar's +hhmm.
func ical_offset(seconds int) string {
	sign := "+"
	if seconds < 0 {
		sign = "-"
		seconds = -seconds
	}
	return fmt.Sprintf("%s%02d%02d", sign, seconds/3600, seconds%3600/60)
}

// ical_local is the wall-clock time at an instant in a fixed offset, in the
// form a VTIMEZONE's DTSTART and RDATE take: the moment the change happens,
// read on the clock the zone kept before it.
func ical_local(at time.Time, offset int) string {
	return at.In(time.FixedZone("", offset)).Format("20060102T150405")
}

// ical_rule is the yearly rule a run of transitions follows, as an RRULE
// value, or "" when they follow none: every one must fall in the same month,
// on the same weekday, at the same clock time, and be the same one of those
// weekdays in the month, counted from the start or, for a last one, the end.
func ical_rule(transitions []ical_transition) string {
	if len(transitions) < 2 {
		return ""
	}
	var month time.Month
	var weekday time.Weekday
	var clock string
	ordinal := 0
	for i, t := range transitions {
		local := t.at.In(time.FixedZone("", t.from))
		nth := (local.Day()-1)/7 + 1
		last := local.AddDate(0, 0, 7).Month() != local.Month()
		position := nth
		if last {
			position = -1
		}
		if i == 0 {
			month, weekday, clock, ordinal = local.Month(), local.Weekday(), local.Format("15:04:05"), position
			continue
		}
		if local.Month() != month || local.Weekday() != weekday || local.Format("15:04:05") != clock {
			return ""
		}
		// A transition that is both the nth weekday and the last one keeps
		// either reading alive; the run decides which.
		if position != ordinal && !(last && nth == ordinal) && !(ordinal == -1 && last) {
			return ""
		}
		if ordinal == -1 && !last {
			return ""
		}
	}
	days := []string{"SU", "MO", "TU", "WE", "TH", "FR", "SA"}
	return fmt.Sprintf("FREQ=YEARLY;BYMONTH=%d;BYDAY=%d%s", int(month), ordinal, days[weekday])
}

// ical_timezone builds the VTIMEZONE for a zone: a STANDARD or DAYLIGHT block
// for each kind of transition the zone makes in the window, described by a
// yearly rule where its transitions follow one and by explicit dates
// otherwise, or a single STANDARD block for a zone that keeps one offset.
func ical_timezone(name string, now time.Time) (*ical.Component, error) {
	if name == "" || name == "Local" {
		return nil, fmt.Errorf("no such zone %q", name)
	}
	loc, err := time.LoadLocation(name)
	if err != nil {
		return nil, err
	}
	zone := ical.NewComponent(ical.CompTimezone)
	zone.Props.SetText(ical.PropTimezoneID, name)

	from := time.Date(now.Year()-ical_timezone_before, 1, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(now.Year()+ical_timezone_after, 12, 31, 0, 0, 0, 0, time.UTC)
	transitions := ical_transitions(loc, from, until)
	if len(transitions) == 0 {
		abbreviation, offset := now.In(loc).Zone()
		block := ical.NewComponent(ical.CompTimezoneStandard)
		block.Props.SetText(ical.PropDateTimeStart, "19700101T000000")
		block.Props.SetText(ical.PropTimezoneOffsetFrom, ical_offset(offset))
		block.Props.SetText(ical.PropTimezoneOffsetTo, ical_offset(offset))
		block.Props.SetText(ical.PropTimezoneName, abbreviation)
		zone.Children = append(zone.Children, block)
		return zone, nil
	}

	// One block per kind of change, in the order the kinds first appear,
	// standard time first among equals.
	type kind struct {
		from, to int
		name     string
		dst      bool
	}
	groups := map[kind][]ical_transition{}
	var kinds []kind
	for _, t := range transitions {
		k := kind{t.from, t.to, t.name, t.dst}
		if _, seen := groups[k]; !seen {
			kinds = append(kinds, k)
		}
		groups[k] = append(groups[k], t)
	}
	sort.SliceStable(kinds, func(i, j int) bool { return !kinds[i].dst && kinds[j].dst })
	for _, k := range kinds {
		run := groups[k]
		component := ical.CompTimezoneStandard
		if k.dst {
			component = ical.CompTimezoneDaylight
		}
		block := ical.NewComponent(component)
		block.Props.SetText(ical.PropDateTimeStart, ical_local(run[0].at, run[0].from))
		block.Props.SetText(ical.PropTimezoneOffsetFrom, ical_offset(k.from))
		block.Props.SetText(ical.PropTimezoneOffsetTo, ical_offset(k.to))
		block.Props.SetText(ical.PropTimezoneName, k.name)
		if rule := ical_rule(run); rule != "" {
			block.Props.SetText(ical.PropRecurrenceRule, rule)
		} else if len(run) > 1 {
			var dates []string
			for _, t := range run[1:] {
				dates = append(dates, ical_local(t.at, t.from))
			}
			block.Props.SetText(ical.PropRecurrenceDates, strings.Join(dates, ","))
		}
		zone.Children = append(zone.Children, block)
	}
	return zone, nil
}

// mochi.ical.timezone(name) -> dict | None: The VTIMEZONE component for an
// IANA zone, built from the zone database: the rule in force this year and
// the next ten, as a yearly rule where the zone's changes follow one and as
// explicit dates otherwise, or one STANDARD block for a zone without daylight
// time. An object that names a zone in a TZID must carry this beside its
// events. None for a name the zone database does not know.
func api_ical_timezone(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var name string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "name", &name); err != nil {
		return nil, err
	}
	zone, err := ical_timezone(name, time.Now())
	if err != nil {
		return sl.None, nil
	}
	return sl_encode(ical_tree(zone)), nil
}

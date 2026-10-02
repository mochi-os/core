// Mochi server: iCalendar and vCard for apps
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/emersion/go-ical"
	"github.com/teambition/rrule-go"
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
	"golang.org/x/image/colornames"
)

// Apps store calendar objects as iCalendar text and never parse it themselves:
// these functions turn the text into a component tree, expand recurrences for
// a range, and write a tree back out. A component is
//
//	{"name": "VEVENT", "properties": [{"name", "params", "value"}, ...], "components": [...]}
//
// with every parameter a list of strings, the same shape a card's property
// list has, so the two formats look alike to an app.

var api_ical = sls.FromStringDict(sl.String("mochi.ical"), sl.StringDict{
	"format":    sl.NewBuiltin("mochi.ical.format", api_ical_format),
	"instances": sl.NewBuiltin("mochi.ical.instances", api_ical_instances),
	"parse":     sl.NewBuiltin("mochi.ical.parse", api_ical_parse),
	"summary":   sl.NewBuiltin("mochi.ical.summary", api_ical_summary),
	"timezone":  sl.NewBuiltin("mochi.ical.timezone", api_ical_timezone),
})

var api_vcard = sls.FromStringDict(sl.String("mochi.vcard"), sl.StringDict{
	"format": sl.NewBuiltin("mochi.vcard.format", api_vcard_format),
})

// ical_text_maximum caps the text an app hands over to parse: a subscribed
// calendar can be large, and the parser holds it all.
const ical_text_maximum = 16 << 20

// ical_instances_maximum caps the occurrences one expansion returns, so a
// daily rule over a decade asked about with no finish cannot fill memory.
const ical_instances_maximum = 10000

// A recurrence is walked from its first occurrence, so the cost of reaching a
// range is set by the rule, not the range: a secondly rule from 1970 is two
// billion steps from today. These budgets count steps, not results, across one
// call; a step is a fraction of a microsecond. What a budget cuts off is left
// out of the answer rather than computed.
const ical_budget_instances = 1000000
const ical_budget_query = 2000000

// ical_budget_object caps one object's share of a query budget, so a single
// runaway rule is left out without starving the ordinary events after it.
const ical_budget_object = 100000

// ical_time_format is the iCalendar UTC form of a timestamp, exposed to apps
// as mochi.time's "ical" format.
const ical_time_format = "20060102T150405Z"

// === Trees ===

func ical_params(params ical.Params) map[string]any {
	out := map[string]any{}
	for k, v := range params {
		values := make([]any, len(v))
		for i, s := range v {
			values[i] = s
		}
		out[k] = values
	}
	return out
}

// Free text a person reads and writes: the tree carries these as the text
// itself, and ical_component escapes them again, so an app never sees or
// writes iCalendar's backslashes. Lists (CATEGORIES, RESOURCES) and tokens
// keep their written form.
var ical_prose = map[string]bool{
	ical.PropSummary:     true,
	ical.PropDescription: true,
	ical.PropLocation:    true,
	ical.PropComment:     true,
	ical.PropContact:     true,
	ical.PropName:        true,
	"X-WR-CALNAME":       true,
	"X-WR-CALDESC":       true,
}

// ical_prosaic says a property is free text, and not written as another type.
func ical_prosaic(prop *ical.Prop) bool {
	if !ical_prose[strings.ToUpper(prop.Name)] {
		return false
	}
	kind := prop.ValueType()
	return kind == ical.ValueText || kind == ical.ValueDefault
}

// ical_unescape reads a text value. A comma or semicolon written without its
// backslash is kept, so a value another client wrote loosely still reads
// whole, and a backslash before anything else is kept as it is.
func ical_unescape(value string) string {
	if !strings.Contains(value, "\\") {
		return value
	}
	var out strings.Builder
	for i := 0; i < len(value); i++ {
		if value[i] == '\\' && i+1 < len(value) {
			switch next := value[i+1]; next {
			case '\\', ';', ',':
				out.WriteByte(next)
				i++
				continue
			case 'n', 'N':
				out.WriteByte('\n')
				i++
				continue
			}
		}
		out.WriteByte(value[i])
	}
	return out.String()
}

var ical_escaper = strings.NewReplacer("\\", "\\\\", ";", "\\;", ",", "\\,", "\r\n", "\\n", "\n", "\\n", "\r", "\\n")

// ical_escape writes text as a text value: backslash, semicolon and comma
// escaped, and each line break as \n.
func ical_escape(text string) string {
	return ical_escaper.Replace(text)
}

// ical_tree turns a component into the dict apps see.
func ical_tree(comp *ical.Component) map[string]any {
	names := make([]string, 0, len(comp.Props))
	for name := range comp.Props {
		names = append(names, name)
	}
	sort.Strings(names)
	properties := make([]any, 0, len(names))
	for _, name := range names {
		for _, prop := range comp.Props[name] {
			value := prop.Value
			if ical_prosaic(&prop) {
				value = ical_unescape(value)
			}
			properties = append(properties, map[string]any{"name": prop.Name, "params": ical_params(prop.Params), "value": value})
		}
	}
	children := make([]any, 0, len(comp.Children))
	for _, child := range comp.Children {
		children = append(children, ical_tree(child))
	}
	return map[string]any{"name": comp.Name, "properties": properties, "components": children}
}

// ical_component is the inverse of ical_tree.
func ical_component(v any) (*ical.Component, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return nil, errors.New("component is not a dict")
	}
	name := strings.ToUpper(strings.TrimSpace(dav_string(m["name"])))
	if name == "" {
		return nil, errors.New("component has no name")
	}
	comp := ical.NewComponent(name)
	if properties, ok := m["properties"].([]any); ok {
		for _, item := range properties {
			pm, ok := item.(map[string]any)
			if !ok {
				continue
			}
			pname := strings.ToUpper(strings.TrimSpace(dav_string(pm["name"])))
			if pname == "" {
				continue
			}
			prop := ical.NewProp(pname)
			prop.Value = dav_string(pm["value"])
			if params, ok := pm["params"].(map[string]any); ok {
				for k, pv := range params {
					key := strings.ToUpper(k)
					switch x := pv.(type) {
					case []any:
						for _, e := range x {
							prop.Params[key] = append(prop.Params[key], dav_string(e))
						}
					case string:
						prop.Params[key] = append(prop.Params[key], x)
					}
				}
			}
			if ical_prosaic(prop) {
				prop.Value = ical_escape(prop.Value)
			}
			comp.Props.Add(prop)
		}
	}
	if children, ok := m["components"].([]any); ok {
		for _, item := range children {
			child, err := ical_component(item)
			if err != nil {
				return nil, err
			}
			comp.Children = append(comp.Children, child)
		}
	}
	return comp, nil
}

func ical_decode(text string) (*ical.Calendar, error) {
	if len(text) > ical_text_maximum {
		return nil, errors.New("calendar too large")
	}
	cal, err := ical.NewDecoder(strings.NewReader(text)).Decode()
	if err != nil {
		return nil, err
	}
	return cal, nil
}

func ical_encode(cal *ical.Calendar) (string, error) {
	var buf bytes.Buffer
	if err := ical.NewEncoder(&buf).Encode(cal); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// === Events ===

// ical_span reads a component's start and finish. An all-day event spans
// whole days in the given zone; an event with neither DTEND nor DURATION
// ends when it starts.
func ical_span(comp *ical.Component, loc *time.Location) (start time.Time, finish time.Time, allday bool, err error) {
	dtstart := comp.Props.Get(ical.PropDateTimeStart)
	if dtstart == nil {
		return start, finish, false, errors.New("no DTSTART")
	}
	allday = dtstart.ValueType() == ical.ValueDate
	start, err = dtstart.DateTime(loc)
	if err != nil {
		return start, finish, allday, err
	}
	if dtend := comp.Props.Get(ical.PropDateTimeEnd); dtend != nil {
		finish, err = dtend.DateTime(loc)
		return start, finish, allday, err
	}
	if duration := comp.Props.Get(ical.PropDuration); duration != nil {
		d, err := duration.Duration()
		if err != nil {
			return start, finish, allday, err
		}
		return start, start.Add(d), allday, nil
	}
	if allday {
		return start, start.AddDate(0, 0, 1), allday, nil
	}
	return start, start, allday, nil
}

// ical_recurrence builds the recurrence set of a component from its RRULE,
// RDATE and EXDATE properties, or nil when it does not recur. Unlike the
// library's own, a component with dates but no rule still recurs.
func ical_recurrence(comp *ical.Component, start time.Time, loc *time.Location) (*rrule.Set, error) {
	option, err := comp.Props.RecurrenceRule()
	if err != nil {
		return nil, err
	}
	if option == nil && len(comp.Props[ical.PropRecurrenceDates]) == 0 {
		return nil, nil
	}
	set := &rrule.Set{}
	set.DTStart(start)
	if option != nil {
		rule, err := rrule.NewRRule(*option)
		if err != nil {
			return nil, err
		}
		set.RRule(rule)
	}
	for _, prop := range comp.Props[ical.PropExceptionDates] {
		for _, value := range ical_dates(&prop, loc) {
			set.ExDate(value)
		}
	}
	for _, prop := range comp.Props[ical.PropRecurrenceDates] {
		for _, value := range ical_dates(&prop, loc) {
			set.RDate(value)
		}
	}
	return set, nil
}

// ical_dates reads a date property that may carry several comma-separated
// values, as RDATE and EXDATE do.
func ical_dates(prop *ical.Prop, loc *time.Location) []time.Time {
	var out []time.Time
	for _, value := range strings.Split(prop.Value, ",") {
		single := *prop
		single.Value = strings.TrimSpace(value)
		if t, err := single.DateTime(loc); err == nil {
			out = append(out, t)
		}
	}
	return out
}

func ical_overlaps(start, finish, from, until time.Time) bool {
	if !until.IsZero() && !start.Before(until) {
		return false
	}
	if !from.IsZero() {
		if finish.After(start) {
			return finish.After(from)
		}
		return !start.Before(from)
	}
	return true
}

func ical_text(comp *ical.Component, name string) string {
	prop := comp.Props.Get(name)
	if prop == nil {
		return ""
	}
	return ical_unescape(prop.Value)
}

// ical_instance is one occurrence as apps see it.
func ical_instance(comp *ical.Component, start, finish time.Time, allday bool, recurring bool) map[string]any {
	out := map[string]any{
		"uid":         ical_text(comp, ical.PropUID),
		"component":   comp.Name,
		"summary":     ical_text(comp, ical.PropSummary),
		"location":    ical_text(comp, ical.PropLocation),
		"description": ical_text(comp, ical.PropDescription),
		"status":      ical_text(comp, ical.PropStatus),
		"start":       start.Unix(),
		"finish":      finish.Unix(),
		"allday":      allday,
		"recurring":   recurring,
	}
	if allday {
		out["date"] = start.Format(time.DateOnly)
	} else {
		out["zone"] = map[string]any{
			"start":  ical_zone(comp, ical.PropDateTimeStart),
			"finish": ical_zone(comp, ical.PropDateTimeEnd),
		}
	}
	if rid := comp.Props.Get(ical.PropRecurrenceID); rid != nil {
		out["exception"] = true
	}
	if ical_alarmed(comp) {
		out["alarm"] = true
	}
	if colour := ical_colour(comp); colour != "" {
		out["colour"] = colour
	}
	return out
}

// ical_colour is the occurrence's own colour as "#rrggbb", or "" for none.
// RFC 7986 names a CSS colour; a hex value, which some clients write, is
// accepted too.
func ical_colour(comp *ical.Component) string {
	value := strings.ToLower(strings.TrimSpace(ical_text(comp, ical.PropColor)))
	if value == "" {
		return ""
	}
	if hex, ok := strings.CutPrefix(value, "#"); ok {
		if len(hex) == 3 {
			hex = string([]byte{hex[0], hex[0], hex[1], hex[1], hex[2], hex[2]})
		}
		if len(hex) != 6 || strings.Trim(hex, "0123456789abcdef") != "" {
			return ""
		}
		return "#" + hex
	}
	named, ok := colornames.Map[value]
	if !ok {
		return ""
	}
	return fmt.Sprintf("#%02x%02x%02x", named.R, named.G, named.B)
}

// ical_alarmed says the event carries a reminder of its own: a changed
// occurrence answers for itself, not for its series.
// ical_alarms is a component's alarms, each {"related": "start" or "end",
// "offset": seconds} for a trigger relative to an occurrence, or {"at":
// seconds} for one at a fixed time, once each.
func ical_alarms(comp *ical.Component, loc *time.Location) []any {
	out := []any{}
	seen := map[string]bool{}
	add := func(alarm map[string]any) {
		key := fmt.Sprint(alarm)
		if !seen[key] {
			seen[key] = true
			out = append(out, alarm)
		}
	}
	for _, child := range comp.Children {
		if child.Name != ical.CompAlarm {
			continue
		}
		trigger := child.Props.Get(ical.PropTrigger)
		if trigger == nil {
			continue
		}
		if !strings.EqualFold(trigger.Params.Get(ical.ParamValue), "DATE-TIME") {
			if offset, err := trigger.Duration(); err == nil {
				related := "start"
				if strings.EqualFold(trigger.Params.Get(ical.ParamRelated), "END") {
					related = "end"
				}
				add(map[string]any{"related": related, "offset": int64(offset / time.Second)})
				continue
			}
		}
		// A fixed time, whether its VALUE parameter says so or not.
		if at, err := trigger.DateTime(loc); err == nil {
			add(map[string]any{"at": at.Unix()})
		}
	}
	return out
}

func ical_alarmed(comp *ical.Component) bool {
	for _, child := range comp.Children {
		if child.Name == ical.CompAlarm {
			return true
		}
	}
	return false
}

// ical_zone is the TZID a date-time property names, "" for a UTC or floating
// value. An event with no DTEND, one with a DURATION or none, ends in the zone
// it starts in.
func ical_zone(comp *ical.Component, name string) string {
	prop := comp.Props.Get(name)
	if prop == nil && name == ical.PropDateTimeEnd {
		prop = comp.Props.Get(ical.PropDateTimeStart)
	}
	if prop == nil {
		return ""
	}
	return prop.Params.Get(ical.ParamTimezoneID)
}

// ical_instances expands the events of a calendar into the occurrences that
// overlap [from, until). A recurring event's overrides (RECURRENCE-ID) replace
// the occurrences they name. Floating times are read in loc.
func ical_instances(cal *ical.Calendar, from, until time.Time, loc *time.Location, alarms bool) []any {
	cal = ical_resolve(cal)
	type group struct {
		master    *ical.Component
		overrides []*ical.Component
	}
	groups := map[string]*group{}
	var order []string
	for _, child := range cal.Children {
		if child.Name != ical.CompEvent {
			continue
		}
		uid := ical_text(child, ical.PropUID)
		g := groups[uid]
		if g == nil {
			g = &group{}
			groups[uid] = g
			order = append(order, uid)
		}
		if child.Props.Get(ical.PropRecurrenceID) != nil {
			g.overrides = append(g.overrides, child)
		} else {
			g.master = child
		}
	}

	out := []any{}
	budget := ical_budget_instances
	// An occurrence, with its own component's alarms when asked for: an
	// override's for the occurrence it changes, the event's for the rest.
	emit := func(comp *ical.Component, start, finish time.Time, allday, recurring bool) {
		instance := ical_instance(comp, start, finish, allday, recurring)
		if alarms {
			instance["alarms"] = ical_alarms(comp, loc)
		}
		out = append(out, instance)
	}
	for _, uid := range order {
		g := groups[uid]
		overridden := map[int64]bool{}
		for _, o := range g.overrides {
			if rid, err := o.Props.Get(ical.PropRecurrenceID).DateTime(loc); err == nil {
				overridden[rid.Unix()] = true
			}
			start, finish, allday, err := ical_span(o, loc)
			if err != nil {
				continue
			}
			if ical_overlaps(start, finish, from, until) {
				emit(o, start, finish, allday, true)
			}
		}
		if g.master == nil {
			continue
		}
		start, finish, allday, err := ical_span(g.master, loc)
		if err != nil {
			continue
		}
		set, err := ical_recurrence(g.master, start, loc)
		if err != nil || set == nil {
			if ical_overlaps(start, finish, from, until) {
				emit(g.master, start, finish, allday, false)
			}
			continue
		}
		duration := finish.Sub(start)
		// An occurrence that started before the range can still reach into
		// it, so the window opens one duration early.
		after := from.Add(-duration)
		if from.IsZero() {
			after = start.AddDate(-1, 0, 0)
		}
		before := until
		if until.IsZero() {
			before = time.Now().AddDate(1, 0, 0)
		}
		next := set.Iterator()
		for budget > 0 && len(out) < ical_instances_maximum {
			budget--
			occurrence, ok := next()
			if !ok || occurrence.After(before) {
				break
			}
			if occurrence.Before(after) || overridden[occurrence.Unix()] {
				continue
			}
			if ical_overlaps(occurrence, occurrence.Add(duration), from, until) {
				emit(g.master, occurrence, occurrence.Add(duration), allday, true)
			}
		}
		if len(out) >= ical_instances_maximum {
			break
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].(map[string]any)["start"].(int64) < out[j].(map[string]any)["start"].(int64)
	})
	return out
}

// ical_expansion_heavy reports whether matching a calendar object against a
// time range ending at until would walk more recurrence steps than one object
// may, or than the query's budget has left, and spends what it walks. The CalDAV matcher expands each rule
// from its first occurrence to the range's end and keeps every result, so this
// runs first and the heavy objects are left out of its answer. A range with no
// end costs the matcher nothing.
func ical_expansion_heavy(cal *ical.Calendar, until time.Time, budget *int) bool {
	if cal == nil || until.IsZero() {
		return false
	}
	for _, child := range cal.Children {
		if child.Props.Get(ical.PropRecurrenceRule) == nil && child.Props.Get(ical.PropRecurrenceDates) == nil {
			continue
		}
		start, _, _, err := ical_span(child, time.UTC)
		if err != nil {
			continue
		}
		set, err := ical_recurrence(child, start, time.UTC)
		if err != nil || set == nil {
			continue
		}
		allowance := min(ical_budget_object, *budget)
		next := set.Iterator()
		for {
			if allowance <= 0 {
				return true
			}
			allowance--
			*budget--
			occurrence, ok := next()
			if !ok || occurrence.After(until) {
				break
			}
		}
	}
	return false
}

// ical_summary reads what an app keeps beside the text of a calendar object:
// the columns it lists and prefilters by. A recurring object's finish is 0,
// meaning open-ended, since its last occurrence is a matter of expansion.
func ical_summary(cal *ical.Calendar) Map {
	cal = ical_resolve(cal)
	out := Map{"uid": "", "component": "", "summary": "", "start": int64(0), "finish": int64(0), "allday": false, "recurring": false}
	for _, child := range cal.Children {
		if child.Name != ical.CompEvent && child.Name != ical.CompToDo && child.Name != "VJOURNAL" {
			continue
		}
		if child.Props.Get(ical.PropRecurrenceID) != nil && out["uid"] != "" {
			continue
		}
		out["uid"] = ical_text(child, ical.PropUID)
		out["component"] = child.Name
		out["summary"] = ical_text(child, ical.PropSummary)
		start, finish, allday, err := ical_span(child, time.UTC)
		if err != nil {
			// An object whose start cannot be read would never expand into a
			// listing; refusing it here keeps such an object out of storage.
			return nil
		}
		out["start"] = start.Unix()
		out["finish"] = finish.Unix()
		out["allday"] = allday
		recurring := child.Props.Get(ical.PropRecurrenceRule) != nil || child.Props.Get(ical.PropRecurrenceDates) != nil
		out["recurring"] = recurring
		if recurring {
			out["finish"] = int64(0)
		}
		break
	}
	return out
}

// === Starlark ===

// mochi.ical.parse(text) -> dict | None: Parse iCalendar text into a component
// tree, the VCALENDAR at the top. None when the text does not parse.
func api_ical_parse(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var text string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "text", &text); err != nil {
		return nil, err
	}
	cal, err := ical_decode(text)
	if err != nil {
		return sl.None, nil
	}
	return sl_encode(ical_tree(cal.Component)), nil
}

// mochi.ical.format(component) -> string: Write a component tree as iCalendar
// text. The tree must be a VCALENDAR carrying VERSION and PRODID, and every
// VEVENT a UID and DTSTAMP, as the format requires.
func api_ical_format(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var component sl.Value
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "component", &component); err != nil {
		return nil, err
	}
	comp, err := ical_component(sl_decode(component))
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	text, err := ical_encode(&ical.Calendar{Component: comp})
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	return sl.String(text), nil
}

// mochi.ical.instances(text, start?, finish?, timezone?, alarms?) -> list:
// Expand the events in iCalendar text into the occurrences overlapping
// [start, finish), Unix seconds, sorted by start. Each is {uid, component,
// summary, location, description, status, start, finish, allday, date?,
// zone?, recurring, exception?, alarms?}; zone is {start, finish}, the TZID
// each end of a timed occurrence was written in, "" for UTC and floating
// values. With alarms=True, alarms is the occurrence's own: its override's
// when it has one, else the event's, each {related, offset} for a trigger
// relative to the start or end in seconds, or {at} for a fixed time. Floating
// times are read in timezone (IANA name), else the user's zone: the timezone
// preference, or the zone the user's device last reported.
func api_ical_instances(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var text, timezone string
	var start, finish int64
	var alarms bool
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "text", &text, "start?", &start, "finish?", &finish, "timezone?", &timezone, "alarms?", &alarms); err != nil {
		return nil, err
	}
	cal, err := ical_decode(text)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	loc := user_timezone(principal_caller(t))
	if timezone != "" {
		l, err := time.LoadLocation(timezone)
		if err != nil || timezone == "Local" {
			l = time.UTC
		}
		loc = l
	}
	var from, until time.Time
	if start > 0 {
		from = time.Unix(start, 0)
	}
	if finish > 0 {
		until = time.Unix(finish, 0)
	}
	return sl_encode(ical_instances(cal, from, until, loc, alarms)), nil
}

// mochi.ical.summary(text) -> dict | None: What an app keeps beside the text
// of a calendar object: {uid, component, summary, start, finish, allday,
// recurring}, from its first event, task or journal entry. start and finish
// are Unix seconds; a recurring entry's finish is 0, meaning open-ended. The
// same values the CalDAV engine passes to dav/put. None when the text does not
// parse or its first entry has no readable start.
func api_ical_summary(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var text string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "text", &text); err != nil {
		return nil, err
	}
	cal, err := ical_decode(text)
	if err != nil {
		return sl.None, nil
	}
	summary := ical_summary(cal)
	if summary == nil {
		return sl.None, nil
	}
	return sl_encode(map[string]any(summary)), nil
}

// mochi.vcard.format(properties) -> string: Write a card's property list as
// vCard text: [{name, params, value, group?}, ...]. A list without VERSION is
// written as 3.0.
func api_vcard_format(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var properties sl.Value
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "properties", &properties); err != nil {
		return nil, err
	}
	text, err := dav_card_text(sl_decode(properties))
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	return sl.String(text), nil
}

// ical_time reads mochi.time's "ical" format: a UTC timestamp, or a date.
func ical_time(s string) (time.Time, error) {
	if t, err := time.Parse(ical_time_format, s); err == nil {
		return t, nil
	}
	if t, err := time.Parse("20060102", s); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("not an iCalendar time: %q", s)
}

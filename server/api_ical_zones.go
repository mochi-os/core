// Mochi server: reading the zones an iCalendar text names that the zone
// database does not know.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"strconv"
	"strings"
	"time"

	"github.com/emersion/go-ical"
)

// ical_windows maps the zone names Windows writes (Outlook, Exchange) to the
// IANA zone CLDR's windowsZones table gives each as its primary, written under
// its current name. Generated from Go's time/zoneinfo_abbrs_windows.go, which
// is itself generated from that table.
var ical_windows = map[string]string{
	"AUS Central Standard Time":       "Australia/Darwin",
	"AUS Eastern Standard Time":       "Australia/Sydney",
	"Afghanistan Standard Time":       "Asia/Kabul",
	"Alaskan Standard Time":           "America/Anchorage",
	"Aleutian Standard Time":          "America/Adak",
	"Altai Standard Time":             "Asia/Barnaul",
	"Arab Standard Time":              "Asia/Riyadh",
	"Arabian Standard Time":           "Asia/Dubai",
	"Arabic Standard Time":            "Asia/Baghdad",
	"Argentina Standard Time":         "America/Argentina/Buenos_Aires",
	"Astrakhan Standard Time":         "Europe/Astrakhan",
	"Atlantic Standard Time":          "America/Halifax",
	"Aus Central W. Standard Time":    "Australia/Eucla",
	"Azerbaijan Standard Time":        "Asia/Baku",
	"Azores Standard Time":            "Atlantic/Azores",
	"Bahia Standard Time":             "America/Bahia",
	"Bangladesh Standard Time":        "Asia/Dhaka",
	"Belarus Standard Time":           "Europe/Minsk",
	"Bougainville Standard Time":      "Pacific/Bougainville",
	"Canada Central Standard Time":    "America/Regina",
	"Cape Verde Standard Time":        "Atlantic/Cape_Verde",
	"Caucasus Standard Time":          "Asia/Yerevan",
	"Cen. Australia Standard Time":    "Australia/Adelaide",
	"Central America Standard Time":   "America/Guatemala",
	"Central Asia Standard Time":      "Asia/Bishkek",
	"Central Brazilian Standard Time": "America/Cuiaba",
	"Central Europe Standard Time":    "Europe/Budapest",
	"Central European Standard Time":  "Europe/Warsaw",
	"Central Pacific Standard Time":   "Pacific/Guadalcanal",
	"Central Standard Time":           "America/Chicago",
	"Central Standard Time (Mexico)":  "America/Mexico_City",
	"Chatham Islands Standard Time":   "Pacific/Chatham",
	"China Standard Time":             "Asia/Shanghai",
	"Cuba Standard Time":              "America/Havana",
	"Dateline Standard Time":          "Etc/GMT+12",
	"E. Africa Standard Time":         "Africa/Nairobi",
	"E. Australia Standard Time":      "Australia/Brisbane",
	"E. Europe Standard Time":         "Europe/Chisinau",
	"E. South America Standard Time":  "America/Sao_Paulo",
	"Easter Island Standard Time":     "Pacific/Easter",
	"Eastern Standard Time":           "America/New_York",
	"Eastern Standard Time (Mexico)":  "America/Cancun",
	"Egypt Standard Time":             "Africa/Cairo",
	"Ekaterinburg Standard Time":      "Asia/Yekaterinburg",
	"FLE Standard Time":               "Europe/Kyiv",
	"Fiji Standard Time":              "Pacific/Fiji",
	"GMT Standard Time":               "Europe/London",
	"GTB Standard Time":               "Europe/Bucharest",
	"Georgian Standard Time":          "Asia/Tbilisi",
	"Greenland Standard Time":         "America/Nuuk",
	"Greenwich Standard Time":         "Atlantic/Reykjavik",
	"Haiti Standard Time":             "America/Port-au-Prince",
	"Hawaiian Standard Time":          "Pacific/Honolulu",
	"India Standard Time":             "Asia/Kolkata",
	"Iran Standard Time":              "Asia/Tehran",
	"Israel Standard Time":            "Asia/Jerusalem",
	"Jordan Standard Time":            "Asia/Amman",
	"Kaliningrad Standard Time":       "Europe/Kaliningrad",
	"Korea Standard Time":             "Asia/Seoul",
	"Libya Standard Time":             "Africa/Tripoli",
	"Line Islands Standard Time":      "Pacific/Kiritimati",
	"Lord Howe Standard Time":         "Australia/Lord_Howe",
	"Magadan Standard Time":           "Asia/Magadan",
	"Magallanes Standard Time":        "America/Punta_Arenas",
	"Marquesas Standard Time":         "Pacific/Marquesas",
	"Mauritius Standard Time":         "Indian/Mauritius",
	"Middle East Standard Time":       "Asia/Beirut",
	"Montevideo Standard Time":        "America/Montevideo",
	"Morocco Standard Time":           "Africa/Casablanca",
	"Mountain Standard Time":          "America/Denver",
	"Mountain Standard Time (Mexico)": "America/Mazatlan",
	"Myanmar Standard Time":           "Asia/Yangon",
	"N. Central Asia Standard Time":   "Asia/Novosibirsk",
	"Namibia Standard Time":           "Africa/Windhoek",
	"Nepal Standard Time":             "Asia/Kathmandu",
	"New Zealand Standard Time":       "Pacific/Auckland",
	"Newfoundland Standard Time":      "America/St_Johns",
	"Norfolk Standard Time":           "Pacific/Norfolk",
	"North Asia East Standard Time":   "Asia/Irkutsk",
	"North Asia Standard Time":        "Asia/Krasnoyarsk",
	"North Korea Standard Time":       "Asia/Pyongyang",
	"Omsk Standard Time":              "Asia/Omsk",
	"Pacific SA Standard Time":        "America/Santiago",
	"Pacific Standard Time":           "America/Los_Angeles",
	"Pacific Standard Time (Mexico)":  "America/Tijuana",
	"Pakistan Standard Time":          "Asia/Karachi",
	"Paraguay Standard Time":          "America/Asuncion",
	"Qyzylorda Standard Time":         "Asia/Qyzylorda",
	"Romance Standard Time":           "Europe/Paris",
	"Russia Time Zone 10":             "Asia/Srednekolymsk",
	"Russia Time Zone 11":             "Asia/Kamchatka",
	"Russia Time Zone 3":              "Europe/Samara",
	"Russian Standard Time":           "Europe/Moscow",
	"SA Eastern Standard Time":        "America/Cayenne",
	"SA Pacific Standard Time":        "America/Bogota",
	"SA Western Standard Time":        "America/La_Paz",
	"SE Asia Standard Time":           "Asia/Bangkok",
	"Saint Pierre Standard Time":      "America/Miquelon",
	"Sakhalin Standard Time":          "Asia/Sakhalin",
	"Samoa Standard Time":             "Pacific/Apia",
	"Sao Tome Standard Time":          "Africa/Sao_Tome",
	"Saratov Standard Time":           "Europe/Saratov",
	"Singapore Standard Time":         "Asia/Singapore",
	"South Africa Standard Time":      "Africa/Johannesburg",
	"South Sudan Standard Time":       "Africa/Juba",
	"Sri Lanka Standard Time":         "Asia/Colombo",
	"Sudan Standard Time":             "Africa/Khartoum",
	"Syria Standard Time":             "Asia/Damascus",
	"Taipei Standard Time":            "Asia/Taipei",
	"Tasmania Standard Time":          "Australia/Hobart",
	"Tocantins Standard Time":         "America/Araguaina",
	"Tokyo Standard Time":             "Asia/Tokyo",
	"Tomsk Standard Time":             "Asia/Tomsk",
	"Tonga Standard Time":             "Pacific/Tongatapu",
	"Transbaikal Standard Time":       "Asia/Chita",
	"Turkey Standard Time":            "Europe/Istanbul",
	"Turks And Caicos Standard Time":  "America/Grand_Turk",
	"US Eastern Standard Time":        "America/Indiana/Indianapolis",
	"US Mountain Standard Time":       "America/Phoenix",
	"UTC":                             "Etc/UTC",
	"UTC+12":                          "Etc/GMT-12",
	"UTC+13":                          "Etc/GMT-13",
	"UTC-02":                          "Etc/GMT+2",
	"UTC-08":                          "Etc/GMT+8",
	"UTC-09":                          "Etc/GMT+9",
	"UTC-11":                          "Etc/GMT+11",
	"Ulaanbaatar Standard Time":       "Asia/Ulaanbaatar",
	"Venezuela Standard Time":         "America/Caracas",
	"Vladivostok Standard Time":       "Asia/Vladivostok",
	"Volgograd Standard Time":         "Europe/Volgograd",
	"W. Australia Standard Time":      "Australia/Perth",
	"W. Central Africa Standard Time": "Africa/Lagos",
	"W. Europe Standard Time":         "Europe/Berlin",
	"W. Mongolia Standard Time":       "Asia/Hovd",
	"West Asia Standard Time":         "Asia/Tashkent",
	"West Bank Standard Time":         "Asia/Hebron",
	"West Pacific Standard Time":      "Pacific/Port_Moresby",
	"Yakutsk Standard Time":           "Asia/Yakutsk",
	"Yukon Standard Time":             "America/Whitehorse",
}

// ical_seconds reads an iCalendar UTC offset (+0100, -0530, +053000) as
// seconds east of UTC, the inverse of ical_offset.
func ical_seconds(value string) (int, bool) {
	value = strings.TrimSpace(value)
	if len(value) != 5 && len(value) != 7 {
		return 0, false
	}
	sign := 1
	switch value[0] {
	case '+':
	case '-':
		sign = -1
	default:
		return 0, false
	}
	hours, err := strconv.Atoi(value[1:3])
	if err != nil {
		return 0, false
	}
	minutes, err := strconv.Atoi(value[3:5])
	if err != nil {
		return 0, false
	}
	seconds := 0
	if len(value) == 7 {
		if seconds, err = strconv.Atoi(value[5:7]); err != nil {
			return 0, false
		}
	}
	return sign * (hours*3600 + minutes*60 + seconds), true
}

// ical_standard is the offset a VTIMEZONE gives in its standard part, else in
// its first part, seconds east of UTC.
func ical_standard(zone *ical.Component) (int, bool) {
	var first *ical.Component
	for _, part := range zone.Children {
		if part.Props.Get("TZOFFSETTO") == nil {
			continue
		}
		if part.Name == "STANDARD" {
			return ical_seconds(part.Props.Get("TZOFFSETTO").Value)
		}
		if first == nil {
			first = part
		}
	}
	if first == nil {
		return 0, false
	}
	return ical_seconds(first.Props.Get("TZOFFSETTO").Value)
}

// ical_loadable reports whether the zone database knows a TZID.
func ical_loadable(name string) bool {
	if name == "" || name == "Local" {
		return false
	}
	_, err := time.LoadLocation(name)
	return err == nil
}

// ical_resolve returns the calendar with every TZID the zone database cannot
// load replaced, for reading its times: a Windows zone name by its IANA zone,
// and any other name by the fixed offset the calendar's own VTIMEZONE gives
// it, its local times written in UTC instead, so the event keeps its place
// with its daylight-saving shifts approximated. A name with neither is left,
// and its event is refused as before. The calendar passed in is not changed:
// when anything needs replacing the answer is a copy, so the text stored and
// served keeps the names it was written with.
func ical_resolve(cal *ical.Calendar) *ical.Calendar {
	if cal == nil || cal.Component == nil {
		return cal
	}
	known := map[string]bool{}
	unknown := false
	var scan func(comp *ical.Component)
	scan = func(comp *ical.Component) {
		for _, props := range comp.Props {
			for _, prop := range props {
				name := prop.Params.Get(ical.ParamTimezoneID)
				if name == "" {
					continue
				}
				if _, seen := known[name]; !seen {
					known[name] = ical_loadable(name)
				}
				if !known[name] {
					unknown = true
				}
			}
		}
		for _, child := range comp.Children {
			scan(child)
		}
	}
	scan(cal.Component)
	if !unknown {
		return cal
	}
	text, err := ical_encode(cal)
	if err != nil {
		return cal
	}
	copied, err := ical_decode(text)
	if err != nil {
		return cal
	}
	offsets := map[string]int{}
	for _, child := range copied.Children {
		if child.Name != ical.CompTimezone {
			continue
		}
		if offset, ok := ical_standard(child); ok {
			// The TZID property is text, so a comma in it is escaped; the
			// parameters naming it carry it quoted instead.
			offsets[ical_unescape(child.Props.Get(ical.PropTimezoneID).Value)] = offset
		}
	}
	var rewrite func(comp *ical.Component)
	rewrite = func(comp *ical.Component) {
		if comp.Name == ical.CompTimezone {
			return
		}
		for key, props := range comp.Props {
			for i := range props {
				prop := &props[i]
				name := prop.Params.Get(ical.ParamTimezoneID)
				if name == "" || known[name] {
					continue
				}
				if zone, ok := ical_windows[name]; ok {
					prop.Params.Set(ical.ParamTimezoneID, zone)
					continue
				}
				offset, ok := offsets[name]
				if !ok {
					continue
				}
				fixed := time.FixedZone(name, offset)
				values := strings.Split(prop.Value, ",")
				for j, value := range values {
					value = strings.TrimSpace(value)
					if t, err := time.ParseInLocation("20060102T150405", value, fixed); err == nil {
						values[j] = t.UTC().Format("20060102T150405Z")
					}
				}
				prop.Value = strings.Join(values, ",")
				prop.Params.Del(ical.ParamTimezoneID)
			}
			comp.Props[key] = props
		}
		for _, child := range comp.Children {
			rewrite(child)
		}
	}
	rewrite(copied.Component)
	return copied
}

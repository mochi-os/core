// Mochi server: times and dates as a user reads them.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// clock_language is how one language writes a time of day and a date, as
// browsers write them: build/scripts/clock.mjs reads it from ICU.
type clock_language struct {
	Twelve   bool       // the language's own clock is the 12-hour one
	Clocks   [2]string  // the 24-hour and the 12-hour clock, with {hour}, {minute} and {period} in place
	Padded   [2]bool    // whether each clock gives a single-digit hour a leading zero
	Periods  [24]string // the 12-hour clock's word for the part of the day, by hour
	Date     string     // the long date, with {day}, {month} and {year} in place
	Months   [12]string // each month as the long date writes it, in Calendar
	Short    [12]string // the Gregorian months abbreviated, for D MMM YYYY
	Digits   string     // the language's digits from zero to nine, when not 0-9
	Calendar string     // gregory, persian or buddhist
	Own      bool       // false when the formats are another language's
}

// clock_language_for returns a language's formats, walking its fallback chain,
// and British English's when nothing on it has any.
func clock_language_for(language string) clock_language {
	for _, tag := range language_fallbacks(language) {
		if l, ok := clock_languages[tag]; ok {
			return l
		}
	}
	return clock_languages["en"]
}

// time_clock is the time of day as a user reads it: hours and minutes on the
// clock their time_format preference names, or with "auto" the one their
// language uses, written as that language writes its short time, with its
// word for the part of the day. Digits are 0-9, as the calendar shows them.
func time_clock(t time.Time, language, preference string) string {
	l := clock_language_for(language)
	twelve := preference == "12h" || (preference != "24h" && l.Twelve)
	index, hour := 0, t.Hour()
	if twelve {
		index = 1
		hour %= 12
		if hour == 0 {
			hour = 12
		}
	}
	hours := strconv.Itoa(hour)
	if l.Padded[index] && hour < 10 {
		hours = "0" + hours
	}
	return strings.NewReplacer(
		"{hour}", hours,
		"{minute}", fmt.Sprintf("%02d", t.Minute()),
		"{period}", l.Periods[t.Hour()],
	).Replace(l.Clocks[index])
}

// time_day is a date as a user reads it in a sentence: in the date_format
// their preference names, or with "auto" their language's long date, in its
// own calendar and digits. A language whose formats are borrowed gets the
// date in numbers rather than another language's month names.
func time_day(t time.Time, language, preference string) string {
	l := clock_language_for(language)
	switch preference {
	case "YYYY-MM-DD":
		return t.Format("2006-01-02")
	case "DD/MM/YYYY":
		return t.Format("02/01/2006")
	case "DD.MM.YYYY":
		return t.Format("02.01.2006")
	case "MM/DD/YYYY":
		return t.Format("01/02/2006")
	case "D MMM YYYY":
		if !l.Own {
			return t.Format("2006-01-02")
		}
		return fmt.Sprintf("%d %s %d", t.Day(), l.Short[t.Month()-1], t.Year())
	}
	if !l.Own {
		return t.Format("2006-01-02")
	}
	year, month, day := t.Year(), int(t.Month()), t.Day()
	switch l.Calendar {
	case "persian":
		year, month, day = persian_date(year, month, day)
	case "buddhist":
		year += 543
	}
	return strings.NewReplacer(
		"{day}", clock_digits(strconv.Itoa(day), l.Digits),
		"{month}", l.Months[month-1],
		"{year}", clock_digits(strconv.Itoa(year), l.Digits),
	).Replace(l.Date)
}

// clock_digits writes a number's 0-9 in a language's own digits.
func clock_digits(number, digits string) string {
	if digits == "" {
		return number
	}
	own := []rune(digits)
	var b strings.Builder
	for _, r := range number {
		if r >= '0' && r <= '9' {
			b.WriteRune(own[r-'0'])
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// persian_date converts a Gregorian date to the Solar Hijri calendar Persian
// and Pashto date by, with the arithmetic of the jalaali-js library: the year
// begins at the March equinox, its leap years following the 2820-year cycle
// the breaks below mark.
func persian_date(gy, gm, gd int) (int, int, int) {
	jdn := persian_day(gy, gm, gd)
	jy := gy - 621
	leap, march := persian_year(jy)
	first := persian_day(gy, 3, march)
	k := jdn - first
	if k >= 0 {
		if k <= 185 {
			return jy, 1 + k/31, k%31 + 1
		}
		k -= 186
	} else {
		jy--
		k += 179
		if leap == 1 {
			k++
		}
	}
	return jy, 7 + k/30, k%30 + 1
}

var persian_breaks = []int{-61, 9, 38, 199, 426, 686, 756, 818, 1111, 1181, 1210, 1635, 2060, 2097, 2192, 2262, 2324, 2394, 2456, 3178}

// persian_year returns where a Solar Hijri year sits in its leap cycle (0 is a
// leap year's following year, as jalaali-js counts) and the day in March of
// the Gregorian year it begins in on which it begins.
func persian_year(jy int) (int, int) {
	jp := persian_breaks[0]
	jump := 0
	leap := -14
	for _, jm := range persian_breaks[1:] {
		jump = jm - jp
		if jy < jm {
			break
		}
		leap += jump/33*8 + jump%33/4
		jp = jm
	}
	n := jy - jp
	leap += n/33*8 + (n%33+3)/4
	if jump%33 == 4 && jump-n == 4 {
		leap++
	}
	gy := jy + 621
	march := 20 + leap - (gy/4 - (gy/100+1)*3/4 - 150)
	if jump-n < 6 {
		n = n - jump + (jump+4)/33*33
	}
	cycle := ((n+1)%33 - 1) % 4
	if cycle == -1 {
		cycle = 4
	}
	return cycle, march
}

// persian_day is the Julian day number of a Gregorian date.
func persian_day(gy, gm, gd int) int {
	d := (gy+(gm-8)/6+100100)*1461/4 + (153*((gm+9)%12)+2)/5 + gd - 34840408
	return d - (gy+100100+(gm-8)/6)/100*3/4 + 752
}

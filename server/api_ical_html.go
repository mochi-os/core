// Mochi server: iCalendar descriptions written as HTML
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"html"
	"regexp"
	"strings"

	"github.com/emersion/go-ical"
	sl "go.starlark.net/starlark"
)

// A Google calendar writes an event's DESCRIPTION as HTML, where the format
// defines it as plain text, so a client reading it as the format says shows
// the tags. Served to other clients, such a description reads as its text,
// with the HTML beside it in X-ALT-DESC, the property clients that show rich
// descriptions read. What is stored keeps the HTML as it came, so the event
// goes back to Google unchanged and the apps' own editors keep it.

// ical_alternative names the property that carries a description's HTML.
const ical_alternative = "X-ALT-DESC"

// A tag with a real name, opening or closing, with attributes or not. "a < b"
// and "<3" are not markup, and neither is an unknown angle-bracketed word. The
// web's isHtml() and the Android client's read the same shape.
var html_tag = regexp.MustCompile(`(?i)</?(a|b|blockquote|br|code|div|em|font|h[1-6]|hr|i|img|li|ol|p|pre|small|span|strong|sub|sup|table|td|th|tr|u|ul)\b[^>]*>`)

var html_break = regexp.MustCompile(`(?i)<br\s*/?>`)
var html_block = regexp.MustCompile(`(?i)</(blockquote|div|h[1-6]|li|p|pre|table|tr)\s*>`)
var html_script = regexp.MustCompile(`(?is)<script\b[^>]*>.*?</script\s*>`)
var html_style = regexp.MustCompile(`(?is)<style\b[^>]*>.*?</style\s*>`)
var html_comment = regexp.MustCompile(`(?s)<!--.*?-->`)

// Any tag an HTML parser would read as one: a name after "<" or "</", or a
// declaration. A "<" before a space or a digit is text, as a browser has it.
var html_markup = regexp.MustCompile(`<(/?[a-zA-Z][^>]*|![^>]*|\?[^>]*)>`)

var html_trailing = regexp.MustCompile(`[ \t]+\n`)
var html_leading = regexp.MustCompile(`\n[ \t]+`)
var html_blank = regexp.MustCompile(`\n{3,}`)

// html_is reports whether text holds HTML markup.
func html_is(text string) bool {
	return html_tag.MatchString(text)
}

// html_text is the text of an HTML fragment: line breaks and block ends
// become newlines, every other tag drops, entities decode, styles and scripts
// vanish, and runs of blank lines collapse to one, as the web's and the
// Android client's textFromHtml() read it.
func html_text(source string) string {
	text := html_break.ReplaceAllString(source, "\n")
	text = html_block.ReplaceAllString(text, "\n")
	text = html_script.ReplaceAllString(text, "")
	text = html_style.ReplaceAllString(text, "")
	text = html_comment.ReplaceAllString(text, "")
	text = html_markup.ReplaceAllString(text, "")
	text = html.UnescapeString(text)
	text = strings.ReplaceAll(text, " ", " ")
	text = html_trailing.ReplaceAllString(text, "\n")
	text = html_leading.ReplaceAllString(text, "\n")
	text = html_blank.ReplaceAllString(text, "\n\n")
	return strings.TrimSpace(text)
}

// ical_described is whether a component kind carries a description people
// read: events, to-dos and journal entries.
func ical_described(name string) bool {
	return name == ical.CompEvent || name == ical.CompToDo || name == ical.CompJournal
}

// ical_plain returns the calendar with every description written as HTML
// served as its text, the HTML kept in X-ALT-DESC unless the component
// already carries one. The calendar passed in is not changed: when anything
// needs rewriting the answer is a copy, so what is stored keeps its HTML.
func ical_plain(cal *ical.Calendar) *ical.Calendar {
	if cal == nil || cal.Component == nil {
		return cal
	}
	found := false
	for _, child := range cal.Children {
		if !ical_described(child.Name) {
			continue
		}
		for _, prop := range child.Props[ical.PropDescription] {
			if html_is(ical_unescape(prop.Value)) {
				found = true
			}
		}
	}
	if !found {
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
	for _, child := range copied.Children {
		if !ical_described(child.Name) {
			continue
		}
		props := child.Props[ical.PropDescription]
		for i := range props {
			source := ical_unescape(props[i].Value)
			if !html_is(source) {
				continue
			}
			if child.Props.Get(ical_alternative) == nil {
				alternative := ical.NewProp(ical_alternative)
				alternative.Params.Set(ical.ParamFormatType, "text/html")
				alternative.Value = ical_escape(source)
				child.Props.Add(alternative)
			}
			props[i].Value = ical_escape(html_text(source))
		}
	}
	return copied
}

// mochi.ical.plain(text) -> string: The iCalendar text with every event
// description written as HTML, as a Google calendar's are, given as its text,
// and the HTML kept in X-ALT-DESC;FMTTYPE=text/html, for clients that read
// DESCRIPTION as the plain text the format defines. The text unchanged when
// no description needs it, or when it does not parse.
func api_ical_plain(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var text string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "text", &text); err != nil {
		return nil, err
	}
	cal, err := ical_decode(text)
	if err != nil {
		return sl.String(text), nil
	}
	plain := ical_plain(cal)
	if plain == cal {
		return sl.String(text), nil
	}
	out, err := ical_encode(plain)
	if err != nil {
		return sl.String(text), nil
	}
	return sl.String(out), nil
}

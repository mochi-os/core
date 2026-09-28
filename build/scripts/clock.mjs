#!/usr/bin/env node
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

// Writes server/clock_languages.go and server/testdata/clock.json: how each
// language Mochi speaks writes a time of day and a date, read from the ICU
// data of the Node running it, which is the data browsers format with. The
// server formats from the table; the test holds it to what ICU writes for
// every language.
//
// Run from core/ after adding a language or moving to a newer Node:
//   node build/scripts/clock.mjs

import { execFileSync } from 'node:child_process'
import fs from 'node:fs'

// A language's formats come from ICU under this locale: Mochi's base English
// is British and its plain Portuguese European, where ICU's plain "pt" is
// Brazilian.
const LOCALES = {
  en: 'en-GB',
  pt: 'pt-PT',
  'zh-hans': 'zh-Hans',
  'zh-hant': 'zh-Hant',
  'zh-hk': 'zh-Hant-HK',
  tl: 'fil',
}

// A language ICU has no data for borrows the formats of the language it is
// commonly read alongside.
const BORROWED = { ay: 'es-419', bho: 'hi', gn: 'es-419', ht: 'fr', su: 'id' }

const CALENDARS = new Set(['gregory', 'persian', 'buddhist'])

function locale(tag) {
  if (LOCALES[tag]) return LOCALES[tag]
  const [language, region] = tag.split('-')
  return region ? `${language}-${region.length === 2 ? region.toUpperCase() : region}` : language
}

function supported(name) {
  return Intl.DateTimeFormat.supportedLocalesOf([name]).length > 0 &&
    new Intl.Locale(new Intl.DateTimeFormat(name).resolvedOptions().locale).language === new Intl.Locale(name).language
}

const utc = (y, m, d, h = 12, n = 0) => new Date(Date.UTC(y, m - 1, d, h, n))
const spaces = (text) => text.replace(/[  ]/g, ' ')

// A clock, 24-hour or 12-hour, as the language's short time writes it: its
// shape with {hour}, {minute} and {period} in place, whether a single-digit
// hour takes a leading zero, and the period's word for each hour of the day.
// Digits are 0-9 and minutes two of them, as the calendar shows a time, where
// ICU leaves Yoruba's minutes unpadded.
function clock(name, cycle) {
  const format = new Intl.DateTimeFormat(name, {
    timeStyle: 'short',
    hourCycle: cycle,
    timeZone: 'UTC',
    numberingSystem: 'latn',
  })
  const shape = format
    .formatToParts(utc(2026, 9, 29, 15, 4))
    .map((p) => {
      switch (p.type) {
        case 'hour':
          return '{hour}'
        case 'minute':
          return '{minute}'
        case 'dayPeriod':
          return '{period}'
        case 'literal':
          return spaces(p.value)
        default:
          throw new Error(`${name}: clock part ${p.type}`)
      }
    })
    .join('')
  const padded = format.formatToParts(utc(2026, 9, 29, 9, 5)).find((p) => p.type === 'hour').value.length === 2
  const periods = Array.from({ length: 24 }, (_, h) => {
    const words = [0, 30, 59].map(
      (m) => format.formatToParts(utc(2026, 9, 29, h, m)).find((p) => p.type === 'dayPeriod')?.value ?? ''
    )
    if (new Set(words).size > 1) throw new Error(`${name}: the period changes within hour ${h}`)
    return spaces(words[0])
  })
  // What the clock writes, with the minutes as the server writes them.
  const write = (date) =>
    spaces(
      format
        .formatToParts(date)
        .map((p) => (p.type === 'minute' ? p.value.padStart(2, '0') : p.value))
        .join('')
    )
  return { shape, padded, periods, write }
}

// The long date as the calendar's headings write it, with its day, month and
// year in place, and each month's name in the language's own calendar.
function day(name) {
  const format = new Intl.DateTimeFormat(name, { day: 'numeric', month: 'long', year: 'numeric', timeZone: 'UTC' })
  const options = format.resolvedOptions()
  if (!CALENDARS.has(options.calendar)) throw new Error(`${name}: calendar ${options.calendar}`)
  // Which month of the language's calendar a date falls in, read in English:
  // some languages write the month number in Roman numerals.
  const numeric = new Intl.DateTimeFormat(`en-u-ca-${options.calendar}-nu-latn`, { month: 'numeric', timeZone: 'UTC' })
  let template = null
  const months = Array(12).fill('')
  for (let n = 0; n < 800; n++) {
    const date = utc(2026, 1, 1 + n)
    const parts = format.formatToParts(date)
    const shape = parts
      .map((p) => {
        switch (p.type) {
          case 'day':
            return '{day}'
          case 'month':
            return '{month}'
          case 'year':
            return '{year}'
          case 'literal':
          case 'era':
            return spaces(p.value)
          default:
            throw new Error(`${name}: part ${p.type}`)
        }
      })
      .join('')
    if (template === null) template = shape
    else if (template !== shape) throw new Error(`${name}: the long date changes shape: ${template} / ${shape}`)
    const index = Number(numeric.formatToParts(date).find((p) => p.type === 'month').value) - 1
    months[index] = parts.find((p) => p.type === 'month').value
  }
  if (months.some((m) => !m)) throw new Error(`${name}: a month has no name`)
  const system = options.numberingSystem
  const digits =
    system === 'latn'
      ? ''
      : Array.from({ length: 10 }, (_, d) => new Intl.NumberFormat(`${name}-u-nu-${system}`, { useGrouping: false }).format(d)).join('')
  const short = Array.from({ length: 12 }, (_, m) =>
    new Intl.DateTimeFormat(`${name}-u-ca-gregory`, { month: 'short', timeZone: 'UTC' }).format(utc(2026, m + 1, 15))
  )
  return { template, months, short, digits, calendar: options.calendar, format }
}

const tags = [
  ...new Set([
    ...fs.readdirSync('server/labels').map((f) => f.replace(/\.conf$/, '')),
    ...fs.readdirSync('../apps/calendars/web/src/locales'),
  ]),
].sort()

const languages = {}
const vectors = {}
const clocks = [utc(2026, 9, 29, 15, 4), utc(2026, 9, 29, 0, 30), utc(2026, 9, 29, 9, 5), utc(2026, 9, 29, 12, 0)]
const days = [utc(2026, 9, 29), utc(2024, 2, 29), utc(2025, 12, 31), utc(2027, 1, 1)]
// A calendar the server converts to is tested across a century, and the
// Persian year turns at the March equinox: the days either side of it.
const converted = [...days]
for (let n = 0; n < 200; n++) converted.push(utc(1990, 1, 1 + n * 191))
for (let year = 2020; year < 2032; year++) for (const d of [19, 20, 21, 22]) converted.push(utc(year, 3, d))

for (const tag of tags) {
  const own = supported(locale(tag))
  const name = own ? locale(tag) : BORROWED[tag]
  if (!name) throw new Error(`${tag}: ICU has no data and nothing is borrowed`)
  if (!own && !supported(name)) throw new Error(`${tag}: the borrowed ${name} has no data either`)
  const hours = new Intl.DateTimeFormat(name, { hour: 'numeric' }).resolvedOptions().hourCycle
  const full = clock(name, 'h23')
  const half = clock(name, 'h12')
  const date = day(name)
  languages[tag] = {
    Twelve: hours === 'h11' || hours === 'h12',
    Clocks: [full.shape, half.shape],
    Padded: [full.padded, half.padded],
    Periods: half.periods,
    Date: date.template,
    Months: date.months,
    Short: date.short,
    Digits: date.digits,
    Calendar: date.calendar,
    Own: own,
  }
  vectors[tag] = {
    full: clocks.map((c) => [c.getTime() / 1000, full.write(c)]),
    twelve: clocks.map((c) => [c.getTime() / 1000, half.write(c)]),
    day: own ? (date.calendar === 'gregory' ? days : converted).map((d) => [d.getTime() / 1000, spaces(date.format.format(d))]) : [],
  }
}

const quote = (s) => JSON.stringify(s)
const list = (a) => `{${a.map(quote).join(', ')}}`
let go = `// Code generated by build/scripts/clock.mjs from ICU ${process.versions.icu} (CLDR ${process.versions.cldr}); DO NOT EDIT.

package main

// clock_languages holds, for each language Mochi speaks, how it writes a time
// of day and a date.
var clock_languages = map[string]clock_language{
`
for (const [tag, l] of Object.entries(languages)) {
  go += `\t${quote(tag)}: {Twelve: ${l.Twelve}, Clocks: [2]string${list(l.Clocks)}, Padded: [2]bool{${l.Padded.join(', ')}}, Periods: [24]string${list(l.Periods)}, Date: ${quote(l.Date)}, Months: [12]string${list(l.Months)}, Short: [12]string${list(l.Short)}, Digits: ${quote(l.Digits)}, Calendar: ${quote(l.Calendar)}, Own: ${l.Own}},\n`
}
go += '}\n'
fs.writeFileSync('server/clock_languages.go', go)
execFileSync('gofmt', ['-w', 'server/clock_languages.go'])
fs.mkdirSync('server/testdata', { recursive: true })
fs.writeFileSync('server/testdata/clock.json', JSON.stringify({ icu: process.versions.icu, vectors }) + '\n')
console.log(`${tags.length} languages; borrowed: ${tags.filter((t) => !languages[t].Own).join(', ') || 'none'}`)

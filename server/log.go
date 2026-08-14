// Mochi server: Logging
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"fmt"
	"github.com/mattn/go-isatty"
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
	"log"
	"os"
	"slices"
	"strings"
	"sync"
	"time"
)

type log_writer struct {
}

var log_color = isatty.IsTerminal(os.Stdout.Fd()) || isatty.IsCygwinTerminal(os.Stdout.Fd())

var (
	api_log = sls.FromStringDict(sl.String("mochi.log"), sl.StringDict{
		"debug": sl.NewBuiltin("mochi.log.debug", sl_log),
		"info":  sl.NewBuiltin("mochi.log.info", sl_log),
		"warn":  sl.NewBuiltin("mochi.log.warn", sl_log),
	})
)

// Deliberately a package init(), not a call from main_serve.
//
// This must be in place before ANY code can log, and package-level variable
// initialisers throughout the package run before main() is entered - a
// hoisted version would leave anything logging during that phase writing
// through Go's default logger with its own flags and destination. The other
// startup registrations moved into main_serve (api_init, directory_init,
// peers_init, events_init, senders_init); this one cannot follow them.
func init() {
	log.SetFlags(0)
	log.SetOutput(new(log_writer))
}

func debug(message string, values ...any) {
	if !log_repeat_allow(message) {
		return
	}
	out := fmt.Sprintf(message, values...)
	if len(out) > 1000 {
		log.Print(out[:1000] + "...\n")
	} else {
		log.Print(out + "\n")
	}
}

func info(message string, values ...any) {
	if !log_repeat_allow(message) {
		return
	}
	log.Printf(message+"\n", values...)
}

// warn_application writes a warning raised by an app, and emails the admin
// under a throttle keyed on the app rather than on the message.
//
// warn() keys its throttle on the format string, which for core is the call
// site's fixed identity. An app supplies its own, so a format built from data -
// mochi.log.warn("rejected: " + text) reads like an ordinary diagnostic - opens
// a fresh key per call, and every fresh key is a first occurrence, which sends.
// Keying on the app instead bounds it to one mail per app per window however
// the app varies its text, and bounds warn_email_state to the number of
// installed apps rather than to what they choose to write.
//
// The line is also repeat-suppressed, unlike core's warn(): that exemption
// rests on warns being rare and their formats fixed, and neither holds here.
// Without it an app floods the journal through the one level that never
// suppressed, which is how the 2026-07 flood cut yuzu's history to ~35 minutes.
func warn_application(app string, message string, values ...any) {
	if !log_repeat_allow(message) {
		return
	}
	out := warn_log(message, values...)

	admin := ini_string("email", "admin", "")
	if admin == "" {
		return
	}
	send, suppressed := warn_email_allow("app:" + app)
	if !send {
		return
	}
	warn_email(admin, out, suppressed)
}

// warn_log writes a warning line and returns the formatted text.
func warn_log(message string, values ...any) string {
	out := fmt.Sprintf(message, values...)
	log.Print(out + "\n")
	return out
}

func warn(message string, values ...any) {
	out := warn_log(message, values...)

	admin := ini_string("email", "admin", "")
	if admin == "" {
		return
	}
	// Rate-limit the admin email per warn FORMAT (not per formatted message —
	// the args vary, the template is the recurring identity), so one repeating
	// fault can't flood the inbox (a tight loop once sent ~3,000 mails). The
	// log line above is always written; only the email is throttled.
	send, suppressed := warn_email_allow(message)
	if !send {
		return
	}
	warn_email(admin, out, suppressed)
}

// warn_email delivers one admin warning mail, with the rollup line when the
// throttle suppressed occurrences since the last one.
func warn_email(admin, out string, suppressed int) {
	if suppressed > 0 {
		out = fmt.Sprintf("%s\n\n(%d further warning(s) of this kind were suppressed since the last email.)", out, suppressed)
	}
	subject := "Mochi error"
	if host := server_hostname(); host != "" {
		subject += " on " + host
	}
	email_send(admin, subject, out)
}

// server_hostname returns the operator-facing name for this box for use in
// admin notifications: the `hostname` setting if set, otherwise the OS
// hostname. Unlike peer_names_announce it ignores hostname_publish — the admin
// email goes only to the operator, who already knows which box it is.
func server_hostname() string {
	name := strings.TrimSpace(setting_effective("hostname"))
	if name == "" {
		if h, err := os.Hostname(); err == nil {
			name = strings.TrimSpace(h)
		}
	}
	return name
}

// log_repeat_threshold / log_repeat_window: a format string emitting more
// than threshold lines inside one window is suppressed for the rest of that
// window, ending with a single rollup line when the window rolls. A flooding
// diagnostic call site otherwise destroys journal retention — the 2026-07
// broadcast gap flood wrote ~60 lines/sec and cut yuzu's journald to ~35
// minutes of history, evicting the evidence needed to diagnose it. Core's
// warn() is exempt: its warns are rare, important, and already email-throttled.
// An app's are not - mochi.log.warn goes through warn_application, which
// suppresses. var (not const) so tests can lower them.
var log_repeat_threshold = 20
var log_repeat_window int64 = 60

type log_repeat_record struct {
	start int64
	count int
}

// log_repeat_maximum bounds log_repeat_state.
//
// The key is the format string, and mochi.log.debug lets an app choose it. A
// format built from data - mochi.log.debug("rejected: " + text) reads like an
// ordinary diagnostic - is a fresh key on every call, so the table gains an
// entry per line and nothing else here ever removes one. The window rollover
// only replaces the entry for a format that recurs.
//
// Over the ceiling the oldest windows go. They are the ones least likely to be
// actively suppressing anything, and losing one is harmless: the format simply
// opens a fresh window on its next line.
const log_repeat_maximum = 10000

var (
	log_repeat_state = map[string]*log_repeat_record{}
	log_repeat_mutex sync.Mutex
)

// log_repeat_allow reports whether a line with this format may print now.
// Keyed by format string, not formatted output: the arguments vary per
// line, the template is the call site's identity (same scheme as
// warn_email_allow below). When a window that suppressed lines rolls over,
// the first line of the new window is preceded by a rollup naming the
// format and the suppressed count. A format that stops flooding entirely
// emits its final rollup on its next line, whenever that is.
func log_repeat_allow(format string) bool {
	now := now()
	log_repeat_mutex.Lock()
	defer log_repeat_mutex.Unlock()
	record, ok := log_repeat_state[format]
	if !ok || now-record.start >= log_repeat_window {
		if ok && record.count > log_repeat_threshold {
			log.Printf("(suppressed %d further lines of %q over %ds)\n", record.count-log_repeat_threshold, format, now-record.start)
		}
		log_repeat_state[format] = &log_repeat_record{start: now, count: 1}
		log_repeat_evict()
		return true
	}
	record.count++
	return record.count <= log_repeat_threshold
}

type warn_email_record struct {
	last       int64
	suppressed int
}

// log_repeat_evict drops the oldest windows until the table is back under its
// ceiling. Caller holds log_repeat_mutex.
//
// Sheds a slice at a time so this runs O(n) rarely rather than on every insert
// once the ceiling is reached, matching message_seen_evict.
//
// Reports through log.Printf rather than warn(): this runs with
// log_repeat_mutex held, and warn() sends the admin email inline over SMTP, so
// routing it there would hold the lock across a network round trip and stall
// every other log line on the server.
func log_repeat_evict() {
	if len(log_repeat_state) <= log_repeat_maximum {
		return
	}
	target := len(log_repeat_state) - log_repeat_maximum + log_repeat_maximum/10

	// Ordered by count first, then age. Dropping purely by age does not work:
	// now() has second resolution, so a burst of fresh formats all carry the
	// same start and a strict "older than the cutoff" test matches none of
	// them - the table stays over its ceiling and re-sorts on every insert.
	//
	// Count first also decides the right victims. A window with a high count is
	// actively suppressing a flood, and dropping it would let that flood clear
	// its own suppression by opening enough new formats to push it out, then
	// resume. A window at count 1 has printed one line and costs nothing to
	// reopen.
	formats := make([]string, 0, len(log_repeat_state))
	for format := range log_repeat_state {
		formats = append(formats, format)
	}
	slices.SortFunc(formats, func(a, b string) int {
		first, second := log_repeat_state[a], log_repeat_state[b]
		if first.count != second.count {
			return first.count - second.count
		}
		return int(first.start - second.start)
	})

	for _, format := range formats[:target] {
		delete(log_repeat_state, format)
	}
	log.Printf("(log repeat table hit its %d-entry ceiling; dropped the %d quietest window(s))\n",
		log_repeat_maximum, target)
}

var (
	warn_email_state = map[string]warn_email_record{}
	warn_email_mutex sync.Mutex
)

// warn_email_window is the minimum gap between admin emails for the same warn
// format string.
const warn_email_window = 60 * 60

// warn_email_allow reports whether the admin email for this warn format may be
// sent now. When it may, it returns the number of occurrences suppressed since
// the previous email (for a rollup line) and opens a fresh window; when it may
// not, it records the suppression and returns (false, 0). The first occurrence
// of any format always sends. In-memory only — a restart resets the windows.
func warn_email_allow(format string) (send bool, suppressed int) {
	warn_email_mutex.Lock()
	defer warn_email_mutex.Unlock()
	record := warn_email_state[format]
	if record.last != 0 && now()-record.last < warn_email_window {
		record.suppressed++
		warn_email_state[format] = record
		return false, 0
	}
	suppressed = record.suppressed
	warn_email_state[format] = warn_email_record{last: now()}
	return true, suppressed
}

// mochi.log.debug/info/warn(format, values...) -> None: Write to application log
func sl_log(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 {
		return sl_error(fn, "syntax: <format: string>, [values: variadic strings]")
	}

	format, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid format")
	}

	a, ok := t.Local("app").(*App)
	// The throttle key for mochi.log.warn's admin email. Empty for a call with
	// no app bound, which shares one key rather than opening one per format.
	app := ""
	if !ok || a == nil {
		format = fmt.Sprintf("%s(): %s", t.Local("function"), format)
	} else {
		app = a.id
		format = fmt.Sprintf("App %s:%s() %s", a.id, t.Local("function"), format)
	}

	values := make([]any, len(args)-1)
	for i, a := range args[1:] {
		values[i] = sl_decode(a)
	}

	switch fn.Name() {
	case "mochi.log.debug":
		debug(format, values...)

	case "mochi.log.info":
		info(format, values...)

	case "mochi.log.warn":
		warn_application(app, format, values...)
	}

	return sl.None, nil
}

func (writer log_writer) Write(b []byte) (int, error) {
	if bytes.HasPrefix(b, []byte("http: TLS handshake error from ")) {
		return len(b), nil
	}
	return fmt.Print(time.Now().Format("2006-01-02 15:04:05.000000") + " " + string(b))
}

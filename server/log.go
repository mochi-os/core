// Mochi server: Logging
// Copyright © 2026 Mochi OÜ
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

func warn(message string, values ...any) {
	out := fmt.Sprintf(message, values...)
	log.Print(out + "\n")

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
	name := strings.TrimSpace(setting_get("hostname", ""))
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
// minutes of history, evicting the evidence needed to diagnose it. warn()
// is exempt: warns are rare, important, and already email-throttled. var
// (not const) so tests can lower them.
var log_repeat_threshold = 20
var log_repeat_window int64 = 60

type log_repeat_record struct {
	start int64
	count int
}

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
		return true
	}
	record.count++
	return record.count <= log_repeat_threshold
}

type warn_email_record struct {
	last       int64
	suppressed int
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
	if !ok || a == nil {
		format = fmt.Sprintf("%s(): %s", t.Local("function"), format)
	} else {
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
		warn(format, values...)
	}

	return sl.None, nil
}

func (writer log_writer) Write(b []byte) (int, error) {
	if bytes.HasPrefix(b, []byte("http: TLS handshake error from ")) {
		return len(b), nil
	}
	return fmt.Print(time.Now().Format("2006-01-02 15:04:05.000000") + " " + string(b))
}

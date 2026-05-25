// Mochi server: Logging
// Copyright Alistair Cunningham 2024-2026

package main

import (
	"bytes"
	"fmt"
	"github.com/mattn/go-isatty"
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
	"log"
	"os"
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
	out := fmt.Sprintf(message, values...)
	if len(out) > 1000 {
		log.Print(out[:1000] + "...\n")
	} else {
		log.Print(out + "\n")
	}
}

func info(message string, values ...any) {
	log.Printf(message+"\n", values...)
}

func warn(message string, values ...any) {
	out := fmt.Sprintf(message, values...)
	log.Print(out + "\n")

	admin := ini_string("email", "admin", "")
	if admin != "" {
		email_send(admin, "Mochi error", out)
	}
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

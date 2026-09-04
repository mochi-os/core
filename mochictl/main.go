// mochictl: Mochi server admin/ops CLI.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.
//
// Talks to the server's admin listener - a UDS at <data_dir>/run/admin.sock, a
// named pipe on Windows - authenticated by the transport itself. Only the
// service lifecycle behind `mochictl start` is Linux-only.

package main

import (
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"core/common/adminclient"
	"core/common/ini"
	"core/common/paths"
)

// default_config is the platform default mochi.conf path (e.g.
// /etc/mochi/mochi.conf on Linux, %ProgramData%\Mochi\mochi.conf on
// Windows), shared with the server via core/common/paths.
var default_config = paths.Config()

var (
	build_version string // set via -ldflags "-X main.build_version=..."

	// Global flags. Each subcommand's own FlagSet starts after these.
	file         string
	socket       string
	flag_json    bool
	flag_tabs    bool
	flag_verbose bool
)

// command is one row of the dispatch table.
type command struct {
	help string
	run  func(args []string) error
}

// The dispatch table, filled by execute from commands.go so main.go stays
// small.
var commands map[string]command

// client builds an HTTP client targeting the admin transport: -s if set, else
// the platform default socket. Optional timeout overrides the 30-second
// default; pass 0 for streaming endpoints such as /_/admin/backup.
func client(timeouts ...time.Duration) *adminclient.Client {
	path := socket
	if path == "" {
		path = admin_socket_default()
	}
	timeout := 30 * time.Second
	if len(timeouts) > 0 {
		timeout = timeouts[0]
	}
	return adminclient.New(path, timeout)
}

func main() {
	os.Exit(execute(os.Args[1:], os.Stderr))
}

// execute is main without the process exit: parse the arguments, build the
// dispatch table, resolve the subcommand and run it. Answers the exit status,
// 2 for a usage error and 1 for a subcommand that failed, so a test can drive
// the same path the binary does.
func execute(args []string, stderr io.Writer) int {
	commands = commands_build()
	file = default_config // initial default; parse_args may override

	positional, err := parse_args(args)
	if err != nil {
		fmt.Fprintf(stderr, "mochictl: %v\n", err)
		return 2
	}
	if len(positional) == 0 {
		usage(stderr)
		return 2
	}

	// Load mochi.conf so subcommands can read [directories] data, [web] ports
	// etc. Failure to load is non-fatal — the subcommand may not need it
	// (e.g. rsync-filter prints static text).
	if err := ini.Load(file); err != nil {
		fmt.Fprintf(stderr, "mochictl: warning: cannot read %s: %v\n", file, err)
	}

	cmd, rest, ok := dispatch(commands, positional)
	if !ok {
		fmt.Fprintf(stderr, "mochictl: unknown subcommand %q\n", positional[0])
		usage(stderr)
		return 2
	}
	if err := cmd.run(rest); err != nil {
		fmt.Fprintf(stderr, "mochictl: %v\n", err)
		return 1
	}
	return 0
}

// parse_args walks the args looking for global flags (-j, -t, -f, -s,
// -h/--help) anywhere in the list, sets the matching package-level vars, and
// returns the remaining positional arguments in original order. This lets
// `mochictl status -t` work the same as `mochictl -t status`.
func parse_args(args []string) ([]string, error) {
	var positional []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch a {
		case "-h", "--help":
			usage(os.Stderr)
			os.Exit(0)
		case "-j":
			flag_json = true
		case "-t":
			flag_tabs = true
		case "-v":
			flag_verbose = true
		case "-f":
			i++
			if i >= len(args) {
				return nil, fmt.Errorf("%s requires a value", a)
			}
			file = args[i]
		case "-s":
			i++
			if i >= len(args) {
				return nil, fmt.Errorf("%s requires a value", a)
			}
			socket = args[i]
		default:
			positional = append(positional, a)
		}
	}
	return positional, nil
}

// dispatch resolves the positional arguments against the table: a one-word
// subcommand first, then a two-word one such as `broadcast lag`, whose second
// word is consumed from the arguments. One lookup for every pair, so the next
// two-word subcommand needs a table entry and nothing else.
func dispatch(table map[string]command, positional []string) (command, []string, bool) {
	name := positional[0]
	args := positional[1:]
	if c, ok := table[name]; ok {
		return c, args, true
	}
	if len(args) > 0 {
		if c, ok := table[name+" "+args[0]]; ok {
			return c, args[1:], true
		}
	}
	return command{}, args, false
}

// usage writes a short help block listing global flags and subcommands.
func usage(w io.Writer) {
	fmt.Fprintf(w, `mochictl %s — Mochi server admin/ops CLI

Usage:
  mochictl [global flags] <subcommand> [subcommand flags]

Global flags:
  -f <path>           mochi.conf path (default %s)
  -s <path>           override admin socket path
  -t                  tab-separated key/value output (TSV-style)
  -j                  JSON output (pretty-printed)
  -v                  verbose: show output for normally silent commands

Subcommands:
`, build_version, default_config)

	names := make([]string, 0, len(commands))
	for n := range commands {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		fmt.Fprintf(w, "  %-18s %s\n", n, commands[n].help)
	}
	fmt.Fprintln(w)
}

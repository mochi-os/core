// mochictl: Mochi server admin/ops CLI.
// Copyright Alistair Cunningham 2026
//
// mochictl is the operator's control tool for a running mochi-server. It
// connects to the server's UDS admin listener at <data_dir>/run/admin.sock
// and is authenticated by Unix peer credentials — no tokens, no network.
//
// See claude/plans/mochictl.md for the design.

//go:build linux

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"core/common/adminclient"
	"core/common/ini"
)

const default_config = "/etc/mochi/mochi.conf"

var (
	build_version string // set via -ldflags "-X main.build_version=..."

	// Global flags. Each subcommand's own FlagSet starts after these.
	file         string
	socket       string
	flag_json    bool
	flag_tabs bool
)

// command is one row of the dispatch table.
type command struct {
	help string
	run  func(args []string) error
}

// Subcommand handlers are populated in commands.go to keep main.go small.
var commands map[string]command

// client builds an HTTP-over-UDS client targeting the admin socket.
// Resolution order:
//  1. -s flag if set
//  2. <data_dir>/run/admin.sock from the loaded mochi.conf
//  3. /var/lib/mochi/run/admin.sock (Linux default)
//
// Optional timeout overrides the default 30-second deadline. Pass 0 for
// long-running endpoints (e.g. /_/admin/backup streams the whole tarball).
func client(timeouts ...time.Duration) *adminclient.Client {
	path := socket
	if path == "" {
		data := ini.String("directories", "data", "/var/lib/mochi")
		path = filepath.Join(data, "run", "admin.sock")
	}
	timeout := 30 * time.Second
	if len(timeouts) > 0 {
		timeout = timeouts[0]
	}
	return adminclient.New(path, timeout)
}

func main() {
	file = default_config // initial default; parse_args may override

	positional, err := parse_args(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "mochictl: %v\n", err)
		os.Exit(2)
	}
	if len(positional) == 0 {
		usage()
		os.Exit(2)
	}

	// Load mochi.conf so subcommands can read [directories] data, [web] ports
	// etc. Failure to load is non-fatal — the subcommand may not need it
	// (e.g. rsync-filter prints static text).
	if err := ini.Load(file); err != nil {
		fmt.Fprintf(os.Stderr, "mochictl: warning: cannot read %s: %v\n", file, err)
	}

	name := positional[0]
	cmd, ok := commands[name]
	args := positional[1:]
	// Allow 'config show' as a two-word subcommand.
	if !ok && name == "config" && len(args) > 0 && args[0] == "show" {
		cmd, ok = commands["config show"]
		args = args[1:]
	}
	if !ok {
		fmt.Fprintf(os.Stderr, "mochictl: unknown subcommand %q\n", name)
		usage()
		os.Exit(2)
	}

	if err := cmd.run(args); err != nil {
		fmt.Fprintf(os.Stderr, "mochictl: %v\n", err)
		os.Exit(1)
	}
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
			usage()
			os.Exit(0)
		case "-j":
			flag_json = true
		case "-t":
			flag_tabs = true
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

// usage writes a short help block listing global flags and subcommands.
func usage() {
	fmt.Fprintf(os.Stderr, `mochictl %s — Mochi server admin/ops CLI

Usage:
  mochictl [global flags] <subcommand> [subcommand flags]

Global flags:
  -f <path>           mochi.conf path (default %s)
  -s <path>           override admin socket path
  -t                  tab-separated key/value output (TSV-style)
  -j                  JSON output (pretty-printed)

Subcommands:
`, build_version, default_config)

	names := make([]string, 0, len(commands))
	for n := range commands {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		fmt.Fprintf(os.Stderr, "  %-18s %s\n", n, commands[n].help)
	}
	fmt.Fprintln(os.Stderr)
}

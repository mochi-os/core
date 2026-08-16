// Mochi server: command-line surface.
//
// The banner used to be logged before the arguments were parsed, so a rejected
// invocation wrote "Mochi X starting" to the journal and then exited without
// starting, leaving a start record with no matching shutdown. Worse, the flag
// package stops at the first non-flag argument and reports no error, so
// `mochi-server version` dropped the word and started a server with the
// default config. These cover the argument surface that both depend on.
//
// Reporting the running version is mochictl's job; the server deliberately has
// no -version flag, so it is rejected like any other unknown flag.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"io"
	"strings"
	"testing"
)

func TestServerArguments(t *testing.T) {
	const fallback = "/etc/mochi/mochi.conf"

	cases := []struct {
		name      string
		arguments []string
		config    string
		fails     bool
	}{
		{name: "no arguments uses the default config", arguments: nil, config: fallback},
		{name: "-f overrides the config", arguments: []string{"-f", "/etc/mochi/mochi1.conf"}, config: "/etc/mochi/mochi1.conf"},
		{name: "--f is accepted as the same flag", arguments: []string{"--f", "/tmp/x.conf"}, config: "/tmp/x.conf"},

		// An unrecognised argument must be refused, never treated as
		// "start normally".
		{name: "unknown flag is refused", arguments: []string{"-wat"}, fails: true},

		// The server has no version flag on purpose - mochictl reports the
		// version - so asking here is an error rather than a silent start.
		{name: "-version is refused", arguments: []string{"-version"}, fails: true},
		{name: "--version is refused", arguments: []string{"--version"}, fails: true},

		// The regression that mattered: the flag package stops at the first
		// non-flag argument and returns no error, so each of these used to
		// start a server with the word silently dropped.
		{name: "bare version subcommand is refused", arguments: []string{"version"}, fails: true},
		{name: "any positional argument is refused", arguments: []string{"serve"}, fails: true},
		{name: "positional after a valid flag is refused", arguments: []string{"-f", "/tmp/x.conf", "serve"}, fails: true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			config, err := server_arguments(c.arguments, fallback, io.Discard)
			if c.fails {
				if err == nil {
					t.Fatal("expected an error, got none — an unrecognised argument must not look like a normal start")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if config != c.config {
				t.Errorf("config = %q, want %q", config, c.config)
			}
		})
	}
}

// TestServerArgumentsReportsTheProblem — a rejected argument has to say why and
// show the usage, since that output is all the operator gets before exit 2.
// Covers both refusal paths: the flag package's own message for an unknown
// flag, and ours for a positional argument.
func TestServerArgumentsReportsTheProblem(t *testing.T) {
	for _, c := range []struct {
		name      string
		arguments []string
		expect    string
	}{
		{name: "unknown flag", arguments: []string{"-wat"}, expect: "not defined"},
		{name: "positional argument", arguments: []string{"version"}, expect: "no positional arguments"},
	} {
		t.Run(c.name, func(t *testing.T) {
			var out strings.Builder
			if _, err := server_arguments(c.arguments, "/etc/mochi/mochi.conf", &out); err == nil {
				t.Fatal("expected an error")
			}
			text := out.String()
			if !strings.Contains(text, c.expect) {
				t.Errorf("output does not explain the problem (want %q): %q", c.expect, text)
			}
			if !strings.Contains(text, "-f") {
				t.Errorf("output does not show the usage: %q", text)
			}
		})
	}
}

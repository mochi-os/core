// mochictl: the dispatch table, its two-word lookup, the shell completions
// that mirror it, and the broadcast threshold parse.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// TestDispatchResolvesTwoWordSubcommands. Five hand-written copies of the
// two-word lookup, one of them hard-coding its second word, are one generic
// lookup: the pair is looked up as a key, and its second word consumed.
func TestDispatchResolvesTwoWordSubcommands(t *testing.T) {
	table := map[string]command{
		"status":        {},
		"config show":   {},
		"broadcast lag": {},
	}
	cases := []struct {
		positional []string
		found      bool
		remaining  []string
	}{
		{[]string{"status"}, true, []string{}},
		{[]string{"status", "extra"}, true, []string{"extra"}},
		{[]string{"broadcast", "lag", "5"}, true, []string{"5"}},
		{[]string{"config", "show"}, true, []string{}},
		{[]string{"config", "bogus"}, false, nil},
		{[]string{"broadcast"}, false, nil},
		{[]string{"nope"}, false, nil},
	}
	for _, c := range cases {
		_, remaining, found := dispatch(table, c.positional)
		if found != c.found {
			t.Errorf("%v: found = %v, want %v", c.positional, found, c.found)
			continue
		}
		if found && !reflect.DeepEqual(append([]string{}, remaining...), c.remaining) {
			t.Errorf("%v: remaining = %v, want %v", c.positional, remaining, c.remaining)
		}
	}
}

// TestExecuteResolvesFromColdState. The table moved from an init() into a
// constructor and main kept indexing the package-level map, which nothing
// filled any more: every subcommand was unknown, the usage listed none, and
// both nightly production backups stopped at `mochictl snapshot`. The
// constructor and the lookup each had a test; the path the binary runs had
// none. The socket does not exist, so a resolved subcommand fails at the
// connection (status 1), never at the lookup (status 2).
func TestExecuteResolvesFromColdState(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "none.sock")
	for _, words := range [][]string{{"snapshot"}, {"version"}, {"broadcast", "lag"}} {
		commands = nil
		var stderr bytes.Buffer
		args := append([]string{"-f", filepath.Join(t.TempDir(), "none.conf"), "-s", socket}, words...)
		status := execute(args, &stderr)
		if status == 2 || strings.Contains(stderr.String(), "unknown subcommand") {
			t.Errorf("%v: status %d, stderr %q: not resolved from a cold table", words, status, stderr.String())
		} else if status != 1 {
			t.Errorf("%v: status %d, want 1 from the unreachable socket", words, status)
		}
	}
	commands = nil
	var stderr bytes.Buffer
	if status := execute(nil, &stderr); status != 2 || !strings.Contains(stderr.String(), "  snapshot ") {
		t.Errorf("no arguments: status %d, stderr %q: usage should list the subcommands", status, stderr.String())
	}
}

// completion_words returns the first words of every dispatch entry, sorted.
func completion_words(table map[string]command) []string {
	seen := map[string]bool{}
	for key := range table {
		seen[strings.Fields(key)[0]] = true
	}
	var words []string
	for word := range seen {
		words = append(words, word)
	}
	sort.Strings(words)
	return words
}

// TestCompletionsMatchTheDispatchTable. The shipped bash and zsh completions
// listed twelve subcommands against eighteen in the table and described
// health as an HTTP probe; nothing held them together. The packages are
// cross-compiled, so the files cannot be generated from the binary at build
// time; this test is what keeps them in step.
func TestCompletionsMatchTheDispatchTable(t *testing.T) {
	table := commands_build()
	want := completion_words(table)

	bash := completion_read(t, "bash-completion", "completions", "mochictl")
	match := regexp.MustCompile(`local subcommands="([^"]*)"`).FindStringSubmatch(bash)
	if match == nil {
		t.Fatal("bash completion no longer lists its subcommands in one variable")
	}
	got := strings.Fields(match[1])
	sort.Strings(got)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("bash completion lists %v\nthe dispatch table has %v", got, want)
	}

	zsh := completion_read(t, "zsh", "site-functions", "_mochictl")
	var described []string
	for _, m := range regexp.MustCompile(`(?m)^\t\t'([a-z-]+):`).FindAllStringSubmatch(zsh, -1) {
		described = append(described, m[1])
	}
	sort.Strings(described)
	if !reflect.DeepEqual(described, want) {
		t.Errorf("zsh completion describes %v\nthe dispatch table has %v", described, want)
	}
	if !strings.Contains(zsh, "'health:Check server health (UDS probe to /_/admin/health)'") {
		t.Errorf("zsh still describes health as something other than the admin-socket probe")
	}

	// Every two-word subcommand's second word completes in both shells.
	for key := range table {
		words := strings.Fields(key)
		if len(words) != 2 {
			continue
		}
		if !strings.Contains(bash, `"`+words[1]+`"`) {
			t.Errorf("bash completion does not offer %q after %q", words[1], words[0])
		}
		if !strings.Contains(zsh, "'"+words[1]+"[") {
			t.Errorf("zsh completion does not offer %q after %q", words[1], words[0])
		}
	}
	for name, text := range map[string]string{"bash": bash, "zsh": zsh} {
		if strings.Contains(text, "Phase 8") {
			t.Errorf("%s completion still carries the stub placeholder", name)
		}
	}
}

func completion_read(t *testing.T, parts ...string) string {
	t.Helper()
	path := filepath.Join(append([]string{"..", "install", "usr", "share"}, parts...)...)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	return string(data)
}

// TestBroadcastThresholdRejectsBadValues. A threshold that did not parse was
// dropped silently, so `broadcast lag 1O` printed every row under a filter the
// operator believed was applied.
func TestBroadcastThresholdRejectsBadValues(t *testing.T) {
	for _, value := range []string{"1O", "5s", "-5", "", "1.5"} {
		if _, err := broadcast_threshold(value); err == nil {
			t.Errorf("%q: accepted, want an error", value)
		}
	}
	for value, want := range map[string]int{"0": 0, "5": 5, "120": 120} {
		got, err := broadcast_threshold(value)
		if err != nil || got != want {
			t.Errorf("%q: got %d, %v; want %d", value, got, err, want)
		}
	}
}

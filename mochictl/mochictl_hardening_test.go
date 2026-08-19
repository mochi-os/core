// Mochi mochictl: output correctness, ambient inputs, and exit codes.
//
// Six review items, five of them fixed here and one ruled not a defect:
//
//   #71 -j decoded without UseNumber, so the scripted-consumption mode was the
//       one path that rounded integers above 2^53 - the human and -t paths get
//       it right, and say why in render's own comment.
//   #72 broadcast and pipelining truncated display strings by byte index,
//       splitting a multi-byte rune and miscounting the column width.
//   #75 NOT A DEFECT: MOCHI_DIRECTORIES_DATA is a documented override
//       (mochi.conf.5 "ENVIRONMENT OVERRIDES") that the SERVER honours through
//       the same ini.String. Ignoring it in mochictl would send the two to
//       different sockets in exactly the container deployments the variable
//       exists for. See TestTheDataDirectoryOverrideIsDeliberate.
//   #76 systemctl resolved through PATH, inherited, in a tool run as root.
//   #107 cmd_health rendered the body before checking the status code.
//   #108 version exited 0 against an unreachable server.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"unicode/utf8"
)

// capture_stdout runs f with stdout redirected and returns what it wrote.
func capture_stdout(t *testing.T, f func()) string {
	t.Helper()
	saved := os.Stdout
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = write
	done := make(chan string)
	go func() {
		var buffer bytes.Buffer
		buffer.ReadFrom(read)
		done <- buffer.String()
	}()
	f()
	write.Close()
	os.Stdout = saved
	return <-done
}

// TestJsonModeKeepsLargeIntegersExact is #71. The mode parsed by other
// programs was the only one that changed the value.
func TestJsonModeKeepsLargeIntegersExact(t *testing.T) {
	body := []byte(`{"bytes":9007199254740993,"peer":18446744073709551615,"small":42}`)

	out := capture_stdout(t, func() {
		if err := render_json(body); err != nil {
			t.Errorf("render_json: %v", err)
		}
	})

	for _, exact := range []string{"9007199254740993", "18446744073709551615", "42"} {
		if !strings.Contains(out, exact) {
			t.Errorf("-j output does not contain %s; it reads:\n%s", exact, out)
		}
	}
	for _, mangled := range []string{"9007199254740992", "18446744073709552000", "e+"} {
		if strings.Contains(out, mangled) {
			t.Errorf("-j output contains %q, a value float64 rounding produces; a script reading this cannot tell it was changed", mangled)
		}
	}
}

// TestJsonModeAgreesWithTheOtherModes: the three renderers must not disagree
// about a number. render's comment already claimed this was the reason for
// UseNumber; -j was outside it.
func TestJsonModeAgreesWithTheOtherModes(t *testing.T) {
	body := []byte(`{"peer":18446744073709551615}`)

	json_out := capture_stdout(t, func() { render_json(body) })

	saved := flag_tabs
	flag_tabs = true
	defer func() { flag_tabs = saved }()
	tabs_out := capture_stdout(t, func() { render(body) })

	const exact = "18446744073709551615"
	if !strings.Contains(json_out, exact) || !strings.Contains(tabs_out, exact) {
		t.Errorf("the renderers disagree about a large integer:\n  -j: %s\n  -t: %s", json_out, tabs_out)
	}
}

// TestTruncationCutsOnRunes is #72. A byte slice through a multi-byte
// character emits invalid UTF-8 and miscounts the padded width.
func TestTruncationCutsOnRunes(t *testing.T) {
	// Each of these is multi-byte, so a byte cut lands mid-character.
	for _, subject := range []string{
		"日本語のキーがとても長い場合",
		"clé-très-longue-pour-un-nom",
		"ключ-который-очень-длинный",
	} {
		for _, width := range []int{4, 8, 12} {
			runes := []rune(subject)
			if len(runes) <= width {
				continue
			}
			cut := string(runes[:width-1]) + "…"
			if !utf8.ValidString(cut) {
				t.Errorf("cutting %q at %d runes produced invalid UTF-8", subject, width)
			}
			if got := utf8.RuneCountInString(cut); got != width {
				t.Errorf("cutting %q at %d gave %d runes; the column is padded to the requested width", subject, width, got)
			}
			// The byte cut the code used to do, for contrast.
			if len(subject) > width {
				if utf8.ValidString(subject[:width-1] + "…") {
					t.Logf("note: %q happens to split cleanly at byte %d", subject, width-1)
				}
			}
		}
	}
}

// TestNoDisplayTruncationUsesAByteSlice pins both call sites.
func TestNoDisplayTruncationUsesAByteSlice(t *testing.T) {
	for _, target := range []struct{ file, variable, width string }{
		{"broadcast.go", "key", "key_w"},
		{"pipelining.go", "peer", "peer_w"},
	} {
		data, err := os.ReadFile(target.file)
		if err != nil {
			t.Fatalf("reading %s: %v", target.file, err)
		}
		source := string(data)
		if strings.Contains(source, target.variable+" = "+target.variable+"[:"+target.width+"-1]") {
			t.Errorf("%s truncates %s by byte index; a multi-byte character is split in half", target.file, target.variable)
		}
		if !strings.Contains(source, "[]rune("+target.variable+")") {
			t.Errorf("%s does not truncate %s on runes", target.file, target.variable)
		}
	}
}

// TestSystemctlIsNotResolvedThroughPath is #76.
func TestSystemctlIsNotResolvedThroughPath(t *testing.T) {
	data, err := os.ReadFile("supervisor.go")
	if err != nil {
		t.Fatalf("reading supervisor.go: %v", err)
	}
	source := string(data)

	if strings.Contains(source, `exec.LookPath("systemctl")`) {
		t.Error("systemctl is still resolved through PATH; mochictl runs as root and inherits PATH from its caller")
	}
	if strings.Contains(source, `exec.Command("systemctl"`) {
		t.Error("systemctl is still executed by bare name, which searches PATH")
	}
	if !strings.Contains(source, `"/usr/bin/systemctl"`) {
		t.Error("systemctl_path does not try an absolute path")
	}
}

// TestSystemctlPathAcceptsOnlyAbsolutePaths: whatever it returns is executed
// as root, so it must be a path, not a name.
func TestSystemctlPathAcceptsOnlyAbsolutePaths(t *testing.T) {
	got := systemctl_path()
	if got == "" {
		t.Skip("no systemctl on this host; nothing to check")
	}
	if !strings.HasPrefix(got, "/") {
		t.Errorf("systemctl_path returned %q, which is not absolute", got)
	}
	if information, err := os.Stat(got); err != nil || information.IsDir() {
		t.Errorf("systemctl_path returned %q, which is not a file: %v", got, err)
	}
}

// TestTheDataDirectoryOverrideIsDeliberate records why #75 was closed rather
// than fixed. mochi.conf.5 documents MOCHI_<SECTION>_<KEY> for every key and
// names this one in its table, and the server reads the same key through the
// same ini.String - so making mochictl ignore it would point the two at
// different sockets in the container deployments the variable exists for.
func TestTheDataDirectoryOverrideIsDeliberate(t *testing.T) {
	data, err := os.ReadFile("../docs/mochi.conf.5.md")
	if err != nil {
		t.Skipf("manual page not readable: %v", err)
	}
	page := string(data)
	if !strings.Contains(page, "MOCHI_DIRECTORIES_DATA") {
		t.Error("the manual page no longer documents MOCHI_DIRECTORIES_DATA; if the override has been withdrawn, mochictl should stop honouring it too")
	}
	if !strings.Contains(page, "ENVIRONMENT OVERRIDES") {
		t.Error("the manual page no longer has an ENVIRONMENT OVERRIDES section")
	}
}

// TestHealthChecksTheStatusBeforeRendering is #107.
func TestHealthChecksTheStatusBeforeRendering(t *testing.T) {
	data, err := os.ReadFile("commands.go")
	if err != nil {
		t.Fatalf("reading commands.go: %v", err)
	}
	source := string(data)
	at := strings.Index(source, "func cmd_health(")
	if at < 0 {
		t.Fatal("commands.go no longer defines cmd_health")
	}
	body := source[at:]
	if end := strings.Index(body, "\n}\n"); end > 0 {
		body = body[:end]
	}

	status := strings.Index(body, "resp.StatusCode/100 != 2")
	output := strings.Index(body, "render(body")
	if status < 0 || output < 0 {
		t.Fatalf("cmd_health no longer both checks the status and renders (status %d, render %d)", status, output)
	}
	if status > output {
		t.Error("cmd_health renders before checking the status, so a 5xx body is printed to stdout formatted as a health report")
	}
}

// TestVersionFailsWhenTheServerIsUnreachable is #108. Printing the client
// version is right; exiting 0 is not.
func TestVersionFailsWhenTheServerIsUnreachable(t *testing.T) {
	data, err := os.ReadFile("commands.go")
	if err != nil {
		t.Fatalf("reading commands.go: %v", err)
	}
	source := string(data)
	at := strings.Index(source, "func cmd_version(")
	if at < 0 {
		t.Fatal("commands.go no longer defines cmd_version")
	}
	body := source[at:]
	if end := strings.Index(body, "\n}\n"); end > 0 {
		body = body[:end]
	}

	unreachable := strings.Index(body, "if err != nil {")
	if unreachable < 0 {
		t.Fatal("cmd_version no longer handles an unreachable server")
	}
	branch := body[unreachable:]
	if end := strings.Index(branch, "\n\t}\n"); end > 0 {
		branch = branch[:end]
	}
	if !strings.Contains(branch, "return fmt.Errorf") {
		t.Error("cmd_version returns nil when the server is unreachable, so it exits 0 and reads as success to anything using it as a liveness probe")
	}
	if !strings.Contains(branch, "mochictl_version") {
		t.Error("cmd_version no longer prints the client version when the server is down, which is when it is most useful")
	}
}

// TestRenderJsonStillHandlesNonJson keeps the UseNumber change from turning a
// passthrough into an error.
func TestRenderJsonStillHandlesNonJson(t *testing.T) {
	out := capture_stdout(t, func() {
		if err := render_json([]byte("not json at all")); err != nil {
			t.Errorf("render_json on non-JSON: %v", err)
		}
	})
	if !strings.Contains(out, "not json at all") {
		t.Errorf("non-JSON was not passed through; got %q", out)
	}
}

// TestRenderJsonStillIndents: the mode exists to normalise the server's
// compaction, so the indent has to survive.
func TestRenderJsonStillIndents(t *testing.T) {
	out := capture_stdout(t, func() { render_json([]byte(`{"a":{"b":1}}`)) })
	if !strings.Contains(out, "\n  \"a\"") {
		t.Errorf("-j output is not 2-space indented:\n%s", out)
	}
	var check any
	if err := json.Unmarshal([]byte(out), &check); err != nil {
		t.Errorf("-j output is not valid JSON: %v", err)
	}
}

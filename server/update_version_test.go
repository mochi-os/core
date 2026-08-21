// Mochi server: the self-install version is an argument, not a trusted token.
//
// update_install_start's argument becomes a filename under data_dir/tmp and a
// quoted argument on the cmd.exe line that runs msiexec, so a quote chains a
// command and "../" walks out. valid(version, "version") is the gate.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"strings"
	"testing"
)

// hostile_versions are strings that must never reach a path or a command line.
// The comment on each says which of the two it targets.
var hostile_versions = []string{
	`9.0.0" & calc & "`,                // closes msiexec's quote, chains a command
	`9.0.0"; whoami; "`,                // same, semicolon form
	`9.0.0" /quiet /l*v "C:\evil.log`,  // injects extra msiexec switches
	`9.0.0 & shutdown /r`,              // no quote needed; cmd.exe splits on &
	`../../../../Windows/Temp/payload`, // walks out of data_dir/tmp
	`..\..\..\Windows\Temp\payload`,    // the Windows separator form
	`9.0.0/../../evil`,                 // traversal after a plausible prefix
	`9.0.0` + "\n" + `del /q C:\*`,     // newline, in case it reaches a shell
	`9.0.0%00`,                         // truncation attempt
	`9.0.0 `,                           // trailing space: a distinct filename
}

func TestUpdateInstallRejectsHostileVersions(t *testing.T) {
	for _, version := range hostile_versions {
		t.Run(version, func(t *testing.T) {
			err := update_install_start(version)
			if err == nil {
				t.Fatalf("update_install_start(%q) returned no error", version)
			}
			if !strings.Contains(err.Error(), "invalid version") {
				t.Errorf("update_install_start(%q) failed with %q; want the version check to be what refuses it, not a later gate that happens to", version, err)
			}
		})
	}
}

// TestUpdateInstallValidatesBeforeWritingTheSetting is the half that was
// reachable without a hostile manifest: update_pending was written from the
// unvalidated argument, and api_server_update_install hands that setting
// straight back as the status dict's "pending" field.
func TestUpdateInstallValidatesBeforeWritingTheSetting(t *testing.T) {
	source, err := os.ReadFile("update.go")
	if err != nil {
		t.Fatalf("reading update.go: %v", err)
	}
	body := string(source)
	at := strings.Index(body, "func update_install_start(")
	if at < 0 {
		t.Fatal("update.go no longer defines update_install_start")
	}
	body = body[at:]
	if end := strings.Index(body, "\n}\n"); end > 0 {
		body = body[:end]
	}

	check := strings.Index(body, `valid(version, "version")`)
	write := strings.Index(body, `setting_set("update_pending", version)`)
	if check < 0 {
		t.Fatal("update_install_start does not validate the version")
	}
	if write < 0 {
		t.Fatal("update_install_start no longer writes update_pending; this test's premise is gone")
	}
	if check > write {
		t.Error("update_install_start writes update_pending before validating the version, so an arbitrary string still reaches the settings row and the update-status API")
	}
}

// TestUpdateInstallAcceptsRealVersions: the guard must not be so tight that a
// genuine release is refused. These get past validation and are then stopped
// by the platform check, which is the correct outcome on a non-Windows host.
func TestUpdateInstallAcceptsRealVersions(t *testing.T) {
	for _, version := range []string{"0.4.231", "1.0", "2026.08.20", "1.0.0-rc1", "0.4.231_2"} {
		err := update_install_start(version)
		if err == nil {
			t.Errorf("update_install_start(%q) succeeded on %s; expected the platform check to stop it", version, build_platform)
			continue
		}
		if strings.Contains(err.Error(), "invalid version") {
			t.Errorf("update_install_start(%q) rejected a well-formed version: %v", version, err)
		}
	}
}

// TestVersionValidatorRefusesShellAndPathMetacharacters pins the property the
// guard depends on, independently of update.go. If valid() is ever relaxed,
// this fails here rather than silently reopening the install path.
func TestVersionValidatorRefusesShellAndPathMetacharacters(t *testing.T) {
	for _, character := range []string{
		`"`, `'`, "`", `&`, `|`, `;`, `<`, `>`, `^`, `%`, `!`,
		`/`, `\`, ` `, "\t", "\n", "\r", `*`, `?`, `$`, `(`, `)`,
	} {
		if valid("1.0"+character+"0", "version") {
			t.Errorf("valid() accepts %q in a version; it becomes a cmd.exe argument and a filename", character)
		}
	}
	if valid("1.0..0", "version") {
		t.Error(`valid() accepts ".." in a version`)
	}
	// The characters a real version needs must still pass.
	for _, version := range []string{"0.4.231", "1.0.0-rc1", "1_0", "20260820"} {
		if !valid(version, "version") {
			t.Errorf("valid() rejects %q, which is a shape real releases use", version)
		}
	}
}

// TestSpawnedCommandLineQuotesTheMsiPath records why the guard has to be this
// strict: the path is interpolated into a cmd.exe line inside double quotes,
// so the quote character is the whole attack.
func TestSpawnedCommandLineQuotesTheMsiPath(t *testing.T) {
	source, err := os.ReadFile("update_windows.go")
	if err != nil {
		t.Skipf("update_windows.go not readable: %v", err)
	}
	body := string(source)
	if !strings.Contains(body, `msiexec /i "`) {
		t.Skip("the msiexec invocation has changed shape; re-read it before trusting this test")
	}
	if !strings.Contains(body, "cmd /c ") {
		t.Error("update_install_spawn no longer goes through cmd.exe; if the shell is gone the version guard is still wanted for the path, but this test's reasoning needs revisiting")
	}
}

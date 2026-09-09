// Mochi server: an execute file missing from disk refuses the app version.
// starlark() warns and loads the rest, so an app whose own files loaded but
// whose shared-library link did not (a fresh checkout without the vendored
// symlinks) failed later inside database_create as a database error that
// never named the file.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// execute_app writes an app.json naming the execute files and creates only
// the ones listed in present.
func execute_app(t *testing.T, execute []string, present []string) string {
	t.Helper()
	base := t.TempDir()
	manifest := `{"version":"1.0","label":"app","architecture":{"engine":"starlark","version":` +
		itoa(app_version_maximum) + `},"execute":["` + strings.Join(execute, `","`) + `"]}`
	if err := os.WriteFile(filepath.Join(base, "app.json"), []byte(manifest), 0o600); err != nil {
		t.Fatalf("writing app.json: %v", err)
	}
	for _, file := range present {
		path := filepath.Join(base, file)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatalf("creating %s: %v", filepath.Dir(file), err)
		}
		if err := os.WriteFile(path, []byte(""), 0o600); err != nil {
			t.Fatalf("writing %s: %v", file, err)
		}
	}
	return base
}

func TestAppReadRefusesAMissingExecuteFile(t *testing.T) {
	base := execute_app(t, []string{"lib/attachments.star", "app.star"}, []string{"app.star"})

	_, err := app_read("app", base)

	if err == nil {
		t.Fatal("app_read loaded a version whose execute file is not on disk; starlark() would warn and load the rest, and the app fails later as a database error")
	}
	if !strings.Contains(err.Error(), "lib/attachments.star") {
		t.Errorf("the error does not name the missing file: %v", err)
	}
}

func TestAppReadAcceptsPresentExecuteFiles(t *testing.T) {
	base := execute_app(t, []string{"lib/attachments.star", "app.star"}, []string{"lib/attachments.star", "app.star"})

	if _, err := app_read("app", base); err != nil {
		t.Fatalf("app_read refused a version whose execute files are all present: %v", err)
	}
}

func TestReloadRefusesAMissingExecuteFile(t *testing.T) {
	av := reload_app(t, `{"version":"1.1","label":"reloaded","architecture":{"engine":"starlark","version":`+
		itoa(app_version_maximum)+`},"execute":["lib/attachments.star"]}`)

	reload_now(av)

	if av.Label != "loaded" {
		t.Error("reload applied a manifest naming an execute file that is not on disk; app_read refuses it at load")
	}
}

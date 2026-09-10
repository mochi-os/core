// Mochi server: mochi.app.package.check answers a package's refusal as text
// and installs nothing. The publisher and the apps app ask it before the
// install, so a malformed upload is a bad request naming the reason rather
// than the raised install error, which aborts the action as a server fault and
// mails the administrator.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	sl "go.starlark.net/starlark"
)

func package_check_call(t *testing.T, thread *sl.Thread, file string) (string, error) {
	t.Helper()
	fn := sl.NewBuiltin("mochi.app.package.check", api_app_package_check)
	value, err := api_app_package_check(thread, fn, sl.Tuple{sl.String(file)}, nil)
	if err != nil {
		return "", err
	}
	text, _ := sl.AsString(value)
	return text, nil
}

func TestPackageCheckNamesTheRefusal(t *testing.T) {
	thread, user, caller, _ := package_install_setup(t)
	file := "packages/refused.zip"
	// An empty version is what manifest_validate refuses first.
	package_install_zip(t, api_file_path(user, caller, file), "", `"refused"`)

	reason, err := package_check_call(t, thread, file)
	if err != nil {
		t.Fatalf("check raised instead of answering: %v", err)
	}
	if !strings.Contains(reason, "App bad version") {
		t.Fatalf("reason %q does not name the manifest refusal", reason)
	}
}

func TestPackageCheckAnswersEmptyForAnInstallablePackage(t *testing.T) {
	thread, user, caller, _ := package_install_setup(t)
	file := "packages/accepted.zip"
	package_install_zip(t, api_file_path(user, caller, file), "1.0", `"accepted"`)

	reason, err := package_check_call(t, thread, file)
	if err != nil {
		t.Fatalf("check raised: %v", err)
	}
	if reason != "" {
		t.Fatalf("an installable package was refused: %q", reason)
	}
}

func TestPackageCheckInstallsNothing(t *testing.T) {
	thread, user, caller, _ := package_install_setup(t)
	file := "packages/accepted.zip"
	package_install_zip(t, api_file_path(user, caller, file), "1.0", `"accepted"`)

	if _, err := package_check_call(t, thread, file); err != nil {
		t.Fatalf("check raised: %v", err)
	}
	// The unpacked tree is discarded, and no app directory appears.
	if left, _ := filepath.Glob(filepath.Join(data_dir, "tmp", "app_install_*")); len(left) != 0 {
		t.Fatalf("check left its unpacked tree behind: %v", left)
	}
	if installed, _ := filepath.Glob(filepath.Join(data_dir, "apps", "*", "1.0")); len(installed) != 0 {
		t.Fatalf("check installed the package: %v", installed)
	}
}

func TestPackageCheckRefusesAnUnreadableArchive(t *testing.T) {
	thread, user, caller, _ := package_install_setup(t)
	file := "packages/broken.zip"
	path := api_file_path(user, caller, file)
	package_install_zip(t, path, "1.0", `"broken"`)
	if err := os.WriteFile(path, []byte("NOTAZIPFILE"), 0644); err != nil {
		t.Fatalf("overwrite: %v", err)
	}

	reason, err := package_check_call(t, thread, file)
	if err != nil {
		t.Fatalf("check raised: %v", err)
	}
	if reason == "" {
		t.Fatal("bytes that are not a zip were accepted")
	}
}

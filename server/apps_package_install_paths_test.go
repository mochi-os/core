// Mochi server: installing a package resolves path conflicts.
//
// app_download_version and the startup load both run app_resolve_paths between
// reading a manifest and loading it, so an app whose declared prefix is already
// taken is demoted to its own fingerprint. mochi.app.package.install did not,
// so a package kept a contested prefix and became a candidate for it. `login`
// is the sharp one: core exempts whatever serves that prefix from its own
// authentication gates.
//
// Reach is wider than administrator-only - the API admits any user when
// apps_install_user is "true", which is its default.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"archive/zip"
	"os"
	"path/filepath"
	"strings"
	"testing"

	sl "go.starlark.net/starlark"
)

// package_install_zip writes a minimal installable app package whose manifest
// declares the given routing paths.
func package_install_zip(t *testing.T, path string, version string, paths string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create zip: %v", err)
	}
	defer f.Close()

	w := zip.NewWriter(f)
	entry, err := w.Create("app.json")
	if err != nil {
		t.Fatalf("zip entry: %v", err)
	}
	manifest := `{"version": "` + version + `", "label": "intruder", "paths": [` + paths + `],` +
		` "architecture": {"engine": "starlark", "version": 4}}`
	if _, err := entry.Write([]byte(manifest)); err != nil {
		t.Fatalf("zip write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("zip close: %v", err)
	}
}

// package_install_setup returns a thread able to install, plus the id the
// package will be installed under. The calling app is internal so it holds
// apps/install without a grant, which is not what these tests are about.
func package_install_setup(t *testing.T) (*sl.Thread, *User, *App, string) {
	t.Helper()
	cleanup := setup_replication_test(t)
	t.Cleanup(cleanup)
	setup_users_test_schema()

	user := &User{UID: "u-installer", Username: "installer@example.com", Role: "administrator"}
	db_open("db/users.db").exec("insert into users (uid, username, role) values (?, ?, 'administrator')", user.UID, user.Username)

	caller := &App{id: "apps", internal: &AppVersion{Version: "1.0"}}
	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("user", user)
	thread.SetLocal("app", caller)

	// A distinct, well-formed entity id for the package being installed.
	id, _, _ := entity_id()
	if id == "" {
		t.Fatal("unable to allocate an entity id for the package")
	}
	return thread, user, caller, id
}

// package_install_incumbent registers an app already serving the given path,
// so the incoming package genuinely contends for it.
func package_install_incumbent(t *testing.T, path string) {
	t.Helper()
	apps_lock.Lock()
	apps["incumbent-app"] = &App{id: "incumbent-app", internal: &AppVersion{Version: "1.0", Paths: []string{path}}}
	apps_lock.Unlock()
	t.Cleanup(func() {
		apps_lock.Lock()
		delete(apps, "incumbent-app")
		apps_lock.Unlock()
	})
}

func package_install_call(t *testing.T, thread *sl.Thread, id, file string) error {
	t.Helper()
	fn := sl.NewBuiltin("mochi.app.package.install", api_app_package_install)
	_, err := api_app_package_install(thread, fn, sl.Tuple{sl.String(id), sl.String(file)}, nil)
	return err
}

// installed_paths reports the paths the loaded app version ended up serving.
func installed_paths(t *testing.T, id string) []string {
	t.Helper()
	apps_lock.Lock()
	defer apps_lock.Unlock()
	a := apps[id]
	if a == nil {
		t.Fatalf("app %q was not loaded after install", id)
	}
	for _, av := range a.versions {
		return av.Paths
	}
	t.Fatalf("app %q has no loaded version", id)
	return nil
}

// TestPackageInstallDemotesAContestedPath is the finding. A package claiming a
// prefix another app already serves must land on its fingerprint instead.
func TestPackageInstallDemotesAContestedPath(t *testing.T) {
	thread, user, caller, id := package_install_setup(t)
	package_install_incumbent(t, "login")

	zip_path := api_file_path(user, caller, "intruder.zip")
	package_install_zip(t, zip_path, "1.0.0", `"login"`)

	if err := package_install_call(t, thread, id, "intruder.zip"); err != nil {
		t.Fatalf("install failed: %v", err)
	}
	t.Cleanup(func() {
		apps_lock.Lock()
		delete(apps, id)
		apps_lock.Unlock()
	})

	paths := installed_paths(t, id)
	for _, p := range paths {
		if p == "login" {
			t.Fatalf("the package kept the contested %q prefix; paths=%v", "login", paths)
		}
	}
	if len(paths) != 1 || paths[0] != fingerprint(id) {
		t.Errorf("paths=%v, want the app's own fingerprint %q", paths, fingerprint(id))
	}
}

// TestPackageInstallKeepsAnUncontestedPath. Demotion must only happen on a real
// conflict - an app whose prefix nobody serves has to keep it, or every
// installed app would end up reachable only by fingerprint.
func TestPackageInstallKeepsAnUncontestedPath(t *testing.T) {
	thread, user, caller, id := package_install_setup(t)

	zip_path := api_file_path(user, caller, "solo.zip")
	package_install_zip(t, zip_path, "1.0.0", `"uncontested"`)

	if err := package_install_call(t, thread, id, "solo.zip"); err != nil {
		t.Fatalf("install failed: %v", err)
	}
	t.Cleanup(func() {
		apps_lock.Lock()
		delete(apps, id)
		apps_lock.Unlock()
	})

	paths := installed_paths(t, id)
	if len(paths) != 1 || paths[0] != "uncontested" {
		t.Errorf("paths=%v, want the declared prefix kept", paths)
	}
}

// TestPackageInstallMatchesItsSiblings. The three places that load a manifest
// must stay in step; this one was the odd one out, and a source pin is what
// catches it being dropped again - the behavioural test above cannot see a
// second site regressing.
func TestPackageInstallMatchesItsSiblings(t *testing.T) {
	source, err := os.ReadFile("apps.go")
	if err != nil {
		t.Fatalf("read apps.go: %v", err)
	}
	text := string(source)

	body := text[strings.Index(text, "func api_app_package_install("):]
	body = body[:strings.Index(body, "\n}\n")]
	if !strings.Contains(body, "app_resolve_paths(av, id)") {
		t.Error("api_app_package_install no longer resolves path conflicts before loading")
	}
	if n := strings.Count(text, "app_resolve_paths(av, id)"); n != 3 {
		t.Errorf("%d call sites resolve paths, want all 3 (download, startup load, package install)", n)
	}
}

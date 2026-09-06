// Mochi server: the install API's user gate is about installing. A check-only
// call validates a package and installs nothing, and it is the only manifest
// validation an app can ask for, so it must stay open to a non-administrator
// even when the administrator has switched apps_install_user off. A real
// install by that same user must still be refused.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"strings"
	"testing"

	sl "go.starlark.net/starlark"
)

// package_install_owner_setup mirrors package_install_setup for an ordinary
// user on a server whose administrator has switched user installs off, which
// is the configuration the publisher's upload used to fail under.
func package_install_owner_setup(t *testing.T) (*sl.Thread, *User, *App, string) {
	t.Helper()
	setup_replication_test(t)
	setup_users_test_schema()
	setup_settings_test_schema()
	setting_set("apps_install_user", "false")

	user := &User{UID: "u-owner", Username: "owner@example.com", Role: "user"}
	db_open("db/users.db").exec("insert into users (uid, username, role) values (?, ?, 'user')", user.UID, user.Username)

	caller := &App{id: "publisher", internal: &AppVersion{Version: "1.0"}}
	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("user", user)
	thread.SetLocal("app", caller)

	id, _, _ := entity_id()
	if id == "" {
		t.Fatal("unable to allocate an entity id for the package")
	}
	return thread, user, caller, id
}

func package_install_loaded(id string) bool {
	apps_lock.Lock()
	defer apps_lock.Unlock()
	_, loaded := apps[id]
	return loaded
}

// TestPackageInstallCheckOnlyOpenWithUserInstallsOff is the finding: with
// apps_install_user off, an ordinary user's check-only call still validates the
// package and answers its version, and nothing is loaded.
func TestPackageInstallCheckOnlyOpenWithUserInstallsOff(t *testing.T) {
	thread, user, caller, id := package_install_owner_setup(t)
	package_install_zip(t, api_file_path(user, caller, "candidate.zip"), "2.3.4", `"candidate"`)

	fn := sl.NewBuiltin("mochi.app.package.install", api_app_package_install)
	value, err := api_app_package_install(thread, fn, sl.Tuple{sl.String(id), sl.String("candidate.zip"), sl.Bool(true)}, nil)
	if err != nil {
		t.Fatalf("check-only install refused for a non-administrator with apps_install_user off: %v", err)
	}
	version, _ := sl.AsString(value)
	if version != "2.3.4" {
		t.Errorf("check-only answered %q, want the manifest's version 2.3.4", version)
	}
	if package_install_loaded(id) {
		t.Errorf("check-only loaded app %q; validation must install nothing", id)
	}
}

// TestPackageInstallStillRefusesUserInstallsOff is the control: the same user
// on the same server is refused a real install, and nothing is loaded.
func TestPackageInstallStillRefusesUserInstallsOff(t *testing.T) {
	thread, user, caller, id := package_install_owner_setup(t)
	package_install_zip(t, api_file_path(user, caller, "candidate.zip"), "2.3.4", `"candidate"`)

	fn := sl.NewBuiltin("mochi.app.package.install", api_app_package_install)
	_, err := api_app_package_install(thread, fn, sl.Tuple{sl.String(id), sl.String("candidate.zip"), sl.Bool(false)}, nil)
	if err == nil || !strings.Contains(err.Error(), "not administrator") {
		t.Fatalf("real install by a non-administrator with apps_install_user off: err=%v, want the not administrator refusal", err)
	}
	if package_install_loaded(id) {
		t.Errorf("refused install loaded app %q", id)
	}
}

// Mochi server: an app's install timestamp is stamped once. load_version runs
// on every startup load, not only on install, so a rewriting insert flattens
// every timestamp and app_select_best's "earliest install wins" tie-break stops
// firing.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"strings"
	"testing"
)

func apps_record_setup(t *testing.T) {
	t.Helper()
	data_dir = t.TempDir()
	os.MkdirAll(data_dir+"/db", 0755)
	db := db_apps()
	db.exec("create table if not exists apps (app text not null primary key, installed integer not null)")
	db.exec("delete from apps")
}

// TestAppsRecordStampsOnce is the finding: a later call must not move the
// timestamp a tie-break depends on.
func TestAppsRecordStampsOnce(t *testing.T) {
	apps_record_setup(t)

	apps_record("app-early")
	first := apps_installed("app-early")
	if first == 0 {
		t.Fatal("first record did not stamp")
	}

	// Backdate, as a real install days ago would be, then re-record the way a
	// restart's load_version does.
	db_apps().exec("update apps set installed=? where app=?", first-86400, "app-early")
	backdated := apps_installed("app-early")

	apps_record("app-early")

	if got := apps_installed("app-early"); got != backdated {
		t.Errorf("timestamp moved from %d to %d; a reload must not re-stamp", backdated, got)
	}
}

// TestAppsRecordPreservesInstallOrder. The property app_select_best actually
// depends on: an app installed earlier must still compare as earlier after a
// restart has re-run load_version over both.
func TestAppsRecordPreservesInstallOrder(t *testing.T) {
	apps_record_setup(t)

	apps_record("app-old")
	apps_record("app-new")
	// Space them as two real installs would be, oldest first.
	db_apps().exec("update apps set installed=? where app=?", now()-7*86400, "app-old")
	db_apps().exec("update apps set installed=? where app=?", now()-86400, "app-new")

	old_before, new_before := apps_installed("app-old"), apps_installed("app-new")
	if !(old_before < new_before) {
		t.Fatalf("setup wrong: %d not before %d", old_before, new_before)
	}

	// A restart re-loads every app, in sorted id order - "app-new" first here,
	// which is exactly the case that used to invert the ordering.
	apps_record("app-new")
	apps_record("app-old")

	old_after, new_after := apps_installed("app-old"), apps_installed("app-new")
	if old_after != old_before || new_after != new_before {
		t.Errorf("a reload rewrote timestamps: old %d->%d, new %d->%d", old_before, old_after, new_before, new_after)
	}
	if !(old_after < new_after) {
		t.Errorf("install order inverted after reload: old=%d new=%d", old_after, new_after)
	}
}

// TestAppsRecordStampsAFreshApp. Write-once must still write the first time -
// an app whose timestamp stayed 0 would sort as "no install time recorded" and
// fall through app_select_best's comparison entirely.
func TestAppsRecordStampsAFreshApp(t *testing.T) {
	apps_record_setup(t)

	if apps_installed("app-fresh") != 0 {
		t.Fatal("app already recorded before the test")
	}
	apps_record("app-fresh")
	if apps_installed("app-fresh") == 0 {
		t.Error("a first install was not stamped")
	}
}

// TestAppsRecordCommentMatchesBehaviour. The two comments contradicted each
// other - load_version said "only recorded once" while apps_record said
// "always writes" - and the code followed the wrong one. Pin that the SQL is
// the write-once form, since the whole defect was a claim the code did not keep.
func TestAppsRecordCommentMatchesBehaviour(t *testing.T) {
	source, err := os.ReadFile("apps.go")
	if err != nil {
		t.Fatalf("read apps.go: %v", err)
	}
	body := string(source)
	body = body[strings.Index(body, "func apps_record("):]
	body = body[:strings.Index(body, "\n}\n")]

	if strings.Contains(body, "replace into apps") {
		t.Error("apps_record still uses REPLACE INTO, which re-stamps on every load")
	}
	if !strings.Contains(body, "on conflict(app) do nothing") {
		t.Error("apps_record no longer writes once")
	}
}

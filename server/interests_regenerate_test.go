// Mochi server: one stale summary rebuild at a time per account.
//
// The stale row is only rewritten once a rebuild finishes, so every request
// arriving in the meantime read the same stale row, decided it was stale, and
// started a rebuild of its own. Feeds and forums call mochi.interests.summary()
// from their ranking paths, so the arrivals are page loads and each rebuild is
// one Wikidata resolution plus one paid provider call.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"testing"
	"time"
)

// interests_settled waits for the account's rebuild marker to clear, and reports
// whether it did. A rebuild that never clears its marker is the failure mode
// that would leave the summary frozen for the life of the process.
func interests_settled(user *User) bool {
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		if _, running := interests_regenerating.Load(user.UID); !running {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// TestRegenerateRefusesWhileOneIsRunning. Without this every page load during a
// rebuild starts a rebuild of its own.
func TestRegenerateRefusesWhileOneIsRunning(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "regenbusy")
	database := db_user(user, "user")

	// Marked directly rather than by starting a real rebuild, so the assertion
	// does not race the goroutine finishing.
	interests_regenerating.Store(user.UID, true)
	defer interests_regenerating.Delete(user.UID)

	if interests_regenerate(user, database) {
		t.Error("a second arrival started its own rebuild while one was already running")
	}
}

// TestRegenerateReleasesItsMarker. The marker has to be cleared by the rebuild
// itself: a marker left behind would refuse every later rebuild for the life of
// the process, freezing the account's summary.
func TestRegenerateReleasesItsMarker(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "regenreleases")
	database := db_user(user, "user")

	if !interests_regenerate(user, database) {
		t.Fatal("the first rebuild did not start")
	}
	if !interests_settled(user) {
		t.Fatal("the rebuild never released its marker; the account can never regenerate again")
	}

	// It wrote the row it was started for, so the next request reads a fresh
	// summary rather than deciding the same stale row is stale again.
	row, _ := database.row("select number from settings where key='interest_summary'")
	if row == nil {
		t.Fatal("the rebuild finished without writing the summary row")
	}
	if number, _ := row["number"].(int64); number == 0 {
		t.Error("the summary row carries no timestamp, so it reads as stale forever")
	}

	// And a later arrival is admitted, now that the first has finished.
	if !interests_regenerate(user, database) {
		t.Error("a rebuild after the first finished was refused")
	}
	interests_settled(user)
}

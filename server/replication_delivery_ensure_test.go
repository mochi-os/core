// Mochi server: journal cursor tables exist eagerly, so the delivery-ack path
// never panics on a missing table (#28/#424 P4 prod crash).
//
// journal_sequence/journal_delivery (replication.db) and journal_inflight
// (queue.db) are created by db_create()/db_upgrade_90 at boot, not lazily on
// first journal op. A freshly reset+rejoined host recreates them on restart
// (replica reset requires the server stopped), so the delivery-ack path can
// insert into journal_delivery without first ensuring it exists. Previously the
// tables were lazy-created per code path, and the ack path hitting a not-yet-
// created journal_delivery panicked the whole server ("no such table:
// journal_delivery") — observed live on wasabi during the yuzu re-pair.
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import "testing"

func TestJournalInflightAckedAdvancesDeliveryCursor(t *testing.T) {
	cleanup := setup_replication_test(t) // runs db_upgrade_90: journal tables exist eagerly
	defer cleanup()

	rdb := db_open("db/replication.db")
	if has, _ := rdb.exists("select 1 from sqlite_master where type='table' and name='journal_delivery'"); !has {
		t.Fatal("journal_delivery not created by db_upgrade_90")
	}

	// Record an inflight op, then ack it. The ack inserts into journal_delivery
	// with no lazy ensure on the path; the table is already present from the
	// migration, so this must not panic.
	journal_inflight_record("op1", "u", "peer1", "core:user", 5)
	journal_inflight_acked([]string{"op1"})

	if seq := journal_delivery_cursor(rdb, "u", "peer1", "core:user"); seq != 5 {
		t.Errorf("journal_delivery cursor = %d, want 5 (ack did not land)", seq)
	}
}

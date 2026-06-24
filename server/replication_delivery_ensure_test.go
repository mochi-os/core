// Mochi server: delivery-ack must ensure journal_delivery exists (P4 prod crash).
//
// A freshly reset+rejoined replication.db has no journal_delivery table until a
// lazy ensure runs. The journaling path ensures it; the delivery-ack path must
// too, or the first ack panics the whole server ("no such table:
// journal_delivery") — observed live on wasabi during the yuzu re-pair.
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import "testing"

func TestJournalInflightAckedEnsuresDeliveryTable(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	// Simulate the post-reset state: replication.db without journal_delivery.
	rdb := db_open("db/replication.db")
	rdb.exec("drop table if exists journal_delivery")

	// Record an inflight op, then ack it. The ack inserts into journal_delivery;
	// without the ensure on the ack path this panics on the missing table.
	journal_inflight_record("op1", "u", "peer1", "core:user", 5)
	journal_inflight_acked([]string{"op1"}) // must NOT panic

	if seq := journal_delivery_cursor(rdb, "u", "peer1", "core:user"); seq != 5 {
		t.Errorf("journal_delivery cursor = %d, want 5 (ack did not land)", seq)
	}
}

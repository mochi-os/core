// Mochi server: replication health-signal tests (recovery rate + stalled trend).
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import "testing"

// TestReplicationHealthRecoveryRate: the recovery-rate counter counts
// bootstraps started in the trailing window and prunes older entries.
func TestReplicationHealthRecoveryRate(t *testing.T) {
	health_recovery_starts = nil

	replication_health_record_bootstrap()
	replication_health_record_bootstrap()
	replication_health_record_bootstrap()
	if r := replication_health_recovery_rate(); r != 3 {
		t.Errorf("recovery rate = %d, want 3", r)
	}

	// Age one entry past the window — it must drop out of the count and the slice.
	health_recovery_starts[0] = now() - health_recovery_window - 1
	if r := replication_health_recovery_rate(); r != 2 {
		t.Errorf("after ageing one entry: rate = %d, want 2", r)
	}
	if len(health_recovery_starts) != 2 {
		t.Errorf("aged entry must be pruned; slice len = %d, want 2", len(health_recovery_starts))
	}
}

// TestReplicationHealthScanTracksBaseline: a scan with no stalled streams
// records a zero baseline and doesn't alert (below the floor).
func TestReplicationHealthScanTracksBaseline(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	health_previous_stalled = -1
	health_recovery_starts = nil

	replication_health_scan()
	if health_previous_stalled != 0 {
		t.Errorf("baseline after empty scan = %d, want 0", health_previous_stalled)
	}
}

package main

import "testing"

// #185: the replication-notify event id must be deterministic across hosts
// (same inputs → same id) and distinct per (topic, recipient), so a leader-gate
// race produces the identical sends row (de-duped) rather than diverging.
func TestReplicationNotifyEventIdDeterministic(t *testing.T) {
	a := replication_notify_event_id("replica/offline", "user-1")
	b := replication_notify_event_id("replica/offline", "user-1")
	if a != b {
		t.Fatalf("same inputs must yield same id: %q vs %q", a, b)
	}
	if len(a) != 32 {
		t.Errorf("expected 32 hex chars, got %d (%q)", len(a), a)
	}
	// Distinct topic or recipient → distinct id.
	if replication_notify_event_id("replica/irreparable", "user-1") == a {
		t.Error("different topic must yield a different id")
	}
	if replication_notify_event_id("replica/offline", "user-2") == a {
		t.Error("different recipient must yield a different id")
	}
	// No per-host component: the value is a pure function of its args, so a
	// second process (other pair member) computes the same thing.
	if replication_notify_event_id("replica/offline", "user-1") != a {
		t.Error("must be a pure function of inputs (no hidden per-host state)")
	}
}

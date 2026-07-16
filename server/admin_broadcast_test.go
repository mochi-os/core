// Mochi server: /_/admin/broadcast/* handler tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

//go:build linux

package main

import (
	"os"
	"path/filepath"
	"testing"
)

// TestBroadcastLagScanDB exercises the per-DB scanner against a
// seeded received + log pair. Verifies:
//   - lag math when both tables exist for the same (key, peer)
//   - lag absent when only received exists (this host is pure
//     subscriber, owner log is remote)
//   - pending buffer count is reported when the table exists
func TestBroadcastLagScanDB(t *testing.T) {
	tmp_dir, err := os.MkdirTemp("", "mochi_bcast_lag")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(tmp_dir)
	orig := data_dir
	data_dir = tmp_dir
	defer func() { data_dir = orig }()

	rel := filepath.Join("users", "u1", "myapp", "db", "myapp.db")
	db := db_open(rel)
	if db == nil {
		t.Fatal("db_open returned nil")
	}

	// Seed received with two streams: one will have matching log
	// (we're the owner), one won't (we're a pure subscriber).
	broadcast_received_table_create(db)
	db.exec("insert into received (sender, key, last) values (?, ?, ?)", "peer-self", "key-owned", 42)
	db.exec("insert into received (sender, key, last) values (?, ?, ?)", "peer-remote", "key-subscribed", 5)

	// Seed log only for the owned stream.
	broadcast_log_table_create(db)
	db.exec("insert into log (key, peer, sequence, event, data, created) values (?, ?, ?, ?, ?, ?)", "key-owned", "peer-self", 100, "e", "{}", now())

	// Seed pending buffer for the subscribed stream (3 events waiting).
	broadcast_pending_table_create(db)
	for seq := int64(7); seq <= 9; seq++ {
		db.exec("insert into pending (peer, key, sequence, source, target, service, event, content, received) values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
			"peer-remote", "key-subscribed", seq, "", "", "", "", []byte{}, now())
	}

	rows := broadcast_lag_scan_db("u1", "myapp", rel)
	if len(rows) != 2 {
		t.Fatalf("scan returned %d rows, want 2", len(rows))
	}

	byKey := map[string]BroadcastLagRow{}
	for _, r := range rows {
		byKey[r.Key] = r
	}

	owned, ok := byKey["key-owned"]
	if !ok {
		t.Fatal("missing key-owned row")
	}
	if owned.ReceivedLast != 42 {
		t.Errorf("key-owned received_last: got %d, want 42", owned.ReceivedLast)
	}
	if owned.OwnerLogMax == nil || *owned.OwnerLogMax != 100 {
		t.Errorf("key-owned owner_log_max: got %v, want 100", owned.OwnerLogMax)
	}
	if owned.Lag == nil || *owned.Lag != 58 {
		t.Errorf("key-owned lag: got %v, want 58 (100 - 42)", owned.Lag)
	}

	sub, ok := byKey["key-subscribed"]
	if !ok {
		t.Fatal("missing key-subscribed row")
	}
	if sub.ReceivedLast != 5 {
		t.Errorf("key-subscribed received_last: got %d, want 5", sub.ReceivedLast)
	}
	if sub.OwnerLogMax != nil {
		t.Errorf("key-subscribed owner_log_max: got %v, want nil (no local log row)", sub.OwnerLogMax)
	}
	if sub.Lag != nil {
		t.Errorf("key-subscribed lag: got %v, want nil", sub.Lag)
	}
	if sub.Pending != 3 {
		t.Errorf("key-subscribed pending: got %d, want 3", sub.Pending)
	}
	if owned.Pending != 0 {
		t.Errorf("key-owned pending: got %d, want 0", owned.Pending)
	}
}

// TestBroadcastLagScanDBNoReceived returns an empty result when the
// app DB has no broadcast traffic - apps without received are the
// common case across the scan.
func TestBroadcastLagScanDBNoReceived(t *testing.T) {
	tmp_dir, err := os.MkdirTemp("", "mochi_bcast_lag2")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(tmp_dir)
	orig := data_dir
	data_dir = tmp_dir
	defer func() { data_dir = orig }()

	rel := filepath.Join("users", "u1", "noisyapp", "db", "noisyapp.db")
	db := db_open(rel)
	db.exec("create table posts (id text primary key, body text)")

	rows := broadcast_lag_scan_db("u1", "noisyapp", rel)
	if len(rows) != 0 {
		t.Errorf("expected empty scan (no received table), got %d rows", len(rows))
	}
}

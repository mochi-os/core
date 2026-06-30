// Mochi server: the generic versioned replicated register. Concurrent writes from
// different hosts must converge to the same row on every host regardless of
// arrival order; a tombstone must not be resurrected by a stale write; and a
// payload-less register must behave as a convergent set.
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import "testing"

// applyReg replays one already-originated register write against db exactly as a
// replica would — explicit version/writer, no local version computation. created is
// fixed (100): the merge must NOT depend on wall-clock time.
func applyReg(t *testing.T, db *DB, d register_def, vals map[string]any, deleted int, writer string, version int64) {
	t.Helper()
	if err := db.exec_e(register_sql(d), register_args(d, vals, deleted, writer, version)...); err != nil {
		t.Fatalf("register apply (%v deleted=%d v=%d) failed: %v", vals, deleted, version, err)
	}
}

// regValue returns the live payload value of column col, or "" if the row is absent
// or tombstoned (deleted=1).
func regValue(db *DB, d register_def, key map[string]any, col string) string {
	where, args := d.predicate(key)
	var row struct{ V string }
	if db.scan(&row, "select "+col+" as v from "+d.table+" where "+where+" and deleted=0", args...) {
		return row.V
	}
	return ""
}

func newReg(t *testing.T, d register_def, schema string) (*DB, *DB, func()) {
	a, ca := create_test_db(t)
	b, cb := create_test_db(t)
	for _, db := range []*DB{a, b} {
		db.exec(schema)
		db.register_columns(d)
	}
	return a, b, func() { ca(); cb() }
}

// Two hosts apply the SAME concurrent writes in OPPOSITE orders → identical final
// row, with the tie at equal version settled deterministically by the higher writer.
func TestRegisterConverges(t *testing.T) {
	d := register_def{"reg_test", []string{"k"}, []string{"v"}}
	a, b, done := newReg(t, d, "create table reg_test ( k text primary key, v text not null default '' )")
	defer done()

	applyReg(t, a, d, map[string]any{"k": "1", "v": "x"}, 0, "A", 1)
	applyReg(t, a, d, map[string]any{"k": "1", "v": "y"}, 0, "B", 1)
	applyReg(t, b, d, map[string]any{"k": "1", "v": "y"}, 0, "B", 1)
	applyReg(t, b, d, map[string]any{"k": "1", "v": "x"}, 0, "A", 1)

	va, vb := regValue(a, d, map[string]any{"k": "1"}, "v"), regValue(b, d, map[string]any{"k": "1"}, "v")
	if va != vb {
		t.Fatalf("diverged across arrival order: A=%q B=%q", va, vb)
	}
	if va != "y" { // writer B > writer A at the same version
		t.Errorf("tie-break violated: want the higher writer's value %q, got %q", "y", va)
	}
}

// A tombstone (deleted=1) hides the row; a stale lower-version write cannot revive
// it, but a higher-version write does.
func TestRegisterTombstoneNoResurrect(t *testing.T) {
	d := register_def{"reg_test", []string{"k"}, []string{"v"}}
	a, _, done := newReg(t, d, "create table reg_test ( k text primary key, v text not null default '' )")
	defer done()

	applyReg(t, a, d, map[string]any{"k": "1", "v": "x"}, 0, "A", 1) // set v1
	applyReg(t, a, d, map[string]any{"k": "1", "v": ""}, 1, "A", 2)  // tombstone v2
	if v := regValue(a, d, map[string]any{"k": "1"}, "v"); v != "" {
		t.Fatalf("after tombstone want absent, got %q", v)
	}
	applyReg(t, a, d, map[string]any{"k": "1", "v": "z"}, 0, "B", 1) // stale grant v1 arrives late
	if v := regValue(a, d, map[string]any{"k": "1"}, "v"); v != "" {
		t.Errorf("stale v1 write resurrected a tombstoned row: got %q, want absent", v)
	}
	applyReg(t, a, d, map[string]any{"k": "1", "v": "z"}, 0, "A", 3) // write that saw the tombstone
	if v := regValue(a, d, map[string]any{"k": "1"}, "v"); v != "z" {
		t.Errorf("higher-version write should revive: got %q, want %q", v, "z")
	}
}

// A payload-less register is a set: present/absent (add/remove) converges, and a
// concurrent add-vs-remove at the same version is settled by writer order.
func TestRegisterSet(t *testing.T) {
	d := register_def{"reg_members", []string{"parent", "member"}, nil}
	a, b, done := newReg(t, d, "create table reg_members ( parent text not null, member text not null, primary key ( parent, member ) )")
	defer done()

	key := map[string]any{"parent": "g", "member": "u"}
	// host A adds (v1, writer A); host B removes (v1, writer B) — concurrent, opposite order.
	applyReg(t, a, d, key, 0, "A", 1)
	applyReg(t, a, d, key, 1, "B", 1)
	applyReg(t, b, d, key, 1, "B", 1)
	applyReg(t, b, d, key, 0, "A", 1)

	pa := regValue(a, d, key, "parent") != "" // present?
	pb := regValue(b, d, key, "parent") != ""
	if pa != pb {
		t.Fatalf("set membership diverged across arrival order: A=%v B=%v", pa, pb)
	}
	if pa { // writer B (remove) > writer A (add) at the same version → absent
		t.Errorf("tie-break: remove by higher writer should win, but member is present")
	}
}

// Mochi server: system-LWW replication for core DBs
// Copyright Alistair Cunningham 2026
//
// Pair-scope replication for operator-tuned tables that live in core
// DBs (not per-user): settings.db.settings, eventually apps.db rows,
// domains.db.routes / delegations. The pattern is the same LWW model
// the apps' mochi.lww uses, but routed via pair-scope (not user-scope)
// and signed at the libp2p transport layer rather than by a user
// entity (system writes don't have one).
//
// Wire: replication/system-lww carrying SystemLWWSet{Database, Table,
// Row, Field, Value, TS, Peer}. Receiver looks up the local row's
// (TS, Peer) for the same (database, table, row) key, compares as
// (TS, Peer) tuple with lexicographic peer tiebreak, and applies if
// incoming wins.
//
// This iteration covers settings.db.settings only. apps.db and
// domains.db rows follow the same shape — each new table gets a
// dispatch branch in replication_system_lww_apply.

package main

import (
	"fmt"
)

// SystemLWWSet is the wire payload for a single per-row LWW write
// against a core system DB. Database/Table identify the destination
// (e.g. "settings"/"settings"); Row is the primary-key value (e.g.
// the setting name); Field is the column being written ("value");
// Value, TS, Peer are the LWW triple.
//
// The table+field model echoes the existing apps-side LWWSet payload
// in replication.go; we keep them as separate types because the
// scope, routing, and apply rules differ — apps-side LWW is per-user
// and signed by the user identity, system-LWW is per-pair-member and
// signed at the libp2p transport (no entity From).
type SystemLWWSet struct {
	Database string `cbor:"db"`
	Table    string `cbor:"table"`
	Row      string `cbor:"row"`
	Field    string `cbor:"field"`
	Value    string `cbor:"value"`
	TS       int64  `cbor:"ts"`
	Peer     string `cbor:"peer"`
}

// replication_system_lww_event is the receive handler. Decodes the
// payload and delegates to the pure-DB apply function.
func replication_system_lww_event(e *Event) {
	var s SystemLWWSet
	if !e.segment(&s) {
		info("Replication system-lww dropping: cannot decode payload")
		return
	}
	replication_system_lww_apply(e.peer, &s)
}

// replication_system_lww_apply applies an incoming system-LWW write
// to the destination DB. The receiver loads the current row's (TS,
// Peer) for the same key and compares as a (TS, Peer) tuple with
// lexicographic peer tiebreak. Idempotent: re-applying the same op is
// a no-op (the local TS already matches and tiebreak doesn't flip).
//
// Dispatch by (Database, Table): each new system table adds a branch.
// Unknown destinations are silently dropped after a warn, not panic,
// so an older receiver doesn't crash on a newer sender's payload.
func replication_system_lww_apply(originPeer string, s *SystemLWWSet) {
	if s.Database == "" || s.Table == "" || s.Row == "" || s.Field == "" {
		info("Replication system-lww dropping: missing key fields")
		return
	}
	switch s.Database + "." + s.Table {
	case "settings.settings":
		replication_system_lww_apply_settings(originPeer, s)
	default:
		warn("Replication system-lww: unsupported destination %q.%q (from peer %q)",
			s.Database, s.Table, originPeer)
	}
}

// replication_system_lww_apply_settings handles settings.db.settings
// writes. Only the `value` field is replicated; other columns (ts,
// peer) are the conflict-resolution metadata that travels in the op
// envelope, not the data itself.
func replication_system_lww_apply_settings(originPeer string, s *SystemLWWSet) {
	if s.Field != "value" {
		info("Replication system-lww settings.settings: unsupported field %q (from peer %q)", s.Field, originPeer)
		return
	}

	db := db_open("db/settings.db")

	var localTS int64
	var localPeer string
	if row, _ := db.row("select ts, peer from settings where name=?", s.Row); row != nil {
		localTS, _ = row["ts"].(int64)
		localPeer, _ = row["peer"].(string)
	}

	// LWW: incoming wins iff (incoming TS, incoming Peer) > (local TS,
	// local Peer) under (ts, peer-id lex) ordering. Equal TS uses
	// peer-id as the deterministic tiebreak.
	if s.TS < localTS {
		return
	}
	if s.TS == localTS && s.Peer <= localPeer {
		return
	}

	db.exec(
		"replace into settings (name, value, ts, peer) values (?, ?, ?, ?)",
		s.Row, s.Value, s.TS, s.Peer)
	debug("Replication system-lww settings.settings applied: name=%q peer=%q ts=%d (from %q)",
		s.Row, s.Peer, s.TS, originPeer)
}

// replication_emit_system_lww is the package-level function variable
// for emitting a system-LWW write to every pair member. Tests can
// stub it out via setup helpers so emit goroutines don't outlive
// cleanup. Production points it at replication_emit_system_lww_real.
var replication_emit_system_lww = replication_emit_system_lww_real

// replication_emit_system_lww_real is the production implementation.
// Called from setting_set (and, in subsequent iterations, from
// apps.db / domains.db write sites). No-op when this server has no
// pair members.
func replication_emit_system_lww_real(database, table, row, field, value string, ts int64) {
	rdb := db_open("db/replication.db")
	rows, err := rdb.rows("select peer from pair")
	if err != nil || len(rows) == 0 {
		return
	}
	payload := &SystemLWWSet{
		Database: database, Table: table, Row: row, Field: field,
		Value: value, TS: ts, Peer: p2p_id,
	}
	for _, r := range rows {
		peer, _ := r["peer"].(string)
		if peer == "" || peer == p2p_id {
			continue
		}
		m := message("", "", "replication", "system-lww")
		m.add(payload)
		m.send_peer(peer)
	}
}

// system_lww_dispatch_string is the destination string for callers
// to compare against (used by tests that assert on the dispatch table
// without importing internals).
func system_lww_dispatch_string(db, table string) string {
	return fmt.Sprintf("%s.%s", db, table)
}

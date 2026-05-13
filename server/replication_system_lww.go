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
	case "apps.classes", "apps.services", "apps.paths":
		replication_system_lww_apply_apps_two_col(originPeer, s)
	case "apps.apps":
		replication_system_lww_apply_apps_installs(originPeer, s)
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

// replication_system_lww_apply_apps_two_col handles classes / services /
// paths in apps.db. All three are (key, app) tables — the key is in
// SystemLWWSet.Row, the new app value is SystemLWWSet.Value. Field is
// "app". An empty value means "delete the row" (LWW-tombstone of the
// binding). The keying column varies per table.
func replication_system_lww_apply_apps_two_col(originPeer string, s *SystemLWWSet) {
	if s.Field != "app" {
		info("Replication system-lww apps.%s: unsupported field %q (from peer %q)", s.Table, s.Field, originPeer)
		return
	}
	var keyCol string
	switch s.Table {
	case "classes":
		keyCol = "class"
	case "services":
		keyCol = "service"
	case "paths":
		keyCol = "path"
	default:
		return
	}

	db := db_apps()

	var localTS int64
	var localPeer string
	if row, _ := db.row(fmt.Sprintf("select ts, peer from %s where %s=?", s.Table, keyCol), s.Row); row != nil {
		localTS, _ = row["ts"].(int64)
		localPeer, _ = row["peer"].(string)
	}

	if s.TS < localTS {
		return
	}
	if s.TS == localTS && s.Peer <= localPeer {
		return
	}

	if s.Value == "" {
		// Delete-via-LWW: tombstone the row by removing it. The ts/peer
		// metadata is lost on delete, so a subsequent stale write could
		// in principle resurrect — acceptable for v1 since the (ts, peer)
		// of the delete travels in the op envelope and stale incoming
		// writes are still rejected by the source-peer's own (ts, peer).
		db.exec(fmt.Sprintf("delete from %s where %s=?", s.Table, keyCol), s.Row)
	} else {
		db.exec(
			fmt.Sprintf("replace into %s (%s, app, ts, peer) values (?, ?, ?, ?)", s.Table, keyCol),
			s.Row, s.Value, s.TS, s.Peer)
	}
	debug("Replication system-lww apps.%s applied: %s=%q value=%q peer=%q ts=%d (from %q)",
		s.Table, keyCol, s.Row, s.Value, s.Peer, s.TS, originPeer)
}

// replication_system_lww_apply_apps_installs handles apps.db.apps —
// the install registry. The Value carries the installed timestamp as
// a decimal string. Empty value means uninstalled (though uninstall
// isn't currently a code path; provided for symmetry).
func replication_system_lww_apply_apps_installs(originPeer string, s *SystemLWWSet) {
	if s.Field != "installed" {
		info("Replication system-lww apps.apps: unsupported field %q (from peer %q)", s.Field, originPeer)
		return
	}

	db := db_apps()
	var localTS int64
	var localPeer string
	if row, _ := db.row("select ts, peer from apps where app=?", s.Row); row != nil {
		localTS, _ = row["ts"].(int64)
		localPeer, _ = row["peer"].(string)
	}

	if s.TS < localTS {
		return
	}
	if s.TS == localTS && s.Peer <= localPeer {
		return
	}

	if s.Value == "" {
		db.exec("delete from apps where app=?", s.Row)
	} else {
		var installed int64
		_, _ = fmt.Sscanf(s.Value, "%d", &installed)
		if installed == 0 {
			installed = s.TS
		}
		db.exec(
			"replace into apps (app, installed, ts, peer) values (?, ?, ?, ?)",
			s.Row, installed, s.TS, s.Peer)
	}
	debug("Replication system-lww apps.apps applied: app=%q value=%q peer=%q ts=%d (from %q)",
		s.Row, s.Value, s.Peer, s.TS, originPeer)
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

// SystemLWWRow is the row-level companion to SystemLWWSet. Used for
// tables where field-level LWW is awkward — multi-column rows, or
// rows with composite primary keys. Key carries the row's primary-key
// columns (1+ columns); Cols carries the remaining columns being
// written. TS / Peer are the LWW metadata, same semantics as
// SystemLWWSet.
//
// Wire: replication/system-lww-row. The dispatcher routes by
// (Database, Table) just like SystemLWWSet; each destination provides
// a per-table apply function that knows which columns are key vs
// data.
type SystemLWWRow struct {
	Database string            `cbor:"db"`
	Table    string            `cbor:"table"`
	Key      map[string]string `cbor:"key"`
	Cols     map[string]string `cbor:"cols"`
	Delete   bool              `cbor:"delete,omitempty"`
	TS       int64             `cbor:"ts"`
	Peer     string            `cbor:"peer"`
}

// replication_system_lww_row_event is the receive handler for
// row-level system-LWW ops.
func replication_system_lww_row_event(e *Event) {
	var s SystemLWWRow
	if !e.segment(&s) {
		info("Replication system-lww-row dropping: cannot decode payload")
		return
	}
	replication_system_lww_row_apply(e.peer, &s)
}

// replication_system_lww_row_apply dispatches an inbound row-level op
// to its table-specific handler. Unknown destinations are silently
// dropped after a warn.
func replication_system_lww_row_apply(originPeer string, s *SystemLWWRow) {
	if s.Database == "" || s.Table == "" || len(s.Key) == 0 {
		info("Replication system-lww-row dropping: missing key fields")
		return
	}
	switch s.Database + "." + s.Table {
	case "domains.domains":
		replication_system_lww_row_apply_domains(originPeer, s)
	default:
		warn("Replication system-lww-row: unsupported destination %q.%q (from peer %q)",
			s.Database, s.Table, originPeer)
	}
}

// replication_system_lww_row_apply_domains handles domains.db.domains
// writes — a single-column key (domain) with multi-column row data.
// Insert / update share one code path; delete is signalled by
// SystemLWWRow.Delete=true.
func replication_system_lww_row_apply_domains(originPeer string, s *SystemLWWRow) {
	name := s.Key["domain"]
	if name == "" {
		return
	}
	db := db_open("db/domains.db")

	var localTS int64
	var localPeer string
	if row, _ := db.row("select ts, peer from domains where domain=?", name); row != nil {
		localTS, _ = row["ts"].(int64)
		localPeer, _ = row["peer"].(string)
	}

	if s.TS < localTS {
		return
	}
	if s.TS == localTS && s.Peer <= localPeer {
		return
	}

	if s.Delete {
		db.exec("delete from domains where domain=?", name)
		debug("Replication system-lww-row domains.domains deleted: %q peer=%q ts=%d (from %q)",
			name, s.Peer, s.TS, originPeer)
		return
	}

	// Upsert. Parse the column values; missing columns get sensible
	// defaults matching the schema.
	var verified, tls, created, updated int64
	_, _ = fmt.Sscanf(s.Cols["verified"], "%d", &verified)
	_, _ = fmt.Sscanf(s.Cols["tls"], "%d", &tls)
	_, _ = fmt.Sscanf(s.Cols["created"], "%d", &created)
	_, _ = fmt.Sscanf(s.Cols["updated"], "%d", &updated)
	token := s.Cols["token"]

	db.exec(
		"replace into domains (domain, verified, token, tls, created, updated, ts, peer) values (?, ?, ?, ?, ?, ?, ?, ?)",
		name, verified, token, tls, created, updated, s.TS, s.Peer)

	debug("Replication system-lww-row domains.domains applied: %q peer=%q ts=%d (from %q)",
		name, s.Peer, s.TS, originPeer)
}

// replication_emit_system_lww_row is the package-level function
// variable for emitting a row-level system-LWW op to every pair
// member. Tests can stub it to no-op.
var replication_emit_system_lww_row = replication_emit_system_lww_row_real

// replication_emit_system_lww_row_real is the production
// implementation. Same shape as replication_emit_system_lww_real but
// with the SystemLWWRow payload.
func replication_emit_system_lww_row_real(database, table string, key, cols map[string]string, del bool, ts int64) {
	rdb := db_open("db/replication.db")
	rows, err := rdb.rows("select peer from pair")
	if err != nil || len(rows) == 0 {
		return
	}
	payload := &SystemLWWRow{
		Database: database, Table: table,
		Key: key, Cols: cols, Delete: del,
		TS: ts, Peer: p2p_id,
	}
	for _, r := range rows {
		peer, _ := r["peer"].(string)
		if peer == "" || peer == p2p_id {
			continue
		}
		m := message("", "", "replication", "system-lww-row")
		m.add(payload)
		m.send_peer(peer)
	}
}

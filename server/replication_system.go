// Mochi server: system-scope replication for core DBs
// Copyright Alistair Cunningham 2026
//
// Pair-scope replication for operator-tuned tables that live in core
// DBs (not per-user): settings.db.settings, apps.db routing tables,
// domains.db domains / routes. Each pair member emits a "this row
// changed" op on local write; receivers replay via REPLACE INTO. No
// LWW conflict resolution — the workload is one-operator-per-pair and
// concurrent same-row edits are rare enough that last-applier-by-
// arrival-order is acceptable. If two operators on different pair
// members concurrently rewrite the same route, the result is
// non-deterministic; the operator notices and rewrites. The trade is
// simplicity over convergence guarantees.
//
// Wire:
//   replication/system-set — field-level write (single-column key,
//     single field changing). Used by settings.db.settings,
//     apps.db.{classes,services,paths,apps}.
//   replication/system-row — row-level write (composite or single key,
//     multiple data columns). Used by domains.db.{domains,routes}.
//
// Server-to-server messages with From="" (libp2p transport auth is
// the origin proof; no entity signature).

package main

import (
	"fmt"
)

// SystemSet is the wire payload for a single field-level write to a
// core system DB. Database/Table identify the destination
// (e.g. "settings"/"settings"); Row is the primary-key value (e.g.
// the setting name); Field is the column being written; Value is the
// new value. Empty Value means "delete the row" (LWW-tombstone of
// the binding).
type SystemSet struct {
	Database string `cbor:"db"`
	Table    string `cbor:"table"`
	Row      string `cbor:"row"`
	Field    string `cbor:"field"`
	Value    string `cbor:"value"`
}

// replication_system_set_event is the receive handler. Decodes the
// payload and delegates to the apply function below.
func replication_system_set_event(e *Event) {
	var s SystemSet
	if !e.segment(&s) {
		info("Replication system-set dropping: cannot decode payload")
		return
	}
	replication_system_set_apply(e.peer, &s)
}

// replication_system_set_apply applies an incoming system-set write
// to the destination DB. Dispatches by (Database, Table); unknown
// destinations are silently dropped after a warn. Order-of-arrival
// determines the winner under concurrent writes — see the file
// header for the trade-off.
func replication_system_set_apply(originPeer string, s *SystemSet) {
	if s.Database == "" || s.Table == "" || s.Row == "" || s.Field == "" {
		info("Replication system-set dropping: missing key fields")
		return
	}
	switch s.Database + "." + s.Table {
	case "settings.settings":
		replication_system_set_apply_settings(originPeer, s)
	case "apps.classes", "apps.services", "apps.paths":
		replication_system_set_apply_apps_two_col(originPeer, s)
	case "apps.apps":
		replication_system_set_apply_apps_installs(originPeer, s)
	default:
		warn("Replication system-set: unsupported destination %q.%q (from peer %q)",
			s.Database, s.Table, originPeer)
	}
}

// replication_system_set_apply_settings handles settings.db.settings.
// Only the `value` field is replicated. Empty value deletes the row.
func replication_system_set_apply_settings(originPeer string, s *SystemSet) {
	if s.Field != "value" {
		info("Replication system-set settings.settings: unsupported field %q (from peer %q)", s.Field, originPeer)
		return
	}
	db := db_open("db/settings.db")
	if s.Value == "" {
		db.exec("delete from settings where name=?", s.Row)
	} else {
		db.exec("replace into settings (name, value) values (?, ?)", s.Row, s.Value)
	}
	debug("Replication system-set settings.settings applied: name=%q value=%q (from %q)",
		s.Row, s.Value, originPeer)
}

// replication_system_set_apply_apps_two_col handles classes / services
// / paths in apps.db. All three are (key, app) tables — the keying
// column varies per table. Empty value deletes the row.
func replication_system_set_apply_apps_two_col(originPeer string, s *SystemSet) {
	if s.Field != "app" {
		info("Replication system-set apps.%s: unsupported field %q (from peer %q)", s.Table, s.Field, originPeer)
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
	if s.Value == "" {
		db.exec(fmt.Sprintf("delete from %s where %s=?", s.Table, keyCol), s.Row)
	} else {
		db.exec(
			fmt.Sprintf("replace into %s (%s, app) values (?, ?)", s.Table, keyCol),
			s.Row, s.Value)
	}
	debug("Replication system-set apps.%s applied: %s=%q value=%q (from %q)",
		s.Table, keyCol, s.Row, s.Value, originPeer)
}

// replication_system_set_apply_apps_installs handles apps.db.apps —
// the install registry. Value carries the install timestamp. A bump
// (re-broadcast of a newer timestamp) means the source just installed
// or upgraded the app; the receiver needs the matching code on disk
// to keep the pair in sync, so app_check_install runs async to pull
// the latest version from the publisher.
func replication_system_set_apply_apps_installs(originPeer string, s *SystemSet) {
	if s.Field != "installed" {
		info("Replication system-set apps.apps: unsupported field %q (from peer %q)", s.Field, originPeer)
		return
	}
	db := db_apps()
	if s.Value == "" {
		db.exec("delete from apps where app=?", s.Row)
	} else {
		var installed int64
		_, _ = fmt.Sscanf(s.Value, "%d", &installed)
		if installed == 0 {
			installed = now()
		}
		db.exec("replace into apps (app, installed) values (?, ?)", s.Row, installed)
	}
	debug("Replication system-set apps.apps applied: app=%q value=%q (from %q)",
		s.Row, s.Value, originPeer)
	if s.Value != "" && valid(s.Row, "entity") {
		go app_check_install(s.Row)
	}
}

// replication_emit_system_set is the package-level emit function
// variable so tests can stub it. Production points it at
// replication_emit_system_set_real.
var replication_emit_system_set = replication_emit_system_set_real

// replication_emit_system_set_real emits a system-set write to every
// pair member. No-op when this server has no pair members.
func replication_emit_system_set_real(database, table, row, field, value string) {
	rdb := db_open("db/replication.db")
	rows, err := rdb.rows("select peer from pair")
	if err != nil || len(rows) == 0 {
		return
	}
	payload := &SystemSet{
		Database: database, Table: table, Row: row, Field: field, Value: value,
	}
	for _, r := range rows {
		peer, _ := r["peer"].(string)
		if peer == "" || peer == p2p_id {
			continue
		}
		m := message("", "", "replication", "system/set")
		m.add(payload)
		m.send_peer(peer)
	}
}

// SystemRow is the row-level companion to SystemSet. Used for tables
// where field-level is awkward — multi-column rows, or rows with
// composite primary keys. Key carries the row's primary-key columns
// (1+ entries); Cols carries the remaining data columns being written.
// Delete=true signals "remove the row".
//
// Wire: replication/system-row. Same order-of-arrival semantics as
// SystemSet — no LWW conflict resolution.
type SystemRow struct {
	Database string            `cbor:"db"`
	Table    string            `cbor:"table"`
	Key      map[string]string `cbor:"key"`
	Cols     map[string]string `cbor:"cols"`
	Delete   bool              `cbor:"delete,omitempty"`
}

// replication_system_row_event is the receive handler for row-level
// ops.
func replication_system_row_event(e *Event) {
	var s SystemRow
	if !e.segment(&s) {
		info("Replication system-row dropping: cannot decode payload")
		return
	}
	replication_system_row_apply(e.peer, &s)
}

// replication_system_row_apply dispatches an inbound row-level op to
// its table-specific handler.
func replication_system_row_apply(originPeer string, s *SystemRow) {
	if s.Database == "" || s.Table == "" || len(s.Key) == 0 {
		info("Replication system-row dropping: missing key fields")
		return
	}
	switch s.Database + "." + s.Table {
	case "domains.domains":
		replication_system_row_apply_domains(originPeer, s)
	case "domains.routes":
		replication_system_row_apply_routes(originPeer, s)
	case "apps.versions":
		replication_system_row_apply_apps_versions(originPeer, s)
	case "apps.tracks":
		replication_system_row_apply_apps_tracks(originPeer, s)
	case "domains.delegations":
		replication_system_row_apply_delegations(originPeer, s)
	default:
		warn("Replication system-row: unsupported destination %q.%q (from peer %q)",
			s.Database, s.Table, originPeer)
	}
}

// replication_system_row_apply_domains handles domains.db.domains.
// Single-column key (domain) with multi-column row data.
func replication_system_row_apply_domains(originPeer string, s *SystemRow) {
	name := s.Key["domain"]
	if name == "" {
		return
	}
	db := db_open("db/domains.db")
	if s.Delete {
		db.exec("delete from domains where domain=?", name)
		debug("Replication system-row domains.domains deleted: %q (from %q)", name, originPeer)
		return
	}
	var verified, tls, created, updated int64
	_, _ = fmt.Sscanf(s.Cols["verified"], "%d", &verified)
	_, _ = fmt.Sscanf(s.Cols["tls"], "%d", &tls)
	_, _ = fmt.Sscanf(s.Cols["created"], "%d", &created)
	_, _ = fmt.Sscanf(s.Cols["updated"], "%d", &updated)
	token := s.Cols["token"]
	db.exec(
		"replace into domains (domain, verified, token, tls, created, updated) values (?, ?, ?, ?, ?, ?)",
		name, verified, token, tls, created, updated)
	debug("Replication system-row domains.domains applied: %q (from %q)", name, originPeer)
}

// replication_system_row_apply_routes handles domains.db.routes —
// composite key (domain, path) carried via Key map.
func replication_system_row_apply_routes(originPeer string, s *SystemRow) {
	domain := s.Key["domain"]
	path := s.Key["path"]
	if domain == "" {
		info("Replication system-row domains.routes dropping: empty domain key (from peer %q)", originPeer)
		return
	}
	db := db_open("db/domains.db")
	if s.Delete {
		db.exec("delete from routes where domain=? and path=?", domain, path)
		debug("Replication system-row domains.routes deleted: %q+%q (from %q)", domain, path, originPeer)
		return
	}
	method := s.Cols["method"]
	target := s.Cols["target"]
	context := s.Cols["context"]
	owner := s.Cols["owner"]
	var priority, enabled, created, updated int64
	_, _ = fmt.Sscanf(s.Cols["priority"], "%d", &priority)
	_, _ = fmt.Sscanf(s.Cols["enabled"], "%d", &enabled)
	_, _ = fmt.Sscanf(s.Cols["created"], "%d", &created)
	_, _ = fmt.Sscanf(s.Cols["updated"], "%d", &updated)
	db.exec(
		"replace into routes (domain, path, method, target, context, owner, priority, enabled, created, updated) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		domain, path, method, target, context, owner, priority, enabled, created, updated)
	debug("Replication system-row domains.routes applied: %q+%q (from %q)", domain, path, originPeer)
}

// replication_emit_system_row is the package-level emit function
// variable for the row-level ops; tests can stub it.
var replication_emit_system_row = replication_emit_system_row_real

// replication_emit_system_row_real emits a row-level op to every pair
// member.
func replication_emit_system_row_real(database, table string, key, cols map[string]string, del bool) {
	rdb := db_open("db/replication.db")
	rows, err := rdb.rows("select peer from pair")
	if err != nil || len(rows) == 0 {
		return
	}
	payload := &SystemRow{
		Database: database, Table: table,
		Key: key, Cols: cols, Delete: del,
	}
	for _, r := range rows {
		peer, _ := r["peer"].(string)
		if peer == "" || peer == p2p_id {
			continue
		}
		m := message("", "", "replication", "system/row")
		m.add(payload)
		m.send_peer(peer)
	}
}

// replication_system_row_apply_apps_versions handles apps.db.versions —
// (app primary key, version, track). Single-column key, two data
// columns. Empty row → delete.
func replication_system_row_apply_apps_versions(originPeer string, s *SystemRow) {
	app := s.Key["app"]
	if app == "" {
		return
	}
	db := db_apps()
	if s.Delete {
		db.exec("delete from versions where app=?", app)
		return
	}
	db.exec("replace into versions (app, version, track) values (?, ?, ?)",
		app, s.Cols["version"], s.Cols["track"])
	debug("Replication system-row apps.versions applied: %q (from %q)", app, originPeer)
	// A version row update for an entity-id app means the source
	// installed or upgraded a published app. The replica needs the
	// matching code on disk to actually serve requests against it;
	// fire app_check_install so the publisher download happens now
	// instead of waiting for the next 24-hour apps_manager tick.
	// Skip non-entity ids (dev / internal apps live on the local
	// filesystem and don't need downloading).
	if valid(app, "entity") {
		go app_check_install(app)
	}
}

// replication_system_row_apply_apps_tracks handles apps.db.tracks —
// composite key (app, track), single data column (version). Operator
// pinning a track to a new version means the source has (or wants)
// that version locally; the receiver needs it on disk to follow the
// pin, so app_check_install runs async — same pattern as the
// versions apply handler above.
func replication_system_row_apply_apps_tracks(originPeer string, s *SystemRow) {
	app := s.Key["app"]
	track := s.Key["track"]
	if app == "" || track == "" {
		return
	}
	db := db_apps()
	if s.Delete {
		db.exec("delete from tracks where app=? and track=?", app, track)
		return
	}
	db.exec("replace into tracks (app, track, version) values (?, ?, ?)",
		app, track, s.Cols["version"])
	debug("Replication system-row apps.tracks applied: %q+%q (from %q)", app, track, originPeer)
	if valid(app, "entity") {
		go app_check_install(app)
	}
}

// replication_system_row_apply_delegations handles domains.db.delegations —
// composite key (domain, path, owner), two data columns (created, updated).
// The id integer PK is local-only; receivers let SQLite assign on insert.
func replication_system_row_apply_delegations(originPeer string, s *SystemRow) {
	domain := s.Key["domain"]
	path := s.Key["path"]
	owner := s.Key["owner"]
	if domain == "" || owner == "" {
		return
	}
	db := db_open("db/domains.db")
	if s.Delete {
		db.exec("delete from delegations where domain=? and path=? and owner=?", domain, path, owner)
		return
	}
	// Insert if not present; the unique(domain, path, owner) index
	// keeps replays idempotent. An incoming op for an already-present
	// row updates the timestamps via ON CONFLICT DO UPDATE pattern —
	// but SQLite doesn't allow direct ON CONFLICT on a non-pk unique
	// index here. Use a DELETE-then-INSERT instead.
	var created, updated int64
	_, _ = fmt.Sscanf(s.Cols["created"], "%d", &created)
	_, _ = fmt.Sscanf(s.Cols["updated"], "%d", &updated)
	db.exec("delete from delegations where domain=? and path=? and owner=?", domain, path, owner)
	db.exec("insert into delegations (domain, path, owner, created, updated) values (?, ?, ?, ?, ?)",
		domain, path, owner, created, updated)
	debug("Replication system-row domains.delegations applied: %q+%q+%q (from %q)",
		domain, path, owner, originPeer)
}

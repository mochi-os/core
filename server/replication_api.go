// Mochi server: mochi.replication.* Starlark API
// Copyright Alistair Cunningham 2026
//
// In-Mochi consumers (the Pair page in apps/settings/system, the
// per-user "My hosts" page in apps/settings/user, mochictl's progress
// display) query replication state via this API instead of scraping
// /_/health. /_/health is reserved for LB consumption and exposes a
// coarser view; the Starlark API is for app-level rendering.
//
// Surface:
//   mochi.replication.status()            -> dict (whole-server view)
//   mochi.replication.links()             -> list (pending inbound link-requests for the calling user)
//   mochi.replication.hosts()             -> list (active per-user peers for the calling user)
//   mochi.replication.link_approve(peer)  -> str  ("approved" | "already-approved")
//   mochi.replication.link_deny(peer)     -> str  ("denied"   | "already-handled")
//   mochi.replication.host_remove(peer)   -> str  ("removed"  | "not-found")
//   mochi.replication.joins()             -> list (pending whole-server join-requests)
//   mochi.replication.join_approve(peer)  -> str  ("approved" | "already-handled")
//   mochi.replication.join_deny(peer)     -> str  ("denied"   | "already-handled")
//   mochi.replication.pair_remove(peer)   -> str  ("removed"  | "not-found")
//   mochi.replication.bootstrap_progress() -> list (per-(peer, scope) progress)
//
// All per-user functions operate on the calling user (resolved from
// t.Local("user")). The settings app cannot read or mutate another
// user's replication state through this API.
//
// The whole-server join/pair functions (joins / join_approve /
// join_deny / pair_remove) are role-agnostic at the API level; the
// settings app's `system/replication.star` action wrapper enforces
// require_admin before calling them — same pattern as
// mochi.server.update.install. Apps without an admin context must not
// call these directly.
//
// Future additions (when #66 / #70 land): bootstrap progress, lag
// thresholds, manual-resync trigger, audit log.

package main

import (
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

// api_replication exposes mochi.replication.{status, links, hosts,
// link_approve, link_deny, host_remove, joins, join_approve, join_deny,
// pair_remove}.
var api_replication = sls.FromStringDict(sl.String("mochi.replication"), sl.StringDict{
	"status":             sl.NewBuiltin("mochi.replication.status", api_replication_status),
	"links":              sl.NewBuiltin("mochi.replication.links", api_replication_links),
	"hosts":              sl.NewBuiltin("mochi.replication.hosts", api_replication_hosts),
	"link_approve":       sl.NewBuiltin("mochi.replication.link_approve", api_replication_link_approve),
	"link_deny":          sl.NewBuiltin("mochi.replication.link_deny", api_replication_link_deny),
	"host_remove":        sl.NewBuiltin("mochi.replication.host_remove", api_replication_host_remove),
	"joins":              sl.NewBuiltin("mochi.replication.joins", api_replication_joins),
	"join_approve":       sl.NewBuiltin("mochi.replication.join_approve", api_replication_join_approve),
	"join_deny":          sl.NewBuiltin("mochi.replication.join_deny", api_replication_join_deny),
	"pair_remove":        sl.NewBuiltin("mochi.replication.pair_remove", api_replication_pair_remove),
	"bootstrap_progress": sl.NewBuiltin("mochi.replication.bootstrap_progress", api_replication_bootstrap_progress),
})

// api_replication_status returns a dict describing this server's
// replication state visible from the local DBs. Same data the
// /_/admin/replication/status endpoint returns to mochictl, exposed
// to Starlark callers so apps can render it directly.
//
// Returned shape:
//
//	{
//	  "peer":              "<this-peer-id>",
//	  "pair":              ["<peer-1>", "<peer-2>"],
//	  "hosts_count":       N,         // total per-user opt-in rows
//	  "links_pending":     N,         // pending per-user link-requests
//	  "joins_pending":     N,         // pending whole-server join-requests
//	  "bootstrap_pending": N,         // (scope, peer) rows still queued/active
//	}
//
// Read-only; no parameters; never returns an error.
func api_replication_status(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	rdb := db_open("db/replication.db")

	var pair []string
	if rows, err := rdb.rows("select peer from pair"); err == nil {
		for _, r := range rows {
			if p, ok := r["peer"].(string); ok && p != "" {
				pair = append(pair, p)
			}
		}
	}

	hosts_count := int64(0)
	if row, _ := rdb.row("select count(*) as c from hosts"); row != nil {
		if v, ok := row["c"].(int64); ok {
			hosts_count = v
		}
	}

	links_pending := int64(0)
	if row, _ := rdb.row("select count(*) as c from links"); row != nil {
		if v, ok := row["c"].(int64); ok {
			links_pending = v
		}
	}

	joins_pending := int64(0)
	if row, _ := rdb.row("select count(*) as c from joins"); row != nil {
		if v, ok := row["c"].(int64); ok {
			joins_pending = v
		}
	}

	// Bootstrap progress: count any (scope, peer) rows still in
	// queued/active. Zero means every active scope has reached
	// 'done' (or there are no peers in bootstrap at all).
	bootstrap_pending := int64(0)
	if row, _ := rdb.row("select count(*) as c from bootstrap where state != 'done'"); row != nil {
		if v, ok := row["c"].(int64); ok {
			bootstrap_pending = v
		}
	}

	pairValues := make([]sl.Value, 0, len(pair))
	for _, p := range pair {
		pairValues = append(pairValues, sl.String(p))
	}

	result := sl.NewDict(6)
	_ = result.SetKey(sl.String("peer"), sl.String(p2p_id))
	_ = result.SetKey(sl.String("pair"), sl.NewList(pairValues))
	_ = result.SetKey(sl.String("hosts_count"), sl.MakeInt64(hosts_count))
	_ = result.SetKey(sl.String("links_pending"), sl.MakeInt64(links_pending))
	_ = result.SetKey(sl.String("joins_pending"), sl.MakeInt64(joins_pending))
	_ = result.SetKey(sl.String("bootstrap_pending"), sl.MakeInt64(bootstrap_pending))
	return result, nil
}

// api_replication_links returns pending inbound link-requests for the
// calling user. Source-side display: "alice on B wants to replicate
// from A — Approve / Deny".
//
// Returned shape: list of dicts {peer, label, expires}.
func api_replication_links(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	u, _ := t.Local("user").(*User)
	if u == nil {
		return sl_error(fn, "no user")
	}

	rdb := db_open("db/replication.db")
	rows, err := rdb.rows(
		"select peer, label, expires from links where user=? order by received",
		u.UID)
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}

	out := sl.NewList(nil)
	for _, r := range rows {
		entry := sl.NewDict(3)
		_ = entry.SetKey(sl.String("peer"), sl.String(row_string(r, "peer")))
		_ = entry.SetKey(sl.String("label"), sl.String(row_string(r, "label")))
		_ = entry.SetKey(sl.String("expires"), sl.MakeInt64(row_int(r, "expires")))
		_ = out.Append(entry)
	}
	return out, nil
}

// api_replication_hosts returns the active per-user host set for the
// calling user — the peers that hold a copy of this user's data via
// the per-user opt-in flow.
//
// Returned shape: list of dicts {peer, added, ack}.
func api_replication_hosts(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	u, _ := t.Local("user").(*User)
	if u == nil {
		return sl_error(fn, "no user")
	}

	rdb := db_open("db/replication.db")
	rows, err := rdb.rows(
		"select peer, added, ack from hosts where user=? order by added",
		u.UID)
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}

	out := sl.NewList(nil)
	for _, r := range rows {
		entry := sl.NewDict(3)
		_ = entry.SetKey(sl.String("peer"), sl.String(row_string(r, "peer")))
		_ = entry.SetKey(sl.String("added"), sl.MakeInt64(row_int(r, "added")))
		_ = entry.SetKey(sl.String("ack"), sl.MakeInt64(row_int(r, "ack")))
		_ = out.Append(entry)
	}
	return out, nil
}

// api_replication_link_approve approves an inbound link-request from
// `peer` targeting the calling user. Wraps replication_link_approve;
// the underlying handler runs the freshness probe, emits the
// keys-transfer, and updates membership.
//
// Returns "approved" on success, "already-approved" on the multi-tab
// loser. Errors surface as Starlark errors.
func api_replication_link_approve(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	u, _ := t.Local("user").(*User)
	if u == nil {
		return sl_error(fn, "no user")
	}
	if len(args) < 1 {
		return sl_error(fn, "peer required")
	}
	peer, ok := sl.AsString(args[0])
	if !ok || peer == "" {
		return sl_error(fn, "invalid peer")
	}

	result, err := replication_link_approve(u.UID, peer)
	if err != nil {
		return sl_error(fn, "approve: %v", err)
	}
	return sl.String(result), nil
}

// api_replication_link_deny denies an inbound link-request from `peer`
// targeting the calling user. Wraps replication_link_deny; the
// underlying handler emits link-denied(reason=denied) to the
// destination.
//
// Returns "denied" on success, "already-handled" on the multi-tab
// loser. Never returns an error (the underlying call swallows DB
// failures with a warning).
func api_replication_link_deny(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	u, _ := t.Local("user").(*User)
	if u == nil {
		return sl_error(fn, "no user")
	}
	if len(args) < 1 {
		return sl_error(fn, "peer required")
	}
	peer, ok := sl.AsString(args[0])
	if !ok || peer == "" {
		return sl_error(fn, "invalid peer")
	}

	return sl.String(replication_link_deny(u.UID, peer)), nil
}

// api_replication_host_remove removes `peer` from the calling user's
// active per-user host set and emits a membership-change to the
// remaining peers (and the removed one) so the cluster converges on
// the smaller set. Returns "removed" or "not-found".
func api_replication_host_remove(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	u, _ := t.Local("user").(*User)
	if u == nil {
		return sl_error(fn, "no user")
	}
	if len(args) < 1 {
		return sl_error(fn, "peer required")
	}
	peer, ok := sl.AsString(args[0])
	if !ok || peer == "" {
		return sl_error(fn, "invalid peer")
	}

	rdb := db_open("db/replication.db")
	exists, _ := rdb.exists(
		"select 1 from hosts where user=? and peer=?", u.UID, peer)
	if !exists {
		return sl.String("not-found"), nil
	}

	rows, err := rdb.rows("select peer from hosts where user=? and peer!=?", u.UID, peer)
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}
	remaining := make([]string, 0, len(rows))
	for _, r := range rows {
		if p := row_string(r, "peer"); p != "" {
			remaining = append(remaining, p)
		}
	}

	// membership-update wipes & rewrites the local hosts table and
	// broadcasts the new set to every remaining host. The departing
	// peer learns it's out of the set when it next receives any op
	// for this user and sees itself missing from the membership list
	// (or when the periodic reconciler in #66's bootstrap protocol
	// confirms divergence).
	replication_membership_update(u.UID, remaining)
	audit_replication_host_removed(u.UID, peer)

	return sl.String("removed"), nil
}

// api_replication_joins returns pending inbound whole-server
// join-requests. Server-wide; the action wrapper must require_admin
// before calling. Returned shape: list of dicts {peer, label, expires}.
func api_replication_joins(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	rdb := db_open("db/replication.db")
	rows, err := rdb.rows("select peer, label, expires from joins order by received")
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}

	out := sl.NewList(nil)
	for _, r := range rows {
		entry := sl.NewDict(3)
		_ = entry.SetKey(sl.String("peer"), sl.String(row_string(r, "peer")))
		_ = entry.SetKey(sl.String("label"), sl.String(row_string(r, "label")))
		_ = entry.SetKey(sl.String("expires"), sl.MakeInt64(row_int(r, "expires")))
		_ = out.Append(entry)
	}
	return out, nil
}

// api_replication_join_approve approves an inbound pair join-request
// from `peer`. Wraps replication_join_approve: replaces the local pair
// table with the new member set and emits join-approved + a
// pair-membership-change to existing members. Returns "approved" or
// "already-handled".
//
// Server-wide; the action wrapper must require_admin before calling.
func api_replication_join_approve(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 {
		return sl_error(fn, "peer required")
	}
	peer, ok := sl.AsString(args[0])
	if !ok || peer == "" {
		return sl_error(fn, "invalid peer")
	}

	result, err := replication_join_approve(peer)
	if err != nil {
		return sl_error(fn, "approve: %v", err)
	}
	return sl.String(result), nil
}

// api_replication_join_deny denies an inbound pair join-request from
// `peer`. Wraps replication_join_deny: emits join-denied(reason=denied)
// to the replica on the winner, no-op on the multi-tab loser. Returns
// "denied" or "already-handled".
//
// Server-wide; the action wrapper must require_admin before calling.
func api_replication_join_deny(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 {
		return sl_error(fn, "peer required")
	}
	peer, ok := sl.AsString(args[0])
	if !ok || peer == "" {
		return sl_error(fn, "invalid peer")
	}
	return sl.String(replication_join_deny(peer)), nil
}

// api_replication_pair_remove drops `peer` from the local pair set and
// announces the new member set to every remaining pair member. Wraps
// replication_pair_remove (shared with the admin HTTP handler).
// Returns "removed" or "not-found".
//
// Server-wide; the action wrapper must require_admin before calling.
// The removed peer is intentionally not announced to — it learns of
// the change via gossip from a remaining member, matching the
// admin HTTP endpoint behavior.
func api_replication_pair_remove(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 {
		return sl_error(fn, "peer required")
	}
	peer, ok := sl.AsString(args[0])
	if !ok || peer == "" {
		return sl_error(fn, "invalid peer")
	}
	_, _, removed := replication_pair_remove(peer)
	if !removed {
		return sl.String("not-found"), nil
	}
	return sl.String("removed"), nil
}

// api_replication_bootstrap_progress returns the per-(peer, scope)
// bulk-bootstrap progress as a list of dicts. Each entry includes
// the peer, scope, state ('queued' | 'active' | 'done'), and a
// position cursor whose meaning depends on the state: for 'active'
// it's the count of items remaining; for 'done' it's empty.
//
// Optional argument: a single peer-id string filters to that peer's
// rows; omitted returns every peer's rows. Whole-server scope; no
// per-user filtering. Action wrappers gate to admin.
//
// Returned shape: list of dicts {peer, scope, state, position}.
func api_replication_bootstrap_progress(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var peerFilter string
	if len(args) > 0 && args[0] != sl.None {
		if p, ok := sl.AsString(args[0]); ok {
			peerFilter = p
		}
	}

	rdb := db_open("db/replication.db")
	var rows []map[string]any
	var err error
	if peerFilter != "" {
		rows, err = rdb.rows("select peer, scope, state, position from bootstrap where peer=? order by peer, scope", peerFilter)
	} else {
		rows, err = rdb.rows("select peer, scope, state, position from bootstrap order by peer, scope")
	}
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}

	out := sl.NewList(nil)
	for _, r := range rows {
		entry := sl.NewDict(4)
		_ = entry.SetKey(sl.String("peer"), sl.String(row_string(r, "peer")))
		_ = entry.SetKey(sl.String("scope"), sl.String(row_string(r, "scope")))
		_ = entry.SetKey(sl.String("state"), sl.String(row_string(r, "state")))
		_ = entry.SetKey(sl.String("position"), sl.String(row_string(r, "position")))
		_ = out.Append(entry)
	}
	return out, nil
}

// row_string / row_int unpack scalar SQL row values defensively. The
// nil checks let api_replication_* return an empty list cleanly when
// a row was scanned with an unexpected column type instead of
// panicking the action.
func row_string(r map[string]any, key string) string {
	if v, ok := r[key].(string); ok {
		return v
	}
	return ""
}

// row_int extracts a numeric field from a map[string]any returned by
// a CBOR-decoded payload or a sqlite row scan. The cbor library
// decodes non-negative integers into uint64 (not int64) when the
// target is interface{}, so callers like the bootstrap chunk-fetch
// handler would see length=0 for every non-empty file because the
// uint64(903) case wasn't matched and fell through to the zero
// return. That broke file-chunk delivery — every file landed as a
// zero-byte file on the receiver, including 21,612 entity-id app
// files whose empty app.json then made the published-apps loader
// silently skip the entire installed-app set.
func row_int(r map[string]any, key string) int64 {
	switch v := r[key].(type) {
	case int64:
		return v
	case uint64:
		return int64(v)
	case int:
		return int64(v)
	case uint:
		return int64(v)
	case int32:
		return int64(v)
	case uint32:
		return int64(v)
	}
	return 0
}

// Mochi server: mochi.replication.* Starlark API
// Copyright Alistair Cunningham 2026
//
// In-Mochi consumers (the Pair page in apps/settings/system, the
// per-user "My hosts" page in apps/settings/user, mochictl's progress
// display) query replication state via this API instead of scraping
// /_/health. /_/health is reserved for LB consumption and exposes a
// coarser view; the Starlark API is for app-level rendering.
//
// Initial surface (v1):
//   mochi.replication.status() -> dict
//
// Future additions (when #66 / #70 land): per-user host detail,
// bootstrap progress, lag thresholds, manual-resync trigger.

package main

import (
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

// api_replication exposes mochi.replication.{status}.
var api_replication = sls.FromStringDict(sl.String("mochi.replication"), sl.StringDict{
	"status": sl.NewBuiltin("mochi.replication.status", api_replication_status),
})

// api_replication_status returns a dict describing this server's
// replication state visible from the local DBs. Same data the
// /_/admin/replication/status endpoint returns to mochictl, exposed
// to Starlark callers so apps can render it directly.
//
// Returned shape:
//
//	{
//	  "peer":           "<this-peer-id>",
//	  "pair":           ["<peer-1>", "<peer-2>"],
//	  "hosts_count":    N,            // total per-user opt-in rows
//	  "links_pending":  N,            // pending per-user link-requests
//	  "joins_pending":  N,            // pending whole-server join-requests
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

	pairValues := make([]sl.Value, 0, len(pair))
	for _, p := range pair {
		pairValues = append(pairValues, sl.String(p))
	}

	result := sl.NewDict(5)
	_ = result.SetKey(sl.String("peer"), sl.String(p2p_id))
	_ = result.SetKey(sl.String("pair"), sl.NewList(pairValues))
	_ = result.SetKey(sl.String("hosts_count"), sl.MakeInt64(hosts_count))
	_ = result.SetKey(sl.String("links_pending"), sl.MakeInt64(links_pending))
	_ = result.SetKey(sl.String("joins_pending"), sl.MakeInt64(joins_pending))
	return result, nil
}

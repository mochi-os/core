// Mochi server: per-peer journal delivery cursor (#28)
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.
//
// The journal's reconnect backfill (#23) re-ships a peer's ENTIRE retained
// window and lets the receiver dedup via `seen`. Correct but wasteful. This
// adds a per-(user, peer, stream) DELIVERY CURSOR — the highest sequence a peer
// has actually received — so backfill ships only the delta above it.
//
// The only sound "the peer got it" signal is the transport ACK, but the ack
// path carries just a message id and is bulk-batched. A small inflight table in
// queue.db (co-located with the ack delete, so no cross-DB cost) bridges
// send→ack: journal_ship records id -> (user, peer, stream, sequence) on send,
// and the ack path resolves matched ids and advances the cursor in
// journal_delivery (replication.db). The common ack is not a journal op, so the
// resolve is one indexed lookup that matches nothing — and a global `active`
// flag skips even that on a host that never journals.

package main

import (
	"strings"
	"sync"
	"sync/atomic"
)

// journal_inflight_ttl bounds how long an unresolved inflight row may linger
// before the sweep drops it — covers a queue message that was dropped (peer
// permanently gone) instead of acked. Generous: an ack normally lands in
// seconds.
const journal_inflight_ttl = 24 * 60 * 60

var (
	journal_inflight_active   atomic.Bool
	journal_inflight_ensured  sync.Map // qdb.path -> struct{}
	journal_inflight_ensureMu sync.Mutex
)

// journal_inflight_ensure lazily creates the inflight table in queue.db, so no
// schema migration is needed and the table only appears on hosts that journal.
func journal_inflight_ensure(qdb *DB) {
	if qdb == nil || qdb.path == "" {
		return
	}
	if _, done := journal_inflight_ensured.Load(qdb.path); done {
		return
	}
	journal_inflight_ensureMu.Lock()
	defer journal_inflight_ensureMu.Unlock()
	if _, done := journal_inflight_ensured.Load(qdb.path); done { // re-check under the lock
		return
	}
	qdb.exec("create table if not exists journal_inflight (id text primary key, user text not null, peer text not null, stream text not null, sequence integer not null, created integer not null)")
	journal_inflight_ensured.Store(qdb.path, struct{}{})
}

// journal_inflight_record notes that message `id` carries op `sequence` of
// (user, peer, stream), so the ack path can advance the delivery cursor when the
// peer confirms receipt. Called from journal_ship_real for every per-peer send.
func journal_inflight_record(id, user, peer, stream string, sequence int64) {
	if id == "" || peer == "" || stream == "" || sequence <= 0 {
		return
	}
	qdb := db_open("db/queue.db")
	if qdb == nil {
		return
	}
	journal_inflight_ensure(qdb)
	qdb.exec("insert into journal_inflight (id, user, peer, stream, sequence, created) values (?, ?, ?, ?, ?, ?) on conflict(id) do nothing",
		id, user, peer, stream, sequence, now())
	journal_inflight_active.Store(true)
}

// journal_inflight_acked resolves transport ACKs: for any acked message that was
// a journal op, advance that peer's delivery cursor and clear the inflight row.
// On the hot ack path, so it stays cheap — the global flag skips it entirely on
// a non-journaling host, and a common (non-journal) ack resolves to no rows.
func journal_inflight_acked(ids []string) {
	if !journal_inflight_active.Load() || len(ids) == 0 {
		return
	}
	qdb := db_open("db/queue.db")
	if qdb == nil {
		return
	}
	ph := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		ph[i] = "?"
		args[i] = id
	}
	rows, err := qdb.rows("select id, user, peer, stream, sequence from journal_inflight where id in ("+strings.Join(ph, ",")+")", args...)
	if err != nil || len(rows) == 0 {
		return
	}
	rdb := db_open("db/replication.db")
	// journal_delivery is created lazily by replication_journal_tables_ensure
	// (the journaling path calls it); the delivery-ack path must too, or a fresh
	// replication.db — e.g. just after a replica reset+rejoin, before any local
	// journal op has run — panics the whole server with "no such table:
	// journal_delivery" on the insert below.
	replication_journal_tables_ensure()
	matched := make([]any, 0, len(rows))
	mph := make([]string, 0, len(rows))
	for _, r := range rows {
		id, _ := r["id"].(string)
		user, _ := r["user"].(string)
		peer, _ := r["peer"].(string)
		stream, _ := r["stream"].(string)
		seq, _ := r["sequence"].(int64)
		rdb.exec("insert into journal_delivery (user, peer, stream, sequence) values (?, ?, ?, ?) on conflict(user, peer, stream) do update set sequence = max(journal_delivery.sequence, excluded.sequence)",
			user, peer, stream, seq)
		matched = append(matched, id)
		mph = append(mph, "?")
	}
	qdb.exec("delete from journal_inflight where id in ("+strings.Join(mph, ",")+")", matched...)
}

// journal_delivery_cursor returns the highest sequence `peer` has confirmed for
// (user, stream), or 0 if none — the backfill floor below which a peer already
// holds every op.
func journal_delivery_cursor(rdb *DB, user, peer, stream string) int64 {
	if rdb == nil {
		return 0
	}
	row, err := rdb.row("select sequence from journal_delivery where user=? and peer=? and stream=?", user, peer, stream)
	if err != nil || row == nil {
		return 0
	}
	seq, _ := row["sequence"].(int64)
	return seq
}

// journal_inflight_sweep drops inflight rows whose message was never acked
// within journal_inflight_ttl (the peer went away before confirming), so the
// table can't grow without bound. Called from journal_sweep.
func journal_inflight_sweep() {
	if !journal_inflight_active.Load() {
		return
	}
	qdb := db_open("db/queue.db")
	if qdb == nil {
		return
	}
	qdb.exec("delete from journal_inflight where created < ?", now()-journal_inflight_ttl)
}

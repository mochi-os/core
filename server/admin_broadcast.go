// Mochi server: /_/admin/broadcast/* handlers
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.
//
// Operator visibility into the broadcast subsystem. Today: lag
// detection (task #83) - scan every per-user-app DB for received
// vs log to surface subscribers that have fallen behind the owner
// without firing user-visible errors. The original broadcast
// investigation report (claude/sessions/2026-05-25-broadcast-resync-
// stuck-diagnosis.md) called this out as fix #5 - drift that the
// gap-detector can't self-detect (idle owner) was invisible to the
// operator and only surfaced when a user noticed wrong data.

//go:build linux || darwin || windows

package main

import (
	"net/http"
	"os"
	"path/filepath"
	"sort"

	"github.com/gin-gonic/gin"
)

// BroadcastLagRow is the per-stream lag report. owner_log_max is the
// owner-side log.max(sequence) for the same (key, peer) when this
// host happens to own that broadcast (the (key, peer) pair lives in
// the local log too). It's null when this host is a pure subscriber
// for the stream, in which case lag has to be computed cross-host -
// the receiver-side report alone shows "we're at N", not "we should
// be at M". Operator follows up with a remote query if needed.
type BroadcastLagRow struct {
	User         string  `json:"user"`
	App          string  `json:"app"`
	Peer         string  `json:"peer"`
	Key          string  `json:"key"`
	ReceivedLast int64   `json:"received_last"`
	OwnerLogMax  *int64  `json:"owner_log_max,omitempty"`
	Lag          *int64  `json:"lag,omitempty"`
	Pending      int     `json:"pending"`
}

// admin_broadcast_lag is GET /_/admin/broadcast/lag. Scans every
// per-user-app DB under users/<uid>/<app>/db/*.db, gathers received
// and log rows, and produces a single flat list keyed on
// (user, app, peer, key). When the local host is also the owner of
// the stream (same (key, peer) appears in log on this same DB),
// lag = log.max - received.last; otherwise omitted.
//
// Query param: ?threshold=N reports only rows with Lag > N. Default
// 0 = all rows including healthy ones, useful for a periodic
// dashboard scrape.
func admin_broadcast_lag(c *gin.Context) {
	threshold := int64(0)
	if v := c.Query("threshold"); v != "" {
		if n := atoi(v, 0); n > 0 {
			threshold = int64(n)
		}
	}
	rows := broadcast_lag_scan()
	out := make([]BroadcastLagRow, 0, len(rows))
	for _, r := range rows {
		if r.Lag != nil && *r.Lag < threshold {
			continue
		}
		out = append(out, r)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].User != out[j].User {
			return out[i].User < out[j].User
		}
		if out[i].App != out[j].App {
			return out[i].App < out[j].App
		}
		if out[i].Lag != nil && out[j].Lag != nil && *out[i].Lag != *out[j].Lag {
			return *out[i].Lag > *out[j].Lag
		}
		return out[i].Key < out[j].Key
	})
	c.JSON(http.StatusOK, gin.H{"rows": out})
}

// broadcast_lag_scan walks the user tree and assembles one
// BroadcastLagRow per (user, app, peer, key) in received. Tables
// that don't exist are skipped silently - most apps don't use the
// broadcast subsystem, so absent tables are the common case.
//
// The broadcast tables (received, log, pending, etc.)
// live in the per-app system DB (users/<uid>/<app>/app.db), NOT in
// the app's writable data DB - see task #90 for the architectural
// rationale. The scanner therefore looks for app.db, not the
// per-app db/*.db files.
func broadcast_lag_scan() []BroadcastLagRow {
	var out []BroadcastLagRow
	users_root := filepath.Join(data_dir, "users")
	users, err := os.ReadDir(users_root)
	if err != nil {
		return out
	}
	for _, u := range users {
		if !u.IsDir() {
			continue
		}
		user := u.Name()
		user_dir := filepath.Join(users_root, user)
		apps, err := os.ReadDir(user_dir)
		if err != nil {
			continue
		}
		for _, a := range apps {
			if !a.IsDir() {
				continue
			}
			app := a.Name()
			path := filepath.Join("users", user, app, "app.db")
			abs := filepath.Join(data_dir, path)
			if !file_exists(abs) {
				continue
			}
			out = append(out, broadcast_lag_scan_db(user, app, path)...)
		}
	}
	return out
}

// admin_broadcast_pending_gc is POST /_/admin/broadcast/pending/gc.
// Runs the unfillable-gap skip pass on demand and returns the count
// of gaps skipped. Mirror of admin_replication_pending_gc - exposed so
// an operator who's just shipped a fix can immediately unstick the
// existing pile of stalled streams instead of waiting for the hourly
// scheduled pass.
//
// ?force=true bypasses the TTL gate. Default false means the endpoint
// behaves like the background pass: only entries older than the
// configured TTL get skipped. Set force=true when an operator KNOWS
// the buffered gap is unfillable (the gap-fill seqs are pruned from
// the sender's log) and waiting for natural TTL expiry is just a
// formality.
func admin_broadcast_pending_gc(c *gin.Context) {
	force := c.Query("force") == "true"
	skipped := broadcast_pending_gc(force)
	c.JSON(http.StatusOK, gin.H{"skipped": skipped, "force": force})
}

// broadcast_lag_scan_db reads one app DB's received table and joins
// against log when present. Per-row lag is omitted when the local
// DB doesn't have the matching log entry (the host isn't the owner
// of the stream).
func broadcast_lag_scan_db(user, app, db_path string) []BroadcastLagRow {
	var out []BroadcastLagRow
	db := db_open(db_path)
	if db == nil {
		return out
	}
	exists, _ := db.exists("select 1 from sqlite_master where type='table' and name='received'")
	if !exists {
		return out
	}
	received_rows, err := db.rows("select sender, key, last from received")
	if err != nil {
		return out
	}
	has_log, _ := db.exists("select 1 from sqlite_master where type='table' and name='log'")
	has_pending, _ := db.exists("select 1 from sqlite_master where type='table' and name='pending'")
	for _, r := range received_rows {
		peer, _ := r["sender"].(string)
		key, _ := r["key"].(string)
		last, _ := r["last"].(int64)
		row := BroadcastLagRow{
			User:         user,
			App:          app,
			Peer:         peer,
			Key:          key,
			ReceivedLast: last,
		}
		if has_log {
			log_row, _ := db.row("select max(sequence) as m from log where key=? and peer=?", key, peer)
			if log_row != nil {
				if m, ok := log_row["m"].(int64); ok && m > 0 {
					owner_max := m
					lag := owner_max - last
					row.OwnerLogMax = &owner_max
					row.Lag = &lag
				}
			}
		}
		if has_pending {
			row.Pending = db.integer("select count(*) from pending where peer=? and key=?", peer, key)
		}
		out = append(out, row)
	}
	return out
}

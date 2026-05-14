// Mochi server: pair-join backfill (replaces bulk bootstrap for sysdbs).
// Copyright Alistair Cunningham 2026
//
// Why this exists: bulk bootstrap's atomic-rename-of-snapshot strategy
// is correct for files (`users/<u>/<app>/files/`), installed app code
// (`apps/<entity>/`), and per-user app DBs (`users/<u>/<app>/db/*.db`)
// because none of those are held open by the running receiver at
// pair-join time. It is fundamentally broken for system DBs (`db/users.db`,
// `db/apps.db`, etc.) — those are opened during server startup and held
// for the process lifetime. A `rename(2)` swaps the directory entry but
// the receiver's existing file descriptors continue to point at the
// now-unlinked original inode. SQLite WAL writes go to the vanishing
// file; reads return the original; the renamed-in snapshot is invisible.
// Observed live: instance 2 crashed with "database is locked" in
// queue_add_direct, and a subsequent run showed 0 users in users.db even
// though the on-disk file was the source's 53KB snapshot.
//
// The fix: don't bootstrap sysdbs as raw files. Instead, on pair-join
// approval, the source enumerates every replicated row of every system
// table and emits it to the new peer via the existing system-set /
// system-row / keys-transfer op channel. Those ops apply through
// normal SQL paths (REPLACE INTO), bypassing the open-fd trap entirely.
//
// Coverage:
//   - users.db: every active user via replication_transfer_keys (one
//     keys-transfer per user, carries username + all owned entities
//     including private keys).
//   - settings.db.settings: every row via SystemSet.
//   - apps.db.classes / .services / .paths: every row via SystemSet.
//   - apps.db.apps (install registry): every row via SystemSet.
//   - apps.db.versions / .tracks: every row via SystemRow.
//   - domains.db.domains / .routes / .delegations: every row via SystemRow.
//
// Not covered (intentional):
//   - directory.db: per-server entity discovery cache, regenerates from
//     P2P traffic.
//   - sessions.db: session activity replicates via per-event ops as
//     things happen.
//   - schedule.db: per-server scheduled events; the leader-claim
//     pattern handles cross-replica coordination separately.
//   - external.db: account state replicates via mochi.account ops.
//   - queue.db / replication.db / peers.db: server-local state
//     machines whose contents are meaningful only to the host that
//     owns them.

package main

import (
	"fmt"
	"strconv"
)

// replication_pair_backfill runs on the source after approving a pair
// join. Direct-emits every replicated row to the new peer via the
// existing op channel. Idempotent on the receiver (REPLACE INTO + INSERT
// OR IGNORE in the apply handlers); a re-run after partial delivery
// just re-sends rows the receiver already has.
//
// Async: returns immediately. The actual messages are queued via
// message().send_peer() which the queue manager drains in the
// background.
//
// Package-level variable so tests can stub the backfill out — useful
// when a test only cares about the local-DB effect of join-approve
// and doesn't want the side-effect emit traffic.
var replication_pair_backfill = replication_pair_backfill_impl

func replication_pair_backfill_impl(peer string) {
	if peer == "" || peer == p2p_id {
		return
	}
	replication_pair_backfill_users(peer)
	replication_pair_backfill_system(peer)
	info("Replication pair-backfill: dispatched users + system rows to peer %q", peer)
}

// replication_pair_backfill_users enumerates active users.db.users and
// emits a keys-transfer for each user to `peer`. Each keys-transfer
// carries the user's username + every owned entity (including private
// keys) — the receiver's keys-transfer handler creates the user row
// fresh when no local user with that username exists, then inserts
// each entity.
func replication_pair_backfill_users(peer string) {
	udb := db_open("db/users.db")
	rows, err := udb.rows("select uid from users where status = 'active'")
	if err != nil {
		warn("Replication pair-backfill users: enumerate failed: %v", err)
		return
	}
	count := 0
	for _, r := range rows {
		uid, _ := r["uid"].(string)
		if uid == "" {
			continue
		}
		if replication_transfer_keys_var(uid, peer) {
			count++
		}
	}
	debug("Replication pair-backfill: keys-transfer queued for %d users to peer %q", count, peer)
}

// replication_pair_backfill_system enumerates every replicated row of
// every replicated system table and emits it to `peer` via the
// single-target send helpers. Order matters for some tables (e.g.
// `apps.apps` install registry typically lands before per-app
// `versions` / `tracks` references), but the receiver's apply handlers
// are tolerant — out-of-order rows insert successfully when the schema
// doesn't enforce a FK ordering constraint.
func replication_pair_backfill_system(peer string) {
	// settings.db.settings
	sdb := db_open("db/settings.db")
	if rows, err := sdb.rows("select name, value from settings"); err == nil {
		for _, r := range rows {
			name, _ := r["name"].(string)
			value, _ := r["value"].(string)
			if name == "" {
				continue
			}
			replication_system_set_to_peer_var(peer, "settings", "settings", name, "value", value)
		}
	}

	// apps.db two-column key tables (classes / services / paths).
	adb := db_apps()
	for _, t := range []struct{ table, keyCol string }{
		{"classes", "class"},
		{"services", "service"},
		{"paths", "path"},
	} {
		if rows, err := adb.rows(fmt.Sprintf("select %s as k, app from %s", t.keyCol, t.table)); err == nil {
			for _, r := range rows {
				key, _ := r["k"].(string)
				app, _ := r["app"].(string)
				if key == "" {
					continue
				}
				replication_system_set_to_peer_var(peer, "apps", t.table, key, "app", app)
			}
		}
	}

	// apps.db.apps (install registry): row=app, field='installed', value=timestamp.
	if rows, err := adb.rows("select app, installed from apps"); err == nil {
		for _, r := range rows {
			app, _ := r["app"].(string)
			if app == "" {
				continue
			}
			installed, _ := r["installed"].(int64)
			replication_system_set_to_peer_var(peer, "apps", "apps", app, "installed",
				strconv.FormatInt(installed, 10))
		}
	}

	// apps.db.versions: row-level, key=(app), cols={version, track}.
	if rows, err := adb.rows("select app, version, track from versions"); err == nil {
		for _, r := range rows {
			app, _ := r["app"].(string)
			if app == "" {
				continue
			}
			version, _ := r["version"].(string)
			track, _ := r["track"].(string)
			replication_system_row_to_peer_var(peer, "apps", "versions",
				map[string]string{"app": app},
				map[string]string{"version": version, "track": track}, false)
		}
	}

	// apps.db.tracks: row-level, key=(app, track), cols={version}.
	if rows, err := adb.rows("select app, track, version from tracks"); err == nil {
		for _, r := range rows {
			app, _ := r["app"].(string)
			track, _ := r["track"].(string)
			if app == "" || track == "" {
				continue
			}
			version, _ := r["version"].(string)
			replication_system_row_to_peer_var(peer, "apps", "tracks",
				map[string]string{"app": app, "track": track},
				map[string]string{"version": version}, false)
		}
	}

	// domains.db.domains: row-level, key=(domain).
	ddb := db_open("db/domains.db")
	if rows, err := ddb.rows("select domain, verified, token, tls, created, updated from domains"); err == nil {
		for _, r := range rows {
			domain, _ := r["domain"].(string)
			if domain == "" {
				continue
			}
			verified, _ := r["verified"].(int64)
			token, _ := r["token"].(string)
			tls, _ := r["tls"].(int64)
			created, _ := r["created"].(int64)
			updated, _ := r["updated"].(int64)
			replication_system_row_to_peer_var(peer, "domains", "domains",
				map[string]string{"domain": domain},
				map[string]string{
					"verified": strconv.FormatInt(verified, 10),
					"token":    token,
					"tls":      strconv.FormatInt(tls, 10),
					"created":  strconv.FormatInt(created, 10),
					"updated":  strconv.FormatInt(updated, 10),
				}, false)
		}
	}

	// domains.db.routes: row-level, key=(domain, path).
	if rows, err := ddb.rows("select domain, path, method, target, context, owner, priority, enabled, created, updated from routes"); err == nil {
		for _, r := range rows {
			domain, _ := r["domain"].(string)
			path, _ := r["path"].(string)
			if domain == "" || path == "" {
				continue
			}
			method, _ := r["method"].(string)
			target, _ := r["target"].(string)
			context, _ := r["context"].(string)
			owner, _ := r["owner"].(string)
			priority, _ := r["priority"].(int64)
			enabled, _ := r["enabled"].(int64)
			created, _ := r["created"].(int64)
			updated, _ := r["updated"].(int64)
			replication_system_row_to_peer_var(peer, "domains", "routes",
				map[string]string{"domain": domain, "path": path},
				map[string]string{
					"method":   method,
					"target":   target,
					"context":  context,
					"owner":    owner,
					"priority": strconv.FormatInt(priority, 10),
					"enabled":  strconv.FormatInt(enabled, 10),
					"created":  strconv.FormatInt(created, 10),
					"updated":  strconv.FormatInt(updated, 10),
				}, false)
		}
	}

	// domains.db.delegations: row-level, key=(domain, path, owner).
	if rows, err := ddb.rows("select domain, path, owner, created from delegations"); err == nil {
		for _, r := range rows {
			domain, _ := r["domain"].(string)
			path, _ := r["path"].(string)
			owner, _ := r["owner"].(string)
			if domain == "" || path == "" || owner == "" {
				continue
			}
			created, _ := r["created"].(int64)
			replication_system_row_to_peer_var(peer, "domains", "delegations",
				map[string]string{"domain": domain, "path": path, "owner": owner},
				map[string]string{"created": strconv.FormatInt(created, 10)}, false)
		}
	}
}

// replication_system_set_to_peer_var emits a SystemSet to a single peer
// (vs replication_emit_system_set which fans out to every pair member).
// Used by pair-backfill to direct-send rows to the newly-joining peer
// without flooding existing pair members with already-have-data noise.
//
// Package-level variable so tests can stub the wire emission.
var replication_system_set_to_peer_var = replication_system_set_to_peer_impl

func replication_system_set_to_peer(peer, database, table, row, field, value string) {
	replication_system_set_to_peer_var(peer, database, table, row, field, value)
}

func replication_system_set_to_peer_impl(peer, database, table, row, field, value string) {
	if peer == "" || peer == p2p_id {
		return
	}
	m := message("", "", "replication", "system-set")
	m.add(&SystemSet{
		Database: database, Table: table, Row: row, Field: field, Value: value,
	})
	m.send_peer(peer)
}

// replication_system_row_to_peer_var is the row-level companion to
// replication_system_set_to_peer_var.
var replication_system_row_to_peer_var = replication_system_row_to_peer_impl

func replication_system_row_to_peer(peer, database, table string, key, cols map[string]string, del bool) {
	replication_system_row_to_peer_var(peer, database, table, key, cols, del)
}

func replication_system_row_to_peer_impl(peer, database, table string, key, cols map[string]string, del bool) {
	if peer == "" || peer == p2p_id {
		return
	}
	m := message("", "", "replication", "system-row")
	m.add(&SystemRow{
		Database: database, Table: table, Key: key, Cols: cols, Delete: del,
	})
	m.send_peer(peer)
}

// replication_transfer_keys_var is the package-level alias for
// replication_transfer_keys, exposed so pair-backfill tests can stub
// the per-user emit out and just record which users were transferred.
var replication_transfer_keys_var = replication_transfer_keys

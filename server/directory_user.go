// Mochi server: Per-user learned directory
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

// The user directory is each user's private routing memory: entity → peer
// mappings learned from claim-verified inbound traffic. It resolves delivery
// to PRIVATE entities, which the public directory deliberately never lists —
// a private subscriber's posts, a private project's comments (#202, #209).
//
// Provenance is the privacy model: a row exists only for entities that have
// contacted this user, so delivery capability mirrors the relationships the
// counterpart itself created. Rows are never shared between users (a
// co-tenant's relationship must not extend another user's reach), never
// synced, never served to peers.
//
// Lifecycle differs from the public directory on purpose. Public entries are
// refreshed by traffic-independent hourly republish, so silence means the
// host is gone and time-based expiry is garbage collection. Here the refresh
// source is contact, whose cadence is workload-dependent — a feed that posts
// twice a year is quiet, not dead — so rows never expire by age. Eviction is
// event-driven only: fresher contact re-upserts, a send that exhausts the
// queue's retry budget deletes the proven-dead row, and a generous LRU cap
// bounds growth against contact spam (minting entities is free, so a spammer
// could otherwise grow the table without limit).

const (
	// directory_user_cap bounds rows per user; beyond it the least
	// recently seen rows are evicted. Real relationship counts sit far
	// below this — the cap exists for spam, not for users.
	directory_user_cap = 10000

	// directory_user_refresh throttles seen updates: a row refreshed
	// within this window is not rewritten unless the peer changed.
	// user.db is the cold, backup-critical store; without the throttle a
	// busy chat would write it once per inbound message.
	directory_user_refresh = 3600
)

func directory_user_table(db *DB) {
	db.exec("create table if not exists directory (entity text not null, peer text not null, fingerprint text not null default '', created integer not null, seen integer not null, primary key (entity, peer))")
}

// directory_user_learn records that `entity` was verifiably reached via
// `peer`, in `user`'s directory. Callers must pass only claim-verified
// (entity, peer) pairs — the claim handshake is what stops a spoofed
// `from` header hijacking an entity's delivery route.
func directory_user_learn(user *User, entity string, peer string) {
	if user == nil || entity == "" || peer == "" || peer == net_id || !valid(entity, "entity") {
		return
	}
	// Local entities resolve locally; a learned row would be stale noise.
	// Fail-safe on an errored ownership check: refuse the learn rather
	// than admit a foreign route for what may be a local entity.
	if local, ok := entity_local(entity); !ok || local {
		return
	}
	db := db_user(user, "user")
	if db == nil {
		return
	}
	now_ts := now()
	row, _ := db.row("select peer, seen from directory where entity=? and peer=?", entity, peer)
	if row != nil {
		if seen, ok := row["seen"].(int64); ok && seen > now_ts-directory_user_refresh {
			return
		}
		db.exec("update directory set seen=? where entity=? and peer=?", now_ts, entity, peer)
		return
	}
	db.exec("insert or replace into directory (entity, peer, fingerprint, created, seen) values (?, ?, ?, ?, ?)",
		entity, peer, fingerprint(entity), now_ts, now_ts)
	// LRU cap: evict the least recently seen rows beyond the cap.
	if total := db.integer("select count(*) from directory"); total > directory_user_cap {
		db.exec("delete from directory where (entity, peer) in (select entity, peer from directory order by seen asc limit ?)",
			total-directory_user_cap)
	}
}

// directory_user_confirm bumps a row's seen after a successful delivery to
// (entity, peer) — outbound success is location proof as good as inbound
// contact, and it keeps one-directional relationships (a quiet feed's
// subscribers) fresh in the LRU ordering. Same write throttle as learning.
func directory_user_confirm(user *User, entity string, peer string) {
	if user == nil || entity == "" || peer == "" || peer == net_id {
		return
	}
	db := db_user(user, "user")
	if db == nil {
		return
	}
	now_ts := now()
	db.exec("update directory set seen=? where entity=? and peer=? and seen <= ?",
		now_ts, entity, peer, now_ts-directory_user_refresh)
}

// directory_user_forget deletes a row proven dead: a send to (entity, peer)
// exhausted the queue's retry budget. This is the table's only eviction
// besides the LRU cap — rows never expire by age, because for private
// entities silence means a quiet relationship, not a dead host.
func directory_user_forget(user *User, entity string, peer string) {
	if user == nil || entity == "" || peer == "" {
		return
	}
	db := db_user(user, "user")
	if db == nil {
		return
	}
	db.exec("delete from directory where entity=? and peer=?", entity, peer)
}

// directory_user_peers returns the peers `user` has learned for `entity`,
// freshest seen first, with NO age filter — an old row is still the best
// available lead for a private entity, and the caller's merge ordering
// lets any fresher public row outrank it naturally.
func directory_user_peers(user *User, entity string) []map[string]any {
	if user == nil || entity == "" {
		return nil
	}
	db := db_user(user, "user")
	if db == nil {
		return nil
	}
	rows, _ := db.rows("select peer, seen from directory where entity=? order by seen desc", entity)
	return rows
}

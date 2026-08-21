// Mochi server: Per-user learned directory
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

// The user directory is each user's private routing memory: entity → peer rows
// learned from claim-verified contact, never shared or served, and the only
// route to PRIVATE entities the public directory never lists. Rows never expire
// by age - a quiet relationship is not a dead host - so eviction is
// event-driven only.

const (
	// directory_user_cap bounds rows per user; beyond it rows are evicted
	// never-confirmed first, then least recently seen. Real relationship
	// counts sit far below this — the cap exists for spam, not for users.
	directory_user_cap = 10000

	// directory_user_peer_cap bounds one peer's rows in a single user's directory.
	// Peer identities are free to mint, so this bounds one identity's share, not
	// the attack - the eviction order below does that.
	directory_user_peer_cap = 1000

	// directory_user_refresh throttles seen updates: a row refreshed
	// within this window is not rewritten unless the peer changed.
	// user.db is the cold, backup-critical store; without the throttle a
	// busy chat would write it once per inbound message.
	directory_user_refresh = 3600
)

func directory_user_table(db *DB) {
	db.exec("create table if not exists directory (entity text not null, peer text not null, fingerprint text not null default '', created integer not null, seen integer not null, confirmed integer not null default 0, primary key (entity, peer))")
	if exists, _ := db.exists("select 1 from pragma_table_info('directory') where name='confirmed'"); !exists {
		db.exec("alter table directory add column confirmed integer not null default 0")
	}
	// The per-peer admission count below filters on peer alone, which the
	// (entity, peer) primary key cannot serve.
	db.exec("create index if not exists directory_peer on directory(peer)")
}

// directory_user_learn records that `entity` was verifiably reached via
// `peer`, in `user`'s directory. Callers must pass only claim-verified
// (entity, peer) pairs — the claim handshake is what stops a spoofed
// `from` header hijacking an entity's delivery route.
func directory_user_learn(user *User, entity string, peer string) {
	if user == nil || entity == "" || peer == "" || peer == net_id || !valid(entity, "entity") {
		return
	}
	// Claim-verified inbound contact from the entity is liveness
	// evidence: clear any delivery-failure streak (unsuspends a
	// recipient whose host just came back and contacted us first).
	health_success(entity)
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
	// One peer may not hold the whole table. Refusing rather than evicting
	// within the peer keeps a flooder from also churning its own earlier
	// rows, which would hide the flood from the row counts an operator sees.
	if db.integer("select count(*) from directory where peer=?", peer) >= directory_user_peer_cap {
		debug("Directory user refusing %q from peer %q for user %q: peer already holds %d rows", entity, peer, user.UID, directory_user_peer_cap)
		return
	}
	db.exec("insert or replace into directory (entity, peer, fingerprint, created, seen) values (?, ?, ?, ?, ?)",
		entity, peer, fingerprint(entity), now_ts, now_ts)
	// Evict never-confirmed rows first, least recently seen within each group.
	// Ordering on seen alone hands a flood the win - its rows are always the
	// freshest - and confirmed, set only on successful delivery, cannot be faked.
	if total := db.integer("select count(*) from directory"); total > directory_user_cap {
		db.exec("delete from directory where (entity, peer) in (select entity, peer from directory order by confirmed asc, seen asc limit ?)",
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
	// Outside the seen throttle deliberately: a relationship whose only delivery
	// fell inside the refresh window would never be marked, and the cap would then
	// evict exactly the quiet counterpart it must protect.
	db.exec("update directory set confirmed=? where entity=? and peer=? and confirmed=0",
		now_ts, entity, peer)
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

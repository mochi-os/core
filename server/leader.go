// Mochi server: Lease-based leader election pattern library helper
// Copyright Alistair Cunningham 2026

package main

import (
	sl "go.starlark.net/starlark"
)

// Lease duration. Long enough that brief partitions don't churn
// leadership, short enough that a dead leader's work resumes within a
// reasonable window. Renewed on every successful claim.
const leader_lease_seconds = 60

// mochi.schedule.leader(scope, key) -> bool: returns True iff this host
// is the current leader for the (scope, key) lease. Apps gate scheduled
// work on the return so only one replica fires per logical event.
//
// V1: each host claims locally via INSERT … ON CONFLICT. The ON CONFLICT
// WHERE clause refuses to overwrite an active lease held by a different
// peer, so the per-(scope, key) lease is single-owner within one host's
// view of the world. Cross-host conflict (two peers each thinking they
// hold the lease during a partition) is resolved at receive time by the
// fence-token check on emitted ops — leaders attach their current fence,
// receivers honour the highest fence seen and drop ops bearing a stale
// one. The fence-token-on-ops + quorum-acquisition protocol lands as a
// follow-up; the local lease semantics work today and the helper API
// stays stable.
//
// Scope is a free-form string carrying a structured prefix:
//   - "user:<uid>" — events scoped to one user (feeds AI tagging, etc.)
//   - "credential:<id>" — passkey sign_count delegation (task #10)
//   - "platform" — server-wide periodic ticks (cleanup, broadcasts)
func api_schedule_leader(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var scope, key string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "scope", &scope, "key", &key); err != nil {
		return nil, err
	}
	if scope == "" || key == "" {
		return sl_error(fn, "scope and key must be non-empty")
	}

	if replication_leader_claim(scope, key) {
		return sl.True, nil
	}
	return sl.False, nil
}

// replication_leader_claim attempts to acquire or renew the lease for
// (scope, key) on the local host. Returns true if the local host now
// holds the lease. The implementation is a single SQL statement so the
// claim is atomic against concurrent local callers; cross-host races
// resolve via the fence token (see api_schedule_leader doc comment).
func replication_leader_claim(scope, key string) bool {
	db := db_open("db/replication.db")
	n := now()
	expires := n + leader_lease_seconds

	// INSERT new row when no lease exists; UPDATE only when we already
	// hold the lease (renewal) or the existing lease is expired. The
	// fence increments on every grant, including renewals, so any op
	// the leader stamps with the current fence is strictly newer than
	// anything any prior leader emitted.
	db.exec(`insert into leadership (scope, key, peer, expires, fence) values (?, ?, ?, ?, 1)
		on conflict(scope, key) do update set
			peer = excluded.peer,
			expires = excluded.expires,
			fence = leadership.fence + 1
		where leadership.peer = excluded.peer or leadership.expires < ?`,
		scope, key, p2p_id, expires, n)

	row, _ := db.row("select peer, expires from leadership where scope=? and key=?", scope, key)
	if row == nil {
		return false
	}
	peer, _ := row["peer"].(string)
	exp, _ := row["expires"].(int64)
	return peer == p2p_id && exp > n
}

// replication_leader_fence returns the current fence token for the
// (scope, key) lease, if held by this host. Returns 0 when we don't
// hold the lease. Leader-gated callers attach the result to outbound
// replication ops so receivers can reject stale-leader writes once the
// fence-aware apply path lands.
func replication_leader_fence(scope, key string) int64 {
	db := db_open("db/replication.db")
	row, _ := db.row("select fence from leadership where scope=? and key=? and peer=? and expires > ?", scope, key, p2p_id, now())
	if row == nil {
		return 0
	}
	if v, ok := row["fence"].(int64); ok {
		return v
	}
	return 0
}

// replication_leader_release voluntarily drops the lease, e.g. when the
// caller knows it's about to shut down and wants another replica to pick
// up work without waiting for the lease to age out.
func replication_leader_release(scope, key string) {
	db := db_open("db/replication.db")
	db.exec("delete from leadership where scope=? and key=? and peer=?", scope, key, p2p_id)
}

// replication_fence_observe records a leader-stamped op's fence for
// (scope, key) and returns whether the op should be accepted. Returns
// false when `fence` is strictly less than the highest fence already
// observed locally — a sign that the emitter has been superseded by a
// newer leader, and any state it's emitting is stale. Equal-or-greater
// fences are accepted; the witness is upserted atomically (a stale
// concurrent observation can't roll back a newer one because of the
// WHERE clause on the UPSERT).
//
// Callers pass scope="" or fence<=0 for ops that aren't leader-gated;
// those return true unconditionally so non-leader ops pass through.
func replication_fence_observe(scope, key, peer string, fence int64) bool {
	if scope == "" || key == "" || fence <= 0 {
		return true
	}

	db := db_open("db/replication.db")
	db.exec("create table if not exists fence_witness (scope text not null, key text not null, fence integer not null default 0, peer text not null default '', seen integer not null default 0, primary key (scope, key))")

	// Upsert only when the incoming fence beats what's stored. The WHERE
	// on the ON CONFLICT clause makes the comparison atomic with the
	// write so two concurrent observations race deterministically: the
	// higher fence wins regardless of order.
	db.exec(`insert into fence_witness (scope, key, fence, peer, seen) values (?, ?, ?, ?, ?)
		on conflict(scope, key) do update set fence=excluded.fence, peer=excluded.peer, seen=excluded.seen
		where excluded.fence > fence_witness.fence`,
		scope, key, fence, peer, now())

	row, _ := db.row("select fence from fence_witness where scope=? and key=?", scope, key)
	if row == nil {
		return true
	}
	current, _ := row["fence"].(int64)
	return fence >= current
}

// replication_fence_current returns the highest fence observed for
// (scope, key) and the peer that emitted it. Returns (0, "") when
// nothing has been observed yet or the witness table doesn't exist.
func replication_fence_current(scope, key string) (int64, string) {
	db := db_open("db/replication.db")
	exists, _ := db.exists("select 1 from sqlite_master where type='table' and name='fence_witness'")
	if !exists {
		return 0, ""
	}
	row, _ := db.row("select fence, peer from fence_witness where scope=? and key=?", scope, key)
	if row == nil {
		return 0, ""
	}
	fence, _ := row["fence"].(int64)
	peer, _ := row["peer"].(string)
	return fence, peer
}

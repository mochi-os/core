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

// Mochi server: Lease-based leader election pattern library helper
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"crypto/sha256"
	"strings"

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
// Three-tier flow inside replication_leader_claim:
//
//  1. Fast path: local row says we hold an alive lease → renew (bump
//     fence, push expires) + return True. Local row says another peer
//     holds an alive lease → return False without any RPC.
//
//  2. Else fan out a sync RPC (replica/leader/claim) to each peer in
//     the (scope, key) membership. Any explicit denial with a current-
//     leader pointer vetoes the claim and we mirror that leader into
//     our local row so future calls go fast-path-deny.
//
//  3. If all peers grant (or are unreachable — optimistic partition
//     policy), commit the lease locally with an incremented fence and
//     fire a fire-and-forget replica/leader/granted notice to every
//     peer so their views stay consistent.
//
// Scope membership is parsed from the prefix: "user:<uid>" resolves
// to replication.db.hosts.peer for that uid; everything else uses
// replication.db.pair.peer. Scope examples:
//   - "user:<uid>" — events scoped to one user (feeds AI tagging, etc.)
//   - "credential:<id>" — passkey sign_count delegation (task #10)
//   - "platform" — server-wide periodic ticks (cleanup, broadcasts)
//
// Tie-break for simultaneous claims: sha256(scope|key|peer) lowest
// digest wins. The vote rule in replication_leader_vote grants if
// (a) the row is vacant or expired, (b) the proposer is the current
// holder (renewal), or (c) replication_leader_prefer picks the
// proposer over the current holder. Combined with a tentative local
// write that lands before fan-out, simultaneous claims converge on
// the hash-favoured host for that (scope, key). The hash-per-key
// distribution means leadership spreads across the peerset by key
// rather than concentrating on the lowest-id host.
//
// Concurrent claims that both reach step 3 (cross-host partition with
// stale views) are caught at apply time by the fence-token check in
// replication_op_event: only one leader's emitted ops carry the higher
// fence, the other's are dropped as superseded. Write correctness
// survives even when compute briefly runs twice during a partition.
func api_schedule_leader(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var scope, key string
	var strict bool
	if err := sl.UnpackArgs(fn.Name(), args, kwargs,
		"scope", &scope,
		"key", &key,
		"strict?", &strict,
	); err != nil {
		return nil, err
	}
	if scope == "" || key == "" {
		return sl_error(fn, "scope and key must be non-empty")
	}

	if replication_leader_claim(scope, key, strict) {
		return sl.True, nil
	}
	return sl.False, nil
}

// replication_leader_claim attempts to acquire or renew the lease for
// (scope, key). See api_schedule_leader's doc comment for the
// three-tier flow + design notes.
//
// strict controls partition policy:
//   - false (default, optimistic): any explicit denial vetoes; nil
//     responses (unreachable peers) count as no-veto. A partition-
//     isolated minority can elect itself; fence-on-ops drops the
//     loser's writes when the partition heals.
//   - true: require a strict majority of (self + membership) to
//     grant. Nil responses count against the proposer. A minority
//     partition cannot elect at all - useful when external side
//     effects (charges, irrevocable emails) make the partition-loser
//     drop-on-heal semantics unacceptable.
func replication_leader_claim(scope, key string, strict bool) bool {
	db := db_open("db/replication.db")
	n := now()
	expires := n + leader_lease_seconds

	// Fast path: local row decides without RPC if it's authoritative.
	row, _ := db.row("select peer, expires from leadership where scope=? and key=?", scope, key)
	if row != nil {
		cur_peer, _ := row["peer"].(string)
		cur_exp, _ := row["expires"].(int64)
		if cur_peer == net_id && cur_exp > n {
			db.exec("update leadership set expires=?, fence=fence+1 where scope=? and key=? and peer=?", expires, scope, key, net_id)
			return true
		}
		if cur_peer != "" && cur_peer != net_id && cur_exp > n {
			return false
		}
	}

	// Tentative-write: claim locally before fanning out so concurrent
	// RPCs from peers see our intent. With the hash tie-break in
	// replication_leader_vote, simultaneous claims converge on the
	// host whose sha256(scope|key|peer) is smallest for this lease -
	// the tentative row makes the proposer the current holder when
	// the other host's vote arrives, and the hash tie-break favours
	// the proposer iff its digest beats the current holder's.
	db.exec(`insert into leadership (scope, key, peer, expires, fence) values (?, ?, ?, ?, 1)
		on conflict(scope, key) do update set
			peer = excluded.peer,
			expires = excluded.expires,
			fence = leadership.fence + 1
		where leadership.peer = excluded.peer or leadership.expires < ?`,
		scope, key, net_id, expires, n)

	// Fan out + count grants. In optimistic mode any explicit denial
	// with a current-leader pointer vetoes the claim; unreachable
	// peers (nil response) count as no-veto. In strict mode we
	// require a strict majority of (self + membership) to grant; any
	// nil response counts against the proposer.
	membership := replication_leader_membership(scope)
	grants := 0
	for _, peer := range membership {
		if peer == "" || peer == net_id {
			continue
		}
		res := replication_leader_claim_rpc(peer, scope, key, expires)
		if res == nil {
			continue
		}
		if res.Granted {
			grants++
			continue
		}
		if res.CurrentLeader != "" && res.CurrentLeader != net_id {
			db.exec(`insert into leadership (scope, key, peer, expires, fence) values (?, ?, ?, ?, ?)
				on conflict(scope, key) do update set
					peer = excluded.peer,
					expires = excluded.expires,
					fence = excluded.fence`,
				scope, key, res.CurrentLeader, res.CurrentExpires, res.CurrentFence)
			return false
		}
	}

	// Strict mode: enforce strict majority of (self + membership).
	// Self counts as 1 grant; need len(membership)/2 more grants
	// from peers to clear the majority threshold. (For membership
	// of 1 peer we need 1 peer grant; for 2 peers, 1; for 3, 2;
	// for 4, 2; ie. floor(total/2) peer grants where total =
	// len(membership) + 1.)
	if strict {
		total := len(membership) + 1
		if grants < total/2 {
			return false
		}
	}

	// Verify our tentative row still stands — a concurrent caller on
	// the same host (rare goroutine race) or a notify from a competing
	// peer could have overwritten it during fan-out.
	row, _ = db.row("select peer, expires, fence from leadership where scope=? and key=?", scope, key)
	if row == nil {
		return false
	}
	cur_peer, _ := row["peer"].(string)
	cur_exp, _ := row["expires"].(int64)
	cur_fence, _ := row["fence"].(int64)
	if cur_peer != net_id || cur_exp <= n {
		return false
	}

	// Notify peers so their views mirror the new leader.
	replication_leader_notify(scope, key, cur_fence, cur_exp)
	return true
}

// replication_leader_fence returns the current fence token for the
// (scope, key) lease, if held by this host. Returns 0 when we don't
// hold the lease. Leader-gated callers attach the result to outbound
// replication ops so receivers can reject stale-leader writes once the
// fence-aware apply path lands.
func replication_leader_fence(scope, key string) int64 {
	db := db_open("db/replication.db")
	row, _ := db.row("select fence from leadership where scope=? and key=? and peer=? and expires > ?", scope, key, net_id, now())
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
	db.exec("delete from leadership where scope=? and key=? and peer=?", scope, key, net_id)
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

// ----------------------------------------------------------------------
// Cross-host election RPC layer (Stage 22 follow-up).
// ----------------------------------------------------------------------

// LeaderClaimRequest is the proposer's claim-time RPC payload: "may I
// hold (scope, key) until expires?" The proposer's own peer id arrives
// implicitly as e.peer on the recipient's side; no need to send it.
type LeaderClaimRequest struct {
	Scope   string `cbor:"scope"`
	Key     string `cbor:"key"`
	Expires int64  `cbor:"expires"`
}

// LeaderClaimResponse is the recipient's vote. Granted=true means the
// recipient sees no obstacle (vacant/expired row, proposer is current
// holder, or proposer's id wins the lower-id tie-break). On denial the
// CurrentLeader/Fence/Expires fields tell the proposer who actually
// holds the lease so the proposer can mirror that row locally and
// fast-path-deny on the next call.
type LeaderClaimResponse struct {
	Granted        bool   `cbor:"granted"`
	CurrentLeader  string `cbor:"leader,omitempty"`
	CurrentFence   int64  `cbor:"fence,omitempty"`
	CurrentExpires int64  `cbor:"expires,omitempty"`
}

// LeaderGrantedNotice is the proposer's post-claim broadcast to peers
// in the membership: "I now hold (scope, key) at this fence until this
// expires." Fire-and-forget; peers mirror the row so their views stay
// in sync with the new leader. The mirror's WHERE clause refuses to
// overwrite a row with later expires or higher fence, so out-of-order
// notices converge on the most recent.
type LeaderGrantedNotice struct {
	Scope   string `cbor:"scope"`
	Key     string `cbor:"key"`
	Peer    string `cbor:"peer"`
	Fence   int64  `cbor:"fence"`
	Expires int64  `cbor:"expires"`
}

// replication_leader_membership returns the peers that vote on a
// (scope, key) claim. Self is excluded. Empty for single-host setups
// where there's nobody else to ask — the optimistic policy then
// auto-grants every claim.
//
// Scope prefix dispatch:
//   - "user:<uid>" → replication.db.hosts.peer where user=<uid>
//   - anything else → replication.db.pair.peer (whole-server pair)
func replication_leader_membership(scope string) []string {
	db := db_open("db/replication.db")
	var rows []map[string]any
	if strings.HasPrefix(scope, "user:") {
		uid := strings.TrimPrefix(scope, "user:")
		// The user's own host set PLUS the operator pair. A paired host serves every
		// user via the `pair` relationship, NOT the per-user `hosts` table, so a
		// user-scoped lease that consulted only `hosts` left the two pair members
		// invisible to each other: both claimed leadership and duplicated the work —
		// e.g. each pair member emitted the "replica offline" notification stamped
		// with its own now(), diverging the user's notifications.db (the #133 class,
		// seen live on yuzu/wasabi 2026-07-02). Union both so the pair partner is in
		// the membership and the lease actually serialises across it. A non-paired
		// per-user setup has an empty `pair`, so the union is a no-op there.
		rows, _ = db.rows(
			"select peer from hosts where user=? and peer != ? "+
				"union select peer from pair where peer != ?",
			uid, net_id, net_id)
	} else {
		rows, _ = db.rows("select peer from pair where peer != ?", net_id)
	}
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		if p, ok := r["peer"].(string); ok && p != "" {
			out = append(out, p)
		}
	}
	return out
}

// replication_leader_vote computes this host's vote on an inbound
// claim. Returns granted plus the current leader / fence / expires so
// the proposer can mirror on a denial.
//
// Voting rules:
//   - row vacant → grant
//   - row expired → grant
//   - row alive, held by proposer → grant (renewal)
//   - row alive, held by anyone else: grant iff the hash tie-break
//     picks the proposer over the current holder.
//
// Tie-break is hash-based: sha256(scope|key|peer) deciding, lowest
// hash wins. Replaces the lex-by-peer-id tie-break used in V2 so
// leadership distributes across peers based on the key instead of
// always favouring the lowest peer-id host - a peer with the
// lexicographically-smallest id was winning every leader election in
// V2, concentrating all the scheduled work on one host. Hash tie-break
// makes the winner-per-key essentially uniform across peers while
// staying deterministic for any given (scope, key, peer-set).
//
// Tie-break is intentionally applied even when the recipient itself
// holds the lease: simultaneous claims converge because each peer's
// vote on a proposer with a smaller hash is a grant.
func replication_leader_vote(scope, key, proposer string, proposed_expires int64) (granted bool, leader string, fence int64, expires int64) {
	db := db_open("db/replication.db")
	n := now()
	row, _ := db.row("select peer, fence, expires from leadership where scope=? and key=?", scope, key)
	if row == nil {
		return true, "", 0, 0
	}
	cur_peer, _ := row["peer"].(string)
	cur_fence, _ := row["fence"].(int64)
	cur_exp, _ := row["expires"].(int64)
	if cur_exp <= n {
		return true, cur_peer, cur_fence, cur_exp
	}
	if proposer == cur_peer {
		return true, cur_peer, cur_fence, cur_exp
	}
	if replication_leader_prefer(scope, key, proposer, cur_peer) {
		return true, cur_peer, cur_fence, cur_exp
	}
	return false, cur_peer, cur_fence, cur_exp
}

// replication_leader_prefer reports whether candidate `a` should win
// the tie-break over candidate `b` for the (scope, key) lease. Hash
// sha256(scope || "|" || key || "|" || peer) for each candidate and
// pick the smaller digest. Deterministic for any given input so all
// peers vote identically; uniform across the peerset so leadership
// distributes evenly when many keys are in play.
func replication_leader_prefer(scope, key, a, b string) bool {
	return bytes.Compare(replication_leader_hash(scope, key, a), replication_leader_hash(scope, key, b)) < 0
}

func replication_leader_hash(scope, key, peer string) []byte {
	digest := sha256.Sum256([]byte(scope + "|" + key + "|" + peer))
	return digest[:]
}

// replication_leader_claim_rpc is a stub-overridable function pointer
// so tests can drive vote scenarios without spinning up real peers.
var replication_leader_claim_rpc = replication_leader_claim_rpc_impl

// replication_leader_claim_rpc_impl opens a sync stream to peer and
// fetches that peer's vote on the proposer's claim. Returns nil on any
// transport or protocol error; the optimistic partition policy in the
// caller treats nil as "no veto" (count as unreachable, not as deny).
func replication_leader_claim_rpc_impl(peer, scope, key string, expires int64) *LeaderClaimResponse {
	s, err := stream_to_peer(peer, "", "", "replication", "replica/leader/claim", "", nil)
	if err != nil {
		return nil
	}
	defer s.close()
	if err := s.write(&LeaderClaimRequest{Scope: scope, Key: key, Expires: expires}); err != nil {
		return nil
	}
	var res LeaderClaimResponse
	if err := s.read(&res); err != nil {
		return nil
	}
	return &res
}

// replica_leader_claim_event is the inbound stream-RPC handler for a
// peer's leader-claim request. Reads the request, votes via
// replication_leader_vote, writes the response back on the same stream.
func replica_leader_claim_event(e *Event) {
	if e.stream == nil {
		info("Replica leader-claim: no stream — dropping")
		return
	}
	scope, _ := e.content["scope"].(string)
	key, _ := e.content["key"].(string)
	expires := event_int64(e.content["expires"])
	if scope == "" || key == "" || expires <= 0 {
		_ = e.stream.write(&LeaderClaimResponse{Granted: false})
		return
	}
	granted, leader, fence, exp := replication_leader_vote(scope, key, e.peer, expires)
	_ = e.stream.write(&LeaderClaimResponse{
		Granted:        granted,
		CurrentLeader:  leader,
		CurrentFence:   fence,
		CurrentExpires: exp,
	})
}

// replication_leader_notify is a stub-overridable function pointer for
// the post-claim fire-and-forget notification so tests can capture or
// suppress outbound side-effects.
var replication_leader_notify = replication_leader_notify_impl

// replication_leader_notify_impl sends a replica/leader/granted notice
// to every peer in the (scope, key) membership. Best-effort: send_peer
// queues if the connection is down and the receiver-side mirror's
// WHERE clause makes out-of-order arrivals converge on the latest
// fence/expires anyway.
func replication_leader_notify_impl(scope, key string, fence, expires int64) {
	for _, peer := range replication_leader_membership(scope) {
		if peer == "" || peer == net_id {
			continue
		}
		m := message("", "", "replication", "replica/leader/granted")
		m.content = map[string]any{
			"scope":   scope,
			"key":     key,
			"peer":    net_id,
			"fence":   fence,
			"expires": expires,
		}
		m.send_peer(peer)
	}
}

// replica_leader_granted_event is the inbound handler for a peer's
// post-claim notification. Mirrors the new lease in our local
// leadership row so the next mochi.schedule.leader call here goes
// straight to fast-path-deny without firing a redundant claim RPC.
//
// The WHERE clause refuses to overwrite a row with a later expires or
// higher fence — out-of-order notices (rare; same peer renewing twice
// during a network blip) converge on the most recent.
func replica_leader_granted_event(e *Event) {
	scope, _ := e.content["scope"].(string)
	key, _ := e.content["key"].(string)
	fence := event_int64(e.content["fence"])
	expires := event_int64(e.content["expires"])
	if scope == "" || key == "" || expires <= 0 {
		return
	}
	// Authorize: the notice must come from a member of this (scope) leadership
	// group — a pair member for server scopes, or a peer in the user's host set
	// for "user:<uid>" scopes — the same set replication_leader_notify emits to.
	// Store the transport-authenticated e.peer as the leader, NEVER the payload's
	// peer field: unauthorized, that field let any network peer install a
	// far-future / max-fence leader for any (scope,key), permanently fast-path-
	// denying all leader-gated scheduled work with no legitimate claim able to
	// supersede it (#145).
	if !replication_leader_is_member(scope, e.peer) {
		info("Replication leader-granted dropping: peer %q is not a member of leadership scope %q", e.peer, scope)
		return
	}
	db := db_open("db/replication.db")
	db.exec(`insert into leadership (scope, key, peer, expires, fence) values (?, ?, ?, ?, ?)
		on conflict(scope, key) do update set
			peer = excluded.peer,
			expires = excluded.expires,
			fence = excluded.fence
		where excluded.expires > leadership.expires or excluded.fence > leadership.fence`,
		scope, key, e.peer, expires, fence)
}

// replication_leader_is_member reports whether peer belongs to the (scope)
// leadership group — the same membership replication_leader_membership fans
// grants out to. "user:<uid>" scopes are gated on the user's host set; every
// other scope is the operator pair. Used to authorize an inbound
// replica/leader/granted notice.
func replication_leader_is_member(scope, peer string) bool {
	if peer == "" || peer == net_id {
		return false
	}
	if strings.HasPrefix(scope, "user:") {
		uid := strings.TrimPrefix(scope, "user:")
		db := db_open("db/replication.db")
		has, _ := db.exists("select 1 from hosts where user=? and peer=?", uid, peer)
		return has
	}
	return peer_is_pair(peer)
}

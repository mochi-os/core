package main

import "testing"

// TestReplicationEpochCurrentBump covers the sender's outbound generation store
// (#65): 0 until bumped, then now()-based and monotonic across bumps.
func TestReplicationEpochCurrentBump(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	if e := replication_epoch_current(); e != 0 {
		t.Fatalf("fresh epoch: want 0, got %d", e)
	}
	replication_epoch_bump()
	first := replication_epoch_current()
	if first <= 0 {
		t.Fatalf("after bump: want > 0, got %d", first)
	}
	// A second bump never goes backwards (max guard); equal within the same
	// now() second is fine.
	replication_epoch_bump()
	if second := replication_epoch_current(); second < first {
		t.Fatalf("second bump went backwards: %d < %d", second, first)
	}
}

// TestBootstrapReceiverComplete covers the bump point (#65): only once ALL of a
// peer's bootstrap scopes are 'done' does the receiver bump its own outbound
// generation and mark the source peer's epoch baseline pending.
func TestBootstrapReceiverComplete(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	rdb := db_open("db/replication.db")
	peer := "peerSrc"

	// One scope done, one still active → incomplete → no bump, no pending.
	rdb.exec("insert into bootstrap (scope, peer, state) values ('files', ?, ?)", peer, bootstrap_state_done)
	rdb.exec("insert into bootstrap (scope, peer, state) values ('userdbs', ?, 'active')", peer)
	bootstrap_receiver_complete(peer)
	if e := replication_epoch_current(); e != 0 {
		t.Fatalf("incomplete bootstrap must not bump epoch, got %d", e)
	}
	if p := rdb.integer("select coalesce(pending, 0) from peer_epoch where peer=?", peer); p != 0 {
		t.Fatalf("incomplete bootstrap must not set pending, got %d", p)
	}

	// Last scope done → complete. `sequence` is empty here (a wiped / reset
	// outbound space), so the gate bumps the epoch + sets the pending baseline.
	rdb.exec("update bootstrap set state=? where peer=? and scope='userdbs'", bootstrap_state_done, peer)
	bootstrap_receiver_complete(peer)
	if e := replication_epoch_current(); e <= 0 {
		t.Fatalf("complete bootstrap must bump epoch, got %d", e)
	}
	if p := rdb.integer("select coalesce(pending, 0) from peer_epoch where peer=?", peer); p != 1 {
		t.Fatalf("complete bootstrap must set source epoch pending, got %d", p)
	}
}

// TestBootstrapReceiverCompleteSkipsBumpWhenSequenceIntact covers the #33 fix:
// the epoch bump on a complete bootstrap is gated on the outbound sequence space
// actually having been wiped. A targeted / recovery bootstrap leaves the
// `sequence` counter intact, so the host keeps emitting HIGH continuing sequences
// — bumping the epoch there makes every peer delete its inbound cursors for us
// and then stall on the high ops it can't chain (cursor=0 with converged data,
// the forums stall). The source epoch baseline is still marked pending either way.
func TestBootstrapReceiverCompleteSkipsBumpWhenSequenceIntact(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	rdb := db_open("db/replication.db")
	peer := "peerSrc"

	// Our outbound sequence counter survived the bootstrap (high — NOT a reset).
	rdb.exec("insert into sequence (user, scope, next) values ('u1', ?, 14508)", repl_scope_app)

	rdb.exec("insert into bootstrap (scope, peer, state) values ('files', ?, ?)", peer, bootstrap_state_done)
	rdb.exec("insert into bootstrap (scope, peer, state) values ('userdbs', ?, ?)", peer, bootstrap_state_done)
	bootstrap_receiver_complete(peer)

	if e := replication_epoch_current(); e != 0 {
		t.Fatalf("intact sequence space must NOT bump epoch (a bump strands peers' cursors at 0, #33), got %d", e)
	}
	// The source epoch baseline is still marked pending regardless of the gate.
	if p := rdb.integer("select coalesce(pending, 0) from peer_epoch where peer=?", peer); p != 1 {
		t.Fatalf("source epoch pending must still be set, got %d", p)
	}
}

// TestReplicationEpochGate covers the receiver generation gate at the top of
// replication_op_receive (#65): higher epoch resets inbound state, lower epoch
// drops, and a pending baseline adopts the generation without resetting.
func TestReplicationEpochGate(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	rdb := db_open("db/replication.db")

	op := func(epoch, seq, prev int64) *ReplicationOp {
		return &ReplicationOp{Scope: repl_scope_app, User: "u1", Database: "epochtest",
			Table: "x", Operation: "noop", Sequence: seq, Prev: prev, Epoch: epoch}
	}

	// (a) Higher epoch than recorded → inbound reset: a stale `seen` row for the
	// peer is cleared and peer_epoch advances to the op's generation.
	peerA := "peerA"
	rdb.exec("insert into peer_epoch (peer, epoch, pending) values (?, 100, 0)", peerA)
	rdb.exec("insert into seen (peer, scope, user, sequence, applied) values (?, ?, 'u1', 999, 1)", peerA, repl_scope_app)
	replication_op_receive(peerA, op(200, 1, 0))
	if has, _ := rdb.exists("select 1 from seen where peer=? and sequence=999", peerA); has {
		t.Fatal("higher epoch: stale seen row should be cleared by the inbound reset")
	}
	if e := rdb.integer64("select epoch from peer_epoch where peer=?", peerA); e != 200 {
		t.Fatalf("higher epoch: peer_epoch should advance to 200, got %d", e)
	}

	// (b) Lower epoch than recorded → dropped: a stale `seen` row is PRESERVED
	// (no reset) and peer_epoch is unchanged.
	peerB := "peerB"
	rdb.exec("insert into peer_epoch (peer, epoch, pending) values (?, 200, 0)", peerB)
	rdb.exec("insert into seen (peer, scope, user, sequence, applied) values (?, ?, 'u1', 999, 1)", peerB, repl_scope_app)
	replication_op_receive(peerB, op(150, 5, 0))
	if has, _ := rdb.exists("select 1 from seen where peer=? and sequence=999", peerB); !has {
		t.Fatal("lower epoch: op must be dropped without resetting (seen row preserved)")
	}
	if e := rdb.integer64("select epoch from peer_epoch where peer=?", peerB); e != 200 {
		t.Fatalf("lower epoch: peer_epoch must be unchanged at 200, got %d", e)
	}

	// (c) Pending baseline → adopt without reset: a freshly-seeded cursor for the
	// peer is preserved and the epoch is adopted, marker cleared.
	peerC := "peerC"
	rdb.exec("insert into peer_epoch (peer, epoch, pending) values (?, 0, 1)", peerC)
	rdb.exec("insert into cursor (peer, scope, user, db, sequence) values (?, ?, 'u1', 'app:seed', 50)", peerC, repl_scope_app)
	replication_op_receive(peerC, op(150, 60, 0))
	if has, _ := rdb.exists("select 1 from cursor where peer=? and db='app:seed' and sequence=50", peerC); !has {
		t.Fatal("pending: freshly-seeded cursor must be preserved (no inbound reset)")
	}
	if e := rdb.integer64("select epoch from peer_epoch where peer=?", peerC); e != 150 {
		t.Fatalf("pending: epoch should be adopted as 150, got %d", e)
	}
	if p := rdb.integer("select pending from peer_epoch where peer=?", peerC); p != 0 {
		t.Fatalf("pending: marker should clear after adoption, got %d", p)
	}
}

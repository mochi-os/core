// Tests for schedule replication to a fresh pair replica (#32).
// Copyright Alistair Cunningham 2026

package main

import (
	"testing"
	"time"

	cbor "github.com/fxamacker/cbor/v2"
)

// TestReplicationOpSelfAnchoring: schedule ops are self-anchoring (emitted
// Prev=0 so a fresh replica applies them without an anchor); other streams
// chain normally.
func TestReplicationOpSelfAnchoring(t *testing.T) {
	if !replication_op_self_anchoring(&ReplicationOp{Database: "schedule"}) {
		t.Error("schedule must be self-anchoring")
	}
	for _, db := range []string{"feeds", "chat", "users", "sessions"} {
		if replication_op_self_anchoring(&ReplicationOp{Database: db}) {
			t.Errorf("%q must NOT be self-anchoring (only schedule is)", db)
		}
	}
}

// TestReplicationPairBackfillSchedule: the pair-backfill re-emits every
// non-system schedule row to the joining peer (so a fresh replica gets the
// existing scheduled events), skipping host-local system events (user="").
func TestReplicationPairBackfillSchedule(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	sdb := schedule_db()
	sdb.exec("create table if not exists schedule (id integer primary key, user text not null, app text not null, due int not null, event text not null, data text not null, interval int not null, created int not null)")
	sdb.exec("insert into schedule (user, app, due, event, data, interval, created) values ('u1', 'feeds', 100, 'poll', '{}', 3600, 10)")
	sdb.exec("insert into schedule (user, app, due, event, data, interval, created) values ('u2', 'crm', 200, 'remind', '', 0, 20)")
	sdb.exec("insert into schedule (user, app, due, event, data, interval, created) values ('', 'core', 300, 'gc', '', 600, 30)") // system: must be skipped

	type emitted struct {
		user, db, op string
		peers        []string
		row          ScheduleRow
	}
	var got []emitted
	orig := replication_emit_to
	replication_emit_to = func(user string, op *ReplicationOp, peers []string) {
		var r ScheduleRow
		_ = cbor.Unmarshal(op.Payload, &r)
		got = append(got, emitted{user: user, db: op.Database, op: op.Operation, peers: peers, row: r})
	}
	defer func() { replication_emit_to = orig }()

	replication_pair_backfill_schedule("peer-X")

	if len(got) != 2 {
		t.Fatalf("backfill emitted %d ops, want 2 (system event must be skipped)", len(got))
	}
	for _, e := range got {
		if e.db != "schedule" || e.op != "schedule-row.set" {
			t.Errorf("unexpected op: db=%q op=%q", e.db, e.op)
		}
		if len(e.peers) != 1 || e.peers[0] != "peer-X" {
			t.Errorf("op must target only the joining peer; peers=%v", e.peers)
		}
		if e.row.Key["user"] != e.user || e.row.Key["app"] == "" || e.row.Key["event"] == "" || e.row.Key["created"] == "" {
			t.Errorf("malformed schedule row key: %+v", e.row.Key)
		}
	}
	// The system event (user="") must not be among them.
	for _, e := range got {
		if e.user == "" {
			t.Error("a host-local system event (user=\"\") was replicated; it must be skipped")
		}
	}
}

// TestReplicationPairBackfillUsersSeedsEntityLess is the #34 emit half: in the
// pair-backfill, a user whose keys-transfer is skipped (no signing entity) must
// still have its bare row seeded to the peer via the system-row path, while an
// entity-bearing user goes through keys-transfer and is NOT seeded.
func TestReplicationPairBackfillUsersSeedsEntityLess(t *testing.T) {
	cleanup := setup_system_replication_test(t)
	defer cleanup()
	setup_users_test_schema()

	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username, role) values ('u-ent', 'has@x', 'user')")
	udb.exec("insert into users (uid, username, role) values ('u-bare', 'bare@x', 'user')")

	// u-ent has an entity (keys-transfer succeeds); u-bare doesn't (skipped).
	orig_keys := replication_transfer_keys_var
	replication_transfer_keys_var = func(uid, peer string) bool { return uid == "u-ent" }
	defer func() { replication_transfer_keys_var = orig_keys }()

	type seed struct {
		peer, db, table string
		key, cols       map[string]string
	}
	var seeds []seed
	orig_row := replication_system_row_to_peer_var
	replication_system_row_to_peer_var = func(peer, db, table string, key, cols map[string]string, del bool) {
		seeds = append(seeds, seed{peer, db, table, key, cols})
	}
	defer func() { replication_system_row_to_peer_var = orig_row }()

	replication_pair_backfill_users("peer-X")

	if len(seeds) != 1 {
		t.Fatalf("seeded %d rows, want exactly 1 (only the entity-less user)", len(seeds))
	}
	s := seeds[0]
	if s.peer != "peer-X" || s.db != "users" || s.table != "users" {
		t.Errorf("seed target = %s %s.%s, want peer-X users.users", s.peer, s.db, s.table)
	}
	if s.key["uid"] != "u-bare" {
		t.Errorf("seeded uid = %q, want u-bare", s.key["uid"])
	}
	if s.cols["username"] != "bare@x" || s.cols["role"] != "user" {
		t.Errorf("seed cols = %v, want username=bare@x role=user", s.cols)
	}
	if _, has := s.cols["status"]; has {
		t.Error("seed must NOT carry status (it relies on the INSERT default)")
	}
}

// TestReplicationBootstrapReconcileOnComplete: the source re-runs the full
// pair-backfill to a pair peer only once its bulk bootstrap is fully acked
// (no bootstrap_served rows) — never while scopes are still being served, and
// never to a non-pair peer.
func TestReplicationBootstrapReconcileOnComplete(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	rdb := db_open("db/replication.db")

	calls := make(chan string, 4)
	orig := replication_pair_backfill
	replication_pair_backfill = func(peer string) { calls <- peer }
	defer func() { replication_pair_backfill = orig }()

	fired := func() bool {
		select {
		case <-calls:
			return true
		case <-time.After(200 * time.Millisecond):
			return false
		}
	}

	// 1. Bulk bootstrap still in progress (a served scope remains) → no reconcile.
	rdb.exec("insert into pair (peer, added) values ('p1', 1)")
	rdb.exec("insert into bootstrap_served (peer, scope, started) values ('p1', 'files', 1)")
	replication_bootstrap_reconcile_on_complete("p1")
	if fired() {
		t.Error("must NOT reconcile while a scope is still served")
	}

	// 2. All scopes acked but NOT a pair member → no reconcile.
	replication_bootstrap_reconcile_on_complete("p2") // p2: no bootstrap_served, not paired
	if fired() {
		t.Error("must NOT reconcile a non-pair peer")
	}

	// 3. Pair member, all scopes acked (no served rows) → full backfill fires.
	rdb.exec("delete from bootstrap_served where peer='p1'")
	replication_bootstrap_reconcile_on_complete("p1")
	select {
	case got := <-calls:
		if got != "p1" {
			t.Errorf("reconcile backfilled %q, want p1", got)
		}
	case <-time.After(2 * time.Second):
		t.Error("must reconcile (full backfill) once a pair peer's bulk bootstrap is fully acked")
	}
}

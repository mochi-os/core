// Tests for schedule replication to a fresh pair replica (#32).
// Copyright Alistair Cunningham 2026

package main

import (
	"testing"

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

package main

import "testing"

// #163: journal_prune must not drop a shipped op a still-paired peer hasn't
// confirmed. An op at/below every recipient's delivery cursor is delivered
// everywhere (prune it); one above a recipient's cursor is still owed and must be
// RETAINED so the reconnect/periodic backfill can re-ship it — pruning it would
// strand that peer with an unfillable gap.
func TestJournalPruneRetainsUnconfirmed(t *testing.T) {
	defer journal_test_dir(t, "u1", "testapp")()
	db := db_open("users/u1/testapp/db/data.db")
	if db == nil {
		t.Fatal("db_open returned nil")
	}
	db.journal_setup()
	rdb := db_open("db/replication.db")

	origAge, origMin := journal_retention_age, journal_retention_minimum
	journal_retention_age = 100 // both ops below are older than this
	journal_retention_minimum = 0
	defer func() { journal_retention_age, journal_retention_minimum = origAge, origMin }()

	// A current recipient (recipients() reads the hosts table) that has confirmed
	// up to sequence 3 on stream "s".
	rdb.exec("create table if not exists hosts (user text not null, peer text not null, primary key (user, peer))")
	rdb.exec("insert into hosts (user, peer) values ('u1','peerP')")
	rdb.exec("insert into journal_delivery (user, peer, stream, sequence) values ('u1','peerP','s',3)")

	old := now() - 1000 // older than retention, but within the 30d hard cap
	ins := func(id string, seq int64) {
		db.exec("insert into journal (id, operation, statement, args, target, uid, schema, created, state) values (?,?,?,?,?,?,?,?,'shipped')",
			id, repl_op_exec, "insert into items (id) values (1)", cbor_encode([]any{}), "items", "", 0, old)
		rdb.exec("insert into journal_sequence (id, user, scope, stream, sequence, prev) values (?, 'u1','app','s',?,0)", id, seq)
	}
	ins("conf", 2)   // <= delivery cursor 3 → every recipient has it → safe to prune
	ins("unconf", 5) // >  delivery cursor 3 → peerP hasn't confirmed → must be kept

	journal_prune(db, "u1")

	if n := db.integer("select count(*) from journal where id='conf'"); n != 0 {
		t.Fatalf("confirmed old op should be pruned (count=%d)", n)
	}
	if n := db.integer("select count(*) from journal where id='unconf'"); n != 1 {
		t.Fatalf("unconfirmed old op must be RETAINED for backfill (#163), got count=%d", n)
	}
}

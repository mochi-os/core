// Mochi server: additional tests for per-app SQL command replication
// Copyright Alistair Cunningham 2026
//
// Covers correctness invariants that aren't exercised by the basic
// apply tests: loop prevention, idempotent replay, receiver-side SQL
// failure handling, mixed Args types through CBOR, schema deferral,
// and the prefix parser's edge cases.

package main

import (
	"testing"

	cbor "github.com/fxamacker/cbor/v2"
)

// TestReplicationApplySQLCommandDoesNotReEmit locks the no-loop
// invariant: when a SQL exec op is applied on the receiver, the apply
// path must not call replication_emit_sql_command on the way through.
// If it did, two-host replication would ping-pong forever.
//
// Probe: replication_emit increments a per-(user, scope) sequence row
// in replication.db.sequence as its first side effect. If apply
// re-emitted, that row would exist with next>=1. We assert it doesn't.
func TestReplicationApplySQLCommandDoesNotReEmit(t *testing.T) {
	cleanup, user_uid, app_id := setup_sql_replication_test(t)
	defer cleanup()
	db_upgrade_50() // creates replication.db.sequence

	op := &ReplicationOp{
		Scope:     repl_scope_app,
		User:      user_uid,
		Database:  app_id,
		Operation: repl_op_exec,
		Schema:    1,
		Payload: cbor_encode(&SQLCommand{
			Statement: "insert into posts (id, title) values (?, ?)",
			Args:      []any{"loop-1", "Hello"},
		}),
	}
	if got := replication_apply_op(op); got != ApplyApplied {
		t.Fatalf("apply: want ApplyApplied, got %v", got)
	}

	repl := db_open("db/replication.db")
	row, _ := repl.row("select next from sequence where user=? and scope=?", user_uid, repl_scope_app)
	if row != nil {
		if next, _ := row["next"].(int64); next > 0 {
			t.Errorf("apply re-emitted: replication.db.sequence row for user=%q scope=%q advanced to %d (expected 0/absent)", user_uid, repl_scope_app, next)
		}
	}
}

// TestReplicationApplySQLCommandIdempotentReplay re-applies the same
// op and verifies the receiver doesn't blow up. INSERT replay produces
// a PK uniqueness violation which the apply path logs and treats as
// ApplyApplied (so the deduper doesn't keep retrying forever); the
// row state matches what one apply would have produced.
func TestReplicationApplySQLCommandIdempotentReplay(t *testing.T) {
	cleanup, user_uid, app_id := setup_sql_replication_test(t)
	defer cleanup()

	op := &ReplicationOp{
		Scope: repl_scope_app, User: user_uid,
		Database: app_id, Operation: repl_op_exec, Schema: 1,
		Payload: cbor_encode(&SQLCommand{
			Statement: "insert into posts (id, title) values (?, ?)",
			Args:      []any{"idem", "Once"},
		}),
	}
	for i := 0; i < 3; i++ {
		if got := replication_apply_op(op); got != ApplyApplied {
			t.Fatalf("apply #%d: want ApplyApplied, got %v", i, got)
		}
	}

	u := &User{UID: user_uid}
	a := app_by_id(app_id)
	db := db_app(u, a)
	count := db.integer("select count(*) from posts where id='idem'")
	if count != 1 {
		t.Errorf("replay must be idempotent; row count = %d, want 1", count)
	}
	title, _ := db.row("select title from posts where id='idem'")
	if v, _ := title["title"].(string); v != "Once" {
		t.Errorf("title: want 'Once', got %q", v)
	}
}

// TestReplicationApplySQLCommandReceiverFailureLogged exercises the
// schema-drift path: a receiver missing a column referenced by the
// op's SQL. The apply must not panic; it logs and returns ApplyApplied
// so the deduper marks it seen and doesn't re-deliver.
func TestReplicationApplySQLCommandReceiverFailureLogged(t *testing.T) {
	cleanup, user_uid, app_id := setup_sql_replication_test(t)
	defer cleanup()

	op := &ReplicationOp{
		Scope: repl_scope_app, User: user_uid,
		Database: app_id, Operation: repl_op_exec, Schema: 1,
		Payload: cbor_encode(&SQLCommand{
			Statement: "insert into posts (id, title, missing) values (?, ?, ?)",
			Args:      []any{"bad", "X", "Y"},
		}),
	}
	if got := replication_apply_op(op); got != ApplyApplied {
		t.Fatalf("receiver-failure: want ApplyApplied (logged), got %v", got)
	}
}

// TestReplicationSQLCommandMixedArgTypesRoundTrip exercises the CBOR
// encode→decode→exec path with the parameter types apps actually pass:
// strings, integers, []byte (blob), and nil. The receiver's SQL
// driver must accept whatever Go types CBOR produces on the other side.
//
// CBOR's `any` decode returns positive ints as uint64; the SQL driver
// accepts both, so the wire format normalising to uint64 is fine. The
// test checks stored values, not the intermediate Go types.
func TestReplicationSQLCommandMixedArgTypesRoundTrip(t *testing.T) {
	cleanup, user_uid, app_id := setup_sql_replication_test(t)
	defer cleanup()

	u := &User{UID: user_uid}
	a := app_by_id(app_id)
	db := db_app(u, a)
	db.exec("create table mixed (id text primary key, n integer, blob blob, opt text)")

	original := &SQLCommand{
		Statement: "insert into mixed (id, n, blob, opt) values (?, ?, ?, ?)",
		Args:      []any{"m1", int64(42), []byte{0x01, 0x02, 0x03}, nil},
	}
	payload := cbor_encode(original)

	// Probe the decoded Args shape before the apply runs, so we can
	// pinpoint where any type confusion happens.
	var decoded SQLCommand
	if err := cbor.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(decoded.Args) != 4 {
		t.Fatalf("args len: want 4, got %d", len(decoded.Args))
	}
	t.Logf("decoded arg types: %T %T %T %T", decoded.Args[0], decoded.Args[1], decoded.Args[2], decoded.Args[3])

	op := &ReplicationOp{
		Scope: repl_scope_app, User: user_uid,
		Database: app_id, Operation: repl_op_exec, Schema: 1,
		Payload: payload,
	}
	if got := replication_apply_op(op); got != ApplyApplied {
		t.Fatalf("apply: want ApplyApplied, got %v", got)
	}

	// DB.row() helpfully converts []byte to string for app code, so we
	// can't .([]byte) the blob column directly. Use length + hex to
	// verify the stored bytes are correct.
	row, _ := db.row("select n, length(blob) as blen, hex(blob) as bhex, opt from mixed where id='m1'")
	if row == nil {
		t.Fatal("row missing after apply")
	}
	if n, _ := row["n"].(int64); n != 42 {
		t.Errorf("integer column: want 42, got %d (raw %v)", n, row["n"])
	}
	if blen, _ := row["blen"].(int64); blen != 3 {
		t.Errorf("blob length: want 3, got %d", blen)
	}
	if bhex, _ := row["bhex"].(string); bhex != "010203" {
		t.Errorf("blob hex: want 010203, got %q", bhex)
	}
	if v := row["opt"]; v != nil {
		t.Errorf("nil arg: want nil, got %v (%T)", v, v)
	}
}

// TestReplicationSQLCommandNoParamsStatement covers a statement that
// uses no bound parameters at all (e.g. a bulk delete).
func TestReplicationSQLCommandNoParamsStatement(t *testing.T) {
	cleanup, user_uid, app_id := setup_sql_replication_test(t)
	defer cleanup()

	u := &User{UID: user_uid}
	a := app_by_id(app_id)
	db := db_app(u, a)
	db.exec("insert into posts (id, title) values ('a', 'A')")
	db.exec("insert into posts (id, title) values ('b', 'B')")

	op := &ReplicationOp{
		Scope: repl_scope_app, User: user_uid,
		Database: app_id, Operation: repl_op_exec, Schema: 1,
		Payload: cbor_encode(&SQLCommand{Statement: "delete from posts"}),
	}
	if got := replication_apply_op(op); got != ApplyApplied {
		t.Fatalf("apply: want ApplyApplied, got %v", got)
	}
	if n := db.integer("select count(*) from posts"); n != 0 {
		t.Errorf("post-delete count: want 0, got %d", n)
	}
}

// TestReplicationSQLCommandSchemaDefer exercises the cross-host schema
// gate: a sender at schema v3 cannot apply on a receiver still at v1.
// The op must defer, not error out.
func TestReplicationSQLCommandSchemaDefer(t *testing.T) {
	cleanup, user_uid, app_id := setup_sql_replication_test(t)
	defer cleanup()

	op := &ReplicationOp{
		Scope: repl_scope_app, User: user_uid,
		Database: app_id, Operation: repl_op_exec, Schema: 99,
		Payload: cbor_encode(&SQLCommand{
			Statement: "insert into posts (id, title) values (?, ?)",
			Args:      []any{"future", "From v99"},
		}),
	}
	if got := replication_apply_op(op); got != ApplyDeferred {
		t.Errorf("op carrying higher sender schema: want ApplyDeferred, got %v", got)
	}
}

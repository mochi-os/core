// Mochi server: replication stream-key class-qualification tests.
//
// Guards the invariant behind the core/app/system stream-key collision fix:
// repl_op_stream (live ops) and bootstrap_stream_key (bootstrapped files)
// must produce the SAME class-qualified key for the same logical stream, and
// a dev app named after a reserved core/system stream must never share a key
// with it. This replaces a runtime app-id guardrail that would have wrongly
// rejected the (now-safe) notifications app.
//
// Copyright Alistair Cunningham 2026

package main

import "testing"

// TestStreamKeyOpAndBootstrapAgree: for every stream class, the live-op key
// (repl_op_stream) and the bootstrap-file key (bootstrap_stream_key) match.
// A mismatch means a bootstrapped DB seeds a cursor the gate never reads, so
// the stream stalls forever — the exact failure this scheme prevents.
func TestStreamKeyOpAndBootstrapAgree(t *testing.T) {
	cases := []struct {
		name string
		op   *ReplicationOp
		path string
		want string
	}{
		{
			name: "app data DB",
			op:   &ReplicationOp{Operation: repl_op_exec, Database: "feeds"},
			path: "users/u1/feeds/db/feeds.db",
			want: "app:feeds",
		},
		{
			name: "app system DB",
			op:   &ReplicationOp{Operation: repl_op_exec_app_system, Database: "feeds"},
			path: "users/u1/feeds/app.db",
			want: "app:feeds/system",
		},
		{
			name: "core user infra DB",
			op:   &ReplicationOp{Operation: repl_op_exec_user_core, Database: repl_db_user_core_sentinel},
			path: "users/u1/user.db",
			want: "core:user",
		},
		{
			name: "core notifications infra DB",
			op:   &ReplicationOp{Operation: repl_op_insert, Database: "notifications", Table: "webpush_delivered"},
			path: "users/u1/notifications.db",
			want: "core:notifications",
		},
	}
	for _, c := range cases {
		if got := repl_op_stream(c.op); got != c.want {
			t.Errorf("%s: repl_op_stream = %q, want %q", c.name, got, c.want)
		}
		if got := bootstrap_stream_key(c.path); got != c.want {
			t.Errorf("%s: bootstrap_stream_key(%q) = %q, want %q", c.name, c.path, got, c.want)
		}
	}
}

// TestStreamKeySystemRow: system-row streams (no per-user file, never
// bootstrapped) take the system class. bootstrap_stream_key never produces
// these, so only the op side is checked.
func TestStreamKeySystemRow(t *testing.T) {
	for _, db := range []string{"users", "sessions", "schedule"} {
		op := &ReplicationOp{Operation: "insert", Database: db, Table: db}
		want := "system:" + db
		if got := repl_op_stream(op); got != want {
			t.Errorf("system-row %q: repl_op_stream = %q, want %q", db, got, want)
		}
	}
}

// TestStreamKeyNotificationsCollisionResolved: the bug. A dev app named
// "notifications" (its data DB) and the core per-user notifications.db must
// resolve to DIFFERENT stream keys, so they no longer share one cursor +
// Prev-chain (which caused silent op loss during bootstrap re-anchoring).
func TestStreamKeyNotificationsCollisionResolved(t *testing.T) {
	appData := repl_op_stream(&ReplicationOp{Operation: repl_op_exec, Database: "notifications"})
	coreInfra := repl_op_stream(&ReplicationOp{Operation: repl_op_insert, Database: "notifications", Table: "webpush_delivered"})
	if appData == coreInfra {
		t.Fatalf("collision: app-data and core notifications both key to %q", appData)
	}
	if appData != "app:notifications" || coreInfra != "core:notifications" {
		t.Errorf("got app=%q core=%q, want app:notifications / core:notifications", appData, coreInfra)
	}

	// Same split on the bootstrap side: the app's data DB vs the infra DB.
	appPath := bootstrap_stream_key("users/u1/notifications/db/notifications.db")
	infraPath := bootstrap_stream_key("users/u1/notifications.db")
	if appPath == infraPath {
		t.Fatalf("bootstrap collision: both key to %q", appPath)
	}
	if appPath != "app:notifications" || infraPath != "core:notifications" {
		t.Errorf("bootstrap got app=%q infra=%q, want app:notifications / core:notifications", appPath, infraPath)
	}
}

// TestStreamMigrateKey: the db_upgrade_85 re-key reproduces the class keys
// from a legacy bare key, and is idempotent (already-qualified keys are left
// alone by the migration's instr(db,':')=0 filter, but the mapper must also
// not mangle them if ever called).
func TestStreamMigrateKey(t *testing.T) {
	cases := map[string]string{
		"feeds":         "app:feeds",       // app data
		"feeds/app":     "app:feeds/system", // legacy app-system suffix
		"user":          "core:user",
		"notifications": "core:notifications",
		"users":         "system:users",
		"sessions":      "system:sessions",
		"schedule":      "system:schedule",
	}
	for old, want := range cases {
		if got := repl_stream_migrate_key(old); got != want {
			t.Errorf("repl_stream_migrate_key(%q) = %q, want %q", old, got, want)
		}
	}
}

// TestUpgrade85RekeysStreams: db_upgrade_85 re-keys legacy bare stream
// identifiers in cursor/tail/pending to the class-qualified scheme,
// preserving the sequence/last watermark (so anchored streams stay
// anchored), and is idempotent.
func TestUpgrade85RekeysStreams(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db := db_open("db/replication.db")

	db.exec("insert into cursor (peer, scope, user, db, sequence) values ('p','app','u','feeds',5)")
	db.exec("insert into cursor (peer, scope, user, db, sequence) values ('p','app','u','feeds/app',3)")
	db.exec("insert into cursor (peer, scope, user, db, sequence) values ('p','app','u','notifications',7)")
	db.exec("insert into cursor (peer, scope, user, db, sequence) values ('p','app','u','sessions',9)")
	db.exec("insert into tail (user, scope, db, last) values ('u','app','feeds',5)")
	db.exec("insert into pending (peer, scope, user, db, sequence, prev, schema, payload, received) values ('p','app','u','users',2,1,0,?,?)", []byte{0}, now())

	db_upgrade_85()

	want := map[string]int{"app:feeds": 5, "app:feeds/system": 3, "core:notifications": 7, "system:sessions": 9}
	for k, v := range want {
		if got := db.integer("select sequence from cursor where db=?", k); got != v {
			t.Errorf("cursor %q sequence = %d, want %d", k, got, v)
		}
	}
	if n := db.integer("select count(*) from cursor where instr(db, ':') = 0"); n != 0 {
		t.Errorf("%d legacy bare cursor keys remain", n)
	}
	if n := db.integer("select count(*) from tail where db='app:feeds'"); n != 1 {
		t.Error("tail not re-keyed to app:feeds")
	}
	if n := db.integer("select count(*) from pending where db='system:users'"); n != 1 {
		t.Error("pending not re-keyed to system:users")
	}

	// Idempotent: a second run must not double-prefix.
	db_upgrade_85()
	if n := db.integer("select count(*) from cursor where db='app:app:feeds'"); n != 0 {
		t.Error("db_upgrade_85 is not idempotent (double-prefixed)")
	}
}

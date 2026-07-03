package main

import (
	"os"
	"path/filepath"
	"testing"
)

// #190: a system: per-user stream (users/sessions/schedule) is a liveness marker
// over a shared sysdb with no per-user file to reseed and which the per-stream
// hash can't cover — so a frozen one used to alert forever. It must instead be
// re-anchored to the peer's tail (no alert), leaving real sysdb divergence to the
// separate core-DB audit.
func TestReplicationAuditLivenessSystemStreamReanchor(t *testing.T) {
	orig := data_dir
	data_dir = t.TempDir()
	defer func() {
		data_dir = orig
		audit_convergence_mutex.Lock()
		audit_cursor_previous = map[string]int64{}
		audit_liveness_alerted = map[string]bool{}
		audit_convergence_mutex.Unlock()
	}()
	os.MkdirAll(filepath.Join(data_dir, "db"), 0o755)
	rdb := db_open("db/replication.db")
	rdb.exec("create table if not exists cursor (peer text not null, scope text not null, user text not null default '', db text not null default '', sequence integer not null default 0, primary key (peer, scope, user, db))")

	peer, stream := "peerP", "system:users"
	rdb.exec("insert into cursor (peer, scope, user, db, sequence) values (?, ?, 'u1', ?, 100)", peer, repl_scope_app, stream)
	manifest := []AuditStream{{User: "u1", Stream: stream, Tail: 200}}

	audit_convergence_mutex.Lock()
	audit_cursor_previous = map[string]int64{}
	audit_critical_cursor_previous = map[string]int64{}
	audit_liveness_alerted = map[string]bool{}
	audit_convergence_mutex.Unlock()

	// system:users is an auth-critical stream, so it is processed in the
	// critical=true pass. Two rounds frozen below tail: a content stream would
	// alert; a system: stream must not — it is re-anchored to the tail.
	replication_audit_liveness(peer, nil, manifest, true)
	replication_audit_liveness(peer, nil, manifest, true)

	audit_convergence_mutex.Lock()
	alerted := audit_liveness_alerted[peer+"|u1|"+stream]
	audit_convergence_mutex.Unlock()
	if alerted {
		t.Fatal("system: liveness stream must not alert — it is re-anchored, not treated as stuck")
	}
	if cur, _ := replication_cursor(rdb, peer, repl_scope_app, "u1", stream); cur != 200 {
		t.Fatalf("system: cursor should be re-anchored to the tail (200), got %d", cur)
	}
}

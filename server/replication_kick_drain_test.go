package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// #161: buffering an op schedules a debounced near-term re-drain of just that
// stream (so a concurrently-buffered follower applies in ~seconds, not up to 30s),
// and repeated kicks for the same stream coalesce to one in-flight goroutine.
func TestReplicationStreamKickDrainDebounces(t *testing.T) {
	orig := data_dir
	data_dir = t.TempDir()
	os.MkdirAll(filepath.Join(data_dir, "db"), 0o755)
	rdb := db_open("db/replication.db")
	rdb.exec("create table if not exists pending (peer text, scope text, user text, db text, sequence integer, prev integer, schema integer, payload blob, received integer, primary key (peer, scope, user, sequence))")
	rdb.exec("create table if not exists cursor (peer text not null, scope text not null, user text not null default '', db text not null default '', sequence integer not null default 0, primary key (peer, scope, user, db))")

	origDelay := pending_kick_drain_delay
	pending_kick_drain_delay = 20 * time.Millisecond
	pending_kick_mu.Lock()
	pending_kick_scheduled = map[string]bool{}
	pending_kick_mu.Unlock()
	defer func() {
		data_dir = orig
		pending_kick_drain_delay = origDelay
	}()

	key := "peerP|" + repl_scope_app + "|u1|app:x"

	// Five rapid kicks for the same stream schedule exactly ONE goroutine.
	for i := 0; i < 5; i++ {
		replication_stream_kick_drain("peerP", repl_scope_app, "u1", "app:x")
	}
	pending_kick_mu.Lock()
	scheduled := pending_kick_scheduled[key]
	pending_kick_mu.Unlock()
	if !scheduled {
		t.Fatal("the first kick must schedule a re-drain")
	}

	// After the delay the goroutine runs the drain and clears the flag, so a later
	// buffer can schedule a fresh kick (not stuck permanently marked).
	time.Sleep(80 * time.Millisecond)
	pending_kick_mu.Lock()
	stillScheduled := pending_kick_scheduled[key]
	pending_kick_mu.Unlock()
	if stillScheduled {
		t.Fatal("kick flag should clear after the delayed drain ran (else a stream kicks only once ever)")
	}
}

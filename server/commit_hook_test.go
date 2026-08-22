// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	sl "go.starlark.net/starlark"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestCommitsTrim (#44): the fired-row trim deletes fired rows past
// commits_log_age, keeps recent fired rows, and never touches unfired (pending)
// rows regardless of age — so a stuck handler's retries are preserved.
func TestCommitsTrim(t *testing.T) {
	orig := data_dir
	data_dir = t.TempDir()
	defer func() { data_dir = orig }()
	if err := os.MkdirAll(filepath.Join(data_dir, "db"), 0o755); err != nil {
		t.Fatal(err)
	}
	db := db_open("db/test.db")
	commits_table_create(db)
	old := now() - commits_log_age - 100
	recent := now() - 10
	db.exec("insert into commits (name, kind, row_uid, ts, fired) values ('t','insert','r1',?,1)", old)    // old fired -> trimmed
	db.exec("insert into commits (name, kind, row_uid, ts, fired) values ('t','insert','r2',?,1)", recent) // recent fired -> kept
	db.exec("insert into commits (name, kind, row_uid, ts, fired) values ('t','insert','r3',?,0)", old)    // old UNfired -> kept (pending)

	commits_trim(db)

	if n := db.integer("select count(*) from commits"); n != 2 {
		t.Fatalf("after trim want 2 rows (recent-fired + old-unfired), got %d", n)
	}
	if db.integer("select count(*) from commits where row_uid='r1'") != 0 {
		t.Error("old fired row should be trimmed")
	}
	if db.integer("select count(*) from commits where row_uid='r2'") != 1 {
		t.Error("recent fired row should be kept")
	}
	if db.integer("select count(*) from commits where row_uid='r3'") != 1 {
		t.Error("unfired (pending) row should never be trimmed regardless of age")
	}
}

// TestCommitFireRefusesPastTheDepthCap. A commit handler runs on a fresh
// Starlark thread with app and user set, so it can call mochi.db.commit.fire
// again. Each level keeps its Starlark slot for the whole descent, so an
// unguarded chain pins the engine for every user on the host.
func TestCommitFireRefusesPastTheDepthCap(t *testing.T) {
	user := &User{UID: "u1", Username: "user1@example.com"}
	app := create_external_app("looper")
	fn := sl.NewBuiltin("mochi.db.commit.fire", nil)
	args := []sl.Tuple{
		{sl.String("table"), sl.String("posts")},
		{sl.String("kind"), sl.String("insert")},
		{sl.String("row_uid"), sl.String("abc")},
	}

	at := func(depth any) error {
		thread := create_test_thread(user, app)
		if depth != nil {
			thread.SetLocal("depth", depth)
		}
		_, err := api_commit_fire(thread, fn, nil, args)
		return err
	}

	// At and below the cap the call proceeds. The app is not installed here, so
	// commit_hook_fire resolves nothing and returns - the point is that it is
	// not the DEPTH that stopped it.
	for _, depth := range []any{nil, 1, commit_hook_depth_maximum} {
		if err := at(depth); err != nil {
			t.Errorf("depth %v was refused: %v", depth, err)
		}
	}

	// Past it, refused.
	err := at(commit_hook_depth_maximum + 1)
	if err == nil {
		t.Fatal("a call past the depth cap was allowed")
	}
	if !strings.Contains(err.Error(), "maximum commit hook depth") {
		t.Errorf("refused with %q, want the depth error", err)
	}
}

// The cap must stay below the Starlark concurrency default: every nesting level
// holds a slot, so a cap at or above it empties the pool before the guard
// fires.
func TestCommitDepthCapProtectsTheSlotPool(t *testing.T) {
	if commit_hook_depth_maximum >= 32 {
		t.Errorf("commit_hook_depth_maximum is %d, at or above the default slot count of 32 - the guard cannot prevent slot exhaustion",
			commit_hook_depth_maximum)
	}
	source, err := os.ReadFile("starlark.go")
	if err != nil {
		t.Fatalf("read starlark.go: %v", err)
	}
	if !strings.Contains(string(source), `ini_int("starlark", "concurrency", 32)`) {
		t.Error("the Starlark concurrency default moved; recheck commit_hook_depth_maximum against it")
	}
}

// The depth must be written onto the thread commit_hook_invoke creates, or
// every level reads 1 and the cap never binds.
func TestCommitDepthReachesTheNestedThread(t *testing.T) {
	source, err := os.ReadFile("commit_hook.go")
	if err != nil {
		t.Fatalf("read commit_hook.go: %v", err)
	}
	text := string(source)

	invoke := text[strings.Index(text, "func commit_hook_invoke("):]
	invoke = invoke[:strings.Index(invoke, "\n}")]
	if !strings.Contains(invoke, `s.set("depth", depth+1)`) {
		t.Error("commit_hook_invoke does not put the depth on the handler's thread, so nesting always reads 1")
	}

	fire := text[strings.Index(text, "func api_commit_fire("):]
	fire = fire[:strings.Index(fire, "\n}")]
	if !strings.Contains(fire, `t.Local("depth")`) {
		t.Error("api_commit_fire does not read the inherited depth")
	}
	if !strings.Contains(fire, "commit_hook_depth_maximum") {
		t.Error("api_commit_fire does not check the depth cap")
	}

	// Every invoke site must carry it; one that does not is a hole.
	if n := strings.Count(text, "commit_hook_invoke(av, a, u, function, table, kind, row_uid, depth)"); n != 2 {
		t.Errorf("%d of the commit_hook_invoke calls pass depth, want both (the direct fire and the drain retry)", n)
	}
}

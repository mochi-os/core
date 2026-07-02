// A replicated apply must NOT report success when the local write didn't land.
// exec_bg used to swallow every write error and the apply returned ApplyApplied,
// so a transient failure (lock / disk) dropped the op from pending and the write
// was silently lost with no retry (#159). exec_bg now returns a tri-state and the
// apply defers on a retryable failure.
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"errors"
	"testing"
)

// The core of the fix: a retryable write DEFERS (op stays in pending, retries),
// while success or a permanent/quarantine skip APPLIES (a quarantined DB reseeds;
// a permanent error won't succeed on retry, so it must not defer forever).
func TestApplyResultMapping(t *testing.T) {
	for _, c := range []struct {
		in   ExecResult
		want ApplyResult
		why  string
	}{
		{ExecWrote, ApplyApplied, "a successful write is applied"},
		{ExecRetryable, ApplyDeferred, "a retryable failure must defer so the op is retried, not dropped"},
		{ExecSkipped, ApplyApplied, "quarantine/permanent must apply, not infinite-defer"},
	} {
		if got := apply_result(c.in); got != c.want {
			t.Errorf("apply_result(%v) = %v, want %v — %s", c.in, got, c.want, c.why)
		}
	}
}

// The classifier that decides retryable-vs-permanent: only lock contention and
// storage pressure are retryable; constraints, missing tables and corruption are
// permanent (deferring on those would wedge the stream forever).
func TestDbErrorIsTransient(t *testing.T) {
	for _, c := range []struct {
		msg  string
		want bool
	}{
		{"sqlite3: database is locked", true},
		{"database table is locked", true},
		{"disk I/O error", true},
		{"database or disk is full", true},
		{"cannot open: SQLITE_BUSY", true},
		{"UNIQUE constraint failed: users.uid", false},
		{"no such table: foo", false},
		{"database disk image is malformed", false}, // corruption is its own class, not retryable
		{"", false},
	} {
		var err error
		if c.msg != "" {
			err = errors.New(c.msg)
		}
		if got := db_error_is_transient(err); got != c.want {
			t.Errorf("db_error_is_transient(%q) = %v, want %v", c.msg, got, c.want)
		}
	}
}

// exec_bg maps a real write's outcome to the tri-state the apply path consumes.
func TestExecBgOutcomes(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	db := db_open("db/scratch.db")
	db.exec("create table t (id integer primary key, v text)")

	if got := db.exec_bg("scratch insert", "insert into t (id, v) values (1, 'a')"); got != ExecWrote {
		t.Errorf("successful write -> %v, want ExecWrote", got)
	}
	// A permanent error (missing table) must be Skipped, not Retryable — retrying
	// it forever would wedge the stream.
	if got := db.exec_bg("scratch bad", "insert into nope (x) values (1)"); got != ExecSkipped {
		t.Errorf("permanent error -> %v, want ExecSkipped", got)
	}
	// A quarantined DB is skipped without touching it — so a corrupt DB defers to
	// its reseed instead of the apply looping on it.
	db_quarantine(db.path, "test", errors.New("database disk image is malformed"))
	if got := db.exec_bg("scratch after quarantine", "insert into t (id, v) values (2, 'b')"); got != ExecSkipped {
		t.Errorf("quarantined-DB write -> %v, want ExecSkipped", got)
	}
}

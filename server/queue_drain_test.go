// Mochi server: the shutdown drain waits for what is actually in flight.
//
// queue_drain must count status='sending'; nothing in the server writes 'sent',
// so counting that returned on the first iteration and logged a drain it never
// did.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// queue_drain_db builds a queue table in a temporary data directory.
func queue_drain_db(t *testing.T) *DB {
	t.Helper()
	original := data_dir
	data_dir = t.TempDir()
	t.Cleanup(func() { data_dir = original })
	if err := os.MkdirAll(filepath.Join(data_dir, "db"), 0o755); err != nil {
		t.Fatalf("creating the db directory: %v", err)
	}
	db := db_open("db/queue.db")
	db.exec(`create table if not exists queue (
		id text primary key, type text not null, target text not null default '',
		from_entity text not null default '', to_entity text not null default '',
		service text not null default '', event text not null default '',
		status text not null default 'pending', attempts integer not null default 0,
		next_retry integer not null default 0, created integer not null default 0,
		claimed integer not null default 0, priority integer not null default 0)`)
	return db
}

// queue_drain_row inserts one row with the given status.
func queue_drain_row(t *testing.T, db *DB, id, status string) {
	t.Helper()
	db.exec("insert into queue (id, type, status, created) values (?, 'direct', ?, ?)", id, status, now())
}

// TestDrainWaitsForSendingRows: a row in flight must hold the drain until its
// budget runs out.
func TestDrainWaitsForSendingRows(t *testing.T) {
	db := queue_drain_db(t)
	queue_drain_row(t, db, "inflight", "sending")

	started := time.Now()
	queue_drain(2 * time.Second)
	waited := time.Since(started)

	if waited < 1500*time.Millisecond {
		t.Errorf("queue_drain returned after %v with a row still sending; it is meant to wait up to its timeout, and returning early makes the \"Queue drained\" line it logs untrue", waited)
	}
}

// TestDrainReturnsWhenNothingIsSending: the wait must end as soon as the
// in-flight set empties, or every shutdown pays the full budget.
func TestDrainReturnsWhenNothingIsSending(t *testing.T) {
	db := queue_drain_db(t)
	queue_drain_row(t, db, "waiting", "pending")
	queue_drain_row(t, db, "stuck", "parked")

	started := time.Now()
	queue_drain(5 * time.Second)
	waited := time.Since(started)

	if waited > time.Second {
		t.Errorf("queue_drain waited %v with nothing in flight; pending and parked rows are not in flight - a busy server always has pending rows, so waiting for them would burn the whole budget on every shutdown", waited)
	}
}

// TestDrainIgnoresPendingAndParked states the same rule from the other side,
// against a queue that holds a realistic backlog and one row in flight. The
// drain must key on the one, not the backlog.
func TestDrainIgnoresPendingAndParked(t *testing.T) {
	db := queue_drain_db(t)
	for i := 0; i < 50; i++ {
		queue_drain_row(t, db, "pending-"+string(rune('a'+i%26))+string(rune('a'+i/26)), "pending")
	}
	queue_drain_row(t, db, "parked-1", "parked")

	started := time.Now()
	queue_drain(3 * time.Second)
	if waited := time.Since(started); waited > time.Second {
		t.Errorf("queue_drain waited %v for a backlog of undeliverable rows; only rows being sent right now are worth waiting for", waited)
	}
}

// TestDrainCountsSending pins the predicate, since the timing tests above
// cannot distinguish "counts sending" from "counts some other always-empty
// status" - which is exactly how the original defect passed unnoticed.
func TestDrainCountsSending(t *testing.T) {
	source, err := os.ReadFile("queue.go")
	if err != nil {
		t.Fatalf("reading queue.go: %v", err)
	}
	text := string(source)
	at := strings.Index(text, "func queue_drain(")
	if at < 0 {
		t.Fatal("queue.go no longer defines queue_drain")
	}
	body := text[at:]
	if end := strings.Index(body, "\n}\n"); end > 0 {
		body = body[:end]
	}

	if !strings.Contains(body, "status = 'sending'") {
		t.Error("queue_drain does not count rows being sent, so it cannot know whether anything is in flight")
	}
	if strings.Contains(body, "'sent'") {
		t.Error("queue_drain counts status 'sent' again; nothing in the server writes that, so the count is always zero and the drain returns immediately")
	}
	// The timeout report must name the in-flight count too: reporting every
	// queued row would make a clean shutdown of a server holding a week of
	// undeliverable rows read as a failed drain.
	if strings.Contains(body, `db.integer("select count(*) from queue")`) {
		t.Error("the drain-timeout message counts every queued row rather than the ones still sending")
	}
}

// TestQueueNeverWritesSent is the fact all of the above rests on. If a status
// named 'sent' is ever introduced, this fails and whoever added it can decide
// what the drain should do about it.
func TestQueueNeverWritesSent(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("reading the package directory: %v", err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		data, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("reading %s: %v", name, err)
		}
		for number, line := range strings.Split(string(data), "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "//") || !strings.Contains(line, "status") {
				continue
			}
			if strings.Contains(line, "'sent'") {
				t.Errorf("%s:%d references status 'sent': %s", name, number+1, trimmed)
			}
		}
	}
}

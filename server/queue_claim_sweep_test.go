// Mochi server: the stuck-sending safety net measures how long a row has been
// claimed, not how long ago it was enqueued.
//
// queue.created is the enqueue time and is never rewritten - the retry path
// bumps attempts and next_retry and leaves it - so `status='sending' AND
// created < now()-60` was already true for any row that had ever been retried,
// which is every row on a flaky link, the exact population the net exists for.
// It swept them back to pending the instant they were claimed, while the
// sender still held them. queue_manager loops straight back into the sweep
// whenever it acted on a row rather than waiting for its tick, so on a busy
// queue that fired continuously.
//
// The other line in that function requeued status='sent'. Nothing in the
// server has ever set 'sent', so it matched nothing, for ever; it is gone.
// (#88 covers the surviving count in queue_drain.)
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// sweep_test_row inserts one queue row with an explicit status, enqueue time
// and claim time, so a test can describe the exact shape it cares about.
func sweep_test_row(t *testing.T, id, status string, created, claimed int64) {
	t.Helper()
	db := db_open("db/queue.db")
	db.exec(`insert into queue (id, target, from_entity, to_entity, service, event,
			next_retry, created, status, claimed)
		values (?, 'peer', 'from', 'to', 'test', 'test/event', 0, ?, ?, ?)`,
		id, created, status, claimed)
}

func sweep_test_status(t *testing.T, id string) string {
	t.Helper()
	db := db_open("db/queue.db")
	row, _ := db.row("select status from queue where id=?", id)
	if row == nil {
		t.Fatalf("row %q vanished", id)
	}
	status, _ := row["status"].(string)
	return status
}

// TestSweepLeavesAFreshlyClaimedRetry is the finding. An old message being
// retried is claimed now; the sender is holding it. created is hours stale and
// must not decide anything.
func TestSweepLeavesAFreshlyClaimedRetry(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db_create()

	sweep_test_row(t, "retried", "sending", now()-86400, now())
	queue_check_ack_timeout()

	if got := sweep_test_status(t, "retried"); got != "sending" {
		t.Errorf("a row claimed a moment ago was swept to %q: the sender still holds it, so this is a duplicate on the wire and a retry ladder advancing on a message that was never failing", got)
	}
}

// TestSweepReclaimsAGenuinelyStuckRow is the other half: the net must still
// catch a row whose sender really did go away.
func TestSweepReclaimsAGenuinelyStuckRow(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db_create()

	sweep_test_row(t, "abandoned", "sending", now()-86400, now()-queue_claim_timeout-10)
	queue_check_ack_timeout()

	if got := sweep_test_status(t, "abandoned"); got != "pending" {
		t.Errorf("a row claimed %ds ago is still %q: a sender that died mid-flight would strand it until restart", queue_claim_timeout+10, got)
	}
}

// TestSweepLeavesAFreshEnqueue. A row claimed just now whose enqueue is also
// recent must be left alone too - the control that shows the test above is not
// passing merely because nothing is ever swept.
func TestSweepLeavesAFreshEnqueue(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db_create()

	sweep_test_row(t, "fresh", "sending", now(), now())
	queue_check_ack_timeout()

	if got := sweep_test_status(t, "fresh"); got != "sending" {
		t.Errorf("a freshly claimed row was swept to %q", got)
	}
}

// TestClaimStampsTheClaimTime. The column is only worth anything if every path
// that marks a row sending sets it; one that forgets leaves claimed at 0, and
// zero is older than any threshold, so the row is swept immediately - exactly
// the bug, reintroduced through a side door.
func TestClaimStampsTheClaimTime(t *testing.T) {
	source, err := os.ReadFile("queue.go")
	if err != nil {
		t.Fatalf("read queue.go: %v", err)
	}
	// `set status='sending'` is the write; the bare string also appears in
	// prose and in the rollback's WHERE clause, so comments are stripped and
	// only the assignment form counted.
	marks, stamps := 0, 0
	for _, line := range strings.Split(string(source), "\n") {
		code := line
		if i := strings.Index(code, "//"); i >= 0 {
			code = code[:i]
		}
		if strings.Contains(code, "set status='sending'") {
			marks++
			if strings.Contains(code, "set status='sending', claimed=") {
				stamps++
			}
		}
	}
	if marks != stamps {
		t.Errorf("%d places mark a row sending but only %d stamp claimed: an unstamped row has claimed=0 and is swept the moment it is claimed", marks, stamps)
	}
	if stamps == 0 {
		t.Error("no claim site stamps the claim time")
	}
}

// TestSweepIsKeyedOnClaimed pins the predicate itself, and pins the removal of
// the dead 'sent' requeue beside it.
func TestSweepIsKeyedOnClaimed(t *testing.T) {
	source, err := os.ReadFile("queue.go")
	if err != nil {
		t.Fatalf("read queue.go: %v", err)
	}
	text := string(source)
	start := strings.Index(text, "func queue_check_ack_timeout(")
	if start < 0 {
		t.Fatal("queue_check_ack_timeout not found")
	}
	body := text[start:]
	body = body[:strings.Index(body, "\n}\n")]

	if strings.Contains(body, "created < ?") {
		t.Error("the sweep still keys on created: any retried row is swept the instant it is claimed")
	}
	if !strings.Contains(body, "claimed < ?") {
		t.Error("the sweep does not key on claimed")
	}
	if strings.Contains(body, "'sent'") {
		t.Error("the dead status='sent' requeue is back; nothing in the server sets that status, so it matches nothing")
	}
}

// TestQueueSchemaCarriesClaimed. db_create builds the column for new installs
// and db_upgrade_7 adds it to existing ones; a fresh install missing it would
// fail the sweep's query outright, and an upgrade missing it would fail on
// every existing deployment instead.
func TestQueueSchemaCarriesClaimed(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db_create()

	db := db_open("db/queue.db")
	have, _ := db.exists("select 1 from pragma_table_info('queue') where name=?", "claimed")
	if !have {
		t.Error("db_create does not build queue.claimed; the sweep's query would fail on a fresh install")
	}

	source, err := os.ReadFile("db.go")
	if err != nil {
		t.Fatalf("read db.go: %v", err)
	}
	text := string(source)
	if !strings.Contains(text, "func db_upgrade_7()") {
		t.Error("no db_upgrade_7: existing installs would never get the column")
	}
	if !strings.Contains(text, "case 7:") {
		t.Error("db_upgrade has no case 7, so db_upgrade_7 is never reached")
	}
	// At least 7: later migrations bump it further, and this test is about
	// db_upgrade_7 having been reachable, not about being the newest.
	if version := atoi(regexp.MustCompile(`schema_version = (\d+)`).FindStringSubmatch(text)[1], 0); version < 7 {
		t.Errorf("schema_version = %d, want at least 7, or db_upgrade_7 never runs", version)
	}
}

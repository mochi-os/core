// Mochi server: multi-master replication convergence tests
// Copyright Alistair Cunningham 2026
//
// Hand-written scenarios that exercise the documented intent under
// concurrent writes from both pair members - the criterion that
// distinguishes a replication architecture that converges from one
// that quietly forks. Built on the harness in
// replication_harness_test.go.
//
// All scenarios follow the same shape:
//   1. Seed both hosts so apply paths pass user_exists.
//   2. Drive operations on h1 and h2 in some interleaving.
//   3. flush() to drain the wire model.
//   4. Assert both hosts hold the same rows (per-table SELECTs +
//      compare).

package main

import (
	"fmt"
	"testing"
)

const (
	mmUID      = "uid-multimaster"
	mmUsername = "mm@example.com"
)

// mm_entity_id mints a 50-char entity id from a single seed byte. The
// real entity-id validator (valid("entity")) accepts 49-51 word chars.
func mm_entity_id(seed byte) string {
	out := make([]byte, 50)
	out[0] = seed
	for i := 1; i < 50; i++ {
		out[i] = 'a'
	}
	return string(out)
}

// seed_both_hosts brings each host to the same starting state: one
// shared user, one shared identity entity, and the schedule + sessions
// schemas the per-user emit paths target.
func seed_both_hosts(t *testing.T, h *harness) {
	for _, name := range []string{"h1", "h2"} {
		h.switchTo(name)
		h.setup_harness_user(mmUID, mmUsername, mm_entity_id('m'))
		schedule_db().exec("create table if not exists schedule (id integer primary key, user text not null, app text not null, due int not null, event text not null, data text not null, interval int not null, created int not null)")
		setup_sessions_test_schema()
	}
}

func mm_settings_schema() {
	db_open("db/settings.db").exec("create table if not exists settings (name text primary key, value text not null)")
	db_open("db/settings.db").exec("create table if not exists documents ( name text not null, language text not null, body text not null, updated integer not null, primary key ( name, language ) )")
}

// TestMultiMasterScheduleCreateBothSides: each host creates a
// distinct schedule for the same user at the same time. After flush,
// both rows exist on both hosts (no double-insert, no loss).
func TestMultiMasterScheduleCreateBothSides(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	seed_both_hosts(t, h)

	h.switchTo("h1")
	id1 := schedule_create(mmUID, "feeds", 1000, "refresh", "{}", 60)
	if id1 == 0 {
		t.Fatal("h1 schedule_create returned 0")
	}

	h.switchTo("h2")
	id2 := schedule_create(mmUID, "crm", 2000, "reminder", "{\"who\":\"alice\"}", 0)
	if id2 == 0 {
		t.Fatal("h2 schedule_create returned 0")
	}

	h.flush()

	// Each host's schedule.db should now contain both rows. The local
	// autoincrement ids will differ; the natural key (user, app, event,
	// created) is what we compare on.
	for _, name := range []string{"h1", "h2"} {
		h.switchTo(name)
		rows, _ := schedule_db().rows(
			"select app, event from schedule where user=? order by app",
			mmUID)
		if len(rows) != 2 {
			t.Errorf("%s: rows = %d, want 2 (got %v)", name, len(rows), rows)
			continue
		}
		got := []string{}
		for _, r := range rows {
			got = append(got, r["app"].(string)+"/"+r["event"].(string))
		}
		want := []string{"crm/reminder", "feeds/refresh"}
		for i, g := range got {
			if g != want[i] {
				t.Errorf("%s: row %d = %q, want %q", name, i, g, want[i])
			}
		}
	}
}

// TestMultiMasterScheduleDeleteWinsOverStaleSet: an out-of-order
// delivery where h2's delete arrives before h1's create would
// resurrect the schedule on the receiver. With in-order per-stream
// delivery the receiver-side framework enforces ordering; this test
// asserts the documented behaviour holds.
func TestMultiMasterScheduleDeleteWinsOverStaleSet(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	seed_both_hosts(t, h)

	// h1 creates a schedule and h2 ends up holding it.
	h.switchTo("h1")
	id := schedule_create(mmUID, "feeds", 1000, "tick", "{}", 0)
	if id == 0 {
		t.Fatal("h1 schedule_create returned 0")
	}
	h.flush()

	// h2 cancels it.
	h.switchTo("h2")
	rows, _ := schedule_db().rows(
		"select id from schedule where user=? and app='feeds' and event='tick'",
		mmUID)
	if len(rows) != 1 {
		t.Fatalf("h2: pre-delete rows = %d, want 1", len(rows))
	}
	id2, _ := rows[0]["id"].(int64)
	schedule_delete(id2)

	h.flush()

	for _, name := range []string{"h1", "h2"} {
		h.switchTo(name)
		exists, _ := schedule_db().exists(
			"select 1 from schedule where user=? and app='feeds' and event='tick'",
			mmUID)
		if exists {
			t.Errorf("%s: row should be deleted everywhere after cross-host cancel", name)
		}
	}
}

// TestMultiMasterPartitionHealSchedule: partition the wire, do
// independent ops on each side, heal, flush, assert convergence. The
// classic "two-replica long-partition" case from the doc - after the
// heal both sides see every op from both sides.
func TestMultiMasterPartitionHealSchedule(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	seed_both_hosts(t, h)

	h.partition()

	// During the partition each side does several local ops. Distinct
	// event names per iteration so the natural-key dedup (user, app,
	// event, created) keeps them as separate rows even when the
	// tight-loop now() returns the same epoch second - that
	// same-second-merge case is its own documented limitation
	// (TestMultiMasterScheduleSameSecondMerge below), not what this
	// test is exercising.
	h.switchTo("h1")
	for i := 0; i < 5; i++ {
		if schedule_create(mmUID, "feeds", int64(1000+i), fmt.Sprintf("h1event-%d", i), "{}", 0) == 0 {
			t.Fatalf("h1: schedule_create %d returned 0", i)
		}
	}

	h.switchTo("h2")
	for i := 0; i < 5; i++ {
		if schedule_create(mmUID, "crm", int64(2000+i), fmt.Sprintf("h2event-%d", i), "{}", 0) == 0 {
			t.Fatalf("h2: schedule_create %d returned 0", i)
		}
	}

	// Both sides should hold five local-only rows; the held bucket
	// should contain ten deliveries (five for each direction). pending()
	// returns queue + held, so it's ten on each side.
	if got := h.pending("h1"); got != 10 {
		t.Errorf("partition: h1 pending = %d, want 10 (5 + 5)", got)
	}
	if got := h.pending("h2"); got != 10 {
		t.Errorf("partition: h2 pending = %d, want 10", got)
	}

	h.heal()
	h.flush()

	// After heal both hosts hold all ten rows (5 distinct events per
	// host, each row keyed uniquely).
	for _, name := range []string{"h1", "h2"} {
		h.switchTo(name)
		var n int64
		row, _ := schedule_db().row(
			"select count(*) as n from schedule where user=?", mmUID)
		if row != nil {
			n, _ = row["n"].(int64)
		}
		if n != 10 {
			t.Errorf("%s: row count = %d, want 10 after partition-heal", name, n)
		}
	}
}

// TestMultiMasterScheduleSameSecondMerge documents the natural-key
// dedup limitation noted in task #49: multiple schedule_create calls
// in the same epoch second with the same (user, app, event) key are
// indistinguishable on the wire and merge into one row at the
// receiver. The local host keeps all N (autoincrement-distinct), the
// remote host gets one. Documented and accepted behaviour - this
// test pins it so a future change can't silently break the contract.
func TestMultiMasterScheduleSameSecondMerge(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	seed_both_hosts(t, h)

	h.switchTo("h1")
	for i := 0; i < 3; i++ {
		if schedule_create(mmUID, "feeds", int64(1000+i), "samename", "{}", 0) == 0 {
			t.Fatalf("h1 schedule_create %d returned 0", i)
		}
	}
	h.flush()

	h.switchTo("h1")
	row, _ := schedule_db().row(
		"select count(*) as n from schedule where user=? and event='samename'",
		mmUID)
	if n, _ := row["n"].(int64); n != 3 {
		t.Errorf("h1 local rows: got %d, want 3 (autoincrement keeps locals distinct)", n)
	}

	h.switchTo("h2")
	row, _ = schedule_db().row(
		"select count(*) as n from schedule where user=? and event='samename'",
		mmUID)
	n, _ := row["n"].(int64)
	if n != 1 {
		t.Errorf("h2 received rows: got %d, want 1 (same-second natural-key merge - documented limitation)", n)
	}
}

// TestMultiMasterSettingSetConcurrent: both hosts write different
// values to the same setting key. The documented intent is "last-
// arrival-order" per replication_system.md (no LWW; whoever's emit
// gets applied last wins on each receiver). The harness applies in
// the order ops were captured, so flush delivers h1's then h2's, h2
// wins on h1 (its op arrives), and h1 wins on h2 (h1's op arrives).
// That's divergence by design (settings are operator-controlled, low
// frequency, operator can resolve). The test documents that the
// architecture does what it says, not what we might wish it did.
func TestMultiMasterSettingSetConcurrent(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	for _, name := range []string{"h1", "h2"} {
		h.switchTo(name)
		mm_settings_schema()
	}

	h.switchTo("h1")
	setting_set("signup_enabled", "h1-wins")

	h.switchTo("h2")
	setting_set("signup_enabled", "h2-wins")

	h.flush()

	// Each host kept its own last local write, then applied the other
	// host's emit on top. Whichever emit arrived second wins. With our
	// flush ordering both arrive once; the receiver's local value
	// before flush is what gets overwritten.
	h.switchTo("h1")
	got1 := setting_get("signup_enabled", "")
	h.switchTo("h2")
	got2 := setting_get("signup_enabled", "")

	if got1 == got2 {
		// Convergence by accident is fine - the receiver-of-second
		// applied last on both sides happens to be the same value.
		return
	}
	// Otherwise document the divergence shape: each host has the
	// other's value as the final state, because the local write
	// happens before the cross-host op arrives. This is the
	// documented "last-arrival-order" behaviour.
	if got1 != "h2-wins" || got2 != "h1-wins" {
		t.Errorf("settings concurrent: got h1=%q h2=%q; want either convergent or h1=h2-wins h2=h1-wins per last-arrival-order doc",
			got1, got2)
	}
}

// TestMultiMasterDocumentSetConcurrent: both hosts edit the same
// (name, language) document override (the task #47 path). settings.go
// gives each write a fresh updated=now() timestamp but the apply path
// replaces unconditionally - so the same last-arrival-order shape as
// settings: receiver of the later emit wins. Asserts the document
// replication path doesn't lose updates entirely and doesn't crash
// under concurrent writes.
func TestMultiMasterDocumentSetConcurrent(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	for _, name := range []string{"h1", "h2"} {
		h.switchTo(name)
		mm_settings_schema()
	}

	h.switchTo("h1")
	if err := document_set("terms", "en", "h1 wrote this"); err != nil {
		t.Fatalf("h1 document_set: %v", err)
	}

	h.switchTo("h2")
	if err := document_set("terms", "en", "h2 wrote this"); err != nil {
		t.Fatalf("h2 document_set: %v", err)
	}

	h.flush()

	for _, name := range []string{"h1", "h2"} {
		h.switchTo(name)
		row, _ := db_open("db/settings.db").row(
			"select body, updated from documents where name='terms' and language='en'")
		if row == nil {
			t.Errorf("%s: document row missing after flush", name)
			continue
		}
		body, _ := row["body"].(string)
		if body != "h1 wrote this" && body != "h2 wrote this" {
			t.Errorf("%s: body = %q, want either side's write", name, body)
		}
	}
}

// TestMultiMasterScheduleReorderedDelivery: queue both hosts' emits
// without flushing in between, shuffle h2's pending queue, flush.
// Receiver-side ordering shouldn't matter for idempotent
// natural-key-based applies (the task #49 design): re-applying the
// same row is a no-op, deletes match by key.
func TestMultiMasterScheduleReorderedDelivery(t *testing.T) {
	h := newHarness(t)
	defer h.cleanup()
	seed_both_hosts(t, h)

	h.switchTo("h1")
	for i := 0; i < 5; i++ {
		schedule_create(mmUID, "feeds", int64(1000+i), fmt.Sprintf("ev-%d", i), "{}", 0)
	}

	h.reorder("h2", 1)
	h.flush()

	h.switchTo("h2")
	rows, _ := schedule_db().rows(
		"select count(*) as n from schedule where user=? and app='feeds'", mmUID)
	if len(rows) == 0 {
		t.Fatal("h2: no rows after reordered flush")
	}
	n, _ := rows[0]["n"].(int64)
	if n != 5 {
		t.Errorf("h2 after reorder: rows = %d, want 5", n)
	}
}

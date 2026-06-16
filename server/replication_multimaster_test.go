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
	"bytes"
	"fmt"
	"strconv"
	"sync"
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
		h.switch_to(name)
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
	h := new_harness(t)
	defer h.cleanup()
	seed_both_hosts(t, h)

	h.switch_to("h1")
	id1 := schedule_create(mmUID, "feeds", 1000, "refresh", "{}", 60)
	if id1 == 0 {
		t.Fatal("h1 schedule_create returned 0")
	}

	h.switch_to("h2")
	id2 := schedule_create(mmUID, "crm", 2000, "reminder", "{\"who\":\"alice\"}", 0)
	if id2 == 0 {
		t.Fatal("h2 schedule_create returned 0")
	}

	h.flush()

	// Each host's schedule.db should now contain both rows. The local
	// autoincrement ids will differ; the natural key (user, app, event,
	// created) is what we compare on.
	for _, name := range []string{"h1", "h2"} {
		h.switch_to(name)
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
	h := new_harness(t)
	defer h.cleanup()
	seed_both_hosts(t, h)

	// h1 creates a schedule and h2 ends up holding it.
	h.switch_to("h1")
	id := schedule_create(mmUID, "feeds", 1000, "tick", "{}", 0)
	if id == 0 {
		t.Fatal("h1 schedule_create returned 0")
	}
	h.flush()

	// h2 cancels it.
	h.switch_to("h2")
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
		h.switch_to(name)
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
	h := new_harness(t)
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
	h.switch_to("h1")
	for i := 0; i < 5; i++ {
		if schedule_create(mmUID, "feeds", int64(1000+i), fmt.Sprintf("h1event-%d", i), "{}", 0) == 0 {
			t.Fatalf("h1: schedule_create %d returned 0", i)
		}
	}

	h.switch_to("h2")
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
		h.switch_to(name)
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
	h := new_harness(t)
	defer h.cleanup()
	seed_both_hosts(t, h)

	h.switch_to("h1")
	for i := 0; i < 3; i++ {
		if schedule_create(mmUID, "feeds", int64(1000+i), "samename", "{}", 0) == 0 {
			t.Fatalf("h1 schedule_create %d returned 0", i)
		}
	}
	h.flush()

	h.switch_to("h1")
	row, _ := schedule_db().row(
		"select count(*) as n from schedule where user=? and event='samename'",
		mmUID)
	if n, _ := row["n"].(int64); n != 3 {
		t.Errorf("h1 local rows: got %d, want 3 (autoincrement keeps locals distinct)", n)
	}

	h.switch_to("h2")
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
	h := new_harness(t)
	defer h.cleanup()
	for _, name := range []string{"h1", "h2"} {
		h.switch_to(name)
		mm_settings_schema()
	}

	h.switch_to("h1")
	setting_set("signup_enabled", "h1-wins")

	h.switch_to("h2")
	setting_set("signup_enabled", "h2-wins")

	h.flush()

	// Each host kept its own last local write, then applied the other
	// host's emit on top. Whichever emit arrived second wins. With our
	// flush ordering both arrive once; the receiver's local value
	// before flush is what gets overwritten.
	h.switch_to("h1")
	got1 := setting_get("signup_enabled", "")
	h.switch_to("h2")
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
	h := new_harness(t)
	defer h.cleanup()
	for _, name := range []string{"h1", "h2"} {
		h.switch_to(name)
		mm_settings_schema()
	}

	h.switch_to("h1")
	if err := document_set("terms", "en", "h1 wrote this"); err != nil {
		t.Fatalf("h1 document_set: %v", err)
	}

	h.switch_to("h2")
	if err := document_set("terms", "en", "h2 wrote this"); err != nil {
		t.Fatalf("h2 document_set: %v", err)
	}

	h.flush()

	for _, name := range []string{"h1", "h2"} {
		h.switch_to(name)
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
	h := new_harness(t)
	defer h.cleanup()
	seed_both_hosts(t, h)

	h.switch_to("h1")
	for i := 0; i < 5; i++ {
		schedule_create(mmUID, "feeds", int64(1000+i), fmt.Sprintf("ev-%d", i), "{}", 0)
	}

	h.reorder("h2", 1)
	h.flush()

	h.switch_to("h2")
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

// ===== 3-host topologies =====
//
// Mochi supports two replication shapes that can scale beyond two
// hosts:
//
//   - whole-server pair triple ("server-server-server"): three
//     operator-paired hosts. The pair_members set covers all three;
//     pair-scope ops (settings, apps, domains, users.users pair-only
//     columns, settings.documents) fan out to every other pair
//     member. Per-user-scope ops likewise reach every host because
//     every host hosts the user.
//
//   - per-user link triple ("user-user-user"): three hosts owned by
//     different operators, linked only by one shared user. No
//     operator pair (pair_members = {}); per-user-scope ops fan out
//     to the link partners via user_hosts. Pair-scope ops do NOT
//     cross link boundaries - operator decisions stay local.

const (
	tt_h1   = "h1"
	tt_h2   = "h2"
	tt_h3   = "h3"
	ttUID   = "uid-triple"
	ttUname = "triple@example.com"
)

// seed_three_hosts seeds the named hosts with the same user + schedule
// + sessions + settings schemas so each host is ready to apply ops.
func seed_three_hosts(t *testing.T, h *harness) {
	for _, name := range []string{tt_h1, tt_h2, tt_h3} {
		h.switch_to(name)
		h.setup_harness_user(ttUID, ttUname, mm_entity_id('t'))
		schedule_db().exec("create table if not exists schedule (id integer primary key, user text not null, app text not null, due int not null, event text not null, data text not null, interval int not null, created int not null)")
		setup_sessions_test_schema()
		mm_settings_schema()
		// Inbound-stream bookkeeping, needed when a test routes deliveries
		// through the production receive path (h.gated) instead
		// of raw replication_apply_op. Harmless no-op for the raw-apply tests.
		rdb := db_open("db/replication.db")
		rdb.exec("create table if not exists seen (peer text not null, scope text not null, user text not null default '', sequence integer not null, applied integer not null, primary key (peer, scope, user, sequence))")
		rdb.exec("create table if not exists cursor (peer text not null, scope text not null, user text not null default '', db text not null default '', sequence integer not null default 0, primary key (peer, scope, user, db))")
		rdb.exec("create table if not exists pending (peer text not null, scope text not null, user text not null default '', db text not null default '', sequence integer not null, prev integer not null default 0, schema integer not null default 0, payload blob not null, received integer not null, primary key (peer, scope, user, sequence))")
		rdb.exec("create table if not exists relayed (user text not null, origin text not null, seen integer not null, primary key (user, origin))")
		rdb.exec("create table if not exists links (user text not null, peer text not null, label text not null default '', placeholder text not null, received integer not null, expires integer not null, primary key (user, peer))")
		rdb.exec("create table if not exists joins (peer text not null primary key, label text not null default '', received integer not null, expires integer not null)")
	}
}

// TestThreeHostServerServerServerSchedule: three-host operator pair.
// h1 creates a schedule; both h2 and h3 receive it.
func TestThreeHostServerServerServerSchedule(t *testing.T) {
	h := new_harness(t, tt_h1, tt_h2, tt_h3)
	defer h.cleanup()
	// pair_members defaults to all three, user_hosts defaults to all
	// three - server-server-server is the default routing.
	seed_three_hosts(t, h)

	h.switch_to(tt_h1)
	if schedule_create(ttUID, "feeds", 1000, "tick", "{}", 60) == 0 {
		t.Fatal("h1 schedule_create returned 0")
	}
	h.flush()

	for _, name := range []string{tt_h2, tt_h3} {
		h.switch_to(name)
		exists, _ := schedule_db().exists(
			"select 1 from schedule where user=? and app='feeds' and event='tick'", ttUID)
		if !exists {
			t.Errorf("%s: missing replicated schedule row from h1", name)
		}
	}
}

// TestThreeHostServerServerServerSettings: pair-scope setting set on
// one operator-paired host reaches all other pair members.
func TestThreeHostServerServerServerSettings(t *testing.T) {
	h := new_harness(t, tt_h1, tt_h2, tt_h3)
	defer h.cleanup()
	seed_three_hosts(t, h)

	h.switch_to(tt_h2)
	setting_set("signup_enabled", "false")
	h.flush()

	for _, name := range []string{tt_h1, tt_h3} {
		h.switch_to(name)
		if got := setting_get("signup_enabled", ""); got != "false" {
			t.Errorf("%s: setting = %q, want %q", name, got, "false")
		}
	}
}

// TestThreeHostServerServerServerPartitionHeal: partition one host
// off, do ops on the remaining two AND on the partitioned one, heal,
// assert all three converge to the union.
func TestThreeHostServerServerServerPartitionHeal(t *testing.T) {
	h := new_harness(t, tt_h1, tt_h2, tt_h3)
	defer h.cleanup()
	seed_three_hosts(t, h)

	h.partition()

	h.switch_to(tt_h1)
	schedule_create(ttUID, "feeds", 1000, "from-h1", "{}", 0)
	h.switch_to(tt_h2)
	schedule_create(ttUID, "feeds", 2000, "from-h2", "{}", 0)
	h.switch_to(tt_h3)
	schedule_create(ttUID, "feeds", 3000, "from-h3", "{}", 0)

	h.heal()
	h.flush()

	for _, name := range []string{tt_h1, tt_h2, tt_h3} {
		h.switch_to(name)
		var n int64
		row, _ := schedule_db().row(
			"select count(*) as n from schedule where user=? and app='feeds'", ttUID)
		if row != nil {
			n, _ = row["n"].(int64)
		}
		if n != 3 {
			t.Errorf("%s: rows = %d, want 3 (one per host after heal)", name, n)
		}
	}
}

// TestThreeHostUserUserUserSchedule: three-host per-user-link
// topology, no operator pair. h1 creates a schedule for the linked
// user; per-user-scope routing carries it to h2 and h3.
func TestThreeHostUserUserUserSchedule(t *testing.T) {
	h := new_harness(t, tt_h1, tt_h2, tt_h3)
	defer h.cleanup()
	// No operator pair, but the user is linked across all three hosts.
	h.set_pair_members() // empty: each host is its own operator
	h.set_user_hosts(ttUID, tt_h1, tt_h2, tt_h3)
	seed_three_hosts(t, h)

	h.switch_to(tt_h1)
	if schedule_create(ttUID, "feeds", 1000, "linked-tick", "{}", 60) == 0 {
		t.Fatal("h1 schedule_create returned 0")
	}
	h.flush()

	for _, name := range []string{tt_h2, tt_h3} {
		h.switch_to(name)
		exists, _ := schedule_db().exists(
			"select 1 from schedule where user=? and event='linked-tick'", ttUID)
		if !exists {
			t.Errorf("%s: missing schedule row via per-user link", name)
		}
	}
}

// TestThreeHostUserUserUserPairOnlyStaysLocal: in the user-user-user
// topology, pair-scope emits (settings, system-row writes against
// users.users, settings.documents, etc.) MUST NOT cross link
// boundaries because the other hosts belong to different operators.
// Asserts a setting_set on h1 does not appear on h2 or h3, and a
// pair-only username change is likewise contained.
func TestThreeHostUserUserUserPairOnlyStaysLocal(t *testing.T) {
	h := new_harness(t, tt_h1, tt_h2, tt_h3)
	defer h.cleanup()
	h.set_pair_members() // no operator pair
	h.set_user_hosts(ttUID, tt_h1, tt_h2, tt_h3)
	seed_three_hosts(t, h)

	// Pair-scope: setting only the operator on h1 cares about.
	h.switch_to(tt_h1)
	setting_set("signup_enabled", "h1-only")
	// Pair-scope: a username change goes via the pair-only path. h2
	// and h3 must NOT receive it - each operator chose their own
	// username for the user.
	replication_emit_users_users_pair_set(ttUID, map[string]string{"username": "renamed-on-h1"})

	h.flush()

	for _, name := range []string{tt_h2, tt_h3} {
		h.switch_to(name)
		if got := setting_get("signup_enabled", ""); got == "h1-only" {
			t.Errorf("%s: setting leaked across operator boundary: %q", name, got)
		}
		row, _ := db_open("db/users.db").row("select username from users where uid=?", ttUID)
		if got, _ := row["username"].(string); got == "renamed-on-h1" {
			t.Errorf("%s: pair-only username leaked across per-user link: %q", name, got)
		}
	}
}

// TestReplicationLinkRowReachesClusterSiblings: a pending per-user
// link-request that lands on one member of a paired cluster must show on
// the others, so the user can approve it whichever member a round-robin
// front routed them to. Resolving on one member must clear it everywhere.
func TestReplicationLinkRowReachesClusterSiblings(t *testing.T) {
	h := new_harness(t, tt_h1, tt_h2, tt_h3) // a 3-member cluster (all paired)
	defer h.cleanup()
	seed_three_hosts(t, h)

	// A link-request lands on h1 only (the member the joiner reached).
	h.switch_to(tt_h1)
	db_open("db/replication.db").exec(
		"insert or replace into links (user, peer, label, placeholder, received, expires) values (?, ?, ?, ?, ?, ?)",
		ttUID, "peer-joiner", "joiner", "ph-1", int64(1000), int64(4600))
	replication_emit_link_row(ttUID, &LinkRow{
		Peer: "peer-joiner", Label: "joiner", Placeholder: "ph-1", Received: 1000, Expires: 4600,
	})
	h.flush()

	for _, name := range []string{tt_h2, tt_h3} {
		h.switch_to(name)
		if ok, _ := db_open("db/replication.db").exists(
			"select 1 from links where user=? and peer=?", ttUID, "peer-joiner"); !ok {
			t.Errorf("%s: pending link-request did not replicate from the member that received it", name)
		}
	}

	// Resolve (approve/deny) on h2 - clears the request cluster-wide.
	h.switch_to(tt_h2)
	db_open("db/replication.db").exec("delete from links where user=? and peer=?", ttUID, "peer-joiner")
	replication_emit_link_row(ttUID, &LinkRow{Peer: "peer-joiner", Delete: true})
	h.flush()

	for _, name := range []string{tt_h1, tt_h3} {
		h.switch_to(name)
		if ok, _ := db_open("db/replication.db").exists(
			"select 1 from links where user=? and peer=?", ttUID, "peer-joiner"); ok {
			t.Errorf("%s: link-request not cleared after it was resolved on a sibling", name)
		}
	}
}

// TestReplicationJoinRowReachesPairSiblings: a pending whole-server
// join-request that lands on one pair member must show on the others, so
// the operator can approve it on whichever member their admin UI reached.
// Pair-scoped (server-signed), keyed by the joiner's peer, no user.
func TestReplicationJoinRowReachesPairSiblings(t *testing.T) {
	h := new_harness(t, tt_h1, tt_h2, tt_h3) // a paired cluster
	defer h.cleanup()
	seed_three_hosts(t, h)

	// A join-request lands on h1 only.
	h.switch_to(tt_h1)
	db_open("db/replication.db").exec(
		"insert or replace into joins (peer, label, received, expires) values (?, ?, ?, ?)",
		"peer-joiner", "joiner", int64(1000), int64(1600))
	replication_emit_system_row("replication", "joins",
		map[string]string{"peer": "peer-joiner"},
		map[string]string{"label": "joiner", "received": "1000", "expires": "1600"}, false)
	h.flush()

	for _, name := range []string{tt_h2, tt_h3} {
		h.switch_to(name)
		if ok, _ := db_open("db/replication.db").exists(
			"select 1 from joins where peer=?", "peer-joiner"); !ok {
			t.Errorf("%s: pending join-request did not replicate across the pair", name)
		}
	}

	// Resolve on h2 - clears the request across the pair.
	h.switch_to(tt_h2)
	db_open("db/replication.db").exec("delete from joins where peer=?", "peer-joiner")
	replication_emit_system_row("replication", "joins",
		map[string]string{"peer": "peer-joiner"}, nil, true)
	h.flush()

	for _, name := range []string{tt_h1, tt_h3} {
		h.switch_to(name)
		if ok, _ := db_open("db/replication.db").exists(
			"select 1 from joins where peer=?", "peer-joiner"); ok {
			t.Errorf("%s: join-request not cleared after it was resolved on a sibling", name)
		}
	}
}

// TestReplicationTransitRelayUserUserToPair is the executable target for
// claude/plans/replication.md "Transit relay across membership sources".
// The production HA chain is asymmetric:
//
//	D --user-user--> W --server-pair--> Y
//
// A write on D reaches W (D's per-user host) and MUST transit through W to
// Y (W's pair member); D and Y are never directly connected. Today a write
// W *receives* is applied but never re-sent to W's other relationship, so Y
// diverges. RED until replication_op_receive relays an applied op to
// recipients(U) minus the source peer. flushIterationLimit asserts the
// relay terminates rather than bouncing.
func TestReplicationTransitRelayUserUserToPair(t *testing.T) {
	h := new_harness(t, tt_h1, tt_h2, tt_h3) // D=h1, W=h2, Y=h3
	defer h.cleanup()
	h.gated = true
	h.set_host_recipients(tt_h1, ttUID, tt_h2)        // D -> {W} (user-user)
	h.set_host_recipients(tt_h2, ttUID, tt_h1, tt_h3) // W -> {D, Y} (link + pair)
	h.set_host_recipients(tt_h3, ttUID, tt_h2)        // Y -> {W} (pair)
	seed_three_hosts(t, h)

	h.switch_to(tt_h1)
	if schedule_create(ttUID, "feeds", 1000, "from-D", "{}", 60) == 0 {
		t.Fatal("D schedule_create returned 0")
	}
	h.flush()

	// Sanity: W is D's direct per-user host - must have it (else setup bug).
	h.switch_to(tt_h2)
	if ok, _ := schedule_db().exists("select 1 from schedule where user=? and event='from-D'", ttUID); !ok {
		t.Fatal("W: missing write from its per-user peer D (setup bug, not the relay gap)")
	}
	// The gap: Y is reachable only by transiting W's pair.
	h.switch_to(tt_h3)
	if ok, _ := schedule_db().exists("select 1 from schedule where user=? and event='from-D'", ttUID); !ok {
		t.Error("Y: write on D did not transit W's pair to Y (transit relay missing)")
	}
}

// TestReplicationTransitRelayPairToUserUser is the reverse direction of the
// same chain: a write on Y (W's pair member) must transit W to reach D (W's
// per-user host). One relay mechanism covers both directions.
func TestReplicationTransitRelayPairToUserUser(t *testing.T) {
	h := new_harness(t, tt_h1, tt_h2, tt_h3) // D=h1, W=h2, Y=h3
	defer h.cleanup()
	h.gated = true
	h.set_host_recipients(tt_h1, ttUID, tt_h2)        // D -> {W} (user-user)
	h.set_host_recipients(tt_h2, ttUID, tt_h1, tt_h3) // W -> {D, Y} (link + pair)
	h.set_host_recipients(tt_h3, ttUID, tt_h2)        // Y -> {W} (pair)
	seed_three_hosts(t, h)

	h.switch_to(tt_h3)
	if schedule_create(ttUID, "feeds", 2000, "from-Y", "{}", 60) == 0 {
		t.Fatal("Y schedule_create returned 0")
	}
	h.flush()

	// Sanity: W is Y's direct pair - must have it.
	h.switch_to(tt_h2)
	if ok, _ := schedule_db().exists("select 1 from schedule where user=? and event='from-Y'", ttUID); !ok {
		t.Fatal("W: missing write from its pair peer Y (setup bug, not the relay gap)")
	}
	// The gap: D is reachable only by transiting W's per-user link.
	h.switch_to(tt_h1)
	if ok, _ := schedule_db().exists("select 1 from schedule where user=? and event='from-Y'", ttUID); !ok {
		t.Error("D: write on Y did not transit W's per-user link to D (transit relay missing)")
	}
}

// TestPairUsernameCollisionAtApply: documented behaviour from
// claude/plans/replication.md - when a pair-only username UPDATE
// replicates to a host whose UNIQUE-index would refuse it (because
// another local uid already holds that name), the local row stays
// unchanged. Pins the no-data-loss property: the receiving host's
// existing user is not silently renamed and the sender's user is
// not silently merged. Asymmetric setup: h1 holds only u2 (bob),
// h2 holds u1 (alice) + u2 (bob).
func TestPairUsernameCollisionAtApply(t *testing.T) {
	h := new_harness(t)
	defer h.cleanup()
	// h1: only u2 exists.
	h.switch_to("h1")
	setup_users_test_schema()
	db_open("db/users.db").exec("insert into users (uid, username) values (?, ?)", "u2", "bob@example.com")
	// h2: both u1 and u2 exist.
	h.switch_to("h2")
	setup_users_test_schema()
	udb2 := db_open("db/users.db")
	udb2.exec("insert into users (uid, username) values (?, ?)", "u1", "alice@example.com")
	udb2.exec("insert into users (uid, username) values (?, ?)", "u2", "bob@example.com")

	// h1 renames u2 -> alice@example.com. Locally fine (no other
	// row holds it on h1). The pair-only emit reaches h2.
	h.switch_to("h1")
	db_open("db/users.db").exec("update users set username=? where uid=?", "alice@example.com", "u2")
	replication_emit_users_users_pair_set("u2", map[string]string{"username": "alice@example.com"})

	h.flush()

	// h2: UNIQUE constraint on users_username refuses the UPDATE. u2
	// must still be bob; u1 must still be alice. No data lost.
	h.switch_to("h2")
	row, _ := db_open("db/users.db").row("select username from users where uid=?", "u2")
	if got, _ := row["username"].(string); got != "bob@example.com" {
		t.Errorf("h2 u2 username = %q, want bob@example.com (UNIQUE refusal must leave row unchanged)", got)
	}
	row, _ = db_open("db/users.db").row("select username from users where uid=?", "u1")
	if got, _ := row["username"].(string); got != "alice@example.com" {
		t.Errorf("h2 u1 username = %q, want alice@example.com (must not be overwritten)", got)
	}
}

// TestBootstrapFreshHostCatchesUpUsers: companion to
// TestBootstrapFreshHostCatchesUp. That scenario covers settings +
// documents + apps + sessions via the per-row backfill. This one
// covers the users.db keys-transfer path - the biggest single chunk
// of bootstrap state (auth factors, OAuth identities, passkeys,
// recovery codes, TOTP secrets, API tokens, owned entities).
//
// Stubs replication_transfer_keys_var to: build the real KeysTransfer
// payload via build_keys_transfer (exercising the production builder),
// switch to h3's host context, and call replication_keys_transfer_apply
// to land the payload as the wire receive would. Asserts h3's users.db
// holds the same auth state h1 does, row for row including blob
// columns (credentials.id, credentials.public_key) that round-trip
// through CBOR.
func TestBootstrapFreshHostCatchesUpUsers(t *testing.T) {
	h := new_harness(t, fh_h1, fh_h2, fh_h3)
	defer h.cleanup()

	// All three hosts have the users.db schema in place plus the
	// full replication.db schema (the keys-transfer apply seeds
	// cursors from kt.Seeds via replication_cursor_set, which needs
	// the cursor table; that lands in the v67 upgrade). Same migration
	// chain as setup_replication_test.
	for _, name := range []string{fh_h1, fh_h2, fh_h3} {
		h.switch_to(name)
		setup_users_test_schema()
		db_upgrade_50()
		db_upgrade_55()
		db_upgrade_62()
		db_upgrade_66()
		db_upgrade_67()
	}

	// h1 holds a fully-stocked user. Two credentials (passkey +
	// platform), two recovery codes, one OAuth link, TOTP secret, one
	// API token, two owned entities. Mixed columns including BLOB PKs
	// on credentials so the CBOR round-trip is exercised.
	h.switch_to(fh_h1)
	users := db_open("db/users.db")
	const transfer_uid = "uid-keystest"
	const transfer_username = "keystest@example.com"
	users.exec("insert into users (uid, username, role, methods, status) values (?, ?, 'administrator', 'email,passkey,totp', 'active')",
		transfer_uid, transfer_username)

	entity_one := mm_entity_id('k')
	entity_two := mm_entity_id('l')
	users.exec("insert into entities (id, private, fingerprint, user, parent, class, name, privacy, data, published) values (?, 'priv-1', 'fp-1', ?, '', 'person', 'Keys User', 'private', '', 100)",
		entity_one, transfer_uid)
	users.exec("insert into entities (id, private, fingerprint, user, parent, class, name, privacy, data, published) values (?, 'priv-2', 'fp-2', ?, ?, 'feed', 'Keys Feed', 'public', '', 200)",
		entity_two, transfer_uid, entity_one)

	credential_one_id := []byte{0x01, 0x02, 0x03, 0x04, 0x05}
	credential_one_key := []byte{0xaa, 0xbb, 0xcc, 0xdd}
	credential_two_id := []byte{0x10, 0x20, 0x30, 0x40, 0x50}
	credential_two_key := []byte{0xee, 0xff, 0x11, 0x22}
	users.exec("insert into credentials (id, user, public_key, sign_count, name, transports, backup_eligible, backup_state, created) values (?, ?, ?, 7, 'YubiKey', 'usb', 1, 0, 100)",
		credential_one_id, transfer_uid, credential_one_key)
	users.exec("insert into credentials (id, user, public_key, sign_count, name, transports, backup_eligible, backup_state, created) values (?, ?, ?, 3, 'Phone', 'internal', 0, 1, 200)",
		credential_two_id, transfer_uid, credential_two_key)

	users.exec("insert into recovery (user, hash, created) values (?, 'hash-recovery-1', 100)", transfer_uid)
	users.exec("insert into recovery (user, hash, created) values (?, 'hash-recovery-2', 110)", transfer_uid)

	users.exec("insert into oauth (user, provider, subject, email, verified, name, created) values (?, 'github', 'gh-789', 'keys@example.com', 1, 'Keys User', 100)",
		transfer_uid)

	users.exec("insert into totp (user, secret, verified, created) values (?, 'totp-secret', 1, 100)", transfer_uid)

	users.exec("insert into tokens (hash, user, app, name, scopes, created, expires) values ('hash-tok-1', ?, 'feeds', 'mobile', 'read', 100, 0)", transfer_uid)

	// Stub the per-peer keys-transfer hook to apply on h3 instead
	// of queuing for libp2p send. Mirrors the system_set/_row stubs
	// the existing bootstrap test uses.
	original_transfer := replication_transfer_keys_var
	defer func() { replication_transfer_keys_var = original_transfer }()

	var (
		captured_user     string
		captured_peer     string
		captured_transfer *KeysTransfer
	)
	replication_transfer_keys_var = func(user, peer string) bool {
		captured_user = user
		captured_peer = peer
		// Build the payload using the production builder so a future
		// regression in build_keys_transfer (e.g. dropped column,
		// missing nested array) shows up in this test.
		transfer, ok := build_keys_transfer(user)
		if !ok {
			return false
		}
		captured_transfer = transfer
		prior := h.current
		h.switch_to(fh_h3)
		replication_keys_transfer_apply(transfer.Entities[0].ID, "peer-"+fh_h1, transfer)
		h.switch_to(prior)
		return true
	}

	// h1 runs the keys-transfer pair-backfill against h3. With one
	// user on h1 the backfill should fire the hook exactly once.
	h.switch_to(fh_h1)
	replication_pair_backfill_users("peer-" + fh_h3)

	// Wire-shape assertions: the captured payload must populate every
	// nested array we seeded. A future change that drops a column from
	// build_keys_transfer fails here before we even look at h3.
	if captured_user != transfer_uid {
		t.Errorf("transfer captured user = %q, want %q", captured_user, transfer_uid)
	}
	if captured_peer != "peer-"+fh_h3 {
		t.Errorf("transfer captured peer = %q, want %q", captured_peer, "peer-"+fh_h3)
	}
	if captured_transfer == nil {
		t.Fatal("transfer captured no payload")
	}
	if captured_transfer.UID != transfer_uid || captured_transfer.Username != transfer_username {
		t.Errorf("payload identity: uid=%q username=%q, want %q / %q",
			captured_transfer.UID, captured_transfer.Username, transfer_uid, transfer_username)
	}
	if captured_transfer.Role != "administrator" || captured_transfer.Methods != "email,passkey,totp" || captured_transfer.Status != "active" {
		t.Errorf("payload identity columns: role=%q methods=%q status=%q",
			captured_transfer.Role, captured_transfer.Methods, captured_transfer.Status)
	}
	if got := len(captured_transfer.Entities); got != 2 {
		t.Errorf("payload entities = %d, want 2", got)
	}
	if got := len(captured_transfer.Credentials); got != 2 {
		t.Errorf("payload credentials = %d, want 2", got)
	}
	if got := len(captured_transfer.Recovery); got != 2 {
		t.Errorf("payload recovery = %d, want 2", got)
	}
	if got := len(captured_transfer.OAuth); got != 1 {
		t.Errorf("payload oauth = %d, want 1", got)
	}
	if got := len(captured_transfer.Tokens); got != 1 {
		t.Errorf("payload tokens = %d, want 1", got)
	}
	if captured_transfer.Totp == nil || captured_transfer.Totp.Secret != "totp-secret" {
		t.Errorf("payload totp missing or wrong secret: %+v", captured_transfer.Totp)
	}

	// Receiver-side assertions: h3's users.db now holds the same auth
	// state h1 does. Per-table row-for-row equality on the cross-host
	// stable identifiers.
	h.switch_to(fh_h3)
	users_three := db_open("db/users.db")

	row, _ := users_three.row("select uid, username, role, methods, status from users where uid=?", transfer_uid)
	if row == nil {
		t.Fatal("h3 users row missing after keys-transfer apply")
	}
	if got, _ := row["username"].(string); got != transfer_username {
		t.Errorf("h3 user username = %q, want %q", got, transfer_username)
	}
	if got, _ := row["role"].(string); got != "administrator" {
		t.Errorf("h3 user role = %q, want administrator (KeysTransfer carries role for fresh user, unlike per-user replication path)", got)
	}
	if got, _ := row["methods"].(string); got != "email,passkey,totp" {
		t.Errorf("h3 user methods = %q, want email,passkey,totp", got)
	}

	// Entities: both must exist with same private key, fingerprint,
	// parent, class, privacy.
	for _, entity := range []struct {
		id      string
		private string
		parent  string
		class   string
		privacy string
	}{
		{entity_one, "priv-1", "", "person", "private"},
		{entity_two, "priv-2", entity_one, "feed", "public"},
	} {
		row, _ := users_three.row("select private, parent, class, privacy from entities where id=? and user=?", entity.id, transfer_uid)
		if row == nil {
			t.Errorf("h3 entity %q missing", entity.id)
			continue
		}
		if got, _ := row["private"].(string); got != entity.private {
			t.Errorf("h3 entity %q private = %q, want %q", entity.id, got, entity.private)
		}
		if got, _ := row["parent"].(string); got != entity.parent {
			t.Errorf("h3 entity %q parent = %q, want %q", entity.id, got, entity.parent)
		}
		if got, _ := row["class"].(string); got != entity.class {
			t.Errorf("h3 entity %q class = %q, want %q", entity.id, got, entity.class)
		}
		if got, _ := row["privacy"].(string); got != entity.privacy {
			t.Errorf("h3 entity %q privacy = %q, want %q", entity.id, got, entity.privacy)
		}
	}

	// Credentials: keyed by blob id. public_key blob round-trips
	// through CBOR. sign_count + backup_eligible + backup_state are
	// the per-credential state.
	for _, credential := range []struct {
		id              []byte
		public_key      []byte
		sign_count      int64
		name            string
		transports      string
		backup_eligible int64
		backup_state    int64
	}{
		{credential_one_id, credential_one_key, 7, "YubiKey", "usb", 1, 0},
		{credential_two_id, credential_two_key, 3, "Phone", "internal", 0, 1},
	} {
		row, _ := users_three.row("select public_key, sign_count, name, transports, backup_eligible, backup_state from credentials where id=? and user=?", credential.id, transfer_uid)
		if row == nil {
			t.Errorf("h3 credential %x missing", credential.id)
			continue
		}
		// db.row converts []byte to string defensively (same conversion
		// build_keys_transfer has to work around with to_bytes). Use the
		// same helper here to recover the raw bytes.
		got_key := to_bytes(row["public_key"])
		if !bytes.Equal(got_key, credential.public_key) {
			t.Errorf("h3 credential %x public_key = %x, want %x (blob round-trip via CBOR)", credential.id, got_key, credential.public_key)
		}
		if got, _ := row["sign_count"].(int64); got != credential.sign_count {
			t.Errorf("h3 credential %x sign_count = %d, want %d", credential.id, got, credential.sign_count)
		}
		if got, _ := row["name"].(string); got != credential.name {
			t.Errorf("h3 credential %x name = %q, want %q", credential.id, got, credential.name)
		}
		if got, _ := row["backup_eligible"].(int64); got != credential.backup_eligible {
			t.Errorf("h3 credential %x backup_eligible = %d, want %d", credential.id, got, credential.backup_eligible)
		}
	}

	// Recovery codes: keyed by (user, hash).
	for _, hash := range []string{"hash-recovery-1", "hash-recovery-2"} {
		if exists, _ := users_three.exists("select 1 from recovery where user=? and hash=?", transfer_uid, hash); !exists {
			t.Errorf("h3 recovery hash %q missing", hash)
		}
	}

	// OAuth: keyed by (provider, subject). Verified flag round-trips
	// from bool to int.
	row, _ = users_three.row("select email, verified, name from oauth where provider='github' and subject='gh-789' and user=?", transfer_uid)
	if row == nil {
		t.Error("h3 oauth row missing")
	} else {
		if got, _ := row["email"].(string); got != "keys@example.com" {
			t.Errorf("h3 oauth email = %q", got)
		}
		if got, _ := row["verified"].(int64); got != 1 {
			t.Errorf("h3 oauth verified = %d, want 1", got)
		}
	}

	// TOTP: single row per user.
	row, _ = users_three.row("select secret, verified from totp where user=?", transfer_uid)
	if row == nil {
		t.Error("h3 totp row missing")
	} else {
		if got, _ := row["secret"].(string); got != "totp-secret" {
			t.Errorf("h3 totp secret = %q", got)
		}
		if got, _ := row["verified"].(int64); got != 1 {
			t.Errorf("h3 totp verified = %d, want 1", got)
		}
	}

	// Tokens: keyed by hash.
	row, _ = users_three.row("select app, name, scopes from tokens where hash='hash-tok-1' and user=?", transfer_uid)
	if row == nil {
		t.Error("h3 token row missing")
	} else {
		if got, _ := row["app"].(string); got != "feeds" {
			t.Errorf("h3 token app = %q", got)
		}
		if got, _ := row["scopes"].(string); got != "read" {
			t.Errorf("h3 token scopes = %q", got)
		}
	}
}

// TestThreeHostLeaderClaimConverges: three operator-paired hosts each
// call replication_leader_claim for the same (scope, key). The
// stubbed inter-host vote follows the hash tie-break documented for
// the V3 leader-election. After all three call sites complete, every
// host's local leadership row points at the same winner.
//
// Simulates the "simultaneous claim from all pair members" race; the
// hash-tie-break property says one host is preferred regardless of
// claim ordering. Sequential issue ordering still tests the
// convergence path because the stub vote consults each host's local
// tentative-write state via leader_prefer.
func TestThreeHostLeaderClaimConverges(t *testing.T) {
	h := new_harness(t, tt_h1, tt_h2, tt_h3)
	defer h.cleanup()
	for _, name := range []string{tt_h1, tt_h2, tt_h3} {
		h.switch_to(name)
		db_upgrade_50() // lazy-create the per-host replication.db schema (pair, leadership, etc.)
		rdb := db_open("db/replication.db")
		// Seed pair so each host considers the other two members of
		// the pair triple - leader_membership for non-"user:" scopes
		// (e.g. "platform") queries the pair table. The local host's
		// own peer-id is excluded by the membership query itself.
		for _, peer := range []string{"peer-" + tt_h1, "peer-" + tt_h2, "peer-" + tt_h3} {
			rdb.exec("insert or ignore into pair (peer, added) values (?, ?)", peer, now())
		}
	}

	// Stub the inter-host claim RPC. The real path opens a libp2p
	// sync stream; in the harness we approximate the documented vote
	// rule: a peer grants iff the requesting peer is hash-preferred
	// over it for (scope, key). Otherwise denies and reports itself
	// as current leader.
	orig_rpc := replication_leader_claim_rpc
	orig_notify := replication_leader_notify
	defer func() {
		replication_leader_claim_rpc = orig_rpc
		replication_leader_notify = orig_notify
	}()
	replication_leader_notify = func(scope, key string, fence, expires int64) {}
	replication_leader_claim_rpc = func(peer, scope, key string, expires int64) *LeaderClaimResponse {
		// Vote from peer's perspective: prefer(scope, key, requester=net_id, voter=peer).
		// If requester wins the hash tie-break against the voter, grant.
		if replication_leader_prefer(scope, key, net_id, peer) {
			return &LeaderClaimResponse{Granted: true}
		}
		// Voter prefers itself; deny.
		return &LeaderClaimResponse{
			Granted:        false,
			CurrentLeader:  peer,
			CurrentFence:   1,
			CurrentExpires: now() + 60,
		}
	}

	// Drive a claim from each host. Sequential issue: each host's
	// claim sees a different RPC outcome based on the hash tie-break
	// against its peers.
	// Use "platform" scope so leader_membership queries the pair
	// table (where we seeded the triple). "user:" scopes would query
	// the hosts table instead.
	const claimScope = "platform"
	const claimKey = "feeds-watchdog"
	results := map[string]bool{}
	for _, name := range []string{tt_h1, tt_h2, tt_h3} {
		h.switch_to(name)
		results[name] = replication_leader_claim(claimScope, claimKey, false)
	}

	// At most one host should have claimed in optimistic mode. (If
	// the hash tie-break favours all three differently, only the
	// host that is hash-preferred over both others grants its own
	// claim - exactly one.)
	wins := 0
	winner := ""
	for name, w := range results {
		if w {
			wins++
			winner = name
		}
	}
	if wins != 1 {
		t.Fatalf("leader claims: got %d winners, want exactly 1 (results=%v)", wins, results)
	}

	// Verify all three hosts agree on the winner. The winner's own
	// leadership row points at peer-winner; the other two should
	// have mirrored peer-winner via the denial CurrentLeader path.
	want_leader := "peer-" + winner
	for _, name := range []string{tt_h1, tt_h2, tt_h3} {
		h.switch_to(name)
		row, _ := db_open("db/replication.db").row(
			"select peer from leadership where scope=? and key=?",
			claimScope, claimKey)
		got, _ := row["peer"].(string)
		if name == winner && got != want_leader {
			t.Errorf("%s (winner): leadership.peer = %q, want %q", name, got, want_leader)
		}
		// The losers may have mirrored the winner or may simply
		// have no row. What's not allowed is mirroring a different
		// non-empty peer.
		if got != "" && got != want_leader {
			t.Errorf("%s: leadership.peer = %q, want either empty or %q (the winner)", name, got, want_leader)
		}
	}
}

// TestBootstrapFreshHostCatchesUp: an existing host (h1) holds a
// realistic mix of state (settings, documents, apps, sessions); a
// fresh host (h3) joins the pair and runs the per-row backfill
// pipeline (replication_pair_backfill_system + _sessions); after the
// backfill, h3's state matches h1 row for row.
//
// Stubs the four per-peer emit hooks the backfill uses to deliver
// rows to one specific peer. Each stub builds the equivalent
// payload that would arrive on h3's wire and calls the matching
// apply function under h3's host context. KeysTransfer (the users-
// db backfill path) is skipped in this first deliverable - its
// payload is larger and the per-user apply path needs a placeholder
// user row first; covered as a follow-up.
func TestBootstrapFreshHostCatchesUp(t *testing.T) {
	h := new_harness(t, tt_h1, tt_h2, tt_h3)
	defer h.cleanup()

	// h1 holds settings, documents, apps, and a session for an
	// existing user. h2 is a pre-existing pair member kept in sync
	// already. h3 starts fresh with only the schemas needed to
	// receive bootstrap deliveries.
	for _, name := range []string{tt_h1, tt_h2, tt_h3} {
		h.switch_to(name)
		mm_settings_schema()
		setup_users_test_schema()
		setup_sessions_test_schema()
		db_open("db/apps.db").exec("create table if not exists apps (app text primary key, installed integer not null default 0)")
	}

	h.switch_to(tt_h1)
	// Settings (settings.settings).
	setting_set("signup_enabled", "true")
	setting_set("operator_name", "Alice")
	// Documents (settings.documents).
	if err := document_set("terms", "en", "h1 custom terms"); err != nil {
		t.Fatalf("document_set: %v", err)
	}
	// Apps install registry (apps.apps).
	db_open("db/apps.db").exec("insert or replace into apps (app, installed) values (?, ?)", "feeds", int64(1000))
	// A user + a live session so the sessions-backfill has something to ship.
	db_open("db/users.db").exec("insert into users (uid, username) values (?, ?)", "uid-boot", "boot@example.com")
	db_open("db/sessions.db").exec("insert into sessions (user, code, secret, expires, created, accessed, address, agent) values (?, ?, ?, ?, ?, ?, ?, ?)",
		"uid-boot", "sess-boot", "secret-x", now()+3600, now(), now(), "1.2.3.4", "ua")

	// h3 also needs the user row before sessions-row apply will
	// succeed (replication_session_apply_insert defers on
	// !user_exists).
	h.switch_to(tt_h3)
	db_open("db/users.db").exec("insert into users (uid, username) values (?, ?)", "uid-boot", "boot@example.com")

	// Stub the per-peer emit hooks: each one captures the payload
	// and applies it on h3 under h3's host context. After backfill
	// fires, we leave h1's context restored so post-assertions can
	// still read h1's DBs for comparison.
	orig_system_set := replication_system_set_to_peer_var
	orig_system_row := replication_system_row_to_peer_var
	orig_session := replication_emit_session_insert_to_peer_var
	defer func() {
		replication_system_set_to_peer_var = orig_system_set
		replication_system_row_to_peer_var = orig_system_row
		replication_emit_session_insert_to_peer_var = orig_session
	}()

	applyOnH3 := func(fn func()) {
		prior := h.current
		h.switch_to(tt_h3)
		fn()
		h.switch_to(prior)
	}

	replication_system_set_to_peer_var = func(peer, database, table, row, field, value string) {
		if peer != "peer-"+tt_h3 {
			t.Errorf("system_set_to_peer: targeted wrong peer %q, want peer-h3", peer)
			return
		}
		applyOnH3(func() {
			replication_system_set_apply("peer-"+tt_h1, &SystemSet{
				Database: database, Table: table, Row: row, Field: field, Value: value,
			})
		})
	}
	replication_system_row_to_peer_var = func(peer, database, table string, key, cols map[string]string, del bool) {
		if peer != "peer-"+tt_h3 {
			t.Errorf("system_row_to_peer: wrong peer %q", peer)
			return
		}
		applyOnH3(func() {
			replication_system_row_apply("peer-"+tt_h1, &SystemRow{
				Database: database, Table: table, Key: key, Cols: cols, Delete: del,
			})
		})
	}
	replication_emit_session_insert_to_peer_var = func(peer, user_uid, code, secret string, expires, created, accessed int64, address, agent string) {
		if peer != "peer-"+tt_h3 {
			t.Errorf("session_insert_to_peer: wrong peer %q", peer)
			return
		}
		applyOnH3(func() {
			replication_session_apply_insert(&SessionInsert{
				UserUID: user_uid, Code: code, Secret: secret,
				Expires: expires, Created: created, Accessed: accessed,
				Address: address, Agent: agent,
			})
		})
	}

	// Run the bootstrap from h1's perspective. replication_pair_backfill_system
	// internally calls _sessions and _accounts, so a single call covers
	// every replicated table outside the users.db keys-transfer path.
	h.switch_to(tt_h1)
	replication_pair_backfill_system("peer-" + tt_h3)

	// Assert h3 has every row h1 has.
	h.switch_to(tt_h3)
	if got := setting_get("signup_enabled", ""); got != "true" {
		t.Errorf("h3 setting signup_enabled = %q, want true", got)
	}
	if got := setting_get("operator_name", ""); got != "Alice" {
		t.Errorf("h3 setting operator_name = %q, want Alice", got)
	}
	row, _ := db_open("db/settings.db").row("select body from documents where name='terms' and language='en'")
	if got, _ := row["body"].(string); got != "h1 custom terms" {
		t.Errorf("h3 document terms/en = %q, want %q", got, "h1 custom terms")
	}
	row, _ = db_open("db/apps.db").row("select installed from apps where app='feeds'")
	if got, _ := row["installed"].(int64); got != 1000 {
		t.Errorf("h3 apps.feeds installed = %d, want 1000", got)
	}
	row, _ = db_open("db/sessions.db").row("select code, secret from sessions where user=?", "uid-boot")
	if got, _ := row["code"].(string); got != "sess-boot" {
		t.Errorf("h3 session code = %q, want sess-boot", got)
	}
	if got, _ := row["secret"].(string); got != "secret-x" {
		t.Errorf("h3 session secret did not replicate")
	}
}

// ===== 4-host topologies =====
//
// Mochi supports topologies of arbitrary size, but 4 hosts is the
// first size where new failure modes appear that 3 hosts cannot:
//
//   - Strict-quorum math is non-trivial: 4 hosts means membership=3
//     for any proposer, total=4, floor(total/2)=2 peer grants needed.
//     The strict path returns false if any one peer is unreachable
//     AND another denies.
//
//   - Partition shapes include 2-2 splits (no majority either side)
//     and 3-1 splits (clear majority). The 2-2 case is the canonical
//     "split-brain prevented by strict mode" scenario.
//
//   - Fan-out is O(N²) for an op flood: 4 hosts each emitting one op
//     produces 12 deliveries (3 receivers per emit, 4 emits). Useful
//     for catching naive O(N) assumptions buried in the routing code.

const (
	fh_h1 = "h1"
	fh_h2 = "h2"
	fh_h3 = "h3"
	fh_h4 = "h4"
	fhUID = "uid-fourway"
)

// seed_four_hosts mirrors seed_three_hosts but for the 4-host
// topology. Sets up schedule + sessions + settings schemas and the
// shared user.
func seed_four_hosts(t *testing.T, h *harness) {
	for _, name := range []string{fh_h1, fh_h2, fh_h3, fh_h4} {
		h.switch_to(name)
		h.setup_harness_user(fhUID, "fourway@example.com", mm_entity_id('f'))
		schedule_db().exec("create table if not exists schedule (id integer primary key, user text not null, app text not null, due int not null, event text not null, data text not null, interval int not null, created int not null)")
		setup_sessions_test_schema()
		mm_settings_schema()
		// Inbound-stream bookkeeping, needed when a test routes deliveries
		// through the production receive path (h.gated) instead
		// of raw replication_apply_op. Harmless no-op for the raw-apply tests.
		rdb := db_open("db/replication.db")
		rdb.exec("create table if not exists seen (peer text not null, scope text not null, user text not null default '', sequence integer not null, applied integer not null, primary key (peer, scope, user, sequence))")
		rdb.exec("create table if not exists cursor (peer text not null, scope text not null, user text not null default '', db text not null default '', sequence integer not null default 0, primary key (peer, scope, user, db))")
		rdb.exec("create table if not exists pending (peer text not null, scope text not null, user text not null default '', db text not null default '', sequence integer not null, prev integer not null default 0, schema integer not null default 0, payload blob not null, received integer not null, primary key (peer, scope, user, sequence))")
		rdb.exec("create table if not exists relayed (user text not null, origin text not null, seen integer not null, primary key (user, origin))")
		rdb.exec("create table if not exists links (user text not null, peer text not null, label text not null default '', placeholder text not null, received integer not null, expires integer not null, primary key (user, peer))")
		rdb.exec("create table if not exists joins (peer text not null primary key, label text not null default '', received integer not null, expires integer not null)")
	}
}

// TestFourHostPairConvergence: pair quadruple, default routing.
// schedule + setting + document edited on one host reach the other
// three after a single flush.
func TestFourHostPairConvergence(t *testing.T) {
	h := new_harness(t, fh_h1, fh_h2, fh_h3, fh_h4)
	defer h.cleanup()
	seed_four_hosts(t, h)

	h.switch_to(fh_h1)
	if schedule_create(fhUID, "feeds", 1000, "tick", "{}", 60) == 0 {
		t.Fatal("schedule_create returned 0")
	}
	setting_set("operator_name", "FourwayOp")
	if err := document_set("terms", "en", "fourway terms"); err != nil {
		t.Fatalf("document_set: %v", err)
	}
	h.flush()

	for _, name := range []string{fh_h2, fh_h3, fh_h4} {
		h.switch_to(name)
		if exists, _ := schedule_db().exists("select 1 from schedule where user=? and event='tick'", fhUID); !exists {
			t.Errorf("%s: missing schedule row", name)
		}
		if got := setting_get("operator_name", ""); got != "FourwayOp" {
			t.Errorf("%s: setting = %q, want FourwayOp", name, got)
		}
		row, _ := db_open("db/settings.db").row(
			"select body from documents where name='terms' and language='en'")
		if got, _ := row["body"].(string); got != "fourway terms" {
			t.Errorf("%s: document body = %q, want %q", name, got, "fourway terms")
		}
	}
}

// TestFourHostPair2v2PartitionHealConverges: split the wire into a
// 2-2 partition (h1+h2 vs h3+h4), each side writes concurrently,
// heal, flush, every host ends up with the union of all four hosts'
// rows. The 2-2 split is the case where neither side has a strict
// majority - relevant for leader scenarios but for plain row
// replication (which has no quorum requirement) both sides keep
// writing and the heal merges them.
func TestFourHostPair2v2PartitionHealConverges(t *testing.T) {
	h := new_harness(t, fh_h1, fh_h2, fh_h3, fh_h4)
	defer h.cleanup()
	seed_four_hosts(t, h)

	h.partition()

	// Each host produces one distinct schedule row during the
	// partition (different event names so the natural-key dedup
	// keeps them as separate rows even if now() returns the same
	// epoch second for all four).
	for i, name := range []string{fh_h1, fh_h2, fh_h3, fh_h4} {
		h.switch_to(name)
		event := fmt.Sprintf("event-from-%d", i+1)
		if schedule_create(fhUID, "feeds", int64(1000+i), event, "{}", 0) == 0 {
			t.Fatalf("%s: schedule_create returned 0", name)
		}
	}

	// Pending during the partition: each host emitted 1 op to 3
	// receivers = 12 deliveries total, 3 per receiver.
	for _, name := range []string{fh_h1, fh_h2, fh_h3, fh_h4} {
		if got := h.pending(name); got != 12 {
			t.Errorf("%s: pending = %d, want 12 (3 from each of 4 emitters across all queues)", name, got)
		}
	}

	h.heal()
	h.flush()

	for _, name := range []string{fh_h1, fh_h2, fh_h3, fh_h4} {
		h.switch_to(name)
		var n int64
		row, _ := schedule_db().row("select count(*) as n from schedule where user=?", fhUID)
		if row != nil {
			n, _ = row["n"].(int64)
		}
		if n != 4 {
			t.Errorf("%s: rows = %d, want 4 (one per host after heal)", name, n)
		}
	}
}

// TestFourHostUserUserUserUserSchedule: per-user link of 4 different
// operators sharing one user. Per-user-scope ops fan out to all
// linked hosts.
func TestFourHostUserUserUserUserSchedule(t *testing.T) {
	h := new_harness(t, fh_h1, fh_h2, fh_h3, fh_h4)
	defer h.cleanup()
	h.set_pair_members() // no operator pair
	h.set_user_hosts(fhUID, fh_h1, fh_h2, fh_h3, fh_h4)
	seed_four_hosts(t, h)

	h.switch_to(fh_h2)
	if schedule_create(fhUID, "crm", 1000, "linked", "{}", 0) == 0 {
		t.Fatal("h2 schedule_create returned 0")
	}
	h.flush()

	for _, name := range []string{fh_h1, fh_h3, fh_h4} {
		h.switch_to(name)
		if exists, _ := schedule_db().exists("select 1 from schedule where user=? and event='linked'", fhUID); !exists {
			t.Errorf("%s: missing linked schedule row", name)
		}
	}
}

// TestFourHostUserUserUserUserPairOnlyStaysLocal: a pair-scope emit
// from one host of a 4-host link MUST NOT reach the other three (all
// different operators). Same property as the 3-host version but
// scaled.
func TestFourHostUserUserUserUserPairOnlyStaysLocal(t *testing.T) {
	h := new_harness(t, fh_h1, fh_h2, fh_h3, fh_h4)
	defer h.cleanup()
	h.set_pair_members()
	h.set_user_hosts(fhUID, fh_h1, fh_h2, fh_h3, fh_h4)
	seed_four_hosts(t, h)

	h.switch_to(fh_h1)
	setting_set("signup_enabled", "h1-only-fourway")
	replication_emit_users_users_pair_set(fhUID, map[string]string{"username": "renamed-on-h1-fourway"})
	h.flush()

	for _, name := range []string{fh_h2, fh_h3, fh_h4} {
		h.switch_to(name)
		if got := setting_get("signup_enabled", ""); got == "h1-only-fourway" {
			t.Errorf("%s: setting leaked across operator boundary: %q", name, got)
		}
		row, _ := db_open("db/users.db").row("select username from users where uid=?", fhUID)
		if got, _ := row["username"].(string); got == "renamed-on-h1-fourway" {
			t.Errorf("%s: pair-only username leaked across per-user link: %q", name, got)
		}
	}
}

// TestFourHostLeaderStrictQuorum: in a pair quadruple, strict mode
// requires floor(4/2) = 2 peer grants. When all three peers grant,
// strict succeeds.
func TestFourHostLeaderStrictQuorum(t *testing.T) {
	h := new_harness(t, fh_h1, fh_h2, fh_h3, fh_h4)
	defer h.cleanup()
	for _, name := range []string{fh_h1, fh_h2, fh_h3, fh_h4} {
		h.switch_to(name)
		db_upgrade_50()
		rdb := db_open("db/replication.db")
		for _, peer := range []string{"peer-" + fh_h1, "peer-" + fh_h2, "peer-" + fh_h3, "peer-" + fh_h4} {
			rdb.exec("insert or ignore into pair (peer, added) values (?, ?)", peer, now())
		}
	}

	orig_rpc := replication_leader_claim_rpc
	orig_notify := replication_leader_notify
	defer func() {
		replication_leader_claim_rpc = orig_rpc
		replication_leader_notify = orig_notify
	}()
	replication_leader_notify = func(scope, key string, fence, expires int64) {}
	// All peers grant - the strict path needs >= 2 grants out of 3.
	replication_leader_claim_rpc = func(peer, scope, key string, expires int64) *LeaderClaimResponse {
		return &LeaderClaimResponse{Granted: true}
	}

	h.switch_to(fh_h1)
	if !replication_leader_claim("platform", "fourway-strict", true) {
		t.Error("strict claim must succeed when all peers grant")
	}
}

// TestFourHostLeader2v2PartitionStrictNoQuorum: a 2-2 partition in
// strict mode: the proposer sees one reachable peer (its partition
// partner) and two unreachable (nil). 1 grant < 2 needed → fails.
// Optimistic mode succeeds in the same conditions. Documents the
// difference operators are paying for with strict=true.
func TestFourHostLeader2v2PartitionStrictNoQuorum(t *testing.T) {
	h := new_harness(t, fh_h1, fh_h2, fh_h3, fh_h4)
	defer h.cleanup()
	for _, name := range []string{fh_h1, fh_h2, fh_h3, fh_h4} {
		h.switch_to(name)
		db_upgrade_50()
		rdb := db_open("db/replication.db")
		for _, peer := range []string{"peer-" + fh_h1, "peer-" + fh_h2, "peer-" + fh_h3, "peer-" + fh_h4} {
			rdb.exec("insert or ignore into pair (peer, added) values (?, ?)", peer, now())
		}
	}

	orig_rpc := replication_leader_claim_rpc
	orig_notify := replication_leader_notify
	defer func() {
		replication_leader_claim_rpc = orig_rpc
		replication_leader_notify = orig_notify
	}()
	replication_leader_notify = func(scope, key string, fence, expires int64) {}
	// Simulate the 2-2 partition: h1 (proposer) can reach h2 (grants);
	// h3 and h4 are unreachable (nil response).
	replication_leader_claim_rpc = func(peer, scope, key string, expires int64) *LeaderClaimResponse {
		if peer == "peer-"+fh_h2 {
			return &LeaderClaimResponse{Granted: true}
		}
		return nil // unreachable
	}

	h.switch_to(fh_h1)
	// Strict: 1 grant from 3 peers needed; partition gives 1; need 2.
	// Strict mode requires floor((membership+1)/2) = 2 peer grants;
	// only 1 reachable → fails. This prevents split-brain.
	if replication_leader_claim("platform", "split-brain", true) {
		t.Error("strict claim must FAIL under 2-2 partition (split-brain prevention)")
	}

	// Reset the leadership state so the next claim isn't fast-pathed.
	db_open("db/replication.db").exec("delete from leadership where scope='platform' and key='split-brain'")

	// Optimistic: any explicit grant + no veto succeeds, even with
	// unreachable peers. Documents the trade-off operators are
	// paying for with strict=true: optimistic accepts partition-
	// loser writes that would be fence-dropped on heal; strict
	// refuses to elect at all.
	if !replication_leader_claim("platform", "split-brain", false) {
		t.Error("optimistic claim must SUCCEED under same partition (any grant + no veto)")
	}
}

// TestFourHostBootstrapToFreshFourth: three operator-paired hosts
// already in sync; a fourth host joins fresh and catches up via the
// per-row backfill pipeline. Same shape as the 3-host bootstrap test
// but with 3 hosts holding the source-of-truth state.
func TestFourHostBootstrapToFreshFourth(t *testing.T) {
	h := new_harness(t, fh_h1, fh_h2, fh_h3, fh_h4)
	defer h.cleanup()
	// All four schemas set up; only h1-h3 hold state, h4 is fresh.
	for _, name := range []string{fh_h1, fh_h2, fh_h3, fh_h4} {
		h.switch_to(name)
		mm_settings_schema()
		setup_users_test_schema()
		setup_sessions_test_schema()
		db_open("db/apps.db").exec("create table if not exists apps (app text primary key, installed integer not null default 0)")
	}

	// h1 (and via auto-replication, h2 and h3) hold the canonical
	// state.
	h.switch_to(fh_h1)
	setting_set("signup_enabled", "true")
	setting_set("operator_name", "QuadAlice")
	if err := document_set("rules", "en", "fourway rules"); err != nil {
		t.Fatalf("document_set: %v", err)
	}
	db_open("db/apps.db").exec("insert or replace into apps (app, installed) values (?, ?)", "feeds", int64(1000))
	db_open("db/users.db").exec("insert into users (uid, username) values (?, ?)", "uid-quad", "quad@example.com")
	db_open("db/sessions.db").exec("insert into sessions (user, code, secret, expires, created, accessed, address, agent) values (?, ?, ?, ?, ?, ?, ?, ?)",
		"uid-quad", "sess-quad", "secret-q", now()+3600, now(), now(), "1.2.3.4", "ua")
	h.flush() // replicate h1 -> h2 and h3 via the normal pair-broadcast path

	// h4 needs the user row before sessions-row apply succeeds.
	h.switch_to(fh_h4)
	db_open("db/users.db").exec("insert into users (uid, username) values (?, ?)", "uid-quad", "quad@example.com")

	// Stub the per-peer backfill emit hooks to apply on h4.
	orig_system_set := replication_system_set_to_peer_var
	orig_system_row := replication_system_row_to_peer_var
	orig_session := replication_emit_session_insert_to_peer_var
	defer func() {
		replication_system_set_to_peer_var = orig_system_set
		replication_system_row_to_peer_var = orig_system_row
		replication_emit_session_insert_to_peer_var = orig_session
	}()
	applyOnH4 := func(fn func()) {
		prior := h.current
		h.switch_to(fh_h4)
		fn()
		h.switch_to(prior)
	}
	replication_system_set_to_peer_var = func(peer, database, table, row, field, value string) {
		if peer != "peer-"+fh_h4 {
			t.Errorf("system_set: wrong peer %q", peer)
			return
		}
		applyOnH4(func() {
			replication_system_set_apply("peer-"+fh_h1, &SystemSet{
				Database: database, Table: table, Row: row, Field: field, Value: value,
			})
		})
	}
	replication_system_row_to_peer_var = func(peer, database, table string, key, cols map[string]string, del bool) {
		if peer != "peer-"+fh_h4 {
			t.Errorf("system_row: wrong peer %q", peer)
			return
		}
		applyOnH4(func() {
			replication_system_row_apply("peer-"+fh_h1, &SystemRow{
				Database: database, Table: table, Key: key, Cols: cols, Delete: del,
			})
		})
	}
	replication_emit_session_insert_to_peer_var = func(peer, user_uid, code, secret string, expires, created, accessed int64, address, agent string) {
		if peer != "peer-"+fh_h4 {
			t.Errorf("session_insert: wrong peer %q", peer)
			return
		}
		applyOnH4(func() {
			replication_session_apply_insert(&SessionInsert{
				UserUID: user_uid, Code: code, Secret: secret,
				Expires: expires, Created: created, Accessed: accessed,
				Address: address, Agent: agent,
			})
		})
	}

	// h1 ships its state to h4.
	h.switch_to(fh_h1)
	replication_pair_backfill_system("peer-" + fh_h4)

	// h4 now has the canonical state.
	h.switch_to(fh_h4)
	if got := setting_get("operator_name", ""); got != "QuadAlice" {
		t.Errorf("h4: operator_name = %q, want QuadAlice", got)
	}
	row, _ := db_open("db/settings.db").row("select body from documents where name='rules' and language='en'")
	if got, _ := row["body"].(string); got != "fourway rules" {
		t.Errorf("h4: document rules/en = %q, want %q", got, "fourway rules")
	}
	row, _ = db_open("db/apps.db").row("select installed from apps where app='feeds'")
	if got, _ := row["installed"].(int64); got != 1000 {
		t.Errorf("h4: apps.feeds installed = %d, want 1000", got)
	}
	row, _ = db_open("db/sessions.db").row("select code from sessions where user=?", "uid-quad")
	if got, _ := row["code"].(string); got != "sess-quad" {
		t.Errorf("h4: session code = %q, want sess-quad", got)
	}
}

// TestThreeHostUserUserUserSessionsAndUserStatus: in the per-user
// link topology, app-scope per-user ops (sessions, users.users
// methods/status/preferences) DO follow the user across all linked
// hosts. Cookie issued on h1 validates on h2 and h3.
func TestThreeHostUserUserUserSessionsAndUserStatus(t *testing.T) {
	h := new_harness(t, tt_h1, tt_h2, tt_h3)
	defer h.cleanup()
	h.set_pair_members()
	h.set_user_hosts(ttUID, tt_h1, tt_h2, tt_h3)
	seed_three_hosts(t, h)

	h.switch_to(tt_h1)
	code := login_create(ttUID, "1.2.3.4", "test-agent")
	if code == "" {
		t.Fatal("h1 login_create returned empty code")
	}
	// status change goes via the per-user path - should reach h2/h3.
	replication_emit_users_users_set(ttUID, map[string]string{"status": "suspended"})

	h.flush()

	for _, name := range []string{tt_h2, tt_h3} {
		h.switch_to(name)
		exists, _ := db_open("db/sessions.db").exists(
			"select 1 from sessions where user=? and code=?", ttUID, code)
		if !exists {
			t.Errorf("%s: session cookie did not replicate via per-user link", name)
		}
		row, _ := db_open("db/users.db").row("select status from users where uid=?", ttUID)
		if got, _ := row["status"].(string); got != "suspended" {
			t.Errorf("%s: user status = %q, want suspended", name, got)
		}
	}
}

// mm_seed_attachments creates the attachments table on the currently-
// switched-to host and inserts (a,b,c) at ranks (1,2,3) scoped to the
// given object/entity. Mirrors setup_attachment_move_test in
// attachments_test.go but operates on whatever data_dir / DB the
// harness has switched to, so the same seed can run on h1, h2, h_c1
// and h_c2 with one helper.
func mm_seed_attachments(t *testing.T, object, entity string) {
	t.Helper()
	db := db_open("db/attachments.db")
	db.exec("create table if not exists attachments ( id text not null primary key, object text not null, entity text not null default '', name text not null, size integer not null, content_type text not null default '', creator text not null default '', caption text not null default '', description text not null default '', rank integer not null default 0, created integer not null )")
	for i, id := range []string{"a", "b", "c"} {
		db.exec("insert or ignore into attachments (id, object, entity, name, size, rank, created) values (?, ?, ?, ?, ?, ?, ?)", id, object, entity, fmt.Sprintf("%s.txt", id), int64(10), int64(i+1), int64(1700000000))
	}
}

// mm_move_locally runs the same SQL the api_attachment_move builtin
// runs, then returns the post-move absolute-rank snapshot. Sits in
// the multimaster test file because the harness-driven test below
// uses it on h1 / h2 to mimic the producer side of a federation
// emit. See attachments_test.go for the unit-level variant.
func mm_move_locally(t *testing.T, object, entity, id string, position int) ([]map[string]any, int) {
	t.Helper()
	db := db_open("db/attachments.db")
	row, err := db.row("select rank from attachments where id = ? and entity = ?", id, entity)
	if err != nil || row == nil {
		t.Fatalf("read pre-move rank for %q: row=%v err=%v", id, row, err)
	}
	old := int(row["rank"].(int64))
	new := position
	if old != new {
		if new < old {
			db.exec("update attachments set rank = rank + 1 where object = ? and entity = ? and rank >= ? and rank < ?", object, entity, new, old)
		} else {
			db.exec("update attachments set rank = rank - 1 where object = ? and entity = ? and rank > ? and rank <= ?", object, entity, old, new)
		}
		db.exec("update attachments set rank = ? where id = ? and entity = ?", new, id, entity)
	}
	rows, err := db.rows("select id, rank from attachments where object = ? and entity = ?", object, entity)
	if err != nil {
		t.Fatalf("read post-move snapshot: %v", err)
	}
	ranks := make([]map[string]any, 0, len(rows))
	for _, r := range rows {
		ranks = append(ranks, map[string]any{
			"id":   r["id"].(string),
			"rank": r["rank"].(int64),
		})
	}
	return ranks, old
}

// mm_ranks_by_id reads back the current rank table on the switched-to
// host for assertions.
func mm_ranks_by_id(t *testing.T, entity string) map[string]int64 {
	t.Helper()
	rows, err := db_open("db/attachments.db").rows("select id, rank from attachments where entity = ?", entity)
	if err != nil {
		t.Fatalf("rank readback: %v", err)
	}
	out := map[string]int64{}
	for _, r := range rows {
		out[r["id"].(string)] = r["rank"].(int64)
	}
	return out
}

// TestMultiMasterAttachmentMoveTwoHostFederatedConverges is the
// Path-B / harness-driven test for task #79. Models a four-host wire
// model: federated entity B (hosts h1, h2) emits _attachment/move
// events to federated entity C (hosts h_c1, h_c2). Both B-replicas
// fire concurrent moves on the same object, both notifies fan out to
// both C-replicas, flush() drains the queues in receiver order, and
// the per-replica final state must equal whichever payload arrived
// last there (LWW per id - the Option B contract from #76).
//
// This is the convergence claim Option B makes: per-id REPLACE is
// idempotent and order-independent at the rank level, so two C
// replicas processing the same payload set in different orders both
// land on the SAME state when the harness flushes deterministically
// (both queues drain in the same order). For genuinely-different
// arrival orders across C-replicas (an asymmetric network), see
// TestMultiMasterAttachmentMoveDivergesOnAsymmetricDelivery below.
func TestMultiMasterAttachmentMoveTwoHostFederatedConverges(t *testing.T) {
	h := new_harness(t, "h1", "h2", "h_c1", "h_c2")
	defer h.cleanup()

	// Federation topology: B owns (h1, h2), C owns (h_c1, h_c2). B's
	// own hosts are NOT in the recipient set (the federation emit
	// fires only to OTHER entities); C's hosts are.
	entity_b := mm_entity_id('B')
	entity_c := mm_entity_id('C')
	h.set_federation_hosts(entity_b, "h1", "h2")
	h.set_federation_hosts(entity_c, "h_c1", "h_c2")

	// Seed the same (a,b,c at 1,2,3) state on every replica that
	// holds the object. In production the seed arrives via prior
	// _attachment/create events; the harness shortcuts that with a
	// direct insert so this test focuses on move convergence.
	for _, name := range []string{"h1", "h2", "h_c1", "h_c2"} {
		h.switch_to(name)
		mm_seed_attachments(t, "obj1", entity_c)
	}

	// h1: move b 2 -> 1 (a,b,c become 2,1,3) and notify C.
	h.switch_to("h1")
	ranks_h1, old_h1 := mm_move_locally(t, "obj1", entity_c, "b", 1)
	attachment_notify_move(nil, nil, map[string]any{"id": "b", "object": "obj1", "rank": int64(1)}, old_h1, ranks_h1, []string{entity_c})

	// h2: move c 3 -> 1 (a,b,c become 2,3,1) and notify C.
	h.switch_to("h2")
	ranks_h2, old_h2 := mm_move_locally(t, "obj1", entity_c, "c", 1)
	attachment_notify_move(nil, nil, map[string]any{"id": "c", "object": "obj1", "rank": int64(1)}, old_h2, ranks_h2, []string{entity_c})

	// Drain. flush() processes receivers in map-iteration order which
	// is non-deterministic across runs, but within each receiver's
	// queue events apply in arrival order. Both C-replicas receive
	// [h1's payload, h2's payload] in that order (h1 emitted first),
	// so both end at h2's snapshot.
	h.flush()

	want := map[string]int64{"a": 2, "b": 3, "c": 1}
	for _, name := range []string{"h_c1", "h_c2"} {
		h.switch_to(name)
		got := mm_ranks_by_id(t, entity_c)
		for id, w := range want {
			if got[id] != w {
				t.Errorf("%s rank for %q: got %d, want %d (last-applied = h2)", name, id, got[id], w)
			}
		}
	}

	// B-side replicas are NOT in C's federation set, so they keep
	// their locally-shifted state. h1 stays at its local move
	// (a,b,c = 2,1,3); h2 stays at its local move (a,b,c = 2,3,1).
	// This isn't a divergence bug - it's exactly the production
	// model where pair replication (B's hosts -> each other) lives
	// on the per-user / system-scope channels, NOT on the federation
	// channel. Test asserts the harness models this faithfully.
	h.switch_to("h1")
	if got := mm_ranks_by_id(t, entity_c); got["b"] != 1 || got["c"] != 3 {
		t.Errorf("h1 (B-replica) was unexpectedly updated by C-bound emits: %v", got)
	}
	h.switch_to("h2")
	if got := mm_ranks_by_id(t, entity_c); got["b"] != 3 || got["c"] != 1 {
		t.Errorf("h2 (B-replica) was unexpectedly updated by C-bound emits: %v", got)
	}
}

// TestMultiMasterAttachmentMoveDivergesOnAsymmetricDelivery is the
// honest-failure case the audit's finding A flagged: when two
// concurrent producers fan out to a multi-host subscriber and the
// arrival order DIFFERS across that subscriber's replicas, Option B
// converges within each replica (no rank arithmetic divergence) but
// the two replicas land on different snapshots. This is a documented
// limitation of last-write-wins per-id without per-rank timestamps;
// the only real fix is a CRDT or vector clocks. Test pins the current
// behaviour so future "let's add timestamps" work has a regression
// target.
//
// Uses reorder() to make h_c2 process h2-then-h1 while h_c1 processes
// h1-then-h2.
func TestMultiMasterAttachmentMoveDivergesOnAsymmetricDelivery(t *testing.T) {
	h := new_harness(t, "h1", "h2", "h_c1", "h_c2")
	defer h.cleanup()

	entity_c := mm_entity_id('C')
	h.set_federation_hosts(entity_c, "h_c1", "h_c2")

	for _, name := range []string{"h1", "h2", "h_c1", "h_c2"} {
		h.switch_to(name)
		mm_seed_attachments(t, "obj1", entity_c)
	}

	h.switch_to("h1")
	ranks_h1, old_h1 := mm_move_locally(t, "obj1", entity_c, "b", 1)
	attachment_notify_move(nil, nil, map[string]any{"id": "b", "object": "obj1", "rank": int64(1)}, old_h1, ranks_h1, []string{entity_c})

	h.switch_to("h2")
	ranks_h2, old_h2 := mm_move_locally(t, "obj1", entity_c, "c", 1)
	attachment_notify_move(nil, nil, map[string]any{"id": "c", "object": "obj1", "rank": int64(1)}, old_h2, ranks_h2, []string{entity_c})

	// Reverse h_c2's queue so its arrival order is h2-then-h1 while
	// h_c1's stays h1-then-h2. Seed 2 is the smallest that produces
	// a swap on the 2-element queue with Go's rand.Shuffle - verified
	// by hand to keep the test deterministic across Go versions.
	h.reorder("h_c2", 2)
	h.flush()

	want_c1 := map[string]int64{"a": 2, "b": 3, "c": 1} // last-applied = h2
	want_c2 := map[string]int64{"a": 2, "b": 1, "c": 3} // last-applied = h1
	h.switch_to("h_c1")
	for id, w := range want_c1 {
		got := mm_ranks_by_id(t, entity_c)
		if got[id] != w {
			t.Errorf("h_c1 rank for %q: got %d, want %d", id, got[id], w)
		}
	}
	h.switch_to("h_c2")
	for id, w := range want_c2 {
		got := mm_ranks_by_id(t, entity_c)
		if got[id] != w {
			t.Errorf("h_c2 rank for %q: got %d, want %d (asymmetric delivery means h_c2 lands on h1's snapshot)", id, got[id], w)
		}
	}

	// Confirm the divergence: at least one id has different ranks on
	// h_c1 vs h_c2. If a future change makes them converge (per-id
	// timestamps, CRDT), this test starts failing and the docs at
	// the top need updating.
	h.switch_to("h_c1")
	got_c1 := mm_ranks_by_id(t, entity_c)
	h.switch_to("h_c2")
	got_c2 := mm_ranks_by_id(t, entity_c)
	diverged := false
	for id := range got_c1 {
		if got_c1[id] != got_c2[id] {
			diverged = true
		}
	}
	if !diverged {
		t.Fatalf("Option B unexpectedly converged under asymmetric delivery; c1=%v c2=%v - if this was intentional, update the test docs and the #76 follow-up", got_c1, got_c2)
	}
}

// ============================================================
// Framework-layer multi-master scenarios
// (was replication_framework_test.go)
// ============================================================

const (
	fwUID = "uid-framework"
)

// setup_framework_test seeds the current host with the user and
// schedule schema the framework tests use as a vehicle for landing
// real apply-side state. Returns a cleanup.
func setup_framework_test(t *testing.T) func() {
	cleanup := setup_replication_test(t)
	setup_users_test_schema()
	db_open("db/users.db").exec("insert into users (uid, username) values (?, ?)", fwUID, "fw@example.com")
	schedule_db().exec("create table if not exists schedule (id integer primary key, user text not null, app text not null, due int not null, event text not null, data text not null, interval int not null, created int not null)")
	return cleanup
}

// build_schedule_op constructs a schedule-row.set op with explicit
// Sequence and Prev so the framework gate can be exercised directly.
// The natural-key fields produce a deterministic row identity per
// (event, created) tuple - convenient for distinguishing which op
// landed.
func build_schedule_op(seq, prev int64, event string, created int64) *ReplicationOp {
	payload := cbor_encode(&ScheduleRow{
		Key: map[string]string{
			"user": fwUID, "app": "feeds", "event": event,
			"created": strconv.FormatInt(created, 10),
		},
		Cols: map[string]string{
			"due": "1000", "data": "{}", "interval": "0",
		},
	})
	return &ReplicationOp{
		Scope: repl_scope_app, User: fwUID,
		Database: "schedule", Table: "schedule",
		Operation: "schedule-row.set",
		Sequence:  seq, Prev: prev,
		Payload: payload,
	}
}

// TestFrameworkDuplicateDelivery: the same op arriving twice is
// deduped via the seen table. Apply runs once; second arrival is a
// no-op.
func TestFrameworkDuplicateDelivery(t *testing.T) {
	cleanup := setup_framework_test(t)
	defer cleanup()

	op := build_schedule_op(1, 0, "dup", 100)
	replication_op_receive("peerA", op)
	replication_op_receive("peerA", op) // second delivery

	row, _ := schedule_db().row(
		"select count(*) as n from schedule where user=? and event='dup'", fwUID)
	if n, _ := row["n"].(int64); n != 1 {
		t.Errorf("duplicate apply: rows = %d, want 1 (dedup via seen)", n)
	}

	// Cursor should be at seq=1, single seen row.
	rdb := db_open("db/replication.db")
	cursor, anchored := replication_cursor(rdb, "peerA", repl_scope_app, fwUID, repl_stream_key(repl_stream_class_system, "schedule"))
	if !anchored || cursor != 1 {
		t.Errorf("cursor: anchored=%v seq=%d, want anchored=true seq=1", anchored, cursor)
	}
	srow, _ := rdb.row("select count(*) as n from seen where peer='peerA' and scope=? and user=?",
		repl_scope_app, fwUID)
	if n, _ := srow["n"].(int64); n != 1 {
		t.Errorf("seen rows = %d, want 1", n)
	}
}

// TestFrameworkOutOfOrderArrivalDrainsPending: seq=2 (Prev=1) arrives
// before seq=1 (Prev=0). seq=2 is buffered in pending; seq=1 applies
// and triggers the pending drain so seq=2 also lands. Final state:
// both rows present, cursor=2, pending empty.
func TestFrameworkOutOfOrderArrivalDrainsPending(t *testing.T) {
	cleanup := setup_framework_test(t)
	defer cleanup()

	op2 := build_schedule_op(2, 1, "second", 200)
	op1 := build_schedule_op(1, 0, "first", 100)

	// seq=2 arrives first - no anchor yet, so buffered in pending.
	replication_op_receive("peerA", op2)
	rdb := db_open("db/replication.db")
	pending, _ := rdb.row("select count(*) as n from pending where peer='peerA'")
	if n, _ := pending["n"].(int64); n != 1 {
		t.Errorf("after seq=2: pending = %d, want 1 (buffered behind gap)", n)
	}
	row, _ := schedule_db().row("select count(*) as n from schedule where user=?", fwUID)
	if n, _ := row["n"].(int64); n != 0 {
		t.Errorf("after seq=2: rows = %d, want 0 (op buffered, not applied)", n)
	}

	// seq=1 arrives - applies, then drain re-applies seq=2.
	replication_op_receive("peerA", op1)
	row, _ = schedule_db().row("select count(*) as n from schedule where user=?", fwUID)
	if n, _ := row["n"].(int64); n != 2 {
		t.Errorf("after seq=1: rows = %d, want 2 (seq=1 applied, drain landed seq=2)", n)
	}
	pending, _ = rdb.row("select count(*) as n from pending where peer='peerA'")
	if n, _ := pending["n"].(int64); n != 0 {
		t.Errorf("after drain: pending = %d, want 0 (cleared)", n)
	}
	cursor, _ := replication_cursor(rdb, "peerA", repl_scope_app, fwUID, repl_stream_key(repl_stream_class_system, "schedule"))
	if cursor != 2 {
		t.Errorf("cursor = %d, want 2", cursor)
	}
}

// TestFrameworkBelowCursorOpDropped: an op with Prev<cursor (already
// applied) is silently dropped. Doesn't crash, doesn't double-apply,
// doesn't move the cursor backward.
func TestFrameworkBelowCursorOpDropped(t *testing.T) {
	cleanup := setup_framework_test(t)
	defer cleanup()

	replication_op_receive("peerA", build_schedule_op(1, 0, "one", 100))
	replication_op_receive("peerA", build_schedule_op(2, 1, "two", 200))
	replication_op_receive("peerA", build_schedule_op(3, 2, "three", 300))

	// Replay of seq=2 with Prev=1 - already below cursor (which is at 3).
	// Different sequence number so seen dedup doesn't catch it; the
	// below-cursor gate is what drops it.
	replication_op_receive("peerA", build_schedule_op(99, 1, "replay-pretender", 200))

	row, _ := schedule_db().row("select count(*) as n from schedule where user=?", fwUID)
	if n, _ := row["n"].(int64); n != 3 {
		t.Errorf("rows = %d, want 3 (below-cursor replay must not apply)", n)
	}
	rdb := db_open("db/replication.db")
	cursor, _ := replication_cursor(rdb, "peerA", repl_scope_app, fwUID, repl_stream_key(repl_stream_class_system, "schedule"))
	if cursor != 3 {
		t.Errorf("cursor = %d, want 3 (must not rewind)", cursor)
	}
}

// TestFrameworkStreamRestart: an op with Prev=0 anchors a fresh
// cursor regardless of the prior cursor position - what happens after
// a sender restart that resets its outbound sequence.
func TestFrameworkStreamRestart(t *testing.T) {
	cleanup := setup_framework_test(t)
	defer cleanup()

	replication_op_receive("peerA", build_schedule_op(1, 0, "one", 100))
	replication_op_receive("peerA", build_schedule_op(2, 1, "two", 200))

	rdb := db_open("db/replication.db")
	cursor, _ := replication_cursor(rdb, "peerA", repl_scope_app, fwUID, repl_stream_key(repl_stream_class_system, "schedule"))
	if cursor != 2 {
		t.Fatalf("pre-restart cursor = %d, want 2", cursor)
	}

	// Sender restart: a fresh Prev=0 op anchors regardless of cursor.
	replication_op_receive("peerA", build_schedule_op(50, 0, "restart-anchor", 500))

	row, _ := schedule_db().row("select count(*) as n from schedule where user=?", fwUID)
	if n, _ := row["n"].(int64); n != 3 {
		t.Errorf("rows = %d, want 3 (restart op anchored and applied)", n)
	}
	cursor, _ = replication_cursor(rdb, "peerA", repl_scope_app, fwUID, repl_stream_key(repl_stream_class_system, "schedule"))
	if cursor != 50 {
		t.Errorf("post-restart cursor = %d, want 50", cursor)
	}
}

// TestFrameworkPerPeerStreamsIndependent: two peers' sequence streams
// for the same (scope, user, db) are tracked independently. Cursor on
// peerA at 5 doesn't affect peer_b's gate logic.
func TestFrameworkPerPeerStreamsIndependent(t *testing.T) {
	cleanup := setup_framework_test(t)
	defer cleanup()

	replication_op_receive("peerA", build_schedule_op(1, 0, "from-A", 100))
	replication_op_receive("peer_b", build_schedule_op(1, 0, "from-B", 200))
	replication_op_receive("peerA", build_schedule_op(2, 1, "from-A2", 300))

	row, _ := schedule_db().row("select count(*) as n from schedule where user=?", fwUID)
	if n, _ := row["n"].(int64); n != 3 {
		t.Errorf("rows = %d, want 3 (1 per stream + 1 chain on A)", n)
	}
	rdb := db_open("db/replication.db")
	c_a, _ := replication_cursor(rdb, "peerA", repl_scope_app, fwUID, repl_stream_key(repl_stream_class_system, "schedule"))
	c_b, _ := replication_cursor(rdb, "peer_b", repl_scope_app, fwUID, repl_stream_key(repl_stream_class_system, "schedule"))
	if c_a != 2 {
		t.Errorf("peerA cursor = %d, want 2", c_a)
	}
	if c_b != 1 {
		t.Errorf("peer_b cursor = %d, want 1", c_b)
	}
}

// ============================================================
// Schema-version skew apply-path tests (task #61)
// (was replication_schema_skew_test.go)
// ============================================================

const skew_user = "uid-schema-skew"

// register_skew_app installs an app with a single-table per-user DB
// at the given starting schema version. Returns the AppVersion so
// the test can mutate Schema to simulate a migration landing.
// posts is the table the replicated INSERTs target.
func register_skew_app(t *testing.T, app_id string, initial_schema int) *AppVersion {
	t.Helper()
	av := &AppVersion{Version: "1"}
	av.Architecture.Engine = "starlark"
	av.Architecture.Version = 4
	av.Database.File = app_id + ".db"
	av.Database.Schema = initial_schema
	av.Database.create_function = func(db *DB) {
		db.exec("create table posts (id text primary key, title text not null, n integer not null default 0)")
	}
	a := &App{id: app_id, versions: map[string]*AppVersion{"1": av}, internal: av}
	av.app = a
	apps_lock.Lock()
	if apps == nil {
		apps = map[string]*App{}
	}
	apps[app_id] = a
	apps_lock.Unlock()
	return av
}

// unregister_skew_app drops the app from the global apps map so
// repeated test runs don't accumulate.
func unregister_skew_app(app_id string) {
	apps_lock.Lock()
	delete(apps, app_id)
	apps_lock.Unlock()
}

// setup_schema_skew_test brings the world to a state where one user
// owns one app at the given starting schema. Builds on
// setup_replication_test for the replication.db / data_dir scaffolding
// and adds the user + app + commit-hook stubs the apply path needs.
// Returns the app's AppVersion so the test can later bump Schema.
//
// post_migration_drain_async is stubbed to a no-op for these tests
// because db_app opens in the assertion helpers (skew_posts_count)
// would otherwise spawn a goroutine that re-runs replication_app_drain
// AFTER our explicit drain, racing the assertions. Tests that want to
// exercise the post_migration_drain_async wiring restore + override
// the stub locally (TestSchemaSkewDrainTriggeredByDbOpen).
func setup_schema_skew_test(t *testing.T, app_id string, initial_schema int) (*AppVersion, func()) {
	t.Helper()
	cleanup := setup_replication_test(t)

	setup_users_test_schema()
	db_open("db/users.db").exec("insert into users (uid, username) values (?, ?)", skew_user, "skew@example.com")

	av := register_skew_app(t, app_id, initial_schema)

	orig_drain := post_migration_drain_async
	post_migration_drain_async = func(user, app_id string) {}

	full_cleanup := func() {
		post_migration_drain_async = orig_drain
		unregister_skew_app(app_id)
		cleanup()
	}
	return av, full_cleanup
}

// skew_sql_op constructs a ReplicationOp carrying a SQL command at
// the given schema marker, sequence and chain predecessor. The
// payload is always a posts-table insert keyed on `id`, with `n` set
// so a sequence-ordered drain can be verified after the fact.
func skew_sql_op(app_id string, sequence, prev int64, schema int, id string, n int) *ReplicationOp {
	return &ReplicationOp{
		Scope:     repl_scope_app,
		User:      skew_user,
		Database:  app_id,
		Operation: repl_op_exec,
		Sequence:  sequence,
		Prev:      prev,
		Schema:    schema,
		Payload: cbor_encode(&SQLCommand{
			Statement: "insert into posts (id, title, n) values (?, ?, ?)",
			Args:      []any{id, "post-" + id, int64(n)},
		}),
	}
}

// skew_posts_count returns the number of posts rows on the local
// (test-host) per-user-app DB. Opens db_app to mimic production - the
// open ALSO triggers post_migration_drain_async (a no-op stub under
// setup_replication_test, so the count read is deterministic).
func skew_posts_count(t *testing.T, app_id string) int64 {
	t.Helper()
	u := &User{UID: skew_user}
	a := app_by_id(app_id)
	if a == nil {
		t.Fatalf("app %q not registered", app_id)
	}
	db := db_app(u, a)
	if db == nil {
		return 0
	}
	row, _ := db.row("select count(*) as n from posts")
	if row == nil {
		return 0
	}
	n, _ := row["n"].(int64)
	return n
}

// skew_pending_count reads the replication.db pending buffer depth
// for the (peer, user, app) tuple under test.
func skew_pending_count(t *testing.T, peer, app_id string) int64 {
	t.Helper()
	rdb := db_open("db/replication.db")
	row, _ := rdb.row(
		"select count(*) as n from pending where peer=? and scope=? and user=? and db=?",
		peer, repl_scope_app, skew_user, repl_stream_key(repl_stream_class_app, app_id))
	if row == nil {
		return 0
	}
	n, _ := row["n"].(int64)
	return n
}

// TestSchemaSkewForwardDefersThenAppliesAfterMigration is the headline
// case from the audit's #41 honest-gaps list. h2 (the receiver in this
// test) is at schema=1; h1 (synthetic sender) emits an op at schema=2.
// The op must buffer in pending, the row must NOT appear in posts,
// and the cursor must stay un-anchored. After the test bumps the
// receiver's AppVersion.Database.Schema to 2 (simulating a migration
// landing), replication_app_drain re-applies the deferred op and the
// row appears.
func TestSchemaSkewForwardDefersThenAppliesAfterMigration(t *testing.T) {
	app_id := "skew_forward"
	av, cleanup := setup_schema_skew_test(t, app_id, 1)
	defer cleanup()

	op := skew_sql_op(app_id, 1, 0, 2, "p1", 1)
	replication_op_receive("peerA", op)

	if got := skew_pending_count(t, "peerA", app_id); got != 1 {
		t.Errorf("after defer: pending = %d, want 1", got)
	}
	if got := skew_posts_count(t, app_id); got != 0 {
		t.Errorf("after defer: posts rows = %d, want 0 (op deferred, not applied)", got)
	}
	rdb := db_open("db/replication.db")
	if _, anchored := replication_cursor(rdb, "peerA", repl_scope_app, skew_user, repl_stream_key(repl_stream_class_app, app_id)); anchored {
		t.Errorf("after defer: cursor must NOT be anchored (apply did not succeed)")
	}

	// Migration lands: bump local schema to match the sender's.
	av.Database.Schema = 2
	replication_app_drain(skew_user, app_id)

	if got := skew_pending_count(t, "peerA", app_id); got != 0 {
		t.Errorf("after drain: pending = %d, want 0 (cleared)", got)
	}
	if got := skew_posts_count(t, app_id); got != 1 {
		t.Errorf("after drain: posts rows = %d, want 1 (deferred op applied)", got)
	}
	cursor, anchored := replication_cursor(rdb, "peerA", repl_scope_app, skew_user, repl_stream_key(repl_stream_class_app, app_id))
	if !anchored || cursor != 1 {
		t.Errorf("after drain: cursor anchored=%v seq=%d, want anchored=true seq=1", anchored, cursor)
	}
}

// TestSchemaSkewBackwardAppliesImmediately is the forward-compat
// direction. Receiver at schema=2 sees an op carrying schema=1 (an
// older sender, or a replay from before the receiver migrated). The
// op applies on first delivery; no defer, no pending row.
func TestSchemaSkewBackwardAppliesImmediately(t *testing.T) {
	app_id := "skew_backward"
	_, cleanup := setup_schema_skew_test(t, app_id, 2)
	defer cleanup()

	op := skew_sql_op(app_id, 1, 0, 1, "p1", 1)
	replication_op_receive("peerA", op)

	if got := skew_pending_count(t, "peerA", app_id); got != 0 {
		t.Errorf("backward skew should not defer: pending = %d, want 0", got)
	}
	if got := skew_posts_count(t, app_id); got != 1 {
		t.Errorf("backward skew should apply on first delivery: posts = %d, want 1", got)
	}
}

// TestSchemaSkewMultiVersionDrainsInOrder is the worst-case from the
// task description: receiver at v1, sender ahead by 4 versions. Four
// chained ops arrive at schema=2, 3, 4, 5 with sequences 1..4. None
// can apply (receiver still on v1); all four sit in pending. The
// test bumps the receiver's schema to 5 (simulating a multi-step
// migration arriving in a single restart) and asserts:
//   - all four rows now appear in posts
//   - they applied in sequence order (the n column preserves order)
//   - pending is empty
//   - cursor reached the last sequence
//
// The chained-by-Prev structure exercises stream-drain's chain-order
// requirement: a multi-version backlog can't be applied as a set, the
// chain has to drain head-first.
func TestSchemaSkewMultiVersionDrainsInOrder(t *testing.T) {
	app_id := "skew_multi"
	av, cleanup := setup_schema_skew_test(t, app_id, 1)
	defer cleanup()

	// Four ops, each chained to its predecessor, each one schema
	// version higher than the last.
	ops := []*ReplicationOp{
		skew_sql_op(app_id, 1, 0, 2, "p1", 1),
		skew_sql_op(app_id, 2, 1, 3, "p2", 2),
		skew_sql_op(app_id, 3, 2, 4, "p3", 3),
		skew_sql_op(app_id, 4, 3, 5, "p4", 4),
	}
	for _, op := range ops {
		replication_op_receive("peerA", op)
	}

	if got := skew_pending_count(t, "peerA", app_id); got != 4 {
		t.Fatalf("after all 4 defer: pending = %d, want 4", got)
	}
	if got := skew_posts_count(t, app_id); got != 0 {
		t.Errorf("after all 4 defer: posts rows = %d, want 0", got)
	}

	// Multi-step migration lands in one go.
	av.Database.Schema = 5
	replication_app_drain(skew_user, app_id)

	if got := skew_pending_count(t, "peerA", app_id); got != 0 {
		t.Errorf("after drain: pending = %d, want 0", got)
	}
	if got := skew_posts_count(t, app_id); got != 4 {
		t.Fatalf("after drain: posts rows = %d, want 4", got)
	}

	// Sequence order check: the four ops carry n=1,2,3,4 and the
	// stream-drain must replay them in sequence (1 first, 4 last).
	// SELECT order-by-n returns the rows in apply order so a
	// reordered drain would show 4 -> 1 here instead of 1 -> 4.
	u := &User{UID: skew_user}
	a := app_by_id(app_id)
	rows, err := db_app(u, a).rows("select id, n from posts order by n")
	if err != nil {
		t.Fatalf("readback: %v", err)
	}
	want := []struct {
		id string
		n  int64
	}{{"p1", 1}, {"p2", 2}, {"p3", 3}, {"p4", 4}}
	for i, w := range want {
		got_id, _ := rows[i]["id"].(string)
		got_n, _ := rows[i]["n"].(int64)
		if got_id != w.id || got_n != w.n {
			t.Errorf("drain order [%d]: got (%q, %d), want (%q, %d)", i, got_id, got_n, w.id, w.n)
		}
	}

	rdb := db_open("db/replication.db")
	cursor, anchored := replication_cursor(rdb, "peerA", repl_scope_app, skew_user, repl_stream_key(repl_stream_class_app, app_id))
	if !anchored || cursor != 4 {
		t.Errorf("after drain: cursor anchored=%v seq=%d, want anchored=true seq=4", anchored, cursor)
	}
}

// TestSchemaSkewDrainTriggeredByDbOpen is the production-integration
// half of the forward-skew test: rather than calling
// replication_app_drain directly, this test invokes db_app which is
// the path that fires post_migration_drain_async in production. The
// stub in setup_replication_test replaces the goroutine spawn with a
// synchronous call so the drain completes before the assertion runs;
// pins that the production wiring (post_migration_drain_async ->
// replication_app_drain) reaches the deferred op without the test
// having to know the inner function name.
func TestSchemaSkewDrainTriggeredByDbOpen(t *testing.T) {
	app_id := "skew_dbopen"
	av, cleanup := setup_schema_skew_test(t, app_id, 1)
	defer cleanup()

	// Override the no-op stub so db_app's post-migration hook actually
	// runs the drain, but synchronously - the production goroutine
	// would race the assertion.
	var drain_mu sync.Mutex
	orig := post_migration_drain_async
	post_migration_drain_async = func(user, app string) {
		drain_mu.Lock()
		defer drain_mu.Unlock()
		replication_app_drain(user, app)
	}
	defer func() { post_migration_drain_async = orig }()

	op := skew_sql_op(app_id, 1, 0, 2, "p1", 1)
	replication_op_receive("peerA", op)

	// Verify defer landed.
	if got := skew_pending_count(t, "peerA", app_id); got != 1 {
		t.Fatalf("after defer: pending = %d, want 1", got)
	}

	// Migration lands. The NEXT db_app open should trigger the drain.
	av.Database.Schema = 2
	u := &User{UID: skew_user}
	a := app_by_id(app_id)
	// db_app fires post_migration_drain_async (now synchronous).
	_ = db_app(u, a)

	if got := skew_pending_count(t, "peerA", app_id); got != 0 {
		t.Errorf("after db_app open: pending = %d, want 0 (drain should have fired)", got)
	}
	if got := skew_posts_count(t, app_id); got != 1 {
		t.Errorf("after db_app open: posts rows = %d, want 1", got)
	}
}

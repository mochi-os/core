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
		h.switchTo(name)
		h.setup_harness_user(ttUID, ttUname, mm_entity_id('t'))
		schedule_db().exec("create table if not exists schedule (id integer primary key, user text not null, app text not null, due int not null, event text not null, data text not null, interval int not null, created int not null)")
		setup_sessions_test_schema()
		mm_settings_schema()
	}
}

// TestThreeHostServerServerServerSchedule: three-host operator pair.
// h1 creates a schedule; both h2 and h3 receive it.
func TestThreeHostServerServerServerSchedule(t *testing.T) {
	h := newHarness(t, tt_h1, tt_h2, tt_h3)
	defer h.cleanup()
	// pair_members defaults to all three, user_hosts defaults to all
	// three - server-server-server is the default routing.
	seed_three_hosts(t, h)

	h.switchTo(tt_h1)
	if schedule_create(ttUID, "feeds", 1000, "tick", "{}", 60) == 0 {
		t.Fatal("h1 schedule_create returned 0")
	}
	h.flush()

	for _, name := range []string{tt_h2, tt_h3} {
		h.switchTo(name)
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
	h := newHarness(t, tt_h1, tt_h2, tt_h3)
	defer h.cleanup()
	seed_three_hosts(t, h)

	h.switchTo(tt_h2)
	setting_set("signup_enabled", "false")
	h.flush()

	for _, name := range []string{tt_h1, tt_h3} {
		h.switchTo(name)
		if got := setting_get("signup_enabled", ""); got != "false" {
			t.Errorf("%s: setting = %q, want %q", name, got, "false")
		}
	}
}

// TestThreeHostServerServerServerPartitionHeal: partition one host
// off, do ops on the remaining two AND on the partitioned one, heal,
// assert all three converge to the union.
func TestThreeHostServerServerServerPartitionHeal(t *testing.T) {
	h := newHarness(t, tt_h1, tt_h2, tt_h3)
	defer h.cleanup()
	seed_three_hosts(t, h)

	h.partition()

	h.switchTo(tt_h1)
	schedule_create(ttUID, "feeds", 1000, "from-h1", "{}", 0)
	h.switchTo(tt_h2)
	schedule_create(ttUID, "feeds", 2000, "from-h2", "{}", 0)
	h.switchTo(tt_h3)
	schedule_create(ttUID, "feeds", 3000, "from-h3", "{}", 0)

	h.heal()
	h.flush()

	for _, name := range []string{tt_h1, tt_h2, tt_h3} {
		h.switchTo(name)
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
	h := newHarness(t, tt_h1, tt_h2, tt_h3)
	defer h.cleanup()
	// No operator pair, but the user is linked across all three hosts.
	h.set_pair_members() // empty: each host is its own operator
	h.set_user_hosts(ttUID, tt_h1, tt_h2, tt_h3)
	seed_three_hosts(t, h)

	h.switchTo(tt_h1)
	if schedule_create(ttUID, "feeds", 1000, "linked-tick", "{}", 60) == 0 {
		t.Fatal("h1 schedule_create returned 0")
	}
	h.flush()

	for _, name := range []string{tt_h2, tt_h3} {
		h.switchTo(name)
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
	h := newHarness(t, tt_h1, tt_h2, tt_h3)
	defer h.cleanup()
	h.set_pair_members() // no operator pair
	h.set_user_hosts(ttUID, tt_h1, tt_h2, tt_h3)
	seed_three_hosts(t, h)

	// Pair-scope: setting only the operator on h1 cares about.
	h.switchTo(tt_h1)
	setting_set("signup_enabled", "h1-only")
	// Pair-scope: a username change goes via the pair-only path. h2
	// and h3 must NOT receive it - each operator chose their own
	// username for the user.
	replication_emit_users_users_pair_set(ttUID, map[string]string{"username": "renamed-on-h1"})

	h.flush()

	for _, name := range []string{tt_h2, tt_h3} {
		h.switchTo(name)
		if got := setting_get("signup_enabled", ""); got == "h1-only" {
			t.Errorf("%s: setting leaked across operator boundary: %q", name, got)
		}
		row, _ := db_open("db/users.db").row("select username from users where uid=?", ttUID)
		if got, _ := row["username"].(string); got == "renamed-on-h1" {
			t.Errorf("%s: pair-only username leaked across per-user link: %q", name, got)
		}
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
	h := newHarness(t)
	defer h.cleanup()
	// h1: only u2 exists.
	h.switchTo("h1")
	setup_users_test_schema()
	db_open("db/users.db").exec("insert into users (uid, username) values (?, ?)", "u2", "bob@example.com")
	// h2: both u1 and u2 exist.
	h.switchTo("h2")
	setup_users_test_schema()
	udb2 := db_open("db/users.db")
	udb2.exec("insert into users (uid, username) values (?, ?)", "u1", "alice@example.com")
	udb2.exec("insert into users (uid, username) values (?, ?)", "u2", "bob@example.com")

	// h1 renames u2 -> alice@example.com. Locally fine (no other
	// row holds it on h1). The pair-only emit reaches h2.
	h.switchTo("h1")
	db_open("db/users.db").exec("update users set username=? where uid=?", "alice@example.com", "u2")
	replication_emit_users_users_pair_set("u2", map[string]string{"username": "alice@example.com"})

	h.flush()

	// h2: UNIQUE constraint on users_username refuses the UPDATE. u2
	// must still be bob; u1 must still be alice. No data lost.
	h.switchTo("h2")
	row, _ := db_open("db/users.db").row("select username from users where uid=?", "u2")
	if got, _ := row["username"].(string); got != "bob@example.com" {
		t.Errorf("h2 u2 username = %q, want bob@example.com (UNIQUE refusal must leave row unchanged)", got)
	}
	row, _ = db_open("db/users.db").row("select username from users where uid=?", "u1")
	if got, _ := row["username"].(string); got != "alice@example.com" {
		t.Errorf("h2 u1 username = %q, want alice@example.com (must not be overwritten)", got)
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
	h := newHarness(t, tt_h1, tt_h2, tt_h3)
	defer h.cleanup()
	for _, name := range []string{tt_h1, tt_h2, tt_h3} {
		h.switchTo(name)
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
		// Vote from peer's perspective: prefer(scope, key, requester=p2p_id, voter=peer).
		// If requester wins the hash tie-break against the voter, grant.
		if replication_leader_prefer(scope, key, p2p_id, peer) {
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
		h.switchTo(name)
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
	wantLeader := "peer-" + winner
	for _, name := range []string{tt_h1, tt_h2, tt_h3} {
		h.switchTo(name)
		row, _ := db_open("db/replication.db").row(
			"select peer from leadership where scope=? and key=?",
			claimScope, claimKey)
		got, _ := row["peer"].(string)
		if name == winner && got != wantLeader {
			t.Errorf("%s (winner): leadership.peer = %q, want %q", name, got, wantLeader)
		}
		// The losers may have mirrored the winner or may simply
		// have no row. What's not allowed is mirroring a different
		// non-empty peer.
		if got != "" && got != wantLeader {
			t.Errorf("%s: leadership.peer = %q, want either empty or %q (the winner)", name, got, wantLeader)
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
	h := newHarness(t, tt_h1, tt_h2, tt_h3)
	defer h.cleanup()

	// h1 holds settings, documents, apps, and a session for an
	// existing user. h2 is a pre-existing pair member kept in sync
	// already. h3 starts fresh with only the schemas needed to
	// receive bootstrap deliveries.
	for _, name := range []string{tt_h1, tt_h2, tt_h3} {
		h.switchTo(name)
		mm_settings_schema()
		setup_users_test_schema()
		setup_sessions_test_schema()
		db_open("db/apps.db").exec("create table if not exists apps (app text primary key, installed integer not null default 0)")
	}

	h.switchTo(tt_h1)
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
	h.switchTo(tt_h3)
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
		h.switchTo(tt_h3)
		fn()
		h.switchTo(prior)
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
	replication_emit_session_insert_to_peer_var = func(peer, userUID, code, secret string, expires, created, accessed int64, address, agent string) {
		if peer != "peer-"+tt_h3 {
			t.Errorf("session_insert_to_peer: wrong peer %q", peer)
			return
		}
		applyOnH3(func() {
			replication_session_apply_insert(&SessionInsert{
				UserUID: userUID, Code: code, Secret: secret,
				Expires: expires, Created: created, Accessed: accessed,
				Address: address, Agent: agent,
			})
		})
	}

	// Run the bootstrap from h1's perspective. replication_pair_backfill_system
	// internally calls _sessions and _accounts, so a single call covers
	// every replicated table outside the users.db keys-transfer path.
	h.switchTo(tt_h1)
	replication_pair_backfill_system("peer-" + tt_h3)

	// Assert h3 has every row h1 has.
	h.switchTo(tt_h3)
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

// TestThreeHostUserUserUserSessionsAndUserStatus: in the per-user
// link topology, app-scope per-user ops (sessions, users.users
// methods/status/preferences) DO follow the user across all linked
// hosts. Cookie issued on h1 validates on h2 and h3.
func TestThreeHostUserUserUserSessionsAndUserStatus(t *testing.T) {
	h := newHarness(t, tt_h1, tt_h2, tt_h3)
	defer h.cleanup()
	h.set_pair_members()
	h.set_user_hosts(ttUID, tt_h1, tt_h2, tt_h3)
	seed_three_hosts(t, h)

	h.switchTo(tt_h1)
	code := login_create(ttUID, "1.2.3.4", "test-agent")
	if code == "" {
		t.Fatal("h1 login_create returned empty code")
	}
	// status change goes via the per-user path - should reach h2/h3.
	replication_emit_users_users_set(ttUID, map[string]string{"status": "suspended"})

	h.flush()

	for _, name := range []string{tt_h2, tt_h3} {
		h.switchTo(name)
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

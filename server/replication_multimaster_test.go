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
	h := newHarness(t, fh_h1, fh_h2, fh_h3)
	defer h.cleanup()

	// All three hosts have the users.db schema in place plus the
	// full replication.db schema (the keys-transfer apply seeds
	// cursors from kt.Seeds via replication_cursor_set, which needs
	// the cursor table; that lands in the v67 upgrade). Same migration
	// chain as setup_replication_test.
	for _, name := range []string{fh_h1, fh_h2, fh_h3} {
		h.switchTo(name)
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
	h.switchTo(fh_h1)
	udb1 := db_open("db/users.db")
	const ktUID = "uid-keystest"
	const ktName = "keystest@example.com"
	udb1.exec("insert into users (uid, username, role, methods, status) values (?, ?, 'administrator', 'email,passkey,totp', 'active')",
		ktUID, ktName)

	ent1 := mm_entity_id('k')
	ent2 := mm_entity_id('l')
	udb1.exec("insert into entities (id, private, fingerprint, user, parent, class, name, privacy, data, published) values (?, 'priv-1', 'fp-1', ?, '', 'person', 'Keys User', 'private', '', 100)",
		ent1, ktUID)
	udb1.exec("insert into entities (id, private, fingerprint, user, parent, class, name, privacy, data, published) values (?, 'priv-2', 'fp-2', ?, ?, 'feed', 'Keys Feed', 'public', '', 200)",
		ent2, ktUID, ent1)

	credID1 := []byte{0x01, 0x02, 0x03, 0x04, 0x05}
	credKey1 := []byte{0xaa, 0xbb, 0xcc, 0xdd}
	credID2 := []byte{0x10, 0x20, 0x30, 0x40, 0x50}
	credKey2 := []byte{0xee, 0xff, 0x11, 0x22}
	udb1.exec("insert into credentials (id, user, public_key, sign_count, name, transports, backup_eligible, backup_state, created) values (?, ?, ?, 7, 'YubiKey', 'usb', 1, 0, 100)",
		credID1, ktUID, credKey1)
	udb1.exec("insert into credentials (id, user, public_key, sign_count, name, transports, backup_eligible, backup_state, created) values (?, ?, ?, 3, 'Phone', 'internal', 0, 1, 200)",
		credID2, ktUID, credKey2)

	udb1.exec("insert into recovery (user, hash, created) values (?, 'hash-recovery-1', 100)", ktUID)
	udb1.exec("insert into recovery (user, hash, created) values (?, 'hash-recovery-2', 110)", ktUID)

	udb1.exec("insert into oauth (user, provider, subject, email, verified, name, created) values (?, 'github', 'gh-789', 'keys@example.com', 1, 'Keys User', 100)",
		ktUID)

	udb1.exec("insert into totp (user, secret, verified, created) values (?, 'totp-secret', 1, 100)", ktUID)

	udb1.exec("insert into tokens (hash, user, app, name, scopes, created, expires) values ('hash-tok-1', ?, 'feeds', 'mobile', 'read', 100, 0)", ktUID)

	// Stub the per-peer keys-transfer hook to apply on h3 instead
	// of queuing for libp2p send. Mirrors the system_set/_row stubs
	// the existing bootstrap test uses.
	orig_transfer := replication_transfer_keys_var
	defer func() { replication_transfer_keys_var = orig_transfer }()

	var (
		captured_uid  string
		captured_peer string
		captured_kt   *KeysTransfer
	)
	replication_transfer_keys_var = func(userUID, peer string) bool {
		captured_uid = userUID
		captured_peer = peer
		// Build the payload using the production builder so a future
		// regression in build_keys_transfer (e.g. dropped column,
		// missing nested array) shows up in this test.
		kt, ok := build_keys_transfer(userUID)
		if !ok {
			return false
		}
		captured_kt = kt
		prior := h.current
		h.switchTo(fh_h3)
		replication_keys_transfer_apply(kt.Entities[0].ID, "peer-"+fh_h1, kt)
		h.switchTo(prior)
		return true
	}

	// h1 runs the keys-transfer pair-backfill against h3. With one
	// user on h1 the backfill should fire the hook exactly once.
	h.switchTo(fh_h1)
	replication_pair_backfill_users("peer-" + fh_h3)

	// Wire-shape assertions: the captured payload must populate every
	// nested array we seeded. A future change that drops a column from
	// build_keys_transfer fails here before we even look at h3.
	if captured_uid != ktUID {
		t.Errorf("transfer captured uid = %q, want %q", captured_uid, ktUID)
	}
	if captured_peer != "peer-"+fh_h3 {
		t.Errorf("transfer captured peer = %q, want %q", captured_peer, "peer-"+fh_h3)
	}
	if captured_kt == nil {
		t.Fatal("transfer captured no payload")
	}
	if captured_kt.UID != ktUID || captured_kt.Username != ktName {
		t.Errorf("payload identity: uid=%q username=%q, want %q / %q",
			captured_kt.UID, captured_kt.Username, ktUID, ktName)
	}
	if captured_kt.Role != "administrator" || captured_kt.Methods != "email,passkey,totp" || captured_kt.Status != "active" {
		t.Errorf("payload identity columns: role=%q methods=%q status=%q",
			captured_kt.Role, captured_kt.Methods, captured_kt.Status)
	}
	if got := len(captured_kt.Entities); got != 2 {
		t.Errorf("payload entities = %d, want 2", got)
	}
	if got := len(captured_kt.Credentials); got != 2 {
		t.Errorf("payload credentials = %d, want 2", got)
	}
	if got := len(captured_kt.Recovery); got != 2 {
		t.Errorf("payload recovery = %d, want 2", got)
	}
	if got := len(captured_kt.OAuth); got != 1 {
		t.Errorf("payload oauth = %d, want 1", got)
	}
	if got := len(captured_kt.Tokens); got != 1 {
		t.Errorf("payload tokens = %d, want 1", got)
	}
	if captured_kt.Totp == nil || captured_kt.Totp.Secret != "totp-secret" {
		t.Errorf("payload totp missing or wrong secret: %+v", captured_kt.Totp)
	}

	// Receiver-side assertions: h3's users.db now holds the same auth
	// state h1 does. Per-table row-for-row equality on the cross-host
	// stable identifiers.
	h.switchTo(fh_h3)
	udb3 := db_open("db/users.db")

	row, _ := udb3.row("select uid, username, role, methods, status from users where uid=?", ktUID)
	if row == nil {
		t.Fatal("h3 users row missing after keys-transfer apply")
	}
	if got, _ := row["username"].(string); got != ktName {
		t.Errorf("h3 user username = %q, want %q", got, ktName)
	}
	if got, _ := row["role"].(string); got != "administrator" {
		t.Errorf("h3 user role = %q, want administrator (KeysTransfer carries role for fresh user, unlike per-user replication path)", got)
	}
	if got, _ := row["methods"].(string); got != "email,passkey,totp" {
		t.Errorf("h3 user methods = %q, want email,passkey,totp", got)
	}

	// Entities: both must exist with same private key, fingerprint,
	// parent, class, privacy.
	for _, e := range []struct {
		id      string
		private string
		parent  string
		class   string
		privacy string
	}{
		{ent1, "priv-1", "", "person", "private"},
		{ent2, "priv-2", ent1, "feed", "public"},
	} {
		r, _ := udb3.row("select private, parent, class, privacy from entities where id=? and user=?", e.id, ktUID)
		if r == nil {
			t.Errorf("h3 entity %q missing", e.id)
			continue
		}
		if got, _ := r["private"].(string); got != e.private {
			t.Errorf("h3 entity %q private = %q, want %q", e.id, got, e.private)
		}
		if got, _ := r["parent"].(string); got != e.parent {
			t.Errorf("h3 entity %q parent = %q, want %q", e.id, got, e.parent)
		}
		if got, _ := r["class"].(string); got != e.class {
			t.Errorf("h3 entity %q class = %q, want %q", e.id, got, e.class)
		}
		if got, _ := r["privacy"].(string); got != e.privacy {
			t.Errorf("h3 entity %q privacy = %q, want %q", e.id, got, e.privacy)
		}
	}

	// Credentials: keyed by blob id. public_key blob round-trips
	// through CBOR. sign_count + backup_eligible + backup_state are
	// the per-credential state.
	for _, c := range []struct {
		id            []byte
		publicKey     []byte
		signCount     int64
		name          string
		transports    string
		backupEligibl int64
		backupState   int64
	}{
		{credID1, credKey1, 7, "YubiKey", "usb", 1, 0},
		{credID2, credKey2, 3, "Phone", "internal", 0, 1},
	} {
		r, _ := udb3.row("select public_key, sign_count, name, transports, backup_eligible, backup_state from credentials where id=? and user=?", c.id, ktUID)
		if r == nil {
			t.Errorf("h3 credential %x missing", c.id)
			continue
		}
		// db.row converts []byte to string defensively (same conversion
		// build_keys_transfer has to work around with toBytes). Use the
		// same helper here to recover the raw bytes.
		gotKey := toBytes(r["public_key"])
		if !bytes_equal(gotKey, c.publicKey) {
			t.Errorf("h3 credential %x public_key = %x, want %x (blob round-trip via CBOR)", c.id, gotKey, c.publicKey)
		}
		if got, _ := r["sign_count"].(int64); got != c.signCount {
			t.Errorf("h3 credential %x sign_count = %d, want %d", c.id, got, c.signCount)
		}
		if got, _ := r["name"].(string); got != c.name {
			t.Errorf("h3 credential %x name = %q, want %q", c.id, got, c.name)
		}
		if got, _ := r["backup_eligible"].(int64); got != c.backupEligibl {
			t.Errorf("h3 credential %x backup_eligible = %d, want %d", c.id, got, c.backupEligibl)
		}
	}

	// Recovery codes: keyed by (user, hash).
	for _, hash := range []string{"hash-recovery-1", "hash-recovery-2"} {
		if exists, _ := udb3.exists("select 1 from recovery where user=? and hash=?", ktUID, hash); !exists {
			t.Errorf("h3 recovery hash %q missing", hash)
		}
	}

	// OAuth: keyed by (provider, subject). Verified flag round-trips
	// from bool to int.
	r, _ := udb3.row("select email, verified, name from oauth where provider='github' and subject='gh-789' and user=?", ktUID)
	if r == nil {
		t.Error("h3 oauth row missing")
	} else {
		if got, _ := r["email"].(string); got != "keys@example.com" {
			t.Errorf("h3 oauth email = %q", got)
		}
		if got, _ := r["verified"].(int64); got != 1 {
			t.Errorf("h3 oauth verified = %d, want 1", got)
		}
	}

	// TOTP: single row per user.
	r, _ = udb3.row("select secret, verified from totp where user=?", ktUID)
	if r == nil {
		t.Error("h3 totp row missing")
	} else {
		if got, _ := r["secret"].(string); got != "totp-secret" {
			t.Errorf("h3 totp secret = %q", got)
		}
		if got, _ := r["verified"].(int64); got != 1 {
			t.Errorf("h3 totp verified = %d, want 1", got)
		}
	}

	// Tokens: keyed by hash.
	r, _ = udb3.row("select app, name, scopes from tokens where hash='hash-tok-1' and user=?", ktUID)
	if r == nil {
		t.Error("h3 token row missing")
	} else {
		if got, _ := r["app"].(string); got != "feeds" {
			t.Errorf("h3 token app = %q", got)
		}
		if got, _ := r["scopes"].(string); got != "read" {
			t.Errorf("h3 token scopes = %q", got)
		}
	}
}

// bytes_equal is a small helper for the credential blob assertions.
// Avoids dragging bytes into the test file's import list for one call.
func bytes_equal(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
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
		h.switchTo(name)
		h.setup_harness_user(fhUID, "fourway@example.com", mm_entity_id('f'))
		schedule_db().exec("create table if not exists schedule (id integer primary key, user text not null, app text not null, due int not null, event text not null, data text not null, interval int not null, created int not null)")
		setup_sessions_test_schema()
		mm_settings_schema()
	}
}

// TestFourHostPairConvergence: pair quadruple, default routing.
// schedule + setting + document edited on one host reach the other
// three after a single flush.
func TestFourHostPairConvergence(t *testing.T) {
	h := newHarness(t, fh_h1, fh_h2, fh_h3, fh_h4)
	defer h.cleanup()
	seed_four_hosts(t, h)

	h.switchTo(fh_h1)
	if schedule_create(fhUID, "feeds", 1000, "tick", "{}", 60) == 0 {
		t.Fatal("schedule_create returned 0")
	}
	setting_set("operator_name", "FourwayOp")
	if err := document_set("terms", "en", "fourway terms"); err != nil {
		t.Fatalf("document_set: %v", err)
	}
	h.flush()

	for _, name := range []string{fh_h2, fh_h3, fh_h4} {
		h.switchTo(name)
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
	h := newHarness(t, fh_h1, fh_h2, fh_h3, fh_h4)
	defer h.cleanup()
	seed_four_hosts(t, h)

	h.partition()

	// Each host produces one distinct schedule row during the
	// partition (different event names so the natural-key dedup
	// keeps them as separate rows even if now() returns the same
	// epoch second for all four).
	for i, name := range []string{fh_h1, fh_h2, fh_h3, fh_h4} {
		h.switchTo(name)
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
		h.switchTo(name)
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
	h := newHarness(t, fh_h1, fh_h2, fh_h3, fh_h4)
	defer h.cleanup()
	h.set_pair_members() // no operator pair
	h.set_user_hosts(fhUID, fh_h1, fh_h2, fh_h3, fh_h4)
	seed_four_hosts(t, h)

	h.switchTo(fh_h2)
	if schedule_create(fhUID, "crm", 1000, "linked", "{}", 0) == 0 {
		t.Fatal("h2 schedule_create returned 0")
	}
	h.flush()

	for _, name := range []string{fh_h1, fh_h3, fh_h4} {
		h.switchTo(name)
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
	h := newHarness(t, fh_h1, fh_h2, fh_h3, fh_h4)
	defer h.cleanup()
	h.set_pair_members()
	h.set_user_hosts(fhUID, fh_h1, fh_h2, fh_h3, fh_h4)
	seed_four_hosts(t, h)

	h.switchTo(fh_h1)
	setting_set("signup_enabled", "h1-only-fourway")
	replication_emit_users_users_pair_set(fhUID, map[string]string{"username": "renamed-on-h1-fourway"})
	h.flush()

	for _, name := range []string{fh_h2, fh_h3, fh_h4} {
		h.switchTo(name)
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
	h := newHarness(t, fh_h1, fh_h2, fh_h3, fh_h4)
	defer h.cleanup()
	for _, name := range []string{fh_h1, fh_h2, fh_h3, fh_h4} {
		h.switchTo(name)
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

	h.switchTo(fh_h1)
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
	h := newHarness(t, fh_h1, fh_h2, fh_h3, fh_h4)
	defer h.cleanup()
	for _, name := range []string{fh_h1, fh_h2, fh_h3, fh_h4} {
		h.switchTo(name)
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

	h.switchTo(fh_h1)
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
	h := newHarness(t, fh_h1, fh_h2, fh_h3, fh_h4)
	defer h.cleanup()
	// All four schemas set up; only h1-h3 hold state, h4 is fresh.
	for _, name := range []string{fh_h1, fh_h2, fh_h3, fh_h4} {
		h.switchTo(name)
		mm_settings_schema()
		setup_users_test_schema()
		setup_sessions_test_schema()
		db_open("db/apps.db").exec("create table if not exists apps (app text primary key, installed integer not null default 0)")
	}

	// h1 (and via auto-replication, h2 and h3) hold the canonical
	// state.
	h.switchTo(fh_h1)
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
	h.switchTo(fh_h4)
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
		h.switchTo(fh_h4)
		fn()
		h.switchTo(prior)
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
	replication_emit_session_insert_to_peer_var = func(peer, userUID, code, secret string, expires, created, accessed int64, address, agent string) {
		if peer != "peer-"+fh_h4 {
			t.Errorf("session_insert: wrong peer %q", peer)
			return
		}
		applyOnH4(func() {
			replication_session_apply_insert(&SessionInsert{
				UserUID: userUID, Code: code, Secret: secret,
				Expires: expires, Created: created, Accessed: accessed,
				Address: address, Agent: agent,
			})
		})
	}

	// h1 ships its state to h4.
	h.switchTo(fh_h1)
	replication_pair_backfill_system("peer-" + fh_h4)

	// h4 now has the canonical state.
	h.switchTo(fh_h4)
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

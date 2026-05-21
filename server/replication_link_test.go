// Mochi server: per-user link-request protocol unit tests
// Copyright Alistair Cunningham 2026

package main

import (
	"sync"
	"testing"
	"time"
)

// TestReplicationLinkRequestApplyStoresRow: a valid link-request from a
// new peer for a known user writes a row to `replication.db.links` with
// the expected fields and a 1h expiry from receipt.
func TestReplicationLinkRequestApplyStoresRow(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()

	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username) values ('u-alice', 'alice')")

	replication_link_request_apply("peer-B", &LinkRequest{
		TargetUser:  "alice",
		Label:       "b.example.net",
		Placeholder: "ph-1",
	})

	rdb := db_open("db/replication.db")
	row, _ := rdb.row("select user, peer, label, placeholder, expires, received from links where user='u-alice' and peer='peer-B'")
	if row == nil {
		t.Fatal("expected one row in links after link-request apply")
	}
	if got, _ := row["label"].(string); got != "b.example.net" {
		t.Errorf("label = %q, want %q", got, "b.example.net")
	}
	if got, _ := row["placeholder"].(string); got != "ph-1" {
		t.Errorf("placeholder = %q, want %q", got, "ph-1")
	}
	received, _ := row["received"].(int64)
	expires, _ := row["expires"].(int64)
	if expires-received != 3600 {
		t.Errorf("expires - received = %d, want 3600 (1h)", expires-received)
	}
}

// TestReplicationLinkRequestApplyReplacesOnSecond: INSERT OR REPLACE
// keyed on (user, peer) — a second request from the same peer for the
// same user overwrites the first, redirecting eventual Approve at the
// fresh placeholder.
func TestReplicationLinkRequestApplyReplacesOnSecond(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()

	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username) values ('u-alice', 'alice')")

	replication_link_request_apply("peer-B", &LinkRequest{
		TargetUser:  "alice",
		Label:       "b.example.net",
		Placeholder: "ph-old",
	})
	replication_link_request_apply("peer-B", &LinkRequest{
		TargetUser:  "alice",
		Label:       "b.example.net",
		Placeholder: "ph-new",
	})

	rdb := db_open("db/replication.db")
	rows, _ := rdb.rows("select placeholder from links where user='u-alice' and peer='peer-B'")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row after second link-request, got %d", len(rows))
	}
	if ph, _ := rows[0]["placeholder"].(string); ph != "ph-new" {
		t.Errorf("placeholder after replace = %q, want %q", ph, "ph-new")
	}
}

// TestReplicationLinkRequestApplyDifferentPeersDistinct: two source peers
// can each have a pending request against the same target user — the
// dedup is per (user, peer), not per user.
func TestReplicationLinkRequestApplyDifferentPeersDistinct(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()

	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username) values ('u-alice', 'alice')")

	replication_link_request_apply("peer-B", &LinkRequest{TargetUser: "alice", Placeholder: "ph-B"})
	replication_link_request_apply("peer-C", &LinkRequest{TargetUser: "alice", Placeholder: "ph-C"})

	rdb := db_open("db/replication.db")
	rows, _ := rdb.rows("select peer from links where user='u-alice' order by peer")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows for alice from different peers, got %d", len(rows))
	}
}

// TestReplicationLinkRequestApplyRefusesReplicationToSelf: if the source
// peer is already in the target user's hosts set (per-user opt-in or
// whole-server pair already covers it), the link-request is silently
// refused — no row created.
func TestReplicationLinkRequestApplyRefusesReplicationToSelf(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()

	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username) values ('u-alice', 'alice')")

	rdb := db_open("db/replication.db")
	rdb.exec("insert into hosts (user, peer, added) values ('u-alice', 'peer-B', 0)")

	replication_link_request_apply("peer-B", &LinkRequest{
		TargetUser:  "alice",
		Placeholder: "ph-1",
	})

	exists, _ := rdb.exists("select 1 from links where user='u-alice' and peer='peer-B'")
	if exists {
		t.Error("link-request from already-hosting peer should be refused (no row written)")
	}
}

// TestReplicationLinkRequestApplyRefusesUnknownUser: targeting a username
// that doesn't exist on this server silently drops — there's no row to
// store the request against.
func TestReplicationLinkRequestApplyRefusesUnknownUser(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()

	replication_link_request_apply("peer-B", &LinkRequest{
		TargetUser:  "nobody",
		Placeholder: "ph-1",
	})

	rdb := db_open("db/replication.db")
	rows, _ := rdb.rows("select 1 from links")
	if len(rows) != 0 {
		t.Errorf("expected no rows for unknown user, got %d", len(rows))
	}
}

// TestReplicationLinkRequestApplyRejectsMissingFields: empty user,
// placeholder, or peer all silently no-op.
func TestReplicationLinkRequestApplyRejectsMissingFields(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()

	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username) values ('u-alice', 'alice')")

	cases := []struct {
		name string
		peer string
		lr   LinkRequest
	}{
		{"empty user", "peer-B", LinkRequest{Placeholder: "ph"}},
		{"empty placeholder", "peer-B", LinkRequest{TargetUser: "alice"}},
		{"empty peer", "", LinkRequest{TargetUser: "alice", Placeholder: "ph"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			replication_link_request_apply(c.peer, &c.lr)
			rdb := db_open("db/replication.db")
			rows, _ := rdb.rows("select 1 from links")
			if len(rows) != 0 {
				t.Errorf("expected 0 rows after %s, got %d", c.name, len(rows))
			}
		})
	}
}

// TestReplicationLinkApproveIdempotent: the DELETE-as-lock pattern in
// replication_link_approve makes a concurrent second call return
// "already-approved" without re-firing the freshness probe or transfer.
// We simulate the race by calling approve twice on a row that no longer
// exists after the first call.
func TestReplicationLinkApproveIdempotent(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()

	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username) values ('u-alice', 'alice')")

	rdb := db_open("db/replication.db")
	rdb.exec(
		"insert into links (user, peer, label, placeholder, received, expires) values ('u-alice', 'peer-B', 'b', 'ph-1', 0, 9999999999)")

	// Simulate the second tab's call to approve after the first already
	// deleted the row. The second call should return "already-approved"
	// rather than error.
	rdb.exec("delete from links where user='u-alice' and peer='peer-B'")

	status, err := replication_link_approve("u-alice", "peer-B")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status != "already-approved" {
		t.Errorf("status = %q, want %q", status, "already-approved")
	}
}

// TestReplicationLinkDenyIdempotent: same DELETE-as-lock pattern. After
// the row is already gone, deny returns "already-handled".
func TestReplicationLinkDenyIdempotent(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()

	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username) values ('u-alice', 'alice')")

	// Row never existed → already-handled.
	if status := replication_link_deny("u-alice", "peer-B"); status != "already-handled" {
		t.Errorf("deny with no row = %q, want %q", status, "already-handled")
	}
}

// TestReplicationLinkDeniedApplyCleansPlaceholder: receiving a denied op
// for a placeholder in 'pending-replication' status wipes the users row
// and any entity rows that might have been scaffolded.
func TestReplicationLinkDeniedApplyCleansPlaceholder(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()

	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username, status) values ('ph-1', 'placeholder-name', 'pending-replication')")

	replication_link_denied_apply("peer-A", &LinkDenied{Placeholder: "ph-1", Reason: "denied"})

	exists, _ := udb.exists("select 1 from users where uid='ph-1'")
	if exists {
		t.Error("denied placeholder row should be deleted")
	}
}

// TestReplicationLinkDeniedApplyLeavesActiveAccountAlone: an active
// (post-Approve) user row must NOT be touched by a stray late-arriving
// link-denied op.
func TestReplicationLinkDeniedApplyLeavesActiveAccountAlone(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()

	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username, status) values ('u-alice', 'alice', 'active')")

	replication_link_denied_apply("peer-A", &LinkDenied{Placeholder: "u-alice"})

	exists, _ := udb.exists("select 1 from users where uid='u-alice'")
	if !exists {
		t.Error("active user must not be deleted by a stray link-denied op")
	}
}

// TestReplicationLinkApplyKeysBackfillsUserData: receiving the
// keys-transfer after the source approves the link must kick off the
// per-user backfill (files + userdbs) AND defer the placeholder's
// activation until the backfill settles. Without the deferral, the
// running web handlers race the rename(2)-replacing bootstrap and
// corrupt SQLite — caught live 2026-05-20 as a "disk image malformed"
// panic plus a feeds.db that landed at 221 KB instead of 1 GB because
// the live SQLite fd was pinned to the pre-snapshot inode.
func TestReplicationLinkApplyKeysBackfillsUserData(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()

	// Pre-create the placeholder user the source's link-request would
	// have left behind, so the apply path can flip it.
	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username, status) values ('u-alice', 'alice@local', 'pending-replication')")

	var mu sync.Mutex
	var fileReqs []struct{ peer, scope, prefix string }
	var dbReqs []struct{ peer, scope, user string }
	origFile := replication_bootstrap_file_manifest_fetch
	origDB := replication_bootstrap_db_manifest_fetch
	replication_bootstrap_file_manifest_fetch = func(peer, scope, prefix string) {
		mu.Lock()
		fileReqs = append(fileReqs, struct{ peer, scope, prefix string }{peer, scope, prefix})
		mu.Unlock()
	}
	replication_bootstrap_db_manifest_fetch = func(peer, scope, user string) {
		mu.Lock()
		dbReqs = append(dbReqs, struct{ peer, scope, user string }{peer, scope, user})
		mu.Unlock()
	}
	// Stub the activation waiter so the test doesn't race a real goroutine.
	origWait := bootstrap_wait_then_activate
	var waitCalls []struct{ peer, uid string }
	bootstrap_wait_then_activate = func(peer, uid string) {
		mu.Lock()
		waitCalls = append(waitCalls, struct{ peer, uid string }{peer, uid})
		mu.Unlock()
	}
	defer func() {
		replication_bootstrap_file_manifest_fetch = origFile
		replication_bootstrap_db_manifest_fetch = origDB
		bootstrap_wait_then_activate = origWait
	}()

	kt := &KeysTransfer{
		Username: "alice@source",
		Role:     "user",
		Methods:  "email",
		Status:   "active",
	}
	if got := replication_link_apply_keys("source-peer", "u-alice", kt); got != 0 {
		// No entities supplied — entity-insert count is 0, but the
		// bootstrap fan-out + deferred activation are the things we
		// care about.
		t.Logf("entities inserted = %d (no entities in kt — expected 0)", got)
	}

	// Bootstrap fires in goroutines; wait for the stubs to record.
	for i := 0; i < 50; i++ {
		mu.Lock()
		done := len(fileReqs) == 1 && len(dbReqs) == 1 && len(waitCalls) == 1
		mu.Unlock()
		if done {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(fileReqs) != 1 {
		t.Fatalf("apply_keys must fire one per-user files manifest; got %d", len(fileReqs))
	}
	if fileReqs[0].prefix != "u-alice/" {
		t.Errorf("file-manifest prefix = %q, want %q (placeholder uid scoped)", fileReqs[0].prefix, "u-alice/")
	}
	if len(dbReqs) != 1 {
		t.Fatalf("apply_keys must fire one per-user userdbs manifest; got %d", len(dbReqs))
	}
	if dbReqs[0].user != "u-alice" {
		t.Errorf("db-manifest user filter = %q, want %q (must equal placeholder uid)", dbReqs[0].user, "u-alice")
	}
	if len(waitCalls) != 1 || waitCalls[0].peer != "source-peer" || waitCalls[0].uid != "u-alice" {
		t.Errorf("apply_keys must spawn bootstrap_wait_then_activate(peer=source-peer, uid=u-alice); got %+v", waitCalls)
	}

	// CRITICAL: status MUST remain pending-replication so /login/replicating
	// keeps the user on the waiting page until the waiter flips them.
	// Activating here would re-introduce the live-rename race.
	row, _ := udb.row("select status from users where uid='u-alice'")
	if status, _ := row["status"].(string); status != "pending-replication" {
		t.Errorf("placeholder status after apply_keys = %q, want %q (must stay pending until bootstrap completes — see CLAUDE.md and the in-comment incident note)", status, "pending-replication")
	}
}

// TestReplicationLinkApplyKeysTransfersAuthFactors: the keys-transfer
// must carry the user's OAuth links, recovery codes, passkeys, API
// tokens and TOTP secret to the replica and apply them re-keyed to the
// placeholder uid. Regression for the 2026-05-21 report that recovery
// codes + 3rd-party logins were missing on a per-user replica — the
// KeysTransfer struct had the fields but neither side populated them.
func TestReplicationLinkApplyKeysTransfersAuthFactors(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()

	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username, methods, status) values ('u-alice', 'alice@local', 'email', 'pending-replication')")

	// Stub bootstrap so the test doesn't spawn real network goroutines.
	origFile := replication_bootstrap_file_manifest_fetch
	origDB := replication_bootstrap_db_manifest_fetch
	origWait := bootstrap_wait_then_activate
	replication_bootstrap_file_manifest_fetch = func(peer, scope, prefix string) {}
	replication_bootstrap_db_manifest_fetch = func(peer, scope, user string) {}
	bootstrap_wait_then_activate = func(peer, uid string) {}
	defer func() {
		replication_bootstrap_file_manifest_fetch = origFile
		replication_bootstrap_db_manifest_fetch = origDB
		bootstrap_wait_then_activate = origWait
	}()

	kt := &KeysTransfer{
		Username: "alice@source",
		Methods:  "email,passkey,oauth,totp",
		Status:   "active",
		OAuth: []KeysOauth{
			{Provider: "github", Subject: "gh-123", Email: "alice@gh", Verified: true, Name: "Alice", Created: 100},
		},
		Credentials: []KeysCredential{
			{ID: []byte("cred-1"), PublicKey: []byte("pk-1"), SignCount: 7, Name: "Yubikey", Created: 110},
		},
		Recovery: []KeysRecovery{
			{Hash: "rec-hash-a", Created: 120},
			{Hash: "rec-hash-b", Created: 121},
		},
		Tokens: []KeysToken{
			{Hash: "tok-hash-1", App: "feeds", Name: "CLI", Scopes: "read", Created: 130, Expires: 0},
		},
		Totp: &KeysTotp{Secret: "TOTPSECRET", Verified: true, Created: 140},
	}

	replication_link_apply_keys("source-peer", "u-alice", kt)

	// OAuth link.
	if n := udb.integer("select count(*) from oauth where user='u-alice' and provider='github' and subject='gh-123'"); n != 1 {
		t.Errorf("oauth rows for placeholder = %d, want 1", n)
	}
	// Passkey credential.
	if n := udb.integer("select count(*) from credentials where user='u-alice'"); n != 1 {
		t.Errorf("credential rows for placeholder = %d, want 1", n)
	}
	// Recovery codes — both.
	if n := udb.integer("select count(*) from recovery where user='u-alice'"); n != 2 {
		t.Errorf("recovery rows for placeholder = %d, want 2", n)
	}
	// API token.
	if n := udb.integer("select count(*) from tokens where user='u-alice' and hash='tok-hash-1'"); n != 1 {
		t.Errorf("token rows for placeholder = %d, want 1", n)
	}
	// TOTP secret.
	totpRow, _ := udb.row("select secret from totp where user='u-alice'")
	if totpRow == nil || row_string(totpRow, "secret") != "TOTPSECRET" {
		t.Errorf("totp secret = %v, want %q", totpRow, "TOTPSECRET")
	}
	// methods column mirrors the source.
	methodsRow, _ := udb.row("select methods from users where uid='u-alice'")
	if methodsRow == nil || row_string(methodsRow, "methods") != "email,passkey,oauth,totp" {
		t.Errorf("methods = %v, want %q", methodsRow, "email,passkey,oauth,totp")
	}

	// Idempotency: a re-applied keys-transfer (apply is not exactly-once)
	// must not duplicate any auth row.
	replication_link_apply_keys("source-peer", "u-alice", kt)
	if n := udb.integer("select count(*) from recovery where user='u-alice'"); n != 2 {
		t.Errorf("recovery rows after re-apply = %d, want 2 (no duplicates)", n)
	}
	if n := udb.integer("select count(*) from oauth where user='u-alice'"); n != 1 {
		t.Errorf("oauth rows after re-apply = %d, want 1 (no duplicates)", n)
	}
	if n := udb.integer("select count(*) from tokens where user='u-alice'"); n != 1 {
		t.Errorf("token rows after re-apply = %d, want 1 (no duplicates)", n)
	}
}

// TestReplicationLinkTransfersScheduledEvents: per-user link must
// carry the user's db/schedule.db rows — that DB is a system DB in no
// bootstrap scope, so without the keys-transfer field the user's
// reminders / recurring jobs are stranded on the source.
func TestReplicationLinkTransfersScheduledEvents(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()

	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username, status) values ('u-alice', 'alice@local', 'pending-replication')")

	// schedule.db is a system DB; create the table the way db_create does.
	sdb := schedule_db()
	sdb.exec("create table schedule (id integer primary key, user text not null, app text not null, due int not null, event text not null, data text not null, interval int not null, created int not null)")

	origFile := replication_bootstrap_file_manifest_fetch
	origDB := replication_bootstrap_db_manifest_fetch
	origWait := bootstrap_wait_then_activate
	replication_bootstrap_file_manifest_fetch = func(peer, scope, prefix string) {}
	replication_bootstrap_db_manifest_fetch = func(peer, scope, user string) {}
	bootstrap_wait_then_activate = func(peer, uid string) {}
	defer func() {
		replication_bootstrap_file_manifest_fetch = origFile
		replication_bootstrap_db_manifest_fetch = origDB
		bootstrap_wait_then_activate = origWait
	}()

	kt := &KeysTransfer{
		Username: "alice@source",
		Status:   "active",
		Schedule: []KeysSchedule{
			{App: "feeds", Due: 5000, Event: "digest", Data: "{}", Interval: 86400, Created: 100},
			{App: "wikis", Due: 6000, Event: "reminder", Data: "", Interval: 0, Created: 101},
		},
	}

	replication_link_apply_keys("source-peer", "u-alice", kt)

	if n := sdb.integer("select count(*) from schedule where user='u-alice'"); n != 2 {
		t.Errorf("scheduled events for placeholder = %d, want 2", n)
	}
	row, _ := sdb.row("select app, due, event, interval from schedule where user='u-alice' and event='digest'")
	if row == nil {
		t.Fatal("digest event not transferred")
	}
	if row_int(row, "interval") != 86400 || row_int(row, "due") != 5000 {
		t.Errorf("digest event fields wrong: %+v", row)
	}

	// Idempotent: a re-applied keys-transfer must not double-insert.
	replication_link_apply_keys("source-peer", "u-alice", kt)
	if n := sdb.integer("select count(*) from schedule where user='u-alice'"); n != 2 {
		t.Errorf("scheduled events after re-apply = %d, want 2 (no duplicates)", n)
	}
}

// TestReplicationLinkPrunesDeviceAccounts: after the userdbs bootstrap
// copies user.db wholesale, the per-device push subscriptions it
// carried must be pruned — those endpoints belong to browsers/phones
// paired with the source host. Non-device account types stay.
func TestReplicationLinkPrunesDeviceAccounts(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()

	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username, status) values ('u-alice', 'alice@local', 'active')")

	// db_user(..., "user") creates the accounts table on open.
	user_db := db_user(&User{UID: "u-alice"}, "user")
	for _, a := range []struct{ typ, label string }{
		{"browser", "Chrome on laptop"},
		{"unifiedpush", "phone"},
		{"fcm", "android"},
		{"email", "alice@example.com"},
		{"ai", "Claude"},
	} {
		user_db.exec("insert into accounts (type, label, created) values (?, ?, ?)", a.typ, a.label, now())
	}

	replication_link_prune_devices("u-alice")

	if n := user_db.integer("select count(*) from accounts where type in ('browser','unifiedpush','fcm')"); n != 0 {
		t.Errorf("per-device accounts after prune = %d, want 0", n)
	}
	if n := user_db.integer("select count(*) from accounts"); n != 2 {
		t.Errorf("accounts surviving prune = %d, want 2 (email + ai — host-shared types)", n)
	}
}

// TestBootstrapWaitThenActivateFlipsOnDone: the waiter polls the
// (scope, peer) bootstrap state and flips the placeholder once BOTH
// files + userdbs reach state='done'. files-only or userdbs-only is
// not enough.
func TestBootstrapWaitThenActivateFlipsOnDone(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()

	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username, status) values ('u-alice', 'alice@local', 'pending-replication')")

	// Pre-seed both scopes as done so the waiter exits on the first poll
	// without us having to wait the full poll interval.
	bootstrap_set_state(bootstrap_scope_files, "source-peer", bootstrap_state_done, "")
	bootstrap_set_state(bootstrap_scope_userdbs, "source-peer", bootstrap_state_done, "")

	bootstrap_wait_then_activate_impl("source-peer", "u-alice")

	row, _ := udb.row("select status from users where uid='u-alice'")
	if status, _ := row["status"].(string); status != "active" {
		t.Errorf("status after waiter = %q, want %q (both scopes done should activate)", status, "active")
	}
}

// TestBootstrapWaitThenActivateIdempotent: a second waiter (e.g. after
// restart re-spawn) must not undo an active flip already applied. The
// UPDATE is gated on status='pending-replication' so this is safe.
func TestBootstrapWaitThenActivateIdempotent(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()

	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username, role, status) values ('u-alice', 'alice@local', 'admin', 'active')")

	bootstrap_set_state(bootstrap_scope_files, "source-peer", bootstrap_state_done, "")
	bootstrap_set_state(bootstrap_scope_userdbs, "source-peer", bootstrap_state_done, "")

	bootstrap_wait_then_activate_impl("source-peer", "u-alice")

	row, _ := udb.row("select role, status from users where uid='u-alice'")
	role, _ := row["role"].(string)
	status, _ := row["status"].(string)
	if status != "active" || role != "admin" {
		t.Errorf("already-active user mutated by waiter: role=%q status=%q (want admin/active)", role, status)
	}
}

// TestReplicationLinkResumePendingActivations: a placeholder still in
// pending-replication on startup (server crashed mid-bootstrap) must
// get a fresh waiter, sourced from the host that's recorded in
// replication.db.hosts at apply_keys time.
func TestReplicationLinkResumePendingActivations(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()

	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username, status) values ('u-alice', 'alice@local', 'pending-replication')")
	udb.exec("insert into users (uid, username, status) values ('u-bob', 'bob@local', 'active')")

	rdb := db_open("db/replication.db")
	rdb.exec("insert or replace into hosts (user, peer, added, ack) values ('u-alice', 'source-peer-A', 1, 0)")
	// A pending user with NO host: pathological state; resume must skip
	// rather than panic.
	udb.exec("insert into users (uid, username, status) values ('u-orphan', 'orphan@local', 'pending-replication')")

	var mu sync.Mutex
	var waitCalls []struct{ peer, uid string }
	orig := bootstrap_wait_then_activate
	bootstrap_wait_then_activate = func(peer, uid string) {
		mu.Lock()
		waitCalls = append(waitCalls, struct{ peer, uid string }{peer, uid})
		mu.Unlock()
	}
	defer func() { bootstrap_wait_then_activate = orig }()

	replication_link_resume_pending_activations()
	for i := 0; i < 50; i++ {
		mu.Lock()
		done := len(waitCalls) >= 1
		mu.Unlock()
		if done {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(waitCalls) != 1 {
		t.Fatalf("expected exactly 1 wait spawn (u-alice only — u-bob is active, u-orphan has no host); got %d: %+v", len(waitCalls), waitCalls)
	}
	if waitCalls[0].peer != "source-peer-A" || waitCalls[0].uid != "u-alice" {
		t.Errorf("wait spawn = %+v, want {peer:source-peer-A uid:u-alice}", waitCalls[0])
	}
}

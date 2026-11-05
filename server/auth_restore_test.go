// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestUserRestoreRoundTrip(t *testing.T) {
	create_test_users_db(t)

	db := db_open("db/users.db")
	// create_test_users_db uses the pre-v70 users schema; add the column
	// the restore path writes.
	db.exec("alter table users add column restore_source text not null default ''")
	db.exec("alter table users add column restore_passkeys integer not null default 0")
	db.exec("create table entities (id text not null primary key, private text not null, fingerprint text not null, user text not null references users(uid) on delete cascade, parent text not null default '', class text not null, name text not null, privacy text not null default 'public', data text not null default '', published integer not null default 0)")
	db.exec("create table relinks (user text not null, service text not null, identifier text not null default '', linked integer not null default 0, primary key (user, service))")
	// Tables the auth-restore path reads and writes.
	db.exec("create table totp (user text primary key, secret text not null, verified integer not null default 0, used integer not null default 0, created integer not null)")
	db.exec("create table recovery (id integer primary key, user text not null, hash text not null, created integer not null)")
	db.exec("create table credentials (id text not null primary key, user text not null, public_key blob, sign_count integer, name text, transports text, backup_eligible integer, backup_state integer, created integer)")
	sched := db_open("db/schedule.db")
	sched.exec("create table schedule (id integer primary key, user text not null, app text not null, due int not null, event text not null, data text not null, interval int not null, created int not null)")
	settings := db_open("db/settings.db")
	settings.exec("create table settings ( name text not null primary key, value text not null )")

	// Source server URL so restore_source has a non-empty value to verify.
	// export_source_server falls back to the email_from domain when no
	// directory domain is configured.
	setting_set("email_from", "noreply@test.example")

	// Source user: a private person entity, a durable schedule event, and
	// one attachment file under an app subtree.
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("keygen: %v", err)
	}
	id := base58_encode(pub)
	private := base58_encode(priv)
	src := "u-src"
	db.exec("insert into users (uid, username) values (?, ?)", src, "src@example.com")
	db.exec("insert into entities (id, private, fingerprint, user, parent, class, name, privacy, data, published) values (?, ?, ?, ?, '', 'person', 'Src', 'private', '', 0)",
		id, private, fingerprint(id), src)
	sched.exec("insert into schedule (user, app, due, event, data, interval, created) values (?, 'chat', 555, 'remind', '{}', 0, 1)", src)

	// Restorable auth credentials: a verified authenticator secret, two
	// recovery-code hashes, and a 2FA requirement (email + authenticator).
	db.exec("update users set methods='email,totp' where uid=?", src)
	db.exec("insert into totp (user, secret, verified, created) values (?, 'JBSWY3DPEHPK3PXP', 1, 100)", src)
	db.exec("insert into recovery (user, hash, created) values (?, 'hash-a', 100)", src)
	db.exec("insert into recovery (user, hash, created) values (?, 'hash-b', 100)", src)
	// A registered passkey: not restored (origin-bound), but the destination
	// records that the source had one so the banner can prompt re-registration.
	db.exec("insert into credentials (id, user, created) values ('cred-1', ?, 100)", src)

	attach := filepath.Join(data_dir, "users", src, "chat", "files")
	if err := os.MkdirAll(attach, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(attach, "note.txt"), []byte("hello world"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	// The source follows and is followed by two people, one of whom this
	// server blocks once the bundle is made: the bundle never saw the block.
	networks_create(networks_database())
	followed := db_user(user_by_uid(src), "user")
	for _, actor := range []string{"https://remote.example/u/blocked", "https://remote.example/u/kept"} {
		followed.exec("insert into followers (account, actor, inbox, created) values ('f1', ?, ?, 1)", actor, actor+"/inbox")
		followed.exec("insert into following (account, actor, activity, created) values ('f1', ?, ?, 1)", actor, actor+"#follow")
	}

	passphrase := "correct horse battery staple"
	rel, err := user_export(src, "settings", passphrase, "")
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	zip_path := filepath.Join(data_dir, "users", src, "settings", "files", rel)

	// ---- Restore into a fresh destination ----
	stage := filepath.Join(data_dir, "users", "u-dst", "restore", "staging")
	bundle, err := restore_unzip(zip_path, stage, file_maximum_storage)
	if err != nil {
		t.Fatalf("unzip: %v", err)
	}

	var manifest export_manifest
	if err := restore_read_json(filepath.Join(bundle, "manifest.json"), &manifest); err != nil {
		t.Fatalf("manifest: %v", err)
	}
	keys, err := restore_decrypt_keys(filepath.Join(bundle, "keys.age"), passphrase)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	// Decrypt secrets before the swap (the real flow decrypts in
	// web_auth_restore, before restore_apply runs the swap that consumes
	// secrets.age from the bundle).
	secrets, err := restore_decrypt_secrets(filepath.Join(bundle, "secrets.age"), passphrase)
	if err != nil {
		t.Fatalf("decrypt secrets: %v", err)
	}
	var account export_account
	if err := restore_read_json(filepath.Join(bundle, "user.json"), &account); err != nil {
		t.Fatalf("user.json: %v", err)
	}
	if primary := restore_primary_entity(account); primary != id {
		t.Fatalf("primary entity = %q, want %q", primary, id)
	}
	if !export_manifest_signed_by(manifest, id) {
		t.Fatal("manifest signature did not verify")
	}
	if app, _ := restore_schema_guard(bundle); app != "" {
		t.Fatalf("schema guard refused unexpectedly: %q", app)
	}

	networks_database().exec("insert into blocked (actor, created) values ('https://remote.example/u/blocked', 1)")

	dst := "u-dst"
	db.exec("insert into users (uid, username) values (?, ?)", dst, "dst@example.com")
	if err := restore_swap(dst, bundle); err != nil {
		t.Fatalf("swap: %v", err)
	}
	restore_entities(dst, account, keys)
	restore_schedule(dst, bundle)
	restore_auth(dst, account, secrets)
	restore_finish_account(dst, manifest, bundle)

	// ---- Assertions on the destination ----
	restored := db_user(user_by_uid(dst), "user")
	for _, table := range []string{"followers", "following"} {
		if found, _ := restored.exists("select 1 from " + table + " where actor='https://remote.example/u/blocked'"); found {
			t.Errorf("THE POINT: the restored %s still hold a person this server blocks", table)
		}
		if found, _ := restored.exists("select 1 from " + table + " where actor='https://remote.example/u/kept'"); !found {
			t.Errorf("CONTROL: the restored %s lost a person this server does not block", table)
		}
	}
	var got Entity
	if !db.scan(&got, "select * from entities where id=?", id) {
		t.Fatal("entity not restored")
	}
	if got.User != dst {
		t.Errorf("entity owner = %q, want %q", got.User, dst)
	}
	if got.Private != private {
		t.Error("entity private key not restored")
	}

	data, err := os.ReadFile(filepath.Join(data_dir, "users", dst, "chat", "files", "note.txt"))
	if err != nil || string(data) != "hello world" {
		t.Errorf("attachment = %q (err %v), want 'hello world'", data, err)
	}

	if any, _ := sched.exists("select 1 from schedule where user=? and event='remind' and due=555", dst); !any {
		t.Error("schedule event not re-inserted for the destination uid")
	}

	row, _ := db.row("select restore_source from users where uid=?", dst)
	if row == nil || as_string(row["restore_source"]) != "https://test.example" {
		t.Errorf("restore_source = %v, want https://test.example", row)
	}

	// Authenticator secret restored (device-independent, so it travels).
	trow, _ := db.row("select secret, verified from totp where user=?", dst)
	if trow == nil || as_string(trow["secret"]) != "JBSWY3DPEHPK3PXP" || as_int64(trow["verified"]) != 1 {
		t.Errorf("totp not restored: %v", trow)
	}
	// Recovery-code hashes restored (the user's saved codes keep working).
	if n, _ := db.row("select count(*) as count from recovery where user=?", dst); n == nil || as_int64(n["count"]) != 2 {
		t.Errorf("recovery codes = %v, want 2", n)
	}
	// The 2FA requirement (email + authenticator) carries over, since the
	// authenticator came back verified.
	mrow, _ := db.row("select methods, disabled from users where uid=?", dst)
	if mrow == nil || as_string(mrow["methods"]) != "email,totp" {
		t.Errorf("methods = %v, want email,totp", mrow)
	}
	if mrow != nil && as_string(mrow["disabled"]) != "" {
		t.Errorf("disabled = %q, want empty", as_string(mrow["disabled"]))
	}
	// The source had a passkey; the destination flags it for the re-register
	// banner (passkeys themselves don't travel).
	if prow, _ := db.row("select restore_passkeys from users where uid=?", dst); prow == nil || as_int64(prow["restore_passkeys"]) != 1 {
		t.Errorf("restore_passkeys = %v, want 1", prow)
	}
}

// TestRestoreSafeMethods verifies the filter never carries a requirement onto
// a factor that can't be re-established on the destination, and never leaves
// the user with no usable login path.
func TestRestoreSafeMethods(t *testing.T) {
	cases := []struct {
		name              string
		methods, disabled string
		totp, recovery    bool
		want_methods      string
		want_disabled     string
	}{
		// Authenticator restored: a 2FA requirement carries over intact.
		{"2fa-totp-restored", "email,totp", "", true, true, "email,totp", ""},
		// Authenticator NOT restored: drop the totp requirement (else lockout).
		{"2fa-totp-missing", "email,totp", "", false, false, "email", ""},
		// Passkey-only required, passkey can't be restored: requirement dropped.
		{"passkey-required", "passkey", "", false, false, "", ""},
		// Passkey-only with email explicitly disabled, passkey gone: un-disable
		// email so the user isn't locked out.
		{"passkey-only-email-disabled", "passkey", "email", false, false, "", ""},
		// Disabling an unavailable factor (passkey) is harmless and preserved.
		{"disable-passkey", "", "passkey", false, false, "", "passkey"},
		// Most users: nothing required, nothing disabled.
		{"baseline", "", "", false, false, "", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m, d := restore_safe_methods(c.methods, c.disabled, c.totp, c.recovery)
			if m != c.want_methods {
				t.Errorf("methods = %q, want %q", m, c.want_methods)
			}
			if d != c.want_disabled {
				t.Errorf("disabled = %q, want %q", d, c.want_disabled)
			}
		})
	}
}

func TestRestoreSourceOrigin(t *testing.T) {
	cases := []struct {
		name   string
		source string
		want   string
	}{
		// The shape export_source_server produces survives unchanged.
		{"plain", "https://mochi.example", "https://mochi.example"},
		{"trailing-slash", "https://mochi.example/", "https://mochi.example"},
		{"port", "https://mochi.example:8443", "https://mochi.example:8443"},
		{"case", "HTTPS://Mochi.Example", "https://mochi.example"},
		// Anything a legitimate export can't contain is dropped entirely.
		{"empty", "", ""},
		{"http", "http://mochi.example", ""},
		{"script", "javascript:alert(1)", ""},
		{"script-slashes", "javascript://mochi.example%0aalert(1)", ""},
		{"credentials", "https://user:secret@mochi.example", ""},
		{"path", "https://mochi.example/phish", ""},
		{"query", "https://mochi.example?q=1", ""},
		{"fragment", "https://mochi.example#f", ""},
		{"garbage", "not a url", ""},
		{"relative", "mochi.example", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := restore_source_origin(c.source); got != c.want {
				t.Errorf("restore_source_origin(%q) = %q, want %q", c.source, got, c.want)
			}
		})
	}
}

// restore_integrity_guard quick_checks every *.db in a user-supplied restore
// bundle before it is swapped into place. The manifest file-hash is
// self-attested, so it cannot prove a DB is sound.
//
// Application Interface Exception - see license.txt and license-exception.md.
func TestRestoreIntegrityGuard(t *testing.T) {
	bundle := t.TempDir()
	write := func(rel string, content []byte) {
		p := filepath.Join(bundle, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, content, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	// A clean sqlite DB nested in the bundle.
	clean_path := filepath.Join(bundle, "app", "db", "clean.db")
	if err := os.MkdirAll(filepath.Dir(clean_path), 0o755); err != nil {
		t.Fatal(err)
	}
	c, err := sql.Open("sqlite3", "file:"+clean_path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Exec("create table t (id integer primary key); insert into t values (1)"); err != nil {
		t.Fatal(err)
	}
	c.Close()

	// Non-.db files and a .db-wal sibling must be ignored (only *.db is checked).
	write("notes.txt", []byte("not a database, and not checked"))
	write("app/db/clean.db-wal", []byte("garbage wal, not a standalone db"))

	// An all-clean bundle passes.
	if bad, err := restore_integrity_guard(bundle); bad != "" || err != nil {
		t.Fatalf("clean bundle rejected: bad=%q err=%v", bad, err)
	}

	// A corrupt .db is rejected and named.
	write("app2/db/corrupt.db", []byte("this is not a sqlite database — it is garbage bytes"))
	bad, err := restore_integrity_guard(bundle)
	if err != nil {
		t.Fatalf("guard errored: %v", err)
	}
	if bad != "corrupt.db" {
		t.Fatalf("corrupt DB not rejected (bad=%q, want corrupt.db)", bad)
	}
}

// restore_panic_bundle stages, for uid, what web_auth_restore hands the
// restore goroutine: a bundle whose user.db is already at version 1 yet has no
// accounts table, a person entity with its key, and a connected-account secret.
// The secret's update then meets the missing table and restore_auth panics.
func restore_panic_bundle(t *testing.T, uid string) (string, export_account, map[string]string, *export_secrets) {
	t.Helper()
	directory := purge_test_env(t, uid)
	// The core tables the purge's entity tombstone writes to.
	db_open("db/directory.db").exec("create table if not exists entries (entity text not null, peer text not null, primary key (entity, peer))")
	db_open("db/queue.db").exec("create table if not exists queue (id text primary key, from_entity text not null, to_entity text not null)")
	bundle := filepath.Join(directory, "restore", "staging")
	if err := os.MkdirAll(bundle, 0o700); err != nil {
		t.Fatal(err)
	}
	raw, err := sql.Open("sqlite3", filepath.Join(bundle, "user.db"))
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{"create table preferences (name text primary key, value text not null)", "pragma user_version=1"} {
		if _, err := raw.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	raw.Close()

	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	id := base58_encode(public)
	account := export_account{Entities: []export_entity{{ID: id, Class: "person", Name: "Panic", Privacy: "private"}}}
	keys := map[string]string{id: base58_encode(private)}
	secrets := &export_secrets{Accounts: []export_account_secret{{Id: "connected", Data: "{}"}}}
	return bundle, account, keys, secrets
}

// TestRestorePanicFailsTheRestore: the restore runs on a goroutine of its own,
// where a panic nothing recovers ends the process. A bundle that makes a step
// panic must instead fail its restore and remove the placeholder account, as
// any other failure does - and the removal must survive the same bundle's
// user.db, which also lacks the group memberships table.
func TestRestorePanicFailsTheRestore(t *testing.T) {
	// The bundle must really reach the panic, or the guard is never exercised.
	bundle, account, keys, secrets := restore_panic_bundle(t, "u-probe")
	panicked := func() (fault any) {
		defer func() { fault = recover() }()
		restore_apply("u-probe", bundle, export_manifest{}, account, keys, secrets)
		return nil
	}()
	if !strings.Contains(fmt.Sprint(panicked), "no such table: accounts") {
		t.Fatalf("the crafted bundle no longer makes restore_auth panic (got %v); the test proves nothing", panicked)
	}

	uid := "u-panic"
	bundle, account, keys, secrets = restore_panic_bundle(t, uid)
	restore_run(uid, bundle, export_manifest{}, account, keys, secrets)

	users := db_open("db/users.db")
	if exists, _ := users.exists("select 1 from users where uid=?", uid); exists {
		t.Error("the placeholder account survived a restore that panicked")
	}
	if exists, _ := users.exists("select 1 from entities where user=?", uid); exists {
		t.Error("the restored entity survived a restore that panicked")
	}
	if _, err := os.Stat(filepath.Join(data_dir, "users", uid)); !os.IsNotExist(err) {
		t.Errorf("the account's directory survived a restore that panicked: %v", err)
	}
}

// TestRestoreCleanupSurvivesAPanickingPurge: the startup sweep removes
// placeholders a restart left behind, on a goroutine of its own. A placeholder
// whose purge panics must not end the process at every start, nor keep the
// placeholders after it from being removed.
func TestRestoreCleanupSurvivesAPanickingPurge(t *testing.T) {
	purge_test_env(t, "u-first")
	users := db_open("db/users.db")
	users.exec("insert into users (uid, username) values ('u-second', 'u-second@example.com')")
	users.exec("update users set status='pending-restore'")
	sessions := db_open("db/sessions.db")
	sessions.exec("insert into sessions (user) values ('u-first'), ('u-second')")
	// Every purge now panics part way, after it has ended the sessions.
	users.exec("drop table totp")

	restore_cleanup_orphans()

	if exists, _ := sessions.exists("select 1 from sessions where user='u-second'"); exists {
		t.Error("the sweep stopped at the first placeholder whose purge panicked")
	}
}

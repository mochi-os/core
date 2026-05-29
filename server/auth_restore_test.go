package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"
)

// TestUserRestoreRoundTrip builds a bundle with user_export, then drives
// the restore helpers (unzip, decrypt, signature, schema guard, swap,
// entities, schedule, finish) to import it into a fresh destination uid.
// It asserts that the identity and its private key, an attachment file,
// the durable schedule event (re-keyed to the destination), and
// restore_source all land. This is the complement to
// TestUserExportRoundTrip and guards the restore path against regression.
func TestUserRestoreRoundTrip(t *testing.T) {
	cleanup := create_test_users_db(t)
	defer cleanup()

	db := db_open("db/users.db")
	// create_test_users_db uses the pre-v70 users schema; add the column
	// the restore path writes.
	db.exec("alter table users add column restore_source text not null default ''")
	db.exec("create table entities (id text not null primary key, private text not null, fingerprint text not null, user text not null references users(uid) on delete cascade, parent text not null default '', class text not null, name text not null, privacy text not null default 'public', data text not null default '', published integer not null default 0)")
	db.exec("create table relinks (user text not null, service text not null, identifier text not null default '', linked integer not null default 0, primary key (user, service))")
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

	attach := filepath.Join(data_dir, "users", src, "chat", "files")
	if err := os.MkdirAll(attach, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(attach, "note.txt"), []byte("hello world"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	passphrase := "correct horse battery staple"
	rel, err := user_export(src, "settings", passphrase)
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	zip_path := filepath.Join(data_dir, "users", src, "settings", "files", rel)

	// ---- Restore into a fresh destination ----
	stage := filepath.Join(data_dir, "users", "u-dst", "restore", "staging")
	bundle, err := restore_unzip(zip_path, stage)
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

	dst := "u-dst"
	db.exec("insert into users (uid, username) values (?, ?)", dst, "dst@example.com")
	if err := restore_swap(dst, bundle); err != nil {
		t.Fatalf("swap: %v", err)
	}
	restore_entities(dst, account, keys)
	restore_schedule(dst, bundle)
	restore_finish_account(dst, manifest, bundle)

	// ---- Assertions on the destination ----
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
}

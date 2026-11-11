// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"archive/zip"
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestUserExportRoundTrip(t *testing.T) {
	create_test_users_db(t)

	db := db_open("db/users.db")
	db.exec("create table entities (id text not null primary key, private text not null, fingerprint text not null, user text not null references users(uid) on delete cascade, parent text not null default '', class text not null, name text not null, privacy text not null default 'public', data text not null default '', published integer not null default 0)")
	sched := db_open("db/schedule.db")
	sched.exec("create table schedule (id integer primary key, user text not null, app text not null, due int not null, event text not null, data text not null, interval int not null, created int not null)")

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("keygen: %v", err)
	}
	id := base58_encode(pub)
	private := base58_encode(priv)
	uid := "u-export"
	db.exec("insert into users (uid, username) values (?, ?)", uid, "alice@example.com")
	db.exec("insert into entities (id, private, fingerprint, user, parent, class, name, privacy, data, published) values (?, ?, ?, ?, '', 'person', 'Alice', 'public', '', 0)",
		id, private, fingerprint(id), uid)
	sched.exec("insert into schedule (user, app, due, event, data, interval, created) values (?, 'chat', 100, 'remind', '{}', 0, 1)", uid)

	passphrase := "correct horse battery staple"
	rel, err := user_export(uid, "settings", passphrase, "")
	if err != nil {
		t.Fatalf("user_export: %v", err)
	}
	if !strings.HasPrefix(rel, "mochi-export/") || !strings.HasSuffix(rel, ".zip") {
		t.Fatalf("unexpected relative path %q", rel)
	}

	zip_path := data_dir + "/users/" + uid + "/settings/files/" + rel
	zr, err := zip.OpenReader(zip_path)
	if err != nil {
		t.Fatalf("open zip %q: %v", zip_path, err)
	}
	defer zr.Close()

	files := map[string][]byte{}
	for _, f := range zr.File {
		if f.FileInfo().IsDir() {
			continue
		}
		name := f.Name
		if i := strings.IndexByte(name, '/'); i >= 0 {
			name = name[i+1:] // strip top-level bundle directory
		}
		rc, err := f.Open()
		if err != nil {
			t.Fatalf("open zip entry %q: %v", f.Name, err)
		}
		b, _ := io.ReadAll(rc)
		rc.Close()
		files[name] = b
	}

	for _, want := range []string{"manifest.json", "user.json", "schedule.json", "linked.json", "keys.age"} {
		if _, ok := files[want]; !ok {
			t.Errorf("bundle missing %s", want)
		}
	}

	var manifest export_manifest
	if err := json.Unmarshal(files["manifest.json"], &manifest); err != nil {
		t.Fatalf("manifest: %v", err)
	}
	if manifest.Version != export_manifest_version {
		t.Errorf("version = %d, want %d", manifest.Version, export_manifest_version)
	}
	if !export_manifest_signed_by(manifest, id) {
		t.Error("manifest signature did not verify against the primary entity key")
	}

	tmp := t.TempDir() + "/keys.age"
	if err := os.WriteFile(tmp, files["keys.age"], 0o600); err != nil {
		t.Fatalf("write keys.age: %v", err)
	}
	keys, err := restore_decrypt_keys(tmp, passphrase)
	if err != nil {
		t.Fatalf("decrypt keys.age: %v", err)
	}
	if keys[id] != private {
		t.Errorf("keys[%s] = %q, want the entity private key", id, keys[id])
	}
	if _, err := restore_decrypt_keys(tmp, "wrong passphrase"); err == nil {
		t.Error("decrypt with wrong passphrase should fail")
	}

	// schedule.json carried the durable event; user.json the entity record.
	var events []export_schedule
	if err := json.Unmarshal(files["schedule.json"], &events); err != nil || len(events) != 1 {
		t.Errorf("schedule.json = %v (err %v), want 1 event", events, err)
	}
	var account export_account
	if err := json.Unmarshal(files["user.json"], &account); err != nil {
		t.Fatalf("user.json: %v", err)
	}
	if restore_primary_entity(account) != id {
		t.Errorf("user.json primary entity = %q, want %q", restore_primary_entity(account), id)
	}
}

// TestTheExportCarriesNoSecretInPlain. The staged copy of user.db, where the
// connected accounts' credentials and the hosted accounts' private keys are
// blanked, was never closed: the blanking stayed in its log, beside a main
// file that still held every one of them in the zip.
func TestTheExportCarriesNoSecretInPlain(t *testing.T) {
	create_test_users_db(t)
	users := db_open("db/users.db")
	users.exec("create table entities (id text not null primary key, private text not null, fingerprint text not null, user text not null references users(uid) on delete cascade, parent text not null default '', class text not null, name text not null, privacy text not null default 'public', data text not null default '', published integer not null default 0)")
	db_open("db/schedule.db").exec("create table schedule (id integer primary key, user text not null, app text not null, due int not null, event text not null, data text not null, interval int not null, created int not null)")
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("keygen: %v", err)
	}
	id := base58_encode(pub)
	uid := "u-secrets"
	users.exec("insert into users (uid, username) values (?, ?)", uid, "secrets@example.com")
	users.exec("insert into entities (id, private, fingerprint, user, parent, class, name, privacy, data, published) values (?, ?, ?, ?, '', 'person', 'Secrets', 'public', '', 0)",
		id, base58_encode(priv), fingerprint(id), uid)

	// One credential short, one past a page, so overflow pages are freed too.
	credential := "credential-4f1c9e"
	long := strings.Repeat("long-credential-8a2d7b ", 400)
	key := "private-key-c63e0a"
	u := &User{UID: uid, Username: "secrets@example.com", Role: "user", Preferences: map[string]string{}}
	live := db_user(u, "user")
	keys_table(live)
	live.exec("insert into accounts (id, type, data, created) values ('a1', 'ntfy', ?, 1), ('a2', 'url', ?, 1)", `{"token":"`+credential+`"}`, long)
	live.exec("insert into keys (account, kind, algorithm, public, private, created) values ('f1', 'signing', 'ed25519', 'public', ?, 1)", key)
	live.close()

	passphrase := "correct horse battery staple"
	rel, err := user_export(uid, "settings", passphrase, "")
	if err != nil {
		t.Fatalf("user_export: %v", err)
	}
	zr, err := zip.OpenReader(filepath.Join(data_dir, "users", uid, "settings", "files", rel))
	if err != nil {
		t.Fatalf("open zip: %v", err)
	}
	defer zr.Close()
	var secrets []byte
	for _, f := range zr.File {
		rc, err := f.Open()
		if err != nil {
			t.Fatalf("open %s: %v", f.Name, err)
		}
		content, _ := io.ReadAll(rc)
		rc.Close()
		for _, secret := range []string{credential, "long-credential-8a2d7b", key} {
			if bytes.Contains(content, []byte(secret)) {
				t.Errorf("THE POINT: %s carries %q in plain", f.Name, secret)
			}
		}
		if strings.HasSuffix(f.Name, "-wal") || strings.HasSuffix(f.Name, "-shm") {
			t.Errorf("the bundle carries the copy's %s", f.Name)
		}
		if strings.HasSuffix(f.Name, "/secrets.age") {
			secrets = content
		}
	}

	// They travel in secrets.age, under the passphrase.
	path := filepath.Join(t.TempDir(), "secrets.age")
	if err := os.WriteFile(path, secrets, 0o600); err != nil {
		t.Fatalf("write secrets.age: %v", err)
	}
	decrypted, err := restore_decrypt_secrets(path, passphrase)
	if err != nil {
		t.Fatalf("decrypt secrets.age: %v", err)
	}
	found := map[string]string{}
	for _, account := range decrypted.Accounts {
		found[account.Id] = account.Data
	}
	if !strings.Contains(found["a1"], credential) || found["a2"] != long {
		t.Errorf("secrets.age carries accounts %v", found)
	}
	if len(decrypted.Keys) != 1 || decrypted.Keys[0].Private != key {
		t.Errorf("secrets.age carries keys %+v", decrypted.Keys)
	}

	// Nothing of the staged copy is left open.
	databases_lock.Lock()
	defer databases_lock.Unlock()
	for key := range databases {
		if strings.Contains(key, string(filepath.Separator)+"staging"+string(filepath.Separator)) {
			t.Errorf("the staged copy is still open: %s", key)
		}
	}
}

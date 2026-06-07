package main

import (
	"archive/zip"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"
)

// TestUserExportRoundTrip exercises the whole export engine: it builds a
// migration bundle for a user with a real person-entity keypair, then
// verifies the zip contains a signed manifest that validates against the
// entity's public key and a keys.age that decrypts with the passphrase
// and carries the entity private key.
func TestUserExportRoundTrip(t *testing.T) {
	cleanup := create_test_users_db(t)
	defer cleanup()

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

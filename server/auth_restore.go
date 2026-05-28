// Mochi server: POST /_/auth/restore — signup-with-restore-from-bundle
// Copyright Alistair Cunningham 2026
//
// The "Advanced disclosure / restore" path on the signup form. The user
// uploads a migration bundle (produced by mochi.user.export with keys)
// and its passphrase. Unlike replicate (which links to a still-running
// source), restore is single-shot: the destination becomes the new home
// for the account's data and network identity, and the source is left
// untouched (the user deletes it themselves — see the post-restore
// banner driven by users.restore_source).
//
// Modelled on auth_replicate.go. Validation that should give the user a
// fast inline error (bad passphrase, wrong bundle mode, schema too new)
// runs synchronously; the actual unpack-and-swap runs in a goroutine so
// a multi-GB restore doesn't block the HTTP response past its timeout.
// The placeholder sits in status='pending-restore' until the swap
// completes, gating every app but /login (see user_pending).

package main

import (
	"archive/zip"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"filippo.io/age"
	"github.com/gin-gonic/gin"
)

// restore_cleanup_orphans deletes pending-restore placeholders left by a
// server restart mid-restore. The apply step runs in a goroutine that
// does not survive a process exit, so any user still pending-restore at
// startup has an incomplete (and unusable) account — delete it so the
// user can simply sign up and restore again. Runs once at startup, not
// periodically.
func restore_cleanup_orphans() {
	udb := db_open("db/users.db")
	rows, err := udb.rows("select uid from users where status='pending-restore'")
	if err != nil {
		return
	}
	for _, r := range rows {
		uid := as_string(r["uid"])
		if uid == "" {
			continue
		}
		info("Restore: deleting orphaned pending-restore placeholder %q (restarted mid-restore)", uid)
		user_delete(uid)
	}
}

// web_auth_restore is POST /_/auth/restore.
// multipart/form-data: email, passphrase, bundle (file).
func web_auth_restore(c *gin.Context) {
	email := strings.TrimSpace(c.PostForm("email"))
	passphrase := c.PostForm("passphrase")

	if email == "" {
		respond_error(c, http.StatusBadRequest, "invalid_request", "errors.invalid_request", nil)
		return
	}
	if !email_valid(email) {
		respond_error(c, http.StatusBadRequest, "invalid_email", "errors.invalid_email", nil)
		return
	}
	if !setting_signup_enabled() {
		respond_error(c, http.StatusForbidden, "signup_disabled", "errors.signup_disabled", nil)
		return
	}

	upload, err := c.FormFile("bundle")
	if err != nil {
		respond_error(c, http.StatusBadRequest, "bundle_required", "errors.bundle_required", nil)
		return
	}

	udb := db_open("db/users.db")
	if taken, _ := udb.exists("select 1 from users where username=?", email); taken {
		respond_error(c, http.StatusConflict, "username_taken", "errors.username_taken", nil)
		return
	}

	// Create the placeholder with a fresh destination-side uid. The source
	// uid in the bundle is informational only; the destination's uid is
	// canonical. First-user-becomes-administrator, exactly as user_create
	// and auth_replicate do — role is never taken from the bundle.
	role := "user"
	if has, _ := udb.exists("select uid from users limit 1"); !has {
		role = "administrator"
	}
	uid := uid()
	udb.exec("insert into users (uid, username, role, status) values (?, ?, ?, 'pending-restore')", uid, email, role)

	// Belt-and-braces freshness check before any staging, so the walk
	// never has to skip the restore/ directory.
	if !user_is_fresh(uid) {
		user_delete(uid)
		respond_error(c, http.StatusConflict, "account_not_fresh", "errors.account_not_fresh", nil)
		return
	}

	// Save and unpack the bundle under the user's own data dir (same
	// filesystem as the eventual destination, so the swap is a real
	// rename(2), not a cross-filesystem copy).
	restore_dir := filepath.Join(data_dir, "users", uid, "restore")
	if err := os.MkdirAll(restore_dir, 0o700); err != nil {
		user_delete(uid)
		respond_error(c, http.StatusInternalServerError, "restore_failed", "errors.restore_failed", nil)
		return
	}
	zip_path := filepath.Join(restore_dir, "bundle.zip")
	if err := c.SaveUploadedFile(upload, zip_path); err != nil {
		user_delete(uid)
		respond_error(c, http.StatusInternalServerError, "restore_failed", "errors.restore_failed", nil)
		return
	}

	staging := filepath.Join(restore_dir, "staging")
	bundle, err := restore_unzip(zip_path, staging)
	if err != nil {
		user_delete(uid)
		respond_error(c, http.StatusBadRequest, "bundle_invalid", "errors.bundle_invalid", nil)
		return
	}

	// Manifest: must be a v1 migration bundle.
	var manifest export_manifest
	if err := restore_read_json(filepath.Join(bundle, "manifest.json"), &manifest); err != nil {
		user_delete(uid)
		respond_error(c, http.StatusBadRequest, "bundle_invalid", "errors.bundle_invalid", nil)
		return
	}
	if manifest.Version != export_manifest_version {
		user_delete(uid)
		respond_error(c, http.StatusBadRequest, "bundle_version", "errors.bundle_version", nil)
		return
	}
	if manifest.Mode != "migration" {
		// A GDPR bundle has no keys.age, so the restored user could not act
		// as the source identity. Refuse with a clear message.
		user_delete(uid)
		respond_error(c, http.StatusBadRequest, "bundle_not_migration", "errors.bundle_not_migration", nil)
		return
	}

	// Decrypt keys.age (validates the passphrase) and read the account so
	// we can find the primary entity that signed the manifest.
	keys, err := restore_decrypt_keys(filepath.Join(bundle, "keys.age"), passphrase)
	if err != nil {
		user_delete(uid)
		respond_error(c, http.StatusBadRequest, "wrong_passphrase", "errors.wrong_passphrase", nil)
		return
	}
	var account export_account
	if err := restore_read_json(filepath.Join(bundle, "user.json"), &account); err != nil {
		user_delete(uid)
		respond_error(c, http.StatusBadRequest, "bundle_invalid", "errors.bundle_invalid", nil)
		return
	}
	primary := restore_primary_entity(account)
	if primary == "" || keys[primary] == "" {
		user_delete(uid)
		respond_error(c, http.StatusBadRequest, "bundle_invalid", "errors.bundle_invalid", nil)
		return
	}

	// Verify the manifest signature against the primary entity's public
	// key (the entity id is the key). Defends against tampering between
	// export and restore.
	if !export_manifest_signed_by(manifest, primary) {
		user_delete(uid)
		respond_error(c, http.StatusBadRequest, "bundle_tampered", "errors.bundle_tampered", nil)
		return
	}

	// Entity-collision check: every entity in the bundle must be absent
	// from this server. Importing an identity already hosted here would
	// fork it.
	for _, e := range account.Entities {
		if here, _ := udb.exists("select 1 from entities where id=?", e.ID); here {
			user_delete(uid)
			respond_error(c, http.StatusConflict, "entity_collision", "errors.entity_collision", nil)
			return
		}
	}

	// Forward-only: refuse a bundle whose app data is newer than this
	// server supports, BEFORE any staged DB is opened through db_open
	// (whose auto-downgrade path would otherwise silently fire).
	if app, _ := restore_schema_guard(bundle); app != "" {
		user_delete(uid)
		respond_error(c, http.StatusConflict, "bundle_schema_newer", "errors.bundle_schema_newer", map[string]any{"app": app})
		return
	}

	// Synchronous validation passed. Issue a session and apply the rest in
	// the background; the client polls /_/identity and the progress route
	// from the /restoring page.
	session := login_create(uid, c.ClientIP(), c.Request.UserAgent())
	web_cookie_set(c, "session", session)
	restore_progress(uid, "validated", 5, "")
	go restore_apply(uid, bundle, manifest, account, keys)

	c.JSON(http.StatusOK, gin.H{"status": "pending", "uid": uid})
}

// restore_apply does the destructive part: verify file hashes, swap the
// staged tree into place, re-insert core rows, install entity keys, then
// flip the user to active. On any failure the placeholder is deleted
// (which removes users/<uid>/ entirely), matching replication cleanup.
func restore_apply(uid, bundle string, manifest export_manifest, account export_account, keys map[string]string) {
	fail := func(reason string) {
		warn("Restore failed for user %q: %s", uid, reason)
		restore_progress(uid, "error", 100, reason)
		user_delete(uid)
	}

	// Integrity: every file the manifest names must match its hash.
	restore_progress(uid, "verifying", 15, "")
	for rel, want := range manifest.Files {
		got, _, err := export_hash(filepath.Join(bundle, filepath.FromSlash(rel)))
		if err != nil || got != want.Hash {
			fail("bundle_tampered")
			return
		}
	}

	// Atomic-ish swap: rename each data entry into the user's directory.
	restore_progress(uid, "unpacking", 45, "")
	if err := restore_swap(uid, bundle); err != nil {
		fail("swap: " + err.Error())
		return
	}

	// Core-DB rows scoped to the user.
	restore_progress(uid, "linking", 75, "")
	restore_entities(uid, account, keys)
	restore_schedule(uid, bundle)
	restore_finish_account(uid, manifest, bundle)

	// Migrations run lazily on first app access via db_open's forward
	// ladder; the schema guard already ruled out any downgrade. Flip to
	// active so the gates release and the user lands on a populated home.
	restore_progress(uid, "migrating", 95, "")
	udb := db_open("db/users.db")
	udb.exec("update users set status='active' where uid=? and status='pending-restore'", uid)
	db_purge_prefix(filepath.Join("users", uid))

	// Restore is single-shot. On a replication-paired destination a follow
	// up bulk-bootstrap would push the freshly-restored user to the pair in
	// one pass; that optimisation is conditional on pairing and tracked
	// separately.
	restore_progress(uid, "done", 100, "")
	_ = os.RemoveAll(filepath.Join(data_dir, "users", uid, "restore"))
}

// restore_swap renames each top-level data entry from the staged bundle
// into the user's data directory, replacing the fresh placeholder files.
// Metadata files are consumed, not stored.
func restore_swap(uid, bundle string) error {
	dest := filepath.Join(data_dir, "users", uid)
	db_purge_prefix(filepath.Join("users", uid))

	entries, err := os.ReadDir(bundle)
	if err != nil {
		return err
	}
	for _, e := range entries {
		switch e.Name() {
		case "manifest.json", "user.json", "schedule.json", "linked.json", "keys.age":
			continue
		}
		dst := filepath.Join(dest, e.Name())
		_ = os.RemoveAll(dst)
		if err := os.Rename(filepath.Join(bundle, e.Name()), dst); err != nil {
			return fmt.Errorf("rename %s: %w", e.Name(), err)
		}
	}
	db_purge_prefix(filepath.Join("users", uid))
	return nil
}

// restore_entities inserts the bundle's entities under the destination
// uid with their private keys, and republishes public ones to the
// directory so the network learns the new host.
func restore_entities(uid string, account export_account, keys map[string]string) {
	udb := db_open("db/users.db")
	for _, e := range account.Entities {
		private := keys[e.ID]
		udb.exec("replace into entities (id, private, fingerprint, user, parent, class, name, privacy, data, published) values (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)",
			e.ID, private, e.Fingerprint, uid, e.Parent, e.Class, e.Name, e.Privacy, e.Data)
		if e.Privacy == "public" {
			ent := Entity{ID: e.ID, Private: private, Fingerprint: e.Fingerprint, User: uid, Parent: e.Parent, Class: e.Class, Name: e.Name, Privacy: e.Privacy, Data: e.Data}
			directory_create(&ent)
			directory_publish(&ent, true)
		}
	}
}

// restore_schedule re-inserts the bundle's durable scheduled events under
// the destination uid. Absolute due times are preserved.
func restore_schedule(uid, bundle string) {
	var events []export_schedule
	if err := restore_read_json(filepath.Join(bundle, "schedule.json"), &events); err != nil {
		return
	}
	db := db_open("db/schedule.db")
	for _, s := range events {
		db.exec("insert into schedule (user, app, due, event, data, interval, created) values (?, ?, ?, ?, ?, ?, ?)",
			uid, s.App, s.Due, s.Event, s.Data, s.Interval, s.Created)
	}
}

// restore_finish_account records the source server (drives the cleanup
// banner) and the pending re-links from linked.json.
func restore_finish_account(uid string, manifest export_manifest, bundle string) {
	udb := db_open("db/users.db")
	udb.exec("update users set restore_source=? where uid=?", manifest.Source, uid)

	var links []export_link
	if err := restore_read_json(filepath.Join(bundle, "linked.json"), &links); err != nil {
		return
	}
	for _, l := range links {
		udb.exec("replace into relinks (user, service, identifier, linked) values (?, ?, ?, ?)",
			uid, l.Service, l.Identifier, l.Linked)
	}
}

// restore_unzip extracts zip_path into dest and returns the bundle root
// (the single top-level directory inside the archive). Guards against
// path traversal in entry names.
func restore_unzip(zip_path, dest string) (string, error) {
	r, err := zip.OpenReader(zip_path)
	if err != nil {
		return "", err
	}
	defer r.Close()

	clean := filepath.Clean(dest) + string(os.PathSeparator)
	var top string
	for _, f := range r.File {
		target := filepath.Join(dest, f.Name)
		if !strings.HasPrefix(filepath.Clean(target)+string(os.PathSeparator), clean) &&
			filepath.Clean(target) != filepath.Clean(dest) {
			return "", fmt.Errorf("unsafe path %q in bundle", f.Name)
		}
		if first := strings.SplitN(filepath.ToSlash(f.Name), "/", 2)[0]; first != "" && top == "" {
			top = first
		}
		if f.FileInfo().IsDir() {
			if err := os.MkdirAll(target, 0o700); err != nil {
				return "", err
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
			return "", err
		}
		in, err := f.Open()
		if err != nil {
			return "", err
		}
		out, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
		if err != nil {
			in.Close()
			return "", err
		}
		if _, err := io.Copy(out, in); err != nil {
			in.Close()
			out.Close()
			return "", err
		}
		in.Close()
		out.Close()
	}
	if top == "" {
		return "", fmt.Errorf("empty bundle")
	}
	return filepath.Join(dest, top), nil
}

// restore_decrypt_keys decrypts keys.age with the passphrase and returns
// the entity-id -> base58-private-key map.
func restore_decrypt_keys(path, passphrase string) (map[string]string, error) {
	if passphrase == "" {
		return nil, fmt.Errorf("passphrase required")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	identity, err := age.NewScryptIdentity(passphrase)
	if err != nil {
		return nil, err
	}
	reader, err := age.Decrypt(f, identity)
	if err != nil {
		return nil, err
	}
	plain, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	keys := map[string]string{}
	if err := json.Unmarshal(plain, &keys); err != nil {
		return nil, err
	}
	return keys, nil
}

// restore_primary_entity returns the id of the account's person-class
// entity (the one that signs the manifest), or "".
func restore_primary_entity(account export_account) string {
	for _, e := range account.Entities {
		if e.Class == "person" {
			return e.ID
		}
	}
	return ""
}

// restore_schema_guard reports the first installed app whose staged DB is
// newer than this server supports (bundle user_version > app.json.schema),
// or "" if every app is restorable. Reads user_version without db_open so
// no migration — and crucially no auto-downgrade — fires.
func restore_schema_guard(bundle string) (string, error) {
	type limit struct {
		id     string
		schema int
	}
	by_file := map[string]limit{}
	apps_lock.Lock()
	for _, a := range apps {
		av := a.active_locked(nil)
		if av != nil && av.Database.File != "" {
			by_file[av.Database.File] = limit{a.id, av.Database.Schema}
		}
	}
	apps_lock.Unlock()

	var newer string
	err := filepath.WalkDir(bundle, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		l, ok := by_file[d.Name()]
		if !ok {
			return nil
		}
		version, verr := restore_db_version(path)
		if verr != nil {
			return nil
		}
		if version > l.schema {
			newer = l.id
			return io.EOF
		}
		return nil
	})
	if err == io.EOF {
		err = nil
	}
	return newer, err
}

// restore_db_version reads a SQLite DB's pragma user_version read-only,
// without the db_open migration wrapper.
func restore_db_version(path string) (int, error) {
	d, err := sql.Open("sqlite3", "file:"+path+"?mode=ro")
	if err != nil {
		return 0, err
	}
	defer d.Close()
	var version int
	if err := d.QueryRow("pragma user_version").Scan(&version); err != nil {
		return 0, err
	}
	return version, nil
}

func restore_read_json(path string, value any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, value)
}

// restore_progress writes the latest step snapshot for the /restoring
// page to poll via /_/auth/restore/progress.
func restore_progress(uid, step string, percent int, detail string) {
	dir := filepath.Join(data_dir, "users", uid, "restore")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return
	}
	data, _ := json.Marshal(gin.H{"step": step, "percent": percent, "detail": detail})
	_ = os.WriteFile(filepath.Join(dir, "progress.json"), data, 0o600)
}

// web_auth_restore_progress is GET /_/auth/restore/progress — the latest
// progress snapshot for the authenticated (pending-restore) user.
func web_auth_restore_progress(c *gin.Context) {
	u := web_auth(c)
	if u == nil {
		respond_error(c, http.StatusUnauthorized, "authentication_required", "errors.authentication_required", nil)
		return
	}
	data, err := os.ReadFile(filepath.Join(data_dir, "users", u.UID, "restore", "progress.json"))
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"step": "", "percent": 0})
		return
	}
	c.Data(http.StatusOK, "application/json", data)
}

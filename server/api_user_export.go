// Mochi server: user data export (GDPR download + server-move bundle)
// Copyright Alistair Cunningham 2026
//
// mochi.user.export(passphrase) -> path builds a .zip bundle of
// everything the server holds about the calling user and returns its
// path. The settings app streams the file to the browser. The bundle
// carries the user's private keys, so the settings action gates this
// behind step-up re-authentication (mochi.user.session.reauthenticate)
// before calling it.
//
// Every export is a complete, restorable backup: the user's data plus
// keys.age, a passphrase-encrypted blob of their entity private keys, so
// the bundle can be restored onto another server as the same network
// identity. The data files inside the zip are plaintext (the user can
// always read their own data); only the keys are passphrase-protected.
// A non-restorable "data only" variant was deliberately dropped — a
// backup you can't restore is a footgun.
//
// The bundle is self-describing via manifest.json, which carries a
// per-file sha256 and a signature over those hashes made with the
// user's primary entity key. Restore verifies both before doing
// anything destructive (see auth_restore.go).

package main

import (
	"archive/zip"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"filippo.io/age"
	sl "go.starlark.net/starlark"
)

const export_manifest_version = 1

// export_file is one entry in the manifest's integrity map.
type export_file struct {
	Hash  string `json:"sha256"`
	Bytes int64  `json:"bytes"`
}

// export_entity is one of the user's entities, sans private key (the
// private keys travel only in keys.age).
type export_entity struct {
	ID          string `json:"id"`
	Fingerprint string `json:"fingerprint"`
	Parent      string `json:"parent"`
	Class       string `json:"class"`
	Name        string `json:"name"`
	Privacy     string `json:"privacy"`
	Data        string `json:"data"`
}

// export_account is the user's core users.db row plus entity records.
// username and role are recorded for the user's own reference (GDPR
// completeness) but are operator-owned and never imported on restore.
// methods/disabled (the per-user login-requirement config) ARE imported on
// restore, filtered to the factors whose credential can be re-established on
// the destination (see restore_safe_methods).
type export_account struct {
	UID      string          `json:"uid"`
	Username string          `json:"username"`
	Role     string          `json:"role"`
	Methods  string          `json:"methods"`
	Disabled string          `json:"disabled"`
	Status   string          `json:"status"`
	Entities []export_entity `json:"entities"`
	// Passkeys can't be restored (they're bound to the source origin), so
	// they don't travel in the bundle. Recording that the account HAD them
	// lets the destination prompt the user to re-register on the new host.
	Passkeys bool `json:"passkeys"`
}

// export_totp is the user's authenticator secret. The secret is device
// independent (unlike a passkey, which is bound to its origin), so it can be
// restored and the user's existing authenticator entries keep working.
type export_totp struct {
	Secret   string `json:"secret"`
	Verified int64  `json:"verified"`
	Created  int64  `json:"created"`
}

// export_recovery is one stored recovery-code hash. Restoring the hashes
// means the user's already-saved codes keep working on the destination.
type export_recovery struct {
	Hash    string `json:"hash"`
	Created int64  `json:"created"`
}

// export_secrets is the passphrase-encrypted secrets.age payload: the
// credentials that are safe and reliable to restore (authenticator secret,
// recovery-code hashes). Passkeys are deliberately excluded — they're bound
// to the source origin and won't authenticate after a server/domain move, so
// the user re-registers them on the destination.
type export_secrets struct {
	Totp     *export_totp      `json:"totp"`
	Recovery []export_recovery `json:"recovery"`
}

// export_schedule is one durable scheduled event from core schedule.db.
type export_schedule struct {
	App      string `json:"app"`
	Due      int64  `json:"due"`
	Event    string `json:"event"`
	Data     string `json:"data"`
	Interval int64  `json:"interval"`
	Created  int64  `json:"created"`
}

// export_link is a reference (no credential) to a linked third-party
// service, driving the re-link banner on the destination.
type export_link struct {
	Service    string `json:"service"`
	Identifier string `json:"identifier"`
	Linked     int64  `json:"linked_at"`
}

// export_manifest is the top-level bundle descriptor. Signature is the
// last field and omitempty so the signing payload is the manifest with
// the signature absent; restore recomputes the identical bytes.
type export_manifest struct {
	Version     int                    `json:"version"`
	Source      string                 `json:"source_server"`
	Exported    string                 `json:"exported_at"`
	Fingerprint string                 `json:"user_fingerprint"`
	Files       map[string]export_file `json:"files"`
	Signature   string                 `json:"signature,omitempty"`
}

// api_user_export is mochi.user.export(passphrase).
func api_user_export(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := require_permission(t, fn, "user/export"); err != nil {
		return sl_error(fn, "%v", err)
	}

	user, _ := t.Local("user").(*User)
	app, _ := t.Local("app").(*App)
	if user == nil || app == nil {
		return sl_error(fn, "no user")
	}

	var passphrase string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "passphrase", &passphrase); err != nil {
		return sl_error(fn, "%v", err)
	}
	if passphrase == "" {
		return sl_error(fn, "passphrase required")
	}

	path, err := user_export(user.UID, app.id, passphrase)
	if err != nil {
		return sl_error(fn, "%v", err)
	}

	// Record the export in the audit log: a key-bearing operation, and
	// the durable trail if a stolen session ever triggers one.
	ip := ""
	if action, ok := t.Local("action").(*Action); ok && action.web != nil {
		ip = rate_limit_client_ip(action.web)
	}
	audit_export(user.Username, ip)

	return sl.String(path), nil
}

// user_export assembles the bundle for uid and writes the finished .zip
// into the calling app's files directory under mochi-export/, returning
// the app-relative path so the action can stream it with a.write.file.
// The bundle is built in a staging tree under users/<uid>/export/ first,
// then zipped across into the app files area.
func user_export(uid, app, passphrase string) (string, error) {
	if passphrase == "" {
		return "", fmt.Errorf("passphrase required")
	}

	root := filepath.Join(data_dir, "users", uid)
	export_cleanup_orphans(uid, app)
	udb := db_open("db/users.db")

	// Primary (person-class) entity signs the manifest and names the
	// bundle. Every user has exactly one at signup.
	var primary Entity
	if !udb.scan(&primary, "select * from entities where user=? and class='person' limit 1", uid) {
		return "", fmt.Errorf("no primary entity for user")
	}

	stamp := time.Unix(now(), 0).UTC()
	bundle := fmt.Sprintf("mochi-export-%s-%s-%s", stamp.Format("20060102-150405"), primary.Fingerprint, export_suffix())
	staging := filepath.Join(root, "export", "staging")
	tree := filepath.Join(staging, bundle)
	if err := os.MkdirAll(tree, 0o700); err != nil {
		return "", fmt.Errorf("create staging: %w", err)
	}
	// Remove only this export's own tree (its name carries a random
	// suffix), never the shared staging directory — a concurrent export
	// for the same user has its own tree under staging/ and must not be
	// wiped. Empty staging/ is reaped by export_cleanup_orphans.
	defer os.RemoveAll(tree)

	// Wholesale per-user data: top-level user.db, then every app/entity
	// subtree. The walk is opaque to what an app stores there.
	if file_exists(filepath.Join(root, "user.db")) {
		if _, err := snapshot_copy_db(filepath.Join(root, "user.db"), filepath.Join(tree, "user.db")); err != nil {
			return "", fmt.Errorf("copy user.db: %w", err)
		}
	}
	entries, err := os.ReadDir(root)
	if err != nil && !os.IsNotExist(err) {
		return "", fmt.Errorf("read user directory: %w", err)
	}
	for _, e := range entries {
		name := e.Name()
		if !e.IsDir() || name == "export" || name == "restore" {
			continue
		}
		if err := export_copy_subtree(filepath.Join(root, name), filepath.Join(tree, name)); err != nil {
			return "", err
		}
	}

	// Core-DB rows scoped to this user, extracted to JSON.
	if err := export_account_json(udb, uid, &primary, filepath.Join(tree, "user.json")); err != nil {
		return "", err
	}
	if err := export_schedule_json(uid, filepath.Join(tree, "schedule.json")); err != nil {
		return "", err
	}
	if err := export_linked_json(udb, uid, filepath.Join(tree, "linked.json")); err != nil {
		return "", err
	}

	if err := export_keys_age(udb, uid, passphrase, filepath.Join(tree, "keys.age")); err != nil {
		return "", err
	}
	if err := export_secrets_age(udb, uid, passphrase, filepath.Join(tree, "secrets.age")); err != nil {
		return "", err
	}

	// Manifest: hash every staged file, then sign the lot with the
	// primary entity key.
	manifest := export_manifest{
		Version:     export_manifest_version,
		Source:      export_source_server(),
		Exported:    stamp.Format(time.RFC3339),
		Fingerprint: primary.Fingerprint,
		Files:       map[string]export_file{},
	}
	if err := filepath.WalkDir(tree, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		hash, bytes, herr := export_hash(path)
		if herr != nil {
			return herr
		}
		rel, _ := filepath.Rel(tree, path)
		manifest.Files[filepath.ToSlash(rel)] = export_file{Hash: hash, Bytes: bytes}
		return nil
	}); err != nil {
		return "", fmt.Errorf("hash bundle: %w", err)
	}

	payload, err := json.Marshal(manifest)
	if err != nil {
		return "", err
	}
	signature := entity_sign(primary.ID, string(payload))
	if signature == "" {
		return "", fmt.Errorf("sign manifest: empty signature")
	}
	manifest.Signature = signature
	final, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(tree, "manifest.json"), final, 0o600); err != nil {
		return "", err
	}

	relative := filepath.ToSlash(filepath.Join("mochi-export", bundle+".zip"))
	zip_path := filepath.Join(root, app, "files", filepath.FromSlash(relative))
	if err := os.MkdirAll(filepath.Dir(zip_path), 0o700); err != nil {
		return "", fmt.Errorf("create download directory: %w", err)
	}
	if err := export_zip(tree, bundle, zip_path, stamp); err != nil {
		return "", fmt.Errorf("zip bundle: %w", err)
	}
	return relative, nil
}

// export_copy_subtree mirrors src into dst, snapshot-copying *.db files
// (online backup, safe against concurrent writers) and plain-copying
// everything else. SQLite sidecars and generated thumbnails are skipped.
func export_copy_subtree(src, dst string) error {
	return filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, path)
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			// Generated thumbnails are a cache, never user content. The
			// mochi-export staging area holds prior bundles — never recurse
			// into it.
			if filepath.Base(filepath.Dir(path)) == "files" && (d.Name() == "thumbnails" || d.Name() == "mochi-export") {
				return filepath.SkipDir
			}
			return os.MkdirAll(target, 0o700)
		}
		name := d.Name()
		// SQLite write-ahead-log / shared-memory sidecars and the operator
		// backup machinery's snapshot siblings: the online-backup copy is a
		// self-contained DB, so all of these are redundant duplicates.
		if strings.HasSuffix(name, "-wal") || strings.HasSuffix(name, "-shm") ||
			strings.HasSuffix(name, ".db.snap") || strings.HasSuffix(name, ".db.backup") {
			return nil
		}
		if strings.HasSuffix(name, ".db") {
			if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
				return err
			}
			_, err := snapshot_copy_db(path, target)
			return err
		}
		return export_copy_file(path, target)
	})
}

func export_copy_file(src, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

func export_hash(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

// export_account_json writes the user's core users.db row and entity
// records to user.json. Credentials are never included.
func export_account_json(udb *DB, uid string, primary *Entity, path string) error {
	account := export_account{UID: uid, Username: primary.User, Entities: []export_entity{}}
	if row, _ := udb.row("select username, role, methods, disabled, status from users where uid=?", uid); row != nil {
		account.Username, _ = row["username"].(string)
		account.Role, _ = row["role"].(string)
		account.Methods, _ = row["methods"].(string)
		account.Disabled, _ = row["disabled"].(string)
		account.Status, _ = row["status"].(string)
	}
	if row, _ := udb.row("select count(*) as count from credentials where user=?", uid); row != nil {
		account.Passkeys = as_int64(row["count"]) > 0
	}
	var entities []Entity
	if err := udb.scans(&entities, "select * from entities where user=?", uid); err != nil {
		return fmt.Errorf("read entities: %w", err)
	}
	for _, e := range entities {
		account.Entities = append(account.Entities, export_entity{
			ID: e.ID, Fingerprint: e.Fingerprint, Parent: e.Parent,
			Class: e.Class, Name: e.Name, Privacy: e.Privacy, Data: e.Data,
		})
	}
	return export_write_json(path, account)
}

// export_schedule_json writes the user's durable scheduled events.
func export_schedule_json(uid, path string) error {
	db := db_open("db/schedule.db")
	rows, err := db.rows("select app, due, event, data, interval, created from schedule where user=?", uid)
	if err != nil {
		return fmt.Errorf("read schedule: %w", err)
	}
	out := []export_schedule{}
	for _, r := range rows {
		out = append(out, export_schedule{
			App:      as_string(r["app"]),
			Due:      as_int64(r["due"]),
			Event:    as_string(r["event"]),
			Data:     as_string(r["data"]),
			Interval: as_int64(r["interval"]),
			Created:  as_int64(r["created"]),
		})
	}
	return export_write_json(path, out)
}

// export_linked_json writes references (no credentials) to the user's
// linked OAuth services.
func export_linked_json(udb *DB, uid, path string) error {
	rows, err := udb.rows("select provider, email, name, created from oauth where user=?", uid)
	if err != nil {
		// oauth table absent on a never-linked server: empty list.
		return export_write_json(path, []export_link{})
	}
	out := []export_link{}
	for _, r := range rows {
		identifier := as_string(r["email"])
		if identifier == "" {
			identifier = as_string(r["name"])
		}
		out = append(out, export_link{
			Service:    as_string(r["provider"]),
			Identifier: identifier,
			Linked:     as_int64(r["created"]),
		})
	}
	return export_write_json(path, out)
}

// export_keys_age writes the user's entity private keys to keys.age,
// passphrase-encrypted with age (scrypt). Contents: a JSON map of
// entity id -> base58 private key.
func export_keys_age(udb *DB, uid, passphrase, path string) error {
	var entities []Entity
	if err := udb.scans(&entities, "select * from entities where user=?", uid); err != nil {
		return fmt.Errorf("read entity keys: %w", err)
	}
	keys := map[string]string{}
	for _, e := range entities {
		keys[e.ID] = e.Private
	}
	plain, err := json.Marshal(keys)
	if err != nil {
		return err
	}

	recipient, err := age.NewScryptRecipient(passphrase)
	if err != nil {
		return fmt.Errorf("passphrase recipient: %w", err)
	}
	out, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer out.Close()
	w, err := age.Encrypt(out, recipient)
	if err != nil {
		return err
	}
	if _, err := w.Write(plain); err != nil {
		w.Close()
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return out.Close()
}

// export_secrets_age writes the user's restorable auth credentials
// (authenticator secret + recovery-code hashes) to secrets.age,
// passphrase-encrypted with age (scrypt) — the same protection tier as
// keys.age, which already carries the entity private keys. Always written so
// the restore path is uniform; an account with neither yields empty fields.
func export_secrets_age(udb *DB, uid, passphrase, path string) error {
	secrets := export_secrets{Recovery: []export_recovery{}}
	if row, _ := udb.row("select secret, verified, created from totp where user=?", uid); row != nil {
		if secret := as_string(row["secret"]); secret != "" {
			secrets.Totp = &export_totp{
				Secret:   secret,
				Verified: as_int64(row["verified"]),
				Created:  as_int64(row["created"]),
			}
		}
	}
	if rows, _ := udb.rows("select hash, created from recovery where user=?", uid); rows != nil {
		for _, r := range rows {
			secrets.Recovery = append(secrets.Recovery, export_recovery{
				Hash:    as_string(r["hash"]),
				Created: as_int64(r["created"]),
			})
		}
	}

	plain, err := json.Marshal(secrets)
	if err != nil {
		return err
	}
	recipient, err := age.NewScryptRecipient(passphrase)
	if err != nil {
		return fmt.Errorf("passphrase recipient: %w", err)
	}
	out, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer out.Close()
	w, err := age.Encrypt(out, recipient)
	if err != nil {
		return err
	}
	if _, err := w.Write(plain); err != nil {
		w.Close()
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return out.Close()
}

func export_write_json(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}

// export_zip writes the staged tree into a .zip whose top-level entry
// is the bundle directory. DBs and JSON deflate well; already-compressed
// media under */files/ is stored. Entries are sorted with a fixed
// timestamp for reproducible output.
func export_zip(tree, bundle, zip_path string, stamp time.Time) error {
	var files []string
	if err := filepath.WalkDir(tree, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		files = append(files, path)
		return nil
	}); err != nil {
		return err
	}
	sort.Strings(files)

	out, err := os.OpenFile(zip_path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer out.Close()
	zw := zip.NewWriter(out)

	for _, path := range files {
		rel, _ := filepath.Rel(tree, path)
		header := &zip.FileHeader{
			Name:     bundle + "/" + filepath.ToSlash(rel),
			Method:   zip.Deflate,
			Modified: stamp,
		}
		if export_store_uncompressed(rel) {
			header.Method = zip.Store
		}
		header.SetMode(0o600)
		w, err := zw.CreateHeader(header)
		if err != nil {
			zw.Close()
			return err
		}
		in, err := os.Open(path)
		if err != nil {
			zw.Close()
			return err
		}
		if _, err := io.Copy(w, in); err != nil {
			in.Close()
			zw.Close()
			return err
		}
		in.Close()
	}
	if err := zw.Close(); err != nil {
		return err
	}
	return out.Close()
}

// export_store_uncompressed reports whether a bundle-relative path is
// already-compressed media that gains nothing from deflate. DBs and
// JSON always deflate; attachment payloads under */files/ are stored.
func export_store_uncompressed(rel string) bool {
	if strings.HasSuffix(rel, ".db") || strings.HasSuffix(rel, ".json") {
		return false
	}
	return strings.Contains(filepath.ToSlash(rel), "/files/")
}

// export_source_server returns this server's public https URL for the
// manifest and the destination's re-link banner. Best-effort: primary
// configured domain, else the email-from domain, else empty.
func export_source_server() string {
	db := db_open("db/domains.db")
	if row, _ := db.row("select domain from domains order by domain limit 1"); row != nil {
		if domain := as_string(row["domain"]); domain != "" {
			return "https://" + domain
		}
	}
	if from := setting_get("email_from", ""); strings.Contains(from, "@") {
		return "https://" + from[strings.LastIndex(from, "@")+1:]
	}
	return ""
}

// as_string and as_int64 coerce a database row value (the driver yields
// int64 for integer columns, string for text) into a concrete type.
func as_string(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	}
	return ""
}

func as_int64(v any) int64 {
	switch t := v.(type) {
	case int64:
		return t
	case int:
		return int64(t)
	case float64:
		return int64(t)
	}
	return 0
}

// export_cleanup_orphans removes export staging trees and finished
// bundles older than an hour — left behind by a crash or an abandoned
// download. Piggybacks on the next export rather than running as a
// periodic per-user sweep (which multiplies badly at scale; see the
// no-scheduled-per-user-tasks rule).
func export_cleanup_orphans(uid, app string) {
	cutoff := now() - 3600
	for _, dir := range []string{
		filepath.Join(data_dir, "users", uid, "export"),
		filepath.Join(data_dir, "users", uid, app, "files", "mochi-export"),
	} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			stat, err := e.Info()
			if err != nil {
				continue
			}
			if stat.ModTime().Unix() < cutoff {
				_ = os.RemoveAll(filepath.Join(dir, e.Name()))
			}
		}
	}
}

// export_suffix is a short random tail so repeated exports within the
// same second don't collide on the bundle name.
func export_suffix() string {
	buf := make([]byte, 3)
	_, _ = rand.Read(buf)
	return hex.EncodeToString(buf)
}

// export_manifest_signed_by verifies a manifest's signature against the
// public key of the given entity id (the id is the base58 ed25519 public
// key). Used by restore.
func export_manifest_signed_by(m export_manifest, entity string) bool {
	signature := m.Signature
	if signature == "" {
		return false
	}
	m.Signature = ""
	payload, err := json.Marshal(m)
	if err != nil {
		return false
	}
	public := base58_decode(entity, "")
	if len(public) != ed25519.PublicKeySize {
		return false
	}
	return ed25519.Verify(ed25519.PublicKey(public), payload, base58_decode(signature, ""))
}

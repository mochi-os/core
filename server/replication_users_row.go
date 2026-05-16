// Mochi server: users.db row replication
// Copyright Alistair Cunningham 2026
//
// Closes audit gap #2 V2 (#78). The bulk-bootstrap snapshot copies
// users.db's auth tables (credentials, recovery, tokens, totp) at
// pair-join time, but any subsequent enrollment on one host doesn't
// reach the others. This file adds row-level emit + apply so a new
// passkey / token / TOTP setup / recovery code on host A lands on
// host B within an op round-trip.
//
// Wire format: UsersRow with both string and []byte Key/Cols maps,
// so credentials.id (BLOB PK) and credentials.public_key (BLOB) can
// travel safely. Per-table dispatch on the receiver knows which
// columns live in which map.
//
// Conflict resolution: simple last-applied-wins via INSERT OR REPLACE
// on the natural PK. Acceptable for these tables because (a) the
// PKs are externally-assigned (WebAuthn credential id, token hash,
// per-user TOTP secret) so two hosts can't generate distinct rows
// with the same PK, and (b) the user typically does enrollment on
// one host at a time.

package main

import (
	"fmt"

	cbor "github.com/fxamacker/cbor/v2"
)

// UsersRow is the wire payload for an insert / delete against a
// users.db auth table. Key and Cols carry the row's columns split
// by type; blob columns travel in KeyBytes / ColsBytes.
type UsersRow struct {
	Table     string            `cbor:"table"`
	Key       map[string]string `cbor:"key,omitempty"`
	KeyBytes  map[string][]byte `cbor:"key_bytes,omitempty"`
	Cols      map[string]string `cbor:"cols,omitempty"`
	ColsBytes map[string][]byte `cbor:"cols_bytes,omitempty"`
	Delete    bool              `cbor:"delete,omitempty"`
}

// replication_emit_users_row sends a row op for one of the users.db
// auth tables to every peer in the user's host set. user must be the
// uid of the row's owner — the op is signed by one of that user's
// entities so receivers can authenticate cross-host writes against
// the user's auth state.
func replication_emit_users_row(user string, r *UsersRow) {
	if user == "" || r.Table == "" {
		return
	}
	payload := cbor_encode(r)
	operation := "users-row.set"
	if r.Delete {
		operation = "users-row.delete"
	}
	replication_emit(user, &ReplicationOp{
		Class:     repl_class_sql,
		Scope:     repl_scope_app,
		User:      user,
		Database:  "users",
		Table:     r.Table,
		Operation: operation,
		Payload:   payload,
	})
}

// replication_users_row_apply lands a remote users-row op into the
// local users.db. Each table's apply path uses the natural PK so
// re-application is idempotent.
func replication_users_row_apply(userUID string, r *UsersRow) ApplyResult {
	if !user_exists(userUID) {
		return ApplyDeferred
	}
	udb := db_open("db/users.db")
	switch r.Table {
	case "credentials":
		return replication_users_credentials_apply(udb, userUID, r)
	case "recovery":
		return replication_users_recovery_apply(udb, userUID, r)
	case "tokens":
		return replication_users_tokens_apply(udb, userUID, r)
	case "totp":
		return replication_users_totp_apply(udb, userUID, r)
	}
	info("Replication users-row dropping: unsupported table %q", r.Table)
	return ApplyInvalid
}

func replication_users_credentials_apply(udb *DB, userUID string, r *UsersRow) ApplyResult {
	id := r.KeyBytes["id"]
	if len(id) == 0 {
		return ApplyInvalid
	}
	if r.Delete {
		udb.exec("delete from credentials where id=? and user=?", id, userUID)
		return ApplyApplied
	}
	pk := r.ColsBytes["public_key"]
	var signCount, created int64
	_, _ = fmt.Sscanf(r.Cols["sign_count"], "%d", &signCount)
	_, _ = fmt.Sscanf(r.Cols["created"], "%d", &created)
	be, bs := 0, 0
	if r.Cols["backup_eligible"] == "1" {
		be = 1
	}
	if r.Cols["backup_state"] == "1" {
		bs = 1
	}
	udb.exec(`insert or replace into credentials
		(id, user, public_key, sign_count, name, transports, backup_eligible, backup_state, created)
		values (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, userUID, pk, signCount, r.Cols["name"], r.Cols["transports"], be, bs, created)
	return ApplyApplied
}

func replication_users_recovery_apply(udb *DB, userUID string, r *UsersRow) ApplyResult {
	hash := r.Cols["hash"]
	if hash == "" {
		return ApplyInvalid
	}
	if r.Delete {
		// Two delete shapes used by the source: by (user, hash) for one
		// code, or by user only when wiping all codes ahead of a fresh
		// regenerate. r.Cols["hash"] is empty for the second shape.
		if hash == "*" {
			udb.exec("delete from recovery where user=?", userUID)
		} else {
			udb.exec("delete from recovery where user=? and hash=?", userUID, hash)
		}
		return ApplyApplied
	}
	if exists, _ := udb.exists("select 1 from recovery where user=? and hash=?", userUID, hash); exists {
		return ApplyApplied
	}
	var created int64
	_, _ = fmt.Sscanf(r.Cols["created"], "%d", &created)
	udb.exec("insert into recovery (user, hash, created) values (?, ?, ?)", userUID, hash, created)
	return ApplyApplied
}

func replication_users_tokens_apply(udb *DB, userUID string, r *UsersRow) ApplyResult {
	hash := r.Cols["hash"]
	if hash == "" {
		return ApplyInvalid
	}
	if r.Delete {
		udb.exec("delete from tokens where hash=?", hash)
		return ApplyApplied
	}
	var created, expires int64
	_, _ = fmt.Sscanf(r.Cols["created"], "%d", &created)
	_, _ = fmt.Sscanf(r.Cols["expires"], "%d", &expires)
	udb.exec(`insert or replace into tokens
		(hash, user, app, name, scopes, created, expires)
		values (?, ?, ?, ?, ?, ?, ?)`,
		hash, userUID, r.Cols["app"], r.Cols["name"], r.Cols["scopes"], created, expires)
	return ApplyApplied
}

func replication_users_totp_apply(udb *DB, userUID string, r *UsersRow) ApplyResult {
	if r.Delete {
		udb.exec("delete from totp where user=?", userUID)
		return ApplyApplied
	}
	secret := r.Cols["secret"]
	if secret == "" {
		return ApplyInvalid
	}
	verified := 0
	if r.Cols["verified"] == "1" {
		verified = 1
	}
	var created int64
	_, _ = fmt.Sscanf(r.Cols["created"], "%d", &created)
	udb.exec(`insert or replace into totp (user, secret, verified, created) values (?, ?, ?, ?)`,
		userUID, secret, verified, created)
	return ApplyApplied
}

// users_row_decode_and_apply is the entry point called from
// replication_apply_op after dispatch detects a users-row op. Decodes
// the payload and runs the per-table apply.
func users_row_decode_and_apply(payload []byte, user string) ApplyResult {
	var r UsersRow
	if err := cbor.Unmarshal(payload, &r); err != nil {
		info("Replication users-row decode failed: %v", err)
		return ApplyInvalid
	}
	return replication_users_row_apply(user, &r)
}

// boolint01 returns "1" for true and "0" for false. Used at emit
// sites to fit a bool through UsersRow.Cols (map[string]string).
func boolint01(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

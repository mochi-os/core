// Mochi server: sessions.db row replication
// Copyright Alistair Cunningham 2026
//
// Closes audit gap #3 for the auth-flow tables that have cross-host
// stable PKs and matter for correctness when a user moves between
// hosts mid-flow:
//
//   - partial: incomplete multi-step auth state (after first factor,
//     before MFA). PK is a random 32-char string — globally unique.
//   - codes:   email/SMS login challenge codes. PK is (code, username).
//
// Explicitly NOT replicated (cosmetic or technically awkward):
//
//   - verifications: PK is the local users.db.oauth integer id, which
//     is per-host. Cross-host emit would need to translate via (user,
//     provider, subject) — possible but adds joining. Cost of loss is
//     one extra OAuth re-verify after a host migration; acceptable.
//   - ceremonies: WebAuthn ceremonies are always continuation of the
//     same browser session, so sticky-session routing keeps them on
//     one host. Replication adds no value.
//   - accesses / logins: per-token / per-user last-used timestamps,
//     cosmetic only.
//   - passkeys: per-host sign_count is intentionally local-only
//     (replay protection counter; cross-host divergence is harmless).

package main

import (
	"fmt"

	cbor "github.com/fxamacker/cbor/v2"
)

// SessionsRow is the wire payload for an insert / delete against one
// of the replicated sessions.db tables. Same shape as UsersRow; the
// receiver dispatches per-table by Table name.
type SessionsRow struct {
	Table  string            `cbor:"table"`
	Key    map[string]string `cbor:"key,omitempty"`
	Cols   map[string]string `cbor:"cols,omitempty"`
	Delete bool              `cbor:"delete,omitempty"`
}

// replication_emit_sessions_row sends a sessions-row op to every peer
// in the user's host set. user must be the uid of the row's owner.
// For codes (where the username — not uid — is the relevant identity
// before the user has actually logged in), look up the uid first;
// pass empty user to no-op the emit if the user isn't local.
func replication_emit_sessions_row(user string, r *SessionsRow) {
	if user == "" || r.Table == "" {
		return
	}
	payload := cbor_encode(r)
	kind := "sessions-row.set"
	if r.Delete {
		kind = "sessions-row.delete"
	}
	replication_emit(user, &ReplicationOp{
		Scope:    repl_scope_app,
		User:     user,
		Database: "sessions",
		Table:    r.Table,
		Kind:     kind,
		Payload:  payload,
	})
}

func replication_sessions_row_apply(userUID string, r *SessionsRow) ApplyResult {
	if !user_exists(userUID) {
		return ApplyDeferred
	}
	sdb := db_open("db/sessions.db")
	switch r.Table {
	case "partial":
		return replication_sessions_partial_apply(sdb, r)
	case "codes":
		return replication_sessions_codes_apply(sdb, r)
	}
	info("Replication sessions-row dropping: unsupported table %q", r.Table)
	return ApplyInvalid
}

func replication_sessions_partial_apply(sdb *DB, r *SessionsRow) ApplyResult {
	id := r.Key["id"]
	if id == "" {
		return ApplyInvalid
	}
	if r.Delete {
		sdb.exec("delete from partial where id=?", id)
		return ApplyApplied
	}
	var expires int64
	_, _ = fmt.Sscanf(r.Cols["expires"], "%d", &expires)
	sdb.exec(`insert or replace into partial (id, user, completed, remaining, expires) values (?, ?, ?, ?, ?)`,
		id, r.Cols["user"], r.Cols["completed"], r.Cols["remaining"], expires)
	return ApplyApplied
}

func replication_sessions_codes_apply(sdb *DB, r *SessionsRow) ApplyResult {
	code := r.Key["code"]
	username := r.Key["username"]
	if code == "" || username == "" {
		return ApplyInvalid
	}
	if r.Delete {
		sdb.exec("delete from codes where code=? and username=?", code, username)
		return ApplyApplied
	}
	var expires int64
	_, _ = fmt.Sscanf(r.Cols["expires"], "%d", &expires)
	sdb.exec("replace into codes (code, username, expires) values (?, ?, ?)",
		code, username, expires)
	return ApplyApplied
}

func sessions_row_decode_and_apply(payload []byte, user string) ApplyResult {
	var r SessionsRow
	if err := cbor.Unmarshal(payload, &r); err != nil {
		info("Replication sessions-row decode failed: %v", err)
		return ApplyInvalid
	}
	return replication_sessions_row_apply(user, &r)
}

// partial_create wraps the insert into sessions.db.partial + the
// cross-host emit. There are five call sites with the same shape;
// wrapping keeps the emit consistent and the insert SQL in one place.
func partial_create(sdb *DB, partialID, userUID, completed, remaining string, expires int64) {
	sdb.exec("insert into partial (id, user, completed, remaining, expires) values (?, ?, ?, ?, ?)",
		partialID, userUID, completed, remaining, expires)
	replication_emit_sessions_row(userUID, &SessionsRow{
		Table: "partial",
		Key:   map[string]string{"id": partialID},
		Cols: map[string]string{
			"user":      userUID,
			"completed": completed,
			"remaining": remaining,
			"expires":   fmt.Sprintf("%d", expires),
		},
	})
}

// partial_delete wraps the delete + cross-host emit. userUID is needed
// only for the emit's signer; the delete itself is by id only (the
// random 32-char id is globally unique).
func partial_delete(sdb *DB, partialID, userUID string) {
	sdb.exec("delete from partial where id=?", partialID)
	if userUID != "" {
		replication_emit_sessions_row(userUID, &SessionsRow{
			Table:  "partial",
			Key:    map[string]string{"id": partialID},
			Delete: true,
		})
	}
}

// Mochi server: the passkey signature counter is read, not just stored.
// go-webauthn sets Authenticator.CloneWarning during ValidateLogin but never
// fails the ceremony - responding to it is the relying party's job.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"strings"
	"testing"

	"github.com/go-webauthn/webauthn/webauthn"
)

// passkey_clone_env creates a users.db with one credential at a known counter
// and returns the user it belongs to, plus the cleanup.
func passkey_clone_env(t *testing.T, stored uint32) *User {
	t.Helper()
	create_web_test_env(t)

	users := db_open("db/users.db")
	users.exec("create table if not exists credentials (id blob primary key, user text not null, public_key blob not null, sign_count integer not null default 0, name text not null default '', transports text not null default '', backup_eligible integer not null default 0, backup_state integer not null default 0, created integer not null)")
	users.exec("insert into credentials (id, user, public_key, sign_count, created) values (?, ?, ?, ?, ?)",
		[]byte("credential-one"), "u1", []byte("key"), stored, now())

	sessions := db_open("db/sessions.db")
	sessions.exec("create table if not exists passkeys (credential blob primary key, user text not null, last integer not null default 0)")

	return &User{UID: "u1", Username: "someone@example.com"}
}

// passkey_stored_count reads the counter back.
func passkey_stored_count(t *testing.T) int64 {
	t.Helper()
	row, err := db_open("db/users.db").row("select sign_count from credentials where id=?", []byte("credential-one"))
	if err != nil || row == nil {
		t.Fatalf("reading the stored counter back: %v", err)
	}
	count, _ := row["sign_count"].(int64)
	return count
}

// passkey_assertion builds the credential ValidateLogin would hand back.
func passkey_assertion(count uint32, cloned bool) *webauthn.Credential {
	return &webauthn.Credential{
		ID: []byte("credential-one"),
		Authenticator: webauthn.Authenticator{
			SignCount:    count,
			CloneWarning: cloned,
		},
	}
}

// TestCloneWarningIsReported is the defect: the flag was computed and dropped.
func TestCloneWarningIsReported(t *testing.T) {
	user := passkey_clone_env(t, 40)
	captured := capture_log(t)

	passkey_credential_finalize(user, passkey_assertion(40, true), "198.51.100.7")

	out := captured.String()
	if !strings.Contains(out, "may be cloned") {
		t.Errorf("a clone warning produced %q; the signal go-webauthn computed is discarded, so the sign_count column asks nothing", out)
	}
	if !strings.Contains(out, user.Username) {
		t.Errorf("the clone report %q does not name the account it concerns", out)
	}
}

// TestCloneWarningDoesNotLowerTheStoredCounter: the counter must never move
// backwards. UpdateCounter already leaves it alone on a clone warning, so this
// pins the invariant rather than the library's current behaviour.
func TestCloneWarningDoesNotLowerTheStoredCounter(t *testing.T) {
	user := passkey_clone_env(t, 40)
	capture_log(t)

	passkey_credential_finalize(user, passkey_assertion(11, true), "198.51.100.7")

	if got := passkey_stored_count(t); got != 40 {
		t.Errorf("the stored counter is %d after a clone-warned assertion carrying %d; it was 40 and must not go backwards", got, 11)
	}
}

// TestNormalAssertionAdvancesTheCounter: the replay-prevention state still
// tracks a healthy authenticator, so the check above cannot be satisfied by
// freezing the column.
func TestNormalAssertionAdvancesTheCounter(t *testing.T) {
	user := passkey_clone_env(t, 40)
	capture_log(t)

	passkey_credential_finalize(user, passkey_assertion(41, false), "198.51.100.7")

	if got := passkey_stored_count(t); got != 41 {
		t.Errorf("the stored counter is %d after a normal assertion at 41; the column no longer tracks the authenticator", got)
	}
}

// TestNormalAssertionIsSilent: every ordinary login must not report a clone.
// Without this the reporting could be unconditional and the tests above would
// still pass, while the operator got a warning on every sign-in.
func TestNormalAssertionIsSilent(t *testing.T) {
	user := passkey_clone_env(t, 40)
	captured := capture_log(t)

	passkey_credential_finalize(user, passkey_assertion(41, false), "198.51.100.7")

	if strings.Contains(captured.String(), "may be cloned") {
		t.Errorf("an ordinary assertion reported a clone: %q", captured.String())
	}
}

// TestLastUsedIsRecordedEvenWhenCloneWarned: the assertion was cryptographically
// valid and the login proceeds, so the cosmetic last-used must still update.
// Putting the counter write behind the flag must not take this with it.
func TestLastUsedIsRecordedEvenWhenCloneWarned(t *testing.T) {
	user := passkey_clone_env(t, 40)
	capture_log(t)

	passkey_credential_finalize(user, passkey_assertion(40, true), "198.51.100.7")

	if lasts := passkey_lasts(user.UID); lasts["credential-one"] == 0 {
		t.Error("a clone-warned assertion recorded no last-used, so the passkey list will show the credential as never used")
	}
}

// TestCloneWarningReachesTheAuditTrail: audit_session_anomaly writes to a
// syslog handle that is nil in a test binary, so only the source shows the call
// happens - hence the source scan rather than a log assertion.
func TestCloneWarningReachesTheAuditTrail(t *testing.T) {
	body := function_body(t, "passkeys.go", "func passkey_credential_finalize(")

	clone := strings.Index(body, "CloneWarning")
	if clone < 0 {
		t.Fatal("passkey_credential_finalize no longer looks at CloneWarning at all")
	}
	if !strings.Contains(body[clone:], "audit_session_anomaly(") {
		t.Error("a clone warning is logged but never audited; the mochi log rotates and is not the auth trail an incident is reconstructed from")
	}
	if !strings.Contains(body, "passkey_clone_anomaly") {
		t.Error("the audit line does not carry the shared reason constant, so the trail cannot be searched for a fixed token")
	}
	if passkey_clone_anomaly != "passkey_clone_warning" {
		t.Errorf("the audit reason is %q; changing it breaks whatever an operator greps their auth log for", passkey_clone_anomaly)
	}
}

// TestBothAssertionPathsFinalize: login and step-up re-auth both validate an
// assertion, so a check that lives in only one of them is half a check.
func TestBothAssertionPathsFinalize(t *testing.T) {
	for _, name := range []string{
		"func web_passkey_login_finish(",
		"func api_user_passkey_verify_finish(",
	} {
		body := function_body(t, "passkeys.go", name)
		if !strings.Contains(body, "passkey_credential_finalize(") {
			t.Errorf("%s validates an assertion without routing through passkey_credential_finalize, so a cloned authenticator goes unnoticed on that path", name)
		}
	}
}

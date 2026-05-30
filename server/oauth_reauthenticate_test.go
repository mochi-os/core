package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

// TestOauthReauthenticate covers the popup OAuth step-up gate: an OAuth identity
// already linked to the user mints a single-use proof keyed by the caller's
// challenge; an unlinked provider account mints nothing. It also exercises the
// retrieval contract that mochi.user.oauth.verify.finish enforces (user-scoped,
// single-use).
func TestOauthReauthenticate(t *testing.T) {
	cleanup := create_test_users_db(t)
	defer cleanup()

	users := db_open("db/users.db")
	users.exec("create table oauth (id integer primary key, user text not null, provider text not null, subject text not null, email text not null default '', verified integer not null default 0, name text not null default '', created integer not null, unique(provider, subject))")
	sessions := db_open("db/sessions.db")
	sessions.exec("create table ceremonies (id text primary key, type text not null, user text not null default '', challenge blob not null, data text not null default '', expires integer not null)")
	sessions.exec("create table reauthentication (id text primary key, user text not null, methods text not null default '', expires integer not null)")
	sessions.exec("create table verifications (oauth integer not null, user text not null, last integer not null, primary key (oauth, user))")

	users.exec("insert into users (uid, username) values ('u-x', 'x@example.com')")
	users.exec("insert into oauth (user, provider, subject, created) values ('u-x', 'google', 'sub-123', 1)")
	user := &User{UID: "u-x", Username: "x@example.com", Methods: "email"}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	challenge := func(verifier string) string {
		h := sha256.Sum256([]byte(verifier))
		return base64.RawURLEncoding.EncodeToString(h[:])
	}

	// Linked identity -> a single-use proof is stored, scoped to the user.
	v1 := random_alphanumeric(64)
	oauth_reauthenticate(c, "google", &oauth_profile{Subject: "sub-123", Email: "x@example.com", Verified: true}, user, challenge(v1))
	row, _ := sessions.row("select data, user from ceremonies where id=? and type='reauthentication_oauth' and expires>?", challenge(v1), now())
	if row == nil {
		t.Fatal("linked identity stored no proof")
	}
	if u, _ := row["user"].(string); u != "u-x" {
		t.Errorf("proof user = %q, want u-x", u)
	}
	var res map[string]any
	json.Unmarshal([]byte(row["data"].(string)), &res)
	if tok, _ := res["token"].(string); tok == "" {
		t.Errorf("email-only user: expected a token, got %v", res)
	}

	// Unlinked provider account -> nothing minted (the stolen-session defence).
	v2 := random_alphanumeric(64)
	oauth_reauthenticate(c, "google", &oauth_profile{Subject: "attacker-sub", Email: "evil@example.com", Verified: true}, user, challenge(v2))
	if r, _ := sessions.row("select 1 from ceremonies where id=? and type='reauthentication_oauth'", challenge(v2)); r != nil {
		t.Error("unlinked provider account minted a proof")
	}

	// Retrieval contract (mirrors mochi.user.oauth.verify.finish): user-scoped,
	// single-use. The wrong user cannot read u-x's proof; the right user reads
	// it exactly once.
	get := func(uid, verifier string) bool {
		id := challenge(verifier)
		r, _ := sessions.row("select data from ceremonies where id=? and type='reauthentication_oauth' and user=? and expires>?", id, uid, now())
		if r == nil {
			return false
		}
		sessions.exec("delete from ceremonies where id=?", id)
		return true
	}
	if get("u-other", v1) {
		t.Error("proof readable by the wrong user")
	}
	if !get("u-x", v1) {
		t.Error("owner could not read the proof")
	}
	if get("u-x", v1) {
		t.Error("proof reusable after retrieval")
	}
}

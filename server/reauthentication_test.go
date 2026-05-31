package main

import "testing"

// TestReauthentication covers the step-up proof engine: the required-factor
// policy (oauth re-verifies as oauth, recovery excluded, email default),
// single- and multi-factor accrual, and that a proof is single-use,
// user-scoped, expiry-checked, and rejected while incomplete.
func TestReauthentication(t *testing.T) {
	cleanup := create_test_sessions_db(t)
	defer cleanup()

	db := db_open("db/sessions.db")

	// Required-factor policy. Nothing configured -> nothing specifically
	// required (any one usable factor satisfies the step-up).
	if got := reauthentication_required(&User{Methods: ""}); len(got) != 0 {
		t.Errorf("no methods required = %v, want empty", got)
	}
	if got := reauthentication_required(&User{Methods: "oauth"}); len(got) != 1 || got[0] != "oauth" {
		t.Errorf("oauth required = %v, want [oauth]", got)
	}
	if got := reauthentication_required(&User{Methods: "email,totp,recovery"}); len(got) != 2 || got[0] != "email" || got[1] != "totp" {
		t.Errorf("required = %v, want [email totp] (recovery excluded)", got)
	}

	// Single-factor (email-only): one verify yields a usable, single-use token.
	alice := &User{UID: "u-alice", Username: "alice@example.com", Methods: "email"}
	token, remaining := reauthentication_advance(alice, "email")
	if token == "" || len(remaining) != 0 {
		t.Fatalf("email-only advance = (%q, %v), want a token and no remaining", token, remaining)
	}
	if !reauthentication_consume(alice, token) {
		t.Error("valid proof rejected")
	}
	if reauthentication_consume(alice, token) {
		t.Error("proof reusable after consume")
	}

	// Multi-factor (email,totp): no token until both factors clear.
	bob := &User{UID: "u-bob", Username: "bob@example.com", Methods: "email,totp"}
	if tok, rem := reauthentication_advance(bob, "email"); tok != "" || len(rem) != 1 || rem[0] != "totp" {
		t.Fatalf("bob email advance = (%q, %v), want no token and remaining [totp]", tok, rem)
	}
	tok, rem := reauthentication_advance(bob, "totp")
	if tok == "" || len(rem) != 0 {
		t.Fatalf("bob totp advance = (%q, %v), want a token", tok, rem)
	}
	if !reauthentication_consume(bob, tok) {
		t.Error("bob's completed proof rejected")
	}

	// Decoupling guarantee: OAuth re-verifies as its own oauth factor and never
	// satisfies a required email factor, so an email-required user who only
	// clears oauth still owes email (an OAuth sign-in can't substitute for inbox
	// control).
	frank := &User{UID: "u-frank", Username: "frank@example.com", Methods: "email"}
	if tok, rem := reauthentication_advance(frank, "oauth"); tok != "" || len(rem) != 1 || rem[0] != "email" {
		t.Fatalf("frank oauth advance = (%q, %v), want no token and remaining [email]", tok, rem)
	}

	// An incomplete proof is not consumable even if its id is known.
	dave := &User{UID: "u-dave", Methods: "email,totp"}
	reauthentication_advance(dave, "email")
	var row Reauthentication
	if db.scan(&row, "select id, user, methods, expires from reauthentication where user=?", dave.UID) {
		if reauthentication_consume(dave, row.Id) {
			t.Error("incomplete proof consumed")
		}
	} else {
		t.Error("dave's accrual row not found")
	}

	// Rejections: nil user, empty, unknown, expired.
	if reauthentication_consume(nil, "x") {
		t.Error("nil user accepted")
	}
	if reauthentication_consume(alice, "") {
		t.Error("empty token accepted")
	}
	if reauthentication_consume(alice, "nope") {
		t.Error("unknown token accepted")
	}
	db.exec("insert into reauthentication (id, user, methods, expires) values ('stale', 'u-alice', 'email', ?)", now()-1)
	if reauthentication_consume(alice, "stale") {
		t.Error("expired token accepted")
	}

	// User-scoped: one user's token can't be consumed by another.
	carol := &User{UID: "u-carol", Username: "carol@example.com", Methods: "email"}
	ctok, _ := reauthentication_advance(carol, "email")
	if reauthentication_consume(alice, ctok) {
		t.Error("another user's token accepted")
	}
	if !reauthentication_consume(carol, ctok) {
		t.Error("carol's own token rejected after the cross-user attempt")
	}
}

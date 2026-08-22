// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"errors"
	"fmt"
	"testing"
)

// reset_verification_limiters clears both buckets so a test starts from a
// known budget whatever ran before it.
func reset_verification_limiters() {
	rate_limit_verification.entries = map[string]*rate_limit_entry{}
	rate_limit_verification_sender.entries = map[string]*rate_limit_entry{}
}

// TestEmailAddressCanonicalises is the fix for the limiter-key bypass: any
// budget keyed on an address must key on the mailbox, not on the spelling.
// mail.ParseAddress accepts all of these forms, so keying on the raw request
// string gave one mailbox as many budgets as an attacker cared to invent.
func TestEmailAddressCanonicalises(t *testing.T) {
	for _, spelling := range []string{
		"a@b.com",
		" a@b.com ",
		"A@B.com",
		"Alice <a@b.com>",
		"<a@b.com>",
		"\"Alice Smith\" <A@b.COM>",
	} {
		if got := email_address(spelling); got != "a@b.com" {
			t.Errorf("email_address(%q) = %q, want a@b.com", spelling, got)
		}
	}

	for _, bad := range []string{"", "not an address", "@b.com", "a@"} {
		if got := email_address(bad); got != "" {
			t.Errorf("email_address(%q) = %q, want empty", bad, got)
		}
	}
}

// TestVerificationLimitPerRecipient: the budget follows the mailbox, so
// rewriting the address does not buy a fresh one.
func TestVerificationLimitPerRecipient(t *testing.T) {
	reset_verification_limiters()

	// Distinct senders, so only the recipient bucket is under test.
	spellings := []string{"v@example.com", " v@example.com ", "V@Example.com",
		"Victim <v@example.com>", "\"V\" <V@EXAMPLE.COM>"}
	for i, spelling := range spellings {
		if err := account_verification_allowed(spelling, fmt.Sprintf("sender-%d", i)); err != nil {
			t.Fatalf("send %d (%q) refused inside the budget: %v", i+1, spelling, err)
		}
	}

	// The sixth spelling of the same mailbox exhausts it.
	err := account_verification_allowed("v@EXAMPLE.com", "sender-last")
	if err == nil {
		t.Fatal("a sixth send to the same mailbox was allowed; the budget is per spelling, not per recipient")
	}
	var limit *RateLimitError
	if !errors.As(err, &limit) {
		t.Errorf("error = %v (%T), want a *RateLimitError so the HTTP layer answers 429", err, err)
	}

	// A different mailbox has its own budget.
	if err := account_verification_allowed("other@example.com", "sender-other"); err != nil {
		t.Errorf("a different recipient was refused: %v", err)
	}
}

// TestVerificationLimitPerSender bounds a spray across many addresses, which
// the recipient bucket never sees because each victim receives only one.
func TestVerificationLimitPerSender(t *testing.T) {
	reset_verification_limiters()

	for i := 0; i < rate_limit_verification_sender.limit; i++ {
		to := fmt.Sprintf("victim-%d@example.com", i)
		if err := account_verification_allowed(to, "u-spammer"); err != nil {
			t.Fatalf("send %d to a fresh address refused inside the sender budget: %v", i+1, err)
		}
	}

	err := account_verification_allowed("victim-final@example.com", "u-spammer")
	if err == nil {
		t.Fatal("a spray across distinct addresses was unbounded; the recipient bucket cannot see it")
	}
	var limit *RateLimitError
	if !errors.As(err, &limit) {
		t.Errorf("error = %v (%T), want a *RateLimitError", err, err)
	}

	// Another sender is unaffected.
	if err := account_verification_allowed("victim-final@example.com", "u-innocent"); err != nil {
		t.Errorf("a second sender was refused by the first one's budget: %v", err)
	}
}

// TestVerificationBudgetSeparateFromLoginCode is the decision core 266798d0
// made for step-up, applied here: a shared bucket would let verification
// traffic aimed at an address exhaust the budget that address needs to receive
// a login code, locking the owner out with nothing but their email address.
func TestVerificationBudgetSeparateFromLoginCode(t *testing.T) {
	reset_verification_limiters()
	rate_limit_code.entries = map[string]*rate_limit_entry{}

	address := "target@example.com"
	for i := 0; i < rate_limit_verification.limit; i++ {
		if err := account_verification_allowed(address, fmt.Sprintf("s-%d", i)); err != nil {
			t.Fatalf("exhausting the verification budget failed early at %d: %v", i+1, err)
		}
	}
	if err := account_verification_allowed(address, "s-last"); err == nil {
		t.Fatal("the verification budget did not exhaust")
	}

	// The login-code budget for the same address is untouched.
	if !rate_limit_code.allow(address) {
		t.Error("exhausting verification locked the address out of login codes; the buckets are shared")
	}
}

// TestCodeSendKeysOnTheCanonicalAddress covers the wiring rather than the
// helper: code_send is where the login-code budget is charged, and it charged
// on what the caller typed.
func TestCodeSendKeysOnTheCanonicalAddress(t *testing.T) {
	create_test_users_db(t)
	db_open("db/sessions.db").exec("create table codes (code text primary key, username text not null, expires integer not null)")
	db_open("db/settings.db").exec("create table settings (name text primary key, value text not null default '')")
	db_open("db/users.db").exec("insert into users (uid, username, methods) values ('u-code', 'code@example.com', 'email')")

	canonical := "code@example.com"
	rate_limit_code.reset(canonical)

	// Three spellings of one mailbox. Before the fix each opened its own
	// budget; now they share the canonical key.
	for _, spelling := range []string{"code@example.com", "CODE@Example.com", " code@example.com "} {
		code_send(spelling, nil)
	}

	entry := rate_limit_code.entries[canonical]
	if entry == nil {
		t.Fatal("no budget was charged against the canonical address")
	}
	if entry.count != 3 {
		t.Errorf("canonical key charged %d times, want 3 - the spellings are opening separate budgets", entry.count)
	}
}

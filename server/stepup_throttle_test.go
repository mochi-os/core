// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Tests for the per-account throttle on step-up re-authentication. Six-digit
// codes are guessable and a per-IP limiter is defeated by rotating addresses,
// so the throttle keys on the account - in its own bucket, so a step-up
// attacker cannot exhaust the legitimate user's login budget.

package main

import (
	"testing"

	sl "go.starlark.net/starlark"
)

func stepup_gate_reset(t *testing.T) {
	t.Helper()
	// The spacing is real time — the gate sleeps it out — so these run at the
	// floor. The tunables are vars for exactly this.
	floor, wait := account_gate_floor, account_wait_maximum
	account_gate_floor, account_wait_maximum = 0, 0
	t.Cleanup(func() { account_gate_floor, account_wait_maximum = floor, wait })

	account_stepup.lock.Lock()
	account_stepup.entries = make(map[string]*account_gate_entry)
	account_stepup.lock.Unlock()
}

// TestStepupGateWidensAfterWrongGuesses is the property that makes the gate a
// guessing gate rather than a rate limiter: a wrong answer costs the next
// attempt more. Free attempts first, then spacing, then refusal.
func TestStepupGateWidensAfterWrongGuesses(t *testing.T) {
	stepup_gate_reset(t)

	// Burn the free attempts, reporting each as a failure.
	for i := 0; i < account_gate_free; i++ {
		if !stepup_gate_reserve("u-guess") {
			t.Fatalf("attempt %d was refused while free attempts remained", i)
		}
		stepup_gate_done("u-guess", false)
	}

	account_stepup.lock.Lock()
	failures := account_stepup.entries["u-guess"].failures
	account_stepup.lock.Unlock()
	if failures != account_gate_free {
		t.Errorf("the gate recorded %d failures after %d wrong guesses; a guess that is not reported does not widen the spacing", failures, account_gate_free)
	}
}

// TestStepupGateClearsOnSuccess: a correct credential must not leave the user
// throttled, or one mistyped code would tax every later legitimate step-up.
func TestStepupGateClearsOnSuccess(t *testing.T) {
	stepup_gate_reset(t)

	stepup_gate_reserve("u-clear")
	stepup_gate_done("u-clear", false)
	stepup_gate_reserve("u-clear")
	stepup_gate_done("u-clear", true)

	account_stepup.lock.Lock()
	_, still_tracked := account_stepup.entries["u-clear"]
	account_stepup.lock.Unlock()
	if still_tracked {
		t.Error("a successful step-up left the account's penalty in place")
	}
}

// TestStepupGateIsSeparateFromLogin is the decision this change turned on.
// Sharing one bucket would let a step-up attacker lock the legitimate user out
// of signing in, using only their address.
func TestStepupGateIsSeparateFromLogin(t *testing.T) {
	stepup_gate_reset(t)

	account_login.lock.Lock()
	delete(account_login.entries, "u-separate")
	account_login.lock.Unlock()

	for i := 0; i < account_gate_free+2; i++ {
		stepup_gate_reserve("u-separate")
		stepup_gate_done("u-separate", false)
	}

	account_login.lock.Lock()
	_, login_touched := account_login.entries["u-separate"]
	account_login.lock.Unlock()
	if login_touched {
		t.Error("step-up guessing consumed the account's login budget; an attacker could lock the user out of signing in with only their address")
	}
}

// TestStepupGateIsPerAccount: the whole point of keying on the account rather
// than the address. One account's penalty must not slow another's.
func TestStepupGateIsPerAccount(t *testing.T) {
	stepup_gate_reset(t)

	for i := 0; i < account_gate_free+2; i++ {
		stepup_gate_reserve("u-noisy")
		stepup_gate_done("u-noisy", false)
	}

	account_stepup.lock.Lock()
	_, other := account_stepup.entries["u-quiet"]
	account_stepup.lock.Unlock()
	if other {
		t.Error("one account's guessing created state for another")
	}
}

// TestStepupGateRefusesAnEmptyAccount: no uid means nothing to key on, so
// there is no gate — refuse rather than let the attempt through ungated.
func TestStepupGateRefusesAnEmptyAccount(t *testing.T) {
	if stepup_gate_reserve("") {
		t.Error("an empty uid was admitted; that attempt would be entirely ungated")
	}
}

// TestTotpVerifyReachesTheGate is the wiring, which is where these defects
// actually live: every test above passes with the gate present and unused.
// A wrong code through the real builtin must leave a penalty on the account.
func TestTotpVerifyReachesTheGate(t *testing.T) {
	setup_replication_test(t)
	setup_users_test_schema()
	stepup_gate_reset(t)
	thread, _ := totp_user(t)

	verify := sl.NewBuiltin("mochi.user.totp.verify", api_user_totp_verify)
	if _, err := api_user_totp_verify(thread, verify, sl.Tuple{sl.String("000000")}, nil); err != nil {
		t.Fatalf("verify: %v", err)
	}

	account_stepup.lock.Lock()
	entry := account_stepup.entries["u-totp"]
	account_stepup.lock.Unlock()
	if entry == nil {
		t.Fatal("a wrong step-up code left no trace on the gate; api_user_totp_verify is not calling it")
	}
	if entry.failures != 1 {
		t.Errorf("the gate recorded %d failures for one wrong code; a guess that is not settled never widens the spacing", entry.failures)
	}
}

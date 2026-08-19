// Mochi server: the recovery-login dummy comparison has to cost what a real one costs.
//
// The unknown-user branch compared against a hand-written 57-byte placeholder.
// bcrypt's minimum is 59 and a real hash is 60, so CompareHashAndPassword
// returned ErrHashTooShort on a length check before reaching the KDF - 12ns,
// against 407ms for a user holding ten codes. A single request distinguished a
// registered address from an unregistered one, and the comment above the line
// asserted the opposite.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"
)

// TestRecoveryDummyIsAUsableHash is the defect. Everything else here follows
// from the comparison actually running.
func TestRecoveryDummyIsAUsableHash(t *testing.T) {
	dummy := recovery_dummy()

	if len(dummy) < 59 {
		t.Fatalf("the dummy hash is %d bytes; bcrypt refuses anything under 59 before it starts work, so the comparison costs nothing", len(dummy))
	}
	err := bcrypt.CompareHashAndPassword(dummy, []byte("wrong-code"))
	if err == bcrypt.ErrHashTooShort {
		t.Fatal("bcrypt rejects the dummy as too short, so the unknown-user path does no key derivation at all")
	}
	if err == nil {
		t.Error("the dummy hash matches an arbitrary code, which would make every unknown user a successful login")
	}

	// Same cost as the hashes it stands in for, or it does a different amount
	// of work than the path it is imitating.
	cost, err := bcrypt.Cost(dummy)
	if err != nil {
		t.Fatalf("the dummy is not a bcrypt hash: %v", err)
	}
	if cost != bcrypt.DefaultCost {
		t.Errorf("the dummy costs %d, the stored recovery hashes cost %d", cost, bcrypt.DefaultCost)
	}
}

// TestRecoveryDummyCostsRealTime. The point is the clock, so measure it: a
// comparison that returns in nanoseconds has not derived a key.
func TestRecoveryDummyCostsRealTime(t *testing.T) {
	if testing.Short() {
		t.Skip("bcrypt timing measurement")
	}
	dummy := recovery_dummy()

	start := time.Now()
	bcrypt.CompareHashAndPassword(dummy, []byte("wrong-code"))
	elapsed := time.Since(start)

	// A real bcrypt round at cost 10 is tens of milliseconds on any machine
	// this runs on. A millisecond floor is far below that and far above the
	// microseconds a rejected hash takes, so it separates the two without
	// depending on how fast the host is.
	if elapsed < time.Millisecond {
		t.Errorf("a comparison against the dummy took %v; that is a rejection, not a key derivation", elapsed)
	}
}

// TestRecoveryDummyIsStable. Generated per call, the unknown-user path would
// cost a GenerateFromPassword on top - slower than any real user, which is the
// same oracle inverted.
func TestRecoveryDummyIsStable(t *testing.T) {
	first := recovery_dummy()
	second := recovery_dummy()
	if string(first) != string(second) {
		t.Error("recovery_dummy returns a fresh hash each call; generating one costs a KDF round of its own, making the unknown-user path slower than any known one")
	}

	start := time.Now()
	for i := 0; i < 3; i++ {
		recovery_dummy()
	}
	if elapsed := time.Since(start); elapsed > 10*time.Millisecond {
		t.Errorf("three calls to recovery_dummy took %v; it is being regenerated rather than reused", elapsed)
	}
}

// TestRecoveryUnknownUserMatchesTheWorstKnownCase. One comparison against ten
// still leaves a tenfold signal, which is all a timing attack needs.
func TestRecoveryUnknownUserMatchesTheWorstKnownCase(t *testing.T) {
	body, err := os.ReadFile("authentication.go")
	if err != nil {
		t.Fatalf("reading authentication.go: %v", err)
	}
	source := string(body)

	// The handler by name. Searching for the audit reason instead found the
	// first of three "user_not_found" sites, in a different handler with no
	// preceding row check - which is how this test first panicked rather than
	// reported.
	at := strings.Index(source, "func web_recovery_login(")
	if at < 0 {
		t.Fatal("web_recovery_login not found; this test is looking in the wrong place")
	}
	region := source[at:]
	if end := strings.Index(region, "\nfunc "); end > 0 {
		region = region[:end]
	}

	if strings.Contains(region, `"$2a$10$xxx`) {
		t.Error("the unknown-user branch still compares against the placeholder string")
	}
	if !strings.Contains(region, "recovery_dummy()") {
		t.Error("the unknown-user branch does not use the real dummy hash")
	}
	if !strings.Contains(region, "i < recovery_code_count") {
		t.Error("the unknown-user branch compares once; a user holding ten codes spends ten comparisons, so one leaves a tenfold difference")
	}
}

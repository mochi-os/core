// Mochi server: the AI call's account resolution and its input bounds.
//
// mochi.ai.prompt is the one Starlark builtin that spends money. Core bounds
// what a caller can put into a call, never how many calls an account makes:
// a spending cap belongs at the provider, where the account owner sets it.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"strings"
	"testing"

	sl "go.starlark.net/starlark"
)

// ai_test_user creates a user with an accounts table and returns its database.
func ai_test_user(t *testing.T, id string) (*User, *DB) {
	t.Helper()
	user := create_permission_test_user(t, id)
	database := db_user(user, "user")
	database.exec("create table if not exists accounts (id text not null primary key, type text not null, " +
		"label text not null default '', identifier text not null default '', data text not null default '', " +
		"created integer not null, verified integer not null default 0, enabled integer not null default 1, " +
		"\"default\" text not null default '', last_delivered integer not null default 0, device text not null default '')")
	return user, database
}

// ai_test_account inserts an account. `designated` is the comma-joined
// capability list the user chose this account as their default for.
func ai_test_account(database *DB, id, provider, key, designated string, enabled int) {
	database.exec("insert into accounts (id, type, data, created, enabled, \"default\") values (?, ?, ?, ?, ?, ?)",
		id, provider, `{"api_key":"`+key+`"}`, now(), enabled, designated)
}

// TestAiAccountPicksTheDesignatedDefault. interests_ai_summary took the first
// enabled AI-capable account ordered by id while mochi.ai.prompt took the one
// the user designated, so a user with two accounts had their interest summary
// billed to whichever they connected first.
func TestAiAccountPicksTheDesignatedDefault(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	_, database := ai_test_user(t, "aiaccount")
	// Ordered by id the first row wins; by designation the second does. The two
	// answers have to differ or nothing below is measuring anything.
	ai_test_account(database, "1first", "claude", "first-key", "", 1)
	ai_test_account(database, "2chosen", "claude", "chosen-key", "ai", 1)

	if _, key, _ := ai_account(database, ""); key != "chosen-key" {
		t.Errorf("default resolution gave key %q, want the designated account's", key)
	}
	// An explicit id still names that account, whatever the designation says.
	if _, key, _ := ai_account(database, "1first"); key != "first-key" {
		t.Errorf("explicit id gave key %q, want the named account's", key)
	}
}

// TestAiAccountRefusesUnusableAccounts. A disabled account and one whose
// provider has no ai capability both resolve to nothing, so the caller answers
// "no account" rather than dialling with an empty key.
func TestAiAccountRefusesUnusableAccounts(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	_, database := ai_test_user(t, "aiunusable")
	ai_test_account(database, "disabled", "claude", "key", "ai", 0)
	ai_test_account(database, "notai", "browser", "key", "ai", 1)

	if _, key, _ := ai_account(database, ""); key != "" {
		t.Errorf("an unusable designated account resolved key %q, want none", key)
	}
	if _, key, _ := ai_account(database, "disabled"); key != "" {
		t.Errorf("a disabled account named by id resolved key %q, want none", key)
	}
	if _, key, _ := ai_account(database, "notai"); key != "" {
		t.Errorf("an account with no ai capability resolved key %q, want none", key)
	}
}

// TestAiAccountAppliesTheDefaultModel. Both callers used to normalise the model
// themselves; the resolution does it once.
func TestAiAccountAppliesTheDefaultModel(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	_, database := ai_test_user(t, "aimodel")
	ai_test_account(database, "a", "claude", "key", "ai", 1)

	provider, _, model := ai_account(database, "")
	if provider != "claude" || model != ai_provider_defaults["claude"] {
		t.Errorf("resolved provider %q model %q, want claude and its default model", provider, model)
	}
}

// ai_test_caller grants accounts/ai to an app and returns a thread acting for
// the user through it.
func ai_test_caller(t *testing.T, user *User, app *App) *sl.Thread {
	t.Helper()
	grant := db_user(user, "user")
	grant.permissions_setup()
	grant.permissions_upsert(app.id, "accounts/ai", "", 1)
	return create_test_thread(user, app)
}

// TestAiPromptIsNotBudgeted. Core once refused the eleventh call in a minute
// and the sixty-first in an hour. The refusal raised inside the caller, so a
// feed poll that ingested more posts than the budget lost every post past it:
// untagged, and never broadcast to subscribers. A user who wants to cap what
// an account spends does that at the provider; core sends every call.
func TestAiPromptIsNotBudgeted(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user, database := ai_test_user(t, "aiunbudgeted")
	ai_test_account(database, "a", "claude", "key", "ai", 1)
	app := create_external_app("unbudgetedapp")
	thread := ai_test_caller(t, user, app)

	calls := 0
	previous := ai_call
	ai_call = func(provider, api_key, model, prompt string, tokens int) ai_result {
		calls++
		return ai_result{status: 200, text: "ok"}
	}
	defer func() { ai_call = previous }()

	fn := sl.NewBuiltin("mochi.ai.prompt", api_ai_prompt)
	// Well past both of the old budgets, from one app against one account.
	for i := 1; i <= 100; i++ {
		value, err := api_ai_prompt(thread, fn, sl.Tuple{sl.String("hello")}, nil)
		if err != nil {
			t.Fatalf("call %d was refused: %v", i, err)
		}
		status, _, _ := value.(*sl.Dict).Get(sl.String("status"))
		if n, _ := sl.AsInt32(status); n != 200 {
			t.Fatalf("call %d answered status %v, want 200 from the provider", i, status)
		}
	}
	if calls != 100 {
		t.Errorf("provider reached %d times, want 100: a call was answered without being sent", calls)
	}
}

// TestAiPromptRefusesAnOverlongPrompt. Apps fold user-supplied text into
// prompts, so the ceiling belongs on the builtin rather than on each caller.
func TestAiPromptRefusesAnOverlongPrompt(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user, _ := ai_test_user(t, "aiprompt")
	app := create_external_app("promptapp")
	thread := ai_test_caller(t, user, app)

	fn := sl.NewBuiltin("mochi.ai.prompt", api_ai_prompt)
	long := sl.String(strings.Repeat("x", ai_prompt_maximum+1))
	_, err := api_ai_prompt(thread, fn, sl.Tuple{long}, nil)
	if err == nil {
		t.Fatal("a prompt over the maximum was accepted")
	}
	if !strings.Contains(err.Error(), "too long") {
		t.Errorf("over-long prompt refused with %q, want the length refusal", err)
	}
	// One byte under is not refused for its length. No account is configured, so
	// it stops at account resolution and never reaches a provider.
	ok := sl.String(strings.Repeat("x", ai_prompt_maximum))
	if _, err := api_ai_prompt(thread, fn, sl.Tuple{ok}, nil); err != nil {
		t.Errorf("a prompt at exactly the maximum was refused: %v", err)
	}
}

// TestAiPromptBoundsTheTokenRequest. The output size was fixed at the ceiling,
// so every call paid for 16k it almost never used and no caller could ask for
// less.
func TestAiPromptBoundsTheTokenRequest(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user, _ := ai_test_user(t, "aitokens")
	app := create_external_app("tokensapp")
	thread := ai_test_caller(t, user, app)

	fn := sl.NewBuiltin("mochi.ai.prompt", api_ai_prompt)
	for _, tokens := range []int{0, -1, ai_tokens_maximum + 1} {
		kwargs := []sl.Tuple{{sl.String("tokens"), sl.MakeInt(tokens)}}
		_, err := api_ai_prompt(thread, fn, sl.Tuple{sl.String("hello")}, kwargs)
		if err == nil {
			t.Errorf("tokens=%d was accepted, want a refusal", tokens)
		} else if !strings.Contains(err.Error(), "invalid tokens") {
			t.Errorf("tokens=%d refused with %q, want the bounds refusal", tokens, err)
		}
	}
	// A value inside the range passes the bound and stops at account resolution.
	kwargs := []sl.Tuple{{sl.String("tokens"), sl.MakeInt(ai_tokens_maximum)}}
	if _, err := api_ai_prompt(thread, fn, sl.Tuple{sl.String("hello")}, kwargs); err != nil {
		t.Errorf("tokens at exactly the maximum was refused: %v", err)
	}
}

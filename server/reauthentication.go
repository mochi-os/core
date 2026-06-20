// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: step-up re-authentication proofs.
//
// A sensitive action (data export, replication approval, an
// account-security change) re-verifies the user with the same factor(s)
// they log in with, then proceeds. Each factor verified through the
// existing mochi.user.code.verify / mochi.user.totp.verify /
// mochi.user.passkey.verify builtins advances an accrual row here; once
// every required factor is covered, a single-use proof token is minted,
// and the action consumes it with reauthentication_consume before doing
// its work.
//
// The required factor set is the user's login methods (user.Methods), with
// recovery excluded (break-glass, not a routine re-auth) - so the proof is
// never below the user's own login bar. OAuth re-verifies as its own oauth
// factor: a linked provider proves the provider account, not the email
// inbox, so an account that requires email at login requires a real email
// code at step-up, never a provider sign-in. The reauthentication table
// lives in sessions.db and replicates like codes/partial, so a proof earned
// on one host is honoured if the action lands on another within the host set.

package main

import (
	"fmt"
	"strings"

	sl "go.starlark.net/starlark"
)

// Reauthentication is one in-progress-then-proof row from sessions.db.
type Reauthentication struct {
	Id      string
	User    string
	Methods string
	Expires int64
}

// reauthentication_required returns the factors the user must re-verify for
// a step-up: their required login methods, recovery excluded. Empty when
// nothing is required - any one usable factor then satisfies the step-up
// (mirroring all-allowed login). The verify verbs refuse a method the user
// has disabled, so "any one" never admits a turned-off factor.
func reauthentication_required(user *User) []string {
	seen := map[string]bool{}
	var out []string
	for _, m := range strings.Split(user.Methods, ",") {
		m = strings.TrimSpace(m)
		if m == "" || m == "recovery" {
			continue
		}
		if !seen[m] {
			seen[m] = true
			out = append(out, m)
		}
	}
	// Email required server-wide is part of the bar at login, so step-up must
	// clear it too (never weaker than login).
	if auth_method_state("email") == "required" && !seen["email"] {
		out = append(out, "email")
	}
	return out
}

// reauthentication_remaining returns the required factors not yet present
// in the completed set (comma-separated).
func reauthentication_remaining(user *User, completed string) []string {
	done := map[string]bool{}
	for _, m := range strings.Split(completed, ",") {
		if m = strings.TrimSpace(m); m != "" {
			done[m] = true
		}
	}
	var remaining []string
	for _, m := range reauthentication_required(user) {
		if !done[m] {
			remaining = append(remaining, m)
		}
	}
	return remaining
}

// reauthentication_advance records that factor was just verified for the
// user's in-progress step-up and returns the proof token once every
// required factor is satisfied, else "" and the still-remaining factors.
func reauthentication_advance(user *User, factor string) (string, []string) {
	sessions := db_open("db/sessions.db")
	expires := now() + 300

	var r Reauthentication
	have := sessions.scan(&r, "select id, user, methods, expires from reauthentication where user=? and expires>=? order by expires desc limit 1", user.UID, now())
	id := ""
	methods := ""
	if have {
		id = r.Id
		methods = r.Methods
	}
	if !reauthentication_contains(methods, factor) {
		if methods == "" {
			methods = factor
		} else {
			methods = methods + "," + factor
		}
	}

	if have {
		sessions.exec("update reauthentication set methods=?, expires=? where id=?", methods, expires, id)
	} else {
		id = uid()
		sessions.exec("insert into reauthentication ( id, user, methods, expires ) values ( ?, ?, ?, ? )", id, user.UID, methods, expires)
	}
	replication_emit_sessions_row(user.UID, &SessionsRow{
		Table: "reauthentication",
		Key:   map[string]string{"id": id},
		Cols: map[string]string{
			"user":    user.UID,
			"methods": methods,
			"expires": fmt.Sprintf("%d", expires),
		},
	})

	if remaining := reauthentication_remaining(user, methods); len(remaining) > 0 {
		return "", remaining
	}
	return id, nil
}

// reauthentication_consume verifies and consumes a completed step-up proof
// for the user, returning true if the token was valid, unexpired, matched
// the user, and covered the user's required factors. Mirrors code_consume,
// including the peer fan-out so a second host drops the token too.
func reauthentication_consume(user *User, token string) bool {
	if user == nil || token == "" {
		return false
	}
	sessions := db_open("db/sessions.db")
	var r Reauthentication
	if !sessions.scan(&r, "delete from reauthentication where id=? and user=? and expires>=? returning id, user, methods, expires", token, user.UID, now()) {
		return false
	}
	if len(reauthentication_remaining(user, r.Methods)) != 0 {
		return false
	}
	replication_emit_sessions_row(user.UID, &SessionsRow{
		Table:  "reauthentication",
		Key:    map[string]string{"id": token},
		Delete: true,
	})
	return true
}

// reauthentication_contains reports whether factor is already present in
// the comma-separated completed set.
func reauthentication_contains(completed, factor string) bool {
	for _, m := range strings.Split(completed, ",") {
		if strings.TrimSpace(m) == factor {
			return true
		}
	}
	return false
}

// reauthentication_result advances the accrual for a just-verified factor
// and returns the Starlark result a verify builtin hands back: a dict
// {"token": ...} once the step-up is complete, or {"remaining": [...]}
// when more factors are still needed.
func reauthentication_result(user *User, factor string) sl.Value {
	token, remaining := reauthentication_advance(user, factor)
	if token != "" {
		return sl_encode(map[string]any{"token": token})
	}
	return sl_encode(map[string]any{"remaining": remaining})
}

// Mochi server: restore requires a verified email address
//
// Restore creates an account from an uploaded bundle. The passphrase that
// decrypts the bundle authenticates the BUNDLE, not the person holding it, so
// on its own it says nothing about whether the address being claimed belongs
// to the caller. Ordinary signup emails a code; restore did not, which left it
// as the one route that could mint an account - and on an empty server an
// administrator - for an unproven address.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

// restore_code_issue writes a code for an address straight into sessions.db,
// as code_send does. Keyed on the address, not an account - the account is
// exactly what does not exist yet.
func restore_code_issue(t *testing.T, email string, code string, expires int64) {
	t.Helper()
	restore_tables_create(t)
	db_open("db/sessions.db").exec("replace into codes ( code, username, expires ) values ( ?, ?, ? )", code, email, expires)
}

// restore_tables_create builds the tables user_delete sweeps. A restore that
// gets past the code gate and then fails on its bundle cleans up the
// placeholder, and db.exec panics on a missing table, so without these the
// success path cannot be exercised at all.
func restore_tables_create(t *testing.T) {
	t.Helper()
	sessions := db_open("db/sessions.db")
	sessions.exec("create table if not exists codes ( code text not null primary key, username text not null, expires integer not null )")
	for _, table := range []string{"sessions", "ceremonies", "partial", "logins", "accesses", "passkeys", "verifications"} {
		sessions.exec("create table if not exists " + table + " ( user text not null )")
	}
	users := db_open("db/users.db")
	for _, table := range []string{"credentials", "totp", "recovery", "oauth"} {
		users.exec("create table if not exists " + table + " ( user text not null )")
	}
	users.exec("create table if not exists settings ( name text not null primary key, value text not null )")
	db_open("db/schedule.db").exec("create table if not exists schedule ( id integer primary key, user text not null, app text not null, due int not null, event text not null, data text not null, interval int not null, created int not null )")
}

// restore_post drives the handler with a multipart body carrying whatever
// fields are set, and reports the status, the error code, and whether an
// account now exists. The error code matters as well as the status: a refused
// code and an unreadable bundle are both 400, so only the code distinguishes
// "stopped at the gate" from "passed the gate and failed later".
func restore_post(t *testing.T, email string, code string) (int, string, bool) {
	t.Helper()
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	writer.WriteField("email", email)
	writer.WriteField("passphrase", "pp")
	if code != "" {
		writer.WriteField("code", code)
	}
	part, err := writer.CreateFormFile("bundle", "bundle.zip")
	if err != nil {
		t.Fatal(err)
	}
	part.Write(bytes.Repeat([]byte("a"), 64))
	writer.Close()

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("POST", "/_/auth/restore", body)
	c.Request.Header.Set("Content-Type", writer.FormDataContentType())
	web_auth_restore(c)

	var body_json struct {
		Error string `json:"error"`
	}
	json.Unmarshal(w.Body.Bytes(), &body_json)

	exists, _ := db_open("db/users.db").exists("select 1 from users where username=?", email)
	return w.Code, body_json.Error, exists
}

// TestRestoreRequiresCode — no code at all. The account must not be created:
// the placeholder row is what reserves the username, so minting it for an
// unproven address is the whole defect.
func TestRestoreRequiresCode(t *testing.T) {
	cleanup := create_test_users_db(t)
	defer cleanup()
	restore_tables_create(t)
	db_open("db/users.db").exec("insert into users (uid, username) values ('u1', 'first@example.com')")

	status, reason, created := restore_post(t, "new@example.com", "")
	if status != http.StatusBadRequest || reason != "missing_code" {
		t.Errorf("missing code: got %d/%q, want 400/missing_code", status, reason)
	}
	if created {
		t.Error("an account was created for an address the caller never proved they control")
	}
}

// TestRestoreRefusesWrongCode — a code was demanded and supplied, but not one
// issued to this address. Guessing must not be equivalent to receiving.
func TestRestoreRefusesWrongCode(t *testing.T) {
	cleanup := create_test_users_db(t)
	defer cleanup()
	restore_tables_create(t)
	db_open("db/users.db").exec("insert into users (uid, username) values ('u1', 'first@example.com')")
	restore_code_issue(t, "new@example.com", "GOODCODE12", now()+3600)

	status, reason, created := restore_post(t, "new@example.com", "WRONGCODE1")
	if status != http.StatusUnauthorized || reason != "invalid_code" {
		t.Errorf("wrong code: got %d/%q, want 401/invalid_code", status, reason)
	}
	if created {
		t.Error("a wrong code still created the account")
	}
}

// TestRestoreRefusesAnotherAddressCode — the code is real and unexpired but
// was issued to a different address. Codes are keyed on the address precisely
// so one cannot be replayed to claim another.
func TestRestoreRefusesAnotherAddressCode(t *testing.T) {
	cleanup := create_test_users_db(t)
	defer cleanup()
	restore_tables_create(t)
	db_open("db/users.db").exec("insert into users (uid, username) values ('u1', 'first@example.com')")
	restore_code_issue(t, "victim@example.com", "GOODCODE12", now()+3600)

	status, reason, created := restore_post(t, "attacker@example.com", "GOODCODE12")
	if status != http.StatusUnauthorized || reason != "invalid_code" {
		t.Errorf("another address's code: got %d/%q, want 401/invalid_code", status, reason)
	}
	if created {
		t.Error("a code issued to one address claimed another")
	}
}

// TestRestoreCodeAcceptedThenConsumed — the valid code must get PAST the gate
// (the request then fails further on, because the bundle here is not a real
// one, which is what distinguishes "code accepted" from "code rejected"), and
// must not be reusable afterwards.
func TestRestoreCodeAcceptedThenConsumed(t *testing.T) {
	cleanup := create_test_users_db(t)
	defer cleanup()
	restore_tables_create(t)
	db_open("db/users.db").exec("insert into users (uid, username) values ('u1', 'first@example.com')")
	restore_code_issue(t, "new@example.com", "GOODCODE12", now()+3600)

	status, reason, _ := restore_post(t, "new@example.com", "GOODCODE12")
	if reason == "missing_code" || reason == "invalid_code" {
		t.Fatalf("a valid code was refused at the gate: %d/%q", status, reason)
	}

	// Single use: the same code must not open the gate a second time.
	status, reason, created := restore_post(t, "new@example.com", "GOODCODE12")
	if status != http.StatusUnauthorized || reason != "invalid_code" {
		t.Errorf("code replay: got %d/%q, want 401/invalid_code", status, reason)
	}
	if created {
		t.Error("a replayed code created the account")
	}
}

// TestRestoreRefusesExpiredCode — expiry is enforced in the same statement
// that consumes, so an old code cannot be presented later.
func TestRestoreRefusesExpiredCode(t *testing.T) {
	cleanup := create_test_users_db(t)
	defer cleanup()
	restore_tables_create(t)
	db_open("db/users.db").exec("insert into users (uid, username) values ('u1', 'first@example.com')")
	restore_code_issue(t, "new@example.com", "GOODCODE12", now()-1)

	status, reason, created := restore_post(t, "new@example.com", "GOODCODE12")
	if status != http.StatusUnauthorized || reason != "invalid_code" {
		t.Errorf("expired code: got %d/%q, want 401/invalid_code", status, reason)
	}
	if created {
		t.Error("an expired code created the account")
	}
}

// TestRestoreBootstrapRequiresCode — the empty-server path mints an
// ADMINISTRATOR (first user becomes one, as with ordinary signup), so it is
// the case where an unproven address matters most.
func TestRestoreBootstrapRequiresCode(t *testing.T) {
	cleanup := create_test_users_db(t)
	defer cleanup()
	restore_tables_create(t)
	// No users: web_auth_restore takes the first-user/administrator path.

	status, reason, created := restore_post(t, "boot@example.com", "")
	if status != http.StatusBadRequest || reason != "missing_code" {
		t.Errorf("bootstrap without a code: got %d/%q, want 400/missing_code", status, reason)
	}
	if created {
		t.Error("an administrator account was created for an unproven address")
	}
}

// TestCodeConsumeEmailIsSingleUse — the helper itself, since code_consume for
// a known user now delegates to it and both paths depend on the delete-and-
// return being the thing that makes a code single-use.
func TestCodeConsumeEmailIsSingleUse(t *testing.T) {
	cleanup := create_test_users_db(t)
	defer cleanup()
	restore_tables_create(t)
	restore_code_issue(t, "someone@example.com", "GOODCODE12", now()+3600)

	if !code_consume_email("someone@example.com", "GOODCODE12") {
		t.Fatal("a valid code was refused")
	}
	if code_consume_email("someone@example.com", "GOODCODE12") {
		t.Error("the same code was consumed twice")
	}
	if code_consume_email("", "GOODCODE12") || code_consume_email("someone@example.com", "") {
		t.Error("an empty address or code was accepted")
	}
}

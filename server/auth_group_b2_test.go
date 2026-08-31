// Mochi server: tests for the account and export hardening (#611, #613, #614, #617)
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/gin-gonic/gin"
	sl "go.starlark.net/starlark"
)

// TestExportAccountsExtract is #611. The bundle's plain user.db copy must not
// carry connected-account credentials; they belong in the passphrase-encrypted
// secrets.age beside the authenticator secret and the recovery hashes.
func TestExportAccountsExtract(t *testing.T) {
	test_data_directory(t)

	staged := db_open(filepath.Join("staged", "user.db"))
	if staged == nil {
		t.Fatal("db_open returned nil")
	}
	t.Cleanup(func() { staged.close() })
	staged.exec("create table accounts (id text primary key, type text not null default '', data text not null default '')")
	staged.exec("insert into accounts (id, type, data) values ('a-1', 'ntfy', '{\"token\":\"secret-one\"}')")
	staged.exec("insert into accounts (id, type, data) values ('a-2', 'url', '{\"token\":\"secret-two\"}')")
	staged.exec("insert into accounts (id, type, data) values ('a-3', 'email', '')")

	taken := export_accounts_extract(staged)
	if len(taken) != 2 {
		t.Fatalf("extracted %d secrets, want 2", len(taken))
	}
	found := map[string]string{}
	for _, s := range taken {
		found[s.Id] = s.Data
	}
	if found["a-1"] != `{"token":"secret-one"}` || found["a-2"] != `{"token":"secret-two"}` {
		t.Errorf("extracted the wrong payloads: %v", found)
	}

	// And the staged copy - the one that goes into the bundle in plain - is blank.
	rows, err := staged.rows("select id, data from accounts")
	if err != nil {
		t.Fatalf("reading back: %v", err)
	}
	for _, r := range rows {
		if as_string(r["data"]) != "" {
			t.Errorf("account %s still carries its credential in the staged copy", as_string(r["id"]))
		}
	}

	if got := export_accounts_extract(nil); len(got) != 0 {
		t.Errorf("nil staged database returned %d secrets, want 0", len(got))
	}
}

// TestOauthUnlinkKeepsAWayBackIn is #613. The unlink used to consult only
// "is there another oauth row, or is email allowed server-wide", which ignores
// the user's own required and disabled sets - both of which strand the account.
func TestOauthUnlinkKeepsAWayBackIn(t *testing.T) {
	test_data_directory(t)
	db_create()

	db := db_open("db/users.db")
	db.exec("insert into users (uid, username, role, methods) values ('u-link', 'link@example.com', 'user', 'oauth')")
	db.exec("insert into oauth (user, provider, subject, created) values ('u-link', 'google', 's', ?)", now())

	unlink := func(user *User) error {
		thread := create_test_thread(user, create_internal_app("settings"))
		_, err := api_user_oauth_unlink(thread, sl.NewBuiltin("unlink", api_user_oauth_unlink), sl.Tuple{sl.String("google")}, nil)
		return err
	}
	linked := func() bool {
		ok, _ := db.exists("select 1 from oauth where user='u-link' and provider='google'")
		return ok
	}

	// OAuth is in the user's required set: unlinking leaves a factor the account
	// can never satisfy.
	required := &User{UID: "u-link", Username: "link@example.com", Methods: "oauth"}
	if err := unlink(required); err == nil {
		t.Error("unlinked the last provider while oauth was required")
	}
	if !linked() {
		t.Error("the row was deleted despite the refusal")
	}

	// Email and the rest switched off: oauth is the only way back in.
	only := &User{UID: "u-link", Username: "link@example.com", Disabled: "email,passkey,totp"}
	if err := unlink(only); err == nil {
		t.Error("unlinked the only usable sign-in factor")
	}
	if !linked() {
		t.Error("the row was deleted despite the refusal")
	}

	// Email still available, so the unlink is safe and must go through.
	spare := &User{UID: "u-link", Username: "link@example.com"}
	if err := unlink(spare); err != nil {
		t.Errorf("a safe unlink was refused: %v", err)
	}
	if linked() {
		t.Error("the provider is still linked after a successful unlink")
	}
}

// TestMethodsSetRejectsLockouts is #617. The legacy list setter validated
// against the allowed-method list, which admits recovery (break-glass, and
// there is no /_/auth handler for it) and an oauth with no linked provider.
func TestMethodsSetRejectsLockouts(t *testing.T) {
	test_data_directory(t)
	db_create()

	db := db_open("db/users.db")
	db.exec("insert into users (uid, username, role) values ('u-set', 'set@example.com', 'user')")
	user := &User{UID: "u-set", Username: "set@example.com"}

	set := func(methods ...string) error {
		values := []sl.Value{}
		for _, m := range methods {
			values = append(values, sl.String(m))
		}
		thread := create_test_thread(user, create_internal_app("settings"))
		_, err := api_user_methods_set(thread, sl.NewBuiltin("set", api_user_methods_set),
			sl.Tuple{sl.NewList(values)}, nil)
		return err
	}
	stored := func() string {
		row, _ := db.row("select methods from users where uid='u-set'")
		if row == nil {
			return ""
		}
		return as_string(row["methods"])
	}

	for _, method := range []string{"recovery", "oauth", "totp", "passkey"} {
		if err := set("email", method); err == nil {
			t.Errorf("required set accepted %q: the account can never satisfy it", method)
		}
		if stored() != "" {
			t.Errorf("required set was written anyway after refusing %q: %q", method, stored())
			db.exec("update users set methods='' where uid='u-set'")
		}
	}

	// Email is available to every account, so this one is legitimate.
	if err := set("email"); err != nil {
		t.Errorf("email-only was refused: %v", err)
	}
	if stored() != "email" {
		t.Errorf("stored methods = %q, want \"email\"", stored())
	}
}

// TestRestoreRefusesBundleBeforeCode is #614. The route is unauthenticated, so
// the emailed code has to be checked while the bundle is still on the wire -
// not after up to 2 GiB has been spooled to disk.
func TestRestoreRefusesBundleBeforeCode(t *testing.T) {
	test_data_directory(t)
	db_create()
	gin.SetMode(gin.TestMode)

	body := &bytes.Buffer{}
	form := multipart.NewWriter(body)
	form.WriteField("email", "restore@example.com")
	form.WriteField("passphrase", "correct horse battery staple")
	// The bundle before the code: the ordering the fix refuses.
	file, _ := form.CreateFormFile("bundle", "bundle.zip")
	file.Write(bytes.Repeat([]byte("Z"), 512*1024))
	form.WriteField("code", "123456")
	form.Close()

	counter := &counting_reader{inner: bytes.NewReader(body.Bytes())}
	request := httptest.NewRequest("POST", "/_/auth/restore", counter)
	request.Header.Set("Content-Type", form.FormDataContentType())
	response := httptest.NewRecorder()

	router := gin.New()
	router.POST("/_/auth/restore", web_auth_restore)
	router.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Errorf("status %d, want %d: a bundle arriving before the code must be refused", response.Code, http.StatusBadRequest)
	}
	// The discriminating half: refused *before* the bundle was drained. The
	// whole body is 512 KiB plus headers; stopping at the bundle part's header
	// means well under a tenth of that was read.
	if counter.read > 64*1024 {
		t.Errorf("the server read %d bytes of a %d-byte body: the bundle was spooled before the code was checked", counter.read, body.Len())
	}
}

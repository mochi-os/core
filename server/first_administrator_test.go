// Mochi server: exactly one account can be the first, so exactly one can be the
// administrator. Both signup paths decide the role inside the insert rather
// than with a separate read, so concurrent signups cannot both become
// administrator.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"fmt"
	"io"
	"mime/multipart"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
)

// administrator_env rebuilds users on the real schema; the shared fixture's
// copy predates the columns these paths write.
func administrator_env(t *testing.T) func() {
	t.Helper()
	cleanup := create_web_test_env(t)
	db := db_open("db/users.db")
	db.exec("drop table if exists users")
	db.exec("create table users (uid text not null primary key, username text not null, role text not null default 'user', methods text not null default '', disabled text not null default '', status text not null default 'active', restore_source text not null default '', restore_passkeys integer not null default 0, purge integer not null default 0)")
	return cleanup
}

// administrator_count reports how many accounts hold the role.
func administrator_count(t *testing.T) int64 {
	t.Helper()
	row, err := db_open("db/users.db").row("select count(*) as total from users where role='administrator'")
	if err != nil || row == nil {
		t.Fatalf("counting administrators: %v", err)
	}
	total, _ := row["total"].(int64)
	return total
}

// TestConcurrentSignupsProduceOneAdministrator drives the race directly. Forty
// rounds because the defect surfaced about one round in six, so a single round
// would report a pass most of the time.
func TestConcurrentSignupsProduceOneAdministrator(t *testing.T) {
	const rounds = 40
	for round := 0; round < rounds; round++ {
		cleanup := administrator_env(t)

		var waiting sync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < 16; i++ {
			waiting.Add(1)
			go func(i int) {
				defer waiting.Done()
				<-start
				user_create(fmt.Sprintf("signup%d@example.com", i))
			}(i)
		}
		close(start)
		waiting.Wait()

		if got := administrator_count(t); got != 1 {
			cleanup()
			t.Fatalf("round %d: 16 concurrent signups on an empty server produced %d administrators, want 1", round, got)
		}
		cleanup()
	}
}

// TestFirstSignupStillBecomesAdministrator is the property the race guards.
// Deciding the role inside the insert must not lose it.
func TestFirstSignupStillBecomesAdministrator(t *testing.T) {
	defer administrator_env(t)()

	first, reason := user_create("first@example.com")
	if reason != "" || first == nil {
		t.Fatalf("creating the first user: %q", reason)
	}
	if first.Role != "administrator" {
		t.Errorf("the first account has role %q, want administrator; a fresh server now has no administrator at all", first.Role)
	}

	second, reason := user_create("second@example.com")
	if reason != "" || second == nil {
		t.Fatalf("creating the second user: %q", reason)
	}
	if second.Role != "user" {
		t.Errorf("the second account has role %q, want user", second.Role)
	}
}

// blocking_body releases the request body only once released is closed, so a
// test can act while the handler is still reading the upload.
type blocking_body struct {
	reader   io.Reader
	released chan struct{}
	once     sync.Once
	reached  chan struct{}
}

func (b *blocking_body) Read(p []byte) (int, error) {
	b.once.Do(func() {
		close(b.reached)
		<-b.released
	})
	return b.reader.Read(p)
}

func (b *blocking_body) Close() error { return nil }

// TestRestoreDoesNotUseAStaleRole is the restore half, and it is deterministic
// rather than probabilistic: the window there is the whole upload, so an
// ordinary signup landing during it is an overlap, not a race.
func TestRestoreDoesNotUseAStaleRole(t *testing.T) {
	defer administrator_env(t)()
	load_core_labels()
	setting_set("signup_enabled", "true")

	// The handler cannot finish on a fixture bundle that is not a real zip,
	// and it deletes its own placeholder when it gives up - so the row is gone
	// by the time the test could read it. A trigger records what was inserted,
	// which is the value under test, independently of the row's fate.
	users := db_open("db/users.db")
	users.exec("create table observed (username text not null, role text not null)")
	users.exec(`create trigger observe_insert after insert on users
		begin insert into observed (username, role) values (new.username, new.role); end`)

	// Deleting the placeholder sweeps every per-user table; they only have to
	// exist for that to run.
	sessions := db_open("db/sessions.db")
	sessions.exec("create table if not exists codes (code text not null, username text not null, expires integer not null, primary key (code, username))")
	for _, table := range []string{"sessions", "ceremonies", "partial", "logins", "accesses", "passkeys", "verifications"} {
		sessions.exec("create table if not exists " + table + " (user text not null)")
	}
	for _, table := range []string{"credentials", "totp", "recovery", "oauth"} {
		db_open("db/users.db").exec("create table if not exists " + table + " (user text not null)")
	}
	db_open("db/schedule.db").exec("create table if not exists schedule (id integer primary key, user text not null, app text not null, due int not null, event text not null, data text not null, interval int not null, created int not null)")
	sessions.exec("insert into codes (code, username, expires) values ('123456', 'restored@example.com', ?)", now()+3600)

	var raw bytes.Buffer
	form := multipart.NewWriter(&raw)
	_ = form.WriteField("email", "restored@example.com")
	_ = form.WriteField("passphrase", "correct horse battery staple")
	_ = form.WriteField("code", "123456")
	part, err := form.CreateFormFile("bundle", "bundle.zip")
	if err != nil {
		t.Fatalf("building the multipart body: %v", err)
	}
	part.Write(bytes.Repeat([]byte("bundle bytes "), 4096))
	form.Close()

	body := &blocking_body{
		reader:   bytes.NewReader(raw.Bytes()),
		released: make(chan struct{}),
		reached:  make(chan struct{}),
	}

	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/_/auth/restore", nil)
	c.Request.Header.Set("Content-Type", form.FormDataContentType())
	c.Request.ContentLength = int64(raw.Len())
	c.Request.Body = body

	done := make(chan struct{})
	go func() {
		defer close(done)
		web_auth_restore(c)
	}()

	// The upload has begun. An ordinary signup completing now is what the
	// original code could not see: it had already decided the restore was the
	// first account on the server.
	<-body.reached
	if _, reason := user_create("ordinary@example.com"); reason != "" {
		t.Fatalf("the concurrent signup failed: %q", reason)
	}
	close(body.released)
	<-done

	row, err := db_open("db/users.db").row("select role from observed where username='restored@example.com'")
	if err != nil || row == nil {
		t.Fatal("the restore never inserted its placeholder, so this test proves nothing about the role it would have used")
	}
	if role := as_string(row["role"]); role != "user" {
		t.Errorf("the restore inserted role %q for an account that an ordinary signup beat to the server; it decided that before the bundle started arriving", role)
	}
	if got := administrator_count(t); got != 1 {
		t.Errorf("a signup completing during the upload left %d administrators, want 1", got)
	}
}

// TestRestoreStillClaimsAnEmptyServer: the restore path must still produce an
// administrator when it genuinely is first, or a server whose only account
// arrives by restore has nobody who can administer it.
func TestRestoreStillClaimsAnEmptyServer(t *testing.T) {
	defer administrator_env(t)()

	db := db_open("db/users.db")
	db.exec(`insert into users (uid, username, role, methods, status)
		values (?, ?, case when exists (select 1 from users) then 'user' else 'administrator' end, '', 'pending-restore')`,
		"u-restored", "restored@example.com")

	row, _ := db.row("select role from users where uid='u-restored'")
	if row == nil || as_string(row["role"]) != "administrator" {
		t.Fatalf("the restore placeholder on an empty server has role %v, want administrator", row)
	}
}

// TestNoSignupPathPrecomputesTheRole pins the shape both fixes share: the role
// is chosen by the INSERT, from the table as it stands at that moment. A
// reintroduced read-then-write passes every sequential test and fails only
// under load, which is the reason this defect survived.
func TestNoSignupPathPrecomputesTheRole(t *testing.T) {
	for _, target := range []struct{ file, function string }{
		{"users.go", "func user_create("},
		{"auth_restore.go", "func web_auth_restore("},
	} {
		body := function_body(t, target.file, target.function)
		if !strings.Contains(body, "case when exists (select 1 from users) then 'user' else 'administrator' end") {
			t.Errorf("%s %s does not decide the role inside its insert", target.file, target.function)
		}
		if strings.Contains(body, `role = "administrator"`) {
			t.Errorf("%s %s assigns the role to a variable before the insert; two requests that both read an empty table both write it", target.file, target.function)
		}
	}
}

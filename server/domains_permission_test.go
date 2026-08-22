// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"errors"
	"os"
	"regexp"
	"strings"
	"testing"

	sl "go.starlark.net/starlark"
)

// TestEveryDomainAPIIsGated. Without require_permission any app an
// administrator opened could repoint a hostname or read the DNS verification
// token. The read/write split matches what the API does; domain_can_manage
// answers who may.
func TestEveryDomainAPIIsGated(t *testing.T) {
	source, err := os.ReadFile("domains.go")
	if err != nil {
		t.Fatalf("read domains.go: %v", err)
	}
	text := string(source)

	functions := regexp.MustCompile(`(?sm)^func (api_domain[a-z_0-9]*)\(t \*sl\.Thread.*?\n\}$`)
	found := 0
	for _, m := range functions.FindAllStringSubmatch(text, -1) {
		name, body := m[1], m[0]
		found++
		if !strings.Contains(body, "require_permission(t, fn,") {
			t.Errorf("%s has no require_permission; an app needs no grant to call it", name)
		}
	}
	if found < 15 {
		t.Errorf("only %d domain builtins found; the scan is not matching them", found)
	}
}

// TestDomainPermissionsAreRestrictedButNotAdministratorOnly. Restricted, so no
// consent dialog hands an app the server's hostnames; not administrator-only,
// because domain_can_manage_route lets a delegate manage one path.
func TestDomainPermissionsAreRestrictedButNotAdministratorOnly(t *testing.T) {
	for _, permission := range []string{"domains/read", "domains/write"} {
		if !permission_restricted(permission) {
			t.Errorf("%s is not restricted", permission)
		}
		if permission_administrator(permission) {
			t.Errorf("%s is administrator-only, which locks out route delegates", permission)
		}
	}

	for _, permission := range []string{"domains/read", "domains/write"} {
		found := false
		for _, app := range apps_default {
			if app.Name != "Settings" {
				continue
			}
			for _, g := range app.Permissions {
				if g.Permission == permission {
					found = true
				}
			}
		}
		if !found {
			t.Errorf("Settings has no %s default grant; it is the only app that manages domains and cannot re-grant a restricted permission itself", permission)
		}
	}
}

// TestDomainVerifyChecksTheUser. Every other mutator in the file read a user;
// verify read none, while it makes an unrate-limited outbound DNS lookup and,
// on a match, writes verified=1 into the server-global domains.db - the flag
// domain_match consults before serving a host.
func TestDomainVerifyChecksTheUser(t *testing.T) {
	create_test_routing_env(t)

	app := create_external_app("prober")
	apps[app.id] = app
	user := &User{UID: "u1", Username: "user1@example.com"}
	thread := create_test_thread(user, app)
	fn := sl.NewBuiltin("mochi.domain.verify", nil)

	_, err := api_domain_verify(thread, fn, sl.Tuple{sl.String("example.com")}, nil)
	if err == nil {
		t.Fatal("an ungranted app was allowed to trigger domain verification")
	}
	var denied *PermissionError
	if !errors.As(err, &denied) || denied.Permission != "domains/write" {
		t.Fatalf("refused with %v, want a domains/write PermissionError", err)
	}

	// The domain must exist, or domain_verify fails for an unrelated reason and
	// asserting that some error came back proves nothing.
	db := db_user(user, "user")
	db.permissions_setup()
	db.permissions_upsert(app.id, "domains/write", "", 1)

	domains := db_open("db/domains.db")
	domains.exec("create table if not exists domains (domain text primary key, verified integer not null default 0, token text not null default '', tls integer not null default 1, created integer not null, updated integer not null)")
	domains.exec("create table if not exists delegations (domain text not null, path text not null, user text not null, created integer not null default 0)")
	domains.exec("insert into domains (domain, verified, token, tls, created, updated) values (?, 0, 'secret', 1, 0, 0)", "someone-else.invalid")

	_, err = api_domain_verify(thread, fn, sl.Tuple{sl.String("someone-else.invalid")}, nil)
	if err == nil {
		t.Fatal("a granted app verified a domain it does not manage")
	}
	if !strings.Contains(err.Error(), "access denied") {
		t.Errorf("refused with %q, want the access-denied user check - anything else means verification ran for a user who does not manage the domain", err)
	}
}

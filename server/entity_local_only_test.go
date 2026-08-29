// Mochi server: an entity URL segment resolves against this server only.
//
// entity_by_any answers "does this server hold the entity", not "does this
// entity exist anywhere". A directory row is a pointer to another host, not a
// local entity, and routing on it would hand a request meant for that host to
// this one's storage. Apps rely on the narrower answer: repositories' proxy
// reads run only when a.entity resolved, which is what keeps them bounded to
// callers on this server.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import "testing"

// A local entity resolves by id and by fingerprint - the two forms a URL
// segment can take. Without this the absence checks below would pass on a
// function that never finds anything.
func TestEntityByAnyFindsALocalEntity(t *testing.T) {
	setup_replication_test(t)
	setup_users_test_schema()

	db := db_open("db/users.db")
	db.exec("insert into users (uid, username) values ('u-local', 'local@example.com')")
	db.exec("insert into entities (id, private, fingerprint, user, class, name) values ('e-local', 'k', 'f-local', 'u-local', 'repository', 'Local')")

	if e := entity_by_any("e-local"); e == nil || e.ID != "e-local" {
		t.Fatalf("entity_by_any(id) = %v, want the local entity", e)
	}
	if e := entity_by_any("f-local"); e == nil || e.ID != "e-local" {
		t.Fatalf("entity_by_any(fingerprint) = %v, want the local entity", e)
	}
}

// The bound itself: an entity this server knows of only through the directory
// is not resolvable as a route. If this ever starts passing, entity routing has
// widened to every entity the network has published, and the comment on
// apps/repositories' proxy_entity - which says a repository hosted elsewhere
// does not route here - is no longer true.
func TestEntityByAnyIgnoresADirectoryOnlyEntity(t *testing.T) {
	setup_replication_test(t)
	setup_users_test_schema()

	directory := db_open("db/directory.db")
	directory.exec("create table if not exists entries ( entity text not null, peer text not null, name text not null, class text not null, data text not null default '', fingerprint text not null default '', version integer not null default 0, created integer not null, seen integer not null, message text not null default '', expires text not null default '', signature text not null default '', primary key ( entity, peer ) )")
	directory.exec("insert into entries (entity, peer, name, class, fingerprint, created, seen) values ('e-remote', 'peer-elsewhere', 'Remote', 'repository', 'f-remote', 1, 1)")

	if got, _ := directory.exists("select entity from entries where entity='e-remote'"); !got {
		t.Fatal("the directory row was not written; the assertions below would pass vacuously")
	}

	if e := entity_by_any("e-remote"); e != nil {
		t.Errorf("entity_by_any(%q) resolved to %v; a directory entry names another host's entity, and routing on it would run the request against this server's storage", "e-remote", e)
	}
	if e := entity_by_any("f-remote"); e != nil {
		t.Errorf("entity_by_any(fingerprint of a directory-only entity) resolved to %v, want nothing", e)
	}
}

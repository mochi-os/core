// Mochi server: an entity that has been deleted is not a signing fault.
//
// pubsub_sign returns nil for two unrelated reasons - the entity row is gone,
// or it is present with an unusable private key - and entity_sign already
// separates them, logging info for the first and warn for the second. Both
// callers then warned on any nil, which re-escalated the benign case: a
// publish already in flight when the entity was deleted mailed the operator
// about a race it had handled correctly. On 2026-08-17 the p2p harness created
// and purged its fixture users and produced 14 of these across 7 entities in
// one second.
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
)

// TestEntityPresentSeesALiveEntity, and does not see one that never existed or
// has been deleted. Everything else here rests on this answer.
func TestEntityPresentSeesALiveEntity(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()

	id := withdraw_test_entity(t)
	db := db_open("db/users.db")
	db.exec("insert into users (uid, username) values ('u-present', 'present@x')")
	db.exec("insert into entities (id, private, fingerprint, user, class, name) values (?, 'k', ?, 'u-present', 'person', 'Present')", id, fingerprint(id))

	if !entity_present(id) {
		t.Error("a live entity reads as absent, so a real signing fault would be silenced as a race")
	}
	if entity_present("1nosuchentityatall") {
		t.Error("an entity that never existed reads as present")
	}
	if entity_present("") {
		t.Error("the empty id reads as present")
	}

	db.exec("delete from entities where id=?", id)
	if entity_present(id) {
		t.Error("a deleted entity still reads as present, which is the case the whole change exists for")
	}
}

// TestSigningFailuresSeparateGoneFromBroken. Both callers of pubsub_sign must
// consult entity_present before warning; one that does not re-raises a race as
// an operator email, which is the reported symptom.
func TestSigningFailuresSeparateGoneFromBroken(t *testing.T) {
	sites := map[string]string{
		"directory.go": "Directory unable to sign entry",
		"pubsub.go":    "Pubsub refusing to flood unsigned announcement",
	}
	for file, message := range sites {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		text := string(body)
		at := strings.Index(text, message)
		if at < 0 {
			t.Errorf("%s: %q not found", file, message)
			continue
		}
		// The guard has to come before the warn, and close to it.
		window := text[:at]
		guard := strings.LastIndex(window, "entity_present(")
		if guard < 0 {
			t.Errorf("%s warns on any nil signature; a deleted entity mails the operator about a handled race", file)
			continue
		}
		if strings.Count(text[guard:at], "\n") > 6 {
			t.Errorf("%s: the entity_present guard is %d lines above the warn, so it is probably guarding something else",
				file, strings.Count(text[guard:at], "\n"))
		}
	}
}

// TestEntitySignStillSeparatesItsOwnCases. The layer below is where the
// distinction is actually drawn; if it stops drawing it, the callers' guard is
// guessing rather than deferring.
func TestEntitySignStillSeparatesItsOwnCases(t *testing.T) {
	body, err := os.ReadFile("entities.go")
	if err != nil {
		t.Fatalf("read entities.go: %v", err)
	}
	fn := string(body)[strings.Index(string(body), "func entity_sign("):]
	fn = fn[:strings.Index(fn, "\n}")]

	if !strings.Contains(fn, `info("Signature entity %q not found"`) {
		t.Error("entity_sign no longer treats a missing entity as informational")
	}
	if !strings.Contains(fn, `warn("Signature entity %q empty private key"`) {
		t.Error("entity_sign no longer warns about an unusable key, which is the case that IS a fault")
	}
}

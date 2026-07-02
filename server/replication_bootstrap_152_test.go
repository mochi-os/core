// Bootstrap robustness fixes (#152): verify the assembled file against its
// manifest hash before accepting it, paginate the DB manifest, and block app SQL
// from writing the replication journal. (The multi-user-per-peer authz fix is
// covered by TestBootstrapPerUserClamp.)
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	cbor "github.com/fxamacker/cbor/v2"
)

// #152.1: a fully-assembled file must be verified against the manifest hash
// BEFORE it is exposed at its final path; a mismatch drops the .partial.
func TestBootstrapWriteChunkHashCheck(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	content := []byte("hello world")
	sum := sha256.Sum256(content)
	good := hex.EncodeToString(sum[:])
	rel := "alice/feed/files/x.md"
	root, _ := bootstrap_file_scope_root(bootstrap_scope_files)
	final, _ := bootstrap_safe_path(root, rel)

	// Wrong hash: rejected, no final file, .partial cleaned up.
	if err := bootstrap_write_chunk(bootstrap_scope_files, rel, 0, content, true, strings.Repeat("00", 32)); err == nil {
		t.Fatal("a hash mismatch must be rejected")
	}
	if _, e := os.Stat(final); e == nil {
		t.Error("final file must not exist after a hash mismatch")
	}
	if _, e := os.Stat(final + ".partial"); e == nil {
		t.Error(".partial must be removed after a hash mismatch")
	}

	// Correct hash: accepted and renamed to final.
	if err := bootstrap_write_chunk(bootstrap_scope_files, rel, 0, content, true, good); err != nil {
		t.Fatalf("correct hash must be accepted: %v", err)
	}
	if got, _ := os.ReadFile(final); string(got) != string(content) {
		t.Errorf("final content = %q, want %q", got, content)
	}
}

// #152.3: the DB manifest is sent in pages ending with Done=true, so a large
// fleet can't produce a single oversized (cbor_max_elements) message.
func TestBootstrapDBManifestPaginated(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	// A pair member fetches the whole-server userdbs manifest (user="").
	db_open("db/replication.db").exec("insert into pair (peer, added, role) values ('ppair', 1, '')")
	pair_membership_refresh()

	// Five synthetic per-user DBs → five manifest entries.
	for _, u := range []string{"u1", "u2", "u3", "u4", "u5"} {
		dir := filepath.Join(data_dir, "users", u)
		os.MkdirAll(dir, 0o755)
		os.WriteFile(filepath.Join(dir, "user.db"), []byte("x"), 0o644)
	}
	old := bootstrap_manifest_page_size
	bootstrap_manifest_page_size = 2
	defer func() { bootstrap_manifest_page_size = old }()

	var buf bytes.Buffer
	replication_bootstrap_db_manifest_event(&Event{
		peer:    "ppair",
		content: map[string]any{"scope": bootstrap_scope_userdbs, "user": "", "paginated": true},
		stream:  &Stream{writer: filePushTestWriteCloser{&buf}},
	})

	dec := cbor.NewDecoder(&buf)
	pages, total, done := 0, 0, false
	for {
		var res BootstrapDBManifestResult
		if err := dec.Decode(&res); err != nil {
			break
		}
		pages++
		total += len(res.Entries)
		if res.Done {
			done = true
		}
	}
	if pages != 3 {
		t.Errorf("pages = %d, want 3 (5 entries / page size 2)", pages)
	}
	if total != 5 {
		t.Errorf("total entries = %d, want 5", total)
	}
	if !done {
		t.Error("the final page must carry Done=true")
	}
}

// #152.4: app SQL may not write the core replication `journal` table (deleting it
// drops un-shipped ops → silent divergence; forging rows manipulates the stream).
func TestJournalTableBlockedFromAppSQL(t *testing.T) {
	blocked := []string{
		"delete from journal",
		"delete from journal where state='pending'",
		"insert into journal (id) values ('x')",
		"update journal set state='shipped'",
		"REPLACE INTO journal (id) VALUES ('x')",
		"  \n update  journal  set state='x'",
	}
	for _, q := range blocked {
		if db_starlark_sql_blocked(q) == "" {
			t.Errorf("app write to journal must be blocked: %q", q)
		}
	}
	allowed := []string{
		"insert into posts (id) values ('x')", // app data
		"delete from journals",                // an app's OWN table (not 'journal')
		"select * from journal",               // a read is harmless
	}
	for _, q := range allowed {
		if r := db_starlark_sql_blocked(q); r != "" {
			t.Errorf("legitimate query blocked (%q): %s", q, r)
		}
	}
}

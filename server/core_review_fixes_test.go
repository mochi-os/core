// Mochi server: fixes from the 2026-08-21 core review
//
// One file for the assertions that have no obvious existing home: a guard's
// spelling, a validator, an error classification, a status the completion path
// must not overwrite. The concurrency and protocol fixes are tested beside the
// code they touch.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strings"
	"testing"

	sl "go.starlark.net/starlark"
)

// --- the introspection cache guard --------------------------------------
//
// mochi.db.table and mochi.db.indexes emit the bare statement form, whose
// lowercase is "pragma table_info(...)" - no underscore. The guard matched only
// the vtable spelling, so those statements were prepared and cached, which is
// the stale-schema case the guard's own comment cites (a table_info guard
// missing a present column and re-running its ALTER).
func TestIntrospectionGuardCoversTheBarePragma(t *testing.T) {
	introspection := []string{
		"PRAGMA table_info(feeds)",                 // mochi.db.table
		"PRAGMA index_list(feeds)",                 // mochi.db.indexes
		"select * from pragma_table_info('feeds')", // the vtable form
		"select name from sqlite_master where type='table'",
		"select name from sqlite_schema",
	}
	for _, query := range introspection {
		if !sql_is_introspection(query) {
			t.Errorf("%q is not treated as introspection, so it would be cached with a stale schema view", query)
		}
	}
	ordinary := []string{
		"select id from feeds where user=?",
		"insert into feeds ( id, name ) values ( ?, ? )",
		"update feeds set name=? where id=?",
	}
	for _, query := range ordinary {
		if sql_is_introspection(query) {
			t.Errorf("%q is treated as introspection; ordinary statements should still be cached", query)
		}
	}
}

// --- a missing function attributable to a failed load --------------------

// call() used to blame the app's handler declaration for a function its own
// file never got to define. The message sent the reader to the wrong file, and
// the P2P classifier dropped the event as unsupported.
func TestCallNamesTheFileThatFailedToLoad(t *testing.T) {
	s := &Starlark{globals: sl.StringDict{}, failed: []string{"attachments.star"}}

	_, err := s.call("database_create", nil)
	if err == nil {
		t.Fatal("a missing function returned no error")
	}
	if !errors.Is(err, ErrStarlarkLoad) {
		t.Error("the error is not marked as load-attributable, so the P2P path still drops the event")
	}
	if got := err.Error(); !strings.Contains(got, "attachments.star") {
		t.Errorf("the message does not name the file that broke: %q", got)
	}
}

// The control: with every file loaded, a missing function really is an
// undeclared handler and must stay unmarked, so it is still dropped rather than
// retried fifty times.
func TestCallWithNoFailedFilesIsNotLoadAttributable(t *testing.T) {
	s := &Starlark{globals: sl.StringDict{}}

	_, err := s.call("database_create", nil)
	if err == nil {
		t.Fatal("a missing function returned no error")
	}
	if errors.Is(err, ErrStarlarkLoad) {
		t.Error("an undeclared handler was marked load-attributable; it would now be retried forever")
	}
}

// The half that loses events: this message starts with "Starlark app function",
// which the prefix switch drops as unsupported.
func TestLoadFailureIsTransientNotUnsupported(t *testing.T) {
	load := fmt.Errorf("Starlark app function %q not found: %s failed to load: %w",
		"database_create", "attachments.star", ErrStarlarkLoad)
	if got := worker_failure_reason(load); got != fail_transient {
		t.Errorf("a load failure classified %q, want %q: the event is dropped instead of retried", got, fail_transient)
	}

	undeclared := errors.New(`Starlark app function "event_nonexistent" not found`)
	if got := worker_failure_reason(undeclared); got != fail_unsupported {
		t.Errorf("an undeclared handler classified %q, want %q", got, fail_unsupported)
	}
}

// --- a.redirect on a non-GET --------------------------------------------
//
// gin's Redirect records the status but only writes a body for a GET, so on any
// other method Written() stays false and the completion path forced 200 - the
// Location survived on a status no browser acts on.
func TestStatusDeliberateProtectsAHandlerRedirect(t *testing.T) {
	for _, status := range []int{http.StatusFound, http.StatusMovedPermanently, http.StatusSeeOther, http.StatusNotModified} {
		if !web_status_deliberate(status) {
			t.Errorf("%d is treated as incidental, so the completion path would overwrite it with 200", status)
		}
	}
	// The two the fallback installs itself, which the path must still be free
	// to replace.
	for _, status := range []int{http.StatusNotFound, http.StatusOK} {
		if web_status_deliberate(status) {
			t.Errorf("%d is treated as deliberate, so a fire-and-forget action would keep the NoRoute 404", status)
		}
	}
}

// --- peer ingest from an arbitrary remote host ---------------------------
//
// peer_connect_url feeds peer_add_known straight from a remote /_/p2p/info, and
// the peers map is only pruned on a 24-hour tick.
func TestPeerAddKnownRefusesAMalformedIdentifier(t *testing.T) {
	previous := peers
	peers = map[string]Peer{}
	t.Cleanup(func() { peers = previous })

	peer_add_known("not a peer id", []string{"/ip4/1.2.3.4/tcp/4001"})
	if len(peers) != 0 {
		t.Errorf("a malformed peer id was admitted to the process-global map: %v", peers)
	}
}

func TestPeerAddKnownFiltersUndialableAddresses(t *testing.T) {
	previous := peers
	peers = map[string]Peer{}
	t.Cleanup(func() { peers = previous })

	id := "12D3KooWGPbbqBmyGpTLxVLZzhNYhbHZBMDdFbEcxHnLzVCJmqXt"
	peer_add_known(id, []string{
		"not-a-multiaddress",         // unparseable, and handed to the dialer as-is
		"/ip4/0.0.0.0/tcp/4001",      // unspecified: no host to dial
		"/ip4/198.51.100.7/tcp/4001", // routable
		"/ip4/127.0.0.1/tcp/4001",    // loopback, which the two local instances use
	})
	p, found := peers[id]
	if !found {
		t.Fatal("a well-formed peer with usable addresses was refused entirely")
	}
	if len(p.addresses) != 2 {
		t.Errorf("kept %d addresses, want 2 (the routable one and loopback): %+v", len(p.addresses), p.addresses)
	}
}

// --- a.logout ------------------------------------------------------------
//
// Ending the session is the capability mochi.user.session.revoke gates on
// user/sessions/write. Reaching it through the action object skipped the gate,
// so any installed app could end the user's session.
func TestLogoutRequiresTheSessionPermission(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "u1")
	app := create_external_app("test-external-app")
	thread := create_test_thread(user, app)

	// a.web is nil: with the gate in place the refusal happens before anything
	// touches the request, so this must return an error rather than panic.
	a := &Action{user: user}
	if _, err := a.sl_logout(thread, sl.NewBuiltin("logout", nil), nil, nil); err == nil {
		t.Error("a.logout() succeeded without user/sessions/write: any installed app can end the session")
	} else if !contains_permission_error(err) {
		t.Errorf("a.logout() failed for the wrong reason: %v", err)
	}
}

// --- mochi.message.send.peer --------------------------------------------
//
// Only emptiness was checked, so a malformed peer became a queue row that can
// never deliver and is retried on the backoff schedule for the life of the row.
func TestMessageSendPeerValidatesTheIdentifier(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "u1")
	app := create_external_app("test-external-app")
	thread := create_test_thread(user, app)
	fn := sl.NewBuiltin("mochi.message.send.peer", nil)

	// The call fails either way further down (no headers), so "an error
	// occurred" proves nothing: the assertion has to be that it was refused AS a
	// peer, and that a well-formed one gets past that check.
	for _, peer := range []string{"", "not a peer id", "../../etc/passwd", "12D3Koo W"} {
		_, err := api_message_send_peer(thread, fn, sl.Tuple{sl.String(peer), sl.String("svc"), sl.String("ev")}, nil)
		if err == nil || !strings.Contains(err.Error(), "peer not specified or invalid") {
			t.Errorf("peer %q was not refused as a peer (%v); it becomes a permanently-failing queue row", peer, err)
		}
	}

	_, err := api_message_send_peer(thread, fn,
		sl.Tuple{sl.String("12D3KooWGPbbqBmyGpTLxVLZzhNYhbHZBMDdFbEcxHnLzVCJmqXt"), sl.String("svc"), sl.String("ev")}, nil)
	if err != nil && strings.Contains(err.Error(), "peer not specified or invalid") {
		t.Errorf("a well-formed peer was refused: the validator is too strict (%v)", err)
	}
}

// --- the two sweeps, asserted as invariants ------------------------------
//
// Both are "every site in this file does X" rather than one behaviour, and a
// new site that forgets is the regression that matters. Source assertions catch
// that; a behavioural test on one site would not.

// mochi.file.* resolved principal_caller while a.upload, mochi.db and
// mochi.cache all resolve principal_storage, so under domain routing and on the
// OpenGraph path the row and the file it names came from different accounts.
func TestFilesResolveTheStorageAccount(t *testing.T) {
	body, err := os.ReadFile("files.go")
	if err != nil {
		t.Fatalf("read files.go: %v", err)
	}
	if n := strings.Count(string(body), "principal_caller("); n != 0 {
		t.Errorf("files.go has %d principal_caller call sites; mochi.file.* must resolve the same storage account as a.upload and mochi.db", n)
	}
	if strings.Count(string(body), "principal_storage(") == 0 {
		t.Error("files.go resolves no principal at all")
	}
}

// db_app_system hands back a shared handle holding two sqlx pools, and
// db_manager only evicts one whose close() has marked it idle. A site that
// never closes pins that user's app.db for the life of the process.
func TestEveryAppSystemHandleIsReleased(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package directory: %v", err)
	}
	open := regexp.MustCompile(`(\w+)\s*(?::=|=)\s*db_app_system\(`)
	unreleased := []string{}
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		body, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		lines := strings.Split(string(body), "\n")
		for i, line := range lines {
			match := open.FindStringSubmatch(line)
			if match == nil || strings.Contains(line, "func db_app_system") {
				continue
			}
			end := len(lines)
			for j := i + 1; j < len(lines); j++ {
				if lines[j] == "}" {
					end = j
					break
				}
			}
			if !strings.Contains(strings.Join(lines[i:end], "\n"), match[1]+".close()") {
				unreleased = append(unreleased, fmt.Sprintf("%s:%d (%s)", name, i+1, match[1]))
			}
		}
	}
	if len(unreleased) > 0 {
		t.Errorf("%d db_app_system handles are never marked idle, so db_manager can never evict them:\n  %s",
			len(unreleased), strings.Join(unreleased, "\n  "))
	}
}

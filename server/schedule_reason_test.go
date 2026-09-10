// Mochi server: a due event that cannot run now is classified, and only a
// final reason retires its recurring row.
//
// The classification has to see what the loader saw. Outside dev_reload the
// version caches its globals once for the process lifetime, so the list of
// files that failed to load is cached beside them; without it a handler
// missing because its file broke reads as "never declared" and the row is
// retired for good.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"strings"
	"testing"
	"time"
)

// reason_app registers an app whose single version executes the given files,
// as the loader builds it at startup.
func reason_app(t *testing.T, id string, files ...string) *App {
	t.Helper()
	av := &AppVersion{Version: "1.0", Execute: files}
	a := &App{id: id, versions: map[string]*AppVersion{"1.0": av}, latest: av}
	apps_lock.Lock()
	apps[id] = a
	apps_lock.Unlock()
	return a
}

func TestScheduleCheckReasons(t *testing.T) {
	create_test_cleanup_env(t)
	log_tables_reset(t)
	defer log_tables_reset(t)
	log_captured(t)
	resolution_invalidate()

	directory := t.TempDir()
	clean := starlark_file(t, directory, "clean.star", "def tick(e):\n    pass\n")
	broken := starlark_file(t, directory, "broken.star", "def digest(e):\n    pass\n\nCONFIG = undefined_name_here\n")

	reason_app(t, "clean-app", clean)
	reason_app(t, "broken-app", clean, broken)
	apps_lock.Lock()
	apps["empty-app"] = &App{id: "empty-app", versions: map[string]*AppVersion{}}
	apps_lock.Unlock()

	cases := []struct {
		name  string
		app   string
		event string
		want  schedule_reason
	}{
		{"handler defined", "clean-app", "tick", schedule_runnable},
		{"handler absent from a clean load", "clean-app", "digest", schedule_handler_absent},
		{"handler absent while a file failed to load", "broken-app", "digest", schedule_handler_failed},
		{"handler in the clean file of a partly failed app", "broken-app", "tick", schedule_runnable},
		{"app with no version", "empty-app", "tick", schedule_version_absent},
		{"app unknown", "no-such-app", "tick", schedule_app_absent},
	}
	for _, c := range cases {
		got := schedule_check(&ScheduledEvent{User: "", App: c.app, Event: c.event, Interval: 300})
		if got != c.want {
			t.Errorf("%s: got %s, want %s", c.name, got, c.want)
		}
	}
}

// TestActiveDoesNotCacheAnAbsentVersion. An app between versions resolves to
// nil; holding that answer for the cache TTL keeps the version that lands
// invisible, so every due event in the window is judged against nothing.
func TestActiveDoesNotCacheAnAbsentVersion(t *testing.T) {
	create_test_cleanup_env(t)
	resolution_invalidate()

	a := &App{id: "landing-app", versions: map[string]*AppVersion{}}
	apps_lock.Lock()
	apps["landing-app"] = a
	apps_lock.Unlock()

	if av := a.active(nil); av != nil {
		t.Fatalf("an app with no versions resolved to %v", av)
	}

	av := &AppVersion{Version: "1.0"}
	apps_lock.Lock()
	a.versions["1.0"] = av
	a.latest = av
	apps_lock.Unlock()

	if got := a.active(nil); got != av {
		t.Errorf("the version that landed is invisible: active() = %v, want %v; the cache is holding the nil answer", got, av)
	}
}

// TestOneShotSurvivesAnUpgradeWindow. The claim used to be the delete, so a
// one-shot that came due while its app had no active version was gone before
// anything looked at it. The claim now holds the row past now, the check
// defers it, and it runs once - and only once - when the version lands.
func TestOneShotSurvivesAnUpgradeWindow(t *testing.T) {
	create_test_cleanup_env(t)
	log_tables_reset(t)
	defer log_tables_reset(t)
	capture := log_captured(t)
	resolution_invalidate()
	if starlark_semaphore == nil {
		starlark_semaphore = make(chan struct{}, 4)
	}
	timeout := starlark_default_timeout
	starlark_default_timeout = 30 * time.Second
	t.Cleanup(func() { starlark_default_timeout = timeout })
	db_open("db/schedule.db").exec(`create table schedule (id integer primary key,
		user text not null, app text not null, due int not null, event text not null,
		data text not null, interval int not null, created int not null)`)

	a := &App{id: "window-app", versions: map[string]*AppVersion{}}
	apps_lock.Lock()
	apps["window-app"] = a
	apps_lock.Unlock()

	id, err := schedule_create("", "window-app", now()-1, "tick", "{}", 0)
	if err != nil || id == 0 {
		t.Fatalf("could not create the one-shot: %v", err)
	}

	if !schedule_claim(id, 0) {
		t.Fatal("the due one-shot was not claimed")
	}
	held := schedule_get(id)
	if held == nil {
		t.Fatal("the claim deleted the one-shot before schedule_check could look at it")
	}
	if held.Due < now()+schedule_retry_seconds-1 {
		t.Errorf("a held one-shot is due at %d, want at least now+%d so the scheduler does not spin on it", held.Due, schedule_retry_seconds)
	}
	if schedule_claim(id, 0) {
		t.Error("a held one-shot was claimed a second time; two scheduler passes would both run it")
	}

	// No active version: the check defers, and the row must survive.
	schedule_run(*held)
	if schedule_get(id) == nil {
		t.Fatal("a one-shot due during an upgrade window was dropped; nothing recreates it")
	}

	// The version lands, and the retry comes due.
	file := starlark_file(t, t.TempDir(), "app.star", "def tick(e):\n    mochi.log.debug(\"one-shot ran\")\n")
	av := &AppVersion{Version: "1.0", Execute: []string{file}}
	apps_lock.Lock()
	a.versions["1.0"] = av
	a.latest = av
	apps_lock.Unlock()
	schedule_update_due(id, now()-1)

	if !schedule_claim(id, 0) {
		t.Fatal("the retried one-shot was not claimed")
	}
	schedule_run(*schedule_get(id))

	if schedule_get(id) != nil {
		t.Error("the one-shot row survived its run; it would fire again after the retry delay")
	}
	if lines := strings.Join(capture.lines, "\n"); !strings.Contains(lines, "one-shot ran") {
		t.Errorf("the handler did not run once the version landed; log:\n%s", lines)
	}
}

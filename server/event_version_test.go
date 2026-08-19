// Mochi server: routing an event to an app with no loadable version.
//
// route() screens a nil version on the app_by_id branch ("No active version
// for this app") and then calls active() a second time for the branch that
// reaches it by service, unchecked - so av.Events dereferenced nil.
//
// The nil is reachable from the wire: app_external registers an App into the
// registry BEFORE its version is read, so a failed load leaves an app with an
// empty version map for the life of the process, and app_for_service_resolve
// looks an inbound service name up as an app id. The panic is contained by the
// guard() at each P2P entry point, so the cost is a warn - which emails the
// operator - and a reset stream, not a crash.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"strings"
	"testing"
	"time"
)

// versionless_app registers an app the way a failed version load leaves one:
// present in the registry, external (so active() does not short-circuit on
// a.internal), with no versions and no latest.
func versionless_app(t *testing.T, id, service string) *App {
	t.Helper()
	a := app_external(id)

	apps_lock.Lock()
	a.versions = map[string]*AppVersion{}
	a.latest = nil
	a.internal = nil
	apps_lock.Unlock()

	t.Cleanup(func() {
		apps_lock.Lock()
		delete(apps, id)
		apps_lock.Unlock()
		resolution_invalidate()
	})
	resolution_invalidate()
	return a
}

// TestAnAppWithNoVersionResolvesToNothing is the premise: this really is the
// state active() reports nil for.
func TestAnAppWithNoVersionResolvesToNothing(t *testing.T) {
	a := versionless_app(t, "app-with-no-versions", "brokenservice")

	if av := a.active(nil); av != nil {
		t.Fatalf("active() returned %v for an app with no versions; this test is not reproducing the state it describes", av)
	}
}

// TestRoutingToAVersionlessAppDoesNotPanic is the defect. route() reached
// av.Events with av nil.
func TestRoutingToAVersionlessAppDoesNotPanic(t *testing.T) {
	versionless_app(t, "app-with-no-versions", "brokenservice")

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("routing an event to an app with no loadable version panicked: %v", r)
		}
	}()

	// Step 4 of app_for_service_resolve looks the service name up as an app
	// id, which is how a remote frame reaches this app at all.
	e := &Event{id: event_id(), service: "app-with-no-versions", event: "some/event"}
	err := e.route()

	if err == nil {
		t.Fatal("route reported success for an app that cannot handle anything")
	}
	if !strings.Contains(err.Error(), "no active version") {
		t.Errorf("route error = %q; want it to name the missing version, so the log says which app needs repair", err)
	}
}

// TestAMissingVersionIsPermanent: the sender must stop redelivering. An
// unrecognised message classifies as transient, which is 50 deliveries of a
// failure that cannot resolve itself until an operator acts.
func TestAMissingVersionIsPermanent(t *testing.T) {
	versionless_app(t, "app-with-no-versions", "brokenservice")

	e := &Event{id: event_id(), service: "app-with-no-versions", event: "some/event"}
	err := e.route()
	if err == nil {
		t.Fatal("route reported success")
	}
	if reason := worker_failure_reason(err); reason != fail_unsupported {
		t.Errorf("worker_failure_reason = %q, want %q; a retryable reason means the sender redelivers a failure only the operator can fix", reason, fail_unsupported)
	}
}

// TestTheEventLookupReleasesItsLockOnPanic is what turned this from noisy into
// fatal. apps_lock is a process-global mutex every app lookup takes, and the
// lookup used a bare Lock/Unlock pair, so a panic between them left it held -
// the guard() at each P2P entry point then contained the faulted goroutine and
// the server stopped routing anything at all. The nil check makes THIS panic
// impossible; the deferred unlock makes the next one survivable.
func TestTheEventLookupReleasesItsLockOnPanic(t *testing.T) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() { _ = recover() }()
		var missing *AppVersion
		missing.event("some/event") // panics inside the lock window
	}()
	<-done

	// If the unlock did not run, this blocks forever and the test times out.
	locked := make(chan struct{})
	go func() {
		apps_lock.Lock()
		apps_lock.Unlock()
		close(locked)
	}()
	select {
	case <-locked:
	case <-time.After(5 * time.Second):
		t.Fatal("apps_lock is still held after a panic inside the event lookup; every app lookup in the process now blocks forever")
	}
}

// TestBothVersionLookupsInRouteAreChecked is the shape. route() calls active()
// twice and the defect was that only one result was screened; a third call
// added later must be screened too.
func TestBothVersionLookupsInRouteAreChecked(t *testing.T) {
	body := function_body(t, "events.go", "func (e *Event) route()")

	calls := strings.Count(body, "a.active(e.user)")
	if calls < 2 {
		t.Fatalf("route() makes %d calls to a.active; this test is reading the wrong function", calls)
	}
	if checks := strings.Count(body, "av != nil") + strings.Count(body, "av == nil"); checks < calls {
		t.Errorf("route() resolves a version %d times but screens the result %d times; every use of av dereferences it", calls, checks)
	}
}

// TestAWorkingAppStillRoutes keeps the guard from swallowing ordinary traffic:
// an app with a version must still reach its handler lookup, and fail there
// for its own reason rather than for a missing version.
func TestAWorkingAppStillRoutes(t *testing.T) {
	a := versionless_app(t, "app-with-a-version", "workingservice")

	av := &AppVersion{Version: "1.0", Events: map[string]AppEvent{}}
	av.app = a
	apps_lock.Lock()
	a.versions["1.0"] = av
	a.latest = av
	apps_lock.Unlock()
	resolution_invalidate()

	e := &Event{id: event_id(), service: "app-with-a-version", event: "absent/event"}
	err := e.route()
	if err == nil {
		t.Fatal("route reported success for an event the app does not declare")
	}
	if strings.Contains(err.Error(), "no active version") {
		t.Errorf("an app with a loaded version was refused for a missing version: %v", err)
	}
	if !strings.Contains(err.Error(), "unknown event") {
		t.Errorf("route error = %q, want the unknown-event refusal", err)
	}
}

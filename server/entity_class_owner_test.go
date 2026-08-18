// Mochi server: an app may only change or destroy entities of a class it handles.
//
// entity_class_allowed asks app_declares_class, which reads the calling app's
// OWN manifest. For every class but "person" that is a self-assertion: an app
// that adds "classes": ["feed"] to its app.json could rename, re-privacy and
// destroy the user's feeds, and mochi.entity.update/delete carry no permission
// gate of their own - the class check is the entire authorization.
//
// The answer was already in the tree. class_app_for resolves a class to its
// handler from the user's binding, then the system binding, then install order:
// three sources the calling app does not control. Creating still goes on the
// manifest, because two apps may legitimately create one class (Apps and
// Publisher both create "app" entities) and the resolution deliberately names
// a single handler.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"regexp"
	"strings"
	"testing"

	sl "go.starlark.net/starlark"
)

// class_owner_share opens a class on an app already built by class_owner_app.
func class_owner_share(app *App, class string) *App {
	app.latest.Shared = append(app.latest.Shared, class)
	return app
}

// class_owner_app builds an app declaring one class. latest is set as well as
// versions: with no user preference and no system default, active() resolves
// through to a.latest, and an app without it declares nothing at all.
func class_owner_app(id string, class string) *App {
	version := &AppVersion{Version: "1.0", Classes: []string{class}}
	return &App{id: id, versions: map[string]*AppVersion{"1.0": version}, latest: version}
}

// class_owner_environment registers two apps that both declare "feed" and
// returns them with a user. handler is the one class_app_for resolves to; other
// declares the class just as loudly and is the app the gate must stop.
func class_owner_environment(t *testing.T) (handler *App, other *App, user *User) {
	t.Helper()
	cleanup := create_test_routing_env(t)
	t.Cleanup(cleanup)

	handler = class_owner_app("feeds", "feed")
	other = class_owner_app("impostor", "feed")
	apps["feeds"] = handler
	apps["impostor"] = other

	user = &User{UID: "u1", Username: "user1@example.com"}
	apps_class_set("feed", "feeds")
	return handler, other, user
}

func class_owner_thread(app *App, user *User) *sl.Thread {
	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("app", app)
	thread.SetLocal("user", user)
	return thread
}

// TestClassOwnerAllowsTheHandler. The app that handles the class must keep
// working, or the gate has broken every app in the tree rather than the one it
// was aimed at.
func TestClassOwnerAllowsTheHandler(t *testing.T) {
	handler, _, user := class_owner_environment(t)

	fn := sl.NewBuiltin("mochi.entity.update", nil)
	if err := entity_class_owned(class_owner_thread(handler, user), fn, handler, user, "feed"); err != nil {
		t.Errorf("the class handler was refused its own class: %v", err)
	}
}

// TestClassOwnerRefusesAnotherApp is the defect. Declaring the class is the
// whole of what the old check asked for, and declaring it is free.
func TestClassOwnerRefusesAnotherApp(t *testing.T) {
	_, other, user := class_owner_environment(t)

	fn := sl.NewBuiltin("mochi.entity.delete", nil)
	err := entity_class_owned(class_owner_thread(other, user), fn, other, user, "feed")
	if err == nil {
		t.Error("an app that merely declares the class was allowed to act on it; adding a line to app.json is enough to destroy another app's entities")
	}
}

// TestClassOwnerFollowsTheUserBinding. Resolution is per user, so the answer
// has to be the one this user's bindings give - not a server-wide default.
func TestClassOwnerFollowsTheUserBinding(t *testing.T) {
	handler, other, user := class_owner_environment(t)

	user.set_class_app("feed", other.id)

	fn := sl.NewBuiltin("mochi.entity.update", nil)
	if err := entity_class_owned(class_owner_thread(other, user), fn, other, user, "feed"); err != nil {
		t.Errorf("the app this user binds the class to was refused: %v", err)
	}
	if err := entity_class_owned(class_owner_thread(handler, user), fn, handler, user, "feed"); err == nil {
		t.Error("the system-bound app still passed after the user bound the class elsewhere; the check is not reading the user's binding")
	}
}

// TestClassOwnerStillRequiresTheDeclaration. The handler check is added to the
// manifest check, not substituted for it: an app that does not declare the
// class at all must still be refused, whatever the binding says.
func TestClassOwnerStillRequiresTheDeclaration(t *testing.T) {
	_, _, user := class_owner_environment(t)

	stranger := class_owner_app("stranger", "wiki")
	apps["stranger"] = stranger
	user.set_class_app("feed", stranger.id) // bound, but does not declare it

	fn := sl.NewBuiltin("mochi.entity.delete", nil)
	if err := entity_class_owned(class_owner_thread(stranger, user), fn, stranger, user, "feed"); err == nil {
		t.Error("an app that does not declare the class passed because it held the binding")
	}
}

// TestClassOwnerFallsBackWhenNobodyHandles. A class with no resolvable handler
// must not become a refusal that strands the entity - the manifest check alone
// then decides, exactly as before.
func TestClassOwnerFallsBackWhenNobodyHandles(t *testing.T) {
	handler, _, user := class_owner_environment(t)

	// A class the fixture's apps declare, with every candidate removed so the
	// resolution has nothing to return.
	orphan := class_owner_app("orphan", "ghost")
	delete(apps, "feeds")
	delete(apps, "impostor")
	resolution_invalidate()

	fn := sl.NewBuiltin("mochi.entity.delete", nil)
	if err := entity_class_owned(class_owner_thread(orphan, user), fn, orphan, user, "ghost"); err != nil {
		t.Errorf("an entity whose class no installed app handles became undeletable: %v", err)
	}
	_ = handler
}

// TestEntityWritesUseTheOwnerCheck pins the wiring. The two functions differ by
// one call, so a call site left on the permissive one is invisible in behaviour
// tests of the helpers.
func TestEntityWritesUseTheOwnerCheck(t *testing.T) {
	body, err := os.ReadFile("entities.go")
	if err != nil {
		t.Fatalf("reading entities.go: %v", err)
	}
	source := string(body)

	for _, want := range []string{
		"func api_entity_delete",
		"func api_entity_update",
	} {
		start := strings.Index(source, want)
		if start < 0 {
			t.Fatalf("%s not found", want)
		}
		end := strings.Index(source[start+1:], "\nfunc ")
		if end < 0 {
			end = len(source) - start - 1
		}
		region := source[start : start+1+end]
		if !strings.Contains(region, "entity_class_owned(") {
			t.Errorf("%s calls the permissive class check; any app declaring the class can act on an entity it did not make", want)
		}
	}

	// Create has its own helper: the handler check would break publishing,
	// since Apps and Publisher both create "app" entities and only one of them
	// can be the handler.
	start := strings.Index(source, "func api_entity_create")
	end := strings.Index(source[start+1:], "\nfunc ")
	create := source[start : start+1+end]
	if strings.Contains(create, "entity_class_owned(") {
		t.Error("api_entity_create uses the handler check; that refuses whichever of Apps and Publisher is not the handler, so publishing breaks")
	}
	if !strings.Contains(create, "entity_class_shared(") {
		t.Error("api_entity_create does not use the shared check; any app declaring a class can mint entities in it")
	}
}

// TestUserRoutingBindingsAreGated. The bindings are what the check above reads,
// so an app able to write them can make itself the answer and walk straight
// through it. Source-level because the accessors are many and uniform, and a
// new one added later is exactly the case that would slip past.
func TestUserRoutingBindingsAreGated(t *testing.T) {
	body, err := os.ReadFile("users.go")
	if err != nil {
		t.Fatalf("reading users.go: %v", err)
	}
	source := string(body)

	accessor := regexp.MustCompile(`func \(p \*UserApp(Class|Service|Path|Version)\) (get|set|delete|list)\([^)]*\) \(sl\.Value, error\) \{\n(.*)\n`)
	found := 0
	for _, m := range accessor.FindAllStringSubmatch(source, -1) {
		found++
		want := "apps/write"
		if m[2] == "get" || m[2] == "list" {
			want = "apps/read"
		}
		if !strings.Contains(m[3], `require_permission(t, fn, "`+want+`")`) {
			t.Errorf("a.user.app.%s.%s does not require %s: any installed app could repoint the user's routing",
				strings.ToLower(m[1]), m[2], want)
		}
	}
	if found < 15 {
		t.Errorf("matched only %d binding accessors, expected at least 15; the pattern has drifted and this test is no longer checking what it claims", found)
	}
}

// TestEntityCreateRefusesAnUninvitedApp is the create half of the defect. An
// app that adds a class to its manifest could mint entities in it, which the
// class's real app then lists as the user's own.
func TestEntityCreateRefusesAnUninvitedApp(t *testing.T) {
	_, other, user := class_owner_environment(t)

	fn := sl.NewBuiltin("mochi.entity.create", nil)
	if err := entity_class_shared(class_owner_thread(other, user), fn, other, user, "feed"); err == nil {
		t.Error("an app that merely declares the class was allowed to create in it")
	}
}

// TestEntityCreateAllowsTheHandler. The class's own app never needs an
// invitation to its own class.
func TestEntityCreateAllowsTheHandler(t *testing.T) {
	handler, _, user := class_owner_environment(t)

	fn := sl.NewBuiltin("mochi.entity.create", nil)
	if err := entity_class_shared(class_owner_thread(handler, user), fn, handler, user, "feed"); err != nil {
		t.Errorf("the class handler was refused a create in its own class: %v", err)
	}
}

// TestEntityCreateAllowsAnInvitedApp is the case that made the handler check
// unusable for create: Publisher mints an "app" entity when a developer
// publishes, Apps mints one when a user sideloads, and only one of them can be
// the handler. Both declaring the class shared is how that is expressed.
func TestEntityCreateAllowsAnInvitedApp(t *testing.T) {
	handler, other, user := class_owner_environment(t)
	class_owner_share(handler, "feed")
	class_owner_share(other, "feed")

	fn := sl.NewBuiltin("mochi.entity.create", nil)
	if err := entity_class_shared(class_owner_thread(other, user), fn, other, user, "feed"); err != nil {
		t.Errorf("an app invited by the class handler was refused: %v", err)
	}
}

// TestEntityCreateInvitationIsNotSelfIssued. The newcomer's own declaration is
// the half that is worthless alone - otherwise the manifest still decides and
// nothing has been gained.
func TestEntityCreateInvitationIsNotSelfIssued(t *testing.T) {
	_, other, user := class_owner_environment(t)
	class_owner_share(other, "feed") // the handler does NOT share it

	fn := sl.NewBuiltin("mochi.entity.create", nil)
	if err := entity_class_shared(class_owner_thread(other, user), fn, other, user, "feed"); err == nil {
		t.Error("an app admitted itself by declaring someone else's class shared")
	}
}

// TestEntityCreateSharingDoesNotOpenTheWrites. Sharing a class is an invitation
// to add, never to change or destroy what another app made.
func TestEntityCreateSharingDoesNotOpenTheWrites(t *testing.T) {
	handler, other, user := class_owner_environment(t)
	class_owner_share(handler, "feed")
	class_owner_share(other, "feed")

	fn := sl.NewBuiltin("mochi.entity.delete", nil)
	if err := entity_class_owned(class_owner_thread(other, user), fn, other, user, "feed"); err == nil {
		t.Error("an invited co-creator was allowed to delete; sharing must not confer the destructive half")
	}
}

// TestSharedClassesAreDeclaredWhereTheyAreCreated pins the two manifests to the
// code. Apps and Publisher both create "app" entities, so publishing breaks the
// moment either stops declaring the class shared.
func TestSharedClassesAreDeclaredWhereTheyAreCreated(t *testing.T) {
	for _, app := range []string{"apps", "publisher"} {
		path := "../../apps/" + app + "/app.json"
		body, err := os.ReadFile(path)
		if err != nil {
			t.Skipf("app tree not present: %v", err)
		}
		if !strings.Contains(string(body), `"shared": ["app"]`) {
			t.Errorf("%s does not declare the \"app\" class shared; it creates app entities and would be refused", path)
		}
	}
}

// TestSharedSurvivesAManifestReload. Classes and Shared are read together by
// the create check, so they have to be refreshed together: app.json hot-reloads
// for a development app, and a field left out of that copy keeps whatever it
// held at load while its sibling moves on. Source-level because the reload path
// wants a real app directory on disk, and the failure is a missing line rather
// than a wrong answer.
func TestSharedSurvivesAManifestReload(t *testing.T) {
	body, err := os.ReadFile("apps.go")
	if err != nil {
		t.Fatalf("reading apps.go: %v", err)
	}
	source := string(body)

	classes := strings.Index(source, "av.Classes = fresh.Classes")
	if classes < 0 {
		t.Fatal("the manifest reload no longer copies Classes; this test is checking the wrong place")
	}
	if !strings.Contains(source, "av.Shared = fresh.Shared") {
		t.Error("the manifest reload does not copy Shared: an app that opens or closes a class while running keeps its old sharing for the life of the process, while its declared classes update")
	}
}

// TestHandlerChoiceIsStableAcrossRestarts. Which app handles a class decides
// who may change and destroy the user's entities, and candidates reach
// app_select_best in Go map order, which is randomised per process. Install
// time settles it only when the times differ, and a default app set installs
// in one batch - on the production server 24 of 26 apps share two adjacent
// seconds. Without a deterministic tie-break the answer would move from one
// restart to the next.
func TestHandlerChoiceIsStableAcrossRestarts(t *testing.T) {
	cleanup := create_test_routing_env(t)
	defer cleanup()

	// Four apps declaring one class, all recorded at the same instant.
	moment := now()
	for _, id := range []string{"delta", "alpha", "charlie", "bravo"} {
		apps[id] = class_owner_app(id, "shared")
		db_open("db/apps.db").exec("replace into apps (app, installed) values (?, ?)", id, moment)
	}

	first := class_app_for(nil, "shared")
	if first == nil {
		t.Fatal("no handler resolved")
	}
	for i := 0; i < 50; i++ {
		resolution_invalidate()
		again := class_app_for(nil, "shared")
		if again == nil || again.id != first.id {
			t.Fatalf("resolution %d returned %v, first returned %s: the handler moves between restarts, so who may delete an entity does too",
				i, again, first.id)
		}
	}
	if first.id != "alpha" {
		t.Errorf("tie resolved to %q, want the lowest id \"alpha\" - any rule will do, but it has to be one a reader can predict", first.id)
	}
}

// TestEarlierInstallStillWins. The tie-break must not displace install order:
// an app installed after an incumbent must never take its class.
func TestEarlierInstallStillWins(t *testing.T) {
	cleanup := create_test_routing_env(t)
	defer cleanup()

	moment := now()
	apps["incumbent"] = class_owner_app("incumbent", "shared")
	apps["aaa-newcomer"] = class_owner_app("aaa-newcomer", "shared")
	db := db_open("db/apps.db")
	db.exec("replace into apps (app, installed) values (?, ?)", "incumbent", moment-3600)
	db.exec("replace into apps (app, installed) values (?, ?)", "aaa-newcomer", moment)

	resolution_invalidate()
	if a := class_app_for(nil, "shared"); a == nil || a.id != "incumbent" {
		t.Errorf("handler is %v, want incumbent: a later-installed app took an existing class by sorting first", a)
	}
}

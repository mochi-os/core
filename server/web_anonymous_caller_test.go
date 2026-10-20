// Mochi server: an anonymous caller is nobody, not the owner.
//
// The account whose data is read and the identity a request carries are
// separate questions; principal_storage answers the first. These tests pin the
// binding and the permission gates a public action can reach, which must
// tolerate a nil caller.//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	sl "go.starlark.net/starlark"
)

// anonymous_reachable is the audited set: every permission-gated API a
// public: true action can reach, by walking each app's Starlark call graph
// from its public entry points. Each must tolerate a nil caller, because a
// crawler or a webhook reaches all of them with no identity at all.
var anonymous_reachable = []string{
	"api_access_check",
	"api_account_list",
	"api_ai_prompt",
	"api_entity_owned",
	"api_interests_bottom",
	"api_interests_list",
	"api_interests_summary",
	"api_interests_top",
	// The one gate behind mochi.url.*, mochi.url.preview, mochi.rss.fetch and
	// mochi.remote.peer. The comptroller's Stripe webhook is a public action
	// and reaches all of them.
	"require_permission_url",
}

// TestAnonymousCallerIsNotTheOwner is the finding. Neither dispatcher may
// substitute the owner for a caller who never authenticated.
func TestAnonymousCallerIsNotTheOwner(t *testing.T) {
	source, err := os.ReadFile("web.go")
	if err != nil {
		t.Fatalf("read web.go: %v", err)
	}
	text := string(source)

	if strings.Contains(text, "effective = owner") {
		t.Error("a dispatcher still substitutes the owner for an anonymous caller: apps are told a stranger is the owner")
	}
	for _, fn := range []string{"web_action", "web_serve_file_with_opengraph"} {
		body := function_source(t, text, fn)
		if !strings.Contains(body, `s.set("user", user)`) {
			t.Errorf("%s does not bind the real caller", fn)
		}
	}
}

// Strict require_permission returns "no user context" before it looks at a
// grant, so any of these left strict turns its public route into a 500.
func TestReachableGatesTolerateAnAnonymousCaller(t *testing.T) {
	sources := map[string]string{}
	for _, name := range []string{"access.go", "accounts.go", "ai.go", "entities.go", "interests.go", "permissions.go"} {
		body, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		sources[name] = string(body)
	}

	for _, fn := range anonymous_reachable {
		found := false
		for name, text := range sources {
			if !strings.Contains(text, "func "+fn+"(") {
				continue
			}
			found = true
			body := function_source(t, text, fn)
			switch {
			case strings.Contains(body, "require_permission(t, fn,"):
				t.Errorf("%s (%s) uses strict require_permission: a public action reaching it gets 500 \"no user context\" instead of an answer", fn, name)
			case !strings.Contains(body, "require_permission_acting(t, fn,"):
				t.Errorf("%s (%s) has no permission check at all", fn, name)
			}
		}
		if !found {
			t.Errorf("%s not found: the audited set has drifted from the code", fn)
		}
	}
}

// TestAnonymousCallerStillReachesTheOwnersData. Removing the substitution must
// not cost a public page its data: principal_storage answers the storage
// question separately, and falls back to the owner when nobody is bound.
func TestAnonymousCallerStillReachesTheOwnersData(t *testing.T) {
	owner := &User{UID: "u-owner", Username: "owner@example.com"}

	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("owner", owner)
	// No "user" local: exactly what a public action now sees anonymously.

	if caller := principal_caller(thread); caller != nil {
		t.Errorf("caller resolved to %v, want nil - anonymous must be representable", caller)
	}
	storage, err := principal_storage(thread)
	if err != nil {
		t.Fatalf("principal_storage refused an anonymous public request: %v", err)
	}
	if storage.UID != owner.UID {
		t.Errorf("reads resolved to %q, want the owner %q - the public page would render empty", storage.UID, owner.UID)
	}
}

// TestAnonymousCallerWithNoOwnerIsRefusedAsAbsentStorage. With neither a user
// nor an owner bound - an anonymous request to a public class-level action that
// arrived by no domain route - the storage question has no answer, and the
// refusal must be the typed storage_absent so web_action_error can answer 401
// instead of logging a fault and answering 500.
func TestAnonymousCallerWithNoOwnerIsRefusedAsAbsentStorage(t *testing.T) {
	thread := &sl.Thread{Name: "test"}

	storage, err := principal_storage(thread)
	if storage != nil {
		t.Fatalf("storage resolved to %q, want nobody - there is no account to operate on", storage.UID)
	}
	if !errors.Is(err, storage_absent) {
		t.Errorf("principal_storage returned %v, want storage_absent - an untyped error maps to a 500", err)
	}
}

// TestActingGatesResolveAgainstStorage confirms what _acting buys: with no
// caller, the grant consulted is the storage account's. A gate that resolved
// the caller instead would refuse, and one that ignored storage would consult
// the wrong account.
func TestActingGatesResolveAgainstStorage(t *testing.T) {
	create_test_routing_env(t)

	owner := &User{UID: "u-owner", Username: "owner@example.com"}
	app := create_external_app("public-app")
	apps[app.id] = app
	t.Cleanup(func() { delete(apps, app.id) })

	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("app", app)
	thread.SetLocal("owner", owner)

	// Ungranted: a refusal naming the permission, NOT "no user context".
	err := require_permission_acting(thread, sl.NewBuiltin("probe", nil), "interests/read")
	if err == nil {
		t.Fatal("an ungranted app was allowed")
	}
	if strings.Contains(err.Error(), "no user context") {
		t.Errorf("refused with %v - the gate never reached the grant lookup, so the public route 500s", err)
	}

	// Granted to the OWNER, and the anonymous call is admitted on it.
	db := db_user(owner, "user")
	db.permissions_setup()
	db.permissions_upsert(app.id, "interests/read", "", 1)
	if err := require_permission_acting(thread, sl.NewBuiltin("probe", nil), "interests/read"); err != nil {
		t.Errorf("the owner's grant did not admit the anonymous call: %v", err)
	}
}

// TestAuthenticatedCallerIsUnaffected. The change is about anonymous requests;
// a caller who authenticated must still be themselves, and must still be the
// account their own permissions are checked against.
func TestAuthenticatedCallerIsUnaffected(t *testing.T) {
	owner := &User{UID: "u-owner", Username: "owner@example.com"}
	caller := &User{UID: "u-caller", Username: "caller@example.com"}

	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("owner", owner)
	thread.SetLocal("user", caller)

	if got := principal_caller(thread); got == nil || got.UID != caller.UID {
		t.Errorf("caller resolved to %v, want %q", got, caller.UID)
	}
	storage, err := principal_storage(thread)
	if err != nil {
		t.Fatalf("principal_storage: %v", err)
	}
	if storage.UID != caller.UID {
		t.Errorf("storage resolved to %q, want the caller %q when no storage is pinned", storage.UID, caller.UID)
	}
}

// function_source returns one Go function's body from a file's text.
func function_source(t *testing.T, text, name string) string {
	t.Helper()
	start := strings.Index(text, "\nfunc "+name+"(")
	if start < 0 {
		t.Fatalf("function %s not found", name)
	}
	rest := text[start+1:]
	end := regexp.MustCompile(`(?m)^\}$`).FindStringIndex(rest)
	if end == nil {
		t.Fatalf("could not find the end of %s", name)
	}
	return rest[:end[1]]
}

// TestURLGateResolvesAgainstStorage. An outbound request from a public action
// is made on the owner's behalf: refused by name when the owner holds no grant
// for the domain, never with "no user context", and admitted on the owner's
// grant with nobody bound as the caller. The comptroller webhook is the live
// case - Stripe's delivery carries no session, and every Stripe call it made
// answered {403, ""} from this gate.
func TestURLGateResolvesAgainstStorage(t *testing.T) {
	create_test_routing_env(t)

	owner := &User{UID: "u-owner", Username: "owner@example.com"}
	app := create_external_app("public-app")
	apps[app.id] = app
	t.Cleanup(func() { delete(apps, app.id) })

	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("app", app)
	thread.SetLocal("owner", owner)
	fn := sl.NewBuiltin("mochi.url.post", nil)
	target := "https://api.stripe.com/v1/payment_intents/pi_1"

	err := require_permission_url(thread, fn, target)
	if err == nil {
		t.Fatal("an ungranted domain was allowed")
	}
	if strings.Contains(err.Error(), "no user context") {
		t.Errorf("refused with %v - the gate never reached the grant lookup, so a public action's outbound call is dead whatever the owner granted", err)
	}
	result, err := api_url_request(thread, fn, sl.Tuple{sl.String(target)}, nil)
	if err != nil {
		t.Fatalf("api_url_request: %v", err)
	}
	expect_url_status(t, result, 403)

	db := db_user(owner, "user")
	db.permissions_setup()
	db.permissions_upsert(app.id, "url", "api.stripe.com", 1)
	if err := require_permission_url(thread, fn, target); err != nil {
		t.Errorf("the owner's url grant did not admit the anonymous call: %v", err)
	}
}

// TestURLIdempotencyCacheResolvesAgainstStorage. The replay cache lives with
// the account the grant was resolved against, so a public action's retried
// call is answered from it rather than sent again. Keyed on the caller, an
// anonymous replay had no cache at all. The host does not resolve, so a call
// that reaches the network answers 0, and only a cache hit answers 200.
func TestURLIdempotencyCacheResolvesAgainstStorage(t *testing.T) {
	create_test_routing_env(t)

	owner := &User{UID: "u-owner", Username: "owner@example.com"}
	app := create_external_app("public-app")
	apps[app.id] = app
	t.Cleanup(func() { delete(apps, app.id) })

	db := db_user(owner, "user")
	db.permissions_setup()
	db.permissions_upsert(app.id, "url", "cache.invalid", 1)
	url_idempotency_store(owner, app, "replay-1", 200, map[string]string{}, []byte(`{"cached":true}`))

	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("app", app)
	thread.SetLocal("owner", owner)
	fn := sl.NewBuiltin("mochi.url.post", nil)
	result, err := api_url_request(thread, fn, sl.Tuple{sl.String("https://cache.invalid/v1/x")},
		[]sl.Tuple{{sl.String("idempotency_key"), sl.String("replay-1")}})
	if err != nil {
		t.Fatalf("api_url_request: %v", err)
	}
	expect_url_status(t, result, 200)
}

// TestScheduleBindsToStorage. A scheduled event created by a public action
// belongs to the storage account, whose database its handler reads when it
// fires. Bound to the caller, a webhook's follow-up check was written with no
// user, and the runner could bind it to no account.
func TestScheduleBindsToStorage(t *testing.T) {
	create_test_routing_env(t)
	db_open("db/schedule.db").exec("create table if not exists schedule ( id integer primary key, user text not null, app text not null, due int not null, event text not null, data text not null, interval int not null, created int not null )")

	owner := &User{UID: "u-owner", Username: "owner@example.com"}
	app := create_external_app("public-app")
	apps[app.id] = app
	t.Cleanup(func() { delete(apps, app.id) })

	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("app", app)
	thread.SetLocal("owner", owner)
	fn := sl.NewBuiltin("mochi.schedule.at", nil)
	if _, err := api_schedule_at(thread, fn, sl.Tuple{sl.String("schedule_probe"), sl.NewDict(0), sl.MakeInt64(now() + 60)}, nil); err != nil {
		t.Fatalf("api_schedule_at: %v", err)
	}
	row, _ := schedule_db().row("select user from schedule where event='schedule_probe'")
	if row == nil {
		t.Fatal("no scheduled event was written")
	}
	if user, _ := row["user"].(string); user != owner.UID {
		t.Errorf("scheduled event bound to %q, want the owner %q - the runner has no account to run it as", user, owner.UID)
	}
}

// anonymous_dispatch_app builds a Starlark app with a database and two public
// class-level actions, one that touches storage and one that only answers:
// what every public `-/...` action looks like to web_action once nobody is
// signed in. The fixture carries an administrator, so that account being
// chosen for the request - the fallback this file's history is about - is
// detectable, and a second account for a domain route to name.
func anonymous_dispatch_app(t *testing.T) *App {
	t.Helper()
	route_owner_env(t)
	route_owner_user(t, "aadmin", "administrator", "active")
	route_owner_identity(t, "aadmin")
	route_owner_user(t, "site", "user", "active")
	route_owner_identity(t, "site")

	// Starlark.call needs the runtime state the server sets up at startup: a
	// concurrency semaphore (a nil channel blocks for ever) and a compute
	// timeout (zero cancels at once).
	if starlark_semaphore == nil {
		starlark_semaphore = make(chan struct{}, 32)
	}
	original := starlark_default_timeout
	starlark_default_timeout = 30 * time.Second
	t.Cleanup(func() { starlark_default_timeout = original })

	star := filepath.Join(t.TempDir(), "app.star")
	source := `
def database_create():
    mochi.db.execute("create table marks (id integer primary key autoincrement)")

def action_touch(a):
    mochi.db.execute("insert into marks default values")
    return mochi.db.row("select count(*) as total from marks")

def action_pure(a):
    return {"pure": True}
`
	if err := os.WriteFile(star, []byte(source), 0o600); err != nil {
		t.Fatalf("writing the test app: %v", err)
	}
	av := &AppVersion{Version: "1.0", Execute: []string{star}, Actions: map[string]AppAction{
		"-/touch": {Function: "action_touch", Public: true},
		"-/pure":  {Function: "action_pure", Public: true},
	}}
	av.Architecture.Engine = "starlark"
	av.Database.File = "anonymous.db"
	av.Database.Schema = 1
	av.Database.Create.Function = "database_create"
	app := &App{id: "anonymoustest", internal: av}
	av.app = app
	if _, found := av.starlark().globals["action_touch"]; !found {
		t.Fatal("the test app did not load; action_touch is not defined")
	}
	t.Cleanup(func() { db_purge_prefix("users") })
	return app
}

// anonymous_dispatch runs one request with no credential through web_action.
// routed is the owner domains_middleware would have published for a domain
// route; empty for a request that arrived by the app's own path.
func anonymous_dispatch(t *testing.T, app *App, action string, routed string) (int, string) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/"+app.id+"/"+action, nil)
	c.Request.Header.Set("Accept", "application/json")
	routing := routing_class
	if routed != "" {
		c.Set("domain_route", "anonymous.example")
		c.Set("domain_owner", routed)
		routing = routing_hosted
	}
	if !web_action(c, app, action, nil, routing) {
		t.Fatalf("web_action did not handle %s", action)
	}
	return w.Code, w.Body.String()
}

// anonymous_database reports whether the app's database exists under uid: the
// trace a request leaves in whichever account it ran against.
func anonymous_database(uid string, app *App) bool {
	path, _ := db_app_locate(&User{UID: uid}, app, app.internal)
	_, err := os.Stat(filepath.Join(data_dir, path))
	return err == nil
}

// TestAnonymousRoutelessStorageActionIsA401 is the contract the unit tests
// above add up to, measured through the dispatcher: an anonymous request to a
// public class-level action that arrived by no route reaches the handler, and
// the handler's first storage call comes back to the caller as 401 - not as
// the administrator's data, and not as a 500.
func TestAnonymousRoutelessStorageActionIsA401(t *testing.T) {
	app := anonymous_dispatch_app(t)

	status, body := anonymous_dispatch(t, app, "-/touch", "")
	if status != 401 || !strings.Contains(body, "authentication_required") {
		t.Errorf("anonymous -/touch answered %d %s, want 401 authentication_required", status, body)
	}
	if anonymous_database("aadmin", app) {
		t.Error("the administrator's database was opened for an anonymous routeless request")
	}
}

// TestAnonymousRoutelessPureActionStillRuns. The refusal is the storage call's,
// not the dispatcher's: a public action that needs no account must keep
// answering anonymous callers, or every public page goes dark with the
// fallback.
func TestAnonymousRoutelessPureActionStillRuns(t *testing.T) {
	app := anonymous_dispatch_app(t)

	status, body := anonymous_dispatch(t, app, "-/pure", "")
	if status != 200 || !strings.Contains(body, `"pure":true`) {
		t.Errorf("anonymous -/pure answered %d %s, want 200 with the handler's answer", status, body)
	}
}

// TestAnonymousRoutedActionRunsAsTheRouteOwner. A domain route naming an owner
// is how an action that serves anonymous callers from one account's data is
// bound to it: the same request, routed, runs against that account and no
// other.
func TestAnonymousRoutedActionRunsAsTheRouteOwner(t *testing.T) {
	app := anonymous_dispatch_app(t)

	status, body := anonymous_dispatch(t, app, "-/touch", "site")
	if status != 200 || !strings.Contains(body, `"total":1`) {
		t.Errorf("routed anonymous -/touch answered %d %s, want 200 with one mark", status, body)
	}
	if !anonymous_database("site", app) {
		t.Error("the route owner's database was not the one written")
	}
	if anonymous_database("aadmin", app) {
		t.Error("the administrator's database was opened for a routed request")
	}
}

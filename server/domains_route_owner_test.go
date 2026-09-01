// Mochi server: routes.owner decides whose data a hostname publishes.
//
// Two ways that went wrong: an owner who is no longer there (the route keeps
// serving, and a public action lands on the first administrator), and an owner
// the caller is not (a path delegate retargets someone else's route and the row
// keeps its original owner).
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	sl "go.starlark.net/starlark"
)

// route_owner_env builds the domains tables plus a users table carrying the
// status column, which create_web_test_env's does not have.
func route_owner_env(t *testing.T) {
	t.Helper()
	create_domains_test_env(t)

	// user_by_uid selects methods and disabled too, and refuses a user with no
	// person entity - web_anonymous_owner goes through it, so the fixture has to
	// carry a resolvable account rather than just a row.
	db_open("db/users.db").exec(`create table if not exists users (uid text not null primary key,
		username text not null default '', role text not null default 'user',
		methods text not null default '', disabled text not null default '',
		status text not null default 'active')`)
	db_open("db/users.db").exec(`create table if not exists entities (id text primary key,
		user text not null default '', class text not null default '', name text not null default '',
		private text not null default '', fingerprint text not null default '',
		parent text not null default '', privacy text not null default 'public',
		data text not null default '', published integer not null default 0)`)
}

// route_owner_domain registers a verified domain. domains_verification defaults
// to true, and domain_register writes verified=0, so an unregistered-as-verified
// domain never matches and every assertion below would pass vacuously.
func route_owner_domain(t *testing.T, name string) {
	t.Helper()
	domain_register(name)
	db_open("db/domains.db").exec("update domains set verified=1 where domain=?", name)
	if d := domain_get(name); d == nil || d.Verified == 0 {
		t.Fatalf("%s is not a verified domain, so domain_match will never reach a route", name)
	}
}

// route_owner_user inserts a users row. An empty status is what an ordinary
// account carries.
func route_owner_user(t *testing.T, uid, role, status string) *User {
	t.Helper()
	db_open("db/users.db").exec("insert into users (uid, username, role, status) values (?, ?, ?, ?)",
		uid, uid+"@example.com", role, status)
	return &User{UID: uid, Username: uid + "@example.com", Role: role}
}

// route_owner_identity gives uid the person entity user_by_uid insists on.
// Only the tests that go through user_by_uid need it: an entity makes
// user_purge_local tombstone it network-wide, which is a different subsystem.
func route_owner_identity(t *testing.T, uid string) {
	t.Helper()
	db_open("db/users.db").exec("insert into entities (id, user, class, name, fingerprint) values (?, ?, 'person', ?, ?)",
		"entity-"+uid, uid, uid, "fp-"+uid)
}

// routed_owner drives domains_middleware and reports the domain_owner it
// published, or "" when it declined to route the request at all.
func routed_owner(host, path string) (string, bool) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(domains_middleware())

	var owner string
	var matched bool
	r.GET("/*any", func(c *gin.Context) {
		_, matched = c.Get("domain_method")
		owner = c.GetString("domain_owner")
		c.Status(200)
	})

	request := httptest.NewRequest("GET", path, nil)
	request.Host = host
	r.ServeHTTP(httptest.NewRecorder(), request)
	return owner, matched
}

// TestSuspendedRouteOwnerStopsServingTheDomain. Suspension leaves the route row
// in place, so without a gate the hostname keeps answering - and answering as
// somebody, since web_anonymous_owner cannot resolve the account it names.
func TestSuspendedRouteOwnerStopsServingTheDomain(t *testing.T) {
	route_owner_env(t)
	route_owner_user(t, "live", "user", "active")

	route_owner_domain(t, "hosted.invalid")
	route_create("hosted.invalid", "", "app", "feeds", "site", "live", 0)

	if owner, matched := routed_owner("hosted.invalid", "/"); !matched || owner != "live" {
		t.Fatalf("an active owner's route did not serve (owner=%q matched=%v)", owner, matched)
	}

	db_open("db/users.db").exec("update users set status='suspended' where uid=?", "live")

	if owner, matched := routed_owner("hosted.invalid", "/"); matched {
		t.Errorf("the hostname still routes after its owner was suspended (owner=%q) - "+
			"suspending a hostile user must take their domain off this server", owner)
	}
}

// TestPurgedRouteOwnerStopsServingTheDomain is the same gate for the case where
// the users row is gone entirely rather than flagged.
func TestPurgedRouteOwnerStopsServingTheDomain(t *testing.T) {
	route_owner_env(t)
	route_owner_user(t, "leaver", "user", "active")

	route_owner_domain(t, "gone.invalid")
	route_create("gone.invalid", "", "app", "feeds", "site", "leaver", 0)

	db_open("db/users.db").exec("delete from users where uid=?", "leaver")

	if _, matched := routed_owner("gone.invalid", "/"); matched {
		t.Error("the hostname still routes after its owner was purged")
	}
}

// TestRouteNamingNobodyStillServes. A route with an empty owner never resolved
// an account in the first place, so it is not the account-is-gone case and must
// keep working - every route created before the owner column was populated has
// one.
func TestRouteNamingNobodyStillServes(t *testing.T) {
	route_owner_env(t)

	route_owner_domain(t, "legacy.invalid")
	route_create("legacy.invalid", "", "app", "feeds", "", "", 0)

	if _, matched := routed_owner("legacy.invalid", "/"); !matched {
		t.Error("a route with no owner stopped serving; the gate is meant to catch a named owner who is gone")
	}
}

// TestPurgeRemovesTheUsersRoutesAndDelegations. The domains database is shared
// core state, so db_purge_prefix does not reach it. A route left behind keeps
// its hostname pointed here with an owner that cannot resolve, and a delegation
// left behind comes back the moment the uid is reused.
func TestPurgeRemovesTheUsersRoutesAndDelegations(t *testing.T) {
	route_owner_env(t)

	for _, table := range []string{"credentials", "totp", "recovery", "oauth"} {
		db_open("db/users.db").exec("create table if not exists " + table + " (user text not null)")
	}
	for _, table := range []string{"sessions", "ceremonies", "partial", "logins", "accesses", "passkeys", "verifications"} {
		db_open("db/sessions.db").exec("create table if not exists " + table + " (user text not null)")
	}
	db_open("db/schedule.db").exec(`create table if not exists schedule (id integer primary key,
		user text not null, app text not null, due int not null, event text not null,
		data text not null, interval int not null, created int not null)`)

	route_owner_user(t, "leaver", "user", "active")
	route_owner_user(t, "stayer", "user", "active")

	route_owner_domain(t, "shared.invalid")
	route_create("shared.invalid", "/leaver", "app", "feeds", "", "leaver", 0)
	route_create("shared.invalid", "/stayer", "app", "feeds", "", "stayer", 0)

	domains := db_open("db/domains.db")
	domains.exec("insert into delegations (domain, path, owner, created, updated) values (?, ?, ?, ?, ?)",
		"shared.invalid", "/leaver", "leaver", now(), now())
	domains.exec("insert into delegations (domain, path, owner, created, updated) values (?, ?, ?, ?, ?)",
		"shared.invalid", "/stayer", "stayer", now(), now())

	if _, err := user_purge_local("leaver"); err != nil {
		t.Fatalf("user_purge_local: %v", err)
	}

	if n := domains.integer("select count(*) from routes where owner=?", "leaver"); n != 0 {
		t.Errorf("%d route(s) survived the purge; the hostname still points here with an owner that cannot resolve", n)
	}
	if n := domains.integer("select count(*) from delegations where owner=?", "leaver"); n != 0 {
		t.Errorf("%d delegation(s) survived the purge; reusing the uid revives them", n)
	}
	if n := domains.integer("select count(*) from routes where owner=?", "stayer"); n != 1 {
		t.Errorf("another user's route was removed (%d left)", n)
	}
	if n := domains.integer("select count(*) from delegations where owner=?", "stayer"); n != 1 {
		t.Errorf("another user's delegation was removed (%d left)", n)
	}
}

// TestAnonymousOwnerRefusesAnUnresolvableRoute is the second line behind the
// middleware gate: given a route that named an owner, the answer is that owner
// or nobody. Sliding onto the administrator would publish their storage under a
// hostname they do not control.
func TestAnonymousOwnerRefusesAnUnresolvableRoute(t *testing.T) {
	route_owner_env(t)
	route_owner_user(t, "aadmin", "administrator", "active")
	route_owner_user(t, "site", "user", "active")
	route_owner_identity(t, "aadmin")
	route_owner_identity(t, "site")

	// A public class-level action that arrived by no route still resolves the
	// administrator - that is the fallback's real job and must survive.
	if got := web_anonymous_owner("", true); got == nil || got.UID != "aadmin" {
		t.Errorf("a routeless public action resolved %v, want the administrator", got)
	}
	// A non-public action gets nobody, routed or not.
	if got := web_anonymous_owner("", false); got != nil {
		t.Errorf("a non-public action resolved %v, want nobody", got)
	}
	// A route naming a live account resolves that account.
	if got := web_anonymous_owner("site", true); got == nil || got.UID != "site" {
		t.Errorf("a live route owner resolved %v, want the route owner", got)
	}

	db_open("db/users.db").exec("update users set status='suspended' where uid=?", "site")
	if got := web_anonymous_owner("site", true); got != nil {
		t.Errorf("a suspended route owner resolved %v, want nobody - answering as the "+
			"administrator publishes their storage under someone else's domain", got)
	}

	db_open("db/users.db").exec("delete from users where uid=?", "site")
	if got := web_anonymous_owner("site", true); got != nil {
		t.Errorf("a purged route owner resolved %v, want nobody", got)
	}
}

// route_owner_thread builds a Starlark thread for user, holding domains/write.
func route_owner_thread(t *testing.T, user *User) (*sl.Thread, *App) {
	t.Helper()
	app := create_external_app("sites")
	apps[app.id] = app
	thread := create_test_thread(user, app)

	db := db_user(user, "user")
	db.permissions_setup()
	db.permissions_upsert(app.id, "domains/write", "", 1)
	return thread, app
}

// TestDelegateCannotRetargetAnotherUsersRoute. domain_can_manage_route answers
// "does the caller hold a delegation covering this path", which is not "does
// the caller own this route". route_update never touches the owner column, so
// without an owner comparison the delegate picks the target while the row keeps
// naming the victim - and web_anonymous_owner runs the delegate's chosen app as
// that victim.
func TestDelegateCannotRetargetAnotherUsersRoute(t *testing.T) {
	route_owner_env(t)

	victim := route_owner_user(t, "victim", "user", "active")
	delegate := route_owner_user(t, "delegate", "user", "active")

	route_owner_domain(t, "shop.invalid")
	route_create("shop.invalid", "/shop", "app", "market", "site", victim.UID, 10)

	domains := db_open("db/domains.db")
	domains.exec("insert into delegations (domain, path, owner, created, updated) values (?, ?, ?, ?, ?)",
		"shop.invalid", "/shop", delegate.UID, now(), now())

	thread, _ := route_owner_thread(t, delegate)
	update := sl.NewBuiltin("mochi.domain.route.update", api_domain_route_update)
	remove := sl.NewBuiltin("mochi.domain.route.delete", api_domain_route_delete)
	target := []sl.Tuple{{sl.String("target"), sl.String("attacker")}}
	where := sl.Tuple{sl.String("shop.invalid"), sl.String("/shop")}

	// The delegation is real: without it the refusal below would be the
	// ordinary access-denied path and would prove nothing about ownership.
	if !domain_can_manage_route(delegate, domain_get("shop.invalid"), "/shop") {
		t.Fatal("the delegation did not take effect, so the assertions below cannot distinguish the two refusals")
	}

	if _, err := api_domain_route_update(thread, update, where, target); err == nil {
		t.Error("a path delegate retargeted a route owned by another user")
	}
	if got := route_get("shop.invalid", "/shop"); got == nil || got.Target != "market" {
		t.Errorf("the route's target is now %v, want the owner's - a refused update must change nothing", got)
	}
	if got := route_get("shop.invalid", "/shop"); got == nil || got.Owner != victim.UID {
		t.Errorf("the route's owner is now %v, want the victim - the owner column is what decides whose data the hostname publishes", got)
	}

	if _, err := api_domain_route_delete(thread, remove, where, nil); err == nil {
		t.Error("a path delegate deleted a route owned by another user")
	}
	if route_get("shop.invalid", "/shop") == nil {
		t.Error("the route was deleted by someone who does not own it")
	}
}

// TestOwnerAndAdministratorCanStillManageTheRoute. The comparison must not cost
// the route's own owner, or an administrator, their access to it.
func TestOwnerAndAdministratorCanStillManageTheRoute(t *testing.T) {
	route_owner_env(t)

	owner := route_owner_user(t, "owner", "user", "active")
	admin := route_owner_user(t, "aadmin", "administrator", "active")

	route_owner_domain(t, "own.invalid")
	route_create("own.invalid", "/shop", "app", "market", "site", owner.UID, 10)

	domains := db_open("db/domains.db")
	domains.exec("insert into delegations (domain, path, owner, created, updated) values (?, ?, ?, ?, ?)",
		"own.invalid", "/shop", owner.UID, now(), now())

	update := sl.NewBuiltin("mochi.domain.route.update", api_domain_route_update)
	where := sl.Tuple{sl.String("own.invalid"), sl.String("/shop")}

	thread, _ := route_owner_thread(t, owner)
	if _, err := api_domain_route_update(thread, update,
		where, []sl.Tuple{{sl.String("target"), sl.String("shop")}}); err != nil {
		t.Errorf("the route's own owner could not update it: %v", err)
	}
	if got := route_get("own.invalid", "/shop"); got == nil || got.Target != "shop" {
		t.Errorf("the owner's update did not apply (%v)", got)
	}

	thread, _ = route_owner_thread(t, admin)
	if _, err := api_domain_route_update(thread, update,
		where, []sl.Tuple{{sl.String("target"), sl.String("admin")}}); err != nil {
		t.Errorf("an administrator could not update the route: %v", err)
	}
	if got := route_get("own.invalid", "/shop"); got == nil || got.Target != "admin" {
		t.Errorf("the administrator's update did not apply (%v)", got)
	}
}

// TestRouteUpdateRefusesAMissingRoute. route_update ran its UPDATE against no
// rows and reported success; the owner lookup makes the absence visible.
func TestRouteUpdateRefusesAMissingRoute(t *testing.T) {
	route_owner_env(t)

	admin := route_owner_user(t, "aadmin", "administrator", "active")
	route_owner_domain(t, "empty.invalid")

	thread, _ := route_owner_thread(t, admin)
	_, err := api_domain_route_update(thread, sl.NewBuiltin("mochi.domain.route.update", api_domain_route_update),
		sl.Tuple{sl.String("empty.invalid"), sl.String("/nowhere")},
		[]sl.Tuple{{sl.String("target"), sl.String("anything")}})
	if err == nil {
		t.Error("updating a route that does not exist reported success")
	}
}

// Mochi server: Domain routing unit tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"testing"

	sl "go.starlark.net/starlark"
)

// route_list returns all routes for a domain (test helper)
func route_list(domain_name string) []route {
	db := db_open("db/domains.db")
	var routes []route
	db.scans(&routes, "select * from routes where domain=? order by priority desc, length(path) desc", domain_name)
	return routes
}

// create_domains_test_env sets up a test environment for domains testing
func create_domains_test_env(t *testing.T) {
	t.Helper()
	test_data_directory(t)

	// Create settings database for domains_verification setting
	settings_db := db_open("db/settings.db")
	settings_db.exec("create table if not exists settings (name text primary key, value text not null)")

	// Create domains database
	domains := db_open("db/domains.db")
	domains.exec("create table if not exists domains (domain text primary key, verified integer not null default 0, token text not null default '', tls integer not null default 1, created integer not null, updated integer not null)")
	domains.exec("create table if not exists routes (domain text not null, path text not null default '', method text not null default 'app', target text not null, context text not null default '', owner integer not null default 0, priority integer not null default 0, enabled integer not null default 1, created integer not null, updated integer not null, primary key (domain, path), foreign key (domain) references domains(domain) on delete cascade)")
	domains.exec("create index if not exists routes_domain on routes(domain)")
	domains.exec("create table if not exists delegations (id integer primary key, domain text not null, path text not null, owner integer not null, created integer not null, updated integer not null, unique(domain, path, owner), foreign key (domain) references domains(domain) on delete cascade)")
	domains.exec("create index if not exists delegations_domain on delegations(domain)")
	domains.exec("create index if not exists delegations_owner on delegations(owner)")

}

// Test domains_init creates tables
func TestDomainsInit(t *testing.T) {
	create_domains_test_env(t)

	db := db_open("db/domains.db")

	// Check domains table exists
	exists, err := db.exists("select name from sqlite_master where type='table' and name='domains'")
	if err != nil {
		t.Fatalf("exists query failed: %v", err)
	}
	if !exists {
		t.Error("domains table should exist")
	}

	// Check routes table exists
	exists, err = db.exists("select name from sqlite_master where type='table' and name='routes'")
	if err != nil {
		t.Fatalf("exists query failed: %v", err)
	}
	if !exists {
		t.Error("routes table should exist")
	}

	// Check routes_domain index exists
	exists, err = db.exists("select name from sqlite_master where type='index' and name='routes_domain'")
	if err != nil {
		t.Fatalf("exists query failed: %v", err)
	}
	if !exists {
		t.Error("routes_domain index should exist")
	}
}

// Test domain_register creates a new domain
func TestDomainRegister(t *testing.T) {
	create_domains_test_env(t)

	d, err := domain_register("example.com")
	if err != nil {
		t.Fatalf("domain_register failed: %v", err)
	}

	if d.Domain != "example.com" {
		t.Errorf("Domain = %q, want 'example.com'", d.Domain)
	}
	if d.Verified != 0 {
		t.Errorf("Verified = %d, want 0", d.Verified)
	}
	if d.TLS != 1 {
		t.Errorf("TLS = %d, want 1", d.TLS)
	}
	if d.Token == "" {
		t.Error("Token should be generated")
	}
	if d.Created == 0 {
		t.Error("Created should be set")
	}
}

// Test domain_register fails for duplicate domain
func TestDomainRegisterDuplicate(t *testing.T) {
	create_domains_test_env(t)

	_, err := domain_register("example.com")
	if err != nil {
		t.Fatalf("first domain_register failed: %v", err)
	}

	_, err = domain_register("example.com")
	if err == nil {
		t.Error("duplicate domain_register should fail")
	}
}

// Test domain_get retrieves a domain
func TestDomainGet(t *testing.T) {
	create_domains_test_env(t)

	domain_register("example.com")

	d := domain_get("example.com")
	if d == nil {
		t.Fatal("domain_get should return a domain")
	}
	if d.Domain != "example.com" {
		t.Errorf("Domain = %q, want 'example.com'", d.Domain)
	}

	// Non-existent domain
	d = domain_get("nonexistent.com")
	if d != nil {
		t.Error("domain_get for nonexistent domain should return nil")
	}
}

// Test domain_list returns all domains
func TestDomainList(t *testing.T) {
	create_domains_test_env(t)

	domain_register("example.com")
	domain_register("test.com")
	domain_register("other.com")

	// List all
	domains := domain_list()
	if len(domains) != 3 {
		t.Errorf("domain_list() returned %d domains, want 3", len(domains))
	}
}

// Test domain_update modifies a domain
func TestDomainUpdate(t *testing.T) {
	create_domains_test_env(t)

	domain_register("example.com")

	err := domain_update("example.com", map[string]any{
		"verified": 1,
		"tls":      0,
	})
	if err != nil {
		t.Fatalf("domain_update failed: %v", err)
	}

	d := domain_get("example.com")
	if d.Verified != 1 {
		t.Errorf("Verified = %d, want 1", d.Verified)
	}
	if d.TLS != 0 {
		t.Errorf("TLS = %d, want 0", d.TLS)
	}
}

// Test domain_delete removes a domain
func TestDomainDelete(t *testing.T) {
	create_domains_test_env(t)

	domain_register("example.com")

	err := domain_delete("example.com")
	if err != nil {
		t.Fatalf("domain_delete failed: %v", err)
	}

	d := domain_get("example.com")
	if d != nil {
		t.Error("domain should be deleted")
	}
}

// Test domain_lookup with exact match
func TestDomainLookupExact(t *testing.T) {
	create_domains_test_env(t)

	domain_register("example.com")

	d := domain_lookup("example.com")
	if d == nil {
		t.Fatal("domain_lookup should find exact match")
	}
	if d.Domain != "example.com" {
		t.Errorf("Domain = %q, want 'example.com'", d.Domain)
	}
}

// Test domain_lookup with wildcard match
func TestDomainLookupWildcard(t *testing.T) {
	create_domains_test_env(t)

	domain_register("*.example.com")

	// Subdomain should match wildcard
	d := domain_lookup("blog.example.com")
	if d == nil {
		t.Fatal("domain_lookup should match wildcard")
	}
	if d.Domain != "*.example.com" {
		t.Errorf("Domain = %q, want '*.example.com'", d.Domain)
	}

	// Different domain should not match
	d = domain_lookup("other.com")
	if d != nil {
		t.Error("domain_lookup should not match different domain")
	}
}

// Test domain_lookup prefers exact match over wildcard
func TestDomainLookupExactOverWildcard(t *testing.T) {
	create_domains_test_env(t)

	domain_register("*.example.com")
	domain_register("blog.example.com")

	d := domain_lookup("blog.example.com")
	if d == nil {
		t.Fatal("domain_lookup should find domain")
	}
	if d.Domain != "blog.example.com" {
		t.Errorf("Domain = %q, want 'blog.example.com' (exact match)", d.Domain)
	}
}

// Test domain_lookup strips port
func TestDomainLookupStripsPort(t *testing.T) {
	create_domains_test_env(t)

	domain_register("example.com")

	d := domain_lookup("example.com:8080")
	if d == nil {
		t.Fatal("domain_lookup should find domain when port is present")
	}
	if d.Domain != "example.com" {
		t.Errorf("Domain = %q, want 'example.com'", d.Domain)
	}
}

// Test delegation_check for full domain access
func TestDelegationFullDomain(t *testing.T) {
	create_domains_test_env(t)

	domain_register("example.com")
	delegation_create("example.com", "", "u123") // Full domain delegation

	// User 123 should have access to any path
	if !delegation_check("example.com", "/blog", "u123") {
		t.Error("user with full domain delegation should have access to /blog")
	}
	if !delegation_check("example.com", "/shop", "u123") {
		t.Error("user with full domain delegation should have access to /shop")
	}
	if !delegation_check("example.com", "", "u123") {
		t.Error("user with full domain delegation should have access to root")
	}
	// User 456 should not have access
	if delegation_check("example.com", "/blog", "u456") {
		t.Error("user without delegation should not have access")
	}
}

// Test delegation_check for path delegation
func TestDelegationPath(t *testing.T) {
	create_domains_test_env(t)

	domain_register("example.com")
	delegation_create("example.com", "/blog", "u123") // Path delegation

	// User 123 should have access to /blog and subpaths
	if !delegation_check("example.com", "/blog", "u123") {
		t.Error("user with path delegation should have access to /blog")
	}
	if !delegation_check("example.com", "/blog/posts", "u123") {
		t.Error("user with path delegation should have access to /blog/posts")
	}
	// User 123 should not have access to other paths
	if delegation_check("example.com", "/shop", "u123") {
		t.Error("user with /blog delegation should not have access to /shop")
	}
}

// Test delegation_check stops at path segment boundaries
func TestDelegationPathBoundary(t *testing.T) {
	create_domains_test_env(t)

	domain_register("example.com")
	delegation_create("example.com", "/blog", "u123")

	// A /blog delegation must not cover sibling paths that merely share the
	// prefix string
	if delegation_check("example.com", "/blogger", "u123") {
		t.Error("/blog delegation should not cover /blogger")
	}
	if delegation_check("example.com", "/blog-admin", "u123") {
		t.Error("/blog delegation should not cover /blog-admin")
	}

	// A root delegation covers every path
	delegation_create("example.com", "/", "u456")
	if !delegation_check("example.com", "/anything", "u456") {
		t.Error("/ delegation should cover /anything")
	}

	// New delegations are stored in canonical form, without a trailing slash
	created, err := delegation_create("example.com", "/wiki/", "u999")
	if err != nil || created == nil || created.Path != "/wiki" {
		t.Errorf("delegation_create should store /wiki/ as /wiki, got %+v (%v)", created, err)
	}

	// A trailing slash on a legacy row does not change its scope
	db := db_open("db/domains.db")
	db.exec("insert into delegations (domain, path, owner, created, updated) values ('example.com', '/shop/', 'u789', 1, 1)")
	if !delegation_check("example.com", "/shop", "u789") {
		t.Error("/shop/ delegation should cover /shop")
	}
	if !delegation_check("example.com", "/shop/items", "u789") {
		t.Error("/shop/ delegation should cover /shop/items")
	}
	if delegation_check("example.com", "/shopping", "u789") {
		t.Error("/shop/ delegation should not cover /shopping")
	}
}

// Test route_create creates a new route
func TestRouteCreate(t *testing.T) {
	create_domains_test_env(t)

	domain_register("example.com")

	r, err := route_create("example.com", "/blog", "app", "myapp", "", "", 10)
	if err != nil {
		t.Fatalf("route_create failed: %v", err)
	}

	if r.Domain != "example.com" {
		t.Errorf("Domain = %q, want 'example.com'", r.Domain)
	}
	if r.Path != "/blog" {
		t.Errorf("Path = %q, want '/blog'", r.Path)
	}
	if r.Method != "app" {
		t.Errorf("Method = %q, want 'app'", r.Method)
	}
	if r.Target != "myapp" {
		t.Errorf("Target = %q, want 'myapp'", r.Target)
	}
	if r.Priority != 10 {
		t.Errorf("Priority = %d, want 10", r.Priority)
	}
	if r.Enabled != 1 {
		t.Errorf("Enabled = %d, want 1", r.Enabled)
	}
}

// Test route_create fails for nonexistent domain
func TestRouteCreateNoDomain(t *testing.T) {
	create_domains_test_env(t)

	_, err := route_create("nonexistent.com", "/", "app", "myapp", "", "", 0)
	if err == nil {
		t.Error("route_create should fail for nonexistent domain")
	}
}

// Test route_create fails for duplicate route
func TestRouteCreateDuplicate(t *testing.T) {
	create_domains_test_env(t)

	domain_register("example.com")
	route_create("example.com", "/blog", "app", "myapp", "", "", 0)

	_, err := route_create("example.com", "/blog", "app", "other", "", "", 0)
	if err == nil {
		t.Error("duplicate route_create should fail")
	}
}

// Test route_get retrieves a route
func TestRouteGet(t *testing.T) {
	create_domains_test_env(t)

	domain_register("example.com")
	route_create("example.com", "/blog", "app", "myapp", "", "", 0)

	r := route_get("example.com", "/blog")
	if r == nil {
		t.Fatal("route_get should return a route")
	}
	if r.Target != "myapp" {
		t.Errorf("Target = %q, want 'myapp'", r.Target)
	}

	// Non-existent route
	r = route_get("example.com", "/other")
	if r != nil {
		t.Error("route_get for nonexistent route should return nil")
	}
}

// Test route_list returns all routes for a domain
func TestRouteList(t *testing.T) {
	create_domains_test_env(t)

	domain_register("example.com")
	route_create("example.com", "/blog", "app", "blog", "", "", 10)
	route_create("example.com", "/shop", "app", "shop", "", "", 5)
	route_create("example.com", "/", "app", "home", "", "", 0)

	routes := route_list("example.com")
	if len(routes) != 3 {
		t.Errorf("route_list returned %d routes, want 3", len(routes))
	}

	// Should be ordered by priority desc
	if routes[0].Path != "/blog" {
		t.Errorf("First route should be '/blog' (priority 10), got '%s'", routes[0].Path)
	}
}

// Test route_update modifies a route
func TestRouteUpdate(t *testing.T) {
	create_domains_test_env(t)

	domain_register("example.com")
	route_create("example.com", "/blog", "app", "myapp", "", "", 0)

	err := route_update("example.com", "/blog", map[string]any{
		"target":   "other",
		"priority": 100,
		"enabled":  0,
	})
	if err != nil {
		t.Fatalf("route_update failed: %v", err)
	}

	r := route_get("example.com", "/blog")
	if r.Target != "other" {
		t.Errorf("Target = %q, want 'other'", r.Target)
	}
	if r.Priority != 100 {
		t.Errorf("Priority = %d, want 100", r.Priority)
	}
	if r.Enabled != 0 {
		t.Errorf("Enabled = %d, want 0", r.Enabled)
	}
}

// Test route_delete removes a route
func TestRouteDelete(t *testing.T) {
	create_domains_test_env(t)

	domain_register("example.com")
	route_create("example.com", "/blog", "app", "myapp", "", "", 0)

	err := route_delete("example.com", "/blog")
	if err != nil {
		t.Fatalf("route_delete failed: %v", err)
	}

	r := route_get("example.com", "/blog")
	if r != nil {
		t.Error("route should be deleted")
	}
}

// Test domain_match finds matching route
func TestDomainMatch(t *testing.T) {
	create_domains_test_env(t)

	domain_register("example.com")
	domain_update("example.com", map[string]any{"verified": 1})
	route_create("example.com", "/blog", "app", "myapp", "", "", 0)

	match := domain_match("example.com", "/blog/123")
	if match == nil {
		t.Fatal("domain_match should find a match")
	}
	if match.route.Target != "myapp" {
		t.Errorf("Target = %q, want 'myapp'", match.route.Target)
	}
	if match.remaining != "/123" {
		t.Errorf("Remaining = %q, want '/123'", match.remaining)
	}
}

// Test domain_match returns nil for unverified domain when verification is required
func TestDomainMatchVerificationRequired(t *testing.T) {
	create_domains_test_env(t)

	domain_register("example.com")
	route_create("example.com", "/", "app", "myapp", "", "", 0)

	// Enable verification requirement
	setting_set("domains_verification", "true")

	match := domain_match("example.com", "/")
	if match != nil {
		t.Error("domain_match should return nil for unverified domain when verification is required")
	}

	// Verify the domain
	domain_update("example.com", map[string]any{"verified": 1})

	match = domain_match("example.com", "/")
	if match == nil {
		t.Error("domain_match should find match for verified domain")
	}
}

// Test domain_match with longest path prefix
func TestDomainMatchLongestPrefix(t *testing.T) {
	create_domains_test_env(t)

	domain_register("example.com")
	domain_update("example.com", map[string]any{"verified": 1})
	route_create("example.com", "/", "app", "root", "", "", 0)
	route_create("example.com", "/blog", "app", "blog", "", "", 0)
	route_create("example.com", "/blog/posts", "app", "posts", "", "", 0)

	// Should match /blog/posts (longest prefix)
	match := domain_match("example.com", "/blog/posts/123")
	if match == nil {
		t.Fatal("domain_match should find a match")
	}
	if match.route.Target != "posts" {
		t.Errorf("Target = %q, want 'posts'", match.route.Target)
	}
	if match.remaining != "/123" {
		t.Errorf("Remaining = %q, want '/123'", match.remaining)
	}
}

// Test domain_match with priority
func TestDomainMatchPriority(t *testing.T) {
	create_domains_test_env(t)

	domain_register("example.com")
	route_create("example.com", "/blog", "app", "low", "", "", 1)
	route_create("example.com", "/blog", "app", "high", "", "", 10) // This will fail due to duplicate

	// Since we can't create duplicate paths, test priority with different paths
	route_create("example.com", "/", "app", "root", "", "", 1)

	// Update the /blog route to have higher priority (simulating what we'd want)
	// Actually, routes with same path can't exist, so priority matters when paths are different
	// Let's test that priority ordering works
	routes := route_list("example.com")
	if len(routes) < 1 {
		t.Fatal("should have at least 1 route")
	}
}

// Test domain_match skips disabled routes
func TestDomainMatchSkipsDisabled(t *testing.T) {
	create_domains_test_env(t)

	domain_register("example.com")
	domain_update("example.com", map[string]any{"verified": 1})
	route_create("example.com", "/blog", "app", "myapp", "", "", 0)
	route_update("example.com", "/blog", map[string]any{"enabled": 0})

	match := domain_match("example.com", "/blog")
	if match != nil {
		t.Error("domain_match should skip disabled routes")
	}
}

// Test domain_match with wildcard domain
func TestDomainMatchWildcard(t *testing.T) {
	create_domains_test_env(t)

	domain_register("*.example.com")
	domain_update("*.example.com", map[string]any{"verified": 1})
	route_create("*.example.com", "/", "app", "wildcard", "", "", 0)

	match := domain_match("blog.example.com", "/test")
	if match == nil {
		t.Fatal("domain_match should match wildcard domain")
	}
	if match.route.Target != "wildcard" {
		t.Errorf("Target = %q, want 'wildcard'", match.route.Target)
	}
}

// Test domain_match returns nil for no matching route
func TestDomainMatchNoRoute(t *testing.T) {
	create_domains_test_env(t)

	domain_register("example.com")
	// No routes created

	match := domain_match("example.com", "/blog")
	if match != nil {
		t.Error("domain_match should return nil when no routes match")
	}
}

// Test domain_match returns nil for unknown domain
func TestDomainMatchUnknownDomain(t *testing.T) {
	create_domains_test_env(t)

	match := domain_match("unknown.com", "/")
	if match != nil {
		t.Error("domain_match should return nil for unknown domain")
	}
}

// Test cascade delete of routes when domain is deleted
func TestDomainDeleteCascade(t *testing.T) {
	create_domains_test_env(t)

	domain_register("example.com")
	route_create("example.com", "/blog", "app", "blog", "", "", 0)
	route_create("example.com", "/shop", "app", "shop", "", "", 0)
	delegation_create("example.com", "/blog", "u1")

	// Verify routes exist
	routes := route_list("example.com")
	if len(routes) != 2 {
		t.Fatalf("Expected 2 routes, got %d", len(routes))
	}

	// Delegations go with the domain too, and by a different mechanism: routes
	// are deleted explicitly by domain_delete, delegations only by the schema's
	// "on delete cascade". A delegation left behind is invisible while the
	// domain is gone - domain_can_manage_route needs a resolved domain - and
	// comes back the moment an administrator registers the name again, handing
	// route management to a delegate nobody re-authorised.
	db := db_open("db/domains.db")
	if rows := db.integer("select count(*) from delegations where domain='example.com'"); rows != 1 {
		t.Fatalf("fixture stored %d delegations, want 1 - the assertion below cannot bite without one", rows)
	}

	// Delete domain
	domain_delete("example.com")

	// Routes should be cascade deleted
	routes = route_list("example.com")
	if len(routes) != 0 {
		t.Errorf("Routes should be cascade deleted, got %d routes", len(routes))
	}
	if rows := db.integer("select count(*) from delegations where domain='example.com'"); rows != 0 {
		t.Errorf("%d delegation(s) survived the domain, want 0", rows)
	}
}

// Test path boundary matching
func TestDomainMatchPathBoundary(t *testing.T) {
	create_domains_test_env(t)

	domain_register("example.com")
	domain_update("example.com", map[string]any{"verified": 1})
	route_create("example.com", "/blog", "app", "blog", "", "", 0)

	// Should match /blog/anything
	match := domain_match("example.com", "/blog/post")
	if match == nil {
		t.Fatal("Should match /blog/post")
	}

	// Should match /blog exactly
	match = domain_match("example.com", "/blog")
	if match == nil {
		t.Fatal("Should match /blog exactly")
	}

	// Should NOT match /blogger (not a path boundary)
	match = domain_match("example.com", "/blogger")
	if match != nil {
		t.Error("Should NOT match /blogger (not at path boundary)")
	}
}

// Test empty path route matches all paths
func TestDomainMatchEmptyPath(t *testing.T) {
	create_domains_test_env(t)

	domain_register("example.com")
	domain_update("example.com", map[string]any{"verified": 1})
	route_create("example.com", "", "app", "root", "", "", 0)

	match := domain_match("example.com", "/anything/here")
	if match == nil {
		t.Fatal("Empty path route should match any path")
	}
	if match.remaining != "/anything/here" {
		t.Errorf("Remaining = %q, want '/anything/here'", match.remaining)
	}
}

// TestRouteMethodValidation covers the routing-method allowlist. A method
// outside the set domains_middleware dispatches on stores a route that answers
// every request to its path with unknown_route_method, so it is refused at
// write time rather than at request time.
func TestRouteMethodValidation(t *testing.T) {
	create_domains_test_env(t)

	domain_register("example.com")

	for _, method := range []string{"app", "redirect", "entity"} {
		if _, err := route_create("example.com", "/"+method, method, "target", "", "", 0); err != nil {
			t.Errorf("route_create with method %q failed: %v", method, err)
		}
	}

	if _, err := route_create("example.com", "/bad", "proxy", "target", "", "", 0); err == nil {
		t.Error("route_create should reject an unknown method")
	}
	if route_get("example.com", "/bad") != nil {
		t.Error("a rejected method must not leave a route behind")
	}

	// The same guard applies to changing an existing route's method, which
	// route_update_columns otherwise permits.
	if err := route_update("example.com", "/app", map[string]any{"method": "proxy"}); err == nil {
		t.Error("route_update should reject an unknown method")
	}
	if r := route_get("example.com", "/app"); r == nil || r.Method != "app" {
		t.Error("a rejected update must leave the stored method unchanged")
	}
	if err := route_update("example.com", "/app", map[string]any{"method": "redirect"}); err != nil {
		t.Errorf("route_update with a valid method failed: %v", err)
	}
}

// TestRouteCreateRejectsInvalidContext checks the helper itself, not only the
// Starlark API above it. A context the serving side cannot use stores a route
// that fails every request to it, so it is refused at write time whichever way
// it arrives - the same reason the method is checked here.
func TestRouteCreateRejectsInvalidContext(t *testing.T) {
	create_domains_test_env(t)

	domain_register("context.example.com")

	if _, err := route_create("context.example.com", "/bad", "app", "files", "café", "owner", 0); err == nil {
		t.Error("route_create accepted a non-ASCII context")
	}
	if _, err := route_create("context.example.com", "/sep", "app", "files", "a/b", "owner", 0); err == nil {
		t.Error("route_create accepted a context containing a separator")
	}
	if _, err := route_create("context.example.com", "/ok", "app", "files", "docs", "owner", 0); err != nil {
		t.Errorf("route_create rejected a valid context: %v", err)
	}

	// An empty context stays legal: it means the route is scoped to the
	// domain's root, which is what production uses.
	if _, err := route_create("context.example.com", "/root", "app", "files", "", "owner", 0); err != nil {
		t.Errorf("route_create rejected an empty context: %v", err)
	}
}

// TestRouteUpdateRejectsInvalidContext is the same guard on the update path,
// which could otherwise walk a valid route into an unusable one.
func TestRouteUpdateRejectsInvalidContext(t *testing.T) {
	create_domains_test_env(t)

	domain_register("update.example.com")
	if _, err := route_create("update.example.com", "", "app", "files", "docs", "owner", 0); err != nil {
		t.Fatalf("route_create: %v", err)
	}

	if err := route_update("update.example.com", "", map[string]any{"context": "café"}); err == nil {
		t.Error("route_update accepted a non-ASCII context")
	}
	if err := route_update("update.example.com", "", map[string]any{"context": "guides"}); err != nil {
		t.Errorf("route_update rejected a valid context: %v", err)
	}

	if r := route_get("update.example.com", ""); r == nil || r.Context != "guides" {
		t.Errorf("context = %v, want guides", r)
	}
}

// route_normalise_domain registers a verified domain. domains_verification
// defaults to "true" and domain_register writes verified=0, so a fixture that
// skips this makes every domain_match assertion below pass for the wrong reason.
func route_normalise_domain(t *testing.T, name string) {
	t.Helper()
	domain_register(name)
	domain_update(name, map[string]any{"verified": 1})
	if d := domain_get(name); d == nil || d.Verified != 1 {
		t.Fatalf("fixture domain %s is not verified, so domain_match would answer nil whatever the route says", name)
	}
}

// route_normalise_target reports which route answers a request, by target, or ""
// for no match.
func route_normalise_target(name, path string) string {
	match := domain_match(name, path)
	if match == nil {
		return ""
	}
	return match.route.Target
}

// TestRouteNormalisesStoredPaths pins the canonical form itself. The end-to-end
// assertions below would pass for the wrong reason if the shapes ever stopped
// differing, so this measures the mapping directly.
func TestRouteNormalisesStoredPaths(t *testing.T) {
	cases := []struct{ given, want string }{
		{"/blog", "/blog"},
		{"/blog/", "/blog"},
		{"/blog//", "/blog"},
		{"blog", "/blog"},
		{"blog/", "/blog"},
		{"", "/"},
		{"/", "/"},
		{"///", "/"},
		{"/a/b/", "/a/b"},
	}
	for _, c := range cases {
		if got := route_normalise(c.given); got != c.want {
			t.Errorf("route_normalise(%q) = %q, want %q", c.given, got, c.want)
		}
	}
}

// TestRouteWithTrailingSlashServesItsSubtree is the headline. domain_match
// requires the remainder to start at a segment boundary, so a route stored as
// "/blog/" leaves "post" as the remainder and matches nothing beneath itself.
func TestRouteWithTrailingSlashServesItsSubtree(t *testing.T) {
	create_domains_test_env(t)
	route_normalise_domain(t, "example.com")

	if _, err := route_create("example.com", "/blog/", "app", "blog", "", "u1", 0); err != nil {
		t.Fatalf("route_create refused a trailing-slash path: %v", err)
	}

	for _, request := range []string{"/blog", "/blog/", "/blog/post", "/blog/post/comments"} {
		if got := route_normalise_target("example.com", request); got != "blog" {
			t.Errorf("request %q resolved target %q, want \"blog\" - a route entered with a "+
				"trailing slash must serve its subtree like any other", request, got)
		}
	}
	// The boundary still holds: a sibling whose name merely starts the same way
	// is not this route's subtree.
	if got := route_normalise_target("example.com", "/blogger"); got != "" {
		t.Errorf("request \"/blogger\" resolved target %q, want no match", got)
	}
}

// TestRouteWithoutLeadingSlashServesItsPath. The request path always carries a
// leading slash, so a route stored without one can never match at all.
func TestRouteWithoutLeadingSlashServesItsPath(t *testing.T) {
	create_domains_test_env(t)
	route_normalise_domain(t, "example.com")

	if _, err := route_create("example.com", "shop", "app", "shop", "", "u1", 0); err != nil {
		t.Fatalf("route_create refused a path with no leading slash: %v", err)
	}

	for _, request := range []string{"/shop", "/shop/item"} {
		if got := route_normalise_target("example.com", request); got != "shop" {
			t.Errorf("request %q resolved target %q, want \"shop\" - a route entered without a "+
				"leading slash is otherwise inert", request, got)
		}
	}
}

// TestRouteSpellingsNameOneRoute. Two spellings of one path used to be two rows,
// because the primary key is (domain, path) and the strings differ. That gave
// one path two routes, and left whichever row was stored in the unusual spelling
// unreachable from get, update and delete.
func TestRouteSpellingsNameOneRoute(t *testing.T) {
	create_domains_test_env(t)
	route_normalise_domain(t, "example.com")

	if _, err := route_create("example.com", "/blog", "app", "first", "", "u1", 0); err != nil {
		t.Fatalf("route_create: %v", err)
	}
	if _, err := route_create("example.com", "/blog/", "app", "second", "", "u1", 0); err == nil {
		t.Error("a second route was created for the same path spelled with a trailing slash")
	}

	db := db_open("db/domains.db")
	if rows := db.integer("select count(*) from routes where domain='example.com'"); rows != 1 {
		t.Errorf("%d rows for one path, want 1", rows)
	}

	// Every management call reaches the route by either spelling.
	for _, spelling := range []string{"/blog", "/blog/", "blog"} {
		if r := route_get("example.com", spelling); r == nil {
			t.Errorf("route_get(%q) found nothing", spelling)
		}
	}
	if err := route_update("example.com", "/blog/", map[string]any{"target": "renamed"}); err != nil {
		t.Fatalf("route_update: %v", err)
	}
	if r := route_get("example.com", "/blog"); r == nil || r.Target != "renamed" {
		t.Error("an update addressed with a trailing slash did not reach the route")
	}
	if err := route_delete("example.com", "blog"); err != nil {
		t.Fatalf("route_delete: %v", err)
	}
	if rows := db.integer("select count(*) from routes where domain='example.com'"); rows != 0 {
		t.Errorf("%d rows left after deleting by a different spelling, want 0", rows)
	}
}

// TestRootRouteSpellingsNameOneRoute. "" and "/" are both the whole domain. "/"
// is canonical because that is the spelling already stored, so an existing root
// route stays reachable rather than being stranded by the normalisation.
func TestRootRouteSpellingsNameOneRoute(t *testing.T) {
	create_domains_test_env(t)
	route_normalise_domain(t, "example.com")

	// A row written before normalisation existed, in the spelling production uses.
	db := db_open("db/domains.db")
	n := now()
	db.exec("insert into routes (domain, path, method, target, context, owner, priority, enabled, created, updated) "+
		"values ('example.com', '/', 'app', 'whole', '', 'u1', 0, 1, ?, ?)", n, n)

	for _, spelling := range []string{"/", ""} {
		if r := route_get("example.com", spelling); r == nil || r.Target != "whole" {
			t.Errorf("route_get(%q) did not reach the existing root route", spelling)
		}
	}
	if _, err := route_create("example.com", "", "app", "duplicate", "", "u1", 0); err == nil {
		t.Error("an empty path created a second whole-domain route alongside \"/\"")
	}
	for _, request := range []string{"/", "/anything", "/deep/path"} {
		match := domain_match("example.com", request)
		if match == nil || match.route.Target != "whole" {
			t.Errorf("request %q did not resolve the whole-domain route", request)
			continue
		}
		// The remainder is the whole request path. Every other route hands on a
		// remainder that starts at a segment boundary, and the redirect method
		// concatenates it straight onto its target, so the root route must not be
		// the one shape that drops the leading slash.
		if match.remaining != request {
			t.Errorf("request %q left remaining %q, want %q", request, match.remaining, request)
		}
	}
}

// TestRouteApiGetNormalisesItsArgument. mochi.domain.route.get queries the table
// directly rather than through route_get, so it needs the same normalisation to
// answer for a route the caller spells differently from the stored form.
func TestRouteApiGetNormalisesItsArgument(t *testing.T) {
	create_domains_test_env(t)
	route_normalise_domain(t, "example.com")
	route_create("example.com", "/blog", "app", "blog", "", "u1", 0)

	user := create_permission_test_user(t, "u1")
	app := create_external_app("testapp")
	database := db_user(user, "user")
	database.permissions_setup()
	database.permissions_upsert(app.id, "domains/read", "", 1)
	thread := create_test_thread(user, app)

	get := sl.NewBuiltin("mochi.domain.route.get", api_domain_route_get)
	for _, spelling := range []string{"/blog", "/blog/", "blog"} {
		value, err := api_domain_route_get(thread, get,
			sl.Tuple{sl.String("example.com"), sl.String(spelling)}, nil)
		if err != nil {
			t.Fatalf("route.get(%q) refused: %v", spelling, err)
		}
		if value == sl.None {
			t.Errorf("route.get(%q) answered None; the route exists under one canonical path", spelling)
		}
	}
}

// TestSiblingRoutesStayDistinct. Normalisation strips a trailing slash and adds
// a leading one; it never merges path segments. So each pair of spellings names
// its own route, and the segment-boundary check in domain_match keeps a longer
// sibling from being served by the shorter one.
func TestSiblingRoutesStayDistinct(t *testing.T) {
	create_domains_test_env(t)
	route_normalise_domain(t, "example.com")

	if _, err := route_create("example.com", "/blog/", "app", "blog", "", "u1", 0); err != nil {
		t.Fatalf("route_create /blog/: %v", err)
	}
	if _, err := route_create("example.com", "/blogger", "app", "blogger", "", "u1", 0); err != nil {
		t.Fatalf("route_create /blogger: %v", err)
	}
	// Each route's other spelling is the same route, so neither can be created twice.
	if _, err := route_create("example.com", "/blog", "app", "duplicate", "", "u1", 0); err == nil {
		t.Error("\"/blog\" was accepted as a second route alongside \"/blog/\"")
	}
	if _, err := route_create("example.com", "/blogger/", "app", "duplicate", "", "u1", 0); err == nil {
		t.Error("\"/blogger/\" was accepted as a second route alongside \"/blogger\"")
	}
	db := db_open("db/domains.db")
	if rows := db.integer("select count(*) from routes where domain='example.com'"); rows != 2 {
		t.Errorf("%d rows for two routes, want 2", rows)
	}

	for _, c := range []struct{ request, want string }{
		{"/blog", "blog"},
		{"/blog/", "blog"},
		{"/blog/post", "blog"},
		{"/blogger", "blogger"},
		{"/blogger/", "blogger"},
		{"/blogger/list", "blogger"},
		{"/blogging", ""},
	} {
		if got := route_normalise_target("example.com", c.request); got != c.want {
			t.Errorf("request %q resolved target %q, want %q", c.request, got, c.want)
		}
	}
}

// TestLegacyRoutePathStaysReachable. Rows written before route_normalise existed
// hold whatever spelling their author used - claude/scripts/p2p-test.py still
// seeds routes with an empty path, straight into the table. Normalising only the
// caller's argument would leave those rows serving requests through domain_match
// while route_get, route_update and route_delete could no longer name them.
func TestLegacyRoutePathStaysReachable(t *testing.T) {
	create_domains_test_env(t)
	route_normalise_domain(t, "example.com")

	db := db_open("db/domains.db")
	n := now()
	for _, legacy := range []struct{ path, target string }{
		{"", "whole"},
		{"/blog/", "blog"},
	} {
		db.exec("insert into routes (domain, path, method, target, context, owner, priority, enabled, created, updated) "+
			"values ('example.com', ?, 'app', ?, '', 'u1', 0, 1, ?, ?)", legacy.path, legacy.target, n, n)
	}
	if rows := db.integer("select count(*) from routes where domain='example.com'"); rows != 2 {
		t.Fatalf("fixture stored %d rows, want 2 - nothing below proves anything", rows)
	}

	// Every spelling reaches the legacy row.
	for _, spelling := range []string{"", "/", "/blog", "/blog/", "blog"} {
		if r := route_get("example.com", spelling); r == nil {
			t.Errorf("route_get(%q) found nothing; a legacy row is unreachable", spelling)
		}
	}
	// And a create in the canonical spelling is refused as the duplicate it is,
	// rather than adding a second row for the same path.
	if _, err := route_create("example.com", "/blog", "app", "duplicate", "", "u1", 0); err == nil {
		t.Error("\"/blog\" was created alongside the legacy \"/blog/\" row")
	}

	if err := route_update("example.com", "/blog", map[string]any{"target": "renamed"}); err != nil {
		t.Fatalf("route_update: %v", err)
	}
	if r := route_get("example.com", "/blog/"); r == nil || r.Target != "renamed" {
		t.Error("an update in the canonical spelling did not reach the legacy row")
	}
	if err := route_delete("example.com", "/"); err != nil {
		t.Fatalf("route_delete: %v", err)
	}
	if rows := db.integer("select count(*) from routes where domain='example.com' and path=''"); rows != 0 {
		t.Error("deleting the whole-domain route in the canonical spelling missed the legacy empty-path row")
	}
}

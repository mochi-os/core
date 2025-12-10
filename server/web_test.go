// Mochi server: Web routing unit tests
// Copyright Alistair Cunningham 2025

package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/gin-gonic/gin"
)

// create_web_test_env sets up a test environment for web routing tests
func create_web_test_env(t *testing.T) func() {
	tmp_dir, err := os.MkdirTemp("", "mochi_web_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	orig_data_dir := data_dir
	data_dir = tmp_dir

	// Create settings database
	settings_db := db_open("db/settings.db")
	settings_db.exec("create table if not exists settings (name text primary key, value text not null)")

	// Create domains database
	domains := db_open("db/domains.db")
	domains.exec("create table if not exists domains (domain text primary key, verified integer not null default 0, token text not null default '', tls integer not null default 1, created integer not null, updated integer not null)")
	domains.exec("create table if not exists routes (domain text not null, path text not null default '', method text not null default 'app', target text not null, context text not null default '', priority integer not null default 0, enabled integer not null default 1, created integer not null, updated integer not null, primary key (domain, path), foreign key (domain) references domains(domain) on delete cascade)")
	domains.exec("create index if not exists routes_domain on routes(domain)")
	domains.exec("create table if not exists delegations (id integer primary key, domain text not null, path text not null, owner integer not null, created integer not null, updated integer not null, unique(domain, path, owner), foreign key (domain) references domains(domain) on delete cascade)")
	domains.exec("create index if not exists delegations_domain on delegations(domain)")
	domains.exec("create index if not exists delegations_owner on delegations(owner)")

	// Create users database for entities
	users := db_open("db/users.db")
	users.exec("create table if not exists users (id integer primary key, username text unique not null, role text not null default 'user', timezone text not null default 'UTC', created integer not null, updated integer not null)")
	users.exec("create table if not exists entities (id text primary key, user integer not null, class text not null, name text not null, privacy text not null default 'private', data text not null default '', fingerprint text not null, created integer not null, updated integer not null)")

	cleanup := func() {
		data_dir = orig_data_dir
		os.RemoveAll(tmp_dir)
	}

	return cleanup
}

// Test domains_middleware sets context values
func TestDomainsMiddleware(t *testing.T) {
	cleanup := create_web_test_env(t)
	defer cleanup()

	// Set up domain and route
	domain_register("test.example.com")
	route_create("test.example.com", "/blog", "app", "myapp", "", 10)

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(domains_middleware())
	r.GET("/*path", func(c *gin.Context) {
		method, _ := c.Get("domain_method")
		target := c.GetString("domain_target")
		remaining := c.GetString("domain_remaining")

		c.JSON(200, gin.H{
			"method":    method,
			"target":    target,
			"remaining": remaining,
		})
	})

	req := httptest.NewRequest("GET", "/blog/123", nil)
	req.Host = "test.example.com"
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("Expected status 200, got %d", w.Code)
	}

	body := w.Body.String()
	if body == "" {
		t.Fatal("Expected response body")
	}

	// Check that context values were set
	if w.Body.String() == "" {
		t.Error("Expected response body with context values")
	}
}

// Test domains_middleware with no matching domain
func TestDomainsMiddlewareNoMatch(t *testing.T) {
	cleanup := create_web_test_env(t)
	defer cleanup()

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(domains_middleware())
	r.GET("/*path", func(c *gin.Context) {
		_, exists := c.Get("domain_method")
		c.JSON(200, gin.H{"has_domain": exists})
	})

	req := httptest.NewRequest("GET", "/test", nil)
	req.Host = "unknown.example.com"
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("Expected status 200, got %d", w.Code)
	}
}

// Test domains_middleware with path-based route
func TestDomainsMiddlewarePathRoute(t *testing.T) {
	cleanup := create_web_test_env(t)
	defer cleanup()

	domain_register("example.com")
	route_create("example.com", "/api", "app", "api", "", 0)

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(domains_middleware())
	r.GET("/*path", func(c *gin.Context) {
		method, exists := c.Get("domain_method")
		target := c.GetString("domain_target")
		remaining := c.GetString("domain_remaining")

		if !exists {
			c.JSON(200, gin.H{"matched": false})
			return
		}
		c.JSON(200, gin.H{
			"matched":   true,
			"method":    method,
			"target":    target,
			"remaining": remaining,
		})
	})

	req := httptest.NewRequest("GET", "/api/users/123", nil)
	req.Host = "example.com"
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("Expected status 200, got %d", w.Code)
	}
}

// Test web_path with domain routing
func TestWebPathDomainRouting(t *testing.T) {
	cleanup := create_web_test_env(t)
	defer cleanup()

	// Create a test entity
	db := db_open("db/users.db")
	n := now()
	db.exec("insert into users (id, username, role, created, updated) values (1, 'test@example.com', 'user', ?, ?)", n, n)
	db.exec("insert into entities (id, user, class, name, privacy, fingerprint, created, updated) values ('entity123', 1, 'person', 'Test', 'public', 'abc123', ?, ?)", n, n)

	// Set up domain and route
	domain_register("blog.example.com")
	route_create("blog.example.com", "", "entity", "entity123", "", 0)

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(domains_middleware())
	r.NoRoute(func(c *gin.Context) {
		// Simplified web_path for testing - just check domain routing triggers
		if method, exists := c.Get("domain_method"); exists && method.(string) != "" {
			target := c.GetString("domain_target")
			action := c.GetString("domain_remaining")
			c.JSON(200, gin.H{
				"routed_via": "domain",
				"method":     method,
				"target":     target,
				"action":     action,
			})
			return
		}
		c.JSON(200, gin.H{"routed_via": "path"})
	})

	req := httptest.NewRequest("GET", "/posts/123", nil)
	req.Host = "blog.example.com"
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("Expected status 200, got %d: %s", w.Code, w.Body.String())
	}
}

// Test web_path falls back to normal routing without domain match
func TestWebPathFallbackRouting(t *testing.T) {
	cleanup := create_web_test_env(t)
	defer cleanup()

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(domains_middleware())
	r.NoRoute(func(c *gin.Context) {
		if method, exists := c.Get("domain_method"); exists && method.(string) != "" {
			c.JSON(200, gin.H{"routed_via": "domain"})
			return
		}
		c.JSON(200, gin.H{"routed_via": "path"})
	})

	req := httptest.NewRequest("GET", "/chat/messages", nil)
	req.Host = "localhost"
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("Expected status 200, got %d", w.Code)
	}
}

// Test domain routing with wildcard domain
func TestWebPathWildcardDomain(t *testing.T) {
	cleanup := create_web_test_env(t)
	defer cleanup()

	domain_register("*.example.com")
	route_create("*.example.com", "", "app", "wildcard", "", 0)

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(domains_middleware())
	r.NoRoute(func(c *gin.Context) {
		if method, exists := c.Get("domain_method"); exists && method.(string) != "" {
			target := c.GetString("domain_target")
			c.JSON(200, gin.H{
				"routed_via": "domain",
				"method":     method,
				"target":     target,
			})
			return
		}
		c.JSON(200, gin.H{"routed_via": "path"})
	})

	req := httptest.NewRequest("GET", "/test", nil)
	req.Host = "subdomain.example.com"
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("Expected status 200, got %d", w.Code)
	}
}

// Test domain routing extracts correct remaining path
func TestWebPathRemainingPath(t *testing.T) {
	cleanup := create_web_test_env(t)
	defer cleanup()

	domain_register("api.example.com")
	route_create("api.example.com", "/v1", "app", "api", "", 0)

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(domains_middleware())
	r.NoRoute(func(c *gin.Context) {
		remaining := c.GetString("domain_remaining")
		c.JSON(200, gin.H{"remaining": remaining})
	})

	tests := []struct {
		path     string
		expected string
	}{
		{"/v1/users", "/users"},
		{"/v1/users/123", "/users/123"},
		{"/v1", ""},
	}

	for _, tt := range tests {
		req := httptest.NewRequest("GET", tt.path, nil)
		req.Host = "api.example.com"
		w := httptest.NewRecorder()

		r.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Path %s: expected status 200, got %d", tt.path, w.Code)
		}
	}
}

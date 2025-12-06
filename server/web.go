// Mochi server: Sample web interface
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/gin-gonic/autotls"
	"github.com/gin-gonic/gin"
	sl "go.starlark.net/starlark"
)

var (
	match_react = regexp.MustCompile(`assets/.*-[\w-]{8}.js$`)
	web_https   = false
)

// Call a web action
func web_action(c *gin.Context, a *App, name string, e *Entity) bool {
	if a == nil || a.active == nil {
		return false
	}
	debug("Web app %q action %q", a.id, name)

	aa := a.active.find_action(name)
	if aa == nil {
		debug("No action found for app %q action %q", a.id, name)
		return false
	}

	// Get user authentication via cookie
	user := web_auth(c)

	// If no cookie auth, try Bearer token authentication
	if user == nil {
		auth_header := c.GetHeader("Authorization")
		if strings.HasPrefix(auth_header, "Bearer ") {
			token := strings.TrimPrefix(auth_header, "Bearer ")
			if uid, err := jwt_verify(token); err == nil && uid > 0 {
				if u := user_by_id(uid); u != nil {
					user = u
					debug("API JWT token accepted for user %d", u.ID)
				}
			}
		}
	}

	// Compute owner based on entity, if present
	var owner *User = user
	if e != nil {
		if o := user_owning_entity(e.ID); o != nil {
			owner = o
		}
	}

	// Require authentication for non-public actions
	if user == nil && !aa.Public {
		// For browser requests, redirect to login
		if strings.Contains(c.GetHeader("Accept"), "text/html") {
			c.Redirect(http.StatusFound, "/login")
			return true
		}
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Authentication required"})
		return true
	}

	// Serve attachment
	if aa.Attachments {
		return web_serve_attachment(c, a, user, name)
	}

	// Serve static file
	if aa.File != "" {
		file := a.active.base + "/" + aa.File
		debug("Serving single file for app %q: %q", a.id, file)
		web_cache_static(c, file)
		c.File(file)
		return true
	}

	// Serve static files from a directory
	if aa.Files != "" {
		parts := strings.SplitN(name, "/", 2)
		if len(parts) == 2 {
			if !valid(parts[1], "filepath") {
				c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid file"})
				return true
			}
			file := a.active.base + "/" + aa.Files + "/" + parts[1]
			debug("Serving file from directory for app %q: %q", a.id, file)
			web_cache_static(c, file)
			c.File(file)
		} else {
			c.JSON(http.StatusBadRequest, gin.H{"error": "No file specified"})
		}
		return true
	}

	// Require authentication for database-backed apps
	if a.active.Database.File != "" && user == nil && owner == nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Authentication required for database access"})
		return true
	}

	// Set up database connections if needed
	if a.active.Database.File != "" {
		if user != nil {
			user.db = db_app(user, a.active)
			if user.db == nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
				return true
			}
			defer user.db.close()
		}

		if owner != nil && (user == nil || owner.ID != user.ID) {
			owner.db = db_app(owner, a.active)
			if owner.db == nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
				return true
			}
			defer owner.db.close()
		}
	}

	// Check access
	if aa.Access.Resource != "" && owner != nil && owner.db != nil && !owner.db.access_check_operation(user, aa) {
		c.JSON(http.StatusForbidden, gin.H{"error": "Access denied"})
		return true
	}

	// Create action
	action := Action{
		id:    action_id(),
		user:  user,
		owner: owner,
		domain: &DomainInfo{
			route: &DomainRouteInfo{
				context:   c.GetString("domain_context"),
				remainder: name,
			},
		},
		app:    a,
		web:    c,
		inputs: make(map[string]string),
	}

	for k, v := range aa.parameters {
		action.inputs[k] = v
	}

	// Parse JSON body and convert to strings for a.input()
	if strings.HasPrefix(c.Request.Header.Get("Content-Type"), "application/json") {
		var data map[string]any
		err := c.ShouldBindJSON(&data)
		if err == nil {
			for key, value := range data {
				action.inputs[key] = any_to_string(value)
			}
		}
	}

	// Check which engine the app uses, and run it
	switch a.active.Architecture.Engine {
	case "": // Internal app
		if aa.internal_function == nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Action has no function"})
			return true
		}

		aa.internal_function(&action)
		c.JSON(http.StatusOK, nil)

	case "starlark":
		if aa.Function == "" {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Action has no function"})
			return true
		}

		// Call Starlark function
		s := a.active.starlark()
		s.set("action", &action)
		s.set("app", a)
		s.set("user", user)
		s.set("owner", owner)

		result, err := s.call(aa.Function, sl.Tuple{&action})
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return true
		}
		if result != sl.None {
			c.JSON(http.StatusOK, sl_decode(result))
		}

	default:
		info("Action unknown engine %q version %q", a.active.Architecture.Engine, a.active.Architecture.Version)
	}

	return true
}

// Get user for login cookie
func web_auth(c *gin.Context) *User {
	return user_by_login(web_cookie_get(c, "login", ""))
}

// Ask browser to cache static files
func web_cache_static(c *gin.Context, path string) {
	if match_react.MatchString(path) {
		debug("Web asking browser to long term cache %q", path)
		c.Header("Cache-Control", "public, max-age=31536000, immutable")
	} else {
		debug("Web asking browser to short term cache %q", path)
		c.Header("Cache-Control", "public, max-age=300")
	}
}

// Get the value of a cookie
func web_cookie_get(c *gin.Context, name string, def string) string {
	value, err := c.Cookie(name)
	if err != nil {
		return def
	}
	return value
}

// Set a cookie
func web_cookie_set(c *gin.Context, name string, value string) {
	secure := web_https && !web_is_localhost(c)
	c.SetSameSite(http.SameSiteLaxMode)
	c.SetCookie(name, value, 365*86400, "/", "", secure, true)
}

// Check if request is from localhost
func web_is_localhost(c *gin.Context) bool {
	ip := c.ClientIP()
	return ip == "127.0.0.1" || ip == "::1" || ip == "localhost"
}

// Unset a cookie
func web_cookie_unset(c *gin.Context, name string) {
	secure := web_https && !web_is_localhost(c)
	c.SetSameSite(http.SameSiteLaxMode)
	c.SetCookie(name, "", -1, "/", "", secure, true)
}

// Security headers middleware
func web_security_headers(c *gin.Context) {
	c.Header("X-Content-Type-Options", "nosniff")
	c.Header("X-Frame-Options", "DENY")
	c.Header("Referrer-Policy", "strict-origin-when-cross-origin")
	c.Next()
}

// Handle login: request code via email (POST with JSON)
func web_login_code(c *gin.Context) {
	var input struct {
		Email string `json:"email"`
	}
	if err := c.ShouldBindJSON(&input); err != nil || input.Email == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	reason := code_send(input.Email)
	if reason != "" {
		switch reason {
		case "signup_disabled":
			c.JSON(http.StatusForbidden, gin.H{"error": "signup_disabled", "message": "New user signup is disabled."})
		default:
			c.JSON(http.StatusBadRequest, gin.H{"error": "Unable to send login email"})
		}
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// Create an identity for a new user
func web_login_identity(c *gin.Context) {
	u := web_auth(c)

	// If no cookie auth, try Bearer token authentication
	if u == nil {
		auth_header := c.GetHeader("Authorization")
		if strings.HasPrefix(auth_header, "Bearer ") {
			token := strings.TrimPrefix(auth_header, "Bearer ")
			if uid, err := jwt_verify(token); err == nil && uid > 0 {
				if user := user_by_id(uid); user != nil {
					u = user
					debug("Identity creation: JWT token accepted for user %d", u.ID)
				}
			}
		}
	}

	if u == nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Authentication required"})
		return
	}

	var input struct {
		Name    string `json:"name"`
		Privacy string `json:"privacy"`
	}
	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	_, err := entity_create(u, "person", input.Name, input.Privacy, "")
	if err != nil {
		info("Identity creation error for user %d: %v", u.ID, err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "Unable to create identity"})
		return
	}

	// Simple notification hook
	admin := ini_string("email", "admin", "")
	if admin != "" {
		email_send(admin, "Mochi new user identity", "New user: "+u.Username+"\nUsername: "+input.Name)
	}

	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// Log the user out
func web_logout(c *gin.Context) {
	login := web_cookie_get(c, "login", "")
	if login != "" {
		login_delete(login)
	}
	web_cookie_unset(c, "login")
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// Handle app paths
func web_path(c *gin.Context) {
	debug("Web path %q", c.Request.URL.Path)

	// Check for domain-based routing first
	if domain_entity, exists := c.Get("domain_entity"); exists && domain_entity.(string) != "" {
		e := entity_by_any(domain_entity.(string))
		if e == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "Entity not found"})
			return
		}

		// Determine which app to use
		a := e.class_app()
		if a == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "No app for entity"})
			return
		}

		// Action is the remaining path (no entity prefix)
		action := strings.TrimPrefix(c.GetString("domain_remaining"), "/")

		web_action(c, a, action, e)
		return
	}

	raw := strings.Trim(c.Request.URL.Path, "/")

	// Check for app that handles root path
	if raw == "" {
		if a := app_by_root(); a != nil {
			web_action(c, a, "", nil)
			return
		}
		c.JSON(http.StatusNotFound, gin.H{"error": "No root app configured"})
		return
	}

	segments := strings.Split(raw, "/")
	first := segments[0]

	// Check for app matching first segment
	a := app_by_any(first)
	if a != nil {
		second := ""
		if len(segments) > 1 {
			second = segments[1]
		}

		// Route on /<app>/<entity>[/<action...>]
		e := entity_by_any(second)
		if e != nil && web_action(c, a, strings.Join(segments[2:], "/"), e) {
			return
		}

		// Route on /<app>/<action...>
		web_action(c, a, strings.Join(segments[1:], "/"), nil)
		return
	}

	// Check for entity matching first segment
	e := entity_by_any(first)
	if e != nil {
		a := e.class_app()
		if a == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "No app for entity class"})
			return
		}

		action := e.Fingerprint
		if len(segments) > 1 {
			action = e.Fingerprint + "/" + strings.Join(segments[1:], "/")
		}

		web_action(c, a, action, e)
		return
	}

	// Unknown path - route to root app if available
	if a := app_by_root(); a != nil {
		web_action(c, a, raw, nil)
		return
	}
	c.JSON(http.StatusNotFound, gin.H{"error": "Not found"})
}

func web_ping(c *gin.Context) {
	c.String(http.StatusOK, "pong")
}

// Start the web server
func web_start() {
	listen := ini_string("web", "listen", "")
	port := ini_int("web", "port", 80)
	if port == 0 {
		return
	}
	domains := ini_strings_commas("web", "domains")

	if !ini_bool("web", "debug", false) {
		gin.SetMode(gin.ReleaseMode)
	}
	r := gin.Default()
	r.SetTrustedProxies(nil)
	r.Use(web_security_headers)
	r.Use(rate_limit_api_middleware)
	r.Use(domains_middleware())
	r.RedirectTrailingSlash = false

	// System endpoints
	r.POST("/_/code", rate_limit_login_middleware, web_login_code)
	r.POST("/_/verify", rate_limit_login_middleware, web_login_verify)
	r.POST("/_/identity", web_login_identity)
	r.POST("/_/logout", web_logout)
	r.GET("/_/ping", web_ping)
	r.GET("/_/websocket", websocket_connection)

	// All other paths are handled by web_path()
	r.NoRoute(web_path)

	if len(domains) > 0 || ini_bool("web", "https", false) {
		web_https = true
		tlsConfig := &tls.Config{
			GetCertificate: domains_get_certificate,
		}
		info("Web listening on HTTPS (domains from database)")
		must(autotls.RunWithManagerAndTLSConfig(r, domains_acme_manager, tlsConfig))
	} else {
		info("Web listening on %q:%d", listen, port)
		must(r.Run(fmt.Sprintf("%s:%d", listen, port)))
	}
}

// Serve an attachment or thumbnail
func web_serve_attachment(c *gin.Context, app *App, user *User, name string) bool {
	// Parse path: files/:id or files/:id/thumbnail
	parts := strings.Split(name, "/")
	if len(parts) < 2 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid path"})
		return true
	}

	id := parts[1]
	thumbnail := len(parts) >= 3 && parts[2] == "thumbnail"

	if !valid(id, "id") {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid attachment ID"})
		return true
	}

	db := db_app(user, app.active)
	if db == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return true
	}

	var att Attachment
	if !db.scan(&att, "select * from _attachments where id = ?", id) {
		c.JSON(http.StatusNotFound, gin.H{"error": "Attachment not found"})
		return true
	}

	// Get file path
	var path string
	if att.Entity != "" {
		path = fmt.Sprintf("%s/attachments/%s/%s/%s", cache_dir, att.Entity, app.id, id)
	} else {
		path = data_dir + "/" + db.attachment_path(att.ID, att.Name)
	}

	if !file_exists(path) {
		c.JSON(http.StatusNotFound, gin.H{"error": "File not found"})
		return true
	}

	if thumbnail {
		if thumb, err := thumbnail_create(path); err == nil && thumb != "" {
			c.Header("Cache-Control", "public, max-age=86400")
			c.File(thumb)
			return true
		}
	}

	c.Header("Content-Disposition", fmt.Sprintf("inline; filename=%q", att.Name))
	c.File(path)
	return true
}

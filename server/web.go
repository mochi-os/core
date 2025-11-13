// Mochi server: Sample web interface
// Copyright Alistair Cunningham

package main

import (
	"embed"
	"fmt"
	"html/template"
	"net/http"
	"sort"
	"strings"

	"github.com/gin-gonic/autotls"
	"github.com/gin-gonic/gin"
	sl "go.starlark.net/starlark"
)

//go:embed templates/en/*.tmpl templates/en/*/*.tmpl templates/en/*/*/*.tmpl
var templates embed.FS

// -----------------------------------------------------------------------------
// Middleware / helpers
// -----------------------------------------------------------------------------

// Simple CORS middleware to allow browser clients
func cors_middleware(c *gin.Context) {
	c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
	c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	c.Writer.Header().Set("Access-Control-Allow-Headers", "Origin, Content-Type, Accept, Authorization, X-Login")
	c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")

	if c.Request.Method == http.MethodOptions {
		c.AbortWithStatus(http.StatusNoContent)
		return
	}
	c.Next()
}

func web_auth(c *gin.Context) *User {
	return user_by_login(web_cookie_get(c, "login", ""))
}

func web_cookie_get(c *gin.Context, name string, def string) string {
	value, err := c.Cookie(name)
	if err != nil {
		return def
	}
	return value
}

func web_cookie_set(c *gin.Context, name string, value string) {
	c.SetCookie(name, value, 365*86400, "/", "", false, true)
}

func web_cookie_unset(c *gin.Context, name string) {
	c.SetCookie(name, "", -1, "/", "", false, true)
}

// Render markdown as a template.HTML object so that Go's templates don't escape it
func web_markdown(in string) template.HTML {
	return template.HTML(markdown([]byte(in)))
}

// -----------------------------------------------------------------------------
// Unified routing helpers
// -----------------------------------------------------------------------------

// normalize_fingerprint removes hyphens from a fingerprint-like string.
func normalize_fingerprint(s string) string {
	return strings.ReplaceAll(s, "-", "")
}

// resolve_app_token attempts to resolve a token to an app using:
//  1. App ID
//  2. App fingerprint (with or without hyphens)
//  3. Preferred paths (short names), e.g. "/feeds".
//
// This assumes AppVersion has:
//
//	Fingerprint    string   `json:"fingerprint"`
//	PreferredPaths []string `json:"paths"` (or similar)
func resolve_app_token(token string) *App {
	if token == "" {
		return nil
	}

	// 1. Exact app id
	if app, ok := apps[token]; ok {
		return app
	}

	nf := normalize_fingerprint(token)

	for _, app := range apps {
		av := app.active
		if av == nil {
			continue
		}

		// 2. Fingerprint (with or without hyphens)
		if app.fingerprint != "" && normalize_fingerprint(app.fingerprint) == nf {
			return app
		}

		// 3. Preferred paths
		for _, p := range av.PreferredPaths {
			if p == token {
				return app
			}
		}
	}

	return nil
}

// resolve_entity_token resolves an entity token by fingerprint (with or
// without hyphens) or id.
func resolve_entity_token(token string) *Entity {
	if token == "" {
		return nil
	}

	nf := normalize_fingerprint(token)
	if e := entity_by_fingerprint(nf); e != nil {
		return e
	}
	if e := entity_by_fingerprint(token); e != nil {
		return e
	}
	if e := entity_by_id(token); e != nil {
		return e
	}
	return nil
}

// app_for_entity returns the app that handles a given entity class.
//
// This relies on AppVersion having a `Classes []string "json:\"classes\""`
// field, as in your updated apps.go.
func app_for_entity(e *Entity) *App {
	if e == nil {
		return nil
	}
	class := e.Class

	for _, app := range apps {
		if app.active == nil {
			continue
		}
		for _, cls := range app.active.Classes {
			if cls == class {
				return app
			}
		}
	}
	return nil
}

// serve_app_static serves static files under an app's directory, e.g.
//
//	/<app>/assets/...
//	/<app>/images/...
func serve_app_static(c *gin.Context, app *App, segs []string) {
	base := app.active.base
	rel := strings.Join(segs, "/")
	file_path := base + "/" + rel
	debug("Static app file '%s' for app '%s'", file_path, app.id)
	c.File(file_path)
}

// -----------------------------------------------------------------------------
// Action lookup (based on AppVersion.Paths map)
// -----------------------------------------------------------------------------

type action_candidate struct {
	key      string
	function string
	file     string
	files    string
	public   bool
	segments int
	literals int
}

// find_action locates the most specific action for the given action_name,
// using the AppVersion.Paths[*].Actions structure defined in apps.go.
func find_action(app *App, action_name string) (string, bool, map[string]string, bool) {
	if app == nil || app.active == nil {
		return "", false, nil, false
	}

	var action_function string
	var is_public bool
	pattern_params := map[string]string{}
	var candidates []action_candidate

	for path_name, path := range app.active.Paths {
		debug("Checking path '%s' with %d actions", path_name, len(path.Actions))
		for action_key, action := range path.Actions {
			debug("Available action: '%s'", action_key)
			segs := strings.Split(action_key, "/")
			lits := 0
			for _, s := range segs {
				if !strings.HasPrefix(s, ":") {
					lits++
				}
			}
			candidates = append(candidates, action_candidate{
				key:      action_key,
				function: action.Function,
				file:     action.File,
				files:    action.Files,
				public:   action.Public,
				segments: len(segs),
				literals: lits,
			})
		}
	}

	// Sort candidates: more segments first, then more literals first
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].segments != candidates[j].segments {
			return candidates[i].segments > candidates[j].segments
		}
		return candidates[i].literals > candidates[j].literals
	})

	found := false
	for _, cand := range candidates {
		action_key := cand.key

		// Try exact match first
		if action_key == action_name {
			action_function = cand.function
			is_public = cand.public
			found = true
			debug("Found action '%s' -> function '%s' (direct)", action_name, action_function)
			break
		}

		// Try dynamic match
		key_segs := strings.Split(action_key, "/")
		val_segs := strings.Split(action_name, "/")
		if len(key_segs) != len(val_segs) {
			continue
		}
		tmp := map[string]string{}
		ok := true
		for i := 0; i < len(key_segs); i++ {
			ks := key_segs[i]
			vs := val_segs[i]
			if strings.HasPrefix(ks, ":") {
				name := ks[1:]
				tmp[name] = vs
			} else if ks != vs {
				ok = false
				break
			}
		}
		if ok {
			action_function = cand.function
			is_public = cand.public
			pattern_params = tmp
			found = true
			debug("Found action '%s' -> function '%s' (pattern '%s')", action_name, action_function, action_key)
			break
		}
	}

	return action_function, is_public, pattern_params, found
}

// dispatch_app_action executes a Starlark action for an app and returns true
// if it handled the request (even in error). If it returns false, the caller
// should treat the path as not found.
func dispatch_app_action(c *gin.Context, app *App, action_name string, e *Entity) bool {
	if app == nil || app.active == nil {
		return false
	}

	debug("Dispatch app '%s' action '%s'", app.id, action_name)

	action_function, is_public, pattern_params, found := find_action(app, action_name)
	if !found || action_function == "" {
		debug("No action found for '%s' in app '%s'", action_name, app.id)
		return false
	}

	// Get user authentication via cookie
	user := web_auth(c)

	// If no cookie auth, try header / query-based authentication
	if user == nil {
		token := ""

		// Authorization: Bearer <token>
		auth_header := c.GetHeader("Authorization")
		if strings.HasPrefix(auth_header, "Bearer ") {
			token = strings.TrimPrefix(auth_header, "Bearer ")
		}
		if token == "" {
			token = c.GetHeader("X-Login")
		}
		if token == "" {
			token = c.Query("login")
		}

		if token != "" {
			// Prefer JWT
			if uid, err := jwt_verify(token); err == nil && uid > 0 {
				if u := user_by_id(uid); u != nil {
					user = u
					debug("API JWT token accepted for user %d", u.ID)
				}
			} else {
				// Fallback: legacy login token
				if u := user_by_login(token); u != nil {
					user = u
					debug("API login token accepted for user %d", u.ID)
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
	if user == nil && !is_public {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Authentication required"})
		return true
	}

	// Require authentication for database-backed apps
	if app.active.Database.File != "" && user == nil && owner == nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Authentication required for database access"})
		return true
	}

	// Role checks
	if app.active.Requires.Role == "administrator" {
		if user == nil || user.Role != "administrator" {
			c.JSON(http.StatusForbidden, gin.H{"error": "Forbidden"})
			return true
		}
	}

	// Set up database connections if needed
	if app.active.Database.File != "" {
		if user != nil {
			user.db = db_app(user, app.active, true)
			if user.db == nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
				return true
			}
			defer user.db.close()
		}
		if owner != nil && (user == nil || owner.ID != user.ID) {
			owner.db = db_app(owner, app.active, true)
			if owner.db == nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
				return true
			}
			defer owner.db.close()
		}
	}

	// Create action context
	action := Action{
		id:    action_id(),
		user:  user,
		owner: owner,
		app:   app,
		web:   c,
		path:  nil,
	}

	// Build inputs: path params, query params, JSON body
	inputs := make(map[string]interface{})

	// Path params from pattern match
	for k, v := range pattern_params {
		inputs[k] = v
	}

	// Query params
	for key, values := range c.Request.URL.Query() {
		if len(values) > 0 {
			if _, exists := inputs[key]; !exists {
				inputs[key] = values[0]
			}
		}
	}

	// JSON body
	if strings.HasPrefix(c.Request.Header.Get("Content-Type"), "application/json") {
		var json_data map[string]interface{}
		if err := c.ShouldBindJSON(&json_data); err == nil {
			for key, value := range json_data {
				inputs[key] = value
			}
		}
	}

	// Prepare fields for Starlark
	fields := map[string]string{
		"format":               "json",
		"identity.id":          "",
		"identity.fingerprint": "",
		"identity.name":        "",
		"path":                 action_name,
	}

	if user != nil && user.Identity != nil {
		fields["identity.id"] = user.Identity.ID
		fields["identity.fingerprint"] = user.Identity.Fingerprint
		fields["identity.name"] = user.Identity.Name
	}

	// Call Starlark function
	s := app.active.starlark()
	s.set("action", &action)
	s.set("app", app)
	s.set("user", user)
	s.set("owner", owner)

	var result sl.Value
	var err error
	if app.active.Engine.Version == 1 {
		result, err = s.call(action_function, sl_encode_tuple(fields, inputs))
	} else {
		result, err = s.call(action_function, sl.Tuple{&action})
	}

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return true
	}

	decoded := sl_decode(result)

	// Honor "format: json" contract
	if result_map, ok := decoded.(map[string]interface{}); ok {
		if format, has_format := result_map["format"]; has_format && format == "json" {
			if data, has_data := result_map["data"]; has_data {
				c.JSON(http.StatusOK, data)
				return true
			}
		}
	}

	c.JSON(http.StatusOK, decoded)
	return true
}

// -----------------------------------------------------------------------------
// Route handlers for app/entity patterns
// -----------------------------------------------------------------------------

// handle_app_entity_route handles:
//
//	/<app>/<entity>
//	/<app>/<entity>/<action...>
func handle_app_entity_route(c *gin.Context, app *App, entity_token string, e *Entity, segs []string) {
	if e == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Entity not found"})
		return
	}

	action_name := entity_token
	if len(segs) > 0 {
		action_name = entity_token + "/" + strings.Join(segs, "/")
	}

	if handled := dispatch_app_action(c, app, action_name, e); handled {
		return
	}

	// Fallback: serve SPA
	share := ini_string("directories", "share", "/usr/share/mochi")
	c.File(share + "/index.html")
}

// handle_direct_entity_route handles:
//
//	/<entity>
//	/<entity>/<action...>
func handle_direct_entity_route(c *gin.Context, e *Entity, segs []string) {
	if e == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Entity not found"})
		return
	}

	app := app_for_entity(e)
	if app == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "No app for entity class"})
		return
	}

	action_name := e.Fingerprint
	if len(segs) > 0 {
		action_name = e.Fingerprint + "/" + strings.Join(segs, "/")
	}

	if handled := dispatch_app_action(c, app, action_name, e); handled {
		return
	}

	share := ini_string("directories", "share", "/usr/share/mochi")
	c.File(share + "/index.html")
}

// handle_app_route handles:
//
//	/<app>
//	/<app>/<...>
func handle_app_route(c *gin.Context, app *App, segs []string) {
	// No extra segments → app main page (SPA)
	if len(segs) == 0 {
		share := ini_string("directories", "share", "/usr/share/mochi")
		c.File(share + "/index.html")
		return
	}

	// Assets
	if segs[0] == "assets" || segs[0] == "images" {
		serve_app_static(c, app, segs)
		return
	}

	// Entity route: /<app>/<entity>[/<action...>]
	if e := resolve_entity_token(segs[0]); e != nil {
		entity_token := segs[0]
		handle_app_entity_route(c, app, entity_token, e, segs[1:])
		return
	}

	// App-level action: /<app>/<action...>
	action_name := strings.Join(segs, "/")
	if handled := dispatch_app_action(c, app, action_name, nil); handled {
		return
	}

	share := ini_string("directories", "share", "/usr/share/mochi")
	c.File(share + "/index.html")
}

// -----------------------------------------------------------------------------
// Unified web path dispatcher
// -----------------------------------------------------------------------------

// web_path is the unified dispatcher for all non-core routes.
// It replaces the old Path.web_path and handle_api.
func web_path(c *gin.Context) {
	raw_path := strings.Trim(c.Request.URL.Path, "/")

	// Let /api/* be handled only by explicit handlers (login, etc)
	if strings.HasPrefix(raw_path, "api/") {
		c.JSON(http.StatusNotFound, gin.H{"error": "Not found"})
		return
	}

	// Root → SPA
	if raw_path == "" {
		share := ini_string("directories", "share", "/usr/share/mochi")
		c.File(share + "/index.html")
		return
	}

	segs := strings.Split(raw_path, "/")
	first := segs[0]

	// 1. App token (id, fingerprint, preferred path)
	if app := resolve_app_token(first); app != nil {
		handle_app_route(c, app, segs[1:])
		return
	}

	// 2. Direct entity
	if e := resolve_entity_token(first); e != nil {
		handle_direct_entity_route(c, e, segs[1:])
		return
	}

	// 3. Fallback: SPA
	share := ini_string("directories", "share", "/usr/share/mochi")
	c.File(share + "/index.html")
}

// -----------------------------------------------------------------------------
// Misc web helpers (error, redirect, login, etc.)
// -----------------------------------------------------------------------------

func web_ping(c *gin.Context) {
	c.String(http.StatusOK, "pong")
}

func web_redirect(c *gin.Context, url string) {
	web_template(c, http.StatusOK, "redirect", url)
}

func web_error(c *gin.Context, code int, message string, values ...any) {
	web_template(c, code, "error", fmt.Sprintf(message, values...))
}

// Create an identity for a new user
func web_identity_create(c *gin.Context) {
	u := web_auth(c)
	if u == nil {
		// Not logged in; redirect to login
		web_redirect(c, "/login")
		return
	}

	_, err := entity_create(u, "person", c.PostForm("name"), c.PostForm("privacy"), "")
	if err != nil {
		web_error(c, http.StatusBadRequest, "Unable to create identity: %s", err)
		return
	}

	// Simple notification hook (same spirit as original)
	admin := ini_string("email", "admin", "")
	if admin != "" {
		email_send(admin, "Mochi new user identity", "New user: "+u.Username+"\nUsername: "+c.PostForm("name"))
	}

	web_redirect(c, "/")
}

// Basic login page + code handling
func web_login(c *gin.Context) {
	// If we already have a valid session, just go home
	if u := web_auth(c); u != nil && u.Identity != nil {
		web_redirect(c, "/")
		return
	}

	// Login by code
	code := c.PostForm("code")
	if code != "" {
		u := user_from_code(code)
		if u == nil {
			web_error(c, http.StatusBadRequest, "Invalid code")
			return
		}
		web_cookie_set(c, "login", login_create(u.ID))
		web_redirect(c, "/")
		return
	}

	// Login by email (send code)
	email := c.PostForm("email")
	if email != "" {
		if !code_send(email) {
			web_error(c, http.StatusBadRequest, "Unable to send login email")
			return
		}
		web_template(c, http.StatusOK, "login/email_sent")
		return
	}

	// Default: show login form
	web_template(c, http.StatusOK, "login/email")
}

// Log the user out
func web_logout(c *gin.Context) {
	login := web_cookie_get(c, "login", "")
	if login != "" {
		login_delete(login)
	}
	web_cookie_unset(c, "login")
	web_template(c, http.StatusOK, "login/logout")
}

// API login: request a code by email
func api_login(c *gin.Context) {
	var input struct {
		Email string `json:"email"`
	}
	if err := c.ShouldBindJSON(&input); err != nil || input.Email == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	if !code_send(input.Email) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Unable to send login email"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "ok"})
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
	r.Use(cors_middleware)
	// Avoid 301 redirects on API preflights (which break CORS)
	r.RedirectTrailingSlash = false

	share := ini_string("directories", "share", "/usr/share/mochi")

	// Static assets and SPA root
	r.Static("/assets", share+"/assets")
	r.Static("/images", share+"/images")
	r.GET("/", func(c *gin.Context) {
		c.File(share + "/index.html")
	})

	// Core auth & infra
	r.POST("/api/login", api_login)
	r.POST("/api/login/auth", api_login_auth)
	r.GET("/login", web_login)
	r.POST("/login", web_login)
	r.POST("/login/identity", web_identity_create)
	r.GET("/logout", web_logout)
	r.GET("/ping", web_ping)
	r.GET("/websocket", websocket_connection)

	// Special case: APT repo hosting
	if ini_string("web", "special", "") == "packages" {
		r.Static("/apt", "/srv/apt")
	}

	// Unified routing for everything else
	r.NoRoute(web_path)

	if len(domains) > 0 {
		info("Web listening on HTTPS domains %v", domains)
		must(autotls.Run(r, domains...))
	} else {
		info("Web listening on '%s:%d'", listen, port)
		must(r.Run(fmt.Sprintf("%s:%d", listen, port)))
	}
}

// Render a web template (using embedded FS)
func web_template(c *gin.Context, code int, file string, values ...any) {
	t, err := template.ParseFS(templates, "templates/en/"+file+".tmpl", "templates/en/include.tmpl")
	if err != nil {
		c.Status(http.StatusInternalServerError)
		// Avoid recursion by not calling web_error here again
		panic("Web template error: " + err.Error())
	}
	c.Status(code)
	if len(values) > 0 {
		err = t.Execute(c.Writer, values[0])
	} else {
		err = t.Execute(c.Writer, nil)
	}
	if err != nil {
		panic("Web template error: " + err.Error())
	}
}

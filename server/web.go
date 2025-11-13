// Mochi server: Sample web interface
// Copyright Alistair Cunningham

package main

import (
	"embed"
	"fmt"
	"github.com/gin-gonic/autotls"
	"github.com/gin-gonic/gin"
	sl "go.starlark.net/starlark"
	"html/template"
	"net/http"
	"strings"
)

//go:embed templates/en/*.tmpl templates/en/*/*.tmpl templates/en/*/*/*.tmpl
var templates embed.FS

// Call a web action
func web_action(c *gin.Context, a *App, name string, e *Entity) bool {
	if a == nil || a.active == nil {
		return false
	}

	debug("Web app '%s' action '%s'", a.id, name)

	aa := a.active.find_action(name)
	if aa == nil || aa.Function == "" {
		debug("No action found for app '%s' action '%s'", a.id, name)
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
	if user == nil && !aa.Public {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Authentication required"})
		return true
	}

	// Require authentication for database-backed apps
	if a.active.Database.File != "" && user == nil && owner == nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Authentication required for database access"})
		return true
	}

	// Role checks
	if a.active.Requires.Role == "administrator" {
		if user == nil || user.Role != "administrator" {
			c.JSON(http.StatusForbidden, gin.H{"error": "Forbidden"})
			return true
		}
	}

	// Set up database connections if needed
	if a.active.Database.File != "" {
		if user != nil {
			user.db = db_app(user, a.active, true)
			if user.db == nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
				return true
			}
			defer user.db.close()
		}
		if owner != nil && (user == nil || owner.ID != user.ID) {
			owner.db = db_app(owner, a.active, true)
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
		app:   a,
		web:   c,
		path:  nil,
	}

	// Build inputs: path params, query params, JSON body
	inputs := make(map[string]interface{})

	// Path parameters from pattern match
	for k, v := range aa.parameters {
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
		"path":                 name,
	}

	if user != nil && user.Identity != nil {
		fields["identity.id"] = user.Identity.ID
		fields["identity.fingerprint"] = user.Identity.Fingerprint
		fields["identity.name"] = user.Identity.Name
	}

	// Call Starlark function
	s := a.active.starlark()
	s.set("action", &action)
	s.set("app", a)
	s.set("user", user)
	s.set("owner", owner)

	var result sl.Value
	var err error
	if a.active.Engine.Version == 1 {
		result, err = s.call(aa.Function, sl_encode_tuple(fields, inputs))
	} else {
		result, err = s.call(aa.Function, sl.Tuple{&action})
	}

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return true
	}

	c.JSON(http.StatusOK, sl_decode(result))
	return true
}

// Get user for login cookie
func web_auth(c *gin.Context) *User {
	return user_by_login(web_cookie_get(c, "login", ""))
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
	c.SetCookie(name, value, 365*86400, "/", "", false, true)
}

// Unset a cookie
func web_cookie_unset(c *gin.Context, name string) {
	c.SetCookie(name, "", -1, "/", "", false, true)
}

// Simple CORS middleware to allow browser clients
func web_cors_middleware(c *gin.Context) {
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

// Render markdown as a template.HTML object so that Go's templates don't escape it
func web_markdown(in string) template.HTML {
	return template.HTML(markdown([]byte(in)))
}

// Handle app paths
func web_path(c *gin.Context) {
	raw := strings.Trim(c.Request.URL.Path, "/")
	if raw == "" {
		web_root(c)
		return
	}

	segments := strings.Split(raw, "/")
	first := segments[0]

	//TODO Remove
	if first == "api" {
		segments = segments[1:]
		first = segments[0]
	}

	debug("Looking for app or entity for '%s'", first)

	// Check for app matching first segment
	a := app_by_any(first)
	if a != nil {
		debug("Found app '%s' for '%s'", a.id, first)

		second := ""
		if len(segments) > 1 {
			second = segments[1]
		}

		// Assets
		//TODO Check the files field rather than hard code assets and images
		if second == "assets" || second == "images" {
			file := a.active.base + "/" + strings.Join(segments[1:], "/")
			debug("Static file for app '%s' file '%s'", a.id, file)
			c.File(file)
			return
		}

		// Entity route: /<app>/<entity>[/<action...>]
		e := entity_by_any(second)
		if e != nil && web_action(c, a, strings.Join(segments[2:], "/"), e) {
			return
		}

		// App-level action: /<app>/<action...>
		web_action(c, a, strings.Join(segments[1:], "/"), nil)
		return
	}

	// Check for entity matching first segment
	e := entity_by_any(first)
	if e != nil {
		debug("Found entity '%s' for '%s'", e.ID, first)

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

	// No path found, pass to web_root()
	debug("Web path not found, calling root")
	web_root(c)
}

// Handle / and any paths not handled by web_path()
func web_root(c *gin.Context) {
	c.File(ini_string("directories", "share", "/usr/share/mochi") + "/index.html")
}

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
	r.Use(web_cors_middleware)
	r.RedirectTrailingSlash = false // Avoid 301 redirects on API preflights, which break CORS

	// Serve built-in paths
	share := ini_string("directories", "share", "/usr/share/mochi")
	r.GET("/", web_root)
	r.Static("/assets", share+"/assets")
	r.POST("/api/login", api_login)
	r.POST("/api/login/auth", api_login_auth)
	r.Static("/images", share+"/images")
	r.GET("/login", web_login)
	r.POST("/login", web_login)
	r.POST("/login/identity", web_identity_create)
	r.GET("/logout", web_logout)
	r.GET("/ping", web_ping)
	r.GET("/websocket", websocket_connection)

	// Remove once we get URL mapping
	if ini_string("web", "special", "") == "packages" {
		r.Static("/apt", "/srv/apt")
	}

	// All other paths are handleed by web_path()
	r.NoRoute(web_path)

	if len(domains) > 0 {
		info("Web listening on HTTPS domains %v", domains)
		must(autotls.Run(r, domains...))
	} else {
		info("Web listening on '%s:%d'", listen, port)
		must(r.Run(fmt.Sprintf("%s:%d", listen, port)))
	}
}

// Render a web template using embedded FS
func web_template(c *gin.Context, code int, file string, values ...any) {
	t, err := template.ParseFS(templates, "templates/en/"+file+".tmpl", "templates/en/include.tmpl")
	if err != nil {
		c.Status(http.StatusInternalServerError)
		panic("Web template error: " + err.Error()) // Avoid recursion by not calling web_error here again
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

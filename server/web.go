// Mochi server: Sample web interface
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"embed"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"github.com/gin-gonic/autotls"
	"github.com/gin-gonic/gin"
)

var (
	//go:embed templates/en/*.tmpl templates/en/*/*.tmpl templates/en/*/*/*.tmpl
	templates embed.FS
)

// Simple CORS middleware to allow browser clients
func corsMiddleware(c *gin.Context) {
	c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
	c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	c.Writer.Header().Set("Access-Control-Allow-Headers", "Origin, Content-Type, Accept, Authorization, X-Login")
	// If you later need cookies over CORS, set Allow-Credentials and restrict origin
	// c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")

	if c.Request.Method == "OPTIONS" {
		c.AbortWithStatus(204)
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

func web_error(c *gin.Context, code int, message string, values ...any) {
	web_template(c, code, "error", fmt.Sprintf(message, values...))
}

// Create an identity for a new user
func web_identity_create(c *gin.Context) {
	u := web_auth(c)
	if u == nil {
		return
	}

	_, err := entity_create(u, "person", c.PostForm("name"), c.PostForm("privacy"), "")
	if err != nil {
		web_error(c, 400, "Unable to create identity: %s", err)
		return
	}

	// Remove once we have hooks
	admin := ini_string("email", "admin", "")
	if admin != "" {
		email_send(admin, "Mochi new user identity", "New user: "+u.Username+"\nUsername: "+c.PostForm("name"))
	}

	web_redirect(c, "/?action=welcome")
}

// Get all inputs
func web_inputs(c *gin.Context) map[string]string {
	inputs := map[string]string{}

	err := c.Request.ParseForm()
	if err == nil {
		for key, values := range c.Request.PostForm {
			for _, value := range values {
				inputs[key] = value
			}
		}
	}

	for key, values := range c.Request.URL.Query() {
		for _, value := range values {
			inputs[key] = value
		}
	}

	for _, param := range c.Params {
		inputs[param.Key] = param.Value
	}

	return inputs
}

// Log the user in using an email code
func web_login(c *gin.Context) {
	code := c.PostForm("code")
	if code != "" {
		u := user_from_code(code)
		if u == nil {
			web_error(c, 400, "Invalid code")
			return
		}
		web_cookie_set(c, "login", login_create(u.ID))

		web_redirect(c, "/")
		return
	}

	email := c.PostForm("email")
	if email != "" {
		if !code_send(email) {
			web_error(c, 400, "Invalid email address")
			return
		}
		web_template(c, 200, "login/code", email)
		return
	}

	web_template(c, 200, "login/email")
}

// Log the user out
func web_logout(c *gin.Context) {
	login := web_cookie_get(c, "login", "")
	if login != "" {
		login_delete(login)
	}
	web_cookie_unset(c, "login")
	web_template(c, 200, "login/logout")
}

// Render markdown as a template.HTML object so that Go's templates don't escape it
func web_markdown(in string) template.HTML {
	return template.HTML(markdown([]byte(in)))
}

// Handle web paths
func (p *Path) web_path(c *gin.Context) {
	var user *User = nil

	referrer, err := url.Parse(c.Request.Header.Get("Referer"))
	if err == nil && (referrer.Host == "" || referrer.Host == c.Request.Host) {
		user = web_auth(c)
		if user != nil && user.Identity == nil {
			web_template(c, 200, "login/identity")
			return
		}
	}

	var e *Entity = nil
	entity := c.Param(p.app.entity_field)
	if entity != "" {
		e = entity_by_fingerprint(entity)
		if e == nil {
			e = entity_by_id(entity)
		}
	}

	if p.app.Database.File != "" && user != nil {
		user.db = db_app(user, p.app)
		if user.db == nil {
			web_error(c, 500, "No user database for app")
			return
		}
		defer user.db.close()
	}

	owner := user
	if e != nil {
		owner = user_owning_entity(e.ID)
		if p.app.Database.File != "" && owner != nil {
			owner.db = db_app(owner, p.app)
			if owner.db == nil {
				web_error(c, 500, "No owner database for app")
				return
			}
			defer owner.db.close()
		}
	}

	if p.app.Database.File != "" && user == nil && owner == nil {
		web_redirect(c, "/login")
		return
	}

	// Require role if app requires it
	if p.app.Requires.Role == "administrator" && user.Role != "administrator" {
		web_error(c, 403, "Forbidden")
		return
	}

	a := Action{user: user, owner: owner, app: p.app, web: c, path: p}

	switch p.app.Engine.Architecture {
	case "internal":
		if p.internal == nil {
			web_error(c, 500, "No function for internal path")
			return
		}
		p.internal(&a)

	case "starlark":
		if p.function == "" {
			web_error(c, 500, "No function for path")
			return
		}

		if user == nil && !p.public {
			web_redirect(c, "/login")
			return
		}

		s := p.app.starlark()
		s.set("action", &a)
		s.set("app", p.app)
		s.set("user", a.user)
		s.set("owner", a.owner)

		fields := map[string]string{
			"format":               a.input("format"),
			"identity.id":          a.user.Identity.ID,
			"identity.fingerprint": a.user.Identity.Fingerprint,
			"identity.name":        a.user.Identity.Name,
			"path":                 p.path,
		}

		_, err := s.call(p.function, starlark_encode_tuple(fields, web_inputs(c)))
		if err != nil {
			web_error(c, 500, "%v", err)
		}

	default:
		web_error(c, 500, "No engine for path")
		return
	}
}

func web_ping(c *gin.Context) {
	c.String(http.StatusOK, "pong")
}

// Handle generic API requests for Starlark apps
// TODO Change variables coding style
func handleAPI(c *gin.Context) {
	appID := c.Param("app")
	actionName := strings.TrimPrefix(c.Param("action"), "/")

	debug("API request: app='%s', action='%s'", appID, actionName)

	// Find the app by ID first, then by name
	app, exists := apps[appID]
	if !exists {
		// Try to find by name
		//TODO Remove
		/*
			for _, a := range apps {
				if a.Name == appID {
					app = a
					exists = true
					debug("Found app by name: %s (ID: %s)", a.Name, a.id)
					break
				}
			}
		*/
	} else {
		debug("Found app by ID: %s", appID)
	}

	if !exists {
		debug("App not found: %s", appID)
		c.JSON(404, gin.H{"error": "App not found"})
		return
	}

	// Find the action in the app's paths (supports dynamic segments like :chat/messages)
	var actionFunction string
	var isPublic bool
	found := false
	patternParams := map[string]string{}

	debug("Looking for action '%s' in app '%s'", actionName, app.id)

	// Collect all actions from all paths
	type actionCandidate struct {
		key      string
		function string
		public   bool
		segments int
		literals int
	}
	var candidates []actionCandidate

	for pathName, path := range app.Paths {
		debug("Checking path '%s' with %d actions", pathName, len(path.Actions))
		for actionKey, action := range path.Actions {
			debug("Available action: '%s'", actionKey)
			segs := strings.Split(actionKey, "/")
			lits := 0
			for _, s := range segs {
				if !strings.HasPrefix(s, ":") {
					lits++
				}
			}
			candidates = append(candidates, actionCandidate{
				key:      actionKey,
				function: action.Function,
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

	// Try to match in order of specificity
	for _, cand := range candidates {
		actionKey := cand.key

		// Try exact match first
		if actionKey == actionName {
			actionFunction = cand.function
			isPublic = cand.public
			found = true
			debug("Found action '%s' -> function '%s' (direct)", actionName, actionFunction)
			break
		}

		// Try dynamic match
		keySegs := strings.Split(actionKey, "/")
		valSegs := strings.Split(actionName, "/")
		if len(keySegs) != len(valSegs) {
			continue
		}
		tmp := map[string]string{}
		ok := true
		for i := 0; i < len(keySegs); i++ {
			ks := keySegs[i]
			vs := valSegs[i]
			if strings.HasPrefix(ks, ":") {
				name := ks[1:]
				tmp[name] = vs
			} else if ks != vs {
				ok = false
				break
			}
		}
		if ok {
			actionFunction = cand.function
			isPublic = cand.public
			patternParams = tmp
			found = true
			debug("Found action '%s' -> function '%s' (pattern match)", actionKey, actionFunction)
			break
		}
	}

	if !found {
		debug("Action '%s' not found in app '%s'", actionName, app.id)
		c.JSON(404, gin.H{"error": "Action not found"})
		return
	}

	// Get user authentication via cookie
	user := web_auth(c)

	// If no cookie auth, try token-based authentication
	if user == nil {
		token := ""

		// Check Authorization header (Bearer token)
		authHeader := c.GetHeader("Authorization")
		if strings.HasPrefix(authHeader, "Bearer ") {
			token = strings.TrimPrefix(authHeader, "Bearer ")
		}

		// Check X-Login header
		if token == "" {
			token = c.GetHeader("X-Login")
		}

		// Check login query parameter
		if token == "" {
			token = c.Query("login")
		}

		// Authenticate with token
		if token != "" {
			if u := user_by_login(token); u != nil {
				user = u
				debug("API login token accepted for user %d", u.ID)
			}
		}
	}

	// Require authentication for non-public actions
	if user == nil && !isPublic {
		c.JSON(401, gin.H{"error": "Authentication required"})
		return
	}

	// Require authentication for database-backed apps
	if user == nil && app.Database.File != "" {
		c.JSON(401, gin.H{"error": "Authentication required for database access"})
		return
	}

	// Require role if app requires it
	debug("ROLE REQUIRED '%s', HAVE '%s'", app.Requires.Role, user.Role)
	if app.Requires.Role == "administrator" && user.Role != "administrator" {
		c.JSON(403, gin.H{"error": "Forbidden"})
		return
	}

	// Set up database if needed
	if app.Database.File != "" {
		user.db = db_app(user, app)
		if user.db == nil {
			c.JSON(500, gin.H{"error": "Database error"})
			return
		}
		defer user.db.close()
	}

	// Create action context
	action := Action{user: user, owner: user, app: app, web: c, path: nil}

	// Prepare inputs from path params, query parameters and JSON body
	inputs := make(map[string]interface{})
	// Add extracted path params first
	for k, v := range patternParams {
		inputs[k] = v
	}

	// Add query parameters
	for key, values := range c.Request.URL.Query() {
		if len(values) > 0 {
			if _, exists := inputs[key]; !exists {
				inputs[key] = values[0]
			}
		}
	}

	// Add JSON body if present
	if c.Request.Header.Get("Content-Type") == "application/json" {
		var jsonData map[string]interface{}
		if err := c.ShouldBindJSON(&jsonData); err == nil {
			for key, value := range jsonData {
				inputs[key] = value
			}
		}
	}

	// Prepare action context for Starlark
	fields := map[string]string{
		"format":               "json",
		"identity.id":          "",
		"identity.fingerprint": "",
		"identity.name":        "",
		"path":                 actionName,
	}

	if user != nil && user.Identity != nil {
		fields["identity.id"] = user.Identity.ID
		fields["identity.fingerprint"] = user.Identity.Fingerprint
		fields["identity.name"] = user.Identity.Name
	}

	// Call the Starlark function
	s := app.starlark()
	s.set("action", &action)
	s.set("app", app)
	s.set("user", user)
	s.set("owner", user)

	result, err := s.call(actionFunction, starlark_encode_tuple(fields, inputs))
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}

	// Check if the result is a JSON response format
	if resultMap, ok := starlark_decode(result).(map[string]interface{}); ok {
		if format, hasFormat := resultMap["format"]; hasFormat && format == "json" {
			if data, hasData := resultMap["data"]; hasData {
				c.JSON(200, data)
				return
			}
		}
	}

	// Fallback: return the raw result
	c.JSON(200, starlark_decode(result))
}

func web_redirect(c *gin.Context, url string) {
	web_template(c, 200, "redirect", url)
}

// Start the web server
func web_start() {
	listen := ini_string("web", "listen", "")
	port := ini_int("web", "port", 80)
	if port == 0 {
		return
	}
	domains := ini_strings_commas("web", "domains")
	debug := ini_bool("web", "debug", false)

	if !debug {
		gin.SetMode(gin.ReleaseMode)
	}
	r := gin.Default()
	r.SetTrustedProxies(nil)
	r.Use(corsMiddleware)
	// Avoid 301 redirects on API preflights (which break CORS)
	r.RedirectTrailingSlash = false

	for _, p := range paths {
		r.GET("/"+p.path, p.web_path)
		r.POST("/"+p.path, p.web_path)
	}
	r.GET("/login", web_login)
	r.POST("/login", web_login)
	r.POST("/login/identity", web_identity_create)
	r.GET("/logout", web_logout)
	r.GET("/ping", web_ping)
	r.GET("/websocket", websocket_connection)
	// Support both /api/:app and /api/:app/<action...>
	r.Any("/api/:app", handleAPI)
	r.Any("/api/:app/*action", handleAPI)

	// Replace when we implement URL mapping
	if ini_string("web", "special", "") == "packages" {
		r.Static("/apt", "/srv/apt")
	}

	if len(domains) > 0 {
		info("Web listening on HTTPS domains %v", domains)
		must(autotls.Run(r, domains...))

	} else {
		info("Web listening on '%s:%d'", listen, port)
		must(r.Run(fmt.Sprintf("%s:%d", listen, port)))
	}
}

// Render a web template
// This could probably be better written using Gin's c.HTML(), but I can't figure out how to load the templates
func web_template(c *gin.Context, code int, file string, values ...any) {
	t, err := template.ParseFS(templates, "templates/en/"+file+".tmpl", "templates/en/include.tmpl")
	if err != nil {
		web_error(c, 500, "Web template error")
		panic("Web template error: " + err.Error())
	}
	if len(values) > 0 {
		err = t.Execute(c.Writer, values[0])
	} else {
		err = t.Execute(c.Writer, nil)
	}
	if err != nil {
		panic("Web template error: " + err.Error())
	}
}

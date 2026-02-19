// Mochi server: Sample web interface
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/gin-gonic/autotls"
	"github.com/gin-gonic/gin"
	sl "go.starlark.net/starlark"
)

var (
	match_react = regexp.MustCompile(`assets/.*-[\w-]{8}\.(js|css)$`)
	web_https   = false
)

// Call a web action
func web_action(c *gin.Context, a *App, name string, e *Entity) bool {
	if a == nil {
		return false
	}

	var user *User
	var api_token *Token

	// Check query parameter token first (for RSS feeds, etc.)
	// This takes priority over cookies so RSS tokens work in logged-in browsers
	if query_token := c.Query("token"); query_token != "" {
		api_token = token_validate(query_token)
		if api_token != nil {
			user = user_by_id(api_token.User)
			if user == nil {
				debug("Query token valid but user %d not found", api_token.User)
				api_token = nil
			}
		}
	}

	// Get user authentication via cookie (needed for version selection)
	if user == nil {
		user = web_auth(c)
	}

	// If no cookie auth, try Bearer token authentication
	if user == nil {
		auth_header := c.GetHeader("Authorization")
		if strings.HasPrefix(auth_header, "Bearer ") {
			bearer := strings.TrimPrefix(auth_header, "Bearer ")
			if strings.HasPrefix(bearer, "mochi-") {
				// API token authentication
				api_token = token_validate(bearer)
				if api_token != nil {
					user = user_by_id(api_token.User)
					if user == nil {
						debug("API token valid but user %d not found", api_token.User)
						api_token = nil
					}
				}
			} else {
				// JWT authentication
				if uid, err := jwt_verify(bearer); err == nil && uid > 0 {
					if u := user_by_id(uid); u != nil {
						user = u
					} else {
						debug("API JWT token valid but user %d not found", uid)
					}
				} else {
					debug("API JWT token verification failed: %v", err)
				}
			}
		}
	}

	// Get the app version for this user (user preference or default)
	av := a.active(user)
	if av == nil {
		return false
	}
	if dev_reload {
		av.reload()
	}

	// API tokens are restricted to their app
	if api_token != nil && api_token.App != a.id {
		c.JSON(http.StatusForbidden, gin.H{"error": "token not valid for this app"})
		return true
	}

	// Run first-time setup for this user and app (grants default permissions)
	app_user_setup(user, a.id)

	// When entity is provided via domain routing, try entity-prefixed actions.
	// Skip this for main site routing where action already includes fingerprint (e.g., "abc123/-/info").
	// Also skip when action is the entity's fingerprint itself (viewing entity root).
	// For browser requests (Accept: text/html), try the non-API action first to serve HTML.
	var aa *AppAction
	accept := c.GetHeader("Accept")
	prefer_html := strings.Contains(accept, "text/html") && !strings.Contains(accept, "application/json")
	if e != nil && e.Class != "" && name != e.Fingerprint {
		if name == "" {
			// Entity root (e.g., /) - use :feed action
			entity_action := ":" + e.Class
			aa = av.find_action(entity_action)
		} else if strings.HasPrefix(name, "-/") {
			// API path (e.g., -/info) - convert to :wiki/-/info
			entity_action := ":" + e.Class + "/" + name
			aa = av.find_action(entity_action)
		} else if !strings.Contains(name, "/") {
			// Simple name (e.g., concepts) - try with entity prefix
			if prefer_html {
				// Try HTML action first (e.g., :wiki/:page), then API action
				html_action := ":" + e.Class + "/" + name
				aa = av.find_action(html_action)
				if aa == nil {
					entity_action := ":" + e.Class + "/-/" + name
					aa = av.find_action(entity_action)
				}
			} else {
				// Try API action first for non-browser requests
				entity_action := ":" + e.Class + "/-/" + name
				aa = av.find_action(entity_action)
			}
		}
	}
	if aa == nil {
		aa = av.find_action(name)
	}
	if aa == nil {
		return false
	}

	// Compute owner based on entity, domain route owner, or authenticated user
	var owner *User = user
	if e != nil {
		if o := user_owning_entity(e.ID); o != nil {
			owner = o
		}
	} else if owner == nil {
		// Fall back to domain route owner for anonymous requests without entity
		if routeOwner, ok := c.Get("domain_owner"); ok {
			if uid, ok := routeOwner.(int); ok && uid > 0 {
				owner = user_by_id(uid)
			}
		}
		// Fall back to primary user for public class-level actions
		if owner == nil && aa.Public {
			owner = user_by_id(1)
		}
	}

	// Require authentication for non-public actions
	if user == nil && !aa.Public {
		// For browser requests, redirect to login
		if strings.Contains(c.GetHeader("Accept"), "text/html") {
			// If user has a session cookie but auth failed (suspended, expired, etc),
			// clear the invalid cookie to prevent redirect loops
			if web_cookie_get(c, "session", "") != "" {
				audit_session_anomaly("", rate_limit_client_ip(c), "invalid_session")
				web_cookie_unset(c, "session")
				c.Redirect(http.StatusFound, "/login?reauth=1")
			} else {
				c.Redirect(http.StatusFound, "/login")
			}
			return true
		}
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Authentication required"})
		return true
	}

	// Require identity for authenticated users accessing non-login apps
	if user != nil && a.id != "login" && !aa.Public {
		if user.identity() == nil {
			if strings.Contains(c.GetHeader("Accept"), "text/html") {
				c.Redirect(http.StatusFound, "/login/identity")
				return true
			}
			c.JSON(http.StatusForbidden, gin.H{"error": "Identity required"})
			return true
		}
	}

	// Check app-level requirements
	if !av.user_allowed(user) {
		c.JSON(http.StatusForbidden, gin.H{"error": "Access denied"})
		return true
	}

	// Serve attachment - ID comes from :id parameter
	if aa.Feature == "attachment" || aa.Feature == "attachment/thumbnail" {
		attachment := aa.parameters["id"]
		entity := ""
		if e != nil {
			entity = e.ID
		} else if aa.parameters["wiki"] != "" {
			entity = aa.parameters["wiki"]
		} else if aa.parameters["feed"] != "" {
			entity = aa.parameters["feed"]
		} else if aa.parameters["forum"] != "" {
			entity = aa.parameters["forum"]
		}
		return web_serve_attachment(c, a, owner, entity, attachment, aa.Feature == "attachment/thumbnail")
	}

	// Handle git Smart HTTP protocol
	if aa.Feature == "git" {
		repo := aa.parameters["repository"]
		if repo == "" {
			c.String(http.StatusBadRequest, "Missing repository")
			return true
		}
		// Strip .git suffix if present (e.g., "my-project.git" -> "my-project")
		repo = strings.TrimSuffix(repo, ".git")
		return git_http_handler(c, a, owner, user, repo, aa.parameters["path"])
	}

	// Serve static file
	// If action has both file and function, do content negotiation:
	// - HTML requests (browsers/crawlers) get the file with opengraph tags
	// - API requests get the function response
	if aa.File != "" {
		serve_file := true

		// Content negotiation: if we have both file and function, check Accept header
		if aa.Function != "" && aa.OpenGraph != "" {
			accept := c.GetHeader("Accept")
			// Serve file only for HTML requests (browsers/crawlers)
			// API requests (application/json, */*) should call the function
			serve_file = strings.Contains(accept, "text/html") && !strings.Contains(accept, "application/json")
		}

		if serve_file {
			file := av.base + "/" + aa.File
			if strings.HasSuffix(aa.File, ".html") {
				web_serve_html(c, a, av, aa, e, file)
				return true
			}
			web_cache_static(c, file, aa.Cache)
			c.File(file)
			return true
		}
	}

	// Serve static files from a directory
	if aa.Files != "" {
		if aa.filepath != "" {
			if !valid(aa.filepath, "filepath") {
				c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid file"})
				return true
			}
			file := av.base + "/" + aa.Files + "/" + aa.filepath
			//debug("Serving file from directory for app %q: %q", a.id, file)
			web_cache_static(c, file, aa.Cache)
			c.File(file)
		} else {
			c.JSON(http.StatusBadRequest, gin.H{"error": "No file specified"})
		}
		return true
	}

	// Require authentication for database-backed apps (unless action is public)
	if av.Database.File != "" && user == nil && owner == nil && !aa.Public {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Authentication required for database access"})
		return true
	}

	// Set up database connections if needed
	if av.Database.File != "" {
		if user != nil {
			user.db = db_app(user, a)
			if user.db == nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
				return true
			}
			defer user.db.close()
		}

		if owner != nil && (user == nil || owner.ID != user.ID) {
			owner.db = db_app(owner, a)
			if owner.db == nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
				return true
			}
			defer owner.db.close()
		}
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
		active: av,
		token:  api_token,
		web:    c,
		inputs: make(map[string]string),
	}

	for k, v := range aa.parameters {
		action.inputs[k] = v
	}

	// Add entity to inputs when present (for entity-aware routing)
	if e != nil && e.Class != "" {
		action.inputs[e.Class] = e.ID
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
	switch av.Architecture.Engine {
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
		s := av.starlark()
		s.set("action", &action)
		s.set("app", a)
		s.set("host", c.Request.Host)
		s.set("user", user)
		s.set("owner", owner)

		result, err := s.call(aa.Function, sl.Tuple{&action})
		if err != nil {
			// Check for permission error and return structured response
			var permErr *PermissionError
			if errors.As(err, &permErr) {
				c.JSON(http.StatusForbidden, gin.H{
					"error":      "permission_required",
					"app":        a.id,
					"permission": permErr.Permission,
					"restricted": permErr.Restricted,
				})
				return true
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return true
		}
		if result != sl.None {
			c.JSON(http.StatusOK, sl_decode(result))
		}

	default:
		info("Action unknown engine %q version %q", av.Architecture.Engine, av.Architecture.Version)
	}

	return true
}

// Get user for session cookie and refresh it to extend expiry
func web_auth(c *gin.Context) *User {
	session := web_cookie_get(c, "session", "")
	user := user_by_login(session)
	if user != nil {
		// Refresh cookie to reset browser expiry limits
		web_cookie_set(c, "session", session)
	}
	return user
}

// Ask browser to cache static files
func web_cache_static(c *gin.Context, path string, cache string) {
	if !web_cache {
		c.Header("Cache-Control", "no-cache, no-store, must-revalidate")
		return
	}
	// Use explicit cache policy if set in app.json action
	if cache != "" {
		switch cache {
		case "immutable":
			c.Header("Cache-Control", "public, max-age=31536000, immutable")
		case "static":
			c.Header("Cache-Control", "public, max-age=300")
		case "revalidate":
			c.Header("Cache-Control", "no-cache, must-revalidate")
			if info, err := os.Stat(path); err == nil {
				etag := fmt.Sprintf(`"%x"`, info.ModTime().UnixNano())
				c.Header("ETag", etag)
				if match := c.GetHeader("If-None-Match"); match == etag {
					c.AbortWithStatus(http.StatusNotModified)
				}
			}
		case "none":
			c.Header("Cache-Control", "no-cache, no-store, must-revalidate")
		}
		return
	}
	// Auto-detect cache policy from file path
	if strings.HasSuffix(path, ".html") {
		// HTML files should revalidate on every request
		// Add ETag based on file modification time for proper cache validation
		c.Header("Cache-Control", "no-cache, must-revalidate")
		if info, err := os.Stat(path); err == nil {
			etag := fmt.Sprintf(`"%x"`, info.ModTime().UnixNano())
			c.Header("ETag", etag)
			// Check If-None-Match header for conditional request
			if match := c.GetHeader("If-None-Match"); match == etag {
				c.AbortWithStatus(http.StatusNotModified)
				return
			}
		}
	} else if match_react.MatchString(path) {
		// debug("Web asking browser to long term cache %q", path)
		c.Header("Cache-Control", "public, max-age=31536000, immutable")
	} else {
		// debug("Web asking browser to short term cache %q", path)
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

// Serve HTML file with dynamic Open Graph meta tags
func web_serve_file_with_opengraph(c *gin.Context, a *App, av *AppVersion, aa *AppAction, e *Entity, file string) bool {
	// Get owner for database access - use entity owner if available
	var owner *User
	if e != nil {
		owner = user_owning_entity(e.ID)
	}

	// Set up database connection if needed
	if av.Database.File != "" && owner != nil {
		owner.db = db_app(owner, a)
		if owner.db != nil {
			defer owner.db.close()
		}
	}

	// Call Starlark function to get OG data
	s := av.starlark()
	s.set("app", a)
	s.set("user", owner) // Use owner for database access
	s.set("owner", owner)

	// Build parameters dict for the function
	params := sl.NewDict(len(aa.parameters))
	for k, v := range aa.parameters {
		params.SetKey(sl.String(k), sl.String(v))
	}

	// Add entity info if present
	if e != nil {
		params.SetKey(sl.String("entity"), sl.String(e.ID))
		params.SetKey(sl.String("fingerprint"), sl.String(e.Fingerprint))
	}

	// Build request URL for og:url
	scheme := "https"
	if !web_https {
		scheme = "http"
	}
	url := scheme + "://" + c.Request.Host + c.Request.URL.Path

	result, err := s.call(aa.OpenGraph, sl.Tuple{params})
	if err != nil {
		debug("OpenGraph function %q error: %v", aa.OpenGraph, err)
		return false
	}

	// Convert result to map
	og := sl_decode_map(result)
	if og == nil {
		debug("OpenGraph function %q returned invalid data", aa.OpenGraph)
		return false
	}

	// Read HTML file
	html := file_read(file)
	if html == nil {
		return false
	}
	content := string(html)

	// Replace OG meta tags
	if title, ok := og["title"].(string); ok && title != "" {
		content = regexp_replace_meta(content, "og:title", title)
		content = regexp_replace_meta(content, "twitter:title", title)
		content = regexp_replace_tag(content, "title", title)
		content = regexp_replace_meta_name(content, "title", title)
	}
	if desc, ok := og["description"].(string); ok && desc != "" {
		content = regexp_replace_meta(content, "og:description", desc)
		content = regexp_replace_meta(content, "twitter:description", desc)
		content = regexp_replace_meta_name(content, "description", desc)
	}
	if image, ok := og["image"].(string); ok && image != "" {
		// Add og:image if not already present
		if !strings.Contains(content, `property="og:image"`) {
			content = strings.Replace(content, `<meta property="og:description"`,
				`<meta property="og:image" content="`+image+`" />`+"\n    "+`<meta property="og:description"`, 1)
		} else {
			content = regexp_replace_meta(content, "og:image", image)
		}
		// Add twitter:image if not already present
		if !strings.Contains(content, `property="twitter:image"`) {
			content = strings.Replace(content, `<meta property="twitter:description"`,
				`<meta property="twitter:image" content="`+image+`" />`+"\n    "+`<meta property="twitter:description"`, 1)
		} else {
			content = regexp_replace_meta(content, "twitter:image", image)
		}
	}
	if ogType, ok := og["type"].(string); ok && ogType != "" {
		content = regexp_replace_meta(content, "og:type", ogType)
	}

	// Always set og:url to current URL
	if !strings.Contains(content, `property="og:url"`) {
		content = strings.Replace(content, `<meta property="og:type"`,
			`<meta property="og:url" content="`+url+`" />`+"\n    "+`<meta property="og:type"`, 1)
	} else {
		content = regexp_replace_meta(content, "og:url", url)
	}

	// Inject routing meta tags
	content = web_inject_meta_tags(c, e, content)

	// Serve modified content
	c.Header("Content-Type", "text/html; charset=utf-8")
	web_cache_static(c, file, aa.Cache)
	c.String(http.StatusOK, content)
	return true
}

// Check if a URL segment looks like an entity identifier (fingerprint or full ID)
var entity_segment_re = regexp.MustCompile(`^[1-9A-HJ-NP-Za-km-z]{9}$|^[1-9A-HJ-NP-Za-km-z]{50,51}$`)

func is_entity_segment(s string) bool {
	return entity_segment_re.MatchString(s)
}

// Build routing meta tags and inject them after <head>
func web_inject_meta_tags(c *gin.Context, e *Entity, content string) string {
	var tags []string
	if app := c.GetString("mochi_app_path"); app != "" {
		tags = append(tags, `<meta name="mochi:app" content="`+app+`">`)
	}
	if e != nil {
		tags = append(tags, `<meta name="mochi:class" content="`+e.Class+`">`)
		tags = append(tags, `<meta name="mochi:entity" content="`+e.ID+`">`)
		tags = append(tags, `<meta name="mochi:fingerprint" content="`+e.Fingerprint+`">`)
	} else if seg := c.GetString("mochi_entity_segment"); seg != "" {
		tags = append(tags, `<meta name="mochi:fingerprint" content="`+seg+`">`)
	}
	if dm := c.GetString("domain_method"); dm == "entity" || dm == "app" {
		tags = append(tags, `<meta name="mochi:domain">`)
	}
	if len(tags) > 0 {
		injection := "\n    " + strings.Join(tags, "\n    ")
		content = strings.Replace(content, "<head>", "<head>"+injection, 1)
	}
	return content
}

// Serve an HTML file with routing meta tags injected after <head>
func web_serve_html(c *gin.Context, a *App, av *AppVersion, aa *AppAction, e *Entity, file string) {
	// Try OG injection first (it also injects routing meta tags)
	if aa.OpenGraph != "" {
		if web_serve_file_with_opengraph(c, a, av, aa, e, file) {
			return
		}
	}

	// Read HTML file
	html := file_read(file)
	if html == nil {
		c.String(http.StatusNotFound, "File not found")
		return
	}
	content := web_inject_meta_tags(c, e, string(html))

	c.Header("Content-Type", "text/html; charset=utf-8")
	web_cache_static(c, file, aa.Cache)
	c.String(http.StatusOK, content)
}

// Replace Open Graph meta tag content
func regexp_replace_meta(html, property, value string) string {
	// Escape HTML in value
	value = strings.ReplaceAll(value, `"`, `&quot;`)
	value = strings.ReplaceAll(value, `<`, `&lt;`)
	value = strings.ReplaceAll(value, `>`, `&gt;`)

	pattern := regexp.MustCompile(`<meta\s+property="` + regexp.QuoteMeta(property) + `"\s+content="[^"]*"\s*/?>`)
	replacement := `<meta property="` + property + `" content="` + value + `" />`
	return pattern.ReplaceAllString(html, replacement)
}

// Replace meta tag with name attribute
func regexp_replace_meta_name(html, name, value string) string {
	value = strings.ReplaceAll(value, `"`, `&quot;`)
	value = strings.ReplaceAll(value, `<`, `&lt;`)
	value = strings.ReplaceAll(value, `>`, `&gt;`)

	pattern := regexp.MustCompile(`<meta\s+name="` + regexp.QuoteMeta(name) + `"\s+content="[^"]*"\s*/?>`)
	replacement := `<meta name="` + name + `" content="` + value + `" />`
	return pattern.ReplaceAllString(html, replacement)
}

// Replace HTML tag content
func regexp_replace_tag(html, tag, value string) string {
	value = strings.ReplaceAll(value, `<`, `&lt;`)
	value = strings.ReplaceAll(value, `>`, `&gt;`)

	pattern := regexp.MustCompile(`<` + regexp.QuoteMeta(tag) + `>[^<]*</` + regexp.QuoteMeta(tag) + `>`)
	replacement := `<` + tag + `>` + value + `</` + tag + `>`
	return pattern.ReplaceAllString(html, replacement)
}

// Handle login begin: check user's required auth methods (POST with JSON)
// Returns the methods required for this user, without sending any codes.
func web_login_begin(c *gin.Context) {
	var input struct {
		Email string `json:"email"`
	}
	if err := c.ShouldBindJSON(&input); err != nil || input.Email == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	if !email_valid(input.Email) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_email"})
		return
	}

	// Check if user exists
	db := db_open("db/users.db")
	user := user_by_username(input.Email)

	if user == nil {
		// User doesn't exist - check if signup is enabled
		if !setting_signup_enabled() {
			c.JSON(http.StatusForbidden, gin.H{"error": "signup_disabled", "message": "New user signup is disabled."})
			return
		}
		// New user - default to email method
		c.JSON(http.StatusOK, gin.H{
			"methods": []string{"email"},
			"new":     true,
		})
		return
	}

	// Get user's required methods
	methods := []string{"email"}
	if user.Methods != "" {
		methods = strings.Split(user.Methods, ",")
		for i := range methods {
			methods[i] = strings.TrimSpace(methods[i])
		}
	}

	// Check if user has passkey as an alternative login method
	has_passkey := false
	count, _ := db.row("select count(*) as count from credentials where user=?", user.ID)
	if count != nil && count["count"].(int64) > 0 {
		has_passkey = true
	}

	c.JSON(http.StatusOK, gin.H{
		"methods":     methods,
		"has_passkey": has_passkey,
	})
}

func web_identity_get(c *gin.Context) {
	user_by_id_allow_no_identity := func(id int) *User {
		db := db_open("db/users.db")
		var user User
		if !db.scan(&user, "select id, username, role, methods, status from users where id=?", id) {
			return nil
		}
		user.Preferences = user_preferences_load(&user)
		user.Identity = user.identity()
		return &user
	}

	u := web_auth(c)

	// If no cookie auth, try Bearer token authentication
	if u == nil {
		auth_header := c.GetHeader("Authorization")
		if strings.HasPrefix(auth_header, "Bearer ") {
			bearer := strings.TrimPrefix(auth_header, "Bearer ")
			if strings.HasPrefix(bearer, "mochi-") {
				// API token authentication
				api_token := token_validate(bearer)
				if api_token != nil {
					u = user_by_id_allow_no_identity(api_token.User)
				}
			} else {
				// JWT authentication
				if uid, err := jwt_verify(bearer); err == nil && uid > 0 {
					if user := user_by_id_allow_no_identity(uid); user != nil {
						u = user
					}
				}
			}
		}
	}

	if u == nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Authentication required"})
		return
	}

	response := gin.H{
		"user": gin.H{
			"email": u.Username,
			"name":  "", // Will be populated below if identity exists
		},
	}

	if u.Identity != nil {
		response["user"].(gin.H)["name"] = u.Identity.Name
		response["identity"] = gin.H{
			"id":          u.Identity.ID,
			"name":        u.Identity.Name,
			"privacy":     u.Identity.Privacy,
			"fingerprint": u.Identity.Fingerprint,
		}
	}

	c.JSON(http.StatusOK, response)
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
	session := web_cookie_get(c, "session", "")
	if session != "" {
		// Get user before deleting session for audit log
		user := web_auth(c)
		login_delete(session)
		if user != nil {
			audit_logout(user.Username, rate_limit_client_ip(c))
		}
	}
	web_cookie_unset(c, "session")
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// Handle app paths
func web_path(c *gin.Context) {
	//debug("Web path %q", c.Request.URL.Path)

	// Get user for path-based routing preferences
	user := web_auth(c)

	// During bootstrap, show setup page until Login and Home are installed
	if !apps_bootstrap_ready {
		c.Header("Refresh", "2")
		c.Data(http.StatusOK, "text/html", []byte(`<!DOCTYPE html>
<html>
<head><title>Setting up</title></head>
<body style="font-family: system-ui, sans-serif; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; background: #f5f5f5;">
<div style="text-align: center;">
<h1 style="font-weight: normal; color: #333;">Setting up</h1>
<p style="color: #666;">Installing system apps, please wait...</p>
</div>
</body>
</html>`))
		return
	}

	// Check for domain-based routing first (skip /_/ paths which are core endpoints)
	if method, exists := c.Get("domain_method"); exists && method.(string) != "" && !strings.HasPrefix(c.Request.URL.Path, "/_/") {
		target := c.GetString("domain_target")
		remaining := c.GetString("domain_remaining")
		action := strings.TrimPrefix(remaining, "/")

		switch method.(string) {
		case "app":
			a := app_by_any(user, target)
			if a == nil {
				c.JSON(http.StatusNotFound, gin.H{"error": "App not found"})
				return
			}
			// Redirect to add trailing slash for correct relative path resolution
			if remaining == "" && !strings.HasSuffix(c.Request.URL.Path, "/") {
				c.Redirect(http.StatusMovedPermanently, c.Request.URL.Path+"/")
				return
			}
			web_action(c, a, action, nil)
			return

		case "redirect":
			c.Redirect(http.StatusFound, target+remaining)
			return

		case "entity":
			e := entity_by_any(target)
			if e == nil {
				c.JSON(http.StatusNotFound, gin.H{"error": "Entity not found"})
				return
			}
			// Use entity owner's preferences for class routing
			owner := user_owning_entity(e.ID)
			a := class_app_for(owner, e.Class)
			if a == nil {
				c.JSON(http.StatusNotFound, gin.H{"error": "No app for entity"})
				return
			}
			// Redirect to add trailing slash for correct relative path resolution
			if remaining == "" && !strings.HasSuffix(c.Request.URL.Path, "/") {
				c.Redirect(http.StatusMovedPermanently, c.Request.URL.Path+"/")
				return
			}
			web_action(c, a, action, e)
			return

		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Unknown route method"})
			return
		}
	}

	raw := strings.Trim(c.Request.URL.Path, "/")

	// Check for app that handles root path
	if raw == "" {
		if a := app_by_root(user); a != nil {
			web_action(c, a, "", nil)
			return
		}
		c.JSON(http.StatusNotFound, gin.H{"error": "No root app configured"})
		return
	}

	segments := strings.Split(raw, "/")
	first := segments[0]

	// Check for app matching first segment (user preferences, then system defaults, then fallback)
	a := app_for_path(user, first)
	if a != nil {
		// Set app path segment so HTML serving can inject meta tags
		c.Set("mochi_app_path", first)

		// Redirect /app to /app/ for correct relative path resolution
		if len(segments) == 1 && !strings.HasSuffix(c.Request.URL.Path, "/") {
			c.Redirect(http.StatusMovedPermanently, "/"+first+"/")
			return
		}

		second := ""
		if len(segments) > 1 {
			second = segments[1]
		}

		// Route on /<app>/<entity>[/<action...>]
		e := entity_by_any(second)
		if e != nil {
			// Construct action with entity fingerprint prefix, same as direct entity routing
			action := e.Fingerprint
			if len(segments) > 2 {
				action = e.Fingerprint + "/" + strings.Join(segments[2:], "/")
			}
			if web_action(c, a, action, e) {
				return
			}
		} else if is_entity_segment(second) {
			// Remote entity not known locally — pass identifier for meta tag injection
			c.Set("mochi_entity_segment", second)
		}

		// Route on /<app>/<action...>
		class_action := strings.Join(segments[1:], "/")

		web_action(c, a, class_action, nil)
		return
	}

	// Check for entity matching first segment
	e := entity_by_any(first)
	if e != nil {
		// Use entity owner's preferences for class routing
		owner := user_owning_entity(e.ID)
		a := class_app_for(owner, e.Class)
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
	if a := app_by_root(user); a != nil {
		web_action(c, a, raw, nil)
		return
	}
	c.JSON(http.StatusNotFound, gin.H{"error": "Not found"})
}

// Return P2P connection info for this server
func web_p2p_info(c *gin.Context) {
	addresses := []string{}
	for _, addr := range p2p_me.Addrs() {
		addresses = append(addresses, addr.String()+"/p2p/"+p2p_id)
	}
	c.JSON(http.StatusOK, gin.H{
		"peer":      p2p_id,
		"addresses": addresses,
	})
}

func web_ping(c *gin.Context) {
	c.String(http.StatusOK, "pong")
}

// Start the web server
func web_start() {
	listen := ini_string("web", "listen", "")
	ports := ini_ints_commas("web", "ports")
	if len(ports) == 0 {
		// Fallback to legacy single port config
		port := ini_int("web", "port", 80)
		if port == 0 {
			return
		}
		ports = []int{port}
	}

	if !ini_bool("web", "debug", false) {
		gin.SetMode(gin.ReleaseMode)
	}
	r := gin.Default()
	r.SetTrustedProxies(nil)
	r.Use(web_security_headers)
	r.Use(rate_limit_api_middleware)
	r.Use(domains_middleware())
	r.RedirectTrailingSlash = false

	// Auth endpoints (grouped under /_/auth/)
	r.POST("/_/auth/begin", rate_limit_login_middleware, web_login_begin)
	r.POST("/_/auth/code", rate_limit_login_middleware, web_login_code)
	r.POST("/_/auth/verify", rate_limit_login_middleware, web_login_verify)
	r.POST("/_/auth/totp", rate_limit_login_middleware, web_auth_totp)
	r.POST("/_/auth/methods", rate_limit_login_middleware, web_auth_mfa)
	r.POST("/_/auth/passkey/begin", rate_limit_login_middleware, web_passkey_login_begin)
	r.POST("/_/auth/passkey/finish", rate_limit_login_middleware, web_passkey_login_finish)
	r.POST("/_/auth/recovery", rate_limit_login_middleware, web_recovery_login)
	r.GET("/_/auth/methods", web_auth_methods)

	// Other system endpoints
	r.GET("/_/identity", web_identity_get)
	r.POST("/_/identity", web_login_identity)
	r.POST("/_/logout", web_logout)
	r.GET("/_/ping", web_ping)
	r.GET("/_/p2p/info", web_p2p_info)
	r.GET("/sw.js", webpush_service_worker)
	r.GET("/_/websocket", websocket_connection)

	// All other paths are handled by web_path()
	r.NoRoute(web_path)

	// Check if HTTPS should be enabled (port 443 with domains configured)
	domains := domain_list()
	https := false
	for _, port := range ports {
		if port == 443 && len(domains) > 0 {
			https = true
			break
		}
	}

	// Start listeners for each port
	for i, port := range ports {
		last := i == len(ports)-1

		if port == 443 {
			if len(domains) == 0 {
				warn("Port 443 configured but no domains in database, skipping HTTPS")
				continue
			}
			web_https = true
			tls_config := &tls.Config{
				GetCertificate: domains_get_certificate,
			}
			info("Web listening on %s:443 (HTTPS)", listen)
			if last {
				must(autotls.RunWithManagerAndTLSConfig(r, domains_acme_manager, tls_config))
			} else {
				go must(autotls.RunWithManagerAndTLSConfig(r, domains_acme_manager, tls_config))
			}
		} else {
			addr := fmt.Sprintf("%s:%d", listen, port)
			if https {
				info("Web listening on %s (HTTP, ACME challenges)", addr)
			} else {
				info("Web listening on %s (HTTP)", addr)
			}
			if last {
				must(r.Run(addr))
			} else {
				go must(r.Run(addr))
			}
		}
	}
}

// Serve an attachment or thumbnail
func web_serve_attachment(c *gin.Context, app *App, user *User, entity, id string, thumbnail bool) bool {
	if !valid(id, "id") {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid attachment ID"})
		return true
	}

	// If no local owner, try to fetch from remote entity (e.g., bookmarked wikis)
	if user == nil {
		if entity == "" || (!valid(entity, "entity") && !valid(entity, "fingerprint")) {
			c.JSON(http.StatusNotFound, gin.H{"error": "Entity not found"})
			return true
		}
		return web_serve_attachment_remote(c, app, entity, id, thumbnail)
	}

	db := db_app_system(user, app)
	if db == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return true
	}

	var att Attachment
	if !db.scan(&att, "select * from attachments where id = ?", id) {
		// Attachment record not in local database — try remote if entity is available
		// (e.g., subscribed feed whose attachments haven't been piggybacked)
		if entity != "" && (valid(entity, "entity") || valid(entity, "fingerprint")) {
			return web_serve_attachment_remote(c, app, entity, id, thumbnail)
		}
		c.JSON(http.StatusNotFound, gin.H{"error": "Attachment not found"})
		return true
	}

	// Get file path - always use local storage, fetching from remote if needed
	path := filepath.Join(data_dir, attachment_path(user.ID, app.id, att.ID, att.Name))
	if !file_exists(path) {
		// Prefer route entity (e.g., feed ID from URL) over stored entity (may be post ID)
		fetch_entity := entity
		if fetch_entity == "" {
			fetch_entity = att.Entity
		}
		if fetch_entity != "" {
			// Fetch from remote and store locally
			cached := attachment_fetch_remote(app, fetch_entity, id)
			if cached == "" {
				c.JSON(http.StatusNotFound, gin.H{"error": "File not found"})
				return true
			}
			file_copy(cached, path)
			// Clear entity so future requests serve from local storage
			db.exec(`update attachments set entity = '' where id = ?`, id)
			info("Attachment %s fetched and stored locally on demand", id)
		} else {
			c.JSON(http.StatusNotFound, gin.H{"error": "File not found"})
			return true
		}
	}

	// Use ETag for cache validation so deleted files don't persist in browser cache
	etag := fmt.Sprintf(`"%s"`, att.ID)
	c.Header("ETag", etag)
	c.Header("Cache-Control", "private, must-revalidate")

	// Check If-None-Match for conditional requests
	if match := c.GetHeader("If-None-Match"); match == etag {
		c.Status(http.StatusNotModified)
		return true
	}

	if thumbnail {
		if thumb, err := thumbnail_create(path); err == nil && thumb != "" {
			c.File(thumb)
			return true
		}
	}

	c.Header("Content-Disposition", fmt.Sprintf("inline; filename=%q", att.Name))
	c.File(path)
	return true
}

// Serve an attachment from a remote entity (for bookmarked wikis, etc.)
func web_serve_attachment_remote(c *gin.Context, app *App, entity, id string, thumbnail bool) bool {
	// Fetch from remote (thumbnail generated on remote side if requested)
	path := attachment_fetch_remote(app, entity, id, thumbnail)
	if path == "" {
		c.JSON(http.StatusNotFound, gin.H{"error": "Attachment not found"})
		return true
	}

	// ETag cache validation
	suffix := ""
	if thumbnail {
		suffix = "-thumb"
	}
	etag := fmt.Sprintf(`"%s%s"`, id, suffix)
	c.Header("ETag", etag)
	c.Header("Cache-Control", "private, must-revalidate")

	if match := c.GetHeader("If-None-Match"); match == etag {
		c.Status(http.StatusNotModified)
		return true
	}

	c.File(path)
	return true
}

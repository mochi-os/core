// Mochi server: Shell page for app isolation
// Copyright Alistair Cunningham 2026

package main

import (
	_ "embed"
	"net/http"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
)

var (
	shell_enabled = false // Controlled by [web] shell config flag
)

//go:embed shell.html
var shell_html string

//go:embed shell.js
var shell_js string

// shell_init reads the shell config flag
func shell_init() {
	shell_enabled = ini_bool("web", "shell", false)
}

// web_should_serve_shell returns true when the request should get the shell page
// instead of the app HTML directly
func web_should_serve_shell(c *gin.Context) bool {
	if !shell_enabled {
		return false
	}

	// Only intercept top-level document navigations
	dest := c.GetHeader("Sec-Fetch-Dest")
	if dest != "document" {
		return false
	}

	// Must accept HTML
	if !strings.Contains(c.GetHeader("Accept"), "text/html") {
		return false
	}

	// Skip system endpoints and static assets
	path := c.Request.URL.Path
	if strings.HasPrefix(path, "/_/") {
		return false
	}

	// User must be authenticated
	session := web_cookie_get(c, "session", "")
	if session == "" {
		return false
	}
	user := user_by_login(session)
	if user == nil {
		return false
	}

	// User must have identity
	if user.identity() == nil {
		return false
	}

	return true
}

// web_serve_shell renders the shell page with the menu app and a sandboxed iframe
func web_serve_shell(c *gin.Context, app_id string) {
	session := web_cookie_get(c, "session", "")
	user := user_by_login(session)
	if user == nil {
		// Shouldn't happen — web_should_serve_shell already checked
		c.Redirect(http.StatusFound, "/")
		return
	}

	// Get menu app to resolve its asset paths
	menu := shell_menu_app(user)

	// Get notification count for the menu app
	notif_token := ""
	notif_app := app_for_path(user, "notifications")
	if notif_app != nil {
		notif_token = auth_create_app_token(user.ID, session, notif_app.id)
	}

	// Get user profile info
	name := ""
	if ident := user.identity(); ident != nil {
		name = ident.Name
	}

	// Build the shell page from template
	page := shell_html

	// Inject values
	page = strings.Replace(page, "{{IFRAME_SRC}}", escape_attr(c.Request.URL.Path), 1)
	page = strings.Replace(page, "{{APP_ID}}", escape_attr(app_id), 1)
	page = strings.Replace(page, "{{NOTIF_TOKEN}}", notif_token, 1)
	page = strings.Replace(page, "{{USER_NAME}}", escape_attr(name), 1)
	page = strings.Replace(page, "{{SHELL_JS}}", shell_js, 1)

	// Menu app assets
	menu_js, menu_css := "", ""
	if menu != nil {
		menu_js, menu_css = shell_menu_assets(menu, user)
	}
	page = strings.Replace(page, "{{MENU_JS}}", menu_js, 1)
	page = strings.Replace(page, "{{MENU_CSS}}", menu_css, 1)

	// Security headers: shell cannot be framed
	c.Header("X-Frame-Options", "DENY")
	c.Header("Cross-Origin-Opener-Policy", "same-origin")
	c.Header("Content-Type", "text/html; charset=utf-8")
	c.Header("Cache-Control", "no-store")
	c.String(http.StatusOK, page)
}

// shell_menu_app returns the menu app for the given user
func shell_menu_app(user *User) *App {
	return app_for_path(user, "menu")
}

// shell_menu_assets returns the JS and CSS paths for the menu app's built assets
func shell_menu_assets(a *App, user *User) (string, string) {
	av := a.active(user)
	if av == nil {
		return "", ""
	}

	// Look for the entry point JS and CSS in the dist/assets directory
	js_path := ""
	css_path := ""

	assets_dir := av.base + "/web/dist/assets"
	entries, err := os.ReadDir(assets_dir)
	if err != nil {
		return "", ""
	}

	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, "index-") || strings.HasPrefix(name, "main-") {
			if strings.HasSuffix(name, ".js") && js_path == "" {
				js_path = "/menu/assets/" + name
			}
			if strings.HasSuffix(name, ".css") && css_path == "" {
				css_path = "/menu/assets/" + name
			}
		}
	}

	return js_path, css_path
}

// web_shell_token handles POST /_/token — returns a per-app JWT token
func web_shell_token(c *gin.Context) {
	user := web_auth(c)
	if user == nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Authentication required"})
		return
	}

	var input struct {
		App string `json:"app"`
	}
	if err := c.ShouldBindJSON(&input); err != nil || input.App == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "app required"})
		return
	}

	// Resolve the app name to an app ID
	a := app_for_path(user, input.App)
	if a == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "App not found"})
		return
	}

	session := web_cookie_get(c, "session", "")
	token := auth_create_app_token(user.ID, session, a.id)
	if token == "" {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create token"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"token": token})
}

// web_is_iframe_request returns true if the request is from a sandboxed iframe
func web_is_iframe_request(c *gin.Context) bool {
	if !shell_enabled {
		return false
	}
	return c.GetHeader("Sec-Fetch-Dest") == "iframe"
}

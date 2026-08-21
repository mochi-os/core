// Mochi server: Shell page for app isolation
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"crypto/rand"
	"encoding/base64"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// shell_nonce returns a fresh 128-bit random value, base64-url-encoded,
// suitable for use as a Content-Security-Policy script nonce.
func shell_nonce() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// Should never happen; fall back to time-derived value rather
		// than serving without a nonce (which would block all scripts).
		return base64.RawURLEncoding.EncodeToString([]byte(time.Now().Format("20060102150405.000000000")))
	}
	return base64.RawURLEncoding.EncodeToString(b[:])
}

// Shell template files (shell.html, shell.js, iframe-shim.js) live in the menu
// app's build output, so frontend rebuilds are picked up without a restart.
// shell_file_load caches by mtime.

type shell_file_entry struct {
	mtime   time.Time
	content string
}

var (
	shell_files_mu    sync.RWMutex
	shell_files_cache = map[string]shell_file_entry{}
)

func shell_file_load(path string) (string, error) {
	information, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	mtime := information.ModTime()

	shell_files_mu.RLock()
	cached, ok := shell_files_cache[path]
	shell_files_mu.RUnlock()
	if ok && cached.mtime.Equal(mtime) {
		return cached.content, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	content := string(data)

	shell_files_mu.Lock()
	shell_files_cache[path] = shell_file_entry{mtime: mtime, content: content}
	shell_files_mu.Unlock()
	return content, nil
}

// shell_wrap_candidate reports whether a request is the kind of top-level
// navigation the shell wraps, independent of who is making it.
// web_should_serve_shell adds the authentication requirements on top.
func shell_wrap_candidate(c *gin.Context) bool {
	// Sec-Fetch-Dest classifies the request: "document" for top-level navigations,
	// "iframe" for iframe loads, "empty" for fetch. Some browsers and proxies
	// strip it, so treat missing as a possible navigation and reject explicit
	// non-document.
	dest := c.GetHeader("Sec-Fetch-Dest")
	if dest != "" && dest != "document" {
		return false
	}

	// Only GET is a top-level page navigation
	if c.Request.Method != http.MethodGet {
		return false
	}

	// Must accept HTML
	if !strings.Contains(c.GetHeader("Accept"), "text/html") {
		return false
	}

	// Iframe loads carry _shell=1, but honour it only when Sec-Fetch-Dest is
	// absent. On an explicit "document" the request may be an un-sandboxed popup;
	// serving raw app HTML there runs the app's bundle same-origin and
	// cookie-bearing, where POST /_/token mints a JWT for every installed app.
	if dest == "" && c.Query("_shell") == "1" {
		return false
	}

	// Skip system endpoints and static assets
	path := c.Request.URL.Path
	if strings.HasPrefix(path, "/_/") {
		return false
	}

	// The login app is always full-page: wrapping loads its interstitials into the
	// sandboxed iframe with cookies stripped, where they loop. Resolved through
	// app_login_owns, not a literal "login": a rename would loop, and an app that
	// squatted the path would get its bundle running top-level and cookie-bearing.
	if app_login_owns(strings.Trim(path, "/")) {
		return false
	}

	// Resource routes (attachment downloads, git Smart-HTTP) never take the shell:
	// the sandboxed iframe's opaque origin breaks Chrome's PDF viewer. The path
	// list lives with web_resource_guard, which sandboxes anything on these paths
	// that comes back as an executable document, so exemption and mitigation
	// cannot drift.
	if shell_resource_path(path) {
		return false
	}

	return true
}

// web_should_serve_shell returns true when the request should get the shell page
// instead of the app HTML directly
func web_should_serve_shell(c *gin.Context) bool {
	if !shell_wrap_candidate(c) {
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

	// Get menu app to resolve its asset paths.
	// shell.html and shell.js live in the menu's dist; if the menu is
	// missing or unbuilt, the shell can't be assembled.
	menu := shell_menu_app(user)
	if menu == nil {
		info("shell: menu app not installed")
		respond_text(c, http.StatusInternalServerError, "errors.shell_unavailable", nil)
		return
	}
	av := menu.active(user)
	if av == nil {
		info("shell: menu app has no active version")
		respond_text(c, http.StatusInternalServerError, "errors.shell_unavailable", nil)
		return
	}

	shell_html, err := shell_file_load(av.base + "/web/dist/shell.html")
	if err != nil {
		info("shell: failed to load shell.html: %v", err)
		respond_text(c, http.StatusInternalServerError, "errors.shell_unavailable", nil)
		return
	}
	shell_js, err := shell_file_load(av.base + "/web/dist/shell.js")
	if err != nil {
		info("shell: failed to load shell.js: %v", err)
		respond_text(c, http.StatusInternalServerError, "errors.shell_unavailable", nil)
		return
	}

	// Per-request nonce — gates all inline scripts via Content-Security-Policy.
	nonce := shell_nonce()

	// Build the shell page from template.
	// Only static, server-controlled values are injected — no user data.
	html_class, appearance_script := web_user_appearance_attrs(user, nonce)
	// Menu JS is an external <script src="..."> from same origin, so 'self'
	// in script-src admits it without needing the nonce.
	menu_js, menu_css := shell_menu_assets(menu, user)
	page := strings.NewReplacer(
		"{{HTML_CLASS}}", html_class,
		"{{APPEARANCE_SCRIPT}}", appearance_script,
		"{{THEME_STYLE}}", web_user_theme_style(user),
		"{{NONCE}}", nonce,
		"{{SHELL_JS}}", shell_js,
		"{{MENU_JS}}", menu_js,
		"{{MENU_CSS}}", menu_css,
	).Replace(shell_html)

	// Clear the stale mochi-theme cookie. Secure comes from the connection itself:
	// nothing sits in front of this server, so X-Forwarded-Proto would be caller
	// input.
	secure := c.Request.TLS != nil
	c.SetCookie("mochi-theme", "", -1, "/", "", secure, false)

	// Security headers
	c.Header("X-Frame-Options", "DENY")
	c.Header("Cross-Origin-Opener-Policy", "same-origin")
	// Strict CSP: scripts only via nonce or same-origin (covers menu's
	// hashed bundle under /menu/assets/). Styles allow 'unsafe-inline'
	// for the inline <style> block in shell.html and the server-injected
	// style="..." attribute on <html>. Iframes load same-origin app pages.
	c.Header("Content-Security-Policy",
		"default-src 'self'; "+
			"script-src 'self' 'nonce-"+nonce+"'; "+
			"style-src 'self' 'unsafe-inline'; "+
			"img-src 'self' data: blob:; "+
			"font-src 'self' data:; "+
			"connect-src 'self'; "+
			"frame-src 'self'; "+
			"base-uri 'none'; "+
			"form-action 'self'")
	c.Header("Content-Type", "text/html; charset=utf-8")
	c.Header("Cache-Control", "no-store")
	c.String(http.StatusOK, page)
}

func shell_menu_app(user *User) *App {
	return app_for_path(user, "menu")
}

// shell_iframe_shim_load returns the JS shim injected into iframe-served app
// HTML: in-memory fallbacks for cookies and storage, unavailable in a sandboxed
// iframe without allow-same-origin. Empty string means skip injection.
func shell_iframe_shim_load(user *User) string {
	menu := shell_menu_app(user)
	if menu == nil {
		return ""
	}
	av := menu.active(user)
	if av == nil {
		return ""
	}
	content, err := shell_file_load(av.base + "/web/dist/iframe-shim.js")
	if err != nil {
		info("shell: failed to load iframe-shim.js: %v", err)
		return ""
	}
	return content
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

	// Wrap in HTML tags
	js_tag := ""
	css_tag := ""
	if js_path != "" {
		js_tag = `<script type="module" src="` + js_path + `"></script>`
	}
	if css_path != "" {
		css_tag = `<link rel="stylesheet" href="` + css_path + `">`
	}

	return js_tag, css_tag
}

// web_shell_token handles POST /_/token — returns a per-app JWT token
func web_shell_token(c *gin.Context) {
	user := web_auth(c)
	if user == nil {
		respond_error(c, http.StatusUnauthorized, "authentication_required", "errors.authentication_required", nil)
		return
	}

	var input struct {
		App string `json:"app"`
	}
	if err := c.ShouldBindJSON(&input); err != nil {
		respond_error(c, http.StatusBadRequest, "app_required", "errors.app_required", nil)
		return
	}

	// Resolve the app path to an app
	a := app_for_path(user, input.App)
	if a == nil {
		respond_error(c, http.StatusNotFound, "app_not_found", "errors.app_not_found", nil)
		return
	}

	session := web_cookie_get(c, "session", "")
	token := auth_create_app_token(user.UID, session, a.id)
	if token == "" {
		respond_error(c, http.StatusInternalServerError, "failed_to_create_token", "errors.failed_to_create_token", nil)
		return
	}

	c.JSON(http.StatusOK, gin.H{"token": token, "app": a.id})
}

// web_shell_init handles POST /_/shell — returns shell bootstrap config.
// Called once by the shell page on load. Protected by session cookie
// (sandboxed iframe apps cannot call this).
func web_shell_init(c *gin.Context) {
	user := web_auth(c)
	if user == nil {
		respond_error(c, http.StatusUnauthorized, "authentication_required", "errors.authentication_required", nil)
		return
	}

	session := web_cookie_get(c, "session", "")

	// Menu app token
	menu_token := ""
	if menu := shell_menu_app(user); menu != nil {
		menu_token = auth_create_app_token(user.UID, session, menu.id)
	}

	// Domain routing context — resolve from Referer since /_/ paths
	// skip the domain routing middleware.
	result := gin.H{"menuToken": menu_token}
	if referer := c.GetHeader("Referer"); referer != "" {
		if u, err := url.Parse(referer); err == nil {
			if match := domain_match(u.Hostname(), u.Path); match != nil {
				domain := gin.H{"method": match.route.Method}
				if match.route.Method == "entity" {
					if e := entity_by_any(match.route.Target); e != nil {
						domain["entity"] = e.ID
						domain["fingerprint"] = e.Fingerprint
						domain["class"] = e.Class
					}
				}
				result["domain"] = domain
			}
		}
	}

	// Locale preferences for formatting
	locale := gin.H{}
	for _, key := range []string{"date_format", "time_format", "timestamp_display", "week_start", "number_format", "units", "timezone"} {
		if v, ok := user.Preferences[key]; ok {
			locale[key] = v
		} else {
			locale[key] = "auto"
		}
	}
	result["locale"] = locale

	// Active i18n language for the user (BCP 47). Logged-in users use their
	// `language` preference; falls through to Accept-Language for the brief
	// anonymous-public window before login completes.
	result["language"] = request_language(c, user)

	// The user's resolved theme, as the same CSS declarations injected into the
	// shell page at load. The shell re-reads this when an app reports the
	// preference changed, so the values styling the trusted chrome always come
	// from the user's own preference rather than from the reporting app.
	result["theme"] = web_user_theme_declarations(user)

	// The appearance PREFERENCE, not the light/dark it resolved to. "auto"
	// means follow the system, and the system can change while the tab stays
	// open, so the shell needs the preference itself to know whether to keep
	// re-resolving.
	result["appearance"] = user_preference_get(user, "appearance", "auto")

	// Source-server cleanup banner, set when the account arrived via a server-move
	// restore. Carried here so home/settings render it without a separate fetch;
	// the restore.show preference records a permanent dismissal.
	if user.Preferences["restore.show"] != "false" {
		udb := db_open("db/users.db")
		if row, _ := udb.row("select restore_source, restore_passkeys from users where uid=?", user.UID); row != nil {
			if source := as_string(row["restore_source"]); source != "" {
				result["restoreSource"] = source
				relinks := []gin.H{}
				if links, _ := udb.rows("select service, identifier from relinks where user=? order by service", user.UID); links != nil {
					for _, l := range links {
						relinks = append(relinks, gin.H{"service": as_string(l["service"]), "identifier": as_string(l["identifier"])})
					}
				}
				result["relinks"] = relinks
				// The source had passkeys, which can't be restored — prompt
				// the user to re-register them here.
				if as_int64(row["restore_passkeys"]) == 1 {
					result["restorePasskeys"] = true
				}
			}
		}
	}

	c.JSON(http.StatusOK, result)
}

// web_is_iframe_request reports whether the request comes from a sandboxed
// iframe: Sec-Fetch-Dest: iframe, or _shell=1 when the browser sent no
// classification. Sec-Fetch-Site is not usable - "cross-site" also matches
// top-level navigations from external links.
func web_is_iframe_request(c *gin.Context) bool {
	dest := c.GetHeader("Sec-Fetch-Dest")
	if dest == "iframe" {
		return true
	}
	// As in shell_wrap_candidate, _shell=1 speaks only for requests the browser
	// did not classify. An explicit "document" is a top-level navigation, and
	// treating one as an iframe skips the login redirect for anonymous callers
	// and serves app HTML outside the sandbox the parameter is meant to mark.
	if dest == "" && c.Query("_shell") == "1" {
		return true
	}
	return false
}

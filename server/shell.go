// Mochi server: Shell page for app isolation
// Copyright Alistair Cunningham 2026

package main

import (
	_ "embed"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
)

// shell_iframe_shim is injected into app HTML served inside sandboxed iframes.
// It provides in-memory fallbacks for APIs forbidden by the sandbox:
// - document.cookie (getter/setter)
// - window.localStorage
// - window.sessionStorage
// This runs before any app code so third-party libraries don't throw.
const shell_iframe_shim = `(function(){
var s={},p=function(){
this._d={};
};
p.prototype={
getItem:function(k){return this._d.hasOwnProperty(k)?this._d[k]:null;},
setItem:function(k,v){this._d[k]=String(v);},
removeItem:function(k){delete this._d[k];},
clear:function(){this._d={};},
key:function(i){return Object.keys(this._d)[i]||null;},
get length(){return Object.keys(this._d).length;}
};
try{window.localStorage}catch(e){Object.defineProperty(window,'localStorage',{value:new p(),configurable:true});}
try{window.sessionStorage}catch(e){Object.defineProperty(window,'sessionStorage',{value:new p(),configurable:true});}
try{document.cookie}catch(e){
var c='';
Object.defineProperty(document,'cookie',{
get:function(){return c;},
set:function(v){
var parts=String(v).split(';');
var kv=parts[0].split('=');
if(kv.length>=2){
var key=kv[0].trim();
var val=kv.slice(1).join('=').trim();
var pairs=c?c.split('; '):[];
var found=false;
for(var i=0;i<pairs.length;i++){
if(pairs[i].split('=')[0]===key){pairs[i]=key+'='+val;found=true;break;}
}
if(!found)pairs.push(key+'='+val);
c=pairs.join('; ');
}
},
configurable:true
});
}
})();`

//go:embed shell.html
var shell_html string

//go:embed shell.js
var shell_js string

// web_should_serve_shell returns true when the request should get the shell page
// instead of the app HTML directly
func web_should_serve_shell(c *gin.Context) bool {
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

	// Build the shell page from template.
	// Only static, server-controlled values are injected — no user data.
	page := shell_html

	// Appearance: set dark class or auto-detect script (controlled values, not user text)
	html_class, appearance_script := web_user_appearance_attrs(user)
	page = strings.Replace(page, "{{HTML_CLASS}}", html_class, 1)
	page = strings.Replace(page, "{{APPEARANCE_SCRIPT}}", appearance_script, 1)

	// Theme: apply color theme as inline CSS variables
	page = strings.Replace(page, "{{THEME_STYLE}}", web_user_theme_style(user), 1)

	// Embedded shell JS (from Go embed, not user input)
	page = strings.Replace(page, "{{SHELL_JS}}", shell_js, 1)

	// Menu app assets (filesystem paths, not user input)
	menu_js, menu_css := "", ""
	if menu != nil {
		menu_js, menu_css = shell_menu_assets(menu, user)
	}
	page = strings.Replace(page, "{{MENU_JS}}", menu_js, 1)
	page = strings.Replace(page, "{{MENU_CSS}}", menu_css, 1)

	// Clear stale mochi-theme cookie (no longer used)
	secure := c.Request.TLS != nil || c.GetHeader("X-Forwarded-Proto") == "https"
	c.SetCookie("mochi-theme", "", -1, "/", "", secure, false)

	// Security headers: shell cannot be framed
	c.Header("X-Frame-Options", "DENY")
	c.Header("Cross-Origin-Opener-Policy", "same-origin")
	c.Header("Content-Type", "text/html; charset=utf-8")
	c.Header("Cache-Control", "no-store")
	c.String(http.StatusOK, page)
}

func web_user_appearance_attrs(user *User) (string, string) {
	appearance := user_preference_get(user, "appearance", "auto")
	switch appearance {
	case "light":
		return `class="light"`, ""
	case "dark":
		return `class="dark"`, ""
	case "auto":
		return "", `<script>if(window.matchMedia('(prefers-color-scheme:dark)').matches)document.documentElement.classList.add('dark')</script>`
	default:
		return "", ""
	}
}

func appendRadiusVarsFromBase(styleParts *[]string, baseRadius string) {
	*styleParts = append(*styleParts,
		fmt.Sprintf("--radius: %s", baseRadius),
		fmt.Sprintf("--radius-sm: calc(%s - 4px)", baseRadius),
		fmt.Sprintf("--radius-md: calc(%s - 2px)", baseRadius),
		fmt.Sprintf("--radius-lg: %s", baseRadius),
		fmt.Sprintf("--radius-xl: calc(%s + 4px)", baseRadius),
	)
}

func appendRadiusPreset(styleParts *[]string, preset string) {
	switch preset {
	case "none":
		*styleParts = append(*styleParts,
			"--radius: 0rem",
			"--radius-sm: 0rem",
			"--radius-md: 0rem",
			"--radius-lg: 0rem",
			"--radius-xl: 0rem",
		)
	case "small":
		*styleParts = append(*styleParts,
			"--radius: 0.375rem",
			"--radius-sm: 0.125rem",
			"--radius-md: 0.25rem",
			"--radius-lg: 0.375rem",
			"--radius-xl: 0.625rem",
		)
	case "medium":
		*styleParts = append(*styleParts,
			"--radius: 0.75rem",
			"--radius-sm: 0.5rem",
			"--radius-md: 0.625rem",
			"--radius-lg: 0.75rem",
			"--radius-xl: 1rem",
		)
	case "large":
		*styleParts = append(*styleParts,
			"--radius: 1.75rem",
			"--radius-sm: 1.5rem",
			"--radius-md: 1.625rem",
			"--radius-lg: 1.75rem",
			"--radius-xl: 2rem",
		)
	}
}

func web_user_theme_style(user *User) string {
	if user == nil {
		return ""
	}

	styleParts := []string{}

	if theme_pref := user_preference_get(user, "theme", setting_get("default_theme", "")); theme_pref != "" {
		if parts := strings.SplitN(theme_pref, ":", 2); len(parts) == 2 {
			if t := app_theme_get(user, parts[0], parts[1]); t != nil {
				styleParts = append(styleParts,
					fmt.Sprintf("--hue: %g", t.Hue),
					fmt.Sprintf("--hue-chroma: %g", t.Chroma),
					fmt.Sprintf("--hue-bg: %g", t.HueBG),
				)
				if t.BorderRadius != "" && !strings.ContainsAny(t.BorderRadius, `;<>"`) {
					appendRadiusVarsFromBase(&styleParts, t.BorderRadius)
				}
				if t.Background != "" {
					// Resolve background URL from theme app's path
					if app_id := parts[0]; app_id != "" {
						apps_lock.Lock()
						a := apps[app_id]
						apps_lock.Unlock()
						if a != nil {
							av := a.active(user)
							if av != nil && len(av.Paths) > 0 {
								base := av.Paths[0]
								if !strings.ContainsAny(t.Background, `<>"`) {
									styleParts = append(styleParts, fmt.Sprintf("--background-image: url(/%s/backgrounds/%s)", base, t.Background))
								}
								if t.BackgroundDark != "" && !strings.ContainsAny(t.BackgroundDark, `<>"`) {
									styleParts = append(styleParts, fmt.Sprintf("--background-image-dark: url(/%s/backgrounds/%s)", base, t.BackgroundDark))
								}
							}
						}
					}
				}
				for key, val := range t.Overrides {
					if strings.HasPrefix(key, "--") && !strings.ContainsAny(key, `;<>"`) && !strings.ContainsAny(val, `;<>"`) {
						styleParts = append(styleParts, fmt.Sprintf("%s: %s", key, val))
					}
				}
			}
		}
	}

	// User border-radius preference takes precedence over theme radius.
	appendRadiusPreset(&styleParts, user_preference_get(user, "border_radius", "default"))

	if len(styleParts) == 0 {
		return ""
	}
	return `style="` + strings.Join(styleParts, "; ") + `"`
}

func web_apply_user_document_theme(content string, user *User) string {
	if user == nil {
		return content
	}

	html_class, appearance_script := web_user_appearance_attrs(user)
	content = web_add_html_attr(content, html_class)
	content = web_add_html_attr(content, web_user_theme_style(user))
	if appearance_script != "" {
		content = strings.Replace(content, "<head>", "<head>"+appearance_script, 1)
	}
	return content
}

// web_add_html_attr injects a class="..." or style="..." attribute into the
// first <html> tag. If the tag already carries the same attribute name the
// values are merged (space-joined for class, semicolon-joined for style)
// instead of creating an invalid duplicate attribute.
func web_add_html_attr(content, attr string) string {
	if attr == "" {
		return content
	}

	start := strings.Index(content, "<html")
	if start == -1 {
		return content
	}
	end := strings.Index(content[start:], ">")
	if end == -1 {
		return content
	}
	end += start
	tag := content[start:end]

	// Extract the attribute name and value from the incoming attr (e.g. class="dark")
	eq := strings.Index(attr, "=")
	if eq == -1 {
		// Plain attribute without value — just append
		return content[:end] + " " + attr + content[end:]
	}
	name := attr[:eq]                     // "class" or "style"
	val := strings.Trim(attr[eq+1:], `"`) // the value without quotes

	// Check if the <html> tag already has this attribute
	needle := name + `="`
	pos := strings.Index(tag, needle)
	if pos == -1 {
		// Attribute doesn't exist yet — append it
		return content[:end] + " " + attr + content[end:]
	}

	// Find the closing quote of the existing attribute value
	val_start := start + pos + len(needle)
	val_end := strings.Index(content[val_start:], `"`)
	if val_end == -1 {
		return content[:end] + " " + attr + content[end:]
	}
	val_end += val_start

	// Merge: space for class, semicolon for style
	sep := " "
	if name == "style" {
		sep = "; "
	}
	existing := content[val_start:val_end]
	if existing == "" {
		existing = val
	} else {
		existing = existing + sep + val
	}
	return content[:val_start] + existing + content[val_end:]
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
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Authentication required"})
		return
	}

	var input struct {
		App string `json:"app"`
	}
	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "app required"})
		return
	}

	// Resolve the app path to an app
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

	c.JSON(http.StatusOK, gin.H{"token": token, "app": a.id})
}

// web_shell_init handles POST /_/shell — returns shell bootstrap config.
// Called once by the shell page on load. Protected by session cookie
// (sandboxed iframe apps cannot call this).
func web_shell_init(c *gin.Context) {
	user := web_auth(c)
	if user == nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Authentication required"})
		return
	}

	session := web_cookie_get(c, "session", "")

	// Menu app token
	menu_token := ""
	if menu := shell_menu_app(user); menu != nil {
		menu_token = auth_create_app_token(user.ID, session, menu.id)
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

	c.JSON(http.StatusOK, result)
}

// web_is_iframe_request returns true if the request is from a sandboxed iframe.
// Detects two cases:
//  1. Shell sets iframe.src → browser sends Sec-Fetch-Dest: iframe
//  2. Navigation within the shell iframe → URL contains _shell=1 query parameter
//     (added by web_serve_shell to the iframe src)
//
// Previously, case 2 used Sec-Fetch-Site: cross-site, but this also matches
// cross-site top-level navigations (e.g., links from Reddit), causing the
// login/landing page to be skipped for external visitors.
func web_is_iframe_request(c *gin.Context) bool {
	if c.GetHeader("Sec-Fetch-Dest") == "iframe" {
		return true
	}
	if c.Query("_shell") == "1" {
		return true
	}
	return false
}

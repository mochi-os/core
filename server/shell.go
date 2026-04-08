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
	appearance := user_preference_get(user, "appearance", "auto")
	html_class := ""
	appearance_script := ""
	switch appearance {
	case "dark":
		html_class = `class="dark"`
	case "auto":
		appearance_script = `<script>if(window.matchMedia('(prefers-color-scheme:dark)').matches)document.documentElement.classList.add('dark')</script>`
	}
	page = strings.Replace(page, "{{HTML_CLASS}}", html_class, 1)
	page = strings.Replace(page, "{{APPEARANCE_SCRIPT}}", appearance_script, 1)

	// Theme: apply color theme as inline CSS variables
	theme_style := ""
	if theme_pref := user_preference_get(user, "theme", setting_get("default_theme", "")); theme_pref != "" {
		if parts := strings.SplitN(theme_pref, ":", 2); len(parts) == 2 {
			if t := app_theme_get(user, parts[0], parts[1]); t != nil {
				theme_style = fmt.Sprintf(`style="--hue: %g; --hue-chroma: %g; --hue-bg: %g`, t.Hue, t.Chroma, t.HueBG)
				if t.BorderRadius != "" && !strings.ContainsAny(t.BorderRadius, `;<>"`) {
					theme_style += fmt.Sprintf("; --radius: %s", t.BorderRadius)
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
									theme_style += fmt.Sprintf("; --background-image: url(/%s/backgrounds/%s)", base, t.Background)
								}
								if t.BackgroundDark != "" && !strings.ContainsAny(t.BackgroundDark, `<>"`) {
									theme_style += fmt.Sprintf("; --background-image-dark: url(/%s/backgrounds/%s)", base, t.BackgroundDark)
								}
							}
						}
					}
				}
				for key, val := range t.Overrides {
					if strings.HasPrefix(key, "--") && !strings.ContainsAny(key, `;<>"`) && !strings.ContainsAny(val, `;<>"`) {
						theme_style += fmt.Sprintf("; %s: %s", key, val)
					}
				}
				theme_style += `"`
			}
		}
	}
	page = strings.Replace(page, "{{THEME_STYLE}}", theme_style, 1)

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

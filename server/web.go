// Mochi server: Sample web interface
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"embed"
	"fmt"
	"github.com/gin-gonic/autotls"
	"github.com/gin-gonic/gin"
	"html/template"
	"net/http"
	"net/url"
)

var (
	//go:embed templates/en/*.tmpl templates/en/*/*.tmpl templates/en/*/*/*.tmpl
	templates embed.FS
)

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
			web_error(c, 500, "No database for app")
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
				web_error(c, 500, "No database for app")
				return
			}
			defer owner.db.close()
		}
	}

	if p.app.Database.File != "" && user == nil && owner == nil {
		web_error(c, 401, "Content not public, and not logged in")
		return
	}

	a := Action{user: user, owner: owner, app: p.app, web: c, path: p}

	switch p.app.Engine {
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
			web_error(c, 401, "Content not public, and not logged in")
			return
		}

		if p.app.starlark == nil {
			p.app.starlark = starlark(file_glob(fmt.Sprintf("%s/code/*.star", p.app.base)))
		}
		p.app.starlark.thread.SetLocal("action", &a)
		err := p.app.starlark.call(p.function, map[string]string{"path": p.path}, web_inputs(c))
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

	for _, p := range paths {
		r.GET("/"+p.path, p.web_path)
		r.POST("/"+p.path, p.web_path)
	}
	r.POST("/login", web_login)
	r.POST("/login/identity", web_identity_create)
	r.GET("/ping", web_ping)
	r.GET("/websocket", websocket_connection)

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

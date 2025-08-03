// Mochi server: Sample web interface
// Copyright Alistair Cunningham 2024

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

	web_redirect(c, "/?action=welcome")
}

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

	owner := user
	if e != nil {
		owner = user_owning_entity(e.ID)
	}

	//TODO Set database location
	var db *DB = nil
	if p.app.db_file != "" {
		if user != nil {
			db = db_user(user, p.app.db_file, p.app.db_create)
			defer db.close()

		} else if owner != nil {
			db = db_user(owner, p.app.db_file, p.app.db_create)
			defer db.close()

		} else {
			web_error(c, 401, "Path not public, and not logged in")
			return
		}
	}

	p.action(&Action{user: user, owner: owner, db: db, web: c, path: p})
}

func web_ping(c *gin.Context) {
	c.String(http.StatusOK, "pong")
}

func web_redirect(c *gin.Context, url string) {
	web_template(c, 200, "redirect", url)
}

func web_start(listen string, port int, domains []string, debug bool) {
	if port == 0 {
		return
	}

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
	//TODO Replace with ini file setting
	r.Static("/apt", "/sys/apt")

	if len(domains) > 0 {
		log_info("Web listening on HTTPS domains %v", domains)
		err := autotls.Run(r, domains...)
		check(err)

	} else {
		log_info("Web listening on '%s:%d'", listen, port)
		err := r.Run(fmt.Sprintf("%s:%d", listen, port))
		check(err)
	}
}

// This could probably be better written using c.HTML(), but I can't figure out how to load the templates.
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

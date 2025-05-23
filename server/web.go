// Comms server: Sample web interface
// Copyright Alistair Cunningham 2024

package main

import (
	"context"
	"embed"
	"fmt"
	"github.com/gin-gonic/autotls"
	"github.com/gin-gonic/gin"
	"html/template"
	"net/http"
	"net/url"
	"nhooyr.io/websocket"
)

var (
	//go:embed templates/en/*.tmpl templates/en/*/*.tmpl templates/en/*/*/*.tmpl
	templates embed.FS
)

var websockets = map[int]map[string]*websocket.Conn{}

/* Not used for now
func web_action(c *gin.Context) {
	var u *User = nil
	referrer, err := url.Parse(c.Request.Header.Get("Referer"))
	if err == nil && (referrer.Host == "" || referrer.Host == c.Request.Host) {
		u = web_auth(c)
		if u != nil && u.Identity == nil {
			web_template(c, 200, "login/identity")
			return
		}
	}

	path := strings.Trim(c.Request.URL.Path, "/")
	if len(path) > 0 && path[0:1] == "+" {
		splits1 := strings.SplitN(path, "/", 2)
		splits2 := strings.SplitN(splits1[0][1:], "+", 2)
		entity := splits2[0]
		action := ""
		if len(splits1) > 1 {
			action = splits1[1]
		}
		log_debug("Entity='%s', action='%s'", entity, action)
		e := identity_by_fingerprint(entity)
		if e == nil {
			e = identity_by_id(entity)
			if e == nil {
				web_error(c, 404, "Web entity not found")
				return
			}
		}
		a, found := classes[e.Class]
		if !found {
			web_error(c, 404, "Web entity has no owning app")
			return
		}
		f, found := actions[action]
		if !found {
			web_error(c, 404, "Web action not found")
			return
		}
		//Also match parent field?
		owner := user_by_id(e.User)
		if owner == nil {
			web_error(c, 500, "Web entity has no owner")
			return
		}
		var db *DB = nil
		if a.db_file != "" {
			db = db_app(owner, a.name, a.db_file, a.db_create)
			defer db.close()
		}
		f(&Action{entity: e, user: u, db: db, web: c})
	}
} */

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

	_, err := identity_create(u, "person", c.Query("name"), c.Query("privacy"), "")
	if err != nil {
		web_error(c, 400, "Unable to create identity: %s", err)
		return
	}

	web_redirect(c, "/")
}

func web_login(c *gin.Context) {
	code := c.Query("code")
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

	email := c.Query("email")
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
	log_debug("Web path '%s', entity='%s'", c.Request.URL.Path, c.Param("entity"))
	var u *User = nil
	referrer, err := url.Parse(c.Request.Header.Get("Referer"))
	if err == nil && (referrer.Host == "" || referrer.Host == c.Request.Host) {
		u = web_auth(c)
		if u != nil && u.Identity == nil {
			web_template(c, 200, "login/identity")
			return
		}
	}

	var e *Identity = nil
	entity := c.Param("entity")
	if entity != "" {
		e = identity_by_fingerprint(entity)
		if e == nil {
			e = identity_by_id(entity)
		}
	}

	var db *DB = nil
	if p.app.db_file != "" {
		dbu := u
		if dbu == nil && e != nil {
			dbu = user_by_id(e.User)
		}
		if dbu == nil {
			web_error(c, 401, "Path not public, and not logged in")
			return
		}
		//log_debug("Loading db for user='%d', app='%s'", dbu.ID, p.app.name)
		db = db_app(dbu, p.app.name, p.app.db_file, p.app.db_create)
		defer db.close()
	}

	p.action(&Action{entity: e, user: u, db: db, web: c})
}

func web_ping(c *gin.Context) {
	c.String(http.StatusOK, "pong")
}

func web_redirect(c *gin.Context, url string) {
	web_template(c, 200, "redirect", url)
}

func web_start(port int, domains []string) {
	//gin.SetMode(gin.ReleaseMode)
	r := gin.Default()
	r.SetTrustedProxies(nil)

	for _, p := range paths {
		r.GET("/"+p.path, p.web_path)
		r.POST("/"+p.path, p.web_path)
	}
	r.GET("/login", web_login)
	r.GET("/login/identity", web_identity_create)
	r.GET("/ping", web_ping)
	r.GET("/websocket", websocket_connection)

	if len(domains) > 0 {
		log_info("Web listening on HTTPS domains %v", domains)
		err := autotls.Run(r, domains...)
		check(err)

	} else {
		log_info("Web listening on HTTP port %d", port)
		err := r.Run(fmt.Sprintf(":%d", port))
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

func websocket_connection(c *gin.Context) {
	u := web_auth(c)
	if u == nil {
		return
	}

	ws, err := websocket.Accept(c.Writer, c.Request, nil)
	if err != nil {
		return
	}
	id := uid()
	defer websocket_terminate(ws, u, id)

	_, found := websockets[u.ID]
	if !found {
		websockets[u.ID] = map[string]*websocket.Conn{}
	}
	websockets[u.ID][id] = ws
	ctx := context.Background()

	for {
		t, j, err := ws.Read(ctx)
		if err != nil {
			websocket_terminate(ws, u, id)
			return
		}
		if t != websocket.MessageText {
			continue
		}

		log_info("Websocket received message; ignoring: %s", j)
	}
}

func websockets_send(u *User, app string, content string) {
	ctx := context.Background()
	j := ""

	for id, ws := range websockets[u.ID] {
		if j == "" {
			j = json_encode(map[string]string{"app": app, "content": content})
		}
		err := ws.Write(ctx, websocket.MessageText, []byte(j))
		if err != nil {
			websocket_terminate(ws, u, id)
		}
	}
}

func websocket_terminate(ws *websocket.Conn, u *User, id string) {
	ws.CloseNow()
	delete(websockets[u.ID], id)
}

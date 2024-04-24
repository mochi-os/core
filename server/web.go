// Comms server: Sample web interface
// Copyright Alistair Cunningham 2024

package main

import (
	"context"
	"embed"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"nhooyr.io/websocket"
	"strings"
)

var (
	//go:embed templates/en/*.tmpl templates/en/*/*.tmpl
	templates embed.FS
)

var web_port int
var websockets = map[int]map[string]*websocket.Conn{}

func web_auth(w http.ResponseWriter, r *http.Request) *User {
	login := web_cookie_get(r, "login", "")
	if login == "" {
		web_template(w, "login/email")
		return nil
	}

	u := user_by_login(login)
	if u == nil {
		web_template(w, "login/email")
		return nil
	}

	return u
}

func web_cookie_get(r *http.Request, name string, def string) string {
	c, err := r.Cookie("login")
	if err != nil {
		return def
	}
	return c.Value
}

func web_cookie_set(w http.ResponseWriter, name string, value string) {
	c := http.Cookie{Name: name, Value: value, Path: "/", MaxAge: 365 * 86400, SameSite: http.SameSiteStrictMode}
	http.SetCookie(w, &c)
}

func web_cookie_unset(w http.ResponseWriter, name string) {
	c := http.Cookie{Name: name, Value: "", Path: "/", MaxAge: -1}
	http.SetCookie(w, &c)
}

func web_error(w http.ResponseWriter, data any) {
	web_template(w, "error", data)
}

func web_action(w http.ResponseWriter, r *http.Request) {
	var u *User
	referrer, err := url.Parse(r.Referer())
	if err == nil && (referrer.Host == "" || referrer.Host == r.Host) {
		u = web_auth(w, r)
	}

	action := strings.Trim(r.URL.Path, "/")
	f, found := actions[action]
	if !found {
		app_error(w, 404, "Not found")
		return
	}
	if u == nil && actions_authenticated[action] {
		app_error(w, 401, "Not logged in")
		return
	}

	f(u, w, r)
}

func web_login(w http.ResponseWriter, r *http.Request) {
	code := r.FormValue("code")
	if code != "" {
		u := user_from_code(code)
		if u == nil {
			web_error(w, "Invalid code")
			return
		}
		web_cookie_set(w, "login", login_create(u.ID))

		if u.Name == "" {
			web_template(w, "login/name")
			return
		}

		web_redirect(w, "/")
		return
	}

	email := r.FormValue("email")
	if email != "" {
		if !code_send(email) {
			web_error(w, "Invalid email address")
			return
		}
		web_template(w, "login/code", email)
		return
	}

	web_template(w, "login/email")
}

func web_name(w http.ResponseWriter, r *http.Request) {
	u := web_auth(w, r)
	if u == nil {
		return
	}

	name := r.FormValue("name")
	if !valid(name, "name") {
		web_error(w, "Invalid name")
		return
	}
	u.Name = name
	db_exec("db/users.db", "update users set name=? where id=?", name, u.ID)
	directory_create(u)
	directory_publish(u)
	web_redirect(w, "/")
}

func web_redirect(w http.ResponseWriter, url string) {
	web_template(w, "redirect", url)
}

func web_start() {
	http.HandleFunc("/", web_action)
	//TODO Decide what to do with fixed URLs
	http.HandleFunc("/login/", web_login)
	http.HandleFunc("/login/name/", web_name)
	http.HandleFunc("/websocket/", websocket_connection)
	log_info("Web listening on ':%d'", web_port)
	err := http.ListenAndServe(fmt.Sprintf(":%d", web_port), nil)
	check(err)
}

func web_template(w http.ResponseWriter, file string, values ...any) {
	t, err := template.ParseFS(templates, "templates/en/"+file+".tmpl", "templates/en/include/head.tmpl", "templates/en/include/foot.tmpl")
	if err != nil {
		log_warn(err.Error())
		http.Error(w, "Web template error", http.StatusInternalServerError)
		return
	}
	if len(values) > 0 {
		err = t.Execute(w, values[0])
	} else {
		err = t.Execute(w, nil)
	}
	if err != nil {
		log_warn("Web template error '%s'", err.Error())
		http.Error(w, "Web template error", http.StatusInternalServerError)
	}
}

func websocket_connection(w http.ResponseWriter, r *http.Request) {
	u := web_auth(w, r)
	if u == nil {
		return
	}

	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	id := uid()
	defer websocket_terminate(c, u, id)

	_, found := websockets[u.ID]
	if !found {
		websockets[u.ID] = map[string]*websocket.Conn{}
	}
	websockets[u.ID][id] = c
	ctx := context.Background()

	for {
		t, j, err := c.Read(ctx)
		if err != nil {
			websocket_terminate(c, u, id)
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

	for id, c := range websockets[u.ID] {
		if j == "" {
			j = json_encode(map[string]string{"app": app, "content": content})
		}
		err := c.Write(ctx, websocket.MessageText, []byte(j))
		if err != nil {
			websocket_terminate(c, u, id)
		}
	}
}

func websocket_terminate(c *websocket.Conn, u *User, id string) {
	c.CloseNow()
	delete(websockets[u.ID], id)
}

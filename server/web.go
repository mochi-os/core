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
	//go:embed templates/en/*.tmpl templates/en/*/*.tmpl templates/en/*/*/*.tmpl
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

func web_home(w http.ResponseWriter, r *http.Request) {
	u := web_auth(w, r)
	if u == nil {
		return
	}

	paths := strings.SplitN(strings.Trim(r.URL.Path, "/"), "/", 2)

	if len(paths) > 0 && paths[0] != "" {
		app := paths[0]
		action := ""
		if len(paths) > 1 {
			action = paths[1]
		}

		parameters := make(app_parameters)
		referrer, err := url.Parse(r.Referer())
		if err == nil && (referrer.Host == "" || referrer.Host == r.Host) {
			for p, v := range r.URL.Query() {
				parameters[p] = v
			}
			err := r.ParseForm()
			check(err)
			for p, v := range r.Form {
				parameters[p] = v
			}
		}

		out, err := app_display(u, app, action, "html", parameters)
		if err != nil {
			web_error(w, err)
			return
		}
		web_template(w, "app/display", template.HTML(out))

	} else {
		action := r.FormValue("action")

		if action == "clear" {
			service(u, "notifications", "clear")
			web_redirect(w, "/")
			return

		} else if action == "logout" {
			login := web_cookie_get(r, "login", "")
			if login != "" {
				login_delete(login)
			}
			web_cookie_unset(w, "login")
			web_template(w, "login/logout")
			return
		}

		a := apps_by_name["notifications"]
		n, err := app_display(u, a.Name, "display", "html", app_parameters{})
		if err != nil {
			web_error(w, err)
			return
		}
		web_template(w, "home", map[string]any{"User": u, "Apps": apps_by_path, "Notifications": template.HTML(n)})
	}
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
	db_exec("users", "update users set name=? where id=?", name, u.ID)
	directory_create(u)
	directory_publish(u)
	web_redirect(w, "/")
}

func web_redirect(w http.ResponseWriter, url string) {
	web_template(w, "redirect", url)
}

func web_start() {
	http.HandleFunc("/", web_home)
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
	log_debug("Websocket new connection")
	u := web_auth(w, r)
	if u == nil {
		return
	}
	log_debug("Websocket user='%d'", u.ID)

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
			log_warn("Websocket read error; closing: %s", err)
			websocket_terminate(c, u, id)
			return
		}
		if t != websocket.MessageText {
			log_info("Websocket discarding non-text message")
			continue
		}

		log_info("Websocket received message; ignoring: %s", j)
	}
}

func websockets_send(u *User, app string, content string) {
	ctx := context.Background()
	j := ""

	for id, c := range websockets[u.ID] {
		log_debug("Websocket writing: user='%d', websocket id='%s', app='%s', content='%s'", u.ID, id, app, content)
		if j == "" {
			j = json_encode(map[string]string{"app": app, "content": content})
		}
		err := c.Write(ctx, websocket.MessageText, []byte(j))
		if err != nil {
			log_info("Websocket write error; closing: %s", err)
			websocket_terminate(c, u, id)
		}
	}
}

func websocket_terminate(c *websocket.Conn, u *User, id string) {
	c.CloseNow()
	delete(websockets[u.ID], id)
}

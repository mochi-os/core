// Comms server: Home app
// Copyright Alistair Cunningham 2024

package main

import (
	"net/http"
)

type HomeAction struct {
	Action string
	Labels map[string]string
}

var home_actions = map[string]HomeAction{}

func init() {
	register_app("home")
	register_action("home", "", home, false)
}

func home(u *User, w http.ResponseWriter, r *http.Request) {
	if u == nil {
		web_login(w, r)
		return
	}

	switch r.FormValue("action") {
	case "clear":
		notifications_clear(u)
		web_redirect(w, "/")
		return

	case "logout":
		login := web_cookie_get(r, "login", "")
		if login != "" {
			login_delete(login)
		}
		web_cookie_unset(w, "login")
		web_template(w, "login/logout")
		return
	}

	app_write(w, "html", "home", map[string]any{"User": u, "Actions": home_actions, "Notifications": notifications_list(u)})
}

func register_home(name string, action string, labels map[string]string) {
	a, found := apps[name]
	if !found || a.Type != "internal" {
		log_warn("register_home() called for non-installed or non-internal app '%s'", name)
		return
	}
	home_actions[action] = HomeAction{Action: action, Labels: labels}
}

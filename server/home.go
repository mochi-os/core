// Comms server: Home app
// Copyright Alistair Cunningham 2024

package main

type HomeAction struct {
	Action string
	Labels map[string]string
}

var home_actions = map[string]HomeAction{}

func init() {
	register_app("home")
	register_action("home", "", home, true)
}

func home(u *User, a *Action) {
	switch a.Input("action") {
	case "clear":
		notifications_clear(u)
		a.Redirect("/")
		return

	case "logout":
		login := web_cookie_get(a.r, "login", "")
		if login != "" {
			login_delete(login)
		}
		web_cookie_unset(a.w, "login")
		a.WriteTemplate("login/logout")
		return
	}

	a.WriteTemplate("home", map[string]any{"User": u, "Actions": home_actions, "Notifications": notifications_list(u)})
}

func register_home(name string, action string, labels map[string]string) {
	a, found := apps[name]
	if !found || a.Type != "internal" {
		log_warn("register_home() called for non-installed or non-internal app '%s'", name)
		return
	}
	home_actions[action] = HomeAction{Action: action, Labels: labels}
}

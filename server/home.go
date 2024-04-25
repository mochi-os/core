// Comms server: Home app
// Copyright Alistair Cunningham 2024

package main

type HomeAction struct {
	Action string
	Labels map[string]string
}

var home_actions = map[string]HomeAction{}

func init() {
	a := register_app("home")
	a.register_action("", home, true)
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

func (a *App) register_home(action string, labels map[string]string) {
	home_actions[action] = HomeAction{Action: action, Labels: labels}
}

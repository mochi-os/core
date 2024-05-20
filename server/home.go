// Comms server: Home app
// Copyright Alistair Cunningham 2024

package main

type HomeAction struct {
	Action string
	Labels map[string]string
}

var home_actions = map[string]HomeAction{}

func init() {
	a := app("home")
	a.path("", home, true)
}

func home(a *Action) {
	switch a.input("action") {
	case "clear":
		notifications_clear(a.user)
		a.redirect("/")
		return

	case "logout":
		login := web_cookie_get(a.r, "login", "")
		if login != "" {
			login_delete(login)
		}
		web_cookie_unset(a.w, "login")
		a.template("login/logout")
		return
	}

	a.template("home", map[string]any{"User": a.user, "Actions": home_actions, "Notifications": notifications_list(a.user)})
}

func (a *App) home(action string, labels map[string]string) {
	home_actions[action] = HomeAction{Action: action, Labels: labels}
}

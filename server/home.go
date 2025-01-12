// Comms server: Home app
// Copyright Alistair Cunningham 2024

package main

//TODO Rename to HomePath?
type HomeAction struct {
	Action string
	Labels map[string]string
}

var home_actions = map[string]HomeAction{}

func init() {
	a := app("home")
	a.path("", home)
}

func home(a *Action) {
	if a.user == nil {
		web_login(a.web)
		return
	}

	switch a.input("action") {
	case "clear":
		notifications_clear(a.user)
		a.redirect("/")
		return

	case "logout":
		login := web_cookie_get(a.web, "login", "")
		if login != "" {
			login_delete(login)
		}
		web_cookie_unset(a.web, "login")
		a.template("login/logout")
		return
	}

	a.template("home", Map{"User": a.user, "Actions": home_actions, "Notifications": notifications_list(a.user)})
}

func (a *App) home(action string, labels map[string]string) {
	home_actions[action] = HomeAction{Action: action, Labels: labels}
}

// Mochi server: Home app
// Copyright Alistair Cunningham 2024-2025

package main

type Icon struct {
	Path  string `json:"path"`
	Label string `json:"label"`
	// Remove Name field once we have multi-language label system in place
	Name string `json:"name"`
	Icon string `json:"icon"`
}

var (
	icons = map[string]Icon{}
)

func init() {
	a := app("home")
	a.path("", home)
}

func home(a *Action) {
	if a.user == nil {
		web_login(a.web)
		return
	}

	welcome := false

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
		a.template("login/logout", a.input("format"))
		return

	case "welcome":
		welcome = true
	}

	a.template("home", a.input("format"), Map{"User": a.user, "Icons": icons, "Notifications": notifications_list(a.user), "Welcome": welcome})
}

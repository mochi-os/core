// Mochi server: Home app
// Copyright Alistair Cunningham 2024-2025

package main

type HomePath struct {
	Path   string
	Labels map[string]string
}

var (
	home_paths = map[string]HomePath{}
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

	a.template("home", a.input("format"), Map{"User": a.user, "Paths": home_paths, "Notifications": notifications_list(a.user), "Welcome": welcome})
}

// Comms server: Apps
// Copyright Alistair Cunningham 2024

package main

import (
	"bytes"
	"html/template"
)

// TODO app_register_service()?
// TODO Register for each call, display action, and event?
type App struct {
	Name     string
	Label    string
	Services []string
	Call     func(*User, string, string, ...any) any
	Display  func(*User, app_parameters, string) string
	Event    func(*User, *Event)
}

type app_parameters map[string][]string

var apps_by_name = map[string]*App{}
var apps_by_service = map[string]*App{}

func app_by_service(service string) *App {
	a, ok := apps_by_service[service]
	if ok {
		return a
	}
	return nil
}

func app_display(u *User, app string, parameters app_parameters, format string) (string, error) {
	a, ok := apps_by_name[app]
	if !ok {
		return "", error_message("App not installed")
	}
	if a.Display == nil {
		return "", error_message("App has no web display handler")
	}

	return a.Display(u, parameters, format), nil
}

func app_error(e error) string {
	return e.Error()
}

func app_parameter(p map[string][]string, key string, def string) string {
	values, ok := p[key]
	if !ok {
		return def
	}
	return values[0]
}

func app_register_function_call(name string, f func(*User, string, string, ...any) any) {
	log_debug("App register function call: name='%s', function='%v'", name, f)
	a, ok := apps_by_name[name]
	if !ok {
		log_warn("app_register_function_call() called for non-installed app '%s'", name)
		return
	}
	a.Call = f
}

func app_register_function_display(name string, f func(*User, app_parameters, string) string) {
	log_debug("App register function display: name='%s', function='%v'", name, f)
	a, ok := apps_by_name[name]
	if !ok {
		log_warn("app_register_function_display() called for non-installed app '%s'", name)
		return
	}
	a.Display = f
}

func app_register_function_event(name string, f func(*User, *Event)) {
	log_debug("App register function event: name='%s', function='%v'", name, f)
	a, ok := apps_by_name[name]
	if !ok {
		log_warn("app_register_function_event() called for non-installed app '%s'", name)
		return
	}
	a.Event = f
}

func app_register_internal(name string, label string, services []string) {
	log_debug("App register internal: name='%s', label='%s', services='%v'", name, label, services)
	a := App{Name: name, Label: label, Services: services, Display: nil}

	apps_by_name[name] = &a
	for _, s := range services {
		//TODO Handle multiple apps for same service
		apps_by_service[s] = &a
	}
}

func app_template(file string, values ...any) string {
	t, err := template.ParseFS(templates, "templates/en/"+file+".tmpl")
	if err != nil {
		log_warn(err.Error())
		return "App template error: " + err.Error()
	}
	var out bytes.Buffer
	if len(values) > 0 {
		err = t.Execute(&out, values[0])
	} else {
		err = t.Execute(&out, nil)
	}
	if err != nil {
		log_warn(err.Error())
		return "App template error: " + err.Error()
	}
	return out.String()
}

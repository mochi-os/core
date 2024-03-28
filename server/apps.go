// Comms server: Apps
// Copyright Alistair Cunningham 2024

package main

import (
	"bytes"
	"html/template"
)

// TODO Register for each display action?
type App struct {
	Name      string
	Labels    map[string]string
	Display   func(*User, app_parameters, string) string
	Events    map[string]func(*User, *Event)
	Functions map[string]func(*User, string, string, ...any) any
	Services  []string
}

type app_parameters map[string][]string

var apps_by_name = map[string]*App{}
var apps_by_service = map[string]*App{}
var app_pubsubs []string

func app_by_service(service string) *App {
	a, found := apps_by_service[service]
	if found {
		return a
	}
	return nil
}

func app_display(u *User, app string, parameters app_parameters, format string) (string, error) {
	log_debug("Displaying app: user='%d', app='%s', parameters='%#v', format='%s'", u.ID, app, parameters, format)
	a, found := apps_by_name[app]
	if !found {
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
	values, found := p[key]
	if !found {
		return def
	}
	return values[0]
}

func app_register(name string, labels map[string]string) {
	log_debug("App register internal: name='%s', label='%s'", name, labels["en"])
	a := App{Name: name, Labels: labels, Display: nil, Events: make(map[string]func(*User, *Event)), Functions: make(map[string]func(*User, string, string, ...any) any), Services: make([]string, 1)}
	apps_by_name[name] = &a
}

func app_register_display(name string, f func(*User, app_parameters, string) string) {
	log_debug("App register function display: name='%s'", name)
	a, found := apps_by_name[name]
	if !found {
		log_warn("app_register_display() called for non-installed app '%s'", name)
		return
	}
	a.Display = f
}

func app_register_event(name string, action string, f func(*User, *Event)) {
	log_debug("App register function event: name='%s', action='%s'", name, action)
	a, found := apps_by_name[name]
	if !found {
		log_warn("app_register_event() called for non-installed app '%s'", name)
		return
	}
	a.Events[action] = f
}

func app_register_function(name string, function string, f func(*User, string, string, ...any) any) {
	log_debug("App register function: name='%s', function='%s'", name, function)
	a, found := apps_by_name[name]
	if !found {
		log_warn("app_register_function() called for non-installed app '%s'", name)
		return
	}
	a.Functions[function] = f
}

func app_register_pubsub(name string, topic string) {
	log_debug("App register pubsub: name='%s', topic='%s'", name, topic)
	_, found := apps_by_name[name]
	if !found {
		log_warn("app_register_pubsub() called for non-installed app '%s'", name)
		return
	}
	app_pubsubs = append(app_pubsubs, topic)
}

func app_register_service(name string, service string) {
	log_debug("App register service: name='%s', service='%s'", name, service)
	a, found := apps_by_name[name]
	if !found {
		log_warn("app_register_service() called for non-installed app '%s'", name)
		return
	}
	a.Services = append(a.Services, service)
	//TODO Handle multiple apps for same service
	apps_by_service[service] = a
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

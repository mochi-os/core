// Comms server: Services
// Copyright Alistair Cunningham 2024

package main

func service(u *User, app string, service string, values ...any) any {
	log_debug("Service call: user='%d', app='%s', service='%s', values='%v'", u.ID, app, service, values)
	a := apps_by_name[app]
	if a == nil {
		log_info("Service call to unnkown app '%s'", service)
		return nil
	}
	_, found := a.Services[service]
	if found {
		return a.Services[service](u, service, values...)
	} else {
		_, found := a.Services[""]
		if found {
			return a.Services[""](u, service, values...)
		}
	}
	log_info("Call to app '%s' without handler for service '%s'", app, service)
	return nil
}

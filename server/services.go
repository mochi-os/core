// Comms server: Services
// Copyright Alistair Cunningham 2024

package main

func service(u *User, service string, function string, values ...any) any {
	log_debug("Service call: user='%d', service='%s', function='%s', values='%v'", u.ID, service, function, values)
	a := app_by_service(service)
	if a == nil {
		log_info("Call to service '%s' without handler app", service)
		return nil
	}
	_, found := a.Functions[function]
	if found {
		return a.Functions[function](u, service, function, values...)
	} else {
		_, found := a.Functions[""]
		if found {
			return a.Functions[""](u, service, function, values...)
		}
	}
	log_info("Call to service '%s' without handler for function '%s'", service, function)
	return nil
}

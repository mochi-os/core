// Comms server: Services
// Copyright Alistair Cunningham 2024

package main

func service(u *User, service string, function string, values ...any) any {
	log_debug("Service call: user='%d', service='%s', function='%s', values='%v'", u.ID, service, function, values)
	a := app_by_service(service)
	if a == nil {
		log_info("Call to service without call handler '%s'", service)
		return nil
	}
	return a.Call(u, service, function, values...)
}

// Comms server: Services
// Copyright Alistair Cunningham 2024

package main

import (
	"encoding/json"
)

func service(u *User, app string, s string, parameters ...any) {
	log_debug("Service call: user='%d', app='%s', service='%s', parameters='%v'", u.ID, app, s, parameters)

	a := apps_by_name[app]
	if a == nil {
		log_info("Service call to unknown app '%s'", app)
		return
	}

	switch a.Type {
	case "internal":
		for _, try := range []string{s, ""} {
			f, found := a.Internal.Services[try]
			if found {
				f(u, s, parameters...)
				return
			}
		}

	case "wasm":
		for _, try := range []string{s, ""} {
			function, found := a.WASM.Services[try]
			if found {
				_, err := wasm_run(u, a, function, map[string]any{"service": s, "parameters": parameters})
				if err != nil {
					log_info("Service returned error: %s", err)
					return
				}
				return
			}
		}
	}

	log_info("Call to app '%s' without handler for service '%s'", app, s)
	return
}

func service_generic[T any](u *User, app string, s string, parameters ...any) (*T, error) {
	log_debug("Service call generic: user='%d', app='%s', service='%s', parameters='%v'", u.ID, app, s, parameters)
	var out T

	a := apps_by_name[app]
	if a == nil {
		log_info("Service call to unknown app '%s'", app)
		return &out, error_message("Service call to unknown app '%s'", app)
	}

	switch a.Type {
	case "internal":
		for _, try := range []string{s, ""} {
			f, found := a.Internal.Services[try]
			if found {
				out = f(u, s, parameters...).(T)
				return &out, nil
			}
		}

	case "wasm":
		for _, try := range []string{s, ""} {
			function, found := a.WASM.Services[try]
			if found {
				jo, err := wasm_run(u, a, function, map[string]any{"service": s, "parameters": parameters})
				if err != nil {
					log_info("Service returned error: %s", err)
					return &out, err
				}
				err = json.Unmarshal([]byte(jo), &out)
				if err != nil {
					log_info("Unable to parse JSON from service: %s", err)
					return &out, err
				}
				return &out, nil
			}
		}
	}

	log_info("Call to app '%s' without handler for service '%s'", app, s)
	return &out, nil
}

func service_json(u *User, app string, s string, parameters ...any) ([]byte, error) {
	log_debug("Service call JSON: user='%d', app='%s', service='%s', parameters='%v'", u.ID, app, s, parameters)

	a := apps_by_name[app]
	if a == nil {
		log_info("Service call to unknown app '%s'", app)
		return []byte{}, error_message("Service call to unknown app '%s'", app)
	}

	switch a.Type {
	case "internal":
		for _, try := range []string{s, ""} {
			f, found := a.Internal.Services[try]
			if found {
				out := f(u, s, parameters...)
				jo, err := json.Marshal(out)
				if err != nil {
					log_warn("Unable to marshal app return data: %s", err)
					return jo, error_message("Unable to marshal app return data: %s", err)
				}
				return jo, nil
			}
		}

	case "wasm":
		for _, try := range []string{s, ""} {
			function, found := a.WASM.Services[try]
			if found {
				jo, err := wasm_run(u, a, function, map[string]any{"service": s, "parameters": parameters})
				if err != nil {
					log_info("Service returned error: %s", err)
					return []byte{}, err
				}
				return []byte(jo), nil
			}
		}
	}

	log_info("Call to app '%s' without handler for service '%s'", app, s)
	return []byte{}, nil
}

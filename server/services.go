// Comms server: Services
// Copyright Alistair Cunningham 2024

package main

func (a *App) register_service(service string, f func(int, string, string, string, ...any) any) {
	a.Internal.Services[service] = f
}

func service(user int, identity string, app string, s string, parameters ...any) {
	log_debug("Service: user='%d', app='%s', service='%s', parameters='%v'", user, app, s, parameters)

	a := apps[app]
	if a == nil {
		log_info("Service call to unknown app '%s'", app)
		return
	}

	switch a.Type {
	case "internal":
		for _, try := range []string{s, ""} {
			f, found := a.Internal.Services[try]
			if found {
				f(user, identity, app, s, parameters...)
				return
			}
		}

	case "wasm":
		for _, try := range []string{s, ""} {
			function, found := a.WASM.Services[try]
			if found {
				_, err := wasm_run(user, identity, a, function, 0, map[string]any{"service": s, "parameters": parameters})
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

func service_json(user int, identity string, app string, s string, depth int, parameters ...any) (string, error) {
	log_debug("Service JSON: user='%d', app='%s', service='%s', parameters='%v'", user, app, s, parameters)

	if depth > 1000 {
		log_warn("Service recursion detected; stopping at 1000 iterations")
		return "", error_message("Service recursion detected; stopping at 1000 iterations")
	}

	a := apps[app]
	if a == nil {
		log_info("Service call to unknown app '%s'", app)
		return "", error_message("Service call to unknown app '%s'", app)
	}

	switch a.Type {
	case "internal":
		for _, try := range []string{s, ""} {
			f, found := a.Internal.Services[try]
			if found {
				return json_encode(f(user, identity, app, s, parameters...)), nil
			}
		}

	case "wasm":
		for _, try := range []string{s, ""} {
			function, found := a.WASM.Services[try]
			if found {
				jo, err := wasm_run(user, identity, a, function, depth, map[string]any{"service": s, "parameters": parameters})
				if err != nil {
					log_info("Service returned error: %s", err)
					return "", err
				}
				return jo, nil
			}
		}
	}

	log_info("Call to app '%s' without handler for service '%s'", app, s)
	return "", nil
}

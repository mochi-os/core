// Mochi server: Starlark API
// Copyright Alistair Cunningham 2025

package main

import (
	"io"
	"strconv"
	"strings"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

const url_max_response_size = 100 * 1024 * 1024 // 100 MB

var (
	api_globals sl.StringDict
)

func init() {
	api_globals = sl.StringDict{
		"mochi": sls.FromStringDict(sl.String("mochi"), sl.StringDict{
			"access":     api_access,
			"app":        api_app,
			"attachment": api_attachment,
			"db":         api_db,
			"directory":  api_directory,
			"domain":     api_domain,
			"entity":     api_entity,
			"file":       api_file,
			"group":      api_group,
			"log":        api_log,
			"markdown": sls.FromStringDict(sl.String("mochi.markdown"), sl.StringDict{
				"render": sl.NewBuiltin("mochi.markdown.render", api_markdown_render),
			}),
			"message": api_message,
			"random": sls.FromStringDict(sl.String("mochi.random"), sl.StringDict{
				"alphanumeric": sl.NewBuiltin("mochi.random.alphanumeric", api_random_alphanumeric),
			}),
			"server": sls.FromStringDict(sl.String("mochi.server"), sl.StringDict{
				"version": sl.NewBuiltin("mochi.server.version", api_server_version),
			}),
			"service": sls.FromStringDict(sl.String("mochi.service"), sl.StringDict{
				"call": sl.NewBuiltin("mochi.service.call", api_service_call),
			}),
			"setting": api_setting,
			"stream":  sl.NewBuiltin("mochi.stream", api_stream),
			"user":    api_user,
			"time": sls.FromStringDict(sl.String("mochi.time"), sl.StringDict{
				"local": sl.NewBuiltin("mochi.time.local", api_time_local),
				"now":   sl.NewBuiltin("mochi.time.now", api_time_now),
			}),
			"uid": sl.NewBuiltin("mochi.uid", api_uid),
			"url": sls.FromStringDict(sl.String("mochi.url"), sl.StringDict{
				"delete": sl.NewBuiltin("mochi.url.delete", api_url_request),
				"get":    sl.NewBuiltin("mochi.url.get", api_url_request),
				"patch":  sl.NewBuiltin("mochi.url.patch", api_url_request),
				"post":   sl.NewBuiltin("mochi.url.post", api_url_request),
				"put":    sl.NewBuiltin("mochi.url.put", api_url_request),
			}),
			"valid":     sl.NewBuiltin("mochi.valid", api_valid),
			"websocket": api_websocket,
		}),
	}
}

// mochi.markdown.render(markdown) -> string: Render markdown to HTML
func api_markdown_render(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <markdown: string>")
	}

	in, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid markdown")
	}

	return sl_encode(string(markdown([]byte(in)))), nil
}

// mochi.random.alphanumeric(length) -> string: Generate a random alphanumeric string
func api_random_alphanumeric(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <length: integer>")
	}

	length, err := sl.AsInt32(args[0])
	if err != nil || length < 1 || length > 1000 {
		return sl_error(fn, "invalid length")
	}

	return sl_encode(random_alphanumeric(length)), nil
}

// mochi.service.call(service, function, params...) -> any: Call a function in another app
func api_service_call(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 2 {
		return sl_error(fn, "syntax: <service: string>, <function: string>, [parameters: variadic any]")
	}

	service, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid service")
	}

	function, ok := sl.AsString(args[1])
	if !ok {
		return sl_error(fn, "invalid function")
	}

	// Check for deep recursion
	depth := 1
	depth_var := t.Local("depth")
	if depth_var != nil {
		depth = depth_var.(int)
	}
	if depth > 1000 {
		return sl_error(fn, "reached maximum service recursion depth")
	}

	// Look for matching app function, using default if necessary
	a := app_for_service(service)
	if a == nil {
		return sl_error(fn, "unknown service %q", service)
	}
	f, found := a.active.Functions[function]
	if !found {
		f, found = a.active.Functions[""]
	}
	if !found {
		return sl_error(fn, "unknown function %q for service %q", function, service)
	}

	// Call function
	s := a.active.starlark()
	s.set("app", a)
	s.set("user", t.Local("user").(*User))
	s.set("owner", t.Local("owner").(*User))
	s.set("depth", depth+1)

	debug("mochi.service.call() calling app %q service %q function %q args: %+v", a.id, service, function, args[2:])
	var result sl.Value
	var err error

	if len(args) > 2 {
		result, err = s.call(f.Function, args[2:])
	} else {
		result, err = s.call(f.Function, nil)
	}
	debug("mochi.service.call() result '%+v', type %T", result, result)
	if err != nil {
		info("mochi.service.call() error: %v", err)
	}

	return result, err
}

// mochi.server.version() -> string: Get the server version
func api_server_version(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	return sl.String(build_version), nil
}

// mochi.stream(headers, content) -> Stream: Create a P2P stream to another entity
func api_stream(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return sl_error(fn, "syntax: <headers: dictionary>, <content: dictionary>")
	}

	headers := sl_decode_strings(args[0])
	if headers == nil {
		return sl_error(fn, "headers not specified or invalid")
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	db := db_open("db/users.db")
	from_valid, err := db.exists("select id from entities where id=? and user=?", headers["from"], user.ID)
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}
	if !from_valid {
		return sl_error(fn, "invalid from header")
	}

	if !valid(headers["to"], "entity") {
		return sl_error(fn, "invalid to header")
	}

	if !valid(headers["service"], "constant") {
		return sl_error(fn, "invalid service header")
	}

	if !valid(headers["event"], "constant") {
		return sl_error(fn, "invalid event header")
	}

	s, err := stream(headers["from"], headers["to"], headers["service"], headers["event"])
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	s.write(sl_decode(args[1]))
	return s, nil
}

// mochi.time.local(timestamp) -> dict: Convert Unix timestamp to local time in user's timezone
func api_time_local(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <timestamp: int64>")
	}

	var time int64
	var err error
	v := sl_decode(args[0])

	switch x := v.(type) {
	case int:
		time = int64(x)

	case int64:
		time = x

	case string:
		s, ok := sl.AsString(args[0])
		if !ok {
			return sl_error(fn, "invalid timestamp '%v'", args[0])
		}
		time, err = strconv.ParseInt(s, 10, 64)
		if err != nil {
			return sl_error(fn, "invalid timestamp '%v': %v", args[0], err)
		}

	default:
		return sl_error(fn, "invalid time type %T", x)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	return sl_encode(time_local(user, time)), nil
}

// mochi.time.now() -> int: Get the current Unix timestamp
func api_time_now(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	return sl_encode(now()), nil
}

// mochi.uid() -> string: Generate a unique ID
func api_uid(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	return sl_encode(uid()), nil
}

// mochi.url.get/post/put/patch/delete(url, options?, headers?, body?) -> dict: Make HTTP request
func api_url_request(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 || len(args) > 4 {
		return sl_error(fn, "syntax: <url: string>, [options: dictionary], [headers: dictionary], [body: string|dictionary]")
	}

	// Rate limit by app ID
	app, _ := t.Local("app").(*App)
	if app != nil && !rate_limit_url.allow(app.id) {
		return sl_error(fn, "rate limit exceeded (100 requests per minute)")
	}

	url, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid URL")
	}

	var options map[string]string
	if len(args) > 1 {
		options = sl_decode_strings(args[1])
	}

	var headers map[string]string
	if len(args) > 2 {
		headers = sl_decode_strings(args[2])
	}

	var body any
	if len(args) > 3 {
		body = sl_decode(args[3])
	}

	parts := strings.Split(fn.Name(), ".")
	r, err := url_request(parts[len(parts)-1], url, options, headers, body)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	defer r.Body.Close()

	data, _ := io.ReadAll(io.LimitReader(r.Body, url_max_response_size))
	return sl_encode(map[string]any{"status": r.StatusCode, "headers": r.Header, "body": string(data)}), nil
}

// mochi.valid(string, pattern) -> bool: Check if a string matches a validation pattern
func api_valid(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return sl_error(fn, "syntax: <string to check: string>, <pattern to match: string>")
	}

	s, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid string to check %q", s)
	}

	match, ok := sl.AsString(args[1])
	if !ok {
		return sl_error(fn, "invalid match pattern %q", match)
	}

	return sl_encode(valid(s, match)), nil
}

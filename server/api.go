// Mochi server: Starlark API
// Copyright Alistair Cunningham 2025

package main

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	gotime "time"

	sl "go.starlark.net/starlark"
	"go.starlark.net/starlarkjson"
	sls "go.starlark.net/starlarkstruct"
)

const url_max_response_size = 100 * 1024 * 1024 // 100 MB

var (
	api_globals sl.StringDict
)

func init() {
	api_globals = sl.StringDict{
		"json": starlarkjson.Module,
		"mochi": sls.FromStringDict(sl.String("mochi"), sl.StringDict{
			"access":     api_access,
			"account":    api_account,
			"app":        api_app,
			"attachment": api_attachment,
			"db":         api_db,
			"directory":  api_directory,
			"domain":     api_domain,
			"entity":     api_entity,
			"file":       api_file,
			"git":        api_git,
			"group":      api_group,
			"log":        api_log,
			"markdown": sls.FromStringDict(sl.String("mochi.markdown"), sl.StringDict{
				"render": sl.NewBuiltin("mochi.markdown.render", api_markdown_render),
			}),
			"message":    api_message,
			"permission": api_permission,
			"peer": sls.FromStringDict(sl.String("mochi.peer"), sl.StringDict{
				"connect": sls.FromStringDict(sl.String("mochi.peer.connect"), sl.StringDict{
					"url": sl.NewBuiltin("mochi.peer.connect.url", api_peer_connect_url),
				}),
			}),
			"remote":   api_remote,
			"schedule": api_schedule,
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
			"stream":  &streamModule{},
			"token":   api_token,
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
			"webpush":   api_webpush,
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

	// Capture calling app ID before switching context
	caller_id := ""
	if caller, ok := t.Local("app").(*App); ok && caller != nil {
		caller_id = caller.id
	}

	// Look for matching app function, using user preferences
	user, _ := t.Local("user").(*User)
	a := app_for_service_for(user, service)
	if a == nil {
		// Return None for missing service (allows graceful degradation during bootstrap)
		return sl.None, nil
	}
	av := a.active(user)
	if av == nil {
		return sl.None, nil
	}
	f, found := av.Functions[function]
	if !found {
		f, found = av.Functions[""]
	}
	if !found {
		return sl_error(fn, "unknown function %q for service %q", function, service)
	}

	// Run first-time setup for target service app (grants default permissions)
	app_user_setup(user, a.id)

	// Call function
	s := av.starlark()
	s.set("app", a)
	s.set("user", t.Local("user").(*User))
	s.set("owner", t.Local("owner").(*User))
	s.set("depth", depth+1)

	// Build call args based on target app's architecture version
	var call_args sl.Tuple
	if av.Architecture.Version >= 3 {
		// v3+: prepend context dict with caller app ID
		context := sl.NewDict(1)
		context.SetKey(sl.String("app"), sl.String(caller_id))
		if len(args) > 2 {
			call_args = make(sl.Tuple, 0, len(args)-1)
			call_args = append(call_args, context)
			call_args = append(call_args, args[2:]...)
		} else {
			call_args = sl.Tuple{context}
		}
	} else {
		// v2: original behavior
		if len(args) > 2 {
			call_args = args[2:]
		}
	}

	var result sl.Value
	var err error
	result, err = s.call(f.Function, call_args)
	if err != nil {
		info("mochi.service.call() error: %v", err)
	}

	return result, err
}

// mochi.server.version() -> string: Get the server version
func api_server_version(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	return sl.String(build_version), nil
}

// streamModule is a callable module that also has a .peer method
// Usage: mochi.stream(headers, content) or mochi.stream.peer(peer, headers, content)
type streamModule struct{}

func (m *streamModule) String() string        { return "mochi.stream" }
func (m *streamModule) Type() string          { return "module" }
func (m *streamModule) Freeze()               {}
func (m *streamModule) Truth() sl.Bool        { return sl.True }
func (m *streamModule) Hash() (uint32, error) { return 0, fmt.Errorf("unhashable type: module") }

func (m *streamModule) AttrNames() []string { return []string{"peer"} }

func (m *streamModule) Attr(name string) (sl.Value, error) {
	if name == "peer" {
		return sl.NewBuiltin("mochi.stream.peer", api_stream_peer), nil
	}
	return nil, nil
}

func (m *streamModule) Name() string { return "mochi.stream" }

func (m *streamModule) CallInternal(thread *sl.Thread, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	return api_stream(thread, nil, args, kwargs)
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

// mochi.stream.peer(peer, headers, content) -> Stream: Create a P2P stream to a specific peer
func api_stream_peer(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 3 {
		return sl_error(fn, "syntax: <peer: string>, <headers: dictionary>, <content: dictionary>")
	}

	peer, ok := sl.AsString(args[0])
	if !ok || peer == "" {
		return sl_error(fn, "peer not specified or invalid")
	}

	headers := sl_decode_strings(args[1])
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

	s, err := stream_to_peer(peer, headers["from"], headers["to"], headers["service"], headers["event"])
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	s.write(sl_decode(args[2]))
	return s, nil
}

// mochi.time.local(timestamp, format?) -> string: Convert Unix timestamp to local time in user's timezone
func api_time_local(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return sl_error(fn, "syntax: <timestamp: int64>, [format: string]")
	}

	var timestamp int64
	var err error
	v := sl_decode(args[0])

	switch x := v.(type) {
	case int:
		timestamp = int64(x)

	case int64:
		timestamp = x

	case string:
		s, ok := sl.AsString(args[0])
		if !ok {
			return sl_error(fn, "invalid timestamp '%v'", args[0])
		}
		timestamp, err = strconv.ParseInt(s, 10, 64)
		if err != nil {
			return sl_error(fn, "invalid timestamp '%v': %v", args[0], err)
		}

	default:
		return sl_error(fn, "invalid time type %T", x)
	}

	// Named formats
	format := gotime.DateTime
	if len(args) == 2 {
		f, ok := sl.AsString(args[1])
		if !ok {
			return sl_error(fn, "format must be a string")
		}
		switch f {
		case "datetime":
			format = gotime.DateTime
		case "date":
			format = gotime.DateOnly
		case "time":
			format = gotime.TimeOnly
		case "rfc822":
			format = gotime.RFC1123Z
		case "rfc3339":
			format = gotime.RFC3339
		default:
			return sl_error(fn, "unknown format %q (valid: datetime, date, time, rfc822, rfc3339)", f)
		}
	}

	// Get user's timezone
	user, _ := t.Local("user").(*User)
	timezone := "UTC"
	if user != nil {
		timezone = user_preference_get(user, "timezone", "UTC")
	}
	if timezone == "auto" {
		timezone = "UTC"
	}

	loc, err := gotime.LoadLocation(timezone)
	if err != nil {
		loc = gotime.UTC
	}

	return sl.String(gotime.Unix(timestamp, 0).In(loc).Format(format)), nil
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

	// Check url permission for external URLs
	if err := require_permission_url(t, fn, url); err != nil {
		return sl_error(fn, "%v", err)
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

// Mochi server: Starlark API
// Copyright Alistair Cunningham 2025-2026

package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	gotime "time"

	sl "go.starlark.net/starlark"
	"go.starlark.net/starlarkjson"
	sls "go.starlark.net/starlarkstruct"
	"golang.org/x/net/html"
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
			"ai":         api_ai,
			"app":        api_app,
			"attachment": api_attachment,
			"crypto": sls.FromStringDict(sl.String("mochi.crypto"), sl.StringDict{
				"equal": sl.NewBuiltin("mochi.crypto.equal", api_crypto_equal),
				"hmac": sls.FromStringDict(sl.String("mochi.crypto.hmac"), sl.StringDict{
					"sha256": sl.NewBuiltin("mochi.crypto.hmac.sha256", api_crypto_hmac_sha256),
				}),
			}),
			"db":         api_db,
			"directory":  api_directory,
			"domain":     api_domain,
			"entity":     api_entity,
			"file":       api_file,
			"git":        api_git,
			"group":      api_group,
			"interests":  api_interests,
			"log":        api_log,
			"message":    api_message,
			"permission": api_permission,
			"qid":        api_qid,
			"remote":     api_remote,
			"rss": sls.FromStringDict(sl.String("mochi.rss"), sl.StringDict{
				"fetch": sl.NewBuiltin("mochi.rss.fetch", api_rss_fetch),
			}),
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
			"text":    api_text,
			"token":   api_token,
			"user":    api_user,
			"time": sls.FromStringDict(sl.String("mochi.time"), sl.StringDict{
				"local": sl.NewBuiltin("mochi.time.local", api_time_local),
				"now":   sl.NewBuiltin("mochi.time.now", api_time_now),
			}),
			"uid": sl.NewBuiltin("mochi.uid", api_uid),
			"url": sls.FromStringDict(sl.String("mochi.url"), sl.StringDict{
				"delete":  sl.NewBuiltin("mochi.url.delete", api_url_request),
				"get":     sl.NewBuiltin("mochi.url.get", api_url_request),
				"patch":   sl.NewBuiltin("mochi.url.patch", api_url_request),
				"post":    sl.NewBuiltin("mochi.url.post", api_url_request),
				"preview": sl.NewBuiltin("mochi.url.preview", api_url_preview),
				"put":     sl.NewBuiltin("mochi.url.put", api_url_request),
			}),
			"webpush":   api_webpush,
			"websocket": api_websocket,
		}),
	}
}

// mochi.crypto.hmac.sha256(key, message) -> string: Hex-encoded HMAC-SHA256 digest
func api_crypto_hmac_sha256(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return sl_error(fn, "syntax: <key: string>, <message: string>")
	}
	key, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "key must be a string")
	}
	message, ok := sl.AsString(args[1])
	if !ok {
		return sl_error(fn, "message must be a string")
	}
	mac := hmac.New(sha256.New, []byte(key))
	mac.Write([]byte(message))
	return sl.String(hex.EncodeToString(mac.Sum(nil))), nil
}

// mochi.crypto.equal(a, b) -> bool: Constant-time string equality, suitable for
// comparing HMAC digests, tokens, and other secret-derived values without leaking
// timing information byte by byte.
func api_crypto_equal(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return sl_error(fn, "syntax: <a: string>, <b: string>")
	}
	a, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "a must be a string")
	}
	b, ok := sl.AsString(args[1])
	if !ok {
		return sl_error(fn, "b must be a string")
	}
	if subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1 {
		return sl.True, nil
	}
	return sl.False, nil
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
	a := app_for_service(user, service)
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

	// Enforce permission if declared on the function (skip when app calls its own service)
	if f.Permission != "" && caller_id != a.id {
		if !permission_granted(user, caller_id, f.Permission) {
			return sl_error(fn, "permission %q required to call %s/%s", f.Permission, service, function)
		}
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
	result, err = s.call(f.Function, call_args, kwargs)
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

	user, _ := t.Local("user").(*User)
	if user == nil {
		user, _ = t.Local("owner").(*User)
	}
	if user == nil {
		return sl_error(fn, "no user")
	}

	db := db_open("db/users.db")
	from_valid, err := db.exists("select id from entities where id=? and user=?", headers["from"], user.ID)
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}
	if !from_valid {
		if re, ok := t.Local("route_entity").(string); ok && re == headers["from"] {
			from_valid = true
		}
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

	app, _ := t.Local("app").(*App)
	from_app := ""
	var services []string
	if app != nil {
		from_app = app.id
		services = app_services(app, user)
	}

	s, err := stream(headers["from"], headers["to"], headers["service"], headers["event"], from_app, services)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	s.write(sl_decode(args[1]))

	// Register stream for cleanup when script returns
	streams, _ := t.Local("streams").([]*Stream)
	t.SetLocal("streams", append(streams, s))

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

	user, _ := t.Local("user").(*User)
	if user == nil {
		user, _ = t.Local("owner").(*User)
	}
	if user == nil {
		return sl_error(fn, "no user")
	}

	db := db_open("db/users.db")
	from_valid, err := db.exists("select id from entities where id=? and user=?", headers["from"], user.ID)
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}
	if !from_valid {
		if re, ok := t.Local("route_entity").(string); ok && re == headers["from"] {
			from_valid = true
		}
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

	app, _ := t.Local("app").(*App)
	from_app := ""
	var services []string
	if app != nil {
		from_app = app.id
		services = app_services(app, user)
	}

	s, err := stream_to_peer(peer, headers["from"], headers["to"], headers["service"], headers["event"], from_app, services)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	s.write(sl_decode(args[2]))

	// Register stream for cleanup when script returns
	streams, _ := t.Local("streams").([]*Stream)
	t.SetLocal("streams", append(streams, s))

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

// header_to_map converts http.Header to a flat map using the first value per key
func header_to_map(h http.Header) map[string]string {
	m := make(map[string]string, len(h))
	for k, v := range h {
		if len(v) > 0 {
			m[k] = v[0]
		}
	}
	return m
}

// mochi.url.get/post/put/patch/delete(url, options?, headers?, body?) -> dict: Make HTTP request
func api_url_request(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 || len(args) > 4 {
		return sl_error(fn, "syntax: <url: string>, [options: dictionary], [headers: dictionary], [body: string|dictionary]")
	}

	// Rate limit by app ID
	app, _ := t.Local("app").(*App)
	if app != nil && !rate_limit_url.allow(app.id) {
		return sl_encode(map[string]any{"status": 429, "headers": map[string]string{}, "body": ""}), nil
	}

	url, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid URL")
	}

	// Check url permission for external URLs
	if err := require_permission_url(t, fn, url); err != nil {
		return sl_encode(map[string]any{"status": 403, "headers": map[string]string{}, "body": ""}), nil
	}

	// Collect all granted url: domains for redirect validation
	var url_domains []string
	if app != nil {
		user, _ := t.Local("user").(*User)
		if user != nil && !app_is_internal(app) {
			db := db_user(user, "user")
			db.permissions_setup()
			rows, _ := db.rows("select object from permissions where app=? and permission='url' and granted=1", app.id)
			for _, row := range rows {
				if obj, ok := row["object"].(string); ok {
					url_domains = append(url_domains, obj)
				}
			}
		}
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
	r, err := url_request(parts[len(parts)-1], url, options, headers, body, url_domains...)
	if err != nil {
		return sl_encode(map[string]any{"status": 0, "headers": map[string]string{}, "body": ""}), nil
	}
	defer r.Body.Close()

	data, _ := io.ReadAll(io.LimitReader(r.Body, url_max_response_size))
	return sl_encode(map[string]any{"status": r.StatusCode, "headers": header_to_map(r.Header), "body": string(data)}), nil
}

// mochi.url.preview(url) -> string: Fetch a web page and return the URL of a
// preview image suitable for link cards. Reads the page's <meta property="og:image">
// (Open Graph) and falls back to <meta name="twitter:image">. Relative URLs in
// the meta tag are resolved against the page URL. Returns "" on any failure.
func api_url_preview(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <url: string>")
	}

	app, _ := t.Local("app").(*App)
	if app != nil && !rate_limit_url.allow(app.id) {
		return sl.String(""), nil
	}

	rawurl, ok := sl.AsString(args[0])
	if !ok || rawurl == "" {
		return sl.String(""), nil
	}

	r, err := url_request("GET", rawurl, map[string]string{"timeout": "10"}, map[string]string{"User-Agent": "Mochi/1.0"}, nil)
	if err != nil {
		return sl.String(""), nil
	}
	defer r.Body.Close()

	if r.StatusCode < 200 || r.StatusCode >= 300 {
		return sl.String(""), nil
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 64*1024))
	if err != nil {
		return sl.String(""), nil
	}

	return sl.String(url_extract_preview(body, rawurl)), nil
}

// url_extract_preview finds the og:image or twitter:image meta tag in HTML
// and resolves relative URLs against the page URL. Returns "" if neither tag
// is present or the head ends without one.
func url_extract_preview(body []byte, pageURL string) string {
	tokenizer := html.NewTokenizer(bytes.NewReader(body))
	var ogImage, twitterImage string

	for {
		tt := tokenizer.Next()
		if tt == html.ErrorToken {
			break
		}
		if tt == html.StartTagToken || tt == html.SelfClosingTagToken {
			tn, hasAttr := tokenizer.TagName()
			tagName := string(tn)

			if tagName == "body" {
				break
			}

			if tagName == "meta" && hasAttr {
				var property, name, content string
				for {
					key, val, more := tokenizer.TagAttr()
					k := string(key)
					v := string(val)
					switch k {
					case "property":
						property = v
					case "name":
						name = v
					case "content":
						content = v
					}
					if !more {
						break
					}
				}
				if property == "og:image" && content != "" {
					ogImage = content
				} else if twitterImage == "" && name == "twitter:image" && content != "" {
					twitterImage = content
				}
			}
		}
		if tt == html.EndTagToken {
			tn, _ := tokenizer.TagName()
			if string(tn) == "head" {
				break
			}
		}
	}

	result := ogImage
	if result == "" {
		result = twitterImage
	}
	if result == "" {
		return ""
	}

	// Resolve relative URLs
	base, err := url.Parse(pageURL)
	if err != nil {
		return result
	}
	ref, err := url.Parse(result)
	if err != nil {
		return result
	}
	return base.ResolveReference(ref).String()
}

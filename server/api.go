// Mochi server: Starlark API
// Copyright Alistair Cunningham 2025-2026

package main

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	gotime "time"

	cbor "github.com/fxamacker/cbor/v2"
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
			"broadcast":  api_broadcast,
			"crypto": sls.FromStringDict(sl.String("mochi.crypto"), sl.StringDict{
				"equal": sl.NewBuiltin("mochi.crypto.equal", api_crypto_equal),
				"hash": sls.FromStringDict(sl.String("mochi.crypto.hash"), sl.StringDict{
					"sha256": sl.NewBuiltin("mochi.crypto.hash.sha256", api_crypto_hash_sha256),
				}),
				"hmac": sls.FromStringDict(sl.String("mochi.crypto.hmac"), sl.StringDict{
					"sha256": sl.NewBuiltin("mochi.crypto.hmac.sha256", api_crypto_hmac_sha256),
				}),
			}),
			"db":          api_db,
			"directory":   api_directory,
			"document":    api_document,
			"domain":      api_domain,
			"entity":      api_entity,
			"file":        api_file,
			"git":         api_git,
			"group":       api_group,
			"interests":   api_interests,
			"log":         api_log,
			"message":     api_message,
			"permission":  api_permission,
			"qid":         api_qid,
			"remote":      api_remote,
			"replication": api_replication,
			"rss": sls.FromStringDict(sl.String("mochi.rss"), sl.StringDict{
				"fetch": sl.NewBuiltin("mochi.rss.fetch", api_rss_fetch),
			}),
			"schedule": api_schedule,
			"random": sls.FromStringDict(sl.String("mochi.random"), sl.StringDict{
				"alphanumeric": sl.NewBuiltin("mochi.random.alphanumeric", api_random_alphanumeric),
				"bytes":        sl.NewBuiltin("mochi.random.bytes", api_random_bytes),
				"choice":       sl.NewBuiltin("mochi.random.choice", api_random_choice),
				"integer":      sl.NewBuiltin("mochi.random.integer", api_random_integer),
				"unambiguous":  sl.NewBuiltin("mochi.random.unambiguous", api_random_unambiguous),
			}),
			"server": sls.FromStringDict(sl.String("mochi.server"), sl.StringDict{
				"id":      sl.NewBuiltin("mochi.server.id", api_server_id),
				"started": sl.NewBuiltin("mochi.server.started", api_server_started),
				"uptime":  sl.NewBuiltin("mochi.server.uptime", api_server_uptime),
				"version": sl.NewBuiltin("mochi.server.version", api_server_version),
				"update": sls.FromStringDict(sl.String("mochi.server.update"), sl.StringDict{
					"info":    sl.NewBuiltin("mochi.server.update.info", api_server_update_info),
					"install": sl.NewBuiltin("mochi.server.update.install", api_server_update_install),
				}),
			}),
			"service": sls.FromStringDict(sl.String("mochi.service"), sl.StringDict{
				"call":   sl.NewBuiltin("mochi.service.call", api_service_call),
				"exists": sl.NewBuiltin("mochi.service.exists", api_service_exists),
			}),
			"setting": api_setting,
			"stream":  &stream_module{},
			"text":    api_text,
			"token":   api_token,
			"user":    api_user,
			"time": sls.FromStringDict(sl.String("mochi.time"), sl.StringDict{
				"local": sl.NewBuiltin("mochi.time.local", api_time_local),
				"now":   sl.NewBuiltin("mochi.time.now", api_time_now),
				"parse": sl.NewBuiltin("mochi.time.parse", api_time_parse),
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

// mochi.crypto.hash.sha256(data) -> string: Hex-encoded SHA-256 digest of data.
// Accepts either a string or bytes — useful for hashing both text content
// (JSON, headers, ETag inputs) and binary data (file contents, random bytes
// from mochi.random.bytes).
func api_crypto_hash_sha256(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <data: string|bytes>")
	}
	var data []byte
	switch v := args[0].(type) {
	case sl.String:
		data = []byte(string(v))
	case sl.Bytes:
		data = []byte(v)
	default:
		return sl_error(fn, "data must be a string or bytes")
	}
	sum := sha256.Sum256(data)
	return sl.String(hex.EncodeToString(sum[:])), nil
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

// mochi.random.alphanumeric(length) -> string: Generate a cryptographically
// random string of `length` characters drawn from [0-9A-Za-z]. Length must be
// in 1..1000.
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

// mochi.random.bytes(length) -> bytes: Generate `length` cryptographically
// random bytes. Suitable for nonces, signing keys, salts. Length must be in
// 1..1024.
func api_random_bytes(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <length: integer>")
	}

	length, err := sl.AsInt32(args[0])
	if err != nil || length < 1 || length > 1024 {
		return sl_error(fn, "invalid length")
	}

	out := make([]byte, length)
	if _, err := rand.Read(out); err != nil {
		return sl_error(fn, "random read failed: %v", err)
	}
	return sl.Bytes(out), nil
}

// mochi.random.choice(list) -> any: Pick a uniformly random element from a
// non-empty list or tuple.
func api_random_choice(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <list: list|tuple>")
	}

	indexable, ok := args[0].(sl.Indexable)
	if !ok {
		return sl_error(fn, "argument must be a list or tuple")
	}
	n := indexable.Len()
	if n < 1 {
		return sl_error(fn, "list is empty")
	}

	idx, err := rand.Int(rand.Reader, big.NewInt(int64(n)))
	if err != nil {
		return sl_error(fn, "random read failed: %v", err)
	}
	return indexable.Index(int(idx.Int64())), nil
}

// mochi.random.integer(min, max) -> integer: Random integer in [min, max]
// inclusive. Errors if min > max.
func api_random_integer(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return sl_error(fn, "syntax: <min: integer>, <max: integer>")
	}

	mn, err := sl.AsInt32(args[0])
	if err != nil {
		return sl_error(fn, "invalid min")
	}
	mx, err := sl.AsInt32(args[1])
	if err != nil {
		return sl_error(fn, "invalid max")
	}
	if mn > mx {
		return sl_error(fn, "min (%d) must be <= max (%d)", mn, mx)
	}
	if mn == mx {
		return sl.MakeInt(mn), nil
	}

	span := big.NewInt(int64(mx - mn + 1))
	offset, err := rand.Int(rand.Reader, span)
	if err != nil {
		return sl_error(fn, "random read failed: %v", err)
	}
	return sl.MakeInt64(int64(mn) + offset.Int64()), nil
}

// mochi.random.unambiguous(length) -> string: Generate a cryptographically
// random string of `length` characters drawn from a 54-character alphabet that
// excludes confusable chars (0/1/O/I/l/i). For one-time codes, recovery codes,
// short shareable IDs that humans need to read or transcribe. Length must be
// in 1..1000.
func api_random_unambiguous(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <length: integer>")
	}

	length, err := sl.AsInt32(args[0])
	if err != nil || length < 1 || length > 1000 {
		return sl_error(fn, "invalid length")
	}

	return sl_encode(random_unambiguous(length)), nil
}

// mochi.service.exists(service) -> bool: Report whether any installed app handles the named service for the current user
func api_service_exists(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <service: string>")
	}
	service, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid service")
	}
	user, _ := t.Local("user").(*User)
	return sl.Bool(app_for_service(user, service) != nil), nil
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

	// Refuse service calls into an account whose per-user replication
	// backfill hasn't finished — its DBs are mid-transfer. Parallels
	// the web_action gate; honours "don't let users use Mochi apps
	// until replication is up to date".
	if user != nil && user.Status == "pending-replication" {
		return sl_error(fn, "account %q is still being replicated", user.UID)
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

// service_call_as_server invokes a service function from the running Mochi
// server itself rather than from a calling app. The handler sees app="" in
// its v3+ context dict; the notifications app treats that as the reserved
// "Mochi Server" sender id (see apps/notifications/notifications.star).
//
// target_user is the user whose app context the call runs in — for instance
// the admin whose notifications.db should receive the row. Suspended users
// or users without an identity are skipped silently and a nil error is
// returned.
//
// args is encoded as kwargs onto the Starlark function call; positional
// parameters after the prepended context dict are not used.
func service_call_as_server(target_user_uid string, service string, function string, args Map) error {
	user := user_by_uid(target_user_uid)
	if user == nil {
		return nil
	}
	// Skip silently while the target's per-user replication backfill is
	// in progress — same treatment as suspended users. A server-side
	// call (e.g. a notification row) would open a DB that's mid-swap.
	if user.Status == "pending-replication" {
		return nil
	}
	a := app_for_service(user, service)
	if a == nil {
		return fmt.Errorf("no app for service %q", service)
	}
	av := a.active(user)
	if av == nil {
		return fmt.Errorf("app %q has no active version", a.id)
	}
	f, found := av.Functions[function]
	if !found {
		f, found = av.Functions[""]
	}
	if !found {
		return fmt.Errorf("unknown function %q for service %q", function, service)
	}

	app_user_setup(user, a.id)

	s := av.starlark()
	s.set("app", a)
	s.set("user", user)
	s.set("owner", user)
	s.set("depth", 1)

	var call_args sl.Tuple
	if av.Architecture.Version >= 3 {
		ctx := sl.NewDict(2)
		ctx.SetKey(sl.String("app"), sl.String(""))
		ctx.SetKey(sl.String("_server"), sl.Bool(true))
		call_args = sl.Tuple{ctx}
	}

	kwargs := make([]sl.Tuple, 0, len(args))
	for k, v := range args {
		kwargs = append(kwargs, sl.Tuple{sl.String(k), sl_encode(v)})
	}

	_, err := s.call(f.Function, call_args, kwargs)
	return err
}

// mochi.server.id() -> string: Get the local libp2p peer ID for this server
func api_server_id(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	return sl.String(p2p_id), nil
}

// mochi.server.started() -> int: Unix timestamp (seconds) when this server process started
func api_server_started(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	return sl.MakeInt64(server_started_at.Unix()), nil
}

// mochi.server.uptime() -> int: Seconds since this server process started
func api_server_uptime(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	return sl.MakeInt64(int64(gotime.Since(server_started_at).Seconds())), nil
}

// mochi.server.version() -> string: Get the server version
func api_server_version(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	return sl.String(build_version), nil
}

// mochi.server.update.info() -> dict: Information about available server upgrades.
// Returns {available, current, latest, platform, track, checked, pending}:
//
//	available  bool   — true if a newer version is available on the configured track
//	current    string — running version
//	latest     string — latest version observed in the daily check ("" if never run)
//	platform   string — packaging tag: "linux-deb", "linux-rpm", "macos-arm64",
//	                    "macos-amd64", "windows", "docker", or "" (dev / unknown)
//	track      string — currently always "production"
//	checked    int    — Unix timestamp of the last successful check (0 if never)
//	pending    string — version currently being installed ("" if none)
func api_server_update_info(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	latest := setting_get("update_notified", "")
	checked, _ := strconv.ParseInt(setting_get("update_checked", "0"), 10, 64)
	pending := setting_get("update_pending", "")
	platform := update_platform_full()
	available := latest != "" && version_compare(latest, build_version) > 0
	return sl_encode(map[string]any{
		"available": available,
		"current":   build_version,
		"latest":    latest,
		"platform":  platform,
		"track":     update_track,
		"checked":   checked,
		"pending":   pending,
	}), nil
}

// mochi.server.update.install([version]) -> dict: Trigger an unattended
// self-install of the latest known upgrade (or the given version) on
// Windows. Returns {pending: <version>} on success. Errors on platforms
// that don't support self-install (currently anything except Windows).
//
// Caller is responsible for admin-gating; the settings app's action
// wrapper does this via require_admin.
func api_server_update_install(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var version string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "version?", &version); err != nil {
		return nil, err
	}
	if version == "" {
		version = setting_get("update_notified", "")
	}
	if err := update_install_start(version); err != nil {
		return sl_error(fn, "%v", err)
	}
	return sl_encode(map[string]any{"pending": version}), nil
}

// stream_module is a callable module that also has a .peer method
// Usage: mochi.stream(headers, content) or mochi.stream.peer(peer, headers, content)
type stream_module struct{}

func (m *stream_module) String() string        { return "mochi.stream" }
func (m *stream_module) Type() string          { return "module" }
func (m *stream_module) Freeze()               {}
func (m *stream_module) Truth() sl.Bool        { return sl.True }
func (m *stream_module) Hash() (uint32, error) { return 0, fmt.Errorf("unhashable type: module") }

func (m *stream_module) AttrNames() []string { return []string{"peer"} }

func (m *stream_module) Attr(name string) (sl.Value, error) {
	if name == "peer" {
		return sl.NewBuiltin("mochi.stream.peer", api_stream_peer), nil
	}
	return nil, nil
}

func (m *stream_module) Name() string { return "mochi.stream" }

func (m *stream_module) CallInternal(thread *sl.Thread, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
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
	from_valid, err := db.exists("select id from entities where id=? and user=?", headers["from"], user.UID)
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
	from_valid, err := db.exists("select id from entities where id=? and user=?", headers["from"], user.UID)
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

// mochi.time.parse(s, format?) -> int | None: Parse a string into a Unix
// timestamp. The inverse of mochi.time.local. Default format is "rfc3339" —
// the format used by virtually every JSON API. Returns None on any parse
// error so callers can substitute a fallback. Same five named formats as
// local: datetime, date, time, rfc822, rfc3339. For datetime/date/time
// (which carry no timezone), the user's timezone preference is assumed —
// matching local's direction so parse(local(ts)) round-trips.
func api_time_parse(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return sl_error(fn, "syntax: <s: string>, [format: string]")
	}

	s, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "s must be a string")
	}
	if s == "" {
		return sl.None, nil
	}

	format := gotime.RFC3339
	carries_tz := true
	if len(args) == 2 {
		f, ok := sl.AsString(args[1])
		if !ok {
			return sl_error(fn, "format must be a string")
		}
		switch f {
		case "datetime":
			format = gotime.DateTime
			carries_tz = false
		case "date":
			format = gotime.DateOnly
			carries_tz = false
		case "time":
			format = gotime.TimeOnly
			carries_tz = false
		case "rfc822":
			format = gotime.RFC1123Z
			carries_tz = true
		case "rfc3339":
			format = gotime.RFC3339
			carries_tz = true
		default:
			return sl_error(fn, "unknown format %q (valid: datetime, date, time, rfc822, rfc3339)", f)
		}
	}

	if carries_tz {
		parsed, err := gotime.Parse(format, s)
		if err != nil {
			return sl.None, nil
		}
		return sl.MakeInt64(parsed.Unix()), nil
	}

	// Naive format — assume the user's timezone (mirroring local's direction)
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

	parsed, err := gotime.ParseInLocation(format, s, loc)
	if err != nil {
		return sl.None, nil
	}
	return sl.MakeInt64(parsed.Unix()), nil
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

	// idempotency_key kwarg: caller-supplied stable key derived from the
	// source event UID so a replayed call (server restart, host failover,
	// queue retry) doesn't produce a duplicate side-effect at the remote
	// API. Stripe and other modern APIs honour the Idempotency-Key header
	// natively; for APIs that don't, the per-app _idempotent_calls cache
	// (below) suppresses the duplicate request before it leaves.
	var idempotency_key string
	for _, kw := range kwargs {
		k, _ := sl.AsString(kw[0])
		if k == "idempotency_key" {
			if v, ok := sl.AsString(kw[1]); ok && v != "" {
				idempotency_key = v
				if headers == nil {
					headers = map[string]string{}
				}
				headers["Idempotency-Key"] = v
			}
		}
	}

	// Response cache: when idempotency_key is set and we have a user+app
	// context, check the per-(user, app) cache for a recent response with
	// the same key. A hit returns the cached response without making
	// another HTTP request — the safety net for APIs that ignore the
	// Idempotency-Key header.
	user, _ := t.Local("user").(*User)
	if idempotency_key != "" && app != nil && user != nil {
		if cached := url_idempotency_lookup(user, app, idempotency_key); cached != nil {
			return sl_encode(cached), nil
		}
	}

	parts := strings.Split(fn.Name(), ".")
	r, err := url_request(parts[len(parts)-1], url, options, headers, body, url_domains...)
	if err != nil {
		return sl_encode(map[string]any{"status": 0, "headers": map[string]string{}, "body": ""}), nil
	}
	defer r.Body.Close()

	data, _ := io.ReadAll(io.LimitReader(r.Body, url_max_response_size))
	response := map[string]any{"status": r.StatusCode, "headers": header_to_map(r.Header), "body": string(data)}

	// Cache the response for future replays with the same key. Only when
	// the request actually reached the server (StatusCode > 0) — network
	// errors stay un-cached so the caller can retry.
	if idempotency_key != "" && app != nil && user != nil && r.StatusCode > 0 {
		url_idempotency_store(user, app, idempotency_key, r.StatusCode, header_to_map(r.Header), data)
	}

	return sl_encode(response), nil
}

const url_idempotency_ttl int64 = 3600 // 1 hour

// url_idempotency_lookup returns a cached response for the given key, or
// nil when no entry exists or the entry has aged out. Stale rows are
// purged opportunistically.
func url_idempotency_lookup(u *User, a *App, key string) map[string]any {
	sysdb := db_app_system(u, a)
	if sysdb == nil {
		return nil
	}
	sysdb.exec("create table if not exists _idempotent_calls (key text primary key, status integer not null, headers blob, body blob, ts integer not null)")
	sysdb.exec("delete from _idempotent_calls where ts < ?", now()-url_idempotency_ttl)

	row, _ := sysdb.row("select status, headers, body from _idempotent_calls where key=? and ts > ?", key, now()-url_idempotency_ttl)
	if row == nil {
		return nil
	}
	status, _ := row["status"].(int64)
	var headers map[string]string
	if hb, ok := row["headers"].([]byte); ok {
		_ = cbor.Unmarshal(hb, &headers)
	} else if hs, ok := row["headers"].(string); ok {
		_ = cbor.Unmarshal([]byte(hs), &headers)
	}
	if headers == nil {
		headers = map[string]string{}
	}
	var body string
	switch v := row["body"].(type) {
	case []byte:
		body = string(v)
	case string:
		body = v
	}
	return map[string]any{"status": int(status), "headers": headers, "body": body}
}

// url_idempotency_store records (key → response) in the per-app cache.
// Headers are CBOR-encoded for round-trip fidelity (sqlite blob).
func url_idempotency_store(u *User, a *App, key string, status int, headers map[string]string, body []byte) {
	sysdb := db_app_system(u, a)
	if sysdb == nil {
		return
	}
	sysdb.exec("create table if not exists _idempotent_calls (key text primary key, status integer not null, headers blob, body blob, ts integer not null)")
	sysdb.exec("insert or replace into _idempotent_calls (key, status, headers, body, ts) values (?, ?, ?, ?, ?)",
		key, status, cbor_encode(headers), body, now())
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

	// Use a recognizable, Mozilla-prefixed UA so sites that gate content on
	// "browser-ish" user-agents still serve us their og:image-bearing HTML
	// rather than a stripped/anti-bot variant. The self-identifying URL lets
	// responsible operators throttle deliberately without us trying to evade
	// detection.
	r, err := url_request("GET", rawurl,
		map[string]string{"timeout": "10"},
		map[string]string{
			"User-Agent": "Mozilla/5.0 (compatible; MochiBot/1.0; +https://mochi-os.org)",
			"Accept":     "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
		}, nil)
	if err != nil {
		return sl.String(""), nil
	}
	defer r.Body.Close()

	if r.StatusCode < 200 || r.StatusCode >= 300 {
		return sl.String(""), nil
	}

	// Stream-parse rather than reading the whole body into memory. The
	// parser breaks at <body> (or `</head>`) so we usually consume <100 KB.
	// The LimitReader cap is a safety bound for pathological pages — 16 MB
	// is well past any real-world <head> length (heavy news/media sites
	// rarely exceed 500 KB, even with embedded preload/JSON-LD/analytics)
	// while still preventing a malicious endpoint from streaming gigabytes.
	return sl.String(url_extract_preview(io.LimitReader(r.Body, 16*1024*1024), rawurl)), nil
}

// url_extract_preview finds the og:image or twitter:image meta tag in HTML
// and resolves relative URLs against the page URL. Returns "" if neither tag
// is present or the head ends without one. Reads incrementally so callers
// stop paying memory cost once <body> is reached.
func url_extract_preview(body io.Reader, pageURL string) string {
	tokenizer := html.NewTokenizer(body)
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

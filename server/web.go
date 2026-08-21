// Mochi server: Sample web interface
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	sl "go.starlark.net/starlark"
	"golang.org/x/crypto/acme"
	"golang.org/x/net/netutil"
)

var (
	match_react = regexp.MustCompile(`assets/.*-[\w-]{8}\.(js|css)$`)
	web_https   = false
	// Redact credential query values from the access log: ?token=, and the OAuth
	// callback's ?code= and ?state=. Anchored to a query delimiter so it never
	// matches a substring such as ?barcode=.
	web_log_secret_query = regexp.MustCompile(`([?&](?:token|code|state)=)[^&]*`)
)

// web_server builds a listener with explicit connection limits: gin's r.Run
// leaves every timeout unset, so any client can hold a goroutine and descriptor
// indefinitely. ReadTimeout and WriteTimeout are deliberately NOT set - they
// bound the whole request and response, breaking git packs, large uploads and
// websockets.
func web_server(addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
}

// Default ceiling on simultaneously accepted connections: half the fd budget
// the packaged unit grants (LimitNOFILE=65535), leaving the rest for databases,
// peer connections and app files. A fixed ceiling, not a throughput limit.
const web_connections_default = 32768

// web_listen opens a listener bounded by the configured connection ceiling.
// Applied to the raw TCP listener, so a connection counts from accept rather
// than from the end of the TLS handshake; past the ceiling excess waits in the
// kernel backlog, holding no descriptor of ours.
func web_listen(addr string, maximum int) (net.Listener, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	if maximum <= 0 {
		return listener, nil
	}
	return netutil.LimitListener(listener, maximum), nil
}

// web_connections_maximum is the configured ceiling, zero or below meaning
// unlimited. Read separately from web_listen so the ceiling can be chosen
// without opening a socket, and a listener tested without global config.
func web_connections_maximum() int {
	return ini_int("web", "connections", web_connections_default)
}

// web_serve_tls serves HTTPS on :443 and ACME/redirect on :80. Not autotls: it
// overwrites the tls.Config's GetCertificate with the bare autocert manager's,
// discarding domains_get_certificate and every manually installed certificate.
// Domain, verification and TLS checks stay with the manager's HostPolicy.
func web_serve_tls(handler http.Handler) error {
	tls_config := web_tls_config()

	errors := make(chan error, 2)

	// HTTP-01 challenges and the plain-HTTP redirect, as autotls served them.
	go func() {
		s := web_server(":80", domains_acme_manager.HTTPHandler(http.HandlerFunc(web_redirect_https)))
		listener, err := web_listen(":80", web_connections_maximum())
		if err != nil {
			errors <- err
			return
		}
		errors <- s.Serve(listener)
	}()

	go func() {
		s := web_server(":443", handler)
		s.TLSConfig = tls_config
		listener, err := web_listen(":443", web_connections_maximum())
		if err != nil {
			errors <- err
			return
		}
		// The bound counts connections from accept, so one stalled in the TLS
		// handshake occupies a slot exactly as a completed one does.
		errors <- s.ServeTLS(listener, "", "")
	}()

	return <-errors
}

// web_tls_config builds the TLS configuration the HTTPS listener serves with.
// Separated from web_serve_tls so a test can exercise the configuration the
// server actually uses, rather than a copy of it that could drift.
func web_tls_config() *tls.Config {
	return &tls.Config{
		// domains_get_certificate tries a manually installed certificate first and
		// only then ACME. Must not be replaced with the bare manager's.
		GetCertificate: domains_get_certificate,
		// acme-tls/1 must stay advertised or TLS-ALPN-01 validation breaks:
		// autocert answers those challenges through GetCertificate, and
		// domains_get_certificate falls through to the manager for any domain
		// without a manual certificate — which is exactly where they land.
		NextProtos: []string{"h2", "http/1.1", acme.ALPNProto},
	}
}

// web_https_serves reports whether the HTTPS listener could present a
// certificate for host, following domains_get_certificate: the manual map
// first, then the ACME manager's HostPolicy. Order matters - the policy governs
// issuance only, so consulting it first refuses a hand-certificated host, the
// only kind a wildcard is.
func web_https_serves(host string) bool {
	if domains_manual_cert(host) != nil {
		return true
	}
	if domains_acme_manager == nil {
		return false // nothing left to issue one
	}
	return domains_host_policy(context.Background(), host) == nil
}

// web_redirect_https redirects plain HTTP to HTTPS, only for a host HTTPS can
// actually answer for - Host is caller-supplied. The Location carries the
// requested name minus any port: a wildcard row's Domain is a pattern, not a
// destination.
func web_redirect_https(w http.ResponseWriter, r *http.Request) {
	host := r.Host
	if name, _, err := net.SplitHostPort(host); err == nil {
		host = name
	}
	if !web_https_serves(host) {
		http.NotFound(w, r)
		return
	}
	http.Redirect(w, r, "https://"+host+r.RequestURI, http.StatusMovedPermanently)
}

// Redact credential query values from a request path before it reaches the
// access log. gin's logger includes the query string, so without this the log
// would hold replayable credentials.
func web_log_redact(path string) string {
	if !strings.Contains(path, "=") {
		return path
	}
	return web_log_secret_query.ReplaceAllString(path, "${1}redacted")
}

// routing names how an action was reached, surfaced to Starlark as a.routing.
// Declared by each dispatch site: only the dispatcher knows whether the entity
// was chosen by the caller (path, direct) or configured by the operator
// (domain).
const (
	routing_class  = "class"  // /<app>/-/<action>, no entity
	routing_path   = "path"   // /<app>/<entity>/-/<action>, entity from the URL
	routing_direct = "direct" // /<entity>/-/<action>, app resolved from the class
	routing_domain = "domain" // domain route, method=entity, entity from the target
	routing_hosted = "hosted" // domain route, method=app, no entity
)

// web_action_error renders an action that aborted. Starlark has no try/except,
// so any builtin refusing unwinds the whole action and lands here; the error
// type is the only signal for the status. A refusal must not be a 500 - clients
// retry those.
func web_action_error(c *gin.Context, app string, err error) {
	var permission *PermissionError
	if errors.As(err, &permission) {
		// A machine-readable code plus the fields the permission dialog needs,
		// which respond_error cannot carry. The client renders the text.
		c.JSON(http.StatusForbidden, gin.H{ // i18n-ok
			"error":      "permission_required",
			"app":        app,
			"permission": permission.Permission,
			"restricted": permission.Restricted,
		})
		return
	}

	var limit *RateLimitError
	if errors.As(err, &limit) {
		// Retry-After carries the wait; the body carries a translated label with no
		// numbers. Which budget, and how large, goes to the log only.
		if limit.Retry > 0 {
			c.Header("Retry-After", strconv.Itoa(limit.Retry))
		}
		// info, not warn: warn mails the administrator, and a limiter refusing a
		// caller is the limiter working. One line per app per minute, so a flood
		// cannot drive our log volume. The detail, not the wrapped error - sl_error
		// repeats itself.
		if rate_limit_refusal_log.allow(app) {
			info("web: %s rate limited (%s), retry after %ds", app, limit.detail, limit.Retry)
		}
		respond_error(c, http.StatusTooManyRequests,
			"rate_limit_exceeded_please_try_again_later", "errors.rate_limit_exceeded", nil)
		return
	}

	// The detail goes to the log, not to the caller: err carries the internal
	// Starlark function name and, when the failure came from the database, the
	// driver's own message.
	warn("web: %s action failed: %s", app, path_scrub(err.Error()))
	respond_error(c, http.StatusInternalServerError, "server_error", "errors.server_error", nil)
}

func web_action(c *gin.Context, a *App, name string, e *Entity, routing string) bool {
	if a == nil {
		return false
	}

	var user *User
	var api_token *Token
	var jwt_app string
	var has_bearer bool

	// Check query parameter token first (for RSS feeds, attachments in sandboxed iframes, etc.)
	// This takes priority over cookies so RSS tokens work in logged-in browsers
	if query_token := c.Query("token"); query_token != "" {
		if strings.HasPrefix(query_token, "mochi-") {
			// API token
			api_token = token_validate(query_token)
			if api_token != nil {
				user = user_by_uid(api_token.User)
				if user == nil {
					debug("Query token valid but user %q not found", api_token.User)
					api_token = nil
				}
			}
		} else {
			// JWT token (used by sandboxed iframes for resource URLs like images)
			if uid, app, err := jwt_verify(query_token); err == nil && uid != "" {
				user = user_by_uid(uid)
				if user != nil {
					jwt_app = app
					has_bearer = true // treat as bearer-authenticated
				}
			}
		}
	}

	// Get user authentication via cookie (needed for version selection)
	if user == nil {
		user = web_auth(c)
	}

	// Always extract the Bearer token for app authorization, even when the session
	// cookie already identified the user. has_bearer means a token verified, never
	// that a header was sent: the prefix alone made "Bearer x" satisfy the app
	// gate.
	auth_header := c.GetHeader("Authorization")
	if strings.HasPrefix(auth_header, "Bearer ") {
		bearer := strings.TrimPrefix(auth_header, "Bearer ")
		if strings.HasPrefix(bearer, "mochi-") {
			// API token authentication
			if api_token == nil {
				api_token = token_validate(bearer)
				if api_token != nil {
					has_bearer = true
					if user == nil {
						user = user_by_uid(api_token.User)
						if user == nil {
							debug("API token valid but user %q not found", api_token.User)
							api_token = nil
							has_bearer = false
						}
					}
				}
			}
		} else {
			// JWT authentication — extract app claim for authorization
			if uid, app, err := jwt_verify(bearer); err == nil && uid != "" {
				jwt_app = app
				has_bearer = true
				if user == nil {
					if u := user_by_uid(uid); u != nil {
						user = u
					} else {
						debug("API JWT token valid but user %q not found", uid)
					}
				}
			} else if user == nil {
				debug("API JWT token verification failed: %v", err)
			}
		}
	}

	// Get the app version for this user (user preference or default)
	av := a.active(user)
	if av == nil {
		return false
	}
	if dev_reload {
		av.reload()
	}

	// API tokens are restricted to their app
	if api_token != nil && api_token.App != a.id {
		respond_error(c, http.StatusForbidden, "token_not_valid_for_this_app", "errors.app_token_invalid", nil)
		return true
	}

	// Block app actions for users whose restore hasn't completed; /login stays
	// reachable for the waiting page. Reaching an app mid-restore can corrupt the
	// running database connection while the restore renames files underneath it.
	if user_pending(user) && !app_is_login(a) {
		// A browser navigating to a gated app loads HTML, not XHR, and the SPA never
		// runs - a JSON error renders as raw JSON in the page. Redirect HTML
		// navigations; API callers still get JSON.
		accept := c.GetHeader("Accept")
		html := strings.Contains(accept, "text/html") && !strings.Contains(accept, "application/json")
		if user.Status == "pending-restore" {
			if html {
				c.Redirect(http.StatusFound, app_login_route("restore"))
			} else {
				respond_error(c, http.StatusServiceUnavailable, "restore_in_progress", "errors.restore_in_progress", nil)
			}
		} else {
			// Legacy pending states (pre-removal replication links) can no
			// longer complete; send the user to /login.
			if html {
				c.Redirect(http.StatusFound, app_login_route(""))
			} else {
				respond_error(c, http.StatusServiceUnavailable, "restore_in_progress", "errors.restore_in_progress", nil)
			}
		}
		c.Abort()
		return true
	}

	// Block app actions for accounts pending closure: the session is valid only to
	// reach the reactivation interstitial, which lives in /login and stays
	// reachable. HTML navigations redirect there; API callers get the JSON 403.
	if user != nil && user.Status == "closing" && !app_is_login(a) {
		accept := c.GetHeader("Accept")
		if strings.Contains(accept, "text/html") && !strings.Contains(accept, "application/json") {
			c.Redirect(http.StatusFound, app_login_route("closing"))
		} else {
			respond_error(c, http.StatusForbidden, "account_closing", "errors.account_closing", nil)
		}
		c.Abort()
		return true
	}

	// Run first-time setup for this user and app (grants default permissions)
	app_user_setup(user, a.id)

	// Built-in catalog endpoint: /<app>/-/labels and /<app>/-/labels/<tag>.
	// Public — used by tooling (Translate Mochi app, dev introspection).
	// The web SPA bundles its own Lingui catalogs and does not call this.
	if name == "-/labels" || strings.HasPrefix(name, "-/labels/") {
		return web_serve_labels(c, av, strings.TrimPrefix(name, "-/labels"))
	}

	// When entity is provided via domain routing, try entity-prefixed actions.
	// Skip this for main site routing where action already includes fingerprint (e.g., "abc123/-/info").
	// Also skip when action is the entity's fingerprint itself (viewing entity root).
	// For browser requests (Accept: text/html), try the non-API action first to serve HTML.
	var aa *AppAction
	accept := c.GetHeader("Accept")
	prefer_html := strings.Contains(accept, "text/html") && !strings.Contains(accept, "application/json")
	if e != nil && e.Class != "" && name != e.Fingerprint {
		if name == "" {
			// Entity root (e.g., /) - use :feed action
			entity_action := ":" + e.Class
			aa = av.find_action(entity_action)
		} else if strings.HasPrefix(name, "-/") {
			// API path (e.g., -/info) - convert to :wiki/-/info
			entity_action := ":" + e.Class + "/" + name
			aa = av.find_action(entity_action)
		} else if !strings.Contains(name, "/") {
			// Simple name (e.g., concepts) - try with entity prefix
			if prefer_html {
				// Try HTML action first (e.g., :wiki/:page), then API action
				html_action := ":" + e.Class + "/" + name
				aa = av.find_action(html_action)
				if aa == nil {
					entity_action := ":" + e.Class + "/-/" + name
					aa = av.find_action(entity_action)
				}
			} else {
				// Try API action first for non-browser requests
				entity_action := ":" + e.Class + "/-/" + name
				aa = av.find_action(entity_action)
			}
		}
	}
	if aa == nil {
		aa = av.find_action(name)
	}
	if aa == nil {
		return false
	}

	// A token may be bound to one action and entity: routing ignores the method,
	// so without this an RSS token is equally good on the app's delete action.
	// Matched on the action pattern (":wiki/-/rss"); unbound tokens stay app-wide.
	if api_token != nil {
		entity_id := ""
		if e != nil {
			entity_id = e.ID
		}
		if !token_allows(api_token, aa.name, entity_id) {
			debug("403 token not valid for action: app=%s action=%s entity=%q token_action=%q token_entity=%q", a.id, aa.name, entity_id, api_token.Action, api_token.Entity)
			respond_error(c, http.StatusForbidden, "token_not_valid_for_this_action", "errors.app_token_action", nil)
			return true
		}
	}

	// Compute owner based on entity, domain route owner, or authenticated user
	var owner *User = user
	if e != nil {
		if o := user_owning_entity(e.ID); o != nil {
			owner = o
		}
	} else if owner == nil {
		// Fall back to domain route owner for anonymous requests without entity
		if route_owner, ok := c.Get("domain_owner"); ok {
			if uid, ok := route_owner.(string); ok && uid != "" {
				owner = user_by_uid(uid)
			}
		}
		// Fall back to first administrator for public class-level actions
		if owner == nil && aa.Public {
			udb := db_open("db/users.db")
			if row, _ := udb.row("select uid from users where role='administrator' order by uid limit 1"); row != nil {
				if admin_uid, _ := row["uid"].(string); admin_uid != "" {
					owner = user_by_uid(admin_uid)
				}
			}
		}
	}

	// Handle git Smart HTTP protocol for domain-routed repository entities.
	// Git clients send requests to /info/refs, /git-upload-pack, /git-receive-pack
	// directly under the entity URL, bypassing standard app action routing.
	if e != nil && e.Class == "repository" {
		if name == "info/refs" || name == "git-upload-pack" || name == "git-receive-pack" {
			return git_http_handler_entity(c, a, owner, user, e, name)
		}
	}

	// Static assets from sandboxed iframes carry no cookies (opaque origin) and
	// are public, so they skip auth. True only when a file will actually be
	// served, not merely declared, and not narrowed to iframes - deep links carry
	// no Bearer.
	shell_static := web_serves_file(c, aa) || (aa.Files != "" && aa.filepath != "")

	// Require authentication for non-public actions
	if user == nil && !aa.Public {
		// Top-level browser navigations: redirect to login page at /
		// This applies even for file-serving actions (shell_static) — only iframe
		// requests should bypass auth for static file loading.
		if strings.Contains(c.GetHeader("Accept"), "text/html") && !web_is_iframe_request(c) {
			// If user has a session cookie but auth failed (suspended, expired, etc),
			// clear the invalid cookie to prevent redirect loops
			if web_cookie_get(c, "session", "") != "" {
				audit_session_anomaly("", rate_limit_client_ip(c), "invalid_session")
				web_cookie_unset(c, "session")
				c.Redirect(http.StatusFound, "/?reauth=1")
			} else {
				c.Redirect(http.StatusFound, "/")
			}
			return true
		}
		if !shell_static {
			respond_error(c, http.StatusUnauthorized, "authentication_required", "errors.authentication_required", nil)
			return true
		}
	}

	// Require identity for authenticated users accessing non-login apps
	if user != nil && !app_is_login(a) && !aa.Public {
		if user.identity() == nil {
			if strings.Contains(c.GetHeader("Accept"), "text/html") {
				c.Redirect(http.StatusFound, app_login_route("identity"))
				return true
			}
			respond_error(c, http.StatusForbidden, "identity_required", "errors.identity_required", nil)
			return true
		}
	}

	// App token authorization: enforce that Bearer JWT matches the target app.
	// Skip for static file serving (HTML, JS, CSS) — these don't expose user data.
	if user != nil && api_token == nil && !aa.Public && !shell_static {
		if !has_bearer {
			debug("403 app token required: app=%s action=%s method=%s has_bearer=%v", a.id, name, c.Request.Method, has_bearer)
			respond_error(c, http.StatusForbidden, "app_token_required", "errors.app_token_required", nil)
			return true
		}
		// Exact match, not "match unless empty": auth_create_app_token is the only
		// issuer and always stamps an app, so an empty claim is not a real token.
		if jwt_app != a.id {
			debug("403 app token mismatch: jwt_app=%s a.id=%s action=%s method=%s", jwt_app, a.id, name, c.Request.Method)
			respond_error(c, http.StatusForbidden, "app_token_mismatch", "errors.app_token_mismatch", nil)
			return true
		}
	}

	// Check app-level requirements (skip for static files in shell mode)
	if !shell_static && !av.user_allowed(user) {
		debug("403 access denied: app=%s action=%s user=%v", a.id, name, user != nil)
		respond_error(c, http.StatusForbidden, "access_denied", "errors.access_denied", nil)
		return true
	}

	// Handle git Smart HTTP protocol
	if aa.Feature == "git" {
		repo := aa.parameters["repository"]
		if repo == "" {
			respond_text(c, http.StatusBadRequest, "errors.repository_required", nil)
			return true
		}
		// Strip .git suffix if present (e.g., "my-project.git" -> "my-project")
		repo = strings.TrimSuffix(repo, ".git")
		return git_http_handler(c, a, owner, user, repo, aa.parameters["path"])
	}

	if web_serves_file(c, aa) {
		file := av.base + "/" + aa.File
		if strings.HasSuffix(aa.File, ".html") {
			web_serve_html(c, a, av, aa, e, file)
			return true
		}
		web_cache_static(c, file, aa.Cache)
		if strings.HasSuffix(strings.ToLower(aa.File), ".svg") {
			web_serve_svg_path(c, file)
			return true
		}
		c.File(file)
		return true
	}

	// Serve static files from a directory
	if aa.Files != "" {
		if aa.filepath != "" {
			if !valid(aa.filepath, "filepath") {
				respond_error(c, http.StatusBadRequest, "invalid_file", "errors.invalid_file", nil)
				return true
			}
			file := av.base + "/" + aa.Files + "/" + aa.filepath
			//debug("Serving file from directory for app %q: %q", a.id, file)
			web_cache_static(c, file, aa.Cache)
			if strings.HasSuffix(strings.ToLower(aa.filepath), ".svg") {
				web_serve_svg_path(c, file)
			} else {
				c.File(file)
			}
		} else {
			respond_error(c, http.StatusBadRequest, "no_file_specified", "errors.no_file_specified", nil)
		}
		return true
	}

	// Require authentication for database-backed apps (unless action is public)
	if av.Database.File != "" && user == nil && owner == nil && !aa.Public {
		respond_error(c, http.StatusUnauthorized, "authentication_required_for_database_access", "errors.auth_required_db", nil)
		return true
	}

	// Set up database connections if needed
	if av.Database.File != "" {
		if user != nil {
			user.db = db_app(user, a)
			if user.db == nil {
				respond_error(c, http.StatusInternalServerError, "database_error", "errors.database_error", nil)
				return true
			}
			defer user.db.close()
		}

		if owner != nil && (user == nil || owner.UID != user.UID) {
			owner.db = db_app(owner, a)
			if owner.db == nil {
				respond_error(c, http.StatusInternalServerError, "database_error", "errors.database_error", nil)
				return true
			}
			defer owner.db.close()
		}
	}

	// Carry the route only when one matched, so a.domain.route is None for a
	// request that arrived by the app's own path. The route's owner is for the
	// file-serving path only - apps read the action's `owner` as "the requester
	// owns this data".
	domain := &DomainInfo{}
	if _, routed := c.Get("domain_route"); routed {
		domain.route = &DomainRouteInfo{
			context:   c.GetString("domain_context"),
			remainder: name,
		}
		if uid, ok := c.Get("domain_owner"); ok {
			if id, ok := uid.(string); ok && id != "" {
				domain.route.owner = user_by_uid(id)
			}
		}
	}

	// Create action
	action := Action{
		id:      action_id(),
		user:    user,
		owner:   owner,
		domain:  domain,
		app:     a,
		active:  av,
		token:   api_token,
		web:     c,
		inputs:  make(map[string]string),
		entity:  e,
		routing: routing,

		definition: aa,
	}

	for k, v := range aa.parameters {
		action.inputs[k] = v
	}

	// Add entity to inputs when present (for entity-aware routing)
	if e != nil && e.Class != "" {
		action.inputs[e.Class] = e.ID
	}

	// Capture the raw body so a.body can return it for signature verification
	// (Stripe webhooks), then restore Request.Body - io.ReadAll leaves it at EOF
	// and breaks multipart and form parsing downstream.
	content_type := c.Request.Header.Get("Content-Type")
	if c.Request.Body != nil && (strings.HasPrefix(content_type, "application/json") || strings.HasPrefix(content_type, "text/")) {
		raw, err := io.ReadAll(c.Request.Body)
		if err == nil {
			action.body = string(raw)
			c.Request.Body.Close()
			c.Request.Body = io.NopCloser(bytes.NewReader(raw))
			if strings.HasPrefix(content_type, "application/json") {
				var data map[string]any
				if jerr := json.Unmarshal(raw, &data); jerr == nil {
					for key, value := range data {
						action.inputs[key] = any_to_string(value)
					}
				}
			}
		}
	}

	// A body key named after the routed entity's class would repoint the app at a
	// different entity, and token_allows compared the token against the ROUTED
	// one. Restore the route's value so an entity-bound token cannot be aimed
	// elsewhere.
	if api_token != nil && api_token.Entity != "" && e != nil && e.Class != "" {
		action.inputs[e.Class] = e.ID
	}

	// Read the entire multipart body BEFORE running the action: parsing lazily
	// inside a.file() charges the client's transfer time against the 90s Starlark
	// timeout. A parse error is left for the handler's own a.file()/form call to
	// surface.
	if strings.HasPrefix(content_type, "multipart/form-data") {
		// Bound the body before parsing it. Content-Length is a hint a client
		// controls, so it only saves us reading a body we already know is too
		// big; MaxBytesReader is what actually enforces the ceiling.
		maximum := web_multipart_maximum(user)
		if c.Request.ContentLength > maximum {
			respond_error(c, http.StatusRequestEntityTooLarge, "body_too_large", "errors.body_too_large", nil)
			return true
		}
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maximum)
		if err := c.Request.ParseMultipartForm(32 << 20); err != nil {
			// Only the size failure is answered here. Every other parse error
			// is still left for the handler's own a.file()/form call to
			// surface, preserving existing behaviour.
			var exceeded *http.MaxBytesError
			if errors.As(err, &exceeded) {
				respond_error(c, http.StatusRequestEntityTooLarge, "body_too_large", "errors.body_too_large", nil)
				return true
			}
			debug("Multipart parse: %v", err)
		}
	}

	// Check which engine the app uses, and run it
	switch av.Architecture.Engine {
	case "": // Internal app
		if aa.internal_function == nil {
			respond_error(c, http.StatusInternalServerError, "action_has_no_function", "errors.action_has_no_function", nil)
			return true
		}

		aa.internal_function(&action)
		c.JSON(http.StatusOK, nil)

	case "starlark":
		if aa.Function == "" {
			respond_error(c, http.StatusInternalServerError, "action_has_no_function", "errors.action_has_no_function", nil)
			return true
		}

		// Call Starlark function
		s := av.starlark()
		s.set("action", &action)
		s.set("app", a)
		s.set("host", c.Request.Host)
		s.set("origin", request_origin(c))
		// A `public: true` action invoked anonymously has no Mochi-authenticated
		// caller, and is bound as exactly that: nothing. principal_storage answers
		// which account's data is read, so binding the owner adds only a false claim.
		s.set("user", user)
		s.set("owner", owner)
		// The caller's own session, so mochi.user.session.list can mark the current
		// row. Empty for anonymous and app-token callers, which have no browser
		// session.
		s.set("session", web_cookie_get(c, "session", ""))
		s.set("language", request_language(c, user))
		if e != nil {
			s.set("route_entity", e.ID)
		}

		result, err := s.call(aa.Function, sl.Tuple{&action})
		if err != nil {
			// If the response has already been written (e.g. file serving),
			// we can't send an error response
			if c.Writer.Written() {
				return true
			}
			web_action_error(c, a.id, err)
			return true
		}
		if !c.Writer.Written() {
			if result != sl.None {
				c.JSON(http.StatusOK, sl_decode(result))
			} else if !starlark_serving_get(s.thread) {
				// NoRoute pre-sets 404 - override when a fire-and-forget action wrote no
				// response. NOT when a file was served: ServeContent may have set 304 with
				// no body, and forcing 200 breaks conditional GETs such as apt's InRelease.
				c.Status(http.StatusOK)
			}
		}

	default:
		info("Action unknown engine %q version %q", av.Architecture.Engine, av.Architecture.Version)
	}

	return true
}

// Get user for session cookie and refresh it to extend expiry
func web_auth(c *gin.Context) *User {
	session := web_cookie_get(c, "session", "")
	user := user_by_login(session)
	if user != nil {
		// Refresh cookie to reset browser expiry limits
		web_cookie_set(c, "session", session)
	}
	return user
}

// web_serve_svg sanitizes SVG content and serves it. svg_sanitize is
// best-effort and bypassable, so the Content-Security-Policy is the real
// guarantee. Content arrives as an open handle so the caller's own check still
// holds; a document too large to buffer is served as a download, which cannot
// execute.
func web_serve_svg(c *gin.Context, reader io.Reader) {
	buffer, err := io.ReadAll(io.LimitReader(reader, stream_svg_maximum+1))
	if err != nil {
		c.Status(http.StatusNotFound)
		return
	}

	if len(buffer) > stream_svg_maximum {
		c.Header("Content-Disposition", "attachment")
		c.Data(http.StatusOK, "image/svg+xml", buffer)
		io.Copy(c.Writer, reader)
		return
	}

	c.Header("Content-Security-Policy", svg_content_policy)
	c.Data(http.StatusOK, "image/svg+xml", svg_sanitize(buffer))
}

// web_serve_svg_path opens a path and serves it through web_serve_svg. It suits
// an app's own bundled files, where the path was built by the server and no
// caller-held handle has to be preserved.
func web_serve_svg_path(c *gin.Context, path string) {
	file, err := os.Open(path)
	if err != nil {
		c.Status(http.StatusNotFound)
		return
	}
	defer file.Close()
	web_serve_svg(c, file)
}

func web_cache_static(c *gin.Context, path string, cache string) {
	if !web_cache {
		c.Header("Cache-Control", "no-cache, no-store, must-revalidate")
		return
	}
	// Use explicit cache policy if set in app.json action
	if cache != "" {
		switch cache {
		case "immutable":
			c.Header("Cache-Control", "public, max-age=31536000, immutable")
		case "static":
			c.Header("Cache-Control", "public, max-age=300")
		case "revalidate":
			c.Header("Cache-Control", "no-cache, must-revalidate")
			if information, err := os.Stat(path); err == nil {
				etag := fmt.Sprintf(`"%x"`, information.ModTime().UnixNano())
				c.Header("ETag", etag)
				if match := c.GetHeader("If-None-Match"); match == etag {
					c.AbortWithStatus(http.StatusNotModified)
				}
			}
		case "none":
			c.Header("Cache-Control", "no-cache, no-store, must-revalidate")
		}
		return
	}
	// Auto-detect cache policy from file path
	if strings.HasSuffix(path, ".html") {
		// HTML files should revalidate on every request
		// Add ETag based on file modification time for proper cache validation
		c.Header("Cache-Control", "no-cache, must-revalidate")
		if information, err := os.Stat(path); err == nil {
			etag := fmt.Sprintf(`"%x"`, information.ModTime().UnixNano())
			c.Header("ETag", etag)
			// Check If-None-Match header for conditional request
			if match := c.GetHeader("If-None-Match"); match == etag {
				c.AbortWithStatus(http.StatusNotModified)
				return
			}
		}
	} else if match_react.MatchString(path) {
		// debug("Web asking browser to long term cache %q", path)
		c.Header("Cache-Control", "public, max-age=31536000, immutable")
	} else {
		// debug("Web asking browser to short term cache %q", path)
		c.Header("Cache-Control", "public, max-age=300")
	}
}

// Get the value of a cookie
func web_cookie_get(c *gin.Context, name string, def string) string {
	value, err := c.Cookie(name)
	if err != nil {
		return def
	}
	return value
}

// Set a cookie
func web_cookie_set(c *gin.Context, name string, value string) {
	secure := web_https && !web_is_localhost(c)
	c.SetSameSite(http.SameSiteLaxMode)
	c.SetCookie(name, value, 365*86400, "/", "", secure, true)
}

// Check if request is from localhost. Reads the socket peer rather than
// ClientIP, so no header can reach it even if a trusted-proxy list is ever
// configured: this decides cookie Secure and whether an OAuth callback is
// advertised as https.
func web_is_localhost(c *gin.Context) bool {
	ip := c.RemoteIP()
	return ip == "127.0.0.1" || ip == "::1" || ip == "localhost"
}

// Unset a cookie
func web_cookie_unset(c *gin.Context, name string) {
	secure := web_https && !web_is_localhost(c)
	c.SetSameSite(http.SameSiteLaxMode)
	c.SetCookie(name, "", -1, "/", "", secure, true)
}

// Security headers middleware
func web_security_headers(c *gin.Context) {
	c.Header("X-Content-Type-Options", "nosniff")
	// Apps must be frameable by the shell (SAMEORIGIN).
	// The shell page itself sets DENY in web_serve_shell().
	c.Header("X-Frame-Options", "SAMEORIGIN")
	// Sandboxed iframes without allow-same-origin have an opaque (null) origin,
	// so all requests are cross-origin. ES module scripts and crossorigin CSS
	// require CORS headers. Static assets are public, so this is safe.
	c.Header("Access-Control-Allow-Origin", "*")
	// Allow JS in sandboxed iframes (null origin) to read Content-Disposition so
	// downloads can use the server-supplied filename instead of falling back to
	// the URL or a hardcoded default.
	c.Header("Access-Control-Expose-Headers", "Content-Disposition")
	// Handle CORS preflight requests from sandboxed iframes.
	// When the iframe sends requests with Authorization header,
	// browsers send an OPTIONS preflight first.
	if c.Request.Method == "OPTIONS" {
		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, PATCH, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Authorization, Content-Type, Accept")
		c.Header("Access-Control-Max-Age", "86400")
		c.AbortWithStatus(204)
		return
	}
	c.Header("Referrer-Policy", "strict-origin-when-cross-origin")
	c.Next()
}

// The ceiling on a request body that carries no file upload. Multipart and git
// pack bodies are exempt here because uploads legitimately exceed it; they are
// bounded separately, by web_multipart_maximum and the git RPC respectively.
const web_body_maximum = 1 << 20 // 1MB

// Allowance for multipart framing — boundaries and per-part headers — on top of
// the payload a caller is entitled to send. Roughly 200 bytes per part, so this
// covers a form with several hundred of them.
const web_multipart_framing = 64 << 10 // 64KB

// Request body size limit middleware (skip multipart/form-data for file uploads
// and git pack data for push operations)
func web_body_limit(c *gin.Context) {
	ct := c.GetHeader("Content-Type")
	if !strings.HasPrefix(ct, "multipart/form-data") && !strings.HasPrefix(ct, "application/x-git-") {
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, web_body_maximum)
	}
	c.Next()
}

// web_multipart_maximum bounds a multipart body, derived from the caller's
// storage quota: multipart is exempt from web_body_limit and ParseMultipartForm
// spools the whole body to os.TempDir(), usually a tmpfs. Gated on the
// authenticated user, never the owner - an anonymous request to a public action
// runs as the owner.
func web_multipart_maximum(user *User) int64 {
	// An anonymous caller has no storage quota and nothing holding them
	// accountable, and so gets no more room for a multipart body than for any
	// other body.
	if user == nil {
		return web_body_maximum + web_multipart_framing
	}
	remaining, err := user_storage_remaining(user)
	// Unmeasurable or already at quota: allow a minimal body so the handler
	// can answer with its own quota error rather than the caller seeing a
	// truncated upload.
	if err != nil || remaining <= 0 {
		return web_body_maximum + web_multipart_framing
	}
	// Administrators are quota-exempt (MaxInt64). Give them a finite ceiling
	// anyway — unbounded is the thing being fixed — sized at the per-user
	// quota, which is far beyond any real single upload.
	if remaining > file_maximum_storage {
		remaining = file_maximum_storage
	}
	return remaining + web_multipart_framing
}

// web_serves_file reports whether an action declaring a file will answer this
// request with the file rather than with its function. Both the shell_static
// bypass and the branch that writes the file ask it, so they cannot disagree.
func web_serves_file(c *gin.Context, aa *AppAction) bool {
	if aa.File == "" {
		return false
	}
	// file+function+opengraph negotiates: HTML to a browser or crawler, the
	// function to an API caller.
	if aa.Function == "" || aa.OpenGraph == "" {
		return true
	}
	accept := c.GetHeader("Accept")
	return strings.Contains(accept, "text/html") && !strings.Contains(accept, "application/json")
}

// opengraph_absolute makes an app's og:image absolute: a crawler fetching from
// outside has nothing to resolve a relative reference against and drops it. The
// path is treated as a directory even without a trailing slash, which is what
// entity routes such as /people/<fingerprint> need.
func opengraph_absolute(image, scheme, host, path string) string {
	if strings.Contains(image, "://") || strings.HasPrefix(image, "//") {
		return image
	}
	if strings.HasPrefix(image, "/") {
		return scheme + "://" + host + image
	}
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}
	return scheme + "://" + host + path + image
}

func web_serve_file_with_opengraph(c *gin.Context, a *App, av *AppVersion, aa *AppAction, e *Entity, file string) bool {
	// Get owner for database access - use entity owner if available
	var owner *User
	if e != nil {
		owner = user_owning_entity(e.ID)
	}
	user := web_auth(c)

	// Set up database connection if needed
	if av.Database.File != "" {
		if owner == nil {
			return false
		}
		owner.db = db_app(owner, a)
		if owner.db != nil {
			defer owner.db.close()
		}
	}

	// Call Starlark function to get OG data
	s := av.starlark()
	s.set("app", a)
	// The caller is whoever actually asked, nobody included; `storage` keeps the
	// reads on the owner's databases. Binding user to the owner would hand an
	// authenticated stranger the owner's identity.
	s.set("user", user)
	s.set("owner", owner)
	s.set("storage", owner)
	// OpenGraph rendering is primarily for external viewers (crawlers, link
	// previews). Use Accept-Language to drive the meta-tag language; a
	// logged-in viewer's specific preference can be wired in later if needed.
	s.set("language", request_language(c, nil))

	// Build parameters dict for the function
	params := sl.NewDict(len(aa.parameters))
	for k, v := range aa.parameters {
		params.SetKey(sl.String(k), sl.String(v))
	}

	// Add entity info if present
	if e != nil {
		params.SetKey(sl.String("entity"), sl.String(e.ID))
		params.SetKey(sl.String("fingerprint"), sl.String(e.Fingerprint))
	}

	// Build request URL for og:url
	scheme := "https"
	if !web_https {
		scheme = "http"
	}
	url := scheme + "://" + c.Request.Host + c.Request.URL.Path

	result, err := s.call(aa.OpenGraph, sl.Tuple{params})
	if err != nil {
		debug("OpenGraph function %q error: %v", aa.OpenGraph, err)
		return false
	}

	// Convert result to map
	og := sl_decode_map(result)
	if og == nil {
		debug("OpenGraph function %q returned invalid data", aa.OpenGraph)
		return false
	}

	// Read HTML file
	html, read_error := os.ReadFile(file)
	if read_error != nil {
		return false
	}
	content := string(html)

	// Replace OG meta tags
	if title, ok := og["title"].(string); ok && title != "" {
		content = regexp_replace_meta(content, "og:title", title)
		content = regexp_replace_meta(content, "twitter:title", title)
		content = regexp_replace_tag(content, "title", title)
		content = regexp_replace_meta_name(content, "title", title)
	}
	if desc, ok := og["description"].(string); ok && desc != "" {
		content = regexp_replace_meta(content, "og:description", desc)
		content = regexp_replace_meta(content, "twitter:description", desc)
		content = regexp_replace_meta_name(content, "description", desc)
	}
	if image, ok := og["image"].(string); ok && image != "" {
		image = opengraph_absolute(image, scheme, c.Request.Host, c.Request.URL.Path)
		escaped := escape_attribute(image)
		// Add og:image if not already present
		if !strings.Contains(content, `property="og:image"`) {
			content = strings.Replace(content, `<meta property="og:description"`,
				`<meta property="og:image" content="`+escaped+`" />`+"\n    "+`<meta property="og:description"`, 1)
		} else {
			content = regexp_replace_meta(content, "og:image", image)
		}
		// Add twitter:image if not already present
		if !strings.Contains(content, `property="twitter:image"`) {
			content = strings.Replace(content, `<meta property="twitter:description"`,
				`<meta property="twitter:image" content="`+escaped+`" />`+"\n    "+`<meta property="twitter:description"`, 1)
		} else {
			content = regexp_replace_meta(content, "twitter:image", image)
		}
	}
	if og_type, ok := og["type"].(string); ok && og_type != "" {
		content = regexp_replace_meta(content, "og:type", og_type)
	}

	// Always set og:url to current URL
	if !strings.Contains(content, `property="og:url"`) {
		escaped := escape_attribute(url)
		content = strings.Replace(content, `<meta property="og:type"`,
			`<meta property="og:url" content="`+escaped+`" />`+"\n    "+`<meta property="og:type"`, 1)
	} else {
		content = regexp_replace_meta(content, "og:url", url)
	}

	// Inject routing meta tags
	content = web_inject_meta_tags(c, e, content)

	// Serve modified content
	c.Header("Content-Type", "text/html; charset=utf-8")
	web_cache_static(c, file, aa.Cache)
	c.String(http.StatusOK, content)
	return true
}

// Check if a URL segment looks like an entity identifier (fingerprint or full ID)
var entity_segment_re = regexp.MustCompile(`^[1-9A-HJ-NP-Za-km-z]{9}$|^[1-9A-HJ-NP-Za-km-z]{50,51}$`)

func is_entity_segment(s string) bool {
	return entity_segment_re.MatchString(s)
}

// Build routing meta tags and base href, inject them after <head>
func web_inject_meta_tags(c *gin.Context, e *Entity, content string) string {
	var tags []string

	app := c.GetString("mochi_app_path")
	dm := c.GetString("domain_method")

	// For domain routing, use the matched route path so SPAs on subpath
	// routes (e.g. acunningham.org/feed → feed entity) resolve assets and
	// client-side routes under that subpath rather than the domain root.
	route_path := "/"
	if dm == "entity" || dm == "app" {
		if r, ok := c.Get("domain_route"); ok {
			if rt, ok := r.(*route); ok && rt.Path != "" {
				route_path = rt.Path
				if !strings.HasSuffix(route_path, "/") {
					route_path += "/"
				}
			}
		}
	}

	// Compute base href for correct relative URL resolution on deep routes.
	// Must be a static tag (not JS-generated) so the browser's preload scanner
	// resolves ./assets/... correctly before any scripts execute.
	if dm == "entity" || dm == "app" {
		tags = append(tags, `<base href="`+escape_attribute(route_path)+`">`)
	} else if e != nil && app != "" {
		tags = append(tags, `<base href="/`+escape_attribute(app)+`/`+escape_attribute(e.Fingerprint)+`/">`)
	} else if e != nil {
		tags = append(tags, `<base href="/`+escape_attribute(e.Fingerprint)+`/">`)
	} else if app != "" {
		tags = append(tags, `<base href="/`+escape_attribute(app)+`/">`)
	}

	if app != "" {
		tags = append(tags, `<meta name="mochi:app" content="`+escape_attribute(app)+`">`)
	}
	if e != nil {
		tags = append(tags, `<meta name="mochi:class" content="`+escape_attribute(e.Class)+`">`)
		tags = append(tags, `<meta name="mochi:entity" content="`+escape_attribute(e.ID)+`">`)
		tags = append(tags, `<meta name="mochi:fingerprint" content="`+escape_attribute(e.Fingerprint)+`">`)
	} else if seg := c.GetString("mochi_entity_segment"); seg != "" {
		tags = append(tags, `<meta name="mochi:fingerprint" content="`+escape_attribute(seg)+`">`)
	}
	if dm == "entity" || dm == "app" {
		// content carries the matched route path so SPAs can derive their basepath
		tags = append(tags, `<meta name="mochi:domain" content="`+escape_attribute(route_path)+`">`)
	}
	if len(tags) > 0 {
		injection := "\n    " + strings.Join(tags, "\n    ")
		content = strings.Replace(content, "<head>", "<head>"+injection, 1)
	}
	return content
}

// Serve an HTML file with routing meta tags injected after <head>
func web_serve_html(c *gin.Context, a *App, av *AppVersion, aa *AppAction, e *Entity, file string) {
	// When shell is active and this is an iframe request, serve static HTML
	// with no injection — token and context are delivered via postMessage.
	// We patch the inline theme script to handle the sandboxed iframe's
	// inability to access document.cookie (opaque origin).
	if web_is_iframe_request(c) {
		html, err := os.ReadFile(file)
		if err != nil {
			respond_text(c, http.StatusNotFound, "errors.file_not_found", nil)
			return
		}
		content := string(html)
		// Inject base href so the browser's preload scanner resolves relative
		// asset paths (./assets/...) correctly even on deep URL paths.
		if app := c.GetString("mochi_app_path"); app != "" {
			content = strings.Replace(content, "<head>", `<head><base href="/`+escape_attribute(app)+`/">`, 1)
		}
		user := web_auth(c)
		content = web_apply_user_document_theme(content, user)
		// Inject the iframe shim before everything else. Loaded from the
		// menu app's dist (apps/menu/web/public/iframe-shim.js) so it
		// hot-reloads on rebuild. Empty string means menu unavailable —
		// skip injection rather than break iframe serving.
		if shim := shell_iframe_shim_load(user); shim != "" {
			content = strings.Replace(content, "<head>", "<head><script>"+shim+"</script>", 1)
		}
		c.Header("Content-Type", "text/html; charset=utf-8")
		// User-specific theme is baked in — prevent shared caches from
		// serving one user's appearance to another. Short private
		// maximum-age covers rapid app switching without a server round-trip.
		if web_cache {
			c.Header("Cache-Control", "private, max-age=60")
		} else {
			c.Header("Cache-Control", "no-cache, no-store, must-revalidate")
		}
		c.Header("Vary", "Cookie")
		c.String(http.StatusOK, content)
		return
	}

	// Try OG injection first (it also injects routing meta tags)
	if aa.OpenGraph != "" {
		if web_serve_file_with_opengraph(c, a, av, aa, e, file) {
			return
		}
	}

	// Read HTML file
	html, err := os.ReadFile(file)
	if err != nil {
		respond_text(c, http.StatusNotFound, "errors.file_not_found", nil)
		return
	}
	content := web_inject_meta_tags(c, e, string(html))
	content = web_apply_user_document_theme(content, web_auth(c))

	c.Header("Content-Type", "text/html; charset=utf-8")
	// User-specific theme is baked in — prevent shared caches from
	// serving one user's appearance to another. Short private
	// maximum-age covers rapid app switching without a server round-trip.
	if web_cache {
		c.Header("Cache-Control", "private, max-age=60")
	} else {
		c.Header("Cache-Control", "no-cache, no-store, must-revalidate")
	}
	c.Header("Vary", "Cookie")
	c.String(http.StatusOK, content)
}

// Replace Open Graph meta tag content
func escape_attribute(value string) string {
	value = strings.ReplaceAll(value, `&`, `&amp;`)
	value = strings.ReplaceAll(value, `"`, `&quot;`)
	value = strings.ReplaceAll(value, `<`, `&lt;`)
	value = strings.ReplaceAll(value, `>`, `&gt;`)
	return value
}

func regexp_replace_meta(html, property, value string) string {
	value = escape_attribute(value)

	pattern := regexp.MustCompile(`<meta\s+property="` + regexp.QuoteMeta(property) + `"\s+content="[^"]*"\s*/?>`)
	replacement := `<meta property="` + property + `" content="` + value + `" />`
	// Literal, not ReplaceAllString: $ in the replacement is a capture-group
	// reference and the value is app-supplied, so "Cost: $100" rendered as "Cost:
	// ".
	return pattern.ReplaceAllLiteralString(html, replacement)
}

// Replace meta tag with name attribute
func regexp_replace_meta_name(html, name, value string) string {
	value = escape_attribute(value)

	pattern := regexp.MustCompile(`<meta\s+name="` + regexp.QuoteMeta(name) + `"\s+content="[^"]*"\s*/?>`)
	replacement := `<meta name="` + name + `" content="` + value + `" />`
	return pattern.ReplaceAllLiteralString(html, replacement)
}

// Replace HTML tag content
func regexp_replace_tag(html, tag, value string) string {
	value = escape_attribute(value)

	pattern := regexp.MustCompile(`<` + regexp.QuoteMeta(tag) + `>[^<]*</` + regexp.QuoteMeta(tag) + `>`)
	replacement := `<` + tag + `>` + value + `</` + tag + `>`
	return pattern.ReplaceAllLiteralString(html, replacement)
}

// Handle login begin: check user's required auth methods (POST with JSON)
// Returns the methods required for this user, without sending any codes.
func web_login_begin(c *gin.Context) {
	var input struct {
		Email string `json:"email"`
	}
	if err := c.ShouldBindJSON(&input); err != nil || input.Email == "" {
		respond_error(c, http.StatusBadRequest, "invalid_request", "errors.invalid_request", nil)
		return
	}

	if !email_valid(input.Email) {
		respond_error(c, http.StatusBadRequest, "invalid_email", "errors.invalid_email", nil)
		return
	}

	// Check if user exists
	user := user_by_username(input.Email)

	if user == nil {
		// User doesn't exist - check if signup is enabled
		if !setting_signup_enabled() {
			respond_error(c, http.StatusForbidden, "signup_disabled", "errors.signup_disabled", nil)
			return
		}
		// New user - default to email method
		c.JSON(http.StatusOK, gin.H{
			"methods": []string{"email"},
			"allowed": []string{"email"},
			"new":     true,
		})
		return
	}

	// allowed is the set the login screen offers after email entry; methods is the
	// effective required set the login AND-s, including the system email floor.
	// Force non-nil so it marshals to [] not null - the client calls .includes().
	methods := auth_remaining_methods(user, "")
	if methods == nil {
		methods = []string{}
	}
	allowed := user_login_offered(user)
	has_passkey := false
	for _, m := range allowed {
		if m == "passkey" {
			has_passkey = true
		}
	}
	oauth_required := false
	for _, m := range methods {
		if m == "oauth" {
			oauth_required = true
		}
	}

	// Offer OAuth at verification when it can verify this account: the provider is
	// usable, and OAuth is required or nothing is. The button carries the entered
	// email so the callback can confirm OAuth resolves to this account.
	offer_oauth := user_method_usable(user, "oauth") && (len(methods) == 0 || oauth_required)

	c.JSON(http.StatusOK, gin.H{
		"methods":     methods,
		"allowed":     allowed,
		"has_passkey": has_passkey,
		"oauth":       offer_oauth,
	})
}

// web_auth_partial returns the pending login partial - its remaining factors and
// id - so the /codes page can resume after a full-page navigation or refresh,
// where the client store holds no MFA state. Reads the login_partial cookie that
// every MFA-continuation flow (email/totp/passkey/oauth) sets; empty when none.
func web_auth_partial(c *gin.Context) {
	id, err := c.Cookie("login_partial")
	if err != nil || id == "" {
		c.JSON(http.StatusOK, gin.H{"partial": "", "remaining": []string{}})
		return
	}
	row, _ := db_open("db/sessions.db").row("select remaining from partial where id=? and expires>?", id, now())
	if row == nil {
		c.JSON(http.StatusOK, gin.H{"partial": "", "remaining": []string{}})
		return
	}
	remaining := []string{}
	for _, m := range strings.Split(row_string(row, "remaining"), ",") {
		if m = strings.TrimSpace(m); m != "" {
			remaining = append(remaining, m)
		}
	}
	c.JSON(http.StatusOK, gin.H{"partial": id, "remaining": remaining})
}

func web_identity_get(c *gin.Context) {
	user_by_id_allow_no_identity := func(id string) *User {
		db := db_open("db/users.db")
		var user User
		if !db.scan(&user, "select uid, username, role, methods, disabled, status from users where uid=?", id) {
			return nil
		}
		user.Preferences = user_preferences_load(&user)
		user.Identity = user.identity()
		return &user
	}

	u := web_auth(c)

	// If no cookie auth, try Bearer token authentication
	if u == nil {
		auth_header := c.GetHeader("Authorization")
		if strings.HasPrefix(auth_header, "Bearer ") {
			bearer := strings.TrimPrefix(auth_header, "Bearer ")
			if strings.HasPrefix(bearer, "mochi-") {
				// API token authentication, for an unbound token only - this
				// answers with the owner's email address. See token_unbound.
				api_token := token_validate(bearer)
				if token_unbound(api_token) {
					u = user_by_id_allow_no_identity(api_token.User)
				}
			} else {
				// JWT authentication
				if uid, _, err := jwt_verify(bearer); err == nil && uid != "" {
					if user := user_by_id_allow_no_identity(uid); user != nil {
						u = user
					}
				}
			}
		}
	}

	if u == nil {
		respond_error(c, http.StatusUnauthorized, "authentication_required", "errors.authentication_required", nil)
		return
	}

	response := gin.H{
		"user": gin.H{
			"email":  u.Username,
			"name":   "", // Will be populated below if identity exists
			"status": u.Status,
		},
	}

	// A closing account carries the purge timestamp so the reactivation
	// interstitial can show the deletion date.
	if u.Status == "closing" {
		response["user"].(gin.H)["purge"] = user_purge(u.UID)
	}

	if u.Identity != nil {
		response["user"].(gin.H)["name"] = u.Identity.Name
		response["identity"] = gin.H{
			"id":          u.Identity.ID,
			"name":        u.Identity.Name,
			"privacy":     u.Identity.Privacy,
			"fingerprint": u.Identity.Fingerprint,
		}
	}

	c.JSON(http.StatusOK, response)
}

// Handle login: request code via email (POST with JSON)
func web_login_code(c *gin.Context) {
	var input struct {
		Email string `json:"email"`
	}
	if err := c.ShouldBindJSON(&input); err != nil || input.Email == "" {
		respond_error(c, http.StatusBadRequest, "invalid_request", "errors.invalid_request", nil)
		return
	}

	reason := code_send(input.Email, c)
	if reason != "" {
		switch reason {
		case "signup_disabled":
			respond_error(c, http.StatusForbidden, "signup_disabled", "errors.signup_disabled", nil)
		case "too_many_codes":
			respond_error(c, http.StatusTooManyRequests, "too_many_login_attempts_please_try_again_later", "errors.too_many_logins", nil)
		default:
			respond_error(c, http.StatusBadRequest, "unable_to_send_login_email", "errors.unable_to_send_login_email", nil)
		}
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// Create an identity for a new user
func web_login_identity(c *gin.Context) {
	u := web_auth(c)

	// If no cookie auth, try Bearer token authentication
	if u == nil {
		auth_header := c.GetHeader("Authorization")
		if strings.HasPrefix(auth_header, "Bearer ") {
			token := strings.TrimPrefix(auth_header, "Bearer ")
			if uid, _, err := jwt_verify(token); err == nil && uid != "" {
				if user := user_by_uid(uid); user != nil {
					u = user
					debug("Identity creation: JWT token accepted for user %q", u.UID)
				}
			}
		}
	}

	if u == nil {
		respond_error(c, http.StatusUnauthorized, "authentication_required", "errors.authentication_required", nil)
		return
	}

	var input struct {
		Name     string `json:"name"`
		Privacy  string `json:"privacy"`
		Language string `json:"language"`
	}
	if err := c.ShouldBindJSON(&input); err != nil {
		respond_error(c, http.StatusBadRequest, "invalid_request", "errors.invalid_request", nil)
		return
	}

	_, err := entity_create(u, "person", input.Name, input.Privacy, "")
	if err != nil {
		info("Identity creation error for user %q: %v", u.UID, err)
		respond_error(c, http.StatusBadRequest, "unable_to_create_identity", "errors.unable_to_create_identity", nil)
		return
	}

	// Persist the language chosen before signup. Source order: an explicit
	// `language` field, else the `mochi_language` cookie. Only when the user has
	// no explicit preference yet, and never the "auto" sentinel.
	lang := strings.ToLower(strings.TrimSpace(input.Language))
	if lang == "" {
		if cookie, cerr := c.Cookie("mochi_language"); cerr == nil {
			lang = strings.ToLower(cookie)
		}
	}
	if lang != "" && lang != "auto" && valid(lang, "locale") {
		if pref := strings.ToLower(user_preference_get(u, "language", "")); pref == "" || pref == "auto" {
			user_preference_set(u, "language", lang)
		}
	}

	// Deduped per (admin_address, new_user_uid) so a repeated signup event doesn't
	// mail twice. [email] signup = false silences these without disabling the
	// admin address for error mail - set it on development instances.
	admin := ini_string("email", "admin", "")
	if admin != "" && ini_bool("email", "signup", true) {
		event_id := "new-user:" + u.UID
		admin_user := user_by_username(admin)
		if admin_user != nil {
			email_send_dedup(admin_user, event_id, admin, "Mochi new user", "User: "+u.Username+"\nName: "+input.Name)
		} else {
			email_send(admin, "Mochi new user", "User: "+u.Username+"\nName: "+input.Name)
		}
	}

	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// Abandon a half-finished signup: delete the user (only when no identity exists
// yet) and clear the session.
func web_abandon(c *gin.Context) {
	u := web_auth(c)
	if u == nil {
		respond_error(c, http.StatusUnauthorized, "authentication_required", "errors.authentication_required", nil)
		return
	}

	if u.identity() != nil {
		respond_error(c, http.StatusForbidden, "account_already_set_up", "errors.account_already_set_up", nil)
		return
	}

	// Delete the account before clearing the session cookie: the reverse leaves
	// the browser with no session but a live account whenever the delete fails.
	target, err := user_delete(u.UID)
	if err != nil {
		respond_error(c, http.StatusInternalServerError, "server_error", "errors.server_error", nil)
		return
	}
	audit_user_deleted(target, target)

	// user_delete already removed this user's session rows; clear the browser
	// cookie so the now-orphaned session id doesn't linger.
	web_cookie_unset(c, "session")

	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// Log the user out
func web_logout(c *gin.Context) {
	session := web_cookie_get(c, "session", "")
	if session != "" {
		// Get user before deleting session for audit log
		user := web_auth(c)
		login_delete(session)
		if user != nil {
			audit_logout(user.Username, rate_limit_client_ip(c))
		}
	}
	web_cookie_unset(c, "session")
	// Clear stale theme cookie (no longer used, but may exist from older versions)
	secure := web_https && !web_is_localhost(c)
	c.SetCookie("mochi-theme", "", -1, "/", "", secure, false)
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// Handle app paths
func web_path(c *gin.Context) {
	//debug("Web path %q", c.Request.URL.Path)

	// A "closing" account may load only the /login reactivation interstitial.
	// Redirect top-level navigations before the shell renders, or the shell loads
	// and its gated app fetches bounce it in a loop.
	if web_should_serve_shell(c) {
		raw := strings.Trim(c.Request.URL.Path, "/")
		if !app_login_owns(raw) {
			if u := web_auth(c); u != nil && u.Status == "closing" {
				c.Redirect(http.StatusFound, app_login_route("closing"))
				return
			}
		}
	}

	// Shell intercept: serve shell page for top-level document navigations
	if web_should_serve_shell(c) {
		// Determine the app ID from the path for the shell
		raw := strings.Trim(c.Request.URL.Path, "/")
		app_name := ""
		if raw != "" {
			segments := strings.Split(raw, "/")
			app_name = segments[0]
		}
		web_serve_shell(c, app_name)
		return
	}

	// Get user for path-based routing preferences
	user := web_auth(c)

	// During bootstrap, show setup page until Login and Home are installed
	if !apps_bootstrap_ready {
		c.Header("Refresh", "2")
		c.Data(http.StatusOK, "text/html", []byte(`<!DOCTYPE html>
<html>
<head><title>Setting up</title></head>
<body style="font-family: system-ui, sans-serif; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; background: #f5f5f5;">
<div style="text-align: center;">
<h1 style="font-weight: normal; color: #333;">Setting up</h1>
<p style="color: #666;">Installing system apps, please wait...</p>
</div>
</body>
</html>`))
		return
	}

	// Check for domain-based routing first (skip /_/ paths which are core endpoints)
	if method, exists := c.Get("domain_method"); exists && method.(string) != "" && !strings.HasPrefix(c.Request.URL.Path, "/_/") {
		target := c.GetString("domain_target")
		remaining := c.GetString("domain_remaining")
		action := strings.TrimPrefix(remaining, "/")

		switch method.(string) {
		case "app":
			a := app_by_any(user, target)
			if a == nil {
				respond_error(c, http.StatusNotFound, "app_not_found", "errors.app_not_found", nil)
				return
			}
			// Redirect to add trailing slash for correct relative path resolution
			if remaining == "" && !strings.HasSuffix(c.Request.URL.Path, "/") {
				c.Redirect(http.StatusMovedPermanently, c.Request.URL.Path+"/")
				return
			}
			web_action(c, a, action, nil, routing_hosted)
			return

		case "redirect":
			c.Redirect(http.StatusFound, target+remaining)
			return

		case "entity":
			e := entity_by_any(target)
			if e == nil {
				respond_error(c, http.StatusNotFound, "entity_not_found", "errors.entity_not_found", nil)
				return
			}
			// Use entity owner's preferences for class routing
			owner := user_owning_entity(e.ID)
			a := class_app_for(owner, e.Class)
			if a == nil {
				respond_error(c, http.StatusNotFound, "no_app_for_entity", "errors.no_app_for_entity", nil)
				return
			}
			// Redirect to add trailing slash for correct relative path resolution
			if remaining == "" && !strings.HasSuffix(c.Request.URL.Path, "/") {
				c.Redirect(http.StatusMovedPermanently, c.Request.URL.Path+"/")
				return
			}
			// Same reason as the direct-entity branch below: the app is known
			// here but absent from the URL, so publish it for the SPA.
			c.Set("mochi_app_path", a.url_path(owner))
			web_action(c, a, action, e, routing_domain)
			return

		default:
			respond_error(c, http.StatusInternalServerError, "unknown_route_method", "errors.unknown_route_method", nil)
			return
		}
	}

	raw := strings.Trim(c.Request.URL.Path, "/")

	// Check for app that handles root path
	if raw == "" {
		if user == nil && !web_is_iframe_request(c) {
			// Serve login app for unauthenticated top-level navigations
			if login_app := app_login(); login_app != nil {
				web_action(c, login_app, "", nil, routing_class)
				return
			}
		}
		if a := app_by_root(user); a != nil {
			web_action(c, a, "", nil, routing_class)
			return
		}
		respond_error(c, http.StatusNotFound, "no_root_app_configured", "errors.no_root_app", nil)
		return
	}

	segments := strings.Split(raw, "/")
	first := segments[0]

	// Check for app matching first segment (user preferences, then system defaults, then fallback)
	a := app_for_path(user, first)
	if a != nil {
		// 301 redirect the login path -> / for unauthenticated users (bookmarks, password managers)
		if first == app_login_path() && len(segments) == 1 && user == nil &&
			strings.Contains(c.GetHeader("Accept"), "text/html") {
			target := "/"
			if c.Request.URL.RawQuery != "" {
				target += "?" + c.Request.URL.RawQuery
			}
			c.Redirect(http.StatusMovedPermanently, target)
			return
		}

		// Set app path segment so HTML serving can inject meta tags
		c.Set("mochi_app_path", first)

		// Redirect /app to /app/ for correct relative path resolution
		if len(segments) == 1 && !strings.HasSuffix(c.Request.URL.Path, "/") {
			c.Redirect(http.StatusMovedPermanently, "/"+first+"/")
			return
		}

		second := ""
		if len(segments) > 1 {
			second = segments[1]
		}

		// Route on /<app>/<entity>[/<action...>]
		e := entity_by_any(second)
		if e != nil {
			// Construct action with entity fingerprint prefix, same as direct entity routing
			action := e.Fingerprint
			if len(segments) > 2 {
				action = e.Fingerprint + "/" + strings.Join(segments[2:], "/")
			}
			if web_action(c, a, action, e, routing_path) {
				return
			}
		} else if is_entity_segment(second) {
			// Remote entity not known locally — pass identifier for meta tag injection
			c.Set("mochi_entity_segment", second)
		}

		// Route on /<app>/<action...>
		class_action := strings.Join(segments[1:], "/")

		web_action(c, a, class_action, nil, routing_class)
		return
	}

	// Check for entity matching first segment
	e := entity_by_any(first)
	if e != nil {
		// Use entity owner's preferences for class routing
		owner := user_owning_entity(e.ID)
		a := class_app_for(owner, e.Class)
		if a == nil {
			respond_error(c, http.StatusNotFound, "no_app_for_entity_class", "errors.no_app_for_class", nil)
			return
		}

		// The app is resolved from the entity's class and is absent from the URL, so
		// publish it: without the mochi:app meta tag getAppPath() returns "" and the
		// app cannot build a class-level URL.
		c.Set("mochi_app_path", a.url_path(owner))

		action := e.Fingerprint
		if len(segments) > 1 {
			action = e.Fingerprint + "/" + strings.Join(segments[1:], "/")
		}

		web_action(c, a, action, e, routing_direct)
		return
	}

	// Unknown path - route to root app if available
	if a := app_by_root(user); a != nil {
		web_action(c, a, raw, nil, routing_class)
		return
	}
	respond_error(c, http.StatusNotFound, "not_found", "errors.not_found", nil)
}

// Return Net connection info for this server
func web_p2p_info(c *gin.Context) {
	// net_addresses is the one rendering of this list: it drops container and
	// undialable addresses, stamps the peer id once, and deduplicates.
	addresses := net_addresses()
	if addresses == nil {
		addresses = []string{}
	}
	c.JSON(http.StatusOK, gin.H{
		"peer":      net_id,
		"addresses": addresses,
	})
}

func web_ping(c *gin.Context) {
	c.String(http.StatusOK, "pong")
}

// Serve robots.txt
func web_robots(c *gin.Context) {
	c.String(http.StatusOK, "User-agent: *\nAllow: /\n\nSitemap: https://%s/sitemap.xml\n", c.Request.Host)
}

// Serve sitemap.xml
func web_sitemap(c *gin.Context) {
	c.Data(http.StatusOK, "application/xml; charset=utf-8", []byte(`<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>https://`+html.EscapeString(c.Request.Host)+`/</loc>
  </url>
</urlset>
`))
}

// Start the web server
func web_start() {
	listen := ini_string("web", "listen", "")
	ports := ini_ints_commas("web", "ports")
	if len(ports) == 0 {
		// Fallback to legacy single port config
		port := ini_int("web", "port", 80)
		if port == 0 {
			return
		}
		ports = []int{port}
	}

	if !ini_bool("web", "debug", false) {
		gin.SetMode(gin.ReleaseMode)
	}
	gin.DefaultWriter = log.Writer()
	r := gin.New()
	// Mochi terminates TLS itself and trusts no proxy. Gin's default trusts every
	// source, letting any caller set their apparent address through
	// X-Forwarded-For. If proxy support is added, this becomes a configured list
	// rather than a deletion.
	if err := r.SetTrustedProxies(nil); err != nil {
		// Cannot fail for a nil list, but a silent failure would leave Gin's
		// trust-everything default in place with nobody the wiser.
		warn("Web unable to refuse proxy headers, X-Forwarded-For may be honoured: %v", err)
	}
	// First, before logging or any other handling: bridge a root-path
	// libp2p WebSocket upgrade to the loopback libp2p listener. No-op
	// unless the 443 fallback is enabled and the request is exactly that.
	r.Use(fallback_middleware)
	r.Use(gin.LoggerWithFormatter(func(param gin.LogFormatterParams) string {
		status := fmt.Sprintf("%d", param.StatusCode)
		if log_color && param.StatusCode >= 400 {
			status = "\033[31m" + status + "\033[0m"
		}
		return fmt.Sprintf("Web %s %s %s %q %v\n",
			status,
			param.ClientIP,
			param.Method,
			web_log_redact(param.Path),
			param.Latency,
		)
	}))
	r.Use(gin.Recovery())
	r.Use(web_security_headers)
	// After the security headers, so the guard's CSP is set on a response that
	// already carries the standard ones, and before anything that can produce a
	// body on a resource-exempt path.
	r.Use(web_resource_guard)
	r.Use(web_body_limit)
	r.Use(rate_limit_api_middleware)
	if web_compress != "none" {
		r.Use(web_compress_middleware)
	}
	r.Use(domains_middleware())
	r.RedirectTrailingSlash = false

	// Auth endpoints (grouped under /_/auth/)
	r.POST("/_/auth/begin", rate_limit_login_middleware, web_login_begin)
	r.POST("/_/auth/code", rate_limit_login_middleware, web_login_code)
	r.POST("/_/auth/verify", rate_limit_login_middleware, web_login_verify)
	r.POST("/_/auth/totp", rate_limit_login_middleware, web_auth_totp)
	r.POST("/_/auth/methods", rate_limit_login_middleware, web_auth_mfa)
	r.POST("/_/auth/passkey/begin", rate_limit_login_middleware, web_passkey_login_begin)
	r.POST("/_/auth/passkey/finish", rate_limit_login_middleware, web_passkey_login_finish)
	r.POST("/_/auth/recovery", rate_limit_login_middleware, web_recovery_login)
	r.POST("/_/auth/restore", rate_limit_login_middleware, web_auth_restore)
	r.GET("/_/auth/restore/progress", web_auth_restore_progress)
	r.POST("/_/auth/oauth/:provider/begin", rate_limit_login_middleware, web_oauth_begin)
	r.GET("/_/auth/oauth/:provider/callback", rate_limit_login_middleware, web_oauth_callback)
	r.POST("/_/auth/oauth/exchange", rate_limit_login_middleware, web_oauth_exchange)
	r.GET("/_/auth/methods", web_auth_methods)
	r.GET("/_/auth/partial", web_auth_partial)
	r.POST("/_/auth/close/cancel", web_auth_close_cancel)

	// Other system endpoints
	r.GET("/_/identity", web_identity_get)
	r.POST("/_/identity", web_login_identity)
	r.POST("/_/logout", web_logout)
	r.POST("/_/abandon", web_abandon)
	r.GET("/_/ping", web_ping)
	r.GET("/_/health", web_health)
	r.GET("/_/p2p/info", web_p2p_info)
	r.GET("/sw.js", webpush_service_worker)
	r.GET("/robots.txt", web_robots)
	r.GET("/sitemap.xml", web_sitemap)
	r.GET("/_/websocket", websocket_connection)
	r.POST("/_/token", web_shell_token)
	r.POST("/_/shell", web_shell_init)
	r.GET("/_/languages", web_languages)

	// All other paths are handled by web_path()
	r.NoRoute(web_path)

	// Check if HTTPS should be enabled (port 443 with domains configured)
	domains := domain_list()
	https := false
	for _, port := range ports {
		if port == 443 && len(domains) > 0 {
			https = true
			break
		}
	}

	// Start listeners for each port
	for i, port := range ports {
		last := i == len(ports)-1

		if port == 443 {
			if len(domains) == 0 {
				warn("Port 443 configured but no domains in database, skipping HTTPS")
				continue
			}
			web_https = true
			info("Web listening on %s:443 (HTTPS)", listen)
			if last {
				must(web_serve_tls(r))
			} else {
				go must(web_serve_tls(r))
			}
		} else {
			addr := fmt.Sprintf("%s:%d", listen, port)
			if https {
				info("Web listening on %s (HTTP, ACME challenges)", addr)
			} else {
				info("Web listening on %s (HTTP)", addr)
			}
			s := web_server(addr, r)
			listener, err := web_listen(addr, web_connections_maximum())
			if err != nil {
				warn("Unable to listen on %s: %v", addr, err)
				continue
			}
			if last {
				must(s.Serve(listener))
			} else {
				go must(s.Serve(listener))
			}
		}
	}
}

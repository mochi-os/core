// Mochi server: Domain routing and management
// Copyright Alistair Cunningham 2025

package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"

	"github.com/gin-gonic/gin"
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
	"golang.org/x/crypto/acme/autocert"
	"gopkg.in/ini.v1"
)

const acme_directory = "https://acme-v02.api.letsencrypt.org/directory"

var (
	domains_certs        map[string]*tls.Certificate
	domains_acme_manager *autocert.Manager
)

// domain represents a row in the domains table
type domain struct {
	Domain    string `db:"domain"`
	Type      string `db:"type"`
	Owner     int    `db:"owner"`
	Delegator string `db:"delegator"`
	Scope     string `db:"scope"`
	Prefix    string `db:"prefix"`
	Verified  int    `db:"verified"`
	Token     string `db:"token"`
	TLS       int    `db:"tls"`
	Created   int64  `db:"created"`
	Updated   int64  `db:"updated"`
}

// route represents a row in the routes table
type route struct {
	Domain   string `db:"domain"`
	Path     string `db:"path"`
	Entity   string `db:"entity"`
	App      string `db:"app"`
	Target   string `db:"target"`
	Priority int    `db:"priority"`
	Enabled  int    `db:"enabled"`
	Created  int64  `db:"created"`
	Updated  int64  `db:"updated"`
}

// route_match contains the result of matching a request to a route
type route_match struct {
	route     *route
	remaining string
}

// delegation represents a path delegation granting route management to a user
type delegation struct {
	ID      int    `db:"id"`
	Domain  string `db:"domain"`
	Path    string `db:"path"`
	Owner   int    `db:"owner"`
	Created int64  `db:"created"`
	Updated int64  `db:"updated"`
}

// domains_migrate_config migrates settings and domains from mochi.conf
func domains_migrate_config() {
	if setting_get("domains_registration", "") != "" {
		return
	}

	registration := ini_string("domains", "registration", "system")
	setting_set("domains_registration", registration)

	verification := ini_bool("domains", "verification", false)
	if verification {
		setting_set("domains_verification", "true")
	} else {
		setting_set("domains_verification", "false")
	}

	// migrate domains from [web] domains
	web_domains := ini_string("web", "domains", "")
	if web_domains != "" {
		db := db_open("db/domains.db")
		domains := strings.Split(web_domains, ",")
		n := now()
		for _, d := range domains {
			d = strings.TrimSpace(d)
			if d == "" {
				continue
			}
			if domain_get(d) != nil {
				continue
			}
			token := random_alphanumeric(32)
			db.exec("insert into domains (domain, type, owner, verified, token, tls, created, updated) values (?, 'system', 0, 1, ?, 1, ?, ?)", d, token, n, n)
			info("migrated domain %s from mochi.conf", d)
		}
	}

	info("domain migration complete - you may remove [domains] section from mochi.conf")
}

// domains_init_acme initializes the Let's Encrypt autocert manager
func domains_init_acme() {
	domains_acme_manager = &autocert.Manager{
		Prompt:     autocert.AcceptTOS,
		HostPolicy: domains_host_policy,
		Cache:      autocert.DirCache(filepath.Join(cache_dir, "certs")),
	}
}

// domains_host_policy determines if we should provision a cert for a host
func domains_host_policy(ctx context.Context, host string) error {
	d := domain_lookup(host)
	if d == nil {
		return fmt.Errorf("unknown domain: %s", host)
	}

	if setting_get("domains_verification", "false") == "true" && d.Verified == 0 {
		return fmt.Errorf("unverified domain: %s", host)
	}

	if d.TLS == 0 {
		return fmt.Errorf("tls disabled for domain: %s", host)
	}

	return nil
}

// domains_load_certs loads manual certificates from domains.conf if configured
func domains_load_certs() error {
	path := ini_string("files", "domains", "")
	if path == "" {
		return nil
	}

	if _, err := os.Stat(path); os.IsNotExist(err) {
		return fmt.Errorf("domains.conf not found: %s", path)
	}

	cfg, err := ini.Load(path)
	if err != nil {
		return err
	}

	domains_certs = make(map[string]*tls.Certificate)

	for _, section := range cfg.Sections() {
		name := section.Name()
		if name == "DEFAULT" {
			continue
		}

		cert_path := section.Key("certificate").String()
		key_path := section.Key("key").String()

		if cert_path == "" || key_path == "" {
			return fmt.Errorf("domain %s: certificate and key required", name)
		}

		cert, err := tls.LoadX509KeyPair(cert_path, key_path)
		if err != nil {
			return fmt.Errorf("domain %s: %v", name, err)
		}

		domains_certs[name] = &cert
		info("loaded certificate for %s", name)
	}

	return nil
}

// domains_get_certificate returns the TLS certificate for a domain
func domains_get_certificate(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
	host := hello.ServerName

	if cert := domains_manual_cert(host); cert != nil {
		return cert, nil
	}

	d := domain_lookup(host)
	if d == nil {
		return nil, fmt.Errorf("unknown domain: %s", host)
	}
	if d.TLS == 0 {
		return nil, fmt.Errorf("tls disabled for domain: %s", host)
	}

	if domains_acme_manager != nil {
		return domains_acme_manager.GetCertificate(hello)
	}

	return nil, fmt.Errorf("no certificate for %s", host)
}

// domains_manual_cert looks up a manual certificate from domains.conf
func domains_manual_cert(host string) *tls.Certificate {
	if domains_certs == nil {
		return nil
	}

	if cert, ok := domains_certs[host]; ok {
		return cert
	}

	if idx := strings.Index(host, "."); idx > 0 {
		wildcard := "*" + host[idx:]
		if cert, ok := domains_certs[wildcard]; ok {
			return cert
		}
	}

	return nil
}

// domain_lookup finds a domain entry by host (exact or wildcard)
func domain_lookup(host string) *domain {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}

	d := domain_get(host)
	if d != nil {
		return d
	}

	if idx := strings.Index(host, "."); idx > 0 {
		wildcard := "*" + host[idx:]
		return domain_get(wildcard)
	}

	return nil
}

// domain_get retrieves a domain by name
func domain_get(name string) *domain {
	db := db_open("db/domains.db")
	var d domain
	if !db.scan(&d, "select * from domains where domain=?", name) {
		return nil
	}
	return &d
}

// domain_list returns all domains, optionally filtered by owner
func domain_list(owner int) []domain {
	db := db_open("db/domains.db")
	var domains []domain
	if owner == 0 {
		db.scans(&domains, "select * from domains order by domain")
	} else {
		db.scans(&domains, "select * from domains where owner=? order by domain", owner)
	}
	return domains
}

// domain_register creates a new domain entry
func domain_register(name, dtype string, owner int) (*domain, error) {
	if domain_get(name) != nil {
		return nil, fmt.Errorf("domain already exists")
	}

	db := db_open("db/domains.db")
	n := now()
	token := random_alphanumeric(32)

	db.exec("insert into domains (domain, type, owner, verified, token, tls, created, updated) values (?, ?, ?, 0, ?, 1, ?, ?)", name, dtype, owner, token, n, n)

	return domain_get(name), nil
}

// domain_update updates a domain entry
func domain_update(name string, updates map[string]any) error {
	if len(updates) == 0 {
		return nil
	}

	db := db_open("db/domains.db")
	var sets []string
	var args []any
	for k, v := range updates {
		sets = append(sets, k+"=?")
		args = append(args, v)
	}
	sets = append(sets, "updated=?")
	args = append(args, now())
	args = append(args, name)

	db.exec("update domains set "+strings.Join(sets, ", ")+" where domain=?", args...)
	return nil
}

// domain_delete removes a domain and its routes
func domain_delete(name string) error {
	db := db_open("db/domains.db")
	db.exec("delete from routes where domain=?", name)
	db.exec("delete from domains where domain=?", name)
	return nil
}

// domain_verify checks DNS TXT record for domain verification
func domain_verify(name string) (bool, error) {
	d := domain_get(name)
	if d == nil {
		return false, fmt.Errorf("domain not found")
	}

	lookup_name := strings.TrimPrefix(name, "*.")

	records, err := net.LookupTXT("_mochi-verify." + lookup_name)
	if err != nil {
		return false, err
	}

	expected := "mochi-verify=" + d.Token
	for _, record := range records {
		if record == expected {
			db := db_open("db/domains.db")
			n := now()
			db.exec("update domains set verified=1, updated=? where domain=?", n, name)
			return true, nil
		}
	}

	return false, nil
}

// domain_delegate creates a delegated domain entry
func domain_delegate(parent, name string, owner int, scope, prefix string) (*domain, error) {
	if domain_get(name) != nil {
		return nil, fmt.Errorf("domain already exists")
	}

	db := db_open("db/domains.db")
	n := now()
	token := random_alphanumeric(32)

	db.exec("insert into domains (domain, type, owner, delegator, scope, prefix, verified, token, tls, created, updated) values (?, 'user', ?, ?, ?, ?, 0, ?, 1, ?, ?)", name, owner, parent, scope, prefix, token, n, n)

	return domain_get(name), nil
}

// domain_revoke removes a delegated domain
func domain_revoke(name string) error {
	d := domain_get(name)
	if d == nil {
		return fmt.Errorf("domain not found")
	}
	if d.Delegator == "" {
		return fmt.Errorf("domain is not delegated")
	}
	return domain_delete(name)
}

// domain_delegated returns domains delegated to an owner
func domain_delegated(owner int) []domain {
	db := db_open("db/domains.db")
	var domains []domain
	db.scans(&domains, "select * from domains where owner=? and delegator!='' order by domain", owner)
	return domains
}

// domain_match finds the best matching route for a host and path
func domain_match(host, path string) *route_match {
	d := domain_lookup(host)
	if d == nil {
		return nil
	}

	if setting_get("domains_verification", "false") == "true" && d.Verified == 0 {
		return nil
	}

	db := db_open("db/domains.db")
	var routes []route
	db.scans(&routes, "select * from routes where domain=? and enabled=1 order by priority desc, length(path) desc", d.Domain)

	for _, r := range routes {
		if strings.HasPrefix(path, r.Path) {
			remaining := strings.TrimPrefix(path, r.Path)
			if r.Path == "" || r.Path == "/" || remaining == "" || strings.HasPrefix(remaining, "/") {
				return &route_match{route: &r, remaining: remaining}
			}
		}
	}

	return nil
}

// route_get retrieves a route by domain and path
func route_get(domain_name, path string) *route {
	db := db_open("db/domains.db")
	var r route
	if !db.scan(&r, "select * from routes where domain=? and path=?", domain_name, path) {
		return nil
	}
	return &r
}

// route_list returns all routes for a domain
func route_list(domain_name string) []route {
	db := db_open("db/domains.db")
	var routes []route
	db.scans(&routes, "select * from routes where domain=? order by priority desc, length(path) desc", domain_name)
	return routes
}

// route_create creates a new route
func route_create(domain_name, path, entity, app, target string, priority int) (*route, error) {
	if domain_get(domain_name) == nil {
		return nil, fmt.Errorf("domain not found")
	}
	if route_get(domain_name, path) != nil {
		return nil, fmt.Errorf("route already exists")
	}

	db := db_open("db/domains.db")
	n := now()
	db.exec("insert into routes (domain, path, entity, app, target, priority, enabled, created, updated) values (?, ?, ?, ?, ?, ?, 1, ?, ?)", domain_name, path, entity, app, target, priority, n, n)

	return route_get(domain_name, path), nil
}

// route_update updates a route
func route_update(domain_name, path string, updates map[string]any) error {
	if len(updates) == 0 {
		return nil
	}

	db := db_open("db/domains.db")
	var sets []string
	var args []any
	for k, v := range updates {
		sets = append(sets, k+"=?")
		args = append(args, v)
	}
	sets = append(sets, "updated=?")
	args = append(args, now())
	args = append(args, domain_name)
	args = append(args, path)

	db.exec("update routes set "+strings.Join(sets, ", ")+" where domain=? and path=?", args...)
	return nil
}

// route_delete removes a route
func route_delete(domain_name, path string) error {
	db := db_open("db/domains.db")
	db.exec("delete from routes where domain=? and path=?", domain_name, path)
	return nil
}

// delegation_get retrieves a delegation by domain, path, and owner
func delegation_get(domain_name, path string, owner int) *delegation {
	db := db_open("db/domains.db")
	var d delegation
	if !db.scan(&d, "select * from delegations where domain=? and path=? and owner=?", domain_name, path, owner) {
		return nil
	}
	return &d
}

// delegation_list returns all delegations for a domain, or all delegations for an owner
func delegation_list(domain_name string, owner int) []delegation {
	db := db_open("db/domains.db")
	var delegations []delegation
	if domain_name != "" && owner != 0 {
		db.scans(&delegations, "select * from delegations where domain=? and owner=? order by path", domain_name, owner)
	} else if domain_name != "" {
		db.scans(&delegations, "select * from delegations where domain=? order by path, owner", domain_name)
	} else if owner != 0 {
		db.scans(&delegations, "select * from delegations where owner=? order by domain, path", owner)
	} else {
		db.scans(&delegations, "select * from delegations order by domain, path, owner")
	}
	return delegations
}

// delegation_create creates a new path delegation
func delegation_create(domain_name, path string, owner int) (*delegation, error) {
	if domain_get(domain_name) == nil {
		return nil, fmt.Errorf("domain not found")
	}
	if delegation_get(domain_name, path, owner) != nil {
		return nil, fmt.Errorf("delegation already exists")
	}

	db := db_open("db/domains.db")
	n := now()
	db.exec("insert into delegations (domain, path, owner, created, updated) values (?, ?, ?, ?, ?)", domain_name, path, owner, n, n)

	return delegation_get(domain_name, path, owner), nil
}

// delegation_delete removes a delegation
func delegation_delete(domain_name, path string, owner int) error {
	db := db_open("db/domains.db")
	db.exec("delete from delegations where domain=? and path=? and owner=?", domain_name, path, owner)
	return nil
}

// delegation_check returns true if the user has a delegation for the given domain and path
func delegation_check(domain_name, path string, owner int) bool {
	db := db_open("db/domains.db")
	var delegations []delegation
	db.scans(&delegations, "select * from delegations where domain=? and owner=?", domain_name, owner)
	for _, d := range delegations {
		if strings.HasPrefix(path, d.Path) {
			return true
		}
	}
	return false
}

// domains_middleware returns gin middleware for domain routing
func domains_middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		match := domain_match(c.Request.Host, c.Request.URL.Path)
		if match != nil {
			c.Set("domain_route", match.route)
			c.Set("domain_entity", match.route.Entity)
			c.Set("domain_app", match.route.App)
			c.Set("domain_target", match.route.Target)
			c.Set("domain_remaining", match.remaining)
			c.Set("domain_original_host", c.Request.Host)
		}
		c.Next()
	}
}

// Starlark API

var api_domain_route = sls.FromStringDict(sl.String("mochi.domain.route"), sl.StringDict{
	"get":    sl.NewBuiltin("mochi.domain.route.get", api_domain_route_get),
	"list":   sl.NewBuiltin("mochi.domain.route.list", api_domain_route_list),
	"create": sl.NewBuiltin("mochi.domain.route.create", api_domain_route_create),
	"update": sl.NewBuiltin("mochi.domain.route.update", api_domain_route_update),
	"delete": sl.NewBuiltin("mochi.domain.route.delete", api_domain_route_delete),
})

var api_domain_delegation = sls.FromStringDict(sl.String("mochi.domain.delegation"), sl.StringDict{
	"list":   sl.NewBuiltin("mochi.domain.delegation.list", api_domain_delegation_list),
	"create": sl.NewBuiltin("mochi.domain.delegation.create", api_domain_delegation_create),
	"delete": sl.NewBuiltin("mochi.domain.delegation.delete", api_domain_delegation_delete),
})

var api_domain = sls.FromStringDict(sl.String("mochi.domain"), sl.StringDict{
	"register":   sl.NewBuiltin("mochi.domain.register", api_domain_register),
	"get":        sl.NewBuiltin("mochi.domain.get", api_domain_get),
	"list":       sl.NewBuiltin("mochi.domain.list", api_domain_list),
	"update":     sl.NewBuiltin("mochi.domain.update", api_domain_update),
	"delete":     sl.NewBuiltin("mochi.domain.delete", api_domain_delete),
	"verify":     sl.NewBuiltin("mochi.domain.verify", api_domain_verify),
	"delegate":   sl.NewBuiltin("mochi.domain.delegate", api_domain_delegate),
	"revoke":     sl.NewBuiltin("mochi.domain.revoke", api_domain_revoke),
	"delegated":  sl.NewBuiltin("mochi.domain.delegated", api_domain_delegated),
	"lookup":     sl.NewBuiltin("mochi.domain.lookup", api_domain_lookup),
	"route":      api_domain_route,
	"delegation": api_domain_delegation,
})

// domain_can_manage checks if a user can manage a domain (admin or owner)
func domain_can_manage(user *User, d *domain) bool {
	if user == nil || d == nil {
		return false
	}
	if user.administrator() {
		return true
	}
	if d.Owner != 0 && d.Owner == user.ID {
		return true
	}
	return false
}

// domain_can_manage_route checks if a user can manage routes on a domain
// Checks domain ownership, subdomain delegation, and path delegations
func domain_can_manage_route(user *User, d *domain, path string) bool {
	if user == nil || d == nil {
		return false
	}
	// Admins can manage any route
	if user.administrator() {
		return true
	}
	// Domain owner can manage routes (for subdomain delegations)
	if d.Owner != 0 && d.Owner == user.ID {
		// For path-scoped subdomain delegations, check prefix
		if d.Scope == "path" && d.Prefix != "" {
			return strings.HasPrefix(path, d.Prefix)
		}
		return true
	}
	// Check path delegations table
	if delegation_check(d.Domain, path, user.ID) {
		return true
	}
	return false
}

// mochi.domain.register(domain) -> dict: Register a new domain
func api_domain_register(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <domain: string>")
	}

	name, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid domain name")
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	if !user.administrator() {
		return sl_error(fn, "not administrator")
	}

	_, err := domain_register(name, "system", 0)
	if err != nil {
		return sl_error(fn, "%v", err)
	}

	db := db_open("db/domains.db")
	row, _ := db.row("select * from domains where domain=?", name)
	return sl_encode(row), nil
}

// mochi.domain.get(domain) -> dict or None: Get domain by name
func api_domain_get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <domain: string>")
	}

	name, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid domain name")
	}

	db := db_open("db/domains.db")
	row, _ := db.row("select * from domains where domain=?", name)
	if row == nil {
		return sl.None, nil
	}
	return sl_encode(row), nil
}

// mochi.domain.list(owner=0) -> list: List domains
func api_domain_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	owner := 0
	if len(args) > 0 {
		owner, _ = sl.AsInt32(args[0])
	}

	db := db_open("db/domains.db")
	var rows []map[string]any
	if owner == 0 {
		rows, _ = db.rows("select * from domains order by domain")
	} else {
		rows, _ = db.rows("select * from domains where owner=? order by domain", owner)
	}
	return sl_encode(rows), nil
}

// mochi.domain.update(domain, verified=None, tls=None) -> dict: Update domain settings
func api_domain_update(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 {
		return sl_error(fn, "syntax: <domain: string>, [verified: bool], [tls: bool]")
	}

	name, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid domain name")
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	d := domain_get(name)
	if d == nil {
		return sl_error(fn, "domain not found")
	}

	if !domain_can_manage(user, d) {
		return sl_error(fn, "access denied")
	}

	updates := make(map[string]any)
	for _, kw := range kwargs {
		key, _ := sl.AsString(kw[0])
		switch key {
		case "verified":
			// Only admins can change verified status
			if !user.administrator() {
				continue
			}
			if b, ok := kw[1].(sl.Bool); ok {
				if b {
					updates["verified"] = 1
				} else {
					updates["verified"] = 0
				}
			}
		case "tls":
			if b, ok := kw[1].(sl.Bool); ok {
				if b {
					updates["tls"] = 1
				} else {
					updates["tls"] = 0
				}
			}
		}
	}

	err := domain_update(name, updates)
	if err != nil {
		return sl_error(fn, "%v", err)
	}

	db := db_open("db/domains.db")
	row, _ := db.row("select * from domains where domain=?", name)
	if row == nil {
		return sl.None, nil
	}
	return sl_encode(row), nil
}

// mochi.domain.delete(domain) -> bool: Delete domain and all its routes
func api_domain_delete(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <domain: string>")
	}

	name, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid domain name")
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	if !user.administrator() {
		return sl_error(fn, "not administrator")
	}

	err := domain_delete(name)
	if err != nil {
		return sl_error(fn, "%v", err)
	}

	return sl.True, nil
}

// mochi.domain.verify(domain) -> bool: Check DNS and update verified status
func api_domain_verify(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <domain: string>")
	}

	name, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid domain name")
	}

	verified, err := domain_verify(name)
	if err != nil {
		return sl_error(fn, "%v", err)
	}

	if verified {
		return sl.True, nil
	}
	return sl.False, nil
}

// mochi.domain.delegate(domain, owner, scope, prefix=None) -> dict: Delegate domain to user
func api_domain_delegate(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 3 {
		return sl_error(fn, "syntax: <parent: string>, <owner: int>, <scope: string>, [prefix: string]")
	}

	parent, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid parent domain")
	}

	owner, err := sl.AsInt32(args[1])
	if err != nil {
		return sl_error(fn, "invalid owner: must be user ID")
	}

	scope, ok := sl.AsString(args[2])
	if !ok {
		return sl_error(fn, "invalid scope")
	}

	prefix := ""
	if len(args) > 3 {
		prefix, _ = sl.AsString(args[3])
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	if !user.administrator() {
		return sl_error(fn, "not administrator")
	}

	// Verify owner user exists
	if user_by_id(int(owner)) == nil {
		return sl_error(fn, "owner user not found")
	}

	// determine the new domain name based on scope
	var name string
	switch scope {
	case "full":
		name = parent
	case "subdomain":
		if prefix == "" {
			return sl_error(fn, "prefix required for subdomain delegation")
		}
		// parent should be *.example.com, new domain is prefix.example.com
		name = prefix + strings.TrimPrefix(parent, "*")
	case "path":
		name = parent
	default:
		return sl_error(fn, "invalid scope: must be 'full', 'subdomain', or 'path'")
	}

	_, err = domain_delegate(parent, name, int(owner), scope, prefix)
	if err != nil {
		return sl_error(fn, "%v", err)
	}

	db := db_open("db/domains.db")
	row, _ := db.row("select * from domains where domain=?", name)
	return sl_encode(row), nil
}

// mochi.domain.revoke(domain) -> bool: Revoke a delegated domain
func api_domain_revoke(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <domain: string>")
	}

	name, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid domain name")
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	if !user.administrator() {
		return sl_error(fn, "not administrator")
	}

	err := domain_revoke(name)
	if err != nil {
		return sl_error(fn, "%v", err)
	}

	return sl.True, nil
}

// mochi.domain.delegated(owner) -> list: List domains delegated to a user
func api_domain_delegated(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <owner: int>")
	}

	owner, err := sl.AsInt32(args[0])
	if err != nil {
		return sl_error(fn, "invalid owner")
	}

	db := db_open("db/domains.db")
	rows, _ := db.rows("select * from domains where owner=? and delegator!='' order by domain", owner)
	return sl_encode(rows), nil
}

// mochi.domain.lookup(host) -> dict or None: Find domain entry for host
func api_domain_lookup(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <host: string>")
	}

	host, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid host")
	}

	d := domain_lookup(host)
	if d == nil {
		return sl.None, nil
	}

	db := db_open("db/domains.db")
	row, _ := db.row("select * from domains where domain=?", d.Domain)
	return sl_encode(row), nil
}

// mochi.domain.route.get(domain, path) -> dict or None: Get a specific route
func api_domain_route_get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return sl_error(fn, "syntax: <domain: string>, <path: string>")
	}

	domain_name, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid domain")
	}

	path, ok := sl.AsString(args[1])
	if !ok {
		return sl_error(fn, "invalid path")
	}

	db := db_open("db/domains.db")
	row, _ := db.row("select * from routes where domain=? and path=?", domain_name, path)
	if row == nil {
		return sl.None, nil
	}
	return sl_encode(row), nil
}

// mochi.domain.route.list(domain) -> list: List all routes for a domain
func api_domain_route_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <domain: string>")
	}

	domain_name, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid domain")
	}

	db := db_open("db/domains.db")
	rows, _ := db.rows("select * from routes where domain=? order by priority desc, length(path) desc", domain_name)
	return sl_encode(rows), nil
}

// mochi.domain.route.create(domain, path, entity, app=None, target=None, priority=0) -> dict: Create route
func api_domain_route_create(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 3 {
		return sl_error(fn, "syntax: <domain: string>, <path: string>, <entity: string>, [app: string], [target: string], [priority: int]")
	}

	domain_name, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid domain")
	}

	path, ok := sl.AsString(args[1])
	if !ok {
		return sl_error(fn, "invalid path")
	}

	entity, ok := sl.AsString(args[2])
	if !ok {
		return sl_error(fn, "invalid entity")
	}

	app := ""
	if len(args) > 3 {
		app, _ = sl.AsString(args[3])
	}

	target := ""
	if len(args) > 4 {
		target, _ = sl.AsString(args[4])
	}

	priority := 0
	if len(args) > 5 {
		if p, err := sl.AsInt32(args[5]); err == nil {
			priority = int(p)
		}
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	d := domain_get(domain_name)
	if d == nil {
		return sl_error(fn, "domain not found")
	}

	if !domain_can_manage_route(user, d, path) {
		return sl_error(fn, "access denied")
	}

	_, err := route_create(domain_name, path, entity, app, target, priority)
	if err != nil {
		return sl_error(fn, "%v", err)
	}

	db := db_open("db/domains.db")
	row, _ := db.row("select * from routes where domain=? and path=?", domain_name, path)
	return sl_encode(row), nil
}

// mochi.domain.route.update(domain, path, entity=None, app=None, target=None, priority=None, enabled=None) -> dict: Update route
func api_domain_route_update(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 2 {
		return sl_error(fn, "syntax: <domain: string>, <path: string>, [entity: string], [app: string], [target: string], [priority: int], [enabled: bool]")
	}

	domain_name, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid domain")
	}

	path, ok := sl.AsString(args[1])
	if !ok {
		return sl_error(fn, "invalid path")
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	d := domain_get(domain_name)
	if d == nil {
		return sl_error(fn, "domain not found")
	}

	if !domain_can_manage_route(user, d, path) {
		return sl_error(fn, "access denied")
	}

	updates := make(map[string]any)
	for _, kw := range kwargs {
		key, _ := sl.AsString(kw[0])
		switch key {
		case "entity":
			if s, ok := sl.AsString(kw[1]); ok {
				updates["entity"] = s
			}
		case "app":
			if s, ok := sl.AsString(kw[1]); ok {
				updates["app"] = s
			}
		case "target":
			if s, ok := sl.AsString(kw[1]); ok {
				updates["target"] = s
			}
		case "priority":
			if p, err := sl.AsInt32(kw[1]); err == nil {
				updates["priority"] = int(p)
			}
		case "enabled":
			if b, ok := kw[1].(sl.Bool); ok {
				if b {
					updates["enabled"] = 1
				} else {
					updates["enabled"] = 0
				}
			}
		}
	}

	err := route_update(domain_name, path, updates)
	if err != nil {
		return sl_error(fn, "%v", err)
	}

	db := db_open("db/domains.db")
	row, _ := db.row("select * from routes where domain=? and path=?", domain_name, path)
	if row == nil {
		return sl.None, nil
	}
	return sl_encode(row), nil
}

// mochi.domain.route.delete(domain, path) -> bool: Delete a route
func api_domain_route_delete(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return sl_error(fn, "syntax: <domain: string>, <path: string>")
	}

	domain_name, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid domain")
	}

	path, ok := sl.AsString(args[1])
	if !ok {
		return sl_error(fn, "invalid path")
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	d := domain_get(domain_name)
	if d == nil {
		return sl_error(fn, "domain not found")
	}

	if !domain_can_manage_route(user, d, path) {
		return sl_error(fn, "access denied")
	}

	err := route_delete(domain_name, path)
	if err != nil {
		return sl_error(fn, "%v", err)
	}

	return sl.True, nil
}

// mochi.domain.delegation.list(domain="", owner=0) -> list: List delegations
func api_domain_delegation_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	domain_name := ""
	owner := 0

	if len(args) > 0 {
		domain_name, _ = sl.AsString(args[0])
	}
	if len(args) > 1 {
		owner, _ = sl.AsInt32(args[1])
	}

	db := db_open("db/domains.db")
	var rows []map[string]any
	var err error
	if domain_name != "" && owner != 0 {
		rows, err = db.rows("select * from delegations where domain=? and owner=? order by path", domain_name, owner)
	} else if domain_name != "" {
		rows, err = db.rows("select * from delegations where domain=? order by path, owner", domain_name)
	} else if owner != 0 {
		rows, err = db.rows("select * from delegations where owner=? order by domain, path", owner)
	} else {
		rows, err = db.rows("select * from delegations order by domain, path, owner")
	}
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	return sl_encode(rows), nil
}

// mochi.domain.delegation.create(domain, path, owner) -> dict: Create a path delegation
func api_domain_delegation_create(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 3 {
		return sl_error(fn, "syntax: <domain: string>, <path: string>, <owner: int>")
	}

	domain_name, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid domain")
	}

	path, ok := sl.AsString(args[1])
	if !ok {
		return sl_error(fn, "invalid path")
	}

	owner, err := sl.AsInt32(args[2])
	if err != nil {
		return sl_error(fn, "invalid owner")
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	if !user.administrator() {
		return sl_error(fn, "not administrator")
	}

	// Verify owner user exists
	if user_by_id(int(owner)) == nil {
		return sl_error(fn, "owner user not found")
	}

	_, err = delegation_create(domain_name, path, int(owner))
	if err != nil {
		return sl_error(fn, "%v", err)
	}

	db := db_open("db/domains.db")
	row, _ := db.row("select * from delegations where domain=? and path=? and owner=?", domain_name, path, owner)
	return sl_encode(row), nil
}

// mochi.domain.delegation.delete(domain, path, owner) -> bool: Delete a delegation
func api_domain_delegation_delete(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 3 {
		return sl_error(fn, "syntax: <domain: string>, <path: string>, <owner: int>")
	}

	domain_name, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid domain")
	}

	path, ok := sl.AsString(args[1])
	if !ok {
		return sl_error(fn, "invalid path")
	}

	owner, err := sl.AsInt32(args[2])
	if err != nil {
		return sl_error(fn, "invalid owner")
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	if !user.administrator() {
		return sl_error(fn, "not administrator")
	}

	err = delegation_delete(domain_name, path, int(owner))
	if err != nil {
		return sl_error(fn, "%v", err)
	}

	return sl.True, nil
}

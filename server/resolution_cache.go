package main

import (
	"sync"
	"sync/atomic"
)

// Resolution caches.
//
// app_for_service / app_for_path / class_app_for and App.active are
// called on the hot paths — app_for_service per routed P2P event (see
// events.go route()), app_for_path per web request (web.go), and each
// asks for the resolved app's active version. Every call previously ran
// several uncached SQL queries: the per-user binding and the system
// binding (a per-app version pin, service/path/class binding), plus —
// when nothing was bound — a fallback that scanned every installed app,
// resolving each one's version with yet more queries. Under load the
// SQLite parser dominated CPU.
//
// These caches collapse the steady-state resolution to a map lookup:
//   - version cache: (user, app)     -> resolved *AppVersion
//   - service cache: (user, service) -> resolved *App
//   - path cache:    (user, path)    -> resolved *App
//   - class cache:   (user, class)   -> resolved *App
//
// Invalidation is two-layered. A generation counter is bumped on every
// local write that changes a resolution input (version prefs, service/
// path/class bindings, system defaults/tracks, app version load/reload);
// a bump makes every cache discard its contents on next access, so a user
// who changes a binding sees it immediately. As a backstop for writes
// that arrive via the replication apply path — which these caches
// deliberately do not instrument, to stay out of that code — entries also
// expire after resolution_cache_ttl, bounding both staleness and memory.

// resolution_cache_ttl is how long (seconds) a resolved entry is trusted
// before it is recomputed. Short enough that a replicated input change
// self-heals quickly; long enough that the per-event query rate collapses
// to at most one query per key per window.
const resolution_cache_ttl = 30

var (
	// resolution_generation is bumped by resolution_invalidate on any
	// local write that changes a resolution input. Lock-free so writers
	// need no cache lock.
	resolution_generation atomic.Uint64

	// Version cache, guarded by apps_lock: App.active_locked (its only
	// reader and writer) already runs with apps_lock held.
	resolution_version_cache     = map[resolution_key]resolution_version_entry{}
	resolution_version_cache_gen uint64

	// Service / path / class caches: (user, subject) -> *App. Each has its
	// own lock because its resolver (app_for_service / app_for_path /
	// class_app_for) runs outside apps_lock — it calls app_by_id, which
	// takes apps_lock.
	resolution_services = &app_resolution_cache{entries: map[resolution_key]resolution_app_entry{}}
	resolution_paths    = &app_resolution_cache{entries: map[resolution_key]resolution_app_entry{}}
	resolution_classes  = &app_resolution_cache{entries: map[resolution_key]resolution_app_entry{}}
)

// resolution_key identifies a cached resolution. subject is the app id
// (version cache) or the service/path/class name. user is the user's UID,
// or "" for an anonymous/nil user.
type resolution_key struct {
	user    string
	subject string
}

type resolution_version_entry struct {
	version *AppVersion
	expires int64
}

type resolution_app_entry struct {
	app     *App
	expires int64
}

// app_resolution_cache caches (user, subject) -> *App for one resolver
// kind (service, path, or class). Entries gate on the shared
// resolution_generation and expire after resolution_cache_ttl.
type app_resolution_cache struct {
	lock    sync.Mutex
	entries map[resolution_key]resolution_app_entry
	gen     uint64
}

func (c *app_resolution_cache) get(key resolution_key) (*App, bool) {
	c.lock.Lock()
	defer c.lock.Unlock()
	gen := resolution_generation.Load()
	if gen != c.gen {
		c.entries = map[resolution_key]resolution_app_entry{}
		c.gen = gen
		return nil, false
	}
	e, ok := c.entries[key]
	if !ok || now() >= e.expires {
		return nil, false
	}
	return e.app, true
}

func (c *app_resolution_cache) put(key resolution_key, a *App) {
	c.lock.Lock()
	defer c.lock.Unlock()
	gen := resolution_generation.Load()
	if gen != c.gen {
		c.entries = map[resolution_key]resolution_app_entry{}
		c.gen = gen
	}
	c.entries[key] = resolution_app_entry{app: a, expires: now() + resolution_cache_ttl}
}

// resolution_user_key returns the cache user component for a resolved
// user: its UID, or "" for nil.
func resolution_user_key(user *User) string {
	if user == nil {
		return ""
	}
	return user.UID
}

// resolution_invalidate discards every cache on its next access. Call
// after any local write that changes a resolution input: app version
// preferences, service/path/class bindings, system defaults/tracks, and
// app version load/reload.
func resolution_invalidate() {
	resolution_generation.Add(1)
}

// resolution_version_get returns the cached active version for a key, or
// (nil, false) on miss/stale. Must be called with apps_lock held.
func resolution_version_get(key resolution_key) (*AppVersion, bool) {
	gen := resolution_generation.Load()
	if gen != resolution_version_cache_gen {
		resolution_version_cache = map[resolution_key]resolution_version_entry{}
		resolution_version_cache_gen = gen
		return nil, false
	}
	e, ok := resolution_version_cache[key]
	if !ok || now() >= e.expires {
		return nil, false
	}
	return e.version, true
}

// resolution_version_put stores a resolved active version. Must be called
// with apps_lock held.
func resolution_version_put(key resolution_key, av *AppVersion) {
	if resolution_generation.Load() != resolution_version_cache_gen {
		resolution_version_cache = map[resolution_key]resolution_version_entry{}
		resolution_version_cache_gen = resolution_generation.Load()
	}
	resolution_version_cache[key] = resolution_version_entry{version: av, expires: now() + resolution_cache_ttl}
}

package main

import (
	"sync"
	"sync/atomic"
)

// Resolution caches.
//
// app_for_service and App.active are called on every routed P2P event
// (see events.go route(), which resolves the handler app and then asks
// for its active version — twice). Each call previously ran several
// uncached SQL queries: the per-user version preference and service
// binding (user.db), the system default version/track and service
// binding (apps.db), and — when nothing was bound — a fallback that
// scanned every installed app, resolving each one's version with yet
// more queries. Under P2P load the SQLite parser dominated CPU.
//
// These two caches collapse the steady-state resolution to a map lookup:
//   - version cache: (user, app)     -> resolved *AppVersion
//   - service cache: (user, service) -> resolved *App
//
// Invalidation is two-layered. A generation counter is bumped on every
// local write that changes a resolution input (version prefs, service
// bindings, system defaults/tracks, app version load/reload); a bump
// makes both caches discard their contents on next access, so a user who
// changes their version sees it immediately. As a backstop for writes
// that arrive via the replication apply path — which this file
// deliberately does not instrument, to stay out of that code — entries
// also expire after resolution_cache_ttl, bounding both staleness and
// memory.

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

	// Service cache, guarded by its own lock: app_for_service does not
	// run under apps_lock (it calls app_by_id, which takes apps_lock).
	resolution_service_lock      sync.Mutex
	resolution_service_cache     = map[resolution_key]resolution_service_entry{}
	resolution_service_cache_gen uint64
)

// resolution_key identifies a cached resolution. subject is the app id
// (version cache) or the service name (service cache). user is the user's
// UID, or "" for an anonymous/nil user.
type resolution_key struct {
	user    string
	subject string
}

type resolution_version_entry struct {
	version *AppVersion
	expires int64
}

type resolution_service_entry struct {
	app     *App
	expires int64
}

// resolution_user_key returns the cache user component for a resolved
// user: its UID, or "" for nil.
func resolution_user_key(user *User) string {
	if user == nil {
		return ""
	}
	return user.UID
}

// resolution_invalidate discards both caches on their next access. Call
// after any local write that changes a resolution input: app version
// preferences, service bindings, system defaults/tracks, and app version
// load/reload.
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

// resolution_service_get returns the cached app for a (user, service)
// key, or (nil, false) on miss/stale.
func resolution_service_get(key resolution_key) (*App, bool) {
	resolution_service_lock.Lock()
	defer resolution_service_lock.Unlock()
	gen := resolution_generation.Load()
	if gen != resolution_service_cache_gen {
		resolution_service_cache = map[resolution_key]resolution_service_entry{}
		resolution_service_cache_gen = gen
		return nil, false
	}
	e, ok := resolution_service_cache[key]
	if !ok || now() >= e.expires {
		return nil, false
	}
	return e.app, true
}

// resolution_service_put stores a resolved (user, service) -> app.
func resolution_service_put(key resolution_key, a *App) {
	resolution_service_lock.Lock()
	defer resolution_service_lock.Unlock()
	if resolution_generation.Load() != resolution_service_cache_gen {
		resolution_service_cache = map[resolution_key]resolution_service_entry{}
		resolution_service_cache_gen = resolution_generation.Load()
	}
	resolution_service_cache[key] = resolution_service_entry{app: a, expires: now() + resolution_cache_ttl}
}

package main

import (
	"sync"
	"sync/atomic"
)

// Resolution caches: (user, app) -> *AppVersion and (user, service|path|class)
// -> *App, collapsing the hot-path resolution queries to a map lookup.
// Invalidated by a generation counter bumped on any local write that changes a
// resolution input, with resolution_cache_ttl as the backstop for a missed
// bump.

// resolution_cache_ttl is how long (seconds) a resolved entry is trusted
// before it is recomputed. Short enough that a missed invalidation self-heals
// quickly; long enough that the per-event query rate collapses to at most one
// query per key per window.
const resolution_cache_ttl = 30

// resolution_cache_maximum bounds the entry count. get() treats an expired
// entry as a miss but leaves it behind, and resolution_invalidate only fires
// on a configuration write, so without a ceiling the map grows for the life of
// the process.
const resolution_cache_maximum = 10000

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

	// Visibility cache: (user, app) -> whether the app's require.function
	// offered it. Its own lock for the same reason as the three above, and
	// its own TTLs because the answer costs a call into the app rather than
	// a local query.
	resolution_visibility = &visibility_resolution_cache{entries: map[resolution_key]visibility_entry{}}
)

// visibility_cache_ttl is how long a real answer from an app's require.function
// is trusted. Longer than resolution_cache_ttl because obtaining it can cost a
// request to another service. The cost is that adding or removing someone takes
// up to this long to show; the app's own handlers gate every action regardless,
// so a stale "yes" offers an icon, not access.
const visibility_cache_ttl = 300

// visibility_cache_failure is how long a *failure* to get an answer is trusted.
// Short, so an authority that comes back is believed again quickly, but not
// zero, or an outage turns every page render into a fresh attempt.
const visibility_cache_failure = 30

type visibility_entry struct {
	allowed bool
	expires int64
}

// visibility_resolution_cache caches (user, app) -> bool. Entries gate on the
// shared resolution_generation and expire after a per-entry lifetime, since a
// real answer and a failed attempt are worth trusting for different lengths of
// time.
type visibility_resolution_cache struct {
	lock    sync.Mutex
	entries map[resolution_key]visibility_entry
	gen     uint64
}

func (c *visibility_resolution_cache) get(key resolution_key) (bool, bool) {
	c.lock.Lock()
	defer c.lock.Unlock()
	gen := resolution_generation.Load()
	if gen != c.gen {
		c.entries = map[resolution_key]visibility_entry{}
		c.gen = gen
		return false, false
	}
	e, ok := c.entries[key]
	if !ok || now() >= e.expires {
		return false, false
	}
	return e.allowed, true
}

// previous returns the last answer stored for a key whether or not it is still
// fresh. Entries are only discarded when the generation changes, so an expired
// one is still evidence of what the app said last time. app_visible falls back
// to it when a check fails, so a momentarily unreachable authority cannot lock
// out someone it has already admitted. Nobody it never admitted is let in: a
// key with no entry answers false.
func (c *visibility_resolution_cache) previous(key resolution_key) (bool, bool) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if resolution_generation.Load() != c.gen {
		return false, false
	}
	e, ok := c.entries[key]
	if !ok {
		return false, false
	}
	return e.allowed, true
}

func (c *visibility_resolution_cache) put(key resolution_key, allowed bool, ttl int64) {
	c.lock.Lock()
	defer c.lock.Unlock()
	gen := resolution_generation.Load()
	if gen != c.gen {
		c.entries = map[resolution_key]visibility_entry{}
		c.gen = gen
	}
	c.entries[key] = visibility_entry{allowed: allowed, expires: now() + ttl}
}

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
	if len(c.entries) >= resolution_cache_maximum {
		for k, e := range c.entries {
			if now() >= e.expires {
				delete(c.entries, k)
			}
		}
		// Still at the ceiling with nothing expired: drop the lot rather than
		// grow. A cleared cache costs one resolve per live service.
		if len(c.entries) >= resolution_cache_maximum {
			c.entries = map[resolution_key]resolution_app_entry{}
		}
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

// resolution_invalidate discards every cache on its next access, the
// visibility cache included. Call after any local write that changes a
// resolution input: app version preferences, service/path/class bindings,
// system defaults/tracks, and app version load/reload.
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

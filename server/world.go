// Mochi server: World server listings
//
// A co-located mochi-world server pushes its status over a local socket
// (world_unix.go / world_windows.go) and this server gossips it over pubsub. A
// listing is an attribute of the PEER - GossipSub's StrictSign already
// authenticates the sender, so no entity keys are involved. Rows are keyed
// (peer, world id), hold only the latest state, and age out after three missed
// refresh floors. The socket's group permission is the entire credential: an
// app JWT is signed with the session secret and would die on logout or a
// sessions.db wipe.//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

const (
	world_publish_minimum = 60   // seconds between gossip publishes per world: the server-side debounce, whatever cadence the world promises
	world_seen_expiry     = 2700 // seconds before an unrefreshed row leaves the table: three missed 15-minute floors
	world_services_most   = 16   // services one world may announce: a bound, not a target
	world_name_most       = 64   // runes in a display name: it renders on every server's join page

	// world_ids_most bounds distinct worlds one peer may hold rows for: the
	// per-world debounce is keyed on the id, so a caller whose id varies writes a
	// fresh row per push. Past the cap the least recently seen row is evicted,
	// never the push refused.
	world_ids_most = 100
)

// world_service is one hosted game's slice of an announcement.
type world_service struct {
	Service string `json:"service"`
	Players int64  `json:"players"`
	Name    string `json:"name,omitempty"` // optional per-service display name; the world name serves otherwise
}

// world_recent debounces outbound gossip per world id: an unchanged push inside
// the minimum interval stores locally but does not flood. Swept by
// world_manager on the table's own expiry, so it cannot outlive the listing it
// debounces.
var (
	world_lock   sync.Mutex
	world_recent = map[string]struct {
		content   string
		published int64
	}{}
)

// world_init registers the built-in world app, creates the table, and starts
// the expiry sweep. Called from main_serve alongside directory_init.
func world_init() {
	a := app("world")
	a.service("world")
	// The payload is a claim about the sending peer itself, which StrictSign
	// has already authenticated; the message envelope is anonymous.
	a.event_anonymous("publish", world_publish_event)

	db := db_open("db/world.db")
	db.exec("create table if not exists worlds ( peer text not null, world text not null, name text not null, address text not null, version integer not null, services text not null, seen integer not null, primary key (peer, world) )")
	db.exec("create index if not exists worlds_seen on worlds( seen )")

	go world_manager()
}

// world_manager ages out listings whose world has stopped refreshing.
func world_manager() {
	for range time.Tick(5 * time.Minute) {
		db := db_open("db/world.db")
		db.exec("delete from worlds where seen < ?", now()-world_seen_expiry)
		world_recent_prune()
	}
}

// world_recent_prune drops debounce entries for worlds that have stopped
// pushing, on the same expiry as the rows they belong to.
func world_recent_prune() {
	cutoff := now() - world_seen_expiry
	world_lock.Lock()
	defer world_lock.Unlock()
	for id, recent := range world_recent {
		if recent.published < cutoff {
			delete(world_recent, id)
		}
	}
}

// match_world_address_scheme captures a leading "scheme:" if the address has
// one. The scheme grammar allows dots, so "example.com:4433" matches with
// "example.com" as the scheme - which is why the caller distinguishes a real
// scheme from a host:port by what follows the colon.
var match_world_address_scheme = regexp.MustCompile(`(?i)^([a-z][a-z0-9+.-]*):`)

// world_address_valid accepts what a world server is reachable at - a host,
// host:port, or an http/https URL - and refuses every other scheme.
// valid(address, "url") is only a charset check: "javascript:alert%281%29"
// passes it, and the listing is gossiped on and rendered on other hosts' join
// pages.
func world_address_valid(address string) bool {
	if address == "" || !valid(address, "url") {
		return false
	}
	found := match_world_address_scheme.FindStringSubmatch(address)
	if found == nil {
		return true // a bare host, no colon at all
	}
	switch strings.ToLower(found[1]) {
	case "http", "https":
		return true
	}
	// Not a scheme we allow, so the only remaining legitimate reading is
	// host:port - everything after the colon must be the port.
	port := address[len(found[0]):]
	if port == "" {
		return false
	}
	for _, digit := range port {
		if digit < '0' || digit > '9' {
			return false
		}
	}
	return true
}

// world_validate checks one announcement's fields, local push and gossip
// alike — the strings render on every server's join page, so bounds are not
// negotiable. Returns the parsed services on success.
func world_validate(id, name, address, version, services string) ([]world_service, bool) {
	if !valid(id, "id") || !valid(name, "display") || len([]rune(name)) > world_name_most {
		return nil, false
	}
	if !world_address_valid(address) {
		return nil, false
	}
	if !valid(version, "integer") {
		return nil, false
	}
	var list []world_service
	if json.Unmarshal([]byte(services), &list) != nil || len(list) == 0 || len(list) > world_services_most {
		return nil, false
	}
	for _, s := range list {
		if !valid(s.Service, "constant") || s.Players < 0 || s.Players > 100000 {
			return nil, false
		}
		if s.Name != "" && (!valid(s.Name, "display") || len([]rune(s.Name)) > world_name_most) {
			return nil, false
		}
	}
	return list, true
}

// world_store upserts one listing row.
func world_store(peer, id, name, address string, version int64, services string) {
	db := db_open("db/world.db")

	// Only a world this peer has not listed before grows the table; a repeated
	// id is a replace. Evicting the least recently seen keeps a caller whose id
	// varies from displacing every other world with its own churn.
	if have, _ := db.exists("select 1 from worlds where peer=? and world=?", peer, id); !have {
		if db.integer("select count(*) from worlds where peer=?", peer) >= world_ids_most {
			db.exec(`delete from worlds where peer=? and world in (
				select world from worlds where peer=? order by seen limit 1)`, peer, peer)
		}
	}

	db.exec("replace into worlds (peer, world, name, address, version, services, seen) values (?, ?, ?, ?, ?, ?, ?)",
		peer, id, name, address, version, services, now())
}

// world_status_handler receives a local world server's push. The socket's
// group permission authorized the connection before it reached Gin, so the
// body is trusted to be a local claim — but never to be well-formed.
func world_status_handler(c *gin.Context) {
	var input struct {
		World struct {
			ID      string `json:"id"`
			Name    string `json:"name"`
			Address string `json:"address"`
			Version int64  `json:"version"`
		} `json:"world"`
		Services []world_service `json:"services"`
	}
	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid status"}) // i18n-ok: local machine channel, operator-facing
		return
	}
	services, err := json.Marshal(input.Services)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid services"}) // i18n-ok: as above
		return
	}
	list, ok := world_validate(input.World.ID, input.World.Name, input.World.Address, i64toa(input.World.Version), string(services))
	if !ok || len(list) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid status"}) // i18n-ok: as above
		return
	}

	world_store(net_id, input.World.ID, input.World.Name, input.World.Address, input.World.Version, string(services))

	// Audited because the effect leaves the machine: the row gossips on to other
	// hosts' join pages, and this socket's group is looser than the admin
	// socket's, so which member pushed and which world matter.
	uid, gid, pid := audit_peer_identity(c.Request.Context())
	audit_log_daemon(fmt.Sprintf("world.status peer_uid=%d peer_gid=%d peer_pid=%d world=%q",
		uid, gid, pid, input.World.ID))

	// Gossip on change, floored to one flood per minute per world: the local
	// row above always holds the latest truth, and a suppressed change
	// surfaces at the world's next push anyway.
	content := input.World.Name + "\x00" + input.World.Address + "\x00" + i64toa(input.World.Version) + "\x00" + string(services)
	world_lock.Lock()
	recent := world_recent[input.World.ID]
	fresh := content != recent.content
	due := now()-recent.published >= world_publish_minimum
	if due && (fresh || now()-recent.published >= world_seen_expiry/3) && rate_limit_world_gossip.allow(net_id) {
		world_recent[input.World.ID] = struct {
			content   string
			published int64
		}{content, now()}
		world_lock.Unlock()
		m := message("", "", "world", "publish")
		m.set("world", input.World.ID, "name", input.World.Name, "address", input.World.Address,
			"version", i64toa(input.World.Version), "services", string(services))
		m.publish(false)
	} else {
		world_lock.Unlock()
	}

	c.JSON(http.StatusOK, gin.H{"stored": true})
}

// world_health lets a world server confirm the pairing works.
func world_health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"peer": net_id})
}

// world_body_maximum bounds a status push. Not a security boundary - the socket
// is a local UDS behind a 0660 group and an SO_PEERCRED check - but insurance
// against a buggy world server: ShouldBindJSON reads to completion before
// world_validate runs, so an uncapped runaway payload OOMs the server.
const world_body_maximum = 64 << 10

// world_register_routes wires the world socket's handlers. Shared by the Unix
// and Windows listeners, so the body cap cannot be applied to one and missed on
// the other.
func world_register_routes(r *gin.Engine) {
	r.Use(func(c *gin.Context) {
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, world_body_maximum)
		c.Next()
	})
	r.POST("/_/world/status", world_status_handler)
	r.GET("/_/world/health", world_health)
}

// rate_limit_world_publish bounds inbound gossip per peer: a correct world
// stack floods at most once a minute per world, so a peer exceeding this
// severalfold is broken or hostile either way.
var rate_limit_world_publish = &rate_limiter{
	entries: make(map[string]*rate_limit_entry),
	limit:   30,
	window:  60,
}

// rate_limit_world_gossip bounds what this server floods outward across all
// worlds: world_publish_minimum is per id, and every fresh id has published ==
// 0, so a caller whose id varies passes the interval check every time.
var rate_limit_world_gossip = &rate_limiter{
	entries: make(map[string]*rate_limit_entry),
	limit:   30,
	window:  60,
}

// world_publish_event stores a listing announced by another peer, keyed on
// e.origin - the StrictSign-verified originator - never on e.peer, the last-hop
// forwarder, which filed one world under every relaying neighbour. Rate
// limiting stays on e.peer.
func world_publish_event(e *Event) {
	if e.origin == "" || e.origin == net_id {
		return // own announcements come back around the flood (the local row is already authoritative); "" is a direct stream, which never carries a world listing
	}
	if !rate_limit_world_publish.allow(e.peer) {
		debug("World dropping publish forwarded by %q: over the rate limit", e.peer)
		return
	}
	id := e.get("world", "")
	name := e.get("name", "")
	address := e.get("address", "")
	version := e.get("version", "")
	services := e.get("services", "")
	if _, ok := world_validate(id, name, address, version, services); !ok {
		debug("World dropping invalid publish from %q", e.origin)
		return
	}
	world_store(e.origin, id, name, address, atoi(version, 0), services)
}

// admin_worlds serves the whole table over the admin socket for mochictl.
func admin_worlds(c *gin.Context) {
	db := db_open("db/world.db")
	rows, err := db.rows("select * from worlds order by peer, world")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()}) // i18n-ok: admin socket, peer-authenticated by uid; the operator wants the raw cause
		return
	}
	c.JSON(http.StatusOK, gin.H{"worlds": rows, "peer": net_id})
}

// mochi.world.list(service) -> [{peer, world, name, address, version, players, seen}, ...]:
// Live listings hosting one service, this host's own included. Display name
// resolution (per-service name over world name) happens here so every app
// renders the same answer; ordering is the consumer's job.
var api_world = sls.FromStringDict(sl.String("mochi.world"), sl.StringDict{
	"list": sl.NewBuiltin("mochi.world.list", api_world_list),
})

func api_world_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <service: string>")
	}
	service, ok := sl.AsString(args[0])
	if !ok || !valid(service, "constant") {
		return sl_error(fn, "invalid service %q", service)
	}

	db := db_open("db/world.db")
	rows, err := db.rows("select * from worlds where seen >= ? order by peer, world", now()-world_seen_expiry)
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}

	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		var list []world_service
		text, _ := row["services"].(string)
		if json.Unmarshal([]byte(text), &list) != nil {
			continue
		}
		for _, s := range list {
			if s.Service != service {
				continue
			}
			name, _ := row["name"].(string)
			if s.Name != "" {
				name = s.Name
			}
			out = append(out, map[string]any{
				"peer":    row["peer"],
				"world":   row["world"],
				"name":    name,
				"address": row["address"],
				"version": row["version"],
				"players": s.Players,
				"seen":    row["seen"],
			})
		}
	}
	return sl_encode(out), nil
}

// Mochi server: World server listings
//
// A mochi-world server running on this machine pushes its status — name,
// address, per-service player counts — to this server over a local socket
// (world_unix.go / world_windows.go), and this server announces it to the
// network over pubsub. A listing is an attribute of the PEER, not of any
// user or entity: GossipSub's StrictSign already authenticates the sending
// peer, so announcements need no entity keys and no extra signatures, and
// there is no "which user owns the listing" question at all.
//
// Rows are keyed (peer, world id) and age out when refresh stops: the world
// pushes on change and on a 10-15 minute idle floor, so a listing that has
// not been refreshed for three floors (45 minutes) is gone, not idle. Only
// the latest state matters — there is no log, no replay, and no repair
// stream; the floor republish IS the repair.
//
// Deliberately NOT app JWTs for the push: an app token is signed with the
// SESSION's secret (auth_create_app_token), so it dies silently on logout,
// session expiry, or a sessions.db wipe — all designed-for operations — and
// it authenticates a whole user to a whole app, far more than a status push
// should hold. The local socket's group permission is the entire credential.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"encoding/json"
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

	// world_ids_most bounds distinct worlds one peer may hold rows for. The
	// per-world debounce below is keyed on the id, so it constrains one world's
	// cadence and does nothing against a caller whose id varies - a world server
	// deriving its id from a match or a restart counter rather than from its own
	// identity writes a fresh row every push. replace into keeps a repeated id
	// free, so only new ids grow the table; past the cap the least recently seen
	// row is evicted rather than the push refused, since a legitimate world must
	// not be lost because a buggy neighbour filled the table.
	world_ids_most = 100
)

// world_service is one hosted game's slice of an announcement.
type world_service struct {
	Service string `json:"service"`
	Players int64  `json:"players"`
	Name    string `json:"name,omitempty"` // optional per-service display name; the world name serves otherwise
}

// world_recent debounces outbound gossip per world id: an unchanged push
// inside the minimum interval stores locally but does not flood. Swept by
// world_manager on the table's own expiry, so debounce state cannot outlive the
// listing it debounces - nothing deleted from it before, and an id seen once
// stayed for the life of the process.
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
// pushing, on the same expiry as the rows they belong to. Without it the map
// only ever grew: the table ages a silent world out after world_seen_expiry
// while its entry stayed for the life of the process, so the two disagreed
// about which worlds exist.
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

// world_address_valid accepts the shapes a world server is actually reachable
// at - an authority (host, or host:port) or an explicit http/https URL - and
// refuses every other scheme.
//
// valid(address, "url") is a charset check, and its class contains ":" and the
// percent sign, so "javascript:alert%281%29" passes it: a browser decodes the
// escapes, and the listing is gossiped between servers and rendered on every
// join page that shows a server list. The air client happens to run addresses
// through normalize_server, which forces an http/https prefix onto anything
// without one, but core hands this string to any app through mochi.world.list
// and cannot assume each one does that.
//
// The "url" case in valid() is left alone: mochi.text.valid exposes it to
// apps as a general URL check, so narrowing it there would change what every
// app's own validation means. This is the world's rule, so it lives here.
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
	if !valid(id, "id") || !valid(name, "line") || len([]rune(name)) > world_name_most {
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
		if s.Name != "" && (!valid(s.Name, "line") || len([]rune(s.Name)) > world_name_most) {
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

// world_register_routes wires the world socket's engine: ONLY these routes.
// The world socket's permissions are looser than the admin socket's (a
// service group rather than the operator), so it must never serve the admin
// engine — that would hand stop/restart and the pprof suite to every group
// member.
// world_body_maximum bounds a status push. Not a security boundary: this
// socket is a local UDS behind both a 0660 group and an SO_PEERCRED check, so
// the caller is software the administrator installed on this machine and put in
// the mochi-world group - and it can already open unbounded connections here,
// which is cheaper than a large body. This is insurance against a BUGGY world
// server: ShouldBindJSON reads to completion before world_validate runs, so
// without a cap a runaway payload OOMs the server and the operator gets a dead
// process with no explanation. With one they get a 413 naming the caller.
//
// A push is a world id, name, address, version and a short services list, so
// 64KB is orders of magnitude above the real payload.
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

// rate_limit_world_gossip bounds what this server floods OUTWARD, across all
// worlds. world_publish_minimum is per id, so it paces one world and is no
// constraint at all on a caller whose id varies: every fresh id has
// published == 0, which makes the interval check trivially true and floods the
// mesh on the spot. Matched to the inbound limiter above, which is what a peer
// receiving this traffic will apply to it anyway.
var rate_limit_world_gossip = &rate_limiter{
	entries: make(map[string]*rate_limit_entry),
	limit:   30,
	window:  60,
}

// world_publish_event stores a listing announced by another peer. The peer
// identity comes from the pubsub layer (StrictSign), never from content —
// and it is e.origin, the signature-verified ORIGINATOR, not e.peer, the
// last-hop mesh neighbour that forwarded the message. A listing is a claim
// about its originator: keying rows on the forwarder filed one world under
// every neighbour that relayed it, so a single server showed up twice in the
// join list until the stale copy aged out. Rate limiting stays on e.peer,
// the identity the transport actually delivers from.
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

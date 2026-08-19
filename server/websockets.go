// Mochi server: Websockets interface
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"github.com/gin-gonic/gin"
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
	"net/url"
	"nhooyr.io/websocket"
	"strings"
	"sync"
)

var (
	api_websocket = sls.FromStringDict(sl.String("mochi.websocket"), sl.StringDict{
		"write": sl.NewBuiltin("mochi.websocket.write", sl_websocket_write),
	})
	websockets        = map[string]map[string]map[string]*websocket_client{}
	websockets_lock   sync.RWMutex
	websocket_context = context.Background()
)

// websocket_client is one live connection and the app that opened it.
//
// The registry is keyed (user, key, connection id) and the key is whatever the
// client passed in the query string, so it never identified an app. Most keys
// are entity fingerprints, which mochi.entity.owned hands to any app ungated,
// and some are bare literals like "notifications" - so without the app here,
// mochi.websocket.write in one app reached sockets belonging to another. The
// app comes from the JWT the connection already verifies; it cannot be claimed
// by the client, unlike the key.
type websocket_client struct {
	ws  *websocket.Conn
	app string
}

// websockets_maximum bounds how many connections one user may hold at once.
// Each is a goroutine parked in ws.Read plus a file descriptor and three map
// entries, held until the client goes away, and nothing else limited them.
//
// Set well above real use rather than tightly: a frontend opens one socket per
// view it is watching, and the whole app tree addresses four keys
// (notifications, staff-events, market-thread-<id>, and entity fingerprints).
const websockets_maximum = 32

// websockets_held counts a user's live connections across every key.
//
// Read before the upgrade so a refusal is an HTTP status rather than a socket
// that opens and closes, which means simultaneous connects can each see the
// same count and a burst can land a little over the cap. That is the right
// trade for a resource bound: the point is that the number cannot grow without
// limit, not that it is never momentarily 33.
func websockets_held(u *User) int {
	websockets_lock.RLock()
	defer websockets_lock.RUnlock()
	held := 0
	for _, connections := range websockets[u.UID] {
		held += len(connections)
	}
	return held
}

func websocket_connection(c *gin.Context) {
	u := web_auth(c)
	token_auth := false
	// The app this socket belongs to, from its JWT. A cookie-authenticated
	// connection has none: it then receives core's own sends but no app's,
	// because there is nothing to say which app's sends it should get.
	// Frontends always hold an app token - the shell supplies one and
	// standalone mode fetches its own - so this is the unusual path.
	app := ""
	if u == nil {
		// Check Authorization header (Bearer token)
		auth_header := c.GetHeader("Authorization")
		if strings.HasPrefix(auth_header, "Bearer ") {
			token := strings.TrimPrefix(auth_header, "Bearer ")
			user_id, token_app, err := jwt_verify(token)
			if err == nil && user_id != "" {
				if user := user_by_uid(user_id); user != nil {
					u = user
					app = token_app
					token_auth = true
				}
			}
		}

		// Check token query parameter (for WebSocket from iframes that can't set headers)
		if u == nil {
			if token := c.Query("token"); token != "" {
				user_id, token_app, err := jwt_verify(token)
				if err == nil && user_id != "" {
					if user := user_by_uid(user_id); user != nil {
						u = user
						app = token_app
						token_auth = true
					}
				}
			}
		}

		if u == nil {
			// A bare return here is a 200 with an empty body: the handshake
			// still fails, since there is no 101 and no Upgrade header, but
			// the reason never reaches the caller and the access log records
			// an authentication failure as a success. The origin check below
			// already answers with a status; this one did not.
			c.Status(401)
			return
		}
	}

	// Validate origin matches request host to prevent cross-origin WebSocket hijacking.
	// Skip origin check for token-authenticated connections (JWT proves authorization,
	// and sandboxed iframes send "null" origin).
	if !token_auth {
		origin := c.GetHeader("Origin")
		if origin != "" {
			if parsed, err := url.Parse(origin); err != nil || parsed.Host != c.Request.Host {
				c.Status(403)
				return
			}
		}
	}

	// The key names the channel this socket listens on. mochi.websocket.write
	// validates it as a constant, and the two ends have to agree: taking
	// whatever the query string holds let a client occupy keys no app could
	// ever address, and made an arbitrarily long string a map key for the life
	// of the connection.
	key := c.Query("key")
	if !valid(key, "constant") {
		c.Status(400)
		return
	}

	if websockets_held(u) >= websockets_maximum {
		c.Status(429)
		return
	}

	ws, err := websocket.Accept(c.Writer, c.Request, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	id := uid()
	defer websocket_terminate(ws, u, key, id)

	websockets_lock.Lock()
	_, found := websockets[u.UID]
	if !found {
		websockets[u.UID] = map[string]map[string]*websocket_client{}
	}
	_, found = websockets[u.UID][key]
	if !found {
		websockets[u.UID][key] = map[string]*websocket_client{}
	}
	websockets[u.UID][key][id] = &websocket_client{ws: ws, app: app}
	websockets_lock.Unlock()
	// debug("Websocket connection user %d, key %q, id %q", u.UID, key, id)

	for {
		t, j, err := ws.Read(websocket_context)
		if err != nil {
			// The deferred terminate above runs on return; calling it here as
			// well closed and deleted the same connection twice on every
			// ordinary disconnect.
			return
		}
		if t != websocket.MessageText {
			continue
		}

		info("Websocket received message %q; ignoring", j)
	}
}

// websockets_send delivers content to a user's connections on this key.
//
// app scopes the delivery: a non-empty value reaches only the connections that
// app opened, which is what stops one app writing into another's socket. Core's
// own sends pass "" and reach every connection on the key, because core is not
// the boundary being enforced and its callers predate any app binding.
func websockets_send(u *User, app string, key string, content any) {
	// debug("Websocket sending to user %d, key %q: %+v", u.UID, key, content)
	j := ""

	// The connection is carried through to the termination pass rather than
	// looked up again. The write failing is the same event that makes ws.Read
	// fail on this connection's own reader goroutine, which terminates it and
	// deletes the entry - so a second lookup races that deletion and yields a
	// nil, and CloseNow dereferences its receiver. Nothing exotic is needed to
	// reach it: an ordinary disconnect drives both paths at once.
	//
	// Termination cannot happen under the read lock, since it takes the write
	// lock, which is why this is a second pass at all. That only requires
	// deferring the CLOSE, not re-fetching a reference already in hand.
	type dead struct {
		id string
		ws *websocket.Conn
	}
	var failed []dead

	websockets_lock.RLock()
	for id, client := range websocket_targets(u, app, key) {
		if j == "" {
			j = json_encode(content)
		}
		err := client.ws.Write(websocket_context, websocket.MessageText, []byte(j))
		if err != nil {
			failed = append(failed, dead{id: id, ws: client.ws})
		}
	}
	websockets_lock.RUnlock()

	for _, entry := range failed {
		websocket_terminate(entry.ws, u, key, entry.id)
	}
}

// websocket_targets returns the connections a send for this app must reach: its
// own when app is set, every connection on the key when it is empty (core).
// Caller holds websockets_lock.
func websocket_targets(u *User, app string, key string) map[string]*websocket_client {
	targets := map[string]*websocket_client{}
	for id, client := range websockets[u.UID][key] {
		if app != "" && client.app != app {
			continue
		}
		targets[id] = client
	}
	return targets
}

// websocket_terminate closes a connection and removes it from the registry.
// Safe to call more than once for the same id, and safe with a nil connection:
// the map deletes below are no-ops on an entry that has already gone, and a
// caller that no longer holds the connection should still be able to clear the
// registry rather than dereference nothing.
func websocket_terminate(ws *websocket.Conn, u *User, key string, id string) {
	if ws != nil {
		ws.CloseNow()
	}
	websockets_lock.Lock()
	delete(websockets[u.UID][key], id)

	if len(websockets[u.UID][key]) == 0 {
		delete(websockets[u.UID], key)
	}

	if len(websockets[u.UID]) == 0 {
		delete(websockets, u.UID)
	}
	websockets_lock.Unlock()
}

// mochi.websocket.write(key, content) -> None: Send content to connected WebSocket clients
func sl_websocket_write(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return sl_error(fn, "syntax: <key: string>, <content: any>")
	}

	key, ok := sl.AsString(args[0])
	if !ok || !valid(key, "constant") {
		return sl_error(fn, "invalid key %q", key)
	}

	user := principal_caller(t)
	if user == nil {
		return sl_error(fn, "no user")
	}

	// Scoped to the calling app. Without this the key alone decided the
	// target, and a key is not a secret: most are entity fingerprints that
	// mochi.entity.owned returns to any app, and the rest are literals.
	a, ok := t.Local("app").(*App)
	if !ok || a == nil || a.id == "" {
		return sl_error(fn, "no app")
	}

	websockets_send(user, a.id, key, sl_decode(args[1]))
	return sl.None, nil
}

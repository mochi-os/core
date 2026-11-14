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
	"time"
)

var (
	api_websocket = sls.FromStringDict(sl.String("mochi.websocket"), sl.StringDict{
		"write": sl.NewBuiltin("mochi.websocket.write", sl_websocket_write),
	})
	websockets        = map[string]map[string]map[string]*websocket_client{}
	websockets_lock   sync.RWMutex
	websocket_context = context.Background()
)

// websocket_write_timeout bounds one frame write. A browser that stops reading
// leaves the socket in TCP zero-window, where an unbounded write never returns.
// A var so a test can drive the expiry deterministically.
var websocket_write_timeout = 10 * time.Second

// websocket_queue bounds the frames waiting for one connection's writer.
const websocket_queue = 64

// websocket_queue_timeout bounds how long a send waits, across all its
// connections together, for room in full queues. Each frame a writer finishes
// frees a place, so a connection that is only behind - a Starlark loop of
// writes outruns any writer - frees one within microseconds and the send goes
// on. One whose writer finishes no frame in this time is closed rather than
// waited for or skipped: it has stopped reading, or its link cannot carry what
// is queued for it this fast, and its client reconnects and fetches what it
// missed, where a skipped frame would leave a count or a list wrong until the
// next one. A var so a test can shorten it.
var websocket_queue_timeout = 1 * time.Second

// websocket_client is one live connection and the app that opened it. The
// registry key is whatever the client passed in the query string and never
// identified an app; the app comes from the JWT, which the client cannot claim.
//
// Its frames go through queue to its own writer goroutine: one write per
// connection at a time, in the order sent, and a connection that stops reading
// holds its own writer rather than the send. Cancelling context ends the
// writer, and the writer ends the connection.
type websocket_client struct {
	ws      *websocket.Conn
	app     string
	queue   chan []byte
	context context.Context
	cancel  context.CancelFunc
}

// websockets_maximum bounds how many connections one user may hold at once;
// each is three parked goroutines (its reader, its writer and the websocket
// library's timeout loop), a file descriptor, a queue of up to websocket_queue
// frames and three map entries. Set well above
// real use - a frontend opens one socket per view it is watching.
const websockets_maximum = 32

// websockets_held counts a user's live connections across every key. Read
// before the upgrade so a refusal is an HTTP status, which means simultaneous
// connects can each see the same count and a burst can land a little over the
// cap.
func websockets_held(u *User) int {
	websockets_lock.RLock()
	defer websockets_lock.RUnlock()
	held := 0
	for _, connections := range websockets[u.UID] {
		held += len(connections)
	}
	return held
}

// websocket_protocol_token prefixes the subprotocol a browser presents its app
// token in. A browser sets no headers on a handshake, so the token used to have
// to travel in the query string, where it lands in every access log and in any
// copied URL; the subprotocol is the one header it will send.
const websocket_protocol_token = "mochi.token."

// websocket_protocol_offered returns the token subprotocol the client offered,
// whole, so the accept can echo the exact string back - a browser fails the
// connection unless the server names a protocol it asked for.
func websocket_protocol_offered(c *gin.Context) string {
	for _, header := range c.Request.Header.Values("Sec-WebSocket-Protocol") {
		for _, offered := range strings.Split(header, ",") {
			offered = strings.TrimSpace(offered)
			if strings.HasPrefix(offered, websocket_protocol_token) && offered != websocket_protocol_token {
				return offered
			}
		}
	}
	return ""
}

// websocket_authenticate resolves who a handshake belongs to and which app the
// connection is tagged with. A valid token names the app and outranks the
// session cookie for tagging: the browser sends the cookie on every same-origin
// handshake, so reading the cookie first left a top-window page's socket tagged
// with no app however it presented its token - and app-scoped delivery cannot
// reach an untagged connection. A token for a different user than the cookie's
// is ignored rather than adopted.
func websocket_authenticate(c *gin.Context) (u *User, app string, token_auth bool) {
	u = web_auth(c)

	// Bearer header first, then the subprotocol a browser can set, then the
	// query parameter older clients still send.
	token := ""
	if header := c.GetHeader("Authorization"); strings.HasPrefix(header, "Bearer ") {
		token = strings.TrimPrefix(header, "Bearer ")
	}
	if token == "" {
		if offered := websocket_protocol_offered(c); offered != "" {
			token = strings.TrimPrefix(offered, websocket_protocol_token)
		}
	}
	if token == "" {
		token = c.Query("token")
	}
	if token == "" {
		return u, "", false
	}

	claims, err := jwt_verify_claims(token)
	if err != nil || claims.User == "" {
		return u, "", false
	}
	// An asset token reads a URL's worth of bytes; a socket is a subscription to
	// everything an app sends this user, so it is not what that credential buys.
	if claims.Purpose != "" {
		return u, "", false
	}
	user_id, token_app := claims.User, claims.App
	holder := user_by_uid(user_id)
	if holder == nil || (u != nil && holder.UID != u.UID) {
		return u, "", false
	}
	return holder, token_app, true
}

func websocket_connection(c *gin.Context) {
	u, app, token_auth := websocket_authenticate(c)
	if u == nil {
		// A bare return here is a 200 with an empty body: the handshake still fails,
		// but the caller learns nothing and the access log records the auth failure
		// as a success.
		c.Status(401)
		return
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

	// mochi.websocket.write validates the key as a constant and both ends must
	// agree: an unvalidated query string let a client occupy keys no app could
	// address, and made an arbitrary string a map key for the connection's life.
	key := c.Query("key")
	if !valid(key, "constant") {
		c.Status(400)
		return
	}

	if websockets_held(u) >= websockets_maximum {
		c.Status(429)
		return
	}

	accept := &websocket.AcceptOptions{InsecureSkipVerify: true}
	// Echo the token subprotocol back: the browser closes the connection at once
	// if the server selects none of the protocols it offered.
	if offered := websocket_protocol_offered(c); offered != "" {
		accept.Subprotocols = []string{offered}
	}
	ws, err := websocket.Accept(c.Writer, c.Request, accept)
	if err != nil {
		return
	}
	id := uid()
	defer websocket_terminate(ws, u, key, id)
	websocket_open(u, key, id, ws, app, web_cookie_get(c, "session", ""))

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

		// Anything the client sends is activity on the connection (presence.go).
		presence_touch(u.UID, id)
		info("Websocket received message %q; ignoring", j)
	}
}

// websocket_open puts a connection in the registry and records the user
// connected (presence.go), under the login session the browser presented,
// where one did.
func websocket_open(u *User, key string, id string, ws *websocket.Conn, app string, session string) {
	websocket_add(u, key, id, ws, app)
	presence_connect(u.UID, id, session, "web", "")
}

// websocket_add puts a connection in the registry and starts its writer.
func websocket_add(u *User, key string, id string, ws *websocket.Conn, app string) {
	client := &websocket_client{ws: ws, app: app, queue: make(chan []byte, websocket_queue)}
	client.context, client.cancel = context.WithCancel(websocket_context)
	websockets_lock.Lock()
	_, found := websockets[u.UID]
	if !found {
		websockets[u.UID] = map[string]map[string]*websocket_client{}
	}
	_, found = websockets[u.UID][key]
	if !found {
		websockets[u.UID][key] = map[string]*websocket_client{}
	}
	websockets[u.UID][key][id] = client
	websockets_lock.Unlock()
	// Guarded: a panic here would otherwise take the process down, where the
	// write it does used to run on a caller that had its own recovery. The
	// writer's deferred terminate still runs.
	go guard("websocket writer", nil, func() { websocket_writer(client, u, key, id) })
}

// websocket_writer writes a connection's frames in the order they were sent,
// and ends the connection when a write fails or the connection is cancelled:
// by websocket_terminate, or by a send that found no room in its queue. It never
// holds websockets_lock while writing, so a connection that has stopped
// reading stalls nothing but itself.
func websocket_writer(client *websocket_client, u *User, key string, id string) {
	defer websocket_terminate(client.ws, u, key, id)
	for {
		select {
		case <-client.context.Done():
			return
		case frame := <-client.queue:
			// Bounded per write: a client in TCP zero-window otherwise parks
			// this goroutine for the life of the connection. Taken from the
			// connection's context, so cancelling it ends a write already
			// blocked.
			deadline, cancel := context.WithTimeout(client.context, websocket_write_timeout)
			err := client.ws.Write(deadline, websocket.MessageText, frame)
			cancel()
			if err != nil {
				return
			}
		}
	}
}

// websockets_send delivers content to a user's connections on this key. A
// non-empty app reaches only that app's connections; core passes "" and reaches
// every connection on the key.
//
// It only queues: each connection's writer does the writing, so the caller -
// an action, an event handler, a commit hook - waits on no connection's writes,
// only for room in a queue that is full (websocket_queue_timeout).
func websockets_send(u *User, app string, key string, content any) {
	var targets []*websocket_client
	websockets_lock.RLock()
	for _, client := range websocket_targets(u, app, key) {
		targets = append(targets, client)
	}
	websockets_lock.RUnlock()
	if len(targets) == 0 {
		return
	}
	frame := []byte(json_encode(content))

	// Outside the lock: a full queue waits for room, and holding the read lock
	// across that would hold the next websocket_terminate, and every send
	// behind it, since Go's RWMutex admits no new reader once a writer waits.
	websocket_enqueue(targets, frame)
}

// websocket_enqueue queues a frame for each connection's writer. Every
// connection with room has it at once; then the send waits for room in the
// full ones against one shared websocket_queue_timeout, so several stalled
// connections cost it that time once, not once each, and cancels each that
// has none by then. A cancelled connection is skipped at once.
func websocket_enqueue(targets []*websocket_client, frame []byte) {
	var full []*websocket_client
	for _, client := range targets {
		select {
		case client.queue <- frame:
		default:
			full = append(full, client)
		}
	}
	if len(full) == 0 {
		return
	}
	wait, stop := context.WithTimeout(websocket_context, websocket_queue_timeout)
	defer stop()
	for _, client := range full {
		select {
		case client.queue <- frame:
		case <-client.context.Done():
		case <-wait.Done():
			select {
			case client.queue <- frame:
			default:
				// Its writer finished no frame in that time. Cancelling
				// never blocks, and the writer closes the connection.
				client.cancel()
			}
		}
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

// websocket_terminate closes a connection, removes it from the registry and
// stops its writer. Safe to call twice for the same id: the reader and the
// writer both call it on the way out. The entry is removed only while it is
// still this connection's, so a late call from either never removes another
// connection registered under the same id.
func websocket_terminate(ws *websocket.Conn, u *User, key string, id string) {
	if ws != nil {
		ws.CloseNow()
	}
	websockets_lock.Lock()
	client := websockets[u.UID][key][id]
	if client != nil && client.ws != ws {
		client = nil
	}
	if client != nil {
		delete(websockets[u.UID][key], id)
	}

	if len(websockets[u.UID][key]) == 0 {
		delete(websockets[u.UID], key)
	}

	if len(websockets[u.UID]) == 0 {
		delete(websockets, u.UID)
	}
	websockets_lock.Unlock()
	if client != nil {
		client.cancel()
	}
	presence_disconnect(u.UID, id)
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

	content := sl_decode(args[1])
	websockets_send(user, a.id, key, content)
	mastodon_heard(user, a, key, content)
	return sl.None, nil
}

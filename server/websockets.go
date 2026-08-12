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
	websockets        = map[string]map[string]map[string]*websocket.Conn{}
	websockets_lock   sync.RWMutex
	websocket_context = context.Background()
)

func websocket_connection(c *gin.Context) {
	u := web_auth(c)
	token_auth := false
	if u == nil {
		// Check Authorization header (Bearer token)
		auth_header := c.GetHeader("Authorization")
		if strings.HasPrefix(auth_header, "Bearer ") {
			token := strings.TrimPrefix(auth_header, "Bearer ")
			user_id, _, err := jwt_verify(token)
			if err == nil && user_id != "" {
				if user := user_by_uid(user_id); user != nil {
					u = user
					token_auth = true
				}
			}
		}

		// Check token query parameter (for WebSocket from iframes that can't set headers)
		if u == nil {
			if token := c.Query("token"); token != "" {
				user_id, _, err := jwt_verify(token)
				if err == nil && user_id != "" {
					if user := user_by_uid(user_id); user != nil {
						u = user
						token_auth = true
					}
				}
			}
		}

		if u == nil {
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

	ws, err := websocket.Accept(c.Writer, c.Request, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	key := c.Query("key")
	id := uid()
	defer websocket_terminate(ws, u, key, id)

	websockets_lock.Lock()
	_, found := websockets[u.UID]
	if !found {
		websockets[u.UID] = map[string]map[string]*websocket.Conn{}
	}
	_, found = websockets[u.UID][key]
	if !found {
		websockets[u.UID][key] = map[string]*websocket.Conn{}
	}
	websockets[u.UID][key][id] = ws
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

func websockets_send(u *User, key string, content any) {
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
	for id, ws := range websockets[u.UID][key] {
		if j == "" {
			j = json_encode(content)
		}
		err := ws.Write(websocket_context, websocket.MessageText, []byte(j))
		if err != nil {
			failed = append(failed, dead{id: id, ws: ws})
		}
	}
	websockets_lock.RUnlock()

	for _, entry := range failed {
		websocket_terminate(entry.ws, u, key, entry.id)
	}
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

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	websockets_send(user, key, sl_decode(args[1]))
	return sl.None, nil
}

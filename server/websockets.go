// Mochi server: Wesockets interface
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"context"
	"github.com/gin-gonic/gin"
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
	"nhooyr.io/websocket"
	"strings"
	"sync"
)

var (
	api_websocket = sls.FromStringDict(sl.String("mochi.websocket"), sl.StringDict{
		"write": sl.NewBuiltin("mochi.websocket.write", sl_websocket_write),
	})
	websockets        = map[int]map[string]map[string]*websocket.Conn{}
	websockets_lock   sync.RWMutex
	websocket_context = context.Background()
)

func websocket_connection(c *gin.Context) {
	u := web_auth(c)
	if u == nil {
		// Check Authorization header (Bearer token)
		auth_header := c.GetHeader("Authorization")
		if strings.HasPrefix(auth_header, "Bearer ") {
			token := strings.TrimPrefix(auth_header, "Bearer ")
			user_id, err := jwt_verify(token)
			if err == nil && user_id > 0 {
				user := user_by_id(user_id)
				if user != nil {
					u = user
					debug("API JWT token accepted for user %d", u.ID)
				}
			}
		}

		if u == nil {
			return
		}
	}

	ws, err := websocket.Accept(c.Writer, c.Request, nil)
	if err != nil {
		return
	}
	key := c.Query("key")
	id := uid()
	defer websocket_terminate(ws, u, key, id)

	websockets_lock.Lock()
	_, found := websockets[u.ID]
	if !found {
		websockets[u.ID] = map[string]map[string]*websocket.Conn{}
	}
	_, found = websockets[u.ID][key]
	if !found {
		websockets[u.ID][key] = map[string]*websocket.Conn{}
	}
	websockets[u.ID][key][id] = ws
	websockets_lock.Unlock()
	//debug("Websocket connection user %d, key %q, id %q", u.ID, key, id)

	for {
		t, j, err := ws.Read(websocket_context)
		if err != nil {
			websocket_terminate(ws, u, key, id)
			return
		}
		if t != websocket.MessageText {
			continue
		}

		info("Websocket received message %q; ignoring", j)
	}
}

func websockets_send(u *User, key string, content any) {
	//debug("Websocket sending to user %d, key %q: %+v", u.ID, key, content)
	j := ""
	var failed []string

	websockets_lock.RLock()
	for id, ws := range websockets[u.ID][key] {
		if j == "" {
			j = json_encode(content)
		}
		err := ws.Write(websocket_context, websocket.MessageText, []byte(j))
		if err != nil {
			failed = append(failed, id)
		}
	}
	websockets_lock.RUnlock()

	for _, id := range failed {
		websockets_lock.RLock()
		ws := websockets[u.ID][key][id]
		websockets_lock.RUnlock()
		websocket_terminate(ws, u, key, id)
	}
}

func websocket_terminate(ws *websocket.Conn, u *User, key string, id string) {
	ws.CloseNow()
	websockets_lock.Lock()
	delete(websockets[u.ID][key], id)

	if len(websockets[u.ID][key]) == 0 {
		delete(websockets[u.ID], key)
	}

	if len(websockets[u.ID]) == 0 {
		delete(websockets, u.ID)
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

// Mochi server: Wesockets interface
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"context"
	"github.com/gin-gonic/gin"
	"nhooyr.io/websocket"
)

var (
	websockets        = map[int]map[string]map[string]*websocket.Conn{}
	websocket_context = context.Background()
)

func websocket_connection(c *gin.Context) {
	u := web_auth(c)
	if u == nil {
		return
	}

	ws, err := websocket.Accept(c.Writer, c.Request, nil)
	if err != nil {
		return
	}
	key := c.Query("key")
	id := uid()
	defer websocket_terminate(ws, u, key, id)

	_, found := websockets[u.ID]
	if !found {
		websockets[u.ID] = map[string]map[string]*websocket.Conn{}
	}
	_, found = websockets[u.ID][key]
	if !found {
		websockets[u.ID][key] = map[string]*websocket.Conn{}
	}
	websockets[u.ID][key][id] = ws
	//debug("Websocket connection user %d, key '%s', id '%s'", u.ID, key, id)

	for {
		t, j, err := ws.Read(websocket_context)
		if err != nil {
			websocket_terminate(ws, u, key, id)
			return
		}
		if t != websocket.MessageText {
			continue
		}

		info("Websocket received message; ignoring: %s", j)
	}
}

func websockets_send(u *User, key string, content any) {
	//debug("Websocket sending to user %d, key '%s': %+v", u.ID, key, content)
	j := ""

	for id, ws := range websockets[u.ID][key] {
		if j == "" {
			j = json_encode(content)
		}
		err := ws.Write(websocket_context, websocket.MessageText, []byte(j))
		if err != nil {
			websocket_terminate(ws, u, key, id)
		}
	}
}

func websocket_terminate(ws *websocket.Conn, u *User, key string, id string) {
	ws.CloseNow()
	delete(websockets[u.ID][key], id)

	if len(websockets[u.ID][key]) == 0 {
		delete(websockets[u.ID], key)
	}

	if len(websockets[u.ID]) == 0 {
		delete(websockets, u.ID)
	}
}

// Mochi server: Wesockets interface
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"context"
	"github.com/gin-gonic/gin"
	"nhooyr.io/websocket"
)

var (
	websockets        = map[int]map[string]*websocket.Conn{}
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
	id := uid()
	defer websocket_terminate(ws, u, id)

	_, found := websockets[u.ID]
	if !found {
		websockets[u.ID] = map[string]*websocket.Conn{}
	}
	websockets[u.ID][id] = ws

	for {
		t, j, err := ws.Read(websocket_context)
		if err != nil {
			websocket_terminate(ws, u, id)
			return
		}
		if t != websocket.MessageText {
			continue
		}

		info("Websocket received message; ignoring: %s", j)
	}
}

//TODO Add object
func websockets_send(u *User, app string, content any) {
	j := ""

	for id, ws := range websockets[u.ID] {
		if j == "" {
			j = json_encode(map[string]any{"app": app, "content": content})
		}
		debug("Websocket sending '%s'", j)
		err := ws.Write(websocket_context, websocket.MessageText, []byte(j))
		if err != nil {
			websocket_terminate(ws, u, id)
		}
	}
}

func websocket_terminate(ws *websocket.Conn, u *User, id string) {
	ws.CloseNow()
	delete(websockets[u.ID], id)
}

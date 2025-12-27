// Mochi server: Remote entity communication
// Copyright Alistair Cunningham 2025

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

var api_remote = sls.FromStringDict(sl.String("mochi.remote"), sl.StringDict{
	"peer":    sl.NewBuiltin("mochi.remote.peer", api_remote_peer),
	"request": sl.NewBuiltin("mochi.remote.request", api_remote_request),
	"stream":  sl.NewBuiltin("mochi.remote.stream", api_remote_stream),
	"ping":    sl.NewBuiltin("mochi.remote.ping", api_remote_ping),
})

// mochi.remote.peer(url) -> string|None: Resolve a server URL to a peer ID, or None on failure
func api_remote_peer(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <url: string>")
	}

	url, ok := sl.AsString(args[0])
	if !ok || url == "" {
		return sl.None, nil
	}

	peer, err := peer_connect_url(url)
	if err != nil {
		return sl.None, nil
	}

	return sl.String(peer), nil
}

// Connect to a peer by server URL, returning the peer ID
func peer_connect_url(url string) (string, error) {
	// Normalize URL: add https:// if no scheme present
	if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
		url = "https://" + url
	}

	// Fetch P2P info from the server
	infoUrl := strings.TrimSuffix(url, "/") + "/_/p2p/info"
	resp, err := url_request("GET", infoUrl, nil, nil, nil)
	if err != nil {
		return "", fmt.Errorf("failed to fetch p2p info: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return "", fmt.Errorf("server returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response: %v", err)
	}

	// Parse JSON response
	var info struct {
		Peer      string   `json:"peer"`
		Addresses []string `json:"addresses"`
	}
	if err := json.Unmarshal(body, &info); err != nil {
		return "", fmt.Errorf("failed to parse p2p info: %v", err)
	}

	if info.Peer == "" || len(info.Addresses) == 0 {
		return "", fmt.Errorf("invalid p2p info: missing peer or addresses")
	}

	// Add peer and connect
	peer_add_known(info.Peer, info.Addresses)
	if !peer_connect(info.Peer) {
		return "", fmt.Errorf("failed to connect to peer %s", info.Peer)
	}

	return info.Peer, nil
}

// Connect to a remote entity, returning the peer ID
// Uses peer if provided, otherwise looks up in directory
// If peer is an entity ID, resolve it to a peer ID first
func remote_connect(entity_id string, peer string) (string, error) {
	if peer != "" {
		// If peer is an entity ID, resolve it first
		if valid(peer, "entity") {
			peer = entity_peer(peer)
			if peer == "" {
				return "", fmt.Errorf("entity not found in directory")
			}
		}

		// Peer ID provided, ensure we're connected
		if !peer_connect(peer) {
			return "", fmt.Errorf("failed to connect to peer %s", peer)
		}
		return peer, nil
	}

	// Look up entity in directory
	peer = entity_peer(entity_id)
	if peer == "" {
		return "", fmt.Errorf("entity not found in directory")
	}

	return peer, nil
}

// mochi.remote.request(entity_id, service, event, payload, peer) -> dict: Make a request to a remote entity
func api_remote_request(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	// Parse arguments
	var entity_id, service, event, peer string
	var payload sl.Value

	if len(args) < 4 {
		return sl_error(fn, "syntax: <entity_id: string>, <service: string>, <event: string>, <payload: dict>, [peer: string]")
	}

	var ok bool
	entity_id, ok = sl.AsString(args[0])
	if !ok || !valid(entity_id, "entity") {
		return sl_error(fn, "invalid entity_id")
	}

	service, ok = sl.AsString(args[1])
	if !ok || !valid(service, "constant") {
		return sl_error(fn, "invalid service")
	}

	event, ok = sl.AsString(args[2])
	if !ok || !valid(event, "constant") {
		return sl_error(fn, "invalid event")
	}

	payload = args[3]

	if len(args) > 4 {
		peer, _ = sl.AsString(args[4])
	}

	// Get user and app from context
	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	if user.Identity == nil {
		return sl_error(fn, "user has no identity")
	}

	// Connect to remote
	peer, err := remote_connect(entity_id, peer)
	if err != nil {
		return sl_encode(map[string]any{"error": err.Error(), "code": 502}), nil
	}

	// Create stream
	from := user.Identity.ID
	s, err := stream_to_peer(peer, from, entity_id, service, event)
	if err != nil {
		return sl_encode(map[string]any{"error": err.Error(), "code": 502}), nil
	}
	defer func() {
		if s.reader != nil {
			s.reader.Close()
		}
		if s.writer != nil {
			s.writer.Close()
		}
	}()

	// Send payload
	err = s.write(sl_decode(payload))
	if err != nil {
		return sl_encode(map[string]any{"error": fmt.Sprintf("failed to send: %v", err), "code": 502}), nil
	}

	// Read response
	var response any
	err = s.read(&response)
	if err != nil {
		return sl_encode(map[string]any{"error": fmt.Sprintf("failed to read response: %v", err), "code": 504}), nil
	}

	return sl_encode(response), nil
}

// mochi.remote.stream(entity_id, service, event, payload, peer) -> Stream: Open a stream to a remote entity
func api_remote_stream(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	// Parse arguments
	var entity_id, service, event, peer string
	var payload sl.Value

	if len(args) < 4 {
		return sl_error(fn, "syntax: <entity_id: string>, <service: string>, <event: string>, <payload: dict>, [peer: string]")
	}

	var ok bool
	entity_id, ok = sl.AsString(args[0])
	if !ok || !valid(entity_id, "entity") {
		return sl_error(fn, "invalid entity_id")
	}

	service, ok = sl.AsString(args[1])
	if !ok || !valid(service, "constant") {
		return sl_error(fn, "invalid service")
	}

	event, ok = sl.AsString(args[2])
	if !ok || !valid(event, "constant") {
		return sl_error(fn, "invalid event")
	}

	payload = args[3]

	if len(args) > 4 {
		peer, _ = sl.AsString(args[4])
	}

	// Get user and app from context
	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	if user.Identity == nil {
		return sl_error(fn, "user has no identity")
	}

	// Connect to remote
	peer, err := remote_connect(entity_id, peer)
	if err != nil {
		return sl_error(fn, "%v", err)
	}

	// Create stream
	from := user.Identity.ID
	s, err := stream_to_peer(peer, from, entity_id, service, event)
	if err != nil {
		return sl_error(fn, "%v", err)
	}

	// Send payload
	err = s.write(sl_decode(payload))
	if err != nil {
		if s.reader != nil {
			s.reader.Close()
		}
		if s.writer != nil {
			s.writer.Close()
		}
		return sl_error(fn, "failed to send: %v", err)
	}

	return s, nil
}

// mochi.remote.ping(entity_id, peer) -> dict: Check if a remote entity is reachable
func api_remote_ping(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	// Parse arguments
	var entity_id, peer string

	if len(args) < 1 {
		return sl_error(fn, "syntax: <entity_id: string>, [peer: string]")
	}

	var ok bool
	entity_id, ok = sl.AsString(args[0])
	if !ok || !valid(entity_id, "entity") {
		return sl_error(fn, "invalid entity_id")
	}

	if len(args) > 1 {
		peer, _ = sl.AsString(args[1])
	}

	// Try to connect
	peer, err := remote_connect(entity_id, peer)
	if err != nil {
		return sl_encode(map[string]any{"reachable": false, "error": err.Error()}), nil
	}

	return sl_encode(map[string]any{"reachable": true, "peer": peer}), nil
}

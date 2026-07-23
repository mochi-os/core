// Mochi server: Remote entity communication
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

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

// Connect to a peer by server URL or peer ID, returning the peer ID
func peer_connect_url(url string) (string, error) {
	// Handle p2p/ prefixed peer IDs (e.g. from directory location field)
	if strings.HasPrefix(url, "p2p/") {
		peer := strings.TrimPrefix(url, "p2p/")
		if peer_connect(peer) {
			return peer, nil
		}
		return "", fmt.Errorf("failed to connect to peer %s", peer)
	}

	// Normalize URL: add https:// if no scheme present
	if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
		url = "https://" + url
	}

	// Fetch Net info from the server
	info_url := strings.TrimSuffix(url, "/") + "/_/p2p/info"
	resp, err := url_request(context.Background(), "GET", info_url, nil, nil, nil)
	if err != nil {
		return "", fmt.Errorf("failed to fetch net info: %v", err)
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
		return "", fmt.Errorf("failed to parse net info: %v", err)
	}

	if info.Peer == "" || len(info.Addresses) == 0 {
		return "", fmt.Errorf("invalid net info: missing peer or addresses")
	}

	// Add peer and connect
	peer_add_known(info.Peer, info.Addresses)
	if !peer_connect(info.Peer) {
		return "", fmt.Errorf("failed to connect to peer %s", info.Peer)
	}

	return info.Peer, nil
}

// remote_address_wait bounds how long a synchronous remote request
// waits for the mesh to answer a peers/request about a peer we hold no
// addresses for (first contact: the entity resolved via the directory,
// but we have never exchanged traffic with its server). Record-holding
// relays answer a peers/request on the target's behalf, so a live
// target's addresses normally arrive well inside a second; five
// seconds is headroom for slow relayed paths, matching the hole-punch
// and shutdown-drain bounds.
var remote_address_wait = 5 * time.Second

// remote_reach connects to the first reachable of `candidates` (peer
// ids in failover order). When no candidate connects from stored
// addresses, it broadcasts a peers/request for each and retries as
// answers arrive, all within one shared remote_address_wait budget.
// Without this recovery a synchronous request to a never-seen peer
// fails instantly — peer_connect requires prior discovery — while
// queued events to the same peer self-heal through the queue's retry
// loop. Returns the connected peer id, or "".
func remote_reach(candidates []string) string {
	for _, p := range candidates {
		if peer_connect(p) {
			return p
		}
	}
	requested := false
	for _, p := range candidates {
		if peer_request_addresses(p) {
			requested = true
		}
	}
	// The answer to that request arrives over pubsub, which drops inbound
	// messages once a peer exceeds its budget. Peer control traffic has its
	// own budget (rate_limit_pubsub_control) so an application flood can no
	// longer starve these announcements, but a flood on the control plane
	// itself, or a genuinely absent peer, can still leave us empty-handed.
	// Snapshot the drop counter so a failure below can say whether messages
	// were being discarded while we waited, rather than leaving "unreachable"
	// indistinguishable from a peer that never answered.
	dropped := pubsub_dropped.Load()

	// Bound the wait. remote_address_wait is sized for a fresh request's answer
	// to arrive. When we broadcast one, wait the full window. When every
	// candidate was suppressed because a request already went out this minute,
	// wait only for the remainder of that request's answer window, measured
	// from when it was actually sent: exactly one broadcast happens per minute
	// per target, so once its window has elapsed no answer is coming until the
	// next, and sitting the full remote_address_wait is the pointless delay
	// that turned a rate-limited cold probe into a multi-second hang.
	budget := remote_address_wait
	if !requested {
		freshest := remote_address_wait
		for _, p := range candidates {
			if age := rate_limit_peer_request.since(p); age < freshest {
				freshest = age
			}
		}
		budget = remote_address_wait - freshest
		if budget < 0 {
			budget = 0
		}
	}
	deadline := time.Now().Add(budget)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			debug("Remote gave up waiting %s for addresses for %v (request broadcast: %t, pubsub messages dropped while waiting: %d)",
				budget, candidates, requested, pubsub_dropped.Load()-dropped)
			return ""
		}
		// Wake early when the primary candidate's addresses arrive.
		// The 500ms floor also catches answers for the other
		// candidates and connects completed by concurrent callers,
		// neither of which signals this waiter.
		wait := 500 * time.Millisecond
		if remaining < wait {
			wait = remaining
		}
		peer_await_addresses(candidates[0], wait)
		for _, p := range candidates {
			if peer_connect(p) {
				return p
			}
		}
	}
}

// Connect to a remote entity, returning the peer ID.
//
// If `peer` is explicitly given (libp2p id or entity id), uses that
// after a single resolution. If only `entity_id` is given, tries each
// location from entity_peers_failover in order, returning the first
// peer we can reach. Same multi-host failover policy as stream().
func remote_connect(from string, entity_id string, peer string) (string, error) {
	if peer != "" {
		// If peer is an entity ID, resolve it first (single shot —
		// caller asked for that specific entity's location).
		if valid(peer, "entity") {
			peer = entity_peer(peer)
			if peer == "" {
				return "", fmt.Errorf("entity not found in directory")
			}
		}

		// Peer ID provided, ensure we're connected
		if remote_reach([]string{peer}) == "" {
			return "", fmt.Errorf("failed to connect to peer %s", peer)
		}
		return peer, nil
	}

	// Look up entity in directory (public tiers, then the calling
	// user's learned rows) and try peers in failover order.
	peers := entity_peers_failover_for(from, entity_id)
	if len(peers) == 0 {
		return "", fmt.Errorf("entity not found in directory")
	}
	if p := remote_reach(peers); p != "" {
		return p, nil
	}
	return "", fmt.Errorf("failed to connect to any peer for entity %s", entity_id)
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
	if !ok || (!valid(entity_id, "entity") && !valid(entity_id, "fingerprint")) {
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

	app, _ := t.Local("app").(*App)
	from_app := ""
	var services []string
	if app != nil {
		from_app = app.id
		services = app_services(app, user)
	}

	// Connect to remote
	peer, err := remote_connect(user.Identity.ID, entity_id, peer)
	if err != nil {
		return sl_encode(map[string]any{"error": err.Error(), "code": 502, "transport": true}), nil
	}

	// Create stream
	from := user.Identity.ID
	s, err := stream_to_peer(peer, from, entity_id, service, event, from_app, services)
	if err != nil {
		return sl_encode(map[string]any{"error": err.Error(), "code": 502, "transport": true}), nil
	}
	defer s.close()

	// Send payload
	err = s.write(sl_decode(payload))
	if err != nil {
		return sl_encode(map[string]any{"error": fmt.Sprintf("failed to send: %v", err), "code": 502, "transport": true}), nil
	}

	// Read response
	var response any
	err = s.read(&response)
	if err != nil {
		// A clean EOF means the far end closed without answering (its
		// handler failed before the error-segment fix, or it died
		// mid-request). Say that in operator terms; the stream id and
		// raw error stay in the log for correlation.
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			info("Remote request %s/%s to %q: far end closed without answering: %v", service, event, entity_id, err)
			return sl_encode(map[string]any{"error": "remote host closed the stream without answering", "code": 504, "transport": true}), nil
		}
		return sl_encode(map[string]any{"error": fmt.Sprintf("failed to read response: %v", err), "code": 504, "transport": true}), nil
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
	if !ok || (!valid(entity_id, "entity") && !valid(entity_id, "fingerprint")) {
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

	// Get user and app from context. Anonymous (user==nil) is permitted —
	// the receiving event handler decides whether to honour anonymous calls.
	user, _ := t.Local("user").(*User)

	app, _ := t.Local("app").(*App)
	from_app := ""
	var services []string
	if app != nil {
		from_app = app.id
		services = app_services(app, user)
	}
	caller := ""
	if user != nil && user.Identity != nil {
		caller = user.Identity.ID
	}

	// Connect to remote
	peer, err := remote_connect(caller, entity_id, peer)
	if err != nil {
		return sl.None, nil
	}

	// Create stream — empty "from" identity for anonymous callers
	from := ""
	if user != nil && user.Identity != nil {
		from = user.Identity.ID
	}
	s, err := stream_to_peer(peer, from, entity_id, service, event, from_app, services)
	if err != nil {
		return sl.None, nil
	}

	// Send payload
	err = s.write(sl_decode(payload))
	if err != nil {
		s.close()
		return sl.None, nil
	}

	// Register stream for cleanup when script returns
	streams, _ := t.Local("streams").([]*Stream)
	t.SetLocal("streams", append(streams, s))

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
	if !ok || (!valid(entity_id, "entity") && !valid(entity_id, "fingerprint")) {
		return sl_error(fn, "invalid entity_id")
	}

	if len(args) > 1 {
		peer, _ = sl.AsString(args[1])
	}
	caller := ""
	if user, _ := t.Local("user").(*User); user != nil && user.Identity != nil {
		caller = user.Identity.ID
	}

	// A bare ping (no explicit peer) must not confirm the existence of a
	// private local entity to an unrelated caller — its unlisting is meant to
	// hide exactly that. An explicit peer is a deliberate probe of a known
	// location and is allowed. request/stream are not gated: their receiving
	// handler is the access boundary.
	if peer == "" && entity_private_local_foreign(caller, entity_id) {
		return sl_encode(map[string]any{"reachable": false}), nil
	}

	// Try to connect
	peer, err := remote_connect(caller, entity_id, peer)
	if err != nil {
		return sl_encode(map[string]any{"reachable": false, "error": err.Error()}), nil
	}

	return sl_encode(map[string]any{"reachable": true, "peer": peer}), nil
}

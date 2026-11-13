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
	neturl "net/url"
	"slices"
	"strings"
	"sync"
	"time"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

var api_remote = sls.FromStringDict(sl.String("mochi.remote"), sl.StringDict{
	"peer":     sl.NewBuiltin("mochi.remote.peer", api_remote_peer),
	"request":  sl.NewBuiltin("mochi.remote.request", api_remote_request),
	"stream":   sl.NewBuiltin("mochi.remote.stream", api_remote_stream),
	"streams":  sl.NewBuiltin("mochi.remote.streams", api_remote_streams),
	"requests": sl.NewBuiltin("mochi.remote.requests", api_remote_requests),
	"ping":     sl.NewBuiltin("mochi.remote.ping", api_remote_ping),
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

	// A bare peer id is the p2p/ form without its prefix: apps store the
	// directory's "p2p/<id>" location stripped, and a peer id names nothing an
	// HTTP fetch could reach, so it is connected directly and never gated.
	if valid(url, "peer") {
		url = "p2p/" + url
	}

	// Every other outbound fetch - mochi.url.*, mochi.rss.fetch, link preview -
	// gates on require_permission_url. This one did not, so an app with no grant
	// could probe any public host and port, and have what came back stored in
	// `peers` and dialed. The p2p/ form does no HTTP and needs no grant, and a
	// value that is no URL at all fetches nothing either: it answers None, as
	// it always did, rather than raising into the caller's action.
	if !strings.HasPrefix(url, "p2p/") {
		if _, err := domain_extract(remote_https(url)); err != nil {
			return sl.None, nil
		}
		if err := require_permission_url(t, fn, remote_https(url)); err != nil {
			return sl_error(fn, "%v", err)
		}
	}

	if err := remote_rate_limit(t, url); err != nil {
		return sl_error(fn, err)
	}

	peer, err := peer_connect_url(url)
	if err != nil {
		return sl.None, nil
	}

	return sl.String(peer), nil
}

// remote_https is the scheme a server name gets when it carries none.
func remote_https(url string) string {
	if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
		return "https://" + url
	}
	return url
}

// Connect to a peer by server URL or peer ID, returning the peer ID
func peer_connect_url(url string) (string, error) {
	// Handle p2p/ prefixed peer IDs (e.g. from directory location field)
	if strings.HasPrefix(url, "p2p/") {
		peer := strings.TrimPrefix(url, "p2p/")
		if peer_connect_waiting(context.Background(), peer) {
			return peer, nil
		}
		return "", fmt.Errorf("failed to connect to peer %s", peer)
	}

	// Plain http is refused below - this fetch learns a peer's addresses and
	// they are then dialed.
	url = remote_https(url)

	// Rebuild from scheme and host alone: an app-supplied string concatenated onto
	// the path would carry an arbitrary path, query or fragment - and userinfo -
	// into an app-chosen GET against any public host.
	parsed, err := neturl.Parse(url)
	if err != nil || parsed.Host == "" {
		return "", fmt.Errorf("invalid server URL")
	}
	if parsed.Scheme != "https" {
		return "", fmt.Errorf("invalid server URL scheme %q", parsed.Scheme)
	}
	info_url := parsed.Scheme + "://" + parsed.Host + "/_/p2p/info"
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
	var information struct {
		Peer      string   `json:"peer"`
		Addresses []string `json:"addresses"`
	}
	if err := json.Unmarshal(body, &information); err != nil {
		return "", fmt.Errorf("failed to parse net info: %v", err)
	}

	if information.Peer == "" || len(information.Addresses) == 0 {
		return "", fmt.Errorf("invalid net info: missing peer or addresses")
	}

	// Add peer and connect
	peer_add_known(information.Peer, information.Addresses)
	if !peer_connect_waiting(context.Background(), information.Peer) {
		return "", fmt.Errorf("failed to connect to peer %s", information.Peer)
	}

	return information.Peer, nil
}

// remote_address_wait bounds a synchronous remote request's wait for a
// peers/request answer about a peer we hold no addresses for. Record-holding
// relays normally answer inside a second; five seconds is headroom for relayed
// paths.
var remote_address_wait = 5 * time.Second

// remote_reach connects to the first reachable of `candidates` (peer ids in
// failover order), broadcasting a peers/request and retrying within one shared
// remote_address_wait budget when none connects. Returns the peer id, or "";
// "" too once ctx ends.
func remote_reach(ctx context.Context, candidates []string) string {
	for _, p := range candidates {
		if peer_connect_waiting(ctx, p) {
			return p
		}
	}
	if ctx.Err() != nil {
		return ""
	}
	requested := false
	for _, p := range candidates {
		if peer_request_addresses(p) {
			requested = true
		}
	}
	// Snapshot the pubsub drop counter: the answer arrives over pubsub, which
	// drops inbound messages past a budget, so a failure below can say whether
	// messages were being discarded rather than the peer never answering.
	dropped := pubsub_dropped.Load()

	// Bound the wait. A fresh broadcast waits the full window; when every
	// candidate was rate-limited, wait only the remainder of the in-flight
	// request's window - exactly one broadcast happens per minute per target, so
	// no answer is coming.
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
	// A connect another caller has in flight is waited on, within the same
	// budget: opens made together to one server's entities would otherwise
	// each give up while the first of them is still dialling.
	waiting, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 || ctx.Err() != nil {
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
			if peer_connect_waiting(waiting, p) {
				return p
			}
		}
	}
}

// Connect to a remote entity, returning the peer ID. An explicit `peer` (libp2p
// or entity id) is resolved once and used; otherwise each location from
// entity_peers_failover is tried in order, same policy as stream().
func remote_connect(from string, entity_id string, peer string) (string, error) {
	return remote_dial(context.Background(), from, entity_id, peer)
}

// remote_dial is remote_connect giving up once ctx ends.
func remote_dial(ctx context.Context, from string, entity_id string, peer string) (string, error) {
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
		if remote_reach(ctx, []string{peer}) == "" {
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
	if p := remote_reach(ctx, peers); p != "" {
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

	if err := remote_rate_limit(t, entity_id); err != nil {
		return sl_error(fn, err)
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
	user := principal_caller(t)
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
	s, err := stream_to_peer(context.Background(), peer, from, entity_id, service, event, from_app, services, stream_content(sl_decode(payload)))
	if err != nil {
		return sl_encode(map[string]any{"error": err.Error(), "code": 502, "transport": true}), nil
	}
	defer s.close()

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

	if err := remote_rate_limit(t, entity_id); err != nil {
		return sl_error(fn, err)
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
	user := principal_caller(t)

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

	s, err := remote_open(context.Background(), caller, entity_id, peer, service, event, from_app, services, sl_decode(payload), false)
	if err != nil {
		return sl.None, nil
	}

	// Register stream for cleanup when script returns
	streams, _ := t.Local("streams").([]*Stream)
	t.SetLocal("streams", append(streams, s))

	return s, nil
}

// remote_open connects to an entity's peer, opens a stream to it as the
// caller - an empty identity for an anonymous one - and sends the payload: the
// work of mochi.remote.stream, which waits on the stream's handshake. Once ctx
// ends it sends nothing more. A held payload waits for the far side's ack, so a
// request whose call gave up before its stream was open is never delivered;
// otherwise the payload may go with the open (stream_open), and such a request
// may be delivered though its call no longer waits for the answer. A variable
// so tests can stand in an open that takes its time.
var remote_open = func(ctx context.Context, caller string, entity_id string, peer string, service string, event string, from_app string, services []string, payload any, held bool) (*Stream, error) {
	peer, err := remote_dial(ctx, caller, entity_id, peer)
	if err != nil {
		return nil, err
	}
	if !held {
		return stream_to_peer(ctx, peer, caller, entity_id, service, event, from_app, services, stream_content(payload))
	}
	s, err := stream_to_peer(ctx, peer, caller, entity_id, service, event, from_app, services, nil)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		s.close()
		return nil, err
	}
	if err := s.write(stream_content(payload)); err != nil {
		s.close()
		return nil, err
	}
	return s, nil
}

// remote_streams_maximum is the most requests one mochi.remote.streams or
// mochi.remote.requests call makes.
const remote_streams_maximum = 32

// remote_segments_maximum is the most segments mochi.remote.requests reads of
// each answer.
const remote_segments_maximum = 16

// remote_streams_keys are the keys a request may hold: a misspelt one would
// otherwise send its stream nothing.
var remote_streams_keys = map[string]bool{"entity": true, "service": true, "event": true, "payload": true, "peer": true}

// remote_ask is one request of a mochi.remote.streams or mochi.remote.requests
// call; refused is set when the remote call budget turned it down.
type remote_ask struct {
	entity, service, event, peer string
	payload                      any
	refused                      bool
	// held keeps the payload for the far side's ack (remote_open).
	held bool
}

// remote_asks reads and checks a call's requests, each as mochi.remote.stream
// checks one, then charges each to the remote call budget: all are checked
// before any is charged, so a bad one late in the list spends nothing. One the
// budget refuses is marked, and answers None as one that could not be opened
// does; the others go on.
func remote_asks(t *sl.Thread, fn *sl.Builtin, list sl.Indexable) ([]remote_ask, error) {
	if list.Len() > remote_streams_maximum {
		return nil, fmt.Errorf("at most %d requests", remote_streams_maximum)
	}
	asks := make([]remote_ask, list.Len())
	for i := range asks {
		d, ok := list.Index(i).(*sl.Dict)
		if !ok {
			return nil, fmt.Errorf("request %d is not a dict", i)
		}
		for _, key := range d.Keys() {
			if name, _ := sl.AsString(key); !remote_streams_keys[name] {
				return nil, fmt.Errorf("request %d: unknown key %s", i, key)
			}
		}
		text := func(key string) string {
			value, _, _ := d.Get(sl.String(key))
			s, _ := sl.AsString(value)
			return s
		}
		r := remote_ask{entity: text("entity"), service: text("service"), event: text("event"), peer: text("peer")}
		if !valid(r.entity, "entity") && !valid(r.entity, "fingerprint") {
			return nil, fmt.Errorf("request %d: invalid entity", i)
		}
		if !valid(r.service, "constant") {
			return nil, fmt.Errorf("request %d: invalid service", i)
		}
		if !valid(r.event, "constant") {
			return nil, fmt.Errorf("request %d: invalid event", i)
		}
		payload, found, _ := d.Get(sl.String("payload"))
		if !found {
			return nil, fmt.Errorf("request %d: no payload", i)
		}
		r.payload = sl_decode(payload)
		asks[i] = r
	}
	for i, r := range asks {
		if err := remote_rate_limit(t, r.entity); err != nil {
			debug("%s: %s/%s to %q not opened: %v", fn.Name(), r.service, r.event, r.entity, err)
			asks[i].refused = true
		}
	}
	return asks, nil
}

// remote_timeout is a call's timeout argument: zero for None, otherwise a
// positive number of seconds, at most ceiling and at least a nanosecond.
func remote_timeout(value sl.Value, ceiling time.Duration) (time.Duration, error) {
	if value == sl.None {
		return 0, nil
	}
	seconds, ok := sl.AsFloat(value)
	if !ok || !(seconds > 0) {
		return 0, fmt.Errorf("timeout must be a positive number of seconds")
	}
	return max(time.Duration(min(seconds, ceiling.Seconds())*float64(time.Second)), time.Nanosecond), nil
}

// remote_together opens a call's requests beside one another, each under
// guard() - a panic in one is a failed open, not the end of the server - and
// hands each opened stream to use, which answers that request's result. It
// returns the results in order, nil where a request was refused, could not be
// opened, or had not finished when limit passed (no limit when zero) or the
// script's own context ended. At that point the context the opens and use run
// under ends too, so a late open sends nothing more, and a result that comes
// afterwards is handed to discard rather than returned.
func remote_together(t *sl.Thread, asks []remote_ask, limit time.Duration, use func(ctx context.Context, s *Stream) any, discard func(any)) []any {
	user := principal_caller(t)
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

	ctx, cancel := context.WithCancel(thread_context(t))
	defer cancel()
	var lock sync.Mutex
	results := make([]any, len(asks))
	late := false
	var wait sync.WaitGroup
	for i, r := range asks {
		if r.refused {
			continue
		}
		wait.Add(1)
		go func() {
			defer wait.Done()
			var result any
			guard("remote_together", nil, func() {
				if s, err := remote_open(ctx, caller, r.entity, r.peer, r.service, r.event, from_app, services, r.payload, r.held); err == nil {
					result = use(ctx, s)
				}
			})
			lock.Lock()
			defer lock.Unlock()
			if late {
				if result != nil {
					discard(result)
				}
				return
			}
			results[i] = result
		}()
	}
	done := make(chan struct{})
	go func() {
		wait.Wait()
		close(done)
	}()
	var expired <-chan time.Time
	if limit > 0 {
		timer := time.NewTimer(limit)
		defer timer.Stop()
		expired = timer.C
	}
	select {
	case <-done:
	case <-expired:
	case <-ctx.Done():
	}
	lock.Lock()
	late = true
	ready := slices.Clone(results)
	lock.Unlock()
	return ready
}

// mochi.remote.streams(requests, timeout) -> list: Open several streams to remote entities at once. Each request is a dict of entity, service, event and payload, and optionally peer, as mochi.remote.stream takes them. The streams come back in the order asked, None where one could not be opened, was refused by the remote call budget, or was not open within timeout seconds when one is given; a request not open in time is not delivered. Each open waits on its own handshake, so opening them together costs about one open rather than one per stream. At most 32 requests a call.
func api_remote_streams(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var list sl.Indexable
	var timeout sl.Value = sl.None
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "requests", &list, "timeout?", &timeout); err != nil {
		return nil, err
	}
	limit, err := remote_timeout(timeout, 300*time.Second)
	if err != nil {
		return sl_error(fn, err)
	}
	asks, err := remote_asks(t, fn, list)
	if err != nil {
		return sl_error(fn, err)
	}
	// A stream comes back to be written on, so a request not open in time must
	// not have gone: each payload waits for its stream's ack.
	for i := range asks {
		asks[i].held = true
	}
	results := remote_together(t, asks, limit,
		func(ctx context.Context, s *Stream) any { return s },
		func(result any) { result.(*Stream).close() })

	// Registered here, on the script's own thread, for cleanup when it returns.
	streams, _ := t.Local("streams").([]*Stream)
	out := make([]sl.Value, len(results))
	for i, result := range results {
		out[i] = sl.None
		if s, ok := result.(*Stream); ok {
			streams = append(streams, s)
			out[i] = s
		}
	}
	t.SetLocal("streams", streams)
	return sl.NewList(out), nil
}

// remote_answer_grace is how long past its deadline mochi.remote.requests
// waits for reads that deadline ended to report.
const remote_answer_grace = 50 * time.Millisecond

// remote_answer reads up to segments segments of an answer on s, then closes
// it: the segments read, fewer where the answer ended sooner, or nil where it
// was still coming at the deadline (none when zero) or when ctx ended -
// whichever side of the deadline its last read fell, so an answer cut off is
// never taken for one that ended.
func remote_answer(ctx context.Context, s *Stream, segments int, deadline time.Time) []any {
	defer s.close()
	// A read waiting on an answer the call no longer wants ends with it.
	stop := context.AfterFunc(ctx, s.close)
	defer stop()
	answer := []any{}
	for range segments {
		wait := time.Duration(0)
		if !deadline.IsZero() {
			if wait = time.Until(deadline); wait <= 0 {
				return nil
			}
		}
		var segment any
		if err := s.read_within(&segment, wait); err != nil {
			if !deadline.IsZero() && !time.Now().Before(deadline) || ctx.Err() != nil {
				return nil
			}
			break
		}
		answer = append(answer, segment)
	}
	return answer
}

// mochi.remote.requests(requests, timeout, segments) -> list: Send several requests to remote entities at once and collect their answers. Each request is a dict of entity, service, event and payload, and optionally peer, as mochi.remote.stream takes them. For each, in the order asked: the list of the first segments its far end answered, up to segments (default 1, at most 16) and fewer where its answer ended sooner, or None where it could not be opened, was refused by the remote call budget, or did not answer in full within timeout seconds when one is given. Nothing is sent once timeout has passed, but a request whose open was still under way then may yet run on the far side, its answer not waited for. The requests are sent and their answers read together, so a call costs about as long as its slowest answer. At most 32 requests a call.
func api_remote_requests(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var list sl.Indexable
	var timeout sl.Value = sl.None
	segments := 1
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "requests", &list, "timeout?", &timeout, "segments?", &segments); err != nil {
		return nil, err
	}
	if segments < 1 || segments > remote_segments_maximum {
		return sl_error(fn, "segments must be from 1 to %d", remote_segments_maximum)
	}
	limit, err := remote_timeout(timeout, 300*time.Second)
	if err != nil {
		return sl_error(fn, err)
	}
	asks, err := remote_asks(t, fn, list)
	if err != nil {
		return sl_error(fn, err)
	}
	deadline := time.Now().Add(limit)
	wait := limit
	if limit == 0 {
		deadline = time.Time{}
	} else {
		// The reads themselves end at the deadline; the call waits a moment
		// longer, so an answer they cut off reports as cut off rather than
		// racing the call's own timer, which is left to bound opens that hang.
		wait = limit + remote_answer_grace
	}
	results := remote_together(t, asks, wait, func(ctx context.Context, s *Stream) any {
		// A nil answer must reach remote_together as no result, not as a typed
		// empty one, or it would be answered as an empty list.
		if answer := remote_answer(ctx, s, segments, deadline); answer != nil {
			return answer
		}
		return nil
	}, func(any) {})

	out := make([]sl.Value, len(results))
	for i, result := range results {
		out[i] = sl.None
		if answer, ok := result.([]any); ok {
			values := make([]sl.Value, len(answer))
			for j, segment := range answer {
				values[j] = sl_encode(segment)
			}
			out[i] = sl.NewList(values)
		}
	}
	return sl.NewList(out), nil
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

	if err := remote_rate_limit(t, entity_id); err != nil {
		return sl_error(fn, err)
	}

	if len(args) > 1 {
		peer, _ = sl.AsString(args[1])
	}
	caller := ""
	if user := principal_caller(t); user != nil && user.Identity != nil {
		caller = user.Identity.ID
	}

	// A bare ping must not confirm a private local entity's existence to an
	// unrelated caller. An explicit peer is a deliberate probe and is allowed;
	// request/stream are not gated, since the receiving handler is their boundary.
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

// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/fclient"
	"github.com/matrix-org/gomatrixserverlib/spec"
	"github.com/sirupsen/logrus"
	sl "go.starlark.net/starlark"
)

// A matrix_remote is another Matrix server for the engine's tests: its rooms
// built with the same library, its key, and the federation endpoints the
// engine calls, recording what it is sent.
type matrix_remote struct {
	t        *testing.T
	server   *httptest.Server
	name     spec.ServerName
	key      gomatrixserverlib.KeyID
	private  ed25519.PrivateKey
	public   ed25519.PublicKey
	version  gomatrixserverlib.IRoomVersion
	lock     sync.Mutex
	events   map[string]gomatrixserverlib.PDU
	before   map[string]map[string]string
	state    map[string]map[string]string
	edge     map[string][]string
	aliases  map[string]string
	received []gomatrixserverlib.PDU
	invited  []gomatrixserverlib.PDU
	asked    []string
	fail     bool
	missing  bool
}

func create_matrix_remote(t *testing.T, version gomatrixserverlib.RoomVersion) *matrix_remote {
	t.Helper()
	public, private, _ := ed25519.GenerateKey(rand.Reader)
	verImpl, err := gomatrixserverlib.GetRoomVersion(version)
	if err != nil {
		t.Fatal(err)
	}
	r := &matrix_remote{t: t, key: "ed25519:remote", private: private, public: public, version: verImpl,
		events: map[string]gomatrixserverlib.PDU{}, before: map[string]map[string]string{}, state: map[string]map[string]string{}, edge: map[string][]string{}, aliases: map[string]string{}}
	r.server = httptest.NewServer(http.HandlerFunc(r.serve))
	t.Cleanup(r.server.Close)
	r.name = spec.ServerName(fake_host(r.server))
	return r
}

func (r *matrix_remote) user(name string) string {
	return "@" + name + ":" + string(r.name)
}

// build makes an event of a room on a state, following the events given.
func (r *matrix_remote) build(room string, sender string, kind string, key *string, content any, prevs []string, state map[string]string) gomatrixserverlib.PDU {
	r.t.Helper()
	proto := gomatrixserverlib.ProtoEvent{SenderID: sender, RoomID: room, Type: kind, StateKey: key}
	if err := proto.SetContent(content); err != nil {
		r.t.Fatal(err)
	}
	if kind == spec.MRoomCreate && r.version.DomainlessRoomIDs() {
		proto.RoomID = ""
	}
	depth := int64(0)
	for _, prev := range prevs {
		depth = max(depth, r.events[prev].Depth())
	}
	proto.PrevEvents = append([]string{}, prevs...)
	proto.Depth = depth + 1
	needed, err := gomatrixserverlib.StateNeededForProtoEvent(&proto)
	if err != nil {
		r.t.Fatal(err)
	}
	var auth []gomatrixserverlib.PDU
	for _, tuple := range needed.Tuples() {
		if id := state[rooms_key(tuple.EventType, tuple.StateKey)]; id != "" {
			auth = append(auth, r.events[id])
		}
	}
	provider, _ := gomatrixserverlib.NewAuthEvents(auth)
	builder := r.version.NewEventBuilderFromProtoEvent(&proto)
	if err := builder.AddAuthEvents(provider); err != nil {
		r.t.Fatal(err)
	}
	event, err := builder.Build(time.Now(), r.name, r.key, r.private)
	if err != nil {
		r.t.Fatal(err)
	}
	return event
}

// adopt learns a room of the engine's from what a join answered: its state
// and auth chain.
func (r *matrix_remote) adopt(room string, body map[string]any) {
	r.lock.Lock()
	defer r.lock.Unlock()
	state := map[string]string{}
	for _, list := range []string{"auth_chain", "state"} {
		items, _ := body[list].([]any)
		for _, item := range items {
			event, err := r.version.NewEventFromUntrustedJSON(mustjson(item))
			if err != nil {
				r.t.Fatal(err)
			}
			r.events[event.EventID()] = event
			if list == "state" && event.StateKey() != nil {
				state[rooms_key(event.Type(), *event.StateKey())] = event.EventID()
			}
		}
	}
	r.state[room] = state
}

// forge builds an event naming the auth events given, whatever the state
// says.
func (r *matrix_remote) forge(room string, sender string, body string, prevs []string, auth []string) gomatrixserverlib.PDU {
	r.t.Helper()
	proto := gomatrixserverlib.ProtoEvent{SenderID: sender, RoomID: room, Type: "m.room.message"}
	proto.SetContent(map[string]any{"msgtype": "m.text", "body": body})
	depth := int64(0)
	for _, prev := range prevs {
		depth = max(depth, r.events[prev].Depth())
	}
	proto.PrevEvents, proto.AuthEvents, proto.Depth = prevs, auth, depth+1
	event, err := r.version.NewEventBuilderFromProtoEvent(&proto).Build(time.Now(), r.name, r.key, r.private)
	if err != nil {
		r.t.Fatal(err)
	}
	return event
}

// take makes an event the newest of its room.
func (r *matrix_remote) take(event gomatrixserverlib.PDU) {
	room := event.RoomID().String()
	r.before[event.EventID()] = maps.Clone(r.state[room])
	r.events[event.EventID()] = event
	if r.state[room] == nil {
		r.state[room] = map[string]string{}
	}
	if event.StateKey() != nil {
		r.state[room][rooms_key(event.Type(), *event.StateKey())] = event.EventID()
	}
	r.edge[room] = []string{event.EventID()}
}

// event builds and takes an event on a room's current state.
func (r *matrix_remote) event(room string, sender string, kind string, key *string, content any) gomatrixserverlib.PDU {
	r.lock.Lock()
	defer r.lock.Unlock()
	event := r.build(room, sender, kind, key, content, r.edge[room], r.state[room])
	r.take(event)
	return event
}

// room makes a room as bob, named, which shares its history.
func (r *matrix_remote) room(name string, rule string) string {
	bob := r.user("bob")
	empty := ""
	create := map[string]any{"room_version": string(r.version.Version())}
	if r.version.CreatorInCreateEvent() {
		create["creator"] = bob
	}
	r.lock.Lock()
	room := "!room" + strconv.Itoa(len(r.state)+1) + ":" + string(r.name)
	event := r.build(room, bob, spec.MRoomCreate, &empty, create, nil, map[string]string{})
	room = event.RoomID().String()
	r.take(event)
	r.lock.Unlock()
	users := map[string]any{}
	if !r.version.PrivilegedCreators() {
		users[bob] = 100
	}
	r.event(room, bob, spec.MRoomMember, &bob, map[string]any{"membership": "join", "displayname": "Bob"})
	r.event(room, bob, spec.MRoomPowerLevels, &empty, map[string]any{"users": users, "invite": 0, "redact": 50, "ban": 50, "kick": 50, "state_default": 50, "events_default": 0, "users_default": 0})
	r.event(room, bob, spec.MRoomJoinRules, &empty, map[string]any{"join_rule": rule})
	r.event(room, bob, spec.MRoomHistoryVisibility, &empty, map[string]any{"history_visibility": "shared"})
	r.event(room, bob, "m.room.name", &empty, map[string]any{"name": name})
	return room
}

// chain is the auth chain of events.
func (r *matrix_remote) chain(events []gomatrixserverlib.PDU) []gomatrixserverlib.PDU {
	seen := map[string]bool{}
	var out []gomatrixserverlib.PDU
	var queue []string
	for _, event := range events {
		queue = append(queue, event.AuthEventIDs()...)
	}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if seen[id] || r.events[id] == nil {
			continue
		}
		seen[id] = true
		out = append(out, r.events[id])
		queue = append(queue, r.events[id].AuthEventIDs()...)
	}
	return out
}

func (r *matrix_remote) list(state map[string]string) []gomatrixserverlib.PDU {
	var out []gomatrixserverlib.PDU
	for _, id := range state {
		out = append(out, r.events[id])
	}
	return out
}

// walk goes back from events by their prev events.
func (r *matrix_remote) walk(from []string, stop map[string]bool, limit int) []gomatrixserverlib.PDU {
	seen := map[string]bool{}
	var out []gomatrixserverlib.PDU
	queue := slices.Clone(from)
	for len(queue) > 0 && len(out) < limit {
		id := queue[0]
		queue = queue[1:]
		if seen[id] || stop[id] || r.events[id] == nil {
			continue
		}
		seen[id] = true
		out = append(out, r.events[id])
		queue = append(queue, r.events[id].PrevEventIDs()...)
	}
	sort.Slice(out, func(i int, j int) bool { return out[i].Depth() < out[j].Depth() })
	return out
}

func raw_events(events []gomatrixserverlib.PDU) []json.RawMessage {
	out := []json.RawMessage{}
	for _, event := range events {
		out = append(out, event.JSON())
	}
	return out
}

// template is a membership event for one of the engine's users to fill.
func (r *matrix_remote) template(room string, user string, membership string) gomatrixserverlib.ProtoEvent {
	state := r.state[room]
	proto := gomatrixserverlib.ProtoEvent{SenderID: user, RoomID: room, Type: spec.MRoomMember, StateKey: &user}
	proto.SetContent(map[string]any{"membership": membership})
	depth := int64(0)
	for _, prev := range r.edge[room] {
		depth = max(depth, r.events[prev].Depth())
	}
	proto.PrevEvents = slices.Clone(r.edge[room])
	proto.Depth = depth + 1
	needed, _ := gomatrixserverlib.StateNeededForProtoEvent(&proto)
	var auth []gomatrixserverlib.PDU
	for _, tuple := range needed.Tuples() {
		if id := state[rooms_key(tuple.EventType, tuple.StateKey)]; id != "" {
			auth = append(auth, r.events[id])
		}
	}
	provider, _ := gomatrixserverlib.NewAuthEvents(auth)
	builder := r.version.NewEventBuilderFromProtoEvent(&proto)
	builder.AddAuthEvents(provider)
	proto.AuthEvents = builder.AuthEvents
	if r.version.Version() == gomatrixserverlib.RoomVersionV1 || r.version.Version() == gomatrixserverlib.RoomVersionV2 {
		// The first two versions name events by reference.
		event, _ := builder.Build(time.Now(), r.name, r.key, r.private)
		var references struct {
			Prev any `json:"prev_events"`
			Auth any `json:"auth_events"`
		}
		json.Unmarshal(event.JSON(), &references)
		proto.PrevEvents, proto.AuthEvents = references.Prev, references.Auth
	}
	return proto
}

func (r *matrix_remote) answer(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(body)
}

// serve answers the engine's requests, each checked against the engine's
// signature.
func (r *matrix_remote) serve(w http.ResponseWriter, req *http.Request) {
	if req.URL.Path == "/_matrix/key/v2/server" {
		document, _ := json.Marshal(map[string]any{"server_name": string(r.name), "valid_until_ts": time.Now().Add(time.Hour).UnixMilli(),
			"verify_keys": map[string]any{string(r.key): map[string]any{"key": base64.RawStdEncoding.EncodeToString(r.public)}}, "old_verify_keys": map[string]any{}})
		signed, _ := gomatrixserverlib.SignJSON(string(r.name), r.key, r.private, document)
		w.Header().Set("Content-Type", "application/json")
		w.Write(signed)
		return
	}
	request, failure := fclient.VerifyHTTPRequest(req, time.Now(), r.name, func(s spec.ServerName) bool { return s == r.name }, federation_keyring())
	if request == nil {
		r.answer(w, failure.Code, failure.JSON)
		return
	}
	r.lock.Lock()
	defer r.lock.Unlock()
	r.asked = append(r.asked, req.Method+" "+req.URL.Path)
	parts := strings.Split(strings.TrimPrefix(req.URL.Path, "/_matrix/federation/"), "/")
	for i := range parts {
		parts[i], _ = url.PathUnescape(parts[i])
	}
	route := req.Method + " " + parts[0] + "/" + parts[1]
	switch {
	case route == "PUT v1/send":
		if r.fail {
			r.answer(w, 500, map[string]any{"errcode": "M_UNKNOWN"})
			return
		}
		var transaction struct {
			PDUs []json.RawMessage `json:"pdus"`
		}
		json.Unmarshal(request.Content(), &transaction)
		for _, raw := range transaction.PDUs {
			if event, err := r.version.NewEventFromUntrustedJSON(raw); err == nil {
				r.received = append(r.received, event)
				r.take(event)
			}
		}
		r.answer(w, 200, map[string]any{"pdus": map[string]any{}})
	case route == "GET v1/make_join":
		r.answer(w, 200, map[string]any{"event": r.template(parts[2], parts[3], "join"), "room_version": string(r.version.Version())})
	case route == "GET v1/make_leave":
		r.answer(w, 200, map[string]any{"event": r.template(parts[2], parts[3], "leave"), "room_version": string(r.version.Version())})
	case route == "PUT v2/send_join":
		event, err := r.version.NewEventFromUntrustedJSON(request.Content())
		if err != nil {
			r.answer(w, 400, map[string]any{"errcode": "M_BAD_JSON", "error": err.Error()})
			return
		}
		state := r.list(r.state[parts[2]])
		r.take(event)
		r.answer(w, 200, map[string]any{"origin": string(r.name), "state": raw_events(state), "auth_chain": raw_events(r.chain(append(state, event))), "event": json.RawMessage(event.JSON()), "members_omitted": false, "servers_in_room": []string{string(r.name)}})
	case route == "PUT v2/send_leave":
		event, err := r.version.NewEventFromUntrustedJSON(request.Content())
		if err != nil {
			r.answer(w, 400, map[string]any{"errcode": "M_BAD_JSON"})
			return
		}
		r.received = append(r.received, event)
		r.take(event)
		r.answer(w, 200, map[string]any{})
	case route == "PUT v2/invite":
		var invite fclient.InviteV2Request
		if err := json.Unmarshal(request.Content(), &invite); err != nil {
			r.answer(w, 400, map[string]any{"errcode": "M_BAD_JSON"})
			return
		}
		signed := invite.Event().Sign(string(r.name), r.key, r.private)
		r.invited = append(r.invited, signed)
		r.answer(w, 200, map[string]any{"event": json.RawMessage(signed.JSON())})
	case route == "GET v1/event":
		event := r.events[parts[2]]
		if event == nil {
			r.answer(w, 404, map[string]any{"errcode": "M_NOT_FOUND"})
			return
		}
		r.answer(w, 200, map[string]any{"origin": string(r.name), "origin_server_ts": time.Now().UnixMilli(), "pdus": []json.RawMessage{event.JSON()}})
	case route == "GET v1/state":
		state := r.list(r.before[req.URL.Query().Get("event_id")])
		r.answer(w, 200, map[string]any{"pdus": raw_events(state), "auth_chain": raw_events(r.chain(state))})
	case route == "GET v1/event_auth":
		r.answer(w, 200, map[string]any{"auth_chain": raw_events(r.chain([]gomatrixserverlib.PDU{r.events[parts[3]]}))})
	case route == "POST v1/get_missing_events":
		if r.missing {
			r.answer(w, 200, map[string]any{"events": []any{}})
			return
		}
		var missing fclient.MissingEvents
		json.Unmarshal(request.Content(), &missing)
		stop := map[string]bool{}
		for _, id := range missing.EarliestEvents {
			stop[id] = true
		}
		var from []string
		for _, id := range missing.LatestEvents {
			if r.events[id] != nil {
				from = append(from, r.events[id].PrevEventIDs()...)
			}
		}
		r.answer(w, 200, map[string]any{"events": raw_events(r.walk(from, stop, missing.Limit))})
	case route == "GET v1/backfill":
		limit, _ := strconv.Atoi(req.URL.Query().Get("limit"))
		r.answer(w, 200, map[string]any{"origin": string(r.name), "origin_server_ts": time.Now().UnixMilli(), "pdus": raw_events(r.walk(req.URL.Query()["v"], map[string]bool{}, limit))})
	case route == "GET v1/query" && parts[2] == "profile":
		if req.URL.Query().Get("user_id") != r.user("bob") {
			r.answer(w, 404, map[string]any{"errcode": "M_NOT_FOUND"})
			return
		}
		r.answer(w, 200, map[string]any{"displayname": "Bob"})
	case route == "GET v1/query" && parts[2] == "directory":
		room := r.aliases[req.URL.Query().Get("room_alias")]
		if room == "" {
			r.answer(w, 404, map[string]any{"errcode": "M_NOT_FOUND"})
			return
		}
		r.answer(w, 200, map[string]any{"room_id": room, "servers": []string{string(r.name)}})
	default:
		r.answer(w, 404, map[string]any{"errcode": "M_UNRECOGNIZED"})
	}
}

// call sends a signed request to the engine as this server, for the domain
// given, answering the status and the body.
func (r *matrix_remote) call(t *testing.T, method string, path string, content any) (int, map[string]any) {
	t.Helper()
	return r.call_as(t, method, path, content, "example.com", r.private)
}

func (r *matrix_remote) call_as(t *testing.T, method string, path string, content any, destination spec.ServerName, private ed25519.PrivateKey) (int, map[string]any) {
	t.Helper()
	request := fclient.NewFederationRequest(method, r.name, destination, path)
	if content != nil {
		if err := request.SetContent(content); err != nil {
			t.Fatal(err)
		}
	}
	if err := request.Sign(r.name, r.key, private); err != nil {
		t.Fatal(err)
	}
	built, err := request.HTTPRequest()
	if err != nil {
		t.Fatal(err)
	}
	built.Host = "example.com"
	built.URL.Scheme = "http"
	built.URL.Host = "example.com"
	if built.Body == nil {
		built.Body = http.NoBody
	}
	return matrix_engine_request(t, built)
}

// matrix_engine_request hands a request to the engine and answers its status
// and body.
func matrix_engine_request(t *testing.T, request *http.Request) (int, map[string]any) {
	t.Helper()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = request
	if !federation_route(c) {
		return 0, nil
	}
	var body map[string]any
	data := w.Body.Bytes()
	if len(data) > 0 && data[0] == '[' {
		var pair []any
		json.Unmarshal(data, &pair)
		if len(pair) == 2 {
			body, _ = pair[1].(map[string]any)
		}
	} else {
		json.Unmarshal(data, &body)
	}
	return w.Code, body
}

// push sends events to the engine in a transaction, answering what it said
// of each.
func (r *matrix_remote) push(t *testing.T, id string, events ...gomatrixserverlib.PDU) map[string]any {
	t.Helper()
	status, body := r.call(t, "PUT", "/_matrix/federation/v1/send/"+id, map[string]any{"origin": string(r.name), "origin_server_ts": time.Now().UnixMilli(), "pdus": raw_events(events)})
	if status != 200 {
		t.Fatalf("transaction answered %d %v", status, body)
	}
	results, _ := body["pdus"].(map[string]any)
	return results
}

// received_of answers the events the remote was sent of a type, by whom.
func (r *matrix_remote) received_of(kind string, sender string) []gomatrixserverlib.PDU {
	r.lock.Lock()
	defer r.lock.Unlock()
	var out []gomatrixserverlib.PDU
	for _, event := range r.received {
		if event.Type() == kind && string(event.SenderID()) == sender {
			out = append(out, event)
		}
	}
	return out
}

// create_matrix_test_env is alice with a hosted Matrix account
// @alice:example.com ("m1"), the engine reaching other servers by plain HTTP
// and with its caches empty, and the inbound log installed.
func create_matrix_test_env(t *testing.T) (*User, *inbound_log) {
	t.Helper()
	alice := create_linked_test_env(t)
	db := db_user(alice, "user")
	db.exec("insert into accounts (id, type, label, identifier, data, created, verified, enabled) values ('m1', 'matrix', '', '@alice:example.com', ?, 1, 1, 1)", json_encode(map[string]any{"hosted": true, "handle": "alice", "domain": "example.com"}))
	actor_register(alice, "m1", "matrix", "example.com", "alice")
	saved_scheme, saved_identities, saved_targets, saved_transports, saved_limiter, saved_version := federation_scheme, federation_identities, federation_targets, federation_transports_cache, rate_limit_host, rooms_version_default
	federation_scheme = "http"
	federation_identities = map[spec.ServerName]*federation_identity{}
	federation_targets = map[string]federation_target{}
	federation_transports_cache = map[string]*http.Transport{}
	rate_limit_host = &rate_limiter{entries: make(map[string]*rate_limit_entry), limit: 100000, window: 60}
	t.Cleanup(func() {
		federation_scheme, federation_identities, federation_targets, federation_transports_cache, rate_limit_host, rooms_version_default = saved_scheme, saved_identities, saved_targets, saved_transports, saved_limiter, saved_version
	})
	logrus.SetOutput(io.Discard)
	log := &inbound_log{}
	log.install(t)
	return alice, log
}

// matrix_join has alice accept an invitation from bob to a room of the
// remote's and join it.
func matrix_join(t *testing.T, r *matrix_remote, alice *User, room string) {
	t.Helper()
	invitee := "@alice:example.com"
	invite := r.build(room, r.user("bob"), spec.MRoomMember, &invitee, map[string]any{"membership": "invite"}, r.edge[room], r.state[room])
	stripped := []gomatrixserverlib.InviteStrippedState{gomatrixserverlib.NewInviteStrippedState(r.events[r.state[room][rooms_key("m.room.name", "")]])}
	request, err := fclient.NewInviteV2Request(invite, stripped)
	if err != nil {
		t.Fatal(err)
	}
	status, body := r.call(t, "PUT", "/_matrix/federation/v2/invite/"+url.PathEscape(room)+"/"+url.PathEscape(invite.EventID()), request)
	if status != 200 {
		t.Fatalf("invitation answered %d %v", status, body)
	}
	signed, err := r.version.NewEventFromUntrustedJSON(mustjson(body["event"]))
	if err != nil {
		t.Fatal(err)
	}
	if err := gomatrixserverlib.VerifyEventSignatures(t.Context(), signed, federation_keyring(), federation_sender); err != nil || !strings.Contains(string(signed.JSON()), `"example.com":{`) {
		t.Fatalf("the invitation came back without this server's signature: %v %s", err, signed.JSON())
	}
	r.lock.Lock()
	r.take(signed)
	r.lock.Unlock()
	thread := linked_thread(alice)
	joined := decoded(t)(api_account_room_join(thread, sl.NewBuiltin("test", nil), sl.Tuple{sl.String("m1"), sl.String(room)}, nil))
	if joined["conversation"] != room {
		t.Fatalf("join = %v", joined)
	}
}

func mustjson(value any) []byte {
	data, _ := json.Marshal(value)
	return data
}

// TestMatrixDiscovery: a Matrix domain names itself on 443 and publishes its
// signing key, signed by itself; another domain of the server is not one.
func TestMatrixDiscovery(t *testing.T) {
	create_matrix_test_env(t)
	get := func(host string, path string) (int, []byte, bool) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("GET", "http://"+host+path, nil)
		answered := federation_route(c)
		return w.Code, w.Body.Bytes(), answered
	}
	if status, body, _ := get("example.com", "/.well-known/matrix/server"); status != 200 || !strings.Contains(string(body), `"m.server":"example.com:443"`) {
		t.Errorf("well-known = %d %s", status, body)
	}
	if _, _, answered := get("other.example", "/.well-known/matrix/server"); answered {
		t.Error("a domain with no Matrix account answered the well-known document")
	}
	status, body, _ := get("example.com", "/_matrix/key/v2/server")
	var keys gomatrixserverlib.ServerKeys
	if status != 200 || json.Unmarshal(body, &keys) != nil || keys.ServerName != "example.com" || len(keys.VerifyKeys) != 1 {
		t.Fatalf("keys = %d %s", status, body)
	}
	for id, key := range keys.VerifyKeys {
		if err := gomatrixserverlib.VerifyJSON("example.com", id, ed25519.PublicKey(key.Key), body); err != nil {
			t.Errorf("the key document is not signed by its key: %v", err)
		}
		if id != federation_identity_of("example.com").key {
			t.Errorf("key id = %s", id)
		}
	}
	if status, _, _ := get("other.example", "/_matrix/key/v2/server"); status != 404 {
		t.Errorf("keys of a domain with no Matrix account = %d", status)
	}
	if status, body, _ := get("example.com", "/_matrix/federation/v1/version"); status != 200 || !strings.Contains(string(body), "Mochi") {
		t.Errorf("version = %d %s", status, body)
	}
	if status, body, _ := get("example.com", "/_matrix/client/v3/sync"); status != 404 || !strings.Contains(string(body), "M_UNRECOGNIZED") {
		t.Errorf("client interface = %d %s, want it unrecognised", status, body)
	}
}

// TestMatrixKeyType: an ed25519 key is made, its public half as Matrix
// writes it.
func TestMatrixKeyType(t *testing.T) {
	public, private, err := key_generate(key_ed25519)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := base64.RawStdEncoding.DecodeString(public)
	if err != nil || len(decoded) != ed25519.PublicKeySize || !strings.Contains(private, "PRIVATE KEY") {
		t.Errorf("ed25519 key = %q, %q", public, private)
	}
}

// TestMatrixRequests: a request from another server is taken only with its
// signature, by its own key, for this server; and not from a server blocked
// here.
func TestMatrixRequests(t *testing.T) {
	create_matrix_test_env(t)
	r := create_matrix_remote(t, gomatrixserverlib.RoomVersionV10)
	path := "/_matrix/federation/v1/query/profile?user_id=" + url.QueryEscape("@alice:example.com")
	if status, body := r.call(t, "GET", path, nil); status != 200 || body["displayname"] != "Alice" {
		t.Errorf("profile = %d %v", status, body)
	}
	if status, _ := r.call(t, "GET", "/_matrix/federation/v1/query/profile?user_id="+url.QueryEscape("@nobody:example.com"), nil); status != 404 {
		t.Errorf("profile of no one = %d", status)
	}
	unsigned := httptest.NewRequest("GET", "http://example.com"+path, nil)
	if status, _ := matrix_engine_request(t, unsigned); status != 401 {
		t.Errorf("unsigned request = %d, want 401", status)
	}
	_, forger, _ := ed25519.GenerateKey(rand.Reader)
	if status, _ := r.call_as(t, "GET", path, nil, "example.com", forger); status != 401 {
		t.Errorf("request signed by another key = %d, want 401", status)
	}
	if status, _ := r.call_as(t, "GET", path, nil, "other.example", r.private); status == 200 {
		t.Errorf("request for another server = %d", status)
	}
	// An event id of version 3 may hold a slash, sent escaped.
	if status, body := r.call(t, "GET", "/_matrix/federation/v1/event/"+url.PathEscape("$abc/def+ghi"), nil); status != 404 || body["errcode"] != "M_NOT_FOUND" {
		t.Errorf("an escaped slash in an event id = %d %v, want the event looked for", status, body)
	}
	networks_database().exec("insert into blocks (domain, created) values ('127.0.0.1', 1)")
	if status, _ := r.call(t, "GET", path, nil); status != 403 {
		t.Errorf("request from a blocked server = %d, want 403", status)
	}
}

// TestMatrixJoin: across every room version, an invitation from another
// server is countersigned and told to the user's apps; accepting it joins
// the room through that server, telling the apps of the room, its members
// and its history in order; the room's messages arrive in transactions; and
// what the user sends reaches the other server signed by this one.
func TestMatrixJoin(t *testing.T) {
	alice, log := create_matrix_test_env(t)
	for _, version := range []gomatrixserverlib.RoomVersion{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"} {
		t.Run("v"+string(version), func(t *testing.T) {
			r := create_matrix_remote(t, version)
			room := r.room("Remote room "+string(version), "invite")
			bob := r.user("bob")
			first := r.event(room, bob, "m.room.message", nil, map[string]any{"msgtype": "m.text", "body": "one " + string(version)})
			r.event(room, bob, "m.room.message", nil, map[string]any{"msgtype": "m.text", "body": "two " + string(version)})
			matrix_join(t, r, alice, room)
			invite := log.find(t, "invite", func(e map[string]any) bool { return e["conversation"] == room })
			if invite["from"] != bob || invite["name"] != "Remote room "+string(version) {
				t.Errorf("invite = %v", invite)
			}
			summary := log.find(t, "room", func(e map[string]any) bool { return e["conversation"] == room })
			if summary["name"] != "Remote room "+string(version) || summary["count"] != 2 {
				t.Errorf("room = %v", summary)
			}
			if member := log.find(t, "member", func(e map[string]any) bool { return e["conversation"] == room }); member["id"] != bob || member["name"] != "Bob" {
				t.Errorf("member = %v", member)
			}
			history := log.matching(func(e map[string]any) bool { return e["kind"] == "message" && e["conversation"] == room })
			if len(history) != 2 || history[0]["body"] != "one "+string(version) || history[1]["body"] != "two "+string(version) || history[0]["id"] != first.EventID() {
				t.Errorf("history = %v", history)
			}
			if membership := rooms_membership(room, "@alice:example.com"); membership != spec.Join {
				t.Errorf("membership = %q", membership)
			}
			// A message in a transaction.
			three := r.event(room, bob, "m.room.message", nil, map[string]any{"msgtype": "m.text", "body": "three"})
			if results := r.push(t, "t1"+string(version), three); results[three.EventID()] == nil || len(results[three.EventID()].(map[string]any)) != 0 {
				t.Errorf("transaction results = %v", results)
			}
			if message := log.find(t, "message", func(e map[string]any) bool { return e["id"] == three.EventID() }); message["body"] != "three" || message["from"] != bob || message["name"] != "Bob" || message["room"] != true {
				t.Errorf("message = %v", message)
			}
			// What the user sends.
			thread := linked_thread(alice)
			object := sl.NewDict(2)
			object.SetKey(sl.String("conversation"), sl.String(room))
			object.SetKey(sl.String("content"), sl.String("hello bob"))
			sent := decoded(t)(api_account_object_create(thread, sl.NewBuiltin("test", nil), sl.Tuple{sl.String("m1"), object}, []sl.Tuple{{sl.String("key"), sl.String("chat|" + string(version))}}))
			wait(t, "the message at the other server", func() bool { return len(r.received_of("m.room.message", "@alice:example.com")) == 1 })
			got := r.received_of("m.room.message", "@alice:example.com")[0]
			if got.EventID() != sent["id"] || !strings.Contains(string(got.Content()), "hello bob") || !slices.Contains(got.PrevEventIDs(), three.EventID()) {
				t.Errorf("sent %v, the other server has %s", sent, got.JSON())
			}
			again := decoded(t)(api_account_object_create(thread, sl.NewBuiltin("test", nil), sl.Tuple{sl.String("m1"), object}, []sl.Tuple{{sl.String("key"), sl.String("chat|" + string(version))}}))
			if again["id"] != sent["id"] {
				t.Errorf("the same key sent again: %v", again)
			}
			if own := log.matching(func(e map[string]any) bool { return e["id"] == sent["id"] }); len(own) > 0 {
				t.Errorf("the user's own message was told back: %v", own)
			}
			// Messages share the state before them.
			count := func() int64 {
				row, _ := federation_database().row("select count(*) as n from snapshots where room=?", room)
				return row_int(row, "n")
			}
			snapshots := count()
			for i := range 3 {
				event := r.event(room, bob, "m.room.message", nil, map[string]any{"msgtype": "m.text", "body": fmt.Sprintf("more %d", i)})
				r.push(t, fmt.Sprintf("more%s-%d", version, i), event)
			}
			object.SetKey(sl.String("content"), sl.String("and more"))
			decoded(t)(api_account_object_create(thread, sl.NewBuiltin("test", nil), sl.Tuple{sl.String("m1"), object}, nil))
			if after := count(); after != snapshots {
				t.Errorf("snapshots went from %d to %d over four messages", snapshots, after)
			}
		})
	}
}

// TestMatrixTransactions: what another server sends is checked: a
// transaction is taken once; an event its auth events do not allow is
// rejected and not shown; events missing before one are asked for, and
// where they cannot be had the state at them is; branches are resolved by
// the room version's algorithm; and an event the room has moved past is
// kept but not shown or built on.
func TestMatrixTransactions(t *testing.T) {
	alice, log := create_matrix_test_env(t)
	r := create_matrix_remote(t, gomatrixserverlib.RoomVersionV10)
	room := r.room("Checks", "invite")
	bob, carol := r.user("bob"), r.user("carol")
	matrix_join(t, r, alice, room)
	message := func(sender string, body string, prevs []string, state map[string]string) gomatrixserverlib.PDU {
		r.lock.Lock()
		defer r.lock.Unlock()
		return r.build(room, sender, "m.room.message", nil, map[string]any{"msgtype": "m.text", "body": body}, prevs, state)
	}
	shown := func(body string) bool {
		return len(log.matching(func(e map[string]any) bool { return e["kind"] == "message" && e["body"] == body })) > 0
	}

	// A transaction taken once: what a repeat carries is not looked at.
	once := r.event(room, bob, "m.room.message", nil, map[string]any{"msgtype": "m.text", "body": "once"})
	r.push(t, "same", once)
	later := r.event(room, bob, "m.room.message", nil, map[string]any{"msgtype": "m.text", "body": "repeat"})
	if results := r.push(t, "same", later); len(results) != 0 || shown("repeat") {
		t.Errorf("a repeated transaction was taken: %v", results)
	}
	r.push(t, "after", later)
	if !shown("repeat") {
		t.Error("the event was not taken in a new transaction")
	}

	// Rejected: carol is not in the room.
	r.lock.Lock()
	edge, state := slices.Clone(r.edge[room]), maps.Clone(r.state[room])
	r.lock.Unlock()
	intruder := message(carol, "let me in", edge, state)
	results := r.push(t, "intruder", intruder)
	if result, _ := results[intruder.EventID()].(map[string]any); result["error"] == nil || shown("let me in") || !rooms_rejected(intruder.EventID()) {
		t.Errorf("an event from someone not in the room: %v", results)
	}

	// Rejected by its own auth events, which leave out bob's membership,
	// though the room's state allows him.
	r.lock.Lock()
	edge, state = slices.Clone(r.edge[room]), maps.Clone(r.state[room])
	unauthorised := r.forge(room, bob, "no membership named", edge, []string{state[rooms_key(spec.MRoomCreate, "")], state[rooms_key(spec.MRoomPowerLevels, "")]})
	r.lock.Unlock()
	results = r.push(t, "unauthorised", unauthorised)
	if result, _ := results[unauthorised.EventID()].(map[string]any); result["error"] == nil || shown("no membership named") {
		t.Errorf("an event its auth events do not allow: %v", results)
	}

	// Missing events asked for.
	gap1 := r.event(room, bob, "m.room.message", nil, map[string]any{"msgtype": "m.text", "body": "missed"})
	gap2 := r.event(room, bob, "m.room.message", nil, map[string]any{"msgtype": "m.text", "body": "after the missed"})
	r.push(t, "gap", gap2)
	if !shown("missed") || !shown("after the missed") {
		t.Error("the missing event was not fetched")
	}
	if rooms_point(gap1.EventID()) == false {
		t.Error("the fetched event has no state")
	}

	// Where they cannot be had, the state at them is.
	r.missing = true
	hole := r.event(room, bob, "m.room.message", nil, map[string]any{"msgtype": "m.text", "body": "never sent"})
	beyond := r.event(room, bob, "m.room.message", nil, map[string]any{"msgtype": "m.text", "body": "beyond the hole"})
	r.push(t, "hole", beyond)
	r.missing = false
	if !shown("beyond the hole") || shown("never sent") || !rooms_point(hole.EventID()) {
		t.Error("the event after a hole was not taken from the state at it")
	}

	// Branches: two names set on parallel branches, the later winning.
	r.lock.Lock()
	edge, state = slices.Clone(r.edge[room]), maps.Clone(r.state[room])
	empty := ""
	alpha := r.build(room, bob, "m.room.name", &empty, map[string]any{"name": "Alpha"}, edge, state)
	time.Sleep(5 * time.Millisecond)
	beta := r.build(room, bob, "m.room.name", &empty, map[string]any{"name": "Beta"}, edge, state)
	r.events[alpha.EventID()], r.events[beta.EventID()] = alpha, beta
	r.before[alpha.EventID()], r.before[beta.EventID()] = state, state
	r.lock.Unlock()
	r.push(t, "beta", beta)
	r.push(t, "alpha", alpha)
	current, _ := rooms_current(room)
	if current[rooms_key("m.room.name", "")] != beta.EventID() {
		t.Errorf("resolved name = %s, want the later branch's %s", current[rooms_key("m.room.name", "")], beta.EventID())
	}
	for _, branch := range []string{alpha.EventID(), beta.EventID()} {
		if found, _ := federation_database().exists("select 1 from extremities where room=? and event=?", room, branch); !found {
			t.Errorf("branch %s is not at the room's edge", branch)
		}
	}

	// Soft-failed: carol joins and is banned, and a message she sent from
	// before the ban arrives after it.
	r.lock.Lock()
	r.edge[room] = []string{alpha.EventID(), beta.EventID()}
	r.state[room][rooms_key("m.room.name", "")] = beta.EventID()
	r.lock.Unlock()
	invite := r.event(room, bob, spec.MRoomMember, &carol, map[string]any{"membership": "invite"})
	join := r.event(room, carol, spec.MRoomMember, &carol, map[string]any{"membership": "join"})
	r.push(t, "carol", invite, join)
	r.lock.Lock()
	edge, state = slices.Clone(r.edge[room]), maps.Clone(r.state[room])
	r.lock.Unlock()
	ban := r.event(room, bob, spec.MRoomMember, &carol, map[string]any{"membership": "ban"})
	r.push(t, "ban", ban)
	// Rejected by the state before it: carol names her join among its auth
	// events, but follows her ban.
	r.lock.Lock()
	banned := r.forge(room, carol, "after the ban", []string{ban.EventID()}, []string{r.state[room][rooms_key(spec.MRoomCreate, "")], r.state[room][rooms_key(spec.MRoomPowerLevels, "")], join.EventID()})
	r.lock.Unlock()
	results = r.push(t, "banned", banned)
	if result, _ := results[banned.EventID()].(map[string]any); result["error"] == nil || shown("after the ban") || !rooms_rejected(banned.EventID()) {
		t.Errorf("an event the state before it does not allow: %v", results)
	}
	late := message(carol, "from before the ban", edge, state)
	r.push(t, "late", late)
	if shown("from before the ban") || rooms_known(late.EventID()) || rooms_rejected(late.EventID()) {
		t.Error("a message the room has moved past was shown or taken into the timeline")
	}
	if found, _ := federation_database().exists("select 1 from extremities where event=?", late.EventID()); found {
		t.Error("a soft-failed event is built on")
	}
	if rooms_event(late.EventID()) == nil {
		t.Error("a soft-failed event is not kept")
	}
}

// TestMatrixOutbound: what the user does reaches the other server, which a
// server that fails waits out with a backoff before it is sent again; edits
// and redactions follow; and leaving is a leave event.
func TestMatrixOutbound(t *testing.T) {
	alice, _ := create_matrix_test_env(t)
	r := create_matrix_remote(t, gomatrixserverlib.RoomVersionV10)
	room := r.room("Outbound", "invite")
	matrix_join(t, r, alice, room)
	thread := linked_thread(alice)
	fn := sl.NewBuiltin("test", nil)
	object := sl.NewDict(2)
	object.SetKey(sl.String("conversation"), sl.String(room))
	object.SetKey(sl.String("content"), sl.String("first"))
	sent := decoded(t)(api_account_object_create(thread, fn, sl.Tuple{sl.String("m1"), object}, nil))
	wait(t, "the first message", func() bool { return len(r.received_of("m.room.message", "@alice:example.com")) == 1 })

	edit := sl.NewDict(2)
	edit.SetKey(sl.String("conversation"), sl.String(room))
	edit.SetKey(sl.String("content"), sl.String("first, edited"))
	decoded(t)(api_account_object_update(thread, fn, sl.Tuple{sl.String("m1"), sl.String(sent["id"].(string)), edit}, nil))
	wait(t, "the edit", func() bool { return len(r.received_of("m.room.message", "@alice:example.com")) == 2 })
	if replaced := r.received_of("m.room.message", "@alice:example.com")[1]; !strings.Contains(string(replaced.Content()), "m.replace") || !strings.Contains(string(replaced.Content()), sent["id"].(string)) {
		t.Errorf("edit = %s", replaced.JSON())
	}
	if answer, err := api_account_object_delete(thread, fn, sl.Tuple{sl.String("m1"), sl.String(sent["id"].(string))}, []sl.Tuple{{sl.String("conversation"), sl.String(room)}}); err != nil || answer != sl.True {
		t.Fatalf("delete = %v %v", answer, err)
	}
	wait(t, "the redaction", func() bool { return len(r.received_of(spec.MRoomRedaction, "@alice:example.com")) == 1 })
	if redaction := r.received_of(spec.MRoomRedaction, "@alice:example.com")[0]; redaction.Redacts() != sent["id"] {
		t.Errorf("version 10 redaction names %q beside its content: %s", redaction.Redacts(), redaction.JSON())
	}
	if !rooms_event(sent["id"].(string)).Redacted() {
		t.Error("the redacted message is kept whole")
	}

	// A failing server waits.
	r.lock.Lock()
	r.fail = true
	r.lock.Unlock()
	object.SetKey(sl.String("content"), sl.String("while down"))
	decoded(t)(api_account_object_create(thread, fn, sl.Tuple{sl.String("m1"), object}, nil))
	db := federation_database()
	wait(t, "the backoff", func() bool {
		row, _ := db.row("select failures, retry from destinations where name=?", string(r.name))
		return row != nil && row_int(row, "failures") == 1 && row_int(row, "retry") > now()
	})
	if count, _ := db.row("select count(*) as n from outbox where destination=?", string(r.name)); row_int(count, "n") != 1 {
		t.Errorf("queued = %v, want the message kept", count)
	}
	r.lock.Lock()
	r.fail = false
	r.lock.Unlock()
	db.exec("update destinations set retry=0 where name=?", string(r.name))
	object.SetKey(sl.String("content"), sl.String("back up"))
	decoded(t)(api_account_object_create(thread, fn, sl.Tuple{sl.String("m1"), object}, nil))
	wait(t, "the queue sent", func() bool {
		count, _ := db.row("select count(*) as n from outbox where destination=?", string(r.name))
		return row_int(count, "n") == 0 && len(r.received_of("m.room.message", "@alice:example.com")) == 4
	})
	if row, _ := db.row("select 1 from destinations where name=?", string(r.name)); row != nil {
		t.Error("the backoff outlived a success")
	}

	if _, err := api_account_room_leave(thread, fn, sl.Tuple{sl.String("m1"), sl.String(room)}, nil); err != nil {
		t.Fatal(err)
	}
	wait(t, "the leave", func() bool { return len(r.received_of(spec.MRoomMember, "@alice:example.com")) == 1 })
	if membership, _ := r.received_of(spec.MRoomMember, "@alice:example.com")[0].Membership(); membership != spec.Leave {
		t.Errorf("leave = %s", membership)
	}
}

// TestMatrixRooms: across room versions, a room this server makes invites
// someone elsewhere through their server; they join through the engine,
// which answers the room's state, history, missing events and single events
// to a server in the room and none of them to one that is not; and they
// leave through it.
func TestMatrixRooms(t *testing.T) {
	alice, log := create_matrix_test_env(t)
	for _, version := range []gomatrixserverlib.RoomVersion{"10", "11", "12"} {
		t.Run("v"+string(version), func(t *testing.T) {
			rooms_version_default = version
			r := create_matrix_remote(t, version)
			stranger := create_matrix_remote(t, version)
			bob := r.user("bob")
			thread := linked_thread(alice)
			fn := sl.NewBuiltin("test", nil)
			people := sl.NewList([]sl.Value{sl.String(bob)})
			made := decoded(t)(api_account_room_create(thread, fn, sl.Tuple{sl.String("m1"), people}, nil))
			room, _ := made["conversation"].(string)
			if room == "" || made["room"] != false || fmt.Sprint(made["people"]) != fmt.Sprint([]any{map[string]any{"id": bob, "name": "Bob"}}) {
				t.Fatalf("create = %v", made)
			}
			if version == "12" && strings.Contains(room, ":") {
				t.Errorf("a version 12 room id names a server: %s", room)
			}
			r.lock.Lock()
			invited := len(r.invited)
			r.lock.Unlock()
			if invited != 1 {
				t.Fatalf("invitations sent = %d", invited)
			}
			if !rooms_direct("@alice:example.com", room) {
				t.Error("the one-to-one room is not marked so")
			}
			object := sl.NewDict(2)
			object.SetKey(sl.String("conversation"), sl.String(room))
			object.SetKey(sl.String("content"), sl.String("before you came"))
			before := decoded(t)(api_account_object_create(thread, fn, sl.Tuple{sl.String("m1"), object}, nil))

			// Bob joins through the engine.
			status, body := r.call(t, "GET", "/_matrix/federation/v1/make_join/"+url.PathEscape(room)+"/"+url.PathEscape(bob)+"?ver="+string(version), nil)
			if status != 200 {
				t.Fatalf("make_join = %d %v", status, body)
			}
			var template gomatrixserverlib.ProtoEvent
			json.Unmarshal(mustjson(body["event"]), &template)
			template.SetContent(map[string]any{"membership": "join", "displayname": "Bob"})
			join, err := r.version.NewEventBuilderFromProtoEvent(&template).Build(time.Now(), r.name, r.key, r.private)
			if err != nil {
				t.Fatal(err)
			}
			status, body = r.call(t, "PUT", "/_matrix/federation/v2/send_join/"+url.PathEscape(room)+"/"+url.PathEscape(join.EventID()), json.RawMessage(join.JSON()))
			if status != 200 {
				t.Fatalf("send_join = %d %v", status, body)
			}
			body_join := body
			state, _ := body["state"].([]any)
			if len(state) < 5 || len(body["auth_chain"].([]any)) == 0 {
				t.Errorf("send_join state = %d events", len(state))
			}
			if membership := rooms_membership(room, bob); membership != spec.Join {
				t.Errorf("bob's membership = %q", membership)
			}
			if member := log.find(t, "member", func(e map[string]any) bool { return e["conversation"] == room && e["id"] == bob }); member["left"] != false {
				t.Errorf("member = %v", member)
			}

			// What a server in the room may read.
			status, body = r.call(t, "GET", "/_matrix/federation/v1/backfill/"+url.PathEscape(room)+"?v="+url.QueryEscape(join.EventID())+"&limit=20", nil)
			if status != 200 || !strings.Contains(string(mustjson(body["pdus"])), "before you came") {
				t.Errorf("backfill = %d %v", status, body)
			}
			status, body = r.call(t, "GET", "/_matrix/federation/v1/event/"+url.PathEscape(before["id"].(string)), nil)
			if status != 200 || len(body["pdus"].([]any)) != 1 {
				t.Errorf("event = %d %v", status, body)
			}
			status, body = r.call(t, "GET", "/_matrix/federation/v1/state_ids/"+url.PathEscape(room)+"?event_id="+url.QueryEscape(join.EventID()), nil)
			if status != 200 || len(body["pdu_ids"].([]any)) < 5 {
				t.Errorf("state_ids = %d %v", status, body)
			}
			var create string
			current, _ := rooms_current(room)
			create = current[rooms_key(spec.MRoomCreate, "")]
			status, body = r.call(t, "POST", "/_matrix/federation/v1/get_missing_events/"+url.PathEscape(room), map[string]any{"earliest_events": []string{create}, "latest_events": []string{join.EventID()}, "limit": 50})
			if status != 200 || !strings.Contains(string(mustjson(body["events"])), "before you came") {
				t.Errorf("missing events = %d %v", status, body)
			}
			status, body = r.call(t, "GET", "/_matrix/federation/v1/event_auth/"+url.PathEscape(room)+"/"+url.PathEscape(join.EventID()), nil)
			if status != 200 || len(body["auth_chain"].([]any)) == 0 {
				t.Errorf("event_auth = %d %v", status, body)
			}
			// And one not in it may not.
			if status, _ := stranger.call(t, "GET", "/_matrix/federation/v1/event/"+url.PathEscape(before["id"].(string)), nil); status != 404 {
				t.Errorf("event for a server not in the room = %d", status)
			}
			if status, body := stranger.call(t, "GET", "/_matrix/federation/v1/backfill/"+url.PathEscape(room)+"?v="+url.QueryEscape(join.EventID())+"&limit=20", nil); status != 200 || len(body["pdus"].([]any)) != 0 {
				t.Errorf("backfill for a server not in the room = %d %v", status, body)
			}
			if status, _ := stranger.call(t, "GET", "/_matrix/federation/v1/make_join/"+url.PathEscape(room)+"/"+url.PathEscape(stranger.user("eve"))+"?ver="+string(version), nil); status != 403 {
				t.Errorf("make_join uninvited = %d, want 403", status)
			}
			if status, _ := stranger.call(t, "GET", "/_matrix/federation/v1/make_join/"+url.PathEscape(room)+"/"+url.PathEscape(bob)+"?ver="+string(version), nil); status != 403 {
				t.Errorf("make_join for another server's user = %d, want 403", status)
			}

			// Bob's message, and his leaving.
			r.adopt(room, body_join)
			r.lock.Lock()
			r.take(join)
			r.lock.Unlock()
			hello := r.event(room, bob, "m.room.message", nil, map[string]any{"msgtype": "m.text", "body": "hello alice " + string(version)})
			r.push(t, "hello"+string(version), hello)
			if message := log.find(t, "message", func(e map[string]any) bool { return e["id"] == hello.EventID() }); message["from"] != bob || message["room"] != false {
				t.Errorf("message in the one-to-one room = %v", message)
			}
			status, body = r.call(t, "GET", "/_matrix/federation/v1/make_leave/"+url.PathEscape(room)+"/"+url.PathEscape(bob), nil)
			if status != 200 {
				t.Fatalf("make_leave = %d %v", status, body)
			}
			json.Unmarshal(mustjson(body["event"]), &template)
			template.SetContent(map[string]any{"membership": "leave"})
			leave, _ := r.version.NewEventBuilderFromProtoEvent(&template).Build(time.Now(), r.name, r.key, r.private)
			if status, body := r.call(t, "PUT", "/_matrix/federation/v2/send_leave/"+url.PathEscape(room)+"/"+url.PathEscape(leave.EventID()), json.RawMessage(leave.JSON())); status != 200 {
				t.Fatalf("send_leave = %d %v", status, body)
			}
			if member := log.find(t, "member", func(e map[string]any) bool { return e["conversation"] == room && e["id"] == bob && e["left"] == true }); member == nil {
				t.Error("the leave was not told")
			}
		})
	}
}

// TestMatrixGroup: a room for several people is named and invites each; an
// address no one is at fails, making nothing.
func TestMatrixGroup(t *testing.T) {
	alice, _ := create_matrix_test_env(t)
	r := create_matrix_remote(t, gomatrixserverlib.RoomVersionV10)
	thread := linked_thread(alice)
	fn := sl.NewBuiltin("test", nil)
	people := sl.NewList([]sl.Value{sl.String(r.user("bob")), sl.String("matrix:u/bob:" + string(r.name))})
	made := decoded(t)(api_account_room_create(thread, fn, sl.Tuple{sl.String("m1"), people}, []sl.Tuple{{sl.String("name"), sl.String("Group")}}))
	if made["room"] != false || len(made["people"].([]any)) != 1 {
		t.Errorf("the same person twice = %v", made)
	}
	people = sl.NewList([]sl.Value{sl.String(r.user("bob")), sl.String(r.user("nobody"))})
	if answer, err := api_account_room_create(thread, fn, sl.Tuple{sl.String("m1"), people}, []sl.Tuple{{sl.String("name"), sl.String("Group")}}); err != nil || answer != sl.None {
		t.Errorf("an address no one is at = %v %v, want None", answer, err)
	}
	rows, _ := federation_database().rows("select id from rooms")
	if len(rows) != 1 {
		t.Errorf("rooms = %d, want only the first", len(rows))
	}
	room := row_string(rows[0], "id")
	current, _ := rooms_current(room)
	if current[rooms_key("m.room.name", "")] != "" {
		t.Error("a one-to-one room was named")
	}
	people = sl.NewList([]sl.Value{sl.String(r.user("bob")), sl.String("@alice:example.com")})
	made = decoded(t)(api_account_room_create(thread, fn, sl.Tuple{sl.String("m1"), people}, []sl.Tuple{{sl.String("name"), sl.String("Group")}}))
	group := made["conversation"].(string)
	current, _ = rooms_current(group)
	name := rooms_event(current[rooms_key("m.room.name", "")])
	if made["room"] != false || name != nil {
		t.Errorf("the user among the people = %v (named %v)", made, name)
	}
	r.lock.Lock()
	invited := len(r.invited)
	r.lock.Unlock()
	if invited != 2 {
		t.Errorf("invitations = %d", invited)
	}
}

// TestMatrixDecline: declining an invitation to a room this server is not in
// is a leave sent through the server it came from.
func TestMatrixDecline(t *testing.T) {
	alice, log := create_matrix_test_env(t)
	r := create_matrix_remote(t, gomatrixserverlib.RoomVersionV10)
	room := r.room("Unwanted", "invite")
	invitee := "@alice:example.com"
	invite := r.event(room, r.user("bob"), spec.MRoomMember, &invitee, map[string]any{"membership": "invite", "is_direct": true})
	request, _ := fclient.NewInviteV2Request(invite, nil)
	if status, body := r.call(t, "PUT", "/_matrix/federation/v2/invite/"+url.PathEscape(room)+"/"+url.PathEscape(invite.EventID()), request); status != 200 {
		t.Fatalf("invite = %d %v", status, body)
	}
	if told := log.find(t, "invite", nil); told["direct"] != true {
		t.Errorf("invite = %v", told)
	}
	if _, err := api_account_room_leave(linked_thread(alice), sl.NewBuiltin("test", nil), sl.Tuple{sl.String("m1"), sl.String(room)}, nil); err != nil {
		t.Fatal(err)
	}
	leaves := r.received_of(spec.MRoomMember, invitee)
	if len(leaves) != 1 {
		t.Fatalf("leaves = %d", len(leaves))
	}
	if membership, _ := leaves[0].Membership(); membership != spec.Leave {
		t.Errorf("membership = %s", membership)
	}
	if found, _ := federation_database().exists("select 1 from invites where room=?", room); found {
		t.Error("the declined invitation is kept")
	}
	// An invitation from a server the user blocks is refused.
	db_user(alice, "user").exec("insert into blocks (domain, created) values ('127.0.0.1', 1)")
	blocked := r.event(room, r.user("bob"), spec.MRoomMember, &invitee, map[string]any{"membership": "invite"})
	request, _ = fclient.NewInviteV2Request(blocked, nil)
	if status, _ := r.call(t, "PUT", "/_matrix/federation/v2/invite/"+url.PathEscape(room)+"/"+url.PathEscape(blocked.EventID()), request); status != 403 {
		t.Errorf("invitation from a server the user blocks = %d, want 403", status)
	}
	db_user(alice, "user").exec("delete from blocks")
	// An invitation for someone not here is refused.
	other := "@nobody:example.com"
	stray := r.event(room, r.user("bob"), spec.MRoomMember, &other, map[string]any{"membership": "invite"})
	request, _ = fclient.NewInviteV2Request(stray, nil)
	if status, _ := r.call(t, "PUT", "/_matrix/federation/v2/invite/"+url.PathEscape(room)+"/"+url.PathEscape(stray.EventID()), request); status != 404 {
		t.Errorf("invitation of no one = %d, want 404", status)
	}
}

// TestMatrixAlias: a room is joined by an alias its server answers for.
func TestMatrixAlias(t *testing.T) {
	alice, _ := create_matrix_test_env(t)
	r := create_matrix_remote(t, gomatrixserverlib.RoomVersionV10)
	room := r.room("Public", "public")
	r.lock.Lock()
	r.aliases["#hall:"+string(r.name)] = room
	r.lock.Unlock()
	joined := decoded(t)(api_account_room_join(linked_thread(alice), sl.NewBuiltin("test", nil), sl.Tuple{sl.String("m1"), sl.String("matrix:r/hall:" + string(r.name))}, nil))
	if joined["conversation"] != room || rooms_membership(room, "@alice:example.com") != spec.Join {
		t.Errorf("join by alias = %v", joined)
	}
	if answer, err := api_account_room_join(linked_thread(alice), sl.NewBuiltin("test", nil), sl.Tuple{sl.String("m1"), sl.String("#nowhere:" + string(r.name))}, nil); err != nil || answer != sl.None {
		t.Errorf("an alias no room has = %v %v", answer, err)
	}
}

// TestMatrixRedactionVersions: a redaction names its event beside its content
// before version 11 and in it from then.
func TestMatrixRedactionVersions(t *testing.T) {
	alice, _ := create_matrix_test_env(t)
	r := create_matrix_remote(t, gomatrixserverlib.RoomVersionV11)
	rooms_version_default = gomatrixserverlib.RoomVersionV11
	thread := linked_thread(alice)
	fn := sl.NewBuiltin("test", nil)
	made := decoded(t)(api_account_room_create(thread, fn, sl.Tuple{sl.String("m1"), sl.NewList([]sl.Value{sl.String(r.user("bob"))})}, nil))
	room := made["conversation"].(string)
	object := sl.NewDict(2)
	object.SetKey(sl.String("conversation"), sl.String(room))
	object.SetKey(sl.String("content"), sl.String("soon gone"))
	sent := decoded(t)(api_account_object_create(thread, fn, sl.Tuple{sl.String("m1"), object}, nil))
	api_account_object_delete(thread, fn, sl.Tuple{sl.String("m1"), sl.String(sent["id"].(string))}, []sl.Tuple{{sl.String("conversation"), sl.String(room)}})
	rows, _ := federation_database().rows("select body from events where room=? and type='m.room.redaction'", room)
	if len(rows) != 1 {
		t.Fatalf("redactions = %d", len(rows))
	}
	var redaction struct {
		Redacts *string        `json:"redacts"`
		Content map[string]any `json:"content"`
	}
	json.Unmarshal([]byte(row_string(rows[0], "body")), &redaction)
	if redaction.Redacts != nil || redaction.Content["redacts"] != sent["id"] {
		t.Errorf("version 11 redaction = %s", row_string(rows[0], "body"))
	}
}

// TestMatrixResolve: a server name with a port or an address is reached as
// it is, one without at its SRV record (the current service before the old
// one), else on 8448, its certificate and Host its own name throughout.
func TestMatrixResolve(t *testing.T) {
	create_matrix_test_env(t)
	saved := federation_lookup
	defer func() { federation_lookup = saved }()
	var asked []string
	federation_lookup = func(_ context.Context, service string, name string) ([]*net.SRV, error) {
		asked = append(asked, service+" "+name)
		if name == "srv.invalid" && service == "matrix-fed" {
			return []*net.SRV{{Target: "host.srv.invalid.", Port: 8443}}, nil
		}
		if name == "old.invalid" && service == "matrix" {
			return []*net.SRV{{Target: "host.old.invalid.", Port: 9000}}, nil
		}
		return nil, fmt.Errorf("none")
	}
	for name, want := range map[string]federation_target{
		"example.invalid:1234": {address: "example.invalid:1234", tls: "example.invalid", host: "example.invalid:1234"},
		"192.0.2.1":            {address: "192.0.2.1:8448", tls: "192.0.2.1", host: "192.0.2.1"},
		"srv.invalid":          {address: "host.srv.invalid:8443", tls: "srv.invalid", host: "srv.invalid"},
		"old.invalid":          {address: "host.old.invalid:9000", tls: "old.invalid", host: "old.invalid"},
		"plain.invalid":        {address: "plain.invalid:8448", tls: "plain.invalid", host: "plain.invalid"},
	} {
		got, err := federation_resolve_fresh(t.Context(), name)
		if err != nil || got.address != want.address || got.tls != want.tls || got.host != want.host {
			t.Errorf("%s = %+v %v, want %+v", name, got, err, want)
		}
	}
	if !slices.Contains(asked, "matrix-fed old.invalid") || slices.Index(asked, "matrix-fed old.invalid") > slices.Index(asked, "matrix old.invalid") {
		t.Errorf("services asked = %v, want the current one first", asked)
	}
}

// TestMatrixBackoff: a failing server waits thirty seconds, doubling to a
// day.
func TestMatrixBackoff(t *testing.T) {
	for failures, want := range map[int64]int64{1: 30, 2: 60, 3: 120, 6: 960, 20: 86400, 100: 86400} {
		if got := federation_backoff(failures); got != want {
			t.Errorf("backoff after %d = %d, want %d", failures, got, want)
		}
	}
}

// TestMatrixHostedFunctions: what a hosted account does not do yet answers
// None without failing the app; a hosted fediverse account has no
// conversations.
func TestMatrixHostedFunctions(t *testing.T) {
	alice, _ := create_matrix_test_env(t)
	thread := linked_thread(alice)
	fn := sl.NewBuiltin("test", nil)
	for name, call := range map[string]func() (sl.Value, error){
		"typing": func() (sl.Value, error) {
			return api_account_typing(thread, fn, sl.Tuple{sl.String("m1"), sl.String("!r:x")}, nil)
		},
		"read": func() (sl.Value, error) {
			return api_account_read(thread, fn, sl.Tuple{sl.String("m1"), sl.String("!r:x"), sl.String("$e")}, nil)
		},
		"presence": func() (sl.Value, error) {
			return api_account_presence(thread, fn, sl.Tuple{sl.String("m1"), sl.False}, nil)
		},
		"upload": func() (sl.Value, error) {
			return api_account_upload(thread, fn, sl.Tuple{sl.String("m1"), sl.Bytes("png"), sl.String("image/png")}, nil)
		},
		"message to a room not joined": func() (sl.Value, error) {
			object := sl.NewDict(2)
			object.SetKey(sl.String("conversation"), sl.String("!r:x"))
			object.SetKey(sl.String("content"), sl.String("hi"))
			return api_account_object_create(thread, fn, sl.Tuple{sl.String("m1"), object}, nil)
		},
	} {
		if answer, err := call(); err != nil || answer != sl.None {
			t.Errorf("%s = %v %v, want None", name, answer, err)
		}
	}
	if _, err := api_account_room_create(thread, fn, sl.Tuple{sl.String("f1"), sl.NewList([]sl.Value{sl.String("@bob:x")})}, nil); err == nil || !strings.Contains(err.Error(), "no conversations") {
		t.Errorf("room on a hosted fediverse account = %v", err)
	}
}

// TestMatrixLocal: two accounts here share a room the engine keeps: the
// invitation reaches the other's apps, joining is the room's own event and
// brings its history from what this server keeps, and each message reaches
// the other.
func TestMatrixLocal(t *testing.T) {
	alice, log := create_matrix_test_env(t)
	root := user_by_uid("u-root")
	db_user(root, "user").exec("insert into accounts (id, type, label, identifier, data, created, verified, enabled) values ('m2', 'matrix', 'Root', '@root:example.com', ?, 1, 1, 1)", json_encode(map[string]any{"hosted": true, "handle": "root", "domain": "example.com"}))
	actor_register(root, "m2", "matrix", "example.com", "root")
	fn := sl.NewBuiltin("test", nil)
	thread := linked_thread(alice)
	made := decoded(t)(api_account_room_create(thread, fn, sl.Tuple{sl.String("m1"), sl.NewList([]sl.Value{sl.String("@root:example.com")})}, nil))
	room := made["conversation"].(string)
	if fmt.Sprint(made["people"]) != fmt.Sprint([]any{map[string]any{"id": "@root:example.com", "name": "Root"}}) {
		t.Errorf("people = %v", made["people"])
	}
	invite := log.find(t, "invite", func(e map[string]any) bool { return e["account"] == "m2" })
	if invite["conversation"] != room || invite["from"] != "@alice:example.com" || invite["direct"] != true {
		t.Errorf("invite = %v", invite)
	}
	object := sl.NewDict(2)
	object.SetKey(sl.String("conversation"), sl.String(room))
	object.SetKey(sl.String("content"), sl.String("before root came"))
	decoded(t)(api_account_object_create(thread, fn, sl.Tuple{sl.String("m1"), object}, nil))
	rooted := linked_thread(root)
	joined := decoded(t)(api_account_room_join(rooted, fn, sl.Tuple{sl.String("m2"), sl.String(room)}, nil))
	if joined["conversation"] != room || rooms_membership(room, "@root:example.com") != spec.Join {
		t.Fatalf("join = %v", joined)
	}
	if summary := log.find(t, "room", func(e map[string]any) bool { return e["account"] == "m2" }); summary["direct"] != true || summary["room"] != false {
		t.Errorf("room = %v", summary)
	}
	if history := log.find(t, "message", func(e map[string]any) bool { return e["account"] == "m2" }); history["body"] != "before root came" || history["from"] != "@alice:example.com" || history["name"] != "Alice" {
		t.Errorf("history = %v", history)
	}
	if member := log.find(t, "member", func(e map[string]any) bool { return e["account"] == "m1" && e["id"] == "@root:example.com" }); member["left"] != false || member["name"] != "Root" {
		t.Errorf("alice told of root = %v", member)
	}
	object.SetKey(sl.String("content"), sl.String("hello alice"))
	reply := decoded(t)(api_account_object_create(rooted, fn, sl.Tuple{sl.String("m2"), object}, nil))
	if message := log.find(t, "message", func(e map[string]any) bool { return e["id"] == reply["id"] }); message["account"] != "m1" || message["from"] != "@root:example.com" {
		t.Errorf("message = %v", message)
	}
}

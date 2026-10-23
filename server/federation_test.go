// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"net/url"
	"os"
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
	edus     []gomatrixserverlib.EDU
	media    map[string][]byte
	fetched  int
	invited  []gomatrixserverlib.PDU
	asked    []string
	fail     bool
	missing  bool
	// What the remote answers a key query and a key claim with, as its
	// users' keys (keyring_test.go).
	keys   map[string]any
	claims map[string]any
	// The key queries and claims it was sent.
	queries []json.RawMessage
}

func create_matrix_remote(t *testing.T, version gomatrixserverlib.RoomVersion) *matrix_remote {
	t.Helper()
	public, private, _ := ed25519.GenerateKey(rand.Reader)
	implementation, err := gomatrixserverlib.GetRoomVersion(version)
	if err != nil {
		t.Fatal(err)
	}
	r := &matrix_remote{t: t, key: "ed25519:remote", private: private, public: public, version: implementation,
		events: map[string]gomatrixserverlib.PDU{}, before: map[string]map[string]string{}, state: map[string]map[string]string{}, edge: map[string][]string{}, aliases: map[string]string{}, media: map[string][]byte{}}
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
			Previous any `json:"prev_events"`
			Auth     any `json:"auth_events"`
		}
		json.Unmarshal(event.JSON(), &references)
		proto.PrevEvents, proto.AuthEvents = references.Previous, references.Auth
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
	// Where a download redirects to, as a server with a separate file store
	// answers.
	if req.URL.Path == "/files/moved" {
		w.Header().Set("Content-Type", "image/gif")
		w.Write([]byte("moved picture"))
		return
	}
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
			PDUs *[]json.RawMessage      `json:"pdus"`
			EDUs []gomatrixserverlib.EDU `json:"edus"`
		}
		json.Unmarshal(request.Content(), &transaction)
		// As Synapse does: a transaction lists its room events, if none.
		if transaction.PDUs == nil {
			r.answer(w, 400, map[string]any{"error": "Invalid transaction"})
			return
		}
		r.edus = append(r.edus, transaction.EDUs...)
		for _, raw := range *transaction.PDUs {
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
	case route == "GET v1/make_knock":
		r.answer(w, 200, map[string]any{"event": r.template(parts[2], parts[3], "knock"), "room_version": string(r.version.Version())})
	case route == "PUT v1/send_knock":
		event, err := r.version.NewEventFromUntrustedJSON(request.Content())
		if err != nil {
			r.answer(w, 400, map[string]any{"errcode": "M_BAD_JSON"})
			return
		}
		r.received = append(r.received, event)
		r.take(event)
		name := r.events[r.state[parts[2]][rooms_key("m.room.name", "")]]
		r.answer(w, 200, map[string]any{"knock_room_state": []gomatrixserverlib.InviteStrippedState{gomatrixserverlib.NewInviteStrippedState(name)}})
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
		names := map[string]string{r.user("bob"): "Bob", r.user("carol"): "Carol"}
		name, found := names[req.URL.Query().Get("user_id")]
		if !found {
			r.answer(w, 404, map[string]any{"errcode": "M_NOT_FOUND"})
			return
		}
		r.answer(w, 200, map[string]any{"displayname": name})
	case route == "GET v1/media" && parts[2] == "download":
		data, found := r.media[parts[3]]
		if !found {
			r.answer(w, 404, map[string]any{"errcode": "M_NOT_FOUND"})
			return
		}
		r.fetched++
		var body bytes.Buffer
		writer := multipart.NewWriter(&body)
		metadata, _ := writer.CreatePart(textproto.MIMEHeader{"Content-Type": {"application/json"}})
		metadata.Write([]byte("{}"))
		if string(data) == "moved" {
			writer.CreatePart(textproto.MIMEHeader{"Location": {r.server.URL + "/files/moved"}})
		} else {
			part, _ := writer.CreatePart(textproto.MIMEHeader{"Content-Type": {"image/png"}})
			part.Write(data)
		}
		writer.Close()
		w.Header().Set("Content-Type", "multipart/mixed; boundary="+writer.Boundary())
		w.Write(body.Bytes())
	case route == "POST v1/user" && len(parts) == 4 && parts[2] == "keys":
		r.queries = append(r.queries, json.RawMessage(request.Content()))
		answer := r.keys
		if parts[3] == "claim" {
			answer = r.claims
		}
		if answer == nil {
			answer = map[string]any{}
		}
		r.answer(w, 200, answer)
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

// edus_of answers the ephemeral events the remote was sent of a type.
func (r *matrix_remote) edus_of(kind string) []gomatrixserverlib.EDU {
	r.lock.Lock()
	defer r.lock.Unlock()
	var out []gomatrixserverlib.EDU
	for _, edu := range r.edus {
		if edu.Type == kind {
			out = append(out, edu)
		}
	}
	return out
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
	t.Cleanup(federation_workers_drain)
	return alice, log
}

// federation_workers_drain ends every destination's worker before the test's
// databases go: a worker ends only once its queue is empty, so one backing off
// from a remote the test has closed would otherwise wake in a later test.
func federation_workers_drain() {
	db := federation_database()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		db.exec("delete from outbox")
		db.exec("delete from destinations")
		federation_workers_lock.Lock()
		running := len(federation_workers)
		for _, wake := range federation_workers {
			select {
			case wake <- struct{}{}:
			default:
			}
		}
		federation_workers_lock.Unlock()
		if running == 0 {
			return
		}
	}
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

// TestMatrixAuthEvents: an event is checked against its own auth events as
// the auth rules consider them, wherever it arrives - in a transaction, in
// the auth chain fetched to check one, in the history fetched on joining and
// at a gap - and is rejected where one of them was itself rejected, where two
// name the same member, or where one is not among those its auth needs.
func TestMatrixAuthEvents(t *testing.T) {
	alice, log := create_matrix_test_env(t)
	r := create_matrix_remote(t, gomatrixserverlib.RoomVersionV10)
	room := r.room("Auth", "invite")
	bob, dave := r.user("bob"), r.user("dave")
	empty := ""
	invite := r.event(room, bob, spec.MRoomMember, &dave, map[string]any{"membership": "invite"})
	join := r.event(room, dave, spec.MRoomMember, &dave, map[string]any{"membership": "join"})
	shown := func(body string) bool {
		return len(log.matching(func(e map[string]any) bool { return e["kind"] == "message" && e["body"] == body })) > 0
	}

	// Dave, with no power, sets the power levels: the room rejects it, and
	// the remote serves it as an auth event of the events after.
	r.lock.Lock()
	create, levels, rules := r.state[room][rooms_key(spec.MRoomCreate, "")], r.state[room][rooms_key(spec.MRoomPowerLevels, "")], r.state[room][rooms_key(spec.MRoomJoinRules, "")]
	seized := r.build(room, dave, spec.MRoomPowerLevels, &empty, map[string]any{"users": map[string]any{dave: 100}}, r.edge[room], r.state[room])
	r.events[seized.EventID()] = seized
	history := r.forge(room, dave, "history on seized power", r.edge[room], []string{create, seized.EventID(), join.EventID()})
	r.take(history)
	r.lock.Unlock()
	r.event(room, bob, "m.room.message", nil, map[string]any{"msgtype": "m.text", "body": "history after it"})

	// History on joining: the message authorised by the rejected power
	// levels is not shown, the one after it is.
	matrix_join(t, r, alice, room)
	if !shown("history after it") {
		t.Fatal("CONTROL: the history was not fetched")
	}
	if !rooms_rejected(seized.EventID()) {
		t.Fatal("CONTROL: the power levels dave had no power to set were not rejected")
	}
	if shown("history on seized power") || !rooms_rejected(history.EventID()) {
		t.Error("THE POINT: history authorised by rejected power levels was shown")
	}

	refused := func(name string, event gomatrixserverlib.PDU, body string, why string) {
		t.Helper()
		results := r.push(t, name, event)
		if result, _ := results[event.EventID()].(map[string]any); result["error"] == nil || shown(body) || !rooms_rejected(event.EventID()) {
			t.Errorf("THE POINT: %s: %v", why, results)
		}
	}
	r.lock.Lock()
	edge, state := slices.Clone(r.edge[room]), maps.Clone(r.state[room])
	// In a transaction: a message naming the rejected power levels.
	direct := r.forge(room, dave, "on seized power", edge, []string{create, seized.EventID(), join.EventID()})
	// Fetched to check another: dave's membership on the rejected power
	// levels, which a message names in place of his join.
	seizing := maps.Clone(state)
	seizing[rooms_key(spec.MRoomPowerLevels, "")] = seized.EventID()
	rejoin := r.build(room, dave, spec.MRoomMember, &dave, map[string]any{"membership": "join", "displayname": "Dave"}, edge, seizing)
	r.events[rejoin.EventID()] = rejoin
	outlier := r.forge(room, dave, "on a rejected membership", edge, []string{create, levels, rejoin.EventID()})
	// Two memberships of dave's, the join last.
	twice := r.forge(room, dave, "two memberships", edge, []string{create, levels, invite.EventID(), join.EventID()})
	// The join rules, which a message's auth does not need.
	extra := r.forge(room, bob, "join rules named", edge, []string{create, levels, rules, state[rooms_key(spec.MRoomMember, bob)]})
	for _, event := range []gomatrixserverlib.PDU{direct, outlier, twice, extra} {
		r.events[event.EventID()] = event
	}
	r.lock.Unlock()
	refused("direct", direct, "on seized power", "a message naming rejected power levels was taken")
	refused("outlier", outlier, "on a rejected membership", "a message naming a membership authorised by rejected power levels was taken")
	if !rooms_rejected(rejoin.EventID()) {
		t.Error("THE POINT: a membership fetched to check another, authorised by rejected power levels, was kept as good")
	}
	refused("twice", twice, "two memberships", "a message naming two of dave's memberships was taken")
	refused("extra", extra, "join rules named", "a message naming the join rules was taken")
	if !shown("history after it") || rooms_rejected(join.EventID()) {
		t.Error("CONTROL: the room's own events stand")
	}

	// The room moves past a rejected event, which leaves the state as it
	// found it: an event following it is taken.
	r.lock.Lock()
	follower := r.build(room, bob, "m.room.message", nil, map[string]any{"msgtype": "m.text", "body": "after the rejected"}, []string{direct.EventID()}, state)
	r.lock.Unlock()
	r.push(t, "follower", follower)
	if !shown("after the rejected") || !rooms_point(direct.EventID()) {
		t.Error("THE POINT: an event following a rejected one was not taken")
	}

	// At a gap: the event the state at it is asked for names the rejected
	// power levels, and is kept rejected though that state allows it.
	r.lock.Lock()
	hole := r.forge(room, dave, "in the hole", r.edge[room], []string{create, seized.EventID(), join.EventID()})
	r.take(hole)
	r.lock.Unlock()
	beyond := r.event(room, bob, "m.room.message", nil, map[string]any{"msgtype": "m.text", "body": "beyond the hole"})
	r.missing = true
	r.push(t, "hole", beyond)
	r.missing = false
	if !shown("beyond the hole") || !rooms_point(hole.EventID()) {
		t.Fatal("CONTROL: the event after the hole was not taken from the state at it")
	}
	if !rooms_rejected(hole.EventID()) {
		t.Error("THE POINT: the event at a gap, authorised by rejected power levels, was kept as good")
	}

}

// TestMatrixRoomVersions: in every room version, a room joined on another
// server is served back to a server in it and to no other, branches setting
// the room's name resolve as the version's algorithm has it, and an event
// whose signature does not verify is refused and never shown.
func TestMatrixRoomVersions(t *testing.T) {
	alice, log := create_matrix_test_env(t)
	for _, version := range []gomatrixserverlib.RoomVersion{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"} {
		t.Run("v"+string(version), func(t *testing.T) {
			r := create_matrix_remote(t, version)
			stranger := create_matrix_remote(t, version)
			room := r.room("Versions "+string(version), "invite")
			bob := r.user("bob")
			empty := ""
			past := r.event(room, bob, "m.room.message", nil, map[string]any{"msgtype": "m.text", "body": "past " + string(version)})
			matrix_join(t, r, alice, room)
			shown := func(body string) bool {
				return len(log.matching(func(e map[string]any) bool { return e["kind"] == "message" && e["body"] == body })) > 0
			}

			// Backfill served: a server in the room reads its past, another
			// reads nothing.
			latest := r.event(room, bob, "m.room.message", nil, map[string]any{"msgtype": "m.text", "body": "latest " + string(version)})
			r.push(t, "latest"+string(version), latest)
			status, body := r.call(t, "GET", "/_matrix/federation/v1/backfill/"+url.PathEscape(room)+"?v="+url.QueryEscape(latest.EventID())+"&limit=50", nil)
			served := map[string]bool{}
			pdus, _ := body["pdus"].([]any)
			for _, item := range pdus {
				if event, err := r.version.NewEventFromUntrustedJSON(mustjson(item)); err == nil {
					served[event.EventID()] = true
				}
			}
			if status != 200 || !served[past.EventID()] || !served[latest.EventID()] {
				t.Errorf("backfill = %d, %d events, past %v, latest %v", status, len(pdus), served[past.EventID()], served[latest.EventID()])
			}
			if status, body := stranger.call(t, "GET", "/_matrix/federation/v1/backfill/"+url.PathEscape(room)+"?v="+url.QueryEscape(latest.EventID())+"&limit=50", nil); status != 200 || len(body["pdus"].([]any)) != 0 {
				t.Errorf("backfill for a server not in the room = %d %v", status, body)
			}

			// A state conflict: two names set on parallel branches, the second
			// both deeper and later, which the first version's algorithm
			// prefers by depth and the later ones by time.
			r.lock.Lock()
			edge, state := slices.Clone(r.edge[room]), maps.Clone(r.state[room])
			alpha := r.build(room, bob, "m.room.name", &empty, map[string]any{"name": "Alpha " + string(version)}, edge, state)
			r.events[alpha.EventID()], r.before[alpha.EventID()] = alpha, state
			r.lock.Unlock()
			time.Sleep(5 * time.Millisecond)
			r.lock.Lock()
			step := r.build(room, bob, "m.room.message", nil, map[string]any{"msgtype": "m.text", "body": "step " + string(version)}, edge, state)
			r.events[step.EventID()], r.before[step.EventID()] = step, state
			beta := r.build(room, bob, "m.room.name", &empty, map[string]any{"name": "Beta " + string(version)}, []string{step.EventID()}, state)
			r.events[beta.EventID()], r.before[beta.EventID()] = beta, state
			r.lock.Unlock()
			r.push(t, "beta"+string(version), beta)
			r.push(t, "alpha"+string(version), alpha)
			current, _ := rooms_current(room)
			if current[rooms_key("m.room.name", "")] != beta.EventID() {
				t.Errorf("resolved name = %s, want the deeper, later branch's %s", current[rooms_key("m.room.name", "")], beta.EventID())
			}
			for _, branch := range []string{alpha.EventID(), beta.EventID()} {
				if found, _ := federation_database().exists("select 1 from extremities where room=? and event=?", room, branch); !found {
					t.Errorf("branch %s is not at the room's edge", branch)
				}
			}

			// A bad signature: an event signed with a key that is not the
			// sending server's.
			_, other, _ := ed25519.GenerateKey(rand.Reader)
			r.lock.Lock()
			own := r.private
			r.private = other
			forged := r.build(room, bob, "m.room.message", nil, map[string]any{"msgtype": "m.text", "body": "forged " + string(version)}, []string{beta.EventID()}, r.state[room])
			r.private = own
			r.lock.Unlock()
			results := r.push(t, "forged"+string(version), forged)
			if result, _ := results[forged.EventID()].(map[string]any); result["error"] == nil || shown("forged "+string(version)) || rooms_known(forged.EventID()) {
				t.Errorf("an event with a bad signature: %v", results)
			}
		})
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

// matrix_signal sends ephemeral events to the engine as the remote.
func (r *matrix_remote) signal(t *testing.T, id string, edus ...map[string]any) {
	t.Helper()
	status, body := r.call(t, "PUT", "/_matrix/federation/v1/send/"+id, map[string]any{"origin": string(r.name), "origin_server_ts": time.Now().UnixMilli(), "pdus": []any{}, "edus": edus})
	if status != 200 {
		t.Fatalf("transaction answered %d %v", status, body)
	}
}

func matrix_edu(kind string, content any) map[string]any {
	return map[string]any{"edu_type": kind, "content": content}
}

// TestMatrixSignals: typing, read receipts and presence go both ways. What
// another server says of its own users reaches the apps of the accounts in a
// room with them, and what it says of someone else's is ignored; what the
// account does goes to the room's servers in transactions of ephemeral events
// alone, typing told it stopped once no fresh word comes, a read kept back
// where the user does not share it, and presence offline where they do not
// share that.
func TestMatrixSignals(t *testing.T) {
	alice, log := create_matrix_test_env(t)
	db_open("db/sessions.db").exec("create table if not exists seen (user text primary key, time integer not null)")
	saved, connections := signals_typing_held, presence_connections
	signals_typing_held = 200 * time.Millisecond
	presence_connections = map[string]map[string]*presence_connection{}
	defer func() { signals_typing_held, presence_connections = saved, connections }()
	r := create_matrix_remote(t, gomatrixserverlib.RoomVersionV10)
	room := r.room("Signals", "invite")
	matrix_join(t, r, alice, room)
	bob := r.user("bob")
	first := r.event(room, bob, "m.room.message", nil, map[string]any{"msgtype": "m.text", "body": "hi"})
	r.push(t, "first", first)

	other := create_matrix_remote(t, gomatrixserverlib.RoomVersionV10)
	other.signal(t, "forged",
		matrix_edu("m.typing", map[string]any{"room_id": room, "user_id": bob, "typing": true}),
		matrix_edu("m.presence", map[string]any{"push": []any{map[string]any{"user_id": bob, "presence": "online"}}}))
	if forged := log.matching(func(e map[string]any) bool { return e["kind"] == "typing" || e["kind"] == "presence" }); len(forged) > 0 {
		t.Errorf("another server spoke for bob: %v", forged)
	}

	r.signal(t, "signals",
		matrix_edu("m.typing", map[string]any{"room_id": room, "user_id": bob, "typing": true}),
		matrix_edu("m.typing", map[string]any{"room_id": room, "user_id": r.user("mallory"), "typing": true}),
		matrix_edu("m.receipt", map[string]any{room: map[string]any{"m.read": map[string]any{bob: map[string]any{"event_ids": []string{first.EventID()}, "data": map[string]any{"ts": 1}}}}}),
		matrix_edu("m.presence", map[string]any{"push": []any{map[string]any{"user_id": bob, "presence": "unavailable", "status_msg": "Lunch"}}}))
	if typing := log.find(t, "typing", nil); typing["from"] != bob || typing["typing"] != true || typing["conversation"] != room {
		t.Errorf("typing = %v", typing)
	}
	if strangers := log.matching(func(e map[string]any) bool { return e["from"] == r.user("mallory") }); len(strangers) > 0 {
		t.Errorf("typing from someone not in the room was taken: %v", strangers)
	}
	if receipt := log.find(t, "receipt", nil); receipt["from"] != bob || receipt["id"] != first.EventID() || receipt["displayed"] != true {
		t.Errorf("receipt = %v", receipt)
	}
	if presence := log.find(t, "presence", nil); presence["from"] != bob || presence["status"] != "away" || presence["message"] != "Lunch" {
		t.Errorf("presence = %v", presence)
	}

	thread := linked_thread(alice)
	fn := sl.NewBuiltin("test", nil)
	if answer, err := api_account_typing(thread, fn, sl.Tuple{sl.String("m1"), sl.String(room)}, nil); err != nil || answer != sl.None {
		t.Fatalf("typing = %v %v", answer, err)
	}
	wait(t, "typing and its end", func() bool { return len(r.edus_of("m.typing")) >= 2 })
	typing := r.edus_of("m.typing")
	if !strings.Contains(string(typing[0].Content), `"typing":true`) || !strings.Contains(string(typing[0].Content), "@alice:example.com") || !strings.Contains(string(typing[1].Content), `"typing":false`) {
		t.Errorf("typing sent = %s then %s", typing[0].Content, typing[1].Content)
	}
	if typing[0].Origin != "example.com" {
		t.Errorf("typing from %q", typing[0].Origin)
	}
	api_account_read(thread, fn, sl.Tuple{sl.String("m1"), sl.String(room), sl.String(first.EventID())}, []sl.Tuple{{sl.String("shared"), sl.False}})
	api_account_read(thread, fn, sl.Tuple{sl.String("m1"), sl.String(room), sl.String(first.EventID())}, nil)
	wait(t, "the read receipt", func() bool { return len(r.edus_of("m.receipt")) >= 1 })
	time.Sleep(100 * time.Millisecond)
	if receipts := r.edus_of("m.receipt"); len(receipts) != 1 || !strings.Contains(string(receipts[0].Content), first.EventID()) {
		t.Errorf("receipts sent = %v, want the shared one alone", receipts)
	}

	presence_connect("u-alice", "c1", "s1", "web", "")
	wait(t, "presence online", func() bool {
		for _, edu := range r.edus_of("m.presence") {
			if strings.Contains(string(edu.Content), `"presence":"online"`) {
				return true
			}
		}
		return false
	})
	api_account_presence(thread, fn, sl.Tuple{sl.String("m1"), sl.False}, nil)
	wait(t, "presence offline", func() bool {
		edus := r.edus_of("m.presence")
		return len(edus) > 0 && strings.Contains(string(edus[len(edus)-1].Content), `"presence":"offline"`)
	})
}

// TestMatrixReactions: a reaction from another server is read as one, and an
// object naming a reaction is sent as an annotation.
func TestMatrixReactions(t *testing.T) {
	alice, log := create_matrix_test_env(t)
	r := create_matrix_remote(t, gomatrixserverlib.RoomVersionV10)
	room := r.room("Reactions", "invite")
	matrix_join(t, r, alice, room)
	bob := r.user("bob")
	first := r.event(room, bob, "m.room.message", nil, map[string]any{"msgtype": "m.text", "body": "react to me"})
	like := r.event(room, bob, "m.reaction", nil, map[string]any{"m.relates_to": map[string]any{"rel_type": "m.annotation", "event_id": first.EventID(), "key": "👍"}})
	r.push(t, "reactions", first, like)
	if reaction := log.find(t, "reaction", nil); reaction["id"] != first.EventID() || reaction["reaction"] != "👍" || reaction["event"] != like.EventID() || reaction["from"] != bob {
		t.Errorf("reaction = %v", reaction)
	}
	thread := linked_thread(alice)
	object := sl.NewDict(3)
	object.SetKey(sl.String("conversation"), sl.String(room))
	object.SetKey(sl.String("reaction"), sl.String("😍"))
	object.SetKey(sl.String("message"), sl.String(first.EventID()))
	sent := decoded(t)(api_account_object_create(thread, sl.NewBuiltin("test", nil), sl.Tuple{sl.String("m1"), object}, nil))
	wait(t, "the reaction", func() bool { return len(r.received_of("m.reaction", "@alice:example.com")) == 1 })
	got := r.received_of("m.reaction", "@alice:example.com")[0]
	if got.EventID() != sent["id"] || !strings.Contains(string(got.Content()), "m.annotation") || !strings.Contains(string(got.Content()), first.EventID()) {
		t.Errorf("reaction sent = %s", got.JSON())
	}
}

// TestMatrixMedia: a file the account sends is kept and served to another
// server, signed, as the specification's multipart answer, and to no one for
// another domain; a file another server's user sends is fetched signed and
// kept in the evictable cache, so a second read does not fetch it again.
func TestMatrixMedia(t *testing.T) {
	alice, log := create_matrix_test_env(t)
	saved := cache_dir
	cache_dir = t.TempDir()
	defer func() { cache_dir = saved }()
	r := create_matrix_remote(t, gomatrixserverlib.RoomVersionV10)
	thread := linked_thread(alice)
	fn := sl.NewBuiltin("test", nil)
	uploaded := decoded(t)(api_account_upload(thread, fn, sl.Tuple{sl.String("m1"), sl.Bytes("picture bytes"), sl.String("image/png")}, []sl.Tuple{{sl.String("name"), sl.String("cat.png")}}))
	address, _ := uploaded["url"].(string)
	id, found := strings.CutPrefix(address, "mxc://example.com/")
	if !found || uploaded["size"] != int64(13) {
		t.Fatalf("upload = %v", uploaded)
	}
	request := fclient.NewFederationRequest("GET", r.name, "example.com", "/_matrix/federation/v1/media/download/"+id)
	request.Sign(r.name, r.key, r.private)
	built, _ := request.HTTPRequest()
	built.Host, built.URL.Scheme, built.URL.Host, built.Body = "example.com", "http", "example.com", http.NoBody
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = built
	federation_route(c)
	if w.Code != 200 || !strings.HasPrefix(w.Header().Get("Content-Type"), "multipart/mixed") || !strings.Contains(w.Body.String(), "picture bytes") || !strings.Contains(w.Body.String(), "image/png") {
		t.Errorf("served = %d %s %q", w.Code, w.Header().Get("Content-Type"), w.Body.String())
	}
	if status, _ := r.call(t, "GET", "/_matrix/federation/v1/media/download/nothing", nil); status != 404 {
		t.Errorf("no such file = %d", status)
	}
	federation_database().exec("update media set domain='other.example' where id=?", id)
	if status, _ := r.call(t, "GET", "/_matrix/federation/v1/media/download/"+id, nil); status != 404 {
		t.Errorf("a file another domain sent, asked of this one = %d", status)
	}
	federation_database().exec("update media set domain='example.com' where id=?", id)

	r.lock.Lock()
	r.media["remote1"] = []byte("their picture")
	r.lock.Unlock()
	link := content_address("mxc://" + string(r.name) + "/remote1")
	for i := range 2 {
		fetched := decoded(t)(api_account_media(thread, fn, sl.Tuple{sl.String("m1"), sl.String(link)}, nil))
		if fetched["status"] != int64(200) || fmt.Sprintf("%s", fetched["data"]) != "their picture" || fetched["type"] != "image/png" {
			t.Errorf("fetch %d = %v", i, fetched)
		}
	}
	r.lock.Lock()
	fetches := r.fetched
	r.lock.Unlock()
	if fetches != 1 {
		t.Errorf("fetched %d times, want once and then from the cache", fetches)
	}
	if _, err := os.Stat(content_cached(alice, string(r.name), "remote1")); err != nil {
		t.Errorf("the fetched file is not in the cache: %v", err)
	}
	r.lock.Lock()
	r.media["remote2"] = []byte("moved")
	r.lock.Unlock()
	moved := decoded(t)(api_account_media(thread, fn, sl.Tuple{sl.String("m1"), sl.String(content_address("mxc://" + string(r.name) + "/remote2"))}, nil))
	if fmt.Sprintf("%s", moved["data"]) != "moved picture" || moved["type"] != "image/gif" {
		t.Errorf("a redirected file = %v", moved)
	}

	// In a room: the file the account sends goes as the message it is, and a
	// file another server's user sends comes with the address it is fetched at.
	room := r.room("Files", "invite")
	matrix_join(t, r, alice, room)
	object := sl.NewDict(2)
	object.SetKey(sl.String("conversation"), sl.String(room))
	object.SetKey(sl.String("file"), sl_encode(uploaded))
	sent := decoded(t)(api_account_object_create(thread, fn, sl.Tuple{sl.String("m1"), object}, nil))
	wait(t, "the file at the other server", func() bool { return len(r.received_of("m.room.message", "@alice:example.com")) == 1 })
	if got := r.received_of("m.room.message", "@alice:example.com")[0]; got.EventID() != sent["id"] || !strings.Contains(string(got.Content()), `"msgtype":"m.image"`) || !strings.Contains(string(got.Content()), address) || !strings.Contains(string(got.Content()), "cat.png") {
		t.Errorf("file sent = %s", got.Content())
	}
	picture := r.event(room, r.user("bob"), "m.room.message", nil, map[string]any{"msgtype": "m.image", "body": "their.png", "url": "mxc://" + string(r.name) + "/remote1", "info": map[string]any{"mimetype": "image/png", "size": 13}})
	r.push(t, "picture", picture)
	message := log.find(t, "message", func(e map[string]any) bool { return e["id"] == picture.EventID() })
	files, _ := message["files"].([]any)
	if len(files) != 1 || fmt.Sprint(files[0]) != fmt.Sprint(map[string]any{"name": "their.png", "size": int64(13), "type": "image/png", "url": link}) {
		t.Errorf("file received = %v", message)
	}
}

// TestMatrixForget: once the last account here leaves a room, its stored
// history goes, but for the leave until it is sent.
func TestMatrixForget(t *testing.T) {
	alice, _ := create_matrix_test_env(t)
	r := create_matrix_remote(t, gomatrixserverlib.RoomVersionV10)
	room := r.room("Forget", "invite")
	matrix_join(t, r, alice, room)
	if _, err := api_account_room_leave(linked_thread(alice), sl.NewBuiltin("test", nil), sl.Tuple{sl.String("m1"), sl.String(room)}, nil); err != nil {
		t.Fatal(err)
	}
	db := federation_database()
	wait(t, "the leave sent and the history gone", func() bool {
		row, _ := db.row("select count(*) as n from events where room=?", room)
		return row_int(row, "n") == 0 && len(r.received_of(spec.MRoomMember, "@alice:example.com")) == 1
	})
	if row, _ := db.row("select state from rooms where id=?", room); row_int(row, "state") != -1 {
		t.Errorf("room state = %v, want it marked forgotten", row)
	}
	if row, _ := db.row("select count(*) as n from snapshots where room=?", room); row_int(row, "n") != 0 {
		t.Errorf("states kept = %v", row)
	}

	// Put out by someone else, the same.
	kicked := r.room("Kicked", "invite")
	matrix_join(t, r, alice, kicked)
	invitee := "@alice:example.com"
	r.push(t, "kick", r.event(kicked, r.user("bob"), spec.MRoomMember, &invitee, map[string]any{"membership": "leave"}))
	if row, _ := db.row("select count(*) as n from events where room=?", kicked); row_int(row, "n") != 0 {
		t.Errorf("events kept of a room the account was put out of = %v", row)
	}
}

// TestMatrixDirectory: the people a hosted account knows of are found by
// name or address, and an address names someone through their server.
func TestMatrixDirectory(t *testing.T) {
	alice, _ := create_matrix_test_env(t)
	r := create_matrix_remote(t, gomatrixserverlib.RoomVersionV10)
	room := r.room("Directory", "invite")
	matrix_join(t, r, alice, room)
	thread := linked_thread(alice)
	fn := sl.NewBuiltin("test", nil)
	found := sl_decode(must_value(t)(api_account_directory(thread, fn, sl.Tuple{sl.String("m1"), sl.String("bo")}, nil)))
	if fmt.Sprint(found) != fmt.Sprint([]any{map[string]any{"id": r.user("bob"), "name": "Bob"}}) {
		t.Errorf("by name = %v", found)
	}
	found = sl_decode(must_value(t)(api_account_directory(thread, fn, sl.Tuple{sl.String("m1"), sl.String("nobody here")}, nil)))
	if list, _ := found.([]any); len(list) != 0 {
		t.Errorf("no one = %v", found)
	}
	// Someone in no room with the account, found by address through their
	// server.
	federation_database().exec("delete from snapshots")
	federation_database().exec("delete from events")
	found = sl_decode(must_value(t)(api_account_directory(thread, fn, sl.Tuple{sl.String("m1"), sl.String(r.user("bob"))}, nil)))
	if fmt.Sprint(found) != fmt.Sprint([]any{map[string]any{"id": r.user("bob"), "name": "Bob"}}) {
		t.Errorf("by address = %v", found)
	}
}

func must_value(t *testing.T) func(sl.Value, error) sl.Value {
	return func(value sl.Value, err error) sl.Value {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
}

// matrix_remote_join has one of the remote's users join a room this server
// holds, as Synapse does: make_join, then send_join, its state then the
// remote's.
func matrix_remote_join(t *testing.T, r *matrix_remote, room string, user string) {
	t.Helper()
	status, body := r.call(t, "GET", "/_matrix/federation/v1/make_join/"+url.PathEscape(room)+"/"+url.PathEscape(user)+"?ver=10", nil)
	if status != 200 {
		t.Fatalf("make_join = %d %v", status, body)
	}
	var template gomatrixserverlib.ProtoEvent
	json.Unmarshal(mustjson(body["event"]), &template)
	template.SetContent(map[string]any{"membership": "join"})
	join, _ := r.version.NewEventBuilderFromProtoEvent(&template).Build(time.Now(), r.name, r.key, r.private)
	status, body = r.call(t, "PUT", "/_matrix/federation/v2/send_join/"+url.PathEscape(room)+"/"+url.PathEscape(join.EventID()), json.RawMessage(join.JSON()))
	if status != 200 {
		t.Fatalf("send_join = %d %v", status, body)
	}
	r.adopt(room, body)
	r.lock.Lock()
	r.take(join)
	r.lock.Unlock()
}

// matrix_bridge_env grants the test app the bridge/write permission and records
// what the engine hands the relaying user.
func matrix_bridge_env(t *testing.T, alice *User) (*sl.Thread, *inbound_log) {
	t.Helper()
	thread := linked_thread(alice)
	app, _ := thread.Local("app").(*App)
	permission_grant(alice, app.id, "bridge/write")
	saved_inbound, saved_avatar := bridge_inbound, bridge_avatar
	log := &inbound_log{}
	bridge_inbound = func(user *User, room string, events []map[string]any) {
		log.lock.Lock()
		for _, event := range events {
			event["room"] = room
			log.events = append(log.events, event)
		}
		log.lock.Unlock()
	}
	bridge_avatar = func(user *User, entity string) ([]byte, string) { return []byte("face"), "image/png" }
	t.Cleanup(func() { bridge_inbound, bridge_avatar = saved_inbound, saved_avatar })
	return thread, log
}

// TestMatrixBridge: a group's room is made on the server's domain for a
// person elsewhere, its members seated as stand-ins under their names and
// avatars; what a member says goes out as their stand-in, and what the
// person says comes to the relaying user; an invitation to a stand-in is
// handed on for the member it stands for, and taken, the stand-in joining;
// and the room closed, every stand-in leaves.
func TestMatrixBridge(t *testing.T) {
	alice, _ := create_matrix_test_env(t)
	r := create_matrix_remote(t, gomatrixserverlib.RoomVersionV10)
	thread, log := matrix_bridge_env(t, alice)
	fn := sl.NewBuiltin("test", nil)
	bob := r.user("bob")
	elsewhere := create_matrix_remote(t, gomatrixserverlib.RoomVersionV10)
	carol := elsewhere.user("carol")
	root := "1RootEntityForTheBridgeTestXXXXXXXXXXXXXXXXXXXXXXX"
	member := map[string]any{"entity": root, "name": "Root"}
	people := sl.NewList([]sl.Value{sl.String(bob), sl.String(carol)})
	made := decoded(t)(api_bridge_create(thread, fn, sl.Tuple{sl.String("chat1"), sl.String("Group"), people}, []sl.Tuple{{sl.String("members"), sl_encode([]any{member})}}))
	room, _ := made["room"].(string)
	if room == "" || !strings.Contains(fmt.Sprint(made["people"]), "id:"+bob+" name:Bob") {
		t.Fatalf("create = %v", made)
	}
	standin := bridge_standin_of("@mochi-" + fmt.Sprintf("%x", fingerprint(root)) + ":example.com")
	if standin == nil || rooms_membership(room, row_string(standin, "id")) != spec.Join {
		t.Fatalf("the member's stand-in = %v", standin)
	}
	content := rooms_member_content(row_string(standin, "id"), spec.Join)
	if content["displayname"] != "Root" || !strings.HasPrefix(fmt.Sprint(content["avatar_url"]), "mxc://example.com/") {
		t.Errorf("the stand-in's member content = %v", content)
	}
	r.lock.Lock()
	invited := len(r.invited)
	r.lock.Unlock()
	if invited != 1 {
		t.Errorf("invitations = %d", invited)
	}
	elsewhere.lock.Lock()
	invited = len(elsewhere.invited)
	elsewhere.lock.Unlock()
	if invited != 1 {
		t.Errorf("invitations elsewhere = %d", invited)
	}
	if !federation_local("example.com") {
		t.Error("the stand-ins' domain is not one of this server's")
	}

	// Bob and carol join. Carol's join, taken here, is passed on to bob's
	// server from this one's domain, but not back to hers; a row left in the
	// queue from a domain this server does not speak for holds it up not at
	// all. Then bob writes, and a member writes back.
	matrix_remote_join(t, r, room, bob)
	federation_database().exec("insert into outbox (destination, origin, kind, body, created) values (?, 'stale.example', 'edu', '{}', ?)", string(r.name), now())
	matrix_remote_join(t, elsewhere, room, carol)
	wait(t, "carol's join passed on to bob's server", func() bool { return len(r.received_of(spec.MRoomMember, carol)) == 1 })
	hello := r.event(room, bob, "m.room.message", nil, map[string]any{"msgtype": "m.text", "body": "hello group"})
	r.push(t, "hello", hello)
	if message := log.find(t, "message", nil); message["room"] != room || message["from"] != bob || message["body"] != "hello group" {
		t.Errorf("relayed message = %v", message)
	}
	saved_cache := cache_dir
	cache_dir = t.TempDir()
	defer func() { cache_dir = saved_cache }()
	r.lock.Lock()
	r.media["group1"] = []byte("group picture")
	r.lock.Unlock()
	fetched, _ := api_bridge_media(thread, fn, sl.Tuple{sl.String(room), sl.String(content_address("mxc://" + string(r.name) + "/group1"))}, nil)
	if dict, _ := fetched.(*sl.Dict); dict == nil {
		t.Errorf("media = %v", fetched)
	} else if data, _, _ := dict.Get(sl.String("data")); data == nil || string(data.(sl.Bytes)) != "group picture" {
		t.Errorf("media data = %v", data)
	}
	object := sl.NewDict(1)
	object.SetKey(sl.String("content"), sl.String("hello bob"))
	sent := decoded(t)(api_bridge_send(thread, fn, sl.Tuple{sl.String(room), sl.String(root), object}, []sl.Tuple{{sl.String("key"), sl.String("m1")}}))
	wait(t, "the member's message", func() bool { return len(r.received_of("m.room.message", row_string(standin, "id"))) == 1 })
	if got := r.received_of("m.room.message", row_string(standin, "id"))[0]; got.EventID() != sent["id"] || !strings.Contains(string(got.Content()), "hello bob") {
		t.Errorf("sent as the stand-in = %s", got.JSON())
	}
	if echoed := log.matching(func(e map[string]any) bool { return e["from"] == row_string(standin, "id") }); len(echoed) > 0 {
		t.Errorf("a stand-in's own message came back to the relaying user: %v", echoed)
	}
	if returned := elsewhere.received_of(spec.MRoomMember, carol); len(returned) > 0 {
		t.Errorf("carol's server was sent her own join back")
	}
	if answer, err := api_bridge_send(thread, fn, sl.Tuple{sl.String(room), sl.String("1StrangerEntityForTheBridgeTestXXXXXXXXXXXXXXXXXXX"), object}, nil); err != nil || answer != sl.None {
		t.Errorf("someone with no stand-in = %v %v", answer, err)
	}
	unseated := "1UnseatedEntityForTheBridgeTestXXXXXXXXXXXXXXXXXXX"
	bridge_standin(alice, "example.com", unseated, "Unseated")
	if answer, err := api_bridge_send(thread, fn, sl.Tuple{sl.String(room), sl.String(unseated), object}, nil); err != nil || answer != sl.None {
		t.Errorf("a stand-in not in the room = %v %v", answer, err)
	}
	administrator := user_by_uid("u-root")
	other := linked_thread(administrator)
	if app, _ := other.Local("app").(*App); app != nil {
		permission_grant(administrator, app.id, "bridge/write")
	}
	if _, err := api_bridge_send(other, fn, sl.Tuple{sl.String(room), sl.String(root), object}, nil); err == nil {
		t.Error("another user relayed through a room not theirs")
	}

	// Bob invites the stand-in to a conversation of his own.
	private := r.room("Private", "invite")
	invitee := row_string(standin, "id")
	r.lock.Lock()
	invite := r.build(private, bob, spec.MRoomMember, &invitee, map[string]any{"membership": "invite", "is_direct": true}, r.edge[private], r.state[private])
	r.lock.Unlock()
	request, _ := fclient.NewInviteV2Request(invite, nil)
	status, body := r.call(t, "PUT", "/_matrix/federation/v2/invite/"+url.PathEscape(private)+"/"+url.PathEscape(invite.EventID()), request)
	if status != 200 {
		t.Fatalf("invite = %d %v", status, body)
	}
	countersigned, err := r.version.NewEventFromUntrustedJSON(mustjson(body["event"]))
	if err != nil {
		t.Fatal(err)
	}
	if told := log.find(t, "invite", nil); told["room"] != private || told["to"] != root || told["from"] != bob || told["direct"] != true {
		t.Errorf("invitation handed on = %v", told)
	}
	r.lock.Lock()
	r.take(countersigned)
	r.lock.Unlock()
	joined := decoded(t)(api_bridge_join(thread, fn, sl.Tuple{sl.String(private), sl.String(root)}, []sl.Tuple{{sl.String("chat"), sl.String("chat2")}}))
	if joined["room"] != private || rooms_membership(private, invitee) != spec.Join {
		t.Errorf("join = %v", joined)
	}
	if b := bridge_of(private); b == nil || b.standin != invitee || b.chat != "chat2" {
		t.Errorf("bridge = %v", b)
	}

	// Closed: every stand-in leaves the room.
	if _, err := api_bridge_close(thread, fn, sl.Tuple{sl.String(room)}, nil); err != nil {
		t.Fatal(err)
	}
	if rooms_membership(room, invitee) != "" && rooms_membership(room, invitee) != spec.Leave {
		t.Errorf("the stand-in is still in the closed room")
	}
	if bridge_of(room) != nil {
		t.Error("the closed room is still relayed")
	}
}

// TestMatrixKeyReplace: the owner replaces a domain's signing key. What the
// domain signs from then on carries the new key, other servers checking it
// are answered with it, and the old key is published as expired, so what it
// signed - this server's own events among them - stays verifiable. A domain
// that is not one of the server's Matrix domains is refused.
func TestMatrixKeyReplace(t *testing.T) {
	alice, _ := create_matrix_test_env(t)
	r := create_matrix_remote(t, gomatrixserverlib.RoomVersionV10)
	room := r.room("Keys", "invite")
	matrix_join(t, r, alice, room)
	state, _ := rooms_current(room)
	joined := rooms_event(state[rooms_key(spec.MRoomMember, "@alice:example.com")])
	if joined == nil {
		t.Fatal("no join kept here")
	}
	old := federation_identity_of("example.com").key
	// Nothing in flight: a request signed with the old key and checked once
	// it has expired is refused, and waits out a backoff before it goes again.
	wait(t, "the queue empty", func() bool {
		row, _ := federation_database().row("select count(*) as n from outbox")
		return row_int(row, "n") == 0
	})

	thread := linked_thread(alice)
	administrator := user_by_uid("u-root")
	owner := linked_thread(administrator)
	app, _ := owner.Local("app").(*App)
	permission_grant(administrator, app.id, "settings/write")
	fn := sl.NewBuiltin("test", nil)
	if _, err := api_server_key_replace(thread, fn, sl.Tuple{sl.String("example.com")}, nil); err == nil {
		t.Error("a user who is not the owner replaced the key")
	}
	answer, err := api_server_key_replace(owner, fn, sl.Tuple{sl.String("example.com")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	replaced, _ := sl.AsString(answer)
	if replaced == "" || replaced == string(old) || string(federation_identity_of("example.com").key) != replaced {
		t.Fatalf("new key = %q, old %q", replaced, old)
	}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "http://example.com/_matrix/key/v2/server", nil)
	federation_route(c)
	var document struct {
		Verify map[string]any            `json:"verify_keys"`
		Old    map[string]map[string]any `json:"old_verify_keys"`
	}
	json.Unmarshal(w.Body.Bytes(), &document)
	if _, found := document.Verify[replaced]; !found || len(document.Verify) != 1 {
		t.Errorf("verify_keys = %v", document.Verify)
	}
	if expired, _ := document.Old[string(old)]["expired_ts"].(float64); expired <= 0 {
		t.Errorf("old_verify_keys = %v", document.Old)
	}
	if !strings.Contains(w.Body.String(), `"ed25519:`+strings.TrimPrefix(replaced, "ed25519:")+`"`) || !strings.Contains(w.Body.String(), `"signatures":{"example.com":{"`+replaced+`"`) {
		t.Errorf("the document is not signed with the new key: %s", w.Body.String())
	}

	// This server's own event, signed with the old key, still verifies.
	if err := gomatrixserverlib.VerifyEventSignatures(t.Context(), joined, federation_keyring(), federation_sender); err != nil {
		t.Errorf("an event signed with the old key no longer verifies: %v", err)
	}

	// What it sends now is signed with the new key, and taken.
	object := sl.NewDict(2)
	object.SetKey(sl.String("conversation"), sl.String(room))
	object.SetKey(sl.String("content"), sl.String("after the key"))
	if _, err := api_account_object_create(thread, fn, sl.Tuple{sl.String("m1"), object}, nil); err != nil {
		t.Fatal(err)
	}
	wait(t, "the message at the other server", func() bool { return len(r.received_of("m.room.message", "@alice:example.com")) == 1 })
	if got := r.received_of("m.room.message", "@alice:example.com")[0]; !strings.Contains(string(got.JSON()), replaced) {
		t.Errorf("sent after the replacement but not signed with the new key: %s", got.JSON())
	}

	if _, err := api_server_key_replace(owner, fn, sl.Tuple{sl.String("other.example")}, nil); err == nil {
		t.Error("a domain that is not one of the server's Matrix domains was given a key")
	}
}

// TestMatrixNotices: a linked Matrix account is told a notification by the
// server's notices account, on the domain of the user's hosted Matrix account,
// in a room made the first time and kept; a hosted account is no destination,
// a network's accounts are told nothing when no account is named, and someone
// who left the room is told nothing more there, nor invited again.
func TestMatrixNotices(t *testing.T) {
	alice, _ := create_matrix_test_env(t)
	// A delivery that fails is queued for another attempt.
	db_open("db/queue.db").exec("create table if not exists pushes (id text primary key, user text not null, account text not null, type text not null, identifier text not null default '', data text not null default '', app text not null default '', category text not null default '', object text not null default '', title text not null default '', body text not null default '', link text not null default '', event text not null default '', attempts integer not null default 0, next_retry integer not null, created integer not null)")
	r := create_matrix_remote(t, gomatrixserverlib.RoomVersionV10)
	bob := r.user("bob")
	db_user(alice, "user").exec("insert into accounts (id, type, label, identifier, data, created, verified, enabled) values ('l1', 'matrix', '', ?, '{}', 1, 1, 1)", bob)
	thread := linked_thread(alice)
	app, _ := thread.Local("app").(*App)
	permission_grant(alice, app.id, "accounts/notify")
	fn := sl.NewBuiltin("test", nil)
	notify := func(account string, title string) int64 {
		t.Helper()
		kwargs := linked_kwargs("app", "chat", "category", "message", "object", "c1", "title", title, "body", "A message", "link", "/chat/c1")
		if account != "" {
			kwargs = append(kwargs, sl.Tuple{sl.String("account"), sl.String(account)})
		}
		answer := decoded(t)(api_account_notify(thread, fn, nil, kwargs))
		sent, _ := answer["sent"].(int64)
		return sent
	}
	invitations := func() int {
		r.lock.Lock()
		defer r.lock.Unlock()
		return len(r.invited)
	}

	row, _ := db_user(alice, "user").row("select * from accounts where id='l1'")
	hosted, _ := db_user(alice, "user").row("select * from accounts where id='m1'")
	if !slices.Contains(account_capabilities(row), "notify") || slices.Contains(account_capabilities(hosted), "notify") {
		t.Errorf("notify on the linked account %v, on the hosted one %v", account_capabilities(row), account_capabilities(hosted))
	}

	if sent := notify("l1", "First"); sent != 1 {
		t.Fatalf("sent = %d", sent)
	}
	wait(t, "the invitation from the notices account", func() bool { return invitations() == 1 })
	r.lock.Lock()
	invite := r.invited[0]
	r.lock.Unlock()
	if string(invite.SenderID()) != "@notices:example.com" || *invite.StateKey() != bob {
		t.Errorf("invitation = %s", invite.JSON())
	}
	room := invite.RoomID().String()
	if standin := bridge_standin_of("@notices:example.com"); standin == nil || row_string(standin, "name") != "Mochi" {
		t.Errorf("notices account = %v", standin)
	}

	// Bob takes it; the next notification reaches him in the same room.
	r.lock.Lock()
	r.take(invite)
	r.lock.Unlock()
	matrix_remote_join(t, r, room, bob)
	if sent := notify("l1", "Second"); sent != 1 {
		t.Fatalf("second sent = %d", sent)
	}
	wait(t, "the notification in the room", func() bool { return len(r.received_of("m.room.message", "@notices:example.com")) == 1 })
	got := r.received_of("m.room.message", "@notices:example.com")[0]
	if got.RoomID().String() != room || !strings.Contains(string(got.Content()), `Second\nA message`) || !strings.Contains(string(got.Content()), `"msgtype":"m.text"`) {
		t.Errorf("notification = %s", got.Content())
	}
	if invitations() != 1 {
		t.Errorf("invited again: %d", invitations())
	}

	// No account named: a network's accounts are not among those told.
	if sent := notify("", "Everyone"); sent != 0 {
		t.Errorf("told with no account named = %d", sent)
	}
	if sent := notify("m1", "Hosted"); sent != 0 {
		t.Errorf("told the hosted account = %d", sent)
	}

	// Bob leaves: nothing more, and no new room.
	r.push(t, "leave", r.event(room, bob, spec.MRoomMember, &bob, map[string]any{"membership": "leave"}))
	if sent := notify("l1", "After"); sent != 0 {
		t.Errorf("told after leaving = %d", sent)
	}
	if invitations() != 1 {
		t.Errorf("invited again after leaving: %d", invitations())
	}
}

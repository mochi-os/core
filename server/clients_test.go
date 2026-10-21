// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/fclient"
	"github.com/matrix-org/gomatrixserverlib/spec"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
	"maunium.net/go/mautrix/pushrules"
)

// A clients_transport hands a Matrix client's requests to the client engine
// as requests to example.com.
type clients_transport struct{}

func (clients_transport) RoundTrip(request *http.Request) (*http.Response, error) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	request.Host = "example.com"
	if request.Body == nil {
		request.Body = http.NoBody
	}
	c.Request = request
	if !clients_route(c) {
		return nil, fmt.Errorf("the client engine did not answer %s", request.URL.Path)
	}
	return w.Result(), nil
}

// create_clients_test_env is create_matrix_test_env with the sign-in
// tables, and root holding a hosted account @root:example.com ("m2") as
// alice holds @alice:example.com ("m1").
func create_clients_test_env(t *testing.T) (*User, *User) {
	t.Helper()
	alice, _ := create_matrix_test_env(t)
	db_open("db/users.db").exec("create table if not exists tokens (hash text primary key not null, user text not null, app text not null, name text not null default '', scopes text not null default '', action text not null default '', entity text not null default '', created integer not null, expires integer not null default 0, kind text not null default '', client text not null default '', account text not null default '', network text not null default '', jkt text not null default '', refresh text not null default '', device text not null default '', scram text not null default '')")
	db_open("db/sessions.db").exec("create table if not exists accesses (hash text primary key not null, user text not null, used integer not null default 0)")
	root := user_by_uid("u-root")
	db_user(root, "user").exec("insert into accounts (id, type, label, identifier, data, created, verified, enabled) values ('m2', 'matrix', 'Root', '@root:example.com', ?, 1, 1, 1)", json_encode(map[string]any{"hosted": true, "handle": "root", "domain": "example.com"}))
	actor_register(root, "m2", "matrix", "example.com", "root")
	t.Cleanup(func() {
		sliding_lock.Lock()
		sliding_connections = map[string]*sliding_connection{}
		sliding_lock.Unlock()
	})
	return alice, root
}

// clients_client is a mautrix client signed in to an account as a device,
// with a token minted as the sign-in server mints one.
func clients_client(t *testing.T, user *User, account string, address string, device string) *mautrix.Client {
	t.Helper()
	m := signin_mint(user, "", "Test", "matrix", account, clients_api_scopes[0], "", device, false)
	if m == nil {
		t.Fatal("no token")
	}
	cli, err := mautrix.NewClient("http://example.com", id.UserID(address), m.token)
	if err != nil {
		t.Fatal(err)
	}
	cli.Client = &http.Client{Transport: clients_transport{}}
	cli.DeviceID = id.DeviceID(m.device)
	cli.DefaultHTTPRetries = 0
	return cli
}

// clients_request sends a client request as JSON and answers the status and
// body.
func clients_request(t *testing.T, cli *mautrix.Client, method string, path string, body any) (int, map[string]any) {
	t.Helper()
	var reader io.Reader = http.NoBody
	if body != nil {
		data, _ := json.Marshal(body)
		reader = strings.NewReader(string(data))
	}
	request := httptest.NewRequest(method, "http://example.com"+path, reader)
	if cli != nil {
		request.Header.Set("Authorization", "Bearer "+cli.AccessToken)
	}
	response, err := clients_transport{}.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	var answer map[string]any
	data, _ := io.ReadAll(response.Body)
	json.Unmarshal(data, &answer)
	return response.StatusCode, answer
}

// clients_bodies is the bodies of a list of events' messages, in order.
func clients_bodies(events []*event.Event) []string {
	var out []string
	for _, e := range events {
		if body, ok := e.Content.Raw["body"].(string); ok && e.Type == event.EventMessage {
			out = append(out, body)
		}
	}
	return out
}

// TestClientsConversation: alice's client makes a direct room and invites
// root; root's client is told of the invitation by its sync, joins, and is
// given alice's message, counted unread until its receipt passes it; a
// device sending one transaction twice sends one event, and is told which
// of the timeline's events it sent; typing reaches the other side; history
// pages back newest first; and a sync waiting for something new answers as
// soon as there is.
func TestClientsConversation(t *testing.T) {
	alice, root := create_clients_test_env(t)
	ctx := t.Context()
	a := clients_client(t, alice, "m1", "@alice:example.com", "ALICE")
	r := clients_client(t, root, "m2", "@root:example.com", "ROOT")

	who, err := a.Whoami(ctx)
	if err != nil || who.UserID != "@alice:example.com" || who.DeviceID != "ALICE" {
		t.Fatalf("whoami = %+v %v", who, err)
	}
	first, err := r.SyncRequest(ctx, 0, "", "", false, "")
	if err != nil {
		t.Fatal(err)
	}
	made, err := a.CreateRoom(ctx, &mautrix.ReqCreateRoom{Preset: "trusted_private_chat", Name: "Plans", Invite: []id.UserID{"@root:example.com"}, IsDirect: true})
	if err != nil {
		t.Fatal(err)
	}
	room := made.RoomID
	invited, err := r.SyncRequest(ctx, 0, first.NextBatch, "", false, "")
	if err != nil {
		t.Fatal(err)
	}
	invite := invited.Rooms.Invite[room]
	if invite == nil {
		t.Fatalf("THE POINT: root's sync was not told of the invitation: %+v", invited.Rooms)
	}
	named := false
	for _, e := range invite.State.Events {
		named = named || (e.Type == event.StateRoomName && e.Content.Raw["name"] == "Plans")
	}
	if !named {
		t.Errorf("the invitation does not show the room's name: %+v", invite.State.Events)
	}
	if _, err := r.JoinRoomByID(ctx, room); err != nil {
		t.Fatal(err)
	}
	joined, err := r.SyncRequest(ctx, 0, invited.NextBatch, "", false, "")
	if err != nil {
		t.Fatal(err)
	}
	if joined.Rooms.Join[room] == nil {
		t.Fatalf("root's sync did not give the room it joined: %+v", joined.Rooms)
	}

	// One transaction sent twice is one event; the device that sent it is
	// told its transaction.
	sent, err := a.SendMessageEvent(ctx, room, event.EventMessage, map[string]any{"msgtype": "m.text", "body": "hello"}, mautrix.ReqSendEvent{TransactionID: "t1"})
	if err != nil {
		t.Fatal(err)
	}
	again, err := a.SendMessageEvent(ctx, room, event.EventMessage, map[string]any{"msgtype": "m.text", "body": "hello"}, mautrix.ReqSendEvent{TransactionID: "t1"})
	if err != nil || again.EventID != sent.EventID {
		t.Errorf("THE POINT: a transaction sent twice made a second event: %v %v", again, err)
	}
	told, err := r.SyncRequest(ctx, 0, joined.NextBatch, "", false, "")
	if err != nil {
		t.Fatal(err)
	}
	entry := told.Rooms.Join[room]
	if entry == nil || fmt.Sprint(clients_bodies(entry.Timeline.Events)) != "[hello]" {
		t.Fatalf("THE POINT: root's sync timeline = %+v", entry)
	}
	if entry.UnreadNotifications == nil || entry.UnreadNotifications.NotificationCount != 1 {
		t.Errorf("THE POINT: the message is not counted unread: %+v", entry.UnreadNotifications)
	}
	if entry.Timeline.Events[len(entry.Timeline.Events)-1].Unsigned.TransactionID != "" {
		t.Error("root is told alice's transaction")
	}
	mine, err := a.SyncRequest(ctx, 0, "", "", false, "")
	if err != nil {
		t.Fatal(err)
	}
	transaction := ""
	for _, e := range mine.Rooms.Join[room].Timeline.Events {
		if e.ID == sent.EventID {
			transaction = e.Unsigned.TransactionID
		}
	}
	if transaction != "t1" {
		t.Errorf("THE POINT: alice's device is not told the transaction it sent by: %q", transaction)
	}

	// A receipt passing the message reads it, and alice is told of it.
	if err := r.SendReceipt(ctx, room, sent.EventID, event.ReceiptTypeRead, nil); err != nil {
		t.Fatal(err)
	}
	read, err := r.SyncRequest(ctx, 0, told.NextBatch, "", false, "")
	if err != nil {
		t.Fatal(err)
	}
	if entry := read.Rooms.Join[room]; entry == nil || entry.UnreadNotifications == nil || entry.UnreadNotifications.NotificationCount != 0 {
		t.Errorf("THE POINT: the receipt did not read the notification: %+v", entry)
	}
	receipted, err := a.SyncRequest(ctx, 0, mine.NextBatch, "", false, "")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	if entry := receipted.Rooms.Join[room]; entry != nil {
		for _, e := range entry.Ephemeral.Events {
			if e.Type == event.EphemeralEventReceipt {
				var content map[string]map[string]map[string]any
				json.Unmarshal(e.Content.VeryRaw, &content)
				_, found = content[string(sent.EventID)]["m.read"]["@root:example.com"]
			}
		}
	}
	if !found {
		t.Errorf("THE POINT: alice is not told root read the message: %+v", receipted.Rooms.Join[room])
	}

	// Typing reaches the other side.
	if _, err := r.UserTyping(ctx, room, true, 30*time.Second); err != nil {
		t.Fatal(err)
	}
	typed, err := a.SyncRequest(ctx, 0, receipted.NextBatch, "", false, "")
	if err != nil {
		t.Fatal(err)
	}
	typing := false
	if entry := typed.Rooms.Join[room]; entry != nil {
		for _, e := range entry.Ephemeral.Events {
			if e.Type == event.EphemeralEventTyping {
				ids, _ := e.Content.Raw["user_ids"].([]any)
				typing = slices.Contains(ids, any("@root:example.com"))
			}
		}
	}
	if !typing {
		t.Errorf("THE POINT: alice is not told root is typing: %+v", typed.Rooms.Join[room])
	}

	// History pages back from the end, newest first.
	if _, err := a.SendText(ctx, room, "second"); err != nil {
		t.Fatal(err)
	}
	page, err := a.Messages(ctx, room, "", "", mautrix.DirectionBackward, nil, 2)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(clients_bodies(page.Chunk)) != "[second hello]" || page.End == "" {
		t.Errorf("THE POINT: history = %v end %q", clients_bodies(page.Chunk), page.End)
	}
	rest, err := a.Messages(ctx, room, page.End, "", mautrix.DirectionBackward, nil, 50)
	if err != nil || slices.Contains(clients_bodies(rest.Chunk), "hello") || len(rest.Chunk) == 0 {
		t.Errorf("the next page repeats the first or is empty: %v %v", clients_bodies(rest.Chunk), err)
	}

	// A sync waiting for something new answers as soon as there is.
	latest, err := r.SyncRequest(ctx, 0, typed.NextBatch, "", false, "")
	if err != nil {
		t.Fatal(err)
	}
	var wait sync.WaitGroup
	var waited *mautrix.RespSync
	var waited_error error
	started := time.Now()
	wait.Add(1)
	go func() {
		defer wait.Done()
		waited, waited_error = r.SyncRequest(ctx, 20000, latest.NextBatch, "", false, "")
	}()
	time.Sleep(200 * time.Millisecond)
	if _, err := a.SendText(ctx, room, "while you wait"); err != nil {
		t.Fatal(err)
	}
	wait.Wait()
	if waited_error != nil || waited.Rooms.Join[room] == nil || !slices.Contains(clients_bodies(waited.Rooms.Join[room].Timeline.Events), "while you wait") || time.Since(started) > 10*time.Second {
		t.Errorf("THE POINT: the waiting sync = %+v %v after %v", waited, waited_error, time.Since(started))
	}

	// What root's client sent is root's in its chat too.
	if rooms_membership(string(room), "@root:example.com") != spec.Join {
		t.Errorf("root's membership = %q", rooms_membership(string(room), "@root:example.com"))
	}
}

// TestClientsAuthentication: a request without a token, with a token of
// another kind or network, or with one lacking the client interface's scope
// is refused, and one with the token in the query is taken.
func TestClientsAuthentication(t *testing.T) {
	alice, _ := create_clients_test_env(t)
	if status, body := clients_request(t, nil, "GET", "/_matrix/client/v3/account/whoami", nil); status != 401 || body["errcode"] != "M_MISSING_TOKEN" {
		t.Errorf("THE POINT: no token: %d %v", status, body)
	}
	fediverse := signin_mint(alice, "", "Other", "fediverse", "f1", "read", "", "", false)
	request := httptest.NewRequest("GET", "http://example.com/_matrix/client/v3/account/whoami", nil)
	request.Header.Set("Authorization", "Bearer "+fediverse.token)
	response, _ := clients_transport{}.RoundTrip(request)
	var body map[string]any
	json.NewDecoder(response.Body).Decode(&body)
	if response.StatusCode != 401 || body["errcode"] != "M_UNKNOWN_TOKEN" {
		t.Errorf("THE POINT: another network's token: %d %v", response.StatusCode, body)
	}
	narrow := signin_mint(alice, "", "Narrow", "matrix", "m1", "openid", "", "NARROW", false)
	request = httptest.NewRequest("GET", "http://example.com/_matrix/client/v3/account/whoami", nil)
	request.Header.Set("Authorization", "Bearer "+narrow.token)
	response, _ = clients_transport{}.RoundTrip(request)
	body = nil
	json.NewDecoder(response.Body).Decode(&body)
	if response.StatusCode != 403 {
		t.Errorf("THE POINT: a token without the interface's scope: %d %v", response.StatusCode, body)
	}
	a := clients_client(t, alice, "m1", "@alice:example.com", "ALICE")
	request = httptest.NewRequest("GET", "http://example.com/_matrix/client/v3/account/whoami?access_token="+url.QueryEscape(a.AccessToken), nil)
	response, _ = clients_transport{}.RoundTrip(request)
	body = nil
	json.NewDecoder(response.Body).Decode(&body)
	if response.StatusCode != 200 || body["user_id"] != "@alice:example.com" {
		t.Errorf("a token in the query: %d %v", response.StatusCode, body)
	}
}

// clients_state_of is a room's state event content as a client reads it.
func clients_state_of(t *testing.T, cli *mautrix.Client, room id.RoomID, kind string, key string) map[string]any {
	t.Helper()
	var content map[string]any
	if err := cli.StateEvent(t.Context(), room, event.NewEventType(kind), key, &content); err != nil {
		return nil
	}
	return content
}

// TestClientsRooms: a room made public with an alias is found by the alias
// and in the directory, and joined by it; its state changes are given with
// what they replaced; reactions and replies in a thread are found by what
// they relate to; a redacted event says what redacted it; search finds a
// message; someone removed is told they left; an upgrade closes the room
// with a tombstone and moves its alias to the room that replaces it, which
// names its predecessor; and a space is walked to its children.
func TestClientsRooms(t *testing.T) {
	alice, root := create_clients_test_env(t)
	ctx := t.Context()
	a := clients_client(t, alice, "m1", "@alice:example.com", "ALICE")
	r := clients_client(t, root, "m2", "@root:example.com", "ROOT")

	made, err := a.CreateRoom(ctx, &mautrix.ReqCreateRoom{Visibility: "public", RoomAliasName: "plans", Name: "Plans", Topic: "What next"})
	if err != nil {
		t.Fatal(err)
	}
	room := made.RoomID
	resolved, err := r.ResolveAlias(ctx, "#plans:example.com")
	if err != nil || resolved.RoomID != room {
		t.Fatalf("THE POINT: the alias resolves to %+v %v", resolved, err)
	}
	if _, err := a.CreateRoom(ctx, &mautrix.ReqCreateRoom{RoomAliasName: "plans"}); err == nil {
		t.Error("a second room took the same alias")
	}
	listed, err := r.PublicRooms(ctx, &mautrix.ReqPublicRooms{})
	if err != nil || len(listed.Chunk) != 1 || listed.Chunk[0].RoomID != room || listed.Chunk[0].Name != "Plans" || listed.Chunk[0].Topic != "What next" {
		t.Errorf("THE POINT: the directory = %+v %v", listed, err)
	}
	if _, err := r.JoinRoom(ctx, "#plans:example.com", nil); err != nil || rooms_membership(string(room), "@root:example.com") != spec.Join {
		t.Fatalf("THE POINT: joining by the alias: %v", err)
	}
	if status, body := clients_request(t, a, "GET", "/_matrix/client/v3/directory/list/room/"+url.PathEscape(string(room)), nil); status != 200 || body["visibility"] != "public" {
		t.Errorf("the room's listing = %d %v", status, body)
	}
	if status, _ := clients_request(t, r, "PUT", "/_matrix/client/v3/directory/list/room/"+url.PathEscape(string(room)), map[string]any{"visibility": "private"}); status != 403 {
		t.Errorf("THE POINT: someone without the power unlisted the room: %d", status)
	}

	// A change of state is given with what it replaced.
	before, err := r.SyncRequest(ctx, 0, "", "", false, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.SendStateEvent(ctx, room, event.StateTopic, "", map[string]any{"topic": "Later"}); err != nil {
		t.Fatal(err)
	}
	if content := clients_state_of(t, r, room, "m.room.topic", ""); content["topic"] != "Later" {
		t.Errorf("the topic read back = %v", content)
	}
	after, err := r.SyncRequest(ctx, 0, before.NextBatch, "", false, "")
	if err != nil {
		t.Fatal(err)
	}
	replaced := false
	for _, e := range after.Rooms.Join[room].Timeline.Events {
		if e.Type == event.StateTopic && e.Unsigned.PrevContent != nil && e.Unsigned.PrevContent.Raw["topic"] == "What next" {
			replaced = true
		}
	}
	if !replaced {
		t.Errorf("THE POINT: the topic's change does not say what it replaced: %+v", after.Rooms.Join[room])
	}

	// Reactions and replies in a thread are found by what they relate to.
	hello, err := a.SendText(ctx, room, "hello there")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.SendMessageEvent(ctx, room, event.EventReaction, map[string]any{"m.relates_to": map[string]any{"rel_type": "m.annotation", "event_id": hello.EventID, "key": "👍"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.SendMessageEvent(ctx, room, event.EventMessage, map[string]any{"msgtype": "m.text", "body": "in a thread", "m.relates_to": map[string]any{"rel_type": "m.thread", "event_id": hello.EventID}}); err != nil {
		t.Fatal(err)
	}
	if status, body := clients_request(t, a, "GET", "/_matrix/client/v1/rooms/"+url.PathEscape(string(room))+"/relations/"+url.PathEscape(string(hello.EventID))+"/m.annotation", nil); status != 200 || len(body["chunk"].([]any)) != 1 || body["chunk"].([]any)[0].(map[string]any)["type"] != "m.reaction" {
		t.Errorf("THE POINT: the annotations = %d %v", status, body)
	}
	if status, body := clients_request(t, a, "GET", "/_matrix/client/v1/rooms/"+url.PathEscape(string(room))+"/threads", nil); status != 200 || len(body["chunk"].([]any)) != 1 || body["chunk"].([]any)[0].(map[string]any)["event_id"] != string(hello.EventID) {
		t.Errorf("THE POINT: the threads = %d %v", status, body)
	}
	around, err := a.Context(ctx, room, hello.EventID, nil, 4)
	if err != nil || around.Event == nil || around.Event.ID != hello.EventID || len(around.EventsAfter) != 2 || len(around.EventsBefore) == 0 {
		t.Errorf("the context = %+v %v", around, err)
	}

	// Search finds a message by its words, and not by others.
	if status, body := clients_request(t, r, "POST", "/_matrix/client/v3/search", map[string]any{"search_categories": map[string]any{"room_events": map[string]any{"search_term": "THERE hello"}}}); status != 200 || fmt.Sprint(body["search_categories"].(map[string]any)["room_events"].(map[string]any)["count"]) != "1" {
		t.Errorf("THE POINT: search = %d %v", status, body)
	}
	if _, body := clients_request(t, r, "POST", "/_matrix/client/v3/search", map[string]any{"search_categories": map[string]any{"room_events": map[string]any{"search_term": "hello nowhere"}}}); fmt.Sprint(body["search_categories"].(map[string]any)["room_events"].(map[string]any)["count"]) != "0" {
		t.Errorf("search matched a message missing a word: %v", body)
	}

	// A redacted event says what redacted it.
	redaction, err := a.RedactEvent(ctx, room, hello.EventID, mautrix.ReqRedact{Reason: "oops"})
	if err != nil {
		t.Fatal(err)
	}
	gone, err := r.GetEvent(ctx, room, hello.EventID)
	if err != nil || gone.Content.Raw["body"] != nil || gone.Unsigned.RedactedBecause == nil || gone.Unsigned.RedactedBecause.ID != redaction.EventID {
		t.Errorf("THE POINT: the redacted event = %+v %v", gone, err)
	}

	// Someone removed is told they left.
	kicked, err := r.SyncRequest(ctx, 0, after.NextBatch, "", false, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.KickUser(ctx, room, &mautrix.ReqKickUser{UserID: "@root:example.com", Reason: "tidying"}); err != nil {
		t.Fatal(err)
	}
	left, err := r.SyncRequest(ctx, 0, kicked.NextBatch, "", false, "")
	if err != nil {
		t.Fatal(err)
	}
	if left.Rooms.Leave[room] == nil || left.Rooms.Join[room] != nil {
		t.Errorf("THE POINT: root's sync after the kick = %+v", left.Rooms)
	}
	if _, err := r.SendText(ctx, room, "still here?"); err == nil {
		t.Error("someone removed could still send")
	}

	// An upgrade replaces the room.
	var upgraded struct {
		Replacement string `json:"replacement_room"`
	}
	status, body := clients_request(t, a, "POST", "/_matrix/client/v3/rooms/"+url.PathEscape(string(room))+"/upgrade", map[string]any{"new_version": "11"})
	upgraded.Replacement, _ = body["replacement_room"].(string)
	if status != 200 || upgraded.Replacement == "" {
		t.Fatalf("upgrade = %d %v", status, body)
	}
	if tombstone := clients_state_of(t, a, room, "m.room.tombstone", ""); tombstone["replacement_room"] != upgraded.Replacement {
		t.Errorf("THE POINT: the tombstone = %v", tombstone)
	}
	create := clients_state_of(t, a, id.RoomID(upgraded.Replacement), "m.room.create", "")
	if predecessor, _ := create["predecessor"].(map[string]any); predecessor["room_id"] != string(room) || create["room_version"] != "11" {
		t.Errorf("THE POINT: the replacement's creation = %v", create)
	}
	if name := clients_state_of(t, a, id.RoomID(upgraded.Replacement), "m.room.name", ""); name["name"] != "Plans" {
		t.Errorf("the replacement lost the name: %v", name)
	}
	if resolved, err := r.ResolveAlias(ctx, "#plans:example.com"); err != nil || string(resolved.RoomID) != upgraded.Replacement {
		t.Errorf("THE POINT: the alias did not move: %+v %v", resolved, err)
	}

	// A space is walked to its children.
	space, err := a.CreateRoom(ctx, &mautrix.ReqCreateRoom{Name: "Home", Preset: "public_chat", CreationContent: map[string]any{"type": "m.space"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.SendStateEvent(ctx, space.RoomID, event.StateSpaceChild, upgraded.Replacement, map[string]any{"via": []string{"example.com"}}); err != nil {
		t.Fatal(err)
	}
	walked, err := a.Hierarchy(ctx, space.RoomID, &mautrix.ReqHierarchy{})
	if err != nil {
		t.Fatal(err)
	}
	var walked_rooms []string
	for _, entry := range walked.Rooms {
		walked_rooms = append(walked_rooms, string(entry.RoomID))
	}
	if !slices.Equal(walked_rooms, []string{string(space.RoomID), upgraded.Replacement}) || walked.Rooms[0].RoomType != "m.space" || len(walked.Rooms[0].ChildrenState) != 1 {
		t.Errorf("THE POINT: the space's walk = %v %+v", walked_rooms, walked.Rooms)
	}
}

// TestClientsKnocking: someone knocks on a room whose rule is to knock and
// their client is told it waits; the room's members are told of the knock;
// and once invited they join.
func TestClientsKnocking(t *testing.T) {
	alice, root := create_clients_test_env(t)
	ctx := t.Context()
	a := clients_client(t, alice, "m1", "@alice:example.com", "ALICE")
	r := clients_client(t, root, "m2", "@root:example.com", "ROOT")
	made, err := a.CreateRoom(ctx, &mautrix.ReqCreateRoom{Name: "Club", InitialState: []*event.Event{{Type: event.StateJoinRules, StateKey: new(string), Content: event.Content{Raw: map[string]any{"join_rule": "knock"}}}}})
	if err != nil {
		t.Fatal(err)
	}
	room := made.RoomID
	if _, err := r.JoinRoomByID(ctx, room); err == nil {
		t.Fatal("THE POINT: someone joined a room whose rule is to knock")
	}
	start, err := a.SyncRequest(ctx, 0, "", "", false, "")
	if err != nil {
		t.Fatal(err)
	}
	waiting, err := r.SyncRequest(ctx, 0, "", "", false, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.KnockRoom(ctx, string(room), &mautrix.ReqKnockRoom{Reason: "let me in"}); err != nil {
		t.Fatal(err)
	}
	knocked, err := r.SyncRequest(ctx, 0, waiting.NextBatch, "", false, "")
	if err != nil {
		t.Fatal(err)
	}
	if knocked.Rooms.Knock[room] == nil || len(knocked.Rooms.Knock[room].State.Events) == 0 {
		t.Errorf("THE POINT: the knocker's sync = %+v", knocked.Rooms)
	}
	heard, err := a.SyncRequest(ctx, 0, start.NextBatch, "", false, "")
	if err != nil {
		t.Fatal(err)
	}
	knock := false
	for _, e := range heard.Rooms.Join[room].Timeline.Events {
		knock = knock || (e.Type == event.StateMember && e.GetStateKey() == "@root:example.com" && e.Content.Raw["membership"] == "knock" && e.Content.Raw["reason"] == "let me in")
	}
	if !knock {
		t.Errorf("THE POINT: the room was not told of the knock: %+v", heard.Rooms.Join[room])
	}
	if _, err := a.InviteUser(ctx, room, &mautrix.ReqInviteUser{UserID: "@root:example.com"}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.JoinRoomByID(ctx, room); err != nil || rooms_membership(string(room), "@root:example.com") != spec.Join {
		t.Errorf("joining once invited: %v", err)
	}
}

// clients_sliding sends a sliding sync request from a position.
func clients_sliding(t *testing.T, cli *mautrix.Client, position string, timeout int, body map[string]any) (int, map[string]any) {
	t.Helper()
	path := "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync?timeout=" + fmt.Sprint(timeout)
	if position != "" {
		path += "&pos=" + url.QueryEscape(position)
	}
	return clients_request(t, cli, "POST", path, body)
}

// TestClientsSliding: sliding sync gives a list's count and the rooms in its
// window, newest activity first, each whole with the state asked for the
// first time and only what is new after; an unknown position is refused,
// the previous one answered again; a request that waits answers when there
// is something new; the extensions carry the account's data and what was
// sent to the device; and a room left is given its leaving.
func TestClientsSliding(t *testing.T) {
	alice, root := create_clients_test_env(t)
	ctx := t.Context()
	a := clients_client(t, alice, "m1", "@alice:example.com", "ALICE")
	r := clients_client(t, root, "m2", "@root:example.com", "ROOT")
	older, err := a.CreateRoom(ctx, &mautrix.ReqCreateRoom{Name: "Older", Invite: []id.UserID{"@root:example.com"}})
	if err != nil {
		t.Fatal(err)
	}
	newer, err := a.CreateRoom(ctx, &mautrix.ReqCreateRoom{Name: "Newer", Invite: []id.UserID{"@root:example.com"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, room := range []id.RoomID{older.RoomID, newer.RoomID} {
		if _, err := r.JoinRoomByID(ctx, room); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := a.SendText(ctx, older.RoomID, "bump"); err != nil {
		t.Fatal(err)
	}
	if err := r.SetAccountData(ctx, "org.example.setting", map[string]any{"on": true}); err != nil {
		t.Fatal(err)
	}
	request := map[string]any{
		"conn_id": "main",
		"lists": map[string]any{"all": map[string]any{
			"ranges":         [][]int{{0, 0}},
			"required_state": [][]string{{"m.room.name", ""}, {"m.room.member", "$LAZY"}},
			"timeline_limit": 5,
		}},
		"extensions": map[string]any{"account_data": map[string]any{"enabled": true}, "to_device": map[string]any{"enabled": true}, "e2ee": map[string]any{"enabled": true}},
	}
	status, first := clients_sliding(t, r, "", 0, request)
	if status != 200 || first["pos"] == nil {
		t.Fatalf("first = %d %v", status, first)
	}
	lists := first["lists"].(map[string]any)
	if fmt.Sprint(lists["all"].(map[string]any)["count"]) != "2" {
		t.Errorf("THE POINT: the list's count = %v", lists)
	}
	rooms := first["rooms"].(map[string]any)
	given, _ := rooms[string(older.RoomID)].(map[string]any)
	if len(rooms) != 1 || given == nil {
		t.Fatalf("THE POINT: the window of one holds %v, want the room with the latest activity", rooms)
	}
	if given["initial"] != true || given["name"] != "Older" || len(given["timeline"].([]any)) == 0 {
		t.Errorf("THE POINT: the room given whole = %v", given)
	}
	names := 0
	for _, e := range given["required_state"].([]any) {
		if e.(map[string]any)["type"] == "m.room.name" {
			names++
		}
	}
	if names != 1 {
		t.Errorf("the state asked for = %v", given["required_state"])
	}
	global := first["extensions"].(map[string]any)["account_data"].(map[string]any)["global"].([]any)
	found := false
	for _, e := range global {
		found = found || e.(map[string]any)["type"] == "org.example.setting"
	}
	if !found {
		t.Errorf("THE POINT: the account's data is not in the extension: %v", global)
	}
	position := first["pos"].(string)

	// Nothing new: a request that waits answers when there is.
	var wait sync.WaitGroup
	var waited map[string]any
	wait.Add(1)
	go func() {
		defer wait.Done()
		_, waited = clients_sliding(t, r, position, 20000, map[string]any{"conn_id": "main"})
	}()
	time.Sleep(200 * time.Millisecond)
	if _, err := a.SendText(ctx, older.RoomID, "news"); err != nil {
		t.Fatal(err)
	}
	wait.Wait()
	entry, _ := waited["rooms"].(map[string]any)[string(older.RoomID)].(map[string]any)
	if entry == nil || entry["initial"] == true || len(entry["timeline"].([]any)) != 1 || entry["timeline"].([]any)[0].(map[string]any)["content"].(map[string]any)["body"] != "news" {
		t.Errorf("THE POINT: what is new = %v", waited)
	}
	next := waited["pos"].(string)

	// The previous position is answered again; an unknown one is refused.
	if status, again := clients_sliding(t, r, position, 0, map[string]any{"conn_id": "main"}); status != 200 || again["rooms"].(map[string]any)[string(older.RoomID)] == nil {
		t.Errorf("THE POINT: the previous position = %d %v", status, again)
	}
	if status, body := clients_sliding(t, r, "999", 0, map[string]any{"conn_id": "main"}); status != 400 || body["errcode"] != "M_UNKNOWN_POS" {
		t.Errorf("THE POINT: an unknown position = %d %v", status, body)
	}
	if status, body := clients_sliding(t, r, next, 0, map[string]any{"conn_id": "other"}); status != 400 || body["errcode"] != "M_UNKNOWN_POS" {
		t.Errorf("another connection's position = %d %v", status, body)
	}

	// A room scrolled into the window is given whole; one left is given its
	// leaving.
	request["lists"] = map[string]any{"all": map[string]any{"ranges": [][]int{{0, 1}}, "required_state": [][]string{{"m.room.name", ""}}, "timeline_limit": 5}}
	_, both := clients_sliding(t, r, "", 0, request)
	if room := both["rooms"].(map[string]any)[string(newer.RoomID)]; room == nil || room.(map[string]any)["initial"] != true {
		t.Errorf("the room scrolled into the window = %v", both["rooms"])
	}
	// A room the user is invited to shows its invitation, and is given whole
	// once they join.
	offer, err := a.CreateRoom(ctx, &mautrix.ReqCreateRoom{Name: "Offer", Invite: []id.UserID{"@root:example.com"}})
	if err != nil {
		t.Fatal(err)
	}
	request["lists"] = map[string]any{"all": map[string]any{"ranges": [][]int{{0, 9}}, "required_state": [][]string{{"m.room.name", ""}}, "timeline_limit": 5}}
	_, offered := clients_sliding(t, r, both["pos"].(string), 0, request)
	if room, _ := offered["rooms"].(map[string]any)[string(offer.RoomID)].(map[string]any); room == nil || room["invite_state"] == nil {
		t.Fatalf("the invitation = %v", offered["rooms"])
	}
	if _, err := r.JoinRoomByID(ctx, offer.RoomID); err != nil {
		t.Fatal(err)
	}
	_, accepted := clients_sliding(t, r, offered["pos"].(string), 0, map[string]any{"conn_id": "main"})
	if room, _ := accepted["rooms"].(map[string]any)[string(offer.RoomID)].(map[string]any); room == nil || room["initial"] != true || room["name"] != "Offer" || len(room["timeline"].([]any)) == 0 {
		t.Errorf("THE POINT: the room joined after its invitation = %v", accepted["rooms"])
	}
	if _, err := r.LeaveRoom(ctx, newer.RoomID); err != nil {
		t.Fatal(err)
	}
	_, gone := clients_sliding(t, r, accepted["pos"].(string), 0, map[string]any{"conn_id": "main"})
	leaving, _ := gone["rooms"].(map[string]any)[string(newer.RoomID)].(map[string]any)
	if leaving == nil {
		t.Fatalf("THE POINT: the room left is not given its leaving: %v", gone)
	}
	timeline := leaving["timeline"].([]any)
	last := timeline[len(timeline)-1].(map[string]any)
	if last["type"] != "m.room.member" || last["content"].(map[string]any)["membership"] != "leave" {
		t.Errorf("the leaving = %v", last)
	}
	if fmt.Sprint(gone["lists"].(map[string]any)["all"].(map[string]any)["count"]) != "2" {
		t.Errorf("the count after leaving = %v", gone["lists"])
	}
}

// TestClientsPush: the push rules a client reads are well-formed, with the
// defaults and the user's own; a room rule that does nothing stops the
// room's messages notifying; a pusher is sent each notification, and one
// whose gateway refuses its key is removed; and the notifications are
// listed.
func TestClientsPush(t *testing.T) {
	alice, root := create_clients_test_env(t)
	ctx := t.Context()
	a := clients_client(t, alice, "m1", "@alice:example.com", "ALICE")
	r := clients_client(t, root, "m2", "@root:example.com", "ROOT")
	rules, err := r.GetPushRules(ctx)
	if err != nil {
		t.Fatalf("THE POINT: the rules do not parse: %v", err)
	}
	ids := []string{}
	for _, rule := range rules.Override {
		ids = append(ids, rule.RuleID)
	}
	if len(ids) == 0 || ids[0] != ".m.rule.master" || !slices.Contains(ids, ".m.rule.is_user_mention") || len(rules.Underride) == 0 {
		t.Errorf("the default rules = %v", ids)
	}

	pushed := make(chan map[string]any, 10)
	refuse := false
	var lock sync.Mutex
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		var body map[string]any
		json.NewDecoder(request.Body).Decode(&body)
		lock.Lock()
		refusing := refuse
		lock.Unlock()
		if refusing {
			json.NewEncoder(w).Encode(map[string]any{"rejected": []string{"phone-key"}})
		} else {
			json.NewEncoder(w).Encode(map[string]any{"rejected": []string{}})
		}
		pushed <- body
	}))
	t.Cleanup(gateway.Close)
	if status, body := clients_request(t, r, "POST", "/_matrix/client/v3/pushers/set", map[string]any{"kind": "http", "app_id": "org.example.app", "pushkey": "phone-key", "app_display_name": "App", "device_display_name": "Phone", "lang": "en", "data": map[string]any{"url": gateway.URL + "/_matrix/push/v1/notify", "format": "event_id_only"}}); status != 200 {
		t.Fatalf("pusher = %d %v", status, body)
	}
	if status, _ := clients_request(t, r, "POST", "/_matrix/client/v3/pushers/set", map[string]any{"kind": "http", "app_id": "x", "pushkey": "k", "data": map[string]any{"url": "http://example.org/elsewhere"}}); status != 400 {
		t.Errorf("THE POINT: a gateway not at the push path was taken: %d", status)
	}
	if _, body := clients_request(t, r, "GET", "/_matrix/client/v3/pushers", nil); len(body["pushers"].([]any)) != 1 {
		t.Errorf("pushers = %v", body)
	}

	made, err := a.CreateRoom(ctx, &mautrix.ReqCreateRoom{Invite: []id.UserID{"@root:example.com"}, IsDirect: true})
	if err != nil {
		t.Fatal(err)
	}
	room := made.RoomID
	select {
	case <-pushed:
	case <-time.After(5 * time.Second):
		t.Fatal("THE POINT: the invitation was not pushed")
	}
	if _, err := r.JoinRoomByID(ctx, room); err != nil {
		t.Fatal(err)
	}
	sent, err := a.SendText(ctx, room, "ping")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case body := <-pushed:
		notification, _ := body["notification"].(map[string]any)
		devices, _ := notification["devices"].([]any)
		if notification["event_id"] != string(sent.EventID) || notification["room_id"] != string(room) || len(devices) != 1 || devices[0].(map[string]any)["pushkey"] != "phone-key" || notification["content"] != nil {
			t.Errorf("THE POINT: the push = %v", body)
		}
		if counts, _ := notification["counts"].(map[string]any); fmt.Sprint(counts["unread"]) != "1" {
			t.Errorf("the push's unread count = %v", notification["counts"])
		}
	case <-time.After(5 * time.Second):
		t.Fatal("THE POINT: the message was not pushed")
	}
	if _, body := clients_request(t, r, "GET", "/_matrix/client/v3/notifications", nil); len(body["notifications"].([]any)) != 2 {
		t.Errorf("THE POINT: the notifications list = %v", body)
	}

	// A room rule that does nothing stops the room notifying.
	if err := r.PutPushRule(ctx, "global", "room", string(room), &mautrix.ReqPutPushRule{Actions: []*pushrules.PushAction{}}); err != nil {
		t.Fatal(err)
	}
	quiet := notifications_unread(t, "@root:example.com", room)
	if _, err := a.SendText(ctx, room, "hush"); err != nil {
		t.Fatal(err)
	}
	if notifications_unread(t, "@root:example.com", room) != quiet {
		t.Error("THE POINT: the room rule did not stop the notification")
	}
	select {
	case body := <-pushed:
		t.Errorf("a silenced message was pushed: %v", body)
	case <-time.After(300 * time.Millisecond):
	}
	if status, _ := clients_request(t, r, "DELETE", "/_matrix/client/v3/pushrules/global/room/"+url.PathEscape(string(room)), nil); status != 200 {
		t.Errorf("removing the rule: %d", status)
	}
	if status, _ := clients_request(t, r, "DELETE", "/_matrix/client/v3/pushrules/global/override/.m.rule.master", nil); status != 404 {
		t.Errorf("a default rule was removed: %d", status)
	}

	// A gateway refusing the key has the pusher removed.
	lock.Lock()
	refuse = true
	lock.Unlock()
	if _, err := a.SendText(ctx, room, "again"); err != nil {
		t.Fatal(err)
	}
	<-pushed
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if _, body := clients_request(t, r, "GET", "/_matrix/client/v3/pushers", nil); len(body["pushers"].([]any)) == 0 {
			return
		}
	}
	t.Error("THE POINT: the refused pusher stayed")
}

// notifications_unread is a room's unread notifications for a user.
func notifications_unread(t *testing.T, user string, room id.RoomID) int64 {
	t.Helper()
	count, _ := notifications_counts(user, string(room))
	return count
}

// TestClientsAccount: a device signs in the old way with a password Mochi
// minted for it, asking for a token that expires, and renews it; a device
// is removed only with that password, and its token ends with it; the
// account's name changes on Matrix and its rooms are told; the account's
// data and a room's tags are kept and given by sync; a kept filter shapes a
// sync; and someone is found by their handle.
func TestClientsAccount(t *testing.T) {
	alice, root := create_clients_test_env(t)
	ctx := t.Context()
	users := db_open("db/users.db")
	users.exec("insert or ignore into handles (domain, name, user, kind, created) values ('example.com', 'alice', 'u-alice', 'handle', 1)")
	users.exec("insert or ignore into handles (domain, name, user, kind, created) values ('example.com', 'root', 'u-root', 'handle', 1)")
	password := password_mint(alice, "", "m1", "matrix", "Laptop")
	anonymous, _ := mautrix.NewClient("http://example.com", "", "")
	anonymous.Client = &http.Client{Transport: clients_transport{}}
	anonymous.DefaultHTTPRetries = 0
	if _, err := anonymous.Login(ctx, &mautrix.ReqLogin{Type: mautrix.AuthTypePassword, Identifier: mautrix.UserIdentifier{Type: mautrix.IdentifierTypeUser, User: "alice"}, Password: "mochi-wrong"}); err == nil {
		t.Error("THE POINT: a wrong password signed in")
	}
	login, err := anonymous.Login(ctx, &mautrix.ReqLogin{Type: mautrix.AuthTypePassword, Identifier: mautrix.UserIdentifier{Type: mautrix.IdentifierTypeUser, User: "alice"}, Password: password, DeviceID: "LAPTOP", RefreshToken: true})
	if err != nil || login.UserID != "@alice:example.com" || login.DeviceID != "LAPTOP" || login.RefreshToken == "" {
		t.Fatalf("THE POINT: login = %+v %v", login, err)
	}
	a, _ := mautrix.NewClient("http://example.com", login.UserID, login.AccessToken)
	a.Client = &http.Client{Transport: clients_transport{}}
	a.DefaultHTTPRetries = 0
	if who, err := a.Whoami(ctx); err != nil || who.DeviceID != "LAPTOP" {
		t.Fatalf("whoami = %+v %v", who, err)
	}
	status, renewed := clients_request(t, nil, "POST", "/_matrix/client/v3/refresh", map[string]any{"refresh_token": login.RefreshToken})
	if status != 200 || renewed["access_token"] == nil || renewed["access_token"] == login.AccessToken {
		t.Fatalf("THE POINT: refresh = %d %v", status, renewed)
	}
	if _, err := a.Whoami(ctx); err == nil {
		t.Error("THE POINT: the token renewed away still works")
	}
	a.AccessToken = renewed["access_token"].(string)
	if status, _ := clients_request(t, nil, "POST", "/_matrix/client/v3/refresh", map[string]any{"refresh_token": login.RefreshToken}); status != 401 {
		t.Errorf("the used refresh token renewed again: %d", status)
	}

	// A device is removed only with the password.
	other := clients_client(t, alice, "m1", "@alice:example.com", "PHONE")
	devices, err := a.GetDevicesInfo(ctx)
	if err != nil || len(devices.Devices) != 2 {
		t.Fatalf("devices = %+v %v", devices, err)
	}
	if status, body := clients_request(t, a, "DELETE", "/_matrix/client/v3/devices/PHONE", map[string]any{}); status != 401 || body["flows"] == nil || body["session"] == nil {
		t.Errorf("THE POINT: removing a device without the password = %d %v", status, body)
	}
	if status, _ := clients_request(t, a, "DELETE", "/_matrix/client/v3/devices/PHONE", map[string]any{"auth": map[string]any{"type": "m.login.password", "password": "mochi-wrong"}}); status != 401 {
		t.Errorf("THE POINT: a wrong password removed the device: %d", status)
	}
	if status, body := clients_request(t, a, "DELETE", "/_matrix/client/v3/devices/PHONE", map[string]any{"auth": map[string]any{"type": "m.login.password", "password": password}}); status != 200 {
		t.Fatalf("removing the device = %d %v", status, body)
	}
	if _, err := other.Whoami(ctx); err == nil {
		t.Error("THE POINT: the removed device's token still works")
	}

	// The name changes on Matrix, and the rooms are told.
	r := clients_client(t, root, "m2", "@root:example.com", "ROOT")
	made, err := a.CreateRoom(ctx, &mautrix.ReqCreateRoom{Invite: []id.UserID{"@root:example.com"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.JoinRoomByID(ctx, made.RoomID); err != nil {
		t.Fatal(err)
	}
	if err := a.SetDisplayName(ctx, "Alice B"); err != nil {
		t.Fatal(err)
	}
	if profile, err := r.GetProfile(ctx, "@alice:example.com"); err != nil || profile.DisplayName != "Alice B" {
		t.Errorf("THE POINT: the profile = %+v %v", profile, err)
	}
	if member := clients_state_of(t, r, made.RoomID, "m.room.member", "@alice:example.com"); member["displayname"] != "Alice B" {
		t.Errorf("THE POINT: the room was not told the name: %v", member)
	}

	// The account's data and a room's tags are kept and given by sync.
	if err := a.SetAccountData(ctx, "org.example.theme", map[string]any{"dark": true}); err != nil {
		t.Fatal(err)
	}
	if err := a.AddTag(ctx, made.RoomID, "m.favourite", 0.5); err != nil {
		t.Fatal(err)
	}
	synced, err := a.SyncRequest(ctx, 0, "", "", false, "")
	if err != nil {
		t.Fatal(err)
	}
	theme, tagged := false, false
	for _, e := range synced.AccountData.Events {
		theme = theme || (e.Type.Type == "org.example.theme" && e.Content.Raw["dark"] == true)
	}
	for _, e := range synced.Rooms.Join[made.RoomID].AccountData.Events {
		if e.Type == event.AccountDataRoomTags {
			tags, _ := e.Content.Raw["tags"].(map[string]any)
			tagged = tags["m.favourite"] != nil
		}
	}
	if !theme || !tagged {
		t.Errorf("THE POINT: sync's account data: theme %v, tags %v", theme, tagged)
	}

	// A kept filter shapes a sync.
	for _, text := range []string{"one", "two", "three"} {
		if _, err := a.SendText(ctx, made.RoomID, text); err != nil {
			t.Fatal(err)
		}
	}
	filter, err := a.CreateFilter(ctx, &mautrix.Filter{Room: &mautrix.RoomFilter{Timeline: &mautrix.FilterPart{Limit: 2}}})
	if err != nil {
		t.Fatal(err)
	}
	limited, err := a.SyncRequest(ctx, 0, "", filter.FilterID, false, "")
	if err != nil {
		t.Fatal(err)
	}
	if timeline := limited.Rooms.Join[made.RoomID].Timeline; len(timeline.Events) != 2 || !timeline.Limited || fmt.Sprint(clients_bodies(timeline.Events)) != "[two three]" {
		t.Errorf("THE POINT: the filtered timeline = %v limited %v", clients_bodies(timeline.Events), timeline.Limited)
	}

	// Someone is found by their handle.
	if found, err := r.SearchUserDirectory(ctx, "alic", 10); err != nil || len(found.Results) != 1 || found.Results[0].UserID != "@alice:example.com" {
		t.Errorf("the directory = %+v %v", found, err)
	}
	if status, _ := clients_request(t, nil, "POST", "/_matrix/client/v3/register", map[string]any{}); status != 403 {
		t.Errorf("registration was not refused: %d", status)
	}
}

// TestClientsKeys: a client's device keys go to the key store, another
// account's client finds them, a message sent to the device reaches it by
// its sync, and its one-time keys are counted there.
func TestClientsKeys(t *testing.T) {
	alice, root := create_clients_test_env(t)
	ctx := t.Context()
	a := clients_client(t, alice, "m1", "@alice:example.com", "ALICE")
	r := clients_client(t, root, "m2", "@root:example.com", "ROOT")
	made, err := a.CreateRoom(ctx, &mautrix.ReqCreateRoom{Invite: []id.UserID{"@root:example.com"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.JoinRoomByID(ctx, made.RoomID); err != nil {
		t.Fatal(err)
	}
	key := create_olm_key(t)
	status, body := clients_request(t, a, "POST", "/_matrix/client/v3/keys/upload", map[string]any{"device_keys": key.device_keys(t, "@alice:example.com", "ALICE"), "one_time_keys": map[string]any{"signed_curve25519:AAAA": key.sign(t, map[string]any{"key": "one"}, "@alice:example.com", "ed25519:ALICE")}})
	if status != 200 || fmt.Sprint(body["one_time_key_counts"].(map[string]any)["signed_curve25519"]) != "1" {
		t.Fatalf("THE POINT: upload = %d %v", status, body)
	}
	queried, err := r.QueryKeys(ctx, &mautrix.ReqQueryKeys{DeviceKeys: mautrix.DeviceKeysRequest{"@alice:example.com": {}}})
	if _, found := queried.DeviceKeys["@alice:example.com"]["ALICE"]; err != nil || !found {
		t.Fatalf("THE POINT: root's query = %+v %v", queried, err)
	}
	first, err := a.SyncRequest(ctx, 0, "", "", false, "")
	if err != nil {
		t.Fatal(err)
	}
	if first.DeviceOTKCount.SignedCurve25519 != 1 {
		t.Errorf("THE POINT: the sync's one-time key count = %+v", first.DeviceOTKCount)
	}
	if _, err := r.SendToDevice(ctx, event.NewEventType("org.example.ping"), &mautrix.ReqSendToDevice{Messages: map[id.UserID]map[id.DeviceID]*event.Content{"@alice:example.com": {"ALICE": {Raw: map[string]any{"hello": "device"}}}}}); err != nil {
		t.Fatal(err)
	}
	delivered, err := a.SyncRequest(ctx, 0, first.NextBatch, "", false, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(delivered.ToDevice.Events) != 1 || delivered.ToDevice.Events[0].Sender != "@root:example.com" || delivered.ToDevice.Events[0].Content.Raw["hello"] != "device" {
		t.Errorf("THE POINT: what was sent to the device = %+v", delivered.ToDevice.Events)
	}
	again, err := a.SyncRequest(ctx, 0, delivered.NextBatch, "", false, "")
	if err != nil || len(again.ToDevice.Events) != 0 {
		t.Errorf("a delivery was given twice: %+v %v", again.ToDevice.Events, err)
	}
	// Root's sync learns alice's device changed.
	watching, err := r.SyncRequest(ctx, 0, "", "", false, "")
	if err != nil {
		t.Fatal(err)
	}
	if status, _ := clients_request(t, a, "PUT", "/_matrix/client/v3/devices/ALICE", map[string]any{"display_name": "Laptop"}); status != 200 {
		t.Fatal("renaming the device")
	}
	changed, err := r.SyncRequest(ctx, 0, watching.NextBatch, "", false, "")
	if err != nil || !slices.Contains(changed.DeviceLists.Changed, "@alice:example.com") {
		t.Errorf("THE POINT: root's sync was not told alice's device changed: %+v %v", changed.DeviceLists, err)
	}
}

// TestClientsFederation: another server is answered what room one of this
// server's aliases names, this server's directory, a walk of a space here,
// and a knock of its user's on a room whose rule is to knock - which the
// room's members are told of - but not one on a room anyone may join, nor
// one for another server's user. A client knocks on a room elsewhere
// through its server and is told it waits, and withdrawing the knock, or
// declining an invitation from elsewhere, is given by its sync as leaving.
func TestClientsFederation(t *testing.T) {
	alice, _ := create_clients_test_env(t)
	ctx := t.Context()
	r := create_matrix_remote(t, gomatrixserverlib.RoomVersionV10)
	a := clients_client(t, alice, "m1", "@alice:example.com", "ALICE")
	lobby, err := a.CreateRoom(ctx, &mautrix.ReqCreateRoom{Visibility: "public", RoomAliasName: "lobby", Name: "Lobby"})
	if err != nil {
		t.Fatal(err)
	}
	status, body := r.call(t, "GET", "/_matrix/federation/v1/query/directory?room_alias="+url.QueryEscape("#lobby:example.com"), nil)
	if status != 200 || body["room_id"] != string(lobby.RoomID) || !slices.Contains(body["servers"].([]any), any("example.com")) {
		t.Errorf("THE POINT: the alias query = %d %v", status, body)
	}
	if status, _ := r.call(t, "GET", "/_matrix/federation/v1/query/directory?room_alias="+url.QueryEscape("#nowhere:example.com"), nil); status != 404 {
		t.Errorf("an unknown alias = %d", status)
	}
	status, body = r.call(t, "GET", "/_matrix/federation/v1/publicRooms", nil)
	if chunk, _ := body["chunk"].([]any); status != 200 || len(chunk) != 1 || chunk[0].(map[string]any)["room_id"] != string(lobby.RoomID) || chunk[0].(map[string]any)["name"] != "Lobby" {
		t.Errorf("THE POINT: the directory = %d %v", status, body)
	}

	// A walk of a space here.
	space, err := a.CreateRoom(ctx, &mautrix.ReqCreateRoom{Name: "Home", Preset: "public_chat", CreationContent: map[string]any{"type": "m.space"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.SendStateEvent(ctx, space.RoomID, event.StateSpaceChild, string(lobby.RoomID), map[string]any{"via": []string{"example.com"}}); err != nil {
		t.Fatal(err)
	}
	status, body = r.call(t, "GET", "/_matrix/federation/v1/hierarchy/"+url.PathEscape(string(space.RoomID)), nil)
	if children, _ := body["children"].([]any); status != 200 || body["room"].(map[string]any)["room_type"] != "m.space" || len(children) != 1 || children[0].(map[string]any)["room_id"] != string(lobby.RoomID) {
		t.Errorf("THE POINT: the walk = %d %v", status, body)
	}

	// A knock from the other server.
	club, err := a.CreateRoom(ctx, &mautrix.ReqCreateRoom{Name: "Club", InitialState: []*event.Event{{Type: event.StateJoinRules, StateKey: new(string), Content: event.Content{Raw: map[string]any{"join_rule": "knock"}}}}})
	if err != nil {
		t.Fatal(err)
	}
	bob := r.user("bob")
	before, err := a.SyncRequest(ctx, 0, "", "", false, "")
	if err != nil {
		t.Fatal(err)
	}
	if status, _ := r.call(t, "GET", "/_matrix/federation/v1/make_knock/"+url.PathEscape(string(lobby.RoomID))+"/"+url.PathEscape(bob)+"?ver=10", nil); status != 403 {
		t.Errorf("THE POINT: a knock on a room anyone may join = %d", status)
	}
	if status, _ := r.call(t, "GET", "/_matrix/federation/v1/make_knock/"+url.PathEscape(string(club.RoomID))+"/"+url.PathEscape("@carol:elsewhere.example")+"?ver=10", nil); status != 403 {
		t.Errorf("THE POINT: a knock for another server's user = %d", status)
	}
	status, body = r.call(t, "GET", "/_matrix/federation/v1/make_knock/"+url.PathEscape(string(club.RoomID))+"/"+url.PathEscape(bob)+"?ver=10", nil)
	if status != 200 {
		t.Fatalf("make_knock = %d %v", status, body)
	}
	var proto gomatrixserverlib.ProtoEvent
	json.Unmarshal(mustjson(body["event"]), &proto)
	knock, err := r.version.NewEventBuilderFromProtoEvent(&proto).Build(time.Now(), r.name, r.key, r.private)
	if err != nil {
		t.Fatal(err)
	}
	status, body = r.call(t, "PUT", "/_matrix/federation/v1/send_knock/"+url.PathEscape(string(club.RoomID))+"/"+url.PathEscape(knock.EventID()), json.RawMessage(knock.JSON()))
	if state, _ := body["knock_room_state"].([]any); status != 200 || len(state) == 0 {
		t.Fatalf("THE POINT: send_knock = %d %v", status, body)
	}
	if rooms_membership(string(club.RoomID), bob) != "knock" {
		t.Errorf("THE POINT: bob's membership = %q", rooms_membership(string(club.RoomID), bob))
	}
	heard, err := a.SyncRequest(ctx, 0, before.NextBatch, "", false, "")
	if err != nil {
		t.Fatal(err)
	}
	told := false
	if entry := heard.Rooms.Join[club.RoomID]; entry != nil {
		for _, e := range entry.Timeline.Events {
			told = told || (e.GetStateKey() == bob && e.Content.Raw["membership"] == "knock")
		}
	}
	if !told {
		t.Errorf("the room was not told of the knock: %+v", heard.Rooms.Join[club.RoomID])
	}

	// A knock through the other server, withdrawn.
	remote := r.room("Remote club", "knock")
	waiting, err := a.SyncRequest(ctx, 0, heard.NextBatch, "", false, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.KnockRoom(ctx, remote, &mautrix.ReqKnockRoom{Via: []string{string(r.name)}}); err != nil {
		t.Fatalf("THE POINT: knocking through the other server: %v", err)
	}
	if knocks := r.received_of(spec.MRoomMember, "@alice:example.com"); len(knocks) != 1 {
		t.Fatalf("the other server was sent %d knocks", len(knocks))
	}
	knocked, err := a.SyncRequest(ctx, 0, waiting.NextBatch, "", false, "")
	if err != nil {
		t.Fatal(err)
	}
	if entry := knocked.Rooms.Knock[id.RoomID(remote)]; entry == nil || len(entry.State.Events) < 2 {
		t.Fatalf("THE POINT: the knock waits = %+v", knocked.Rooms)
	}
	if _, err := a.LeaveRoom(ctx, id.RoomID(remote)); err != nil {
		t.Fatal(err)
	}
	withdrawn, err := a.SyncRequest(ctx, 0, knocked.NextBatch, "", false, "")
	if err != nil {
		t.Fatal(err)
	}
	if withdrawn.Rooms.Leave[id.RoomID(remote)] == nil || withdrawn.Rooms.Knock[id.RoomID(remote)] != nil {
		t.Errorf("THE POINT: the withdrawn knock = %+v", withdrawn.Rooms)
	}

	// An invitation from elsewhere, declined.
	unwanted := r.room("Unwanted", "invite")
	invitee := "@alice:example.com"
	invite := r.event(unwanted, bob, spec.MRoomMember, &invitee, map[string]any{"membership": "invite"})
	request, _ := fclient.NewInviteV2Request(invite, nil)
	if status, body := r.call(t, "PUT", "/_matrix/federation/v2/invite/"+url.PathEscape(unwanted)+"/"+url.PathEscape(invite.EventID()), request); status != 200 {
		t.Fatalf("invite = %d %v", status, body)
	}
	invited, err := a.SyncRequest(ctx, 0, withdrawn.NextBatch, "", false, "")
	if err != nil || invited.Rooms.Invite[id.RoomID(unwanted)] == nil {
		t.Fatalf("THE POINT: the invitation from elsewhere = %+v %v", invited.Rooms, err)
	}
	if _, err := a.LeaveRoom(ctx, id.RoomID(unwanted)); err != nil {
		t.Fatal(err)
	}
	declined, err := a.SyncRequest(ctx, 0, invited.NextBatch, "", false, "")
	if err != nil || declined.Rooms.Leave[id.RoomID(unwanted)] == nil {
		t.Errorf("THE POINT: the declined invitation = %+v %v", declined.Rooms, err)
	}
	if again, err := a.SyncRequest(ctx, 0, declined.NextBatch, "", false, ""); err != nil || again.Rooms.Leave[id.RoomID(unwanted)] != nil {
		t.Errorf("the declining was given twice: %+v %v", again, err)
	}
}

// TestClientsNotificationsKept: notifications are kept for an account only
// once a Matrix client is signed in to it.
func TestClientsNotificationsKept(t *testing.T) {
	alice, root := create_clients_test_env(t)
	ctx := t.Context()
	a := clients_client(t, alice, "m1", "@alice:example.com", "ALICE")
	made, err := a.CreateRoom(ctx, &mautrix.ReqCreateRoom{Invite: []id.UserID{"@root:example.com"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rooms_join(ctx, "@root:example.com", string(made.RoomID)); err != nil {
		t.Fatal(err)
	}
	kept := func() int64 {
		row, _ := federation_database().row("select count(*) as n from notifications where user='@root:example.com'")
		return row_int(row, "n")
	}
	if _, err := a.SendText(ctx, made.RoomID, "before"); err != nil {
		t.Fatal(err)
	}
	if kept() != 0 {
		t.Errorf("THE POINT: notifications kept for an account no client is signed in to: %d", kept())
	}
	clients_client(t, root, "m2", "@root:example.com", "ROOT")
	if _, err := a.SendText(ctx, made.RoomID, "after"); err != nil {
		t.Fatal(err)
	}
	if kept() != 1 {
		t.Errorf("notifications once a client is signed in = %d", kept())
	}
}

// TestClientsPositions: an order's position never goes back when its rows
// go - the invitations' when the invited user joins - so a client's token is
// never ahead of the server's.
func TestClientsPositions(t *testing.T) {
	create_clients_test_env(t)
	clients_pending("!a:example.com", "@alice:example.com", "invite", "", "")
	clients_pending("!b:example.com", "@alice:example.com", "invite", "", "")
	before := sync_latest().invites
	if before < 2 {
		t.Fatalf("invites position = %d after two invitations", before)
	}
	federation_database().exec("delete from pending")
	if after := sync_latest().invites; after != before {
		t.Errorf("invites position went from %d to %d when the rows went", before, after)
	}
	clients_pending("!c:example.com", "@alice:example.com", "invite", "", "")
	if next := sync_latest().invites; next != before+1 {
		t.Errorf("the next invitation is at %d, want %d", next, before+1)
	}
}

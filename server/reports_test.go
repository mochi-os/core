// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/fclient"
	"github.com/matrix-org/gomatrixserverlib/spec"
	sl "go.starlark.net/starlark"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

// A report_notice is a notification an administrator was sent.
type report_notice struct {
	user string
	args Map
}

// report_capture makes a user an active administrator and stands in for the
// notifications service, answering what each administrator was told.
func report_capture(t *testing.T, administrator *User) *[]report_notice {
	t.Helper()
	db_open("db/users.db").exec("update users set role='administrator', status='active' where uid=?", administrator.UID)
	told := &[]report_notice{}
	saved := report_tell
	report_tell = func(user *User, args Map) error {
		*told = append(*told, report_notice{user: user.UID, args: args})
		return nil
	}
	t.Cleanup(func() { report_tell = saved })
	return told
}

// reports_rows reads the reports kept, oldest first.
func reports_rows(t *testing.T) []map[string]any {
	t.Helper()
	rows, _ := networks_database().rows("select * from reports order by created, rowid")
	return rows
}

func report_objects(row map[string]any) []string {
	var objects []string
	json.Unmarshal([]byte(row_string(row, "objects")), &objects)
	return objects
}

// TestReportsFlag: a Flag from another server about one of this server's
// people is kept as a report, never queued for the reported person's apps,
// with the reporter, the objects named other than the account, and the words
// as text; each administrator is told once; and past the bound of reports
// waiting from one server, more are dropped until one is resolved.
func TestReportsFlag(t *testing.T) {
	alice, bob := create_activitypub_test_env(t)
	load_core_labels()
	told := report_capture(t, alice)
	flag := func(id string) map[string]any {
		return map[string]any{"@context": activitystreams, "id": bob.id + "#flags/" + id, "type": "Flag", "actor": bob.id, "object": []any{"https://example.com/~f1", "https://example.com/~f1/notes/1"}, "content": "<p>Spam &amp; scams</p>"}
	}
	if w := signed_post(t, bob, "/~f1/inbox", flag("1"), nil); w.Code != http.StatusAccepted {
		t.Fatalf("flag answered %d %s", w.Code, w.Body.String())
	}
	rows := reports_rows(t)
	if len(rows) != 1 {
		t.Fatalf("THE POINT: reports = %v", rows)
	}
	row := rows[0]
	if row_string(row, "network") != "fediverse" || row_string(row, "reporter") != bob.id || row_string(row, "person") != "https://example.com/~f1" || row_string(row, "user") != alice.UID || row_string(row, "comment") != "Spam & scams" || row_string(row, "host") != "remote.example" {
		t.Errorf("THE POINT: the report = %v", row)
	}
	if objects := report_objects(row); len(objects) != 1 || objects[0] != "https://example.com/~f1/notes/1" {
		t.Errorf("objects = %v", objects)
	}
	if queued, _ := networks_database().exists("select 1 from inbound where type='Flag'"); queued {
		t.Error("THE POINT: the Flag was queued for the reported person's apps")
	}
	var notices []report_notice
	for _, n := range *told {
		if n.user == alice.UID {
			notices = append(notices, n)
		}
	}
	if len(notices) != 1 {
		t.Fatalf("THE POINT: alice was told %d times of %v", len(notices), *told)
	}
	notice := notices[0]
	body, _ := notice.args["body"].(string)
	if notice.args["url"] != "/settings/system/reports" || notice.args["event"] != "report:"+row_string(row, "id") || notice.args["topic"] != "reports/received" || body != "@bob@remote.example reported @alice@example.com" || notice.args["title"] != "New report" {
		t.Errorf("notice = %v", notice)
	}

	saved := report_open_maximum
	report_open_maximum = 1
	t.Cleanup(func() { report_open_maximum = saved })
	if w := signed_post(t, bob, "/~f1/inbox", flag("2"), nil); w.Code != http.StatusAccepted {
		t.Fatalf("second flag answered %d", w.Code)
	}
	if rows, before := reports_rows(t), len(*told); len(rows) != 1 || len(*told) != before {
		t.Errorf("THE POINT: a report past the bound was kept: %d rows", len(rows))
	}
	networks_database().exec("update reports set resolved=1, outcome='dismissed'")
	if w := signed_post(t, bob, "/~f1/inbox", flag("3"), nil); w.Code != http.StatusAccepted {
		t.Fatalf("third flag answered %d", w.Code)
	}
	if rows := reports_rows(t); len(rows) != 2 {
		t.Errorf("a report once the waiting one was resolved was not kept: %d rows", len(rows))
	}
}

// TestReportsFlagCommunity: a Flag sent to a community stays its moderators',
// queued for the app that keeps it, and is no report for the administrators.
func TestReportsFlagCommunity(t *testing.T) {
	alice, bob, id, _ := create_community_test_env(t)
	report_capture(t, alice)
	flag := map[string]any{"@context": activitystreams, "id": bob.id + "#flags/c1", "type": "Flag", "actor": bob.id, "object": []any{"https://example.com/~" + id + "/posts/1"}, "content": "Off topic"}
	if w := signed_post(t, bob, "/~"+id+"/inbox", flag, nil); w.Code != http.StatusAccepted {
		t.Fatalf("flag answered %d", w.Code)
	}
	if queued, _ := networks_database().exists("select 1 from inbound where type='Flag' and account=?", id); !queued {
		t.Error("THE POINT: the community's Flag was not queued for its app")
	}
	if rows := reports_rows(t); len(rows) != 0 {
		t.Errorf("THE POINT: the community's Flag became a report: %v", rows)
	}
}

// TestReportsBlockPerson: a person the owner blocks is no follower of anyone
// here, what they send the inbox is dropped and they are not looked up; the
// block is the owner's, added, listed and lifted through the API; and a
// person or a domain of this server's own cannot be blocked.
func TestReportsBlockPerson(t *testing.T) {
	alice, bob := create_activitypub_test_env(t)
	db := db_user(alice, "user")
	db.exec("insert into followers (account, actor, inbox, shared, created) values ('f1', ?, ?, '', 1)", bob.id, bob.inbox)
	db.exec("insert into following (account, actor, inbox, activity, accepted, created) values ('f1', ?, ?, 'https://example.com/~f1/follows/x', 1, 1)", bob.id, bob.inbox)

	app := create_external_app("settings")
	admin := create_test_admin(t, "u-root")
	thread := create_test_thread(admin, app)
	permission_grant(admin, app.id, "settings/write")
	permission_grant(admin, app.id, "server/read")
	fn := sl.NewBuiltin("test", nil)
	check := func(target string) string {
		value, err := api_server_block_check(thread, fn, sl.Tuple{sl.String(target)}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return string(value.(sl.String))
	}
	// A domain is this server's own where a hosted account is on it, on no
	// network but the fediverse.
	actor_register(alice, "f2", "fediverse", "own.example", "alice")
	for target, want := range map[string]string{bob.id: "", "spam.example": "", "@bot:matrix.example": "", "xmpp:troll@chat.example": "", "https://example.com/~f1": "local", "example.com": "local", "own.example": "local", "https://own.example/~f2": "local", "not a thing": "invalid"} {
		if got := check(target); got != want {
			t.Errorf("THE POINT: check(%q) = %q, want %q", target, got, want)
		}
	}
	if _, err := api_server_block_add(thread, fn, sl.Tuple{sl.String("https://example.com/~f1")}, nil); err == nil {
		t.Error("THE POINT: a person here was blocked")
	}
	if _, err := api_server_block_add(thread, fn, sl.Tuple{sl.String("example.com")}, nil); err == nil {
		t.Error("THE POINT: this server's own domain was blocked")
	}

	if _, err := api_server_block_add(thread, fn, sl.Tuple{sl.String(bob.id)}, nil); err != nil {
		t.Fatal(err)
	}
	if !server_blocked(bob.id) {
		t.Fatal("THE POINT: the person is not blocked")
	}
	if found, _ := db.exists("select 1 from followers where actor=?", bob.id); found {
		t.Error("THE POINT: the blocked person is still a follower")
	}
	if found, _ := db.exists("select 1 from following where actor=?", bob.id); found {
		t.Error("THE POINT: the blocked person is still followed")
	}
	if w := signed_post(t, bob, "/~f1/inbox", follow_activity(bob, bob.id+"#follows/9"), nil); w.Code != http.StatusAccepted {
		t.Fatalf("follow answered %d", w.Code)
	}
	if found, _ := db.exists("select 1 from followers where actor=?", bob.id); found {
		t.Error("THE POINT: the blocked person's follow was taken")
	}
	if queued, _ := networks_database().exists("select 1 from inbound where actor=?", bob.id); queued {
		t.Error("THE POINT: the blocked person's activity was queued")
	}
	if err := resolve_allowed(alice, bob.id); err == nil {
		t.Error("THE POINT: the blocked person may be looked up")
	}

	value, err := api_server_block_list(thread, fn, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if list := sl_decode(value).([]any); len(list) != 1 || list[0].(map[string]any)["actor"] != bob.id {
		t.Errorf("server blocks = %v", list)
	}
	if _, err := api_server_block_remove(thread, fn, sl.Tuple{sl.String(bob.id)}, nil); err != nil {
		t.Fatal(err)
	}
	if server_blocked(bob.id) || resolve_allowed(alice, bob.id) != nil {
		t.Error("the lifted block still applies")
	}
}

// TestReportsMastodon: a report from a Mastodon client is kept for the
// administrators, with the statuses named and the comment, and goes on to
// the person's server unless the client asks it not to; someone here is
// reported to the administrators alone; the user cannot report themselves.
func TestReportsMastodon(t *testing.T) {
	alice, _ := create_mastodon_test_env(t)
	told := report_capture(t, alice)
	token := mastodon_token(t, alice, "read write")
	resolve_lock.Lock()
	resolve_cache["https://remote.example/users/bob"] = resolve_entry{value: &resolved{Network: "fediverse", Type: "person", ID: "https://remote.example/users/bob", Username: "bob", Host: "remote.example", Inbox: "https://remote.example/users/bob/inbox", Shared: "https://remote.example/inbox"}, expires: time.Now().Add(time.Hour)}
	resolve_lock.Unlock()
	db_open("db/networks.db").exec("delete from deliveries")
	bob := mastodon_number(alice, "account", "https://remote.example/users/bob", 0)
	mastodon_snapshot(alice, "account", "https://remote.example/users/bob", map[string]any{"id": bob, "acct": "bob@remote.example"})
	mastodon_snapshot(alice, "status", "note:https://remote.example/notes/9", map[string]any{"id": "x"})
	note := mastodon_number(alice, "status", "note:https://remote.example/notes/9", 0)

	w, answer := mastodon_request(t, token, "POST", "/api/v1/reports", map[string]any{"account_id": bob, "status_ids": []string{note}, "comment": "Spam, again", "category": "spam", "forward": false})
	if w.Code != http.StatusOK || mastodon_map(answer)["forwarded"] != false {
		t.Fatalf("the report = %d %v", w.Code, answer)
	}
	rows := reports_rows(t)
	if len(rows) != 1 {
		t.Fatalf("THE POINT: reports = %v", rows)
	}
	row := rows[0]
	if row_string(row, "reporter") != account_url("example.com", "f1") || row_string(row, "person") != "https://remote.example/users/bob" || row_string(row, "user") != "" || row_string(row, "category") != "spam" || row_string(row, "comment") != "Spam, again" {
		t.Errorf("THE POINT: the report = %v", row)
	}
	if objects := report_objects(row); len(objects) != 1 || objects[0] != "https://remote.example/notes/9" {
		t.Errorf("objects = %v", objects)
	}
	if found, _ := db_open("db/networks.db").exists("select 1 from deliveries"); found {
		t.Error("THE POINT: a report the client asked not be forwarded was sent on")
	}
	if len(*told) == 0 {
		t.Error("the administrators were not told")
	}

	w, answer = mastodon_request(t, token, "POST", "/api/v1/reports", map[string]any{"account_id": bob, "comment": "And again"})
	if w.Code != http.StatusOK || mastodon_map(answer)["forwarded"] != true {
		t.Fatalf("the forwarded report = %d %v", w.Code, answer)
	}
	if found, _ := db_open("db/networks.db").exists("select 1 from deliveries"); !found {
		t.Error("a report was not sent on by default")
	}
	if rows := reports_rows(t); len(rows) != 2 {
		t.Errorf("THE POINT: a forwarded report was not kept here: %d rows", len(rows))
	}

	self := mastodon_number(alice, "account", "identity:e-alice", 0)
	if w, refused := mastodon_request(t, token, "POST", "/api/v1/reports", map[string]any{"account_id": self, "comment": "x"}); w.Code != http.StatusUnprocessableEntity || mastodon_map(refused)["error"] != "Validation failed: You cannot report yourself" {
		t.Errorf("reporting oneself = %d %v", w.Code, refused)
	}
}

// TestReportsMatrixClient: a Matrix client's report of an event is kept with
// its sender, the room, the event and its text; one the user cannot see, or
// of a room not here, is not found; a room and a person are reported too.
func TestReportsMatrixClient(t *testing.T) {
	alice, root := create_clients_test_env(t)
	report_capture(t, root)
	ctx := t.Context()
	a := clients_client(t, alice, "m1", "@alice:example.com", "ALICE")
	r := clients_client(t, root, "m2", "@root:example.com", "ROOT")
	made, err := a.CreateRoom(ctx, &mautrix.ReqCreateRoom{Preset: "private_chat", Name: "Plans", Invite: []id.UserID{"@root:example.com"}})
	if err != nil {
		t.Fatal(err)
	}
	room := made.RoomID
	if _, err := r.JoinRoomByID(ctx, room); err != nil {
		t.Fatal(err)
	}
	sent, err := a.SendMessageEvent(ctx, room, event.EventMessage, map[string]any{"msgtype": "m.text", "body": "rude words"})
	if err != nil {
		t.Fatal(err)
	}
	path := "/_matrix/client/v3/rooms/" + url.PathEscape(room.String()) + "/report"
	if status, body := clients_request(t, r, "POST", path+"/"+url.PathEscape(sent.EventID.String()), map[string]any{"reason": "Rude", "score": -100}); status != 200 {
		t.Fatalf("report = %d %v", status, body)
	}
	rows := reports_rows(t)
	if len(rows) != 1 {
		t.Fatalf("THE POINT: reports = %v", rows)
	}
	row := rows[0]
	if row_string(row, "network") != "matrix" || row_string(row, "reporter") != "@root:example.com" || row_string(row, "person") != "@alice:example.com" || row_string(row, "user") != alice.UID || row_string(row, "room") != room.String() || row_string(row, "content") != "rude words" || row_string(row, "comment") != "Rude" {
		t.Errorf("THE POINT: the report = %v", row)
	}
	if objects := report_objects(row); len(objects) != 1 || objects[0] != sent.EventID.String() {
		t.Errorf("objects = %v", objects)
	}
	if view := report_view(row); view["room"].(map[string]any)["name"] != "Plans" {
		t.Errorf("the room is shown as %v", view["room"])
	}

	if status, body := clients_request(t, r, "POST", path+"/$nothing", map[string]any{"reason": "x"}); status != 404 || body["errcode"] != "M_NOT_FOUND" {
		t.Errorf("an unknown event = %d %v", status, body)
	}
	private, err := a.CreateRoom(ctx, &mautrix.ReqCreateRoom{Preset: "private_chat", Name: "Alone"})
	if err != nil {
		t.Fatal(err)
	}
	unseen, err := a.SendMessageEvent(ctx, private.RoomID, event.EventMessage, map[string]any{"msgtype": "m.text", "body": "not for root"})
	if err != nil {
		t.Fatal(err)
	}
	if status, body := clients_request(t, r, "POST", "/_matrix/client/v3/rooms/"+url.PathEscape(private.RoomID.String())+"/report/"+url.PathEscape(unseen.EventID.String()), map[string]any{"reason": "x"}); status != 404 || body["errcode"] != "M_NOT_FOUND" {
		t.Errorf("THE POINT: an event in a room the reporter is not in = %d %v", status, body)
	}
	if status, body := clients_request(t, r, "POST", "/_matrix/client/v3/rooms/"+url.PathEscape("!nowhere:example.com")+"/report", map[string]any{"reason": "x"}); status != 404 || body["errcode"] != "M_NOT_FOUND" {
		t.Errorf("an unknown room = %d %v", status, body)
	}
	if status, _ := clients_request(t, r, "POST", path, map[string]any{"reason": "The whole room"}); status != 200 {
		t.Errorf("a room report = %d", status)
	}
	if status, _ := clients_request(t, r, "POST", "/_matrix/client/v3/users/"+url.PathEscape("@spammer:remote.example")+"/report", map[string]any{"reason": "Spam"}); status != 200 {
		t.Errorf("a person report = %d", status)
	}
	if status, _ := clients_request(t, r, "POST", "/_matrix/client/v3/users/nobody/report", map[string]any{"reason": "x"}); status != 400 {
		t.Errorf("a report of no user id = %d, want 400", status)
	}
	rows = reports_rows(t)
	if len(rows) != 3 || row_string(rows[1], "person") != "" || row_string(rows[1], "room") != room.String() || row_string(rows[2], "person") != "@spammer:remote.example" || row_string(rows[2], "user") != "" {
		t.Errorf("THE POINT: the room and person reports = %v", rows)
	}
}

// TestReportsMatrixBlocked: what a person the owner blocks says in a room is
// not shown, while their state events are taken; their typing is not taken,
// their invitation is refused, and they may not join or knock on a room here.
func TestReportsMatrixBlocked(t *testing.T) {
	alice, log := create_matrix_test_env(t)
	r := create_matrix_remote(t, gomatrixserverlib.RoomVersionV10)
	room := r.room("Blocked", "invite")
	matrix_join(t, r, alice, room)
	bob := r.user("bob")
	shown := func(body string) bool {
		return len(log.matching(func(e map[string]any) bool { return e["kind"] == "message" && e["body"] == body })) > 0
	}
	typing := func() int {
		return len(log.matching(func(e map[string]any) bool { return e["kind"] == "typing" }))
	}
	r.push(t, "before", r.event(room, bob, "m.room.message", nil, map[string]any{"msgtype": "m.text", "body": "before the block"}))
	if !shown("before the block") {
		t.Fatal("CONTROL: a message before the block was not shown")
	}
	if err := server_blocked_add(bob); err != nil {
		t.Fatal(err)
	}
	r.push(t, "after", r.event(room, bob, "m.room.message", nil, map[string]any{"msgtype": "m.text", "body": "after the block"}))
	if shown("after the block") {
		t.Error("THE POINT: a blocked person's message was shown")
	}
	empty := ""
	r.push(t, "named", r.event(room, bob, "m.room.name", &empty, map[string]any{"name": "Renamed"}))
	current, _ := rooms_current(room)
	if named := rooms_event(current[rooms_key("m.room.name", "")]); named == nil || !strings.Contains(string(named.Content()), "Renamed") {
		t.Error("THE POINT: a blocked person's state event was not taken")
	}
	r.signal(t, "typing", matrix_edu("m.typing", map[string]any{"room_id": room, "user_id": bob, "typing": true}))
	if typing() != 0 {
		t.Error("THE POINT: a blocked person's typing was taken")
	}

	other := r.room("Another", "invite")
	invitee := "@alice:example.com"
	invite := r.event(other, bob, spec.MRoomMember, &invitee, map[string]any{"membership": "invite"})
	request, _ := fclient.NewInviteV2Request(invite, nil)
	if status, body := r.call(t, "PUT", "/_matrix/federation/v2/invite/"+url.PathEscape(other)+"/"+url.PathEscape(invite.EventID()), request); status != 403 {
		t.Errorf("THE POINT: a blocked person's invitation = %d %v", status, body)
	}
	for _, kind := range []string{"make_join", "make_knock"} {
		if status, body := r.call(t, "GET", "/_matrix/federation/v1/"+kind+"/"+url.PathEscape("!here:example.com")+"/"+url.PathEscape(bob), nil); status != 403 || body["error"] != "the user is blocked" {
			t.Errorf("THE POINT: %s for a blocked person = %d %v", kind, status, body)
		}
	}
}

// TestReportsXMPP: a client's blocking command carrying a spam report keeps
// the report, with the messages it names and the words; the server says it
// takes reports; and a person the owner blocks reaches nobody over a link.
func TestReportsXMPP(t *testing.T) {
	env := create_xmpp_server_env(t)
	fake := env.apps(t)
	report_capture(t, env.bob)
	fake.allow(env.alice, "bob@example.com")
	a, _ := env.pair(t)
	a.send(t, `<iq xmlns="jabber:client" type="get" id="d1" to="example.com"><query xmlns="http://jabber.org/protocol/disco#info"/></iq>`)
	a.expect(t, "THE POINT: reports offered", `id="d1"`, "urn:xmpp:reporting:1")
	a.send(t, `<iq xmlns="jabber:client" type="set" id="k1"><block xmlns="urn:xmpp:blocking"><item jid="bob@example.com"><report xmlns="urn:xmpp:reporting:1" reason="urn:xmpp:reporting:spam"><stanza-id xmlns="urn:xmpp:sid:0" by="alice@example.com" id="s-1"/><text xml:lang="en">Selling things</text></report></item><item jid="quiet@example.com"/></block></iq>`)
	a.expect(t, "the block answered", `id="k1"`, `type="result"`)
	rows := reports_rows(t)
	if len(rows) != 1 {
		t.Fatalf("THE POINT: reports = %v", rows)
	}
	row := rows[0]
	if row_string(row, "network") != "xmpp" || row_string(row, "reporter") != "xmpp:alice@example.com" || row_string(row, "person") != "xmpp:bob@example.com" || row_string(row, "user") != env.bob.UID || row_string(row, "category") != "spam" || row_string(row, "comment") != "Selling things" {
		t.Errorf("THE POINT: the report = %v", row)
	}
	if objects := report_objects(row); len(objects) != 1 || objects[0] != "s-1" {
		t.Errorf("objects = %v", objects)
	}
	if !actor_blocked(env.alice, "xmpp:bob@example.com") {
		t.Error("the reported person was not blocked as well")
	}

	remote := env.remote(t, "remote.test")
	fake.allow(env.alice, "carol@remote.test")
	if err := server_blocked_add("xmpp:carol@remote.test"); err != nil {
		t.Fatal(err)
	}
	x, answer := remote.link(t, false)
	if answer != "valid" {
		t.Fatalf("the remote's link was refused: %s", answer)
	}
	x.write(`<message xmlns="jabber:server" from="carol@remote.test/phone" to="alice@example.com" type="chat" id="i1"><body>From the blocked</body></message>`)
	if s, err := x.next(); err != nil || s.name != "message" || s.get("type") != "error" || !strings.Contains(s.inner, "service-unavailable") {
		t.Errorf("THE POINT: the blocked person was not refused: %v %v", s, err)
	}
	a.silent(t, "THE POINT: the blocked person's message reached alice", "From the blocked")
	a.send(t, `<message xmlns="jabber:client" to="carol@remote.test" type="chat" id="o1"><body>To the blocked</body></message>`)
	a.expect(t, "THE POINT: a message to the blocked person refused", `id="o1"`, `type="error"`)
	if heard := remote.heard("To the blocked"); len(heard) != 0 {
		t.Errorf("THE POINT: a message reached the blocked person: %v", heard)
	}
}

// TestReportsAPI: the administrators list the reports waiting and those
// resolved, each party with its name, whether it can be blocked and its
// server; a report is resolved once, in one of the outcomes.
func TestReportsAPI(t *testing.T) {
	alice, bob := create_activitypub_test_env(t)
	report_capture(t, alice)
	first := report_add(report{Network: "fediverse", Reporter: bob.id, Person: "https://example.com/~f1", User: alice.UID, Comment: "First", Names: map[string]string{bob.id: "remote.example", "https://example.com/~f1": "@alice@example.com"}, Host: "remote.example"})
	second := report_add(report{Network: "xmpp", Reporter: "xmpp:alice@example.com", Person: "xmpp:troll@chat.example", Category: "spam"})
	app := create_external_app("settings")
	admin := create_test_admin(t, "u-root")
	thread := create_test_thread(admin, app)
	permission_grant(admin, app.id, "settings/write")
	permission_grant(admin, app.id, "server/read")
	fn := sl.NewBuiltin("test", nil)
	list := func(state string) []any {
		value, err := api_server_report_list(thread, fn, nil, []sl.Tuple{{sl.String("state"), sl.String(state)}})
		if err != nil {
			t.Fatal(err)
		}
		return sl_decode(value).([]any)
	}
	open := list("open")
	if len(open) != 2 || open[0].(map[string]any)["id"] != first {
		t.Fatalf("open = %v", open)
	}
	view := open[0].(map[string]any)
	reporter, person := view["reporter"].(map[string]any), view["person"].(map[string]any)
	if reporter["name"] != "remote.example" || reporter["blockable"] != true || reporter["host"] != "remote.example" {
		t.Errorf("THE POINT: the reporter = %v", reporter)
	}
	if person["name"] != "@alice@example.com" || person["blockable"] != false || person["host"] != "" || view["user"] != alice.UID {
		t.Errorf("THE POINT: someone here = %v", person)
	}
	troll := open[1].(map[string]any)["person"].(map[string]any)
	if troll["name"] != "troll@chat.example" || troll["blockable"] != true || troll["host"] != "chat.example" {
		t.Errorf("a person on XMPP = %v", troll)
	}

	if _, err := api_server_report_resolve(thread, fn, sl.Tuple{sl.String(first), sl.String("ignored")}, nil); err == nil {
		t.Error("THE POINT: an unknown outcome was taken")
	}
	if _, err := api_server_report_resolve(thread, fn, sl.Tuple{sl.String(first), sl.String("dismissed")}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := api_server_report_resolve(thread, fn, sl.Tuple{sl.String(first), sl.String("blocked")}, nil); err == nil {
		t.Error("THE POINT: a report was resolved twice")
	}
	if open := list("open"); len(open) != 1 || open[0].(map[string]any)["id"] != second {
		t.Errorf("open after resolving = %v", open)
	}
	resolved := list("resolved")
	if len(resolved) != 1 || resolved[0].(map[string]any)["outcome"] != "dismissed" || resolved[0].(map[string]any)["resolved"].(int64) == 0 {
		t.Errorf("resolved = %v", resolved)
	}
	value, err := api_server_report_get(thread, fn, sl.Tuple{sl.String("nothing")}, nil)
	if err != nil || value != sl.None {
		t.Errorf("an unknown report = %v %v", value, err)
	}

	// An ordinary user reads none of it.
	plain := create_test_thread(alice, app)
	if _, err := api_server_report_list(plain, fn, nil, nil); err == nil {
		t.Error("THE POINT: an ordinary user listed the reports")
	}
}

// TestReportsHosts: the record of the servers this one talks to: each
// fediverse host with its failures and waiting deliveries, held back where
// its request limit is reached; each Matrix server's backoff and waiting
// transactions; each XMPP link and the clients signed in.
func TestReportsHosts(t *testing.T) {
	create_matrix_test_env(t)
	db := networks_database()
	db.exec("insert into hosts (host, failures, success, failure, next, error) values ('down.example', 3, 10, 20, 30, 'refused')")
	db.exec("insert into hosts (host, failures, success, failure, next, error) values ('slow.example', 0, 10, 0, 0, '')")
	for i, host := range []string{"down.example", "busy.example", "busy.example"} {
		db.exec("insert into deliveries (id, user, account, network, inbox, host, body, next, created) values (?, 'u', 'f1', 'fediverse', ?, ?, '{}', 0, 1)", uid(), "https://"+host+"/inbox", host)
		_ = i
	}
	for i := 0; i < rate_limit_host.limit; i++ {
		rate_limit_host.allow("busy.example")
		rate_limit_host.allow("slow.example")
	}
	federation_database().exec("insert into destinations (name, failures, retry) values ('matrix.example', 2, 40)")
	federation_database().exec("insert into outbox (destination, kind, body, created) values ('matrix.example', 'pdu', '{}', 1)")
	peer := xmpp_peer_of("example.com", "chat.example")
	peer.lock.Lock()
	peer.failures = 1
	peer.waiting = []string{"<message/>"}
	peer.lock.Unlock()
	t.Cleanup(func() {
		xmpp_peers_lock.Lock()
		delete(xmpp_peers, "example.com chat.example")
		xmpp_peers_lock.Unlock()
	})

	health := hosts_health()
	hosts := map[string]map[string]any{}
	for _, entry := range health["fediverse"].([]any) {
		hosts[entry.(map[string]any)["host"].(string)] = entry.(map[string]any)
	}
	if down := hosts["down.example"]; down == nil || down["failures"] != int64(3) || down["error"] != "refused" || down["waiting"] != int64(1) || down["limited"] != false {
		t.Errorf("THE POINT: a failing host = %v", down)
	}
	if busy := hosts["busy.example"]; busy == nil || busy["waiting"] != int64(2) || busy["limited"] != true {
		t.Errorf("THE POINT: a host at its limit with deliveries waiting = %v", busy)
	}
	if slow := hosts["slow.example"]; slow == nil || slow["limited"] != true {
		t.Errorf("THE POINT: a host at its limit = %v", slow)
	}
	matrix := health["matrix"].([]any)
	if len(matrix) != 1 || matrix[0].(map[string]any)["server"] != "matrix.example" || matrix[0].(map[string]any)["failures"] != int64(2) || matrix[0].(map[string]any)["waiting"] != int64(1) {
		t.Errorf("THE POINT: matrix = %v", matrix)
	}
	xmpp := health["xmpp"].(map[string]any)
	found := false
	for _, entry := range xmpp["links"].([]any) {
		if link := entry.(map[string]any); link["server"] == "chat.example" {
			found = link["domain"] == "example.com" && link["up"] == false && link["failures"] == 1 && link["waiting"] == 1
		}
	}
	if !found {
		t.Errorf("THE POINT: links = %v", xmpp["links"])
	}
}

// TestReportsBlockedElsewhere: Bluesky notifications from someone the owner
// blocks are left out, as are a Mastodon client's statuses by them.
func TestReportsBlockedElsewhere(t *testing.T) {
	alice, _, _ := create_appview_test_env(t)
	thread, fn := atproto_thread(alice), sl.NewBuiltin("test", nil)
	count := func() int {
		value, err := api_account_notifications(thread, fn, sl.Tuple{sl.String("b1")}, nil)
		if err != nil {
			t.Fatal(err)
		}
		items, _ := sl_decode_map(value)["items"].([]any)
		return len(items)
	}
	if count() != 1 {
		t.Fatal("CONTROL: the notification is not there unblocked")
	}
	if err := server_blocked_add("did:plc:bobbobbobbobbobbobbobbob"); err != nil {
		t.Fatal(err)
	}
	if count() != 0 {
		t.Error("THE POINT: a blocked person's notification was given")
	}

	s := &mastodon_session{user: alice, actor: account_url("example.com", "f1"), domain: "example.com"}
	posts := []map[string]any{{"id": "p1", "remote": "https://remote.example/notes/1", "author": map[string]any{"id": "https://remote.example/users/bob", "name": "Bob"}, "body": "hi", "created": int64(1)}}
	if len(mastodon_statuses(s, posts)) != 1 {
		t.Fatal("CONTROL: the status is not there unblocked")
	}
	if err := server_blocked_add(mastodon_string(mastodon_post(s, posts[0])["account"].(map[string]any), "uri")); err != nil {
		t.Fatal(err)
	}
	if len(mastodon_statuses(s, posts)) != 0 {
		t.Error("THE POINT: a blocked person's status was given")
	}
}

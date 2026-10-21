// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: Matrix client
//
// A linked Matrix account, signed in with the user's address and password on
// the home server the address's domain names through its client well-known
// document. The sign-in registers Mochi as a device of the account and issues
// the tokens kept on it, renewed with the refresh token when the server says
// the access token has run out. The manager runs a sync loop per linked
// account for as long as it is linked, handing what arrives to the chat
// functions through linked_inbound as conversation events, and the functions
// at the end send the other way. The rooms and messages themselves are
// chat's.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
)

// matrix_sync_timeout is how long a sync waits at the server for an event
// before answering with nothing. A variable so tests can lower it.
var matrix_sync_timeout = 30 * time.Second

// matrix_address reads a user's address, @user:server, into its parts; a
// bare localpart takes the server given.
func matrix_address(username string, server string) (string, string, error) {
	username = strings.TrimSpace(username)
	server = strings.ToLower(strings.TrimSpace(server))
	if strings.HasPrefix(username, "@") {
		if !resolve_matrix_re.MatchString(username) {
			return "", "", fmt.Errorf("invalid address")
		}
		return username, strings.ToLower(username[strings.IndexByte(username, ':')+1:]), nil
	}
	if username == "" || strings.ContainsAny(username, "@: /") {
		return "", "", fmt.Errorf("invalid address")
	}
	if host, err := mastodon_host(server); err != nil {
		return "", "", fmt.Errorf("a server is needed with a bare user name")
	} else {
		server = host
	}
	return "@" + username + ":" + server, server, nil
}

// matrix_discover finds a server's client interface: the base URL its client
// well-known names, else the server itself.
func matrix_discover(ctx context.Context, user *User, server string) string {
	base := network_scheme + server
	status, body, err := bluesky_fetch(ctx, user, base+"/.well-known/matrix/client", "application/json")
	if err != nil || status != 200 {
		return base
	}
	var known struct {
		Homeserver struct {
			URL string `json:"base_url"`
		} `json:"m.homeserver"`
	}
	if json.Unmarshal(body, &known) == nil {
		if u, err := url.Parse(known.Homeserver.URL); err == nil && u.Scheme+"://" == network_scheme && u.Hostname() != "" {
			return strings.TrimSuffix(known.Homeserver.URL, "/")
		}
	}
	return base
}

// matrix_api is the address of a path under the server's /_matrix/.
func matrix_api(l *linked, path string, query map[string]string) string {
	address := l.get("base") + "/_matrix/" + path
	if len(query) > 0 {
		values := url.Values{}
		for key, value := range query {
			values.Set(key, value)
		}
		address += "?" + values.Encode()
	}
	return address
}

// matrix_error_code reads the error code of a failed answer.
func matrix_error_code(body []byte) string {
	var answer struct {
		Code string `json:"errcode"`
	}
	json.Unmarshal(body, &answer)
	return answer.Code
}

// matrix_link signs in with the fields of the form - the address or user
// name, the password and the server where the address does not name it - as
// a new device named for this server, and answers the account and what to
// store.
func matrix_link(ctx context.Context, user *User, fields map[string]string) (*linked_profile, map[string]any, error) {
	address, server, err := matrix_address(fields["username"], fields["server"])
	if err != nil {
		return nil, nil, err
	}
	if err := host_allowed(user, "", server); err != nil {
		return nil, nil, err
	}
	l := &linked{user: user, kind: "matrix", data: map[string]any{"server": server, "base": matrix_discover(ctx, user, server)}}
	var flows struct {
		Flows []struct {
			Type string `json:"type"`
		} `json:"flows"`
	}
	status, body, err := l.json(ctx, "GET", matrix_api(l, "client/v3/login", nil), nil, &flows)
	if err != nil {
		return nil, nil, err
	}
	if status != 200 {
		return nil, nil, fmt.Errorf("the server does not offer a sign-in: %v", linked_failure(status, body))
	}
	offered := false
	for _, flow := range flows.Flows {
		if flow.Type == "m.login.password" {
			offered = true
		}
	}
	if !offered {
		return nil, nil, fmt.Errorf("the server does not sign in with a password")
	}
	device := "Mochi"
	if domain := server_domain(); domain != "" {
		device = "Mochi (" + domain + ")"
	}
	var session struct {
		User    string `json:"user_id"`
		Token   string `json:"access_token"`
		Device  string `json:"device_id"`
		Refresh string `json:"refresh_token"`
		Expires int64  `json:"expires_in_ms"`
	}
	l.set("token", "")
	status, body, err = l.json(ctx, "POST", matrix_api(l, "client/v3/login", nil), map[string]any{
		"type":                        "m.login.password",
		"identifier":                  map[string]any{"type": "m.id.user", "user": address},
		"password":                    fields["password"],
		"initial_device_display_name": device,
		"refresh_token":               true,
	}, &session)
	if err != nil {
		return nil, nil, err
	}
	if status == 403 || status == 401 {
		return nil, nil, fmt.Errorf("the server did not accept the sign-in: %v", linked_failure(status, body))
	}
	if status != 200 || session.Token == "" || session.User == "" {
		return nil, nil, fmt.Errorf("the sign-in failed: %v", linked_failure(status, body))
	}
	l.set("token", session.Token)
	l.set("refresh", session.Refresh)
	l.set("device", session.Device)
	if session.Expires > 0 {
		l.set("expires", now()+session.Expires/1000)
	}
	profile, err := matrix_profile(ctx, l, session.User)
	if err != nil {
		return nil, nil, err
	}
	return profile, l.data, nil
}

// matrix_profile reads the account's public profile.
func matrix_profile(ctx context.Context, l *linked, address string) (*linked_profile, error) {
	profile := &linked_profile{ID: address, Address: address, Name: address[1:strings.IndexByte(address, ':')], URL: "https://matrix.to/#/" + address, Server: l.get("server")}
	var answer struct {
		Name   string `json:"displayname"`
		Avatar string `json:"avatar_url"`
	}
	status, _, err := l.json(ctx, "GET", matrix_api(l, "client/v3/profile/"+url.PathEscape(address), nil), nil, &answer)
	if err == nil && status == 200 {
		if answer.Name != "" {
			profile.Name = answer.Name
		}
		if rest, ok := strings.CutPrefix(answer.Avatar, "mxc://"); ok && strings.Count(rest, "/") == 1 {
			profile.Avatar = l.get("base") + "/_matrix/client/v1/media/download/" + rest
		}
	}
	return profile, nil
}

// matrix_renew renews the access token with the refresh token.
func matrix_renew(ctx context.Context, l *linked) error {
	refresh := l.get("refresh")
	if refresh == "" {
		return linked_signed_out
	}
	var session struct {
		Token   string `json:"access_token"`
		Refresh string `json:"refresh_token"`
		Expires int64  `json:"expires_in_ms"`
	}
	l.set("token", "")
	status, _, err := l.json(ctx, "POST", matrix_api(l, "client/v3/refresh", nil), map[string]any{"refresh_token": refresh}, &session)
	if err != nil {
		return err
	}
	if status == 401 || status == 403 {
		return linked_signed_out
	}
	if status != 200 || session.Token == "" {
		return fmt.Errorf("the token refresh failed with status %d", status)
	}
	l.set("token", session.Token)
	if session.Refresh != "" {
		l.set("refresh", session.Refresh)
	}
	if session.Expires > 0 {
		l.set("expires", now()+session.Expires/1000)
	}
	l.save()
	return nil
}

// matrix_verify checks the token with the server.
func matrix_verify(ctx context.Context, l *linked) error {
	var answer struct {
		User string `json:"user_id"`
	}
	status, body, err := l.json(ctx, "GET", matrix_api(l, "client/v3/account/whoami", nil), nil, &answer)
	if err != nil {
		return err
	}
	if status == 401 {
		return linked_signed_out
	}
	if status != 200 || answer.User != l.get("id") {
		return linked_failure(status, body)
	}
	return nil
}

// matrix_logout ends the device's session when the account is unlinked, best
// effort.
func matrix_logout(ctx context.Context, l *linked) {
	if l.get("token") == "" {
		return
	}
	l.json(ctx, "POST", matrix_api(l, "client/v3/logout", nil), map[string]any{}, nil)
}

// matrix_request sends an app's own request on the server's interface.
func matrix_request(ctx context.Context, l *linked, method string, path string, query map[string]string, body any) (int, []byte, error) {
	return l.json(ctx, method, matrix_api(l, path, query), body, nil)
}

// === The account's conversations ===
//
// The sync loop reads each sync as conversation events (conversations.go): a
// joined room's messages, edits and redactions, who is typing and how far
// each member has read, its name, members and whether it is encrypted; an
// invitation; a room left; other people's presence. A message this device
// sent is left out, since the app keeps it already. The user's own presence
// is sent as they share it, with every sync and whenever it changes.

// matrix_filter is the sync filter: the latest messages of each room, and a
// room's members as they speak rather than all at once.
const matrix_filter = `{"room":{"timeline":{"limit":20},"state":{"lazy_load_members":true}}}`

// matrix_typing_timeout is how long the server shows the user typing without
// a fresh word.
const matrix_typing_timeout = 10000

// A matrix_event is an event of a sync.
type matrix_event struct {
	Type     string         `json:"type"`
	Sender   string         `json:"sender"`
	ID       string         `json:"event_id"`
	Key      *string        `json:"state_key"`
	Time     int64          `json:"origin_server_ts"`
	Content  map[string]any `json:"content"`
	Redacts  string         `json:"redacts"`
	Unsigned struct {
		Transaction string `json:"transaction_id"`
	} `json:"unsigned"`
}

type matrix_events struct {
	Events []matrix_event `json:"events"`
}

// A matrix_sync is a sync's answer, as far as the loop reads it.
type matrix_sync struct {
	Next  string `json:"next_batch"`
	Rooms struct {
		Join map[string]struct {
			Summary struct {
				Heroes []string `json:"m.heroes"`
				Joined *int     `json:"m.joined_member_count"`
			} `json:"summary"`
			State     matrix_events `json:"state"`
			Timeline  matrix_events `json:"timeline"`
			Ephemeral matrix_events `json:"ephemeral"`
		} `json:"join"`
		Invite map[string]struct {
			State matrix_events `json:"invite_state"`
		} `json:"invite"`
		Leave map[string]json.RawMessage `json:"leave"`
	} `json:"rooms"`
	Presence matrix_events `json:"presence"`
	Data     matrix_events `json:"account_data"`
	Retry    int64         `json:"retry_after_ms"`
}

// A matrix_state is what the loop keeps between syncs: who is typing in each
// room, which rooms are one-to-one, and the names members go by. The engine's
// rooms (rooms.go) read their events with one too, and download is where each
// reads a file from.
type matrix_state struct {
	user     string
	typing   map[string][]string
	direct   map[string]bool
	names    map[string]string
	download func(mxc string) string
	// decrypt reads an encrypted event as the event it carries, nil where
	// the account's device cannot yet, and remember keeps the key of an
	// encrypted file by the address it is fetched at (encryption.go).
	decrypt  func(ctx context.Context, room string, e *matrix_event) *matrix_event
	remember func(address string, file map[string]any)
	// verify hands the account's device a step of a verification in a room,
	// which is its to answer and never a message.
	verify func(ctx context.Context, room string, e *matrix_event)
}

// matrix_presence is how a Matrix account shows a status of Mochi's.
func matrix_presence(status string) string {
	switch status {
	case "online":
		return "online"
	case "away", "busy":
		return "unavailable"
	}
	return "offline"
}

// matrix_run is the account's sync loop: the app is told the session began,
// then each sync waits at the server for events and hands what arrives to
// linked_inbound, with the position kept on the account so a restart carries
// on. It returns when the connection fails, to be run again, or with
// linked_signed_out when the server no longer accepts the account.
func matrix_run(ctx context.Context, l *linked, s *linked_session) error {
	ctx = linked_session_context(ctx)
	since := l.get("since")
	state := &matrix_state{user: l.get("id"), typing: map[string][]string{}, direct: map[string]bool{}, names: map[string]string{}, download: func(mxc string) string { return matrix_download(l, mxc) }}
	// The account's encryption device (encryption.go), which reads the
	// encrypted events and takes what each sync brings it.
	device, failure := encryption_matrix_of(l.user, l.id)
	if failure != nil {
		info("Linked Matrix account %s: no encryption device: %v", l.id, failure)
	} else {
		state.decrypt, state.remember, state.verify = device.decrypt, device.remember, device.dispatch
	}
	linked_inbound(l.user, l.id, "matrix", []map[string]any{{"kind": "session"}})
	shown := matrix_announce(ctx, l, "")
	done := make(chan struct{})
	defer close(done)
	go func() {
		for {
			select {
			case <-s.presence:
				shown = matrix_announce(ctx, l, shown)
			case <-done:
				return
			}
		}
	}()
	for {
		status, _ := linked_presence(l)
		query := map[string]string{"timeout": strconv.FormatInt(matrix_sync_timeout.Milliseconds(), 10), "filter": matrix_filter, "set_presence": matrix_presence(status)}
		if since != "" {
			query["since"] = since
		}
		var sync matrix_sync
		bounded, cancel := context.WithTimeout(ctx, matrix_sync_timeout+linked_timeout)
		code, body, err := l.json(bounded, "GET", matrix_api(l, "client/v3/sync", query), nil, &sync)
		cancel()
		if ctx.Err() != nil {
			return nil
		}
		// A sync over the session's budget waits for the next window rather
		// than ending the session.
		var limited *RateLimitError
		if errors.As(err, &limited) {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(time.Duration(max(limited.Retry, 1)) * time.Second):
			}
			continue
		}
		if err != nil {
			return err
		}
		switch {
		case code == 401:
			return linked_signed_out
		case code == 429:
			wait := time.Duration(sync.Retry) * time.Millisecond
			if wait <= 0 {
				wait = 10 * time.Second
			}
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(wait):
			}
			continue
		case code != 200:
			return linked_failure(code, body)
		}
		s.connected.Store(true)
		s.reason.Store("")
		if device != nil {
			device.sync(ctx, body, since)
		}
		if events := state.read(&sync); len(events) > 0 {
			linked_inbound(l.user, l.id, "matrix", events)
		}
		if sync.Next != "" && sync.Next != since {
			since = sync.Next
			l.store("since", since)
		}
	}
}

// read turns a sync into conversation events.
func (state *matrix_state) read(sync *matrix_sync) []map[string]any {
	var events []map[string]any
	// Which rooms are one-to-one, as the account's own data says.
	for _, e := range sync.Data.Events {
		if e.Type != "m.direct" {
			continue
		}
		direct := map[string]bool{}
		for _, rooms := range e.Content {
			list, _ := rooms.([]any)
			for _, room := range list {
				if id, ok := room.(string); ok {
					direct[id] = true
				}
			}
		}
		state.direct = direct
	}
	for room, joined := range sync.Rooms.Join {
		information := map[string]any{"kind": "room", "conversation": room, "room": !state.direct[room], "direct": state.direct[room]}
		if joined.Summary.Joined != nil {
			information["count"] = *joined.Summary.Joined
		}
		var members []map[string]any
		for _, e := range append(append([]matrix_event{}, joined.State.Events...), joined.Timeline.Events...) {
			if e.Key == nil {
				continue
			}
			switch e.Type {
			case "m.room.name":
				if name, _ := e.Content["name"].(string); name != "" {
					information["name"] = name
				}
			case "m.room.encryption":
				information["encrypted"] = true
			case "m.room.member":
				user := *e.Key
				membership, _ := e.Content["membership"].(string)
				name, _ := e.Content["displayname"].(string)
				if name != "" {
					state.names[room+" "+user] = name
				}
				if user == state.user {
					continue
				}
				members = append(members, map[string]any{"kind": "member", "conversation": room, "id": user, "name": name, "left": membership != "join" && membership != "invite"})
			}
		}
		// The members a room without a name is called after, as other clients
		// show it.
		var heroes []any
		for _, hero := range joined.Summary.Heroes {
			heroes = append(heroes, map[string]any{"id": hero, "name": state.names[room+" "+hero]})
		}
		if len(heroes) > 0 {
			information["heroes"] = heroes
		}
		events = append(events, information)
		events = append(events, members...)
		for _, e := range joined.Timeline.Events {
			if event := state.timeline(room, &e); event != nil {
				events = append(events, event)
			}
		}
		for _, e := range joined.Ephemeral.Events {
			events = append(events, state.ephemeral(room, &e)...)
		}
	}
	for room, invited := range sync.Rooms.Invite {
		invite := map[string]any{"kind": "invite", "conversation": room, "name": ""}
		for _, e := range invited.State.Events {
			switch {
			case e.Type == "m.room.name":
				invite["name"], _ = e.Content["name"].(string)
			case e.Type == "m.room.member" && e.Key != nil && *e.Key == state.user:
				invite["from"] = e.Sender
				direct, _ := e.Content["is_direct"].(bool)
				invite["direct"] = direct
			}
		}
		if invite["from"] != nil {
			events = append(events, invite)
		}
	}
	for room := range sync.Rooms.Leave {
		delete(state.typing, room)
		events = append(events, map[string]any{"kind": "room", "conversation": room, "left": true})
	}
	for _, e := range sync.Presence.Events {
		if e.Type != "m.presence" || e.Sender == state.user {
			continue
		}
		presence, _ := e.Content["presence"].(string)
		status := "offline"
		switch presence {
		case "online":
			status = "online"
		case "unavailable":
			status = "away"
		}
		message, _ := e.Content["status_msg"].(string)
		events = append(events, map[string]any{"kind": "presence", "from": e.Sender, "status": status, "message": message})
	}
	return events
}

// timeline reads one event of a room's timeline: a message, an edit, an
// encrypted message, or a redaction.
func (state *matrix_state) timeline(room string, e *matrix_event) map[string]any {
	own := e.Sender == state.user
	// What this device sent carries its transaction id; the app has it.
	if own && e.Unsigned.Transaction != "" {
		return nil
	}
	// An encrypted event is what it carries, still marked encrypted.
	sealed := e.Type == "m.room.encrypted"
	if sealed && state.decrypt != nil {
		if decrypted := state.decrypt(context.Background(), room, e); decrypted != nil {
			e = decrypted
		}
	}
	if strings.HasPrefix(e.Type, "m.key.verification.") || (e.Type == "m.room.message" && e.Content["msgtype"] == "m.key.verification.request") {
		if state.verify != nil {
			state.verify(context.Background(), room, e)
		}
		return nil
	}
	switch e.Type {
	case "m.room.redaction":
		redacts := e.Redacts
		if redacts == "" {
			redacts, _ = e.Content["redacts"].(string)
		}
		if redacts == "" {
			return nil
		}
		return map[string]any{"kind": "delete", "conversation": room, "from": e.Sender, "id": redacts, "own": own}
	case "m.reaction":
		relates, _ := e.Content["m.relates_to"].(map[string]any)
		relation, _ := relates["rel_type"].(string)
		target, _ := relates["event_id"].(string)
		key, _ := relates["key"].(string)
		if relation != "m.annotation" || target == "" || key == "" || e.ID == "" {
			return nil
		}
		return map[string]any{"kind": "reaction", "conversation": room, "from": e.Sender, "id": target, "reaction": key, "event": e.ID, "own": own}
	case "m.room.encrypted":
		// Its key not come: an edit or a reaction comes once it can be read,
		// a message is shown waiting and filled in then.
		relates, _ := e.Content["m.relates_to"].(map[string]any)
		if relation, _ := relates["rel_type"].(string); relation == "m.replace" || relation == "m.annotation" {
			return nil
		}
		return map[string]any{"kind": "message", "conversation": room, "room": !state.direct[room], "from": e.Sender, "name": state.names[room+" "+e.Sender], "id": e.ID, "body": "", "created": e.Time / 1000, "own": own, "encrypted": true}
	case "m.room.message", "m.sticker":
	default:
		return nil
	}
	if e.Key != nil || e.ID == "" {
		return nil
	}
	relates, _ := e.Content["m.relates_to"].(map[string]any)
	relation, _ := relates["rel_type"].(string)
	if relation == "m.replace" {
		replaced, _ := relates["event_id"].(string)
		updated, _ := e.Content["m.new_content"].(map[string]any)
		body, _ := updated["body"].(string)
		if replaced == "" || body == "" {
			return nil
		}
		return map[string]any{"kind": "edit", "conversation": room, "from": e.Sender, "id": replaced, "body": body, "own": own}
	}
	body, _ := e.Content["body"].(string)
	message := map[string]any{"kind": "message", "conversation": room, "room": !state.direct[room], "from": e.Sender, "name": state.names[room+" "+e.Sender], "id": e.ID, "created": e.Time / 1000, "own": own, "encrypted": sealed}
	if relation == "m.thread" {
		message["thread"], _ = relates["event_id"].(string)
	} else if reply, _ := relates["m.in_reply_to"].(map[string]any); reply != nil {
		message["reply"], _ = reply["event_id"].(string)
		body = matrix_unquote(body)
	}
	kind, _ := e.Content["msgtype"].(string)
	if e.Type == "m.sticker" {
		kind = "m.image"
	}
	switch kind {
	case "m.image", "m.file", "m.video", "m.audio":
		mxc, _ := e.Content["url"].(string)
		// An encrypted room's file is encrypted, its key in the message.
		sealed, _ := e.Content["file"].(map[string]any)
		if mxc == "" && sealed != nil {
			mxc, _ = sealed["url"].(string)
		}
		address := state.download(mxc)
		if address == "" {
			return nil
		}
		if sealed != nil && state.remember != nil {
			state.remember(address, sealed)
		}
		name, _ := e.Content["filename"].(string)
		if name == "" {
			name = body
		}
		file := map[string]any{"url": address, "name": name}
		if information, _ := e.Content["info"].(map[string]any); information != nil {
			if mime, _ := information["mimetype"].(string); mime != "" {
				file["type"] = mime
			}
			if size, ok := information["size"].(float64); ok {
				file["size"] = int64(size)
			}
		}
		message["files"] = []any{file}
		// A body beside a file name is the file's caption.
		if body == name {
			body = ""
		}
	case "m.emote":
		body = "* " + body
	}
	message["body"] = body
	return message
}

// matrix_unquote takes off the quoted message a reply begins with for clients
// that do not read replies.
func matrix_unquote(body string) string {
	if !strings.HasPrefix(body, "> ") {
		return body
	}
	lines := strings.Split(body, "\n")
	i := 0
	for i < len(lines) && strings.HasPrefix(lines[i], ">") {
		i++
	}
	if i < len(lines) && lines[i] == "" {
		i++
	}
	return strings.Join(lines[i:], "\n")
}

// ephemeral reads a room's passing events: who is typing, compared with who
// was, and how far each member has read.
func (state *matrix_state) ephemeral(room string, e *matrix_event) []map[string]any {
	var events []map[string]any
	switch e.Type {
	case "m.typing":
		var now []string
		list, _ := e.Content["user_ids"].([]any)
		for _, user := range list {
			if id, ok := user.(string); ok && id != state.user {
				now = append(now, id)
			}
		}
		for _, user := range now {
			events = append(events, map[string]any{"kind": "typing", "conversation": room, "from": user, "typing": true})
		}
		for _, user := range state.typing[room] {
			if !slices.Contains(now, user) {
				events = append(events, map[string]any{"kind": "typing", "conversation": room, "from": user, "typing": false})
			}
		}
		state.typing[room] = now
	case "m.receipt":
		for event, kinds := range e.Content {
			receipts, _ := kinds.(map[string]any)
			readers, _ := receipts["m.read"].(map[string]any)
			for user := range readers {
				if user != state.user {
					events = append(events, map[string]any{"kind": "receipt", "conversation": room, "from": user, "id": event, "displayed": true})
				}
			}
		}
	}
	return events
}

// matrix_download is the address a file's mxc address is fetched from, with
// the account's credential, on its server.
func matrix_download(l *linked, mxc string) string {
	rest, ok := strings.CutPrefix(mxc, "mxc://")
	if !ok || strings.Count(rest, "/") != 1 || strings.ContainsAny(rest, "?#") {
		return ""
	}
	return l.get("base") + "/_matrix/client/v1/media/download/" + rest
}

// matrix_announce sends the user's presence as the account shows it, unless
// it is what was shown before, and answers what it shows.
func matrix_announce(ctx context.Context, l *linked, before string) string {
	status, message := linked_presence(l)
	shown := status + "\n" + message
	user := l.get("id")
	if user == "" || shown == before {
		return shown
	}
	body := map[string]any{"presence": matrix_presence(status)}
	if message != "" && status != "offline" {
		body["status_msg"] = message
	}
	bounded, cancel := context.WithTimeout(ctx, linked_timeout)
	defer cancel()
	l.json(bounded, "PUT", matrix_api(l, "client/v3/presence/"+url.PathEscape(user)+"/status", nil), body, nil)
	return shown
}

// === Sending ===

// matrix_txn is the transaction id of a send: the app's key where it gave
// one, so a repeat is the same event, else a fresh one.
func matrix_txn(key string) string {
	if key == "" {
		return "mochi" + uid()
	}
	return "mochi" + strings.NewReplacer("|", "-", "/", "-").Replace(key)
}

// matrix_send sends an event of a kind to a room, answering its id.
func matrix_send(ctx context.Context, l *linked, room string, kind string, content map[string]any, key string) (string, error) {
	// An encrypted room's events go encrypted by the account's device.
	if kind != "m.room.encrypted" {
		var err error
		if kind, content, err = encryption_seal(ctx, l.user, l.id, room, kind, content); err != nil {
			return "", err
		}
	}
	var answer struct {
		ID string `json:"event_id"`
	}
	status, body, err := l.json(ctx, "PUT", matrix_api(l, "client/v3/rooms/"+url.PathEscape(room)+"/send/"+url.PathEscape(kind)+"/"+url.PathEscape(matrix_txn(key)), nil), content, &answer)
	if err != nil {
		return "", err
	}
	if status != 200 || answer.ID == "" {
		return "", linked_failure(status, body)
	}
	return answer.ID, nil
}

// matrix_type is the event an app's object is sent as: a reaction, or a
// message.
func matrix_type(object map[string]any) string {
	if reaction, _ := object["reaction"].(string); reaction != "" {
		return "m.reaction"
	}
	return "m.room.message"
}

// matrix_content is a message's content from an app's object: its text, or a
// file the account uploaded with the text as its caption, a reply to another
// message or a reply in a thread; or a reaction to a message by its id.
func matrix_content(object map[string]any) (map[string]any, error) {
	if reaction, _ := object["reaction"].(string); reaction != "" {
		target, _ := object["message"].(string)
		if !strings.HasPrefix(target, "$") || len(reaction) > 100 {
			return nil, fmt.Errorf("a reaction needs the message it is to")
		}
		return map[string]any{"m.relates_to": map[string]any{"rel_type": "m.annotation", "event_id": target, "key": reaction}}, nil
	}
	text, _ := object["content"].(string)
	content := map[string]any{"msgtype": "m.text", "body": text}
	if file, _ := object["file"].(map[string]any); file != nil {
		mxc, _ := file["url"].(string)
		if !strings.HasPrefix(mxc, "mxc://") {
			return nil, fmt.Errorf("a file is sent by the address its upload answered")
		}
		name, _ := file["name"].(string)
		kind, _ := file["type"].(string)
		msgtype := "m.file"
		switch {
		case strings.HasPrefix(kind, "image/"):
			msgtype = "m.image"
		case strings.HasPrefix(kind, "video/"):
			msgtype = "m.video"
		case strings.HasPrefix(kind, "audio/"):
			msgtype = "m.audio"
		}
		information := map[string]any{}
		if kind != "" {
			information["mimetype"] = kind
		}
		if size := as_int64(file["size"]); size > 0 {
			information["size"] = size
		}
		content = map[string]any{"msgtype": msgtype, "body": name, "filename": name, "url": mxc, "info": information}
		if text != "" {
			content["body"] = text
		}
	} else if text == "" {
		return nil, fmt.Errorf("a message needs content or a file")
	}
	thread, _ := object["thread"].(string)
	reply, _ := object["reply"].(string)
	switch {
	case thread != "":
		relation := map[string]any{"rel_type": "m.thread", "event_id": thread, "is_falling_back": true}
		if reply != "" {
			relation["m.in_reply_to"] = map[string]any{"event_id": reply}
		}
		content["m.relates_to"] = relation
	case reply != "":
		content["m.relates_to"] = map[string]any{"m.in_reply_to": map[string]any{"event_id": reply}}
	}
	return content, nil
}

// matrix_create sends a message to the room the object names.
func matrix_create(ctx context.Context, l *linked, object map[string]any, key string) (map[string]any, error) {
	room, _ := object["conversation"].(string)
	if !strings.HasPrefix(room, "!") {
		return nil, fmt.Errorf("a message needs the room it goes to")
	}
	content, err := matrix_content(object)
	if err != nil {
		return nil, err
	}
	id, err := matrix_send(ctx, l, room, matrix_type(object), content, key)
	if err != nil {
		return nil, err
	}
	return map[string]any{"id": id, "url": "https://matrix.to/#/" + room + "/" + id, "data": content}, nil
}

// matrix_update sends a new version of a message the account sent.
func matrix_update(ctx context.Context, l *linked, id string, object map[string]any) (map[string]any, error) {
	room, _ := object["conversation"].(string)
	text, _ := object["content"].(string)
	if !strings.HasPrefix(room, "!") || text == "" {
		return nil, fmt.Errorf("an edit needs the room and the new text")
	}
	content := map[string]any{
		"msgtype":       "m.text",
		"body":          "* " + text,
		"m.new_content": map[string]any{"msgtype": "m.text", "body": text},
		"m.relates_to":  map[string]any{"rel_type": "m.replace", "event_id": id},
	}
	edit, err := matrix_send(ctx, l, room, "m.room.message", content, "")
	if err != nil {
		return nil, err
	}
	return map[string]any{"id": id, "url": "https://matrix.to/#/" + room + "/" + id, "data": map[string]any{"edit": edit}}, nil
}

// matrix_delete redacts a message of a room.
func matrix_delete(ctx context.Context, l *linked, id string, room string) error {
	if !strings.HasPrefix(room, "!") {
		return fmt.Errorf("a redaction needs the room")
	}
	status, body, err := l.json(ctx, "PUT", matrix_api(l, "client/v3/rooms/"+url.PathEscape(room)+"/redact/"+url.PathEscape(id)+"/"+url.PathEscape(matrix_txn("")), nil), map[string]any{}, nil)
	if err != nil {
		return err
	}
	if status != 200 && status != 404 {
		return linked_failure(status, body)
	}
	return nil
}

// matrix_typing tells a room the user is typing, or stopped.
func matrix_typing(ctx context.Context, l *linked, room string, typing bool) error {
	body := map[string]any{"typing": typing}
	if typing {
		body["timeout"] = matrix_typing_timeout
	}
	status, answer, err := l.json(ctx, "PUT", matrix_api(l, "client/v3/rooms/"+url.PathEscape(room)+"/typing/"+url.PathEscape(l.get("id")), nil), body, nil)
	if err != nil {
		return err
	}
	if status != 200 {
		return linked_failure(status, answer)
	}
	return nil
}

// matrix_read says how far the user has read a room: to its members where
// they share it, and to the server alone otherwise, which still keeps the
// unread count of the user's other clients right.
func matrix_read(ctx context.Context, l *linked, room string, id string, shared bool) error {
	kind := "m.read.private"
	if shared {
		kind = "m.read"
	}
	status, answer, err := l.json(ctx, "POST", matrix_api(l, "client/v3/rooms/"+url.PathEscape(room)+"/receipt/"+kind+"/"+url.PathEscape(id), nil), map[string]any{}, nil)
	if err != nil {
		return err
	}
	if status != 200 {
		return linked_failure(status, answer)
	}
	return nil
}

// matrix_upload puts a file in the server's media store and answers its mxc
// address for a message to name.
func matrix_upload(ctx context.Context, l *linked, content []byte, kind string, name string) (map[string]any, error) {
	if kind == "" {
		kind = "application/octet-stream"
	}
	status, body, _, err := l.request(ctx, "POST", matrix_api(l, "media/v3/upload", map[string]string{"filename": name}), map[string]string{"Content-Type": kind}, content)
	if err != nil {
		return nil, err
	}
	var answer struct {
		URI string `json:"content_uri"`
	}
	if status != 200 || json.Unmarshal(body, &answer) != nil || answer.URI == "" {
		return nil, linked_failure(status, body)
	}
	return map[string]any{"url": answer.URI, "name": name, "type": kind, "size": len(content)}, nil
}

// matrix_name is the name a person goes by on Matrix, which fails for an
// address the server knows no one at.
func matrix_name(ctx context.Context, l *linked, person string) (string, error) {
	if !resolve_matrix_re.MatchString(person) {
		return "", fmt.Errorf("invalid address")
	}
	var answer struct {
		Name string `json:"displayname"`
	}
	status, body, err := l.json(ctx, "GET", matrix_api(l, "client/v3/profile/"+url.PathEscape(person), nil), nil, &answer)
	if err != nil {
		return "", err
	}
	if status != 200 {
		return "", linked_failure(status, body)
	}
	if answer.Name == "" {
		answer.Name = person[1:strings.IndexByte(person, ':')]
	}
	return answer.Name, nil
}

// matrix_directory searches the user directory of a linked account's server,
// answering at most twenty people as {id, name}.
func matrix_directory(ctx context.Context, l *linked, query string) ([]map[string]any, error) {
	var answer struct {
		Results []struct {
			ID   string `json:"user_id"`
			Name string `json:"display_name"`
		} `json:"results"`
	}
	status, body, err := l.json(ctx, "POST", matrix_api(l, "client/v3/user_directory/search", nil), map[string]any{"search_term": query, "limit": 20}, &answer)
	if err != nil {
		return nil, err
	}
	if status != 200 {
		return nil, linked_failure(status, body)
	}
	people := []map[string]any{}
	for _, result := range answer.Results {
		if result.ID == "" || result.ID == l.get("id") {
			continue
		}
		name := result.Name
		if name == "" {
			name = strings.TrimPrefix(strings.SplitN(result.ID, ":", 2)[0], "@")
		}
		people = append(people, map[string]any{"id": result.ID, "name": name})
	}
	return people, nil
}

// matrix_room_create makes a private room with the people invited: a
// one-to-one conversation, marked so in the account's data, for one person;
// a named group for several.
func matrix_room_create(ctx context.Context, l *linked, people []string, name string) (string, error) {
	invite := []any{}
	for _, person := range people {
		invite = append(invite, person)
	}
	body := map[string]any{"invite": invite, "preset": "private_chat"}
	direct := len(people) == 1
	if direct {
		body["is_direct"] = true
		body["preset"] = "trusted_private_chat"
	} else if name != "" {
		body["name"] = name
	}
	var answer struct {
		Room string `json:"room_id"`
	}
	status, data, err := l.json(ctx, "POST", matrix_api(l, "client/v3/createRoom", nil), body, &answer)
	if err != nil {
		return "", err
	}
	if status != 200 || answer.Room == "" {
		return "", linked_failure(status, data)
	}
	if direct {
		matrix_direct_add(ctx, l, people[0], answer.Room)
	}
	return answer.Room, nil
}

// matrix_direct_add marks a room as the account's one-to-one conversation
// with a person, as other clients read it.
func matrix_direct_add(ctx context.Context, l *linked, person string, room string) {
	address := matrix_api(l, "client/v3/user/"+url.PathEscape(l.get("id"))+"/account_data/m.direct", nil)
	direct := map[string]any{}
	if status, _, err := l.json(ctx, "GET", address, nil, &direct); err != nil || (status != 200 && status != 404) {
		return
	}
	rooms, _ := direct[person].([]any)
	direct[person] = append(rooms, room)
	l.json(ctx, "PUT", address, direct, nil)
}

// matrix_room_join joins a room by its id or an alias, answering its id.
func matrix_room_join(ctx context.Context, l *linked, address string) (string, error) {
	if !strings.HasPrefix(address, "!") && !strings.HasPrefix(address, "#") {
		return "", fmt.Errorf("not a room")
	}
	var answer struct {
		Room string `json:"room_id"`
	}
	status, data, err := l.json(ctx, "POST", matrix_api(l, "client/v3/join/"+url.PathEscape(address), nil), map[string]any{}, &answer)
	if err != nil {
		return "", err
	}
	if status != 200 || answer.Room == "" {
		return "", linked_failure(status, data)
	}
	return answer.Room, nil
}

// matrix_room_leave leaves a room, or declines an invitation to it.
func matrix_room_leave(ctx context.Context, l *linked, room string) error {
	status, data, err := l.json(ctx, "POST", matrix_api(l, "client/v3/rooms/"+url.PathEscape(room)+"/leave", nil), map[string]any{}, nil)
	if err != nil {
		return err
	}
	if status != 200 && status != 404 && status != 403 {
		return linked_failure(status, data)
	}
	return nil
}

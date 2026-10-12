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
// functions through linked_inbound. The rooms and messages themselves are
// chat's, built on this in its own phases.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
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

// matrix_run is the account's sync loop: each sync waits at the server for
// events and hands what arrives to linked_inbound, with the position kept on
// the account so a restart carries on. It returns when the connection fails,
// to be run again, or with linked_signed_out when the server no longer
// accepts the account.
func matrix_run(ctx context.Context, l *linked, s *linked_session) error {
	since := l.get("since")
	for {
		query := map[string]string{"timeout": strconv.FormatInt(matrix_sync_timeout.Milliseconds(), 10)}
		if since != "" {
			query["since"] = since
		} else {
			query["filter"] = `{"room":{"timeline":{"limit":20}}}`
		}
		var sync struct {
			Next     string          `json:"next_batch"`
			Rooms    json.RawMessage `json:"rooms"`
			Device   json.RawMessage `json:"to_device"`
			Presence json.RawMessage `json:"presence"`
			Retry    int64           `json:"retry_after_ms"`
		}
		bounded, cancel := context.WithTimeout(ctx, matrix_sync_timeout+linked_timeout)
		status, body, err := l.json(bounded, "GET", matrix_api(l, "client/v3/sync", query), nil, &sync)
		cancel()
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			return err
		}
		switch {
		case status == 401:
			return linked_signed_out
		case status == 429:
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
		case status != 200:
			return linked_failure(status, body)
		}
		s.connected.Store(true)
		s.reason.Store("")
		if len(sync.Rooms) > 2 || len(sync.Device) > 2 || len(sync.Presence) > 2 {
			event := map[string]any{"kind": "sync", "since": since}
			var rooms, device, presence any
			json.Unmarshal(sync.Rooms, &rooms)
			json.Unmarshal(sync.Device, &device)
			json.Unmarshal(sync.Presence, &presence)
			event["rooms"], event["device"], event["presence"] = rooms, device, presence
			linked_inbound(l.user, l.id, "matrix", event)
		}
		if sync.Next != "" && sync.Next != since {
			since = sync.Next
			l.set("since", since)
			l.save()
		}
	}
}

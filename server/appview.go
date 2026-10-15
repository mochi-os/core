// Mochi server: Bluesky's index
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Reading a followed account needs no central service, but notifications,
// reply threads, custom feeds, lists and search need an index of the whole
// network, and Mochi forwards those requests to Bluesky's (decision 22 of the
// federation plan) rather than indexing the network itself. A hosted account
// speaks to the index as itself: each request carries a short-lived token
// signed with the account's key, naming the index and the method, as the
// protocol has a data server mint for its clients. The labellers named on the
// request - Bluesky's own moderation service, the owner's and the user's -
// have their labels come back on the posts and people read.

package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// bluesky_appview is the index requests are forwarded to, and its identifier,
// which the token names; a development setting points them at one on this
// machine.
var bluesky_appview = "https://api.bsky.app"
var bluesky_appview_did = "did:web:api.bsky.app"

// bluesky_labelers are the labellers every request names: Bluesky's own
// moderation service, whose labels its own app applies.
var bluesky_labelers = []string{"did:plc:ar7c4by46qjdydhdevvrndac"}

// appview_token_lifetime bounds a token: one request's worth.
const appview_token_lifetime = 60

// atproto_token mints the token a hosted account presents to another service:
// a JWT signed with the account's signing key, naming the account, the service
// and the method, good for a minute.
func atproto_token(user *User, row map[string]any, audience string, method string) (string, error) {
	id, _ := row["id"].(string)
	did := atproto_did(row)
	key := keypair_get(user, id, key_signing)
	if did == "" || key == nil {
		return "", fmt.Errorf("account has no identity")
	}
	encode := func(v map[string]any) string {
		return base64.RawURLEncoding.EncodeToString([]byte(json_encode(v)))
	}
	data := encode(map[string]any{"typ": "JWT", "alg": "ES256"}) + "." + encode(map[string]any{
		"iss": did,
		"aud": audience,
		"exp": now() + appview_token_lifetime,
		"iat": now(),
		"jti": uid(),
		"lxm": method,
	})
	signature, err := key_sign_p256(key.Private, []byte(data))
	if err != nil {
		return "", err
	}
	return data + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}

// appview_labelers is the header naming the labellers whose labels a request
// wants: Bluesky's own, the owner's setting and the account's own list.
func appview_labelers(row map[string]any) string {
	seen := map[string]bool{}
	var out []string
	add := func(did string) {
		did = strings.TrimSpace(did)
		if did != "" && strings.HasPrefix(did, "did:") && !seen[did] {
			seen[did] = true
			out = append(out, did)
		}
	}
	for _, did := range bluesky_labelers {
		add(did)
	}
	for _, did := range strings.Split(setting_effective("labelers"), ",") {
		add(did)
	}
	for _, did := range object_strings(account_data(row)["labelers"]) {
		add(did)
	}
	return strings.Join(out, ", ")
}

// appview_request sends one request to the index as a hosted account: a
// query by GET with its parameters, a procedure by POST with a JSON body. The
// index is reached as any other host is, under the bluesky setting, the host
// policy and the limiter.
func appview_request(ctx context.Context, user *User, row map[string]any, method string, nsid string, query map[string]string, body any) (int, []byte, error) {
	token, err := atproto_token(user, row, bluesky_appview_did, nsid)
	if err != nil {
		return 0, nil, err
	}
	address := strings.TrimSuffix(bluesky_appview, "/") + "/xrpc/" + nsid
	if len(query) > 0 {
		values := url.Values{}
		for key, value := range query {
			values.Set(key, value)
		}
		address += "?" + values.Encode()
	}
	var reader *bytes.Reader
	if body != nil {
		reader = bytes.NewReader([]byte(json_encode(body)))
	} else {
		reader = bytes.NewReader(nil)
	}
	ctx, cancel := context.WithTimeout(ctx, network_timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, method, address, reader)
	if err != nil {
		return 0, nil, err
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Atproto-Accept-Labelers", appview_labelers(row))
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	id, _ := row["id"].(string)
	response, err := network_send(ctx, user, "bluesky", strings.ToLower(request.URL.Hostname())+"|"+id, request, 3)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	data, err := read_limited(response.Body, linked_body_maximum)
	if err != nil {
		return response.StatusCode, nil, err
	}
	return response.StatusCode, data, nil
}

// appview_page reads one page of a feed query from the index: the items under
// field and the cursor.
func appview_page(ctx context.Context, user *User, row map[string]any, nsid string, query map[string]string, field string, cursor string, limit int) (*linked_page, error) {
	if limit > 100 {
		limit = 100
	}
	query["limit"] = strconv.Itoa(limit)
	if cursor != "" {
		query["cursor"] = cursor
	}
	status, body, err := appview_request(ctx, user, row, "GET", nsid, query, nil)
	if err != nil {
		return nil, err
	}
	if status < 200 || status > 299 {
		return nil, linked_failure(status, body)
	}
	var answer map[string]any
	if err := json_decode(body, &answer); err != nil {
		return nil, err
	}
	items, _ := answer[field].([]any)
	page := &linked_page{Items: items}
	if len(items) > 0 {
		page.Cursor, _ = answer["cursor"].(string)
	}
	return page, nil
}

// appview_forward sends an app's own request through a hosted account: the
// index's methods go to the index; the account's own repository is this
// server's, which the app reads through mochi.account.posts and the engine.
func appview_forward(ctx context.Context, user *User, row map[string]any, method string, nsid string, query map[string]string, body any) (map[string]any, error) {
	if strings.Contains(nsid, "/") {
		return nil, fmt.Errorf("the path is an XRPC method name")
	}
	if !strings.HasPrefix(nsid, "app.bsky.") && !strings.HasPrefix(nsid, "chat.bsky.") {
		return nil, fmt.Errorf("%s is not forwarded to the index", nsid)
	}
	status, data, err := appview_request(ctx, user, row, method, nsid, query, body)
	if err != nil {
		return nil, err
	}
	return map[string]any{"status": status, "data": linked_decode(data)}, nil
}

// atproto_mute mutes or unmutes a person for a hosted account at the index,
// which keeps mutes for the account: they are private, not records.
func atproto_mute(ctx context.Context, user *User, row map[string]any, did string, on bool) error {
	method := "app.bsky.graph.muteActor"
	if !on {
		method = "app.bsky.graph.unmuteActor"
	}
	status, body, err := appview_request(ctx, user, row, "POST", method, nil, map[string]any{"actor": did})
	if err != nil {
		return err
	}
	if status < 200 || status > 299 {
		return linked_failure(status, body)
	}
	return nil
}

// atproto_relay_cache keeps the relay's last answer for a few minutes.
var atproto_relay_cache struct {
	when   time.Time
	answer map[string]any
}

// atproto_relay_status is this server's standing with the relay, for the
// owner's Status page: whether the relay follows it, how many accounts it
// admits and how many it counts, read at most every few minutes.
func atproto_relay_status(ctx context.Context) map[string]any {
	if time.Since(atproto_relay_cache.when) < 5*time.Minute && atproto_relay_cache.answer != nil {
		return atproto_relay_cache.answer
	}
	endpoint, err := url.Parse(atproto_endpoint(server_domain()))
	if err != nil || endpoint.Host == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, "GET", strings.TrimSuffix(bluesky_relay, "/")+"/xrpc/com.atproto.sync.getHostStatus?hostname="+url.QueryEscape(endpoint.Host), nil)
	if err != nil {
		return nil
	}
	request.Header.Set("Accept", "application/json")
	answer := map[string]any{"host": endpoint.Host, "status": "unknown"}
	if response, err := network_request(ctx, nil, "", request, 0); err == nil {
		defer response.Body.Close()
		var status struct {
			Status       string `json:"status"`
			AccountCount int64  `json:"accountCount"`
			AccountLimit int64  `json:"accountLimit"`
			Seq          int64  `json:"seq"`
			Error        string `json:"error"`
		}
		if data, err := read_limited(response.Body, 64<<10); err == nil && json.Unmarshal(data, &status) == nil {
			switch {
			case response.StatusCode == 200 && status.Status != "":
				answer["status"] = status.Status
				answer["accounts"] = status.AccountCount
				answer["limit"] = status.AccountLimit
				answer["sequence"] = status.Seq
			case status.Error != "":
				answer["status"] = status.Error
			}
		}
	}
	atproto_relay_cache.when, atproto_relay_cache.answer = time.Now(), answer
	return answer
}

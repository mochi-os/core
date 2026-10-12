// Mochi server: People on other networks
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// An author in feeds, forums and chat is either a Mochi identity or a person's
// own permanent identifier on another network, stored as it is: an ActivityPub
// account URL, a Bluesky DID, a Matrix address, an XMPP address with its
// scheme. The two kinds cannot be confused, since a Mochi identity never
// contains a colon and each of these does. Apps validate an author with
// mochi.text.valid(..., "author") and show one with mochi.account.resolve, which
// turns the identifier into a name, an avatar and a readable address fetched
// from the network and cached. The raw identifier is never displayed.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	sl "go.starlark.net/starlark"
)

// The identifier forms, by network.
var resolve_did_re = regexp.MustCompile(`^did:(plc:[a-z2-7]{24}|web:[a-z0-9.-]+(%3A[0-9]+)?(:[a-zA-Z0-9._-]+)*)$`)
var resolve_matrix_re = regexp.MustCompile(`^@[^@:\s/]{1,255}:[a-zA-Z0-9.-]+(:[0-9]+)?$`)
var resolve_xmpp_re = regexp.MustCompile(`^xmpp:[^@\s/]{1,255}@[a-zA-Z0-9.-]+$`)
var resolve_acct_re = regexp.MustCompile(`^@?([^@\s/]{1,255})@([a-zA-Z0-9.-]+)$`)

// author_network says which network an identifier belongs to - "fediverse",
// "bluesky", "matrix" or "xmpp" - or empty for none of them.
func author_network(identifier string) string {
	switch {
	case strings.HasPrefix(identifier, "https://"):
		u, err := url.Parse(identifier)
		if err != nil || u.Host == "" || u.User != nil || len(identifier) > 2048 || strings.ContainsAny(identifier, " \t\r\n") {
			return ""
		}
		return "fediverse"
	case resolve_did_re.MatchString(identifier):
		return "bluesky"
	case resolve_matrix_re.MatchString(identifier):
		return "matrix"
	case resolve_xmpp_re.MatchString(identifier):
		return "xmpp"
	}
	return ""
}

// author_valid is the "author" validator: a Mochi identity or a network
// identifier of a known kind.
func author_valid(s string) bool {
	return valid(s, "entity") || author_network(s) != ""
}

// resolved is what the UI shows for a person on another network, and what
// sending to them needs.
type resolved struct {
	Network string `json:"network"`
	Address string `json:"address"` // readable, such as @bob@lemmy.world
	Name    string `json:"name"`
	Avatar  string `json:"avatar"`
	URL     string `json:"url"`    // the person's page, where the network has one
	ID      string `json:"id"`     // the person's permanent identifier
	Inbox   string `json:"inbox"`  // where to deliver to the person, on the fediverse
	Shared  string `json:"shared"` // their server's shared inbox, where it has one
}

// identifier_host is the host an identifier names, for the host policy: empty
// where the identifier names none, as a did:plc does.
func identifier_host(identifier string) string {
	host := ""
	switch {
	case strings.HasPrefix(identifier, "https://"):
		if u, err := url.Parse(identifier); err == nil {
			host = u.Hostname()
		}
	case strings.HasPrefix(identifier, "xmpp:"):
		if i := strings.LastIndexByte(identifier, '@'); i >= 0 {
			host = identifier[i+1:]
		}
	case strings.HasPrefix(identifier, "did:web:"):
		host = strings.ReplaceAll(strings.SplitN(strings.TrimPrefix(identifier, "did:web:"), ":", 2)[0], "%3A", ":")
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}
	case resolve_matrix_re.MatchString(identifier):
		host = identifier[strings.IndexByte(identifier, ':')+1:]
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}
	default:
		if m := resolve_acct_re.FindStringSubmatch(identifier); m != nil {
			host = m[2]
		}
	}
	return strings.ToLower(host)
}

// The cache: a lookup fetched from the network is kept for a day, a failed one
// for an hour, so a feed of a hundred comments by one person costs one fetch.
// Bounded by count; when full the whole cache is dropped, which costs a refetch
// and nothing else.
const resolve_cache_success = 24 * time.Hour
const resolve_cache_failure = time.Hour
const resolve_cache_maximum = 10000

type resolve_entry struct {
	value   *resolved
	expires time.Time
}

var resolve_cache = map[string]resolve_entry{}
var resolve_lock sync.Mutex

// resolve_fetch does the network request, under the host policy: the owner's
// domain blocks, the limiter per host and the private-address check. A variable
// so tests substitute documents without a server.
var resolve_fetch = func(ctx context.Context, address string, accept string) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	request, err := http.NewRequestWithContext(ctx, "GET", address, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Accept", accept)
	response, err := network_request(ctx, nil, "", request, 5)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode > 299 {
		return nil, fmt.Errorf("status %d", response.StatusCode)
	}
	return io.ReadAll(io.LimitReader(response.Body, resolve_document_maximum))
}

// resolve_document_maximum caps a profile document; actor documents run to a
// few kilobytes.
const resolve_document_maximum = 512 << 10

// author_resolve looks an identifier up, from the cache or the network.
func author_resolve(ctx context.Context, identifier string) (*resolved, error) {
	identifier = strings.TrimSpace(identifier)
	resolve_lock.Lock()
	if entry, found := resolve_cache[identifier]; found && time.Now().Before(entry.expires) {
		resolve_lock.Unlock()
		if entry.value == nil {
			return nil, fmt.Errorf("not resolved")
		}
		return entry.value, nil
	}
	resolve_lock.Unlock()

	value, err := resolve_network(ctx, identifier)

	resolve_lock.Lock()
	if len(resolve_cache) >= resolve_cache_maximum {
		resolve_cache = map[string]resolve_entry{}
	}
	if err != nil {
		resolve_cache[identifier] = resolve_entry{nil, time.Now().Add(resolve_cache_failure)}
	} else {
		resolve_cache[identifier] = resolve_entry{value, time.Now().Add(resolve_cache_success)}
	}
	resolve_lock.Unlock()
	return value, err
}

// resolve_network fetches what the identifier's network publishes about the
// person.
func resolve_network(ctx context.Context, identifier string) (*resolved, error) {
	// A fediverse handle, @user@host or user@host, is turned into the account
	// URL by WebFinger first; it is a convenience of lookup, not an author form.
	if m := resolve_acct_re.FindStringSubmatch(identifier); m != nil && !strings.HasPrefix(identifier, "xmpp:") {
		actor, err := resolve_webfinger(ctx, m[1], strings.ToLower(m[2]))
		if err != nil {
			return nil, err
		}
		identifier = actor
	}
	switch author_network(identifier) {
	case "fediverse":
		return resolve_activitypub(ctx, identifier)
	case "bluesky":
		return resolve_bluesky(ctx, identifier)
	case "matrix":
		return resolve_matrix(ctx, identifier)
	case "xmpp":
		return resolve_xmpp(identifier), nil
	}
	return nil, fmt.Errorf("not a network identifier")
}

// resolve_json fetches and decodes a JSON document.
func resolve_json(ctx context.Context, address string, accept string, into any) error {
	body, err := resolve_fetch(ctx, address, accept)
	if err != nil {
		return err
	}
	return json.Unmarshal(body, into)
}

// resolve_webfinger turns user@host into the account URL the host publishes.
func resolve_webfinger(ctx context.Context, user string, host string) (string, error) {
	var document struct {
		Links []struct {
			Rel  string `json:"rel"`
			Type string `json:"type"`
			Href string `json:"href"`
		} `json:"links"`
	}
	address := "https://" + host + "/.well-known/webfinger?resource=" + url.QueryEscape("acct:"+user+"@"+host)
	if err := resolve_json(ctx, address, "application/jrd+json, application/json", &document); err != nil {
		return "", err
	}
	for _, link := range document.Links {
		if link.Rel == "self" && strings.HasPrefix(link.Href, "https://") && (strings.Contains(link.Type, "activity+json") || strings.Contains(link.Type, "ld+json")) {
			return link.Href, nil
		}
	}
	return "", fmt.Errorf("no account at %s@%s", user, host)
}

// resolve_activitypub reads an actor document: the person's preferred username,
// display name and icon.
func resolve_activitypub(ctx context.Context, actor string) (*resolved, error) {
	var document struct {
		ID                string `json:"id"`
		Type              string `json:"type"`
		PreferredUsername string `json:"preferredUsername"`
		Name              string `json:"name"`
		URL               any    `json:"url"`
		Icon              any    `json:"icon"`
		Inbox             string `json:"inbox"`
		Endpoints         struct {
			SharedInbox string `json:"sharedInbox"`
		} `json:"endpoints"`
	}
	if err := resolve_json(ctx, actor, "application/activity+json, application/ld+json", &document); err != nil {
		return nil, err
	}
	host := ""
	if u, err := url.Parse(actor); err == nil {
		host = strings.ToLower(u.Host)
	}
	// An actor's document must be served by the host its id names, so no
	// server can claim an id on another domain.
	if document.ID != "" && identifier_host(document.ID) != strings.ToLower(host) {
		return nil, fmt.Errorf("actor id %s served by %s", document.ID, host)
	}
	r := &resolved{Network: "fediverse", Name: document.Name, URL: actor, ID: actor, Inbox: document.Inbox, Shared: document.Endpoints.SharedInbox}
	if document.PreferredUsername != "" {
		r.Address = "@" + document.PreferredUsername + "@" + host
	} else {
		r.Address = actor
	}
	if r.Name == "" {
		r.Name = document.PreferredUsername
	}
	if page := resolve_first_url(document.URL); page != "" {
		r.URL = page
	}
	r.Avatar = resolve_icon(document.Icon)
	return r, nil
}

// resolve_first_url reads an ActivityPub url property, a string or a list of
// strings or Link objects, and returns the first https URL in it.
func resolve_first_url(v any) string {
	switch value := v.(type) {
	case string:
		if strings.HasPrefix(value, "https://") {
			return value
		}
	case []any:
		for _, item := range value {
			if found := resolve_first_url(item); found != "" {
				return found
			}
		}
	case map[string]any:
		if href, _ := value["href"].(string); strings.HasPrefix(href, "https://") {
			return href
		}
	}
	return ""
}

// resolve_icon reads an actor's icon, an Image object or a list of them.
func resolve_icon(v any) string {
	switch value := v.(type) {
	case map[string]any:
		if address, _ := value["url"].(string); strings.HasPrefix(address, "https://") {
			return address
		}
	case []any:
		for _, item := range value {
			if found := resolve_icon(item); found != "" {
				return found
			}
		}
	}
	return ""
}

// resolve_bluesky reads the DID document for the account's handle and data
// server, then its profile record there.
func resolve_bluesky(ctx context.Context, did string) (*resolved, error) {
	var document struct {
		AlsoKnownAs []string `json:"alsoKnownAs"`
		Service     []struct {
			ID              string `json:"id"`
			Type            string `json:"type"`
			ServiceEndpoint string `json:"serviceEndpoint"`
		} `json:"service"`
	}
	address := "https://plc.directory/" + did
	if strings.HasPrefix(did, "did:web:") {
		host := strings.ReplaceAll(strings.TrimPrefix(did, "did:web:"), "%3A", ":")
		address = "https://" + host + "/.well-known/did.json"
	}
	if err := resolve_json(ctx, address, "application/did+ld+json, application/json", &document); err != nil {
		return nil, err
	}
	r := &resolved{Network: "bluesky", Address: did}
	for _, known := range document.AlsoKnownAs {
		if handle, ok := strings.CutPrefix(known, "at://"); ok {
			r.Address = "@" + handle
			r.URL = "https://bsky.app/profile/" + handle
			break
		}
	}
	pds := ""
	for _, service := range document.Service {
		if service.ID == "#atproto_pds" && strings.HasPrefix(service.ServiceEndpoint, "https://") {
			pds = strings.TrimSuffix(service.ServiceEndpoint, "/")
		}
	}
	if pds == "" {
		return r, nil
	}
	var record struct {
		Value struct {
			DisplayName string `json:"displayName"`
			Avatar      struct {
				Ref struct {
					Link string `json:"$link"`
				} `json:"ref"`
			} `json:"avatar"`
		} `json:"value"`
	}
	profile := pds + "/xrpc/com.atproto.repo.getRecord?repo=" + url.QueryEscape(did) + "&collection=app.bsky.actor.profile&rkey=self"
	if err := resolve_json(ctx, profile, "application/json", &record); err == nil {
		r.Name = record.Value.DisplayName
		if cid := record.Value.Avatar.Ref.Link; cid != "" {
			r.Avatar = pds + "/xrpc/com.atproto.sync.getBlob?did=" + url.QueryEscape(did) + "&cid=" + url.QueryEscape(cid)
		}
	}
	if r.Name == "" {
		r.Name = strings.TrimPrefix(r.Address, "@")
	}
	return r, nil
}

// resolve_matrix finds the home server through its client well-known and reads
// the public profile.
func resolve_matrix(ctx context.Context, address string) (*resolved, error) {
	server := address[strings.IndexByte(address, ':')+1:]
	base := "https://" + server
	var known struct {
		Homeserver struct {
			URL string `json:"base_url"`
		} `json:"m.homeserver"`
	}
	if err := resolve_json(ctx, base+"/.well-known/matrix/client", "application/json", &known); err == nil && strings.HasPrefix(known.Homeserver.URL, "https://") {
		base = strings.TrimSuffix(known.Homeserver.URL, "/")
	}
	var profile struct {
		Name   string `json:"displayname"`
		Avatar string `json:"avatar_url"`
	}
	if err := resolve_json(ctx, base+"/_matrix/client/v3/profile/"+url.PathEscape(address), "application/json", &profile); err != nil {
		return nil, err
	}
	r := &resolved{Network: "matrix", Address: address, Name: profile.Name, URL: "https://matrix.to/#/" + address}
	if r.Name == "" {
		r.Name = address[1:strings.IndexByte(address, ':')]
	}
	// An mxc:// avatar is served by the home server's media endpoint.
	if rest, ok := strings.CutPrefix(profile.Avatar, "mxc://"); ok && strings.Count(rest, "/") == 1 {
		r.Avatar = base + "/_matrix/client/v1/media/download/" + rest
	}
	return r, nil
}

// resolve_xmpp needs no request: an XMPP address is readable as it is, and a
// profile is fetched over a stream, which comes with the XMPP engine.
func resolve_xmpp(identifier string) *resolved {
	address := strings.TrimPrefix(identifier, "xmpp:")
	return &resolved{Network: "xmpp", Address: address, Name: address[:strings.IndexByte(address, '@')], URL: identifier}
}

// mochi.account.resolve(identifier) -> dict | None: What the UI shows for a
// person on another network - network, address, name, avatar and url - fetched
// from the network and cached, or None when the identifier is not a network
// identifier or the network did not answer.
func api_account_resolve(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <identifier: string>")
	}
	user := principal_caller(t)
	if user == nil {
		return sl_error(fn, "no user")
	}
	identifier, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid identifier")
	}
	// The user's own blocks and the protocol setting are the caller's; the
	// owner's blocks and the limiter apply inside the fetch, to every host it
	// reaches.
	network := author_network(identifier)
	if network == "" && resolve_acct_re.MatchString(identifier) && !strings.HasPrefix(identifier, "xmpp:") {
		network = "fediverse"
	}
	if host := identifier_host(identifier); host != "" {
		if err := host_allowed(user, network, host); err != nil {
			return sl.None, nil
		}
	} else if network != "" && !network_enabled(network) {
		return sl.None, nil
	}
	r, err := author_resolve(thread_context(t), identifier)
	if err != nil {
		return sl.None, nil
	}
	return sl_encode(map[string]any{
		"network": r.Network,
		"address": r.Address,
		"name":    r.Name,
		"avatar":  r.Avatar,
		"url":     r.URL,
	}), nil
}
